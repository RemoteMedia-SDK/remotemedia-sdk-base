//! Server-side WebRTC peer with pipeline integration
//!
//! ServerPeer represents a server-side WebRTC peer connection that automatically
//! routes media through a RemoteMedia pipeline. Created when clients announce
//! via gRPC signaling.
//!
//! ## Multi-Track Streaming (Spec 013)
//!
//! ServerPeer supports dynamic multi-track streaming via TrackRegistry and FrameRouter:
//! - Tracks are created lazily when first frame with new stream_id arrives
//! - Frames are routed to appropriate tracks based on stream_id field
//! - Backward compatible: frames without stream_id use default track

// Phase 4 (US2) server peer infrastructure
#![allow(dead_code)]
use crate::media::{
    extract_stream_id,
    tracks::{AudioTrack, VideoTrack},
    TrackRegistry, DEFAULT_STREAM_ID,
};
#[cfg(feature = "ws-signaling")]
use crate::signaling::{current_timestamp_ns, WebRtcEventBridge};
use crate::{config::WebRtcTransportConfig, peer::PeerConnection, Error, Result};
use prost::Message;
use remotemedia_core::{
    data::RuntimeData,
    manifest::Manifest,
    transport::{
        data::participant, session_control::SessionControl, Participant, ParticipantSessionHandle,
        PipelineSessionHost, SharedPipelineOutputReceivers,
        SharedPipelineSession as CoreSharedPipelineSession, TransportData,
    },
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Mutex as TokioMutex, RwLock};
use tracing::{debug, error, info, trace, warn};
use webrtc::data_channel::RTCDataChannel;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

fn participant_metadata(
    peer_id: &str,
    track_id: impl Into<String>,
    modality: &'static str,
) -> HashMap<String, String> {
    HashMap::from([
        (participant::ID.to_string(), peer_id.to_string()),
        (
            participant::ROLE.to_string(),
            participant::role::CLIENT.to_string(),
        ),
        (participant::TRACK_ID.to_string(), track_id.into()),
        (participant::MODALITY.to_string(), modality.to_string()),
    ])
}

/// Fan-out dispatcher that routes session outputs to per-track consumer
/// tasks instead of awaiting each `track.send()` inline.
///
/// **Why this exists.** The session emits Audio + Video + Json outputs
/// for multiple stream_ids on a single `recv_output()` channel. Awaiting
/// each `send_to_webrtc()` inline serialises every output: a slow video
/// encode for stream A blocks audio for stream A *and* anything for
/// stream B *and* the data channel. In observed runs this introduced
/// 50+ s gaps between audio2face emitting TTS audio and the browser
/// receiving it.
///
/// **What we do instead.** Each `(media kind, stream_id)` pair gets its
/// own bounded mpsc + a dedicated consumer task. Order *within* a stream
/// is preserved (single consumer pops in order); independent streams /
/// kinds run concurrently. The drainer's only blocking op per output is
/// a bounded `mpsc::send` to the right shard, which fills only when that
/// specific shard is itself overloaded.
///
/// Json/Text envelopes still go through the data channel inline because
/// they're low-rate and don't have parallelism to extract.
struct PerTrackDispatcher {
    audio_txs: TokioMutex<HashMap<String, mpsc::Sender<TransportData>>>,
    video_txs: TokioMutex<HashMap<String, mpsc::Sender<TransportData>>>,
    track_registry: Arc<TrackRegistry<AudioTrack, VideoTrack>>,
    data_channel: Arc<RwLock<Option<Arc<RTCDataChannel>>>>,
    peer_id: String,
    /// Total video frames dropped across all streams for this peer due to a
    /// full per-stream shard. Incremented on `try_send` Full; sampled by the
    /// rate-limited drop log so we don't spam when the encoder is steadily
    /// behind.
    video_dropped: std::sync::atomic::AtomicU64,
}

/// Audio shard depth. Audio gaps from dropped frames are immediately
/// audible (clicks, glitches), so audio uses a deep buffer + blocking
/// send: backpressure propagates upstream and we never drop. At 50 Hz
/// this is ~5 s of headroom.
const PER_TRACK_SHARD_CAPACITY: usize = 256;

/// Video shard depth. Kept tiny on purpose: a deep video queue hides
/// latency behind a buffer the *user sees* as lag. At 30 fps this is
/// ~133 ms of backlog before drops kick in — small enough that the
/// queue stays fresh, large enough to absorb a brief encoder hiccup.
/// When full, the dispatcher drops the incoming frame (drop-newest)
/// rather than blocking, since for live video the next captured frame
/// is more useful than waiting for a stale one to clear.
const PER_TRACK_VIDEO_SHARD_CAPACITY: usize = 4;

impl PerTrackDispatcher {
    fn new(
        track_registry: Arc<TrackRegistry<AudioTrack, VideoTrack>>,
        data_channel: Arc<RwLock<Option<Arc<RTCDataChannel>>>>,
        peer_id: String,
    ) -> Arc<Self> {
        Arc::new(Self {
            audio_txs: TokioMutex::new(HashMap::new()),
            video_txs: TokioMutex::new(HashMap::new()),
            track_registry,
            data_channel,
            peer_id,
            video_dropped: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Route one TransportData to the appropriate per-track shard.
    /// Awaits only on the shard's bounded mpsc send (fast unless that
    /// shard is overwhelmed). Json/Text go inline through the data
    /// channel.
    async fn dispatch(self: &Arc<Self>, transport_data: TransportData) {
        let stream_id = extract_stream_id(&transport_data.data)
            .unwrap_or(DEFAULT_STREAM_ID)
            .to_string();
        match &transport_data.data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                ..
            } => {
                // DIAG: per-chunk arrival timestamp at the per-peer
                // dispatch entry. Compared against the LFM2 plugin's
                // `[audio] first audio emitted` log + the audio
                // consumer's `consumer RECV` line, this triangulates
                // where streaming stalls in the
                //   plugin → IPC → router → peer-dispatch → consumer
                //         → audio_sender ring buffer → Opus encoder
                // chain. INFO-level so we don't need an env var to
                // capture during a real session.
                let samples_len = samples.len();
                let sr = *sample_rate;
                info!(
                    "dispatch RECV audio: stream='{}' samples={} sr={} peer={}",
                    stream_id, samples_len, sr, self.peer_id
                );
                let pre_send = std::time::Instant::now();
                let tx = self.get_or_spawn_audio(&stream_id).await;
                let send_res = tx.send(transport_data).await;
                let send_ms = pre_send.elapsed().as_millis();
                if send_res.is_err() {
                    warn!(
                        "audio dispatch closed for stream '{}' (peer {})",
                        stream_id, self.peer_id
                    );
                } else if send_ms > 50 {
                    // Per-stream shard backpressure — diagnostic-worthy
                    // because it indicates the per-stream consumer (audio
                    // encoder + ring-buffer sender) is the bottleneck, not
                    // anything upstream.
                    info!(
                        "audio dispatch slow: stream='{}' send_wait={}ms (per-stream shard full)",
                        stream_id, send_ms
                    );
                }
            }
            RuntimeData::Video { .. } => {
                // Drop-newest semantics: the per-stream video shard is
                // tiny (~4 frames) and `try_send` lets us drop the
                // incoming frame instead of blocking when the encoder
                // can't keep up. Dropping the *new* frame (rather than
                // an old one already in the queue) is cheap to
                // implement; the encoder still makes forward progress
                // and the next captured frame supersedes this one
                // anyway. Backpressure that propagates all the way
                // upstream just shifts the lag into a different buffer
                // — and that lag is what the user perceives as latency.
                let tx = self.get_or_spawn_video(&stream_id).await;
                match tx.try_send(transport_data) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        let total = self
                            .video_dropped
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                            + 1;
                        // Log at the start of a drop run and then every
                        // ~30 frames (~1 s at 30 fps) so we can see the
                        // sustained drop rate without spamming.
                        if total == 1 || total % 30 == 0 {
                            warn!(
                                "video shard full for stream '{}' (peer {}): dropped {} frames total — encoder slower than source",
                                stream_id, self.peer_id, total
                            );
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        warn!(
                            "video dispatch closed for stream '{}' (peer {})",
                            stream_id, self.peer_id
                        );
                    }
                }
            }
            RuntimeData::Json(_) | RuntimeData::Text(_) => {
                if let Err(e) =
                    Self::send_data_channel(&self.data_channel, &transport_data.data).await
                {
                    error!("data channel send failed (peer {}): {}", self.peer_id, e);
                }
            }
            _ => {
                trace!(
                    "ignoring unsupported RuntimeData variant for peer {}",
                    self.peer_id
                );
            }
        }
    }

    async fn flush_audio_tracks(&self) -> usize {
        let mut dropped = 0;
        for stream_id in self.track_registry.audio_stream_ids().await {
            if let Some(track) = self.track_registry.get_audio_track(&stream_id).await {
                dropped += track.flush_send_buffer().await;
            }
        }
        dropped
    }

    async fn get_or_spawn_audio(self: &Arc<Self>, stream_id: &str) -> mpsc::Sender<TransportData> {
        let mut map = self.audio_txs.lock().await;
        if let Some(tx) = map.get(stream_id) {
            return tx.clone();
        }
        let (tx, mut rx) = mpsc::channel::<TransportData>(PER_TRACK_SHARD_CAPACITY);
        let stream = stream_id.to_string();
        let registry = self.track_registry.clone();
        let peer = self.peer_id.clone();
        tokio::spawn(async move {
            let mut first = true;
            let mut chunk_idx: u64 = 0;
            while let Some(td) = rx.recv().await {
                let RuntimeData::Audio {
                    samples,
                    sample_rate,
                    ..
                } = td.data
                else {
                    continue;
                };
                let n = samples.len();
                let Some(track) = registry.get_audio_track(&stream).await else {
                    warn!(
                        "no audio track for stream '{}' (peer {}); dropping {} samples",
                        stream, peer, n
                    );
                    continue;
                };
                chunk_idx += 1;
                // DIAG: per-chunk arrival at the consumer task —
                // sibling of `dispatch RECV audio`. If `dispatch RECV`
                // and `consumer RECV` interleave in real time but
                // `FIRST send_audio` is delayed, the buffer between
                // them (per-track shard, default cap=256) is what's
                // absorbing the reply.
                info!(
                    "consumer RECV audio #{}: stream='{}' samples={} sr={}",
                    chunk_idx, stream, n, sample_rate
                );
                let send_start = std::time::Instant::now();
                if first {
                    info!(
                        "audio consumer FIRST send_audio: stream='{}' samples={} sr={}",
                        stream, n, sample_rate
                    );
                    first = false;
                }
                let res = track
                    .send_audio(Arc::new(samples.to_vec()), sample_rate)
                    .await;
                let send_ms = send_start.elapsed().as_millis();
                if let Err(e) = res {
                    warn!(
                        "audio send failed for stream '{}' (peer {}): {}",
                        stream, peer, e
                    );
                } else if send_ms > 100 {
                    info!(
                        "audio consumer slow send_audio: stream='{}' samples={} took={}ms",
                        stream, n, send_ms
                    );
                }
                registry.record_audio_frame(&stream).await;
            }
            debug!(
                "audio consumer ended for stream '{}' (peer {})",
                stream, peer
            );
        });
        map.insert(stream_id.to_string(), tx.clone());
        tx
    }

    async fn get_or_spawn_video(self: &Arc<Self>, stream_id: &str) -> mpsc::Sender<TransportData> {
        let mut map = self.video_txs.lock().await;
        if let Some(tx) = map.get(stream_id) {
            return tx.clone();
        }
        let (tx, mut rx) = mpsc::channel::<TransportData>(PER_TRACK_VIDEO_SHARD_CAPACITY);
        let stream = stream_id.to_string();
        let registry = self.track_registry.clone();
        let peer = self.peer_id.clone();
        tokio::spawn(async move {
            while let Some(td) = rx.recv().await {
                let Some(track) = registry.get_video_track(&stream).await else {
                    warn!(
                        "no video track for stream '{}' (peer {}); dropping frame",
                        stream, peer
                    );
                    continue;
                };
                if let Err(e) = track.send_video_runtime_data(td.data).await {
                    warn!(
                        "video send failed for stream '{}' (peer {}): {}",
                        stream, peer, e
                    );
                }
                registry.record_video_frame(&stream).await;
            }
            debug!(
                "video consumer ended for stream '{}' (peer {})",
                stream, peer
            );
        });
        map.insert(stream_id.to_string(), tx.clone());
        tx
    }

    async fn send_data_channel(
        data_channel: &Arc<RwLock<Option<Arc<RTCDataChannel>>>>,
        runtime_data: &RuntimeData,
    ) -> Result<()> {
        let dc_guard = data_channel.read().await;
        let Some(dc) = dc_guard.as_ref() else {
            trace!("no data channel available; dropping json/text output");
            return Ok(());
        };
        let data_buffer = crate::adapters::runtime_data_to_data_buffer(runtime_data);
        let encoded = data_buffer.encode_to_vec();
        if let Err(e) = dc.send(&bytes::Bytes::from(encoded)).await {
            return Err(Error::WebRtcError(format!(
                "Data channel send failed: {}",
                e
            )));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct PeerOutputTarget {
    dispatcher: Arc<PerTrackDispatcher>,
    #[cfg(feature = "ws-signaling")]
    event_tx: Option<mpsc::Sender<WebRtcEventBridge>>,
}

/// Shared core pipeline session for multiple WebRTC peers.
///
/// One instance owns the `SessionHandle` and drains its output receivers.
/// Peers register their own output dispatchers; every pipeline output is
/// fanned out to registered peers while all peer inputs share one core router.
pub struct SharedPipelineSession {
    core: Arc<CoreSharedPipelineSession>,
    output_targets: Arc<RwLock<HashMap<String, PeerOutputTarget>>>,
    close_when_empty: bool,
}

impl SharedPipelineSession {
    pub async fn new(
        core: Arc<CoreSharedPipelineSession>,
        control: Option<Arc<SessionControl>>,
        close_when_empty: bool,
    ) -> Result<Arc<Self>> {
        let session_id = core.session_id().to_string();
        let receivers = core.subscribe_outputs();
        let shared = Arc::new(Self {
            core,
            output_targets: Arc::new(RwLock::new(HashMap::new())),
            close_when_empty,
        });

        shared.spawn_output_drainers(receivers);

        if let Some(ctrl) = control {
            let (flush_tx, mut flush_rx) = tokio::sync::mpsc::channel::<()>(4);
            ctrl.install_flush_audio_hook(flush_tx).await;
            let output_targets = Arc::clone(&shared.output_targets);
            tokio::spawn(async move {
                while flush_rx.recv().await.is_some() {
                    let targets: Vec<PeerOutputTarget> =
                        output_targets.read().await.values().cloned().collect();
                    let mut dropped = 0usize;
                    for target in targets {
                        dropped += target.dispatcher.flush_audio_tracks().await;
                    }
                    if dropped > 0 {
                        tracing::debug!(
                            "[shared_pipeline_session] session {} flushed {} queued audio frames",
                            session_id,
                            dropped
                        );
                    }
                }
            });
        }

        Ok(shared)
    }

    pub fn session_id(&self) -> &str {
        self.core.session_id()
    }

    pub fn input_sender(&self) -> remotemedia_core::transport::SessionInputSender {
        self.core.input_sender()
    }

    pub fn participant_handle(&self, participant: Participant) -> ParticipantSessionHandle {
        self.core.participant_handle(participant)
    }

    pub async fn is_active(&self) -> bool {
        self.core.is_active().await
    }

    pub async fn attach_peer(
        &self,
        peer_id: String,
        track_registry: Arc<TrackRegistry<AudioTrack, VideoTrack>>,
        data_channel: Arc<RwLock<Option<Arc<RTCDataChannel>>>>,
        #[cfg(feature = "ws-signaling")] event_tx: Option<mpsc::Sender<WebRtcEventBridge>>,
    ) {
        let dispatcher = PerTrackDispatcher::new(track_registry, data_channel, peer_id.clone());
        self.output_targets.write().await.insert(
            peer_id,
            PeerOutputTarget {
                dispatcher,
                #[cfg(feature = "ws-signaling")]
                event_tx,
            },
        );
    }

    pub async fn detach_peer(&self, peer_id: &str) {
        let should_close = {
            let mut targets = self.output_targets.write().await;
            targets.remove(peer_id);
            self.close_when_empty && targets.is_empty()
        };

        if should_close {
            if let Err(e) = self.close().await {
                warn!(
                    "Error closing shared pipeline session {} after last peer detached: {}",
                    self.session_id(),
                    e
                );
            }
        }
    }

    pub async fn close(&self) -> Result<()> {
        self.core.close().await.map_err(|e| {
            Error::InternalError(format!("Failed to close shared pipeline session: {e}"))
        })
    }

    fn spawn_output_drainers(self: &Arc<Self>, receivers: SharedPipelineOutputReceivers) {
        let (audio_rx, video_rx, data_rx) = receivers.split();

        self.spawn_output_drainer(audio_rx, "audio");
        self.spawn_output_drainer(video_rx, "video");
        self.spawn_output_drainer(data_rx, "data");
    }

    fn spawn_output_drainer(
        self: &Arc<Self>,
        mut rx: broadcast::Receiver<TransportData>,
        kind: &'static str,
    ) {
        let output_targets = Arc::clone(&self.output_targets);
        let session_id = self.session_id().to_string();
        tokio::spawn(async move {
            loop {
                let transport_data = match rx.recv().await {
                    Ok(data) => data,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                };
                let targets: Vec<PeerOutputTarget> =
                    output_targets.read().await.values().cloned().collect();
                if targets.is_empty() {
                    trace!(
                        "dropping {} output for shared session {}: no attached WebRTC peers",
                        kind,
                        session_id
                    );
                    continue;
                }

                for target in targets {
                    #[cfg(feature = "ws-signaling")]
                    if let Some(ref tx) = target.event_tx {
                        let data_json = serde_json::to_string(&transport_data.data)
                            .unwrap_or_else(|_| "{}".to_string());
                        let event = WebRtcEventBridge::pipeline_output(
                            target.dispatcher.peer_id.clone(),
                            data_json,
                            current_timestamp_ns(),
                        );
                        if let Err(e) = tx.try_send(event) {
                            trace!(
                                "ws pipeline_output event dropped (peer {}, kind {}): {}",
                                target.dispatcher.peer_id,
                                kind,
                                e
                            );
                        }
                    }

                    target.dispatcher.dispatch(transport_data.clone()).await;
                }
            }

            debug!(
                "{} drainer ended for shared session {} (channel closed)",
                kind, session_id
            );
        });
    }
}

/// Server-side WebRTC peer with pipeline integration
///
/// Automatically created when a client announces via gRPC signaling.
/// Handles bidirectional media routing: client ↔ WebRTC ↔ pipeline ↔ WebRTC ↔ client
///
/// ## Multi-Track Support (Spec 013)
///
/// The ServerPeer now supports dynamic multi-track streaming:
/// - Uses `TrackRegistry` to manage multiple audio/video tracks per peer
/// - Routes frames based on `stream_id` field in RuntimeData
/// - Auto-creates tracks on first frame with new stream_id
/// - Maintains backward compatibility via DEFAULT_STREAM_ID fallback
pub struct ServerPeer {
    /// Unique identifier for the remote client
    peer_id: String,

    /// WebRTC peer connection
    peer_connection: Arc<PeerConnection>,

    /// Pipeline runner
    executor: Arc<dyn PipelineSessionHost>,

    /// Pipeline manifest
    manifest: Arc<Manifest>,

    /// Track registry for multi-track support (Spec 013)
    track_registry: Arc<TrackRegistry<AudioTrack, VideoTrack>>,

    /// Optional event sender for FFI integration
    #[cfg(feature = "ws-signaling")]
    event_tx: Option<mpsc::Sender<WebRtcEventBridge>>,

    /// Shutdown signal
    shutdown_tx: mpsc::Sender<()>,
    shutdown_rx: Arc<RwLock<Option<mpsc::Receiver<()>>>>,

    /// Pipeline session id for this peer. Populated by `handle_offer`.
    /// Transport-level control handlers read this to look up the
    /// per-session `SessionControl` on the executor's `SessionControlBus`.
    session_id: Arc<RwLock<Option<String>>>,

    /// Retained RTP senders for every track registered on this peer.
    /// Stored to keep `Arc<RTCRtpSender>` alive for the duration of the
    /// peer (avoids any reliance on webrtc-rs's internal retention).
    track_senders: Arc<RwLock<Vec<Arc<webrtc::rtp_transceiver::rtp_sender::RTCRtpSender>>>>,
}

impl ServerPeer {
    /// Create a new server peer without event forwarding
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Unique identifier for the remote client
    /// * `config` - WebRTC transport configuration (STUN/TURN servers, etc.)
    /// * `runner` - Pipeline runner for media processing
    /// * `manifest` - Pipeline manifest (defines processing graph)
    ///
    /// # Returns
    ///
    /// ServerPeer ready to accept offers and stream media through pipeline
    pub async fn new(
        peer_id: String,
        config: &WebRtcTransportConfig,
        executor: Arc<dyn PipelineSessionHost>,
        manifest: Arc<Manifest>,
    ) -> Result<Self> {
        info!("Creating server peer: {}", peer_id);

        // Create WebRTC peer connection
        let peer_connection = Arc::new(PeerConnection::new(peer_id.clone(), config).await?);

        // Create track registry for multi-track support (Spec 013)
        let track_registry = Arc::new(TrackRegistry::new(peer_id.clone()));

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        Ok(Self {
            peer_id,
            peer_connection,
            executor,
            manifest,
            track_registry,
            #[cfg(feature = "ws-signaling")]
            event_tx: None,
            shutdown_tx,
            shutdown_rx: Arc::new(RwLock::new(Some(shutdown_rx))),
            session_id: Arc::new(RwLock::new(None)),
            track_senders: Arc::new(RwLock::new(Vec::new())),
        })
    }

    /// Create a new server peer with optional event forwarding
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Unique identifier for the remote client
    /// * `config` - WebRTC transport configuration (STUN/TURN servers, etc.)
    /// * `runner` - Pipeline runner for media processing
    /// * `manifest` - Pipeline manifest (defines processing graph)
    /// * `event_tx` - Optional event sender for FFI integration (only with ws-signaling feature)
    ///
    /// # Returns
    ///
    /// ServerPeer ready to accept offers and stream media through pipeline
    #[cfg(feature = "ws-signaling")]
    pub async fn new_with_events(
        peer_id: String,
        config: &WebRtcTransportConfig,
        executor: Arc<dyn PipelineSessionHost>,
        manifest: Arc<Manifest>,
        event_tx: Option<mpsc::Sender<WebRtcEventBridge>>,
    ) -> Result<Self> {
        info!("Creating server peer: {}", peer_id);

        // Create WebRTC peer connection
        let peer_connection = Arc::new(PeerConnection::new(peer_id.clone(), config).await?);

        // Create track registry for multi-track support (Spec 013)
        let track_registry = Arc::new(TrackRegistry::new(peer_id.clone()));

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        Ok(Self {
            peer_id,
            peer_connection,
            executor,
            manifest,
            track_registry,
            event_tx,
            shutdown_tx,
            shutdown_rx: Arc::new(RwLock::new(Some(shutdown_rx))),
            session_id: Arc::new(RwLock::new(None)),
            track_senders: Arc::new(RwLock::new(Vec::new())),
        })
    }

    /// Session id assigned by `PipelineExecutor::create_session` when this
    /// peer handled its first offer. `None` before the offer is processed
    /// — callers racing the initial SDP exchange should retry.
    pub async fn session_id(&self) -> Option<String> {
        self.session_id.read().await.clone()
    }

    /// Drain every registered audio track's send ring buffer.
    ///
    /// Called from the control plane on barge-in so the user
    /// doesn't have to wait for ~10 s of already-queued TTS to
    /// play out before the assistant actually stops. Returns the
    /// total number of frames dropped across all tracks.
    pub async fn flush_audio_tracks(&self) -> usize {
        let mut dropped = 0;
        for stream_id in self.track_registry.audio_stream_ids().await {
            if let Some(track) = self.track_registry.get_audio_track(&stream_id).await {
                dropped += track.flush_send_buffer().await;
            }
        }
        dropped
    }

    /// Handle incoming SDP offer from client
    ///
    /// Processes the offer, creates a pipeline session, sets up media routing,
    /// and generates a real SDP answer.
    ///
    /// # Arguments
    ///
    /// * `offer_sdp` - SDP offer string from client
    ///
    /// # Returns
    ///
    /// SDP answer string to send back to client
    pub async fn handle_offer(&self, offer_sdp: String) -> Result<String> {
        info!("ServerPeer {} handling offer", self.peer_id);

        let key = format!("webrtc:{}", self.peer_id);
        let core_shared = self
            .executor
            .get_or_create_shared_session(key, Arc::clone(&self.manifest))
            .await
            .map_err(|e| {
                Error::InternalError(format!("Failed to create pipeline session: {}", e))
            })?;
        let control = self.executor.control_bus().get(core_shared.session_id());
        let shared_session = SharedPipelineSession::new(core_shared, control, true).await?;

        self.handle_offer_with_shared_session(offer_sdp, shared_session)
            .await
    }

    /// Handle incoming SDP offer using an existing shared pipeline session.
    pub async fn handle_offer_with_shared_session(
        &self,
        offer_sdp: String,
        shared_session: Arc<SharedPipelineSession>,
    ) -> Result<String> {
        info!(
            "ServerPeer {} handling offer on shared session {}",
            self.peer_id,
            shared_session.session_id()
        );

        // Surface the session id on this peer so transport-level control
        // plane handlers (control.* JSON-RPC) can look up the matching
        // SessionControl on the executor's SessionControlBus.
        *self.session_id.write().await = Some(shared_session.session_id().to_string());

        info!(
            "Using pipeline session {} for peer {}",
            shared_session.session_id(),
            self.peer_id
        );

        // Phase 5.1/5.2 (Option C): hand SessionControl to this peer's
        // TrackRegistry so registered tracks publish media_clock on the
        // shared pipeline session bus at `<medium>:<stream_id>`.
        if let Some(ctrl) = self.executor.control_bus().get(shared_session.session_id()) {
            self.track_registry
                .attach_session_control(Arc::clone(&ctrl))
                .await;
        }

        // Scan the manifest for outbound media streams. Pre-registering
        // tracks before set_remote_description guarantees the answer SDP
        // carries one m-line per track — no later renegotiation needed.
        // See spec 2026-04-29-webrtc-multi-track-video-design.
        let plan = {
            let registry_arc = self.executor.registry();
            let registry = registry_arc.read().await;
            crate::peer::scanner::scan(&self.manifest, &registry)
        };
        info!(
            "Media plan for peer {}: {} audio, {} video",
            self.peer_id,
            plan.audio_outputs.len(),
            plan.video_outputs.len()
        );

        // Back-compat: if no node declared an audio output (e.g. the
        // simpler LFM2 example whose audio node has no schema declaring
        // it produces Audio), still register a single default audio
        // track. The pipeline emits Audio with stream_id: None and
        // extract_stream_id then resolves to DEFAULT_STREAM_ID.
        let audio_specs = if plan.audio_outputs.is_empty() {
            vec![crate::peer::scanner::AudioStreamSpec::default_named(
                DEFAULT_STREAM_ID,
            )]
        } else {
            plan.audio_outputs
        };

        for spec in &audio_specs {
            // Opus on the wire is always 48 kHz mono — RTCRtpCodecCapability
            // hardcodes clock_rate=48000 in PeerConnection, and the SAME
            // AudioTrack instance is reused as the inbound Opus DECODER
            // (see `on_track` handler below), which also expects 48 kHz.
            // The pipeline does its own resampling on either side, so
            // `spec.sample_rate` is informational only — never propagate
            // it into the codec config.
            let cfg = crate::media::audio::AudioEncoderConfig {
                sample_rate: 48_000,
                channels: 1,
                bitrate: 64_000,
                complexity: 10,
                ..Default::default()
            };

            // Single-track default path uses the legacy add_audio_track
            // (which retains the sender internally on PeerConnection),
            // matching prior behaviour for the LFM2 example.
            if audio_specs.len() == 1 && spec.stream_id == DEFAULT_STREAM_ID {
                self.peer_connection
                    .add_audio_track(cfg)
                    .await
                    .map_err(|e| Error::InternalError(format!("add_audio_track: {}", e)))?;
                let track = self.peer_connection.audio_track().await.ok_or_else(|| {
                    Error::InternalError("audio_track missing after add_audio_track".into())
                })?;
                self.track_registry
                    .register_audio_track(&spec.stream_id, track)
                    .await
                    .map_err(|e| Error::InternalError(format!("register_audio_track: {}", e)))?;
            } else {
                let (track, sender) = self
                    .peer_connection
                    .add_audio_track_with_stream_id(&spec.stream_id, cfg)
                    .await
                    .map_err(|e| {
                        Error::InternalError(format!(
                            "add_audio_track_with_stream_id({}): {}",
                            spec.stream_id, e
                        ))
                    })?;
                self.track_senders.write().await.push(sender);
                self.track_registry
                    .register_audio_track(&spec.stream_id, track)
                    .await
                    .map_err(|e| Error::InternalError(format!("register_audio_track: {}", e)))?;
            }
            info!(
                "Registered audio track '{}' for peer {} ({}Hz, {}ch)",
                spec.stream_id, self.peer_id, spec.sample_rate, spec.channels
            );
        }

        for spec in &plan.video_outputs {
            let cfg = crate::media::video::VideoEncoderConfig {
                width: spec.width,
                height: spec.height,
                framerate: spec.framerate,
                bitrate: 2_000_000,
                keyframe_interval: 60,
            };
            let (track, sender) = self
                .peer_connection
                .add_video_track_with_stream_id(&spec.stream_id, cfg)
                .await
                .map_err(|e| {
                    Error::InternalError(format!(
                        "add_video_track_with_stream_id({}): {}",
                        spec.stream_id, e
                    ))
                })?;
            self.track_senders.write().await.push(sender);
            self.track_registry
                .register_video_track(&spec.stream_id, track)
                .await
                .map_err(|e| Error::InternalError(format!("register_video_track: {}", e)))?;
            info!(
                "Registered video track '{}' for peer {} ({}x{} @ {}fps)",
                spec.stream_id, self.peer_id, spec.width, spec.height, spec.framerate
            );
        }

        // Set up bidirectional media routing and data channel (this will set up the data channel handler)
        self.setup_media_routing_and_data_channel(shared_session)
            .await?;

        // Now set remote description (offer) - data channel handler is already registered
        let offer = RTCSessionDescription::offer(offer_sdp)
            .map_err(|e| Error::WebRtcError(format!("Invalid offer SDP: {}", e)))?;

        self.peer_connection
            .peer_connection()
            .set_remote_description(offer)
            .await
            .map_err(|e| Error::WebRtcError(format!("Failed to set remote description: {}", e)))?;

        // Create answer
        let answer = self
            .peer_connection
            .peer_connection()
            .create_answer(None)
            .await
            .map_err(|e| Error::WebRtcError(format!("Failed to create answer: {}", e)))?;

        // Set local description (answer)
        self.peer_connection
            .peer_connection()
            .set_local_description(answer.clone())
            .await
            .map_err(|e| Error::WebRtcError(format!("Failed to set local description: {}", e)))?;

        info!("Generated SDP answer for peer {}", self.peer_id);

        Ok(answer.sdp)
    }

    /// Set up bidirectional media routing and data channel
    ///
    /// - Incoming: WebRTC tracks + data channel → RuntimeData → pipeline input
    /// - Outgoing: pipeline output → RuntimeData → WebRTC tracks + data channel
    async fn setup_media_routing_and_data_channel(
        &self,
        shared_session: Arc<SharedPipelineSession>,
    ) -> Result<()> {
        info!(
            "Setting up media routing and data channel for peer {}",
            self.peer_id
        );

        // Create channel for data channel messages to pipeline
        let (dc_input_tx, mut dc_input_rx) = mpsc::channel::<TransportData>(32);

        // Clone dc_input_tx before moving into closures
        let dc_input_tx_for_dc = dc_input_tx.clone();
        let dc_input_tx_for_track = dc_input_tx.clone();

        // Clone event_tx for closures (FFI event forwarding)
        #[cfg(feature = "ws-signaling")]
        let event_tx_for_dc = self.event_tx.clone();
        #[cfg(feature = "ws-signaling")]
        let event_tx_for_output = self.event_tx.clone();

        // Create shared data channel reference for output routing
        let data_channel_ref: Arc<RwLock<Option<Arc<RTCDataChannel>>>> =
            Arc::new(RwLock::new(None));
        let data_channel_ref_for_dc = Arc::clone(&data_channel_ref);

        // Set up data channel handler
        let peer_id_for_dc = self.peer_id.clone();
        #[cfg(feature = "ws-signaling")]
        let event_tx_for_dc_clone = event_tx_for_dc.clone();
        // Control-bus handler is feature-gated on `grpc-signaling` because the
        // prost-generated ControlFrame/ControlEvent types live under that feature.
        #[cfg(feature = "grpc-signaling")]
        let control_bus_for_dc = self.executor.control_bus();
        self.peer_connection
            .peer_connection()
            .on_data_channel(Box::new(move |data_channel| {
                let peer_id = peer_id_for_dc.clone();
                let dc_input_tx = dc_input_tx_for_dc.clone();
                let data_channel_ref = Arc::clone(&data_channel_ref_for_dc);
                #[cfg(feature = "ws-signaling")]
                let event_tx = event_tx_for_dc_clone.clone();
                #[cfg(feature = "grpc-signaling")]
                let control_bus = Arc::clone(&control_bus_for_dc);
                let data_channel = Arc::new(data_channel);

                Box::pin(async move {
                    info!("Data channel opened: label={}, id={:?} for peer {}",
                        data_channel.label(), data_channel.id(), peer_id);

                    // Route the "remotemedia-control" data channel to the
                    // Session Control Bus instead of the data plane.
                    #[cfg(feature = "grpc-signaling")]
                    if data_channel.label() == crate::control::CONTROL_CHANNEL_LABEL {
                        info!("Data channel '{}' routed to Session Control Bus (peer {})",
                            data_channel.label(), peer_id);
                        crate::control::attach_control_channel(
                            Arc::clone(&data_channel),
                            control_bus,
                        )
                        .await;
                        return;
                    }

                    // Store data channel reference for output routing
                    {
                        let mut dc_ref = data_channel_ref.write().await;
                        *dc_ref = Some(Arc::clone(&data_channel));
                        info!("Stored data channel reference for output routing (peer {})", peer_id);
                    }

                    // Clone data_channel for the message handler
                    let dc_for_handler = Arc::clone(&data_channel);

                    // Reassembler for data channel chunks
                    let reassembler = Arc::new(tokio::sync::Mutex::new(crate::channels::DataChannelReassembler::new()));

                    // Set up message handler - expects DataChannelMessage protobuf envelope
                    #[cfg(feature = "ws-signaling")]
                    let event_tx_for_msg = event_tx.clone();
                    dc_for_handler.on_message(Box::new(move |msg| {
                        let peer_id = peer_id.clone();
                        let dc_input_tx = dc_input_tx.clone();
                        #[cfg(feature = "ws-signaling")]
                        let event_tx = event_tx_for_msg.clone();
                        let reassembler = Arc::clone(&reassembler);

                        Box::pin(async move {
                            info!("Received data channel message: {} bytes from peer {}", msg.data.len(), peer_id);

                            // Emit raw data received event for FFI integration
                            #[cfg(feature = "ws-signaling")]
                            if let Some(ref tx) = event_tx {
                                let event = WebRtcEventBridge::data_received(
                                    peer_id.clone(),
                                    msg.data.to_vec(),
                                    current_timestamp_ns(),
                                );
                                if let Err(e) = tx.send(event).await {
                                    warn!("Failed to emit data_received event: {}", e);
                                }
                            }

                            // Try to decode message as our binary DataChannelMessage envelope
                            use crate::channels::DataChannelMessage;
                            let parsed_msg = match DataChannelMessage::decode(&msg.data[..]) {
                                Ok(m) => Some(m),
                                Err(e) => {
                                    // Fallback to legacy raw DataBuffer decode for old clients
                                    match crate::generated::DataBuffer::decode(&msg.data[..]) {
                                        Ok(_) => {
                                            info!("Decoded legacy raw DataBuffer from data channel (peer {})", peer_id);
                                            Some(DataChannelMessage::runtime_data(msg.data.to_vec()))
                                        }
                                        Err(_) => {
                                            error!("Failed to decode DataChannelMessage envelope or raw DataBuffer: {}", e);
                                            None
                                        }
                                    }
                                }
                            };

                            if let Some(parsed) = parsed_msg {
                                match parsed {
                                    DataChannelMessage::RuntimeData { data_buffer, .. } => {
                                        // Deserialize Protobuf DataBuffer
                                        match crate::generated::DataBuffer::decode(&data_buffer[..]) {
                                            Ok(data_buffer) => {
                                                // Convert Protobuf DataBuffer → RuntimeData
                                                if let Some(runtime_data) = crate::adapters::data_buffer_to_runtime_data(&data_buffer) {
                                                    info!("Decoded RuntimeData from data channel: type={}", runtime_data.data_type());

                                                    let transport_data = remotemedia_core::transport::TransportData {
                                                        data: runtime_data,
                                                        sequence: None,
                                                        metadata: participant_metadata(
                                                            &peer_id,
                                                            format!("{}:data", peer_id),
                                                            participant::modality::CONTROL,
                                                        ),
                                                    };

                                                    if let Err(e) = dc_input_tx.send(transport_data).await {
                                                        error!("Failed to forward data channel message to pipeline: {}", e);
                                                    }
                                                } else {
                                                    error!("Failed to convert DataBuffer to RuntimeData: invalid data type");
                                                }
                                            }
                                            Err(e) => {
                                                error!("Failed to decode Protobuf DataBuffer from RuntimeData payload: {}", e);
                                            }
                                        }
                                    }
                                    DataChannelMessage::Chunk { chunk, .. } => {
                                        let mut r = reassembler.lock().await;
                                        if let Some(reassembled_bytes) = r.feed_chunk(&chunk) {
                                            // Deserialize Protobuf DataBuffer from reassembled chunks
                                            match crate::generated::DataBuffer::decode(&reassembled_bytes[..]) {
                                                Ok(data_buffer) => {
                                                    if let Some(runtime_data) = crate::adapters::data_buffer_to_runtime_data(&data_buffer) {
                                                        info!("Reassembled and decoded RuntimeData from chunks: type={}", runtime_data.data_type());

                                                        let transport_data = remotemedia_core::transport::TransportData {
                                                            data: runtime_data,
                                                            sequence: None,
                                                            metadata: participant_metadata(
                                                                &peer_id,
                                                                format!("{}:data", peer_id),
                                                                participant::modality::CONTROL,
                                                            ),
                                                        };

                                                        if let Err(e) = dc_input_tx.send(transport_data).await {
                                                            error!("Failed to forward reassembled data to pipeline: {}", e);
                                                        }
                                                    } else {
                                                        error!("Failed to convert reassembled DataBuffer to RuntimeData: invalid data type");
                                                    }
                                                }
                                                Err(e) => {
                                                    error!("Failed to decode Protobuf DataBuffer from reassembled payload: {}", e);
                                                }
                                            }
                                        }
                                    }
                                    DataChannelMessage::Control { action, json, .. } => {
                                        info!("Received control message on data-plane channel: action={}, json={}", action, json);
                                    }
                                    DataChannelMessage::Text { action, text, .. } => {
                                        info!("Received text message on data-plane channel: action={}, text={}", action, text);
                                    }
                                    DataChannelMessage::Binary { action, data, .. } => {
                                        info!("Received binary message on data-plane channel: action={}, bytes={}", action, data.len());
                                    }
                                }
                            }
                        })
                    }));
                })
            }));

        // Set up incoming track handlers (audio from client microphone)
        let peer_id_for_track = self.peer_id.clone();
        let peer_connection_for_track = Arc::clone(&self.peer_connection);

        self.peer_connection.on_track(move |track, _receiver, _transceiver| {
                let peer_id = peer_id_for_track.clone();
                let dc_input_tx = dc_input_tx_for_track.clone();
                let peer_connection = Arc::clone(&peer_connection_for_track);

                Box::pin(async move {
                    info!("Remote track added for peer {}: kind={}", peer_id, track.kind());

                    // Only process audio tracks
                    if track.kind() != webrtc::rtp_transceiver::rtp_codec::RTPCodecType::Audio {
                        info!("Ignoring non-audio track for peer {}", peer_id);
                        return;
                    }

                    info!("Starting audio reception task for peer {}", peer_id);

                    // Spawn task to continuously read RTP packets and decode audio
                    tokio::spawn(async move {
                        // Get the audio track decoder
                        let audio_track = match peer_connection.audio_track().await {
                            Some(track) => track,
                            None => {
                                error!("No audio track available for decoding for peer {}", peer_id);
                                return;
                            }
                        };

                        // Track consecutive read errors so a genuinely
                        // closed track terminates the loop eventually,
                        // but one transient RTP hiccup (e.g. a stray
                        // RTCP BYE while the remote reassigns SSRCs
                        // during a long outbound TTS burst) doesn't
                        // kill mic reception for the rest of the
                        // session. Before this, a single read_rtp Err
                        // broke the loop silently at `debug!` level,
                        // stalling the whole pipeline after turn 1.
                        let mut consecutive_errors: u32 = 0;
                        const MAX_CONSECUTIVE_ERRORS: u32 = 50;

                        loop {
                            // Read RTP packet
                            let (rtp_packet, _) = match track.read_rtp().await {
                                Ok(packet) => {
                                    consecutive_errors = 0;
                                    packet
                                }
                                Err(e) => {
                                    consecutive_errors += 1;
                                    // Surface this at warn level so
                                    // operators can see when the
                                    // reception stream is degrading.
                                    // Only break once we've hit a run
                                    // of errors that looks like an
                                    // actual close.
                                    warn!(
                                        "RTP read error for peer {} ({}/{}): {}",
                                        peer_id,
                                        consecutive_errors,
                                        MAX_CONSECUTIVE_ERRORS,
                                        e,
                                    );
                                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                                        warn!(
                                            "RTP reception for peer {} ending after {} consecutive errors",
                                            peer_id, consecutive_errors,
                                        );
                                        break;
                                    }
                                    // Brief backoff so we don't
                                    // tight-loop on a persistent error.
                                    tokio::time::sleep(
                                        tokio::time::Duration::from_millis(20),
                                    )
                                    .await;
                                    continue;
                                }
                            };

                            // Decode Opus payload to audio samples
                            match audio_track.on_rtp_packet(&rtp_packet.payload).await {
                                Ok(samples) => {
                                    // Diagnostic: every ~50 packets (~1s at 50Hz),
                                    // log RMS + peak so we can confirm whether mic
                                    // RTP is carrying real audio or silence. RTP
                                    // payload size + decoded length are also useful
                                    // — short payloads (<10 B) are likely Opus DTX
                                    // (comfort-noise) frames, which the encoder
                                    // emits when it thinks the mic is silent.
                                    use std::sync::atomic::{AtomicU64, Ordering};
                                    static LOG_COUNTER: AtomicU64 = AtomicU64::new(0);
                                    let n = LOG_COUNTER.fetch_add(1, Ordering::Relaxed);
                                    if n % 50 == 0 {
                                        let len = samples.len();
                                        let mut peak: f32 = 0.0;
                                        let mut sumsq: f64 = 0.0;
                                        for s in &samples {
                                            let a = s.abs();
                                            if a > peak { peak = a; }
                                            sumsq += (*s as f64) * (*s as f64);
                                        }
                                        let rms = if len > 0 {
                                            (sumsq / len as f64).sqrt()
                                        } else { 0.0 };
                                        debug!(
                                            "[mic-decode] peer={} rtp_payload_bytes={} decoded_samples={} peak={:.4e} rms={:.4e}",
                                            peer_id, rtp_packet.payload.len(), len, peak, rms
                                        );
                                    } else {
                                        trace!("Decoded {} audio samples from peer {}", samples.len(), peer_id);
                                    }

                                    // Send decoded audio to pipeline
                                    let transport_data = TransportData {
                                        data: RuntimeData::Audio {
                                            samples: samples.into(),
                                            sample_rate: 48000, // Opus always decodes to 48kHz
                                            channels: 1,
                                            stream_id: Some(format!("{}:audio", peer_id)),
                                            timestamp_us: Some(0),
                                            arrival_ts_us: None,
                                            metadata: None,
                                        },
                                        sequence: None,
                                        metadata: participant_metadata(
                                            &peer_id,
                                            format!("{}:audio", peer_id),
                                            participant::modality::AUDIO,
                                        ),
                                    };

                                    if let Err(e) = dc_input_tx.send(transport_data).await {
                                        debug!("Audio reception ended for peer {}: {}", peer_id, e);
                                        break;
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to decode audio packet for peer {}: {}", peer_id, e);
                                    // Continue processing next packets
                                }
                            }
                        }

                        warn!("Audio reception task ended for peer {}", peer_id);
                    });
                })
            }).await;

        // Spawn task to handle bidirectional routing
        let peer_id = self.peer_id.clone();
        let _peer_connection = Arc::clone(&self.peer_connection);
        let track_registry = Arc::clone(&self.track_registry);
        let data_channel_for_output = Arc::clone(&data_channel_ref);
        #[cfg(feature = "ws-signaling")]
        let event_tx_for_output = event_tx_for_output;
        let mut shutdown_rx =
            self.shutdown_rx.write().await.take().ok_or_else(|| {
                Error::InternalError("Shutdown receiver already taken".to_string())
            })?;

        // Split input forwarding and output draining into independent
        // tasks. Before this, a single `select! { biased; .. }` coupled
        // them: if the router's input channel was full (bursty mic
        // during a slow model response), the select! blocked inside
        // the input arm, which prevented the output arm from draining
        // the router's output channel, which in turn prevented the
        // router from making progress on the next input — classic
        // bounded-channel ring deadlock. Separate tasks = each side
        // has its own backpressure path.

        // Input forwarder: dc_input_rx -> shared session input.
        let participant = Participant::new(peer_id.clone(), participant::role::CLIENT)
            .with_track_id(format!("{peer_id}:webrtc"))
            .with_modality(participant::modality::CONTROL);
        let participant_session = shared_session.participant_handle(participant);
        let peer_id_for_input = peer_id.clone();
        tokio::spawn(async move {
            while let Some(transport_data) = dc_input_rx.recv().await {
                trace!("Forwarding data for peer {} to session", peer_id_for_input);
                if let Err(e) = participant_session.send(transport_data).await {
                    debug!(
                        "Input forwarder ended for peer {}: {}",
                        peer_id_for_input, e
                    );
                    break;
                }
            }
            debug!("Input forwarder task ended for peer {}", peer_id_for_input);
        });

        shared_session
            .attach_peer(
                peer_id.clone(),
                track_registry,
                data_channel_for_output,
                #[cfg(feature = "ws-signaling")]
                event_tx_for_output,
            )
            .await;

        // Shutdown handler: when the peer disconnects, detach only this peer
        // from the shared session. The shared session owner continues running
        // while other peers remain connected.
        let shared_session_for_shutdown = Arc::clone(&shared_session);
        tokio::spawn(async move {
            let _ = shutdown_rx.recv().await;
            info!("Shutting down media routing for peer {}", peer_id);
            shared_session_for_shutdown.detach_peer(&peer_id).await;
            info!("Media routing task ended for peer {}", peer_id);
        });

        Ok(())
    }

    /// Send TransportData to WebRTC peer connection using multi-track routing (Spec 013)
    ///
    /// Routes RuntimeData to appropriate tracks based on stream_id field.
    /// Falls back to DEFAULT_STREAM_ID for backward compatibility.
    /// Json and Text data are sent through the data channel.
    ///
    /// # Arguments
    ///
    /// * `track_registry` - Registry of audio/video tracks keyed by stream_id
    /// * `data_channel` - Optional data channel for Json/Text output
    /// * `transport_data` - Data to send, containing RuntimeData with optional stream_id
    async fn send_to_webrtc_multitrack(
        track_registry: &Arc<TrackRegistry<AudioTrack, VideoTrack>>,
        data_channel: &Arc<RwLock<Option<Arc<RTCDataChannel>>>>,
        transport_data: TransportData,
    ) -> Result<()> {
        // Get RuntimeData and extract stream_id
        let runtime_data = transport_data.data;
        let stream_id = extract_stream_id(&runtime_data).unwrap_or(DEFAULT_STREAM_ID);

        match &runtime_data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                ..
            } => {
                trace!(
                    "Sending audio to stream '{}': {} samples, {}Hz, {} channels",
                    stream_id,
                    samples.len(),
                    sample_rate,
                    channels
                );

                // Get the audio track from registry
                if let Some(audio_track) = track_registry.get_audio_track(stream_id).await {
                    // Send audio samples through the track with dynamic sample rate.
                    // `send_audio` takes `Arc<Vec<f32>>`; copy via `to_vec`
                    // regardless of `AudioSamples` variant (Vec path is a
                    // full clone, same as before this refactor).
                    audio_track
                        .send_audio(Arc::new(samples.to_vec()), *sample_rate)
                        .await?;

                    // Record frame for activity tracking
                    track_registry.record_audio_frame(stream_id).await;
                    trace!("Audio sent successfully to stream '{}'", stream_id);
                } else {
                    warn!(
                        "No audio track for stream_id '{}', cannot send audio (registered tracks: {:?})",
                        stream_id,
                        track_registry.audio_stream_ids().await
                    );
                }
            }
            RuntimeData::Video { .. } => {
                trace!("Sending video frame to stream '{}' via WebRTC", stream_id);

                // Get the video track from registry
                if let Some(video_track) = track_registry.get_video_track(stream_id).await {
                    // Send video using VideoTrack's send_video_runtime_data method
                    video_track
                        .send_video_runtime_data(runtime_data.clone())
                        .await?;

                    // Record frame for activity tracking
                    track_registry.record_video_frame(stream_id).await;
                    trace!("Video sent successfully to stream '{}'", stream_id);
                } else {
                    warn!(
                        "No video track for stream_id '{}', cannot send video (registered tracks: {:?})",
                        stream_id,
                        track_registry.video_stream_ids().await
                    );
                }
            }
            RuntimeData::Json(_) | RuntimeData::Text(_) => {
                // Send Json/Text data through data channel
                let dc_guard = data_channel.read().await;
                if let Some(dc) = dc_guard.as_ref() {
                    // Convert RuntimeData to Protobuf DataBuffer
                    let data_buffer = crate::adapters::runtime_data_to_data_buffer(&runtime_data);
                    let encoded = data_buffer.encode_to_vec();

                    debug!(
                        "Sending {} data ({} bytes) through data channel",
                        runtime_data.data_type(),
                        encoded.len()
                    );

                    // Send through data channel
                    if let Err(e) = dc.send(&bytes::Bytes::from(encoded)).await {
                        error!("Failed to send data through data channel: {}", e);
                        return Err(Error::WebRtcError(format!(
                            "Data channel send failed: {}",
                            e
                        )));
                    }

                    debug!(
                        "Successfully sent {} data through data channel",
                        runtime_data.data_type()
                    );
                } else {
                    trace!(
                        "No data channel available to send {} output",
                        runtime_data.data_type()
                    );
                }
            }
            _ => {
                debug!(
                    "Unsupported RuntimeData type for WebRTC output: {}",
                    runtime_data.data_type()
                );
            }
        }

        Ok(())
    }

    /// Handle incoming ICE candidate from client
    pub async fn handle_ice_candidate(
        &self,
        candidate: String,
        sdp_mid: Option<String>,
        sdp_mline_index: Option<u16>,
    ) -> Result<()> {
        use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;

        info!("ServerPeer {} adding ICE candidate", self.peer_id);

        debug!(
            "ICE candidate: candidate={}, sdp_mid={:?}, sdp_mline_index={:?}",
            candidate, sdp_mid, sdp_mline_index
        );

        // Create ICE candidate init
        let ice_candidate_init = RTCIceCandidateInit {
            candidate: candidate.clone(),
            sdp_mid,
            sdp_mline_index,
            username_fragment: None,
        };

        // Add ICE candidate to peer connection
        self.peer_connection
            .peer_connection()
            .add_ice_candidate(ice_candidate_init)
            .await
            .map_err(|e| Error::WebRtcError(format!("Failed to add ICE candidate: {}", e)))?;

        info!("ICE candidate added successfully for peer {}", self.peer_id);

        Ok(())
    }

    /// Get the peer ID
    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    /// Get the underlying peer connection
    pub fn peer_connection(&self) -> &Arc<PeerConnection> {
        &self.peer_connection
    }

    /// Get the track registry for multi-track management (Spec 013)
    ///
    /// The registry allows external code to:
    /// - Query registered tracks by stream_id
    /// - Register new tracks dynamically
    /// - Monitor track activity
    pub fn track_registry(&self) -> &Arc<TrackRegistry<AudioTrack, VideoTrack>> {
        &self.track_registry
    }

    /// Get the number of registered audio tracks
    pub async fn audio_track_count(&self) -> usize {
        self.track_registry.audio_track_count().await
    }

    /// Get the number of registered video tracks
    pub async fn video_track_count(&self) -> usize {
        self.track_registry.video_track_count().await
    }

    /// Get all registered audio stream IDs
    pub async fn audio_stream_ids(&self) -> Vec<String> {
        self.track_registry.audio_stream_ids().await
    }

    /// Get all registered video stream IDs
    pub async fn video_stream_ids(&self) -> Vec<String> {
        self.track_registry.video_stream_ids().await
    }

    /// Shutdown the server peer
    ///
    /// Closes the pipeline session and WebRTC connection
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down server peer: {}", self.peer_id);

        // Signal shutdown to media routing task
        let _ = self.shutdown_tx.send(()).await;

        // Note: Pipeline session is owned by the media routing task
        // and will be cleaned up when that task ends

        // Close WebRTC connection
        if let Err(e) = self.peer_connection.close().await {
            warn!("Error closing peer connection for {}: {}", self.peer_id, e);
        }

        info!("Server peer {} shut down", self.peer_id);

        Ok(())
    }
}

impl Drop for ServerPeer {
    fn drop(&mut self) {
        debug!("ServerPeer {} dropped", self.peer_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_server_peer_creation() {
        // This is a placeholder test
        // Real tests would require mock WebRTC and pipeline components
        assert!(true);
    }
}
