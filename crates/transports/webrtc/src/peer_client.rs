//! A WebRTC member for a RemoteMedia signalling server, in Rust.
//!
//! [`PeerClient`] does what the browser client does: it announces over the
//! WebSocket JSON-RPC signalling, reads the media plan, offers one section
//! per planned output plus the pipeline data channel, applies the answer and
//! the trickled candidates, and then exchanges media and data with the
//! server peer. It exists so a pipeline served over WebRTC can be exercised
//! without a browser: tests and the `remotemedia test` command drive it.
//!
//! Received media is decoded far enough to be checked: audio is Opus-decoded
//! to PCM, video is reassembled into frames whose keyframes are parsed for
//! their dimensions (H.264 SPS or VP8 frame header). Nothing is rendered.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use remotemedia_core::data::RuntimeData;
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::tungstenite::Message;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::setting_engine::SettingEngine;
use webrtc::api::APIBuilder;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtp::packetizer::Depacketizer;
use webrtc::rtp_transceiver::rtp_codec::{RTCRtpCodecCapability, RTPCodecType};
use webrtc::rtp_transceiver::rtp_transceiver_direction::RTCRtpTransceiverDirection;
use webrtc::rtp_transceiver::RTCRtpTransceiverInit;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;
use webrtc::track::track_local::TrackLocal;
use webrtc::track::track_remote::TrackRemote;

use crate::generated::DataBuffer;
use crate::media::audio::{AudioDecoder, AudioEncoder, AudioEncoderConfig};
use crate::{Error, Result};

/// The peer id the server answers as.
pub const SERVER_PEER_ID: &str = "remotemedia-server";
/// Label of the data channel that carries pipeline inputs and outputs.
pub const PIPELINE_CHANNEL_LABEL: &str = "pipeline";
/// Sample rate of the audio this client sends and receives.
pub const AUDIO_SAMPLE_RATE: u32 = 48_000;
const OPUS_FRAME_SAMPLES: usize = 960; // 20 ms at 48 kHz

/// Which of the pipeline's video outputs this member asks for.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ReceiveVideo {
    /// Every video output in the plan (no `receive` in the offer).
    #[default]
    Planned,
    /// None: no video sections, `receive: { video: false }`.
    None,
    /// These stream ids: one section each, `receive: { video: [ids] }`.
    Streams(Vec<String>),
    /// A client from before the media plan: ignores the plan, offers no
    /// video section and sends no `receive`.
    Unaware,
}

/// How a [`PeerClient`] joins.
#[derive(Debug, Clone)]
pub struct PeerClientOptions {
    /// Member id announced to the server.
    pub peer_id: String,
    /// Capabilities announced (informational on the server).
    pub capabilities: Vec<String>,
    /// Admission ticket, for a server that requires one.
    pub ticket: Option<String>,
    /// Add a microphone-like audio track, so [`PeerClient::send_audio`] works.
    pub send_audio: bool,
    /// Offer an audio section to receive the pipeline's audio.
    pub receive_audio: bool,
    /// Which video outputs to receive.
    pub receive_video: ReceiveVideo,
    /// Open the pipeline data channel.
    pub data_channel: bool,
    /// Bound on every signalling reply and on the connection coming up.
    pub connect_timeout: Duration,
}

impl Default for PeerClientOptions {
    fn default() -> Self {
        Self {
            peer_id: format!("rm-test-{}", uuid::Uuid::new_v4().simple()),
            capabilities: vec!["audio".into(), "video".into(), "data".into()],
            ticket: None,
            send_audio: true,
            receive_audio: true,
            receive_video: ReceiveVideo::Planned,
            data_channel: true,
            connect_timeout: Duration::from_secs(20),
        }
    }
}

/// One answered media section: which planned stream a `mid` carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnsweredSection {
    /// The section's `mid`.
    pub mid: String,
    /// `"audio"` or `"video"`.
    pub kind: String,
    /// The pipeline stream id.
    pub stream: String,
}

/// What a [`PeerClient`] observes after joining.
#[derive(Debug, Clone)]
pub enum PeerEvent {
    /// The peer connection changed state (`connected`, `failed`, ...).
    State(String),
    /// The pipeline data channel opened.
    DataChannelOpen,
    /// A pipeline output on the data channel.
    Data {
        /// The decoded value.
        data: RuntimeData,
        /// The `DataBuffer` metadata (participant attribution and so on).
        metadata: HashMap<String, String>,
    },
    /// A data-channel message that is not a `DataBuffer`.
    UndecodableData {
        /// Size of the message.
        bytes: usize,
    },
    /// A remote track started.
    Track {
        /// The section's `mid`.
        mid: Option<String>,
        /// `"audio"` or `"video"`.
        kind: String,
        /// The pipeline stream id the answer gave this `mid`, when it did.
        stream: Option<String>,
        /// Codec mime type, for example `audio/opus`.
        mime: String,
    },
    /// Decoded audio from a remote track (mono, [`AUDIO_SAMPLE_RATE`]).
    Audio {
        /// The section's `mid`.
        mid: Option<String>,
        /// The pipeline stream id.
        stream: Option<String>,
        /// Decoded samples.
        samples: Vec<f32>,
        /// RTP timestamp of the packet.
        rtp_timestamp: u32,
    },
    /// An audio packet that did not decode.
    AudioDecodeError {
        /// The section's `mid`.
        mid: Option<String>,
        /// The decoder's message.
        message: String,
    },
    /// One reassembled video frame from a remote track.
    VideoFrame {
        /// The section's `mid`.
        mid: Option<String>,
        /// The pipeline stream id.
        stream: Option<String>,
        /// Encoded size of the frame.
        bytes: usize,
        /// Whether the frame is a keyframe.
        keyframe: bool,
        /// Width and height, on frames that carry them (keyframes).
        dimensions: Option<(u32, u32)>,
        /// RTP timestamp of the frame.
        rtp_timestamp: u32,
    },
    /// The signalling socket closed.
    SignalingClosed,
}

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Value>>>>;

/// A joined member. Dropping it does not hang up; call [`PeerClient::close`].
/// Sending takes `&self`, so a client can be shared behind an `Arc`.
pub struct PeerClient {
    peer_id: String,
    rtc: Arc<RTCPeerConnection>,
    channel: Option<Arc<RTCDataChannel>>,
    audio_out: Option<AudioOut>,
    events: Option<mpsc::UnboundedReceiver<PeerEvent>>,
    announce: Value,
    answered: Vec<AnsweredSection>,
    signaling: SignalingHandle,
    channel_open: Arc<AtomicBool>,
}

struct AudioOut {
    track: Arc<TrackLocalStaticSample>,
    encoder: Mutex<AudioEncoder>,
}

#[derive(Clone)]
struct SignalingHandle {
    out: mpsc::UnboundedSender<Message>,
    pending: Pending,
    next_id: Arc<std::sync::atomic::AtomicU64>,
    timeout: Duration,
}

impl SignalingHandle {
    /// One JSON-RPC request; the `result`, or the server's error.
    async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(id, tx);
        let frame = json!({"jsonrpc": "2.0", "method": method, "params": params, "id": id});
        self.out
            .send(Message::Text(frame.to_string()))
            .map_err(|_| Error::SignalingError("signalling socket closed".into()))?;
        let reply = tokio::time::timeout(self.timeout, rx)
            .await
            .map_err(|_| {
                Error::SignalingError(format!("no reply to {method} within {:?}", self.timeout))
            })?
            .map_err(|_| Error::SignalingError(format!("socket closed awaiting {method}")))?;
        if let Some(error) = reply.get("error") {
            return Err(Error::SignalingError(format!("{method} refused: {error}")));
        }
        Ok(reply.get("result").cloned().unwrap_or(Value::Null))
    }
}

impl PeerClient {
    /// Join the server at `url` (`ws://host:port/ws`, optionally `?room=`).
    pub async fn connect(url: &str, options: PeerClientOptions) -> Result<Self> {
        let (socket, _) = tokio::time::timeout(
            options.connect_timeout,
            tokio_tungstenite::connect_async(url),
        )
        .await
        .map_err(|_| Error::SignalingError(format!("connecting to {url} timed out")))?
        .map_err(|e| Error::SignalingError(format!("connecting to {url}: {e}")))?;
        let (mut sink, mut stream) = socket.split();
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Message>();
        tokio::spawn(async move {
            while let Some(message) = out_rx.recv().await {
                if sink.send(message).await.is_err() {
                    break;
                }
            }
            let _ = sink.close().await;
        });
        let pending: Pending = Arc::default();
        let (events_tx, events) = mpsc::unbounded_channel();
        let (candidates_tx, mut candidates_rx) = mpsc::unbounded_channel::<RTCIceCandidateInit>();
        {
            let pending = Arc::clone(&pending);
            let events_tx = events_tx.clone();
            tokio::spawn(async move {
                while let Some(Ok(message)) = stream.next().await {
                    let Message::Text(text) = message else { continue };
                    let Ok(value) = serde_json::from_str::<Value>(&text) else { continue };
                    if let Some(id) = value.get("id").and_then(Value::as_u64) {
                        if let Some(waiter) = pending.lock().await.remove(&id) {
                            let _ = waiter.send(value);
                        }
                        continue;
                    }
                    if value["method"] == "peer.ice_candidate" {
                        let params = &value["params"];
                        let candidate = params["candidate"].as_str().unwrap_or_default();
                        if !candidate.is_empty() {
                            let _ = candidates_tx.send(RTCIceCandidateInit {
                                candidate: candidate.to_string(),
                                sdp_mid: params["sdp_mid"].as_str().map(str::to_string),
                                sdp_mline_index: params["sdp_m_line_index"]
                                    .as_u64()
                                    .map(|i| i as u16),
                                username_fragment: None,
                            });
                        }
                    }
                }
                pending.lock().await.clear();
                let _ = events_tx.send(PeerEvent::SignalingClosed);
            });
        }
        let signaling = SignalingHandle {
            out: out_tx,
            pending,
            next_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            timeout: options.connect_timeout,
        };

        let mut announce_params = json!({
            "peer_id": options.peer_id,
            "capabilities": options.capabilities,
        });
        if let Some(ticket) = &options.ticket {
            announce_params["ticket"] = json!(ticket);
        }
        let announce = signaling.request("peer.announce", announce_params).await?;

        // Video sections to offer, and the `receive` the offer carries.
        let planned_video = announce["media"]["video"]
            .as_array()
            .map(Vec::len)
            .unwrap_or(0);
        let (video_sections, receive) = match &options.receive_video {
            ReceiveVideo::Planned => (planned_video, None),
            ReceiveVideo::None => (0, Some(json!({ "video": false }))),
            ReceiveVideo::Streams(ids) => (ids.len(), Some(json!({ "video": ids }))),
            ReceiveVideo::Unaware => (0, None),
        };

        let mut media = MediaEngine::default();
        media
            .register_default_codecs()
            .map_err(|e| Error::PeerConnectionError(format!("codecs: {e}")))?;
        // Host candidates on the machine's interfaces: the server peer
        // gathers no loopback candidates.
        let mut settings = SettingEngine::default();
        settings.set_network_types(vec![webrtc::ice::network_type::NetworkType::Udp4]);
        let rtc = Arc::new(
            APIBuilder::new()
                .with_media_engine(media)
                .with_setting_engine(settings)
                .build()
                .new_peer_connection(Default::default())
                .await
                .map_err(|e| Error::PeerConnectionError(e.to_string()))?,
        );

        let answered_shared: Arc<std::sync::RwLock<Vec<AnsweredSection>>> = Arc::default();
        {
            let events_tx = events_tx.clone();
            let answered = Arc::clone(&answered_shared);
            rtc.on_track(Box::new(move |track, _receiver, transceiver| {
                let events_tx = events_tx.clone();
                let answered = Arc::clone(&answered);
                Box::pin(async move {
                    let mid = transceiver.mid().map(|m| m.to_string());
                    let stream = mid.as_ref().and_then(|mid| {
                        answered
                            .read()
                            .ok()?
                            .iter()
                            .find(|section| &section.mid == mid)
                            .map(|section| section.stream.clone())
                    });
                    tokio::spawn(read_track(track, mid, stream, events_tx));
                })
            }));
        }
        {
            let events_tx = events_tx.clone();
            rtc.on_peer_connection_state_change(Box::new(move |state| {
                let _ = events_tx.send(PeerEvent::State(state.to_string()));
                Box::pin(async {})
            }));
        }

        let recvonly = || {
            Some(RTCRtpTransceiverInit {
                direction: RTCRtpTransceiverDirection::Recvonly,
                send_encodings: vec![],
            })
        };
        let mut audio_out = None;
        if options.send_audio {
            let track = Arc::new(TrackLocalStaticSample::new(
                RTCRtpCodecCapability {
                    mime_type: webrtc::api::media_engine::MIME_TYPE_OPUS.to_string(),
                    clock_rate: AUDIO_SAMPLE_RATE,
                    channels: 2,
                    sdp_fmtp_line: "minptime=10;useinbandfec=1".into(),
                    rtcp_feedback: vec![],
                },
                "audio".into(),
                format!("{}-mic", options.peer_id),
            ));
            // A sending track's section is sendrecv, so it also receives.
            rtc.add_track(Arc::clone(&track) as Arc<dyn TrackLocal + Send + Sync>)
                .await
                .map_err(|e| Error::MediaTrackError(format!("audio track: {e}")))?;
            audio_out = Some(AudioOut {
                track,
                encoder: Mutex::new(AudioEncoder::new(AudioEncoderConfig::default())?),
            });
        } else if options.receive_audio {
            rtc.add_transceiver_from_kind(RTPCodecType::Audio, recvonly())
                .await
                .map_err(|e| Error::PeerConnectionError(e.to_string()))?;
        }
        for _ in 0..video_sections {
            rtc.add_transceiver_from_kind(RTPCodecType::Video, recvonly())
                .await
                .map_err(|e| Error::PeerConnectionError(e.to_string()))?;
        }

        let channel_open = Arc::new(AtomicBool::new(false));
        let mut channel = None;
        if options.data_channel {
            let dc = rtc
                .create_data_channel(PIPELINE_CHANNEL_LABEL, None)
                .await
                .map_err(|e| Error::DataChannelError(e.to_string()))?;
            let open = Arc::clone(&channel_open);
            let open_events = events_tx.clone();
            dc.on_open(Box::new(move || {
                open.store(true, Ordering::SeqCst);
                let _ = open_events.send(PeerEvent::DataChannelOpen);
                Box::pin(async {})
            }));
            let data_events = events_tx.clone();
            dc.on_message(Box::new(move |message| {
                let event = match DataBuffer::decode(message.data.clone()) {
                    Ok(buffer) => match crate::adapters::data_buffer_to_runtime_data(&buffer) {
                        Some(data) => PeerEvent::Data {
                            data,
                            metadata: buffer.metadata.clone(),
                        },
                        None => PeerEvent::UndecodableData {
                            bytes: message.data.len(),
                        },
                    },
                    Err(_) => PeerEvent::UndecodableData {
                        bytes: message.data.len(),
                    },
                };
                let _ = data_events.send(event);
                Box::pin(async {})
            }));
            channel = Some(dc);
        }

        let mut gathered = rtc.gathering_complete_promise().await;
        let offer = rtc
            .create_offer(None)
            .await
            .map_err(|e| Error::SdpError(e.to_string()))?;
        rtc.set_local_description(offer)
            .await
            .map_err(|e| Error::SdpError(e.to_string()))?;
        let _ = tokio::time::timeout(options.connect_timeout, gathered.recv()).await;
        let offer_sdp = rtc
            .local_description()
            .await
            .ok_or_else(|| Error::SdpError("no local description".into()))?
            .sdp;
        let mut offer_params =
            json!({"from": options.peer_id, "to": SERVER_PEER_ID, "sdp": offer_sdp});
        if let Some(receive) = receive {
            offer_params["receive"] = receive;
        }
        let answer = signaling.request("peer.offer", offer_params).await?;
        let answered: Vec<AnsweredSection> = answer["media"]
            .as_array()
            .map(|sections| {
                sections
                    .iter()
                    .filter_map(|section| {
                        Some(AnsweredSection {
                            mid: section["mid"].as_str()?.to_string(),
                            kind: section["kind"].as_str()?.to_string(),
                            stream: section["stream"].as_str()?.to_string(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        if let Ok(mut shared) = answered_shared.write() {
            *shared = answered.clone();
        }
        let sdp = answer["sdp"]
            .as_str()
            .ok_or_else(|| Error::SdpError(format!("answer without sdp: {answer}")))?;
        rtc.set_remote_description(
            RTCSessionDescription::answer(sdp.to_string())
                .map_err(|e| Error::SdpError(e.to_string()))?,
        )
        .await
        .map_err(|e| Error::SdpError(e.to_string()))?;
        {
            let rtc = Arc::clone(&rtc);
            tokio::spawn(async move {
                while let Some(candidate) = candidates_rx.recv().await {
                    let _ = rtc.add_ice_candidate(candidate).await;
                }
            });
        }

        Ok(Self {
            peer_id: options.peer_id,
            rtc,
            channel,
            audio_out,
            events: Some(events),
            announce,
            answered,
            signaling,
            channel_open,
        })
    }

    /// The id this member announced.
    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    /// The whole `peer.announce` result.
    pub fn announce_result(&self) -> &Value {
        &self.announce
    }

    /// The media plan the server announced (`None` from an older runtime).
    pub fn media_plan(&self) -> Option<&Value> {
        self.announce.get("media").filter(|plan| !plan.is_null())
    }

    /// Which stream each answered section carries.
    pub fn answered_media(&self) -> &[AnsweredSection] {
        &self.answered
    }

    /// Whether the pipeline data channel is open.
    pub fn data_channel_open(&self) -> bool {
        self.channel_open.load(Ordering::SeqCst)
    }

    /// The next event, or `None` once the client is closed (or after
    /// [`Self::take_events`]).
    pub async fn next_event(&mut self) -> Option<PeerEvent> {
        self.events.as_mut()?.recv().await
    }

    /// Take the event stream, to read it from another task while this
    /// client keeps sending.
    pub fn take_events(&mut self) -> Option<mpsc::UnboundedReceiver<PeerEvent>> {
        self.events.take()
    }

    /// Send one value to the pipeline over the data channel.
    pub async fn send_data(&self, data: &RuntimeData) -> Result<()> {
        let channel = self
            .channel
            .as_ref()
            .ok_or_else(|| Error::DataChannelError("joined without a data channel".into()))?;
        if !self.data_channel_open() {
            return Err(Error::DataChannelError("data channel is not open".into()));
        }
        let wire = crate::adapters::runtime_data_to_data_buffer(data).encode_to_vec();
        channel
            .send(&Bytes::from(wire))
            .await
            .map_err(|e| Error::DataChannelError(e.to_string()))?;
        Ok(())
    }

    /// Send mono [`AUDIO_SAMPLE_RATE`] audio as Opus, 20 ms per packet.
    /// With `realtime` each packet waits for its turn, as a microphone would
    /// deliver it; without, packets go as fast as they encode. A trailing
    /// partial frame is padded with silence.
    pub async fn send_audio(&self, samples: &[f32], realtime: bool) -> Result<()> {
        let out = self
            .audio_out
            .as_ref()
            .ok_or_else(|| Error::MediaTrackError("joined without a sending audio track".into()))?;
        let mut encoder = out.encoder.lock().await;
        let frame_time = Duration::from_millis(20);
        let mut next = tokio::time::Instant::now();
        for frame in samples.chunks(OPUS_FRAME_SAMPLES) {
            let payload = if frame.len() == OPUS_FRAME_SAMPLES {
                encoder.encode(frame)?
            } else {
                let mut padded = frame.to_vec();
                padded.resize(OPUS_FRAME_SAMPLES, 0.0);
                encoder.encode(&padded)?
            };
            out.track
                .write_sample(&webrtc::media::Sample {
                    data: payload.into(),
                    duration: frame_time,
                    timestamp: SystemTime::now(),
                    ..Default::default()
                })
                .await
                .map_err(|e| Error::MediaTrackError(format!("write audio: {e}")))?;
            if realtime {
                next += frame_time;
                tokio::time::sleep_until(next).await;
            }
        }
        Ok(())
    }

    /// One more JSON-RPC request on the signalling socket (`control.*`,
    /// `peer.list`, `server.info`, ...).
    pub async fn signaling_request(&self, method: &str, params: Value) -> Result<Value> {
        self.signaling.request(method, params).await
    }

    /// Hang up: tell the server, close the peer connection and the socket.
    pub async fn close(&self) -> Result<()> {
        let _ = self
            .signaling
            .out
            .send(Message::Text(
                json!({"jsonrpc": "2.0", "method": "peer.disconnect",
                       "params": {"peer_id": self.peer_id}})
                .to_string(),
            ));
        let closed = self.rtc.close().await;
        let _ = self.signaling.out.send(Message::Close(None));
        closed.map_err(|e| Error::PeerConnectionError(e.to_string()))
    }
}

/// Read one remote track until it ends, emitting decoded audio or frames.
async fn read_track(
    track: Arc<TrackRemote>,
    mid: Option<String>,
    stream: Option<String>,
    events: mpsc::UnboundedSender<PeerEvent>,
) {
    let mime = track.codec().capability.mime_type.to_lowercase();
    let kind = if mime.starts_with("audio/") { "audio" } else { "video" };
    let _ = events.send(PeerEvent::Track {
        mid: mid.clone(),
        kind: kind.to_string(),
        stream: stream.clone(),
        mime: mime.clone(),
    });
    if kind == "audio" {
        let mut decoder = match AudioDecoder::new(AudioEncoderConfig::default()) {
            Ok(decoder) => decoder,
            Err(e) => {
                let _ = events.send(PeerEvent::AudioDecodeError {
                    mid,
                    message: e.to_string(),
                });
                return;
            }
        };
        while let Ok((packet, _)) = track.read_rtp().await {
            if packet.payload.is_empty() {
                continue;
            }
            let event = match decoder.decode(&packet.payload) {
                Ok(samples) => PeerEvent::Audio {
                    mid: mid.clone(),
                    stream: stream.clone(),
                    samples,
                    rtp_timestamp: packet.header.timestamp,
                },
                Err(e) => PeerEvent::AudioDecodeError {
                    mid: mid.clone(),
                    message: e.to_string(),
                },
            };
            if events.send(event).is_err() {
                return;
            }
        }
        return;
    }

    let mut frames = FrameAssembler::new(&mime);
    while let Ok((packet, _)) = track.read_rtp().await {
        if let Some(frame) = frames.push(&packet) {
            let event = PeerEvent::VideoFrame {
                mid: mid.clone(),
                stream: stream.clone(),
                bytes: frame.bytes,
                keyframe: frame.keyframe,
                dimensions: frame.dimensions,
                rtp_timestamp: frame.rtp_timestamp,
            };
            if events.send(event).is_err() {
                return;
            }
        }
    }
}

/// A reassembled video frame's facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VideoFrameInfo {
    /// Encoded size.
    pub bytes: usize,
    /// Whether it is a keyframe.
    pub keyframe: bool,
    /// Width and height, when the frame carries them.
    pub dimensions: Option<(u32, u32)>,
    /// RTP timestamp.
    pub rtp_timestamp: u32,
}

enum VideoKind {
    H264(webrtc::rtp::codecs::h264::H264Packet),
    Vp8(webrtc::rtp::codecs::vp8::Vp8Packet),
    Other,
}

/// Groups a video track's RTP packets into frames (by timestamp and marker).
pub struct FrameAssembler {
    kind: VideoKind,
    data: Vec<u8>,
    timestamp: Option<u32>,
}

impl FrameAssembler {
    /// For a track of this codec mime type.
    pub fn new(mime: &str) -> Self {
        let mime = mime.to_lowercase();
        let kind = if mime.contains("h264") {
            VideoKind::H264(Default::default())
        } else if mime.contains("vp8") {
            VideoKind::Vp8(Default::default())
        } else {
            VideoKind::Other
        };
        Self {
            kind,
            data: Vec::new(),
            timestamp: None,
        }
    }

    /// Feed one packet; a frame comes back when one completes.
    pub fn push(&mut self, packet: &webrtc::rtp::packet::Packet) -> Option<VideoFrameInfo> {
        let mut finished = None;
        if let Some(timestamp) = self.timestamp {
            if timestamp != packet.header.timestamp {
                finished = self.finish();
            }
        }
        self.timestamp = Some(packet.header.timestamp);
        let payload = match &mut self.kind {
            VideoKind::H264(depacketizer) => depacketizer.depacketize(&packet.payload).ok(),
            VideoKind::Vp8(depacketizer) => depacketizer.depacketize(&packet.payload).ok(),
            VideoKind::Other => Some(packet.payload.clone()),
        };
        if let Some(payload) = payload {
            self.data.extend_from_slice(&payload);
        }
        if packet.header.marker && finished.is_none() {
            finished = self.finish();
        }
        finished
    }

    fn finish(&mut self) -> Option<VideoFrameInfo> {
        let rtp_timestamp = self.timestamp.take()?;
        let data = std::mem::take(&mut self.data);
        if data.is_empty() {
            return None;
        }
        let (keyframe, dimensions) = match self.kind {
            VideoKind::H264(_) => h264_frame_facts(&data),
            VideoKind::Vp8(_) => vp8_frame_facts(&data),
            VideoKind::Other => (false, None),
        };
        Some(VideoFrameInfo {
            bytes: data.len(),
            keyframe,
            dimensions,
            rtp_timestamp,
        })
    }
}

/// `(is keyframe, dimensions)` of a VP8 frame.
fn vp8_frame_facts(frame: &[u8]) -> (bool, Option<(u32, u32)>) {
    if frame.len() < 10 || frame[0] & 1 != 0 {
        return (false, None);
    }
    if frame[3..6] != [0x9d, 0x01, 0x2a] {
        return (true, None);
    }
    let width = u16::from_le_bytes([frame[6], frame[7]]) & 0x3fff;
    let height = u16::from_le_bytes([frame[8], frame[9]]) & 0x3fff;
    (true, Some((width as u32, height as u32)))
}

/// `(has an IDR slice, dimensions from an SPS)` of an Annex B H.264 frame.
fn h264_frame_facts(frame: &[u8]) -> (bool, Option<(u32, u32)>) {
    let mut keyframe = false;
    let mut dimensions = None;
    for nal in annex_b_nals(frame) {
        match nal.first().map(|header| header & 0x1f) {
            Some(5) => keyframe = true,
            Some(7) => dimensions = h264_sps_dimensions(&nal[1..]),
            _ => {}
        }
    }
    (keyframe, dimensions)
}

fn annex_b_nals(data: &[u8]) -> Vec<&[u8]> {
    let mut starts = Vec::new();
    let mut i = 0;
    while i + 3 <= data.len() {
        if data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 1 {
            starts.push((i, i + 3));
            i += 3;
        } else {
            i += 1;
        }
    }
    let mut nals = Vec::with_capacity(starts.len());
    for (index, &(_, begin)) in starts.iter().enumerate() {
        let mut end = starts.get(index + 1).map(|&(code, _)| code).unwrap_or(data.len());
        // A four-byte start code leaves its leading zero on the previous NAL.
        while end > begin && data[end - 1] == 0 {
            end -= 1;
        }
        if end > begin {
            nals.push(&data[begin..end]);
        }
    }
    nals
}

struct BitReader {
    data: Vec<u8>,
    bit: usize,
}

impl BitReader {
    /// Over an SPS payload, with emulation-prevention bytes removed.
    fn new(payload: &[u8]) -> Self {
        let mut data = Vec::with_capacity(payload.len());
        let mut zeros = 0;
        for &byte in payload {
            if zeros >= 2 && byte == 3 {
                zeros = 0;
                continue;
            }
            zeros = if byte == 0 { zeros + 1 } else { 0 };
            data.push(byte);
        }
        Self { data, bit: 0 }
    }

    fn bit(&mut self) -> Option<u32> {
        let byte = *self.data.get(self.bit / 8)?;
        let value = (byte >> (7 - self.bit % 8)) & 1;
        self.bit += 1;
        Some(value as u32)
    }

    fn bits(&mut self, count: u32) -> Option<u32> {
        let mut value = 0;
        for _ in 0..count {
            value = (value << 1) | self.bit()?;
        }
        Some(value)
    }

    fn ue(&mut self) -> Option<u32> {
        let mut zeros = 0;
        while self.bit()? == 0 {
            zeros += 1;
            if zeros > 31 {
                return None;
            }
        }
        Some((1u32 << zeros) - 1 + self.bits(zeros)?)
    }

    fn se(&mut self) -> Option<i32> {
        let value = self.ue()?;
        Some(if value % 2 == 0 {
            -((value / 2) as i32)
        } else {
            value.div_ceil(2) as i32
        })
    }
}

/// Cropped width and height from an SPS payload (after the NAL header).
fn h264_sps_dimensions(payload: &[u8]) -> Option<(u32, u32)> {
    let mut r = BitReader::new(payload);
    let profile = r.bits(8)?;
    r.bits(16)?; // constraint flags, level
    r.ue()?; // seq_parameter_set_id
    let mut chroma_format = 1;
    if matches!(profile, 100 | 110 | 122 | 244 | 44 | 83 | 86 | 118 | 128 | 138 | 139 | 134 | 135) {
        chroma_format = r.ue()?;
        if chroma_format == 3 {
            r.bit()?;
        }
        r.ue()?; // bit_depth_luma
        r.ue()?; // bit_depth_chroma
        r.bit()?; // qpprime_y_zero_transform_bypass
        if r.bit()? == 1 {
            let lists = if chroma_format == 3 { 12 } else { 8 };
            for list in 0..lists {
                if r.bit()? == 1 {
                    let size = if list < 6 { 16 } else { 64 };
                    let (mut last, mut next) = (8i32, 8i32);
                    for _ in 0..size {
                        if next != 0 {
                            next = (last + r.se()? + 256) % 256;
                        }
                        if next != 0 {
                            last = next;
                        }
                    }
                }
            }
        }
    }
    r.ue()?; // log2_max_frame_num
    let order_type = r.ue()?;
    if order_type == 0 {
        r.ue()?;
    } else if order_type == 1 {
        r.bit()?;
        r.se()?;
        r.se()?;
        let cycle = r.ue()?;
        for _ in 0..cycle {
            r.se()?;
        }
    }
    r.ue()?; // max_num_ref_frames
    r.bit()?; // gaps_in_frame_num_value_allowed
    let width_mbs = r.ue()? + 1;
    let height_map_units = r.ue()? + 1;
    let frame_mbs_only = r.bit()?;
    if frame_mbs_only == 0 {
        r.bit()?; // mb_adaptive_frame_field
    }
    r.bit()?; // direct_8x8_inference
    let (mut left, mut right, mut top, mut bottom) = (0, 0, 0, 0);
    if r.bit()? == 1 {
        left = r.ue()?;
        right = r.ue()?;
        top = r.ue()?;
        bottom = r.ue()?;
    }
    let (crop_x, crop_y) = match chroma_format {
        0 => (1, 2 - frame_mbs_only),
        1 => (2, 2 * (2 - frame_mbs_only)),
        2 => (2, 2 - frame_mbs_only),
        _ => (1, 2 - frame_mbs_only),
    };
    let width = (width_mbs * 16).checked_sub((left + right) * crop_x)?;
    let height =
        ((2 - frame_mbs_only) * height_map_units * 16).checked_sub((top + bottom) * crop_y)?;
    Some((width, height))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sps_dimensions_include_cropping() {
        // x264-style SPS for 1920x1080 (68 rows cropped by 8 lines).
        let sps_1080 = [
            0x64, 0x00, 0x28, 0xac, 0xd9, 0x40, 0x78, 0x02, 0x27, 0xe5, 0xc0, 0x44, 0x00, 0x00,
            0x03, 0x00, 0x04, 0x00, 0x00, 0x03, 0x00, 0xf0, 0x3c, 0x60, 0xc6, 0x58,
        ];
        assert_eq!(h264_sps_dimensions(&sps_1080), Some((1920, 1080)));
    }

    #[test]
    fn vp8_keyframe_header_gives_dimensions() {
        let mut frame = vec![0x10, 0x02, 0x00, 0x9d, 0x01, 0x2a];
        frame.extend_from_slice(&320u16.to_le_bytes());
        frame.extend_from_slice(&240u16.to_le_bytes());
        assert_eq!(vp8_frame_facts(&frame), (true, Some((320, 240))));
        frame[0] |= 1;
        assert_eq!(vp8_frame_facts(&frame), (false, None));
    }

    #[test]
    fn annex_b_split_handles_both_start_codes() {
        let data = [0, 0, 0, 1, 0x67, 0xaa, 0, 0, 1, 0x65, 0xbb, 0xcc];
        let nals = annex_b_nals(&data);
        assert_eq!(nals, vec![&[0x67, 0xaa][..], &[0x65, 0xbb, 0xcc][..]]);
    }
}
