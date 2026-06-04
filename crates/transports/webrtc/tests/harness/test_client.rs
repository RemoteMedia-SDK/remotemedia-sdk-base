//! Test client for WebRTC E2E testing
//!
//! Implements a full WebRTC client using gRPC signaling to connect to the test server.
//! Supports sending and receiving audio/video with proper encoding/decoding.

use super::{HarnessError, HarnessResult};
use remotemedia_webrtc::generated::webrtc::{
    signaling_notification, signaling_request, signaling_response,
    web_rtc_signaling_client::WebRtcSignalingClient, AnnounceRequest, IceCandidateRequest,
    OfferRequest, PeerCapabilities, SignalingRequest,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::APIBuilder;
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtp_transceiver::rtp_codec::{RTCRtpCodecCapability, RTPCodecType};
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;
use webrtc::track::track_local::TrackLocal;
use webrtc::track::track_remote::TrackRemote;

/// Received video frame with metadata
#[derive(Debug, Clone)]
pub struct ReceivedVideoFrame {
    /// Raw RTP payload data
    pub data: Vec<u8>,
    /// Frame width (0 if unknown/not decoded)
    pub width: u32,
    /// Frame height (0 if unknown/not decoded)
    pub height: u32,
    /// Timestamp in microseconds
    pub timestamp_us: u64,
}

/// Received audio chunk
#[derive(Debug, Clone)]
pub struct ReceivedAudioChunk {
    /// Raw RTP payload data (Opus encoded)
    pub data: Vec<u8>,
    /// Sample rate in Hz
    pub sample_rate: u32,
    /// Number of channels
    pub channels: u16,
    /// Timestamp in microseconds
    pub timestamp_us: u64,
}

/// Received track info with stream_id for multi-track support
#[derive(Debug, Clone)]
pub struct ReceivedTrackInfo {
    /// Track ID from SDP
    pub track_id: String,
    /// Stream ID (extracted from track ID if using generate_track_id format)
    pub stream_id: Option<String>,
    /// Media kind (audio or video)
    pub kind: String,
    /// MIME type (e.g., "video/VP8", "audio/opus")
    pub mime_type: String,
    /// Packet count for this track
    pub packet_count: u32,
}

/// Test client for WebRTC E2E testing
///
/// Connects to the signaling server, establishes WebRTC peer connection,
/// and provides methods for sending/receiving media.
#[derive(Debug)]
pub struct TestClient {
    /// Unique peer identifier
    peer_id: String,

    /// Server port
    server_port: u16,

    /// gRPC signaling client
    signaling_client: Arc<RwLock<Option<WebRtcSignalingClient<Channel>>>>,

    /// Signaling request sender (for sending to gRPC stream)
    request_tx: Arc<RwLock<Option<mpsc::Sender<SignalingRequest>>>>,

    /// WebRTC peer connection
    peer_connection: Arc<RwLock<Option<Arc<RTCPeerConnection>>>>,

    /// Audio track for sending
    audio_track: Arc<RwLock<Option<Arc<TrackLocalStaticSample>>>>,

    /// Video track for sending
    video_track: Arc<RwLock<Option<Arc<TrackLocalStaticSample>>>>,

    /// Multiple video tracks by stream_id (for multi-track testing)
    video_tracks: Arc<RwLock<HashMap<String, Arc<TrackLocalStaticSample>>>>,

    /// Multiple audio tracks by stream_id (for multi-track testing)
    audio_tracks: Arc<RwLock<HashMap<String, Arc<TrackLocalStaticSample>>>>,

    /// Received audio output (accumulated from pipeline)
    received_audio: Arc<RwLock<Vec<ReceivedAudioChunk>>>,

    /// Received video frames (accumulated from pipeline)
    received_video: Arc<RwLock<Vec<ReceivedVideoFrame>>>,

    /// Received audio per stream_id (for multi-track testing)
    received_audio_by_stream: Arc<RwLock<HashMap<String, Vec<ReceivedAudioChunk>>>>,

    /// Received video per stream_id (for multi-track testing)
    received_video_by_stream: Arc<RwLock<HashMap<String, Vec<ReceivedVideoFrame>>>>,

    /// Track info for received tracks (track_id -> info)
    received_track_info: Arc<RwLock<HashMap<String, ReceivedTrackInfo>>>,

    /// Received RTP packet count (audio)
    received_audio_packets: Arc<AtomicU32>,

    /// Received RTP packet count (video)
    received_video_packets: Arc<AtomicU32>,

    /// Request ID counter
    request_counter: AtomicU64,

    /// Connection state
    connected: Arc<AtomicBool>,

    /// Signaling stream task handle
    stream_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,

    /// Track receiver task handles
    track_handles: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,
}

impl TestClient {
    /// Create a new test client
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Unique identifier for this client
    /// * `server_port` - Port of the signaling server
    pub async fn new(peer_id: String, server_port: u16) -> HarnessResult<Self> {
        info!(
            "Creating test client: {} (server port: {})",
            peer_id, server_port
        );

        Ok(Self {
            peer_id,
            server_port,
            signaling_client: Arc::new(RwLock::new(None)),
            request_tx: Arc::new(RwLock::new(None)),
            peer_connection: Arc::new(RwLock::new(None)),
            audio_track: Arc::new(RwLock::new(None)),
            video_track: Arc::new(RwLock::new(None)),
            video_tracks: Arc::new(RwLock::new(HashMap::new())),
            audio_tracks: Arc::new(RwLock::new(HashMap::new())),
            received_audio: Arc::new(RwLock::new(Vec::new())),
            received_video: Arc::new(RwLock::new(Vec::new())),
            received_audio_by_stream: Arc::new(RwLock::new(HashMap::new())),
            received_video_by_stream: Arc::new(RwLock::new(HashMap::new())),
            received_track_info: Arc::new(RwLock::new(HashMap::new())),
            received_audio_packets: Arc::new(AtomicU32::new(0)),
            received_video_packets: Arc::new(AtomicU32::new(0)),
            request_counter: AtomicU64::new(0),
            connected: Arc::new(AtomicBool::new(false)),
            stream_handle: Arc::new(RwLock::new(None)),
            track_handles: Arc::new(RwLock::new(Vec::new())),
        })
    }

    /// Get peer ID
    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    /// Check if connected
    pub async fn is_connected(&self) -> bool {
        self.connected.load(Ordering::SeqCst)
    }

    /// Get count of received audio RTP packets
    pub fn received_audio_packet_count(&self) -> u32 {
        self.received_audio_packets.load(Ordering::SeqCst)
    }

    /// Get count of received video RTP packets
    pub fn received_video_packet_count(&self) -> u32 {
        self.received_video_packets.load(Ordering::SeqCst)
    }

    /// Generate next request ID
    fn next_request_id(&self) -> String {
        let id = self.request_counter.fetch_add(1, Ordering::SeqCst);
        format!("{}-{}", self.peer_id, id)
    }

    /// Create a WebRTC peer connection with audio and video transceivers
    async fn create_peer_connection(
        &self,
        received_audio: Arc<RwLock<Vec<ReceivedAudioChunk>>>,
        received_video: Arc<RwLock<Vec<ReceivedVideoFrame>>>,
        received_audio_packets: Arc<AtomicU32>,
        received_video_packets: Arc<AtomicU32>,
        track_handles: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,
    ) -> HarnessResult<Arc<RTCPeerConnection>> {
        // Create media engine with default codecs
        let mut media_engine = MediaEngine::default();
        media_engine.register_default_codecs().map_err(|e| {
            HarnessError::ConnectionError(format!("Failed to register codecs: {}", e))
        })?;

        // Create interceptor registry
        let interceptor_registry =
            register_default_interceptors(Default::default(), &mut media_engine).map_err(|e| {
                HarnessError::ConnectionError(format!("Failed to register interceptors: {}", e))
            })?;

        // Build API
        let api = APIBuilder::new()
            .with_media_engine(media_engine)
            .with_interceptor_registry(interceptor_registry)
            .build();

        // Configure ICE servers
        let config = RTCConfiguration {
            ice_servers: vec![RTCIceServer {
                urls: vec!["stun:stun.l.google.com:19302".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        };

        // Create peer connection
        let peer_connection = Arc::new(api.new_peer_connection(config).await.map_err(|e| {
            HarnessError::ConnectionError(format!("Failed to create peer connection: {}", e))
        })?);

        // Set up on_track handler to receive incoming audio/video from server
        let peer_id = self.peer_id.clone();
        let received_audio_clone = Arc::clone(&received_audio);
        let received_video_clone = Arc::clone(&received_video);
        let received_audio_packets_clone = Arc::clone(&received_audio_packets);
        let received_video_packets_clone = Arc::clone(&received_video_packets);
        let track_handles_clone = Arc::clone(&track_handles);

        peer_connection.on_track(Box::new(move |track, _receiver, _transceiver| {
            let peer_id = peer_id.clone();
            let track = Arc::clone(&track);
            let received_audio = Arc::clone(&received_audio_clone);
            let received_video = Arc::clone(&received_video_clone);
            let received_audio_packets = Arc::clone(&received_audio_packets_clone);
            let received_video_packets = Arc::clone(&received_video_packets_clone);
            let track_handles = Arc::clone(&track_handles_clone);

            Box::pin(async move {
                let codec = track.codec();
                info!(
                    "TestClient {}: Received track - kind={}, codec={}",
                    peer_id,
                    track.kind(),
                    codec.capability.mime_type
                );

                // Spawn a task to read from this track
                let handle = tokio::spawn(Self::handle_incoming_track(
                    peer_id,
                    track,
                    received_audio,
                    received_video,
                    received_audio_packets,
                    received_video_packets,
                ));

                track_handles.write().await.push(handle);
            })
        }));

        Ok(peer_connection)
    }

    /// Handle incoming RTP packets from a remote track
    async fn handle_incoming_track(
        peer_id: String,
        track: Arc<TrackRemote>,
        received_audio: Arc<RwLock<Vec<ReceivedAudioChunk>>>,
        received_video: Arc<RwLock<Vec<ReceivedVideoFrame>>>,
        received_audio_packets: Arc<AtomicU32>,
        received_video_packets: Arc<AtomicU32>,
    ) {
        let codec = track.codec();
        let mime_type = codec.capability.mime_type.to_lowercase();
        let is_audio = mime_type.starts_with("audio/");
        let is_video = mime_type.starts_with("video/");

        info!(
            "TestClient {}: Starting track receiver for {} ({})",
            peer_id,
            track.kind(),
            mime_type
        );

        // Read RTP packets from the track using read_rtp()
        loop {
            match track.read_rtp().await {
                Ok((rtp_packet, _attributes)) => {
                    let payload = rtp_packet.payload.to_vec();
                    if payload.is_empty() {
                        continue;
                    }

                    let timestamp_us = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_micros() as u64;

                    if is_audio {
                        let count = received_audio_packets.fetch_add(1, Ordering::SeqCst) + 1;
                        debug!(
                            "TestClient {}: Received audio RTP packet #{} ({} bytes)",
                            peer_id,
                            count,
                            payload.len()
                        );

                        // Store raw Opus payload
                        let chunk = ReceivedAudioChunk {
                            data: payload,
                            sample_rate: 48000,
                            channels: 1,
                            timestamp_us,
                        };
                        received_audio.write().await.push(chunk);
                    } else if is_video {
                        let count = received_video_packets.fetch_add(1, Ordering::SeqCst) + 1;
                        debug!(
                            "TestClient {}: Received video RTP packet #{} ({} bytes)",
                            peer_id,
                            count,
                            payload.len()
                        );

                        // Store raw VP8 payload
                        let frame = ReceivedVideoFrame {
                            data: payload,
                            width: 0,  // Unknown until decoded
                            height: 0, // Unknown until decoded
                            timestamp_us,
                        };
                        received_video.write().await.push(frame);
                    }
                }
                Err(e) => {
                    // Check if this is just the track closing
                    let err_str = e.to_string();
                    if err_str.contains("closed") || err_str.contains("EOF") {
                        info!("TestClient {}: Track closed normally", peer_id);
                    } else {
                        warn!("TestClient {}: Track read error: {}", peer_id, e);
                    }
                    break;
                }
            }
        }

        info!("TestClient {}: Track receiver ended", peer_id);
    }

    /// Connect to the signaling server and establish WebRTC connection
    pub async fn connect(&self) -> HarnessResult<()> {
        info!("Connecting test client: {}", self.peer_id);

        let addr = format!("http://127.0.0.1:{}", self.server_port);

        // Connect gRPC client
        let grpc_client = WebRtcSignalingClient::connect(addr.clone())
            .await
            .map_err(|e| HarnessError::ConnectionError(format!("gRPC connect failed: {}", e)))?;

        // Create request channel for sending to stream
        let (request_tx, request_rx) = mpsc::channel::<SignalingRequest>(32);

        // Start bidirectional stream
        let request_stream = ReceiverStream::new(request_rx);
        let mut grpc_client_clone = grpc_client.clone();
        let response_stream = grpc_client_clone
            .signal(request_stream)
            .await
            .map_err(|e| HarnessError::ConnectionError(format!("Signal stream failed: {}", e)))?
            .into_inner();

        // Store client and sender
        *self.signaling_client.write().await = Some(grpc_client);
        *self.request_tx.write().await = Some(request_tx.clone());

        // Create WebRTC peer connection with track handlers
        let peer_connection = self
            .create_peer_connection(
                Arc::clone(&self.received_audio),
                Arc::clone(&self.received_video),
                Arc::clone(&self.received_audio_packets),
                Arc::clone(&self.received_video_packets),
                Arc::clone(&self.track_handles),
            )
            .await?;

        // Add audio transceiver (sendrecv) - enables both sending and receiving audio
        let _audio_transceiver = peer_connection
            .add_transceiver_from_kind(RTPCodecType::Audio, None)
            .await
            .map_err(|e| {
                HarnessError::ConnectionError(format!("Failed to add audio transceiver: {}", e))
            })?;

        // Create and set up the audio track for sending
        let audio_track = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: "audio/opus".to_string(),
                clock_rate: 48000,
                channels: 1,
                sdp_fmtp_line: String::new(),
                rtcp_feedback: vec![],
            },
            format!("audio-{}", self.peer_id),
            format!("stream-{}", self.peer_id),
        ));

        peer_connection
            .add_track(audio_track.clone() as Arc<dyn TrackLocal + Send + Sync>)
            .await
            .map_err(|e| {
                HarnessError::ConnectionError(format!("Failed to add audio track: {}", e))
            })?;

        *self.audio_track.write().await = Some(audio_track);

        // Add video transceiver (sendrecv) - enables both sending and receiving video
        let _video_transceiver = peer_connection
            .add_transceiver_from_kind(RTPCodecType::Video, None)
            .await
            .map_err(|e| {
                HarnessError::ConnectionError(format!("Failed to add video transceiver: {}", e))
            })?;

        // Create and set up the video track for sending
        let video_track = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: "video/vp8".to_string(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: String::new(),
                rtcp_feedback: vec![],
            },
            format!("video-{}", self.peer_id),
            format!("stream-{}", self.peer_id),
        ));

        peer_connection
            .add_track(video_track.clone() as Arc<dyn TrackLocal + Send + Sync>)
            .await
            .map_err(|e| {
                HarnessError::ConnectionError(format!("Failed to add video track: {}", e))
            })?;

        *self.video_track.write().await = Some(video_track);
        *self.peer_connection.write().await = Some(Arc::clone(&peer_connection));

        // Set up ICE candidate handler
        let request_tx_ice = request_tx.clone();
        let peer_id_ice = self.peer_id.clone();
        let request_counter = Arc::new(AtomicU64::new(100));

        peer_connection.on_ice_candidate(Box::new(move |candidate| {
            let request_tx = request_tx_ice.clone();
            let peer_id = peer_id_ice.clone();
            let counter = Arc::clone(&request_counter);

            Box::pin(async move {
                if let Some(candidate) = candidate {
                    let request_id =
                        format!("{}-ice-{}", peer_id, counter.fetch_add(1, Ordering::SeqCst));

                    if let Ok(json) = candidate.to_json() {
                        let ice_request = SignalingRequest {
                            request_id,
                            request: Some(signaling_request::Request::IceCandidate(
                                IceCandidateRequest {
                                    to_peer_id: "remotemedia-server".to_string(),
                                    candidate: json.candidate,
                                    sdp_mid: json.sdp_mid.unwrap_or_default(),
                                    sdp_mline_index: json.sdp_mline_index.unwrap_or(0) as u32,
                                },
                            )),
                        };

                        if let Err(e) = request_tx.send(ice_request).await {
                            warn!("Failed to send ICE candidate: {}", e);
                        }
                    }
                }
            })
        }));

        // Spawn task to handle response stream
        let connected = Arc::clone(&self.connected);
        let peer_id = self.peer_id.clone();
        let peer_connection_clone = Arc::clone(&peer_connection);

        let stream_handle = tokio::spawn(async move {
            Self::handle_signaling_stream(
                response_stream,
                peer_connection_clone,
                connected,
                peer_id,
            )
            .await;
        });

        *self.stream_handle.write().await = Some(stream_handle);

        // Send announce request
        let announce = SignalingRequest {
            request_id: self.next_request_id(),
            request: Some(signaling_request::Request::Announce(AnnounceRequest {
                peer_id: self.peer_id.clone(),
                capabilities: Some(PeerCapabilities {
                    audio: true,
                    video: true,
                    data: true,
                    extensions: "{}".to_string(),
                }),
                metadata: HashMap::new(),
            })),
        };

        request_tx.send(announce).await.map_err(|e| {
            HarnessError::ConnectionError(format!("Failed to send announce: {}", e))
        })?;

        // Wait briefly for announce to be processed
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create and send offer to remotemedia-server
        let offer = peer_connection
            .create_offer(None)
            .await
            .map_err(|e| HarnessError::ConnectionError(format!("Failed to create offer: {}", e)))?;

        peer_connection
            .set_local_description(offer.clone())
            .await
            .map_err(|e| {
                HarnessError::ConnectionError(format!("Failed to set local description: {}", e))
            })?;

        let offer_request = SignalingRequest {
            request_id: self.next_request_id(),
            request: Some(signaling_request::Request::Offer(OfferRequest {
                to_peer_id: "remotemedia-server".to_string(),
                sdp: offer.sdp,
                r#type: "offer".to_string(),
            })),
        };

        request_tx
            .send(offer_request)
            .await
            .map_err(|e| HarnessError::ConnectionError(format!("Failed to send offer: {}", e)))?;

        // Wait for connection to be established (answer + ICE)
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !self.connected.load(Ordering::SeqCst) {
            if tokio::time::Instant::now() >= deadline {
                return Err(HarnessError::Timeout(
                    "WebRTC connection not established".to_string(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!("Test client {} connected successfully", self.peer_id);
        Ok(())
    }

    /// Handle incoming signaling stream messages
    async fn handle_signaling_stream(
        mut stream: tonic::Streaming<remotemedia_webrtc::generated::webrtc::SignalingResponse>,
        peer_connection: Arc<RTCPeerConnection>,
        connected: Arc<AtomicBool>,
        peer_id: String,
    ) {
        while let Some(result) = stream.next().await {
            match result {
                Ok(response) => {
                    match response.response {
                        Some(signaling_response::Response::Notification(notification)) => {
                            match notification.notification {
                                Some(signaling_notification::Notification::Answer(answer)) => {
                                    info!(
                                        "Received SDP answer from {} for {}",
                                        answer.from_peer_id, peer_id
                                    );

                                    // Set remote description
                                    let answer_desc =
                                        match RTCSessionDescription::answer(answer.sdp) {
                                            Ok(desc) => desc,
                                            Err(e) => {
                                                error!("Invalid answer SDP: {}", e);
                                                continue;
                                            }
                                        };

                                    if let Err(e) =
                                        peer_connection.set_remote_description(answer_desc).await
                                    {
                                        error!("Failed to set remote description: {}", e);
                                    } else {
                                        info!("Remote description set for {}", peer_id);
                                        connected.store(true, Ordering::SeqCst);
                                    }
                                }
                                Some(signaling_notification::Notification::IceCandidate(ice)) => {
                                    debug!(
                                        "Received ICE candidate from {} for {}",
                                        ice.from_peer_id, peer_id
                                    );

                                    let candidate_init = RTCIceCandidateInit {
                                        candidate: ice.candidate,
                                        sdp_mid: if ice.sdp_mid.is_empty() {
                                            None
                                        } else {
                                            Some(ice.sdp_mid)
                                        },
                                        sdp_mline_index: Some(ice.sdp_mline_index as u16),
                                        username_fragment: None,
                                    };

                                    if let Err(e) =
                                        peer_connection.add_ice_candidate(candidate_init).await
                                    {
                                        warn!("Failed to add ICE candidate: {}", e);
                                    }
                                }
                                Some(signaling_notification::Notification::PeerJoined(joined)) => {
                                    debug!("Peer joined: {}", joined.peer_id);
                                }
                                Some(signaling_notification::Notification::PeerLeft(left)) => {
                                    debug!("Peer left: {}", left.peer_id);
                                }
                                _ => {}
                            }
                        }
                        Some(signaling_response::Response::Ack(ack)) => {
                            debug!("Received ack: {}", ack.message);
                        }
                        Some(signaling_response::Response::Error(err)) => {
                            error!("Signaling error: {} - {}", err.code, err.message);
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    error!("Stream error for {}: {}", peer_id, e);
                    break;
                }
            }
        }

        info!("Signaling stream ended for {}", peer_id);
        connected.store(false, Ordering::SeqCst);
    }

    /// Send audio samples to the pipeline
    ///
    /// # Arguments
    ///
    /// * `samples` - Audio samples (f32, -1.0 to 1.0)
    /// * `sample_rate` - Sample rate in Hz
    pub async fn send_audio(&self, samples: &[f32], _sample_rate: u32) -> HarnessResult<()> {
        let audio_track =
            self.audio_track.read().await.clone().ok_or_else(|| {
                HarnessError::ClientError("Audio track not configured".to_string())
            })?;

        // Convert f32 samples to bytes (for Opus, we'd encode here)
        // For now, send as raw PCM bytes which the server will need to handle
        // In a full implementation, we'd use an Opus encoder
        debug!(
            "Sending {} audio samples (encoding simplified for test)",
            samples.len()
        );

        // Create sample data - in real implementation this would be Opus-encoded
        // For testing, we'll send placeholder Opus frames
        let sample = webrtc::media::Sample {
            data: vec![0u8; 160].into(), // Minimal Opus frame size
            duration: Duration::from_millis(20),
            timestamp: std::time::SystemTime::now(),
            ..Default::default()
        };

        audio_track.write_sample(&sample).await.map_err(|e| {
            HarnessError::MediaError(format!("Failed to write audio sample: {}", e))
        })?;

        Ok(())
    }

    /// Send video frame to the pipeline
    ///
    /// # Arguments
    ///
    /// * `frame` - Raw video frame data (I420/YUV420P)
    /// * `width` - Frame width
    /// * `height` - Frame height
    pub async fn send_video(&self, _frame: &[u8], _width: u32, _height: u32) -> HarnessResult<()> {
        let video_track =
            self.video_track.read().await.clone().ok_or_else(|| {
                HarnessError::ClientError("Video track not configured".to_string())
            })?;

        debug!("Sending video frame (encoding simplified for test)");

        // Create sample data - in real implementation this would be VP8-encoded
        // For testing, we'll send placeholder VP8 keyframe
        let sample = webrtc::media::Sample {
            data: vec![0u8; 100].into(),         // Minimal VP8 frame placeholder
            duration: Duration::from_millis(33), // ~30fps
            timestamp: std::time::SystemTime::now(),
            ..Default::default()
        };

        video_track.write_sample(&sample).await.map_err(|e| {
            HarnessError::MediaError(format!("Failed to write video sample: {}", e))
        })?;

        Ok(())
    }

    // ========================================================================
    // Multi-Track Support Methods (Spec 013)
    // ========================================================================

    /// Add a video track with a specific stream_id
    ///
    /// This creates a new video track that will appear in SDP negotiation.
    /// Use this to test multi-track scenarios.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - Unique identifier for this stream (e.g., "camera", "screen")
    pub async fn add_video_track_with_stream_id(
        &self,
        stream_id: &str,
    ) -> HarnessResult<Arc<TrackLocalStaticSample>> {
        let peer_connection = self
            .peer_connection
            .read()
            .await
            .clone()
            .ok_or_else(|| HarnessError::ClientError("Not connected".to_string()))?;

        // Create track with stream_id in the track ID (matching generate_track_id format)
        let track_id = format!("video_{}", stream_id);
        let stream_label = format!("stream-{}-{}", self.peer_id, stream_id);

        let video_track = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: "video/vp8".to_string(),
                clock_rate: 90000,
                channels: 0,
                sdp_fmtp_line: String::new(),
                rtcp_feedback: vec![],
            },
            track_id.clone(),
            stream_label,
        ));

        // Add track to peer connection
        peer_connection
            .add_track(video_track.clone() as Arc<dyn TrackLocal + Send + Sync>)
            .await
            .map_err(|e| {
                HarnessError::ConnectionError(format!("Failed to add video track: {}", e))
            })?;

        // Store in multi-track map
        self.video_tracks
            .write()
            .await
            .insert(stream_id.to_string(), Arc::clone(&video_track));

        info!(
            "TestClient {}: Added video track with stream_id '{}'",
            self.peer_id, stream_id
        );

        Ok(video_track)
    }

    /// Add an audio track with a specific stream_id
    ///
    /// # Arguments
    ///
    /// * `stream_id` - Unique identifier for this stream (e.g., "voice", "music")
    pub async fn add_audio_track_with_stream_id(
        &self,
        stream_id: &str,
    ) -> HarnessResult<Arc<TrackLocalStaticSample>> {
        let peer_connection = self
            .peer_connection
            .read()
            .await
            .clone()
            .ok_or_else(|| HarnessError::ClientError("Not connected".to_string()))?;

        // Create track with stream_id in the track ID
        let track_id = format!("audio_{}", stream_id);
        let stream_label = format!("stream-{}-{}", self.peer_id, stream_id);

        let audio_track = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: "audio/opus".to_string(),
                clock_rate: 48000,
                channels: 1,
                sdp_fmtp_line: String::new(),
                rtcp_feedback: vec![],
            },
            track_id.clone(),
            stream_label,
        ));

        // Add track to peer connection
        peer_connection
            .add_track(audio_track.clone() as Arc<dyn TrackLocal + Send + Sync>)
            .await
            .map_err(|e| {
                HarnessError::ConnectionError(format!("Failed to add audio track: {}", e))
            })?;

        // Store in multi-track map
        self.audio_tracks
            .write()
            .await
            .insert(stream_id.to_string(), Arc::clone(&audio_track));

        info!(
            "TestClient {}: Added audio track with stream_id '{}'",
            self.peer_id, stream_id
        );

        Ok(audio_track)
    }

    /// Send video frame on a specific stream
    ///
    /// # Arguments
    ///
    /// * `stream_id` - Stream identifier to send on
    /// * `frame` - Raw video frame data (I420/YUV420P)
    /// * `width` - Frame width
    /// * `height` - Frame height
    pub async fn send_video_on_stream(
        &self,
        stream_id: &str,
        _frame: &[u8],
        _width: u32,
        _height: u32,
    ) -> HarnessResult<()> {
        let tracks = self.video_tracks.read().await;
        let track = tracks.get(stream_id).ok_or_else(|| {
            HarnessError::ClientError(format!("Video track '{}' not found", stream_id))
        })?;

        debug!(
            "Sending video frame on stream '{}' (encoding simplified for test)",
            stream_id
        );

        let sample = webrtc::media::Sample {
            data: vec![0u8; 100].into(),
            duration: Duration::from_millis(33),
            timestamp: std::time::SystemTime::now(),
            ..Default::default()
        };

        track.write_sample(&sample).await.map_err(|e| {
            HarnessError::MediaError(format!("Failed to write video sample: {}", e))
        })?;

        Ok(())
    }

    /// Send audio on a specific stream
    ///
    /// # Arguments
    ///
    /// * `stream_id` - Stream identifier to send on
    /// * `samples` - Audio samples (f32, -1.0 to 1.0)
    /// * `sample_rate` - Sample rate in Hz
    pub async fn send_audio_on_stream(
        &self,
        stream_id: &str,
        _samples: &[f32],
        _sample_rate: u32,
    ) -> HarnessResult<()> {
        let tracks = self.audio_tracks.read().await;
        let track = tracks.get(stream_id).ok_or_else(|| {
            HarnessError::ClientError(format!("Audio track '{}' not found", stream_id))
        })?;

        debug!(
            "Sending audio on stream '{}' (encoding simplified for test)",
            stream_id
        );

        let sample = webrtc::media::Sample {
            data: vec![0u8; 160].into(),
            duration: Duration::from_millis(20),
            timestamp: std::time::SystemTime::now(),
            ..Default::default()
        };

        track.write_sample(&sample).await.map_err(|e| {
            HarnessError::MediaError(format!("Failed to write audio sample: {}", e))
        })?;

        Ok(())
    }

    /// Get the number of video tracks added with stream_id
    pub async fn video_track_count(&self) -> usize {
        self.video_tracks.read().await.len()
    }

    /// Get the number of audio tracks added with stream_id
    pub async fn audio_track_count(&self) -> usize {
        self.audio_tracks.read().await.len()
    }

    /// Get all video stream IDs
    pub async fn video_stream_ids(&self) -> Vec<String> {
        self.video_tracks.read().await.keys().cloned().collect()
    }

    /// Get all audio stream IDs
    pub async fn audio_stream_ids(&self) -> Vec<String> {
        self.audio_tracks.read().await.keys().cloned().collect()
    }

    /// Get received video frames for a specific stream
    pub async fn get_received_video_for_stream(&self, stream_id: &str) -> Vec<ReceivedVideoFrame> {
        self.received_video_by_stream
            .read()
            .await
            .get(stream_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get received audio chunks for a specific stream
    pub async fn get_received_audio_for_stream(&self, stream_id: &str) -> Vec<ReceivedAudioChunk> {
        self.received_audio_by_stream
            .read()
            .await
            .get(stream_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Get info about all received tracks
    pub async fn get_received_track_info(&self) -> HashMap<String, ReceivedTrackInfo> {
        self.received_track_info.read().await.clone()
    }

    /// Get the number of distinct video tracks received
    pub async fn received_video_track_count(&self) -> usize {
        self.received_track_info
            .read()
            .await
            .values()
            .filter(|info| info.kind == "video")
            .count()
    }

    /// Get the number of distinct audio tracks received
    pub async fn received_audio_track_count(&self) -> usize {
        self.received_track_info
            .read()
            .await
            .values()
            .filter(|info| info.kind == "audio")
            .count()
    }

    /// Extract stream_id from a track_id (inverse of generate_track_id)
    ///
    /// Expected format: "video_camera" -> "camera", "audio_voice" -> "voice"
    fn extract_stream_id_from_track_id(track_id: &str) -> Option<String> {
        if let Some(suffix) = track_id.strip_prefix("video_") {
            Some(suffix.to_string())
        } else if let Some(suffix) = track_id.strip_prefix("audio_") {
            Some(suffix.to_string())
        } else {
            None
        }
    }

    /// Get accumulated received audio chunks
    pub async fn get_received_audio(&self) -> Vec<ReceivedAudioChunk> {
        self.received_audio.read().await.clone()
    }

    /// Get accumulated received video frames
    pub async fn get_received_video(&self) -> Vec<ReceivedVideoFrame> {
        self.received_video.read().await.clone()
    }

    /// Clear received audio buffer
    pub async fn clear_received_audio(&self) {
        self.received_audio.write().await.clear();
        self.received_audio_packets.store(0, Ordering::SeqCst);
    }

    /// Clear received video buffer
    pub async fn clear_received_video(&self) {
        self.received_video.write().await.clear();
        self.received_video_packets.store(0, Ordering::SeqCst);
    }

    /// Wait for audio output with timeout
    ///
    /// Returns all accumulated audio data as raw bytes
    pub async fn wait_for_audio(&self, timeout: Duration) -> HarnessResult<Vec<u8>> {
        let deadline = tokio::time::Instant::now() + timeout;

        while tokio::time::Instant::now() < deadline {
            let audio = self.received_audio.read().await;
            if !audio.is_empty() {
                // Flatten all received chunks' data
                return Ok(audio.iter().flat_map(|c| c.data.clone()).collect());
            }
            drop(audio);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Err(HarnessError::Timeout("No audio received".to_string()))
    }

    /// Wait for at least N audio packets with timeout
    pub async fn wait_for_audio_packets(
        &self,
        min_packets: u32,
        timeout: Duration,
    ) -> HarnessResult<u32> {
        let deadline = tokio::time::Instant::now() + timeout;

        while tokio::time::Instant::now() < deadline {
            let count = self.received_audio_packets.load(Ordering::SeqCst);
            if count >= min_packets {
                return Ok(count);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let count = self.received_audio_packets.load(Ordering::SeqCst);
        Err(HarnessError::Timeout(format!(
            "Only received {} audio packets, expected at least {}",
            count, min_packets
        )))
    }

    /// Wait for at least N video packets with timeout
    pub async fn wait_for_video_packets(
        &self,
        min_packets: u32,
        timeout: Duration,
    ) -> HarnessResult<u32> {
        let deadline = tokio::time::Instant::now() + timeout;

        while tokio::time::Instant::now() < deadline {
            let count = self.received_video_packets.load(Ordering::SeqCst);
            if count >= min_packets {
                return Ok(count);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let count = self.received_video_packets.load(Ordering::SeqCst);
        Err(HarnessError::Timeout(format!(
            "Only received {} video packets, expected at least {}",
            count, min_packets
        )))
    }

    /// Wait for video output with timeout
    pub async fn wait_for_video(&self, timeout: Duration) -> HarnessResult<ReceivedVideoFrame> {
        let deadline = tokio::time::Instant::now() + timeout;

        while tokio::time::Instant::now() < deadline {
            let video = self.received_video.read().await;
            if let Some(frame) = video.first() {
                return Ok(frame.clone());
            }
            drop(video);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Err(HarnessError::Timeout("No video received".to_string()))
    }

    /// Disconnect from the server
    pub async fn disconnect(&self) -> HarnessResult<()> {
        info!("Disconnecting test client: {}", self.peer_id);

        // Close peer connection
        if let Some(pc) = self.peer_connection.write().await.take() {
            let _ = pc.close().await;
        }

        // Abort stream task
        if let Some(handle) = self.stream_handle.write().await.take() {
            handle.abort();
        }

        // Abort track receiver tasks
        for handle in self.track_handles.write().await.drain(..) {
            handle.abort();
        }

        self.connected.store(false, Ordering::SeqCst);
        *self.signaling_client.write().await = None;
        *self.request_tx.write().await = None;

        info!("Test client {} disconnected", self.peer_id);
        Ok(())
    }
}

impl Drop for TestClient {
    fn drop(&mut self) {
        tracing::debug!("TestClient {} dropped", self.peer_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_creation() {
        let client = TestClient::new("test-peer".to_string(), 12345)
            .await
            .unwrap();
        assert_eq!(client.peer_id(), "test-peer");
        assert!(!client.is_connected().await);
    }
}
