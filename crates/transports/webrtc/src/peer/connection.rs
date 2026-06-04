//! WebRTC peer connection management

// Phase 4 (US2) peer connection infrastructure
#![allow(dead_code)]

use crate::channels::{DataChannel, DataChannelMessage};
use crate::config::DataChannelMode;
use crate::media::audio::AudioEncoderConfig;
use crate::media::tracks::AudioTrack;
use crate::media::tracks::VideoTrack;
use crate::media::video::VideoEncoderConfig;
use crate::peer::lifecycle::{
    ConnectionQualityMetrics, ReconnectionManager, ReconnectionPolicy, ReconnectionState,
};
use crate::sync::{
    AudioFrame as SyncAudioFrame, RtcpSenderReport, SyncConfig, SyncManager, SyncState,
    SyncedAudioFrame, SyncedVideoFrame, VideoFrame as SyncVideoFrame,
};
use crate::{Error, Result};
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::APIBuilder;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection as WebRTCPeerConnection;
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
use webrtc::rtp_transceiver::rtp_receiver::RTCRtpReceiver;
use webrtc::rtp_transceiver::rtp_sender::RTCRtpSender;
use webrtc::rtp_transceiver::RTCRtpTransceiver;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;
use webrtc::track::track_local::TrackLocal;
use webrtc::track::track_remote::TrackRemote;

/// Peer connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Initial state, connection not yet started
    New,
    /// ICE candidates being gathered
    GatheringIce,
    /// Connection negotiation in progress
    Connecting,
    /// Connection established successfully
    Connected,
    /// Connection failed
    Failed,
    /// Connection closed
    Closed,
}

/// WebRTC peer connection wrapper
///
/// Wraps a webrtc::RTCPeerConnection and provides high-level management
/// for peer-to-peer connections.
pub struct PeerConnection {
    /// Unique identifier for remote peer
    peer_id: String,

    /// Unique identifier for this connection instance
    connection_id: String,

    /// Current connection state
    state: Arc<RwLock<ConnectionState>>,

    /// Actual WebRTC peer connection
    peer_connection: Arc<WebRTCPeerConnection>,

    /// Timestamp when connection was created
    created_at: SystemTime,

    /// Timestamp when connection was established
    connected_at: Arc<RwLock<Option<SystemTime>>>,

    /// Audio track for sending audio (if configured)
    audio_track: Arc<RwLock<Option<Arc<AudioTrack>>>>,

    /// Video track for sending video (if configured)
    video_track: Arc<RwLock<Option<Arc<VideoTrack>>>>,

    /// Audio RTP sender (retained to prevent track cleanup)
    audio_sender: Arc<RwLock<Option<Arc<RTCRtpSender>>>>,

    /// Video RTP sender (retained to prevent track cleanup)
    video_sender: Arc<RwLock<Option<Arc<RTCRtpSender>>>>,

    /// Synchronization manager for this peer (Phase 4 US2)
    sync_manager: Arc<RwLock<SyncManager>>,

    /// Data channel for reliable/unreliable messaging (Phase 7 US4)
    data_channel: Arc<RwLock<Option<DataChannel>>>,

    /// Reconnection manager for automatic recovery (Phase 8 US5)
    reconnection_manager: Arc<ReconnectionManager>,

    /// Connection quality metrics (Phase 8 US5)
    quality_metrics: Arc<RwLock<ConnectionQualityMetrics>>,

    /// Configuration stored for reconnection
    config: crate::config::WebRtcTransportConfig,
}

impl PeerConnection {
    /// Create a new peer connection
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Unique identifier for the remote peer
    /// * `config` - Configuration for STUN/TURN servers and codec preferences
    pub async fn new(
        peer_id: String,
        config: &crate::config::WebRtcTransportConfig,
    ) -> Result<Self> {
        let connection_id = uuid::Uuid::new_v4().to_string();

        info!(
            "Creating peer connection: peer_id={}, connection_id={}",
            peer_id, connection_id
        );

        // Create MediaEngine with default codecs
        let mut media_engine = MediaEngine::default();

        // Register default codecs (Opus for audio, VP8/VP9/H.264 for video)
        // Phase 5 T064-T066: VP8 and H.264 codecs are registered by webrtc-rs internally
        // AV1 support requires webrtc-rs v0.15+ (currently using v0.14)
        // Actual encoding/decoding delegated to core VideoEncoderNode/VideoDecoderNode
        media_engine
            .register_default_codecs()
            .map_err(|e| Error::WebRtcError(format!("Failed to register codecs: {}", e)))?;

        // Create InterceptorRegistry with default interceptors
        let interceptor_registry =
            register_default_interceptors(Default::default(), &mut media_engine).map_err(|e| {
                Error::WebRtcError(format!("Failed to register interceptors: {}", e))
            })?;

        // Build WebRTC API
        let api = APIBuilder::new()
            .with_media_engine(media_engine)
            .with_interceptor_registry(interceptor_registry)
            .build();

        // Configure ICE servers (STUN/TURN)
        let ice_servers: Vec<RTCIceServer> = config
            .stun_servers
            .iter()
            .map(|url| RTCIceServer {
                urls: vec![url.clone()],
                ..Default::default()
            })
            .chain(config.turn_servers.iter().map(|turn| {
                #[allow(clippy::needless_update)]
                RTCIceServer {
                    urls: vec![turn.url.clone()],
                    username: turn.username.clone(),
                    credential: turn.credential.clone(),
                    ..Default::default()
                }
            }))
            .collect();

        let rtc_config = RTCConfiguration {
            ice_servers,
            ..Default::default()
        };

        // Create peer connection
        let peer_connection =
            Arc::new(api.new_peer_connection(rtc_config).await.map_err(|e| {
                Error::WebRtcError(format!("Failed to create peer connection: {}", e))
            })?);

        let state = Arc::new(RwLock::new(ConnectionState::New));
        let connected_at = Arc::new(RwLock::new(None));

        // Set up connection state change handler
        let state_clone = Arc::clone(&state);
        let connected_at_clone = Arc::clone(&connected_at);
        let peer_id_clone = peer_id.clone();

        peer_connection.on_peer_connection_state_change(Box::new(
            move |s: RTCPeerConnectionState| {
                let state_clone = Arc::clone(&state_clone);
                let connected_at_clone = Arc::clone(&connected_at_clone);
                let peer_id = peer_id_clone.clone();

                Box::pin(async move {
                    let new_state = match s {
                        RTCPeerConnectionState::New => ConnectionState::New,
                        RTCPeerConnectionState::Connecting => ConnectionState::Connecting,
                        RTCPeerConnectionState::Connected => {
                            *connected_at_clone.write().await = Some(SystemTime::now());
                            ConnectionState::Connected
                        }
                        RTCPeerConnectionState::Disconnected | RTCPeerConnectionState::Closed => {
                            ConnectionState::Closed
                        }
                        RTCPeerConnectionState::Failed => ConnectionState::Failed,
                        _ => return,
                    };

                    let mut state_guard = state_clone.write().await;
                    let old_state = *state_guard;

                    if old_state != new_state {
                        debug!(
                            "Peer {} state transition: {:?} -> {:?}",
                            peer_id, old_state, new_state
                        );
                        *state_guard = new_state;
                    }
                })
            },
        ));

        // Initialize SyncManager for this peer (Phase 4 US2)
        let sync_config = SyncConfig::default();
        let sync_manager = SyncManager::new(peer_id.clone(), sync_config)
            .map_err(|e| Error::InvalidConfig(e.to_string()))?;

        // Initialize ReconnectionManager (Phase 8 US5)
        let reconnection_manager = Arc::new(ReconnectionManager::new(
            peer_id.clone(),
            ReconnectionPolicy::default(),
        ));

        // Initialize quality metrics (Phase 8 US5)
        let quality_metrics = Arc::new(RwLock::new(ConnectionQualityMetrics::new()));

        Ok(Self {
            peer_id,
            connection_id,
            state,
            peer_connection,
            created_at: SystemTime::now(),
            connected_at,
            audio_track: Arc::new(RwLock::new(None)),
            video_track: Arc::new(RwLock::new(None)),
            audio_sender: Arc::new(RwLock::new(None)),
            video_sender: Arc::new(RwLock::new(None)),
            sync_manager: Arc::new(RwLock::new(sync_manager)),
            data_channel: Arc::new(RwLock::new(None)),
            reconnection_manager,
            quality_metrics,
            config: config.clone(),
        })
    }

    /// Get the peer ID
    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    /// Get the connection ID
    pub fn connection_id(&self) -> &str {
        &self.connection_id
    }

    /// Get the current connection state
    pub async fn state(&self) -> ConnectionState {
        *self.state.read().await
    }

    /// Set the connection state
    async fn set_state(&self, new_state: ConnectionState) {
        let mut state = self.state.write().await;
        let old_state = *state;

        if old_state != new_state {
            debug!(
                "Peer {} state transition: {:?} -> {:?}",
                self.peer_id, old_state, new_state
            );
            *state = new_state;

            if new_state == ConnectionState::Connected {
                *self.connected_at.write().await = Some(SystemTime::now());
            }
        }
    }

    /// Create an SDP offer
    ///
    /// Generates a local SDP offer for initiating a connection.
    /// Returns the SDP string to be sent to the remote peer via signaling.
    pub async fn create_offer(&self) -> Result<String> {
        self.set_state(ConnectionState::GatheringIce).await;

        // Create SDP offer
        let offer = self
            .peer_connection
            .create_offer(None)
            .await
            .map_err(|e| Error::SdpError(format!("Failed to create offer: {}", e)))?;

        // Set local description
        self.peer_connection
            .set_local_description(offer)
            .await
            .map_err(|e| Error::SdpError(format!("Failed to set local description: {}", e)))?;

        // Get the SDP string
        let local_desc = self
            .peer_connection
            .local_description()
            .await
            .ok_or_else(|| {
                Error::SdpError("No local description after setting offer".to_string())
            })?;

        let sdp = local_desc.sdp.clone();

        debug!("Created SDP offer for peer {}", self.peer_id);

        Ok(sdp)
    }

    /// Create an SDP answer in response to an offer
    ///
    /// # Arguments
    ///
    /// * `offer_sdp` - The SDP offer received from the remote peer
    pub async fn create_answer(&self, offer_sdp: String) -> Result<String> {
        self.set_state(ConnectionState::GatheringIce).await;

        // Set remote description (the offer)
        let offer = RTCSessionDescription::offer(offer_sdp)
            .map_err(|e| Error::SdpError(format!("Failed to parse offer: {}", e)))?;

        self.peer_connection
            .set_remote_description(offer)
            .await
            .map_err(|e| Error::SdpError(format!("Failed to set remote description: {}", e)))?;

        // Create SDP answer
        let answer = self
            .peer_connection
            .create_answer(None)
            .await
            .map_err(|e| Error::SdpError(format!("Failed to create answer: {}", e)))?;

        // Set local description
        self.peer_connection
            .set_local_description(answer)
            .await
            .map_err(|e| Error::SdpError(format!("Failed to set local description: {}", e)))?;

        // Get the SDP string
        let local_desc = self
            .peer_connection
            .local_description()
            .await
            .ok_or_else(|| {
                Error::SdpError("No local description after setting answer".to_string())
            })?;

        let sdp = local_desc.sdp.clone();

        debug!("Created SDP answer for peer {}", self.peer_id);

        Ok(sdp)
    }

    /// Set the remote SDP description
    ///
    /// # Arguments
    ///
    /// * `sdp` - The SDP string (answer) from the remote peer
    pub async fn set_remote_description(&self, sdp: String) -> Result<()> {
        debug!("Setting remote description for peer {}", self.peer_id);

        // Assume it's an answer (since offers are handled in create_answer)
        let answer = RTCSessionDescription::answer(sdp)
            .map_err(|e| Error::SdpError(format!("Failed to parse answer: {}", e)))?;

        self.peer_connection
            .set_remote_description(answer)
            .await
            .map_err(|e| Error::SdpError(format!("Failed to set remote description: {}", e)))?;

        self.set_state(ConnectionState::Connecting).await;

        Ok(())
    }

    /// Add an ICE candidate
    ///
    /// # Arguments
    ///
    /// * `candidate` - ICE candidate JSON string from remote peer
    pub async fn add_ice_candidate(&self, candidate: String) -> Result<()> {
        debug!(
            "Adding ICE candidate for peer {}: {}",
            self.peer_id, candidate
        );

        // Parse the ICE candidate from JSON
        let ice_candidate: webrtc::ice_transport::ice_candidate::RTCIceCandidateInit =
            serde_json::from_str(&candidate).map_err(|e| {
                Error::IceCandidateError(format!("Failed to parse ICE candidate: {}", e))
            })?;

        self.peer_connection
            .add_ice_candidate(ice_candidate)
            .await
            .map_err(|e| Error::IceCandidateError(format!("Failed to add ICE candidate: {}", e)))?;

        Ok(())
    }

    /// Close the connection
    pub async fn close(&self) -> Result<()> {
        info!("Closing peer connection for peer {}", self.peer_id);

        self.set_state(ConnectionState::Closed).await;

        // Close the underlying RTCPeerConnection
        self.peer_connection.close().await.map_err(|e| {
            Error::PeerConnectionError(format!("Failed to close connection: {}", e))
        })?;

        Ok(())
    }

    /// Get connection duration (if connected)
    pub async fn connection_duration(&self) -> Option<std::time::Duration> {
        self.connected_at
            .read()
            .await
            .and_then(|connected_at| connected_at.elapsed().ok())
    }

    /// Add an audio track to this peer connection
    ///
    /// Creates a new audio track with Opus codec and adds it to the peer connection.
    /// The track will be used for sending audio data via RTP.
    ///
    /// # Arguments
    ///
    /// * `config` - Audio encoder configuration (sample rate, channels, bitrate)
    ///
    /// # Returns
    ///
    /// Returns the created AudioTrack wrapped in Arc for shared ownership.
    pub async fn add_audio_track(&self, config: AudioEncoderConfig) -> Result<Arc<AudioTrack>> {
        info!("Adding audio track to peer {}", self.peer_id);

        // Create TrackLocalStaticSample with Opus codec
        let track = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: "audio/opus".to_string(),
                clock_rate: config.sample_rate,
                channels: config.channels,
                sdp_fmtp_line: String::new(),
                rtcp_feedback: vec![],
            },
            format!("audio-{}", self.peer_id),
            format!("stream-{}", self.connection_id),
        ));

        // Add track to peer connection
        let sender = self
            .peer_connection
            .add_track(track.clone() as Arc<dyn TrackLocal + Send + Sync>)
            .await
            .map_err(|e| Error::MediaTrackError(format!("Failed to add audio track: {}", e)))?;

        // Create AudioTrack wrapper
        let audio_track = Arc::new(AudioTrack::new(track, config)?);

        // Store track and sender
        *self.audio_track.write().await = Some(audio_track.clone());
        *self.audio_sender.write().await = Some(sender);

        debug!("Audio track added to peer {}", self.peer_id);

        Ok(audio_track)
    }

    /// Add a video track to this peer connection
    ///
    /// Creates a new video track with VP9 codec and adds it to the peer connection.
    /// The track will be used for sending video data via RTP.
    ///
    /// # Arguments
    ///
    /// * `config` - Video encoder configuration (resolution, framerate, bitrate)
    ///
    /// # Returns
    ///
    /// Returns the created VideoTrack wrapped in Arc for shared ownership.
    pub async fn add_video_track(&self, config: VideoEncoderConfig) -> Result<Arc<VideoTrack>> {
        info!("Adding video track to peer {}", self.peer_id);

        // Create TrackLocalStaticSample with H.264 codec.
        //
        // Constrained Baseline (profile-level-id=42e01f, level 3.1) +
        // packetization-mode=1 is the universally-supported H.264
        // profile across Chrome / Firefox / Safari WebRTC stacks. The
        // browser will negotiate this against its own H.264 decoder
        // capabilities; on a mismatch register_default_codecs() leaves
        // VP8/VP9/AV1 as fallback offers.
        //
        // H.264 (instead of VP8) is what unlocks the NVENC encoder
        // path in `core::nodes::video::codec` — VP8 has no NVENC
        // equivalent so libvpx software encode is the only option.
        let track = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: "video/H264".to_string(),
                clock_rate: 90000, // Standard 90kHz clock for video
                channels: 0,       // Not used for video
                sdp_fmtp_line:
                    "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"
                        .to_string(),
                rtcp_feedback: vec![],
            },
            format!("video-{}", self.peer_id),
            format!("stream-{}", self.connection_id),
        ));

        // Add track to peer connection
        let sender = self
            .peer_connection
            .add_track(track.clone() as Arc<dyn TrackLocal + Send + Sync>)
            .await
            .map_err(|e| Error::MediaTrackError(format!("Failed to add video track: {}", e)))?;

        // Create VideoTrack wrapper with core encoder/decoder.
        // H.264 + hardware_accel=true (default) → NVENC on NVIDIA, with
        // libx264 ultrafast/zerolatency as fallback.
        let video_track = Arc::new(VideoTrack::new(
            track,
            remotemedia_core::data::video::VideoCodec::H264,
            config.width,
            config.height,
            config.bitrate,
            config.framerate,
        )?);

        // Store track and sender
        *self.video_track.write().await = Some(video_track.clone());
        *self.video_sender.write().await = Some(sender);

        debug!("Video track added to peer {}", self.peer_id);

        Ok(video_track)
    }

    /// Add a video track with a specific stream_id (Spec 013: US2 - Multiple Video Streams)
    ///
    /// Creates a new video track with a unique stream_id for multi-track support.
    /// Each call adds a new m=video section to the SDP when renegotiation occurs.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - Unique identifier for this video stream (e.g., "camera", "screen")
    /// * `config` - Video encoder configuration (resolution, framerate, bitrate)
    ///
    /// # Returns
    ///
    /// Returns the created VideoTrack wrapped in Arc for shared ownership.
    /// The track should be registered in a TrackRegistry for routing.
    ///
    /// # Note
    ///
    /// Unlike `add_video_track()`, this method does NOT store the track internally.
    /// The caller (ServerPeer/TrackRegistry) is responsible for track management.
    pub async fn add_video_track_with_stream_id(
        &self,
        stream_id: &str,
        config: VideoEncoderConfig,
    ) -> Result<(Arc<VideoTrack>, Arc<RTCRtpSender>)> {
        use crate::media::generate_track_id;

        info!(
            "Adding video track '{}' to peer {} (multi-track)",
            stream_id, self.peer_id
        );

        // Generate deterministic track ID from stream_id
        let track_id = generate_track_id("video", stream_id);

        // Create TrackLocalStaticSample with H.264 codec.
        // Constrained Baseline 3.1 — see `add_video_track` for rationale.
        let track = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: "video/H264".to_string(),
                clock_rate: 90000, // Standard 90kHz clock for video
                channels: 0,       // Not used for video
                sdp_fmtp_line:
                    "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"
                        .to_string(),
                rtcp_feedback: vec![],
            },
            track_id,
            format!("stream-{}-{}", self.connection_id, stream_id),
        ));

        // Add track to peer connection (creates new m=video in SDP)
        let sender = self
            .peer_connection
            .add_track(track.clone() as Arc<dyn TrackLocal + Send + Sync>)
            .await
            .map_err(|e| {
                Error::MediaTrackError(format!("Failed to add video track '{}': {}", stream_id, e))
            })?;

        // Create VideoTrack wrapper with core encoder/decoder.
        // H.264 → NVENC if available, libx264 ultrafast otherwise.
        let video_track = Arc::new(VideoTrack::new(
            track,
            remotemedia_core::data::video::VideoCodec::H264,
            config.width,
            config.height,
            config.bitrate,
            config.framerate,
        )?);

        debug!(
            "Video track '{}' added to peer {} (will appear as separate m=video in SDP)",
            stream_id, self.peer_id
        );

        Ok((video_track, sender))
    }

    /// Add an audio track with a specific stream_id (Spec 013: US3 - Multiple Audio Streams)
    ///
    /// Creates a new audio track with a unique stream_id for multi-track support.
    /// Each call adds a new m=audio section to the SDP when renegotiation occurs.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - Unique identifier for this audio stream (e.g., "voice", "music")
    /// * `config` - Audio encoder configuration
    ///
    /// # Returns
    ///
    /// Returns the created AudioTrack wrapped in Arc for shared ownership.
    pub async fn add_audio_track_with_stream_id(
        &self,
        stream_id: &str,
        config: AudioEncoderConfig,
    ) -> Result<(Arc<AudioTrack>, Arc<RTCRtpSender>)> {
        use crate::media::generate_track_id;

        info!(
            "Adding audio track '{}' to peer {} (multi-track)",
            stream_id, self.peer_id
        );

        // Generate deterministic track ID from stream_id
        let track_id = generate_track_id("audio", stream_id);

        // Create TrackLocalStaticSample with Opus codec
        let track = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: "audio/opus".to_string(),
                clock_rate: config.sample_rate,
                channels: config.channels,
                sdp_fmtp_line: String::new(),
                rtcp_feedback: vec![],
            },
            track_id,
            format!("stream-{}-{}", self.connection_id, stream_id),
        ));

        // Add track to peer connection (creates new m=audio in SDP)
        let sender = self
            .peer_connection
            .add_track(track.clone() as Arc<dyn TrackLocal + Send + Sync>)
            .await
            .map_err(|e| {
                Error::MediaTrackError(format!("Failed to add audio track '{}': {}", stream_id, e))
            })?;

        // Create AudioTrack wrapper
        let audio_track = Arc::new(AudioTrack::new(track, config)?);

        // Inbound mic decoding reuses a single `audio_track` slot on the
        // PeerConnection (see `on_track` handler in server_peer.rs which
        // calls `peer_connection.audio_track().await` to grab the decoder).
        // The single-track path (`add_audio_track`) populates that slot;
        // the multi-track path used to skip it, breaking inbound audio
        // any time the manifest declared a non-default outbound track.
        // Opus on the wire is 48 kHz mono regardless of stream_id, so any
        // of the multi-track AudioTrack wrappers can serve as the decoder
        // — populate the slot on the first call and leave subsequent
        // calls alone.
        {
            let mut slot = self.audio_track.write().await;
            if slot.is_none() {
                *slot = Some(audio_track.clone());
            }
        }

        debug!(
            "Audio track '{}' added to peer {} (will appear as separate m=audio in SDP)",
            stream_id, self.peer_id
        );

        Ok((audio_track, sender))
    }

    /// Get the audio track (if configured)
    pub async fn audio_track(&self) -> Option<Arc<AudioTrack>> {
        self.audio_track.read().await.clone()
    }

    /// Get the video track (if configured)
    pub async fn video_track(&self) -> Option<Arc<VideoTrack>> {
        self.video_track.read().await.clone()
    }

    /// Get the underlying WebRTC peer connection
    ///
    /// Provides access to the raw RTCPeerConnection for advanced operations.
    /// Used by ServerPeer for SDP handling and ICE candidates.
    pub fn peer_connection(&self) -> &Arc<WebRTCPeerConnection> {
        &self.peer_connection
    }

    // ========== Phase 7 (US4) Data Channel Integration ==========

    /// Add a data channel to this peer connection (T166)
    ///
    /// Creates a new data channel with the specified delivery mode.
    /// The data channel can be used for reliable or unreliable messaging
    /// between peers.
    ///
    /// # Arguments
    ///
    /// * `mode` - Delivery mode (Reliable or Unreliable)
    ///
    /// # Returns
    ///
    /// Result indicating success or error if channel creation fails.
    pub async fn add_data_channel(&self, mode: DataChannelMode) -> Result<()> {
        info!(
            "Adding data channel to peer {} with mode {:?}",
            self.peer_id, mode
        );

        // Check if we already have a data channel
        if self.data_channel.read().await.is_some() {
            return Err(Error::DataChannelError(
                "Data channel already exists for this peer".to_string(),
            ));
        }

        // Create the data channel
        let channel = DataChannel::new(
            &self.peer_connection,
            DataChannel::CONTROL_CHANNEL_LABEL,
            mode,
        )
        .await?;

        // Store the channel
        *self.data_channel.write().await = Some(channel);

        debug!("Data channel added to peer {}", self.peer_id);

        Ok(())
    }

    /// Get the data channel (if configured)
    pub async fn data_channel(&self) -> Option<DataChannel> {
        // Note: We can't clone DataChannel easily, so we return None here
        // and provide specific methods for data channel operations
        None
    }

    /// Check if data channel is configured
    pub async fn has_data_channel(&self) -> bool {
        self.data_channel.read().await.is_some()
    }

    /// Check if data channel is open and ready
    pub async fn is_data_channel_open(&self) -> bool {
        if let Some(ref channel) = *self.data_channel.read().await {
            channel.is_open().await
        } else {
            false
        }
    }

    /// Send a message over the data channel (T168)
    ///
    /// # Arguments
    ///
    /// * `msg` - Message to send
    ///
    /// # Returns
    ///
    /// Result indicating success or error.
    pub async fn send_data_channel_message(&self, msg: &DataChannelMessage) -> Result<()> {
        let channel_guard = self.data_channel.read().await;
        let channel = channel_guard.as_ref().ok_or_else(|| {
            Error::DataChannelError("No data channel configured for this peer".to_string())
        })?;

        channel.send_message(msg).await
    }

    /// Send raw binary data over the data channel
    ///
    /// # Arguments
    ///
    /// * `data` - Raw bytes to send
    pub async fn send_data_channel_binary(&self, data: &[u8]) -> Result<()> {
        let channel_guard = self.data_channel.read().await;
        let channel = channel_guard.as_ref().ok_or_else(|| {
            Error::DataChannelError("No data channel configured for this peer".to_string())
        })?;

        channel.send_binary(data).await
    }

    /// Set message handler for incoming data channel messages
    ///
    /// # Arguments
    ///
    /// * `handler` - Async callback for incoming messages
    pub async fn on_data_channel_message<F, Fut>(&self, handler: F) -> Result<()>
    where
        F: Fn(DataChannelMessage) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let channel_guard = self.data_channel.read().await;
        let channel = channel_guard.as_ref().ok_or_else(|| {
            Error::DataChannelError("No data channel configured for this peer".to_string())
        })?;

        channel.on_message(handler);
        Ok(())
    }

    /// Close the data channel
    pub async fn close_data_channel(&self) -> Result<()> {
        let channel_guard = self.data_channel.read().await;
        if let Some(ref channel) = *channel_guard {
            channel.close().await?;
        }
        Ok(())
    }

    /// Register a callback for when a remote track is added
    ///
    /// This handler fires when the remote peer adds a media track (audio/video).
    /// Used for receiving audio from client microphone for VAD, STT, etc.
    ///
    /// # Arguments
    ///
    /// * `handler` - Callback function that receives the remote track and RTP receiver
    ///
    /// # Example
    ///
    /// Register a callback to handle incoming media tracks.
    /// The callback receives the remote track, RTP receiver, and transceiver.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use std::pin::Pin;
    /// use std::future::Future;
    ///
    /// // Handler signature for on_track callback
    /// type TrackHandler = Box<dyn Fn(
    ///     Arc<webrtc::track::track_remote::TrackRemote>,
    ///     Arc<webrtc::rtp_transceiver::rtp_receiver::RTCRtpReceiver>,
    ///     Arc<webrtc::rtp_transceiver::RTCRtpTransceiver>,
    /// ) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;
    /// ```
    pub async fn on_track<F>(&self, handler: F)
    where
        F: Fn(
                Arc<TrackRemote>,
                Arc<RTCRtpReceiver>,
                Arc<RTCRtpTransceiver>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            + Send
            + Sync
            + 'static,
    {
        self.peer_connection.on_track(Box::new(handler));
    }

    // ========== Phase 4 (US2) Sync Integration ==========

    /// Process incoming audio frame through SyncManager (T114)
    ///
    /// Routes audio data through the jitter buffer and applies clock drift correction.
    ///
    /// # Arguments
    /// * `frame` - Audio frame with RTP metadata
    pub async fn process_audio_frame(&self, frame: SyncAudioFrame) -> Result<()> {
        let mut sync = self.sync_manager.write().await;
        sync.process_audio_frame(frame)
            .map_err(|e| Error::SyncError(e.to_string()))
    }

    /// Process incoming video frame through SyncManager (T115)
    ///
    /// Routes video data through the jitter buffer and calculates audio sync offset.
    ///
    /// # Arguments
    /// * `frame` - Video frame with RTP metadata
    pub async fn process_video_frame(&self, frame: SyncVideoFrame) -> Result<()> {
        let mut sync = self.sync_manager.write().await;
        sync.process_video_frame(frame)
            .map_err(|e| Error::SyncError(e.to_string()))
    }

    /// Update synchronization with RTCP Sender Report (T116)
    ///
    /// Updates NTP/RTP mapping for timestamp correlation and clock drift estimation.
    ///
    /// # Arguments
    /// * `sr` - RTCP Sender Report data
    /// * `is_audio` - Whether this SR is for audio (true) or video (false)
    pub async fn update_rtcp_sender_report(&self, sr: RtcpSenderReport, is_audio: bool) {
        let mut sync = self.sync_manager.write().await;
        sync.update_rtcp_sender_report(sr, is_audio);
    }

    /// Pop next synchronized audio frame for playback
    ///
    /// Returns frame if buffer delay has elapsed, with sync metadata.
    pub async fn pop_synced_audio_frame(&self) -> Option<SyncedAudioFrame> {
        let mut sync = self.sync_manager.write().await;
        sync.pop_next_audio_frame()
    }

    /// Pop next synchronized video frame for display
    ///
    /// Returns frame with audio sync offset for lip-sync.
    pub async fn pop_synced_video_frame(&self) -> Option<SyncedVideoFrame> {
        let mut sync = self.sync_manager.write().await;
        sync.pop_next_video_frame()
    }

    /// Get current synchronization state
    pub async fn sync_state(&self) -> SyncState {
        let sync = self.sync_manager.read().await;
        sync.get_sync_state()
    }

    /// Get the SyncManager for advanced operations
    pub fn sync_manager(&self) -> &Arc<RwLock<SyncManager>> {
        &self.sync_manager
    }

    /// Reset synchronization state
    ///
    /// Clears jitter buffers, clock drift data, and NTP mappings.
    pub async fn reset_sync(&self) {
        let mut sync = self.sync_manager.write().await;
        sync.reset();
    }

    // ========== Phase 8 (US5) Reconnection & Quality Monitoring ==========

    /// Attempt to reconnect the peer connection (T172)
    ///
    /// Resets ICE, creates a new offer, and attempts to re-establish the connection.
    /// Uses exponential backoff with circuit breaker protection.
    ///
    /// # Returns
    /// Ok(()) if reconnection succeeded, Err if failed or circuit breaker blocked
    pub async fn reconnect(&self) -> Result<()> {
        // Check if reconnection should be attempted
        if !self.reconnection_manager.should_attempt_reconnect().await {
            return Err(Error::PeerConnectionError(
                "Reconnection blocked: max retries exceeded or circuit breaker open".to_string(),
            ));
        }

        info!("Attempting reconnection for peer {}", self.peer_id);

        // Start reconnection process (includes backoff delay)
        self.reconnection_manager.start_reconnection().await;

        // Reset ICE connection
        match self.reset_ice_connection().await {
            Ok(_) => {
                // Create new offer
                match self.create_offer().await {
                    Ok(_sdp) => {
                        info!("Reconnection offer created for peer {}", self.peer_id);
                        self.reconnection_manager.report_success().await;
                        Ok(())
                    }
                    Err(e) => {
                        warn!(
                            "Reconnection offer creation failed for peer {}: {}",
                            self.peer_id, e
                        );
                        self.reconnection_manager.report_failure().await;
                        Err(e)
                    }
                }
            }
            Err(e) => {
                warn!("ICE reset failed for peer {}: {}", self.peer_id, e);
                self.reconnection_manager.report_failure().await;
                Err(e)
            }
        }
    }

    /// Reset ICE connection state (helper for reconnection)
    async fn reset_ice_connection(&self) -> Result<()> {
        // Restart ICE by calling restartIce on the peer connection
        // Note: webrtc-rs may require creating a new offer with ICE restart option
        self.peer_connection
            .create_offer(Some(
                webrtc::peer_connection::offer_answer_options::RTCOfferOptions {
                    ice_restart: true,
                    ..Default::default()
                },
            ))
            .await
            .map_err(|e| Error::IceCandidateError(format!("ICE restart failed: {}", e)))?;

        debug!("ICE connection reset for peer {}", self.peer_id);
        Ok(())
    }

    /// Get current reconnection state
    pub async fn reconnection_state(&self) -> ReconnectionState {
        self.reconnection_manager.state().await
    }

    /// Check if reconnection should be attempted based on current state
    pub async fn should_reconnect(&self) -> bool {
        let state = self.state().await;
        matches!(state, ConnectionState::Failed | ConnectionState::Closed)
            && self.reconnection_manager.should_attempt_reconnect().await
    }

    /// Get the reconnection manager for advanced control
    pub fn reconnection_manager(&self) -> &Arc<ReconnectionManager> {
        &self.reconnection_manager
    }

    /// Get connection quality metrics (T184)
    pub async fn get_metrics(&self) -> ConnectionQualityMetrics {
        self.quality_metrics.read().await.clone()
    }

    /// Update quality metrics from RTCP Receiver Report (T182)
    ///
    /// # Arguments
    /// * `packet_loss_rate` - Fraction of packets lost (0.0 - 1.0)
    /// * `jitter_ms` - Interarrival jitter in milliseconds
    /// * `rtt_ms` - Round-trip time in milliseconds (if available)
    pub async fn update_rtcp_receiver_report(
        &self,
        packet_loss_rate: f64,
        jitter_ms: f64,
        rtt_ms: Option<f64>,
    ) {
        let mut metrics = self.quality_metrics.write().await;
        metrics.packet_loss_rate = packet_loss_rate;
        metrics.jitter_ms = jitter_ms;
        if let Some(rtt) = rtt_ms {
            metrics.latency_ms = rtt;
        }
        metrics.updated_at = Instant::now();

        debug!(
            "Peer {} quality metrics updated: loss={:.2}%, jitter={:.1}ms, rtt={:.1}ms",
            self.peer_id,
            packet_loss_rate * 100.0,
            jitter_ms,
            metrics.latency_ms
        );

        // Check if adaptive bitrate adjustment is needed (T185)
        if metrics.should_reduce_bitrate() {
            warn!(
                "Peer {} quality degraded, bitrate reduction recommended",
                self.peer_id
            );
        }
    }

    /// Update packet statistics for quality metrics
    pub async fn update_packet_stats(
        &self,
        packets_sent: u64,
        packets_received: u64,
        packets_lost: u64,
    ) {
        let mut metrics = self.quality_metrics.write().await;
        metrics.packets_sent = packets_sent;
        metrics.packets_received = packets_received;
        metrics.packets_lost = packets_lost;

        // Calculate packet loss rate
        let total = packets_received + packets_lost;
        if total > 0 {
            metrics.packet_loss_rate = packets_lost as f64 / total as f64;
        }

        metrics.updated_at = Instant::now();
    }

    /// Update video quality metrics
    pub async fn update_video_metrics(
        &self,
        width: u32,
        height: u32,
        framerate: f32,
        bitrate_kbps: u32,
    ) {
        let mut metrics = self.quality_metrics.write().await;
        metrics.video_width = width;
        metrics.video_height = height;
        metrics.video_framerate = framerate;
        metrics.video_bitrate_kbps = bitrate_kbps;
        metrics.updated_at = Instant::now();
    }

    /// Update audio quality metrics
    pub async fn update_audio_metrics(&self, bitrate_kbps: u32) {
        let mut metrics = self.quality_metrics.write().await;
        metrics.audio_bitrate_kbps = bitrate_kbps;
        metrics.updated_at = Instant::now();
    }

    /// Get connection quality score (0-100)
    pub async fn quality_score(&self) -> u32 {
        self.quality_metrics.read().await.quality_score()
    }

    /// Check if connection quality is acceptable
    pub async fn is_quality_acceptable(&self) -> bool {
        self.quality_metrics.read().await.is_acceptable()
    }

    /// Check if bitrate should be reduced based on quality metrics
    pub async fn should_reduce_bitrate(&self) -> bool {
        self.quality_metrics.read().await.should_reduce_bitrate()
    }

    /// Check if bitrate can be increased based on quality metrics
    pub async fn can_increase_bitrate(&self) -> bool {
        self.quality_metrics.read().await.can_increase_bitrate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WebRtcTransportConfig;

    #[tokio::test]
    async fn test_peer_connection_creation() {
        let config = WebRtcTransportConfig::default();
        let pc = PeerConnection::new("peer-test".to_string(), &config)
            .await
            .unwrap();

        assert_eq!(pc.peer_id(), "peer-test");
        assert_eq!(pc.state().await, ConnectionState::New);
        assert!(pc.audio_track().await.is_none());
        assert!(pc.video_track().await.is_none());
    }

    #[tokio::test]
    async fn test_create_offer() {
        let config = WebRtcTransportConfig::default();
        let pc = PeerConnection::new("peer-test".to_string(), &config)
            .await
            .unwrap();

        let sdp = pc.create_offer().await.unwrap();
        assert!(!sdp.is_empty());
        // Note: State transitions are handled asynchronously by webrtc callbacks
    }

    #[tokio::test]
    async fn test_add_audio_track() {
        let config = WebRtcTransportConfig::default();
        let pc = PeerConnection::new("peer-test".to_string(), &config)
            .await
            .unwrap();

        let audio_config = AudioEncoderConfig::default();
        let track = pc.add_audio_track(audio_config).await.unwrap();

        assert!(pc.audio_track().await.is_some());
        assert!(Arc::ptr_eq(&track, &pc.audio_track().await.unwrap()));
    }

    #[tokio::test]
    async fn test_add_video_track() {
        let config = WebRtcTransportConfig::default();
        let pc = PeerConnection::new("peer-test".to_string(), &config)
            .await
            .unwrap();

        let video_config = VideoEncoderConfig::default();
        let track = pc.add_video_track(video_config).await.unwrap();

        assert!(pc.video_track().await.is_some());
        assert!(Arc::ptr_eq(&track, &pc.video_track().await.unwrap()));
    }

    #[tokio::test]
    async fn test_add_both_tracks() {
        let config = WebRtcTransportConfig::default();
        let pc = PeerConnection::new("peer-test".to_string(), &config)
            .await
            .unwrap();

        // Add audio track
        let audio_config = AudioEncoderConfig::default();
        pc.add_audio_track(audio_config).await.unwrap();

        // Add video track
        let video_config = VideoEncoderConfig::default();
        pc.add_video_track(video_config).await.unwrap();

        // Verify both tracks are present
        assert!(pc.audio_track().await.is_some());
        assert!(pc.video_track().await.is_some());
    }

    #[tokio::test]
    async fn test_create_offer_with_tracks() {
        let config = WebRtcTransportConfig::default();
        let pc = PeerConnection::new("peer-test".to_string(), &config)
            .await
            .unwrap();

        // Add audio track before creating offer
        let audio_config = AudioEncoderConfig::default();
        pc.add_audio_track(audio_config).await.unwrap();

        // Create offer - should include audio track in SDP
        let sdp = pc.create_offer().await.unwrap();
        assert!(!sdp.is_empty());
        assert!(sdp.contains("audio"));
    }

    #[tokio::test]
    async fn test_close() {
        let config = WebRtcTransportConfig::default();
        let pc = PeerConnection::new("peer-test".to_string(), &config)
            .await
            .unwrap();

        pc.close().await.unwrap();
        // State will eventually become Closed via callback
    }

    #[tokio::test]
    async fn test_add_data_channel() {
        let config = WebRtcTransportConfig::default();
        let pc = PeerConnection::new("peer-test".to_string(), &config)
            .await
            .unwrap();

        // Initially no data channel
        assert!(!pc.has_data_channel().await);

        // Add data channel
        pc.add_data_channel(DataChannelMode::Reliable)
            .await
            .unwrap();

        // Now has data channel
        assert!(pc.has_data_channel().await);
    }

    #[tokio::test]
    async fn test_add_data_channel_duplicate_fails() {
        let config = WebRtcTransportConfig::default();
        let pc = PeerConnection::new("peer-test".to_string(), &config)
            .await
            .unwrap();

        // Add first data channel
        pc.add_data_channel(DataChannelMode::Reliable)
            .await
            .unwrap();

        // Adding second should fail
        let result = pc.add_data_channel(DataChannelMode::Unreliable).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_data_channel_with_offer() {
        let config = WebRtcTransportConfig::default();
        let pc = PeerConnection::new("peer-test".to_string(), &config)
            .await
            .unwrap();

        // Add data channel before creating offer
        pc.add_data_channel(DataChannelMode::Reliable)
            .await
            .unwrap();

        // Create offer - should include data channel in SDP
        let sdp = pc.create_offer().await.unwrap();
        assert!(!sdp.is_empty());
        // SDP should contain application media section for data channel
        assert!(sdp.contains("application"));
    }
}
