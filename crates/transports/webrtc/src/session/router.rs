//! Session router for WebRTC pipeline integration
//!
//! Routes data between WebRTC peers and pipeline execution.

// Public API types - fields and methods used by library consumers, not internally
#![allow(dead_code)]

use crate::channels::{DataChannelMessage, DataChannelSplitter, MAX_CHUNK_SIZE};
use crate::media::tracks::rtp_to_runtime_data;
use crate::peer::PeerManager;
use crate::session::{Session, SessionId};
use crate::sync::RtpHeader;
use crate::{Error, Result};

use remotemedia_core::data::video::PixelFormat;
use remotemedia_core::data::RuntimeData;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, trace, warn};

/// Default capacity of each per-peer input channel (client → router).
///
/// Small by design: this is the transport-level backpressure surface
/// toward the WebRTC peer. Override via
/// `REMOTEMEDIA_WEBRTC_PEER_INPUT_CAPACITY`.
pub const DEFAULT_WEBRTC_PEER_INPUT_CAPACITY: usize = 8;

/// Default capacity of the shared router output channel (nodes → peers).
///
/// Larger than peer-input because outputs are fanned to multiple peers
/// in broadcast mode. Override via
/// `REMOTEMEDIA_WEBRTC_OUTPUT_CAPACITY`.
pub const DEFAULT_WEBRTC_OUTPUT_CAPACITY: usize = 64;

fn webrtc_peer_input_capacity() -> usize {
    std::env::var("REMOTEMEDIA_WEBRTC_PEER_INPUT_CAPACITY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_WEBRTC_PEER_INPUT_CAPACITY)
}

fn webrtc_output_capacity() -> usize {
    std::env::var("REMOTEMEDIA_WEBRTC_OUTPUT_CAPACITY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_WEBRTC_OUTPUT_CAPACITY)
}

// ============================================================================
// Phase 6 (US3) Routing Types (T148-T151)
// ============================================================================

/// Routing policy for data distribution (T148)
///
/// Defines how data is routed to peers in a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RoutingPolicy {
    /// Send to a single specific peer
    Unicast,
    /// Send to all connected peers (default)
    #[default]
    Broadcast,
    /// Send to specific peers based on configured routes
    Selective,
}

impl RoutingPolicy {
    /// Check if this is unicast routing
    pub fn is_unicast(&self) -> bool {
        matches!(self, Self::Unicast)
    }

    /// Check if this is broadcast routing
    pub fn is_broadcast(&self) -> bool {
        matches!(self, Self::Broadcast)
    }

    /// Check if this is selective routing
    pub fn is_selective(&self) -> bool {
        matches!(self, Self::Selective)
    }
}

/// Quality tier for adaptive streaming (T149, T153)
///
/// Represents different quality levels for video streaming.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum QualityTier {
    /// High quality (1080p, high bitrate)
    High,
    /// Medium quality (720p, medium bitrate)
    #[default]
    Medium,
    /// Low quality (480p, low bitrate)
    Low,
    /// Custom tier with specific settings
    Custom(String),
}

impl QualityTier {
    /// Get recommended bitrate in kbps for this tier
    pub fn recommended_bitrate_kbps(&self) -> u32 {
        match self {
            QualityTier::High => 4000,
            QualityTier::Medium => 1500,
            QualityTier::Low => 500,
            QualityTier::Custom(_) => 1500, // Default to medium
        }
    }

    /// Get recommended resolution for this tier
    pub fn recommended_resolution(&self) -> (u32, u32) {
        match self {
            QualityTier::High => (1920, 1080),
            QualityTier::Medium => (1280, 720),
            QualityTier::Low => (854, 480),
            QualityTier::Custom(_) => (1280, 720), // Default to medium
        }
    }
}

/// Output route configuration for selective routing (T149)
///
/// Maps pipeline outputs to specific peers with optional quality tiers.
#[derive(Debug, Clone)]
pub struct OutputRoute {
    /// Output identifier (pipeline output node ID)
    pub output_id: String,
    /// Target peer IDs for this output
    pub target_peers: Vec<String>,
    /// Quality tier for this route (affects encoding parameters)
    pub quality_tier: Option<QualityTier>,
    /// Priority (higher = more important, affects bandwidth allocation)
    pub priority: u8,
    /// Whether this route is currently active
    pub active: bool,
}

impl OutputRoute {
    /// Create a new output route
    pub fn new(output_id: impl Into<String>, target_peers: Vec<String>) -> Self {
        Self {
            output_id: output_id.into(),
            target_peers,
            quality_tier: None,
            priority: 5, // Default medium priority
            active: true,
        }
    }

    /// Create a broadcast route (all peers)
    pub fn broadcast(output_id: impl Into<String>) -> Self {
        Self {
            output_id: output_id.into(),
            target_peers: Vec::new(), // Empty = all peers
            quality_tier: None,
            priority: 5,
            active: true,
        }
    }

    /// Set quality tier for this route
    pub fn with_quality(mut self, tier: QualityTier) -> Self {
        self.quality_tier = Some(tier);
        self
    }

    /// Set priority for this route
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Check if this route targets all peers (broadcast)
    pub fn is_broadcast(&self) -> bool {
        self.target_peers.is_empty()
    }

    /// Check if a peer is targeted by this route
    pub fn targets_peer(&self, peer_id: &str) -> bool {
        self.target_peers.is_empty() || self.target_peers.iter().any(|p| p == peer_id)
    }
}

/// Bitrate adaptation recommendation (T156)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BitrateAction {
    /// Maintain current bitrate
    Maintain,
    /// Increase bitrate (good conditions)
    Increase,
    /// Decrease bitrate (poor conditions)
    Decrease,
    /// Pause sending (severe congestion)
    Pause,
}

/// Per-peer bitrate adaptation state (T156)
#[derive(Debug, Clone)]
pub struct PeerBitrateState {
    /// Current target bitrate in kbps
    pub current_bitrate_kbps: u32,
    /// Minimum bitrate in kbps
    pub min_bitrate_kbps: u32,
    /// Maximum bitrate in kbps
    pub max_bitrate_kbps: u32,
    /// Recent packet loss rate (0.0 - 1.0)
    pub packet_loss_rate: f32,
    /// Recent RTT in milliseconds
    pub rtt_ms: u32,
    /// Last adaptation action
    pub last_action: BitrateAction,
    /// Timestamp of last adaptation
    pub last_adaptation_ms: u64,
}

impl Default for PeerBitrateState {
    fn default() -> Self {
        Self {
            current_bitrate_kbps: 1500,
            min_bitrate_kbps: 200,
            max_bitrate_kbps: 4000,
            packet_loss_rate: 0.0,
            rtt_ms: 0,
            last_action: BitrateAction::Maintain,
            last_adaptation_ms: 0,
        }
    }
}

impl PeerBitrateState {
    /// Create with specific bitrate range
    pub fn with_range(min_kbps: u32, max_kbps: u32) -> Self {
        Self {
            current_bitrate_kbps: (min_kbps + max_kbps) / 2,
            min_bitrate_kbps: min_kbps,
            max_bitrate_kbps: max_kbps,
            ..Default::default()
        }
    }

    /// Update metrics and recommend bitrate action (T156)
    pub fn update_metrics(&mut self, packet_loss: f32, rtt_ms: u32) -> BitrateAction {
        self.packet_loss_rate = packet_loss;
        self.rtt_ms = rtt_ms;

        let action = if packet_loss > 0.1 || rtt_ms > 500 {
            // High loss or latency - decrease bitrate
            BitrateAction::Decrease
        } else if packet_loss > 0.05 || rtt_ms > 200 {
            // Moderate issues - maintain current
            BitrateAction::Maintain
        } else if packet_loss < 0.01 && rtt_ms < 100 {
            // Excellent conditions - can increase
            BitrateAction::Increase
        } else {
            BitrateAction::Maintain
        };

        self.last_action = action;
        self.last_adaptation_ms = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        action
    }

    /// Apply the recommended action
    pub fn apply_action(&mut self, action: BitrateAction) {
        match action {
            BitrateAction::Increase => {
                let new_bitrate = (self.current_bitrate_kbps as f32 * 1.2) as u32;
                self.current_bitrate_kbps = new_bitrate.min(self.max_bitrate_kbps);
            }
            BitrateAction::Decrease => {
                let new_bitrate = (self.current_bitrate_kbps as f32 * 0.8) as u32;
                self.current_bitrate_kbps = new_bitrate.max(self.min_bitrate_kbps);
            }
            BitrateAction::Pause | BitrateAction::Maintain => {}
        }
    }

    /// Check if bitrate should be reduced based on current metrics
    pub fn should_reduce_bitrate(&self) -> bool {
        self.packet_loss_rate > 0.05 || self.rtt_ms > 300
    }

    /// Check if bitrate can be increased based on current metrics
    pub fn can_increase_bitrate(&self) -> bool {
        self.packet_loss_rate < 0.01
            && self.rtt_ms < 100
            && self.current_bitrate_kbps < self.max_bitrate_kbps
    }
}

// ============================================================================
// Phase 8 (US5) Session Recovery Types (T179-T180)
// ============================================================================

/// Session state snapshot for recovery during reconnection (T179)
#[derive(Debug, Clone)]
pub struct SessionStateSnapshot {
    /// Session ID
    pub session_id: SessionId,
    /// Connected peer IDs at time of snapshot
    pub peer_ids: Vec<String>,
    /// Timestamp when snapshot was taken
    pub timestamp: SystemTime,
    /// Pipeline state (serialized manifest or config)
    pub pipeline_state: Option<String>,
    /// Custom metadata
    pub metadata: HashMap<String, String>,
}

impl SessionStateSnapshot {
    /// Create a new snapshot
    pub fn new(session_id: SessionId) -> Self {
        Self {
            session_id,
            peer_ids: Vec::new(),
            timestamp: SystemTime::now(),
            pipeline_state: None,
            metadata: HashMap::new(),
        }
    }

    /// Age of the snapshot
    pub fn age(&self) -> std::time::Duration {
        self.timestamp.elapsed().unwrap_or_default()
    }
}

/// Reconnection event for session recovery notification (T180)
#[derive(Debug, Clone)]
pub struct ReconnectionEvent {
    /// Peer ID that reconnected
    pub peer_id: String,
    /// Session ID
    pub session_id: SessionId,
    /// Time of reconnection
    pub timestamp: Instant,
    /// Previous connection duration (if known)
    pub previous_duration: Option<std::time::Duration>,
    /// Whether this is a full reconnect (new connection) or ICE restart
    pub is_full_reconnect: bool,
}

/// Callback type for reconnection notifications
pub type ReconnectionCallback = Box<
    dyn Fn(ReconnectionEvent) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

// ============================================================================
// Session Metrics (T200)
// ============================================================================

/// Metrics for session routing performance
#[derive(Debug, Clone, Default)]
pub struct SessionMetrics {
    /// Total audio frames routed
    pub audio_frames_in: u64,
    /// Total audio frames sent out
    pub audio_frames_out: u64,
    /// Total video frames routed
    pub video_frames_in: u64,
    /// Total video frames sent out
    pub video_frames_out: u64,
    /// Total errors during routing
    pub routing_errors: u64,
    /// Total bytes received
    pub bytes_received: u64,
    /// Total bytes sent
    pub bytes_sent: u64,
    /// Average routing latency (microseconds)
    pub avg_latency_us: u64,
    /// Maximum routing latency (microseconds)
    pub max_latency_us: u64,
    /// Timestamp of last activity
    pub last_activity_ms: u64,
}

impl SessionMetrics {
    /// Create new metrics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an incoming audio frame
    pub fn record_audio_in(&mut self, bytes: usize) {
        self.audio_frames_in += 1;
        self.bytes_received += bytes as u64;
        self.update_activity();
    }

    /// Record an outgoing audio frame
    pub fn record_audio_out(&mut self, bytes: usize) {
        self.audio_frames_out += 1;
        self.bytes_sent += bytes as u64;
        self.update_activity();
    }

    /// Record an incoming video frame
    pub fn record_video_in(&mut self, bytes: usize) {
        self.video_frames_in += 1;
        self.bytes_received += bytes as u64;
        self.update_activity();
    }

    /// Record an outgoing video frame
    pub fn record_video_out(&mut self, bytes: usize) {
        self.video_frames_out += 1;
        self.bytes_sent += bytes as u64;
        self.update_activity();
    }

    /// Record a routing error
    pub fn record_error(&mut self) {
        self.routing_errors += 1;
    }

    /// Record routing latency
    pub fn record_latency(&mut self, latency_us: u64) {
        // Update max
        if latency_us > self.max_latency_us {
            self.max_latency_us = latency_us;
        }
        // Update average (exponential moving average)
        if self.avg_latency_us == 0 {
            self.avg_latency_us = latency_us;
        } else {
            // EMA with alpha = 0.1
            self.avg_latency_us = (self.avg_latency_us * 9 + latency_us) / 10;
        }
    }

    fn update_activity(&mut self) {
        self.last_activity_ms = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
    }

    /// Get total frames (in + out)
    pub fn total_frames(&self) -> u64 {
        self.audio_frames_in + self.audio_frames_out + self.video_frames_in + self.video_frames_out
    }

    /// Get total bytes (received + sent)
    pub fn total_bytes(&self) -> u64 {
        self.bytes_received + self.bytes_sent
    }
}

/// Router for session data flow
///
/// Routes incoming RTP data from peers to application layer, and outgoing
/// application data back to target peers.
///
/// Note: This router does NOT run the pipeline directly - it's a routing
/// layer between WebRTC peers and the application (which uses PipelineTransport).
pub struct SessionRouter {
    /// Session ID
    session_id: SessionId,

    /// Associated session
    session: Arc<Session>,

    /// Peer manager for WebRTC connections
    peer_manager: Arc<PeerManager>,

    /// Per-peer bounded input channels (peer_id -> sender).
    ///
    /// Bounded at [`DEFAULT_WEBRTC_PEER_INPUT_CAPACITY`]. Sends `.await`
    /// so the peer ingest path applies real backpressure to the
    /// WebRTC stream when the session router is behind.
    peer_inputs: Arc<RwLock<HashMap<String, mpsc::Sender<RuntimeData>>>>,

    /// Shared bounded output channel for all nodes. Sized larger than a
    /// single peer's input because it fans out to multiple peers in
    /// broadcast mode. See [`DEFAULT_WEBRTC_OUTPUT_CAPACITY`].
    output_tx: mpsc::Sender<RuntimeData>,

    /// Shared output receiver
    output_rx: Arc<RwLock<mpsc::Receiver<RuntimeData>>>,

    /// Shutdown signal
    shutdown_tx: mpsc::Sender<()>,
    shutdown_rx: Arc<RwLock<mpsc::Receiver<()>>>,

    // ========== Phase 6 (US3) Selective Routing (T150-T156) ==========
    /// Routing policy for this session (T150)
    routing_policy: Arc<RwLock<RoutingPolicy>>,

    /// Configured output routes for selective routing (T151)
    output_routes: Arc<RwLock<Vec<OutputRoute>>>,

    /// Per-peer bitrate adaptation state (T156)
    peer_bitrate_states: Arc<RwLock<HashMap<String, PeerBitrateState>>>,

    // ========== Phase 8 (US5) Session Recovery ==========
    /// Latest state snapshot for recovery (T179)
    state_snapshot: Arc<RwLock<Option<SessionStateSnapshot>>>,

    /// Reconnection event callback (T180)
    reconnection_callback: Arc<RwLock<Option<ReconnectionCallback>>>,

    /// Session metrics (T200)
    metrics: Arc<RwLock<SessionMetrics>>,
}

impl SessionRouter {
    /// Create a new session router
    ///
    /// # Arguments
    ///
    /// * `session_id` - Unique session identifier
    /// * `session` - Session handle
    /// * `peer_manager` - Peer manager for WebRTC connections
    pub fn new(
        session_id: SessionId,
        session: Arc<Session>,
        peer_manager: Arc<PeerManager>,
    ) -> Result<Self> {
        info!("Creating session router for session: {}", session_id);

        let (output_tx, output_rx) = mpsc::channel(webrtc_output_capacity());
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        Ok(Self {
            session_id,
            session,
            peer_manager,
            peer_inputs: Arc::new(RwLock::new(HashMap::new())),
            output_tx,
            output_rx: Arc::new(RwLock::new(output_rx)),
            shutdown_tx,
            shutdown_rx: Arc::new(RwLock::new(shutdown_rx)),
            // Phase 6 routing
            routing_policy: Arc::new(RwLock::new(RoutingPolicy::default())),
            output_routes: Arc::new(RwLock::new(Vec::new())),
            peer_bitrate_states: Arc::new(RwLock::new(HashMap::new())),
            // Phase 8 recovery
            state_snapshot: Arc::new(RwLock::new(None)),
            reconnection_callback: Arc::new(RwLock::new(None)),
            metrics: Arc::new(RwLock::new(SessionMetrics::new())),
        })
    }

    /// Start the routing task
    ///
    /// Spawns a background task that continuously routes data between
    /// peers.
    pub async fn start(self: Arc<Self>) -> Result<()> {
        info!("Starting session router for: {}", self.session_id);

        // Spawn routing task
        let router = Arc::clone(&self);
        tokio::spawn(async move {
            if let Err(e) = router.run_routing_loop().await {
                error!("Session router error for {}: {}", router.session_id, e);
            }
        });

        Ok(())
    }

    /// Main routing loop
    async fn run_routing_loop(&self) -> Result<()> {
        debug!("Session router loop started for: {}", self.session_id);

        let mut shutdown_rx = self.shutdown_rx.write().await;

        loop {
            tokio::select! {
                // Handle shutdown signal
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received for session: {}", self.session_id);
                    break;
                }

                // Handle output to send to peers
                Some(output) = self.recv_output_internal() => {
                    if let Err(e) = self.route_outgoing(output).await {
                        error!("Failed to route outgoing data: {}", e);
                    }
                }
            }
        }

        info!("Session router stopped for: {}", self.session_id);
        Ok(())
    }

    /// Route incoming RTP data from peer (T126)
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Peer ID that sent the data
    /// * `payload` - RTP payload (encoded audio or video)
    /// * `is_audio` - True for audio, false for video
    pub async fn route_incoming(
        &self,
        peer_id: &str,
        payload: &[u8],
        is_audio: bool,
    ) -> Result<RuntimeData> {
        let start = Instant::now();

        // Parse RTP header for debugging and synchronization
        if let Some(rtp_header) = RtpHeader::parse(payload) {
            trace!(
                "RTP packet: seq={}, ts={}, ssrc={}, pt={}, marker={}",
                rtp_header.sequence_number,
                rtp_header.timestamp,
                rtp_header.ssrc,
                rtp_header.payload_type,
                rtp_header.marker
            );
        }

        debug!(
            "Routing incoming {} data from peer {} (session: {})",
            if is_audio { "audio" } else { "video" },
            peer_id,
            self.session_id
        );

        // Get peer connection
        let peer = self.peer_manager.get_peer(peer_id).await?;

        // Convert RTP to RuntimeData
        let runtime_data =
            if is_audio {
                let audio_track = peer.audio_track().await.ok_or_else(|| {
                    Error::MediaTrackError("No audio track configured".to_string())
                })?;
                rtp_to_runtime_data(payload, true, Some(&audio_track), None).await?
            } else {
                let video_track = peer.video_track().await.ok_or_else(|| {
                    Error::MediaTrackError("No video track configured".to_string())
                })?;
                rtp_to_runtime_data(payload, false, None, Some(&video_track)).await?
            };

        // Record metrics
        {
            let mut metrics = self.metrics.write().await;
            if is_audio {
                metrics.record_audio_in(payload.len());
            } else {
                metrics.record_video_in(payload.len());
            }
            metrics.record_latency(start.elapsed().as_micros() as u64);
        }

        Ok(runtime_data)
    }

    /// Route outgoing data to peers (T127)
    ///
    /// # Arguments
    ///
    /// * `data` - RuntimeData to send
    async fn route_outgoing(&self, data: RuntimeData) -> Result<()> {
        debug!("Routing outgoing data (session: {})", self.session_id);

        // Get all peers in this session
        let peer_ids = self.session.list_peers().await;

        if peer_ids.is_empty() {
            warn!(
                "No peers connected to session {}, dropping output",
                self.session_id
            );
            return Ok(());
        }

        // Broadcast to all peers
        for peer_id in peer_ids {
            if let Err(e) = self.send_to_peer(&peer_id, &data).await {
                error!("Failed to send to peer {}: {}", peer_id, e);
            }
        }

        Ok(())
    }

    /// Send data to a specific peer (T070 integration)
    async fn send_to_peer(&self, peer_id: &str, data: &RuntimeData) -> Result<()> {
        let peer = self.peer_manager.get_peer(peer_id).await?;

        match data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                ..
            } => {
                let audio_track = peer.audio_track().await.ok_or_else(|| {
                    Error::MediaTrackError("No audio track configured".to_string())
                })?;

                // Send audio directly (handles encoding + RTP transmission).
                // Use sample rate from RuntimeData. `send_audio` takes
                // `Arc<Vec<f32>>`; materialize via `to_vec` — same cost
                // as the previous `samples.clone()` on a `Vec<f32>`.
                audio_track
                    .send_audio(Arc::new(samples.to_vec()), *sample_rate)
                    .await?;

                // Record outgoing audio metrics
                let bytes = samples.len() * std::mem::size_of::<f32>();
                self.metrics.write().await.record_audio_out(bytes);
                Ok(())
            }
            RuntimeData::Video {
                width,
                height,
                pixel_data,
                format,
                timestamp_us,
                ..
            } => {
                let video_track = peer.video_track().await.ok_or_else(|| {
                    Error::MediaTrackError("No video track configured".to_string())
                })?;

                // Convert PixelFormat enum to VideoFormat enum
                use crate::media::video::VideoFormat;
                let video_format = match format {
                    PixelFormat::Rgb24 => VideoFormat::RGB24,
                    PixelFormat::Yuv420p | PixelFormat::I420 => VideoFormat::I420,
                    _ => {
                        return Err(Error::EncodingError(format!(
                            "Unsupported video format: {:?}",
                            format
                        )))
                    }
                };

                // Create VideoFrame
                use crate::media::video::VideoFrame;
                let frame = VideoFrame {
                    width: *width,
                    height: *height,
                    format: video_format,
                    data: pixel_data.clone(),
                    timestamp_us: *timestamp_us,
                    is_keyframe: video_track.should_force_keyframe().await,
                };

                // Send video directly (handles encoding + RTP transmission)
                video_track.send_video(&frame).await?;

                // Record outgoing video metrics
                self.metrics
                    .write()
                    .await
                    .record_video_out(pixel_data.len());
                Ok(())
            }
            _ => {
                // Route non-media RuntimeData through the data channel.
                use crate::adapters::runtime_data_to_data_buffer;
                use prost::Message;

                // Serialize to protobuf DataBuffer
                let buffer = runtime_data_to_data_buffer(data);
                let encoded = buffer.encode_to_vec();

                if encoded.len() > MAX_CHUNK_SIZE {
                    // Large payload: split into chunks
                    self.send_data_channel_chunks(&peer, &encoded).await
                } else {
                    // Small payload: single RuntimeData message
                    self.send_data_channel_message(&peer, &encoded).await
                }
            }
        }
    }

    /// Send a single RuntimeData message over the data channel.
    async fn send_data_channel_message(
        &self,
        peer: &crate::peer::PeerConnection,
        encoded: &[u8],
    ) -> Result<()> {
        let msg = DataChannelMessage::runtime_data(encoded.to_vec());
        peer.send_data_channel_message(&msg).await
    }

    /// Send a large payload as chunked messages over the data channel.
    async fn send_data_channel_chunks(
        &self,
        peer: &crate::peer::PeerConnection,
        encoded: &[u8],
    ) -> Result<()> {
        use uuid::Uuid;

        let stream_id = self.session_id.clone();
        let message_id = Uuid::new_v4().to_string();
        let splitter = DataChannelSplitter::new(&stream_id, &message_id);
        let chunks = splitter.split(encoded);
        let num_chunks = chunks.len();

        trace!(
            stream_id = %stream_id,
            message_id = %message_id,
            chunks = num_chunks,
            total_bytes = encoded.len(),
            "Sending chunked data channel message"
        );

        for chunk_info in chunks {
            let msg = DataChannelMessage::chunk(chunk_info);
            peer.send_data_channel_message(&msg).await?;
        }

        debug!(
            stream_id = %stream_id,
            message_id = %message_id,
            chunks = num_chunks,
            "Completed sending chunked message"
        );

        Ok(())
    }

    /// Send data to output channel (T128).
    ///
    /// Bounded `.await` applies real backpressure when peers are slow
    /// to consume their outgoing frames.
    pub async fn send_output(&self, data: RuntimeData) -> Result<()> {
        debug!("Sending output to router (session: {})", self.session_id);

        self.output_tx
            .send(data)
            .await
            .map_err(|e| Error::SessionError(format!("Failed to send output: {}", e)))?;

        Ok(())
    }

    /// Receive output from router (internal)
    async fn recv_output_internal(&self) -> Option<RuntimeData> {
        let mut output_rx = self.output_rx.write().await;
        output_rx.recv().await
    }

    /// Get a per-peer bounded input channel (T129).
    ///
    /// Creates a new input channel for the peer if one doesn't exist.
    /// Callers should `.await` the send — the channel applies real
    /// backpressure to the WebRTC ingest path when saturated.
    pub async fn get_peer_input_channel(&self, peer_id: &str) -> Result<mpsc::Sender<RuntimeData>> {
        let mut peer_inputs = self.peer_inputs.write().await;

        if let Some(tx) = peer_inputs.get(peer_id) {
            return Ok(tx.clone());
        }

        // Create a new bounded channel for this peer.
        let (tx, _rx) = mpsc::channel(webrtc_peer_input_capacity());
        peer_inputs.insert(peer_id.to_string(), tx.clone());

        Ok(tx)
    }

    /// Get shared bounded output channel (T130).
    pub fn get_output_channel(&self) -> mpsc::Sender<RuntimeData> {
        self.output_tx.clone()
    }

    /// Shutdown the router (T131)
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down session router for: {}", self.session_id);

        // Take snapshot before shutdown for potential recovery
        self.save_state_snapshot(None).await?;

        // Send shutdown signal
        if let Err(e) = self.shutdown_tx.send(()).await {
            warn!("Failed to send shutdown signal: {}", e);
        }

        // Clear peer input channels
        let mut peer_inputs = self.peer_inputs.write().await;
        peer_inputs.clear();

        Ok(())
    }

    // ========== Phase 8 (US5) Session Recovery Methods ==========

    /// Save session state snapshot for recovery (T179)
    ///
    /// Captures current session state for restoration after reconnection.
    ///
    /// # Arguments
    /// * `pipeline_state` - Optional serialized pipeline state/manifest
    pub async fn save_state_snapshot(&self, pipeline_state: Option<String>) -> Result<()> {
        info!("Saving state snapshot for session: {}", self.session_id);

        let peer_ids = self.session.list_peers().await;

        let snapshot = SessionStateSnapshot {
            session_id: self.session_id.clone(),
            peer_ids,
            timestamp: SystemTime::now(),
            pipeline_state,
            metadata: HashMap::new(),
        };

        *self.state_snapshot.write().await = Some(snapshot);

        debug!(
            "State snapshot saved for session {} with {} peers",
            self.session_id,
            self.session.list_peers().await.len()
        );

        Ok(())
    }

    /// Save snapshot with custom metadata
    pub async fn save_state_snapshot_with_metadata(
        &self,
        pipeline_state: Option<String>,
        metadata: HashMap<String, String>,
    ) -> Result<()> {
        info!(
            "Saving state snapshot with metadata for session: {}",
            self.session_id
        );

        let peer_ids = self.session.list_peers().await;

        let snapshot = SessionStateSnapshot {
            session_id: self.session_id.clone(),
            peer_ids,
            timestamp: SystemTime::now(),
            pipeline_state,
            metadata,
        };

        *self.state_snapshot.write().await = Some(snapshot);

        Ok(())
    }

    /// Get the latest state snapshot
    pub async fn get_state_snapshot(&self) -> Option<SessionStateSnapshot> {
        self.state_snapshot.read().await.clone()
    }

    /// Restore session from snapshot (T179)
    ///
    /// Attempts to restore session state after reconnection.
    /// Note: This restores metadata; actual peer reconnections must be
    /// handled separately by the reconnection manager.
    ///
    /// # Arguments
    /// * `snapshot` - State snapshot to restore from
    pub async fn restore_from_snapshot(&self, snapshot: &SessionStateSnapshot) -> Result<()> {
        info!(
            "Restoring session {} from snapshot (age: {:?})",
            self.session_id,
            snapshot.age()
        );

        // Validate snapshot is for this session
        if snapshot.session_id != self.session_id {
            return Err(Error::SessionError(format!(
                "Snapshot session ID {} doesn't match router session {}",
                snapshot.session_id, self.session_id
            )));
        }

        // Log restoration details
        debug!("Restoring {} peers from snapshot", snapshot.peer_ids.len());

        // The actual peer reconnections are handled by ReconnectionManager
        // This method provides the snapshot data for the application to use

        Ok(())
    }

    /// Set reconnection event callback (T180)
    ///
    /// Registers a callback to be notified when peers reconnect.
    pub async fn on_peer_reconnected<F, Fut>(&self, callback: F)
    where
        F: Fn(ReconnectionEvent) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        info!(
            "Setting reconnection callback for session: {}",
            self.session_id
        );

        let boxed: ReconnectionCallback = Box::new(move |event| {
            let fut = callback(event);
            Box::pin(fut)
        });

        *self.reconnection_callback.write().await = Some(boxed);
    }

    /// Emit reconnection event (T180)
    ///
    /// Called when a peer reconnects to notify the application.
    ///
    /// # Arguments
    /// * `peer_id` - ID of the reconnected peer
    /// * `is_full_reconnect` - Whether this was a full reconnection or ICE restart
    /// * `previous_duration` - How long the peer was connected before disconnect
    pub async fn emit_reconnection_event(
        &self,
        peer_id: &str,
        is_full_reconnect: bool,
        previous_duration: Option<std::time::Duration>,
    ) {
        let event = ReconnectionEvent {
            peer_id: peer_id.to_string(),
            session_id: self.session_id.clone(),
            timestamp: Instant::now(),
            previous_duration,
            is_full_reconnect,
        };

        info!(
            "Peer {} reconnected to session {} (full: {})",
            peer_id, self.session_id, is_full_reconnect
        );

        // Call the callback if registered
        let callback_guard = self.reconnection_callback.read().await;
        if let Some(ref callback) = *callback_guard {
            callback(event).await;
        }
    }

    /// Check if recovery is available
    pub async fn has_recovery_snapshot(&self) -> bool {
        self.state_snapshot.read().await.is_some()
    }

    /// Clear the recovery snapshot
    pub async fn clear_recovery_snapshot(&self) {
        *self.state_snapshot.write().await = None;
    }

    // ========== Session Metrics Methods (T200) ==========

    /// Get current session metrics
    pub async fn get_metrics(&self) -> SessionMetrics {
        self.metrics.read().await.clone()
    }

    /// Reset session metrics
    pub async fn reset_metrics(&self) {
        *self.metrics.write().await = SessionMetrics::new();
    }

    /// Log current metrics (for periodic logging)
    pub async fn log_metrics(&self) {
        let metrics = self.metrics.read().await;
        info!(
            session_id = %self.session_id,
            audio_in = metrics.audio_frames_in,
            audio_out = metrics.audio_frames_out,
            video_in = metrics.video_frames_in,
            video_out = metrics.video_frames_out,
            errors = metrics.routing_errors,
            bytes_total = metrics.total_bytes(),
            avg_latency_us = metrics.avg_latency_us,
            max_latency_us = metrics.max_latency_us,
            "Session metrics"
        );
    }

    // ========== Phase 6 (US3) Selective Routing Methods (T151-T156) ==========

    /// Get current routing policy (T150)
    pub async fn get_routing_policy(&self) -> RoutingPolicy {
        *self.routing_policy.read().await
    }

    /// Set routing policy for this session (T150)
    pub async fn set_routing_policy(&self, policy: RoutingPolicy) {
        info!(
            "Setting routing policy to {:?} for session {}",
            policy, self.session_id
        );
        *self.routing_policy.write().await = policy;
    }

    /// Configure selective routing (T151)
    ///
    /// Sets up output routes for selective data distribution.
    ///
    /// # Arguments
    /// * `routes` - Output routes mapping outputs to target peers
    ///
    /// # Example
    /// ```ignore
    /// router.configure_selective_routing(vec![
    ///     OutputRoute::new("hd_output", vec!["peer1".to_string(), "peer2".to_string()])
    ///         .with_quality(QualityTier::High),
    ///     OutputRoute::new("sd_output", vec!["peer3".to_string()])
    ///         .with_quality(QualityTier::Low),
    /// ]).await;
    /// ```
    pub async fn configure_selective_routing(&self, routes: Vec<OutputRoute>) {
        info!(
            "Configuring {} output routes for session {}",
            routes.len(),
            self.session_id
        );

        // Log route details
        for route in &routes {
            debug!(
                "Route '{}': {} targets, quality={:?}, priority={}",
                route.output_id,
                if route.target_peers.is_empty() {
                    "all".to_string()
                } else {
                    route.target_peers.len().to_string()
                },
                route.quality_tier,
                route.priority
            );
        }

        // Set routing policy to selective when routes are configured
        *self.routing_policy.write().await = RoutingPolicy::Selective;
        *self.output_routes.write().await = routes;
    }

    /// Add an output route (T151)
    pub async fn add_output_route(&self, route: OutputRoute) {
        info!(
            "Adding output route '{}' for session {}",
            route.output_id, self.session_id
        );
        self.output_routes.write().await.push(route);
    }

    /// Remove an output route by output_id (T151)
    pub async fn remove_output_route(&self, output_id: &str) -> bool {
        let mut routes = self.output_routes.write().await;
        let initial_len = routes.len();
        routes.retain(|r| r.output_id != output_id);
        let removed = routes.len() < initial_len;

        if removed {
            info!(
                "Removed output route '{}' from session {}",
                output_id, self.session_id
            );
        }

        removed
    }

    /// Get all configured routes
    pub async fn get_output_routes(&self) -> Vec<OutputRoute> {
        self.output_routes.read().await.clone()
    }

    /// Get routes targeting a specific peer (T152)
    pub async fn get_routes_for_peer(&self, peer_id: &str) -> Vec<OutputRoute> {
        self.output_routes
            .read()
            .await
            .iter()
            .filter(|r| r.active && r.targets_peer(peer_id))
            .cloned()
            .collect()
    }

    /// Route outgoing data with selective routing support (T152)
    ///
    /// Uses configured routes to determine target peers.
    pub async fn route_outgoing_selective(
        &self,
        output_id: &str,
        data: RuntimeData,
    ) -> Result<Vec<String>> {
        let policy = *self.routing_policy.read().await;
        let mut sent_to = Vec::new();

        match policy {
            RoutingPolicy::Broadcast => {
                // Broadcast to all peers
                let peer_ids = self.session.list_peers().await;
                for peer_id in peer_ids {
                    if let Err(e) = self.send_to_peer(&peer_id, &data).await {
                        error!("Failed to send to peer {}: {}", peer_id, e);
                    } else {
                        sent_to.push(peer_id);
                    }
                }
            }
            RoutingPolicy::Selective => {
                // Use configured routes
                let routes = self.output_routes.read().await;
                let matching_routes: Vec<_> = routes
                    .iter()
                    .filter(|r| r.active && r.output_id == output_id)
                    .collect();

                if matching_routes.is_empty() {
                    warn!(
                        "No routes configured for output '{}', falling back to broadcast",
                        output_id
                    );
                    let peer_ids = self.session.list_peers().await;
                    for peer_id in peer_ids {
                        if let Err(e) = self.send_to_peer(&peer_id, &data).await {
                            error!("Failed to send to peer {}: {}", peer_id, e);
                        } else {
                            sent_to.push(peer_id);
                        }
                    }
                } else {
                    // Sort by priority (higher first)
                    let mut sorted_routes = matching_routes;
                    sorted_routes.sort_by(|a, b| b.priority.cmp(&a.priority));

                    for route in sorted_routes {
                        let targets = if route.is_broadcast() {
                            self.session.list_peers().await
                        } else {
                            route.target_peers.clone()
                        };

                        for peer_id in targets {
                            if sent_to.contains(&peer_id) {
                                continue; // Skip if already sent
                            }
                            if let Err(e) = self.send_to_peer(&peer_id, &data).await {
                                error!("Failed to send to peer {}: {}", peer_id, e);
                            } else {
                                sent_to.push(peer_id);
                            }
                        }
                    }
                }
            }
            RoutingPolicy::Unicast => {
                // Unicast requires explicit peer_id, warn and skip
                warn!("route_outgoing_selective called with Unicast policy - use send_to_peer directly");
            }
        }

        debug!(
            "Routed output '{}' to {} peers: {:?}",
            output_id,
            sent_to.len(),
            sent_to
        );

        Ok(sent_to)
    }

    /// Negotiate quality tier for a peer based on capabilities (T153)
    ///
    /// Determines the appropriate quality tier based on peer connection quality.
    pub async fn negotiate_quality_tier(&self, peer_id: &str) -> QualityTier {
        // Get peer bitrate state if available
        let states = self.peer_bitrate_states.read().await;
        if let Some(state) = states.get(peer_id) {
            // Determine tier based on current bitrate
            if state.current_bitrate_kbps >= 3000 {
                QualityTier::High
            } else if state.current_bitrate_kbps >= 1000 {
                QualityTier::Medium
            } else {
                QualityTier::Low
            }
        } else {
            // Default to medium for unknown peers
            QualityTier::Medium
        }
    }

    /// Get or initialize peer bitrate state (T156)
    pub async fn get_peer_bitrate_state(&self, peer_id: &str) -> PeerBitrateState {
        let states = self.peer_bitrate_states.read().await;
        states.get(peer_id).cloned().unwrap_or_default()
    }

    /// Update peer bitrate state with new metrics (T156)
    pub async fn update_peer_bitrate(
        &self,
        peer_id: &str,
        packet_loss: f32,
        rtt_ms: u32,
    ) -> BitrateAction {
        let mut states = self.peer_bitrate_states.write().await;
        let state = states.entry(peer_id.to_string()).or_default();

        let action = state.update_metrics(packet_loss, rtt_ms);
        state.apply_action(action);

        debug!(
            "Updated bitrate for peer {}: {:?} -> {} kbps",
            peer_id, action, state.current_bitrate_kbps
        );

        action
    }

    /// Set bitrate range for a peer (T156)
    pub async fn set_peer_bitrate_range(&self, peer_id: &str, min_kbps: u32, max_kbps: u32) {
        let mut states = self.peer_bitrate_states.write().await;
        let state = states.entry(peer_id.to_string()).or_default();
        state.min_bitrate_kbps = min_kbps;
        state.max_bitrate_kbps = max_kbps;
        state.current_bitrate_kbps = state.current_bitrate_kbps.clamp(min_kbps, max_kbps);
    }

    /// Get recommended bitrate for a peer (T156)
    pub async fn get_recommended_bitrate(&self, peer_id: &str) -> u32 {
        let states = self.peer_bitrate_states.read().await;
        states
            .get(peer_id)
            .map(|s| s.current_bitrate_kbps)
            .unwrap_or(1500)
    }

    /// Check if peer needs bitrate reduction (T156)
    pub async fn should_reduce_peer_bitrate(&self, peer_id: &str) -> bool {
        let states = self.peer_bitrate_states.read().await;
        states
            .get(peer_id)
            .map(|s| s.should_reduce_bitrate())
            .unwrap_or(false)
    }

    /// Get all peer bitrate states
    pub async fn get_all_bitrate_states(&self) -> HashMap<String, PeerBitrateState> {
        self.peer_bitrate_states.read().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_session_router_creation() {
        let session = Arc::new(Session::new("test-session".to_string()));
        let peer_manager = Arc::new(PeerManager::new(10).unwrap());

        let router = SessionRouter::new("test-session".to_string(), session, peer_manager);

        assert!(router.is_ok());
    }

    #[test]
    fn test_session_metrics_default() {
        let metrics = SessionMetrics::new();
        assert_eq!(metrics.audio_frames_in, 0);
        assert_eq!(metrics.audio_frames_out, 0);
        assert_eq!(metrics.video_frames_in, 0);
        assert_eq!(metrics.video_frames_out, 0);
        assert_eq!(metrics.routing_errors, 0);
        assert_eq!(metrics.total_frames(), 0);
        assert_eq!(metrics.total_bytes(), 0);
    }

    #[test]
    fn test_session_metrics_recording() {
        let mut metrics = SessionMetrics::new();

        metrics.record_audio_in(1000);
        metrics.record_audio_out(1000);
        metrics.record_video_in(5000);
        metrics.record_video_out(5000);
        metrics.record_error();

        assert_eq!(metrics.audio_frames_in, 1);
        assert_eq!(metrics.audio_frames_out, 1);
        assert_eq!(metrics.video_frames_in, 1);
        assert_eq!(metrics.video_frames_out, 1);
        assert_eq!(metrics.routing_errors, 1);
        assert_eq!(metrics.total_frames(), 4);
        assert_eq!(metrics.bytes_received, 6000);
        assert_eq!(metrics.bytes_sent, 6000);
        assert_eq!(metrics.total_bytes(), 12000);
    }

    #[test]
    fn test_session_metrics_latency() {
        let mut metrics = SessionMetrics::new();

        // First latency sets the average
        metrics.record_latency(100);
        assert_eq!(metrics.avg_latency_us, 100);
        assert_eq!(metrics.max_latency_us, 100);

        // Second latency uses EMA
        metrics.record_latency(200);
        // EMA: (100 * 9 + 200) / 10 = 110
        assert_eq!(metrics.avg_latency_us, 110);
        assert_eq!(metrics.max_latency_us, 200);

        // Max should update
        metrics.record_latency(300);
        assert_eq!(metrics.max_latency_us, 300);
    }

    #[tokio::test]
    async fn test_session_router_metrics() {
        let session = Arc::new(Session::new("test-session".to_string()));
        let peer_manager = Arc::new(PeerManager::new(10).unwrap());

        let router = SessionRouter::new("test-session".to_string(), session, peer_manager).unwrap();

        // Get initial metrics
        let metrics = router.get_metrics().await;
        assert_eq!(metrics.total_frames(), 0);

        // Reset and verify
        router.reset_metrics().await;
        let metrics = router.get_metrics().await;
        assert_eq!(metrics.total_frames(), 0);
    }

    // ========== Phase 6 Tests ==========

    #[test]
    fn test_routing_policy() {
        assert!(RoutingPolicy::Unicast.is_unicast());
        assert!(RoutingPolicy::Broadcast.is_broadcast());
        assert!(RoutingPolicy::Selective.is_selective());
        assert_eq!(RoutingPolicy::default(), RoutingPolicy::Broadcast);
    }

    #[test]
    fn test_quality_tier() {
        assert_eq!(QualityTier::High.recommended_bitrate_kbps(), 4000);
        assert_eq!(QualityTier::Medium.recommended_bitrate_kbps(), 1500);
        assert_eq!(QualityTier::Low.recommended_bitrate_kbps(), 500);

        assert_eq!(QualityTier::High.recommended_resolution(), (1920, 1080));
        assert_eq!(QualityTier::Medium.recommended_resolution(), (1280, 720));
        assert_eq!(QualityTier::Low.recommended_resolution(), (854, 480));

        assert_eq!(QualityTier::default(), QualityTier::Medium);
    }

    #[test]
    fn test_output_route() {
        let route = OutputRoute::new("video_out", vec!["peer1".to_string(), "peer2".to_string()])
            .with_quality(QualityTier::High)
            .with_priority(10);

        assert_eq!(route.output_id, "video_out");
        assert_eq!(route.target_peers.len(), 2);
        assert_eq!(route.quality_tier, Some(QualityTier::High));
        assert_eq!(route.priority, 10);
        assert!(route.active);
        assert!(!route.is_broadcast());
        assert!(route.targets_peer("peer1"));
        assert!(route.targets_peer("peer2"));
        assert!(!route.targets_peer("peer3"));
    }

    #[test]
    fn test_output_route_broadcast() {
        let route = OutputRoute::broadcast("audio_out");

        assert!(route.is_broadcast());
        assert!(route.targets_peer("any_peer"));
    }

    #[test]
    fn test_peer_bitrate_state() {
        let mut state = PeerBitrateState::default();
        assert_eq!(state.current_bitrate_kbps, 1500);

        // Test good conditions - should increase
        let action = state.update_metrics(0.005, 50);
        assert_eq!(action, BitrateAction::Increase);
        state.apply_action(action);
        assert!(state.current_bitrate_kbps > 1500);

        // Test poor conditions - should decrease
        let mut state2 = PeerBitrateState::default();
        let action = state2.update_metrics(0.15, 600);
        assert_eq!(action, BitrateAction::Decrease);
        state2.apply_action(action);
        assert!(state2.current_bitrate_kbps < 1500);
    }

    #[test]
    fn test_peer_bitrate_state_range() {
        let state = PeerBitrateState::with_range(500, 2000);
        assert_eq!(state.min_bitrate_kbps, 500);
        assert_eq!(state.max_bitrate_kbps, 2000);
        assert_eq!(state.current_bitrate_kbps, 1250); // (500 + 2000) / 2
    }

    #[test]
    fn test_peer_bitrate_helpers() {
        let mut state = PeerBitrateState::default();

        // Normal conditions
        state.packet_loss_rate = 0.02;
        state.rtt_ms = 100;
        assert!(!state.should_reduce_bitrate());

        // Poor conditions
        state.packet_loss_rate = 0.1;
        state.rtt_ms = 400;
        assert!(state.should_reduce_bitrate());

        // Check increase capability
        state.packet_loss_rate = 0.005;
        state.rtt_ms = 50;
        state.current_bitrate_kbps = 1000;
        assert!(state.can_increase_bitrate());
    }

    #[tokio::test]
    async fn test_session_router_routing_policy() {
        let session = Arc::new(Session::new("test-session".to_string()));
        let peer_manager = Arc::new(PeerManager::new(10).unwrap());
        let router = SessionRouter::new("test-session".to_string(), session, peer_manager).unwrap();

        // Default is broadcast
        assert_eq!(router.get_routing_policy().await, RoutingPolicy::Broadcast);

        // Can change policy
        router.set_routing_policy(RoutingPolicy::Selective).await;
        assert_eq!(router.get_routing_policy().await, RoutingPolicy::Selective);
    }

    #[tokio::test]
    async fn test_session_router_output_routes() {
        let session = Arc::new(Session::new("test-session".to_string()));
        let peer_manager = Arc::new(PeerManager::new(10).unwrap());
        let router = SessionRouter::new("test-session".to_string(), session, peer_manager).unwrap();

        // No routes initially
        assert!(router.get_output_routes().await.is_empty());

        // Add routes
        router
            .configure_selective_routing(vec![
                OutputRoute::new("hd_out", vec!["peer1".to_string()]),
                OutputRoute::new("sd_out", vec!["peer2".to_string(), "peer3".to_string()]),
            ])
            .await;

        let routes = router.get_output_routes().await;
        assert_eq!(routes.len(), 2);

        // Policy should be selective now
        assert_eq!(router.get_routing_policy().await, RoutingPolicy::Selective);

        // Remove a route
        assert!(router.remove_output_route("hd_out").await);
        assert_eq!(router.get_output_routes().await.len(), 1);

        // Remove non-existent route
        assert!(!router.remove_output_route("nonexistent").await);
    }

    #[tokio::test]
    async fn test_session_router_bitrate_state() {
        let session = Arc::new(Session::new("test-session".to_string()));
        let peer_manager = Arc::new(PeerManager::new(10).unwrap());
        let router = SessionRouter::new("test-session".to_string(), session, peer_manager).unwrap();

        // Get default state for unknown peer
        let state = router.get_peer_bitrate_state("peer1").await;
        assert_eq!(state.current_bitrate_kbps, 1500);

        // Update with metrics
        let action = router.update_peer_bitrate("peer1", 0.005, 50).await;
        assert_eq!(action, BitrateAction::Increase);

        // Check updated state
        let state = router.get_peer_bitrate_state("peer1").await;
        assert!(state.current_bitrate_kbps > 1500);

        // Set range
        router.set_peer_bitrate_range("peer2", 500, 3000).await;
        let state = router.get_peer_bitrate_state("peer2").await;
        assert_eq!(state.min_bitrate_kbps, 500);
        assert_eq!(state.max_bitrate_kbps, 3000);

        // Check recommended bitrate
        let bitrate = router.get_recommended_bitrate("peer1").await;
        assert!(bitrate > 1500);
    }

    #[tokio::test]
    async fn test_session_router_quality_negotiation() {
        let session = Arc::new(Session::new("test-session".to_string()));
        let peer_manager = Arc::new(PeerManager::new(10).unwrap());
        let router = SessionRouter::new("test-session".to_string(), session, peer_manager).unwrap();

        // Unknown peer defaults to medium
        assert_eq!(
            router.negotiate_quality_tier("unknown").await,
            QualityTier::Medium
        );

        // High bitrate peer gets high quality
        router.set_peer_bitrate_range("high_peer", 2000, 5000).await;
        router.update_peer_bitrate("high_peer", 0.005, 50).await;

        // Update bitrate to be high enough
        {
            let mut states = router.peer_bitrate_states.write().await;
            if let Some(state) = states.get_mut("high_peer") {
                state.current_bitrate_kbps = 4000;
            }
        }

        assert_eq!(
            router.negotiate_quality_tier("high_peer").await,
            QualityTier::High
        );
    }
}
