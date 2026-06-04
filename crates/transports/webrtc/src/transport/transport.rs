//! Main WebRTC transport implementation

use crate::{
    config::WebRtcTransportConfig,
    media::tracks::runtime_data_to_rtp,
    peer::{PeerConnection, PeerInfo, PeerManager},
    session::{Session, SessionId, SessionManager},
    signaling::{IceCandidateParams, SignalingClient},
    sync::{SyncedAudioFrame, SyncedVideoFrame},
    Error, Result,
};
use remotemedia_core::data::RuntimeData;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Statistics from broadcasting data to multiple peers (T071)
#[derive(Debug, Clone)]
pub struct BroadcastStats {
    /// Total number of peers targeted for broadcast
    pub total_peers: usize,
    /// Number of successful transmissions
    pub sent_count: usize,
    /// Number of failed transmissions
    pub failed_count: usize,
    /// List of peer IDs that failed to receive
    pub failed_peers: Vec<String>,
    /// Total duration of broadcast operation in milliseconds
    pub total_duration_ms: u64,
}

/// WebRTC transport for RemoteMedia pipelines
///
/// Manages WebRTC peer connections in a mesh topology, coordinating
/// signaling, media streams, and pipeline integration.
pub struct WebRtcTransport {
    /// Transport configuration
    config: WebRtcTransportConfig,

    /// Signaling client for peer discovery
    signaling_client: Arc<RwLock<Option<SignalingClient>>>,

    /// Peer connection manager
    peer_manager: Arc<PeerManager>,

    /// Session manager for pipeline integration
    session_manager: Arc<SessionManager>,

    /// Local peer ID (assigned after announcement)
    local_peer_id: Arc<RwLock<Option<String>>>,
}

impl WebRtcTransport {
    /// Create a new WebRTC transport
    ///
    /// # Arguments
    ///
    /// * `config` - Transport configuration
    ///
    /// # Errors
    ///
    /// Returns error if configuration is invalid.
    pub fn new(config: WebRtcTransportConfig) -> Result<Self> {
        // Validate configuration
        config.validate()?;

        info!("Creating WebRTC transport");

        let peer_manager = Arc::new(PeerManager::new(config.max_peers)?);

        let signaling_client = SignalingClient::new(&config.signaling_url);

        // Create session manager with same max limit as peers
        let session_manager = Arc::new(SessionManager::new(config.max_peers as usize)?);

        Ok(Self {
            config,
            signaling_client: Arc::new(RwLock::new(Some(signaling_client))),
            peer_manager,
            session_manager,
            local_peer_id: Arc::new(RwLock::new(None)),
        })
    }

    /// Start the transport
    ///
    /// Connects to the signaling server and announces this peer.
    pub async fn start(&self) -> Result<()> {
        info!("Starting WebRTC transport");

        // Generate or use configured peer ID
        let peer_id = self
            .config
            .peer_id
            .clone()
            .unwrap_or_else(|| format!("peer-{}", uuid::Uuid::new_v4()));

        *self.local_peer_id.write().await = Some(peer_id.clone());

        // Connect to signaling server
        {
            let mut client_guard = self.signaling_client.write().await;
            if let Some(client) = client_guard.as_mut() {
                client.connect().await?;

                // Set up callbacks
                self.setup_signaling_callbacks(client).await;

                // Announce this peer
                let capabilities = self.get_capabilities();
                client.announce_peer(peer_id, capabilities).await?;
            }
        }

        info!("WebRTC transport started");

        Ok(())
    }

    /// Get capabilities based on configuration
    fn get_capabilities(&self) -> Vec<String> {
        let mut caps = Vec::new();

        match self.config.audio_codec {
            crate::config::AudioCodec::Opus => caps.push("audio".to_string()),
        }

        match self.config.video_codec {
            crate::config::VideoCodec::VP8 => caps.push("video".to_string()),
            crate::config::VideoCodec::VP9 => caps.push("video".to_string()),
            crate::config::VideoCodec::H264 => caps.push("video".to_string()),
        }

        if self.config.enable_data_channel {
            caps.push("data".to_string());
        }

        caps
    }

    /// Setup signaling callbacks
    async fn setup_signaling_callbacks(&self, client: &mut SignalingClient) {
        let peer_manager = self.peer_manager.clone();
        let config = self.config.clone();
        let signaling_client = self.signaling_client.clone();

        // On offer received: create answer
        client
            .on_offer_received(move |from, _to, sdp| {
                let peer_manager = peer_manager.clone();
                let config = config.clone();
                let signaling_client = signaling_client.clone();

                tokio::spawn(async move {
                    if let Err(e) = Self::handle_offer_received(
                        peer_manager,
                        config,
                        signaling_client,
                        from,
                        sdp,
                    )
                    .await
                    {
                        warn!("Failed to handle offer: {}", e);
                    }
                });
            })
            .await;

        let peer_manager = self.peer_manager.clone();

        // On answer received: set remote description
        client
            .on_answer_received(move |from, _to, sdp| {
                let peer_manager = peer_manager.clone();

                tokio::spawn(async move {
                    if let Err(e) = Self::handle_answer_received(peer_manager, from, sdp).await {
                        warn!("Failed to handle answer: {}", e);
                    }
                });
            })
            .await;

        let peer_manager = self.peer_manager.clone();

        // On ICE candidate received: add to peer connection
        client
            .on_ice_candidate_received(move |from, _to, params| {
                let peer_manager = peer_manager.clone();

                tokio::spawn(async move {
                    if let Err(e) = Self::handle_ice_candidate(peer_manager, from, params).await {
                        warn!("Failed to handle ICE candidate: {}", e);
                    }
                });
            })
            .await;
    }

    /// Handle received offer
    async fn handle_offer_received(
        peer_manager: Arc<PeerManager>,
        config: WebRtcTransportConfig,
        signaling_client: Arc<RwLock<Option<SignalingClient>>>,
        from: String,
        sdp: String,
    ) -> Result<()> {
        debug!("Handling offer from peer: {}", from);

        // Create or get peer connection
        let peer = if peer_manager.has_peer(&from).await {
            peer_manager.get_peer(&from).await?
        } else {
            let connection = Arc::new(PeerConnection::new(from.clone(), &config).await?);
            peer_manager
                .add_peer(from.clone(), connection.clone())
                .await?;
            connection
        };

        // Create answer
        let answer = peer.create_answer(sdp).await?;

        // Send answer via signaling
        let client_guard = signaling_client.read().await;
        if let Some(client) = client_guard.as_ref() {
            client.send_answer(from, answer).await?;
        } else {
            return Err(Error::SignalingError(
                "Signaling client not initialized".to_string(),
            ));
        }

        Ok(())
    }

    /// Handle received answer
    async fn handle_answer_received(
        peer_manager: Arc<PeerManager>,
        from: String,
        sdp: String,
    ) -> Result<()> {
        debug!("Handling answer from peer: {}", from);

        let peer = peer_manager.get_peer(&from).await?;
        peer.set_remote_description(sdp).await?;

        Ok(())
    }

    /// Handle received ICE candidate
    async fn handle_ice_candidate(
        peer_manager: Arc<PeerManager>,
        from: String,
        params: IceCandidateParams,
    ) -> Result<()> {
        debug!("Handling ICE candidate from peer: {}", from);

        let peer = peer_manager.get_peer(&from).await?;
        peer.add_ice_candidate(params.candidate).await?;

        Ok(())
    }

    /// Connect to a remote peer
    ///
    /// Initiates a connection by creating an offer and sending it via signaling.
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Identifier of the remote peer to connect to
    pub async fn connect_peer(&self, peer_id: String) -> Result<String> {
        info!("Connecting to peer: {}", peer_id);

        // Create peer connection
        let connection = Arc::new(PeerConnection::new(peer_id.clone(), &self.config).await?);

        // Add to manager
        self.peer_manager
            .add_peer(peer_id.clone(), connection.clone())
            .await?;

        // Create offer
        let offer_sdp = connection.create_offer().await?;

        // Send offer via signaling
        {
            let client_guard = self.signaling_client.read().await;
            if let Some(client) = client_guard.as_ref() {
                client.send_offer(peer_id.clone(), offer_sdp).await?;
            } else {
                return Err(Error::SignalingError(
                    "Signaling client not initialized".to_string(),
                ));
            }
        }

        Ok(peer_id)
    }

    /// Disconnect from a peer
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Identifier of the peer to disconnect from
    pub async fn disconnect_peer(&self, peer_id: &str) -> Result<()> {
        info!("Disconnecting from peer: {}", peer_id);

        // Remove from peer manager (will close connection)
        self.peer_manager.remove_peer(peer_id).await?;

        // Send disconnect notification via signaling
        {
            let client_guard = self.signaling_client.read().await;
            if let Some(client) = client_guard.as_ref() {
                // Note: disconnect() sends own peer_id, not target peer_id
                client.disconnect().await?;
            }
        }

        Ok(())
    }

    /// List all connected peers
    pub async fn list_peers(&self) -> Vec<PeerInfo> {
        self.peer_manager.list_connected_peers().await
    }

    /// List all peers (any state)
    pub async fn list_all_peers(&self) -> Vec<PeerInfo> {
        self.peer_manager.list_all_peers().await
    }

    /// Get the local peer ID
    pub async fn local_peer_id(&self) -> Option<String> {
        self.local_peer_id.read().await.clone()
    }

    /// Send audio samples to a specific peer
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Identifier of the peer to send audio to
    /// * `samples` - Audio samples as f32 (range -1.0 to 1.0)
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Peer not found or not connected
    /// - Peer has no audio track configured
    /// - Audio encoding or transmission fails
    pub async fn send_audio(&self, peer_id: &str, samples: Arc<Vec<f32>>) -> Result<()> {
        debug!(
            "Sending audio to peer: {} ({} samples)",
            peer_id,
            samples.len()
        );

        // Get peer connection
        let peer = self.peer_manager.get_peer(peer_id).await?;

        // Get audio track
        let audio_track = peer.audio_track().await.ok_or_else(|| {
            Error::MediaTrackError(format!("Peer {} has no audio track", peer_id))
        })?;

        // Send audio via track with default 24kHz sample rate (for backwards compatibility)
        // TODO: Add sample_rate parameter to this method
        audio_track.send_audio(samples, 24000).await?;

        Ok(())
    }

    /// Send video frame to a specific peer
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Identifier of the peer to send video to
    /// * `frame` - Video frame to send
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Peer not found or not connected
    /// - Peer has no video track configured
    /// - Video encoding or transmission fails
    pub async fn send_video(
        &self,
        peer_id: &str,
        frame: &crate::media::video::VideoFrame,
    ) -> Result<()> {
        debug!(
            "Sending video frame to peer: {} ({}x{})",
            peer_id, frame.width, frame.height
        );

        // Get peer connection
        let peer = self.peer_manager.get_peer(peer_id).await?;

        // Get video track
        let video_track = peer.video_track().await.ok_or_else(|| {
            Error::MediaTrackError(format!("Peer {} has no video track", peer_id))
        })?;

        // Send video via track
        video_track.send_video(frame).await?;

        Ok(())
    }

    /// Broadcast audio samples to all connected peers
    ///
    /// # Arguments
    ///
    /// * `samples` - Audio samples as f32 (range -1.0 to 1.0)
    /// * `exclude` - Optional list of peer IDs to exclude from broadcast
    ///
    /// # Note
    ///
    /// Silently skips peers that don't have audio tracks configured.
    /// Use `send_audio()` if you need to handle individual peer errors.
    pub async fn broadcast_audio(
        &self,
        samples: Arc<Vec<f32>>,
        exclude: Option<Vec<String>>,
    ) -> Result<()> {
        let exclude_set: std::collections::HashSet<String> =
            exclude.unwrap_or_default().into_iter().collect();

        let peers = self.peer_manager.list_connected_peers().await;
        let peer_count = peers.len();

        debug!(
            "Broadcasting audio to {} peers ({} samples)",
            peer_count,
            samples.len()
        );

        // Send to all connected peers with audio tracks
        for peer_info in peers {
            if exclude_set.contains(&peer_info.peer_id) {
                continue;
            }

            match self
                .send_audio(&peer_info.peer_id, Arc::clone(&samples))
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    // Log error but continue broadcasting to other peers
                    warn!("Failed to send audio to peer {}: {}", peer_info.peer_id, e);
                }
            }
        }

        Ok(())
    }

    /// Broadcast video frame to all connected peers
    ///
    /// # Arguments
    ///
    /// * `frame` - Video frame to broadcast
    /// * `exclude` - Optional list of peer IDs to exclude from broadcast
    ///
    /// # Note
    ///
    /// Silently skips peers that don't have video tracks configured.
    /// Use `send_video()` if you need to handle individual peer errors.
    pub async fn broadcast_video(
        &self,
        frame: &crate::media::video::VideoFrame,
        exclude: Option<Vec<String>>,
    ) -> Result<()> {
        let exclude_set: std::collections::HashSet<String> =
            exclude.unwrap_or_default().into_iter().collect();

        let peers = self.peer_manager.list_connected_peers().await;
        let peer_count = peers.len();

        debug!(
            "Broadcasting video to {} peers ({}x{})",
            peer_count, frame.width, frame.height
        );

        // Send to all connected peers with video tracks
        for peer_info in peers {
            if exclude_set.contains(&peer_info.peer_id) {
                continue;
            }

            match self.send_video(&peer_info.peer_id, frame).await {
                Ok(_) => {}
                Err(e) => {
                    // Log error but continue broadcasting to other peers
                    warn!("Failed to send video to peer {}: {}", peer_info.peer_id, e);
                }
            }
        }

        Ok(())
    }

    /// Create a new streaming session
    ///
    /// # Arguments
    ///
    /// * `session_id` - Unique identifier for the session
    ///
    /// # Returns
    ///
    /// Returns the created Session wrapped in Arc for shared ownership.
    pub async fn create_session(&self, session_id: SessionId) -> Result<Arc<Session>> {
        info!("Creating session: {}", session_id);
        self.session_manager.create_session(session_id).await
    }

    /// Get an existing session
    ///
    /// # Arguments
    ///
    /// * `session_id` - Session identifier
    pub async fn get_session(&self, session_id: &str) -> Result<Arc<Session>> {
        self.session_manager.get_session(session_id).await
    }

    /// Check if a session exists
    pub async fn has_session(&self, session_id: &str) -> bool {
        self.session_manager.has_session(session_id).await
    }

    /// Remove a session
    ///
    /// # Arguments
    ///
    /// * `session_id` - Session identifier
    pub async fn remove_session(&self, session_id: &str) -> Result<()> {
        self.session_manager.remove_session(session_id).await
    }

    /// List all active sessions
    pub async fn list_sessions(&self) -> Vec<SessionId> {
        self.session_manager.list_sessions().await
    }

    /// Get session count
    pub async fn session_count(&self) -> usize {
        self.session_manager.session_count().await
    }

    /// Associate a peer with a session
    ///
    /// # Arguments
    ///
    /// * `session_id` - Session identifier
    /// * `peer_id` - Peer identifier
    pub async fn add_peer_to_session(&self, session_id: &str, peer_id: String) -> Result<()> {
        let session = self.get_session(session_id).await?;
        session.add_peer(peer_id).await
    }

    /// Remove a peer from a session
    ///
    /// # Arguments
    ///
    /// * `session_id` - Session identifier
    /// * `peer_id` - Peer identifier
    pub async fn remove_peer_from_session(&self, session_id: &str, peer_id: &str) -> Result<()> {
        let session = self.get_session(session_id).await?;
        session.remove_peer(peer_id).await
    }

    /// Send RuntimeData to a specific peer (T069)
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Target peer identifier
    /// * `data` - RuntimeData to send (Audio or Video)
    ///
    /// # Note
    ///
    /// This method encodes the RuntimeData to the appropriate codec format (Opus for audio, VP9 for video)
    /// and sends it via RTP to the specified peer.
    pub async fn send_to_peer(&self, peer_id: &str, data: &RuntimeData) -> Result<()> {
        debug!("Sending RuntimeData to peer: {}", peer_id);

        // Get the peer connection
        let peer = self.peer_manager.get_peer(peer_id).await?;

        // Determine if this is audio or video and get the appropriate track
        match data {
            RuntimeData::Audio { .. } => {
                let audio_track = peer.audio_track().await.ok_or_else(|| {
                    Error::MediaTrackError("No audio track configured for peer".to_string())
                })?;

                // Encode and send audio
                let _payload = runtime_data_to_rtp(data, Some(&audio_track), None).await?;

                // Note: The actual RTP sending happens inside runtime_data_to_rtp via AudioTrack::send_audio
                // which is called by the encoder. For direct sending, we'd need to:
                // audio_track.send_audio(samples).await?;

                Ok(())
            }
            RuntimeData::Video { .. } => {
                let video_track = peer.video_track().await.ok_or_else(|| {
                    Error::MediaTrackError("No video track configured for peer".to_string())
                })?;

                // Encode and send video
                let _payload = runtime_data_to_rtp(data, None, Some(&video_track)).await?;

                // Note: Similar to audio, actual sending happens inside runtime_data_to_rtp
                // via VideoTrack::send_video which is called by the encoder

                Ok(())
            }
            _ => Err(Error::MediaTrackError(
                "Unsupported RuntimeData type for peer-to-peer transmission".to_string(),
            )),
        }
    }

    /// Broadcast RuntimeData to all connected peers (T070)
    ///
    /// # Arguments
    ///
    /// * `data` - RuntimeData to broadcast (Audio or Video)
    ///
    /// # Returns
    ///
    /// BroadcastStats with success/failure counts and timing information
    ///
    /// # Note
    ///
    /// This method sends data to all connected peers in parallel, collecting statistics
    /// about successful and failed transmissions.
    pub async fn broadcast(&self, data: &RuntimeData) -> Result<BroadcastStats> {
        let start = Instant::now();
        let peers = self.peer_manager.list_connected_peers().await;
        let total_peers = peers.len();

        debug!("Broadcasting RuntimeData to {} peers", total_peers);

        let mut sent_count = 0;
        let mut failed_count = 0;
        let mut failed_peers = Vec::new();

        // Send to each peer
        for peer_info in peers {
            match self.send_to_peer(&peer_info.peer_id, data).await {
                Ok(_) => sent_count += 1,
                Err(e) => {
                    warn!("Failed to send to peer {}: {}", peer_info.peer_id, e);
                    failed_count += 1;
                    failed_peers.push(peer_info.peer_id);
                }
            }
        }

        let duration = start.elapsed();

        Ok(BroadcastStats {
            total_peers,
            sent_count,
            failed_count,
            failed_peers,
            total_duration_ms: duration.as_millis() as u64,
        })
    }

    // ========== Phase 7 (US4) Data Channel Transport API ==========

    /// Send a data channel message to a specific peer (T168)
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Target peer identifier
    /// * `msg` - Message to send
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Peer not found
    /// - Peer has no data channel configured
    /// - Data channel is not open
    pub async fn send_data_channel_message(
        &self,
        peer_id: &str,
        msg: &crate::channels::DataChannelMessage,
    ) -> Result<()> {
        debug!("Sending data channel message to peer: {}", peer_id);

        let peer = self.peer_manager.get_peer(peer_id).await?;
        peer.send_data_channel_message(msg).await
    }

    /// Broadcast a data channel message to all connected peers
    ///
    /// # Arguments
    ///
    /// * `msg` - Message to broadcast
    /// * `exclude` - Optional list of peer IDs to exclude from broadcast
    ///
    /// # Returns
    ///
    /// BroadcastStats with success/failure counts
    pub async fn broadcast_data_channel_message(
        &self,
        msg: &crate::channels::DataChannelMessage,
        exclude: Option<Vec<String>>,
    ) -> BroadcastStats {
        let start = Instant::now();
        let exclude_set: std::collections::HashSet<String> =
            exclude.unwrap_or_default().into_iter().collect();

        let peers = self.peer_manager.list_connected_peers().await;
        let total_peers = peers.len();

        debug!("Broadcasting data channel message to {} peers", total_peers);

        let mut sent_count = 0;
        let mut failed_count = 0;
        let mut failed_peers = Vec::new();

        for peer_info in peers {
            if exclude_set.contains(&peer_info.peer_id) {
                continue;
            }

            match self
                .send_data_channel_message(&peer_info.peer_id, msg)
                .await
            {
                Ok(_) => sent_count += 1,
                Err(e) => {
                    warn!(
                        "Failed to send data channel message to peer {}: {}",
                        peer_info.peer_id, e
                    );
                    failed_count += 1;
                    failed_peers.push(peer_info.peer_id);
                }
            }
        }

        let duration = start.elapsed();

        BroadcastStats {
            total_peers,
            sent_count,
            failed_count,
            failed_peers,
            total_duration_ms: duration.as_millis() as u64,
        }
    }

    /// Add a data channel to a specific peer
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Target peer identifier
    /// * `mode` - Delivery mode (Reliable or Unreliable)
    pub async fn add_peer_data_channel(
        &self,
        peer_id: &str,
        mode: crate::config::DataChannelMode,
    ) -> Result<()> {
        debug!(
            "Adding data channel to peer {} with mode {:?}",
            peer_id, mode
        );

        let peer = self.peer_manager.get_peer(peer_id).await?;
        peer.add_data_channel(mode).await
    }

    /// Set data channel message handler for a specific peer (T169)
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Target peer identifier
    /// * `handler` - Async callback for incoming messages
    pub async fn on_peer_data_channel_message<F, Fut>(
        &self,
        peer_id: &str,
        handler: F,
    ) -> Result<()>
    where
        F: Fn(crate::channels::DataChannelMessage) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        debug!("Setting data channel message handler for peer: {}", peer_id);

        let peer = self.peer_manager.get_peer(peer_id).await?;
        peer.on_data_channel_message(handler).await
    }

    /// Check if a peer has a data channel configured
    pub async fn peer_has_data_channel(&self, peer_id: &str) -> Result<bool> {
        let peer = self.peer_manager.get_peer(peer_id).await?;
        Ok(peer.has_data_channel().await)
    }

    /// Check if a peer's data channel is open
    pub async fn is_peer_data_channel_open(&self, peer_id: &str) -> Result<bool> {
        let peer = self.peer_manager.get_peer(peer_id).await?;
        Ok(peer.is_data_channel_open().await)
    }

    // ========== Phase 4 (US2) Multi-Peer Synchronization ==========

    /// Collect synchronized audio frames from all peers, aligned by wall-clock time (T119)
    ///
    /// Pops frames from all peers' sync managers and aligns them by wall-clock timestamp
    /// within a 100ms window for synchronized mixing/processing.
    ///
    /// # Returns
    ///
    /// Vector of (peer_id, synced_frame) tuples for all peers that have frames ready
    pub async fn collect_aligned_frames(&self) -> Vec<(String, SyncedAudioFrame)> {
        let peers = self.peer_manager.list_connected_peers().await;
        let mut frames = Vec::new();

        // Collect frames from all peers
        for peer_info in &peers {
            if let Ok(peer) = self.peer_manager.get_peer(&peer_info.peer_id).await {
                if let Some(frame) = peer.pop_synced_audio_frame().await {
                    frames.push((peer_info.peer_id.clone(), frame));
                }
            }
        }

        if frames.is_empty() {
            return frames;
        }

        // Align frames by wall-clock timestamp within 100ms window
        // Find the reference timestamp (earliest frame)
        let reference_ts = frames
            .iter()
            .map(|(_, f)| f.wall_clock_timestamp_us)
            .min()
            .unwrap_or(0);

        // Filter frames within 100ms of reference
        const ALIGNMENT_WINDOW_US: u64 = 100_000; // 100ms
        frames.retain(|(_, f)| {
            let diff = f.wall_clock_timestamp_us.saturating_sub(reference_ts);
            diff <= ALIGNMENT_WINDOW_US
        });

        frames
    }

    /// Collect synchronized video frames from all peers, aligned by wall-clock time
    ///
    /// Similar to `collect_aligned_frames` but for video frames.
    ///
    /// # Returns
    ///
    /// Vector of (peer_id, synced_frame) tuples for all peers that have frames ready
    pub async fn collect_aligned_video_frames(&self) -> Vec<(String, SyncedVideoFrame)> {
        let peers = self.peer_manager.list_connected_peers().await;
        let mut frames = Vec::new();

        for peer_info in &peers {
            if let Ok(peer) = self.peer_manager.get_peer(&peer_info.peer_id).await {
                if let Some(frame) = peer.pop_synced_video_frame().await {
                    frames.push((peer_info.peer_id.clone(), frame));
                }
            }
        }

        if frames.is_empty() {
            return frames;
        }

        // Align frames by wall-clock timestamp within 100ms window
        let reference_ts = frames
            .iter()
            .map(|(_, f)| f.wall_clock_timestamp_us)
            .min()
            .unwrap_or(0);

        const ALIGNMENT_WINDOW_US: u64 = 100_000;
        frames.retain(|(_, f)| {
            let diff = f.wall_clock_timestamp_us.saturating_sub(reference_ts);
            diff <= ALIGNMENT_WINDOW_US
        });

        frames
    }

    /// Shutdown the transport
    ///
    /// Closes all peer connections, sessions, and disconnects from signaling server.
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down WebRTC transport");

        // Send disconnect notification
        {
            let client_guard = self.signaling_client.read().await;
            if let Some(client) = client_guard.as_ref() {
                if let Err(e) = client.disconnect().await {
                    warn!("Failed to send disconnect notification: {}", e);
                }
            }
        }

        // Clear all sessions
        self.session_manager.clear().await?;

        // Close all peer connections
        self.peer_manager.clear().await?;

        // Clear signaling client
        *self.signaling_client.write().await = None;

        info!("WebRTC transport shutdown complete");

        Ok(())
    }
}

// ============================================================================
// PipelineTransport Trait Implementation (T132-T135)
// ============================================================================

use async_trait::async_trait;
use remotemedia_core::manifest::Manifest;
use remotemedia_core::transport::{PipelineTransport, TransportData};

#[async_trait]
impl PipelineTransport for WebRtcTransport {
    /// Execute a pipeline with unary semantics (T133)
    ///
    /// This uses PipelineExecutor directly for pipeline execution (spec 026 migration).
    async fn execute(
        &self,
        manifest: Arc<Manifest>,
        input: TransportData,
    ) -> remotemedia_core::Result<TransportData> {
        use remotemedia_core::transport::PipelineExecutor;

        // Create pipeline executor (spec 026)
        let executor = PipelineExecutor::new()?;

        // Execute directly
        executor.execute_unary(manifest, input).await
    }

    /// Start a streaming pipeline session (T134)
    ///
    /// Creates a persistent session with continuous data flow.
    async fn stream(
        &self,
        manifest: Arc<Manifest>,
    ) -> remotemedia_core::Result<Box<dyn remotemedia_core::transport::StreamSession>> {
        use remotemedia_core::transport::PipelineExecutor;

        // Create pipeline executor (spec 026)
        let executor = PipelineExecutor::new()?;

        // Create streaming session using SessionHandle (spec 026)
        let session = executor.create_session(manifest).await?;

        // Wrap SessionHandle in StreamSession adapter
        Ok(Box::new(SessionHandleWrapper::new(session)))
    }
}

/// Wrapper to adapt SessionHandle to StreamSession trait (spec 026)
struct SessionHandleWrapper {
    handle: remotemedia_core::transport::SessionHandle,
}

impl SessionHandleWrapper {
    fn new(handle: remotemedia_core::transport::SessionHandle) -> Self {
        Self { handle }
    }
}

#[async_trait::async_trait]
impl remotemedia_core::transport::StreamSession for SessionHandleWrapper {
    fn session_id(&self) -> &str {
        &self.handle.session_id
    }

    async fn send_input(&mut self, data: TransportData) -> remotemedia_core::Result<()> {
        self.handle.send_input(data).await
    }

    async fn recv_output(&mut self) -> remotemedia_core::Result<Option<TransportData>> {
        self.handle.recv_output().await
    }

    async fn close(&mut self) -> remotemedia_core::Result<()> {
        self.handle.close().await
    }

    fn is_active(&self) -> bool {
        self.handle.is_active()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transport_creation() {
        let config = WebRtcTransportConfig::default();
        let transport = WebRtcTransport::new(config).unwrap();

        assert!(transport.local_peer_id.blocking_read().is_none());
    }

    #[test]
    fn test_invalid_config() {
        let mut config = WebRtcTransportConfig::default();
        config.max_peers = 0;

        assert!(WebRtcTransport::new(config).is_err());
    }

    #[test]
    fn test_get_capabilities() {
        let config = WebRtcTransportConfig::default();
        let transport = WebRtcTransport::new(config).unwrap();

        let caps = transport.get_capabilities();
        assert!(caps.contains(&"audio".to_string()));
        assert!(caps.contains(&"video".to_string()));
        assert!(caps.contains(&"data".to_string()));
    }

    #[tokio::test]
    async fn test_list_peers_empty() {
        let config = WebRtcTransportConfig::default();
        let transport = WebRtcTransport::new(config).unwrap();

        let peers = transport.list_peers().await;
        assert_eq!(peers.len(), 0);
    }

    #[tokio::test]
    async fn test_create_session() {
        let config = WebRtcTransportConfig::default();
        let transport = WebRtcTransport::new(config).unwrap();

        let session = transport
            .create_session("test-session".to_string())
            .await
            .unwrap();

        assert_eq!(session.session_id(), "test-session");
        assert_eq!(transport.session_count().await, 1);
        assert!(transport.has_session("test-session").await);
    }

    #[tokio::test]
    async fn test_get_session() {
        let config = WebRtcTransportConfig::default();
        let transport = WebRtcTransport::new(config).unwrap();

        transport
            .create_session("test-session".to_string())
            .await
            .unwrap();

        let session = transport.get_session("test-session").await.unwrap();
        assert_eq!(session.session_id(), "test-session");
    }

    #[tokio::test]
    async fn test_list_sessions() {
        let config = WebRtcTransportConfig::default();
        let transport = WebRtcTransport::new(config).unwrap();

        transport
            .create_session("session-1".to_string())
            .await
            .unwrap();
        transport
            .create_session("session-2".to_string())
            .await
            .unwrap();

        let sessions = transport.list_sessions().await;
        assert_eq!(sessions.len(), 2);
        assert!(sessions.contains(&"session-1".to_string()));
        assert!(sessions.contains(&"session-2".to_string()));
    }

    #[tokio::test]
    async fn test_remove_session() {
        let config = WebRtcTransportConfig::default();
        let transport = WebRtcTransport::new(config).unwrap();

        transport
            .create_session("test-session".to_string())
            .await
            .unwrap();

        assert_eq!(transport.session_count().await, 1);

        transport.remove_session("test-session").await.unwrap();

        assert_eq!(transport.session_count().await, 0);
        assert!(!transport.has_session("test-session").await);
    }

    #[tokio::test]
    async fn test_add_peer_to_session() {
        let config = WebRtcTransportConfig::default();
        let transport = WebRtcTransport::new(config).unwrap();

        transport
            .create_session("test-session".to_string())
            .await
            .unwrap();

        transport
            .add_peer_to_session("test-session", "peer-1".to_string())
            .await
            .unwrap();

        let session = transport.get_session("test-session").await.unwrap();
        let peers = session.list_peers().await;
        assert_eq!(peers.len(), 1);
        assert!(peers.contains(&"peer-1".to_string()));
    }

    #[tokio::test]
    async fn test_remove_peer_from_session() {
        let config = WebRtcTransportConfig::default();
        let transport = WebRtcTransport::new(config).unwrap();

        transport
            .create_session("test-session".to_string())
            .await
            .unwrap();

        transport
            .add_peer_to_session("test-session", "peer-1".to_string())
            .await
            .unwrap();

        transport
            .remove_peer_from_session("test-session", "peer-1")
            .await
            .unwrap();

        let session = transport.get_session("test-session").await.unwrap();
        let peers = session.list_peers().await;
        assert_eq!(peers.len(), 0);
    }
}
