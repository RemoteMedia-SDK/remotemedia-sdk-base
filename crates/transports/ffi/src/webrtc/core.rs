//! WebRTC server core implementation
//!
//! This module provides the core WebRTC server functionality that is shared
//! between Node.js and Python bindings. It wraps the `remotemedia-webrtc`
//! crate's signaling service and provides event forwarding.

use super::config::{PeerInfo, PeerState, ReconnectConfig, ServerState, WebRtcServerConfig};
use super::error::{WebRtcError, WebRtcResult};
use super::events::{
    DataReceivedEvent, ErrorCode, ErrorEvent, PeerConnectedEvent, PeerDisconnectedEvent,
    PipelineOutputEvent, SessionEvent, WebRtcEvent,
};
use remotemedia_core::manifest::Manifest;
use remotemedia_core::transport::PipelineExecutor;
use remotemedia_webrtc::signaling::{
    WebRtcEventBridge, WebSocketServerHandle, WebSocketSignalingServer,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

/// Channel capacity for event forwarding
const EVENT_CHANNEL_CAPACITY: usize = 1024;

/// WebRTC server core that wraps the signaling service
///
/// This struct is language-agnostic and is wrapped by language-specific
/// bindings (NapiWebRtcServer, PyWebRtcServer).
pub struct WebRtcServerCore {
    /// Unique server instance ID
    id: String,

    /// Server configuration
    config: WebRtcServerConfig,

    /// Current server state
    state: Arc<RwLock<ServerState>>,

    /// Connected peers
    peers: Arc<RwLock<HashMap<String, PeerInfo>>>,

    /// Active sessions
    sessions: Arc<RwLock<HashMap<String, SessionInfo>>>,

    /// Event sender for forwarding to FFI callbacks
    event_tx: mpsc::Sender<WebRtcEvent>,

    /// Event receiver for FFI layer to poll
    event_rx: Arc<RwLock<Option<mpsc::Receiver<WebRtcEvent>>>>,

    /// Shutdown flag
    shutdown: Arc<AtomicBool>,

    /// Tokio runtime handle for async operations (reserved for future use)
    #[allow(dead_code)]
    runtime_handle: Option<tokio::runtime::Handle>,

    /// WebSocket signaling server handle
    ws_server_handle: Arc<RwLock<Option<WebSocketServerHandle>>>,

    /// Current reconnection attempt count (for external signaling mode)
    reconnect_attempt: Arc<AtomicU32>,

    /// Flag indicating if reconnection is in progress
    reconnecting: Arc<AtomicBool>,
}

/// Session information for room/group management
#[derive(Debug, Clone)]
pub struct SessionInfo {
    /// Unique session identifier
    pub session_id: String,
    /// Peer IDs in this session
    pub peers: Vec<String>,
    /// Unix timestamp of creation
    pub created_at: u64,
    /// Custom session metadata
    pub metadata: HashMap<String, String>,
}

impl WebRtcServerCore {
    /// Create a new WebRTC server core
    pub fn new(config: WebRtcServerConfig) -> WebRtcResult<Self> {
        // Validate configuration
        config.validate()?;

        let (event_tx, event_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);

        Ok(Self {
            id: Uuid::new_v4().to_string(),
            config,
            state: Arc::new(RwLock::new(ServerState::Created)),
            peers: Arc::new(RwLock::new(HashMap::new())),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            event_tx,
            event_rx: Arc::new(RwLock::new(Some(event_rx))),
            shutdown: Arc::new(AtomicBool::new(false)),
            runtime_handle: tokio::runtime::Handle::try_current().ok(),
            ws_server_handle: Arc::new(RwLock::new(None)),
            reconnect_attempt: Arc::new(AtomicU32::new(0)),
            reconnecting: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Get server ID
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Get server configuration
    pub fn config(&self) -> &WebRtcServerConfig {
        &self.config
    }

    /// Get current server state
    pub async fn state(&self) -> ServerState {
        *self.state.read().await
    }

    /// Get connected peers
    pub async fn peers(&self) -> HashMap<String, PeerInfo> {
        self.peers.read().await.clone()
    }

    /// Get active sessions
    pub async fn sessions(&self) -> HashMap<String, SessionInfo> {
        self.sessions.read().await.clone()
    }

    /// Take the event receiver for polling
    ///
    /// This can only be called once - subsequent calls return None.
    pub async fn take_event_receiver(&self) -> Option<mpsc::Receiver<WebRtcEvent>> {
        self.event_rx.write().await.take()
    }

    /// Get a clone of the event sender for forwarding events
    pub fn event_sender(&self) -> mpsc::Sender<WebRtcEvent> {
        self.event_tx.clone()
    }

    /// Start the server
    ///
    /// For embedded mode: binds to the configured port and starts WebSocket signaling server
    /// For external mode: connects to the signaling server
    pub async fn start(&self) -> WebRtcResult<()> {
        let mut state = self.state.write().await;

        if *state != ServerState::Created {
            return Err(WebRtcError::invalid_state(format!(
                "Cannot start server in {:?} state",
                *state
            )));
        }

        *state = ServerState::Starting;
        drop(state);

        tracing::info!(
            server_id = %self.id,
            port = ?self.config.port,
            signaling_url = ?self.config.signaling_url,
            "Starting WebRTC server with WebSocket JSON-RPC 2.0 signaling"
        );

        // Get the port to bind to
        let port = self
            .config
            .port
            .ok_or_else(|| WebRtcError::config("Port is required for embedded signaling mode"))?;

        // Parse the pipeline manifest
        let manifest: Manifest = serde_json::from_value(self.config.manifest.clone())
            .map_err(|e| WebRtcError::config(format!("Invalid manifest: {}", e)))?;
        let manifest = Arc::new(manifest);

        // Create PipelineExecutor
        let runner = Arc::new(PipelineExecutor::new().map_err(|e| {
            WebRtcError::Internal(format!("Failed to create PipelineExecutor: {}", e))
        })?);

        // Build WebRTC transport configuration using the conversion helper
        let transport_config = Arc::new(self.config.to_webrtc_config());

        // Create bridge channel for event forwarding from signaling to FFI
        let (bridge_tx, mut bridge_rx) = mpsc::channel::<WebRtcEventBridge>(EVENT_CHANNEL_CAPACITY);

        let runner_host: Arc<dyn remotemedia_core::transport::PipelineSessionHost> = runner.clone();

        // Create WebSocket signaling server WITH event forwarding
        let ws_server = WebSocketSignalingServer::new_with_events(
            port as u16,
            Arc::clone(&transport_config),
            runner_host,
            Arc::clone(&manifest),
            Some(bridge_tx),
        );

        tracing::info!(
            "WebSocket signaling server listening on ws://0.0.0.0:{}/ws",
            port
        );

        // Start the WebSocket server
        let ws_handle = ws_server.start().await.map_err(|e| {
            WebRtcError::Internal(format!("Failed to start WebSocket server: {}", e))
        })?;

        // Store the server handle
        *self.ws_server_handle.write().await = Some(ws_handle);

        // Spawn task to convert bridge events to FFI events and forward them
        let event_tx = self.event_tx.clone();
        let peers = Arc::clone(&self.peers);
        tokio::spawn(async move {
            tracing::debug!("WebRTC event bridge task started");
            while let Some(bridge_event) = bridge_rx.recv().await {
                let ffi_event = convert_bridge_to_ffi_event(&bridge_event);

                // Sync peer state based on events
                match &bridge_event {
                    WebRtcEventBridge::PeerConnected {
                        peer_id,
                        capabilities,
                        metadata,
                    } => {
                        let peer_info = PeerInfo {
                            peer_id: peer_id.clone(),
                            capabilities: super::config::PeerCapabilities {
                                audio: capabilities.contains(&"audio".to_string()),
                                video: capabilities.contains(&"video".to_string()),
                                data: capabilities.contains(&"data".to_string()),
                            },
                            metadata: metadata
                                .iter()
                                .map(|(k, v)| (k.clone(), v.to_string()))
                                .collect(),
                            state: PeerState::Connected,
                            connected_at: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_secs())
                                .unwrap_or(0),
                        };
                        peers.write().await.insert(peer_id.clone(), peer_info);
                        tracing::debug!("Synced peer {} to FFI peers map", peer_id);
                    }
                    WebRtcEventBridge::PeerDisconnected { peer_id, .. } => {
                        peers.write().await.remove(peer_id);
                        tracing::debug!("Removed peer {} from FFI peers map", peer_id);
                    }
                    _ => {}
                }

                // Forward event to FFI callback system
                if let Err(e) = event_tx.send(ffi_event).await {
                    tracing::warn!("Failed to forward event to FFI: {}", e);
                }
            }
            tracing::debug!("WebRTC event bridge task ended");
        });

        let mut state = self.state.write().await;
        *state = ServerState::Running;

        tracing::info!(
            server_id = %self.id,
            port = port,
            "WebRTC server started with WebSocket JSON-RPC 2.0 signaling"
        );

        Ok(())
    }

    /// Shutdown the server gracefully
    ///
    /// Can be called from Created, Running, or Stopping states.
    /// If called from Created state, just transitions to Stopped.
    ///
    /// Graceful shutdown process:
    /// 1. Set shutdown flag to stop accepting new connections
    /// 2. Disconnect all active peers with "Server shutdown" reason
    /// 3. Emit peer_disconnected events for each peer
    /// 4. Clear all sessions
    /// 5. Shutdown the WebSocket signaling server
    /// 6. Transition to Stopped state
    pub async fn shutdown(&self) -> WebRtcResult<()> {
        self.shutdown_with_timeout(Duration::from_secs(30)).await
    }

    /// Shutdown the server with a custom timeout for peer disconnections
    ///
    /// If disconnecting all peers takes longer than the timeout, remaining
    /// peers are forcefully disconnected without sending disconnect messages.
    pub async fn shutdown_with_timeout(&self, timeout: Duration) -> WebRtcResult<()> {
        let mut state = self.state.write().await;

        if *state == ServerState::Stopped {
            return Ok(()); // Already stopped
        }

        // Allow shutdown from Created state (no cleanup needed)
        if *state == ServerState::Created {
            *state = ServerState::Stopped;
            self.shutdown.store(true, Ordering::SeqCst);
            return Ok(());
        }

        if *state != ServerState::Running && *state != ServerState::Stopping {
            return Err(WebRtcError::invalid_state(format!(
                "Cannot shutdown server in {:?} state",
                *state
            )));
        }

        *state = ServerState::Stopping;
        self.shutdown.store(true, Ordering::SeqCst);
        drop(state);

        tracing::info!(
            server_id = %self.id,
            timeout_secs = timeout.as_secs(),
            "Initiating graceful shutdown of WebRTC server"
        );

        // Shutdown WebSocket server first to stop accepting new connections
        if let Some(handle) = self.ws_server_handle.write().await.take() {
            tracing::info!("Shutting down WebSocket signaling server...");
            handle.shutdown().await;
            tracing::info!("WebSocket signaling server shut down complete");
        }

        // Collect all peer IDs before starting disconnection
        let peers = self.peers.read().await.keys().cloned().collect::<Vec<_>>();
        let peer_count = peers.len();

        if peer_count > 0 {
            tracing::info!(
                server_id = %self.id,
                peer_count = peer_count,
                "Disconnecting active peers during graceful shutdown"
            );

            // Create a future that disconnects all peers
            let disconnect_all = async {
                for peer_id in peers {
                    if let Err(e) = self
                        .disconnect_peer_graceful(&peer_id, "Server shutdown")
                        .await
                    {
                        tracing::warn!(
                            peer_id = %peer_id,
                            error = %e,
                            "Failed to gracefully disconnect peer during shutdown"
                        );
                    }
                }
            };

            // Apply timeout to peer disconnection
            match tokio::time::timeout(timeout, disconnect_all).await {
                Ok(()) => {
                    tracing::info!(
                        server_id = %self.id,
                        "All peers disconnected gracefully"
                    );
                }
                Err(_) => {
                    tracing::warn!(
                        server_id = %self.id,
                        timeout_secs = timeout.as_secs(),
                        "Graceful peer disconnection timed out, forcing cleanup"
                    );
                    // Force clear remaining peers
                    self.peers.write().await.clear();
                }
            }
        }

        // Clear sessions
        let session_count = self.sessions.read().await.len();
        if session_count > 0 {
            tracing::info!(
                server_id = %self.id,
                session_count = session_count,
                "Clearing sessions during shutdown"
            );
        }
        self.sessions.write().await.clear();

        let mut state = self.state.write().await;
        *state = ServerState::Stopped;

        tracing::info!(server_id = %self.id, "WebRTC server stopped");

        Ok(())
    }

    /// Gracefully disconnect a peer, ensuring events are emitted
    async fn disconnect_peer_graceful(&self, peer_id: &str, reason: &str) -> WebRtcResult<()> {
        // Check if peer exists
        let peer_exists = self.peers.read().await.contains_key(peer_id);
        if !peer_exists {
            return Ok(()); // Already disconnected
        }

        // Update peer state to Disconnecting
        if let Some(peer) = self.peers.write().await.get_mut(peer_id) {
            peer.state = PeerState::Disconnecting;
        }

        // TODO: Send disconnect message to peer via WebRTC data channel
        // For now, we just emit the event and clean up

        // Emit disconnect event
        let event = PeerDisconnectedEvent::new(peer_id.to_string()).with_reason(reason.to_string());
        let _ = self
            .event_tx
            .send(WebRtcEvent::PeerDisconnected(event))
            .await;

        // Remove peer from internal tracking
        self.peers.write().await.remove(peer_id);

        // Remove peer from all sessions and emit session events
        let mut sessions = self.sessions.write().await;
        for (session_id, session) in sessions.iter_mut() {
            if session.peers.contains(&peer_id.to_string()) {
                session.peers.retain(|p| p != peer_id);

                let _ = self
                    .event_tx
                    .send(WebRtcEvent::Session(SessionEvent::peer_left(
                        session_id.clone(),
                        peer_id.to_string(),
                    )))
                    .await;
            }
        }

        tracing::debug!(
            peer_id = %peer_id,
            reason = %reason,
            "Gracefully disconnected peer"
        );

        Ok(())
    }

    /// Check if shutdown was requested
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Send data to a specific peer
    pub async fn send_to_peer(&self, peer_id: &str, data: Vec<u8>) -> WebRtcResult<()> {
        let peers = self.peers.read().await;

        if !peers.contains_key(peer_id) {
            return Err(WebRtcError::PeerNotFound(peer_id.to_string()));
        }

        // TODO: Implement actual data sending via WebRTC data channel
        tracing::debug!(peer_id = %peer_id, data_len = data.len(), "Sending data to peer");

        Ok(())
    }

    /// Broadcast data to all connected peers
    pub async fn broadcast(&self, data: Vec<u8>) -> WebRtcResult<()> {
        let peers = self.peers.read().await.keys().cloned().collect::<Vec<_>>();

        for peer_id in peers {
            if let Err(e) = self.send_to_peer(&peer_id, data.clone()).await {
                tracing::warn!(peer_id = %peer_id, error = %e, "Failed to send to peer during broadcast");
            }
        }

        Ok(())
    }

    /// Disconnect a specific peer
    pub async fn disconnect_peer(&self, peer_id: &str, reason: Option<&str>) -> WebRtcResult<()> {
        self.disconnect_peer_internal(peer_id, reason).await
    }

    async fn disconnect_peer_internal(
        &self,
        peer_id: &str,
        reason: Option<&str>,
    ) -> WebRtcResult<()> {
        let mut peers = self.peers.write().await;

        if peers.remove(peer_id).is_none() {
            return Err(WebRtcError::PeerNotFound(peer_id.to_string()));
        }

        // Emit disconnect event
        let event = PeerDisconnectedEvent::new(peer_id.to_string());
        let event = if let Some(r) = reason {
            event.with_reason(r.to_string())
        } else {
            event
        };

        let _ = self
            .event_tx
            .send(WebRtcEvent::PeerDisconnected(event))
            .await;

        // Remove from all sessions
        drop(peers);
        let mut sessions = self.sessions.write().await;
        for (session_id, session) in sessions.iter_mut() {
            if session.peers.contains(&peer_id.to_string()) {
                session.peers.retain(|p| p != peer_id);

                let _ = self
                    .event_tx
                    .send(WebRtcEvent::Session(SessionEvent::peer_left(
                        session_id.clone(),
                        peer_id.to_string(),
                    )))
                    .await;
            }
        }

        tracing::info!(peer_id = %peer_id, reason = ?reason, "Disconnected peer");

        Ok(())
    }

    /// Create a new session (room)
    pub async fn create_session(
        &self,
        session_id: &str,
        metadata: Option<HashMap<String, String>>,
    ) -> WebRtcResult<SessionInfo> {
        let mut sessions = self.sessions.write().await;

        if sessions.contains_key(session_id) {
            return Err(WebRtcError::Internal(format!(
                "Session already exists: {}",
                session_id
            )));
        }

        let session = SessionInfo {
            session_id: session_id.to_string(),
            peers: Vec::new(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            metadata: metadata.unwrap_or_default(),
        };

        sessions.insert(session_id.to_string(), session.clone());

        tracing::info!(session_id = %session_id, "Created session");

        Ok(session)
    }

    /// Get a session by ID
    pub async fn get_session(&self, session_id: &str) -> Option<SessionInfo> {
        self.sessions.read().await.get(session_id).cloned()
    }

    /// Delete a session
    pub async fn delete_session(&self, session_id: &str) -> WebRtcResult<()> {
        let mut sessions = self.sessions.write().await;

        if sessions.remove(session_id).is_none() {
            return Err(WebRtcError::SessionNotFound(session_id.to_string()));
        }

        tracing::info!(session_id = %session_id, "Deleted session");

        Ok(())
    }

    /// Add a peer to a session
    pub async fn add_peer_to_session(&self, session_id: &str, peer_id: &str) -> WebRtcResult<()> {
        // Verify peer exists
        let peers = self.peers.read().await;
        if !peers.contains_key(peer_id) {
            return Err(WebRtcError::PeerNotFound(peer_id.to_string()));
        }
        drop(peers);

        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| WebRtcError::SessionNotFound(session_id.to_string()))?;

        if !session.peers.contains(&peer_id.to_string()) {
            session.peers.push(peer_id.to_string());

            let _ = self
                .event_tx
                .send(WebRtcEvent::Session(SessionEvent::peer_joined(
                    session_id.to_string(),
                    peer_id.to_string(),
                )))
                .await;
        }

        Ok(())
    }

    /// Remove a peer from a session
    pub async fn remove_peer_from_session(
        &self,
        session_id: &str,
        peer_id: &str,
    ) -> WebRtcResult<()> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| WebRtcError::SessionNotFound(session_id.to_string()))?;

        if session.peers.contains(&peer_id.to_string()) {
            session.peers.retain(|p| p != peer_id);

            let _ = self
                .event_tx
                .send(WebRtcEvent::Session(SessionEvent::peer_left(
                    session_id.to_string(),
                    peer_id.to_string(),
                )))
                .await;
        }

        Ok(())
    }

    /// Broadcast to all peers in a session
    pub async fn broadcast_to_session(&self, session_id: &str, data: Vec<u8>) -> WebRtcResult<()> {
        let sessions = self.sessions.read().await;
        let session = sessions
            .get(session_id)
            .ok_or_else(|| WebRtcError::SessionNotFound(session_id.to_string()))?;

        let peers = session.peers.clone();
        drop(sessions);

        for peer_id in peers {
            if let Err(e) = self.send_to_peer(&peer_id, data.clone()).await {
                tracing::warn!(
                    peer_id = %peer_id,
                    session_id = %session_id,
                    error = %e,
                    "Failed to send to peer during session broadcast"
                );
            }
        }

        Ok(())
    }

    // ============ Internal event emission methods ============
    // Note: Most event emission is now handled by the event bridge task spawned in start().
    // The bridge converts WebRtcEventBridge events from the signaling layer to FFI WebRtcEvent.

    /// Emit an error event (for internal error reporting)
    #[allow(dead_code)]
    pub(crate) async fn emit_error(&self, event: ErrorEvent) {
        let _ = self.event_tx.send(WebRtcEvent::Error(event)).await;
    }

    // ============ Reconnection logic for external signaling ============

    /// Calculate backoff delay for reconnection attempt
    fn calculate_backoff(&self, attempt: u32, config: &ReconnectConfig) -> Duration {
        let delay_ms = (config.initial_backoff_ms as f64
            * config.backoff_multiplier.powi(attempt as i32))
        .min(config.max_backoff_ms as f64) as u64;
        Duration::from_millis(delay_ms)
    }

    /// Attempt to connect to external signaling with reconnection logic
    ///
    /// This method implements exponential backoff for reconnection attempts.
    /// It emits `ReconnectAttempt` events during reconnection and `ReconnectFailed`
    /// if all attempts are exhausted.
    pub async fn connect_with_retry<F, Fut>(&self, connect_fn: F) -> WebRtcResult<()>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: std::future::Future<Output = WebRtcResult<()>> + Send,
    {
        let reconnect_config = &self.config.reconnect;
        let max_attempts = reconnect_config.max_attempts;

        // If reconnection is disabled, try once
        if max_attempts == 0 {
            return connect_fn().await;
        }

        let mut last_error: Option<WebRtcError> = None;

        for attempt in 0..=max_attempts {
            // Check for shutdown
            if self.shutdown.load(Ordering::SeqCst) {
                return Err(WebRtcError::internal(
                    "Shutdown requested during reconnection",
                ));
            }

            // Update reconnection state
            self.reconnect_attempt.store(attempt, Ordering::SeqCst);

            if attempt > 0 {
                self.reconnecting.store(true, Ordering::SeqCst);

                // Emit reconnect attempt event
                let event = ErrorEvent::new(
                    ErrorCode::ReconnectAttempt,
                    format!(
                        "Reconnection attempt {}/{} - waiting {:?}",
                        attempt,
                        max_attempts,
                        self.calculate_backoff(attempt - 1, reconnect_config)
                    ),
                );
                self.emit_error(event).await;

                // Wait with exponential backoff
                let backoff = self.calculate_backoff(attempt - 1, reconnect_config);
                tokio::time::sleep(backoff).await;
            }

            // Attempt connection
            match connect_fn().await {
                Ok(()) => {
                    // Success! Reset reconnection state
                    self.reconnecting.store(false, Ordering::SeqCst);
                    self.reconnect_attempt.store(0, Ordering::SeqCst);

                    if attempt > 0 {
                        tracing::info!(
                            server_id = %self.id,
                            attempt = attempt,
                            "Reconnected to external signaling server"
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(
                        server_id = %self.id,
                        attempt = attempt,
                        max_attempts = max_attempts,
                        error = %e,
                        "Connection attempt failed"
                    );
                    last_error = Some(e);
                }
            }
        }

        // All attempts exhausted
        self.reconnecting.store(false, Ordering::SeqCst);

        let error_msg = format!(
            "Failed to connect after {} attempts: {}",
            max_attempts + 1,
            last_error
                .as_ref()
                .map(|e| e.to_string())
                .unwrap_or_default()
        );

        // Emit reconnect failed event
        let event = ErrorEvent::new(ErrorCode::ReconnectFailed, error_msg.clone());
        self.emit_error(event).await;

        Err(WebRtcError::signaling(error_msg))
    }

    /// Start external signaling connection with reconnection support
    ///
    /// This is called when the server is configured with `signaling_url` instead of `port`.
    /// The connection will automatically retry with exponential backoff according to
    /// the `reconnect` configuration.
    pub async fn start_external(&self) -> WebRtcResult<()> {
        let signaling_url = self.config.signaling_url.as_ref().ok_or_else(|| {
            WebRtcError::config("signaling_url is required for external signaling mode")
        })?;

        let mut state = self.state.write().await;

        if *state != ServerState::Created {
            return Err(WebRtcError::invalid_state(format!(
                "Cannot start server in {:?} state",
                *state
            )));
        }

        *state = ServerState::Starting;
        drop(state);

        tracing::info!(
            server_id = %self.id,
            signaling_url = %signaling_url,
            max_reconnect_attempts = self.config.reconnect.max_attempts,
            "Starting WebRTC server in external signaling mode"
        );

        // Clone values needed for the closure
        let signaling_url = signaling_url.clone();
        let _config = self.config.clone();

        // Connect with retry logic
        let result = self.connect_with_retry(|| async {
            // TODO: Implement actual external signaling client connection
            // This would connect to the external signaling server via gRPC or WebSocket
            // For now, we simulate the connection check
            tracing::debug!(signaling_url = %signaling_url, "Attempting to connect to external signaling server");

            // Placeholder: In a real implementation, this would:
            // 1. Create a gRPC/WebSocket client to the signaling server
            // 2. Establish the connection and authenticate
            // 3. Start receiving peer events from the signaling server

            // For now, return an error to indicate external signaling is not yet implemented
            Err(WebRtcError::signaling(format!(
                "External signaling connection to {} is not yet implemented. Use embedded signaling (port) instead.",
                signaling_url
            )))
        }).await;

        match result {
            Ok(()) => {
                let mut state = self.state.write().await;
                *state = ServerState::Running;
                tracing::info!(
                    server_id = %self.id,
                    signaling_url = %signaling_url,
                    "WebRTC server started in external signaling mode"
                );
                Ok(())
            }
            Err(e) => {
                let mut state = self.state.write().await;
                *state = ServerState::Created; // Reset to Created state on failure
                Err(e)
            }
        }
    }

    /// Check if the server is currently reconnecting
    pub fn is_reconnecting(&self) -> bool {
        self.reconnecting.load(Ordering::SeqCst)
    }

    /// Get the current reconnection attempt number
    pub fn reconnect_attempt(&self) -> u32 {
        self.reconnect_attempt.load(Ordering::SeqCst)
    }

    /// Get the reconnection configuration
    pub fn reconnect_config(&self) -> &ReconnectConfig {
        &self.config.reconnect
    }
}

/// Convert a bridge event from the WebRTC signaling layer to an FFI event
fn convert_bridge_to_ffi_event(bridge_event: &WebRtcEventBridge) -> WebRtcEvent {
    match bridge_event {
        WebRtcEventBridge::PeerConnected {
            peer_id,
            capabilities,
            metadata,
        } => {
            let ffi_capabilities = super::config::PeerCapabilities {
                audio: capabilities.contains(&"audio".to_string()),
                video: capabilities.contains(&"video".to_string()),
                data: capabilities.contains(&"data".to_string()),
            };
            let ffi_metadata: HashMap<String, String> = metadata
                .iter()
                .map(|(k, v)| (k.clone(), v.to_string()))
                .collect();

            WebRtcEvent::PeerConnected(PeerConnectedEvent {
                peer_id: peer_id.clone(),
                capabilities: ffi_capabilities,
                metadata: ffi_metadata,
            })
        }
        WebRtcEventBridge::PeerDisconnected { peer_id, reason } => {
            let mut event = PeerDisconnectedEvent::new(peer_id.clone());
            if let Some(r) = reason {
                event = event.with_reason(r.clone());
            }
            WebRtcEvent::PeerDisconnected(event)
        }
        WebRtcEventBridge::PipelineOutput {
            peer_id,
            data_json,
            timestamp_ns,
        } => {
            // Deserialize RuntimeData from JSON
            let runtime_data = serde_json::from_str(data_json).unwrap_or_else(|e| {
                tracing::warn!("Failed to deserialize pipeline output RuntimeData: {}", e);
                remotemedia_core::data::RuntimeData::Text(format!("Error deserializing: {}", e))
            });

            WebRtcEvent::PipelineOutput(PipelineOutputEvent {
                peer_id: peer_id.clone(),
                data: runtime_data,
                timestamp: *timestamp_ns,
            })
        }
        WebRtcEventBridge::DataReceived {
            peer_id,
            data,
            timestamp_ns,
        } => WebRtcEvent::DataReceived(DataReceivedEvent {
            peer_id: peer_id.clone(),
            data: data.clone(),
            timestamp: *timestamp_ns,
        }),
        WebRtcEventBridge::Error {
            code,
            message,
            peer_id,
        } => {
            let error_code = match code {
                remotemedia_webrtc::signaling::WebRtcErrorCode::SignalingError => {
                    ErrorCode::SignalingError
                }
                remotemedia_webrtc::signaling::WebRtcErrorCode::PeerError => ErrorCode::PeerError,
                remotemedia_webrtc::signaling::WebRtcErrorCode::PipelineError => {
                    ErrorCode::PipelineError
                }
                remotemedia_webrtc::signaling::WebRtcErrorCode::InternalError => {
                    ErrorCode::InternalError
                }
            };
            let mut event = ErrorEvent::new(error_code, message.clone());
            if let Some(pid) = peer_id {
                event = event.with_peer(pid.clone());
            }
            WebRtcEvent::Error(event)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> WebRtcServerConfig {
        use std::sync::atomic::{AtomicU16, Ordering};
        // Use unique high ports to avoid "Address already in use" errors in parallel tests
        static PORT_COUNTER: AtomicU16 = AtomicU16::new(50100);
        let port = PORT_COUNTER.fetch_add(1, Ordering::SeqCst);

        WebRtcServerConfig {
            port: Some(port),
            signaling_url: None,
            manifest: serde_json::json!({
                "version": "1.0",
                "metadata": { "name": "test-pipeline" },
                "nodes": [],
                "connections": []
            }),
            stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_server_lifecycle() {
        let core = WebRtcServerCore::new(test_config()).unwrap();

        assert_eq!(core.state().await, ServerState::Created);

        core.start().await.unwrap();
        assert_eq!(core.state().await, ServerState::Running);

        core.shutdown().await.unwrap();
        assert_eq!(core.state().await, ServerState::Stopped);
    }

    #[tokio::test]
    async fn test_session_management() {
        let core = WebRtcServerCore::new(test_config()).unwrap();
        core.start().await.unwrap();

        // Create session
        let session = core
            .create_session(
                "room-1",
                Some(HashMap::from([("name".to_string(), "Test".to_string())])),
            )
            .await
            .unwrap();

        assert_eq!(session.session_id, "room-1");
        assert!(session.peers.is_empty());
        assert_eq!(session.metadata.get("name"), Some(&"Test".to_string()));

        // Get session
        let fetched = core.get_session("room-1").await.unwrap();
        assert_eq!(fetched.session_id, "room-1");

        // Delete session
        core.delete_session("room-1").await.unwrap();
        assert!(core.get_session("room-1").await.is_none());
    }

    #[tokio::test]
    async fn test_event_receiver() {
        let core = WebRtcServerCore::new(test_config()).unwrap();

        // Take receiver
        let rx = core.take_event_receiver().await;
        assert!(rx.is_some());

        // Second take returns None
        let rx2 = core.take_event_receiver().await;
        assert!(rx2.is_none());
    }

    fn external_signaling_config() -> WebRtcServerConfig {
        WebRtcServerConfig {
            port: None,
            signaling_url: Some("grpc://localhost:50052".to_string()),
            manifest: serde_json::json!({
                "version": "1.0",
                "metadata": { "name": "external-signaling-test" },
                "nodes": [],
                "connections": []
            }),
            stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
            reconnect: ReconnectConfig {
                max_attempts: 2,
                initial_backoff_ms: 10, // Short for tests
                max_backoff_ms: 50,
                backoff_multiplier: 2.0,
            },
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_backoff_calculation() {
        let core = WebRtcServerCore::new(external_signaling_config()).unwrap();
        let config = core.reconnect_config();

        // First attempt: 10ms
        let delay0 = core.calculate_backoff(0, config);
        assert_eq!(delay0.as_millis(), 10);

        // Second attempt: 10ms * 2 = 20ms
        let delay1 = core.calculate_backoff(1, config);
        assert_eq!(delay1.as_millis(), 20);

        // Third attempt: 10ms * 4 = 40ms
        let delay2 = core.calculate_backoff(2, config);
        assert_eq!(delay2.as_millis(), 40);

        // Fourth attempt: should be capped at max_backoff_ms (50ms)
        let delay3 = core.calculate_backoff(3, config);
        assert_eq!(delay3.as_millis(), 50);
    }

    #[tokio::test]
    async fn test_connect_with_retry_success() {
        let core = WebRtcServerCore::new(external_signaling_config()).unwrap();

        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = Arc::clone(&attempt_count);

        // Succeed on first attempt
        let result = core
            .connect_with_retry(|| {
                let count = Arc::clone(&attempt_count_clone);
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(attempt_count.load(Ordering::SeqCst), 1);
        assert!(!core.is_reconnecting());
        assert_eq!(core.reconnect_attempt(), 0);
    }

    #[tokio::test]
    async fn test_connect_with_retry_eventual_success() {
        let core = WebRtcServerCore::new(external_signaling_config()).unwrap();

        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = Arc::clone(&attempt_count);

        // Succeed on second attempt
        let result = core
            .connect_with_retry(|| {
                let count = Arc::clone(&attempt_count_clone);
                async move {
                    let current = count.fetch_add(1, Ordering::SeqCst);
                    if current < 1 {
                        Err(WebRtcError::signaling("Connection refused"))
                    } else {
                        Ok(())
                    }
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(attempt_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_connect_with_retry_exhausted() {
        let core = WebRtcServerCore::new(external_signaling_config()).unwrap();

        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = Arc::clone(&attempt_count);

        // Always fail
        let result = core
            .connect_with_retry(|| {
                let count = Arc::clone(&attempt_count_clone);
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Err(WebRtcError::signaling("Connection refused"))
                }
            })
            .await;

        assert!(result.is_err());
        // max_attempts=2, so we try: attempt 0, 1, 2 = 3 total
        assert_eq!(attempt_count.load(Ordering::SeqCst), 3);
        assert!(!core.is_reconnecting());
    }

    #[tokio::test]
    async fn test_reconnection_state() {
        let core = WebRtcServerCore::new(external_signaling_config()).unwrap();

        assert!(!core.is_reconnecting());
        assert_eq!(core.reconnect_attempt(), 0);
    }
}
