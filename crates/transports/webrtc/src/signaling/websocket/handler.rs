//! WebSocket message handler for JSON-RPC 2.0 signaling
//!
//! Handles individual WebSocket connections and processes JSON-RPC messages.

use super::control_handlers::{handle_control_method, ControlSessionState};
use super::events::WebRtcEventBridge;
use crate::config::WebRtcTransportConfig;
use crate::peer::ServerPeer;
use crate::signaling::protocol::{
    error_codes, IceCandidateParams, JsonRpcError, JsonRpcRequest, JsonRpcResponse,
    PeerAnnounceParams, PeerAnswerParams, PeerDisconnectParams, PeerOfferParams,
    PeerStateChangeParams,
};
use futures_util::{SinkExt, StreamExt};
use remotemedia_core::{
    manifest::Manifest,
    transport::{PipelineExecutor, PipelineSessionHost},
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, RwLock};
use tokio_tungstenite::{
    accept_async,
    tungstenite::{Message, Result as WsResult},
};
use tracing::{debug, error, info, warn};

/// Peer connection info for WebSocket clients
pub struct WsPeerConnection {
    pub peer_id: String,
    pub capabilities: Vec<String>,
    pub metadata: HashMap<String, Value>,
    pub connected_at: u64,
    pub tx: mpsc::Sender<String>,
}

/// Shared state across all WebSocket connections
pub struct SharedState {
    pub peers: Arc<RwLock<HashMap<String, WsPeerConnection>>>,
    pub server_peers: Arc<RwLock<HashMap<String, Arc<ServerPeer>>>>,
    pub config: Arc<WebRtcTransportConfig>,
    pub runner: Arc<dyn PipelineSessionHost>,
    pub manifest: Arc<Manifest>,
    /// Optional event sender for FFI integration
    /// When set, peer connect/disconnect and pipeline events are forwarded
    pub event_tx: Option<mpsc::Sender<WebRtcEventBridge>>,
}

impl SharedState {
    /// Create new shared state without event forwarding
    pub fn new(
        config: Arc<WebRtcTransportConfig>,
        runner: Arc<dyn PipelineSessionHost>,
        manifest: Arc<Manifest>,
    ) -> Self {
        Self::new_with_events(config, runner, manifest, None)
    }

    /// Create new shared state with optional event forwarding
    pub fn new_with_events(
        config: Arc<WebRtcTransportConfig>,
        runner: Arc<dyn PipelineSessionHost>,
        manifest: Arc<Manifest>,
        event_tx: Option<mpsc::Sender<WebRtcEventBridge>>,
    ) -> Self {
        Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
            server_peers: Arc::new(RwLock::new(HashMap::new())),
            config,
            runner,
            manifest,
            event_tx,
        }
    }

    /// Emit an event if event_tx is configured
    pub async fn emit_event(&self, event: WebRtcEventBridge) {
        if let Some(ref tx) = self.event_tx {
            if let Err(e) = tx.send(event).await {
                warn!("Failed to emit WebRTC event: {}", e);
            }
        }
    }
}

/// Handle a single WebSocket connection
pub async fn handle_connection(stream: TcpStream, state: Arc<SharedState>) -> WsResult<()> {
    let addr = stream.peer_addr()?;
    info!("New WebSocket connection from: {}", addr);

    let ws_stream = accept_async(stream).await?;

    let (ws_tx, ws_rx) = ws_stream.split();

    // Channel for sending messages back to this connection
    let (tx, mut rx) = mpsc::channel::<String>(128);

    // Current peer ID for this connection
    let peer_id: Arc<RwLock<Option<String>>> = Arc::new(RwLock::new(None));

    // Per-connection control-plane subscriptions. Dropped on disconnect
    // to abort forwarder tasks and release broadcast receivers.
    let control_state: Arc<ControlSessionState> = ControlSessionState::new();

    // Task to forward messages from channel to WebSocket
    let ws_tx = Arc::new(RwLock::new(ws_tx));
    let ws_tx_clone = Arc::clone(&ws_tx);
    let forward_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let mut ws_tx = ws_tx_clone.write().await;
            if let Err(e) = ws_tx.send(Message::Text(msg)).await {
                error!("Failed to send WebSocket message: {}", e);
                break;
            }
        }
    });

    // Process incoming messages
    let state_clone = Arc::clone(&state);
    let peer_id_clone = Arc::clone(&peer_id);
    let tx_clone = tx.clone();

    let mut ws_rx = ws_rx;
    while let Some(msg) = ws_rx.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Err(e) = handle_message(
                    &text,
                    &state_clone,
                    &peer_id_clone,
                    &tx_clone,
                    &control_state,
                )
                .await
                {
                    error!("Error handling message: {}", e);
                    // Send error response
                    let error_response =
                        JsonRpcError::new(error_codes::INTERNAL_ERROR, e.to_string(), json!(null));
                    if let Ok(json) = error_response.to_json() {
                        let _ = tx_clone.send(json).await;
                    }
                }
            }
            Ok(Message::Close(_)) => {
                info!("WebSocket connection closed by client: {}", addr);
                break;
            }
            Ok(Message::Ping(data)) => {
                let mut ws_tx = ws_tx.write().await;
                let _ = ws_tx.send(Message::Pong(data)).await;
            }
            Err(e) => {
                error!("WebSocket error: {}", e);
                break;
            }
            _ => {}
        }
    }

    // Abort any active control-bus subscription forwarders. Must run
    // before ServerPeer shutdown so we don't publish events into a
    // half-torn-down session.
    control_state.shutdown().await;

    // Cleanup on disconnect
    if let Some(peer_id) = peer_id.read().await.clone() {
        info!("Cleaning up peer: {}", peer_id);

        // Emit peer disconnected event for FFI integration BEFORE cleanup
        state
            .emit_event(WebRtcEventBridge::peer_disconnected(
                peer_id.clone(),
                Some("WebSocket connection closed".to_string()),
            ))
            .await;

        // Shutdown ServerPeer if exists
        if let Some(server_peer) = state.server_peers.write().await.remove(&peer_id) {
            info!(
                "Shutting down ServerPeer for disconnected peer: {}",
                peer_id
            );
            if let Err(e) = server_peer.shutdown().await {
                error!("Failed to shutdown ServerPeer: {}", e);
            }
        }

        // Remove from peers
        state.peers.write().await.remove(&peer_id);

        // Broadcast peer left
        broadcast_peer_left(&state, &peer_id).await;
    }

    forward_task.abort();

    Ok(())
}

/// Handle a JSON-RPC message
async fn handle_message(
    text: &str,
    state: &Arc<SharedState>,
    peer_id: &Arc<RwLock<Option<String>>>,
    tx: &mpsc::Sender<String>,
    control_state: &Arc<ControlSessionState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Parse JSON-RPC request
    let request: JsonRpcRequest = match serde_json::from_str(text) {
        Ok(req) => req,
        Err(e) => {
            let error = JsonRpcError::new(
                error_codes::PARSE_ERROR,
                format!("Failed to parse JSON: {}", e),
                json!(null),
            );
            tx.send(error.to_json()?).await?;
            return Ok(());
        }
    };

    let request_id = request.id.clone().unwrap_or(json!(null));

    // Control-plane methods proxy the Session Control Bus on the
    // peer's session. They require peer.announce to have run and the
    // SDP exchange to have finished (session_id populated).
    if request.method.starts_with("control.") {
        let current_peer_id = peer_id.read().await.clone();
        let current_peer_id = match current_peer_id {
            Some(id) => id,
            None => {
                let error = JsonRpcError::new(
                    error_codes::INVALID_REQUEST,
                    "Not announced: call peer.announce before any control.* method".to_string(),
                    request_id,
                );
                tx.send(error.to_json()?).await?;
                return Ok(());
            }
        };
        if let Some(result) = handle_control_method(
            &request.method,
            &request.params,
            &request_id,
            state,
            &current_peer_id,
            tx,
            control_state,
        )
        .await
        {
            match result {
                Ok(response_json) => {
                    tx.send(response_json).await?;
                }
                Err(msg) => {
                    let error = JsonRpcError::new(error_codes::INVALID_PARAMS, msg, request_id);
                    tx.send(error.to_json()?).await?;
                }
            }
            return Ok(());
        }
    }

    match request.method.as_str() {
        "peer.announce" => {
            handle_announce(request, state, peer_id, tx).await?;
        }
        "peer.offer" => {
            handle_offer(request, state, peer_id, tx).await?;
        }
        "peer.answer" => {
            handle_answer(request, state, peer_id, tx).await?;
        }
        "peer.ice_candidate" => {
            handle_ice_candidate(request, state, peer_id, tx).await?;
        }
        "peer.disconnect" => {
            handle_disconnect(request, state, tx).await?;
        }
        "peer.list" | "list_peers" => {
            handle_list_peers(request, state, tx).await?;
        }
        "server.info" => {
            handle_server_info(request, tx).await?;
        }
        _ => {
            let error = JsonRpcError::new(
                error_codes::METHOD_NOT_FOUND,
                format!("Unknown method: {}", request.method),
                request_id,
            );
            tx.send(error.to_json()?).await?;
        }
    }

    Ok(())
}

/// Handle peer.announce
async fn handle_announce(
    request: JsonRpcRequest,
    state: &Arc<SharedState>,
    peer_id_ref: &Arc<RwLock<Option<String>>>,
    tx: &mpsc::Sender<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let request_id = request.id.clone().unwrap_or(json!(null));

    let params: PeerAnnounceParams = serde_json::from_value(request.params.clone())?;

    let mut peers = state.peers.write().await;

    // Check if peer already exists
    if peers.contains_key(&params.peer_id) {
        let error = JsonRpcError::new(
            error_codes::INVALID_PARAMS,
            format!("Peer {} already announced", params.peer_id),
            request_id,
        );
        tx.send(error.to_json()?).await?;
        return Ok(());
    }

    // Get list of existing peers
    let other_peers: Vec<String> = peers.keys().cloned().collect();

    // Register peer
    let peer_conn = WsPeerConnection {
        peer_id: params.peer_id.clone(),
        capabilities: params.capabilities.clone(),
        metadata: params.user_data.clone().unwrap_or_default(),
        connected_at: current_timestamp(),
        tx: tx.clone(),
    };

    peers.insert(params.peer_id.clone(), peer_conn);
    *peer_id_ref.write().await = Some(params.peer_id.clone());

    debug!(
        "Peer announced: {} (total: {})",
        params.peer_id,
        peers.len()
    );

    drop(peers);

    // Emit peer connected event for FFI integration
    state
        .emit_event(WebRtcEventBridge::peer_connected(
            params.peer_id.clone(),
            params.capabilities.clone(),
            params.user_data.clone().unwrap_or_default(),
        ))
        .await;

    // Send success response
    let response = JsonRpcResponse::new(
        json!({
            "success": true,
            "peer_id": params.peer_id,
            "peers_count": other_peers.len() + 1,
            "other_peers": other_peers
        }),
        request_id,
    );
    tx.send(response.to_json()?).await?;

    // Broadcast peer joined to others
    broadcast_peer_joined(
        state,
        &params.peer_id,
        &params.capabilities,
        &params.user_data,
    )
    .await;

    Ok(())
}

/// Handle peer.offer
async fn handle_offer(
    request: JsonRpcRequest,
    state: &Arc<SharedState>,
    peer_id_ref: &Arc<RwLock<Option<String>>>,
    tx: &mpsc::Sender<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let request_id = request.id.clone().unwrap_or(json!(null));

    let from_peer_id = peer_id_ref.read().await.clone();
    let from_peer_id = match from_peer_id {
        Some(id) => id,
        None => {
            let error = JsonRpcError::new(
                error_codes::INVALID_REQUEST,
                "Not announced: call peer.announce first".to_string(),
                request_id,
            );
            tx.send(error.to_json()?).await?;
            return Ok(());
        }
    };

    let params: PeerOfferParams = serde_json::from_value(request.params.clone())?;

    // Check if target is remotemedia-server
    if params.to == "remotemedia-server" {
        info!("Creating ServerPeer for offer from {}", from_peer_id);
        debug!(
            "Offer SDP from {}: {} bytes",
            from_peer_id,
            params.sdp.len()
        );

        // Basic SDP validation (T011)
        if !params.sdp.starts_with("v=0") {
            let error = JsonRpcError::with_data(
                error_codes::OFFER_INVALID,
                "Invalid SDP offer: must start with v=0".to_string(),
                json!({"reason": "Missing SDP version line"}),
                request_id,
            );
            tx.send(error.to_json()?).await?;
            return Ok(());
        }
        if !params.sdp.contains("m=") {
            let error = JsonRpcError::with_data(
                error_codes::OFFER_INVALID,
                "Invalid SDP offer: no media sections".to_string(),
                json!({"reason": "Missing m= line"}),
                request_id,
            );
            tx.send(error.to_json()?).await?;
            return Ok(());
        }

        // Clean up any existing ServerPeer
        if let Some(old_server_peer) = state.server_peers.write().await.remove(&from_peer_id) {
            info!(
                "Shutting down existing ServerPeer for peer {}",
                from_peer_id
            );
            if let Err(e) = old_server_peer.shutdown().await {
                warn!("Failed to shutdown old ServerPeer: {}", e);
            }
        }

        // Create ServerPeer with event forwarding if configured
        let server_peer: Arc<ServerPeer> = match ServerPeer::new_with_events(
            from_peer_id.clone(),
            &*state.config,
            Arc::clone(&state.runner),
            Arc::clone(&state.manifest),
            state.event_tx.clone(),
        )
        .await
        {
            Ok(peer) => Arc::new(peer),
            Err(e) => {
                error!("Failed to create ServerPeer: {}", e);
                let error = JsonRpcError::new(
                    error_codes::INTERNAL_ERROR,
                    format!("Failed to create ServerPeer: {}", e),
                    request_id,
                );
                tx.send(error.to_json()?).await?;
                return Ok(());
            }
        };

        // Handle offer and get SDP answer
        let answer_sdp: String = match server_peer.handle_offer(params.sdp).await {
            Ok(sdp) => sdp,
            Err(e) => {
                error!("ServerPeer failed to handle offer: {}", e);
                let error = JsonRpcError::with_data(
                    error_codes::OFFER_INVALID,
                    format!("Failed to handle offer: {}", e),
                    json!({"reason": e.to_string(), "peer_id": from_peer_id}),
                    request_id,
                );
                tx.send(error.to_json()?).await?;
                return Ok(());
            }
        };

        info!("ServerPeer generated answer for {}", from_peer_id);
        debug!("Answer SDP: {} bytes", answer_sdp.len());

        // Store server peer
        state
            .server_peers
            .write()
            .await
            .insert(from_peer_id.clone(), Arc::clone(&server_peer));

        // Set up ICE candidate callback to forward server candidates to client (T014)
        let tx_clone = tx.clone();
        let from_peer_id_clone = from_peer_id.clone();
        server_peer
            .peer_connection()
            .peer_connection()
            .on_ice_candidate(Box::new(move |candidate| {
                let tx = tx_clone.clone();
                let peer_id = from_peer_id_clone.clone();
                Box::pin(async move {
                    if let Some(candidate) = candidate {
                        // Convert RTCIceCandidate to RTCIceCandidateInit for JSON serialization
                        match candidate.to_json() {
                            Ok(candidate_init) => {
                                debug!(
                                    "Server ICE candidate for {}: {}",
                                    peer_id, candidate_init.candidate
                                );
                                // Send ICE candidate notification to client (T015)
                                let notification = json!({
                                    "jsonrpc": "2.0",
                                    "method": "peer.ice_candidate",
                                    "params": {
                                        "from": "remotemedia-server",
                                        "to": peer_id,
                                        "candidate": candidate_init.candidate,
                                        "sdp_mid": candidate_init.sdp_mid,
                                        "sdp_m_line_index": candidate_init.sdp_mline_index
                                    }
                                });
                                if let Err(e) = tx
                                    .send(serde_json::to_string(&notification).unwrap_or_default())
                                    .await
                                {
                                    warn!("Failed to send ICE candidate to client: {}", e);
                                }
                            }
                            Err(e) => {
                                warn!("Failed to convert ICE candidate to JSON: {}", e);
                            }
                        }
                    } else {
                        // ICE gathering complete (T016: empty candidate signals end)
                        debug!("ICE gathering complete for peer {}", peer_id);
                        let notification = json!({
                            "jsonrpc": "2.0",
                            "method": "peer.ice_candidate",
                            "params": {
                                "from": "remotemedia-server",
                                "to": peer_id,
                                "candidate": "",
                                "sdp_mid": null,
                                "sdp_m_line_index": null
                            }
                        });
                        if let Err(e) = tx
                            .send(serde_json::to_string(&notification).unwrap_or_default())
                            .await
                        {
                            warn!("Failed to send ICE gathering complete to client: {}", e);
                        }
                    }
                })
            }));

        // Set up connection state change callback (T017-T018)
        let tx_clone = tx.clone();
        let from_peer_id_clone = from_peer_id.clone();
        server_peer.peer_connection().peer_connection().on_peer_connection_state_change(Box::new(
            move |conn_state| {
                let tx = tx_clone.clone();
                let peer_id = from_peer_id_clone.clone();
                Box::pin(async move {
                    let state_str = match conn_state {
                        webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState::New => "new",
                        webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState::Connecting => "connecting",
                        webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState::Connected => "connected",
                        webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState::Disconnected => "disconnected",
                        webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState::Failed => "failed",
                        webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState::Closed => "closed",
                        _ => "unknown",
                    };
                    info!("Connection state change for peer {}: {}", peer_id, state_str);

                    // Send state change notification using typed params
                    let params = PeerStateChangeParams {
                        peer_id: peer_id.clone(),
                        connection_state: state_str.to_string(),
                        ice_connection_state: None,
                        ice_gathering_state: None,
                        previous_state: None,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::SystemTime::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64,
                    };
                    let notification = json!({
                        "jsonrpc": "2.0",
                        "method": "peer.state_change",
                        "params": params
                    });
                    if let Err(e) = tx.send(serde_json::to_string(&notification).unwrap_or_default()).await {
                        warn!("Failed to send state change to client: {}", e);
                    }
                })
            },
        ));

        // Send JSON-RPC response with answer (T009-T010: per contract, answer in result)
        let response = JsonRpcResponse::new(
            json!({
                "type": "answer",
                "sdp": answer_sdp,
                "from": "remotemedia-server",
                "to": from_peer_id
            }),
            request_id,
        );
        tx.send(response.to_json()?).await?;
    } else {
        // P2P forwarding mode
        debug!("Forwarding offer: {} -> {}", from_peer_id, params.to);

        let peers = state.peers.read().await;
        if let Some(target_peer) = peers.get(&params.to) {
            // Forward offer
            let notification = json!({
                "jsonrpc": "2.0",
                "method": "peer.offer",
                "params": {
                    "from": from_peer_id,
                    "to": params.to,
                    "sdp": params.sdp
                }
            });
            let _ = target_peer
                .tx
                .send(serde_json::to_string(&notification)?)
                .await;

            let response = JsonRpcResponse::new(
                json!({
                    "success": true,
                    "to_peer_id": params.to
                }),
                request_id,
            );
            tx.send(response.to_json()?).await?;
        } else {
            let error = JsonRpcError::new(
                error_codes::PEER_NOT_FOUND,
                format!("Peer not found: {}", params.to),
                request_id,
            );
            tx.send(error.to_json()?).await?;
        }
    }

    Ok(())
}

/// Handle peer.answer
async fn handle_answer(
    request: JsonRpcRequest,
    state: &Arc<SharedState>,
    peer_id_ref: &Arc<RwLock<Option<String>>>,
    tx: &mpsc::Sender<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let request_id = request.id.clone().unwrap_or(json!(null));

    let from_peer_id = peer_id_ref.read().await.clone();
    let from_peer_id = match from_peer_id {
        Some(id) => id,
        None => {
            let error = JsonRpcError::new(
                error_codes::INVALID_REQUEST,
                "Not announced: call peer.announce first".to_string(),
                request_id,
            );
            tx.send(error.to_json()?).await?;
            return Ok(());
        }
    };

    let params: PeerAnswerParams = serde_json::from_value(request.params.clone())?;

    // Forward answer to target peer
    let peers = state.peers.read().await;
    if let Some(target_peer) = peers.get(&params.to) {
        let notification = json!({
            "jsonrpc": "2.0",
            "method": "peer.answer",
            "params": {
                "from": from_peer_id,
                "to": params.to,
                "sdp": params.sdp
            }
        });
        let _ = target_peer
            .tx
            .send(serde_json::to_string(&notification)?)
            .await;

        let response = JsonRpcResponse::new(
            json!({
                "success": true,
                "to_peer_id": params.to
            }),
            request_id,
        );
        tx.send(response.to_json()?).await?;
    } else {
        let error = JsonRpcError::new(
            error_codes::PEER_NOT_FOUND,
            format!("Peer not found: {}", params.to),
            request_id,
        );
        tx.send(error.to_json()?).await?;
    }

    Ok(())
}

/// Handle peer.ice_candidate
async fn handle_ice_candidate(
    request: JsonRpcRequest,
    state: &Arc<SharedState>,
    peer_id_ref: &Arc<RwLock<Option<String>>>,
    tx: &mpsc::Sender<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let request_id = request.id.clone().unwrap_or(json!(null));

    let from_peer_id = peer_id_ref.read().await.clone();
    let from_peer_id = match from_peer_id {
        Some(id) => id,
        None => {
            let error = JsonRpcError::new(
                error_codes::INVALID_REQUEST,
                "Not announced: call peer.announce first".to_string(),
                request_id,
            );
            tx.send(error.to_json()?).await?;
            return Ok(());
        }
    };

    let params: IceCandidateParams = serde_json::from_value(request.params.clone())?;

    // Check if target is remotemedia-server
    if params.to == "remotemedia-server" {
        debug!("ICE candidate from {} for ServerPeer", from_peer_id);

        let server_peers = state.server_peers.read().await;
        if let Some(server_peer) = server_peers.get(&from_peer_id) {
            let server_peer: Arc<ServerPeer> = Arc::clone(server_peer);
            drop(server_peers);

            match server_peer
                .handle_ice_candidate(
                    params.candidate.clone(),
                    params.sdp_mid.clone(),
                    params.sdp_m_line_index,
                )
                .await
            {
                Ok(_) => {
                    info!("ICE candidate added to ServerPeer for {}", from_peer_id);
                }
                Err(e) => {
                    error!("Failed to add ICE candidate to ServerPeer: {}", e);
                    let error = JsonRpcError::new(
                        error_codes::ICE_CANDIDATE_INVALID,
                        format!("Failed to add ICE candidate: {}", e),
                        request_id,
                    );
                    tx.send(error.to_json()?).await?;
                    return Ok(());
                }
            }

            let response = JsonRpcResponse::new(
                json!({
                    "success": true,
                    "to_peer_id": params.to
                }),
                request_id,
            );
            tx.send(response.to_json()?).await?;
        } else {
            // ServerPeer not ready - silently acknowledge
            warn!(
                "ICE candidate from {} arrived before ServerPeer was created",
                from_peer_id
            );

            let response = JsonRpcResponse::new(
                json!({
                    "success": true,
                    "to_peer_id": params.to,
                    "buffered": false
                }),
                request_id,
            );
            tx.send(response.to_json()?).await?;
        }
    } else {
        // P2P forwarding mode
        debug!(
            "Forwarding ICE candidate: {} -> {}",
            from_peer_id, params.to
        );

        let peers = state.peers.read().await;
        if let Some(target_peer) = peers.get(&params.to) {
            let notification = json!({
                "jsonrpc": "2.0",
                "method": "peer.ice_candidate",
                "params": {
                    "from": from_peer_id,
                    "to": params.to,
                    "candidate": params.candidate,
                    "sdp_mid": params.sdp_mid,
                    "sdp_m_line_index": params.sdp_m_line_index
                }
            });
            let _ = target_peer
                .tx
                .send(serde_json::to_string(&notification)?)
                .await;

            let response = JsonRpcResponse::new(
                json!({
                    "success": true,
                    "to_peer_id": params.to
                }),
                request_id,
            );
            tx.send(response.to_json()?).await?;
        } else {
            let error = JsonRpcError::new(
                error_codes::PEER_NOT_FOUND,
                format!("Peer not found: {}", params.to),
                request_id,
            );
            tx.send(error.to_json()?).await?;
        }
    }

    Ok(())
}

/// Handle peer.disconnect
async fn handle_disconnect(
    request: JsonRpcRequest,
    state: &Arc<SharedState>,
    tx: &mpsc::Sender<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let request_id = request.id.clone().unwrap_or(json!(null));

    let params: PeerDisconnectParams = serde_json::from_value(request.params.clone())?;

    let peers = state.peers.read().await;
    if let Some(target_peer) = peers.get(&params.peer_id) {
        let notification = json!({
            "jsonrpc": "2.0",
            "method": "peer.disconnected",
            "params": {
                "peer_id": params.peer_id,
                "reason": params.reason
            }
        });
        let _ = target_peer
            .tx
            .send(serde_json::to_string(&notification)?)
            .await;
    }

    let response = JsonRpcResponse::new(
        json!({
            "success": true,
            "peer_id": params.peer_id
        }),
        request_id,
    );
    tx.send(response.to_json()?).await?;

    Ok(())
}

/// Handle peer.list / list_peers
async fn handle_list_peers(
    request: JsonRpcRequest,
    state: &Arc<SharedState>,
    tx: &mpsc::Sender<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let request_id = request.id.clone().unwrap_or(json!(null));

    let peers = state.peers.read().await;

    let peer_list: Vec<Value> = peers
        .values()
        .map(|peer| {
            json!({
                "peer_id": peer.peer_id,
                "capabilities": peer.capabilities,
                "metadata": peer.metadata,
                "connected_at": peer.connected_at
            })
        })
        .collect();

    let response = JsonRpcResponse::new(
        json!({
            "peers": peer_list,
            "count": peer_list.len()
        }),
        request_id,
    );
    tx.send(response.to_json()?).await?;

    Ok(())
}

/// Handle server.info — return server network interfaces for client-side IP discovery
async fn handle_server_info(
    request: JsonRpcRequest,
    tx: &mpsc::Sender<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let request_id = request.id.clone().unwrap_or(json!(null));

    let mut addresses: Vec<Value> = Vec::new();

    // Gather all non-loopback IPv4 addresses from network interfaces
    if let Ok(interfaces) = std::net::UdpSocket::bind("0.0.0.0:0") {
        // Connect to a public address to determine the default route interface
        if interfaces.connect("8.8.8.8:80").is_ok() {
            if let Ok(local_addr) = interfaces.local_addr() {
                addresses.push(json!({
                    "ip": local_addr.ip().to_string(),
                    "is_default": true,
                }));
            }
        }
    }

    // Also enumerate all interfaces via getifaddrs equivalent
    // Use a simpler approach: read from /proc/net on Linux, or just use the UDP trick above
    // The UDP socket approach gives us the default route IP which is what the client needs

    let response = JsonRpcResponse::new(
        json!({
            "addresses": addresses,
            "hostname": std::env::var("HOSTNAME").unwrap_or_default(),
        }),
        request_id,
    );
    tx.send(response.to_json()?).await?;

    Ok(())
}

/// Broadcast peer joined notification
async fn broadcast_peer_joined(
    state: &Arc<SharedState>,
    peer_id: &str,
    capabilities: &[String],
    metadata: &Option<HashMap<String, Value>>,
) {
    let notification = json!({
        "jsonrpc": "2.0",
        "method": "peer.joined",
        "params": {
            "peer_id": peer_id,
            "capabilities": capabilities,
            "metadata": metadata
        }
    });

    let msg = match serde_json::to_string(&notification) {
        Ok(m) => m,
        Err(_) => return,
    };

    let peers = state.peers.read().await;
    for (id, peer) in peers.iter() {
        if id != peer_id {
            let _ = peer.tx.send(msg.clone()).await;
        }
    }
}

/// Broadcast peer left notification
async fn broadcast_peer_left(state: &Arc<SharedState>, peer_id: &str) {
    let notification = json!({
        "jsonrpc": "2.0",
        "method": "peer.left",
        "params": {
            "peer_id": peer_id
        }
    });

    let msg = match serde_json::to_string(&notification) {
        Ok(m) => m,
        Err(_) => return,
    };

    let peers = state.peers.read().await;
    for peer in peers.values() {
        let _ = peer.tx.send(msg.clone()).await;
    }
}

/// Get current Unix timestamp
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
