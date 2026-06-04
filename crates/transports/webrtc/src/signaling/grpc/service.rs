// WebRTC signaling service implementation
//
// Provides gRPC bidirectional streaming for WebRTC peer signaling

// Phase 4 (US2) signaling infrastructure
#![allow(dead_code)]

use crate::config::WebRtcTransportConfig;
use crate::generated::webrtc::{
    web_rtc_signaling_server::{WebRtcSignaling, WebRtcSignalingServer},
    *,
};
use crate::peer::{ServerPeer, SharedPipelineSession};
use remotemedia_core::{manifest::Manifest, transport::PipelineSessionHost};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

/// Peer information stored in registry
struct PeerConnection {
    peer_id: String,
    capabilities: Option<PeerCapabilities>,
    metadata: HashMap<String, String>,
    state: i32, // PeerState enum value
    connected_at: u64,
    tx: mpsc::Sender<Result<SignalingResponse, Status>>,
}

/// WebRTC signaling service
///
/// Manages peer connections and routes signaling messages between peers.
pub struct WebRtcSignalingService {
    /// Connected peers (peer_id -> PeerConnection)
    peers: Arc<RwLock<HashMap<String, PeerConnection>>>,

    /// Pending offers (offer_id -> OfferInfo)
    pending_offers: Arc<RwLock<HashMap<String, PendingOffer>>>,

    /// Server-side peers (peer_id -> ServerPeer)
    server_peers: Arc<RwLock<HashMap<String, Arc<ServerPeer>>>>,

    /// Shared pipeline session used by all server-side WebRTC peers.
    shared_pipeline_session: Arc<RwLock<Option<Arc<SharedPipelineSession>>>>,

    /// Observable shared pipeline session ID for diagnostics and tests.
    shared_pipeline_session_id: Arc<RwLock<Option<String>>>,

    /// WebRTC transport configuration
    config: Arc<WebRtcTransportConfig>,

    /// Pipeline runner
    executor: Arc<dyn PipelineSessionHost>,

    /// Pipeline manifest
    manifest: Arc<Manifest>,

    /// Server start time for uptime tracking
    start_time: SystemTime,
}

struct PendingOffer {
    from_peer_id: String,
    to_peer_id: String,
    sdp: String,
    timestamp: SystemTime,
}

impl WebRtcSignalingService {
    /// Create a new signaling service
    pub fn new(
        config: Arc<WebRtcTransportConfig>,
        executor: Arc<dyn PipelineSessionHost>,
        manifest: Arc<Manifest>,
    ) -> Self {
        info!("Creating WebRTC signaling service");

        let peers = Arc::new(RwLock::new(HashMap::new()));

        Self {
            peers,
            pending_offers: Arc::new(RwLock::new(HashMap::new())),
            server_peers: Arc::new(RwLock::new(HashMap::new())),
            shared_pipeline_session: Arc::new(RwLock::new(None)),
            shared_pipeline_session_id: Arc::new(RwLock::new(None)),
            config,
            executor,
            manifest,
            start_time: SystemTime::now(),
        }
    }

    /// Register a server-side peer that will handle all client offers
    ///
    /// This creates a virtual "remotemedia-server" peer that clients can send offers to.
    /// The actual ServerPeer will be created when an offer is received.
    pub async fn register_server_peer(&self, tx: mpsc::Sender<Result<SignalingResponse, Status>>) {
        let mut peers_write = self.peers.write().await;

        let server_peer = PeerConnection {
            peer_id: "remotemedia-server".to_string(),
            capabilities: Some(PeerCapabilities {
                audio: true,
                video: true,
                data: true,
                extensions: "{}".to_string(),
            }),
            metadata: HashMap::from([
                ("type".to_string(), "server".to_string()),
                ("version".to_string(), env!("CARGO_PKG_VERSION").to_string()),
            ]),
            state: PeerState::Available as i32,
            connected_at: Self::current_timestamp(),
            tx,
        };

        peers_write.insert("remotemedia-server".to_string(), server_peer);
        info!("Registered server peer: remotemedia-server");
    }

    /// Create a gRPC server
    pub fn into_server(self) -> WebRtcSignalingServer<Self> {
        WebRtcSignalingServer::new(self)
    }

    /// Return a cloneable observer for the shared server-side pipeline session ID.
    pub fn shared_pipeline_session_id_handle(&self) -> Arc<RwLock<Option<String>>> {
        Arc::clone(&self.shared_pipeline_session_id)
    }

    fn shared_session_key(config: &WebRtcTransportConfig) -> String {
        config
            .shared_session_key
            .as_deref()
            .filter(|key| !key.is_empty())
            .unwrap_or("default")
            .to_string()
    }

    /// Broadcast peer joined notification
    async fn broadcast_peer_joined(
        &self,
        peer_id: &str,
        capabilities: Option<PeerCapabilities>,
        metadata: HashMap<String, String>,
    ) {
        let notification = SignalingResponse {
            request_id: String::new(),
            response: Some(signaling_response::Response::Notification(
                SignalingNotification {
                    notification: Some(signaling_notification::Notification::PeerJoined(
                        PeerJoinedNotification {
                            peer_id: peer_id.to_string(),
                            capabilities,
                            metadata,
                        },
                    )),
                },
            )),
        };

        let peers = self.peers.read().await;
        for (id, peer) in peers.iter() {
            if id != peer_id {
                let _ = peer.tx.send(Ok(notification.clone())).await;
            }
        }
    }

    /// Broadcast peer left notification
    async fn broadcast_peer_left(&self, peer_id: &str) {
        let notification = SignalingResponse {
            request_id: String::new(),
            response: Some(signaling_response::Response::Notification(
                SignalingNotification {
                    notification: Some(signaling_notification::Notification::PeerLeft(
                        PeerLeftNotification {
                            peer_id: peer_id.to_string(),
                        },
                    )),
                },
            )),
        };

        let peers = self.peers.read().await;
        for peer in peers.values() {
            let _ = peer.tx.send(Ok(notification.clone())).await;
        }
    }

    /// Get current Unix timestamp
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
}

#[tonic::async_trait]
impl WebRtcSignaling for WebRtcSignalingService {
    type SignalStream = Pin<Box<dyn Stream<Item = Result<SignalingResponse, Status>> + Send>>;

    /// Bidirectional signaling stream
    async fn signal(
        &self,
        request: Request<tonic::Streaming<SignalingRequest>>,
    ) -> Result<Response<Self::SignalStream>, Status> {
        let mut in_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(128);

        let peers = Arc::clone(&self.peers);
        let pending_offers = Arc::clone(&self.pending_offers);
        let service_peers = Arc::clone(&self.peers);
        let server_peers = Arc::clone(&self.server_peers);
        let shared_pipeline_session = Arc::clone(&self.shared_pipeline_session);
        let shared_pipeline_session_id = Arc::clone(&self.shared_pipeline_session_id);
        let config = Arc::clone(&self.config);
        let executor = Arc::clone(&self.executor);
        let manifest = Arc::clone(&self.manifest);

        // Spawn task to handle incoming messages
        tokio::spawn(async move {
            let mut peer_id: Option<String> = None;

            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(req) => {
                        if let Err(e) = Self::handle_request(
                            req,
                            &peers,
                            &pending_offers,
                            &server_peers,
                            &shared_pipeline_session,
                            &shared_pipeline_session_id,
                            &config,
                            &executor,
                            &manifest,
                            &tx,
                            &mut peer_id,
                        )
                        .await
                        {
                            warn!("Error handling request: {}", e);
                            let _ = tx.send(Err(e)).await;
                        }
                    }
                    Err(status) => {
                        warn!("Stream error: {}", status);
                        break;
                    }
                }
            }

            // Clean up when stream ends
            if let Some(peer_id) = peer_id {
                info!("Peer disconnected: {}", peer_id);

                // CRITICAL: Clean up ServerPeer and its pipeline session FIRST
                // This must happen before removing from peers to ensure proper shutdown
                if let Some(server_peer) = server_peers.write().await.remove(&peer_id) {
                    info!(
                        "Shutting down ServerPeer for disconnected peer: {}",
                        peer_id
                    );
                    if let Err(e) = server_peer.shutdown().await {
                        error!("Failed to shutdown ServerPeer for {}: {}", peer_id, e);
                    } else {
                        info!("ServerPeer shutdown complete for: {}", peer_id);
                    }
                }

                // Remove from signaling peers
                service_peers.write().await.remove(&peer_id);

                // Broadcast peer left (using service_peers reference)
                let notification = SignalingResponse {
                    request_id: String::new(),
                    response: Some(signaling_response::Response::Notification(
                        SignalingNotification {
                            notification: Some(signaling_notification::Notification::PeerLeft(
                                PeerLeftNotification {
                                    peer_id: peer_id.clone(),
                                },
                            )),
                        },
                    )),
                };

                let peers_read = service_peers.read().await;
                for peer in peers_read.values() {
                    let _ = peer.tx.send(Ok(notification.clone())).await;
                }
            }
        });

        let out_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(out_stream)))
    }

    /// Get list of connected peers (unary RPC)
    async fn get_peers(
        &self,
        _request: Request<GetPeersRequest>,
    ) -> Result<Response<GetPeersResponse>, Status> {
        let peers = self.peers.read().await;

        let peer_list: Vec<PeerInfo> = peers
            .values()
            .map(|peer| PeerInfo {
                peer_id: peer.peer_id.clone(),
                capabilities: peer.capabilities.clone(),
                state: peer.state,
                metadata: peer.metadata.clone(),
                connected_at: peer.connected_at,
            })
            .collect();

        Ok(Response::new(GetPeersResponse {
            peers: peer_list,
            count: peers.len() as u32,
        }))
    }

    /// Health check
    async fn health_check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let uptime = self.start_time.elapsed().unwrap_or_default().as_secs();

        let peers_count = self.peers.read().await.len();

        Ok(Response::new(HealthCheckResponse {
            status: "healthy".to_string(),
            uptime_seconds: uptime,
            connected_peers: peers_count as u32,
            metrics: serde_json::json!({
                "pending_offers": self.pending_offers.read().await.len()
            })
            .to_string(),
        }))
    }
}

impl WebRtcSignalingService {
    /// Handle a single signaling request
    async fn handle_request(
        req: SignalingRequest,
        peers: &Arc<RwLock<HashMap<String, PeerConnection>>>,
        pending_offers: &Arc<RwLock<HashMap<String, PendingOffer>>>,
        server_peers: &Arc<RwLock<HashMap<String, Arc<ServerPeer>>>>,
        shared_pipeline_session: &Arc<RwLock<Option<Arc<SharedPipelineSession>>>>,
        shared_pipeline_session_id: &Arc<RwLock<Option<String>>>,
        config: &Arc<WebRtcTransportConfig>,
        executor: &Arc<dyn PipelineSessionHost>,
        manifest: &Arc<Manifest>,
        tx: &mpsc::Sender<Result<SignalingResponse, Status>>,
        peer_id: &mut Option<String>,
    ) -> Result<(), Status> {
        let request_id = req.request_id.clone();

        match req.request {
            Some(signaling_request::Request::Announce(announce)) => {
                Self::handle_announce(announce, peers, tx, peer_id, &request_id).await
            }
            Some(signaling_request::Request::Offer(offer)) => {
                Self::handle_offer(
                    offer,
                    peers,
                    pending_offers,
                    server_peers,
                    shared_pipeline_session,
                    shared_pipeline_session_id,
                    config,
                    executor,
                    manifest,
                    tx,
                    peer_id,
                    &request_id,
                )
                .await
            }
            Some(signaling_request::Request::Answer(answer)) => {
                Self::handle_answer(answer, peers, tx, peer_id, &request_id).await
            }
            Some(signaling_request::Request::IceCandidate(ice)) => {
                Self::handle_ice_candidate(ice, peers, server_peers, tx, peer_id, &request_id).await
            }
            Some(signaling_request::Request::Disconnect(disconnect)) => {
                Self::handle_disconnect(disconnect, peers, tx, &request_id).await
            }
            Some(signaling_request::Request::ListPeers(_)) => {
                Self::handle_list_peers(peers, tx, &request_id).await
            }
            None => Err(Status::invalid_argument("No request provided")),
        }
    }

    /// Handle announce request
    async fn handle_announce(
        announce: AnnounceRequest,
        peers: &Arc<RwLock<HashMap<String, PeerConnection>>>,
        tx: &mpsc::Sender<Result<SignalingResponse, Status>>,
        peer_id: &mut Option<String>,
        request_id: &str,
    ) -> Result<(), Status> {
        let mut peers_write = peers.write().await;

        // Check if peer already exists
        if peers_write.contains_key(&announce.peer_id) {
            return Err(Status::already_exists(format!(
                "Peer {} already announced",
                announce.peer_id
            )));
        }

        // Get list of other peers before adding this one
        let other_peers: Vec<String> = peers_write.keys().cloned().collect();

        // Register peer
        let peer_conn = PeerConnection {
            peer_id: announce.peer_id.clone(),
            capabilities: announce.capabilities.clone(),
            metadata: announce.metadata.clone(),
            state: PeerState::Available as i32,
            connected_at: Self::current_timestamp(),
            tx: tx.clone(),
        };

        peers_write.insert(announce.peer_id.clone(), peer_conn);
        *peer_id = Some(announce.peer_id.clone());

        debug!(
            "Peer announced: {} (total: {})",
            announce.peer_id,
            peers_write.len()
        );

        // Drop write lock before sending response
        drop(peers_write);

        // Send acknowledgment
        let ack_data = serde_json::json!({
            "peer_id": announce.peer_id,
            "peers_count": other_peers.len() + 1,
            "other_peers": other_peers
        });

        let response = SignalingResponse {
            request_id: request_id.to_string(),
            response: Some(signaling_response::Response::Ack(AckResponse {
                message: format!("Peer {} announced successfully", announce.peer_id),
                data: ack_data.to_string(),
            })),
        };

        tx.send(Ok(response))
            .await
            .map_err(|_| Status::internal("Failed to send response"))?;

        // Broadcast peer joined to others
        let notification = SignalingResponse {
            request_id: String::new(),
            response: Some(signaling_response::Response::Notification(
                SignalingNotification {
                    notification: Some(signaling_notification::Notification::PeerJoined(
                        PeerJoinedNotification {
                            peer_id: announce.peer_id.clone(),
                            capabilities: announce.capabilities,
                            metadata: announce.metadata,
                        },
                    )),
                },
            )),
        };

        let peers_read = peers.read().await;
        for (id, peer) in peers_read.iter() {
            if id != &announce.peer_id {
                let _ = peer.tx.send(Ok(notification.clone())).await;
            }
        }

        Ok(())
    }

    /// Handle offer request
    async fn handle_offer(
        offer: OfferRequest,
        peers: &Arc<RwLock<HashMap<String, PeerConnection>>>,
        pending_offers: &Arc<RwLock<HashMap<String, PendingOffer>>>,
        server_peers: &Arc<RwLock<HashMap<String, Arc<ServerPeer>>>>,
        shared_pipeline_session: &Arc<RwLock<Option<Arc<SharedPipelineSession>>>>,
        shared_pipeline_session_id: &Arc<RwLock<Option<String>>>,
        config: &Arc<WebRtcTransportConfig>,
        executor: &Arc<dyn PipelineSessionHost>,
        manifest: &Arc<Manifest>,
        tx: &mpsc::Sender<Result<SignalingResponse, Status>>,
        from_peer_id: &Option<String>,
        request_id: &str,
    ) -> Result<(), Status> {
        let from_peer_id = from_peer_id
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Not announced: call announce first"))?;

        // Create offer ID
        let offer_id = format!(
            "{}_{}_{}",
            from_peer_id,
            offer.to_peer_id,
            Self::current_timestamp()
        );

        // Store pending offer
        let mut pending = pending_offers.write().await;
        pending.insert(
            offer_id.clone(),
            PendingOffer {
                from_peer_id: from_peer_id.clone(),
                to_peer_id: offer.to_peer_id.clone(),
                sdp: offer.sdp.clone(),
                timestamp: SystemTime::now(),
            },
        );
        drop(pending);

        // Check if target is remotemedia-server
        if offer.to_peer_id == "remotemedia-server" {
            info!("Creating ServerPeer for offer from {}", from_peer_id);

            // CRITICAL: Clean up any existing ServerPeer for this peer_id first
            // This prevents stale sessions from processing old data
            if let Some(old_server_peer) = server_peers.write().await.remove(from_peer_id) {
                info!(
                    "Shutting down existing ServerPeer for peer {} before creating new one",
                    from_peer_id
                );
                if let Err(e) = old_server_peer.shutdown().await {
                    warn!(
                        "Failed to shutdown old ServerPeer for {}: {} (continuing with new peer)",
                        from_peer_id, e
                    );
                } else {
                    info!("Old ServerPeer shutdown complete for: {}", from_peer_id);
                }
            }

            // Create ServerPeer
            let server_peer = match ServerPeer::new(
                from_peer_id.clone(),
                &**config,
                Arc::clone(executor),
                Arc::clone(manifest),
            )
            .await
            {
                Ok(peer) => Arc::new(peer),
                Err(e) => {
                    error!("Failed to create ServerPeer: {}", e);
                    return Err(Status::internal(format!(
                        "Failed to create ServerPeer: {}",
                        e
                    )));
                }
            };

            let shared_session = {
                let mut guard = shared_pipeline_session.write().await;
                let should_create = match guard.as_ref() {
                    Some(shared) => !shared.is_active().await,
                    None => true,
                };
                if !should_create {
                    Arc::clone(guard.as_ref().expect("shared session checked above"))
                } else {
                    let key = Self::shared_session_key(config);
                    let core_shared = executor
                        .get_or_create_shared_session(key.clone(), Arc::clone(manifest))
                        .await
                        .map_err(|e| {
                            error!("Failed to create shared pipeline session: {}", e);
                            Status::internal(format!(
                                "Failed to create shared pipeline session: {}",
                                e
                            ))
                        })?;
                    let control = executor.control_bus().get(core_shared.session_id());
                    let shared = SharedPipelineSession::new(core_shared, control, false)
                        .await
                        .map_err(|e| {
                            error!("Failed to initialize shared pipeline session: {}", e);
                            Status::internal(format!(
                                "Failed to initialize shared pipeline session: {}",
                                e
                            ))
                        })?;
                    info!(
                        "Created shared pipeline session {} for WebRTC peers with key {}",
                        shared.session_id(),
                        key
                    );
                    *shared_pipeline_session_id.write().await =
                        Some(shared.session_id().to_string());
                    *guard = Some(Arc::clone(&shared));
                    shared
                }
            };

            // Handle offer and get SDP answer
            let answer_sdp = match server_peer
                .handle_offer_with_shared_session(offer.sdp, shared_session)
                .await
            {
                Ok(sdp) => sdp,
                Err(e) => {
                    error!("ServerPeer failed to handle offer: {}", e);
                    return Err(Status::internal(format!("Failed to handle offer: {}", e)));
                }
            };

            info!("ServerPeer generated answer, sending to {}", from_peer_id);

            // Store server peer
            server_peers
                .write()
                .await
                .insert(from_peer_id.clone(), Arc::clone(&server_peer));

            // Send answer notification back to client
            let answer_notification = SignalingResponse {
                request_id: String::new(),
                response: Some(signaling_response::Response::Notification(
                    SignalingNotification {
                        notification: Some(signaling_notification::Notification::Answer(
                            AnswerNotification {
                                from_peer_id: "remotemedia-server".to_string(),
                                sdp: answer_sdp,
                                r#type: "answer".to_string(),
                            },
                        )),
                    },
                )),
            };

            tx.send(Ok(answer_notification))
                .await
                .map_err(|_| Status::internal("Failed to send answer"))?;

            // Send acknowledgment
            let ack_data = serde_json::json!({
                "offer_id": offer_id,
                "to_peer_id": offer.to_peer_id,
                "answer_sent": true
            });

            let response = SignalingResponse {
                request_id: request_id.to_string(),
                response: Some(signaling_response::Response::Ack(AckResponse {
                    message: format!("Offer processed by remotemedia-server, answer sent"),
                    data: ack_data.to_string(),
                })),
            };

            tx.send(Ok(response))
                .await
                .map_err(|_| Status::internal("Failed to send response"))?;
        } else {
            // P2P forwarding mode (original logic)
            debug!("Forwarding offer: {} -> {}", from_peer_id, offer.to_peer_id);

            // Get target peer
            let peers_read = peers.read().await;
            let target_peer = peers_read.get(&offer.to_peer_id).ok_or_else(|| {
                Status::not_found(format!("Peer not found: {}", offer.to_peer_id))
            })?;

            // Forward offer to target peer
            let notification = SignalingResponse {
                request_id: String::new(),
                response: Some(signaling_response::Response::Notification(
                    SignalingNotification {
                        notification: Some(signaling_notification::Notification::Offer(
                            OfferNotification {
                                from_peer_id: from_peer_id.clone(),
                                offer_id: offer_id.clone(),
                                sdp: offer.sdp,
                                r#type: offer.r#type,
                            },
                        )),
                    },
                )),
            };

            target_peer
                .tx
                .send(Ok(notification))
                .await
                .map_err(|_| Status::internal("Failed to forward offer"))?;

            // Send acknowledgment
            let ack_data = serde_json::json!({
                "offer_id": offer_id,
                "to_peer_id": offer.to_peer_id
            });

            let response = SignalingResponse {
                request_id: request_id.to_string(),
                response: Some(signaling_response::Response::Ack(AckResponse {
                    message: format!("Offer sent to {}", offer.to_peer_id),
                    data: ack_data.to_string(),
                })),
            };

            tx.send(Ok(response))
                .await
                .map_err(|_| Status::internal("Failed to send response"))?;
        }

        Ok(())
    }

    /// Handle answer request
    async fn handle_answer(
        answer: AnswerRequest,
        peers: &Arc<RwLock<HashMap<String, PeerConnection>>>,
        tx: &mpsc::Sender<Result<SignalingResponse, Status>>,
        from_peer_id: &Option<String>,
        request_id: &str,
    ) -> Result<(), Status> {
        let from_peer_id = from_peer_id
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Not announced: call announce first"))?;

        // Get target peer
        let peers_read = peers.read().await;
        let target_peer = peers_read
            .get(&answer.to_peer_id)
            .ok_or_else(|| Status::not_found(format!("Peer not found: {}", answer.to_peer_id)))?;

        debug!(
            "Forwarding answer: {} -> {}",
            from_peer_id, answer.to_peer_id
        );

        // Forward answer to target peer
        let notification = SignalingResponse {
            request_id: String::new(),
            response: Some(signaling_response::Response::Notification(
                SignalingNotification {
                    notification: Some(signaling_notification::Notification::Answer(
                        AnswerNotification {
                            from_peer_id: from_peer_id.clone(),
                            sdp: answer.sdp,
                            r#type: answer.r#type,
                        },
                    )),
                },
            )),
        };

        target_peer
            .tx
            .send(Ok(notification))
            .await
            .map_err(|_| Status::internal("Failed to forward answer"))?;

        // Send acknowledgment
        let ack_data = serde_json::json!({"to_peer_id": answer.to_peer_id});

        let response = SignalingResponse {
            request_id: request_id.to_string(),
            response: Some(signaling_response::Response::Ack(AckResponse {
                message: format!("Answer sent to {}", answer.to_peer_id),
                data: ack_data.to_string(),
            })),
        };

        tx.send(Ok(response))
            .await
            .map_err(|_| Status::internal("Failed to send response"))?;

        Ok(())
    }

    /// Handle ICE candidate request
    async fn handle_ice_candidate(
        ice: IceCandidateRequest,
        peers: &Arc<RwLock<HashMap<String, PeerConnection>>>,
        server_peers: &Arc<RwLock<HashMap<String, Arc<ServerPeer>>>>,
        tx: &mpsc::Sender<Result<SignalingResponse, Status>>,
        from_peer_id: &Option<String>,
        request_id: &str,
    ) -> Result<(), Status> {
        let from_peer_id = from_peer_id
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("Not announced: call announce first"))?;

        // Check if target is remotemedia-server (ServerPeer)
        if ice.to_peer_id == "remotemedia-server" {
            debug!("ICE candidate from {} for ServerPeer", from_peer_id);

            // Get the ServerPeer for this client
            let server_peers_read = server_peers.read().await;

            if let Some(server_peer) = server_peers_read.get(from_peer_id) {
                // ServerPeer exists, add the ICE candidate
                let server_peer = Arc::clone(server_peer);
                drop(server_peers_read); // Release lock before async operation

                match server_peer
                    .handle_ice_candidate(
                        ice.candidate.clone(),
                        if ice.sdp_mid.is_empty() {
                            None
                        } else {
                            Some(ice.sdp_mid)
                        },
                        Some(ice.sdp_mline_index as u16),
                    )
                    .await
                {
                    Ok(_) => {
                        info!("ICE candidate added to ServerPeer for {}", from_peer_id);
                    }
                    Err(e) => {
                        error!("Failed to add ICE candidate to ServerPeer: {}", e);
                        return Err(Status::internal(format!(
                            "Failed to add ICE candidate: {}",
                            e
                        )));
                    }
                }

                // Send acknowledgment
                let ack_data = serde_json::json!({"to_peer_id": ice.to_peer_id});

                let response = SignalingResponse {
                    request_id: request_id.to_string(),
                    response: Some(signaling_response::Response::Ack(AckResponse {
                        message: format!("ICE candidate added to remotemedia-server"),
                        data: ack_data.to_string(),
                    })),
                };

                tx.send(Ok(response))
                    .await
                    .map_err(|_| Status::internal("Failed to send response"))?;
            } else {
                // ServerPeer doesn't exist yet - silently ignore
                // This can happen when ICE candidates arrive before the offer is processed
                warn!(
                    "ICE candidate from {} arrived before ServerPeer was created - ignoring",
                    from_peer_id
                );

                // Send success acknowledgment anyway to avoid crashing the stream
                let ack_data = serde_json::json!({"to_peer_id": ice.to_peer_id, "buffered": false});

                let response = SignalingResponse {
                    request_id: request_id.to_string(),
                    response: Some(signaling_response::Response::Ack(AckResponse {
                        message: format!("ICE candidate ignored (ServerPeer not ready)"),
                        data: ack_data.to_string(),
                    })),
                };

                tx.send(Ok(response))
                    .await
                    .map_err(|_| Status::internal("Failed to send response"))?;
            }
        } else {
            // P2P forwarding mode
            debug!(
                "Forwarding ICE candidate: {} -> {}",
                from_peer_id, ice.to_peer_id
            );

            // Get target peer
            let peers_read = peers.read().await;
            let target_peer = peers_read
                .get(&ice.to_peer_id)
                .ok_or_else(|| Status::not_found(format!("Peer not found: {}", ice.to_peer_id)))?;

            // Forward ICE candidate to target peer
            let notification = SignalingResponse {
                request_id: String::new(),
                response: Some(signaling_response::Response::Notification(
                    SignalingNotification {
                        notification: Some(signaling_notification::Notification::IceCandidate(
                            IceCandidateNotification {
                                from_peer_id: from_peer_id.clone(),
                                candidate: ice.candidate,
                                sdp_mid: ice.sdp_mid,
                                sdp_mline_index: ice.sdp_mline_index,
                            },
                        )),
                    },
                )),
            };

            target_peer
                .tx
                .send(Ok(notification))
                .await
                .map_err(|_| Status::internal("Failed to forward ICE candidate"))?;

            // Send acknowledgment
            let ack_data = serde_json::json!({"to_peer_id": ice.to_peer_id});

            let response = SignalingResponse {
                request_id: request_id.to_string(),
                response: Some(signaling_response::Response::Ack(AckResponse {
                    message: format!("ICE candidate sent to {}", ice.to_peer_id),
                    data: ack_data.to_string(),
                })),
            };

            tx.send(Ok(response))
                .await
                .map_err(|_| Status::internal("Failed to send response"))?;
        }

        Ok(())
    }

    /// Handle disconnect request
    async fn handle_disconnect(
        disconnect: DisconnectRequest,
        peers: &Arc<RwLock<HashMap<String, PeerConnection>>>,
        tx: &mpsc::Sender<Result<SignalingResponse, Status>>,
        request_id: &str,
    ) -> Result<(), Status> {
        let peers_read = peers.read().await;

        if let Some(target_peer) = peers_read.get(&disconnect.peer_id) {
            let notification = SignalingResponse {
                request_id: String::new(),
                response: Some(signaling_response::Response::Notification(
                    SignalingNotification {
                        notification: Some(signaling_notification::Notification::PeerDisconnected(
                            PeerDisconnectedNotification {
                                peer_id: disconnect.peer_id.clone(),
                            },
                        )),
                    },
                )),
            };

            let _ = target_peer.tx.send(Ok(notification)).await;
        }

        // Send acknowledgment
        let ack_data = serde_json::json!({"peer_id": disconnect.peer_id});

        let response = SignalingResponse {
            request_id: request_id.to_string(),
            response: Some(signaling_response::Response::Ack(AckResponse {
                message: format!("Disconnect notification sent to {}", disconnect.peer_id),
                data: ack_data.to_string(),
            })),
        };

        tx.send(Ok(response))
            .await
            .map_err(|_| Status::internal("Failed to send response"))?;

        Ok(())
    }

    /// Handle list peers request
    async fn handle_list_peers(
        peers: &Arc<RwLock<HashMap<String, PeerConnection>>>,
        tx: &mpsc::Sender<Result<SignalingResponse, Status>>,
        request_id: &str,
    ) -> Result<(), Status> {
        let peers_read = peers.read().await;

        let peer_list: Vec<PeerInfo> = peers_read
            .values()
            .map(|peer| PeerInfo {
                peer_id: peer.peer_id.clone(),
                capabilities: peer.capabilities.clone(),
                state: peer.state,
                metadata: peer.metadata.clone(),
                connected_at: peer.connected_at,
            })
            .collect();

        let response = SignalingResponse {
            request_id: request_id.to_string(),
            response: Some(signaling_response::Response::PeerList(PeerListResponse {
                peers: peer_list,
                count: peers_read.len() as u32,
            })),
        };

        tx.send(Ok(response))
            .await
            .map_err(|_| Status::internal("Failed to send response"))?;

        Ok(())
    }
}
