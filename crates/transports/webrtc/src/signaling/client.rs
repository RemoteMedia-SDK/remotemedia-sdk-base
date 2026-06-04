//! WebSocket signaling client for peer discovery and SDP exchange

use super::protocol::*;
use crate::{Error, Result};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error, info, warn};

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

/// Callback for handling received peer announcements
pub type PeerAnnouncedCallback = Arc<dyn Fn(String, Vec<String>) + Send + Sync>;

/// Callback for handling received offers
pub type OfferReceivedCallback = Arc<dyn Fn(String, String, String) + Send + Sync>;

/// Callback for handling received answers
pub type AnswerReceivedCallback = Arc<dyn Fn(String, String, String) + Send + Sync>;

/// Callback for handling received ICE candidates
pub type IceCandidateReceivedCallback =
    Arc<dyn Fn(String, String, IceCandidateParams) + Send + Sync>;

/// Callback for handling peer disconnections
pub type PeerDisconnectedCallback = Arc<dyn Fn(String) + Send + Sync>;

/// WebSocket signaling client for JSON-RPC 2.0 peer signaling
pub struct SignalingClient {
    /// Signaling server URL
    url: String,

    /// Outgoing message sender
    tx: mpsc::UnboundedSender<Message>,

    /// Local peer ID
    peer_id: Arc<RwLock<Option<String>>>,

    /// Callback handlers
    callbacks: Arc<SignalingCallbacks>,
}

/// Callback handlers for signaling events
#[derive(Default)]
struct SignalingCallbacks {
    on_peer_announced: Mutex<Option<PeerAnnouncedCallback>>,
    on_offer_received: Mutex<Option<OfferReceivedCallback>>,
    on_answer_received: Mutex<Option<AnswerReceivedCallback>>,
    on_ice_candidate_received: Mutex<Option<IceCandidateReceivedCallback>>,
    on_peer_disconnected: Mutex<Option<PeerDisconnectedCallback>>,
}

impl SignalingClient {
    /// Create a new signaling client
    ///
    /// # Arguments
    ///
    /// * `url` - WebSocket signaling server URL (ws:// or wss://)
    pub fn new(url: &str) -> Self {
        let (tx, _rx) = mpsc::unbounded_channel();

        Self {
            url: url.to_string(),
            tx,
            peer_id: Arc::new(RwLock::new(None)),
            callbacks: Arc::new(SignalingCallbacks::default()),
        }
    }

    /// Connect to the signaling server
    ///
    /// Establishes WebSocket connection and starts background tasks for
    /// sending and receiving messages.
    pub async fn connect(&mut self) -> Result<()> {
        info!("Connecting to signaling server: {}", self.url);

        let (ws_stream, _) = connect_async(&self.url)
            .await
            .map_err(|e| Error::WebSocketError(format!("Failed to connect: {}", e)))?;

        info!("Connected to signaling server");

        let (write, read) = ws_stream.split();

        // Create channels for communication
        let (tx, rx) = mpsc::unbounded_channel();
        self.tx = tx.clone();

        // Spawn sender task
        tokio::spawn(Self::sender_task(write, rx));

        // Spawn receiver task
        let peer_id = self.peer_id.clone();
        let callbacks = self.callbacks.clone();
        tokio::spawn(Self::receiver_task(read, peer_id, callbacks));

        Ok(())
    }

    /// Sender task: sends messages from channel to WebSocket
    async fn sender_task(
        mut write: futures::stream::SplitSink<WsStream, Message>,
        mut rx: mpsc::UnboundedReceiver<Message>,
    ) {
        while let Some(msg) = rx.recv().await {
            if let Err(e) = write.send(msg).await {
                error!("Failed to send WebSocket message: {}", e);
                break;
            }
        }

        debug!("Sender task terminated");
    }

    /// Receiver task: receives messages from WebSocket and invokes callbacks
    async fn receiver_task(
        mut read: futures::stream::SplitStream<WsStream>,
        peer_id: Arc<RwLock<Option<String>>>,
        callbacks: Arc<SignalingCallbacks>,
    ) {
        while let Some(msg_result) = read.next().await {
            match msg_result {
                Ok(Message::Text(text)) => {
                    if let Err(e) = Self::handle_message(&text, &peer_id, &callbacks).await {
                        warn!("Failed to handle signaling message: {}", e);
                    }
                }
                Ok(Message::Close(_)) => {
                    info!("WebSocket connection closed");
                    break;
                }
                Err(e) => {
                    error!("WebSocket error: {}", e);
                    break;
                }
                _ => {}
            }
        }

        debug!("Receiver task terminated");
    }

    /// Handle incoming signaling message
    async fn handle_message(
        text: &str,
        _peer_id: &Arc<RwLock<Option<String>>>,
        callbacks: &Arc<SignalingCallbacks>,
    ) -> Result<()> {
        debug!("Received signaling message: {}", text);

        // Try to parse as JSON-RPC request
        if let Ok(req) = JsonRpcRequest::from_json(text) {
            match req.method.as_str() {
                "peer.announce" => {
                    let params: PeerAnnounceParams =
                        serde_json::from_value(req.params).map_err(|e| {
                            Error::InvalidData(format!("Invalid peer.announce params: {}", e))
                        })?;

                    if let Some(cb) = callbacks.on_peer_announced.lock().await.as_ref() {
                        cb(params.peer_id, params.capabilities);
                    }
                }
                "peer.offer" => {
                    let params: PeerOfferParams =
                        serde_json::from_value(req.params).map_err(|e| {
                            Error::InvalidData(format!("Invalid peer.offer params: {}", e))
                        })?;

                    if let Some(cb) = callbacks.on_offer_received.lock().await.as_ref() {
                        cb(params.from, params.to, params.sdp);
                    }
                }
                "peer.answer" => {
                    let params: PeerAnswerParams =
                        serde_json::from_value(req.params).map_err(|e| {
                            Error::InvalidData(format!("Invalid peer.answer params: {}", e))
                        })?;

                    if let Some(cb) = callbacks.on_answer_received.lock().await.as_ref() {
                        cb(params.from, params.to, params.sdp);
                    }
                }
                "peer.ice_candidate" => {
                    let params: IceCandidateParams =
                        serde_json::from_value(req.params).map_err(|e| {
                            Error::InvalidData(format!("Invalid peer.ice_candidate params: {}", e))
                        })?;

                    if let Some(cb) = callbacks.on_ice_candidate_received.lock().await.as_ref() {
                        cb(params.from.clone(), params.to.clone(), params);
                    }
                }
                "peer.disconnect" => {
                    let params: PeerDisconnectParams =
                        serde_json::from_value(req.params).map_err(|e| {
                            Error::InvalidData(format!("Invalid peer.disconnect params: {}", e))
                        })?;

                    if let Some(cb) = callbacks.on_peer_disconnected.lock().await.as_ref() {
                        cb(params.peer_id);
                    }
                }
                _ => {
                    warn!("Unknown signaling method: {}", req.method);
                }
            }
        }

        Ok(())
    }

    /// Announce this peer to the signaling server
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Local peer identifier
    /// * `capabilities` - List of capabilities (e.g., ["audio", "video"])
    pub async fn announce_peer(&self, peer_id: String, capabilities: Vec<String>) -> Result<()> {
        *self.peer_id.write().await = Some(peer_id.clone());

        let msg = SignalingMessage::PeerAnnounce {
            params: PeerAnnounceParams {
                peer_id,
                capabilities,
                user_data: None,
            },
            id: Some(uuid::Uuid::new_v4().to_string()),
        };

        self.send_message(msg).await
    }

    /// Send SDP offer to remote peer
    ///
    /// # Arguments
    ///
    /// * `to` - Target peer ID
    /// * `sdp` - SDP offer string
    pub async fn send_offer(&self, to: String, sdp: String) -> Result<()> {
        let from = self.peer_id.read().await.clone().ok_or_else(|| {
            Error::InvalidData("Peer ID not set, call announce_peer first".to_string())
        })?;

        let msg = SignalingMessage::PeerOffer {
            params: PeerOfferParams { from, to, sdp },
            id: Some(uuid::Uuid::new_v4().to_string()),
        };

        self.send_message(msg).await
    }

    /// Send SDP answer to remote peer
    ///
    /// # Arguments
    ///
    /// * `to` - Target peer ID
    /// * `sdp` - SDP answer string
    pub async fn send_answer(&self, to: String, sdp: String) -> Result<()> {
        let from = self.peer_id.read().await.clone().ok_or_else(|| {
            Error::InvalidData("Peer ID not set, call announce_peer first".to_string())
        })?;

        let msg = SignalingMessage::PeerAnswer {
            params: PeerAnswerParams { from, to, sdp },
            id: Some(uuid::Uuid::new_v4().to_string()),
        };

        self.send_message(msg).await
    }

    /// Send ICE candidate to remote peer
    ///
    /// # Arguments
    ///
    /// * `to` - Target peer ID
    /// * `candidate` - ICE candidate string
    #[allow(dead_code)] // Phase 4 (US2) ICE candidate exchange
    pub async fn send_ice_candidate(&self, to: String, candidate: String) -> Result<()> {
        let from = self.peer_id.read().await.clone().ok_or_else(|| {
            Error::InvalidData("Peer ID not set, call announce_peer first".to_string())
        })?;

        let msg = SignalingMessage::IceCandidate {
            params: IceCandidateParams {
                from,
                to,
                candidate,
                sdp_mid: None,
                sdp_m_line_index: None,
            },
            id: None, // Notification (no response expected)
        };

        self.send_message(msg).await
    }

    /// Send disconnect notification
    pub async fn disconnect(&self) -> Result<()> {
        let peer_id = self
            .peer_id
            .read()
            .await
            .clone()
            .ok_or_else(|| Error::InvalidData("Peer ID not set".to_string()))?;

        let msg = SignalingMessage::PeerDisconnect {
            params: PeerDisconnectParams {
                peer_id,
                reason: None,
            },
            id: None,
        };

        self.send_message(msg).await
    }

    /// Send a signaling message
    async fn send_message(&self, msg: SignalingMessage) -> Result<()> {
        let json = msg.to_json()?;
        debug!("Sending signaling message: {}", json);

        self.tx
            .send(Message::Text(json))
            .map_err(|e| Error::SignalingError(format!("Failed to send message: {}", e)))?;

        Ok(())
    }

    /// Set callback for peer announced events
    #[allow(dead_code)] // Phase 4 (US2) peer discovery
    pub async fn on_peer_announced<F>(&self, callback: F)
    where
        F: Fn(String, Vec<String>) + Send + Sync + 'static,
    {
        *self.callbacks.on_peer_announced.lock().await = Some(Arc::new(callback));
    }

    /// Set callback for offer received events
    pub async fn on_offer_received<F>(&self, callback: F)
    where
        F: Fn(String, String, String) + Send + Sync + 'static,
    {
        *self.callbacks.on_offer_received.lock().await = Some(Arc::new(callback));
    }

    /// Set callback for answer received events
    pub async fn on_answer_received<F>(&self, callback: F)
    where
        F: Fn(String, String, String) + Send + Sync + 'static,
    {
        *self.callbacks.on_answer_received.lock().await = Some(Arc::new(callback));
    }

    /// Set callback for ICE candidate received events
    pub async fn on_ice_candidate_received<F>(&self, callback: F)
    where
        F: Fn(String, String, IceCandidateParams) + Send + Sync + 'static,
    {
        *self.callbacks.on_ice_candidate_received.lock().await = Some(Arc::new(callback));
    }

    /// Set callback for peer disconnected events
    #[allow(dead_code)] // Phase 4 (US2) peer lifecycle management
    pub async fn on_peer_disconnected<F>(&self, callback: F)
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        *self.callbacks.on_peer_disconnected.lock().await = Some(Arc::new(callback));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signaling_client_creation() {
        let client = SignalingClient::new("ws://localhost:8080");
        assert_eq!(client.url, "ws://localhost:8080");
    }

    #[tokio::test]
    async fn test_message_parsing() {
        let json = r#"{
            "jsonrpc": "2.0",
            "method": "peer.announce",
            "params": {
                "peer_id": "test-peer",
                "capabilities": ["audio", "video"]
            },
            "id": "req-1"
        }"#;

        let req = JsonRpcRequest::from_json(json).unwrap();
        assert_eq!(req.method, "peer.announce");
    }
}
