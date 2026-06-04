//! WebRtcSession napi binding
//!
//! Implements the Node.js WebRtcSession class for room/group management
//! with Event Emitter pattern for peer join/leave events.

use super::config::SessionInfo;
use crate::webrtc::core::WebRtcServerCore;
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Type aliases for callback storage
type PeerJoinedCallback = ThreadsafeFunction<String, ErrorStrategy::Fatal>;
type PeerLeftCallback = ThreadsafeFunction<String, ErrorStrategy::Fatal>;

/// WebRTC session (room) for Node.js
///
/// Provides logical grouping of peers with session-specific events.
/// Created via WebRtcServer::createSession().
#[napi]
pub struct WebRtcSession {
    /// Reference to the parent server core
    core: Arc<WebRtcServerCore>,

    /// Session ID
    session_id: String,

    /// Peer joined callbacks
    on_peer_joined: Arc<Mutex<Vec<PeerJoinedCallback>>>,

    /// Peer left callbacks
    on_peer_left: Arc<Mutex<Vec<PeerLeftCallback>>>,
}

impl WebRtcSession {
    /// Create a new WebRtcSession (internal constructor)
    pub(crate) fn new(core: Arc<WebRtcServerCore>, session_id: String) -> Self {
        Self {
            core,
            session_id,
            on_peer_joined: Arc::new(Mutex::new(Vec::new())),
            on_peer_left: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Emit peer joined event to all registered callbacks
    async fn emit_peer_joined(&self, peer_id: &str) {
        let callbacks = self.on_peer_joined.lock().await;
        for callback in callbacks.iter() {
            callback.call(peer_id.to_string(), ThreadsafeFunctionCallMode::NonBlocking);
        }
    }

    /// Emit peer left event to all registered callbacks
    async fn emit_peer_left(&self, peer_id: &str) {
        let callbacks = self.on_peer_left.lock().await;
        for callback in callbacks.iter() {
            callback.call(peer_id.to_string(), ThreadsafeFunctionCallMode::NonBlocking);
        }
    }
}

#[napi]
impl WebRtcSession {
    /// Get the session ID
    #[napi(getter)]
    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }

    /// Get the peer IDs in this session
    #[napi(getter)]
    pub async fn peers(&self) -> Result<Vec<String>> {
        match self.core.get_session(&self.session_id).await {
            Some(session) => Ok(session.peers),
            None => Err(Error::from_reason(format!(
                "Session not found: {}",
                self.session_id
            ))),
        }
    }

    /// Get the session creation timestamp
    #[napi(getter)]
    pub async fn created_at(&self) -> Result<f64> {
        match self.core.get_session(&self.session_id).await {
            Some(session) => Ok(session.created_at as f64),
            None => Err(Error::from_reason(format!(
                "Session not found: {}",
                self.session_id
            ))),
        }
    }

    /// Get the session metadata
    #[napi(getter)]
    pub async fn metadata(&self) -> Result<HashMap<String, String>> {
        match self.core.get_session(&self.session_id).await {
            Some(session) => Ok(session.metadata),
            None => Err(Error::from_reason(format!(
                "Session not found: {}",
                self.session_id
            ))),
        }
    }

    /// Register an event listener for session events
    ///
    /// @param event - Event name ('peer_joined', 'peer_left')
    /// @param callback - Event handler function
    #[napi]
    pub fn on(&self, _env: Env, event: String, callback: JsFunction) -> Result<()> {
        match event.as_str() {
            "peer_joined" => {
                let tsfn: PeerJoinedCallback = callback.create_threadsafe_function(
                    0,
                    |ctx: napi::threadsafe_function::ThreadSafeCallContext<String>| {
                        Ok(vec![ctx.value])
                    },
                )?;
                let callbacks = self.on_peer_joined.clone();
                tokio::spawn(async move {
                    callbacks.lock().await.push(tsfn);
                });
            }
            "peer_left" => {
                let tsfn: PeerLeftCallback = callback.create_threadsafe_function(
                    0,
                    |ctx: napi::threadsafe_function::ThreadSafeCallContext<String>| {
                        Ok(vec![ctx.value])
                    },
                )?;
                let callbacks = self.on_peer_left.clone();
                tokio::spawn(async move {
                    callbacks.lock().await.push(tsfn);
                });
            }
            _ => {
                return Err(Error::from_reason(format!(
                    "Unknown session event: {}. Valid events: peer_joined, peer_left",
                    event
                )));
            }
        }
        Ok(())
    }

    /// Broadcast data to all peers in the session
    ///
    /// @param data - Data to broadcast (Buffer)
    #[napi]
    pub async fn broadcast(&self, data: Buffer) -> Result<()> {
        self.core
            .broadcast_to_session(&self.session_id, data.to_vec())
            .await
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Send data to a specific peer in the session
    ///
    /// @param peer_id - Target peer ID
    /// @param data - Data to send (Buffer)
    #[napi]
    pub async fn send_to_peer(&self, peer_id: String, data: Buffer) -> Result<()> {
        // Verify peer is in this session
        let session = self
            .core
            .get_session(&self.session_id)
            .await
            .ok_or_else(|| Error::from_reason(format!("Session not found: {}", self.session_id)))?;

        if !session.peers.contains(&peer_id) {
            return Err(Error::from_reason(format!(
                "Peer {} is not in session {}",
                peer_id, self.session_id
            )));
        }

        self.core
            .send_to_peer(&peer_id, data.to_vec())
            .await
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Add a peer to this session
    ///
    /// @param peer_id - Peer ID to add
    #[napi]
    pub async fn add_peer(&self, peer_id: String) -> Result<()> {
        self.core
            .add_peer_to_session(&self.session_id, &peer_id)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;

        // Emit peer_joined event to all registered callbacks
        self.emit_peer_joined(&peer_id).await;
        Ok(())
    }

    /// Remove a peer from this session
    ///
    /// @param peer_id - Peer ID to remove
    #[napi]
    pub async fn remove_peer(&self, peer_id: String) -> Result<()> {
        self.core
            .remove_peer_from_session(&self.session_id, &peer_id)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;

        // Emit peer_left event to all registered callbacks
        self.emit_peer_left(&peer_id).await;
        Ok(())
    }

    /// Get session info
    #[napi]
    pub async fn get_info(&self) -> Result<SessionInfo> {
        match self.core.get_session(&self.session_id).await {
            Some(session) => Ok(session.into()),
            None => Err(Error::from_reason(format!(
                "Session not found: {}",
                self.session_id
            ))),
        }
    }
}
