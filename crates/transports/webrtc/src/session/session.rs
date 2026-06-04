//! Session management for WebRTC pipeline execution
//!
//! Maps incoming WebRTC media streams to RemoteMedia pipeline sessions.

use crate::{Error, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Session identifier
pub type SessionId = String;

/// Session state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    /// Session is initializing
    Initializing,
    /// Session is active and processing data
    Active,
    /// Session is paused
    Paused,
    /// Session is closing
    Closing,
    /// Session is closed
    Closed,
}

/// WebRTC streaming session
///
/// Represents a single streaming session that processes media through
/// a RemoteMedia pipeline and routes output to connected peers.
pub struct Session {
    /// Unique session identifier
    session_id: SessionId,

    /// Current session state
    state: Arc<RwLock<SessionState>>,

    /// Peer IDs associated with this session
    peers: Arc<RwLock<Vec<String>>>,

    /// Session metadata (arbitrary key-value pairs)
    metadata: Arc<RwLock<HashMap<String, String>>>,
}

impl Session {
    /// Create a new session
    ///
    /// # Arguments
    ///
    /// * `session_id` - Unique identifier for the session
    pub fn new(session_id: SessionId) -> Self {
        info!("Creating new session: {}", session_id);

        Self {
            session_id,
            state: Arc::new(RwLock::new(SessionState::Initializing)),
            peers: Arc::new(RwLock::new(Vec::new())),
            metadata: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the session ID
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Get the current session state
    pub async fn state(&self) -> SessionState {
        *self.state.read().await
    }

    /// Set the session state
    pub async fn set_state(&self, new_state: SessionState) -> Result<()> {
        let mut state = self.state.write().await;
        let old_state = *state;

        if old_state != new_state {
            debug!(
                "Session {} state transition: {:?} -> {:?}",
                self.session_id, old_state, new_state
            );
            *state = new_state;
        }

        Ok(())
    }

    /// Add a peer to this session
    pub async fn add_peer(&self, peer_id: String) -> Result<()> {
        let mut peers = self.peers.write().await;

        if !peers.contains(&peer_id) {
            debug!("Adding peer {} to session {}", peer_id, self.session_id);
            peers.push(peer_id);
        }

        Ok(())
    }

    /// Remove a peer from this session
    pub async fn remove_peer(&self, peer_id: &str) -> Result<()> {
        let mut peers = self.peers.write().await;

        if let Some(pos) = peers.iter().position(|p| p == peer_id) {
            debug!("Removing peer {} from session {}", peer_id, self.session_id);
            peers.remove(pos);
        }

        Ok(())
    }

    /// Get all peers in this session
    pub async fn list_peers(&self) -> Vec<String> {
        self.peers.read().await.clone()
    }

    /// Set session metadata
    pub async fn set_metadata(&self, key: String, value: String) -> Result<()> {
        self.metadata.write().await.insert(key, value);
        Ok(())
    }

    /// Get session metadata
    pub async fn get_metadata(&self, key: &str) -> Option<String> {
        self.metadata.read().await.get(key).cloned()
    }

    /// Close the session
    pub async fn close(&self) -> Result<()> {
        info!("Closing session: {}", self.session_id);
        self.set_state(SessionState::Closed).await?;
        Ok(())
    }
}

/// Session manager
///
/// Manages multiple streaming sessions and their lifecycle.
pub struct SessionManager {
    /// Active sessions
    sessions: Arc<RwLock<HashMap<SessionId, Arc<Session>>>>,

    /// Maximum number of concurrent sessions (0 = unlimited)
    max_sessions: usize,
}

impl SessionManager {
    /// Create a new session manager
    ///
    /// # Arguments
    ///
    /// * `max_sessions` - Maximum number of concurrent sessions (0 = unlimited)
    pub fn new(max_sessions: usize) -> Result<Self> {
        info!("Creating session manager (max_sessions: {})", max_sessions);

        Ok(Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            max_sessions,
        })
    }

    /// Create a new session
    ///
    /// # Arguments
    ///
    /// * `session_id` - Unique identifier for the session
    pub async fn create_session(&self, session_id: SessionId) -> Result<Arc<Session>> {
        let mut sessions = self.sessions.write().await;

        // Check if session already exists
        if sessions.contains_key(&session_id) {
            return Err(Error::SessionError(format!(
                "Session {} already exists",
                session_id
            )));
        }

        // Check max sessions limit
        if self.max_sessions > 0 && sessions.len() >= self.max_sessions {
            return Err(Error::SessionError(format!(
                "Maximum number of sessions reached ({})",
                self.max_sessions
            )));
        }

        // Create new session
        let session = Arc::new(Session::new(session_id.clone()));
        sessions.insert(session_id, session.clone());

        Ok(session)
    }

    /// Get an existing session
    ///
    /// # Arguments
    ///
    /// * `session_id` - Session identifier
    pub async fn get_session(&self, session_id: &str) -> Result<Arc<Session>> {
        self.sessions
            .read()
            .await
            .get(session_id)
            .cloned()
            .ok_or_else(|| Error::SessionError(format!("Session {} not found", session_id)))
    }

    /// Check if a session exists
    pub async fn has_session(&self, session_id: &str) -> bool {
        self.sessions.read().await.contains_key(session_id)
    }

    /// Remove a session
    ///
    /// # Arguments
    ///
    /// * `session_id` - Session identifier
    pub async fn remove_session(&self, session_id: &str) -> Result<()> {
        info!("Removing session: {}", session_id);

        let mut sessions = self.sessions.write().await;

        if let Some(session) = sessions.remove(session_id) {
            // Close the session
            session.close().await?;
        }

        Ok(())
    }

    /// List all active sessions
    pub async fn list_sessions(&self) -> Vec<SessionId> {
        self.sessions.read().await.keys().cloned().collect()
    }

    /// Get session count
    pub async fn session_count(&self) -> usize {
        self.sessions.read().await.len()
    }

    /// Clear all sessions
    pub async fn clear(&self) -> Result<()> {
        info!("Clearing all sessions");

        let session_ids: Vec<SessionId> = self.sessions.read().await.keys().cloned().collect();

        for session_id in session_ids {
            self.remove_session(&session_id).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_session_creation() {
        let session = Session::new("test-session".to_string());

        assert_eq!(session.session_id(), "test-session");
        assert_eq!(session.state().await, SessionState::Initializing);
        assert_eq!(session.list_peers().await.len(), 0);
    }

    #[tokio::test]
    async fn test_session_state_transitions() {
        let session = Session::new("test-session".to_string());

        session.set_state(SessionState::Active).await.unwrap();
        assert_eq!(session.state().await, SessionState::Active);

        session.set_state(SessionState::Paused).await.unwrap();
        assert_eq!(session.state().await, SessionState::Paused);

        session.close().await.unwrap();
        assert_eq!(session.state().await, SessionState::Closed);
    }

    #[tokio::test]
    async fn test_session_peer_management() {
        let session = Session::new("test-session".to_string());

        // Add peers
        session.add_peer("peer-1".to_string()).await.unwrap();
        session.add_peer("peer-2".to_string()).await.unwrap();

        let peers = session.list_peers().await;
        assert_eq!(peers.len(), 2);
        assert!(peers.contains(&"peer-1".to_string()));
        assert!(peers.contains(&"peer-2".to_string()));

        // Remove peer
        session.remove_peer("peer-1").await.unwrap();
        let peers = session.list_peers().await;
        assert_eq!(peers.len(), 1);
        assert!(peers.contains(&"peer-2".to_string()));
    }

    #[tokio::test]
    async fn test_session_metadata() {
        let session = Session::new("test-session".to_string());

        session
            .set_metadata("key1".to_string(), "value1".to_string())
            .await
            .unwrap();
        session
            .set_metadata("key2".to_string(), "value2".to_string())
            .await
            .unwrap();

        assert_eq!(
            session.get_metadata("key1").await,
            Some("value1".to_string())
        );
        assert_eq!(
            session.get_metadata("key2").await,
            Some("value2".to_string())
        );
        assert_eq!(session.get_metadata("key3").await, None);
    }

    #[tokio::test]
    async fn test_session_manager_creation() {
        let manager = SessionManager::new(10).unwrap();

        assert_eq!(manager.session_count().await, 0);
        assert_eq!(manager.list_sessions().await.len(), 0);
    }

    #[tokio::test]
    async fn test_session_manager_create_session() {
        let manager = SessionManager::new(10).unwrap();

        let session = manager
            .create_session("session-1".to_string())
            .await
            .unwrap();

        assert_eq!(session.session_id(), "session-1");
        assert_eq!(manager.session_count().await, 1);
        assert!(manager.has_session("session-1").await);
    }

    #[tokio::test]
    async fn test_session_manager_duplicate_session() {
        let manager = SessionManager::new(10).unwrap();

        manager
            .create_session("session-1".to_string())
            .await
            .unwrap();

        let result = manager.create_session("session-1".to_string()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_session_manager_max_sessions() {
        let manager = SessionManager::new(2).unwrap();

        manager
            .create_session("session-1".to_string())
            .await
            .unwrap();
        manager
            .create_session("session-2".to_string())
            .await
            .unwrap();

        let result = manager.create_session("session-3".to_string()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_session_manager_get_session() {
        let manager = SessionManager::new(10).unwrap();

        manager
            .create_session("session-1".to_string())
            .await
            .unwrap();

        let session = manager.get_session("session-1").await.unwrap();
        assert_eq!(session.session_id(), "session-1");

        let result = manager.get_session("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_session_manager_remove_session() {
        let manager = SessionManager::new(10).unwrap();

        manager
            .create_session("session-1".to_string())
            .await
            .unwrap();

        assert_eq!(manager.session_count().await, 1);

        manager.remove_session("session-1").await.unwrap();

        assert_eq!(manager.session_count().await, 0);
        assert!(!manager.has_session("session-1").await);
    }

    #[tokio::test]
    async fn test_session_manager_clear() {
        let manager = SessionManager::new(10).unwrap();

        manager
            .create_session("session-1".to_string())
            .await
            .unwrap();
        manager
            .create_session("session-2".to_string())
            .await
            .unwrap();

        assert_eq!(manager.session_count().await, 2);

        manager.clear().await.unwrap();

        assert_eq!(manager.session_count().await, 0);
    }
}
