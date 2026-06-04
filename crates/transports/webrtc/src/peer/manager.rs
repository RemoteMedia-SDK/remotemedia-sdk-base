//! Peer connection management

// Phase 4 (US2) peer management infrastructure
#![allow(dead_code)]

use super::connection::{ConnectionState, PeerConnection};
use crate::{Error, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Information about a connected peer
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Peer identifier
    pub peer_id: String,

    /// Connection state
    pub state: ConnectionState,

    /// Connection duration (if connected)
    pub duration_secs: Option<u64>,
}

/// Manages multiple peer connections in a mesh topology
pub struct PeerManager {
    /// Map of peer_id to PeerConnection
    peers: Arc<RwLock<HashMap<String, Arc<PeerConnection>>>>,

    /// Maximum number of peers allowed
    max_peers: u32,
}

impl PeerManager {
    /// Create a new peer manager
    ///
    /// # Arguments
    ///
    /// * `max_peers` - Maximum number of simultaneous peer connections (1-10)
    pub fn new(max_peers: u32) -> Result<Self> {
        if max_peers == 0 || max_peers > 10 {
            return Err(Error::InvalidConfig(format!(
                "max_peers must be in range 1-10, got {}",
                max_peers
            )));
        }

        Ok(Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
            max_peers,
        })
    }

    /// Add a new peer connection
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Unique identifier for the peer
    /// * `connection` - The PeerConnection instance
    ///
    /// # Errors
    ///
    /// Returns error if max_peers limit is reached or peer_id already exists.
    pub async fn add_peer(&self, peer_id: String, connection: Arc<PeerConnection>) -> Result<()> {
        let mut peers = self.peers.write().await;

        // Check max peers limit
        if peers.len() >= self.max_peers as usize {
            return Err(Error::PeerConnectionError(format!(
                "Maximum peer limit reached ({})",
                self.max_peers
            )));
        }

        // Check if peer already exists
        if peers.contains_key(&peer_id) {
            return Err(Error::PeerConnectionError(format!(
                "Peer {} already exists",
                peer_id
            )));
        }

        info!("Adding peer to manager: {}", peer_id);
        peers.insert(peer_id, connection);

        Ok(())
    }

    /// Remove a peer connection
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Identifier of the peer to remove
    pub async fn remove_peer(&self, peer_id: &str) -> Result<()> {
        let mut peers = self.peers.write().await;

        if let Some(peer) = peers.remove(peer_id) {
            info!("Removing peer from manager: {}", peer_id);

            // Close the connection
            if let Err(e) = peer.close().await {
                warn!("Error closing peer connection for {}: {}", peer_id, e);
            }

            Ok(())
        } else {
            Err(Error::PeerNotFound(peer_id.to_string()))
        }
    }

    /// Get a peer connection by ID
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Identifier of the peer to retrieve
    pub async fn get_peer(&self, peer_id: &str) -> Result<Arc<PeerConnection>> {
        let peers = self.peers.read().await;

        peers
            .get(peer_id)
            .cloned()
            .ok_or_else(|| Error::PeerNotFound(peer_id.to_string()))
    }

    /// List all connected peers (in Connected state only)
    pub async fn list_connected_peers(&self) -> Vec<PeerInfo> {
        let peers = self.peers.read().await;

        let mut connected_peers = Vec::new();

        for (peer_id, connection) in peers.iter() {
            let state = connection.state().await;

            if state == ConnectionState::Connected {
                let duration_secs = connection.connection_duration().await.map(|d| d.as_secs());

                connected_peers.push(PeerInfo {
                    peer_id: peer_id.clone(),
                    state,
                    duration_secs,
                });
            }
        }

        connected_peers
    }

    /// List all peers regardless of state
    pub async fn list_all_peers(&self) -> Vec<PeerInfo> {
        let peers = self.peers.read().await;

        let mut all_peers = Vec::new();

        for (peer_id, connection) in peers.iter() {
            let state = connection.state().await;
            let duration_secs = connection.connection_duration().await.map(|d| d.as_secs());

            all_peers.push(PeerInfo {
                peer_id: peer_id.clone(),
                state,
                duration_secs,
            });
        }

        all_peers
    }

    /// Get the number of peers in the manager
    pub async fn peer_count(&self) -> usize {
        self.peers.read().await.len()
    }

    /// Check if a peer exists
    pub async fn has_peer(&self, peer_id: &str) -> bool {
        self.peers.read().await.contains_key(peer_id)
    }

    /// Remove all peers and close their connections
    pub async fn clear(&self) -> Result<()> {
        debug!("Clearing all peers from manager");

        let mut peers = self.peers.write().await;

        for (peer_id, connection) in peers.drain() {
            debug!("Closing connection for peer: {}", peer_id);
            if let Err(e) = connection.close().await {
                warn!("Error closing peer {}: {}", peer_id, e);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WebRtcTransportConfig;

    #[tokio::test]
    async fn test_peer_manager_creation() {
        let manager = PeerManager::new(10).unwrap();
        assert_eq!(manager.peer_count().await, 0);
    }

    #[tokio::test]
    async fn test_invalid_max_peers() {
        assert!(PeerManager::new(0).is_err());
        assert!(PeerManager::new(11).is_err());
    }

    #[tokio::test]
    async fn test_add_and_get_peer() {
        let manager = PeerManager::new(10).unwrap();
        let config = WebRtcTransportConfig::default();

        let peer = Arc::new(
            PeerConnection::new("peer-1".to_string(), &config)
                .await
                .unwrap(),
        );
        manager
            .add_peer("peer-1".to_string(), peer.clone())
            .await
            .unwrap();

        assert_eq!(manager.peer_count().await, 1);

        let retrieved = manager.get_peer("peer-1").await.unwrap();
        assert_eq!(retrieved.peer_id(), "peer-1");
    }

    #[tokio::test]
    async fn test_remove_peer() {
        let manager = PeerManager::new(10).unwrap();
        let config = WebRtcTransportConfig::default();

        let peer = Arc::new(
            PeerConnection::new("peer-1".to_string(), &config)
                .await
                .unwrap(),
        );
        manager.add_peer("peer-1".to_string(), peer).await.unwrap();

        manager.remove_peer("peer-1").await.unwrap();
        assert_eq!(manager.peer_count().await, 0);
    }

    #[tokio::test]
    async fn test_max_peers_limit() {
        let manager = PeerManager::new(2).unwrap();
        let config = WebRtcTransportConfig::default();

        let peer1 = Arc::new(
            PeerConnection::new("peer-1".to_string(), &config)
                .await
                .unwrap(),
        );
        let peer2 = Arc::new(
            PeerConnection::new("peer-2".to_string(), &config)
                .await
                .unwrap(),
        );
        let peer3 = Arc::new(
            PeerConnection::new("peer-3".to_string(), &config)
                .await
                .unwrap(),
        );

        manager.add_peer("peer-1".to_string(), peer1).await.unwrap();
        manager.add_peer("peer-2".to_string(), peer2).await.unwrap();

        // Third peer should fail
        assert!(manager.add_peer("peer-3".to_string(), peer3).await.is_err());
    }

    #[tokio::test]
    async fn test_duplicate_peer() {
        let manager = PeerManager::new(10).unwrap();
        let config = WebRtcTransportConfig::default();

        let peer1 = Arc::new(
            PeerConnection::new("peer-1".to_string(), &config)
                .await
                .unwrap(),
        );
        let peer2 = Arc::new(
            PeerConnection::new("peer-1".to_string(), &config)
                .await
                .unwrap(),
        );

        manager.add_peer("peer-1".to_string(), peer1).await.unwrap();

        // Duplicate should fail
        assert!(manager.add_peer("peer-1".to_string(), peer2).await.is_err());
    }

    #[tokio::test]
    async fn test_list_peers() {
        let manager = PeerManager::new(10).unwrap();
        let config = WebRtcTransportConfig::default();

        let peer1 = Arc::new(
            PeerConnection::new("peer-1".to_string(), &config)
                .await
                .unwrap(),
        );
        let peer2 = Arc::new(
            PeerConnection::new("peer-2".to_string(), &config)
                .await
                .unwrap(),
        );

        manager.add_peer("peer-1".to_string(), peer1).await.unwrap();
        manager.add_peer("peer-2".to_string(), peer2).await.unwrap();

        let all_peers = manager.list_all_peers().await;
        assert_eq!(all_peers.len(), 2);

        // No peers are connected yet
        let connected = manager.list_connected_peers().await;
        assert_eq!(connected.len(), 0);
    }

    #[tokio::test]
    async fn test_clear() {
        let manager = PeerManager::new(10).unwrap();
        let config = WebRtcTransportConfig::default();

        let peer1 = Arc::new(
            PeerConnection::new("peer-1".to_string(), &config)
                .await
                .unwrap(),
        );
        let peer2 = Arc::new(
            PeerConnection::new("peer-2".to_string(), &config)
                .await
                .unwrap(),
        );

        manager.add_peer("peer-1".to_string(), peer1).await.unwrap();
        manager.add_peer("peer-2".to_string(), peer2).await.unwrap();

        manager.clear().await.unwrap();
        assert_eq!(manager.peer_count().await, 0);
    }
}
