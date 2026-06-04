//! WebRTC peer connection management
//!
//! Handles peer lifecycle, ICE negotiation, and media track management.

pub mod connection;
pub mod lifecycle;
pub mod manager;
pub mod scanner;

#[cfg(any(feature = "grpc-signaling", feature = "ws-signaling"))]
pub mod server_peer;

pub use connection::{ConnectionState, PeerConnection};
// Re-exports for public API - used by external consumers of this crate
#[allow(unused_imports)]
pub use lifecycle::{
    CircuitBreaker, CircuitState, ConnectionQualityMetrics, ReconnectionManager,
    ReconnectionPolicy, ReconnectionState,
};
pub use manager::{PeerInfo, PeerManager};

#[cfg(any(feature = "grpc-signaling", feature = "ws-signaling"))]
pub use server_peer::{ServerPeer, SharedPipelineSession};
