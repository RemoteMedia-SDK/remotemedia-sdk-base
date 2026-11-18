//! WebRTC peer connection management
//!
//! Handles peer lifecycle, ICE negotiation, and media track management.

pub mod connection;
pub mod lifecycle;
pub mod manager;
pub mod scanner;

#[cfg(any(feature = "grpc-signaling", feature = "ws-signaling"))]
mod data_channel_ingress;

#[cfg(any(feature = "grpc-signaling", feature = "ws-signaling"))]
mod media_ready;

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
pub(crate) use data_channel_ingress::{decode_pipeline_input, END_OF_INPUT_ACTION};
#[cfg(any(feature = "grpc-signaling", feature = "ws-signaling"))]
pub(crate) use server_peer::send_framed;
#[cfg(any(feature = "grpc-signaling", feature = "ws-signaling"))]
pub use server_peer::DATA_CHANNEL_FRAME_LIMIT;
#[cfg(any(feature = "grpc-signaling", feature = "ws-signaling"))]
pub use server_peer::{ServerPeer, SharedPipelineSession};
