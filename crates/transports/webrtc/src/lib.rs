//! WebRTC transport for RemoteMedia pipelines
//!
//! This crate provides WebRTC-based real-time media streaming transport with
//! multi-peer mesh networking support.
//!
//! # Features
//!
//! - **Multi-peer mesh topology**: Up to 10 simultaneous peer connections
//! - **Audio/Video synchronization**: Per-peer sync managers with jitter buffers
//! - **Media codecs**: Opus audio, VP9/H264 video
//! - **Data channels**: Reliable/unreliable messaging
//! - **JSON-RPC 2.0 signaling**: WebSocket-based peer discovery and SDP exchange
//! - **RemoteMedia pipeline integration**: Implements PipelineTransport trait
//!
//! # Architecture
//!
//! ```text
//! ┌────────────────────────────────────────────────────────┐
//! │  WebRTC Peers (Browser/Native)                         │
//! │  ↓ (WebRTC peer connections - mesh topology)           │
//! │  WebRtcTransport                                       │
//! │  ├─ SignalingClient (JSON-RPC 2.0 over WebSocket)     │
//! │  ├─ PeerManager (mesh of PeerConnections)             │
//! │  │   └─ Per-peer SyncManager (A/V sync, jitter)       │
//! │  ├─ SessionManager (pipeline sessions)                 │
//! │  └─ SessionRouter (routes data: peers ↔ pipeline)     │
//! │     ↓                                                   │
//! │  remotemedia-core::PipelineExecutor            │
//! └────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```
//! use remotemedia_webrtc::WebRtcTransportConfig;
//!
//! // Configure transport
//! let config = WebRtcTransportConfig {
//!     signaling_url: "ws://localhost:8080".to_string(),
//!     stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
//!     max_peers: 10,
//!     ..Default::default()
//! };
//!
//! // Validate configuration
//! assert!(config.validate().is_ok());
//! assert_eq!(config.max_peers, 10);
//! ```
//!
//! ## Async Usage
//!
//! ```no_run
//! use remotemedia_webrtc::{WebRtcTransport, WebRtcTransportConfig};
//!
//! # async fn example() -> remotemedia_webrtc::Result<()> {
//! let config = WebRtcTransportConfig::default();
//! let transport = WebRtcTransport::new(config)?;
//!
//! // Start transport
//! transport.start().await?;
//!
//! // Connect to peer
//! let peer_id = transport.connect_peer("peer-abc123".to_string()).await?;
//! # Ok(())
//! # }
//! ```

#![warn(clippy::all)]

// Public modules
pub mod builder;
#[cfg(feature = "cli")]
pub mod cli;
pub mod client;
pub mod config;
#[cfg(feature = "grpc-signaling")]
pub mod control;
pub mod custom_nodes;
pub mod error;
pub mod plugin;

// Internal modules
#[cfg(not(feature = "grpc-signaling"))]
mod signaling;
#[cfg(feature = "grpc-signaling")]
pub mod signaling;

pub mod channels;
pub mod media;
mod peer;
mod session;
mod sync;
mod transport;

// Protobuf adapters
pub mod adapters;

// Generated protobuf code
pub mod generated {
    // Common types (DataBuffer, etc.) from remotemedia.v1 package
    include!("generated/remotemedia.v1.rs");

    // WebRTC signaling types from remotemedia.v1.webrtc package
    pub mod webrtc {
        include!("generated/remotemedia.v1.webrtc.rs");
    }
}

// Re-exports for public API
pub use client::{WebRtcPipelineClient, WebRtcStreamSession};
pub use config::{
    AudioCodec, ConfigOptions, DataChannelMode, TurnServerConfig, VideoCodec, VideoResolution,
    WebRtcTransportConfig,
};
pub use error::{Error, Result};
pub use peer::{ConnectionState, PeerInfo};
pub use plugin::WebRtcTransportPlugin;
pub use session::{Session, SessionId, SessionManager, SessionState};
pub use transport::WebRtcTransport;

pub use builder::WebRtcServerBuilder;
#[cfg(feature = "grpc-signaling")]
pub use builder::WebRtcSignalingServerBuilder;

#[cfg(feature = "cli")]
pub use cli::WebRtcServeArgs;
#[cfg(all(feature = "cli", feature = "grpc-signaling"))]
pub use cli::WebRtcSignalingServeArgs;

/// Get the version of this crate
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        let ver = version();
        assert!(!ver.is_empty());
    }
}
