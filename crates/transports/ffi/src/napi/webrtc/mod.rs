//! Node.js WebRTC bindings for RemoteMedia
//!
//! This module provides napi-rs bindings for WebRTC transport,
//! enabling Node.js applications to create and manage WebRTC media servers
//! with embedded or external signaling.
//!
//! # Architecture
//!
//! - **config.rs**: WebRtcServerConfig and related types
//! - **events.rs**: Event types (PeerConnectedEvent, PipelineOutputEvent, etc.)
//! - **server.rs**: WebRtcServer napi class with Event Emitter pattern
//! - **session.rs**: WebRtcSession for room/group management
//!
//! # Event Emitter Pattern
//!
//! Uses `ThreadsafeFunction` for event callbacks, following Node.js conventions:
//!
//! ```javascript
//! const server = await WebRtcServer.create({ port: 50051, ... });
//!
//! server.on('peer_connected', ({ peerId, capabilities }) => {
//!     console.log(`Peer ${peerId} connected`);
//! });
//!
//! server.on('pipeline_output', ({ peerId, data }) => {
//!     console.log(`Output for ${peerId}:`, data);
//! });
//!
//! await server.start();
//! ```
//!
//! # Threading Model
//!
//! Unlike the IPC bindings, WebRTC types from `webrtc-rs` are `Send + Sync`
//! and can be used directly in tokio async tasks. Events are forwarded to
//! JavaScript via `ThreadsafeFunction` callbacks in non-blocking mode.

pub mod config;
pub mod events;
pub mod server;
pub mod session;

// Re-export main types
pub use config::*;
pub use events::*;
pub use server::*;
pub use session::*;
