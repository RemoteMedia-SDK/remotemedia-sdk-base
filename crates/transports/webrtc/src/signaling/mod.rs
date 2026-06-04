//! Signaling protocol and client for WebRTC peer discovery and SDP exchange
//!
//! This module implements three signaling transports:
//! - WebSocket-based JSON-RPC 2.0 (default)
//! - WebSocket signaling server (ws-signaling feature)
//! - gRPC bidirectional streaming (optional, requires `grpc-signaling` feature)

pub mod client;
pub mod connection;
pub mod protocol;

#[cfg(feature = "ws-signaling")]
pub mod websocket;

#[cfg(feature = "grpc-signaling")]
pub mod grpc;

pub use client::SignalingClient;
pub use protocol::IceCandidateParams;

#[cfg(feature = "ws-signaling")]
pub use websocket::{
    current_timestamp_ns, SharedState, WebRtcErrorCode, WebRtcEventBridge, WebSocketServerHandle,
    WebSocketSignalingServer,
};

#[cfg(feature = "grpc-signaling")]
pub use grpc::WebRtcSignalingService;
