//! gRPC signaling service for WebRTC
//!
//! Provides gRPC-based signaling as an alternative to WebSocket JSON-RPC 2.0.

pub mod service;

pub use service::WebRtcSignalingService;
