//! WebSocket signaling server for JSON-RPC 2.0 protocol
//!
//! This module provides WebSocket-based signaling as an alternative to gRPC.
//! It implements JSON-RPC 2.0 over WebSocket for browser compatibility.

mod control_handlers;
mod events;
mod handler;
mod server;

pub use control_handlers::ControlSessionState;
pub use events::{current_timestamp_ns, WebRtcErrorCode, WebRtcEventBridge};
pub use handler::SharedState;
pub use server::WebSocketServerHandle;
pub use server::WebSocketSignalingServer;
