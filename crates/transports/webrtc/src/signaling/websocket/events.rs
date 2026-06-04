//! WebRTC event bridge types for FFI integration
//!
//! These event types flow from the WebSocket signaling handler to the FFI layer
//! without creating circular dependencies between crates.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Bridge event types for WebRTC FFI integration
///
/// These events are emitted from the WebSocket signaling handler and converted
/// to FFI-specific event types in the FFI layer.
#[derive(Debug, Clone)]
pub enum WebRtcEventBridge {
    /// Emitted when a peer completes the announce handshake
    PeerConnected {
        /// Unique peer identifier
        peer_id: String,
        /// Peer capabilities (e.g., ["audio", "video", "data"])
        capabilities: Vec<String>,
        /// Custom peer metadata
        metadata: HashMap<String, Value>,
    },

    /// Emitted when a peer disconnects
    PeerDisconnected {
        /// Disconnected peer's identifier
        peer_id: String,
        /// Optional disconnect reason
        reason: Option<String>,
    },

    /// Emitted when pipeline produces output for a peer
    PipelineOutput {
        /// Target peer's identifier
        peer_id: String,
        /// Serialized RuntimeData (using serde_json for cross-crate compatibility)
        data_json: String,
        /// Nanosecond timestamp
        timestamp_ns: u64,
    },

    /// Emitted when raw data arrives on a WebRTC data channel
    DataReceived {
        /// Source peer's identifier
        peer_id: String,
        /// Raw data bytes
        data: Vec<u8>,
        /// Nanosecond timestamp
        timestamp_ns: u64,
    },

    /// Emitted on errors
    Error {
        /// Error code for categorization
        code: WebRtcErrorCode,
        /// Human-readable error message
        message: String,
        /// Related peer ID (if applicable)
        peer_id: Option<String>,
    },
}

/// Error codes for WebRTC bridge events
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WebRtcErrorCode {
    /// Signaling connection error
    SignalingError,
    /// Peer connection error
    PeerError,
    /// Pipeline execution error
    PipelineError,
    /// Internal error
    InternalError,
}

impl WebRtcEventBridge {
    /// Create a peer connected event
    pub fn peer_connected(
        peer_id: String,
        capabilities: Vec<String>,
        metadata: HashMap<String, Value>,
    ) -> Self {
        Self::PeerConnected {
            peer_id,
            capabilities,
            metadata,
        }
    }

    /// Create a peer disconnected event
    pub fn peer_disconnected(peer_id: String, reason: Option<String>) -> Self {
        Self::PeerDisconnected { peer_id, reason }
    }

    /// Create a pipeline output event
    pub fn pipeline_output(peer_id: String, data_json: String, timestamp_ns: u64) -> Self {
        Self::PipelineOutput {
            peer_id,
            data_json,
            timestamp_ns,
        }
    }

    /// Create a data received event
    pub fn data_received(peer_id: String, data: Vec<u8>, timestamp_ns: u64) -> Self {
        Self::DataReceived {
            peer_id,
            data,
            timestamp_ns,
        }
    }

    /// Create an error event
    pub fn error(code: WebRtcErrorCode, message: String, peer_id: Option<String>) -> Self {
        Self::Error {
            code,
            message,
            peer_id,
        }
    }

    /// Get the event name for logging/debugging
    pub fn name(&self) -> &'static str {
        match self {
            Self::PeerConnected { .. } => "peer_connected",
            Self::PeerDisconnected { .. } => "peer_disconnected",
            Self::PipelineOutput { .. } => "pipeline_output",
            Self::DataReceived { .. } => "data_received",
            Self::Error { .. } => "error",
        }
    }
}

/// Get current timestamp in nanoseconds
pub fn current_timestamp_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_connected_event() {
        let event = WebRtcEventBridge::peer_connected(
            "peer-123".to_string(),
            vec!["audio".to_string(), "video".to_string()],
            HashMap::new(),
        );
        assert_eq!(event.name(), "peer_connected");
        if let WebRtcEventBridge::PeerConnected {
            peer_id,
            capabilities,
            ..
        } = event
        {
            assert_eq!(peer_id, "peer-123");
            assert_eq!(capabilities.len(), 2);
        } else {
            panic!("Expected PeerConnected event");
        }
    }

    #[test]
    fn test_peer_disconnected_event() {
        let event = WebRtcEventBridge::peer_disconnected(
            "peer-123".to_string(),
            Some("timeout".to_string()),
        );
        assert_eq!(event.name(), "peer_disconnected");
    }

    #[test]
    fn test_data_received_event() {
        let event = WebRtcEventBridge::data_received(
            "peer-123".to_string(),
            vec![1, 2, 3, 4],
            current_timestamp_ns(),
        );
        assert_eq!(event.name(), "data_received");
    }
}
