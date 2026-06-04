//! WebRTC event types for FFI callbacks
//!
//! These event types are emitted from the WebRTC layer and forwarded to
//! language-specific callbacks (ThreadsafeFunction for Node.js, Py<PyAny> for Python).

use super::config::PeerCapabilities;
use remotemedia_core::data::RuntimeData;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Event emitted when a peer completes WebRTC handshake
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConnectedEvent {
    /// Connected peer's unique identifier
    pub peer_id: String,
    /// Peer's audio/video/data capabilities
    pub capabilities: PeerCapabilities,
    /// Custom peer metadata
    pub metadata: HashMap<String, String>,
}

impl PeerConnectedEvent {
    /// Create a new peer connected event
    pub fn new(peer_id: String, capabilities: PeerCapabilities) -> Self {
        Self {
            peer_id,
            capabilities,
            metadata: HashMap::new(),
        }
    }

    /// Create with metadata
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Event emitted when a peer disconnects
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerDisconnectedEvent {
    /// Disconnected peer's identifier
    pub peer_id: String,
    /// Disconnect reason (optional)
    pub reason: Option<String>,
}

impl PeerDisconnectedEvent {
    /// Create a new peer disconnected event
    pub fn new(peer_id: String) -> Self {
        Self {
            peer_id,
            reason: None,
        }
    }

    /// Create with reason
    pub fn with_reason(mut self, reason: String) -> Self {
        self.reason = Some(reason);
        self
    }
}

/// Event emitted when pipeline produces output for a peer
#[derive(Debug, Clone)]
pub struct PipelineOutputEvent {
    /// Target peer's identifier
    pub peer_id: String,
    /// Pipeline output data
    pub data: RuntimeData,
    /// Nanosecond timestamp
    pub timestamp: u64,
}

impl PipelineOutputEvent {
    /// Create a new pipeline output event
    pub fn new(peer_id: String, data: RuntimeData) -> Self {
        Self {
            peer_id,
            data,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
        }
    }
}

/// Event emitted when raw data received from peer (bypasses pipeline)
#[derive(Debug, Clone)]
pub struct DataReceivedEvent {
    /// Source peer's identifier
    pub peer_id: String,
    /// Raw data bytes
    pub data: Vec<u8>,
    /// Nanosecond timestamp
    pub timestamp: u64,
}

impl DataReceivedEvent {
    /// Create a new data received event
    pub fn new(peer_id: String, data: Vec<u8>) -> Self {
        Self {
            peer_id,
            data,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
        }
    }
}

/// Error codes for WebRTC errors
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    /// Signaling connection failed
    SignalingError,
    /// Peer connection error
    PeerError,
    /// Pipeline execution error
    PipelineError,
    /// Invalid configuration
    ConfigError,
    /// Maximum peer limit reached
    MaxPeersReached,
    /// Session not found
    SessionNotFound,
    /// Peer not found
    PeerNotFound,
    /// Internal error
    InternalError,
    /// Reconnection attempt (informational)
    ReconnectAttempt,
    /// Reconnection failed after max attempts
    ReconnectFailed,
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorCode::SignalingError => write!(f, "SIGNALING_ERROR"),
            ErrorCode::PeerError => write!(f, "PEER_ERROR"),
            ErrorCode::PipelineError => write!(f, "PIPELINE_ERROR"),
            ErrorCode::ConfigError => write!(f, "CONFIG_ERROR"),
            ErrorCode::MaxPeersReached => write!(f, "MAX_PEERS_REACHED"),
            ErrorCode::SessionNotFound => write!(f, "SESSION_NOT_FOUND"),
            ErrorCode::PeerNotFound => write!(f, "PEER_NOT_FOUND"),
            ErrorCode::InternalError => write!(f, "INTERNAL_ERROR"),
            ErrorCode::ReconnectAttempt => write!(f, "RECONNECT_ATTEMPT"),
            ErrorCode::ReconnectFailed => write!(f, "RECONNECT_FAILED"),
        }
    }
}

/// Event emitted when an error occurs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorEvent {
    /// Error code
    pub code: ErrorCode,
    /// Human-readable error message
    pub message: String,
    /// Related peer ID (optional)
    pub peer_id: Option<String>,
}

impl ErrorEvent {
    /// Create a new error event
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            peer_id: None,
        }
    }

    /// Create with peer ID
    pub fn with_peer(mut self, peer_id: String) -> Self {
        self.peer_id = Some(peer_id);
        self
    }
}

/// Session event types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionEventType {
    PeerJoined,
    PeerLeft,
}

impl std::fmt::Display for SessionEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SessionEventType::PeerJoined => write!(f, "peer_joined"),
            SessionEventType::PeerLeft => write!(f, "peer_left"),
        }
    }
}

/// Event emitted for session lifecycle events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEvent {
    /// Session identifier
    pub session_id: String,
    /// Event type
    pub event_type: SessionEventType,
    /// Affected peer's identifier
    pub peer_id: String,
}

impl SessionEvent {
    /// Create a peer joined event
    pub fn peer_joined(session_id: String, peer_id: String) -> Self {
        Self {
            session_id,
            event_type: SessionEventType::PeerJoined,
            peer_id,
        }
    }

    /// Create a peer left event
    pub fn peer_left(session_id: String, peer_id: String) -> Self {
        Self {
            session_id,
            event_type: SessionEventType::PeerLeft,
            peer_id,
        }
    }
}

/// All possible WebRTC events (for internal routing)
#[derive(Debug, Clone)]
pub enum WebRtcEvent {
    PeerConnected(PeerConnectedEvent),
    PeerDisconnected(PeerDisconnectedEvent),
    PipelineOutput(PipelineOutputEvent),
    DataReceived(DataReceivedEvent),
    Error(ErrorEvent),
    Session(SessionEvent),
}

impl WebRtcEvent {
    /// Get the event name for logging/debugging
    pub fn name(&self) -> &'static str {
        match self {
            WebRtcEvent::PeerConnected(_) => "peer_connected",
            WebRtcEvent::PeerDisconnected(_) => "peer_disconnected",
            WebRtcEvent::PipelineOutput(_) => "pipeline_output",
            WebRtcEvent::DataReceived(_) => "data",
            WebRtcEvent::Error(_) => "error",
            WebRtcEvent::Session(_) => "session",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_connected_event() {
        let event = PeerConnectedEvent::new(
            "peer-123".to_string(),
            PeerCapabilities {
                audio: true,
                video: false,
                data: true,
            },
        );
        assert_eq!(event.peer_id, "peer-123");
        assert!(event.capabilities.audio);
        assert!(!event.capabilities.video);
        assert!(event.capabilities.data);
    }

    #[test]
    fn test_error_event_serialization() {
        let event = ErrorEvent::new(ErrorCode::PeerError, "Connection failed")
            .with_peer("peer-123".to_string());

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("PEER_ERROR"));
        assert!(json.contains("Connection failed"));
        assert!(json.contains("peer-123"));
    }

    #[test]
    fn test_session_event() {
        let joined = SessionEvent::peer_joined("room-1".to_string(), "peer-1".to_string());
        assert_eq!(joined.event_type, SessionEventType::PeerJoined);

        let left = SessionEvent::peer_left("room-1".to_string(), "peer-1".to_string());
        assert_eq!(left.event_type, SessionEventType::PeerLeft);
    }
}
