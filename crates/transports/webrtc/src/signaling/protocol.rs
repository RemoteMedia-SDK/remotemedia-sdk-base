//! JSON-RPC 2.0 signaling protocol types

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// JSON-RPC 2.0 protocol version
#[allow(dead_code)] // Used by JsonRpcRequest/Response constructors
pub const JSONRPC_VERSION: &str = "2.0";

/// JSON-RPC 2.0 request
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcRequest {
    /// Protocol version (must be "2.0")
    pub jsonrpc: String,

    /// Method name to invoke
    pub method: String,

    /// Method parameters
    pub params: serde_json::Value,

    /// Request ID for matching with response (optional for notifications)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<serde_json::Value>,
}

/// JSON-RPC 2.0 response (success)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcResponse {
    /// Protocol version (must be "2.0")
    pub jsonrpc: String,

    /// Result data
    pub result: serde_json::Value,

    /// Request ID this response corresponds to
    pub id: serde_json::Value,
}

/// JSON-RPC 2.0 error response
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonRpcError {
    /// Protocol version (must be "2.0")
    pub jsonrpc: String,

    /// Error details
    pub error: ErrorObject,

    /// Request ID this error corresponds to
    pub id: serde_json::Value,
}

/// JSON-RPC 2.0 error object
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ErrorObject {
    /// Error code
    pub code: i32,

    /// Human-readable error message
    pub message: String,

    /// Additional error data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// Standard JSON-RPC 2.0 error codes
#[allow(dead_code)] // Error codes for Phase 4 (US2) signaling error handling
pub mod error_codes {
    /// Invalid JSON was received
    pub const PARSE_ERROR: i32 = -32700;

    /// The JSON sent is not a valid Request object
    pub const INVALID_REQUEST: i32 = -32600;

    /// The method does not exist / is not available
    pub const METHOD_NOT_FOUND: i32 = -32601;

    /// Invalid method parameter(s)
    pub const INVALID_PARAMS: i32 = -32602;

    /// Internal JSON-RPC error
    pub const INTERNAL_ERROR: i32 = -32603;

    // WebRTC-specific error codes

    /// Peer not found in registry
    pub const PEER_NOT_FOUND: i32 = -32000;

    /// Invalid SDP offer
    pub const OFFER_INVALID: i32 = -32002;

    /// Invalid SDP answer
    pub const ANSWER_INVALID: i32 = -32003;

    /// Invalid ICE candidate
    pub const ICE_CANDIDATE_INVALID: i32 = -32004;

    /// Negotiation already in progress (spec 017)
    pub const NEGOTIATION_IN_PROGRESS: i32 = -32005;

    /// ICE connection timeout (spec 017)
    pub const CONNECTION_TIMEOUT: i32 = -32006;

    /// No compatible codecs (spec 017)
    pub const CODEC_MISMATCH: i32 = -32007;

    /// Session limit exceeded
    pub const SESSION_LIMIT_EXCEEDED: i32 = -32001;
}

/// Signaling message types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "method", rename_all = "snake_case")]
pub enum SignalingMessage {
    /// Announce peer to signaling server
    #[serde(rename = "peer.announce")]
    PeerAnnounce {
        /// Request parameters
        params: PeerAnnounceParams,
        /// Request ID
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
    },

    /// Send SDP offer to remote peer
    #[serde(rename = "peer.offer")]
    PeerOffer {
        /// Request parameters
        params: PeerOfferParams,
        /// Request ID
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
    },

    /// Send SDP answer to remote peer
    #[serde(rename = "peer.answer")]
    PeerAnswer {
        /// Request parameters
        params: PeerAnswerParams,
        /// Request ID
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
    },

    /// Send ICE candidate to remote peer
    #[serde(rename = "peer.ice_candidate")]
    IceCandidate {
        /// Request parameters
        params: IceCandidateParams,
        /// Request ID
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
    },

    /// Notify disconnection from peer
    #[serde(rename = "peer.disconnect")]
    PeerDisconnect {
        /// Request parameters
        params: PeerDisconnectParams,
        /// Request ID
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
    },
}

/// Parameters for peer.announce
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PeerAnnounceParams {
    /// Unique peer identifier
    pub peer_id: String,

    /// Capabilities (audio, video, data)
    pub capabilities: Vec<String>,

    /// Optional user metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_data: Option<HashMap<String, serde_json::Value>>,
}

/// Parameters for peer.offer
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PeerOfferParams {
    /// Sender peer ID
    pub from: String,

    /// Recipient peer ID
    pub to: String,

    /// SDP offer
    pub sdp: String,
}

/// Parameters for peer.answer
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PeerAnswerParams {
    /// Sender peer ID
    pub from: String,

    /// Recipient peer ID
    pub to: String,

    /// SDP answer
    pub sdp: String,
}

/// Parameters for peer.ice_candidate
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IceCandidateParams {
    /// Sender peer ID
    pub from: String,

    /// Recipient peer ID
    pub to: String,

    /// ICE candidate string
    pub candidate: String,

    /// SDP media line index
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdp_mid: Option<String>,

    /// SDP media line index number
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdp_m_line_index: Option<u16>,
}

/// Parameters for peer.disconnect
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PeerDisconnectParams {
    /// Peer ID that is disconnecting
    pub peer_id: String,

    /// Optional disconnection reason
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Parameters for peer.state_change notification (spec 017)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PeerStateChangeParams {
    /// Peer ID whose state changed
    pub peer_id: String,

    /// Connection state (new, connecting, connected, disconnected, failed, closed)
    pub connection_state: String,

    /// ICE connection state
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ice_connection_state: Option<String>,

    /// ICE gathering state
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ice_gathering_state: Option<String>,

    /// Previous connection state
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_state: Option<String>,

    /// Timestamp of state change (epoch ms)
    pub timestamp: u64,
}

impl SignalingMessage {
    /// Convert message to JSON string
    pub fn to_json(&self) -> crate::Result<String> {
        serde_json::to_string(self).map_err(|e| {
            crate::Error::SerializationError(format!(
                "Failed to serialize signaling message: {}",
                e
            ))
        })
    }

    /// Parse message from JSON string
    #[allow(dead_code)] // Used in tests and Phase 4 (US2) signaling
    pub fn from_json(json: &str) -> crate::Result<Self> {
        serde_json::from_str(json).map_err(|e| {
            crate::Error::SerializationError(format!(
                "Failed to deserialize signaling message: {}",
                e
            ))
        })
    }

    /// Get the request ID if present
    #[allow(dead_code)] // Phase 4 (US2) request/response correlation
    pub fn request_id(&self) -> Option<&str> {
        match self {
            SignalingMessage::PeerAnnounce { id, .. } => id.as_deref(),
            SignalingMessage::PeerOffer { id, .. } => id.as_deref(),
            SignalingMessage::PeerAnswer { id, .. } => id.as_deref(),
            SignalingMessage::IceCandidate { id, .. } => id.as_deref(),
            SignalingMessage::PeerDisconnect { id, .. } => id.as_deref(),
        }
    }

    /// Get the method name
    #[allow(dead_code)] // Phase 4 (US2) message routing
    pub fn method_name(&self) -> &str {
        match self {
            SignalingMessage::PeerAnnounce { .. } => "peer.announce",
            SignalingMessage::PeerOffer { .. } => "peer.offer",
            SignalingMessage::PeerAnswer { .. } => "peer.answer",
            SignalingMessage::IceCandidate { .. } => "peer.ice_candidate",
            SignalingMessage::PeerDisconnect { .. } => "peer.disconnect",
        }
    }
}

#[allow(dead_code)] // Phase 4 (US2) signaling protocol implementation
impl JsonRpcRequest {
    /// Create a new JSON-RPC request
    pub fn new(method: String, params: serde_json::Value, id: Option<serde_json::Value>) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_string(),
            method,
            params,
            id,
        }
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> crate::Result<String> {
        serde_json::to_string(self).map_err(|e| {
            crate::Error::SerializationError(format!("Failed to serialize JSON-RPC request: {}", e))
        })
    }

    /// Parse from JSON string
    pub fn from_json(json: &str) -> crate::Result<Self> {
        serde_json::from_str(json).map_err(|e| {
            crate::Error::SerializationError(format!(
                "Failed to deserialize JSON-RPC request: {}",
                e
            ))
        })
    }
}

#[allow(dead_code)] // Phase 4 (US2) signaling protocol implementation
impl JsonRpcResponse {
    /// Create a new JSON-RPC response
    pub fn new(result: serde_json::Value, id: serde_json::Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_string(),
            result,
            id,
        }
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> crate::Result<String> {
        serde_json::to_string(self).map_err(|e| {
            crate::Error::SerializationError(format!(
                "Failed to serialize JSON-RPC response: {}",
                e
            ))
        })
    }
}

#[allow(dead_code)] // Phase 4 (US2) signaling error handling
impl JsonRpcError {
    /// Create a new JSON-RPC error
    pub fn new(code: i32, message: String, id: serde_json::Value) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_string(),
            error: ErrorObject {
                code,
                message,
                data: None,
            },
            id,
        }
    }

    /// Create a new JSON-RPC error with data
    pub fn with_data(
        code: i32,
        message: String,
        data: serde_json::Value,
        id: serde_json::Value,
    ) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION.to_string(),
            error: ErrorObject {
                code,
                message,
                data: Some(data),
            },
            id,
        }
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> crate::Result<String> {
        serde_json::to_string(self).map_err(|e| {
            crate::Error::SerializationError(format!("Failed to serialize JSON-RPC error: {}", e))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_announce_serialization() {
        let msg = SignalingMessage::PeerAnnounce {
            params: PeerAnnounceParams {
                peer_id: "peer-123".to_string(),
                capabilities: vec!["audio".to_string(), "video".to_string()],
                user_data: None,
            },
            id: Some("req-1".to_string()),
        };

        let json = msg.to_json().unwrap();
        let parsed = SignalingMessage::from_json(&json).unwrap();
        assert_eq!(msg, parsed);
    }

    #[test]
    fn test_peer_offer_serialization() {
        let msg = SignalingMessage::PeerOffer {
            params: PeerOfferParams {
                from: "peer-alice".to_string(),
                to: "peer-bob".to_string(),
                sdp: "v=0\r\no=- ...".to_string(),
            },
            id: Some("offer-1".to_string()),
        };

        let json = msg.to_json().unwrap();
        let parsed = SignalingMessage::from_json(&json).unwrap();
        assert_eq!(msg, parsed);
    }

    #[test]
    fn test_jsonrpc_request() {
        let req = JsonRpcRequest::new(
            "peer.announce".to_string(),
            serde_json::json!({"peer_id": "test"}),
            Some(serde_json::json!("req-1")),
        );

        assert_eq!(req.jsonrpc, "2.0");
        assert_eq!(req.method, "peer.announce");

        let json = req.to_json().unwrap();
        let parsed = JsonRpcRequest::from_json(&json).unwrap();
        assert_eq!(req, parsed);
    }

    #[test]
    fn test_jsonrpc_response() {
        let resp = JsonRpcResponse::new(
            serde_json::json!({"success": true}),
            serde_json::json!("req-1"),
        );

        assert_eq!(resp.jsonrpc, "2.0");
        let json = resp.to_json().unwrap();
        assert!(json.contains("\"result\""));
    }

    #[test]
    fn test_jsonrpc_error() {
        let err = JsonRpcError::new(
            error_codes::PEER_NOT_FOUND,
            "Peer not found".to_string(),
            serde_json::json!("req-1"),
        );

        assert_eq!(err.error.code, -32000);
        let json = err.to_json().unwrap();
        assert!(json.contains("\"error\""));
    }

    #[test]
    fn test_ice_candidate_with_optional_fields() {
        let msg = SignalingMessage::IceCandidate {
            params: IceCandidateParams {
                from: "peer-alice".to_string(),
                to: "peer-bob".to_string(),
                candidate: "candidate:...".to_string(),
                sdp_mid: Some("audio".to_string()),
                sdp_m_line_index: Some(0),
            },
            id: None, // Notification (no response expected)
        };

        let json = msg.to_json().unwrap();
        let parsed = SignalingMessage::from_json(&json).unwrap();
        assert_eq!(msg, parsed);
    }

    #[test]
    fn test_method_name() {
        let msg = SignalingMessage::PeerAnnounce {
            params: PeerAnnounceParams {
                peer_id: "test".to_string(),
                capabilities: vec!["audio".to_string()],
                user_data: None,
            },
            id: None,
        };

        assert_eq!(msg.method_name(), "peer.announce");
    }
}
