//! Node.js event types for WebRTC callbacks
//!
//! These types are passed to JavaScript callbacks via ThreadsafeFunction.

use super::config::PeerCapabilities;
use crate::webrtc::events as core_events;
use napi_derive::napi;
use std::collections::HashMap;

/// Event data for peer_connected callback
#[napi(object)]
#[derive(Debug, Clone)]
pub struct PeerConnectedData {
    /// Connected peer's unique identifier
    pub peer_id: String,
    /// Peer's audio/video/data capabilities
    pub capabilities: PeerCapabilities,
    /// Custom peer metadata as key-value pairs
    pub metadata: HashMap<String, String>,
}

impl From<core_events::PeerConnectedEvent> for PeerConnectedData {
    fn from(event: core_events::PeerConnectedEvent) -> Self {
        PeerConnectedData {
            peer_id: event.peer_id,
            capabilities: PeerCapabilities {
                audio: event.capabilities.audio,
                video: event.capabilities.video,
                data: event.capabilities.data,
            },
            metadata: event.metadata,
        }
    }
}

/// Event data for peer_disconnected callback
#[napi(object)]
#[derive(Debug, Clone)]
pub struct PeerDisconnectedData {
    /// Disconnected peer's identifier
    pub peer_id: String,
    /// Disconnect reason (optional)
    pub reason: Option<String>,
}

impl From<core_events::PeerDisconnectedEvent> for PeerDisconnectedData {
    fn from(event: core_events::PeerDisconnectedEvent) -> Self {
        PeerDisconnectedData {
            peer_id: event.peer_id,
            reason: event.reason,
        }
    }
}

/// Event data for pipeline_output callback
#[napi(object)]
#[derive(Debug, Clone)]
pub struct PipelineOutputData {
    /// Target peer's identifier
    pub peer_id: String,
    /// Pipeline output data type
    pub data_type: String,
    /// Pipeline output data as JSON string
    pub data: String,
    /// Nanosecond timestamp
    pub timestamp: f64,
}

impl From<core_events::PipelineOutputEvent> for PipelineOutputData {
    fn from(event: core_events::PipelineOutputEvent) -> Self {
        use remotemedia_core::data::RuntimeData;

        let (data_type, data) = match &event.data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                ..
            } => (
                "audio".to_string(),
                serde_json::json!({
                    "sample_rate": sample_rate,
                    "channels": channels,
                    "num_samples": samples.len(),
                })
                .to_string(),
            ),
            RuntimeData::Text(content) => (
                "text".to_string(),
                serde_json::json!({ "content": content }).to_string(),
            ),
            RuntimeData::Video {
                width,
                height,
                format,
                ..
            } => (
                "video".to_string(),
                serde_json::json!({
                    "width": width,
                    "height": height,
                    "format": format!("{:?}", format),
                })
                .to_string(),
            ),
            RuntimeData::Binary(data) => (
                "binary".to_string(),
                serde_json::json!({
                    "size": data.len(),
                })
                .to_string(),
            ),
            RuntimeData::Json(value) => ("json".to_string(), value.to_string()),
            _ => ("unknown".to_string(), "{}".to_string()),
        };

        PipelineOutputData {
            peer_id: event.peer_id,
            data_type,
            data,
            timestamp: event.timestamp as f64,
        }
    }
}

/// Event data for data callback (raw data received)
#[napi(object)]
#[derive(Debug, Clone)]
pub struct DataReceivedData {
    /// Source peer's identifier
    pub peer_id: String,
    /// Raw data size in bytes
    pub size: u32,
    /// Nanosecond timestamp
    pub timestamp: f64,
}

impl From<core_events::DataReceivedEvent> for DataReceivedData {
    fn from(event: core_events::DataReceivedEvent) -> Self {
        DataReceivedData {
            peer_id: event.peer_id,
            size: event.data.len() as u32,
            timestamp: event.timestamp as f64,
        }
    }
}

/// Event data for error callback
#[napi(object)]
#[derive(Debug, Clone)]
pub struct ErrorData {
    /// Error code
    pub code: String,
    /// Human-readable error message
    pub message: String,
    /// Related peer ID (optional)
    pub peer_id: Option<String>,
}

impl From<core_events::ErrorEvent> for ErrorData {
    fn from(event: core_events::ErrorEvent) -> Self {
        ErrorData {
            code: event.code.to_string(),
            message: event.message,
            peer_id: event.peer_id,
        }
    }
}

/// Event data for session callbacks
#[napi(object)]
#[derive(Debug, Clone)]
pub struct SessionEventData {
    /// Session identifier
    pub session_id: String,
    /// Event type ("peer_joined" or "peer_left")
    pub event_type: String,
    /// Affected peer's identifier (optional)
    pub peer_id: Option<String>,
}

impl From<core_events::SessionEvent> for SessionEventData {
    fn from(event: core_events::SessionEvent) -> Self {
        let event_type = match event.event_type {
            core_events::SessionEventType::PeerJoined => "peer_joined".to_string(),
            core_events::SessionEventType::PeerLeft => "peer_left".to_string(),
        };
        SessionEventData {
            session_id: event.session_id,
            event_type,
            peer_id: Some(event.peer_id),
        }
    }
}
