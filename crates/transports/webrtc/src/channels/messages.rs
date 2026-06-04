//! Data channel message types (T157-T158)
//!
//! Defines the message format for WebRTC data channel communication.
//! Supports JSON, binary, text, runtime-data, and chunked message types.
//!
//! # Message Types
//!
//! - **Control**: Control messages and pipeline configuration (JSON encoded)
//! - **Binary**: Raw bytes for efficient data transfer
//! - **Text**: UTF-8 strings for simple text messages
//! - **RuntimeData**: Protobuf-encoded `DataBuffer` for non-media RuntimeData
//! - **Chunk**: Framed chunk of a larger payload (with reassembly metadata)
//!
//! # RTP vs Data Channel
//!
//! - `RuntimeData::Audio` / `RuntimeData::Video` → RTP media tracks
//! - All other variants → WebRTC Data Channel (this module)

use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use crate::generated::webrtc::data_channel_envelope::Payload;
use crate::generated::webrtc::{
    BinaryFrame, ChunkFrame, ControlJsonFrame, DataChannelEnvelope, FrameKind, RuntimeDataFrame,
    TextFrame,
};

/// Maximum message size for data channels (16 MB).
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Maximum chunk size for a single data channel frame (128 KB).
pub const MAX_CHUNK_SIZE: usize = 128 * 1024;

/// Chunk metadata for reassembling a fragmented payload.
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkInfo {
    /// Logical stream this chunk belongs to (e.g., node output id).
    pub stream_id: String,
    /// Unique identifier for this message within the stream.
    pub message_id: String,
    /// Zero-based index of this chunk within the message.
    pub chunk_index: u32,
    /// Total number of chunks in the message.
    pub total_chunks: Option<u32>,
    /// `true` if this is the last chunk of the message.
    pub is_final: bool,
    /// Optional MIME type or content descriptor.
    pub content_type: Option<String>,
    /// Optional type hint for the reassembled payload.
    pub data_type_hint: Option<String>,
    /// Chunk payload bytes.
    pub data: Vec<u8>,
}

impl ChunkInfo {
    /// Create a new chunk info.
    pub fn new(
        stream_id: impl Into<String>,
        message_id: impl Into<String>,
        chunk_index: u32,
        total_chunks: Option<u32>,
        is_final: bool,
        data: Vec<u8>,
    ) -> Self {
        Self {
            stream_id: stream_id.into(),
            message_id: message_id.into(),
            chunk_index,
            total_chunks,
            is_final,
            content_type: None,
            data_type_hint: None,
            data,
        }
    }

    /// Set the content type hint.
    #[must_use]
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    /// Set the data type hint.
    #[must_use]
    pub fn with_data_type_hint(mut self, hint: impl Into<String>) -> Self {
        self.data_type_hint = Some(hint.into());
        self
    }
}

/// Message types that can be sent over a WebRTC data channel (T157)
#[derive(Debug, Clone, PartialEq)]
pub enum DataChannelMessage {
    Control {
        action: String,
        headers: BTreeMap<String, String>,
        json: Value,
    },

    Text {
        action: String,
        headers: BTreeMap<String, String>,
        text: String,
    },

    Binary {
        action: String,
        headers: BTreeMap<String, String>,
        data: Vec<u8>,
    },

    RuntimeData {
        action: String,
        headers: BTreeMap<String, String>,
        data_buffer: Vec<u8>,
    },

    Chunk {
        action: String,
        headers: BTreeMap<String, String>,
        chunk: ChunkInfo,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum DataChannelEncodeError {
    #[error("failed to serialize control message json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("protobuf encode error: {0}")]
    Protobuf(#[from] prost::EncodeError),
}

#[derive(Debug, thiserror::Error)]
pub enum DataChannelDecodeError {
    #[error("protobuf decode error: {0}")]
    Protobuf(#[from] prost::DecodeError),

    #[error("invalid envelope kind: {0}")]
    InvalidKind(i32),

    #[error("missing payload for envelope kind: {0}")]
    MissingPayload(String),

    #[error("failed to deserialize control message json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("utf8 decode error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

impl DataChannelMessage {
    /// Create a new JSON control message from a serializable value
    pub fn json<T: Serialize>(value: &T) -> Result<Self, serde_json::Error> {
        let json_value = serde_json::to_value(value)?;
        Ok(DataChannelMessage::Control {
            action: "control".to_string(),
            headers: BTreeMap::new(),
            json: json_value,
        })
    }

    /// Create a new binary message
    pub fn binary(data: Vec<u8>) -> Self {
        DataChannelMessage::Binary {
            action: "binary".to_string(),
            headers: BTreeMap::new(),
            data,
        }
    }

    /// Create a new text message
    pub fn text(text: impl Into<String>) -> Self {
        DataChannelMessage::Text {
            action: "text".to_string(),
            headers: BTreeMap::new(),
            text: text.into(),
        }
    }

    /// Create a new RuntimeData message from protobuf-encoded bytes.
    pub fn runtime_data(data_buffer: Vec<u8>) -> Self {
        DataChannelMessage::RuntimeData {
            action: "node_output".to_string(),
            headers: BTreeMap::new(),
            data_buffer,
        }
    }

    /// Create a new chunk message.
    pub fn chunk(chunk: ChunkInfo) -> Self {
        DataChannelMessage::Chunk {
            action: "runtime_data_chunk".to_string(),
            headers: BTreeMap::new(),
            chunk,
        }
    }

    /// Get the size of this message in bytes (returns payload size)
    pub fn size(&self) -> usize {
        match self {
            DataChannelMessage::Control { json, .. } => json.to_string().len(),
            DataChannelMessage::Binary { data, .. } => data.len(),
            DataChannelMessage::Text { text, .. } => text.len(),
            DataChannelMessage::RuntimeData { data_buffer, .. } => data_buffer.len(),
            DataChannelMessage::Chunk { chunk, .. } => chunk.data.len(),
        }
    }

    /// Check if this message exceeds the maximum size
    pub fn exceeds_max_size(&self) -> bool {
        self.size() > MAX_MESSAGE_SIZE
    }

    /// Serialize message to binary protobuf envelope
    pub fn encode(&self) -> Result<Vec<u8>, DataChannelEncodeError> {
        let mut envelope = DataChannelEnvelope {
            version: 1,
            kind: 0,
            action: String::new(),
            headers: std::collections::HashMap::new(),
            payload: None,
        };

        match self {
            DataChannelMessage::Control {
                action,
                headers,
                json,
            } => {
                envelope.kind = FrameKind::Control as i32;
                envelope.action = action.clone();
                envelope.headers = headers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                let json_bytes = serde_json::to_vec(json)?;
                envelope.payload =
                    Some(Payload::ControlJson(ControlJsonFrame { json: json_bytes }));
            }
            DataChannelMessage::Text {
                action,
                headers,
                text,
            } => {
                envelope.kind = FrameKind::Text as i32;
                envelope.action = action.clone();
                envelope.headers = headers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                envelope.payload = Some(Payload::Text(TextFrame { text: text.clone() }));
            }
            DataChannelMessage::Binary {
                action,
                headers,
                data,
            } => {
                envelope.kind = FrameKind::Binary as i32;
                envelope.action = action.clone();
                envelope.headers = headers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                envelope.payload = Some(Payload::Binary(BinaryFrame {
                    data: data.clone(),
                    content_type: headers.get("content_type").cloned(),
                    data_type_hint: headers.get("data_type_hint").cloned(),
                }));
            }
            DataChannelMessage::RuntimeData {
                action,
                headers,
                data_buffer,
            } => {
                envelope.kind = FrameKind::RuntimeData as i32;
                envelope.action = action.clone();
                envelope.headers = headers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                envelope.payload = Some(Payload::RuntimeData(RuntimeDataFrame {
                    data_buffer: data_buffer.clone(),
                    content_type: headers.get("content_type").cloned(),
                    data_type_hint: headers.get("data_type_hint").cloned(),
                }));
            }
            DataChannelMessage::Chunk {
                action,
                headers,
                chunk,
            } => {
                envelope.kind = FrameKind::Chunk as i32;
                envelope.action = action.clone();
                envelope.headers = headers
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();

                envelope.payload = Some(Payload::Chunk(ChunkFrame {
                    stream_id: chunk.stream_id.clone(),
                    message_id: chunk.message_id.clone(),
                    chunk_index: chunk.chunk_index,
                    total_chunks: chunk.total_chunks,
                    is_final: chunk.is_final,
                    content_type: chunk.content_type.clone(),
                    data_type_hint: chunk.data_type_hint.clone(),
                    data: chunk.data.clone(),
                }));
            }
        }

        let mut buf = Vec::new();
        envelope.encode(&mut buf)?;
        Ok(buf)
    }

    /// Deserialize message from binary protobuf envelope (with deprecated JSON fallback)
    pub fn decode(bytes: &[u8]) -> Result<Self, DataChannelDecodeError> {
        let envelope = match DataChannelEnvelope::decode(bytes) {
            Ok(env) => env,
            Err(pb_err) => {
                // Fallback to legacy JSON/base64 decoding
                #[derive(serde::Deserialize)]
                #[serde(tag = "type", content = "payload")]
                enum LegacyMessage {
                    Json(serde_json::Value),
                    Binary(String),
                    Text(String),
                    RuntimeData(String),
                    Chunk(LegacyChunkInfo),
                }

                #[derive(serde::Deserialize)]
                struct LegacyChunkInfo {
                    stream_id: String,
                    message_id: String,
                    chunk_index: u32,
                    total_chunks: Option<u32>,
                    is_final: bool,
                    content_type: Option<String>,
                    data_type_hint: Option<String>,
                    data: String,
                }

                if let Ok(legacy) = serde_json::from_slice::<LegacyMessage>(bytes) {
                    tracing::warn!("Decoded legacy JSON/base64 DataChannelMessage (deprecated!)");
                    use base64::{engine::general_purpose::STANDARD, Engine};
                    match legacy {
                        LegacyMessage::Json(json) => {
                            let action = json
                                .get("action")
                                .and_then(|a| a.as_str())
                                .unwrap_or("control")
                                .to_string();
                            return Ok(DataChannelMessage::Control {
                                action,
                                headers: Default::default(),
                                json,
                            });
                        }
                        LegacyMessage::Binary(b64) => {
                            if let Ok(data) = STANDARD.decode(&b64) {
                                return Ok(DataChannelMessage::Binary {
                                    action: "binary".to_string(),
                                    headers: Default::default(),
                                    data,
                                });
                            }
                        }
                        LegacyMessage::Text(text) => {
                            return Ok(DataChannelMessage::Text {
                                action: "text".to_string(),
                                headers: Default::default(),
                                text,
                            });
                        }
                        LegacyMessage::RuntimeData(b64) => {
                            if let Ok(data_buffer) = STANDARD.decode(&b64) {
                                return Ok(DataChannelMessage::RuntimeData {
                                    action: "node_output".to_string(),
                                    headers: Default::default(),
                                    data_buffer,
                                });
                            }
                        }
                        LegacyMessage::Chunk(chunk) => {
                            if let Ok(data) = STANDARD.decode(&chunk.data) {
                                return Ok(DataChannelMessage::Chunk {
                                    action: "runtime_data_chunk".to_string(),
                                    headers: Default::default(),
                                    chunk: ChunkInfo {
                                        stream_id: chunk.stream_id,
                                        message_id: chunk.message_id,
                                        chunk_index: chunk.chunk_index,
                                        total_chunks: chunk.total_chunks,
                                        is_final: chunk.is_final,
                                        content_type: chunk.content_type,
                                        data_type_hint: chunk.data_type_hint,
                                        data,
                                    },
                                });
                            }
                        }
                    }
                }
                return Err(DataChannelDecodeError::Protobuf(pb_err));
            }
        };

        let kind = FrameKind::try_from(envelope.kind)
            .map_err(|_| DataChannelDecodeError::InvalidKind(envelope.kind))?;

        let action = envelope.action;
        let headers: BTreeMap<String, String> = envelope.headers.into_iter().collect();

        let payload = envelope
            .payload
            .ok_or_else(|| DataChannelDecodeError::MissingPayload(format!("{:?}", kind)))?;

        match (kind, payload) {
            (FrameKind::Control, Payload::ControlJson(frame)) => {
                let json_value = serde_json::from_slice(&frame.json)?;
                Ok(DataChannelMessage::Control {
                    action,
                    headers,
                    json: json_value,
                })
            }
            (FrameKind::Text, Payload::Text(frame)) => Ok(DataChannelMessage::Text {
                action,
                headers,
                text: frame.text,
            }),
            (FrameKind::Binary, Payload::Binary(frame)) => Ok(DataChannelMessage::Binary {
                action,
                headers,
                data: frame.data,
            }),
            (FrameKind::RuntimeData, Payload::RuntimeData(frame)) => {
                Ok(DataChannelMessage::RuntimeData {
                    action,
                    headers,
                    data_buffer: frame.data_buffer,
                })
            }
            (FrameKind::Chunk, Payload::Chunk(frame)) => {
                let chunk_info = ChunkInfo {
                    stream_id: frame.stream_id,
                    message_id: frame.message_id,
                    chunk_index: frame.chunk_index,
                    total_chunks: frame.total_chunks,
                    is_final: frame.is_final,
                    content_type: frame.content_type,
                    data_type_hint: frame.data_type_hint,
                    data: frame.data,
                };
                Ok(DataChannelMessage::Chunk {
                    action,
                    headers,
                    chunk: chunk_info,
                })
            }
            _ => Err(DataChannelDecodeError::MissingPayload(format!(
                "Payload mismatch for frame kind {:?}",
                kind
            ))),
        }
    }

    /// Check if this is a JSON message
    pub fn is_json(&self) -> bool {
        matches!(self, DataChannelMessage::Control { .. })
    }

    /// Check if this is a binary message
    pub fn is_binary(&self) -> bool {
        matches!(self, DataChannelMessage::Binary { .. })
    }

    /// Check if this is a text message
    pub fn is_text(&self) -> bool {
        matches!(self, DataChannelMessage::Text { .. })
    }

    /// Check if this is a RuntimeData message
    pub fn is_runtime_data(&self) -> bool {
        matches!(self, DataChannelMessage::RuntimeData { .. })
    }

    /// Check if this is a chunk message
    pub fn is_chunk(&self) -> bool {
        matches!(self, DataChannelMessage::Chunk { .. })
    }

    /// Get the JSON payload if this is a JSON message
    pub fn as_json(&self) -> Option<&serde_json::Value> {
        match self {
            DataChannelMessage::Control { json, .. } => Some(json),
            _ => None,
        }
    }

    /// Get the binary payload if this is a binary message
    pub fn as_binary(&self) -> Option<&[u8]> {
        match self {
            DataChannelMessage::Binary { data, .. } => Some(data),
            _ => None,
        }
    }

    /// Get the text payload if this is a text message
    pub fn as_text(&self) -> Option<&str> {
        match self {
            DataChannelMessage::Text { text, .. } => Some(text),
            _ => None,
        }
    }

    /// Get the protobuf-encoded DataBuffer bytes if this is a RuntimeData message
    pub fn as_runtime_data(&self) -> Option<&[u8]> {
        match self {
            DataChannelMessage::RuntimeData { data_buffer, .. } => Some(data_buffer),
            _ => None,
        }
    }

    /// Get the chunk info if this is a chunk message
    pub fn as_chunk(&self) -> Option<&ChunkInfo> {
        match self {
            DataChannelMessage::Chunk { chunk, .. } => Some(chunk),
            _ => None,
        }
    }
}

/// Control message types for pipeline management
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action")]
pub enum ControlMessage {
    /// Request to reconfigure the pipeline
    Reconfigure {
        /// New manifest JSON for the pipeline
        manifest: serde_json::Value,
    },

    /// Request to pause media streaming
    Pause,

    /// Request to resume media streaming
    Resume,

    /// Request current pipeline status
    GetStatus,

    /// Status response
    Status {
        /// Current pipeline state
        state: String,
        /// Number of active nodes
        active_nodes: usize,
        /// Current timestamp
        timestamp_ms: u64,
    },

    /// Ping for latency measurement
    Ping {
        /// Timestamp when ping was sent
        timestamp_ms: u64,
    },

    /// Pong response to ping
    Pong {
        /// Original ping timestamp
        ping_timestamp_ms: u64,
        /// Timestamp when pong was sent
        pong_timestamp_ms: u64,
    },

    /// Custom application message
    Custom {
        /// Message type identifier
        message_type: String,
        /// Message payload
        data: serde_json::Value,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ControlMessageDecodeError {
    #[error("data channel message is not a control message")]
    NotControlMessage,

    #[error("failed to decode control message json: {0}")]
    Json(#[from] serde_json::Error),
}

impl ControlMessage {
    /// Convert to DataChannelMessage
    pub fn to_data_channel_message(&self) -> Result<DataChannelMessage, serde_json::Error> {
        let action = match self {
            ControlMessage::Reconfigure { .. } => "reconfigure",
            ControlMessage::Pause => "pause",
            ControlMessage::Resume => "resume",
            ControlMessage::GetStatus => "get_status",
            ControlMessage::Status { .. } => "status",
            ControlMessage::Ping { .. } => "ping",
            ControlMessage::Pong { .. } => "pong",
            ControlMessage::Custom { message_type, .. } => message_type.as_str(),
        };

        Ok(DataChannelMessage::Control {
            action: action.to_string(),
            headers: Default::default(),
            json: serde_json::to_value(self)?,
        })
    }

    /// Parse from DataChannelMessage
    pub fn from_data_channel_message(
        msg: &DataChannelMessage,
    ) -> Result<Self, ControlMessageDecodeError> {
        match msg {
            DataChannelMessage::Control { json, .. } => Ok(serde_json::from_value(json.clone())?),
            _ => Err(ControlMessageDecodeError::NotControlMessage),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_json_message() {
        let msg = DataChannelMessage::json(&json!({
            "key": "value",
            "number": 42
        }))
        .unwrap();

        assert!(msg.is_json());
        assert!(!msg.is_binary());
        assert!(!msg.is_text());

        let json = msg.as_json().unwrap();
        assert_eq!(json["key"], "value");
        assert_eq!(json["number"], 42);
    }

    #[test]
    fn test_binary_message() {
        let data = vec![1, 2, 3, 4, 5];
        let msg = DataChannelMessage::binary(data.clone());

        assert!(msg.is_binary());
        assert_eq!(msg.as_binary(), Some(&data[..]));
    }

    #[test]
    fn test_text_message() {
        let msg = DataChannelMessage::text("Hello, World!");

        assert!(msg.is_text());
        assert_eq!(msg.as_text(), Some("Hello, World!"));
    }

    #[test]
    fn test_message_serialization() {
        let msg = DataChannelMessage::text("test");
        let bytes = msg.encode().unwrap();
        let decoded = DataChannelMessage::decode(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_binary_serialization() {
        let msg = DataChannelMessage::binary(vec![0xDE, 0xAD, 0xBE, 0xEF]);
        let bytes = msg.encode().unwrap();
        let decoded = DataChannelMessage::decode(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_message_size() {
        let msg = DataChannelMessage::text("12345");
        assert_eq!(msg.size(), 5);
        assert!(!msg.exceeds_max_size());
    }

    #[test]
    fn test_control_message_reconfigure() {
        let ctrl = ControlMessage::Reconfigure {
            manifest: json!({"nodes": []}),
        };
        let msg = ctrl.to_data_channel_message().unwrap();
        let decoded = ControlMessage::from_data_channel_message(&msg).unwrap();
        match decoded {
            ControlMessage::Reconfigure { manifest } => {
                assert_eq!(manifest["nodes"], json!([]));
            }
            _ => panic!("Expected Reconfigure"),
        }
    }

    #[test]
    fn test_control_message_ping_pong() {
        let ping = ControlMessage::Ping {
            timestamp_ms: 1234567890,
        };
        let msg = ping.to_data_channel_message().unwrap();
        let decoded = ControlMessage::from_data_channel_message(&msg).unwrap();
        match decoded {
            ControlMessage::Ping { timestamp_ms } => {
                assert_eq!(timestamp_ms, 1234567890);
            }
            _ => panic!("Expected Ping"),
        }
    }

    #[test]
    fn test_runtime_data_message() {
        let payload = vec![1, 2, 3, 4, 5];
        let msg = DataChannelMessage::runtime_data(payload.clone());

        assert!(msg.is_runtime_data());
        assert!(!msg.is_chunk());
        assert_eq!(msg.as_runtime_data(), Some(&payload[..]));
        assert_eq!(msg.size(), 5);
    }

    #[test]
    fn test_runtime_data_serialization() {
        let payload = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let msg = DataChannelMessage::runtime_data(payload.clone());
        let bytes = msg.encode().unwrap();
        let decoded = DataChannelMessage::decode(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_chunk_message() {
        let chunk = ChunkInfo::new("stream-1", "msg-abc", 0, Some(3), false, vec![1, 2, 3])
            .with_content_type("application/octet-stream")
            .with_data_type_hint("binary");

        let msg = DataChannelMessage::chunk(chunk.clone());
        assert!(msg.is_chunk());
        assert!(!msg.is_runtime_data());

        let info = msg.as_chunk().unwrap();
        assert_eq!(info.stream_id, "stream-1");
        assert_eq!(info.message_id, "msg-abc");
        assert_eq!(info.chunk_index, 0);
        assert_eq!(info.total_chunks, Some(3));
        assert!(!info.is_final);
        assert_eq!(
            info.content_type,
            Some("application/octet-stream".to_string())
        );
        assert_eq!(info.data_type_hint, Some("binary".to_string()));
        assert_eq!(info.data, vec![1, 2, 3]);
    }

    #[test]
    fn test_chunk_final_message() {
        let chunk = ChunkInfo::new("s", "m", 2, Some(3), true, vec![4, 5]);
        let msg = DataChannelMessage::chunk(chunk);

        let info = msg.as_chunk().unwrap();
        assert!(info.is_final);
        assert_eq!(info.chunk_index, 2);
    }

    #[test]
    fn test_chunk_serialization() {
        let chunk = ChunkInfo::new("stream-1", "msg-1", 0, Some(2), false, vec![1, 2, 3]);
        let msg = DataChannelMessage::chunk(chunk);
        let bytes = msg.encode().unwrap();
        let decoded = DataChannelMessage::decode(&bytes).unwrap();
        assert_eq!(msg, decoded);
    }

    #[test]
    fn test_chunk_size() {
        let data = vec![0u8; 1000];
        let chunk = ChunkInfo::new("s", "m", 0, Some(1), true, data.clone());
        let msg = DataChannelMessage::chunk(chunk);
        assert_eq!(msg.size(), 1000);
    }

    #[test]
    fn test_max_chunk_size_constant() {
        assert_eq!(MAX_CHUNK_SIZE, 128 * 1024);
        assert!(MAX_CHUNK_SIZE < MAX_MESSAGE_SIZE);
    }

    #[test]
    fn binary_wire_format_does_not_use_json_or_base64() {
        let msg = DataChannelMessage::Binary {
            action: "model_weights".to_string(),
            headers: Default::default(),
            data: vec![0xDE, 0xAD, 0xBE, 0xEF],
        };

        let encoded = msg.encode().unwrap();
        let encoded_text = String::from_utf8_lossy(&encoded);

        assert!(!encoded_text.contains("\"type\""));
        assert!(!encoded_text.contains("\"payload\""));
        assert!(!encoded_text.contains("3q2+7w=="));

        let decoded = DataChannelMessage::decode(&encoded).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_control_message_from_data_channel_message_non_control() {
        let msg = DataChannelMessage::runtime_data(vec![1, 2, 3]);
        let err = ControlMessage::from_data_channel_message(&msg).unwrap_err();
        assert!(matches!(err, ControlMessageDecodeError::NotControlMessage));
    }
}
