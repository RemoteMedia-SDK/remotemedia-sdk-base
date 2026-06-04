//! Control message data structure
//!
//! Standardized message for pipeline control flow including cancellation,
//! batching hints, and deadline warnings. Propagates across all execution contexts.
//!
//! The `ControlMessageType` enum (the wire-shape type tag) lives in
//! `remotemedia-types` and is re-exported here. The richer
//! `ControlMessage` struct (uuid-tagged, validation, IPC byte
//! codec) is core-only because it pulls `uuid` and is not part of
//! the wire `RuntimeData::ControlMessage` field shape.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

pub use remotemedia_types::ControlMessageType;

/// Standardized message for pipeline control flow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlMessage {
    /// Type of control message
    pub message_type: ControlMessageType,

    /// Session ID this message applies to
    pub session_id: String,

    /// Timestamp when message was created (microseconds since epoch)
    pub timestamp: u64,

    /// Optional target segment ID (for cancellation)
    pub target_segment_id: Option<Uuid>,

    /// Extensible metadata (JSON-compatible)
    #[serde(default)]
    pub metadata: JsonValue,
}

impl ControlMessage {
    /// Create a new cancel speculation message
    pub fn cancel_speculation(
        session_id: String,
        from_timestamp: u64,
        to_timestamp: u64,
        target_segment_id: Option<Uuid>,
    ) -> Self {
        Self {
            message_type: ControlMessageType::CancelSpeculation {
                from_timestamp,
                to_timestamp,
            },
            session_id,
            timestamp: current_timestamp_us(),
            target_segment_id,
            metadata: JsonValue::Null,
        }
    }

    /// Create a new batch hint message
    pub fn batch_hint(session_id: String, suggested_batch_size: usize) -> Self {
        Self {
            message_type: ControlMessageType::BatchHint {
                suggested_batch_size,
            },
            session_id,
            timestamp: current_timestamp_us(),
            target_segment_id: None,
            metadata: JsonValue::Null,
        }
    }

    /// Create a new deadline warning message
    pub fn deadline_warning(session_id: String, deadline_us: u64) -> Self {
        Self {
            message_type: ControlMessageType::DeadlineWarning { deadline_us },
            session_id,
            timestamp: current_timestamp_us(),
            target_segment_id: None,
            metadata: JsonValue::Null,
        }
    }

    /// Validate control message
    ///
    /// Returns Ok(()) if message is valid, Err with reason if invalid
    pub fn validate(&self) -> Result<(), String> {
        // Check timestamp is not too old (warn if >1 second stale)
        let now = current_timestamp_us();
        let age_ms = (now.saturating_sub(self.timestamp)) / 1000;
        if age_ms > 1000 {
            return Err(format!("Message is {}ms old (>1000ms threshold)", age_ms));
        }

        // Validate message-type specific constraints
        match &self.message_type {
            ControlMessageType::CancelSpeculation {
                from_timestamp,
                to_timestamp,
            } => {
                if from_timestamp >= to_timestamp {
                    return Err(format!(
                        "CancelSpeculation: from_timestamp ({}) >= to_timestamp ({})",
                        from_timestamp, to_timestamp
                    ));
                }
            }
            ControlMessageType::BatchHint {
                suggested_batch_size,
            } => {
                if *suggested_batch_size == 0 {
                    return Err("BatchHint: suggested_batch_size must be > 0".to_string());
                }
                if *suggested_batch_size > 100 {
                    return Err(format!(
                        "BatchHint: suggested_batch_size ({}) is unusually large (>100)",
                        suggested_batch_size
                    ));
                }
            }
            ControlMessageType::DeadlineWarning { deadline_us } => {
                if *deadline_us == 0 {
                    return Err("DeadlineWarning: deadline_us must be > 0".to_string());
                }
            }
        }

        Ok(())
    }

    /// Check if message is a cancellation
    pub fn is_cancellation(&self) -> bool {
        matches!(
            self.message_type,
            ControlMessageType::CancelSpeculation { .. }
        )
    }

    /// Check if message is a batch hint
    pub fn is_batch_hint(&self) -> bool {
        matches!(self.message_type, ControlMessageType::BatchHint { .. })
    }

    /// Check if message is a deadline warning
    pub fn is_deadline_warning(&self) -> bool {
        matches!(
            self.message_type,
            ControlMessageType::DeadlineWarning { .. }
        )
    }

    /// Serialize to bytes for IPC transfer
    ///
    /// Format: type (1 byte) = 5 | session_len (2 bytes) | session_id
    ///         | timestamp (8 bytes) | payload_len (4 bytes) | payload (JSON)
    pub fn to_bytes(&self) -> Result<Vec<u8>, String> {
        let mut bytes = Vec::new();

        // Data type = 5 (ControlMessage)
        bytes.push(5u8);

        // Session ID
        let session_bytes = self.session_id.as_bytes();
        if session_bytes.len() > u16::MAX as usize {
            return Err("Session ID too long".to_string());
        }
        bytes.extend_from_slice(&(session_bytes.len() as u16).to_le_bytes());
        bytes.extend_from_slice(session_bytes);

        // Timestamp
        bytes.extend_from_slice(&self.timestamp.to_le_bytes());

        // Payload (JSON-encoded message)
        let payload_json = serde_json::to_string(self)
            .map_err(|e| format!("Failed to serialize control message to JSON: {}", e))?;

        let payload_bytes = payload_json.as_bytes();
        if payload_bytes.len() > u32::MAX as usize {
            return Err("Payload too large".to_string());
        }
        bytes.extend_from_slice(&(payload_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(payload_bytes);

        Ok(bytes)
    }

    /// Deserialize from bytes after IPC transfer
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 15 {
            return Err("Invalid data: too short for control message".to_string());
        }

        let mut pos = 0;

        // Data type (expect 5)
        let data_type = bytes[pos];
        if data_type != 5 {
            return Err(format!(
                "Invalid data type: expected 5 (ControlMessage), got {}",
                data_type
            ));
        }
        pos += 1;

        // Session ID
        let session_len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        pos += 2;
        if pos + session_len > bytes.len() {
            return Err("Invalid session length".to_string());
        }
        let _session_id = String::from_utf8_lossy(&bytes[pos..pos + session_len]).to_string(); // TODO: Use for session validation
        pos += session_len;

        // Timestamp
        if pos + 8 > bytes.len() {
            return Err("Invalid timestamp offset".to_string());
        }
        let _timestamp = u64::from_le_bytes([
            bytes[pos],
            bytes[pos + 1],
            bytes[pos + 2],
            bytes[pos + 3],
            bytes[pos + 4],
            bytes[pos + 5],
            bytes[pos + 6],
            bytes[pos + 7],
        ]);
        pos += 8;

        // Payload length
        if pos + 4 > bytes.len() {
            return Err("Invalid payload length offset".to_string());
        }
        let payload_len =
            u32::from_le_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
                as usize;
        pos += 4;

        // Payload (JSON)
        if pos + payload_len > bytes.len() {
            return Err(format!(
                "Invalid payload: expected {} bytes, got {}",
                payload_len,
                bytes.len() - pos
            ));
        }
        let payload_json = String::from_utf8_lossy(&bytes[pos..pos + payload_len]).to_string();

        // Deserialize from JSON
        serde_json::from_str(&payload_json)
            .map_err(|e| format!("Failed to deserialize control message from JSON: {}", e))
    }
}

/// Get current timestamp in microseconds since Unix epoch
fn current_timestamp_us() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time is before Unix epoch")
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_cancel_speculation_message() {
        let segment_id = Uuid::new_v4();
        let msg = ControlMessage::cancel_speculation(
            "session_123".to_string(),
            1000000,
            1020000,
            Some(segment_id),
        );

        assert!(msg.is_cancellation());
        assert!(!msg.is_batch_hint());
        assert!(!msg.is_deadline_warning());
        assert_eq!(msg.session_id, "session_123");
        assert_eq!(msg.target_segment_id, Some(segment_id));
    }

    #[test]
    fn test_create_batch_hint_message() {
        let msg = ControlMessage::batch_hint("session_456".to_string(), 5);

        assert!(msg.is_batch_hint());
        assert!(!msg.is_cancellation());
        assert_eq!(msg.session_id, "session_456");

        match msg.message_type {
            ControlMessageType::BatchHint {
                suggested_batch_size,
            } => {
                assert_eq!(suggested_batch_size, 5);
            }
            _ => panic!("Expected BatchHint message type"),
        }
    }

    #[test]
    fn test_create_deadline_warning_message() {
        let msg = ControlMessage::deadline_warning("session_789".to_string(), 50000);

        assert!(msg.is_deadline_warning());
        assert!(!msg.is_cancellation());
        assert_eq!(msg.session_id, "session_789");
    }

    #[test]
    fn test_cancel_speculation_validation_success() {
        let msg =
            ControlMessage::cancel_speculation("session_123".to_string(), 1000000, 1020000, None);

        assert!(msg.validate().is_ok());
    }

    #[test]
    fn test_cancel_speculation_validation_fails_invalid_timestamps() {
        let msg = ControlMessage::cancel_speculation(
            "session_123".to_string(),
            2000000, // from > to (invalid)
            1000000,
            None,
        );

        let result = msg.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("from_timestamp"));
    }

    #[test]
    fn test_batch_hint_validation_fails_zero_size() {
        let mut msg = ControlMessage::batch_hint("session_123".to_string(), 0);

        let result = msg.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be > 0"));
    }

    #[test]
    fn test_batch_hint_validation_warns_large_size() {
        let msg = ControlMessage::batch_hint("session_123".to_string(), 150);

        let result = msg.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unusually large"));
    }

    #[test]
    fn test_message_serialization_roundtrip() {
        let original = ControlMessage::cancel_speculation(
            "session_test".to_string(),
            1000000,
            1020000,
            Some(Uuid::new_v4()),
        );

        // Serialize to JSON
        let json = serde_json::to_string(&original).expect("Failed to serialize");

        // Deserialize back
        let deserialized: ControlMessage =
            serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(original.session_id, deserialized.session_id);
        assert_eq!(original.message_type, deserialized.message_type);
        assert_eq!(original.target_segment_id, deserialized.target_segment_id);
    }

    #[test]
    fn test_to_bytes_cancel_speculation() {
        let msg =
            ControlMessage::cancel_speculation("sess_123".to_string(), 1000000, 1020000, None);

        let bytes = msg.to_bytes().expect("Failed to serialize");

        // Verify format
        assert_eq!(bytes[0], 5); // Type = ControlMessage

        // Verify session length
        let session_len = u16::from_le_bytes([bytes[1], bytes[2]]) as usize;
        assert_eq!(session_len, 8); // "sess_123".len()

        // Verify we have timestamp and payload
        assert!(bytes.len() > 15);
    }

    #[test]
    fn test_from_bytes_cancel_speculation() {
        let original =
            ControlMessage::cancel_speculation("sess_456".to_string(), 2000000, 2050000, None);

        // Serialize
        let bytes = original.to_bytes().expect("Failed to serialize");

        // Deserialize
        let deserialized = ControlMessage::from_bytes(&bytes).expect("Failed to deserialize");

        // Verify fields
        assert_eq!(deserialized.session_id, "sess_456");
        assert!(deserialized.is_cancellation());

        match deserialized.message_type {
            ControlMessageType::CancelSpeculation {
                from_timestamp,
                to_timestamp,
            } => {
                assert_eq!(from_timestamp, 2000000);
                assert_eq!(to_timestamp, 2050000);
            }
            _ => panic!("Expected CancelSpeculation"),
        }
    }

    #[test]
    fn test_from_bytes_batch_hint() {
        let original = ControlMessage::batch_hint("sess_789".to_string(), 5);

        let bytes = original.to_bytes().unwrap();
        let deserialized = ControlMessage::from_bytes(&bytes).unwrap();

        assert_eq!(deserialized.session_id, "sess_789");
        assert!(deserialized.is_batch_hint());

        match deserialized.message_type {
            ControlMessageType::BatchHint {
                suggested_batch_size,
            } => {
                assert_eq!(suggested_batch_size, 5);
            }
            _ => panic!("Expected BatchHint"),
        }
    }

    #[test]
    fn test_from_bytes_deadline_warning() {
        let original = ControlMessage::deadline_warning("sess_abc".to_string(), 50000);

        let bytes = original.to_bytes().unwrap();
        let deserialized = ControlMessage::from_bytes(&bytes).unwrap();

        assert_eq!(deserialized.session_id, "sess_abc");
        assert!(deserialized.is_deadline_warning());

        match deserialized.message_type {
            ControlMessageType::DeadlineWarning { deadline_us } => {
                assert_eq!(deadline_us, 50000);
            }
            _ => panic!("Expected DeadlineWarning"),
        }
    }

    #[test]
    fn test_from_bytes_invalid_type() {
        let mut bytes = vec![3, 0, 5]; // Type = 3 (not ControlMessage)
        bytes.extend_from_slice(b"sess1");
        bytes.extend_from_slice(&1234567890u64.to_le_bytes());
        bytes.extend_from_slice(&10u32.to_le_bytes());
        bytes.extend_from_slice(b"{}");

        let result = ControlMessage::from_bytes(&bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("expected 5"));
    }

    #[test]
    fn test_from_bytes_too_short() {
        let bytes = vec![5, 0, 5]; // Only 3 bytes

        let result = ControlMessage::from_bytes(&bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too short"));
    }

    #[test]
    fn test_from_bytes_invalid_json() {
        let mut bytes = vec![5]; // Type
        bytes.extend_from_slice(&5u16.to_le_bytes()); // Session len
        bytes.extend_from_slice(b"sess1"); // Session ID
        bytes.extend_from_slice(&1234567890u64.to_le_bytes()); // Timestamp
        bytes.extend_from_slice(&10u32.to_le_bytes()); // Payload len
        bytes.extend_from_slice(b"invalid!!!"); // Invalid JSON

        let result = ControlMessage::from_bytes(&bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Failed to deserialize"));
    }

    #[test]
    fn test_serialization_roundtrip_all_types() {
        let messages = vec![
            ControlMessage::cancel_speculation("s1".to_string(), 1000, 2000, None),
            ControlMessage::batch_hint("s2".to_string(), 3),
            ControlMessage::deadline_warning("s3".to_string(), 100000),
        ];

        for original in messages {
            let bytes = original.to_bytes().expect("Serialization failed");
            let deserialized = ControlMessage::from_bytes(&bytes).expect("Deserialization failed");

            assert_eq!(original.session_id, deserialized.session_id);
            assert_eq!(original.message_type, deserialized.message_type);
        }
    }
}
