//! Transport-agnostic data container
//!
//! Provides `TransportData` which wraps core `RuntimeData` with optional
//! metadata for transport-specific information.

use crate::data::RuntimeData;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Canonical frame metadata keys for identifying the sender of a payload.
///
/// These keys are transport-neutral. Specific transports may keep their
/// existing metadata keys for compatibility, but should also mirror into these
/// fields when participant identity is known.
pub mod participant {
    /// Stable sender identity within the session.
    pub const ID: &str = "participant.id";
    /// Pipeline-facing role such as `user`, `client`, or `hermes_request`.
    pub const ROLE: &str = "participant.role";
    /// Logical track, stream, or call-leg identifier.
    pub const TRACK_ID: &str = "participant.track_id";
    /// Media/control modality label such as `text`, `audio`, or `video`.
    pub const MODALITY: &str = "participant.modality";
    /// Human-readable label for UI/logging only.
    pub const DISPLAY_NAME: &str = "participant.display_name";

    /// Conventional participant roles. Roles remain open strings; these values
    /// are the shared vocabulary used by SDK helpers and built-in integrations.
    pub mod role {
        pub const USER: &str = "user";
        pub const CLIENT: &str = "client";
        pub const AGENT: &str = "agent";
        pub const COPILOT: &str = "copilot";
        pub const SYSTEM: &str = "system";
        pub const TOOL: &str = "tool";
        pub const HERMES_REQUEST: &str = "hermes_request";
        pub const HERMES_RESPONSE: &str = "hermes_response";
    }

    /// Conventional modality labels.
    pub mod modality {
        pub const TEXT: &str = "text";
        pub const AUDIO: &str = "audio";
        pub const VIDEO: &str = "video";
        pub const CONTROL: &str = "control";
    }
}

/// Participant identity for a client attached to a pipeline session.
///
/// Roles are intentionally open strings: built-in transports should use the
/// constants in [`participant::role`], while applications may define their own
/// role vocabulary for pipeline-specific routing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Participant {
    /// Stable sender identity within the pipeline session.
    pub id: String,
    /// Pipeline-facing role for this sender.
    pub role: String,
    /// Logical track, stream, or call-leg identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track_id: Option<String>,
    /// Media/control modality.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modality: Option<String>,
    /// Human-readable label for UI/logging only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Additional participant-scoped metadata.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
}

impl Participant {
    /// Create a participant with required identity and role.
    pub fn new(id: impl Into<String>, role: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            role: role.into(),
            track_id: None,
            modality: None,
            display_name: None,
            metadata: HashMap::new(),
        }
    }

    /// Set a logical track, stream, or call-leg identifier.
    pub fn with_track_id(mut self, track_id: impl Into<String>) -> Self {
        self.track_id = Some(track_id.into());
        self
    }

    /// Set a media/control modality.
    pub fn with_modality(mut self, modality: impl Into<String>) -> Self {
        self.modality = Some(modality.into());
        self
    }

    /// Set a display name.
    pub fn with_display_name(mut self, display_name: impl Into<String>) -> Self {
        self.display_name = Some(display_name.into());
        self
    }

    /// Add participant-scoped metadata.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Convert this participant into canonical frame metadata.
    pub fn frame_metadata(&self) -> HashMap<String, String> {
        let mut metadata = self.metadata.clone();
        metadata.insert(participant::ID.to_string(), self.id.clone());
        metadata.insert(participant::ROLE.to_string(), self.role.clone());
        if let Some(track_id) = &self.track_id {
            metadata.insert(participant::TRACK_ID.to_string(), track_id.clone());
        }
        if let Some(modality) = &self.modality {
            metadata.insert(participant::MODALITY.to_string(), modality.clone());
        }
        if let Some(display_name) = &self.display_name {
            metadata.insert(participant::DISPLAY_NAME.to_string(), display_name.clone());
        }
        metadata
    }
}

/// Transport-agnostic data container
///
/// Wraps core RuntimeData with optional metadata for transport-specific
/// information (sequence numbers, headers, tags, etc.).
///
/// # Design
///
/// - **data**: Core payload (Audio, Text, Image, Binary) - required
/// - **sequence**: Optional sequence number for stream ordering
/// - **metadata**: Extensible key-value pairs for transport-specific info
///
/// # Examples
///
/// ```
/// use remotemedia_core::transport::TransportData;
/// use remotemedia_core::data::RuntimeData;
///
/// // Simple usage
/// let data = TransportData::new(RuntimeData::Text("hello".into()));
///
/// // With metadata
/// let data = TransportData::new(RuntimeData::Text("hello".into()))
///     .with_metadata("request_id".into(), "abc123".into());
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransportData {
    /// Core data payload (audio, text, image, binary)
    pub data: RuntimeData,

    /// Optional sequence number for ordering in streams
    ///
    /// Transports should set this for streaming sessions to maintain
    /// message order. Core may use this for metrics and debugging.
    pub sequence: Option<u64>,

    /// Transport-specific metadata (extensible key-value pairs)
    ///
    /// Examples:
    /// - gRPC: HTTP headers, request IDs, client info
    /// - FFI: Python call context, thread info
    /// - Custom: Any transport-specific info
    pub metadata: HashMap<String, String>,
}

impl TransportData {
    /// Create new TransportData with just payload (no metadata)
    ///
    /// # Arguments
    ///
    /// * `data` - Core RuntimeData payload
    ///
    /// # Examples
    ///
    /// ```
    /// use remotemedia_core::transport::TransportData;
    /// use remotemedia_core::data::RuntimeData;
    ///
    /// let data = TransportData::new(RuntimeData::Text("hello".into()));
    /// assert!(data.sequence.is_none());
    /// assert!(data.metadata.is_empty());
    /// ```
    pub fn new(data: RuntimeData) -> Self {
        let metadata = extract_metadata_from_runtime(&data).unwrap_or_default();
        Self {
            data,
            sequence: None,
            metadata,
        }
    }

    /// Builder pattern: add sequence number
    ///
    /// # Arguments
    ///
    /// * `seq` - Sequence number for ordering
    ///
    /// # Examples
    ///
    /// ```
    /// use remotemedia_core::transport::TransportData;
    /// use remotemedia_core::data::RuntimeData;
    ///
    /// let data = TransportData::new(RuntimeData::Text("hello".into()))
    ///     .with_sequence(1);
    /// assert_eq!(data.sequence, Some(1));
    /// ```
    pub fn with_sequence(mut self, seq: u64) -> Self {
        self.sequence = Some(seq);
        self
    }

    /// Builder pattern: add metadata key-value pair
    ///
    /// # Arguments
    ///
    /// * `key` - Metadata key
    /// * `value` - Metadata value
    ///
    /// # Examples
    ///
    /// ```
    /// use remotemedia_core::transport::TransportData;
    /// use remotemedia_core::data::RuntimeData;
    ///
    /// let data = TransportData::new(RuntimeData::Text("hello".into()))
    ///     .with_metadata("client_id".into(), "user123".into())
    ///     .with_metadata("request_id".into(), "req456".into());
    /// assert_eq!(data.metadata.get("client_id"), Some(&"user123".to_string()));
    /// ```
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// Attach canonical participant metadata to this frame.
    pub fn with_participant(
        mut self,
        participant_id: impl Into<String>,
        role: impl Into<String>,
    ) -> Self {
        self.metadata
            .insert(participant::ID.to_string(), participant_id.into());
        self.metadata
            .insert(participant::ROLE.to_string(), role.into());
        self
    }

    /// Attach an optional canonical participant metadata field.
    pub fn with_participant_field(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.metadata.insert(key.to_string(), value.into());
        self
    }

    /// Get metadata value by key
    ///
    /// # Arguments
    ///
    /// * `key` - Metadata key to lookup
    ///
    /// # Returns
    ///
    /// * `Some(&String)` - Value if key exists
    /// * `None` - Key not found
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }

    /// Get the canonical participant ID, if present.
    pub fn participant_id(&self) -> Option<&str> {
        self.get_metadata(participant::ID).map(String::as_str)
    }

    /// Get the canonical participant role, if present.
    pub fn participant_role(&self) -> Option<&str> {
        self.get_metadata(participant::ROLE).map(String::as_str)
    }

    /// Merge another metadata map into this frame.
    pub fn with_metadata_map(mut self, metadata: &HashMap<String, String>) -> Self {
        self.metadata.extend(metadata.clone());
        self
    }

    /// Attach a full participant descriptor to this frame.
    pub fn with_participant_descriptor(mut self, participant: &Participant) -> Self {
        for (key, value) in participant.frame_metadata() {
            self.metadata.entry(key).or_insert(value);
        }
        self
    }
}

/// Materialize transport metadata into a `RuntimeData` payload when the
/// payload shape supports metadata.
///
/// `TransportData.metadata` is the transport envelope. Nodes, however, see
/// `RuntimeData`. This bridge preserves canonical participant fields for the
/// live media paths that already carry runtime metadata, especially audio.
pub fn apply_transport_metadata_to_runtime(
    data: RuntimeData,
    metadata: &HashMap<String, String>,
) -> RuntimeData {
    if metadata.is_empty() {
        return data;
    }

    match data {
        RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            stream_id,
            timestamp_us,
            arrival_ts_us,
            metadata: runtime_metadata,
        } => RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            stream_id,
            timestamp_us,
            arrival_ts_us,
            metadata: Some(merge_runtime_metadata(runtime_metadata, metadata)),
        },
        RuntimeData::Tensor {
            data,
            shape,
            dtype,
            metadata: runtime_metadata,
        } => RuntimeData::Tensor {
            data,
            shape,
            dtype,
            metadata: Some(merge_runtime_metadata(runtime_metadata, metadata)),
        },
        RuntimeData::ControlMessage {
            message_type,
            segment_id,
            timestamp_ms,
            metadata: runtime_metadata,
        } => RuntimeData::ControlMessage {
            message_type,
            segment_id,
            timestamp_ms,
            metadata: merge_runtime_metadata(Some(runtime_metadata), metadata),
        },
        RuntimeData::Json(mut value) => {
            if let Some(obj) = value.as_object_mut() {
                for (k, v) in metadata {
                    obj.insert(k.clone(), serde_json::Value::String(v.clone()));
                }
            }
            RuntimeData::Json(value)
        }
        other => other,
    }
}

fn merge_runtime_metadata(
    existing: Option<serde_json::Value>,
    metadata: &HashMap<String, String>,
) -> serde_json::Value {
    let mut object = match existing {
        Some(serde_json::Value::Object(object)) => object,
        Some(value) => {
            let mut object = serde_json::Map::new();
            object.insert("value".to_string(), value);
            object
        }
        None => serde_json::Map::new(),
    };

    for (key, value) in metadata {
        object.insert(key.clone(), serde_json::Value::String(value.clone()));
    }

    serde_json::Value::Object(object)
}

/// Extract transport metadata from a `RuntimeData` payload if it contains metadata.
pub fn extract_metadata_from_runtime(data: &RuntimeData) -> Option<HashMap<String, String>> {
    match data {
        RuntimeData::Json(value) => {
            let obj = value.as_object()?;
            let mut map = HashMap::new();
            for (k, v) in obj {
                if k.starts_with("participant.") {
                    if let Some(s) = v.as_str() {
                        map.insert(k.clone(), s.to_string());
                    }
                }
            }
            if map.is_empty() {
                None
            } else {
                Some(map)
            }
        }
        _ => {
            let val = match data {
                RuntimeData::Audio { metadata, .. } => metadata.as_ref()?,
                RuntimeData::Tensor { metadata, .. } => metadata.as_ref()?,
                RuntimeData::ControlMessage { metadata, .. } => metadata,
                _ => return None,
            };

            let obj = val.as_object()?;
            let mut map = HashMap::new();
            for (k, v) in obj {
                if let Some(s) = v.as_str() {
                    map.insert(k.clone(), s.to_string());
                }
            }
            Some(map)
        }
    }
}

/// Convert RuntimeData directly to TransportData
impl From<RuntimeData> for TransportData {
    fn from(data: RuntimeData) -> Self {
        TransportData::new(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn participant_helpers_write_canonical_metadata() {
        let data = TransportData::new(RuntimeData::Text("hello".into()))
            .with_participant("alice", participant::role::USER)
            .with_participant_field(participant::TRACK_ID, "text:main")
            .with_participant_field(participant::MODALITY, participant::modality::TEXT);

        assert_eq!(data.participant_id(), Some("alice"));
        assert_eq!(data.participant_role(), Some("user"));
        assert_eq!(
            data.get_metadata(participant::TRACK_ID).map(String::as_str),
            Some("text:main")
        );
        assert_eq!(
            data.get_metadata(participant::MODALITY).map(String::as_str),
            Some("text")
        );
    }

    #[test]
    fn participant_descriptor_writes_canonical_metadata() {
        let participant = Participant::new("alice", participant::role::USER)
            .with_track_id("mic")
            .with_modality(participant::modality::AUDIO)
            .with_display_name("Alice");

        let data = TransportData::new(RuntimeData::Text("hello".into()))
            .with_participant_descriptor(&participant);

        assert_eq!(data.participant_id(), Some("alice"));
        assert_eq!(data.participant_role(), Some("user"));
        assert_eq!(
            data.get_metadata(participant::TRACK_ID).map(String::as_str),
            Some("mic")
        );
        assert_eq!(
            data.get_metadata(participant::DISPLAY_NAME)
                .map(String::as_str),
            Some("Alice")
        );
    }

    #[test]
    fn participant_descriptor_does_not_overwrite_frame_metadata() {
        let participant = Participant::new("alice", participant::role::USER)
            .with_track_id("connection")
            .with_modality(participant::modality::CONTROL);

        let data = TransportData::new(RuntimeData::Text("hello".into()))
            .with_metadata(participant::TRACK_ID.to_string(), "mic".to_string())
            .with_metadata(
                participant::MODALITY.to_string(),
                participant::modality::AUDIO.to_string(),
            )
            .with_participant_descriptor(&participant);

        assert_eq!(data.participant_id(), Some("alice"));
        assert_eq!(
            data.get_metadata(participant::TRACK_ID).map(String::as_str),
            Some("mic")
        );
        assert_eq!(
            data.get_metadata(participant::MODALITY).map(String::as_str),
            Some("audio")
        );
    }

    #[test]
    fn applies_transport_metadata_to_audio_runtime_metadata() {
        let data = TransportData::new(RuntimeData::Audio {
            samples: vec![0.0; 16].into(),
            sample_rate: 48_000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: Some(serde_json::json!({"existing": "kept"})),
        })
        .with_participant("alice", participant::role::USER);

        let RuntimeData::Audio { metadata, .. } =
            apply_transport_metadata_to_runtime(data.data, &data.metadata)
        else {
            panic!("expected audio");
        };

        let metadata = metadata.expect("audio metadata");
        assert_eq!(metadata["existing"], "kept");
        assert_eq!(metadata[participant::ID], "alice");
        assert_eq!(metadata[participant::ROLE], "user");
    }
}
