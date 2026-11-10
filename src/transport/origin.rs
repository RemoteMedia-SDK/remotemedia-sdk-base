//! Participant provenance carried through a session's graph (openspec:
//! add-multi-participant-sessions, capability `participant-provenance`).
//!
//! A frame entering the router through a participant handle carries the
//! sender's `participant.*` fields in its [`DataPacket`] metadata. The router
//! keeps that identity next to the payload, in a [`Routed`] item, on every
//! queue between nodes. A node's output inherits the identity of the input it
//! was processing when it emitted the output, and the identity reaches the
//! client as transport metadata on the output, for every payload type
//! (including `RuntimeData::Text`, which has no metadata of its own).
//!
//! [`DataPacket`]: super::session_router::DataPacket

use super::data::{participant, TransportData};
use crate::data::RuntimeData;
use std::collections::HashMap;
use std::sync::Arc;

/// Who produced a frame: the sender's canonical `participant.*` fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Origin {
    id: String,
    fields: HashMap<String, String>,
}

impl Origin {
    /// The identity in `metadata`, or `None` without a `participant.id`.
    /// Only `participant.*` keys are kept.
    pub fn from_metadata(metadata: &HashMap<String, String>) -> Option<Self> {
        let id = metadata.get(participant::ID)?.clone();
        let fields = metadata
            .iter()
            .filter(|(key, _)| key.starts_with("participant."))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Some(Self { id, fields })
    }

    /// `participant.id`.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// All carried `participant.*` fields.
    pub fn fields(&self) -> &HashMap<String, String> {
        &self.fields
    }
}

/// A payload on its way between nodes, with the identity of the participant
/// whose input produced it (`None` for unattributed frames, e.g. paced output
/// or input sent without a participant).
#[derive(Debug, Clone)]
pub struct Routed {
    pub data: RuntimeData,
    pub origin: Option<Arc<Origin>>,
}

impl Routed {
    pub fn new(data: RuntimeData, origin: Option<Arc<Origin>>) -> Self {
        Self { data, origin }
    }

    /// A payload with no participant identity.
    pub fn unattributed(data: RuntimeData) -> Self {
        Self { data, origin: None }
    }

    /// The client-facing output: the payload's own `participant.*` fields win
    /// (a node set them explicitly); missing ones come from the carried origin.
    pub fn into_transport(self) -> TransportData {
        let mut output = TransportData::new(self.data);
        if let Some(origin) = self.origin {
            for (key, value) in origin.fields() {
                output
                    .metadata
                    .entry(key.clone())
                    .or_insert_with(|| value.clone());
            }
        }
        output
    }
}

impl From<RuntimeData> for Routed {
    fn from(data: RuntimeData) -> Self {
        Self::unattributed(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alice() -> Arc<Origin> {
        let metadata = HashMap::from([
            (participant::ID.to_string(), "alice".to_string()),
            (participant::ROLE.to_string(), "user".to_string()),
            ("request_id".to_string(), "not a participant field".to_string()),
        ]);
        Arc::new(Origin::from_metadata(&metadata).unwrap())
    }

    #[test]
    fn origin_keeps_only_participant_fields() {
        let origin = alice();
        assert_eq!(origin.id(), "alice");
        assert_eq!(origin.fields().len(), 2);
        assert!(Origin::from_metadata(&HashMap::new()).is_none());
    }

    #[test]
    fn text_output_is_attributed_from_its_origin() {
        let out = Routed::new(RuntimeData::Text("hi".into()), Some(alice())).into_transport();
        assert_eq!(out.metadata.get(participant::ID).map(String::as_str), Some("alice"));
        assert_eq!(out.metadata.get(participant::ROLE).map(String::as_str), Some("user"));
    }

    #[test]
    fn payload_participant_fields_win() {
        let json = serde_json::json!({ "participant.id": "narrator", "text": "hi" });
        let out = Routed::new(RuntimeData::Json(json), Some(alice())).into_transport();
        assert_eq!(out.metadata.get(participant::ID).map(String::as_str), Some("narrator"));
        // Missing fields still come from the origin.
        assert_eq!(out.metadata.get(participant::ROLE).map(String::as_str), Some("user"));
    }

    #[test]
    fn unattributed_output_has_no_participant() {
        let out = Routed::unattributed(RuntimeData::Text("hi".into())).into_transport();
        assert!(out.metadata.get(participant::ID).is_none());
    }
}
