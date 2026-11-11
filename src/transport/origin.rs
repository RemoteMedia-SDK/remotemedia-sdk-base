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

use super::data::TransportData;
use crate::data::RuntimeData;
use std::collections::HashMap;
use std::sync::Arc;

/// Packet metadata key marking a participant lifecycle event; the router
/// handles such packets itself and never routes them to a node.
pub const LIFECYCLE: &str = "participant.lifecycle";
/// [`LIFECYCLE`] value: the participant in `participant.id` left the session.
pub const LEAVE: &str = "leave";
/// [`LIFECYCLE`] value: the participant in the packet joined the session. A
/// participant's first input announces it as well.
pub const JOIN: &str = "join";

pub use remotemedia_traits::emit::Origin;

/// The participants currently in a session, as the router has seen them: a
/// participant is known from its first input until it leaves. Lets a node
/// attribute emitted output to a participant by id without inventing identity
/// fields (openspec: add-node-initiated-output).
#[derive(Debug, Default)]
pub struct Roster {
    current: parking_lot::RwLock<HashMap<String, Arc<Origin>>>,
}

impl Roster {
    /// Record the sender of an input. The latest fields win. `true` when the
    /// participant was not known: it has just joined.
    pub fn note(&self, origin: &Arc<Origin>) -> bool {
        if let Some(known) = self.current.read().get(origin.id()) {
            if Arc::ptr_eq(known, origin) || **known == **origin {
                return false;
            }
        }
        self.current
            .write()
            .insert(origin.id().to_string(), Arc::clone(origin))
            .is_none()
    }

    /// The participant left the session.
    pub fn leave(&self, id: &str) {
        self.current.write().remove(id);
    }

    /// The identity of a current participant.
    pub fn get(&self, id: &str) -> Option<Arc<Origin>> {
        self.current.read().get(id).cloned()
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
    use crate::transport::data::participant;

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
