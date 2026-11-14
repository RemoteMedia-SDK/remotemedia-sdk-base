//! Names on the wire: the socket's path and subprotocols, the event types
//! the transport handles itself, and the internal frames it exchanges with
//! the member's `RealtimeEventsNode`.

use serde_json::{json, Value};

/// Path of the realtime socket on the signalling listener.
pub const REALTIME_PATH: &str = "/realtime";
/// The socket's subprotocol. The server answers it in the handshake.
pub const SUBPROTOCOL: &str = "remotemedia.realtime.v1";
/// The single-use ticket travels as a second subprotocol, never in the URL.
pub const TICKET_SUBPROTOCOL_PREFIX: &str = "remotemedia.ticket.";

/// Member metadata key of the realtime admission: a JSON string
/// `{ "configurable": [...], "client_tools": [...] }`.
pub const ADMISSION_KEY: &str = "rm.realtime";

/// Participant metadata the member is joined with, so a node can tell a
/// realtime member from one on another transport.
pub const TRANSPORT_KEY: &str = "participant.transport";
/// Value of [`TRANSPORT_KEY`], of `transport` in `joined`, and the transport
/// name in `media_ready` / `media_lost`.
pub const TRANSPORT_NAME: &str = "realtime";

/// The node type whose `realtime` port the transport talks to.
pub const EVENTS_NODE_TYPE: &str = "RealtimeEventsNode";
/// The port of that node, both directions.
pub const REALTIME_PORT: &str = "realtime";

/// Audio metadata: the item id of a frame, stamped by the node.
pub const ITEM_ID_KEY: &str = "rm.item_id";

/// Frames with a type under this prefix go between the transport and the
/// node only. They are never written to the socket and never accepted from
/// it.
pub const INTERNAL_PREFIX: &str = "remotemedia.internal.";
pub const INTERNAL_RELEASED: &str = "remotemedia.internal.released";
pub const INTERNAL_FLUSH: &str = "remotemedia.internal.flush";
pub const INTERNAL_JOINED: &str = "remotemedia.internal.joined";

pub const SESSION_CREATED: &str = "session-created";
pub const SESSION_UPDATE: &str = "session-update";
pub const SESSION_UPDATED: &str = "session-updated";
pub const INPUT_AUDIO_APPEND: &str = "input-audio-append";
pub const AUDIO_DELTA: &str = "audio-delta";
pub const CUSTOM: &str = "custom";
pub const CONFIG_IGNORED: &str = "remotemedia.config-ignored";

/// The `type` of a frame, when it is a JSON object with one.
pub fn frame_type(frame: &Value) -> Option<&str> {
    frame.get("type").and_then(Value::as_str)
}

pub fn is_internal(frame_type: &str) -> bool {
    frame_type.starts_with(INTERNAL_PREFIX)
}

/// `remotemedia.internal.joined`, to the member's node.
pub fn joined(member: &str, output_rate: u32, input_rate: u32) -> Value {
    json!({
        "type": INTERNAL_JOINED,
        "member": member,
        "transport": TRANSPORT_NAME,
        "outputRate": output_rate,
        "inputRate": input_rate,
    })
}

/// `audio-delta`, to the socket.
pub fn audio_delta(response_id: &str, item_id: &str, delta: String) -> Value {
    json!({
        "type": AUDIO_DELTA,
        "responseId": response_id,
        "itemId": item_id,
        "delta": delta,
    })
}

/// The ticket among the subprotocols a client offered. A ticket anywhere
/// else (the URL, a header) is not looked at.
pub fn ticket_from_protocols(protocols: &[String]) -> Option<&str> {
    protocols
        .iter()
        .find_map(|protocol| protocol.strip_prefix(TICKET_SUBPROTOCOL_PREFIX))
        .filter(|ticket| !ticket.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_ticket_is_read_from_the_subprotocols_only() {
        let offered = vec![SUBPROTOCOL.to_string(), format!("{TICKET_SUBPROTOCOL_PREFIX}abc")];
        assert_eq!(ticket_from_protocols(&offered), Some("abc"));
        assert_eq!(ticket_from_protocols(&[SUBPROTOCOL.to_string()]), None);
        assert_eq!(
            ticket_from_protocols(&[TICKET_SUBPROTOCOL_PREFIX.to_string()]),
            None
        );
    }
}
