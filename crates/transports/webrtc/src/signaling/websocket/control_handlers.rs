//! `control.*` JSON-RPC methods that proxy the per-session
//! `SessionControlBus` over the same WebSocket the client already uses
//! for WebRTC signaling.
//!
//! # Wire methods
//!
//! - `control.subscribe { topic: "node.out[.port]" }` — start streaming
//!   the node's output as `control.event` notifications on this WS.
//! - `control.unsubscribe { topic }` — cancel the forwarder task.
//! - `control.publish { topic: "node.in[.port]", payload }` — inject
//!   data into a node's (possibly aux) input. `payload` carries
//!   `{ "text": "..." }`, `{ "json": {...} }`, or
//!   `{ "binary_b64": "..." }` and is coerced into `RuntimeData`.
//! - `control.set_node_state { node_id, state }` — flip a node between
//!   `enabled`, `bypass`, `disabled`. Returns `{ success: true }`.
//!
//! # Notifications
//!
//! Each subscription emits:
//!
//! ```json
//! {
//!   "jsonrpc": "2.0",
//!   "method": "control.event",
//!   "params": {
//!     "topic": "vad.out",
//!     "kind": "json" | "text" | "audio" | "binary" | "other",
//!     "payload": <type-dependent>,
//!     "ts": <unix_ms>
//!   }
//! }
//! ```
//!
//! Audio payloads are dropped from the JSON path by default — audio for
//! the browser flows over the WebRTC media track, not the control WS.
//! Text tokens and VAD JSON events are forwarded verbatim.
//!
//! # Topic grammar
//!
//! ```text
//!   <topic>     ::= <node_id> "." <direction> [ "." <port> ]
//!   <direction> ::= "in" | "out"
//! ```
//!
//! Examples:
//! - `audio.out`             — main output of node `audio`
//! - `audio.in.context`      — `context` aux port of node `audio`
//! - `vad.out`               — main output of node `vad`

use super::control_wire::{coerce_payload, encode_event};
use super::handler::SharedState;
use remotemedia_core::data::RuntimeData;
use remotemedia_core::transport::session_control::{
    ControlAddress, Direction, NodeState, SessionControl,
};
use serde_json::value::RawValue;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

fn event_or_error(
    topic: &str,
    data: &RuntimeData,
    include_audio: bool,
    copies: &remotemedia_types::CopyContext,
) -> String {
    encode_event(topic, data, include_audio, copies).unwrap_or_else(|message| {
        let mut error = crate::signaling::protocol::JsonRpcError::new(
            crate::signaling::protocol::error_codes::INTERNAL_ERROR,
            message,
            Value::Null,
        );
        error.error.data = Some(json!({"topic": topic}));
        error.to_json().expect("metadata control error")
    })
}

/// Per-connection state for in-flight control-bus subscriptions. One map
/// per WebSocket — lifetime bound to the connection.
#[derive(Default)]
pub struct ControlSessionState {
    /// Active subscription forwarder tasks, keyed by topic.
    tasks: RwLock<HashMap<String, JoinHandle<()>>>,
}

impl ControlSessionState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Abort every forwarder task this connection owns. Called on
    /// WebSocket close so we don't leak `broadcast::Receiver`s or
    /// tokio tasks after the client disappears.
    pub async fn shutdown(&self) {
        let mut tasks = self.tasks.write().await;
        for (_topic, handle) in tasks.drain() {
            handle.abort();
        }
    }
}

// ─── Topic parsing ──────────────────────────────────────────────────────

struct ParsedTopic {
    node_id: String,
    direction: Direction,
    port: Option<String>,
}

fn parse_topic(topic: &str) -> Result<ParsedTopic, String> {
    let mut parts = topic.splitn(3, '.');
    let node_id = parts
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("empty topic: {topic:?}"))?
        .to_string();
    let direction = match parts.next() {
        Some("in") => Direction::In,
        Some("out") => Direction::Out,
        Some(other) => {
            return Err(format!(
                "topic {topic:?}: expected 'in' or 'out' after node, got {other:?}"
            ))
        }
        None => return Err(format!("topic {topic:?}: missing direction")),
    };
    let port = parts.next().map(|s| s.to_string());
    Ok(ParsedTopic {
        node_id,
        direction,
        port,
    })
}

impl ParsedTopic {
    fn as_address(&self) -> ControlAddress {
        let mut addr = match self.direction {
            Direction::In => ControlAddress::node_in(self.node_id.clone()),
            Direction::Out => ControlAddress::node_out(self.node_id.clone()),
        };
        if let Some(port) = &self.port {
            addr = addr.with_port(port.clone());
        }
        addr
    }
}

// ─── SessionControl lookup ──────────────────────────────────────────────

/// Resolve the SessionControl attached to the peer currently driving this
/// WebSocket. Returns `Err(human-readable)` if the peer hasn't finished
/// its SDP exchange yet (in which case no session exists).
async fn resolve_session_control(
    state: &Arc<SharedState>,
    peer_id: &str,
) -> Result<Arc<SessionControl>, String> {
    let server_peer = {
        let peers = state.server_peers.read().await;
        peers.get(peer_id).cloned()
    };
    let server_peer = server_peer
        .ok_or_else(|| format!("peer {peer_id:?} has no ServerPeer (offer not yet negotiated)"))?;
    let session_id = server_peer
        .session_id()
        .await
        .ok_or_else(|| format!("peer {peer_id:?}: session not yet created"))?;
    state
        .runner
        .control_bus()
        .get(&session_id)
        .ok_or_else(|| format!("session {session_id} not found on control bus"))
}

// ─── Public entry point: dispatched from handler::handle_message ────────

/// Returns `Ok(Some(response_json))` if the method matched, `Ok(None)` if
/// the method is not a control.* method (caller falls through).
pub async fn handle_control_method(
    method: &str,
    params: &RawValue,
    request_id: &Value,
    state: &Arc<SharedState>,
    peer_id: &str,
    tx: &mpsc::Sender<String>,
    control_state: &Arc<ControlSessionState>,
) -> Option<Result<String, String>> {
    if method == "control.publish" {
        return Some(handle_publish(params, request_id, state, peer_id).await);
    }
    let parsed: Value = match serde_json::from_str(params.get()) {
        Ok(value) => value,
        Err(error) => return Some(Err(error.to_string())),
    };
    let params = &parsed;
    match method {
        "control.subscribe" => Some(
            handle_subscribe(params, request_id, state, peer_id, tx, control_state)
                .await
                .map_err(|e| e.to_string()),
        ),
        "control.unsubscribe" => Some(
            handle_unsubscribe(params, request_id, control_state)
                .await
                .map_err(|e| e.to_string()),
        ),
        "control.set_node_state" => Some(
            handle_set_node_state(params, request_id, state, peer_id)
                .await
                .map_err(|e| e.to_string()),
        ),
        "control.flush_audio" => Some(
            handle_flush_audio(request_id, state, peer_id)
                .await
                .map_err(|e| e.to_string()),
        ),
        _ => None,
    }
}

async fn handle_flush_audio(
    request_id: &Value,
    state: &Arc<SharedState>,
    peer_id: &str,
) -> Result<String, String> {
    // Bypasses the SessionControl entirely — this one reaches into
    // the ServerPeer to drain its audio track ring buffer. Used by
    // the client on barge-in so the assistant shuts up *now* instead
    // of after the ~10 s of already-enqueued TTS finishes playing.
    let server_peer = {
        let peers = state.server_peers.read().await;
        peers.get(peer_id).cloned()
    };
    let server_peer = server_peer
        .ok_or_else(|| format!("peer {peer_id:?} has no ServerPeer (offer not yet negotiated)"))?;
    let dropped = server_peer.flush_audio_tracks().await;
    Ok(json!({
        "jsonrpc": "2.0",
        "result": { "flushed": true, "frames_dropped": dropped },
        "id": request_id,
    })
    .to_string())
}

// ─── Individual methods ─────────────────────────────────────────────────

async fn handle_subscribe(
    params: &Value,
    request_id: &Value,
    state: &Arc<SharedState>,
    peer_id: &str,
    tx: &mpsc::Sender<String>,
    control_state: &Arc<ControlSessionState>,
) -> Result<String, String> {
    let topic = params
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "control.subscribe: missing 'topic' string param".to_string())?
        .to_string();

    let parsed = parse_topic(&topic)?;
    if parsed.direction != Direction::Out {
        return Err(format!(
            "control.subscribe: topic {topic:?} must be an .out address"
        ));
    }
    let include_audio = params
        .get("include_audio")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let ctrl = resolve_session_control(state, peer_id).await?;
    let addr = parsed.as_address();
    // `__system__.out` is the lifecycle topic — late subscribers (the
    // SPA connecting after the warm-session prewarm already published
    // `loading_node` / `node_ready` / `tools_available` etc.) need the
    // retained-buffer replay or they see a permanently blank loading
    // pane / tool selector. Route through `subscribe_system()` which
    // yields buffered events in publication order before transitioning
    // to the live stream. Plain `subscribe(addr)` is live-only and is
    // the right call for every other topic.
    let is_system = addr.node_id == "__system__" && addr.port.is_none();

    let tx_out = tx.clone();
    let topic_for_task = topic.clone();
    let copies = ctrl.transport_copy_context();
    let handle = if is_system {
        let mut rx = ctrl.subscribe_system();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Some(data) => {
                        let msg = event_or_error(&topic_for_task, &data, include_audio, &copies);
                        if tx_out.send(msg).await.is_err() {
                            debug!("control.event forwarder: ws tx closed for {topic_for_task:?}");
                            break;
                        }
                    }
                    None => {
                        debug!("control.event forwarder: system tap closed for {topic_for_task:?}");
                        break;
                    }
                }
            }
        })
    } else {
        let mut rx = ctrl.subscribe(&addr).map_err(|e| e.to_string())?;
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(data) => {
                        let msg = event_or_error(&topic_for_task, &data, include_audio, &copies);
                        if tx_out.send(msg).await.is_err() {
                            debug!("control.event forwarder: ws tx closed for {topic_for_task:?}");
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        warn!("control.event forwarder for {topic_for_task:?}: lagged {n} frames");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        debug!("control.event forwarder: tap closed for {topic_for_task:?}");
                        break;
                    }
                }
            }
        })
    };

    // Replace any prior subscription on the same topic (idempotent).
    {
        let mut tasks = control_state.tasks.write().await;
        if let Some(old) = tasks.insert(topic.clone(), handle) {
            old.abort();
        }
    }

    Ok(json!({
        "jsonrpc": "2.0",
        "result": { "subscribed": true, "topic": topic },
        "id": request_id,
    })
    .to_string())
}

async fn handle_unsubscribe(
    params: &Value,
    request_id: &Value,
    control_state: &Arc<ControlSessionState>,
) -> Result<String, String> {
    let topic = params
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "control.unsubscribe: missing 'topic' string param".to_string())?
        .to_string();

    let mut tasks = control_state.tasks.write().await;
    let found = tasks.remove(&topic).is_some();
    if let Some(handle) = tasks.remove(&topic) {
        handle.abort();
    }
    Ok(json!({
        "jsonrpc": "2.0",
        "result": { "unsubscribed": found, "topic": topic },
        "id": request_id,
    })
    .to_string())
}

async fn handle_publish(
    params: &RawValue,
    request_id: &Value,
    state: &Arc<SharedState>,
    peer_id: &str,
) -> Result<String, String> {
    #[derive(serde::Deserialize)]
    struct Params<'a> {
        topic: String,
        #[serde(borrow)]
        payload: &'a RawValue,
    }
    let params: Params<'_> = serde_json::from_str(params.get()).map_err(|e| e.to_string())?;
    let topic = &params.topic;
    let parsed = parse_topic(topic)?;
    if parsed.direction != Direction::In {
        return Err(format!(
            "control.publish: topic {topic:?} must be an .in address"
        ));
    }
    let ctrl = resolve_session_control(state, peer_id).await?;
    let data = coerce_payload(params.payload, &ctrl.transport_copy_context())?;
    ctrl.publish(&parsed.as_address(), data)
        .await
        .map_err(|e| e.to_string())?;
    Ok(
        json!({"jsonrpc":"2.0", "result":{"published":true,"topic":topic}, "id":request_id})
            .to_string(),
    )
}

async fn handle_set_node_state(
    params: &Value,
    request_id: &Value,
    state: &Arc<SharedState>,
    peer_id: &str,
) -> Result<String, String> {
    let node_id = params
        .get("node_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "control.set_node_state: missing 'node_id' param".to_string())?
        .to_string();
    let new_state = match params.get("state").and_then(|v| v.as_str()) {
        Some("enabled") | Some("Enabled") => NodeState::Enabled,
        Some("bypass") | Some("Bypass") => NodeState::Bypass,
        Some("disabled") | Some("Disabled") => NodeState::Disabled,
        Some(other) => {
            return Err(format!(
                "control.set_node_state: unknown state {other:?}; expected enabled|bypass|disabled"
            ))
        }
        None => return Err("control.set_node_state: missing 'state' param".to_string()),
    };

    let ctrl = resolve_session_control(state, peer_id).await?;
    ctrl.set_node_state(node_id.clone(), new_state);
    Ok(json!({
        "jsonrpc": "2.0",
        "result": { "success": true, "node_id": node_id, "state": format!("{:?}", new_state) },
        "id": request_id,
    })
    .to_string())
}
