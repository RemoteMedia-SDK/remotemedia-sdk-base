//! Session Control Bus — client-side pub/sub/intercept over a live session.
//!
//! # Goal
//!
//! Let a client (Python SDK, browser, operator UI) observe, inject, and edit
//! data flowing through any node of a running session without redefining the
//! pipeline manifest. The session is the first-class address; nodes are named
//! endpoints within it; ports are an optional qualifier defaulting to `main`.
//!
//! # Address model
//!
//! ```text
//! session_id / node_id [ . port ] / direction
//! ─────┬────── ──┬──── ───┬──── ────┬─────
//!   primary   topic     default   "in" = inject   (client -> node input)
//!   key       segment   "main"    "out" = tap     (node output -> client)
//! ```
//!
//! The control bus is **per-session** — a client "attaches to session X" and
//! from there addresses `{node_id}[.port]` for any operation. Ports are not
//! modeled in this prototype beyond an optional string; extending them to
//! structured capabilities is a later step (see spec 023/025).
//!
//! # Topology
//!
//! ```text
//!                  ┌────────────────────────────┐
//!                  │        SessionRouter       │
//!                  │                            │
//!   client ──publish──> input_tx (to_node = X) ─┤  [inject path]
//!                  │           │                │
//!                  │           ▼                │
//!                  │       node X runs          │
//!                  │           │                │
//!                  │           ▼                │
//!                  │     outputs ───────────────┤  [tap path]
//!                  │           │   ├─> broadcast to subscribers
//!                  │           │   └─> (if intercepted) oneshot-to-client,
//!                  │           │       awaits reply with deadline
//!                  │           ▼                │
//!                  │     downstream nodes       │
//!                  └────────────────────────────┘
//! ```
//!
//! # What this prototype provides
//!
//! - [`SessionControlBus`]       — process-wide registry, keyed by session_id.
//! - [`SessionControl`]          — per-session handle holding subscribers,
//!                                 injectors, and intercept hooks.
//! - [`ControlFrame`]            — wire frame (Subscribe/Publish/Intercept/Reply).
//! - [`ControlAddress`]          — `{node_id, port, direction}` tuple.
//! - A single integration hook for [`SessionRouter`]:
//!     * `SessionRouter::attach_control(&SessionControl)` — called at session
//!       creation; gives the router a handle it consults in `process_input`
//!       to broadcast outputs and apply intercepts.
//!
//! # What this prototype does NOT do (yet)
//!
//! - Per-node auxiliary *port schemas* / capability typing. Ports here are
//!   free-form strings; nodes choose how to interpret auxiliary publishes
//!   (e.g. `llm.in.context` is just metadata on the `DataPacket` that the
//!   node's `process_streaming` inspects). Typed ports are a follow-up.
//! - Authorization. Attach frames are trusted in this prototype; a real
//!   deployment must gate `attach(session_id)` on ownership.
//!   Noted as [`ControlAuth`] placeholder below.
//! - Transport framing. `ControlFrame` is the logical message; gRPC /
//!   WebSocket wrapping lives in the transport crates.

use crate::data::RuntimeData;
use crate::transport::session_router::DataPacket;
use crate::Result;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, RwLock};

// Capacity of the per-(node,port) output broadcast channel.
// Slow subscribers get `RecvError::Lagged` and skip ahead — never block the
// hot path. Sized for bursty multi-output nodes (e.g. LFM2-Audio emitting
// 20+ text tokens + 60+ audio-liveness envelopes per turn). 16 was enough
// for 20ms mic frames but truncated every model reply.
const DEFAULT_TAP_CAPACITY: usize = 1024;

/// Default cap for `SessionControl::system_retained`. Override per-session
/// via [`SessionControl::set_system_retained_cap`].
pub const DEFAULT_SYSTEM_RETAINED_CAP: usize = 256;

// Deadline a node's output will wait on an intercept reply before the
// original frame is passed through unchanged (and a warning is logged).
// Prevents a disconnected client from stalling the pipeline.
const DEFAULT_INTERCEPT_DEADLINE: Duration = Duration::from_millis(50);

// Capacity of each media_clock broadcast channel. Sized for ~1 second
// of bursty audio frames (50 Hz Opus = 50 events/sec) so a momentarily
// stalled subscriber gets `Lagged` rather than the publisher dropping
// silently. Wire pacers re-seed from the source's next event after a
// Lagged anyway.
const MEDIA_CLOCK_CAPACITY: usize = 64;

// ────────────────────────────────────────────────────────────────────────────
// Addressing
// ────────────────────────────────────────────────────────────────────────────

/// A point within a session the client wants to talk to.
#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlAddress {
    /// Node id within the session's manifest.
    pub node_id: String,
    /// Optional named port. `None` means the node's `main` port.
    pub port: Option<String>,
    /// Which side of the node: input-facing or output-facing.
    pub direction: Direction,
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    /// `publish` target — client -> node input (auxiliary input if port is set).
    In,
    /// `subscribe` / `intercept` target — node output -> client.
    Out,
}

impl ControlAddress {
    pub fn node_in(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            port: None,
            direction: Direction::In,
        }
    }
    pub fn node_out(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            port: None,
            direction: Direction::Out,
        }
    }
    pub fn with_port(mut self, port: impl Into<String>) -> Self {
        self.port = Some(port.into());
        self
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Wire frames
// ────────────────────────────────────────────────────────────────────────────

/// Logical control-plane frame. Transport crates wrap this in gRPC/WS framing.
#[derive(Clone, Debug)]
pub enum ControlFrame {
    /// Start observing outputs of a node/port.
    Subscribe(ControlAddress),
    /// Stop observing.
    Unsubscribe(ControlAddress),
    /// Push data into a node's input (main or auxiliary port).
    Publish {
        addr: ControlAddress,
        data: RuntimeData,
    },
    /// Splice a hook between a node's output and its downstream fanout.
    /// The client is expected to respond with `InterceptReply` keyed by
    /// `correlation_id` within the deadline.
    Intercept {
        addr: ControlAddress,
        deadline: Option<Duration>,
    },
    /// Detach an intercept.
    RemoveIntercept(ControlAddress),
    /// Client response to an `InterceptRequest` event.
    InterceptReply {
        correlation_id: u64,
        decision: InterceptDecision,
    },
    /// Flip a node's runtime execution state (Enabled / Bypass / Disabled).
    /// Applies on the next packet the router processes.
    SetNodeState { node_id: String, state: NodeState },
    /// Reset a node to the default `Enabled` state.
    ClearNodeState { node_id: String },
}

#[derive(Clone, Debug)]
pub enum InterceptDecision {
    /// Forward this data unchanged to downstream.
    Pass,
    /// Replace the frame with this data before forwarding.
    Replace(RuntimeData),
    /// Drop the frame entirely — downstream nodes don't see it.
    Drop,
}

/// Events the bus emits toward a connected client.
#[derive(Clone, Debug)]
pub enum ControlEvent {
    /// A tapped output. Addr is the origin (`node_id`, `port`, `Out`).
    Tap {
        addr: ControlAddress,
        data: RuntimeData,
    },
    /// An intercept asking for a decision; reply with `InterceptReply`.
    InterceptRequest {
        addr: ControlAddress,
        correlation_id: u64,
        data: RuntimeData,
    },
    /// Out-of-band error (e.g., publish rejected, deadline exceeded).
    Error {
        addr: Option<ControlAddress>,
        message: String,
    },
}

// ────────────────────────────────────────────────────────────────────────────
// Per-session control state
// ────────────────────────────────────────────────────────────────────────────

/// Placeholder — real deployments will gate attach on this.
#[derive(Clone, Debug, Default)]
pub struct ControlAuth {
    pub principal: Option<String>,
}

type TapKey = (String, Option<String>); // (node_id, port)
type InterceptKey = (String, Option<String>);

/// One active interception. The router awaits `reply_rx` for at most `deadline`.
struct InterceptSlot {
    events_tx: mpsc::Sender<ControlEvent>,
    deadline: Duration,
    next_correlation: std::sync::atomic::AtomicU64,
    /// Map correlation_id -> reply oneshot. Bounded by pipeline latency; we
    /// clear entries on deadline expiry.
    pending: DashMap<u64, oneshot::Sender<InterceptDecision>>,
}

/// Reason a session closed — propagated to every attached control client
/// via [`SessionControl::close_subscriber`].
#[derive(Clone, Debug)]
pub enum CloseReason {
    Normal,
    Error(String),
}

/// Envelope field name for aux-port publishes. Nodes inspect the payload
/// for this key to distinguish aux-channel input from the main input.
pub const AUX_PORT_ENVELOPE_KEY: &str = "__aux_port__";

/// Reserved aux port name for "the user has barged in / cancel the
/// in-flight call". Handled universally by the router (see
/// `session_router::spawn_node_pipeline`) — nodes never see a frame
/// on this port; the runtime aborts whatever they're currently doing
/// and drops the envelope.
pub const BARGE_IN_PORT: &str = "barge_in";

/// Reserved tap channel name for periodic per-node performance
/// snapshots (`PerfSnapshot`). The session router's perf aggregator
/// publishes here on a 1 Hz timer when `REMOTEMEDIA_PERF_TAP=1`.
/// Frontends subscribe to `__perf__.out` to render a HUD without
/// changing any node code. Distinct from `__system__` (loading
/// progress) so consumers can opt in/out independently.
pub const PERF_PORT: &str = "__perf__";

/// Reserved tap channel name for ad-hoc pipeline timeline events.
/// Different from `__system__` (lifecycle) and `__perf__` (rolling
/// metrics): this port carries one-shot debug markers like
/// `tool_call`, `motion_intent`, `motion_visible` that the UI strings
/// together into a per-turn timeline. Use [`publish_timeline_event`].
pub const TIMELINE_PORT: &str = "__timeline__";

/// Publish a one-shot debug marker on `__timeline__.out` so the UI can
/// thread together a wall-clock timeline of pipeline events for the
/// current turn (tool calls, motion-intent dispatch, animation start,
/// etc). Silent no-op when no global control bus / session is
/// registered (e.g. unit tests, gRPC transport).
///
/// Schema of the JSON payload:
/// ```json
/// {
///   "kind":   "timeline_event",
///   "label":  "tool_call",            // canonical, machine-readable
///   "text":   "perform_motion",       // optional human-readable detail
///   "source": "llm",                  // node id that emitted the marker
///   "ts_ms":  1715200000000           // unix epoch ms
/// }
/// ```
pub fn publish_timeline_event(session_id: &str, label: &str, text: Option<&str>, source: &str) {
    let Some(bus) = global_bus() else { return };
    let Some(ctrl) = bus.get(session_id) else {
        return;
    };
    let ts_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let envelope = RuntimeData::Json(serde_json::json!({
        "kind":   "timeline_event",
        "label":  label,
        "text":   text,
        "source": source,
        "ts_ms":  ts_ms,
    }));
    ctrl.publish_tap(TIMELINE_PORT, None, envelope);
}

/// If `data` is an aux-port envelope produced by [`wrap_aux_port`],
/// return the port name. Otherwise return `None`.
///
/// Recognises envelopes in two on-the-wire shapes:
///   1. `RuntimeData::Json({"__aux_port__": "...", "payload": ...})`
///      — the canonical form produced by Rust nodes via [`wrap_aux_port`].
///   2. `RuntimeData::Text("[channel-tagged]{...JSON...}")` whose decoded
///      JSON contains an `__aux_port__` key — produced by the Python
///      multiprocess runner's [`_make_reply_frame`] (typed RPC replies),
///      where the IPC layer ships JSON envelopes as channel-tagged text
///      rather than promoting to `Json` (only the `"json"` channel-tag
///      is auto-promoted today; `"rpc"` and other tags stay as Text).
///      Recognising both forms is what lets typed-RPC reply frames
///      (`__reply__`) route to the right tap regardless of whether
///      the producing node was spawned in-host multiprocess or as a
///      loadable cdylib plugin — see [`Self::on_node_output`].
///
/// Used by the router to filter universal control frames (currently
/// `barge_in`) before they reach `process_*` and to decide whether a
/// frame is aux-channel or main-channel for routing purposes.
///
/// Returns `Option<String>` (not `Option<&str>`) because the Text path
/// has to parse JSON to extract the port and the returned slice would
/// outlive the parsed value otherwise. The Json path still avoids the
/// allocation when no aux-port marker is present.
pub fn aux_port_of(data: &RuntimeData) -> Option<String> {
    match data {
        RuntimeData::Json(v) => v
            .get(AUX_PORT_ENVELOPE_KEY)
            .and_then(|p| p.as_str())
            .map(str::to_owned),
        RuntimeData::Text(s) => {
            // Strip the optional `[0x00][len][channel]` channel tag
            // before attempting to parse — Python wraps reply frames as
            // `RuntimeData.text(json_str, channel="rpc")` so the wire
            // bytes are `[0x00][3]rpc{...}`. Untagged text passes
            // through `split_text_str` unchanged.
            let body = crate::data::text_channel::split_text_str(s).1;
            let trimmed = body.trim_start();
            // Cheap early-exit so the common case (non-JSON text like
            // transcripts, tts output) skips the parser entirely.
            if !trimmed.starts_with('{') {
                return None;
            }
            serde_json::from_str::<serde_json::Value>(trimmed)
                .ok()
                .and_then(|v| {
                    v.get(AUX_PORT_ENVELOPE_KEY)
                        .and_then(|p| p.as_str())
                        .map(str::to_owned)
                })
        }
        _ => None,
    }
}

/// Wrap a `RuntimeData` payload in the aux-port envelope described in
/// [`SessionControl::publish`]. Pulled out so `python_streaming_node.rs`
/// and test code can construct the same shape.
pub fn wrap_aux_port(port: &str, data: RuntimeData) -> RuntimeData {
    let inner = match data {
        RuntimeData::Json(v) => v,
        RuntimeData::Text(t) => serde_json::json!({ "text": t }),
        RuntimeData::Binary(b) => {
            use base64::Engine as _;
            serde_json::json!({
                "binary_b64": base64::engine::general_purpose::STANDARD.encode(&b),
            })
        }
        // For richer types (Audio, Video, Tensor, Numpy), the envelope
        // carries a type tag; the node is expected to consult the
        // original payload via another code path if needed. For the
        // common aux-port use cases (text context, JSON facts, tool
        // results) the cases above cover the traffic.
        other => serde_json::json!({
            "_unsupported_for_envelope": format!("{:?}", std::mem::discriminant(&other)),
        }),
    };
    RuntimeData::Json(serde_json::json!({
        AUX_PORT_ENVELOPE_KEY: port,
        "payload": inner,
    }))
}

/// Runtime execution state for a single node, controlled via the bus.
///
/// - `Enabled` (default): node runs normally.
/// - `Bypass`: node is skipped; its inputs are forwarded as its outputs.
///   Downstream nodes see the same data the bypassed node would have
///   received — as if the node were a passthrough.
/// - `Disabled`: node is skipped and produces no outputs. Downstream
///   nodes see nothing from this branch for the current input. Useful
///   when you want to temporarily sever a subgraph without modifying
///   the manifest.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Enabled,
    Bypass,
    Disabled,
}

impl Default for NodeState {
    fn default() -> Self {
        NodeState::Enabled
    }
}

/// Per-session control state. Created alongside the `SessionRouter` and
/// handed to the router via `attach_control`.
pub struct SessionControl {
    session_id: String,
    /// Broadcasts a node output to every subscriber of (node_id, port).
    /// `broadcast::Sender` is cheap to clone; slow receivers lag, not block.
    taps: DashMap<TapKey, broadcast::Sender<RuntimeData>>,
    /// Active intercepts keyed by (node_id, port). Only one intercept per
    /// output port in v1 — stacking is possible but adds ordering ambiguity.
    intercepts: DashMap<InterceptKey, Arc<InterceptSlot>>,
    /// Handle to the router's input channel so `publish` can inject packets.
    /// Set by `attach_input_sender` after the router is constructed.
    input_tx: RwLock<Option<mpsc::Sender<DataPacket>>>,
    /// Monotonic sequence for injected packets (kept separate from the
    /// transport's own sequence to make out-of-band traffic identifiable).
    inject_seq: std::sync::atomic::AtomicU64,
    /// Broadcasts session-close to every attached client. Sent once by the
    /// router when its run-loop exits. Unbounded lag is fine — the payload
    /// is one terminal message.
    close_tx: broadcast::Sender<CloseReason>,
    /// Per-node execution state, driven by the bus. Absent entry = Enabled.
    /// Read on the router hot path once per node per packet; lock-free via
    /// DashMap to avoid an async RwLock on every output.
    node_states: DashMap<String, NodeState>,
    /// Optional fire-and-forget channel installed by the transport layer
    /// (e.g. WebRTC `ServerPeer`) so Rust nodes can request a drain of
    /// the transport's outbound audio buffer without depending on the
    /// transport crate directly. The transport sets the hook once at
    /// session setup; nodes call `request_flush_audio()` to send a
    /// ping. Absent on transports that don't have a flushable audio
    /// queue (gRPC, etc.) — callers should treat its absence as a
    /// no-op.
    flush_audio_tx: RwLock<Option<mpsc::Sender<()>>>,
    /// Capability transient channel, addressed by `<medium>:<stream_id>`
    /// (e.g. `"video:default"`, `"audio:default"`).
    ///
    /// **Retain-latest semantics**: the most recently published value per
    /// address is stored in the `DashMap`, so subscribers that register
    /// after the publish still observe the current value via
    /// [`Self::current_capability`]. New publishes go to all live
    /// subscribers via the per-address `broadcast::Sender`.
    ///
    /// Spec: `node-runtime-context` capability transient channel. Producers
    /// are transports (e.g. WebRTC `ServerPeer` post-SDP); consumers are
    /// nodes whose `on_capability_update` is invoked by the session router
    /// when an update lands on an address the node is subscribed to.
    capabilities: DashMap<String, CapabilityEntry>,

    /// Media-clock transient channel, addressed by `<medium>:<stream_id>`
    /// (e.g. `"audio:default"`, `"video:default"`).
    ///
    /// Producers — typically the WebRTC `AudioSender` after each Opus
    /// frame and `VideoTrack::send_video_runtime_data` after each
    /// encoded frame — publish a [`MediaClock`] event carrying the
    /// sender-side `pts_us`, monotonic `frame_idx`, and
    /// `wire_deadline_us` (when the frame must be queued by to hit RTP
    /// timing). Consumers are wire-bound `WireTickSource`s in
    /// [`crate::transport::pacer`] driving `ClockedToOutboundMedia`
    /// nodes.
    ///
    /// Unlike capabilities (retain-latest), media_clock is a fire-and-
    /// forget broadcast: each tick is a discrete event, not a state
    /// snapshot. Late subscribers don't replay missed ticks. Slow
    /// subscribers see `Lagged` and skip; the wire pacer exits
    /// cleanly when the broadcast closes via [`Self::stop_media_clock`].
    media_clocks: DashMap<String, broadcast::Sender<MediaClock>>,

    /// Retained ring buffer of `__system__` topic events.
    ///
    /// `publish_tap("__system__", None, ...)` appends here in addition to
    /// broadcasting. `subscribe_system` returns a receiver primed with a
    /// snapshot of the current buffer (so peers connecting after warm-pool
    /// prewarm completes still see the per-node "loading…" UX).
    ///
    /// Cap is configurable via [`Self::set_system_retained_cap`]. Default
    /// 256. Setting cap to 0 disables retention entirely.
    ///
    /// Phase 7 of the warm-session-pool feature.
    system_retained: std::sync::Mutex<std::collections::VecDeque<RuntimeData>>,

    /// Cap for `system_retained`. Atomic so it can be tuned at runtime
    /// from any thread without locking the buffer.
    system_retained_cap: std::sync::atomic::AtomicUsize,
}

/// Per-frame clock event published by an outbound RTP sender.
///
/// Consumers (`WireTickSource`) translate this into a [`crate::nodes::Tick`]:
/// `clock_id = "<medium>:<stream_id>"`, `pts_us`, `frame_idx`,
/// `deadline_us = wire_deadline_us`.
///
/// `wire_deadline_us` is the absolute wall-clock microsecond at which
/// the next RTP packet must be queued to hit the encoder's frame
/// budget. For a 30 fps video stream that's `pts_us + 33_333`; for a
/// 50 Hz Opus audio stream it's `pts_us + 20_000`. Pacer uses this to
/// detect overruns and apply the on-miss policy.
#[derive(Debug, Clone)]
pub struct MediaClock {
    /// Medium name. Conventionally `"audio"` or `"video"` so the
    /// address matches the capability bus convention.
    pub medium: String,
    /// Logical stream identifier on the producer (typically `"default"`
    /// in single-track sessions; named tracks for multi-stream
    /// scenarios).
    pub stream_id: String,
    /// Sender-side presentation timestamp in microseconds, originating
    /// from the producer's wall-clock or RTP-clock-derived monotonic
    /// origin.
    pub pts_us: u64,
    /// Monotonic per-stream frame index. Increments by 1 per published
    /// event. Wraps at `u64::MAX` (not a real concern at audio/video
    /// rates).
    pub frame_idx: u64,
    /// Absolute deadline (microseconds, same axis as `pts_us`) by which
    /// the consumer must produce its next frame. Pacer measures
    /// elapsed time against this when applying on-miss policy.
    pub wire_deadline_us: u64,
}

/// One capability address's retain-latest entry. `value` is the most
/// recently published `MediaCapabilities`; `tx` fans out to live
/// subscribers and is created lazily on first subscribe-or-publish.
struct CapabilityEntry {
    value: parking_lot::RwLock<Option<crate::capabilities::MediaCapabilities>>,
    tx: broadcast::Sender<crate::capabilities::MediaCapabilities>,
}

impl CapabilityEntry {
    fn new() -> Self {
        let (tx, _) = broadcast::channel(8);
        Self {
            value: parking_lot::RwLock::new(None),
            tx,
        }
    }
}

/// Receiver returned by [`SessionControl::subscribe_system`].
///
/// Yields any retained `__system__` events in publication order, then
/// transitions to the live broadcast stream. After all retained events
/// are yielded, behaves identically to a `tokio::sync::broadcast::Receiver`.
pub struct SystemEventReceiver {
    buffered: std::collections::VecDeque<RuntimeData>,
    live: tokio::sync::broadcast::Receiver<RuntimeData>,
}

impl SystemEventReceiver {
    /// Receive the next event. Returns `None` when the underlying
    /// broadcast channel is closed AND the buffered queue is drained.
    /// `Lagged` errors from the live channel are converted to `None`
    /// — slow subscribers should re-subscribe if they need fresh state.
    pub async fn recv(&mut self) -> Option<RuntimeData> {
        if let Some(ev) = self.buffered.pop_front() {
            return Some(ev);
        }
        match self.live.recv().await {
            Ok(ev) => Some(ev),
            Err(tokio::sync::broadcast::error::RecvError::Closed) => None,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => None,
        }
    }
}

impl SessionControl {
    pub fn new(session_id: impl Into<String>) -> Arc<Self> {
        let (close_tx, _) = broadcast::channel(1);

        // Honor the env-var opt-out for tests that want strict live-only semantics.
        let retained_cap = if std::env::var_os("RUNTIME_DISABLE_SYSTEM_REPLAY").is_some() {
            0
        } else {
            DEFAULT_SYSTEM_RETAINED_CAP
        };

        Arc::new(Self {
            session_id: session_id.into(),
            taps: DashMap::new(),
            intercepts: DashMap::new(),
            input_tx: RwLock::new(None),
            inject_seq: std::sync::atomic::AtomicU64::new(0),
            close_tx,
            node_states: DashMap::new(),
            flush_audio_tx: RwLock::new(None),
            capabilities: DashMap::new(),
            media_clocks: DashMap::new(),
            system_retained: std::sync::Mutex::new(std::collections::VecDeque::with_capacity(
                retained_cap,
            )),
            system_retained_cap: std::sync::atomic::AtomicUsize::new(retained_cap),
        })
    }

    /// Install the flush-audio hook. Called by the transport layer at
    /// session setup; later calls overwrite the previous hook (last-
    /// writer-wins, rare in practice since each session has one
    /// transport).
    pub async fn install_flush_audio_hook(&self, tx: mpsc::Sender<()>) {
        *self.flush_audio_tx.write().await = Some(tx);
    }

    /// Request an audio-buffer flush on the transport attached to this
    /// session. Fire-and-forget: returns `true` if a hook was installed
    /// and the ping was accepted (or the channel is just slow),
    /// `false` if no transport registered a hook. Never blocks.
    pub async fn request_flush_audio(&self) -> bool {
        let tx = self.flush_audio_tx.read().await.clone();
        match tx {
            Some(tx) => tx.try_send(()).is_ok(),
            None => false,
        }
    }

    /// Set a node's runtime execution state.
    ///
    /// Takes effect on the **next** packet the router processes. Callers
    /// that need a happens-before guarantee on an in-flight packet should
    /// coordinate with the data plane separately — this is a best-effort
    /// toggle, not a barrier.
    pub fn set_node_state(&self, node_id: impl Into<String>, state: NodeState) {
        self.node_states.insert(node_id.into(), state);
    }

    /// Reset a node to the default `Enabled` state (removes the override).
    pub fn clear_node_state(&self, node_id: &str) {
        self.node_states.remove(node_id);
    }

    /// Current state for a node. Nodes without an explicit override are
    /// `Enabled`. Called on the router hot path — DashMap lookup is
    /// lock-free.
    pub fn node_state(&self, node_id: &str) -> NodeState {
        self.node_states
            .get(node_id)
            .map(|e| *e.value())
            .unwrap_or(NodeState::Enabled)
    }

    /// Per-attach handle that fires once when the session closes.
    /// Every attach subscribes; dropping the receiver is harmless.
    pub fn close_subscriber(&self) -> broadcast::Receiver<CloseReason> {
        self.close_tx.subscribe()
    }

    /// Called once from `SessionRouter` just before its run-loop returns.
    /// Signals every attach to drain and exit. Idempotent.
    pub fn signal_close(&self, reason: CloseReason) {
        let _ = self.close_tx.send(reason);
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Wire the router's input channel so `publish` can reach nodes.
    /// Called once from `SessionRouter::attach_control`.
    pub async fn attach_input_sender(&self, tx: mpsc::Sender<DataPacket>) {
        *self.input_tx.write().await = Some(tx);
    }

    /// Clone the router input sender for transport-level participant joins.
    pub async fn input_sender(&self) -> Option<mpsc::Sender<DataPacket>> {
        self.input_tx.read().await.clone()
    }

    // ─── Client-facing API (driven by `handle_frame`) ──────────────────────

    /// Subscribe to a node's output. Returns a broadcast receiver the
    /// transport layer forwards to the client as `ControlEvent::Tap`.
    pub fn subscribe(&self, addr: &ControlAddress) -> Result<broadcast::Receiver<RuntimeData>> {
        if addr.direction != Direction::Out {
            return Err(crate::Error::Execution(
                "subscribe requires Direction::Out".into(),
            ));
        }
        self._subscribe(addr)
    }

    /// Subscribe to a node's input — bypasses the direction check of
    /// :method:`subscribe`.
    ///
    /// Used for control-plane communication where an external process must
    /// read from a ``.in`` address that the publisher writes to.
    pub fn subscribe_in(&self, addr: &ControlAddress) -> Result<broadcast::Receiver<RuntimeData>> {
        if addr.direction != Direction::In {
            return Err(crate::Error::Execution(
                "subscribe_in requires Direction::In (use subscribe for Direction::Out)".into(),
            ));
        }
        self._subscribe(addr)
    }

    fn _subscribe(&self, addr: &ControlAddress) -> Result<broadcast::Receiver<RuntimeData>> {
        let key: TapKey = (addr.node_id.clone(), addr.port.clone());
        let sender = self
            .taps
            .entry(key)
            .or_insert_with(|| broadcast::channel(DEFAULT_TAP_CAPACITY).0)
            .clone();
        Ok(sender.subscribe())
    }

    /// Subscribe to the `__system__` topic with replay of retained events.
    ///
    /// Returns a [`SystemEventReceiver`] that yields buffered events
    /// first (in publication order, snapshot taken atomically with the
    /// subscription), then transitions to the live broadcast stream.
    /// Equivalent to calling [`Self::subscribe`] for `__system__.out`
    /// except that the late subscriber sees the retained ring's contents.
    ///
    /// Phase 7 of the warm-session-pool feature.
    pub fn subscribe_system(&self) -> SystemEventReceiver {
        let key: TapKey = ("__system__".to_string(), None);
        // CRITICAL: snapshot and subscribe must be atomic with respect to the
        // publish path. We hold `system_retained` across BOTH the buffer
        // clone and `sender.subscribe()`. `publish_tap` for `__system__`
        // holds the same mutex across BOTH retain-append and broadcast::send.
        // The mutex therefore serializes the two operations:
        //   * publish before subscribe → event in buffered, broadcast precedes
        //     our subscribe (live misses, buffered delivers — once).
        //   * publish after subscribe → event NOT in buffered, broadcast
        //     follows our subscribe (live delivers — once).
        // No third state: a publish during our locked region blocks until we
        // release. Phase 7 of the warm-session-pool feature.
        let sender = self
            .taps
            .entry(key)
            .or_insert_with(|| broadcast::channel(DEFAULT_TAP_CAPACITY).0)
            .clone();
        let (buffered, live) = {
            let buf = self
                .system_retained
                .lock()
                .expect("system_retained mutex poisoned");
            let snapshot = buf.clone();
            let receiver = sender.subscribe();
            (snapshot, receiver)
        };
        SystemEventReceiver { buffered, live }
    }

    /// Set the cap for the `__system__` retained ring buffer. The buffer
    /// is truncated immediately if the new cap is below the current size.
    ///
    /// Setting cap to 0 disables retention; subsequent `__system__` events
    /// are not added to the buffer. Already-buffered events are dropped.
    pub fn set_system_retained_cap(&self, cap: usize) {
        self.system_retained_cap
            .store(cap, std::sync::atomic::Ordering::Relaxed);
        let mut buf = self
            .system_retained
            .lock()
            .expect("system_retained mutex poisoned");
        while buf.len() > cap {
            buf.pop_front();
        }
    }

    /// Inject data into a node's input port.
    ///
    /// When `addr.port` is `Some(port_name)` — i.e. the client is writing
    /// to an auxiliary input rail, not the node's main input — the payload
    /// is wrapped in an envelope so the node can distinguish aux-channel
    /// input from main-channel input:
    ///
    /// ```json
    /// {
    ///   "__aux_port__": "context",
    ///   "payload": <original payload, recursive>
    /// }
    /// ```
    ///
    /// Nodes that care about aux ports (e.g. an LLM reading a `context`
    /// rail) inspect the envelope in their `.process()`. Nodes that don't
    /// care can treat the envelope as opaque and ignore it, or unwrap
    /// `payload` to get the original shape.
    ///
    /// `addr.port == None` means the main rail — payload is forwarded
    /// unchanged, preserving the existing wire behavior.
    pub async fn publish(&self, addr: &ControlAddress, data: RuntimeData) -> Result<()> {
        if addr.direction != Direction::In {
            return Err(crate::Error::Execution(
                "publish requires Direction::In".into(),
            ));
        }
        let tx_guard = self.input_tx.read().await;
        let tx = tx_guard.as_ref().ok_or_else(|| {
            crate::Error::Execution("session control not yet attached to router".into())
        })?;

        let payload = match &addr.port {
            None => data,
            Some(port) => wrap_aux_port(port, data),
        };

        let packet = DataPacket {
            data: payload,
            from_node: format!("__control__:{}", self.session_id),
            to_node: Some(addr.node_id.clone()),
            session_id: self.session_id.clone(),
            sequence: self
                .inject_seq
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            sub_sequence: 0,
            metadata: std::collections::HashMap::new(),
        };
        tx.send(packet).await.map_err(|_| {
            crate::Error::Execution(format!("session {} router input closed", self.session_id))
        })
    }

    /// Publish data to a ``.out`` address — bypasses the direction check of
    /// :method:`publish`.
    ///
    /// Used for control-plane communication where an external process must
    /// write to a ``.out`` address that a subscriber reads from.
    pub async fn publish_out(&self, addr: &ControlAddress, data: RuntimeData) -> Result<()> {
        if addr.direction != Direction::Out {
            return Err(crate::Error::Execution(
                "publish_out requires Direction::Out (use publish for Direction::In)".into(),
            ));
        }
        // Write to the tap (broadcast channel) for this node/port.
        self.publish_tap(&addr.node_id, addr.port.as_deref(), data);
        Ok(())
    }

    /// Install an intercept for a node's output port. The returned
    /// `events_rx` is handed to the transport to forward
    /// `InterceptRequest`s to the client.
    pub fn intercept(
        &self,
        addr: &ControlAddress,
        deadline: Option<Duration>,
    ) -> Result<mpsc::Receiver<ControlEvent>> {
        if addr.direction != Direction::Out {
            return Err(crate::Error::Execution(
                "intercept requires Direction::Out".into(),
            ));
        }
        let key: InterceptKey = (addr.node_id.clone(), addr.port.clone());
        let (events_tx, events_rx) = mpsc::channel(DEFAULT_TAP_CAPACITY);
        let slot = Arc::new(InterceptSlot {
            events_tx,
            deadline: deadline.unwrap_or(DEFAULT_INTERCEPT_DEADLINE),
            next_correlation: std::sync::atomic::AtomicU64::new(0),
            pending: DashMap::new(),
        });
        if self.intercepts.insert(key, slot).is_some() {
            tracing::warn!(
                session = %self.session_id,
                node = %addr.node_id,
                "replaced existing intercept"
            );
        }
        Ok(events_rx)
    }

    pub fn remove_intercept(&self, addr: &ControlAddress) {
        let key: InterceptKey = (addr.node_id.clone(), addr.port.clone());
        self.intercepts.remove(&key);
    }

    /// Complete a pending intercept. Called by the transport when the
    /// client sends an `InterceptReply` frame.
    pub fn complete_intercept(&self, correlation_id: u64, decision: InterceptDecision) {
        // We don't know which (node,port) owns this correlation_id without
        // searching — intercepts are few, so a linear scan is fine.
        for entry in self.intercepts.iter() {
            if let Some((_, tx)) = entry.value().pending.remove(&correlation_id) {
                let _ = tx.send(decision);
                return;
            }
        }
    }

    /// Dispatch a client-originated frame. This is the single entry point the
    /// transport layer calls; it returns any event stream the frame opens
    /// (for Subscribe/Intercept) wrapped in `FrameOutcome`.
    pub async fn handle_frame(&self, frame: ControlFrame) -> Result<FrameOutcome> {
        match frame {
            ControlFrame::Subscribe(addr) => {
                let rx = self.subscribe(&addr)?;
                Ok(FrameOutcome::Tap(rx))
            }
            ControlFrame::Unsubscribe(addr) => {
                // Receivers are reclaimed when the transport drops its
                // broadcast::Receiver; nothing to actively cancel here.
                let _ = addr;
                Ok(FrameOutcome::Done)
            }
            ControlFrame::Publish { addr, data } => {
                self.publish(&addr, data).await?;
                Ok(FrameOutcome::Done)
            }
            ControlFrame::Intercept { addr, deadline } => {
                let rx = self.intercept(&addr, deadline)?;
                Ok(FrameOutcome::Intercept(rx))
            }
            ControlFrame::RemoveIntercept(addr) => {
                self.remove_intercept(&addr);
                Ok(FrameOutcome::Done)
            }
            ControlFrame::InterceptReply {
                correlation_id,
                decision,
            } => {
                self.complete_intercept(correlation_id, decision);
                Ok(FrameOutcome::Done)
            }
            ControlFrame::SetNodeState { node_id, state } => {
                self.set_node_state(node_id, state);
                Ok(FrameOutcome::Done)
            }
            ControlFrame::ClearNodeState { node_id } => {
                self.clear_node_state(&node_id);
                Ok(FrameOutcome::Done)
            }
        }
    }

    /// Publish a frame to a node's tap WITHOUT sending it through the
    /// data-path. This is the side channel nodes use to emit
    /// control-plane events (e.g. `turn_state` envelopes from the
    /// `ConversationCoordinatorNode`) that clients subscribed to
    /// `<node>.out` should see but that must NOT reach the
    /// downstream consumer. `broadcast::Sender::send` is sync so the
    /// method stays sync; callers can invoke from any context
    /// including the middle of a sync `process_streaming`.
    ///
    /// Ensures the tap sender exists so the broadcast ring captures
    /// the event even if no subscriber has attached yet — the first
    /// late subscriber then sees whatever's still in the buffer.
    pub fn publish_tap(&self, node_id: &str, port: Option<&str>, data: RuntimeData) {
        let key: TapKey = (node_id.to_string(), port.map(|s| s.to_string()));
        let sender = self
            .taps
            .entry(key)
            .or_insert_with(|| broadcast::channel(DEFAULT_TAP_CAPACITY).0)
            .clone();
        self.publish_tap_inner(node_id, port, data, sender);
    }

    /// Returns the live subscriber count on `(node_id, port)`. `0` when
    /// no broadcast channel has been created yet **or** when all
    /// subscribers have dropped. Cheap (one shard lookup +
    /// `Sender::receiver_count` which is an atomic load).
    ///
    /// Producers that pay a non-trivial serialization cost (e.g. the
    /// `__perf__` flush task building JSON) gate on this so an idle
    /// session with no HUD attached pays nothing past the in-process
    /// HDR-histogram aggregation. The retain-latest `__system__` ring
    /// must NOT use this — late subscribers replay missed events from
    /// the ring buffer.
    pub fn tap_receiver_count(&self, node_id: &str, port: Option<&str>) -> usize {
        let key: TapKey = (node_id.to_string(), port.map(|s| s.to_string()));
        self.taps
            .get(&key)
            .map(|s| s.value().receiver_count())
            .unwrap_or(0)
    }

    fn publish_tap_inner(
        &self,
        node_id: &str,
        port: Option<&str>,
        data: RuntimeData,
        sender: broadcast::Sender<RuntimeData>,
    ) {
        if node_id == "__system__" && port.is_none() {
            // CRITICAL: hold `system_retained` across both retain-append AND
            // broadcast::send so a concurrent subscribe_system either sees this
            // event in its snapshot OR is registered before our broadcast fires.
            // broadcast::send is non-blocking (slow subscribers lag rather than
            // block the publisher), so the lock-hold time stays in microseconds.
            // Phase 7 of the warm-session-pool feature.
            let cap = self
                .system_retained_cap
                .load(std::sync::atomic::Ordering::Relaxed);
            let mut buf = self
                .system_retained
                .lock()
                .expect("system_retained mutex poisoned");
            if cap > 0 {
                buf.push_back(data.clone());
                while buf.len() > cap {
                    buf.pop_front();
                }
            }
            let _ = sender.send(data);
            // lock drops at end of scope
        } else {
            let _ = sender.send(data);
        }
    }

    // ─── Capability transient channel ───────────────────────────────────────
    //
    // Spec: `node-runtime-context` capability transient channel. Producers
    // are transports (e.g. WebRTC `ServerPeer` post-SDP); consumers are
    // nodes whose `on_capability_update` is invoked by the session router
    // when an update lands on an address the node is subscribed to.
    //
    // Address scheme: `<medium>:<stream_id>` (e.g. `"video:default"`,
    // `"audio:default"`). Retain-latest: late subscribers immediately see
    // the current value.

    /// Publish (or re-publish) a `MediaCapabilities` value to `addr`.
    /// Stores under retain-latest semantics — subsequent
    /// [`Self::current_capability`] calls return this value until
    /// overwritten by a newer publish. Live subscribers receive the new
    /// value via their broadcast channel.
    pub fn publish_capability(&self, addr: &str, caps: crate::capabilities::MediaCapabilities) {
        let entry = self
            .capabilities
            .entry(addr.to_string())
            .or_insert_with(CapabilityEntry::new);
        *entry.value.write() = Some(caps.clone());
        // Slow subscribers receive `Lagged`; we never block the publisher.
        let _ = entry.tx.send(caps);
    }

    /// Read the latest published capability for `addr`, or `None` if no
    /// publisher has emitted yet. Atomic load against the per-address
    /// retain-latest cache; never blocks. Safe to call from any context
    /// (sync or async, hot or cold path).
    ///
    /// `NodeRuntimeContext::capability` writes through to a per-(node,
    /// session) cache populated by the default `on_capability_update`,
    /// so most node code reads via `ctx.capability(addr)` rather than
    /// going through the bus directly.
    pub fn current_capability(&self, addr: &str) -> Option<crate::capabilities::MediaCapabilities> {
        self.capabilities
            .get(addr)
            .and_then(|e| e.value.read().clone())
    }

    /// Subscribe to capability updates on `addr`. The returned receiver
    /// fires on every subsequent [`Self::publish_capability`]. **Does not
    /// replay** the current retained value — callers that need it should
    /// call [`Self::current_capability`] after subscribing to seed their
    /// state. (Subscribe-then-load is race-free against later publishes
    /// because the lookup happens after the subscriber is registered.)
    ///
    /// The session router uses this in its connection-graph walk at
    /// session start to wire each node to the streams it terminates into.
    pub fn subscribe_capability(
        &self,
        addr: &str,
    ) -> broadcast::Receiver<crate::capabilities::MediaCapabilities> {
        let entry = self
            .capabilities
            .entry(addr.to_string())
            .or_insert_with(CapabilityEntry::new);
        entry.tx.subscribe()
    }

    // ─── Media-clock transient channel (pacing-domains spec, Phase 5.1/5.2) ─

    /// Publish one media-clock tick to `addr` (e.g. `"audio:default"`,
    /// `"video:default"`). Producers call this once per outbound RTP
    /// frame from the WebRTC sender path. Slow subscribers see
    /// `Lagged`; the publisher never blocks. No-op if no subscriber
    /// has registered yet (the broadcast channel is created lazily on
    /// first subscribe).
    pub fn publish_media_clock(&self, addr: &str, clock: MediaClock) {
        if let Some(entry) = self.media_clocks.get(addr) {
            let _ = entry.send(clock);
        }
    }

    /// Subscribe to the media-clock stream on `addr`. Each subsequent
    /// [`Self::publish_media_clock`] fires an event. Late subscribers
    /// do **not** replay missed ticks — media_clock is a discrete
    /// event stream, not a retain-latest state snapshot.
    ///
    /// The broadcast channel is created lazily on first subscribe so
    /// publishers without subscribers don't allocate. Returns
    /// `RecvError::Closed` to subscribers when the producer calls
    /// [`Self::stop_media_clock`] (peer disconnect, sender shutdown);
    /// the wire pacer interprets this as a clean shutdown signal.
    pub fn subscribe_media_clock(&self, addr: &str) -> broadcast::Receiver<MediaClock> {
        let entry = self
            .media_clocks
            .entry(addr.to_string())
            .or_insert_with(|| broadcast::channel(MEDIA_CLOCK_CAPACITY).0);
        entry.subscribe()
    }

    /// Signal that the producer at `addr` has stopped emitting ticks.
    /// Closes the broadcast channel so subscribers see `RecvError::Closed`
    /// on the next `recv()`. Idempotent — calling on an already-stopped
    /// or never-published address is a no-op.
    ///
    /// Producers call this on graceful shutdown (peer disconnect, sender
    /// teardown) so wire-bound pacers exit cleanly. Phase 5.9.
    pub fn stop_media_clock(&self, addr: &str) {
        // Removing the entry from the DashMap drops the broadcast::Sender,
        // which closes all receivers' channels. Existing receivers
        // observe `RecvError::Closed` on their next `recv()`.
        self.media_clocks.remove(addr);
    }

    // ─── Router-facing API (called from SessionRouter::process_input) ──────

    /// Called by the router after a node produces an output, before the
    /// output is stored for downstream routing. Returns the (possibly
    /// replaced) data, or `None` if the intercept asked to drop it.
    ///
    /// Fans out to tap subscribers as a side effect.
    pub async fn on_node_output(
        &self,
        node_id: &str,
        port: Option<&str>,
        data: RuntimeData,
    ) -> Option<RuntimeData> {
        let port_owned = port.map(|s| s.to_string());

        // 1. Broadcast to tap subscribers (fire-and-forget; slow subs lag).
        let tap_key: TapKey = (node_id.to_string(), port_owned.clone());
        if let Some(tap) = self.taps.get(&tap_key) {
            let _ = tap.send(data.clone());
        }

        // 1b. Aux-port dual broadcast.
        //
        // If `data` carries an `__aux_port__` envelope (typed-RPC reply
        // frame, `context`/`barge_in` aux-port emissions, etc.), ALSO
        // broadcast to the (`node_id`, `Some(aux_port)`) tap so
        // subscribers using `subscribe("<node>.out.<aux_port>")` see
        // their frames. The caller-supplied `port` parameter is always
        // `None` for normal node outputs (see
        // `session_router::spawn_node_pipeline`), so without this branch
        // every aux-port-tagged frame would land only on the catch-all
        // tap. This is what makes typed RPC work end-to-end against
        // both the in-host multiprocess executor and loadable cdylib
        // plugins — the routing key lives inside the frame, not in
        // the caller's argument.
        //
        // The catch-all tap above still receives the same frame so
        // `subscribe("<node>.out")` callers (port=None) keep their
        // current behaviour. The aux-port tap is additive.
        if let Some(aux) = aux_port_of(&data) {
            if Some(aux.as_str()) != port_owned.as_deref() {
                let aux_key: TapKey = (node_id.to_string(), Some(aux));
                if let Some(tap) = self.taps.get(&aux_key) {
                    let _ = tap.send(data.clone());
                }
            }
        }

        // 2. If an intercept is installed, ask the client and wait (bounded).
        let intercept = self.intercepts.get(&tap_key).map(|r| r.value().clone());
        let Some(slot) = intercept else {
            return Some(data);
        };

        let correlation_id = slot
            .next_correlation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (reply_tx, reply_rx) = oneshot::channel();
        slot.pending.insert(correlation_id, reply_tx);

        let request = ControlEvent::InterceptRequest {
            addr: ControlAddress {
                node_id: node_id.to_string(),
                port: port_owned,
                direction: Direction::Out,
            },
            correlation_id,
            data: data.clone(),
        };

        // If the event channel is full or closed, we fall through to Pass so
        // the data plane never stalls on a wedged control plane.
        if slot.events_tx.try_send(request).is_err() {
            slot.pending.remove(&correlation_id);
            tracing::warn!(
                session = %self.session_id,
                node = %node_id,
                "intercept event channel saturated; passing through"
            );
            return Some(data);
        }

        match tokio::time::timeout(slot.deadline, reply_rx).await {
            Ok(Ok(InterceptDecision::Pass)) => Some(data),
            Ok(Ok(InterceptDecision::Replace(replacement))) => Some(replacement),
            Ok(Ok(InterceptDecision::Drop)) => None,
            Ok(Err(_canceled)) => {
                tracing::warn!(
                    session = %self.session_id,
                    node = %node_id,
                    "intercept reply channel dropped; passing through"
                );
                Some(data)
            }
            Err(_elapsed) => {
                slot.pending.remove(&correlation_id);
                tracing::warn!(
                    session = %self.session_id,
                    node = %node_id,
                    deadline_ms = slot.deadline.as_millis() as u64,
                    "intercept deadline exceeded; passing through"
                );
                Some(data)
            }
        }
    }
}

/// Return type of [`SessionControl::handle_frame`]. The transport layer
/// decides how to ferry streams (broadcast receiver / mpsc receiver) back
/// to the client over its wire protocol.
pub enum FrameOutcome {
    Done,
    Tap(broadcast::Receiver<RuntimeData>),
    Intercept(mpsc::Receiver<ControlEvent>),
}

// ────────────────────────────────────────────────────────────────────────────
// Process-wide registry
// ────────────────────────────────────────────────────────────────────────────

/// Process-wide registry of active session controls. The control-plane
/// transport (gRPC/WebSocket handler) looks up a `SessionControl` here by
/// `session_id` after the client sends its `Attach(session_id)` frame.
///
/// The `PipelineExecutor` is the natural owner of this bus; `create_session`
/// inserts, `terminate_session` removes.
#[derive(Default)]
pub struct SessionControlBus {
    sessions: DashMap<String, Arc<SessionControl>>,
}

impl SessionControlBus {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register a new session. Typically called by the executor right after
    /// `SessionRouter::new`.
    pub fn register(&self, control: Arc<SessionControl>) {
        self.sessions.insert(control.session_id.clone(), control);
    }

    /// Remove a session (on shutdown / termination). All active subscribers
    /// drop their receivers naturally.
    pub fn unregister(&self, session_id: &str) {
        self.sessions.remove(session_id);
    }

    /// Look up a session's control handle. Transport handler calls this
    /// once per client `Attach` and caches the Arc for the connection's
    /// lifetime.
    pub fn get(&self, session_id: &str) -> Option<Arc<SessionControl>> {
        self.sessions.get(session_id).map(|e| e.value().clone())
    }

    /// Install `bus` as the process-wide singleton if none is set yet.
    /// First-writer-wins: later calls from secondary executors (typical
    /// only in multi-executor tests) silently no-op. Regular single-
    /// executor deployments install their bus here once.
    ///
    /// Also installs the `plugin-sdk` control-message hook (when the
    /// `python-source-plugin` feature is enabled) so runtime
    /// `PROGRESS:` / `PUBLISH:` traffic from Python source-plugin
    /// subprocesses (e.g. LFM2-Audio blip events, in-`initialize()`
    /// progress) is routed to this bus instead of being dropped.
    /// Idempotent and best-effort: a second install attempt
    /// (multi-executor tests) returns the boxed hook from
    /// `install_control_message_hook` which we just discard.
    ///
    /// MUST be called from inside a tokio runtime — we capture
    /// `Handle::current()` so the hook can spawn router publishes
    /// from the plugin-sdk IPC OS thread (which has no TLS runtime).
    /// Panics if invoked outside any runtime; callers that legitimately
    /// install from a sync context should use `install_global_no_hook`.
    pub fn install_global(bus: Arc<Self>) {
        let _ = GLOBAL_BUS.set(bus);
        #[cfg(feature = "python-source-plugin")]
        {
            // Capture the host's runtime handle so the control-message
            // hook can fire `ctrl.publish(...).await` even when called
            // from plugin-sdk's std::thread::spawn IPC worker (which
            // has no `tokio::runtime::Handle` in TLS).
            let rt_handle = tokio::runtime::Handle::try_current().ok();
            if rt_handle.is_none() {
                tracing::warn!(
                    "SessionControlBus::install_global was called outside a tokio runtime — \
                     plugin-sdk PUBLISH messages will be dropped. Call from inside the runtime."
                );
            }
            let _ = remotemedia_plugin_sdk::control_hook::install_control_message_hook(Box::new(
                move |session_id: &str, node_id: &str, bytes: &[u8]| {
                    handle_plugin_control_message(rt_handle.as_ref(), session_id, node_id, bytes);
                },
            ));
        }
    }
}

/// Dispatch a single runtime control-message payload from a Python
/// source-plugin subprocess. Recognised prefixes mirror the in-tree
/// multiprocess executor (`PROGRESS:` / `PUBLISH:`); unknown payloads
/// are dropped silently. See
/// `crates/core/src/python/multiprocess/multiprocess_executor.rs::handle_runtime_control_message`
/// for the canonical wire format definitions.
#[cfg(feature = "python-source-plugin")]
fn handle_plugin_control_message(
    rt_handle: Option<&tokio::runtime::Handle>,
    session_id: &str,
    node_id: &str,
    bytes: &[u8],
) {
    let preview = std::str::from_utf8(bytes)
        .map(|s| s.chars().take(160).collect::<String>())
        .unwrap_or_else(|_| format!("<{} non-utf8 bytes>", bytes.len()));
    tracing::info!(
        "[plugin-ctrl] handle_plugin_control_message node={} session={} payload={:?}",
        node_id,
        session_id,
        preview
    );

    let Some(bus) = global_bus() else {
        tracing::warn!(
            "[plugin-ctrl] node {} payload dropped — no global SessionControlBus installed",
            node_id
        );
        return;
    };
    let Some(ctrl) = bus.get(session_id) else {
        tracing::warn!(
            "[plugin-ctrl] node {} payload dropped — no SessionControl for session {}",
            node_id,
            session_id
        );
        return;
    };

    if bytes.starts_with(b"PROGRESS:") {
        let json_str = std::str::from_utf8(&bytes[9..]).unwrap_or("{}");
        let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) else {
            tracing::warn!(
                "[plugin-ctrl] node {} PROGRESS: payload was not valid JSON: {:?}",
                node_id,
                json_str
            );
            return;
        };
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let status = json
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("loading");
        let message = json.get("message").and_then(|v| v.as_str()).unwrap_or("");
        tracing::info!(
            "[{}] Plugin node {} runtime progress: {} - {}",
            session_id,
            node_id,
            status,
            message
        );
        ctrl.publish_tap(
            "__system__",
            None,
            crate::data::RuntimeData::Json(serde_json::json!({
                "kind": "loading",
                "status": status,
                "node": node_id,
                "message": message,
                "ts_ms": ts,
            })),
        );
    } else if bytes.starts_with(b"PUBLISH:") {
        let json_str = std::str::from_utf8(&bytes[8..]).unwrap_or("{}");
        let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) else {
            tracing::warn!(
                "[plugin-ctrl] node {} PUBLISH: payload was not valid JSON: {:?}",
                node_id,
                json_str
            );
            return;
        };
        let target = match json.get("target").and_then(|v| v.as_str()) {
            Some(t) if !t.is_empty() => t.to_string(),
            _ => return,
        };
        let port = match json.get("port").and_then(|v| v.as_str()) {
            Some(p) if !p.is_empty() => p.to_string(),
            _ => return,
        };
        let payload = json
            .get("payload")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let addr = ControlAddress::node_in(target.clone()).with_port(port.clone());
        let data = crate::data::RuntimeData::Json(payload);

        // Hook runs on plugin-sdk's IPC OS thread, which has no tokio
        // runtime in TLS. Get the current Handle if we're inside one
        // (typical for a server) and spawn there; otherwise drop the
        // publish and log. Spawning rather than block_on avoids stalling
        // the IPC thread on a slow router send.
        let publish_target = format!("{}.in.{}", target, port);
        tracing::info!(
            "[plugin-ctrl] node {} PUBLISH -> {} (spawning router send)",
            node_id,
            publish_target
        );
        // Prefer the runtime handle captured at install time (the
        // plugin-sdk IPC OS thread has no `Handle::current()`), fall
        // back to `try_current` for in-runtime callers (tests).
        let handle = rt_handle
            .cloned()
            .or_else(|| tokio::runtime::Handle::try_current().ok());
        match handle {
            Some(handle) => {
                let node_id_owned = node_id.to_string();
                handle.spawn(async move {
                    match ctrl.publish(&addr, data).await {
                        Ok(()) => tracing::info!(
                            "[plugin-ctrl] node {} PUBLISH -> {} delivered",
                            node_id_owned,
                            publish_target
                        ),
                        Err(e) => tracing::warn!(
                            "[plugin-ctrl] node {} PUBLISH -> {} failed: {}",
                            node_id_owned,
                            publish_target,
                            e
                        ),
                    }
                });
            }
            None => {
                tracing::warn!(
                    "[plugin-ctrl] node {} PUBLISH -> {} dropped — no tokio runtime captured at install_global nor available via try_current",
                    node_id, publish_target
                );
            }
        }
    } else {
        tracing::debug!(
            "[plugin-ctrl] node {} unknown payload prefix — dropping",
            node_id
        );
    }
}

static GLOBAL_BUS: std::sync::OnceLock<Arc<SessionControlBus>> = std::sync::OnceLock::new();

/// Get the process-wide [`SessionControlBus`] if one has been installed.
///
/// Returns `None` until `PipelineExecutor::new` / `with_config` has run
/// once in this process. Rust nodes that need to publish to another
/// node's aux port (e.g. `ConversationCoordinatorNode` firing
/// `llm.in.barge_in` on a user-barge) look themselves up via
/// `global_bus()?.get(session_id)?.publish(...)`.
///
/// Intentionally permissive about missing state — nodes that reach for
/// the bus before an executor exists just degrade to a no-op (the
/// client-side barge-in fanout is the fallback), so a wrong test setup
/// doesn't panic the data plane.
pub fn global_bus() -> Option<Arc<SessionControlBus>> {
    GLOBAL_BUS.get().cloned()
}

// ────────────────────────────────────────────────────────────────────────────
// Integration hook for SessionRouter
// ────────────────────────────────────────────────────────────────────────────
//
// Adding the bus to the existing router requires two small edits to
// session_router.rs. They are written out here rather than applied, so you
// can review before I touch that file.
//
// 1.  Field + setter on `SessionRouter`:
//
//         control: Option<Arc<SessionControl>>,
//
//     and:
//
//         pub async fn attach_control(&mut self, control: Arc<SessionControl>) {
//             control
//                 .attach_input_sender(self.input_tx.clone().expect("pre-run"))
//                 .await;
//             self.control = Some(control);
//         }
//
// 2.  One hook inside `process_input`, after the node's outputs are
//     collected and before they're stored for downstream routing:
//
//         let node_outputs = node_outputs_ref.lock().unwrap().clone();
//         let node_outputs = if let Some(ctrl) = &self.control {
//             let mut filtered = Vec::with_capacity(node_outputs.len());
//             for out in node_outputs {
//                 if let Some(kept) = ctrl.on_node_output(node_id, None, out).await {
//                     filtered.push(kept);
//                 }
//             }
//             filtered
//         } else {
//             node_outputs
//         };
//         all_node_outputs.insert(node_id.clone(), node_outputs);
//
//     (Once typed output ports land, replace `None` with the actual port
//     name the node wrote to.)
//
// 3.  Inject path is already handled: publish() pushes a `DataPacket` with
//     `to_node = addr.node_id` into the router's existing input channel,
//     which process_input already respects (see `packet.to_node` branch).
//     The only missing field is `port` on DataPacket for aux-input routing;
//     adding it is backwards compatible (Option<String>, default None).

// ────────────────────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn text(s: &str) -> RuntimeData {
        RuntimeData::Text(s.to_string())
    }

    #[tokio::test]
    async fn tap_receives_node_outputs() {
        let ctrl = SessionControl::new("sess-1");
        let addr = ControlAddress::node_out("whisper");
        let mut rx = ctrl.subscribe(&addr).unwrap();

        let kept = ctrl.on_node_output("whisper", None, text("hello")).await;
        assert!(kept.is_some());

        let got = rx.recv().await.unwrap();
        match got {
            RuntimeData::Text(s) => assert_eq!(s, "hello"),
            _ => panic!("wrong type"),
        }
    }

    #[tokio::test]
    async fn intercept_replace_mutates_downstream() {
        let ctrl = SessionControl::new("sess-1");
        let addr = ControlAddress::node_out("llm");
        let mut events = ctrl
            .intercept(&addr, Some(Duration::from_millis(200)))
            .unwrap();

        // Spawn the data-plane side.
        let ctrl_dp = ctrl.clone();
        let data_task =
            tokio::spawn(async move { ctrl_dp.on_node_output("llm", None, text("raw")).await });

        // Receive the intercept request and reply with Replace.
        let ev = events.recv().await.unwrap();
        let ControlEvent::InterceptRequest { correlation_id, .. } = ev else {
            panic!("expected InterceptRequest");
        };
        ctrl.complete_intercept(correlation_id, InterceptDecision::Replace(text("redacted")));

        let out = data_task.await.unwrap().unwrap();
        match out {
            RuntimeData::Text(s) => assert_eq!(s, "redacted"),
            _ => panic!("wrong type"),
        }
    }

    #[tokio::test]
    async fn intercept_deadline_passes_through() {
        let ctrl = SessionControl::new("sess-1");
        let addr = ControlAddress::node_out("llm");
        let _events = ctrl
            .intercept(&addr, Some(Duration::from_millis(20)))
            .unwrap();
        // No reply — deadline should fire and the frame passes through.
        let out = ctrl.on_node_output("llm", None, text("x")).await.unwrap();
        match out {
            RuntimeData::Text(s) => assert_eq!(s, "x"),
            _ => panic!("wrong type"),
        }
    }

    #[tokio::test]
    async fn publish_without_router_errors_cleanly() {
        let ctrl = SessionControl::new("sess-1");
        let addr = ControlAddress::node_in("llm").with_port("context");
        let err = ctrl.publish(&addr, text("doc")).await.unwrap_err();
        assert!(err.to_string().contains("not yet attached"));
    }

    #[tokio::test]
    async fn bus_registry_lookup() {
        let bus = SessionControlBus::new();
        let ctrl = SessionControl::new("sess-A");
        bus.register(ctrl.clone());

        assert!(bus.get("sess-A").is_some());
        assert!(bus.get("missing").is_none());

        bus.unregister("sess-A");
        assert!(bus.get("sess-A").is_none());
    }

    fn caps_audio(rate: u32) -> crate::capabilities::MediaCapabilities {
        use crate::capabilities::{
            AudioConstraints, AudioSampleFormat, ConstraintValue, MediaCapabilities,
            MediaConstraints,
        };
        MediaCapabilities::with_input(MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(rate)),
            channels: Some(ConstraintValue::Exact(1)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        }))
    }

    /// `current_capability` returns `None` before any publish, then the
    /// most recent value, replaced atomically on republish.
    #[test]
    fn capability_retain_latest_round_trip() {
        let ctrl = SessionControl::new("sess-1");
        assert!(ctrl.current_capability("audio:default").is_none());

        ctrl.publish_capability("audio:default", caps_audio(48_000));
        assert!(ctrl.current_capability("audio:default").is_some());

        ctrl.publish_capability("audio:default", caps_audio(16_000));
        let observed = ctrl.current_capability("audio:default").expect("populated");
        // Cheapest stable check — MediaCapabilities doesn't impl PartialEq
        // across all inner enums.
        let dbg = format!("{:?}", observed);
        assert!(
            dbg.contains("16000"),
            "expected the latest publish: {}",
            dbg
        );
    }

    /// Live subscribers see every publish. New subscribers register lazily
    /// (the entry is created on first publish or subscribe), so the order
    /// of `subscribe()` vs `publish_capability()` matters only for the
    /// first event — the retained value is always reachable via
    /// `current_capability`.
    #[tokio::test]
    async fn capability_live_subscriber_receives_publishes() {
        let ctrl = SessionControl::new("sess-2");
        let mut rx = ctrl.subscribe_capability("video:default");

        ctrl.publish_capability("video:default", caps_audio(60));
        let got = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .expect("timed out")
            .expect("rx not closed");
        assert!(format!("{:?}", got).contains("60"));
    }

    /// Late subscriber (registered after the publish) does not replay the
    /// retained value via the broadcast channel — but
    /// `current_capability` returns it. This is the documented contract:
    /// subscribers seed via `current_capability`, then receive deltas.
    #[tokio::test]
    async fn capability_late_subscriber_seeds_via_current() {
        let ctrl = SessionControl::new("sess-3");
        ctrl.publish_capability("audio:default", caps_audio(48_000));

        // Late subscriber.
        let mut rx = ctrl.subscribe_capability("audio:default");

        // No publish since subscribe → broadcast channel has nothing.
        let immediate = tokio::time::timeout(std::time::Duration::from_millis(20), rx.recv()).await;
        assert!(immediate.is_err(), "broadcast should not replay");

        // Retained value still reachable via current_capability.
        assert!(ctrl.current_capability("audio:default").is_some());

        // New publish reaches the live subscriber.
        ctrl.publish_capability("audio:default", caps_audio(16_000));
        let got = tokio::time::timeout(std::time::Duration::from_millis(100), rx.recv())
            .await
            .expect("timed out")
            .expect("rx not closed");
        assert!(format!("{:?}", got).contains("16000"));
    }

    fn json_event(label: &str) -> RuntimeData {
        RuntimeData::Json(serde_json::json!({ "status": label }))
    }

    #[tokio::test]
    async fn subscribe_system_replays_retained_events() {
        let control = SessionControl::new("test");
        control.publish_tap("__system__", None, json_event("loading_a"));
        control.publish_tap("__system__", None, json_event("loading_b"));
        control.publish_tap("__system__", None, json_event("loading_c"));

        let mut rx = control.subscribe_system();
        let a = rx.recv().await.unwrap();
        let b = rx.recv().await.unwrap();
        let c = rx.recv().await.unwrap();

        // Order matches publication order.
        match (a, b, c) {
            (RuntimeData::Json(va), RuntimeData::Json(vb), RuntimeData::Json(vc)) => {
                assert_eq!(va["status"], "loading_a");
                assert_eq!(vb["status"], "loading_b");
                assert_eq!(vc["status"], "loading_c");
            }
            _ => panic!("expected three Json events"),
        }
    }

    #[tokio::test]
    async fn subscribe_system_yields_buffered_then_live() {
        let control = SessionControl::new("test");
        control.publish_tap("__system__", None, json_event("buffered"));

        let mut rx = control.subscribe_system();

        // Live event published AFTER subscribe.
        control.publish_tap("__system__", None, json_event("live"));

        let buffered = rx.recv().await.unwrap();
        let live = rx.recv().await.unwrap();
        match (buffered, live) {
            (RuntimeData::Json(vb), RuntimeData::Json(vl)) => {
                assert_eq!(vb["status"], "buffered");
                assert_eq!(vl["status"], "live");
            }
            _ => panic!("expected Json events"),
        }
    }

    #[tokio::test]
    async fn buffer_overflow_drops_oldest() {
        let control = SessionControl::new("test");
        control.set_system_retained_cap(3);

        for i in 0..5 {
            control.publish_tap("__system__", None, json_event(&format!("event_{i}")));
        }

        let mut rx = control.subscribe_system();
        let mut got = Vec::new();
        for _ in 0..3 {
            let RuntimeData::Json(v) = rx.recv().await.unwrap() else {
                panic!("expected Json")
            };
            got.push(v["status"].as_str().unwrap().to_string());
        }
        assert_eq!(
            got,
            vec![
                "event_2".to_string(),
                "event_3".to_string(),
                "event_4".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn cap_zero_disables_retention() {
        let control = SessionControl::new("test");
        control.set_system_retained_cap(0);
        control.publish_tap("__system__", None, json_event("dropped_a"));
        control.publish_tap("__system__", None, json_event("dropped_b"));

        let mut rx = control.subscribe_system();

        // No buffered events. The next recv awaits a live event;
        // wrap in a short timeout to avoid hanging the test.
        let next = tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv()).await;
        assert!(next.is_err(), "expected no buffered events when cap=0");

        // Live events still flow.
        control.publish_tap("__system__", None, json_event("live"));
        let live = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
            .await
            .expect("live event should arrive within 500ms")
            .unwrap();
        if let RuntimeData::Json(v) = live {
            assert_eq!(v["status"], "live");
        } else {
            panic!("expected Json");
        }
    }

    #[tokio::test]
    async fn non_system_topics_are_not_buffered() {
        let control = SessionControl::new("test");
        control.publish_tap("audio_node", None, json_event("audio_event"));

        // The `__system__` retained buffer must be empty.
        let buffered = control.system_retained.lock().unwrap().clone();
        assert!(
            buffered.is_empty(),
            "non-__system__ topics should not be retained"
        );

        // The `audio_node.out` broadcast still works (existing semantics
        // unchanged — late subscribers miss the event).
        let addr = ControlAddress::node_out("audio_node");
        let mut rx = control.subscribe(&addr).unwrap();

        // Publish a NEW event after subscribing.
        control.publish_tap("audio_node", None, json_event("new"));
        let got = tokio::time::timeout(std::time::Duration::from_millis(500), rx.recv())
            .await
            .expect("event should arrive within 500ms")
            .unwrap();
        if let RuntimeData::Json(v) = got {
            assert_eq!(v["status"], "new");
        } else {
            panic!("expected Json");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn mid_stream_subscriber_sees_every_concurrent_event() {
        use std::collections::HashSet;

        // Regression for the snapshot↔subscribe race in `subscribe_system`.
        // Pre-fix, that function dropped `system_retained` between cloning
        // the retained buffer and calling `sender.subscribe()`, and pre-fix
        // `publish_tap` dropped the mutex between the retain-append and
        // the broadcast. A `publish_tap` running fully in the subscriber's
        // snapshot↔subscribe window landed in retained AFTER the snapshot
        // AND broadcast BEFORE the live receiver registered — invisible on
        // both paths. Post-fix, the mutex is held across BOTH halves of
        // `publish_tap` and BOTH halves of `subscribe_system`, so the two
        // operations strictly serialize: every event is either in the
        // subscriber's `buffered` snapshot OR delivered to its live
        // receiver, never neither.
        //
        // The previous regression test only asserted on a post-publish
        // retained-snapshot subscriber, which pre-fix code already
        // handled correctly. This test exercises the live-receive path:
        // a single mid-stream subscriber registers BEFORE the publisher
        // starts and drains its receiver throughout the publisher's run,
        // asserting every event was observed via either the buffered
        // snapshot or the live broadcast.
        let ctrl = SessionControl::new("race");
        const N: usize = 200;

        // Subscriber spawns FIRST so it'll race the publisher. Subscribes
        // once, drains until it sees the sentinel "done" event.
        let sub_ctrl = ctrl.clone();
        let sub_task = tokio::spawn(async move {
            let mut rx = sub_ctrl.subscribe_system();
            let mut seen: HashSet<String> = HashSet::new();
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, rx.recv()).await {
                    Ok(Some(RuntimeData::Json(v))) => {
                        let s = v["status"].as_str().unwrap_or("").to_string();
                        if s == "done" {
                            break;
                        }
                        seen.insert(s);
                    }
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }
            }
            seen
        });

        // Yield once so the subscriber actually registers before the
        // publisher starts; otherwise the entire publish stream could
        // land purely in retained (which the pre-fix code handled
        // correctly — no race exposure on the live-receive path).
        tokio::task::yield_now().await;

        // Publisher fires N events with yields to encourage interleaving.
        // Each yield gives the runtime a chance to context-switch into
        // the subscriber's snapshot↔subscribe window (pre-fix), the exact
        // window this regression test targets.
        let pub_ctrl = ctrl.clone();
        let pub_task = tokio::spawn(async move {
            for i in 0..N {
                pub_ctrl.publish_tap("__system__", None, json_event(&format!("e{i}")));
                tokio::task::yield_now().await;
            }
            // Sentinel so the subscriber stops draining cleanly.
            pub_ctrl.publish_tap("__system__", None, json_event("done"));
        });

        pub_task.await.unwrap();
        let seen = sub_task.await.unwrap();

        let expected: HashSet<String> = (0..N).map(|i| format!("e{i}")).collect();
        let missing: Vec<&String> = expected.difference(&seen).collect();
        assert!(
            missing.is_empty(),
            "mid-stream subscriber lost {} of {N} events under contention: {:?}",
            missing.len(),
            missing,
        );
    }
}
