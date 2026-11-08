//! Client-hosted endpoints (openspec `add-agent-harness-integration`).
//!
//! A control client attached to a session (an agent-harness bridge, the
//! Hermes plugin, a service) registers a named **endpoint**. The bus then
//! treats that name as a virtual node: nodes call methods on it with the
//! typed-RPC envelope shapes, and the host answers with typed-RPC replies.
//!
//! Semantics (see `specs/client-hosted-endpoints/spec.md`):
//!
//! * **Exclusive, attach-scoped registration.** At most one host per
//!   endpoint; a second registration fails with `already_hosted` unless it
//!   asks for takeover. Dropping the [`EndpointHost`] unregisters it.
//! * **Host-only, bounded delivery.** Calls go through a per-endpoint queue
//!   (never through broadcast taps). A full queue fails the call at once with
//!   `overloaded`.
//! * **Presence.** With no host, a call fails with `no_responder` at once,
//!   unless it carries `hold_ms`, in which case it waits up to
//!   `min(hold_ms, deadline)` for a host to register.
//! * **Caller-owned deadlines.** On expiry the caller gets `timeout` and the
//!   host gets `Cancelled { reason: Deadline }`. Dropping the caller's
//!   [`EndpointCallStream`] cancels the call (`reason: Caller`). Replies that
//!   arrive afterwards are discarded and counted as `late`.
//! * **Host loss.** Unregistering, takeover and transport closure fail every
//!   pending call with `responder_gone` immediately.

// Shared wire types, re-exported so transports need not depend on
// `remotemedia-traits` directly.
pub use remotemedia_traits::call::{
    call_reply_method, CallErrorKind, CallReplyEnvelope, CallReplyEvent, CallRequestEnvelope,
    CALL_AUX_PORT, CALL_REPLY_METHOD, DEFAULT_CALL_DEADLINE_MS,
};
pub use remotemedia_traits::rpc::{RpcId, RpcMethod, RpcMode, RpcRequest, RpcResponder};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, Notify};

/// Default per-endpoint queue capacity (undelivered calls).
pub const DEFAULT_ENDPOINT_QUEUE_CAPACITY: usize = 64;

/// Virtual node ids an endpoint may never use.
pub const RESERVED_ENDPOINT_NAMES: &[&str] =
    &["__system__", "__telemetry__", "__perf__", "__timeline__"];

/// How many finished call ids each host remembers for late-reply accounting.
const FINISHED_MEMORY: usize = 512;
const PRESENCE_CAPACITY: usize = 256;
/// Samples kept per method for latency percentiles.
const LATENCY_MAX_US: u64 = 3_600_000_000;

// ────────────────────────────────────────────────────────────────────────────
// Public types
// ────────────────────────────────────────────────────────────────────────────

/// Who answers an endpoint. Feeds the session's `agent_backed` flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HostKind {
    /// An AI agent / harness answers (latency is not representative).
    Agent,
    /// A deterministic service answers.
    #[default]
    Service,
}

impl HostKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Service => "service",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "agent" => Some(Self::Agent),
            "service" | "" => Some(Self::Service),
            _ => None,
        }
    }
}

/// Registration request for one endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointSpec {
    /// Endpoint id, e.g. `"harness"`. Must not equal a node id or a
    /// reserved virtual node.
    pub name: String,
    /// Mode per method. Empty accepts any method, with the mode taken from
    /// the request.
    #[serde(default)]
    pub methods: Vec<RpcMethod>,
    #[serde(default)]
    pub host_kind: HostKind,
    /// Informational adapter name (agent hosts).
    #[serde(default)]
    pub adapter: Option<String>,
    /// Bound on calls in flight at the host (queued, or delivered and not
    /// yet answered); a call beyond it fails at once with `overloaded`.
    /// 0 = default (64).
    #[serde(default = "default_queue_capacity")]
    pub queue_capacity: usize,
}

fn default_queue_capacity() -> usize {
    DEFAULT_ENDPOINT_QUEUE_CAPACITY
}

impl EndpointSpec {
    pub fn new(name: impl Into<String>, host_kind: HostKind) -> Self {
        Self {
            name: name.into(),
            methods: Vec::new(),
            host_kind,
            adapter: None,
            queue_capacity: DEFAULT_ENDPOINT_QUEUE_CAPACITY,
        }
    }

    /// Builder: declare a method with its mode.
    pub fn method(mut self, name: impl Into<String>, mode: RpcMode) -> Self {
        self.methods.push(RpcMethod::new(name, mode));
        self
    }

    pub fn adapter(mut self, adapter: impl Into<String>) -> Self {
        self.adapter = Some(adapter.into());
        self
    }

    pub fn queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity;
        self
    }

    fn capacity(&self) -> usize {
        if self.queue_capacity == 0 {
            DEFAULT_ENDPOINT_QUEUE_CAPACITY
        } else {
            self.queue_capacity
        }
    }
}

/// Why a registration was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointError {
    /// Another client hosts this endpoint (and takeover was not requested).
    AlreadyHosted,
    /// The name collides with a node id, a reserved virtual node, or is not
    /// a valid endpoint id.
    NameConflict,
    /// The caller is not authorized to host endpoints.
    Forbidden,
    /// The session is closing / closed.
    SessionClosed,
}

impl EndpointError {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AlreadyHosted => "already_hosted",
            Self::NameConflict => "name_conflict",
            Self::Forbidden => "forbidden",
            Self::SessionClosed => "session_closed",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "already_hosted" => Self::AlreadyHosted,
            "name_conflict" => Self::NameConflict,
            "forbidden" => Self::Forbidden,
            "session_closed" => Self::SessionClosed,
            _ => return None,
        })
    }
}

impl std::fmt::Display for EndpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::error::Error for EndpointError {}

/// One hosted endpoint, as listed by [`crate::transport::session_control::SessionControl::endpoints`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EndpointInfo {
    pub name: String,
    pub methods: Vec<EndpointMethodInfo>,
    pub host_kind: HostKind,
    pub adapter: Option<String>,
    /// Calls queued or delivered and not yet finished.
    pub pending: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EndpointMethodInfo {
    pub name: String,
    pub mode: RpcMode,
}

/// Presence change broadcast by `watch_endpoints`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EndpointPresence {
    pub name: String,
    pub hosted: bool,
}

/// Why the host was told to stop working on a call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelReason {
    /// The call's deadline expired; the caller got `timeout`.
    Deadline,
    /// The caller cancelled (explicit cancel, barge-in, node shutdown).
    Caller,
    /// The session is closing.
    SessionClosed,
}

impl CancelReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Deadline => "deadline",
            Self::Caller => "caller",
            Self::SessionClosed => "session_closed",
        }
    }
}

/// Work delivered to an endpoint host.
#[derive(Debug, Clone)]
pub enum HostEvent {
    Call(IncomingCall),
    Cancelled {
        call_id: String,
        reason: CancelReason,
    },
}

/// One call delivered to the host.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IncomingCall {
    /// Host-unique id: `"<caller_node>/<caller corr_id>"`.
    pub call_id: String,
    pub caller_node: String,
    pub session_id: String,
    pub method: String,
    pub mode: RpcMode,
    pub args: Vec<Value>,
    pub kwargs: Map<String, Value>,
    /// Deadline remaining at delivery.
    pub deadline_ms: u64,
    pub route_hint: Option<String>,
    pub adapter_hint: Option<String>,
}

/// Options for [`crate::nodes::NodeRuntimeContext::call_endpoint`].
#[derive(Debug, Clone)]
pub struct CallOptions {
    pub mode: RpcMode,
    pub deadline_ms: u64,
    pub hold_ms: Option<u64>,
    pub route_hint: Option<String>,
    pub adapter_hint: Option<String>,
}

impl Default for CallOptions {
    fn default() -> Self {
        Self {
            mode: RpcMode::Reply,
            deadline_ms: DEFAULT_CALL_DEADLINE_MS,
            hold_ms: None,
            route_hint: None,
            adapter_hint: None,
        }
    }
}

impl CallOptions {
    pub fn mode(mut self, mode: RpcMode) -> Self {
        self.mode = mode;
        self
    }
    pub fn deadline_ms(mut self, ms: u64) -> Self {
        self.deadline_ms = ms;
        self
    }
    pub fn hold_ms(mut self, ms: u64) -> Self {
        self.hold_ms = Some(ms);
        self
    }
    pub fn route_hint(mut self, hint: impl Into<String>) -> Self {
        self.route_hint = Some(hint.into());
        self
    }
    pub fn adapter_hint(mut self, hint: impl Into<String>) -> Self {
        self.adapter_hint = Some(hint.into());
        self
    }
}

/// Terminal failure of an endpoint call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointCallError {
    pub kind: CallErrorKind,
    pub message: String,
}

impl std::fmt::Display for EndpointCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.message.is_empty() {
            write!(f, "{}", self.kind)
        } else {
            write!(f, "{}: {}", self.kind, self.message)
        }
    }
}

impl std::error::Error for EndpointCallError {}

// ────────────────────────────────────────────────────────────────────────────
// Caller handle
// ────────────────────────────────────────────────────────────────────────────

/// Reply stream of one endpoint call. Yields `value` chunks in order and
/// ends after the terminal chunk (`end` or `error`, also yielded).
/// Dropping it before the terminal chunk cancels the call.
pub struct EndpointCallStream {
    rx: mpsc::UnboundedReceiver<CallReplyEnvelope>,
    registry: Option<Weak<EndpointRegistry>>,
    caller_node: String,
    corr_id: String,
    done: bool,
}

impl EndpointCallStream {
    fn new(
        rx: mpsc::UnboundedReceiver<CallReplyEnvelope>,
        registry: Option<Weak<EndpointRegistry>>,
        caller_node: &str,
        corr_id: &str,
    ) -> Self {
        Self {
            rx,
            registry,
            caller_node: caller_node.to_string(),
            corr_id: corr_id.to_string(),
            done: false,
        }
    }

    /// An already-completed stream yielding just `envelope`.
    fn immediate(caller_node: &str, envelope: CallReplyEnvelope) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let corr = envelope.corr_id.clone();
        let _ = tx.send(envelope);
        Self::new(rx, None, caller_node, &corr)
    }

    /// A stream that yields nothing (used for cancellation envelopes).
    pub(crate) fn empty(caller_node: &str, corr_id: &str) -> Self {
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut s = Self::new(rx, None, caller_node, corr_id);
        s.done = true;
        s
    }

    pub fn corr_id(&self) -> &str {
        &self.corr_id
    }

    pub fn caller_node(&self) -> &str {
        &self.caller_node
    }

    /// Next reply chunk; `None` after the terminal chunk.
    pub async fn recv(&mut self) -> Option<CallReplyEnvelope> {
        if self.done {
            return None;
        }
        match self.rx.recv().await {
            Some(env) => {
                if env.is_terminal() {
                    self.done = true;
                }
                Some(env)
            }
            None => {
                self.done = true;
                None
            }
        }
    }

    /// Resolve a `reply` call: the first value (`end` without a value ⇒
    /// `Null`), or the terminal error.
    pub async fn value(mut self) -> Result<Value, EndpointCallError> {
        match self.recv().await {
            Some(CallReplyEnvelope {
                event: CallReplyEvent::Value { data },
                ..
            }) => {
                // Reply mode: a value is terminal. Stream mode: callers that
                // want every chunk use `collect`; drop the rest here.
                Ok(data)
            }
            Some(CallReplyEnvelope {
                event: CallReplyEvent::End,
                ..
            }) => Ok(Value::Null),
            Some(CallReplyEnvelope {
                event: CallReplyEvent::Error { error, message },
                ..
            }) => Err(EndpointCallError {
                kind: error,
                message,
            }),
            None => Err(EndpointCallError {
                kind: CallErrorKind::Cancelled,
                message: "call ended without a reply".into(),
            }),
        }
    }

    /// Collect every `value` of a `stream` call until `end`.
    pub async fn collect(mut self) -> Result<Vec<Value>, EndpointCallError> {
        let mut out = Vec::new();
        while let Some(env) = self.recv().await {
            match env.event {
                CallReplyEvent::Value { data } => out.push(data),
                CallReplyEvent::End => return Ok(out),
                CallReplyEvent::Error { error, message } => {
                    return Err(EndpointCallError {
                        kind: error,
                        message,
                    })
                }
            }
        }
        Ok(out)
    }

    /// Cancel explicitly (same as dropping).
    pub fn cancel(self) {}
}

impl futures::Stream for EndpointCallStream {
    type Item = CallReplyEnvelope;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.done {
            return std::task::Poll::Ready(None);
        }
        match self.rx.poll_recv(cx) {
            std::task::Poll::Ready(Some(env)) => {
                if env.is_terminal() {
                    self.done = true;
                }
                std::task::Poll::Ready(Some(env))
            }
            std::task::Poll::Ready(None) => {
                self.done = true;
                std::task::Poll::Ready(None)
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl Drop for EndpointCallStream {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        if let Some(reg) = self.registry.as_ref().and_then(Weak::upgrade) {
            reg.cancel_call(&self.caller_node, &self.corr_id, CancelReason::Caller);
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Host handles
// ────────────────────────────────────────────────────────────────────────────

struct HostShared {
    notify: Notify,
    end_reason: parking_lot::Mutex<Option<&'static str>>,
}

/// Registration handle returned by `host_endpoint`. Dropping it unregisters
/// the endpoint and fails every pending call with `responder_gone`.
pub struct EndpointHost {
    registry: Weak<EndpointRegistry>,
    name: String,
    host_id: u64,
    shared: Arc<HostShared>,
    spec: EndpointSpec,
}

impl std::fmt::Debug for EndpointHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EndpointHost")
            .field("name", &self.name)
            .field("host_id", &self.host_id)
            .finish()
    }
}

impl EndpointHost {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn spec(&self) -> &EndpointSpec {
        &self.spec
    }

    /// Next call or cancellation. `None` once this host no longer hosts the
    /// endpoint (unregistered, taken over, session closed) — see
    /// [`Self::end_reason`].
    pub async fn next(&mut self) -> Option<HostEvent> {
        loop {
            let notified = self.shared.notify.notified();
            {
                let Some(reg) = self.registry.upgrade() else {
                    return None;
                };
                match reg.pop_event(&self.name, self.host_id) {
                    Pop::Event(ev) => return Some(ev),
                    Pop::Ended => return None,
                    Pop::Empty => {}
                }
            }
            notified.await;
        }
    }

    /// Why [`Self::next`] returned `None`: `"taken_over"`, `"session_closed"`
    /// or `"unhosted"`.
    pub fn end_reason(&self) -> Option<&'static str> {
        *self.shared.end_reason.lock()
    }

    /// Whether this handle still hosts the endpoint.
    pub fn is_hosted(&self) -> bool {
        self.registry
            .upgrade()
            .map(|r| r.host_is_current(&self.name, self.host_id))
            .unwrap_or(false)
    }

    /// Cloneable reply handle.
    pub fn replier(&self) -> EndpointReplier {
        EndpointReplier {
            registry: self.registry.clone(),
            name: self.name.clone(),
            host_id: self.host_id,
        }
    }

    /// Unregister now (same as drop).
    pub fn unhost(self) {}
}

impl Drop for EndpointHost {
    fn drop(&mut self) {
        if let Some(reg) = self.registry.upgrade() {
            reg.remove_host(&self.name, self.host_id, "unhosted");
        }
    }
}

/// Cloneable handle used by a host to answer calls.
#[derive(Clone)]
pub struct EndpointReplier {
    registry: Weak<EndpointRegistry>,
    name: String,
    host_id: u64,
}

impl EndpointReplier {
    /// Deliver one reply chunk for `call_id`.
    ///
    /// Errors: `not_found` (unknown call), `timeout` (the call already timed
    /// out — counted late), `cancelled` (the call was cancelled or already
    /// completed — counted late), `responder_gone` (this host no longer hosts
    /// the endpoint).
    pub fn send(
        &self,
        call_id: &str,
        event: CallReplyEvent,
        meta: Option<Value>,
    ) -> Result<(), CallErrorKind> {
        let Some(reg) = self.registry.upgrade() else {
            return Err(CallErrorKind::ResponderGone);
        };
        reg.reply(&self.name, self.host_id, call_id, event, meta)
    }

    pub fn endpoint(&self) -> &str {
        &self.name
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Registry
// ────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Finished {
    Completed,
    Timeout,
    Cancelled,
}

struct Pending {
    corr_id: String,
    method: String,
    mode: RpcMode,
    reply_tx: mpsc::UnboundedSender<CallReplyEnvelope>,
    started: Instant,
    delivered: bool,
    timer: Option<tokio::task::AbortHandle>,
}

enum Queued {
    Call(IncomingCall, Instant),
    Cancel(String, CancelReason),
}

struct HostSlot {
    host_id: u64,
    spec: EndpointSpec,
    shared: Arc<HostShared>,
    queue: VecDeque<Queued>,
    queued_calls: usize,
    pending: HashMap<String, Pending>,
    finished: VecDeque<(String, Finished, String)>,
    closed: bool,
}

impl HostSlot {
    fn remember(&mut self, call_id: String, how: Finished, method: &str) {
        if self.finished.len() >= FINISHED_MEMORY {
            self.finished.pop_front();
        }
        self.finished.push_back((call_id, how, method.to_string()));
    }

    fn finished_as(&self, call_id: &str) -> Option<(Finished, String)> {
        self.finished
            .iter()
            .rev()
            .find(|(id, _, _)| id == call_id)
            .map(|(_, f, m)| (*f, m.clone()))
    }

    fn mode_for(&self, method: &str, requested: RpcMode) -> Option<RpcMode> {
        if self.spec.methods.is_empty() {
            return Some(requested);
        }
        self.spec
            .methods
            .iter()
            .find(|m| m.name == method)
            .map(|m| m.mode)
    }

    /// Calls at the host: queued, or delivered and not yet answered
    /// (fire-and-forget calls count until delivered).
    fn in_flight(&self) -> usize {
        let queued_ff = self
            .queue
            .iter()
            .filter(|q| matches!(q, Queued::Call(c, _) if c.mode == RpcMode::FireAndForget))
            .count();
        self.pending.len() + queued_ff
    }

    /// Remove a still-queued call. Returns true when it was undelivered.
    fn unqueue(&mut self, call_id: &str) -> bool {
        let before = self.queue.len();
        self.queue
            .retain(|q| !matches!(q, Queued::Call(c, _) if c.call_id == call_id));
        let removed = before != self.queue.len();
        if removed {
            self.queued_calls = self.queued_calls.saturating_sub(1);
        }
        removed
    }
}

struct HeldCall {
    call_id: String,
    caller_node: String,
    req: CallRequestEnvelope,
    reply_tx: mpsc::UnboundedSender<CallReplyEnvelope>,
    started: Instant,
    call_deadline: Instant,
    timer: Option<tokio::task::AbortHandle>,
}

#[derive(Default)]
struct RegistryState {
    hosts: HashMap<String, HostSlot>,
    held: HashMap<String, Vec<HeldCall>>,
}

enum Pop {
    Event(HostEvent),
    Empty,
    Ended,
}

#[derive(Default)]
struct OutcomeCounts {
    calls: u64,
    ok: u64,
    errors: BTreeMap<String, u64>,
    late: u64,
}

struct MethodStats {
    counts: OutcomeCounts,
    hist: Option<hdrhistogram::Histogram<u64>>,
    adapters: BTreeMap<String, OutcomeCounts>,
}

impl Default for MethodStats {
    fn default() -> Self {
        Self {
            counts: OutcomeCounts::default(),
            hist: hdrhistogram::Histogram::new_with_bounds(1, LATENCY_MAX_US, 2).ok(),
            adapters: BTreeMap::new(),
        }
    }
}

enum Outcome {
    Ok(u64),
    Err(CallErrorKind),
    Late,
}

/// Per-session endpoint registry. Owned by `SessionControl`.
pub(crate) struct EndpointRegistry {
    session_id: String,
    me: Weak<EndpointRegistry>,
    state: parking_lot::Mutex<RegistryState>,
    presence_tx: broadcast::Sender<EndpointPresence>,
    closed: AtomicBool,
    next_host_id: AtomicU64,
    stats: parking_lot::Mutex<BTreeMap<(String, String), MethodStats>>,
    /// Set once any call is dispatched to a `HostKind::Agent` endpoint.
    agent_hit: AtomicBool,
}

fn call_id_for(caller_node: &str, corr_id: &str) -> String {
    format!("{caller_node}/{corr_id}")
}

fn err_env(corr_id: &str, kind: CallErrorKind, message: impl Into<String>) -> CallReplyEnvelope {
    CallReplyEnvelope::error(corr_id, kind, message)
}

impl EndpointRegistry {
    pub(crate) fn new(session_id: impl Into<String>) -> Arc<Self> {
        let session_id = session_id.into();
        Arc::new_cyclic(|me| Self {
            session_id,
            me: me.clone(),
            state: parking_lot::Mutex::new(RegistryState::default()),
            presence_tx: broadcast::channel(PRESENCE_CAPACITY).0,
            closed: AtomicBool::new(false),
            next_host_id: AtomicU64::new(1),
            stats: parking_lot::Mutex::new(BTreeMap::new()),
            agent_hit: AtomicBool::new(false),
        })
    }

    pub(crate) fn agent_hit(&self) -> bool {
        self.agent_hit.load(Ordering::Relaxed)
    }

    fn presence(&self, name: &str, hosted: bool) {
        let _ = self.presence_tx.send(EndpointPresence {
            name: name.to_string(),
            hosted,
        });
    }

    pub(crate) fn watch(&self) -> broadcast::Receiver<EndpointPresence> {
        self.presence_tx.subscribe()
    }

    pub(crate) fn has(&self, name: &str) -> bool {
        self.state
            .lock()
            .hosts
            .get(name)
            .map(|s| !s.closed)
            .unwrap_or(false)
    }

    fn host_is_current(&self, name: &str, host_id: u64) -> bool {
        self.state
            .lock()
            .hosts
            .get(name)
            .map(|s| s.host_id == host_id && !s.closed)
            .unwrap_or(false)
    }

    pub(crate) fn list(&self) -> Vec<EndpointInfo> {
        let state = self.state.lock();
        let mut out: Vec<EndpointInfo> = state
            .hosts
            .values()
            .filter(|s| !s.closed)
            .map(|s| EndpointInfo {
                name: s.spec.name.clone(),
                methods: s
                    .spec
                    .methods
                    .iter()
                    .map(|m| EndpointMethodInfo {
                        name: m.name.clone(),
                        mode: m.mode,
                    })
                    .collect(),
                host_kind: s.spec.host_kind,
                adapter: s.spec.adapter.clone(),
                pending: s.pending.len(),
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Validate an endpoint id independent of the session's nodes.
    pub(crate) fn valid_name(name: &str) -> bool {
        !name.is_empty()
            && !name.starts_with("__")
            && !name.contains('.')
            && !name.contains('/')
            && !RESERVED_ENDPOINT_NAMES.contains(&name)
    }

    pub(crate) fn host(
        &self,
        spec: EndpointSpec,
        takeover: bool,
        is_node: impl Fn(&str) -> bool,
    ) -> Result<EndpointHost, EndpointError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(EndpointError::SessionClosed);
        }
        if !Self::valid_name(&spec.name) || is_node(&spec.name) {
            return Err(EndpointError::NameConflict);
        }
        let name = spec.name.clone();
        let host_id = self.next_host_id.fetch_add(1, Ordering::Relaxed);
        let shared = Arc::new(HostShared {
            notify: Notify::new(),
            end_reason: parking_lot::Mutex::new(None),
        });
        let mut replaced = false;
        let held;
        {
            let mut state = self.state.lock();
            if let Some(existing) = state.hosts.get(&name) {
                if !existing.closed && !takeover {
                    return Err(EndpointError::AlreadyHosted);
                }
                let old = state.hosts.remove(&name).expect("present");
                replaced = !old.closed;
                self.retire(old, if takeover { "taken_over" } else { "unhosted" });
            }
            state.hosts.insert(
                name.clone(),
                HostSlot {
                    host_id,
                    spec: spec.clone(),
                    shared: shared.clone(),
                    queue: VecDeque::new(),
                    queued_calls: 0,
                    pending: HashMap::new(),
                    finished: VecDeque::new(),
                    closed: false,
                },
            );
            held = state.held.remove(&name).unwrap_or_default();
        }
        if replaced {
            self.presence(&name, false);
        }
        self.presence(&name, true);
        tracing::info!(
            session = %self.session_id,
            endpoint = %name,
            host_kind = spec.host_kind.as_str(),
            takeover,
            "endpoint hosted"
        );
        // Calls waiting in `hold_ms` go to the new host.
        for h in held {
            if let Some(t) = &h.timer {
                t.abort();
            }
            let now = Instant::now();
            if h.call_deadline <= now {
                self.record(
                    &name,
                    &h.req.method,
                    None,
                    Outcome::Err(CallErrorKind::Timeout),
                );
                let _ = h.reply_tx.send(err_env(
                    &h.req.corr_id,
                    CallErrorKind::Timeout,
                    "deadline expired",
                ));
                continue;
            }
            self.dispatch(
                &name,
                h.call_id,
                &h.caller_node,
                h.req,
                h.reply_tx,
                h.started,
                h.call_deadline,
            );
        }
        Ok(EndpointHost {
            registry: self.me.clone(),
            name,
            host_id,
            shared,
            spec,
        })
    }

    /// Fail a removed slot's work and wake its host.
    fn retire(&self, mut slot: HostSlot, reason: &'static str) {
        for (_, p) in slot.pending.drain() {
            if let Some(t) = p.timer {
                t.abort();
            }
            self.record(
                &slot.spec.name,
                &p.method,
                slot.spec.adapter.as_deref(),
                Outcome::Err(CallErrorKind::ResponderGone),
            );
            let _ = p.reply_tx.send(err_env(
                &p.corr_id,
                CallErrorKind::ResponderGone,
                format!("endpoint '{}' host lost ({reason})", slot.spec.name),
            ));
        }
        slot.queue.clear();
        *slot.shared.end_reason.lock() = Some(reason);
        slot.shared.notify.notify_one();
    }

    fn remove_host(&self, name: &str, host_id: u64, reason: &'static str) {
        let removed = {
            let mut state = self.state.lock();
            match state.hosts.get(name) {
                Some(s) if s.host_id == host_id => state.hosts.remove(name),
                _ => None,
            }
        };
        if let Some(slot) = removed {
            let was_live = !slot.closed;
            self.retire(slot, reason);
            if was_live {
                self.presence(name, false);
                tracing::info!(session = %self.session_id, endpoint = %name, reason, "endpoint unhosted");
            }
        }
    }

    fn pop_event(&self, name: &str, host_id: u64) -> Pop {
        let mut state = self.state.lock();
        let Some(slot) = state.hosts.get_mut(name).filter(|s| s.host_id == host_id) else {
            return Pop::Ended;
        };
        match slot.queue.pop_front() {
            Some(Queued::Call(mut call, deadline)) => {
                slot.queued_calls = slot.queued_calls.saturating_sub(1);
                call.deadline_ms = deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis() as u64;
                if let Some(p) = slot.pending.get_mut(&call.call_id) {
                    p.delivered = true;
                }
                Pop::Event(HostEvent::Call(call))
            }
            Some(Queued::Cancel(call_id, reason)) => {
                Pop::Event(HostEvent::Cancelled { call_id, reason })
            }
            None if slot.closed => Pop::Ended,
            None => Pop::Empty,
        }
    }

    /// Start a call. Never blocks.
    pub(crate) fn call(&self, caller_node: &str, req: CallRequestEnvelope) -> EndpointCallStream {
        if req.cancel {
            self.cancel_call(caller_node, &req.corr_id, CancelReason::Caller);
            return EndpointCallStream::empty(caller_node, &req.corr_id);
        }
        let endpoint = req.endpoint.clone();
        let method = req.method.clone();
        self.record_issued(&endpoint, &method);
        let corr_id = req.corr_id.clone();
        if self.closed.load(Ordering::Acquire) {
            self.record(
                &endpoint,
                &method,
                None,
                Outcome::Err(CallErrorKind::Cancelled),
            );
            return EndpointCallStream::immediate(
                caller_node,
                err_env(&corr_id, CallErrorKind::Cancelled, "session closed"),
            );
        }
        let call_id = call_id_for(caller_node, &corr_id);
        let (tx, rx) = mpsc::unbounded_channel();
        let stream = EndpointCallStream::new(rx, Some(self.me.clone()), caller_node, &corr_id);
        let started = Instant::now();
        let call_deadline = started + Duration::from_millis(req.deadline_ms.max(1));

        let hosted = {
            let mut state = self.state.lock();
            let duplicate = state
                .hosts
                .values()
                .any(|s| s.pending.contains_key(&call_id))
                || state
                    .held
                    .values()
                    .any(|v| v.iter().any(|h| h.call_id == call_id));
            if duplicate {
                drop(state);
                self.record(&endpoint, &method, None, Outcome::Err(CallErrorKind::Error));
                let _ = tx.send(err_env(
                    &corr_id,
                    CallErrorKind::Error,
                    "duplicate in-flight corr_id",
                ));
                return stream;
            }
            let hosted = state
                .hosts
                .get(&endpoint)
                .map(|s| !s.closed)
                .unwrap_or(false);
            if !hosted {
                if let Some(hold) = req.hold_ms.filter(|h| *h > 0) {
                    let hold_until = (started + Duration::from_millis(hold)).min(call_deadline);
                    let timer = self.spawn_timer(hold_until, {
                        let endpoint = endpoint.clone();
                        let call_id = call_id.clone();
                        move |reg| reg.expire_hold(&endpoint, &call_id)
                    });
                    state
                        .held
                        .entry(endpoint.clone())
                        .or_default()
                        .push(HeldCall {
                            call_id,
                            caller_node: caller_node.to_string(),
                            req,
                            reply_tx: tx,
                            started,
                            call_deadline,
                            timer,
                        });
                    return stream;
                }
            }
            hosted
        };
        if !hosted {
            self.record(
                &endpoint,
                &method,
                None,
                Outcome::Err(CallErrorKind::NoResponder),
            );
            let _ = tx.send(err_env(
                &corr_id,
                CallErrorKind::NoResponder,
                format!("no host for endpoint '{endpoint}'"),
            ));
            return stream;
        }
        self.dispatch(
            &endpoint,
            call_id,
            caller_node,
            req,
            tx,
            started,
            call_deadline,
        );
        stream
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch(
        &self,
        endpoint: &str,
        call_id: String,
        caller_node: &str,
        req: CallRequestEnvelope,
        tx: mpsc::UnboundedSender<CallReplyEnvelope>,
        started: Instant,
        call_deadline: Instant,
    ) {
        let corr_id = req.corr_id.clone();
        let method = req.method.clone();
        let mut state = self.state.lock();
        let Some(slot) = state.hosts.get_mut(endpoint).filter(|s| !s.closed) else {
            drop(state);
            self.record(
                endpoint,
                &method,
                None,
                Outcome::Err(CallErrorKind::NoResponder),
            );
            let _ = tx.send(err_env(&corr_id, CallErrorKind::NoResponder, "no host"));
            return;
        };
        let adapter = slot.spec.adapter.clone();
        let Some(mode) = slot.mode_for(&method, req.mode) else {
            drop(state);
            self.record(
                endpoint,
                &method,
                adapter.as_deref(),
                Outcome::Err(CallErrorKind::NotFound),
            );
            let _ = tx.send(err_env(
                &corr_id,
                CallErrorKind::NotFound,
                format!("endpoint '{endpoint}' has no method '{method}'"),
            ));
            return;
        };
        if slot.in_flight() >= slot.spec.capacity() {
            drop(state);
            self.record(
                endpoint,
                &method,
                adapter.as_deref(),
                Outcome::Err(CallErrorKind::Overloaded),
            );
            let _ = tx.send(err_env(
                &corr_id,
                CallErrorKind::Overloaded,
                format!("endpoint '{endpoint}' queue is full"),
            ));
            return;
        }
        if slot.spec.host_kind == HostKind::Agent {
            self.agent_hit.store(true, Ordering::Relaxed);
        }
        let incoming = IncomingCall {
            call_id: call_id.clone(),
            caller_node: caller_node.to_string(),
            session_id: self.session_id.clone(),
            method: method.clone(),
            mode,
            args: req.args,
            kwargs: req.kwargs,
            deadline_ms: 0,
            route_hint: req.route_hint,
            adapter_hint: req.adapter_hint,
        };
        slot.queue.push_back(Queued::Call(incoming, call_deadline));
        slot.queued_calls += 1;
        slot.shared.notify.notify_one();
        if mode == RpcMode::FireAndForget {
            // Complete once enqueued; nothing is pending.
            drop(state);
            self.record(
                endpoint,
                &method,
                adapter.as_deref(),
                Outcome::Ok(started.elapsed().as_micros() as u64),
            );
            let _ = tx.send(CallReplyEnvelope::end(&corr_id));
            return;
        }
        let timer = self.spawn_timer(call_deadline, {
            let endpoint = endpoint.to_string();
            let call_id = call_id.clone();
            let host_id = slot.host_id;
            move |reg| reg.expire_call(&endpoint, host_id, &call_id)
        });
        slot.pending.insert(
            call_id,
            Pending {
                corr_id,
                method,
                mode,
                reply_tx: tx,
                started,
                delivered: false,
                timer,
            },
        );
    }

    fn spawn_timer(
        &self,
        at: Instant,
        f: impl FnOnce(Arc<EndpointRegistry>) + Send + 'static,
    ) -> Option<tokio::task::AbortHandle> {
        let me = self.me.clone();
        let handle = tokio::runtime::Handle::try_current().ok()?;
        let task = handle.spawn(async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await;
            if let Some(reg) = me.upgrade() {
                f(reg);
            }
        });
        Some(task.abort_handle())
    }

    fn expire_hold(&self, endpoint: &str, call_id: &str) {
        let held = {
            let mut state = self.state.lock();
            let Some(list) = state.held.get_mut(endpoint) else {
                return;
            };
            let Some(pos) = list.iter().position(|h| h.call_id == call_id) else {
                return;
            };
            let h = list.remove(pos);
            if list.is_empty() {
                state.held.remove(endpoint);
            }
            h
        };
        let timed_out = held.call_deadline <= Instant::now();
        let kind = if timed_out {
            CallErrorKind::Timeout
        } else {
            CallErrorKind::NoResponder
        };
        self.record(endpoint, &held.req.method, None, Outcome::Err(kind));
        let _ = held.reply_tx.send(err_env(
            &held.req.corr_id,
            kind,
            format!("no host for endpoint '{endpoint}' within hold_ms"),
        ));
    }

    fn expire_call(&self, endpoint: &str, host_id: u64, call_id: &str) {
        let (pending, adapter) = {
            let mut state = self.state.lock();
            let Some(slot) = state
                .hosts
                .get_mut(endpoint)
                .filter(|s| s.host_id == host_id)
            else {
                return;
            };
            let Some(p) = slot.pending.remove(call_id) else {
                return;
            };
            slot.remember(call_id.to_string(), Finished::Timeout, &p.method);
            if !slot.unqueue(call_id) {
                slot.queue
                    .push_back(Queued::Cancel(call_id.to_string(), CancelReason::Deadline));
                slot.shared.notify.notify_one();
            }
            (p, slot.spec.adapter.clone())
        };
        self.record(
            endpoint,
            &pending.method,
            adapter.as_deref(),
            Outcome::Err(CallErrorKind::Timeout),
        );
        let _ = pending.reply_tx.send(err_env(
            &pending.corr_id,
            CallErrorKind::Timeout,
            "endpoint call deadline expired",
        ));
    }

    /// Cancel a caller's in-flight call (explicit cancel, stream drop,
    /// barge-in). The host receives `Cancelled { reason }` if the call was
    /// already delivered; a queued call is withdrawn silently.
    pub(crate) fn cancel_call(&self, caller_node: &str, corr_id: &str, reason: CancelReason) {
        let call_id = call_id_for(caller_node, corr_id);
        let mut found: Option<(String, Pending, Option<String>)> = None;
        let mut held_found: Option<HeldCall> = None;
        {
            let mut state = self.state.lock();
            for (name, slot) in state.hosts.iter_mut() {
                if let Some(p) = slot.pending.remove(&call_id) {
                    if let Some(t) = &p.timer {
                        t.abort();
                    }
                    slot.remember(call_id.clone(), Finished::Cancelled, &p.method);
                    if !slot.unqueue(&call_id) {
                        slot.queue
                            .push_back(Queued::Cancel(call_id.clone(), reason));
                        slot.shared.notify.notify_one();
                    }
                    found = Some((name.clone(), p, slot.spec.adapter.clone()));
                    break;
                }
            }
            if found.is_none() {
                for list in state.held.values_mut() {
                    if let Some(pos) = list.iter().position(|h| h.call_id == call_id) {
                        held_found = Some(list.remove(pos));
                        break;
                    }
                }
                state.held.retain(|_, v| !v.is_empty());
            }
        }
        if let Some((endpoint, p, adapter)) = found {
            self.record(
                &endpoint,
                &p.method,
                adapter.as_deref(),
                Outcome::Err(CallErrorKind::Cancelled),
            );
            let _ = p.reply_tx.send(err_env(
                &p.corr_id,
                CallErrorKind::Cancelled,
                "call cancelled",
            ));
        } else if let Some(h) = held_found {
            if let Some(t) = &h.timer {
                t.abort();
            }
            self.record(
                &h.req.endpoint,
                &h.req.method,
                None,
                Outcome::Err(CallErrorKind::Cancelled),
            );
            let _ = h.reply_tx.send(err_env(
                &h.req.corr_id,
                CallErrorKind::Cancelled,
                "call cancelled",
            ));
        }
    }

    fn reply(
        &self,
        name: &str,
        host_id: u64,
        call_id: &str,
        event: CallReplyEvent,
        meta: Option<Value>,
    ) -> Result<(), CallErrorKind> {
        let meta_adapter = meta
            .as_ref()
            .and_then(|m| m.get("adapter"))
            .and_then(|a| a.as_str())
            .map(str::to_owned);
        let (env, tx, terminal, method, adapter, started) = {
            let mut state = self.state.lock();
            let Some(slot) = state.hosts.get_mut(name).filter(|s| s.host_id == host_id) else {
                return Err(CallErrorKind::ResponderGone);
            };
            let adapter = meta_adapter.or_else(|| slot.spec.adapter.clone());
            let Some(p) = slot.pending.get(call_id) else {
                let late = slot.finished_as(call_id);
                drop(state);
                return match late {
                    Some((f, method)) => {
                        self.record(name, &method, adapter.as_deref(), Outcome::Late);
                        Err(if f == Finished::Timeout {
                            CallErrorKind::Timeout
                        } else {
                            CallErrorKind::Cancelled
                        })
                    }
                    None => Err(CallErrorKind::NotFound),
                };
            };
            let terminal = match &event {
                CallReplyEvent::Value { .. } => p.mode == RpcMode::Reply,
                CallReplyEvent::End | CallReplyEvent::Error { .. } => true,
            };
            let env = CallReplyEnvelope {
                corr_id: p.corr_id.clone(),
                event,
                meta,
            };
            let tx = p.reply_tx.clone();
            let method = p.method.clone();
            let started = p.started;
            if terminal {
                if let Some(p) = slot.pending.remove(call_id) {
                    if let Some(t) = p.timer {
                        t.abort();
                    }
                }
                slot.remember(call_id.to_string(), Finished::Completed, &method);
            }
            (env, tx, terminal, method, adapter, started)
        };
        if terminal {
            let outcome = match &env.event {
                CallReplyEvent::Error { error, .. } => Outcome::Err(*error),
                _ => Outcome::Ok(started.elapsed().as_micros() as u64),
            };
            self.record(name, &method, adapter.as_deref(), outcome);
        }
        // A closed caller channel means the caller dropped its stream; its
        // Drop already cancelled the call (or is about to).
        let _ = tx.send(env);
        Ok(())
    }

    /// Session close: cancel everything, end every host.
    pub(crate) fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut failed: Vec<(
            String,
            String,
            Option<String>,
            mpsc::UnboundedSender<CallReplyEnvelope>,
            String,
        )> = Vec::new();
        {
            let mut state = self.state.lock();
            for (name, slot) in state.hosts.iter_mut() {
                let ids: Vec<String> = slot.pending.keys().cloned().collect();
                for id in ids {
                    let p = slot.pending.remove(&id).expect("listed");
                    if let Some(t) = &p.timer {
                        t.abort();
                    }
                    slot.remember(id.clone(), Finished::Cancelled, &p.method);
                    if !slot.unqueue(&id) {
                        slot.queue
                            .push_back(Queued::Cancel(id.clone(), CancelReason::SessionClosed));
                    }
                    failed.push((
                        name.clone(),
                        p.method,
                        slot.spec.adapter.clone(),
                        p.reply_tx,
                        p.corr_id,
                    ));
                }
                slot.queue.retain(|q| matches!(q, Queued::Cancel(..)));
                slot.queued_calls = 0;
                slot.closed = true;
                *slot.shared.end_reason.lock() = Some("session_closed");
                slot.shared.notify.notify_one();
            }
            for (name, list) in state.held.drain() {
                for h in list {
                    if let Some(t) = &h.timer {
                        t.abort();
                    }
                    failed.push((name.clone(), h.req.method, None, h.reply_tx, h.req.corr_id));
                }
            }
        }
        for (name, method, adapter, tx, corr) in failed {
            self.record(
                &name,
                &method,
                adapter.as_deref(),
                Outcome::Err(CallErrorKind::Cancelled),
            );
            let _ = tx.send(err_env(&corr, CallErrorKind::Cancelled, "session closed"));
        }
    }

    // ── Telemetry ───────────────────────────────────────────────────────────

    fn record_issued(&self, endpoint: &str, method: &str) {
        let mut stats = self.stats.lock();
        stats
            .entry((endpoint.to_string(), method.to_string()))
            .or_default()
            .counts
            .calls += 1;
    }

    fn record(&self, endpoint: &str, method: &str, adapter: Option<&str>, outcome: Outcome) {
        let mut stats = self.stats.lock();
        let entry = stats
            .entry((endpoint.to_string(), method.to_string()))
            .or_default();
        let apply = |c: &mut OutcomeCounts, outcome: &Outcome, count_call: bool| {
            if count_call {
                c.calls += 1;
            }
            match outcome {
                Outcome::Ok(_) => c.ok += 1,
                Outcome::Err(kind) => *c.errors.entry(kind.as_str().to_string()).or_default() += 1,
                Outcome::Late => c.late += 1,
            }
        };
        apply(&mut entry.counts, &outcome, false);
        if let Outcome::Ok(us) = outcome {
            if let Some(h) = entry.hist.as_mut() {
                let _ = h.record(us.clamp(1, LATENCY_MAX_US));
            }
        }
        if let Some(adapter) = adapter {
            let per = entry.adapters.entry(adapter.to_string()).or_default();
            // Calls are attributed to an adapter when their outcome is
            // known (the adapter can be chosen per call by the host).
            apply(per, &outcome, !matches!(outcome, Outcome::Late));
        }
    }

    /// Per-endpoint, per-method counters for the telemetry snapshot.
    pub(crate) fn telemetry(&self, histograms: bool) -> Vec<crate::telemetry::EndpointTelemetry> {
        let hosts: HashMap<String, (HostKind, usize, Option<String>)> = self
            .state
            .lock()
            .hosts
            .iter()
            .filter(|(_, s)| !s.closed)
            .map(|(k, s)| {
                (
                    k.clone(),
                    (s.spec.host_kind, s.pending.len(), s.spec.adapter.clone()),
                )
            })
            .collect();
        let stats = self.stats.lock();
        stats
            .iter()
            .map(|((endpoint, method), st)| {
                let host = hosts.get(endpoint);
                crate::telemetry::EndpointTelemetry {
                    endpoint: endpoint.clone(),
                    method: method.clone(),
                    hosted: host.is_some(),
                    host_kind: host.map(|h| h.0),
                    calls: st.counts.calls,
                    ok: st.counts.ok,
                    errors: st.counts.errors.clone(),
                    late: st.counts.late,
                    latency_us: if histograms {
                        st.hist.as_ref().filter(|h| !h.is_empty()).map(|h| {
                            crate::data::LatencyPercentiles {
                                p50_us: h.value_at_quantile(0.50),
                                p95_us: h.value_at_quantile(0.95),
                                p99_us: h.value_at_quantile(0.99),
                                max_us: h.max(),
                            }
                        })
                    } else {
                        None
                    },
                    adapters: st
                        .adapters
                        .iter()
                        .map(|(name, c)| crate::telemetry::EndpointAdapterTelemetry {
                            adapter: name.clone(),
                            calls: c.calls,
                            ok: c.ok,
                            errors: c.errors.clone(),
                            late: c.late,
                        })
                        .collect(),
                }
            })
            .collect()
    }
}
