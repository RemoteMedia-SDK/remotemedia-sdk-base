//! Live pipeline telemetry (openspec: `add-live-pipeline-observer`).
//!
//! Three pieces:
//!
//! - [`SessionDescriptor`] — a serializable description of the *running*
//!   topology (post-negotiation nodes, ports, edges, sources/sinks). Stored on
//!   [`crate::transport::session_control::SessionControl`] so any transport /
//!   observer can read it for the life of the session.
//! - [`SessionTelemetry`] — lock-free per-node / per-edge counters written on
//!   the streaming hot path by `SessionRouter`'s node tasks. Every counter has a
//!   single writer task, so `Relaxed` `fetch_add`s never contend; cells are
//!   cache-line padded and pre-indexed so the hot path does no hashing.
//! - [`publisher`] — a per-session task that, while somebody is subscribed to
//!   the reserved `__telemetry__` tap, turns the counters into cumulative
//!   [`TelemetrySnapshot`]s. It also owns the `__perf__` flush so the existing
//!   per-window perf HUD keeps working, now toggleable at runtime.
//!
//! Cost model: ≈6–8 relaxed single-writer RMWs per hop; latency histograms
//! (≈1 µs per record) only run while observed in [`TelemetryLevel::Auto`].

pub mod descriptor;
pub mod publisher;
pub mod redact;
pub mod size;
pub mod snapshot;

pub use descriptor::{
    EdgeDescriptor, EdgeKind, NodeDescriptor, PortDescriptor, QueueCapacity, SessionDescriptor,
    CLIENT_NODE_ID,
};
pub use size::approx_payload_bytes;
pub use snapshot::{
    EdgeTelemetry, ErrorInfo, NodeStatus, NodeTelemetry, QueueDepth, TelemetrySnapshot,
};

use crate::executor::StreamingScheduler;
use crate::transport::perf_aggregator::PerfAggregator;
use crate::transport::session_control::NodeState;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

/// Version of the descriptor / snapshot wire schema. Bumped on breaking
/// changes; additive changes keep the version and rely on `serde(default)`.
pub const SCHEMA_VERSION: u16 = 1;

/// Default snapshot interval.
pub const DEFAULT_INTERVAL_MS: u32 = 1000;
/// Allowed snapshot interval range.
pub const MIN_INTERVAL_MS: u32 = 250;
pub const MAX_INTERVAL_MS: u32 = 5000;

/// Maximum length of a retained error message.
pub const MAX_ERROR_MESSAGE_LEN: usize = 512;

/// How much telemetry a session collects.
///
/// Counters (frames, bytes, queue depth, errors) are always maintained — they
/// are cheap enough to never gate. The level controls the HDR latency
/// histograms and whether snapshots are produced at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum TelemetryLevel {
    /// No snapshots are published.
    Off = 0,
    /// Snapshots carry counters and queue depths, never latency histograms.
    Counters = 1,
    /// Histograms are enabled exactly while a `__telemetry__` / `__perf__`
    /// subscriber exists (or `REMOTEMEDIA_PERF_TAP` is set).
    #[default]
    Auto = 2,
    /// Histograms always on.
    Full = 3,
}

impl TelemetryLevel {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Off,
            1 => Self::Counters,
            3 => Self::Full,
            _ => Self::Auto,
        }
    }

    /// Parse `off|counters|auto|full` (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "off" | "none" | "0" => Some(Self::Off),
            "counters" | "counter" => Some(Self::Counters),
            "auto" | "" => Some(Self::Auto),
            "full" | "on" | "1" => Some(Self::Full),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Counters => "counters",
            Self::Auto => "auto",
            Self::Full => "full",
        }
    }

    /// Process default from `REMOTEMEDIA_TELEMETRY`.
    pub fn from_env() -> Self {
        std::env::var("REMOTEMEDIA_TELEMETRY")
            .ok()
            .and_then(|v| Self::parse(&v))
            .unwrap_or_default()
    }
}

/// Samples a channel's occupancy without keeping it alive.
pub trait QueueProbe: Send + Sync {
    /// `Some(len)` while the channel is open, `None` once it has closed.
    fn depth(&self) -> Option<usize>;
}

/// A queue that does not exist on this router (always empty).
pub struct NoQueue;

impl QueueProbe for NoQueue {
    fn depth(&self) -> Option<usize> {
        Some(0)
    }
}

impl<T: Send + 'static> QueueProbe for tokio::sync::mpsc::WeakSender<T> {
    fn depth(&self) -> Option<usize> {
        // `upgrade` holds a strong sender only for the duration of this
        // call, so telemetry never delays the teardown cascade.
        self.upgrade()
            .map(|tx| tx.max_capacity().saturating_sub(tx.capacity()))
    }
}

const STATE_ENABLED: u8 = 0;
const STATE_BYPASS: u8 = 1;
const STATE_DISABLED: u8 = 2;

fn state_to_u8(state: NodeState) -> u8 {
    match state {
        NodeState::Enabled => STATE_ENABLED,
        NodeState::Bypass => STATE_BYPASS,
        NodeState::Disabled => STATE_DISABLED,
    }
}

fn state_from_u8(v: u8) -> NodeState {
    match v {
        STATE_BYPASS => NodeState::Bypass,
        STATE_DISABLED => NodeState::Disabled,
        _ => NodeState::Enabled,
    }
}

/// Per-node counters. Padded to two cache lines so adjacent nodes' tasks
/// never false-share.
#[repr(align(128))]
pub struct NodeCell {
    pub(crate) inputs: AtomicU64,
    pub(crate) outputs: AtomicU64,
    pub(crate) errors: AtomicU64,
    pub(crate) bypassed: AtomicU64,
    pub(crate) discarded: AtomicU64,
    pub(crate) busy_us: AtomicU64,
    /// Microseconds since the telemetry epoch at which the in-flight
    /// dispatch started, **plus one** so that `0` means idle.
    pub(crate) in_flight_since_us: AtomicU64,
    /// Epoch-relative microsecond of the most recent error (0 = never).
    pub(crate) last_error_at_us: AtomicU64,
    pub(crate) alive: AtomicBool,
    pub(crate) started: AtomicBool,
    pub(crate) state: AtomicU8,
    last_error: parking_lot::Mutex<Option<ErrorInfo>>,
    /// Error-event coalescing (cold path): last `node_error` publish time and
    /// failures accumulated since.
    error_publish: parking_lot::Mutex<ErrorPublishState>,
    pub(crate) q_input: OnceLock<Box<dyn QueueProbe>>,
    pub(crate) q_filter: OnceLock<Box<dyn QueueProbe>>,
    pub(crate) q_fanout: OnceLock<Box<dyn QueueProbe>>,
}

#[derive(Default)]
struct ErrorPublishState {
    last_publish_us: u64,
    pending: u64,
    pending_message: Option<String>,
}

/// Outcome of [`NodeCell::record_error`]: whether the caller should publish a
/// `node_error` event now, and with how many coalesced failures.
pub struct ErrorPublish {
    pub count: u64,
    pub message: String,
}

impl NodeCell {
    fn new() -> Self {
        Self {
            inputs: AtomicU64::new(0),
            outputs: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            bypassed: AtomicU64::new(0),
            discarded: AtomicU64::new(0),
            busy_us: AtomicU64::new(0),
            in_flight_since_us: AtomicU64::new(0),
            last_error_at_us: AtomicU64::new(0),
            alive: AtomicBool::new(false),
            started: AtomicBool::new(false),
            state: AtomicU8::new(STATE_ENABLED),
            last_error: parking_lot::Mutex::new(None),
            error_publish: parking_lot::Mutex::new(ErrorPublishState::default()),
            q_input: OnceLock::new(),
            q_filter: OnceLock::new(),
            q_fanout: OnceLock::new(),
        }
    }

    /// Cached runtime node state (mirror of `SessionControl::node_state`).
    #[inline]
    pub fn node_state(&self) -> NodeState {
        state_from_u8(self.state.load(Relaxed))
    }

    // Public hot-path hooks for routers outside this crate (e.g. the gRPC
    // transport's router). The core router touches the fields directly.

    #[inline]
    pub fn count_input(&self) {
        self.inputs.fetch_add(1, Relaxed);
    }
    #[inline]
    pub fn count_output(&self) {
        self.outputs.fetch_add(1, Relaxed);
    }
    #[inline]
    pub fn count_bypassed(&self) {
        self.bypassed.fetch_add(1, Relaxed);
    }
    #[inline]
    pub fn count_discarded(&self) {
        self.discarded.fetch_add(1, Relaxed);
    }
    /// Mark a dispatch as started at `now_us` (telemetry epoch).
    #[inline]
    pub fn begin_dispatch(&self, now_us: u64) {
        self.in_flight_since_us.store(now_us + 1, Relaxed);
    }
    /// Mark the in-flight dispatch finished after `elapsed_us`.
    #[inline]
    pub fn end_dispatch(&self, elapsed_us: u64) {
        self.busy_us.fetch_add(elapsed_us, Relaxed);
        self.in_flight_since_us.store(0, Relaxed);
    }
}

/// Per-edge counters.
#[repr(align(64))]
pub struct EdgeCell {
    pub(crate) frames: AtomicU64,
    pub(crate) bytes: AtomicU64,
    pub(crate) stalls: AtomicU64,
    pub(crate) stall_us: AtomicU64,
    pub(crate) drops: AtomicU64,
    /// Sends currently blocked on this edge (0 or 1 in practice: one writer).
    pub(crate) active_stalls: AtomicU32,
}

impl EdgeCell {
    fn new() -> Self {
        Self {
            frames: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            stalls: AtomicU64::new(0),
            stall_us: AtomicU64::new(0),
            drops: AtomicU64::new(0),
            active_stalls: AtomicU32::new(0),
        }
    }

    /// Count one delivered frame of `bytes` approximate payload.
    #[inline]
    pub fn record_frame(&self, bytes: u64) {
        self.frames.fetch_add(1, Relaxed);
        self.bytes.fetch_add(bytes, Relaxed);
    }

    /// A send found the receiver full and is about to block. Counted at the
    /// start so an in-progress stall is visible to observers.
    #[inline]
    pub fn begin_stall(&self) {
        self.stalls.fetch_add(1, Relaxed);
        self.active_stalls.fetch_add(1, Relaxed);
    }

    /// The blocked send from [`Self::begin_stall`] completed after `us`.
    #[inline]
    pub fn end_stall(&self, us: u64) {
        self.stall_us.fetch_add(us, Relaxed);
        self.active_stalls.fetch_sub(1, Relaxed);
    }

    /// Count one completed backpressure stall that lasted `us` microseconds.
    #[inline]
    pub fn record_stall(&self, us: u64) {
        self.begin_stall();
        self.end_stall(us);
    }

    /// Count a frame rejected by a closed receiver or failed queue admission.
    #[inline]
    pub fn record_drop(&self) {
        self.drops.fetch_add(1, Relaxed);
    }
}

/// Telemetry state for one session. Built once by `SessionRouter` from the
/// session's [`SessionDescriptor`]; shared (via `Arc`) with the router's node
/// tasks, the publisher, and `SessionControl`.
pub struct SessionTelemetry {
    session_id: String,
    epoch: Instant,
    level: AtomicU8,
    interval_ms: AtomicU32,
    seq: AtomicU64,
    /// Wakes the publisher when the level / interval changes.
    pub(crate) config_changed: tokio::sync::Notify,
    nodes: Box<[NodeCell]>,
    node_ids: Box<[String]>,
    node_index: HashMap<String, usize>,
    edges: Box<[EdgeCell]>,
    /// `edges[i]` originates at node `edge_from[i]` (None for ingress).
    edge_from: Box<[Option<usize>]>,
    queue_caps: QueueCapacity,
    scheduler: Arc<StreamingScheduler>,
    perf: Arc<PerfAggregator>,
}

impl SessionTelemetry {
    /// Build telemetry cells matching `descriptor`'s nodes and edges.
    pub fn new(
        descriptor: &SessionDescriptor,
        scheduler: Arc<StreamingScheduler>,
        perf: Arc<PerfAggregator>,
    ) -> Self {
        let node_ids: Box<[String]> = descriptor.nodes.iter().map(|n| n.id.clone()).collect();
        let node_index = node_ids
            .iter()
            .enumerate()
            .map(|(i, id)| (id.clone(), i))
            .collect::<HashMap<_, _>>();
        let nodes = (0..node_ids.len()).map(|_| NodeCell::new()).collect();
        let edges = (0..descriptor.edges.len()).map(|_| EdgeCell::new()).collect();
        let edge_from = descriptor
            .edges
            .iter()
            .map(|e| node_index.get(&e.from).copied())
            .collect();
        Self {
            session_id: descriptor.session_id.clone(),
            epoch: Instant::now(),
            level: AtomicU8::new(TelemetryLevel::from_env() as u8),
            interval_ms: AtomicU32::new(
                std::env::var("REMOTEMEDIA_TELEMETRY_INTERVAL_MS")
                    .ok()
                    .and_then(|v| v.parse::<u32>().ok())
                    .map(|ms| ms.clamp(MIN_INTERVAL_MS, MAX_INTERVAL_MS))
                    .unwrap_or(DEFAULT_INTERVAL_MS),
            ),
            seq: AtomicU64::new(0),
            config_changed: tokio::sync::Notify::new(),
            nodes,
            node_ids,
            node_index,
            edges,
            edge_from,
            queue_caps: descriptor
                .nodes
                .first()
                .map(|n| n.queue_capacity.clone())
                .unwrap_or_default(),
            scheduler,
            perf,
        }
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Microseconds since this telemetry handle was created.
    #[inline]
    pub fn now_us(&self) -> u64 {
        self.epoch.elapsed().as_micros() as u64
    }

    pub fn uptime_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    pub fn level(&self) -> TelemetryLevel {
        TelemetryLevel::from_u8(self.level.load(Relaxed))
    }

    pub fn set_level(&self, level: TelemetryLevel) {
        self.level.store(level as u8, Relaxed);
        self.config_changed.notify_waiters();
    }

    pub fn interval_ms(&self) -> u32 {
        self.interval_ms.load(Relaxed)
    }

    /// Set the snapshot interval (clamped to 250–5000 ms).
    pub fn set_interval_ms(&self, ms: u32) {
        self.interval_ms
            .store(ms.clamp(MIN_INTERVAL_MS, MAX_INTERVAL_MS), Relaxed);
        self.config_changed.notify_waiters();
    }

    pub(crate) fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Relaxed) + 1
    }

    pub fn node_index(&self, node_id: &str) -> Option<usize> {
        self.node_index.get(node_id).copied()
    }

    pub fn node_ids(&self) -> &[String] {
        &self.node_ids
    }

    /// Cell for node `idx`. Panics on out-of-range (router bug).
    #[inline]
    pub fn node(&self, idx: usize) -> &NodeCell {
        &self.nodes[idx]
    }

    #[inline]
    pub fn edge(&self, idx: usize) -> Option<&EdgeCell> {
        self.edges.get(idx)
    }

    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    pub(crate) fn edge_from(&self, idx: usize) -> Option<usize> {
        self.edge_from.get(idx).copied().flatten()
    }

    pub(crate) fn queue_caps(&self) -> &QueueCapacity {
        &self.queue_caps
    }

    pub fn scheduler(&self) -> &Arc<StreamingScheduler> {
        &self.scheduler
    }

    pub fn perf(&self) -> &Arc<PerfAggregator> {
        &self.perf
    }

    // ── Cold-path helpers used by SessionControl / router ───────────────

    /// Mirror a node-state change into the hot-path cache.
    pub fn set_cached_state(&self, node_id: &str, state: NodeState) {
        if let Some(i) = self.node_index(node_id) {
            self.nodes[i].state.store(state_to_u8(state), Relaxed);
        }
    }

    /// Install weak queue handles for node `idx`. Idempotent (first wins).
    pub fn install_queues(
        &self,
        idx: usize,
        input: Box<dyn QueueProbe>,
        filter: Box<dyn QueueProbe>,
        fanout: Box<dyn QueueProbe>,
    ) {
        let cell = &self.nodes[idx];
        let _ = cell.q_input.set(input);
        let _ = cell.q_filter.set(filter);
        let _ = cell.q_fanout.set(fanout);
    }

    /// Mark node `idx`'s task as running / stopped.
    pub fn set_alive(&self, idx: usize, alive: bool) {
        let cell = &self.nodes[idx];
        if alive {
            cell.started.store(true, Relaxed);
        }
        cell.alive.store(alive, Relaxed);
        if !alive {
            cell.in_flight_since_us.store(0, Relaxed);
        }
    }

    /// Record a dispatch error on node `idx`. Returns `Some` when a
    /// rate-limited `node_error` event should be published now (at most one
    /// per node per second; `count` carries coalesced repeats).
    pub fn record_error(&self, idx: usize, message: &str) -> Option<ErrorPublish> {
        let now_us = self.now_us();
        let cell = &self.nodes[idx];
        cell.errors.fetch_add(1, Relaxed);
        cell.last_error_at_us.store(now_us.max(1), Relaxed);
        let msg = truncate(message, MAX_ERROR_MESSAGE_LEN);
        {
            let mut last = cell.last_error.lock();
            let ts_ms = unix_ms();
            match last.as_mut() {
                Some(info) if info.message == msg => {
                    info.repeat = info.repeat.saturating_add(1);
                    info.ts_ms = ts_ms;
                }
                _ => {
                    *last = Some(ErrorInfo {
                        ts_ms,
                        message: msg.clone(),
                        repeat: 1,
                    })
                }
            }
        }
        let mut pub_state = cell.error_publish.lock();
        pub_state.pending += 1;
        pub_state.pending_message = Some(msg);
        if pub_state.last_publish_us == 0 || now_us.saturating_sub(pub_state.last_publish_us) >= 1_000_000 {
            pub_state.last_publish_us = now_us.max(1);
            let count = std::mem::take(&mut pub_state.pending);
            let message = pub_state.pending_message.take().unwrap_or_default();
            Some(ErrorPublish { count, message })
        } else {
            None
        }
    }

    /// Drain coalesced-but-unpublished error counts whose 1 s window has
    /// elapsed. Called by the publisher so trailing errors are not lost.
    pub(crate) fn take_due_error_events(&self) -> Vec<(usize, ErrorPublish)> {
        let now_us = self.now_us();
        let mut out = Vec::new();
        for (i, cell) in self.nodes.iter().enumerate() {
            let mut st = cell.error_publish.lock();
            if st.pending > 0 && now_us.saturating_sub(st.last_publish_us) >= 1_000_000 {
                st.last_publish_us = now_us.max(1);
                let count = std::mem::take(&mut st.pending);
                let message = st.pending_message.take().unwrap_or_default();
                out.push((i, ErrorPublish { count, message }));
            }
        }
        out
    }

    pub(crate) fn last_error(&self, idx: usize) -> Option<ErrorInfo> {
        self.nodes[idx].last_error.lock().clone()
    }
}

pub(crate) fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

pub(crate) fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
