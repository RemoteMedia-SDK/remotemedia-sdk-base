//! Performance snapshot types
//!
//! Structures for the periodic per-node I/O performance snapshots
//! emitted on the `__perf__` tap channel. The aggregator that
//! produces these lives in `transport::perf_aggregator`.
//!
//! Each `PerfSnapshot` represents one fixed-window roll-up (default
//! 1 s) of dispatch-site measurements. The frontend reads it as a
//! JSON envelope on the `__perf__` tap and renders a HUD without any
//! per-node code changes — the runtime instruments the dispatch
//! point once and every node becomes observable for free.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Latency percentiles in microseconds. Cheap to render numerically
/// (`p50/p95/p99`) and small enough to log periodically without
/// flooding.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencyPercentiles {
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

/// Per-node roll-up for one snapshot window.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NodeStats {
    /// Number of inputs the node received in this window.
    pub inputs: u64,
    /// Number of outputs the node emitted in this window. Differs
    /// from `inputs` for filter/aggregator (`outputs < inputs`) and
    /// fan-out streaming (`outputs > inputs`) nodes.
    pub outputs: u64,

    /// Latency from input arrival to *each* output emission. For
    /// streaming nodes that yield N outputs per input, this includes
    /// every yield, so p99 captures both fast and slow tokens.
    pub latency_us: LatencyPercentiles,

    /// Latency from input arrival to the *first* output emission.
    /// This is what TTFT-style metrics actually want — the moment a
    /// streaming reply starts speaking, regardless of how long the
    /// rest of the stream takes.
    pub first_output_latency_us: LatencyPercentiles,
}

/// Snapshot envelope. Published as `RuntimeData::Json` on the
/// `__perf__` tap once per `window_ms`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerfSnapshot {
    /// Discriminator so the frontend can route this alongside other
    /// `__perf__` event shapes added later (turn metrics, health
    /// flags, etc.) without breaking schemas.
    #[serde(rename = "kind")]
    pub kind: PerfEventKind,
    pub session_id: String,
    /// Wall-clock timestamp this snapshot represents (ms since
    /// epoch). The frontend uses this for sparkline x-axis
    /// alignment.
    pub ts_ms: u64,
    /// Length of the roll-up window in milliseconds. Combined with
    /// `inputs`/`outputs` lets the consumer compute throughput.
    pub window_ms: u32,
    /// Per-node stats keyed by `node_id`.
    pub nodes: HashMap<String, NodeStats>,
}

/// Tag for snapshot envelope variants. We start with one and grow
/// the enum (turn metrics, health flags) without reshaping the JSON.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PerfEventKind {
    PerfSnapshot,
}

impl Default for PerfEventKind {
    fn default() -> Self {
        Self::PerfSnapshot
    }
}
