//! Telemetry snapshot wire types (published as JSON on `__telemetry__.out`).
//!
//! Counters are **cumulative** since session start; clients derive rates from
//! the delta between consecutive snapshots (`seq` / `ts_ms`). A lost or late
//! snapshot therefore loses nothing. Latency percentiles are **per window**
//! (reset at each perf flush).

use crate::data::LatencyPercentiles;
use crate::transport::session_control::NodeState;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// `kind` discriminator value.
pub const SNAPSHOT_KIND: &str = "telemetry_snapshot";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TelemetrySnapshot {
    pub kind: String,
    pub schema_version: u16,
    pub session_id: String,
    pub seq: u64,
    pub ts_ms: u64,
    pub uptime_ms: u64,
    /// Snapshot interval.
    pub interval_ms: u32,
    /// Length of the latency window the percentiles cover, when present.
    pub window_ms: u32,
    pub level: super::TelemetryLevel,
    /// Whether latency histograms were being recorded this window.
    pub histograms: bool,
    /// Last snapshot of a closing session.
    #[serde(rename = "final")]
    pub is_final: bool,
    pub nodes: Vec<NodeTelemetry>,
    pub edges: Vec<EdgeTelemetry>,
    /// Client-hosted endpoint calls, per endpoint and method (omitted while
    /// no endpoint call has been issued in the session).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub endpoints: Vec<EndpointTelemetry>,
}

/// Cumulative call counters for one `(endpoint, method)`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct EndpointTelemetry {
    pub endpoint: String,
    pub method: String,
    /// Whether the endpoint is hosted right now.
    pub hosted: bool,
    /// Kind of the current host, when hosted.
    pub host_kind: Option<crate::transport::endpoint::HostKind>,
    /// Calls issued.
    pub calls: u64,
    /// Calls that completed successfully.
    pub ok: u64,
    /// Failures by kind (`timeout`, `no_responder`, `responder_gone`,
    /// `overloaded`, `cancelled`, `unavailable`, `error`, …).
    pub errors: BTreeMap<String, u64>,
    /// Replies discarded because their call had already finished.
    pub late: u64,
    /// Latency of successful calls (cumulative), when histograms are on.
    pub latency_us: Option<LatencyPercentiles>,
    /// Per-adapter attribution, where the host names the adapter (agent
    /// hosts: `EndpointSpec::adapter` or the reply's `meta.adapter`).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub adapters: Vec<EndpointAdapterTelemetry>,
}

/// Outcome counters attributed to one adapter of an endpoint method.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EndpointAdapterTelemetry {
    pub adapter: String,
    pub calls: u64,
    pub ok: u64,
    pub errors: BTreeMap<String, u64>,
    pub late: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct QueueDepth {
    pub input: u32,
    pub filter: u32,
    pub fanout: u32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ErrorInfo {
    pub ts_ms: u64,
    pub message: String,
    /// How many consecutive times this exact message occurred.
    pub repeat: u32,
}

/// Server-derived status, so every client agrees on what "stalled" means.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    /// Task not started yet (initializing).
    Pending,
    #[default]
    Idle,
    Busy,
    /// Input backlog ≥80% of capacity, or an outgoing edge stalled this window.
    Backpressured,
    /// A single dispatch has been running abnormally long.
    Stalled,
    /// Errors within the last 5 s.
    Errored,
    CircuitOpen,
    Bypassed,
    Disabled,
    /// Node task has exited.
    Stopped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NodeTelemetry {
    pub id: String,
    pub state: NodeState,
    pub status: NodeStatus,
    pub inputs: u64,
    pub outputs: u64,
    pub errors: u64,
    pub bypassed: u64,
    pub discarded: u64,
    /// Cumulative time spent inside `process_streaming_async`.
    pub busy_us: u64,
    /// Age of the in-flight dispatch, if any.
    pub in_flight_ms: Option<u64>,
    pub q: QueueDepth,
    pub latency_us: Option<LatencyPercentiles>,
    pub first_output_latency_us: Option<LatencyPercentiles>,
    pub circuit_open: bool,
    pub last_error: Option<ErrorInfo>,
    /// Turns re-issued to an LLM fallback provider (cumulative).
    pub llm_fallbacks: u64,
}

impl Default for NodeTelemetry {
    fn default() -> Self {
        Self {
            id: String::new(),
            state: NodeState::Enabled,
            status: NodeStatus::Idle,
            inputs: 0,
            outputs: 0,
            errors: 0,
            bypassed: 0,
            discarded: 0,
            busy_us: 0,
            in_flight_ms: None,
            q: QueueDepth::default(),
            latency_us: None,
            first_output_latency_us: None,
            circuit_open: false,
            last_error: None,
            llm_fallbacks: 0,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct EdgeTelemetry {
    pub idx: u32,
    pub frames: u64,
    pub bytes: u64,
    pub stalls: u64,
    pub stall_us: u64,
    pub drops: u64,
    /// A send is blocked on this edge right now.
    pub stalled_now: bool,
}

/// Inputs to [`derive_status`]; kept separate so it is unit-testable.
#[derive(Debug, Clone, Default)]
pub struct StatusInputs {
    pub started: bool,
    pub alive: bool,
    pub state: Option<NodeState>,
    pub circuit_open: bool,
    /// Milliseconds since the most recent error (None = never).
    pub since_error_ms: Option<u64>,
    pub in_flight_ms: Option<u64>,
    /// p95 dispatch latency, when histograms are on.
    pub p95_us: Option<u64>,
    pub filter_depth: u32,
    pub filter_capacity: u32,
    /// Router→node input queue (the whole backlog on routers without a
    /// separate filter stage).
    pub input_depth: u32,
    pub input_capacity: u32,
    /// An outgoing edge recorded new stalls since the previous snapshot.
    pub out_edge_stalled: bool,
}

/// Errors this recent mark a node `errored`.
pub const ERRORED_WINDOW_MS: u64 = 5_000;
/// Minimum in-flight age before a node is `stalled`.
pub const STALL_FLOOR_MS: u64 = 5_000;

pub fn derive_status(i: &StatusInputs) -> NodeStatus {
    if !i.started {
        return NodeStatus::Pending;
    }
    if !i.alive {
        return NodeStatus::Stopped;
    }
    match i.state {
        Some(NodeState::Disabled) => return NodeStatus::Disabled,
        Some(NodeState::Bypass) => return NodeStatus::Bypassed,
        _ => {}
    }
    if i.circuit_open {
        return NodeStatus::CircuitOpen;
    }
    if matches!(i.since_error_ms, Some(ms) if ms < ERRORED_WINDOW_MS) {
        return NodeStatus::Errored;
    }
    if let Some(age) = i.in_flight_ms {
        let threshold = i
            .p95_us
            .map(|p| (p / 1000).saturating_mul(10))
            .unwrap_or(0)
            .max(STALL_FLOOR_MS);
        if age >= threshold {
            return NodeStatus::Stalled;
        }
    }
    let near_full = |depth: u32, cap: u32| cap > 0 && (depth as u64) * 5 >= (cap as u64) * 4;
    let backlog_full =
        near_full(i.filter_depth, i.filter_capacity) || near_full(i.input_depth, i.input_capacity);
    if backlog_full || i.out_edge_stalled {
        return NodeStatus::Backpressured;
    }
    if i.in_flight_ms.is_some() {
        return NodeStatus::Busy;
    }
    NodeStatus::Idle
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> StatusInputs {
        StatusInputs {
            started: true,
            alive: true,
            filter_capacity: 1024,
            ..Default::default()
        }
    }

    #[test]
    fn status_precedence() {
        assert_eq!(derive_status(&StatusInputs::default()), NodeStatus::Pending);
        assert_eq!(derive_status(&base()), NodeStatus::Idle);
        let mut s = base();
        s.in_flight_ms = Some(3);
        assert_eq!(derive_status(&s), NodeStatus::Busy);
        s.out_edge_stalled = true;
        assert_eq!(derive_status(&s), NodeStatus::Backpressured);
        s.in_flight_ms = Some(6_000);
        assert_eq!(derive_status(&s), NodeStatus::Stalled);
        s.since_error_ms = Some(100);
        assert_eq!(derive_status(&s), NodeStatus::Errored);
        s.circuit_open = true;
        assert_eq!(derive_status(&s), NodeStatus::CircuitOpen);
        s.state = Some(NodeState::Bypass);
        assert_eq!(derive_status(&s), NodeStatus::Bypassed);
        s.alive = false;
        assert_eq!(derive_status(&s), NodeStatus::Stopped);
    }

    #[test]
    fn stall_threshold_scales_with_p95() {
        let mut s = base();
        s.in_flight_ms = Some(8_000);
        s.p95_us = Some(1_000_000); // 1 s p95 → 10 s threshold
        assert_eq!(derive_status(&s), NodeStatus::Busy);
        s.in_flight_ms = Some(10_000);
        assert_eq!(derive_status(&s), NodeStatus::Stalled);
    }

    #[test]
    fn filter_backlog_marks_backpressure() {
        let mut s = base();
        s.filter_depth = 820;
        assert_eq!(derive_status(&s), NodeStatus::Backpressured);
        s.filter_depth = 100;
        assert_eq!(derive_status(&s), NodeStatus::Idle);
        s.input_capacity = 16;
        s.input_depth = 16;
        assert_eq!(derive_status(&s), NodeStatus::Backpressured);
    }
}
