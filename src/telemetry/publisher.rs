//! Per-session telemetry publisher task.
//!
//! Replaces the old conditional `__perf__` flush task. Spawned for every
//! session; each tick costs two atomic loads when nobody is watching.
//!
//! Each tick:
//! 1. emits any coalesced `node_error` events whose 1 s window elapsed;
//! 2. decides whether latency histograms should run (level + subscribers);
//! 3. flushes the perf aggregator on its window (single reset owner) and
//!    publishes the unchanged `PerfSnapshot` on `__perf__` if subscribed;
//! 4. if `__telemetry__` has subscribers, builds and publishes a cumulative
//!    [`TelemetrySnapshot`].
//!
//! On shutdown (after node tasks have stopped) a final snapshot with
//! `final: true` is published so observers keep the end state.

use super::snapshot::{derive_status, StatusInputs, SNAPSHOT_KIND};
use super::{
    EdgeTelemetry, NodeTelemetry, QueueDepth, SessionTelemetry, TelemetryLevel, TelemetrySnapshot,
};
use crate::data::{PerfNodeStats, RuntimeData};
use crate::transport::perf_aggregator::PerfAggregator;
use crate::transport::session_control::{SessionControl, PERF_PORT, TELEMETRY_PORT};
use std::collections::HashMap;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// Mutable state carried between ticks.
#[derive(Default)]
pub struct PublisherState {
    prev_stalls: Vec<u64>,
    /// Latest per-window perf stats (from the most recent flush).
    latency: Option<HashMap<String, PerfNodeStats>>,
    latency_window_ms: u32,
    last_perf_flush: Option<Instant>,
}

/// Spawn the publisher for `tel`. Exits (after a final snapshot) when
/// `shutdown` is notified.
pub fn spawn_publisher(
    tel: Arc<SessionTelemetry>,
    control: Option<Arc<SessionControl>>,
    shutdown: Arc<Notify>,
) -> JoinHandle<()> {
    // `REMOTEMEDIA_PERF_TAP=1` keeps its historical meaning: histograms on
    // for the whole session, flushed on the aggregator's own window.
    let perf_env = PerfAggregator::enabled_from_env();
    tokio::spawn(async move {
        let mut state = PublisherState::default();
        loop {
            let interval = Duration::from_millis(tel.interval_ms() as u64);
            tokio::select! {
                biased;
                _ = shutdown.notified() => break,
                _ = tel.config_changed.notified() => {}
                _ = tokio::time::sleep(interval) => {}
            }
            tick(&tel, control.as_deref(), perf_env, &mut state, false).await;
        }
        tick(&tel, control.as_deref(), perf_env, &mut state, true).await;
    })
}

/// One publisher iteration. `is_final` forces a perf flush and marks the
/// telemetry snapshot final.
pub async fn tick(
    tel: &SessionTelemetry,
    control: Option<&SessionControl>,
    perf_env: bool,
    state: &mut PublisherState,
    is_final: bool,
) {
    let Some(ctrl) = control else {
        // No bus → nobody can observe; still honour the env perf flag so
        // `SessionHandle::perf_aggregator().peek_snapshot()` tooling works.
        tel.perf()
            .set_enabled(perf_env || tel.level() == TelemetryLevel::Full);
        return;
    };

    for (idx, ev) in tel.take_due_error_events() {
        publish_node_error(ctrl, &tel.node_ids()[idx], &ev.message, ev.count);
    }

    let n_tel = ctrl.tap_receiver_count(TELEMETRY_PORT, None);
    let n_perf = ctrl.tap_receiver_count(PERF_PORT, None);
    let level = tel.level();

    let want_hist = match level {
        TelemetryLevel::Off | TelemetryLevel::Counters => perf_env,
        TelemetryLevel::Auto => perf_env || n_tel > 0 || n_perf > 0,
        TelemetryLevel::Full => true,
    };
    let perf = tel.perf();
    perf.set_enabled(want_hist);

    if want_hist {
        let window = if perf_env {
            perf.window_ms()
        } else {
            tel.interval_ms()
        };
        let due = state
            .last_perf_flush
            .map(|t| {
                t.elapsed()
                    >= Duration::from_millis(window as u64)
                        .saturating_sub(Duration::from_millis(20))
            })
            .unwrap_or(true);
        // When histograms were just turned on, the first window is partial;
        // `last_perf_flush == None` handles that by flushing immediately
        // (empty) to establish the window start.
        if due || is_final {
            let had_activity = perf.has_activity();
            let snap = perf.flush_snapshot();
            if had_activity && n_perf > 0 {
                match serde_json::to_value(&snap) {
                    Ok(v) => ctrl.publish_tap(PERF_PORT, None, RuntimeData::Json(v)),
                    Err(e) => tracing::warn!("perf: failed to serialize snapshot: {}", e),
                }
            }
            state.latency = if had_activity { Some(snap.nodes) } else { None };
            state.latency_window_ms = window;
            state.last_perf_flush = Some(Instant::now());
        }
    } else {
        state.latency = None;
        state.last_perf_flush = None;
    }

    if level == TelemetryLevel::Off || n_tel == 0 {
        return;
    }

    let circuit: HashMap<String, bool> = tel
        .scheduler()
        .get_all_node_stats()
        .await
        .into_iter()
        .map(|(k, v)| (k, v.circuit_breaker_open))
        .collect();

    let mut snap = build_snapshot(tel, state, &circuit, want_hist, is_final);
    snap.endpoints = ctrl.endpoint_telemetry(want_hist);
    match serde_json::to_value(&snap) {
        Ok(v) => ctrl.publish_tap(TELEMETRY_PORT, None, RuntimeData::Json(v)),
        Err(e) => tracing::warn!("telemetry: failed to serialize snapshot: {}", e),
    }
}

/// Build a snapshot from the current counters. Public within the crate so
/// tests and synchronous callers can sample without the task.
pub(crate) fn build_snapshot(
    tel: &SessionTelemetry,
    state: &mut PublisherState,
    circuit: &HashMap<String, bool>,
    histograms: bool,
    is_final: bool,
) -> TelemetrySnapshot {
    let now_us = tel.now_us();
    let caps = tel.queue_caps().clone();

    // Edges first: we need per-node "an outgoing edge stalled" for status.
    let n_edges = tel.edge_count();
    if state.prev_stalls.len() != n_edges {
        state.prev_stalls = vec![0; n_edges];
    }
    let mut out_stalled = vec![false; tel.node_ids().len()];
    let mut edges = Vec::with_capacity(n_edges);
    for i in 0..n_edges {
        let cell = tel.edge(i).expect("edge index in range");
        let stalls = cell.stalls.load(Relaxed);
        let stalled_now = cell.active_stalls.load(Relaxed) > 0;
        if stalls > state.prev_stalls[i] || stalled_now {
            if let Some(from) = tel.edge_from(i) {
                out_stalled[from] = true;
            }
        }
        state.prev_stalls[i] = stalls;
        edges.push(EdgeTelemetry {
            idx: i as u32,
            frames: cell.frames.load(Relaxed),
            bytes: cell.bytes.load(Relaxed),
            stalls,
            stall_us: cell.stall_us.load(Relaxed),
            drops: cell.drops.load(Relaxed),
            stalled_now,
        });
    }

    let nodes = tel
        .node_ids()
        .iter()
        .enumerate()
        .map(|(idx, id)| {
            let cell = tel.node(idx);
            let in_flight_since = cell.in_flight_since_us.load(Relaxed);
            let in_flight_ms =
                (in_flight_since > 0).then(|| now_us.saturating_sub(in_flight_since - 1) / 1000);
            let last_err_at = cell.last_error_at_us.load(Relaxed);
            let since_error_ms =
                (last_err_at > 0).then(|| now_us.saturating_sub(last_err_at) / 1000);
            let q = QueueDepth {
                input: probe(&cell.q_input),
                filter: probe(&cell.q_filter),
                fanout: probe(&cell.q_fanout),
            };
            let perf = state.latency.as_ref().and_then(|m| m.get(id));
            let latency_us = perf.filter(|p| p.outputs > 0).map(|p| p.latency_us.clone());
            let first_output_latency_us = perf
                .filter(|p| p.first_output_latency_us.max_us > 0)
                .map(|p| p.first_output_latency_us.clone());
            let node_state = cell.node_state();
            let circuit_open = circuit.get(id).copied().unwrap_or(false);
            let status = derive_status(&StatusInputs {
                started: cell.started.load(Relaxed),
                alive: cell.alive.load(Relaxed),
                state: Some(node_state),
                circuit_open,
                since_error_ms,
                in_flight_ms,
                p95_us: latency_us.as_ref().map(|l| l.p95_us),
                filter_depth: q.filter,
                filter_capacity: caps.filter,
                input_depth: q.input,
                input_capacity: caps.input,
                out_edge_stalled: out_stalled[idx],
            });
            NodeTelemetry {
                id: id.clone(),
                state: node_state,
                status,
                inputs: cell.inputs.load(Relaxed),
                outputs: cell.outputs.load(Relaxed),
                errors: cell.errors.load(Relaxed),
                bypassed: cell.bypassed.load(Relaxed),
                discarded: cell.discarded.load(Relaxed),
                busy_us: cell.busy_us.load(Relaxed),
                in_flight_ms,
                q,
                latency_us,
                first_output_latency_us,
                circuit_open,
                last_error: tel.last_error(idx),
                llm_fallbacks: cell.llm_fallbacks.load(Relaxed),
                logs: super::node_logs::NodeLogStore::global().counts(tel.session_id(), id),
            }
        })
        .collect();

    TelemetrySnapshot {
        kind: SNAPSHOT_KIND.to_string(),
        schema_version: super::SCHEMA_VERSION,
        session_id: tel.session_id().to_string(),
        seq: tel.next_seq(),
        ts_ms: super::unix_ms(),
        uptime_ms: tel.uptime_ms(),
        interval_ms: tel.interval_ms(),
        window_ms: state.latency_window_ms,
        level: tel.level(),
        histograms,
        is_final,
        nodes,
        edges,
        endpoints: Vec::new(),
    }
}

fn probe(slot: &std::sync::OnceLock<Box<dyn super::QueueProbe>>) -> u32 {
    slot.get()
        .and_then(|p| p.depth())
        .map(|d| d.min(u32::MAX as usize) as u32)
        .unwrap_or(0)
}

/// Count a dispatch or admission error and publish a rate-limited node event.
pub fn record_node_error(
    telemetry: &SessionTelemetry,
    index: Option<usize>,
    control: Option<&SessionControl>,
    node_id: &str,
    message: &str,
) {
    let Some(index) = index else { return };
    if let Some(event) = telemetry.record_error(index, message) {
        if let Some(control) = control {
            publish_node_error(control, node_id, &event.message, event.count);
        }
    }
}

/// Publish a (non-retained) `node_error` event on `__system__`.
pub fn publish_node_error(ctrl: &SessionControl, node_id: &str, message: &str, count: u64) {
    ctrl.publish_system_ephemeral(RuntimeData::Json(serde_json::json!({
        "kind": "node_error",
        "node": node_id,
        "message": message,
        "count": count,
        "ts_ms": super::unix_ms(),
    })));
}
