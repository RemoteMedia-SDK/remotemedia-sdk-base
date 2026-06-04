//! Per-session performance aggregator
//!
//! Records dispatch-site I/O timings into HDR histograms and emits a
//! single roll-up snapshot per session per window. The aggregator is
//! the runtime side of the `__perf__` tap channel — see
//! [`crate::data::perf`] for the JSON schema.
//!
//! ## Design
//!
//! - One [`PerfAggregator`] instance per session.
//! - Per `node_id`, two HDR histograms: total latency (every output)
//!   and first-output latency (only the first emission per input).
//! - `record(...)` is sub-microsecond — a single relaxed atomic load
//!   for the "enabled" flag, a `DashMap` shard lookup (lock-free read
//!   on the hot "bucket already exists" path), a `parking_lot::Mutex`
//!   lock on the per-node slot (uncontested in steady state — one
//!   writer per node), two `record()` calls. No JSON, no allocation
//!   on the hit path, no awaits. Safe to call from the dispatch hot
//!   path.
//! - The previous `Mutex<HashMap<String, NodeBucket>>` design
//!   serialized every node in a session behind one lock and allocated
//!   a `String` per record (the `entry().or_insert_with()` key path
//!   clones even on hit). The `DashMap<Arc<str>, Mutex<NodeBucket>>`
//!   shape removes both — different nodes in the same session land on
//!   different shards, and `&str` lookups against `Arc<str>` keys
//!   borrow without alloc.
//! - `flush_snapshot()` builds a [`PerfSnapshot`] and **resets** the
//!   histograms in place (`reset()` keeps capacity). One snapshot
//!   covers exactly `window_ms` of activity. Frontend renders the
//!   latest; sparklines are built from a series.
//! - `enable_perf_tap` flag is set at construction. When `false`,
//!   `record()` returns immediately without acquiring any lock.
//!
//! ## Why not store every event
//!
//! At 50 fps audio + LLM token rate + TTS chunk rate, a busy session
//! emits ~200 events/s. Publishing every one as JSON on the tap
//! channel would dominate runtime cost and saturate the WebSocket.
//! The histogram approach keeps memory bounded (~3 KB per node) and
//! costs ~1 µs per record(). The frontend gets richer information
//! (percentiles, not just a stream of dots) for less work.

use crate::data::perf::{LatencyPercentiles, NodeStats, PerfEventKind, PerfSnapshot};
use dashmap::DashMap;
use hdrhistogram::Histogram;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

/// HDR histogram precision. 3 significant figures + 1 µs to 60 s
/// range covers everything from "fast Rust path" up to "LLM stalled".
/// Memory cost: ~3 KB per histogram.
const HDR_PRECISION: u8 = 3;
const HDR_MIN_US: u64 = 1;
const HDR_MAX_US: u64 = 60_000_000;

/// Per-node slot. One per (session, node_id).
struct NodeBucket {
    /// Inputs received in the current window.
    inputs: u64,
    /// Outputs emitted in the current window.
    outputs: u64,
    /// Latency from input arrival to *each* output (us).
    latency: Histogram<u64>,
    /// Latency from input arrival to the *first* output of that
    /// input (us). One sample per input that produced ≥1 output.
    first_output_latency: Histogram<u64>,
}

impl NodeBucket {
    fn new() -> Self {
        Self {
            inputs: 0,
            outputs: 0,
            latency: Histogram::new_with_bounds(HDR_MIN_US, HDR_MAX_US, HDR_PRECISION)
                .expect("HDR histogram bounds valid"),
            first_output_latency: Histogram::new_with_bounds(HDR_MIN_US, HDR_MAX_US, HDR_PRECISION)
                .expect("HDR histogram bounds valid"),
        }
    }

    fn record_input(&mut self) {
        self.inputs = self.inputs.saturating_add(1);
    }

    fn record_output(&mut self, latency_us: u64, is_first: bool) {
        self.outputs = self.outputs.saturating_add(1);
        // `record_correct` clamps at the histogram's max, so a
        // pathological 60+ s value won't poison the snapshot.
        let _ = self
            .latency
            .record(latency_us.clamp(HDR_MIN_US, HDR_MAX_US));
        if is_first {
            let _ = self
                .first_output_latency
                .record(latency_us.clamp(HDR_MIN_US, HDR_MAX_US));
        }
    }

    /// Drain into a snapshot view and reset for the next window.
    fn drain_into(&mut self) -> NodeStats {
        let stats = NodeStats {
            inputs: self.inputs,
            outputs: self.outputs,
            latency_us: percentiles(&self.latency),
            first_output_latency_us: percentiles(&self.first_output_latency),
        };
        self.inputs = 0;
        self.outputs = 0;
        self.latency.reset();
        self.first_output_latency.reset();
        stats
    }

    /// Build a snapshot view **without** resetting. The histograms keep
    /// accumulating; subsequent `peek_into` calls return strictly more
    /// data. Used by external tooling to read end-of-run merged
    /// percentiles without racing the periodic flush task.
    fn peek_into(&self) -> NodeStats {
        NodeStats {
            inputs: self.inputs,
            outputs: self.outputs,
            latency_us: percentiles(&self.latency),
            first_output_latency_us: percentiles(&self.first_output_latency),
        }
    }
}

fn percentiles(h: &Histogram<u64>) -> LatencyPercentiles {
    if h.is_empty() {
        return LatencyPercentiles::default();
    }
    LatencyPercentiles {
        p50_us: h.value_at_quantile(0.50),
        p95_us: h.value_at_quantile(0.95),
        p99_us: h.value_at_quantile(0.99),
        max_us: h.max(),
    }
}

/// Per-session performance aggregator.
///
/// Construct one and share `Arc<PerfAggregator>` between the session
/// router (which calls [`Self::record_input`] / [`Self::record_output`]
/// from `spawn_node_pipeline`) and the periodic flush task.
pub struct PerfAggregator {
    session_id: String,
    enabled: AtomicBool,
    /// `node_id → NodeBucket`. `DashMap` shards the outer map so
    /// different nodes don't serialize, and `Arc<str>` keys let
    /// `&str` lookups borrow without alloc on the hit path. The
    /// per-slot `Mutex` is uncontested in steady state (one writer
    /// per node).
    buckets: DashMap<Arc<str>, Mutex<NodeBucket>>,
    /// Window length used in emitted snapshots. Set once at
    /// construction; aggregator does not enforce — the flush task
    /// owns the timer.
    window_ms: u32,
    /// Sample stride. `1` means "record every event" (default);
    /// `N` means "record 1 in N". A monotonic counter is incremented
    /// on every `record_*` call regardless of `enabled`; the modulo
    /// gate skips the lock for `(N-1)/N` of the calls. P50/P95 stay
    /// accurate at any practical N; P99 needs `N ≤ 100` to be
    /// meaningful at 200 events/s/session.
    sample_stride: u32,
    /// Monotonic counter for the sample-stride gate. Wraps. Atomic
    /// add is the only cost paid by sampled-out calls (~5 ns).
    sample_counter: std::sync::atomic::AtomicU64,
}

impl PerfAggregator {
    pub fn new(session_id: String, enabled: bool, window_ms: u32) -> Self {
        Self::with_sample_stride(session_id, enabled, window_ms, 1)
    }

    /// Like [`Self::new`] but with an explicit sample stride. See
    /// [`Self::sample_stride_from_env`] for the env-var hook.
    pub fn with_sample_stride(
        session_id: String,
        enabled: bool,
        window_ms: u32,
        sample_stride: u32,
    ) -> Self {
        Self {
            session_id,
            enabled: AtomicBool::new(enabled),
            buckets: DashMap::new(),
            window_ms,
            sample_stride: sample_stride.max(1),
            sample_counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Returns `true` if this call should be recorded under the
    /// current sample stride. Always `true` when stride is 1.
    #[inline]
    fn sample_admit(&self) -> bool {
        if self.sample_stride <= 1 {
            return true;
        }
        let n = self.sample_counter.fetch_add(1, Ordering::Relaxed);
        n % (self.sample_stride as u64) == 0
    }

    /// Returns `true` if the aggregator is recording. Hot path uses
    /// this to skip the per-record lock entirely when disabled.
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Enable/disable at runtime (e.g., from a control-bus toggle).
    pub fn set_enabled(&self, on: bool) {
        self.enabled.store(on, Ordering::Relaxed);
    }

    /// Get or create a bucket for `node_id`. Hot path: shard lookup
    /// against an `Arc<str>` key via `Borrow<str>` — no alloc on hit.
    /// Cold path (first time we see this `node_id`): single
    /// `Arc::from(&str)` + insert.
    #[inline]
    fn bucket_for(
        &self,
        node_id: &str,
    ) -> dashmap::mapref::one::Ref<'_, Arc<str>, Mutex<NodeBucket>> {
        if let Some(entry) = self.buckets.get(node_id) {
            return entry;
        }
        // Insert if absent. Racing inserts converge (DashMap entry
        // is per-shard atomic). Allocation happens at most once per
        // (session, node_id).
        self.buckets
            .entry(Arc::<str>::from(node_id))
            .or_insert_with(|| Mutex::new(NodeBucket::new()));
        self.buckets
            .get(node_id)
            .expect("just inserted above; entry must exist")
    }

    /// Record that `node_id` accepted an input. Cheap when disabled.
    #[inline]
    pub fn record_input(&self, node_id: &str) {
        if !self.is_enabled() {
            return;
        }
        if !self.sample_admit() {
            return;
        }
        let bucket = self.bucket_for(node_id);
        bucket.value().lock().record_input();
    }

    /// Record that `node_id` emitted an output `latency_us`
    /// microseconds after its input arrived. `is_first` flags the
    /// first emission for that input (used for first-output
    /// percentiles).
    ///
    /// `is_first` is honored even on sampled-out calls in the sense
    /// that the *first* output for an input is still always recorded
    /// from the caller's side — sampling here only drops *recording*,
    /// not the caller's first-emit bookkeeping. P50/P95 of
    /// `first_output_latency_us` stay accurate at any stride; P99 of
    /// `latency_us` (per-output) degrades with stride.
    #[inline]
    pub fn record_output(&self, node_id: &str, latency_us: u64, is_first: bool) {
        if !self.is_enabled() {
            return;
        }
        // First-output samples are rare (one per input) and
        // load-bearing for TTFA-style metrics — never drop them.
        if !is_first && !self.sample_admit() {
            return;
        }
        let bucket = self.bucket_for(node_id);
        bucket.value().lock().record_output(latency_us, is_first);
    }

    /// Drain all node buckets into a [`PerfSnapshot`] and reset
    /// histograms. Called by the periodic flush task; safe to call
    /// from any thread.
    pub fn flush_snapshot(&self) -> PerfSnapshot {
        let ts_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let nodes: HashMap<String, NodeStats> = self
            .buckets
            .iter()
            .map(|entry| {
                let id = entry.key().as_ref().to_string();
                let stats = entry.value().lock().drain_into();
                (id, stats)
            })
            .collect();

        PerfSnapshot {
            kind: PerfEventKind::PerfSnapshot,
            session_id: self.session_id.clone(),
            ts_ms,
            window_ms: self.window_ms,
            nodes,
        }
    }

    /// Build a snapshot **without** resetting histograms. Unlike
    /// [`Self::flush_snapshot`], repeated calls return strictly more
    /// data, so tooling can use this to capture an entire execution
    /// window in one merged HDR histogram instead of trying to combine
    /// percentile-of-percentiles across periodic flushes.
    ///
    /// Note: this does not interact with the periodic flush task — if
    /// the flush task is also running, it will continue to reset on
    /// its own cadence. For deterministic measurements, either
    /// disable the periodic flush (set window very large) or take the
    /// peek before the first periodic flush fires.
    pub fn peek_snapshot(&self) -> PerfSnapshot {
        let ts_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let nodes: HashMap<String, NodeStats> = self
            .buckets
            .iter()
            .map(|entry| {
                let id = entry.key().as_ref().to_string();
                let stats = entry.value().lock().peek_into();
                (id, stats)
            })
            .collect();

        PerfSnapshot {
            kind: PerfEventKind::PerfSnapshot,
            session_id: self.session_id.clone(),
            ts_ms,
            window_ms: self.window_ms,
            nodes,
        }
    }

    /// Returns `true` if the latest window had any activity. Used
    /// by the flush task to skip a publish when nothing happened
    /// (silent steady state — no point spamming the tap).
    pub fn has_activity(&self) -> bool {
        self.buckets.iter().any(|entry| {
            let b = entry.value().lock();
            b.inputs > 0 || b.outputs > 0
        })
    }
}

impl PerfAggregator {
    /// Read the perf-tap enable flag from the environment. Set
    /// `REMOTEMEDIA_PERF_TAP=1` to opt in. Off by default so
    /// production sessions pay zero overhead.
    pub fn enabled_from_env() -> bool {
        std::env::var("REMOTEMEDIA_PERF_TAP")
            .ok()
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
            .unwrap_or(false)
    }

    /// Read the snapshot window length (ms) from the environment.
    /// Clamped to `[100, 3_600_000]` (100 ms .. 1 hour). The upper
    /// bound is generous on purpose: tooling can configure a large window
    /// (e.g., 600_000 ms) so a single
    /// `peek_snapshot()` at end-of-run captures the whole run as one
    /// merged HDR histogram — silently clamping to 10 s drains the
    /// histograms mid-run and leaves the upstream nodes'
    /// inputs/outputs reading as zero (only the latest tail window
    /// survives).
    ///
    /// Default depends on the profile:
    /// - `REMOTEMEDIA_PERF_PROD=1` → 5000 ms (5× cheaper
    ///   serialization + broadcast cost; HUD still feels live).
    /// - Otherwise → 1000 ms (interactive dev default).
    pub fn window_ms_from_env() -> u32 {
        if let Ok(v) = std::env::var("REMOTEMEDIA_PERF_WINDOW_MS") {
            if let Ok(n) = v.parse::<u32>() {
                return n.clamp(100, 3_600_000);
            }
        }
        if Self::prod_profile_from_env() {
            5_000
        } else {
            1_000
        }
    }

    /// Read the sample stride from the environment. `1` (default)
    /// records every event. Higher values record `1 in N`. Use this
    /// on extremely chatty pipelines (LLM token rate, 50fps video)
    /// where the ~1 µs per record is still load-bearing. Clamped to
    /// `[1, 10_000]`.
    ///
    /// Accepts two forms for convenience:
    /// - `REMOTEMEDIA_PERF_SAMPLE_STRIDE=10`   → 1 in 10
    /// - `REMOTEMEDIA_PERF_SAMPLE_RATE=0.1`    → 1 in 10 (rounded)
    ///
    /// If both are set, `STRIDE` wins. With no env vars and
    /// `REMOTEMEDIA_PERF_PROD=1`, default stride is `10`; otherwise
    /// `1`.
    pub fn sample_stride_from_env() -> u32 {
        if let Ok(v) = std::env::var("REMOTEMEDIA_PERF_SAMPLE_STRIDE") {
            if let Ok(n) = v.parse::<u32>() {
                return n.clamp(1, 10_000);
            }
        }
        if let Ok(v) = std::env::var("REMOTEMEDIA_PERF_SAMPLE_RATE") {
            if let Ok(rate) = v.parse::<f64>() {
                if rate > 0.0 && rate <= 1.0 {
                    return ((1.0 / rate).round() as u32).clamp(1, 10_000);
                }
            }
        }
        if Self::prod_profile_from_env() {
            10
        } else {
            1
        }
    }

    /// Returns `true` when `REMOTEMEDIA_PERF_PROD` is set to a truthy
    /// value. Switches defaults to the production profile (slower
    /// flush + sampled recording). Explicit `WINDOW_MS` / `SAMPLE_*`
    /// env vars always win.
    pub fn prod_profile_from_env() -> bool {
        std::env::var("REMOTEMEDIA_PERF_PROD")
            .ok()
            .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
            .unwrap_or(false)
    }
}

/// Spawn the periodic flush task. Returns a `JoinHandle` so the
/// session can await teardown. The task exits when the `shutdown`
/// `Notify` fires.
pub fn spawn_flush_task<P>(
    aggregator: Arc<PerfAggregator>,
    publish: P,
    shutdown: Arc<tokio::sync::Notify>,
) -> tokio::task::JoinHandle<()>
where
    P: Fn(PerfSnapshot) + Send + Sync + 'static,
{
    let window = std::time::Duration::from_millis(aggregator.window_ms as u64);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(window);
        // First tick fires immediately; skip it so the first
        // snapshot represents a real window of activity.
        ticker.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = shutdown.notified() => break,
                _ = ticker.tick() => {
                    if !aggregator.is_enabled() || !aggregator.has_activity() {
                        continue;
                    }
                    let snapshot = aggregator.flush_snapshot();
                    publish(snapshot);
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_aggregator_records_nothing() {
        let agg = PerfAggregator::new("s".into(), false, 1000);
        agg.record_input("n");
        agg.record_output("n", 1234, true);
        let snap = agg.flush_snapshot();
        assert!(
            snap.nodes.is_empty(),
            "disabled aggregator must skip slot creation"
        );
    }

    #[test]
    fn enabled_aggregator_records_and_resets() {
        let agg = PerfAggregator::new("s".into(), true, 1000);
        agg.record_input("n1");
        agg.record_output("n1", 100, true);
        agg.record_output("n1", 200, false);
        agg.record_input("n2");
        agg.record_output("n2", 50, true);

        let snap = agg.flush_snapshot();
        let n1 = snap.nodes.get("n1").expect("n1 stats");
        assert_eq!(n1.inputs, 1);
        assert_eq!(n1.outputs, 2);
        assert!(n1.latency_us.p50_us > 0);
        assert!(n1.first_output_latency_us.p50_us > 0);

        let n2 = snap.nodes.get("n2").expect("n2 stats");
        assert_eq!(n2.inputs, 1);
        assert_eq!(n2.outputs, 1);

        // Second flush after no activity → empty stats but slot
        // remains.
        let snap2 = agg.flush_snapshot();
        let n1b = snap2.nodes.get("n1").expect("slot persists across flush");
        assert_eq!(n1b.inputs, 0);
        assert_eq!(n1b.outputs, 0);
        assert_eq!(n1b.latency_us.p50_us, 0);
    }

    #[test]
    fn first_output_latency_only_records_first() {
        let agg = PerfAggregator::new("s".into(), true, 1000);
        agg.record_input("n");
        agg.record_output("n", 100, true);
        agg.record_output("n", 999_000, false);
        let snap = agg.flush_snapshot();
        let stats = snap.nodes.get("n").expect("stats");
        // first_output histogram has exactly the 100 µs sample —
        // p99 of one sample = the sample itself.
        assert_eq!(stats.first_output_latency_us.max_us, 100);
        // Total latency histogram has both samples; max should be
        // the slow one (clamped + bucketed but ~999000).
        assert!(stats.latency_us.max_us > 100_000);
    }

    #[test]
    fn peek_does_not_reset_histograms() {
        let agg = PerfAggregator::new("s".into(), true, 1000);
        agg.record_input("n");
        agg.record_output("n", 100, true);
        agg.record_output("n", 200, false);

        let snap1 = agg.peek_snapshot();
        let n1 = snap1.nodes.get("n").expect("n stats");
        assert_eq!(n1.inputs, 1);
        assert_eq!(n1.outputs, 2);
        let p50_first = n1.latency_us.p50_us;
        assert!(p50_first > 0);

        // Second peek with no new activity returns the same counts
        let snap2 = agg.peek_snapshot();
        let n2 = snap2.nodes.get("n").expect("n stats");
        assert_eq!(n2.inputs, 1);
        assert_eq!(n2.outputs, 2);
        assert_eq!(n2.latency_us.p50_us, p50_first);

        // Adding more records and peeking again accumulates
        agg.record_input("n");
        agg.record_output("n", 300, true);
        let snap3 = agg.peek_snapshot();
        let n3 = snap3.nodes.get("n").expect("n stats");
        assert_eq!(n3.inputs, 2);
        assert_eq!(n3.outputs, 3);
    }

    #[test]
    fn sample_stride_drops_most_records_but_keeps_first_emit() {
        let agg = PerfAggregator::with_sample_stride("s".into(), true, 1000, 10);
        // 100 inputs at stride 10 → ~10 admitted.
        for _ in 0..100 {
            agg.record_input("n");
        }
        let snap = agg.peek_snapshot();
        let n = snap.nodes.get("n").expect("n stats");
        // Counter starts at 0 so n=0,10,20,...,90 admit → exactly 10.
        assert_eq!(n.inputs, 10, "stride should admit 1 in 10 inputs");

        // First-output samples bypass the stride: 50 inputs each
        // producing one first emission → 50 first samples.
        for _ in 0..50 {
            agg.record_output("n2", 100, true);
        }
        let snap2 = agg.peek_snapshot();
        let n2 = snap2.nodes.get("n2").expect("n2 stats");
        assert_eq!(n2.outputs, 50, "is_first outputs must never be sampled out");
    }

    #[test]
    fn stride_one_records_everything() {
        let agg = PerfAggregator::with_sample_stride("s".into(), true, 1000, 1);
        for _ in 0..25 {
            agg.record_input("n");
        }
        let snap = agg.peek_snapshot();
        assert_eq!(snap.nodes.get("n").unwrap().inputs, 25);
    }

    #[test]
    fn many_nodes_share_aggregator_without_outer_lock_contention() {
        // Smoke test that the DashMap-keyed-by-Arc<str> shape behaves
        // sanely under concurrent inserts of distinct node ids — the
        // pre-refactor Mutex<HashMap> shape serialized these.
        use std::sync::Arc as StdArc;
        let agg = StdArc::new(PerfAggregator::new("s".into(), true, 1000));
        let mut handles = Vec::new();
        for tid in 0..16u32 {
            let agg = agg.clone();
            handles.push(std::thread::spawn(move || {
                let id = format!("node_{tid}");
                for _ in 0..1000 {
                    agg.record_input(&id);
                    agg.record_output(&id, 100, true);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let snap = agg.peek_snapshot();
        assert_eq!(snap.nodes.len(), 16);
        for (_, stats) in &snap.nodes {
            assert_eq!(stats.inputs, 1000);
            assert_eq!(stats.outputs, 1000);
        }
    }

    #[test]
    fn has_activity_reports_correctly() {
        let agg = PerfAggregator::new("s".into(), true, 1000);
        assert!(!agg.has_activity());
        agg.record_input("n");
        assert!(agg.has_activity());
        let _ = agg.flush_snapshot();
        assert!(!agg.has_activity(), "flush resets activity counters");
    }
}
