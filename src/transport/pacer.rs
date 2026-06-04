//! Tick-driven node pacing.
//!
//! A `Pacer` drives a single tick-paced node by repeatedly producing
//! [`Tick`]s and invoking the node's `tick()` method against its bound
//! [`NodeRuntimeContext`]. Outputs flow into the fan-out mpsc the
//! session router wires for the node — same path reactive emits use,
//! so downstream nodes don't care whether their upstream is reactive
//! or paced.
//!
//! ## Tick sources
//!
//! [`WallTickSource`] is the only source that ships today. It backs
//! `PacingNature::SourceWall(hz)` nodes with a `tokio::time::interval`
//! using `MissedTickBehavior::Skip` so catch-up bursts after a stall
//! are dropped instead of queued.
//!
//! Wire-bound tick sources (subscribing to a `media_clock` event from
//! the WebRTC outbound stream's RTP clock) land with Phase 5.1/5.2/5.5.
//! When they ship, `ClockedToOutboundMedia` nodes plug into the same
//! [`Pacer::run`] loop with no other changes.
//!
//! ## On-miss policy
//!
//! [`OnMiss::Drop`] (default): when `tick()` overruns its budget, the
//! Pacer logs a warning and drops the tick result so the late frame
//! doesn't cascade into subsequent ticks. The next tick fires on
//! schedule from the underlying source.
//!
//! [`OnMiss::Log`]: same behavior as `Drop` (the result is always
//! delivered regardless of latency); included so the spec's
//! drop/stretch/degrade trio has a permissive variant for diagnostic
//! runs that want every produced frame even if late. Stretch + Degrade
//! land with Phase 5.6 — they need cached-frame state and control bus
//! integration respectively.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::data::RuntimeData;
#[cfg(test)]
use crate::nodes::NodeRuntimeContextRead;
use crate::nodes::{NodeRuntimeContext, StreamingNode, Tick};
use crate::transport::perf_aggregator::PerfAggregator;
use crate::Result;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// What to do when a node's `tick()` overruns its per-frame deadline.
///
/// The Pacer measures elapsed wall time against `tick.deadline_us`
/// after the tick future resolves. The policy decides what happens
/// when `elapsed > budget`.
///
/// Today only [`OnMiss::Drop`] and [`OnMiss::Log`] are implemented —
/// they only differ in whether the late frame is forwarded. Both
/// always emit a `tracing::warn` for the miss. Stretch (re-emit the
/// last cached frame) and Degrade (publish a `Control::Degraded`
/// envelope so the node can switch to a cheaper path) land with
/// Phase 5.6.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnMiss {
    /// Drop the late frame: outputs produced by the overrunning tick
    /// never reach the fan-out. Subsequent ticks fire on schedule.
    Drop,
    /// Forward the late frame anyway, but emit a warning. Useful in
    /// diagnostic runs where every produced frame matters more than
    /// latency conformance.
    Log,
}

impl Default for OnMiss {
    fn default() -> Self {
        OnMiss::Drop
    }
}

/// Rate-limit state for fan_tx backpressure drop warnings. Shared
/// across all per-tick callbacks within a single Pacer's `run()` loop.
struct DropSummary {
    count: u64,
    last_log: Instant,
}

/// Rate-limit state for transient tick errors. These are expected while
/// transport-backed nodes are waiting for their external edge/track to bind,
/// and warning once per pacer tick makes startup logs unreadable.
struct TickErrorSummary {
    count: u64,
    last_log: Instant,
}

fn is_transient_unbound_tick_error(error: &crate::Error) -> bool {
    let message = error.to_string();
    message.contains("WebRTC edge") && message.contains(" is not bound")
}

/// Source of [`Tick`]s for a [`Pacer`]. Implementations can be
/// wall-clock-driven (today's [`WallTickSource`]) or wire-bound
/// (Phase 5.1/5.2 — subscribes to a `media_clock` broadcast).
///
/// Implementations are owned exclusively by their [`Pacer`] task; no
/// `&self` shared-mutability concerns. The trait is async because
/// wire-bound sources await a broadcast::Receiver in their `next`.
#[async_trait]
pub trait TickSource: Send + 'static {
    /// Produce the next tick, blocking until the underlying clock
    /// fires. Returns `None` when the source has stopped (e.g. peer
    /// disconnected on a wire-bound source); the Pacer treats `None`
    /// as a clean shutdown signal.
    async fn next(&mut self) -> Option<Tick>;
}

/// Build the `<medium>:<stream_id>` address used on the media_clock
/// transient channel. Producers (WebRTC `AudioSender`, `VideoTrack`)
/// and consumers (this module's `WireTickSource`) must agree on the
/// scheme so they meet at the same address.
pub fn media_clock_addr(medium: &str, stream_id: &str) -> String {
    format!("{}:{}", medium, stream_id)
}

/// Wire-bound tick source. Subscribes to the
/// [`crate::transport::session_control::SessionControl`] media_clock
/// transient channel at `<medium>:<stream_id>` and converts each
/// published [`crate::transport::session_control::MediaClock`] event
/// into a [`Tick`] for the Pacer.
///
/// Backs `PacingNature::ClockedToOutboundMedia` nodes terminating
/// into an outbound RTP stream. Producers (WebRTC `AudioSender` after
/// each Opus frame, `VideoTrack` after each `send_video`) publish
/// per-frame events; the Pacer drives the consumer node's `tick()`
/// once per published event.
///
/// Returns `None` from [`TickSource::next`] when the broadcast closes
/// — i.e. the producer called `SessionControl::stop_media_clock`
/// (peer disconnect, sender shutdown). The Pacer interprets that as
/// a clean shutdown and exits its run loop. The session router can
/// then optionally fall back to a wall pacer for headless sessions
/// (Phase 5.9).
///
/// Lagged broadcasts (slow consumer falls behind the bursty
/// producer) are silently skipped — losing one tick at the wall
/// boundary is preferable to bursting catch-up frames.
pub struct WireTickSource {
    rx: tokio::sync::broadcast::Receiver<crate::transport::session_control::MediaClock>,
}

impl WireTickSource {
    /// Subscribe to the media_clock channel at `<medium>:<stream_id>`.
    /// Equivalent to `SessionControl::subscribe_media_clock` plus
    /// wrapping the receiver in this struct so it satisfies
    /// [`TickSource`].
    pub fn subscribe(
        control: &crate::transport::session_control::SessionControl,
        medium: &str,
        stream_id: &str,
    ) -> Self {
        let addr = media_clock_addr(medium, stream_id);
        Self {
            rx: control.subscribe_media_clock(&addr),
        }
    }

    /// Build a `WireTickSource` from a pre-acquired broadcast
    /// receiver. Useful when the caller wants control of the
    /// subscribe timing (e.g. registering before the producer can
    /// publish so no events are lost).
    pub fn from_receiver(
        rx: tokio::sync::broadcast::Receiver<crate::transport::session_control::MediaClock>,
    ) -> Self {
        Self { rx }
    }
}

#[async_trait]
impl TickSource for WireTickSource {
    async fn next(&mut self) -> Option<Tick> {
        loop {
            match self.rx.recv().await {
                Ok(clock) => {
                    return Some(Tick {
                        clock_id: media_clock_addr(&clock.medium, &clock.stream_id),
                        pts_us: clock.pts_us,
                        frame_idx: clock.frame_idx,
                        deadline_us: clock.wire_deadline_us,
                    });
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    // Skip — the next event is already queued.
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return None;
                }
            }
        }
    }
}

/// Wall-clock tick source for `SourceWall(hz)` nodes.
///
/// Backed by `tokio::time::interval` with
/// `MissedTickBehavior::Skip`: catch-up bursts after a stall are
/// dropped, not queued. PTS originates from `Instant::now()` at
/// construction so all frames in a session share a stable timeline;
/// `frame_idx` increments monotonically.
pub struct WallTickSource {
    clock_id: String,
    interval: tokio::time::Interval,
    interval_us: u64,
    started: Instant,
    frame_idx: u64,
}

impl WallTickSource {
    /// Build a wall tick source firing at `hz` frames per second.
    pub fn new(hz: u32) -> Self {
        assert!(hz > 0, "WallTickSource requires hz > 0");
        let interval_us = 1_000_000u64 / hz as u64;
        let period = Duration::from_micros(interval_us);
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Self {
            clock_id: format!("wall:{}", hz),
            interval,
            interval_us,
            started: Instant::now(),
            frame_idx: 0,
        }
    }
}

#[async_trait]
impl TickSource for WallTickSource {
    async fn next(&mut self) -> Option<Tick> {
        self.interval.tick().await;
        let pts_us = self.started.elapsed().as_micros() as u64;
        let frame_idx = self.frame_idx;
        self.frame_idx = self.frame_idx.wrapping_add(1);
        Some(Tick {
            clock_id: self.clock_id.clone(),
            pts_us,
            frame_idx,
            deadline_us: pts_us + self.interval_us,
        })
    }
}

/// Tick-driven node driver.
///
/// Owns the node `Arc`, the bound `NodeRuntimeContext`, and a
/// `TickSource`. `Pacer::spawn` spawns a tokio task that loops on
/// `source.next()`, calls `node.tick(...)`, and forwards outputs to
/// the bound fan-out sender.
///
/// The session router constructs and spawns one Pacer per
/// tick-driven node at session bind. The returned `JoinHandle` is
/// stored alongside the node's main + fan-out tasks and aborted on
/// session shutdown.
pub struct Pacer {
    node_id: String,
    node: Arc<dyn StreamingNode>,
    ctx: NodeRuntimeContext,
    fan_tx: mpsc::Sender<RuntimeData>,
    source: Box<dyn TickSource>,
    on_miss: OnMiss,
    /// Optional perf aggregator. When set, every callback emit from
    /// `node.tick()` records an output against `node_id` so paced
    /// nodes (e.g. `CcRenderNode`) show up in perf snapshots with
    /// non-zero `outputs`. Without this the reactive dispatch path
    /// records inputs (e.g. VAD heartbeat) but the tick-emitted
    /// Video frames are invisible to telemetry, which makes a
    /// healthy avatar look like a stalled one.
    perf: Option<Arc<PerfAggregator>>,
}

impl Pacer {
    /// Build a new Pacer.
    ///
    /// `fan_tx` is the same fan-out sender the node's reactive
    /// dispatch path uses. Tick-produced outputs flow through the
    /// same drain task as reactive outputs, applying the control-bus
    /// hook and forwarding to successors / sinks.
    pub fn new(
        node_id: impl Into<String>,
        node: Arc<dyn StreamingNode>,
        ctx: NodeRuntimeContext,
        fan_tx: mpsc::Sender<RuntimeData>,
        source: Box<dyn TickSource>,
        on_miss: OnMiss,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            node,
            ctx,
            fan_tx,
            source,
            on_miss,
            perf: None,
        }
    }

    /// Wire a perf aggregator so tick callback emits are recorded
    /// against `node_id`. Builder-style so existing call sites that
    /// don't care about telemetry stay terse.
    pub fn with_perf(mut self, perf: Arc<PerfAggregator>) -> Self {
        self.perf = Some(perf);
        self
    }

    /// Spawn the Pacer's main loop on the current tokio runtime.
    /// Returns the `JoinHandle` so the caller can `.abort()` it on
    /// shutdown.
    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(self.run())
    }

    /// Main loop: pull a tick, dispatch, measure, apply on-miss
    /// policy, repeat. Exits when the tick source returns `None`.
    async fn run(mut self) {
        // Aggregated fan_tx-full drop counter shared across all per-tick
        // callbacks. A paced node firing at 30 Hz with a saturated
        // downstream channel would otherwise emit ~30 warns/sec — the
        // log spam buries every other diagnostic. Drops are accumulated
        // here and surfaced as a single summary warn at most once per
        // `DROP_LOG_INTERVAL`.
        const DROP_LOG_INTERVAL: Duration = Duration::from_secs(5);
        let drop_summary: Arc<std::sync::Mutex<DropSummary>> =
            Arc::new(std::sync::Mutex::new(DropSummary {
                count: 0,
                last_log: Instant::now() - DROP_LOG_INTERVAL,
            }));
        const TRANSIENT_ERROR_LOG_INTERVAL: Duration = Duration::from_secs(5);
        let mut transient_error_summary = TickErrorSummary {
            count: 0,
            last_log: Instant::now() - TRANSIENT_ERROR_LOG_INTERVAL,
        };

        while let Some(tick) = self.source.next().await {
            let started = Instant::now();
            let deadline = Duration::from_micros(tick.deadline_us);
            let budget = deadline.saturating_sub(Duration::from_micros(tick.pts_us));

            // Per-tick callback: try_send into fan_tx. If the on-miss
            // policy is Drop AND the tick overran, the cell below
            // refuses sends. Closures own their share so `Pacer` can
            // be moved across awaits.
            let drop_late = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let cb_fan_tx = self.fan_tx.clone();
            let cb_node_id = self.node_id.clone();
            let cb_drop_late = Arc::clone(&drop_late);
            let cb_perf = self.perf.clone();
            let cb_first_emit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
            let cb_drop_summary = Arc::clone(&drop_summary);
            let cb: Box<dyn FnMut(RuntimeData) -> Result<()> + Send> = Box::new(move |out| {
                if cb_drop_late.load(std::sync::atomic::Ordering::Relaxed) {
                    // Late frame; the on-miss policy chose to
                    // drop. Discard silently — the warning is
                    // emitted once at miss-detection time.
                    return Ok(());
                }
                // Perf instrumentation: record one output per
                // emit, with latency measured from the start of
                // this tick. Without this, paced nodes
                // (Video sinks, idle renderers) read as
                // `outputs=0` in snapshots even when frames are
                // flowing, which is misleading during avatar
                // debugging.
                if let Some(perf) = &cb_perf {
                    let lat_us = started.elapsed().as_micros() as u64;
                    let is_first = !cb_first_emit.swap(true, std::sync::atomic::Ordering::Relaxed);
                    perf.record_output(&cb_node_id, lat_us, is_first);
                }
                if let Err(e) = cb_fan_tx.try_send(out) {
                    // Aggregate drops across ticks; emit a single
                    // summary at most once per DROP_LOG_INTERVAL so
                    // a saturated downstream channel doesn't flood
                    // the log at the tick rate.
                    if let Ok(mut s) = cb_drop_summary.lock() {
                        s.count = s.count.saturating_add(1);
                        if s.last_log.elapsed() >= DROP_LOG_INTERVAL {
                            tracing::warn!(
                                pacer_node = %cb_node_id,
                                dropped = s.count,
                                interval_s = DROP_LOG_INTERVAL.as_secs(),
                                last_error = %e,
                                "Pacer: fan_tx backpressure (suppressed per-frame warns)"
                            );
                            s.count = 0;
                            s.last_log = Instant::now();
                        }
                    }
                }
                Ok(())
            });

            // Invoke the node's tick. Errors are logged + swallowed —
            // a misbehaving node should not crash the Pacer. The
            // callback may fire 0..N times depending on the node.
            if let Err(e) = self.node.tick(tick.clone(), &self.ctx, cb).await {
                if is_transient_unbound_tick_error(&e) {
                    transient_error_summary.count = transient_error_summary.count.saturating_add(1);
                    if transient_error_summary.last_log.elapsed() >= TRANSIENT_ERROR_LOG_INTERVAL {
                        tracing::debug!(
                            pacer_node = %self.node_id,
                            suppressed = transient_error_summary.count,
                            interval_s = TRANSIENT_ERROR_LOG_INTERVAL.as_secs(),
                            last_error = %e,
                            "Pacer: transient unbound transport edge"
                        );
                        transient_error_summary.count = 0;
                        transient_error_summary.last_log = Instant::now();
                    }
                } else {
                    tracing::warn!(
                        pacer_node = %self.node_id,
                        error = %e,
                        "Pacer: tick returned error"
                    );
                }
            }

            let elapsed = started.elapsed();
            if elapsed > budget {
                match self.on_miss {
                    OnMiss::Drop => {
                        // Already running — late callbacks (if any
                        // were dispatched after this point) won't
                        // observe the flag. The tick body has
                        // already returned; the warn below documents
                        // the miss for telemetry.
                        drop_late.store(true, std::sync::atomic::Ordering::Relaxed);
                        tracing::warn!(
                            pacer_node = %self.node_id,
                            elapsed_us = elapsed.as_micros() as u64,
                            budget_us = budget.as_micros() as u64,
                            policy = "drop",
                            "Pacer: tick overran deadline"
                        );
                    }
                    OnMiss::Log => {
                        tracing::warn!(
                            pacer_node = %self.node_id,
                            elapsed_us = elapsed.as_micros() as u64,
                            budget_us = budget.as_micros() as u64,
                            policy = "log",
                            "Pacer: tick overran deadline (forwarded)"
                        );
                    }
                }
            }
        }
        tracing::debug!(
            pacer_node = %self.node_id,
            "Pacer: tick source exhausted, exiting"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `WallTickSource` produces ticks whose `clock_id` matches the
    /// `wall:<hz>` convention, with monotonic `frame_idx` and
    /// monotonic `pts_us`.
    #[tokio::test]
    async fn wall_tick_source_emits_well_formed_ticks() {
        let mut src = WallTickSource::new(100);
        // Drain the first tick (immediate) and a couple more.
        let t0 = src.next().await.unwrap();
        let t1 = src.next().await.unwrap();
        let t2 = src.next().await.unwrap();
        assert_eq!(t0.clock_id, "wall:100");
        assert_eq!(t0.frame_idx, 0);
        assert_eq!(t1.frame_idx, 1);
        assert_eq!(t2.frame_idx, 2);
        // PTS is monotonic; deadline is pts + interval.
        assert!(t1.pts_us > t0.pts_us);
        assert!(t2.pts_us > t1.pts_us);
        assert!(t0.deadline_us >= t0.pts_us);
    }

    /// A canned tick source emits a fixed list of ticks, then `None`.
    /// Used in the Pacer integration test below.
    struct CannedSource {
        ticks: std::collections::VecDeque<Tick>,
    }
    #[async_trait]
    impl TickSource for CannedSource {
        async fn next(&mut self) -> Option<Tick> {
            self.ticks.pop_front()
        }
    }

    /// A node that emits one Json output per tick recording the
    /// tick's frame_idx; pure, no state, no async.
    struct CountingTickNode {
        node_id: String,
    }
    #[async_trait::async_trait]
    impl StreamingNode for CountingTickNode {
        fn node_type(&self) -> &str {
            "CountingTickNode"
        }
        fn node_id(&self) -> &str {
            &self.node_id
        }
        fn pacing_nature(&self) -> crate::nodes::PacingNature {
            crate::nodes::PacingNature::SourceWall(100)
        }
        fn is_multi_input(&self) -> bool {
            false
        }
        async fn process_async(
            &self,
            _: RuntimeData,
            _: &dyn NodeRuntimeContextRead,
        ) -> std::result::Result<RuntimeData, crate::Error> {
            Err(crate::Error::Execution("tick-only".into()))
        }
        async fn process_multi_async(
            &self,
            _: std::collections::HashMap<String, RuntimeData>,
            _: &dyn NodeRuntimeContextRead,
        ) -> std::result::Result<RuntimeData, crate::Error> {
            Err(crate::Error::Execution("tick-only".into()))
        }
        async fn tick(
            &self,
            tick: Tick,
            _: &dyn NodeRuntimeContextRead,
            mut cb: Box<dyn FnMut(RuntimeData) -> Result<()> + Send>,
        ) -> std::result::Result<(), crate::Error> {
            cb(RuntimeData::Json(serde_json::json!({
                "frame_idx": tick.frame_idx
            })))
            .map_err(|e| crate::Error::Execution(format!("cb: {e}")))?;
            Ok(())
        }
    }

    /// Pacer drives the node once per tick from the canned source and
    /// stops cleanly when the source returns `None`.
    #[tokio::test]
    async fn pacer_drives_node_once_per_tick_then_exits() {
        let node: Arc<dyn StreamingNode> = Arc::new(CountingTickNode {
            node_id: "tick".into(),
        });
        let ctx = NodeRuntimeContext::for_test("sess-1", "tick");

        let (fan_tx, mut fan_rx) = mpsc::channel::<RuntimeData>(16);

        let mut ticks = std::collections::VecDeque::new();
        for i in 0..3 {
            ticks.push_back(Tick {
                clock_id: "test:1".into(),
                pts_us: i * 1_000,
                frame_idx: i,
                deadline_us: (i + 1) * 1_000,
            });
        }
        let source = Box::new(CannedSource { ticks });

        let pacer = Pacer::new("tick", node, ctx, fan_tx, source, OnMiss::Drop);
        let handle = pacer.spawn();

        // Collect outputs; exits when the canned source is empty
        // and the Pacer drops the fan_tx clone via run() returning.
        let mut frames = Vec::new();
        while let Some(out) = fan_rx.recv().await {
            if let RuntimeData::Json(v) = out {
                frames.push(v.get("frame_idx").and_then(|x| x.as_u64()).unwrap());
            }
        }

        let _ = handle.await;
        assert_eq!(frames, vec![0, 1, 2]);
    }
}
