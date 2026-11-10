//! The non-RT worker thread body.
//!
//! One thread per bridge. The loop is dead simple:
//!
//!   1. Drain as many inputs as are available right now.
//!   2. For each one, call `node.process(data)` and push the result
//!      into the output ring (drop on overflow — we must not block).
//!   3. If the shutdown flag is set, exit. Otherwise spin-yield and
//!      go back to step 1.
//!
//! Why spin-yield instead of park-and-unpark: the producer (the RT
//! callback) must not call `Thread::unpark` or any other syscall, so
//! the worker cannot be woken by a producer-side notification. Instead
//! we poll. On modern schedulers `std::thread::yield_now` is cheap
//! (~50ns) and cedes CPU to the RT thread when it's active.
//!
//! An adaptive back-off could cut idle CPU further, but the worker is
//! expected to either be pegged (audio arrives every N samples) or
//! idle between sessions; a short spin keeps latency predictable.

use crate::data::RuntimeData;
use crate::nodes::SyncStreamingNode;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

/// Shared stats accumulators. Incremented by the worker, read by
/// [`crate::rt_bridge::RtBridge::stats`]. All atomic, no contention with the RT
/// producer / consumer.
#[derive(Default)]
pub(crate) struct WorkerStats {
    pub(crate) processed: AtomicU64,
    pub(crate) process_errors: AtomicU64,
    pub(crate) output_overflows: AtomicU64,
}

pub(crate) enum WorkerInput {
    Legacy(rtrb::Consumer<RuntimeData>),
    Prepared(crate::rt_bridge::prepared::PreparedWorker),
}

impl WorkerInput {
    fn service(&mut self) {
        if let Self::Prepared(prepared) = self {
            prepared.service();
        }
    }

    fn pop(&mut self) -> Option<RuntimeData> {
        match self {
            Self::Prepared(prepared) => prepared.pop(),
            Self::Legacy(input) => input.pop().ok(),
        }
    }

    fn process(
        &self,
        node: &dyn SyncStreamingNode,
        data: RuntimeData,
    ) -> crate::Result<RuntimeData> {
        match self {
            Self::Prepared(prepared) => node.process_with_ctx(data, &prepared.context),
            Self::Legacy(_) => node.process(data),
        }
    }
}

pub(crate) struct WorkerCtx {
    pub(crate) input: WorkerInput,
    pub(crate) output: rtrb::Producer<RuntimeData>,
    pub(crate) node: Box<dyn SyncStreamingNode>,
    pub(crate) shutdown: Arc<AtomicBool>,
    pub(crate) stats: Arc<WorkerStats>,
}

pub(crate) fn run(mut ctx: WorkerCtx) {
    tracing::debug!("rt-bridge worker thread started");

    // Steady-state loop. Drain everything ready, then yield.
    loop {
        ctx.input.service();
        // Drain burst. This is the hot path when audio is flowing.
        let mut processed_this_round = false;
        while let Some(data) = ctx.input.pop() {
            processed_this_round = true;
            match ctx.input.process(ctx.node.as_ref(), data) {
                Ok(out) => {
                    // try push; on overflow, drop. We are *not* allowed
                    // to block the worker when the RT consumer is slow
                    // to drain — blocking here would eventually stall
                    // the input ring and silently drop RT producer
                    // writes. Better to drop stale output explicitly
                    // and surface it in `output_overflows`.
                    if ctx.output.push(out).is_err() {
                        ctx.stats.output_overflows.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!("rt-bridge output ring full — dropping output");
                    } else {
                        ctx.stats.processed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(e) => {
                    ctx.stats.process_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!("rt-bridge node.process failed: {}", e);
                }
            }
        }

        if ctx.shutdown.load(Ordering::Acquire) {
            break;
        }

        if !processed_this_round {
            // Nothing to do right now. Yield once so the OS can give
            // CPU to the RT thread or other work. This is NOT the RT
            // thread — it's the bridge worker — so calling
            // yield_now is fine.
            std::thread::yield_now();
        }
    }

    tracing::debug!(
        "rt-bridge worker exiting: processed={} errors={} output_overflows={}",
        ctx.stats.processed.load(Ordering::Relaxed),
        ctx.stats.process_errors.load(Ordering::Relaxed),
        ctx.stats.output_overflows.load(Ordering::Relaxed),
    );
}
