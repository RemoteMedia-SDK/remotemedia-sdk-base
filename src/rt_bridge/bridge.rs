//! The [`RtBridge`] owner struct: spawns the worker, holds the join
//! handle, and cleans up on drop.

use crate::nodes::SyncStreamingNode;
use crate::rt_bridge::error::SpawnError;
use crate::rt_bridge::handles::{RtInputProducer, RtOutputConsumer};
use crate::rt_bridge::worker::{self, WorkerCtx, WorkerInput, WorkerStats};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

/// Configuration knobs for spawning an [`RtBridge`].
///
/// The default configuration is tuned for interactive audio: 64-slot
/// rings (about 1.3ms of headroom at 48kHz / 960-sample packets) and a
/// named worker thread. Enable the `realtime` feature to also apply
/// thread priority and core pinning.
#[derive(Debug, Clone)]
pub struct RtBridgeConfig {
    /// Capacity of the RT → worker input ring. Must be ≥ 2.
    pub input_capacity: usize,

    /// Capacity of the worker → RT output ring. Must be ≥ 2.
    pub output_capacity: usize,

    /// Optional thread name for diagnostics / profilers.
    pub thread_name: Option<String>,

    /// (`realtime` feature only) Optional thread scheduling policy.
    #[cfg(feature = "rt-bridge-realtime")]
    pub priority: Option<thread_priority::ThreadPriority>,

    /// (`realtime` feature only) Optional CPU core to pin the worker to.
    #[cfg(feature = "rt-bridge-realtime")]
    pub core_id: Option<core_affinity::CoreId>,
}

impl Default for RtBridgeConfig {
    fn default() -> Self {
        Self {
            // 64 slots × ~20ms chunks ≈ 1.3s of headroom, more than
            // enough for a worker that processes each chunk in under
            // one audio period.
            input_capacity: 64,
            output_capacity: 64,
            thread_name: Some("rt-bridge".to_string()),
            #[cfg(feature = "rt-bridge-realtime")]
            priority: None,
            #[cfg(feature = "rt-bridge-realtime")]
            core_id: None,
        }
    }
}

/// Snapshot of bridge worker counters. Read-only.
#[derive(Debug, Clone, Copy, Default)]
pub struct RtBridgeStats {
    /// Number of packets successfully processed and pushed to the
    /// output ring.
    pub processed: u64,
    /// Number of `node.process` calls that returned an error.
    pub process_errors: u64,
    /// Number of outputs dropped because the output ring was full
    /// (consumer slower than worker).
    pub output_overflows: u64,
}

/// A pinned worker thread that pumps data from an RT thread through a
/// [`SyncStreamingNode`] and back. See the crate-level docs for the
/// full model.
///
/// The bridge owns its worker thread. Dropping the `RtBridge` requests
/// shutdown and joins. The producer and consumer handles are returned
/// separately from [`RtBridge::spawn`] so they can be moved into the
/// RT callback independently.
pub struct RtBridge {
    /// Join handle for the worker thread. `Option` so we can take it
    /// in `Drop`.
    handle: Option<JoinHandle<()>>,
    /// Flag the worker polls; flipped by `shutdown` / `Drop`.
    shutdown: Arc<AtomicBool>,
    /// Shared stats counters.
    stats: Arc<WorkerStats>,
}

impl RtBridge {
    /// Spawn the bridge and return `(bridge, producer, consumer)`.
    ///
    /// The `producer` and `consumer` are the RT-safe handles — keep
    /// them alive for the lifetime of the RT callback. The `bridge`
    /// owns the worker thread; dropping it joins.
    ///
    /// # Errors
    ///
    /// Returns [`SpawnError::ThreadSpawn`] if the OS refuses to start
    /// the worker thread. With the `realtime` feature, priority and
    /// core affinity are best-effort: a refusal is logged as a warning and
    /// the worker continues at default priority (it is not an error).
    pub fn spawn<N>(
        node: N,
        config: RtBridgeConfig,
    ) -> Result<(Self, RtInputProducer, RtOutputConsumer), SpawnError>
    where
        N: SyncStreamingNode + 'static,
    {
        Self::spawn_boxed(Box::new(node), config)
    }

    /// `spawn` but taking an already-boxed node — useful when the node
    /// type is erased via `Box<dyn SyncStreamingNode>`.
    pub fn spawn_boxed(
        node: Box<dyn SyncStreamingNode>,
        config: RtBridgeConfig,
    ) -> Result<(Self, RtInputProducer, RtOutputConsumer), SpawnError> {
        assert!(config.input_capacity >= 2, "input_capacity must be >= 2");
        assert!(config.output_capacity >= 2, "output_capacity must be >= 2");

        let (input_tx, input_rx) = rtrb::RingBuffer::new(config.input_capacity);
        let (output_tx, output_rx) = rtrb::RingBuffer::new(config.output_capacity);

        let shutdown = Arc::new(AtomicBool::new(false));
        let stats = Arc::new(WorkerStats::default());

        let ctx = WorkerCtx {
            input: WorkerInput::Legacy(input_rx),
            output: output_tx,
            node,
            shutdown: Arc::clone(&shutdown),
            stats: Arc::clone(&stats),
        };

        let bridge = Self::spawn_worker(ctx, config)?;
        Ok((
            bridge,
            RtInputProducer::new(input_tx),
            RtOutputConsumer::new(output_rx),
        ))
    }

    /// Prepare audio loans with the context's allocator before starting capture.
    ///
    /// The worker refills loans, freezes captured packets, invokes
    /// `process_with_ctx`, and releases consumed output owners. No heap fallback
    /// occurs on provider exhaustion. Use a provider with real-time-safe mutable
    /// slice access; arbitrary foreign providers need their own validation.
    /// Stop capture and destroy both callback
    /// handles off the real-time thread before shutting down the bridge.
    pub fn spawn_prepared_audio<N: SyncStreamingNode + 'static>(
        node: N,
        config: RtBridgeConfig,
        audio: crate::rt_bridge::PreparedAudioConfig,
        context: crate::nodes::NodeRuntimeContext,
    ) -> Result<
        (
            Self,
            crate::rt_bridge::PreparedAudioProducer,
            crate::rt_bridge::PreparedAudioConsumer,
        ),
        SpawnError,
    > {
        Self::spawn_prepared(node, config, audio, context, false)
    }

    /// Adapt borrowed device input with one accounted copy into prepared storage.
    /// Strict zero-copy policy rejects this boundary before allocating loans.
    /// One additional input slot is reserved for a partial final packet. Stop the
    /// device, finish/drop the producer off the callback thread, drain output,
    /// then shut down the bridge. Output overflow is reported by `stats`.
    pub fn spawn_prepared_capture<N: SyncStreamingNode + 'static>(
        node: N,
        config: RtBridgeConfig,
        audio: crate::rt_bridge::PreparedAudioConfig,
        context: crate::nodes::NodeRuntimeContext,
    ) -> Result<
        (
            Self,
            crate::rt_bridge::PreparedCaptureProducer,
            crate::rt_bridge::PreparedAudioConsumer,
        ),
        SpawnError,
    > {
        let (bridge, producer, consumer) =
            Self::spawn_prepared(node, config, audio, context, true)?;
        let producer = crate::rt_bridge::PreparedCaptureProducer {
            producer,
            channels: audio.channels,
            sample_rate: audio.sample_rate,
            finished: false,
        };
        Ok((bridge, producer, consumer))
    }

    fn spawn_prepared<N: SyncStreamingNode + 'static>(
        node: N,
        config: RtBridgeConfig,
        audio: crate::rt_bridge::PreparedAudioConfig,
        context: crate::nodes::NodeRuntimeContext,
        capture: bool,
    ) -> Result<
        (
            Self,
            crate::rt_bridge::PreparedAudioProducer,
            crate::rt_bridge::PreparedAudioConsumer,
        ),
        SpawnError,
    > {
        assert!(config.input_capacity >= 2, "input_capacity must be >= 2");
        assert!(config.output_capacity >= 2, "output_capacity must be >= 2");
        let capacity = config
            .input_capacity
            .checked_add(usize::from(capture))
            .ok_or_else(|| SpawnError::PreparedAudio("capture input capacity overflow".into()))?;
        let (output, output_rx) = rtrb::RingBuffer::new(config.output_capacity);
        let shutdown = Arc::new(AtomicBool::new(false));
        let (prepared, producer, consumer) = crate::rt_bridge::prepared::PreparedWorker::prepare(
            context,
            audio,
            capacity,
            output_rx,
            shutdown.clone(),
            capture,
        )?;
        let ctx = WorkerCtx {
            input: WorkerInput::Prepared(prepared),
            output,
            node: Box::new(node),
            shutdown,
            stats: Arc::new(WorkerStats::default()),
        };
        Ok((Self::spawn_worker(ctx, config)?, producer, consumer))
    }

    fn spawn_worker(ctx: WorkerCtx, config: RtBridgeConfig) -> Result<Self, SpawnError> {
        let shutdown = ctx.shutdown.clone();
        let stats = ctx.stats.clone();
        let mut builder = std::thread::Builder::new();
        if let Some(name) = &config.thread_name {
            builder = builder.name(name.clone());
        }

        #[cfg(feature = "rt-bridge-realtime")]
        let rt_config = (config.priority, config.core_id);

        let handle = builder
            .spawn(move || {
                #[cfg(feature = "rt-bridge-realtime")]
                {
                    crate::rt_bridge::realtime::apply(rt_config.0, rt_config.1);
                }
                worker::run(ctx);
            })
            .map_err(SpawnError::ThreadSpawn)?;

        Ok(Self {
            handle: Some(handle),
            shutdown,
            stats,
        })
    }

    /// Read the worker's running counters.
    pub fn stats(&self) -> RtBridgeStats {
        RtBridgeStats {
            processed: self.stats.processed.load(Ordering::Relaxed),
            process_errors: self.stats.process_errors.load(Ordering::Relaxed),
            output_overflows: self.stats.output_overflows.load(Ordering::Relaxed),
        }
    }

    /// Request worker shutdown and join. Consumes the bridge.
    pub fn shutdown(mut self) {
        let _ = self.request_shutdown_and_join();
    }

    /// Join the worker and return final counters, preserving a worker panic.
    /// Stop and reclaim producers first. Remaining output can then be taken via
    /// `PreparedAudioConsumer::drain_after_shutdown` on the control thread.
    pub fn shutdown_checked(mut self) -> std::thread::Result<RtBridgeStats> {
        self.request_shutdown_and_join()?;
        Ok(self.stats())
    }

    fn request_shutdown_and_join(&mut self) -> std::thread::Result<()> {
        self.shutdown.store(true, Ordering::Release);
        if let Some(h) = self.handle.take() {
            h.join()?;
        }
        Ok(())
    }
}

impl Drop for RtBridge {
    fn drop(&mut self) {
        if self.handle.is_some() {
            let _ = self.request_shutdown_and_join();
        }
    }
}
