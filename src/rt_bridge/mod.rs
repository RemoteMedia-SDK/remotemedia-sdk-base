//! Real-time thread ↔ async pipeline bridge.
//!
//! `remotemedia-rt-bridge` lets real-time audio hosts — Core Audio HAL
//! plugins, AudioUnit render callbacks, JACK client process callbacks,
//! AAudio data callbacks — drive a `remotemedia-core` node without ever
//! entering the tokio scheduler, awaiting a bounded channel, or
//! contending a shared lock.
//!
//! # Model
//!
//! ```text
//!                            ┌──────────────────────┐
//!   RT audio thread          │  Pinned worker thread │   (non-RT) async world
//!   (HAL IO proc)            │  (remotemedia-rt-bridge) │
//!                            │                      │
//!   fill input slice ──┐     │                      │
//!                      │     │                      │
//!   RtInputProducer    │     │                      │
//!    .try_push()  ─────┼────►│  input  (rtrb SPSC)  │
//!                      │     │  ring   consumer     │
//!                      │     │                      │
//!                      │     │  node.process(data)  │
//!                      │     │   [SyncStreamingNode]│
//!                      │     │                      │
//!   RtOutputConsumer   │     │  output (rtrb SPSC)  │
//!    .try_pop() ◄──────┼─────│  ring   producer     │
//!                      │     │                      │
//!   write output  ─────┘     └──────────────────────┘
//! ```
//!
//! # What the bridge guarantees
//!
//! - **No tokio.** The worker thread is a plain OS thread; it does not
//!   run a tokio executor. The RT thread does not touch tokio either.
//! - **No `.await` in the RT path.** [`RtInputProducer::try_push`] and
//!   [`RtOutputConsumer::try_pop`] are wait-free lock-free.
//! - **No shared lock between RT and worker.** `rtrb` rings are
//!   single-producer / single-consumer — the only synchronization is
//!   an atomic head/tail pair.
//! - **No hidden allocation inside the bridge.** RuntimeData payload owners move
//!   through the ring without copying their samples. Explicit AudioSamples::Vec
//!   supports move-in/move-out without allocating an owner header. Shared owners
//!   preserve payload allocations and pool returns, but constructing a new shared
//!   owner allocates reference-count metadata: do that off the RT callback or use
//!   a preallocated producer loan. The raw ring operations do not allocate.
//!   Payload construction, final release and handle destruction are separate
//!   operations and belong off the real-time callback.
//!
//! [`RtBridge::spawn_prepared_audio`] supplies a complete ownership path: the
//! worker prepares loans from the selected context allocator; capture borrows
//! samples through [`PreparedAudioProducer::try_fill`]; playback borrows outputs
//! through [`PreparedAudioConsumer::try_read`]. The worker freezes inputs and
//! releases consumed outputs. Queue or provider exhaustion returns immediately
//! without heap fallback. These callbacks must themselves avoid allocation and
//! blocking. Destroy both handles off the callback after stopping the device.
//!
//! # What it does *not* guarantee
//!
//! - The bridge is only as RT-safe as the node you put in it. If
//!   `node.process()` allocates, takes a lock, or calls a blocking
//!   syscall, the worker thread will hiccup — but the RT thread is
//!   protected, and the hiccup surfaces as a full input ring (drop
//!   in `try_push`) or an empty output ring (silence in `try_pop`),
//!   never as a parked audio callback.
//! - The `realtime` feature enables `thread_priority` and
//!   `core_affinity` on the worker. On Linux this requires
//!   `CAP_SYS_NICE` or suitable rlimit; on macOS it requires the
//!   RT entitlement (`com.apple.security.app-sandbox` +
//!   `com.apple.security.realtime-audio`). Fall back to the default
//!   feature-off build if those aren't available.
//!
//! # Public surface
//!
//! - [`RtBridge`] — owns the worker thread.
//! - [`RtInputProducer`] — **RT-safe** handle for pushing audio.
//! - [`RtOutputConsumer`] — **RT-safe** handle for pulling results.
//! - [`RtBridgeConfig`] — ring capacities, worker thread name, and
//!   (with the `realtime` feature) priority and core pinning.
//!
//! # Example
//!
//! ```ignore
//! use remotemedia_rt_bridge::{RtBridge, RtBridgeConfig, PreparedAudioConfig};
//! use crate::data::RuntimeData;
//! use crate::nodes::{SyncStreamingNode, NodeRuntimeContext};
//!
//! # fn example(node: impl SyncStreamingNode + 'static, context: NodeRuntimeContext)
//! # -> Result<(), remotemedia_rt_bridge::SpawnError> {
//! // Before starting the device callback:
//! let (bridge, mut producer, mut consumer) = RtBridge::spawn_prepared_audio(
//!     node, RtBridgeConfig::default(),
//!     PreparedAudioConfig {
//!         samples: 960, sample_rate: 48000, channels: 1, ready_capacity: 4,
//!     }, context,
//! )?;
//!
//! // Inside the callback, synthesize directly into the prepared allocation.
//! // Full/exhausted queues return an error without giving us an owner to drop.
//! let _ = producer.try_fill(None, |samples| samples.fill(0.0));
//! let _ = consumer.try_read(|data| {
//!     if let RuntimeData::Audio { samples, .. } = data {
//!         // Consume borrowed samples here; the worker releases their owner.
//!         // Copying into a device-owned buffer is a separate device adaptation.
//!         let _ = samples.len();
//!     }
//! });
//!
//! // After stopping callbacks, on the control thread:
//! drop(producer);
//! drop(consumer);
//! bridge.shutdown();
//! # Ok(())
//! # }
//! ```

#![warn(clippy::all)]
#![warn(missing_docs)]

mod bridge;
mod error;
mod handles;
mod playback;
mod prepared;
mod worker;

/// Test helpers shared with `audio_io` capture/playback tests. `allocations`
/// installs the counting global allocator for the lib test binary.
#[cfg(test)]
pub(crate) mod test_support {
    pub mod allocations;
    pub mod mapped;
    pub mod poll;
}

#[cfg(feature = "rt-bridge-realtime")]
mod realtime;

pub use bridge::{RtBridge, RtBridgeConfig, RtBridgeStats};
pub use error::{SpawnError, TryPushError};
pub use handles::{RtInputProducer, RtOutputConsumer};
pub use playback::{
    playback_queue, PlaybackConsumer, PlaybackError, PlaybackQueue, PlaybackReclaimer,
    PlaybackStats,
};
pub use prepared::{
    CaptureProgress, PreparedAudioConfig, PreparedAudioConsumer, PreparedAudioError,
    PreparedAudioProducer, PreparedCaptureProducer,
};
