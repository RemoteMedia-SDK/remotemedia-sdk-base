//! Prepared audio ingress and deferred output release for callbacks.

use crate::rt_bridge::SpawnError;
use crate::{data::RuntimeData, nodes::NodeRuntimeContext};
use remotemedia_types::{BufferLoan, CopyOperation, CopyReason};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Fixed interleaved audio layout and number of loans prepared ahead of capture.
#[derive(Clone, Copy, Debug)]
pub struct PreparedAudioConfig {
    /// Samples per packet, including every channel. Must be positive and divisible by channels.
    pub samples: usize,
    /// Sample rate in Hz. Must be positive.
    pub sample_rate: u32,
    /// Interleaved channels. Must be positive.
    pub channels: u16,
    /// Number of initialized loans to keep ready. Must be positive.
    pub ready_capacity: usize,
}

/// A bounded callback operation could not proceed. No payload ownership escapes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparedAudioError {
    /// The worker has stopped or shutdown was requested.
    Stopped,
    /// An input or reclamation queue is full. Retry on a later callback.
    Full,
    /// No prepared loan is ready, including when the selected allocator is exhausted.
    Unavailable,
    /// Input does not contain a whole number of interleaved channel frames.
    InvalidLayout,
}

struct AudioFrame {
    loan: BufferLoan<f32>,
    timestamp_us: Option<u64>,
    filled: usize,
    capture: Option<CaptureAccounting>,
}

// Only counters are touched on the callback. The owning frame is released on
// the worker or, after callbacks stop, the control thread, including on failure.
struct CaptureAccounting {
    operation: Option<CopyOperation>,
    copied_bytes: u64,
}

impl Drop for CaptureAccounting {
    fn drop(&mut self) {
        if let Some(operation) = self.operation.take() {
            operation
                .complete_bytes(self.copied_bytes)
                .expect("capture copies stay within the prepared loan");
        }
    }
}

/// Callback handle that fills storage prepared by the selected context allocator.
///
/// Queue operations never allocate, free owners, lock or wait. The selected
/// provider's mutable slice access and the supplied closure must obey the same
/// constraints. Spawning rejects providers whose `prepared_realtime_support()`
/// is not `Verified` (heap and mapped pool are; native ABI imported loans are
/// tested but need the explicit `AssumePreparedRealtime` opt-in). Fill the
/// complete packet. A panic
/// retains the loan in this handle; unwinding itself is not real-time safe.
/// Create and destroy this handle outside the callback, after stopping capture.
pub struct PreparedAudioProducer {
    ready: rtrb::Consumer<AudioFrame>,
    input: rtrb::Producer<AudioFrame>,
    current: Option<AudioFrame>,
    stopped: Arc<AtomicBool>,
}

impl PreparedAudioProducer {
    /// Fill and enqueue one fixed-size packet without copying its sample storage.
    /// The closure is not called on backpressure, exhaustion or shutdown.
    pub fn try_fill(
        &mut self,
        timestamp_us: Option<u64>,
        fill: impl FnOnce(&mut [f32]),
    ) -> Result<(), PreparedAudioError> {
        if self.stopped.load(Ordering::Acquire) || self.input.is_abandoned() {
            return Err(PreparedAudioError::Stopped);
        }
        if self.input.slots() == 0 {
            return Err(PreparedAudioError::Full);
        }
        if self.current.is_none() {
            let frame = self
                .ready
                .pop()
                .map_err(|_| PreparedAudioError::Unavailable)?;
            self.current = Some(frame);
        }
        let frame = self.current.as_mut().expect("prepared loan retained");
        fill(frame.loan.as_mut_slice());
        frame.filled = frame.loan.len();
        frame.timestamp_us = timestamp_us;
        publish_retained(&mut self.current, &mut self.input)
    }
}

/// Borrow output during a callback; release its owner on the bridge worker.
///
/// The callback must not allocate, block, or retain clones whose last release
/// would occur on the real-time thread. This handle retains outputs if the return
/// queue fills or the callback panics. Destroy the handle off the real-time thread.
pub struct PreparedAudioConsumer {
    output: rtrb::Consumer<RuntimeData>,
    retired: rtrb::Producer<RuntimeData>,
    current: Option<RuntimeData>,
    stopped: Arc<AtomicBool>,
}

impl PreparedAudioConsumer {
    /// Take remaining owned outputs after joining the worker. Call and consume
    /// this iterator off the callback thread: releasing these owners may lock,
    /// deallocate, unmap or call foreign code. Previously read output awaiting
    /// retirement is released, not delivered a second time.
    pub fn drain_after_shutdown(self) -> impl Iterator<Item = RuntimeData> {
        let mut output = self.output;
        std::iter::from_fn(move || output.pop().ok())
    }

    /// Read the next output by reference. `Ok(None)` means no output is ready.
    /// Queue backpressure leaves owners in this handle until a later call.
    pub fn try_read<T>(
        &mut self,
        read: impl FnOnce(&RuntimeData) -> T,
    ) -> Result<Option<T>, PreparedAudioError> {
        if self.stopped.load(Ordering::Acquire) || self.retired.is_abandoned() {
            return Err(PreparedAudioError::Stopped);
        }
        publish_retained(&mut self.current, &mut self.retired)?;
        self.current = self.output.pop().ok();
        let Some(data) = self.current.as_ref() else {
            return Ok(None);
        };
        let result = read(data);
        let _ = publish_retained(&mut self.current, &mut self.retired);
        Ok(Some(result))
    }
}

// All callback queues retain ownership on failure; no last-owner release can
// run on this path. Shared by generated input, captured input and output return.
pub(crate) fn publish_retained<T>(
    current: &mut Option<T>,
    queue: &mut rtrb::Producer<T>,
) -> Result<(), PreparedAudioError> {
    if let Some(value) = current.take() {
        if let Err(rtrb::PushError::Full(value)) = queue.push(value) {
            *current = Some(value);
            return Err(PreparedAudioError::Full);
        }
    }
    Ok(())
}

pub(crate) struct PreparedWorker {
    input: rtrb::Consumer<AudioFrame>,
    ready: rtrb::Producer<AudioFrame>,
    retired: rtrb::Consumer<RuntimeData>,
    pub(crate) context: NodeRuntimeContext,
    config: PreparedAudioConfig,
    capture: bool,
}

impl PreparedWorker {
    #[cfg(test)]
    pub(crate) fn new(
        context: NodeRuntimeContext,
        config: PreparedAudioConfig,
        input_capacity: usize,
        output: rtrb::Consumer<RuntimeData>,
        stopped: Arc<AtomicBool>,
    ) -> Result<(Self, PreparedAudioProducer, PreparedAudioConsumer), SpawnError> {
        Self::prepare(context, config, input_capacity, output, stopped, false)
    }

    pub(crate) fn prepare(
        context: NodeRuntimeContext,
        config: PreparedAudioConfig,
        input_capacity: usize,
        output: rtrb::Consumer<RuntimeData>,
        stopped: Arc<AtomicBool>,
        capture: bool,
    ) -> Result<(Self, PreparedAudioProducer, PreparedAudioConsumer), SpawnError> {
        if config.samples == 0
            || config.channels == 0
            || config.sample_rate == 0
            || config.samples % usize::from(config.channels) != 0
            || config.ready_capacity == 0
        {
            return Err(SpawnError::PreparedAudio(
                "invalid fixed audio layout or loan capacity".into(),
            ));
        }
        let bytes = (config.samples as u64)
            .checked_mul(4)
            .ok_or_else(|| SpawnError::PreparedAudio("audio payload byte count overflow".into()))?;
        // Reject providers not validated for prepared real-time use before any
        // loan exists. Callers that validated their own provider opt in with
        // `remotemedia_types::AssumePreparedRealtime`.
        if context.output_allocator.prepared_realtime_support()
            != remotemedia_types::PreparedRealtimeSupport::Verified
        {
            return Err(SpawnError::PreparedAudio(
                concat!(
                    "the selected output provider is not validated for prepared real-time use; ",
                    "use a heap or mapped-pool provider, or wrap a validated provider in ",
                    "AssumePreparedRealtime"
                )
                .into(),
            ));
        }
        if capture {
            context
                .materialization
                .check(CopyReason::BorrowedInput, bytes)
                .map_err(|error| SpawnError::PreparedAudio(error.to_string()))?;
        }
        let (input_tx, input) = rtrb::RingBuffer::new(input_capacity);
        let (ready, ready_rx) = rtrb::RingBuffer::new(config.ready_capacity);
        let (retired_tx, retired) = rtrb::RingBuffer::new(output.buffer().capacity());
        let mut worker = Self {
            input,
            ready,
            retired,
            context,
            config,
            capture,
        };
        worker
            .refill()
            .map_err(|e| SpawnError::PreparedAudio(e.to_string()))?;
        let producer = PreparedAudioProducer {
            ready: ready_rx,
            input: input_tx,
            current: None,
            stopped: stopped.clone(),
        };
        let consumer = PreparedAudioConsumer {
            output,
            retired: retired_tx,
            current: None,
            stopped,
        };
        Ok((worker, producer, consumer))
    }

    pub(crate) fn service(&mut self) {
        // Destructors may unmap, lock provider state, or run foreign release code.
        while let Ok(data) = self.retired.pop() {
            drop(data);
        }
        // Exhaustion leaves the ready queue short. Never switch memory domains.
        let _ = self.refill();
    }

    fn refill(&mut self) -> Result<(), remotemedia_types::BufferAllocationError> {
        // Bound each round even if capture drains concurrently.
        for _ in 0..self.ready.slots() {
            let loan = self
                .context
                .output_allocator
                .allocate_audio(self.config.samples)?;
            // Validate foreign providers off the callback before exposing a slice.
            if loan.len() != self.config.samples {
                return Err(remotemedia_types::BufferAllocationError::InvalidLength);
            }
            let capture = self.capture.then(|| CaptureAccounting {
                operation: Some(
                    self.context
                        .materialization
                        .begin(CopyReason::BorrowedInput, (self.config.samples as u64) * 4)
                        .expect("immutable copy policy checked before preparation"),
                ),
                copied_bytes: 0,
            });
            let _ = self.ready.push(AudioFrame {
                loan,
                timestamp_us: None,
                filled: 0,
                capture,
            });
        }
        Ok(())
    }

    pub(crate) fn pop(&mut self) -> Option<RuntimeData> {
        let frame = self.input.pop().ok()?;
        Some(RuntimeData::Audio {
            samples: frame
                .loan
                .freeze()
                .slice(0..frame.filled)
                .expect("filled range fits the prepared loan")
                .into(),
            sample_rate: self.config.sample_rate,
            channels: u32::from(self.config.channels),
            timestamp_us: frame.timestamp_us,
            stream_id: None,
            arrival_ts_us: None,
            metadata: None,
        })
    }
}

#[cfg(test)]
mod tests;

mod capture;
pub use capture::{CaptureProgress, PreparedCaptureProducer};
