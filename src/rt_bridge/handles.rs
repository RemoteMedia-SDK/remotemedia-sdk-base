//! Real-time-safe producer and consumer handles.
//!
//! Both handles wrap a `rtrb` half-ring and expose only wait-free,
//! lock-free operations. Every method is inlined so the hot path
//! compiles to a handful of atomic loads / stores without any function
//! call overhead.

use crate::data::RuntimeData;
use crate::rt_bridge::error::TryPushError;

/// **REAL-TIME SAFE** producer handle.
///
/// Held by the RT callback (Core Audio HAL IO proc, AU render callback,
/// etc.). Push [`RuntimeData`] values toward the bridge worker via
/// [`try_push`]. This type is `Send` so it can be moved into the RT
/// callback closure, but it is **not** `Sync` — only one producer may
/// write at a time (`rtrb` is single-producer).
///
/// # RT-safety
///
/// All methods are wait-free: they perform at most a bounded sequence
/// of atomic operations. No heap allocation, no syscalls, no locking.
/// Constructing or finally dropping payloads can allocate, free, lock or invoke
/// foreign release code. Defer those operations and handle destruction off the
/// callback, or use [`crate::rt_bridge::RtBridge::spawn_prepared_audio`].
pub struct RtInputProducer {
    producer: rtrb::Producer<RuntimeData>,
}

impl RtInputProducer {
    pub(crate) fn new(producer: rtrb::Producer<RuntimeData>) -> Self {
        Self { producer }
    }

    /// Push `data` toward the worker thread.
    ///
    /// Returns `Ok(())` if the data was enqueued, or
    /// `Err(TryPushError::Full(data))` if the input ring is full — in
    /// which case the caller still owns the value and must defer its final
    /// release off the real-time thread. **Never blocks.**
    #[inline]
    pub fn try_push(&mut self, data: RuntimeData) -> Result<(), TryPushError> {
        match self.producer.push(data) {
            Ok(()) => Ok(()),
            Err(rtrb::PushError::Full(d)) => Err(TryPushError::Full(d)),
        }
    }

    /// Number of free slots, viewed from this producer's side.
    ///
    /// Lock-free. The value is a lower bound — the worker may pop in
    /// parallel, so by the time you act on this reading the real count
    /// may be larger. Useful as a backpressure hint.
    #[inline]
    pub fn slots(&self) -> usize {
        self.producer.slots()
    }

    /// Total ring capacity (fixed for the bridge's lifetime).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.producer.buffer().capacity()
    }
}

// Safety: rtrb::Producer is Send when T is Send.
// RuntimeData is Send, so the handle can move between threads
// (useful for handing the producer into an RT callback).
unsafe impl Send for RtInputProducer {}

/// **REAL-TIME SAFE** consumer handle.
///
/// Held by the RT callback so it can drain processed outputs back into
/// the HAL speaker buffer. `Send` but not `Sync` — single-consumer.
///
/// # RT-safety
///
/// Wait-free. See [`RtInputProducer`] notes.
pub struct RtOutputConsumer {
    consumer: rtrb::Consumer<RuntimeData>,
}

impl RtOutputConsumer {
    pub(crate) fn new(consumer: rtrb::Consumer<RuntimeData>) -> Self {
        Self { consumer }
    }

    /// Pop the next available output.
    ///
    /// Returns `Some(data)` if the worker has produced an output since
    /// the last call, `None` if the output ring is empty. **Never
    /// blocks.**
    #[inline]
    pub fn try_pop(&mut self) -> Option<RuntimeData> {
        self.consumer.pop().ok()
    }

    /// Number of outputs currently waiting to be consumed.
    ///
    /// Lock-free, lower bound (see [`RtInputProducer::slots`]).
    #[inline]
    pub fn slots(&self) -> usize {
        self.consumer.slots()
    }

    /// Total ring capacity (fixed for the bridge's lifetime).
    #[inline]
    pub fn capacity(&self) -> usize {
        self.consumer.buffer().capacity()
    }
}

// Safety: rtrb::Consumer is Send when T is Send. Same as producer.
unsafe impl Send for RtOutputConsumer {}
