//! Retained audio owners and deferred device-transfer accounting.

use crate::rt_bridge::prepared::publish_retained;
use remotemedia_types::{CopyContext, CopyOperation, CopyReason, CopyRejected, SharedBuffer};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};

/// Failure to admit audio or render a valid interleaved device buffer.
#[derive(Debug)]
pub enum PlaybackError {
    /// No queue slot is free. No samples were accepted.
    Full,
    /// The callback or retirement receiver has gone away.
    Stopped,
    /// Channel count, queue capacity or sample alignment is invalid.
    InvalidLayout,
    /// A sample total or generation cannot be represented.
    Overflow,
    /// The call's copy policy rejects the required device write.
    CopyRejected(CopyRejected),
}
impl std::fmt::Display for PlaybackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CopyRejected(error) => error.fmt(f),
            _ => write!(f, "playback: {self:?}"),
        }
    }
}
impl std::error::Error for PlaybackError {}

/// Current generation's admission, device-write and retirement progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlaybackStats {
    /// Samples admitted since the most recent clear.
    pub queued_samples: usize,
    /// Samples copied to a device output slice, not certified audible samples.
    pub rendered_samples: usize,
    /// Packets whose final retirement/accounting has not finished.
    pub pending_packets: usize,
}

#[derive(Default)]
struct Progress {
    queued: AtomicUsize,
    rendered: AtomicUsize,
    pending: AtomicUsize,
}

struct Packet {
    samples: SharedBuffer<f32>,
    copied: usize,
    generation: u64,
    progress: Arc<Progress>,
    operation: Option<CopyOperation>,
}
impl Drop for Packet {
    fn drop(&mut self) {
        // Packet destruction is exclusively an admission/control-thread operation.
        self.operation
            .take()
            .expect("single packet completion")
            .complete_bytes((self.copied as u64) * 4)
            .expect("device writes stay within the authorized packet");
        drop(std::mem::take(&mut self.samples));
        self.progress.pending.fetch_sub(1, Ordering::Release);
    }
}

/// Non-real-time producer. Queueing retains storage without sample staging.
/// Serialize calls externally if multiple producers share this handle.
pub struct PlaybackQueue {
    ready: rtrb::Producer<Packet>,
    generation: Arc<AtomicU64>,
    progress: Arc<Progress>,
    channels: usize,
}

/// Callback-side consumer. Methods do not allocate, lock or release owners.
/// Provider slice access must itself be real-time safe. Destroy this entire handle
/// off the callback thread after the device stops, including error/unwind paths.
pub struct PlaybackConsumer {
    ready: rtrb::Consumer<Packet>,
    retired: rtrb::Producer<Packet>,
    current: Option<Packet>,
    generation: Arc<AtomicU64>,
    channels: usize,
}

/// Control-thread retirement. Keep it alive until the device consumer returns.
pub struct PlaybackReclaimer(rtrb::Consumer<Packet>);

/// Prepare bounded packet and retirement queues before starting device callbacks.
/// `capacity` bounds each queue; at most one extra packet is held by the callback.
pub fn playback_queue(
    capacity: usize,
    channels: u16,
) -> Result<(PlaybackQueue, PlaybackConsumer, PlaybackReclaimer), PlaybackError> {
    if capacity == 0 || channels == 0 {
        return Err(PlaybackError::InvalidLayout);
    }
    let (tx, rx) = rtrb::RingBuffer::new(capacity);
    let (retire, reclaim) = rtrb::RingBuffer::new(capacity);
    let generation = Arc::new(AtomicU64::new(0));
    Ok((
        PlaybackQueue {
            ready: tx,
            generation: generation.clone(),
            progress: Arc::default(),
            channels: usize::from(channels),
        },
        PlaybackConsumer {
            ready: rx,
            retired: retire,
            current: None,
            generation,
            channels: usize::from(channels),
        },
        PlaybackReclaimer(reclaim),
    ))
}

impl PlaybackQueue {
    /// Admit an owning buffer with a call-specific device-copy authorization.
    /// Failure accepts nothing; its input owner is released on the calling thread.
    /// Empty buffers produce no packet or accounting event.
    pub fn try_queue(
        &mut self,
        samples: SharedBuffer<f32>,
        copies: &CopyContext,
    ) -> Result<(), PlaybackError> {
        if samples.len() % self.channels != 0 {
            return Err(PlaybackError::InvalidLayout);
        }
        if self.ready.is_abandoned() {
            return Err(PlaybackError::Stopped);
        }
        if samples.is_empty() {
            return Ok(());
        }
        if self.ready.slots() == 0 {
            return Err(PlaybackError::Full);
        }
        let total = self
            .progress
            .queued
            .load(Ordering::Relaxed)
            .checked_add(samples.len())
            .ok_or(PlaybackError::Overflow)?;
        let bytes = u64::try_from(samples.len())
            .ok()
            .and_then(|len| len.checked_mul(4))
            .ok_or(PlaybackError::Overflow)?;
        let operation = copies
            .begin(CopyReason::DeviceTransfer, bytes)
            .map_err(PlaybackError::CopyRejected)?;
        self.progress.pending.fetch_add(1, Ordering::Relaxed);
        let packet = Packet {
            samples,
            copied: 0,
            generation: self.generation.load(Ordering::Relaxed),
            progress: self.progress.clone(),
            operation: Some(operation),
        };
        // Only this serialized producer can consume a free slot. Keep an explicit
        // failure path anyway: its packet/accounting drops here, never on the RT side.
        self.ready.push(packet).map_err(|_| PlaybackError::Full)?;
        self.progress.queued.store(total, Ordering::Release);
        Ok(())
    }

    /// Invalidate queued/partial packets. An in-progress device write may finish;
    /// already submitted device samples cannot be retracted. Old owners remain
    /// retained until callback retirement or off-thread shutdown.
    pub fn clear(&mut self) -> Result<(), PlaybackError> {
        let next = self
            .generation
            .load(Ordering::Relaxed)
            .checked_add(1)
            .ok_or(PlaybackError::Overflow)?;
        self.progress = Arc::default();
        self.generation.store(next, Ordering::Release);
        Ok(())
    }

    /// Read progress for the current generation. Completion includes accounting.
    pub fn stats(&self) -> PlaybackStats {
        let pending_packets = self.progress.pending.load(Ordering::Acquire);
        PlaybackStats {
            queued_samples: self.progress.queued.load(Ordering::Acquire),
            rendered_samples: self.progress.rendered.load(Ordering::Acquire),
            pending_packets,
        }
    }
}

impl PlaybackConsumer {
    /// Fill complete interleaved frames, then silence. Return the number of sample
    /// values copied from queued owners. Retirement pressure preserves the current
    /// packet and stops this callback's consumption; it never drops a last owner.
    pub fn render(&mut self, output: &mut [f32]) -> Result<usize, PlaybackError> {
        output.fill(0.0);
        if output.len() % self.channels != 0 {
            return Err(PlaybackError::InvalidLayout);
        }
        if self.retired.is_abandoned() {
            return Err(PlaybackError::Stopped);
        }
        let mut written = 0;
        // Bound work even if another thread continuously fills/clears the queue.
        for _ in 0..=self.ready.buffer().capacity() {
            if self.current.is_none() {
                self.current = self.ready.pop().ok();
            }
            let Some(packet) = self.current.as_mut() else {
                break;
            };
            if packet.generation == self.generation.load(Ordering::Acquire)
                && written < output.len()
            {
                let count = (packet.samples.len() - packet.copied).min(output.len() - written);
                output[written..written + count]
                    .copy_from_slice(&packet.samples[packet.copied..packet.copied + count]);
                packet.copied += count;
                packet.progress.rendered.fetch_add(count, Ordering::Release);
                written += count;
            }
            if packet.generation == self.generation.load(Ordering::Acquire)
                && packet.copied != packet.samples.len()
            {
                break;
            }
            if publish_retained(&mut self.current, &mut self.retired).is_err() {
                break;
            }
            if written == output.len() {
                break;
            }
        }
        Ok(written)
    }

    /// Stop accepting input and release current/queued packets off the RT thread.
    /// Stop admission first, then stop callbacks, then call this method. Queued
    /// retirement packets are released separately by the reclaimer.
    pub fn discard(mut self) {
        drop(self.current.take());
        while let Ok(packet) = self.ready.pop() {
            drop(packet);
        }
    }
}

impl PlaybackReclaimer {
    /// Complete accounting and release at most one queue capacity of retired
    /// packets. Call periodically on a worker/control thread, never in a callback.
    pub fn reclaim(&mut self) -> usize {
        let mut count = 0;
        for _ in 0..self.0.buffer().capacity() {
            let Ok(packet) = self.0.pop() else {
                break;
            };
            drop(packet);
            count += 1;
        }
        count
    }
}
