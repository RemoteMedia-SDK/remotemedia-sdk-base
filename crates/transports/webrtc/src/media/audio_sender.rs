//! Audio sender with ring buffer and dedicated transmission thread
//!
//! This module provides a ring buffer-based audio transmission system that:
//! - Decouples audio production from transmission
//! - Maintains consistent real-time playback timing
//! - Handles backpressure gracefully via ring buffer
//! - Uses a dedicated OS thread for precise timing

// Phase 4 (US2) audio transmission infrastructure
#![allow(dead_code)]

use crate::{Error, Result};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Notify};
use tracing::{debug, warn};
use webrtc::media::Sample;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;

use remotemedia_core::transport::session_control::{MediaClock, SessionControl};

/// Per-stream media_clock publisher. Attached to an AudioSender via
/// [`AudioSender::with_media_clock`]; when present, the transmission
/// thread publishes one [`MediaClock`] event per successful Opus frame
/// send.
///
/// Consumers (`ClockedToOutboundMedia` nodes' `WireTickSource`) read
/// `pts_us` (sender-side wall clock relative to `origin`) and
/// `wire_deadline_us` (one frame budget ahead) to drive `tick()` at
/// the sender's actual outbound cadence.
///
/// Cheap to clone (every field Arc-backed); the AudioSender's Clone
/// impl shares the same publisher with the transmission thread.
#[derive(Clone)]
pub struct MediaClockPublisher {
    control: Arc<SessionControl>,
    addr: String,
    medium: String,
    stream_id: String,
    frame_idx: Arc<AtomicU64>,
    origin: Instant,
}

impl MediaClockPublisher {
    /// Build a publisher for the given (medium, stream_id). `addr` is
    /// computed as `<medium>:<stream_id>` to match the
    /// `WireTickSource` subscription scheme.
    pub fn new(
        control: Arc<SessionControl>,
        medium: impl Into<String>,
        stream_id: impl Into<String>,
    ) -> Self {
        let medium = medium.into();
        let stream_id = stream_id.into();
        Self {
            control,
            addr: format!("{}:{}", medium, stream_id),
            medium,
            stream_id,
            frame_idx: Arc::new(AtomicU64::new(0)),
            origin: Instant::now(),
        }
    }

    /// Publish one tick for a frame whose duration is `frame_duration`.
    /// `pts_us` is the wall-clock microseconds since this publisher was
    /// created; `wire_deadline_us = pts_us + frame_duration_us` (the
    /// next frame must be queued by then).
    pub fn publish(&self, frame_duration: Duration) {
        let pts_us = self.origin.elapsed().as_micros() as u64;
        let frame_idx = self.frame_idx.fetch_add(1, Ordering::Relaxed);
        let dur_us = frame_duration.as_micros() as u64;
        self.control.publish_media_clock(
            &self.addr,
            MediaClock {
                medium: self.medium.clone(),
                stream_id: self.stream_id.clone(),
                pts_us,
                frame_idx,
                wire_deadline_us: pts_us.saturating_add(dur_us),
            },
        );
    }

    /// Stop the wire clock — closes the broadcast so subscribed
    /// `WireTickSource`s exit their `next()` with `None`. Idempotent.
    /// Called on AudioSender shutdown / peer disconnect (Phase 5.9).
    pub fn stop(&self) {
        self.control.stop_media_clock(&self.addr);
    }
}

/// Audio frame ready for transmission
#[derive(Clone)]
struct AudioFrame {
    /// Encoded Opus data
    data: bytes::Bytes,
    /// Number of samples this frame represents (for timestamp calculation)
    sample_count: u32,
    /// Frame duration
    duration: Duration,
}

/// Ring buffer for audio frames
struct AudioRingBuffer {
    /// A single lock serializes publication, pop, flush and shutdown. Moving
    /// these owners never moves their encoded payload allocation.
    buffer: parking_lot::Mutex<VecDeque<AudioFrame>>,
    /// Usable capacity, preserving the previous capacity-minus-one convention.
    capacity: usize,
    /// Wakes any task awaiting space in the buffer. Notified on every
    /// successful pop (one slot freed) and on `clear()` (all slots
    /// freed at once). Producers use it to implement async
    /// backpressure rather than dropping frames on overflow.
    space_available: Notify,
    /// Shutdown signal. Producers awaiting space check this on each
    /// retry so they bail out instead of hanging when the sender is
    /// being torn down.
    shutdown: AtomicBool,
    /// Separate from capacity notifications so the sender cannot consume a
    /// producer's space-available wakeup while waiting for closure.
    closed: Notify,
}

impl AudioRingBuffer {
    /// Create a new ring buffer with given capacity
    fn new(capacity: usize) -> Self {
        // Degenerate configurations still admit one frame instead of dividing
        // by zero or leaving every producer permanently backpressured.
        let capacity = capacity.max(2) - 1;
        Self {
            buffer: parking_lot::Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            space_available: Notify::new(),
            shutdown: AtomicBool::new(false),
            closed: Notify::new(),
        }
    }

    /// Async-backpressuring push.
    ///
    /// If the buffer has room, push immediately. If it's full, wait
    /// for the consumer thread to free a slot (or for `clear()` to
    /// drain on barge-in) and retry. This is the path producers
    /// should use; the previous "drop frame on full" behaviour
    /// caused audible glitches under long TTS replies.
    ///
    /// Returns `Err` only on shutdown — the buffer being full is
    /// not an error here, just a wait condition.
    async fn push(&self, frame: AudioFrame) -> Result<()> {
        let mut frame = Some(frame);
        loop {
            // Register interest BEFORE the try_push attempt. If the
            // consumer drains a slot between try_push() and notified()
            // the permit is held by the Notify and the next
            // notified().await returns immediately — no missed wakeups.
            let waiter = self.space_available.notified();
            tokio::pin!(waiter);
            // Enable the waiter so it captures any notifications that
            // arrive between this point and the next .await.
            waiter.as_mut().enable();
            if self.shutdown.load(Ordering::Acquire) {
                return Err(Error::MediaTrackError(
                    "audio sender shutdown — refusing to enqueue frame".to_string(),
                ));
            }

            match self.try_push_internal(frame.take().expect("frame consumed only on success")) {
                Ok(()) => return Ok(()),
                Err(rejected) => {
                    frame = Some(rejected);
                    waiter.await;
                }
            }
        }
    }

    /// Try to push a frame into the buffer (non-blocking).
    /// Returns Ok(()) if successful, Err if buffer is full.
    ///
    /// Kept on the public surface for callers that genuinely want
    /// drop-on-full semantics (currently none in the production path,
    /// but we leave it because tests + the existing API contract
    /// reference it).
    fn try_push(&self, frame: AudioFrame) -> Result<()> {
        self.try_push_internal(frame).map_err(|_| {
            Error::MediaTrackError("Audio ring buffer full - dropping frame".to_string())
        })
    }

    /// Internal try-push that hands the frame back on rejection so
    /// the async `push` can retry without cloning. `Err(frame)` =
    /// "buffer was full, here's your frame back."
    fn try_push_internal(&self, frame: AudioFrame) -> std::result::Result<(), AudioFrame> {
        let mut buffer = self.buffer.lock();
        if self.shutdown.load(Ordering::Acquire) || buffer.len() == self.capacity {
            return Err(frame);
        }
        buffer.push_back(frame);
        Ok(())
    }

    /// Try to pop a frame from the buffer
    /// Returns None if buffer is empty
    fn try_pop(&self) -> Option<AudioFrame> {
        let frame = self.buffer.lock().pop_front()?;

        // Wake any producer parked on `push().await` waiting for
        // space. Notify is `Send + Sync` and `notify_one` is callable
        // from a non-tokio thread, so this is safe from the std::thread
        // transmission loop. If no one is waiting, the permit is
        // queued and the next `push()` consumes it without waiting.
        self.space_available.notify_one();

        Some(frame)
    }

    /// Get the number of frames currently in the buffer
    fn len(&self) -> usize {
        self.buffer.lock().len()
    }

    /// Drain the ring buffer without stopping the sender thread.
    ///
    /// Called on barge-in. An already-popped frame remains owned by the sender;
    /// queued owners are released atomically with respect to push and pop.
    fn clear(&self) -> usize {
        let mut buffer = self.buffer.lock();
        let dropped = buffer.len();
        let discarded = std::mem::take(&mut *buffer);
        drop(buffer);
        drop(discarded);
        // Wake every parked producer — clearing freed every slot at
        // once. A single `notify_one` would only wake one of them and
        // leave the rest sleeping despite the buffer being empty.
        self.space_available.notify_waiters();
        dropped
    }

    /// Mark the ring buffer as shutting down and wake every parked
    /// producer so they observe the flag and return promptly.
    fn signal_shutdown(&self) {
        let mut buffer = self.buffer.lock();
        self.shutdown.store(true, Ordering::Release);
        let discarded = std::mem::take(&mut *buffer);
        drop(buffer);
        drop(discarded);
        self.space_available.notify_waiters();
        self.closed.notify_waiters();
    }

    async fn wait_closed(&self) {
        let closed = self.closed.notified();
        tokio::pin!(closed);
        closed.as_mut().enable();
        if !self.shutdown.load(Ordering::Acquire) {
            closed.await;
        }
    }

    /// Cancel a pending transport write or pacing wait when ownership closes.
    async fn run_until_closed<T>(&self, work: impl std::future::Future<Output = T>) -> Option<T> {
        tokio::select! {
            biased;
            _ = self.wait_closed() => None,
            output = work => Some(output),
        }
    }

    fn wait_for(&self, runtime: &tokio::runtime::Runtime, duration: Duration) -> bool {
        runtime
            .block_on(async { self.run_until_closed(tokio::time::sleep(duration)).await })
            .is_some()
    }
}

/// Audio sender with ring buffer and dedicated transmission thread
pub struct AudioSender {
    /// Ring buffer for audio frames
    buffer: Arc<AudioRingBuffer>,
    /// WebRTC track for transmission
    track: Arc<TrackLocalStaticSample>,
    /// Shutdown signal
    shutdown: Arc<AtomicBool>,
    /// Current RTP timestamp
    timestamp: Arc<AtomicU32>,
    /// Thread handle (wrapped in Mutex for Drop)
    thread_handle: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
    /// Optional media-clock publisher (Phase 5.1). When attached via
    /// `with_media_clock`, the transmission thread fires one
    /// [`MediaClock`] event per successful Opus frame send.
    /// `parking_lot::RwLock` because it's set once at construction and
    /// otherwise only read by the transmission thread.
    media_clock: Arc<parking_lot::RwLock<Option<MediaClockPublisher>>>,
    /// Only sender handles own this guard; the transmission thread must not.
    lifetime: Arc<SenderLifetime>,
}

/// Arc's final release, rather than a racy strong-count snapshot, requests stop
/// when the last sender handle is dropped. Explicit shutdown additionally joins.
struct SenderLifetime {
    buffer: Arc<AudioRingBuffer>,
    shutdown: Arc<AtomicBool>,
    media_clock: Arc<parking_lot::RwLock<Option<MediaClockPublisher>>>,
}

impl Drop for SenderLifetime {
    fn drop(&mut self) {
        self.request_shutdown();
    }
}

impl SenderLifetime {
    fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.buffer.signal_shutdown();
        if let Some(publisher) = self.media_clock.write().take() {
            publisher.stop();
        }
    }
}

impl AudioSender {
    /// Create a new audio sender
    ///
    /// # Arguments
    ///
    /// * `track` - WebRTC track for transmission
    /// * `buffer_capacity` - Ring buffer capacity in frames
    ///   - Recommended: 1500 frames = 30 seconds @ 20ms (allows TTS burst generation)
    ///   - Minimum: 200 frames = 4 seconds @ 20ms
    pub fn new(track: Arc<TrackLocalStaticSample>, buffer_capacity: usize) -> Self {
        let buffer = Arc::new(AudioRingBuffer::new(buffer_capacity));
        let shutdown = Arc::new(AtomicBool::new(false));
        let timestamp = Arc::new(AtomicU32::new(0));

        let media_clock: Arc<parking_lot::RwLock<Option<MediaClockPublisher>>> =
            Arc::new(parking_lot::RwLock::new(None));

        let sender = Self {
            buffer: Arc::clone(&buffer),
            track: Arc::clone(&track),
            shutdown: Arc::clone(&shutdown),
            timestamp: Arc::clone(&timestamp),
            thread_handle: Arc::new(Mutex::new(None)),
            media_clock: Arc::clone(&media_clock),
            lifetime: Arc::new(SenderLifetime {
                buffer: buffer.clone(),
                shutdown: shutdown.clone(),
                media_clock: media_clock.clone(),
            }),
        };

        // Start the transmission thread
        let thread_buffer = Arc::clone(&buffer);
        let thread_track = Arc::clone(&track);
        let thread_shutdown = Arc::clone(&shutdown);
        let thread_timestamp = Arc::clone(&timestamp);
        let thread_media_clock = Arc::clone(&media_clock);

        let handle = std::thread::Builder::new()
            .name("audio-sender".to_string())
            .spawn(move || {
                Self::transmission_thread(
                    thread_buffer,
                    thread_track,
                    thread_shutdown,
                    thread_timestamp,
                    thread_media_clock,
                )
            })
            .expect("Failed to spawn audio sender thread");

        // Publish before returning: immediate shutdown must always see the
        // thread it must join, even before the caller's runtime yields.
        *sender
            .thread_handle
            .try_lock()
            .expect("new sender handle lock") = Some(handle);

        use tracing::info;
        info!(
            "AudioSender created with buffer capacity: {} frames",
            buffer_capacity
        );
        sender
    }

    /// Transmission thread - runs on dedicated OS thread for precise timing
    fn transmission_thread(
        buffer: Arc<AudioRingBuffer>,
        track: Arc<TrackLocalStaticSample>,
        shutdown: Arc<AtomicBool>,
        timestamp: Arc<AtomicU32>,
        media_clock: Arc<parking_lot::RwLock<Option<MediaClockPublisher>>>,
    ) {
        use tracing::info;
        // Use println as a backup in case tracing isn't working
        println!("[AUDIO-SENDER-THREAD] Thread spawned!");
        info!("Audio transmission thread started - creating tokio runtime");

        // Create a dedicated tokio runtime for this thread
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(rt) => {
                info!("Audio transmission thread: tokio runtime created successfully");
                rt
            }
            Err(e) => {
                warn!("Failed to create tokio runtime for audio sender: {}", e);
                return;
            }
        };

        let mut frame_count = 0u64;
        let mut loop_iterations = 0u64;
        let start_time = Instant::now();

        // Deadline-based pacing. `next_deadline` is the wall-clock time
        // the *next* frame is allowed to be sent. Sleep-after-work (the
        // old `std::thread::sleep(frame.duration)`) ignored the cost of
        // `write_sample`, so a ~10ms write + 20ms sleep produced a 30ms
        // frame period — the sender ran at ~0.66× real-time and the
        // receiver's jitter buffer drained.
        //
        // `None` means "no active stream yet" (either we just started or
        // the buffer emptied) — when the next frame arrives we'll anchor
        // the deadline at "now", not catch up to a stale clock.
        let mut next_deadline: Option<Instant> = None;
        // Catch-up clamp: if we fall this far behind real-time (e.g.
        // a write_sample stalled or we were descheduled), don't try to
        // burst N frames in a row to make up — that would just shove a
        // pile of packets at the encoder. Reset the deadline to "now"
        // and continue.
        const MAX_CATCHUP: Duration = Duration::from_millis(100);

        info!(
            "Audio transmission thread: entering main loop, shutdown={}",
            shutdown.load(Ordering::Acquire)
        );

        while !shutdown.load(Ordering::Acquire) {
            loop_iterations += 1;

            // Log first few iterations for debugging
            if loop_iterations <= 3 {
                let buffer_len = buffer.len();
                info!(
                    "Audio sender: Loop iteration {}, shutdown={}, buffer_len={}",
                    loop_iterations,
                    shutdown.load(Ordering::Acquire),
                    buffer_len
                );
            }

            // Try to get a frame from the buffer
            let Some(frame) = buffer.try_pop() else {
                // Empty streams re-anchor pacing at the next arrival.
                next_deadline = None;
                if !buffer.wait_for(&rt, Duration::from_millis(5)) {
                    break;
                }
                continue;
            };
            // Update RTP timestamp
            let old_ts = timestamp.fetch_add(frame.sample_count, Ordering::AcqRel);

            // Create WebRTC sample
            let sample = Sample {
                data: frame.data,
                duration: frame.duration,
                timestamp: std::time::SystemTime::now(),
                ..Default::default()
            };

            // Send the sample using the dedicated runtime
            let track_clone = Arc::clone(&track);
            let Some(send_result) = rt.block_on(async {
                buffer
                    .run_until_closed(track_clone.write_sample(&sample))
                    .await
            }) else {
                break;
            };

            if let Err(e) = send_result {
                warn!("Failed to send audio frame: {}", e);
            } else {
                frame_count += 1;

                // Phase 5.1: publish a media_clock tick after each
                // successful Opus frame send. Subscribers (wire-
                // bound `WireTickSource`s driving Pacer instances
                // for `ClockedToOutboundMedia` nodes) read the
                // event and invoke their node's `tick()` once per
                // outbound frame. No-op when no publisher is
                // attached.
                let publisher = media_clock.read().clone();
                if let Some(publisher) = publisher {
                    publisher.publish(frame.duration);
                }

                // Log first frame and periodically
                if frame_count == 1 {
                    info!("Audio sender: FIRST frame sent successfully!");
                } else if frame_count.is_multiple_of(50) {
                    let elapsed = start_time.elapsed().as_secs_f64();
                    let buffer_len = buffer.len();
                    info!(
                        "Audio sender: {} frames sent in {:.2}s, buffer: {} frames, timestamp: {}",
                        frame_count, elapsed, buffer_len, old_ts
                    );
                }
            }

            // PACING: sleep until the next frame's wall-clock
            // deadline, accounting for the time write_sample took.
            let now = Instant::now();
            let deadline = match next_deadline {
                // Stream just (re)started: anchor here so the first
                // frame after an empty buffer doesn't sprint.
                None => now + frame.duration,
                // Behind by more than MAX_CATCHUP — give up the
                // catch-up budget and re-anchor at now. Avoids
                // bursting packets that would only make jitter worse.
                Some(d) if now > d + MAX_CATCHUP => now + frame.duration,
                Some(d) => d + frame.duration,
            };
            next_deadline = Some(deadline);
            if let Some(wait) = deadline.checked_duration_since(now) {
                if !buffer.wait_for(&rt, wait) {
                    break;
                }
            }
            // If `wait` was None we're behind schedule — skip the
            // sleep and let the next iteration pop the next frame
            // immediately. The deadline stays consistent so we
            // self-correct on the very next on-time frame.
        }

        info!(
            "Audio transmission thread shutting down (sent {} frames, {} loop iterations, shutdown={})",
            frame_count,
            loop_iterations,
            shutdown.load(Ordering::Acquire)
        );
    }

    /// Enqueue an audio frame for transmission.
    ///
    /// Applies async backpressure: if the ring buffer is full, this
    /// awaits a slot opening up rather than dropping the frame. The
    /// wait cascades naturally through the calling drain task →
    /// session output channel → fan task → TTS callback, eventually
    /// stalling the producer instead of dropping audio.
    ///
    /// Returns `Err` only if the sender has been shut down — a full
    /// buffer is no longer an error condition.
    ///
    /// # Arguments
    ///
    /// * `encoded_data` - Opus-encoded audio data
    /// * `sample_count` - Number of audio samples this frame represents
    /// * `duration` - Frame duration (typically 20ms)
    pub async fn enqueue_frame(
        &self,
        encoded_data: Vec<u8>,
        sample_count: u32,
        duration: Duration,
    ) -> Result<()> {
        self.enqueue_packet(encoded_data.into(), sample_count, duration)
            .await
    }

    /// Retain a runtime buffer owner and its receipts until transmission, flush,
    /// cancellation or shutdown releases the final view. No payload copy.
    pub async fn enqueue_owned_frame(
        &self,
        encoded_data: remotemedia_types::SharedBytes,
        sample_count: u32,
        duration: Duration,
    ) -> Result<()> {
        self.enqueue_packet(
            bytes::Bytes::from_owner(encoded_data),
            sample_count,
            duration,
        )
        .await
    }

    async fn enqueue_packet(
        &self,
        data: bytes::Bytes,
        sample_count: u32,
        duration: Duration,
    ) -> Result<()> {
        let frame = AudioFrame {
            data,
            sample_count,
            duration,
        };
        self.buffer.push(frame).await
    }

    /// Get the number of frames currently buffered
    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    /// Drain the ring buffer without stopping the sender thread.
    ///
    /// Used on barge-in: we want the assistant to stop speaking
    /// immediately, not after the already-queued ~10 s of TTS audio
    /// finishes playing. The sender thread keeps running, it just
    /// finds the buffer empty and waits for the next push.
    pub fn flush_buffer(&self) -> usize {
        self.buffer.clear()
    }

    /// Get the current RTP timestamp
    pub fn timestamp(&self) -> u32 {
        self.timestamp.load(Ordering::Acquire)
    }

    /// Attach a [`MediaClockPublisher`]. After this call, every
    /// successful Opus frame send fires one [`MediaClock`] event on
    /// the publisher's address. Idempotent — replaces any previously
    /// attached publisher (the prior publisher's `stop` is **not**
    /// called automatically; callers that want a clean handoff should
    /// call `stop` on the prior publisher first).
    ///
    /// Phase 5.1 of the pacing-domains spec.
    pub fn set_media_clock(&self, publisher: MediaClockPublisher) {
        *self.media_clock.write() = Some(publisher);
    }

    /// Detach the media-clock publisher (if any) and call `stop` on it
    /// so subscribed wire pacers exit cleanly. Safe to call when no
    /// publisher is attached.
    ///
    /// Phase 5.9 of the pacing-domains spec.
    pub fn detach_media_clock(&self) {
        if let Some(publisher) = self.media_clock.write().take() {
            publisher.stop();
        }
    }

    /// Shutdown the sender and wait for thread to complete
    pub async fn shutdown(&self) -> Result<()> {
        debug!("Shutting down AudioSender");
        self.request_shutdown();

        // Wait for thread to finish
        if let Some(handle) = self.thread_handle.lock().await.take() {
            handle.join().map_err(|_| {
                Error::MediaTrackError("Failed to join audio sender thread".to_string())
            })?;
        }

        Ok(())
    }

    /// Close before acquiring producer locks: queued/blocked owners must release
    /// before a track can acquire its packetizer and join the sender.
    pub(crate) fn request_shutdown(&self) {
        self.lifetime.request_shutdown();
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
}

impl Clone for AudioSender {
    fn clone(&self) -> Self {
        Self {
            buffer: Arc::clone(&self.buffer),
            track: Arc::clone(&self.track),
            shutdown: Arc::clone(&self.shutdown),
            timestamp: Arc::clone(&self.timestamp),
            thread_handle: Arc::clone(&self.thread_handle),
            media_clock: Arc::clone(&self.media_clock),
            lifetime: Arc::clone(&self.lifetime),
        }
    }
}

#[cfg(test)]
#[path = "audio_sender_owner_tests.rs"]
mod owner_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer_basic() {
        let buffer = AudioRingBuffer::new(4);

        // Push some frames
        let frame = AudioFrame {
            data: vec![1, 2, 3].into(),
            sample_count: 480,
            duration: Duration::from_millis(20),
        };

        assert!(buffer.try_push(frame.clone()).is_ok());
        assert_eq!(buffer.len(), 1);

        // Pop frame
        let popped = buffer.try_pop();
        assert!(popped.is_some());
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_ring_buffer_full() {
        let buffer = AudioRingBuffer::new(3);

        let frame = AudioFrame {
            data: vec![1, 2, 3].into(),
            sample_count: 480,
            duration: Duration::from_millis(20),
        };

        // Fill buffer (capacity - 1 due to ring buffer semantics)
        assert!(buffer.try_push(frame.clone()).is_ok());
        assert!(buffer.try_push(frame.clone()).is_ok());

        // Should be full now
        assert!(buffer.try_push(frame.clone()).is_err());
    }

    #[test]
    fn test_ring_buffer_wraparound() {
        let buffer = AudioRingBuffer::new(4);

        let frame = AudioFrame {
            data: vec![1, 2, 3].into(),
            sample_count: 480,
            duration: Duration::from_millis(20),
        };

        // Push and pop multiple times to test wraparound
        for _ in 0..10 {
            assert!(buffer.try_push(frame.clone()).is_ok());
            assert!(buffer.try_pop().is_some());
        }
    }
}
