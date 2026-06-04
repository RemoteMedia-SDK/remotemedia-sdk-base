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
    data: Vec<u8>,
    /// Number of samples this frame represents (for timestamp calculation)
    sample_count: u32,
    /// Frame duration
    duration: Duration,
}

/// Ring buffer for audio frames
struct AudioRingBuffer {
    /// Circular buffer of frames
    buffer: Vec<Option<AudioFrame>>,
    /// Write position
    write_pos: AtomicU32,
    /// Read position
    read_pos: AtomicU32,
    /// Buffer capacity
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
}

impl AudioRingBuffer {
    /// Create a new ring buffer with given capacity
    fn new(capacity: usize) -> Self {
        let mut buffer = Vec::with_capacity(capacity);
        buffer.resize_with(capacity, || None);

        Self {
            buffer,
            write_pos: AtomicU32::new(0),
            read_pos: AtomicU32::new(0),
            capacity,
            space_available: Notify::new(),
            shutdown: AtomicBool::new(false),
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
            if self.shutdown.load(Ordering::Acquire) {
                return Err(Error::MediaTrackError(
                    "audio sender shutdown — refusing to enqueue frame".to_string(),
                ));
            }
            // Register interest BEFORE the try_push attempt. If the
            // consumer drains a slot between try_push() and notified()
            // the permit is held by the Notify and the next
            // notified().await returns immediately — no missed wakeups.
            let waiter = self.space_available.notified();
            tokio::pin!(waiter);
            // Enable the waiter so it captures any notifications that
            // arrive between this point and the next .await.
            waiter.as_mut().enable();

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
        let write_pos = self.write_pos.load(Ordering::Acquire);
        let read_pos = self.read_pos.load(Ordering::Acquire);

        let next_write = (write_pos + 1) % self.capacity as u32;
        if next_write == read_pos {
            return Err(frame);
        }

        // SAFETY: position is in bounds and won't overlap the reader.
        unsafe {
            let slot = self.buffer.as_ptr().add(write_pos as usize) as *mut Option<AudioFrame>;
            *slot = Some(frame);
        }
        self.write_pos.store(next_write, Ordering::Release);
        Ok(())
    }

    /// Try to pop a frame from the buffer
    /// Returns None if buffer is empty
    fn try_pop(&self) -> Option<AudioFrame> {
        let write_pos = self.write_pos.load(Ordering::Acquire);
        let read_pos = self.read_pos.load(Ordering::Acquire);

        // Check if buffer is empty
        if read_pos == write_pos {
            return None;
        }

        // SAFETY: We've verified there's data to read
        let frame = unsafe {
            let slot = self.buffer.as_ptr().add(read_pos as usize) as *mut Option<AudioFrame>;
            (*slot).take()
        };

        // Update read position
        let next_read = (read_pos + 1) % self.capacity as u32;
        self.read_pos.store(next_read, Ordering::Release);

        // Wake any producer parked on `push().await` waiting for
        // space. Notify is `Send + Sync` and `notify_one` is callable
        // from a non-tokio thread, so this is safe from the std::thread
        // transmission loop. If no one is waiting, the permit is
        // queued and the next `push()` consumes it without waiting.
        self.space_available.notify_one();

        frame
    }

    /// Get the number of frames currently in the buffer
    fn len(&self) -> usize {
        let write_pos = self.write_pos.load(Ordering::Acquire) as usize;
        let read_pos = self.read_pos.load(Ordering::Acquire) as usize;

        if write_pos >= read_pos {
            write_pos - read_pos
        } else {
            self.capacity - read_pos + write_pos
        }
    }

    /// Drain the ring buffer without stopping the sender thread.
    ///
    /// Called on barge-in: the user started speaking while the
    /// assistant was still playing queued TTS audio. Advancing
    /// `read_pos` to `write_pos` discards all queued frames so the
    /// next frames the thread pops are the fresh ones. The `Option`
    /// slots are left as-is — the thread's `try_pop` tolerates both
    /// None and stale Some readings (it checks read_pos==write_pos
    /// first, and any stale Some is overwritten on the next push).
    fn clear(&self) -> usize {
        let write_pos = self.write_pos.load(Ordering::Acquire);
        let old_read_pos = self.read_pos.swap(write_pos, Ordering::AcqRel);
        let dropped = if write_pos >= old_read_pos {
            (write_pos - old_read_pos) as usize
        } else {
            self.capacity - old_read_pos as usize + write_pos as usize
        };
        // Proactively clear the Option slots we just skipped over so
        // we don't hold onto AudioFrame allocations until the next
        // wrap-around.
        let mut cursor = old_read_pos as usize;
        while cursor != write_pos as usize {
            // SAFETY: same reasoning as try_pop — positions are in
            // bounds and we just claimed this range by moving read_pos.
            unsafe {
                let slot = self.buffer.as_ptr().add(cursor) as *mut Option<AudioFrame>;
                *slot = None;
            }
            cursor = (cursor + 1) % self.capacity;
        }
        // Wake every parked producer — clearing freed every slot at
        // once. A single `notify_one` would only wake one of them and
        // leave the rest sleeping despite the buffer being empty.
        self.space_available.notify_waiters();
        dropped
    }

    /// Mark the ring buffer as shutting down and wake every parked
    /// producer so they observe the flag and return promptly.
    fn signal_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.space_available.notify_waiters();
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

        // Store the thread handle
        let sender_clone = sender.clone();
        tokio::spawn(async move {
            *sender_clone.thread_handle.lock().await = Some(handle);
        });

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
            if let Some(frame) = buffer.try_pop() {
                // Update RTP timestamp
                let old_ts = timestamp.fetch_add(frame.sample_count, Ordering::AcqRel);

                // Create WebRTC sample
                let sample = Sample {
                    data: frame.data.into(),
                    duration: frame.duration,
                    timestamp: std::time::SystemTime::now(),
                    ..Default::default()
                };

                // Send the sample using the dedicated runtime
                let track_clone = Arc::clone(&track);
                let send_result = rt.block_on(async { track_clone.write_sample(&sample).await });

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
                    std::thread::sleep(wait);
                }
                // If `wait` was None we're behind schedule — skip the
                // sleep and let the next iteration pop the next frame
                // immediately. The deadline stays consistent so we
                // self-correct on the very next on-time frame.
            } else {
                // Buffer is empty — drop the deadline so the next frame
                // re-anchors at its arrival time instead of trying to
                // "catch up" to a stale clock.
                next_deadline = None;
                std::thread::sleep(Duration::from_millis(5));
            }
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
        let frame = AudioFrame {
            data: encoded_data,
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
        self.shutdown.store(true, Ordering::Release);
        // Phase 5.9: stop the media_clock channel so subscribed wire
        // pacers exit their `next()` loop with `None`. Idempotent if no
        // publisher is attached.
        self.detach_media_clock();
        // Also flip the buffer's shutdown flag and wake every parked
        // producer; otherwise a producer awaiting space hangs forever
        // because the transmission thread has stopped popping.
        self.buffer.signal_shutdown();

        // Wait for thread to finish
        if let Some(handle) = self.thread_handle.lock().await.take() {
            handle.join().map_err(|_| {
                Error::MediaTrackError("Failed to join audio sender thread".to_string())
            })?;
        }

        Ok(())
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
        }
    }
}

// NOTE: We do NOT implement Drop to set shutdown=true because AudioSender
// is cloned internally (for storing thread handle), and dropping the clone
// would kill the thread prematurely. Instead, shutdown must be called explicitly
// or the AudioTrack that owns this sender will clean up when it's dropped.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer_basic() {
        let buffer = AudioRingBuffer::new(4);

        // Push some frames
        let frame = AudioFrame {
            data: vec![1, 2, 3],
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
            data: vec![1, 2, 3],
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
            data: vec![1, 2, 3],
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
