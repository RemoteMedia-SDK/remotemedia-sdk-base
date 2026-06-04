//! Jitter buffer for audio/video synchronization
//!
//! Generic jitter buffer that reorders packets by RTP sequence number
//! and provides configurable buffering delay for smooth playback.

// Public API types - fields and methods used by library consumers, not internally
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// Statistics about jitter buffer performance
#[derive(Debug, Clone, Copy, Default)]
pub struct BufferStats {
    /// Current number of frames in buffer
    pub current_frames: usize,
    /// Peak number of frames ever held
    pub peak_frames: usize,
    /// Total frames dropped due to lateness
    pub dropped_frames: u64,
    /// Number of late packets received
    pub late_packet_count: u64,
    /// Number of buffer overrun events
    pub buffer_overrun_count: u64,
    /// Current buffer delay in milliseconds
    pub current_delay_ms: u32,
    /// Average delay over recent history
    pub average_delay_ms: f32,
    /// Estimated packet loss rate (0.0 to 1.0)
    pub estimated_loss_rate: f32,
}

/// Trait for frames that can be stored in the jitter buffer
pub trait JitterBufferFrame: Clone + Send {
    /// Get RTP sequence number
    fn sequence_number(&self) -> u16;
    /// Get RTP timestamp
    fn rtp_timestamp(&self) -> u32;
    /// Get time when frame was received
    fn received_at(&self) -> Instant;
}

/// Generic jitter buffer for media frames
///
/// Maintains frames in sequence order and releases them after a configurable
/// delay to smooth out network jitter.
pub struct JitterBuffer<T: JitterBufferFrame> {
    /// Frames stored by sequence number (using u32 for wraparound handling)
    frames: BTreeMap<u32, T>,
    /// Target buffer delay in milliseconds
    buffer_size_ms: u32,
    /// Maximum buffer delay before discarding
    max_buffer_ms: u32,
    /// Extended sequence of last popped frame (for wraparound tracking)
    last_popped_extended: Option<u32>,
    /// Base sequence number (first frame received) for extended sequence calculation
    base_seq: Option<u16>,
    /// Time when buffering started
    buffer_start_time: Option<Instant>,
    /// Statistics
    stats: BufferStats,
    /// Maximum frames to hold
    max_frames: usize,
    /// Expected sequence numbers for loss estimation
    expected_seq: Option<u16>,
    /// Total expected packets
    total_expected: u64,
    /// Total received packets
    total_received: u64,
}

impl<T: JitterBufferFrame> JitterBuffer<T> {
    /// Create a new jitter buffer
    ///
    /// # Arguments
    /// * `buffer_size_ms` - Target buffering delay (typically 50-200ms)
    /// * `max_buffer_ms` - Maximum delay before frames are discarded
    pub fn new(buffer_size_ms: u32, max_buffer_ms: u32) -> Self {
        Self {
            frames: BTreeMap::new(),
            buffer_size_ms,
            max_buffer_ms,
            last_popped_extended: None,
            base_seq: None,
            buffer_start_time: None,
            stats: BufferStats::default(),
            max_frames: 1000, // Reasonable default
            expected_seq: None,
            total_expected: 0,
            total_received: 0,
        }
    }

    /// Insert a frame into the buffer
    ///
    /// Frames are stored in sequence order using binary search for O(log n)
    /// insertion.
    ///
    /// # Arguments
    /// * `frame` - Frame to insert
    ///
    /// # Returns
    /// Ok(()) on success, Err if frame is too late or buffer is full
    pub fn insert(&mut self, frame: T) -> Result<(), &'static str> {
        let seq = frame.sequence_number();
        let received_at = frame.received_at();

        // Initialize buffer start time
        if self.buffer_start_time.is_none() {
            self.buffer_start_time = Some(received_at);
        }

        // Track packet loss
        self.track_sequence(seq);

        // Check if frame is too late
        if let Some(last_ext) = self.last_popped_extended {
            let extended_seq = self.extend_sequence(seq);
            if extended_seq <= last_ext {
                self.stats.late_packet_count += 1;
                self.stats.dropped_frames += 1;
                return Err("Frame arrived too late");
            }
        }

        // Check buffer capacity
        if self.frames.len() >= self.max_frames {
            self.stats.buffer_overrun_count += 1;
            // Remove oldest frame
            if let Some(&oldest_key) = self.frames.keys().next() {
                self.frames.remove(&oldest_key);
                self.stats.dropped_frames += 1;
            }
        }

        // Insert using extended sequence number for proper ordering
        let extended_seq = self.extend_sequence(seq);
        self.frames.insert(extended_seq, frame);

        // Update stats
        self.stats.current_frames = self.frames.len();
        if self.stats.current_frames > self.stats.peak_frames {
            self.stats.peak_frames = self.stats.current_frames;
        }

        Ok(())
    }

    /// Pop the next frame if buffer delay has elapsed
    ///
    /// Returns the frame with the lowest sequence number if it has been
    /// in the buffer for at least `buffer_size_ms`.
    ///
    /// # Returns
    /// Next frame ready for playback, or None if not ready
    pub fn pop_next(&mut self) -> Option<T> {
        let (&extended_seq, frame) = self.frames.iter().next()?;
        let now = Instant::now();

        // Check if buffer delay has elapsed
        let elapsed = now.duration_since(frame.received_at());
        if elapsed < Duration::from_millis(self.buffer_size_ms as u64) {
            return None;
        }

        // Update delay stats
        self.stats.current_delay_ms = elapsed.as_millis() as u32;
        self.update_average_delay(elapsed.as_millis() as f32);

        // Remove and return the frame
        let frame = self.frames.remove(&extended_seq)?;
        self.last_popped_extended = Some(extended_seq);
        self.stats.current_frames = self.frames.len();

        Some(frame)
    }

    /// Force pop the next frame regardless of delay
    ///
    /// Useful when draining the buffer or in low-latency mode.
    pub fn pop_next_immediate(&mut self) -> Option<T> {
        let (&extended_seq, _) = self.frames.iter().next()?;
        let frame = self.frames.remove(&extended_seq)?;

        // Update delay stats
        let elapsed = Instant::now().duration_since(frame.received_at());
        self.stats.current_delay_ms = elapsed.as_millis() as u32;
        self.update_average_delay(elapsed.as_millis() as f32);

        self.last_popped_extended = Some(extended_seq);
        self.stats.current_frames = self.frames.len();

        Some(frame)
    }

    /// Discard frames older than the cutoff time
    ///
    /// # Arguments
    /// * `cutoff_ms` - Maximum age in milliseconds
    pub fn discard_late_frames(&mut self, cutoff_ms: u32) {
        let now = Instant::now();
        let cutoff = Duration::from_millis(cutoff_ms as u64);

        let mut to_remove = Vec::new();
        for (&extended_seq, frame) in &self.frames {
            if now.duration_since(frame.received_at()) > cutoff {
                to_remove.push(extended_seq);
            }
        }

        for seq in to_remove {
            self.frames.remove(&seq);
            self.stats.dropped_frames += 1;
        }

        self.stats.current_frames = self.frames.len();
    }

    /// Get buffer statistics
    pub fn get_statistics(&self) -> BufferStats {
        let mut stats = self.stats;
        stats.estimated_loss_rate = if self.total_expected > 0 {
            1.0 - (self.total_received as f32 / self.total_expected as f32)
        } else {
            0.0
        };
        stats.estimated_loss_rate = stats.estimated_loss_rate.clamp(0.0, 1.0);
        stats
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    /// Get number of frames in buffer
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    /// Clear all frames from buffer
    pub fn clear(&mut self) {
        self.frames.clear();
        self.last_popped_extended = None;
        self.base_seq = None;
        self.buffer_start_time = None;
        self.stats.current_frames = 0;
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = BufferStats::default();
        self.expected_seq = None;
        self.total_expected = 0;
        self.total_received = 0;
    }

    /// Set maximum frames to hold
    pub fn set_max_frames(&mut self, max: usize) {
        self.max_frames = max;
    }

    /// Set buffer delay
    pub fn set_buffer_size_ms(&mut self, ms: u32) {
        self.buffer_size_ms = ms;
    }

    // Private helper methods

    /// Extend 16-bit sequence to 32-bit for proper ordering
    ///
    /// Uses signed difference arithmetic to correctly handle wraparound.
    /// When we have a reference (last popped or base), we compute the
    /// signed distance to the new sequence and add it to the reference's
    /// extended sequence.
    fn extend_sequence(&mut self, seq: u16) -> u32 {
        // If we have a last popped frame, use its extended sequence as reference
        if let Some(last_ext) = self.last_popped_extended {
            let last_seq = (last_ext & 0xFFFF) as u16;
            // Signed difference handles wraparound correctly
            let diff = seq.wrapping_sub(last_seq) as i16 as i32;
            return (last_ext as i32 + diff) as u32;
        }

        // Otherwise use base_seq if available
        if let Some(base) = self.base_seq {
            let diff = seq.wrapping_sub(base) as i16 as i32;
            return (base as i32 + diff) as u32;
        }

        // First frame ever - establish base
        self.base_seq = Some(seq);
        seq as u32
    }

    /// Track sequence numbers for loss estimation
    fn track_sequence(&mut self, seq: u16) {
        self.total_received += 1;

        if let Some(expected) = self.expected_seq {
            // Calculate expected packets since last
            let diff = seq.wrapping_sub(expected);
            if diff < 0x8000 {
                // Forward sequence
                self.total_expected += diff as u64 + 1;
            }
        } else {
            self.total_expected += 1;
        }

        self.expected_seq = Some(seq.wrapping_add(1));
    }

    /// Update running average delay
    fn update_average_delay(&mut self, delay_ms: f32) {
        const ALPHA: f32 = 0.1; // Smoothing factor
        if self.stats.average_delay_ms == 0.0 {
            self.stats.average_delay_ms = delay_ms;
        } else {
            self.stats.average_delay_ms =
                ALPHA * delay_ms + (1.0 - ALPHA) * self.stats.average_delay_ms;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct TestFrame {
        seq: u16,
        rtp_ts: u32,
        received: Instant,
    }

    impl JitterBufferFrame for TestFrame {
        fn sequence_number(&self) -> u16 {
            self.seq
        }
        fn rtp_timestamp(&self) -> u32 {
            self.rtp_ts
        }
        fn received_at(&self) -> Instant {
            self.received
        }
    }

    fn make_frame(seq: u16, ts: u32) -> TestFrame {
        TestFrame {
            seq,
            rtp_ts: ts,
            received: Instant::now(),
        }
    }

    fn make_frame_at(seq: u16, ts: u32, received: Instant) -> TestFrame {
        TestFrame {
            seq,
            rtp_ts: ts,
            received,
        }
    }

    #[test]
    fn test_new_buffer() {
        let buffer: JitterBuffer<TestFrame> = JitterBuffer::new(50, 200);
        assert!(buffer.is_empty());
        assert_eq!(buffer.len(), 0);
    }

    #[test]
    fn test_insert_and_pop() {
        let mut buffer: JitterBuffer<TestFrame> = JitterBuffer::new(0, 200);

        // Insert frame
        let frame = make_frame(1, 960);
        buffer.insert(frame.clone()).unwrap();

        assert_eq!(buffer.len(), 1);
        assert!(!buffer.is_empty());

        // Pop immediately (buffer_size_ms = 0)
        let popped = buffer.pop_next_immediate().unwrap();
        assert_eq!(popped.seq, 1);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_ordering() {
        let mut buffer: JitterBuffer<TestFrame> = JitterBuffer::new(0, 200);

        // Insert out of order
        buffer.insert(make_frame(3, 2880)).unwrap();
        buffer.insert(make_frame(1, 960)).unwrap();
        buffer.insert(make_frame(2, 1920)).unwrap();

        // Should pop in order
        assert_eq!(buffer.pop_next_immediate().unwrap().seq, 1);
        assert_eq!(buffer.pop_next_immediate().unwrap().seq, 2);
        assert_eq!(buffer.pop_next_immediate().unwrap().seq, 3);
    }

    #[test]
    fn test_buffer_delay() {
        let mut buffer: JitterBuffer<TestFrame> = JitterBuffer::new(50, 200);
        let now = Instant::now();

        // Insert frame received "now"
        buffer.insert(make_frame_at(1, 960, now)).unwrap();

        // Should not be ready yet (< 50ms)
        assert!(buffer.pop_next().is_none());
    }

    #[test]
    fn test_late_packet_rejection() {
        let mut buffer: JitterBuffer<TestFrame> = JitterBuffer::new(0, 200);

        // Pop frame 5
        buffer.insert(make_frame(5, 4800)).unwrap();
        buffer.pop_next_immediate();

        // Try to insert frame 3 (too late)
        let result = buffer.insert(make_frame(3, 2880));
        assert!(result.is_err());

        let stats = buffer.get_statistics();
        assert_eq!(stats.late_packet_count, 1);
    }

    #[test]
    fn test_sequence_wraparound() {
        let mut buffer: JitterBuffer<TestFrame> = JitterBuffer::new(0, 200);

        // Insert near wraparound point
        buffer.insert(make_frame(65534, 1000)).unwrap();
        buffer.insert(make_frame(65535, 2000)).unwrap();
        buffer.insert(make_frame(0, 3000)).unwrap();
        buffer.insert(make_frame(1, 4000)).unwrap();

        // Should pop in correct order despite wraparound
        assert_eq!(buffer.pop_next_immediate().unwrap().seq, 65534);
        assert_eq!(buffer.pop_next_immediate().unwrap().seq, 65535);
        assert_eq!(buffer.pop_next_immediate().unwrap().seq, 0);
        assert_eq!(buffer.pop_next_immediate().unwrap().seq, 1);
    }

    #[test]
    fn test_discard_late_frames() {
        let mut buffer: JitterBuffer<TestFrame> = JitterBuffer::new(0, 200);
        let old_time = Instant::now() - Duration::from_millis(300);

        // Insert old frame
        buffer.insert(make_frame_at(1, 960, old_time)).unwrap();
        // Insert fresh frame
        buffer.insert(make_frame(2, 1920)).unwrap();

        // Discard frames older than 200ms
        buffer.discard_late_frames(200);

        // Only frame 2 should remain
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.pop_next_immediate().unwrap().seq, 2);
    }

    #[test]
    fn test_statistics() {
        let mut buffer: JitterBuffer<TestFrame> = JitterBuffer::new(0, 200);

        for i in 0..10 {
            buffer.insert(make_frame(i, i as u32 * 960)).unwrap();
        }

        let stats = buffer.get_statistics();
        assert_eq!(stats.current_frames, 10);
        assert_eq!(stats.peak_frames, 10);
        assert_eq!(stats.dropped_frames, 0);
    }

    #[test]
    fn test_buffer_overrun() {
        let mut buffer: JitterBuffer<TestFrame> = JitterBuffer::new(0, 200);
        buffer.set_max_frames(5);

        // Insert more than max
        for i in 0..10 {
            buffer.insert(make_frame(i, i as u32 * 960)).unwrap();
        }

        // Should only have 5 frames (oldest dropped)
        assert_eq!(buffer.len(), 5);

        let stats = buffer.get_statistics();
        assert!(stats.buffer_overrun_count > 0);
    }

    #[test]
    fn test_clear_and_reset() {
        let mut buffer: JitterBuffer<TestFrame> = JitterBuffer::new(0, 200);

        for i in 0..5 {
            buffer.insert(make_frame(i, i as u32 * 960)).unwrap();
        }

        buffer.clear();
        assert!(buffer.is_empty());

        buffer.reset_stats();
        let stats = buffer.get_statistics();
        assert_eq!(stats.peak_frames, 0);
    }
}
