//! Lock-free ring buffer for speculative segments
//!
//! Thread-safe circular buffer using crossbeam::ArrayQueue for storing
//! speculative audio segments with automatic overwrite of oldest entries.

use crossbeam::queue::ArrayQueue;
use std::sync::Arc;

use super::speculative_segment::SpeculativeSegment;

/// Lock-free ring buffer for speculative segments
///
/// Uses crossbeam::ArrayQueue for lock-free MPMC operations.
/// Automatically overwrites oldest segments when full.
pub struct RingBuffer {
    /// Underlying lock-free queue
    queue: Arc<ArrayQueue<SpeculativeSegment>>,

    /// Capacity (fixed at creation)
    capacity: usize,

    /// Count of overwrites (for debugging/metrics)
    overwrites: Arc<std::sync::atomic::AtomicU64>,
}

impl RingBuffer {
    /// Create a new ring buffer with fixed capacity
    ///
    /// Capacity calculation example:
    /// - Lookback: 200ms, Lookahead: 50ms → Total: 250ms
    /// - Segment size: 20ms (typical VAD hop)
    /// - Capacity: 250ms / 20ms = 12.5 → round to 16 (power of 2)
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: Arc::new(ArrayQueue::new(capacity)),
            capacity,
            overwrites: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Push segment, overwrite oldest if full
    ///
    /// Returns the most recently overwritten segment if any pops occurred, None otherwise.
    pub fn push_overwrite(&self, segment: SpeculativeSegment) -> Option<SpeculativeSegment> {
        // Loop because under concurrent contention another thread may refill the slot
        // between our pop and retry push. ArrayQueue::push returns the value back on
        // failure, so we can retry without cloning.
        let mut pending = segment;
        let mut overwritten = None;
        loop {
            match self.queue.push(pending) {
                Ok(_) => return overwritten,
                Err(returned) => {
                    pending = returned;
                    if let Some(oldest) = self.queue.pop() {
                        self.overwrites
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        overwritten = Some(oldest);
                    }
                    // If pop returned None, another thread already drained a slot;
                    // the next push attempt should succeed.
                }
            }
        }
    }

    /// Get segments in timestamp range
    ///
    /// Returns all segments where segment overlaps [from_ts, to_ts]
    ///
    /// Note: This creates a snapshot by popping all segments and re-adding them.
    /// Not ideal for high-frequency calls, but necessary given ArrayQueue's limitations.
    pub fn get_range(&self, from_ts: u64, to_ts: u64) -> Vec<SpeculativeSegment> {
        let mut all_segments = Vec::new();
        let mut matching_segments = Vec::new();

        // Pop all segments to create snapshot
        while let Some(segment) = self.queue.pop() {
            all_segments.push(segment);
        }

        // Find matching segments
        for segment in &all_segments {
            if segment.start_timestamp < to_ts && segment.end_timestamp > from_ts {
                matching_segments.push(segment.clone());
            }
        }

        // Re-add all segments
        for segment in all_segments {
            if let Err(e) = self.queue.push(segment) {
                eprintln!("Failed to re-add segment during get_range: {:?}", e);
            }
        }

        matching_segments
    }

    /// Clear segments before timestamp
    ///
    /// Removes all segments where end_timestamp < threshold
    /// Note: This is not truly "clearing" from ArrayQueue (which doesn't support removal),
    /// but rather relies on natural consumption. For production, segments are popped
    /// and re-added if they should be kept.
    pub fn clear_before(&self, threshold: u64) -> usize {
        let mut removed_count = 0;
        let mut temp_segments = Vec::new();

        // Pop all segments
        while let Some(segment) = self.queue.pop() {
            if segment.end_timestamp >= threshold {
                // Keep this segment
                temp_segments.push(segment);
            } else {
                // Discard this segment
                removed_count += 1;
            }
        }

        // Re-add segments we want to keep
        for segment in temp_segments {
            if let Err(e) = self.queue.push(segment) {
                eprintln!("Failed to re-add segment during clear_before: {:?}", e);
            }
        }

        removed_count
    }

    /// Get current buffer size
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Get buffer capacity
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get overwrite count
    pub fn overwrite_count(&self) -> u64 {
        self.overwrites.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Check if buffer is full
    pub fn is_full(&self) -> bool {
        self.queue.len() >= self.capacity
    }
}

impl Clone for RingBuffer {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            capacity: self.capacity,
            overwrites: Arc::clone(&self.overwrites),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_segment(session_id: &str, start_us: u64, end_us: u64) -> SpeculativeSegment {
        SpeculativeSegment::new(
            session_id.to_string(),
            start_us,
            end_us,
            (0, 320), // Dummy buffer range
        )
    }

    #[test]
    fn test_create_ring_buffer() {
        let buffer = RingBuffer::new(16);

        assert_eq!(buffer.capacity(), 16);
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
        assert!(!buffer.is_full());
    }

    #[test]
    fn test_push_overwrite_within_capacity() {
        let buffer = RingBuffer::new(4);

        let seg1 = create_test_segment("sess1", 1000, 2000);
        let seg2 = create_test_segment("sess1", 2000, 3000);

        let overwritten1 = buffer.push_overwrite(seg1);
        assert!(overwritten1.is_none()); // No overwrite

        let overwritten2 = buffer.push_overwrite(seg2);
        assert!(overwritten2.is_none()); // No overwrite

        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer.overwrite_count(), 0);
    }

    #[test]
    fn test_push_overwrite_when_full() {
        let buffer = RingBuffer::new(3);

        // Fill buffer
        let seg1 = create_test_segment("sess1", 1000, 2000);
        let seg2 = create_test_segment("sess1", 2000, 3000);
        let seg3 = create_test_segment("sess1", 3000, 4000);

        buffer.push_overwrite(seg1.clone());
        buffer.push_overwrite(seg2);
        buffer.push_overwrite(seg3);

        assert_eq!(buffer.len(), 3);
        assert!(buffer.is_full());

        // Push fourth segment - should overwrite oldest (seg1)
        let seg4 = create_test_segment("sess1", 4000, 5000);
        let overwritten = buffer.push_overwrite(seg4);

        assert!(overwritten.is_some());
        let overwritten_seg = overwritten.unwrap();
        assert_eq!(overwritten_seg.start_timestamp, seg1.start_timestamp);
        assert_eq!(buffer.len(), 3); // Still at capacity
        assert_eq!(buffer.overwrite_count(), 1);
    }

    #[test]
    fn test_get_range_exact_match() {
        let buffer = RingBuffer::new(10);

        let seg1 = create_test_segment("sess1", 1000, 2000);
        let seg2 = create_test_segment("sess1", 2000, 3000);
        let seg3 = create_test_segment("sess1", 3000, 4000);

        buffer.push_overwrite(seg1);
        buffer.push_overwrite(seg2);
        buffer.push_overwrite(seg3);

        // Get range that matches seg2 exactly
        let range = buffer.get_range(2000, 3000);

        assert_eq!(range.len(), 1);
        assert_eq!(range[0].start_timestamp, 2000);
        assert_eq!(range[0].end_timestamp, 3000);
    }

    #[test]
    fn test_get_range_overlapping() {
        let buffer = RingBuffer::new(10);

        let seg1 = create_test_segment("sess1", 1000, 2000);
        let seg2 = create_test_segment("sess1", 2000, 3000);
        let seg3 = create_test_segment("sess1", 3000, 4000);

        buffer.push_overwrite(seg1);
        buffer.push_overwrite(seg2);
        buffer.push_overwrite(seg3);

        // Get range that overlaps seg1 and seg2
        let range = buffer.get_range(1500, 2500);

        assert_eq!(range.len(), 2);
        assert!(range.iter().any(|s| s.start_timestamp == 1000));
        assert!(range.iter().any(|s| s.start_timestamp == 2000));
    }

    #[test]
    fn test_get_range_no_match() {
        let buffer = RingBuffer::new(10);

        let seg1 = create_test_segment("sess1", 1000, 2000);
        buffer.push_overwrite(seg1);

        // Get range that doesn't overlap
        let range = buffer.get_range(5000, 6000);

        assert_eq!(range.len(), 0);
    }

    #[test]
    fn test_clear_before_removes_old_segments() {
        let buffer = RingBuffer::new(10);

        let seg1 = create_test_segment("sess1", 1000, 2000);
        let seg2 = create_test_segment("sess1", 2000, 3000);
        let seg3 = create_test_segment("sess1", 3000, 4000);
        let seg4 = create_test_segment("sess1", 4000, 5000);

        buffer.push_overwrite(seg1);
        buffer.push_overwrite(seg2);
        buffer.push_overwrite(seg3);
        buffer.push_overwrite(seg4);

        assert_eq!(buffer.len(), 4);

        // Clear segments before 3500μs (should remove seg1 and seg2)
        let removed = buffer.clear_before(3500);

        assert_eq!(removed, 2);
        assert_eq!(buffer.len(), 2);

        // Verify remaining segments
        let remaining = buffer.get_range(0, u64::MAX);
        assert_eq!(remaining.len(), 2);
        assert!(remaining.iter().all(|s| s.end_timestamp >= 3500));
    }

    #[test]
    fn test_clear_before_removes_all() {
        let buffer = RingBuffer::new(10);

        buffer.push_overwrite(create_test_segment("sess1", 1000, 2000));
        buffer.push_overwrite(create_test_segment("sess1", 2000, 3000));

        let removed = buffer.clear_before(10000); // Clear everything

        assert_eq!(removed, 2);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_clear_before_removes_none() {
        let buffer = RingBuffer::new(10);

        buffer.push_overwrite(create_test_segment("sess1", 5000, 6000));
        buffer.push_overwrite(create_test_segment("sess1", 6000, 7000));

        let removed = buffer.clear_before(1000); // Before all segments

        assert_eq!(removed, 0);
        assert_eq!(buffer.len(), 2);
    }

    #[test]
    fn test_buffer_clone_shares_underlying_queue() {
        let buffer1 = RingBuffer::new(10);

        let seg = create_test_segment("sess1", 1000, 2000);
        buffer1.push_overwrite(seg);

        // Clone the buffer
        let buffer2 = buffer1.clone();

        // Both should see the same data (shared Arc)
        assert_eq!(buffer1.len(), 1);
        assert_eq!(buffer2.len(), 1);

        // Push to buffer2
        buffer2.push_overwrite(create_test_segment("sess1", 2000, 3000));

        // Both should see 2 segments
        assert_eq!(buffer1.len(), 2);
        assert_eq!(buffer2.len(), 2);
    }

    #[test]
    fn test_concurrent_push_overwrite() {
        use std::sync::Arc;
        use std::thread;

        let buffer = Arc::new(RingBuffer::new(100));

        // Spawn multiple threads pushing concurrently
        let mut handles = vec![];
        for i in 0..10 {
            let buf = buffer.clone();
            handles.push(thread::spawn(move || {
                for j in 0..50 {
                    let start_us = ((i * 50 + j) * 10000) as u64;
                    let end_us = start_us + 20000;
                    let seg = create_test_segment("concurrent_sess", start_us, end_us);
                    buf.push_overwrite(seg);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should have 100 segments (capacity limit)
        assert_eq!(buffer.len(), 100);

        // Should have approximately 400 overwrites (500 pushes - 100 capacity)
        // Due to concurrent timing and lock contention, there may be variance
        let overwrites = buffer.overwrite_count();
        assert!(
            overwrites >= 380 && overwrites <= 420,
            "Expected ~400 overwrites, got {}",
            overwrites
        );
    }
}
