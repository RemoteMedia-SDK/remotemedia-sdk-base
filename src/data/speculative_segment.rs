//! Speculative audio segment data structure
//!
//! Represents an audio segment forwarded speculatively before final VAD decision.
//! Used by SpeculativeVADGate to track segments that may need retroactive cancellation.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Represents an audio segment forwarded before final VAD decision
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeculativeSegment {
    /// Unique identifier for this segment
    pub segment_id: Uuid,

    /// Start timestamp (microseconds since session start)
    pub start_timestamp: u64,

    /// End timestamp (microseconds since session start)
    pub end_timestamp: u64,

    /// Current status
    pub status: SegmentStatus,

    /// Reference to audio data in ring buffer (index range)
    pub buffer_range: (usize, usize),

    /// Session ID this segment belongs to
    pub session_id: String,
}

/// Status of a speculative segment
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SegmentStatus {
    /// Speculatively forwarded, awaiting VAD confirmation
    Speculative,

    /// VAD confirmed as speech, safe to process
    Confirmed,

    /// VAD retroactively cancelled (was noise/silence)
    Cancelled { reason: String },
}

impl SpeculativeSegment {
    /// Create a new speculative segment
    pub fn new(
        session_id: String,
        start_timestamp: u64,
        end_timestamp: u64,
        buffer_range: (usize, usize),
    ) -> Self {
        Self {
            segment_id: Uuid::new_v4(),
            start_timestamp,
            end_timestamp,
            status: SegmentStatus::Speculative,
            buffer_range,
            session_id,
        }
    }

    /// Validate segment integrity
    ///
    /// Returns Ok(()) if segment is valid, Err with reason if invalid
    pub fn validate(&self) -> Result<(), String> {
        // Check timestamp ordering
        if self.start_timestamp >= self.end_timestamp {
            return Err(format!(
                "Invalid timestamps: start ({}) >= end ({})",
                self.start_timestamp, self.end_timestamp
            ));
        }

        // Check VAD window duration (10-50ms typical)
        let duration_ms = (self.end_timestamp - self.start_timestamp) / 1000;
        if duration_ms < 5 || duration_ms > 100 {
            return Err(format!(
                "Unusual segment duration: {}ms (expected 10-50ms)",
                duration_ms
            ));
        }

        // Check buffer range
        if self.buffer_range.1 <= self.buffer_range.0 {
            return Err(format!(
                "Invalid buffer_range: ({}, {})",
                self.buffer_range.0, self.buffer_range.1
            ));
        }

        Ok(())
    }

    /// Confirm this segment as speech
    pub fn confirm(&mut self) {
        self.status = SegmentStatus::Confirmed;
    }

    /// Cancel this segment with a reason
    pub fn cancel(&mut self, reason: String) {
        self.status = SegmentStatus::Cancelled { reason };
    }

    /// Check if segment is in Speculative status
    pub fn is_speculative(&self) -> bool {
        matches!(self.status, SegmentStatus::Speculative)
    }

    /// Check if segment is confirmed
    pub fn is_confirmed(&self) -> bool {
        matches!(self.status, SegmentStatus::Confirmed)
    }

    /// Check if segment is cancelled
    pub fn is_cancelled(&self) -> bool {
        matches!(self.status, SegmentStatus::Cancelled { .. })
    }

    /// Get duration in microseconds
    pub fn duration_us(&self) -> u64 {
        self.end_timestamp - self.start_timestamp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_segment_creates_with_speculative_status() {
        let segment = SpeculativeSegment::new(
            "session_123".to_string(),
            1000000,  // 1 second
            1020000,  // 20ms later
            (0, 320), // ~20ms @ 16kHz
        );

        assert!(segment.is_speculative());
        assert!(!segment.is_confirmed());
        assert!(!segment.is_cancelled());
        assert_eq!(segment.session_id, "session_123");
    }

    #[test]
    fn test_segment_validation_success() {
        let segment = SpeculativeSegment::new(
            "session_123".to_string(),
            1000000,
            1020000, // 20ms duration - valid
            (0, 320),
        );

        assert!(segment.validate().is_ok());
    }

    #[test]
    fn test_segment_validation_fails_for_invalid_timestamps() {
        let mut segment =
            SpeculativeSegment::new("session_123".to_string(), 1000000, 1020000, (0, 320));

        // Swap timestamps to make invalid
        segment.start_timestamp = 2000000;
        segment.end_timestamp = 1000000;

        assert!(segment.validate().is_err());
    }

    #[test]
    fn test_segment_validation_warns_for_unusual_duration() {
        let segment = SpeculativeSegment::new(
            "session_123".to_string(),
            1000000,
            1200000, // 200ms - unusual
            (0, 3200),
        );

        let result = segment.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unusual segment duration"));
    }

    #[test]
    fn test_segment_state_transitions() {
        let mut segment =
            SpeculativeSegment::new("session_123".to_string(), 1000000, 1020000, (0, 320));

        // Initial state: Speculative
        assert!(segment.is_speculative());

        // Transition to Confirmed
        segment.confirm();
        assert!(segment.is_confirmed());
        assert!(!segment.is_speculative());

        // Create another segment and cancel it
        let mut segment2 =
            SpeculativeSegment::new("session_123".to_string(), 2000000, 2020000, (320, 640));

        segment2.cancel("VAD determined it was noise".to_string());
        assert!(segment2.is_cancelled());
        assert!(!segment2.is_speculative());
    }

    #[test]
    fn test_segment_duration_us() {
        let segment =
            SpeculativeSegment::new("session_123".to_string(), 1000000, 1020000, (0, 320));

        assert_eq!(segment.duration_us(), 20000); // 20ms = 20000 microseconds
    }

    #[test]
    fn test_buffer_range_validation() {
        let mut segment =
            SpeculativeSegment::new("session_123".to_string(), 1000000, 1020000, (0, 320));

        assert!(segment.validate().is_ok());

        // Invalid buffer range
        segment.buffer_range = (320, 320);
        assert!(segment.validate().is_err());

        segment.buffer_range = (640, 320);
        assert!(segment.validate().is_err());
    }
}
