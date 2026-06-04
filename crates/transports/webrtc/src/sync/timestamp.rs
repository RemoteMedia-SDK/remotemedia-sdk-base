//! RTP Timestamp utilities for audio/video synchronization
//!
//! Provides utilities for extracting, converting, and handling RTP timestamps
//! with proper 32-bit wraparound handling.

// Public API types - fields and methods used by library consumers, not internally
#![allow(dead_code)]

use std::time::{Duration, SystemTime};

/// RTP header structure for timestamp extraction
#[derive(Debug, Clone, Copy)]
pub struct RtpHeader {
    /// RTP version (should be 2)
    pub version: u8,
    /// Padding flag
    pub padding: bool,
    /// Extension flag
    pub extension: bool,
    /// CSRC count
    pub csrc_count: u8,
    /// Marker bit
    pub marker: bool,
    /// Payload type
    pub payload_type: u8,
    /// Sequence number (16-bit)
    pub sequence_number: u16,
    /// Timestamp (32-bit)
    pub timestamp: u32,
    /// Synchronization source identifier
    pub ssrc: u32,
}

impl RtpHeader {
    /// Parse RTP header from bytes
    ///
    /// # Arguments
    /// * `data` - Raw RTP packet data (at least 12 bytes)
    ///
    /// # Returns
    /// Parsed RtpHeader or None if data is too short
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.len() < 12 {
            return None;
        }

        let first_byte = data[0];
        let second_byte = data[1];

        Some(Self {
            version: (first_byte >> 6) & 0x03,
            padding: (first_byte >> 5) & 0x01 == 1,
            extension: (first_byte >> 4) & 0x01 == 1,
            csrc_count: first_byte & 0x0F,
            marker: (second_byte >> 7) & 0x01 == 1,
            payload_type: second_byte & 0x7F,
            sequence_number: u16::from_be_bytes([data[2], data[3]]),
            timestamp: u32::from_be_bytes([data[4], data[5], data[6], data[7]]),
            ssrc: u32::from_be_bytes([data[8], data[9], data[10], data[11]]),
        })
    }
}

/// NTP to RTP timestamp mapping from RTCP Sender Reports
#[derive(Debug, Clone, Copy)]
pub struct NtpRtpMapping {
    /// NTP timestamp (64-bit: seconds since 1900 in upper 32, fraction in lower 32)
    pub ntp_timestamp: u64,
    /// Corresponding RTP timestamp
    pub rtp_timestamp: u32,
    /// Clock rate for this media type (e.g., 48000 for audio, 90000 for video)
    pub clock_rate: u32,
    /// When this mapping was received (local time)
    pub received_at: SystemTime,
}

/// RTP timestamp utilities
pub struct RtpTimestamp;

impl RtpTimestamp {
    /// Extract RTP timestamp from RTP header
    ///
    /// # Arguments
    /// * `header` - Parsed RTP header
    ///
    /// # Returns
    /// 32-bit RTP timestamp
    pub fn from_rtp_header(header: &RtpHeader) -> u32 {
        header.timestamp
    }

    /// Increment RTP timestamp with proper 32-bit wraparound handling
    ///
    /// # Arguments
    /// * `current` - Current RTP timestamp
    /// * `samples` - Number of samples to increment by
    /// * `clock_rate` - Clock rate (samples per second)
    ///
    /// # Returns
    /// New timestamp with wraparound at 0xFFFFFFFF
    pub fn increment(current: u32, samples: u32, _clock_rate: u32) -> u32 {
        // RTP timestamp increments by number of samples, not time
        // Wraparound is automatic with u32 overflow
        current.wrapping_add(samples)
    }

    /// Convert RTP timestamp to wall-clock time (microseconds since Unix epoch)
    ///
    /// Uses NTP/RTP mapping from RTCP Sender Reports to convert RTP timestamps
    /// to absolute wall-clock time.
    ///
    /// # Arguments
    /// * `rtp_ts` - RTP timestamp to convert
    /// * `mapping` - NTP/RTP mapping from most recent RTCP SR
    /// * `clock_rate` - Media clock rate (e.g., 48000 Hz for audio)
    ///
    /// # Returns
    /// Wall-clock time in microseconds since Unix epoch, or None if mapping invalid
    pub fn to_wall_clock(rtp_ts: u32, mapping: &NtpRtpMapping, clock_rate: u32) -> Option<u64> {
        // Calculate RTP timestamp difference with wraparound handling
        let rtp_diff = Self::signed_diff(rtp_ts, mapping.rtp_timestamp);

        // Convert RTP diff to microseconds
        let us_diff = (rtp_diff as i64 * 1_000_000) / clock_rate as i64;

        // Convert NTP timestamp to Unix microseconds
        // NTP epoch is 1900-01-01, Unix epoch is 1970-01-01
        // Difference is 2208988800 seconds (70 years)
        const NTP_UNIX_OFFSET_SECS: u64 = 2_208_988_800;

        let ntp_secs = mapping.ntp_timestamp >> 32;
        let ntp_frac = mapping.ntp_timestamp & 0xFFFFFFFF;

        // Check for valid NTP timestamp (must be after Unix epoch)
        if ntp_secs < NTP_UNIX_OFFSET_SECS {
            return None;
        }

        let unix_secs = ntp_secs - NTP_UNIX_OFFSET_SECS;
        let unix_frac_us = (ntp_frac * 1_000_000) >> 32;
        let mapping_us = unix_secs * 1_000_000 + unix_frac_us;

        // Apply the RTP difference
        Some((mapping_us as i64 + us_diff) as u64)
    }

    /// Calculate signed difference between two RTP timestamps
    ///
    /// Handles 32-bit wraparound correctly by interpreting the difference
    /// as a signed value in the range [-2^31, 2^31-1].
    ///
    /// # Arguments
    /// * `ts1` - First timestamp
    /// * `ts2` - Second timestamp (reference)
    ///
    /// # Returns
    /// Signed difference (ts1 - ts2)
    pub fn signed_diff(ts1: u32, ts2: u32) -> i32 {
        ts1.wrapping_sub(ts2) as i32
    }

    /// Check if timestamp ts1 is "after" ts2 considering wraparound
    ///
    /// # Arguments
    /// * `ts1` - First timestamp
    /// * `ts2` - Second timestamp
    ///
    /// # Returns
    /// true if ts1 is considered to be after ts2
    pub fn is_after(ts1: u32, ts2: u32) -> bool {
        Self::signed_diff(ts1, ts2) > 0
    }

    /// Convert duration to RTP timestamp units
    ///
    /// # Arguments
    /// * `duration` - Duration to convert
    /// * `clock_rate` - Clock rate (samples per second)
    ///
    /// # Returns
    /// Number of RTP timestamp units
    pub fn from_duration(duration: Duration, clock_rate: u32) -> u32 {
        let micros = duration.as_micros() as u64;
        ((micros * clock_rate as u64) / 1_000_000) as u32
    }

    /// Convert RTP timestamp units to duration
    ///
    /// # Arguments
    /// * `rtp_units` - RTP timestamp units
    /// * `clock_rate` - Clock rate (samples per second)
    ///
    /// # Returns
    /// Duration
    pub fn to_duration(rtp_units: u32, clock_rate: u32) -> Duration {
        let micros = (rtp_units as u64 * 1_000_000) / clock_rate as u64;
        Duration::from_micros(micros)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rtp_header_parse() {
        // Minimal valid RTP packet (12 bytes)
        let data = [
            0x80, // V=2, P=0, X=0, CC=0
            0x60, // M=0, PT=96
            0x00, 0x01, // Seq=1
            0x00, 0x00, 0x03, 0xE8, // TS=1000
            0x12, 0x34, 0x56, 0x78, // SSRC
        ];

        let header = RtpHeader::parse(&data).unwrap();
        assert_eq!(header.version, 2);
        assert!(!header.padding);
        assert!(!header.extension);
        assert_eq!(header.csrc_count, 0);
        assert!(!header.marker);
        assert_eq!(header.payload_type, 96);
        assert_eq!(header.sequence_number, 1);
        assert_eq!(header.timestamp, 1000);
        assert_eq!(header.ssrc, 0x12345678);
    }

    #[test]
    fn test_from_rtp_header() {
        let header = RtpHeader {
            version: 2,
            padding: false,
            extension: false,
            csrc_count: 0,
            marker: false,
            payload_type: 96,
            sequence_number: 100,
            timestamp: 48000,
            ssrc: 12345,
        };

        assert_eq!(RtpTimestamp::from_rtp_header(&header), 48000);
    }

    #[test]
    fn test_increment_normal() {
        // Normal increment
        assert_eq!(RtpTimestamp::increment(1000, 960, 48000), 1960);
    }

    #[test]
    fn test_increment_wraparound() {
        // Wraparound at 0xFFFFFFFF
        let near_max = 0xFFFFFF00u32;
        let result = RtpTimestamp::increment(near_max, 0x200, 48000);
        assert_eq!(result, 0x100); // Wrapped around
    }

    #[test]
    fn test_signed_diff() {
        // Normal case
        assert_eq!(RtpTimestamp::signed_diff(1000, 500), 500);
        assert_eq!(RtpTimestamp::signed_diff(500, 1000), -500);

        // Wraparound case
        let ts1 = 100u32;
        let ts2 = 0xFFFFFF00u32;
        // ts1 is "after" ts2 (wrapped around)
        assert!(RtpTimestamp::signed_diff(ts1, ts2) > 0);
    }

    #[test]
    fn test_is_after() {
        assert!(RtpTimestamp::is_after(1000, 500));
        assert!(!RtpTimestamp::is_after(500, 1000));

        // Wraparound
        assert!(RtpTimestamp::is_after(100, 0xFFFFFF00));
    }

    #[test]
    fn test_to_wall_clock() {
        // Create a mapping
        // NTP timestamp for 2024-01-01 00:00:00 UTC
        // Unix: 1704067200 seconds
        // NTP: 1704067200 + 2208988800 = 3913056000 seconds
        let ntp_secs: u64 = 3_913_056_000;
        let ntp_timestamp = ntp_secs << 32; // No fractional part

        let mapping = NtpRtpMapping {
            ntp_timestamp,
            rtp_timestamp: 0,
            clock_rate: 48000,
            received_at: SystemTime::now(),
        };

        // RTP timestamp 48000 = 1 second later
        let wall_clock = RtpTimestamp::to_wall_clock(48000, &mapping, 48000).unwrap();

        // Should be 1704067201 seconds in microseconds
        let expected_us = 1_704_067_201_000_000u64;
        assert_eq!(wall_clock, expected_us);
    }

    #[test]
    fn test_duration_conversion() {
        let duration = Duration::from_millis(20);
        let rtp_units = RtpTimestamp::from_duration(duration, 48000);
        assert_eq!(rtp_units, 960); // 20ms * 48000Hz = 960 samples

        let back = RtpTimestamp::to_duration(960, 48000);
        assert_eq!(back.as_millis(), 20);
    }
}
