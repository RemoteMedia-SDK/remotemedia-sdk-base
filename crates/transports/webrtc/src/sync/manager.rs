//! Synchronization manager for multi-peer audio/video
//!
//! Manages per-peer synchronization state including jitter buffers,
//! clock drift estimation, and audio/video lip-sync.

// Public API types - fields and methods used by library consumers, not internally
#![allow(dead_code)]

use super::clock_drift::{ClockDriftEstimate, ClockDriftEstimator, DriftAction};
use super::jitter_buffer::{BufferStats, JitterBuffer, JitterBufferFrame};
use super::timestamp::{NtpRtpMapping, RtpTimestamp};
use std::sync::Arc;
use std::time::{Instant, SystemTime};

/// Configuration for synchronization
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Audio clock rate (typically 48000 Hz)
    pub audio_clock_rate: u32,
    /// Video clock rate (typically 90000 Hz)
    pub video_clock_rate: u32,
    /// Jitter buffer size in milliseconds (50-200)
    pub jitter_buffer_size_ms: u32,
    /// Maximum jitter buffer size
    pub max_jitter_buffer_ms: u32,
    /// Enable clock drift correction
    pub enable_clock_drift_correction: bool,
    /// Drift threshold for correction (in PPM)
    pub drift_correction_threshold_ppm: f64,
    /// RTCP interval in milliseconds
    pub rtcp_interval_ms: u32,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            audio_clock_rate: 48000,
            video_clock_rate: 90000,
            jitter_buffer_size_ms: 50,
            max_jitter_buffer_ms: 200,
            enable_clock_drift_correction: true,
            drift_correction_threshold_ppm: 100.0,
            rtcp_interval_ms: 5000,
        }
    }
}

impl SyncConfig {
    /// Validate configuration
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.audio_clock_rate != 48000 {
            return Err("Audio clock rate must be 48000 Hz");
        }
        if self.video_clock_rate != 90000 {
            return Err("Video clock rate must be 90000 Hz");
        }
        if self.jitter_buffer_size_ms < 50 || self.jitter_buffer_size_ms > 200 {
            return Err("Jitter buffer size must be between 50 and 200 ms");
        }
        if self.max_jitter_buffer_ms < self.jitter_buffer_size_ms {
            return Err("Max jitter buffer must be >= jitter buffer size");
        }
        Ok(())
    }
}

/// Raw audio frame from RTP
#[derive(Debug, Clone)]
pub struct AudioFrame {
    /// RTP timestamp
    pub rtp_timestamp: u32,
    /// RTP sequence number
    pub rtp_sequence: u16,
    /// Audio samples (f32, mono or interleaved stereo)
    pub samples: Arc<Vec<f32>>,
    /// When frame was received
    pub received_at: Instant,
    /// Payload size in bytes
    pub payload_size: usize,
}

impl JitterBufferFrame for AudioFrame {
    fn sequence_number(&self) -> u16 {
        self.rtp_sequence
    }
    fn rtp_timestamp(&self) -> u32 {
        self.rtp_timestamp
    }
    fn received_at(&self) -> Instant {
        self.received_at
    }
}

/// Synchronized audio frame ready for playback
#[derive(Debug, Clone)]
pub struct SyncedAudioFrame {
    /// Audio samples
    pub samples: Arc<Vec<f32>>,
    /// Sample rate (Hz)
    pub sample_rate: u32,
    /// Wall-clock timestamp in microseconds since Unix epoch
    pub wall_clock_timestamp_us: u64,
    /// Original RTP timestamp
    pub rtp_timestamp: u32,
    /// Buffer delay applied (ms)
    pub buffer_delay_ms: u32,
    /// Confidence in synchronization (0.0-1.0)
    pub sync_confidence: f32,
    /// Estimated clock drift (PPM)
    pub clock_drift_ppm: f64,
}

/// Raw video frame from RTP
#[derive(Debug, Clone)]
pub struct VideoFrame {
    /// RTP timestamp
    pub rtp_timestamp: u32,
    /// RTP sequence number
    pub rtp_sequence: u16,
    /// Frame width
    pub width: u32,
    /// Frame height
    pub height: u32,
    /// Pixel format (I420, NV12, etc.)
    pub format: String,
    /// Plane data (Y, U, V for I420)
    pub planes: Vec<Vec<u8>>,
    /// When frame was received
    pub received_at: Instant,
    /// RTP marker bit (end of frame)
    pub marker_bit: bool,
    /// Is keyframe
    pub is_keyframe: bool,
}

impl JitterBufferFrame for VideoFrame {
    fn sequence_number(&self) -> u16 {
        self.rtp_sequence
    }
    fn rtp_timestamp(&self) -> u32 {
        self.rtp_timestamp
    }
    fn received_at(&self) -> Instant {
        self.received_at
    }
}

/// Synchronized video frame ready for display
#[derive(Debug, Clone)]
pub struct SyncedVideoFrame {
    /// Frame width
    pub width: u32,
    /// Frame height
    pub height: u32,
    /// Pixel format
    pub format: String,
    /// Plane data
    pub planes: Vec<Vec<u8>>,
    /// Wall-clock timestamp in microseconds
    pub wall_clock_timestamp_us: u64,
    /// Original RTP timestamp
    pub rtp_timestamp: u32,
    /// Estimated framerate
    pub framerate_estimate: f32,
    /// Buffer delay applied (ms)
    pub buffer_delay_ms: u32,
    /// Audio sync offset in milliseconds (positive = video ahead)
    pub audio_sync_offset_ms: i32,
    /// Confidence in synchronization (0.0-1.0)
    pub sync_confidence: f32,
}

/// Synchronization state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncState {
    /// No synchronization data yet
    Unsynced,
    /// Collecting RTCP SRs for sync
    Syncing,
    /// Fully synchronized
    Synced,
}

/// RTCP Sender Report data
#[derive(Debug, Clone, Copy)]
pub struct RtcpSenderReport {
    /// NTP timestamp (64-bit)
    pub ntp_timestamp: u64,
    /// RTP timestamp at NTP time
    pub rtp_timestamp: u32,
    /// Packets sent
    pub packet_count: u32,
    /// Octets sent
    pub octet_count: u32,
    /// When SR was sent (sender's perspective)
    pub sender_time: SystemTime,
}

impl RtcpSenderReport {
    /// Convert NTP timestamp to microseconds since Unix epoch
    pub fn ntp_to_us(&self) -> u64 {
        const NTP_UNIX_OFFSET_SECS: u64 = 2_208_988_800;

        let ntp_secs = self.ntp_timestamp >> 32;
        let ntp_frac = self.ntp_timestamp & 0xFFFFFFFF;

        if ntp_secs < NTP_UNIX_OFFSET_SECS {
            return 0;
        }

        let unix_secs = ntp_secs - NTP_UNIX_OFFSET_SECS;
        let unix_frac_us = (ntp_frac * 1_000_000) >> 32;
        unix_secs * 1_000_000 + unix_frac_us
    }
}

/// Per-peer synchronization manager
pub struct SyncManager {
    /// Peer ID this manager belongs to
    peer_id: String,
    /// Configuration
    config: SyncConfig,
    /// Audio jitter buffer
    audio_buffer: JitterBuffer<AudioFrame>,
    /// Video jitter buffer
    video_buffer: JitterBuffer<VideoFrame>,
    /// Clock drift estimator
    clock_drift: ClockDriftEstimator,
    /// Audio NTP/RTP mapping
    audio_ntp_mapping: Option<NtpRtpMapping>,
    /// Video NTP/RTP mapping
    video_ntp_mapping: Option<NtpRtpMapping>,
    /// Last RTCP SR time
    last_rtcp_time: Option<Instant>,
    /// Audio/video sync offset (in RTP timestamp units at video clock rate)
    sync_offset: i64,
    /// Current sync state
    sync_state: SyncState,
    /// RTCP SR count received
    rtcp_sr_count: u32,
    /// Last audio RTP timestamp for framerate estimation
    last_audio_rtp_ts: Option<u32>,
    /// Last video RTP timestamp for framerate estimation
    last_video_rtp_ts: Option<u32>,
    /// Video frame timestamps for framerate calculation
    video_timestamps: Vec<Instant>,
}

impl SyncManager {
    /// Create a new synchronization manager for a peer
    ///
    /// # Arguments
    /// * `peer_id` - Identifier for this peer
    /// * `config` - Synchronization configuration
    pub fn new(peer_id: String, config: SyncConfig) -> Result<Self, &'static str> {
        config.validate()?;

        let audio_buffer =
            JitterBuffer::new(config.jitter_buffer_size_ms, config.max_jitter_buffer_ms);
        let video_buffer =
            JitterBuffer::new(config.jitter_buffer_size_ms, config.max_jitter_buffer_ms);
        let clock_drift = ClockDriftEstimator::with_threshold(
            peer_id.clone(),
            config.drift_correction_threshold_ppm,
        );

        Ok(Self {
            peer_id,
            config,
            audio_buffer,
            video_buffer,
            clock_drift,
            audio_ntp_mapping: None,
            video_ntp_mapping: None,
            last_rtcp_time: None,
            sync_offset: 0,
            sync_state: SyncState::Unsynced,
            rtcp_sr_count: 0,
            last_audio_rtp_ts: None,
            last_video_rtp_ts: None,
            video_timestamps: Vec::with_capacity(30),
        })
    }

    /// Get peer ID
    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    /// Get current sync state
    pub fn get_sync_state(&self) -> SyncState {
        self.sync_state
    }

    /// Process incoming audio frame
    ///
    /// Inserts frame into jitter buffer and tracks RTP timestamps
    pub fn process_audio_frame(&mut self, frame: AudioFrame) -> Result<(), &'static str> {
        self.last_audio_rtp_ts = Some(frame.rtp_timestamp);
        self.audio_buffer.insert(frame)
    }

    /// Process incoming video frame
    ///
    /// Inserts frame into jitter buffer and calculates audio sync offset
    pub fn process_video_frame(&mut self, frame: VideoFrame) -> Result<(), &'static str> {
        self.last_video_rtp_ts = Some(frame.rtp_timestamp);

        // Track timestamps for framerate estimation
        self.video_timestamps.push(frame.received_at);
        if self.video_timestamps.len() > 60 {
            self.video_timestamps.remove(0);
        }

        self.video_buffer.insert(frame)
    }

    /// Pop next synchronized audio frame
    ///
    /// Returns frame if ready (buffer delay elapsed), with sync metadata
    pub fn pop_next_audio_frame(&mut self) -> Option<SyncedAudioFrame> {
        let frame = self.audio_buffer.pop_next()?;

        // Calculate wall-clock timestamp
        let wall_clock_us = self.calculate_audio_wall_clock(frame.rtp_timestamp);

        // Get drift estimate
        let drift_estimate = self.clock_drift.estimate_drift();
        let drift_ppm = drift_estimate.map(|e| e.drift_ppm).unwrap_or(0.0);
        let confidence = drift_estimate.map(|e| e.confidence as f32).unwrap_or(0.0);

        // Apply drift correction if enabled
        let corrected_samples = if self.config.enable_clock_drift_correction {
            self.apply_drift_correction(&frame.samples, drift_estimate)
        } else {
            frame.samples.clone()
        };

        let buffer_stats = self.audio_buffer.get_statistics();

        Some(SyncedAudioFrame {
            samples: corrected_samples,
            sample_rate: self.config.audio_clock_rate,
            wall_clock_timestamp_us: wall_clock_us,
            rtp_timestamp: frame.rtp_timestamp,
            buffer_delay_ms: buffer_stats.current_delay_ms,
            sync_confidence: if self.sync_state == SyncState::Synced {
                confidence
            } else {
                0.0
            },
            clock_drift_ppm: drift_ppm,
        })
    }

    /// Pop next synchronized video frame
    ///
    /// Returns frame with audio sync offset for lip-sync
    pub fn pop_next_video_frame(&mut self) -> Option<SyncedVideoFrame> {
        let frame = self.video_buffer.pop_next()?;

        // Calculate wall-clock timestamp
        let wall_clock_us = self.calculate_video_wall_clock(frame.rtp_timestamp);

        // Calculate audio sync offset
        let audio_sync_offset_ms = self.calculate_audio_sync_offset(frame.rtp_timestamp);

        // Estimate framerate
        let framerate = self.estimate_framerate();

        let buffer_stats = self.video_buffer.get_statistics();
        let confidence = if self.sync_state == SyncState::Synced {
            1.0
        } else {
            0.0
        };

        Some(SyncedVideoFrame {
            width: frame.width,
            height: frame.height,
            format: frame.format,
            planes: frame.planes,
            wall_clock_timestamp_us: wall_clock_us,
            rtp_timestamp: frame.rtp_timestamp,
            framerate_estimate: framerate,
            buffer_delay_ms: buffer_stats.current_delay_ms,
            audio_sync_offset_ms,
            sync_confidence: confidence,
        })
    }

    /// Update with RTCP Sender Report
    ///
    /// Updates NTP/RTP mapping for timestamp correlation
    pub fn update_rtcp_sender_report(&mut self, sr: RtcpSenderReport, is_audio: bool) {
        let now = Instant::now();
        self.last_rtcp_time = Some(now);
        self.rtcp_sr_count += 1;

        let mapping = NtpRtpMapping {
            ntp_timestamp: sr.ntp_timestamp,
            rtp_timestamp: sr.rtp_timestamp,
            clock_rate: if is_audio {
                self.config.audio_clock_rate
            } else {
                self.config.video_clock_rate
            },
            received_at: SystemTime::now(),
        };

        if is_audio {
            self.audio_ntp_mapping = Some(mapping);
        } else {
            self.video_ntp_mapping = Some(mapping);
        }

        // Add observation for clock drift
        self.clock_drift
            .add_observation(sr.rtp_timestamp, sr.ntp_timestamp, now);

        // Update sync state
        self.update_sync_state();

        // Calculate audio/video sync offset if we have both mappings
        self.calculate_sync_offset();
    }

    /// Estimate clock drift
    pub fn estimate_clock_drift(&self) -> Option<ClockDriftEstimate> {
        self.clock_drift.estimate_drift()
    }

    /// Apply clock drift correction
    ///
    /// # Arguments
    /// * `correction_factor` - Factor to apply (should be close to 1.0, e.g., 0.99 to 1.01)
    pub fn apply_clock_drift_correction_factor(
        &mut self,
        correction_factor: f32,
    ) -> Result<(), &'static str> {
        if !(0.99..=1.01).contains(&correction_factor) {
            return Err("Correction factor must be in range [0.99, 1.01]");
        }

        // Adjust buffer timing based on correction
        // This is a gradual adjustment to avoid audio glitches
        let adjustment_ms = ((correction_factor - 1.0) * 1000.0) as i32;
        let new_buffer_size =
            (self.config.jitter_buffer_size_ms as i32 + adjustment_ms).clamp(50, 200) as u32;

        self.audio_buffer.set_buffer_size_ms(new_buffer_size);
        self.video_buffer.set_buffer_size_ms(new_buffer_size);

        Ok(())
    }

    /// Reset synchronization state
    pub fn reset(&mut self) {
        self.audio_buffer.clear();
        self.video_buffer.clear();
        self.clock_drift.reset();
        self.audio_ntp_mapping = None;
        self.video_ntp_mapping = None;
        self.last_rtcp_time = None;
        self.sync_offset = 0;
        self.sync_state = SyncState::Unsynced;
        self.rtcp_sr_count = 0;
        self.last_audio_rtp_ts = None;
        self.last_video_rtp_ts = None;
        self.video_timestamps.clear();
    }

    /// Get audio buffer statistics
    pub fn audio_buffer_stats(&self) -> BufferStats {
        self.audio_buffer.get_statistics()
    }

    /// Get video buffer statistics
    pub fn video_buffer_stats(&self) -> BufferStats {
        self.video_buffer.get_statistics()
    }

    /// Align timestamps with another peer's SyncManager
    ///
    /// Calculates the offset between two peers' wall-clock timestamps
    pub fn align_with_peer(&self, other: &SyncManager) -> Option<TimestampOffset> {
        // Need both managers to be synced
        if self.sync_state != SyncState::Synced || other.sync_state != SyncState::Synced {
            return None;
        }

        // Get reference audio timestamps
        let self_mapping = self.audio_ntp_mapping.as_ref()?;
        let other_mapping = other.audio_ntp_mapping.as_ref()?;

        // Calculate wall-clock difference at same RTP timestamp
        let self_wall = RtpTimestamp::to_wall_clock(
            self_mapping.rtp_timestamp,
            self_mapping,
            self.config.audio_clock_rate,
        )?;
        let other_wall = RtpTimestamp::to_wall_clock(
            other_mapping.rtp_timestamp,
            other_mapping,
            other.config.audio_clock_rate,
        )?;

        let offset_us = self_wall as i64 - other_wall as i64;
        let offset_ms = (offset_us / 1000) as i32;

        // Determine stability (based on drift estimates)
        let self_drift = self.clock_drift.estimate_drift();
        let other_drift = other.clock_drift.estimate_drift();

        let is_stable = match (self_drift, other_drift) {
            (Some(d1), Some(d2)) => {
                d1.confidence > 0.8
                    && d2.confidence > 0.8
                    && d1.drift_ppm.abs() < 200.0
                    && d2.drift_ppm.abs() < 200.0
            }
            _ => false,
        };

        let confidence = match (self_drift, other_drift) {
            (Some(d1), Some(d2)) => (d1.confidence + d2.confidence) / 2.0,
            _ => 0.5,
        };

        Some(TimestampOffset {
            offset_ms,
            confidence,
            is_stable,
        })
    }

    // Private helper methods

    fn update_sync_state(&mut self) {
        self.sync_state = match self.rtcp_sr_count {
            0 => SyncState::Unsynced,
            1..=2 => SyncState::Syncing,
            _ => SyncState::Synced,
        };
    }

    fn calculate_sync_offset(&mut self) {
        if let (Some(audio), Some(video)) = (&self.audio_ntp_mapping, &self.video_ntp_mapping) {
            // Convert audio RTP to video clock rate for comparison
            let audio_at_video_rate = (audio.rtp_timestamp as u64
                * self.config.video_clock_rate as u64)
                / self.config.audio_clock_rate as u64;

            self.sync_offset = video.rtp_timestamp as i64 - audio_at_video_rate as i64;
        }
    }

    fn calculate_audio_wall_clock(&self, rtp_ts: u32) -> u64 {
        if let Some(mapping) = &self.audio_ntp_mapping {
            RtpTimestamp::to_wall_clock(rtp_ts, mapping, self.config.audio_clock_rate)
                .unwrap_or_else(|| {
                    // Fallback to current time
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_micros() as u64)
                        .unwrap_or(0)
                })
        } else {
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0)
        }
    }

    fn calculate_video_wall_clock(&self, rtp_ts: u32) -> u64 {
        if let Some(mapping) = &self.video_ntp_mapping {
            RtpTimestamp::to_wall_clock(rtp_ts, mapping, self.config.video_clock_rate)
                .unwrap_or_else(|| {
                    SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .map(|d| d.as_micros() as u64)
                        .unwrap_or(0)
                })
        } else {
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0)
        }
    }

    fn calculate_audio_sync_offset(&self, _video_rtp_ts: u32) -> i32 {
        // Convert sync_offset from video clock units to milliseconds
        let offset_ms = (self.sync_offset * 1000) / self.config.video_clock_rate as i64;
        offset_ms as i32
    }

    fn estimate_framerate(&self) -> f32 {
        if self.video_timestamps.len() < 2 {
            return 30.0; // Default
        }

        let first = self.video_timestamps.first().unwrap();
        let last = self.video_timestamps.last().unwrap();
        let duration = last.duration_since(*first);

        if duration.as_secs_f32() > 0.0 {
            (self.video_timestamps.len() - 1) as f32 / duration.as_secs_f32()
        } else {
            30.0
        }
    }

    fn apply_drift_correction(
        &self,
        samples: &Arc<Vec<f32>>,
        drift: Option<ClockDriftEstimate>,
    ) -> Arc<Vec<f32>> {
        match drift {
            Some(d) if d.recommended_action == DriftAction::Adjust => {
                // For now, just pass through - actual resampling would require
                // a proper audio resampler like rubato
                // TODO: Implement actual drift correction with resampling
                samples.clone()
            }
            _ => samples.clone(),
        }
    }
}

/// Timestamp offset between two peers
#[derive(Debug, Clone, Copy)]
pub struct TimestampOffset {
    /// Offset in milliseconds
    pub offset_ms: i32,
    /// Confidence in the offset (0.0-1.0)
    pub confidence: f64,
    /// Whether the offset is stable
    pub is_stable: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_config_validation() {
        let config = SyncConfig::default();
        assert!(config.validate().is_ok());

        let bad_config = SyncConfig {
            audio_clock_rate: 44100, // Wrong!
            ..Default::default()
        };
        assert!(bad_config.validate().is_err());
    }

    #[test]
    fn test_sync_manager_creation() {
        let config = SyncConfig::default();
        let manager = SyncManager::new("peer1".to_string(), config).unwrap();
        assert_eq!(manager.peer_id(), "peer1");
        assert_eq!(manager.get_sync_state(), SyncState::Unsynced);
    }

    #[test]
    fn test_sync_state_progression() {
        let config = SyncConfig::default();
        let mut manager = SyncManager::new("peer1".to_string(), config).unwrap();

        // Initial state
        assert_eq!(manager.get_sync_state(), SyncState::Unsynced);

        // After first SR
        let sr = RtcpSenderReport {
            ntp_timestamp: 3_913_056_000u64 << 32,
            rtp_timestamp: 0,
            packet_count: 100,
            octet_count: 10000,
            sender_time: SystemTime::now(),
        };
        manager.update_rtcp_sender_report(sr, true);
        assert_eq!(manager.get_sync_state(), SyncState::Syncing);

        // After multiple SRs
        manager.update_rtcp_sender_report(sr, true);
        manager.update_rtcp_sender_report(sr, true);
        assert_eq!(manager.get_sync_state(), SyncState::Synced);
    }

    #[test]
    fn test_audio_frame_processing() {
        let config = SyncConfig {
            jitter_buffer_size_ms: 50, // Minimum valid delay
            ..Default::default()
        };
        let mut manager = SyncManager::new("peer1".to_string(), config).unwrap();

        // Use an old timestamp so the frame is "ready" immediately
        let frame = AudioFrame {
            rtp_timestamp: 960,
            rtp_sequence: 1,
            samples: Arc::new(vec![0.0; 960]),
            received_at: Instant::now() - std::time::Duration::from_millis(100),
            payload_size: 160,
        };

        manager.process_audio_frame(frame).unwrap();

        // Frame should be ready since it's 100ms old (> 50ms buffer)
        let synced = manager.pop_next_audio_frame().unwrap();
        assert_eq!(synced.sample_rate, 48000);
    }

    #[test]
    fn test_reset() {
        let config = SyncConfig::default();
        let mut manager = SyncManager::new("peer1".to_string(), config).unwrap();

        // Add some state
        let sr = RtcpSenderReport {
            ntp_timestamp: 3_913_056_000u64 << 32,
            rtp_timestamp: 0,
            packet_count: 100,
            octet_count: 10000,
            sender_time: SystemTime::now(),
        };
        manager.update_rtcp_sender_report(sr, true);
        manager.update_rtcp_sender_report(sr, true);
        manager.update_rtcp_sender_report(sr, true);

        assert_eq!(manager.get_sync_state(), SyncState::Synced);

        manager.reset();

        assert_eq!(manager.get_sync_state(), SyncState::Unsynced);
    }

    #[test]
    fn test_rtcp_sender_report_ntp_conversion() {
        // NTP for 2024-01-01 00:00:00 UTC
        let sr = RtcpSenderReport {
            ntp_timestamp: 3_913_056_000u64 << 32,
            rtp_timestamp: 0,
            packet_count: 0,
            octet_count: 0,
            sender_time: SystemTime::now(),
        };

        let us = sr.ntp_to_us();
        // Should be 1704067200 seconds * 1_000_000
        assert_eq!(us, 1_704_067_200_000_000);
    }

    #[test]
    fn test_drift_correction_bounds() {
        let config = SyncConfig::default();
        let mut manager = SyncManager::new("peer1".to_string(), config).unwrap();

        // Valid correction
        assert!(manager.apply_clock_drift_correction_factor(1.001).is_ok());

        // Invalid corrections
        assert!(manager.apply_clock_drift_correction_factor(1.02).is_err());
        assert!(manager.apply_clock_drift_correction_factor(0.98).is_err());
    }
}
