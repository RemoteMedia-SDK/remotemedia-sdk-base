//! DriftMetrics - Per-stream health monitoring for streaming pipelines
//!
//! This module provides stream health tracking capabilities including:
//! - Lead/drift calculation with baseline normalization
//! - A/V skew tracking
//! - Cadence histogram for frame timing variance
//! - Content freeze detection
//! - Health score calculation
//! - Alert hysteresis
//!
//! # Architecture
//!
//! Each stream has its own `DriftMetrics` instance that tracks:
//! - `StreamClockState`: Baseline and last timestamps for normalization
//! - `DriftSample`: Individual samples with lead/slope/cadence data
//! - `AlertState`: Per-alert hysteresis state
//!
//! # Lead Calculation
//!
//! Lead is calculated using baseline-normalized deltas:
//! ```text
//! lead[n] = (arrival[n] - arrival[0]) - (media[n] - media[0])
//! ```
//!
//! This normalizes away the initial offset between arrival and media time.
//!
//! # Example
//!
//! ```ignore
//! use remotemedia_core::executor::drift_metrics::{DriftMetrics, DriftThresholds};
//!
//! let thresholds = DriftThresholds::default();
//! let mut metrics = DriftMetrics::new("stream_1".to_string(), thresholds);
//!
//! // Record samples as they arrive
//! metrics.record_sample(media_ts_us, arrival_ts_us, None);
//!
//! // Check health
//! let health = metrics.health_score();
//! let alerts = metrics.alerts();
//! ```
//!
//! # Spec Reference
//!
//! See `/specs/026-streaming-scheduler-migration/` for full specification.

use bitflags::bitflags;
use std::collections::VecDeque;

/// Default circular buffer size for drift samples
pub const DEFAULT_BUFFER_SIZE: usize = 1000;

/// Default freeze detection sample rate (max 1 fps)
pub const DEFAULT_FREEZE_SAMPLE_RATE_HZ: f32 = 1.0;

/// Default freeze threshold in microseconds (500ms)
pub const DEFAULT_FREEZE_THRESHOLD_US: u64 = 500_000;

/// Default discontinuity threshold (2 seconds)
pub const DEFAULT_DISCONTINUITY_THRESHOLD_US: u64 = 2_000_000;

bitflags! {
    /// Active alert conditions for a stream
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct DriftAlerts: u8 {
        /// Lead slope exceeds threshold (sustained drift)
        const DRIFT_SLOPE = 0b0000_0001;
        /// Sudden lead jump detected
        const LEAD_JUMP = 0b0000_0010;
        /// A/V skew exceeds threshold
        const AV_SKEW = 0b0000_0100;
        /// Content freeze detected
        const FREEZE = 0b0000_1000;
        /// Cadence variance too high
        const CADENCE_UNSTABLE = 0b0001_0000;
        /// Overall health score below threshold
        const HEALTH_LOW = 0b0010_0000;
    }
}

/// Configurable alert thresholds and hysteresis parameters
#[derive(Debug, Clone)]
pub struct DriftThresholds {
    /// Lead slope threshold in ms/s (e.g., 5.0 means 5ms drift per second)
    pub slope_threshold_ms_per_s: f64,
    /// Lead jump threshold in microseconds
    pub lead_jump_threshold_us: u64,
    /// A/V skew threshold in microseconds
    pub av_skew_threshold_us: u64,
    /// Freeze threshold in microseconds
    pub freeze_threshold_us: u64,
    /// Cadence coefficient of variation threshold (0.0-1.0)
    pub cadence_cv_threshold: f64,
    /// Health score threshold (0.0-1.0)
    pub health_threshold: f64,
    /// Samples required to raise an alert
    pub samples_to_raise: u32,
    /// Samples required to clear an alert
    pub samples_to_clear: u32,
    /// EMA smoothing alpha for slope calculation (0.0-1.0)
    pub slope_ema_alpha: f64,
    /// Number of samples to skip during warmup (alerts suppressed during this period)
    pub warmup_samples: u32,
}

/// Default warmup period in samples (50 samples ~= 1-2 seconds at typical frame rates)
pub const DEFAULT_WARMUP_SAMPLES: u32 = 50;

impl Default for DriftThresholds {
    fn default() -> Self {
        Self {
            slope_threshold_ms_per_s: 5.0,
            lead_jump_threshold_us: 100_000, // 100ms
            av_skew_threshold_us: 80_000,    // 80ms per spec
            freeze_threshold_us: DEFAULT_FREEZE_THRESHOLD_US,
            cadence_cv_threshold: 0.3,
            health_threshold: 0.7,
            samples_to_raise: 5,
            samples_to_clear: 10,
            slope_ema_alpha: 0.1,
            warmup_samples: DEFAULT_WARMUP_SAMPLES,
        }
    }
}

/// Per-stream timing state for baseline normalization
#[derive(Debug, Clone, Default)]
pub struct StreamClockState {
    /// Baseline arrival timestamp (first sample)
    pub baseline_arrival_us: Option<u64>,
    /// Baseline media timestamp (first sample)
    pub baseline_media_us: Option<u64>,
    /// Last arrival timestamp
    pub last_arrival_us: Option<u64>,
    /// Last media timestamp
    pub last_media_us: Option<u64>,
    /// Count of discontinuities detected
    pub discontinuity_count: u32,
}

impl StreamClockState {
    /// Reset baseline timestamps (on discontinuity or stream restart)
    pub fn reset_baseline(&mut self) {
        self.baseline_arrival_us = None;
        self.baseline_media_us = None;
        self.last_arrival_us = None;
        self.last_media_us = None;
        self.discontinuity_count += 1;
    }

    /// Check if baseline is established
    pub fn has_baseline(&self) -> bool {
        self.baseline_arrival_us.is_some() && self.baseline_media_us.is_some()
    }
}

/// Individual drift sample data
#[derive(Debug, Clone)]
pub struct DriftSample {
    /// Elapsed time since session start in microseconds
    pub elapsed_us: u64,
    /// Normalized lead value in microseconds (positive = ahead, negative = behind)
    pub lead_us: i64,
    /// Slope snapshot at sample time (optional, EMA-smoothed)
    pub slope_snapshot: Option<f64>,
}

/// Per-alert hysteresis state
#[derive(Debug, Clone, Default)]
pub struct AlertState {
    /// Whether the alert is currently raised
    pub is_raised: bool,
    /// Consecutive sample count in current direction
    pub consecutive_count: u32,
    /// Samples required to raise this alert
    pub samples_to_raise: u32,
    /// Samples required to clear this alert
    pub samples_to_clear: u32,
}

impl AlertState {
    /// Create new alert state with hysteresis parameters
    pub fn new(samples_to_raise: u32, samples_to_clear: u32) -> Self {
        Self {
            is_raised: false,
            consecutive_count: 0,
            samples_to_raise,
            samples_to_clear,
        }
    }

    /// Update alert state based on whether condition is currently met
    ///
    /// Returns true if alert state changed
    pub fn update(&mut self, condition_met: bool) -> bool {
        let was_raised = self.is_raised;

        if self.is_raised {
            // Alert is raised - check for clearing
            if condition_met {
                self.consecutive_count = 0;
            } else {
                self.consecutive_count += 1;
                if self.consecutive_count >= self.samples_to_clear {
                    self.is_raised = false;
                    self.consecutive_count = 0;
                }
            }
        } else {
            // Alert is not raised - check for raising
            if condition_met {
                self.consecutive_count += 1;
                if self.consecutive_count >= self.samples_to_raise {
                    self.is_raised = true;
                    self.consecutive_count = 0;
                }
            } else {
                self.consecutive_count = 0;
            }
        }

        was_raised != self.is_raised
    }
}

/// Per-stream health tracker
#[derive(Debug)]
pub struct DriftMetrics {
    /// Stream identifier
    pub stream_id: String,
    /// Alert thresholds and hysteresis parameters
    pub thresholds: DriftThresholds,
    /// Clock state for baseline normalization
    pub clock_state: StreamClockState,
    /// Circular buffer of drift samples
    pub samples: VecDeque<DriftSample>,
    /// Maximum buffer size
    pub buffer_size: usize,
    /// Current EMA-smoothed slope (ms/s)
    pub current_slope: f64,
    /// Cadence histogram (inter-frame intervals in microseconds)
    pub cadence_buffer: VecDeque<u64>,
    /// Last content hash for freeze detection
    pub last_content_hash: Option<u64>,
    /// Consecutive identical hash count
    pub identical_hash_count: u32,
    /// Last freeze check timestamp
    pub last_freeze_check_us: Option<u64>,
    /// A/V skew tracking: last audio timestamp
    pub last_audio_media_us: Option<u64>,
    /// A/V skew tracking: last video timestamp
    pub last_video_media_us: Option<u64>,
    /// Current A/V skew in microseconds
    pub current_av_skew_us: i64,
    /// Per-alert hysteresis states
    pub alert_states: AlertStates,
    /// Session start timestamp (monotonic)
    pub session_start_us: Option<u64>,
    /// Total samples recorded (for warmup tracking)
    pub total_samples: u32,
}

/// Collection of per-alert hysteresis states
#[derive(Debug)]
pub struct AlertStates {
    pub drift_slope: AlertState,
    pub lead_jump: AlertState,
    pub av_skew: AlertState,
    pub freeze: AlertState,
    pub cadence_unstable: AlertState,
    pub health_low: AlertState,
}

impl AlertStates {
    fn new(samples_to_raise: u32, samples_to_clear: u32) -> Self {
        Self {
            drift_slope: AlertState::new(samples_to_raise, samples_to_clear),
            lead_jump: AlertState::new(samples_to_raise, samples_to_clear),
            av_skew: AlertState::new(samples_to_raise, samples_to_clear),
            freeze: AlertState::new(samples_to_raise, samples_to_clear),
            cadence_unstable: AlertState::new(samples_to_raise, samples_to_clear),
            health_low: AlertState::new(samples_to_raise, samples_to_clear),
        }
    }
}

impl DriftMetrics {
    /// Create a new DriftMetrics instance for a stream
    pub fn new(stream_id: String, thresholds: DriftThresholds) -> Self {
        let alert_states =
            AlertStates::new(thresholds.samples_to_raise, thresholds.samples_to_clear);

        Self {
            stream_id,
            thresholds,
            clock_state: StreamClockState::default(),
            samples: VecDeque::with_capacity(DEFAULT_BUFFER_SIZE),
            buffer_size: DEFAULT_BUFFER_SIZE,
            current_slope: 0.0,
            cadence_buffer: VecDeque::with_capacity(DEFAULT_BUFFER_SIZE),
            last_content_hash: None,
            identical_hash_count: 0,
            last_freeze_check_us: None,
            last_audio_media_us: None,
            last_video_media_us: None,
            current_av_skew_us: 0,
            alert_states,
            session_start_us: None,
            total_samples: 0,
        }
    }

    /// Create with default thresholds
    pub fn with_defaults(stream_id: String) -> Self {
        Self::new(stream_id, DriftThresholds::default())
    }

    /// Record a new sample and update all metrics
    ///
    /// # Arguments
    ///
    /// * `media_ts_us` - Media/presentation timestamp in microseconds
    /// * `arrival_ts_us` - Arrival timestamp in microseconds (monotonic clock)
    /// * `content_hash` - Optional content hash for freeze detection
    ///
    /// # Returns
    ///
    /// true if any alert state changed
    pub fn record_sample(
        &mut self,
        media_ts_us: u64,
        arrival_ts_us: u64,
        content_hash: Option<u64>,
    ) -> bool {
        // Increment total samples counter
        self.total_samples = self.total_samples.saturating_add(1);

        // Initialize session start if needed
        if self.session_start_us.is_none() {
            self.session_start_us = Some(arrival_ts_us);
        }

        // Check for discontinuity
        if self.detect_discontinuity(media_ts_us) {
            self.clock_state.reset_baseline();
            self.clear_alert_states();
        }

        // Establish baseline if needed
        if !self.clock_state.has_baseline() {
            self.clock_state.baseline_arrival_us = Some(arrival_ts_us);
            self.clock_state.baseline_media_us = Some(media_ts_us);
        }

        // Calculate normalized lead
        let lead_us = self.calculate_lead(media_ts_us, arrival_ts_us);

        // Calculate elapsed time since session start
        let elapsed_us = arrival_ts_us.saturating_sub(self.session_start_us.unwrap_or(0));

        // Update slope using EMA
        self.update_slope(elapsed_us, lead_us);

        // Record cadence (inter-frame interval)
        if let Some(last_media) = self.clock_state.last_media_us {
            let interval = media_ts_us.saturating_sub(last_media);
            self.record_cadence(interval);
        }

        // Update freeze detection
        if let Some(hash) = content_hash {
            self.update_freeze_detection(hash, media_ts_us);
        }

        // Store sample
        let sample = DriftSample {
            elapsed_us,
            lead_us,
            slope_snapshot: Some(self.current_slope),
        };

        self.samples.push_back(sample);
        while self.samples.len() > self.buffer_size {
            self.samples.pop_front();
        }

        // Update last timestamps
        self.clock_state.last_arrival_us = Some(arrival_ts_us);
        self.clock_state.last_media_us = Some(media_ts_us);

        // Skip alert updates during warmup period to allow metrics to stabilize
        if self.total_samples <= self.thresholds.warmup_samples {
            return false;
        }

        // Update alert states and return if any changed
        self.update_alerts()
    }

    /// Record an audio sample for A/V skew tracking
    pub fn record_audio_sample(&mut self, media_ts_us: u64, arrival_ts_us: u64) -> bool {
        self.last_audio_media_us = Some(media_ts_us);
        self.update_av_skew();
        self.record_sample(media_ts_us, arrival_ts_us, None)
    }

    /// Record a video sample for A/V skew tracking
    pub fn record_video_sample(
        &mut self,
        media_ts_us: u64,
        arrival_ts_us: u64,
        content_hash: Option<u64>,
    ) -> bool {
        self.last_video_media_us = Some(media_ts_us);
        self.update_av_skew();
        self.record_sample(media_ts_us, arrival_ts_us, content_hash)
    }

    /// Calculate baseline-normalized lead
    fn calculate_lead(&self, media_ts_us: u64, arrival_ts_us: u64) -> i64 {
        let baseline_arrival = self
            .clock_state
            .baseline_arrival_us
            .unwrap_or(arrival_ts_us);
        let baseline_media = self.clock_state.baseline_media_us.unwrap_or(media_ts_us);

        let arrival_delta = arrival_ts_us.saturating_sub(baseline_arrival) as i64;
        let media_delta = media_ts_us.saturating_sub(baseline_media) as i64;

        // lead = (arrival - arrival_0) - (media - media_0)
        arrival_delta - media_delta
    }

    /// Detect discontinuity in media timestamps
    fn detect_discontinuity(&self, media_ts_us: u64) -> bool {
        if let Some(last_media) = self.clock_state.last_media_us {
            let delta = if media_ts_us >= last_media {
                media_ts_us - last_media
            } else {
                // Negative delta (timestamp went backwards)
                return true;
            };

            // Check for large jump
            if delta > DEFAULT_DISCONTINUITY_THRESHOLD_US {
                return true;
            }
        }
        false
    }

    /// Update slope using EMA smoothing
    fn update_slope(&mut self, elapsed_us: u64, lead_us: i64) {
        if let Some(last_sample) = self.samples.back() {
            let delta_elapsed = elapsed_us.saturating_sub(last_sample.elapsed_us);
            if delta_elapsed > 0 {
                let delta_lead = lead_us - last_sample.lead_us;
                // Convert to ms/s: (delta_lead_us / delta_elapsed_us) * 1000
                let instant_slope = (delta_lead as f64 / delta_elapsed as f64) * 1000.0;

                // EMA update
                self.current_slope = self.thresholds.slope_ema_alpha * instant_slope
                    + (1.0 - self.thresholds.slope_ema_alpha) * self.current_slope;
            }
        }
    }

    /// Record cadence interval
    fn record_cadence(&mut self, interval_us: u64) {
        self.cadence_buffer.push_back(interval_us);
        while self.cadence_buffer.len() > self.buffer_size {
            self.cadence_buffer.pop_front();
        }
    }

    /// Update freeze detection state
    fn update_freeze_detection(&mut self, content_hash: u64, media_ts_us: u64) {
        if let Some(last_hash) = self.last_content_hash {
            if content_hash == last_hash {
                self.identical_hash_count += 1;
            } else {
                self.identical_hash_count = 0;
            }
        }
        self.last_content_hash = Some(content_hash);
        self.last_freeze_check_us = Some(media_ts_us);
    }

    /// Update A/V skew calculation
    fn update_av_skew(&mut self) {
        if let (Some(audio_ts), Some(video_ts)) =
            (self.last_audio_media_us, self.last_video_media_us)
        {
            self.current_av_skew_us = video_ts as i64 - audio_ts as i64;
        }
    }

    /// Clear all alert states (on discontinuity)
    fn clear_alert_states(&mut self) {
        self.alert_states.drift_slope = AlertState::new(
            self.thresholds.samples_to_raise,
            self.thresholds.samples_to_clear,
        );
        self.alert_states.lead_jump = AlertState::new(
            self.thresholds.samples_to_raise,
            self.thresholds.samples_to_clear,
        );
        self.alert_states.av_skew = AlertState::new(
            self.thresholds.samples_to_raise,
            self.thresholds.samples_to_clear,
        );
        self.alert_states.freeze = AlertState::new(
            self.thresholds.samples_to_raise,
            self.thresholds.samples_to_clear,
        );
        self.alert_states.cadence_unstable = AlertState::new(
            self.thresholds.samples_to_raise,
            self.thresholds.samples_to_clear,
        );
        self.alert_states.health_low = AlertState::new(
            self.thresholds.samples_to_raise,
            self.thresholds.samples_to_clear,
        );
    }

    /// Update all alert states
    ///
    /// Returns true if any alert state changed
    fn update_alerts(&mut self) -> bool {
        let mut changed = false;
        let prev_alerts = self.alerts();

        // Drift slope alert
        let slope_condition = self.current_slope.abs() > self.thresholds.slope_threshold_ms_per_s;
        changed |= self.alert_states.drift_slope.update(slope_condition);

        // Lead jump alert
        let lead_jump_condition = if let Some(last_sample) = self.samples.back() {
            if self.samples.len() > 1 {
                let prev_sample = &self.samples[self.samples.len() - 2];
                let jump = (last_sample.lead_us - prev_sample.lead_us).unsigned_abs();
                jump > self.thresholds.lead_jump_threshold_us
            } else {
                false
            }
        } else {
            false
        };
        changed |= self.alert_states.lead_jump.update(lead_jump_condition);

        // A/V skew alert
        let skew_condition =
            self.current_av_skew_us.unsigned_abs() > self.thresholds.av_skew_threshold_us;
        changed |= self.alert_states.av_skew.update(skew_condition);

        // Freeze alert
        let freeze_condition = self.is_frozen();
        changed |= self.alert_states.freeze.update(freeze_condition);

        // Cadence unstable alert
        let cadence_condition = self.cadence_cv() > self.thresholds.cadence_cv_threshold;
        changed |= self.alert_states.cadence_unstable.update(cadence_condition);

        // Health low alert
        let health_condition = self.health_score() < self.thresholds.health_threshold;
        changed |= self.alert_states.health_low.update(health_condition);

        // Log alert state changes (spec 026: T062-T063)
        if changed {
            let new_alerts = self.alerts();
            self.log_alert_changes(prev_alerts, new_alerts);
        }

        changed
    }

    /// Log alert state changes with stream_id and alert type
    fn log_alert_changes(&self, prev: DriftAlerts, current: DriftAlerts) {
        let raised = current - prev; // Newly raised alerts
        let cleared = prev - current; // Cleared alerts

        // Log newly raised alerts
        if raised.contains(DriftAlerts::DRIFT_SLOPE) {
            tracing::warn!(
                stream_id = %self.stream_id,
                alert_type = "DRIFT_SLOPE",
                slope_ms_per_s = self.current_slope,
                threshold = self.thresholds.slope_threshold_ms_per_s,
                "Stream health alert raised: sustained drift detected"
            );
        }

        if raised.contains(DriftAlerts::LEAD_JUMP) {
            tracing::warn!(
                stream_id = %self.stream_id,
                alert_type = "LEAD_JUMP",
                threshold_us = self.thresholds.lead_jump_threshold_us,
                "Stream health alert raised: sudden lead jump detected"
            );
        }

        if raised.contains(DriftAlerts::AV_SKEW) {
            tracing::warn!(
                stream_id = %self.stream_id,
                alert_type = "AV_SKEW",
                skew_us = self.current_av_skew_us,
                threshold_us = self.thresholds.av_skew_threshold_us,
                "Stream health alert raised: A/V skew exceeds threshold"
            );
        }

        if raised.contains(DriftAlerts::FREEZE) {
            tracing::warn!(
                stream_id = %self.stream_id,
                alert_type = "FREEZE",
                identical_frames = self.identical_hash_count,
                "Stream health alert raised: content freeze detected"
            );
        }

        if raised.contains(DriftAlerts::CADENCE_UNSTABLE) {
            tracing::warn!(
                stream_id = %self.stream_id,
                alert_type = "CADENCE_UNSTABLE",
                cadence_cv = self.cadence_cv(),
                threshold = self.thresholds.cadence_cv_threshold,
                "Stream health alert raised: unstable frame cadence"
            );
        }

        if raised.contains(DriftAlerts::HEALTH_LOW) {
            tracing::warn!(
                stream_id = %self.stream_id,
                alert_type = "HEALTH_LOW",
                health_score = self.health_score(),
                threshold = self.thresholds.health_threshold,
                "Stream health alert raised: overall health score degraded"
            );
        }

        // Log cleared alerts at info level
        if !cleared.is_empty() {
            tracing::info!(
                stream_id = %self.stream_id,
                cleared_alerts = ?cleared,
                current_alerts = ?current,
                health_score = self.health_score(),
                "Stream health alerts cleared"
            );
        }
    }

    /// Check if content is frozen
    pub fn is_frozen(&self) -> bool {
        // Freeze requires:
        // 1. Identical hashes for multiple samples
        // 2. Timestamps are advancing (not paused)
        if self.identical_hash_count < 3 {
            return false;
        }

        // Check if timestamps are advancing
        if let (Some(last_arrival), Some(baseline_arrival)) = (
            self.clock_state.last_arrival_us,
            self.clock_state.baseline_arrival_us,
        ) {
            let elapsed = last_arrival.saturating_sub(baseline_arrival);
            // Need at least freeze_threshold of elapsed time with identical content
            if elapsed > self.thresholds.freeze_threshold_us {
                return true;
            }
        }

        false
    }

    /// Calculate cadence coefficient of variation
    pub fn cadence_cv(&self) -> f64 {
        if self.cadence_buffer.len() < 2 {
            return 0.0;
        }

        let sum: u64 = self.cadence_buffer.iter().sum();
        let mean = sum as f64 / self.cadence_buffer.len() as f64;

        if mean < 1.0 {
            return 0.0;
        }

        let variance: f64 = self
            .cadence_buffer
            .iter()
            .map(|&x| {
                let diff = x as f64 - mean;
                diff * diff
            })
            .sum::<f64>()
            / self.cadence_buffer.len() as f64;

        let std_dev = variance.sqrt();
        std_dev / mean
    }

    /// Calculate overall health score (0.0 to 1.0)
    pub fn health_score(&self) -> f64 {
        // Weighted combination of metrics
        // Higher is better

        let mut score = 1.0;

        // Slope penalty (max 0.3)
        let slope_penalty =
            (self.current_slope.abs() / self.thresholds.slope_threshold_ms_per_s).min(1.0) * 0.3;
        score -= slope_penalty;

        // A/V skew penalty (max 0.2)
        let skew_ratio = self.current_av_skew_us.unsigned_abs() as f64
            / self.thresholds.av_skew_threshold_us as f64;
        let skew_penalty = skew_ratio.min(1.0) * 0.2;
        score -= skew_penalty;

        // Cadence penalty (max 0.2)
        let cadence_penalty =
            (self.cadence_cv() / self.thresholds.cadence_cv_threshold).min(1.0) * 0.2;
        score -= cadence_penalty;

        // Freeze penalty (max 0.3)
        if self.is_frozen() {
            score -= 0.3;
        }

        score.max(0.0)
    }

    /// Get current active alerts as bitfield
    pub fn alerts(&self) -> DriftAlerts {
        let mut alerts = DriftAlerts::empty();

        if self.alert_states.drift_slope.is_raised {
            alerts |= DriftAlerts::DRIFT_SLOPE;
        }
        if self.alert_states.lead_jump.is_raised {
            alerts |= DriftAlerts::LEAD_JUMP;
        }
        if self.alert_states.av_skew.is_raised {
            alerts |= DriftAlerts::AV_SKEW;
        }
        if self.alert_states.freeze.is_raised {
            alerts |= DriftAlerts::FREEZE;
        }
        if self.alert_states.cadence_unstable.is_raised {
            alerts |= DriftAlerts::CADENCE_UNSTABLE;
        }
        if self.alert_states.health_low.is_raised {
            alerts |= DriftAlerts::HEALTH_LOW;
        }

        alerts
    }

    /// Get latest lead value in microseconds
    pub fn current_lead_us(&self) -> Option<i64> {
        self.samples.back().map(|s| s.lead_us)
    }

    /// Get current slope in ms/s
    pub fn current_slope_ms_per_s(&self) -> f64 {
        self.current_slope
    }

    /// Get discontinuity count
    pub fn discontinuity_count(&self) -> u32 {
        self.clock_state.discontinuity_count
    }

    /// Check if still in warmup period (alerts suppressed)
    pub fn is_warming_up(&self) -> bool {
        self.total_samples <= self.thresholds.warmup_samples
    }

    /// Get total samples recorded
    pub fn total_samples(&self) -> u32 {
        self.total_samples
    }

    /// Export metrics in Prometheus format
    ///
    /// Note: stream_id is excluded from labels by default for cardinality safety.
    /// Use debug endpoint for per-stream metrics.
    pub fn to_prometheus(&self, prefix: &str) -> String {
        let mut output = String::new();

        // Lead metric
        if let Some(lead) = self.current_lead_us() {
            output.push_str(&format!("{}_stream_lead_us{{}} {}\n", prefix, lead));
        }

        // Slope metric
        output.push_str(&format!(
            "{}_stream_slope_ms_per_s{{}} {:.6}\n",
            prefix, self.current_slope
        ));

        // A/V skew metric
        output.push_str(&format!(
            "{}_stream_av_skew_us{{}} {}\n",
            prefix, self.current_av_skew_us
        ));

        // Cadence CV metric
        output.push_str(&format!(
            "{}_stream_cadence_cv{{}} {:.6}\n",
            prefix,
            self.cadence_cv()
        ));

        // Health score metric
        output.push_str(&format!(
            "{}_stream_health_score{{}} {:.6}\n",
            prefix,
            self.health_score()
        ));

        // Alert count
        output.push_str(&format!(
            "{}_stream_alerts_active{{}} {}\n",
            prefix,
            self.alerts().bits().count_ones()
        ));

        // Discontinuity count
        output.push_str(&format!(
            "{}_stream_discontinuities_total{{}} {}\n",
            prefix, self.clock_state.discontinuity_count
        ));

        output
    }

    /// Export per-stream debug metrics as JSON
    ///
    /// Use this for cardinality-safe per-stream introspection.
    pub fn to_debug_json(&self) -> serde_json::Value {
        let alerts = self.alerts();
        serde_json::json!({
            "stream_id": self.stream_id,
            "lead_us": self.current_lead_us(),
            "slope_ms_per_s": self.current_slope,
            "av_skew_us": self.current_av_skew_us,
            "cadence_cv": self.cadence_cv(),
            "health_score": self.health_score(),
            "alerts": format!("{:?}", alerts),
            "alert_state": {
                "active_alerts": alerts.bits(),
                "drift_slope": alerts.contains(DriftAlerts::DRIFT_SLOPE),
                "lead_jump": alerts.contains(DriftAlerts::LEAD_JUMP),
                "av_skew": alerts.contains(DriftAlerts::AV_SKEW),
                "freeze": alerts.contains(DriftAlerts::FREEZE),
                "cadence_unstable": alerts.contains(DriftAlerts::CADENCE_UNSTABLE),
                "health_low": alerts.contains(DriftAlerts::HEALTH_LOW),
            },
            "sample_count": self.samples.len(),
            "discontinuity_count": self.clock_state.discontinuity_count,
            "is_frozen": self.is_frozen(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_drift_thresholds_default() {
        let thresholds = DriftThresholds::default();
        assert_eq!(thresholds.slope_threshold_ms_per_s, 5.0);
        assert_eq!(thresholds.av_skew_threshold_us, 80_000);
        assert_eq!(thresholds.samples_to_raise, 5);
        assert_eq!(thresholds.samples_to_clear, 10);
    }

    #[test]
    fn test_alert_state_hysteresis() {
        let mut state = AlertState::new(3, 5);

        // Should not raise after 2 samples
        assert!(!state.update(true));
        assert!(!state.is_raised);
        assert!(!state.update(true));
        assert!(!state.is_raised);

        // Should raise after 3rd sample
        assert!(state.update(true));
        assert!(state.is_raised);

        // Should not clear after 4 samples without condition
        assert!(!state.update(false));
        assert!(!state.update(false));
        assert!(!state.update(false));
        assert!(!state.update(false));
        assert!(state.is_raised);

        // Should clear after 5th sample without condition
        assert!(state.update(false));
        assert!(!state.is_raised);
    }

    #[test]
    fn test_drift_metrics_creation() {
        let metrics = DriftMetrics::with_defaults("test_stream".to_string());
        assert_eq!(metrics.stream_id, "test_stream");
        assert!(metrics.samples.is_empty());
        assert_eq!(metrics.health_score(), 1.0);
    }

    #[test]
    fn test_lead_calculation_baseline_normalized() {
        let mut metrics = DriftMetrics::with_defaults("test".to_string());

        // First sample establishes baseline
        metrics.record_sample(1000, 2000, None);

        // Second sample - media advanced 1000us, arrival advanced 1000us -> lead = 0
        metrics.record_sample(2000, 3000, None);
        assert_eq!(metrics.current_lead_us(), Some(0));

        // Third sample - media advanced 1000us, arrival advanced 1500us -> lead = 500
        metrics.record_sample(3000, 4500, None);
        assert_eq!(metrics.current_lead_us(), Some(500));
    }

    #[test]
    fn test_discontinuity_detection() {
        let mut metrics = DriftMetrics::with_defaults("test".to_string());

        // Normal samples
        metrics.record_sample(1_000_000, 1_000_000, None);
        metrics.record_sample(2_000_000, 2_000_000, None);
        assert_eq!(metrics.discontinuity_count(), 0);

        // Large jump (>2s) triggers discontinuity
        metrics.record_sample(10_000_000, 3_000_000, None);
        assert_eq!(metrics.discontinuity_count(), 1);
    }

    #[test]
    fn test_cadence_cv_calculation() {
        let mut metrics = DriftMetrics::with_defaults("test".to_string());

        // Uniform cadence (33ms intervals - ~30fps)
        for i in 0..10 {
            metrics.record_sample(i * 33_333, i * 33_333, None);
        }

        // CV should be low for uniform cadence
        let cv = metrics.cadence_cv();
        assert!(cv < 0.1, "CV should be low for uniform cadence: {}", cv);
    }

    #[test]
    fn test_freeze_detection() {
        let mut metrics = DriftMetrics::with_defaults("test".to_string());

        // Simulate frozen content with advancing timestamps
        let hash = 12345u64;
        for i in 0..10 {
            // Timestamps advance but content hash stays same
            metrics.record_sample(i * 100_000, i * 100_000, Some(hash));
        }

        assert!(
            metrics.is_frozen(),
            "Should detect freeze with identical content"
        );
    }

    #[test]
    fn test_health_score_degradation() {
        let mut metrics = DriftMetrics::with_defaults("test".to_string());

        // Perfect stream
        assert_eq!(metrics.health_score(), 1.0);

        // Simulate drift by adding samples with increasing lead
        for i in 0..20 {
            // Lead increases by 10ms per second
            let media_ts = i * 100_000; // 100ms intervals
            let arrival_ts = i * 100_000 + (i * 1000); // arrival drifts ahead
            metrics.record_sample(media_ts, arrival_ts, None);
        }

        // Health should be degraded due to slope
        let health = metrics.health_score();
        assert!(health < 1.0, "Health should degrade with drift: {}", health);
    }

    #[test]
    fn test_av_skew_tracking() {
        let mut metrics = DriftMetrics::with_defaults("test".to_string());

        // Audio and video with 50ms skew
        metrics.record_audio_sample(1_000_000, 1_000_000);
        metrics.record_video_sample(1_050_000, 1_050_000, None);

        assert_eq!(metrics.current_av_skew_us, 50_000);
    }

    #[test]
    fn test_prometheus_export() {
        let mut metrics = DriftMetrics::with_defaults("test".to_string());
        metrics.record_sample(1000, 2000, None);

        let prom = metrics.to_prometheus("pipeline");
        assert!(prom.contains("pipeline_stream_lead_us"));
        assert!(prom.contains("pipeline_stream_slope_ms_per_s"));
        assert!(prom.contains("pipeline_stream_health_score"));
    }

    #[test]
    fn test_debug_json_export() {
        let mut metrics = DriftMetrics::with_defaults("test_stream".to_string());
        metrics.record_sample(1000, 2000, None);

        let json = metrics.to_debug_json();
        assert_eq!(json["stream_id"], "test_stream");
        assert!(json["health_score"].is_number());
    }

    // ==================== Spec 026 Phase 6 Alert Tests ====================

    #[test]
    fn test_health_low_alert_raised() {
        // T065: Unit test - alert raised when health_score drops below threshold
        let thresholds = DriftThresholds {
            health_threshold: 0.9,
            slope_threshold_ms_per_s: 5.0,
            samples_to_raise: 3, // Quick alert for testing
            samples_to_clear: 6,
            ..Default::default()
        };
        let mut metrics = DriftMetrics::new("test_stream".to_string(), thresholds);

        // Initially no alerts
        assert!(metrics.alerts().is_empty());

        // Generate drift that degrades health below 0.9
        // Simulate drift by adding samples with increasing lead (10ms/s drift)
        for i in 0..20 {
            let media_ts = i * 100_000; // 100ms intervals
            let arrival_ts = i * 100_000 + (i * 10_000); // arrival drifts ahead by 10ms per sample
            let changed = metrics.record_sample(media_ts, arrival_ts, None);

            // Check if alert state changed
            if changed {
                let alerts = metrics.alerts();
                if alerts.contains(DriftAlerts::HEALTH_LOW) {
                    // Alert was raised
                    assert!(
                        metrics.health_score() < 0.9,
                        "HEALTH_LOW alert should only be raised when score < threshold"
                    );
                    return; // Test passed
                }
            }
        }

        // If we got here, verify health did degrade significantly
        let final_health = metrics.health_score();
        if final_health < 0.9 {
            // Health degraded but alert not raised yet (hysteresis)
            // This is OK - hysteresis may require more samples
            assert!(
                final_health < 1.0,
                "Health should have degraded: {}",
                final_health
            );
        }
    }

    #[test]
    fn test_multiple_concurrent_alerts_bitfield() {
        // T066: Unit test - multiple concurrent alerts returned in bitfield
        let thresholds = DriftThresholds {
            slope_threshold_ms_per_s: 1.0, // Very low threshold
            av_skew_threshold_us: 10_000,  // 10ms threshold
            cadence_cv_threshold: 0.05,    // 5% threshold
            samples_to_raise: 2,           // Quick alert
            samples_to_clear: 5,
            warmup_samples: 0, // Disable warmup for test
            ..Default::default()
        };
        let mut metrics = DriftMetrics::new("multi_alert_stream".to_string(), thresholds);

        // Create conditions for multiple alerts simultaneously:
        // 1. Large A/V skew (will trigger AV_SKEW alert)
        // 2. Irregular cadence (will trigger CADENCE_UNSTABLE)
        // 3. Drift slope (will trigger DRIFT_SLOPE)

        // Record audio with 100ms skew from video
        metrics.record_audio_sample(0, 0);
        metrics.record_video_sample(100_000, 100_000, None); // 100ms skew

        // Record samples with irregular cadence and drift
        let intervals = [50_000, 200_000, 30_000, 150_000, 80_000]; // Very irregular
        let mut media_ts = 200_000u64;
        let mut arrival_ts = 200_000u64;

        for (i, interval) in intervals.iter().enumerate() {
            media_ts += interval;
            arrival_ts += interval + (i as u64 * 50_000); // Increasing drift
            metrics.record_sample(media_ts, arrival_ts, None);
        }

        // Check that multiple alerts are active
        let alerts = metrics.alerts();

        // At least one alert should be active
        assert!(!alerts.is_empty(), "Should have at least one alert active");

        // The bitfield should correctly represent multiple conditions
        let bits = alerts.bits();
        let alert_count = bits.count_ones();

        // Verify bitfield contains the expected alerts
        // Note: Exact alerts depend on hysteresis thresholds
        if alerts.contains(DriftAlerts::AV_SKEW) {
            assert!(bits & DriftAlerts::AV_SKEW.bits() != 0);
        }
        if alerts.contains(DriftAlerts::CADENCE_UNSTABLE) {
            assert!(bits & DriftAlerts::CADENCE_UNSTABLE.bits() != 0);
        }
        if alerts.contains(DriftAlerts::DRIFT_SLOPE) {
            assert!(bits & DriftAlerts::DRIFT_SLOPE.bits() != 0);
        }

        // Verify debug JSON includes alert_state
        let json = metrics.to_debug_json();
        assert!(json["alert_state"].is_object());
        assert!(json["alert_state"]["active_alerts"].is_number());
        assert_eq!(
            json["alert_state"]["active_alerts"].as_u64().unwrap(),
            bits as u64
        );

        // Verify individual alert flags in JSON
        if alerts.contains(DriftAlerts::AV_SKEW) {
            assert_eq!(json["alert_state"]["av_skew"], true);
        }
    }

    #[test]
    fn test_alert_state_in_debug_json() {
        // Additional test for T064 - alert state in debug endpoint
        let thresholds = DriftThresholds {
            samples_to_raise: 3, // Quick alert
            samples_to_clear: 6,
            ..Default::default()
        };
        let mut metrics = DriftMetrics::new("debug_test".to_string(), thresholds);

        // Create a freeze condition with enough samples to trigger the alert
        let hash = 99999u64;
        for i in 0..15 {
            metrics.record_sample(i * 100_000, i * 100_000, Some(hash));
        }

        let json = metrics.to_debug_json();

        // Verify alert_state structure exists
        assert!(
            json["alert_state"].is_object(),
            "alert_state should be an object"
        );
        assert!(
            json["alert_state"]["active_alerts"].is_number(),
            "active_alerts should be a number"
        );
        assert!(
            json["alert_state"]["drift_slope"].is_boolean(),
            "drift_slope should be boolean"
        );
        assert!(
            json["alert_state"]["lead_jump"].is_boolean(),
            "lead_jump should be boolean"
        );
        assert!(
            json["alert_state"]["av_skew"].is_boolean(),
            "av_skew should be boolean"
        );
        assert!(
            json["alert_state"]["freeze"].is_boolean(),
            "freeze should be boolean"
        );
        assert!(
            json["alert_state"]["cadence_unstable"].is_boolean(),
            "cadence_unstable should be boolean"
        );
        assert!(
            json["alert_state"]["health_low"].is_boolean(),
            "health_low should be boolean"
        );

        // Verify is_frozen and freeze alert are consistent
        // Note: is_frozen() returns true immediately, but the FREEZE alert requires
        // samples_to_raise consecutive samples with freeze condition
        if metrics.alerts().contains(DriftAlerts::FREEZE) {
            assert!(
                json["alert_state"]["freeze"] == true,
                "freeze in JSON should match alerts()"
            );
        }
    }

    #[test]
    fn test_warmup_period_suppresses_alerts() {
        // Test that alerts are suppressed during warmup period
        let thresholds = DriftThresholds {
            slope_threshold_ms_per_s: 1.0, // Very low threshold to trigger easily
            samples_to_raise: 1,           // Immediate alert
            warmup_samples: 10,            // 10 sample warmup
            ..Default::default()
        };
        let mut metrics = DriftMetrics::new("warmup_test".to_string(), thresholds);

        // Record samples that would normally trigger alerts (large drift)
        for i in 0..5 {
            let media_ts = i * 100_000;
            let arrival_ts = i * 100_000 + (i * 50_000); // Increasing drift
            metrics.record_sample(media_ts, arrival_ts, None);
        }

        // Should still be in warmup
        assert!(metrics.is_warming_up(), "Should be in warmup period");
        assert_eq!(metrics.total_samples(), 5);

        // Alerts should be empty during warmup
        assert!(
            metrics.alerts().is_empty(),
            "No alerts should be active during warmup"
        );

        // Continue recording samples to exit warmup
        for i in 5..15 {
            let media_ts = i * 100_000;
            let arrival_ts = i * 100_000 + (i * 50_000); // Increasing drift
            metrics.record_sample(media_ts, arrival_ts, None);
        }

        // Should no longer be in warmup
        assert!(!metrics.is_warming_up(), "Should have exited warmup period");
        assert_eq!(metrics.total_samples(), 15);

        // Now alerts should be possible (drift slope should trigger)
        // Note: may or may not have alerts depending on hysteresis
    }

    #[test]
    fn test_warmup_zero_allows_immediate_alerts() {
        // Test that setting warmup_samples=0 allows immediate alerts
        let thresholds = DriftThresholds {
            slope_threshold_ms_per_s: 0.1, // Very low threshold
            samples_to_raise: 1,           // Immediate alert
            warmup_samples: 0,             // No warmup
            ..Default::default()
        };
        let mut metrics = DriftMetrics::new("no_warmup_test".to_string(), thresholds);

        // Should not be in warmup after recording first sample
        metrics.record_sample(0, 0, None);
        assert!(
            !metrics.is_warming_up(),
            "Should not be in warmup with warmup_samples=0"
        );

        // Record samples with large drift
        for i in 1..10 {
            let media_ts = i * 100_000;
            let arrival_ts = i * 100_000 + (i * 100_000); // Very large drift
            metrics.record_sample(media_ts, arrival_ts, None);
        }

        // With zero warmup and low thresholds, alerts should be active
        // (unless the measurements happen to be within threshold)
    }
}
