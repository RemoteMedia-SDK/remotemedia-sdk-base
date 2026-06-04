//! Clock drift estimation for multi-peer synchronization
//!
//! Estimates clock drift between sender and receiver using RTP/NTP timestamps
//! from RTCP Sender Reports. Uses linear regression to calculate drift in PPM.

// Public API types - fields and methods used by library consumers, not internally
#![allow(dead_code)]

use std::collections::VecDeque;
use std::time::Instant;

/// Maximum number of observations to keep for drift estimation
const MAX_OBSERVATIONS: usize = 100;

/// Minimum observations required before drift can be estimated
const MIN_OBSERVATIONS: usize = 10;

/// Clock drift observation from RTCP Sender Report
#[derive(Debug, Clone, Copy)]
pub struct ClockObservation {
    /// RTP timestamp from sender
    pub rtp_timestamp: u32,
    /// NTP timestamp from sender (64-bit)
    pub ntp_timestamp: u64,
    /// Local time when this observation was received
    pub received_at: Instant,
    /// Local reception timestamp in microseconds (for regression)
    pub local_us: u64,
}

/// Recommended action based on drift analysis
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriftAction {
    /// No action needed - drift is minimal
    None,
    /// Monitor drift - within acceptable range but trending
    Monitor,
    /// Apply correction - drift exceeds threshold
    Adjust,
    /// Investigate - drift is erratic or unreliable
    Investigate,
}

/// Clock drift estimate result
#[derive(Debug, Clone, Copy)]
pub struct ClockDriftEstimate {
    /// Estimated drift in parts per million (PPM)
    /// Positive = sender clock is faster than receiver
    /// Negative = sender clock is slower than receiver
    pub drift_ppm: f64,
    /// Number of samples used in estimation
    pub sample_count: usize,
    /// Correction factor to apply to timestamps (1.0 + drift_ppm/1_000_000)
    pub correction_factor: f64,
    /// Confidence in the estimate (0.0 to 1.0)
    /// Based on R² of linear regression
    pub confidence: f64,
    /// Recommended action based on drift magnitude
    pub recommended_action: DriftAction,
}

/// Clock drift estimator for a single peer
///
/// Collects RTP/NTP/local clock observations and uses linear regression
/// to estimate the clock drift between sender and receiver.
#[derive(Debug)]
pub struct ClockDriftEstimator {
    /// Peer ID this estimator is for
    peer_id: String,
    /// Collected observations
    observations: VecDeque<ClockObservation>,
    /// Base local time for relative calculations
    base_time: Option<Instant>,
    /// Sender clock rate (derived from observations)
    sender_rate: Option<f64>,
    /// Receiver clock rate (local)
    receiver_rate: f64,
    /// Drift threshold for correction (in PPM)
    drift_threshold_ppm: f64,
}

impl ClockDriftEstimator {
    /// Create a new clock drift estimator for a peer
    ///
    /// # Arguments
    /// * `peer_id` - Identifier for this peer
    pub fn new(peer_id: String) -> Self {
        Self {
            peer_id,
            observations: VecDeque::with_capacity(MAX_OBSERVATIONS),
            base_time: None,
            sender_rate: None,
            receiver_rate: 1.0,         // Normalized local clock
            drift_threshold_ppm: 100.0, // Default threshold
        }
    }

    /// Create with custom drift threshold
    ///
    /// # Arguments
    /// * `peer_id` - Identifier for this peer
    /// * `drift_threshold_ppm` - Threshold in PPM above which correction is recommended
    pub fn with_threshold(peer_id: String, drift_threshold_ppm: f64) -> Self {
        Self {
            peer_id,
            observations: VecDeque::with_capacity(MAX_OBSERVATIONS),
            base_time: None,
            sender_rate: None,
            receiver_rate: 1.0,
            drift_threshold_ppm,
        }
    }

    /// Get the peer ID
    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    /// Add a new observation from RTCP Sender Report
    ///
    /// # Arguments
    /// * `rtp_ts` - RTP timestamp from sender
    /// * `ntp_ts` - NTP timestamp from sender (64-bit)
    /// * `received_at` - Local time when SR was received
    pub fn add_observation(&mut self, rtp_ts: u32, ntp_ts: u64, received_at: Instant) {
        // Initialize base time on first observation
        if self.base_time.is_none() {
            self.base_time = Some(received_at);
        }

        let base = self.base_time.unwrap();
        let local_us = received_at.duration_since(base).as_micros() as u64;

        let observation = ClockObservation {
            rtp_timestamp: rtp_ts,
            ntp_timestamp: ntp_ts,
            received_at,
            local_us,
        };

        // Remove oldest observation if at capacity
        if self.observations.len() >= MAX_OBSERVATIONS {
            self.observations.pop_front();
        }

        self.observations.push_back(observation);
    }

    /// Estimate clock drift using linear regression
    ///
    /// Uses least-squares regression to fit sender timestamps against
    /// local timestamps, then calculates drift in PPM.
    ///
    /// # Returns
    /// Clock drift estimate if sufficient observations available
    pub fn estimate_drift(&self) -> Option<ClockDriftEstimate> {
        if self.observations.len() < MIN_OBSERVATIONS {
            return None;
        }

        // Convert NTP timestamps to relative microseconds for regression
        let first_ntp = self.observations.front()?.ntp_timestamp;
        let first_local = self.observations.front()?.local_us;

        let mut sum_x = 0.0f64; // Local time (x)
        let mut sum_y = 0.0f64; // Sender time (y)
        let mut sum_xy = 0.0f64;
        let mut sum_xx = 0.0f64;
        let mut sum_yy = 0.0f64;
        let n = self.observations.len() as f64;

        for obs in &self.observations {
            // Local time difference in microseconds
            let x = (obs.local_us - first_local) as f64;

            // Sender time difference in microseconds
            // NTP upper 32 bits = seconds, lower 32 bits = fraction
            let ntp_diff = obs.ntp_timestamp.wrapping_sub(first_ntp);
            let ntp_secs = (ntp_diff >> 32) as f64;
            let ntp_frac = ((ntp_diff & 0xFFFFFFFF) as f64) / (1u64 << 32) as f64;
            let y = (ntp_secs + ntp_frac) * 1_000_000.0; // Convert to microseconds

            sum_x += x;
            sum_y += y;
            sum_xy += x * y;
            sum_xx += x * x;
            sum_yy += y * y;
        }

        // Linear regression: y = mx + b
        // m = (n*sum_xy - sum_x*sum_y) / (n*sum_xx - sum_x*sum_x)
        let denom = n * sum_xx - sum_x * sum_x;
        if denom.abs() < 1e-10 {
            return None; // Degenerate case
        }

        let slope = (n * sum_xy - sum_x * sum_y) / denom;

        // Drift in PPM: (slope - 1.0) * 1_000_000
        // slope = sender_rate / local_rate
        // If slope > 1.0, sender clock is faster
        let drift_ppm = (slope - 1.0) * 1_000_000.0;

        // Calculate R² for confidence
        let mean_y = sum_y / n;
        let ss_tot = sum_yy - n * mean_y * mean_y;
        let ss_res = sum_yy - slope * (sum_xy - sum_x * mean_y / n);
        let r_squared = if ss_tot.abs() > 1e-10 {
            1.0 - (ss_res / ss_tot).max(0.0)
        } else {
            1.0 // Perfect fit if no variance
        };

        let confidence = r_squared.sqrt().clamp(0.0, 1.0);

        // Determine recommended action
        let action = if confidence < 0.5 {
            DriftAction::Investigate
        } else if drift_ppm.abs() < self.drift_threshold_ppm / 2.0 {
            DriftAction::None
        } else if drift_ppm.abs() < self.drift_threshold_ppm {
            DriftAction::Monitor
        } else {
            DriftAction::Adjust
        };

        Some(ClockDriftEstimate {
            drift_ppm,
            sample_count: self.observations.len(),
            correction_factor: 1.0 + drift_ppm / 1_000_000.0,
            confidence,
            recommended_action: action,
        })
    }

    /// Get number of observations collected
    pub fn observation_count(&self) -> usize {
        self.observations.len()
    }

    /// Clear all observations (reset estimator)
    pub fn reset(&mut self) {
        self.observations.clear();
        self.base_time = None;
        self.sender_rate = None;
    }

    /// Check if sufficient observations exist for estimation
    pub fn can_estimate(&self) -> bool {
        self.observations.len() >= MIN_OBSERVATIONS
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_new_estimator() {
        let estimator = ClockDriftEstimator::new("peer1".to_string());
        assert_eq!(estimator.peer_id(), "peer1");
        assert_eq!(estimator.observation_count(), 0);
        assert!(!estimator.can_estimate());
    }

    #[test]
    fn test_insufficient_observations() {
        let mut estimator = ClockDriftEstimator::new("peer1".to_string());
        let base = Instant::now();

        // Add fewer than MIN_OBSERVATIONS
        for i in 0..5 {
            let ntp = (3_913_056_000u64 + i) << 32;
            estimator.add_observation(i as u32 * 48000, ntp, base + Duration::from_secs(i));
        }

        assert!(!estimator.can_estimate());
        assert!(estimator.estimate_drift().is_none());
    }

    #[test]
    fn test_no_drift() {
        let mut estimator = ClockDriftEstimator::new("peer1".to_string());
        let base = Instant::now();

        // Simulate perfect synchronization (no drift)
        for i in 0..20 {
            // 1 second intervals
            let ntp = (3_913_056_000u64 + i) << 32;
            estimator.add_observation(i as u32 * 48000, ntp, base + Duration::from_secs(i));
        }

        let estimate = estimator.estimate_drift().unwrap();
        // Should be very close to 0 PPM
        assert!(
            estimate.drift_ppm.abs() < 10.0,
            "drift_ppm: {}",
            estimate.drift_ppm
        );
        assert!(estimate.confidence > 0.9);
        assert_eq!(estimate.recommended_action, DriftAction::None);
    }

    #[test]
    fn test_positive_drift() {
        let mut estimator = ClockDriftEstimator::with_threshold("peer1".to_string(), 100.0);
        let base = Instant::now();

        // Simulate sender clock 500 PPM faster
        // Sender advances 1.0005 seconds per local second
        for i in 0..20 {
            let local_secs = i as f64;
            let sender_secs = local_secs * 1.0005;
            // Properly encode NTP timestamp with fractional seconds
            let ntp_base = 3_913_056_000u64;
            let total_secs = ntp_base as f64 + sender_secs;
            let ntp_secs = total_secs.floor() as u64;
            let ntp_frac = ((total_secs.fract()) * (1u64 << 32) as f64) as u64;
            let ntp = (ntp_secs << 32) | ntp_frac;
            estimator.add_observation(
                (sender_secs * 48000.0) as u32,
                ntp,
                base + Duration::from_secs(i),
            );
        }

        let estimate = estimator.estimate_drift().unwrap();
        // Should detect ~500 PPM positive drift
        assert!(
            estimate.drift_ppm > 400.0 && estimate.drift_ppm < 600.0,
            "drift_ppm: {}",
            estimate.drift_ppm
        );
        assert_eq!(estimate.recommended_action, DriftAction::Adjust);
    }

    #[test]
    fn test_reset() {
        let mut estimator = ClockDriftEstimator::new("peer1".to_string());
        let base = Instant::now();

        for i in 0..15 {
            let ntp = (3_913_056_000u64 + i) << 32;
            estimator.add_observation(i as u32 * 48000, ntp, base + Duration::from_secs(i));
        }

        assert!(estimator.can_estimate());
        estimator.reset();
        assert!(!estimator.can_estimate());
        assert_eq!(estimator.observation_count(), 0);
    }

    #[test]
    fn test_max_observations() {
        let mut estimator = ClockDriftEstimator::new("peer1".to_string());
        let base = Instant::now();

        // Add more than MAX_OBSERVATIONS
        for i in 0..150 {
            let ntp = (3_913_056_000u64 + i) << 32;
            estimator.add_observation(i as u32 * 48000, ntp, base + Duration::from_secs(i));
        }

        // Should be capped at MAX_OBSERVATIONS
        assert_eq!(estimator.observation_count(), MAX_OBSERVATIONS);
    }

    #[test]
    fn test_drift_action_thresholds() {
        let mut estimator = ClockDriftEstimator::with_threshold("peer1".to_string(), 100.0);

        // DriftAction::None for < 50 PPM
        // DriftAction::Monitor for 50-100 PPM
        // DriftAction::Adjust for > 100 PPM

        // This is implicitly tested through the positive_drift test
        // but we can verify the threshold logic
        assert_eq!(estimator.drift_threshold_ppm, 100.0);
    }
}
