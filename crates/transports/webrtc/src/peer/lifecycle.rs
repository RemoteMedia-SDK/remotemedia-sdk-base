//! Peer connection lifecycle management (Phase 8: US5)
//!
//! Provides automatic reconnection with exponential backoff and circuit breaker
//! pattern for robust WebRTC peer connections.

// Public API types - fields and methods used by library consumers, not internally
#![allow(dead_code)]

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Reconnection policy configuration (T170)
///
/// Controls how reconnection attempts are made when a peer connection fails.
#[derive(Debug, Clone)]
pub struct ReconnectionPolicy {
    /// Maximum number of reconnection attempts (default: 5)
    pub max_retries: u32,
    /// Initial backoff delay in milliseconds (default: 1000ms)
    pub backoff_initial_ms: u64,
    /// Maximum backoff delay in milliseconds (default: 30000ms)
    pub backoff_max_ms: u64,
    /// Backoff multiplier (default: 2.0)
    pub backoff_multiplier: f64,
    /// Whether to add jitter to backoff (default: true)
    pub jitter_enabled: bool,
}

impl Default for ReconnectionPolicy {
    fn default() -> Self {
        Self {
            max_retries: 5,
            backoff_initial_ms: 1000,
            backoff_max_ms: 30000,
            backoff_multiplier: 2.0,
            jitter_enabled: true,
        }
    }
}

impl ReconnectionPolicy {
    /// Create a policy with aggressive reconnection (for low-latency scenarios)
    pub fn aggressive() -> Self {
        Self {
            max_retries: 10,
            backoff_initial_ms: 100,
            backoff_max_ms: 5000,
            backoff_multiplier: 1.5,
            jitter_enabled: true,
        }
    }

    /// Create a policy with conservative reconnection (for stable connections)
    pub fn conservative() -> Self {
        Self {
            max_retries: 3,
            backoff_initial_ms: 2000,
            backoff_max_ms: 60000,
            backoff_multiplier: 2.5,
            jitter_enabled: true,
        }
    }

    /// Calculate backoff duration for a given attempt number (T171)
    ///
    /// Uses exponential backoff with optional jitter.
    ///
    /// # Arguments
    /// * `attempt` - Current attempt number (0-indexed)
    ///
    /// # Returns
    /// Duration to wait before next reconnection attempt
    pub fn calculate_backoff(&self, attempt: u32) -> Duration {
        // Calculate exponential backoff
        let backoff_ms =
            (self.backoff_initial_ms as f64) * self.backoff_multiplier.powi(attempt as i32);

        // Clamp to maximum
        let backoff_ms = backoff_ms.min(self.backoff_max_ms as f64);

        // Add jitter (0-25% of backoff)
        let final_ms = if self.jitter_enabled {
            let jitter = rand_jitter(backoff_ms * 0.25);
            backoff_ms + jitter
        } else {
            backoff_ms
        };

        Duration::from_millis(final_ms as u64)
    }

    /// Check if more retries are allowed
    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_retries
    }
}

/// Simple pseudo-random jitter using time-based seed
fn rand_jitter(max: f64) -> f64 {
    let seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as f64;
    (seed % 1000.0) / 1000.0 * max
}

// ============================================================================
// Circuit Breaker Pattern (T175-T178)
// ============================================================================

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed (normal operation)
    Closed,
    /// Circuit is open (blocking all attempts)
    Open,
    /// Circuit is half-open (testing recovery)
    HalfOpen,
}

/// Circuit breaker for preventing repeated failures (T175)
///
/// Implements the circuit breaker pattern to prevent cascading failures
/// during network issues.
pub struct CircuitBreaker {
    /// Current failure count
    failure_count: AtomicU32,
    /// Success count in half-open state
    success_count: AtomicU32,
    /// Failure threshold to open circuit
    failure_threshold: u32,
    /// Success threshold to close circuit from half-open
    success_threshold: u32,
    /// Current circuit state
    state: Arc<RwLock<CircuitState>>,
    /// Timestamp when circuit was opened (microseconds since epoch)
    opened_at: AtomicU64,
    /// Recovery timeout in milliseconds
    recovery_timeout_ms: u64,
    /// Circuit breaker name/label
    name: String,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    ///
    /// # Arguments
    /// * `name` - Label for logging
    /// * `failure_threshold` - Number of failures before opening circuit
    /// * `success_threshold` - Number of successes to close circuit from half-open
    /// * `recovery_timeout_ms` - Time before attempting recovery
    pub fn new(
        name: impl Into<String>,
        failure_threshold: u32,
        success_threshold: u32,
        recovery_timeout_ms: u64,
    ) -> Self {
        Self {
            failure_count: AtomicU32::new(0),
            success_count: AtomicU32::new(0),
            failure_threshold,
            success_threshold,
            state: Arc::new(RwLock::new(CircuitState::Closed)),
            opened_at: AtomicU64::new(0),
            recovery_timeout_ms,
            name: name.into(),
        }
    }

    /// Create a circuit breaker with default settings
    pub fn with_defaults(name: impl Into<String>) -> Self {
        Self::new(name, 5, 2, 30000)
    }

    /// Get current circuit state
    pub async fn state(&self) -> CircuitState {
        // Check if we should transition from Open to HalfOpen
        self.check_recovery_timeout().await;
        *self.state.read().await
    }

    /// Check if the circuit allows requests
    pub async fn is_allowed(&self) -> bool {
        let state = self.state().await;
        matches!(state, CircuitState::Closed | CircuitState::HalfOpen)
    }

    /// Record a failure (T176)
    ///
    /// Increments failure count and opens circuit if threshold is exceeded.
    pub async fn record_failure(&self) {
        let current_state = self.state().await;

        match current_state {
            CircuitState::Closed => {
                let count = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
                debug!(
                    "Circuit '{}': failure recorded ({}/{})",
                    self.name, count, self.failure_threshold
                );

                if count >= self.failure_threshold {
                    self.open_circuit().await;
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in half-open state reopens the circuit
                warn!(
                    "Circuit '{}': failure in half-open state, reopening",
                    self.name
                );
                self.open_circuit().await;
            }
            CircuitState::Open => {
                // Already open, nothing to do
            }
        }
    }

    /// Record a success (T177)
    ///
    /// Resets failure count and closes circuit from half-open state.
    pub async fn record_success(&self) {
        let current_state = self.state().await;

        match current_state {
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::SeqCst);
            }
            CircuitState::HalfOpen => {
                let count = self.success_count.fetch_add(1, Ordering::SeqCst) + 1;
                debug!(
                    "Circuit '{}': success in half-open ({}/{})",
                    self.name, count, self.success_threshold
                );

                if count >= self.success_threshold {
                    self.close_circuit().await;
                }
            }
            CircuitState::Open => {
                // Shouldn't happen - requests blocked when open
                warn!("Circuit '{}': unexpected success while open", self.name);
            }
        }
    }

    /// Open the circuit
    async fn open_circuit(&self) {
        let mut state = self.state.write().await;
        if *state != CircuitState::Open {
            info!(
                "Circuit '{}': opening after {} failures",
                self.name,
                self.failure_count.load(Ordering::SeqCst)
            );
            *state = CircuitState::Open;
            self.opened_at.store(now_micros(), Ordering::SeqCst);
            self.success_count.store(0, Ordering::SeqCst);
        }
    }

    /// Close the circuit
    async fn close_circuit(&self) {
        let mut state = self.state.write().await;
        info!("Circuit '{}': closing after successful recovery", self.name);
        *state = CircuitState::Closed;
        self.failure_count.store(0, Ordering::SeqCst);
        self.success_count.store(0, Ordering::SeqCst);
    }

    /// Check if recovery timeout has elapsed and transition to half-open
    async fn check_recovery_timeout(&self) {
        let state = *self.state.read().await;
        if state != CircuitState::Open {
            return;
        }

        let opened_at = self.opened_at.load(Ordering::SeqCst);
        let elapsed_ms = (now_micros() - opened_at) / 1000;

        if elapsed_ms >= self.recovery_timeout_ms {
            let mut state = self.state.write().await;
            if *state == CircuitState::Open {
                info!(
                    "Circuit '{}': transitioning to half-open after {}ms",
                    self.name, elapsed_ms
                );
                *state = CircuitState::HalfOpen;
                self.success_count.store(0, Ordering::SeqCst);
            }
        }
    }

    /// Reset the circuit breaker to initial state
    pub async fn reset(&self) {
        let mut state = self.state.write().await;
        *state = CircuitState::Closed;
        self.failure_count.store(0, Ordering::SeqCst);
        self.success_count.store(0, Ordering::SeqCst);
        debug!("Circuit '{}': reset to closed state", self.name);
    }

    /// Get failure count
    pub fn failure_count(&self) -> u32 {
        self.failure_count.load(Ordering::SeqCst)
    }
}

/// Get current time in microseconds since epoch
fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

// ============================================================================
// Connection Quality Metrics (T181)
// ============================================================================

/// Connection quality metrics (T181)
///
/// Tracks various network performance metrics for adaptive quality control.
#[derive(Debug, Clone)]
pub struct ConnectionQualityMetrics {
    /// Round-trip latency in milliseconds
    pub latency_ms: f64,
    /// Packet loss rate (0.0 - 1.0)
    pub packet_loss_rate: f64,
    /// Jitter in milliseconds
    pub jitter_ms: f64,
    /// Estimated bandwidth in kbps
    pub bandwidth_kbps: u32,
    /// Current video resolution (width)
    pub video_width: u32,
    /// Current video resolution (height)
    pub video_height: u32,
    /// Video framerate
    pub video_framerate: f32,
    /// Audio bitrate in kbps
    pub audio_bitrate_kbps: u32,
    /// Video bitrate in kbps
    pub video_bitrate_kbps: u32,
    /// Total packets sent
    pub packets_sent: u64,
    /// Total packets received
    pub packets_received: u64,
    /// Total packets lost
    pub packets_lost: u64,
    /// Timestamp when metrics were last updated
    pub updated_at: Instant,
}

impl Default for ConnectionQualityMetrics {
    fn default() -> Self {
        Self {
            latency_ms: 0.0,
            packet_loss_rate: 0.0,
            jitter_ms: 0.0,
            bandwidth_kbps: 0,
            video_width: 0,
            video_height: 0,
            video_framerate: 0.0,
            audio_bitrate_kbps: 0,
            video_bitrate_kbps: 0,
            packets_sent: 0,
            packets_received: 0,
            packets_lost: 0,
            updated_at: Instant::now(),
        }
    }
}

impl ConnectionQualityMetrics {
    /// Create new metrics with current timestamp
    pub fn new() -> Self {
        Self::default()
    }

    /// Calculate quality score (0-100)
    ///
    /// Higher is better. Based on latency, packet loss, and jitter.
    pub fn quality_score(&self) -> u32 {
        let mut score = 100u32;

        // Deduct for latency (ideal < 100ms)
        if self.latency_ms > 100.0 {
            let deduction = ((self.latency_ms - 100.0) / 10.0).min(30.0) as u32;
            score = score.saturating_sub(deduction);
        }

        // Deduct for packet loss (each 1% costs 10 points)
        let loss_deduction = (self.packet_loss_rate * 100.0 * 10.0).min(40.0) as u32;
        score = score.saturating_sub(loss_deduction);

        // Deduct for jitter (ideal < 30ms)
        if self.jitter_ms > 30.0 {
            let deduction = ((self.jitter_ms - 30.0) / 5.0).min(20.0) as u32;
            score = score.saturating_sub(deduction);
        }

        score
    }

    /// Check if connection quality is acceptable
    pub fn is_acceptable(&self) -> bool {
        self.quality_score() >= 50
    }

    /// Check if adaptive bitrate should be reduced
    pub fn should_reduce_bitrate(&self) -> bool {
        // Reduce bitrate if packet loss > 5% or latency > 300ms
        self.packet_loss_rate > 0.05 || self.latency_ms > 300.0
    }

    /// Check if adaptive bitrate can be increased
    pub fn can_increase_bitrate(&self) -> bool {
        // Only increase if packet loss < 1% and latency < 100ms
        self.packet_loss_rate < 0.01 && self.latency_ms < 100.0
    }
}

// ============================================================================
// Reconnection State Machine (T172-T174)
// ============================================================================

/// Reconnection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconnectionState {
    /// No reconnection needed
    Idle,
    /// Waiting for backoff delay
    WaitingForBackoff,
    /// Attempting reconnection
    Reconnecting,
    /// Reconnection succeeded
    Succeeded,
    /// Reconnection failed (gave up)
    Failed,
}

/// Reconnection manager for a peer connection
pub struct ReconnectionManager {
    /// Peer ID being reconnected
    peer_id: String,
    /// Reconnection policy
    policy: ReconnectionPolicy,
    /// Circuit breaker
    circuit_breaker: Arc<CircuitBreaker>,
    /// Current state
    state: Arc<RwLock<ReconnectionState>>,
    /// Current attempt number
    current_attempt: AtomicU32,
    /// Last reconnection attempt timestamp
    last_attempt: Arc<RwLock<Option<Instant>>>,
}

impl ReconnectionManager {
    /// Create a new reconnection manager
    pub fn new(peer_id: impl Into<String>, policy: ReconnectionPolicy) -> Self {
        let peer_id = peer_id.into();
        let circuit_breaker = Arc::new(CircuitBreaker::with_defaults(format!(
            "peer-{}-reconnect",
            peer_id
        )));

        Self {
            peer_id,
            policy,
            circuit_breaker,
            state: Arc::new(RwLock::new(ReconnectionState::Idle)),
            current_attempt: AtomicU32::new(0),
            last_attempt: Arc::new(RwLock::new(None)),
        }
    }

    /// Get current state
    pub async fn state(&self) -> ReconnectionState {
        *self.state.read().await
    }

    /// Check if reconnection should be attempted
    pub async fn should_attempt_reconnect(&self) -> bool {
        // Check circuit breaker
        if !self.circuit_breaker.is_allowed().await {
            debug!(
                "Reconnection to peer {} blocked by circuit breaker",
                self.peer_id
            );
            return false;
        }

        // Check retry limit
        let attempt = self.current_attempt.load(Ordering::SeqCst);
        if !self.policy.should_retry(attempt) {
            debug!(
                "Reconnection to peer {} exceeded max retries ({})",
                self.peer_id, attempt
            );
            return false;
        }

        true
    }

    /// Start reconnection process
    pub async fn start_reconnection(&self) {
        let attempt = self.current_attempt.fetch_add(1, Ordering::SeqCst);
        let backoff = self.policy.calculate_backoff(attempt);

        info!(
            "Starting reconnection to peer {} (attempt {}/{}, backoff {:?})",
            self.peer_id,
            attempt + 1,
            self.policy.max_retries,
            backoff
        );

        *self.state.write().await = ReconnectionState::WaitingForBackoff;
        *self.last_attempt.write().await = Some(Instant::now());

        // Wait for backoff
        tokio::time::sleep(backoff).await;

        *self.state.write().await = ReconnectionState::Reconnecting;
    }

    /// Report reconnection success
    pub async fn report_success(&self) {
        info!("Reconnection to peer {} succeeded", self.peer_id);
        self.circuit_breaker.record_success().await;
        self.current_attempt.store(0, Ordering::SeqCst);
        *self.state.write().await = ReconnectionState::Succeeded;
    }

    /// Report reconnection failure
    pub async fn report_failure(&self) {
        let attempt = self.current_attempt.load(Ordering::SeqCst);
        warn!(
            "Reconnection to peer {} failed (attempt {})",
            self.peer_id, attempt
        );

        self.circuit_breaker.record_failure().await;

        if !self.policy.should_retry(attempt) {
            *self.state.write().await = ReconnectionState::Failed;
        } else {
            *self.state.write().await = ReconnectionState::Idle;
        }
    }

    /// Reset manager state
    pub async fn reset(&self) {
        self.current_attempt.store(0, Ordering::SeqCst);
        self.circuit_breaker.reset().await;
        *self.state.write().await = ReconnectionState::Idle;
        *self.last_attempt.write().await = None;
    }

    /// Get current attempt number
    pub fn current_attempt(&self) -> u32 {
        self.current_attempt.load(Ordering::SeqCst)
    }

    /// Get the circuit breaker
    pub fn circuit_breaker(&self) -> &Arc<CircuitBreaker> {
        &self.circuit_breaker
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reconnection_policy_default() {
        let policy = ReconnectionPolicy::default();
        assert_eq!(policy.max_retries, 5);
        assert_eq!(policy.backoff_initial_ms, 1000);
        assert_eq!(policy.backoff_max_ms, 30000);
    }

    #[test]
    fn test_exponential_backoff() {
        let mut policy = ReconnectionPolicy::default();
        policy.jitter_enabled = false; // Disable jitter for predictable tests

        let b0 = policy.calculate_backoff(0);
        let b1 = policy.calculate_backoff(1);
        let b2 = policy.calculate_backoff(2);

        // Should increase exponentially
        assert_eq!(b0, Duration::from_millis(1000));
        assert_eq!(b1, Duration::from_millis(2000));
        assert_eq!(b2, Duration::from_millis(4000));
    }

    #[test]
    fn test_backoff_max_clamp() {
        let mut policy = ReconnectionPolicy::default();
        policy.jitter_enabled = false;
        policy.backoff_max_ms = 5000;

        // After many attempts, should clamp to max
        let b10 = policy.calculate_backoff(10);
        assert_eq!(b10, Duration::from_millis(5000));
    }

    #[test]
    fn test_should_retry() {
        let policy = ReconnectionPolicy {
            max_retries: 3,
            ..Default::default()
        };

        assert!(policy.should_retry(0));
        assert!(policy.should_retry(2));
        assert!(!policy.should_retry(3));
        assert!(!policy.should_retry(10));
    }

    #[tokio::test]
    async fn test_circuit_breaker_closed() {
        let cb = CircuitBreaker::new("test", 3, 2, 1000);

        assert_eq!(cb.state().await, CircuitState::Closed);
        assert!(cb.is_allowed().await);
    }

    #[tokio::test]
    async fn test_circuit_breaker_opens_after_failures() {
        let cb = CircuitBreaker::new("test", 3, 2, 1000);

        // Record failures
        cb.record_failure().await;
        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Closed);

        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Open);
        assert!(!cb.is_allowed().await);
    }

    #[tokio::test]
    async fn test_circuit_breaker_success_resets_failures() {
        let cb = CircuitBreaker::new("test", 3, 2, 1000);

        cb.record_failure().await;
        cb.record_failure().await;
        assert_eq!(cb.failure_count(), 2);

        cb.record_success().await;
        assert_eq!(cb.failure_count(), 0);
    }

    #[tokio::test]
    async fn test_circuit_breaker_reset() {
        let cb = CircuitBreaker::new("test", 3, 2, 1000);

        cb.record_failure().await;
        cb.record_failure().await;
        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Open);

        cb.reset().await;
        assert_eq!(cb.state().await, CircuitState::Closed);
        assert_eq!(cb.failure_count(), 0);
    }

    #[test]
    fn test_connection_quality_metrics_new() {
        let metrics = ConnectionQualityMetrics::new();
        assert_eq!(metrics.latency_ms, 0.0);
        assert_eq!(metrics.packet_loss_rate, 0.0);
    }

    #[test]
    fn test_quality_score_perfect() {
        let metrics = ConnectionQualityMetrics {
            latency_ms: 50.0,
            packet_loss_rate: 0.0,
            jitter_ms: 10.0,
            ..Default::default()
        };

        assert_eq!(metrics.quality_score(), 100);
    }

    #[test]
    fn test_quality_score_poor() {
        let metrics = ConnectionQualityMetrics {
            latency_ms: 500.0,
            packet_loss_rate: 0.1, // 10% loss
            jitter_ms: 100.0,
            ..Default::default()
        };

        // Should be significantly reduced
        let score = metrics.quality_score();
        assert!(score < 50);
    }

    #[test]
    fn test_should_reduce_bitrate() {
        let high_loss = ConnectionQualityMetrics {
            packet_loss_rate: 0.1,
            ..Default::default()
        };
        assert!(high_loss.should_reduce_bitrate());

        let high_latency = ConnectionQualityMetrics {
            latency_ms: 500.0,
            ..Default::default()
        };
        assert!(high_latency.should_reduce_bitrate());

        let good = ConnectionQualityMetrics {
            latency_ms: 50.0,
            packet_loss_rate: 0.01,
            ..Default::default()
        };
        assert!(!good.should_reduce_bitrate());
    }

    #[tokio::test]
    async fn test_reconnection_manager_creation() {
        let manager = ReconnectionManager::new("peer-1", ReconnectionPolicy::default());

        assert_eq!(manager.state().await, ReconnectionState::Idle);
        assert_eq!(manager.current_attempt(), 0);
    }

    #[tokio::test]
    async fn test_reconnection_manager_should_attempt() {
        let manager = ReconnectionManager::new("peer-1", ReconnectionPolicy::default());

        assert!(manager.should_attempt_reconnect().await);
    }
}
