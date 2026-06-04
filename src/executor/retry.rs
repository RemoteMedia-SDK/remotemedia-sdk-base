//! Retry policy and execution
//!
//! Implements exponential backoff and retry logic for failed operations.

use crate::executor::error::ExecutionErrorExt;
use crate::{Error, Result};
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;

/// Retry policy for failed operations
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RetryPolicy {
    /// No retries
    None,

    /// Fixed number of retry attempts with constant delay
    Fixed {
        /// Number of retry attempts
        attempts: usize,
        /// Delay between retries
        delay: Duration,
    },

    /// Exponential backoff retries
    Exponential {
        /// Base delay for first retry
        base_delay: Duration,
        /// Maximum delay between retries
        max_delay: Duration,
        /// Maximum number of attempts
        max_attempts: usize,
        /// Backoff multiplier (typically 2.0)
        multiplier: f64,
    },
}

impl RetryPolicy {
    /// Create a fixed retry policy
    pub fn fixed(attempts: usize, delay: Duration) -> Self {
        RetryPolicy::Fixed { attempts, delay }
    }

    /// Create an exponential backoff policy
    pub fn exponential(max_attempts: usize) -> Self {
        RetryPolicy::Exponential {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            max_attempts,
            multiplier: 2.0,
        }
    }

    /// Get delay for a specific attempt number (0-indexed)
    pub fn delay_for_attempt(&self, attempt: usize) -> Option<Duration> {
        match self {
            RetryPolicy::None => None,
            RetryPolicy::Fixed { attempts, delay } => {
                if attempt < *attempts {
                    Some(*delay)
                } else {
                    None
                }
            }
            RetryPolicy::Exponential {
                base_delay,
                max_delay,
                max_attempts,
                multiplier,
            } => {
                if attempt >= *max_attempts {
                    return None;
                }

                let delay_ms = (base_delay.as_millis() as f64) * multiplier.powi(attempt as i32);
                let delay = Duration::from_millis(delay_ms as u64);

                Some(delay.min(*max_delay))
            }
        }
    }

    /// Get maximum number of attempts
    pub fn max_attempts(&self) -> usize {
        match self {
            RetryPolicy::None => 0,
            RetryPolicy::Fixed { attempts, .. } => *attempts,
            RetryPolicy::Exponential { max_attempts, .. } => *max_attempts,
        }
    }
}

impl Default for RetryPolicy {
    /// Default retry policy: 3 attempts with exponential backoff (100/200/400ms)
    fn default() -> Self {
        RetryPolicy::Exponential {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(400),
            max_attempts: 3,
            multiplier: 2.0,
        }
    }
}

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed - normal operation, requests are allowed
    Closed,
    /// Circuit is open - requests are blocked until reset timeout
    Open,
    /// Circuit is half-open - allowing a test request to see if service recovered
    HalfOpen,
}

/// Circuit breaker to prevent cascading failures (spec 026)
///
/// Tracks consecutive failures and "trips" (opens) after a threshold,
/// preventing further execution attempts until the reset timeout expires.
///
/// # States
///
/// - **Closed**: Normal operation, requests flow through
/// - **Open**: Requests blocked, waiting for reset_timeout to expire
/// - **Half-Open**: One test request allowed; success → Closed, failure → Open
///
/// # Example
///
/// ```
/// use remotemedia_core::executor::retry::CircuitBreaker;
/// use std::time::Duration;
///
/// let mut cb = CircuitBreaker::with_timeout(3, Duration::from_secs(30));
///
/// // Record failures
/// cb.record_failure();
/// cb.record_failure();
/// cb.record_failure(); // Circuit opens
///
/// assert!(cb.is_open());
///
/// // After reset_timeout, circuit becomes half-open
/// // Next success closes it, next failure re-opens it
/// ```
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    /// Maximum consecutive failures before tripping
    failure_threshold: usize,
    /// Current count of consecutive failures
    failure_count: usize,
    /// Current circuit state
    state: CircuitState,
    /// Time when the circuit last opened (for timeout-based reset)
    last_failure_time: Option<std::time::Instant>,
    /// Duration after which an open circuit becomes half-open
    reset_timeout: Duration,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the given failure threshold
    ///
    /// Uses default reset timeout of 30 seconds
    pub fn new(failure_threshold: usize) -> Self {
        Self::with_timeout(failure_threshold, Duration::from_secs(30))
    }

    /// Create a circuit breaker with custom failure threshold and reset timeout
    pub fn with_timeout(failure_threshold: usize, reset_timeout: Duration) -> Self {
        Self {
            failure_threshold,
            failure_count: 0,
            state: CircuitState::Closed,
            last_failure_time: None,
            reset_timeout,
        }
    }

    /// Record a successful execution
    ///
    /// - If closed: resets failure count
    /// - If half-open: transitions to closed
    pub fn record_success(&mut self) {
        self.failure_count = 0;
        self.state = CircuitState::Closed;
        self.last_failure_time = None;
    }

    /// Record a failed execution
    ///
    /// - If closed: increments failure count, may transition to open
    /// - If half-open: transitions back to open
    pub fn record_failure(&mut self) {
        self.failure_count += 1;
        self.last_failure_time = Some(std::time::Instant::now());

        match self.state {
            CircuitState::Closed => {
                if self.failure_count >= self.failure_threshold {
                    self.state = CircuitState::Open;
                    tracing::warn!(
                        "Circuit breaker opened after {} consecutive failures",
                        self.failure_count
                    );
                }
            }
            CircuitState::HalfOpen => {
                // Test request failed, go back to open
                self.state = CircuitState::Open;
                tracing::warn!("Circuit breaker test request failed, returning to open state");
            }
            CircuitState::Open => {
                // Already open, just update failure time
            }
        }
    }

    /// Check if the circuit is open (preventing execution)
    ///
    /// This also handles the timeout-based transition from Open to HalfOpen.
    /// If the circuit is open but the reset timeout has elapsed, it transitions
    /// to half-open and returns false (allowing one test request).
    pub fn is_open(&mut self) -> bool {
        match self.state {
            CircuitState::Closed => false,
            CircuitState::HalfOpen => false, // Allow test request
            CircuitState::Open => {
                // Check if reset timeout has elapsed
                if let Some(last_failure) = self.last_failure_time {
                    if last_failure.elapsed() >= self.reset_timeout {
                        self.state = CircuitState::HalfOpen;
                        tracing::info!(
                            "Circuit breaker transitioning to half-open after {:?}",
                            self.reset_timeout
                        );
                        return false; // Allow test request
                    }
                }
                true
            }
        }
    }

    /// Check if the circuit is open without modifying state
    ///
    /// Unlike `is_open()`, this does not transition from Open to HalfOpen.
    pub fn is_open_readonly(&self) -> bool {
        match self.state {
            CircuitState::Closed => false,
            CircuitState::HalfOpen => false,
            CircuitState::Open => true,
        }
    }

    /// Reset the circuit breaker to closed state
    pub fn reset(&mut self) {
        self.failure_count = 0;
        self.state = CircuitState::Closed;
        self.last_failure_time = None;
        tracing::info!("Circuit breaker reset");
    }

    /// Get the current circuit state
    pub fn state(&self) -> CircuitState {
        self.state
    }

    /// Get the current number of consecutive failures
    pub fn consecutive_failures(&self) -> usize {
        self.failure_count
    }

    /// Get the failure threshold
    pub fn failure_threshold(&self) -> usize {
        self.failure_threshold
    }

    /// Get the reset timeout duration
    pub fn reset_timeout(&self) -> Duration {
        self.reset_timeout
    }

    /// Get time since last failure, if any
    pub fn time_since_last_failure(&self) -> Option<Duration> {
        self.last_failure_time.map(|t| t.elapsed())
    }
}

impl Default for CircuitBreaker {
    /// Default circuit breaker: trips after 5 consecutive failures, 30s reset timeout
    fn default() -> Self {
        Self::new(5)
    }
}

/// Execute a function with retry logic
pub async fn execute_with_retry<F, Fut, T>(policy: RetryPolicy, mut operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let max_attempts = policy.max_attempts();

    if max_attempts == 0 {
        // No retry policy
        return operation().await;
    }

    let mut last_error;
    let mut attempt = 0;

    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(err) => {
                // Check if error is retryable
                if !err.is_retryable() {
                    return Err(err);
                }

                last_error = err;
                attempt += 1;

                // Check if we should retry
                if let Some(delay) = policy.delay_for_attempt(attempt - 1) {
                    tracing::warn!(
                        "Operation failed (attempt {}/{}), retrying in {:?}",
                        attempt,
                        max_attempts,
                        delay
                    );
                    sleep(delay).await;
                } else {
                    // No more retries
                    break;
                }
            }
        }
    }

    // All retries exhausted
    Err(Error::execution(format!(
        "Retry exhausted after {} attempts: {}",
        attempt,
        last_error.to_string()
    )))
}

/// Execute a synchronous function with retry logic
pub fn execute_with_retry_sync<F, T>(policy: RetryPolicy, mut operation: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    let max_attempts = policy.max_attempts();

    if max_attempts == 0 {
        return operation();
    }

    let mut last_error;
    let mut attempt = 0;

    loop {
        match operation() {
            Ok(result) => return Ok(result),
            Err(err) => {
                if !err.is_retryable() {
                    return Err(err);
                }

                last_error = err;
                attempt += 1;

                if let Some(delay) = policy.delay_for_attempt(attempt - 1) {
                    tracing::warn!(
                        "Operation failed (attempt {}/{}), retrying in {:?}",
                        attempt,
                        max_attempts,
                        delay
                    );
                    std::thread::sleep(delay);
                } else {
                    break;
                }
            }
        }
    }

    Err(Error::execution(format!(
        "Retry exhausted after {} attempts: {}",
        attempt,
        last_error.to_string()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_policy() {
        let policy = RetryPolicy::fixed(3, Duration::from_millis(100));

        assert_eq!(
            policy.delay_for_attempt(0),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            policy.delay_for_attempt(1),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            policy.delay_for_attempt(2),
            Some(Duration::from_millis(100))
        );
        assert_eq!(policy.delay_for_attempt(3), None);
    }

    #[test]
    fn test_exponential_policy() {
        let policy = RetryPolicy::exponential(4);

        let delay0 = policy.delay_for_attempt(0).unwrap();
        let delay1 = policy.delay_for_attempt(1).unwrap();
        let delay2 = policy.delay_for_attempt(2).unwrap();

        assert!(delay1 > delay0);
        assert!(delay2 > delay1);
        assert_eq!(policy.delay_for_attempt(4), None);
    }

    #[tokio::test]
    async fn test_execute_with_retry_success() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        let attempt = Arc::new(AtomicU32::new(0));
        let attempt_clone = attempt.clone();

        let result: Result<i32> =
            execute_with_retry(RetryPolicy::fixed(3, Duration::from_millis(10)), || {
                let attempt = attempt_clone.clone();
                async move {
                    let current = attempt.fetch_add(1, Ordering::SeqCst) + 1;
                    if current < 3 {
                        Err(Error::timeout("test"))
                    } else {
                        Ok(42)
                    }
                }
            })
            .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempt.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_execute_with_retry_exhausted() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        let attempt = Arc::new(AtomicU32::new(0));
        let attempt_clone = attempt.clone();

        let result: Result<()> =
            execute_with_retry(RetryPolicy::fixed(2, Duration::from_millis(10)), || {
                let attempt = attempt_clone.clone();
                async move {
                    attempt.fetch_add(1, Ordering::SeqCst);
                    Err(Error::timeout("test"))
                }
            })
            .await;

        assert!(result.unwrap_err().to_string().contains("Retry exhausted"));
        assert_eq!(attempt.load(Ordering::SeqCst), 3); // Initial + 2 retries
    }

    #[tokio::test]
    async fn test_non_retryable_error() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Arc;

        let attempt = Arc::new(AtomicU32::new(0));
        let attempt_clone = attempt.clone();

        let result: Result<()> =
            execute_with_retry(RetryPolicy::fixed(3, Duration::from_millis(10)), || {
                let attempt = attempt_clone.clone();
                async move {
                    attempt.fetch_add(1, Ordering::SeqCst);
                    Err(Error::Manifest("test".into()))
                }
            })
            .await;

        assert!(matches!(result, Err(Error::Manifest(_))));
        assert_eq!(attempt.load(Ordering::SeqCst), 1); // Should not retry
    }

    // spec 026: CircuitBreaker state transition tests (T012)
    #[test]
    fn test_circuit_breaker_default() {
        let cb = CircuitBreaker::default();
        assert_eq!(cb.failure_threshold(), 5);
        assert_eq!(cb.reset_timeout(), Duration::from_secs(30));
        assert_eq!(cb.state(), CircuitState::Closed);
        assert_eq!(cb.consecutive_failures(), 0);
    }

    #[test]
    fn test_circuit_breaker_with_timeout() {
        let cb = CircuitBreaker::with_timeout(3, Duration::from_millis(100));
        assert_eq!(cb.failure_threshold(), 3);
        assert_eq!(cb.reset_timeout(), Duration::from_millis(100));
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_closed_to_open() {
        let mut cb = CircuitBreaker::new(3);

        // First two failures don't trip it
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(!cb.is_open());

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(!cb.is_open());

        // Third failure trips it
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(cb.is_open());
        assert_eq!(cb.consecutive_failures(), 3);
    }

    #[test]
    fn test_circuit_breaker_success_resets() {
        let mut cb = CircuitBreaker::new(3);

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.consecutive_failures(), 2);

        // Success resets the counter
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert_eq!(cb.consecutive_failures(), 0);

        // Need 3 more failures to trip
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_open_to_half_open() {
        // Use very short timeout for testing
        let mut cb = CircuitBreaker::with_timeout(2, Duration::from_millis(10));

        // Trip the circuit
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(cb.is_open());

        // Wait for reset timeout
        std::thread::sleep(Duration::from_millis(15));

        // is_open() should transition to half-open
        assert!(!cb.is_open());
        assert_eq!(cb.state(), CircuitState::HalfOpen);
    }

    #[test]
    fn test_circuit_breaker_half_open_success() {
        let mut cb = CircuitBreaker::with_timeout(2, Duration::from_millis(5));

        // Trip and wait
        cb.record_failure();
        cb.record_failure();
        std::thread::sleep(Duration::from_millis(10));
        cb.is_open(); // Transition to half-open

        // Success should close the circuit
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(!cb.is_open());
        assert_eq!(cb.consecutive_failures(), 0);
    }

    #[test]
    fn test_circuit_breaker_half_open_failure() {
        let mut cb = CircuitBreaker::with_timeout(2, Duration::from_millis(5));

        // Trip and wait
        cb.record_failure();
        cb.record_failure();
        std::thread::sleep(Duration::from_millis(10));
        cb.is_open(); // Transition to half-open

        // Failure should re-open the circuit
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(cb.is_open());
    }

    #[test]
    fn test_circuit_breaker_reset() {
        let mut cb = CircuitBreaker::new(2);

        // Trip the circuit
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        // Manual reset
        cb.reset();
        assert_eq!(cb.state(), CircuitState::Closed);
        assert_eq!(cb.consecutive_failures(), 0);
        assert!(!cb.is_open());
    }

    #[test]
    fn test_circuit_breaker_is_open_readonly() {
        let mut cb = CircuitBreaker::with_timeout(2, Duration::from_millis(5));

        // Trip and wait
        cb.record_failure();
        cb.record_failure();
        std::thread::sleep(Duration::from_millis(10));

        // is_open_readonly should not transition state
        assert!(cb.is_open_readonly()); // Still reports open (no state change)
        assert_eq!(cb.state(), CircuitState::Open);

        // is_open() triggers transition
        assert!(!cb.is_open());
        assert_eq!(cb.state(), CircuitState::HalfOpen);
    }

    #[test]
    fn test_circuit_breaker_time_since_failure() {
        let mut cb = CircuitBreaker::new(5);

        assert!(cb.time_since_last_failure().is_none());

        cb.record_failure();
        let elapsed = cb.time_since_last_failure();
        assert!(elapsed.is_some());
        assert!(elapsed.unwrap() < Duration::from_millis(100));
    }
}
