//! Circuit breaker pattern for remote endpoint protection
//!
//! Prevents cascading failures by stopping requests to failing endpoints.
//!
//! # States
//!
//! - **Closed**: Normal operation, requests allowed
//! - **Open**: Too many failures, requests rejected immediately
//! - **HalfOpen**: Testing recovery, limited requests allowed
//!
//! # State Transitions
//!
//! ```text
//! Closed ──(failures >= threshold)──> Open
//!   ↑                                   │
//!   │                                   │
//!   │                            (reset_timeout)
//!   │                                   │
//!   │                                   ▼
//!   └──(successes >= threshold)── HalfOpen
//! ```

use crate::{Error, Result};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Circuit breaker configuration
///
/// Re-exported from remote_pipeline.rs
pub use crate::nodes::remote_pipeline::CircuitBreakerConfig;

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation - requests allowed
    Closed,
    /// Too many failures - requests rejected
    Open,
    /// Testing recovery - limited requests allowed
    HalfOpen,
}

/// Circuit breaker for endpoint protection
///
/// Thread-safe circuit breaker that tracks endpoint health and
/// prevents requests to failing endpoints.
#[derive(Debug)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Arc<RwLock<CircuitBreakerState>>,
    endpoint: String,
}

#[derive(Debug, Clone)]
struct CircuitBreakerState {
    state: CircuitState,
    consecutive_failures: u32,
    consecutive_successes: u32,
    last_failure_time: Option<Instant>,
    last_state_change: Instant,
}

impl CircuitBreaker {
    /// Create new circuit breaker
    pub fn new(endpoint: String, config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(CircuitBreakerState {
                state: CircuitState::Closed,
                consecutive_failures: 0,
                consecutive_successes: 0,
                last_failure_time: None,
                last_state_change: Instant::now(),
            })),
            endpoint,
        }
    }

    /// Execute operation with circuit breaker protection
    ///
    /// # Arguments
    ///
    /// * `operation` - Async function to execute
    ///
    /// # Returns
    ///
    /// * `Ok(T)` - Operation succeeded
    /// * `Err(Error::CircuitBreakerOpen)` - Circuit is open, request rejected
    /// * `Err(Error)` - Operation failed
    pub async fn execute<F, Fut, T>(&self, operation: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        // Check if we should allow the request
        self.check_and_maybe_transition().await?;

        // Execute operation
        match operation().await {
            Ok(result) => {
                self.on_success().await;
                Ok(result)
            }
            Err(e) => {
                self.on_failure().await;
                Err(e)
            }
        }
    }

    /// Check current state and transition if needed
    async fn check_and_maybe_transition(&self) -> Result<()> {
        let mut state = self.state.write().await;

        match state.state {
            CircuitState::Closed => {
                // Allow request
                Ok(())
            }
            CircuitState::Open => {
                // Check if we should transition to half-open
                if let Some(last_failure) = state.last_failure_time {
                    let elapsed = last_failure.elapsed();
                    if elapsed >= Duration::from_millis(self.config.reset_timeout_ms) {
                        debug!(
                            "Circuit breaker for '{}' transitioning to HalfOpen after {}ms",
                            self.endpoint,
                            elapsed.as_millis()
                        );
                        state.state = CircuitState::HalfOpen;
                        state.consecutive_successes = 0;
                        state.last_state_change = Instant::now();
                        Ok(())
                    } else {
                        // Circuit still open
                        Err(Error::CircuitBreakerOpen {
                            endpoint: self.endpoint.clone(),
                            reason: format!(
                                "Circuit open for {}ms (reset in {}ms)",
                                state.last_state_change.elapsed().as_millis(),
                                self.config.reset_timeout_ms
                                    - state.last_state_change.elapsed().as_millis() as u64
                            ),
                        })
                    }
                } else {
                    // Should never happen (open without last_failure_time)
                    warn!("Circuit breaker in Open state but no last_failure_time - resetting to Closed");
                    state.state = CircuitState::Closed;
                    Ok(())
                }
            }
            CircuitState::HalfOpen => {
                // Allow limited requests
                Ok(())
            }
        }
    }

    /// Record successful execution
    async fn on_success(&self) {
        let mut state = self.state.write().await;

        state.consecutive_failures = 0;
        state.consecutive_successes += 1;

        if state.state == CircuitState::HalfOpen
            && state.consecutive_successes >= self.config.success_threshold
        {
            debug!(
                "Circuit breaker for '{}' closing after {} successful requests",
                self.endpoint, state.consecutive_successes
            );
            state.state = CircuitState::Closed;
            state.consecutive_successes = 0;
            state.last_state_change = Instant::now();
        }
    }

    /// Record failed execution
    async fn on_failure(&self) {
        let mut state = self.state.write().await;

        state.consecutive_successes = 0;
        state.consecutive_failures += 1;
        state.last_failure_time = Some(Instant::now());

        if state.state != CircuitState::Open
            && state.consecutive_failures >= self.config.failure_threshold
        {
            warn!(
                "Circuit breaker for '{}' opening after {} consecutive failures",
                self.endpoint, state.consecutive_failures
            );
            state.state = CircuitState::Open;
            state.last_state_change = Instant::now();
        }
    }

    /// Get current circuit state
    pub async fn get_state(&self) -> CircuitState {
        self.state.read().await.state
    }

    /// Force reset to closed state (for testing/admin)
    pub async fn reset(&self) {
        let mut state = self.state.write().await;
        state.state = CircuitState::Closed;
        state.consecutive_failures = 0;
        state.consecutive_successes = 0;
        state.last_failure_time = None;
        state.last_state_change = Instant::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_opens_after_threshold_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            reset_timeout_ms: 1000,
        };

        let cb = CircuitBreaker::new("test-endpoint".to_string(), config);

        // First 3 failures should open circuit
        for i in 0..3 {
            let result = cb
                .execute(|| async {
                    Err::<(), _>(crate::Error::Transport("Test failure".to_string()))
                })
                .await;
            assert!(result.is_err());

            if i < 2 {
                // Should still be closed
                assert_eq!(cb.get_state().await, CircuitState::Closed);
            } else {
                // Should now be open
                assert_eq!(cb.get_state().await, CircuitState::Open);
            }
        }
    }

    #[tokio::test]
    async fn test_circuit_rejects_requests_when_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 2,
            reset_timeout_ms: 5000,
        };

        let cb = CircuitBreaker::new("test-endpoint".to_string(), config);

        // Fail once to open circuit
        let _ = cb
            .execute(|| async { Err::<(), _>(crate::Error::Transport("Test failure".to_string())) })
            .await;

        assert_eq!(cb.get_state().await, CircuitState::Open);

        // Next request should be rejected immediately
        let result = cb.execute(|| async { Ok::<(), crate::Error>(()) }).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::CircuitBreakerOpen { .. }
        ));
    }

    #[tokio::test]
    async fn test_circuit_closes_after_successes_in_half_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 2,
            reset_timeout_ms: 10, // Fast for testing
        };

        let cb = CircuitBreaker::new("test-endpoint".to_string(), config);

        // Open circuit
        let _ = cb
            .execute(|| async { Err::<(), _>(crate::Error::Transport("Test failure".to_string())) })
            .await;
        assert_eq!(cb.get_state().await, CircuitState::Open);

        // Wait for reset timeout
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Next request transitions to HalfOpen
        let _ = cb.execute(|| async { Ok::<(), crate::Error>(()) }).await;
        assert_eq!(cb.get_state().await, CircuitState::HalfOpen);

        // Second success should close circuit
        let _ = cb.execute(|| async { Ok::<(), crate::Error>(()) }).await;
        assert_eq!(cb.get_state().await, CircuitState::Closed);
    }
}
