//! Retry logic with exponential backoff for remote pipeline execution
//!
//! Implements retry policies for transient failures in remote execution.
//!
//! # Features
//!
//! - Exponential backoff with configurable base delay
//! - Maximum retry attempts
//! - Jitter to prevent thundering herd
//! - Integration with circuit breaker

use crate::Result;
use std::time::Duration;
use tracing::{debug, warn};

/// Retry configuration
///
/// Copied from remote_pipeline.rs for re-export convenience
pub use crate::nodes::remote_pipeline::RetryConfig;

/// Retry executor with exponential backoff
///
/// # Example
///
/// ```
/// use remotemedia_core::transport::client::RetryConfig;
/// use remotemedia_core::transport::client::retry::RetryExecutor;
///
/// let config = RetryConfig {
///     max_retries: 3,
///     backoff_ms: 1000,
/// };
///
/// let executor = RetryExecutor::new(config);
/// // Use executor.execute(|| async { ... }).await for retryable operations
/// ```
pub struct RetryExecutor {
    config: RetryConfig,
}

impl RetryExecutor {
    /// Create new retry executor
    pub fn new(config: RetryConfig) -> Self {
        Self { config }
    }

    /// Execute operation with retry logic
    ///
    /// # Arguments
    ///
    /// * `operation` - Async function to execute (can be called multiple times)
    ///
    /// # Returns
    ///
    /// * `Ok(T)` - Operation succeeded (possibly after retries)
    /// * `Err(Error)` - Operation failed after all retries exhausted
    pub async fn execute<F, Fut, T>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempts = 0;
        let max_attempts = self.config.max_retries + 1;

        loop {
            attempts += 1;

            match operation().await {
                Ok(result) => {
                    if attempts > 1 {
                        debug!("Operation succeeded after {} attempts", attempts);
                    }
                    return Ok(result);
                }
                Err(e) if attempts < max_attempts => {
                    // Calculate backoff with exponential increase
                    let backoff = self.config.backoff_ms * (2_u64.pow(attempts - 1));

                    // Add jitter (±25%) to prevent thundering herd
                    let jitter_range = (backoff / 4) as i64;
                    let jitter = (rand::random::<f64>() * 2.0 - 1.0) * jitter_range as f64;
                    let backoff_with_jitter = ((backoff as i64) + jitter as i64) as u64;

                    warn!(
                        "Operation failed (attempt {}/{}): {} - retrying in {}ms",
                        attempts, max_attempts, e, backoff_with_jitter
                    );

                    tokio::time::sleep(Duration::from_millis(backoff_with_jitter)).await;
                }
                Err(e) => {
                    warn!("Operation failed after {} attempts: {}", attempts, e);
                    return Err(e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_retry_succeeds_on_second_attempt() {
        let config = RetryConfig {
            max_retries: 3,
            backoff_ms: 10, // Fast for testing
        };

        let executor = RetryExecutor::new(config);
        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = Arc::clone(&attempt_count);

        let result = executor
            .execute(|| async {
                let count = attempt_count_clone.fetch_add(1, Ordering::SeqCst) + 1;
                if count < 2 {
                    Err(crate::Error::Transport("Transient failure".to_string()))
                } else {
                    Ok(42)
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempt_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_retry_exhausts_attempts() {
        let config = RetryConfig {
            max_retries: 2,
            backoff_ms: 10,
        };

        let executor = RetryExecutor::new(config);
        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = Arc::clone(&attempt_count);

        let result = executor
            .execute(|| async {
                attempt_count_clone.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(crate::Error::Transport("Always fails".to_string()))
            })
            .await;

        assert!(result.is_err());
        assert_eq!(attempt_count.load(Ordering::SeqCst), 3); // 1 + 2 retries
    }
}
