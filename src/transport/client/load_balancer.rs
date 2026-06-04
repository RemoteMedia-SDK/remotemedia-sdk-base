//! Load balancing and endpoint pool management
//!
//! Manages multiple remote endpoints with health tracking and load distribution.
//!
//! # Features
//!
//! - Multiple load balancing strategies (round-robin, least-connections, random)
//! - Per-endpoint circuit breakers
//! - Active connection tracking
//! - Health-based endpoint selection
//!
//! # Example
//!
//! ```
//! use remotemedia_core::transport::client::LoadBalanceStrategy;
//!
//! // Load balancing strategies
//! let strategy = LoadBalanceStrategy::RoundRobin;
//! // Other options: LoadBalanceStrategy::LeastConnections, LoadBalanceStrategy::Random
//! ```

use super::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitState};
use crate::{Error, Result};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Load balancing strategy
///
/// Re-exported from remote_pipeline.rs
pub use crate::nodes::remote_pipeline::LoadBalanceStrategy;

/// Single endpoint with health and circuit breaker state
#[derive(Debug)]
pub struct Endpoint {
    /// Endpoint URL
    pub url: String,

    /// Circuit breaker for this endpoint
    pub circuit_breaker: CircuitBreaker,

    /// Number of active connections
    pub active_connections: Arc<RwLock<u32>>,

    /// Last successful request time
    pub last_success: Arc<RwLock<Option<std::time::Instant>>>,

    /// Last failed request time
    pub last_failure: Arc<RwLock<Option<std::time::Instant>>>,
}

impl Endpoint {
    /// Create new endpoint
    pub fn new(url: String, circuit_config: CircuitBreakerConfig) -> Self {
        Self {
            circuit_breaker: CircuitBreaker::new(url.clone(), circuit_config),
            url,
            active_connections: Arc::new(RwLock::new(0)),
            last_success: Arc::new(RwLock::new(None)),
            last_failure: Arc::new(RwLock::new(None)),
        }
    }

    /// Check if endpoint is healthy
    ///
    /// Endpoint is healthy if circuit breaker is not open
    pub async fn is_healthy(&self) -> bool {
        self.circuit_breaker.get_state().await != CircuitState::Open
    }

    /// Increment active connection count
    pub async fn acquire(&self) {
        *self.active_connections.write().await += 1;
    }

    /// Decrement active connection count
    pub async fn release(&self) {
        let mut count = self.active_connections.write().await;
        if *count > 0 {
            *count -= 1;
        }
    }

    /// Get active connection count
    pub async fn connections(&self) -> u32 {
        *self.active_connections.read().await
    }
}

/// Pool of remote endpoints with load balancing
pub struct EndpointPool {
    endpoints: Vec<Arc<Endpoint>>,
    strategy: LoadBalanceStrategy,
    round_robin_index: Arc<RwLock<usize>>,
}

impl EndpointPool {
    /// Create new endpoint pool
    pub fn new(
        urls: Vec<String>,
        strategy: LoadBalanceStrategy,
        circuit_config: CircuitBreakerConfig,
    ) -> Self {
        let endpoints = urls
            .into_iter()
            .map(|url| Arc::new(Endpoint::new(url, circuit_config.clone())))
            .collect();

        Self {
            endpoints,
            strategy,
            round_robin_index: Arc::new(RwLock::new(0)),
        }
    }

    /// Select next endpoint based on load balancing strategy
    ///
    /// # Returns
    ///
    /// * `Ok(Arc<Endpoint>)` - Selected endpoint
    /// * `Err(Error::AllEndpointsFailed)` - No healthy endpoints available
    pub async fn select(&self) -> Result<Arc<Endpoint>> {
        // Filter to healthy endpoints only
        let mut healthy = Vec::new();
        for endpoint in &self.endpoints {
            if endpoint.is_healthy().await {
                healthy.push(Arc::clone(endpoint));
            }
        }

        if healthy.is_empty() {
            return Err(Error::AllEndpointsFailed {
                count: self.endpoints.len(),
                details: "All endpoints have open circuit breakers".to_string(),
            });
        }

        // Select based on strategy
        let selected = match self.strategy {
            LoadBalanceStrategy::RoundRobin => self.select_round_robin(&healthy).await,
            LoadBalanceStrategy::LeastConnections => self.select_least_connections(&healthy).await,
            LoadBalanceStrategy::Random => self.select_random(&healthy),
        };

        Ok(selected)
    }

    /// Round-robin selection
    async fn select_round_robin(&self, healthy: &[Arc<Endpoint>]) -> Arc<Endpoint> {
        let mut index = self.round_robin_index.write().await;
        let selected = Arc::clone(&healthy[*index % healthy.len()]);
        *index = (*index + 1) % healthy.len();
        selected
    }

    /// Least-connections selection
    async fn select_least_connections(&self, healthy: &[Arc<Endpoint>]) -> Arc<Endpoint> {
        let mut min_connections = u32::MAX;
        let mut best_endpoint = Arc::clone(&healthy[0]);

        for endpoint in healthy {
            let connections = endpoint.connections().await;
            if connections < min_connections {
                min_connections = connections;
                best_endpoint = Arc::clone(endpoint);
            }
        }

        best_endpoint
    }

    /// Random selection
    fn select_random(&self, healthy: &[Arc<Endpoint>]) -> Arc<Endpoint> {
        let index = rand::random::<usize>() % healthy.len();
        Arc::clone(&healthy[index])
    }

    /// Record successful execution on endpoint
    pub async fn record_success(&self, endpoint: &Arc<Endpoint>) {
        *endpoint.last_success.write().await = Some(std::time::Instant::now());
        endpoint
            .circuit_breaker
            .execute(|| async { Ok::<(), Error>(()) })
            .await
            .ok();
    }

    /// Record failed execution on endpoint
    pub async fn record_failure(&self, endpoint: &Arc<Endpoint>) {
        *endpoint.last_failure.write().await = Some(std::time::Instant::now());
    }

    /// Get count of healthy endpoints
    pub async fn healthy_count(&self) -> usize {
        let mut count = 0;
        for endpoint in &self.endpoints {
            if endpoint.is_healthy().await {
                count += 1;
            }
        }
        count
    }

    /// Get all endpoints
    pub fn endpoints(&self) -> &[Arc<Endpoint>] {
        &self.endpoints
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_round_robin_distribution() {
        let urls = vec![
            "server1:50051".to_string(),
            "server2:50051".to_string(),
            "server3:50051".to_string(),
        ];

        let config = CircuitBreakerConfig::default();
        let pool = EndpointPool::new(urls, LoadBalanceStrategy::RoundRobin, config);

        // Select 6 times, should cycle through all endpoints twice
        let mut selections = Vec::new();
        for _ in 0..6 {
            let endpoint = pool.select().await.unwrap();
            selections.push(endpoint.url.clone());
        }

        assert_eq!(selections[0], "server1:50051");
        assert_eq!(selections[1], "server2:50051");
        assert_eq!(selections[2], "server3:50051");
        assert_eq!(selections[3], "server1:50051");
        assert_eq!(selections[4], "server2:50051");
        assert_eq!(selections[5], "server3:50051");
    }

    #[tokio::test]
    async fn test_all_endpoints_failed_error() {
        let urls = vec!["server1:50051".to_string()];

        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 2,
            reset_timeout_ms: 60000,
        };

        let pool = EndpointPool::new(urls, LoadBalanceStrategy::RoundRobin, config);

        // Fail the only endpoint
        let endpoint = pool.select().await.unwrap();
        endpoint
            .circuit_breaker
            .execute(|| async { Err::<(), _>(crate::Error::Transport("Failure".to_string())) })
            .await
            .ok();

        // Next select should fail
        let result = pool.select().await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::AllEndpointsFailed { .. }
        ));
    }
}
