//! StreamingScheduler - Production-grade node execution with reliability features
//!
//! This module provides a scheduler for streaming pipeline node execution with:
//! - Semaphore-based concurrency control
//! - Per-node timeout configuration
//! - Retry with exponential backoff (for retryable nodes only)
//! - Circuit breaker pattern per node instance
//! - Latency metrics collection (HDR histogram for P50/P95/P99)
//!
//! # Architecture
//!
//! The scheduler wraps node execution with reliability patterns:
//! 1. Concurrency limiting via semaphore
//! 2. Timeout enforcement
//! 3. Retry logic (only for nodes marked as retryable)
//! 4. Circuit breaker to prevent cascading failures
//! 5. Metrics recording for observability
//!
//! # Two Execution Paths
//!
//! - `execute_streaming_node()` - Full features: timeout, circuit breaker, metrics, retry
//! - `execute_streaming_node_fast()` - Minimal overhead: no timeout, read-only CB check, no metrics
//!
//! Use `execute_streaming_node_fast()` for latency-critical nodes where you want
//! minimal scheduler overhead (target: <1µs for fast nodes).
//!
//! # Retry Policy
//!
//! Retries are **disabled by default** and only apply to nodes explicitly
//! marked as retryable. This is critical for streaming pipelines where:
//! - Stateful transforms should not retry (may produce duplicates)
//! - Side-effecting nodes should not retry (may cause data corruption)
//!
//! Appropriate for retry:
//! - External inference calls
//! - Transient network operations
//! - Cache lookups
//!
//! NOT appropriate for retry:
//! - Audio/video encoders (stateful)
//! - Database writes (side effects)
//! - Streaming transforms
//!
//! # Example
//!
//! ```ignore
//! use remotemedia_core::executor::streaming_scheduler::{
//!     StreamingScheduler, SchedulerConfig
//! };
//!
//! let config = SchedulerConfig::default();
//! let scheduler = StreamingScheduler::new(config);
//!
//! // Execute a node with all protections (full features)
//! let result = scheduler.execute_streaming_node(
//!     "whisper",
//!     || async { node.process(input).await },
//! ).await?;
//!
//! // Execute with minimal overhead (fast path)
//! let result = scheduler.execute_streaming_node_fast(
//!     "audio_transform",
//!     || async { node.process(input).await },
//! ).await?;
//! ```
//!
//! # Spec Reference
//!
//! See `/specs/026-streaming-scheduler-migration/` for full specification.

use crate::executor::latency_metrics::{LatencyMetrics, Window};
use crate::executor::metrics::PipelineMetrics;
use crate::executor::retry::{CircuitBreaker, RetryPolicy};
use crate::{Error, Result};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::time::timeout;

/// Default maximum concurrency
pub const DEFAULT_MAX_CONCURRENCY: usize = 32;

/// Default node timeout in milliseconds
pub const DEFAULT_TIMEOUT_MS: u64 = 30_000;

/// Default circuit breaker failure threshold
pub const DEFAULT_CIRCUIT_BREAKER_THRESHOLD: usize = 5;

/// Scheduler configuration
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// Maximum concurrent node executions
    pub max_concurrency: usize,
    /// Default timeout for node execution (milliseconds)
    pub default_timeout_ms: u64,
    /// Per-node timeout overrides (node_id -> timeout_ms)
    pub node_timeouts: HashMap<String, u64>,
    /// Retry policy for retryable nodes
    pub retry_policy: RetryPolicy,
    /// Set of node IDs that are retryable (empty = no retries)
    pub retryable_nodes: HashSet<String>,
    /// Set of node IDs that use fast path execution (lock-free, no timeout)
    pub fast_path_nodes: HashSet<String>,
    /// Circuit breaker failure threshold
    pub circuit_breaker_threshold: usize,
    /// Enable metrics collection
    pub enable_metrics: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        // Honour REMOTEMEDIA_NODE_TIMEOUT_MS so heavy first-call
        // inferences (fp16 LFM2-Audio, torch.compile, etc.) can finish.
        // Falls back to the 30s ceiling otherwise.
        let default_timeout_ms = std::env::var("REMOTEMEDIA_NODE_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_TIMEOUT_MS);
        Self {
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
            default_timeout_ms,
            node_timeouts: HashMap::new(),
            retry_policy: RetryPolicy::None, // No retries by default
            retryable_nodes: HashSet::new(),
            fast_path_nodes: HashSet::new(),
            circuit_breaker_threshold: DEFAULT_CIRCUIT_BREAKER_THRESHOLD,
            enable_metrics: true,
        }
    }
}

impl SchedulerConfig {
    /// Create a new scheduler config with specified max concurrency
    pub fn with_concurrency(max_concurrency: usize) -> Self {
        Self {
            max_concurrency,
            ..Default::default()
        }
    }

    /// Set the default timeout
    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.default_timeout_ms = timeout_ms;
        self
    }

    /// Add a node-specific timeout override
    pub fn with_node_timeout(mut self, node_id: impl Into<String>, timeout_ms: u64) -> Self {
        self.node_timeouts.insert(node_id.into(), timeout_ms);
        self
    }

    /// Set the retry policy
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Mark a node as retryable
    pub fn with_retryable_node(mut self, node_id: impl Into<String>) -> Self {
        self.retryable_nodes.insert(node_id.into());
        self
    }

    /// Set circuit breaker threshold
    pub fn with_circuit_breaker_threshold(mut self, threshold: usize) -> Self {
        self.circuit_breaker_threshold = threshold;
        self
    }

    /// Mark a node for fast path execution (lock-free, no timeout)
    pub fn with_fast_path_node(mut self, node_id: impl Into<String>) -> Self {
        self.fast_path_nodes.insert(node_id.into());
        self
    }

    /// Check if a node should use fast path execution
    pub fn is_fast_path(&self, node_id: &str) -> bool {
        self.fast_path_nodes.contains(node_id)
    }
}

/// Result of a scheduler execution
#[derive(Debug, Clone)]
pub struct SchedulerResult<T> {
    /// The actual result
    pub result: T,
    /// Execution duration in microseconds
    pub duration_us: u64,
    /// Number of retry attempts (0 if succeeded first try)
    pub retry_count: u32,
}

/// Per-node execution state with atomics for lock-free hot path operations
struct NodeExecutionState {
    /// Circuit breaker for this node (protected by Mutex for state transitions)
    circuit_breaker: Mutex<CircuitBreaker>,
    /// Atomic flag for fast circuit breaker check (avoids lock on hot path)
    circuit_open: AtomicBool,
    /// Consecutive failure count for lock-free CB updates
    consecutive_failures: AtomicU64,
    /// CB failure threshold
    failure_threshold: u64,
    /// Latency metrics for this node (used by full path)
    latency_metrics: LatencyMetrics,
    /// Total execution count (atomic for lock-free increment)
    execution_count: AtomicU64,
    /// Total error count (atomic for lock-free increment)
    error_count: AtomicU64,
    /// Fast path latency sum (for computing average without locks)
    latency_sum_us: AtomicU64,
    /// Fast path latency count
    latency_count: AtomicU64,
    /// Fast path min latency
    latency_min_us: AtomicU64,
    /// Fast path max latency
    latency_max_us: AtomicU64,
}

impl NodeExecutionState {
    fn new(node_id: &str, threshold: usize) -> Result<Self> {
        let latency_metrics = LatencyMetrics::new(node_id)
            .map_err(|e| Error::Execution(format!("Failed to create latency metrics: {}", e)))?;

        Ok(Self {
            circuit_breaker: Mutex::new(CircuitBreaker::new(threshold)),
            circuit_open: AtomicBool::new(false),
            consecutive_failures: AtomicU64::new(0),
            failure_threshold: threshold as u64,
            latency_metrics,
            execution_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            latency_sum_us: AtomicU64::new(0),
            latency_count: AtomicU64::new(0),
            latency_min_us: AtomicU64::new(u64::MAX),
            latency_max_us: AtomicU64::new(0),
        })
    }

    /// Check if circuit breaker is open (lock-free fast path)
    fn is_circuit_open(&self) -> bool {
        self.circuit_open.load(Ordering::Acquire)
    }

    /// Update circuit breaker state atomically
    fn set_circuit_open(&self, open: bool) {
        self.circuit_open.store(open, Ordering::Release);
    }

    /// Record success with full latency metrics (takes locks)
    async fn record_success(&self, duration_us: u64) {
        self.execution_count.fetch_add(1, Ordering::Relaxed);
        let _ = self.latency_metrics.record_latency(duration_us);

        let mut cb = self.circuit_breaker.lock().await;
        cb.record_success();
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.set_circuit_open(false);
    }

    /// Record success lock-free (fast path - no locks, no histogram)
    ///
    /// Only updates atomic counters. Use for latency-critical nodes.
    fn record_success_fast(&self, duration_us: u64) {
        self.execution_count.fetch_add(1, Ordering::Relaxed);

        // Lock-free latency tracking (sum/count/min/max)
        self.latency_sum_us
            .fetch_add(duration_us, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);

        // Update min (atomic compare-and-swap loop)
        let mut current_min = self.latency_min_us.load(Ordering::Relaxed);
        while duration_us < current_min {
            match self.latency_min_us.compare_exchange_weak(
                current_min,
                duration_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_min) => current_min = new_min,
            }
        }

        // Update max (atomic compare-and-swap loop)
        let mut current_max = self.latency_max_us.load(Ordering::Relaxed);
        while duration_us > current_max {
            match self.latency_max_us.compare_exchange_weak(
                current_max,
                duration_us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_max) => current_max = new_max,
            }
        }

        // Lock-free circuit breaker reset on success
        self.consecutive_failures.store(0, Ordering::Relaxed);
        // Only clear open flag if it was set (avoid unnecessary writes)
        if self.circuit_open.load(Ordering::Relaxed) {
            self.set_circuit_open(false);
        }
    }

    /// Record failure: increment counters, update circuit breaker
    async fn record_failure(&self, duration_us: u64) {
        self.execution_count.fetch_add(1, Ordering::Relaxed);
        self.error_count.fetch_add(1, Ordering::Relaxed);
        let _ = self.latency_metrics.record_latency(duration_us);

        let mut cb = self.circuit_breaker.lock().await;
        cb.record_failure();
        self.set_circuit_open(cb.is_open_readonly());
    }

    /// Record failure lock-free (fast path)
    fn record_failure_fast(&self, duration_us: u64) {
        self.execution_count.fetch_add(1, Ordering::Relaxed);
        self.error_count.fetch_add(1, Ordering::Relaxed);

        // Lock-free latency tracking
        self.latency_sum_us
            .fetch_add(duration_us, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);

        // Lock-free circuit breaker update
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= self.failure_threshold {
            self.set_circuit_open(true);
        }
    }

    /// Check and potentially transition circuit breaker (for full path)
    async fn check_circuit_breaker(&self) -> bool {
        let mut cb = self.circuit_breaker.lock().await;
        let is_open = cb.is_open();
        self.set_circuit_open(is_open);
        is_open
    }

    /// Get execution count
    fn get_execution_count(&self) -> u64 {
        self.execution_count.load(Ordering::Relaxed)
    }

    /// Get error count
    fn get_error_count(&self) -> u64 {
        self.error_count.load(Ordering::Relaxed)
    }
}

/// Production-grade streaming node scheduler
pub struct StreamingScheduler {
    /// Configuration
    pub config: SchedulerConfig,
    /// Concurrency semaphore
    semaphore: Arc<Semaphore>,
    /// Per-node execution state (Arc for sharing across tasks)
    node_states: Arc<RwLock<HashMap<String, Arc<NodeExecutionState>>>>,
    /// Pipeline-level metrics
    metrics: Arc<RwLock<PipelineMetrics>>,
}

impl StreamingScheduler {
    /// Create a new streaming scheduler
    pub fn new(config: SchedulerConfig) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrency)),
            node_states: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(RwLock::new(PipelineMetrics::new("streaming_scheduler"))),
            config,
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(SchedulerConfig::default())
    }

    /// Get or create node state, returning Arc for lock-free access
    async fn get_or_create_node_state(&self, node_id: &str) -> Result<Arc<NodeExecutionState>> {
        // Fast path: check with read lock
        {
            let states = self.node_states.read().await;
            if let Some(state) = states.get(node_id) {
                return Ok(state.clone());
            }
        }

        // Slow path: acquire write lock and insert
        let mut states = self.node_states.write().await;
        // Double-check after acquiring write lock
        if let Some(state) = states.get(node_id) {
            return Ok(state.clone());
        }

        let state = Arc::new(NodeExecutionState::new(
            node_id,
            self.config.circuit_breaker_threshold,
        )?);
        states.insert(node_id.to_string(), state.clone());
        Ok(state)
    }

    /// Execute a streaming node with all scheduler protections
    ///
    /// This method applies:
    /// 1. Concurrency limiting
    /// 2. Timeout enforcement
    /// 3. Retry logic (if node is marked retryable)
    /// 4. Circuit breaker
    /// 5. Metrics recording
    ///
    /// For minimal overhead execution, use `execute_streaming_node_fast()` instead.
    ///
    /// # Arguments
    ///
    /// * `node_id` - Unique identifier for the node
    /// * `operation` - The async operation to execute
    ///
    /// # Returns
    ///
    /// * `Ok(SchedulerResult<T>)` - Execution succeeded
    /// * `Err(Error)` - Execution failed after all retries or circuit open
    pub async fn execute_streaming_node<T, F, Fut>(
        &self,
        node_id: &str,
        operation: F,
    ) -> Result<SchedulerResult<T>>
    where
        F: Fn() -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<T>> + Send,
        T: Send,
    {
        // Get or create node state (single lock acquisition)
        let node_state = self.get_or_create_node_state(node_id).await?;

        // Check circuit breaker (takes mutex only if needed for state transition)
        if node_state.check_circuit_breaker().await {
            return Err(Error::Execution(format!(
                "Circuit breaker open for node '{}'",
                node_id
            )));
        }

        // Acquire semaphore permit
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| Error::Execution(format!("Failed to acquire semaphore: {}", e)))?;

        // Get timeout for this node
        let timeout_duration = self.get_timeout(node_id);

        // Check if node is retryable
        let is_retryable = self.config.retryable_nodes.contains(node_id);

        let start = std::time::Instant::now();
        let mut retry_count = 0;
        let mut last_error: Option<Error> = None;

        // Retry loop (only if retryable)
        let max_attempts = if is_retryable {
            self.config.retry_policy.max_attempts().max(1)
        } else {
            1
        };

        for attempt in 0..max_attempts {
            if attempt > 0 {
                retry_count = attempt as u32;
                // Apply backoff delay using the existing delay_for_attempt method
                if let Some(delay) = self.config.retry_policy.delay_for_attempt(attempt) {
                    tokio::time::sleep(delay).await;
                }
            }

            // Execute with timeout
            let result = timeout(timeout_duration, operation()).await;

            match result {
                Ok(Ok(value)) => {
                    // Success - record metrics and update circuit breaker
                    let duration = start.elapsed();
                    let duration_us = duration.as_micros() as u64;

                    // Record to node state (atomics + per-node mutex)
                    node_state.record_success(duration_us).await;

                    // Update pipeline metrics (if enabled)
                    if self.config.enable_metrics {
                        let mut metrics = self.metrics.write().await;
                        metrics.record_node_execution(node_id, duration, true);
                    }

                    return Ok(SchedulerResult {
                        result: value,
                        duration_us,
                        retry_count,
                    });
                }
                Ok(Err(e)) => {
                    // Operation failed
                    last_error = Some(e);
                    if !is_retryable || attempt + 1 >= max_attempts {
                        break;
                    }
                    tracing::warn!(
                        "Node '{}' attempt {} failed, retrying...",
                        node_id,
                        attempt + 1
                    );
                }
                Err(_) => {
                    // Timeout
                    last_error = Some(Error::Execution(format!(
                        "Node '{}' timed out after {:?}",
                        node_id, timeout_duration
                    )));
                    if !is_retryable || attempt + 1 >= max_attempts {
                        break;
                    }
                    tracing::warn!(
                        "Node '{}' attempt {} timed out, retrying...",
                        node_id,
                        attempt + 1
                    );
                }
            }
        }

        // All attempts failed
        let duration = start.elapsed();
        let duration_us = duration.as_micros() as u64;

        // Record failure to node state
        node_state.record_failure(duration_us).await;

        // Update pipeline metrics (if enabled)
        if self.config.enable_metrics {
            let mut metrics = self.metrics.write().await;
            metrics.record_node_execution(node_id, duration, false);
        }

        Err(last_error.unwrap_or_else(|| {
            Error::Execution(format!("Node '{}' failed with unknown error", node_id))
        }))
    }

    /// Execute a streaming node with minimal scheduler overhead (fast path)
    ///
    /// This method provides:
    /// - Lock-free circuit breaker check (atomic read)
    /// - No timeout wrapper (avoids tokio timer overhead)
    /// - No pipeline metrics recording (avoids extra lock)
    /// - Atomic counter updates (no lock for counters)
    /// - Lock-free latency recording (no HDR histogram, just sum/count/min/max)
    /// - Lock-free circuit breaker updates (no mutex on success/failure)
    /// - try_acquire() for semaphore (falls back to async acquire only if needed)
    ///
    /// Use this for latency-critical nodes where scheduler overhead matters.
    /// Target overhead: <1µs for fast nodes.
    ///
    /// # Arguments
    ///
    /// * `node_id` - Unique identifier for the node
    /// * `operation` - The async operation to execute
    ///
    /// # Returns
    ///
    /// * `Ok(SchedulerResult<T>)` - Execution succeeded
    /// * `Err(Error)` - Execution failed or circuit breaker open
    pub async fn execute_streaming_node_fast<T, F, Fut>(
        &self,
        node_id: &str,
        operation: F,
    ) -> Result<SchedulerResult<T>>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = Result<T>> + Send,
        T: Send,
    {
        // Get or create node state
        let node_state = self.get_or_create_node_state(node_id).await?;

        // Lock-free circuit breaker check (atomic read only)
        if node_state.is_circuit_open() {
            return Err(Error::Execution(format!(
                "Circuit breaker open for node '{}'",
                node_id
            )));
        }

        // Try non-blocking semaphore acquire first (fast path)
        // Falls back to async acquire only if permits exhausted
        let _permit = match self.semaphore.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                // Slow path: wait for permit
                self.semaphore
                    .acquire()
                    .await
                    .map_err(|e| Error::Execution(format!("Failed to acquire semaphore: {}", e)))?
            }
        };

        let start = std::time::Instant::now();

        // Execute directly without timeout wrapper
        match operation().await {
            Ok(value) => {
                let duration_us = start.elapsed().as_micros() as u64;

                // Lock-free success recording (atomics only, no mutex, no HDR histogram)
                node_state.record_success_fast(duration_us);

                Ok(SchedulerResult {
                    result: value,
                    duration_us,
                    retry_count: 0,
                })
            }
            Err(e) => {
                let duration_us = start.elapsed().as_micros() as u64;

                // Lock-free failure recording (atomics only, no mutex)
                node_state.record_failure_fast(duration_us);

                Err(e)
            }
        }
    }

    /// Get timeout duration for a node
    fn get_timeout(&self, node_id: &str) -> Duration {
        let timeout_ms = self
            .config
            .node_timeouts
            .get(node_id)
            .copied()
            .unwrap_or(self.config.default_timeout_ms);
        Duration::from_millis(timeout_ms)
    }

    /// Get latency percentiles for a node
    ///
    /// Returns (P50, P95, P99) in microseconds
    pub async fn get_latency_percentiles(&self, node_id: &str) -> Option<(u64, u64, u64)> {
        let states = self.node_states.read().await;
        states.get(node_id).map(|state| {
            (
                state.latency_metrics.p50(Window::OneMinute),
                state.latency_metrics.p95(Window::OneMinute),
                state.latency_metrics.p99(Window::OneMinute),
            )
        })
    }

    /// Get node execution statistics
    pub async fn get_node_stats(&self, node_id: &str) -> Option<NodeStats> {
        let states = self.node_states.read().await;
        states.get(node_id).map(|state| NodeStats {
            execution_count: state.get_execution_count(),
            error_count: state.get_error_count(),
            circuit_breaker_open: state.is_circuit_open(),
            p50_us: state.latency_metrics.p50(Window::OneMinute),
            p95_us: state.latency_metrics.p95(Window::OneMinute),
            p99_us: state.latency_metrics.p99(Window::OneMinute),
        })
    }

    /// Get all node statistics
    pub async fn get_all_node_stats(&self) -> HashMap<String, NodeStats> {
        let states = self.node_states.read().await;
        states
            .iter()
            .map(|(id, state)| {
                (
                    id.clone(),
                    NodeStats {
                        execution_count: state.get_execution_count(),
                        error_count: state.get_error_count(),
                        circuit_breaker_open: state.is_circuit_open(),
                        p50_us: state.latency_metrics.p50(Window::OneMinute),
                        p95_us: state.latency_metrics.p95(Window::OneMinute),
                        p99_us: state.latency_metrics.p99(Window::OneMinute),
                    },
                )
            })
            .collect()
    }

    /// Get pipeline metrics
    pub async fn get_metrics(&self) -> PipelineMetrics {
        self.metrics.read().await.clone()
    }

    /// Reset circuit breaker for a node
    pub async fn reset_circuit_breaker(&self, node_id: &str) {
        let states = self.node_states.read().await;
        if let Some(state) = states.get(node_id) {
            let mut cb = state.circuit_breaker.lock().await;
            cb.reset();
            state.set_circuit_open(false);
        }
    }

    /// Export metrics in Prometheus format
    pub async fn to_prometheus(&self) -> String {
        let mut output = String::new();

        // Export node-level metrics
        let states = self.node_states.read().await;
        for (node_id, state) in states.iter() {
            output.push_str(&format!(
                "streaming_scheduler_node_executions_total{{node_id=\"{}\"}} {}\n",
                node_id,
                state.get_execution_count()
            ));
            output.push_str(&format!(
                "streaming_scheduler_node_errors_total{{node_id=\"{}\"}} {}\n",
                node_id,
                state.get_error_count()
            ));
            output.push_str(&format!(
                "streaming_scheduler_node_circuit_breaker_open{{node_id=\"{}\"}} {}\n",
                node_id,
                if state.is_circuit_open() { 1 } else { 0 }
            ));
            output.push_str(&format!(
                "streaming_scheduler_node_latency_p50_us{{node_id=\"{}\"}} {}\n",
                node_id,
                state.latency_metrics.p50(Window::OneMinute)
            ));
            output.push_str(&format!(
                "streaming_scheduler_node_latency_p95_us{{node_id=\"{}\"}} {}\n",
                node_id,
                state.latency_metrics.p95(Window::OneMinute)
            ));
            output.push_str(&format!(
                "streaming_scheduler_node_latency_p99_us{{node_id=\"{}\"}} {}\n",
                node_id,
                state.latency_metrics.p99(Window::OneMinute)
            ));
        }

        // Export scheduler-level metrics
        output.push_str(&format!(
            "streaming_scheduler_max_concurrency {}\n",
            self.config.max_concurrency
        ));
        output.push_str(&format!(
            "streaming_scheduler_available_permits {}\n",
            self.semaphore.available_permits()
        ));

        output
    }
}

/// Node execution statistics
#[derive(Debug, Clone)]
pub struct NodeStats {
    /// Total execution count
    pub execution_count: u64,
    /// Total error count
    pub error_count: u64,
    /// Whether circuit breaker is open
    pub circuit_breaker_open: bool,
    /// P50 latency in microseconds
    pub p50_us: u64,
    /// P95 latency in microseconds
    pub p95_us: u64,
    /// P99 latency in microseconds
    pub p99_us: u64,
}

impl NodeStats {
    /// Calculate error rate (0.0-1.0)
    pub fn error_rate(&self) -> f64 {
        if self.execution_count == 0 {
            0.0
        } else {
            self.error_count as f64 / self.execution_count as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn test_scheduler_creation() {
        let scheduler = StreamingScheduler::with_defaults();
        assert_eq!(scheduler.config.max_concurrency, DEFAULT_MAX_CONCURRENCY);
    }

    #[tokio::test]
    async fn test_successful_execution() {
        let scheduler = StreamingScheduler::with_defaults();

        let result = scheduler
            .execute_streaming_node("test_node", || async { Ok::<_, Error>(42) })
            .await
            .unwrap();

        assert_eq!(result.result, 42);
        assert_eq!(result.retry_count, 0);
        assert!(result.duration_us > 0);
    }

    #[tokio::test]
    async fn test_execution_failure_no_retry() {
        let scheduler = StreamingScheduler::with_defaults();

        let result = scheduler
            .execute_streaming_node::<i32, _, _>("test_node", || async {
                Err(Error::Execution("test error".to_string()))
            })
            .await;

        assert!(result.is_err());

        // Verify error count
        let stats = scheduler.get_node_stats("test_node").await.unwrap();
        assert_eq!(stats.error_count, 1);
    }

    #[tokio::test]
    async fn test_retry_for_retryable_node() {
        let config = SchedulerConfig::default()
            .with_retryable_node("retryable_node")
            .with_retry_policy(RetryPolicy::fixed(3, Duration::from_millis(10)));

        let scheduler = StreamingScheduler::new(config);

        let attempt_count = Arc::new(AtomicU32::new(0));
        let attempt_count_clone = attempt_count.clone();

        let result = scheduler
            .execute_streaming_node("retryable_node", || {
                let count = attempt_count_clone.clone();
                async move {
                    let attempt = count.fetch_add(1, Ordering::SeqCst);
                    if attempt < 2 {
                        Err(Error::Execution("transient error".to_string()))
                    } else {
                        Ok(42)
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(result.result, 42);
        assert_eq!(result.retry_count, 2);
        assert_eq!(attempt_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_timeout() {
        let config = SchedulerConfig::default().with_timeout(10);

        let scheduler = StreamingScheduler::new(config);

        let result = scheduler
            .execute_streaming_node::<i32, _, _>("slow_node", || async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(42)
            })
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("timed out"));
    }

    #[tokio::test]
    async fn test_circuit_breaker() {
        let config = SchedulerConfig::default().with_circuit_breaker_threshold(3);

        let scheduler = StreamingScheduler::new(config);

        // Fail 3 times to open circuit breaker
        for _ in 0..3 {
            let _ = scheduler
                .execute_streaming_node::<i32, _, _>("failing_node", || async {
                    Err(Error::Execution("failure".to_string()))
                })
                .await;
        }

        // Circuit should be open now
        let result = scheduler
            .execute_streaming_node::<i32, _, _>("failing_node", || async { Ok(42) })
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Circuit breaker open"));

        // Verify stats
        let stats = scheduler.get_node_stats("failing_node").await.unwrap();
        assert!(stats.circuit_breaker_open);
    }

    #[tokio::test]
    async fn test_per_node_timeout() {
        let config = SchedulerConfig::default()
            .with_timeout(1000)
            .with_node_timeout("fast_node", 10);

        let scheduler = StreamingScheduler::new(config);

        let result = scheduler
            .execute_streaming_node::<i32, _, _>("fast_node", || async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(42)
            })
            .await;

        // Should timeout with the 10ms override
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_latency_percentiles() {
        let scheduler = StreamingScheduler::with_defaults();

        // Execute multiple times to build up metrics
        for _ in 0..10 {
            scheduler
                .execute_streaming_node("test_node", || async {
                    tokio::time::sleep(Duration::from_micros(100)).await;
                    Ok::<_, Error>(())
                })
                .await
                .unwrap();
        }

        let percentiles = scheduler.get_latency_percentiles("test_node").await;
        assert!(percentiles.is_some());

        let (p50, p95, p99) = percentiles.unwrap();
        assert!(p50 > 0);
        assert!(p95 >= p50);
        assert!(p99 >= p95);
    }

    #[tokio::test]
    async fn test_prometheus_export() {
        let scheduler = StreamingScheduler::with_defaults();

        scheduler
            .execute_streaming_node("test_node", || async { Ok::<_, Error>(42) })
            .await
            .unwrap();

        let prom = scheduler.to_prometheus().await;
        assert!(prom.contains("streaming_scheduler_node_executions_total"));
        assert!(prom.contains("streaming_scheduler_node_latency_p50_us"));
        assert!(prom.contains("streaming_scheduler_max_concurrency"));
    }

    #[tokio::test]
    async fn test_concurrency_limiting() {
        let config = SchedulerConfig::with_concurrency(2);
        let scheduler = Arc::new(StreamingScheduler::new(config));

        let active_count = Arc::new(AtomicU32::new(0));
        let max_concurrent = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();

        for _ in 0..5 {
            let sched = scheduler.clone();
            let active = active_count.clone();
            let max = max_concurrent.clone();

            handles.push(tokio::spawn(async move {
                sched
                    .execute_streaming_node("concurrent_node", || {
                        let active = active.clone();
                        let max = max.clone();
                        async move {
                            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                            max.fetch_max(current, Ordering::SeqCst);

                            tokio::time::sleep(Duration::from_millis(10)).await;

                            active.fetch_sub(1, Ordering::SeqCst);
                            Ok::<_, Error>(())
                        }
                    })
                    .await
            }));
        }

        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        // Max concurrent should not exceed 2
        assert!(max_concurrent.load(Ordering::SeqCst) <= 2);
    }
}
