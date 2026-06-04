//! Task scheduler and execution context
//!
//! Manages concurrent node execution with tokio runtime.

use crate::executor::error::ExecutionErrorExt;
use crate::executor::metrics::PipelineMetrics;
use crate::executor::retry::{execute_with_retry, CircuitBreaker, RetryPolicy};
use crate::{Error, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore};
use tokio::time::timeout;

/// Execution context for a node
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    /// Pipeline ID
    pub pipeline_id: String,

    /// Node ID
    pub node_id: String,

    /// Input data
    pub input_data: Value,

    /// Additional metadata
    pub metadata: HashMap<String, Value>,

    /// Execution timeout
    pub timeout: Option<Duration>,
}

impl ExecutionContext {
    /// Create a new execution context
    pub fn new(pipeline_id: impl Into<String>, node_id: impl Into<String>) -> Self {
        Self {
            pipeline_id: pipeline_id.into(),
            node_id: node_id.into(),
            input_data: Value::Null,
            metadata: HashMap::new(),
            timeout: None,
        }
    }

    /// Set input data
    pub fn with_input(mut self, data: Value) -> Self {
        self.input_data = data;
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, duration: Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }
}

/// Task scheduler for concurrent node execution
pub struct Scheduler {
    /// Maximum concurrent tasks
    max_concurrency: usize,

    /// Semaphore for limiting concurrency
    semaphore: Arc<Semaphore>,

    /// Metrics collector
    metrics: Arc<RwLock<PipelineMetrics>>,

    /// Default timeout for tasks
    default_timeout: Option<Duration>,

    /// Circuit breaker for fault tolerance
    circuit_breaker: Arc<RwLock<CircuitBreaker>>,

    /// Retry policy for failed operations
    retry_policy: RetryPolicy,
}

impl Scheduler {
    /// Create a new scheduler
    pub fn new(max_concurrency: usize, pipeline_id: impl Into<String>) -> Self {
        Self {
            max_concurrency,
            semaphore: Arc::new(Semaphore::new(max_concurrency)),
            metrics: Arc::new(RwLock::new(PipelineMetrics::new(pipeline_id))),
            default_timeout: None,
            circuit_breaker: Arc::new(RwLock::new(CircuitBreaker::default())),
            retry_policy: RetryPolicy::default(),
        }
    }

    /// Set default timeout
    pub fn with_default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = Some(timeout);
        self
    }

    /// Set retry policy
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Set circuit breaker
    pub fn with_circuit_breaker(mut self, breaker: CircuitBreaker) -> Self {
        self.circuit_breaker = Arc::new(RwLock::new(breaker));
        self
    }

    /// Execute node with retry and circuit breaker logic
    pub async fn execute_node_with_retry<F, Fut>(
        &self,
        ctx: ExecutionContext,
        operation: F,
    ) -> Result<Value>
    where
        F: Fn(Value) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<Value>> + Send,
    {
        let node_id = ctx.node_id.clone();

        // Check circuit breaker
        {
            let mut breaker = self.circuit_breaker.write().await;
            if breaker.is_open() {
                return Err(Error::execution(format!(
                    "Circuit breaker is open for node '{}' - too many consecutive failures",
                    node_id
                )));
            }
        }

        // Execute with retry
        let result = execute_with_retry(self.retry_policy, || {
            let input_data = ctx.input_data.clone();
            operation(input_data)
        })
        .await;

        // Update circuit breaker based on result
        {
            let mut breaker = self.circuit_breaker.write().await;
            match &result {
                Ok(_) => breaker.record_success(),
                Err(_) => breaker.record_failure(),
            }
        }

        result
    }

    /// Schedule a node for execution
    pub async fn schedule_node<F, Fut>(&self, ctx: ExecutionContext, operation: F) -> Result<Value>
    where
        F: FnOnce(Value) -> Fut + Send,
        Fut: std::future::Future<Output = Result<Value>> + Send,
    {
        // Acquire permit for concurrency control
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| Error::Execution(format!("Failed to acquire semaphore: {}", e)))?;

        let start_time = Instant::now();
        let node_id = ctx.node_id.clone();

        // Determine timeout
        let task_timeout = ctx.timeout.or(self.default_timeout);

        // Execute with optional timeout
        let result = if let Some(duration) = task_timeout {
            match timeout(duration, operation(ctx.input_data)).await {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(Error::timeout(format!(
                    "Node '{}' exceeded timeout of {:?}",
                    node_id, duration
                ))),
            }
        } else {
            operation(ctx.input_data).await
        };

        // Record metrics
        let duration = start_time.elapsed();
        let success = result.is_ok();

        let mut metrics = self.metrics.write().await;
        metrics.record_node_execution(&node_id, duration, success);

        result
    }

    /// Execute multiple nodes in parallel
    pub async fn execute_parallel<F, Fut>(
        &self,
        contexts: Vec<ExecutionContext>,
        operation: F,
    ) -> Result<Vec<Value>>
    where
        F: Fn(ExecutionContext) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<Value>> + Send + 'static,
    {
        let operation = Arc::new(operation);
        let mut tasks = Vec::new();

        for ctx in contexts {
            let op = operation.clone();
            let semaphore = self.semaphore.clone();
            let metrics = self.metrics.clone();
            let default_timeout = self.default_timeout;

            let task = tokio::spawn(async move {
                let _permit = semaphore
                    .acquire()
                    .await
                    .map_err(|e| Error::Execution(format!("Failed to acquire semaphore: {}", e)))?;

                let start_time = Instant::now();
                let node_id = ctx.node_id.clone();
                let task_timeout = ctx.timeout.or(default_timeout);

                let input_data = ctx.input_data.clone();
                let result = if let Some(duration) = task_timeout {
                    match timeout(duration, op(ctx)).await {
                        Ok(Ok(value)) => Ok(value),
                        Ok(Err(e)) => Err(e),
                        Err(_) => Err(Error::timeout(format!(
                            "Node '{}' exceeded timeout of {:?}",
                            node_id, duration
                        ))),
                    }
                } else {
                    op(ExecutionContext {
                        pipeline_id: "".to_string(),
                        node_id: node_id.clone(),
                        input_data,
                        metadata: HashMap::new(),
                        timeout: None,
                    })
                    .await
                };

                let duration = start_time.elapsed();
                let success = result.is_ok();

                let mut metrics_guard = metrics.write().await;
                metrics_guard.record_node_execution(&node_id, duration, success);

                result
            });

            tasks.push(task);
        }

        // Wait for all tasks to complete
        let mut results = Vec::new();
        for task in tasks {
            let result = task
                .await
                .map_err(|e| Error::Execution(format!("Task join error: {}", e)))??;
            results.push(result);
        }

        Ok(results)
    }

    /// Get collected metrics
    pub async fn get_metrics(&self) -> PipelineMetrics {
        self.metrics.read().await.clone()
    }

    /// Get maximum concurrency
    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_scheduler_creation() {
        let scheduler = Scheduler::new(4, "test_pipeline");
        assert_eq!(scheduler.max_concurrency(), 4);
    }

    #[tokio::test]
    async fn test_schedule_node() {
        let scheduler = Scheduler::new(2, "test");

        let ctx = ExecutionContext::new("test", "node1").with_input(Value::from(42));

        let result = scheduler
            .schedule_node(ctx, |input| async move {
                let num = input.as_i64().unwrap();
                Ok(Value::from(num * 2))
            })
            .await
            .unwrap();

        assert_eq!(result, Value::from(84));
    }

    #[tokio::test]
    async fn test_timeout() {
        let scheduler = Scheduler::new(2, "test").with_default_timeout(Duration::from_millis(100));

        let ctx = ExecutionContext::new("test", "slow_node").with_input(Value::Null);

        let result = scheduler
            .schedule_node(ctx, |_| async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Ok(Value::Null)
            })
            .await;

        assert!(result.unwrap_err().to_string().contains("Timeout"));
    }

    #[tokio::test]
    async fn test_parallel_execution() {
        let scheduler = Scheduler::new(4, "test");

        let contexts = vec![
            ExecutionContext::new("test", "node1").with_input(Value::from(1)),
            ExecutionContext::new("test", "node2").with_input(Value::from(2)),
            ExecutionContext::new("test", "node3").with_input(Value::from(3)),
        ];

        let results = scheduler
            .execute_parallel(contexts, |ctx| async move {
                let num = ctx.input_data.as_i64().unwrap();
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(Value::from(num * 2))
            })
            .await
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0], Value::from(2));
        assert_eq!(results[1], Value::from(4));
        assert_eq!(results[2], Value::from(6));
    }

    #[tokio::test]
    async fn test_metrics_collection() {
        let scheduler = Scheduler::new(2, "test");

        let ctx1 = ExecutionContext::new("test", "node1").with_input(Value::from(1));
        let ctx2 = ExecutionContext::new("test", "node2").with_input(Value::from(2));

        scheduler
            .schedule_node(ctx1, |input| async move { Ok(input) })
            .await
            .unwrap();

        scheduler
            .schedule_node(ctx2, |_| async move {
                Err(Error::Execution("test error".to_string()))
            })
            .await
            .ok();

        let metrics = scheduler.get_metrics().await;
        assert_eq!(metrics.node_metrics().len(), 2);

        let node1_metrics = metrics.get_node_metrics("node1").unwrap();
        assert_eq!(node1_metrics.success_count, 1);

        let node2_metrics = metrics.get_node_metrics("node2").unwrap();
        assert_eq!(node2_metrics.error_count, 1);
    }
}
