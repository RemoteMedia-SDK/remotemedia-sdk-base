//! Execution metrics collection
//!
//! Tracks performance metrics for nodes and pipelines.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Metrics for a single node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeMetrics {
    /// Node ID
    pub node_id: String,

    /// Total number of executions
    pub execution_count: usize,

    /// Total execution time
    pub total_duration: Duration,

    /// Average execution time
    pub avg_duration: Duration,

    /// Minimum execution time
    pub min_duration: Duration,

    /// Maximum execution time
    pub max_duration: Duration,

    /// Number of errors
    pub error_count: usize,

    /// Number of successful executions
    pub success_count: usize,
}

impl NodeMetrics {
    /// Create new metrics for a node
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            execution_count: 0,
            total_duration: Duration::ZERO,
            avg_duration: Duration::ZERO,
            min_duration: Duration::MAX,
            max_duration: Duration::ZERO,
            error_count: 0,
            success_count: 0,
        }
    }

    /// Record a successful execution
    pub fn record_success(&mut self, duration: Duration) {
        self.execution_count += 1;
        self.success_count += 1;
        self.total_duration += duration;
        self.min_duration = self.min_duration.min(duration);
        self.max_duration = self.max_duration.max(duration);
        self.avg_duration = self.total_duration / self.execution_count as u32;
    }

    /// Record an error
    pub fn record_error(&mut self, duration: Duration) {
        self.execution_count += 1;
        self.error_count += 1;
        self.total_duration += duration;
        self.min_duration = self.min_duration.min(duration);
        self.max_duration = self.max_duration.max(duration);
        self.avg_duration = self.total_duration / self.execution_count as u32;
    }

    /// Get success rate
    pub fn success_rate(&self) -> f64 {
        if self.execution_count == 0 {
            0.0
        } else {
            self.success_count as f64 / self.execution_count as f64
        }
    }
}

/// Metrics for an entire pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineMetrics {
    /// Pipeline ID
    pub pipeline_id: String,

    /// Per-node metrics
    node_metrics: HashMap<String, NodeMetrics>,

    /// Total pipeline executions
    total_executions: usize,

    /// Pipeline start time
    #[serde(skip)]
    start_time: Option<Instant>,

    /// Pipeline end time
    #[serde(skip)]
    end_time: Option<Instant>,

    /// Peak memory usage (estimated, in bytes)
    peak_memory_bytes: usize,
}

impl PipelineMetrics {
    /// Create new pipeline metrics
    pub fn new(pipeline_id: impl Into<String>) -> Self {
        Self {
            pipeline_id: pipeline_id.into(),
            node_metrics: HashMap::new(),
            total_executions: 0,
            start_time: None,
            end_time: None,
            peak_memory_bytes: 0,
        }
    }

    /// Start execution timing
    pub fn start_execution(&mut self) {
        self.start_time = Some(Instant::now());
        self.total_executions += 1;
    }

    /// End execution timing
    pub fn end_execution(&mut self) {
        self.end_time = Some(Instant::now());
    }

    /// Get total execution time
    pub fn total_duration(&self) -> Option<Duration> {
        if let (Some(start), Some(end)) = (self.start_time, self.end_time) {
            Some(end.duration_since(start))
        } else {
            None
        }
    }

    /// Record a node execution
    pub fn record_node_execution(
        &mut self,
        node_id: impl Into<String>,
        duration: Duration,
        success: bool,
    ) {
        let node_id = node_id.into();
        let metrics = self
            .node_metrics
            .entry(node_id.clone())
            .or_insert_with(|| NodeMetrics::new(node_id));

        if success {
            metrics.record_success(duration);
        } else {
            metrics.record_error(duration);
        }
    }

    /// Get metrics for a specific node
    pub fn get_node_metrics(&self, node_id: &str) -> Option<&NodeMetrics> {
        self.node_metrics.get(node_id)
    }

    /// Get all node metrics
    pub fn node_metrics(&self) -> &HashMap<String, NodeMetrics> {
        &self.node_metrics
    }

    /// Update peak memory usage
    pub fn update_peak_memory(&mut self, bytes: usize) {
        self.peak_memory_bytes = self.peak_memory_bytes.max(bytes);
    }

    /// Get peak memory usage
    pub fn peak_memory_bytes(&self) -> usize {
        self.peak_memory_bytes
    }

    /// Get total executions
    pub fn total_executions(&self) -> usize {
        self.total_executions
    }

    /// Export metrics as JSON with microsecond precision
    pub fn to_json(&self) -> serde_json::Value {
        let start = Instant::now();

        let total_duration_us = self.total_duration().map(|d| d.as_micros());

        // Convert node metrics to JSON with microsecond precision
        let node_metrics_json: Vec<serde_json::Value> = self.node_metrics
            .values()
            .map(|m| serde_json::json!({
                "node_id": m.node_id,
                "execution_count": m.execution_count,
                "success_count": m.success_count,
                "error_count": m.error_count,
                "success_rate": m.success_rate(),
                "total_duration_us": m.total_duration.as_micros(),
                "avg_duration_us": m.avg_duration.as_micros(),
                "min_duration_us": if m.min_duration == Duration::MAX { 0 } else { m.min_duration.as_micros() },
                "max_duration_us": m.max_duration.as_micros(),
            }))
            .collect();

        let serialization_overhead_us = start.elapsed().as_micros();

        serde_json::json!({
            "pipeline_id": self.pipeline_id,
            "total_executions": self.total_executions,
            "total_duration_us": total_duration_us,
            "total_duration_ms": self.total_duration().map(|d| d.as_millis()),
            "peak_memory_bytes": self.peak_memory_bytes,
            "peak_memory_mb": self.peak_memory_bytes as f64 / (1024.0 * 1024.0),
            "node_metrics": node_metrics_json,
            "metrics_overhead_us": serialization_overhead_us,
        })
    }
}

/// Thread-safe metrics collector
#[derive(Clone)]
pub struct MetricsCollector {
    inner: Arc<MetricsCollectorInner>,
}

struct MetricsCollectorInner {
    execution_count: AtomicUsize,
    error_count: AtomicUsize,
    total_duration_nanos: AtomicU64,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsCollectorInner {
                execution_count: AtomicUsize::new(0),
                error_count: AtomicUsize::new(0),
                total_duration_nanos: AtomicU64::new(0),
            }),
        }
    }

    /// Record an execution
    pub fn record_execution(&self, duration: Duration, success: bool) {
        self.inner.execution_count.fetch_add(1, Ordering::Relaxed);
        self.inner
            .total_duration_nanos
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);

        if !success {
            self.inner.error_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get execution count
    pub fn execution_count(&self) -> usize {
        self.inner.execution_count.load(Ordering::Relaxed)
    }

    /// Get error count
    pub fn error_count(&self) -> usize {
        self.inner.error_count.load(Ordering::Relaxed)
    }

    /// Get average duration
    pub fn avg_duration(&self) -> Duration {
        let count = self.execution_count();
        if count == 0 {
            return Duration::ZERO;
        }

        let total_nanos = self.inner.total_duration_nanos.load(Ordering::Relaxed);
        Duration::from_nanos(total_nanos / count as u64)
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_metrics() {
        let mut metrics = NodeMetrics::new("test_node");

        metrics.record_success(Duration::from_millis(100));
        metrics.record_success(Duration::from_millis(200));
        metrics.record_error(Duration::from_millis(150));

        assert_eq!(metrics.execution_count, 3);
        assert_eq!(metrics.success_count, 2);
        assert_eq!(metrics.error_count, 1);
        assert_eq!(metrics.success_rate(), 2.0 / 3.0);
    }

    #[test]
    fn test_pipeline_metrics() {
        let mut metrics = PipelineMetrics::new("test_pipeline");

        metrics.start_execution();
        metrics.record_node_execution("node1", Duration::from_millis(100), true);
        metrics.record_node_execution("node2", Duration::from_millis(200), false);
        metrics.end_execution();

        assert_eq!(metrics.total_executions(), 1);
        assert!(metrics.total_duration().is_some());

        let node1_metrics = metrics.get_node_metrics("node1").unwrap();
        assert_eq!(node1_metrics.success_count, 1);

        let node2_metrics = metrics.get_node_metrics("node2").unwrap();
        assert_eq!(node2_metrics.error_count, 1);
    }

    #[test]
    fn test_metrics_collector() {
        let collector = MetricsCollector::new();

        collector.record_execution(Duration::from_millis(100), true);
        collector.record_execution(Duration::from_millis(200), true);
        collector.record_execution(Duration::from_millis(150), false);

        assert_eq!(collector.execution_count(), 3);
        assert_eq!(collector.error_count(), 1);
        assert!(collector.avg_duration().as_millis() > 0);
    }
}
