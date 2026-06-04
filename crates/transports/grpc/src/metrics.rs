//! Prometheus metrics collection for gRPC service
//!
//! Tracks request counters, latency histograms, active connections gauge.
//! Metrics exposed at /metrics HTTP endpoint.

use prometheus::{CounterVec, HistogramOpts, HistogramVec, IntGauge, IntGaugeVec, Opts, Registry};
use std::sync::Arc;

/// Prometheus metrics for gRPC service
#[derive(Clone)]
pub struct ServiceMetrics {
    /// Total requests processed (labeled by RPC method and status)
    pub requests_total: CounterVec,

    /// Request latency distribution in seconds (labeled by RPC method)
    pub request_duration_seconds: HistogramVec,

    /// Active connections gauge
    pub active_connections: IntGauge,

    /// Active concurrent executions gauge
    pub active_executions: IntGauge,

    /// Pipeline execution errors (labeled by error type)
    pub execution_errors_total: CounterVec,

    /// Audio samples processed
    pub audio_samples_processed: CounterVec,

    /// Memory usage gauge per execution (labeled by execution ID)
    pub execution_memory_bytes: IntGaugeVec,

    /// Active streaming sessions gauge (Phase 5 - T057)
    pub active_streams: IntGauge,

    /// Streaming chunks processed counter (Phase 5 - T057)
    pub stream_chunks_total: CounterVec,

    /// Streaming chunk latency distribution in seconds (Phase 5 - T057)
    pub stream_chunk_latency_seconds: HistogramVec,

    /// Streaming chunks dropped counter (Phase 5 - T057)
    pub stream_chunks_dropped_total: CounterVec,

    /// Node cache hits counter (Feature 005 - Backend Infrastructure)
    pub node_cache_hits_total: CounterVec,

    /// Node cache misses counter (Feature 005 - Backend Infrastructure)
    pub node_cache_misses_total: CounterVec,

    /// Cached nodes gauge (Feature 005 - Backend Infrastructure)
    pub cached_nodes: IntGauge,

    /// Prometheus registry
    pub registry: Arc<Registry>,
}

impl ServiceMetrics {
    /// Create new metrics with a custom registry
    pub fn new(registry: Registry) -> Result<Self, prometheus::Error> {
        let requests_total = CounterVec::new(
            Opts::new(
                "remotemedia_grpc_requests_total",
                "Total number of gRPC requests processed",
            ),
            &["method", "status"],
        )?;

        let request_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "remotemedia_grpc_request_duration_seconds",
                "Request latency distribution in seconds",
            )
            .buckets(vec![
                0.001, // 1ms
                0.0025, 0.005, // 2.5ms, 5ms (target: <5ms p50)
                0.01, 0.025, 0.05, // 10ms, 25ms, 50ms
                0.1, 0.25, 0.5, 1.0, // 100ms, 250ms, 500ms, 1s
            ]),
            &["method"],
        )?;

        let active_connections = IntGauge::new(
            "remotemedia_grpc_active_connections",
            "Number of active gRPC connections",
        )?;

        let active_executions = IntGauge::new(
            "remotemedia_grpc_active_executions",
            "Number of concurrent pipeline executions",
        )?;

        let execution_errors_total = CounterVec::new(
            Opts::new(
                "remotemedia_execution_errors_total",
                "Total pipeline execution errors",
            ),
            &["error_type"],
        )?;

        let audio_samples_processed = CounterVec::new(
            Opts::new(
                "remotemedia_audio_samples_processed_total",
                "Total audio samples processed",
            ),
            &["node_type"],
        )?;

        let execution_memory_bytes = IntGaugeVec::new(
            Opts::new(
                "remotemedia_execution_memory_bytes",
                "Current memory usage per execution",
            ),
            &["execution_id"],
        )?;

        // Phase 5 - Streaming metrics (T057)
        let active_streams = IntGauge::new(
            "remotemedia_grpc_active_streams",
            "Number of active streaming sessions",
        )?;

        let stream_chunks_total = CounterVec::new(
            Opts::new(
                "remotemedia_stream_chunks_total",
                "Total streaming chunks processed",
            ),
            &["session_id", "status"],
        )?;

        let stream_chunk_latency_seconds = HistogramVec::new(
            HistogramOpts::new(
                "remotemedia_stream_chunk_latency_seconds",
                "Per-chunk processing latency distribution (target: <0.05s)",
            )
            .buckets(vec![
                0.001, 0.005, 0.01, 0.025, // 1ms, 5ms, 10ms, 25ms
                0.05,  // 50ms (target threshold)
                0.1, 0.25, 0.5, 1.0, // 100ms, 250ms, 500ms, 1s
            ]),
            &["session_id"],
        )?;

        let stream_chunks_dropped_total = CounterVec::new(
            Opts::new(
                "remotemedia_stream_chunks_dropped_total",
                "Total chunks dropped due to backpressure",
            ),
            &["session_id", "reason"],
        )?;

        // Feature 005 - Node cache metrics
        let node_cache_hits_total = CounterVec::new(
            Opts::new("remotemedia_node_cache_hits_total", "Total node cache hits"),
            &["node_type"],
        )?;

        let node_cache_misses_total = CounterVec::new(
            Opts::new(
                "remotemedia_node_cache_misses_total",
                "Total node cache misses",
            ),
            &["node_type"],
        )?;

        let cached_nodes = IntGauge::new(
            "remotemedia_cached_nodes",
            "Number of nodes currently cached",
        )?;

        // Register all metrics
        registry.register(Box::new(requests_total.clone()))?;
        registry.register(Box::new(request_duration_seconds.clone()))?;
        registry.register(Box::new(active_connections.clone()))?;
        registry.register(Box::new(active_executions.clone()))?;
        registry.register(Box::new(execution_errors_total.clone()))?;
        registry.register(Box::new(audio_samples_processed.clone()))?;
        registry.register(Box::new(execution_memory_bytes.clone()))?;
        registry.register(Box::new(active_streams.clone()))?;
        registry.register(Box::new(stream_chunks_total.clone()))?;
        registry.register(Box::new(stream_chunk_latency_seconds.clone()))?;
        registry.register(Box::new(stream_chunks_dropped_total.clone()))?;
        registry.register(Box::new(node_cache_hits_total.clone()))?;
        registry.register(Box::new(node_cache_misses_total.clone()))?;
        registry.register(Box::new(cached_nodes.clone()))?;

        Ok(Self {
            requests_total,
            request_duration_seconds,
            active_connections,
            active_executions,
            execution_errors_total,
            audio_samples_processed,
            execution_memory_bytes,
            active_streams,
            stream_chunks_total,
            stream_chunk_latency_seconds,
            stream_chunks_dropped_total,
            node_cache_hits_total,
            node_cache_misses_total,
            cached_nodes,
            registry: Arc::new(registry),
        })
    }

    /// Create with default registry
    pub fn with_default_registry() -> Result<Self, prometheus::Error> {
        Self::new(Registry::new())
    }

    /// Record RPC request start (returns start time for duration calculation)
    pub fn record_request_start(&self, _method: &str) -> std::time::Instant {
        self.active_executions.inc();
        std::time::Instant::now()
    }

    /// Record RPC request completion
    pub fn record_request_end(&self, method: &str, status: &str, start: std::time::Instant) {
        let duration = start.elapsed();
        self.requests_total
            .with_label_values(&[method, status])
            .inc();
        self.request_duration_seconds
            .with_label_values(&[method])
            .observe(duration.as_secs_f64());
        self.active_executions.dec();
    }

    /// Record execution error
    pub fn record_error(&self, error_type: &str) {
        self.execution_errors_total
            .with_label_values(&[error_type])
            .inc();
    }

    /// Record audio samples processed
    pub fn record_samples_processed(&self, node_type: &str, samples: u64) {
        self.audio_samples_processed
            .with_label_values(&[node_type])
            .inc_by(samples as f64);
    }

    /// Update execution memory usage
    pub fn set_execution_memory(&self, execution_id: &str, bytes: i64) {
        self.execution_memory_bytes
            .with_label_values(&[execution_id])
            .set(bytes);
    }

    /// Remove execution memory tracking
    pub fn clear_execution_memory(&self, execution_id: &str) {
        let _ = self
            .execution_memory_bytes
            .remove_label_values(&[execution_id]);
    }

    // Phase 5 - Streaming metrics methods (T057)

    /// Record streaming session start
    pub fn record_stream_start(&self) {
        self.active_streams.inc();
    }

    /// Record streaming session end
    pub fn record_stream_end(&self) {
        self.active_streams.dec();
    }

    /// Record chunk processed successfully
    pub fn record_chunk_processed(&self, session_id: &str, latency_seconds: f64) {
        self.stream_chunks_total
            .with_label_values(&[session_id, "success"])
            .inc();
        self.stream_chunk_latency_seconds
            .with_label_values(&[session_id])
            .observe(latency_seconds);
    }

    /// Record chunk processing error
    pub fn record_chunk_error(&self, session_id: &str) {
        self.stream_chunks_total
            .with_label_values(&[session_id, "error"])
            .inc();
    }

    /// Record chunk dropped due to backpressure
    pub fn record_chunk_dropped(&self, session_id: &str, reason: &str) {
        self.stream_chunks_dropped_total
            .with_label_values(&[session_id, reason])
            .inc();
    }

    // Feature 005 - Node cache metrics methods

    /// Record node cache hit
    pub fn record_cache_hit(&self, node_type: &str) {
        self.node_cache_hits_total
            .with_label_values(&[node_type])
            .inc();
    }

    /// Record node cache miss
    pub fn record_cache_miss(&self, node_type: &str) {
        self.node_cache_misses_total
            .with_label_values(&[node_type])
            .inc();
    }

    /// Update cached nodes count
    pub fn set_cached_nodes_count(&self, count: i64) {
        self.cached_nodes.set(count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = ServiceMetrics::with_default_registry();
        assert!(metrics.is_ok());
    }

    #[test]
    fn test_request_metrics() {
        let metrics = ServiceMetrics::with_default_registry().unwrap();

        let start = metrics.record_request_start("ExecutePipeline");
        std::thread::sleep(std::time::Duration::from_millis(10));
        metrics.record_request_end("ExecutePipeline", "success", start);

        // Verify counter incremented by checking the metric value
        let counter = metrics
            .requests_total
            .with_label_values(&["ExecutePipeline", "success"]);
        assert!(counter.get() > 0.0);
    }

    #[test]
    fn test_error_metrics() {
        let metrics = ServiceMetrics::with_default_registry().unwrap();

        metrics.record_error("validation");
        metrics.record_error("execution");

        // Verify error counters incremented
        let validation_errors = metrics
            .execution_errors_total
            .with_label_values(&["validation"]);
        assert!(validation_errors.get() > 0.0);
    }

    #[test]
    fn test_audio_samples_metrics() {
        let metrics = ServiceMetrics::with_default_registry().unwrap();

        metrics.record_samples_processed("AudioResample", 44100);
        metrics.record_samples_processed("VAD", 16000);

        // Verify samples counter incremented
        let resample_samples = metrics
            .audio_samples_processed
            .with_label_values(&["AudioResample"]);
        assert!(resample_samples.get() >= 44100.0);
    }

    #[test]
    fn test_execution_memory_tracking() {
        let metrics = ServiceMetrics::with_default_registry().unwrap();

        metrics.set_execution_memory("exec-123", 10_000_000); // 10MB
        metrics.set_execution_memory("exec-456", 5_000_000); // 5MB

        // Verify memory gauge set correctly
        let exec_456_mem = metrics
            .execution_memory_bytes
            .with_label_values(&["exec-456"]);
        assert_eq!(exec_456_mem.get(), 5_000_000);

        metrics.clear_execution_memory("exec-123");
    }

    #[test]
    fn test_active_connections() {
        let metrics = ServiceMetrics::with_default_registry().unwrap();

        metrics.active_connections.inc();
        metrics.active_connections.inc();
        assert_eq!(metrics.active_connections.get(), 2);

        metrics.active_connections.dec();
        assert_eq!(metrics.active_connections.get(), 1);
    }
}
