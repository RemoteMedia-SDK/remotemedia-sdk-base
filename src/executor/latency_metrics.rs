//! Latency metrics collection and reporting
//!
//! Per-node performance metrics using HDR Histogram for accurate P50/P95/P99 tracking
//! with minimal overhead (<1% of node processing time).

use hdrhistogram::Histogram;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Time window for metrics aggregation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Window {
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
}

/// Per-node performance metrics
pub struct LatencyMetrics {
    /// Node identifier
    pub node_id: String,

    /// Latency histogram for 1-minute window
    histogram_1min: Arc<Mutex<Histogram<u64>>>,

    /// Latency histogram for 5-minute window
    histogram_5min: Arc<Mutex<Histogram<u64>>>,

    /// Latency histogram for 15-minute window
    histogram_15min: Arc<Mutex<Histogram<u64>>>,

    /// Current queue depth
    pub queue_depth_current: Arc<AtomicUsize>,

    /// Maximum queue depth observed
    pub queue_depth_max: Arc<AtomicUsize>,

    /// Average batch size (fixed-point: value * 100)
    pub batch_size_avg: Arc<AtomicU64>,

    /// Speculation acceptance rate (percentage * 100)
    pub speculation_acceptance_rate: Arc<AtomicU64>,

    /// Total inputs processed
    pub total_inputs: Arc<AtomicU64>,

    /// Timestamp of last reset (microseconds)
    pub last_reset: Arc<AtomicU64>,
}

impl LatencyMetrics {
    /// Create new metrics for a node
    ///
    /// Histograms are configured to track latencies from 1μs to 1 second with 3 significant figures
    pub fn new(node_id: impl Into<String>) -> Result<Self, String> {
        // Create histograms with appropriate range (1μs to 1 second = 1,000,000μs)
        let hist_1min = Histogram::<u64>::new_with_max(1_000_000, 3)
            .map_err(|e| format!("Failed to create 1min histogram: {}", e))?;
        let hist_5min = Histogram::<u64>::new_with_max(1_000_000, 3)
            .map_err(|e| format!("Failed to create 5min histogram: {}", e))?;
        let hist_15min = Histogram::<u64>::new_with_max(1_000_000, 3)
            .map_err(|e| format!("Failed to create 15min histogram: {}", e))?;

        Ok(Self {
            node_id: node_id.into(),
            histogram_1min: Arc::new(Mutex::new(hist_1min)),
            histogram_5min: Arc::new(Mutex::new(hist_5min)),
            histogram_15min: Arc::new(Mutex::new(hist_15min)),
            queue_depth_current: Arc::new(AtomicUsize::new(0)),
            queue_depth_max: Arc::new(AtomicUsize::new(0)),
            batch_size_avg: Arc::new(AtomicU64::new(100)), // Default 1.00 (100/100)
            speculation_acceptance_rate: Arc::new(AtomicU64::new(0)),
            total_inputs: Arc::new(AtomicU64::new(0)),
            last_reset: Arc::new(AtomicU64::new(current_timestamp_us())),
        })
    }

    /// Record a latency sample (microseconds)
    ///
    /// Records in all three time windows. Returns error if value exceeds histogram range.
    pub fn record_latency(&self, latency_us: u64) -> Result<(), String> {
        // Clamp to histogram range (1μs to 1s)
        let clamped = latency_us.clamp(1, 1_000_000);

        // Record in all windows
        self.histogram_1min
            .lock()
            .unwrap()
            .record(clamped)
            .map_err(|e| format!("Failed to record in 1min histogram: {}", e))?;

        self.histogram_5min
            .lock()
            .unwrap()
            .record(clamped)
            .map_err(|e| format!("Failed to record in 5min histogram: {}", e))?;

        self.histogram_15min
            .lock()
            .unwrap()
            .record(clamped)
            .map_err(|e| format!("Failed to record in 15min histogram: {}", e))?;

        // Increment total inputs
        self.total_inputs.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Get percentile from specified window
    ///
    /// Returns latency in microseconds at the given percentile (0.0-1.0)
    pub fn percentile(&self, p: f64, window: Window) -> u64 {
        let histogram = match window {
            Window::OneMinute => &self.histogram_1min,
            Window::FiveMinutes => &self.histogram_5min,
            Window::FifteenMinutes => &self.histogram_15min,
        };

        histogram.lock().unwrap().value_at_quantile(p)
    }

    /// Get P50 latency (median)
    pub fn p50(&self, window: Window) -> u64 {
        self.percentile(0.50, window)
    }

    /// Get P95 latency
    pub fn p95(&self, window: Window) -> u64 {
        self.percentile(0.95, window)
    }

    /// Get P99 latency
    pub fn p99(&self, window: Window) -> u64 {
        self.percentile(0.99, window)
    }

    /// Update current queue depth
    pub fn set_queue_depth(&self, depth: usize) {
        self.queue_depth_current.store(depth, Ordering::Relaxed);

        // Update max if needed
        let current_max = self.queue_depth_max.load(Ordering::Relaxed);
        if depth > current_max {
            self.queue_depth_max.store(depth, Ordering::Relaxed);
        }
    }

    /// Update batch size (moving average)
    pub fn record_batch_size(&self, size: usize) {
        const ALPHA_FIXED: u64 = 10; // 0.1 as fixed-point (10/100)
        let size_fixed = (size as u64) * 100; // Convert to fixed-point

        let current = self.batch_size_avg.load(Ordering::Relaxed);
        let new_value = (ALPHA_FIXED * size_fixed + (100 - ALPHA_FIXED) * current) / 100;

        self.batch_size_avg.store(new_value, Ordering::Relaxed);
    }

    /// Update speculation acceptance rate (percentage 0-100)
    pub fn set_speculation_acceptance_rate(&self, rate_percent: f64) {
        let rate_fixed = (rate_percent * 100.0) as u64; // Convert to fixed-point (percent * 100)
        self.speculation_acceptance_rate
            .store(rate_fixed, Ordering::Relaxed);
    }

    /// Reset histograms (for rotation)
    ///
    /// Typically called every 1/5/15 minutes to implement sliding windows
    pub fn reset_window(&self, window: Window) -> Result<(), String> {
        let histogram = match window {
            Window::OneMinute => &self.histogram_1min,
            Window::FiveMinutes => &self.histogram_5min,
            Window::FifteenMinutes => &self.histogram_15min,
        };

        histogram.lock().unwrap().reset();
        Ok(())
    }

    /// Export to Prometheus format
    ///
    /// Generates Prometheus histogram and gauge metrics for this node
    pub fn to_prometheus(&self) -> String {
        let mut output = String::new();

        // Latency histograms (P50/P95/P99 for each window)
        for (window_name, window) in [
            ("1min", Window::OneMinute),
            ("5min", Window::FiveMinutes),
            ("15min", Window::FifteenMinutes),
        ] {
            let p50 = self.p50(window);
            let p95 = self.p95(window);
            let p99 = self.p99(window);

            output.push_str(&format!(
                "node_latency_us{{node_id=\"{}\",quantile=\"0.5\",window=\"{}\"}} {}\n",
                self.node_id, window_name, p50
            ));
            output.push_str(&format!(
                "node_latency_us{{node_id=\"{}\",quantile=\"0.95\",window=\"{}\"}} {}\n",
                self.node_id, window_name, p95
            ));
            output.push_str(&format!(
                "node_latency_us{{node_id=\"{}\",quantile=\"0.99\",window=\"{}\"}} {}\n",
                self.node_id, window_name, p99
            ));
        }

        // Queue depth (current and max)
        let queue_current = self.queue_depth_current.load(Ordering::Relaxed);
        let queue_max = self.queue_depth_max.load(Ordering::Relaxed);

        output.push_str(&format!(
            "node_queue_depth{{node_id=\"{}\"}} {}\n",
            self.node_id, queue_current
        ));
        output.push_str(&format!(
            "node_queue_depth_max{{node_id=\"{}\"}} {}\n",
            self.node_id, queue_max
        ));

        // Batch size average (convert from fixed-point)
        let batch_avg_fixed = self.batch_size_avg.load(Ordering::Relaxed);
        let batch_avg = (batch_avg_fixed as f64) / 100.0;
        output.push_str(&format!(
            "node_batch_size_avg{{node_id=\"{}\"}} {:.2}\n",
            self.node_id, batch_avg
        ));

        // Speculation acceptance rate (convert from fixed-point)
        let spec_rate_fixed = self.speculation_acceptance_rate.load(Ordering::Relaxed);
        let spec_rate = (spec_rate_fixed as f64) / 100.0;
        output.push_str(&format!(
            "node_speculation_acceptance_rate{{node_id=\"{}\"}} {:.2}\n",
            self.node_id, spec_rate
        ));

        // Total inputs processed
        let total = self.total_inputs.load(Ordering::Relaxed);
        output.push_str(&format!(
            "node_total_inputs{{node_id=\"{}\"}} {}\n",
            self.node_id, total
        ));

        output
    }

    /// Get current queue depth
    pub fn get_queue_depth(&self) -> usize {
        self.queue_depth_current.load(Ordering::Relaxed)
    }

    /// Get maximum queue depth observed
    pub fn get_queue_depth_max(&self) -> usize {
        self.queue_depth_max.load(Ordering::Relaxed)
    }

    /// Get average batch size as float
    pub fn get_batch_size_avg(&self) -> f64 {
        let fixed = self.batch_size_avg.load(Ordering::Relaxed);
        (fixed as f64) / 100.0
    }

    /// Get speculation acceptance rate as percentage
    pub fn get_speculation_acceptance_rate(&self) -> f64 {
        let fixed = self.speculation_acceptance_rate.load(Ordering::Relaxed);
        (fixed as f64) / 100.0
    }

    /// Get total inputs processed
    pub fn get_total_inputs(&self) -> u64 {
        self.total_inputs.load(Ordering::Relaxed)
    }
}

/// Get current timestamp in microseconds since Unix epoch
fn current_timestamp_us() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time is before Unix epoch")
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_latency_metrics() {
        let metrics = LatencyMetrics::new("test_node").expect("Failed to create metrics");

        assert_eq!(metrics.node_id, "test_node");
        assert_eq!(metrics.get_queue_depth(), 0);
        assert_eq!(metrics.get_total_inputs(), 0);
    }

    #[test]
    fn test_record_latency_updates_histograms() {
        let metrics = LatencyMetrics::new("test_node").unwrap();

        // Record some latencies
        for i in 1..=100 {
            metrics.record_latency(i * 1000).expect("Failed to record");
        }

        // Verify percentiles are reasonable
        let p50 = metrics.p50(Window::OneMinute);
        let p95 = metrics.p95(Window::OneMinute);
        let p99 = metrics.p99(Window::OneMinute);

        // P50 should be around 50,000μs (50ms)
        assert!(p50 >= 45_000 && p50 <= 55_000, "P50 is {}", p50);

        // P95 should be around 95,000μs (95ms)
        assert!(p95 >= 90_000 && p95 <= 100_000, "P95 is {}", p95);

        // P99 should be around 99,000μs (99ms)
        assert!(p99 >= 95_000 && p99 <= 105_000, "P99 is {}", p99);

        // All windows should have same values (no rotation yet)
        assert_eq!(
            metrics.p50(Window::OneMinute),
            metrics.p50(Window::FiveMinutes)
        );
        assert_eq!(
            metrics.p99(Window::OneMinute),
            metrics.p99(Window::FifteenMinutes)
        );
    }

    #[test]
    fn test_record_latency_increments_total_inputs() {
        let metrics = LatencyMetrics::new("test_node").unwrap();

        assert_eq!(metrics.get_total_inputs(), 0);

        metrics.record_latency(1000).unwrap();
        assert_eq!(metrics.get_total_inputs(), 1);

        metrics.record_latency(2000).unwrap();
        assert_eq!(metrics.get_total_inputs(), 2);
    }

    #[test]
    fn test_queue_depth_tracking() {
        let metrics = LatencyMetrics::new("test_node").unwrap();

        metrics.set_queue_depth(5);
        assert_eq!(metrics.get_queue_depth(), 5);
        assert_eq!(metrics.get_queue_depth_max(), 5);

        metrics.set_queue_depth(10);
        assert_eq!(metrics.get_queue_depth(), 10);
        assert_eq!(metrics.get_queue_depth_max(), 10);

        metrics.set_queue_depth(3);
        assert_eq!(metrics.get_queue_depth(), 3);
        assert_eq!(metrics.get_queue_depth_max(), 10); // Max stays at 10
    }

    #[test]
    fn test_batch_size_tracking() {
        let metrics = LatencyMetrics::new("test_node").unwrap();

        // Initial batch size avg is 1.00 (100/100)
        assert_eq!(metrics.get_batch_size_avg(), 1.0);

        // Record batch of size 5
        metrics.record_batch_size(5);

        // EMA: 0.1 * 5.0 + 0.9 * 1.0 = 1.4
        let avg = metrics.get_batch_size_avg();
        assert!((avg - 1.4).abs() < 0.01, "Batch avg is {}", avg);

        // Record another batch of size 3
        metrics.record_batch_size(3);

        // EMA: 0.1 * 3.0 + 0.9 * 1.4 = 1.56
        let avg = metrics.get_batch_size_avg();
        assert!((avg - 1.56).abs() < 0.01, "Batch avg is {}", avg);
    }

    #[test]
    fn test_speculation_acceptance_rate() {
        let metrics = LatencyMetrics::new("test_node").unwrap();

        metrics.set_speculation_acceptance_rate(95.5);
        let rate = metrics.get_speculation_acceptance_rate();
        assert!((rate - 95.5).abs() < 0.01, "Rate is {}", rate);

        metrics.set_speculation_acceptance_rate(100.0);
        assert_eq!(metrics.get_speculation_acceptance_rate(), 100.0);
    }

    #[test]
    fn test_reset_window() {
        let metrics = LatencyMetrics::new("test_node").unwrap();

        // Record some data
        for i in 1..=50 {
            metrics.record_latency(i * 1000).unwrap();
        }

        let p99_before = metrics.p99(Window::OneMinute);
        assert!(p99_before > 0);

        // Reset 1-minute window
        metrics.reset_window(Window::OneMinute).unwrap();

        let p99_after = metrics.p99(Window::OneMinute);
        assert_eq!(p99_after, 0); // No data after reset

        // Other windows should still have data
        let p99_5min = metrics.p99(Window::FiveMinutes);
        assert!(p99_5min > 0);
    }

    #[test]
    fn test_prometheus_export_format() {
        let metrics = LatencyMetrics::new("vad_node").unwrap();

        // Record some latencies
        metrics.record_latency(10_000).unwrap(); // 10ms
        metrics.record_latency(50_000).unwrap(); // 50ms
        metrics.record_latency(100_000).unwrap(); // 100ms

        // Set queue depth and other metrics
        metrics.set_queue_depth(7);
        metrics.record_batch_size(3);
        metrics.set_speculation_acceptance_rate(96.5);

        let prometheus_output = metrics.to_prometheus();

        // Verify output contains expected metrics
        assert!(prometheus_output.contains("node_id=\"vad_node\""));
        assert!(prometheus_output.contains("quantile=\"0.5\""));
        assert!(prometheus_output.contains("quantile=\"0.95\""));
        assert!(prometheus_output.contains("quantile=\"0.99\""));
        assert!(prometheus_output.contains("window=\"1min\""));
        assert!(prometheus_output.contains("node_queue_depth{"));
        assert!(prometheus_output.contains("node_batch_size_avg{"));
        assert!(prometheus_output.contains("node_speculation_acceptance_rate{"));
        assert!(prometheus_output.contains("node_total_inputs{"));
    }

    #[test]
    fn test_histogram_range_clamping() {
        let metrics = LatencyMetrics::new("test_node").unwrap();

        // Record value exceeding max range (1 second = 1,000,000μs)
        // Should be clamped to 1,000,000
        metrics.record_latency(5_000_000).unwrap();

        // P99 should be at or near max range (histogram may round up slightly)
        let p99 = metrics.p99(Window::OneMinute);
        assert!(
            p99 <= 1_100_000,
            "P99 should be close to max range, got {}",
            p99
        );

        // Record very small value
        metrics.record_latency(0).unwrap(); // Should be clamped to 1μs

        // After adding more samples, P50 should be reasonable
        for _ in 0..50 {
            metrics.record_latency(500_000).unwrap(); // 500ms
        }

        let p50 = metrics.p50(Window::OneMinute);
        assert!(p50 >= 1, "P50 should be >= 1μs, got {}", p50);
        assert!(p50 < 1_000_000, "P50 should be < 1s, got {}", p50);
    }

    #[test]
    fn test_concurrent_recording() {
        use std::sync::Arc;
        use std::thread;

        let metrics = Arc::new(LatencyMetrics::new("concurrent_node").unwrap());

        // Spawn multiple threads recording concurrently
        let mut handles = vec![];
        for i in 0..10 {
            let m = metrics.clone();
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let latency = (i * 100 + j) * 100; // 0-99,900μs
                    m.record_latency(latency).ok();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify total inputs (10 threads * 100 samples = 1000)
        assert_eq!(metrics.get_total_inputs(), 1000);

        // Verify percentiles are computed
        let p99 = metrics.p99(Window::OneMinute);
        assert!(p99 > 0);
    }

    #[test]
    #[ignore = "perf-sensitive — flaky on shared CI runners; run manually with --ignored"]
    fn test_metrics_overhead_is_minimal() {
        use std::time::Instant;

        let metrics = LatencyMetrics::new("perf_node").unwrap();

        // Measure overhead of 1000 recordings
        let start = Instant::now();
        for i in 1..=1000 {
            metrics.record_latency(i * 10).unwrap();
        }
        let elapsed = start.elapsed();

        // Should be <1ms for 1000 recordings (~1μs per sample)
        assert!(
            elapsed.as_micros() < 1000,
            "Overhead too high: {}μs",
            elapsed.as_micros()
        );
    }
}
