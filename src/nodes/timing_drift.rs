//! Timing Drift Analysis Node
//!
//! Exposes timing metrics from DriftMetrics as streaming node events.
//! Provides jitter and drift analysis for infrastructure debugging.
//!
//! This is a thin wrapper around the existing DriftMetrics infrastructure
//! that makes it available as a standalone streaming node.

use crate::data::RuntimeData;
use crate::executor::drift_metrics::{DriftAlerts, DriftMetrics, DriftThresholds};
use crate::nodes::{StreamingNode, SyncNodeWrapper, SyncStreamingNode};
use crate::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// Configuration for timing drift node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimingDriftConfig {
    /// Jitter spike threshold in milliseconds
    #[serde(default = "default_jitter_threshold_ms")]
    pub jitter_threshold_ms: u32,

    /// Clock drift threshold in ms/s
    #[serde(default = "default_drift_threshold_ms_per_s")]
    pub drift_threshold_ms_per_s: f64,

    /// Emit interval for periodic timing reports in milliseconds
    #[serde(default = "default_emit_interval_ms")]
    pub emit_interval_ms: u32,

    /// Lead/drift threshold in milliseconds
    #[serde(default = "default_lead_threshold_ms")]
    pub lead_threshold_ms: u32,
}

fn default_jitter_threshold_ms() -> u32 {
    50
}

fn default_drift_threshold_ms_per_s() -> f64 {
    5.0
}

fn default_emit_interval_ms() -> u32 {
    1000
}

fn default_lead_threshold_ms() -> u32 {
    100
}

impl Default for TimingDriftConfig {
    fn default() -> Self {
        Self {
            jitter_threshold_ms: default_jitter_threshold_ms(),
            drift_threshold_ms_per_s: default_drift_threshold_ms_per_s(),
            emit_interval_ms: default_emit_interval_ms(),
            lead_threshold_ms: default_lead_threshold_ms(),
        }
    }
}

impl From<&TimingDriftConfig> for DriftThresholds {
    fn from(config: &TimingDriftConfig) -> Self {
        Self {
            slope_threshold_ms_per_s: config.drift_threshold_ms_per_s,
            lead_jump_threshold_us: config.lead_threshold_ms as u64 * 1000,
            samples_to_raise: 3,
            samples_to_clear: 5,
            ..Default::default()
        }
    }
}

/// Jitter sample for tracking inter-arrival variance
#[derive(Debug, Clone)]
struct JitterSample {
    /// Timestamp in microseconds
    timestamp_us: u64,
    /// Inter-arrival time in microseconds
    inter_arrival_us: u64,
}

/// Internal state for timing drift node
struct TimingDriftState {
    /// DriftMetrics instance for core calculations
    drift_metrics: DriftMetrics,
    /// Jitter samples for variance calculation
    jitter_samples: VecDeque<JitterSample>,
    /// Last arrival timestamp for jitter calculation
    last_arrival_us: Option<u64>,
    /// Last emit timestamp
    last_emit_us: u64,
    /// Previous alerts for change detection
    prev_alerts: DriftAlerts,
}

/// Node that analyzes timing drift and jitter
pub struct TimingDriftNode {
    #[allow(dead_code)]
    node_id: String,
    config: TimingDriftConfig,
    state: RwLock<TimingDriftState>,
}

impl TimingDriftNode {
    /// Create a new TimingDriftNode
    pub fn new(node_id: String, config: TimingDriftConfig) -> Self {
        let thresholds = DriftThresholds::from(&config);
        Self {
            node_id: node_id.clone(),
            config,
            state: RwLock::new(TimingDriftState {
                drift_metrics: DriftMetrics::new(node_id, thresholds),
                jitter_samples: VecDeque::with_capacity(100),
                last_arrival_us: None,
                last_emit_us: 0,
                prev_alerts: DriftAlerts::empty(),
            }),
        }
    }

    /// Get current timestamp in microseconds
    fn current_timestamp_us() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0)
    }

    /// Calculate jitter (inter-arrival variance) in milliseconds
    fn calculate_jitter_ms(samples: &VecDeque<JitterSample>) -> f64 {
        if samples.len() < 2 {
            return 0.0;
        }

        // Calculate mean inter-arrival time
        let sum: u64 = samples.iter().map(|s| s.inter_arrival_us).sum();
        let mean = sum as f64 / samples.len() as f64;

        // Calculate variance
        let variance: f64 = samples
            .iter()
            .map(|s| {
                let diff = s.inter_arrival_us as f64 - mean;
                diff * diff
            })
            .sum::<f64>()
            / samples.len() as f64;

        // Return standard deviation in milliseconds
        variance.sqrt() / 1000.0
    }

    /// Build periodic timing report event
    fn build_report_event(&self, state: &TimingDriftState, current_us: u64) -> Value {
        let lead_us = state.drift_metrics.current_lead_us().unwrap_or(0);
        let slope = state.drift_metrics.current_slope_ms_per_s();
        let jitter_ms = Self::calculate_jitter_ms(&state.jitter_samples);
        let cadence_cv = state.drift_metrics.cadence_cv();

        serde_json::json!({
            "event_type": "timing.report",
            "_schema": "timing_report_v1",
            "lead_ms": lead_us as f64 / 1000.0,
            "slope_ms_per_s": (slope * 100.0).round() / 100.0,
            "jitter_ms": (jitter_ms * 100.0).round() / 100.0,
            "cadence_cv": (cadence_cv * 1000.0).round() / 1000.0,
            "timestamp_us": current_us,
        })
    }

    /// Build jitter spike alert event
    fn build_jitter_alert(&self, jitter_ms: f64, current_us: u64) -> Value {
        serde_json::json!({
            "event_type": "timing.jitter_spike",
            "_schema": "timing_alert_v1",
            "jitter_ms": (jitter_ms * 100.0).round() / 100.0,
            "threshold_ms": self.config.jitter_threshold_ms,
            "timestamp_us": current_us,
        })
    }

    /// Build drift alert event
    fn build_drift_alert(&self, slope_ms_per_s: f64, current_us: u64) -> Value {
        serde_json::json!({
            "event_type": "timing.clock_drift",
            "_schema": "timing_alert_v1",
            "slope_ms_per_s": (slope_ms_per_s * 100.0).round() / 100.0,
            "threshold_ms_per_s": self.config.drift_threshold_ms_per_s,
            "timestamp_us": current_us,
        })
    }

    /// Build lead jump alert event
    fn build_lead_jump_alert(&self, lead_ms: f64, current_us: u64) -> Value {
        serde_json::json!({
            "event_type": "timing.lead_jump",
            "_schema": "timing_alert_v1",
            "lead_ms": (lead_ms * 100.0).round() / 100.0,
            "threshold_ms": self.config.lead_threshold_ms,
            "timestamp_us": current_us,
        })
    }

    fn process_input(&self, input: RuntimeData) -> Result<RuntimeData, Error> {
        let current_us = Self::current_timestamp_us();

        // Extract timing from input
        let (media_ts_us, arrival_ts_us) = match &input {
            RuntimeData::Audio {
                timestamp_us,
                arrival_ts_us,
                ..
            } => {
                let media = timestamp_us.unwrap_or(current_us);
                let arrival = arrival_ts_us.unwrap_or(current_us);
                (media, arrival)
            }
            RuntimeData::Video {
                timestamp_us,
                arrival_ts_us,
                ..
            } => {
                let arrival = arrival_ts_us.unwrap_or(current_us);
                (*timestamp_us, arrival)
            }
            _ => {
                // Non-media data, pass through
                return Ok(input);
            }
        };

        let mut state = self
            .state
            .write()
            .map_err(|e| Error::Execution(format!("Failed to lock timing drift state: {}", e)))?;

        // Calculate inter-arrival time for jitter tracking
        if let Some(last_arrival) = state.last_arrival_us {
            let inter_arrival = arrival_ts_us.saturating_sub(last_arrival);
            state.jitter_samples.push_back(JitterSample {
                timestamp_us: arrival_ts_us,
                inter_arrival_us: inter_arrival,
            });

            // Keep only samples within a 5 second window for accurate jitter calculation
            let window_us = 5_000_000; // 5 seconds
            let cutoff = arrival_ts_us.saturating_sub(window_us);
            while let Some(front) = state.jitter_samples.front() {
                if front.timestamp_us < cutoff {
                    state.jitter_samples.pop_front();
                } else {
                    break;
                }
            }
        }
        state.last_arrival_us = Some(arrival_ts_us);

        // Record sample in DriftMetrics
        let alerts_changed = state
            .drift_metrics
            .record_sample(media_ts_us, arrival_ts_us, None);

        // Collect events to emit
        let mut events = Vec::new();

        // Check for new alerts
        if alerts_changed {
            let current_alerts = state.drift_metrics.alerts();
            let new_alerts = current_alerts - state.prev_alerts;

            // Jitter spike alert
            let jitter_ms = Self::calculate_jitter_ms(&state.jitter_samples);
            if jitter_ms > self.config.jitter_threshold_ms as f64 {
                events.push(self.build_jitter_alert(jitter_ms, current_us));
            }

            // Drift slope alert
            if new_alerts.contains(DriftAlerts::DRIFT_SLOPE) {
                let slope = state.drift_metrics.current_slope_ms_per_s();
                events.push(self.build_drift_alert(slope, current_us));
            }

            // Lead jump alert
            if new_alerts.contains(DriftAlerts::LEAD_JUMP) {
                if let Some(lead_us) = state.drift_metrics.current_lead_us() {
                    events.push(self.build_lead_jump_alert(lead_us as f64 / 1000.0, current_us));
                }
            }

            state.prev_alerts = current_alerts;
        }

        // Check if we should emit periodic report
        let elapsed_us = current_us.saturating_sub(state.last_emit_us);
        let elapsed_ms = elapsed_us / 1000;
        let should_emit = elapsed_ms >= self.config.emit_interval_ms as u64;

        if should_emit {
            state.last_emit_us = current_us;
            events.push(self.build_report_event(&state, current_us));
        }

        // Return events
        if events.is_empty() {
            Ok(RuntimeData::Json(Value::Null))
        } else if events.len() == 1 {
            Ok(RuntimeData::Json(events.into_iter().next().unwrap()))
        } else {
            Ok(RuntimeData::Json(Value::Array(events)))
        }
    }
}

// Phase A-Wave 1: `SyncStreamingNode` — body wraps DriftMetrics (sync).
// Wrapped through `SyncNodeWrapper` at the registry boundary.
impl SyncStreamingNode for TimingDriftNode {
    fn node_type(&self) -> &str {
        "TimingDriftNode"
    }

    fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        self.process_input(data)
    }

    fn process_multi(&self, inputs: HashMap<String, RuntimeData>) -> Result<RuntimeData, Error> {
        if let Some((_key, data)) = inputs.into_iter().next() {
            self.process_input(data)
        } else {
            Err(Error::Execution("No input data".to_string()))
        }
    }

    fn is_multi_input(&self) -> bool {
        false
    }
}

/// Factory for creating TimingDriftNode instances
pub struct TimingDriftNodeFactory;

impl crate::nodes::StreamingNodeFactory for TimingDriftNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: TimingDriftConfig =
            if params.is_null() || params.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                TimingDriftConfig::default()
            } else {
                serde_json::from_value(params.clone()).unwrap_or_default()
            };

        Ok(Box::new(SyncNodeWrapper(TimingDriftNode::new(
            node_id, config,
        ))))
    }

    fn node_type(&self) -> &str {
        "TimingDriftNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("TimingDriftNode")
                .description(
                    "Monitors A/V timing drift and cadence stability, emits JSON health events",
                )
                .category("health")
                .accepts([RuntimeDataType::Audio, RuntimeDataType::Video])
                .produces([RuntimeDataType::Json]),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node() -> TimingDriftNode {
        TimingDriftNode::new(
            "test".to_string(),
            TimingDriftConfig {
                emit_interval_ms: 100, // Quick emit for testing
                ..Default::default()
            },
        )
    }

    #[test]
    fn test_config_default() {
        let config = TimingDriftConfig::default();
        assert_eq!(config.jitter_threshold_ms, 50);
        assert_eq!(config.drift_threshold_ms_per_s, 5.0);
        assert_eq!(config.emit_interval_ms, 1000);
        assert_eq!(config.lead_threshold_ms, 100);
    }

    #[test]
    fn test_jitter_calculation() {
        let mut samples = VecDeque::new();

        // Uniform inter-arrival times = low jitter
        for i in 0..10 {
            samples.push_back(JitterSample {
                timestamp_us: i * 100_000,
                inter_arrival_us: 100_000,
            });
        }
        let jitter = TimingDriftNode::calculate_jitter_ms(&samples);
        assert!(jitter < 1.0, "Uniform arrivals should have low jitter");

        // Variable inter-arrival times = high jitter
        samples.clear();
        let intervals = [50_000, 150_000, 80_000, 120_000, 60_000, 140_000];
        for (i, interval) in intervals.iter().enumerate() {
            samples.push_back(JitterSample {
                timestamp_us: i as u64 * 100_000,
                inter_arrival_us: *interval,
            });
        }
        let jitter = TimingDriftNode::calculate_jitter_ms(&samples);
        assert!(jitter > 10.0, "Variable arrivals should have high jitter");
    }

    #[tokio::test]
    async fn test_audio_processing() {
        let node = create_test_node();

        let input = RuntimeData::Audio {
            samples: vec![0.0; 1600].into(),
            sample_rate: 16000,
            channels: 1,
            stream_id: Some("test".to_string()),
            timestamp_us: Some(100_000),
            arrival_ts_us: Some(100_000),
            metadata: None,
        };

        let result = node.process(input).unwrap();

        // First sample won't emit yet
        if let RuntimeData::Json(json) = result {
            // Either null or timing event
            if !json.is_null() {
                assert!(
                    json.get("event_type").is_some(),
                    "Should have event_type if not null"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_periodic_report_emission() {
        let node = create_test_node();

        // Process several samples
        for i in 0..5 {
            let input = RuntimeData::Audio {
                samples: vec![0.0; 1600].into(),
                sample_rate: 16000,
                channels: 1,
                stream_id: Some("test".to_string()),
                timestamp_us: Some(i * 100_000),
                arrival_ts_us: Some(i * 100_000),
                metadata: None,
            };
            node.process(input).unwrap();
        }

        // Wait for emit interval
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

        // Next sample should trigger report
        let input = RuntimeData::Audio {
            samples: vec![0.0; 1600].into(),
            sample_rate: 16000,
            channels: 1,
            stream_id: Some("test".to_string()),
            timestamp_us: Some(500_000),
            arrival_ts_us: Some(500_000),
            metadata: None,
        };

        let result = node.process(input).unwrap();

        if let RuntimeData::Json(json) = result {
            if !json.is_null() {
                // Could be report or array of events
                let event_type = if json.is_array() {
                    json.as_array()
                        .and_then(|arr| arr.first())
                        .and_then(|e| e.get("event_type"))
                        .and_then(|v| v.as_str())
                } else {
                    json.get("event_type").and_then(|v| v.as_str())
                };

                assert!(
                    event_type == Some("timing.report")
                        || event_type == Some("timing.jitter_spike")
                        || event_type == Some("timing.clock_drift"),
                    "Should emit timing event"
                );
            }
        }
    }
}
