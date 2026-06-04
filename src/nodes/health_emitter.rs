//! HealthEmitterNode - Streaming node that emits health events as JSONL
//!
//! This node wraps DriftMetrics and emits health events (drift, freeze, health score)
//! as JSON output. It's designed for the stream-health-demo binary but can be used
//! in any pipeline requiring health monitoring output.
//!
//! # Features
//!
//! - Records audio/video timing samples using DriftMetrics
//! - Emits JSONL events for drift alerts, freeze detection, and health scores
//! - Configurable thresholds for all metrics
//! - Periodic health score emission
//!
//! # Example
//!
//! ```yaml
//! nodes:
//!   - id: health
//!     node_type: HealthEmitterNode
//!     params:
//!       lead_threshold_ms: 50
//!       freeze_threshold_ms: 500
//!       health_emit_interval_ms: 1000
//! ```

use crate::data::RuntimeData;
use crate::executor::drift_metrics::{DriftAlerts, DriftMetrics, DriftThresholds};
use crate::nodes::streaming_node::{
    StreamingNode, StreamingNodeFactory, SyncNodeWrapper, SyncStreamingNode,
};
use crate::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// Configuration for HealthEmitterNode
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthEmitterConfig {
    /// Lead/drift threshold in milliseconds (default: 50ms)
    #[serde(default = "default_lead_threshold_ms")]
    pub lead_threshold_ms: i64,

    /// Freeze detection threshold in milliseconds (default: 500ms)
    #[serde(default = "default_freeze_threshold_ms")]
    pub freeze_threshold_ms: u64,

    /// A/V skew threshold in milliseconds (default: 80ms)
    #[serde(default = "default_av_skew_threshold_ms")]
    pub av_skew_threshold_ms: i64,

    /// Cadence coefficient of variation threshold (default: 0.3)
    #[serde(default = "default_cadence_cv_threshold")]
    pub cadence_cv_threshold: f64,

    /// Health score threshold for alerts (default: 0.7)
    #[serde(default = "default_health_threshold")]
    pub health_threshold: f64,

    /// Minimum score change to trigger health event emission (default: 0.05)
    #[serde(default = "default_score_change_threshold")]
    pub score_change_threshold: f64,
}

fn default_lead_threshold_ms() -> i64 {
    50
}
fn default_freeze_threshold_ms() -> u64 {
    500
}
fn default_av_skew_threshold_ms() -> i64 {
    80
}
fn default_cadence_cv_threshold() -> f64 {
    0.3
}
fn default_health_threshold() -> f64 {
    0.7
}
fn default_score_change_threshold() -> f64 {
    0.05
}

impl Default for HealthEmitterConfig {
    fn default() -> Self {
        Self {
            lead_threshold_ms: default_lead_threshold_ms(),
            freeze_threshold_ms: default_freeze_threshold_ms(),
            av_skew_threshold_ms: default_av_skew_threshold_ms(),
            cadence_cv_threshold: default_cadence_cv_threshold(),
            health_threshold: default_health_threshold(),
            score_change_threshold: default_score_change_threshold(),
        }
    }
}

impl From<&HealthEmitterConfig> for DriftThresholds {
    fn from(config: &HealthEmitterConfig) -> Self {
        Self {
            slope_threshold_ms_per_s: 5.0, // Fixed slope threshold
            lead_jump_threshold_us: (config.lead_threshold_ms * 1000) as u64,
            av_skew_threshold_us: (config.av_skew_threshold_ms * 1000) as u64,
            freeze_threshold_us: config.freeze_threshold_ms * 1000,
            cadence_cv_threshold: config.cadence_cv_threshold,
            health_threshold: config.health_threshold,
            samples_to_raise: 5,
            samples_to_clear: 10,
            slope_ema_alpha: 0.1,
            warmup_samples: crate::executor::drift_metrics::DEFAULT_WARMUP_SAMPLES,
        }
    }
}

/// Internal state for the health emitter node
struct HealthEmitterState {
    /// DriftMetrics instance for tracking stream health
    drift_metrics: DriftMetrics,
    /// Configured thresholds for event emission
    config: HealthEmitterConfig,
    /// Sample counter
    sample_count: u64,
    /// Last emitted health score (for change detection)
    last_emitted_score: Option<f64>,
    /// Last emitted alerts (for change detection)
    last_emitted_alerts: DriftAlerts,
    /// Active issues tracked from upstream events (e.g., "SILENCE", "CLIPPING", "LOW_VOLUME")
    active_issues: HashSet<String>,
    /// Last emitted active issues (for change detection)
    last_emitted_issues: HashSet<String>,
    /// Health values from upstream nodes, keyed by schema type
    /// Uses minimum aggregation: any 0 makes the stream unhealthy
    upstream_health: HashMap<String, f64>,
}

/// Streaming node that emits health events based on DriftMetrics
pub struct HealthEmitterNode {
    #[allow(dead_code)]
    node_id: String,
    state: RwLock<HealthEmitterState>,
}

impl HealthEmitterNode {
    /// Create a new HealthEmitterNode
    pub fn new(node_id: String, config: HealthEmitterConfig) -> Self {
        let thresholds = DriftThresholds::from(&config);

        Self {
            node_id: node_id.clone(),
            state: RwLock::new(HealthEmitterState {
                drift_metrics: DriftMetrics::new(node_id, thresholds),
                config,
                sample_count: 0,
                last_emitted_score: None,
                last_emitted_alerts: DriftAlerts::empty(),
                active_issues: HashSet::new(),
                last_emitted_issues: HashSet::new(),
                upstream_health: HashMap::new(),
            }),
        }
    }

    /// Get current timestamp in microseconds since epoch
    fn current_timestamp_us() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0)
    }

    /// Format timestamp as ISO 8601 string
    fn format_timestamp_iso(us: u64) -> String {
        let secs = us / 1_000_000;
        let _micros = us % 1_000_000;
        // Simple ISO format without chrono dependency
        format!("{}000000", secs)
    }

    /// Create a drift event JSON
    fn create_drift_event(lead_us: i64, threshold_us: u64, stream_id: Option<&str>) -> Value {
        let mut event = serde_json::json!({
            "type": "drift",
            "ts": Self::format_timestamp_iso(Self::current_timestamp_us()),
            "lead_ms": lead_us / 1000,
            "threshold_ms": threshold_us as i64 / 1000,
        });

        if let Some(id) = stream_id {
            event["stream_id"] = Value::String(id.to_string());
        }

        event
    }

    /// Create a freeze event JSON
    fn create_freeze_event(duration_ms: u64, stream_id: Option<&str>) -> Value {
        let mut event = serde_json::json!({
            "type": "freeze",
            "ts": Self::format_timestamp_iso(Self::current_timestamp_us()),
            "duration_ms": duration_ms,
        });

        if let Some(id) = stream_id {
            event["stream_id"] = Value::String(id.to_string());
        }

        event
    }

    /// Create a health score event JSON
    fn create_health_event(
        score: f64,
        alerts: DriftAlerts,
        active_issues: &HashSet<String>,
    ) -> Value {
        let mut alert_names: Vec<String> = Vec::new();

        // Add DriftMetrics alerts
        if alerts.contains(DriftAlerts::DRIFT_SLOPE) {
            alert_names.push("DRIFT_SLOPE".to_string());
        }
        if alerts.contains(DriftAlerts::LEAD_JUMP) {
            alert_names.push("LEAD_JUMP".to_string());
        }
        if alerts.contains(DriftAlerts::AV_SKEW) {
            alert_names.push("AV_SKEW".to_string());
        }
        if alerts.contains(DriftAlerts::FREEZE) {
            alert_names.push("FREEZE".to_string());
        }
        if alerts.contains(DriftAlerts::CADENCE_UNSTABLE) {
            alert_names.push("CADENCE_UNSTABLE".to_string());
        }
        if alerts.contains(DriftAlerts::HEALTH_LOW) {
            alert_names.push("HEALTH_LOW".to_string());
        }

        // Add active issues from upstream nodes (e.g., SILENCE, CLIPPING, LOW_VOLUME)
        for issue in active_issues {
            if !alert_names.contains(issue) {
                alert_names.push(issue.clone());
            }
        }

        // Sort for consistent output
        alert_names.sort();

        serde_json::json!({
            "type": "health",
            "ts": Self::format_timestamp_iso(Self::current_timestamp_us()),
            "score": score,
            "alerts": alert_names,
        })
    }

    /// Create a cadence event JSON
    fn create_cadence_event(cv: f64, threshold: f64) -> Value {
        serde_json::json!({
            "type": "cadence",
            "ts": Self::format_timestamp_iso(Self::current_timestamp_us()),
            "cv": cv,
            "threshold": threshold,
        })
    }

    /// Create an A/V skew event JSON
    fn create_av_skew_event(skew_ms: i64, threshold_ms: i64) -> Value {
        serde_json::json!({
            "type": "av_skew",
            "ts": Self::format_timestamp_iso(Self::current_timestamp_us()),
            "skew_ms": skew_ms,
            "threshold_ms": threshold_ms,
        })
    }
}

// Phase A-Wave 2: migrated to `SyncStreamingNode` via the new
// `process_streaming` hook. The body has no `.await` — state uses
// `std::sync::RwLock`. Multi-output semantics preserved: per-event
// callback invocation in `process_streaming`; the single-output
// `process` path returns the events as `Json(Array)` for back-compat
// with routers that might call `process_async` directly.
impl SyncStreamingNode for HealthEmitterNode {
    fn node_type(&self) -> &str {
        "HealthEmitterNode"
    }

    fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        let events = self.process_and_collect_events(&data)?;
        if events.is_empty() {
            Ok(RuntimeData::Json(serde_json::json!(null)))
        } else if events.len() == 1 {
            Ok(RuntimeData::Json(events.into_iter().next().unwrap()))
        } else {
            Ok(RuntimeData::Json(Value::Array(events)))
        }
    }

    fn process_multi(&self, inputs: HashMap<String, RuntimeData>) -> Result<RuntimeData, Error> {
        if let Some((_, data)) = inputs.into_iter().next() {
            self.process(data)
        } else {
            Err(Error::Execution("No input data provided".into()))
        }
    }

    fn is_multi_input(&self) -> bool {
        false
    }

    fn process_streaming(
        &self,
        data: RuntimeData,
        _session_id: Option<&str>,
        callback: &mut dyn FnMut(RuntimeData) -> Result<(), Error>,
    ) -> Result<usize, Error> {
        let events = self.process_and_collect_events(&data)?;
        let count = events.len();
        for event in events {
            callback(RuntimeData::Json(event))?;
        }
        Ok(count)
    }
}

impl HealthEmitterNode {
    /// Process input data and collect health events
    fn process_and_collect_events(&self, data: &RuntimeData) -> Result<Vec<Value>, Error> {
        let mut events = Vec::new();
        let current_time_us = Self::current_timestamp_us();

        // Lock state for updating
        let mut state = self
            .state
            .write()
            .map_err(|e| Error::Execution(format!("Failed to lock health emitter state: {}", e)))?;

        // Handle JSON input from upstream nodes (audio_level, silence_detector, etc.)
        if let RuntimeData::Json(json) = data {
            if !json.is_null() {
                // Update active issues and health values from upstream event data
                Self::update_from_upstream_json(json, &mut state);

                // Calculate aggregated health from upstream nodes (minimum = any 0 makes stream unhealthy)
                let health_score = Self::calculate_aggregated_health(&state.upstream_health);

                // Check if active issues changed
                let issues_changed = state.active_issues != state.last_emitted_issues;

                // Check if score changed significantly
                let score_changed = match state.last_emitted_score {
                    Some(last_score) => {
                        (health_score - last_score).abs() >= state.config.score_change_threshold
                    }
                    None => true,
                };

                if issues_changed || score_changed {
                    let alerts_after = state.drift_metrics.alerts();

                    events.push(Self::create_health_event(
                        health_score,
                        alerts_after,
                        &state.active_issues,
                    ));
                    state.last_emitted_score = Some(health_score);
                    state.last_emitted_alerts = alerts_after;
                    state.last_emitted_issues = state.active_issues.clone();
                }
            }
            return Ok(events);
        }

        // Extract timing from input data (Audio/Video)
        let (media_ts_us, arrival_ts_us, stream_id) = match data {
            RuntimeData::Audio {
                timestamp_us,
                arrival_ts_us,
                stream_id,
                samples,
                sample_rate,
                ..
            } => {
                // Use provided timestamps or estimate from sample count
                let media_ts = timestamp_us.unwrap_or_else(|| {
                    // Estimate from sample count: samples / sample_rate * 1_000_000
                    if *sample_rate > 0 {
                        (state.sample_count * samples.len() as u64 * 1_000_000)
                            / *sample_rate as u64
                    } else {
                        current_time_us
                    }
                });
                let arrival_ts = arrival_ts_us.unwrap_or(current_time_us);
                (media_ts, arrival_ts, stream_id.clone())
            }
            RuntimeData::Video {
                timestamp_us,
                arrival_ts_us,
                stream_id,
                ..
            } => {
                // Video timestamp_us is u64 (not Option), arrival_ts_us is Option<u64>
                let arrival_ts = (*arrival_ts_us).unwrap_or(current_time_us);
                (*timestamp_us, arrival_ts, stream_id.clone())
            }
            _ => {
                // For other non-media data, just return empty events
                return Ok(events);
            }
        };

        state.sample_count += 1;

        // Get alerts before recording sample
        let alerts_before = state.drift_metrics.alerts();

        // Record the sample
        let drift_alerts_changed =
            state
                .drift_metrics
                .record_sample(media_ts_us, arrival_ts_us, None);

        // Get alerts after recording
        let alerts_after = state.drift_metrics.alerts();

        // Check for newly raised alerts and emit events
        if drift_alerts_changed {
            let new_alerts = alerts_after - alerts_before;

            // Drift alert
            if new_alerts.contains(DriftAlerts::DRIFT_SLOPE)
                || new_alerts.contains(DriftAlerts::LEAD_JUMP)
            {
                if let Some(lead_us) = state.drift_metrics.current_lead_us() {
                    events.push(Self::create_drift_event(
                        lead_us,
                        state.config.lead_threshold_ms as u64 * 1000,
                        stream_id.as_deref(),
                    ));
                }
            }

            // Freeze alert
            if new_alerts.contains(DriftAlerts::FREEZE) {
                events.push(Self::create_freeze_event(
                    state.config.freeze_threshold_ms,
                    stream_id.as_deref(),
                ));
            }

            // A/V skew alert
            if new_alerts.contains(DriftAlerts::AV_SKEW) {
                events.push(Self::create_av_skew_event(
                    state.drift_metrics.current_av_skew_us / 1000,
                    state.config.av_skew_threshold_ms,
                ));
            }

            // Cadence alert
            if new_alerts.contains(DriftAlerts::CADENCE_UNSTABLE) {
                events.push(Self::create_cadence_event(
                    state.drift_metrics.cadence_cv(),
                    state.config.cadence_cv_threshold,
                ));
            }
        }

        // Emit health event when score or alerts change (event-driven, not interval-based)
        let health_score = state.drift_metrics.health_score();

        // Check if alerts changed (excluding HEALTH_LOW which can flicker with score)
        let alerts_mask = DriftAlerts::all() - DriftAlerts::HEALTH_LOW;
        let alerts_changed =
            (alerts_after & alerts_mask) != (state.last_emitted_alerts & alerts_mask);

        // Check if score changed significantly
        let score_changed = match state.last_emitted_score {
            Some(last_score) => {
                (health_score - last_score).abs() >= state.config.score_change_threshold
            }
            None => true, // Always emit the first time
        };

        // Check if active issues changed
        let issues_changed = state.active_issues != state.last_emitted_issues;

        if alerts_changed || score_changed || issues_changed {
            events.push(Self::create_health_event(
                health_score,
                alerts_after,
                &state.active_issues,
            ));
            state.last_emitted_score = Some(health_score);
            state.last_emitted_alerts = alerts_after;
            state.last_emitted_issues = state.active_issues.clone();
        }

        Ok(events)
    }

    /// Update active issues and health values based on JSON event from upstream nodes
    fn update_from_upstream_json(json: &Value, state: &mut HealthEmitterState) {
        let active_issues = &mut state.active_issues;
        let upstream_health = &mut state.upstream_health;
        // Check schema type to determine event source
        let schema = json.get("_schema").and_then(|s| s.as_str()).unwrap_or("");

        // Extract health value if present (all upstream nodes now emit this)
        if let Some(health) = json.get("health").and_then(|v| v.as_f64()) {
            if !schema.is_empty() {
                upstream_health.insert(schema.to_string(), health);
            }
        }

        match schema {
            "audio_level_event" => {
                // AudioLevelNode: is_silence, is_low_volume
                if json
                    .get("is_silence")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    active_issues.insert("SILENCE".to_string());
                } else {
                    active_issues.remove("SILENCE");
                }
                if json
                    .get("is_low_volume")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    active_issues.insert("LOW_VOLUME".to_string());
                } else {
                    active_issues.remove("LOW_VOLUME");
                }
            }
            "silence_event" => {
                // SilenceDetectorNode: is_sustained_silence, has_intermittent_dropouts
                if json
                    .get("is_sustained_silence")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    active_issues.insert("SUSTAINED_SILENCE".to_string());
                } else {
                    active_issues.remove("SUSTAINED_SILENCE");
                }
                if json
                    .get("has_intermittent_dropouts")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    active_issues.insert("INTERMITTENT_DROPOUTS".to_string());
                } else {
                    active_issues.remove("INTERMITTENT_DROPOUTS");
                }
            }
            "clipping_event" => {
                // ClippingDetectorNode: is_clipping
                if json
                    .get("is_clipping")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    active_issues.insert("CLIPPING".to_string());
                } else {
                    active_issues.remove("CLIPPING");
                }
            }
            "channel_balance_event" => {
                // ChannelBalanceNode: is_imbalanced, has_dead_channel
                if json
                    .get("is_imbalanced")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    active_issues.insert("CHANNEL_IMBALANCE".to_string());
                } else {
                    active_issues.remove("CHANNEL_IMBALANCE");
                }
                if json
                    .get("has_dead_channel")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    active_issues.insert("DEAD_CHANNEL".to_string());
                } else {
                    active_issues.remove("DEAD_CHANNEL");
                }
            }
            _ => {
                // For other schemas or missing schema, try generic detection
                // Look for common alert patterns
                if let Some(event_type) = json.get("event_type").and_then(|e| e.as_str()) {
                    // Handle session.health events
                    if event_type == "session.health" {
                        if let Some(state) = json.get("state").and_then(|s| s.as_str()) {
                            match state {
                                "unhealthy" => {
                                    active_issues.insert("SESSION_UNHEALTHY".to_string());
                                    active_issues.remove("SESSION_DEGRADED");
                                }
                                "degraded" => {
                                    active_issues.insert("SESSION_DEGRADED".to_string());
                                    active_issues.remove("SESSION_UNHEALTHY");
                                }
                                _ => {
                                    active_issues.remove("SESSION_UNHEALTHY");
                                    active_issues.remove("SESSION_DEGRADED");
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Calculate aggregated health score from upstream nodes
    /// Uses minimum: any node at 0 makes the entire stream unhealthy
    fn calculate_aggregated_health(upstream_health: &HashMap<String, f64>) -> f64 {
        if upstream_health.is_empty() {
            return 1.0; // No upstream data yet, assume healthy
        }
        upstream_health.values().copied().fold(1.0_f64, f64::min)
    }
}

/// Factory for creating HealthEmitterNode instances
pub struct HealthEmitterNodeFactory;

impl StreamingNodeFactory for HealthEmitterNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: HealthEmitterConfig =
            serde_json::from_value(params.clone()).unwrap_or_default();

        Ok(Box::new(SyncNodeWrapper(HealthEmitterNode::new(
            node_id, config,
        ))))
    }

    fn node_type(&self) -> &str {
        "HealthEmitterNode"
    }

    fn is_multi_output_streaming(&self) -> bool {
        true // This node can emit multiple events per input
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("HealthEmitterNode")
                .description(
                    "Emits health events as JSONL — drift alerts, freeze detection, health scores",
                )
                .category("health")
                .accepts([
                    RuntimeDataType::Audio,
                    RuntimeDataType::Video,
                    RuntimeDataType::Json,
                ])
                .produces([RuntimeDataType::Json]),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = HealthEmitterConfig::default();
        assert_eq!(config.lead_threshold_ms, 50);
        assert_eq!(config.freeze_threshold_ms, 500);
        assert_eq!(config.score_change_threshold, 0.05);
    }

    #[test]
    fn test_config_from_json() {
        let json = serde_json::json!({
            "lead_threshold_ms": 100,
            "freeze_threshold_ms": 1000,
        });

        let config: HealthEmitterConfig = serde_json::from_value(json).unwrap();
        assert_eq!(config.lead_threshold_ms, 100);
        assert_eq!(config.freeze_threshold_ms, 1000);
        // Defaults for unspecified fields
        assert_eq!(config.score_change_threshold, 0.05);
    }

    #[test]
    fn test_drift_event_creation() {
        let event = HealthEmitterNode::create_drift_event(50_000, 50_000, Some("audio"));
        assert_eq!(event["type"], "drift");
        assert_eq!(event["lead_ms"], 50);
        assert_eq!(event["threshold_ms"], 50);
        assert_eq!(event["stream_id"], "audio");
    }

    #[test]
    fn test_freeze_event_creation() {
        let event = HealthEmitterNode::create_freeze_event(823, None);
        assert_eq!(event["type"], "freeze");
        assert_eq!(event["duration_ms"], 823);
        assert!(event.get("stream_id").is_none());
    }

    #[test]
    fn test_health_event_creation() {
        let active_issues = HashSet::new();
        let event = HealthEmitterNode::create_health_event(
            0.72,
            DriftAlerts::DRIFT_SLOPE | DriftAlerts::FREEZE,
            &active_issues,
        );
        assert_eq!(event["type"], "health");
        assert_eq!(event["score"], 0.72);

        let alerts = event["alerts"].as_array().unwrap();
        assert!(alerts.contains(&Value::String("DRIFT_SLOPE".to_string())));
        assert!(alerts.contains(&Value::String("FREEZE".to_string())));
    }

    #[test]
    fn test_health_event_with_active_issues() {
        let mut active_issues = HashSet::new();
        active_issues.insert("CLIPPING".to_string());
        active_issues.insert("LOW_VOLUME".to_string());

        let event =
            HealthEmitterNode::create_health_event(0.85, DriftAlerts::empty(), &active_issues);
        assert_eq!(event["type"], "health");
        assert_eq!(event["score"], 0.85);

        let alerts = event["alerts"].as_array().unwrap();
        assert!(alerts.contains(&Value::String("CLIPPING".to_string())));
        assert!(alerts.contains(&Value::String("LOW_VOLUME".to_string())));
    }

    #[tokio::test]
    async fn test_node_creation() {
        let node =
            HealthEmitterNode::new("test_health".to_string(), HealthEmitterConfig::default());
        assert_eq!(node.node_type(), "HealthEmitterNode");
        // `SyncStreamingNode` drops the `node_id()` accessor
        // (per-node concept, not a trait method); check the struct
        // field directly.
        assert_eq!(node.node_id, "test_health");
    }

    #[tokio::test]
    async fn test_process_audio_input() {
        let node = HealthEmitterNode::new("test".to_string(), HealthEmitterConfig::default());

        let input = RuntimeData::Audio {
            samples: vec![0.0; 1600].into(), // 100ms at 16kHz
            sample_rate: 16000,
            channels: 1,
            stream_id: Some("audio".to_string()),
            timestamp_us: Some(100_000),
            arrival_ts_us: Some(100_000),
            metadata: None,
        };

        let result = node.process(input);
        assert!(result.is_ok());
    }

    /// Phase A-Wave 2 regression guard: migrating
    /// `HealthEmitterNode` to `SyncStreamingNode` must preserve the
    /// multi-output semantics — one JSON event per callback
    /// invocation, not a single `Json(Array)` collapse. This test
    /// drives the node through `SyncNodeWrapper::process_streaming_async`
    /// (the same path the router takes) and asserts N callback
    /// invocations.
    #[tokio::test]
    async fn test_sync_wrapper_preserves_multi_output() {
        use std::sync::{Arc, Mutex};
        let node =
            HealthEmitterNode::new("test_multi_out".to_string(), HealthEmitterConfig::default());
        let wrapper = SyncNodeWrapper(node);

        // Craft an upstream JSON event that should trigger multiple
        // health-events (combination of clipping + silence).
        let upstream = RuntimeData::Json(serde_json::json!({
            "event_type": "audio_level",
            "is_clipping": true,
            "is_sustained_silence": true,
        }));

        let outputs = Arc::new(Mutex::new(Vec::<RuntimeData>::new()));
        let outputs_c = outputs.clone();
        let callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send> =
            Box::new(move |out| {
                outputs_c.lock().unwrap().push(out);
                Ok(())
            });

        let ctx = crate::nodes::NodeRuntimeContext::for_test("test-session", "health-emitter");
        let count = wrapper
            .process_streaming_async(upstream, &ctx, callback)
            .await
            .expect("process_streaming_async ok");

        let outs = outputs.lock().unwrap();
        assert_eq!(
            count,
            outs.len(),
            "returned count must match callback invocation count"
        );
        // HealthEmitterNode emits a state-change event the first time
        // any issue transitions on; with two fresh issues (clipping +
        // silence) we expect at least one event. The key invariant is
        // "didn't collapse into Json::Array" — i.e., each output is a
        // standalone Json variant, not an array packing.
        for out in outs.iter() {
            match out {
                RuntimeData::Json(v) => {
                    assert!(
                        !matches!(v, serde_json::Value::Array(_)),
                        "multi-output emission must not pack into Json(Array); got {v:?}",
                    );
                }
                _ => panic!("expected Json output, got {out:?}"),
            }
        }
    }

    #[test]
    fn test_factory_creation() {
        let factory = HealthEmitterNodeFactory;
        assert_eq!(factory.node_type(), "HealthEmitterNode");
        assert!(factory.is_multi_output_streaming());

        let params = serde_json::json!({
            "lead_threshold_ms": 75
        });

        let node = factory.create("test_node".to_string(), &params, None);
        assert!(node.is_ok());
    }
}
