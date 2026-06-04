//! Session Health Aggregation Node
//!
//! Aggregates multiple signal sources into a single health status (ok/degraded/unhealthy).
//! Designed as a multi-input node that consumes events from various analysis nodes.

use crate::data::RuntimeData;
use crate::nodes::{StreamingNode, SyncNodeWrapper, SyncStreamingNode};
use crate::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// Session health states
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    Ok,
    Degraded,
    Unhealthy,
}

impl std::fmt::Display for HealthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthState::Ok => write!(f, "ok"),
            HealthState::Degraded => write!(f, "degraded"),
            HealthState::Unhealthy => write!(f, "unhealthy"),
        }
    }
}

/// Health issue contributors
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthContributor {
    Silence,
    DeadAir,
    Clipping,
    LowVolume,
    Imbalance,
    DeadChannel,
    Drift,
    Freeze,
    CadenceUnstable,
}

impl std::fmt::Display for HealthContributor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthContributor::Silence => write!(f, "silence"),
            HealthContributor::DeadAir => write!(f, "dead_air"),
            HealthContributor::Clipping => write!(f, "clipping"),
            HealthContributor::LowVolume => write!(f, "low_volume"),
            HealthContributor::Imbalance => write!(f, "imbalance"),
            HealthContributor::DeadChannel => write!(f, "dead_channel"),
            HealthContributor::Drift => write!(f, "drift"),
            HealthContributor::Freeze => write!(f, "freeze"),
            HealthContributor::CadenceUnstable => write!(f, "cadence_unstable"),
        }
    }
}

/// Issue severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum IssueSeverity {
    Minor,
    Moderate,
    Severe,
}

impl HealthContributor {
    /// Get the severity of this contributor
    fn severity(&self) -> IssueSeverity {
        match self {
            // Severe: Immediate quality impact
            HealthContributor::Clipping => IssueSeverity::Severe,
            HealthContributor::Freeze => IssueSeverity::Severe,
            HealthContributor::DeadChannel => IssueSeverity::Severe,

            // Moderate: Noticeable quality degradation
            HealthContributor::DeadAir => IssueSeverity::Moderate,
            HealthContributor::Drift => IssueSeverity::Moderate,
            HealthContributor::Imbalance => IssueSeverity::Moderate,

            // Minor: May not be immediately noticeable
            HealthContributor::Silence => IssueSeverity::Minor,
            HealthContributor::LowVolume => IssueSeverity::Minor,
            HealthContributor::CadenceUnstable => IssueSeverity::Minor,
        }
    }
}

/// Configuration for session health node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionHealthConfig {
    /// Emit interval in milliseconds
    #[serde(default = "default_emit_interval_ms")]
    pub emit_interval_ms: u32,

    /// Health score threshold for "degraded"
    #[serde(default = "default_degraded_threshold")]
    pub degraded_threshold: f64,

    /// Health score threshold for "unhealthy"
    #[serde(default = "default_unhealthy_threshold")]
    pub unhealthy_threshold: f64,

    /// Dead air duration (ms) to count as issue
    #[serde(default = "default_dead_air_issue_ms")]
    pub dead_air_issue_ms: u32,

    /// Issue retention time in ms (how long to remember an issue)
    #[serde(default = "default_issue_retention_ms")]
    pub issue_retention_ms: u32,
}

fn default_emit_interval_ms() -> u32 {
    1000
}

fn default_degraded_threshold() -> f64 {
    0.8
}

fn default_unhealthy_threshold() -> f64 {
    0.5
}

fn default_dead_air_issue_ms() -> u32 {
    3000
}

fn default_issue_retention_ms() -> u32 {
    5000
}

impl Default for SessionHealthConfig {
    fn default() -> Self {
        Self {
            emit_interval_ms: default_emit_interval_ms(),
            degraded_threshold: default_degraded_threshold(),
            unhealthy_threshold: default_unhealthy_threshold(),
            dead_air_issue_ms: default_dead_air_issue_ms(),
            issue_retention_ms: default_issue_retention_ms(),
        }
    }
}

/// Tracked issue with timestamp
#[derive(Debug, Clone)]
struct TrackedIssue {
    contributor: HealthContributor,
    first_seen_us: u64,
    last_seen_us: u64,
}

/// Internal state for session health node
struct SessionHealthState {
    /// Currently active issues (contributor -> tracked issue)
    active_issues: HashMap<HealthContributor, TrackedIssue>,
    /// Last emitted health state
    last_state: Option<HealthState>,
    /// Last emit timestamp
    last_emit_us: u64,
    /// Base health score from HealthEmitterNode (if available)
    base_health_score: Option<f64>,
}

impl Default for SessionHealthState {
    fn default() -> Self {
        Self {
            active_issues: HashMap::new(),
            last_state: None,
            last_emit_us: 0,
            base_health_score: None,
        }
    }
}

/// Node that aggregates health signals into a single status
pub struct SessionHealthNode {
    #[allow(dead_code)]
    node_id: String,
    config: SessionHealthConfig,
    state: RwLock<SessionHealthState>,
}

impl SessionHealthNode {
    /// Create a new SessionHealthNode
    pub fn new(node_id: String, config: SessionHealthConfig) -> Self {
        Self {
            node_id,
            config,
            state: RwLock::new(SessionHealthState::default()),
        }
    }

    /// Get current timestamp in microseconds
    fn current_timestamp_us() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0)
    }

    /// Process an incoming event and update state
    fn process_event(&self, json: &Value, current_us: u64, state: &mut SessionHealthState) {
        // Extract event type
        let event_type = json
            .get("event_type")
            .or_else(|| json.get("type"))
            .and_then(|v| v.as_str());

        // Also check _schema for older event formats
        let schema = json.get("_schema").and_then(|v| v.as_str());

        // Process based on event type or schema
        match (event_type, schema) {
            // SilenceDetectorNode events
            (_, Some("silence_event")) | (Some("silence"), _) => {
                if json
                    .get("is_sustained_silence")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    self.add_issue(HealthContributor::Silence, current_us, state);
                }
                if json
                    .get("has_intermittent_dropouts")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    self.add_issue(HealthContributor::Silence, current_us, state);
                }
            }

            // ClippingDetectorNode events
            (_, Some("clipping_event")) | (Some("clipping"), _) => {
                if json
                    .get("is_clipping")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    self.add_issue(HealthContributor::Clipping, current_us, state);
                }
            }

            // AudioLevelNode events
            (_, Some("audio_level_event")) | (Some("audio_level"), _) => {
                if json
                    .get("is_silence")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    self.add_issue(HealthContributor::Silence, current_us, state);
                }
                if json
                    .get("is_low_volume")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    self.add_issue(HealthContributor::LowVolume, current_us, state);
                }
            }

            // ChannelBalanceNode events
            (_, Some("channel_balance_event")) | (Some("channel_balance"), _) => {
                if json
                    .get("has_dead_channel")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    self.add_issue(HealthContributor::DeadChannel, current_us, state);
                } else if json
                    .get("is_imbalanced")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    self.add_issue(HealthContributor::Imbalance, current_us, state);
                }
            }

            // SpeechPresenceNode events
            (Some("speech.presence"), _) => {
                let speech_state = json.get("state").and_then(|v| v.as_str());
                if speech_state == Some("dead_air") {
                    let duration_ms = json
                        .get("duration_ms")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as u32;
                    if duration_ms >= self.config.dead_air_issue_ms {
                        self.add_issue(HealthContributor::DeadAir, current_us, state);
                    }
                }
            }

            // HealthEmitterNode events
            (Some("health"), _) => {
                // Extract base health score
                if let Some(score) = json.get("score").and_then(|v| v.as_f64()) {
                    state.base_health_score = Some(score);
                }

                // Check for specific alerts
                if let Some(alerts) = json.get("alerts").and_then(|v| v.as_array()) {
                    for alert in alerts {
                        if let Some(alert_str) = alert.as_str() {
                            match alert_str {
                                "DRIFT_SLOPE" | "LEAD_JUMP" => {
                                    self.add_issue(HealthContributor::Drift, current_us, state);
                                }
                                "FREEZE" => {
                                    self.add_issue(HealthContributor::Freeze, current_us, state);
                                }
                                "CADENCE_UNSTABLE" => {
                                    self.add_issue(
                                        HealthContributor::CadenceUnstable,
                                        current_us,
                                        state,
                                    );
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }

            (Some("drift"), _) => {
                self.add_issue(HealthContributor::Drift, current_us, state);
            }

            (Some("freeze"), _) => {
                self.add_issue(HealthContributor::Freeze, current_us, state);
            }

            (Some("cadence"), _) => {
                self.add_issue(HealthContributor::CadenceUnstable, current_us, state);
            }

            _ => {
                // Unknown event type, ignore
            }
        }
    }

    /// Add or update an issue
    fn add_issue(
        &self,
        contributor: HealthContributor,
        current_us: u64,
        state: &mut SessionHealthState,
    ) {
        if let Some(issue) = state.active_issues.get_mut(&contributor) {
            issue.last_seen_us = current_us;
        } else {
            state.active_issues.insert(
                contributor,
                TrackedIssue {
                    contributor,
                    first_seen_us: current_us,
                    last_seen_us: current_us,
                },
            );
        }
    }

    /// Clean up expired issues
    fn cleanup_issues(&self, current_us: u64, state: &mut SessionHealthState) {
        let retention_us = self.config.issue_retention_ms as u64 * 1000;
        state
            .active_issues
            .retain(|_, issue| current_us.saturating_sub(issue.last_seen_us) < retention_us);
    }

    /// Calculate aggregate health score
    fn calculate_health_score(&self, state: &SessionHealthState, current_us: u64) -> f64 {
        // Start with base score if available
        let mut score = state.base_health_score.unwrap_or(1.0);

        // Apply penalties for each active issue
        // Longer-standing issues have a greater penalty (up to 2x for issues > 10s old)
        for issue in state.active_issues.values() {
            let base_penalty = match issue.contributor.severity() {
                IssueSeverity::Severe => 0.25,
                IssueSeverity::Moderate => 0.15,
                IssueSeverity::Minor => 0.05,
            };
            // Calculate duration multiplier: issues get worse over time
            // Ramps from 1.0 to 2.0 over 10 seconds
            let duration_us = current_us.saturating_sub(issue.first_seen_us);
            let duration_s = duration_us as f64 / 1_000_000.0;
            let duration_multiplier = (1.0 + (duration_s / 10.0)).min(2.0);
            score -= base_penalty * duration_multiplier;
        }

        score.max(0.0)
    }

    /// Determine health state from score and issues
    fn determine_state(&self, score: f64, state: &SessionHealthState) -> HealthState {
        // Check for any severe issues -> immediate unhealthy
        let has_severe = state
            .active_issues
            .values()
            .any(|i| i.contributor.severity() == IssueSeverity::Severe);

        if has_severe {
            return HealthState::Unhealthy;
        }

        // Check score thresholds
        if score < self.config.unhealthy_threshold {
            HealthState::Unhealthy
        } else if score < self.config.degraded_threshold {
            HealthState::Degraded
        } else if !state.active_issues.is_empty() {
            // Any active issues but good score -> degraded
            HealthState::Degraded
        } else {
            HealthState::Ok
        }
    }

    /// Build the output event
    fn build_event(
        &self,
        health_state: HealthState,
        score: f64,
        state: &SessionHealthState,
        current_us: u64,
    ) -> Value {
        let contributors: Vec<String> = state.active_issues.keys().map(|c| c.to_string()).collect();

        serde_json::json!({
            "event_type": "session.health",
            "_schema": "session_health_v1",
            "state": health_state.to_string(),
            "score": (score * 100.0).round() / 100.0, // Round to 2 decimal places
            "contributors": contributors,
            "active_issues": state.active_issues.len(),
            "timestamp_us": current_us,
        })
    }

    fn process_input(&self, input: RuntimeData) -> Result<RuntimeData, Error> {
        let current_us = Self::current_timestamp_us();

        let mut state = self
            .state
            .write()
            .map_err(|e| Error::Execution(format!("Failed to lock session health state: {}", e)))?;

        // Process the input event
        if let RuntimeData::Json(ref json) = input {
            if !json.is_null() {
                self.process_event(json, current_us, &mut state);
            }
        }

        // Cleanup expired issues
        self.cleanup_issues(current_us, &mut state);

        // Check if we should emit
        let elapsed_us = current_us.saturating_sub(state.last_emit_us);
        let elapsed_ms = elapsed_us / 1000;
        let should_emit = elapsed_ms >= self.config.emit_interval_ms as u64;

        if should_emit {
            let score = self.calculate_health_score(&state, current_us);
            let health_state = self.determine_state(score, &state);

            state.last_state = Some(health_state);
            state.last_emit_us = current_us;

            let event = self.build_event(health_state, score, &state, current_us);
            Ok(RuntimeData::Json(event))
        } else {
            Ok(RuntimeData::Json(Value::Null))
        }
    }
}

// Phase A-Wave 1: `SyncStreamingNode` — state uses `std::sync::RwLock`.
// Multi-input aggregation preserved.
impl SyncStreamingNode for SessionHealthNode {
    fn node_type(&self) -> &str {
        "SessionHealthNode"
    }

    fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        self.process_input(data)
    }

    fn process_multi(&self, inputs: HashMap<String, RuntimeData>) -> Result<RuntimeData, Error> {
        // Process all inputs
        let current_us = Self::current_timestamp_us();

        let mut state = self
            .state
            .write()
            .map_err(|e| Error::Execution(format!("Failed to lock session health state: {}", e)))?;

        for (_key, data) in inputs {
            if let RuntimeData::Json(ref json) = data {
                if !json.is_null() {
                    self.process_event(json, current_us, &mut state);
                }
            }
        }

        // Cleanup expired issues
        self.cleanup_issues(current_us, &mut state);

        // Check if we should emit
        let elapsed_us = current_us.saturating_sub(state.last_emit_us);
        let elapsed_ms = elapsed_us / 1000;
        let should_emit = elapsed_ms >= self.config.emit_interval_ms as u64;

        if should_emit {
            let score = self.calculate_health_score(&state, current_us);
            let health_state = self.determine_state(score, &state);

            state.last_state = Some(health_state);
            state.last_emit_us = current_us;

            let event = self.build_event(health_state, score, &state, current_us);
            Ok(RuntimeData::Json(event))
        } else {
            Ok(RuntimeData::Json(Value::Null))
        }
    }

    fn is_multi_input(&self) -> bool {
        true
    }
}

/// Factory for creating SessionHealthNode instances
pub struct SessionHealthNodeFactory;

impl crate::nodes::StreamingNodeFactory for SessionHealthNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: SessionHealthConfig =
            if params.is_null() || params.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                SessionHealthConfig::default()
            } else {
                serde_json::from_value(params.clone()).unwrap_or_default()
            };

        Ok(Box::new(SyncNodeWrapper(SessionHealthNode::new(
            node_id, config,
        ))))
    }

    fn node_type(&self) -> &str {
        "SessionHealthNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("SessionHealthNode")
                .description("Aggregates session health from multiple upstream health sources into a unified score")
                .category("health")
                .accepts([RuntimeDataType::Json])
                .produces([RuntimeDataType::Json]),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node() -> SessionHealthNode {
        SessionHealthNode::new("test".to_string(), SessionHealthConfig::default())
    }

    #[test]
    fn test_health_state_display() {
        assert_eq!(HealthState::Ok.to_string(), "ok");
        assert_eq!(HealthState::Degraded.to_string(), "degraded");
        assert_eq!(HealthState::Unhealthy.to_string(), "unhealthy");
    }

    #[test]
    fn test_contributor_severity() {
        assert_eq!(
            HealthContributor::Clipping.severity(),
            IssueSeverity::Severe
        );
        assert_eq!(
            HealthContributor::DeadAir.severity(),
            IssueSeverity::Moderate
        );
        assert_eq!(
            HealthContributor::LowVolume.severity(),
            IssueSeverity::Minor
        );
    }

    #[test]
    fn test_config_default() {
        let config = SessionHealthConfig::default();
        assert_eq!(config.emit_interval_ms, 1000);
        assert_eq!(config.degraded_threshold, 0.8);
        assert_eq!(config.unhealthy_threshold, 0.5);
    }

    #[tokio::test]
    async fn test_silence_event_processing() {
        let node = create_test_node();

        let event = serde_json::json!({
            "_schema": "silence_event",
            "is_sustained_silence": true
        });

        let result = node.process(RuntimeData::Json(event)).unwrap();

        // Force an emit by waiting and processing another event
        tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

        let result = node
            .process(RuntimeData::Json(serde_json::json!({})))
            .unwrap();

        if let RuntimeData::Json(json) = result {
            if !json.is_null() {
                assert!(json["contributors"]
                    .as_array()
                    .unwrap()
                    .contains(&Value::String("silence".to_string())));
            }
        }
    }

    #[tokio::test]
    async fn test_clipping_causes_unhealthy() {
        let node = create_test_node();

        let event = serde_json::json!({
            "_schema": "clipping_event",
            "is_clipping": true
        });

        node.process(RuntimeData::Json(event)).unwrap();

        // Force emit
        tokio::time::sleep(tokio::time::Duration::from_millis(1100)).await;

        let result = node
            .process(RuntimeData::Json(serde_json::json!({})))
            .unwrap();

        if let RuntimeData::Json(json) = result {
            if !json.is_null() {
                // Clipping is severe, should be unhealthy
                assert_eq!(json["state"], "unhealthy");
            }
        }
    }

    #[test]
    fn test_health_score_calculation() {
        let node = create_test_node();
        let mut state = SessionHealthState::default();
        let current_us = 0u64;

        // No issues = perfect score
        assert_eq!(node.calculate_health_score(&state, current_us), 1.0);

        // Add minor issue
        state.active_issues.insert(
            HealthContributor::LowVolume,
            TrackedIssue {
                contributor: HealthContributor::LowVolume,
                first_seen_us: 0,
                last_seen_us: 0,
            },
        );
        let score = node.calculate_health_score(&state, current_us);
        assert!((score - 0.95).abs() < 0.01);

        // Add severe issue
        state.active_issues.insert(
            HealthContributor::Clipping,
            TrackedIssue {
                contributor: HealthContributor::Clipping,
                first_seen_us: 0,
                last_seen_us: 0,
            },
        );
        let score = node.calculate_health_score(&state, current_us);
        assert!((score - 0.70).abs() < 0.01);
    }

    #[test]
    fn test_health_score_increases_with_issue_duration() {
        let node = create_test_node();
        let mut state = SessionHealthState::default();

        // Add a minor issue that started at time 0
        state.active_issues.insert(
            HealthContributor::LowVolume,
            TrackedIssue {
                contributor: HealthContributor::LowVolume,
                first_seen_us: 0,
                last_seen_us: 0,
            },
        );

        // At time 0, penalty is base (0.05)
        let score_at_0 = node.calculate_health_score(&state, 0);
        assert!((score_at_0 - 0.95).abs() < 0.01);

        // At time 10s, penalty should be doubled (0.10)
        let score_at_10s = node.calculate_health_score(&state, 10_000_000);
        assert!((score_at_10s - 0.90).abs() < 0.01);

        // Penalty caps at 2x (at 10s+)
        let score_at_20s = node.calculate_health_score(&state, 20_000_000);
        assert!((score_at_20s - 0.90).abs() < 0.01);
    }
}
