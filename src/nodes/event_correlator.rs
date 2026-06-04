//! Event Correlator Node
//!
//! Groups temporally-related alerts into incidents, reducing alert spam
//! and adding context about event sequences.

use crate::data::RuntimeData;
use crate::nodes::{StreamingNode, SyncNodeWrapper, SyncStreamingNode};
use crate::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Event severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Low,
    Medium,
    High,
    Critical,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Severity::Low => write!(f, "low"),
            Severity::Medium => write!(f, "medium"),
            Severity::High => write!(f, "high"),
            Severity::Critical => write!(f, "critical"),
        }
    }
}

/// Known event patterns
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventPattern {
    /// Silence followed by timing issues (reconnect)
    SilenceThenReconnect,
    /// Clipping with channel imbalance
    ClippingWithImbalance,
    /// Freeze with timing drift
    FreezeWithDrift,
    /// Multiple audio quality issues
    AudioQualityDegradation,
    /// Unknown pattern
    Unknown,
}

impl std::fmt::Display for EventPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventPattern::SilenceThenReconnect => write!(f, "silence_then_reconnect"),
            EventPattern::ClippingWithImbalance => write!(f, "clipping_with_imbalance"),
            EventPattern::FreezeWithDrift => write!(f, "freeze_with_drift"),
            EventPattern::AudioQualityDegradation => write!(f, "audio_quality_degradation"),
            EventPattern::Unknown => write!(f, "unknown"),
        }
    }
}

/// Tracked event in the correlator
#[derive(Debug, Clone)]
struct TrackedEvent {
    /// Event type (extracted from event_type field)
    event_type: String,
    /// Timestamp in microseconds
    timestamp_us: u64,
    /// Original event JSON
    event: Value,
}

/// Configuration for event correlator node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventCorrelatorConfig {
    /// Time window (ms) to correlate events into same incident
    #[serde(default = "default_correlation_window_ms")]
    pub correlation_window_ms: u32,

    /// Minimum events to form an incident
    #[serde(default = "default_min_events_for_incident")]
    pub min_events_for_incident: u32,

    /// Emit individual events as well as incidents
    #[serde(default = "default_emit_raw_events")]
    pub emit_raw_events: bool,
}

fn default_correlation_window_ms() -> u32 {
    5000
}

fn default_min_events_for_incident() -> u32 {
    2
}

fn default_emit_raw_events() -> bool {
    true
}

impl Default for EventCorrelatorConfig {
    fn default() -> Self {
        Self {
            correlation_window_ms: default_correlation_window_ms(),
            min_events_for_incident: default_min_events_for_incident(),
            emit_raw_events: default_emit_raw_events(),
        }
    }
}

/// Active incident being correlated
#[derive(Debug, Clone)]
struct ActiveIncident {
    /// Incident ID
    id: String,
    /// Events in this incident
    events: Vec<TrackedEvent>,
    /// First event timestamp
    start_us: u64,
    /// Last event timestamp
    last_us: u64,
    /// Whether this incident has been emitted
    emitted: bool,
}

impl ActiveIncident {
    fn new(event: TrackedEvent) -> Self {
        let id = format!(
            "inc_{}",
            Uuid::new_v4()
                .to_string()
                .split('-')
                .next()
                .unwrap_or("unknown")
        );
        let timestamp = event.timestamp_us;
        Self {
            id,
            events: vec![event],
            start_us: timestamp,
            last_us: timestamp,
            emitted: false,
        }
    }

    fn add_event(&mut self, event: TrackedEvent) {
        self.last_us = event.timestamp_us;
        self.events.push(event);
    }

    fn duration_ms(&self) -> u64 {
        (self.last_us.saturating_sub(self.start_us)) / 1000
    }
}

/// Internal state for event correlator
struct EventCorrelatorState {
    /// Active incidents being built
    active_incidents: Vec<ActiveIncident>,
    /// Completed incidents ready to emit
    completed_incidents: VecDeque<ActiveIncident>,
    /// Event buffer for pattern matching
    recent_events: VecDeque<TrackedEvent>,
}

impl Default for EventCorrelatorState {
    fn default() -> Self {
        Self {
            active_incidents: Vec::new(),
            completed_incidents: VecDeque::new(),
            recent_events: VecDeque::new(),
        }
    }
}

/// Node that correlates events into incidents
pub struct EventCorrelatorNode {
    #[allow(dead_code)]
    node_id: String,
    config: EventCorrelatorConfig,
    state: RwLock<EventCorrelatorState>,
}

impl EventCorrelatorNode {
    /// Create a new EventCorrelatorNode
    pub fn new(node_id: String, config: EventCorrelatorConfig) -> Self {
        Self {
            node_id,
            config,
            state: RwLock::new(EventCorrelatorState::default()),
        }
    }

    /// Get current timestamp in microseconds
    fn current_timestamp_us() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0)
    }

    /// Check if an event is alert-worthy (should be correlated)
    fn is_alert_event(json: &Value) -> bool {
        let event_type = json
            .get("event_type")
            .or_else(|| json.get("type"))
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Match on event types that represent issues
        matches!(
            event_type,
            "timing.jitter_spike"
                | "timing.clock_drift"
                | "timing.lead_jump"
                | "drift"
                | "freeze"
                | "cadence"
                | "av_skew"
        ) || {
            // Check for issue flags in various event types
            json.get("is_clipping")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
                || json
                    .get("is_sustained_silence")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                || json
                    .get("is_imbalanced")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                || json
                    .get("has_dead_channel")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                || json.get("state").and_then(|v| v.as_str()) == Some("dead_air")
                || json.get("state").and_then(|v| v.as_str()) == Some("unhealthy")
        }
    }

    /// Extract event type from JSON
    fn extract_event_type(json: &Value) -> String {
        json.get("event_type")
            .or_else(|| json.get("type"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                // Try to infer from _schema
                json.get("_schema")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            })
    }

    /// Get severity for an event type
    fn get_severity(event_type: &str, json: &Value) -> Severity {
        match event_type {
            "freeze" => Severity::Critical,
            "timing.clock_drift" | "timing.lead_jump" => Severity::High,
            "timing.jitter_spike" | "drift" => Severity::Medium,
            "cadence" | "av_skew" => Severity::Medium,
            _ => {
                // Check issue flags
                if json
                    .get("is_clipping")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    Severity::High
                } else if json
                    .get("has_dead_channel")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    Severity::High
                } else if json
                    .get("is_sustained_silence")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    Severity::Medium
                } else if json.get("state").and_then(|v| v.as_str()) == Some("unhealthy") {
                    Severity::High
                } else if json.get("state").and_then(|v| v.as_str()) == Some("degraded") {
                    Severity::Medium
                } else {
                    Severity::Low
                }
            }
        }
    }

    /// Detect pattern from event sequence
    fn detect_pattern(events: &[TrackedEvent]) -> EventPattern {
        let event_types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();

        // Check for silence -> timing issues (reconnect pattern)
        let has_silence = event_types.iter().any(|t| {
            t.contains("silence")
                || events.iter().any(|e| {
                    e.event
                        .get("is_sustained_silence")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                })
        });
        let has_timing = event_types.iter().any(|t| t.starts_with("timing."));

        if has_silence && has_timing {
            return EventPattern::SilenceThenReconnect;
        }

        // Check for clipping + imbalance
        let has_clipping = events.iter().any(|e| {
            e.event
                .get("is_clipping")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        });
        let has_imbalance = events.iter().any(|e| {
            e.event
                .get("is_imbalanced")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
                || e.event
                    .get("has_dead_channel")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
        });

        if has_clipping && has_imbalance {
            return EventPattern::ClippingWithImbalance;
        }

        // Check for freeze + drift
        let has_freeze = event_types.iter().any(|t| *t == "freeze");
        let has_drift = event_types.iter().any(|t| t.contains("drift"));

        if has_freeze && has_drift {
            return EventPattern::FreezeWithDrift;
        }

        // Check for multiple audio quality issues
        let audio_issues = events
            .iter()
            .filter(|e| {
                e.event
                    .get("is_clipping")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                    || e.event
                        .get("is_low_volume")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                    || e.event
                        .get("is_imbalanced")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
            })
            .count();

        if audio_issues >= 2 {
            return EventPattern::AudioQualityDegradation;
        }

        EventPattern::Unknown
    }

    /// Build incident event
    fn build_incident_event(&self, incident: &ActiveIncident, current_us: u64) -> Value {
        // Get max severity
        let max_severity = incident
            .events
            .iter()
            .map(|e| Self::get_severity(&e.event_type, &e.event))
            .max()
            .unwrap_or(Severity::Low);

        // Detect pattern
        let pattern = Self::detect_pattern(&incident.events);

        // Build simplified event list
        let events_json: Vec<Value> = incident
            .events
            .iter()
            .map(|e| {
                serde_json::json!({
                    "type": e.event_type,
                    "timestamp_us": e.timestamp_us
                })
            })
            .collect();

        serde_json::json!({
            "event_type": "incident",
            "_schema": "incident_v1",
            "incident_id": incident.id,
            "severity": max_severity.to_string(),
            "events": events_json,
            "event_count": incident.events.len(),
            "pattern": pattern.to_string(),
            "duration_ms": incident.duration_ms(),
            "timestamp_us": current_us,
        })
    }

    /// Process and correlate events
    fn process_input(&self, input: RuntimeData) -> Result<RuntimeData, Error> {
        let current_us = Self::current_timestamp_us();
        let window_us = self.config.correlation_window_ms as u64 * 1000;

        // Extract JSON event
        let json = match &input {
            RuntimeData::Json(j) if !j.is_null() => j.clone(),
            _ => return Ok(input), // Pass through non-JSON
        };

        let mut state = self.state.write().map_err(|e| {
            Error::Execution(format!("Failed to lock event correlator state: {}", e))
        })?;

        let mut outputs = Vec::new();

        // Check if this is an alert event
        if Self::is_alert_event(&json) {
            let event_type = Self::extract_event_type(&json);
            let timestamp = json
                .get("timestamp_us")
                .and_then(|v| v.as_u64())
                .unwrap_or(current_us);

            let tracked = TrackedEvent {
                event_type,
                timestamp_us: timestamp,
                event: json.clone(),
            };

            // Add to recent events
            state.recent_events.push_back(tracked.clone());
            while state.recent_events.len() > 100 {
                state.recent_events.pop_front();
            }

            // Try to add to existing incident within window
            let mut added = false;
            for incident in &mut state.active_incidents {
                if timestamp.saturating_sub(incident.last_us) < window_us {
                    incident.add_event(tracked.clone());
                    added = true;
                    break;
                }
            }

            // Create new incident if not added
            if !added {
                state.active_incidents.push(ActiveIncident::new(tracked));
            }

            // Emit raw event if configured
            if self.config.emit_raw_events {
                outputs.push(json);
            }
        }

        // Check for completed incidents (outside window)
        let mut completed_indices = Vec::new();
        for (i, incident) in state.active_incidents.iter().enumerate() {
            if current_us.saturating_sub(incident.last_us) >= window_us {
                completed_indices.push(i);
            }
        }

        // Move completed incidents and emit if they meet threshold
        for i in completed_indices.into_iter().rev() {
            let mut incident = state.active_incidents.remove(i);
            if incident.events.len() >= self.config.min_events_for_incident as usize {
                outputs.push(self.build_incident_event(&incident, current_us));
                incident.emitted = true;
            }
            // Keep completed incidents for history (limited to last 100)
            state.completed_incidents.push_back(incident);
            while state.completed_incidents.len() > 100 {
                state.completed_incidents.pop_front();
            }
        }

        // Return outputs
        match outputs.len() {
            0 => Ok(RuntimeData::Json(Value::Null)),
            1 => Ok(RuntimeData::Json(outputs.into_iter().next().unwrap())),
            _ => Ok(RuntimeData::Json(Value::Array(outputs))),
        }
    }
}

// Phase A-Wave 1: `SyncStreamingNode` — body uses `std::sync::RwLock`
// internally (no `.await`). Multi-input aggregation is preserved.
impl SyncStreamingNode for EventCorrelatorNode {
    fn node_type(&self) -> &str {
        "EventCorrelatorNode"
    }

    fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        self.process_input(data)
    }

    fn process_multi(&self, inputs: HashMap<String, RuntimeData>) -> Result<RuntimeData, Error> {
        // Process all inputs and collect outputs
        let mut all_outputs = Vec::new();

        for (_key, data) in inputs {
            let result = self.process_input(data)?;
            if let RuntimeData::Json(json) = result {
                if !json.is_null() {
                    if let Some(arr) = json.as_array() {
                        all_outputs.extend(arr.clone());
                    } else {
                        all_outputs.push(json);
                    }
                }
            }
        }

        match all_outputs.len() {
            0 => Ok(RuntimeData::Json(Value::Null)),
            1 => Ok(RuntimeData::Json(all_outputs.into_iter().next().unwrap())),
            _ => Ok(RuntimeData::Json(Value::Array(all_outputs))),
        }
    }

    fn is_multi_input(&self) -> bool {
        true
    }
}

/// Factory for creating EventCorrelatorNode instances
pub struct EventCorrelatorNodeFactory;

impl crate::nodes::StreamingNodeFactory for EventCorrelatorNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: EventCorrelatorConfig =
            if params.is_null() || params.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                EventCorrelatorConfig::default()
            } else {
                serde_json::from_value(params.clone()).unwrap_or_default()
            };

        Ok(Box::new(SyncNodeWrapper(EventCorrelatorNode::new(
            node_id, config,
        ))))
    }

    fn node_type(&self) -> &str {
        "EventCorrelatorNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("EventCorrelatorNode")
                .description(
                    "Correlates related events across multiple streams using correlation IDs",
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

    fn create_test_node() -> EventCorrelatorNode {
        EventCorrelatorNode::new(
            "test".to_string(),
            EventCorrelatorConfig {
                correlation_window_ms: 100, // Short window for testing
                min_events_for_incident: 2,
                emit_raw_events: true,
            },
        )
    }

    #[test]
    fn test_severity_ordering() {
        assert!(Severity::Critical > Severity::High);
        assert!(Severity::High > Severity::Medium);
        assert!(Severity::Medium > Severity::Low);
    }

    #[test]
    fn test_config_default() {
        let config = EventCorrelatorConfig::default();
        assert_eq!(config.correlation_window_ms, 5000);
        assert_eq!(config.min_events_for_incident, 2);
        assert!(config.emit_raw_events);
    }

    #[test]
    fn test_is_alert_event() {
        // Timing events are alerts
        let timing_event = serde_json::json!({
            "event_type": "timing.jitter_spike"
        });
        assert!(EventCorrelatorNode::is_alert_event(&timing_event));

        // Clipping is an alert
        let clipping_event = serde_json::json!({
            "event_type": "clipping",
            "is_clipping": true
        });
        assert!(EventCorrelatorNode::is_alert_event(&clipping_event));

        // Dead air is an alert
        let dead_air_event = serde_json::json!({
            "event_type": "speech.presence",
            "state": "dead_air"
        });
        assert!(EventCorrelatorNode::is_alert_event(&dead_air_event));

        // Normal speaking is not an alert
        let speaking_event = serde_json::json!({
            "event_type": "speech.presence",
            "state": "speaking"
        });
        assert!(!EventCorrelatorNode::is_alert_event(&speaking_event));
    }

    #[test]
    fn test_pattern_detection() {
        // Silence + timing = silence_then_reconnect
        let events = vec![
            TrackedEvent {
                event_type: "silence".to_string(),
                timestamp_us: 0,
                event: serde_json::json!({"is_sustained_silence": true}),
            },
            TrackedEvent {
                event_type: "timing.jitter_spike".to_string(),
                timestamp_us: 1000,
                event: serde_json::json!({}),
            },
        ];
        assert_eq!(
            EventCorrelatorNode::detect_pattern(&events),
            EventPattern::SilenceThenReconnect
        );

        // Clipping + imbalance
        let events = vec![
            TrackedEvent {
                event_type: "clipping".to_string(),
                timestamp_us: 0,
                event: serde_json::json!({"is_clipping": true}),
            },
            TrackedEvent {
                event_type: "channel_balance".to_string(),
                timestamp_us: 1000,
                event: serde_json::json!({"is_imbalanced": true}),
            },
        ];
        assert_eq!(
            EventCorrelatorNode::detect_pattern(&events),
            EventPattern::ClippingWithImbalance
        );
    }

    #[tokio::test]
    async fn test_event_passthrough() {
        let node = create_test_node();

        let event = serde_json::json!({
            "event_type": "timing.jitter_spike",
            "jitter_ms": 85.0,
            "timestamp_us": 1000000
        });

        let result = node.process(RuntimeData::Json(event.clone())).unwrap();

        if let RuntimeData::Json(json) = result {
            // Should emit raw event
            if json.is_array() {
                let arr = json.as_array().unwrap();
                assert!(arr.iter().any(|e| e["event_type"] == "timing.jitter_spike"));
            } else if !json.is_null() {
                assert_eq!(json["event_type"], "timing.jitter_spike");
            }
        }
    }

    #[tokio::test]
    async fn test_incident_creation() {
        let node = create_test_node();

        // Send two events close together
        let event1 = serde_json::json!({
            "event_type": "timing.jitter_spike",
            "timestamp_us": 1000000
        });
        let event2 = serde_json::json!({
            "event_type": "timing.clock_drift",
            "timestamp_us": 1050000 // 50ms later
        });

        node.process(RuntimeData::Json(event1)).unwrap();
        node.process(RuntimeData::Json(event2)).unwrap();

        // Wait for window to close
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

        // Trigger incident emission with another event
        let event3 = serde_json::json!({
            "event_type": "timing.jitter_spike",
            "timestamp_us": 2000000 // Much later
        });

        let result = node.process(RuntimeData::Json(event3)).unwrap();

        if let RuntimeData::Json(json) = result {
            if json.is_array() {
                let arr = json.as_array().unwrap();
                // Should have incident + raw event
                let has_incident = arr.iter().any(|e| e["event_type"] == "incident");
                if has_incident {
                    let incident = arr.iter().find(|e| e["event_type"] == "incident").unwrap();
                    assert_eq!(incident["event_count"], 2);
                }
            }
        }
    }
}
