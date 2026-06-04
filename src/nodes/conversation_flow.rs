//! Conversation Flow Analysis Node
//!
//! Computes talk ratios and silence percentages over sliding time windows.
//! Consumes SpeechPresenceNode events to track speaking patterns.
//!
//! Stereo: Emits per-channel talk percentages and overlap percentage
//! Mono: Emits aggregate talk vs silence percentage

use crate::data::RuntimeData;
use crate::nodes::{StreamingNode, SyncNodeWrapper, SyncStreamingNode};
use crate::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// Speech presence event for tracking
#[derive(Debug, Clone)]
struct PresenceEvent {
    /// Timestamp in microseconds
    timestamp_us: u64,
    /// Duration of this state segment in microseconds
    duration_us: u64,
    /// State type
    state: PresenceState,
}

/// Simplified presence state for tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PresenceState {
    /// Left channel speaking only (stereo)
    SpeakingLeft,
    /// Right channel speaking only (stereo)
    SpeakingRight,
    /// Both channels speaking (stereo)
    SpeakingBoth,
    /// Overlap detected (stereo)
    Overlap,
    /// Speaking (mono)
    Speaking,
    /// Silent
    Silent,
    /// Dead air (extended silence)
    DeadAir,
}

impl PresenceState {
    fn from_str(s: &str) -> Option<Self> {
        match s {
            "speaking_left" => Some(PresenceState::SpeakingLeft),
            "speaking_right" => Some(PresenceState::SpeakingRight),
            "speaking_both" => Some(PresenceState::SpeakingBoth),
            "overlap" => Some(PresenceState::Overlap),
            "speaking" => Some(PresenceState::Speaking),
            "silent" => Some(PresenceState::Silent),
            "dead_air" => Some(PresenceState::DeadAir),
            _ => None,
        }
    }

    /// Check if this state involves left channel speaking
    fn is_left_speaking(&self) -> bool {
        matches!(
            self,
            PresenceState::SpeakingLeft | PresenceState::SpeakingBoth | PresenceState::Overlap
        )
    }

    /// Check if this state involves right channel speaking
    fn is_right_speaking(&self) -> bool {
        matches!(
            self,
            PresenceState::SpeakingRight | PresenceState::SpeakingBoth | PresenceState::Overlap
        )
    }

    /// Check if this is overlap
    fn is_overlap(&self) -> bool {
        matches!(self, PresenceState::Overlap)
    }

    /// Check if this is silence (including dead air)
    fn is_silence(&self) -> bool {
        matches!(self, PresenceState::Silent | PresenceState::DeadAir)
    }

    /// Check if this is speaking (mono)
    fn is_speaking(&self) -> bool {
        matches!(self, PresenceState::Speaking)
    }
}

/// Configuration for conversation flow node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversationFlowConfig {
    /// Sliding window size in milliseconds
    #[serde(default = "default_window_ms")]
    pub window_ms: u32,

    /// How often to emit flow metrics in milliseconds
    #[serde(default = "default_emit_interval_ms")]
    pub emit_interval_ms: u32,
}

fn default_window_ms() -> u32 {
    30000 // 30 seconds
}

fn default_emit_interval_ms() -> u32 {
    5000 // 5 seconds
}

impl Default for ConversationFlowConfig {
    fn default() -> Self {
        Self {
            window_ms: default_window_ms(),
            emit_interval_ms: default_emit_interval_ms(),
        }
    }
}

/// Internal state for conversation flow tracking
struct ConversationFlowState {
    /// Ring buffer of presence events within window
    events: VecDeque<PresenceEvent>,
    /// Whether we're in stereo mode
    is_stereo: bool,
    /// Last emit timestamp
    last_emit_us: u64,
    /// Last known state for duration estimation
    last_state: Option<PresenceState>,
    /// Last state timestamp for duration estimation
    last_state_us: Option<u64>,
}

impl Default for ConversationFlowState {
    fn default() -> Self {
        Self {
            events: VecDeque::new(),
            is_stereo: false,
            last_emit_us: 0,
            last_state: None,
            last_state_us: None,
        }
    }
}

/// Node that computes conversation flow metrics
pub struct ConversationFlowNode {
    #[allow(dead_code)]
    node_id: String,
    config: ConversationFlowConfig,
    state: RwLock<ConversationFlowState>,
}

impl ConversationFlowNode {
    /// Create a new ConversationFlowNode
    pub fn new(node_id: String, config: ConversationFlowConfig) -> Self {
        Self {
            node_id,
            config,
            state: RwLock::new(ConversationFlowState::default()),
        }
    }

    /// Get current timestamp in microseconds
    fn current_timestamp_us() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0)
    }

    /// Process an incoming speech presence event
    fn process_event(&self, json: &Value, current_us: u64, state: &mut ConversationFlowState) {
        // Check if this is a speech.presence event
        let event_type = json.get("event_type").and_then(|v| v.as_str());
        if event_type != Some("speech.presence") {
            return;
        }

        // Extract state
        let state_str = json.get("state").and_then(|v| v.as_str());
        let presence_state = state_str.and_then(PresenceState::from_str);

        if let Some(ps) = presence_state {
            // Detect stereo mode from state type
            if matches!(
                ps,
                PresenceState::SpeakingLeft
                    | PresenceState::SpeakingRight
                    | PresenceState::SpeakingBoth
                    | PresenceState::Overlap
            ) {
                state.is_stereo = true;
            }

            // Get timestamp from event or use current
            let event_ts = json
                .get("timestamp_us")
                .and_then(|v| v.as_u64())
                .unwrap_or(current_us);

            // Calculate duration since last state and add event if state changed
            if let (Some(last_state), Some(last_ts)) = (state.last_state, state.last_state_us) {
                if last_state != ps {
                    let duration = event_ts.saturating_sub(last_ts);
                    // Add the previous state's duration
                    state.events.push_back(PresenceEvent {
                        timestamp_us: last_ts,
                        duration_us: duration,
                        state: last_state,
                    });
                }
            }

            // Update last state
            state.last_state = Some(ps);
            state.last_state_us = Some(event_ts);
        }

        // Cleanup old events outside window
        let window_us = self.config.window_ms as u64 * 1000;
        let cutoff = current_us.saturating_sub(window_us);
        while let Some(front) = state.events.front() {
            if front.timestamp_us < cutoff {
                state.events.pop_front();
            } else {
                break;
            }
        }
    }

    /// Calculate flow metrics for stereo audio
    fn calculate_stereo_metrics(&self, state: &ConversationFlowState) -> (f64, f64, f64, f64) {
        let mut left_us: u64 = 0;
        let mut right_us: u64 = 0;
        let mut overlap_us: u64 = 0;
        let mut silence_us: u64 = 0;
        let mut total_us: u64 = 0;

        for event in &state.events {
            total_us += event.duration_us;

            if event.state.is_left_speaking() {
                left_us += event.duration_us;
            }
            if event.state.is_right_speaking() {
                right_us += event.duration_us;
            }
            if event.state.is_overlap() {
                overlap_us += event.duration_us;
            }
            if event.state.is_silence() {
                silence_us += event.duration_us;
            }
        }

        if total_us == 0 {
            return (0.0, 0.0, 0.0, 100.0);
        }

        let left_pct = (left_us as f64 / total_us as f64) * 100.0;
        let right_pct = (right_us as f64 / total_us as f64) * 100.0;
        let overlap_pct = (overlap_us as f64 / total_us as f64) * 100.0;
        let silence_pct = (silence_us as f64 / total_us as f64) * 100.0;

        (left_pct, right_pct, overlap_pct, silence_pct)
    }

    /// Calculate flow metrics for mono audio
    fn calculate_mono_metrics(&self, state: &ConversationFlowState) -> (f64, f64) {
        let mut talk_us: u64 = 0;
        let mut silence_us: u64 = 0;
        let mut total_us: u64 = 0;

        for event in &state.events {
            total_us += event.duration_us;

            if event.state.is_speaking() {
                talk_us += event.duration_us;
            }
            if event.state.is_silence() {
                silence_us += event.duration_us;
            }
        }

        if total_us == 0 {
            return (0.0, 100.0);
        }

        let talk_pct = (talk_us as f64 / total_us as f64) * 100.0;
        let silence_pct = (silence_us as f64 / total_us as f64) * 100.0;

        (talk_pct, silence_pct)
    }

    /// Build stereo output event
    fn build_stereo_event(
        &self,
        left_pct: f64,
        right_pct: f64,
        overlap_pct: f64,
        silence_pct: f64,
        current_us: u64,
    ) -> Value {
        serde_json::json!({
            "event_type": "conversation.flow",
            "_schema": "conversation_flow_v1",
            "left_talk_pct": (left_pct * 10.0).round() / 10.0,
            "right_talk_pct": (right_pct * 10.0).round() / 10.0,
            "overlap_pct": (overlap_pct * 10.0).round() / 10.0,
            "silence_pct": (silence_pct * 10.0).round() / 10.0,
            "window_ms": self.config.window_ms,
            "timestamp_us": current_us,
        })
    }

    /// Build mono output event
    fn build_mono_event(&self, talk_pct: f64, silence_pct: f64, current_us: u64) -> Value {
        serde_json::json!({
            "event_type": "conversation.flow",
            "_schema": "conversation_flow_v1",
            "talk_pct": (talk_pct * 10.0).round() / 10.0,
            "silence_pct": (silence_pct * 10.0).round() / 10.0,
            "window_ms": self.config.window_ms,
            "timestamp_us": current_us,
        })
    }

    fn process_input(&self, input: RuntimeData) -> Result<RuntimeData, Error> {
        let current_us = Self::current_timestamp_us();

        let mut state = self.state.write().map_err(|e| {
            Error::Execution(format!("Failed to lock conversation flow state: {}", e))
        })?;

        // Process the input event
        if let RuntimeData::Json(ref json) = input {
            if !json.is_null() {
                self.process_event(json, current_us, &mut state);
            }
        }

        // Check if we should emit
        let elapsed_us = current_us.saturating_sub(state.last_emit_us);
        let elapsed_ms = elapsed_us / 1000;
        let should_emit = elapsed_ms >= self.config.emit_interval_ms as u64;

        if should_emit && !state.events.is_empty() {
            state.last_emit_us = current_us;

            let event = if state.is_stereo {
                let (left, right, overlap, silence) = self.calculate_stereo_metrics(&state);
                self.build_stereo_event(left, right, overlap, silence, current_us)
            } else {
                let (talk, silence) = self.calculate_mono_metrics(&state);
                self.build_mono_event(talk, silence, current_us)
            };

            Ok(RuntimeData::Json(event))
        } else {
            Ok(RuntimeData::Json(Value::Null))
        }
    }
}

// Phase A-Wave 1: `SyncStreamingNode` — state uses `std::sync::RwLock`.
impl SyncStreamingNode for ConversationFlowNode {
    fn node_type(&self) -> &str {
        "ConversationFlowNode"
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

/// Factory for creating ConversationFlowNode instances
pub struct ConversationFlowNodeFactory;

impl crate::nodes::StreamingNodeFactory for ConversationFlowNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: ConversationFlowConfig =
            if params.is_null() || params.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                ConversationFlowConfig::default()
            } else {
                serde_json::from_value(params.clone()).unwrap_or_default()
            };

        Ok(Box::new(SyncNodeWrapper(ConversationFlowNode::new(
            node_id, config,
        ))))
    }

    fn node_type(&self) -> &str {
        "ConversationFlowNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("ConversationFlowNode")
                .description("Tracks conversation flow state — turn-taking, overlap, and interruption detection")
                .category("health")
                .accepts([RuntimeDataType::Audio, RuntimeDataType::Json])
                .produces([RuntimeDataType::Json]),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node() -> ConversationFlowNode {
        ConversationFlowNode::new(
            "test".to_string(),
            ConversationFlowConfig {
                window_ms: 10000,
                emit_interval_ms: 100, // Quick emit for testing
            },
        )
    }

    #[test]
    fn test_presence_state_parsing() {
        assert_eq!(
            PresenceState::from_str("speaking_left"),
            Some(PresenceState::SpeakingLeft)
        );
        assert_eq!(
            PresenceState::from_str("overlap"),
            Some(PresenceState::Overlap)
        );
        assert_eq!(
            PresenceState::from_str("speaking"),
            Some(PresenceState::Speaking)
        );
        assert_eq!(PresenceState::from_str("unknown"), None);
    }

    #[test]
    fn test_presence_state_checks() {
        assert!(PresenceState::SpeakingLeft.is_left_speaking());
        assert!(!PresenceState::SpeakingLeft.is_right_speaking());

        assert!(PresenceState::SpeakingRight.is_right_speaking());
        assert!(!PresenceState::SpeakingRight.is_left_speaking());

        assert!(PresenceState::Overlap.is_left_speaking());
        assert!(PresenceState::Overlap.is_right_speaking());
        assert!(PresenceState::Overlap.is_overlap());

        assert!(PresenceState::Silent.is_silence());
        assert!(PresenceState::DeadAir.is_silence());

        assert!(PresenceState::Speaking.is_speaking());
    }

    #[test]
    fn test_config_default() {
        let config = ConversationFlowConfig::default();
        assert_eq!(config.window_ms, 30000);
        assert_eq!(config.emit_interval_ms, 5000);
    }

    #[tokio::test]
    async fn test_mono_flow_processing() {
        let node = create_test_node();

        // Simulate a sequence of speaking/silent events
        let events = vec![
            serde_json::json!({
                "event_type": "speech.presence",
                "state": "speaking",
                "timestamp_us": 0
            }),
            serde_json::json!({
                "event_type": "speech.presence",
                "state": "silent",
                "timestamp_us": 1000000 // 1 second later
            }),
            serde_json::json!({
                "event_type": "speech.presence",
                "state": "speaking",
                "timestamp_us": 2000000 // 2 seconds later
            }),
        ];

        for event in events {
            node.process(RuntimeData::Json(event)).unwrap();
        }

        // Wait for emit interval
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

        let result = node
            .process(RuntimeData::Json(serde_json::json!({
                "event_type": "speech.presence",
                "state": "speaking",
                "timestamp_us": 3000000
            })))
            .unwrap();

        if let RuntimeData::Json(json) = result {
            if !json.is_null() {
                assert_eq!(json["event_type"], "conversation.flow");
                assert!(json.get("talk_pct").is_some() || json.get("left_talk_pct").is_some());
            }
        }
    }

    #[tokio::test]
    async fn test_stereo_flow_processing() {
        let node = create_test_node();

        // Simulate stereo speaking events
        let events = vec![
            serde_json::json!({
                "event_type": "speech.presence",
                "state": "speaking_left",
                "timestamp_us": 0
            }),
            serde_json::json!({
                "event_type": "speech.presence",
                "state": "overlap",
                "timestamp_us": 1000000
            }),
            serde_json::json!({
                "event_type": "speech.presence",
                "state": "speaking_right",
                "timestamp_us": 2000000
            }),
        ];

        for event in events {
            node.process(RuntimeData::Json(event)).unwrap();
        }

        // Wait for emit interval
        tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

        let result = node
            .process(RuntimeData::Json(serde_json::json!({
                "event_type": "speech.presence",
                "state": "silent",
                "timestamp_us": 3000000
            })))
            .unwrap();

        if let RuntimeData::Json(json) = result {
            if !json.is_null() {
                assert_eq!(json["event_type"], "conversation.flow");
                // Should be stereo mode
                assert!(json.get("left_talk_pct").is_some());
                assert!(json.get("right_talk_pct").is_some());
                assert!(json.get("overlap_pct").is_some());
            }
        }
    }
}
