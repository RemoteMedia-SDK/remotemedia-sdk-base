//! Speech Presence Detection Node
//!
//! Converts raw VAD signals into semantic speech states with duration tracking.
//! Supports both stereo (per-channel) and mono (aggregate) analysis.
//!
//! States:
//! - Mono: speaking, silent, dead_air
//! - Stereo: speaking_left, speaking_right, speaking_both, overlap, silent, dead_air

use crate::data::RuntimeData;
use crate::nodes::{StreamingNode, SyncNodeWrapper, SyncStreamingNode};
use crate::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::RwLock;

/// Speech presence states for mono audio
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MonoSpeechState {
    Speaking,
    Silent,
    DeadAir,
}

/// Speech presence states for stereo audio
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StereoSpeechState {
    SpeakingLeft,
    SpeakingRight,
    SpeakingBoth,
    Overlap,
    Silent,
    DeadAir,
}

/// Configuration for speech presence detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeechPresenceConfig {
    /// Duration (ms) of silence before classifying as "dead_air"
    #[serde(default = "default_dead_air_threshold_ms")]
    pub dead_air_threshold_ms: u32,

    /// Duration (ms) of simultaneous speech before classifying as "overlap" (stereo only)
    #[serde(default = "default_overlap_threshold_ms")]
    pub overlap_threshold_ms: u32,

    /// Threshold in dB below which audio is considered silence (for internal VAD)
    #[serde(default = "default_silence_threshold_db")]
    pub silence_threshold_db: f32,

    /// Emit periodic updates even without state change (None = emit on change only)
    #[serde(default)]
    pub emit_interval_ms: Option<u32>,
}

fn default_dead_air_threshold_ms() -> u32 {
    2000
}

fn default_overlap_threshold_ms() -> u32 {
    500
}

fn default_silence_threshold_db() -> f32 {
    -50.0
}

impl Default for SpeechPresenceConfig {
    fn default() -> Self {
        Self {
            dead_air_threshold_ms: default_dead_air_threshold_ms(),
            overlap_threshold_ms: default_overlap_threshold_ms(),
            silence_threshold_db: default_silence_threshold_db(),
            emit_interval_ms: None,
        }
    }
}

/// Per-channel speech tracking state
#[derive(Debug, Clone)]
struct ChannelState {
    /// Whether speech is currently active
    is_active: bool,
    /// Duration of current state in samples
    state_samples: u64,
    /// Start timestamp of current state
    state_start_us: Option<u64>,
}

impl Default for ChannelState {
    fn default() -> Self {
        Self {
            is_active: false,
            state_samples: 0,
            state_start_us: None,
        }
    }
}

/// Internal state for the speech presence node
struct SpeechPresenceState {
    /// Left channel state (or mono)
    left: ChannelState,
    /// Right channel state (stereo only)
    right: ChannelState,
    /// Current overlap duration in samples (stereo only)
    overlap_samples: u64,
    /// Sample rate from audio
    sample_rate: u32,
    /// Whether we're in stereo mode
    is_stereo: bool,
    /// Last emitted state (for change detection)
    last_state: Option<String>,
    /// Last emit timestamp
    last_emit_us: u64,
}

impl Default for SpeechPresenceState {
    fn default() -> Self {
        Self {
            left: ChannelState::default(),
            right: ChannelState::default(),
            overlap_samples: 0,
            sample_rate: 16000,
            is_stereo: false,
            last_state: None,
            last_emit_us: 0,
        }
    }
}

/// Node that detects speech presence and tracks semantic states
pub struct SpeechPresenceNode {
    #[allow(dead_code)]
    node_id: String,
    config: SpeechPresenceConfig,
    state: RwLock<SpeechPresenceState>,
}

impl SpeechPresenceNode {
    /// Create a new SpeechPresenceNode
    pub fn new(node_id: String, config: SpeechPresenceConfig) -> Self {
        Self {
            node_id,
            config,
            state: RwLock::new(SpeechPresenceState::default()),
        }
    }

    /// Calculate RMS level of audio samples
    fn calculate_rms(samples: &[f32]) -> f32 {
        if samples.is_empty() {
            return 0.0;
        }
        let sum_squares: f32 = samples.iter().map(|s| s * s).sum();
        (sum_squares / samples.len() as f32).sqrt()
    }

    /// Convert linear amplitude to dB
    fn to_db(amplitude: f32) -> f32 {
        if amplitude <= 0.0 {
            -120.0
        } else {
            20.0 * amplitude.log10()
        }
    }

    /// Extract left channel from interleaved stereo
    fn extract_left(samples: &[f32]) -> Vec<f32> {
        samples.iter().step_by(2).copied().collect()
    }

    /// Extract right channel from interleaved stereo
    fn extract_right(samples: &[f32]) -> Vec<f32> {
        samples.iter().skip(1).step_by(2).copied().collect()
    }

    /// Detect speech activity using simple RMS thresholding
    fn detect_speech(&self, samples: &[f32]) -> bool {
        let rms = Self::calculate_rms(samples);
        let rms_db = Self::to_db(rms);
        rms_db > self.config.silence_threshold_db
    }

    /// Convert samples to milliseconds given sample rate
    fn samples_to_ms(samples: u64, sample_rate: u32) -> u32 {
        if sample_rate == 0 {
            return 0;
        }
        ((samples as f64 * 1000.0) / sample_rate as f64) as u32
    }

    /// Process mono audio and return state
    fn process_mono(
        &self,
        samples: &[f32],
        timestamp_us: Option<u64>,
        stream_id: Option<String>,
        state: &mut SpeechPresenceState,
    ) -> Option<Value> {
        let has_speech = self.detect_speech(samples);
        let chunk_samples = samples.len() as u64;

        // Update state tracking
        if has_speech != state.left.is_active {
            // State transition
            state.left.is_active = has_speech;
            state.left.state_samples = chunk_samples;
            state.left.state_start_us = timestamp_us;
        } else {
            state.left.state_samples += chunk_samples;
        }

        let duration_ms = Self::samples_to_ms(state.left.state_samples, state.sample_rate);

        // Determine semantic state
        let speech_state = if has_speech {
            MonoSpeechState::Speaking
        } else if duration_ms >= self.config.dead_air_threshold_ms {
            MonoSpeechState::DeadAir
        } else {
            MonoSpeechState::Silent
        };

        let state_str = match speech_state {
            MonoSpeechState::Speaking => "speaking",
            MonoSpeechState::Silent => "silent",
            MonoSpeechState::DeadAir => "dead_air",
        };

        // Check if we should emit
        let current_us = timestamp_us.unwrap_or(0);
        let should_emit = self.should_emit(state_str, current_us, state);

        if should_emit {
            state.last_state = Some(state_str.to_string());
            state.last_emit_us = current_us;

            Some(serde_json::json!({
                "event_type": "speech.presence",
                "_schema": "speech_presence_v1",
                "state": state_str,
                "duration_ms": duration_ms,
                "timestamp_us": timestamp_us,
                "stream_id": stream_id,
            }))
        } else {
            None
        }
    }

    /// Process stereo audio and return state
    fn process_stereo(
        &self,
        samples: &[f32],
        timestamp_us: Option<u64>,
        stream_id: Option<String>,
        state: &mut SpeechPresenceState,
    ) -> Option<Value> {
        let left_samples = Self::extract_left(samples);
        let right_samples = Self::extract_right(samples);

        let left_active = self.detect_speech(&left_samples);
        let right_active = self.detect_speech(&right_samples);
        let chunk_samples = left_samples.len() as u64;

        // Update left channel state
        if left_active != state.left.is_active {
            state.left.is_active = left_active;
            state.left.state_samples = chunk_samples;
            state.left.state_start_us = timestamp_us;
        } else {
            state.left.state_samples += chunk_samples;
        }

        // Update right channel state
        if right_active != state.right.is_active {
            state.right.is_active = right_active;
            state.right.state_samples = chunk_samples;
            state.right.state_start_us = timestamp_us;
        } else {
            state.right.state_samples += chunk_samples;
        }

        // Update overlap tracking
        if left_active && right_active {
            state.overlap_samples += chunk_samples;
        } else {
            state.overlap_samples = 0;
        }

        let left_duration_ms = Self::samples_to_ms(state.left.state_samples, state.sample_rate);
        let right_duration_ms = Self::samples_to_ms(state.right.state_samples, state.sample_rate);
        let overlap_ms = Self::samples_to_ms(state.overlap_samples, state.sample_rate);

        // Calculate silence duration (when neither channel is active)
        let silence_duration_ms = if !left_active && !right_active {
            left_duration_ms.min(right_duration_ms)
        } else {
            0
        };

        // Determine semantic state
        let speech_state = if left_active && right_active {
            if overlap_ms >= self.config.overlap_threshold_ms {
                StereoSpeechState::Overlap
            } else {
                StereoSpeechState::SpeakingBoth
            }
        } else if left_active {
            StereoSpeechState::SpeakingLeft
        } else if right_active {
            StereoSpeechState::SpeakingRight
        } else if silence_duration_ms >= self.config.dead_air_threshold_ms {
            StereoSpeechState::DeadAir
        } else {
            StereoSpeechState::Silent
        };

        let state_str = match speech_state {
            StereoSpeechState::SpeakingLeft => "speaking_left",
            StereoSpeechState::SpeakingRight => "speaking_right",
            StereoSpeechState::SpeakingBoth => "speaking_both",
            StereoSpeechState::Overlap => "overlap",
            StereoSpeechState::Silent => "silent",
            StereoSpeechState::DeadAir => "dead_air",
        };

        // Check if we should emit
        let current_us = timestamp_us.unwrap_or(0);
        let should_emit = self.should_emit(state_str, current_us, state);

        if should_emit {
            state.last_state = Some(state_str.to_string());
            state.last_emit_us = current_us;

            Some(serde_json::json!({
                "event_type": "speech.presence",
                "_schema": "speech_presence_v1",
                "state": state_str,
                "speakers": {
                    "left": {
                        "active": left_active,
                        "duration_ms": left_duration_ms
                    },
                    "right": {
                        "active": right_active,
                        "duration_ms": right_duration_ms
                    }
                },
                "overlap_duration_ms": overlap_ms,
                "timestamp_us": timestamp_us,
                "stream_id": stream_id,
            }))
        } else {
            None
        }
    }

    /// Determine if we should emit an event
    fn should_emit(
        &self,
        current_state: &str,
        current_us: u64,
        state: &SpeechPresenceState,
    ) -> bool {
        // Always emit on state change
        if state.last_state.as_deref() != Some(current_state) {
            return true;
        }

        // Check periodic emission
        if let Some(interval_ms) = self.config.emit_interval_ms {
            let elapsed_us = current_us.saturating_sub(state.last_emit_us);
            let elapsed_ms = elapsed_us / 1000;
            if elapsed_ms >= interval_ms as u64 {
                return true;
            }
        }

        false
    }

    fn process_audio(&self, input: RuntimeData) -> Result<RuntimeData, Error> {
        let (samples, sample_rate, channels, stream_id, timestamp_us) = match &input {
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                stream_id,
                timestamp_us,
                ..
            } => (
                samples.clone(),
                *sample_rate,
                *channels,
                stream_id.clone(),
                *timestamp_us,
            ),
            _ => return Ok(input), // Pass through non-audio data
        };

        let mut state = self.state.write().map_err(|e| {
            Error::Execution(format!("Failed to lock speech presence state: {}", e))
        })?;

        // Update sample rate if changed
        state.sample_rate = sample_rate;
        state.is_stereo = channels == 2;

        let event = if channels == 2 {
            self.process_stereo(&samples, timestamp_us, stream_id, &mut state)
        } else {
            self.process_mono(&samples, timestamp_us, stream_id, &mut state)
        };

        match event {
            Some(json) => Ok(RuntimeData::Json(json)),
            None => Ok(RuntimeData::Json(Value::Null)),
        }
    }
}

// Phase A-Wave 1: `SyncStreamingNode` — body is sync; internal state
// uses `std::sync::RwLock` (no `.await`). Wrapped through
// `SyncNodeWrapper` at the registry boundary.
impl SyncStreamingNode for SpeechPresenceNode {
    fn node_type(&self) -> &str {
        "SpeechPresenceNode"
    }

    fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        self.process_audio(data)
    }

    fn process_multi(&self, inputs: HashMap<String, RuntimeData>) -> Result<RuntimeData, Error> {
        if let Some((_key, data)) = inputs.into_iter().next() {
            self.process_audio(data)
        } else {
            Err(Error::Execution("No input data".to_string()))
        }
    }

    fn is_multi_input(&self) -> bool {
        false
    }
}

/// Factory for creating SpeechPresenceNode instances
pub struct SpeechPresenceNodeFactory;

impl crate::nodes::StreamingNodeFactory for SpeechPresenceNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: SpeechPresenceConfig =
            if params.is_null() || params.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                SpeechPresenceConfig::default()
            } else {
                serde_json::from_value(params.clone()).unwrap_or_default()
            };

        Ok(Box::new(SyncNodeWrapper(SpeechPresenceNode::new(
            node_id, config,
        ))))
    }

    fn node_type(&self) -> &str {
        "SpeechPresenceNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("SpeechPresenceNode")
                .description("Detects speech presence in audio streams and emits JSON events with VAD state and metrics")
                .category("health")
                .accepts([RuntimeDataType::Audio])
                .produces([RuntimeDataType::Json]),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node() -> SpeechPresenceNode {
        SpeechPresenceNode::new("test".to_string(), SpeechPresenceConfig::default())
    }

    #[test]
    fn test_speech_detection_silence() {
        let node = create_test_node();
        let samples = vec![0.0f32; 1000];
        assert!(!node.detect_speech(&samples));
    }

    #[test]
    fn test_speech_detection_active() {
        let node = create_test_node();
        // Create audio at -20dB (0.1 amplitude)
        let samples: Vec<f32> = (0..1000).map(|i| 0.1 * (i as f32 * 0.01).sin()).collect();
        assert!(node.detect_speech(&samples));
    }

    #[test]
    fn test_samples_to_ms() {
        assert_eq!(SpeechPresenceNode::samples_to_ms(16000, 16000), 1000);
        assert_eq!(SpeechPresenceNode::samples_to_ms(8000, 16000), 500);
        assert_eq!(SpeechPresenceNode::samples_to_ms(0, 16000), 0);
    }

    #[test]
    fn test_channel_extraction() {
        let interleaved = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        let left = SpeechPresenceNode::extract_left(&interleaved);
        let right = SpeechPresenceNode::extract_right(&interleaved);

        assert_eq!(left, vec![1.0, 3.0, 5.0]);
        assert_eq!(right, vec![2.0, 4.0, 6.0]);
    }

    #[tokio::test]
    async fn test_mono_processing() {
        let node = create_test_node();

        // Silent audio
        let silent_input = RuntimeData::Audio {
            samples: vec![0.0; 1600].into(),
            sample_rate: 16000,
            channels: 1,
            stream_id: Some("test".to_string()),
            timestamp_us: Some(100_000),
            arrival_ts_us: None,
            metadata: None,
        };

        let result = node.process(silent_input).unwrap();
        if let RuntimeData::Json(json) = result {
            if !json.is_null() {
                assert_eq!(json["state"], "silent");
            }
        }
    }

    #[tokio::test]
    async fn test_stereo_processing() {
        let node = create_test_node();

        // Left channel active, right silent (interleaved)
        let samples: Vec<f32> = (0..3200)
            .map(|i| {
                if i % 2 == 0 {
                    0.1 * ((i / 2) as f32 * 0.01).sin() // Left: active
                } else {
                    0.0 // Right: silent
                }
            })
            .collect();

        let input = RuntimeData::Audio {
            samples: samples.into(),
            sample_rate: 16000,
            channels: 2,
            stream_id: Some("test".to_string()),
            timestamp_us: Some(100_000),
            arrival_ts_us: None,
            metadata: None,
        };

        let result = node.process(input).unwrap();
        if let RuntimeData::Json(json) = result {
            if !json.is_null() {
                assert_eq!(json["state"], "speaking_left");
                assert_eq!(json["speakers"]["left"]["active"], true);
                assert_eq!(json["speakers"]["right"]["active"], false);
            }
        }
    }

    #[test]
    fn test_config_default() {
        let config = SpeechPresenceConfig::default();
        assert_eq!(config.dead_air_threshold_ms, 2000);
        assert_eq!(config.overlap_threshold_ms, 500);
        assert_eq!(config.silence_threshold_db, -50.0);
        assert!(config.emit_interval_ms.is_none());
    }
}
