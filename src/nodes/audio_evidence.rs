//! Audio Evidence Capture Node
//!
//! Maintains a rolling audio buffer and exports clips when alerts occur.
//! This is an opt-in, privacy-sensitive node that never persists audio automatically.

use crate::data::RuntimeData;
use crate::nodes::{StreamingNode, SyncNodeWrapper, SyncStreamingNode};
use crate::Error;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Configuration for audio evidence node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioEvidenceConfig {
    /// Rolling buffer duration in seconds
    #[serde(default = "default_buffer_duration_s")]
    pub buffer_duration_s: u32,

    /// Pre-alert clip duration (seconds before alert)
    #[serde(default = "default_pre_alert_s")]
    pub pre_alert_s: u32,

    /// Post-alert clip duration (seconds after alert)
    #[serde(default = "default_post_alert_s")]
    pub post_alert_s: u32,

    /// Maximum clips to retain per session
    #[serde(default = "default_max_clips")]
    pub max_clips: u32,

    /// Whether to emit audio data or just references
    #[serde(default = "default_emit_audio_data")]
    pub emit_audio_data: bool,
}

fn default_buffer_duration_s() -> u32 {
    10
}

fn default_pre_alert_s() -> u32 {
    3
}

fn default_post_alert_s() -> u32 {
    2
}

fn default_max_clips() -> u32 {
    10
}

fn default_emit_audio_data() -> bool {
    false
}

impl Default for AudioEvidenceConfig {
    fn default() -> Self {
        Self {
            buffer_duration_s: default_buffer_duration_s(),
            pre_alert_s: default_pre_alert_s(),
            post_alert_s: default_post_alert_s(),
            max_clips: default_max_clips(),
            emit_audio_data: default_emit_audio_data(),
        }
    }
}

/// Audio chunk in the buffer
#[derive(Debug, Clone)]
struct AudioChunk {
    /// Audio samples
    samples: Vec<f32>,
    /// Sample rate
    sample_rate: u32,
    /// Number of channels
    channels: u32,
    /// Timestamp in microseconds
    timestamp_us: u64,
}

/// Pending clip waiting for post-alert audio
#[derive(Debug)]
struct PendingClip {
    /// Clip ID
    id: String,
    /// Trigger event type
    trigger_event: String,
    /// Audio collected so far
    samples: Vec<f32>,
    /// Sample rate
    sample_rate: u32,
    /// Channels
    channels: u32,
    /// Start timestamp
    start_us: u64,
    /// Samples needed for post-alert
    post_samples_remaining: usize,
}

/// Stored clip (when not emitting immediately)
#[derive(Debug, Clone)]
struct StoredClip {
    /// Clip ID
    id: String,
    /// Trigger event type
    trigger_event: String,
    /// Audio samples
    samples: Vec<f32>,
    /// Sample rate
    sample_rate: u32,
    /// Channels
    channels: u32,
    /// Start timestamp
    start_us: u64,
    /// Duration in milliseconds
    duration_ms: u32,
}

/// Internal state for audio evidence node
struct AudioEvidenceState {
    /// Rolling audio buffer
    buffer: VecDeque<AudioChunk>,
    /// Total samples in buffer
    buffer_samples: usize,
    /// Sample rate (from first audio)
    sample_rate: u32,
    /// Channels (from first audio)
    channels: u32,
    /// Pending clips waiting for post-alert audio
    pending_clips: Vec<PendingClip>,
    /// Stored clips (when emit_audio_data is false)
    stored_clips: VecDeque<StoredClip>,
}

impl Default for AudioEvidenceState {
    fn default() -> Self {
        Self {
            buffer: VecDeque::new(),
            buffer_samples: 0,
            sample_rate: 16000,
            channels: 1,
            pending_clips: Vec::new(),
            stored_clips: VecDeque::new(),
        }
    }
}

/// Node that captures audio evidence on alerts
pub struct AudioEvidenceNode {
    #[allow(dead_code)]
    node_id: String,
    config: AudioEvidenceConfig,
    state: RwLock<AudioEvidenceState>,
}

impl AudioEvidenceNode {
    /// Create a new AudioEvidenceNode
    pub fn new(node_id: String, config: AudioEvidenceConfig) -> Self {
        Self {
            node_id,
            config,
            state: RwLock::new(AudioEvidenceState::default()),
        }
    }

    /// Get current timestamp in microseconds
    fn current_timestamp_us() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0)
    }

    /// Calculate max buffer samples based on sample rate
    fn max_buffer_samples(&self, sample_rate: u32) -> usize {
        self.config.buffer_duration_s as usize * sample_rate as usize
    }

    /// Calculate pre-alert samples
    fn pre_alert_samples(&self, sample_rate: u32) -> usize {
        self.config.pre_alert_s as usize * sample_rate as usize
    }

    /// Calculate post-alert samples
    fn post_alert_samples(&self, sample_rate: u32) -> usize {
        self.config.post_alert_s as usize * sample_rate as usize
    }

    /// Check if an event is a trigger event
    fn is_trigger_event(json: &Value) -> Option<String> {
        let event_type = json
            .get("event_type")
            .or_else(|| json.get("type"))
            .and_then(|v| v.as_str());

        // Check for explicit trigger events
        if let Some(et) = event_type {
            match et {
                "timing.jitter_spike" | "timing.clock_drift" | "freeze" | "drift" | "incident" => {
                    return Some(et.to_string());
                }
                _ => {}
            }
        }

        // Check for issue flags
        if json
            .get("is_clipping")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            return Some("clipping".to_string());
        }
        if json
            .get("is_sustained_silence")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            return Some("silence".to_string());
        }
        if json
            .get("has_dead_channel")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            return Some("dead_channel".to_string());
        }
        if json.get("state").and_then(|v| v.as_str()) == Some("dead_air") {
            return Some("dead_air".to_string());
        }
        if json.get("state").and_then(|v| v.as_str()) == Some("unhealthy") {
            return Some("unhealthy".to_string());
        }

        None
    }

    /// Extract pre-alert audio from buffer
    /// Returns (samples, start_timestamp_us, sample_rate, channels)
    fn extract_pre_alert(&self, state: &AudioEvidenceState) -> (Vec<f32>, u64, u32, u32) {
        let pre_samples = self.pre_alert_samples(state.sample_rate);
        let mut collected = Vec::new();
        let mut earliest_timestamp = Self::current_timestamp_us();
        let mut earliest_sample_rate = state.sample_rate;
        let mut earliest_channels = state.channels;

        // Collect from buffer in reverse order (most recent first)
        for chunk in state.buffer.iter().rev() {
            if collected.len() >= pre_samples {
                break;
            }
            let needed = pre_samples - collected.len();
            let start = chunk.samples.len().saturating_sub(needed);
            collected.extend_from_slice(&chunk.samples[start..]);
            // Track metadata from the earliest chunk we're using
            earliest_timestamp = chunk.timestamp_us;
            earliest_sample_rate = chunk.sample_rate;
            earliest_channels = chunk.channels;
        }

        // Reverse to get chronological order
        collected.reverse();

        // Truncate if we got too much
        if collected.len() > pre_samples {
            collected = collected[collected.len() - pre_samples..].to_vec();
        }

        (
            collected,
            earliest_timestamp,
            earliest_sample_rate,
            earliest_channels,
        )
    }

    /// Build clip reference event
    fn build_clip_reference(&self, clip: &StoredClip) -> Value {
        serde_json::json!({
            "event_type": "audio.evidence",
            "_schema": "audio_evidence_v1",
            "clip_id": clip.id,
            "trigger_event": clip.trigger_event,
            "start_offset_ms": -(self.config.pre_alert_s as i32 * 1000),
            "duration_ms": clip.duration_ms,
            "sample_rate": clip.sample_rate,
            "channels": clip.channels,
            "timestamp_us": clip.start_us,
        })
    }

    /// Build clip event with audio data
    fn build_clip_with_audio(&self, clip: &StoredClip) -> Value {
        // Convert f32 samples to bytes (little-endian)
        let bytes: Vec<u8> = clip.samples.iter().flat_map(|s| s.to_le_bytes()).collect();

        let audio_base64 = BASE64.encode(&bytes);

        serde_json::json!({
            "event_type": "audio.evidence",
            "_schema": "audio_evidence_v1",
            "clip_id": clip.id,
            "trigger_event": clip.trigger_event,
            "start_offset_ms": -(self.config.pre_alert_s as i32 * 1000),
            "duration_ms": clip.duration_ms,
            "sample_rate": clip.sample_rate,
            "channels": clip.channels,
            "audio_base64": audio_base64,
            "audio_format": "f32le",
            "timestamp_us": clip.start_us,
        })
    }

    /// Process audio data
    fn process_audio(
        &self,
        samples: Vec<f32>,
        sample_rate: u32,
        channels: u32,
        timestamp_us: u64,
        state: &mut AudioEvidenceState,
    ) {
        // Update sample rate/channels
        state.sample_rate = sample_rate;
        state.channels = channels;

        // Add to buffer
        let chunk_samples = samples.len();
        state.buffer.push_back(AudioChunk {
            samples: samples.clone(),
            sample_rate,
            channels,
            timestamp_us,
        });
        state.buffer_samples += chunk_samples;

        // Trim buffer if needed
        let max_samples = self.max_buffer_samples(sample_rate);
        while state.buffer_samples > max_samples {
            if let Some(old) = state.buffer.pop_front() {
                state.buffer_samples -= old.samples.len();
            } else {
                break;
            }
        }

        // Feed pending clips
        for clip in &mut state.pending_clips {
            if clip.post_samples_remaining > 0 {
                let take = clip.post_samples_remaining.min(chunk_samples);
                clip.samples.extend_from_slice(&samples[..take]);
                clip.post_samples_remaining -= take;
            }
        }
    }

    /// Start a new clip on trigger
    fn start_clip(&self, trigger_event: String, state: &mut AudioEvidenceState) {
        // Check clip limit
        if state.pending_clips.len() + state.stored_clips.len() >= self.config.max_clips as usize {
            // Remove oldest stored clip
            state.stored_clips.pop_front();
        }

        let clip_id = format!(
            "clip_{}",
            Uuid::new_v4()
                .to_string()
                .split('-')
                .next()
                .unwrap_or("unknown")
        );
        let (pre_audio, start_us, sample_rate, channels) = self.extract_pre_alert(state);

        let pending = PendingClip {
            id: clip_id,
            trigger_event,
            samples: pre_audio,
            sample_rate,
            channels,
            start_us,
            post_samples_remaining: self.post_alert_samples(sample_rate),
        };

        state.pending_clips.push(pending);
    }

    /// Check for completed clips and return events
    fn collect_completed_clips(&self, state: &mut AudioEvidenceState) -> Vec<Value> {
        let mut events = Vec::new();
        let mut completed_indices = Vec::new();

        for (i, clip) in state.pending_clips.iter().enumerate() {
            if clip.post_samples_remaining == 0 {
                completed_indices.push(i);
            }
        }

        for i in completed_indices.into_iter().rev() {
            let clip = state.pending_clips.remove(i);
            let duration_ms = (clip.samples.len() as u32 * 1000) / clip.sample_rate;

            let stored = StoredClip {
                id: clip.id,
                trigger_event: clip.trigger_event,
                samples: clip.samples,
                sample_rate: clip.sample_rate,
                channels: clip.channels,
                start_us: clip.start_us,
                duration_ms,
            };

            // Emit or store
            let event = if self.config.emit_audio_data {
                self.build_clip_with_audio(&stored)
            } else {
                self.build_clip_reference(&stored)
            };

            events.push(event);

            // Store for later retrieval if not emitting data
            if !self.config.emit_audio_data {
                state.stored_clips.push_back(stored);
                while state.stored_clips.len() > self.config.max_clips as usize {
                    state.stored_clips.pop_front();
                }
            }
        }

        events
    }

    fn process_input(&self, input: RuntimeData) -> Result<RuntimeData, Error> {
        let mut state = self
            .state
            .write()
            .map_err(|e| Error::Execution(format!("Failed to lock audio evidence state: {}", e)))?;

        let mut outputs = Vec::new();

        match &input {
            // Process audio - add to buffer and feed pending clips
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                timestamp_us,
                ..
            } => {
                let ts = timestamp_us.unwrap_or_else(Self::current_timestamp_us);
                self.process_audio(samples.to_vec(), *sample_rate, *channels, ts, &mut state);
            }

            // Process JSON - check for triggers
            RuntimeData::Json(json) if !json.is_null() => {
                if let Some(trigger) = Self::is_trigger_event(json) {
                    self.start_clip(trigger, &mut state);
                }
            }

            _ => {}
        }

        // Collect completed clips
        outputs.extend(self.collect_completed_clips(&mut state));

        match outputs.len() {
            0 => Ok(RuntimeData::Json(Value::Null)),
            1 => Ok(RuntimeData::Json(outputs.into_iter().next().unwrap())),
            _ => Ok(RuntimeData::Json(Value::Array(outputs))),
        }
    }
}

// Phase A-Wave 1: `SyncStreamingNode` — state uses `std::sync::RwLock`.
// Multi-input aggregation preserved.
impl SyncStreamingNode for AudioEvidenceNode {
    fn node_type(&self) -> &str {
        "AudioEvidenceNode"
    }

    fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        self.process_input(data)
    }

    fn process_multi(&self, inputs: HashMap<String, RuntimeData>) -> Result<RuntimeData, Error> {
        let mut all_outputs = Vec::new();

        // Process audio inputs first
        for (_key, data) in inputs.iter() {
            if matches!(data, RuntimeData::Audio { .. }) {
                let result = self.process_input(data.clone())?;
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
        }

        // Then process JSON/trigger inputs
        for (_key, data) in inputs.iter() {
            if !matches!(data, RuntimeData::Audio { .. }) {
                let result = self.process_input(data.clone())?;
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

/// Factory for creating AudioEvidenceNode instances
pub struct AudioEvidenceNodeFactory;

impl crate::nodes::StreamingNodeFactory for AudioEvidenceNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: AudioEvidenceConfig =
            if params.is_null() || params.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                AudioEvidenceConfig::default()
            } else {
                serde_json::from_value(params.clone()).unwrap_or_default()
            };

        Ok(Box::new(SyncNodeWrapper(AudioEvidenceNode::new(
            node_id, config,
        ))))
    }

    fn node_type(&self) -> &str {
        "AudioEvidenceNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("AudioEvidenceNode")
                .description(
                    "Buffers audio evidence around events for forensic playback and analysis",
                )
                .category("health")
                .accepts([RuntimeDataType::Audio, RuntimeDataType::Json])
                .produces([RuntimeDataType::Audio, RuntimeDataType::Json]),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_node() -> AudioEvidenceNode {
        AudioEvidenceNode::new(
            "test".to_string(),
            AudioEvidenceConfig {
                buffer_duration_s: 5,
                pre_alert_s: 1,
                post_alert_s: 1,
                max_clips: 5,
                emit_audio_data: true,
            },
        )
    }

    #[test]
    fn test_config_default() {
        let config = AudioEvidenceConfig::default();
        assert_eq!(config.buffer_duration_s, 10);
        assert_eq!(config.pre_alert_s, 3);
        assert_eq!(config.post_alert_s, 2);
        assert_eq!(config.max_clips, 10);
        assert!(!config.emit_audio_data);
    }

    #[test]
    fn test_trigger_detection() {
        // Clipping triggers
        let event = serde_json::json!({"is_clipping": true});
        assert_eq!(
            AudioEvidenceNode::is_trigger_event(&event),
            Some("clipping".to_string())
        );

        // Dead air triggers
        let event = serde_json::json!({"state": "dead_air"});
        assert_eq!(
            AudioEvidenceNode::is_trigger_event(&event),
            Some("dead_air".to_string())
        );

        // Timing events trigger
        let event = serde_json::json!({"event_type": "freeze"});
        assert_eq!(
            AudioEvidenceNode::is_trigger_event(&event),
            Some("freeze".to_string())
        );

        // Normal events don't trigger
        let event = serde_json::json!({"state": "speaking"});
        assert!(AudioEvidenceNode::is_trigger_event(&event).is_none());
    }

    #[tokio::test]
    async fn test_audio_buffering() {
        let node = create_test_node();

        // Send audio chunks
        for i in 0..10 {
            let input = RuntimeData::Audio {
                samples: vec![0.1; 16000].into(), // 1 second
                sample_rate: 16000,
                channels: 1,
                stream_id: None,
                timestamp_us: Some(i * 1_000_000),
                arrival_ts_us: None,
                metadata: None,
            };
            node.process(input).unwrap();
        }

        // Buffer should be limited to 5 seconds
        let state = node.state.read().unwrap();
        assert!(state.buffer_samples <= 5 * 16000 + 16000); // Some tolerance
    }

    #[tokio::test]
    async fn test_clip_creation() {
        let node = create_test_node();

        // First, buffer some audio
        for i in 0..3 {
            let input = RuntimeData::Audio {
                samples: vec![0.1; 16000].into(), // 1 second each
                sample_rate: 16000,
                channels: 1,
                stream_id: None,
                timestamp_us: Some(i * 1_000_000),
                arrival_ts_us: None,
                metadata: None,
            };
            node.process(input).unwrap();
        }

        // Trigger clip creation
        let trigger = serde_json::json!({
            "event_type": "freeze"
        });
        node.process(RuntimeData::Json(trigger)).unwrap();

        // Add post-alert audio
        for i in 3..5 {
            let input = RuntimeData::Audio {
                samples: vec![0.1; 16000].into(),
                sample_rate: 16000,
                channels: 1,
                stream_id: None,
                timestamp_us: Some(i * 1_000_000),
                arrival_ts_us: None,
                metadata: None,
            };
            let result = node.process(input).unwrap();

            // Check if clip was emitted
            if let RuntimeData::Json(json) = result {
                if !json.is_null() {
                    if json.is_array() {
                        for event in json.as_array().unwrap() {
                            if event["event_type"] == "audio.evidence" {
                                assert!(event["clip_id"].is_string());
                                assert_eq!(event["trigger_event"], "freeze");
                                return; // Success
                            }
                        }
                    } else if json["event_type"] == "audio.evidence" {
                        assert!(json["clip_id"].is_string());
                        assert_eq!(json["trigger_event"], "freeze");
                        return; // Success
                    }
                }
            }
        }
    }
}
