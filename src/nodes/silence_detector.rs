//! Silence and Dropout Detection Node
//!
//! Detects silence periods and intermittent audio dropouts.

use crate::data::RuntimeData;
use crate::nodes::{StreamingNode, SyncNodeWrapper, SyncStreamingNode};
use crate::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

/// Silence detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SilenceEvent {
    /// Whether the current chunk is silent
    pub is_silent: bool,
    /// RMS level in dB
    pub rms_db: f32,
    /// Duration of current silence in milliseconds (0 if not silent)
    pub silence_duration_ms: f32,
    /// Whether this is a sustained silence (longer than threshold)
    pub is_sustained_silence: bool,
    /// Number of dropout events detected in the window
    pub dropout_count: u32,
    /// Whether intermittent dropouts pattern detected
    pub has_intermittent_dropouts: bool,
    /// Health: 1.0 = healthy, 0.0 = unhealthy
    /// Based on silence duration relative to sustained threshold
    pub health: f32,
    /// Stream identifier
    pub stream_id: Option<String>,
    /// Timestamp in microseconds
    pub timestamp_us: Option<u64>,
}

/// Configuration for silence detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SilenceConfig {
    /// Threshold in dB below which audio is considered silence (default: -50dB)
    pub silence_threshold_db: f32,
    /// Duration in ms to consider silence "sustained" (default: 500ms)
    pub sustained_silence_ms: f32,
    /// Minimum dropout count in window to trigger intermittent dropout alert (default: 3)
    pub dropout_count_threshold: u32,
    /// Window size in ms for tracking dropouts (default: 5000ms)
    pub dropout_window_ms: f32,
}

impl Default for SilenceConfig {
    fn default() -> Self {
        Self {
            silence_threshold_db: -50.0,
            sustained_silence_ms: 500.0,
            dropout_count_threshold: 3,
            dropout_window_ms: 5000.0,
        }
    }
}

/// Node that detects silence and intermittent dropouts
pub struct SilenceDetectorNode {
    node_id: String,
    config: SilenceConfig,
    /// Current silence duration in samples
    silence_samples: AtomicU64,
    /// Was the previous chunk silent?
    was_silent: AtomicBool,
    /// Track transitions for dropout detection (timestamp_us, was_silent)
    transitions: Mutex<Vec<(u64, bool)>>,
    /// Sample rate from first audio chunk
    sample_rate: AtomicU64,
}

impl SilenceDetectorNode {
    /// Create a new SilenceDetectorNode
    pub fn new(node_id: String, config: SilenceConfig) -> Self {
        Self {
            node_id,
            config,
            silence_samples: AtomicU64::new(0),
            was_silent: AtomicBool::new(false),
            transitions: Mutex::new(Vec::new()),
            sample_rate: AtomicU64::new(0),
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

    fn process_audio(&self, input: RuntimeData) -> Result<RuntimeData, Error> {
        let (samples, sample_rate, stream_id, timestamp_us) = match &input {
            RuntimeData::Audio {
                samples,
                sample_rate,
                stream_id,
                timestamp_us,
                ..
            } => (
                samples.clone(),
                *sample_rate,
                stream_id.clone(),
                *timestamp_us,
            ),
            _ => return Ok(input), // Pass through non-audio data
        };

        // Initialize sample rate if needed
        if self.sample_rate.load(Ordering::Relaxed) == 0 {
            self.sample_rate
                .store(sample_rate as u64, Ordering::Relaxed);
        }

        // Calculate current chunk's silence status
        let rms = Self::calculate_rms(&samples);
        let rms_db = Self::to_db(rms);
        let is_silent = rms_db <= self.config.silence_threshold_db;

        // Update silence duration
        if is_silent {
            self.silence_samples
                .fetch_add(samples.len() as u64, Ordering::Relaxed);
        } else {
            self.silence_samples.store(0, Ordering::Relaxed);
        }

        let sr = self.sample_rate.load(Ordering::Relaxed);
        let silence_duration_ms = if sr > 0 {
            (self.silence_samples.load(Ordering::Relaxed) as f32 * 1000.0) / sr as f32
        } else {
            0.0
        };

        let is_sustained_silence = silence_duration_ms >= self.config.sustained_silence_ms;

        // Track transitions for dropout detection
        let current_time_us = timestamp_us.unwrap_or(0);
        let prev_silent = self.was_silent.swap(is_silent, Ordering::Relaxed);

        let dropout_count = {
            let mut transitions = self.transitions.lock().unwrap();

            if is_silent != prev_silent {
                transitions.push((current_time_us, is_silent));

                // Clean up old transitions outside the window
                let window_us = (self.config.dropout_window_ms * 1000.0) as u64;
                transitions.retain(|(ts, _)| current_time_us.saturating_sub(*ts) < window_us);
            }

            // Count dropouts (silence starts) in window
            transitions
                .iter()
                .filter(|(_, was_silent)| *was_silent)
                .count() as u32
        };

        let has_intermittent_dropouts = dropout_count >= self.config.dropout_count_threshold;

        // Calculate health: 1.0 = healthy, 0.0 = unhealthy
        // Degrades linearly as silence approaches sustained threshold, then stays at 0
        let health = if !is_silent {
            1.0
        } else if silence_duration_ms >= self.config.sustained_silence_ms {
            0.0
        } else {
            // Linear degradation from 1.0 to 0.0 as silence approaches threshold
            1.0 - (silence_duration_ms / self.config.sustained_silence_ms)
        };

        // Create event
        let event = SilenceEvent {
            is_silent,
            rms_db,
            silence_duration_ms,
            is_sustained_silence,
            dropout_count,
            has_intermittent_dropouts,
            health,
            stream_id: stream_id.clone(),
            timestamp_us,
        };

        // Log if issues detected
        if is_sustained_silence {
            tracing::debug!(
                "SilenceDetectorNode {}: Sustained silence detected ({:.0}ms)",
                self.node_id,
                silence_duration_ms
            );
        } else if has_intermittent_dropouts {
            tracing::debug!(
                "SilenceDetectorNode {}: Intermittent dropouts detected ({} in window)",
                self.node_id,
                dropout_count
            );
        }

        // Output the silence event as JSON
        let mut event_json = serde_json::to_value(&event).unwrap_or(Value::Null);
        if let Value::Object(ref mut map) = event_json {
            map.insert(
                "_schema".to_string(),
                Value::String("silence_event".to_string()),
            );
        }
        Ok(RuntimeData::Json(event_json))
    }
}

// Phase A-Wave 1: `SyncStreamingNode` — body is sync; internal state
// already uses `std::sync::Mutex` / `AtomicU64` (no `.await`). Wrapped
// through `SyncNodeWrapper` at the registry boundary.
impl SyncStreamingNode for SilenceDetectorNode {
    fn node_type(&self) -> &str {
        "SilenceDetectorNode"
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

/// Factory for creating SilenceDetectorNode instances
pub struct SilenceDetectorNodeFactory;

impl crate::nodes::StreamingNodeFactory for SilenceDetectorNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: SilenceConfig =
            if params.is_null() || params.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                SilenceConfig::default()
            } else {
                serde_json::from_value(params.clone()).unwrap_or_default()
            };

        Ok(Box::new(SyncNodeWrapper(SilenceDetectorNode::new(
            node_id, config,
        ))))
    }

    fn node_type(&self) -> &str {
        "SilenceDetectorNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("SilenceDetectorNode")
                .description("Detects silence in audio streams and emits JSON events with silence state and stats")
                .category("health")
                .accepts([RuntimeDataType::Audio])
                .produces([RuntimeDataType::Json]),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_silence_detection() {
        let samples = vec![0.0f32; 1000];
        let rms = SilenceDetectorNode::calculate_rms(&samples);
        let rms_db = SilenceDetectorNode::to_db(rms);
        assert!(rms_db <= -50.0);
    }

    #[test]
    fn test_audio_detection() {
        let samples: Vec<f32> = (0..1000).map(|i| 0.5 * (i as f32 * 0.01).sin()).collect();
        let rms = SilenceDetectorNode::calculate_rms(&samples);
        let rms_db = SilenceDetectorNode::to_db(rms);
        assert!(rms_db > -50.0);
    }
}
