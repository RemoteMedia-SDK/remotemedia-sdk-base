//! Audio Level Detection Node
//!
//! Calculates RMS energy and detects low volume conditions.

use crate::data::RuntimeData;
use crate::nodes::{StreamingNode, SyncNodeWrapper, SyncStreamingNode};
use crate::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Audio level detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioLevelEvent {
    /// RMS level in dB (0 = full scale)
    pub rms_db: f32,
    /// Peak level in dB
    pub peak_db: f32,
    /// Whether the audio is below the low volume threshold
    pub is_low_volume: bool,
    /// Whether complete silence was detected
    pub is_silence: bool,
    /// Health: 1.0 = healthy, 0.0 = unhealthy
    /// Silence = 0.0, Low volume = 0.5, Normal = 1.0
    pub health: f32,
    /// Stream identifier
    pub stream_id: Option<String>,
    /// Timestamp in microseconds
    pub timestamp_us: Option<u64>,
}

/// Configuration for audio level detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioLevelConfig {
    /// Threshold in dB below which audio is considered "low volume" (default: -20dB)
    pub low_volume_threshold_db: f32,
    /// Threshold in dB below which audio is considered "silence" (default: -60dB)
    pub silence_threshold_db: f32,
}

impl Default for AudioLevelConfig {
    fn default() -> Self {
        Self {
            low_volume_threshold_db: -20.0,
            silence_threshold_db: -60.0,
        }
    }
}

/// Node that analyzes audio levels and detects low volume/silence
pub struct AudioLevelNode {
    node_id: String,
    config: AudioLevelConfig,
}

impl AudioLevelNode {
    /// Create a new AudioLevelNode
    pub fn new(node_id: String, config: AudioLevelConfig) -> Self {
        Self { node_id, config }
    }

    /// Calculate RMS level of audio samples
    fn calculate_rms(samples: &[f32]) -> f32 {
        if samples.is_empty() {
            return 0.0;
        }
        let sum_squares: f32 = samples.iter().map(|s| s * s).sum();
        (sum_squares / samples.len() as f32).sqrt()
    }

    /// Calculate peak level of audio samples
    fn calculate_peak(samples: &[f32]) -> f32 {
        samples.iter().map(|s| s.abs()).fold(0.0f32, f32::max)
    }

    /// Convert linear amplitude to dB
    fn to_db(amplitude: f32) -> f32 {
        if amplitude <= 0.0 {
            -120.0 // Floor for silence
        } else {
            20.0 * amplitude.log10()
        }
    }

    fn process_audio(&self, input: RuntimeData) -> Result<RuntimeData, Error> {
        let (samples, stream_id, timestamp_us) = match &input {
            RuntimeData::Audio {
                samples,
                stream_id,
                timestamp_us,
                ..
            } => (samples.clone(), stream_id.clone(), *timestamp_us),
            _ => return Ok(input), // Pass through non-audio data
        };

        // Calculate levels
        let rms = Self::calculate_rms(&samples);
        let peak = Self::calculate_peak(&samples);
        let rms_db = Self::to_db(rms);
        let peak_db = Self::to_db(peak);

        // Detect conditions
        let is_silence = rms_db <= self.config.silence_threshold_db;
        let is_low_volume = !is_silence && rms_db <= self.config.low_volume_threshold_db;

        // Calculate health as continuous value: 1.0 = healthy, 0.0 = unhealthy
        // Maps rms_db from [silence_threshold, low_volume_threshold] to [0.0, 1.0]
        let health = if rms_db <= self.config.silence_threshold_db {
            0.0
        } else if rms_db >= self.config.low_volume_threshold_db {
            1.0
        } else {
            // Linear interpolation between silence and low_volume thresholds
            let range = self.config.low_volume_threshold_db - self.config.silence_threshold_db;
            ((rms_db - self.config.silence_threshold_db) / range).clamp(0.0, 1.0)
        };

        // Create level event
        let event = AudioLevelEvent {
            rms_db,
            peak_db,
            is_low_volume,
            is_silence,
            health,
            stream_id: stream_id.clone(),
            timestamp_us,
        };

        // Log if issues detected
        if is_silence {
            tracing::debug!(
                "AudioLevelNode {}: Silence detected (RMS: {:.1}dB)",
                self.node_id,
                rms_db
            );
        } else if is_low_volume {
            tracing::debug!(
                "AudioLevelNode {}: Low volume detected (RMS: {:.1}dB)",
                self.node_id,
                rms_db
            );
        }

        // Output the level event as JSON
        let mut event_json = serde_json::to_value(&event).unwrap_or(Value::Null);
        if let Value::Object(ref mut map) = event_json {
            map.insert(
                "_schema".to_string(),
                Value::String("audio_level_event".to_string()),
            );
        }
        Ok(RuntimeData::Json(event_json))
    }
}

// Migrated to `SyncStreamingNode` in Phase A-Wave 1: body has no
// `.await`, state is read-only (`config`, `node_id`). Runs through
// `SyncNodeWrapper` at the registry boundary, removing the
// `#[async_trait]`-induced `Box<dyn Future>` allocation per call.
impl SyncStreamingNode for AudioLevelNode {
    fn node_type(&self) -> &str {
        "AudioLevelNode"
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

/// Factory for creating AudioLevelNode instances
pub struct AudioLevelNodeFactory;

impl crate::nodes::StreamingNodeFactory for AudioLevelNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: AudioLevelConfig =
            if params.is_null() || params.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                AudioLevelConfig::default()
            } else {
                serde_json::from_value(params.clone()).unwrap_or_default()
            };

        Ok(Box::new(SyncNodeWrapper(AudioLevelNode::new(
            node_id, config,
        ))))
    }

    fn node_type(&self) -> &str {
        "AudioLevelNode"
    }

    fn is_multi_output_streaming(&self) -> bool {
        false // Outputs single JSON event per input
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("AudioLevelNode")
                .description("Monitors audio levels (RMS, peak, LUFS) and emits JSON health events")
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
    fn test_rms_calculation() {
        // Full scale sine wave has RMS of 1/sqrt(2) ≈ 0.707
        let samples: Vec<f32> = (0..1000).map(|i| (i as f32 * 0.01).sin()).collect();
        let rms = AudioLevelNode::calculate_rms(&samples);
        assert!((rms - 0.707).abs() < 0.05);
    }

    #[test]
    fn test_silence_detection() {
        let samples = vec![0.0f32; 1000];
        let rms = AudioLevelNode::calculate_rms(&samples);
        let rms_db = AudioLevelNode::to_db(rms);
        assert!(rms_db <= -60.0);
    }

    #[test]
    fn test_low_volume_detection() {
        // -20dB is about 0.1 amplitude
        let samples: Vec<f32> = (0..1000).map(|i| 0.1 * (i as f32 * 0.01).sin()).collect();
        let rms = AudioLevelNode::calculate_rms(&samples);
        let rms_db = AudioLevelNode::to_db(rms);
        assert!(rms_db < -15.0 && rms_db > -25.0);
    }
}
