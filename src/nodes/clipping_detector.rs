//! Clipping Detection Node
//!
//! Detects audio clipping/distortion by analyzing peak saturation ratio and crest factor.

use crate::data::RuntimeData;
use crate::nodes::{StreamingNode, SyncNodeWrapper, SyncStreamingNode};
use crate::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Clipping detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClippingEvent {
    /// Percentage of samples at or near clipping threshold (±0.99)
    pub saturation_ratio: f32,
    /// Crest factor (peak / RMS ratio in dB) - low values indicate heavy compression/clipping
    pub crest_factor_db: f32,
    /// Whether clipping was detected
    pub is_clipping: bool,
    /// Health: 1.0 = healthy (no clipping), 0.0 = unhealthy (severe clipping)
    pub health: f32,
    /// Stream identifier
    pub stream_id: Option<String>,
    /// Timestamp in microseconds
    pub timestamp_us: Option<u64>,
}

/// Configuration for clipping detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClippingConfig {
    /// Threshold for considering a sample "saturated" (default: 0.99)
    pub saturation_threshold: f32,
    /// Percentage of saturated samples to trigger clipping alert (default: 1.0%)
    pub saturation_ratio_threshold: f32,
    /// Crest factor threshold in dB - below this indicates clipping (default: 3.0 dB)
    pub crest_factor_threshold_db: f32,
}

impl Default for ClippingConfig {
    fn default() -> Self {
        Self {
            saturation_threshold: 0.99,
            saturation_ratio_threshold: 1.0, // 1%
            crest_factor_threshold_db: 3.0,
        }
    }
}

/// Node that detects audio clipping and distortion
pub struct ClippingDetectorNode {
    node_id: String,
    config: ClippingConfig,
}

impl ClippingDetectorNode {
    /// Create a new ClippingDetectorNode
    pub fn new(node_id: String, config: ClippingConfig) -> Self {
        Self { node_id, config }
    }

    /// Calculate saturation ratio (percentage of samples at clipping threshold)
    fn calculate_saturation_ratio(samples: &[f32], threshold: f32) -> f32 {
        if samples.is_empty() {
            return 0.0;
        }
        let saturated_count = samples.iter().filter(|s| s.abs() >= threshold).count();
        (saturated_count as f32 / samples.len() as f32) * 100.0
    }

    /// Calculate crest factor (peak / RMS ratio in dB)
    fn calculate_crest_factor(samples: &[f32]) -> f32 {
        if samples.is_empty() {
            return 0.0;
        }

        let peak = samples.iter().map(|s| s.abs()).fold(0.0f32, f32::max);
        let sum_squares: f32 = samples.iter().map(|s| s * s).sum();
        let rms = (sum_squares / samples.len() as f32).sqrt();

        if rms <= 0.0 {
            return 20.0; // High crest factor for silence
        }

        20.0 * (peak / rms).log10()
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

        // Calculate metrics
        let saturation_ratio =
            Self::calculate_saturation_ratio(&samples, self.config.saturation_threshold);
        let crest_factor_db = Self::calculate_crest_factor(&samples);

        // Detect clipping - either high saturation ratio OR low crest factor
        let is_clipping = saturation_ratio >= self.config.saturation_ratio_threshold
            || crest_factor_db <= self.config.crest_factor_threshold_db;

        // Calculate health: 1.0 = healthy, 0.0 = unhealthy
        // Based on saturation ratio relative to threshold (0% = healthy, threshold = unhealthy)
        let health =
            (1.0 - (saturation_ratio / self.config.saturation_ratio_threshold).min(1.0)).max(0.0);

        // Create event
        let event = ClippingEvent {
            saturation_ratio,
            crest_factor_db,
            is_clipping,
            health,
            stream_id: stream_id.clone(),
            timestamp_us,
        };

        // Log if clipping detected
        if is_clipping {
            tracing::debug!(
                "ClippingDetectorNode {}: Clipping detected (saturation: {:.1}%, crest: {:.1}dB)",
                self.node_id,
                saturation_ratio,
                crest_factor_db
            );
        }

        // Output the clipping event as JSON
        let mut event_json = serde_json::to_value(&event).unwrap_or(Value::Null);
        if let Value::Object(ref mut map) = event_json {
            map.insert(
                "_schema".to_string(),
                Value::String("clipping_event".to_string()),
            );
        }
        Ok(RuntimeData::Json(event_json))
    }
}

// Phase A-Wave 1: `SyncStreamingNode` — body is sync, state is
// read-only config. Wrapped through `SyncNodeWrapper` at the registry
// boundary.
impl SyncStreamingNode for ClippingDetectorNode {
    fn node_type(&self) -> &str {
        "ClippingDetectorNode"
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

/// Factory for creating ClippingDetectorNode instances
pub struct ClippingDetectorNodeFactory;

impl crate::nodes::StreamingNodeFactory for ClippingDetectorNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: ClippingConfig =
            if params.is_null() || params.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                ClippingConfig::default()
            } else {
                serde_json::from_value(params.clone()).unwrap_or_default()
            };

        Ok(Box::new(SyncNodeWrapper(ClippingDetectorNode::new(
            node_id, config,
        ))))
    }

    fn node_type(&self) -> &str {
        "ClippingDetectorNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("ClippingDetectorNode")
                .description("Detects audio clipping and emits JSON events with clipping metrics")
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
    fn test_saturation_ratio_clean() {
        // Normal audio should have low saturation
        let samples: Vec<f32> = (0..1000).map(|i| 0.5 * (i as f32 * 0.01).sin()).collect();
        let ratio = ClippingDetectorNode::calculate_saturation_ratio(&samples, 0.99);
        assert!(ratio < 1.0);
    }

    #[test]
    fn test_saturation_ratio_clipped() {
        // Clipped audio has many samples at ±1.0
        let samples: Vec<f32> = (0..1000)
            .map(|i| {
                let s = 2.0 * (i as f32 * 0.01).sin();
                s.max(-1.0).min(1.0)
            })
            .collect();
        let ratio = ClippingDetectorNode::calculate_saturation_ratio(&samples, 0.99);
        assert!(ratio > 1.0);
    }

    #[test]
    fn test_crest_factor_clean() {
        // Clean sine wave has crest factor of ~3dB
        let samples: Vec<f32> = (0..10000).map(|i| (i as f32 * 0.01).sin()).collect();
        let crest = ClippingDetectorNode::calculate_crest_factor(&samples);
        assert!((crest - 3.0).abs() < 0.5);
    }

    #[test]
    fn test_crest_factor_clipped() {
        // Square wave (heavily clipped) has crest factor of 0dB
        let samples: Vec<f32> = (0..1000)
            .map(|i| if i % 100 < 50 { 1.0 } else { -1.0 })
            .collect();
        let crest = ClippingDetectorNode::calculate_crest_factor(&samples);
        assert!(crest < 1.0);
    }
}
