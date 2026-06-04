//! Channel Balance Detection Node
//!
//! Detects audio channel imbalance (one-sided audio) in stereo streams.

use crate::data::RuntimeData;
use crate::nodes::{StreamingNode, SyncNodeWrapper, SyncStreamingNode};
use crate::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Channel balance detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelBalanceEvent {
    /// RMS level of left channel in dB
    pub left_rms_db: f32,
    /// RMS level of right channel in dB
    pub right_rms_db: f32,
    /// Balance ratio (L/R) - 1.0 means balanced, 0.0 means right only, inf means left only
    pub balance_ratio: f32,
    /// Imbalance in dB (positive = left louder, negative = right louder)
    pub imbalance_db: f32,
    /// Whether significant imbalance was detected
    pub is_imbalanced: bool,
    /// Whether one channel is completely silent
    pub has_dead_channel: bool,
    /// Which channel is dead (if any): "left", "right", or "none"
    pub dead_channel: String,
    /// Health: 1.0 = healthy (balanced), 0.0 = unhealthy (dead channel or severe imbalance)
    pub health: f32,
    /// Stream identifier
    pub stream_id: Option<String>,
    /// Timestamp in microseconds
    pub timestamp_us: Option<u64>,
}

/// Configuration for channel balance detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelBalanceConfig {
    /// Imbalance threshold in dB to trigger alert (default: 10dB)
    pub imbalance_threshold_db: f32,
    /// Threshold in dB below which a channel is considered "dead" (default: -60dB)
    pub dead_channel_threshold_db: f32,
}

impl Default for ChannelBalanceConfig {
    fn default() -> Self {
        Self {
            imbalance_threshold_db: 10.0,
            dead_channel_threshold_db: -60.0,
        }
    }
}

/// Node that detects channel imbalance in stereo audio
pub struct ChannelBalanceNode {
    node_id: String,
    config: ChannelBalanceConfig,
}

impl ChannelBalanceNode {
    /// Create a new ChannelBalanceNode
    pub fn new(node_id: String, config: ChannelBalanceConfig) -> Self {
        Self { node_id, config }
    }

    /// Calculate RMS of a channel
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

    fn process_audio(&self, input: RuntimeData) -> Result<RuntimeData, Error> {
        let (samples, channels, stream_id, timestamp_us) = match &input {
            RuntimeData::Audio {
                samples,
                channels,
                stream_id,
                timestamp_us,
                ..
            } => (samples.clone(), *channels, stream_id.clone(), *timestamp_us),
            _ => return Ok(input), // Pass through non-audio data
        };

        // Only process stereo audio
        if channels != 2 {
            // For mono, output a "balanced" event (no imbalance possible)
            let event = ChannelBalanceEvent {
                left_rms_db: 0.0,
                right_rms_db: 0.0,
                balance_ratio: 1.0,
                imbalance_db: 0.0,
                is_imbalanced: false,
                has_dead_channel: false,
                dead_channel: "none".to_string(),
                health: 1.0, // Mono is always "balanced"
                stream_id: stream_id.clone(),
                timestamp_us,
            };
            let mut event_json = serde_json::to_value(&event).unwrap_or(Value::Null);
            if let Value::Object(ref mut map) = event_json {
                map.insert(
                    "_schema".to_string(),
                    Value::String("channel_balance_event".to_string()),
                );
            }
            return Ok(RuntimeData::Json(event_json));
        }

        // Extract channels from interleaved stereo
        let left = Self::extract_left(&samples);
        let right = Self::extract_right(&samples);

        // Calculate RMS for each channel
        let left_rms = Self::calculate_rms(&left);
        let right_rms = Self::calculate_rms(&right);
        let left_rms_db = Self::to_db(left_rms);
        let right_rms_db = Self::to_db(right_rms);

        // Calculate balance
        let balance_ratio = if right_rms > 0.0 {
            left_rms / right_rms
        } else if left_rms > 0.0 {
            f32::INFINITY
        } else {
            1.0 // Both silent
        };

        let imbalance_db = left_rms_db - right_rms_db;

        // Detect conditions
        let left_dead = left_rms_db <= self.config.dead_channel_threshold_db;
        let right_dead = right_rms_db <= self.config.dead_channel_threshold_db;
        let has_dead_channel = (left_dead && !right_dead) || (!left_dead && right_dead);
        let dead_channel = if left_dead && !right_dead {
            "left".to_string()
        } else if right_dead && !left_dead {
            "right".to_string()
        } else {
            "none".to_string()
        };

        let is_imbalanced =
            has_dead_channel || imbalance_db.abs() >= self.config.imbalance_threshold_db;

        // Calculate health: 1.0 = healthy, 0.0 = unhealthy
        // Dead channel = 0.0, imbalance degrades linearly based on threshold
        let health = if has_dead_channel {
            0.0
        } else {
            (1.0 - (imbalance_db.abs() / self.config.imbalance_threshold_db).min(1.0)).max(0.0)
        };

        // Create event
        let event = ChannelBalanceEvent {
            left_rms_db,
            right_rms_db,
            balance_ratio,
            imbalance_db,
            is_imbalanced,
            has_dead_channel,
            dead_channel: dead_channel.clone(),
            health,
            stream_id: stream_id.clone(),
            timestamp_us,
        };

        // Log if issues detected
        if has_dead_channel {
            tracing::debug!(
                "ChannelBalanceNode {}: Dead {} channel detected",
                self.node_id,
                dead_channel
            );
        } else if is_imbalanced {
            tracing::debug!(
                "ChannelBalanceNode {}: Channel imbalance detected ({:.1}dB)",
                self.node_id,
                imbalance_db
            );
        }

        // Output the balance event as JSON
        let mut event_json = serde_json::to_value(&event).unwrap_or(Value::Null);
        if let Value::Object(ref mut map) = event_json {
            map.insert(
                "_schema".to_string(),
                Value::String("channel_balance_event".to_string()),
            );
        }
        Ok(RuntimeData::Json(event_json))
    }
}

// Phase A-Wave 1: `SyncStreamingNode` — body is sync, state is
// read-only config. Wrapped through `SyncNodeWrapper` at the registry
// boundary.
impl SyncStreamingNode for ChannelBalanceNode {
    fn node_type(&self) -> &str {
        "ChannelBalanceNode"
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

/// Factory for creating ChannelBalanceNode instances
pub struct ChannelBalanceNodeFactory;

impl crate::nodes::StreamingNodeFactory for ChannelBalanceNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: ChannelBalanceConfig =
            if params.is_null() || params.as_object().map(|o| o.is_empty()).unwrap_or(true) {
                ChannelBalanceConfig::default()
            } else {
                serde_json::from_value(params.clone()).unwrap_or_default()
            };

        Ok(Box::new(SyncNodeWrapper(ChannelBalanceNode::new(
            node_id, config,
        ))))
    }

    fn node_type(&self) -> &str {
        "ChannelBalanceNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("ChannelBalanceNode")
                .description(
                    "Monitors channel balance in stereo audio and emits JSON health events",
                )
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
    fn test_balanced_stereo() {
        // Create balanced stereo: interleaved L,R,L,R...
        let samples: Vec<f32> = (0..2000)
            .map(|i| 0.5 * ((i / 2) as f32 * 0.01).sin())
            .collect();

        let left = ChannelBalanceNode::extract_left(&samples);
        let right = ChannelBalanceNode::extract_right(&samples);

        let left_rms = ChannelBalanceNode::calculate_rms(&left);
        let right_rms = ChannelBalanceNode::calculate_rms(&right);

        assert!((left_rms - right_rms).abs() < 0.01);
    }

    #[test]
    fn test_one_sided_audio() {
        // Create one-sided stereo: left has audio, right is silent
        let samples: Vec<f32> = (0..2000)
            .map(|i| {
                if i % 2 == 0 {
                    0.5 * ((i / 2) as f32 * 0.01).sin()
                } else {
                    0.0
                }
            })
            .collect();

        let left = ChannelBalanceNode::extract_left(&samples);
        let right = ChannelBalanceNode::extract_right(&samples);

        let left_rms = ChannelBalanceNode::calculate_rms(&left);
        let right_rms = ChannelBalanceNode::calculate_rms(&right);
        let right_db = ChannelBalanceNode::to_db(right_rms);

        assert!(left_rms > 0.1);
        assert!(right_db <= -60.0);
    }
}
