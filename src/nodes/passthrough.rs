//! PassThroughNode - Simple pass-through node for testing
//!
//! This node simply returns its input unchanged, useful for testing
//! the streaming pipeline infrastructure.

use crate::data::RuntimeData;
use crate::nodes::SyncStreamingNode;
use crate::Error;

/// PassThroughNode that returns input unchanged
pub struct PassThroughNode {
    pub id: String,
}

impl PassThroughNode {
    pub fn new(id: String, _params: &str) -> Result<Self, Error> {
        Ok(Self { id })
    }
}

impl SyncStreamingNode for PassThroughNode {
    fn node_type(&self) -> &str {
        "PassThrough"
    }

    fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::audio_samples::AudioSamples;
    use crate::transport::data::participant;

    #[test]
    fn preserves_participant_metadata_on_audio_frames() {
        let node = PassThroughNode::new("pass".into(), "{}").unwrap();
        let input = RuntimeData::Audio {
            samples: AudioSamples::from(vec![0.0, 0.25]),
            sample_rate: 16_000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: Some(serde_json::json!({
                participant::ID: "alice",
                participant::ROLE: participant::role::USER,
                participant::MODALITY: participant::modality::AUDIO,
            })),
        };

        let output = node.process(input).unwrap();

        match output {
            RuntimeData::Audio { metadata, .. } => {
                let metadata = metadata.unwrap();
                assert_eq!(metadata[participant::ID], "alice");
                assert_eq!(metadata[participant::ROLE], "user");
                assert_eq!(metadata[participant::MODALITY], "audio");
            }
            _ => panic!("expected Audio output"),
        }
    }
}
