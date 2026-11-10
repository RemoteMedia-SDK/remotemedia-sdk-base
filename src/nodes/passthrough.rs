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
    fn factory_declares_metadata_or_explicit_bounded_forwarding() {
        use crate::nodes::{streaming_registry::PassThroughNodeFactory, StreamingNodeFactory};
        use remotemedia_traits::buffer_contract::*;
        let factory = PassThroughNodeFactory;
        let metadata = factory
            .try_buffer_contract(&serde_json::json!({}))
            .unwrap()
            .unwrap();
        assert_eq!(
            metadata,
            BufferContract::metadata_only(BufferProtocol::RustOwners)
        );
        let params = serde_json::json!({
            "passthrough_layouts":["audio_interleaved_f32","binary"],
            "buffer_limits":{"domain":"session_mapped","max_input_bytes":64,"max_output_bytes":64,
                "retention":{"owners":2,"bytes":8192}}
        });
        let contract = factory.try_buffer_contract(&params).unwrap().unwrap();
        assert_eq!(contract.production, BufferProduction::ForwardInput);
        assert_eq!(
            contract.inputs[""].domains,
            vec![BufferDomain::SessionMapped]
        );
        assert_eq!(
            contract.inputs[""].layouts,
            vec![BufferLayout::AudioInterleavedF32, BufferLayout::Binary]
        );
        for invalid in [
            serde_json::json!({"passthrough_layouts":["binary"]}),
            serde_json::json!({"buffer_limits":params["buffer_limits"]}),
            {
                let mut value = params.clone();
                value["buffer_limits"]["max_output_bytes"] = 32.into();
                value
            },
        ] {
            assert!(factory.try_buffer_contract(&invalid).is_err());
            assert!(factory.create("pass".into(), &invalid, None).is_err());
        }
    }

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
