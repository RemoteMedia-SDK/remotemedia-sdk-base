//! Streaming sample quantization with direct output allocation.
//!
//! RuntimeData::Audio carries F32 samples. I16/I32 targets retain the existing
//! quantize-then-dequantize semantics of FastFormatConverter, computing directly
//! into one output loan rather than constructing intermediate integer buffers.
//! F32 is an owning passthrough. This node does not emit native I16/I32 wire data.

use crate::audio::buffer::AudioFormat;
use crate::capabilities::{
    AudioConstraints, AudioSampleFormat, CapabilityBehavior, ConstraintValue, MediaCapabilities,
    MediaConstraints,
};
use crate::data::RuntimeData;
use crate::error::{Error, Result};
use crate::nodes::schema::{NodeSchema, RuntimeDataType};
use crate::nodes::{AsyncStreamingNode, StreamingNode, StreamingNodeFactory};
use async_trait::async_trait;
use remotemedia_traits::runtime_context::NodeRuntimeContextRead;
use remotemedia_types::{HeapOutputAllocator, OutputAllocator};
use serde_json::Value;
use std::sync::Arc;

pub const NODE_TYPE: &str = "FastFormatConverterNode";

/// Stateless quantization into the execution context's selected storage.
pub struct AudioFormatConverterStreamingNode {
    target_format: AudioFormat,
}

impl AudioFormatConverterStreamingNode {
    pub fn new(target_format: AudioFormat) -> Self {
        Self { target_format }
    }

    fn process_allocated(
        &self,
        mut data: RuntimeData,
        allocator: Option<&dyn OutputAllocator>,
    ) -> Result<RuntimeData> {
        if self.target_format == AudioFormat::F32 {
            return Ok(data);
        }
        let RuntimeData::Audio { samples, .. } = &mut data else {
            return Ok(data);
        };
        let allocator = allocator.ok_or_else(|| {
            Error::Execution("format converter requires an output allocator".into())
        })?;
        let mut output = allocator.allocate_audio(samples.len()).map_err(|error| {
            Error::Execution(format!("format converter allocation failed: {error}"))
        })?;
        // Keep the same clamp, saturating cast, f32 rounding and division as
        // FastFormatConverter's two passes, including non-finite inputs.
        match self.target_format {
            AudioFormat::I16 => {
                for (out, sample) in output.iter_mut().zip(samples.iter()) {
                    *out = (sample.clamp(-1.0, 1.0) * 32767.0) as i16 as f32 / 32767.0;
                }
            }
            AudioFormat::I32 => {
                for (out, sample) in output.iter_mut().zip(samples.iter()) {
                    *out = (sample.clamp(-1.0, 1.0) * 2147483647.0) as i32 as f32 / 2147483647.0;
                }
            }
            AudioFormat::F32 => unreachable!("identity returned before allocation"),
        }
        *samples = output.freeze().into();
        Ok(data)
    }
}

#[async_trait]
impl AsyncStreamingNode for AudioFormatConverterStreamingNode {
    fn node_type(&self) -> &str {
        NODE_TYPE
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        self.process_allocated(data, Some(&HeapOutputAllocator))
    }

    async fn process_with_ctx(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData> {
        let allocator = ctx.output_allocator();
        self.process_allocated(data, allocator.as_deref())
    }

    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: F,
    ) -> Result<usize>
    where
        F: FnMut(RuntimeData) -> Result<()> + Send,
    {
        callback(self.process_with_ctx(data, ctx).await?)?;
        Ok(1)
    }
}

/// Factory entry. Declared `Configured` because the node's input and output
/// capabilities are fully determined by the `target_format` param — the
/// resolver doesn't need to look downstream.
pub struct FastFormatConverterNodeFactory;

impl FastFormatConverterNodeFactory {
    fn parse_format(s: &str) -> Option<AudioFormat> {
        match s.to_ascii_lowercase().as_str() {
            "f32" | "float32" => Some(AudioFormat::F32),
            "i16" | "int16" | "s16" => Some(AudioFormat::I16),
            "i32" | "int32" | "s32" => Some(AudioFormat::I32),
            _ => None,
        }
    }

    fn parse_sample_format(s: &str) -> Option<AudioSampleFormat> {
        match s.to_ascii_lowercase().as_str() {
            "f32" | "float32" => Some(AudioSampleFormat::F32),
            "i16" | "int16" | "s16" => Some(AudioSampleFormat::I16),
            "i32" | "int32" | "s32" => Some(AudioSampleFormat::I32),
            "u8" | "uint8" => Some(AudioSampleFormat::U8),
            _ => None,
        }
    }
}

impl StreamingNodeFactory for FastFormatConverterNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>> {
        let target_str = params
            .get("target_format")
            .or_else(|| params.get("targetFormat"))
            .and_then(|v| v.as_str())
            .unwrap_or("f32");

        let target_format = Self::parse_format(target_str).ok_or_else(|| {
            Error::ConfigError(format!(
                "FastFormatConverterNode: unsupported target_format '{}'. \
                 Supported: f32, i16, i32 (u8 declared but not implemented).",
                target_str
            ))
        })?;

        let node = AudioFormatConverterStreamingNode::new(target_format);
        Ok(Box::new(crate::nodes::AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        NODE_TYPE
    }

    fn capability_behavior(&self) -> CapabilityBehavior {
        CapabilityBehavior::Configured
    }

    fn media_capabilities(&self, params: &Value) -> Option<MediaCapabilities> {
        // Input format defaults to "accept any" so the negotiation layer can
        // wire any upstream into this node; output is locked to the configured
        // target. Sample rate and channels are fully passthrough — we don't
        // touch them.
        let source_format = params
            .get("source_format")
            .or_else(|| params.get("sourceFormat"))
            .and_then(|v| v.as_str())
            .and_then(Self::parse_sample_format);

        let target_format = params
            .get("target_format")
            .or_else(|| params.get("targetFormat"))
            .and_then(|v| v.as_str())
            .and_then(Self::parse_sample_format)
            .unwrap_or(AudioSampleFormat::F32);

        let input = AudioConstraints {
            sample_rate: None,
            channels: None,
            format: source_format.map(ConstraintValue::Exact),
        };
        let output = AudioConstraints {
            sample_rate: None,
            channels: None,
            format: Some(ConstraintValue::Exact(target_format)),
        };

        Some(MediaCapabilities::with_input_output(
            MediaConstraints::Audio(input),
            MediaConstraints::Audio(output),
        ))
    }

    fn schema(&self) -> Option<NodeSchema> {
        use crate::nodes::schema::LatencyClass;
        Some(
            NodeSchema::new(NODE_TYPE)
                .description(
                    "Audio sample-format converter (F32/I16/I32). Quantize-then-dequantize \
                     with direct output allocation; useful for capability negotiation \
                     of declared sample formats when the runtime carries audio as F32.",
                )
                .category("audio")
                .accepts([RuntimeDataType::Audio])
                .produces([RuntimeDataType::Audio])
                .config_schema(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "target_format": {
                            "type": "string",
                            "enum": ["f32", "i16", "i32"],
                            "description": "Desired output sample format. The streaming transport always carries F32 today; selecting i16/i32 runs a quantize-then-dequantize roundtrip."
                        },
                        "source_format": {
                            "type": "string",
                            "enum": ["f32", "i16", "i32", "u8"],
                            "description": "Optional declared input format for capability negotiation. Does not affect runtime processing."
                        }
                    },
                    "additionalProperties": false
                }))
                .capabilities(crate::nodes::schema::NodeCapabilitiesSchema {
                    parallelizable: true,
                    batch_aware: false,
                    supports_control: false,
                    latency_class: LatencyClass::Realtime,
                }),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::audio_samples::AudioSamples;
    use crate::transport::data::participant;

    fn audio_frame(samples: Vec<f32>) -> RuntimeData {
        RuntimeData::Audio {
            samples: AudioSamples::from(samples),
            sample_rate: 48_000,
            channels: 1,
            stream_id: None,
            timestamp_us: Some(123),
            arrival_ts_us: None,
            metadata: None,
        }
    }

    #[test]
    fn missing_allocator_rejects_only_transforming_audio() {
        let node = AudioFormatConverterStreamingNode::new(AudioFormat::I16);
        assert!(node
            .process_allocated(audio_frame(vec![0.5]), None)
            .is_err());
        assert!(node
            .process_allocated(RuntimeData::Text("control".into()), None)
            .is_ok());
        let identity = AudioFormatConverterStreamingNode::new(AudioFormat::F32);
        let input = vec![f32::from_bits(0x7fc01234), -0.0];
        let pointer = input.as_ptr();
        let output = identity
            .process_allocated(audio_frame(input), None)
            .unwrap();
        assert!(matches!(output, RuntimeData::Audio { samples, .. }
            if samples.as_ptr() == pointer && samples[0].to_bits() == 0x7fc01234
                && samples[1].to_bits() == (-0.0f32).to_bits()));
    }

    #[tokio::test]
    async fn passthrough_non_audio() {
        let node = AudioFormatConverterStreamingNode::new(AudioFormat::I16);
        let input = RuntimeData::Text("hello".into());
        let out = node.process(input.clone()).await.unwrap();
        // Text bypasses conversion unchanged
        match (input, out) {
            (RuntimeData::Text(a), RuntimeData::Text(b)) => assert_eq!(a, b),
            _ => panic!("expected Text passthrough"),
        }
    }

    #[tokio::test]
    async fn f32_target_is_identity() {
        let node = AudioFormatConverterStreamingNode::new(AudioFormat::F32);
        let samples = vec![0.0, 0.25, -0.5, 0.75];
        let out = node.process(audio_frame(samples.clone())).await.unwrap();
        match out {
            RuntimeData::Audio { samples: s, .. } => {
                assert_eq!(s.as_ref(), samples.as_slice());
            }
            _ => panic!("expected Audio output"),
        }
    }

    #[tokio::test]
    async fn i16_roundtrip_quantization_within_tolerance() {
        // 16-bit quantization error bound = 1 / 32767 ≈ 3.05e-5 per sample.
        // Allow a generous 1e-4 envelope to absorb the clamp + cast rounding.
        let node = AudioFormatConverterStreamingNode::new(AudioFormat::I16);
        let samples = vec![0.0, 0.25, -0.5, 0.75, -0.9999, 0.9999];
        let out = node.process(audio_frame(samples.clone())).await.unwrap();
        match out {
            RuntimeData::Audio { samples: s, .. } => {
                for (i, (orig, got)) in samples.iter().zip(s.as_ref().iter()).enumerate() {
                    let err = (orig - got).abs();
                    assert!(err < 1.0e-4, "sample {i}: orig={orig} got={got} err={err}",);
                }
            }
            _ => panic!("expected Audio output"),
        }
    }

    #[tokio::test]
    async fn metadata_and_timestamps_carry_through() {
        let node = AudioFormatConverterStreamingNode::new(AudioFormat::I16);
        let mut input = audio_frame(vec![0.1, 0.2]);
        if let RuntimeData::Audio { metadata, .. } = &mut input {
            *metadata = Some(serde_json::json!({
                participant::ID: "caller-1",
                participant::ROLE: participant::role::CLIENT,
                participant::TRACK_ID: "sip-leg-a",
                participant::MODALITY: participant::modality::AUDIO,
            }));
        }

        let out = node.process(input).await.unwrap();
        match out {
            RuntimeData::Audio {
                timestamp_us,
                sample_rate,
                channels,
                metadata,
                ..
            } => {
                assert_eq!(timestamp_us, Some(123));
                assert_eq!(sample_rate, 48_000);
                assert_eq!(channels, 1);
                let metadata = metadata.unwrap();
                assert_eq!(metadata[participant::ID], "caller-1");
                assert_eq!(metadata[participant::ROLE], "client");
                assert_eq!(metadata[participant::TRACK_ID], "sip-leg-a");
                assert_eq!(metadata[participant::MODALITY], "audio");
            }
            _ => panic!("expected Audio output"),
        }
    }

    #[test]
    fn factory_declares_configured_caps_for_target_format() {
        let factory = FastFormatConverterNodeFactory;
        assert_eq!(
            factory.capability_behavior(),
            CapabilityBehavior::Configured
        );

        let caps = factory
            .media_capabilities(&serde_json::json!({"target_format": "i16"}))
            .unwrap();
        let output = caps.default_output().unwrap();
        match output {
            MediaConstraints::Audio(a) => {
                assert!(matches!(
                    a.format,
                    Some(ConstraintValue::Exact(AudioSampleFormat::I16))
                ));
            }
            _ => panic!("expected audio output constraints"),
        }
    }

    #[test]
    fn factory_rejects_unknown_format() {
        let factory = FastFormatConverterNodeFactory;
        // `Box<dyn StreamingNode>` doesn't implement `Debug`, so we can't use
        // `unwrap_err()` directly — match the Result instead.
        let result = factory.create(
            "n".into(),
            &serde_json::json!({"target_format": "wat"}),
            None,
        );
        match result {
            Ok(_) => panic!("expected error for unknown target_format"),
            Err(err) => assert!(format!("{err}").contains("unsupported target_format")),
        }
    }
}
