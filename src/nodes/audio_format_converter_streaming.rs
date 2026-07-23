//! Streaming wrapper for [`FastFormatConverter`].
//!
//! Adapts the synchronous [`FastFormatConverter`] (a [`FastAudioNode`]) into
//! the async streaming pipeline ([`AsyncStreamingNode`]) so the capability
//! negotiation layer can splice it in for declared audio sample-format
//! mismatches between connected nodes.
//!
//! # Data-plane caveat
//!
//! [`RuntimeData::Audio.samples`] is **always** an F32-backed
//! [`crate::data::audio_samples::AudioSamples`] today — there is no I16/I32
//! variant in the streaming runtime's transport. Concretely, this node runs
//! a **quantize-then-dequantize roundtrip** through [`FastFormatConverter`]:
//!
//! 1. Take the F32 samples carried by `RuntimeData::Audio`.
//! 2. Build an `AudioBuffer::F32`, convert to the configured `target_format`
//!    via [`FastFormatConverter`].
//! 3. Convert that back to F32 to repack into the next `RuntimeData::Audio`.
//!
//! For a 16-bit target this introduces ~-96 dB of quantization noise — below
//! the audibility threshold for any realistic signal, but technically not a
//! no-op. The roundtrip is honest about the data plane's constraint: any node
//! downstream that declares it accepts only `target_format` will see audio
//! whose values are quantized to that format's resolution, even though the
//! transport byte-shape remains F32.
//!
//! When a future runtime carries multi-format `AudioSamples` natively, this
//! wrapper should be updated to bypass the F32 repack and emit the target
//! format directly.

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::Value;

use crate::audio::buffer::{AudioBuffer, AudioData, AudioFormat};
use crate::capabilities::{
    AudioConstraints, AudioSampleFormat, CapabilityBehavior, ConstraintValue, MediaCapabilities,
    MediaConstraints,
};
use crate::data::RuntimeData;
use crate::error::{Error, Result};
use crate::nodes::audio::{FastAudioNode, FastFormatConverter};
use crate::nodes::schema::{NodeSchema, RuntimeDataType};
use crate::nodes::{AsyncStreamingNode, StreamingNode, StreamingNodeFactory};

/// `node_type` exposed to manifests and the registry.
pub const NODE_TYPE: &str = "FastFormatConverterNode";

/// Streaming wrapper around [`FastFormatConverter`].
///
/// Holds the inner converter behind a [`parking_lot::Mutex`] because the
/// underlying `FastAudioNode::process_audio` is `&mut self`. The lock is only
/// held across a synchronous CPU pass (no `.await`), so `parking_lot::Mutex`
/// is preferred over the async-aware alternative — same rationale as
/// `ResampleStreamingNode`.
pub struct AudioFormatConverterStreamingNode {
    inner: Mutex<FastFormatConverter>,
    /// Buffer format the wrapper is configured to "produce". Even though the
    /// data leaves as F32 in `RuntimeData::Audio`, the inner converter runs a
    /// quantize roundtrip into this format so the values match what a future
    /// native-format consumer would see.
    #[allow(dead_code)]
    target_format: AudioFormat,
}

impl AudioFormatConverterStreamingNode {
    pub fn new(target_format: AudioFormat) -> Self {
        Self {
            inner: Mutex::new(FastFormatConverter::new(target_format)),
            target_format,
        }
    }
}

#[async_trait]
impl AsyncStreamingNode for AudioFormatConverterStreamingNode {
    fn node_type(&self) -> &str {
        NODE_TYPE
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        // Non-audio passes through unchanged. Mirrors ResampleStreamingNode's
        // behavior — keeps the node safe to drop into a pipeline that
        // multiplexes audio + text + control frames on the same connection.
        if data.data_type() != "audio" {
            return Ok(data);
        }

        let (samples, sample_rate, channels, stream_id, ts_us, arrival_us, metadata) = match data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                stream_id,
                timestamp_us,
                arrival_ts_us,
                metadata,
            } => (
                samples,
                sample_rate,
                channels,
                stream_id,
                timestamp_us,
                arrival_ts_us,
                metadata,
            ),
            other => return Ok(other),
        };

        // Step 1: pack the f32 samples into an AudioBuffer::F32. We deref via
        // `as_ref` to get a `&[f32]` and only allocate when we cross the
        // converter boundary — keeps the steady-state hot path cheap.
        let f32_slice: &[f32] = samples.as_ref();
        let f32_audio = AudioData::new(
            AudioBuffer::new_f32(f32_slice.to_vec()),
            sample_rate,
            channels as usize,
        );

        // Step 2: F32 → target_format (the meaningful quantization step).
        let converted = {
            let mut inner = self.inner.lock();
            inner.process_audio(f32_audio)?
        };

        // Step 3: target_format → F32 for the wire. When target was already
        // F32 (caller misconfigured us) this short-circuits to a memcpy of the
        // existing buffer.
        let out_f32: Vec<f32> = match converted.buffer.format() {
            AudioFormat::F32 => converted
                .buffer
                .as_f32()
                .map(|s| s.to_vec())
                .unwrap_or_default(),
            AudioFormat::I16 => {
                let mut back = FastFormatConverter::new(AudioFormat::F32);
                let back_data = back.process_audio(AudioData::new(
                    converted.buffer.clone(),
                    converted.sample_rate,
                    converted.channels,
                ))?;
                back_data
                    .buffer
                    .as_f32()
                    .map(|s| s.to_vec())
                    .unwrap_or_default()
            }
            AudioFormat::I32 => {
                let mut back = FastFormatConverter::new(AudioFormat::F32);
                let back_data = back.process_audio(AudioData::new(
                    converted.buffer.clone(),
                    converted.sample_rate,
                    converted.channels,
                ))?;
                back_data
                    .buffer
                    .as_f32()
                    .map(|s| s.to_vec())
                    .unwrap_or_default()
            }
        };

        Ok(RuntimeData::Audio {
            samples: out_f32.into(),
            sample_rate,
            channels,
            stream_id,
            timestamp_us: ts_us,
            arrival_ts_us: arrival_us,
            metadata,
        })
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
                     wrapper over FastFormatConverter; mainly useful for capability negotiation \
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
