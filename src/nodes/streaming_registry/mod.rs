//! Default streaming node registry with built-in node factories

use crate::capabilities::{
    AudioConstraints, AudioSampleFormat, CapabilityBehavior, ConstraintValue, MediaCapabilities,
    MediaConstraints,
};
use crate::nodes::calculator::CalculatorNode;
use crate::nodes::passthrough::PassThroughNode;
use remotemedia_traits::runtime_context::InitializeContextRead;
// Temporarily disabled - incomplete implementation
// use crate::nodes::sync_av::SynchronizedAudioVideoNode;
use crate::nodes::video_flip::VideoFlipNode;
// use crate::nodes::video_processor::VideoProcessorNode;
use crate::data::RuntimeData;
#[cfg(feature = "video")]
use crate::nodes::video::{
    VideoDecoderConfig, VideoDecoderNode, VideoEncoderConfig, VideoEncoderNode,
};
use crate::nodes::{
    AsyncNodeWrapper, StreamingNode, StreamingNodeFactory, StreamingNodeRegistry, SyncNodeWrapper,
};
// Note: NodeContext and NodeExecutor are available via crate::nodes if needed
use crate::Error;
use serde_json::Value;
use std::sync::Arc;

// Private sub-module: dead-code Python-wrapping `StreamingNodeFactory`s
// kept for future reference. See `python_factories.rs` for rationale.
#[cfg(feature = "multiprocess")]
#[allow(dead_code)]
mod python_factories;

// Factory implementations for built-in streaming nodes
// NOTE: Factories are pub(crate) to allow registration from core_provider.rs

pub(crate) struct CalculatorNodeFactory;
impl StreamingNodeFactory for CalculatorNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let params_str = params.to_string();
        let node = CalculatorNode::new(node_id, &params_str)?;
        Ok(Box::new(SyncNodeWrapper(node)))
    }

    fn node_type(&self) -> &str {
        "CalculatorNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("CalculatorNode")
                .description(
                    "JSON calculator node — accepts operation + operands and returns result",
                )
                .category("utility")
                .accepts([RuntimeDataType::Json])
                .produces([RuntimeDataType::Json]),
        )
    }
}

// Temporarily disabled - VideoProcessorNode has incomplete implementation
/*
struct VideoProcessorNodeFactory;
impl StreamingNodeFactory for VideoProcessorNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let params_str = params.to_string();
        let node = VideoProcessorNode::new(node_id, &params_str)?;
        Ok(Box::new(SyncNodeWrapper(node)))
    }

    fn node_type(&self) -> &str {
        "VideoProcessorNode"
    }
}
*/

pub(crate) struct VideoFlipNodeFactory;
impl StreamingNodeFactory for VideoFlipNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::video_flip::VideoFlipConfig;
        use crate::nodes::SyncNodeWrapper;
        let config: VideoFlipConfig = serde_json::from_value(params.clone()).unwrap_or_default();
        let node = VideoFlipNode::new(config);
        Ok(Box::new(SyncNodeWrapper(node)))
    }

    fn node_type(&self) -> &str {
        "VideoFlip"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        use crate::nodes::video_flip::VideoFlipConfig;
        Some(
            NodeSchema::new("VideoFlip")
                .description("Flips video frames horizontally or vertically")
                .category("video")
                .accepts([RuntimeDataType::Video])
                .produces([RuntimeDataType::Video])
                .config_schema_from::<VideoFlipConfig>(),
        )
    }
}

#[cfg(feature = "video")]
pub(crate) struct VideoEncoderNodeFactory;

#[cfg(feature = "video")]
impl StreamingNodeFactory for VideoEncoderNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: VideoEncoderConfig = serde_json::from_value(params.clone()).unwrap_or_default();
        let node = VideoEncoderNode::new(config)
            .map_err(|e| Error::Execution(format!("Failed to create VideoEncoder: {}", e)))?;
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "VideoEncoder"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("VideoEncoder")
                .description("Encodes raw video frames to compressed bitstreams (VP8/AV1/H.264)")
                .category("video")
                .accepts([RuntimeDataType::Video])
                .produces([RuntimeDataType::Video])
                .config_schema_from::<VideoEncoderConfig>(),
        )
    }
}

#[cfg(feature = "video")]
pub(crate) struct VideoDecoderNodeFactory;

#[cfg(feature = "video")]
impl StreamingNodeFactory for VideoDecoderNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: VideoDecoderConfig = serde_json::from_value(params.clone()).unwrap_or_default();
        let node = VideoDecoderNode::new(config)
            .map_err(|e| Error::Execution(format!("Failed to create VideoDecoder: {}", e)))?;
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "VideoDecoder"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("VideoDecoder")
                .description("Decodes compressed video bitstreams to raw frames")
                .category("video")
                .accepts([RuntimeDataType::Video])
                .produces([RuntimeDataType::Video])
                .config_schema_from::<VideoDecoderConfig>(),
        )
    }
}

#[cfg(feature = "video")]
pub(crate) struct VideoScalerNodeFactory;

#[cfg(feature = "video")]
impl StreamingNodeFactory for VideoScalerNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::video::{VideoScalerConfig, VideoScalerNode};
        let config: VideoScalerConfig = serde_json::from_value(params.clone()).unwrap_or_default();
        let node = VideoScalerNode::new(config)
            .map_err(|e| Error::Execution(format!("Failed to create VideoScaler: {}", e)))?;
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "VideoScaler"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        use crate::nodes::video::VideoScalerConfig;
        Some(
            NodeSchema::new("VideoScaler")
                .description("Scales/resizes video frames")
                .category("video")
                .accepts([RuntimeDataType::Video])
                .produces([RuntimeDataType::Video])
                .config_schema_from::<VideoScalerConfig>(),
        )
    }
}

#[cfg(feature = "video")]
pub(crate) struct VideoFormatConverterNodeFactory;

#[cfg(feature = "video")]
impl StreamingNodeFactory for VideoFormatConverterNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::video::{VideoFormatConverterConfig, VideoFormatConverterNode};
        let config: VideoFormatConverterConfig =
            serde_json::from_value(params.clone()).unwrap_or_default();
        let node = VideoFormatConverterNode::new(config).map_err(|e| {
            Error::Execution(format!("Failed to create VideoFormatConverter: {}", e))
        })?;
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "VideoFormatConverter"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        use crate::nodes::video::VideoFormatConverterConfig;
        Some(
            NodeSchema::new("VideoFormatConverter")
                .description("Converts between pixel formats (RGB/YUV/NV12)")
                .category("video")
                .accepts([RuntimeDataType::Video])
                .produces([RuntimeDataType::Video])
                .config_schema_from::<VideoFormatConverterConfig>(),
        )
    }
}

// Temporarily disabled - SynchronizedAudioVideoNode has incomplete implementation
/*
struct SynchronizedAudioVideoNodeFactory;
impl StreamingNodeFactory for SynchronizedAudioVideoNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let params_str = params.to_string();
        let node = SynchronizedAudioVideoNode::new(node_id, &params_str)?;
        Ok(Box::new(SyncNodeWrapper(node)))
    }

    fn node_type(&self) -> &str {
        "SynchronizedAudioVideoNode"
    }
}
*/

pub(crate) struct PassThroughNodeFactory;
impl StreamingNodeFactory for PassThroughNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let params_str = params.to_string();
        let node = PassThroughNode::new(node_id, &params_str)?;
        Ok(Box::new(SyncNodeWrapper(node)))
    }

    fn node_type(&self) -> &str {
        "PassThrough"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("PassThrough")
                .description(
                    "Passes input through unchanged — useful for testing and pipeline wiring",
                )
                .category("utility")
                .accepts([
                    RuntimeDataType::Audio,
                    RuntimeDataType::Text,
                    RuntimeDataType::Json,
                    RuntimeDataType::Video,
                ])
                .produces([
                    RuntimeDataType::Audio,
                    RuntimeDataType::Text,
                    RuntimeDataType::Json,
                    RuntimeDataType::Video,
                ]),
        )
    }
}

pub(crate) struct AudioBufferAccumulatorNodeFactory;
impl StreamingNodeFactory for AudioBufferAccumulatorNodeFactory {
    fn create(
        &self,
        _node_id: String, // Reserved for future node identification/logging
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::AudioBufferAccumulatorNode;

        let min_duration_ms = params
            .get("minUtteranceDurationMs")
            .or(params.get("min_utterance_duration_ms"))
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);
        let max_duration_ms = params
            .get("maxUtteranceDurationMs")
            .or(params.get("max_utterance_duration_ms"))
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);

        use crate::nodes::SyncNodeWrapper;
        let node = AudioBufferAccumulatorNode::new(min_duration_ms, max_duration_ms);
        Ok(Box::new(SyncNodeWrapper(node)))
    }

    fn node_type(&self) -> &str {
        "AudioBufferAccumulatorNode"
    }

    fn is_multi_output_streaming(&self) -> bool {
        true // Can output 0 or 1 items (when speech ends)
    }

    fn capability_behavior(&self) -> CapabilityBehavior {
        CapabilityBehavior::Passthrough
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("AudioBufferAccumulatorNode")
                .description("Accumulates audio frames until utterance duration thresholds are met")
                .category("audio")
                .accepts([RuntimeDataType::Audio])
                .produces([RuntimeDataType::Audio]),
        )
    }
}

pub(crate) struct SpeculativeAudioCommitNodeFactory;
impl StreamingNodeFactory for SpeculativeAudioCommitNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::SpeculativeAudioCommitNode;

        let commit_delay_ms = params
            .get("commitDelayMs")
            .or(params.get("commit_delay_ms"))
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);
        let pre_roll_ms = params
            .get("preRollMs")
            .or(params.get("pre_roll_ms"))
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);
        let min_utterance_duration_ms = params
            .get("minUtteranceDurationMs")
            .or(params.get("min_utterance_duration_ms"))
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);
        let max_utterance_duration_ms = params
            .get("maxUtteranceDurationMs")
            .or(params.get("max_utterance_duration_ms"))
            .and_then(|v| v.as_u64())
            .map(|v| v as u32);

        use crate::nodes::SyncNodeWrapper;
        let node = SpeculativeAudioCommitNode::new(
            commit_delay_ms,
            pre_roll_ms,
            min_utterance_duration_ms,
            max_utterance_duration_ms,
        );
        Ok(Box::new(SyncNodeWrapper(node)))
    }

    fn node_type(&self) -> &str {
        "SpeculativeAudioCommitNode"
    }

    fn is_multi_output_streaming(&self) -> bool {
        true // Emits 0 or 1 RuntimeData::Audio per call (committed utterance)
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("SpeculativeAudioCommitNode")
                .description("Speculative audio commit — buffers audio during VAD and commits when speech ends")
                .category("audio")
                .accepts([RuntimeDataType::Audio])
                .produces([RuntimeDataType::Audio]),
        )
    }
}

pub(crate) struct TextCollectorNodeFactory;
impl StreamingNodeFactory for TextCollectorNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::text_collector::{TextCollectorConfig, TextCollectorNode};
        use crate::nodes::SyncNodeWrapper;
        let config: TextCollectorConfig =
            serde_json::from_value(params.clone()).unwrap_or_default();
        let node = TextCollectorNode::with_config(config);
        Ok(Box::new(SyncNodeWrapper(node)))
    }

    fn node_type(&self) -> &str {
        "TextCollectorNode"
    }

    fn is_multi_output_streaming(&self) -> bool {
        true // Can output 0 or multiple items (complete sentences)
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        use crate::nodes::text_collector::TextCollectorConfig;
        Some(
            NodeSchema::new("TextCollectorNode")
                .description("Accumulates streaming text tokens and yields complete sentences")
                .category("text")
                .accepts([RuntimeDataType::Text])
                .produces([RuntimeDataType::Text])
                .config_schema_from::<TextCollectorConfig>(),
        )
    }
}

pub(crate) struct AvatarIntentTapNodeFactory;
impl StreamingNodeFactory for AvatarIntentTapNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::avatar_intent_tap::{AvatarIntentTapConfig, AvatarIntentTapNode};
        use crate::nodes::SyncNodeWrapper;
        let config: AvatarIntentTapConfig = serde_json::from_value(params.clone())
            .map_err(|e| Error::Execution(format!("AvatarIntentTapNode params: {e}")))?;
        let node = AvatarIntentTapNode::with_config(config)?;
        Ok(Box::new(SyncNodeWrapper(node)))
    }

    fn node_type(&self) -> &str {
        "AvatarIntentTapNode"
    }

    fn is_multi_output_streaming(&self) -> bool {
        // Emits 0 (rejected) or 1 (echoed) per input.
        true
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::avatar_intent_tap::AvatarIntentTapConfig;
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("AvatarIntentTapNode")
                .description(
                    "Validates LLM/UI-emitted avatar motion intents (walk_to, sit, \
                     stand_up, reach, look_at, ...) and appends them to an NDJSON \
                     file consumed by the standalone avatar prototype's IntentWatcher. \
                     Echoes accepted intents on `.out`.",
                )
                .category("avatar")
                .accepts([RuntimeDataType::Json, RuntimeDataType::Text])
                .produces([RuntimeDataType::Json])
                .config_schema_from::<AvatarIntentTapConfig>(),
        )
    }
}

#[cfg(feature = "avatar-emotion")]
pub(crate) struct EmotionExtractorNodeFactory;

#[cfg(feature = "avatar-emotion")]
impl StreamingNodeFactory for EmotionExtractorNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::emotion_extractor::{EmotionExtractorConfig, EmotionExtractorNode};
        let config: EmotionExtractorConfig =
            serde_json::from_value(params.clone()).unwrap_or_default();
        let node = EmotionExtractorNode::new(config).map_err(|e| {
            Error::Execution(format!("invalid EmotionExtractorNode pattern: {}", e))
        })?;
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "EmotionExtractorNode"
    }

    fn is_multi_output_streaming(&self) -> bool {
        true // Outputs 1 Text + N Json per input frame
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::emotion_extractor::EmotionExtractorConfig;
        use crate::nodes::schema::{
            LatencyClass, NodeCapabilitiesSchema, NodeSchema, RuntimeDataType,
        };
        Some(
            NodeSchema::new("EmotionExtractorNode")
                .description(
                    "Extracts [EMOTION:<emoji>] tags from text streams. \
                     Emits the original text with tags removed plus a Json \
                     emotion event per matched tag (spec 2026-04-27 §3.1).",
                )
                .category("text")
                .accepts([RuntimeDataType::Text])
                .produces([RuntimeDataType::Text, RuntimeDataType::Json])
                .capabilities(NodeCapabilitiesSchema {
                    parallelizable: true,
                    batch_aware: false,
                    supports_control: false,
                    latency_class: LatencyClass::Fast,
                })
                .config_schema_from::<EmotionExtractorConfig>(),
        )
    }
}

#[cfg(feature = "avatar-lipsync")]
pub(crate) struct SyntheticLipSyncNodeFactory;

#[cfg(feature = "avatar-lipsync")]
impl StreamingNodeFactory for SyntheticLipSyncNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::lip_sync::{SyntheticLipSyncConfig, SyntheticLipSyncNode};
        let config: SyntheticLipSyncConfig =
            serde_json::from_value(params.clone()).unwrap_or_default();
        let node = SyntheticLipSyncNode::new(config);
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "SyntheticLipSyncNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::lip_sync::SyntheticLipSyncConfig;
        use crate::nodes::schema::{
            LatencyClass, NodeCapabilitiesSchema, NodeSchema, RuntimeDataType,
        };
        Some(
            NodeSchema::new("SyntheticLipSyncNode")
                .description(
                    "Deterministic stand-in for Audio2FaceLipSyncNode — derives \
                     ARKit-52 BlendshapeFrame envelopes from the input audio's \
                     RMS. Used for tests + manifest fallback when the real \
                     Audio2Face bundle is unavailable.",
                )
                .category("avatar")
                .accepts([RuntimeDataType::Audio])
                .produces([RuntimeDataType::Json])
                .capabilities(NodeCapabilitiesSchema {
                    parallelizable: false,
                    batch_aware: false,
                    supports_control: true,
                    latency_class: LatencyClass::Fast,
                })
                .config_schema_from::<SyntheticLipSyncConfig>(),
        )
    }
}

/// Factory for [`AudioFileWriterNode`] — appends incoming Audio
/// frames to a WAV file on disk.
///
/// Manifest params:
///
/// ```json
/// { "output_path": "/path/to/out.wav" }
/// ```
pub(crate) struct AudioFileWriterNodeFactory;

impl StreamingNodeFactory for AudioFileWriterNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::file_sink::{AudioFileWriterConfig, AudioFileWriterNode};
        use std::path::PathBuf;
        let output_path = params
            .get("output_path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Error::Execution(
                    "AudioFileWriterNode requires 'output_path' (target .wav file)".into(),
                )
            })?;
        let node = AudioFileWriterNode::new(AudioFileWriterConfig {
            output_path: PathBuf::from(output_path),
        });
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "AudioFileWriterNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{
            LatencyClass, NodeCapabilitiesSchema, NodeSchema, RuntimeDataType,
        };
        Some(
            NodeSchema::new("AudioFileWriterNode")
                .description(
                    "Appends RuntimeData::Audio frames to a WAV file on disk \
                     (IEEE 32-bit float PCM). Pass-through: emits the input \
                     unchanged so it can sit on a tap edge. Header sizes are \
                     patched on Drop.",
                )
                .category("io")
                .accepts([RuntimeDataType::Audio])
                .produces([RuntimeDataType::Audio])
                .capabilities(NodeCapabilitiesSchema {
                    parallelizable: false,
                    batch_aware: false,
                    supports_control: false,
                    latency_class: LatencyClass::Fast,
                }),
        )
    }
}

/// Factory for `MotionPlayerNode` — replays pre-baked skeletal poses
/// from a JSONL file into the pipeline.
///
/// Manifest params:
///
/// ```json
/// { "jsonl_path": "/path/to/motion.jsonl", "fps": 30, "loop_forever": false, "pace_realtime": false }
/// ```
pub(crate) struct MotionPlayerNodeFactory;

impl StreamingNodeFactory for MotionPlayerNodeFactory {
    fn node_type(&self) -> &str {
        "MotionPlayerNode"
    }

    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: crate::nodes::motion::MotionPlayerConfig =
            serde_json::from_value(params.clone()).map_err(|e| {
                Error::InvalidData(format!(
                    "MotionPlayerNode '{node_id}' params invalid: {e} (need jsonl_path)"
                ))
            })?;
        let node = crate::nodes::motion::MotionPlayerNode::new(config);
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("MotionPlayerNode")
                .description("Replays pre-baked skeletal poses from a JSONL file into the pipeline")
                .category("avatar")
                .accepts([])
                .produces([RuntimeDataType::Json]),
        )
    }
}

/// Factory for `RenderReadyGateNode` — buffers blendshape /
/// skeletal_pose envelopes until a `{kind:"render_ready"}` Json
/// envelope opens the gate, then drains them paced by `pts_ms`. Use
/// in offline-batch render pipelines where the upstream
/// (Kokoro+Audio2Face) bursts a multi-second envelope stream before
/// `CcRenderNode` finishes warmup. See [`crate::nodes::render_ready_gate`].
///
/// Manifest params (all optional, sensible defaults):
///
/// ```json
/// {
///   "gated_kinds":      ["blendshapes", "skeletal_pose"],
///   "open_kind":        "render_ready",
///   "pace_realtime":    true,
///   "buffer_capacity":  4096
/// }
/// ```
pub(crate) struct RenderReadyGateNodeFactory;

impl StreamingNodeFactory for RenderReadyGateNodeFactory {
    fn node_type(&self) -> &str {
        "RenderReadyGateNode"
    }

    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: crate::nodes::render_ready_gate::RenderReadyGateConfig =
            serde_json::from_value(params.clone()).map_err(|e| {
                Error::InvalidData(format!(
                    "RenderReadyGateNode '{node_id}' params invalid: {e}"
                ))
            })?;
        let node = crate::nodes::render_ready_gate::RenderReadyGateNode::new(config);
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("RenderReadyGateNode")
                .description("Buffers blendshape/skeletal_pose envelopes until a render_ready signal opens the gate")
                .category("avatar")
                .accepts([RuntimeDataType::Json])
                .produces([RuntimeDataType::Json]),
        )
    }
}

pub(crate) struct EnvelopeFirstNotifierNodeFactory;

impl StreamingNodeFactory for EnvelopeFirstNotifierNodeFactory {
    fn node_type(&self) -> &str {
        "EnvelopeFirstNotifierNode"
    }

    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: crate::nodes::envelope_first_notifier::EnvelopeFirstNotifierConfig =
            serde_json::from_value(params.clone()).map_err(|e| {
                Error::InvalidData(format!(
                    "EnvelopeFirstNotifierNode '{node_id}' params invalid: {e}"
                ))
            })?;
        let node = crate::nodes::envelope_first_notifier::EnvelopeFirstNotifierNode::new(config);
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("EnvelopeFirstNotifierNode")
                .description("One-shot notifier — emits a first_emit envelope when the watched stream first produces a target kind")
                .category("debug")
                .accepts([RuntimeDataType::Json])
                .produces([RuntimeDataType::Json]),
        )
    }
}

/// Factory for [`crate::nodes::debug_log::EnvelopeDebugLogNode`] —
/// passthrough node that logs `RuntimeData::Json` envelopes flowing
/// through it. Designed for diagnosing avatar pipelines.
///
/// Manifest params:
///
/// ```json
/// {
///   "label": "post_audio2face",
///   "kind": "blendshapes",
///   "every": 30,
///   "topK": 5
/// }
/// ```
pub(crate) struct EnvelopeDebugLogNodeFactory;

impl StreamingNodeFactory for EnvelopeDebugLogNodeFactory {
    fn node_type(&self) -> &str {
        "EnvelopeDebugLogNode"
    }

    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: crate::nodes::debug_log::EnvelopeDebugLogConfig =
            serde_json::from_value(params.clone()).map_err(|e| {
                Error::InvalidData(format!(
                    "EnvelopeDebugLogNode '{node_id}' params invalid: {e} (need label)"
                ))
            })?;
        let node = crate::nodes::debug_log::EnvelopeDebugLogNode::new(config);
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("EnvelopeDebugLogNode")
                .description(
                    "Passthrough node that logs JSON envelopes flowing through it for debugging",
                )
                .category("debug")
                .accepts([RuntimeDataType::Json])
                .produces([RuntimeDataType::Json]),
        )
    }
}

/// Factory for [`VideoFileWriterNode`] — writes incoming Video
/// frames to a Y4M file on disk.
///
/// Manifest params:
///
/// ```json
/// {
///   "output_path": "/path/to/out.y4m",
///   "fps": 30
/// }
/// ```
pub(crate) struct VideoFileWriterNodeFactory;

impl StreamingNodeFactory for VideoFileWriterNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::file_sink::{VideoFileWriterConfig, VideoFileWriterNode};
        use std::path::PathBuf;
        let output_path = params
            .get("output_path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Error::Execution(
                    "VideoFileWriterNode requires 'output_path' (target .y4m file)".into(),
                )
            })?;
        let fps = params
            .get("fps")
            .and_then(|v| v.as_u64())
            .map(|v| v as u32)
            .unwrap_or(30);
        let node = VideoFileWriterNode::new(VideoFileWriterConfig {
            output_path: PathBuf::from(output_path),
            fps,
        });
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "VideoFileWriterNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{
            LatencyClass, NodeCapabilitiesSchema, NodeSchema, RuntimeDataType,
        };
        Some(
            NodeSchema::new("VideoFileWriterNode")
                .description(
                    "Writes RuntimeData::Video (RGB24) frames to a Y4M file on \
                     disk. RGB→YUV420p conversion happens per frame; output is \
                     ffmpeg-compatible. Pass-through: emits the input unchanged \
                     so it can sit on a tap edge.",
                )
                .category("io")
                .accepts([RuntimeDataType::Video])
                .produces([RuntimeDataType::Video])
                .capabilities(NodeCapabilitiesSchema {
                    parallelizable: false,
                    batch_aware: false,
                    supports_control: false,
                    latency_class: LatencyClass::Fast,
                }),
        )
    }
}

/// Factory for [`VideoFrameDiffNode`] — diagnostic pass-through that
/// hashes incoming Video frames + logs whether consecutive frames
/// actually differ. Drop on a tap edge between the renderer and a
/// downstream sink to detect "static image" bugs.
///
/// Manifest params:
///
/// ```json
/// { "log_every": 30, "label": "renderer-out" }
/// ```
pub(crate) struct VideoFrameDiffNodeFactory;

impl StreamingNodeFactory for VideoFrameDiffNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::file_sink::{VideoFrameDiffConfig, VideoFrameDiffNode};
        let log_every = params
            .get("log_every")
            .and_then(|v| v.as_u64())
            .unwrap_or(30);
        let label = params
            .get("label")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let node = VideoFrameDiffNode::new(VideoFrameDiffConfig { log_every, label });
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "VideoFrameDiffNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{
            LatencyClass, NodeCapabilitiesSchema, NodeSchema, RuntimeDataType,
        };
        Some(
            NodeSchema::new("VideoFrameDiffNode")
                .description(
                    "Diagnostic pass-through that hashes incoming Video frames \
                     and logs `same` vs `differ` counts. Pass-through: emits the \
                     same Video frames it received.",
                )
                .category("debug")
                .accepts([RuntimeDataType::Video])
                .produces([RuntimeDataType::Video])
                .capabilities(NodeCapabilitiesSchema {
                    parallelizable: false,
                    batch_aware: false,
                    supports_control: false,
                    latency_class: LatencyClass::Fast,
                }),
        )
    }
}

// `Audio2FaceLipSyncNodeFactory` moved to a standalone plugin:
// github.com/RemoteMedia-SDK/audio2face. The whole factory + the
// `audio2face/` submodule (BVLS/PGD solvers, NPZ I/O, identity tables,
// inference) live there now. Load via
// `"plugins": ["audio2face@v0.1.0"]` in the pipeline manifest.

// `Live2DRenderNodeFactory` moved to standalone plugin:
// github.com/RemoteMedia-SDK/live2d-render@v0.1.0. The full
// live2d_render/ subdirectory + the vendored cubism-core +
// cubism-core-sys crates live there now, so the host no longer needs
// the Live2D Cubism SDK (`LIVE2D_CUBISM_CORE_DIR` env var).

// `CcRenderNodeFactory` moved to standalone plugin:
// github.com/RemoteMedia-SDK/cc-render@v0.1.0. The full cc_render/
// subdirectory (Bevy app, physics, capture, GPU select, pose pipeline)
// lives there now — the host no longer compiles Bevy / bevy_rapier3d /
// wgpu-23.0.1.

// `SileroVADNodeFactory` and `WhisperToVadNodeFactory` moved to
// standalone plugins (RemoteMedia-SDK/silero-vad,
// RemoteMedia-SDK/whisper-to-vad). `VadToFaceNodeFactory` stays here
// (pure Rust, no ort dependency).

#[cfg(feature = "affect-listener-face")]
pub(crate) struct VadToFaceNodeFactory;

#[cfg(feature = "affect-listener-face")]
impl StreamingNodeFactory for VadToFaceNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::vad_to_face::{VadToFaceConfig, VadToFaceNode};
        let config: VadToFaceConfig = serde_json::from_value(params.clone()).unwrap_or_default();
        let node = VadToFaceNode::with_config(config);
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "VadToFaceNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{
            LatencyClass, NodeCapabilitiesSchema, NodeSchema, RuntimeDataType,
        };
        use crate::nodes::vad_to_face::VadToFaceConfig;
        Some(
            NodeSchema::new("VadToFaceNode")
                .description(
                    "V/A/D + intensity → ARKit-52 blendshapes via RBF over \
                     MEAD-derived emotion-anchors. Listener-mode Path B \
                     downstream; sub-millisecond, audio-free.",
                )
                .category("affect")
                .accepts([RuntimeDataType::Json, RuntimeDataType::Tensor])
                .produces([RuntimeDataType::Json])
                .capabilities(NodeCapabilitiesSchema {
                    parallelizable: false,
                    batch_aware: false,
                    supports_control: false,
                    latency_class: LatencyClass::Realtime,
                })
                .config_schema_from::<VadToFaceConfig>(),
        )
    }
}

// Spec 007: Speculative VAD Gate for low-latency streaming
pub(crate) struct SpeculativeVADGateFactory;
impl StreamingNodeFactory for SpeculativeVADGateFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::speculative_vad_gate::{SpeculativeVADGate, SpeculativeVADGateConfig};

        // Deserialize config directly - #[serde(default)] handles missing fields,
        // #[serde(alias = "camelCase")] handles both snake_case and camelCase
        let config: SpeculativeVADGateConfig =
            serde_json::from_value(params.clone()).unwrap_or_default();

        let node = SpeculativeVADGate::new(config);
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "SpeculativeVADGate"
    }

    fn is_multi_output_streaming(&self) -> bool {
        true // Outputs audio + optional cancellation messages
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{
            LatencyClass, NodeCapabilitiesSchema, NodeSchema, RuntimeDataType,
        };
        use crate::nodes::speculative_vad_gate::SpeculativeVADGateConfig;
        Some(
            NodeSchema::new("SpeculativeVADGate")
                .description("Speculative VAD gate for low-latency voice interaction")
                .category("audio")
                .accepts([RuntimeDataType::Audio])
                .produces([RuntimeDataType::Audio, RuntimeDataType::ControlMessage])
                .capabilities(NodeCapabilitiesSchema {
                    parallelizable: false,
                    batch_aware: false,
                    supports_control: true,
                    latency_class: LatencyClass::Realtime,
                })
                .config_schema_from::<SpeculativeVADGateConfig>(),
        )
    }
}

// `SpeculativeVADCoordinatorFactory` moved with `SileroVADNode` to
// the silero-vad plugin (RemoteMedia-SDK/silero-vad@v0.3.0). The
// coordinator wraps Silero internally so it had to move alongside.

// `LFM25AudioOnnxNodeFactory` and `VogentTurnOnnxNodeFactory` moved
// to standalone plugins (RemoteMedia-SDK/lfm25-audio-onnx and
// RemoteMedia-SDK/vogent-turn).

pub(crate) struct AudioChunkerNodeFactory;
impl StreamingNodeFactory for AudioChunkerNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::audio_chunker::{AudioChunkerConfig, AudioChunkerNode};
        use crate::nodes::SyncNodeWrapper;
        let config: AudioChunkerConfig = serde_json::from_value(params.clone()).unwrap_or_default();
        let node = AudioChunkerNode::with_config(config);
        Ok(Box::new(SyncNodeWrapper(node)))
    }

    fn node_type(&self) -> &str {
        "AudioChunkerNode"
    }

    fn is_multi_output_streaming(&self) -> bool {
        true // Can output 0 or multiple chunks per input
    }

    fn capability_behavior(&self) -> CapabilityBehavior {
        CapabilityBehavior::Passthrough
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::audio_chunker::AudioChunkerConfig;
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("AudioChunkerNode")
                .description("Splits incoming audio into fixed-size chunks")
                .category("audio")
                .accepts([RuntimeDataType::Audio])
                .produces([RuntimeDataType::Audio])
                .config_schema_from::<AudioChunkerConfig>(),
        )
    }
}

pub(crate) struct FastResampleNodeFactory;
impl StreamingNodeFactory for FastResampleNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::audio::{FastResampleNode, ResampleQuality};
        use crate::nodes::audio_resample_streaming::{
            AutoResampleConfig, AutoResampleStreamingNode, ResampleStreamingNode,
        };

        // Parse optional source_rate (can be "auto" or omitted for auto-detection)
        let source_rate = params
            .get("sourceRate")
            .or(params.get("source_rate"))
            .and_then(|v| {
                // Allow "auto" string to mean auto-detect
                if v.as_str() == Some("auto") {
                    None
                } else {
                    v.as_u64().map(|n| n as u32)
                }
            });

        // Parse optional target_rate (can be "auto" or omitted for passthrough/adaptive)
        let target_rate = params
            .get("targetRate")
            .or(params.get("target_rate"))
            .and_then(|v| {
                if v.as_str() == Some("auto") {
                    None
                } else {
                    v.as_u64().map(|n| n as u32)
                }
            });

        let quality_str = params
            .get("quality")
            .and_then(|v| v.as_str())
            .unwrap_or("Medium");

        let quality = match quality_str {
            "Low" => ResampleQuality::Low,
            "Medium" => ResampleQuality::Medium,
            "High" => ResampleQuality::High,
            _ => ResampleQuality::Medium,
        };

        let channels = params
            .get("channels")
            .and_then(|v| v.as_u64())
            .map(|n| n as usize);

        // `audio_stream_id`: when present, force-stamps this stream id on
        // every emitted audio chunk so the WebRTC frame_router routes it
        // to the matching outbound track. Mirrors the scanner's SDP param
        // name so a single manifest entry drives both layers consistently.
        let audio_stream_id = params
            .get("audio_stream_id")
            .or(params.get("output_stream_id"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Use AutoResampleStreamingNode when source or target rate is not specified
        // This enables lazy initialization with auto-detection from incoming data
        if source_rate.is_none() || target_rate.is_none() {
            use crate::nodes::audio_resample_streaming::AutoResampleStreamingNodeWrapper;

            let config = AutoResampleConfig {
                source_rate,
                target_rate,
                quality,
                channels,
                output_stream_id: audio_stream_id,
            };
            let node = AutoResampleStreamingNode::new(node_id, config);
            // Use AutoResampleStreamingNodeWrapper for spec 025 configure_from_upstream support
            return Ok(Box::new(AutoResampleStreamingNodeWrapper::new(node)));
        }

        // Both rates specified - use the original fixed-rate resampler
        let source_rate = source_rate.unwrap();
        let target_rate = target_rate.unwrap();
        let channels = channels.unwrap_or(1);

        let inner = FastResampleNode::new(source_rate, target_rate, quality, channels)?;
        let node =
            ResampleStreamingNode::new(inner, target_rate).with_output_stream_id(audio_stream_id);
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "FastResampleNode"
    }

    fn is_multi_output_streaming(&self) -> bool {
        false // Always outputs exactly 1 chunk per input
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("FastResampleNode")
                .description("High-quality audio resampling using sinc interpolation. Supports auto-detection of sample rates from connected nodes.")
                .category("audio")
                .accepts([RuntimeDataType::Audio])
                .produces([RuntimeDataType::Audio])
                .config_schema(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "source_rate": {
                            "oneOf": [
                                { "type": "integer", "minimum": 8000, "maximum": 192000 },
                                { "type": "string", "enum": ["auto"] }
                            ],
                            "description": "Source sample rate in Hz. Use 'auto' or omit to detect from incoming audio."
                        },
                        "target_rate": {
                            "oneOf": [
                                { "type": "integer", "minimum": 8000, "maximum": 192000 },
                                { "type": "string", "enum": ["auto"] }
                            ],
                            "description": "Target sample rate in Hz. Use 'auto' or omit to adapt to downstream requirements."
                        },
                        "quality": {
                            "type": "string",
                            "description": "Resampling quality",
                            "enum": ["Low", "Medium", "High"],
                            "default": "Medium"
                        },
                        "channels": {
                            "type": "integer",
                            "description": "Number of audio channels. Omit to detect from incoming audio.",
                            "minimum": 1,
                            "maximum": 8
                        }
                    }
                })),
        )
    }

    fn media_capabilities(&self, params: &Value) -> Option<MediaCapabilities> {
        // Check if explicit source and target rates are provided
        let source_rate = params
            .get("sourceRate")
            .or(params.get("source_rate"))
            .and_then(|v| {
                if v.as_str() == Some("auto") {
                    None
                } else {
                    v.as_u64().map(|n| n as u32)
                }
            });

        let target_rate = params
            .get("targetRate")
            .or(params.get("target_rate"))
            .and_then(|v| {
                if v.as_str() == Some("auto") {
                    None
                } else {
                    v.as_u64().map(|n| n as u32)
                }
            });

        let channels = params
            .get("channels")
            .and_then(|v| v.as_u64())
            .map(|n| n as u32);

        // When both source and target rates are explicit, return Configured capabilities
        // Note: Channels are kept flexible on input (range 1-8) to allow the resample node
        // to accept any channel count and output the configured count.
        // This is similar to how a resampler can accept various input rates.
        if let (Some(_source), Some(target)) = (source_rate, target_rate) {
            // Output channel constraint (exact if specified, else flexible)
            let output_channel_constraint = channels
                .map(ConstraintValue::Exact)
                .unwrap_or(ConstraintValue::Range { min: 1, max: 8 });

            return Some(MediaCapabilities::with_input_output(
                // Input: accept wide range of sample rates and channels
                MediaConstraints::Audio(AudioConstraints {
                    sample_rate: Some(ConstraintValue::Range {
                        min: 8000,
                        max: 192000,
                    }),
                    channels: Some(ConstraintValue::Range { min: 1, max: 8 }),
                    format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
                }),
                // Output: exact target rate with optional exact channels
                MediaConstraints::Audio(AudioConstraints {
                    sample_rate: Some(ConstraintValue::Exact(target)),
                    channels: Some(output_channel_constraint),
                    format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
                }),
            ));
        }

        // When auto-configuration is needed, return Adaptive capabilities:
        // - Input: accepts a wide range of sample rates (8kHz - 192kHz)
        // - Output: None initially - adapts to downstream requirements during reverse pass
        Some(MediaCapabilities::with_input(MediaConstraints::Audio(
            AudioConstraints {
                sample_rate: Some(ConstraintValue::Range {
                    min: 8000,
                    max: 192000,
                }),
                channels: Some(ConstraintValue::Range { min: 1, max: 8 }),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            },
        )))
    }

    fn capability_behavior(&self) -> CapabilityBehavior {
        // Default to Adaptive - the actual behavior is determined by media_capabilities():
        // - If media_capabilities() returns both input AND output, it acts as Configured
        // - If media_capabilities() returns only input, it acts as Adaptive
        // The resolver checks for output capabilities to determine if adaptation is needed.
        CapabilityBehavior::Adaptive
    }
}

/// Create a default streaming node registry with all built-in nodes registered
///
/// This function collects node factories from two sources:
/// 1. **Node Providers** - External crates that implement `NodeProvider` and register via `inventory::submit!`
/// 2. **Built-in nodes** - Legacy inline registrations (being migrated to providers)
///
/// Providers are loaded in priority order (highest first), allowing higher-priority
/// providers to override nodes from lower-priority ones.
pub fn create_default_streaming_registry() -> StreamingNodeRegistry {
    let mut registry = StreamingNodeRegistry::new();

    // Phase 1: Collect from all registered NodeProviders (inventory-based)
    // Providers are sorted by priority (highest first)
    let mut providers_loaded = 0;
    let mut nodes_from_providers = 0;
    for provider in crate::nodes::provider::iter_providers() {
        let before_count = registry.list_types().len();
        provider.register(&mut registry);
        let added = registry.list_types().len() - before_count;
        nodes_from_providers += added;
        providers_loaded += 1;
        tracing::debug!(
            provider = provider.provider_name(),
            nodes_added = added,
            "Loaded node provider"
        );
    }
    if providers_loaded > 0 {
        tracing::info!(
            providers = providers_loaded,
            nodes = nodes_from_providers,
            "Loaded node providers via inventory"
        );
    }

    // Phase 2: No legacy registrations needed
    // - Core Rust nodes are registered via CoreNodesProvider
    // - Python nodes are registered via PythonNodesProvider (when python-nodes feature is enabled)
    // - Candle ML nodes ship as a loadable plugin (whisper-loadable etc.)
    //   and are registered by the runtime's plugin loader at startup, not
    //   statically here.

    registry
}

/// SRT output node that converts Whisper JSON segments to SRT subtitle format
struct SrtOutputStreamingNode {
    include_numbers: bool,
    max_line_length: usize,
    segment_counter: std::sync::atomic::AtomicUsize,
}

impl SrtOutputStreamingNode {
    fn new(include_numbers: bool, max_line_length: usize) -> Self {
        Self {
            include_numbers,
            max_line_length,
            segment_counter: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn seconds_to_timecode(seconds: f64) -> String {
        let total_ms = (seconds * 1000.0).round() as u64;
        let ms = total_ms % 1000;
        let total_secs = total_ms / 1000;
        let secs = total_secs % 60;
        let total_mins = total_secs / 60;
        let mins = total_mins % 60;
        let hours = total_mins / 60;
        format!("{:02}:{:02}:{:02},{:03}", hours, mins, secs, ms)
    }

    fn wrap_text(text: &str, max_len: usize) -> String {
        if max_len == 0 || text.len() <= max_len {
            return text.to_string();
        }

        let mut lines = Vec::new();
        let mut current_line = String::new();

        for word in text.split_whitespace() {
            if current_line.is_empty() {
                current_line = word.to_string();
            } else if current_line.len() + 1 + word.len() <= max_len {
                current_line.push(' ');
                current_line.push_str(word);
            } else {
                lines.push(current_line);
                current_line = word.to_string();
            }
        }

        if !current_line.is_empty() {
            lines.push(current_line);
        }

        lines.join("\n")
    }
}

#[async_trait::async_trait]
impl crate::nodes::AsyncStreamingNode for SrtOutputStreamingNode {
    fn node_type(&self) -> &str {
        "SrtOutput"
    }

    async fn initialize(&self, _ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        Ok(())
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        let json = match &data {
            RuntimeData::Json(j) => j.clone(),
            RuntimeData::Text(t) => {
                // Plain text - wrap in a single segment
                serde_json::json!({
                    "text": t,
                    "segments": [{"start": 0.0, "end": 10.0, "text": t}]
                })
            }
            _ => {
                return Err(Error::Execution(format!(
                    "SrtOutput expects JSON or Text, got: {}",
                    data.data_type()
                )));
            }
        };

        let mut srt_output = String::new();

        // Check if this is a WordUpdate from Python HFWhisper (has "word" field)
        if json.get("word").is_some() {
            // Single word update - accumulate for streaming mode
            // For now, just format it as a single subtitle
            let word = json.get("word").and_then(|v| v.as_str()).unwrap_or("");
            let start = json.get("start").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let end = json.get("end").and_then(|v| v.as_f64()).unwrap_or(0.0);

            if !word.trim().is_empty() {
                let counter = self
                    .segment_counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    + 1;
                let start_tc = Self::seconds_to_timecode(start);
                let end_tc = Self::seconds_to_timecode(end);

                if self.include_numbers {
                    srt_output.push_str(&format!(
                        "{}\n{} --> {}\n{}\n\n",
                        counter,
                        start_tc,
                        end_tc,
                        word.trim()
                    ));
                } else {
                    srt_output.push_str(&format!(
                        "{} --> {}\n{}\n\n",
                        start_tc,
                        end_tc,
                        word.trim()
                    ));
                }
            }
        } else if let Some(segments) = json.get("segments").and_then(|s| s.as_array()) {
            // Standard segments format from Rust Whisper
            for segment in segments {
                let start = segment.get("start").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let end = segment.get("end").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let text = segment.get("text").and_then(|v| v.as_str()).unwrap_or("");

                if !text.trim().is_empty() {
                    let counter = self
                        .segment_counter
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                        + 1;
                    let start_tc = Self::seconds_to_timecode(start);
                    let end_tc = Self::seconds_to_timecode(end);
                    let formatted_text = Self::wrap_text(text.trim(), self.max_line_length);

                    if self.include_numbers {
                        srt_output.push_str(&format!(
                            "{}\n{} --> {}\n{}\n\n",
                            counter, start_tc, end_tc, formatted_text
                        ));
                    } else {
                        srt_output.push_str(&format!(
                            "{} --> {}\n{}\n\n",
                            start_tc, end_tc, formatted_text
                        ));
                    }
                }
            }
        }

        Ok(RuntimeData::Text(srt_output))
    }
}

pub(crate) struct SrtOutputNodeFactory;

impl StreamingNodeFactory for SrtOutputNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let include_numbers = params
            .get("include_numbers")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let max_line_length = params
            .get("max_line_length")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;

        let node = SrtOutputStreamingNode::new(include_numbers, max_line_length);
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "SrtOutput"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("SrtOutput")
                .description("Converts Whisper JSON output to SRT subtitle format")
                .category("utility")
                .accepts([RuntimeDataType::Json, RuntimeDataType::Text])
                .produces([RuntimeDataType::Text]),
        )
    }
}
