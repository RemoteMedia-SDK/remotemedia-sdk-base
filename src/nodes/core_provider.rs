//! Core nodes provider - registers all built-in core streaming nodes
//!
//! This provider registers the fundamental nodes that are part of remotemedia-core:
//! - Audio processing nodes (resampling, chunking, VAD)
//! - Video processing nodes (flip, encode, decode, scale)
//! - Text processing nodes (collector)
//! - Health monitoring nodes
//! - Utility nodes (passthrough, calculator)
//!
//! Python-based nodes are NOT registered here - they are in the separate
//! `remotemedia-nodes-python` crate.

use crate::nodes::provider::NodeProvider;
use crate::nodes::streaming_node::StreamingNodeRegistry;
use std::sync::Arc;

// Import all factory types from streaming_registry
use super::streaming_registry::{
    AudioBufferAccumulatorNodeFactory, AudioChunkerNodeFactory, AvatarIntentTapNodeFactory,
    CalculatorNodeFactory, FastResampleNodeFactory, PassThroughNodeFactory,
    SpeculativeAudioCommitNodeFactory, SpeculativeVADGateFactory, TextCollectorNodeFactory,
    VideoFlipNodeFactory,
};

// Import factories defined in their own modules
use crate::nodes::audio_channel_splitter::AudioChannelSplitterNodeFactory;
use crate::nodes::audio_evidence::AudioEvidenceNodeFactory;
use crate::nodes::audio_level::AudioLevelNodeFactory;
use crate::nodes::channel_balance::ChannelBalanceNodeFactory;
use crate::nodes::clipping_detector::ClippingDetectorNodeFactory;
use crate::nodes::conversation_coordinator::ConversationCoordinatorNodeFactory;
use crate::nodes::conversation_flow::ConversationFlowNodeFactory;
use crate::nodes::event_correlator::EventCorrelatorNodeFactory;
use crate::nodes::health_emitter::HealthEmitterNodeFactory;
use crate::nodes::multimodal_llm::MultimodalLLMNodeFactory;
use crate::nodes::openai_chat::OpenAIChatNodeFactory;
use crate::nodes::remote_pipeline::RemotePipelineNodeFactory;
use crate::nodes::session_health::SessionHealthNodeFactory;
use crate::nodes::silence_detector::SilenceDetectorNodeFactory;
use crate::nodes::speech_presence::SpeechPresenceNodeFactory;
use crate::nodes::timing_drift::TimingDriftNodeFactory;

/// Provider for core built-in nodes.
///
/// Registers fundamental audio, video, and utility nodes that ship with remotemedia-core.
/// Has high priority (1000) to ensure core nodes are registered first.
pub struct CoreNodesProvider;

impl NodeProvider for CoreNodesProvider {
    fn register(&self, registry: &mut StreamingNodeRegistry) {
        // Basic utility nodes
        registry.register(Arc::new(CalculatorNodeFactory));
        registry.register(Arc::new(PassThroughNodeFactory));

        // Video processing nodes
        registry.register(Arc::new(VideoFlipNodeFactory));

        #[cfg(feature = "video")]
        {
            use super::streaming_registry::{
                VideoDecoderNodeFactory, VideoEncoderNodeFactory, VideoFormatConverterNodeFactory,
                VideoScalerNodeFactory,
            };
            registry.register(Arc::new(VideoEncoderNodeFactory));
            registry.register(Arc::new(VideoDecoderNodeFactory));
            registry.register(Arc::new(VideoScalerNodeFactory));
            registry.register(Arc::new(VideoFormatConverterNodeFactory));
        }

        // Audio processing nodes
        registry.register(Arc::new(AudioChunkerNodeFactory));
        registry.register(Arc::new(AudioBufferAccumulatorNodeFactory));
        registry.register(Arc::new(SpeculativeAudioCommitNodeFactory));
        registry.register(Arc::new(FastResampleNodeFactory));
        registry.register(Arc::new(
            crate::nodes::audio_format_converter_streaming::FastFormatConverterNodeFactory,
        ));
        registry.register(Arc::new(AudioChannelSplitterNodeFactory));

        // Text processing nodes
        registry.register(Arc::new(TextCollectorNodeFactory));
        registry.register(Arc::new(ConversationCoordinatorNodeFactory));

        // Avatar control bridge — appends LLM/UI motion intents to an
        // NDJSON file the standalone avatar prototype watches.
        registry.register(Arc::new(AvatarIntentTapNodeFactory));

        // File-sink nodes — write Audio / Video to disk in WAV / Y4M.
        {
            use super::streaming_registry::{
                AudioFileWriterNodeFactory, EnvelopeDebugLogNodeFactory,
                EnvelopeFirstNotifierNodeFactory, MotionPlayerNodeFactory,
                RenderReadyGateNodeFactory, VideoFileWriterNodeFactory, VideoFrameDiffNodeFactory,
            };
            registry.register(Arc::new(AudioFileWriterNodeFactory));
            registry.register(Arc::new(VideoFileWriterNodeFactory));
            registry.register(Arc::new(VideoFrameDiffNodeFactory));
            // File-driven motion replay — companion to Python KimodoMotionNode.
            registry.register(Arc::new(MotionPlayerNodeFactory));
            // Diagnostic Json-envelope logger — taps blendshape /
            // skeletal_pose / audio_clock streams, prints values
            // applied to the avatar.
            registry.register(Arc::new(EnvelopeDebugLogNodeFactory));
            // Buffers blendshape / skeletal_pose envelopes until a
            // {kind:"render_ready"} signal opens the gate, then drains
            // paced by pts_ms. Required for offline-batch renders where
            // the upstream (Kokoro+Audio2Face) bursts envelopes before
            // CcRenderNode finishes its bind/settle warmup.
            registry.register(Arc::new(RenderReadyGateNodeFactory));
            // One-shot notifier — fans out a `{kind:"first_emit"}`
            // envelope when the watched stream first produces an
            // envelope of `target_kind`. Used by the smoke binary to
            // align audio + motion timelines.
            registry.register(Arc::new(EnvelopeFirstNotifierNodeFactory));
        }

        // Avatar pipeline (spec 2026-04-27): emoji-tag extraction
        #[cfg(feature = "avatar-emotion")]
        {
            use super::streaming_registry::EmotionExtractorNodeFactory;
            registry.register(Arc::new(EmotionExtractorNodeFactory));
        }

        // Avatar pipeline (spec 2026-04-27 §3.4): SyntheticLipSyncNode —
        // deterministic stand-in for tests + manifest fallback.
        #[cfg(feature = "avatar-lipsync")]
        {
            use super::streaming_registry::SyntheticLipSyncNodeFactory;
            registry.register(Arc::new(SyntheticLipSyncNodeFactory));
        }

        // `Audio2FaceLipSyncNode` moved to standalone plugin:
        // github.com/RemoteMedia-SDK/audio2face. Load via
        // `"plugins": ["audio2face@v0.1.0"]` in the pipeline manifest.

        // `Live2DRenderNode` moved to standalone plugin:
        // github.com/RemoteMedia-SDK/live2d-render@v0.1.0. Load via
        // `"plugins": ["live2d-render@v0.1.0"]` in the pipeline manifest.
        // The plugin bundles cubism-core + cubism-core-sys, so the host
        // no longer requires LIVE2D_CUBISM_CORE_DIR to build.

        // `CcRenderNode` moved to standalone plugin:
        // github.com/RemoteMedia-SDK/cc-render@v0.1.0. Load via
        // `"plugins": ["cc-render@v0.1.0"]` in the pipeline manifest.
        // The plugin bundles bevy + bevy_rapier3d + wgpu — the host
        // no longer compiles Bevy at all.

        // LLM nodes
        registry.register(Arc::new(OpenAIChatNodeFactory));
        registry.register(Arc::new(MultimodalLLMNodeFactory));

        // S2S tool-orchestration nodes (openspec change:
        // `add-s2s-tool-orchestrator`). Coordinator joins audio +
        // tool decisions; executor dispatches `{tool, args}` to a
        // registered `ContextTool` (ships with `clinical_lookup`).
        registry.register(Arc::new(crate::nodes::s2s::S2SCoordinatorNodeFactory));
        registry.register(Arc::new(
            crate::nodes::s2s::ToolExecutorNodeFactory::with_builtins(),
        ));
        // `LFM25AudioOnnxNode` + `VogentTurnOnnxNode` moved to standalone
        // plugins (RemoteMedia-SDK/lfm25-audio-onnx, RemoteMedia-SDK/vogent-turn).

        // Remote pipeline node
        registry.register(Arc::new(RemotePipelineNodeFactory));

        // `SileroVADNode` + `SpeculativeVADCoordinator` moved to
        // standalone plugin: github.com/RemoteMedia-SDK/silero-vad (v0.3.0).

        // Listener-mode face pipeline (Path B per
        // `tools/affect_avatar/INTEGRATION.md`): pooled Whisper hidden
        // states → V/A/D → ARKit-52 blendshapes, no audio path.
        // `WhisperToVadNode` moved to standalone plugin
        // (RemoteMedia-SDK/whisper-to-vad). `VadToFaceNode` stays here
        // (pure Rust, no ort).
        #[cfg(feature = "affect-listener-face")]
        {
            use super::streaming_registry::VadToFaceNodeFactory;
            registry.register(Arc::new(VadToFaceNodeFactory));
        }

        // Speculative VAD gate (spec 007). The coordinator moved out
        // with silero-vad.
        registry.register(Arc::new(SpeculativeVADGateFactory));

        // `SpeakerDiarizationNode` moved to standalone plugin
        // (RemoteMedia-SDK/speaker-diarization@v0.1.0). Pulled the
        // matbeedotcom/pyannote-rs#ort-rc12 fork dep out of the host
        // workspace with it. Load via
        // `"plugins": ["speaker-diarization@v0.1.0"]`.

        // Health monitoring nodes (spec 027)
        registry.register(Arc::new(HealthEmitterNodeFactory));
        registry.register(Arc::new(AudioLevelNodeFactory));
        registry.register(Arc::new(ClippingDetectorNodeFactory));
        registry.register(Arc::new(ChannelBalanceNodeFactory));
        registry.register(Arc::new(SilenceDetectorNodeFactory));

        // Stream health monitoring - business layer
        registry.register(Arc::new(SpeechPresenceNodeFactory));
        registry.register(Arc::new(ConversationFlowNodeFactory));
        registry.register(Arc::new(SessionHealthNodeFactory));

        // Stream health monitoring - technical layer
        registry.register(Arc::new(TimingDriftNodeFactory));
        registry.register(Arc::new(EventCorrelatorNodeFactory));
        registry.register(Arc::new(AudioEvidenceNodeFactory));

        // Output formatters
        use super::streaming_registry::SrtOutputNodeFactory;
        registry.register(Arc::new(SrtOutputNodeFactory));

        // Avatar pipeline nodes — Phases 6.1 + 6.2 of pacing-domains
        // spec. Idle generator (SourceWall) and AvatarNode skeleton
        // (snapshot-input renderer); Audio2FaceNode follows with
        // generalized media_clock subscription (spec 5.1/5.2/5.4).
        registry.register(Arc::new(crate::nodes::avatar::IdleAnimationNodeFactory));
        registry.register(Arc::new(crate::nodes::avatar::AvatarNodeFactory));
        registry.register(Arc::new(crate::nodes::avatar::Audio2FaceNodeFactory));

        // The 4 llama.cpp nodes (LlamaCppGeneration, LlamaCppEmbedding,
        // LlamaCppActivation, LlamaCppSteer) moved to standalone
        // plugin: github.com/RemoteMedia-SDK/llama-cpp@v0.1.0. The
        // llama-cpp-4 / llama-cpp-sys-4 / minijinja stack is no
        // longer in the host workspace. Load via
        // `"plugins": ["llama-cpp@v0.1.0"]` in the pipeline manifest.



        // Activation-projection face node — consumes
        // `RuntimeData::Tensor` envelopes tagged
        // `metadata.kind == "activation_tap"` (emitted by the Rust
        // llama-cpp tap on `LlamaCppGenerationNode` or the Python MLX
        // tap on `QwenTextMlxNode`), projects each onto a calibrated
        // direction NPZ, blends per-session input/response taps, and
        // emits a canonical `BlendshapeFrame` JSON envelope. See
        // `openspec/changes/add-activation-projection-face/`.
        #[cfg(feature = "activation-face")]
        registry.register(Arc::new(
            crate::nodes::activation_face::ActivationFaceNodeFactory,
        ));
    }

    fn provider_name(&self) -> &'static str {
        "core-nodes"
    }

    fn node_count(&self) -> usize {
        // Approximate count - varies by feature flags
        25
    }

    fn priority(&self) -> i32 {
        // Core nodes have highest priority
        1000
    }
}

// Auto-register the core nodes provider
// Uses static reference for const initialization required by inventory
inventory::submit! {
    &CoreNodesProvider as &'static dyn NodeProvider
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_core_provider_registers_nodes() {
        let mut registry = StreamingNodeRegistry::new();
        let provider = CoreNodesProvider;

        provider.register(&mut registry);

        // Should have registered some nodes
        assert!(!registry.list_types().is_empty());

        // Check some expected nodes exist
        assert!(registry.has_node_type("CalculatorNode"));
        assert!(registry.has_node_type("PassThrough"));
        assert!(registry.has_node_type("VideoFlip"));
        assert!(registry.has_node_type("FastResampleNode"));
    }

    #[test]
    fn test_provider_metadata() {
        let provider = CoreNodesProvider;
        assert_eq!(provider.provider_name(), "core-nodes");
        assert_eq!(provider.priority(), 1000);
        assert!(provider.node_count() > 0);
    }
}
