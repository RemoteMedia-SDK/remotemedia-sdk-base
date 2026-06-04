//! Python node registry for dynamic registration
//!
//! This module provides a global registry where Python nodes can register themselves.
//! The registry stores metadata about each node type, which is then used to create
//! factories dynamically.

use std::collections::HashMap;
use std::sync::{LazyLock, RwLock};

/// Execution mode for Python nodes
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PythonExecutionMode {
    /// Run in a separate process with iceoryx2 IPC (default, Linux/macOS)
    #[default]
    Multiprocess,
    /// Run in-process via PyO3 (Android, or opt-in on Linux/macOS)
    InProcess,
}

impl PythonExecutionMode {
    /// Check if this is in-process mode
    pub fn is_inprocess(&self) -> bool {
        matches!(self, PythonExecutionMode::InProcess)
    }
    
    /// Check if this is multiprocess mode
    pub fn is_multiprocess(&self) -> bool {
        matches!(self, PythonExecutionMode::Multiprocess)
    }
}

/// Configuration for a Python node
#[derive(Debug, Clone)]
pub struct PythonNodeConfig {
    /// The node type name used in manifests (e.g., "KokoroTTSNode")
    pub node_type: String,

    /// The Python class name to instantiate (e.g., "remotemedia.nodes.tts.KokoroTTSNode")
    /// If not specified, defaults to node_type
    pub python_class: String,

    /// Whether this node can produce multiple outputs per input
    pub is_multi_output: bool,

    /// Whether this is a Python node (always true for this registry)
    pub is_python_node: bool,

    /// Description of the node (for schema/documentation)
    pub description: Option<String>,

    /// Category for the node (e.g., "ml", "audio", "text")
    pub category: Option<String>,

    /// Input data types this node accepts (e.g., ["audio", "text"])
    pub accepts: Vec<String>,

    /// Output data types this node produces (e.g., ["audio", "json"])
    pub produces: Vec<String>,

    /// Execution mode: "multiprocess" (default) or "inprocess" (PyO3)
    pub execution_mode: PythonExecutionMode,
}

impl Default for PythonNodeConfig {
    fn default() -> Self {
        Self {
            node_type: String::new(),
            python_class: String::new(),
            is_multi_output: false,
            is_python_node: true,
            description: None,
            category: None,
            accepts: Vec::new(),
            produces: Vec::new(),
            execution_mode: PythonExecutionMode::default(),
        }
    }
}

impl PythonNodeConfig {
    /// Create a new config with just the node type (python_class defaults to node_type)
    pub fn new(node_type: impl Into<String>) -> Self {
        let node_type = node_type.into();
        Self {
            python_class: node_type.clone(),
            node_type,
            ..Default::default()
        }
    }

    /// Set the Python class name
    pub fn with_python_class(mut self, python_class: impl Into<String>) -> Self {
        self.python_class = python_class.into();
        self
    }

    /// Mark as multi-output streaming
    pub fn with_multi_output(mut self, is_multi_output: bool) -> Self {
        self.is_multi_output = is_multi_output;
        self
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set category
    pub fn with_category(mut self, category: impl Into<String>) -> Self {
        self.category = Some(category.into());
        self
    }

    /// Set accepted input types
    pub fn accepts(mut self, types: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.accepts = types.into_iter().map(|t| t.into()).collect();
        self
    }

    /// Set produced output types
    pub fn produces(mut self, types: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.produces = types.into_iter().map(|t| t.into()).collect();
        self
    }

    /// Set execution mode to in-process (PyO3)
    pub fn with_inprocess(mut self, inprocess: bool) -> Self {
        self.execution_mode = if inprocess {
            PythonExecutionMode::InProcess
        } else {
            PythonExecutionMode::Multiprocess
        };
        self
    }
}

/// Global registry of Python nodes
pub struct PythonNodeRegistry {
    nodes: RwLock<HashMap<String, PythonNodeConfig>>,
}

impl PythonNodeRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
        }
    }

    /// Register a Python node configuration
    pub fn register(&self, config: PythonNodeConfig) {
        let node_type = config.node_type.clone();
        let mut nodes = self.nodes.write().unwrap();
        tracing::debug!(
            node_type = %node_type,
            python_class = %config.python_class,
            multi_output = config.is_multi_output,
            "Registering Python node"
        );
        nodes.insert(node_type, config);
    }

    /// Get a registered node configuration
    pub fn get(&self, node_type: &str) -> Option<PythonNodeConfig> {
        let nodes = self.nodes.read().unwrap();
        nodes.get(node_type).cloned()
    }

    /// Get all registered node configurations
    pub fn get_all(&self) -> Vec<PythonNodeConfig> {
        let nodes = self.nodes.read().unwrap();
        nodes.values().cloned().collect()
    }

    /// Get the number of registered nodes
    pub fn len(&self) -> usize {
        let nodes = self.nodes.read().unwrap();
        nodes.len()
    }

    /// Check if registry is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clear all registrations
    pub fn clear(&self) {
        let mut nodes = self.nodes.write().unwrap();
        nodes.clear();
    }

    /// Check if a node type is registered
    pub fn contains(&self, node_type: &str) -> bool {
        let nodes = self.nodes.read().unwrap();
        nodes.contains_key(node_type)
    }

    /// Remove a registration. Returns true if the entry existed.
    pub fn remove(&self, node_type: &str) -> bool {
        let mut nodes = self.nodes.write().unwrap();
        nodes.remove(node_type).is_some()
    }
}

impl Default for PythonNodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Global Python node registry instance
pub static PYTHON_NODE_REGISTRY: LazyLock<PythonNodeRegistry> =
    LazyLock::new(PythonNodeRegistry::new);

/// Register a Python node with the global registry
///
/// # Example
///
/// ```ignore
/// use remotemedia_python_nodes::{register_python_node, PythonNodeConfig};
///
/// // Simple registration
/// register_python_node(PythonNodeConfig::new("MyNode"));
///
/// // With options
/// register_python_node(
///     PythonNodeConfig::new("KokoroTTSNode")
///         .with_python_class("remotemedia.nodes.tts.KokoroTTSNode")
///         .with_multi_output(true)
///         .with_category("tts")
///         .accepts(["text"])
///         .produces(["audio"])
/// );
/// ```
pub fn register_python_node(config: PythonNodeConfig) {
    PYTHON_NODE_REGISTRY.register(config);
}

/// Get all registered Python nodes
pub fn get_registered_nodes() -> Vec<PythonNodeConfig> {
    PYTHON_NODE_REGISTRY.get_all()
}

/// Clear all registered Python nodes (mainly for testing)
pub fn clear_registry() {
    PYTHON_NODE_REGISTRY.clear();
}

/// Register the default set of Python nodes
///
/// This registers the common Python nodes that ship with RemoteMedia.
/// Called automatically by the PythonNodesProvider.
pub fn register_default_python_nodes() {
    // Whisper transcription nodes
    register_python_node(
        PythonNodeConfig::new("WhisperXNode")
            .with_python_class("WhisperXTranscriber")
            .with_description("Speech-to-text with word-level timestamps using WhisperX")
            .with_category("stt")
            .accepts(["audio"])
            .produces(["json"]),
    );

    register_python_node(
        PythonNodeConfig::new("HFWhisperNode")
            .with_python_class("WhisperTranscriptionNode")
            .with_multi_output(true)
            .with_description("Speech-to-text with word-level timestamps using HuggingFace Whisper")
            .with_category("stt")
            .accepts(["audio"])
            .produces(["json"]),
    );

    register_python_node(
        PythonNodeConfig::new("WhisperSTTNode")
            .with_description(
                "Minimal multiprocess-capable Whisper STT: audio in, plain-text \
                 transcript out. For observability / hookpoints on audio pipelines.",
            )
            .with_category("stt")
            .accepts(["audio"])
            .produces(["text"]),
    );

    // TTS nodes
    register_python_node(
        PythonNodeConfig::new("KokoroTTSNode")
            .with_multi_output(true)
            .with_description("Text-to-speech using Kokoro TTS")
            .with_category("tts")
            .accepts(["text"])
            .produces(["audio"]),
    );

    register_python_node(
        PythonNodeConfig::new("VibeVoiceTTSNode")
            .with_multi_output(true)
            .with_description("Text-to-speech using VibeVoice TTS")
            .with_category("tts")
            .accepts(["text"])
            .produces(["audio"]),
    );

    register_python_node(
        PythonNodeConfig::new("CosyVoice3TTSNode")
            .with_multi_output(true)
            .with_description("Text-to-speech using CosyVoice3 with zero-shot voice cloning")
            .with_category("tts")
            .accepts(["text"])
            .produces(["audio"]),
    );

    register_python_node(
        PythonNodeConfig::new("VoxtralTTSNode")
            .with_multi_output(true)
            .with_description("Text-to-speech using Voxtral-4B TTS")
            .with_category("tts")
            .accepts(["text"])
            .produces(["audio"]),
    );

    // MOSS-TTS family (OpenMOSS / MossTTSDelay 1.7B – 8B + MossTTSRealtime 1.7B).
    // The Python implementations live in
    // `clients/python/remotemedia/nodes/ml/moss_tts.py` and
    // `clients/python/remotemedia/nodes/ml/moss_tts_realtime.py` and are
    // registered with the multiprocess runner via @register_node. The
    // class lookup is by bare name (no with_python_class needed since
    // node_type matches the class name).
    register_python_node(
        PythonNodeConfig::new("MossTTSNode")
            .with_multi_output(true)
            .with_description(
                "Text-to-speech using MOSS-TTS (8B MossTTSDelay) — zero-shot \
                 voice cloning, multilingual / code-switched, pinyin / IPA / \
                 duration control.",
            )
            .with_category("tts")
            .accepts(["text"])
            .produces(["audio"]),
    );

    register_python_node(
        PythonNodeConfig::new("MossVoiceGeneratorNode")
            .with_multi_output(true)
            .with_description(
                "Voice design + TTS using MOSS-VoiceGenerator (1.7B \
                 MossTTSDelay) — synthesises voices from free-form \
                 instruction prompts; no reference audio needed.",
            )
            .with_category("tts")
            .accepts(["text"])
            .produces(["audio"]),
    );

    register_python_node(
        PythonNodeConfig::new("MossSoundEffectNode")
            .with_multi_output(true)
            .with_description(
                "Environmental sound and SFX generation using \
                 MOSS-SoundEffect (8B MossTTSDelay).",
            )
            .with_category("tts")
            .accepts(["text"])
            .produces(["audio"]),
    );

    register_python_node(
        PythonNodeConfig::new("MossTTSRealtimeNode")
            .with_multi_output(true)
            .with_description(
                "Streaming, multi-turn, context-aware TTS using \
                 MOSS-TTS-Realtime (1.7B MossTTSRealtime). Push text deltas \
                 from an LLM token stream; audio frames flow back at ~180 ms \
                 TTFB. Aux-ports: reference / reset / flush / new_turn.",
            )
            .with_category("tts")
            .accepts(["text"])
            .produces(["audio"]),
    );

    // ML nodes
    register_python_node(
        PythonNodeConfig::new("LFM2AudioNode")
            .with_multi_output(true)
            .with_description("Speech-to-speech generation using LFM2 (torch / liquid-audio)")
            .with_category("llm")
            .accepts(["audio"])
            .produces(["audio", "text"]),
    );

    register_python_node(
        PythonNodeConfig::new("LFM2AudioMlxNode")
            .with_multi_output(true)
            .with_description(
                "Speech-to-speech LFM2 via Apple MLX (mlx-audio). \
                 Apple Silicon / macOS only; no CUDA, no torch.",
            )
            .with_category("llm")
            .accepts(["audio"])
            .produces(["audio", "text"]),
    );

    register_python_node(
        PythonNodeConfig::new("PersonaPlexAudioMlxNode")
            .with_multi_output(true)
            .with_description(
                "Full-duplex speech-to-speech using PersonaPlex-7B (Kyutai \
                 Moshi family) via Apple MLX. Apple Silicon only. Same \
                 control-bus surface as LFM2AudioMlxNode (context / \
                 system_prompt / reset / barge_in), but streams \
                 continuously with no end-of-turn sentinels.",
            )
            .with_category("llm")
            .accepts(["audio"])
            .produces(["audio", "text"]),
    );

    register_python_node(
        PythonNodeConfig::new("LFM2TextNode")
            .with_description(
                "Multi-turn text chat using LFM2 (e.g. LiquidAI/LFM2-350M). \
                 Consumes user turns on the main input and control-bus aux-port \
                 envelopes on 'context' / 'system_prompt' / 'reset'.",
            )
            .with_category("llm")
            .accepts(["text"])
            .produces(["text"]),
    );

    // S2S tool orchestration — classifier emits {tool, args, confidence}
    // JSON decisions. See openspec/changes/add-s2s-tool-orchestrator/.
    register_python_node(
        PythonNodeConfig::new("ToolClassifierNode")
            .with_description(
                "LFM2.5-1.2B-Instruct + outlines JSON grammar classifier. \
                 Reads a transcript, emits {tool, args, confidence} JSON or \
                 {tool: null}. Pairs with the Rust ToolExecutorNode and \
                 S2SCoordinatorNode for tool-augmented voice replies under \
                 800 ms.",
            )
            .with_category("llm")
            .accepts(["text"])
            .produces(["text"]),
    );

    // LFM2InstructAgentNode — tool-calling text assistant. The
    // "Instruct plans, Audio speaks" S2S architecture: this node owns
    // the tool registry + chat history, runs native LFM2 tool-calling,
    // and emits a typed-RPC set_context envelope so a downstream
    // LFM2-Audio node can speak the result. Ships with no built-in
    // tools — pass OpenAI-shape schemas via `extra_tools` and register
    // matching handlers from the host.
    register_python_node(
        PythonNodeConfig::new("LFM2InstructAgentNode")
            .with_description(
                "LFM2.5-1.2B-Instruct text agent that runs the native \
                 LFM2 tool-call loop over caller-supplied `extra_tools` \
                 schemas and emits a typed-RPC `set_context` envelope \
                 for a downstream audio LLM to speak.",
            )
            .with_category("llm")
            .accepts(["text"])
            .produces(["text"]),
    );

    // LFM2DemoAgentNode — subclass of LFM2InstructAgentNode preloaded
    // with 12 offline demo tools (alarm/weather/music/smart-home/etc.)
    // matched to the v4.1 fine-tune's training scenarios. Used by the
    // `lfm2-tool-aware-voice-assistant` example so the demo runs out
    // of the box without wiring real tool implementations.
    register_python_node(
        PythonNodeConfig::new("LFM2DemoAgentNode")
            .with_description(
                "LFM2InstructAgentNode subclass preloaded with 12 \
                 offline demo tools (set_alarm, get_weather, \
                 play_music, set_thermostat, …). All tools return \
                 plausible mock data; no API keys or network needed.",
            )
            .with_category("llm")
            .accepts(["text"])
            .produces(["text"]),
    );

    // Parakeet EOU — streaming ASR + end-of-utterance detection in one model.
    // Wraps nvidia/parakeet_realtime_eou_120m-v1 via NeMo ASR. English only,
    // no punctuation/capitalization. Requires CUDA.
    register_python_node(
        PythonNodeConfig::new("ParakeetEouNode")
            .with_multi_output(true)
            .with_description(
                "Streaming ASR with EOU detection via Parakeet \
                 (nvidia/parakeet_realtime_eou_120m-v1). Accepts audio \
                 chunks, emits partial text + JSON {text, eou: true} on \
                 end-of-utterance. Replaces VAD + Whisper in English-only \
                 s2s pipelines. Requires CUDA GPU.",
            )
            .with_category("stt")
            .accepts(["audio"])
            .produces(["text", "json"]),
    );

    register_python_node(
        PythonNodeConfig::new("QwenTextMlxNode")
            .with_multi_output(true)
            .with_description(
                "Multi-turn text chat using Qwen MLX (mlx-vlm, e.g. \
                 Qwen3.5-9B-MLX-4bit). Streams text tokens and fires a \
                 `<|text_end|>` sentinel at end-of-reply so a downstream \
                 TTS node knows to flush. Aux-ports: context / system_prompt \
                 / reset / barge_in.",
            )
            .with_category("llm")
            .accepts(["text"])
            .produces(["text"]),
    );

    register_python_node(
        PythonNodeConfig::new("QwenTTSMlxNode")
            .with_multi_output(true)
            .with_description(
                "Streaming TTS using mlx-audio's Qwen3-TTS. Passes upstream \
                 text tokens through and synthesises 24 kHz mono audio when \
                 a `<|text_end|>` sentinel arrives, then emits `<|audio_end|>`.",
            )
            .with_category("tts")
            .accepts(["text"])
            .produces(["audio", "text"]),
    );

    register_python_node(
        PythonNodeConfig::new("SimplePyTorchNode")
            .with_description("Simple PyTorch inference node for testing")
            .with_category("test"),
    );

    // Avatar / motion nodes
    register_python_node(
        PythonNodeConfig::new("KimodoMotionNode")
            .with_multi_output(true)
            .with_description(
                "Text-prompt → SMPL-22 skeletal pose stream via Kimodo \
                 motion-diffusion. Wraps scripts/avatars/kimodo_gen.py.",
            )
            .with_category("avatar")
            .accepts(["json", "text"])
            .produces(["json"]),
    );

    // Test nodes
    register_python_node(
        PythonNodeConfig::new("ExpanderNode")
            .with_multi_output(true)
            .with_category("test"),
    );

    register_python_node(
        PythonNodeConfig::new("RangeGeneratorNode")
            .with_multi_output(true)
            .with_category("test"),
    );

    register_python_node(
        PythonNodeConfig::new("TransformAndExpandNode")
            .with_multi_output(true)
            .with_category("test"),
    );

    register_python_node(
        PythonNodeConfig::new("ChainedTransformNode")
            .with_multi_output(true)
            .with_category("test"),
    );

    register_python_node(
        PythonNodeConfig::new("ConditionalExpanderNode")
            .with_multi_output(true)
            .with_category("test"),
    );

    register_python_node(
        PythonNodeConfig::new("FilterNode")
            .with_multi_output(true)
            .with_category("test"),
    );

    tracing::info!(
        count = PYTHON_NODE_REGISTRY.len(),
        "Registered default Python nodes"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_get() {
        let registry = PythonNodeRegistry::new();

        registry.register(
            PythonNodeConfig::new("TestNode")
                .with_python_class("test.TestNode")
                .with_multi_output(true),
        );

        let config = registry.get("TestNode").unwrap();
        assert_eq!(config.node_type, "TestNode");
        assert_eq!(config.python_class, "test.TestNode");
        assert!(config.is_multi_output);
        assert!(config.is_python_node);
    }

    #[test]
    fn test_config_builder() {
        let config = PythonNodeConfig::new("MyNode")
            .with_python_class("my.module.MyNode")
            .with_multi_output(true)
            .with_description("A test node")
            .with_category("test")
            .accepts(["audio", "text"])
            .produces(["json"]);

        assert_eq!(config.node_type, "MyNode");
        assert_eq!(config.python_class, "my.module.MyNode");
        assert!(config.is_multi_output);
        assert_eq!(config.description, Some("A test node".to_string()));
        assert_eq!(config.category, Some("test".to_string()));
        assert_eq!(config.accepts, vec!["audio", "text"]);
        assert_eq!(config.produces, vec!["json"]);
    }

    #[test]
    fn test_registry_len_and_clear() {
        let registry = PythonNodeRegistry::new();
        assert!(registry.is_empty());

        registry.register(PythonNodeConfig::new("Node1"));
        registry.register(PythonNodeConfig::new("Node2"));
        assert_eq!(registry.len(), 2);

        registry.clear();
        assert!(registry.is_empty());
    }
}
