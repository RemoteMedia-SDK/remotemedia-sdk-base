//! Re-exports of the node schema surface from `remotemedia-traits`.
//!
//! The schema types moved out of core in Task A4 so plugin authors can
//! return `NodeSchema` from `StreamingNodeFactory::schema()` without
//! depending on the heavy host crate. Core re-exports them here to keep
//! the historical `use remotemedia_core::nodes::schema::NodeSchema` and
//! `remotemedia_core::nodes::schema::RegisteredNodeConfig` paths
//! working unchanged. The `core-derive` macro emits these paths.

pub use remotemedia_traits::schema::{
    collect_registered_configs, generate_typescript, HasNodeSchema, LatencyClass,
    NodeCapabilitiesSchema, NodeConfigSchema, NodeModelSourceFile, NodeModelSources, NodeParameter,
    NodeSchema, NodeSchemaRegistry, ParameterType, RegisteredNodeConfig, RuntimeDataType,
};

// =============================================================================
// Built-in node schemas (host-side; uses moved schema types but stays in
// core because it constructs schemas for built-in nodes whose names are
// owned by core).
// =============================================================================

/// Create registry with schemas for all built-in nodes
pub fn create_builtin_schema_registry() -> NodeSchemaRegistry {
    let mut registry = NodeSchemaRegistry::new();

    // Audio nodes
    registry.register(
        NodeSchema::new("AudioResample")
            .description("Resamples audio to target sample rate")
            .category("audio")
            .accepts([RuntimeDataType::Audio])
            .produces([RuntimeDataType::Audio])
            .config_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "target_sample_rate": {
                        "type": "integer",
                        "description": "Target sample rate in Hz",
                        "default": 16000,
                        "minimum": 8000,
                        "maximum": 48000
                    }
                }
            }))
            .capabilities(NodeCapabilitiesSchema {
                parallelizable: true,
                latency_class: LatencyClass::Realtime,
                ..Default::default()
            }),
    );

    registry.register(
        NodeSchema::new("AudioChunker")
            .description("Splits audio into fixed-size chunks")
            .category("audio")
            .accepts([RuntimeDataType::Audio])
            .produces([RuntimeDataType::Audio])
            .multi_output()
            .config_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "chunk_size_ms": {
                        "type": "integer",
                        "description": "Chunk duration in milliseconds",
                        "default": 20
                    }
                }
            })),
    );

    // VAD nodes
    registry.register(
        NodeSchema::new("SileroVAD")
            .description("Voice Activity Detection using Silero VAD model")
            .category("audio")
            .accepts([RuntimeDataType::Audio])
            .produces([RuntimeDataType::Audio, RuntimeDataType::ControlMessage])
            .config_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "threshold": {
                        "type": "number",
                        "description": "Speech probability threshold",
                        "default": 0.5,
                        "minimum": 0.0,
                        "maximum": 1.0
                    },
                    "min_speech_duration_ms": {
                        "type": "integer",
                        "description": "Minimum speech duration in ms",
                        "default": 250
                    },
                    "min_silence_duration_ms": {
                        "type": "integer",
                        "description": "Minimum silence duration in ms",
                        "default": 100
                    }
                }
            }))
            .model_sources(NodeModelSources {
                files: vec![
                    NodeModelSourceFile {
                        path: "silero-vad/silero_vad.onnx".to_string(),
                        filename: "silero_vad.onnx".to_string(),
                        url: "https://github.com/snakers4/silero-vad/raw/master/src/silero_vad/data/silero_vad.onnx".to_string(),
                        expected_size: Some(1_800_000),
                        required: true,
                    }
                ]
            })
            .capabilities(NodeCapabilitiesSchema {
                parallelizable: true,
                supports_control: true,
                latency_class: LatencyClass::Fast,
                ..Default::default()
            }),
    );

    // Text/ML nodes
    registry.register(
        NodeSchema::new("KokoroTTSNode")
            .description("Text-to-speech synthesis using Kokoro TTS")
            .category("ml")
            .accepts([RuntimeDataType::Text])
            .produces([RuntimeDataType::Audio])
            .python()
            .multi_output()
            .config_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "voice": {
                        "type": "string",
                        "description": "Voice ID to use",
                        "default": "af_bella",
                        "enum": ["af_bella", "af_nicole", "af_sarah", "af_sky", "am_adam", "am_michael", "bf_emma", "bf_isabella", "bm_george", "bm_lewis"]
                    },
                    "language": {
                        "type": "string",
                        "description": "Language code",
                        "default": "en-us",
                        "enum": ["en-us", "en-gb", "es", "fr", "de", "it", "ja", "ko", "pt-br", "zh"]
                    },
                    "speed": {
                        "type": "number",
                        "description": "Speech speed multiplier",
                        "default": 1.0,
                        "minimum": 0.5,
                        "maximum": 2.0
                    }
                }
            }))
            .model_sources(NodeModelSources {
                files: vec![
                    NodeModelSourceFile {
                        path: "kokoro/onnx/model_fp16.onnx".to_string(),
                        filename: "kokoro_onnx_fp16.onnx".to_string(),
                        url: "https://huggingface.co/hexgrad/Kokoro-82M-ONNX/resolve/main/kokoro-v1.0-fp16.onnx".to_string(),
                        expected_size: Some(160_000_000),
                        required: true,
                    },
                    NodeModelSourceFile {
                        path: "kokoro/tokenizer.json".to_string(),
                        filename: "tokenizer.json".to_string(),
                        url: "https://huggingface.co/hexgrad/Kokoro-82M-ONNX/resolve/main/tokenizer.json".to_string(),
                        expected_size: Some(500_000),
                        required: true,
                    },
                    NodeModelSourceFile {
                        path: "kokoro/voices/af_bella.bin".to_string(),
                        filename: "af_bella.bin".to_string(),
                        url: "https://huggingface.co/hexgrad/Kokoro-82M-ONNX/resolve/main/voices/af_bella.bin".to_string(),
                        expected_size: Some(20_000_000),
                        required: true,
                    },
                    // Misaki G2P files
                    NodeModelSourceFile {
                        path: "misaki-g2p/en-US/gold.json".to_string(),
                        filename: "en-US-gold.json".to_string(),
                        url: "https://huggingface.co/hexgrad/Kokoro-82M-ONNX/resolve/main/g2p/en-US/gold.json".to_string(),
                        expected_size: Some(1_000_000),
                        required: true,
                    },
                    NodeModelSourceFile {
                        path: "misaki-g2p/en-US/silver.json".to_string(),
                        filename: "en-US-silver.json".to_string(),
                        url: "https://huggingface.co/hexgrad/Kokoro-82M-ONNX/resolve/main/g2p/en-US/silver.json".to_string(),
                        expected_size: Some(1_000_000),
                        required: true,
                    },
                    NodeModelSourceFile {
                        path: "misaki-g2p/en-GB/gold.json".to_string(),
                        filename: "en-GB-gold.json".to_string(),
                        url: "https://huggingface.co/hexgrad/Kokoro-82M-ONNX/resolve/main/g2p/en-GB/gold.json".to_string(),
                        expected_size: Some(1_000_000),
                        required: false,
                    },
                    NodeModelSourceFile {
                        path: "misaki-g2p/en-GB/silver.json".to_string(),
                        filename: "en-GB-silver.json".to_string(),
                        url: "https://huggingface.co/hexgrad/Kokoro-82M-ONNX/resolve/main/g2p/en-GB/silver.json".to_string(),
                        expected_size: Some(1_000_000),
                        required: false,
                    },
                ]
            })
            .capabilities(NodeCapabilitiesSchema {
                parallelizable: false,
                batch_aware: true,
                latency_class: LatencyClass::Slow,
                ..Default::default()
            }),
    );

    registry.register(
        NodeSchema::new("WhisperSTTNode")
            .description("Speech-to-text transcription using Whisper")
            .category("stt")
            .accepts([RuntimeDataType::Audio])
            .produces([RuntimeDataType::Text])
            .python()
            .model_sources(NodeModelSources {
                files: vec![
                    NodeModelSourceFile {
                        path: "whisper/whisper_tiny_30s_f32.tflite".to_string(),
                        filename: "whisper_tiny_30s_f32.tflite".to_string(),
                        url: "https://huggingface.co/google/litert-whisper/resolve/main/whisper_tiny_30s_f32.tflite".to_string(),
                        expected_size: Some(75_000_000),
                        required: true,
                    },
                    NodeModelSourceFile {
                        path: "whisper/whisper_base_30s_f32.tflite".to_string(),
                        filename: "whisper_base_30s_f32.tflite".to_string(),
                        url: "https://huggingface.co/google/litert-whisper/resolve/main/whisper_base_30s_f32.tflite".to_string(),
                        expected_size: Some(150_000_000),
                        required: false,
                    },
                    NodeModelSourceFile {
                        path: "whisper/tokenizer.json".to_string(),
                        filename: "tokenizer.json".to_string(),
                        url: "https://huggingface.co/google/litert-whisper/resolve/main/tokenizer.json".to_string(),
                        expected_size: Some(1_000_000),
                        required: true,
                    },
                    NodeModelSourceFile {
                        path: "whisper/config.json".to_string(),
                        filename: "config.json".to_string(),
                        url: "https://huggingface.co/google/litert-whisper/resolve/main/config.json".to_string(),
                        expected_size: Some(10_000),
                        required: false,
                    },
                ]
            }),
    );

    // Utility nodes
    registry.register(
        NodeSchema::new("Echo")
            .description("Passes input through unchanged (for testing)")
            .category("utility")
            .accepts(RuntimeDataType::all().iter().copied())
            .produces(RuntimeDataType::all().iter().copied()),
    );

    registry.register(
        NodeSchema::new("PassThrough")
            .description("Passes input through unchanged")
            .category("utility")
            .accepts(RuntimeDataType::all().iter().copied())
            .produces(RuntimeDataType::all().iter().copied()),
    );

    registry.register(
        NodeSchema::new("CalculatorNode")
            .description("Performs arithmetic operations on JSON input")
            .category("utility")
            .accepts([RuntimeDataType::Json])
            .produces([RuntimeDataType::Json])
            .config_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "precision": {
                        "type": "integer",
                        "description": "Decimal precision for results",
                        "default": 10
                    }
                }
            })),
    );

    registry.register(
        NodeSchema::new("TextCollector")
            .description("Collects text chunks into complete utterances")
            .category("text")
            .accepts([RuntimeDataType::Text])
            .produces([RuntimeDataType::Text])
            .config_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "delimiter": {
                        "type": "string",
                        "description": "Delimiter to split on",
                        "default": ""
                    },
                    "flush_on_silence": {
                        "type": "boolean",
                        "description": "Flush buffer when silence detected",
                        "default": true
                    }
                }
            })),
    );

    // Android-specific node aliases (used in mobile manifests)
    registry.register(
        NodeSchema::new("AudioChunkerNode")
            .description("Splits audio into fixed-size chunks")
            .category("audio")
            .accepts([RuntimeDataType::Audio])
            .produces([RuntimeDataType::Audio])
            .multi_output()
            .config_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "chunk_size_ms": {
                        "type": "integer",
                        "description": "Chunk duration in milliseconds",
                        "default": 20
                    }
                }
            })),
    );

    registry.register(
        NodeSchema::new("AudioBufferAccumulatorNode")
            .description("Accumulates audio buffers until speech segment complete")
            .category("audio")
            .accepts([RuntimeDataType::Audio])
            .produces([RuntimeDataType::Audio])
            .config_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "min_utterance_duration_ms": {
                        "type": "integer",
                        "description": "Minimum utterance duration in ms",
                        "default": 300
                    },
                    "max_utterance_duration_ms": {
                        "type": "integer",
                        "description": "Maximum utterance duration in ms",
                        "default": 30000
                    },
                    "emit_cancel_on_speech_start": {
                        "type": "boolean",
                        "description": "Emit cancel when new speech starts",
                        "default": false
                    }
                }
            })),
    );

    registry.register(
        NodeSchema::new("SileroVADNode")
            .description("Voice Activity Detection using Silero VAD model (Android alias)")
            .category("audio")
            .accepts([RuntimeDataType::Audio])
            .produces([RuntimeDataType::Audio, RuntimeDataType::ControlMessage])
            .config_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "threshold": {
                        "type": "number",
                        "description": "Speech probability threshold",
                        "default": 0.5,
                        "minimum": 0.0,
                        "maximum": 1.0
                    },
                    "min_speech_duration_ms": {
                        "type": "integer",
                        "description": "Minimum speech duration in ms",
                        "default": 250
                    },
                    "min_silence_duration_ms": {
                        "type": "integer",
                        "description": "Minimum silence duration in ms",
                        "default": 100
                    }
                }
            }))
            .model_sources(NodeModelSources {
                files: vec![
                    NodeModelSourceFile {
                        path: "silero-vad/silero_vad.onnx".to_string(),
                        filename: "silero_vad.onnx".to_string(),
                        url: "https://github.com/snakers4/silero-vad/raw/master/src/silero_vad/data/silero_vad.onnx".to_string(),
                        expected_size: Some(1_800_000),
                        required: true,
                    }
                ]
            })
            .capabilities(NodeCapabilitiesSchema {
                parallelizable: true,
                supports_control: true,
                latency_class: LatencyClass::Fast,
                ..Default::default()
            }),
    );

    registry.register(
        NodeSchema::new("LiteRtLmGenerationNode")
            .description("LLM text generation using LiteRT (Google)")
            .category("llm")
            .accepts([RuntimeDataType::Text])
            .produces([RuntimeDataType::Text])
            .config_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "model_path": {
                        "type": "string",
                        "description": "Path to LiteRT LLM model file"
                    },
                    "backend": {
                        "type": "string",
                        "description": "Backend to use",
                        "default": "cpu"
                    },
                    "cache_dir": {
                        "type": "string",
                        "description": "Cache directory for model"
                    },
                    "max_num_tokens": {
                        "type": "integer",
                        "description": "Maximum number of tokens to generate",
                        "default": 2048
                    },
                    "parallel_file_section_loading": {
                        "type": "boolean",
                        "description": "Enable parallel loading",
                        "default": true
                    }
                }
            }))
            .model_sources(NodeModelSources {
                files: vec![
                    NodeModelSourceFile {
                        path: "gemma-4-E2B-it.litertlm".to_string(),
                        filename: "gemma-4-E2B-it.litertlm".to_string(),
                        url: "https://huggingface.co/google/gemma-2b-it-litertlm/resolve/main/gemma-2b-it.litertlm".to_string(),
                        expected_size: Some(1_200_000_000),
                        required: true,
                    }
                ]
            }),
    );

    registry.register(
        NodeSchema::new("TextCollectorNode")
            .description("Collects text chunks into complete utterances (Android alias)")
            .category("text")
            .accepts([RuntimeDataType::Text])
            .produces([RuntimeDataType::Text])
            .config_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "split_pattern": {
                        "type": "string",
                        "description": "Regex pattern to split on",
                        "default": "[.!?;\\\\n]+"
                    },
                    "min_sentence_length": {
                        "type": "integer",
                        "description": "Minimum sentence length",
                        "default": 8
                    },
                    "yield_partial_on_end": {
                        "type": "boolean",
                        "description": "Yield partial on end",
                        "default": true
                    },
                    "partial_flush_chars": {
                        "type": "integer",
                        "description": "Partial flush chars",
                        "default": 0
                    }
                }
            })),
    );

    registry.register(
        NodeSchema::new("MisakiG2PNode")
            .description("Misaki Grapheme-to-Phoneme for Kokoro TTS")
            .category("g2p")
            .accepts([RuntimeDataType::Text])
            .produces([RuntimeDataType::Text])
            .config_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "resource_dir": {
                        "type": "string",
                        "description": "Path to Misaki G2P resources"
                    },
                    "dialect": {
                        "type": "string",
                        "description": "English dialect",
                        "default": "en-US"
                    },
                    "unknown_policy": {
                        "type": "string",
                        "description": "Policy for unknown words",
                        "default": "grapheme"
                    },
                    "emit_token_metadata": {
                        "type": "boolean",
                        "description": "Emit token metadata",
                        "default": false
                    }
                }
            }))
            .model_sources(NodeModelSources {
                files: vec![
                    NodeModelSourceFile {
                        path: "misaki-g2p/en-US/gold.json".to_string(),
                        filename: "en-US-gold.json".to_string(),
                        url: "https://huggingface.co/hexgrad/Kokoro-82M-ONNX/resolve/main/g2p/en-US/gold.json".to_string(),
                        expected_size: Some(1_000_000),
                        required: true,
                    },
                    NodeModelSourceFile {
                        path: "misaki-g2p/en-US/silver.json".to_string(),
                        filename: "en-US-silver.json".to_string(),
                        url: "https://huggingface.co/hexgrad/Kokoro-82M-ONNX/resolve/main/g2p/en-US/silver.json".to_string(),
                        expected_size: Some(1_000_000),
                        required: true,
                    },
                    NodeModelSourceFile {
                        path: "misaki-g2p/en-GB/gold.json".to_string(),
                        filename: "en-GB-gold.json".to_string(),
                        url: "https://huggingface.co/hexgrad/Kokoro-82M-ONNX/resolve/main/g2p/en-GB/gold.json".to_string(),
                        expected_size: Some(1_000_000),
                        required: false,
                    },
                    NodeModelSourceFile {
                        path: "misaki-g2p/en-GB/silver.json".to_string(),
                        filename: "en-GB-silver.json".to_string(),
                        url: "https://huggingface.co/hexgrad/Kokoro-82M-ONNX/resolve/main/g2p/en-GB/silver.json".to_string(),
                        expected_size: Some(1_000_000),
                        required: false,
                    }
                ]
            }),
    );

    registry.register(
        NodeSchema::new("DataSinkNode")
            .description("Sink node for sending output to external systems")
            .category("io")
            .accepts(RuntimeDataType::all().iter().copied())
            .produces(Vec::new()),
    );

    // WhisperNode alias (used in mobile manifests)
    registry.register(
        NodeSchema::new("WhisperNode")
            .description("Speech-to-text transcription using Whisper (Android alias)")
            .category("stt")
            .accepts([RuntimeDataType::Audio])
            .produces([RuntimeDataType::Text])
            .python()
            .model_sources(NodeModelSources {
                files: vec![
                    NodeModelSourceFile {
                        path: "whisper/whisper_tiny_30s_f32.tflite".to_string(),
                        filename: "whisper_tiny_30s_f32.tflite".to_string(),
                        url: "https://huggingface.co/google/litert-whisper/resolve/main/whisper_tiny_30s_f32.tflite".to_string(),
                        expected_size: Some(75_000_000),
                        required: true,
                    },
                    NodeModelSourceFile {
                        path: "whisper/whisper_base_30s_f32.tflite".to_string(),
                        filename: "whisper_base_30s_f32.tflite".to_string(),
                        url: "https://huggingface.co/google/litert-whisper/resolve/main/whisper_base_30s_f32.tflite".to_string(),
                        expected_size: Some(150_000_000),
                        required: false,
                    },
                    NodeModelSourceFile {
                        path: "whisper/tokenizer.json".to_string(),
                        filename: "tokenizer.json".to_string(),
                        url: "https://huggingface.co/google/litert-whisper/resolve/main/tokenizer.json".to_string(),
                        expected_size: Some(1_000_000),
                        required: true,
                    },
                    NodeModelSourceFile {
                        path: "whisper/config.json".to_string(),
                        filename: "config.json".to_string(),
                        url: "https://huggingface.co/google/litert-whisper/resolve/main/config.json".to_string(),
                        expected_size: Some(10_000),
                        required: false,
                    },
                ]
            }),
    );

    // LiteRtLmGenerationNode - LLM model
    registry.register(
        NodeSchema::new("LiteRtLmGenerationNode")
            .description("LLM text generation using LiteRT (Google)")
            .category("llm")
            .accepts([RuntimeDataType::Text])
            .produces([RuntimeDataType::Text])
            .config_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "model_path": {
                        "type": "string",
                        "description": "Path to LiteRT LLM model file"
                    },
                    "backend": {
                        "type": "string",
                        "description": "Backend to use",
                        "default": "cpu"
                    },
                    "cache_dir": {
                        "type": "string",
                        "description": "Cache directory for model"
                    },
                    "max_num_tokens": {
                        "type": "integer",
                        "description": "Maximum number of tokens to generate",
                        "default": 2048
                    },
                    "parallel_file_section_loading": {
                        "type": "boolean",
                        "description": "Enable parallel loading",
                        "default": true
                    }
                }
            }))
            .model_sources(NodeModelSources {
                files: vec![
                    NodeModelSourceFile {
                        path: "gemma-4-E2B-it.litertlm".to_string(),
                        filename: "gemma-4-E2B-it.litertlm".to_string(),
                        url: "https://huggingface.co/litert-community/gemma-4-E2B-it-litert-lm/resolve/main/gemma-4-E2B-it.litertlm".to_string(),
                        expected_size: Some(1_200_000_000),
                        required: true,
                    }
                ]
            }),
    );

    // NOTE: SpeculativeVADGate is now auto-registered via #[derive(NodeConfig)]
    // See speculative_vad_gate.rs - the schema is collected via inventory

    // Video nodes
    registry.register(
        NodeSchema::new("VideoFlip")
            .description("Flips video frames horizontally or vertically")
            .category("video")
            .accepts([RuntimeDataType::Video])
            .produces([RuntimeDataType::Video])
            .config_schema(serde_json::json!({
                "type": "object",
                "properties": {
                    "horizontal": {
                        "type": "boolean",
                        "description": "Flip horizontally",
                        "default": true
                    },
                    "vertical": {
                        "type": "boolean",
                        "description": "Flip vertically",
                        "default": false
                    }
                }
            })),
    );

    registry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_registry() {
        let registry = create_builtin_schema_registry();

        assert!(registry.get("Echo").is_some());
        assert!(registry.get("KokoroTTSNode").is_some());
        assert!(registry.get("NonExistent").is_none());
    }

    #[test]
    fn test_json_export() {
        let registry = create_builtin_schema_registry();
        let json = registry.to_json();

        assert!(json.is_array());
        let arr = json.as_array().unwrap();
        assert!(!arr.is_empty());
    }
}
