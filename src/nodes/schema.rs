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
    NodeCapabilitiesSchema, NodeConfigSchema, NodeParameter, NodeSchema, NodeSchemaRegistry,
    ParameterType, RegisteredNodeConfig, RuntimeDataType,
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
            .python(),
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
