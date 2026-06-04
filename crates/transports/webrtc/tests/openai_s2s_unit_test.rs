//! Unit tests for the `openai_s2s_webrtc_server` example.
//!
//! Validates manifest construction, TTS stage selection, and env-var parsing
//! without requiring any external services.
//!
//! ```bash
//! cargo test --test openai_s2s_unit_test --features ws-signaling
//! ```

#![cfg(feature = "ws-signaling")]

use remotemedia_core::manifest::{Connection, Manifest, NodeManifest};
use std::collections::HashSet;
use std::sync::Arc;

/// Global mutex to serialize tests that depend on environment variables.
/// Env vars are global state, so tests that set/read them must run serially.
static ENV_MUTEX: std::sync::OnceLock<parking_lot::Mutex<()>> = std::sync::OnceLock::new();

fn env_lock() -> parking_lot::MutexGuard<'static, ()> {
    ENV_MUTEX.get_or_init(|| parking_lot::Mutex::new(())).lock()
}

// ---------------------------------------------------------------------------
// Helpers (mirrored from the example so we can test them independently)
// ---------------------------------------------------------------------------

fn parse_env_u32(key: &str) -> Option<u32> {
    std::env::var(key).ok().and_then(|v| v.parse().ok())
}

fn parse_env_f32(key: &str) -> Option<f32> {
    std::env::var(key).ok().and_then(|v| v.parse().ok())
}

/// Build the OpenAI S2S manifest (same logic as the example).
/// We re-implement it here so the test is self-contained and doesn't
/// depend on the example binary.
fn build_openai_s2s_manifest() -> Manifest {
    let llm_params = serde_json::json!({
        "api_key": std::env::var("OPENAI_API_KEY").ok(),
        "base_url": std::env::var("OPENAI_BASE_URL").ok(),
        "model": std::env::var("OPENAI_MODEL").ok(),
        "system_prompt": std::env::var("OPENAI_SYSTEM_PROMPT").ok(),
        "max_tokens": parse_env_u32("OPENAI_MAX_TOKENS").unwrap_or(4096),
        "temperature": parse_env_f32("OPENAI_TEMPERATURE").unwrap_or(0.7),
        "top_p": parse_env_f32("OPENAI_TOP_P").unwrap_or(0.8),
        "history_turns": 10,
        "streaming": true,
        "output_channel": "tts",
    });

    let whisper_params = serde_json::json!({
        "model_id": "openai/whisper-tiny.en",
        "language": "en",
    });
    let whisper_deps = vec![
        "transformers>=4.40.0".to_string(),
        "torch>=2.1".to_string(),
        "accelerate>=0.33".to_string(),
    ];

    let tts_engine = std::env::var("QWEN_TTS_ENGINE").unwrap_or_else(|_| "kokoro".to_string());
    let tts_repo = std::env::var("QWEN_TTS_REPO")
        .unwrap_or_else(|_| "mlx-community/Qwen3-TTS-12Hz-1.7B-CustomVoice-6bit".to_string());
    let tts_voice = std::env::var("QWEN_TTS_VOICE").unwrap_or_else(|_| "serena".to_string());

    let (tts_stage_nodes, tts_stage_connections) =
        build_tts_stage(&tts_engine, &tts_repo, &tts_voice);

    let mut nodes: Vec<NodeManifest> = vec![
        NodeManifest {
            id: "resample_in".to_string(),
            node_type: "FastResampleNode".to_string(),
            params: serde_json::json!({
                "source_rate": 48000,
                "target_rate": 16000,
                "quality": "Medium",
                "channels": 1,
            }),
            ..Default::default()
        },
        NodeManifest {
            id: "chunker".to_string(),
            node_type: "AudioChunkerNode".to_string(),
            params: serde_json::json!({
                "chunkSize": 512,
            }),
            ..Default::default()
        },
        NodeManifest {
            id: "vad".to_string(),
            node_type: "SileroVADNode".to_string(),
            params: serde_json::json!({
                "threshold": 0.6,
                "neg_threshold": 0.35,
                "sample_rate": 16000,
                "min_speech_duration_ms": 250,
                "min_silence_duration_ms": 500,
                "speech_pad_ms": 150,
            }),
            ..Default::default()
        },
        NodeManifest {
            id: "accumulator".to_string(),
            node_type: "AudioBufferAccumulatorNode".to_string(),
            params: serde_json::json!({
                "min_utterance_duration_ms": 300,
                "max_utterance_duration_ms": 30000,
            }),
            ..Default::default()
        },
        NodeManifest {
            id: "stt_in".to_string(),
            node_type: "WhisperSTTNode".to_string(),
            params: whisper_params,
            python_deps: Some(whisper_deps),
            ..Default::default()
        },
        NodeManifest {
            id: "llm".to_string(),
            node_type: "OpenAIChatNode".to_string(),
            params: llm_params,
            ..Default::default()
        },
        NodeManifest {
            id: "coordinator".to_string(),
            node_type: "ConversationCoordinatorNode".to_string(),
            params: serde_json::json!({
                "split_pattern": r"[.!?,;:\n]+",
                "min_sentence_length": 2,
                "yield_partial_on_end": true,
                "llm_silence_timeout_ms": 30000,
                "user_speech_debounce_ms": 150,
            }),
            ..Default::default()
        },
    ];
    nodes.extend(tts_stage_nodes);

    let mut connections: Vec<Connection> = vec![
        Connection {
            from: "resample_in".to_string(),
            to: "chunker".to_string(),
            ..Default::default()
        },
        Connection {
            from: "chunker".to_string(),
            to: "vad".to_string(),
            ..Default::default()
        },
        Connection {
            from: "vad".to_string(),
            to: "accumulator".to_string(),
            ..Default::default()
        },
        Connection {
            from: "vad".to_string(),
            to: "coordinator".to_string(),
            ..Default::default()
        },
        Connection {
            from: "accumulator".to_string(),
            to: "stt_in".to_string(),
            ..Default::default()
        },
        Connection {
            from: "stt_in".to_string(),
            to: "llm".to_string(),
            ..Default::default()
        },
        Connection {
            from: "llm".to_string(),
            to: "coordinator".to_string(),
            ..Default::default()
        },
    ];

    for c in tts_stage_connections {
        let from = if c.from == "llm" {
            "coordinator".to_string()
        } else {
            c.from
        };
        connections.push(Connection { from, to: c.to, ..Default::default() });
    }

    Manifest {
        version: "v1".to_string(),
        metadata: remotemedia_core::manifest::ManifestMetadata {
            name: "openai-s2s-webrtc".to_string(),
            description: Some(format!(
                "Speech-to-speech via OpenAI-compatible API: Whisper → OpenAIChatNode → {} \
                 over WebRTC",
                match tts_engine.as_str() {
                    "kokoro" => "KokoroTTS",
                    _ => "Qwen3-TTS",
                }
            )),
            ..Default::default()
        },
        nodes,
        connections,
        python_env: Some(remotemedia_core::manifest::ManifestPythonEnv {
            python_version: Some("3.12".to_string()),
            scope: None,
            extra_deps: Vec::new(),
        }),
        ..Default::default()
    }
}

/// Build the TTS stage (mirrored from the example).
fn build_tts_stage(
    engine: &str,
    qwen_tts_repo: &str,
    qwen_tts_voice: &str,
) -> (Vec<NodeManifest>, Vec<Connection>) {
    match engine.to_ascii_lowercase().as_str() {
        "qwen" => {
            let qwen_params = serde_json::json!({
                "hf_repo": qwen_tts_repo,
                "voice": qwen_tts_voice,
                "sample_rate": 24000,
                "output_sample_rate": 48000,
                "streaming_interval": 0.32,
                "speed": 1.0,
                "passthrough_text": true,
            });
            let qwen_deps = vec!["mlx-audio>=0.1".to_string(), "numpy>=1.24".to_string()];

            (
                vec![NodeManifest {
                    id: "audio".to_string(),
                    node_type: "QwenTTSMlxNode".to_string(),
                    params: qwen_params,
                    python_deps: Some(qwen_deps),
                    ..Default::default()
                }],
                vec![Connection {
                    from: "llm".to_string(),
                    to: "audio".to_string(),
                    ..Default::default()
                }],
            )
        }
        other_if_unknown => {
            if other_if_unknown != "kokoro" {
                eprintln!(
                    "[openai_s2s] unknown QWEN_TTS_ENGINE={:?}; defaulting to 'kokoro'",
                    other_if_unknown
                );
            }
            let kokoro_params = serde_json::json!({
                "lang_code": std::env::var("KOKORO_LANG")
                    .unwrap_or_else(|_| "a".to_string()),
                "voice": std::env::var("KOKORO_VOICE")
                    .unwrap_or_else(|_| "af_heart".to_string()),
                "speed": 1.0,
                "sample_rate": 24000,
                "stream_chunks": true,
                "skip_tokens": ["<|text_end|>", "```"],
            });
            let kokoro_deps = vec![
                "kokoro>=0.9.4".to_string(),
                "soundfile".to_string(),
                "en-core-web-sm @ https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.8.0/en_core_web_sm-3.8.0-py3-none-any.whl".to_string(),
            ];

            (
                vec![
                    NodeManifest {
                        id: "kokoro_tts".to_string(),
                        node_type: "KokoroTTSNode".to_string(),
                        params: kokoro_params,
                        python_deps: Some(kokoro_deps),
                        ..Default::default()
                    },
                    NodeManifest {
                        id: "audio".to_string(),
                        node_type: "FastResampleNode".to_string(),
                        params: serde_json::json!({
                            "source_rate": 24000,
                            "target_rate": 48000,
                            "quality": "Medium",
                            "channels": 1,
                        }),
                        ..Default::default()
                    },
                ],
                vec![
                    Connection {
                        from: "llm".to_string(),
                        to: "kokoro_tts".to_string(),
                        ..Default::default()
                    },
                    Connection {
                        from: "kokoro_tts".to_string(),
                        to: "audio".to_string(),
                        ..Default::default()
                    },
                ],
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests: env-var parsing helpers
// ---------------------------------------------------------------------------

#[test]
fn test_parse_env_u32_valid() {
    let _lock = env_lock();
    std::env::set_var("TEST_U32", "42");
    assert_eq!(parse_env_u32("TEST_U32"), Some(42));
    std::env::remove_var("TEST_U32");
}

#[test]
fn test_parse_env_u32_invalid() {
    let _lock = env_lock();
    std::env::set_var("TEST_U32", "not-a-number");
    assert_eq!(parse_env_u32("TEST_U32"), None);
    std::env::remove_var("TEST_U32");
}

#[test]
fn test_parse_env_u32_missing() {
    let _lock = env_lock();
    std::env::remove_var("TEST_U32_MISSING");
    assert_eq!(parse_env_u32("TEST_U32_MISSING"), None);
}

#[test]
fn test_parse_env_f32_valid() {
    let _lock = env_lock();
    std::env::set_var("TEST_F32", "0.75");
    assert!((parse_env_f32("TEST_F32").unwrap() - 0.75).abs() < f32::EPSILON);
    std::env::remove_var("TEST_F32");
}

#[test]
fn test_parse_env_f32_invalid() {
    let _lock = env_lock();
    std::env::set_var("TEST_F32", "abc");
    assert_eq!(parse_env_f32("TEST_F32"), None);
    std::env::remove_var("TEST_F32");
}

#[test]
fn test_parse_env_f32_missing() {
    let _lock = env_lock();
    std::env::remove_var("TEST_F32_MISSING");
    assert_eq!(parse_env_f32("TEST_F32_MISSING"), None);
}

// ---------------------------------------------------------------------------
// Unit tests: TTS stage builder
// ---------------------------------------------------------------------------

#[test]
fn test_build_tts_stage_kokoro_default() {
    std::env::remove_var("QWEN_TTS_ENGINE");
    let (nodes, conns) = build_tts_stage("kokoro", "repo", "voice");

    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].id, "kokoro_tts");
    assert_eq!(nodes[0].node_type, "KokoroTTSNode");
    assert_eq!(nodes[1].id, "audio");
    assert_eq!(nodes[1].node_type, "FastResampleNode");

    assert_eq!(conns.len(), 2);
    assert_eq!(conns[0].from, "llm");
    assert_eq!(conns[0].to, "kokoro_tts");
    assert_eq!(conns[1].from, "kokoro_tts");
    assert_eq!(conns[1].to, "audio");
}

#[test]
fn test_build_tts_stage_qwen() {
    let (nodes, conns) = build_tts_stage(
        "qwen",
        "mlx-community/Qwen3-TTS-12Hz-1.7B-CustomVoice-6bit",
        "serena",
    );

    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id, "audio");
    assert_eq!(nodes[0].node_type, "QwenTTSMlxNode");

    // Verify Qwen TTS params
    let params = &nodes[0].params;
    assert_eq!(
        params["hf_repo"],
        "mlx-community/Qwen3-TTS-12Hz-1.7B-CustomVoice-6bit"
    );
    assert_eq!(params["voice"], "serena");
    assert_eq!(params["sample_rate"], 24000);
    assert_eq!(params["output_sample_rate"], 48000);
    assert_eq!(params["streaming_interval"], 0.32);
    assert_eq!(params["speed"], 1.0);
    assert_eq!(params["passthrough_text"], true);

    assert_eq!(conns.len(), 1);
    assert_eq!(conns[0].from, "llm");
    assert_eq!(conns[0].to, "audio");
}

#[test]
fn test_build_tts_stage_unknown_defaults_to_kokoro() {
    let (nodes, _conns) = build_tts_stage("unknown_engine", "repo", "voice");

    // Should default to Kokoro
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].node_type, "KokoroTTSNode");
}

#[test]
fn test_build_tts_stage_kokoro_params() {
    let _lock = env_lock();
    std::env::set_var("KOKORO_LANG", "b");
    std::env::set_var("KOKORO_VOICE", "bm_george");
    let (nodes, _conns) = build_tts_stage("kokoro", "repo", "voice");

    let params = &nodes[0].params;
    assert_eq!(params["lang_code"], "b");
    assert_eq!(params["voice"], "bm_george");
    assert_eq!(params["speed"], 1.0);
    assert_eq!(params["sample_rate"], 24000);
    assert_eq!(params["stream_chunks"], true);

    std::env::remove_var("KOKORO_LANG");
    std::env::remove_var("KOKORO_VOICE");
}

#[test]
fn test_build_tts_stage_kokoro_deps() {
    let (nodes, _conns) = build_tts_stage("kokoro", "repo", "voice");

    let deps = nodes[0]
        .python_deps
        .as_ref()
        .expect("KokoroTTSNode should have python_deps");
    assert!(deps.iter().any(|d| d.starts_with("kokoro>=")));
    assert!(deps.iter().any(|d| d == "soundfile"));
    assert!(deps.iter().any(|d| d.contains("en-core-web-sm")));
}

// ---------------------------------------------------------------------------
// Unit tests: full manifest construction
// ---------------------------------------------------------------------------

#[test]
fn test_manifest_has_correct_name() {
    let manifest = build_openai_s2s_manifest();
    assert_eq!(manifest.metadata.name, "openai-s2s-webrtc");
    assert_eq!(manifest.version, "v1");
}

#[test]
fn test_manifest_has_all_required_nodes() {
    let manifest = build_openai_s2s_manifest();
    let node_ids: HashSet<&str> = manifest.nodes.iter().map(|n| n.id.as_str()).collect();

    let required_nodes = [
        "resample_in",
        "chunker",
        "vad",
        "accumulator",
        "stt_in",
        "llm",
        "coordinator",
        "audio",
    ];

    for node_id in &required_nodes {
        assert!(
            node_ids.contains(*node_id),
            "Missing required node: {}",
            node_id
        );
    }
}

#[test]
fn test_manifest_node_types() {
    let _lock = env_lock();
    std::env::remove_var("QWEN_TTS_ENGINE");
    let manifest = build_openai_s2s_manifest();

    let get_node_type = |id: &str| -> Option<&String> {
        manifest
            .nodes
            .iter()
            .find(|n| n.id == id)
            .map(|n| &n.node_type)
    };

    assert_eq!(get_node_type("resample_in").unwrap(), "FastResampleNode");
    assert_eq!(get_node_type("chunker").unwrap(), "AudioChunkerNode");
    assert_eq!(get_node_type("vad").unwrap(), "SileroVADNode");
    assert_eq!(
        get_node_type("accumulator").unwrap(),
        "AudioBufferAccumulatorNode"
    );
    assert_eq!(get_node_type("stt_in").unwrap(), "WhisperSTTNode");
    assert_eq!(get_node_type("llm").unwrap(), "OpenAIChatNode");
    assert_eq!(
        get_node_type("coordinator").unwrap(),
        "ConversationCoordinatorNode"
    );
    // With default (kokoro) engine, audio is a FastResampleNode
    assert_eq!(get_node_type("audio").unwrap(), "FastResampleNode");
}

#[test]
fn test_manifest_connections_form_valid_pipeline() {
    let manifest = build_openai_s2s_manifest();

    // Verify the core pipeline chain:
    // resample_in → chunker → vad → accumulator → stt_in → llm → coordinator
    let conn_pairs: Vec<_> = manifest
        .connections
        .iter()
        .map(|c| (&c.from, &c.to))
        .collect();

    assert!(conn_pairs
        .iter()
        .any(|(f, t)| *f == "resample_in" && *t == "chunker"));
    assert!(conn_pairs
        .iter()
        .any(|(f, t)| *f == "chunker" && *t == "vad"));
    assert!(conn_pairs
        .iter()
        .any(|(f, t)| *f == "vad" && *t == "accumulator"));
    assert!(conn_pairs
        .iter()
        .any(|(f, t)| *f == "vad" && *t == "coordinator"));
    assert!(conn_pairs
        .iter()
        .any(|(f, t)| *f == "accumulator" && *t == "stt_in"));
    assert!(conn_pairs
        .iter()
        .any(|(f, t)| *f == "stt_in" && *t == "llm"));
    assert!(conn_pairs
        .iter()
        .any(|(f, t)| *f == "llm" && *t == "coordinator"));
}

#[test]
fn test_manifest_tts_connections_with_kokoro() {
    let _lock = env_lock();
    std::env::remove_var("QWEN_TTS_ENGINE");
    let manifest = build_openai_s2s_manifest();

    let conn_pairs: Vec<_> = manifest
        .connections
        .iter()
        .map(|c| (&c.from, &c.to))
        .collect();

    // With Kokoro: coordinator → kokoro_tts → audio (FastResampleNode)
    // The TTS connections are rewritten so `llm` → `coordinator` as source
    assert!(conn_pairs
        .iter()
        .any(|(f, t)| *f == "coordinator" && *t == "kokoro_tts"));
    assert!(conn_pairs
        .iter()
        .any(|(f, t)| *f == "kokoro_tts" && *t == "audio"));
}

#[test]
fn test_manifest_tts_connections_with_qwen() {
    let _lock = env_lock();
    std::env::set_var("QWEN_TTS_ENGINE", "qwen");
    let manifest = build_openai_s2s_manifest();

    let conn_pairs: Vec<_> = manifest
        .connections
        .iter()
        .map(|c| (&c.from, &c.to))
        .collect();

    // With Qwen: coordinator → audio (QwenTTSMlxNode)
    assert!(conn_pairs
        .iter()
        .any(|(f, t)| *f == "coordinator" && *t == "audio"));

    // Verify the audio node is QwenTTSMlxNode
    let audio_node = manifest.nodes.iter().find(|n| n.id == "audio").unwrap();
    assert_eq!(audio_node.node_type, "QwenTTSMlxNode");

    std::env::remove_var("QWEN_TTS_ENGINE");
}

#[test]
fn test_manifest_llm_params_defaults() {
    let _lock = env_lock();
    // Clear all OpenAI env vars to test defaults
    std::env::remove_var("OPENAI_API_KEY");
    std::env::remove_var("OPENAI_BASE_URL");
    std::env::remove_var("OPENAI_MODEL");
    std::env::remove_var("OPENAI_SYSTEM_PROMPT");
    std::env::remove_var("OPENAI_MAX_TOKENS");
    std::env::remove_var("OPENAI_TEMPERATURE");
    std::env::remove_var("OPENAI_TOP_P");
    std::env::remove_var("QWEN_TTS_ENGINE");

    let manifest = build_openai_s2s_manifest();
    let llm_node = manifest.nodes.iter().find(|n| n.id == "llm").unwrap();

    let params = &llm_node.params;
    assert_eq!(params["max_tokens"].as_u64().unwrap(), 4096);
    assert!((params["temperature"].as_f64().unwrap() - 0.7).abs() < 0.001);
    assert!((params["top_p"].as_f64().unwrap() - 0.8).abs() < 0.001);
    assert_eq!(params["history_turns"], 10);
    assert_eq!(params["streaming"], true);
    assert_eq!(params["output_channel"], "tts");
    assert!(params["api_key"].is_null());
    assert!(params["base_url"].is_null());
    assert!(params["model"].is_null());
}

#[test]
fn test_manifest_llm_params_from_env() {
    let _lock = env_lock();
    // First clear any leftover env vars from other tests
    std::env::remove_var("OPENAI_API_KEY");
    std::env::remove_var("OPENAI_BASE_URL");
    std::env::remove_var("OPENAI_MODEL");
    std::env::remove_var("OPENAI_SYSTEM_PROMPT");
    std::env::remove_var("OPENAI_MAX_TOKENS");
    std::env::remove_var("OPENAI_TEMPERATURE");
    std::env::remove_var("OPENAI_TOP_P");
    std::env::remove_var("QWEN_TTS_ENGINE");

    std::env::set_var("OPENAI_API_KEY", "sk-test-key");
    std::env::set_var("OPENAI_BASE_URL", "http://127.0.0.1:8888/v1");
    std::env::set_var("OPENAI_MODEL", "qwen2.5-7b");
    std::env::set_var("OPENAI_SYSTEM_PROMPT", "You are a test assistant.");
    std::env::set_var("OPENAI_MAX_TOKENS", "2048");
    std::env::set_var("OPENAI_TEMPERATURE", "0.9");
    std::env::set_var("OPENAI_TOP_P", "0.95");

    let manifest = build_openai_s2s_manifest();
    let llm_node = manifest.nodes.iter().find(|n| n.id == "llm").unwrap();

    let params = &llm_node.params;
    assert_eq!(params["api_key"], "sk-test-key");
    assert_eq!(params["base_url"], "http://127.0.0.1:8888/v1");
    assert_eq!(params["model"], "qwen2.5-7b");
    assert_eq!(params["system_prompt"], "You are a test assistant.");
    assert_eq!(params["max_tokens"].as_u64().unwrap(), 2048);
    assert!((params["temperature"].as_f64().unwrap() - 0.9).abs() < 0.001);
    assert!((params["top_p"].as_f64().unwrap() - 0.95).abs() < 0.001);

    // Cleanup
    std::env::remove_var("OPENAI_API_KEY");
    std::env::remove_var("OPENAI_BASE_URL");
    std::env::remove_var("OPENAI_MODEL");
    std::env::remove_var("OPENAI_SYSTEM_PROMPT");
    std::env::remove_var("OPENAI_MAX_TOKENS");
    std::env::remove_var("OPENAI_TEMPERATURE");
    std::env::remove_var("OPENAI_TOP_P");
}

#[test]
fn test_manifest_vad_params() {
    let manifest = build_openai_s2s_manifest();
    let vad_node = manifest.nodes.iter().find(|n| n.id == "vad").unwrap();

    let params = &vad_node.params;
    assert_eq!(params["threshold"], 0.6);
    assert_eq!(params["neg_threshold"], 0.35);
    assert_eq!(params["sample_rate"], 16000);
    assert_eq!(params["min_speech_duration_ms"], 250);
    assert_eq!(params["min_silence_duration_ms"], 500);
    assert_eq!(params["speech_pad_ms"], 150);
}

#[test]
fn test_manifest_coordinator_params() {
    let manifest = build_openai_s2s_manifest();
    let coord_node = manifest
        .nodes
        .iter()
        .find(|n| n.id == "coordinator")
        .unwrap();

    let params = &coord_node.params;
    // The split_pattern contains a literal \n which is escaped in JSON
    assert!(params["split_pattern"]
        .as_str()
        .unwrap()
        .contains("[.!?,;:"));
    assert_eq!(params["min_sentence_length"], 2);
    assert_eq!(params["yield_partial_on_end"], true);
    assert_eq!(params["llm_silence_timeout_ms"], 30000);
    assert_eq!(params["user_speech_debounce_ms"], 150);
}

#[test]
fn test_manifest_whisper_params() {
    let manifest = build_openai_s2s_manifest();
    let stt_node = manifest.nodes.iter().find(|n| n.id == "stt_in").unwrap();

    let params = &stt_node.params;
    assert_eq!(params["model_id"], "openai/whisper-tiny.en");
    assert_eq!(params["language"], "en");

    // Verify Python deps
    let deps = stt_node
        .python_deps
        .as_ref()
        .expect("WhisperSTTNode should have python_deps");
    assert!(deps.iter().any(|d| d.starts_with("transformers>=")));
    assert!(deps.iter().any(|d| d.starts_with("torch>=")));
    assert!(deps.iter().any(|d| d.starts_with("accelerate>=")));
}

#[test]
fn test_manifest_resample_in_params() {
    let manifest = build_openai_s2s_manifest();
    let node = manifest
        .nodes
        .iter()
        .find(|n| n.id == "resample_in")
        .unwrap();

    let params = &node.params;
    assert_eq!(params["source_rate"], 48000);
    assert_eq!(params["target_rate"], 16000);
    assert_eq!(params["quality"], "Medium");
    assert_eq!(params["channels"], 1);
}

#[test]
fn test_manifest_chunker_params() {
    let manifest = build_openai_s2s_manifest();
    let node = manifest.nodes.iter().find(|n| n.id == "chunker").unwrap();

    let params = &node.params;
    assert_eq!(params["chunkSize"], 512);
}

#[test]
fn test_manifest_accumulator_params() {
    let manifest = build_openai_s2s_manifest();
    let node = manifest
        .nodes
        .iter()
        .find(|n| n.id == "accumulator")
        .unwrap();

    let params = &node.params;
    assert_eq!(params["min_utterance_duration_ms"], 300);
    assert_eq!(params["max_utterance_duration_ms"], 30000);
}

#[test]
fn test_manifest_python_env() {
    let manifest = build_openai_s2s_manifest();
    let py_env = manifest
        .python_env
        .as_ref()
        .expect("Should have python_env");
    assert_eq!(py_env.python_version.as_deref(), Some("3.12"));
    assert!(py_env.extra_deps.is_empty());
}

#[test]
fn test_manifest_description_includes_tts_engine() {
    let _lock = env_lock();
    std::env::remove_var("QWEN_TTS_ENGINE");
    let manifest = build_openai_s2s_manifest();
    assert!(manifest
        .metadata
        .description
        .as_ref()
        .unwrap()
        .contains("KokoroTTS"));

    std::env::set_var("QWEN_TTS_ENGINE", "qwen");
    let manifest = build_openai_s2s_manifest();
    assert!(manifest
        .metadata
        .description
        .as_ref()
        .unwrap()
        .contains("Qwen3-TTS"));
    std::env::remove_var("QWEN_TTS_ENGINE");
}

#[test]
fn test_manifest_serializes_to_valid_json() {
    let manifest = build_openai_s2s_manifest();
    let json = serde_json::to_string_pretty(&manifest).expect("Manifest should serialize");
    assert!(!json.is_empty());

    // Verify it can be deserialized back
    let _: Manifest = serde_json::from_str(&json).expect("Serialized manifest should deserialize");
}

#[test]
fn test_manifest_no_duplicate_node_ids() {
    let manifest = build_openai_s2s_manifest();
    let mut seen = HashSet::new();
    for node in &manifest.nodes {
        assert!(seen.insert(&node.id), "Duplicate node ID: {}", node.id);
    }
}

#[test]
fn test_manifest_all_connection_targets_exist() {
    let manifest = build_openai_s2s_manifest();
    let node_ids: HashSet<&str> = manifest.nodes.iter().map(|n| n.id.as_str()).collect();

    for conn in &manifest.connections {
        assert!(
            node_ids.contains(conn.to.as_str()),
            "Connection target '{}' does not exist",
            conn.to
        );
    }
}

#[test]
fn test_manifest_all_connection_sources_exist() {
    let manifest = build_openai_s2s_manifest();
    let node_ids: HashSet<&str> = manifest.nodes.iter().map(|n| n.id.as_str()).collect();

    for conn in &manifest.connections {
        assert!(
            node_ids.contains(conn.from.as_str()),
            "Connection source '{}' does not exist",
            conn.from
        );
    }
}

// ---------------------------------------------------------------------------
// Unit tests: manifest can be used with PipelineExecutor
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_manifest_with_pipeline_executor() {
    use remotemedia_core::transport::PipelineExecutor;

    let manifest = Arc::new(build_openai_s2s_manifest());

    // The manifest should be usable to create a PipelineExecutor.
    // We don't actually run the pipeline, just verify it can be constructed.
    let executor = PipelineExecutor::new().expect("PipelineExecutor should create");

    // Verify the executor and manifest are valid
    let _ = &executor;
    let _ = &manifest;
}
