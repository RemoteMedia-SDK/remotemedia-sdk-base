//! Real-model generator conformance (openspec: add-generator-cancellation):
//! a cancelled Gemma reply stops, and the replies after it are not held up.
//!
//! Needs `REMOTEMEDIA_GEMMA_LITERTLM` (the `.litertlm` model) and
//! `REMOTEMEDIA_LITERT_LM_LIB` (the LiteRT-LM library); skipped without them.
//! `REMOTEMEDIA_GEMMA_BACKEND` picks `cpu` (default) or `gpu`.
//! Run in release: `cargo test --release --test cancel_conformance -- --nocapture`.

use std::sync::Arc;
use std::time::Duration;

use remotemedia_litert_nodes::assistant::{config_from_params, start};
use remotemedia_plugin_sdk::testing::{assert_generator_cancels, ConformanceOptions};
use remotemedia_plugin_sdk::types::RuntimeData;
use serde_json::json;

/// Model text: a sentence (`say`) or plain text, not the status frames.
fn is_model_text(data: &RuntimeData) -> bool {
    match data {
        RuntimeData::Text(_) => true,
        RuntimeData::Json(v) => v.get("say").is_some(),
        _ => false,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_gemma_reply_does_not_hold_up_the_next() {
    let (Ok(model), Ok(lib)) = (
        std::env::var("REMOTEMEDIA_GEMMA_LITERTLM"),
        std::env::var("REMOTEMEDIA_LITERT_LM_LIB"),
    ) else {
        eprintln!("skipped: set REMOTEMEDIA_GEMMA_LITERTLM and REMOTEMEDIA_LITERT_LM_LIB");
        return;
    };
    // `gpu` needs the WebGPU runtime (dxcompiler.dll on Windows) beside the
    // test binary; the cancel behavior is the same on either backend.
    let backend = std::env::var("REMOTEMEDIA_GEMMA_BACKEND").unwrap_or_else(|_| "cpu".into());
    let config = config_from_params(&json!({
        "model_path": model,
        "runtime_lib": lib,
        "backend": backend,
        "respond": "all",
    }))
    .unwrap();
    let node = Arc::new(start(config).unwrap());
    let report = assert_generator_cancels(
        node,
        || RuntimeData::Text("Tell me a long story about the ocean, in many sentences.".into()),
        ConformanceOptions {
            first_output_timeout: Duration::from_secs(60),
            next_call_slack: Duration::from_millis(1500),
            counts: is_model_text,
            ..Default::default()
        },
    )
    .await
    .unwrap_or_else(|e| panic!("{e}"));
    eprintln!("baseline first text: {:?}", report.baseline);
    for (cancellation, next) in &report.cancelled {
        eprintln!(
            "after a {:?} cancel, first text in {next:?}",
            cancellation.disposition
        );
    }
}

/// Kokoro: a cancelled utterance stops before its next slice, so the next
/// one starts on time. Needs `REMOTEMEDIA_KOKORO_LITERTLM` (and
/// `REMOTEMEDIA_LITERT_LIB` where the LiteRT library is not found by default).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_kokoro_utterance_does_not_hold_up_the_next() {
    let Ok(model) = std::env::var("REMOTEMEDIA_KOKORO_LITERTLM") else {
        eprintln!("skipped: set REMOTEMEDIA_KOKORO_LITERTLM");
        return;
    };
    let mut params = json!({ "model_path": model, "accelerator": "cpu", "chunk_ms": 200 });
    if let Ok(lib) = std::env::var("REMOTEMEDIA_LITERT_LIB") {
        params["runtime_lib"] = lib.into();
    }
    let (config, chunk) = remotemedia_litert_nodes::tts_node::config_from_params(&params).unwrap();
    let node =
        Arc::new(remotemedia_litert_nodes::tts_node::KokoroTtsNode::start(config, chunk).unwrap());
    // Many sentences: synthesis spans many slices, so a run-to-the-end
    // regression would hold up the next call by seconds.
    let text = "The ocean covers most of the planet and holds most of its water. ".repeat(12);
    let report = assert_generator_cancels(
        node,
        move || RuntimeData::Text(text.clone()),
        ConformanceOptions {
            first_output_timeout: Duration::from_secs(60),
            next_call_slack: Duration::from_millis(1500),
            counts: |data| matches!(data, RuntimeData::Audio { .. }),
            ..Default::default()
        },
    )
    .await
    .unwrap_or_else(|e| panic!("{e}"));
    eprintln!("baseline first audio: {:?}", report.baseline);
    for (cancellation, next) in &report.cancelled {
        eprintln!(
            "after a {:?} cancel, first audio in {next:?}",
            cancellation.disposition
        );
    }
}
