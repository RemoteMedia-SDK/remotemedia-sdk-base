//! Real-model generator conformance (openspec: add-generator-cancellation):
//! a cancelled Gemma reply stops, and the replies after it are not held up.
//!
//! Needs `REMOTEMEDIA_GEMMA_LITERTLM` (the `.litertlm` model) and
//! `REMOTEMEDIA_LITERT_LM_LIB` (the LiteRT-LM library); skipped without them.
//! `REMOTEMEDIA_GEMMA_BACKEND` picks `cpu` (default) or `gpu`.
//! Run in release: `cargo test --release --test cancel_conformance -- --nocapture`.

use std::sync::Arc;
use std::time::Duration;

use remotemedia_litert_nodes::assistant::{config_from_params, GemmaAssistantNode};
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
    let node = Arc::new(GemmaAssistantNode::start(config).unwrap());
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
        eprintln!("after a {:?} cancel, first text in {next:?}", cancellation.disposition);
    }
}
