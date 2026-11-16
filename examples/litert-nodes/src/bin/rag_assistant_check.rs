//! Controlled direct-node quality harness; this does not measure room/audio transport.
//! Usage: rag-assistant-check CONFIG.json INPUT.jsonl
//! Input: {"id":"case-name","data":{...room line/history...}}
use remotemedia_litert_nodes::rag::{config_from_params, RagAssistantNode};
use remotemedia_plugin_sdk::{
    traits::{AsyncStreamingNode, CancelToken, MediaCapabilities, NodeRuntimeContextRead},
    types::RuntimeData,
};
use serde_json::{json, Value};
use std::{
    io::{self, BufRead, Write},
    sync::Arc,
    time::Instant,
};

struct Context;
impl NodeRuntimeContextRead for Context {
    fn session_id(&self) -> &str {
        "rag-quality-check"
    }
    fn node_id(&self) -> &str {
        "assistant"
    }
    fn cancel_token(&self) -> CancelToken {
        CancelToken::never()
    }
    fn capability(&self, _: &str) -> Option<MediaCapabilities> {
        None
    }
    fn insert_capability(&self, _: &str, _: MediaCapabilities) {}
    fn snapshot_any(&self, _: &str) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
    fn state_any(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
    fn try_state_any(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
}

fn emit(value: Value) {
    println!("{value}");
    io::stdout().flush().expect("flush evidence");
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        return Err("usage: rag-assistant-check CONFIG.json INPUT.jsonl".into());
    }
    let params: Value = serde_json::from_str(&std::fs::read_to_string(&args[1])?)?;
    let started = Instant::now();
    let node = RagAssistantNode::start(config_from_params(&params)?)?;
    emit(
        json!({"event":"ready", "load_ms":started.elapsed().as_secs_f64()*1000.0,
        "path":"direct_node", "answer_backend":params["answer_backend"]}),
    );
    for line in io::BufReader::new(std::fs::File::open(&args[2])?).lines() {
        let input: Value = serde_json::from_str(&line?)?;
        let started = Instant::now();
        let result = node
            .process_streaming_with_ctx(
                RuntimeData::Json(input["data"].clone()),
                &Context,
                |data| {
                    let payload = match data {
                        RuntimeData::Json(value) => value,
                        RuntimeData::Text(text) => json!({"text":text}),
                        _ => return Ok(()),
                    };
                    emit(json!({"event":"output", "id":input["id"],
                "elapsed_ms":started.elapsed().as_secs_f64()*1000.0, "payload":payload}));
                    Ok(())
                },
            )
            .await;
        emit(json!({"event":"complete", "id":input["id"],
            "elapsed_ms":started.elapsed().as_secs_f64()*1000.0,
            "error":result.as_ref().err().map(ToString::to_string)}));
        result?;
    }
    Ok(())
}
