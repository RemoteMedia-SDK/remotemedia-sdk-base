//! Private JSONL worker for the example's PC-local Gemma provider.
//! One engine, fresh conversations, correlated deltas and out-of-band cancel.
use remotemedia_litert_nodes::lm::{Engine, LmConfig, Piece};
use serde_json::{json, Value};
use std::{
    io::{self, BufRead, Write},
    sync::mpsc,
    time::{Duration, Instant},
};

fn emit(value: Value) {
    println!("{value}");
    let _ = io::stdout().flush();
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = LmConfig::new(std::env::var("REMOTEMEDIA_GEMMA_LITERTLM")?);
    config.runtime_lib = Some(std::env::var("REMOTEMEDIA_LITERT_LM_LIB")?);
    config.backend = std::env::var("REMOTEMEDIA_GEMMA_BACKEND").unwrap_or_else(|_| "cpu".into());
    config.cache_dir = std::env::var("REMOTEMEDIA_GEMMA_CACHE").ok();
    let engine = Engine::load(&config)?;
    engine
        .conversation("Readiness")?
        .first_prompt_tokens(&engine, "ready")?;
    emit(json!({"kind":"ready", "version":1, "context_tokens":engine.max_tokens}));
    let (tx, rx) = mpsc::sync_channel::<Value>(2);
    std::thread::spawn(move || {
        for line in io::stdin().lock().lines() {
            let Ok(line) = line else { break };
            if line.len() > 131072 {
                break;
            }
            let Ok(value) = serde_json::from_str(&line) else {
                break;
            };
            if tx.send(value).is_err() {
                break;
            }
        }
    });
    while let Ok(job) = rx.recv() {
        if job["kind"] == "cancel" {
            continue;
        }
        let id = job["request_id"].as_str().ok_or("missing request ID")?;
        match generate(&engine, &job, &rx) {
            Ok(()) => emit(json!({"kind":"done", "request_id":id})),
            Err(error) => emit(json!({"kind":"error", "request_id":id, "error":error})),
        }
    }
    Ok(())
}

fn generate(engine: &Engine, job: &Value, commands: &mpsc::Receiver<Value>) -> Result<(), String> {
    let id = job["request_id"].as_str().ok_or("missing request ID")?;
    let system = job["system"].as_str().ok_or("missing system prompt")?;
    let prompt = job["prompt"].as_str().ok_or("missing user prompt")?;
    let budget = job["max_tokens"]
        .as_u64()
        .filter(|n| (1..=1024).contains(n))
        .ok_or("invalid output budget")?;
    let mut conversation = engine.conversation(system)?;
    let tokens = conversation.first_prompt_tokens(engine, prompt)?;
    if tokens.saturating_add(budget as usize).saturating_add(32) > engine.max_tokens {
        return Err("context_full".into());
    }
    emit(json!({"kind":"prepared", "request_id":id, "prompt_tokens":tokens}));
    let pieces = conversation.send(prompt, budget as i32)?;
    let deadline = Instant::now() + Duration::from_secs(120);
    let mut output_bytes = 0usize;
    loop {
        match commands.try_recv() {
            Ok(command) if command["kind"] == "cancel" && command["request_id"] == id => {
                conversation.cancel();
                return Err("cancelled".into());
            }
            Ok(_) => {
                conversation.cancel();
                return Err("unexpected overlapping command".into());
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                conversation.cancel();
                return Err("closed".into());
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }
        if Instant::now() >= deadline {
            conversation.cancel();
            return Err("timeout".into());
        }
        match pieces.recv_timeout(Duration::from_millis(20)) {
            Ok(Piece::Text(text)) => {
                output_bytes = output_bytes.saturating_add(text.len());
                if output_bytes > 32768 {
                    conversation.cancel();
                    return Err("output limit".into());
                }
                emit(json!({"kind":"delta", "request_id":id, "text":text}));
            }
            Ok(Piece::Done) => return Ok(()),
            Ok(Piece::Error(error)) => return Err(error),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err("missing terminal event".into())
            }
        }
    }
}
