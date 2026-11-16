//! The loaded engine is reusable; each RAG turn owns a fresh conversation.
use super::{
    prompt::{self, BudgetError, SYSTEM},
    protocol::Evidence,
};
use crate::lm::{Engine, LmConfig, Piece};
use remotemedia_plugin_sdk::traits::CancelToken;
use serde_json::Value;
use std::{
    sync::mpsc,
    time::{Duration, Instant},
};

pub struct Job {
    pub question: String,
    pub history: Vec<Value>,
    pub evidence: Vec<Evidence>,
    pub reply_id: String,
    pub reply_tokens: i32,
    pub deadline: Instant,
    pub cancel: CancelToken,
    pub output: tokio::sync::mpsc::Sender<Event>,
}
pub enum Event {
    Prepared(Vec<Evidence>),
    InsufficientContext,
    Piece(Piece),
}
pub struct Model {
    jobs: mpsc::SyncSender<Job>,
}
impl Model {
    pub fn start(config: LmConfig) -> Result<Self, String> {
        let (jobs, receiver) = mpsc::sync_channel::<Job>(1);
        let (ready, loaded) = mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("rag-gemma".into())
            .spawn(move || {
                let engine = match Engine::load(&config) {
                    Ok(engine) => engine,
                    Err(e) => {
                        let _ = ready.send(Err(e));
                        return;
                    }
                };
                // Fail readiness if this runtime cannot budget the actual template.
                if let Err(e) = engine
                    .conversation(SYSTEM)
                    .and_then(|mut c| c.first_prompt_tokens(&engine, "readiness"))
                {
                    let _ = ready.send(Err(e));
                    return;
                }
                let _ = ready.send(Ok(()));
                while let Ok(job) = receiver.recv() {
                    if job.cancel.is_cancelled() || job.output.is_closed() {
                        continue;
                    }
                    if let Err(e) = generate(&engine, &job) {
                        let _ = job.output.blocking_send(Event::Piece(Piece::Error(e)));
                    }
                }
            })
            .map_err(|e| e.to_string())?;
        loaded.recv().map_err(|_| "RAG model thread exited")??;
        Ok(Self { jobs })
    }
    pub fn send(&self, job: Job) -> Result<(), String> {
        self.jobs
            .try_send(job)
            .map_err(|_| "RAG model is busy".into())
    }
}

fn generate(engine: &Engine, job: &Job) -> Result<(), String> {
    if Instant::now() >= job.deadline {
        return Err("timeout".into());
    }
    let mut conversation = engine.conversation(SYSTEM)?;
    let (prompt, selected, prompt_tokens) = match prompt::grounded(
        &job.question,
        &job.history,
        &job.evidence,
        engine.max_tokens,
        job.reply_tokens as usize,
        |text| conversation.first_prompt_tokens(engine, text),
    ) {
        Ok(prepared) => prepared,
        Err(BudgetError::Context) => {
            let _ = job.output.blocking_send(Event::InsufficientContext);
            return Ok(());
        }
        Err(BudgetError::Tokenizer(error)) => return Err(error),
    };
    if job.cancel.is_cancelled() || job.output.is_closed() {
        return Ok(());
    }
    if Instant::now() >= job.deadline {
        return Err("timeout".into());
    }
    eprintln!(
        "rag prompt reply={} tokens={} reply_budget={} capacity={} chunks={}",
        job.reply_id,
        prompt_tokens,
        job.reply_tokens,
        engine.max_tokens,
        selected.len()
    );
    if job.output.blocking_send(Event::Prepared(selected)).is_err() {
        return Ok(());
    }
    let receiver = conversation.send(&prompt, job.reply_tokens)?;
    loop {
        if job.cancel.is_cancelled() || job.output.is_closed() {
            conversation.cancel();
            return Ok(());
        }
        if Instant::now() >= job.deadline {
            conversation.cancel();
            return Err("timeout".into());
        }
        match receiver.recv_timeout(Duration::from_millis(20)) {
            Ok(piece) => {
                let done = matches!(piece, Piece::Done | Piece::Error(_));
                if job.output.blocking_send(Event::Piece(piece)).is_err() {
                    conversation.cancel();
                    return Ok(());
                }
                if done {
                    return Ok(());
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err("generation ended without a terminal event".into())
            }
        }
    }
}
