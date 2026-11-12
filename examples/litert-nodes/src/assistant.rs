//! `GemmaAssistantNode`: a voice assistant in a multi-person room, on a
//! LiteRT-LM model (e.g. Gemma 4 E2B).
//!
//! It reads everything said in the room: transcripts (Text, attributed to
//! the speaker by the runtime) and typed messages (`{"kind": "text", "text"}`
//! JSON). When a line names the assistant (`wake_words`, or every line with
//! `respond: "all"`), it replies to the room, with the lines since its last
//! reply as context. A line that is little more than the name ("Hey Pixel")
//! gets no reply of its own: turn detection often ends a turn there, so the
//! same speaker's next line within `follow_up_secs` is taken as addressed.
//! The first piece spoken may end at a comma, so speech starts sooner. One conversation is kept (the model's KV cache holds
//! the history) and started over when it nears the context length.
//!
//! Output, as the reply streams (JSON):
//!
//! * `{"kind": "assistant", "state": "listening", "to", "for_ms"}`: a bare
//!   "Hey Pixel"; it waits `for_ms` for the question;
//! * `{"kind": "assistant", "state": "thinking", "reply", "to"}`: a reply has
//!   started generating (the page shows the assistant thinking until the
//!   first sentence);
//! * `{"kind": "assistant", "say": "<sentence>", "reply": "<id>", "to": "<name>"}`:
//!   one complete sentence, for text-to-speech and for the page;
//! * `{"kind": "assistant", "text": "<reply>", "reply": "<id>", "done": true,
//!   "first_ms", "total_ms"}`: the whole reply.
//!
//! Params (JSON):
//!
//! | Key | Default | |
//! |-----|---------|---|
//! | `model_path` | required | `.litertlm` model (`${VAR}` expands) |
//! | `runtime_lib` | platform default | LiteRT-LM C API library (also `REMOTEMEDIA_LITERT_LM_LIB`) |
//! | `backend` | `gpu` | `gpu` or `cpu` |
//! | `cache_dir` | none | compiled GPU kernels, kept between runs |
//! | `threads` | 0 (default) | CPU threads |
//! | `max_tokens` | 4096 | context length |
//! | `max_reply_tokens` | 160 | |
//! | `name` | `Pixel` | what the assistant is called |
//! | `wake_words` | `[name]` | words that address it (case-insensitive) |
//! | `respond` | `mention` | `mention` or `all` |
//! | `follow_up_secs` | 8 | how long a bare "Hey Pixel" waits for its question |
//! | `system_prompt` | built in | |

use std::collections::{HashMap, VecDeque};
use std::sync::{mpsc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::StreamExt;
use remotemedia_plugin_sdk::{
    abi_stable::{
        sabi_trait::TD_Opaque,
        std_types::{RErr, ROk, RResult, RString},
    },
    adapter::StreamingNodeFfiAdapter,
    traits::{AsyncStreamingNode, NodeRuntimeContextRead},
    types::{Error, RuntimeData},
    FfiNodeBox, FfiNodeFactory, FfiNode_TO,
};
use serde_json::{json, Value};

use crate::lm::{Engine, LmConfig, Piece, Sentences};
use crate::node::expand;

pub const NODE_TYPE: &str = "GemmaAssistantNode";

/// Lines kept for context between replies.
const MAX_PENDING: usize = 24;
/// Tokens left free before the conversation is started over.
const CONTEXT_HEADROOM: usize = 800;

#[derive(Clone, Debug)]
pub struct AssistantConfig {
    pub lm: LmConfig,
    pub name: String,
    pub wake_words: Vec<String>,
    pub respond_all: bool,
    pub system_prompt: String,
    pub max_reply_tokens: i32,
    pub follow_up: Duration,
}

/// Words that do not make a line a question ("hey Pixel", "ok Pixel").
const FILLER: &[&str] = &["hey", "hi", "hello", "ok", "okay", "so", "um", "uh", "yo", "excuse", "me"];

fn default_system(name: &str) -> String {
    format!(
        "You are {name}, a voice assistant taking part in a group voice chat. \
         People's speech reaches you as transcripts, prefixed with their names; \
         transcripts can contain recognition mistakes. Your replies are spoken \
         aloud to everyone: answer in one to three short, plain sentences, with \
         no markdown, lists, emoji or code. Address people by name when it helps."
    )
}

pub fn config_from_params(params: &Value) -> Result<AssistantConfig, String> {
    let s = |key: &str| params[key].as_str().map(expand).filter(|v| !v.is_empty());
    let model_path = s("model_path").ok_or("GemmaAssistantNode needs params.model_path")?;
    let mut lm = LmConfig::new(model_path);
    lm.runtime_lib = s("runtime_lib");
    if let Some(b) = s("backend") {
        lm.backend = b;
    }
    lm.cache_dir = s("cache_dir");
    if let Some(t) = params["threads"].as_i64() {
        lm.threads = t as i32;
    }
    if let Some(t) = params["max_tokens"].as_i64() {
        lm.max_tokens = t as i32;
    }
    let name = s("name").unwrap_or_else(|| "Pixel".into());
    let wake_words = match params["wake_words"].as_array() {
        Some(words) => words
            .iter()
            .filter_map(|w| w.as_str())
            .map(str::to_lowercase)
            .collect(),
        None => vec![name.to_lowercase()],
    };
    let respond_all = match s("respond").as_deref() {
        None | Some("mention") => false,
        Some("all") => true,
        Some(other) => return Err(format!("respond must be mention or all, not {other:?}")),
    };
    Ok(AssistantConfig {
        lm,
        system_prompt: s("system_prompt").unwrap_or_else(|| default_system(&name)),
        name,
        wake_words,
        respond_all,
        max_reply_tokens: params["max_reply_tokens"].as_i64().unwrap_or(160) as i32,
        follow_up: Duration::from_secs_f64(params["follow_up_secs"].as_f64().unwrap_or(8.0)),
    })
}

struct Job {
    prompt: String,
    pieces: futures_channel::mpsc::UnboundedSender<Piece>,
}

struct Room {
    /// "name: line" since the last reply.
    pending: VecDeque<String>,
    replies: u64,
    /// Speakers who named the assistant without a question yet: when, and
    /// what they said.
    waiting: HashMap<String, (Instant, String)>,
}

enum Addressed {
    No,
    /// Only the name: wait for the question.
    NameOnly,
    Yes,
}

pub struct GemmaAssistantNode {
    config: AssistantConfig,
    jobs: Mutex<mpsc::Sender<Job>>,
    room: Mutex<Room>,
    /// One reply at a time, in order.
    turn: futures_util::lock::Mutex<()>,
    /// Tags this instance's reply ids.
    instance: String,
}

/// The model thread: one engine and conversation; replies stream back as
/// pieces.
fn model_thread(config: AssistantConfig, jobs: mpsc::Receiver<Job>, ready: mpsc::Sender<Result<String, String>>) {
    let engine = match Engine::load(&config.lm) {
        Ok(e) => e,
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };
    let mut conversation = match engine.conversation(&config.system_prompt) {
        Ok(c) => c,
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };
    let _ = ready.send(Ok(engine.runtime.clone()));
    while let Ok(job) = jobs.recv() {
        if conversation.tokens() + CONTEXT_HEADROOM > engine.max_tokens {
            match engine.conversation(&config.system_prompt) {
                Ok(c) => {
                    eprintln!("[{NODE_TYPE}] context full; started a new conversation");
                    conversation = c;
                }
                Err(e) => {
                    let _ = job.pieces.unbounded_send(Piece::Error(e));
                    continue;
                }
            }
        }
        let rx = match conversation.send(&job.prompt, config.max_reply_tokens) {
            Ok(rx) => rx,
            Err(e) => {
                let _ = job.pieces.unbounded_send(Piece::Error(e));
                continue;
            }
        };
        // Forward until the reply ends, then take the next job.
        for piece in rx {
            let end = matches!(piece, Piece::Done | Piece::Error(_));
            let _ = job.pieces.unbounded_send(piece);
            if end {
                break;
            }
        }
    }
}

/// `participant.name` when the client set one, else the peer id without the
/// page's `-xxxx` suffix.
fn speaker_name(ctx: &dyn NodeRuntimeContextRead) -> String {
    let Some(origin) = ctx.origin() else {
        return "Someone".into();
    };
    if let Some(name) = origin.fields().get("participant.name").filter(|n| !n.is_empty()) {
        return name.clone();
    }
    let id = origin.id();
    match id.rsplit_once('-') {
        Some((name, tag)) if tag.len() == 4 && tag.chars().all(|c| c.is_ascii_hexdigit()) => name.to_string(),
        _ => id.to_string(),
    }
}

impl GemmaAssistantNode {
    pub fn start(config: AssistantConfig) -> Result<Self, String> {
        let (jobs_tx, jobs_rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        let thread_config = config.clone();
        std::thread::Builder::new()
            .name("litert-lm".into())
            .spawn(move || model_thread(thread_config, jobs_rx, ready_tx))
            .map_err(|e| e.to_string())?;
        let runtime = ready_rx
            .recv()
            .map_err(|_| "the model thread exited while loading".to_string())??;
        eprintln!("[{NODE_TYPE}] loaded {}", runtime);
        Ok(Self {
            config,
            jobs: Mutex::new(jobs_tx),
            room: Mutex::new(Room {
                pending: VecDeque::new(),
                replies: 0,
                waiting: HashMap::new(),
            }),
            turn: futures_util::lock::Mutex::new(()),
            instance: format!(
                "{:04x}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.subsec_nanos() & 0xffff)
                    .unwrap_or(0)
            ),
        })
    }

    fn addressed(&self, text: &str) -> Addressed {
        if self.config.respond_all {
            return Addressed::Yes;
        }
        let words: Vec<String> = text
            .split(|c: char| !c.is_alphanumeric() && c != '\'')
            .filter(|w| !w.is_empty())
            .map(str::to_lowercase)
            .collect();
        let is_wake = |w: &String| self.config.wake_words.iter().any(|wake| wake == w);
        if !words.iter().any(is_wake) {
            return Addressed::No;
        }
        let rest = words
            .iter()
            .filter(|w| !is_wake(w) && !FILLER.contains(&w.as_str()))
            .count();
        if rest <= 1 {
            Addressed::NameOnly
        } else {
            Addressed::Yes
        }
    }

    async fn reply<F>(&self, speaker: &str, said: &str, mut callback: F) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let _turn = self.turn.lock().await;
        let (prompt, reply) = {
            let mut room = self.room.lock().unwrap();
            room.replies += 1;
            let lines: Vec<String> = room.pending.drain(..).collect();
            (
                format!(
                    "What was said in the room:\n{}\n\n{speaker} said to you: \"{said}\"\nReply to {speaker}.",
                    lines.join("\n")
                ),
                // Unique across instances, so a reply from the runner a unit
                // moved to is not mistaken for one from before the move.
                format!("{}-{}", self.instance, room.replies),
            )
        };
        let (tx, mut rx) = futures_channel::mpsc::unbounded();
        self.jobs
            .lock()
            .unwrap()
            .send(Job { prompt, pieces: tx })
            .map_err(|_| Error::Execution("the model thread is gone".into()))?;
        callback(RuntimeData::Json(json!({
            "kind": "assistant", "state": "thinking", "reply": reply, "to": speaker,
        })))?;

        let start = Instant::now();
        let mut first_ms = None;
        let mut sentences = Sentences::first_clause();
        let mut full = String::new();
        let mut outputs = 1;
        let mut say = |sentence: String, outputs: &mut usize, callback: &mut F| -> Result<(), Error> {
            eprintln!(
                "[{NODE_TYPE}] reply {reply} +{:.0} ms: {sentence}",
                start.elapsed().as_secs_f64() * 1000.0
            );
            *outputs += 1;
            callback(RuntimeData::Json(json!({
                "kind": "assistant", "say": sentence, "reply": reply, "to": speaker,
            })))
        };
        while let Some(piece) = rx.next().await {
            match piece {
                Piece::Text(text) => {
                    first_ms.get_or_insert_with(|| start.elapsed().as_secs_f64() * 1000.0);
                    full.push_str(&text);
                    for sentence in sentences.push(&text) {
                        say(sentence, &mut outputs, &mut callback)?;
                    }
                }
                Piece::Error(e) => {
                    eprintln!("[{NODE_TYPE}] reply {reply} failed: {e}");
                    break;
                }
                Piece::Done => break,
            }
        }
        if let Some(rest) = sentences.finish() {
            say(rest, &mut outputs, &mut callback)?;
        }
        let total_ms = start.elapsed().as_secs_f64() * 1000.0;
        let text = full.trim().to_string();
        self.room
            .lock()
            .unwrap()
            .pending
            .push_back(format!("{}: {text}", self.config.name));
        callback(RuntimeData::Json(json!({
            "kind": "assistant", "text": text, "reply": reply, "to": speaker, "done": true,
            "first_ms": first_ms.map(|m| m.round()), "total_ms": total_ms.round(),
        })))?;
        Ok(outputs + 1)
    }
}

/// The words a line says: a transcript (Text) or a typed message.
fn line_text(data: &RuntimeData) -> Option<String> {
    match data {
        RuntimeData::Text(t) => Some(t.trim().to_string()),
        RuntimeData::Json(v) if v["kind"] == "text" => v["text"].as_str().map(|t| t.trim().to_string()),
        _ => None,
    }
    .filter(|t| !t.is_empty())
}

#[async_trait]
impl AsyncStreamingNode for GemmaAssistantNode {
    fn node_type(&self) -> &str {
        NODE_TYPE
    }

    async fn process(&self, _data: RuntimeData) -> Result<RuntimeData, Error> {
        Err(Error::InvalidData(format!("{NODE_TYPE} streams its replies")))
    }

    async fn process_streaming<F>(
        &self,
        _data: RuntimeData,
        _session_id: Option<String>,
        _callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        Ok(0)
    }

    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let Some(text) = line_text(&data) else {
            return Ok(0);
        };
        let mut callback = callback;
        let speaker = speaker_name(ctx);
        let addressed = self.addressed(&text);
        let said = {
            let mut room = self.room.lock().unwrap();
            room.pending.push_back(format!("{speaker}: {text}"));
            while room.pending.len() > MAX_PENDING {
                room.pending.pop_front();
            }
            let follow_up = room
                .waiting
                .remove(&speaker)
                .filter(|(at, _)| at.elapsed() <= self.config.follow_up)
                .map(|(_, name)| format!("{name} {text}"));
            match addressed {
                Addressed::Yes => Some(text.clone()),
                Addressed::NameOnly => {
                    room.waiting.insert(speaker.clone(), (Instant::now(), text.clone()));
                    None
                }
                Addressed::No => follow_up,
            }
        };
        let Some(said) = said else {
            if matches!(addressed, Addressed::NameOnly) {
                // Shown on the page while it waits for the question.
                callback(RuntimeData::Json(json!({
                    "kind": "assistant", "state": "listening", "to": speaker,
                    "for_ms": self.config.follow_up.as_millis() as u64,
                })))?;
                return Ok(1);
            }
            return Ok(0);
        };
        self.reply(&speaker, &said, callback).await
    }
}

#[derive(Default)]
pub struct GemmaAssistantFactory;

impl FfiNodeFactory for GemmaAssistantFactory {
    fn node_type(&self) -> RString {
        NODE_TYPE.into()
    }

    fn create(&self, params: RString) -> RResult<FfiNodeBox, RString> {
        let params: Value = serde_json::from_str(params.as_str()).unwrap_or(Value::Null);
        match config_from_params(&params).and_then(GemmaAssistantNode::start) {
            Ok(node) => ROk(FfiNode_TO::from_value(
                StreamingNodeFfiAdapter::new(node),
                TD_Opaque,
            )),
            Err(e) => RErr(format!("{NODE_TYPE}: {e}").into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_and_spoken_lines() {
        assert_eq!(line_text(&RuntimeData::Text(" hi ".into())).as_deref(), Some("hi"));
        assert_eq!(
            line_text(&RuntimeData::Json(json!({"kind": "text", "text": "yo"}))).as_deref(),
            Some("yo")
        );
        assert!(line_text(&RuntimeData::Json(json!({"kind": "turn", "state": "end"}))).is_none());
    }
}
