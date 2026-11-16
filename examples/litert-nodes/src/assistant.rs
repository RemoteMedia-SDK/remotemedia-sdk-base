//! `GemmaAssistantNode`: a voice assistant in a multi-person room, on a
//! LiteRT-LM model (e.g. Gemma 4 E2B).
//!
//! The node is a [`RoomAgent`] (crate `remotemedia-room-agent`): it reads the
//! room, decides which lines are for it, runs one reply at a time, publishes
//! it and handles cancellation; see that crate's docs and
//! `remotemedia-plugin-sdk/docs/ROOM_AGENT.md` for the frames it reads and
//! publishes, the room-agent parameters (`name`, `wake_words`, `respond`,
//! `follow_up_secs`, `member_id`, `publish`, `sentences`) and both
//! publication modes (`publish: "room"` for a room agent, unset for a
//! pipeline model node's `{"kind": "assistant"}` messages).
//!
//! This module is the model side, [`LiteRtLmResponder`]: one conversation is
//! kept (the model's KV cache holds the history) and started over when it
//! nears the context length, or after a cancelled reply. A reply's prompt is
//! the room's lines since the last reply; a conversation started over gets
//! the recent lines instead, so it knows what was said, including a reply
//! that was cut off (marked as such). The first piece spoken may end at a
//! comma, so speech starts sooner.
//!
//! Model params (JSON; `${VAR}` expands in string values):
//!
//! | Key | Default | |
//! |-----|---------|---|
//! | `model_path` | required | `.litertlm` model |
//! | `runtime_lib` | platform default | LiteRT-LM C API library (also `REMOTEMEDIA_LITERT_LM_LIB`) |
//! | `backend` | `gpu` | `gpu` or `cpu` |
//! | `cache_dir` | none | compiled GPU kernels, kept between runs |
//! | `threads` | 0 (default) | CPU threads |
//! | `max_tokens` | 4096 | context length |
//! | `max_reply_tokens` | 160 | |
//! | `system_prompt` | built in | |

use std::sync::{mpsc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::StreamExt;
use remotemedia_plugin_sdk::{
    abi_stable::{
        sabi_trait::TD_Opaque,
        std_types::{RErr, ROk, RResult, RString},
    },
    adapter::StreamingNodeFfiAdapter,
    params::expand_env_params,
    traits::CancelToken,
    types::Error,
    FfiNodeBox, FfiNodeFactory, FfiNode_TO,
};
use remotemedia_room_agent::{
    Line, Outcome, ReplyOut, Responder, RoomAgent, RoomAgentConfig, Sentences, Turn,
};
use serde_json::Value;

use crate::lm::{Engine, LmConfig, Piece};

pub const NODE_TYPE: &str = "GemmaAssistantNode";

/// Tokens left free before the conversation is started over.
const CONTEXT_HEADROOM: usize = 800;

/// How often the model thread looks at a reply's cancellation while it
/// waits for the next piece.
const CANCEL_POLL: Duration = Duration::from_millis(20);

#[derive(Clone, Debug)]
pub struct AssistantConfig {
    pub lm: LmConfig,
    pub room: RoomAgentConfig,
    pub system_prompt: String,
    pub max_reply_tokens: i32,
}

fn default_system(name: &str) -> String {
    format!(
        "You are {name}, a voice assistant taking part in a group voice chat. \
         People's speech reaches you as transcripts, prefixed with their names; \
         transcripts can contain recognition mistakes. Your replies are spoken \
         aloud to everyone: answer in one to three short, plain sentences, with \
         no markdown, lists, emoji or code. Address people by name when it helps."
    )
}

/// Parses the node's params, expanding `${VAR}` in string values.
pub fn config_from_params(params: &Value) -> Result<AssistantConfig, String> {
    let params = expand_env_params(params);
    let lm = LmConfig::from_params(&params, NODE_TYPE)?;
    let room = RoomAgentConfig::from_params(&params)?;
    let system_prompt = params["system_prompt"]
        .as_str()
        .filter(|v| !v.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| default_system(&room.name));
    Ok(AssistantConfig {
        lm,
        system_prompt,
        max_reply_tokens: params["max_reply_tokens"].as_i64().unwrap_or(160) as i32,
        room,
    })
}

/// The node: the room agent around the LiteRT-LM model.
pub type GemmaAssistantNode = RoomAgent<LiteRtLmResponder>;

/// Loads the model and starts the node.
pub fn start(config: AssistantConfig) -> Result<GemmaAssistantNode, String> {
    let responder = LiteRtLmResponder::start(&config)?;
    Ok(RoomAgent::new(NODE_TYPE, config.room, responder))
}

struct Job {
    prompt: String,
    /// The prompt for a conversation started over since the last reply: it
    /// carries the recent lines that conversation never saw.
    fresh_prompt: String,
    pieces: futures_channel::mpsc::UnboundedSender<Piece>,
    /// The reply's cancellation: the model thread stops the reply when it
    /// fires, also before the first token.
    cancel: CancelToken,
}

/// Replies from a LiteRT-LM model running on its own thread.
pub struct LiteRtLmResponder {
    jobs: Mutex<mpsc::Sender<Job>>,
}

impl LiteRtLmResponder {
    pub fn start(config: &AssistantConfig) -> Result<Self, String> {
        let (jobs_tx, jobs_rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (lm, system, max_reply_tokens) = (
            config.lm.clone(),
            config.system_prompt.clone(),
            config.max_reply_tokens,
        );
        std::thread::Builder::new()
            .name("litert-lm".into())
            .spawn(move || model_thread(lm, system, max_reply_tokens, jobs_rx, ready_tx))
            .map_err(|e| e.to_string())?;
        let runtime = ready_rx
            .recv()
            .map_err(|_| "the model thread exited while loading".to_string())??;
        eprintln!("[{NODE_TYPE}] loaded {runtime}");
        Ok(Self {
            jobs: Mutex::new(jobs_tx),
        })
    }
}

/// The model thread: one engine and conversation; replies stream back as
/// pieces.
fn model_thread(
    lm: LmConfig,
    system_prompt: String,
    max_reply_tokens: i32,
    jobs: mpsc::Receiver<Job>,
    ready: mpsc::Sender<Result<String, String>>,
) {
    let engine = match Engine::load(&lm) {
        Ok(e) => e,
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };
    // `None` after a cancelled reply, until the next job replaces it.
    let mut conversation = match engine.conversation(&system_prompt) {
        Ok(c) => Some(c),
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };
    let _ = ready.send(Ok(engine.runtime.clone()));
    // The conversation has had no prompt since it was started over.
    let mut fresh = false;
    while let Ok(job) = jobs.recv() {
        let full = conversation
            .as_ref()
            .is_some_and(|c| c.tokens() + CONTEXT_HEADROOM > engine.max_tokens);
        if full || conversation.is_none() {
            match engine.conversation(&system_prompt) {
                Ok(c) => {
                    if full {
                        eprintln!("[{NODE_TYPE}] context full; started a new conversation");
                    }
                    conversation = Some(c);
                    fresh = true;
                }
                Err(e) => {
                    let _ = job.pieces.unbounded_send(Piece::Error(e));
                    continue;
                }
            }
        }
        let Some(current) = conversation.as_mut() else {
            continue;
        };
        let prompt = if fresh {
            &job.fresh_prompt
        } else {
            &job.prompt
        };
        let rx = match current.send(prompt, max_reply_tokens) {
            Ok(rx) => rx,
            Err(e) => {
                let _ = job.pieces.unbounded_send(Piece::Error(e));
                continue;
            }
        };
        fresh = false;
        // Forward until the reply ends, then take the next job. The other
        // end of `pieces` is gone when nobody wants the rest: the room
        // stopped the reply, or the runtime dropped the call (a barge, a
        // cancelled or closed session).
        // It also stops as soon as the reply's token fires, without waiting
        // for the next token (openspec: add-generator-cancellation).
        let mut stopped = false;
        loop {
            if job.cancel.is_cancelled() {
                stopped = true;
                break;
            }
            let piece = match rx.recv_timeout(CANCEL_POLL) {
                Ok(piece) => piece,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            };
            let end = matches!(piece, Piece::Done | Piece::Error(_));
            let sent = job.pieces.unbounded_send(piece);
            if end {
                break;
            }
            if sent.is_err() {
                stopped = true;
                break;
            }
        }
        if stopped {
            // A cancelled conversation cannot be used again: delete it (the
            // stream ends with it); the next job starts another.
            current.cancel();
            conversation = None;
            eprintln!("[{NODE_TYPE}] reply stopped; the next one starts a new conversation");
        }
    }
}

/// The prompt for a reply to `speaker`, after `lines` of the room.
fn reply_prompt<'a>(
    lines: impl IntoIterator<Item = &'a Line>,
    speaker: &str,
    said: &str,
) -> String {
    let lines: Vec<String> = lines.into_iter().map(Line::prompt_line).collect();
    format!(
        "What was said in the room:\n{}\n\n{speaker} said to you: \"{said}\"\nReply to {speaker}.",
        lines.join("\n")
    )
}

#[async_trait]
impl Responder for LiteRtLmResponder {
    fn sentences(&self) -> Sentences {
        Sentences::spoken()
    }

    async fn respond(&self, turn: Turn<'_>, out: &mut ReplyOut<'_>) -> Result<Outcome, Error> {
        let (tx, mut rx) = futures_channel::mpsc::unbounded();
        self.jobs
            .lock()
            .unwrap()
            .send(Job {
                prompt: reply_prompt(&turn.history.since_reply, turn.speaker, turn.question),
                fresh_prompt: reply_prompt(&turn.history.recent, turn.speaker, turn.question),
                pieces: tx,
                cancel: turn.cancel.clone(),
            })
            .map_err(|_| Error::Execution("the model thread is gone".into()))?;
        while let Some(piece) = rx.next().await {
            match piece {
                Piece::Text(text) => {
                    if !out.text(&text)? {
                        // Stopped: the model thread cancels the reply when it
                        // finds this end of the channel gone.
                        return Ok(Outcome::Cancelled);
                    }
                }
                Piece::Error(e) => {
                    eprintln!("[{NODE_TYPE}] reply {} failed: {e}", turn.reply);
                    break;
                }
                Piece::Done => break,
            }
        }
        Ok(Outcome::Done)
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
        match config_from_params(&params).and_then(start) {
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
    use remotemedia_room_agent::Publish;
    use serde_json::json;

    #[test]
    fn sentences_param() {
        let base = json!({"model_path": "m.litertlm"});
        assert_eq!(
            config_from_params(&base).unwrap().room.publish,
            Publish::Pipeline {
                sentences_as_text: false
            }
        );
        let mut text = base.clone();
        text["sentences"] = json!("text");
        assert_eq!(
            config_from_params(&text).unwrap().room.publish,
            Publish::Pipeline {
                sentences_as_text: true
            }
        );
        text["publish"] = json!("room");
        assert!(config_from_params(&text).is_err());
        let mut bad = base.clone();
        bad["sentences"] = json!("words");
        assert!(config_from_params(&bad).is_err());
    }

    #[test]
    fn publish_param() {
        let config =
            |publish: Value| config_from_params(&json!({"model_path": "m", "publish": publish}));
        assert!(!config(Value::Null).unwrap().room.is_room());
        assert!(config(json!("room")).unwrap().room.is_room());
        assert!(config(json!("elsewhere")).is_err());
    }

    #[test]
    fn model_path_is_required_and_expands() {
        assert!(config_from_params(&json!({"name": "Pixel"})).is_err());
        std::env::set_var("RM_GEMMA_TEST_MODELS", "/models");
        let config =
            config_from_params(&json!({"model_path": "${RM_GEMMA_TEST_MODELS}/g.litertlm"}))
                .unwrap();
        assert_eq!(config.lm.model_path, "/models/g.litertlm");
        assert!(config.system_prompt.starts_with("You are Pixel,"));
    }

    #[test]
    fn prompt_shape() {
        let lines = vec![
            Line::person("ana", "hi", None),
            Line::person("ben", "Pixel, time?", None),
            Line::agent("Pixel", "It is", true, None),
        ];
        assert_eq!(
            reply_prompt(&lines, "ben", "Pixel, time?"),
            "What was said in the room:\nana: hi\nben: Pixel, time?\nPixel: It is [interrupted before finishing]\n\nben said to you: \"Pixel, time?\"\nReply to ben."
        );
    }
}
