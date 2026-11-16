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
//! | `sentences` | `json` | `text`: each sentence is emitted as Text instead of the `say` message, for a text-to-speech node that takes Text |
//! | `publish` | unset | `room`: run as a room agent (below) |
//!
//! # As a room agent (`publish: "room"`)
//!
//! The node sits in the agent's own session, between a `RoomSourceNode` and
//! a `RoomSinkNode` (and a `KokoroTtsNode` for the voice), and speaks the
//! participant link's frames instead of the `assistant` messages above.
//!
//! Input, attributed to its author (`participant.id`,
//! `participant.display_name`):
//!
//! * a room-log line, `{"kind": "text" | "voice", "text", "seq",
//!   "author_kind": "person" | "agent"}`. A person's line is answered as
//!   above. An agent's line is context and is never answered, whatever
//!   `respond` says.
//! * the same with `"history": true`: context, never answered. The agent's
//!   own lines (an agent author whose display name or id is `name`, in any
//!   case) are kept under `name`. A speaker whose last history line is only
//!   the name is waiting for their question again, for `follow_up_secs` from
//!   the moment that line arrives (history lines carry no age).
//!   `{"kind": "history_end"}` ends the history.
//! * `{"kind": "reply_interrupted", "reply"}`: the session no longer wants
//!   that reply. If it is still being generated it stops at the next piece of
//!   text: nothing more is published for it, the model is cancelled and a new
//!   conversation is started, which gets the recent lines with its first
//!   prompt. The runtime hands a node one input at a time, so a frame that
//!   arrives while `process_streaming_with_ctx` is generating is read only
//!   after that reply has ended, and then changes nothing.
//!
//! A reply is also cancelled from outside: a barge wired to this node (an
//! `InterruptNode` fed with the start signal) makes the runtime drop the call
//! in flight. The model is cancelled the same way, what had been generated is
//! kept in the recent lines marked as cut off, and the next reply starts a
//! new conversation that is given those lines: the assistant knows what it
//! was saying and that it did not finish.
//! * anything else (`floor` granted / waiting / not_held / refused with
//!   `floor_busy`, `turn`, `presence`, `refused`) is ignored.
//!
//! Output (JSON):
//!
//! * `{"kind": "presence", "state": "listening", "to"}`: a bare "Hey Pixel";
//! * `{"kind": "presence", "state": "thinking", "reply", "to", "line"}`: a
//!   reply has started generating; `line` is the `seq` of the line it
//!   answers (of the question, when that followed a bare name), left out
//!   when the line had none;
//! * `{"kind": "text", "text": "<sentence>", "say": "<sentence>", "reply"}`:
//!   one complete sentence; the room posts `text`, `KokoroTtsNode` speaks
//!   `say`;
//! * `{"kind": "presence", "state": "idle", "reply"}`: the reply has ended.

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
    traits::{
        rpc::CallContext, AsyncStreamingNode, CancelStrategy, CancelToken, Cancellable,
        Cancellation, GeneratorRole, NodeRuntimeContextRead, Retention, TextGenerator,
    },
    types::{Error, RuntimeData},
    FfiNodeBox, FfiNodeFactory, FfiNode_TO,
};
use serde_json::{json, Value};

use crate::assistant_room::*;
use crate::lm::{Engine, LmConfig, Piece, Sentences};
use crate::node::expand;

pub const NODE_TYPE: &str = "GemmaAssistantNode";

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
    /// Run as a room agent: read the participant link's frames and publish
    /// lines and presence.
    pub publish_room: bool,
    /// Emit each sentence as Text instead of the `say` message (not as a
    /// room agent).
    pub sentences_as_text: bool,
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
    let publish_room = match s("publish").as_deref() {
        None => false,
        Some("room") => true,
        Some(other) => return Err(format!("publish must be room when set, not {other:?}")),
    };
    let sentences_as_text = match s("sentences").as_deref() {
        None | Some("json") => false,
        Some("text") if publish_room => {
            return Err("sentences: text does not apply to a room agent (publish: room)".into())
        }
        Some("text") => true,
        Some(other) => return Err(format!("sentences must be json or text, not {other:?}")),
    };
    Ok(AssistantConfig {
        lm,
        publish_room,
        sentences_as_text,
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
    /// The prompt for a conversation started over since the last reply: it
    /// carries the recent lines that conversation never saw.
    fresh_prompt: String,
    pieces: futures_channel::mpsc::UnboundedSender<Piece>,
    /// The call's cancellation: the model thread stops the reply when it
    /// fires, also before the first token.
    cancel: CancelToken,
}

/// How often the model thread looks at a reply's cancellation while it
/// waits for the next piece.
const CANCEL_POLL: Duration = Duration::from_millis(20);

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
fn model_thread(
    config: AssistantConfig,
    jobs: mpsc::Receiver<Job>,
    ready: mpsc::Sender<Result<String, String>>,
) {
    let engine = match Engine::load(&config.lm) {
        Ok(e) => e,
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };
    // `None` after a cancelled reply, until the next job replaces it.
    let mut conversation = match engine.conversation(&config.system_prompt) {
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
            match engine.conversation(&config.system_prompt) {
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
        let rx = match current.send(prompt, config.max_reply_tokens) {
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
        // It also stops as soon as the call's token fires, without waiting
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
    lines: impl IntoIterator<Item = &'a String>,
    speaker: &str,
    said: &str,
) -> String {
    let lines: Vec<&str> = lines.into_iter().map(String::as_str).collect();
    format!(
        "What was said in the room:\n{}\n\n{speaker} said to you: \"{said}\"\nReply to {speaker}.",
        lines.join("\n")
    )
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
            room: Mutex::new(Room::default()),
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
        addressed(&self.config.wake_words, self.config.respond_all, text)
    }

    /// Whether the author of a frame, an agent, is this assistant: its
    /// display name or id is `name`, in any case.
    fn is_self(&self, ctx: &dyn NodeRuntimeContextRead, speaker: &str) -> bool {
        let name = self.config.name.to_lowercase();
        speaker.to_lowercase() == name
            || ctx
                .origin()
                .is_some_and(|origin| origin.id().to_lowercase() == name)
    }

    /// Replies to what `speaker` said; `line` is the `seq` of that line in
    /// the room log, when it has one.
    async fn reply<F>(
        &self,
        speaker: &str,
        said: &str,
        line: Option<u64>,
        cancel: CancelToken,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let _turn = self.turn.lock().await;
        let as_agent = self.config.publish_room;
        let (prompt, fresh_prompt, reply) = {
            let mut room = self.room.lock().unwrap();
            room.replies += 1;
            let fresh_prompt = reply_prompt(&room.recent, speaker, said);
            let lines: Vec<String> = room.pending.drain(..).collect();
            (
                reply_prompt(&lines, speaker, said),
                fresh_prompt,
                // Unique across instances, so a reply from the runner a unit
                // moved to is not mistaken for one from before the move.
                format!("{}-{}", self.instance, room.replies),
            )
        };
        let (tx, mut rx) = futures_channel::mpsc::unbounded();
        self.jobs
            .lock()
            .unwrap()
            .send(Job {
                prompt,
                fresh_prompt,
                pieces: tx,
                cancel: cancel.clone(),
            })
            .map_err(|_| Error::Execution("the model thread is gone".into()))?;
        callback(RuntimeData::Json(if as_agent {
            presence_thinking(&reply, speaker, line)
        } else {
            json!({
                "kind": "assistant", "state": "thinking", "reply": reply, "to": speaker,
            })
        }))?;

        let start = Instant::now();
        let mut first_ms = None;
        let mut sentences = Sentences::first_clause();
        let mut partial = Partial {
            room: &self.room,
            name: &self.config.name,
            text: String::new(),
            ended: false,
            cancel,
        };
        let mut outputs = 1;
        let mut say =
            |sentence: String, outputs: &mut usize, callback: &mut F| -> Result<(), Error> {
                eprintln!(
                    "[{NODE_TYPE}] reply {reply} +{:.0} ms: {sentence}",
                    start.elapsed().as_secs_f64() * 1000.0
                );
                *outputs += 1;
                if self.config.sentences_as_text {
                    return callback(RuntimeData::Text(sentence));
                }
                callback(RuntimeData::Json(if as_agent {
                    sentence_line(&sentence, &reply)
                } else {
                    json!({
                        "kind": "assistant", "say": sentence, "reply": reply, "to": speaker,
                    })
                }))
            };
        let mut stopped = false;
        while let Some(piece) = rx.next().await {
            // The room stopped this reply: publish nothing more of it.
            if as_agent && self.room.lock().unwrap().stopped.as_deref() == Some(reply.as_str()) {
                eprintln!("[{NODE_TYPE}] reply {reply} stopped by the room");
                stopped = true;
                break;
            }
            match piece {
                Piece::Text(text) => {
                    first_ms.get_or_insert_with(|| start.elapsed().as_secs_f64() * 1000.0);
                    partial.text.push_str(&text);
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
        // The model thread cancels a stopped reply when it finds this end
        // of the channel gone.
        drop(rx);
        if !stopped {
            if let Some(rest) = sentences.finish() {
                say(rest, &mut outputs, &mut callback)?;
            }
        }
        let total_ms = start.elapsed().as_secs_f64() * 1000.0;
        // It ended here: what follows records it as it was.
        partial.ended = true;
        let text = partial.text.trim().to_string();
        if as_agent {
            // The sentences were the reply; what was generated of it stays
            // in the context.
            eprintln!(
                "[{NODE_TYPE}] reply {reply} ended: first text {:.0} ms, total {total_ms:.0} ms",
                first_ms.unwrap_or(total_ms)
            );
            if !text.is_empty() {
                self.room
                    .lock()
                    .unwrap()
                    .note(format!("{}: {text}", self.config.name));
            }
            callback(RuntimeData::Json(presence_idle(&reply)))?;
            return Ok(outputs + 1);
        }
        self.room
            .lock()
            .unwrap()
            .note(format!("{}: {text}", self.config.name));
        callback(RuntimeData::Json(json!({
            "kind": "assistant", "text": text, "reply": reply, "to": speaker, "done": true,
            "first_ms": first_ms.map(|m| m.round()), "total_ms": total_ms.round(),
        })))?;
        Ok(outputs + 1)
    }
}

#[async_trait]
impl AsyncStreamingNode for GemmaAssistantNode {
    fn node_type(&self) -> &str {
        NODE_TYPE
    }

    fn generator(&self) -> Option<GeneratorRole<'_>> {
        GeneratorRole::text(self)
    }

    async fn process(&self, _data: RuntimeData) -> Result<RuntimeData, Error> {
        Err(Error::InvalidData(format!(
            "{NODE_TYPE} streams its replies"
        )))
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
        let as_agent = self.config.publish_room;
        let (text, seq, history, by_agent) = if as_agent {
            match room_frame(&data) {
                Some(RoomFrame::Line {
                    text,
                    seq,
                    history,
                    agent,
                }) => (text, seq, history, agent),
                Some(RoomFrame::Stop(reply)) => {
                    self.room.lock().unwrap().stopped = Some(reply);
                    return Ok(0);
                }
                Some(RoomFrame::HistoryEnd) => {
                    let room = self.room.lock().unwrap();
                    eprintln!(
                        "[{NODE_TYPE}] history: {} lines, {} waiting for their question",
                        room.pending.len(),
                        room.waiting.len()
                    );
                    return Ok(0);
                }
                None => return Ok(0),
            }
        } else {
            let Some(text) = line_text(&data) else {
                return Ok(0);
            };
            (text, None, false, false)
        };
        let mut callback = callback;
        let speaker = if as_agent {
            room_speaker_name(ctx)
        } else {
            speaker_name(ctx)
        };
        if by_agent {
            // An agent's line is context: its own under its name, another's
            // under that agent's.
            let author = if self.is_self(ctx, &speaker) {
                &self.config.name
            } else {
                &speaker
            };
            self.room.lock().unwrap().note(format!("{author}: {text}"));
            return Ok(0);
        }
        let addressed = self.addressed(&text);
        if history {
            self.room
                .lock()
                .unwrap()
                .recall(&speaker, &text, &addressed, Instant::now());
            return Ok(0);
        }
        let said = self.room.lock().unwrap().hear(
            &speaker,
            &text,
            &addressed,
            self.config.follow_up,
            Instant::now(),
        );
        let Some(said) = said else {
            if matches!(addressed, Addressed::NameOnly) {
                // Shown on the page while it waits for the question.
                callback(RuntimeData::Json(if as_agent {
                    presence_listening(&speaker)
                } else {
                    json!({
                        "kind": "assistant", "state": "listening", "to": speaker,
                        "for_ms": self.config.follow_up.as_millis() as u64,
                    })
                }))?;
                return Ok(1);
            }
            return Ok(0);
        };
        self.reply(&speaker, &said, seq, ctx.cancel_token(), callback)
            .await
    }
}

/// A text generator (openspec: add-generator-cancellation). The model runs on
/// its own thread, which watches the call's token and stops the reply
/// (`Conversation::cancel`) as soon as it fires. A cut-off reply stays in the
/// context, marked, unless the cancel discards partial output; a cancel that
/// discards pending input also drops the lines heard since the last reply.
#[async_trait]
impl Cancellable for GemmaAssistantNode {
    fn cancel_strategy(&self) -> CancelStrategy {
        CancelStrategy::ObservesToken
    }

    async fn on_cancel(&self, _call: &CallContext, cancellation: &Cancellation) {
        if cancellation.disposition.pending_input == Retention::Discard {
            let dropped = {
                let mut room = self.room.lock().unwrap();
                let n = room.pending.len();
                room.pending.clear();
                n
            };
            if dropped > 0 {
                eprintln!("[{NODE_TYPE}] cancel discards {dropped} pending lines");
            }
        }
    }
}
impl TextGenerator for GemmaAssistantNode {}

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
        assert_eq!(
            line_text(&RuntimeData::Text(" hi ".into())).as_deref(),
            Some("hi")
        );
        assert_eq!(
            line_text(&RuntimeData::Json(json!({"kind": "text", "text": "yo"}))).as_deref(),
            Some("yo")
        );
        assert!(line_text(&RuntimeData::Json(json!({"kind": "turn", "state": "end"}))).is_none());
        // Outside a room agent a `voice` line is not read.
        assert!(line_text(&RuntimeData::Json(json!({"kind": "voice", "text": "yo"}))).is_none());
    }

    fn frame(v: Value) -> Option<RoomFrame> {
        room_frame(&RuntimeData::Json(v))
    }

    fn line(text: &str, seq: Option<u64>, history: bool, agent: bool) -> Option<RoomFrame> {
        Some(RoomFrame::Line {
            text: text.into(),
            seq,
            history,
            agent,
        })
    }

    #[test]
    fn sentences_param() {
        let base = json!({"model_path": "m.litertlm"});
        assert!(!config_from_params(&base).unwrap().sentences_as_text);
        let mut text = base.clone();
        text["sentences"] = json!("text");
        assert!(config_from_params(&text).unwrap().sentences_as_text);
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
        assert!(!config(Value::Null).unwrap().publish_room);
        assert!(config(json!("room")).unwrap().publish_room);
        assert!(config(json!("elsewhere")).is_err());
    }

    #[test]
    fn room_lines() {
        assert_eq!(
            frame(json!({"kind": "text", "text": " hi ", "seq": 3, "author_kind": "person"})),
            line("hi", Some(3), false, false)
        );
        assert_eq!(
            frame(json!({"kind": "voice", "text": "hey pixel", "seq": 4})),
            line("hey pixel", Some(4), false, false)
        );
        assert_eq!(
            frame(
                json!({"kind": "voice", "text": "yo", "seq": 1, "history": true, "author_kind": "agent"})
            ),
            line("yo", Some(1), true, true)
        );
        assert_eq!(
            frame(json!({"kind": "text", "text": "hi"})),
            line("hi", None, false, false)
        );
        assert_eq!(
            room_frame(&RuntimeData::Text(" hi ".into())),
            line("hi", None, false, false)
        );
        assert_eq!(frame(json!({"kind": "text", "text": "  "})), None);
        assert_eq!(
            frame(json!({"kind": "history_end", "seq": 4})),
            Some(RoomFrame::HistoryEnd)
        );
    }

    fn partial<'a>(
        room: &'a std::sync::Mutex<Room>,
        text: &str,
        ended: bool,
        cancel: CancelToken,
    ) -> Partial<'a> {
        Partial {
            room,
            name: "Pixel",
            text: text.into(),
            ended,
            cancel,
        }
    }

    #[test]
    fn a_reply_dropped_before_it_ended_stays_in_the_context_marked() {
        let room = std::sync::Mutex::new(Room::default());
        let mut cut = partial(&room, "", false, CancelToken::never());
        cut.text.push_str(" The largest ocean is ");
        drop(cut);
        assert_eq!(
            room.lock().unwrap().recent.back().map(String::as_str),
            Some("Pixel: The largest ocean is [interrupted before finishing]")
        );
        // A reply that ended, and one cut before any text, add nothing here.
        drop(partial(&room, "done", true, CancelToken::never()));
        drop(partial(&room, "  ", false, CancelToken::never()));
        assert_eq!(room.lock().unwrap().recent.len(), 1);
    }

    #[test]
    fn the_cancel_decides_whether_a_cut_off_reply_is_kept() {
        use remotemedia_plugin_sdk::traits::{CancelReason, CancelSource, Disposition};
        for (disposition, kept) in [(Disposition::INTERRUPT, 1), (Disposition::ABORT, 0)] {
            let room = std::sync::Mutex::new(Room::default());
            let source = CancelSource::new();
            source.cancel(Cancellation::with_disposition(
                CancelReason::BargeIn,
                disposition,
            ));
            drop(partial(
                &room,
                "The largest ocean is",
                false,
                source.token(),
            ));
            assert_eq!(room.lock().unwrap().recent.len(), kept, "{disposition:?}");
        }
    }

    #[test]
    fn room_stop_frames() {
        let stop = Some(RoomFrame::Stop("a1-2".into()));
        assert_eq!(
            frame(json!({"kind": "reply_interrupted", "reply": "a1-2", "reason": "barge"})),
            stop
        );
        // Text of a reply refused for a busy floor is still posted.
        assert_eq!(
            frame(
                json!({"kind": "floor", "event": "refused", "reason": "floor_busy", "reply": "a1-2"})
            ),
            None
        );
        for event in ["granted", "waiting", "not_held"] {
            assert_eq!(
                frame(json!({"kind": "floor", "event": event, "reply": "a1-2"})),
                None
            );
        }
        assert_eq!(frame(json!({"kind": "turn", "state": "start"})), None);
        assert_eq!(
            frame(json!({"kind": "presence", "state": "thinking", "reply": "b-1"})),
            None
        );
        assert_eq!(
            frame(json!({"kind": "refused", "what": "text", "reason": "rate"})),
            None
        );
    }

    fn named(text: &str) -> Addressed {
        addressed(&["pixel".to_string()], false, text)
    }

    #[test]
    fn names_and_questions() {
        assert_eq!(named("Hey Pixel"), Addressed::NameOnly);
        assert_eq!(named("Pixel, what time is it?"), Addressed::Yes);
        assert_eq!(named("what time is it?"), Addressed::No);
        assert_eq!(
            addressed(&["pixel".to_string()], true, "anything"),
            Addressed::Yes
        );
    }

    const FOLLOW_UP: Duration = Duration::from_secs(8);

    /// A live line of `speaker` at `now`.
    fn hear(room: &mut Room, speaker: &str, text: &str, now: Instant) -> Option<String> {
        room.hear(speaker, text, &named(text), FOLLOW_UP, now)
    }

    /// A history line of `speaker`, received at `now`.
    fn recall(room: &mut Room, speaker: &str, text: &str, now: Instant) {
        room.recall(speaker, text, &named(text), now)
    }

    #[test]
    fn live_follow_up() {
        let mut room = Room::default();
        let t0 = Instant::now();
        assert_eq!(hear(&mut room, "ana", "Hey Pixel", t0), None);
        // Someone else's line is not the question.
        assert_eq!(hear(&mut room, "ben", "lunch?", t0), None);
        assert_eq!(
            hear(
                &mut room,
                "ana",
                "what time is it?",
                t0 + Duration::from_secs(3)
            )
            .as_deref(),
            Some("Hey Pixel what time is it?")
        );
        // The wait is used up.
        assert_eq!(
            hear(&mut room, "ana", "and then?", t0 + Duration::from_secs(4)),
            None
        );
        assert_eq!(room.pending.len(), 4);
    }

    #[test]
    fn history_restores_waiting() {
        let mut room = Room::default();
        let t0 = Instant::now();
        // Ana's last history line is the bare name; Ben's bare name was
        // followed by another line of his.
        recall(&mut room, "ben", "Hey Pixel", t0);
        recall(&mut room, "ana", "Hey Pixel", t0);
        recall(&mut room, "ben", "never mind", t0);
        // A question in the history leaves nobody waiting.
        recall(&mut room, "cy", "Pixel, what time is it?", t0);
        assert_eq!(room.waiting.len(), 1);
        assert_eq!(
            room.pending,
            [
                "ben: Hey Pixel",
                "ana: Hey Pixel",
                "ben: never mind",
                "cy: Pixel, what time is it?"
            ]
        );

        let later = t0 + Duration::from_secs(2);
        assert_eq!(hear(&mut room, "ben", "what time is it?", later), None);
        assert_eq!(
            hear(&mut room, "ana", "what time is it?", later).as_deref(),
            Some("Hey Pixel what time is it?")
        );
    }

    #[test]
    fn history_wait_runs_from_receipt() {
        let mut room = Room::default();
        let t0 = Instant::now();
        recall(&mut room, "ana", "Hey Pixel", t0);
        let late = t0 + FOLLOW_UP + Duration::from_secs(1);
        assert_eq!(hear(&mut room, "ana", "what time is it?", late), None);
    }

    #[test]
    fn context_is_bounded() {
        let mut room = Room::default();
        for i in 0..MAX_PENDING + 5 {
            room.note(format!("ana: {i}"));
        }
        assert_eq!(room.pending.len(), MAX_PENDING);
        assert_eq!(room.recent.len(), MAX_PENDING);
        assert_eq!(room.pending.front().map(String::as_str), Some("ana: 5"));
    }

    #[test]
    fn prompt_shape() {
        let lines = vec!["ana: hi".to_string(), "ben: Pixel, time?".to_string()];
        assert_eq!(
            reply_prompt(&lines, "ben", "Pixel, time?"),
            "What was said in the room:\nana: hi\nben: Pixel, time?\n\nben said to you: \"Pixel, time?\"\nReply to ben."
        );
    }

    #[test]
    fn room_publications() {
        assert_eq!(
            presence_listening("ana"),
            json!({"kind": "presence", "state": "listening", "to": "ana"})
        );
        assert_eq!(
            presence_thinking("a1-2", "ana", Some(7)),
            json!({"kind": "presence", "state": "thinking", "reply": "a1-2", "to": "ana", "line": 7})
        );
        // A line without a `seq` gives no `line`.
        assert_eq!(
            presence_thinking("a1-2", "ana", None),
            json!({"kind": "presence", "state": "thinking", "reply": "a1-2", "to": "ana"})
        );
        assert_eq!(
            sentence_line("It is noon.", "a1-2"),
            json!({"kind": "text", "text": "It is noon.", "say": "It is noon.", "reply": "a1-2"})
        );
        assert_eq!(
            presence_idle("a1-2"),
            json!({"kind": "presence", "state": "idle", "reply": "a1-2"})
        );
    }
}
