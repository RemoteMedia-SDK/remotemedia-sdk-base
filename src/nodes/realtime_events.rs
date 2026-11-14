//! `RealtimeEventsNode`: a voice agent's turns as the AI SDK's normalized
//! realtime events (openspec: add-ai-sdk-voice-library, design D10, tasks 9.3,
//! 9.4 and the node half of 9.5).
//!
//! The realtime transport (`crates/transports/realtime`) owns the socket,
//! the audio and the clock. This node owns the meaning: which turn, which
//! response and item, which text was heard. It sits between TTS and the
//! output, reads `HostedTurnNode`'s `ui` frames, the VAD events and the TTS
//! audio, and talks to the member's socket through its `realtime` port.
//!
//! # Inputs
//!
//! The session router does not tell a node which connection an input came
//! from, so inputs are told apart by shape:
//!
//! | Input | Recognised as |
//! |---|---|
//! | `RuntimeData::Audio` tagged with a turn (`participant.turn_id`) | TTS audio: forwarded with `rm.turn_id` / `rm.item_id` |
//! | `RuntimeData::Audio` without a turn tag | The member's own audio from VAD or the accumulator: dropped |
//! | Json `{ "kind": "speech_end" }` | The TTS node's end mark of a text chunk: forwarded with the audio |
//! | Json `{ "v": 1, "turn", "chunk" }` | A `ui` frame of `HostedTurnNode` |
//! | Json with `is_speech_start` / `is_speech_end` | A VAD event |
//! | Json `{ "__aux_port__": "realtime", "payload" }` | A client event, or an internal frame of the transport |
//!
//! # Outputs
//!
//! The TTS audio on the default output, and frames for a member's socket as
//! `{ "__aux_port__": "realtime", "payload": <frame> }`, attributed and
//! addressed to that member. The transport reads them from the tap
//! `<node>.out.realtime`. Everything else the node does goes through the
//! session's control: typed text and VAD events are published to the nodes
//! named in the parameters, `interrupt` and `set_client_instructions` are
//! typed-RPC calls on the turn node, client tool traffic is an `event` call
//! on the agent's endpoint, and what a member heard is published as
//! `audio_released`, which is how `HostedTurnNode` settles a turn.
//!
//! # Instances
//!
//! The node is `per_participant`. A turn spoken to several members reaches
//! only the instance of the member whose turn it is, while each listener's
//! client events reach that listener's own instance, so the instances of a
//! session share one state ([`Core`]), keyed by member.

use crate::data::RuntimeData;
use crate::error::Error;
use crate::nodes::schema::{NodeSchema, RuntimeDataType};
use crate::nodes::{
    AsyncNodeWrapper, AsyncStreamingNode, Emitter, InstanceScope, NodeRuntimeContext, StreamingNode,
    StreamingNodeFactory,
};
use crate::transport::data::participant;
use crate::transport::endpoint::CallOptions;
use crate::transport::origin::Origin;
use crate::transport::session_control::{
    aux_port_of, wrap_aux_port, ControlAddress, RpcCallScope, SessionControl,
};
use crate::transport::turn_audio::{keys, AudioReleased, TurnTag};
use remotemedia_traits::rpc::RpcMode;
use remotemedia_traits::runtime_context::NodeRuntimeContextRead;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

/// The node type name in manifests.
pub const REALTIME_EVENTS_NODE: &str = "RealtimeEventsNode";
/// The node's port to and from the realtime transport.
pub const REALTIME_PORT: &str = "realtime";
/// Audio metadata: the item a frame belongs to.
pub const ITEM_ID_KEY: &str = "rm.item_id";
/// `participant.*` field a realtime member is joined with.
const TRANSPORT_KEY: &str = "participant.transport";
const TRANSPORT_NAME: &str = "realtime";

const INTERNAL_JOINED: &str = "remotemedia.internal.joined";
const INTERNAL_RELEASED: &str = "remotemedia.internal.released";
const INTERNAL_FLUSH: &str = "remotemedia.internal.flush";

/// Slack when a released or truncated position is compared with a chunk's
/// end: resampling and rounding to whole milliseconds.
const POSITION_SLACK_MS: f64 = 5.0;
/// Turns remembered after they ended, to answer late frames.
const REMEMBERED_TURNS: usize = 16;

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

/// Whether backend tool activity is shown to the browser.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolActivity {
    /// `custom` events with `rawType: "remotemedia.tool"`.
    #[default]
    Visible,
    /// Nothing names a backend tool.
    Hidden,
}

/// The typed-RPC method each configurable session field is applied with. A
/// field without a method is reported to the browser as ignored.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct ConfigRpc {
    /// Method of `tts_node` that takes the `voice` value.
    pub voice: Option<String>,
    /// Method of `vad_node` that takes the `turnDetection` value.
    #[serde(alias = "turnDetection")]
    pub turn_detection: Option<String>,
    /// Method of `stt_node` that takes the transcription language.
    pub language: Option<String>,
}

fn default_lead_ms() -> u64 {
    500
}
fn default_truncate_grace_ms() -> u64 {
    300
}

/// `RealtimeEventsNode` parameters. None is required.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RealtimeEventsConfig {
    /// The `HostedTurnNode` whose turns these are. Default: the session's
    /// node of that type.
    #[serde(default, alias = "turnNode")]
    pub turn_node: Option<String>,
    /// The agent's endpoint, for client tool traffic (`event` calls).
    /// Default: `endpoint` of the turn node.
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default, alias = "toolActivity")]
    pub tool_activity: ToolActivity,
    /// How far ahead of real time the transport sends audio. Read by the
    /// transport.
    #[serde(default = "default_lead_ms", alias = "leadMs")]
    pub lead_ms: u64,
    /// How long a cut waits for the browser's `conversation-item-truncate`.
    #[serde(default = "default_truncate_grace_ms", alias = "truncateGraceMs")]
    pub truncate_grace_ms: u64,
    /// The rate member audio enters the pipeline at. Read by the transport.
    #[serde(default, alias = "inputRate")]
    pub input_rate: Option<u32>,
    /// The rate of the TTS audio. Informational.
    #[serde(default, alias = "outputRate")]
    pub output_rate: Option<u32>,
    #[serde(default, alias = "vadNode")]
    pub vad_node: Option<String>,
    #[serde(default, alias = "accumulatorNode")]
    pub accumulator_node: Option<String>,
    #[serde(default, alias = "sttNode")]
    pub stt_node: Option<String>,
    #[serde(default, alias = "ttsNode")]
    pub tts_node: Option<String>,
    #[serde(default, alias = "configRpc")]
    pub config_rpc: ConfigRpc,
}

// ---------------------------------------------------------------------------
// What the state machine asks the node to do
// ---------------------------------------------------------------------------

/// One effect of an input. The node carries them out; tests read them.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Action {
    /// A frame for `member`'s socket (or, with an internal type, for its
    /// transport).
    Frame { member: String, frame: Value },
    /// What `member` heard of a turn, for the turn node's settlement.
    Released(AudioReleased),
    /// A typed-RPC call on a node.
    Rpc { node: String, method: String, arg: Value },
    /// Data for a node's input, as `member`'s.
    Publish { node: String, member: String, data: Value },
    /// A fire-and-forget `event` call on the agent's endpoint.
    HostEvent(Value),
    /// Call back ([`Core::on_timer`]) after `ms`.
    Timer { member: String, turn: String, ms: u64 },
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct MemberState {
    /// Connected through the realtime transport: it gets frames.
    realtime: bool,
    /// Text of `conversation-item-create` waiting for `response-create`.
    held_text: Vec<String>,
    /// `instructions` the browser set, when its admission allows them.
    instructions: Option<String>,
    /// The browser turned server turn detection off, and the VAD node took it.
    manual_turns: bool,
}

/// The audio of one response of a turn.
#[derive(Debug, Default)]
struct Item {
    /// Index of the turn's first text chunk that belongs to this response.
    first_chunk: usize,
    /// `(text chunk, milliseconds)` in arrival order, one entry per chunk.
    audio: Vec<(u64, f64)>,
}

impl Item {
    /// How many of the turn's text chunks end at or before `ms` of this
    /// item's audio. `open_last`: the last chunk with audio may still be
    /// growing, so it does not count.
    fn chunks_within(&self, ms: f64, open_last: bool) -> usize {
        let mut cumulative = 0.0;
        let mut chunks = self.first_chunk;
        for (at, (chunk, length)) in self.audio.iter().enumerate() {
            cumulative += length;
            let last = at + 1 == self.audio.len();
            if cumulative > ms + POSITION_SLACK_MS || (last && open_last) {
                break;
            }
            chunks = chunks.max(*chunk as usize + 1);
        }
        chunks
    }

    /// The text chunk whose audio is at `ms`.
    fn chunk_at(&self, ms: f64) -> u64 {
        let mut cumulative = 0.0;
        for (chunk, length) in &self.audio {
            cumulative += length;
            if cumulative > ms + POSITION_SLACK_MS {
                return *chunk;
            }
        }
        self.audio.last().map_or(self.first_chunk as u64, |(chunk, _)| *chunk)
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct Position {
    ms: u64,
    /// The transport has nothing of the item left to send.
    drained: bool,
}

/// A member is being cut off from a turn: barge-in, `response-cancel`, or
/// the browser's own truncation.
#[derive(Debug, Default)]
struct Cut {
    /// `audioEndMs` of the browser's `conversation-item-truncate`.
    truncate_ms: Option<u64>,
    /// The released position the transport reported with its flush.
    flushed_ms: Option<u64>,
    /// The spoken point was published to the turn node.
    published: bool,
}

/// One member hearing one turn.
#[derive(Debug, Default)]
struct Listen {
    /// The response open for the member, from 1.
    open: Option<usize>,
    /// Text chunks sent as `audio-transcript-delta`.
    sent_chunks: usize,
    /// Released position per response.
    positions: HashMap<usize, Position>,
    cut: Option<Cut>,
    /// The response ended; its last events wait for the audio to leave.
    pending_end: bool,
}

#[derive(Debug, Default)]
struct Turn {
    /// Whose turn it is.
    owner: Option<String>,
    /// The text chunks, in order.
    texts: Vec<String>,
    /// One per response of the turn.
    items: Vec<Item>,
    /// A client tool was called: the next response starts at the browser's
    /// `response-create`.
    awaiting: bool,
    /// Client tool calls whose output has not arrived.
    calls: Vec<String>,
    listeners: HashMap<String, Listen>,
}

fn response_id(turn: &str, response: usize) -> String {
    if response <= 1 {
        turn.to_string()
    } else {
        format!("{turn}#{response}")
    }
}

fn item_id(turn: &str, response: usize) -> String {
    format!("{}:assistant", response_id(turn, response))
}

/// `(turn, response)` of an assistant item id.
fn parse_item(item: &str) -> (String, usize) {
    let response = item.strip_suffix(":assistant").unwrap_or(item);
    match response.rsplit_once('#') {
        Some((turn, number)) => match number.parse::<usize>() {
            Ok(number) if number >= 2 => (turn.to_string(), number),
            _ => (response.to_string(), 1),
        },
        None => (response.to_string(), 1),
    }
}

fn vad_flag(value: &Value, key: &str) -> Option<bool> {
    value.get(key).and_then(Value::as_bool).or_else(|| {
        ["payload", "data", "event"].iter().find_map(|nested| {
            value
                .get(nested)
                .and_then(|nested| nested.get(key))
                .and_then(Value::as_bool)
        })
    })
}

/// A TTS node's end mark: `{ "kind": "speech_end" }`.
fn is_end_mark(value: &Value) -> bool {
    value.get("kind").and_then(Value::as_str) == Some("speech_end")
}

/// `Some(true)` for a speech start, `Some(false)` for a speech end.
fn vad_edge(value: &Value) -> Option<bool> {
    if vad_flag(value, "is_speech_start") == Some(true) {
        Some(true)
    } else if vad_flag(value, "is_speech_end") == Some(true) {
        Some(false)
    } else {
        None
    }
}

/// The state of one session's realtime members and turns. Pure: every input
/// returns the [`Action`]s it causes.
#[derive(Debug)]
pub(crate) struct Core {
    config: RealtimeEventsConfig,
    members: HashMap<String, MemberState>,
    turns: HashMap<String, Turn>,
    /// Turn ids, oldest first.
    order: VecDeque<String>,
}

impl Core {
    pub(crate) fn new(config: RealtimeEventsConfig) -> Self {
        Self {
            config,
            members: HashMap::new(),
            turns: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    fn is_realtime(&self, member: &str) -> bool {
        self.members.get(member).is_some_and(|state| state.realtime)
    }

    /// The member connected through the realtime transport.
    pub(crate) fn joined(&mut self, member: &str) {
        self.members.entry(member.to_string()).or_default().realtime = true;
    }

    /// The realtime members among `to`; with nobody named, all of them.
    fn recipients(&self, owner: Option<&str>, to: Option<&[String]>) -> Vec<String> {
        let mut members: Vec<String> = match to {
            Some(to) => to.iter().filter(|member| self.is_realtime(member)).cloned().collect(),
            None => self
                .members
                .iter()
                .filter(|(_, state)| state.realtime)
                .map(|(member, _)| member.clone())
                .collect(),
        };
        // The owner first, then by name: a stable order.
        members.sort_by_key(|member| (Some(member.as_str()) != owner, member.clone()));
        members
    }

    fn turn_mut(&mut self, id: &str) -> &mut Turn {
        if !self.turns.contains_key(id) {
            self.order.push_back(id.to_string());
            while self.order.len() > REMEMBERED_TURNS {
                if let Some(old) = self.order.pop_front() {
                    self.turns.remove(&old);
                }
            }
            self.turns.insert(
                id.to_string(),
                Turn {
                    items: vec![Item::default()],
                    ..Default::default()
                },
            );
        }
        self.turns.get_mut(id).expect("turn")
    }

    // -- audio -------------------------------------------------------------

    /// A piece of TTS audio of `turn_id`. Returns the item id to stamp on it.
    pub(crate) fn on_audio(&mut self, turn_id: &str, chunk: u64, ms: f64) -> String {
        let turn = self.turn_mut(turn_id);
        let response = turn.items.len();
        let item = turn.items.last_mut().expect("a turn has an item");
        match item.audio.last_mut() {
            Some((last, length)) if *last == chunk => *length += ms,
            _ => item.audio.push((chunk, ms)),
        }
        item_id(turn_id, response)
    }

    // -- VAD ---------------------------------------------------------------

    pub(crate) fn on_vad(&mut self, member: &str, start: bool, out: &mut Vec<Action>) {
        if !self.is_realtime(member) {
            return;
        }
        out.push(Action::Frame {
            member: member.to_string(),
            frame: json!({ "type": if start { "speech-started" } else { "speech-stopped" } }),
        });
    }

    // -- ui frames ---------------------------------------------------------

    /// A `ui` frame of the turn node: `owner` is the member it is attributed
    /// to, `to` the members it is addressed to (`None`: everyone).
    pub(crate) fn on_ui(
        &mut self,
        owner: Option<&str>,
        to: Option<&[String]>,
        frame: &Value,
        out: &mut Vec<Action>,
    ) {
        let chunk = &frame["chunk"];
        let Some(kind) = chunk.get("type").and_then(Value::as_str) else {
            return;
        };
        let members = self.recipients(owner, to);
        let Some(turn_id) = frame.get("turn").and_then(Value::as_str).map(str::to_string) else {
            // Outside a turn (`session.send`): only data parts mean anything.
            if kind.starts_with("data-") {
                for member in members {
                    out.push(custom(&member, "remotemedia.data", chunk));
                }
            }
            return;
        };
        match kind {
            "data-transcript" => {
                let speaker = chunk["data"]["member"]["id"].as_str().or(owner).unwrap_or_default();
                if !self.is_realtime(speaker) {
                    return;
                }
                self.turn_mut(&turn_id).owner = Some(speaker.to_string());
                let user_item = format!("{turn_id}:user");
                // Typed text reports nothing: the hook that sent it
                // (`sendTextMessage`) has already added the user message, and
                // an `input-transcription-completed` would make the AI SDK's
                // reducer add a second one with the same text.
                if chunk["data"]["source"] == "text" {
                    return;
                }
                if chunk["data"]["source"] == "speech" {
                    out.push(Action::Frame {
                        member: speaker.to_string(),
                        frame: json!({ "type": "audio-committed", "itemId": user_item }),
                    });
                }
                out.push(Action::Frame {
                    member: speaker.to_string(),
                    frame: json!({
                        "type": "input-transcription-completed",
                        "itemId": user_item,
                        "transcript": chunk["data"]["text"].as_str().unwrap_or_default(),
                    }),
                });
            }
            "start" => {
                if let Some(owner) = owner {
                    let turn = self.turn_mut(&turn_id);
                    turn.owner.get_or_insert_with(|| owner.to_string());
                }
                for member in members {
                    self.open_response(&turn_id, &member, out);
                }
            }
            "text-delta" => {
                if chunk["id"].as_str() != Some(&format!("{turn_id}:text")) {
                    return;
                }
                // Text after a client tool call is the next response, also
                // when the browser never asked for it.
                if self.turns.get(&turn_id).is_some_and(|turn| turn.awaiting) {
                    self.continue_turn(&turn_id, out);
                }
                let text = chunk["delta"].as_str().unwrap_or_default().to_string();
                self.turn_mut(&turn_id).texts.push(text);
                // A member that joined the audience late still gets a response.
                for member in members {
                    let known = self
                        .turns
                        .get(&turn_id)
                        .is_some_and(|turn| turn.listeners.contains_key(&member));
                    if !known {
                        self.open_response(&turn_id, &member, out);
                    }
                }
            }
            "finish" => {
                for member in members {
                    self.end_or_hold(&turn_id, &member, out);
                }
            }
            "data-turn" => match chunk["data"]["status"].as_str() {
                Some("interrupted") => {
                    let spoken = chunk["data"]["spoken"].as_str().unwrap_or_default().to_string();
                    for member in members {
                        self.end_response(&turn_id, &member, Some(&spoken), out);
                    }
                }
                // The utterance was taken back (its speech went on): a
                // response still open ends as cancelled, and the browser is
                // told which items were never a turn.
                Some("retracted") => {
                    for member in members {
                        self.end_response(&turn_id, &member, Some(""), out);
                        out.push(custom(
                            &member,
                            "remotemedia.turn-retracted",
                            &json!({ "turnId": turn_id, "itemId": format!("{turn_id}:user") }),
                        ));
                    }
                }
                _ => {}
            },
            "data-client-tool-call" => self.client_tool_call(&turn_id, owner, chunk, out),
            "error" => {
                for member in members {
                    out.push(custom(&member, "remotemedia.error", chunk));
                }
            }
            kind if kind.starts_with("tool-") => {
                // Backend tools: never function-call events (design D10).
                if self.config.tool_activity == ToolActivity::Visible {
                    for member in members {
                        out.push(custom(&member, "remotemedia.tool", chunk));
                    }
                }
            }
            kind if kind.starts_with("data-") => {
                for member in members {
                    out.push(custom(&member, "remotemedia.data", chunk));
                }
            }
            // `text-start`, `text-end`, `abort`, steps: nothing of their own.
            _ => {}
        }
    }

    /// Open the turn's current response for `member`.
    fn open_response(&mut self, turn_id: &str, member: &str, out: &mut Vec<Action>) {
        // A response of an earlier turn still waiting for its audio ends now.
        let waiting: Vec<String> = self
            .turns
            .iter()
            .filter(|(id, turn)| {
                id.as_str() != turn_id
                    && turn.listeners.get(member).is_some_and(|listen| listen.open.is_some())
            })
            .map(|(id, _)| id.clone())
            .collect();
        for earlier in waiting {
            self.end_response(&earlier, member, None, out);
        }
        let turn = self.turn_mut(turn_id);
        let response = turn.items.len();
        let listen = turn.listeners.entry(member.to_string()).or_default();
        if listen.open == Some(response) {
            return;
        }
        listen.open = Some(response);
        listen.pending_end = false;
        listen.cut = None;
        listen.sent_chunks = listen.sent_chunks.max(turn.items[response - 1].first_chunk);
        out.push(Action::Frame {
            member: member.to_string(),
            frame: json!({ "type": "response-created", "responseId": response_id(turn_id, response) }),
        });
    }

    /// End the member's response as completed, once its audio has left.
    fn end_or_hold(&mut self, turn_id: &str, member: &str, out: &mut Vec<Action>) {
        let Some(turn) = self.turns.get_mut(turn_id) else { return };
        let Some(listen) = turn.listeners.get_mut(member) else { return };
        let Some(response) = listen.open else { return };
        let has_audio = !turn.items[response - 1].audio.is_empty();
        let left = listen.positions.get(&response).is_some_and(|position| position.drained);
        if has_audio && !left {
            listen.pending_end = true;
            return;
        }
        self.end_response(turn_id, member, None, out);
    }

    /// End the member's open response. `interrupted`: the text the turn node
    /// settled as heard by the member, for a cancelled response.
    fn end_response(
        &mut self,
        turn_id: &str,
        member: &str,
        interrupted: Option<&str>,
        out: &mut Vec<Action>,
    ) {
        let Some(turn) = self.turns.get_mut(turn_id) else { return };
        let Some(listen) = turn.listeners.get_mut(member) else { return };
        let Some(response) = listen.open.take() else { return };
        listen.pending_end = false;
        let item = &turn.items[response - 1];
        let first = item.first_chunk.min(turn.texts.len());
        let end = turn
            .items
            .get(response)
            .map_or(turn.texts.len(), |next| next.first_chunk.min(turn.texts.len()));
        let response_id = response_id(turn_id, response);
        let item_id = item_id(turn_id, response);
        let frame = |frame: Value| Action::Frame {
            member: member.to_string(),
            frame,
        };
        let (transcript, status) = match interrupted {
            Some(spoken) => {
                // `spoken` is of the whole turn; this item's part follows
                // the earlier responses' text.
                let before = turn.texts[..first].concat();
                let own = spoken.strip_prefix(before.as_str()).unwrap_or(spoken);
                (own.to_string(), "cancelled")
            }
            None => {
                // What the turn node counts as heard: everything once the
                // audio has all left, else the chunks wholly released.
                let position = listen.positions.get(&response).copied();
                let heard = match position {
                    Some(position) if position.drained => end,
                    Some(position) => item.chunks_within(position.ms as f64, true).clamp(first, end),
                    None => first,
                };
                for chunk in listen.sent_chunks.max(first)..heard {
                    out.push(frame(json!({
                        "type": "audio-transcript-delta",
                        "responseId": response_id, "itemId": item_id,
                        "delta": turn.texts[chunk],
                    })));
                }
                listen.sent_chunks = listen.sent_chunks.max(heard);
                (turn.texts[first..heard].concat(), "completed")
            }
        };
        out.push(frame(json!({ "type": "audio-done", "responseId": response_id, "itemId": item_id })));
        out.push(frame(json!({
            "type": "audio-transcript-done",
            "responseId": response_id, "itemId": item_id, "transcript": transcript,
        })));
        out.push(frame(json!({ "type": "response-done", "responseId": response_id, "status": status })));
    }

    /// The model called a client tool: the browser runs it (design D10,
    /// "Tools"). Only the member whose turn it is gets the call.
    fn client_tool_call(&mut self, turn_id: &str, owner: Option<&str>, chunk: &Value, out: &mut Vec<Action>) {
        let turn = self.turn_mut(turn_id);
        let Some(owner) = turn.owner.clone().or_else(|| owner.map(str::to_string)) else {
            return;
        };
        if !self.is_realtime(&owner) {
            return;
        }
        let data = &chunk["data"];
        let call_id = data["callId"].as_str().or(chunk["id"].as_str()).unwrap_or_default().to_string();
        let name = data["name"].as_str().unwrap_or_default();
        let arguments = match &data["arguments"] {
            Value::String(text) => text.clone(),
            Value::Null => "{}".to_string(),
            other => other.to_string(),
        };
        let turn = self.turn_mut(turn_id);
        let response = turn.items.len();
        turn.calls.push(call_id.clone());
        turn.awaiting = true;
        let response_id = response_id(turn_id, response);
        let call_item = format!("{response_id}:call:{call_id}");
        out.push(Action::Frame {
            member: owner.clone(),
            frame: json!({
                "type": "function-call-arguments-delta",
                "responseId": response_id, "itemId": call_item, "callId": call_id, "delta": arguments,
            }),
        });
        out.push(Action::Frame {
            member: owner.clone(),
            frame: json!({
                "type": "function-call-arguments-done",
                "responseId": response_id, "itemId": call_item, "callId": call_id,
                "name": name, "arguments": arguments,
            }),
        });
        // The response ends here; the hook answers with the tool output and
        // a `response-create`.
        self.end_or_hold(turn_id, &owner, out);
    }

    /// The turn goes on as a new response, for everyone hearing it.
    fn continue_turn(&mut self, turn_id: &str, out: &mut Vec<Action>) {
        let Some(turn) = self.turns.get_mut(turn_id) else { return };
        turn.awaiting = false;
        let first_chunk = turn.texts.len();
        let mut listeners: Vec<String> = turn.listeners.keys().cloned().collect();
        listeners.sort();
        for member in &listeners {
            self.end_response(turn_id, member, None, out);
        }
        let turn = self.turns.get_mut(turn_id).expect("turn");
        turn.items.push(Item {
            first_chunk,
            audio: Vec::new(),
        });
        for member in &listeners {
            self.open_response(turn_id, member, out);
        }
    }

    // -- frames from the transport and the browser ---------------------------

    /// A frame that arrived on the `realtime` port for `member`.
    pub(crate) fn on_client(&mut self, member: &str, frame: &Value, out: &mut Vec<Action>) {
        let Some(kind) = frame.get("type").and_then(Value::as_str) else {
            return;
        };
        // Only the realtime transport writes to this port.
        self.joined(member);
        match kind {
            INTERNAL_JOINED => {}
            INTERNAL_RELEASED => self.on_released(member, frame, out),
            "conversation-item-create" => self.on_item_create(member, &frame["item"], out),
            "response-create" => self.on_response_create(member, out),
            "response-cancel" => {
                if let Some(turn_id) = self.open_turn(member) {
                    self.start_cut(member, &turn_id, true, out);
                }
            }
            "conversation-item-truncate" => self.on_truncate(member, frame, out),
            "session-update" => self.on_session_update(member, &frame["config"], out),
            "input-audio-commit" | "input-audio-clear" => self.on_manual_turn(member, kind, out),
            _ => {}
        }
    }

    /// The turn `member` has an open response of.
    fn open_turn(&self, member: &str) -> Option<String> {
        self.order
            .iter()
            .rev()
            .find(|id| {
                self.turns
                    .get(*id)
                    .and_then(|turn| turn.listeners.get(member))
                    .is_some_and(|listen| listen.open.is_some())
            })
            .cloned()
    }

    fn turn_node(&self) -> Option<String> {
        self.config.turn_node.clone().filter(|node| !node.is_empty())
    }

    /// Begin cutting `member` off from the turn. `ask`: this node asks for
    /// the cut (flush at the transport, `interrupt` at the turn node); not
    /// when the turn node already flushed.
    fn start_cut(&mut self, member: &str, turn_id: &str, ask: bool, out: &mut Vec<Action>) {
        let grace = self.config.truncate_grace_ms;
        let turn_node = self.turn_node();
        let Some(turn) = self.turns.get_mut(turn_id) else { return };
        let Some(listen) = turn.listeners.get_mut(member) else { return };
        let Some(response) = listen.open else { return };
        if listen.cut.is_some() {
            return;
        }
        listen.cut = Some(Cut::default());
        if ask {
            out.push(Action::Frame {
                member: member.to_string(),
                frame: json!({ "type": INTERNAL_FLUSH, "itemId": item_id(turn_id, response) }),
            });
            if let Some(node) = turn_node {
                out.push(Action::Rpc {
                    node,
                    method: "interrupt".into(),
                    arg: json!({ "member": member }),
                });
            }
        }
        // The browser's truncation gets this long to arrive.
        out.push(Action::Timer {
            member: member.to_string(),
            turn: turn_id.to_string(),
            ms: grace,
        });
    }

    /// Publish what `member` heard of the turn it is being cut off from: up
    /// to the browser's truncation point, else everything released.
    fn publish_cut(&mut self, member: &str, turn_id: &str, out: &mut Vec<Action>) {
        let Some(turn) = self.turns.get_mut(turn_id) else { return };
        let Some(listen) = turn.listeners.get_mut(member) else { return };
        let Some(response) = listen.open else { return };
        let released = listen.positions.get(&response).map(|position| position.ms);
        let Some(cut) = listen.cut.as_mut().filter(|cut| !cut.published) else {
            return;
        };
        cut.published = true;
        let released = cut.flushed_ms.or(released);
        let spoken_ms = match (cut.truncate_ms, released) {
            // The browser cannot have played more than it was sent.
            (Some(truncated), Some(released)) => truncated.min(released),
            (Some(truncated), None) => truncated,
            (None, released) => released.unwrap_or(0),
        };
        let chunks = turn.items[response - 1].chunks_within(spoken_ms as f64, false);
        out.push(Action::Released(AudioReleased {
            member: member.to_string(),
            turn_id: turn_id.to_string(),
            released_ms: spoken_ms,
            chunks_released: chunks as u64,
            chunk: chunks.saturating_sub(1) as u64,
            flushed: true,
            drained: false,
            stream_id: None,
        }));
    }

    /// The grace period of a cut ended without a truncation.
    pub(crate) fn on_timer(&mut self, member: &str, turn_id: &str, out: &mut Vec<Action>) {
        self.publish_cut(member, turn_id, out);
    }

    fn on_truncate(&mut self, member: &str, frame: &Value, out: &mut Vec<Action>) {
        let Some(item) = frame.get("itemId").and_then(Value::as_str) else {
            return;
        };
        let (turn_id, response) = parse_item(item);
        let open = self
            .turns
            .get(&turn_id)
            .and_then(|turn| turn.listeners.get(member))
            .and_then(|listen| listen.open);
        if open != Some(response) {
            // The response is over: the whole reply counts as spoken.
            return;
        }
        // The browser stopped playing, whatever the barge-in policy says.
        self.start_cut(member, &turn_id, true, out);
        let end_ms = frame
            .get("audioEndMs")
            .and_then(Value::as_f64)
            .map_or(0, |ms| ms.max(0.0) as u64);
        if let Some(cut) = self
            .turns
            .get_mut(&turn_id)
            .and_then(|turn| turn.listeners.get_mut(member))
            .and_then(|listen| listen.cut.as_mut())
        {
            cut.truncate_ms.get_or_insert(end_ms);
        }
        self.publish_cut(member, &turn_id, out);
    }

    /// The transport's report of what it released of an item.
    fn on_released(&mut self, member: &str, frame: &Value, out: &mut Vec<Action>) {
        let Some(item) = frame.get("itemId").and_then(Value::as_str) else {
            return;
        };
        let (turn_id, response) = parse_item(item);
        let ms = frame.get("audioMs").and_then(Value::as_u64).unwrap_or(0);
        let flushed = frame.get("flushed").and_then(Value::as_bool).unwrap_or(false);
        let drained = frame.get("drained").and_then(Value::as_bool).unwrap_or(false);
        let Some(turn) = self.turns.get_mut(&turn_id) else { return };
        let Some(listen) = turn.listeners.get_mut(member) else { return };
        if listen.open != Some(response) {
            return;
        }
        if listen.cut.as_ref().is_some_and(|cut| cut.published) {
            return;
        }
        if listen.cut.is_some() || flushed {
            // The item's audio was dropped: by this node's flush, or by the
            // turn node's (barge-in under its policy).
            if listen.cut.is_none() {
                self.start_cut(member, &turn_id, false, out);
            }
            let cut = self
                .turns
                .get_mut(&turn_id)
                .and_then(|turn| turn.listeners.get_mut(member))
                .and_then(|listen| listen.cut.as_mut());
            if let (Some(cut), true) = (cut, flushed) {
                cut.flushed_ms.get_or_insert(ms);
                if cut.truncate_ms.is_some() {
                    self.publish_cut(member, &turn_id, out);
                }
            }
            return;
        }
        listen.positions.insert(response, Position { ms, drained });
        let item = &turn.items[response - 1];
        let strict = item.chunks_within(ms as f64, true);
        let current = item.chunk_at(ms as f64);
        // What the turn node counts: with a drained sender, also the chunk
        // whose audio so far has all been written.
        let counted = if drained && !item.audio.is_empty() {
            strict.max(current as usize + 1)
        } else {
            strict
        };
        let (response_id, item_id) = (response_id(&turn_id, response), item_id(&turn_id, response));
        for chunk in listen.sent_chunks..counted.min(turn.texts.len()) {
            out.push(Action::Frame {
                member: member.to_string(),
                frame: json!({
                    "type": "audio-transcript-delta",
                    "responseId": response_id, "itemId": item_id, "delta": turn.texts[chunk],
                }),
            });
        }
        listen.sent_chunks = listen.sent_chunks.max(counted.min(turn.texts.len()));
        let pending_end = listen.pending_end;
        out.push(Action::Released(AudioReleased {
            member: member.to_string(),
            turn_id: turn_id.clone(),
            released_ms: ms,
            chunks_released: strict as u64,
            chunk: current,
            flushed: false,
            drained,
            stream_id: None,
        }));
        if pending_end && drained {
            self.end_response(&turn_id, member, None, out);
        }
    }

    fn on_item_create(&mut self, member: &str, item: &Value, out: &mut Vec<Action>) {
        match item.get("type").and_then(Value::as_str) {
            Some("text-message") => {
                let text = item.get("text").and_then(Value::as_str).unwrap_or_default().trim();
                if !text.is_empty() {
                    self.members
                        .entry(member.to_string())
                        .or_default()
                        .held_text
                        .push(text.to_string());
                }
            }
            Some("function-call-output") => {
                let Some(call_id) = item.get("callId").and_then(Value::as_str) else {
                    return;
                };
                let turn = self.order.iter().rev().find(|id| {
                    self.turns
                        .get(*id)
                        .is_some_and(|turn| turn.calls.iter().any(|call| call == call_id))
                });
                let Some(turn_id) = turn.cloned() else { return };
                if let Some(turn) = self.turns.get_mut(&turn_id) {
                    turn.calls.retain(|call| call != call_id);
                }
                out.push(Action::HostEvent(json!({
                    "type": "client_tool_output",
                    "turn_id": turn_id,
                    "call_id": call_id,
                    "output": item.get("output").cloned().unwrap_or(Value::Null),
                })));
            }
            _ => {}
        }
    }

    fn on_response_create(&mut self, member: &str, out: &mut Vec<Action>) {
        // Typed text: `sendTextMessage` sends the item, then this.
        let held = self
            .members
            .get_mut(member)
            .map(|state| std::mem::take(&mut state.held_text))
            .unwrap_or_default();
        if !held.is_empty() {
            let Some(node) = self.turn_node() else { return };
            let mut data = json!({ "v": 1, "type": "text", "text": held.join("\n") });
            if let Some(instructions) = self.members.get(member).and_then(|state| state.instructions.clone()) {
                data["client_instructions"] = instructions.into();
            }
            out.push(Action::Publish {
                node,
                member: member.to_string(),
                data,
            });
            return;
        }
        // The continuation of a turn that called a client tool.
        let awaiting = self.order.iter().rev().find(|id| {
            self.turns
                .get(*id)
                .is_some_and(|turn| turn.awaiting && turn.owner.as_deref() == Some(member))
        });
        if let Some(turn_id) = awaiting.cloned() {
            out.push(Action::HostEvent(json!({ "type": "response_create", "turn_id": turn_id })));
            self.continue_turn(&turn_id, out);
        }
        // Otherwise the pipeline's own turn detection starts turns.
    }

    /// The fields of a `session-update` the member's admission let through
    /// (design D10, "Session configuration policy").
    fn on_session_update(&mut self, member: &str, config: &Value, out: &mut Vec<Action>) {
        let Some(config) = config.as_object() else { return };
        let mut ignored: Vec<&str> = Vec::new();
        let rpc = self.config.config_rpc.clone();
        let apply = |node: &Option<String>, method: &Option<String>, value: &Value, out: &mut Vec<Action>| {
            match (node, method) {
                (Some(node), Some(method)) => {
                    out.push(Action::Rpc {
                        node: node.clone(),
                        method: method.clone(),
                        arg: value.clone(),
                    });
                    true
                }
                _ => false,
            }
        };
        for (key, value) in config {
            match key.as_str() {
                "voice" => {
                    if !apply(&self.config.tts_node, &rpc.voice, value, out) {
                        ignored.push("voice");
                    }
                }
                "turnDetection" => {
                    if apply(&self.config.vad_node, &rpc.turn_detection, value, out) {
                        let manual = value.is_null()
                            || value == &Value::Bool(false)
                            || value.get("type").and_then(Value::as_str) == Some("disabled");
                        let state = self.members.entry(member.to_string()).or_default();
                        let was = std::mem::replace(&mut state.manual_turns, manual);
                        if manual && !was {
                            // The accumulator collects until the browser commits.
                            if let Some(node) = self.config.accumulator_node.clone() {
                                out.push(Action::Publish {
                                    node,
                                    member: member.to_string(),
                                    data: json!({ "is_speech_start": true, "is_speech_end": false }),
                                });
                            }
                        }
                    } else {
                        ignored.push("turnDetection");
                    }
                }
                "inputAudioTranscription" => {
                    let language = value.get("language").cloned().unwrap_or(Value::Null);
                    if !apply(&self.config.stt_node, &rpc.language, &language, out) {
                        ignored.push("inputAudioTranscription.language");
                    }
                }
                "instructions" => {
                    let instructions = value.as_str().filter(|text| !text.trim().is_empty());
                    self.members.entry(member.to_string()).or_default().instructions =
                        instructions.map(str::to_string);
                    // The turn node passes them on with the member's turns,
                    // typed or spoken, as `client_instructions`.
                    if let Some(node) = self.turn_node() {
                        out.push(Action::Rpc {
                            node,
                            method: "set_client_instructions".into(),
                            arg: json!({ "member": member, "instructions": instructions }),
                        });
                    }
                }
                // Client tool definitions: the transport checked them against
                // the admission; the host already has them.
                _ => {}
            }
        }
        if !ignored.is_empty() {
            out.push(Action::Frame {
                member: member.to_string(),
                frame: json!({
                    "type": "custom",
                    "rawType": "remotemedia.config-ignored",
                    "raw": {
                        "fields": ignored,
                        "reason": "the pipeline's node does not accept this change at runtime",
                    },
                }),
            });
        }
    }

    /// `input-audio-commit` / `input-audio-clear`: the browser ends or drops
    /// an utterance itself. Only when it turned server turn detection off.
    fn on_manual_turn(&mut self, member: &str, kind: &str, out: &mut Vec<Action>) {
        if !self.members.get(member).is_some_and(|state| state.manual_turns) {
            return;
        }
        let Some(node) = self.config.accumulator_node.clone() else { return };
        let mut event = |start: bool| {
            out.push(Action::Publish {
                node: node.clone(),
                member: member.to_string(),
                data: json!({ "is_speech_start": start, "is_speech_end": !start }),
            });
        };
        if kind == "input-audio-commit" {
            // The utterance is closed and sent on.
            event(false);
        }
        // A speech start drops what was collected and collects again.
        event(true);
    }
}

fn custom(member: &str, raw_type: &str, raw: &Value) -> Action {
    Action::Frame {
        member: member.to_string(),
        frame: json!({ "type": "custom", "rawType": raw_type, "raw": raw }),
    }
}

// ---------------------------------------------------------------------------
// The node
// ---------------------------------------------------------------------------

type SharedCore = Arc<parking_lot::Mutex<Core>>;

/// The state the instances of one session's node share, by session and node.
fn shared_core(session: &str, node: &str, config: &RealtimeEventsConfig) -> SharedCore {
    static CORES: OnceLock<parking_lot::Mutex<HashMap<(String, String), Weak<parking_lot::Mutex<Core>>>>> =
        OnceLock::new();
    let mut cores = CORES.get_or_init(Default::default).lock();
    cores.retain(|_, core| core.strong_count() > 0);
    let key = (session.to_string(), node.to_string());
    if let Some(core) = cores.get(&key).and_then(Weak::upgrade) {
        return core;
    }
    let core = Arc::new(parking_lot::Mutex::new(Core::new(config.clone())));
    cores.insert(key, Arc::downgrade(&core));
    core
}

/// The node id without the suffix of a per-participant instance.
fn base_node_id(node_id: &str) -> &str {
    match node_id.rsplit_once("__p") {
        Some((base, number)) if !number.is_empty() && number.bytes().all(|b| b.is_ascii_digit()) => base,
        _ => node_id,
    }
}

/// See the module docs.
pub struct RealtimeEventsNode {
    node_id: String,
    config: RealtimeEventsConfig,
    core: OnceLock<SharedCore>,
    /// `participant.*` fields of the members seen, for attribution.
    fields: parking_lot::Mutex<HashMap<String, HashMap<String, String>>>,
    corr: std::sync::atomic::AtomicU64,
}

impl RealtimeEventsNode {
    pub fn new(node_id: impl Into<String>, config: RealtimeEventsConfig) -> Self {
        Self {
            node_id: node_id.into(),
            config,
            core: OnceLock::new(),
            fields: parking_lot::Mutex::new(HashMap::new()),
            corr: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// The session's shared state, with the parameters the session's
    /// descriptor completes (the turn node and its endpoint).
    fn core(&self, ctx: &dyn NodeRuntimeContextRead) -> SharedCore {
        self.core
            .get_or_init(|| {
                let mut config = self.config.clone();
                let descriptor = NodeRuntimeContext::of(ctx).and_then(|ctx| ctx.control.descriptor());
                if let Some(descriptor) = descriptor {
                    let turn = descriptor.nodes.iter().find(|node| match &config.turn_node {
                        Some(id) => &node.id == id,
                        None => node.node_type == crate::nodes::hosted_turn::HOSTED_TURN_NODE,
                    });
                    if let Some(turn) = turn {
                        config.turn_node.get_or_insert_with(|| turn.id.clone());
                        if config.endpoint.is_none() {
                            config.endpoint = turn
                                .params
                                .get("endpoint")
                                .and_then(Value::as_str)
                                .map(str::to_string);
                        }
                    }
                }
                shared_core(ctx.session_id(), base_node_id(&self.node_id), &config)
            })
            .clone()
    }

    fn endpoint(&self, core: &SharedCore) -> Option<String> {
        core.lock().config.endpoint.clone().filter(|endpoint| !endpoint.is_empty())
    }

    fn note(&self, origin: &Origin) {
        let mut fields = origin.fields().clone();
        for key in [participant::TO, keys::TURN_ID, keys::CHUNK] {
            fields.remove(key);
        }
        self.fields.lock().insert(origin.id().to_string(), fields);
    }

    fn member_fields(&self, member: &str) -> HashMap<String, String> {
        let mut fields = self.fields.lock().get(member).cloned().unwrap_or_default();
        fields.insert(participant::ID.to_string(), member.to_string());
        fields
    }

    /// Carry out what the state machine asked for.
    async fn perform(
        &self,
        actions: Vec<Action>,
        core: &SharedCore,
        control: Option<&Arc<SessionControl>>,
        emitter: &Emitter,
    ) {
        for action in actions {
            match action {
                Action::Frame { member, frame } => {
                    // Attributed and addressed to the member: its connection
                    // takes it, and no other member's output does.
                    let mut fields = self.member_fields(&member);
                    fields.insert(participant::TO.to_string(), member.clone());
                    let origin = Origin::from_metadata(&fields).map(Arc::new);
                    let data = wrap_aux_port(REALTIME_PORT, RuntimeData::Json(frame));
                    if let Err(error) = emitter.emit_as(data, origin).await {
                        tracing::debug!(node = %self.node_id, %member, %error, "realtime frame not emitted");
                    }
                }
                other => {
                    let Some(control) = control else { continue };
                    self.perform_control(other, core, control).await;
                }
            }
        }
    }

    async fn perform_control(&self, action: Action, core: &SharedCore, control: &Arc<SessionControl>) {
        match action {
            Action::Frame { .. } => {}
            Action::Released(report) => control.publish_audio_released(report),
            Action::Rpc { node, method, arg } => {
                let corr = self.corr.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let address = ControlAddress::node_in(&node).with_port(&method);
                let call = json!({
                    "corr_id": format!("{}-{corr}", self.node_id), "args": [arg], "kwargs": {},
                });
                if let Err(error) = control.publish(&address, RuntimeData::Json(call)).await {
                    tracing::warn!(node = %self.node_id, target = %node, %method, %error, "RPC not delivered");
                }
            }
            Action::Publish { node, member, data } => {
                let sent = control
                    .publish_scoped_with_metadata(
                        &ControlAddress::node_in(&node),
                        RuntimeData::Json(data),
                        &RpcCallScope::default(),
                        self.member_fields(&member),
                    )
                    .await;
                if let Err(error) = sent {
                    tracing::warn!(node = %self.node_id, target = %node, %member, %error, "input not delivered");
                }
            }
            Action::HostEvent(event) => {
                let Some(endpoint) = self.endpoint(core) else {
                    tracing::warn!(
                        node = %self.node_id,
                        "no endpoint is known (params.endpoint, or the turn node's): client tool event dropped"
                    );
                    return;
                };
                let corr = self.corr.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let opts = CallOptions::default().mode(RpcMode::FireAndForget).deadline_ms(5000);
                let mut request = remotemedia_traits::call::CallRequestEnvelope::new(
                    endpoint,
                    "event",
                    format!("{}-event-{corr}", self.node_id),
                );
                request.mode = opts.mode;
                request.args = vec![event];
                request.deadline_ms = opts.deadline_ms;
                let mut stream = control.call_endpoint(base_node_id(&self.node_id), request);
                // Dropping the stream would cancel the call.
                tokio::spawn(async move { while stream.recv().await.is_some() {} });
            }
            Action::Timer { member, turn, ms } => {
                let core = core.clone();
                let control = control.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    let mut actions = Vec::new();
                    core.lock().on_timer(&member, &turn, &mut actions);
                    for action in actions {
                        if let Action::Released(report) = action {
                            control.publish_audio_released(report);
                        }
                    }
                });
            }
        }
    }
}

/// The members an output is addressed to; `None`: everyone.
fn addressed_to(origin: Option<&Origin>) -> Option<Vec<String>> {
    let to = origin?.fields().get(participant::TO)?;
    Some(
        to.split(',')
            .map(str::trim)
            .filter(|member| !member.is_empty())
            .map(str::to_string)
            .collect(),
    )
}

#[async_trait::async_trait]
impl AsyncStreamingNode for RealtimeEventsNode {
    fn node_type(&self) -> &str {
        REALTIME_EVENTS_NODE
    }

    fn instance_scope(&self) -> Option<InstanceScope> {
        Some(InstanceScope::PerParticipant)
    }

    async fn process(&self, _data: RuntimeData) -> Result<RuntimeData, Error> {
        Err(Error::Execution(
            "RealtimeEventsNode runs in a streaming session".into(),
        ))
    }

    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let origin = ctx.origin();
        let core = self.core(ctx);
        let control = NodeRuntimeContext::of(ctx).map(|ctx| ctx.control.clone());
        if let Some(origin) = origin.as_deref() {
            self.note(origin);
            if origin.fields().get(TRANSPORT_KEY).map(String::as_str) == Some(TRANSPORT_NAME) {
                core.lock().joined(origin.id());
            }
        }
        let owner = origin.as_deref().map(|origin| origin.id().to_string());

        let mut actions = Vec::new();
        match data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                stream_id,
                timestamp_us,
                arrival_ts_us,
                metadata,
            } => {
                let mut tags: HashMap<String, String> = origin
                    .as_deref()
                    .map(|origin| origin.fields().clone())
                    .unwrap_or_default();
                if let Some(own) = metadata.as_ref().and_then(Value::as_object) {
                    for (key, value) in own {
                        if let Some(value) = value.as_str() {
                            tags.insert(key.clone(), value.to_string());
                        }
                    }
                }
                // Audio without a turn is the member's own, forwarded by the
                // VAD node or the accumulator: it is not output.
                let Some(tag) = TurnTag::from_metadata(&tags) else {
                    return Ok(0);
                };
                let frames = samples.len() as f64 / f64::from(channels.max(1));
                let ms = frames * 1000.0 / f64::from(sample_rate.max(1));
                let item = core.lock().on_audio(&tag.turn_id, tag.chunk, ms);
                let mut stamped = match metadata {
                    Some(Value::Object(map)) => map,
                    _ => Map::new(),
                };
                stamped.insert(keys::TURN_ID_ALIAS.to_string(), tag.turn_id.to_string().into());
                stamped.insert(ITEM_ID_KEY.to_string(), item.into());
                callback(RuntimeData::Audio {
                    samples,
                    sample_rate,
                    channels,
                    stream_id,
                    timestamp_us,
                    arrival_ts_us,
                    metadata: Some(Value::Object(stamped)),
                })?;
                return Ok(1);
            }
            RuntimeData::Json(value) if is_end_mark(&value) => {
                // The TTS node's mark that a text chunk's audio is complete
                // belongs to the audio stream: a WebRTC or telephony sender
                // reads it to close the chunk and send its last partial
                // frame. Without it a turn ends only once the sender has
                // stayed drained (`drain_quiet_ms` of the turn node).
                callback(RuntimeData::Json(value))?;
                return Ok(1);
            }
            RuntimeData::Json(value) => {
                if let Some(port) = aux_port_of(&RuntimeData::Json(value.clone())) {
                    if port == REALTIME_PORT {
                        let member = owner.clone().or_else(|| {
                            value.get(participant::ID).and_then(Value::as_str).map(str::to_string)
                        });
                        if let Some(member) = member {
                            let frame = value.get("payload").unwrap_or(&Value::Null);
                            core.lock().on_client(&member, frame, &mut actions);
                        }
                    }
                } else if value.get("v").and_then(Value::as_u64) == Some(1) && value.get("chunk").is_some() {
                    let to = addressed_to(origin.as_deref());
                    core.lock().on_ui(owner.as_deref(), to.as_deref(), &value, &mut actions);
                } else if let (Some(start), Some(member)) = (vad_edge(&value), owner.as_deref()) {
                    core.lock().on_vad(member, start, &mut actions);
                }
            }
            _ => {}
        }
        if !actions.is_empty() {
            let emitter = Emitter::for_call(ctx);
            self.perform(actions, &core, control.as_ref(), &emitter).await;
        }
        Ok(0)
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Factory for [`RealtimeEventsNode`].
pub struct RealtimeEventsNodeFactory;

impl StreamingNodeFactory for RealtimeEventsNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let params = if params.is_null() { json!({}) } else { params.clone() };
        let config: RealtimeEventsConfig = serde_json::from_value(params).map_err(|error| {
            Error::Execution(format!("{REALTIME_EVENTS_NODE} '{node_id}': invalid params: {error}"))
        })?;
        Ok(Box::new(AsyncNodeWrapper(Arc::new(RealtimeEventsNode::new(
            node_id, config,
        )))))
    }

    fn node_type(&self) -> &str {
        REALTIME_EVENTS_NODE
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }

    fn schema(&self) -> Option<NodeSchema> {
        Some(
            NodeSchema::new(REALTIME_EVENTS_NODE)
                .description(
                    "Reports a voice agent's turns as the AI SDK's normalized realtime events \
                     for members connected through the realtime transport, and forwards TTS \
                     audio stamped with its turn and item. Inputs, told apart by shape: TTS \
                     audio, `HostedTurnNode` `ui` frames, VAD events, and client events on the \
                     `realtime` port. One instance per participant.",
                )
                .category("conversation")
                .accepts([RuntimeDataType::Audio, RuntimeDataType::Json])
                .produces([RuntimeDataType::Audio, RuntimeDataType::Json])
                .config_schema_from::<RealtimeEventsConfig>(),
        )
    }
}

#[cfg(test)]
#[path = "realtime_events_tests.rs"]
mod tests;
