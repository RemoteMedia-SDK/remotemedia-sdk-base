//! `RealtimeEventsNode`: a voice agent's turns as the AI SDK's normalized
//! realtime events (openspec: add-ai-sdk-voice-library, designs D10 and D13).
//!
//! A member whose browser runs the AI SDK realtime hook connects over WebRTC
//! like any other member. Its audio is on its media tracks; its events are
//! JSON frames `{ "v": 1, "rt": <normalized v4 event> }` on its data channel,
//! in both directions. This node produces and consumes those frames. It
//! carries no audio: it sits on the event path only, and its output is the
//! member's data channel.
//!
//! # Inputs
//!
//! The session router does not tell a node which connection an input came
//! from, so inputs are told apart by shape:
//!
//! | Input | Recognised as |
//! |---|---|
//! | Json `{ "v": 1, "rt": <event> }` | A client event the member's browser sent on its data channel |
//! | Json `{ "v": 1, "turn", "chunk" }` | A `ui` frame of `HostedTurnNode` |
//! | Json with `is_speech_start` / `is_speech_end` | A VAD event |
//! | Anything else (the member's audio, typed-text frames) | Not this node's: dropped |
//!
//! # Outputs
//!
//! Server events as Json `{ "v": 1, "rt": <event> }`, attributed and
//! addressed (`participant.to`) to the member they are for. Everything else
//! the node does goes through the session's control: `interrupt` and
//! `set_client_instructions` are typed-RPC calls on the turn node, client
//! tool traffic is an `event` call on the agent's endpoint, and session
//! settings are typed-RPC calls on the nodes named in the parameters.
//!
//! # What the node reads from elsewhere
//!
//! - **The member's admission** (`participant.metadata.rm.realtime`, the JSON
//!   string `{ "configurable", "client_tools", "transport" }` the backend put
//!   in the admission metadata): which session fields the browser may set and
//!   which client tools exist. A browser cannot change it.
//! - **The WebRTC sender's `audio_released` reports** (`__media__`): the
//!   transcript deltas of a response follow what was released to the member's
//!   track. The spoken text of an interrupted response is the turn node's
//!   settlement, which comes from the same reports. A
//!   `conversation-item-truncate` from the browser is accepted and changes
//!   nothing: the browser does not play the audio, the track does.
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
use crate::transport::session_control::{ControlAddress, RpcCallScope, SessionControl};
use crate::transport::turn_audio::{keys, AudioReleased, MediaEvent, TurnTag};
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

/// Key of a data-channel frame's realtime event: `{ "v": 1, "rt": <event> }`.
pub const RT_FRAME_KEY: &str = "rt";
/// `participant.*` field that carries a member's realtime admission: the
/// JSON string `{ "configurable": [...], "client_tools": [...], "transport"? }`
/// the backend put in the admission metadata under `rm.realtime`.
pub const ADMISSION_FIELD: &str = "participant.metadata.rm.realtime";
/// `transport` of an admission for the hook over WebRTC.
pub const ADMISSION_TRANSPORT_WEBRTC: &str = "webrtc";

/// The realtime event of a data-channel frame `{ "v": 1, "rt": <event> }`.
pub fn rt_frame_event(value: &Value) -> Option<&Value> {
    if value.get("v").and_then(Value::as_u64) != Some(1) {
        return None;
    }
    value.get(RT_FRAME_KEY).filter(|event| event.is_object())
}

/// Whether a Json output is a D6 UI frame: `{ "v": 1, "turn", "chunk" }`.
pub fn is_ui_frame(value: &Value) -> bool {
    value.get("v").and_then(Value::as_u64) == Some(1) && value.get("chunk").is_some_and(Value::is_object)
}

/// Whether an admission (the value of `rm.realtime`) is for the hook over
/// WebRTC: such a member receives `rt` frames and no D6 UI frames.
pub fn admission_is_webrtc(admission: &str) -> bool {
    serde_json::from_str::<Value>(admission)
        .ok()
        .and_then(|value| value.get("transport").and_then(Value::as_str).map(str::to_string))
        .as_deref()
        == Some(ADMISSION_TRANSPORT_WEBRTC)
}

/// Whether an admission (the value of `rm.realtime`) lets the member's
/// browser set `field` of the session (`voice`, `turnDetection`, `language`,
/// `instructions`).
pub fn admission_allows(admission: &str, field: &str) -> bool {
    serde_json::from_str::<Value>(admission).ok().is_some_and(|value| {
        value
            .get("configurable")
            .and_then(Value::as_array)
            .is_some_and(|fields| fields.iter().any(|allowed| allowed == field))
    })
}

/// What the backend allowed a member's browser to configure (design D10,
/// "Session configuration policy").
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct Admission {
    /// `voice`, `turnDetection`, `language`, `instructions`.
    configurable: Vec<String>,
    /// The agent's client tool definitions, by name.
    client_tools: HashMap<String, Value>,
    /// Admitted for the hook over WebRTC.
    webrtc: bool,
}

impl Admission {
    /// The admission in a member's `participant.*` fields. A member without
    /// one, or with one that cannot be read, may configure nothing.
    pub(crate) fn from_fields(fields: &HashMap<String, String>) -> Self {
        let Some(text) = fields.get(ADMISSION_FIELD) else {
            return Self::default();
        };
        let Ok(parsed) = serde_json::from_str::<Value>(text) else {
            tracing::warn!("unreadable rm.realtime admission");
            return Self::default();
        };
        Self {
            configurable: parsed
                .get("configurable")
                .and_then(Value::as_array)
                .map(|fields| fields.iter().filter_map(Value::as_str).map(str::to_string).collect())
                .unwrap_or_default(),
            client_tools: parsed
                .get("client_tools")
                .and_then(Value::as_array)
                .map(|tools| {
                    tools
                        .iter()
                        .filter_map(|tool| Some((tool.get("name")?.as_str()?.to_string(), tool.clone())))
                        .collect()
                })
                .unwrap_or_default(),
            webrtc: parsed.get("transport").and_then(Value::as_str) == Some(ADMISSION_TRANSPORT_WEBRTC),
        }
    }

    fn allows(&self, field: &str) -> bool {
        self.configurable.iter().any(|allowed| allowed == field)
    }

    /// Split a `session-update`'s `config` into the fields the admission lets
    /// through, the ignored fields and the ignored tools, both sorted. Audio
    /// formats and modalities select nothing on media tracks: always ignored.
    fn filter(&self, config: &Value) -> (Map<String, Value>, Vec<String>, Vec<String>) {
        let mut forward = Map::new();
        let mut ignored: Vec<String> = Vec::new();
        let mut ignored_tools: Vec<String> = Vec::new();
        for (key, value) in config.as_object().into_iter().flatten() {
            match key.as_str() {
                "voice" | "turnDetection" | "instructions" if self.allows(key) => {
                    forward.insert(key.clone(), value.clone());
                }
                "inputAudioTranscription" if value.is_object() => {
                    for (field, value) in value.as_object().into_iter().flatten() {
                        if field == "language"
                            && (self.allows("language") || self.allows("inputAudioTranscription.language"))
                        {
                            forward.insert(key.clone(), json!({ "language": value }));
                        } else {
                            ignored.push(format!("{key}.{field}"));
                        }
                    }
                }
                "tools" if value.is_array() => {
                    for tool in value.as_array().into_iter().flatten() {
                        let name = tool.get("name").and_then(Value::as_str).unwrap_or("");
                        // The schema must be the admitted one; the host
                        // already has the admitted definition.
                        let admitted = self
                            .client_tools
                            .get(name)
                            .is_some_and(|admitted| admitted.get("parameters") == tool.get("parameters"));
                        if !admitted {
                            ignored.push(format!("tools.{name}"));
                            ignored_tools.push(name.to_string());
                        }
                    }
                }
                _ => ignored.push(key.clone()),
            }
        }
        ignored.sort();
        ignored.dedup();
        ignored_tools.sort();
        (forward, ignored, ignored_tools)
    }
}

/// A response whose turn has settled ends when its audio has left the
/// member's track. This is how long the track's sender may report no
/// progress before the response ends anyway (a member that hears nothing
/// reports nothing).
const SETTLED_WAIT_MS: u64 = 1500;
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

/// `RealtimeEventsNode` parameters. None is required. Parameters this node
/// does not know are ignored, so a manifest written for an earlier version
/// (`lead_ms`, `truncate_grace_ms`, `input_rate`, `output_rate`) still loads.
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
    /// A server event for `member`'s data channel.
    Frame { member: String, frame: Value },
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
    /// A member of the realtime hook: it gets frames.
    realtime: bool,
    /// Text of `conversation-item-create` waiting for `response-create`.
    held_text: Vec<String>,
    /// `instructions` the browser set, when its admission allows them.
    instructions: Option<String>,
    /// The browser turned server turn detection off, and the VAD node took it.
    manual_turns: bool,
    /// What the backend allowed the member to configure.
    admission: Admission,
    /// `session-created` was sent on this connection.
    created: bool,
    /// `remotemedia.config-ignored` was sent on this connection.
    ignored_reported: bool,
}

/// One member hearing one turn.
#[derive(Debug, Default)]
struct Listen {
    /// The response open for the member, from 1.
    open: Option<usize>,
    /// Text chunks sent as `audio-transcript-delta`.
    sent_chunks: usize,
    /// The response ended; its last events wait for the audio to leave.
    pending_end: bool,
    /// The turn's text chunks the member's WebRTC sender has released, from
    /// the `audio_released` reports.
    released: Option<usize>,
    /// Milliseconds of the turn the sender has released.
    released_ms: u64,
    /// The turn node ended the turn, so all its text counts as spoken; the
    /// response waits for the member's own audio.
    settled: bool,
    /// `released_ms` when the settled response was last checked for progress.
    watched_ms: Option<u64>,
}

#[derive(Debug, Default)]
struct Turn {
    /// Whose turn it is.
    owner: Option<String>,
    /// The text chunks, in order.
    texts: Vec<String>,
    /// Per response of the turn: the index of its first text chunk.
    responses: Vec<usize>,
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
    /// The session id, for `session-created`.
    session: String,
}

impl Core {
    pub(crate) fn new(config: RealtimeEventsConfig) -> Self {
        Self {
            config,
            members: HashMap::new(),
            turns: HashMap::new(),
            order: VecDeque::new(),
            session: String::new(),
        }
    }

    /// Whether `member` is a member of the realtime hook.
    pub(crate) fn is_realtime(&self, member: &str) -> bool {
        self.members.get(member).is_some_and(|state| state.realtime)
    }

    /// A connection of `member` reached the node: what belongs to the
    /// connection starts over. With an admission for the hook over WebRTC
    /// the member is a realtime member from here on; without one it becomes
    /// one at its first `rt` frame.
    pub(crate) fn connected(&mut self, member: &str, admission: Admission) {
        let state = self.members.entry(member.to_string()).or_default();
        state.created = false;
        state.ignored_reported = false;
        state.held_text.clear();
        if admission.webrtc {
            state.realtime = true;
        }
        state.admission = admission;
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
                    responses: vec![0],
                    ..Default::default()
                },
            );
        }
        self.turns.get_mut(id).expect("turn")
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
                    self.end_or_hold(&turn_id, &member, true, out);
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
        let response = turn.responses.len();
        let first = turn.responses[response - 1];
        let listen = turn.listeners.entry(member.to_string()).or_default();
        if listen.open == Some(response) {
            return;
        }
        listen.open = Some(response);
        listen.pending_end = false;
        listen.settled = false;
        listen.watched_ms = None;
        listen.sent_chunks = listen.sent_chunks.max(first);
        out.push(Action::Frame {
            member: member.to_string(),
            frame: json!({ "type": "response-created", "responseId": response_id(turn_id, response) }),
        });
    }

    /// End the member's response as completed, once its audio has left the
    /// member's track. `settled`: the turn node ended the turn.
    fn end_or_hold(&mut self, turn_id: &str, member: &str, settled: bool, out: &mut Vec<Action>) {
        let Some(turn) = self.turns.get_mut(turn_id) else { return };
        let end = turn.texts.len();
        let Some(listen) = turn.listeners.get_mut(member) else { return };
        let Some(response) = listen.open else { return };
        let first = turn.responses[response - 1].min(end);
        listen.settled |= settled;
        if end > first && listen.released.unwrap_or(0) < end {
            // Text of this response is still being spoken to the member. The
            // turn node settles once the audio was released (on another
            // runner it has the reports from a `MemberMediaRelayNode`), and
            // the last report is on its way here; a turn node that gets no
            // reports settles at once, while this node, which is with the
            // member, follows them.
            listen.pending_end = true;
            if settled {
                listen.watched_ms = Some(listen.released_ms);
                out.push(Action::Timer {
                    member: member.to_string(),
                    turn: turn_id.to_string(),
                    ms: SETTLED_WAIT_MS,
                });
            }
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
        let first = turn.responses[response - 1].min(turn.texts.len());
        let end = turn
            .responses
            .get(response)
            .map_or(turn.texts.len(), |next| (*next).min(turn.texts.len()));
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
                if !own.is_empty() && listen.sent_chunks <= first {
                    // The hook finalizes only an item it has a text part
                    // for, and a part starts with a delta.
                    out.push(frame(json!({
                        "type": "audio-transcript-delta",
                        "responseId": response_id, "itemId": item_id, "delta": own,
                    })));
                }
                (own.to_string(), "cancelled")
            }
            None => {
                let heard = if listen.settled {
                    // The turn node counted everything as spoken.
                    end
                } else {
                    // What the member's WebRTC sender reported.
                    listen.released.unwrap_or(first).clamp(first, end)
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
        let response = turn.responses.len();
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
        // The response ends here; the browser answers with the tool output
        // and a `response-create`.
        self.end_or_hold(turn_id, &owner, false, out);
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
        turn.responses.push(first_chunk);
        for member in &listeners {
            self.open_response(turn_id, member, out);
        }
    }

    // -- events from the browser ---------------------------------------------

    /// A realtime event `member`'s browser sent on its data channel as
    /// `{ "v": 1, "rt": <event> }`.
    pub(crate) fn on_client(&mut self, member: &str, frame: &Value, out: &mut Vec<Action>) {
        let Some(kind) = frame.get("type").and_then(Value::as_str) else {
            return;
        };
        let session = self.session.clone();
        let state = self.members.entry(member.to_string()).or_default();
        state.realtime = true;
        if !std::mem::replace(&mut state.created, true) {
            out.push(Action::Frame {
                member: member.to_string(),
                frame: json!({ "type": "session-created", "sessionId": session }),
            });
        }
        match kind {
            "session-update" => {
                // The session-configuration policy (design D10): what the
                // member's admission allows is applied, the rest is ignored
                // and named once per connection. The answer is always
                // `session-updated`.
                let (forward, mut ignored, tools) = state.admission.filter(&frame["config"]);
                let unsupported = self.apply_session_config(member, &Value::Object(forward), out);
                ignored.extend(unsupported.into_iter().map(str::to_string));
                ignored.sort();
                ignored.dedup();
                let state = self.members.entry(member.to_string()).or_default();
                if !ignored.is_empty() && !std::mem::replace(&mut state.ignored_reported, true) {
                    out.push(Action::Frame {
                        member: member.to_string(),
                        frame: json!({
                            "type": "custom",
                            "rawType": "remotemedia.config-ignored",
                            "raw": { "fields": ignored, "tools": tools },
                        }),
                    });
                }
                out.push(Action::Frame {
                    member: member.to_string(),
                    frame: json!({ "type": "session-updated" }),
                });
            }
            // The browser does not play the audio (the track does), so it
            // has no playback position: accepted, and nothing changes.
            "conversation-item-truncate" => {}
            "response-cancel" => {
                // The turn node cuts the member off, flushes its track and
                // settles with what the sender released.
                if let (Some(_), Some(node)) = (self.open_turn(member), self.turn_node()) {
                    out.push(Action::Rpc {
                        node,
                        method: "interrupt".into(),
                        arg: json!({ "member": member }),
                    });
                }
            }
            "conversation-item-create" => self.on_item_create(member, &frame["item"], out),
            "response-create" => self.on_response_create(member, out),
            "input-audio-commit" | "input-audio-clear" => self.on_manual_turn(member, kind, out),
            // `input-audio-append` (audio is on the media track) and anything
            // this node does not know.
            _ => {}
        }
    }

    /// What a member's WebRTC audio sender released of a turn: the
    /// transcript follows it.
    pub(crate) fn on_track_released(&mut self, report: &AudioReleased, out: &mut Vec<Action>) {
        if !self.is_realtime(&report.member) {
            return;
        }
        // After a flush the turn node settles the cut and says what was heard.
        if report.flushed {
            return;
        }
        let member = report.member.as_str();
        let Some(turn) = self.turns.get_mut(&report.turn_id) else { return };
        let end = turn.texts.len();
        let Some(listen) = turn.listeners.get_mut(member) else { return };
        // Chunks wholly written; with a drained sender, also the chunk whose
        // audio so far has all been written (as the turn node counts).
        let mut counted = report.chunks_released as usize;
        if report.drained {
            counted = counted.max(report.chunk as usize + 1);
        }
        let counted = counted.min(end);
        listen.released = Some(listen.released.map_or(counted, |known| known.max(counted)));
        listen.released_ms = listen.released_ms.max(report.released_ms);
        let Some(response) = listen.open else { return };
        let first = turn.responses[response - 1].min(end);
        let (response_id, item_id) = (response_id(&report.turn_id, response), item_id(&report.turn_id, response));
        for chunk in listen.sent_chunks.max(first)..counted {
            out.push(Action::Frame {
                member: member.to_string(),
                frame: json!({
                    "type": "audio-transcript-delta",
                    "responseId": response_id, "itemId": item_id, "delta": turn.texts[chunk],
                }),
            });
        }
        listen.sent_chunks = listen.sent_chunks.max(counted);
        if listen.pending_end && counted >= end {
            self.end_response(&report.turn_id, member, None, out);
        }
    }

    /// A settled response is checked for progress of the member's audio.
    pub(crate) fn on_timer(&mut self, member: &str, turn_id: &str, out: &mut Vec<Action>) {
        let Some(listen) = self
            .turns
            .get_mut(turn_id)
            .and_then(|turn| turn.listeners.get_mut(member))
        else {
            return;
        };
        if listen.open.is_none() || !listen.pending_end || !listen.settled {
            return;
        }
        if listen.watched_ms != Some(listen.released_ms) {
            // The member's audio is still leaving: look again later.
            listen.watched_ms = Some(listen.released_ms);
            out.push(Action::Timer {
                member: member.to_string(),
                turn: turn_id.to_string(),
                ms: SETTLED_WAIT_MS,
            });
            return;
        }
        self.end_response(turn_id, member, None, out);
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
        // Typed text as realtime events: the item, then this. (The model's
        // own transport sends typed text as the member's text frame instead,
        // which reaches the turn node without this node.) Allowed
        // instructions reach the turn node by `set_client_instructions`.
        let held = self
            .members
            .get_mut(member)
            .map(|state| std::mem::take(&mut state.held_text))
            .unwrap_or_default();
        if !held.is_empty() {
            let Some(node) = self.turn_node() else { return };
            out.push(Action::Publish {
                node,
                member: member.to_string(),
                data: json!({ "v": 1, "type": "text", "text": held.join("\n") }),
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

    /// Apply the admitted fields of a `session-update`. Returns the fields no
    /// node of the pipeline accepts at runtime.
    fn apply_session_config(&mut self, member: &str, config: &Value, out: &mut Vec<Action>) -> Vec<&'static str> {
        let Some(config) = config.as_object() else { return Vec::new() };
        let mut ignored: Vec<&'static str> = Vec::new();
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
                _ => {}
            }
        }
        ignored
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
    let mut state = Core::new(config.clone());
    state.session = session.to_string();
    let core = Arc::new(parking_lot::Mutex::new(state));
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
    /// The member whose connection this instance serves, once seen.
    connection: OnceLock<String>,
    /// Follows the member's `audio_released` reports.
    media: OnceLock<AbortOnDrop>,
    /// Audio reached the node: said once.
    audio_warned: std::sync::atomic::AtomicBool,
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl RealtimeEventsNode {
    pub fn new(node_id: impl Into<String>, config: RealtimeEventsConfig) -> Self {
        Self {
            node_id: node_id.into(),
            config,
            core: OnceLock::new(),
            fields: parking_lot::Mutex::new(HashMap::new()),
            corr: std::sync::atomic::AtomicU64::new(0),
            connection: OnceLock::new(),
            media: OnceLock::new(),
            audio_warned: std::sync::atomic::AtomicBool::new(false),
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

    /// Follow what `member`'s WebRTC audio sender releases, for as long as
    /// this instance lives: the transcript deltas follow these reports.
    fn follow_media(&self, member: &str, core: &SharedCore, control: &Arc<SessionControl>, emitter: Emitter) {
        let mut media = control.subscribe_media();
        let member = member.to_string();
        let core = core.clone();
        let node_id = self.node_id.clone();
        let fields = self.member_fields(&member);
        let task = tokio::spawn(async move {
            loop {
                let data = match media.recv().await {
                    Ok(data) => data,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                };
                let Some(MediaEvent::AudioReleased(report)) = MediaEvent::from_runtime(&data) else {
                    continue;
                };
                if report.member != member {
                    continue;
                }
                let mut actions = Vec::new();
                core.lock().on_track_released(&report, &mut actions);
                for action in actions {
                    if let Action::Frame { member, frame } = action {
                        emit_frame(&node_id, &emitter, fields.clone(), &member, frame).await;
                    }
                }
            }
        });
        let _ = self.media.set(AbortOnDrop(task));
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
                    let fields = self.member_fields(&member);
                    emit_frame(&self.node_id, emitter, fields, &member, frame).await;
                }
                other => {
                    let Some(control) = control else { continue };
                    self.perform_control(other, core, control, emitter).await;
                }
            }
        }
    }

    async fn perform_control(
        &self,
        action: Action,
        core: &SharedCore,
        control: &Arc<SessionControl>,
        emitter: &Emitter,
    ) {
        match action {
            Action::Frame { .. } => {}
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
                let emitter = emitter.clone();
                let node_id = self.node_id.clone();
                let fields = self.member_fields(&member);
                tokio::spawn(async move {
                    let mut wait = Some(ms);
                    while let Some(ms) = wait.take() {
                        tokio::time::sleep(Duration::from_millis(ms)).await;
                        let mut actions = Vec::new();
                        core.lock().on_timer(&member, &turn, &mut actions);
                        for action in actions {
                            match action {
                                Action::Frame { member: to, frame } => {
                                    emit_frame(&node_id, &emitter, fields.clone(), &to, frame).await;
                                }
                                // The same timer, again.
                                Action::Timer { ms, .. } => wait = Some(ms),
                                _ => {}
                            }
                        }
                    }
                });
            }
        }
    }
}

/// Emit a server event for `member` as `{ "v": 1, "rt": <event> }`,
/// attributed and addressed to it: its data channel takes it, and no other
/// member's does.
async fn emit_frame(
    node_id: &str,
    emitter: &Emitter,
    mut fields: HashMap<String, String>,
    member: &str,
    frame: Value,
) {
    fields.insert(participant::ID.to_string(), member.to_string());
    fields.insert(participant::TO.to_string(), member.to_string());
    let origin = Origin::from_metadata(&fields).map(Arc::new);
    let output = RuntimeData::Json(json!({ "v": 1, RT_FRAME_KEY: frame }));
    if let Err(error) = emitter.emit_as(output, origin).await {
        tracing::debug!(node = %node_id, %member, %error, "realtime frame not emitted");
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
        _callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let value = match data {
            RuntimeData::Json(value) => value,
            RuntimeData::Audio { metadata, .. } => {
                // The member's own audio, forwarded by the VAD node: not
                // this node's. Audio of a turn is the speech: the node
                // carries none, and a pipeline that still sends the speech
                // through it loses that speech. Say so, once.
                let mut tags: HashMap<String, String> = ctx
                    .origin()
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
                if TurnTag::from_metadata(&tags).is_some()
                    && !self.audio_warned.swap(true, std::sync::atomic::Ordering::Relaxed)
                {
                    tracing::warn!(
                        node = %self.node_id,
                        "RealtimeEventsNode received audio and dropped it: it is on the event path only. \
                         Connect the speech output to the pipeline's sink, not to this node."
                    );
                }
                return Ok(0);
            }
            _ => return Ok(0),
        };
        let origin = ctx.origin();
        let core = self.core(ctx);
        let control = NodeRuntimeContext::of(ctx).map(|ctx| ctx.control.clone());
        if let Some(origin) = origin.as_deref() {
            self.note(origin);
            // A member's own input marks its connection's instance. (A
            // turn's `ui` frames carry the turn's member, who may be
            // another one.)
            let own = rt_frame_event(&value).is_some() || vad_edge(&value).is_some();
            if own && self.connection.get().is_none() && self.connection.set(origin.id().to_string()).is_ok() {
                core.lock()
                    .connected(origin.id(), Admission::from_fields(origin.fields()));
                if let Some(control) = control.as_ref() {
                    self.follow_media(origin.id(), &core, control, Emitter::for_call(ctx));
                }
            }
        }
        let owner = origin.as_deref().map(|origin| origin.id().to_string());

        let mut actions = Vec::new();
        if let Some(event) = rt_frame_event(&value) {
            // Only the member's own data channel speaks for it.
            if let Some(member) = owner.as_deref() {
                core.lock().on_client(member, event, &mut actions);
            }
        } else if is_ui_frame(&value) {
            let to = addressed_to(origin.as_deref());
            core.lock().on_ui(owner.as_deref(), to.as_deref(), &value, &mut actions);
        } else if let (Some(start), Some(member)) = (vad_edge(&value), owner.as_deref()) {
            core.lock().on_vad(member, start, &mut actions);
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
                     for members of the AI SDK realtime hook, who connect over WebRTC: it sends \
                     and receives `{ v: 1, rt: <event> }` frames on the member's data channel, \
                     while the reply is heard on the member's media track. It carries no audio. \
                     Inputs, told apart by shape: `rt` frames, `HostedTurnNode` `ui` frames and \
                     VAD events. One instance per participant.",
                )
                .category("conversation")
                // The VAD node forwards the member's audio with its events; it is dropped here.
                .accepts([RuntimeDataType::Audio, RuntimeDataType::Json])
                .produces([RuntimeDataType::Json])
                .config_schema_from::<RealtimeEventsConfig>(),
        )
    }
}

#[cfg(test)]
#[path = "realtime_events_tests.rs"]
mod tests;
