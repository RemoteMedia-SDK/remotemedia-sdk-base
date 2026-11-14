//! `RealtimeEventsNode`'s state machine with scripted `ui` frames, VAD
//! events, audio and client frames (no socket, no session): one test per row
//! of the two event tables of design D10, the spoken-text rules of task 9.4,
//! the session-configuration policy of task 9.5, and client tools.

use super::*;

const T: &str = "turn-1";

fn core() -> Core {
    core_with(json!({ "turn_node": "turn", "endpoint": "agent", "truncate_grace_ms": 300 }))
}

fn core_with(params: Value) -> Core {
    Core::new(serde_json::from_value(params).unwrap())
}

fn ui(turn: Option<&str>, chunk: Value) -> Value {
    json!({ "v": 1, "turn": turn, "chunk": chunk })
}

/// What a script produced, as `(member, frame)` for frames.
#[derive(Default)]
struct Seen(Vec<Action>);

impl Seen {
    fn frames(&self, member: &str) -> Vec<Value> {
        self.0
            .iter()
            .filter_map(|action| match action {
                Action::Frame { member: to, frame } if to == member => Some(frame.clone()),
                _ => None,
            })
            .collect()
    }

    fn types(&self, member: &str) -> Vec<String> {
        self.frames(member)
            .iter()
            .map(|frame| frame["type"].as_str().unwrap_or_default().to_string())
            .collect()
    }

    fn released(&self) -> Vec<AudioReleased> {
        self.0
            .iter()
            .filter_map(|action| match action {
                Action::Released(report) => Some(report.clone()),
                _ => None,
            })
            .collect()
    }

    fn take(&mut self) -> Seen {
        Seen(std::mem::take(&mut self.0))
    }
}

struct Script {
    core: Core,
    seen: Seen,
}

impl Script {
    fn new(core: Core, members: &[&str]) -> Self {
        let mut script = Self {
            core,
            seen: Seen::default(),
        };
        for member in members {
            script.client(member, json!({
                "type": INTERNAL_JOINED, "member": member, "transport": "realtime",
                "outputRate": 24000, "inputRate": 24000,
            }));
        }
        script.seen.take();
        script
    }

    /// A `ui` frame of `owner`'s turn, addressed to `to`.
    fn ui_to(&mut self, owner: &str, to: &[&str], turn: Option<&str>, chunk: Value) {
        let to: Vec<String> = to.iter().map(|member| member.to_string()).collect();
        self.core
            .on_ui(Some(owner), Some(&to), &ui(turn, chunk), &mut self.seen.0);
    }

    fn ui(&mut self, owner: &str, chunk: Value) {
        self.ui_to(owner, &[owner], Some(T), chunk);
    }

    fn client(&mut self, member: &str, frame: Value) {
        self.core.on_client(member, &frame, &mut self.seen.0);
    }

    fn released(&mut self, member: &str, item: &str, ms: u64, flushed: bool, drained: bool) {
        let (turn, _) = parse_item(item);
        self.client(member, json!({
            "type": INTERNAL_RELEASED, "itemId": item, "audioMs": ms, "responseId": turn,
            "receivedMs": ms, "flushed": flushed, "drained": drained,
        }));
    }

    /// The start of `owner`'s spoken turn up to its first text.
    fn start(&mut self, owner: &str, to: &[&str], said: &str) {
        self.ui_to(owner, to, Some(T), json!({
            "type": "data-transcript", "id": format!("{T}:user"),
            "data": { "text": said, "source": "speech", "member": { "id": owner } },
        }));
        self.ui_to(owner, to, Some(T), json!({ "type": "start", "messageId": format!("{T}:assistant") }));
    }

    /// One text chunk of the turn and `ms` of its audio.
    fn say(&mut self, owner: &str, to: &[&str], chunk: u64, text: &str, ms: f64) -> String {
        self.ui_to(owner, to, Some(T), json!({ "type": "text-delta", "id": format!("{T}:text"), "delta": text }));
        self.core.on_audio(T, chunk, ms)
    }

    fn take(&mut self) -> Seen {
        self.seen.take()
    }
}

#[test]
fn defaults_and_aliases_parse() {
    let config: RealtimeEventsConfig = serde_json::from_value(json!({})).unwrap();
    assert_eq!(config.tool_activity, ToolActivity::Visible);
    assert_eq!((config.lead_ms, config.truncate_grace_ms), (500, 300));
    // What `voiceAgent` generates.
    let config: RealtimeEventsConfig = serde_json::from_value(json!({
        "turn_node": "turn", "endpoint": "support", "output_rate": 24000, "input_rate": 48000,
        "tool_activity": "hidden", "lead_ms": 400, "truncate_grace_ms": 250,
        "vad_node": "vad", "accumulator_node": "accumulator", "stt_node": "stt", "tts_node": "tts",
    }))
    .unwrap();
    assert_eq!(config.tool_activity, ToolActivity::Hidden);
    assert_eq!(config.endpoint.as_deref(), Some("support"));
    assert_eq!(config.input_rate, Some(48000));
    let schema = RealtimeEventsNodeFactory.schema().unwrap().config_schema.unwrap();
    for name in ["turn_node", "endpoint", "tool_activity", "lead_ms", "truncate_grace_ms", "input_rate", "config_rpc"] {
        assert!(schema["properties"].as_object().unwrap().contains_key(name), "schema lacks {name}");
    }
    assert!(RealtimeEventsNodeFactory.create("realtime".into(), &Value::Null, None).is_ok());
    assert_eq!(base_node_id("realtime__p3"), "realtime");
    assert_eq!(base_node_id("realtime"), "realtime");
    assert_eq!(parse_item("turn-1:assistant"), ("turn-1".to_string(), 1));
    assert_eq!(parse_item("turn-1#2:assistant"), ("turn-1".to_string(), 2));
}

#[test]
fn vad_start_and_stop_are_speech_events_of_that_member() {
    let mut script = Script::new(core(), &["alice"]);
    script.core.on_vad("alice", true, &mut script.seen.0);
    script.core.on_vad("alice", false, &mut script.seen.0);
    assert_eq!(script.seen.types("alice"), ["speech-started", "speech-stopped"]);
    assert_eq!(vad_edge(&json!({ "is_speech_start": true, "is_speech_end": false })), Some(true));
    assert_eq!(vad_edge(&json!({ "event": { "is_speech_end": true } })), Some(false));
    assert_eq!(vad_edge(&json!({ "has_speech": true, "is_speech_start": false, "is_speech_end": false })), None);
}

#[test]
fn a_member_on_another_transport_gets_no_frames() {
    // bob never joined through the realtime transport.
    let mut script = Script::new(core(), &[]);
    script.core.on_vad("bob", true, &mut script.seen.0);
    script.start("bob", &["bob"], "hi");
    let item = script.say("bob", &["bob"], 0, "Hello. ", 400.0);
    script.ui("bob", json!({ "type": "finish" }));
    // Audio still gets its ids.
    assert_eq!(item, "turn-1:assistant");
    assert!(script.seen.0.is_empty(), "{:?}", script.seen.0);
}

#[test]
fn a_spoken_turn_is_one_response_whose_transcript_follows_released_audio() {
    let mut script = Script::new(core(), &["alice"]);
    script.start("alice", &["alice"], "what time is it");
    let seen = script.take();
    assert_eq!(
        seen.frames("alice"),
        [
            json!({ "type": "audio-committed", "itemId": "turn-1:user" }),
            json!({ "type": "input-transcription-completed", "itemId": "turn-1:user", "transcript": "what time is it" }),
            json!({ "type": "response-created", "responseId": "turn-1" }),
        ]
    );

    let item = script.say("alice", &["alice"], 0, "It is noon. ", 400.0);
    script.say("alice", &["alice"], 1, "Anything else?", 600.0);
    // Text alone produces nothing: the transcript follows the audio.
    assert!(script.take().0.is_empty());

    // 300 ms released: no chunk has wholly left.
    script.released("alice", &item, 300, false, false);
    let seen = script.take();
    assert!(seen.frames("alice").is_empty());
    assert_eq!(seen.released()[0].chunks_released, 0);
    assert_eq!(seen.released()[0].turn_id, T);
    assert_eq!(seen.released()[0].member, "alice");

    // 450 ms: chunk 0 has left.
    script.released("alice", &item, 450, false, false);
    let seen = script.take();
    assert_eq!(
        seen.frames("alice"),
        [json!({
            "type": "audio-transcript-delta", "responseId": "turn-1", "itemId": "turn-1:assistant",
            "delta": "It is noon. ",
        })]
    );
    assert_eq!((seen.released()[0].chunks_released, seen.released()[0].chunk), (1, 1));

    // Everything left: the last chunk counts once the sender is drained.
    script.released("alice", &item, 1000, false, true);
    let seen = script.take();
    assert_eq!(seen.frames("alice")[0]["delta"], "Anything else?");
    assert!(seen.released()[0].drained);

    // The turn node settles and closes the turn on `ui`.
    script.ui("alice", json!({ "type": "text-end", "id": "turn-1:text" }));
    script.ui("alice", json!({ "type": "finish" }));
    script.ui("alice", json!({ "type": "data-turn", "id": T, "data": { "status": "listening" }, "transient": true }));
    assert_eq!(
        script.take().frames("alice"),
        [
            json!({ "type": "audio-done", "responseId": "turn-1", "itemId": "turn-1:assistant" }),
            json!({
                "type": "audio-transcript-done", "responseId": "turn-1", "itemId": "turn-1:assistant",
                "transcript": "It is noon. Anything else?",
            }),
            json!({ "type": "response-done", "responseId": "turn-1", "status": "completed" }),
        ]
    );
}

#[test]
fn the_end_of_a_response_waits_until_its_audio_has_left() {
    let mut script = Script::new(core(), &["alice"]);
    script.start("alice", &["alice"], "hi");
    let item = script.say("alice", &["alice"], 0, "Hello.", 400.0);
    script.released("alice", &item, 200, false, false);
    script.take();
    script.ui("alice", json!({ "type": "finish" }));
    assert!(script.take().frames("alice").is_empty(), "held: 200 of 400 ms released");
    script.released("alice", &item, 400, false, true);
    assert_eq!(
        script.take().types("alice"),
        ["audio-transcript-delta", "audio-done", "audio-transcript-done", "response-done"]
    );
}

#[test]
fn typed_text_is_held_until_response_create_and_sent_as_the_members_text() {
    let mut script = Script::new(core(), &["alice"]);
    script.client("alice", json!({
        "type": "conversation-item-create",
        "item": { "type": "text-message", "role": "user", "text": "hello there" },
    }));
    assert!(script.take().0.is_empty(), "held until response-create");
    script.client("alice", json!({ "type": "response-create" }));
    assert_eq!(
        script.take().0,
        [Action::Publish {
            node: "turn".into(),
            member: "alice".into(),
            data: json!({ "v": 1, "type": "text", "text": "hello there" }),
        }]
    );
    // A second `response-create` has nothing to start.
    script.client("alice", json!({ "type": "response-create" }));
    assert!(script.take().0.is_empty());

    // The typed turn's user part is reported as nothing: the hook that sent
    // the text already shows it, and a transcription would show it twice.
    script.ui("alice", json!({
        "type": "data-transcript", "id": "turn-1:user",
        "data": { "text": "hello there", "source": "text", "member": { "id": "alice" } },
    }));
    assert!(script.take().types("alice").is_empty());
}

#[test]
fn response_cancel_flushes_interrupts_and_falls_back_to_the_released_position() {
    let mut script = Script::new(core(), &["alice"]);
    script.start("alice", &["alice"], "hi");
    let item = script.say("alice", &["alice"], 0, "One. ", 400.0);
    script.say("alice", &["alice"], 1, "Two. ", 400.0);
    script.say("alice", &["alice"], 2, "Three.", 400.0);
    script.released("alice", &item, 500, false, false);
    script.take();

    script.client("alice", json!({ "type": "response-cancel" }));
    assert_eq!(
        script.take().0,
        [
            Action::Frame {
                member: "alice".into(),
                frame: json!({ "type": INTERNAL_FLUSH, "itemId": "turn-1:assistant" }),
            },
            Action::Rpc { node: "turn".into(), method: "interrupt".into(), arg: json!({ "member": "alice" }) },
            Action::Timer { member: "alice".into(), turn: T.into(), ms: 300 },
        ]
    );

    // The transport reports where the flush left the item; no truncation
    // arrives within the grace period, so all released audio counts.
    script.released("alice", &item, 850, true, true);
    assert!(script.take().0.is_empty(), "waiting for a truncation");
    script.core.on_timer("alice", T, &mut script.seen.0);
    let report = script.take().released().remove(0);
    assert_eq!((report.released_ms, report.chunks_released, report.flushed), (850, 2, true));
    assert!(!report.drained);

    // The turn node settles with that and says so on `ui`.
    script.ui("alice", json!({ "type": "abort" }));
    script.ui("alice", json!({
        "type": "data-turn", "id": T, "transient": true,
        "data": { "status": "interrupted", "spoken": "One. Two. " },
    }));
    assert_eq!(
        script.take().frames("alice"),
        [
            json!({ "type": "audio-done", "responseId": "turn-1", "itemId": "turn-1:assistant" }),
            json!({
                "type": "audio-transcript-done", "responseId": "turn-1", "itemId": "turn-1:assistant",
                "transcript": "One. Two. ",
            }),
            json!({ "type": "response-done", "responseId": "turn-1", "status": "cancelled" }),
        ]
    );
}

#[test]
fn a_truncation_maps_audio_end_ms_to_text_chunks() {
    let mut script = Script::new(core(), &["alice"]);
    script.start("alice", &["alice"], "hi");
    let item = script.say("alice", &["alice"], 0, "One. ", 400.0);
    script.say("alice", &["alice"], 1, "Two. ", 400.0);
    script.say("alice", &["alice"], 2, "Three.", 400.0);
    script.released("alice", &item, 900, false, false);
    script.take();

    // Barge-in: the turn node flushed (its policy), the transport reports.
    script.released("alice", &item, 1000, true, true);
    assert_eq!(
        script.take().0,
        [Action::Timer { member: "alice".into(), turn: T.into(), ms: 300 }],
        "the turn node already flushed and cancelled: only the grace period starts"
    );
    // The browser played 450 ms: chunk 0 ends at 400, chunk 1 at 800.
    script.client("alice", json!({
        "type": "conversation-item-truncate", "itemId": item, "contentIndex": 0, "audioEndMs": 450,
    }));
    let report = script.take().released().remove(0);
    assert_eq!((report.released_ms, report.chunks_released, report.flushed), (450, 1, true));
    // The grace period ending later changes nothing.
    script.core.on_timer("alice", T, &mut script.seen.0);
    assert!(script.take().0.is_empty());
}

#[test]
fn a_truncation_before_any_flush_asks_for_the_cut_itself() {
    let mut script = Script::new(core(), &["alice"]);
    script.start("alice", &["alice"], "hi");
    let item = script.say("alice", &["alice"], 0, "One. ", 400.0);
    script.say("alice", &["alice"], 1, "Two. ", 400.0);
    script.released("alice", &item, 700, false, false);
    script.take();
    script.client("alice", json!({
        "type": "conversation-item-truncate", "itemId": item, "contentIndex": 0, "audioEndMs": 800,
    }));
    let seen = script.take();
    assert_eq!(seen.types("alice"), [INTERNAL_FLUSH]);
    assert!(seen.0.iter().any(|action| matches!(action, Action::Rpc { method, .. } if method == "interrupt")));
    // Never more than was released to the browser: 700 ms, so one chunk.
    let report = seen.released().remove(0);
    assert_eq!((report.released_ms, report.chunks_released), (700, 1));

    // A truncation of a response that is over is not a cut.
    let mut script = Script::new(core(), &["alice"]);
    script.start("alice", &["alice"], "hi");
    let item = script.say("alice", &["alice"], 0, "One. ", 400.0);
    script.released("alice", &item, 400, false, true);
    script.ui("alice", json!({ "type": "finish" }));
    script.take();
    script.client("alice", json!({
        "type": "conversation-item-truncate", "itemId": item, "contentIndex": 0, "audioEndMs": 100,
    }));
    assert!(script.take().0.is_empty());
}

#[test]
fn speaker_scope_cancels_only_the_response_of_the_member_who_spoke() {
    // alice's turn is spoken to alice and bob.
    let both = ["alice", "bob"];
    let mut script = Script::new(core(), &both);
    script.start("alice", &both, "tell us");
    let seen = script.take();
    // Only alice gets the user part; both get the response.
    assert_eq!(
        seen.types("alice"),
        ["audio-committed", "input-transcription-completed", "response-created"]
    );
    assert_eq!(seen.types("bob"), ["response-created"]);

    let item = script.say("alice", &both, 0, "One. ", 400.0);
    script.say("alice", &both, 1, "Two. ", 400.0);
    script.released("alice", &item, 500, false, false);
    script.released("bob", &item, 500, false, false);
    script.take();

    // bob speaks: the turn node stops the turn for bob only and flushes his
    // audio; bob's browser truncates at 420 ms.
    script.core.on_vad("bob", true, &mut script.seen.0);
    script.released("bob", &item, 600, true, true);
    script.client("bob", json!({
        "type": "conversation-item-truncate", "itemId": item, "contentIndex": 0, "audioEndMs": 420,
    }));
    let seen = script.take();
    assert_eq!(seen.types("bob"), ["speech-started"]);
    let report = seen.released().remove(0);
    assert_eq!((report.member.as_str(), report.chunks_released, report.flushed), ("bob", 1, true));
    assert!(seen.frames("alice").is_empty());

    // The turn node tells bob alone what he heard (`ui_for`).
    script.ui_to("bob", &["bob"], Some(T), json!({ "type": "abort" }));
    script.ui_to("bob", &["bob"], Some(T), json!({
        "type": "data-turn", "id": T, "transient": true,
        "data": { "status": "interrupted", "spoken": "One. " },
    }));
    let seen = script.take();
    assert_eq!(seen.types("bob"), ["audio-done", "audio-transcript-done", "response-done"]);
    assert_eq!(seen.frames("bob")[1]["transcript"], "One. ");
    assert_eq!(seen.frames("bob")[2]["status"], "cancelled");
    assert!(seen.frames("alice").is_empty(), "alice's response goes on");

    // The turn goes on for alice and completes.
    script.say("alice", &["alice"], 2, "Three.", 400.0);
    script.released("alice", &item, 1200, false, true);
    script.ui_to("alice", &["alice"], Some(T), json!({ "type": "finish" }));
    let seen = script.take();
    assert!(seen.frames("bob").is_empty());
    let alice = seen.frames("alice");
    let done = alice.last().unwrap();
    assert_eq!((done["type"].as_str(), done["status"].as_str()), (Some("response-done"), Some("completed")));
    let transcript = alice.iter().find(|frame| frame["type"] == "audio-transcript-done").unwrap();
    assert_eq!(transcript["transcript"], "One. Two. Three.");
}

#[test]
fn backend_tools_are_custom_events_and_never_function_calls() {
    let tool_in = json!({ "type": "tool-input-available", "toolCallId": "c1", "toolName": "lookup", "input": { "q": "x" } });
    let tool_out = json!({ "type": "tool-output-available", "toolCallId": "c1", "output": { "ok": true } });
    let data = json!({ "type": "data-weather", "id": "w1", "data": { "city": "Oslo" } });

    let mut script = Script::new(core(), &["alice"]);
    script.start("alice", &["alice"], "hi");
    script.take();
    for chunk in [&tool_in, &tool_out, &data] {
        script.ui("alice", chunk.clone());
    }
    // Library parts are not data events.
    script.ui("alice", json!({ "type": "data-turn", "id": T, "data": { "status": "speaking" }, "transient": true }));
    assert_eq!(
        script.take().frames("alice"),
        [
            json!({ "type": "custom", "rawType": "remotemedia.tool", "raw": tool_in }),
            json!({ "type": "custom", "rawType": "remotemedia.tool", "raw": tool_out }),
            json!({ "type": "custom", "rawType": "remotemedia.data", "raw": data }),
        ]
    );
    // `session.send` outside a turn.
    script.ui_to("alice", &["alice"], None, data.clone());
    assert_eq!(script.take().frames("alice")[0]["rawType"], "remotemedia.data");

    // Hidden: nothing names the tool; data parts still arrive.
    let mut script = Script::new(
        core_with(json!({ "turn_node": "turn", "tool_activity": "hidden" })),
        &["alice"],
    );
    script.start("alice", &["alice"], "hi");
    script.take();
    for chunk in [&tool_in, &tool_out, &data] {
        script.ui("alice", chunk.clone());
    }
    let frames = script.take().frames("alice");
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0]["rawType"], "remotemedia.data");
    assert!(!frames[0].to_string().contains("lookup"));
}

#[test]
fn a_client_tool_ends_the_response_and_the_turn_continues_as_a_second_one() {
    let mut script = Script::new(core(), &["alice"]);
    script.start("alice", &["alice"], "show me Oslo");
    script.take();
    // The host's client-tool-call chunk (clients/ai/src/voice/client-tools.ts).
    script.ui("alice", json!({
        "type": "data-client-tool-call", "id": "call-1", "transient": true,
        "data": { "callId": "call-1", "name": "showMap", "arguments": "{\"place\":\"Oslo\"}" },
    }));
    assert_eq!(
        script.take().frames("alice"),
        [
            json!({
                "type": "function-call-arguments-delta", "responseId": "turn-1",
                "itemId": "turn-1:call:call-1", "callId": "call-1", "delta": "{\"place\":\"Oslo\"}",
            }),
            json!({
                "type": "function-call-arguments-done", "responseId": "turn-1",
                "itemId": "turn-1:call:call-1", "callId": "call-1", "name": "showMap",
                "arguments": "{\"place\":\"Oslo\"}",
            }),
            json!({ "type": "audio-done", "responseId": "turn-1", "itemId": "turn-1:assistant" }),
            json!({
                "type": "audio-transcript-done", "responseId": "turn-1", "itemId": "turn-1:assistant",
                "transcript": "",
            }),
            json!({ "type": "response-done", "responseId": "turn-1", "status": "completed" }),
        ]
    );

    // The browser's output goes to the host...
    script.client("alice", json!({
        "type": "conversation-item-create",
        "item": { "type": "function-call-output", "callId": "call-1", "output": "{\"shown\":true}" },
    }));
    assert_eq!(
        script.take().0,
        [Action::HostEvent(json!({
            "type": "client_tool_output", "turn_id": T, "call_id": "call-1", "output": "{\"shown\":true}",
        }))]
    );
    // ...and its `response-create` releases the continuation as response 2.
    script.client("alice", json!({ "type": "response-create" }));
    assert_eq!(
        script.take().0,
        [
            Action::HostEvent(json!({ "type": "response_create", "turn_id": T })),
            Action::Frame {
                member: "alice".into(),
                frame: json!({ "type": "response-created", "responseId": "turn-1#2" }),
            },
        ]
    );

    let item = script.say("alice", &["alice"], 0, "Here is Oslo.", 500.0);
    assert_eq!(item, "turn-1#2:assistant");
    script.released("alice", &item, 500, false, true);
    script.ui("alice", json!({ "type": "finish" }));
    let frames = script.take().frames("alice");
    assert_eq!(
        frames.iter().map(|frame| frame["type"].as_str().unwrap()).collect::<Vec<_>>(),
        ["audio-transcript-delta", "audio-done", "audio-transcript-done", "response-done"]
    );
    for frame in &frames {
        assert_eq!(frame["responseId"], "turn-1#2");
    }
    assert_eq!(frames[2]["transcript"], "Here is Oslo.");
    assert_eq!(frames[2]["itemId"], "turn-1#2:assistant");
}

#[test]
fn text_after_a_client_tool_without_response_create_still_becomes_a_second_response() {
    // The tool timed out at the host: the hook never asks for a response.
    let mut script = Script::new(core(), &["alice"]);
    script.start("alice", &["alice"], "show me Oslo");
    script.ui("alice", json!({
        "type": "data-client-tool-call", "id": "call-1",
        "data": { "callId": "call-1", "name": "showMap", "arguments": "{}" },
    }));
    script.take();
    script.say("alice", &["alice"], 0, "I could not show it.", 300.0);
    assert_eq!(
        script.take().frames("alice"),
        [json!({ "type": "response-created", "responseId": "turn-1#2" })]
    );
}

#[test]
fn session_config_is_applied_where_a_node_takes_it_and_reported_ignored_where_not() {
    // Nothing in the pipeline accepts a runtime change (the default).
    let mut script = Script::new(
        core_with(json!({ "turn_node": "turn", "tts_node": "tts", "vad_node": "vad", "stt_node": "stt" })),
        &["alice"],
    );
    script.client("alice", json!({
        "type": "session-update",
        "config": {
            "voice": "af_heart",
            "turnDetection": { "type": "server-vad" },
            "inputAudioTranscription": { "language": "nb" },
            "instructions": "Answer in Norwegian.",
        },
    }));
    let seen = script.take();
    let ignored = seen.frames("alice").remove(0);
    assert_eq!(ignored["rawType"], "remotemedia.config-ignored");
    let mut fields: Vec<&str> = ignored["raw"]["fields"].as_array().unwrap().iter().filter_map(Value::as_str).collect();
    fields.sort();
    assert_eq!(fields, ["inputAudioTranscription.language", "turnDetection", "voice"]);
    // Instructions go to the turn node, for typed and spoken turns alike.
    assert!(seen.0.contains(&Action::Rpc {
        node: "turn".into(),
        method: "set_client_instructions".into(),
        arg: json!({ "member": "alice", "instructions": "Answer in Norwegian." }),
    }));
    // ...and ride on the member's typed text.
    script.client("alice", json!({
        "type": "conversation-item-create", "item": { "type": "text-message", "role": "user", "text": "hei" },
    }));
    script.client("alice", json!({ "type": "response-create" }));
    assert_eq!(
        script.take().0,
        [Action::Publish {
            node: "turn".into(),
            member: "alice".into(),
            data: json!({ "v": 1, "type": "text", "text": "hei", "client_instructions": "Answer in Norwegian." }),
        }]
    );

    // Nodes that take the change: each field is a typed-RPC call.
    let mut script = Script::new(
        core_with(json!({
            "turn_node": "turn", "tts_node": "tts", "vad_node": "vad", "stt_node": "stt",
            "accumulator_node": "accumulator",
            "config_rpc": { "voice": "set_voice", "turn_detection": "set_turn_detection", "language": "set_language" },
        })),
        &["alice"],
    );
    script.client("alice", json!({
        "type": "session-update",
        "config": {
            "voice": "af_heart",
            "turnDetection": { "type": "disabled" },
            "inputAudioTranscription": { "language": "nb" },
        },
    }));
    let seen = script.take();
    assert!(seen.frames("alice").is_empty(), "nothing ignored");
    for (node, method, arg) in [
        ("tts", "set_voice", json!("af_heart")),
        ("vad", "set_turn_detection", json!({ "type": "disabled" })),
        ("stt", "set_language", json!("nb")),
    ] {
        assert!(
            seen.0.contains(&Action::Rpc { node: node.into(), method: method.into(), arg }),
            "{node}.{method}"
        );
    }
}

#[test]
fn commit_and_clear_reach_the_accumulator_only_with_turn_detection_disabled() {
    let params = json!({
        "turn_node": "turn", "vad_node": "vad", "accumulator_node": "accumulator",
        "config_rpc": { "turn_detection": "set_turn_detection" },
    });
    let mut script = Script::new(core_with(params), &["alice"]);
    // Server turn detection: the browser's commit means nothing.
    script.client("alice", json!({ "type": "input-audio-commit" }));
    script.client("alice", json!({ "type": "input-audio-clear" }));
    assert!(script.take().0.is_empty());

    script.client("alice", json!({ "type": "session-update", "config": { "turnDetection": { "type": "disabled" } } }));
    let event = |start: bool| Action::Publish {
        node: "accumulator".into(),
        member: "alice".into(),
        data: json!({ "is_speech_start": start, "is_speech_end": !start }),
    };
    assert!(script.take().0.contains(&event(true)), "the accumulator collects from now on");
    script.client("alice", json!({ "type": "input-audio-commit" }));
    assert_eq!(script.take().0, [event(false), event(true)]);
    script.client("alice", json!({ "type": "input-audio-clear" }));
    assert_eq!(script.take().0, [event(true)]);
}

#[test]
fn a_failed_turn_is_reported_and_still_ends() {
    let mut script = Script::new(core(), &["alice"]);
    script.start("alice", &["alice"], "hi");
    script.take();
    script.ui("alice", json!({ "type": "error", "errorText": "no_responder" }));
    script.ui("alice", json!({ "type": "finish" }));
    assert_eq!(
        script.take().types("alice"),
        ["custom", "audio-done", "audio-transcript-done", "response-done"]
    );
}
