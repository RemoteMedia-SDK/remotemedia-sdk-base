//! The realtime transport and `RealtimeEventsNode` together, in a real
//! shared session (design D10, tasks 9.3 and 9.4): a browser-side WebSocket,
//! `HostedTurnNode` answered by an in-process host, a stand-in for TTS that
//! turns each text chunk into audio, and the node between TTS and the output.
//!
//! What is checked is the thing neither half can show alone: after an
//! interruption, the text of the settlement the host receives and the text of
//! the browser's final `audio-transcript-done` are the same, and both stop at
//! the browser's truncation point.

mod common;

use common::*;
use remotemedia_core::data::{AudioSamples, RuntimeData};
use remotemedia_core::manifest::{Connection, Manifest, NodeManifest};
use remotemedia_core::nodes::{AsyncNodeWrapper, AsyncStreamingNode, StreamingNode, StreamingNodeFactory};
use remotemedia_core::transport::endpoint::{
    EndpointHost, EndpointReplier, EndpointSpec, HostEvent, HostKind, IncomingCall,
};
use remotemedia_core::transport::session_control::{ControlAddress, RpcCallScope, SessionControl};
use remotemedia_realtime::RealtimeConfig;
use remotemedia_traits::call::CallReplyEvent;
use remotemedia_traits::rpc::RpcMode;
use remotemedia_webrtc::WebRtcTransportConfig;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

const WAIT: Duration = Duration::from_secs(10);
/// Audio the TTS stand-in produces per text chunk.
const CHUNK_MS: u64 = 600;
const TTS_RATE: u32 = 16_000;

/// Stands in for TTS: each text chunk becomes `CHUNK_MS` of audio. The
/// router carries the chunk's turn tag to the audio, as for a real TTS node.
struct Tts;

#[async_trait::async_trait]
impl AsyncStreamingNode for Tts {
    fn node_type(&self) -> &str {
        "TestTts"
    }
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, remotemedia_core::Error> {
        Ok(match data {
            RuntimeData::Text(_) => RuntimeData::Audio {
                samples: AudioSamples::from(sine_f32(TTS_RATE, 330.0, (u64::from(TTS_RATE) * CHUNK_MS / 1000) as usize)),
                sample_rate: TTS_RATE,
                channels: 1,
                stream_id: None,
                timestamp_us: None,
                arrival_ts_us: None,
                metadata: None,
            },
            other => other,
        })
    }
}

struct TtsFactory;

impl StreamingNodeFactory for TtsFactory {
    fn create(
        &self,
        _node_id: String,
        _params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, remotemedia_core::Error> {
        Ok(Box::new(AsyncNodeWrapper(Arc::new(Tts))))
    }
    fn node_type(&self) -> &str {
        "TestTts"
    }
}

fn node(id: &str, node_type: &str, params: Value) -> NodeManifest {
    NodeManifest {
        id: id.into(),
        node_type: node_type.into(),
        params,
        ..Default::default()
    }
}

fn connection(from: &str, from_port: Option<&str>, to: &str) -> Connection {
    Connection {
        from: from.into(),
        to: to.into(),
        from_port: from_port.map(str::to_string),
        ..Default::default()
    }
}

/// member input -> turn -> tts -> realtime, and turn.ui -> realtime.
fn manifest(turn: Value) -> Manifest {
    let mut turn_params = json!({ "endpoint": "agent", "flush_wait_ms": 1500 });
    for (key, value) in turn.as_object().cloned().unwrap_or_default() {
        turn_params[key] = value;
    }
    Manifest {
        nodes: vec![
            node("in", "PassThrough", json!({})),
            node("turn", "HostedTurnNode", turn_params),
            node("tts", "TestTts", json!({})),
            node(
                "realtime",
                "RealtimeEventsNode",
                json!({ "turn_node": "turn", "endpoint": "agent", "input_rate": 16000, "lead_ms": 300, "truncate_grace_ms": 300 }),
            ),
        ],
        connections: vec![
            connection("in", None, "turn"),
            connection("turn", None, "tts"),
            connection("tts", None, "realtime"),
            connection("turn", Some("ui"), "realtime"),
        ],
        ..Default::default()
    }
}

/// The in-process host of the agent's endpoint.
struct Host {
    host: EndpointHost,
    reply: EndpointReplier,
    seen: VecDeque<IncomingCall>,
}

impl Host {
    fn new(control: &SessionControl) -> Self {
        let spec = EndpointSpec::new("agent", HostKind::Service)
            .method("turn", RpcMode::Stream)
            .method("turn_settled", RpcMode::FireAndForget)
            .method("event", RpcMode::FireAndForget);
        let host = control.host_endpoint(spec, true).expect("the endpoint is free");
        Self {
            reply: host.replier(),
            host,
            seen: VecDeque::new(),
        }
    }

    /// The next call of `method` that satisfies `wanted`.
    async fn call(&mut self, method: &str, wanted: impl Fn(&Value) -> bool) -> IncomingCall {
        let matches = |call: &IncomingCall| call.method == method && wanted(&call.args[0]);
        if let Some(at) = self.seen.iter().position(&matches) {
            return self.seen.remove(at).unwrap();
        }
        loop {
            let event = tokio::time::timeout(WAIT, self.host.next())
                .await
                .unwrap_or_else(|_| panic!("a `{method}` call in time"))
                .expect("still hosting");
            if let HostEvent::Call(call) = event {
                if matches(&call) {
                    return call;
                }
                self.seen.push_back(call);
            }
        }
    }

    /// Answer the next turn with `chunks`; its request.
    async fn answer(&mut self, chunks: &[&str]) -> Value {
        let call = self.call("turn", |_| true).await;
        for chunk in chunks {
            self.reply
                .send(&call.call_id, CallReplyEvent::Value { data: (*chunk).into() }, None)
                .unwrap();
        }
        self.reply.send(&call.call_id, CallReplyEvent::End, None).unwrap();
        call.args[0].clone()
    }
}

struct Rig {
    url: String,
    state: Arc<remotemedia_webrtc::signaling::websocket::SharedState>,
}

impl Rig {
    async fn start(turn: Value, members: &[&str]) -> Self {
        let state = shared_state(manifest(turn), WebRtcTransportConfig::default());
        state.runner.registry().write().await.register(Arc::new(TtsFactory));
        let tickets: Vec<(String, _)> = members
            .iter()
            .map(|id| (format!("t-{id}"), member(id)))
            .collect();
        let tickets: Vec<(&str, _)> = tickets.iter().map(|(ticket, peer)| (ticket.as_str(), peer.clone())).collect();
        state.set_admission(Tickets::new(&tickets));
        remotemedia_realtime::mount(&state, RealtimeConfig::default());
        let addr = listen(state.clone()).await;
        Self {
            url: format!("ws://{addr}/realtime"),
            state,
        }
    }

    async fn connect(&self, member: &str) -> Client {
        let mut client = connect_url(
            &self.url,
            &["remotemedia.realtime.v1", &format!("remotemedia.ticket.t-{member}")],
        )
        .await
        .expect("admitted");
        assert_eq!(client.next().await["type"], "session-created");
        client
    }

    async fn control(&self) -> Arc<SessionControl> {
        let room = self.state.rooms.lock().await.get("room").cloned().expect("the room's session");
        self.state.runner.control_bus().get(room.session_id()).expect("session control")
    }
}

/// `member` starts speaking, as the VAD node tells the turn node.
async fn speech_start(control: &SessionControl, member: &str) {
    control
        .publish_scoped_with_metadata(
            &ControlAddress::node_in("turn"),
            RuntimeData::Json(json!({ "has_speech": true, "is_speech_start": true, "is_speech_end": false })),
            &RpcCallScope::default(),
            HashMap::from([
                ("participant.id".to_string(), member.to_string()),
                ("participant.role".to_string(), "client".to_string()),
            ]),
        )
        .await
        .unwrap();
}

/// Frames of a connection, with what was played so far.
struct Listener {
    client: Client,
    frames: Vec<Value>,
    played_ms: f64,
}

impl Listener {
    fn new(client: Client) -> Self {
        Self {
            client,
            frames: Vec::new(),
            played_ms: 0.0,
        }
    }

    /// Read until `done` holds for a frame. Audio is counted, not kept.
    async fn until(&mut self, what: &str, done: impl Fn(&Value, f64) -> bool) -> Value {
        self.until_all(what, |frame, played, _| done(frame, played)).await
    }

    /// [`Self::until`], with every frame kept so far.
    async fn until_all(&mut self, what: &str, done: impl Fn(&Value, f64, &[Value]) -> bool) -> Value {
        let deadline = Instant::now() + WAIT;
        loop {
            let frame = self
                .client
                .next_before(deadline)
                .await
                .unwrap_or_else(|| panic!("{what} in time; got {:?}", self.types()));
            if frame["type"] == "audio-delta" {
                self.played_ms += delta_ms(&frame, 24_000);
            } else {
                self.frames.push(frame.clone());
            }
            if done(&frame, self.played_ms, &self.frames) {
                return frame;
            }
        }
    }

    fn types(&self) -> Vec<String> {
        self.frames
            .iter()
            .map(|frame| frame["type"].as_str().unwrap_or_default().to_string())
            .collect()
    }

    fn of(&self, kind: &str) -> Vec<&Value> {
        self.frames.iter().filter(|frame| frame["type"] == kind).collect()
    }
}

async fn typed(client: &mut Client, text: &str) {
    client
        .send(json!({
            "type": "conversation-item-create",
            "item": { "type": "text-message", "role": "user", "text": text },
        }))
        .await;
    client.send(json!({ "type": "response-create" })).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interruption_gives_the_settlement_and_the_final_transcript_the_same_text() {
    let rig = Rig::start(json!({}), &["alice"]).await;
    let mut alice = Listener::new(rig.connect("alice").await);
    let control = rig.control().await;
    let mut host = Host::new(&control);

    // Typed text starts a turn (`conversation-item-create`, `response-create`).
    typed(&mut alice.client, "count to three").await;
    let request = host.answer(&["One. ", "Two. ", "Three. "]).await;
    assert_eq!(request["transcript"], "count to three");
    assert_eq!(request["member"]["id"], "alice");
    let turn = request["turn_id"].as_str().unwrap().to_string();
    let item = format!("{turn}:assistant");

    // Two chunks (1200 ms) have been sent to the browser and their text
    // followed; the third is still on its way.
    alice
        .until_all("1300 ms of audio and the second transcript delta", |_, played, frames| {
            played >= 1300.0
                && frames
                    .iter()
                    .any(|frame| frame["type"] == "audio-transcript-delta" && frame["delta"] == "Two. ")
        })
        .await;
    assert!(alice.of("audio-transcript-delta").len() == 2, "{:?}", alice.types());
    // Typed text is not reported back as a transcription: the hook that
    // sent it already shows the user message.
    assert_eq!(alice.types()[..2], ["response-created", "audio-transcript-delta"]);
    assert!(alice.of("input-transcription-completed").is_empty());
    assert_eq!(alice.frames[0]["responseId"], turn.as_str());

    // Barge-in. The browser had played 700 ms: only "One. " (0 to 600 ms)
    // was heard whole, although "Two. " had been released to it.
    speech_start(&control, "alice").await;
    alice
        .client
        .send(json!({
            "type": "conversation-item-truncate", "itemId": item, "contentIndex": 0, "audioEndMs": 700,
        }))
        .await;

    let done = alice
        .until("response-done", |frame, _| frame["type"] == "response-done")
        .await;
    assert_eq!(done["status"], "cancelled");
    assert_eq!(done["responseId"], turn.as_str());
    let transcript = alice.of("audio-transcript-done");
    assert_eq!(transcript.len(), 1);
    assert_eq!(transcript[0]["itemId"], item.as_str());
    let final_text = transcript[0]["transcript"].as_str().unwrap().to_string();

    let settled = host.call("turn_settled", |_| true).await.args[0].clone();
    assert_eq!(settled["turn_id"], turn.as_str());
    assert_eq!(settled["interrupted"], true);
    // The same text in the settlement and in the browser's final transcript,
    // and it is the text up to the truncation point.
    assert_eq!(settled["spoken"], final_text.as_str());
    assert_eq!(settled["heard"]["alice"], final_text.as_str());
    assert_eq!(final_text, "One. ");
    // The flush stopped the audio: far less than the 1800 ms was sent.
    assert!(alice.played_ms < 1700.0, "sent {} ms", alice.played_ms);

    // The next turn's request repeats it as `previous`.
    typed(&mut alice.client, "go on").await;
    let request = host.answer(&["Four."]).await;
    assert_eq!(request["previous"]["spoken"], "One. ");
    assert_eq!(request["previous"]["interrupted"], true);
    // The whole reply plays out: completed, with the whole text.
    let done = alice
        .until("the second response-done", |frame, _| {
            frame["type"] == "response-done" && frame["responseId"] != turn.as_str()
        })
        .await;
    assert_eq!(done["status"], "completed");
    let transcript = alice.of("audio-transcript-done");
    assert_eq!(transcript.last().unwrap()["transcript"], "Four.");
    let settled = host.call("turn_settled", |settled| settled["turn_id"] != turn.as_str()).await.args[0].clone();
    assert_eq!(settled["spoken"], "Four.");
    assert_eq!(settled["interrupted"], false);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn speaker_scope_cancels_only_the_response_of_the_member_who_spoke() {
    let rig = Rig::start(
        json!({ "reply_to": "everyone", "ui": "all", "barge_in": { "scope": "speaker" } }),
        &["alice", "bob"],
    )
    .await;
    let mut alice = Listener::new(rig.connect("alice").await);
    let mut bob = Listener::new(rig.connect("bob").await);
    let control = rig.control().await;
    let ready_by = Instant::now() + WAIT;
    while !(control.is_media_ready("alice") && control.is_media_ready("bob")) {
        assert!(Instant::now() < ready_by, "both members are media-ready");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut host = Host::new(&control);

    typed(&mut alice.client, "tell us").await;
    let request = host.answer(&["One. ", "Two. ", "Three. "]).await;
    let turn = request["turn_id"].as_str().unwrap().to_string();

    // Both hear the reply: one response each.
    bob.until("bob's first audio", |_, played| played >= 700.0).await;
    assert_eq!(bob.of("response-created")[0]["responseId"], turn.as_str());
    // The user part is alice's alone.
    assert!(bob.of("input-transcription-completed").is_empty());

    // bob speaks. No truncation arrives: all audio released to him counts.
    speech_start(&control, "bob").await;
    let done = bob.until("bob's response-done", |frame, _| frame["type"] == "response-done").await;
    assert_eq!(done["status"], "cancelled");
    let bob_text = bob.of("audio-transcript-done")[0]["transcript"].as_str().unwrap().to_string();
    assert!(bob_text == "One. " || bob_text == "One. Two. ", "bob heard {bob_text:?}");

    // alice's response goes on to the end.
    let done = alice.until("alice's response-done", |frame, _| frame["type"] == "response-done").await;
    assert_eq!(done["status"], "completed");
    assert_eq!(alice.of("audio-transcript-done")[0]["transcript"], "One. Two. Three. ");
    assert!(alice.played_ms > 1700.0, "alice got the whole reply: {} ms", alice.played_ms);
    // alice typed her turn, so nothing reports it back to her either.
    assert!(alice.of("input-transcription-completed").is_empty());

    // The settlement: not interrupted, and each member's own text.
    let settled = host.call("turn_settled", |_| true).await.args[0].clone();
    assert_eq!(settled["interrupted"], false);
    assert_eq!(settled["spoken"], "One. Two. Three. ");
    assert_eq!(settled["heard"]["alice"], "One. Two. Three. ");
    assert_eq!(settled["heard"]["bob"], bob_text.as_str());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_tool_round_trip_reaches_the_host_as_event_calls() {
    let rig = Rig::start(json!({}), &["alice"]).await;
    let mut alice = Listener::new(rig.connect("alice").await);
    let control = rig.control().await;
    let mut host = Host::new(&control);

    typed(&mut alice.client, "show me Oslo").await;
    let call = host.call("turn", |_| true).await;
    let turn = call.args[0]["turn_id"].as_str().unwrap().to_string();
    // The host's client-tool-call chunk (clients/ai/src/voice/client-tools.ts).
    let tool_call = json!({
        "type": "data-client-tool-call", "id": "call-1", "transient": true,
        "data": { "callId": "call-1", "name": "showMap", "arguments": "{\"place\":\"Oslo\"}" },
    });
    host.reply
        .send(&call.call_id, CallReplyEvent::Value { data: "".into() }, Some(json!({ "ui": tool_call })))
        .unwrap();

    let done = alice.until("the first response-done", |frame, _| frame["type"] == "response-done").await;
    assert_eq!((done["responseId"].as_str(), done["status"].as_str()), (Some(turn.as_str()), Some("completed")));
    let call_done = alice.of("function-call-arguments-done")[0].clone();
    assert_eq!(call_done["callId"], "call-1");
    assert_eq!(call_done["name"], "showMap");
    assert_eq!(call_done["arguments"], "{\"place\":\"Oslo\"}");

    // What the hook does next: the tool output, then `response-create`.
    alice
        .client
        .send(json!({
            "type": "conversation-item-create",
            "item": { "type": "function-call-output", "callId": "call-1", "output": "{\"shown\":true}" },
        }))
        .await;
    alice.client.send(json!({ "type": "response-create" })).await;
    let output = host.call("event", |event| event["type"] == "client_tool_output").await.args[0].clone();
    assert_eq!(
        output,
        json!({ "type": "client_tool_output", "turn_id": turn, "call_id": "call-1", "output": "{\"shown\":true}" })
    );
    let create = host.call("event", |event| event["type"] == "response_create").await.args[0].clone();
    assert_eq!(create, json!({ "type": "response_create", "turn_id": turn }));

    // The host releases the continuation: a second response of the turn.
    host.reply
        .send(&call.call_id, CallReplyEvent::Value { data: "Here is Oslo.".into() }, None)
        .unwrap();
    host.reply.send(&call.call_id, CallReplyEvent::End, None).unwrap();
    let second = format!("{turn}#2");
    let done = alice
        .until("the second response-done", |frame, _| {
            frame["type"] == "response-done" && frame["responseId"] == second.as_str()
        })
        .await;
    assert_eq!(done["status"], "completed");
    assert!(alice.of("response-created").iter().any(|frame| frame["responseId"] == second.as_str()));
    let transcript = alice.of("audio-transcript-done");
    assert_eq!(transcript.last().unwrap()["transcript"], "Here is Oslo.");
    assert_eq!(transcript.last().unwrap()["itemId"], format!("{second}:assistant").as_str());
    assert!((550.0..=620.0).contains(&alice.played_ms), "one chunk of audio: {} ms", alice.played_ms);
}
