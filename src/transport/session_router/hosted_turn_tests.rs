//! `HostedTurnNode` in a real session (openspec: add-ai-sdk-voice-library,
//! tasks 3.1 to 3.8): the node runs under the session router, its turns are
//! answered by an in-process host through `SessionControl::host_endpoint`,
//! and what a transport would do (media readiness, release reports, flushes)
//! is driven through the session bus.
//!
//! The pipeline is `turn -> tts`, `turn.ui -> ui_out`. `tts` stands in for a
//! TTS node and marks the text it receives, so a frame that took the wrong
//! connection shows.

use super::{tests::create_test_manifest, *};
use crate::nodes::hosted_turn::{HostedTurnNode, HostedTurnNodeFactory, HOSTED_TURN_NODE};
use crate::nodes::{AsyncNodeWrapper, AsyncStreamingNode};
use crate::transport::endpoint::{EndpointHost, EndpointReplier, EndpointSpec, HostEvent, HostKind, IncomingCall};
use crate::transport::session_control::{ControlAddress, SessionControl};
use crate::transport::turn_audio::{AudioReleased, FlushTarget};
use remotemedia_traits::call::{CallErrorKind, CallReplyEvent};
use remotemedia_traits::rpc::RpcMode;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const ENDPOINT: &str = "agent";
const WAIT: Duration = Duration::from_secs(3);

/// Stands in for TTS: marks the text it was given.
struct Tts;

#[async_trait::async_trait]
impl AsyncStreamingNode for Tts {
    fn node_type(&self) -> &str {
        "Tts"
    }
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        Ok(match data {
            RuntimeData::Text(text) => RuntimeData::Text(format!("tts:{text}")),
            other => other,
        })
    }
}

struct Passthrough;

#[async_trait::async_trait]
impl AsyncStreamingNode for Passthrough {
    fn node_type(&self) -> &str {
        "Passthrough"
    }
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        Ok(data)
    }
}

/// One output of the session, as a client would receive it.
#[derive(Debug, Clone)]
enum Out {
    /// Text that reached the TTS stand-in.
    Speech { text: String, meta: HashMap<String, String> },
    /// A frame that reached the `ui` sink.
    Ui { frame: Value, meta: HashMap<String, String> },
}

struct Rig {
    node: Arc<HostedTurnNode>,
    control: Arc<SessionControl>,
    input: crate::transport::SessionPacketSender,
    out: mpsc::Receiver<TransportData>,
    flushes: mpsc::Receiver<FlushTarget>,
    replies: tokio::sync::broadcast::Receiver<RuntimeData>,
    corr: u64,
    _shutdown: mpsc::Sender<()>,
    _running: JoinHandle<Result<()>>,
}

async fn rig(mut params: Value) -> Rig {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let session = format!(
        "hosted-turn-{}",
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    params["endpoint"] = ENDPOINT.into();
    let mut manifest = create_test_manifest(
        vec![("turn", HOSTED_TURN_NODE), ("tts", "Tts"), ("ui_out", "Passthrough")],
        vec![("turn", "tts"), ("turn", "ui_out")],
    );
    manifest.connections[1].from_port = Some("ui".into());
    let mut registry = StreamingNodeRegistry::new();
    registry.register(Arc::new(HostedTurnNodeFactory));
    let (output, receivers) = ClientOutputRouter::new(1024);
    let (mut router, shutdown) =
        SessionRouter::new(session, Arc::new(manifest), Arc::new(registry), output).unwrap();
    let control = SessionControl::new(router.session_id.clone());
    router.attach_control(control.clone()).await;
    let config = serde_json::from_value(params).unwrap();
    let node = Arc::new(HostedTurnNode::new("turn", config));
    router
        .cached_nodes
        .insert("turn".into(), Box::new(AsyncNodeWrapper(node.clone())));
    router
        .cached_nodes
        .insert("tts".into(), Box::new(AsyncNodeWrapper(Arc::new(Tts))));
    router
        .cached_nodes
        .insert("ui_out".into(), Box::new(AsyncNodeWrapper(Arc::new(Passthrough))));
    let (flush_tx, flushes) = mpsc::channel(64);
    control.install_flush_audio_for_hook(flush_tx);
    let replies = control
        .subscribe(&ControlAddress::node_out("turn").with_port("__reply__"))
        .unwrap();
    let rig = Rig {
        node,
        input: router.get_input_sender(),
        control,
        out: receivers.data_rx,
        flushes,
        replies,
        corr: 0,
        _shutdown: shutdown,
        _running: tokio::spawn(router.run_after_init()),
    };
    // The session is up once the node's body runs.
    tokio::time::timeout(WAIT, async {
        while !rig.node.is_running() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the session starts");
    rig
}

fn member(id: &str, role: Option<&str>) -> HashMap<String, String> {
    let mut meta = HashMap::from([
        ("participant.id".to_string(), id.to_string()),
        ("participant.role".to_string(), "client".to_string()),
        ("participant.display_name".to_string(), format!("{id}!")),
    ]);
    if let Some(role) = role {
        meta.insert("participant.metadata.role".to_string(), role.to_string());
    }
    meta
}

impl Rig {
    async fn send(&self, data: RuntimeData, metadata: HashMap<String, String>) {
        self.input
            .send(DataPacket {
                data,
                from_node: "client".into(),
                to_node: None,
                session_id: self.control.session_id().to_string(),
                sequence: 0,
                sub_sequence: 0,
                metadata,
            })
            .await
            .unwrap();
    }

    /// A final transcript of `who`.
    async fn transcript(&self, who: &str, text: &str) {
        self.send(RuntimeData::Text(text.into()), member(who, None)).await;
    }

    /// The accumulator's control message at a speech start, as it reaches
    /// the node through the transcription node. `retract`: the speech
    /// continues `who`'s last utterance, which is emitted again, merged.
    async fn cancel_speculation(&self, who: &str, retract: bool) {
        self.send(
            RuntimeData::ControlMessage {
                message_type: crate::data::ControlMessageType::CancelSpeculation {
                    from_timestamp: 0,
                    to_timestamp: 0,
                },
                segment_id: None,
                timestamp_ms: 0,
                metadata: if retract { json!({ "retract_utterance": true }) } else { Value::Null },
            },
            member(who, None),
        )
        .await;
    }

    /// `who` started speaking, as the VAD node reports it.
    async fn speech_start(&self, who: &HashMap<String, String>) {
        self.send(
            RuntimeData::Json(json!({
                "has_speech": true, "is_speech_start": true, "is_speech_end": false,
            })),
            who.clone(),
        )
        .await;
    }

    fn host(&self) -> Host {
        let spec = EndpointSpec::new(ENDPOINT, HostKind::Service)
            .method("turn", RpcMode::Stream)
            .method("turn_settled", RpcMode::FireAndForget)
            .method("event", RpcMode::FireAndForget);
        let host = self.control.host_endpoint(spec, true).unwrap();
        Host {
            reply: host.replier(),
            host,
            seen: VecDeque::new(),
        }
    }

    async fn next(&mut self) -> Out {
        let data = tokio::time::timeout(WAIT, self.out.recv())
            .await
            .expect("an output in time")
            .expect("the session runs");
        Self::out(data)
    }

    fn out(data: TransportData) -> Out {
        match data.data {
            RuntimeData::Text(text) => Out::Speech {
                text,
                meta: data.metadata,
            },
            RuntimeData::Json(frame) => Out::Ui {
                frame,
                meta: data.metadata,
            },
            other => panic!("unexpected output {}", other.data_type()),
        }
    }

    /// Outputs up to and including the first `ui` chunk of type `last`
    /// (with `status`, for a `data-turn`).
    async fn until(&mut self, last: &str, status: Option<&str>) -> Vec<Out> {
        let mut seen = Vec::new();
        loop {
            let out = self.next().await;
            let done = matches!(&out, Out::Ui { frame, .. }
                if frame["chunk"]["type"] == last
                    && status.is_none_or(|status| frame["chunk"]["data"]["status"] == status));
            seen.push(out);
            if done {
                break;
            }
        }
        // `tts` and the `ui` sink are separate outputs of the session: what
        // was produced before the frame may arrive just after it.
        while let Ok(Some(data)) = tokio::time::timeout(Duration::from_millis(100), self.out.recv()).await {
            seen.push(Self::out(data));
        }
        seen
    }

    /// Outputs until the turn is back to `listening`.
    async fn until_listening(&mut self) -> Vec<Out> {
        self.until("data-turn", Some("listening")).await
    }

    async fn speech(&mut self) -> (String, HashMap<String, String>) {
        loop {
            if let Out::Speech { text, meta } = self.next().await {
                return (text, meta);
            }
        }
    }

    /// Nothing more comes out for a moment.
    async fn quiet(&mut self) {
        if let Ok(Some(out)) = tokio::time::timeout(Duration::from_millis(150), self.out.recv()).await {
            panic!("unexpected output: {:?} {:?}", out.data, out.metadata);
        }
    }

    /// A typed-RPC call on the node, as `ctrl.turn.<method>(arg)` makes it.
    async fn rpc(&mut self, method: &str, arg: Option<Value>) -> std::result::Result<Value, String> {
        self.corr += 1;
        let corr = format!("c{}", self.corr);
        self.control
            .publish(
                &ControlAddress::node_in("turn").with_port(method),
                RuntimeData::Json(json!({
                    "corr_id": corr, "args": arg.into_iter().collect::<Vec<_>>(), "kwargs": {},
                })),
            )
            .await
            .unwrap();
        loop {
            let reply = tokio::time::timeout(WAIT, self.replies.recv())
                .await
                .expect("an RPC reply in time")
                .unwrap();
            let RuntimeData::Json(reply) = reply else { continue };
            let payload = &reply["payload"];
            if payload["corr_id"] != corr.as_str() {
                continue;
            }
            return match payload["kind"].as_str() {
                Some("value") => Ok(payload["data"].clone()),
                Some("error") => Err(payload["data"].as_str().unwrap_or_default().to_string()),
                other => panic!("unexpected RPC reply kind {other:?}: {reply}"),
            };
        }
    }

    async fn flush(&mut self) -> FlushTarget {
        tokio::time::timeout(WAIT, self.flushes.recv())
            .await
            .expect("a targeted flush in time")
            .unwrap()
    }

    /// A release report of `who`'s sender, as a transport publishes it.
    fn released(&self, who: &str, turn: &str, chunks_released: u64, chunk: u64, flushed: bool, drained: bool) {
        self.control.publish_audio_released(AudioReleased {
            member: who.into(),
            turn_id: turn.into(),
            released_ms: 0,
            chunks_released,
            chunk,
            flushed,
            drained,
            stream_id: None,
        });
    }
}

/// The in-process host of the agent's endpoint.
struct Host {
    host: EndpointHost,
    reply: EndpointReplier,
    seen: VecDeque<HostEvent>,
}

impl Host {
    async fn event(&mut self) -> HostEvent {
        tokio::time::timeout(WAIT, self.host.next())
            .await
            .expect("a call in time")
            .expect("still hosting")
    }

    /// The next call of `method`; other work is kept for later.
    async fn call(&mut self, method: &str) -> IncomingCall {
        let at = self
            .seen
            .iter()
            .position(|event| matches!(event, HostEvent::Call(call) if call.method == method));
        if let Some(HostEvent::Call(call)) = at.and_then(|at| self.seen.remove(at)) {
            return call;
        }
        loop {
            match self.event().await {
                HostEvent::Call(call) if call.method == method => return call,
                other => self.seen.push_back(other),
            }
        }
    }

    async fn turn(&mut self) -> (String, Value) {
        let call = self.call("turn").await;
        assert_eq!(call.mode, RpcMode::Stream);
        (call.call_id, call.args[0].clone())
    }

    async fn settled(&mut self) -> Value {
        self.call("turn_settled").await.args[0].clone()
    }

    /// The next `event` call of `kind`.
    async fn host_event(&mut self, kind: &str) -> Value {
        loop {
            let event = self.call("event").await.args[0].clone();
            if event["type"] == kind {
                return event;
            }
        }
    }

    /// The cancellation of `call_id`.
    async fn cancelled(&mut self, call_id: &str) {
        let at = self
            .seen
            .iter()
            .position(|event| matches!(event, HostEvent::Cancelled { call_id: id, .. } if id == call_id));
        if at.and_then(|at| self.seen.remove(at)).is_some() {
            return;
        }
        loop {
            match self.event().await {
                HostEvent::Cancelled { call_id: id, .. } if id == call_id => return,
                other => self.seen.push_back(other),
            }
        }
    }

    fn text(&self, call: &str, text: &str) {
        self.reply
            .send(call, CallReplyEvent::Value { data: text.into() }, None)
            .unwrap();
    }

    fn meta(&self, call: &str, meta: Value) -> std::result::Result<(), CallErrorKind> {
        self.reply
            .send(call, CallReplyEvent::Value { data: "".into() }, Some(meta))
    }

    fn end(&self, call: &str) {
        self.reply.send(call, CallReplyEvent::End, None).unwrap();
    }

    /// Answer a whole turn with `chunks`.
    async fn answer(&mut self, chunks: &[&str]) -> Value {
        let (call, request) = self.turn().await;
        for chunk in chunks {
            self.text(&call, chunk);
        }
        self.end(&call);
        request
    }
}

fn ui_types(outs: &[Out]) -> Vec<String> {
    outs.iter()
        .filter_map(|out| match out {
            Out::Ui { frame, .. } => {
                let chunk = &frame["chunk"];
                let kind = chunk["type"].as_str().unwrap_or_default();
                Some(match chunk["data"]["status"].as_str() {
                    Some(status) if kind == "data-turn" => format!("data-turn:{status}"),
                    _ => kind.to_string(),
                })
            }
            Out::Speech { .. } => None,
        })
        .collect()
}

fn speech(outs: &[Out]) -> Vec<String> {
    outs.iter()
        .filter_map(|out| match out {
            Out::Speech { text, .. } => Some(text.clone()),
            Out::Ui { .. } => None,
        })
        .collect()
}

fn ui_chunk<'a>(outs: &'a [Out], kind: &str) -> &'a Value {
    outs.iter()
        .find_map(|out| match out {
            Out::Ui { frame, .. } if frame["chunk"]["type"] == kind => Some(&frame["chunk"]),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no {kind} chunk in {outs:?}"))
}

// -- 3.1, 3.4: a streamed turn ---------------------------------------------

#[tokio::test]
async fn a_turn_streams_text_downstream_and_frames_on_ui() {
    let mut rig = rig(json!({})).await;
    let mut host = rig.host();
    rig.transcript("alice", "What's up?").await;

    let (call, request) = host.turn().await;
    let turn = request["turn_id"].as_str().unwrap().to_string();
    assert_eq!(request["transcript"], "What's up?");
    assert_eq!(
        request["member"],
        json!({ "id": "alice", "name": "alice!" })
    );
    assert_eq!(request["session"]["id"], rig.control.session_id());
    assert_eq!(request["messages"], json!([]));
    assert!(request.get("previous").is_none());

    // The first chunk comes out before the host has sent the last.
    host.text(&call, "Not ");
    let (first, meta) = rig.speech().await;
    assert_eq!(first, "tts:Not ");
    assert_eq!(meta["participant.turn_id"], turn);
    assert_eq!(meta["participant.chunk"], "0");
    assert_eq!(meta["participant.to"], "alice");
    assert_eq!(meta["participant.id"], "alice");
    host.text(&call, "much");
    host.text(&call, ".");
    host.end(&call);
    let rest = rig.until_listening().await;
    assert_eq!(speech(&rest), ["tts:much", "tts:."]);
    let chunks: Vec<_> = rest
        .iter()
        .filter_map(|out| match out {
            Out::Speech { meta, .. } => Some(meta["participant.chunk"].clone()),
            _ => None,
        })
        .collect();
    assert_eq!(chunks, ["1", "2"]);

    let settled = host.settled().await;
    assert_eq!(
        settled,
        json!({ "turn_id": turn, "spoken": "Not much.", "interrupted": false,
                "heard": { "alice": "Not much." } })
    );
    rig.quiet().await;
}

#[tokio::test]
async fn the_ui_port_carries_the_wire_contract_in_order() {
    let mut rig = rig(json!({})).await;
    let mut host = rig.host();
    rig.transcript("alice", "Hi").await;
    let (call, request) = host.turn().await;
    let turn = request["turn_id"].as_str().unwrap().to_string();
    host.text(&call, "One ");
    host.text(&call, "two ");
    // The host attaches a chunk between its second and third text chunks.
    host.meta(&call, json!({ "ui": { "type": "data-citations", "data": [1, 2] } }))
        .unwrap();
    host.text(&call, "three");
    host.end(&call);
    let outs = rig.until_listening().await;

    assert_eq!(
        ui_types(&outs),
        [
            "data-transcript",
            "data-turn:thinking",
            "start",
            "data-turn:speaking",
            "text-start",
            "text-delta",
            "text-delta",
            "data-citations",
            "text-delta",
            "text-end",
            "finish",
            "data-turn:listening",
        ]
    );
    // Text reached TTS only, frames reached the `ui` sink only.
    assert_eq!(speech(&outs), ["tts:One ", "tts:two ", "tts:three"]);
    for out in &outs {
        let Out::Ui { frame, meta } = out else { continue };
        assert_eq!(frame["v"], 1);
        assert_eq!(frame["turn"], turn.as_str());
        assert!(frame.get("__aux_port__").is_none(), "the sink gets the frame itself");
        assert_eq!(meta["participant.to"], "alice", "addressed to the turn's member");
        assert!(!meta.contains_key("participant.turn_id"));
    }
    assert_eq!(
        ui_chunk(&outs, "data-transcript"),
        &json!({ "type": "data-transcript", "id": format!("{turn}:user"),
                 "data": { "text": "Hi", "source": "speech", "member": { "id": "alice", "name": "alice!" } } })
    );
    assert_eq!(
        ui_chunk(&outs, "data-turn"),
        &json!({ "type": "data-turn", "id": turn, "data": { "status": "thinking" }, "transient": true })
    );
    assert_eq!(
        ui_chunk(&outs, "start"),
        &json!({ "type": "start", "messageId": format!("{turn}:assistant") })
    );
    assert_eq!(
        ui_chunk(&outs, "text-start"),
        &json!({ "type": "text-start", "id": format!("{turn}:text") })
    );
    assert_eq!(
        ui_chunk(&outs, "text-delta"),
        &json!({ "type": "text-delta", "id": format!("{turn}:text"), "delta": "One " })
    );
    assert_eq!(ui_chunk(&outs, "data-citations"), &json!({ "type": "data-citations", "data": [1, 2] }));
    assert_eq!(ui_chunk(&outs, "finish"), &json!({ "type": "finish" }));
}

#[tokio::test]
async fn ui_all_addresses_frames_to_everyone() {
    let mut rig = rig(json!({ "ui": "all", "reply_to": "everyone" })).await;
    let mut host = rig.host();
    rig.transcript("alice", "Hi").await;
    host.answer(&["Hello."]).await;
    for out in rig.until_listening().await {
        match out {
            Out::Ui { meta, .. } => assert!(!meta.contains_key("participant.to")),
            Out::Speech { meta, .. } => assert!(!meta.contains_key("participant.to")),
        }
    }
}

// -- 3.7: typed text -----------------------------------------------------------

#[tokio::test]
async fn typed_text_starts_a_turn_like_a_transcript() {
    let mut rig = rig(json!({})).await;
    let mut host = rig.host();
    rig.send(
        RuntimeData::Json(json!({ "v": 1, "type": "text", "text": "typed question" })),
        member("alice", None),
    )
    .await;
    let request = host.answer(&["Typed answer."]).await;
    assert_eq!(request["transcript"], "typed question");
    assert_eq!(request["member"]["id"], "alice");
    let outs = rig.until_listening().await;
    assert_eq!(ui_chunk(&outs, "data-transcript")["data"]["source"], "text");
    assert_eq!(speech(&outs), ["tts:Typed answer."]);
}

// -- 3.2: history, settlement, `previous` ----------------------------------------

#[tokio::test]
async fn node_mode_sends_the_conversation_and_the_last_settlement() {
    let mut rig = rig(json!({})).await;
    let mut host = rig.host();
    rig.transcript("alice", "First?").await;
    host.answer(&["First ", "answer."]).await;
    rig.until_listening().await;
    let settled = host.settled().await;

    rig.transcript("alice", "Second?").await;
    let request = host.answer(&["Second answer."]).await;
    assert_eq!(
        request["messages"],
        json!([
            { "role": "user", "content": "First?" },
            { "role": "assistant", "content": "First answer." }
        ])
    );
    // The last settlement is repeated, so a lost `turn_settled` is harmless.
    assert_eq!(request["previous"], settled);
    rig.until_listening().await;
    assert_eq!(
        rig.rpc("get_history", None).await.unwrap(),
        json!([
            { "role": "user", "content": "First?" },
            { "role": "assistant", "content": "First answer." },
            { "role": "user", "content": "Second?" },
            { "role": "assistant", "content": "Second answer." }
        ])
    );
}

#[tokio::test]
async fn host_mode_keeps_no_history_and_still_settles() {
    let mut rig = rig(json!({ "history": "host" })).await;
    let mut host = rig.host();
    rig.transcript("alice", "First?").await;
    let request = host.answer(&["Answer."]).await;
    assert!(request.get("messages").is_none());
    rig.until_listening().await;
    let settled = host.settled().await;
    assert_eq!(settled["spoken"], "Answer.");

    rig.transcript("alice", "Second?").await;
    let request = host.answer(&["Again."]).await;
    assert!(request.get("messages").is_none());
    assert_eq!(request["previous"], settled);
    rig.until_listening().await;
    assert_eq!(rig.rpc("get_history", None).await.unwrap(), json!([]));
    let refused = rig
        .rpc("set_history", Some(json!([{ "role": "user", "content": "x" }])))
        .await
        .unwrap_err();
    assert!(refused.contains("kept by the host"), "{refused}");
}

#[tokio::test]
async fn an_interrupted_reply_is_cut_at_what_was_released() {
    let mut rig = rig(json!({})).await;
    let mut host = rig.host();
    // Alice's transport reports what it releases.
    rig.control.publish_media_ready("alice", "webrtc", "c1");
    rig.transcript("alice", "Tell me").await;
    let (call, request) = host.turn().await;
    let turn = request["turn_id"].as_str().unwrap().to_string();
    host.text(&call, "One. ");
    host.text(&call, "Two. ");
    host.text(&call, "Three.");
    for _ in 0..3 {
        rig.speech().await;
    }
    // Chunk 0 has wholly left; chunk 1 is being sent.
    rig.released("alice", &turn, 1, 1, false, false);

    rig.speech_start(&member("alice", None)).await;
    host.cancelled(&call).await;
    assert_eq!(
        rig.flush().await,
        FlushTarget { members: vec!["alice".into()], turn_id: turn.clone(), drop_rest: true }
    );
    // The host is not heard after the cut.
    assert!(host.meta(&call, json!({})).is_err());
    rig.released("alice", &turn, 1, 1, true, false);

    let outs = rig.until("data-turn", Some("interrupted")).await;
    assert!(speech(&outs).is_empty(), "nothing more of the turn is emitted");
    assert_eq!(ui_types(&outs), ["abort", "data-turn:interrupted"]);
    assert_eq!(
        ui_chunk(&outs, "data-turn")["data"],
        json!({ "status": "interrupted", "spoken": "One. " })
    );
    let settled = host.settled().await;
    assert_eq!(
        settled,
        json!({ "turn_id": turn, "spoken": "One. ", "interrupted": true, "heard": { "alice": "One. " } })
    );

    rig.transcript("alice", "Go on").await;
    let request = host.answer(&["Ok."]).await;
    assert_eq!(
        request["messages"],
        json!([
            { "role": "user", "content": "Tell me" },
            { "role": "assistant", "content": "One. ", "interrupted": true }
        ])
    );
    assert_eq!(request["previous"], settled);
}

#[tokio::test]
async fn a_reply_played_to_the_end_settles_when_the_sender_says_so() {
    let mut rig = rig(json!({})).await;
    let mut host = rig.host();
    rig.control.publish_media_ready("alice", "webrtc", "c1");
    rig.transcript("alice", "Hi").await;
    let (call, request) = host.turn().await;
    let turn = request["turn_id"].as_str().unwrap().to_string();
    host.text(&call, "One. ");
    host.text(&call, "Two.");
    host.end(&call);
    let outs = rig.until("text-delta", None).await;
    assert!(!ui_types(&outs).contains(&"finish".to_string()));
    // The reply ended, but its audio is still playing: not settled yet.
    rig.released("alice", &turn, 1, 1, false, false);
    assert!(
        tokio::time::timeout(Duration::from_millis(200), host.settled()).await.is_err(),
        "settled before the audio was released"
    );
    // The end mark of the last chunk.
    rig.released("alice", &turn, 2, 1, false, false);
    assert_eq!(host.settled().await["spoken"], "One. Two.");
    assert!(ui_types(&rig.until_listening().await).ends_with(&["finish".into(), "data-turn:listening".into()]));
}

#[tokio::test]
async fn a_drained_sender_ends_the_turn_without_an_end_mark() {
    let mut rig = rig(json!({ "drain_quiet_ms": 100 })).await;
    let mut host = rig.host();
    rig.control.publish_media_ready("alice", "webrtc", "c1");
    rig.transcript("alice", "Hi").await;
    let (call, request) = host.turn().await;
    let turn = request["turn_id"].as_str().unwrap().to_string();
    host.text(&call, "One. ");
    host.text(&call, "Two.");
    host.end(&call);
    // TTS grouped both chunks into audio tagged with the first; the sender
    // ran dry and stayed dry.
    rig.released("alice", &turn, 0, 0, false, true);
    let settled = host.settled().await;
    assert_eq!((settled["spoken"].clone(), settled["interrupted"].clone()), (json!("One. Two."), json!(false)));
}

#[tokio::test]
async fn a_sender_that_runs_empty_ahead_of_tts_does_not_end_the_turn() {
    let mut rig = rig(json!({ "drain_quiet_ms": 300 })).await;
    let mut host = rig.host();
    rig.control.publish_media_ready("alice", "webrtc", "c1");
    rig.transcript("alice", "Hi").await;
    let (call, request) = host.turn().await;
    let turn = request["turn_id"].as_str().unwrap().to_string();
    host.text(&call, "One sentence.");
    host.end(&call);
    // The first frame of the reply left the moment it arrived, so the
    // sender is empty in the middle of the only chunk.
    rig.released("alice", &turn, 0, 0, false, true);
    // More of the chunk follows before the quiet period is over.
    tokio::time::sleep(Duration::from_millis(100)).await;
    rig.released("alice", &turn, 0, 0, false, false);
    assert!(
        tokio::time::timeout(Duration::from_millis(450), host.settled()).await.is_err(),
        "the turn settled while its audio was still being sent"
    );
    // Now it has all left, and stays that way.
    rig.released("alice", &turn, 0, 0, false, true);
    let settled = host.settled().await;
    assert_eq!((settled["spoken"].clone(), settled["interrupted"].clone()), (json!("One sentence."), json!(false)));
}

// -- 3.2a: the host overrides the history ------------------------------------------

#[tokio::test]
async fn the_host_reads_and_edits_the_history_by_rpc() {
    let mut rig = rig(json!({})).await;
    let mut host = rig.host();
    rig.transcript("alice", "Long story").await;
    host.answer(&["Indeed."]).await;
    rig.until_listening().await;

    let summary = json!([{ "role": "system", "content": "Summary: a long story was told." }]);
    assert_eq!(rig.rpc("set_history", Some(summary.clone())).await.unwrap(), Value::Null);
    rig.transcript("alice", "And then?").await;
    let request = host.answer(&["The end."]).await;
    assert_eq!(request["messages"], summary);
    rig.until_listening().await;

    rig.rpc("append_history", Some(json!({ "role": "assistant", "content": "Noted.", "interrupted": true })))
        .await
        .unwrap();
    let history = rig.rpc("get_history", None).await.unwrap();
    assert_eq!(history.as_array().unwrap().len(), 4);
    assert_eq!(history[3], json!({ "role": "assistant", "content": "Noted.", "interrupted": true }));
    assert!(rig.rpc("append_history", Some(json!({ "role": "tool", "content": "x" }))).await.is_err());

    rig.rpc("reset_history", None).await.unwrap();
    assert_eq!(rig.rpc("get_history", None).await.unwrap(), json!([]));
}

#[tokio::test]
async fn a_history_in_the_reply_replaces_what_came_before_the_turn() {
    let mut rig = rig(json!({})).await;
    let mut host = rig.host();
    rig.transcript("alice", "One").await;
    host.answer(&["First."]).await;
    rig.until_listening().await;

    rig.transcript("alice", "Two").await;
    let (call, _) = host.turn().await;
    host.text(&call, "Second.");
    host.meta(&call, json!({ "history": [{ "role": "system", "content": "Summary." }] }))
        .unwrap();
    host.end(&call);
    rig.until_listening().await;

    rig.transcript("alice", "Three").await;
    let request = host.answer(&["Third."]).await;
    // Nothing from before the replacement; this turn follows it.
    assert_eq!(
        request["messages"],
        json!([
            { "role": "system", "content": "Summary." },
            { "role": "user", "content": "Two" },
            { "role": "assistant", "content": "Second." }
        ])
    );
}

// -- 3.3: `protect` -------------------------------------------------------------------

#[tokio::test]
async fn a_protected_section_suppresses_barge_in_until_it_ends() {
    let mut rig = rig(json!({})).await;
    let mut host = rig.host();
    rig.transcript("alice", "Book it").await;
    let (call, _) = host.turn().await;
    host.text(&call, "Booking. ");
    host.meta(&call, json!({ "protect": true })).unwrap();
    rig.speech().await;
    // The protect chunk follows the text: wait until the node holds it.
    tokio::time::timeout(WAIT, async {
        while !rig.node.is_protected() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    rig.speech_start(&member("alice", None)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Not cancelled: the host still streams.
    host.text(&call, "Done. ");
    assert_eq!(rig.speech().await.0, "tts:Done. ");

    host.meta(&call, json!({ "protect": false })).unwrap();
    host.text(&call, "Anything else?");
    assert_eq!(rig.speech().await.0, "tts:Anything else?");
    assert!(!rig.node.is_protected());
    rig.speech_start(&member("alice", None)).await;
    host.cancelled(&call).await;
    assert!(host.settled().await["interrupted"].as_bool().unwrap());
}

#[tokio::test]
async fn a_host_killed_while_protected_does_not_leave_barge_in_disabled() {
    let mut rig = rig(json!({ "hold_ms": 0, "fallback": { "line": "Sorry." } })).await;
    let mut host = rig.host();
    rig.transcript("alice", "Book it").await;
    let (call, _) = host.turn().await;
    host.meta(&call, json!({ "protect": true })).unwrap();
    tokio::time::timeout(WAIT, async {
        while !rig.node.is_protected() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();

    // The host process dies mid-tool.
    drop(host);
    let outs = rig.until_listening().await;
    assert!(!rig.node.is_protected(), "the guard outlived its host");
    assert_eq!(ui_chunk(&outs, "error")["errorText"].as_str().map(|t| t.starts_with("responder_gone")), Some(true));
    assert_eq!(speech(&outs), ["tts:Sorry."]);

    // A later barge is not suppressed.
    let mut host = rig.host();
    rig.transcript("alice", "Again").await;
    let (call, _) = host.turn().await;
    host.text(&call, "Working on it");
    rig.speech().await;
    rig.speech_start(&member("alice", None)).await;
    host.cancelled(&call).await;
    assert!(host.settled().await["interrupted"].as_bool().unwrap());
}

#[tokio::test]
async fn protection_ends_with_the_stream_and_with_an_error() {
    let mut rig = rig(json!({})).await;
    let mut host = rig.host();
    for fail in [false, true] {
        rig.transcript("alice", "Go").await;
        let (call, _) = host.turn().await;
        host.meta(&call, json!({ "protect": true })).unwrap();
        host.text(&call, "x");
        rig.speech().await;
        if fail {
            host.reply
                .send(
                    &call,
                    CallReplyEvent::Error { error: CallErrorKind::Error, message: "onTurn threw".into() },
                    None,
                )
                .unwrap();
        } else {
            host.end(&call);
        }
        let outs = rig.until_listening().await;
        assert!(!rig.node.is_protected());
        assert_eq!(ui_types(&outs).contains(&"error".to_string()), fail);
    }
}

// -- 3.5: fallback ---------------------------------------------------------------------

#[tokio::test]
async fn no_host_and_no_local_backend_speaks_the_configured_line() {
    let mut rig = rig(json!({ "hold_ms": 50, "fallback": { "line": "Sorry, I can't answer right now." } })).await;
    rig.transcript("alice", "Anyone?").await;
    let outs = rig.until_listening().await;
    assert_eq!(
        ui_types(&outs),
        [
            "data-transcript",
            "data-turn:thinking",
            "start",
            "error",
            "data-turn:speaking",
            "text-start",
            "text-delta",
            "text-end",
            "finish",
            "data-turn:listening",
        ]
    );
    assert!(ui_chunk(&outs, "error")["errorText"].as_str().unwrap().starts_with("no_responder"));
    assert_eq!(speech(&outs), ["tts:Sorry, I can't answer right now."]);
    // The line was spoken, so it is in the conversation.
    assert_eq!(
        rig.rpc("get_history", None).await.unwrap(),
        json!([
            { "role": "user", "content": "Anyone?" },
            { "role": "assistant", "content": "Sorry, I can't answer right now." }
        ])
    );
}

#[tokio::test]
async fn without_a_line_a_failed_turn_only_reports_on_ui() {
    let mut rig = rig(json!({ "hold_ms": 0 })).await;
    rig.transcript("alice", "Anyone?").await;
    let outs = rig.until_listening().await;
    assert!(speech(&outs).is_empty());
    assert_eq!(
        ui_types(&outs),
        ["data-transcript", "data-turn:thinking", "start", "error", "finish", "data-turn:listening"]
    );
}

#[tokio::test]
async fn the_first_turn_holds_for_a_host_that_attaches_late() {
    let mut rig = rig(json!({ "hold_ms": 2000, "fallback": { "line": "Sorry." } })).await;
    rig.transcript("alice", "Early bird").await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mut host = rig.host();
    let request = host.answer(&["Here now."]).await;
    assert_eq!(request["transcript"], "Early bird");
    let outs = rig.until_listening().await;
    assert_eq!(speech(&outs), ["tts:Here now."]);
    assert!(!ui_types(&outs).contains(&"error".to_string()));

    // Only the first turn holds: with the host gone, the next fails at once.
    drop(host);
    let asked = Instant::now();
    rig.transcript("alice", "Still there?").await;
    let outs = rig.until_listening().await;
    assert_eq!(speech(&outs), ["tts:Sorry."]);
    assert!(asked.elapsed() < Duration::from_millis(1500), "{:?}", asked.elapsed());
}

#[tokio::test]
async fn a_missed_first_chunk_deadline_cancels_the_host_and_falls_back() {
    let mut rig = rig(json!({ "first_chunk_deadline_ms": 100, "hold_ms": 0, "fallback": { "line": "Sorry." } })).await;
    let mut host = rig.host();
    rig.transcript("alice", "Slow?").await;
    let (call, _) = host.turn().await;
    host.cancelled(&call).await;
    let outs = rig.until_listening().await;
    assert_eq!(speech(&outs), ["tts:Sorry."]);
    assert_eq!(ui_chunk(&outs, "error")["errorText"], "no reply within 100 ms");
}

#[tokio::test]
async fn a_host_lost_mid_reply_keeps_what_was_said_and_adds_the_line() {
    let mut rig = rig(json!({ "hold_ms": 0, "fallback": { "line": "Sorry." } })).await;
    let mut host = rig.host();
    rig.transcript("alice", "Go").await;
    let (call, _) = host.turn().await;
    host.text(&call, "Half an ");
    rig.speech().await;
    drop(host);
    let outs = rig.until_listening().await;
    assert_eq!(speech(&outs), ["tts:Sorry."]);
    let history = rig.rpc("get_history", None).await.unwrap();
    assert_eq!(history[1], json!({ "role": "assistant", "content": "Half an Sorry." }));
}

/// A local OpenAI-compatible server that streams `tokens` and records the
/// request bodies.
async fn chat_server(tokens: &'static [&'static str]) -> (String, Arc<parking_lot::Mutex<Vec<Value>>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    let bodies = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let seen = bodies.clone();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let seen = seen.clone();
            tokio::spawn(async move {
                let mut buffer = Vec::new();
                let mut chunk = [0u8; 4096];
                let body = loop {
                    let Ok(read) = socket.read(&mut chunk).await else { return };
                    if read == 0 {
                        return;
                    }
                    buffer.extend_from_slice(&chunk[..read]);
                    let Some(head) = buffer.windows(4).position(|w| w == b"\r\n\r\n") else {
                        continue;
                    };
                    let headers = String::from_utf8_lossy(&buffer[..head]).to_ascii_lowercase();
                    let length: usize = headers
                        .lines()
                        .find_map(|line| line.strip_prefix("content-length:"))
                        .and_then(|value| value.trim().parse().ok())
                        .unwrap_or(0);
                    if buffer.len() >= head + 4 + length {
                        break buffer[head + 4..head + 4 + length].to_vec();
                    }
                };
                seen.lock().push(serde_json::from_slice(&body).unwrap_or(Value::Null));
                let mut reply =
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n".to_string();
                for token in tokens {
                    reply.push_str(&format!(
                        "data: {}\n\n",
                        json!({ "choices": [{ "delta": { "content": token } }] })
                    ));
                }
                reply.push_str("data: [DONE]\n\n");
                let _ = socket.write_all(reply.as_bytes()).await;
                let _ = socket.shutdown().await;
            });
        }
    });
    (url, bodies)
}

#[tokio::test]
async fn an_unanswered_turn_runs_on_the_local_chat_backend() {
    let (url, bodies) = chat_server(&["Local ", "answer."]).await;
    let mut rig = rig(json!({
        "hold_ms": 0,
        "fallback": { "line": "Sorry.", "chat": { "base_url": url, "model": "local", "system_prompt": "Be brief." } }
    }))
    .await;
    rig.rpc("set_history", Some(json!([
        { "role": "user", "content": "Earlier" },
        { "role": "assistant", "content": "Cut", "interrupted": true }
    ])))
    .await
    .unwrap();
    rig.transcript("alice", "Anyone?").await;
    let outs = rig.until_listening().await;
    assert_eq!(speech(&outs), ["tts:Local ", "tts:answer."]);
    // The failure is still reported; the line is not needed.
    assert!(ui_chunk(&outs, "error")["errorText"].as_str().unwrap().starts_with("no_responder"));
    let body = bodies.lock()[0].clone();
    assert_eq!(body["model"], "local");
    assert_eq!(
        body["messages"],
        json!([
            { "role": "system", "content": "Be brief." },
            { "role": "user", "content": "Earlier" },
            { "role": "assistant", "content": "Cut" },
            { "role": "user", "content": "Anyone?" }
        ])
    );
    let history = rig.rpc("get_history", None).await.unwrap();
    assert_eq!(history[3], json!({ "role": "assistant", "content": "Local answer." }));
}

#[tokio::test]
async fn a_failing_local_backend_falls_back_to_the_line() {
    // Nothing listens on this port.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/v1", listener.local_addr().unwrap());
    drop(listener);
    let mut rig = rig(json!({
        "hold_ms": 0, "fallback": { "line": "Sorry.", "chat": { "base_url": url } }
    }))
    .await;
    rig.transcript("alice", "Anyone?").await;
    let outs = tokio::time::timeout(Duration::from_secs(20), rig.until_listening())
        .await
        .unwrap();
    assert_eq!(speech(&outs), ["tts:Sorry."]);
    assert_eq!(ui_types(&outs).iter().filter(|kind| *kind == "error").count(), 2);
}

// -- 3.6: events --------------------------------------------------------------------------

#[tokio::test]
async fn the_host_is_told_about_media_readiness_joins_and_leaves() {
    let rig = rig(json!({ "hold_ms": 2000 })).await;
    // Ready before the host attached: the event waits for it.
    rig.control.publish_media_ready("alice", "webrtc", "c1");
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut host = rig.host();
    assert_eq!(
        host.host_event("media_ready").await,
        json!({ "type": "media_ready", "member": { "id": "alice" } })
    );

    // A member's first input is its join.
    rig.send(
        RuntimeData::Json(json!({ "is_speech_start": false, "is_speech_end": true })),
        member("bob", Some("host")),
    )
    .await;
    assert_eq!(
        host.host_event("member_joined").await,
        json!({ "type": "member_joined",
                "member": { "id": "bob", "name": "bob!", "metadata": { "role": "host" } } })
    );
    rig.control.publish_media_ready("bob", "webrtc", "c2");
    assert_eq!(host.host_event("media_ready").await["member"]["name"], "bob!");
    // Once per connection: the same connection again is not reported.
    rig.control.publish_media_ready("bob", "webrtc", "c2");

    let mut leave = member("bob", None);
    leave.insert(crate::transport::origin::LIFECYCLE.into(), crate::transport::origin::LEAVE.into());
    rig.send(RuntimeData::Text(String::new()), leave).await;
    let left = host.host_event("member_left").await;
    assert_eq!((left["member"]["id"].clone(), left["reason"].clone()), (json!("bob"), json!("left")));
    assert!(
        !host.seen.iter().any(|event| matches!(event, HostEvent::Call(call)
            if call.method == "event" && call.args[0]["type"] == "media_ready")),
        "media_ready was reported twice for one connection"
    );
}

// -- 3.7a: barge-in policy ------------------------------------------------------------------

/// A turn of alice's, heard by alice and bob, streaming its first chunk.
async fn shared_turn(rig: &mut Rig, host: &mut Host) -> (String, String) {
    // Bob is in the session before the turn starts.
    rig.send(
        RuntimeData::Json(json!({ "is_speech_start": false, "is_speech_end": true })),
        member("bob", None),
    )
    .await;
    rig.transcript("alice", "Tell us").await;
    let (call, request) = host.turn().await;
    host.text(&call, "One. ");
    rig.speech().await;
    (call, request["turn_id"].as_str().unwrap().to_string())
}

#[tokio::test]
async fn scope_everyone_cancels_the_turn_for_the_whole_audience() {
    let mut rig = rig(json!({ "reply_to": "everyone" })).await;
    let mut host = rig.host();
    let (call, turn) = shared_turn(&mut rig, &mut host).await;
    rig.speech_start(&member("bob", None)).await;
    host.cancelled(&call).await;
    assert_eq!(
        rig.flush().await,
        FlushTarget { members: vec![], turn_id: turn.clone(), drop_rest: true },
        "everyone's audio of the turn is flushed"
    );
    let settled = host.settled().await;
    assert_eq!(settled["interrupted"], true);
    // Neither transport reports: what was sent counts as heard.
    assert_eq!(settled["heard"], json!({ "alice": "One. ", "bob": "One. " }));
}

#[tokio::test]
async fn scope_speaker_stops_the_turn_for_the_speaker_only() {
    let mut rig = rig(json!({ "reply_to": "everyone", "ui": "all", "barge_in": { "scope": "speaker" } })).await;
    let mut host = rig.host();
    rig.control.publish_media_ready("alice", "webrtc", "c1");
    rig.control.publish_media_ready("bob", "webrtc", "c2");
    let (call, turn) = shared_turn(&mut rig, &mut host).await;
    host.text(&call, "Two. ");
    rig.speech().await;
    rig.released("bob", &turn, 1, 1, false, false);

    rig.speech_start(&member("bob", None)).await;
    assert_eq!(
        rig.flush().await,
        FlushTarget { members: vec!["bob".into()], turn_id: turn.clone(), drop_rest: true }
    );
    rig.released("bob", &turn, 1, 1, true, false);
    // Only bob is told, with what bob heard.
    let outs = rig.until("data-turn", Some("interrupted")).await;
    let told: Vec<_> = outs
        .iter()
        .filter_map(|out| match out {
            Out::Ui { frame, meta } if matches!(frame["chunk"]["type"].as_str(), Some("abort" | "data-turn")) => {
                Some((meta["participant.to"].clone(), frame["chunk"]["data"]["spoken"].clone()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(told, [("bob".to_string(), Value::Null), ("bob".to_string(), json!("One. "))]);

    // The turn goes on for alice, and only for alice.
    host.text(&call, "Three.");
    let (text, meta) = rig.speech().await;
    assert_eq!((text.as_str(), meta["participant.to"].as_str()), ("tts:Three.", "alice"));
    host.end(&call);
    rig.released("alice", &turn, 3, 2, false, false);
    let outs = rig.until_listening().await;
    for out in &outs {
        if let Out::Ui { meta, .. } = out {
            assert_eq!(meta["participant.to"], "alice", "bob gets no more of the turn");
        }
    }
    let settled = host.settled().await;
    assert_eq!(
        settled,
        json!({ "turn_id": turn, "spoken": "One. Two. Three.", "interrupted": false,
                "heard": { "alice": "One. Two. Three.", "bob": "One. " } })
    );
    // The room heard the whole reply: the history keeps it.
    let history = rig.rpc("get_history", None).await.unwrap();
    assert_eq!(history[1], json!({ "role": "assistant", "content": "One. Two. Three." }));
    assert!(host.seen.iter().all(|event| !matches!(event, HostEvent::Cancelled { .. })));
}

#[tokio::test]
async fn scope_speaker_cancels_when_the_last_listener_speaks() {
    let mut rig = rig(json!({ "reply_to": "everyone", "barge_in": { "scope": "speaker" } })).await;
    let mut host = rig.host();
    let (call, turn) = shared_turn(&mut rig, &mut host).await;
    rig.speech_start(&member("bob", None)).await;
    assert_eq!(rig.flush().await.members, ["bob"]);
    // Bob speaking again changes nothing.
    rig.speech_start(&member("bob", None)).await;
    rig.speech_start(&member("alice", None)).await;
    assert_eq!(rig.flush().await.members, ["alice"]);
    host.cancelled(&call).await;
    // Cancelled as for a barge-in: everyone's queue of the turn is flushed.
    assert_eq!(rig.flush().await, FlushTarget { members: vec![], turn_id: turn, drop_rest: true });
    assert_eq!(host.settled().await["interrupted"], true);
}

#[tokio::test]
async fn scope_none_ignores_speech_but_not_the_hosts_interrupt() {
    let mut rig = rig(json!({ "reply_to": "everyone", "barge_in": { "scope": "none" } })).await;
    let mut host = rig.host();
    let (call, _) = shared_turn(&mut rig, &mut host).await;
    rig.speech_start(&member("bob", None)).await;
    rig.speech_start(&member("alice", None)).await;
    // A browser's `interrupt` frame counts as speech.
    rig.send(RuntimeData::Json(json!({ "v": 1, "type": "interrupt" })), member("alice", None))
        .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    host.text(&call, "Two. ");
    assert_eq!(rig.speech().await.0, "tts:Two. ");

    assert_eq!(rig.rpc("interrupt", Some(json!({}))).await.unwrap(), json!({ "interrupted": true }));
    host.cancelled(&call).await;
    assert_eq!(host.settled().await["interrupted"], true);
    assert_eq!(rig.rpc("interrupt", Some(json!({}))).await.unwrap(), json!({ "interrupted": false }));
}

#[tokio::test]
async fn a_moderator_role_stops_the_room_and_others_only_themselves() {
    let policy = json!({ "scope": "everyone", "by": { "members": [], "roles": ["host"] }, "otherwise": "speaker" });
    let mut rig = rig(json!({ "reply_to": "everyone", "barge_in": policy })).await;
    let mut host = rig.host();
    // Carol is the moderator.
    rig.send(
        RuntimeData::Json(json!({ "is_speech_start": false, "is_speech_end": true })),
        member("carol", Some("host")),
    )
    .await;
    let (call, turn) = shared_turn(&mut rig, &mut host).await;

    rig.speech_start(&member("bob", None)).await;
    assert_eq!(rig.flush().await.members, ["bob"], "a guest only mutes it for themselves");
    host.text(&call, "Two. ");
    assert_eq!(rig.speech().await.0, "tts:Two. ");

    rig.speech_start(&member("carol", Some("host"))).await;
    host.cancelled(&call).await;
    assert_eq!(rig.flush().await, FlushTarget { members: vec![], turn_id: turn, drop_rest: true });
    let settled = host.settled().await;
    assert_eq!(settled["interrupted"], true);
    assert_eq!(settled["heard"]["bob"], "One. ");
    assert_eq!(settled["heard"]["carol"], "One. Two. ");
}

#[tokio::test]
async fn a_member_outside_the_audience_does_not_interrupt_and_waits_its_turn() {
    // reply_to: speaker — alice's turn is for alice alone.
    let mut rig = rig(json!({})).await;
    let mut host = rig.host();
    let (call, _) = shared_turn(&mut rig, &mut host).await;
    rig.speech_start(&member("bob", None)).await;
    rig.transcript("bob", "My question").await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    // Alice's turn still streams; bob's has not started.
    host.text(&call, "Two.");
    assert_eq!(rig.speech().await.0, "tts:Two.");
    assert!(
        host.seen.iter().all(|event| matches!(event, HostEvent::Call(call) if call.method == "event")),
        "{:?}",
        host.seen
    );
    host.end(&call);
    assert_eq!(host.settled().await["interrupted"], false);

    // One turn at a time: bob's starts when alice's is over.
    let (call, request) = host.turn().await;
    assert_eq!((request["member"]["id"].clone(), request["transcript"].clone()), (json!("bob"), json!("My question")));
    host.text(&call, "Bob's answer.");
    let (_, meta) = rig.speech().await;
    assert_eq!(meta["participant.to"], "bob");
}

#[tokio::test]
async fn the_policy_can_be_replaced_mid_session() {
    let mut rig = rig(json!({ "reply_to": "everyone", "barge_in": { "scope": "none" } })).await;
    let mut host = rig.host();
    let (call, _) = shared_turn(&mut rig, &mut host).await;
    rig.speech_start(&member("bob", None)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    host.text(&call, "Two. ");
    assert_eq!(rig.speech().await.0, "tts:Two. ");

    // A presenter joined: from now on bob stops the room.
    let policy = json!({ "scope": "everyone", "by": { "members": ["bob"], "roles": [] }, "otherwise": "none" });
    rig.rpc("set_barge_in", Some(policy)).await.unwrap();
    assert_eq!(rig.node.barge_in().by.unwrap().members, ["bob"]);
    rig.speech_start(&member("alice", None)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    host.text(&call, "Three. ");
    assert_eq!(rig.speech().await.0, "tts:Three. ");
    rig.speech_start(&member("bob", None)).await;
    host.cancelled(&call).await;
    assert!(rig.rpc("set_barge_in", Some(json!({ "scope": "loud" }))).await.is_err());
}

#[tokio::test]
async fn interrupt_for_a_member_has_the_speaker_effect() {
    let mut rig = rig(json!({ "reply_to": "everyone", "barge_in": { "scope": "none" } })).await;
    let mut host = rig.host();
    let (call, turn) = shared_turn(&mut rig, &mut host).await;
    assert_eq!(
        rig.rpc("interrupt", Some(json!({ "member": "bob" }))).await.unwrap(),
        json!({ "interrupted": true })
    );
    assert_eq!(
        rig.flush().await,
        FlushTarget { members: vec!["bob".into()], turn_id: turn, drop_rest: true }
    );
    host.text(&call, "Two.");
    host.end(&call);
    let settled = host.settled().await;
    assert_eq!(settled["interrupted"], false);
    assert_eq!(settled["heard"], json!({ "alice": "One. Two.", "bob": "One. " }));
}

#[tokio::test]
async fn an_unattributed_barge_cancels_the_turn_at_the_host() {
    let mut rig = rig(json!({})).await;
    let mut host = rig.host();
    rig.transcript("alice", "Go").await;
    let (call, _) = host.turn().await;
    host.text(&call, "On it");
    rig.speech().await;
    rig.control
        .publish(
            &ControlAddress::node_in("turn").with_port("barge_in"),
            RuntimeData::Text("barge".into()),
        )
        .await
        .unwrap();
    host.cancelled(&call).await;
    assert_eq!(host.settled().await["interrupted"], true);
}

// -- speak, send_ui ---------------------------------------------------------------------------

#[tokio::test]
async fn speak_waits_for_media_and_is_settled_like_a_turn() {
    let mut rig = rig(json!({})).await;
    let mut host = rig.host();
    let accepted = rig
        .rpc("speak", Some(json!({ "text": "Hi Alice, how can I help?", "to": ["alice"] })))
        .await
        .unwrap();
    assert_eq!(accepted["queued"], true);
    let turn = accepted["turn_id"].as_str().unwrap().to_string();
    rig.quiet().await;

    rig.control.publish_media_ready("alice", "webrtc", "c1");
    let (text, meta) = rig.speech().await;
    assert_eq!(text, "tts:Hi Alice, how can I help?");
    assert_eq!(meta["participant.turn_id"], turn);
    assert_eq!(meta["participant.to"], "alice");
    // Reported like a turn: it settles when its audio was released.
    rig.released("alice", &turn, 1, 0, false, false);
    assert_eq!(
        host.settled().await,
        json!({ "turn_id": turn, "spoken": "Hi Alice, how can I help?", "interrupted": false,
                "heard": { "alice": "Hi Alice, how can I help?" } })
    );
    assert_eq!(
        rig.rpc("get_history", None).await.unwrap(),
        json!([{ "role": "assistant", "content": "Hi Alice, how can I help?" }])
    );
    assert!(rig.rpc("speak", Some(json!({ "text": " " }))).await.is_err());
}

#[tokio::test]
async fn speech_that_is_not_interruptible_ignores_barge_in() {
    let mut rig = rig(json!({})).await;
    let mut host = rig.host();
    rig.control.publish_media_ready("alice", "webrtc", "c1");
    let turn = rig
        .rpc("speak", Some(json!({ "text": "Please hold.", "to": "alice", "interruptible": false })))
        .await
        .unwrap()["turn_id"]
        .as_str()
        .unwrap()
        .to_string();
    rig.speech().await;
    rig.speech_start(&member("alice", None)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    rig.released("alice", &turn, 1, 0, false, false);
    assert_eq!(host.settled().await["interrupted"], false);
    assert!(!rig.node.is_protected());
}

#[tokio::test]
async fn send_ui_emits_a_frame_outside_any_turn() {
    let mut rig = rig(json!({})).await;
    let chunk = json!({ "type": "data-notice", "data": { "text": "maintenance at 5" } });
    rig.rpc("send_ui", Some(json!({ "chunk": chunk, "to": ["bob"] }))).await.unwrap();
    let Out::Ui { frame, meta } = rig.next().await else { panic!("a ui frame") };
    assert_eq!(frame, json!({ "v": 1, "turn": null, "chunk": chunk }));
    assert_eq!(meta["participant.to"], "bob");
    rig.rpc("send_ui", Some(json!({ "chunk": chunk }))).await.unwrap();
    let Out::Ui { meta, .. } = rig.next().await else { panic!("a ui frame") };
    assert!(!meta.contains_key("participant.to"));
    assert!(rig.rpc("send_ui", Some(json!({ "chunk": "x" }))).await.is_err());
}

// -- a turn cut by a unit move (design D7) -------------------------------------------------------

/// The unit moved mid-turn: the instance that ran the turn is gone, and the
/// host, which settled the turn itself, has this (new) instance tell the
/// turn's member and carry the settlement as `previous`.
#[tokio::test]
async fn settle_turn_closes_a_turn_of_an_earlier_instance_and_carries_it_as_previous() {
    let mut rig = rig(json!({ "history": "host" })).await;
    let mut host = rig.host();
    let reply = rig
        .rpc(
            "settle_turn",
            Some(json!({ "turn_id": "turn-0ld000-3", "spoken": "One. ", "member": "alice" })),
        )
        .await
        .unwrap();
    assert_eq!(reply, json!({ "settled": true }));
    let Out::Ui { frame, meta } = rig.next().await else { panic!("a ui frame") };
    assert_eq!(frame, json!({ "v": 1, "turn": "turn-0ld000-3", "chunk": { "type": "abort" } }));
    assert_eq!(meta["participant.to"], "alice");
    let Out::Ui { frame, meta } = rig.next().await else { panic!("a ui frame") };
    assert_eq!(
        frame,
        json!({ "v": 1, "turn": "turn-0ld000-3", "chunk": {
            "type": "data-turn", "id": "turn-0ld000-3", "transient": true,
            "data": { "status": "interrupted", "spoken": "One. " },
        } })
    );
    assert_eq!(meta["participant.to"], "alice");

    // Told once: a repeated call (the host retries) emits nothing.
    let again = rig
        .rpc(
            "settle_turn",
            Some(json!({ "turn_id": "turn-0ld000-3", "spoken": "One. ", "member": "alice" })),
        )
        .await
        .unwrap();
    assert_eq!(again, json!({ "settled": false, "reason": "already_settled" }));
    rig.quiet().await;

    // The next `turn` repeats the settlement, like one of the node's own.
    rig.transcript("alice", "And then?").await;
    let request = host.answer(&["Then two."]).await;
    assert_eq!(
        request["previous"],
        json!({
            "turn_id": "turn-0ld000-3", "spoken": "One. ", "interrupted": true,
            "heard": { "alice": "One. " },
        })
    );
    rig.until_listening().await;
}

#[tokio::test]
async fn settle_turn_leaves_this_instances_own_turns_alone() {
    let mut rig = rig(json!({ "history": "host" })).await;
    let mut host = rig.host();
    rig.transcript("alice", "First?").await;
    let (call, request) = host.turn().await;
    let id = request["turn_id"].as_str().unwrap().to_string();
    host.text(&call, "One. ");
    rig.until("text-delta", None).await;
    // The node runs this turn and settles it itself.
    let reply = rig
        .rpc("settle_turn", Some(json!({ "turn_id": id, "spoken": "", "member": "alice" })))
        .await
        .unwrap();
    assert_eq!(reply, json!({ "settled": false, "reason": "own_turn" }));
    rig.quiet().await;
    host.end(&call);
    rig.until_listening().await;
    let settled = host.settled().await;
    assert_eq!(settled["interrupted"], false);

    // A settlement of the node's own is not replaced by an older one.
    let reply = rig
        .rpc("settle_turn", Some(json!({ "turn_id": "turn-0ld000-9", "spoken": "x", "member": "alice", "open": false })))
        .await
        .unwrap();
    assert_eq!(reply, json!({ "settled": true }));
    let Out::Ui { frame, .. } = rig.next().await else { panic!("a ui frame") };
    assert_eq!(frame["chunk"]["data"]["status"], "interrupted", "no abort when the message is not open");
    rig.transcript("alice", "Second?").await;
    let request = host.answer(&["Again."]).await;
    assert_eq!(request["previous"], settled);
    rig.until_listening().await;

    assert!(rig.rpc("settle_turn", Some(json!({ "spoken": "x" }))).await.is_err());
}

/// The member's next turn overtook the host's `settle_turn`: the front end,
/// sent to `listening` by `interrupted`, is told the running turn's status.
#[tokio::test]
async fn settle_turn_during_a_turn_repeats_that_turns_status() {
    let mut rig = rig(json!({ "history": "host" })).await;
    let mut host = rig.host();
    rig.transcript("alice", "After?").await;
    let (call, request) = host.turn().await;
    assert!(request.get("previous").is_none());
    host.text(&call, "Here. ");
    rig.until("text-delta", None).await;
    rig.rpc("settle_turn", Some(json!({ "turn_id": "turn-0ld000-3", "spoken": "One. ", "member": "alice" })))
        .await
        .unwrap();
    let outs = rig.until("data-turn", Some("speaking")).await;
    assert_eq!(ui_types(&outs), ["abort", "data-turn:interrupted", "data-turn:speaking"]);
    let Out::Ui { frame, .. } = &outs[2] else { panic!("a ui frame") };
    assert_eq!(frame["turn"], request["turn_id"]);
    host.end(&call);
    rig.until_listening().await;
}

#[tokio::test]
async fn settle_turn_follows_the_ui_audience() {
    let mut rig = rig(json!({ "history": "host", "ui": "all" })).await;
    rig.rpc("settle_turn", Some(json!({ "turn_id": "turn-0ld000-1", "spoken": "", "member": "alice" })))
        .await
        .unwrap();
    let Out::Ui { meta, .. } = rig.next().await else { panic!("a ui frame") };
    assert!(!meta.contains_key("participant.to"));
    rig.next().await;
    // `to` overrides it.
    rig.rpc(
        "settle_turn",
        Some(json!({ "turn_id": "turn-0ld000-2", "spoken": "", "member": "alice", "to": ["bob"] })),
    )
    .await
    .unwrap();
    let Out::Ui { meta, .. } = rig.next().await else { panic!("a ui frame") };
    assert_eq!(meta["participant.to"], "bob");
}

// -- 3.8: barge-in to host-cancel latency ---------------------------------------------------------

/// From a member's speech start entering the session to the host seeing the
/// cancellation of its `turn` call, through the session router and
/// `SessionControl::host_endpoint`. Prints the distribution.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn barge_in_to_host_cancel_latency() {
    const ROUNDS: usize = 200;
    let mut rig = rig(json!({ "flush_wait_ms": 0 })).await;
    let mut host = rig.host();
    let alice = member("alice", None);
    let mut micros = Vec::with_capacity(ROUNDS);
    for round in 0..ROUNDS {
        rig.transcript("alice", "Tell me a long story").await;
        let (call, _) = host.turn().await;
        host.text(&call, "Once upon a time ");
        rig.speech().await;
        let barged = Instant::now();
        rig.speech_start(&alice).await;
        host.cancelled(&call).await;
        micros.push(barged.elapsed().as_micros() as u64);
        let settled = host.settled().await;
        assert_eq!(settled["interrupted"], true, "round {round}");
        rig.until("data-turn", Some("interrupted")).await;
    }
    micros.sort_unstable();
    let at = |q: f64| micros[((micros.len() - 1) as f64 * q) as usize];
    println!(
        "barge-in -> host cancel over {ROUNDS} turns (in-process host): \
         min {} us, p50 {} us, p95 {} us, p99 {} us, max {} us",
        micros[0],
        at(0.50),
        at(0.95),
        at(0.99),
        micros[micros.len() - 1]
    );
    assert!(at(0.95) < 50_000, "p95 {} us", at(0.95));
}

// -- design D10: what the realtime path needs of the turn node ---------------

#[tokio::test]
async fn client_instructions_set_by_rpc_ride_on_that_members_spoken_turns() {
    let mut rig = rig(json!({})).await;
    let mut host = rig.host();
    rig.rpc(
        "set_client_instructions",
        Some(json!({ "member": "alice", "instructions": "Answer in Norwegian." })),
    )
    .await
    .unwrap();

    rig.transcript("alice", "hi").await;
    let request = host.answer(&["Hei."]).await;
    assert_eq!(request["client_instructions"], "Answer in Norwegian.");
    rig.until_listening().await;

    // Another member's turn carries none; a typed frame's own wins.
    rig.transcript("bob", "hi").await;
    let request = host.answer(&["Hi."]).await;
    assert!(request.get("client_instructions").is_none());
    rig.until_listening().await;
    rig.send(
        RuntimeData::Json(json!({ "v": 1, "type": "text", "text": "hi", "client_instructions": "Be brief." })),
        member("alice", None),
    )
    .await;
    let request = host.answer(&["Hi."]).await;
    assert_eq!(request["client_instructions"], "Be brief.");
    rig.until_listening().await;

    // Cleared.
    rig.rpc("set_client_instructions", Some(json!({ "member": "alice", "instructions": null })))
        .await
        .unwrap();
    rig.transcript("alice", "hi").await;
    let request = host.answer(&["Hi."]).await;
    assert!(request.get("client_instructions").is_none());
}

#[tokio::test]
async fn a_flush_report_replaces_what_progress_reports_counted() {
    let mut rig = rig(json!({})).await;
    let mut host = rig.host();
    rig.control.publish_media_ready("alice", "realtime", "c1");
    rig.transcript("alice", "count").await;
    let (call, request) = host.turn().await;
    let turn = request["turn_id"].as_str().unwrap().to_string();
    for chunk in ["One. ", "Two. ", "Three. "] {
        host.text(&call, chunk);
    }
    rig.until("text-delta", None).await;
    // Two chunks were released to the member's browser...
    rig.released("alice", &turn, 2, 2, false, false);
    tokio::time::sleep(Duration::from_millis(50)).await;
    rig.speech_start(&member("alice", None)).await;
    rig.flush().await;
    // ...but it had played only the first when it stopped (its truncation).
    rig.released("alice", &turn, 1, 0, true, false);
    let settled = host.settled().await;
    assert_eq!(settled["interrupted"], true);
    assert_eq!(settled["spoken"], "One. ");
    assert_eq!(settled["heard"]["alice"], "One. ");
}

// -- a retracted utterance ---------------------------------------------------

/// "Hello. <pause> This is a test.": the accumulator emits "Hello." at the
/// pause, then takes it back when the speech goes on and emits the whole
/// sentence. The barge-in of the resumed speech has already settled the
/// first turn when the retraction arrives.
#[tokio::test]
async fn a_retracted_utterance_and_its_continuation_are_one_turn() {
    let mut rig = rig(json!({})).await;
    let mut host = rig.host();
    rig.transcript("alice", "Earlier?").await;
    host.answer(&["Earlier."]).await;
    rig.until_listening().await;
    let earlier = host.settled().await;

    rig.transcript("alice", "Hello.").await;
    let (call, request) = host.turn().await;
    let turn = request["turn_id"].as_str().unwrap().to_string();
    rig.until("start", None).await;

    // The speech goes on: a barge-in first, as the VAD reports it.
    rig.speech_start(&member("alice", None)).await;
    host.cancelled(&call).await;
    let outs = rig.until("data-turn", Some("interrupted")).await;
    assert_eq!(ui_types(&outs), ["abort", "data-turn:interrupted"]);
    assert_eq!(host.settled().await["turn_id"], turn.as_str());

    // A speech start that continues nothing retracts nothing.
    rig.cancel_speculation("alice", false).await;
    rig.quiet().await;

    rig.cancel_speculation("alice", true).await;
    let outs = rig.until("data-turn", Some("retracted")).await;
    assert_eq!(ui_types(&outs), ["data-turn:retracted"]);
    let Out::Ui { frame, meta } = &outs[0] else { panic!("a ui frame") };
    assert_eq!(
        frame,
        &json!({ "v": 1, "turn": turn, "chunk": {
            "type": "data-turn", "id": turn, "transient": true, "data": { "status": "retracted" },
        } })
    );
    assert_eq!(meta["participant.to"], "alice");
    let event = host.host_event("turn_retracted").await;
    assert_eq!(event["turn_id"], turn.as_str());
    assert_eq!(event["transcript"], "Hello.");
    assert_eq!(event["spoken"], "");
    assert_eq!(event["member"]["id"], "alice");
    let before = json!([
        { "role": "user", "content": "Earlier?" },
        { "role": "assistant", "content": "Earlier." }
    ]);
    assert_eq!(rig.rpc("get_history", None).await.unwrap(), before);

    // The whole sentence is the one turn; the retracted one never was.
    rig.transcript("alice", "Hello. This is a test.").await;
    let request = host.answer(&["Thank you."]).await;
    assert_eq!(request["transcript"], "Hello. This is a test.");
    assert_eq!(request["messages"], before);
    assert_eq!(request["previous"], earlier);
    rig.until_listening().await;
    assert_eq!(
        rig.rpc("get_history", None).await.unwrap(),
        json!([
            { "role": "user", "content": "Earlier?" },
            { "role": "assistant", "content": "Earlier." },
            { "role": "user", "content": "Hello. This is a test." },
            { "role": "assistant", "content": "Thank you." }
        ])
    );
}

#[tokio::test]
async fn a_retraction_cancels_the_turn_in_flight_and_keeps_nothing_of_it() {
    // No barge-in, so the turn is still running when it is retracted.
    let mut rig = rig(json!({ "barge_in": { "scope": "none" } })).await;
    let mut host = rig.host();
    rig.transcript("alice", "Hello.").await;
    let (call, request) = host.turn().await;
    let turn = request["turn_id"].as_str().unwrap().to_string();
    host.text(&call, "Hi there. ");
    host.text(&call, "How can I help?");
    for _ in 0..2 {
        rig.speech().await;
    }

    rig.cancel_speculation("alice", true).await;
    // The host's call is cancelled and the turn's audio flushed.
    host.cancelled(&call).await;
    assert_eq!(
        rig.flush().await,
        FlushTarget { members: vec!["alice".into()], turn_id: turn.clone(), drop_rest: true }
    );
    assert!(host.meta(&call, json!({})).is_err());
    let outs = rig.until("data-turn", Some("retracted")).await;
    assert!(speech(&outs).is_empty());
    assert_eq!(ui_types(&outs), ["abort", "data-turn:retracted"]);
    let event = host.host_event("turn_retracted").await;
    assert_eq!((event["turn_id"].as_str(), event["transcript"].as_str()), (Some(turn.as_str()), Some("Hello.")));
    assert_eq!(rig.rpc("get_history", None).await.unwrap(), json!([]));

    rig.transcript("alice", "Hello. This is a test.").await;
    let request = host.answer(&["Thank you."]).await;
    assert_eq!(request["messages"], json!([]));
    // It was never a turn: there is no settlement of it to repeat.
    assert!(request.get("previous").is_none());
    rig.until_listening().await;
    let settled = host.settled().await;
    assert_ne!(settled["turn_id"], turn.as_str(), "the retracted turn is never settled");
    assert_eq!(
        rig.rpc("get_history", None).await.unwrap(),
        json!([
            { "role": "user", "content": "Hello. This is a test." },
            { "role": "assistant", "content": "Thank you." }
        ])
    );
}

#[tokio::test]
async fn a_retracted_utterance_still_waiting_for_the_slot_never_becomes_a_turn() {
    let mut rig = rig(json!({ "barge_in": { "scope": "none" } })).await;
    let mut host = rig.host();
    rig.transcript("bob", "A long question").await;
    let (call, _) = host.turn().await;
    rig.transcript("alice", "Hello.").await;
    rig.cancel_speculation("alice", true).await;
    rig.transcript("alice", "Hello. This is a test.").await;
    // Alice's inputs reach the node while Bob's turn still holds the slot.
    tokio::time::sleep(Duration::from_millis(200)).await;
    host.text(&call, "Answer.");
    host.end(&call);
    let request = host.answer(&["Thank you."]).await;
    assert_eq!(request["transcript"], "Hello. This is a test.");
    assert_eq!(
        request["messages"],
        json!([
            { "role": "user", "content": "A long question" },
            { "role": "assistant", "content": "Answer." }
        ])
    );
    // Bob's turn is untouched by Alice's retraction.
    let outs = rig.until_listening().await;
    assert!(!ui_types(&outs).iter().any(|kind| kind == "data-turn:retracted"));
}
