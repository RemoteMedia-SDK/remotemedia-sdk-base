//! Node-initiated output (openspec: add-node-initiated-output): a node puts
//! data on its output edge through its emitter, with or without an input in
//! progress, and says who the data is attributed to.
use super::*;
use crate::nodes::{
    EmitOrigin, Emitter, InitializeContextRead, InstanceScope, NodeRuntimeContext,
    NodeRuntimeContextRead, StreamingNode, StreamingNodeFactory,
};
use crate::transport::data::participant;
use crate::transport::origin::Origin;
use crate::transport::session_control::{wrap_aux_port, ControlAddress, BARGE_IN_PORT};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;

/// What the test can observe of the node's instances after the fact.
#[derive(Default)]
struct Probe {
    /// Emitters of every instance, in creation order.
    emitters: Mutex<Vec<Emitter>>,
    /// Set when a background task saw its emitter close.
    saw_close: AtomicBool,
}

/// A node driven by text commands (see `process_streaming_async`).
struct EmitNode {
    scope: Option<InstanceScope>,
    /// Frames a background task emits right after initialization.
    ticks: usize,
    /// Pause before each of those frames.
    tick_ms: u64,
    /// The background task closes the emitter after its last frame.
    finish: bool,
    /// What the node itself declares.
    self_driven: Option<bool>,
    probe: Arc<Probe>,
    emitter: Mutex<Option<Emitter>>,
    kept: Mutex<Option<Arc<Origin>>>,
}

fn text(value: impl Into<String>) -> RuntimeData {
    RuntimeData::Text(value.into())
}

#[async_trait::async_trait]
impl StreamingNode for EmitNode {
    fn node_type(&self) -> &str {
        "EmitNode"
    }

    async fn initialize(&self, ctx: &dyn InitializeContextRead) -> crate::Result<()> {
        let emitter = Emitter::of(ctx);
        self.probe.emitters.lock().unwrap().push(emitter.clone());
        *self.emitter.lock().unwrap() = Some(emitter.clone());
        if self.ticks > 0 {
            let (ticks, probe) = (self.ticks, self.probe.clone());
            let (tick_ms, finish) = (self.tick_ms, self.finish);
            tokio::spawn(async move {
                for n in 0..ticks {
                    if tick_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(tick_ms)).await;
                    }
                    if emitter.emit(text(format!("tick-{n}"))).await.is_err() {
                        return;
                    }
                }
                if finish {
                    emitter.finish();
                }
                emitter.closed().await;
                probe.saw_close.store(true, Ordering::SeqCst);
            });
        }
        Ok(())
    }

    async fn process_async(
        &self,
        data: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> crate::Result<RuntimeData> {
        Ok(data)
    }

    async fn process_multi_async(
        &self,
        _inputs: HashMap<String, RuntimeData>,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> crate::Result<RuntimeData> {
        Err(crate::Error::Execution("single input".into()))
    }

    fn is_multi_input(&self) -> bool {
        false
    }

    async fn process_streaming_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut reply: Box<dyn FnMut(RuntimeData) -> crate::Result<()> + Send>,
    ) -> crate::Result<usize> {
        let RuntimeData::Text(command) = data else {
            reply(data)?;
            return Ok(1);
        };
        let emitter = Emitter::for_call(ctx);
        let origin = NodeRuntimeContext::of(ctx).and_then(NodeRuntimeContext::origin);
        let (verb, rest) = command.split_once(':').unwrap_or((command.as_str(), ""));
        match verb {
            // Keep the attribution of this input for later.
            "remember" => {
                *self.kept.lock().unwrap() = origin;
                reply(text("remembered"))?;
            }
            // Emit with the attribution kept by `remember`.
            "later" => {
                let kept = self.kept.lock().unwrap().clone();
                emitter.emit_as(text(rest), kept).await?;
                reply(text("ack"))?;
            }
            // Emit as the participant named by id; answer how it went.
            "as" => {
                let (id, body) = rest.split_once(':').unwrap();
                match emitter.emit_as(text(body), id).await {
                    Ok(()) => reply(text("ok"))?,
                    Err(error) => reply(text(format!("error: {error}")))?,
                }
            }
            // Emit with the instance's default attribution.
            "plain" => {
                emitter.emit(text(rest)).await?;
                reply(text("ack"))?;
            }
            // Emit unattributed, also on a per-participant instance.
            "nobody" => {
                emitter.emit_as(text(rest), EmitOrigin::Unattributed).await?;
                reply(text("ack"))?;
            }
            // A reply, an emit and a reply.
            "order" => {
                reply(text("reply-1"))?;
                emitter.emit(text("emitted")).await?;
                reply(text("reply-2"))?;
            }
            // Emit from a task after this call has returned.
            "after" => {
                let body = rest.to_string();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let _ = emitter.emit(text(body)).await;
                });
                reply(text("started"))?;
            }
            _ => reply(text(command.clone()))?,
        }
        Ok(1)
    }

    fn instance_scope(&self) -> Option<InstanceScope> {
        self.scope
    }

    fn self_driven(&self) -> Option<bool> {
        self.self_driven
    }
}

struct EmitFactory {
    node_type: &'static str,
    scope: Option<InstanceScope>,
    probe: Arc<Probe>,
}

impl StreamingNodeFactory for EmitFactory {
    fn create(
        &self,
        _node_id: String,
        params: &serde_json::Value,
        _session_id: Option<String>,
    ) -> crate::Result<Box<dyn StreamingNode>> {
        Ok(Box::new(EmitNode {
            scope: self.scope,
            ticks: params.get("ticks").and_then(|v| v.as_u64()).unwrap_or(0) as usize,
            tick_ms: params.get("tick_ms").and_then(|v| v.as_u64()).unwrap_or(0),
            finish: params.get("finish").and_then(|v| v.as_bool()).unwrap_or(false),
            self_driven: params.get("self_driven").and_then(|v| v.as_bool()),
            probe: self.probe.clone(),
            emitter: Mutex::new(None),
            kept: Mutex::new(None),
        }))
    }
    fn node_type(&self) -> &str {
        self.node_type
    }
}

/// Sleeps before passing each input on: a slow consumer.
struct SlowNode;

#[async_trait::async_trait]
impl crate::nodes::AsyncStreamingNode for SlowNode {
    fn node_type(&self) -> &str {
        "SlowNode"
    }
    async fn process(&self, data: RuntimeData) -> crate::Result<RuntimeData> {
        tokio::time::sleep(Duration::from_millis(2)).await;
        Ok(data)
    }
}

struct SlowFactory;

impl StreamingNodeFactory for SlowFactory {
    fn create(
        &self,
        _node_id: String,
        _params: &serde_json::Value,
        _session_id: Option<String>,
    ) -> crate::Result<Box<dyn StreamingNode>> {
        Ok(Box::new(crate::nodes::AsyncNodeWrapper(Arc::new(SlowNode))))
    }
    fn node_type(&self) -> &str {
        "SlowNode"
    }
}

async fn executor() -> (PipelineExecutor, Arc<Probe>) {
    let executor = PipelineExecutor::new().unwrap();
    let probe = Arc::new(Probe::default());
    for (node_type, scope) in [
        ("EmitNode", None),
        ("EmitPerParticipant", Some(InstanceScope::PerParticipant)),
    ] {
        executor
            .register_factory(Arc::new(EmitFactory {
                node_type,
                scope,
                probe: probe.clone(),
            }))
            .await;
    }
    executor.register_factory(Arc::new(SlowFactory)).await;
    (executor, probe)
}

/// `nodes`: (id, type, params).
fn manifest(nodes: &[(&str, &str, serde_json::Value)], connections: &[(&str, &str)]) -> Arc<Manifest> {
    Arc::new(Manifest {
        version: "v1".to_string(),
        metadata: ManifestMetadata {
            name: "emit-test".to_string(),
            ..Default::default()
        },
        nodes: nodes
            .iter()
            .map(|(id, node_type, params)| NodeManifest {
                id: id.to_string(),
                node_type: node_type.to_string(),
                // `session_input` in the params stands for the manifest field.
                session_input: params.get("session_input").and_then(|v| v.as_bool()),
                params: params.clone(),
                ..Default::default()
            })
            .collect(),
        connections: connections
            .iter()
            .map(|(from, to)| Connection {
                from: from.to_string(),
                to: to.to_string(),
                ..Default::default()
            })
            .collect(),
        python_env: None,
        plugins: Vec::new(),
    })
}

fn one(node_type: &str) -> Arc<Manifest> {
    manifest(&[("node", node_type, serde_json::json!({}))], &[])
}

fn user(id: &str) -> Participant {
    Participant::new(id, participant::role::USER).with_track_id(format!("{id}-mic"))
}

async fn say(handle: &ParticipantSessionHandle, command: &str) {
    handle.send(TransportData::new(text(command))).await.unwrap();
}

/// The next output as (text, participant id).
async fn next(session: &mut SessionHandle) -> (String, Option<String>) {
    let output = tokio::time::timeout(Duration::from_secs(5), session.recv_output())
        .await
        .expect("output in time")
        .unwrap()
        .expect("session open");
    let RuntimeData::Text(body) = &output.data else {
        panic!("expected text, got {:?}", output.data)
    };
    (body.clone(), output.metadata.get(participant::ID).cloned())
}

async fn collect(session: &mut SessionHandle, count: usize) -> HashMap<String, Option<String>> {
    let mut seen = HashMap::new();
    for _ in 0..count {
        let (body, who) = next(session).await;
        seen.insert(body, who);
    }
    seen
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_started_in_initialize_emits_with_no_input() {
    let (executor, probe) = executor().await;
    let mut session = executor
        .create_session(manifest(&[("node", "EmitNode", serde_json::json!({"ticks": 3}))], &[]))
        .await
        .unwrap();
    let control = executor.control_bus().get(&session.session_id).expect("session control");
    let mut tap = control.subscribe(&ControlAddress::node_out("node")).unwrap();

    // Nothing was sent to the session. Frames arrive in the order emitted,
    // attributed to nobody.
    for n in 0..3 {
        assert_eq!(next(&mut session).await, (format!("tick-{n}"), None));
    }
    // Observable like any output: on the node's tap and in its telemetry.
    let telemetry = control.telemetry().expect("telemetry");
    let cell = telemetry.node(telemetry.node_index("node").unwrap());
    assert_eq!(cell.emitted.load(Ordering::Relaxed), 3);
    assert_eq!(cell.outputs.load(Ordering::Relaxed), 3);
    assert_eq!(cell.inputs.load(Ordering::Relaxed), 0);
    assert!(matches!(tap.try_recv(), Ok(RuntimeData::Text(body)) if body == "tick-0"));

    // Closing the session closes the emitter: the task sees it, and a later
    // emit is an error.
    session.close().await.unwrap();
    let emitter = probe.emitters.lock().unwrap()[0].clone();
    tokio::time::timeout(Duration::from_secs(5), emitter.closed())
        .await
        .expect("emitter closed with the session");
    assert!(emitter.emit(text("late")).await.is_err());
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(probe.saw_close.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_emit_after_the_call_returned_is_delivered_unattributed() {
    let (executor, _probe) = executor().await;
    let mut session = executor.create_session(one("EmitNode")).await.unwrap();
    let alice = session.join_participant(user("alice")).unwrap();

    say(&alice, "after:done").await;
    // The reply is alice's; the frame emitted later belongs to no input.
    assert_eq!(next(&mut session).await, ("started".into(), Some("alice".into())));
    assert_eq!(next(&mut session).await, ("done".into(), None));
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replies_and_emits_of_one_node_keep_their_order() {
    let (executor, _probe) = executor().await;
    let mut session = executor.create_session(one("EmitNode")).await.unwrap();
    let alice = session.join_participant(user("alice")).unwrap();

    say(&alice, "order").await;
    assert_eq!(next(&mut session).await, ("reply-1".into(), Some("alice".into())));
    // Emitted during alice's turn, and still not alice's.
    assert_eq!(next(&mut session).await, ("emitted".into(), None));
    assert_eq!(next(&mut session).await, ("reply-2".into(), Some("alice".into())));
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_keeps_an_inputs_origin_and_emits_with_it_later() {
    let (executor, _probe) = executor().await;
    let mut session = executor.create_session(one("EmitNode")).await.unwrap();
    let alice = session.join_participant(user("alice")).unwrap();
    let bob = session.join_participant(user("bob")).unwrap();

    say(&alice, "remember").await;
    assert_eq!(next(&mut session).await, ("remembered".into(), Some("alice".into())));
    // Bob's input triggers the emit; the frame carries what the node kept.
    say(&bob, "later:for alice").await;
    let seen = collect(&mut session, 2).await;
    assert_eq!(seen["for alice"].as_deref(), Some("alice"));
    assert_eq!(seen["ack"].as_deref(), Some("bob"));
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_emits_as_a_current_participant_by_id() {
    let (executor, _probe) = executor().await;
    let mut session = executor.create_session(one("EmitNode")).await.unwrap();
    let alice = session.join_participant(user("alice")).unwrap();
    let bob = session.join_participant(user("bob")).unwrap();

    say(&alice, "hello").await;
    assert_eq!(next(&mut session).await.0, "hello");

    // The frame carries alice's fields as the session knows them.
    say(&bob, "as:alice:on her behalf").await;
    let mut attributed = None;
    for _ in 0..2 {
        let output = tokio::time::timeout(Duration::from_secs(5), session.recv_output())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if matches!(&output.data, RuntimeData::Text(body) if body == "on her behalf") {
            attributed = Some(output);
        }
    }
    let attributed = attributed.expect("the emitted frame");
    assert_eq!(attributed.metadata.get(participant::ID).map(String::as_str), Some("alice"));
    assert_eq!(
        attributed.metadata.get(participant::TRACK_ID).map(String::as_str),
        Some("alice-mic")
    );

    // Someone who never sent input is not a participant: an error, and
    // nothing is delivered for it.
    say(&bob, "as:ghost:boo").await;
    let (reply, _) = next(&mut session).await;
    assert!(reply.starts_with("error:") && reply.contains("ghost"), "{reply}");

    // Someone who left is not one either.
    alice.leave().await.unwrap();
    say(&bob, "as:alice:too late").await;
    let (reply, _) = next(&mut session).await;
    assert!(reply.starts_with("error:"), "{reply}");
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_per_participant_instance_emits_as_its_participant() {
    let (executor, _probe) = executor().await;
    let mut session = executor.create_session(one("EmitPerParticipant")).await.unwrap();
    let alice = session.join_participant(user("alice")).unwrap();
    let bob = session.join_participant(user("bob")).unwrap();

    // Alice is served by the instance created at session start, bob by one
    // created for him; both emit as their participant without saying so.
    say(&alice, "after:from alice's instance").await;
    say(&bob, "after:from bob's instance").await;
    say(&bob, "nobody:from nobody").await;
    let seen = collect(&mut session, 6).await;
    assert_eq!(seen["from alice's instance"].as_deref(), Some("alice"));
    assert_eq!(seen["from bob's instance"].as_deref(), Some("bob"));
    assert_eq!(seen["from nobody"], None);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slow_downstream_node_delays_emits_and_drops_none() {
    const FRAMES: usize = 400;
    let (executor, _probe) = executor().await;
    let mut session = executor
        .create_session(manifest(
            &[
                ("source", "EmitNode", serde_json::json!({"ticks": FRAMES})),
                ("slow", "SlowNode", serde_json::json!({})),
            ],
            &[("source", "slow")],
        ))
        .await
        .unwrap();
    // More frames than the queues between the two nodes hold: the emitter
    // has to wait for room, and every frame arrives, in order.
    for n in 0..FRAMES {
        assert_eq!(next(&mut session).await.0, format!("tick-{n}"));
    }
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_barge_in_keeps_emitted_frames() {
    let (executor, _probe) = executor().await;
    let mut session = executor
        .create_session(manifest(
            &[
                ("source", "EmitNode", serde_json::json!({"ticks": 20})),
                ("slow", "SlowNode", serde_json::json!({})),
            ],
            &[("source", "slow")],
        ))
        .await
        .unwrap();
    // Frames are queued behind the slow node when the barge-in reaches the
    // node that emitted them.
    session
        .send_input(TransportData::new(wrap_aux_port(
            BARGE_IN_PORT,
            RuntimeData::Json(serde_json::json!({})),
        )))
        .await
        .unwrap();
    for n in 0..20 {
        assert_eq!(next(&mut session).await.0, format!("tick-{n}"));
    }
    session.close().await.unwrap();
}

// ── Self-driven nodes ────────────────────────────────────────────────────

async fn ended(session: &mut SessionHandle) -> bool {
    matches!(
        tokio::time::timeout(Duration::from_secs(5), session.recv_output()).await,
        Ok(Ok(None)) | Ok(Err(_))
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_input_does_not_reach_a_self_driven_source() {
    let (executor, _probe) = executor().await;
    // Two sources. The manifest says one of them takes no session input.
    let mut session = executor
        .create_session(manifest(
            &[
                ("source", "EmitNode", serde_json::json!({"ticks": 2, "session_input": false})),
                ("echo", "EmitNode", serde_json::json!({})),
            ],
            &[],
        ))
        .await
        .unwrap();
    session.send_input(TransportData::new(text("hello"))).await.unwrap();
    // Two frames from the source, and the input answered once: by `echo` only.
    let mut seen = Vec::new();
    for _ in 0..3 {
        seen.push(next(&mut session).await.0);
    }
    seen.sort();
    assert_eq!(seen, ["hello", "tick-0", "tick-1"]);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), session.recv_output()).await.is_err(),
        "the self-driven source answered client input"
    );
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_with_only_a_self_driven_source_runs_without_input_and_ends_with_it() {
    let (executor, _probe) = executor().await;
    // The node declares it itself; nothing in the manifest.
    let mut session = executor
        .create_session(manifest(
            &[(
                "source",
                "EmitNode",
                serde_json::json!({"ticks": 3, "tick_ms": 20, "finish": true, "self_driven": true}),
            )],
            &[],
        ))
        .await
        .unwrap();
    for n in 0..3 {
        assert_eq!(next(&mut session).await.0, format!("tick-{n}"));
    }
    // The source finished: the session drains and ends, though nobody
    // closed it and the client still holds its input.
    assert!(ended(&mut session).await, "the session did not end with its source");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_end_of_client_input_does_not_end_a_session_with_a_running_source() {
    let (executor, _probe) = executor().await;
    let mut session = executor
        .create_session(manifest(
            &[
                (
                    "source",
                    "EmitNode",
                    serde_json::json!({"ticks": 4, "tick_ms": 150, "finish": true, "session_input": false}),
                ),
                ("echo", "EmitNode", serde_json::json!({})),
            ],
            &[],
        ))
        .await
        .unwrap();
    session.send_input(TransportData::new(text("last words"))).await.unwrap();
    session.signal_input_complete();
    // The input-driven branch drains, and the source's output keeps coming.
    let mut seen = Vec::new();
    for _ in 0..5 {
        seen.push(next(&mut session).await.0);
    }
    seen.sort();
    assert_eq!(seen, ["last words", "tick-0", "tick-1", "tick-2", "tick-3"]);
    // Input has ended and the source has finished: now the session ends.
    assert!(ended(&mut session).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unary_execution_rejects_a_pipeline_without_an_input_taking_node() {
    let (executor, _probe) = executor().await;
    let error = executor
        .execute_unary(
            manifest(&[("source", "EmitNode", serde_json::json!({"session_input": false}))], &[]),
            TransportData::new(text("anyone?")),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("self-driven"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_per_participant_self_driven_instance_runs_from_join_to_leave() {
    let (executor, probe) = executor().await;
    let mut session = executor
        .create_session(manifest(
            &[(
                "source",
                "EmitPerParticipant",
                serde_json::json!({"ticks": 1, "tick_ms": 30, "session_input": false}),
            )],
            &[],
        ))
        .await
        .unwrap();
    let alice = session.join_participant(user("alice")).unwrap();
    let bob = session.join_participant(user("bob")).unwrap();
    // Joining sends no data; each participant gets an instance that emits as it.
    alice.join().await.unwrap();
    bob.join().await.unwrap();
    let mut who = vec![next(&mut session).await, next(&mut session).await];
    who.sort();
    assert_eq!(
        who,
        [("tick-0".to_string(), Some("alice".to_string())), ("tick-0".to_string(), Some("bob".to_string()))]
    );

    // Alice's instance stops when she leaves; bob's keeps running.
    alice.leave().await.unwrap();
    let mut closed = Vec::new();
    for _ in 0..100 {
        closed = probe.emitters.lock().unwrap().iter().map(Emitter::is_closed).collect::<Vec<_>>();
        if closed.iter().any(|closed| *closed) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(closed.iter().filter(|closed| **closed).count(), 1, "{closed:?}");
    session.close().await.unwrap();
}
