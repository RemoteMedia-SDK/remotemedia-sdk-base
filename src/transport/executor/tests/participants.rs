//! Per-participant node instances (openspec: add-multi-participant-sessions,
//! `participant-scoped-nodes`).
use super::*;
use crate::nodes::{AsyncNodeWrapper, AsyncStreamingNode, InstanceScope, StreamingNode, StreamingNodeFactory};
use crate::transport::data::participant;
use crate::transport::session_control::aux_port_of;
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// Counts its inputs and answers `"{instance}|{count}|{text}"`; declares
/// `per_participant` so each participant gets its own count.
struct Counter {
    instance: String,
    count: AtomicUsize,
    shutdowns: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl AsyncStreamingNode for Counter {
    fn node_type(&self) -> &str {
        "ParticipantCounter"
    }
    async fn process(&self, data: RuntimeData) -> crate::Result<RuntimeData> {
        let RuntimeData::Text(text) = data else {
            return Ok(data);
        };
        let count = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(RuntimeData::Text(format!("{}|{count}|{text}", self.instance)))
    }
    async fn process_control_message(&self, data: RuntimeData, _: Option<String>) -> crate::Result<bool> {
        if aux_port_of(&data).as_deref() == Some("shutdown") {
            self.shutdowns.fetch_add(1, Ordering::SeqCst);
            return Ok(true);
        }
        Ok(false)
    }
    fn instance_scope(&self) -> Option<InstanceScope> {
        Some(InstanceScope::PerParticipant)
    }
}

struct CounterFactory {
    created: Arc<AtomicUsize>,
    shutdowns: Arc<AtomicUsize>,
}

impl StreamingNodeFactory for CounterFactory {
    fn create(
        &self,
        node_id: String,
        _params: &serde_json::Value,
        _session_id: Option<String>,
    ) -> crate::Result<Box<dyn StreamingNode>> {
        self.created.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(AsyncNodeWrapper(Arc::new(Counter {
            instance: node_id,
            count: AtomicUsize::new(0),
            shutdowns: self.shutdowns.clone(),
        }))))
    }
    fn node_type(&self) -> &str {
        "ParticipantCounter"
    }
}

struct Fixture {
    executor: PipelineExecutor,
    created: Arc<AtomicUsize>,
    shutdowns: Arc<AtomicUsize>,
}

async fn fixture() -> Fixture {
    let executor = PipelineExecutor::new().unwrap();
    let created = Arc::new(AtomicUsize::new(0));
    let shutdowns = Arc::new(AtomicUsize::new(0));
    executor
        .register_factory(Arc::new(CounterFactory {
            created: created.clone(),
            shutdowns: shutdowns.clone(),
        }))
        .await;
    Fixture { executor, created, shutdowns }
}

/// `nodes`: (id, type, manifest instance_scope, max_instances).
fn manifest(
    nodes: &[(&str, &str, Option<InstanceScope>, Option<usize>)],
    connections: &[(&str, &str)],
) -> Arc<Manifest> {
    Arc::new(Manifest {
        version: "v1".to_string(),
        metadata: ManifestMetadata {
            name: "participants-test".to_string(),
            ..Default::default()
        },
        nodes: nodes
            .iter()
            .map(|(id, node_type, scope, max)| NodeManifest {
                id: id.to_string(),
                node_type: node_type.to_string(),
                params: serde_json::json!({}),
                instance_scope: *scope,
                max_instances: *max,
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

fn user(id: &str) -> Participant {
    Participant::new(id, participant::role::USER)
}

async fn text(handle: &ParticipantSessionHandle, text: &str) {
    handle
        .send(TransportData::new(RuntimeData::Text(text.into())))
        .await
        .unwrap();
}

/// `(participant, instance, count, text)` for each of `n` outputs.
async fn collect(session: &mut SessionHandle, n: usize) -> Vec<(String, String, usize, String)> {
    let mut out = Vec::new();
    for _ in 0..n {
        let output = tokio::time::timeout(Duration::from_secs(5), session.recv_output())
            .await
            .expect("output in time")
            .unwrap()
            .expect("session open");
        let RuntimeData::Text(body) = &output.data else {
            panic!("expected text, got {:?}", output.data)
        };
        let mut parts = body.splitn(3, '|');
        let instance = parts.next().unwrap().to_string();
        let count = parts.next().unwrap().parse().unwrap();
        let text = parts.next().unwrap().to_string();
        let who = output.metadata.get(participant::ID).cloned().unwrap_or_default();
        out.push((who, instance, count, text));
    }
    out
}

async fn no_more_output(session: &mut SessionHandle) {
    assert!(
        tokio::time::timeout(Duration::from_millis(300), session.recv_output())
            .await
            .is_err(),
        "unexpected output"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn five_participants_get_their_own_instance_and_state() {
    let f = fixture().await;
    let mut session = f
        .executor
        .create_session(manifest(
            &[("count", "ParticipantCounter", None, None), ("echo", "PassThrough", None, None)],
            &[("count", "echo")],
        ))
        .await
        .unwrap();
    let users: Vec<_> = (1..=5)
        .map(|i| session.join_participant(user(&format!("user-{i}"))).unwrap())
        .collect();
    // Interleaved: every participant's frames alternate with the others'.
    for round in 1..=3 {
        for handle in &users {
            text(handle, &format!("r{round}")).await;
        }
    }
    let outputs = collect(&mut session, 15).await;
    let mut per_user: BTreeMap<String, Vec<(usize, String)>> = BTreeMap::new();
    let mut instance_of: BTreeMap<String, HashSet<String>> = BTreeMap::new();
    for (who, instance, count, text) in outputs {
        per_user.entry(who.clone()).or_default().push((count, text));
        instance_of.entry(who).or_default().insert(instance);
    }
    assert_eq!(per_user.len(), 5, "{per_user:?}");
    for (who, seen) in &per_user {
        // Each participant's own counter, in order: 1, 2, 3.
        assert_eq!(seen, &[(1, "r1".to_string()), (2, "r2".into()), (3, "r3".into())], "{who}");
        assert_eq!(instance_of[who].len(), 1, "{who} used one instance");
    }
    let instances: HashSet<_> = instance_of.values().flatten().cloned().collect();
    assert_eq!(instances.len(), 5);
    // The startup instance plus four created for later participants.
    assert_eq!(f.created.load(Ordering::SeqCst), 5);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_participant_that_leaves_has_its_instance_shut_down() {
    let f = fixture().await;
    let mut session = f
        .executor
        .create_session(manifest(&[("count", "ParticipantCounter", None, None)], &[]))
        .await
        .unwrap();
    let alice = session.join_participant(user("alice")).unwrap();
    let bob = session.join_participant(user("bob")).unwrap();
    text(&alice, "a1").await;
    text(&bob, "b1").await;
    collect(&mut session, 2).await;
    assert_eq!(f.shutdowns.load(Ordering::SeqCst), 0);

    alice.leave().await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while f.shutdowns.load(Ordering::SeqCst) == 0 {
        assert!(std::time::Instant::now() < deadline, "alice's instance did not shut down");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Bob's instance keeps its state.
    text(&bob, "b2").await;
    let out = collect(&mut session, 1).await;
    assert_eq!((out[0].0.as_str(), out[0].2), ("bob", 2));
    // Alice coming back starts a fresh instance.
    text(&alice, "a2").await;
    let out = collect(&mut session, 1).await;
    assert_eq!((out[0].0.as_str(), out[0].2), ("alice", 1));
    session.close().await.unwrap();
    session.wait().await.unwrap();
    // Alice's first instance (on leave), then Bob's and Alice's second (on close).
    assert_eq!(f.shutdowns.load(Ordering::SeqCst), 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unattributed_input_has_its_own_default_instance() {
    let f = fixture().await;
    let mut session = f
        .executor
        .create_session(manifest(&[("count", "ParticipantCounter", None, None)], &[]))
        .await
        .unwrap();
    let alice = session.join_participant(user("alice")).unwrap();
    for _ in 0..2 {
        session
            .send_input(TransportData::new(RuntimeData::Text("anon".into())))
            .await
            .unwrap();
    }
    text(&alice, "a1").await;
    let mut out = collect(&mut session, 3).await;
    out.sort();
    assert_eq!(
        out.iter().map(|(who, _, count, _)| (who.as_str(), *count)).collect::<Vec<_>>(),
        vec![("", 1), ("", 2), ("alice", 1)]
    );
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn max_instances_refuses_further_participants() {
    let f = fixture().await;
    let mut session = f
        .executor
        .create_session(manifest(&[("count", "ParticipantCounter", None, Some(2))], &[]))
        .await
        .unwrap();
    let handles: Vec<_> = ["alice", "bob", "carol"]
        .iter()
        .map(|id| session.join_participant(user(id)).unwrap())
        .collect();
    for handle in &handles {
        text(handle, "hi").await;
    }
    let mut out = collect(&mut session, 2).await;
    out.sort();
    assert_eq!(out[0].0, "alice");
    assert_eq!(out[1].0, "bob");
    no_more_output(&mut session).await;
    assert_eq!(f.created.load(Ordering::SeqCst), 2);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_manifest_can_make_a_per_participant_node_shared() {
    let f = fixture().await;
    let mut session = f
        .executor
        .create_session(manifest(
            &[("count", "ParticipantCounter", Some(InstanceScope::Shared), None)],
            &[],
        ))
        .await
        .unwrap();
    let alice = session.join_participant(user("alice")).unwrap();
    let bob = session.join_participant(user("bob")).unwrap();
    text(&alice, "a").await;
    let first = collect(&mut session, 1).await;
    text(&bob, "b").await;
    let second = collect(&mut session, 1).await;
    // One instance: the count carries on across participants.
    assert_eq!((first[0].2, second[0].2), (1, 2));
    assert_eq!(first[0].1, second[0].1);
    assert_eq!(f.created.load(Ordering::SeqCst), 1);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chain_of_per_participant_nodes_keeps_each_participant_apart() {
    let f = fixture().await;
    let mut session = f
        .executor
        .create_session(manifest(
            &[("first", "ParticipantCounter", None, None), ("second", "ParticipantCounter", None, None)],
            &[("first", "second")],
        ))
        .await
        .unwrap();
    let alice = session.join_participant(user("alice")).unwrap();
    let bob = session.join_participant(user("bob")).unwrap();
    for _ in 0..2 {
        text(&alice, "a").await;
        text(&bob, "b").await;
    }
    let out = collect(&mut session, 4).await;
    // `second` sees "<first instance>|<count>|text": its own count per participant.
    let mut counts: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (who, _, count, _) in out {
        counts.entry(who).or_default().push(count);
    }
    assert_eq!(counts["alice"], vec![1, 2]);
    assert_eq!(counts["bob"], vec![1, 2]);
    // Two nodes x two participants.
    assert_eq!(f.created.load(Ordering::SeqCst), 4);
    session.close().await.unwrap();
}

/// Paces its own output (10 Hz): cannot be split by participant.
struct Ticker;

#[async_trait::async_trait]
impl AsyncStreamingNode for Ticker {
    fn node_type(&self) -> &str {
        "PacedTicker"
    }
    async fn process(&self, data: RuntimeData) -> crate::Result<RuntimeData> {
        Ok(data)
    }
    fn pacing_nature(&self) -> crate::nodes::PacingNature {
        crate::nodes::PacingNature::SourceWall(10)
    }
}

struct TickerFactory;

impl StreamingNodeFactory for TickerFactory {
    fn create(
        &self,
        _node_id: String,
        _params: &serde_json::Value,
        _session_id: Option<String>,
    ) -> crate::Result<Box<dyn StreamingNode>> {
        Ok(Box::new(AsyncNodeWrapper(Arc::new(Ticker))))
    }
    fn node_type(&self) -> &str {
        "PacedTicker"
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paced_node_cannot_run_per_participant() {
    let f = fixture().await;
    f.executor.register_factory(Arc::new(TickerFactory)).await;
    let error = f
        .executor
        .create_session(manifest(
            &[("ticker", "PacedTicker", Some(InstanceScope::PerParticipant), None)],
            &[],
        ))
        .await
        .err()
        .expect("session creation fails");
    assert!(error.to_string().contains("cannot run per participant"), "{error}");
    assert!(error.to_string().contains("ticker"), "{error}");
}

#[test]
fn built_in_stateful_audio_nodes_run_per_participant() {
    let registry = crate::nodes::streaming_registry::create_default_streaming_registry();
    for (node_type, params) in [
        ("AudioBufferAccumulatorNode", serde_json::json!({})),
        ("AudioChunkerNode", serde_json::json!({})),
        ("TextCollectorNode", serde_json::json!({})),
        ("SpeculativeVADGate", serde_json::json!({})), // declared through #[node]
        ("FastResampleNode", serde_json::json!({"target_rate": 16000})),
    ] {
        let node = registry
            .create_node(node_type, "n".into(), &params, Some("s".into()))
            .unwrap_or_else(|e| panic!("{node_type}: {e}"));
        assert_eq!(node.instance_scope(), Some(InstanceScope::PerParticipant), "{node_type}");
    }
    // Stateless nodes keep the default (shared).
    let node = registry
        .create_node("PassThrough", "n".into(), &serde_json::json!({}), None)
        .unwrap();
    assert_eq!(node.instance_scope(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_descriptor_shows_scope_and_telemetry_counts_live_instances() {
    let f = fixture().await;
    let mut session = f
        .executor
        .create_session(manifest(
            &[("count", "ParticipantCounter", None, None), ("echo", "PassThrough", None, None)],
            &[("count", "echo")],
        ))
        .await
        .unwrap();
    let control = f.executor.control_bus().get(&session.session_id).expect("session control");
    let descriptor = control.descriptor().expect("descriptor");
    let scope = |id: &str| {
        descriptor
            .nodes
            .iter()
            .find(|n| n.id == id)
            .and_then(|n| n.instance_scope.clone())
    };
    assert_eq!(scope("count").as_deref(), Some("per_participant"));
    assert_eq!(scope("echo").as_deref(), Some("shared"));

    let telemetry = control.telemetry().expect("telemetry");
    let instances = |id: &str| {
        let snapshot = crate::telemetry::publisher::build_snapshot(
            &telemetry,
            &mut crate::telemetry::publisher::PublisherState::default(),
            &std::collections::HashMap::new(),
            false,
            false,
        );
        snapshot.nodes.iter().find(|n| n.id == id).and_then(|n| n.instances)
    };
    assert_eq!(instances("count"), None, "no participant yet");

    let alice = session.join_participant(user("alice")).unwrap();
    let bob = session.join_participant(user("bob")).unwrap();
    text(&alice, "a1").await;
    text(&bob, "b1").await;
    collect(&mut session, 2).await;
    assert_eq!(instances("count"), Some(2));
    assert_eq!(instances("echo"), None, "shared nodes report no instance count");

    alice.leave().await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while instances("count") != Some(1) {
        assert!(std::time::Instant::now() < deadline, "count stayed {:?}", instances("count"));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    session.close().await.unwrap();
}
