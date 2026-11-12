//! End-to-end telemetry tests: real `SessionRouter` task graph, test nodes,
//! snapshots read off the `__telemetry__` tap (openspec:
//! add-live-pipeline-observer, `pipeline-telemetry` scenarios).

use super::*;
use crate::manifest::{Connection, ManifestMetadata, NodeManifest};
use crate::telemetry::{NodeStatus, TelemetrySnapshot};
use crate::transport::session_control::{
    ControlAddress, NodeState, SessionControl, PERF_PORT, TELEMETRY_PORT,
};
use std::time::Duration;

/// Behaviour knobs for the generic test node.
#[derive(Clone, Default)]
struct Behaviour {
    sleep: Option<Duration>,
    fail: bool,
    /// Block every dispatch until the watch value becomes `true`.
    gate: Option<tokio::sync::watch::Receiver<bool>>,
}

struct TestNode {
    behaviour: Behaviour,
}

#[async_trait::async_trait]
impl crate::nodes::AsyncStreamingNode for TestNode {
    fn node_type(&self) -> &str {
        "TestNode"
    }
    async fn process(&self, data: RuntimeData) -> crate::Result<RuntimeData> {
        Ok(data)
    }
    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        _session: Option<String>,
        mut callback: F,
    ) -> crate::Result<usize>
    where
        F: FnMut(RuntimeData) -> crate::Result<()> + Send,
    {
        if let Some(d) = self.behaviour.sleep {
            tokio::time::sleep(d).await;
        }
        if let Some(gate) = &self.behaviour.gate {
            let mut g = gate.clone();
            let _ = g.wait_for(|open| *open).await;
        }
        if self.behaviour.fail {
            return Err(crate::Error::Execution("boom".into()));
        }
        callback(data)?;
        Ok(1)
    }
}

fn manifest(nodes: &[&str], conns: &[(&str, &str)]) -> Manifest {
    Manifest {
        version: "v1".into(),
        metadata: ManifestMetadata {
            name: "telemetry-test".into(),
            ..Default::default()
        },
        nodes: nodes
            .iter()
            .map(|id| NodeManifest {
                id: id.to_string(),
                node_type: "TestNode".into(),
                params: serde_json::json!({}),
                ..Default::default()
            })
            .collect(),
        connections: conns
            .iter()
            .map(|(f, t)| Connection {
                from: f.to_string(),
                to: t.to_string(),
                from_port: None,
                to_port: None,
                edge_transport: None,
            })
            .collect(),
        ..Default::default()
    }
}

struct Harness {
    control: Arc<SessionControl>,
    input: crate::transport::SessionPacketSender,
    outputs: ClientOutputReceivers,
    shutdown: mpsc::Sender<()>,
    task: JoinHandle<Result<()>>,
    tel: Arc<SessionTelemetry>,
}

async fn start(
    nodes: &[(&str, Behaviour)],
    conns: &[(&str, &str)],
    output_capacity: usize,
) -> Harness {
    let ids: Vec<&str> = nodes.iter().map(|(id, _)| *id).collect();
    let m = manifest(&ids, conns);
    let (out, outputs) = ClientOutputRouter::new(output_capacity);
    let (mut router, shutdown) = SessionRouter::new(
        format!("tel-{}", uuid_like()),
        Arc::new(m),
        Arc::new(StreamingNodeRegistry::new()),
        out,
    )
    .unwrap();
    let control = SessionControl::new(router.session_id.clone());
    router.attach_control(control.clone()).await;
    for (id, b) in nodes {
        router.cached_nodes.insert(
            id.to_string(),
            Box::new(crate::nodes::AsyncNodeWrapper(Arc::new(TestNode {
                behaviour: b.clone(),
            }))),
        );
    }
    let tel = router.telemetry();
    tel.set_interval_ms(250);
    let input = router.get_input_sender();
    let task = tokio::spawn(router.run_after_init());
    Harness {
        control,
        input,
        outputs,
        shutdown,
        task,
        tel,
    }
}

fn uuid_like() -> u64 {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

fn packet(i: u64) -> DataPacket {
    DataPacket {
        data: RuntimeData::Text(format!("msg-{i}")),
        from_node: "client".into(),
        to_node: None,
        session_id: String::new(),
        sequence: i,
        sub_sequence: 0,
        metadata: Default::default(),
    }
}

async fn next_snapshot(
    rx: &mut tokio::sync::broadcast::Receiver<RuntimeData>,
) -> TelemetrySnapshot {
    let data = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("snapshot within 5s")
        .expect("tap open");
    match data {
        RuntimeData::Json(v) => serde_json::from_value(v).expect("valid snapshot"),
        other => panic!("unexpected tap payload {other:?}"),
    }
}

/// Wait until `pred` holds for a snapshot (or time out).
async fn snapshot_where(
    rx: &mut tokio::sync::broadcast::Receiver<RuntimeData>,
    pred: impl Fn(&TelemetrySnapshot) -> bool,
) -> TelemetrySnapshot {
    for _ in 0..40 {
        let s = next_snapshot(rx).await;
        if pred(&s) {
            return s;
        }
    }
    panic!("condition not reached within 40 snapshots");
}

fn node<'a>(s: &'a TelemetrySnapshot, id: &str) -> &'a crate::telemetry::NodeTelemetry {
    s.nodes.iter().find(|n| n.id == id).expect("node present")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edge_frames_match_node_outputs() {
    let mut h = start(
        &[("A", Behaviour::default()), ("B", Behaviour::default()), ("C", Behaviour::default())],
        &[("A", "B"), ("B", "C")],
        256,
    )
    .await;
    let mut tap = h
        .control
        .subscribe(&ControlAddress::node_out(TELEMETRY_PORT))
        .unwrap();
    for i in 0..100 {
        h.input.send(packet(i)).await.unwrap();
    }
    let mut got = 0;
    while got < 100 {
        tokio::time::timeout(Duration::from_secs(5), h.outputs.data_rx.recv())
            .await
            .unwrap()
            .unwrap();
        got += 1;
    }
    let s = snapshot_where(&mut tap, |s| node(s, "C").outputs == 100).await;
    let desc = h.control.descriptor().unwrap();
    let idx = |from: &str, to: &str| {
        desc.edges
            .iter()
            .position(|e| e.from == from && e.to == to)
            .unwrap()
    };
    assert_eq!(node(&s, "A").inputs, 100);
    assert_eq!(s.edges[idx("A", "B")].frames, node(&s, "A").outputs);
    assert_eq!(s.edges[idx("B", "C")].frames, node(&s, "B").outputs);
    assert_eq!(s.edges[desc.ingress_edge("A").unwrap()].frames, 100);
    assert_eq!(s.edges[desc.egress_edge("C").unwrap()].frames, 100);
    assert!(s.edges.iter().all(|e| e.drops == 0));
    assert!(s.edges[idx("A", "B")].bytes > 0);
    // Histograms come on automatically while observed.
    assert!(s.histograms);

    let _ = h.shutdown.send(()).await;
    // Final snapshot arrives before the tap closes.
    let fin = snapshot_where(&mut tap, |s| s.is_final).await;
    assert!(fin.nodes.iter().all(|n| n.status == NodeStatus::Stopped));
    h.task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_consumer_shows_queue_depth_and_stalls_without_drops() {
    const N: u64 = 1100; // > B's filter (1024) + input (16) capacity
    let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
    let mut h = start(
        &[
            ("A", Behaviour::default()),
            (
                "B",
                Behaviour {
                    gate: Some(gate_rx),
                    ..Default::default()
                },
            ),
        ],
        &[("A", "B")],
        4096,
    )
    .await;
    let mut tap = h
        .control
        .subscribe(&ControlAddress::node_out(TELEMETRY_PORT))
        .unwrap();
    let input = h.input.clone();
    let feeder = tokio::spawn(async move {
        for i in 0..N {
            input.send(packet(i)).await.unwrap();
        }
    });
    let desc = h.control.descriptor().unwrap();
    let ab = desc
        .edges
        .iter()
        .position(|e| e.from == "A" && e.to == "B")
        .unwrap();
    // While B is blocked: its filter backlog fills and A→B stalls.
    let s = snapshot_where(&mut tap, |s| {
        node(s, "B").q.filter > 1000 && s.edges[ab].stalls > 0
    })
    .await;
    assert_eq!(node(&s, "B").status, NodeStatus::Backpressured);
    assert_eq!(node(&s, "A").status, NodeStatus::Backpressured);
    // Release B and drain everything: nothing may be dropped.
    gate_tx.send(true).unwrap();
    feeder.await.unwrap();
    for _ in 0..N {
        tokio::time::timeout(Duration::from_secs(10), h.outputs.data_rx.recv())
            .await
            .unwrap()
            .unwrap();
    }
    let s = snapshot_where(&mut tap, |s| node(s, "B").outputs == N).await;
    assert!(s.edges.iter().all(|e| e.drops == 0));
    assert!(s.edges[ab].stall_us > 0);
    assert_eq!(s.edges[ab].frames, N);
    let _ = h.shutdown.send(()).await;
    h.task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn errors_are_counted_and_published_coalesced() {
    let h = start(
        &[(
            "F",
            Behaviour {
                fail: true,
                ..Default::default()
            },
        )],
        &[],
        64,
    )
    .await;
    let mut tap = h
        .control
        .subscribe(&ControlAddress::node_out(TELEMETRY_PORT))
        .unwrap();
    let mut sys = h.control.subscribe_system();
    for i in 0..50 {
        h.input.send(packet(i)).await.unwrap();
    }
    let s = snapshot_where(&mut tap, |s| node(s, "F").errors == 50).await;
    let f = node(&s, "F");
    // 50 straight failures may also trip the scheduler's circuit breaker,
    // which outranks `errored`.
    assert!(
        matches!(f.status, NodeStatus::Errored | NodeStatus::CircuitOpen),
        "status {:?}",
        f.status
    );
    let last = f.last_error.as_ref().unwrap();
    let msg = last.message.to_lowercase();
    assert!(
        msg.contains("boom") || msg.contains("circuit"),
        "last error {msg:?}"
    );

    // node_error events: coalesced, counts add up to 50 eventually.
    let mut total = 0;
    let mut events = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    while total < 50 && tokio::time::Instant::now() < deadline {
        let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(500), sys.recv()).await else {
            continue;
        };
        if let RuntimeData::Json(v) = ev {
            if v["kind"] == "node_error" {
                events += 1;
                total += v["count"].as_u64().unwrap();
            }
        }
    }
    assert_eq!(total, 50);
    assert!(events <= 6, "coalesced to ~1/s, got {events}");

    // Ephemeral: a late system subscriber does not replay node_error.
    let mut late = h.control.subscribe_system();
    while let Ok(Some(ev)) = tokio::time::timeout(Duration::from_millis(50), late.recv()).await {
        if let RuntimeData::Json(v) = ev {
            assert_ne!(v["kind"], "node_error");
        }
    }
    let _ = h.shutdown.send(()).await;
    h.task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bypass_and_disable_are_counted() {
    let mut h = start(
        &[("A", Behaviour::default()), ("B", Behaviour::default())],
        &[("A", "B")],
        256,
    )
    .await;
    let mut tap = h
        .control
        .subscribe(&ControlAddress::node_out(TELEMETRY_PORT))
        .unwrap();
    h.control.set_node_state("B", NodeState::Bypass);
    for i in 0..10 {
        h.input.send(packet(i)).await.unwrap();
    }
    for _ in 0..10 {
        h.outputs.data_rx.recv().await.unwrap();
    }
    let s = snapshot_where(&mut tap, |s| node(s, "B").bypassed == 10).await;
    assert_eq!(node(&s, "B").status, NodeStatus::Bypassed);
    assert_eq!(node(&s, "B").inputs, 0);
    h.control.set_node_state("B", NodeState::Disabled);
    for i in 0..5 {
        h.input.send(packet(i)).await.unwrap();
    }
    let s = snapshot_where(&mut tap, |s| node(s, "B").discarded == 5).await;
    assert_eq!(node(&s, "B").status, NodeStatus::Disabled);
    h.control.clear_node_state("B");
    let _ = h.shutdown.send(()).await;
    h.task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_without_observers_and_late_subscriber_sees_cumulative() {
    let mut h = start(&[("A", Behaviour::default())], &[], 1024).await;
    for i in 0..500 {
        h.input.send(packet(i)).await.unwrap();
    }
    for _ in 0..500 {
        h.outputs.data_rx.recv().await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(700)).await;
    // Nobody subscribed → no snapshot was ever built.
    assert_eq!(h.tel.next_seq(), 1, "publisher must not build snapshots unobserved");
    // Histograms were off while unobserved.
    assert!(!h.tel.perf().is_enabled());
    let mut tap = h
        .control
        .subscribe(&ControlAddress::node_out(TELEMETRY_PORT))
        .unwrap();
    let s = next_snapshot(&mut tap).await;
    assert!(node(&s, "A").inputs >= 500);
    let _ = h.shutdown.send(()).await;
    h.task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn perf_tap_works_without_env_and_keeps_schema() {
    let mut h = start(&[("A", Behaviour::default())], &[], 1024).await;
    let mut perf = h
        .control
        .subscribe(&ControlAddress::node_out(PERF_PORT))
        .unwrap();
    // Let the publisher notice the subscriber and enable histograms.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let input = h.input.clone();
    let feeder = tokio::spawn(async move {
        for i in 0..200 {
            input.send(packet(i)).await.unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let v = loop {
        let d = tokio::time::timeout(Duration::from_secs(5), perf.recv())
            .await
            .unwrap()
            .unwrap();
        let RuntimeData::Json(v) = d else { panic!() };
        if v["nodes"]["A"]["outputs"].as_u64().unwrap_or(0) > 0 {
            break v;
        }
    };
    // Historical schema.
    assert_eq!(v["kind"], "perf_snapshot");
    assert!(v["window_ms"].is_u64());
    let snap: crate::data::PerfSnapshot = serde_json::from_value(v).unwrap();
    assert!(snap.nodes["A"].latency_us.max_us > 0 || snap.nodes["A"].outputs > 0);
    feeder.await.unwrap();
    while h.outputs.data_rx.try_recv().is_ok() {}
    let _ = h.shutdown.send(()).await;
    h.task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn counters_level_omits_latency() {
    let mut h = start(&[("A", Behaviour::default())], &[], 1024).await;
    h.tel.set_level(crate::telemetry::TelemetryLevel::Counters);
    let mut tap = h
        .control
        .subscribe(&ControlAddress::node_out(TELEMETRY_PORT))
        .unwrap();
    for i in 0..20 {
        h.input.send(packet(i)).await.unwrap();
    }
    for _ in 0..20 {
        h.outputs.data_rx.recv().await.unwrap();
    }
    let s = snapshot_where(&mut tap, |s| node(s, "A").outputs == 20).await;
    assert!(!s.histograms);
    assert!(node(&s, "A").latency_us.is_none());
    let _ = h.shutdown.send(()).await;
    h.task.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_with_observer_attached_does_not_hang() {
    let h = start(
        &[("A", Behaviour::default()), ("B", Behaviour::default())],
        &[("A", "B")],
        16,
    )
    .await;
    let _tap = h
        .control
        .subscribe(&ControlAddress::node_out(TELEMETRY_PORT))
        .unwrap();
    let _sys = h.control.subscribe_system();
    for i in 0..8 {
        h.input.send(packet(i)).await.unwrap();
    }
    // Outputs are never drained: the sink is blocked on a full client
    // channel while observers are attached.
    let _ = h.shutdown.send(()).await;
    tokio::time::timeout(Duration::from_secs(5), h.task)
        .await
        .expect("teardown must not hang with observers attached")
        .unwrap()
        .unwrap();
}
