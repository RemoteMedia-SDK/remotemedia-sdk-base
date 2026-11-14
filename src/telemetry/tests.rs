//! Unit tests for telemetry cells and snapshot building. End-to-end router
//! tests live in `tests/telemetry_router.rs`.

use super::*;
use crate::executor::{PipelineGraph, SchedulerConfig};
use crate::manifest::{Connection, Manifest, ManifestMetadata, NodeManifest};
use crate::nodes::StreamingNodeRegistry;

fn manifest(nodes: &[&str], conns: &[(&str, &str)]) -> Manifest {
    Manifest {
        version: "v1".into(),
        metadata: ManifestMetadata {
            name: "t".into(),
            ..Default::default()
        },
        nodes: nodes
            .iter()
            .map(|id| NodeManifest {
                id: id.to_string(),
                node_type: "Passthrough".into(),
                params: serde_json::json!({"api_key": "x", "gain": 2}),
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

fn telemetry_for(m: &Manifest) -> (SessionDescriptor, SessionTelemetry) {
    let graph = PipelineGraph::from_manifest(m).unwrap();
    let registry = StreamingNodeRegistry::new();
    let d = SessionDescriptor::build("s1", m, &graph, &registry, QueueCapacity::default());
    let t = SessionTelemetry::new(
        &d,
        Arc::new(StreamingScheduler::new(SchedulerConfig::default())),
        Arc::new(PerfAggregator::new("s1".into(), false, 1000)),
    );
    (d, t)
}

#[test]
fn descriptor_edges_follow_connections_then_client_edges() {
    let m = manifest(&["a", "b", "c"], &[("a", "b"), ("b", "c")]);
    let (d, _) = telemetry_for(&m);
    assert_eq!(d.schema_version, SCHEMA_VERSION);
    assert_eq!(d.nodes.len(), 3);
    assert_eq!(d.edges.len(), 4);
    assert_eq!((d.edges[0].from.as_str(), d.edges[0].to.as_str()), ("a", "b"));
    assert_eq!((d.edges[1].from.as_str(), d.edges[1].to.as_str()), ("b", "c"));
    assert_eq!(d.edges[2].kind, EdgeKind::Ingress);
    assert_eq!(d.edges[2].to, "a");
    assert_eq!(d.edges[3].kind, EdgeKind::Egress);
    assert_eq!(d.edges[3].from, "c");
    assert_eq!(d.ingress_edge("a"), Some(2));
    assert_eq!(d.egress_edge("c"), Some(3));
    assert_eq!(d.nodes[0].params["api_key"], redact::REDACTED);
    assert_eq!(d.nodes[0].params["gain"], 2);
    // Round-trips through JSON.
    let back: SessionDescriptor =
        serde_json::from_value(serde_json::to_value(&d).unwrap()).unwrap();
    assert_eq!(back, d);
}

#[test]
fn mark_inserted_and_pacing() {
    let m = manifest(&["a", "b"], &[("a", "b")]);
    let (mut d, _) = telemetry_for(&m);
    d.mark_inserted(&["b".to_string()].into_iter().collect());
    d.set_pacing(&[("a".to_string(), "reactive".to_string())].into_iter().collect());
    assert!(!d.nodes[0].inserted_by_negotiation);
    assert!(d.nodes[1].inserted_by_negotiation);
    assert_eq!(d.nodes[0].pacing.as_deref(), Some("reactive"));
    assert_eq!(d.nodes[1].pacing, None);
}

#[test]
fn snapshot_reflects_counters_and_status() {
    let m = manifest(&["a", "b"], &[("a", "b")]);
    let (_, t) = telemetry_for(&m);
    let a = t.node_index("a").unwrap();
    let b = t.node_index("b").unwrap();
    t.set_alive(a, true);
    t.set_alive(b, true);
    t.node(a).inputs.fetch_add(10, Relaxed);
    t.node(a).outputs.fetch_add(10, Relaxed);
    t.edge(0).unwrap().record_frame(100);
    t.edge(0).unwrap().record_stall(50);
    t.node(b).in_flight_since_us.store(t.now_us() + 1, Relaxed);
    t.set_cached_state("b", NodeState::Enabled);

    let mut st = publisher::PublisherState::default();
    let snap = publisher::build_snapshot(&t, &mut st, &HashMap::new(), false, false);
    assert_eq!(snap.kind, snapshot::SNAPSHOT_KIND);
    assert_eq!(snap.seq, 1);
    let na = &snap.nodes[a];
    assert_eq!(na.inputs, 10);
    // a's outgoing edge stalled this window → backpressured.
    assert_eq!(na.status, NodeStatus::Backpressured);
    assert_eq!(snap.nodes[b].status, NodeStatus::Busy);
    assert_eq!(snap.edges[0].frames, 1);
    assert_eq!(snap.edges[0].bytes, 100);
    assert_eq!(snap.edges[0].stalls, 1);

    // No new stalls → no longer backpressured; seq increases.
    let snap2 = publisher::build_snapshot(&t, &mut st, &HashMap::new(), false, true);
    assert_eq!(snap2.seq, 2);
    assert!(snap2.is_final);
    assert_eq!(snap2.nodes[a].status, NodeStatus::Idle);

    let json = serde_json::to_value(&snap2).unwrap();
    assert_eq!(json["final"], true);
    assert_eq!(json["level"], "auto");
    assert_eq!(json["nodes"][0]["status"], "idle");
}

#[test]
fn node_state_cache_is_mirrored() {
    let m = manifest(&["a"], &[]);
    let (_, t) = telemetry_for(&m);
    let a = t.node_index("a").unwrap();
    t.set_cached_state("a", NodeState::Bypass);
    assert_eq!(t.node(a).node_state(), NodeState::Bypass);
    t.set_cached_state("missing", NodeState::Disabled);
    t.set_cached_state("a", NodeState::Enabled);
    assert_eq!(t.node(a).node_state(), NodeState::Enabled);
}

#[test]
fn error_events_are_coalesced_per_second() {
    let m = manifest(&["a"], &[]);
    let (_, t) = telemetry_for(&m);
    let a = t.node_index("a").unwrap();
    let first = t.record_error(a, "boom");
    assert!(first.is_some());
    assert_eq!(first.unwrap().count, 1);
    let mut published = 0;
    for _ in 0..49 {
        if t.record_error(a, "boom").is_some() {
            published += 1;
        }
    }
    assert_eq!(published, 0, "within one second only the first publishes");
    assert_eq!(t.node(a).errors.load(Relaxed), 50);
    let last = t.last_error(a).unwrap();
    assert_eq!(last.message, "boom");
    assert_eq!(last.repeat, 50);
    // Not yet due.
    assert!(t.take_due_error_events().is_empty());
}

#[test]
fn long_error_messages_are_truncated() {
    let m = manifest(&["a"], &[]);
    let (_, t) = telemetry_for(&m);
    let a = t.node_index("a").unwrap();
    let long = "é".repeat(1000);
    t.record_error(a, &long);
    let info = t.last_error(a).unwrap();
    assert!(info.message.len() <= MAX_ERROR_MESSAGE_LEN + "…".len());
}

#[test]
fn level_parse_and_interval_clamp() {
    assert_eq!(TelemetryLevel::parse("FULL"), Some(TelemetryLevel::Full));
    assert_eq!(TelemetryLevel::parse("counters"), Some(TelemetryLevel::Counters));
    assert_eq!(TelemetryLevel::parse("bogus"), None);
    let m = manifest(&["a"], &[]);
    let (_, t) = telemetry_for(&m);
    t.set_interval_ms(10);
    assert_eq!(t.interval_ms(), MIN_INTERVAL_MS);
    t.set_interval_ms(60_000);
    assert_eq!(t.interval_ms(), MAX_INTERVAL_MS);
}

#[tokio::test]
async fn queue_probe_is_weak() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(4);
    let probe: Box<dyn QueueProbe> = Box::new(tx.downgrade());
    tx.send(1).await.unwrap();
    tx.send(2).await.unwrap();
    assert_eq!(probe.depth(), Some(2));
    drop(tx);
    // The probe must not keep the channel open.
    assert_eq!(rx.recv().await, Some(1));
    assert_eq!(rx.recv().await, Some(2));
    assert_eq!(rx.recv().await, None);
    assert_eq!(probe.depth(), None);
}

/// Hot-path budget check (openspec `pipeline-telemetry`: ≤30 ns per hop).
/// Times exactly the telemetry work the router adds per hop: node in/out
/// counters, in-flight stamps, busy time, cached state read, one payload
/// size estimate and one edge frame record.
///
/// ```bash
/// cargo test --release --lib telemetry::tests::hot_path_budget -- --ignored --nocapture
/// ```
#[test]
#[ignore = "timing benchmark; run in --release"]
fn hot_path_budget() {
    use crate::data::{AudioSamples, RuntimeData};
    use std::hint::black_box;
    let m = manifest(&["a", "b"], &[("a", "b")]);
    let (_, t) = telemetry_for(&m);
    let a = t.node_index("a").unwrap();
    let cell = t.node(a);
    let edge = t.edge(0).unwrap();
    let frame = RuntimeData::Audio {
        samples: AudioSamples::Vec(vec![0.0; 960]),
        sample_rate: 48_000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    };
    const N: u64 = 5_000_000;
    // Warm up.
    for _ in 0..10_000 {
        black_box(approx_payload_bytes(black_box(&frame)));
    }
    let start = std::time::Instant::now();
    for i in 0..N {
        black_box(cell.node_state());
        cell.inputs.fetch_add(1, Relaxed);
        cell.in_flight_since_us.store(i + 1, Relaxed);
        cell.busy_us.fetch_add(3, Relaxed);
        cell.in_flight_since_us.store(0, Relaxed);
        cell.outputs.fetch_add(1, Relaxed);
        let bytes = approx_payload_bytes(black_box(&frame));
        edge.record_frame(bytes);
    }
    let per_hop_ns = start.elapsed().as_nanos() as f64 / N as f64;
    println!("telemetry per-hop cost: {per_hop_ns:.2} ns");
    assert_eq!(cell.outputs.load(Relaxed), N);
    assert!(
        per_hop_ns <= 30.0,
        "telemetry per-hop cost {per_hop_ns:.2} ns exceeds the 30 ns budget"
    );
}

struct NamedFactory(&'static str);

impl crate::nodes::StreamingNodeFactory for NamedFactory {
    fn create(
        &self,
        _node_id: String,
        _params: &serde_json::Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn crate::nodes::StreamingNode>, crate::error::Error> {
        Err(crate::error::Error::Execution("test factory".into()))
    }

    fn node_type(&self) -> &str {
        self.0
    }
}

#[test]
fn descriptor_reports_build_identity_and_params_hash() {
    let mut a = manifest(&["n"], &[]);
    a.nodes[0].node_type = "Plugged".into();
    a.nodes[0].params =
        serde_json::from_str(r#"{"gain":2,"opts":{"x":1,"y":2},"api_key":"k1"}"#).unwrap();
    let mut b = a.clone();
    b.nodes[0].params =
        serde_json::from_str(r#"{"opts":{"y":2,"x":1},"api_key":"k2","gain":2}"#).unwrap();

    let mut registry = StreamingNodeRegistry::new();
    let plugin = NodeImplementation {
        kind: ImplementationKind::Loadable,
        plugin_name: Some("demo".into()),
        plugin_version: Some("0.1.0".into()),
        digest: Some("sha256:abc".into()),
        loaded_at_ms: Some(1),
    };
    registry.register_with_implementation(Arc::new(NamedFactory("Plugged")), plugin.clone());

    let describe = |m: &Manifest, registry: &StreamingNodeRegistry| {
        let graph = PipelineGraph::from_manifest(m).unwrap();
        SessionDescriptor::build("s", m, &graph, registry, QueueCapacity::default())
    };
    let (da, db) = (describe(&a, &registry), describe(&b, &registry));
    assert_eq!(da.nodes[0].implementation, plugin);
    assert_eq!(da.nodes[0].params_hash, db.nodes[0].params_hash);
    assert!(da.nodes[0].params_hash.starts_with("sha256:"));

    // Re-registering without identity reverts to builtin.
    registry.register(Arc::new(NamedFactory("Plugged")));
    let dc = describe(&a, &registry);
    assert_eq!(dc.nodes[0].implementation.kind, ImplementationKind::Builtin);
    let json = serde_json::to_value(&dc.nodes[0]).unwrap();
    assert_eq!(
        json["implementation"],
        serde_json::json!({"kind":"builtin"})
    );
    assert!(json["params_hash"].is_string());
}
