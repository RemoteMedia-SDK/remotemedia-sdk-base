//! Per-hop cost of live-pipeline telemetry (openspec:
//! add-live-pipeline-observer, `pipeline-telemetry` "Hot-path budget":
//! ≤30 ns per hop).
//!
//! `hop/baseline` does the payload work every hop already does (a clone of
//! the frame handle); `hop/telemetry` adds exactly what the router adds per
//! hop: node-state read, input/output counters, in-flight stamps, busy time,
//! one payload-size estimate and one edge frame record.
//!
//! ```bash
//! cargo bench --bench telemetry_hot_path
//! ```

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use remotemedia_sdk_base::data::{AudioSamples, RuntimeData};
use remotemedia_sdk_base::executor::{PipelineGraph, SchedulerConfig, StreamingScheduler};
use remotemedia_sdk_base::manifest::{Connection, Manifest, ManifestMetadata, NodeManifest};
use remotemedia_sdk_base::nodes::StreamingNodeRegistry;
use remotemedia_sdk_base::telemetry::{
    approx_payload_bytes, QueueCapacity, SessionDescriptor, SessionTelemetry,
};
use remotemedia_sdk_base::transport::perf_aggregator::PerfAggregator;
use std::sync::Arc;

fn telemetry() -> SessionTelemetry {
    let node = |id: &str| NodeManifest {
        id: id.into(),
        node_type: "PassThrough".into(),
        ..Default::default()
    };
    let m = Manifest {
        version: "v1".into(),
        metadata: ManifestMetadata {
            name: "bench".into(),
            ..Default::default()
        },
        nodes: vec![node("a"), node("b")],
        connections: vec![Connection {
            from: "a".into(),
            to: "b".into(),
            from_port: None,
            to_port: None,
        }],
        ..Default::default()
    };
    let g = PipelineGraph::from_manifest(&m).unwrap();
    let d = SessionDescriptor::build(
        "bench",
        &m,
        &g,
        &StreamingNodeRegistry::new(),
        QueueCapacity::default(),
    );
    SessionTelemetry::new(
        &d,
        Arc::new(StreamingScheduler::new(SchedulerConfig::default())),
        Arc::new(PerfAggregator::new("bench".into(), false, 1000)),
    )
}

fn audio_frame() -> RuntimeData {
    RuntimeData::Audio {
        samples: AudioSamples::Arc(vec![0.0f32; 960].into()),
        sample_rate: 48_000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    }
}

fn bench(c: &mut Criterion) {
    let tel = telemetry();
    let cell = tel.node(tel.node_index("a").unwrap());
    let edge = tel.edge(0).unwrap();
    let frame = audio_frame();
    let mut g = c.benchmark_group("hop");
    g.bench_function("baseline", |b| {
        b.iter(|| black_box(black_box(&frame).clone()))
    });
    g.bench_function("telemetry", |b| {
        let mut t = 0u64;
        b.iter(|| {
            let out = black_box(&frame).clone();
            black_box(cell.node_state());
            cell.count_input();
            cell.begin_dispatch(t);
            cell.end_dispatch(3);
            cell.count_output();
            edge.record_frame(approx_payload_bytes(&out));
            t += 1;
            black_box(out)
        })
    });
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
