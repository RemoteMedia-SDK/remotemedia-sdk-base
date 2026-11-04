//! gRPC `StreamPipeline` sessions are observable (openspec:
//! add-live-pipeline-observer): discoverable with a descriptor, transport
//! `grpc`, live `__telemetry__` counters (node + edge, incl. ingress/egress),
//! and node-state control through the gRPC router.
//!
//! ```bash
//! cargo test -p remotemedia-grpc --features multiprocess --test stream_pipeline_telemetry_e2e
//! ```

use remotemedia_core::data::RuntimeData;
use remotemedia_core::transport::session_control::{
    ControlAddress, NodeState, SessionPhase, TELEMETRY_PORT,
};
use remotemedia_core::transport::PipelineExecutor;
use remotemedia_grpc::generated::{
    stream_request::Request as StreamRequestType, stream_response::Response as StreamResponseType,
    streaming_pipeline_service_client::StreamingPipelineServiceClient, Connection, DataChunk,
    ManifestMetadata, NodeManifest, PipelineManifest, StreamInit, StreamRequest,
};
use remotemedia_grpc::metrics::ServiceMetrics;
use remotemedia_grpc::{ServiceConfig, StreamingServiceImpl};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;

fn manifest() -> PipelineManifest {
    let node = |id: &str| NodeManifest {
        id: id.to_string(),
        node_type: "PassThrough".to_string(),
        params: "{}".to_string(),
        ..Default::default()
    };
    PipelineManifest {
        version: "v1".to_string(),
        metadata: Some(ManifestMetadata {
            name: "grpc-telemetry".to_string(),
            ..Default::default()
        }),
        nodes: vec![node("a"), node("b")],
        connections: vec![Connection {
            from: "a".to_string(),
            to: "b".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn text_chunk(seq: u64) -> StreamRequest {
    StreamRequest {
        request: Some(StreamRequestType::DataChunk(DataChunk {
            node_id: "a".to_string(),
            buffer: Some(remotemedia_grpc::adapters::runtime_data_to_data_buffer(
                &RuntimeData::Text(format!("msg {seq}")),
            )),
            sequence: seq,
            ..Default::default()
        })),
    }
}

async fn next_snapshot(tap: &mut tokio::sync::broadcast::Receiver<RuntimeData>) -> Value {
    match tokio::time::timeout(Duration::from_secs(5), tap.recv()).await {
        Ok(Ok(RuntimeData::Json(v))) => v,
        other => panic!("no snapshot: {other:?}"),
    }
}

fn node<'a>(snap: &'a Value, id: &str) -> &'a Value {
    snap["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["id"] == id)
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_pipeline_sessions_report_live_telemetry() {
    if std::env::var_os("TEL_TEST_TRACE").is_some() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new("info"))
            .try_init();
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let executor = Arc::new(PipelineExecutor::new().unwrap());
    let config = ServiceConfig::default();
    let service = StreamingServiceImpl::new(
        config.auth,
        config.limits,
        Arc::new(ServiceMetrics::with_default_registry().unwrap()),
        executor.clone(),
    );
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(
                remotemedia_grpc::generated::streaming_pipeline_service_server::StreamingPipelineServiceServer::new(service),
            )
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client = StreamingPipelineServiceClient::connect(url).await.unwrap();
    let (tx, rx) = mpsc::channel::<StreamRequest>(64);
    tx.send(StreamRequest {
        request: Some(StreamRequestType::Init(StreamInit {
            manifest: Some(manifest()),
            client_version: "v1".to_string(),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut responses = client
        .stream_pipeline(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();

    // StreamReady carries the session id.
    let sid = loop {
        let r = tokio::time::timeout(Duration::from_secs(10), responses.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Some(StreamResponseType::Ready(ready)) = r.response {
            break ready.session_id;
        }
    };

    // Discoverable with descriptor, transport and phase.
    let bus = executor.control_bus();
    let summary = bus
        .list()
        .into_iter()
        .find(|s| s.session_id == sid)
        .expect("gRPC session registered");
    assert_eq!(summary.transport.as_deref(), Some("grpc"));
    assert_eq!(summary.phase, SessionPhase::Active);
    assert!(summary.has_telemetry, "gRPC sessions carry live telemetry");
    let ctrl = bus.get(&sid).unwrap();
    let desc = ctrl.descriptor().unwrap();
    assert_eq!(desc.nodes.len(), 2);
    assert_eq!(desc.edges.len(), 3); // a→b, ingress→a, b→egress
    assert_eq!(desc.nodes[0].pacing.as_deref(), Some("reactive"));

    let mut tap = ctrl
        .subscribe(&ControlAddress::node_out(TELEMETRY_PORT))
        .unwrap();
    ctrl.telemetry().unwrap().set_interval_ms(250);

    // Drain client responses in the background.
    tokio::spawn(async move {
        loop {
            match responses.message().await {
                Ok(Some(r)) => {
                    if std::env::var_os("TEL_TEST_TRACE").is_some() {
                        eprintln!("response: {:?}", r.response.as_ref().map(|x| format!("{x:?}").chars().take(200).collect::<String>()));
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    eprintln!("response stream error: {e}");
                    break;
                }
            }
        }
    });

    for i in 0..10 {
        tx.send(text_chunk(i)).await.unwrap();
    }
    let mut snap = Value::Null;
    for _ in 0..40 {
        snap = next_snapshot(&mut tap).await;
        if node(&snap, "b")["outputs"] == 10 {
            break;
        }
    }
    assert_eq!(node(&snap, "a")["inputs"], 10);
    assert_eq!(node(&snap, "b")["outputs"], 10);
    let edge = |from: &str, to: &str| {
        desc.edges
            .iter()
            .position(|e| e.from == from && e.to == to)
            .unwrap()
    };
    assert_eq!(snap["edges"][edge("a", "b")]["frames"], 10);
    assert_eq!(snap["edges"][desc.ingress_edge("a").unwrap()]["frames"], 10);
    assert_eq!(snap["edges"][desc.egress_edge("b").unwrap()]["frames"], 10);
    assert!(snap["histograms"].as_bool().unwrap());

    // Node state now applies on the gRPC router too.
    ctrl.set_node_state("b", NodeState::Bypass);
    for i in 10..13 {
        tx.send(text_chunk(i)).await.unwrap();
    }
    for _ in 0..40 {
        snap = next_snapshot(&mut tap).await;
        if node(&snap, "b")["bypassed"] == 3 {
            break;
        }
    }
    assert_eq!(node(&snap, "b")["bypassed"], 3);
    assert_eq!(node(&snap, "b")["status"], "bypassed");
    assert_eq!(snap["edges"][desc.egress_edge("b").unwrap()]["frames"], 13);

    drop(tx);
}
