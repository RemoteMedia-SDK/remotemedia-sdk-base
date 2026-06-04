//! End-to-end test for the gRPC Session Control Bus.
//!
//! Starts a real gRPC server hosting `PipelineControl`, creates a session
//! via `PipelineExecutor::create_session` (which registers a `SessionControl`
//! in the bus), connects a client, and exercises the four operations:
//!
//!   - Subscribe  (tap a node's output)
//!   - Publish    (inject at a node's input)
//!   - Intercept  (replace a node's output)
//!   - SetNodeState (Bypass / Disabled)
//!
//! Plus:
//!   - Attach -> Attached roundtrip
//!   - SessionNotFound for a bogus session_id
//!   - SessionClosed event when the router shuts down

use remotemedia_core::data::RuntimeData;
use remotemedia_core::manifest::{Connection, Manifest, ManifestMetadata, NodeManifest};
use remotemedia_core::transport::{PipelineExecutor, TransportData};
use remotemedia_grpc::control::ControlServiceImpl;
use remotemedia_grpc::generated::{
    control_event::Event as PbEvent, control_frame::Op as PbOp,
    intercept_decision::Decision as PbDecision, pipeline_control_client::PipelineControlClient,
    pipeline_control_server::PipelineControlServer, Attached, ClearNodeState,
    ControlAddress as PbAddress, ControlDirection, ControlErrorCode, ControlEvent, ControlFrame,
    Empty, Hello, Intercept, InterceptDecision as PbInterceptDecision, InterceptReply,
    NodeState as PbNodeState, Publish as PbPublish, SetNodeState as PbSetNodeState,
    Subscribe as PbSubscribe, TapEvent,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Server;

// ── Test server bringup ─────────────────────────────────────────────────────

async fn start_server() -> (String, Arc<PipelineExecutor>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let url = format!("http://{}", addr);

    let executor = Arc::new(PipelineExecutor::new().unwrap());
    let control = ControlServiceImpl::new(executor.control_bus());

    let srv = Server::builder()
        .add_service(PipelineControlServer::new(control))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener));

    let handle = tokio::spawn(async move {
        let _ = srv.await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (url, executor, handle)
}

fn calc_manifest() -> Manifest {
    Manifest {
        version: "v1".to_string(),
        metadata: ManifestMetadata {
            name: "ctrl-grpc-e2e".to_string(),
            ..Default::default()
        },
        nodes: vec![NodeManifest {
            id: "calc".to_string(),
            node_type: "CalculatorNode".to_string(),
            params: serde_json::json!({}),
            ..Default::default()
        }],
        connections: Vec::<Connection>::new(),
        ..Default::default()
    }
}

async fn connect_control(url: &str) -> PipelineControlClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(url.to_string())
        .unwrap()
        .connect()
        .await
        .unwrap();
    PipelineControlClient::new(channel)
}

// Open an Attach RPC. Returns (outbound-frame sender, inbound-event stream),
// and waits for the `Attached` event so subsequent ops see a live attach.
async fn open_attach(
    client: &mut PipelineControlClient<tonic::transport::Channel>,
    session_id: &str,
) -> (mpsc::Sender<ControlFrame>, tonic::Streaming<ControlEvent>) {
    let (tx, rx) = mpsc::channel::<ControlFrame>(32);
    let req_stream = ReceiverStream::new(rx);

    tx.send(ControlFrame {
        op: Some(PbOp::Hello(Hello {
            session_id: session_id.to_string(),
            attach_id: "test-attach".to_string(),
        })),
    })
    .await
    .unwrap();

    let response = client.attach(req_stream).await.unwrap();
    let mut events = response.into_inner();

    // First event MUST be Attached.
    let first = tokio::time::timeout(Duration::from_secs(2), events.next())
        .await
        .expect("attached event timeout")
        .expect("stream ended before attached")
        .unwrap();
    match first.event {
        Some(PbEvent::Attached(Attached {
            session_id: sid, ..
        })) => {
            assert_eq!(sid, session_id);
        }
        other => panic!("expected Attached, got {:?}", other),
    }

    (tx, events)
}

fn addr(node_id: &str, direction: ControlDirection) -> PbAddress {
    PbAddress {
        node_id: node_id.to_string(),
        port: String::new(),
        direction: direction as i32,
    }
}

// Send a calc input via `publish` on the same control attach used for the test.
async fn publish_calc(tx: &mpsc::Sender<ControlFrame>, a: f64, b: f64) {
    let json = serde_json::json!({
        "operation": "add",
        "operands": [a, b],
    });
    let data = remotemedia_grpc::adapters::runtime_data_to_data_buffer(&RuntimeData::Json(json));
    tx.send(ControlFrame {
        op: Some(PbOp::Publish(PbPublish {
            addr: Some(addr("calc", ControlDirection::In)),
            data: Some(data),
        })),
    })
    .await
    .unwrap();
}

async fn next_event(
    events: &mut tonic::Streaming<ControlEvent>,
    timeout: Duration,
) -> Option<ControlEvent> {
    tokio::time::timeout(timeout, events.next())
        .await
        .ok()
        .and_then(|o| o.and_then(|r| r.ok()))
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn attach_reports_session_not_found_for_bogus_id() {
    let (url, _exec, _h) = start_server().await;
    let mut client = connect_control(&url).await;

    let (tx, _rx) = mpsc::channel::<ControlFrame>(2);
    tx.send(ControlFrame {
        op: Some(PbOp::Hello(Hello {
            session_id: "no-such-session".to_string(),
            attach_id: "x".to_string(),
        })),
    })
    .await
    .unwrap();

    let result = client.attach(ReceiverStream::new(_rx)).await;
    match result {
        Ok(mut resp) => {
            // Either the RPC errored immediately (preferred), or the first
            // frame on the stream is a terminal error.
            let first = resp.get_mut().next().await;
            assert!(first.is_none() || first.unwrap().is_err());
        }
        Err(status) => {
            assert_eq!(status.code(), tonic::Code::NotFound);
        }
    }
}

#[tokio::test]
async fn tap_receives_node_output_via_grpc() {
    let (url, executor, _h) = start_server().await;
    let manifest = Arc::new(calc_manifest());
    let mut session = executor.create_session(manifest).await.unwrap();

    let mut client = connect_control(&url).await;
    let (tx, mut events) = open_attach(&mut client, &session.session_id).await;

    // Subscribe to the calc output.
    tx.send(ControlFrame {
        op: Some(PbOp::Subscribe(PbSubscribe {
            addr: Some(addr("calc", ControlDirection::Out)),
        })),
    })
    .await
    .unwrap();

    // Give the server a beat to install the tap forwarder.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drive the pipeline with one input via the session handle.
    session
        .send_input(TransportData::new(RuntimeData::Json(serde_json::json!({
            "operation": "add",
            "operands": [3.0, 4.0],
        }))))
        .await
        .unwrap();

    // Expect a Tap event carrying the calc result.
    let event = next_event(&mut events, Duration::from_secs(3))
        .await
        .expect("tap event timeout");
    match event.event {
        Some(PbEvent::Tap(TapEvent {
            data: Some(buf), ..
        })) => {
            let rt = remotemedia_grpc::adapters::data_buffer_to_runtime_data(&buf).unwrap();
            match rt {
                RuntimeData::Json(v) => {
                    assert_eq!(v["result"].as_f64().unwrap(), 7.0);
                }
                other => panic!("expected Json result, got {:?}", other),
            }
        }
        other => panic!("expected Tap event, got {:?}", other),
    }

    session.close().await.unwrap();
}

#[tokio::test]
async fn publish_injects_into_running_pipeline_via_grpc() {
    let (url, executor, _h) = start_server().await;
    let manifest = Arc::new(calc_manifest());
    let mut session = executor.create_session(manifest).await.unwrap();

    let mut client = connect_control(&url).await;
    let (tx, _events) = open_attach(&mut client, &session.session_id).await;

    // Inject a multiply via the control bus (not via session.send_input).
    publish_calc(&tx, 6.0, 7.0).await;
    // Overwrite with the real op we want — add with specific operands.
    tx.send(ControlFrame {
        op: Some(PbOp::Publish(PbPublish {
            addr: Some(addr("calc", ControlDirection::In)),
            data: Some(remotemedia_grpc::adapters::runtime_data_to_data_buffer(
                &RuntimeData::Json(serde_json::json!({
                    "operation": "multiply",
                    "operands": [6.0, 7.0],
                })),
            )),
        })),
    })
    .await
    .unwrap();

    // Expect the sink output on the session handle.
    let out = tokio::time::timeout(Duration::from_secs(3), session.recv_output())
        .await
        .expect("sink recv timeout")
        .unwrap();
    let result = out.unwrap().data;
    match result {
        RuntimeData::Json(v) => {
            // First injection was add[6,7]=13; the second is multiply[6,7]=42.
            let r = v["result"].as_f64().unwrap();
            assert!(r == 13.0 || r == 42.0, "unexpected result {r}");
        }
        other => panic!("expected Json, got {:?}", other),
    }

    session.close().await.unwrap();
}

#[tokio::test]
async fn intercept_replaces_downstream_output_via_grpc() {
    let (url, executor, _h) = start_server().await;
    let manifest = Arc::new(calc_manifest());
    let mut session = executor.create_session(manifest).await.unwrap();

    let mut client = connect_control(&url).await;
    let (tx, mut events) = open_attach(&mut client, &session.session_id).await;

    tx.send(ControlFrame {
        op: Some(PbOp::Intercept(Intercept {
            addr: Some(addr("calc", ControlDirection::Out)),
            deadline_ms: 500,
        })),
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Spawn a task that replies to InterceptRequests with a Replace.
    let replace_json = serde_json::json!({ "result": 999.0, "operation": "replaced" });
    let tx_reply = tx.clone();
    let replace_data = replace_json.clone();
    let handler = tokio::spawn(async move {
        while let Some(ev) = events.next().await {
            let ev = match ev {
                Ok(e) => e,
                Err(_) => break,
            };
            if let Some(PbEvent::InterceptRequest(req)) = ev.event {
                let _ = tx_reply
                    .send(ControlFrame {
                        op: Some(PbOp::InterceptReply(InterceptReply {
                            correlation_id: req.correlation_id,
                            decision: Some(PbInterceptDecision {
                                decision: Some(PbDecision::Replace(
                                    remotemedia_grpc::adapters::runtime_data_to_data_buffer(
                                        &RuntimeData::Json(replace_data.clone()),
                                    ),
                                )),
                            }),
                        })),
                    })
                    .await;
            }
        }
    });

    session
        .send_input(TransportData::new(RuntimeData::Json(serde_json::json!({
            "operation": "add",
            "operands": [1.0, 1.0],
        }))))
        .await
        .unwrap();

    let out = tokio::time::timeout(Duration::from_secs(3), session.recv_output())
        .await
        .expect("sink recv timeout")
        .unwrap();
    let result = out.unwrap().data;
    match result {
        RuntimeData::Json(v) => {
            assert_eq!(v["result"].as_f64().unwrap(), 999.0);
        }
        other => panic!("expected Json, got {:?}", other),
    }

    handler.abort();
    session.close().await.unwrap();
}

#[tokio::test]
async fn set_node_state_bypass_forwards_input_via_grpc() {
    let (url, executor, _h) = start_server().await;
    let manifest = Arc::new(calc_manifest());
    let mut session = executor.create_session(manifest).await.unwrap();

    let mut client = connect_control(&url).await;
    let (tx, _events) = open_attach(&mut client, &session.session_id).await;

    tx.send(ControlFrame {
        op: Some(PbOp::SetNodeState(PbSetNodeState {
            node_id: "calc".to_string(),
            state: PbNodeState::Bypass as i32,
        })),
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    session
        .send_input(TransportData::new(RuntimeData::Json(serde_json::json!({
            "operation": "add",
            "operands": [10.0, 32.0],
        }))))
        .await
        .unwrap();

    let out = tokio::time::timeout(Duration::from_secs(3), session.recv_output())
        .await
        .expect("sink recv timeout")
        .unwrap();
    match out.unwrap().data {
        RuntimeData::Json(v) => {
            // Bypass forwards the input unchanged — no `result` field.
            assert!(v.get("result").is_none(), "bypass should skip execution");
            assert_eq!(v["operation"], "add");
            assert_eq!(v["operands"][0], 10.0);
        }
        other => panic!("expected Json, got {:?}", other),
    }

    // Clear to re-enable; subsequent inputs should compute again.
    tx.send(ControlFrame {
        op: Some(PbOp::ClearNodeState(ClearNodeState {
            node_id: "calc".to_string(),
        })),
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    session
        .send_input(TransportData::new(RuntimeData::Json(serde_json::json!({
            "operation": "multiply",
            "operands": [3.0, 5.0],
        }))))
        .await
        .unwrap();

    let out = tokio::time::timeout(Duration::from_secs(3), session.recv_output())
        .await
        .expect("sink recv timeout after clear")
        .unwrap();
    match out.unwrap().data {
        RuntimeData::Json(v) => assert_eq!(v["result"].as_f64().unwrap(), 15.0),
        other => panic!("expected Json, got {:?}", other),
    }

    session.close().await.unwrap();
}

#[tokio::test]
async fn session_closed_event_fires_on_shutdown() {
    let (url, executor, _h) = start_server().await;
    let manifest = Arc::new(calc_manifest());
    let mut session = executor.create_session(manifest).await.unwrap();

    let mut client = connect_control(&url).await;
    let (_tx, mut events) = open_attach(&mut client, &session.session_id).await;

    session.close().await.unwrap();

    let mut saw_closed = false;
    while let Some(ev) = next_event(&mut events, Duration::from_secs(3)).await {
        if let Some(PbEvent::SessionClosed(_)) = ev.event {
            saw_closed = true;
            break;
        }
    }
    assert!(saw_closed, "expected SessionClosed event on shutdown");
}

// Silence unused-import warnings when a sub-test is commented out.
fn _use_types() {
    let _ = std::mem::size_of::<Empty>();
    let _ = ControlErrorCode::Unspecified;
}
