//! gRPC clients sharing one pipeline session (openspec:
//! add-multi-participant-sessions, multi-participant-ingress):
//! `StreamInit.shared_session_key` + `participant` join one core session,
//! every output carries the producing participant in `DataBuffer.metadata`,
//! and a control attach that declares a participant in `Hello` has it
//! stamped on the frames it publishes.
//!
//! ```bash
//! cargo test -p remotemedia-grpc --test shared_stream_e2e
//! ```

use remotemedia_core::data::RuntimeData;
use remotemedia_core::transport::PipelineExecutor;
use remotemedia_grpc::control::ControlServiceImpl;
use remotemedia_grpc::generated::{
    control_event::Event as PbEvent, control_frame::Op as PbOp, data_buffer::DataType,
    pipeline_control_client::PipelineControlClient, pipeline_control_server::PipelineControlServer,
    stream_request::Request as StreamRequestType, stream_response::Response as StreamResponseType,
    streaming_pipeline_service_client::StreamingPipelineServiceClient,
    streaming_pipeline_service_server::StreamingPipelineServiceServer, ControlAddress, ControlDirection,
    ControlFrame, DataChunk, Hello, ManifestMetadata, NodeManifest, ParticipantInfo, PipelineManifest,
    Publish, StreamControl, StreamInit, StreamRequest,
};
use remotemedia_grpc::metrics::ServiceMetrics;
use remotemedia_grpc::{ServiceConfig, StreamingServiceImpl};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::Server;

fn manifest() -> PipelineManifest {
    PipelineManifest {
        version: "v1".to_string(),
        metadata: Some(ManifestMetadata { name: "grpc-shared".to_string(), ..Default::default() }),
        nodes: vec![NodeManifest {
            id: "echo".to_string(),
            node_type: "PassThrough".to_string(),
            params: "{}".to_string(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

async fn start() -> (String, Arc<PipelineExecutor>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let executor = Arc::new(PipelineExecutor::new().unwrap());
    let config = ServiceConfig::default();
    let streaming = StreamingServiceImpl::new(
        config.auth,
        config.limits,
        Arc::new(ServiceMetrics::with_default_registry().unwrap()),
        executor.clone(),
    );
    let control = ControlServiceImpl::new(executor.control_bus());
    tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(StreamingPipelineServiceServer::new(streaming))
            .add_service(PipelineControlServer::new(control))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (url, executor)
}

struct Client {
    tx: mpsc::Sender<StreamRequest>,
    responses: tonic::Streaming<remotemedia_grpc::generated::StreamResponse>,
    session_id: String,
}

async fn join(url: &str, key: &str, who: &str) -> Client {
    let mut client = StreamingPipelineServiceClient::connect(url.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel::<StreamRequest>(16);
    tx.send(StreamRequest {
        request: Some(StreamRequestType::Init(StreamInit {
            manifest: Some(manifest()),
            client_version: "v1".to_string(),
            shared_session_key: key.to_string(),
            participant: Some(ParticipantInfo {
                id: who.to_string(),
                role: "user".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut responses = client.stream_pipeline(ReceiverStream::new(rx)).await.unwrap().into_inner();
    let session_id = loop {
        let response = tokio::time::timeout(Duration::from_secs(10), responses.message())
            .await
            .expect("StreamReady in time")
            .unwrap()
            .expect("stream open");
        match response.response {
            Some(StreamResponseType::Ready(ready)) => break ready.session_id,
            Some(StreamResponseType::Error(error)) => panic!("{}", error.message),
            _ => {}
        }
    };
    Client { tx, responses, session_id }
}

impl Client {
    async fn say(&self, text: &str, seq: u64) {
        self.tx
            .send(StreamRequest {
                request: Some(StreamRequestType::DataChunk(DataChunk {
                    node_id: "echo".to_string(),
                    buffer: Some(remotemedia_grpc::adapters::runtime_data_to_data_buffer(
                        &RuntimeData::Text(text.to_string()),
                    )),
                    sequence: seq,
                    ..Default::default()
                })),
            })
            .await
            .unwrap();
    }

    /// Send 20 ms of 16 kHz audio whose `DataBuffer.metadata` names the
    /// microphone track.
    async fn speak(&self, track: &str, seq: u64) {
        let mut buffer = remotemedia_grpc::adapters::runtime_data_to_data_buffer(&RuntimeData::Audio {
            samples: vec![0.1f32; 320].into(),
            sample_rate: 16000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        });
        buffer.metadata.insert("participant.track_id".to_string(), track.to_string());
        self.tx
            .send(StreamRequest {
                request: Some(StreamRequestType::DataChunk(DataChunk {
                    node_id: "echo".to_string(),
                    buffer: Some(buffer),
                    sequence: seq,
                    ..Default::default()
                })),
            })
            .await
            .unwrap();
    }

    async fn next_buffer(&mut self) -> remotemedia_grpc::generated::DataBuffer {
        loop {
            let response = tokio::time::timeout(Duration::from_secs(10), self.responses.message())
                .await
                .expect("output in time")
                .unwrap()
                .expect("stream open");
            let Some(StreamResponseType::Result(mut result)) = response.response else { continue };
            return result.data_outputs.remove("output").expect("output entry");
        }
    }

    /// Next output as `(participant.id, text)`.
    async fn next(&mut self) -> (String, String) {
        let buffer = self.next_buffer().await;
        let Some(DataType::Text(text)) = &buffer.data_type else { panic!("{buffer:?}") };
        let who = buffer.metadata.get("participant.id").cloned().unwrap_or_default();
        (who, String::from_utf8(text.text_data.clone()).unwrap())
    }

    async fn close(self) {
        let _ = self
            .tx
            .send(StreamRequest {
                request: Some(StreamRequestType::Control(StreamControl { command: 1, ..Default::default() })),
            })
            .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_clients_on_one_key_share_a_session_with_attributed_outputs() {
    let (url, executor) = start().await;
    let mut alice = join(&url, "standup", "alice").await;
    let mut bob = join(&url, "standup", "bob").await;
    assert_eq!(alice.session_id, bob.session_id, "one session for the key");
    let other = join(&url, "other-room", "carol").await;
    assert_ne!(other.session_id, alice.session_id, "another key, another session");
    other.close().await;

    alice.say("hi from alice", 1).await;
    assert_eq!(alice.next().await, ("alice".into(), "hi from alice".into()));
    assert_eq!(bob.next().await, ("alice".into(), "hi from alice".into()));
    bob.say("hello from bob", 1).await;
    assert_eq!(alice.next().await, ("bob".into(), "hello from bob".into()));
    assert_eq!(bob.next().await, ("bob".into(), "hello from bob".into()));

    // Audio: attributed too, and the buffer's own participant metadata
    // (here the track) is honoured.
    alice.speak("alice-mic", 3).await;
    for client in [&mut alice, &mut bob] {
        let buffer = client.next_buffer().await;
        assert!(matches!(buffer.data_type, Some(DataType::Audio(_))), "{buffer:?}");
        assert_eq!(buffer.metadata.get("participant.id").map(String::as_str), Some("alice"));
        assert_eq!(buffer.metadata.get("participant.track_id").map(String::as_str), Some("alice-mic"));
    }

    // A control attach that declares a participant publishes as it.
    let mut control = PipelineControlClient::connect(url.clone()).await.unwrap();
    let (ctx, crx) = mpsc::channel::<ControlFrame>(8);
    ctx.send(ControlFrame {
        op: Some(PbOp::Hello(Hello {
            session_id: alice.session_id.clone(),
            attach_id: "moderator".to_string(),
            participant: Some(ParticipantInfo { id: "moderator".to_string(), role: "agent".to_string(), ..Default::default() }),
        })),
    })
    .await
    .unwrap();
    let mut events = control.attach(ReceiverStream::new(crx)).await.unwrap().into_inner();
    let first = tokio::time::timeout(Duration::from_secs(5), events.next()).await.unwrap().unwrap().unwrap();
    assert!(matches!(first.event, Some(PbEvent::Attached(_))), "{first:?}");
    ctx.send(ControlFrame {
        op: Some(PbOp::Publish(Publish {
            addr: Some(ControlAddress {
                node_id: "echo".to_string(),
                port: String::new(),
                direction: ControlDirection::In as i32,
            }),
            data: Some(remotemedia_grpc::adapters::runtime_data_to_data_buffer(&RuntimeData::Text(
                "time is up".to_string(),
            ))),
        })),
    })
    .await
    .unwrap();
    assert_eq!(alice.next().await, ("moderator".into(), "time is up".into()));
    assert_eq!(bob.next().await, ("moderator".into(), "time is up".into()));

    // Alice leaves; Bob keeps the session. The last one out closes it.
    alice.close().await;
    bob.say("still here", 2).await;
    assert_eq!(bob.next().await, ("bob".into(), "still here".into()));
    let session_id = bob.session_id.clone();
    bob.close().await;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while executor.control_bus().get(&session_id).is_some() {
        assert!(std::time::Instant::now() < deadline, "shared session not closed after the last client left");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
