use remotemedia_core::{data::RuntimeData, transport::PipelineExecutor};
use remotemedia_grpc::{
    data_buffer_to_runtime_data,
    generated::{
        stream_control::Command, stream_request::Request as RequestKind,
        stream_response::Response as ResponseKind,
        streaming_pipeline_service_client::StreamingPipelineServiceClient, DataChunk,
        PipelineManifest, StreamControl, StreamInit, StreamRequest,
    },
    runtime_data_to_data_buffer, GrpcServer, ServiceConfig,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

fn free_address() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address.to_string()
}

fn manifest(name: &str, node_id: &str) -> PipelineManifest {
    PipelineManifest {
        manifest_json: serde_json::to_vec(&serde_json::json!({
            "version": "v1",
            "metadata": {"name": name},
            "nodes": [{
                "id": node_id,
                "node_type": "PassThrough",
                "params": {},
                "is_streaming": true,
                "is_output_node": true
            }],
            "connections": []
        }))
        .unwrap(),
        ..Default::default()
    }
}

async fn run_manifest(endpoint: &str, name: &str, node_id: &str, text: &str) -> String {
    let mut client = StreamingPipelineServiceClient::connect(endpoint.to_string())
        .await
        .unwrap();
    let (requests, receiver) = mpsc::channel(8);
    requests
        .send(StreamRequest {
            request: Some(RequestKind::Init(StreamInit {
                manifest: Some(manifest(name, node_id)),
                data_inputs: HashMap::new(),
                resource_limits: None,
                client_version: "v1".to_string(),
                expected_chunk_size: 1,
                output_taps: Vec::new(),
            })),
        })
        .await
        .unwrap();
    let mut responses = client
        .stream_pipeline(ReceiverStream::new(receiver))
        .await
        .unwrap()
        .into_inner();
    loop {
        match responses.message().await.unwrap().unwrap().response {
            Some(ResponseKind::Ready(_)) => break,
            Some(ResponseKind::Error(error)) => panic!("init failed: {}", error.message),
            _ => {}
        }
    }

    requests
        .send(StreamRequest {
            request: Some(RequestKind::DataChunk(DataChunk {
                node_id: node_id.to_string(),
                buffer: Some(runtime_data_to_data_buffer(&RuntimeData::Text(
                    text.to_string(),
                ))),
                named_buffers: HashMap::new(),
                sequence: 0,
                timestamp_ms: 0,
            })),
        })
        .await
        .unwrap();

    let output = loop {
        match responses.message().await.unwrap().unwrap().response {
            Some(ResponseKind::Result(result)) => {
                let value = result
                    .data_outputs
                    .values()
                    .find_map(data_buffer_to_runtime_data)
                    .unwrap();
                let RuntimeData::Text(value) = value else {
                    panic!("unexpected output type")
                };
                break value;
            }
            Some(ResponseKind::Error(error)) => panic!("stream failed: {}", error.message),
            _ => {}
        }
    };

    requests
        .send(StreamRequest {
            request: Some(RequestKind::Control(StreamControl {
                command: Command::Close as i32,
            })),
        })
        .await
        .unwrap();
    output
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn loads_two_complete_manifests_without_restart() {
    let address = free_address();
    let endpoint = format!("http://{address}");
    let mut config = ServiceConfig::default();
    config.bind_address = address;
    config.auth.require_auth = false;
    let executor = Arc::new(PipelineExecutor::new().unwrap());
    let server = GrpcServer::new(config, executor).unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));
    let server_shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        server
            .serve_with_shutdown_flag(server_shutdown)
            .await
            .unwrap()
    });

    let mut connected = false;
    for _ in 0..50 {
        if StreamingPipelineServiceClient::connect(endpoint.clone())
            .await
            .is_ok()
        {
            connected = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(connected, "server did not become ready");

    assert_eq!(
        run_manifest(&endpoint, "pipeline-a", "first", "alpha").await,
        "alpha"
    );
    assert_eq!(
        run_manifest(&endpoint, "pipeline-b", "second", "beta").await,
        "beta"
    );

    shutdown.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
}
