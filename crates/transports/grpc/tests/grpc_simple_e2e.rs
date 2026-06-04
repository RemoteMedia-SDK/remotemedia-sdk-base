//! Simplified end-to-end gRPC integration test
//!
//! This test validates:
//! 1. Server starts successfully
//! 2. Client can connect
//! 3. Basic streaming session flow works

use remotemedia_core::data::PixelFormat;
use remotemedia_core::transport::PipelineExecutor;
use remotemedia_grpc::{metrics::ServiceMetrics, ServiceConfig, StreamingServiceImpl};
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tonic::transport::Server;

/// Start gRPC server in background
async fn start_test_server() -> (String, tokio::task::JoinHandle<()>) {
    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();
    let server_url = format!("http://{}", local_addr);

    // Create PipelineExecutor
    let runner = Arc::new(PipelineExecutor::new().unwrap());

    // Create service
    let config = ServiceConfig::default();
    let metrics = Arc::new(ServiceMetrics::with_default_registry().unwrap());
    let service = StreamingServiceImpl::new(config.auth, config.limits, metrics, runner);

    // Spawn server
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(
                remotemedia_grpc::generated::streaming_pipeline_service_server::StreamingPipelineServiceServer::new(
                    service,
                ),
            )
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    // Give server time to start
    sleep(Duration::from_millis(100)).await;

    (server_url, handle)
}

#[tokio::test]
async fn test_grpc_server_starts_successfully() {
    println!("\n🧪 Testing gRPC Server Startup\n");

    // Step 1: Start server
    println!("📡 Step 1: Starting gRPC server...");
    let (server_url, _server_handle) = start_test_server().await;
    println!("  ✓ Server started at {}", server_url);

    // Step 2: Verify server is reachable
    println!("\n🔌 Step 2: Verifying server is reachable...");
    let client_result = remotemedia_grpc::generated::streaming_pipeline_service_client::StreamingPipelineServiceClient::connect(server_url.clone())
        .await;

    assert!(
        client_result.is_ok(),
        "Failed to connect to server: {:?}",
        client_result.err()
    );
    println!("  ✓ Successfully connected to server");

    // Step 3: Verify PipelineExecutor integration
    println!("\n✅ Step 3: Verifying PipelineExecutor integration...");
    let executor = PipelineExecutor::new().unwrap();
    println!("  ✓ PipelineExecutor created successfully");

    // Access the registry to verify node types
    let node_types = executor.list_node_types().await;
    println!("  ✓ Node registry accessible from executor");

    // Verify VideoFlip is in registry
    assert!(
        node_types.contains(&"VideoFlip".to_string()),
        "VideoFlip not found in registry. Available types: {:?}",
        node_types
    );
    println!("  ✓ VideoFlip node confirmed in registry");

    println!("\n🎉 All integration tests passed!");
    println!("\n✅ Validated:");
    println!("   1. Server startup ✓");
    println!("   2. Client connection ✓");
    println!("   3. PipelineExecutor integration ✓");
    println!("   4. Registry accessibility ✓");
    println!("   5. VideoFlip node registration ✓");
}

#[tokio::test]
async fn test_multiple_concurrent_clients() {
    println!("\n🧪 Testing Multiple Concurrent Clients\n");

    // Start server
    let (server_url, _server_handle) = start_test_server().await;
    println!("✓ Server started");

    // Create multiple clients concurrently
    const NUM_CLIENTS: usize = 5;
    let mut handles = Vec::new();

    for i in 0..NUM_CLIENTS {
        let url = server_url.clone();
        let handle = tokio::spawn(async move {
            let client = remotemedia_grpc::generated::streaming_pipeline_service_client::StreamingPipelineServiceClient::connect(url)
                .await;
            assert!(client.is_ok(), "Client {} failed to connect", i);
            i
        });
        handles.push(handle);
    }

    // Wait for all clients to connect
    for handle in handles {
        let client_id = handle.await.unwrap();
        println!("✓ Client {} connected successfully", client_id);
    }

    println!("\n🎉 All {} clients connected successfully!", NUM_CLIENTS);
}

#[tokio::test]
async fn test_pipeline_runner_end_to_end() {
    println!("\n🧪 Testing PipelineExecutor End-to-End\n");

    use remotemedia_core::{
        data::RuntimeData,
        manifest::Manifest,
        transport::{StreamSession, TransportData},
    };

    // Create executor
    let executor = Arc::new(PipelineExecutor::new().unwrap());
    println!("✓ PipelineExecutor created");

    // Create manifest with VideoFlip
    let manifest_json = r#"{
        "version": "v1",
        "metadata": {
            "name": "video-flip-test",
            "description": "Test pipeline"
        },
        "nodes": [
            {
                "id": "flip",
                "node_type": "VideoFlip",
                "params": {
                    "direction": "vertical"
                }
            }
        ],
        "connections": []
    }"#;

    let manifest: Manifest = serde_json::from_str(manifest_json).unwrap();
    println!("✓ Manifest parsed");

    // Create streaming session
    let mut session = executor.create_session(Arc::new(manifest)).await.unwrap();
    println!("✓ Session created: {}", session.session_id);

    // Send test frame
    let test_frame = RuntimeData::Video {
        codec: None,
        is_keyframe: true,
        stream_id: None,
        pixel_data: vec![
            255, 0, 0, // red
            0, 255, 0, // green
            0, 0, 255, // blue
            255, 255, 255, // white
        ],
        width: 2,
        height: 2,
        format: PixelFormat::Rgb24,
        frame_number: 0,
        timestamp_us: 0,
        arrival_ts_us: None,
    };

    session
        .send_input(TransportData::new(test_frame))
        .await
        .unwrap();
    println!("✓ Sent test frame");

    // Receive result
    let result = tokio::time::timeout(Duration::from_secs(5), session.recv_output())
        .await
        .expect("Timeout")
        .unwrap()
        .expect("No output")
        .data;

    println!("✓ Received result");

    // Verify flipped
    if let RuntimeData::Video { pixel_data, .. } = result {
        assert_eq!(pixel_data[0..3], [0, 0, 255]); // blue
        assert_eq!(pixel_data[3..6], [255, 255, 255]); // white
        assert_eq!(pixel_data[6..9], [255, 0, 0]); // red
        assert_eq!(pixel_data[9..12], [0, 255, 0]); // green
        println!("✓ Video correctly flipped!");
    } else {
        panic!("Expected Video output");
    }

    // Close session
    session.close().await.unwrap();
    println!("✓ Session closed");

    println!("\n🎉 PipelineExecutor end-to-end test passed!");
}
