//! End-to-end integration test for gRPC streaming service
//!
//! This test validates the complete flow:
//! 1. Start gRPC server
//! 2. Connect client
//! 3. Create streaming session with manifest
//! 4. Send video frames through pipeline
//! 5. Receive processed video frames
//! 6. Verify VideoFlip node processes frames correctly

use remotemedia_core::transport::PipelineExecutor;
use remotemedia_grpc::generated::StreamResponse;
use remotemedia_grpc::generated::VideoCodec;
use remotemedia_grpc::generated::{
    data_buffer::DataType, stream_request::Request as StreamRequestType,
    stream_response::Response as StreamResponseType,
    streaming_pipeline_service_client::StreamingPipelineServiceClient, DataBuffer, DataChunk,
    PipelineManifest, StreamControl, StreamInit, StreamRequest,
};
use remotemedia_grpc::metrics::ServiceMetrics;
use remotemedia_grpc::{ServiceConfig, StreamingServiceImpl};
use std::sync::Arc;
use tokio::time::{sleep, timeout, Duration};
use tonic::transport::Server;
use tonic::Request;

/// Wait for StreamReady response, skipping any intermediate status messages
async fn wait_for_stream_ready(
    response_stream: &mut tonic::Streaming<StreamResponse>,
) -> StreamResponse {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let response = timeout(Duration::from_secs(5), response_stream.message())
            .await
            .expect("Timeout waiting for StreamReady")
            .expect("Stream error")
            .expect("No response");

        match &response.response {
            Some(StreamResponseType::Ready(_)) => return response,
            Some(StreamResponseType::Result(result)) => {
                // Skip status/initialization messages
                if result.data_outputs.contains_key("_status") {
                    continue;
                }
                panic!("Unexpected Result before StreamReady: {:?}", result);
            }
            Some(StreamResponseType::Error(e)) => {
                panic!("Expected StreamReady but got Error: {:?}", e);
            }
            _ => {}
        }

        if tokio::time::Instant::now() > deadline {
            panic!("Timeout waiting for StreamReady");
        }
    }
}

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
    let service = StreamingServiceImpl::new(
        config.auth,
        config.limits,
        Arc::new(ServiceMetrics::with_default_registry().unwrap()),
        runner,
    );

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

/// Create a test manifest with VideoFlip node
fn create_test_manifest() -> PipelineManifest {
    use remotemedia_grpc::generated::{ManifestMetadata, NodeManifest};

    PipelineManifest {
        version: "v1".to_string(),
        metadata: Some(ManifestMetadata {
            name: "video-flip-test".to_string(),
            description: "Test pipeline with VideoFlip node".to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
        }),
        nodes: vec![NodeManifest {
            id: "flip".to_string(),
            node_type: "VideoFlip".to_string(),
            params: "{}".to_string(),
            is_streaming: false,
            capabilities: None,
            host: String::new(),
            runtime_hint: 0,      // Auto
            input_types: vec![],  // Accept any type
            output_types: vec![], // Output any type
        }],
        connections: vec![],
        manifest_json: vec![],
    }
}
/// Create test video frame as DataBuffer (2x2 RGB24)
fn create_test_video_buffer(frame_number: u64) -> DataBuffer {
    use remotemedia_grpc::generated::{PixelFormat, VideoFrame};

    // Create 2x2 RGB24 image
    // Top row: red, green
    // Bottom row: blue, white
    let pixels = vec![
        255, 0, 0, // red
        0, 255, 0, // green
        0, 0, 255, // blue
        255, 255, 255, // white
    ];

    let video_frame = VideoFrame {
        codec: VideoCodec::H264 as i32,
        is_keyframe: true,
        pixel_data: pixels,
        width: 2,
        height: 2,
        format: PixelFormat::Rgb24 as i32,
        frame_number,
        timestamp_us: frame_number * 33333, // ~30 FPS
    };

    DataBuffer {
        data_type: Some(DataType::Video(video_frame)),
        metadata: std::collections::HashMap::new(),
    }
}

#[tokio::test]
async fn test_grpc_streaming_video_pipeline() {
    println!("\n🧪 gRPC Streaming E2E Test Starting...\n");

    // Step 1: Start gRPC server
    println!("📡 Step 1: Starting gRPC server...");
    let (server_url, _server_handle) = start_test_server().await;
    println!("  ✓ Server started at {}", server_url);

    // Step 2: Connect client
    println!("\n🔌 Step 2: Connecting client to server...");
    let mut client = timeout(
        Duration::from_secs(5),
        StreamingPipelineServiceClient::connect(server_url.clone()),
    )
    .await
    .expect("Timeout connecting to server")
    .expect("Failed to connect client");
    println!("  ✓ Client connected");

    // Step 3: Create streaming session
    println!("\n📝 Step 3: Creating streaming session with manifest...");
    let (request_tx, response_rx) = tokio::sync::mpsc::channel(32);

    // Send StreamInit
    let manifest = create_test_manifest();
    let init_request = StreamRequest {
        request: Some(StreamRequestType::Init(StreamInit {
            manifest: Some(manifest),
            data_inputs: std::collections::HashMap::new(),
            resource_limits: None,
            client_version: "test-v1.0".to_string(),
            expected_chunk_size: 0, // Use server default
            output_taps: Vec::new(),
        })),
    };

    request_tx.send(init_request).await.unwrap();

    // Create streaming request
    let request_stream = tokio_stream::wrappers::ReceiverStream::new(response_rx);
    let request = Request::new(request_stream);

    // Start bidirectional stream
    let mut response_stream = client
        .stream_pipeline(request)
        .await
        .expect("Failed to start stream")
        .into_inner();

    // Wait for StreamReady (skip any intermediate status messages)
    let ready_response = wait_for_stream_ready(&mut response_stream).await;
    if let Some(StreamResponseType::Ready(ready)) = ready_response.response {
        println!("  ✓ Session created: {}", ready.session_id);
        println!(
            "  ✓ Recommended chunk size: {}",
            ready.recommended_chunk_size
        );
    }

    // Step 4: Send video frames through pipeline
    println!("\n📹 Step 4: Sending video frames through pipeline...");
    const NUM_FRAMES: usize = 3;

    for i in 0..NUM_FRAMES {
        let video_buffer = create_test_video_buffer(i as u64);

        let chunk_request = StreamRequest {
            request: Some(StreamRequestType::DataChunk(DataChunk {
                node_id: "flip".to_string(),
                buffer: Some(video_buffer),
                named_buffers: std::collections::HashMap::new(),
                sequence: i as u64,
                timestamp_ms: i as u64 * 33, // ~30 FPS
            })),
        };

        request_tx
            .send(chunk_request)
            .await
            .expect("Failed to send chunk");
        println!("  → Sent frame {} (2x2 RGB24)", i);
    }

    // Step 5: Receive processed video frames
    println!("\n📦 Step 5: Receiving processed video frames...");
    for i in 0..NUM_FRAMES {
        // Loop to skip status messages and metrics
        let result = loop {
            let response = timeout(Duration::from_secs(5), response_stream.message())
                .await
                .expect(&format!("Timeout waiting for frame {}", i))
                .expect("Stream error")
                .expect(&format!("No response for frame {}", i));

            match response.response {
                Some(StreamResponseType::Result(r)) => {
                    // Skip status messages
                    if r.data_outputs.contains_key("_status") {
                        continue;
                    }
                    break r;
                }
                Some(StreamResponseType::Metrics(metrics)) => {
                    println!("  📊 Received metrics update");
                    println!("    - Chunks processed: {}", metrics.chunks_processed);
                    println!("    - Avg latency: {:.2}ms", metrics.average_latency_ms);
                    continue;
                }
                other => panic!("Expected Result but got: {:?}", other),
            }
        };

        println!("  ← Received frame {} result", i);

        // Verify we got video data back in data_outputs
        assert!(
            !result.data_outputs.is_empty(),
            "Frame {} missing data_outputs",
            i
        );

        // Extract the video frame from the "flip" node output
        let output_buffer = result
            .data_outputs
            .get("flip")
            .expect(&format!("Frame {} missing 'flip' output", i));

        let video_frame = match &output_buffer.data_type {
            Some(DataType::Video(frame)) => frame,
            _ => panic!("Frame {} output is not video", i),
        };

        assert_eq!(video_frame.width, 2, "Frame {} wrong width", i);
        assert_eq!(video_frame.height, 2, "Frame {} wrong height", i);
        assert_eq!(
            video_frame.pixel_data.len(),
            12,
            "Frame {} wrong data size",
            i
        );

        // Step 6: Verify VideoFlip processed the frame correctly
        println!(
            "\n✅ Step 6: Verifying VideoFlip processing for frame {}...",
            i
        );

        // After vertical flip, the pixels should be reordered:
        // Original top row: red (255,0,0), green (0,255,0)
        // Original bottom row: blue (0,0,255), white (255,255,255)
        // Flipped top row: blue, white
        // Flipped bottom row: red, green

        let pixels = &video_frame.pixel_data;
        assert_eq!(pixels[0..3], [0, 0, 255], "Frame {} pixel 0 (blue)", i);
        assert_eq!(pixels[3..6], [255, 255, 255], "Frame {} pixel 1 (white)", i);
        assert_eq!(pixels[6..9], [255, 0, 0], "Frame {} pixel 2 (red)", i);
        assert_eq!(pixels[9..12], [0, 255, 0], "Frame {} pixel 3 (green)", i);

        println!("  ✓ Frame {} correctly flipped!", i);
        println!("    - Top row: blue, white (was bottom)");
        println!("    - Bottom row: red, green (was top)");

        // Verify sequence number
        assert_eq!(result.sequence, i as u64, "Frame {} wrong sequence", i);
        println!("  ✓ Sequence number correct: {}", result.sequence);

        // Verify processing time exists
        println!("  ✓ Processing time: {:.2}ms", result.processing_time_ms);
    }

    // Step 7: Close session gracefully
    println!("\n🛑 Step 7: Closing session...");
    let close_request = StreamRequest {
        request: Some(StreamRequestType::Control(StreamControl {
            command: 1, // CLOSE
        })),
    };

    request_tx
        .send(close_request)
        .await
        .expect("Failed to send close");

    // Wait for StreamClosed
    let close_response = timeout(Duration::from_secs(5), response_stream.message())
        .await
        .expect("Timeout waiting for StreamClosed")
        .expect("Stream error");

    if let Some(response) = close_response {
        match response.response {
            Some(StreamResponseType::Closed(closed)) => {
                println!("  ✓ Session closed: {}", closed.session_id);
                println!("  ✓ Reason: {}", closed.reason);
                if let Some(final_metrics) = closed.final_metrics {
                    println!(
                        "  ✓ Final metrics: {:.2}ms wall time",
                        final_metrics.wall_time_ms
                    );
                }
            }
            _ => panic!("Expected StreamClosed response"),
        }
    }

    println!("\n🎉 All gRPC streaming E2E tests passed!");
    println!("\n✅ Validated complete flow:");
    println!("   1. Server startup ✓");
    println!("   2. Client connection ✓");
    println!("   3. Session creation with manifest ✓");
    println!("   4. Video frame transmission ✓");
    println!("   5. Pipeline processing (VideoFlip) ✓");
    println!("   6. Processed frame reception ✓");
    println!("   7. Graceful session closure ✓");
}

#[tokio::test]
async fn test_grpc_streaming_multiple_sessions() {
    println!("\n🧪 gRPC Multiple Sessions Test...\n");

    // Start server
    let (server_url, _server_handle) = start_test_server().await;
    println!("✓ Server started");

    // Create two concurrent sessions
    let session1 = tokio::spawn({
        let url = server_url.clone();
        async move {
            let mut client = StreamingPipelineServiceClient::connect(url).await.unwrap();

            let (tx, rx) = tokio::sync::mpsc::channel(32);
            let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

            // Init session 1
            tx.send(StreamRequest {
                request: Some(StreamRequestType::Init(StreamInit {
                    manifest: Some(create_test_manifest()),
                    data_inputs: std::collections::HashMap::new(),
                    resource_limits: None,
                    client_version: "session1".to_string(),
                    expected_chunk_size: 0,
                    output_taps: Vec::new(),
                })),
            })
            .await
            .unwrap();

            let mut response_stream = client
                .stream_pipeline(Request::new(stream))
                .await
                .unwrap()
                .into_inner();

            // Wait for ready (skip status messages)
            let ready = wait_for_stream_ready(&mut response_stream).await;
            assert!(matches!(ready.response, Some(StreamResponseType::Ready(_))));

            // Send one frame
            tx.send(StreamRequest {
                request: Some(StreamRequestType::DataChunk(DataChunk {
                    node_id: "flip".to_string(),
                    buffer: Some(create_test_video_buffer(0)),
                    named_buffers: std::collections::HashMap::new(),
                    sequence: 0,
                    timestamp_ms: 0,
                })),
            })
            .await
            .unwrap();

            // Receive result (skip any additional status messages)
            loop {
                let result = response_stream.message().await.unwrap().unwrap();
                match &result.response {
                    Some(StreamResponseType::Result(r))
                        if !r.data_outputs.contains_key("_status") =>
                    {
                        break;
                    }
                    Some(StreamResponseType::Result(_)) => continue, // Skip status messages
                    _ => panic!("Expected Result but got: {:?}", result.response),
                }
            }

            "session1 complete"
        }
    });

    let session2 = tokio::spawn({
        let url = server_url.clone();
        async move {
            let mut client = StreamingPipelineServiceClient::connect(url).await.unwrap();

            let (tx, rx) = tokio::sync::mpsc::channel(32);
            let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

            // Init session 2
            tx.send(StreamRequest {
                request: Some(StreamRequestType::Init(StreamInit {
                    manifest: Some(create_test_manifest()),
                    data_inputs: std::collections::HashMap::new(),
                    resource_limits: None,
                    client_version: "session2".to_string(),
                    expected_chunk_size: 0,
                    output_taps: Vec::new(),
                })),
            })
            .await
            .unwrap();

            let mut response_stream = client
                .stream_pipeline(Request::new(stream))
                .await
                .unwrap()
                .into_inner();

            // Wait for ready (skip status messages)
            let ready = wait_for_stream_ready(&mut response_stream).await;
            assert!(matches!(ready.response, Some(StreamResponseType::Ready(_))));

            // Send one frame
            tx.send(StreamRequest {
                request: Some(StreamRequestType::DataChunk(DataChunk {
                    node_id: "flip".to_string(),
                    buffer: Some(create_test_video_buffer(0)),
                    named_buffers: std::collections::HashMap::new(),
                    sequence: 0,
                    timestamp_ms: 0,
                })),
            })
            .await
            .unwrap();

            // Receive result (skip any additional status messages)
            loop {
                let result = response_stream.message().await.unwrap().unwrap();
                match &result.response {
                    Some(StreamResponseType::Result(r))
                        if !r.data_outputs.contains_key("_status") =>
                    {
                        break;
                    }
                    Some(StreamResponseType::Result(_)) => continue, // Skip status messages
                    _ => panic!("Expected Result but got: {:?}", result.response),
                }
            }

            "session2 complete"
        }
    });

    // Wait for both sessions
    let result1 = session1.await.unwrap();
    let result2 = session2.await.unwrap();

    println!("✓ {}", result1);
    println!("✓ {}", result2);
    println!("\n🎉 Multiple concurrent sessions test passed!");
}
