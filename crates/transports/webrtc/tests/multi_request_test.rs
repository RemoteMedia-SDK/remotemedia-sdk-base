//! Integration test for multi-request pipeline execution
//!
//! Tests that multiple TTS requests can be processed through the same session
//! without the pipeline execution blocking or failing.

use remotemedia_core::{
    data::RuntimeData,
    manifest::Manifest,
    transport::{PipelineExecutor, StreamSession, TransportData},
};
use std::sync::Arc;
use tokio::time::{timeout, Duration};

/// Test that multiple inputs can be sent through a streaming session
/// and all produce outputs
#[tokio::test]
async fn test_multi_request_streaming_session() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();

    // Create a simple manifest with PassthroughNode
    let manifest_json = r#"
    {
        "version": "v1",
        "metadata": {
            "name": "test-passthrough",
            "description": "Test multi-request processing"
        },
        "nodes": [
            {
                "id": "passthrough",
                "node_type": "PassThrough",
                "params": {}
            }
        ],
        "connections": []
    }
    "#;

    let manifest: Manifest = serde_json::from_str(manifest_json).expect("Failed to parse manifest");
    let manifest = Arc::new(manifest);

    // Create pipeline executor
    let runner = PipelineExecutor::new().expect("Failed to create pipeline executor");

    // Create streaming session
    let mut session = runner
        .create_session(Arc::clone(&manifest))
        .await
        .expect("Failed to create streaming session");

    println!("Session created: {}", session.session_id());
    assert!(
        session.is_active(),
        "Session should be active after creation"
    );

    // Test 3 consecutive requests
    for request_num in 1..=3 {
        println!("\n=== Request {} ===", request_num);

        // Create input data (text)
        let input_text = format!("Test request {}", request_num);
        let input_data = RuntimeData::Text(input_text.clone());
        let transport_data = TransportData::new(input_data);

        // Send input
        println!("Sending input: {}", input_text);
        session
            .send_input(transport_data)
            .await
            .expect("Failed to send input");

        // Receive output with timeout
        println!("Waiting for output...");
        let output = timeout(Duration::from_secs(5), session.recv_output())
            .await
            .expect("Timeout waiting for output")
            .expect("Failed to receive output")
            .expect("No output received");

        println!("Received output: {:?}", output.data);

        // Verify output matches input (passthrough)
        match &output.data {
            RuntimeData::Text(text) => {
                assert_eq!(text, &input_text, "Output text should match input");
            }
            _ => panic!("Expected Text output, got {:?}", output.data),
        }

        println!("✓ Request {} succeeded", request_num);

        // Verify session is still active
        assert!(
            session.is_active(),
            "Session should remain active after request {}",
            request_num
        );
    }

    println!("\n=== All requests succeeded ===");

    // Clean up
    session.close().await.expect("Failed to close session");
    println!("Session closed");
}

/// Test that demonstrates the select starvation issue with mock delays
#[tokio::test]
async fn test_select_starvation_with_delays() {
    use tokio::sync::mpsc;

    let (input_tx, mut input_rx) = mpsc::unbounded_channel::<String>();
    let (output_tx, mut output_rx) = mpsc::unbounded_channel::<String>();

    // Simulate the select loop from server_peer.rs
    let task = tokio::spawn(async move {
        let mut input_count = 0;
        let mut output_count = 0;

        loop {
            tokio::select! {
                biased;

                // Input branch (should have priority)
                Some(input) = input_rx.recv() => {
                    input_count += 1;
                    println!("Processing input #{}: {}", input_count, input);

                    // Simulate pipeline execution
                    tokio::time::sleep(Duration::from_millis(10)).await;

                    // Produce output
                    output_tx.send(format!("Output for {}", input)).unwrap();
                }

                // Output branch (blocks if no data)
                // This demonstrates the starvation issue if we don't have a timeout
                Some(output) = output_rx.recv() => {
                    output_count += 1;
                    println!("Received output #{}: {}", output_count, output);
                }

                else => {
                    println!("All channels closed, exiting");
                    break;
                }
            }

            // Check if we've processed enough
            if input_count >= 3 && output_count >= 3 {
                break;
            }
        }

        (input_count, output_count)
    });

    // Send 3 inputs
    for i in 1..=3 {
        println!("\nSending input {}", i);
        input_tx.send(format!("Input {}", i)).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    drop(input_tx);
    // Note: output_rx was moved into the task, so we can't drop it here

    // Wait for task to complete
    let (input_count, output_count) = timeout(Duration::from_secs(5), task)
        .await
        .expect("Task timed out")
        .expect("Task panicked");

    println!(
        "\nProcessed {} inputs and {} outputs",
        input_count, output_count
    );
    assert_eq!(input_count, 3, "Should process all 3 inputs");
}

/// Test the timeout approach to prevent select starvation
#[tokio::test]
async fn test_select_with_timeout() {
    use tokio::sync::mpsc;

    let (input_tx, mut input_rx) = mpsc::unbounded_channel::<String>();
    let (output_tx, mut output_rx) = mpsc::unbounded_channel::<String>();

    // Simulate the fixed select loop with timeout
    let task = tokio::spawn(async move {
        let mut input_count = 0;
        let mut output_count = 0;

        loop {
            tokio::select! {
                biased;

                // Input branch (highest priority)
                Some(input) = input_rx.recv() => {
                    input_count += 1;
                    println!("Processing input #{}: {}", input_count, input);

                    // Simulate pipeline execution
                    tokio::time::sleep(Duration::from_millis(10)).await;

                    // Produce output
                    output_tx.send(format!("Output for {}", input)).unwrap();
                }

                // Output branch with TIMEOUT to prevent starvation
                output_result = tokio::time::timeout(
                    Duration::from_millis(10),
                    output_rx.recv()
                ) => {
                    match output_result {
                        Ok(Some(output)) => {
                            output_count += 1;
                            println!("Received output #{}: {}", output_count, output);
                        }
                        Ok(None) => {
                            println!("Output channel closed");
                        }
                        Err(_timeout) => {
                            // Timeout is normal - allows checking other branches
                        }
                    }
                }

                else => {
                    println!("Input channel closed, exiting");
                    break;
                }
            }

            // Check if we've processed enough
            if input_count >= 3 && output_count >= 3 {
                break;
            }
        }

        (input_count, output_count)
    });

    // Send 3 inputs
    for i in 1..=3 {
        println!("\nSending input {}", i);
        input_tx.send(format!("Input {}", i)).unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    drop(input_tx);

    // Wait for task to complete
    let (input_count, output_count) = timeout(Duration::from_secs(5), task)
        .await
        .expect("Task timed out")
        .expect("Task panicked");

    println!(
        "\nProcessed {} inputs and {} outputs",
        input_count, output_count
    );
    assert_eq!(input_count, 3, "Should process all 3 inputs");
    assert_eq!(output_count, 3, "Should receive all 3 outputs");
}
