//! Multi-Track Streaming End-to-End Tests
//!
//! Spec 013: Dynamic Multi-Track Streaming - E2E validation
//!
//! These tests use the WebRtcTestHarness to validate multi-track functionality
//! in a real WebRTC environment with signaling, SDP negotiation, and RTP transport.
//!
//! ## Test Scenarios
//!
//! 1. Single client with multiple video tracks
//! 2. Single client with multiple audio tracks
//! 3. Mixed audio and video multi-track
//! 4. Multiple clients with multi-track streams
//! 5. Track addition after initial connection

mod harness;

use harness::{HarnessError, HarnessResult, WebRtcTestHarness, PASSTHROUGH_MANIFEST};
use std::time::Duration;
use tracing::info;

// =============================================================================
// Test Setup Helpers
// =============================================================================

/// Initialize tracing for tests (call once per test)
fn init_test_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,webrtc=warn")
        .try_init();
}

// =============================================================================
// US1-E2E: Single Dynamic Video Stream E2E Tests
// =============================================================================

/// Test that a client can add a video track with a specific stream_id
#[tokio::test]
async fn test_e2e_add_video_track_with_stream_id() -> HarnessResult<()> {
    init_test_tracing();
    info!("Starting test_e2e_add_video_track_with_stream_id");

    let harness = WebRtcTestHarness::new(PASSTHROUGH_MANIFEST).await?;
    harness
        .wait_for_server_ready(Duration::from_secs(5))
        .await?;

    // Create and connect a test client
    let client = harness.create_client("multi-track-client-1").await?;

    // Add a video track with stream_id "camera"
    let _camera_track = client.add_video_track_with_stream_id("camera").await?;
    assert_eq!(client.video_track_count().await, 1);

    // Verify the track was created
    let stream_ids = client.video_stream_ids().await;
    assert!(stream_ids.contains(&"camera".to_string()));

    info!("Successfully added video track with stream_id 'camera'");

    // Cleanup
    harness.shutdown().await;
    Ok(())
}

/// Test that a client can add multiple video tracks with different stream_ids
#[tokio::test]
async fn test_e2e_multiple_video_tracks() -> HarnessResult<()> {
    init_test_tracing();
    info!("Starting test_e2e_multiple_video_tracks");

    let harness = WebRtcTestHarness::new(PASSTHROUGH_MANIFEST).await?;
    harness
        .wait_for_server_ready(Duration::from_secs(5))
        .await?;

    let client = harness.create_client("multi-video-client").await?;

    // Add multiple video tracks
    client.add_video_track_with_stream_id("camera").await?;
    client.add_video_track_with_stream_id("screen").await?;
    client.add_video_track_with_stream_id("backup").await?;

    // Verify all tracks were created
    assert_eq!(client.video_track_count().await, 3);

    let stream_ids = client.video_stream_ids().await;
    assert!(stream_ids.contains(&"camera".to_string()));
    assert!(stream_ids.contains(&"screen".to_string()));
    assert!(stream_ids.contains(&"backup".to_string()));

    info!(
        "Successfully added {} video tracks",
        client.video_track_count().await
    );

    harness.shutdown().await;
    Ok(())
}

// =============================================================================
// US2-E2E: Multiple Video Streams Per Peer E2E Tests
// =============================================================================

/// Test sending video frames on specific streams
#[tokio::test]
async fn test_e2e_send_video_on_stream() -> HarnessResult<()> {
    init_test_tracing();
    info!("Starting test_e2e_send_video_on_stream");

    let harness = WebRtcTestHarness::new(PASSTHROUGH_MANIFEST).await?;
    harness
        .wait_for_server_ready(Duration::from_secs(5))
        .await?;

    let client = harness.create_client("video-sender-client").await?;

    // Add camera and screen tracks
    client.add_video_track_with_stream_id("camera").await?;
    client.add_video_track_with_stream_id("screen").await?;

    // Generate test frames
    let camera_frame = harness.generate_solid_frame(1920, 1080, 128, 128, 128);
    let screen_frame = harness.generate_test_pattern(1280, 720);

    // Send frames on each stream
    client
        .send_video_on_stream("camera", &camera_frame, 1920, 1080)
        .await?;
    client
        .send_video_on_stream("screen", &screen_frame, 1280, 720)
        .await?;

    info!("Successfully sent video frames on multiple streams");

    harness.shutdown().await;
    Ok(())
}

/// Test that sending to non-existent stream fails gracefully
#[tokio::test]
async fn test_e2e_send_video_on_nonexistent_stream() -> HarnessResult<()> {
    init_test_tracing();
    info!("Starting test_e2e_send_video_on_nonexistent_stream");

    let harness = WebRtcTestHarness::new(PASSTHROUGH_MANIFEST).await?;
    harness
        .wait_for_server_ready(Duration::from_secs(5))
        .await?;

    let client = harness.create_client("error-test-client").await?;

    // Try to send on a stream that doesn't exist
    let frame = harness.generate_solid_frame(640, 480, 128, 128, 128);
    let result = client
        .send_video_on_stream("nonexistent", &frame, 640, 480)
        .await;

    assert!(result.is_err());
    info!("Correctly rejected send to non-existent stream");

    harness.shutdown().await;
    Ok(())
}

// =============================================================================
// US3-E2E: Multiple Audio Streams E2E Tests
// =============================================================================

/// Test that a client can add an audio track with a specific stream_id
#[tokio::test]
async fn test_e2e_add_audio_track_with_stream_id() -> HarnessResult<()> {
    init_test_tracing();
    info!("Starting test_e2e_add_audio_track_with_stream_id");

    let harness = WebRtcTestHarness::new(PASSTHROUGH_MANIFEST).await?;
    harness
        .wait_for_server_ready(Duration::from_secs(5))
        .await?;

    let client = harness.create_client("audio-track-client").await?;

    // Add an audio track with stream_id "voice"
    client.add_audio_track_with_stream_id("voice").await?;
    assert_eq!(client.audio_track_count().await, 1);

    let stream_ids = client.audio_stream_ids().await;
    assert!(stream_ids.contains(&"voice".to_string()));

    info!("Successfully added audio track with stream_id 'voice'");

    harness.shutdown().await;
    Ok(())
}

/// Test that a client can add multiple audio tracks
#[tokio::test]
async fn test_e2e_multiple_audio_tracks() -> HarnessResult<()> {
    init_test_tracing();
    info!("Starting test_e2e_multiple_audio_tracks");

    let harness = WebRtcTestHarness::new(PASSTHROUGH_MANIFEST).await?;
    harness
        .wait_for_server_ready(Duration::from_secs(5))
        .await?;

    let client = harness.create_client("multi-audio-client").await?;

    // Add multiple audio tracks
    client.add_audio_track_with_stream_id("voice").await?;
    client.add_audio_track_with_stream_id("music").await?;
    client.add_audio_track_with_stream_id("effects").await?;

    assert_eq!(client.audio_track_count().await, 3);

    let stream_ids = client.audio_stream_ids().await;
    assert!(stream_ids.contains(&"voice".to_string()));
    assert!(stream_ids.contains(&"music".to_string()));
    assert!(stream_ids.contains(&"effects".to_string()));

    info!(
        "Successfully added {} audio tracks",
        client.audio_track_count().await
    );

    harness.shutdown().await;
    Ok(())
}

/// Test sending audio on specific streams
#[tokio::test]
async fn test_e2e_send_audio_on_stream() -> HarnessResult<()> {
    init_test_tracing();
    info!("Starting test_e2e_send_audio_on_stream");

    let harness = WebRtcTestHarness::new(PASSTHROUGH_MANIFEST).await?;
    harness
        .wait_for_server_ready(Duration::from_secs(5))
        .await?;

    let client = harness.create_client("audio-sender-client").await?;

    // Add voice and music tracks
    client.add_audio_track_with_stream_id("voice").await?;
    client.add_audio_track_with_stream_id("music").await?;

    // Generate test audio
    let voice_samples = harness.generate_sine_wave(440.0, 0.1, 48000);
    let music_samples = harness.generate_sine_wave(880.0, 0.1, 48000);

    // Send audio on each stream
    client
        .send_audio_on_stream("voice", &voice_samples, 48000)
        .await?;
    client
        .send_audio_on_stream("music", &music_samples, 48000)
        .await?;

    info!("Successfully sent audio on multiple streams");

    harness.shutdown().await;
    Ok(())
}

// =============================================================================
// Mixed Audio/Video Multi-Track E2E Tests
// =============================================================================

/// Test a client with both multiple video and audio tracks
#[tokio::test]
async fn test_e2e_mixed_audio_video_multi_track() -> HarnessResult<()> {
    init_test_tracing();
    info!("Starting test_e2e_mixed_audio_video_multi_track");

    let harness = WebRtcTestHarness::new(PASSTHROUGH_MANIFEST).await?;
    harness
        .wait_for_server_ready(Duration::from_secs(5))
        .await?;

    let client = harness.create_client("mixed-media-client").await?;

    // Add video tracks
    client.add_video_track_with_stream_id("camera").await?;
    client.add_video_track_with_stream_id("screen").await?;

    // Add audio tracks
    client.add_audio_track_with_stream_id("voice").await?;
    client.add_audio_track_with_stream_id("music").await?;

    // Verify counts
    assert_eq!(client.video_track_count().await, 2);
    assert_eq!(client.audio_track_count().await, 2);

    // Send media on all streams
    let video_frame = harness.generate_solid_frame(1280, 720, 128, 128, 128);
    let audio_samples = harness.generate_sine_wave(440.0, 0.1, 48000);

    client
        .send_video_on_stream("camera", &video_frame, 1280, 720)
        .await?;
    client
        .send_video_on_stream("screen", &video_frame, 1280, 720)
        .await?;
    client
        .send_audio_on_stream("voice", &audio_samples, 48000)
        .await?;
    client
        .send_audio_on_stream("music", &audio_samples, 48000)
        .await?;

    info!("Successfully managed mixed audio/video multi-track client");

    harness.shutdown().await;
    Ok(())
}

/// Test that same stream_id can be used for both audio and video independently
#[tokio::test]
async fn test_e2e_same_stream_id_audio_and_video() -> HarnessResult<()> {
    init_test_tracing();
    info!("Starting test_e2e_same_stream_id_audio_and_video");

    let harness = WebRtcTestHarness::new(PASSTHROUGH_MANIFEST).await?;
    harness
        .wait_for_server_ready(Duration::from_secs(5))
        .await?;

    let client = harness.create_client("same-stream-id-client").await?;

    // Use "main" as stream_id for both audio and video
    client.add_video_track_with_stream_id("main").await?;
    client.add_audio_track_with_stream_id("main").await?;

    // Both should work independently
    assert_eq!(client.video_track_count().await, 1);
    assert_eq!(client.audio_track_count().await, 1);

    // Video stream IDs and audio stream IDs are tracked separately
    let video_ids = client.video_stream_ids().await;
    let audio_ids = client.audio_stream_ids().await;

    assert!(video_ids.contains(&"main".to_string()));
    assert!(audio_ids.contains(&"main".to_string()));

    info!("Successfully used same stream_id for both audio and video");

    harness.shutdown().await;
    Ok(())
}

// =============================================================================
// Multiple Clients E2E Tests
// =============================================================================

/// Test multiple clients each with their own multi-track streams
#[tokio::test]
async fn test_e2e_multiple_clients_multi_track() -> HarnessResult<()> {
    init_test_tracing();
    info!("Starting test_e2e_multiple_clients_multi_track");

    let harness = WebRtcTestHarness::new(PASSTHROUGH_MANIFEST).await?;
    harness
        .wait_for_server_ready(Duration::from_secs(5))
        .await?;

    // Create two clients
    let client1 = harness.create_client("multi-client-1").await?;
    let client2 = harness.create_client("multi-client-2").await?;

    // Client 1: camera + voice
    client1.add_video_track_with_stream_id("camera").await?;
    client1.add_audio_track_with_stream_id("voice").await?;

    // Client 2: screen + camera
    client2.add_video_track_with_stream_id("screen").await?;
    client2.add_video_track_with_stream_id("camera").await?;

    // Verify each client has its own tracks
    assert_eq!(client1.video_track_count().await, 1);
    assert_eq!(client1.audio_track_count().await, 1);
    assert_eq!(client2.video_track_count().await, 2);
    assert_eq!(client2.audio_track_count().await, 0);

    info!("Successfully managed multiple clients with independent multi-track streams");

    harness.shutdown().await;
    Ok(())
}

/// Test concurrent client operations
#[tokio::test]
async fn test_e2e_concurrent_multi_track_operations() -> HarnessResult<()> {
    init_test_tracing();
    info!("Starting test_e2e_concurrent_multi_track_operations");

    let harness = WebRtcTestHarness::new(PASSTHROUGH_MANIFEST).await?;
    harness
        .wait_for_server_ready(Duration::from_secs(5))
        .await?;

    // Create multiple clients concurrently
    let clients = harness.create_clients(3).await?;
    assert_eq!(clients.len(), 3);

    // Each client adds tracks concurrently
    let mut handles = vec![];
    for (i, client) in clients.iter().enumerate() {
        let client = std::sync::Arc::clone(client);
        let handle = tokio::spawn(async move {
            client
                .add_video_track_with_stream_id(&format!("video{}", i))
                .await?;
            client
                .add_audio_track_with_stream_id(&format!("audio{}", i))
                .await?;
            Ok::<_, HarnessError>(())
        });
        handles.push(handle);
    }

    // Wait for all to complete
    for handle in handles {
        handle.await.unwrap()?;
    }

    // Verify each client has 1 video and 1 audio track
    for client in &clients {
        assert_eq!(client.video_track_count().await, 1);
        assert_eq!(client.audio_track_count().await, 1);
    }

    info!("Successfully completed concurrent multi-track operations");

    harness.shutdown().await;
    Ok(())
}

// =============================================================================
// Edge Cases and Error Handling E2E Tests
// =============================================================================

/// Test adding tracks before connection fails gracefully
#[tokio::test]
async fn test_e2e_add_track_before_connect() -> HarnessResult<()> {
    init_test_tracing();
    info!("Starting test_e2e_add_track_before_connect");

    let harness = WebRtcTestHarness::new(PASSTHROUGH_MANIFEST).await?;
    harness
        .wait_for_server_ready(Duration::from_secs(5))
        .await?;

    // Create client but DON'T connect (just construct it)
    let client = harness::test_client::TestClient::new(
        "unconnected-client".to_string(),
        harness.server_port(),
    )
    .await?;

    // Try to add a track - should fail since not connected
    let result = client.add_video_track_with_stream_id("camera").await;
    assert!(result.is_err());

    info!("Correctly rejected track addition before connection");

    harness.shutdown().await;
    Ok(())
}

/// Test client cleanup properly releases multi-track resources
#[tokio::test]
async fn test_e2e_multi_track_cleanup() -> HarnessResult<()> {
    init_test_tracing();
    info!("Starting test_e2e_multi_track_cleanup");

    let harness = WebRtcTestHarness::new(PASSTHROUGH_MANIFEST).await?;
    harness
        .wait_for_server_ready(Duration::from_secs(5))
        .await?;

    let client = harness.create_client("cleanup-test-client").await?;

    // Add multiple tracks
    client.add_video_track_with_stream_id("camera").await?;
    client.add_video_track_with_stream_id("screen").await?;
    client.add_audio_track_with_stream_id("voice").await?;

    assert_eq!(client.video_track_count().await, 2);
    assert_eq!(client.audio_track_count().await, 1);

    // Remove client (disconnects and removes from harness)
    harness.remove_client("cleanup-test-client").await?;

    // Client should be cleaned up from harness
    assert_eq!(harness.client_count().await, 0);

    info!("Successfully cleaned up multi-track resources");

    harness.shutdown().await;
    Ok(())
}

// =============================================================================
// Stream ID Validation E2E Tests
// =============================================================================

/// Test that various valid stream IDs work correctly
#[tokio::test]
async fn test_e2e_valid_stream_id_patterns() -> HarnessResult<()> {
    init_test_tracing();
    info!("Starting test_e2e_valid_stream_id_patterns");

    let harness = WebRtcTestHarness::new(PASSTHROUGH_MANIFEST).await?;
    harness
        .wait_for_server_ready(Duration::from_secs(5))
        .await?;

    let client = harness.create_client("stream-id-test-client").await?;

    // Test various valid stream_id patterns
    let valid_ids = [
        "camera",
        "screen_share",
        "audio-main",
        "Track_123",
        "a",
        "UPPERCASE",
        "_leading_underscore",
        "-leading-hyphen",
    ];

    for stream_id in &valid_ids {
        let result = client.add_video_track_with_stream_id(stream_id).await;
        assert!(
            result.is_ok(),
            "Stream ID '{}' should be valid, got {:?}",
            stream_id,
            result.err()
        );
    }

    assert_eq!(client.video_track_count().await, valid_ids.len());

    info!(
        "Successfully validated {} stream ID patterns",
        valid_ids.len()
    );

    harness.shutdown().await;
    Ok(())
}

// =============================================================================
// Integration with FrameRouter E2E Tests (if applicable)
// =============================================================================

/// Test end-to-end video frame routing with stream_id
#[tokio::test]
async fn test_e2e_frame_routing_basic() -> HarnessResult<()> {
    init_test_tracing();
    info!("Starting test_e2e_frame_routing_basic");

    let harness = WebRtcTestHarness::new(PASSTHROUGH_MANIFEST).await?;
    harness
        .wait_for_server_ready(Duration::from_secs(5))
        .await?;

    let client = harness.create_client("frame-routing-client").await?;

    // Add camera track and send multiple frames
    client.add_video_track_with_stream_id("camera").await?;

    let frame = harness.generate_solid_frame(1280, 720, 128, 128, 128);

    // Send 10 frames rapidly
    for _ in 0..10 {
        client
            .send_video_on_stream("camera", &frame, 1280, 720)
            .await?;
    }

    info!("Successfully sent 10 video frames on 'camera' stream");

    harness.shutdown().await;
    Ok(())
}

#[cfg(test)]
mod unit_tests {
    #[test]
    fn test_harness_module_exists() {
        // Simple test to verify harness module is accessible
        assert!(true);
    }
}
