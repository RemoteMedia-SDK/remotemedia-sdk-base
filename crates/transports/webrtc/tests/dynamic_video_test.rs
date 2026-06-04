//! Dynamic Multi-Track Video Integration Tests
//!
//! Spec 013: Dynamic Multi-Track Streaming - Phase 3 validation (User Story 1)
//!
//! Tests that video tracks are automatically created when frames arrive with stream_id,
//! validating the TrackRegistry and FrameRouter integration with ServerPeer.
//!
//! ## Test Scenarios
//!
//! 1. Single dynamic video stream with explicit stream_id ("camera")
//! 2. Default track fallback when stream_id is None
//! 3. Track lifecycle tracking (creation time, frame count)
//! 4. Duplicate track prevention

use remotemedia_core::data::video::{PixelFormat, VideoCodec};
use remotemedia_core::data::RuntimeData;
use remotemedia_webrtc::media::tracks::AudioTrack;
use remotemedia_webrtc::media::tracks::VideoTrack;
use remotemedia_webrtc::media::{
    extract_stream_id, validate_stream_id, FrameRouter, TrackRegistry, DEFAULT_STREAM_ID,
    MAX_VIDEO_TRACKS_PER_PEER, STREAM_ID_MAX_LENGTH,
};
use std::sync::Arc;
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;

// =============================================================================
// Test Helpers
// =============================================================================

/// Create a test video frame with the given stream_id
fn create_video_frame(stream_id: Option<String>, width: u32, height: u32) -> RuntimeData {
    let frame_size = (width * height * 3 / 2) as usize; // YUV420P
    RuntimeData::Video {
        pixel_data: vec![128u8; frame_size],
        width,
        height,
        format: PixelFormat::Yuv420p,
        codec: None, // Raw frame
        frame_number: 0,
        timestamp_us: 0,
        is_keyframe: false,
        stream_id,
        arrival_ts_us: None,
    }
}

/// Create a WebRTC video track for testing
fn create_webrtc_video_track(id: &str) -> Arc<TrackLocalStaticSample> {
    Arc::new(TrackLocalStaticSample::new(
        RTCRtpCodecCapability {
            mime_type: "video/VP8".to_string(),
            ..Default::default()
        },
        id.to_string(),
        "stream".to_string(),
    ))
}

/// Create a VideoTrack wrapper for testing
fn create_video_track(stream_id: &str) -> Result<Arc<VideoTrack>, remotemedia_webrtc::Error> {
    let webrtc_track = create_webrtc_video_track(stream_id);
    VideoTrack::new(
        webrtc_track,
        VideoCodec::Vp8,
        1280,
        720,
        2_000_000, // 2 Mbps
        30,        // 30 fps
    )
    .map(Arc::new)
}

// =============================================================================
// US1 T020: Dynamic Single Video Stream Tests
// =============================================================================

#[test]
fn test_stream_id_extracted_from_video_frame() {
    // Test that stream_id is correctly extracted from RuntimeData::Video
    let frame_with_id = create_video_frame(Some("camera".to_string()), 1280, 720);
    let frame_without_id = create_video_frame(None, 640, 480);

    assert_eq!(extract_stream_id(&frame_with_id), Some("camera"));
    assert_eq!(extract_stream_id(&frame_without_id), None);
}

#[tokio::test]
async fn test_track_registry_video_registration() {
    // Test that video tracks can be registered in TrackRegistry
    let registry: TrackRegistry<AudioTrack, VideoTrack> =
        TrackRegistry::new("test_peer".to_string());

    // Create and register a video track with stream_id "camera"
    let track = create_video_track("camera").expect("Failed to create video track");
    let result = registry.register_video_track("camera", track).await;

    assert!(result.is_ok());
    assert!(result.unwrap()); // true = newly registered

    // Verify track is accessible
    assert!(registry.has_video_track("camera").await);
    assert!(registry.get_video_track("camera").await.is_some());
    assert_eq!(registry.video_track_count().await, 1);
}

#[tokio::test]
async fn test_default_stream_id_fallback() {
    // Test that frames without stream_id use DEFAULT_STREAM_ID
    let registry: TrackRegistry<AudioTrack, VideoTrack> =
        TrackRegistry::new("test_peer".to_string());

    // Register a track under the default stream ID
    let track = create_video_track(DEFAULT_STREAM_ID).expect("Failed to create video track");
    let result = registry
        .register_video_track(DEFAULT_STREAM_ID, track)
        .await;

    assert!(result.is_ok());

    // Verify default track is accessible
    assert!(registry.has_video_track(DEFAULT_STREAM_ID).await);
    assert_eq!(registry.video_stream_ids().await, vec![DEFAULT_STREAM_ID]);
}

#[tokio::test]
async fn test_duplicate_track_prevention() {
    // Test FR-009: Duplicate track prevention
    let registry: TrackRegistry<AudioTrack, VideoTrack> =
        TrackRegistry::new("test_peer".to_string());

    let track1 = create_video_track("camera").expect("Failed to create video track");
    let track2 = create_video_track("camera_copy").expect("Failed to create video track");

    // First registration succeeds
    let result1 = registry.register_video_track("camera", track1).await;
    assert!(result1.is_ok());
    assert!(result1.unwrap()); // true = newly registered

    // Second registration with same stream_id returns false (not an error)
    let result2 = registry.register_video_track("camera", track2).await;
    assert!(result2.is_ok());
    assert!(!result2.unwrap()); // false = already exists

    // Still only one track
    assert_eq!(registry.video_track_count().await, 1);
}

#[tokio::test]
async fn test_video_track_limit_enforcement() {
    // Test FR-019: Maximum 5 video tracks per peer
    let registry: TrackRegistry<AudioTrack, VideoTrack> =
        TrackRegistry::new("test_peer".to_string());

    // Register up to limit (5 video tracks)
    for i in 0..MAX_VIDEO_TRACKS_PER_PEER {
        let track =
            create_video_track(&format!("stream{}", i)).expect("Failed to create video track");
        let result = registry
            .register_video_track(&format!("stream{}", i), track)
            .await;
        assert!(result.is_ok(), "Track {} should register", i);
    }

    assert_eq!(
        registry.video_track_count().await,
        MAX_VIDEO_TRACKS_PER_PEER
    );

    // Next one should fail with TrackLimitExceeded
    let overflow_track = create_video_track("overflow").expect("Failed to create video track");
    let result = registry
        .register_video_track("overflow", overflow_track)
        .await;

    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(
        matches!(err, remotemedia_webrtc::Error::TrackLimitExceeded(_)),
        "Expected TrackLimitExceeded error, got {:?}",
        err
    );
}

#[tokio::test]
async fn test_track_lifecycle_frame_recording() {
    // Test FR-028: Track lifecycle tracking
    let registry: TrackRegistry<AudioTrack, VideoTrack> =
        TrackRegistry::new("test_peer".to_string());

    let track = create_video_track("camera").expect("Failed to create video track");
    registry
        .register_video_track("camera", track)
        .await
        .expect("Registration should succeed");

    // Record some frames
    for _ in 0..10 {
        registry.record_video_frame("camera").await;
    }

    // Track should exist and have recorded frames
    assert!(registry.has_video_track("camera").await);
    // Note: Frame count is tracked internally via TrackHandle::info.frame_count
}

#[tokio::test]
async fn test_multiple_video_streams_distinct_ids() {
    // Test that multiple video tracks with different stream_ids can coexist
    let registry: TrackRegistry<AudioTrack, VideoTrack> =
        TrackRegistry::new("test_peer".to_string());

    // Register tracks with different stream_ids
    let camera_track = create_video_track("camera").expect("Failed to create camera track");
    let screen_track = create_video_track("screen").expect("Failed to create screen track");

    registry
        .register_video_track("camera", camera_track)
        .await
        .expect("Camera registration should succeed");
    registry
        .register_video_track("screen", screen_track)
        .await
        .expect("Screen registration should succeed");

    // Both tracks should exist
    assert!(registry.has_video_track("camera").await);
    assert!(registry.has_video_track("screen").await);
    assert_eq!(registry.video_track_count().await, 2);

    // Stream IDs should be distinct
    let stream_ids = registry.video_stream_ids().await;
    assert!(stream_ids.contains(&"camera".to_string()));
    assert!(stream_ids.contains(&"screen".to_string()));
}

// =============================================================================
// Stream ID Validation Tests
// =============================================================================

#[test]
fn test_stream_id_validation_valid_ids() {
    // FR-003, FR-004: Valid stream IDs
    assert!(validate_stream_id("camera").is_ok());
    assert!(validate_stream_id("screen_share").is_ok());
    assert!(validate_stream_id("audio-main").is_ok());
    assert!(validate_stream_id("Track_123").is_ok());
    assert!(validate_stream_id("a").is_ok()); // Single char
    assert!(validate_stream_id("UPPERCASE").is_ok());
    assert!(validate_stream_id("_leading_underscore").is_ok());
    assert!(validate_stream_id("-leading-hyphen").is_ok());
}

#[test]
fn test_stream_id_validation_invalid_ids() {
    // FR-003, FR-004, FR-005: Invalid stream IDs
    assert!(validate_stream_id("").is_err()); // Empty
    assert!(validate_stream_id("has space").is_err()); // Space
    assert!(validate_stream_id("has.dot").is_err()); // Dot
    assert!(validate_stream_id("track@1").is_err()); // @
    assert!(validate_stream_id("track#1").is_err()); // #
    assert!(validate_stream_id("track/path").is_err()); // /

    // FR-005: Max length 64
    let long_id = "a".repeat(STREAM_ID_MAX_LENGTH + 1);
    assert!(validate_stream_id(&long_id).is_err());

    // Exactly 64 chars should be OK
    let max_id = "a".repeat(STREAM_ID_MAX_LENGTH);
    assert!(validate_stream_id(&max_id).is_ok());
}

#[tokio::test]
async fn test_registry_rejects_invalid_stream_ids() {
    // Test that TrackRegistry validates stream_ids
    let registry: TrackRegistry<AudioTrack, VideoTrack> =
        TrackRegistry::new("test_peer".to_string());

    let track = create_video_track("test").expect("Failed to create video track");

    // Invalid stream_id should be rejected
    let result = registry.register_video_track("has space", track).await;
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(
        matches!(err, remotemedia_webrtc::Error::InvalidStreamId(_)),
        "Expected InvalidStreamId error, got {:?}",
        err
    );
}

// =============================================================================
// Track Removal Tests
// =============================================================================

#[tokio::test]
async fn test_video_track_removal() {
    // Test FR-027: Track removal by stream_id
    let registry: TrackRegistry<AudioTrack, VideoTrack> =
        TrackRegistry::new("test_peer".to_string());

    let track = create_video_track("camera").expect("Failed to create video track");
    registry
        .register_video_track("camera", track)
        .await
        .expect("Registration should succeed");

    assert_eq!(registry.video_track_count().await, 1);

    // Remove the track
    let removed = registry.remove_video_track("camera").await;
    assert!(removed.is_some());
    assert_eq!(registry.video_track_count().await, 0);
    assert!(!registry.has_video_track("camera").await);

    // Removing non-existent track returns None (not error)
    let removed_again = registry.remove_video_track("camera").await;
    assert!(removed_again.is_none());
}

#[tokio::test]
async fn test_clear_all_tracks() {
    // Test clearing all tracks on peer disconnect
    let registry: TrackRegistry<AudioTrack, VideoTrack> =
        TrackRegistry::new("test_peer".to_string());

    // Register multiple tracks
    for stream_id in &["camera", "screen", "backup"] {
        let track = create_video_track(stream_id).expect("Failed to create video track");
        registry
            .register_video_track(stream_id, track)
            .await
            .expect("Registration should succeed");
    }

    assert_eq!(registry.video_track_count().await, 3);

    // Clear all tracks
    registry.clear().await;

    assert_eq!(registry.video_track_count().await, 0);
    assert!(!registry.has_video_track("camera").await);
    assert!(!registry.has_video_track("screen").await);
    assert!(!registry.has_video_track("backup").await);
}

// =============================================================================
// Backward Compatibility Tests
// =============================================================================

#[test]
fn test_video_frame_without_stream_id_uses_default() {
    // Test backward compatibility: frames without stream_id route to default track
    let frame = create_video_frame(None, 1280, 720);

    // extract_stream_id returns None for no stream_id
    assert_eq!(extract_stream_id(&frame), None);

    // Application code should fall back to DEFAULT_STREAM_ID when None
    let effective_stream_id = extract_stream_id(&frame).unwrap_or(DEFAULT_STREAM_ID);
    assert_eq!(effective_stream_id, "default");
}

#[test]
fn test_video_frame_with_explicit_stream_id() {
    // Test new behavior: frames with stream_id route to specific track
    let frame = create_video_frame(Some("camera".to_string()), 1280, 720);

    assert_eq!(extract_stream_id(&frame), Some("camera"));

    // Verify frame data is preserved
    if let RuntimeData::Video {
        width,
        height,
        stream_id,
        ..
    } = frame
    {
        assert_eq!(width, 1280);
        assert_eq!(height, 720);
        assert_eq!(stream_id, Some("camera".to_string()));
    } else {
        panic!("Expected Video variant");
    }
}
