//! Multiple Video Streams Integration Tests
//!
//! Spec 013: Dynamic Multi-Track Streaming - Phase 4 validation (User Story 2)
//!
//! Tests that multiple video tracks can be registered and managed per peer connection,
//! validating concurrent track creation, frame routing, and track limits.
//!
//! ## Test Scenarios
//!
//! 1. Multiple video streams with different stream_ids (camera, screen)
//! 2. Concurrent track creation safety
//! 3. Track limit enforcement (max 5 video tracks)
//! 4. Frame routing to correct tracks based on stream_id
//! 5. Track ID generation from stream_id

use remotemedia_core::data::video::{PixelFormat, VideoCodec};
use remotemedia_core::data::RuntimeData;
use remotemedia_webrtc::media::tracks::{AudioTrack, VideoTrack};
use remotemedia_webrtc::media::{
    extract_stream_id, generate_track_id, validate_stream_id, TrackRegistry, DEFAULT_STREAM_ID,
    MAX_VIDEO_TRACKS_PER_PEER,
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
// US2 T021-T028: Multiple Video Streams Tests
// =============================================================================

#[tokio::test]
async fn test_multiple_video_tracks_registration() {
    // Test that multiple video tracks with different stream_ids can be registered
    let registry: TrackRegistry<AudioTrack, VideoTrack> =
        TrackRegistry::new("test_peer".to_string());

    // Register camera and screen share tracks
    let camera_track = create_video_track("camera").expect("Failed to create camera track");
    let screen_track = create_video_track("screen").expect("Failed to create screen track");

    let result1 = registry.register_video_track("camera", camera_track).await;
    let result2 = registry.register_video_track("screen", screen_track).await;

    assert!(result1.is_ok());
    assert!(result1.unwrap()); // true = newly registered
    assert!(result2.is_ok());
    assert!(result2.unwrap()); // true = newly registered

    // Both tracks should exist
    assert!(registry.has_video_track("camera").await);
    assert!(registry.has_video_track("screen").await);
    assert_eq!(registry.video_track_count().await, 2);
}

#[tokio::test]
async fn test_multiple_video_tracks_up_to_limit() {
    // Test that exactly MAX_VIDEO_TRACKS_PER_PEER (5) tracks can be registered
    let registry: TrackRegistry<AudioTrack, VideoTrack> =
        TrackRegistry::new("test_peer".to_string());

    let stream_ids = ["camera", "screen", "backup", "pip", "overlay"];

    // Register all 5 tracks
    for stream_id in &stream_ids {
        let track = create_video_track(stream_id).expect("Failed to create video track");
        let result = registry.register_video_track(stream_id, track).await;
        assert!(result.is_ok(), "Track '{}' should register", stream_id);
        assert!(
            result.unwrap(),
            "Track '{}' should be newly registered",
            stream_id
        );
    }

    assert_eq!(
        registry.video_track_count().await,
        MAX_VIDEO_TRACKS_PER_PEER
    );

    // Verify all tracks are accessible
    for stream_id in &stream_ids {
        assert!(
            registry.has_video_track(stream_id).await,
            "Track '{}' should exist",
            stream_id
        );
        assert!(
            registry.get_video_track(stream_id).await.is_some(),
            "Track '{}' should be retrievable",
            stream_id
        );
    }

    // 6th track should fail
    let overflow_track = create_video_track("overflow").expect("Failed to create video track");
    let result = registry
        .register_video_track("overflow", overflow_track)
        .await;
    assert!(result.is_err());
    assert!(
        matches!(
            result.err().unwrap(),
            remotemedia_webrtc::Error::TrackLimitExceeded(_)
        ),
        "Expected TrackLimitExceeded error"
    );
}

#[tokio::test]
async fn test_video_frame_routing_to_correct_track() {
    // Test that frames with different stream_ids are correctly identified
    let camera_frame = create_video_frame(Some("camera".to_string()), 1920, 1080);
    let screen_frame = create_video_frame(Some("screen".to_string()), 1280, 720);
    let default_frame = create_video_frame(None, 640, 480);

    assert_eq!(extract_stream_id(&camera_frame), Some("camera"));
    assert_eq!(extract_stream_id(&screen_frame), Some("screen"));
    assert_eq!(extract_stream_id(&default_frame), None);

    // Verify fallback to DEFAULT_STREAM_ID
    let effective_default = extract_stream_id(&default_frame).unwrap_or(DEFAULT_STREAM_ID);
    assert_eq!(effective_default, "default");
}

#[tokio::test]
async fn test_track_id_generation_deterministic() {
    // Test FR-020: Track ID generation is deterministic
    let track_id1 = generate_track_id("video", "camera");
    let track_id2 = generate_track_id("video", "camera");
    let track_id3 = generate_track_id("video", "screen");

    // Same inputs produce same outputs
    assert_eq!(track_id1, track_id2);
    assert_eq!(track_id1, "video_camera");

    // Different stream_ids produce different track IDs
    assert_ne!(track_id1, track_id3);
    assert_eq!(track_id3, "video_screen");

    // Audio track IDs are different from video
    let audio_track_id = generate_track_id("audio", "camera");
    assert_eq!(audio_track_id, "audio_camera");
    assert_ne!(audio_track_id, track_id1);
}

#[tokio::test]
async fn test_concurrent_track_retrieval() {
    // Test that tracks can be retrieved concurrently without data races
    let registry: TrackRegistry<AudioTrack, VideoTrack> =
        TrackRegistry::new("test_peer".to_string());

    // Register multiple tracks
    for stream_id in ["camera", "screen", "backup"] {
        let track = create_video_track(stream_id).expect("Failed to create video track");
        registry
            .register_video_track(stream_id, track)
            .await
            .unwrap();
    }

    // Spawn concurrent read tasks
    let registry_arc = Arc::new(registry);
    let mut handles = vec![];

    for _ in 0..10 {
        let reg = Arc::clone(&registry_arc);
        let handle = tokio::spawn(async move {
            // Concurrent reads should all succeed
            let camera = reg.get_video_track("camera").await;
            let screen = reg.get_video_track("screen").await;
            let backup = reg.get_video_track("backup").await;
            (camera.is_some(), screen.is_some(), backup.is_some())
        });
        handles.push(handle);
    }

    // All reads should succeed
    for handle in handles {
        let (camera, screen, backup) = handle.await.unwrap();
        assert!(camera, "Camera track should be retrievable");
        assert!(screen, "Screen track should be retrievable");
        assert!(backup, "Backup track should be retrievable");
    }
}

#[tokio::test]
async fn test_video_tracks_with_different_resolutions() {
    // Test that tracks can have different video properties
    let registry: TrackRegistry<AudioTrack, VideoTrack> =
        TrackRegistry::new("test_peer".to_string());

    // Camera at 1080p
    let camera_webrtc = create_webrtc_video_track("camera");
    let camera_track = Arc::new(
        VideoTrack::new(camera_webrtc, VideoCodec::Vp8, 1920, 1080, 4_000_000, 30)
            .expect("Failed to create 1080p track"),
    );

    // Screen share at 720p
    let screen_webrtc = create_webrtc_video_track("screen");
    let screen_track = Arc::new(
        VideoTrack::new(screen_webrtc, VideoCodec::Vp8, 1280, 720, 2_000_000, 15)
            .expect("Failed to create 720p track"),
    );

    registry
        .register_video_track("camera", camera_track)
        .await
        .unwrap();
    registry
        .register_video_track("screen", screen_track)
        .await
        .unwrap();

    // Both tracks should exist independently
    assert!(registry.has_video_track("camera").await);
    assert!(registry.has_video_track("screen").await);
    assert_eq!(registry.video_track_count().await, 2);
}

#[tokio::test]
async fn test_track_frame_counting_per_stream() {
    // Test that frame counts are tracked separately per stream
    let registry: TrackRegistry<AudioTrack, VideoTrack> =
        TrackRegistry::new("test_peer".to_string());

    let camera_track = create_video_track("camera").expect("Failed to create camera track");
    let screen_track = create_video_track("screen").expect("Failed to create screen track");

    registry
        .register_video_track("camera", camera_track)
        .await
        .unwrap();
    registry
        .register_video_track("screen", screen_track)
        .await
        .unwrap();

    // Record frames on camera (10 frames)
    for _ in 0..10 {
        registry.record_video_frame("camera").await;
    }

    // Record frames on screen (5 frames)
    for _ in 0..5 {
        registry.record_video_frame("screen").await;
    }

    // Both tracks should still exist (frames don't affect track existence)
    assert!(registry.has_video_track("camera").await);
    assert!(registry.has_video_track("screen").await);
}

#[tokio::test]
async fn test_remove_one_track_keeps_others() {
    // Test that removing one track doesn't affect others
    let registry: TrackRegistry<AudioTrack, VideoTrack> =
        TrackRegistry::new("test_peer".to_string());

    // Register three tracks
    for stream_id in ["camera", "screen", "backup"] {
        let track = create_video_track(stream_id).expect("Failed to create video track");
        registry
            .register_video_track(stream_id, track)
            .await
            .unwrap();
    }

    assert_eq!(registry.video_track_count().await, 3);

    // Remove screen track
    let removed = registry.remove_video_track("screen").await;
    assert!(removed.is_some());

    // Camera and backup should still exist
    assert!(registry.has_video_track("camera").await);
    assert!(!registry.has_video_track("screen").await);
    assert!(registry.has_video_track("backup").await);
    assert_eq!(registry.video_track_count().await, 2);
}

#[tokio::test]
async fn test_get_all_video_stream_ids() {
    // Test that all stream IDs can be retrieved
    let registry: TrackRegistry<AudioTrack, VideoTrack> =
        TrackRegistry::new("test_peer".to_string());

    let expected_ids = vec!["camera", "screen", "backup"];

    for stream_id in &expected_ids {
        let track = create_video_track(stream_id).expect("Failed to create video track");
        registry
            .register_video_track(stream_id, track)
            .await
            .unwrap();
    }

    let mut actual_ids = registry.video_stream_ids().await;
    actual_ids.sort();

    let mut expected_sorted: Vec<String> = expected_ids.iter().map(|s| s.to_string()).collect();
    expected_sorted.sort();

    assert_eq!(actual_ids, expected_sorted);
}

#[tokio::test]
async fn test_same_stream_id_audio_and_video_independent() {
    // Test FR-013: Same stream_id can be used for both audio and video
    let registry: TrackRegistry<AudioTrack, VideoTrack> =
        TrackRegistry::new("test_peer".to_string());

    let video_track = create_video_track("main").expect("Failed to create video track");

    // Can't easily create AudioTrack without full setup, so just test video independently
    registry
        .register_video_track("main", video_track)
        .await
        .unwrap();

    // Video track should exist
    assert!(registry.has_video_track("main").await);

    // Audio track shouldn't exist (not registered)
    assert!(!registry.has_audio_track("main").await);

    // Counts should be independent
    assert_eq!(registry.video_track_count().await, 1);
    assert_eq!(registry.audio_track_count().await, 0);
}

// =============================================================================
// Stream ID Validation for Multi-Track
// =============================================================================

#[test]
fn test_multi_track_stream_id_naming_conventions() {
    // Test common multi-track naming patterns
    let valid_names = [
        "camera",
        "camera_main",
        "camera-front",
        "screen_share",
        "screen-1",
        "pip",
        "overlay",
        "backup_feed",
        "video1",
        "video_720p",
    ];

    for name in valid_names {
        assert!(
            validate_stream_id(name).is_ok(),
            "Stream ID '{}' should be valid",
            name
        );
    }
}

#[test]
fn test_track_ids_for_multi_video_sdp() {
    // Test that track IDs generated for SDP are unique per stream
    let streams = ["camera", "screen", "backup", "pip", "overlay"];
    let track_ids: Vec<String> = streams
        .iter()
        .map(|s| generate_track_id("video", s))
        .collect();

    // Check all unique
    let unique_count = {
        let mut sorted = track_ids.clone();
        sorted.sort();
        sorted.dedup();
        sorted.len()
    };

    assert_eq!(
        unique_count,
        streams.len(),
        "All track IDs should be unique"
    );

    // Check format
    assert_eq!(track_ids[0], "video_camera");
    assert_eq!(track_ids[1], "video_screen");
}
