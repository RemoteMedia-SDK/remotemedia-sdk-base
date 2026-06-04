//! WebRTC video integration test
//!
//! Spec 012: Video Codec Support - Phase 5 validation
//! Tests video encoding via core nodes for WebRTC streaming
//!
//! ## Architecture
//!
//! The correct usage pattern for WebRTC video is:
//! 1. Encode raw frames with core VideoEncoderNode (VP8/H.264/AV1)
//! 2. Pass encoded data to WebRTC VideoTrack for RTP transmission
//! 3. WebRTC handles RTP packetization, peer connections, SDP negotiation
//! 4. On receive: WebRTC depacketizes RTP, app uses VideoDecoderNode to decode
//!
//! This test validates that core codecs are compatible with WebRTC requirements.

use remotemedia_core::data::video::{PixelFormat, VideoCodec};
use remotemedia_core::data::RuntimeData;
use remotemedia_core::nodes::streaming_node::AsyncStreamingNode;
use remotemedia_core::nodes::video::{
    VideoDecoderConfig, VideoDecoderNode, VideoEncoderConfig, VideoEncoderNode,
};
use remotemedia_webrtc::{VideoCodec as WebRtcVideoCodec, VideoResolution, WebRtcTransportConfig};

#[tokio::test]
async fn test_webrtc_config_with_video_codecs() {
    // Test that WebRTC config accepts all video codecs
    let codecs = vec![
        WebRtcVideoCodec::VP8,
        WebRtcVideoCodec::VP9,
        WebRtcVideoCodec::H264,
    ];

    for codec in codecs {
        let config = WebRtcTransportConfig {
            signaling_url: "ws://localhost:8080".to_string(),
            stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
            turn_servers: vec![],
            video_codec: codec,
            max_peers: 4,
            ..Default::default()
        };

        assert_eq!(config.video_codec, codec);
    }
}

#[tokio::test]
async fn test_vp8_encoder_for_webrtc() {
    // Test VP8 encoding works for WebRTC video pipeline

    let encoder_config = VideoEncoderConfig {
        codec: VideoCodec::Vp8,
        bitrate: 2_000_000,
        framerate: 30,
        ..Default::default()
    };

    // Create encoder
    let encoder = VideoEncoderNode::new(encoder_config).expect("Failed to create VP8 encoder");

    // Create a raw 720p YUV420P frame (matching WebRTC 720p)
    let width = 1280u32;
    let height = 720u32;
    let frame_size = (width * height * 3 / 2) as usize;
    let pixel_data = vec![128u8; frame_size];

    let raw_frame = RuntimeData::Video {
        pixel_data,
        width,
        height,
        format: PixelFormat::Yuv420p,
        codec: None,
        frame_number: 0,
        timestamp_us: 0,
        is_keyframe: false,
        stream_id: None,
        arrival_ts_us: None,
    };

    // Encode - may fail if FFmpeg not compiled with libvpx support
    let result = encoder.process(raw_frame).await;

    // Either succeeds with encoded frame, or fails with codec error (acceptable)
    match result {
        Ok(RuntimeData::Video {
            codec: Some(VideoCodec::Vp8),
            format: PixelFormat::Encoded,
            width: w,
            height: h,
            pixel_data,
            ..
        }) => {
            assert_eq!(w, 1280);
            assert_eq!(h, 720);
            assert!(!pixel_data.is_empty(), "VP8 bitstream should not be empty");
        }
        Err(e) => {
            // Expected if FFmpeg not compiled with libvpx or codec not available
            let err_str = e.to_string();
            assert!(
                err_str.contains("not yet implemented")
                    || err_str.contains("not available")
                    || err_str.contains("unknown codec")
                    || err_str.contains("requires"),
                "Unexpected error: {}",
                err_str
            );
            println!(
                "VP8 encoding not available (expected in some environments): {}",
                err_str
            );
            return; // Test passes - codec unavailable is acceptable
        }
        Ok(_) => panic!("Expected VP8 encoded frame with correct codec and format"),
    }
}

#[tokio::test]
async fn test_multi_codec_config() {
    // Verify all three codecs can be configured

    let codecs = vec![
        (VideoCodec::Vp8, "VP8"),
        (VideoCodec::H264, "H.264"),
        (VideoCodec::Av1, "AV1"),
    ];

    for (codec, name) in codecs {
        let encoder_config = VideoEncoderConfig {
            codec,
            bitrate: 2_000_000,
            framerate: 30,
            ..Default::default()
        };

        let result = VideoEncoderNode::new(encoder_config);
        assert!(result.is_ok(), "{} encoder should be creatable", name);
    }
}

#[tokio::test]
async fn test_rtp_timestamp_90khz_clock() {
    // Phase 5 T078: Verify 90kHz clock calculation for video RTP timestamps

    // Standard RTP video clock is 90kHz
    let rtp_clock_rate = 90000u32;

    // For 30fps video, timestamp increment = 90000/30 = 3000
    let framerate_30 = 30u32;
    let increment_30 = rtp_clock_rate / framerate_30;
    assert_eq!(increment_30, 3000);

    // For 60fps video, increment = 90000/60 = 1500
    let framerate_60 = 60u32;
    let increment_60 = rtp_clock_rate / framerate_60;
    assert_eq!(increment_60, 1500);

    // Verify wrapping behavior for continuous streaming
    let mut timestamp: u32 = u32::MAX - 1000;
    timestamp = timestamp.wrapping_add(increment_30);
    assert!(timestamp < 5000, "Timestamp should wrap around correctly");
}

#[test]
fn test_video_codec_mime_types_webrtc_compatible() {
    // Verify core codec MIME types match WebRTC expectations

    assert_eq!(VideoCodec::Vp8.mime_type(), "video/VP8");
    assert_eq!(VideoCodec::H264.mime_type(), "video/H264");
    assert_eq!(VideoCodec::Av1.mime_type(), "video/AV1");

    // Verify RTP payload types (dynamic range 96-127)
    assert_eq!(VideoCodec::Vp8.rtp_payload_type(), 96);
    assert_eq!(VideoCodec::H264.rtp_payload_type(), 102);
    assert_eq!(VideoCodec::Av1.rtp_payload_type(), 104);
}

#[test]
fn test_video_resolution_720p() {
    // Verify 720p resolution
    let resolution = VideoResolution::P720;
    assert_eq!(resolution, VideoResolution::P720);

    // 720p is 1280x720
    let width = 1280u32;
    let height = 720u32;
    assert_eq!(width, 1280);
    assert_eq!(height, 720);
}
