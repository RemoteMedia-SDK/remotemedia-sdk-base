//! Unit tests for VideoEncoderNode
//!
//! Spec 012: Video Codec Support - T039

#[cfg(test)]
mod tests {
    use crate::data::video::{PixelFormat, VideoCodec};
    use crate::data::RuntimeData;
    use crate::nodes::streaming_node::AsyncStreamingNode;
    use crate::nodes::video::{VideoEncoderConfig, VideoEncoderNode};

    #[tokio::test]
    async fn test_encoder_node_creation() {
        // Test creating encoder with default config
        let config = VideoEncoderConfig::default();
        let result = VideoEncoderNode::new(config);

        // Should succeed (or fail gracefully if FFmpeg not available)
        match result {
            Ok(_) => {
                // Encoder created successfully
            }
            Err(e) => {
                // Expected if FFmpeg not integrated yet or not installed
                assert!(
                    e.to_string().contains("not yet implemented")
                        || e.to_string().contains("not available")
                );
            }
        }
    }

    #[tokio::test]
    async fn test_encoder_validates_input() {
        let config = VideoEncoderConfig {
            codec: VideoCodec::Vp8,
            bitrate: 2_000_000,
            framerate: 30,
            ..Default::default()
        };

        // Try to create encoder (may fail if FFmpeg not available)
        if let Ok(encoder) = VideoEncoderNode::new(config) {
            // Test 1: Reject already-encoded frames
            let encoded_frame = RuntimeData::Video {
                pixel_data: vec![0u8; 1000],
                width: 1280,
                height: 720,
                format: PixelFormat::Encoded,
                codec: Some(VideoCodec::Vp8), // Already encoded
                frame_number: 0,
                timestamp_us: 0,
                is_keyframe: true,
                stream_id: None,
                arrival_ts_us: None,
            };

            let result = encoder.process(encoded_frame).await;
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("already-encoded"));

            // Test 2: Reject non-video data
            let text_data = RuntimeData::Text("hello".to_string());
            let result = encoder.process(text_data).await;
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("Expected RuntimeData::Video"));
        }
    }

    #[tokio::test]
    async fn test_encoder_accepts_raw_frames() {
        let config = VideoEncoderConfig {
            codec: VideoCodec::Vp8,
            bitrate: 1_000_000,
            framerate: 30,
            ..Default::default()
        };

        if let Ok(encoder) = VideoEncoderNode::new(config) {
            // Create a raw 720p YUV420P frame
            let width = 1280u32;
            let height = 720u32;
            let frame_size = (width * height * 3 / 2) as usize; // YUV420P
            let pixel_data = vec![128u8; frame_size]; // Gray frame

            let raw_frame = RuntimeData::Video {
                pixel_data,
                width,
                height,
                format: PixelFormat::Yuv420p,
                codec: None, // Raw frame
                frame_number: 0,
                timestamp_us: 0,
                is_keyframe: false,
                stream_id: None,
                arrival_ts_us: None,
            };

            // Should accept raw frame (but encoding may fail if FFmpeg not available)
            let result = encoder.process(raw_frame).await;

            // Either succeeds with encoded frame, or fails with codec error
            match result {
                Ok(RuntimeData::Video { codec: Some(_), .. }) => {
                    // Encoding worked! Check it returned encoded frame
                }
                Err(e) => {
                    // Expected if FFmpeg not yet integrated or codec not available
                    let err_str = e.to_string();
                    assert!(
                        err_str.contains("not yet implemented")
                            || err_str.contains("not available")
                            || err_str.contains("unknown codec")
                            || err_str.contains("requires"),
                        "Unexpected error: {}",
                        err_str
                    );
                }
                Ok(_) => panic!("Expected encoded video frame with codec set"),
            }
        }
    }

    #[test]
    fn test_encoder_config_defaults() {
        let config = VideoEncoderConfig::default();
        assert_eq!(config.codec, VideoCodec::Vp8);
        assert_eq!(config.bitrate, 1_000_000);
        assert_eq!(config.framerate, 30);
        assert_eq!(config.keyframe_interval, 60);
        assert_eq!(config.quality_preset, "medium");
        assert_eq!(config.hardware_accel, true);
        assert_eq!(config.threads, 0);
    }

    #[tokio::test]
    async fn test_h264_encoder() {
        // Test H.264 encoding
        let config = VideoEncoderConfig {
            codec: VideoCodec::H264,
            bitrate: 2_000_000,
            framerate: 30,
            ..Default::default()
        };

        if let Ok(encoder) = VideoEncoderNode::new(config) {
            // Create a raw 720p YUV420P frame
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

            // Encode with H.264
            let result = encoder.process(raw_frame).await;

            match result {
                Ok(RuntimeData::Video {
                    codec: Some(VideoCodec::H264),
                    format: PixelFormat::Encoded,
                    ..
                }) => {
                    // Successfully encoded to H.264
                }
                Err(e) => {
                    // May fail if libx264 not available. Three observed
                    // error paths: codec lookup fails ("not available"),
                    // codec context init fails ("Failed to create encoder"),
                    // or encode-time call fails (`encoder.rs:219`
                    // "Encoding failed: ..." — the path vendored static
                    // FFmpeg without --enable-libx264 takes).
                    let s = e.to_string();
                    assert!(
                        s.contains("not available")
                            || s.contains("Failed to create encoder")
                            || s.contains("Encoding failed"),
                        "Unexpected encoder error: {s}"
                    );
                }
                Ok(_) => panic!("Expected H.264 encoded frame"),
            }
        }
    }

    #[tokio::test]
    async fn test_encoder_accepts_rgb24_frames() {
        // Regression: the avatar/Live2D pipeline emits RGB24 frames; an
        // earlier version of FFmpegEncoder::encode() blindly memcpy'd
        // RGB bytes into the YUV planes (because the length check used
        // `>=` instead of `==` and the `format` field was ignored),
        // producing green-tinted, horizontally banded output over WebRTC.
        let config = VideoEncoderConfig {
            codec: VideoCodec::Vp8,
            bitrate: 1_000_000,
            framerate: 30,
            ..Default::default()
        };

        if let Ok(encoder) = VideoEncoderNode::new(config) {
            let width = 64u32;
            let height = 64u32;
            // Solid red 64x64 RGB24 frame (3 bytes/pixel, packed RGB).
            let mut pixel_data = Vec::with_capacity((width * height * 3) as usize);
            for _ in 0..(width * height) {
                pixel_data.extend_from_slice(&[255, 0, 0]);
            }

            let raw_frame = RuntimeData::Video {
                pixel_data,
                width,
                height,
                format: PixelFormat::Rgb24,
                codec: None,
                frame_number: 0,
                timestamp_us: 0,
                is_keyframe: false,
                stream_id: None,
                arrival_ts_us: None,
            };

            let result = encoder.process(raw_frame).await;
            match result {
                Ok(RuntimeData::Video {
                    codec: Some(VideoCodec::Vp8),
                    format: PixelFormat::Encoded,
                    ..
                }) => {}
                Err(e) => {
                    // CI builds vendored libvpx via scripts/install-libvpx.sh
                    // and passes --enable-libvpx to FFmpeg, so this branch
                    // is expected to be unreachable in CI. Local builds
                    // without `./scripts/install-libvpx.sh` will hit
                    // "Encoding failed" — keep that accepted.
                    let err_str = e.to_string();
                    assert!(
                        err_str.contains("not available")
                            || err_str.contains("Failed to create encoder")
                            || err_str.contains("Encoding failed"),
                        "Unexpected error: {}",
                        err_str
                    );
                }
                Ok(_) => panic!("Expected VP8-encoded frame from RGB24 input"),
            }
        }
    }

    #[tokio::test]
    async fn test_encoder_rejects_wrong_buffer_size() {
        // Defense in depth: silent corruption (the original bug) was
        // possible because the length check was a permissive `>=`.
        // We now require an exact match per declared format.
        let config = VideoEncoderConfig::default();
        if let Ok(encoder) = VideoEncoderNode::new(config) {
            let width = 64u32;
            let height = 64u32;
            // RGB24 expects W*H*3 = 12_288 bytes. Send YUV-sized buffer instead.
            let undersized = vec![0u8; (width * height * 3 / 2) as usize];
            let bad_frame = RuntimeData::Video {
                pixel_data: undersized,
                width,
                height,
                format: PixelFormat::Rgb24,
                codec: None,
                frame_number: 0,
                timestamp_us: 0,
                is_keyframe: false,
                stream_id: None,
                arrival_ts_us: None,
            };

            let err = encoder.process(bad_frame).await.unwrap_err();
            assert!(
                err.to_string().contains("buffer size mismatch"),
                "expected buffer-size-mismatch error, got: {}",
                err
            );
        }
    }

    #[tokio::test]
    async fn test_av1_encoder() {
        // Test AV1 encoding
        let config = VideoEncoderConfig {
            codec: VideoCodec::Av1,
            bitrate: 1_500_000,
            framerate: 30,
            ..Default::default()
        };

        if let Ok(encoder) = VideoEncoderNode::new(config) {
            // Create a raw 720p YUV420P frame
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

            // Encode with AV1
            let result = encoder.process(raw_frame).await;

            match result {
                Ok(RuntimeData::Video {
                    codec: Some(VideoCodec::Av1),
                    format: PixelFormat::Encoded,
                    ..
                }) => {
                    // Successfully encoded to AV1
                }
                Err(e) => {
                    // May fail if libaom-av1 not available — same three
                    // error paths as test_h264_encoder.
                    let s = e.to_string();
                    assert!(
                        s.contains("not available")
                            || s.contains("Failed to create encoder")
                            || s.contains("Encoding failed"),
                        "Unexpected encoder error: {s}"
                    );
                }
                Ok(_) => panic!("Expected AV1 encoded frame"),
            }
        }
    }
}
