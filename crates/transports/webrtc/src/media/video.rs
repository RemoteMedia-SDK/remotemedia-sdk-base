//! Video codec support (VP8/H.264/AV1)
//!
//! WebRTC video integration with core video encoder/decoder nodes.
//! Supports VP8, H.264 (AVC), and AV1 codecs via ac-ffmpeg.
//!
//! ## Architecture
//!
//! For actual video encoding/decoding, use `remotemedia_core::nodes::video`:
//! - `VideoEncoderNode`: Encode raw frames to VP8/H.264/AV1 bitstreams
//! - `VideoDecoderNode`: Decode bitstreams to raw frames
//!
//! This module provides WebRTC-specific video frame types and RTP integration.
//! The webrtc-rs library handles RTP packetization, codec negotiation in SDP,
//! and built-in VP8/H.264 MediaEngine registration via `register_default_codecs()`.

// Phase 4/5 video codec infrastructure
#![allow(dead_code)]

use crate::{Error, Result};

/// Video frame format
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoFormat {
    /// I420 (YUV 4:2:0 planar)
    I420,
    /// NV12 (YUV 4:2:0 semi-planar)
    NV12,
    /// RGB24
    RGB24,
}

/// Video frame
#[derive(Debug, Clone)]
pub struct VideoFrame {
    /// Frame width in pixels
    pub width: u32,
    /// Frame height in pixels
    pub height: u32,
    /// Pixel format
    pub format: VideoFormat,
    /// Frame data (format-dependent layout)
    pub data: Vec<u8>,
    /// Timestamp in microseconds
    pub timestamp_us: u64,
    /// Whether this is a keyframe
    pub is_keyframe: bool,
}

/// Video encoder configuration
#[derive(Debug, Clone)]
pub struct VideoEncoderConfig {
    /// Frame width in pixels
    pub width: u32,
    /// Frame height in pixels
    pub height: u32,
    /// Target framerate (frames per second)
    pub framerate: u32,
    /// Target bitrate in bits per second
    pub bitrate: u32,
    /// Keyframe interval (frames)
    pub keyframe_interval: u32,
}

impl Default for VideoEncoderConfig {
    fn default() -> Self {
        Self {
            width: 1280,
            height: 720,
            framerate: 30,
            bitrate: 2_000_000, // 2 Mbps
            keyframe_interval: 60,
        }
    }
}

/// Video encoder for WebRTC (VP8/H.264/AV1 support)
///
///## Integration with core
///
/// For production video encoding, use `remotemedia_core::nodes::video::VideoEncoderNode`
/// which provides full VP8/H.264/AV1 support via ac-ffmpeg.
///
/// This WebRTC-specific encoder provides the RTP framing layer. The actual codec
/// operations should be delegated to core nodes for consistency across
/// gRPC and WebRTC transports.
pub struct VideoEncoder {
    config: VideoEncoderConfig,
    frame_count: u64,
}

impl VideoEncoder {
    /// Create a new video encoder
    pub fn new(config: VideoEncoderConfig) -> Result<Self> {
        // Validate configuration
        if config.width == 0 || config.height == 0 {
            return Err(Error::InvalidConfig(
                "Video dimensions must be greater than 0".to_string(),
            ));
        }

        if config.framerate == 0 {
            return Err(Error::InvalidConfig(
                "Framerate must be greater than 0".to_string(),
            ));
        }

        if !config.width.is_multiple_of(2) || !config.height.is_multiple_of(2) {
            return Err(Error::InvalidConfig(
                "VP9 requires even dimensions for I420 format".to_string(),
            ));
        }

        Ok(Self {
            config,
            frame_count: 0,
        })
    }

    /// Encode a video frame to VP9 format
    ///
    /// # Arguments
    ///
    /// * `frame` - Input video frame (I420 format)
    ///
    /// # Returns
    ///
    /// Encoded VP9 packet as bytes (RTP payload)
    #[cfg(not(feature = "codecs"))]
    pub fn encode(&mut self, _frame: &VideoFrame) -> Result<Vec<u8>> {
        Err(Error::EncodingError(
            "VP9 encoding requires the 'codecs' feature flag".to_string(),
        ))
    }

    /// Encode a video frame to VP9 format
    ///
    /// # Arguments
    ///
    /// * `frame` - Input video frame (I420 format)
    ///
    /// # Returns
    ///
    /// Encoded VP9 packet as bytes (RTP payload)
    ///
    /// # Implementation Note
    ///
    /// This is a structural placeholder. Full VP9 encoding requires:
    /// 1. Native libvpx bindings (vpx-sys or direct FFI)
    /// 2. Image format conversion for I420
    /// 3. Keyframe management and RTP packetization
    /// 4. Rate control and bitrate management
    ///
    /// For production use, consider webrtc-rs built-in video codecs or
    /// integrate with a higher-level VP9 encoder library.
    #[cfg(feature = "codecs")]
    pub fn encode(&mut self, frame: &VideoFrame) -> Result<Vec<u8>> {
        if frame.format != VideoFormat::I420 {
            return Err(Error::EncodingError(
                "VP9 encoder only supports I420 format".to_string(),
            ));
        }

        if frame.width != self.config.width || frame.height != self.config.height {
            return Err(Error::EncodingError(format!(
                "Frame dimensions ({}x{}) don't match encoder config ({}x{})",
                frame.width, frame.height, self.config.width, self.config.height
            )));
        }

        self.frame_count += 1;

        // TODO: Implement native VP9 encoding via vpx-sys FFI
        // Required steps:
        // 1. Initialize vpx_codec_ctx_t with VP9 encoder interface
        // 2. Configure bitrate, framerate, keyframe interval
        // 3. Wrap frame.data as vpx_image_t (I420 planes: Y, U, V)
        // 4. Call vpx_codec_encode() with appropriate flags
        // 5. Retrieve encoded packets via vpx_codec_get_cx_data()
        // 6. Return packet data as Vec<u8>

        Err(Error::EncodingError(
            "VP9 native encoding not yet implemented - requires vpx-sys FFI integration"
                .to_string(),
        ))
    }

    /// Get the current frame count
    pub fn frame_count(&self) -> u64 {
        self.frame_count
    }

    /// Check if next frame should be a keyframe
    pub fn should_force_keyframe(&self) -> bool {
        self.frame_count
            .is_multiple_of(self.config.keyframe_interval as u64)
    }
}

/// Video decoder for WebRTC (VP8/H.264/AV1 support)
///
/// ## Integration with core
///
/// For production video decoding, use `remotemedia_core::nodes::video::VideoDecoderNode`
/// which provides full VP8/H.264/AV1 support via ac-ffmpeg.
///
/// This WebRTC-specific decoder handles RTP depacketization. The actual codec
/// operations should be delegated to core nodes for consistency across
/// gRPC and WebRTC transports.
pub struct VideoDecoder {
    config: VideoEncoderConfig,
    frame_count: u64,
}

impl VideoDecoder {
    /// Create a new video decoder
    pub fn new(config: VideoEncoderConfig) -> Result<Self> {
        // Validate configuration
        if config.width == 0 || config.height == 0 {
            return Err(Error::InvalidConfig(
                "Video dimensions must be greater than 0".to_string(),
            ));
        }

        if !config.width.is_multiple_of(2) || !config.height.is_multiple_of(2) {
            return Err(Error::InvalidConfig(
                "VP9 requires even dimensions for I420 format".to_string(),
            ));
        }

        Ok(Self {
            config,
            frame_count: 0,
        })
    }

    /// Decode VP9 packet to video frame
    ///
    /// # Arguments
    ///
    /// * `payload` - Encoded VP9 packet (RTP payload)
    ///
    /// # Returns
    ///
    /// Decoded video frame (I420 format)
    #[cfg(not(feature = "codecs"))]
    pub fn decode(&mut self, _payload: &[u8]) -> Result<VideoFrame> {
        Err(Error::EncodingError(
            "VP9 decoding requires the 'codecs' feature flag".to_string(),
        ))
    }

    /// Decode VP9 packet to video frame
    ///
    /// # Arguments
    ///
    /// * `payload` - Encoded VP9 packet (RTP payload)
    ///
    /// # Returns
    ///
    /// Decoded video frame (I420 format)
    ///
    /// # Implementation Note
    ///
    /// This is a structural placeholder. Full VP9 decoding requires:
    /// 1. Native libvpx bindings (vpx-sys or direct FFI)
    /// 2. Initialize vpx_codec_ctx_t with VP9 decoder interface
    /// 3. Feed compressed data via vpx_codec_decode()
    /// 4. Retrieve decoded image via vpx_codec_get_frame()
    /// 5. Convert vpx_image_t to VideoFrame with I420 data
    ///
    /// For production use, consider webrtc-rs built-in video codecs or
    /// integrate with a higher-level VP9 decoder library.
    #[cfg(feature = "codecs")]
    pub fn decode(&mut self, payload: &[u8]) -> Result<VideoFrame> {
        if payload.is_empty() {
            return Err(Error::EncodingError(
                "Empty payload for VP9 decoding".to_string(),
            ));
        }

        self.frame_count += 1;

        // TODO: Implement native VP9 decoding via vpx-sys FFI
        // Required steps:
        // 1. Initialize vpx_codec_ctx_t with VP9 decoder interface (once)
        // 2. Call vpx_codec_decode() with payload data
        // 3. Retrieve decoded frame via vpx_codec_get_frame()
        // 4. Extract Y, U, V planes from vpx_image_t
        // 5. Copy/construct VideoFrame with I420 data
        // 6. Handle keyframes and inter-frames appropriately

        Err(Error::EncodingError(
            "VP9 native decoding not yet implemented - requires vpx-sys FFI integration"
                .to_string(),
        ))
    }

    /// Get the current frame count
    pub fn frame_count(&self) -> u64 {
        self.frame_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_video_encoder_config_default() {
        let config = VideoEncoderConfig::default();
        assert_eq!(config.width, 1280);
        assert_eq!(config.height, 720);
        assert_eq!(config.framerate, 30);
    }

    #[test]
    fn test_video_encoder_creation() {
        let config = VideoEncoderConfig::default();
        let encoder = VideoEncoder::new(config);
        assert!(encoder.is_ok());
    }

    #[test]
    fn test_video_encoder_invalid_dimensions() {
        let config = VideoEncoderConfig {
            width: 0,
            height: 0,
            ..Default::default()
        };
        let encoder = VideoEncoder::new(config);
        assert!(encoder.is_err());
    }

    #[test]
    fn test_video_decoder_creation() {
        let config = VideoEncoderConfig::default();
        let decoder = VideoDecoder::new(config);
        assert!(decoder.is_ok());
    }

    #[test]
    fn test_video_frame_creation() {
        let frame = VideoFrame {
            width: 640,
            height: 480,
            format: VideoFormat::I420,
            data: vec![0u8; 640 * 480 * 3 / 2], // I420 size
            timestamp_us: 1000000,
            is_keyframe: true,
        };

        assert_eq!(frame.width, 640);
        assert_eq!(frame.height, 480);
        assert_eq!(frame.format, VideoFormat::I420);
        assert!(frame.is_keyframe);
    }
}
