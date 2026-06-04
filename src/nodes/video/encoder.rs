//! Video encoder node implementation
//!
//! Encodes raw video frames to compressed bitstreams (VP8/AV1/H.264)

use crate::data::video::VideoCodec;
use crate::data::RuntimeData;
use crate::nodes::streaming_node::AsyncStreamingNode;
use crate::Error;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::codec::{CodecError, FFmpegEncoder, VideoEncoderBackend};

/// Configuration for video encoding
///
/// Configuration for the video encoder node. Uses `#[serde(default)]` to allow
/// partial config, and `#[serde(alias)]` to accept both snake_case and camelCase.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct VideoEncoderConfig {
    /// Codec to use (vp8, h264, av1)
    pub codec: VideoCodec,

    /// Target bitrate in bits per second (e.g., 2_000_000 for 2 Mbps)
    #[schemars(range(min = 100000, max = 50000000))]
    pub bitrate: u32,

    /// Target frame rate (fps) (e.g., 30 for 30fps)
    #[schemars(range(min = 1, max = 120))]
    pub framerate: u32,

    /// Keyframe interval in frames (e.g., 60 means I-frame every 60 frames)
    #[serde(alias = "keyframeInterval")]
    #[schemars(range(min = 1, max = 300))]
    pub keyframe_interval: u32,

    /// Quality preset (codec-specific)
    /// VP8: "good", "best", "realtime"
    /// H.264: "ultrafast", "fast", "medium", "slow"
    /// AV1: "0" (slowest) to "10" (fastest)
    #[serde(alias = "qualityPreset")]
    pub quality_preset: String,

    /// Enable hardware acceleration (VAAPI on Linux, VideoToolbox on macOS, NVENC/QuickSync on Windows)
    #[serde(alias = "hardwareAccel")]
    pub hardware_accel: bool,

    /// Number of threads for encoding (0 = auto-detect based on CPU cores)
    #[schemars(range(min = 0, max = 64))]
    pub threads: u32,
}

impl Default for VideoEncoderConfig {
    fn default() -> Self {
        Self {
            codec: VideoCodec::Vp8,
            bitrate: 1_000_000, // 1 Mbps
            framerate: 30,
            keyframe_interval: 60,
            quality_preset: "medium".to_string(),
            hardware_accel: true,
            threads: 0,
        }
    }
}

/// Video encoder node for real-time frame encoding
///
/// This node encodes raw video frames to compressed bitstreams using a backend encoder
/// (FFmpeg, rav1e, etc.). The encoder is wrapped in Arc<Mutex<>> for thread-safe access
/// across async contexts via tokio::spawn_blocking.
///
/// # Thread Safety
///
/// The encoder backend is not Send/Sync across async boundaries, so encoding operations
/// are run on the blocking thread pool using tokio::spawn_blocking. This ensures proper
/// isolation while maintaining async ergonomics for the StreamingNode trait.
///
/// # Example
///
/// ```
/// use remotemedia_core::nodes::video::encoder::VideoEncoderConfig;
/// use remotemedia_core::data::VideoCodec;
///
/// let config = VideoEncoderConfig {
///     codec: VideoCodec::Vp8,
///     bitrate: 2_000_000,
///     framerate: 30,
///     ..Default::default()
/// };
/// ```
pub struct VideoEncoderNode {
    /// Mutex-wrapped encoder backend for thread-safe encoding
    /// Wrapped in Arc to allow cloning for spawn_blocking
    encoder: Arc<Mutex<Box<dyn VideoEncoderBackend>>>,
    /// Configuration for this encoder
    config: VideoEncoderConfig,
}

impl VideoEncoderNode {
    /// Create a new video encoder node with the given configuration
    ///
    /// # Arguments
    ///
    /// * `config` - Encoder configuration (codec, bitrate, quality, etc.)
    ///
    /// # Returns
    ///
    /// * `Ok(Self)` - Initialized encoder node
    /// * `Err(CodecError)` - If encoder backend cannot be initialized
    ///
    /// # Errors
    ///
    /// Returns `CodecError::NotAvailable` if:
    /// - The video feature is not enabled
    /// - The codec is not available on this platform
    /// - Required codec libraries (FFmpeg) cannot be found
    pub fn new(config: VideoEncoderConfig) -> Result<Self, CodecError> {
        // Create FFmpeg encoder backend
        let encoder = FFmpegEncoder::new(config.clone())?;

        Ok(Self {
            encoder: Arc::new(Mutex::new(Box::new(encoder))),
            config,
        })
    }

    /// Encode a raw video frame to a compressed bitstream
    ///
    /// This method runs the encoding operation on tokio's blocking thread pool
    /// to avoid blocking the async runtime. The encoder backend is not Send/Sync
    /// across thread boundaries, so it must be wrapped in Arc<Mutex<>> and accessed
    /// via the blocking API.
    ///
    /// # Arguments
    ///
    /// * `input` - Raw video frame (RuntimeData::Video with codec=None)
    ///
    /// # Returns
    ///
    /// * `Ok(RuntimeData)` - Encoded video frame (RuntimeData::Video with codec=Some(...))
    /// * `Err(CodecError)` - If encoding fails
    ///
    /// # Errors
    ///
    /// Returns `CodecError::EncodingFailed` if:
    /// - Input is not a video frame
    /// - Encoding operation fails (codec error, buffer issues, etc.)
    /// - Task cannot be spawned on the blocking thread pool
    pub async fn encode_frame(&self, input: RuntimeData) -> Result<RuntimeData, CodecError> {
        let encoder = Arc::clone(&self.encoder);

        tokio::task::spawn_blocking(move || {
            let mut enc = encoder.lock().unwrap();
            enc.encode(input)
        })
        .await
        .map_err(|e| CodecError::EncodingFailed(e.to_string()))?
    }

    /// Get the encoder configuration
    pub fn config(&self) -> &VideoEncoderConfig {
        &self.config
    }

    /// Reconfigure the encoder (bitrate, quality, etc.)
    ///
    /// # Arguments
    ///
    /// * `new_config` - New encoder configuration
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Configuration updated successfully
    /// * `Err(CodecError)` - If reconfiguration fails
    pub fn reconfigure(&mut self, new_config: VideoEncoderConfig) -> Result<(), CodecError> {
        let mut enc = self.encoder.lock().unwrap();
        enc.reconfigure(&new_config)?;
        self.config = new_config;
        Ok(())
    }

    /// Get the codec this encoder produces
    pub fn codec(&self) -> VideoCodec {
        self.config.codec
    }
}

#[async_trait]
impl AsyncStreamingNode for VideoEncoderNode {
    fn node_type(&self) -> &str {
        "VideoEncoder"
    }

    async fn process(&self, input: RuntimeData) -> Result<RuntimeData, Error> {
        // Validate input is raw video frame (codec must be None)
        match &input {
            RuntimeData::Video { codec: None, .. } => {
                // Frame is raw (unencoded), proceed with encoding
            }
            RuntimeData::Video { codec: Some(_), .. } => {
                return Err(Error::Execution(
                    "Expected raw video frame for encoding, but received already-encoded frame"
                        .to_string(),
                ));
            }
            _ => {
                return Err(Error::Execution(
                    "Expected RuntimeData::Video for encoding".to_string(),
                ));
            }
        }

        // Encode the frame using the backend encoder
        self.encode_frame(input)
            .await
            .map_err(|e| Error::Execution(format!("Encoding failed: {}", e)))
    }

    async fn process_multi(
        &self,
        inputs: HashMap<String, RuntimeData>,
    ) -> Result<RuntimeData, Error> {
        // Video encoder expects single input
        let input = inputs
            .into_iter()
            .next()
            .ok_or_else(|| Error::Execution("No input data provided".to_string()))?
            .1;

        self.process(input).await
    }

    fn is_multi_input(&self) -> bool {
        false // Single input node
    }
}

#[cfg(test)]
#[path = "encoder_tests.rs"]
mod tests;
