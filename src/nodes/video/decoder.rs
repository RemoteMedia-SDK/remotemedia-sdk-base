//! Video decoder node implementation
//!
//! Decodes compressed bitstreams to raw video frames

use crate::data::video::{PixelFormat, VideoCodec};
use crate::data::RuntimeData;
use crate::nodes::streaming_node::AsyncStreamingNode;
use crate::Error;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tracing::warn;

use super::codec::{CodecError, FFmpegDecoder, VideoDecoderBackend};

/// Configuration for video decoding
///
/// Configuration for the video decoder node. Uses `#[serde(default)]` to allow
/// partial config, and `#[serde(alias)]` to accept both snake_case and camelCase.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct VideoDecoderConfig {
    /// Expected codec (for validation). None means auto-detect from bitstream.
    #[serde(alias = "expectedCodec")]
    pub expected_codec: Option<VideoCodec>,

    /// Output pixel format (yuv420p, i420, nv12, rgb24, rgba32)
    #[serde(alias = "outputFormat")]
    pub output_format: PixelFormat,

    /// Enable hardware acceleration
    #[serde(alias = "hardwareAccel")]
    pub hardware_accel: bool,

    /// Number of threads for decoding (0 = auto-detect)
    #[schemars(range(min = 0, max = 64))]
    pub threads: u32,

    /// Error resilience mode: "strict" (fail on error) or "lenient" (attempt partial decode)
    #[serde(alias = "errorResilience")]
    pub error_resilience: String,
}

impl Default for VideoDecoderConfig {
    fn default() -> Self {
        Self {
            expected_codec: None,
            output_format: PixelFormat::Yuv420p,
            hardware_accel: true,
            threads: 0,
            error_resilience: "lenient".to_string(),
        }
    }
}

/// Video decoder node for decoding compressed video streams
///
/// Holds a thread-safe reference to the underlying decoder backend
/// and provides async decoding via `tokio::spawn_blocking`.
pub struct VideoDecoderNode {
    /// Thread-safe decoder backend
    decoder: Arc<Mutex<Box<dyn VideoDecoderBackend>>>,
    /// Decoder configuration
    config: VideoDecoderConfig,
}

impl VideoDecoderNode {
    /// Create a new video decoder node
    ///
    /// # Arguments
    /// * `config` - Decoder configuration
    ///
    /// # Returns
    /// * `Ok(Self)` - Initialized decoder node
    /// * `Err(CodecError)` - Initialization failure
    pub fn new(config: VideoDecoderConfig) -> Result<Self, CodecError> {
        // Create FFmpeg decoder backend
        let decoder = FFmpegDecoder::new(config.clone())?;
        Ok(Self {
            decoder: Arc::new(Mutex::new(Box::new(decoder))),
            config,
        })
    }

    /// Decode a video frame asynchronously
    ///
    /// Uses `tokio::spawn_blocking` to run the expensive decoding operation
    /// on a blocking thread pool, preventing it from blocking the async executor.
    ///
    /// # Arguments
    /// * `input` - Encoded video frame (RuntimeData::Video with codec=Some(...))
    ///
    /// # Returns
    /// * `Ok(RuntimeData)` - Decoded raw frame with codec=None
    /// * `Err(CodecError)` - Decoding failure
    pub async fn decode_frame(&self, input: RuntimeData) -> Result<RuntimeData, CodecError> {
        let decoder = Arc::clone(&self.decoder);
        tokio::task::spawn_blocking(move || {
            let mut dec = decoder.lock().unwrap();
            dec.decode(input)
        })
        .await
        .map_err(|e| CodecError::DecodingFailed(e.to_string()))?
    }
}

#[async_trait]
impl AsyncStreamingNode for VideoDecoderNode {
    fn node_type(&self) -> &str {
        "VideoDecoder"
    }

    async fn process(&self, input: RuntimeData) -> Result<RuntimeData, Error> {
        // Validate input is encoded frame
        match &input {
            RuntimeData::Video { codec: Some(_), .. } => {
                // Expected: encoded frame with codec specified
            }
            RuntimeData::Video { codec: None, .. } => {
                return Err(Error::Execution(
                    "Expected encoded video frame for decoding (codec must be specified)"
                        .to_string(),
                ));
            }
            _ => {
                return Err(Error::Execution(
                    "Expected encoded video frame for decoding".to_string(),
                ));
            }
        }

        // Decode frame with error resilience
        match self.decode_frame(input).await {
            Ok(frame) => Ok(frame),
            Err(e) if self.config.error_resilience == "lenient" => {
                warn!("Dropped corrupted frame: {}", e);
                Ok(RuntimeData::Video {
                    pixel_data: vec![],
                    width: 0,
                    height: 0,
                    format: PixelFormat::Unspecified,
                    codec: None,
                    frame_number: 0,
                    timestamp_us: 0,
                    is_keyframe: false,
                    stream_id: None,
                    arrival_ts_us: None,
                })
            }
            Err(e) => Err(Error::Execution(format!("Decoding failed: {}", e))),
        }
    }

    async fn process_multi(
        &self,
        inputs: std::collections::HashMap<String, RuntimeData>,
    ) -> Result<RuntimeData, Error> {
        // Extract first input and process
        if let Some((_name, data)) = inputs.into_iter().next() {
            self.process(data).await
        } else {
            Err(Error::Execution("No input data provided".into()))
        }
    }

    fn is_multi_input(&self) -> bool {
        false
    }
}

#[cfg(test)]
#[path = "decoder_tests.rs"]
mod tests;
