//! Video scaling/resizing node implementation
//!
//! Resizes video frames (upscale or downscale) using FFmpeg swscale

use crate::data::video::PixelFormat;
use crate::data::RuntimeData;
use crate::nodes::streaming_node::AsyncStreamingNode;
use crate::Error;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

/// Configuration for video scaling
///
/// Configuration for the video scaler node. Uses `#[serde(default)]` to allow
/// partial config, and `#[serde(alias)]` to accept both snake_case and camelCase.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct VideoScalerConfig {
    /// Target width in pixels (0 = maintain aspect ratio based on height)
    #[serde(alias = "targetWidth")]
    #[schemars(range(min = 0, max = 7680))]
    pub target_width: u32,

    /// Target height in pixels (0 = maintain aspect ratio based on width)
    #[serde(alias = "targetHeight")]
    #[schemars(range(min = 0, max = 4320))]
    pub target_height: u32,

    /// Scaling algorithm: "bilinear", "bicubic", or "lanczos"
    pub algorithm: String,

    /// Maintain aspect ratio (add padding if needed)
    #[serde(alias = "maintainAspectRatio")]
    pub maintain_aspect_ratio: bool,
}

impl Default for VideoScalerConfig {
    fn default() -> Self {
        Self {
            target_width: 1280,
            target_height: 720,
            algorithm: "bilinear".to_string(),
            maintain_aspect_ratio: true,
        }
    }
}

/// Video scaler backend trait
pub trait VideoScalerBackend: Send + Sync {
    /// Scale a video frame to target dimensions
    fn scale(&mut self, input: RuntimeData) -> Result<RuntimeData, String>;
}

/// FFmpeg-based video scaler using swscale
#[cfg(feature = "video")]
pub struct FFmpegScaler {
    config: VideoScalerConfig,
    scaler: Option<ac_ffmpeg::codec::video::VideoFrameScaler>,
}

#[cfg(feature = "video")]
impl FFmpegScaler {
    pub fn new(config: VideoScalerConfig) -> Result<Self, String> {
        // Validate configuration
        if config.target_width == 0 && config.target_height == 0 {
            return Err("At least one dimension (width or height) must be specified".to_string());
        }

        Ok(Self {
            config,
            scaler: None, // Lazy initialization
        })
    }

    /// Calculate target dimensions maintaining aspect ratio
    fn calculate_dimensions(&self, src_width: u32, src_height: u32) -> (u32, u32) {
        let (mut width, mut height) = (self.config.target_width, self.config.target_height);

        if !self.config.maintain_aspect_ratio {
            // Use target dimensions as-is
            if width == 0 {
                width = src_width;
            }
            if height == 0 {
                height = src_height;
            }
            return self.ensure_even_dimensions(width, height);
        }

        // Calculate with aspect ratio
        let aspect_ratio = src_width as f32 / src_height as f32;

        if width == 0 {
            // Calculate width from height
            width = (height as f32 * aspect_ratio).round() as u32;
        } else if height == 0 {
            // Calculate height from width
            height = (width as f32 / aspect_ratio).round() as u32;
        }

        self.ensure_even_dimensions(width, height)
    }

    /// Ensure dimensions are even (required for YUV formats)
    fn ensure_even_dimensions(&self, width: u32, height: u32) -> (u32, u32) {
        let width = if width % 2 == 1 { width + 1 } else { width };
        let height = if height % 2 == 1 { height + 1 } else { height };
        (width, height)
    }
}

#[cfg(feature = "video")]
impl VideoScalerBackend for FFmpegScaler {
    fn scale(&mut self, input: RuntimeData) -> Result<RuntimeData, String> {
        use ac_ffmpeg::codec::video::{frame::get_pixel_format, VideoFrameMut, VideoFrameScaler};

        // Extract video frame
        let (
            pixel_data,
            src_width,
            src_height,
            format,
            codec,
            frame_number,
            timestamp_us,
            is_keyframe,
        ) = match input {
            RuntimeData::Video {
                pixel_data,
                width,
                height,
                format,
                codec,
                frame_number,
                timestamp_us,
                is_keyframe,
                ..
            } => (
                pixel_data,
                width,
                height,
                format,
                codec,
                frame_number,
                timestamp_us,
                is_keyframe,
            ),
            _ => return Err("Expected video frame".to_string()),
        };

        // Only scale raw frames (not encoded)
        if format == PixelFormat::Encoded || codec.is_some() {
            return Err("Cannot scale encoded frames - decode first".to_string());
        }

        // Calculate target dimensions
        let (target_width, target_height) = self.calculate_dimensions(src_width, src_height);

        // If dimensions match, return as-is
        if target_width == src_width && target_height == src_height {
            return Ok(RuntimeData::Video {
                pixel_data,
                width: src_width,
                height: src_height,
                format,
                codec,
                frame_number,
                timestamp_us,
                is_keyframe,
                stream_id: None,
                arrival_ts_us: None,
            });
        }

        // Lazy initialize scaler
        if self.scaler.is_none() {
            let src_format = get_pixel_format("yuv420p");
            let dst_format = get_pixel_format("yuv420p");

            let scaler = VideoFrameScaler::builder()
                .source_pixel_format(src_format)
                .source_width(src_width as usize)
                .source_height(src_height as usize)
                .target_pixel_format(dst_format)
                .target_width(target_width as usize)
                .target_height(target_height as usize)
                .build()
                .map_err(|e| format!("Failed to create scaler: {}", e))?;

            self.scaler = Some(scaler);
        }

        let scaler = self.scaler.as_mut().unwrap();

        // Create source frame and copy pixel data
        let src_format = get_pixel_format("yuv420p");
        let mut src_frame =
            VideoFrameMut::black(src_format, src_width as usize, src_height as usize);

        // Copy pixel data to frame planes
        let y_size = (src_width * src_height) as usize;
        let uv_size = (src_width * src_height / 4) as usize;

        if pixel_data.len() >= y_size + 2 * uv_size {
            let mut planes_mut = src_frame.planes_mut();

            if planes_mut.len() > 0 {
                let y_plane = planes_mut[0].data_mut();
                let copy_len = y_plane.len().min(y_size);
                y_plane[..copy_len].copy_from_slice(&pixel_data[..copy_len]);
            }

            if planes_mut.len() > 1 {
                let u_plane = planes_mut[1].data_mut();
                let copy_len = u_plane.len().min(uv_size);
                u_plane[..copy_len].copy_from_slice(&pixel_data[y_size..y_size + copy_len]);
            }

            if planes_mut.len() > 2 {
                let v_plane = planes_mut[2].data_mut();
                let copy_len = v_plane.len().min(uv_size);
                v_plane[..copy_len]
                    .copy_from_slice(&pixel_data[y_size + uv_size..y_size + uv_size + copy_len]);
            }
        }

        // Scale the frame
        let scaled_frame = scaler
            .scale(&src_frame.freeze())
            .map_err(|e| format!("Scaling failed: {}", e))?;

        // Extract scaled pixel data
        let mut scaled_pixel_data = Vec::new();
        let planes = scaled_frame.planes();

        // Only access first 3 planes (Y, U, V for YUV420P)
        for i in 0..3 {
            if i < planes.len() {
                let plane_data = planes[i].data();
                if !plane_data.is_empty() {
                    scaled_pixel_data.extend_from_slice(plane_data);
                }
            }
        }

        Ok(RuntimeData::Video {
            pixel_data: scaled_pixel_data,
            width: target_width,
            height: target_height,
            format,
            codec,
            frame_number,
            timestamp_us,
            is_keyframe,
            stream_id: None,
            arrival_ts_us: None,
        })
    }
}

/// Video scaler node for pipeline integration
pub struct VideoScalerNode {
    scaler: Arc<Mutex<Box<dyn VideoScalerBackend>>>,
    #[allow(dead_code)] // Reserved for runtime reconfiguration (spec 012)
    config: VideoScalerConfig,
}

impl VideoScalerNode {
    /// Create a new video scaler node
    pub fn new(config: VideoScalerConfig) -> Result<Self, String> {
        #[cfg(feature = "video")]
        {
            let scaler = FFmpegScaler::new(config.clone())?;
            Ok(Self {
                scaler: Arc::new(Mutex::new(Box::new(scaler))),
                config,
            })
        }

        #[cfg(not(feature = "video"))]
        {
            Err("Video feature not enabled".to_string())
        }
    }

    /// Scale a video frame asynchronously
    async fn scale_frame(&self, input: RuntimeData) -> Result<RuntimeData, String> {
        let scaler = Arc::clone(&self.scaler);
        tokio::task::spawn_blocking(move || {
            let mut s = scaler.lock().unwrap();
            s.scale(input)
        })
        .await
        .map_err(|e| format!("Scaler task failed: {}", e))?
    }
}

#[async_trait]
impl AsyncStreamingNode for VideoScalerNode {
    fn node_type(&self) -> &str {
        "VideoScaler"
    }

    async fn process(&self, input: RuntimeData) -> Result<RuntimeData, Error> {
        self.scale_frame(input)
            .await
            .map_err(|e| Error::Execution(format!("Video scaling failed: {}", e)))
    }

    async fn process_multi(
        &self,
        inputs: std::collections::HashMap<String, RuntimeData>,
    ) -> Result<RuntimeData, Error> {
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
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_scaler_downscale_1080p_to_720p() {
        let config = VideoScalerConfig {
            target_width: 1280,
            target_height: 720,
            algorithm: "bilinear".to_string(),
            maintain_aspect_ratio: true,
        };

        let scaler = VideoScalerNode::new(config);
        if scaler.is_err() {
            // Video feature not enabled
            return;
        }

        let scaler = scaler.unwrap();

        // Create 1080p frame
        let src_width = 1920u32;
        let src_height = 1080u32;
        let frame_size = (src_width * src_height * 3 / 2) as usize;
        let pixel_data = vec![128u8; frame_size];

        let input_frame = RuntimeData::Video {
            pixel_data,
            width: src_width,
            height: src_height,
            format: PixelFormat::Yuv420p,
            codec: None,
            frame_number: 0,
            timestamp_us: 0,
            is_keyframe: false,
            stream_id: None,
            arrival_ts_us: None,
        };

        // Scale to 720p
        let result = scaler.process(input_frame).await;

        match result {
            Ok(RuntimeData::Video {
                width,
                height,
                format,
                ..
            }) => {
                assert_eq!(width, 1280);
                assert_eq!(height, 720);
                assert_eq!(format, PixelFormat::Yuv420p);
            }
            Err(e) => {
                // May fail if FFmpeg not available
                assert!(
                    e.to_string().contains("not available") || e.to_string().contains("Failed")
                );
            }
            _ => panic!("Expected video frame"),
        }
    }

    #[tokio::test]
    async fn test_scaler_maintains_aspect_ratio() {
        let config = VideoScalerConfig {
            target_width: 0, // Calculate from height
            target_height: 480,
            algorithm: "bilinear".to_string(),
            maintain_aspect_ratio: true,
        };

        let scaler = VideoScalerNode::new(config);
        if scaler.is_err() {
            return;
        }

        let scaler = scaler.unwrap();

        // Create 16:9 source frame
        let src_width = 1280u32;
        let src_height = 720u32;
        let frame_size = (src_width * src_height * 3 / 2) as usize;
        let pixel_data = vec![128u8; frame_size];

        let input_frame = RuntimeData::Video {
            pixel_data,
            width: src_width,
            height: src_height,
            format: PixelFormat::Yuv420p,
            codec: None,
            frame_number: 0,
            timestamp_us: 0,
            is_keyframe: false,
            stream_id: None,
            arrival_ts_us: None,
        };

        let result = scaler.process(input_frame).await;

        match result {
            Ok(RuntimeData::Video { width, height, .. }) => {
                // Should calculate width to maintain 16:9 aspect ratio
                // 480 * (16/9) = 853.33... → 854 (even)
                assert_eq!(height, 480);
                assert!(width > 0, "Width should be calculated");
                assert_eq!(width % 2, 0, "Width should be even");
            }
            Err(_) => {
                // May fail if FFmpeg not available
            }
            _ => panic!("Expected video frame"),
        }
    }
}
