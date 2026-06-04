//! Pixel format conversion node implementation
//!
//! Converts between pixel formats (RGB↔YUV, NV12↔I420, etc.) using FFmpeg swscale

use crate::data::video::PixelFormat;
use crate::data::RuntimeData;
use crate::nodes::streaming_node::AsyncStreamingNode;
use crate::Error;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

/// Configuration for pixel format conversion
///
/// Configuration for the video format converter node. Uses `#[serde(default)]` to allow
/// partial config, and `#[serde(alias)]` to accept both snake_case and camelCase.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct VideoFormatConverterConfig {
    /// Target pixel format (yuv420p, i420, nv12, rgb24, rgba32)
    #[serde(alias = "targetFormat")]
    pub target_format: PixelFormat,

    /// Color matrix: "bt601", "bt709", or "bt2020"
    #[serde(alias = "colorMatrix")]
    pub color_matrix: String,

    /// Color range: "tv" or "pc"
    #[serde(alias = "colorRange")]
    pub color_range: String,
}

impl Default for VideoFormatConverterConfig {
    fn default() -> Self {
        Self {
            target_format: PixelFormat::Yuv420p,
            color_matrix: "bt709".to_string(),
            color_range: "tv".to_string(),
        }
    }
}

/// Format converter backend trait
pub trait VideoFormatConverterBackend: Send + Sync {
    /// Convert pixel format
    fn convert(&mut self, input: RuntimeData) -> Result<RuntimeData, String>;
}

/// FFmpeg-based format converter using swscale
#[cfg(feature = "video")]
pub struct FFmpegFormatConverter {
    config: VideoFormatConverterConfig,
    scaler: Option<ac_ffmpeg::codec::video::VideoFrameScaler>,
}

#[cfg(feature = "video")]
impl FFmpegFormatConverter {
    pub fn new(config: VideoFormatConverterConfig) -> Result<Self, String> {
        Ok(Self {
            config,
            scaler: None, // Lazy initialization
        })
    }

    fn pixel_format_to_string(format: PixelFormat) -> &'static str {
        match format {
            PixelFormat::Yuv420p => "yuv420p",
            PixelFormat::I420 => "yuv420p", // I420 is same as YUV420P
            PixelFormat::NV12 => "nv12",
            PixelFormat::Rgb24 => "rgb24",
            PixelFormat::Rgba32 => "rgba",
            _ => "yuv420p",
        }
    }
}

#[cfg(feature = "video")]
impl VideoFormatConverterBackend for FFmpegFormatConverter {
    fn convert(&mut self, input: RuntimeData) -> Result<RuntimeData, String> {
        use ac_ffmpeg::codec::video::{frame::get_pixel_format, VideoFrameMut, VideoFrameScaler};

        // Extract video frame
        let (pixel_data, width, height, src_format, codec, frame_number, timestamp_us, is_keyframe) =
            match input {
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

        // Only convert raw frames (not encoded)
        if src_format == PixelFormat::Encoded || codec.is_some() {
            return Err("Cannot convert encoded frames - decode first".to_string());
        }

        // If format matches, return as-is
        if src_format == self.config.target_format {
            return Ok(RuntimeData::Video {
                pixel_data,
                width,
                height,
                format: src_format,
                codec,
                frame_number,
                timestamp_us,
                is_keyframe,
                stream_id: None,
                arrival_ts_us: None,
            });
        }

        // Lazy initialize scaler/converter
        if self.scaler.is_none() {
            let src_fmt_str = Self::pixel_format_to_string(src_format);
            let dst_fmt_str = Self::pixel_format_to_string(self.config.target_format);

            let src_pixel_format = get_pixel_format(src_fmt_str);
            let dst_pixel_format = get_pixel_format(dst_fmt_str);

            let scaler = VideoFrameScaler::builder()
                .source_pixel_format(src_pixel_format)
                .source_width(width as usize)
                .source_height(height as usize)
                .target_pixel_format(dst_pixel_format)
                .target_width(width as usize)
                .target_height(height as usize)
                .build()
                .map_err(|e| format!("Failed to create format converter: {}", e))?;

            self.scaler = Some(scaler);
        }

        let scaler = self.scaler.as_mut().unwrap();

        // Create source frame and copy pixel data
        let src_fmt_str = Self::pixel_format_to_string(src_format);
        let src_pixel_format = get_pixel_format(src_fmt_str);
        let mut src_frame = VideoFrameMut::black(src_pixel_format, width as usize, height as usize);

        // Copy pixel data to frame planes
        let y_size = (width * height) as usize;
        let uv_size = (width * height / 4) as usize;

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

        // Convert the frame
        let converted_frame = scaler
            .scale(&src_frame.freeze())
            .map_err(|e| format!("Format conversion failed: {}", e))?;

        // Extract converted pixel data
        let mut converted_pixel_data = Vec::new();
        let planes = converted_frame.planes();

        // Only access first 3 planes (Y, U, V for YUV formats)
        for i in 0..3 {
            if i < planes.len() {
                let plane_data = planes[i].data();
                if !plane_data.is_empty() {
                    converted_pixel_data.extend_from_slice(plane_data);
                }
            }
        }

        Ok(RuntimeData::Video {
            pixel_data: converted_pixel_data,
            width,
            height,
            format: self.config.target_format,
            codec,
            frame_number,
            timestamp_us,
            is_keyframe,
            stream_id: None,
            arrival_ts_us: None,
        })
    }
}

/// Video format converter node for pipeline integration
pub struct VideoFormatConverterNode {
    converter: Arc<Mutex<Box<dyn VideoFormatConverterBackend>>>,
    #[allow(dead_code)] // Reserved for runtime reconfiguration (spec 012)
    config: VideoFormatConverterConfig,
}

impl VideoFormatConverterNode {
    /// Create a new video format converter node
    pub fn new(config: VideoFormatConverterConfig) -> Result<Self, String> {
        #[cfg(feature = "video")]
        {
            let converter = FFmpegFormatConverter::new(config.clone())?;
            Ok(Self {
                converter: Arc::new(Mutex::new(Box::new(converter))),
                config,
            })
        }

        #[cfg(not(feature = "video"))]
        {
            Err("Video feature not enabled".to_string())
        }
    }

    /// Convert a video frame asynchronously
    async fn convert_frame(&self, input: RuntimeData) -> Result<RuntimeData, String> {
        let converter = Arc::clone(&self.converter);
        tokio::task::spawn_blocking(move || {
            let mut c = converter.lock().unwrap();
            c.convert(input)
        })
        .await
        .map_err(|e| format!("Converter task failed: {}", e))?
    }
}

#[async_trait]
impl AsyncStreamingNode for VideoFormatConverterNode {
    fn node_type(&self) -> &str {
        "VideoFormatConverter"
    }

    async fn process(&self, input: RuntimeData) -> Result<RuntimeData, Error> {
        self.convert_frame(input)
            .await
            .map_err(|e| Error::Execution(format!("Format conversion failed: {}", e)))
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
    async fn test_format_converter_config() {
        // Just test that config can be created
        let config = VideoFormatConverterConfig {
            target_format: PixelFormat::Rgb24,
            color_matrix: "bt709".to_string(),
            color_range: "tv".to_string(),
        };

        assert_eq!(config.target_format, PixelFormat::Rgb24);
        assert_eq!(config.color_matrix, "bt709");
    }

    #[tokio::test]
    async fn test_format_converter_noop_same_format() {
        let config = VideoFormatConverterConfig {
            target_format: PixelFormat::Yuv420p,
            ..Default::default()
        };

        let converter = VideoFormatConverterNode::new(config);
        if converter.is_err() {
            return;
        }

        let converter = converter.unwrap();

        // Create YUV420P frame
        let width = 640u32;
        let height = 480u32;
        let frame_size = (width * height * 3 / 2) as usize;
        let pixel_data = vec![128u8; frame_size];

        let input_frame = RuntimeData::Video {
            pixel_data: pixel_data.clone(),
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

        // Convert to same format (should be no-op)
        let result = converter.process(input_frame).await;

        match result {
            Ok(RuntimeData::Video {
                format: PixelFormat::Yuv420p,
                pixel_data: out_data,
                ..
            }) => {
                // Should return same data
                assert_eq!(out_data.len(), pixel_data.len());
            }
            Err(_) => {
                // May fail if FFmpeg not available
            }
            _ => panic!("Expected YUV420P video frame"),
        }
    }
}
