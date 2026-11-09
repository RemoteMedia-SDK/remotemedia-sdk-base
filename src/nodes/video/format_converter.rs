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
    /// Convert pixel format. Raw input is staged into the backend and the
    /// result written into `allocator`; both transfers are charged to `copies`
    /// (strict policy rejects them before any payload access).
    fn convert(
        &mut self,
        input: RuntimeData,
        copies: &remotemedia_types::CopyContext,
        allocator: &dyn remotemedia_types::OutputAllocator,
    ) -> Result<RuntimeData, String>;
}

/// FFmpeg-based format converter using swscale
#[cfg(feature = "video")]
pub struct FFmpegFormatConverter {
    config: VideoFormatConverterConfig,
}

#[cfg(feature = "video")]
impl FFmpegFormatConverter {
    pub fn new(config: VideoFormatConverterConfig) -> Result<Self, String> {
        Ok(Self { config })
    }

    /// Native format name and visible plane shapes `(row bytes, rows)`.
    fn source_layout(
        format: PixelFormat,
        width: usize,
        height: usize,
    ) -> Result<(&'static str, Vec<(usize, usize)>), String> {
        let even = width % 2 == 0 && height % 2 == 0;
        match format {
            PixelFormat::Yuv420p | PixelFormat::I420 if even => Ok((
                "yuv420p",
                vec![
                    (width, height),
                    (width / 2, height / 2),
                    (width / 2, height / 2),
                ],
            )),
            PixelFormat::NV12 if even => Ok(("nv12", vec![(width, height), (width, height / 2)])),
            PixelFormat::Yuv420p | PixelFormat::I420 | PixelFormat::NV12 => {
                Err("4:2:0 input requires even dimensions".to_string())
            }
            PixelFormat::Rgb24 => Ok(("rgb24", vec![(width * 3, height)])),
            PixelFormat::Rgba32 => Ok(("rgba", vec![(width * 4, height)])),
            other => Err(format!("unsupported source pixel format: {other:?}")),
        }
    }

    /// Stage a tightly packed input span into FFmpeg's strided planes. This is
    /// a counted ownership transfer, authorized before allocation or writes.
    fn stage(
        data: &[u8],
        width: u32,
        height: u32,
        format: PixelFormat,
        copies: &remotemedia_types::CopyContext,
    ) -> Result<ac_ffmpeg::codec::video::VideoFrame, String> {
        use ac_ffmpeg::codec::video::{frame::get_pixel_format, VideoFrameMut};

        if width == 0 || height == 0 || width > i32::MAX as u32 || height > i32::MAX as u32 {
            return Err("invalid frame dimensions".to_string());
        }
        let (w, h) = (width as usize, height as usize);
        let (name, shapes) = Self::source_layout(format, w, h)?;
        let expected = shapes.iter().try_fold(0usize, |acc, &(columns, rows)| {
            acc.checked_add(columns.checked_mul(rows)?)
        });
        if expected != Some(data.len()) {
            return Err("frame buffer size mismatch: span length differs from dimensions".into());
        }
        let transfer = copies
            .begin(
                remotemedia_types::CopyReason::ForeignPoolIngress,
                data.len() as u64,
            )
            .map_err(|e| e.to_string())?;
        let mut frame = VideoFrameMut::black(get_pixel_format(name), w, h);
        let mut planes = frame.planes_mut();
        if planes.len() < shapes.len() {
            return Err("native frame has too few planes".to_string());
        }
        for (plane, &(columns, rows)) in planes.iter_mut().zip(&shapes) {
            let stride = plane.line_size();
            crate::data::video::validate_plane_span(plane.data_mut().len(), stride, columns, rows)
                .map_err(|e| e.to_string())?;
        }
        let mut offset = 0;
        for (plane, &(columns, rows)) in planes.iter_mut().zip(&shapes) {
            let stride = plane.line_size();
            let dst = plane.data_mut();
            for row in 0..rows {
                dst[row * stride..row * stride + columns]
                    .copy_from_slice(&data[offset..offset + columns]);
                offset += columns;
            }
        }
        drop(planes);
        transfer.complete();
        Ok(frame.freeze())
    }
}

#[cfg(feature = "video")]
impl VideoFormatConverterBackend for FFmpegFormatConverter {
    fn convert(
        &mut self,
        input: RuntimeData,
        copies: &remotemedia_types::CopyContext,
        allocator: &dyn remotemedia_types::OutputAllocator,
    ) -> Result<RuntimeData, String> {
        let RuntimeData::Video {
            pixel_data,
            width,
            height,
            format: src_format,
            codec,
            frame_number,
            timestamp_us,
            is_keyframe,
            stream_id,
            arrival_ts_us,
        } = input
        else {
            return Err("Expected video frame".to_string());
        };

        // Only convert raw frames (not encoded)
        if src_format == PixelFormat::Encoded || codec.is_some() {
            return Err("Cannot convert encoded frames - decode first".to_string());
        }

        let target = self.config.target_format;
        let same = src_format == target
            || matches!(
                (src_format, target),
                (PixelFormat::Yuv420p, PixelFormat::I420)
                    | (PixelFormat::I420, PixelFormat::Yuv420p)
            );
        // Same layout: forward the original owner unchanged.
        if same {
            return Ok(RuntimeData::Video {
                pixel_data,
                width,
                height,
                format: target,
                codec,
                frame_number,
                timestamp_us,
                is_keyframe,
                stream_id,
                arrival_ts_us,
            });
        }

        let source = Self::stage(&pixel_data, width, height, src_format, copies)?;
        // Convert and pack into the selected output provider (counted).
        let pixel_data = super::decoded_frames::decoder_frame(&source, target, copies, allocator)
            .map_err(|e| format!("Format conversion failed: {e}"))?;

        Ok(RuntimeData::Video {
            pixel_data,
            width,
            height,
            format: target,
            codec,
            frame_number,
            timestamp_us,
            is_keyframe,
            stream_id,
            arrival_ts_us,
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

    /// Convert a video frame asynchronously under explicit copy authority.
    async fn convert_frame(
        &self,
        input: RuntimeData,
        copies: remotemedia_types::CopyContext,
        allocator: Arc<dyn remotemedia_types::OutputAllocator>,
    ) -> Result<RuntimeData, String> {
        let converter = Arc::clone(&self.converter);
        tokio::task::spawn_blocking(move || {
            let mut c = converter.lock().unwrap();
            c.convert(input, &copies, allocator.as_ref())
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

    async fn process_with_ctx(
        &self,
        input: RuntimeData,
        ctx: &dyn remotemedia_traits::runtime_context::NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        let copies = ctx.copy_context().ok_or_else(|| {
            Error::Execution("VideoFormatConverter requires context copy authority".into())
        })?;
        let allocator = ctx
            .output_allocator()
            .unwrap_or_else(|| Arc::new(remotemedia_types::HeapOutputAllocator));
        self.convert_frame(input, copies, allocator)
            .await
            .map_err(|e| Error::Execution(format!("Format conversion failed: {}", e)))
    }

    async fn process_streaming_with_ctx<F>(
        &self,
        input: RuntimeData,
        ctx: &dyn remotemedia_traits::runtime_context::NodeRuntimeContextRead,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        callback(self.process_with_ctx(input, ctx).await?)?;
        Ok(1)
    }

    /// Context-free compatibility path: charged to the global legacy ledger
    /// and allocated on the heap. Sessions use `process_with_ctx`.
    async fn process(&self, input: RuntimeData) -> Result<RuntimeData, Error> {
        self.convert_frame(
            input,
            remotemedia_types::CopyContext::legacy().clone(),
            Arc::new(remotemedia_types::HeapOutputAllocator),
        )
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

        let converter = VideoFormatConverterNode::new(config).unwrap();

        // Create YUV420P frame
        let width = 640u32;
        let height = 480u32;
        let frame_size = (width * height * 3 / 2) as usize;
        let pixel_data: remotemedia_types::SharedBytes = vec![128u8; frame_size].into();

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
        let RuntimeData::Video {
            format,
            pixel_data: output,
            ..
        } = converter.process(input_frame).await.unwrap()
        else {
            panic!("Expected video frame")
        };
        assert_eq!(format, PixelFormat::Yuv420p);
        assert_eq!(output.as_ptr(), pixel_data.as_ptr());
        assert!(output.shares_storage(&pixel_data));
        assert_eq!(output.as_slice(), pixel_data.as_slice());
    }

    #[cfg(feature = "video")]
    fn ingress(copies: &remotemedia_types::CopyContext) -> remotemedia_types::CopyCounts {
        copies
            .report()
            .entries
            .into_iter()
            .find(|(reason, _)| *reason == remotemedia_types::CopyReason::ForeignPoolIngress)
            .unwrap()
            .1
    }

    #[cfg(feature = "video")]
    fn frame(pixel_data: Vec<u8>, width: u32, height: u32, format: PixelFormat) -> RuntimeData {
        RuntimeData::Video {
            pixel_data: pixel_data.into(),
            width,
            height,
            format,
            codec: None,
            frame_number: 7,
            timestamp_us: 9,
            is_keyframe: true,
            stream_id: Some("cam".into()),
            arrival_ts_us: Some(11),
        }
    }

    #[cfg(feature = "video")]
    #[test]
    fn converter_strict_policy_rejects_before_staging() {
        use remotemedia_types::{CopyContext, CopyPolicy, HeapOutputAllocator};
        let mut backend = FFmpegFormatConverter::new(VideoFormatConverterConfig {
            target_format: PixelFormat::Rgba32,
            ..Default::default()
        })
        .unwrap();
        let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "strict");
        let input = frame(vec![9; 4 * 4 * 3], 4, 4, PixelFormat::Rgb24);
        assert!(backend
            .convert(input, &copies, &HeapOutputAllocator)
            .is_err());
        let counts = ingress(&copies);
        assert_eq!(counts.completed, 0);
        assert_eq!(counts.rejected, 1);
        assert_eq!(counts.rejected_bytes, 48);
    }

    #[cfg(feature = "video")]
    #[test]
    fn converter_counts_staging_and_output_and_keeps_metadata() {
        use remotemedia_types::{CopyContext, CopyPolicy, HeapOutputAllocator};
        let mut backend = FFmpegFormatConverter::new(VideoFormatConverterConfig {
            target_format: PixelFormat::Rgba32,
            ..Default::default()
        })
        .unwrap();
        let rgb: Vec<u8> = (0..4 * 4 * 3).map(|i| (i * 5 % 256) as u8).collect();
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "session");
        let input = frame(rgb.clone(), 4, 4, PixelFormat::Rgb24);
        let RuntimeData::Video {
            pixel_data,
            width,
            height,
            format,
            frame_number,
            timestamp_us,
            is_keyframe,
            stream_id,
            arrival_ts_us,
            ..
        } = backend
            .convert(input, &copies, &HeapOutputAllocator)
            .unwrap()
        else {
            panic!("expected video")
        };
        assert_eq!((width, height, format), (4, 4, PixelFormat::Rgba32));
        assert_eq!((frame_number, timestamp_us, is_keyframe), (7, 9, true));
        assert_eq!(stream_id.as_deref(), Some("cam"));
        assert_eq!(arrival_ts_us, Some(11));
        let expected: Vec<u8> = rgb
            .chunks(3)
            .flat_map(|p| [p[0], p[1], p[2], 255])
            .collect();
        assert_eq!(pixel_data.as_slice(), expected.as_slice());
        let counts = ingress(&copies);
        assert_eq!(counts.completed, 2, "input staging + provider output");
        assert_eq!(counts.copied_bytes, 48 + 64);
    }

    #[cfg(feature = "video")]
    #[test]
    fn converter_rejects_malformed_spans_and_odd_420() {
        use remotemedia_types::{CopyContext, CopyPolicy, HeapOutputAllocator};
        let mut backend = FFmpegFormatConverter::new(VideoFormatConverterConfig {
            target_format: PixelFormat::Rgb24,
            ..Default::default()
        })
        .unwrap();
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "session");
        for input in [
            frame(vec![0; 10], 4, 4, PixelFormat::Yuv420p),
            frame(vec![0; 13], 3, 3, PixelFormat::Yuv420p),
            frame(vec![0; 16], 4, 4, PixelFormat::Unspecified),
        ] {
            assert!(backend
                .convert(input, &copies, &HeapOutputAllocator)
                .is_err());
        }
        assert_eq!(ingress(&copies).completed, 0);
    }
}
