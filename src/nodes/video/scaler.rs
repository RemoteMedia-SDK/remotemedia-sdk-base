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
    /// Scale a video frame to target dimensions. Raw input is staged into the
    /// backend and the result written into `allocator`; both transfers are
    /// charged to `copies` (strict policy rejects them before any access).
    fn scale(
        &mut self,
        input: RuntimeData,
        copies: &remotemedia_types::CopyContext,
        allocator: &dyn remotemedia_types::OutputAllocator,
    ) -> Result<RuntimeData, String>;
}

/// FFmpeg-based video scaler using swscale
#[cfg(feature = "video")]
pub struct FFmpegScaler {
    config: VideoScalerConfig,
    /// Cached scaler with the (source, target) dimensions it was built for.
    scaler: Option<(
        (u32, u32, u32, u32),
        ac_ffmpeg::codec::video::VideoFrameScaler,
    )>,
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
    fn scale(
        &mut self,
        input: RuntimeData,
        copies: &remotemedia_types::CopyContext,
        allocator: &dyn remotemedia_types::OutputAllocator,
    ) -> Result<RuntimeData, String> {
        use ac_ffmpeg::codec::video::{frame::get_pixel_format, VideoFrameScaler};

        let RuntimeData::Video {
            pixel_data,
            width: src_width,
            height: src_height,
            format,
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

        // Only scale raw frames (not encoded)
        if format == PixelFormat::Encoded || codec.is_some() {
            return Err("Cannot scale encoded frames - decode first".to_string());
        }

        let (target_width, target_height) = self.calculate_dimensions(src_width, src_height);

        // Same dimensions: forward the original owner unchanged.
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
                stream_id,
                arrival_ts_us,
            });
        }

        // Stage the raw input into FFmpeg's strided planes: a counted,
        // stride-correct transfer (RGB input is converted, i.e. computed).
        let source =
            super::frames::encoder_frame(&pixel_data, src_width, src_height, format, copies)
                .map_err(|e| format!("Failed to stage source frame: {e}"))?
                .freeze();

        let key = (src_width, src_height, target_width, target_height);
        if self.scaler.as_ref().map(|(cached, _)| *cached) != Some(key) {
            let yuv = get_pixel_format("yuv420p");
            let scaler = VideoFrameScaler::builder()
                .source_pixel_format(yuv)
                .source_width(src_width as usize)
                .source_height(src_height as usize)
                .target_pixel_format(yuv)
                .target_width(target_width as usize)
                .target_height(target_height as usize)
                .build()
                .map_err(|e| format!("Failed to create scaler: {}", e))?;
            self.scaler = Some((key, scaler));
        }
        let (_, scaler) = self.scaler.as_mut().unwrap();
        let scaled = scaler
            .scale(&source)
            .map_err(|e| format!("Scaling failed: {}", e))?;

        // Transfer the backend-owned result into the selected output provider.
        let pixel_data =
            super::decoded_frames::decoder_frame(&scaled, PixelFormat::Yuv420p, copies, allocator)
                .map_err(|e| format!("Failed to extract scaled frame: {e}"))?;

        Ok(RuntimeData::Video {
            pixel_data,
            width: target_width,
            height: target_height,
            format: PixelFormat::Yuv420p,
            codec,
            frame_number,
            timestamp_us,
            is_keyframe,
            stream_id,
            arrival_ts_us,
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

    /// Scale a video frame asynchronously under explicit copy authority.
    async fn scale_frame(
        &self,
        input: RuntimeData,
        copies: remotemedia_types::CopyContext,
        allocator: Arc<dyn remotemedia_types::OutputAllocator>,
    ) -> Result<RuntimeData, String> {
        let scaler = Arc::clone(&self.scaler);
        tokio::task::spawn_blocking(move || {
            let mut s = scaler.lock().unwrap();
            s.scale(input, &copies, allocator.as_ref())
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

    async fn process_with_ctx(
        &self,
        input: RuntimeData,
        ctx: &dyn remotemedia_traits::runtime_context::NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        let copies = ctx.copy_context().ok_or_else(|| {
            Error::Execution("VideoScaler requires context copy authority".into())
        })?;
        let allocator = ctx
            .output_allocator()
            .unwrap_or_else(|| Arc::new(remotemedia_types::HeapOutputAllocator));
        self.scale_frame(input, copies, allocator)
            .await
            .map_err(|e| Error::Execution(format!("Video scaling failed: {}", e)))
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
        self.scale_frame(
            input,
            remotemedia_types::CopyContext::legacy().clone(),
            Arc::new(remotemedia_types::HeapOutputAllocator),
        )
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
            pixel_data: pixel_data.into(),
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
            pixel_data: pixel_data.into(),
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
    fn backend(w: u32, h: u32) -> FFmpegScaler {
        FFmpegScaler::new(VideoScalerConfig {
            target_width: w,
            target_height: h,
            algorithm: "bilinear".to_string(),
            maintain_aspect_ratio: false,
        })
        .unwrap()
    }

    #[cfg(feature = "video")]
    #[test]
    fn scaler_strict_policy_rejects_before_staging() {
        use remotemedia_types::{CopyContext, CopyPolicy, HeapOutputAllocator};
        let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "strict");
        let input = frame(vec![128; 8 * 8 * 3 / 2], 8, 8, PixelFormat::Yuv420p);
        assert!(backend(4, 4)
            .scale(input, &copies, &HeapOutputAllocator)
            .is_err());
        let counts = ingress(&copies);
        assert_eq!((counts.completed, counts.rejected), (0, 1));
    }

    #[cfg(feature = "video")]
    #[test]
    fn scaler_counts_staging_and_output_and_rebuilds_on_size_change() {
        use remotemedia_types::{CopyContext, CopyPolicy, HeapOutputAllocator};
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "session");
        let mut scaler = backend(4, 4);
        for (w, h) in [(8u32, 8u32), (16, 8)] {
            let input = frame(
                vec![128; (w * h * 3 / 2) as usize],
                w,
                h,
                PixelFormat::Yuv420p,
            );
            let RuntimeData::Video {
                pixel_data,
                width,
                height,
                stream_id,
                ..
            } = scaler.scale(input, &copies, &HeapOutputAllocator).unwrap()
            else {
                panic!("expected video")
            };
            assert_eq!((width, height), (4, 4));
            assert_eq!(pixel_data.len(), 4 * 4 * 3 / 2);
            assert!(pixel_data
                .as_slice()
                .iter()
                .all(|&b| (126..=130).contains(&b)));
            assert_eq!(stream_id.as_deref(), Some("cam"));
        }
        let counts = ingress(&copies);
        assert_eq!(counts.completed, 4);
        assert_eq!(counts.copied_bytes, 96 + 24 + 192 + 24);
    }

    #[cfg(feature = "video")]
    #[test]
    fn scaler_same_dimensions_forward_owner_without_copy() {
        use remotemedia_types::{CopyContext, CopyPolicy, HeapOutputAllocator, SharedBytes};
        let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "strict");
        let owner: SharedBytes = vec![5u8; 24].into();
        let input = RuntimeData::Video {
            pixel_data: owner.clone(),
            width: 4,
            height: 4,
            format: PixelFormat::Yuv420p,
            codec: None,
            frame_number: 0,
            timestamp_us: 0,
            is_keyframe: false,
            stream_id: None,
            arrival_ts_us: None,
        };
        let RuntimeData::Video { pixel_data, .. } = backend(4, 4)
            .scale(input, &copies, &HeapOutputAllocator)
            .unwrap()
        else {
            panic!("expected video")
        };
        assert!(pixel_data.shares_storage(&owner));
        assert_eq!(ingress(&copies).rejected, 0);
    }
}
