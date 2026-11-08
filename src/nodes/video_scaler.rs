//! `VideoScaler`: resize raw video frames.
//!
//! Pure Rust (`fast_image_resize`, SIMD), so it needs no ffmpeg and is always
//! available. Handles packed `Rgb24` / `Rgba32` and planar `Yuv420p` / `I420`
//! / `NV12` frames, keeps each frame's format, and copes with the input size
//! changing between frames (a resized window, a switched camera mode).
//!
//! ```yaml
//! - id: small
//!   node_type: VideoScaler
//!   params: { target_width: 1920, target_height: 0 }   # 1920 wide, height by aspect
//! ```

use crate::data::video::PixelFormat;
use crate::data::RuntimeData;
use crate::nodes::streaming_node::AsyncStreamingNode;
use crate::Error;
use async_trait::async_trait;
use fast_image_resize::images::{Image, ImageRef};
use fast_image_resize::{FilterType, PixelType, ResizeAlg, ResizeOptions, Resizer};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

/// `VideoScaler` params.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct VideoScalerConfig {
    /// Target width in pixels; 0 = from `target_height` and the aspect ratio.
    #[serde(alias = "targetWidth")]
    #[schemars(range(min = 0, max = 16384))]
    pub target_width: u32,

    /// Target height in pixels; 0 = from `target_width` and the aspect ratio.
    #[serde(alias = "targetHeight")]
    #[schemars(range(min = 0, max = 16384))]
    pub target_height: u32,

    /// `nearest`, `bilinear` (default), `bicubic`, `lanczos` or `area`
    /// (box filter; good for large downscales).
    pub algorithm: String,

    /// With both dimensions set: fit the frame inside them keeping its aspect
    /// ratio (no padding; one side may come out smaller). `false` stretches
    /// to exactly `target_width` x `target_height`.
    #[serde(alias = "maintainAspectRatio")]
    pub maintain_aspect_ratio: bool,

    /// Never enlarge: a frame already within the target passes through.
    #[serde(alias = "onlyDownscale")]
    pub only_downscale: bool,
}

impl Default for VideoScalerConfig {
    fn default() -> Self {
        Self {
            target_width: 1280,
            target_height: 720,
            algorithm: "bilinear".to_string(),
            maintain_aspect_ratio: true,
            only_downscale: false,
        }
    }
}

fn resize_alg(name: &str) -> Result<ResizeAlg, String> {
    Ok(match name.to_ascii_lowercase().as_str() {
        "nearest" => ResizeAlg::Nearest,
        "bilinear" | "" => ResizeAlg::Convolution(FilterType::Bilinear),
        "bicubic" | "catmullrom" => ResizeAlg::Convolution(FilterType::CatmullRom),
        "lanczos" | "lanczos3" => ResizeAlg::Convolution(FilterType::Lanczos3),
        "area" | "box" => ResizeAlg::Convolution(FilterType::Box),
        other => {
            return Err(format!(
                "unknown algorithm '{other}' (nearest, bilinear, bicubic, lanczos, area)"
            ))
        }
    })
}

impl VideoScalerConfig {
    /// Output size for a `src_w` x `src_h` frame (`even` rounds both sides
    /// down to even numbers, as 4:2:0 chroma needs).
    pub fn output_size(&self, src_w: u32, src_h: u32, even: bool) -> (u32, u32) {
        let (tw, th) = (self.target_width, self.target_height);
        let aspect = src_w as f64 / src_h.max(1) as f64;
        let (mut w, mut h) = match (tw, th) {
            (0, 0) => (src_w, src_h),
            (w, 0) => (w, (w as f64 / aspect).round() as u32),
            (0, h) => ((h as f64 * aspect).round() as u32, h),
            (w, h) if !self.maintain_aspect_ratio => (w, h),
            (w, h) => {
                // Fit inside w x h.
                let scale = (w as f64 / src_w as f64).min(h as f64 / src_h as f64);
                (
                    ((src_w as f64 * scale).round() as u32).min(w),
                    ((src_h as f64 * scale).round() as u32).min(h),
                )
            }
        };
        if self.only_downscale && w >= src_w && h >= src_h {
            return (src_w, src_h);
        }
        if even {
            w &= !1;
            h &= !1;
        }
        (
            w.max(if even { 2 } else { 1 }),
            h.max(if even { 2 } else { 1 }),
        )
    }
}

/// Resize one `pixel_type` plane.
fn resize_plane(
    resizer: &mut Resizer,
    opts: &ResizeOptions,
    data: &[u8],
    (w, h): (u32, u32),
    (dw, dh): (u32, u32),
    pixel_type: PixelType,
) -> Result<Vec<u8>, String> {
    let src = ImageRef::new(w, h, data, pixel_type).map_err(|e| e.to_string())?;
    let mut dst = Image::new(dw, dh, pixel_type);
    resizer
        .resize(&src, &mut dst, opts)
        .map_err(|e| e.to_string())?;
    Ok(dst.into_vec())
}

/// Scale `pixel_data` (`format`, `w` x `h`) to the configured size; `None`
/// when the size does not change.
pub fn scale_pixels(
    config: &VideoScalerConfig,
    resizer: &mut Resizer,
    pixel_data: &[u8],
    format: PixelFormat,
    w: u32,
    h: u32,
) -> Result<Option<(Vec<u8>, u32, u32)>, String> {
    let planar = matches!(
        format,
        PixelFormat::Yuv420p | PixelFormat::I420 | PixelFormat::NV12
    );
    let (dw, dh) = config.output_size(w, h, planar);
    if (dw, dh) == (w, h) {
        return Ok(None);
    }
    let opts = ResizeOptions::new()
        .resize_alg(resize_alg(&config.algorithm)?)
        .use_alpha(false);
    let need = |n: usize| -> Result<(), String> {
        if pixel_data.len() < n {
            Err(format!(
                "{format:?} {w}x{h} frame needs {n} bytes, got {}",
                pixel_data.len()
            ))
        } else {
            Ok(())
        }
    };
    let (wu, hu) = (w as usize, h as usize);
    let out = match format {
        PixelFormat::Rgb24 => {
            need(wu * hu * 3)?;
            resize_plane(resizer, &opts, &pixel_data[..wu * hu * 3], (w, h), (dw, dh), PixelType::U8x3)?
        }
        PixelFormat::Rgba32 => {
            need(wu * hu * 4)?;
            resize_plane(resizer, &opts, &pixel_data[..wu * hu * 4], (w, h), (dw, dh), PixelType::U8x4)?
        }
        PixelFormat::Yuv420p | PixelFormat::I420 | PixelFormat::NV12 => {
            if w % 2 == 1 || h % 2 == 1 {
                return Err(format!("{format:?} frame {w}x{h} has odd dimensions"));
            }
            let (cw, ch) = (w / 2, h / 2);
            let (dcw, dch) = (dw / 2, dh / 2);
            let y_len = wu * hu;
            let c_len = (cw * ch) as usize;
            need(y_len + 2 * c_len)?;
            let mut out = resize_plane(resizer, &opts, &pixel_data[..y_len], (w, h), (dw, dh), PixelType::U8)?;
            if format == PixelFormat::NV12 {
                out.extend(resize_plane(
                    resizer,
                    &opts,
                    &pixel_data[y_len..y_len + 2 * c_len],
                    (cw, ch),
                    (dcw, dch),
                    PixelType::U8x2,
                )?);
            } else {
                for plane in 0..2 {
                    let off = y_len + plane * c_len;
                    out.extend(resize_plane(
                        resizer,
                        &opts,
                        &pixel_data[off..off + c_len],
                        (cw, ch),
                        (dcw, dch),
                        PixelType::U8,
                    )?);
                }
            }
            out
        }
        other => {
            return Err(format!(
                "cannot scale {other:?} frames (raw rgb24, rgba32, yuv420p, i420 or nv12 only; decode encoded video first)"
            ))
        }
    };
    Ok(Some((out, dw, dh)))
}

/// The `VideoScaler` node.
pub struct VideoScalerNode {
    config: VideoScalerConfig,
    resizer: Arc<Mutex<Resizer>>,
}

impl VideoScalerNode {
    pub fn new(config: VideoScalerConfig) -> Result<Self, String> {
        resize_alg(&config.algorithm)?;
        Ok(Self {
            config,
            resizer: Arc::new(Mutex::new(Resizer::new())),
        })
    }

    async fn scale(&self, input: RuntimeData) -> Result<RuntimeData, String> {
        let RuntimeData::Video {
            pixel_data,
            width,
            height,
            format,
            codec,
            frame_number,
            timestamp_us,
            is_keyframe,
            stream_id,
            arrival_ts_us,
        } = input
        else {
            // Not video: pass it on untouched.
            return Ok(input);
        };
        if codec.is_some() || format == PixelFormat::Encoded {
            return Err("cannot scale encoded frames; decode them first".into());
        }
        let config = self.config.clone();
        let resizer = self.resizer.clone();
        // Resizing a large frame takes milliseconds: keep it off the runtime.
        let (pixel_data, width, height) = tokio::task::spawn_blocking(move || {
            let mut r = resizer.lock().unwrap_or_else(|p| p.into_inner());
            scale_pixels(&config, &mut r, &pixel_data, format, width, height)
                .map(|s| s.unwrap_or((pixel_data, width, height)))
        })
        .await
        .map_err(|e| format!("scaler task failed: {e}"))??;
        Ok(RuntimeData::Video {
            pixel_data,
            width,
            height,
            format,
            codec,
            frame_number,
            timestamp_us,
            is_keyframe,
            stream_id,
            arrival_ts_us,
        })
    }
}

#[async_trait]
impl AsyncStreamingNode for VideoScalerNode {
    fn node_type(&self) -> &str {
        "VideoScaler"
    }

    async fn process(&self, input: RuntimeData) -> Result<RuntimeData, Error> {
        self.scale(input)
            .await
            .map_err(|e| Error::Execution(format!("VideoScaler: {e}")))
    }

    async fn process_multi(
        &self,
        inputs: std::collections::HashMap<String, RuntimeData>,
    ) -> Result<RuntimeData, Error> {
        match inputs.into_iter().next() {
            Some((_, data)) => self.process(data).await,
            None => Err(Error::Execution("No input data provided".into())),
        }
    }

    fn is_multi_input(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn video(w: u32, h: u32, format: PixelFormat, data: Vec<u8>) -> RuntimeData {
        RuntimeData::Video {
            pixel_data: data,
            width: w,
            height: h,
            format,
            codec: None,
            frame_number: 7,
            timestamp_us: 1234,
            is_keyframe: true,
            stream_id: Some("screen".into()),
            arrival_ts_us: None,
        }
    }

    fn cfg(w: u32, h: u32) -> VideoScalerConfig {
        VideoScalerConfig {
            target_width: w,
            target_height: h,
            ..Default::default()
        }
    }

    #[test]
    fn output_sizes_follow_the_aspect_ratio_and_the_box() {
        // 5120x1440 (32:9) to 1920 wide.
        assert_eq!(cfg(1920, 0).output_size(5120, 1440, false), (1920, 540));
        assert_eq!(cfg(0, 720).output_size(1920, 1080, false), (1280, 720));
        // Fit inside 1280x720 keeping 32:9.
        assert_eq!(cfg(1280, 720).output_size(5120, 1440, false), (1280, 360));
        let stretch = VideoScalerConfig {
            maintain_aspect_ratio: false,
            ..cfg(640, 640)
        };
        assert_eq!(stretch.output_size(1920, 1080, false), (640, 640));
        let down = VideoScalerConfig {
            only_downscale: true,
            ..cfg(1920, 1080)
        };
        assert_eq!(down.output_size(640, 480, false), (640, 480));
        // 4:2:0 output is even.
        assert_eq!(cfg(0, 101).output_size(200, 100, true), (202, 100));
    }

    #[tokio::test]
    async fn rgb24_frames_are_resized_and_keep_their_metadata() {
        let node = VideoScalerNode::new(cfg(4, 0)).unwrap();
        let red = [255u8, 0, 0].repeat(8 * 2);
        let out = node
            .process(video(8, 2, PixelFormat::Rgb24, red))
            .await
            .unwrap();
        let RuntimeData::Video {
            pixel_data,
            width,
            height,
            format,
            stream_id,
            frame_number,
            ..
        } = out
        else {
            panic!("video out")
        };
        assert_eq!((width, height, format), (4, 1, PixelFormat::Rgb24));
        assert_eq!(pixel_data, [255u8, 0, 0].repeat(4));
        assert_eq!((stream_id.as_deref(), frame_number), (Some("screen"), 7));
    }

    #[tokio::test]
    async fn yuv420p_and_nv12_planes_are_scaled_separately() {
        let (w, h) = (16u32, 8u32);
        let y = vec![200u8; (w * h) as usize];
        let c = vec![100u8; (w * h / 4) as usize];
        let node = VideoScalerNode::new(cfg(8, 0)).unwrap();
        for format in [PixelFormat::Yuv420p, PixelFormat::NV12] {
            let data = [y.clone(), c.clone(), c.clone()].concat();
            let RuntimeData::Video {
                pixel_data,
                width,
                height,
                ..
            } = node.process(video(w, h, format, data)).await.unwrap()
            else {
                panic!("video out")
            };
            assert_eq!((width, height), (8, 4));
            assert_eq!(pixel_data.len(), 8 * 4 * 3 / 2, "{format:?}");
            assert!(pixel_data[..32].iter().all(|&v| v == 200));
            assert!(pixel_data[32..].iter().all(|&v| v == 100));
        }
    }

    #[tokio::test]
    async fn bad_input_is_reported_and_other_data_passes_through() {
        let node = VideoScalerNode::new(cfg(4, 0)).unwrap();
        let short = node
            .process(video(8, 2, PixelFormat::Rgb24, vec![0; 10]))
            .await
            .unwrap_err();
        assert!(short.to_string().contains("needs 48 bytes"), "{short}");
        assert!(VideoScalerNode::new(VideoScalerConfig {
            algorithm: "sharp".into(),
            ..cfg(4, 0)
        })
        .is_err());
        let text = node.process(RuntimeData::Text("hi".into())).await.unwrap();
        assert!(matches!(text, RuntimeData::Text(_)));
    }
}
