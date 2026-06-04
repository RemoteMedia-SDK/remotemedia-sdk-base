//! `VideoFileWriterNode` — writes incoming `RuntimeData::Video`
//! frames to a Y4M file on disk.
//!
//! Y4M (YUV4MPEG2) is the simplest standard video container that
//! ffmpeg / mpv read directly: a one-line ASCII header followed by
//! `FRAME\n` markers + raw planar YUV bytes per frame. No
//! per-chunk size fields means no Drop-time fixup is needed.
//!
//! ## Format
//!
//! `C420jpeg` (YUV420p, JPEG-range chroma siting). The renderer
//! emits RGB24 (per `Live2DRenderNode`); we convert to YUV420p
//! per frame using the standard BT.601 coefficients. Subsequent
//! ffmpeg conversion to H.264 / VP9 / etc. has no quality loss
//! beyond the RGB→YUV step.
//!
//! ## Lifecycle
//!
//! - First Video frame: writes Y4M header (captures width / height
//!   / fps from the first frame's metadata).
//! - Each Video frame: writes `FRAME\n` + Y plane + Cb plane + Cr
//!   plane.
//! - On `Drop`: simply closes the file (no fixup needed — Y4M has
//!   no length fields).
//!
//! ## Inferring fps
//!
//! `RuntimeData::Video` carries `timestamp_us` per frame. We
//! capture the first frame's pts as t0, then default to 30 fps
//! for the header. Mismatches between header fps and actual frame
//! cadence are not a correctness issue — Y4M readers tolerate
//! variable frame rate; the header is a pacing hint.

use crate::data::RuntimeData;
use crate::error::Result;
use crate::nodes::AsyncStreamingNode;
use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

/// Configuration for [`VideoFileWriterNode`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoFileWriterConfig {
    /// Output Y4M path. Parent directory is auto-created.
    pub output_path: PathBuf,
    /// Frame rate to stamp in the header. Defaults to 30; affects
    /// how downstream players pace playback. Real frame cadence is
    /// determined by the producer, not this header.
    #[serde(default = "default_fps")]
    pub fps: u32,
}

fn default_fps() -> u32 {
    30
}

/// Streaming node that writes incoming Video frames to a `.y4m`
/// file. Pass-through: emits the same Video frames it received so
/// it can sit on a tap edge without breaking the data flow.
pub struct VideoFileWriterNode {
    config: VideoFileWriterConfig,
    state: Arc<Mutex<WriterState>>,
}

struct WriterState {
    file: Option<File>,
    width: u32,
    height: u32,
    /// Frames written so far. Diagnostic only; Y4M doesn't store it.
    frames_written: u64,
}

impl VideoFileWriterNode {
    pub fn new(config: VideoFileWriterConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(WriterState {
                file: None,
                width: 0,
                height: 0,
                frames_written: 0,
            })),
            config,
        }
    }
}

impl std::fmt::Debug for VideoFileWriterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.state.lock();
        f.debug_struct("VideoFileWriterNode")
            .field("output_path", &self.config.output_path)
            .field("fps", &self.config.fps)
            .field("frames_written", &s.frames_written)
            .field("dims", &(s.width, s.height))
            .finish()
    }
}

#[async_trait]
impl AsyncStreamingNode for VideoFileWriterNode {
    fn node_type(&self) -> &str {
        "VideoFileWriterNode"
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        write_one(&self.state, &self.config, &data)?;
        Ok(data)
    }
}

fn write_one(
    state: &Mutex<WriterState>,
    config: &VideoFileWriterConfig,
    data: &RuntimeData,
) -> Result<()> {
    let RuntimeData::Video {
        pixel_data,
        width,
        height,
        format,
        ..
    } = data
    else {
        return Ok(()); // pass-through non-video
    };
    use crate::data::video::PixelFormat;
    if !matches!(format, PixelFormat::Rgb24) {
        tracing::warn!(
            "VideoFileWriterNode: dropping frame with format {:?} \
             (expected Rgb24)",
            format
        );
        return Ok(());
    }
    let expected_len = (*width as usize) * (*height as usize) * 3;
    if pixel_data.len() != expected_len {
        tracing::warn!(
            "VideoFileWriterNode: dropping frame with bad payload \
             length {} (expected {} for {}x{} RGB24)",
            pixel_data.len(),
            expected_len,
            width,
            height
        );
        return Ok(());
    }

    let mut s = state.lock();
    if s.file.is_none() {
        if let Some(parent) = config.output_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let mut f = File::create(&config.output_path).map_err(|e| {
            crate::Error::Execution(format!(
                "VideoFileWriterNode: open {:?}: {e}",
                config.output_path
            ))
        })?;
        write_y4m_header(&mut f, *width, *height, config.fps)?;
        s.file = Some(f);
        s.width = *width;
        s.height = *height;
    }

    if *width != s.width || *height != s.height {
        tracing::warn!(
            "VideoFileWriterNode: dropping frame with mismatched dimensions \
             ({}x{} vs initial {}x{})",
            width,
            height,
            s.width,
            s.height
        );
        return Ok(());
    }

    let f = s.file.as_mut().expect("file open after init");
    f.write_all(b"FRAME\n")
        .map_err(|e| crate::Error::Execution(format!("Y4M FRAME marker: {e}")))?;
    let yuv = crate::data::video::rgb24_to_yuv420p(pixel_data, *width as usize, *height as usize);
    f.write_all(&yuv)
        .map_err(|e| crate::Error::Execution(format!("Y4M frame data: {e}")))?;
    s.frames_written += 1;
    Ok(())
}

fn write_y4m_header(f: &mut File, width: u32, height: u32, fps: u32) -> Result<()> {
    // Standard Y4M header. Fields:
    //   W{width}    — frame width
    //   H{height}   — frame height
    //   F{n}:{d}    — frame rate as a rational (n/d)
    //   Ip          — progressive interlacing
    //   A1:1        — square pixel aspect
    //   C420jpeg    — YUV420p, JPEG-range chroma siting
    let header = format!(
        "YUV4MPEG2 W{} H{} F{}:1 Ip A1:1 C420jpeg\n",
        width, height, fps
    );
    f.write_all(header.as_bytes())
        .map_err(|e| crate::Error::Execution(format!("Y4M header: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::video::PixelFormat;

    fn rgb_frame(w: u32, h: u32, fill: u8) -> RuntimeData {
        RuntimeData::Video {
            pixel_data: vec![fill; (w * h * 3) as usize],
            width: w,
            height: h,
            format: PixelFormat::Rgb24,
            codec: None,
            frame_number: 0,
            timestamp_us: 0,
            is_keyframe: true,
            stream_id: None,
            arrival_ts_us: None,
        }
    }

    #[tokio::test]
    async fn writes_a_valid_y4m_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v.y4m");
        let node = VideoFileWriterNode::new(VideoFileWriterConfig {
            output_path: path.clone(),
            fps: 30,
        });
        node.process(rgb_frame(4, 4, 128)).await.unwrap();
        node.process(rgb_frame(4, 4, 64)).await.unwrap();
        node.process(rgb_frame(4, 4, 200)).await.unwrap();
        drop(node);

        let bytes = std::fs::read(&path).unwrap();
        // Header: starts with "YUV4MPEG2 ".
        assert!(bytes.starts_with(b"YUV4MPEG2 "));
        // Expected sizes: each 4x4 frame = 16 Y + 4 Cb + 4 Cr = 24 bytes
        // + 6-byte "FRAME\n" marker. 3 frames total = 90 bytes of payload.
        let expected_payload = 3 * (24 + 6);
        let header_end = bytes.iter().position(|&b| b == b'\n').unwrap() + 1;
        assert_eq!(
            bytes.len() - header_end,
            expected_payload,
            "expected {} bytes of payload after the header",
            expected_payload
        );
        // First frame marker.
        assert_eq!(&bytes[header_end..header_end + 6], b"FRAME\n");
    }

    #[tokio::test]
    async fn drops_mismatched_dim_frames() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("v.y4m");
        let node = VideoFileWriterNode::new(VideoFileWriterConfig {
            output_path: path.clone(),
            fps: 30,
        });
        node.process(rgb_frame(4, 4, 128)).await.unwrap();
        // 8x8 — different dimensions, dropped.
        node.process(rgb_frame(8, 8, 64)).await.unwrap();
        node.process(rgb_frame(4, 4, 200)).await.unwrap();
        drop(node);

        let bytes = std::fs::read(&path).unwrap();
        let header_end = bytes.iter().position(|&b| b == b'\n').unwrap() + 1;
        // 2 frames × 30 bytes each = 60 bytes of payload.
        assert_eq!(bytes.len() - header_end, 60);
    }

    #[test]
    fn rgb_to_yuv_white_is_full_luma() {
        let rgb = vec![255u8; 2 * 2 * 3]; // 2x2 white
        let yuv = crate::data::video::rgb24_to_yuv420p(&rgb, 2, 2);
        // Y plane = 4 bytes, all should be 255 (full luma).
        for &y in &yuv[..4] {
            assert!(y >= 254, "white pixel Y should be ~255, got {}", y);
        }
        // Cb / Cr should be ~128 (chroma neutral).
        assert!(
            yuv[4].abs_diff(128) < 2,
            "white Cb should be ~128, got {}",
            yuv[4]
        );
        assert!(
            yuv[5].abs_diff(128) < 2,
            "white Cr should be ~128, got {}",
            yuv[5]
        );
    }

    #[test]
    fn rgb_to_yuv_pure_red_chroma_check() {
        let rgb = vec![255u8, 0, 0, 255, 0, 0, 255, 0, 0, 255, 0, 0]; // 2x2 red
        let yuv = crate::data::video::rgb24_to_yuv420p(&rgb, 2, 2);
        // Y for pure red ≈ 0.299 * 255 ≈ 76.
        assert!(yuv[0].abs_diff(76) < 2);
        // Cb for pure red ≈ 128 - 0.169 * 255 ≈ 85.
        assert!(yuv[4].abs_diff(85) < 2);
        // Cr for pure red ≈ 128 + 0.5 * 255 = 255.5 → 255.
        assert!(yuv[5] > 250, "Cr for red should be ≥250, got {}", yuv[5]);
    }

    #[tokio::test]
    async fn non_video_input_is_passthrough_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("never.y4m");
        let node = VideoFileWriterNode::new(VideoFileWriterConfig {
            output_path: path.clone(),
            fps: 30,
        });
        node.process(RuntimeData::Text("hi".into())).await.unwrap();
        drop(node);
        assert!(
            !path.exists(),
            "file should not be created without video input"
        );
    }
}
