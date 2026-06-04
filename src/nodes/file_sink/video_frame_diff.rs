//! `VideoFrameDiffNode` — diagnostic pass-through that hashes each
//! incoming `RuntimeData::Video` frame's pixels and logs whether
//! consecutive frames actually differ.
//!
//! Use case: the avatar pipeline was producing visually-static video
//! despite blendshapes + pose params clearly varying per tick. Drop
//! this node on the renderer's output edge to confirm whether the
//! bytes leaving the renderer are actually changing or whether the
//! whole render path is emitting one frame repeatedly.
//!
//! Logging:
//! - First frame: prints "first frame, hash=<…>".
//! - Each subsequent frame: increments either `same` or `differ`.
//! - Every N (default 30) frames: emits a one-line summary
//!   `n=<N> same=<S> differ=<D> recent_hash=<h>`.
//! - On Drop: emits a final summary so the test log captures the
//!   total even when the renderer stops mid-collection.
//!
//! Pass-through: emits the input unchanged so it can sit on a tap
//! edge between the renderer and a downstream sink (e.g. the y4m
//! writer) without breaking the chain.

use crate::data::RuntimeData;
use crate::error::Result;
use crate::nodes::AsyncStreamingNode;
use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Configuration for [`VideoFrameDiffNode`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoFrameDiffConfig {
    /// Print a summary line every `log_every` frames (default 30 = 1s @ 30fps).
    #[serde(default = "default_log_every")]
    pub log_every: u64,
    /// Optional label included in every log line so multi-tap pipelines
    /// can be told apart in the same log stream.
    #[serde(default)]
    pub label: Option<String>,
}

fn default_log_every() -> u64 {
    30
}

impl Default for VideoFrameDiffConfig {
    fn default() -> Self {
        Self {
            log_every: default_log_every(),
            label: None,
        }
    }
}

/// Diagnostic pass-through that compares consecutive Video frames.
pub struct VideoFrameDiffNode {
    config: VideoFrameDiffConfig,
    state: Arc<Mutex<DiffState>>,
}

#[derive(Default)]
struct DiffState {
    last_hash: Option<u64>,
    n: u64,
    same: u64,
    differ: u64,
}

impl VideoFrameDiffNode {
    pub fn new(config: VideoFrameDiffConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(DiffState::default())),
        }
    }

    fn label(&self) -> &str {
        self.config.label.as_deref().unwrap_or("video_frame_diff")
    }
}

impl Drop for VideoFrameDiffNode {
    fn drop(&mut self) {
        let s = self.state.lock();
        eprintln!(
            "[{} drop] total frames={} same={} differ={} (last_hash={:?})",
            self.label(),
            s.n,
            s.same,
            s.differ,
            s.last_hash
        );
    }
}

/// FNV-1a 64-bit hash. Cheap and good enough to detect any pixel
/// change at all — we don't need cryptographic strength here, just
/// "did anything move?". Folded over the whole pixel buffer.
fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

#[async_trait]
impl AsyncStreamingNode for VideoFrameDiffNode {
    fn node_type(&self) -> &str {
        "VideoFrameDiffNode"
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        Ok(data)
    }

    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        _session_id: Option<String>,
        mut callback: F,
    ) -> Result<usize>
    where
        F: FnMut(RuntimeData) -> Result<()> + Send,
    {
        if let RuntimeData::Video {
            ref pixel_data,
            width,
            height,
            ref format,
            frame_number,
            ..
        } = data
        {
            let h = fnv1a_64(pixel_data);
            let mut s = self.state.lock();
            s.n += 1;

            let label = self.label();
            match s.last_hash {
                None => {
                    eprintln!(
                        "[{}] first frame: #{} {}x{} {:?} {} bytes hash={:#018x}",
                        label,
                        frame_number,
                        width,
                        height,
                        format,
                        pixel_data.len(),
                        h
                    );
                }
                Some(prev) => {
                    if prev == h {
                        s.same += 1;
                    } else {
                        s.differ += 1;
                    }
                }
            }
            s.last_hash = Some(h);

            if s.n % self.config.log_every == 0 {
                eprintln!(
                    "[{}] n={} same={} differ={} recent_hash={:#018x} (frame_number={})",
                    label, s.n, s.same, s.differ, h, frame_number
                );
            }
        }

        callback(data)?;
        Ok(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::video::PixelFormat;

    fn mk_video(stream_id: &str, frame_number: u64, fill: u8) -> RuntimeData {
        RuntimeData::Video {
            pixel_data: vec![fill; 4 * 4 * 3],
            width: 4,
            height: 4,
            format: PixelFormat::Rgb24,
            codec: None,
            frame_number,
            timestamp_us: frame_number * 33_333,
            is_keyframe: true,
            stream_id: Some(stream_id.to_string()),
            arrival_ts_us: None,
        }
    }

    #[tokio::test]
    async fn passthrough_and_counts_consecutive_duplicates() {
        let node = VideoFrameDiffNode::new(VideoFrameDiffConfig {
            log_every: 1,
            label: Some("test".into()),
        });
        let mut count = 0usize;
        let mut callback = |out: RuntimeData| -> Result<()> {
            assert!(matches!(out, RuntimeData::Video { .. }));
            count += 1;
            Ok(())
        };
        node.process_streaming(mk_video("a", 0, 10), None, &mut callback)
            .await
            .unwrap();
        node.process_streaming(mk_video("a", 1, 10), None, &mut callback)
            .await
            .unwrap();
        node.process_streaming(mk_video("a", 2, 20), None, &mut callback)
            .await
            .unwrap();
        assert_eq!(count, 3);
        let s = node.state.lock();
        assert_eq!(s.n, 3);
        assert_eq!(s.same, 1, "frames 0+1 are identical");
        assert_eq!(s.differ, 1, "frames 1→2 differ");
    }

    #[test]
    fn fnv1a_changes_with_input() {
        assert_ne!(fnv1a_64(&[1, 2, 3]), fnv1a_64(&[1, 2, 4]));
        assert_eq!(fnv1a_64(&[]), 0xcbf29ce484222325);
    }
}
