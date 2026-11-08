//! Self-driven capture sources: `MicInput` (feature `microphone`, cpal),
//! `ScreenCapture` (`screen-capture`, xcap) and `WebcamCapture` (`webcam`,
//! nokhwa).
//!
//! The video sources below share [`CaptureSourceNode`]:
//!
//! A [`CaptureSourceNode`] owns a [`CaptureBackend`]. On `initialize` the
//! backend opens its device on a dedicated thread (device handles are often
//! `!Send`) and keeps publishing the latest frame into a [`FrameSlot`]. The
//! node declares [`PacingNature::SourceWall`] at its `fps`, so the session
//! ticks it from the moment it starts; each tick emits the newest frame if
//! one arrived since the last tick. A consumer that falls behind therefore
//! gets fewer, current frames instead of a growing backlog.

use crate::capabilities::{
    CapabilityBehavior, ConstraintValue, MediaCapabilities, MediaConstraints,
    PixelFormat as CapsPixelFormat, VideoConstraints,
};
use crate::data::{PixelFormat, RuntimeData};
use crate::nodes::streaming_node::{InitializeContextRead, NodeRuntimeContextRead, StreamingNode};
use crate::nodes::{PacingNature, Tick};
use crate::Error;
use async_trait::async_trait;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How long `initialize` waits for the device to deliver its first frame.
pub const OPEN_TIMEOUT: Duration = Duration::from_secs(15);

/// Pixel layout of emitted frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PixelOut {
    /// Packed RGB, 3 bytes per pixel (what `VideoFileWriterNode` takes).
    #[default]
    Rgb24,
    /// Packed RGBA, 4 bytes per pixel.
    Rgba32,
}

impl PixelOut {
    pub fn wire(self) -> PixelFormat {
        match self {
            PixelOut::Rgb24 => PixelFormat::Rgb24,
            PixelOut::Rgba32 => PixelFormat::Rgba32,
        }
    }

    fn caps(self) -> CapsPixelFormat {
        match self {
            PixelOut::Rgb24 => CapsPixelFormat::RGB24,
            PixelOut::Rgba32 => CapsPixelFormat::RGBA,
        }
    }

    /// `rgba` (4 bytes per pixel) in this layout.
    pub fn from_rgba(self, rgba: Vec<u8>) -> Vec<u8> {
        match self {
            PixelOut::Rgba32 => rgba,
            PixelOut::Rgb24 => {
                let mut rgb = Vec::with_capacity(rgba.len() / 4 * 3);
                for px in rgba.chunks_exact(4) {
                    rgb.extend_from_slice(&px[..3]);
                }
                rgb
            }
        }
    }
}

/// One captured frame, already in the node's output layout.
pub struct Frame {
    pub pixels: Vec<u8>,
    pub width: u32,
    pub height: u32,
}

/// The latest frame, shared between the capture thread and the node.
#[derive(Default)]
pub struct FrameSlot {
    latest: Mutex<Option<(Frame, u64)>>,
    stop: AtomicBool,
    /// Frames the capture thread produced.
    captured: AtomicU64,
}

impl FrameSlot {
    /// Publish a frame (replacing one the node has not taken yet).
    pub fn put(&self, frame: Frame, captured_us: u64) {
        self.captured.fetch_add(1, Ordering::Relaxed);
        *self.latest.lock().unwrap_or_else(|p| p.into_inner()) = Some((frame, captured_us));
    }

    fn take(&self) -> Option<(Frame, u64)> {
        self.latest.lock().unwrap_or_else(|p| p.into_inner()).take()
    }

    /// Set when the node is dropped; capture threads exit on it.
    pub fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }
}

/// What the capture thread reports once the device is open.
pub type OpenResult = Result<String, String>;

/// A device a [`CaptureSourceNode`] captures from.
pub trait CaptureBackend: Send + Sync + 'static {
    fn node_type(&self) -> &'static str;
    /// Spawn the capture thread. It opens the device, sends `Ok(description)`
    /// (or `Err(why, with what is available)`) on `opened` once the first
    /// frame is in `slot`, then keeps capturing about `fps` times a second
    /// until `slot.stopped()`.
    fn spawn(
        &self,
        slot: Arc<FrameSlot>,
        out: PixelOut,
        fps: u32,
        started: Instant,
        opened: std::sync::mpsc::Sender<OpenResult>,
    );
}

/// Run `body` on a named thread inside the caller's tracing span (so the
/// thread's logs belong to the node, see `rm_node_logs`).
pub fn spawn_capture_thread(name: &str, body: impl FnOnce() + Send + 'static) {
    let span = tracing::Span::current();
    let _ = std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            let _span = span.entered();
            body();
        });
}

/// Sleeps the remainder of one frame period; returns `false` once stopped.
pub fn pace(slot: &FrameSlot, period: Duration, frame_started: Instant) -> bool {
    if let Some(left) = period.checked_sub(frame_started.elapsed()) {
        std::thread::sleep(left);
    }
    !slot.stopped()
}

/// Logs, once, how many frames per second a device actually delivers
/// (after [`RateMeter::WINDOW`]): cameras slow down in low light and a large
/// screen may not keep up with the requested `fps`.
pub struct RateMeter {
    started: Option<Instant>,
    frames: u32,
    done: bool,
}

impl RateMeter {
    pub const WINDOW: Duration = Duration::from_secs(2);

    pub fn new() -> Self {
        Self {
            started: None,
            frames: 0,
            done: false,
        }
    }

    /// Count one frame of `what`, requested at `fps`.
    pub fn frame(&mut self, what: &str, fps: u32) {
        if self.done {
            return;
        }
        let started = *self.started.get_or_insert_with(Instant::now);
        self.frames += 1;
        let el = started.elapsed();
        if el >= Self::WINDOW {
            self.done = true;
            let rate = (self.frames - 1) as f64 / el.as_secs_f64();
            if rate < fps as f64 * 0.9 {
                tracing::warn!("{what} delivers {rate:.1} fps (requested {fps})");
            } else {
                tracing::info!("{what} delivers {rate:.1} fps");
            }
        }
    }
}

impl Default for RateMeter {
    fn default() -> Self {
        Self::new()
    }
}

/// A self-driven video source.
pub struct CaptureSourceNode {
    node_id: String,
    fps: u32,
    out: PixelOut,
    stream_id: Option<String>,
    backend: Box<dyn CaptureBackend>,
    slot: Arc<FrameSlot>,
    frame_number: AtomicU64,
    started: Instant,
}

impl CaptureSourceNode {
    pub fn new(
        node_id: String,
        fps: u32,
        out: PixelOut,
        stream_id: Option<String>,
        backend: Box<dyn CaptureBackend>,
    ) -> Self {
        Self {
            node_id,
            fps: fps.clamp(1, 60),
            out,
            stream_id,
            backend,
            slot: Arc::new(FrameSlot::default()),
            frame_number: AtomicU64::new(0),
            started: Instant::now(),
        }
    }

    /// Output capabilities: raw video in `out` at `fps`; the frame size is
    /// only known once the device is open.
    pub fn caps(out: PixelOut, fps: u32) -> MediaCapabilities {
        MediaCapabilities::with_output(MediaConstraints::Video(VideoConstraints {
            width: None,
            height: None,
            framerate: Some(ConstraintValue::Exact(fps.clamp(1, 60) as f32)),
            pixel_format: Some(ConstraintValue::Exact(out.caps())),
        }))
    }
}

impl Drop for CaptureSourceNode {
    fn drop(&mut self) {
        self.slot.stop.store(true, Ordering::Relaxed);
    }
}

#[async_trait]
impl StreamingNode for CaptureSourceNode {
    fn node_type(&self) -> &str {
        self.backend.node_type()
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }

    async fn initialize(&self, _ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.backend
            .spawn(self.slot.clone(), self.out, self.fps, self.started, tx);
        let ty = self.backend.node_type();
        let opened = tokio::task::spawn_blocking(move || rx.recv_timeout(OPEN_TIMEOUT))
            .await
            .map_err(|e| Error::Execution(format!("{ty}: capture thread: {e}")))?;
        match opened {
            Ok(Ok(desc)) => {
                tracing::info!("{ty} capturing {desc} at {} fps", self.fps);
                Ok(())
            }
            Ok(Err(why)) => Err(Error::Execution(format!("{ty}: {why}"))),
            Err(_) => {
                self.slot.stop.store(true, Ordering::Relaxed);
                Err(Error::Execution(format!(
                    "{ty}: no frame within {}s of opening the device",
                    OPEN_TIMEOUT.as_secs()
                )))
            }
        }
    }

    async fn process_async(
        &self,
        _data: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Err(Error::Execution(format!(
            "{} is a source; it does not take inputs",
            self.backend.node_type()
        )))
    }

    async fn process_multi_async(
        &self,
        _inputs: HashMap<String, RuntimeData>,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        self.process_async(RuntimeData::Text(String::new()), ctx)
            .await
    }

    fn is_multi_input(&self) -> bool {
        false
    }

    /// Inputs are ignored: the node runs on its own clock ([`Self::tick`]).
    async fn process_streaming_async(
        &self,
        _data: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
        _callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<usize, Error> {
        Ok(0)
    }

    fn pacing_nature(&self) -> PacingNature {
        PacingNature::SourceWall(self.fps)
    }

    async fn tick(
        &self,
        _tick: Tick,
        _ctx: &dyn NodeRuntimeContextRead,
        mut cb: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<(), Error> {
        let Some((frame, captured_us)) = self.slot.take() else {
            return Ok(());
        };
        let frame_number = self.frame_number.fetch_add(1, Ordering::Relaxed);
        cb(RuntimeData::Video {
            pixel_data: frame.pixels,
            width: frame.width,
            height: frame.height,
            format: self.out.wire(),
            codec: None,
            frame_number,
            timestamp_us: captured_us,
            is_keyframe: true,
            stream_id: self.stream_id.clone(),
            arrival_ts_us: None,
        })
    }

    fn media_capabilities(&self) -> Option<MediaCapabilities> {
        Some(Self::caps(self.out, self.fps))
    }

    fn capability_behavior(&self) -> CapabilityBehavior {
        CapabilityBehavior::Configured
    }
}

/// Default `fps` of `ScreenCapture`.
pub fn default_fps_10() -> u32 {
    10
}

/// Default `fps` of `WebcamCapture`.
pub fn default_fps_30() -> u32 {
    30
}

/// Matches `needle` (case-insensitive substring) against `hay`.
pub fn matches(hay: &str, needle: &str) -> bool {
    hay.to_lowercase().contains(&needle.to_lowercase())
}

#[cfg(feature = "microphone")]
pub mod mic;
#[cfg(feature = "screen-capture")]
pub mod screen;
#[cfg(feature = "webcam")]
pub mod webcam;

#[cfg(feature = "microphone")]
pub use mic::{MicInputConfig, MicInputNodeFactory, MicInputStreamingNode};
#[cfg(feature = "screen-capture")]
pub use screen::{ScreenCaptureConfig, ScreenCaptureNodeFactory};
#[cfg(feature = "webcam")]
pub use webcam::{WebcamCaptureConfig, WebcamCaptureNodeFactory};

/// Schema of a capture source: no inputs, video out, closed params.
pub(crate) fn source_schema(
    node_type: &str,
    description: &str,
    category: &str,
    accepts: &[crate::nodes::schema::RuntimeDataType],
    produces: &[crate::nodes::schema::RuntimeDataType],
    properties: serde_json::Value,
) -> crate::nodes::schema::NodeSchema {
    crate::nodes::schema::NodeSchema::new(node_type)
        .description(description)
        .category(category)
        .accepts(accepts.iter().cloned())
        .produces(produces.iter().cloned())
        .config_schema(serde_json::json!({
            "type": "object",
            "properties": properties,
            "additionalProperties": false
        }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rgba_is_converted_to_the_output_layout() {
        let rgba = vec![1, 2, 3, 255, 4, 5, 6, 255];
        assert_eq!(
            PixelOut::Rgb24.from_rgba(rgba.clone()),
            vec![1, 2, 3, 4, 5, 6]
        );
        assert_eq!(PixelOut::Rgba32.from_rgba(rgba.clone()), rgba);
    }

    #[test]
    fn the_slot_keeps_only_the_newest_frame() {
        let slot = FrameSlot::default();
        for w in 1..=3 {
            slot.put(
                Frame {
                    pixels: vec![],
                    width: w,
                    height: 1,
                },
                w as u64,
            );
        }
        let (f, ts) = slot.take().unwrap();
        assert_eq!((f.width, ts), (3, 3));
        assert!(slot.take().is_none());
        assert_eq!(slot.captured.load(Ordering::Relaxed), 3);
    }
}
