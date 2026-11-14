//! `WebcamCapture`: a camera as a video source.
//!
//! ```yaml
//! - id: cam
//!   node_type: WebcamCapture
//!   params: { device: 0, width: 1280, height: 720, fps: 30 }
//! ```
//!
//! Backed by `nokhwa` (Media Foundation on Windows, V4L2 on Linux,
//! AVFoundation on macOS). The camera format closest to the requested size
//! and rate is used; MJPEG / YUYV / NV12 frames are decoded to RGB.

use super::{
    default_fps_30, matches, spawn_capture_thread, CaptureBackend, CaptureSourceNode, Frame,
    FrameSlot, OpenResult, PixelOut,
};
use crate::capabilities::{CapabilityBehavior, MediaCapabilities};
use crate::nodes::streaming_node::{StreamingNode, StreamingNodeFactory};
use crate::Error;
use nokhwa::pixel_format::{RgbAFormat, RgbFormat};
use nokhwa::utils::{
    ApiBackend, CameraFormat, CameraIndex, FrameFormat, RequestedFormat, RequestedFormatType,
};
use nokhwa::Camera;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Instant;

fn default_width() -> u32 {
    1280
}

fn default_height() -> u32 {
    720
}

/// `WebcamCapture` params.
#[derive(Debug, Clone, Deserialize)]
pub struct WebcamCaptureConfig {
    /// Camera index (0-based, as listed) or a substring of its name;
    /// default the first camera.
    #[serde(default)]
    pub device: Option<Value>,
    #[serde(default = "default_width")]
    pub width: u32,
    #[serde(default = "default_height")]
    pub height: u32,
    #[serde(default = "default_fps_30")]
    pub fps: u32,
    #[serde(default)]
    pub pixel_format: PixelOut,
    #[serde(default)]
    pub stream_id: Option<String>,
    /// `"tick"` (default): the session clock polls for a frame `fps` times a
    /// second. `"body"`: each frame is emitted when it is captured.
    #[serde(default)]
    pub delivery: super::Delivery,
}

impl Default for WebcamCaptureConfig {
    fn default() -> Self {
        serde_json::from_value(json!({})).expect("defaults")
    }
}

/// The camera `device` selects, with every camera listed for errors.
fn resolve(device: &Option<Value>) -> Result<(CameraIndex, String), String> {
    let cams = nokhwa::query(ApiBackend::Auto).map_err(|e| format!("cannot list cameras: {e}"))?;
    let listed = || {
        if cams.is_empty() {
            return "none found".to_string();
        }
        cams.iter()
            .enumerate()
            .map(|(i, c)| format!("#{i} '{}'", c.human_name()))
            .collect::<Vec<_>>()
            .join("; ")
    };
    let pick = match device {
        None | Some(Value::Null) => (!cams.is_empty()).then_some(0),
        Some(Value::Number(n)) => n.as_u64().map(|i| i as usize).filter(|i| *i < cams.len()),
        Some(Value::String(s)) => match s.parse::<usize>() {
            Ok(i) if i < cams.len() => Some(i),
            _ => cams.iter().position(|c| matches(&c.human_name(), s)),
        },
        Some(other) => return Err(format!("`device` must be an index or a name, not {other}")),
    };
    match pick {
        Some(i) => Ok((cams[i].index().clone(), cams[i].human_name())),
        None => Err(format!(
            "no camera matches {}. Cameras: {}",
            device
                .as_ref()
                .map(|d| d.to_string())
                .unwrap_or_else(|| "(first)".into()),
            listed()
        )),
    }
}

/// Decode one camera buffer to `out`.
fn decode(buf: &nokhwa::Buffer, out: PixelOut) -> Result<Frame, String> {
    let res = buf.resolution();
    let pixels = match out {
        PixelOut::Rgb24 => buf.decode_image::<RgbFormat>().map(|i| i.into_raw()),
        PixelOut::Rgba32 => buf.decode_image::<RgbAFormat>().map(|i| i.into_raw()),
    }
    .map_err(|e| e.to_string())?;
    Ok(Frame {
        pixels,
        width: res.width_x,
        height: res.height_y,
    })
}

struct WebcamBackend {
    cfg: WebcamCaptureConfig,
}

impl CaptureBackend for WebcamBackend {
    fn node_type(&self) -> &'static str {
        "WebcamCapture"
    }

    fn spawn(
        &self,
        slot: Arc<FrameSlot>,
        out: PixelOut,
        fps: u32,
        started: Instant,
        opened: std::sync::mpsc::Sender<OpenResult>,
    ) {
        let cfg = self.cfg.clone();
        spawn_capture_thread("webcam-capture", move || {
            let open = || -> Result<(Camera, String), String> {
                let (index, name) = resolve(&cfg.device)?;
                let want = CameraFormat::new_from(cfg.width, cfg.height, FrameFormat::MJPEG, fps);
                let request = match out {
                    PixelOut::Rgb24 => {
                        RequestedFormat::new::<RgbFormat>(RequestedFormatType::Closest(want))
                    }
                    PixelOut::Rgba32 => {
                        RequestedFormat::new::<RgbAFormat>(RequestedFormatType::Closest(want))
                    }
                };
                let mut cam = Camera::new(index, request)
                    .map_err(|e| format!("cannot open camera '{name}': {e}"))?;
                cam.open_stream()
                    .map_err(|e| format!("cannot start camera '{name}': {e}"))?;
                // The frame rate the backend reports is not reliable (Media
                // Foundation can report 1); `RateMeter` logs the real one.
                let f = cam.camera_format();
                Ok((
                    cam,
                    format!(
                        "camera '{name}' {}x{} {}",
                        f.width(),
                        f.height(),
                        f.format()
                    ),
                ))
            };
            let (mut cam, desc) = match open() {
                Ok(c) => c,
                Err(e) => {
                    let _ = opened.send(Err(e));
                    return;
                }
            };
            let mut opened = Some(opened);
            let mut failures = 0u64;
            let mut meter = super::RateMeter::new();
            // `frame()` blocks until the camera delivers, so the camera's
            // own rate paces this loop.
            while !slot.stopped() {
                match cam
                    .frame()
                    .map_err(|e| e.to_string())
                    .and_then(|b| decode(&b, out))
                {
                    Ok(frame) => {
                        failures = 0;
                        meter.frame(&desc, fps);
                        slot.put(frame, started.elapsed().as_micros() as u64);
                        if let Some(tx) = opened.take() {
                            let _ = tx.send(Ok(desc.clone()));
                        }
                    }
                    Err(e) => {
                        if let Some(tx) = opened.take() {
                            let _ = tx.send(Err(format!("{desc}: first frame failed: {e}")));
                            break;
                        }
                        failures += 1;
                        if failures == 1 || failures % 100 == 0 {
                            tracing::warn!(failures, "{desc}: frame failed: {e}");
                        }
                        if failures > 300 {
                            tracing::error!("{desc}: giving up after {failures} failed frames");
                            break;
                        }
                    }
                }
            }
            let _ = cam.stop_stream();
        });
    }
}

/// Factory for `WebcamCapture`.
pub struct WebcamCaptureNodeFactory;

impl StreamingNodeFactory for WebcamCaptureNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let cfg: WebcamCaptureConfig = serde_json::from_value(params.clone())
            .map_err(|e| Error::Execution(format!("WebcamCapture params: {e}")))?;
        let delivery = cfg.delivery;
        Ok(Box::new(
            CaptureSourceNode::new(
                node_id,
                cfg.fps,
                cfg.pixel_format,
                cfg.stream_id.clone(),
                Box::new(WebcamBackend { cfg }),
            )
            .with_delivery(delivery),
        ))
    }

    fn node_type(&self) -> &str {
        "WebcamCapture"
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::RuntimeDataType;
        Some(super::source_schema(
            "WebcamCapture",
            "Webcam capture source: raw video frames (rgb24 by default) from a camera, using the camera format closest to width x height at fps, on its own clock from session start. No inputs; it starts the pipeline.",
            "io",
            &[],
            &[RuntimeDataType::Video],
            json!({
                "device": {"type": ["integer", "string", "null"], "description": "Camera index (0-based) or name substring; default the first camera"},
                "width": {"type": "integer", "minimum": 1, "default": 1280},
                "height": {"type": "integer", "minimum": 1, "default": 720},
                "fps": {"type": "integer", "minimum": 1, "maximum": 60, "default": 30},
                "pixel_format": {"type": "string", "enum": ["rgb24", "rgba32"], "default": "rgb24"},
                "stream_id": {"type": ["string", "null"], "description": "Video track id for multi-track routing"}
            }),
        ))
    }

    fn media_capabilities(&self, params: &Value) -> Option<MediaCapabilities> {
        let cfg: WebcamCaptureConfig = serde_json::from_value(params.clone()).unwrap_or_default();
        Some(CaptureSourceNode::caps(cfg.pixel_format, cfg.fps))
    }

    fn capability_behavior(&self) -> CapabilityBehavior {
        CapabilityBehavior::Configured
    }
}
