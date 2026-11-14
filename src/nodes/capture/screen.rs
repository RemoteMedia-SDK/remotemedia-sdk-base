//! `ScreenCapture`: a monitor or one application window as a video source.
//!
//! ```yaml
//! - id: screen
//!   node_type: ScreenCapture
//!   params: { fps: 10 }                      # primary monitor
//! - id: editor
//!   node_type: ScreenCapture
//!   params: { app: "code", fps: 5 }          # the largest window of an app
//! - id: call
//!   node_type: ScreenCapture
//!   params: { window: "Zoom Meeting" }       # a window by title
//! ```
//!
//! Backed by `xcap` (Windows, macOS, Linux X11/Wayland). A window is
//! captured even when other windows cover it; a minimized window produces
//! no frames until it is restored.

use super::{
    default_fps_10, matches, pace, spawn_capture_thread, CaptureBackend, CaptureSourceNode, Frame,
    FrameSlot, OpenResult, PixelOut,
};
use crate::capabilities::{CapabilityBehavior, MediaCapabilities};
use crate::nodes::streaming_node::{StreamingNode, StreamingNodeFactory};
use crate::Error;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// `ScreenCapture` params.
#[derive(Debug, Clone, Deserialize)]
pub struct ScreenCaptureConfig {
    /// Monitor index (0-based, as listed) or a substring of its name;
    /// default the primary monitor. Ignored when `window` or `app` is set.
    #[serde(default)]
    pub monitor: Option<Value>,
    /// Capture the window whose title contains this (case-insensitive).
    #[serde(default)]
    pub window: Option<String>,
    /// Capture a window of the application whose name contains this.
    #[serde(default)]
    pub app: Option<String>,
    #[serde(default = "default_fps_10")]
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

impl Default for ScreenCaptureConfig {
    fn default() -> Self {
        serde_json::from_value(json!({})).expect("defaults")
    }
}

enum Target {
    Monitor(xcap::Monitor),
    Window(xcap::Window),
}

impl Target {
    fn capture(&self) -> Result<xcap::image::RgbaImage, String> {
        match self {
            Target::Monitor(m) => m.capture_image(),
            Target::Window(w) => w.capture_image(),
        }
        .map_err(|e| e.to_string())
    }
}

fn describe_monitor(i: usize, m: &xcap::Monitor) -> String {
    format!(
        "#{i} '{}'{} {}x{}",
        m.friendly_name().or_else(|_| m.name()).unwrap_or_default(),
        if m.is_primary().unwrap_or(false) {
            " (primary)"
        } else {
            ""
        },
        m.width().unwrap_or(0),
        m.height().unwrap_or(0)
    )
}

fn describe_window(w: &xcap::Window) -> String {
    format!(
        "'{}' — {} ({}x{})",
        w.title().unwrap_or_default(),
        w.app_name().unwrap_or_default(),
        w.width().unwrap_or(0),
        w.height().unwrap_or(0)
    )
}

fn resolve(cfg: &ScreenCaptureConfig) -> Result<(Target, String), String> {
    if cfg.window.is_some() || cfg.app.is_some() {
        let all = xcap::Window::all().map_err(|e| format!("cannot list windows: {e}"))?;
        // Capturable: titled, not minimized, with an area.
        let visible: Vec<xcap::Window> = all
            .into_iter()
            .filter(|w| {
                !w.title().unwrap_or_default().is_empty()
                    && !w.is_minimized().unwrap_or(false)
                    && w.width().unwrap_or(0) > 0
                    && w.height().unwrap_or(0) > 0
            })
            .collect();
        let best = visible
            .iter()
            .filter(|w| {
                cfg.window
                    .as_deref()
                    .map(|t| matches(&w.title().unwrap_or_default(), t))
                    .unwrap_or(true)
                    && cfg
                        .app
                        .as_deref()
                        .map(|a| matches(&w.app_name().unwrap_or_default(), a))
                        .unwrap_or(true)
            })
            .max_by_key(|w| w.width().unwrap_or(0) as u64 * w.height().unwrap_or(0) as u64);
        return match best {
            Some(w) => {
                let d = describe_window(w);
                Ok((Target::Window(w.clone()), format!("window {d}")))
            }
            None => {
                let mut listed: Vec<String> =
                    visible.iter().take(20).map(describe_window).collect();
                if visible.len() > 20 {
                    listed.push(format!("… {} more", visible.len() - 20));
                }
                Err(format!(
                    "no open, non-minimized window matches{}{}. Open windows: {}",
                    cfg.window
                        .as_deref()
                        .map(|t| format!(" title '{t}'"))
                        .unwrap_or_default(),
                    cfg.app
                        .as_deref()
                        .map(|a| format!(" app '{a}'"))
                        .unwrap_or_default(),
                    if listed.is_empty() {
                        "none".into()
                    } else {
                        listed.join("; ")
                    }
                ))
            }
        };
    }
    let monitors = xcap::Monitor::all().map_err(|e| format!("cannot list monitors: {e}"))?;
    let listed = || {
        monitors
            .iter()
            .enumerate()
            .map(|(i, m)| describe_monitor(i, m))
            .collect::<Vec<_>>()
            .join("; ")
    };
    let pick = match &cfg.monitor {
        None | Some(Value::Null) => monitors
            .iter()
            .position(|m| m.is_primary().unwrap_or(false))
            .or(if monitors.is_empty() { None } else { Some(0) }),
        Some(Value::Number(n)) => n
            .as_u64()
            .map(|i| i as usize)
            .filter(|i| *i < monitors.len()),
        Some(Value::String(s)) => match s.parse::<usize>() {
            Ok(i) if i < monitors.len() => Some(i),
            _ => monitors.iter().position(|m| {
                matches(&m.name().unwrap_or_default(), s)
                    || matches(&m.friendly_name().unwrap_or_default(), s)
            }),
        },
        Some(other) => return Err(format!("`monitor` must be an index or a name, not {other}")),
    };
    match pick {
        Some(i) => Ok((
            Target::Monitor(monitors[i].clone()),
            format!("monitor {}", describe_monitor(i, &monitors[i])),
        )),
        None => Err(format!(
            "no monitor matches {}. Monitors: {}",
            cfg.monitor
                .as_ref()
                .map(|m| m.to_string())
                .unwrap_or_default(),
            listed()
        )),
    }
}

struct ScreenBackend {
    cfg: ScreenCaptureConfig,
}

impl CaptureBackend for ScreenBackend {
    fn node_type(&self) -> &'static str {
        "ScreenCapture"
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
        spawn_capture_thread("screen-capture", move || {
            let (target, desc) = match resolve(&cfg) {
                Ok(t) => t,
                Err(e) => {
                    let _ = opened.send(Err(e));
                    return;
                }
            };
            let period = Duration::from_secs_f64(1.0 / fps.max(1) as f64);
            let mut opened = Some(opened);
            let mut failures = 0u64;
            let mut meter = super::RateMeter::new();
            loop {
                let t0 = Instant::now();
                match target.capture() {
                    Ok(img) => {
                        if failures > 0 {
                            tracing::info!("capturing {desc} again after {failures} failed frames");
                        }
                        failures = 0;
                        meter.frame(&desc, fps);
                        let (width, height) = (img.width(), img.height());
                        let pixels = out.from_rgba(img.into_raw());
                        slot.put(
                            Frame {
                                pixels,
                                width,
                                height,
                            },
                            started.elapsed().as_micros() as u64,
                        );
                        if let Some(tx) = opened.take() {
                            let _ = tx.send(Ok(format!("{desc}, {width}x{height}")));
                        }
                    }
                    Err(e) => {
                        if let Some(tx) = opened.take() {
                            let _ = tx.send(Err(format!("cannot capture {desc}: {e}")));
                            return;
                        }
                        failures += 1;
                        // A closed or minimized window fails every frame:
                        // say so once, then every ~100 frames.
                        if failures == 1 || failures % 100 == 0 {
                            tracing::warn!(
                                failures,
                                "cannot capture {desc} (closed or minimized?): {e}"
                            );
                        }
                    }
                }
                if !pace(&slot, period, t0) {
                    break;
                }
            }
        });
    }
}

/// Factory for `ScreenCapture`.
pub struct ScreenCaptureNodeFactory;

impl StreamingNodeFactory for ScreenCaptureNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let cfg: ScreenCaptureConfig = serde_json::from_value(params.clone())
            .map_err(|e| Error::Execution(format!("ScreenCapture params: {e}")))?;
        let delivery = cfg.delivery;
        Ok(Box::new(
            CaptureSourceNode::new(
                node_id,
                cfg.fps,
                cfg.pixel_format,
                cfg.stream_id.clone(),
                Box::new(ScreenBackend { cfg }),
            )
            .with_delivery(delivery),
        ))
    }

    fn node_type(&self) -> &str {
        "ScreenCapture"
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::RuntimeDataType;
        Some(super::source_schema(
            "ScreenCapture",
            "Screen or app-window capture source: raw video frames (rgb24 by default) at fps, on its own clock from session start. Captures the primary monitor, a monitor by index or name, or one window by title (`window`) or application name (`app`); covered windows still capture, minimized ones pause. No inputs; it starts the pipeline.",
            "io",
            &[],
            &[RuntimeDataType::Video],
            json!({
                "monitor": {"type": ["integer", "string", "null"], "description": "Monitor index (0-based) or name substring; default the primary monitor"},
                "window": {"type": ["string", "null"], "description": "Capture the window whose title contains this (case-insensitive)"},
                "app": {"type": ["string", "null"], "description": "Capture the largest window of the application whose name contains this"},
                "fps": {"type": "integer", "minimum": 1, "maximum": 60, "default": 10},
                "pixel_format": {"type": "string", "enum": ["rgb24", "rgba32"], "default": "rgb24"},
                "stream_id": {"type": ["string", "null"], "description": "Video track id for multi-track routing"}
            }),
        ))
    }

    fn media_capabilities(&self, params: &Value) -> Option<MediaCapabilities> {
        let cfg: ScreenCaptureConfig = serde_json::from_value(params.clone()).unwrap_or_default();
        Some(CaptureSourceNode::caps(cfg.pixel_format, cfg.fps))
    }

    fn capability_behavior(&self) -> CapabilityBehavior {
        CapabilityBehavior::Configured
    }
}
