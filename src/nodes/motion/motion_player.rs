//! `MotionPlayerNode` — file-driven skeletal pose source.
//!
//! Loads a `.skeletal_pose.jsonl` (one pose per line, format produced by
//! `scripts/avatars/kimodo_pose_to_jsonl.py`) and re-emits each line as
//! a `RuntimeData::Json` envelope on the input-trigger heartbeat.
//!
//! Source semantics: has a single nominal input port so the manifest
//! connection graph can fan the user's Text packet to it as a "start
//! playback" trigger. Has no outbound dependency on input contents.

use crate::data::RuntimeData;
use crate::error::{Error, Result};
use crate::nodes::AsyncStreamingNode;
use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MotionPlayerConfig {
    /// Path to a `.skeletal_pose.jsonl` file (one pose per line,
    /// `kind = "skeletal_pose"`).
    pub jsonl_path: PathBuf,
    /// Output frame rate. When `pace_realtime: true`, the node sleeps
    /// `1000/fps` ms between envelope emissions so each pose actually
    /// reaches the renderer (whose `push_skeletal_pose` is a watch
    /// channel — latest-wins, drops bursts).
    #[serde(default = "default_fps")]
    pub fps: u32,
    /// If true, replay forever. Default false.
    #[serde(default)]
    pub loop_forever: bool,
    /// If true (default), sleep `1000/fps` ms between envelopes so the
    /// renderer's watch channel sees each pose. Set false in tests
    /// that just count envelopes.
    #[serde(default = "default_pace_realtime")]
    pub pace_realtime: bool,
}

fn default_fps() -> u32 {
    30
}

fn default_pace_realtime() -> bool {
    true
}

pub struct MotionPlayerNode {
    config: MotionPlayerConfig,
    state: Arc<Mutex<PlayerState>>,
}

struct PlayerState {
    /// Cached JSONL lines (already validated as `kind=skeletal_pose`
    /// at load time). Re-yielded on every trigger when `loop_forever`.
    lines: Vec<serde_json::Value>,
    loaded: bool,
}

impl MotionPlayerNode {
    pub fn new(config: MotionPlayerConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(PlayerState {
                lines: Vec::new(),
                loaded: false,
            })),
        }
    }

    fn load_if_needed(&self) -> Result<()> {
        let mut s = self.state.lock();
        if s.loaded {
            return Ok(());
        }
        let text = std::fs::read_to_string(&self.config.jsonl_path).map_err(|e| {
            Error::Execution(format!(
                "MotionPlayerNode: read {}: {e}",
                self.config.jsonl_path.display()
            ))
        })?;
        for (lineno, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let v: serde_json::Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        target: "motion_player",
                        "skipping malformed JSONL line {} ({}): {e}",
                        lineno + 1,
                        self.config.jsonl_path.display()
                    );
                    continue;
                }
            };
            if v.get("kind").and_then(|k| k.as_str()) != Some("skeletal_pose") {
                tracing::warn!(
                    target: "motion_player",
                    "skipping line {} — kind != skeletal_pose",
                    lineno + 1
                );
                continue;
            }
            s.lines.push(v);
        }
        tracing::info!(
            target: "motion_player",
            "loaded {} poses from {} (fps={}, loop={})",
            s.lines.len(),
            self.config.jsonl_path.display(),
            self.config.fps,
            self.config.loop_forever
        );
        s.loaded = true;
        Ok(())
    }
}

#[async_trait]
impl AsyncStreamingNode for MotionPlayerNode {
    fn node_type(&self) -> &str {
        "MotionPlayerNode"
    }

    async fn process(&self, _data: RuntimeData) -> Result<RuntimeData> {
        Err(Error::Execution(
            "MotionPlayerNode requires streaming mode — use process_streaming()".into(),
        ))
    }

    async fn process_streaming<F>(
        &self,
        _data: RuntimeData,
        _session_id: Option<String>,
        mut callback: F,
    ) -> Result<usize>
    where
        F: FnMut(RuntimeData) -> Result<()> + Send,
    {
        self.load_if_needed()?;
        let lines = {
            let s = self.state.lock();
            s.lines.clone()
        };
        if lines.is_empty() {
            return Ok(0);
        }

        let frame_period = Duration::from_secs_f64(1.0 / (self.config.fps.max(1) as f64));
        let mut emitted = 0usize;
        let mut next_emit = Instant::now();

        loop {
            for line in &lines {
                if self.config.pace_realtime {
                    let now = Instant::now();
                    if next_emit > now {
                        tokio::time::sleep(next_emit - now).await;
                    }
                    next_emit += frame_period;
                }
                let pts_ms = line
                    .get("pts_ms")
                    .and_then(|p| p.as_u64())
                    .map(|u| u as i64)
                    .unwrap_or(-1);
                tracing::info!(
                    target: "timing",
                    stage = "motion_player_emit",
                    pts_ms = pts_ms,
                    kind = "skeletal_pose",
                );
                callback(RuntimeData::Json(line.clone()))?;
                emitted += 1;
            }
            if !self.config.loop_forever {
                break;
            }
        }
        Ok(emitted)
    }
}
