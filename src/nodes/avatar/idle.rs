//! Source-wall idle animation generator.
//!
//! Self-paced node that emits a fresh idle pose envelope per tick at a
//! configurable rate (default 30 Hz). Designed to feed an `AvatarNode`'s
//! `idle_pose` snapshot input — the avatar reads the latest pose on
//! every video tick whenever no fresh `weights` snapshot is available
//! (silent or pre-speech intervals).
//!
//! The pose envelope is shaped as `RuntimeData::Json` carrying:
//! ```json
//! {
//!   "kind": "idle_pose",
//!   "frame_idx": <u64>,
//!   "pts_us": <u64>,
//!   "phase": <f32 in [0..2π) — phase of the breathing cycle>,
//!   "blink": <f32 in [0..1] — 1 during a blink, 0 otherwise>
//! }
//! ```
//!
//! The breathing curve is a simple sinusoid: `sin(phase)` returns a
//! [-1..1] amplitude scalar consumers map onto whatever blendshape they
//! want (typically a `breathe_in` weight from 0..1). The blink burst is
//! a deterministic Bernoulli-like trigger: every `blink_period_frames`
//! frames the next `blink_duration_frames` frames return 1.0.
//!
//! This node deliberately ships **without** any specific blendshape /
//! pose representation — that's the consumer's job. We only emit timing
//! signals (phase, blink). Real avatars project these onto whatever
//! their model expects.

use crate::data::RuntimeData;
use crate::nodes::ports::{OutputPort, PortKind, SnapshotPort};
#[cfg(test)]
use crate::nodes::NodeRuntimeContext;
use crate::nodes::{
    InitializeContextRead, NodeRuntimeContextRead, PacingNature, StreamingNode,
    StreamingNodeFactory, Tick,
};
use crate::Error;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Output port name for the idle pose snapshot. Consumers (e.g.
/// `AvatarNode`) read via `remotemedia_traits::runtime_context::snapshot::<IdlePose>(ctx, "idle_pose")`.
pub const IDLE_POSE_PORT: &str = "idle_pose";

/// Per-tick idle pose published on the `idle_pose` snapshot output port.
///
/// Consumers (typically `AvatarNode`) read the latest published pose on
/// every video tick. Fields mirror the JSON envelope this node emits on
/// its streaming output, but typed so downstream nodes don't pay for
/// JSON parse/serialize on the hot path.
#[derive(Debug, Clone)]
pub struct IdlePose {
    /// Sequential frame index — increments by 1 each tick. Wraps at
    /// `u64::MAX`.
    pub frame_idx: u64,
    /// Presentation timestamp (microseconds). Origin = session start.
    pub pts_us: u64,
    /// Phase of the breathing cycle in radians (`0..2π`). Map onto
    /// blendshape weights via `(phase.sin() + 1.0) / 2.0` for an
    /// `inhale` weight in `[0, 1]`.
    pub phase: f32,
    /// Blink trigger. `1.0` during a blink, `0.0` otherwise. Consumers
    /// can either gate a blendshape directly or smooth-step between
    /// neighbouring snapshots.
    pub blink: f32,
}

/// Default tick rate, suitable for 30 fps avatars. Override via
/// `params.rate_hz`.
pub const DEFAULT_IDLE_RATE_HZ: u32 = 30;

/// Default breathing cycle duration in seconds (full inhale-exhale).
pub const DEFAULT_BREATHE_PERIOD_SEC: f32 = 4.0;

/// Default blink cycle: one blink every 4 seconds at 30 fps = 120 frames.
pub const DEFAULT_BLINK_PERIOD_FRAMES: u64 = 120;

/// Default blink duration: ~100 ms at 30 fps = 3 frames.
pub const DEFAULT_BLINK_DURATION_FRAMES: u64 = 3;

#[derive(Debug, Clone)]
pub struct IdleAnimationConfig {
    pub rate_hz: u32,
    pub breathe_period_sec: f32,
    pub blink_period_frames: u64,
    pub blink_duration_frames: u64,
}

impl Default for IdleAnimationConfig {
    fn default() -> Self {
        Self {
            rate_hz: DEFAULT_IDLE_RATE_HZ,
            breathe_period_sec: DEFAULT_BREATHE_PERIOD_SEC,
            blink_period_frames: DEFAULT_BLINK_PERIOD_FRAMES,
            blink_duration_frames: DEFAULT_BLINK_DURATION_FRAMES,
        }
    }
}

impl IdleAnimationConfig {
    fn from_params(params: &Value) -> Self {
        let mut cfg = Self::default();
        if let Some(v) = params.get("rate_hz").and_then(|v| v.as_u64()) {
            if v > 0 {
                cfg.rate_hz = v as u32;
            }
        }
        if let Some(v) = params.get("breathe_period_sec").and_then(|v| v.as_f64()) {
            if v > 0.0 {
                cfg.breathe_period_sec = v as f32;
            }
        }
        if let Some(v) = params.get("blink_period_frames").and_then(|v| v.as_u64()) {
            if v > 0 {
                cfg.blink_period_frames = v;
            }
        }
        if let Some(v) = params.get("blink_duration_frames").and_then(|v| v.as_u64()) {
            cfg.blink_duration_frames = v;
        }
        cfg
    }
}

/// Wall-paced idle animation generator. See module docs for the output
/// envelope shape.
///
/// Two parallel outputs:
///
/// - **streaming** — every tick emits a `RuntimeData::Json` envelope
///   to the node's primary output, fanned out to manifest successors.
///   Useful for debug logging, recording, or text/event consumers that
///   want every frame.
/// - **snapshot port `idle_pose`** — every tick publishes a typed
///   [`IdlePose`] to an `OutputPort<IdlePose>`. Consumers (e.g.
///   `AvatarNode`) wire this in their manifest with
///   `to_port: "idle_pose"` and read via
///   `remotemedia_traits::runtime_context::snapshot::<IdlePose>(ctx, "idle_pose")` on their own clock.
pub struct IdleAnimationNode {
    node_id: String,
    config: IdleAnimationConfig,
    /// Tick counter. Wraps at `u64::MAX` (not a real concern — at 30 Hz
    /// that's ~19 billion years).
    frame_idx: AtomicU64,
    /// Snapshot output port. Cheap-cloneable handle around an
    /// `ArcSwapOption`; consumers wire to it via the session router's
    /// snapshot path.
    idle_pose_out: OutputPort<IdlePose>,
}

impl IdleAnimationNode {
    pub fn new(node_id: impl Into<String>, config: IdleAnimationConfig) -> Self {
        Self {
            node_id: node_id.into(),
            config,
            frame_idx: AtomicU64::new(0),
            idle_pose_out: OutputPort::empty(),
        }
    }

    pub fn from_params(node_id: impl Into<String>, params: &Value) -> Self {
        Self::new(node_id, IdleAnimationConfig::from_params(params))
    }
}

#[async_trait::async_trait]
impl StreamingNode for IdleAnimationNode {
    fn node_type(&self) -> &str {
        "IdleAnimationNode"
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn pacing_nature(&self) -> PacingNature {
        PacingNature::SourceWall(self.config.rate_hz)
    }

    fn is_multi_input(&self) -> bool {
        false
    }

    async fn initialize(&self, _ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        // No setup — pose synthesis is pure math driven by the tick.
        Ok(())
    }

    async fn process_async(
        &self,
        _data: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Err(Error::Execution(
            "IdleAnimationNode is SourceWall-paced; it does not consume reactive inputs".into(),
        ))
    }

    async fn process_multi_async(
        &self,
        _inputs: std::collections::HashMap<String, RuntimeData>,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Err(Error::Execution(
            "IdleAnimationNode is SourceWall-paced; it does not consume reactive inputs".into(),
        ))
    }

    async fn tick(
        &self,
        tick: Tick,
        _ctx: &dyn NodeRuntimeContextRead,
        mut cb: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<(), Error> {
        let frame = self.frame_idx.fetch_add(1, Ordering::Relaxed);

        // Breathing phase: wraps every `breathe_period_sec` * `rate_hz`
        // ticks. Computed in radians so consumers can plug into
        // `sin()` / `cos()` directly.
        let frames_per_breath =
            (self.config.breathe_period_sec * self.config.rate_hz as f32).max(1.0);
        let phase =
            ((frame as f32) % frames_per_breath) / frames_per_breath * std::f32::consts::TAU;

        // Blink burst: 1.0 during the first `blink_duration_frames` of
        // every `blink_period_frames`-frame window. 0.0 elsewhere.
        let blink = if self.config.blink_period_frames == 0 {
            0.0
        } else {
            let into_period = frame % self.config.blink_period_frames;
            if into_period < self.config.blink_duration_frames {
                1.0
            } else {
                0.0
            }
        };

        // Snapshot output: typed pose for tick-driven consumers
        // (AvatarNode). Atomic publish — never blocks. Consumers read
        // via `remotemedia_traits::runtime_context::snapshot::<IdlePose>(ctx, "idle_pose")`.
        self.idle_pose_out.publish(
            IdlePose {
                frame_idx: tick.frame_idx,
                pts_us: tick.pts_us,
                phase,
                blink,
            },
            tick.pts_us,
        );

        // Streaming output: same data as JSON for stream consumers
        // (debug taps, loggers, downstream nodes that don't tick).
        let pose = serde_json::json!({
            "kind": "idle_pose",
            "frame_idx": tick.frame_idx,
            "pts_us": tick.pts_us,
            "phase": phase,
            "blink": blink,
        });
        cb(RuntimeData::Json(pose))?;
        Ok(())
    }

    fn snapshot_outputs(&self) -> HashMap<String, Arc<dyn SnapshotPort>> {
        let mut out: HashMap<String, Arc<dyn SnapshotPort>> = HashMap::new();
        out.insert(
            IDLE_POSE_PORT.to_string(),
            Arc::new(self.idle_pose_out.input()),
        );
        out
    }
}

/// Factory for `IdleAnimationNode`. Reads `rate_hz`,
/// `breathe_period_sec`, `blink_period_frames`, `blink_duration_frames`
/// from `params`; missing keys fall back to module-level defaults.
pub struct IdleAnimationNodeFactory;

impl Default for IdleAnimationNodeFactory {
    fn default() -> Self {
        Self
    }
}

impl StreamingNodeFactory for IdleAnimationNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        Ok(Box::new(IdleAnimationNode::from_params(node_id, params)))
    }

    fn node_type(&self) -> &str {
        "IdleAnimationNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("IdleAnimationNode")
                .description("Source-wall idle animation generator — emits breathing/blinking idle pose envelopes")
                .category("avatar")
                .accepts([])
                .produces([RuntimeDataType::Json]),
        )
    }

    /// Declare the `idle_pose` output as a snapshot port. Producers
    /// publishing to a `Snapshot` port don't bypass the streaming path
    /// (the streaming envelope is still emitted to the primary output);
    /// this declaration makes intent explicit for tooling and matches
    /// what `snapshot_outputs()` exposes on the node instance.
    fn output_port_kinds(&self) -> HashMap<String, PortKind> {
        let mut m = HashMap::new();
        m.insert(IDLE_POSE_PORT.to_string(), PortKind::Snapshot);
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_from_params_overrides_defaults() {
        let params = serde_json::json!({
            "rate_hz": 60,
            "breathe_period_sec": 2.0,
            "blink_period_frames": 200,
            "blink_duration_frames": 5,
        });
        let cfg = IdleAnimationConfig::from_params(&params);
        assert_eq!(cfg.rate_hz, 60);
        assert!((cfg.breathe_period_sec - 2.0).abs() < f32::EPSILON);
        assert_eq!(cfg.blink_period_frames, 200);
        assert_eq!(cfg.blink_duration_frames, 5);
    }

    #[test]
    fn config_from_empty_params_uses_defaults() {
        let params = serde_json::json!({});
        let cfg = IdleAnimationConfig::from_params(&params);
        assert_eq!(cfg.rate_hz, DEFAULT_IDLE_RATE_HZ);
        assert_eq!(cfg.blink_period_frames, DEFAULT_BLINK_PERIOD_FRAMES);
    }

    #[test]
    fn config_from_params_clamps_invalid_values() {
        let params = serde_json::json!({ "rate_hz": 0, "breathe_period_sec": -1.0 });
        let cfg = IdleAnimationConfig::from_params(&params);
        assert_eq!(cfg.rate_hz, DEFAULT_IDLE_RATE_HZ);
        assert!((cfg.breathe_period_sec - DEFAULT_BREATHE_PERIOD_SEC).abs() < f32::EPSILON);
    }

    #[test]
    fn pacing_nature_reflects_configured_rate() {
        let node = IdleAnimationNode::new(
            "idle",
            IdleAnimationConfig {
                rate_hz: 60,
                ..Default::default()
            },
        );
        assert!(matches!(node.pacing_nature(), PacingNature::SourceWall(60)));
    }

    /// Simulate a sequence of ticks against the synthesis math directly:
    /// every `blink_period_frames` window starts with `blink_duration_frames`
    /// frames of `blink == 1.0`; the rest are 0.0. Phase wraps over
    /// `breathe_period_sec * rate_hz` frames.
    #[tokio::test]
    async fn tick_emits_well_formed_pose() {
        let node = IdleAnimationNode::new(
            "idle",
            IdleAnimationConfig {
                rate_hz: 30,
                breathe_period_sec: 1.0,
                blink_period_frames: 10,
                blink_duration_frames: 2,
            },
        );
        let ctx = NodeRuntimeContext::for_test("sess-1", "idle");

        let collected: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        for i in 0..10 {
            let tick = Tick {
                clock_id: "wall:30".to_string(),
                pts_us: i as u64 * 33_333,
                frame_idx: i,
                deadline_us: (i as u64 + 1) * 33_333,
            };
            let collected_clone = collected.clone();
            let cb: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send> =
                Box::new(move |out| {
                    if let RuntimeData::Json(v) = out {
                        collected_clone.lock().unwrap().push(v);
                    }
                    Ok(())
                });
            node.tick(tick, &ctx, cb).await.unwrap();
        }

        let out = collected.lock().unwrap();
        assert_eq!(out.len(), 10);
        // Frames 0..2 are the blink window; 2..10 are not.
        for (i, v) in out.iter().enumerate() {
            let blink = v.get("blink").and_then(|x| x.as_f64()).unwrap();
            if i < 2 {
                assert!(blink > 0.5, "frame {} should be blinking, got {}", i, blink);
            } else {
                assert!(
                    blink < 0.5,
                    "frame {} should not be blinking, got {}",
                    i,
                    blink
                );
            }
            assert_eq!(v.get("kind").and_then(|x| x.as_str()), Some("idle_pose"));
        }

        // Phase progresses monotonically over one breath cycle of 30 frames;
        // we only collected 10 frames so phase should be < TAU.
        let last_phase = out
            .last()
            .unwrap()
            .get("phase")
            .and_then(|x| x.as_f64())
            .unwrap();
        assert!(
            last_phase < std::f64::consts::TAU,
            "phase wrapped early: {}",
            last_phase
        );
        assert!(last_phase > 0.0, "phase didn't advance: {}", last_phase);
    }

    #[test]
    fn factory_creates_node_with_overridden_rate() {
        let factory = IdleAnimationNodeFactory;
        let node = factory
            .create(
                "idle".to_string(),
                &serde_json::json!({ "rate_hz": 60 }),
                None,
            )
            .unwrap();
        assert_eq!(node.node_type(), "IdleAnimationNode");
        assert!(matches!(node.pacing_nature(), PacingNature::SourceWall(60)));
    }
}
