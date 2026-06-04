//! Tick-driven avatar render node.
//!
//! Reads two snapshot inputs on every tick:
//!
//! - **`weights`** — typed [`AvatarWeights`] published by an
//!   `Audio2FaceNode` (or any reactive lipsync producer). When fresh,
//!   the avatar renders a *speaking* frame derived from the published
//!   blendshape weights.
//! - **`idle_pose`** — typed [`crate::nodes::avatar::IdlePose`] published
//!   by `IdleAnimationNode`. Read when no fresh `weights` are available
//!   (silent intervals, pre-speech).
//!
//! The render path itself is intentionally a stub. This skeleton ships
//! to validate two things: **(a)** snapshot reads on the tick path
//! work end-to-end via the manifest-wired `NodeRuntimeContext::snapshot`,
//! and **(b)** freshness gating between `weights` and `idle_pose`
//! correctly picks the speaking-vs-idle path. The actual rendering — a
//! GPU-side blendshape application + frame encode — lives in a follow-up
//! PR alongside the wire-bound clock tap path (spec Phase 5.4).
//!
//! ## Pacing
//!
//! Spec target is `PacingNature::ClockedToOutboundMedia` (driven by the
//! outbound video stream's wire clock). Until Phase 5.4 lands the
//! wire-bound pacer, this skeleton declares `SourceWall(rate_hz)` so
//! it self-paces and is testable in headless setups (no WebRTC peer
//! required). The migration to ClockedToOutboundMedia is a one-line
//! change in `pacing_nature()` once the dedicated Pacer module exists;
//! every other contract (snapshot reads, freshness gating, render path
//! selection, `RuntimeData::Video` shape) stays identical.
//!
//! ## Output
//!
//! Each tick emits one `RuntimeData::Video` placeholder frame to the
//! streaming output. `pixel_data` is a deterministic 1-byte payload that
//! encodes the chosen render mode so consumers can verify the
//! speaking/idle decision in tests:
//!
//! - `[0xAA]` — speaking (fresh weights snapshot was used)
//! - `[0xBB]` — idle (idle_pose snapshot was used)
//! - `[0x00]` — neither input had a snapshot yet (cold start)
//!
//! Real renders will populate `pixel_data` with raw or encoded frames
//! and set `format` / `width` / `height` accordingly.

use crate::data::video::{PixelFormat, VideoCodec};
use crate::data::RuntimeData;
use crate::nodes::avatar::IdlePose;
use crate::nodes::ports::PortKind;
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

/// Default tick rate when no wire clock is bound. 30 fps matches the
/// `IdleAnimationNode` default and the typical avatar render budget.
pub const DEFAULT_AVATAR_RATE_HZ: u32 = 30;

/// Default freshness window for the `weights` snapshot, in microseconds.
/// When the latest published weights' `pts_us` is older than this
/// relative to the current tick's `pts_us`, the avatar falls back to
/// the idle path.
///
/// 100 ms = 3 frames at 30 fps. Generous enough to absorb a single
/// missed Audio2Face inference, tight enough that a stalled lipsync
/// stream doesn't keep the avatar speaking after audio has stopped.
pub const DEFAULT_WEIGHTS_FRESH_US: u64 = 100_000;

/// Input port name for the lipsync weights snapshot.
pub const WEIGHTS_PORT: &str = "weights";

/// Input port name for the idle pose snapshot. Conventionally the same
/// name as `IdleAnimationNode`'s output port.
pub const IDLE_POSE_PORT: &str = "idle_pose";

/// Lipsync blendshape weights published on the `weights` snapshot port.
///
/// Producers (typically `Audio2FaceNode`) publish one snapshot per
/// inference output; the avatar reads the latest on every tick. Field
/// shape is intentionally minimal for the skeleton — full ARKit-style
/// blendshape vectors land with the real Audio2Face implementation.
#[derive(Debug, Clone)]
pub struct AvatarWeights {
    /// Lipsync amplitude, `0..1`. Drives a generic "mouth open" weight
    /// in the absence of a full blendshape map.
    pub mouth_open: f32,
    /// Reserved for the full blendshape vector. Empty in the skeleton;
    /// real producers populate this with `(name, weight)` pairs from
    /// the model's output layer.
    pub blendshapes: Vec<(String, f32)>,
}

/// Per-tick render decision the avatar picked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenderMode {
    /// Fresh `weights` snapshot was used. Sentinel byte: `0xAA`.
    Speaking,
    /// `weights` was missing or stale; `idle_pose` was used. Sentinel
    /// byte: `0xBB`.
    Idle,
    /// Neither input had a snapshot yet (cold start before first
    /// publish on either side). Sentinel byte: `0x00`. Real renders
    /// would emit a default neutral pose here.
    Cold,
}

impl RenderMode {
    /// Single-byte payload encoded into the placeholder frame so
    /// integration tests can assert which path the avatar took on
    /// each tick.
    pub fn sentinel_byte(self) -> u8 {
        match self {
            RenderMode::Speaking => 0xAA,
            RenderMode::Idle => 0xBB,
            RenderMode::Cold => 0x00,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AvatarConfig {
    pub rate_hz: u32,
    pub weights_fresh_us: u64,
    pub width: u32,
    pub height: u32,
}

impl Default for AvatarConfig {
    fn default() -> Self {
        Self {
            rate_hz: DEFAULT_AVATAR_RATE_HZ,
            weights_fresh_us: DEFAULT_WEIGHTS_FRESH_US,
            width: 512,
            height: 512,
        }
    }
}

impl AvatarConfig {
    fn from_params(params: &Value) -> Self {
        let mut cfg = Self::default();
        if let Some(v) = params.get("rate_hz").and_then(|v| v.as_u64()) {
            if v > 0 {
                cfg.rate_hz = v as u32;
            }
        }
        if let Some(v) = params.get("weights_fresh_us").and_then(|v| v.as_u64()) {
            cfg.weights_fresh_us = v;
        }
        if let Some(v) = params.get("width").and_then(|v| v.as_u64()) {
            cfg.width = v as u32;
        }
        if let Some(v) = params.get("height").and_then(|v| v.as_u64()) {
            cfg.height = v as u32;
        }
        cfg
    }
}

/// Picks the render mode for a tick given the latest snapshot states.
///
/// `weights` is preferred when its `pts_us` falls within
/// `[tick_pts_us - fresh_us, tick_pts_us + fresh_us]` of the tick (the
/// upper bound covers slight clock skew between the producer and the
/// pacer; the lower bound is the actual freshness gate). Otherwise falls
/// back to `idle_pose` when present, or to `Cold` when neither input has
/// published yet.
///
/// Pure function — pulled out so unit tests can pin the gating logic
/// without spinning up the full session router.
fn pick_render_mode(
    tick_pts_us: u64,
    weights_pts_us: Option<u64>,
    idle_present: bool,
    fresh_us: u64,
) -> RenderMode {
    if let Some(w_pts) = weights_pts_us {
        // Fresh window: within `fresh_us` on either side of the tick's
        // PTS. `saturating_sub` prevents underflow before the producer
        // ever publishes (when tick_pts_us is small).
        let lower = tick_pts_us.saturating_sub(fresh_us);
        let upper = tick_pts_us.saturating_add(fresh_us);
        if w_pts >= lower && w_pts <= upper {
            return RenderMode::Speaking;
        }
    }
    if idle_present {
        RenderMode::Idle
    } else {
        RenderMode::Cold
    }
}

/// Tick-driven avatar render node.
pub struct AvatarNode {
    node_id: String,
    config: AvatarConfig,
    /// Monotonic frame counter for the `RuntimeData::Video::frame_number`
    /// field. Wraps at `u64::MAX`.
    frame_number: AtomicU64,
}

impl AvatarNode {
    pub fn new(node_id: impl Into<String>, config: AvatarConfig) -> Self {
        Self {
            node_id: node_id.into(),
            config,
            frame_number: AtomicU64::new(0),
        }
    }

    pub fn from_params(node_id: impl Into<String>, params: &Value) -> Self {
        Self::new(node_id, AvatarConfig::from_params(params))
    }

    fn build_video_frame(&self, tick: &Tick, mode: RenderMode) -> RuntimeData {
        let frame_number = self.frame_number.fetch_add(1, Ordering::Relaxed);
        // Placeholder pixel payload: a single byte encoding the render
        // mode so downstream tests can assert which path was taken.
        // Real renders will populate this with raw or encoded frames.
        RuntimeData::Video {
            pixel_data: vec![mode.sentinel_byte()],
            width: self.config.width,
            height: self.config.height,
            format: PixelFormat::Rgb24,
            codec: None,
            frame_number,
            timestamp_us: tick.pts_us,
            is_keyframe: matches!(mode, RenderMode::Cold) || frame_number == 0,
            stream_id: None,
            arrival_ts_us: None,
        }
    }
}

#[async_trait::async_trait]
impl StreamingNode for AvatarNode {
    fn node_type(&self) -> &str {
        "AvatarNode"
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn pacing_nature(&self) -> PacingNature {
        // Spec target is ClockedToOutboundMedia; until Phase 5.4 ships
        // the wire-bound pacer this skeleton self-paces via the wall
        // pacer at the configured rate. See module docs.
        PacingNature::SourceWall(self.config.rate_hz)
    }

    fn is_multi_input(&self) -> bool {
        false
    }

    async fn initialize(&self, _ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        Ok(())
    }

    async fn process_async(
        &self,
        _data: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Err(Error::Execution(
            "AvatarNode is tick-driven; does not consume reactive inputs".into(),
        ))
    }

    async fn process_multi_async(
        &self,
        _inputs: HashMap<String, RuntimeData>,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Err(Error::Execution(
            "AvatarNode is tick-driven; does not consume reactive inputs".into(),
        ))
    }

    async fn tick(
        &self,
        tick: Tick,
        ctx: &dyn NodeRuntimeContextRead,
        mut cb: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<(), Error> {
        let weights_snap =
            remotemedia_traits::runtime_context::snapshot::<AvatarWeights>(ctx, WEIGHTS_PORT);
        let idle_snap =
            remotemedia_traits::runtime_context::snapshot::<IdlePose>(ctx, IDLE_POSE_PORT);

        let mode = pick_render_mode(
            tick.pts_us,
            weights_snap.as_ref().map(|s| s.pts_us),
            idle_snap.is_some(),
            self.config.weights_fresh_us,
        );

        // `weights_snap` and `idle_snap` are dropped after this point
        // — render path consumes their fields directly here. In the
        // real avatar, `mode == Speaking` would feed `weights_snap.value`
        // into the GPU blendshape pass; `mode == Idle` would feed
        // `idle_snap.value` into the same pass with neutral lipsync.
        let _ = (&weights_snap, &idle_snap);

        let frame = self.build_video_frame(&tick, mode);
        cb(frame)?;
        Ok(())
    }
}

/// Factory for `AvatarNode`. Reads `rate_hz`, `weights_fresh_us`,
/// `width`, `height` from `params`; missing keys fall back to
/// module-level defaults.
pub struct AvatarNodeFactory;

impl Default for AvatarNodeFactory {
    fn default() -> Self {
        Self
    }
}

impl StreamingNodeFactory for AvatarNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        Ok(Box::new(AvatarNode::from_params(node_id, params)))
    }

    fn node_type(&self) -> &str {
        "AvatarNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("AvatarNode")
                .description("Tick-driven avatar render node — reads blendshape weights and idle pose snapshots")
                .category("avatar")
                .accepts([RuntimeDataType::Json])
                .produces([RuntimeDataType::Video]),
        )
    }

    /// Both inputs are snapshot ports. The session router consults this
    /// at session bind to wire the producer's snapshot read handles
    /// into `NodeRuntimeContext::input_snapshots`.
    fn input_port_kinds(&self) -> HashMap<String, PortKind> {
        let mut m = HashMap::new();
        m.insert(WEIGHTS_PORT.to_string(), PortKind::Snapshot);
        m.insert(IDLE_POSE_PORT.to_string(), PortKind::Snapshot);
        m
    }
}

// Suppress the unused `VideoCodec` import on the rare nightly that
// otherwise warns; we re-export it at module scope so consumers can
// build encoded frames in a follow-up without changing imports.
#[allow(dead_code)]
fn _video_codec_marker(_: VideoCodec) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::ports::OutputPort;
    use crate::nodes::ports::SnapshotPort;
    use std::sync::Arc;

    #[test]
    fn pick_render_mode_uses_weights_when_fresh() {
        let mode = pick_render_mode(
            /* tick_pts_us */ 1_000_000,
            /* weights_pts_us */ Some(1_000_000),
            /* idle_present */ true,
            /* fresh_us */ 100_000,
        );
        assert_eq!(mode, RenderMode::Speaking);
    }

    #[test]
    fn pick_render_mode_falls_back_to_idle_when_weights_stale() {
        let mode = pick_render_mode(
            1_000_000,
            Some(500_000), // 500 ms old; outside the 100 ms window
            true,
            100_000,
        );
        assert_eq!(mode, RenderMode::Idle);
    }

    #[test]
    fn pick_render_mode_falls_back_to_idle_when_weights_missing() {
        let mode = pick_render_mode(1_000_000, None, true, 100_000);
        assert_eq!(mode, RenderMode::Idle);
    }

    #[test]
    fn pick_render_mode_returns_cold_when_both_missing() {
        let mode = pick_render_mode(1_000_000, None, false, 100_000);
        assert_eq!(mode, RenderMode::Cold);
    }

    #[test]
    fn pick_render_mode_handles_pts_underflow() {
        // Tick PTS is small, fresh_us is large — `saturating_sub` must
        // not panic, and a publish at pts=0 must still count as fresh.
        let mode = pick_render_mode(
            /* tick */ 50_000,
            /* weights pts */ Some(0),
            true,
            /* fresh */ 1_000_000,
        );
        assert_eq!(mode, RenderMode::Speaking);
    }

    #[test]
    fn factory_creates_node_with_overridden_rate() {
        let factory = AvatarNodeFactory;
        let node = factory
            .create(
                "avatar".to_string(),
                &serde_json::json!({ "rate_hz": 60, "width": 1024, "height": 768 }),
                None,
            )
            .unwrap();
        assert_eq!(node.node_type(), "AvatarNode");
        assert!(matches!(node.pacing_nature(), PacingNature::SourceWall(60)));
    }

    #[test]
    fn factory_declares_snapshot_inputs() {
        let factory = AvatarNodeFactory;
        let kinds = factory.input_port_kinds();
        assert_eq!(kinds.get(WEIGHTS_PORT), Some(&PortKind::Snapshot));
        assert_eq!(kinds.get(IDLE_POSE_PORT), Some(&PortKind::Snapshot));
    }

    /// `tick` returns one Video frame whose pixel_data sentinel byte
    /// reflects the chosen render mode given the wired snapshots.
    /// Exercises the integration: `remotemedia_traits::runtime_context::snapshot::<T>(ctx, port)` →
    /// `pick_render_mode` → `build_video_frame`.
    #[tokio::test]
    async fn tick_renders_speaking_when_weights_fresh() {
        let node = AvatarNode::new("avatar", AvatarConfig::default());

        // Wire a typed snapshot into the ctx — what the session router
        // does at session bind.
        let weights_out: OutputPort<AvatarWeights> = OutputPort::empty();
        weights_out.publish(
            AvatarWeights {
                mouth_open: 0.7,
                blendshapes: vec![],
            },
            5_000,
        );

        let mut snapshots: HashMap<String, Arc<dyn SnapshotPort>> = HashMap::new();
        snapshots.insert(WEIGHTS_PORT.to_string(), Arc::new(weights_out.input()));

        let ctx = NodeRuntimeContext::with_input_snapshots(
            "sess-1",
            "avatar",
            crate::transport::session_control::SessionControl::new("sess-1"),
            Arc::new(()),
            snapshots,
        );

        let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured_clone = captured.clone();
        let cb: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send> = Box::new(move |out| {
            captured_clone.lock().unwrap().push(out);
            Ok(())
        });

        node.tick(
            Tick {
                clock_id: "wall:30".to_string(),
                pts_us: 5_000,
                frame_idx: 0,
                deadline_us: 38_333,
            },
            &ctx,
            cb,
        )
        .await
        .unwrap();

        let out = captured.lock().unwrap();
        assert_eq!(out.len(), 1);
        let RuntimeData::Video {
            pixel_data,
            timestamp_us,
            ..
        } = &out[0]
        else {
            panic!("expected Video, got {:?}", out[0]);
        };
        assert_eq!(pixel_data, &[0xAA]);
        assert_eq!(*timestamp_us, 5_000);
    }

    #[tokio::test]
    async fn tick_renders_idle_when_only_pose_present() {
        let node = AvatarNode::new("avatar", AvatarConfig::default());

        let pose_out: OutputPort<IdlePose> = OutputPort::empty();
        pose_out.publish(
            IdlePose {
                frame_idx: 1,
                pts_us: 5_000,
                phase: 0.0,
                blink: 0.0,
            },
            5_000,
        );

        let mut snapshots: HashMap<String, Arc<dyn SnapshotPort>> = HashMap::new();
        snapshots.insert(IDLE_POSE_PORT.to_string(), Arc::new(pose_out.input()));

        let ctx = NodeRuntimeContext::with_input_snapshots(
            "sess-1",
            "avatar",
            crate::transport::session_control::SessionControl::new("sess-1"),
            Arc::new(()),
            snapshots,
        );

        let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured_clone = captured.clone();
        let cb: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send> = Box::new(move |out| {
            captured_clone.lock().unwrap().push(out);
            Ok(())
        });

        node.tick(
            Tick {
                clock_id: "wall:30".to_string(),
                pts_us: 5_000,
                frame_idx: 0,
                deadline_us: 38_333,
            },
            &ctx,
            cb,
        )
        .await
        .unwrap();

        let out = captured.lock().unwrap();
        let RuntimeData::Video { pixel_data, .. } = &out[0] else {
            panic!("expected Video");
        };
        assert_eq!(pixel_data, &[0xBB]);
    }

    #[tokio::test]
    async fn tick_renders_cold_when_no_inputs() {
        let node = AvatarNode::new("avatar", AvatarConfig::default());
        let ctx = NodeRuntimeContext::for_test("sess-1", "avatar");

        let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured_clone = captured.clone();
        let cb: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send> = Box::new(move |out| {
            captured_clone.lock().unwrap().push(out);
            Ok(())
        });

        node.tick(
            Tick {
                clock_id: "wall:30".to_string(),
                pts_us: 0,
                frame_idx: 0,
                deadline_us: 33_333,
            },
            &ctx,
            cb,
        )
        .await
        .unwrap();

        let out = captured.lock().unwrap();
        let RuntimeData::Video { pixel_data, .. } = &out[0] else {
            panic!("expected Video");
        };
        assert_eq!(pixel_data, &[0x00]);
    }
}
