//! `RenderReadyGateNode` — buffers blendshape / skeletal_pose envelopes
//! until the downstream renderer has finished its warmup, then drains
//! the buffer paced by `pts_ms`.
//!
//! ## Why this exists
//!
//! `CcRenderNode` warms up for several seconds (GLB load, bind capture,
//! settle frames). During that window it discards rendered frames.
//! Upstream emitters like `Audio2FaceLipSyncNode` and `MotionPlayerNode`
//! pace their output by `pts_ms` anchored at first emission — so if
//! the audio synthesis (Kokoro) bursts a multi-second chunk before the
//! renderer is ready, the entire envelope stream pre-pacing pipes
//! through the renderer's *latest-wins* `tokio::watch` pose channel
//! and lip sync freezes on the FINAL envelope.
//!
//! In live-WebRTC pipelines audio chunks arrive paced by mic playback,
//! so the issue is invisible. In offline-batch renders (TTS → render →
//! MP4) it's the dominant correctness failure mode.
//!
//! ## Behavior
//!
//! - **Closed (default).** Any input whose `kind` is in
//!   `gated_kinds` (default `["blendshapes", "skeletal_pose"]`) is
//!   buffered. Other inputs (Audio, non-gated Json) pass through.
//! - **Open trigger.** A `RuntimeData::Json` with
//!   `{ "kind": "render_ready" }` opens the gate. The buffered
//!   envelopes are drained — paced by `pts_ms` if `pace_realtime`,
//!   otherwise back-to-back. After draining, the node becomes a
//!   passthrough (still applying pacing on subsequent envelopes).
//! - **Idempotent.** Multiple `render_ready` envelopes are no-ops
//!   after the first.
//!
//! ## Pacing semantics
//!
//! Pacing anchors at the wallclock of the FIRST emit *after* the gate
//! opens. Each subsequent envelope sleeps until
//! `anchor_wall + (pts_ms - anchor_pts)` is reached. Late envelopes
//! (already past target) emit immediately. Mirrors the pacing in
//! `Audio2FaceLipSyncNode` and `MotionPlayerNode`.

mod gate_node;

pub use gate_node::{RenderReadyGateConfig, RenderReadyGateNode};
