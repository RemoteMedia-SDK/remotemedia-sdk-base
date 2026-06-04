//! Avatar / talking-head pipeline nodes.
//!
//! See [`docs/MULTI_SESSION_NODES.md`](../../../../docs/MULTI_SESSION_NODES.md)
//! and the `pacing-domains` OpenSpec change for the runtime contract these
//! nodes target. Phase 6.1 (`IdleAnimationNode`, ships today) is a
//! `SourceWall(hz)` node; the avatar + audio2face nodes are gated on
//! Phase 4.4 (manifest snapshot connection wiring) and Phase 5.4
//! (wire-bound clock-tap pacer) and land in a follow-up PR.

pub mod audio2face;
pub mod avatar;
pub mod idle;

pub use audio2face::{
    Audio2FaceConfig, Audio2FaceNode, Audio2FaceNodeFactory, DEFAULT_MOUTH_OPEN_SCALE,
};
pub use avatar::{
    AvatarConfig, AvatarNode, AvatarNodeFactory, AvatarWeights, RenderMode, DEFAULT_AVATAR_RATE_HZ,
    DEFAULT_WEIGHTS_FRESH_US, IDLE_POSE_PORT as AVATAR_IDLE_POSE_PORT, WEIGHTS_PORT,
};
pub use idle::{IdleAnimationNode, IdleAnimationNodeFactory, IdlePose, IDLE_POSE_PORT};
