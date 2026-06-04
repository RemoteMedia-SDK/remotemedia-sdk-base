//! Motion source nodes — file-driven and prompt-driven skeletal pose
//! emitters that feed `CcRenderNode`'s `kind="skeletal_pose"` envelope
//! channel. Pure-Rust replay (this module) complements the Python
//! `KimodoMotionNode` (text-to-motion diffusion).

mod motion_player;

pub use motion_player::{MotionPlayerConfig, MotionPlayerNode};
