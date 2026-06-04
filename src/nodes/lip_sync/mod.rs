//! Avatar lip-sync nodes — `LipSyncNode` trait, `BlendshapeFrame`
//! envelope, and concrete impls (synthetic now; ONNX `Audio2Face` next).
//!
//! See spec 2026-04-27 §3.3 for the wire-format contract:
//! `RuntimeData::Json {kind: "blendshapes", arkit_52: [f32; 52],
//! pts_ms: u64, turn_id?: u64}` — renderer-agnostic ARKit-52, mapped
//! to Live2D VBridger params inside the renderer node.

pub mod arkit_smoother;
// `Audio2FaceLipSyncNode` and the `audio2face/` submodule moved to a
// standalone plugin: github.com/RemoteMedia-SDK/audio2face. Load via
// `"plugins": ["audio2face@v0.1.0"]` in the pipeline manifest.
mod blendshape;
mod synthetic;
mod trait_def;

pub use arkit_smoother::ArkitSmoother;
pub use blendshape::{BlendshapeFrame, ARKIT_52, ARKIT_BLENDSHAPE_NAMES};
pub use synthetic::{SyntheticLipSyncConfig, SyntheticLipSyncNode};
pub use trait_def::LipSyncNode;
