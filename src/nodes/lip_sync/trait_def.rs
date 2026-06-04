//! `LipSyncNode` trait — port a `LipSyncNode` impl satisfies on top of
//! `AsyncStreamingNode`. Spec 2026-04-27 §3.3.
//!
//! Concrete impls:
//! - [`super::SyntheticLipSyncNode`] — deterministic stand-in (now)
//! - `Audio2FaceLipSyncNode` — ONNX + PGD/BVLS solver port (next turn)
//! - phoneme-driven impls — out of scope for this spec

use crate::nodes::AsyncStreamingNode;

/// A node that consumes `RuntimeData::Audio` and emits
/// `RuntimeData::Json` blendshape frames per
/// [`super::BlendshapeFrame`].
///
/// The trait itself is a marker plus capability declaration — the
/// actual `process_streaming` lives on `AsyncStreamingNode`. We keep
/// this distinct from `AsyncStreamingNode` because the resolver (spec
/// 023) needs to know which audio sample rate the node requires
/// before pipeline construction, and that's an impl-specific value
/// (Audio2Face is locked to 16 kHz; phoneme impls don't care about
/// rate).
pub trait LipSyncNode: AsyncStreamingNode {
    /// Required input audio sample rate in Hz. Capability resolver
    /// inserts a resampler upstream if the actual rate differs.
    fn required_sample_rate(&self) -> u32;

    /// Required input channel count (1 for all known impls; reserved
    /// for the day a stereo phoneme impl ships).
    fn required_channels(&self) -> u32 {
        1
    }
}
