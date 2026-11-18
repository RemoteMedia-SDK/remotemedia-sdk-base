//! Video processing nodes
//!
//! This module provides video encoding, decoding, and processing nodes
//! for the RemoteMedia SDK pipeline architecture.
//!
//! See spec 012: Video Codec Support (AV1/VP8/AVC)

pub mod codec;
#[cfg(feature = "video")]
mod decoded_frames;
pub mod decoder;
pub mod encoder;
pub mod format_converter;
#[cfg(feature = "video")]
mod frames;
#[cfg(feature = "video")]
mod packets;

// Re-export encoder and decoder nodes (T020-T027 complete)
pub use decoder::{VideoDecoderConfig, VideoDecoderNode};
pub use encoder::{VideoEncoderConfig, VideoEncoderNode};

// Phase 6: Video processing nodes (T084-T095)
/// `VideoScaler` lives in `nodes::video_scaler` (no ffmpeg needed).
pub use crate::nodes::video_scaler::{VideoScalerConfig, VideoScalerNode};
pub use format_converter::{VideoFormatConverterConfig, VideoFormatConverterNode};
