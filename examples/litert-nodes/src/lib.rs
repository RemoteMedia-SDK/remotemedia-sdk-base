//! LiteRT/TFLite-backed RemoteMedia nodes for Android and mobile targets.
//!
//! This crate provides reusable Rust implementations of LiteRT/TFLite model nodes,
//! including shared model loading, signature runner invocation, tensor marshaling,
//! audio preprocessing, and a Whisper ASR implementation.

#[cfg(feature = "whisper")]
pub mod whisper;

mod audio_preprocessing;
mod error;
mod litert_ffi;
mod litert_model;
mod litert_session;
mod tensor_marshaling;
mod tokenizer;

pub use error::{LiteRtError, Result};
pub use litert_model::{LiteRtModel, LiteRtModelConfig};
pub use litert_session::{LiteRtSession, SessionConfig, SignatureRunner};
pub use tensor_marshaling::{TensorSpec, TensorValidator, TensorMarshaler};
pub use audio_preprocessing::{AudioPreprocessor, ResampleConfig, LogMelConfig};

#[cfg(feature = "loadable-export")]
mod loadable_export;

#[cfg(test)]
mod tests;
