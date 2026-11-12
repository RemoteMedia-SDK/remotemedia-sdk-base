//! LiteRT (TFLite) nodes for RemoteMedia.
//!
//! `WhisperLiteRtNode` runs a Whisper model exported to TFLite with `encode`
//! and `decode` signatures (e.g. `whisper_tiny_30s_f32.tflite`). By default it
//! compiles the model for the GPU with LiteRT's CompiledModel API (`npu` and
//! `npu,gpu` ask for the NPU; `cpu` uses the TFLite interpreter on XNNPACK).
//! The runtime library is loaded when the node starts, not linked: on Android
//! LiteRT's `libLiteRt.so` with its GPU accelerator and NPU dispatch
//! libraries beside it, on Windows LiteRT-LM's `litert-lm.dll`. The same
//! model and node run on a phone and on a PC, so a unit holding it can move
//! between them.
//!
//! Built as a loadable plugin (`plugin_export!`), and as a library for the
//! `whisper-litert` command that transcribes a WAV file.

pub mod backend;
pub mod compiled;
pub mod litert;
pub mod mel;
pub mod node;
pub mod tokenizer;
pub mod whisper;

pub use whisper::{Transcript, Whisper, WhisperConfig};

remotemedia_plugin_sdk::plugin_export!(node::WhisperLiteRtFactory);
