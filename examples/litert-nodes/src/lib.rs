//! LiteRT nodes for RemoteMedia: the same models on an Android phone's GPU
//! and on a desktop CPU, so a unit holding them can move between the two.
//!
//! * `WhisperLiteRtNode`: Whisper (TFLite) speech-to-text, on LiteRT's
//!   CompiledModel API (GPU / NPU) or the TFLite interpreter (`cpu`).
//! * `GemmaAssistantNode`: a voice assistant for a room, on LiteRT-LM's C API
//!   (`lm`), e.g. Gemma 4 E2B.
//! * `KokoroTtsNode` (feature `kokoro`): Kokoro-82M text-to-speech from its
//!   `.litertlm` bundle (`container`, `kokoro`), with the bundle's
//!   BiLSTM custom op (`bilstm`) and LiteRT-LM's English phonemizer (C++).
//!
//! Runtime libraries are loaded when a node starts, not linked: on Android
//! LiteRT's `libLiteRt.so` (with its GPU accelerator beside it) and
//! LiteRT-LM's `liblitert-lm.so`, on Windows LiteRT-LM's `litert-lm.dll`.
//!
//! Built as a loadable plugin (`plugin_export!`), and as a library for the
//! `whisper-litert`, `gemma-litert` and `kokoro-litert` commands.

pub mod assistant;
pub mod backend;
pub mod bilstm;
pub mod compiled;
pub mod container;
#[cfg(feature = "kokoro")]
pub mod kokoro;
pub mod litert;
pub mod lm;
pub mod mel;
pub mod node;
pub mod tokenizer;
#[cfg(feature = "kokoro")]
pub mod tts_node;
pub mod whisper;

pub use whisper::{Transcript, Whisper, WhisperConfig};

#[cfg(feature = "kokoro")]
remotemedia_plugin_sdk::plugin_export!(
    node::WhisperLiteRtFactory,
    assistant::GemmaAssistantFactory,
    tts_node::KokoroTtsFactory,
);
#[cfg(not(feature = "kokoro"))]
remotemedia_plugin_sdk::plugin_export!(node::WhisperLiteRtFactory, assistant::GemmaAssistantFactory);
