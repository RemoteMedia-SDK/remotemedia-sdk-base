//! Per-session LLM conversation history.
//!
//! The implementation moved to the skinny `remotemedia-modality` crate so
//! loadable-node plugins (which can't link `remotemedia-core`) share
//! one source of truth with the in-tree LLM nodes. This file is a
//! transparent re-export shim — `crate::llm::history::HistoryEntry`
//! and `crate::llm::history::window_start` still resolve to the same
//! types they always did.

pub use remotemedia_modality::llm::history::{window_start, HistoryEntry};
