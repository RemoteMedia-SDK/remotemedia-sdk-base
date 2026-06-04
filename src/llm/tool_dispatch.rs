//! Streaming tool-call accumulation and side-effect dispatch.
//!
//! The implementation moved to the skinny `remotemedia-modality` crate so
//! loadable-node plugins (which can't link `remotemedia-core`) share
//! one source of truth with the in-tree LLM nodes. This file is a
//! transparent re-export shim — `crate::llm::tool_dispatch::ToolCallAccum`
//! and `dispatch_tool_call` still resolve to the same items they
//! always did.

pub use remotemedia_modality::llm::tool_dispatch::{dispatch_tool_call, ToolCallAccum};
