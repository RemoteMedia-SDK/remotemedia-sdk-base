//! LLM tool-call schema + side-effect dispatch hints.
//!
//! The implementation moved to the skinny `remotemedia-modality` crate so
//! loadable-node plugins (which can't link `remotemedia-core`) share
//! one source of truth with the in-tree LLM nodes (`OpenAIChatNode`,
//! `MultimodalLLMNode`). This file is a transparent re-export shim —
//! `crate::nodes::tool_spec::ToolSpec`, `ToolKind`,
//! `default_say_tool`, `default_show_tool`, `default_motion_tool`,
//! and `to_openai_tools_array` still resolve to the same items they
//! always did.

pub use remotemedia_modality::llm::tool_spec::{
    default_motion_tool, default_say_tool, default_show_tool, to_openai_tools_array, ToolKind,
    ToolSpec,
};
