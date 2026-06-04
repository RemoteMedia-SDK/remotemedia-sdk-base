//! Speech-to-speech tool orchestration (openspec change:
//! `add-s2s-tool-orchestrator`).
//!
//! Three nodes work together so a voice-driven assistant can call
//! tools while keeping end-to-end latency under 800 ms:
//!
//! - [`S2SCoordinatorNode`] — joins one audio utterance with one tool
//!   decision per turn, then emits typed-RPC envelopes
//!   (`set_context`, `reset_history`) followed by the audio frame on
//!   a single main-port edge to a downstream audio LLM.
//! - [`ToolExecutorNode`] — receives `{tool, args}` JSON from the
//!   classifier and dispatches to a registered [`ContextTool`] impl,
//!   emitting `{context: <string> | null}`.
//! - [`ToolClassifierNode`] — Python multiprocess, lives in
//!   `clients/python/remotemedia/nodes/ml/tool_classifier.py`.
//!
//! The audio model is `LFM25AudioOnnxNode` (loadable plugin
//! `lfm25-audio-onnx@v0.2.0`+) — the only backend whose `handle_aux`
//! accepts both wire types and both port-name conventions.

mod clinical_lookup;
mod coordinator;
mod tool;
mod tool_executor;

pub use clinical_lookup::{ClinicalLookupConfig, ClinicalLookupTool};
pub use coordinator::{
    S2SCoordinatorConfig, S2SCoordinatorNode, S2SCoordinatorNodeFactory,
    BARGE_IN_WINDOW_MS_DEFAULT, DECISION_TIMEOUT_MS_DEFAULT,
};
pub use tool::{ContextTool, ContextToolError, ContextToolRegistry};
pub use tool_executor::{ToolExecutorConfig, ToolExecutorNode, ToolExecutorNodeFactory};
