//! The `ContextTool` trait — tools that produce a *context string*
//! to be injected into a downstream audio LLM's system prompt before
//! the next turn.
//!
//! Distinct from the existing [`crate::llm::tool_dispatch`] path:
//! that path is for chat-backend tools where the model EMITTED the
//! call (`say` / `show` / `perform_motion`) and the dispatcher
//! routes the call into the data plane. Here the tool is invoked
//! *upstream* of the audio LLM and its return value becomes the
//! grounding context.

use crate::nodes::tool_spec::ToolSpec;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Errors a [`ContextTool::execute`] call may raise. The
/// [`ToolExecutorNode`](super::ToolExecutorNode) catches these and
/// emits `{context: null}` rather than failing the pipeline.
#[derive(Debug, thiserror::Error)]
pub enum ContextToolError {
    #[error("invalid args: {0}")]
    InvalidArgs(String),
    #[error("execution failed: {0}")]
    Execution(String),
}

/// A tool whose return value is a natural-language string injected
/// into the audio LLM's system prompt for the next turn.
///
/// Implementations are async to allow network / disk lookups
/// (e.g. clinical record store) without blocking the executor's
/// tokio task.
#[async_trait]
pub trait ContextTool: Send + Sync {
    /// Stable tool name — matches `ToolSpec::name` and is the key
    /// the classifier emits in its decision JSON.
    fn name(&self) -> &str;

    /// Tool spec (description + JSON-schema for args). Used to
    /// build the classifier's system prompt via
    /// [`crate::nodes::tool_spec::to_openai_tools_array`].
    fn spec(&self) -> ToolSpec;

    /// Execute the tool. Return `Ok(Some(string))` on success,
    /// `Ok(None)` on a clean miss (no record found / no answer
    /// available), or `Err(_)` on a programming-level failure (args
    /// didn't validate, downstream service died). The executor maps
    /// `Ok(None)` and `Err(_)` to the same `{context: null}` output
    /// — the difference only matters for logging.
    async fn execute(&self, args: &Value) -> Result<Option<String>, ContextToolError>;
}

/// Registry of `ContextTool` impls keyed by name. Provider crates
/// populate this with their tools at startup; the
/// [`ToolExecutorNode`](super::ToolExecutorNode) builds a per-node
/// view of it based on the manifest's `tools: [...]` config block.
#[derive(Default, Clone)]
pub struct ContextToolRegistry {
    by_name: HashMap<String, Arc<dyn ContextTool>>,
}

impl ContextToolRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, tool: Arc<dyn ContextTool>) {
        self.by_name.insert(tool.name().to_string(), tool);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn ContextTool>> {
        self.by_name.get(name).cloned()
    }

    pub fn names(&self) -> Vec<String> {
        let mut v: Vec<_> = self.by_name.keys().cloned().collect();
        v.sort();
        v
    }

    pub fn len(&self) -> usize {
        self.by_name.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }
}

impl std::fmt::Debug for ContextToolRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextToolRegistry")
            .field("names", &self.names())
            .finish()
    }
}
