//! `ToolExecutorNode` — dispatches `{tool, args}` JSON decisions
//! emitted by [`super::ToolClassifierNode`](classifier) to a
//! registered [`ContextTool`] impl and emits
//! `RuntimeData::Json({context: <string|null>})` for the downstream
//! [`super::S2SCoordinatorNode`].
//!
//! The node is `AsyncStreamingNode` because individual tool
//! `execute` calls may be I/O bound (HTTP, DB, fuzzy index).

use super::tool::{ContextTool, ContextToolRegistry};
use crate::data::RuntimeData;
use crate::error::Error;
use crate::nodes::AsyncStreamingNode;
use async_trait::async_trait;
use remotemedia_traits::runtime_context::InitializeContextRead;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

/// Per-instance configuration for [`ToolExecutorNode`].
///
/// `tools` selects which tools out of the registry this node
/// instance exposes. An empty list (the default) means "all
/// registered tools". Per-instance restriction is useful for
/// pipelines where different executors should expose different
/// surfaces (PII-safe vs full).
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, Default)]
#[serde(default)]
pub struct ToolExecutorConfig {
    /// Names of tools (matching `ContextTool::name`) to expose. If
    /// empty, all tools in the registry are reachable.
    pub tools: Vec<String>,
}

/// Streaming node that dispatches classifier decisions to a
/// `ContextTool` implementation and emits the resulting context (or
/// `null` on miss / unknown / panic / error).
pub struct ToolExecutorNode {
    registry: Arc<ContextToolRegistry>,
    /// Names allowed for this node. `None` = no restriction.
    allowed: Option<Vec<String>>,
}

impl ToolExecutorNode {
    pub fn new(registry: Arc<ContextToolRegistry>, cfg: ToolExecutorConfig) -> Self {
        let allowed = if cfg.tools.is_empty() {
            None
        } else {
            Some(cfg.tools)
        };
        Self { registry, allowed }
    }

    fn is_allowed(&self, name: &str) -> bool {
        match &self.allowed {
            None => true,
            Some(list) => list.iter().any(|n| n == name),
        }
    }

    /// Decode `{tool, args}` out of either a `RuntimeData::Json` or
    /// `RuntimeData::Text` (JSON-as-string). Returns
    /// `(tool_name, args)`. `tool_name == None` means "no tool" — the
    /// classifier explicitly chose null.
    fn extract_decision(data: &RuntimeData) -> Option<(Option<String>, Value)> {
        let v: Value = match data {
            RuntimeData::Json(j) => j.clone(),
            RuntimeData::Text(s) => serde_json::from_str(s).ok()?,
            _ => return None,
        };
        let obj = v.as_object()?;
        // `tool` may be `null`, missing, or a string.
        let tool = match obj.get("tool") {
            Some(Value::Null) | None => None,
            Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
            _ => None,
        };
        let args = obj
            .get("args")
            .cloned()
            .unwrap_or(Value::Object(Default::default()));
        Some((tool, args))
    }

    fn emit_null_context() -> RuntimeData {
        RuntimeData::Json(json!({ "context": Value::Null }))
    }

    fn emit_context(s: &str) -> RuntimeData {
        RuntimeData::Json(json!({ "context": s }))
    }

    /// Look up a tool and run it. Errors / misses both produce a
    /// null-context envelope. Spawns onto the current runtime so a
    /// tool panic surfaces as a `JoinError` instead of unwinding the
    /// caller.
    async fn dispatch(&self, tool_name: &str, args: Value) -> RuntimeData {
        let tool: Arc<dyn ContextTool> = match self.registry.get(tool_name) {
            Some(t) => t,
            None => {
                tracing::warn!(
                    target: "s2s::tool_executor",
                    tool = tool_name,
                    "unknown tool name; emitting null context"
                );
                return Self::emit_null_context();
            }
        };

        let tool_name_owned = tool_name.to_string();
        let result = tokio::task::spawn(async move { tool.execute(&args).await }).await;
        let tool_name = tool_name_owned.as_str();

        match result {
            Ok(Ok(Some(s))) => {
                tracing::debug!(
                    target: "s2s::tool_executor",
                    tool = tool_name,
                    ctx_chars = s.len(),
                    "tool returned context"
                );
                Self::emit_context(&s)
            }
            Ok(Ok(None)) => {
                tracing::info!(
                    target: "s2s::tool_executor",
                    tool = tool_name,
                    "tool returned no context (miss)"
                );
                Self::emit_null_context()
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    target: "s2s::tool_executor",
                    tool = tool_name,
                    error = %e,
                    "tool returned error; emitting null context"
                );
                Self::emit_null_context()
            }
            Err(join_err) => {
                tracing::error!(
                    target: "s2s::tool_executor",
                    tool = tool_name,
                    error = %join_err,
                    "tool panicked; emitting null context"
                );
                Self::emit_null_context()
            }
        }
    }
}

#[async_trait]
impl AsyncStreamingNode for ToolExecutorNode {
    fn node_type(&self) -> &str {
        "ToolExecutorNode"
    }

    async fn initialize(&self, _ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        let names = self
            .allowed
            .as_ref()
            .cloned()
            .unwrap_or_else(|| self.registry.names());
        tracing::info!(
            target: "s2s::tool_executor",
            tools = ?names,
            "ToolExecutorNode ready"
        );
        Ok(())
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        let (tool, args) = match Self::extract_decision(&data) {
            Some(p) => p,
            None => {
                tracing::trace!(
                    target: "s2s::tool_executor",
                    "input not a {{tool, args}} envelope; emitting null context"
                );
                return Ok(Self::emit_null_context());
            }
        };

        let Some(tool_name) = tool else {
            tracing::debug!(
                target: "s2s::tool_executor",
                "decision explicitly chose no tool"
            );
            return Ok(Self::emit_null_context());
        };

        if !self.is_allowed(&tool_name) {
            tracing::warn!(
                target: "s2s::tool_executor",
                tool = %tool_name,
                "tool not in this node's allowed list; emitting null context"
            );
            return Ok(Self::emit_null_context());
        }

        Ok(self.dispatch(&tool_name, args).await)
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/// Factory for [`ToolExecutorNode`]. Holds a registry shared across
/// every node instance created by this factory; the manifest's
/// `tools: [...]` block narrows which tools each instance exposes.
pub struct ToolExecutorNodeFactory {
    registry: Arc<ContextToolRegistry>,
}

impl ToolExecutorNodeFactory {
    pub fn new(registry: Arc<ContextToolRegistry>) -> Self {
        Self { registry }
    }

    /// Factory with the SDK's built-in tools (currently
    /// `clinical_lookup`).
    pub fn with_builtins() -> Self {
        let mut reg = ContextToolRegistry::new();
        reg.register(Arc::new(
            super::clinical_lookup::ClinicalLookupTool::default(),
        ));
        Self::new(Arc::new(reg))
    }
}

impl Default for ToolExecutorNodeFactory {
    fn default() -> Self {
        Self::with_builtins()
    }
}

impl crate::nodes::StreamingNodeFactory for ToolExecutorNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn crate::nodes::StreamingNode>, Error> {
        use crate::nodes::AsyncNodeWrapper;
        let cfg: ToolExecutorConfig = if params.is_null() {
            ToolExecutorConfig::default()
        } else {
            serde_json::from_value(params.clone())
                .map_err(|e| Error::Execution(format!("ToolExecutorNode params: {e}")))?
        };
        let node = ToolExecutorNode::new(self.registry.clone(), cfg);
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "ToolExecutorNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("ToolExecutorNode")
                .description(
                    "Dispatches {tool, args} JSON decisions to a registered \
                     ContextTool implementation; emits {context: <string|null>}.",
                )
                .category("s2s")
                .accepts([RuntimeDataType::Json, RuntimeDataType::Text])
                .produces([RuntimeDataType::Json])
                .config_schema_from::<ToolExecutorConfig>(),
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::super::tool::ContextToolError;
    use super::*;
    use crate::nodes::tool_spec::{ToolKind, ToolSpec};

    /// Test tool that echoes its `name` arg as the context string.
    struct EchoTool;

    #[async_trait]
    impl ContextTool for EchoTool {
        fn name(&self) -> &str {
            "echo"
        }
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "echo".into(),
                description: "Echoes the name arg as context.".into(),
                parameters: json!({
                    "type": "object",
                    "properties": { "name": {"type": "string"} },
                    "required": ["name"],
                }),
                kind: ToolKind::SideEffect,
                cancelable: true,
            }
        }
        async fn execute(&self, args: &Value) -> Result<Option<String>, ContextToolError> {
            let name = args
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| ContextToolError::InvalidArgs("missing name".into()))?;
            if name == "MISS" {
                Ok(None)
            } else if name == "BOOM" {
                Err(ContextToolError::Execution("intentional".into()))
            } else {
                Ok(Some(format!("Echo: {name}")))
            }
        }
    }

    fn build_node() -> ToolExecutorNode {
        let mut reg = ContextToolRegistry::new();
        reg.register(Arc::new(EchoTool));
        ToolExecutorNode::new(Arc::new(reg), ToolExecutorConfig::default())
    }

    fn context_of(out: &RuntimeData) -> Option<Option<String>> {
        let RuntimeData::Json(v) = out else {
            return None;
        };
        Some(match v.get("context") {
            Some(Value::Null) | None => None,
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        })
    }

    #[tokio::test]
    async fn known_tool_emits_context_string() {
        let node = build_node();
        let input = RuntimeData::Json(json!({
            "tool": "echo",
            "args": {"name": "Bed B00"},
        }));
        let out = node.process(input).await.unwrap();
        assert_eq!(context_of(&out), Some(Some("Echo: Bed B00".into())));
    }

    #[tokio::test]
    async fn tool_miss_emits_null_context() {
        let node = build_node();
        let input = RuntimeData::Json(json!({
            "tool": "echo",
            "args": {"name": "MISS"},
        }));
        let out = node.process(input).await.unwrap();
        assert_eq!(context_of(&out), Some(None));
    }

    #[tokio::test]
    async fn tool_error_emits_null_context() {
        let node = build_node();
        let input = RuntimeData::Json(json!({
            "tool": "echo",
            "args": {"name": "BOOM"},
        }));
        let out = node.process(input).await.unwrap();
        assert_eq!(context_of(&out), Some(None));
    }

    #[tokio::test]
    async fn unknown_tool_emits_null_context() {
        let node = build_node();
        let input = RuntimeData::Json(json!({
            "tool": "not_registered",
            "args": {},
        }));
        let out = node.process(input).await.unwrap();
        assert_eq!(context_of(&out), Some(None));
    }

    #[tokio::test]
    async fn null_tool_emits_null_context_without_dispatch() {
        let node = build_node();
        let input = RuntimeData::Json(json!({
            "tool": Value::Null,
            "args": {},
        }));
        let out = node.process(input).await.unwrap();
        assert_eq!(context_of(&out), Some(None));
    }

    #[tokio::test]
    async fn text_envelope_accepted() {
        // Python clients emit JSON-as-text.
        let node = build_node();
        let input = RuntimeData::Text(r#"{"tool":"echo","args":{"name":"text-path"}}"#.into());
        let out = node.process(input).await.unwrap();
        assert_eq!(context_of(&out), Some(Some("Echo: text-path".into())));
    }

    #[tokio::test]
    async fn allow_list_restricts_visible_tools() {
        let mut reg = ContextToolRegistry::new();
        reg.register(Arc::new(EchoTool));
        let cfg = ToolExecutorConfig {
            tools: vec!["something_else".into()],
        };
        let node = ToolExecutorNode::new(Arc::new(reg), cfg);
        let input = RuntimeData::Json(json!({
            "tool": "echo",
            "args": {"name": "blocked"},
        }));
        let out = node.process(input).await.unwrap();
        // Even though `echo` is registered, the manifest scoped this
        // node away from it.
        assert_eq!(context_of(&out), Some(None));
    }

    #[tokio::test]
    async fn malformed_input_emits_null_context() {
        let node = build_node();
        let out = node
            .process(RuntimeData::Text("totally not json".into()))
            .await
            .unwrap();
        assert_eq!(context_of(&out), Some(None));
    }
}
