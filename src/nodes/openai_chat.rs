//! OpenAI-compatible Chat Completion Node
//!
//! Makes streaming HTTP calls to an OpenAI-compatible API (OpenAI, Azure,
//! local vLLM/Ollama/LLama.cpp servers) and emits channel-tagged
//! `RuntimeData::Text` tokens downstream.
//!
//! Pipeline flow:
//!   UserText → OpenAIChatNode → [streaming tokens] → TextCollector → TTS
//!
//! The node accepts `RuntimeData::Text` (user message) and
//! `RuntimeData::Json` (structured message with role/content), then
//! streams assistant responses as tagged text on the `"tts"` channel
//! by default. Use `output_channel` to override (e.g., `"ui"` for
//! display-only text).
//!
//! Implementation note: text-only input adaptation only. The
//! transport layer (HTTP, SSE, tool dispatch, history round-trip)
//! lives in [`crate::llm::ChatBackend`] and is shared with the
//! multimodal LLM node.

use crate::data::{RuntimeData, TEXT_CHANNEL_DEFAULT};
use crate::error::Error;
use crate::llm::{ChatBackend, ChatBackendConfig, ProviderKind};
use crate::nodes::AsyncStreamingNode;
use remotemedia_traits::runtime_context::{InitializeContextRead, NodeRuntimeContextRead};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration for the OpenAI-compatible chat node.
///
/// All fields support both `snake_case` and `camelCase` keys via
/// `#[serde(alias)]` so manifests can use either convention.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct OpenAIChatConfig {
    /// OpenAI API key. Also read from `OPENAI_API_KEY` env var if omitted.
    #[serde(alias = "apiKey")]
    pub api_key: Option<String>,

    /// Base URL for the API endpoint.
    /// Default: `https://api.openai.com/v1`
    #[serde(alias = "baseUrl")]
    pub base_url: Option<String>,

    /// Model identifier (e.g. `"gpt-4o"`, `"gpt-4o-mini"`, `"qwen2.5-7b"`).
    #[serde(alias = "model")]
    pub model: Option<String>,

    /// System prompt sent as the first message in every request.
    /// Omit to use no system message.
    #[serde(alias = "systemPrompt")]
    pub system_prompt: Option<String>,

    /// Output channel tag for emitted text tokens.
    /// Default: `"tts"` (spoken). Use `"ui"` for display-only text.
    #[serde(alias = "outputChannel")]
    pub output_channel: Option<String>,

    /// Channel tag for reasoning / thinking tokens emitted by reasoning
    /// models (Qwen3, DeepSeek, etc.) on the SSE field
    /// `delta.reasoning_content`. These tokens are forwarded so the
    /// pipeline sees continuous LLM activity (preventing the coordinator's
    /// silence watchdog from firing during long reasoning phases) but
    /// land on a non-`tts` channel so TTS does not speak them.
    ///
    /// Default: `"think"`. Set to `null`/empty to drop reasoning content
    /// entirely (which will re-introduce silence-watchdog timeouts on
    /// reasoning models — usually you don't want that).
    #[serde(alias = "reasoningChannel")]
    pub reasoning_channel: Option<String>,

    /// Maximum tokens to generate. Default: `4096`.
    #[serde(alias = "maxTokens")]
    pub max_tokens: Option<u32>,

    /// Temperature for sampling (0.0–2.0). Default: `1.0`.
    pub temperature: Option<f32>,

    /// Top-p nucleus sampling cutoff (0.0–1.0). Default: `1.0`.
    #[serde(alias = "topP")]
    pub top_p: Option<f32>,

    /// Number of conversation turns to retain as context history.
    /// Default: `10` (i.e. last 10 user+assistant pairs).
    /// Set to `0` for stateless single-turn mode.
    #[serde(alias = "historyTurns")]
    pub history_turns: usize,

    /// Whether to enable streaming responses. Default: `true`.
    /// When `false`, the full response is buffered then emitted as one chunk.
    #[serde(alias = "streaming")]
    pub streaming: bool,

    // ── Tool calling ────────────────────────────────────────────────
    //
    // Mirrors the registry on the Python `QwenTextMlxNode`. When any
    // tools are active, the request body includes a `tools` array and
    // `tool_choice` field; the SSE parser accumulates
    // `delta.tool_calls` and dispatches `side_effect` tools (`say`,
    // `show`, ...) to the appropriate downstream channel without
    // feeding a result back to the model.
    /// Register the built-in `say` tool. Routes its `text` argument
    /// to the `output_channel` (default `tts`) so it's spoken
    /// immediately.
    #[serde(alias = "enableSayTool")]
    pub enable_say_tool: bool,

    /// Register the built-in `show` tool. Routes its `content`
    /// argument to the `ui` channel for display-only rendering.
    #[serde(alias = "enableShowTool")]
    pub enable_show_tool: bool,

    /// Additional user-defined tools registered alongside the
    /// built-ins. Tool names must be unique across the union.
    #[serde(default, alias = "tools")]
    pub tools: Vec<crate::nodes::tool_spec::ToolSpec>,

    /// Subset of registered tool names exposed to the model.
    /// `None` means "expose every registered tool".
    #[serde(default, alias = "activeTools")]
    pub active_tools: Option<Vec<String>>,

    /// Tool-choice hint sent to the model. Common values: `"auto"`,
    /// `"required"`, `"none"`. JSON-shaped values like
    /// `{"type":"function","function":{"name":"say"}}` are also
    /// accepted (the value is forwarded verbatim).
    #[serde(default, alias = "toolChoice")]
    pub tool_choice: Option<Value>,

    /// LLM vendor profile. Default `OpenAI`. Set to `anthropic` to
    /// use the Messages API. Note: the public node name remains
    /// `OpenAIChatNode` for back-compat, but the profile genuinely
    /// drives the wire format — pick what matches your `base_url`.
    #[serde(default, alias = "provider")]
    pub provider: ProviderKind,
}

impl Default for OpenAIChatConfig {
    fn default() -> Self {
        Self {
            api_key: None,
            base_url: None,
            model: None,
            system_prompt: None,
            output_channel: None,
            reasoning_channel: Some("think".to_string()),
            max_tokens: None,
            temperature: None,
            top_p: None,
            history_turns: 10,
            streaming: true,
            enable_say_tool: false,
            enable_show_tool: false,
            tools: Vec::new(),
            active_tools: None,
            tool_choice: None,
            provider: ProviderKind::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

/// OpenAI-compatible streaming chat completion node.
///
/// Cancellation note: this node intentionally has no barge-in
/// awareness. The runtime (see `session_router::spawn_node_pipeline`)
/// intercepts `<node>.in.barge_in` envelopes and cancels the in-flight
/// `process_streaming_async` future by dropping it. Dropping the
/// future cascades into dropping the reqwest stream, which closes
/// the HTTP connection and stops upstream token generation. We rely
/// on that universal mechanism instead of duplicating per-node
/// cancellation plumbing.
pub struct OpenAIChatNode {
    config: OpenAIChatConfig,
    backend: Arc<ChatBackend>,
}

impl OpenAIChatNode {
    /// Create from a config struct.
    pub fn with_config(config: OpenAIChatConfig) -> Self {
        let profile = config.provider.into_profile();
        Self {
            config,
            backend: Arc::new(ChatBackend::new(profile)),
        }
    }

    /// Resolve the effective API key (config → env var).
    fn resolve_api_key(&self) -> Option<String> {
        self.config
            .api_key
            .clone()
            .filter(|s| !s.is_empty())
            .or_else(|| {
                std::env::var("OPENAI_API_KEY")
                    .ok()
                    .filter(|s| !s.is_empty())
            })
    }

    fn resolve_base_url(&self) -> String {
        self.config
            .base_url
            .clone()
            .unwrap_or_else(|| self.config.provider.default_base_url().to_string())
    }

    fn resolve_model(&self) -> String {
        self.config
            .model
            .clone()
            .unwrap_or_else(|| "gpt-4o-mini".to_string())
    }

    /// Build the active tool registry: built-ins gated by config flags
    /// + user-defined tools, deduped by name (later wins), then
    /// optionally filtered by `active_tools`.
    fn build_tool_registry(&self) -> Vec<crate::nodes::tool_spec::ToolSpec> {
        use crate::nodes::tool_spec::{default_say_tool, default_show_tool, ToolSpec};
        let mut out: Vec<ToolSpec> = Vec::new();
        if self.config.enable_say_tool {
            out.push(default_say_tool());
        }
        if self.config.enable_show_tool {
            out.push(default_show_tool());
        }
        for spec in &self.config.tools {
            if let Some(existing) = out.iter_mut().find(|t| t.name == spec.name) {
                *existing = spec.clone();
            } else {
                out.push(spec.clone());
            }
        }
        if let Some(ref active) = self.config.active_tools {
            let active: std::collections::HashSet<&str> =
                active.iter().map(String::as_str).collect();
            out.retain(|t| active.contains(t.name.as_str()));
        }
        out
    }

    /// Snapshot the per-call backend config from the node config.
    fn backend_config(&self) -> ChatBackendConfig {
        ChatBackendConfig {
            api_key: self.resolve_api_key(),
            base_url: self.resolve_base_url(),
            model: self.resolve_model(),
            system_prompt: self.config.system_prompt.clone(),
            max_tokens: self.config.max_tokens,
            temperature: self.config.temperature,
            top_p: self.config.top_p,
            history_turns: self.config.history_turns,
            streaming: self.config.streaming,
            output_channel: self
                .config
                .output_channel
                .clone()
                .unwrap_or_else(|| TEXT_CHANNEL_DEFAULT.to_string()),
            reasoning_channel: self
                .config
                .reasoning_channel
                .clone()
                .filter(|s| !s.is_empty()),
            tools: self.build_tool_registry(),
            tool_choice: self.config.tool_choice.clone(),
        }
    }

    /// Extract a user-facing text payload from `Text` or `Json` input.
    ///
    /// Returns `Ok(None)` for inputs we should silently drop (empty
    /// text, `Json` with no `content`/`text` field) and `Err` for
    /// type-mismatched inputs.
    fn extract_user_text(data: &RuntimeData) -> Result<Option<String>, Error> {
        match data {
            RuntimeData::Text(t) => {
                if t.trim().is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(t.clone()))
                }
            }
            RuntimeData::Json(j) => {
                match j.get("content").or(j.get("text")).and_then(|v| v.as_str()) {
                    Some(s) if !s.is_empty() => Ok(Some(s.to_string())),
                    _ => {
                        tracing::debug!(
                            node = "OpenAIChatNode",
                            "Dropping JSON input with no `content`/`text` field"
                        );
                        Ok(None)
                    }
                }
            }
            other => Err(Error::Execution(format!(
                "OpenAIChatNode expects Text or Json input, got: {}",
                other.data_type()
            ))),
        }
    }
}

#[async_trait::async_trait]
impl AsyncStreamingNode for OpenAIChatNode {
    fn node_type(&self) -> &str {
        "OpenAIChatNode"
    }

    async fn initialize(&self, ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        let api_key = self.resolve_api_key();
        let base_url = self.resolve_base_url();
        let model = self.resolve_model();

        let masked_key = match api_key.as_deref() {
            Some(k) if k.len() > 8 => format!("{}****{}", &k[..4], &k[k.len() - 4..]),
            Some(_) => "****".to_string(),
            None => "(none)".to_string(),
        };

        ctx.emit_progress(
            "loading_node",
            &format!("OpenAIChatNode: model={}, endpoint={}", model, base_url),
        );

        tracing::info!(
            node = "OpenAIChatNode",
            model = %model,
            base_url = %base_url,
            api_key = %masked_key,
            streaming = self.config.streaming,
            history_turns = self.config.history_turns,
            "Initializing OpenAIChatNode"
        );

        ctx.emit_progress("ready", &format!("OpenAIChatNode ready (model={})", model));

        Ok(())
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        // Single-shot path: buffer the streaming output and return the
        // last frame. The streaming path (process_streaming) is
        // preferred.
        let mut outputs: Vec<RuntimeData> = Vec::new();
        let session_id = "default".to_string();

        let user_text = match Self::extract_user_text(&data)? {
            Some(t) => t,
            None => {
                return Ok(RuntimeData::Text(crate::data::tag_text_str(
                    "",
                    TEXT_CHANNEL_DEFAULT,
                )))
            }
        };

        let cfg = self.backend_config();
        let user_msg = serde_json::json!({"role": "user", "content": user_text});
        let mut cb = |out: RuntimeData| -> Result<(), Error> {
            outputs.push(out);
            Ok(())
        };
        // Single-shot `process` path has no ctx (legacy
        // AsyncStreamingNode shape), so the backend gets no cancel
        // gate. The pipeline runtime always uses the streaming path.
        self.backend
            .run(&session_id, user_msg, &cfg, None, &mut cb)
            .await?;

        outputs
            .into_iter()
            .last()
            .ok_or_else(|| Error::Execution("OpenAIChatNode: no output generated".into()))
    }

    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        session_id: Option<String>,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        // Legacy entry point — kept so tests / direct callers that
        // don't have a `NodeRuntimeContext` still work. Production
        // dispatch from the session router goes through
        // `process_streaming_with_ctx` (see below) so the chat
        // backend receives the per-call cancel gate and can take a
        // ProtectGuard for non-cancelable tools. From here we have
        // no ctx, so the backend runs without barge suppression.
        let sid = session_id.unwrap_or_else(|| "default".to_string());

        let user_text = match Self::extract_user_text(&data)? {
            Some(t) => t,
            None => return Ok(0),
        };

        let cfg = self.backend_config();
        let user_msg = serde_json::json!({"role": "user", "content": user_text});
        self.backend
            .run(&sid, user_msg, &cfg, None, &mut callback)
            .await
    }

    /// Ctx-aware streaming entry. Forwards `ctx.cancel_gate` to the
    /// chat backend so any streamed tool call resolving to a
    /// `ToolSpec` with `cancelable=false` can suppress barge_in for
    /// the rest of the call. Without this override the
    /// AsyncStreamingNode default would route through
    /// `process_streaming` (above) which has no ctx and therefore no
    /// cancel gate, leaving non-cancelable tools at the mercy of any
    /// VAD-triggered barge.
    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let sid = if ctx.session_id().is_empty() {
            "default".to_string()
        } else {
            ctx.session_id().to_string()
        };

        let user_text = match Self::extract_user_text(&data)? {
            Some(t) => t,
            None => return Ok(0),
        };

        let cfg = self.backend_config();
        let user_msg = serde_json::json!({"role": "user", "content": user_text});
        // Reach through the read-trait `as_any` escape hatch to grab
        // the heavy `Arc<CancelGate>` field — plugins can't do this and
        // don't need to (they're cancelled by the universal future-drop
        // mechanism), but the chat backend wants the explicit gate so
        // it can `.protect()` non-cancelable tool dispatches.
        let cancel_gate = ctx
            .as_any()
            .downcast_ref::<crate::nodes::NodeRuntimeContext>()
            .map(|c| Arc::clone(&c.cancel_gate));
        self.backend
            .run(&sid, user_msg, &cfg, cancel_gate, &mut callback)
            .await
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

pub struct OpenAIChatNodeFactory;

impl crate::nodes::StreamingNodeFactory for OpenAIChatNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn crate::nodes::StreamingNode>, Error> {
        let config: OpenAIChatConfig = serde_json::from_value(params.clone()).unwrap_or_default();
        let node = OpenAIChatNode::with_config(config);
        Ok(Box::new(crate::nodes::AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "OpenAIChatNode"
    }

    fn is_multi_output_streaming(&self) -> bool {
        true // Emits one RuntimeData::Text per streaming token.
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{
            LatencyClass, NodeCapabilitiesSchema, NodeSchema, RuntimeDataType,
        };
        Some(
            NodeSchema::new("OpenAIChatNode")
                .description("Streaming chat completion node for OpenAI-compatible APIs (OpenAI, Azure, vLLM, Ollama, etc.)")
                .category("llm")
                .accepts([RuntimeDataType::Text, RuntimeDataType::Json])
                .produces([RuntimeDataType::Text])
                .capabilities(NodeCapabilitiesSchema {
                    parallelizable: false,
                    batch_aware: false,
                    supports_control: false,
                    latency_class: LatencyClass::Slow,
                })
                .config_schema_from::<OpenAIChatConfig>(),
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::HistoryEntry;
    use crate::nodes::{schema::RuntimeDataType, StreamingNodeFactory};

    #[test]
    fn test_config_defaults() {
        let config = OpenAIChatConfig::default();
        assert_eq!(config.history_turns, 10);
        assert!(config.streaming);
        assert_eq!(config.output_channel, None);
    }

    #[test]
    fn test_config_from_camel_case() {
        let params = serde_json::json!({
            "apiKey": "sk-test",
            "baseUrl": "http://localhost:8080/v1",
            "model": "qwen2.5-7b",
            "systemPrompt": "You are helpful.",
            "outputChannel": "ui",
            "maxTokens": 2048,
            "temperature": 0.7,
            "topP": 0.9,
            "historyTurns": 5,
            "streaming": false,
        });
        let config: OpenAIChatConfig = serde_json::from_value(params).unwrap();
        assert_eq!(config.api_key, Some("sk-test".into()));
        assert_eq!(config.base_url, Some("http://localhost:8080/v1".into()));
        assert_eq!(config.model, Some("qwen2.5-7b".into()));
        assert_eq!(config.system_prompt, Some("You are helpful.".into()));
        assert_eq!(config.output_channel, Some("ui".into()));
        assert_eq!(config.max_tokens, Some(2048));
        assert!((config.temperature.unwrap() - 0.7).abs() < f32::EPSILON);
        assert!((config.top_p.unwrap() - 0.9).abs() < f32::EPSILON);
        assert_eq!(config.history_turns, 5);
        assert!(!config.streaming);
    }

    #[test]
    fn test_config_from_snake_case() {
        let params = serde_json::json!({
            "api_key": "sk-test",
            "base_url": "http://localhost:8080/v1",
            "model": "gpt-4o",
            "system_prompt": "Be concise.",
            "output_channel": "tts",
            "max_tokens": 1024,
            "temperature": 0.5,
            "top_p": 0.95,
            "history_turns": 20,
            "streaming": true,
        });
        let config: OpenAIChatConfig = serde_json::from_value(params).unwrap();
        assert_eq!(config.api_key, Some("sk-test".into()));
        assert_eq!(config.base_url, Some("http://localhost:8080/v1".into()));
        assert_eq!(config.model, Some("gpt-4o".into()));
        assert_eq!(config.system_prompt, Some("Be concise.".into()));
        assert_eq!(config.output_channel, Some("tts".into()));
        assert_eq!(config.max_tokens, Some(1024));
        assert_eq!(config.history_turns, 20);
        assert!(config.streaming);
    }

    #[test]
    fn test_node_type() {
        let config = OpenAIChatConfig::default();
        let node = OpenAIChatNode::with_config(config);
        assert_eq!(node.node_type(), "OpenAIChatNode");
    }

    #[test]
    fn test_resolve_base_url_default() {
        let config = OpenAIChatConfig::default();
        let node = OpenAIChatNode::with_config(config);
        assert_eq!(node.resolve_base_url(), "https://api.openai.com/v1");
    }

    #[test]
    fn test_resolve_model_default() {
        let config = OpenAIChatConfig::default();
        let node = OpenAIChatNode::with_config(config);
        assert_eq!(node.resolve_model(), "gpt-4o-mini");
    }

    #[test]
    fn test_missing_api_key_returns_none() {
        let config = OpenAIChatConfig::default();
        let node = OpenAIChatNode::with_config(config);
        std::env::remove_var("OPENAI_API_KEY");
        assert!(node.resolve_api_key().is_none());
    }

    #[test]
    fn test_resolve_api_key_from_config() {
        std::env::remove_var("OPENAI_API_KEY");
        let mut config = OpenAIChatConfig::default();
        config.api_key = Some("sk-test".into());
        let node = OpenAIChatNode::with_config(config);
        assert_eq!(node.resolve_api_key().as_deref(), Some("sk-test"));
    }

    #[test]
    fn test_resolve_api_key_empty_string_treated_as_none() {
        std::env::remove_var("OPENAI_API_KEY");
        let mut config = OpenAIChatConfig::default();
        config.api_key = Some(String::new());
        let node = OpenAIChatNode::with_config(config);
        assert!(node.resolve_api_key().is_none());
    }

    /// Build a request body via the backend's test hook for assertions.
    fn body_for(node: &OpenAIChatNode, session: &str, user_text: &str) -> Value {
        let cfg = node.backend_config();
        let user_msg = serde_json::json!({"role": "user", "content": user_text});
        node.backend
            .build_request_body_for_test(session, &cfg, &user_msg)
    }

    #[test]
    fn test_build_request_body_includes_system_prompt() {
        let mut config = OpenAIChatConfig::default();
        config.api_key = Some("sk-test".into());
        config.system_prompt = Some("You are a translator.".into());
        config.model = Some("gpt-4o".into());
        let node = OpenAIChatNode::with_config(config);
        let body = body_for(&node, "sess1", "Hello");
        let messages = body["messages"].as_array().unwrap();
        assert_eq!(messages[0]["role"], "system");
        assert_eq!(messages[0]["content"], "You are a translator.");
        assert_eq!(messages[1]["role"], "user");
        assert_eq!(messages[1]["content"], "Hello");
    }

    #[test]
    fn test_build_request_body_without_system_prompt() {
        let mut config = OpenAIChatConfig::default();
        config.api_key = Some("sk-test".into());
        let node = OpenAIChatNode::with_config(config);
        let body = body_for(&node, "sess1", "Hello");
        let messages = body["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["role"], "user");
    }

    #[test]
    fn test_history_append_and_retrieve() {
        let mut config = OpenAIChatConfig::default();
        config.api_key = Some("sk-test".into());
        let node = OpenAIChatNode::with_config(config);
        node.backend.append_history("s1", HistoryEntry::user("Hi"));
        node.backend
            .append_history("s1", HistoryEntry::assistant_text("Hello!"));

        let entries = node.backend.history_snapshot("s1");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].role(), "user");
        assert_eq!(entries[0].message["content"], "Hi");
        assert_eq!(entries[1].role(), "assistant");
        assert_eq!(entries[1].message["content"], "Hello!");
    }

    #[test]
    fn test_history_round_trips_tool_calls() {
        // Tools-only replies must commit an assistant message with
        // tool_calls + a tool result per call, otherwise the model
        // can't recall its own outputs and the next request 400s on
        // dangling tool_call_ids.
        let mut config = OpenAIChatConfig::default();
        config.api_key = Some("sk-test".into());
        config.enable_say_tool = true;
        let node = OpenAIChatNode::with_config(config);

        node.backend.append_history("s1", HistoryEntry::user("hi"));
        node.backend.extend_history(
            "s1",
            vec![
                HistoryEntry::assistant_with_tool_calls(
                    None,
                    serde_json::json!([{
                        "id": "call_0",
                        "type": "function",
                        "function": {
                            "name": "say",
                            "arguments": "{\"text\":\"Hello!\"}",
                        },
                    }]),
                ),
                HistoryEntry::tool_result("call_0", "say", ""),
            ],
        );

        let body = body_for(&node, "s1", "what did you just say?");
        let messages = body["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 4);
        assert_eq!(messages[0]["role"], "user");
        assert_eq!(messages[1]["role"], "assistant");
        assert!(messages[1]["content"].is_null());
        assert_eq!(messages[1]["tool_calls"][0]["id"], "call_0");
        assert_eq!(messages[1]["tool_calls"][0]["function"]["name"], "say");
        assert_eq!(messages[2]["role"], "tool");
        assert_eq!(messages[2]["tool_call_id"], "call_0");
        assert_eq!(messages[3]["role"], "user");
        assert_eq!(messages[3]["content"], "what did you just say?");
    }

    #[test]
    fn test_history_window_keeps_tool_results_intact() {
        let mut config = OpenAIChatConfig::default();
        config.api_key = Some("sk-test".into());
        config.history_turns = 1;
        let node = OpenAIChatNode::with_config(config);

        node.backend.append_history("s1", HistoryEntry::user("hi"));
        node.backend
            .append_history("s1", HistoryEntry::assistant_text("hello"));
        node.backend
            .append_history("s1", HistoryEntry::user("again"));
        node.backend.extend_history(
            "s1",
            vec![
                HistoryEntry::assistant_with_tool_calls(
                    None,
                    serde_json::json!([{
                        "id": "call_0",
                        "type": "function",
                        "function": {"name": "say", "arguments": "{}"},
                    }]),
                ),
                HistoryEntry::tool_result("call_0", "say", ""),
            ],
        );

        let body = body_for(&node, "s1", "next");
        let messages = body["messages"].as_array().unwrap();
        let roles: Vec<&str> = messages
            .iter()
            .map(|m| m["role"].as_str().unwrap_or(""))
            .collect();
        assert_eq!(roles, vec!["user", "assistant", "tool", "user"]);
        assert_eq!(messages[1]["tool_calls"][0]["id"], "call_0");
        assert_eq!(messages[2]["tool_call_id"], "call_0");
    }

    #[test]
    fn test_factory_node_type() {
        let factory = OpenAIChatNodeFactory;
        assert_eq!(factory.node_type(), "OpenAIChatNode");
        assert!(factory.is_multi_output_streaming());
    }

    #[test]
    fn test_provider_default_is_openai() {
        let cfg = OpenAIChatConfig::default();
        assert_eq!(cfg.provider, crate::llm::ProviderKind::OpenAI);
    }

    #[test]
    fn test_provider_anthropic_uses_anthropic_default_base_url() {
        let mut cfg = OpenAIChatConfig::default();
        cfg.provider = crate::llm::ProviderKind::Anthropic;
        let node = OpenAIChatNode::with_config(cfg);
        assert_eq!(node.resolve_base_url(), "https://api.anthropic.com/v1");
    }

    #[test]
    fn test_provider_explicit_base_url_overrides_default() {
        let mut cfg = OpenAIChatConfig::default();
        cfg.provider = crate::llm::ProviderKind::Anthropic;
        cfg.base_url = Some("http://localhost:9999/v1".into());
        let node = OpenAIChatNode::with_config(cfg);
        assert_eq!(node.resolve_base_url(), "http://localhost:9999/v1");
    }

    #[test]
    fn test_factory_creates_node() {
        let factory = OpenAIChatNodeFactory;
        let params = serde_json::json!({
            "api_key": "sk-test",
            "model": "gpt-4o-mini",
        });
        let result = factory.create("node1".into(), &params, None);
        assert!(result.is_ok());
        let node = result.unwrap();
        assert_eq!(node.node_type(), "OpenAIChatNode");
    }

    #[test]
    fn test_factory_schema() {
        let factory = OpenAIChatNodeFactory;
        let schema = factory.schema().unwrap();
        assert_eq!(schema.node_type, "OpenAIChatNode");
        assert!(!schema.description.as_ref().unwrap().is_empty());
        assert_eq!(schema.category, Some("llm".into()));
        assert!(schema.accepts.contains(&RuntimeDataType::Text));
        assert!(schema.accepts.contains(&RuntimeDataType::Json));
        assert!(schema.produces.contains(&RuntimeDataType::Text));
        assert!(schema.config_schema.is_some());
    }

    #[test]
    fn test_sse_parse_basic() {
        let line = "data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"}}]}";
        let payload = &line[6..];
        let json: Value = serde_json::from_str(payload).unwrap();
        let content = json["choices"][0]["delta"]["content"].as_str();
        assert_eq!(content, Some("Hello"));
    }

    #[test]
    fn test_sse_done_marker() {
        assert_eq!("data: [DONE]".len() > 6, true);
        let payload = "data: [DONE]";
        assert_eq!(payload[6..].trim(), "[DONE]");
    }
}
