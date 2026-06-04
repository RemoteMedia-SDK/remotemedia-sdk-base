//! Vendor-agnostic chat-completion provider profile.
//!
//! A `ProviderProfile` shapes the wire format for one LLM vendor:
//! endpoint URL, auth header, request body, SSE chunk parsing. The
//! `ChatBackend` is otherwise modality- and vendor-agnostic — it just
//! drives the profile and routes streaming events.
//!
//! v1 ships only [`OpenAIProfile`] (cloud OpenAI, Azure, vLLM, modern
//! llama.cpp, Ollama). Anthropic and Gemini profiles will be added in
//! follow-up steps and don't affect the trait surface.

use crate::nodes::tool_spec::{to_openai_tools_array, ToolSpec};
use serde_json::Value;

// ---------------------------------------------------------------------------
// ChatRequest — modality-agnostic request payload the backend hands to
// the profile to be wire-shaped.
// ---------------------------------------------------------------------------

/// Pre-built chat-completion request, before vendor shaping.
///
/// `messages` is the full ordered array (system + history + new user
/// message). For OpenAI the profile passes it through verbatim; for
/// other vendors the profile rewrites field names.
#[derive(Debug, Clone)]
pub struct ChatRequest<'a> {
    pub model: &'a str,
    pub messages: Vec<Value>,
    pub tools: &'a [ToolSpec],
    pub tool_choice: Option<&'a Value>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub streaming: bool,
}

// ---------------------------------------------------------------------------
// ChatStreamEvent — vendor-neutral SSE chunk events.
// ---------------------------------------------------------------------------

/// One unit of streaming output from the model, normalised across
/// vendors. The backend handles each variant uniformly.
#[derive(Debug, Clone)]
pub enum ChatStreamEvent {
    /// Visible reply token (assistant content).
    VisibleText(String),
    /// Reasoning / chain-of-thought token (Qwen3 / DeepSeek style).
    ReasoningText(String),
    /// One streamed `tool_calls` delta, keyed by `index`.
    ToolCallDelta {
        index: u64,
        id: Option<String>,
        name: Option<String>,
        arguments_chunk: Option<String>,
    },
    /// `[DONE]` sentinel. Profiles that lack this marker can omit it.
    Done,
}

// ---------------------------------------------------------------------------
// ProviderProfile trait
// ---------------------------------------------------------------------------

/// Per-vendor chat-completion profile.
///
/// Implementations are stateless and shared across all sessions of a
/// node — the backend wraps an `Arc<dyn ProviderProfile>`.
pub trait ProviderProfile: Send + Sync {
    /// Vendor name for logs / tracing (e.g. `"openai"`).
    fn name(&self) -> &'static str;

    /// Resolve the full POST URL for streaming chat completions.
    fn endpoint(&self, base_url: &str) -> String;

    /// Apply auth headers / bearer token. Most vendors take a single
    /// `Authorization: Bearer …` header; Anthropic differs.
    fn apply_auth(
        &self,
        req: reqwest::RequestBuilder,
        api_key: Option<&str>,
    ) -> reqwest::RequestBuilder;

    /// Wire-shape the `ChatRequest` into the JSON body the vendor
    /// expects.
    fn shape_request(&self, req: &ChatRequest<'_>) -> Value;

    /// Parse one SSE `data:` payload into zero or more
    /// [`ChatStreamEvent`]s. The backend strips the `data: ` prefix
    /// and `[DONE]` handling before calling — implementations parse a
    /// JSON value (or whatever the vendor sends).
    fn parse_sse_payload(&self, payload: &Value) -> Vec<ChatStreamEvent>;
}

// ---------------------------------------------------------------------------
// OpenAIProfile — default profile.
//
// Wire shape lifted verbatim from the previous in-line implementation
// in `nodes::openai_chat::process_streaming_internal`.
// ---------------------------------------------------------------------------

/// Default profile for OpenAI-compatible chat-completions APIs:
/// cloud OpenAI, Azure OpenAI, vLLM, modern llama.cpp, Ollama.
#[derive(Debug, Default, Clone, Copy)]
pub struct OpenAIProfile;

impl ProviderProfile for OpenAIProfile {
    fn name(&self) -> &'static str {
        "openai"
    }

    fn endpoint(&self, base_url: &str) -> String {
        format!("{}/chat/completions", base_url)
    }

    fn apply_auth(
        &self,
        req: reqwest::RequestBuilder,
        api_key: Option<&str>,
    ) -> reqwest::RequestBuilder {
        match api_key {
            Some(k) if !k.is_empty() => req.bearer_auth(k),
            _ => req,
        }
    }

    fn shape_request(&self, req: &ChatRequest<'_>) -> Value {
        let mut body = serde_json::json!({
            "model": req.model,
            "messages": req.messages,
            "stream": req.streaming,
        });
        if let Some(max_tokens) = req.max_tokens {
            body["max_tokens"] = Value::Number(max_tokens.into());
        }
        if let Some(temperature) = req.temperature {
            if let Some(num) = serde_json::Number::from_f64(temperature as f64) {
                body["temperature"] = Value::Number(num);
            }
        }
        if let Some(top_p) = req.top_p {
            if let Some(num) = serde_json::Number::from_f64(top_p as f64) {
                body["top_p"] = Value::Number(num);
            }
        }
        if !req.tools.is_empty() {
            body["tools"] = to_openai_tools_array(req.tools);
            if let Some(tc) = req.tool_choice {
                body["tool_choice"] = tc.clone();
            }
        }
        body
    }

    fn parse_sse_payload(&self, payload: &Value) -> Vec<ChatStreamEvent> {
        let delta = &payload["choices"][0]["delta"];
        let mut out: Vec<ChatStreamEvent> = Vec::new();

        if let Some(reasoning) = delta.get("reasoning_content").and_then(Value::as_str) {
            if !reasoning.is_empty() {
                out.push(ChatStreamEvent::ReasoningText(reasoning.to_string()));
            }
        }

        if let Some(content) = delta.get("content").and_then(Value::as_str) {
            if !content.is_empty() {
                out.push(ChatStreamEvent::VisibleText(content.to_string()));
            }
        }

        if let Some(tcs) = delta.get("tool_calls").and_then(Value::as_array) {
            for tc in tcs {
                let index = tc.get("index").and_then(Value::as_u64).unwrap_or(0);
                let id = tc
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string);
                let name = tc
                    .pointer("/function/name")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string);
                let arguments_chunk = tc
                    .pointer("/function/arguments")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                out.push(ChatStreamEvent::ToolCallDelta {
                    index,
                    id,
                    name,
                    arguments_chunk,
                });
            }
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_appends_chat_completions() {
        let p = OpenAIProfile;
        assert_eq!(
            p.endpoint("https://api.openai.com/v1"),
            "https://api.openai.com/v1/chat/completions"
        );
    }

    #[test]
    fn shape_request_omits_tools_when_empty() {
        let p = OpenAIProfile;
        let messages = vec![serde_json::json!({"role":"user","content":"hi"})];
        let req = ChatRequest {
            model: "gpt-4o-mini",
            messages: messages.clone(),
            tools: &[],
            tool_choice: None,
            max_tokens: Some(64),
            temperature: Some(0.5),
            top_p: None,
            streaming: true,
        };
        let body = p.shape_request(&req);
        assert_eq!(body["model"], "gpt-4o-mini");
        assert_eq!(body["messages"], serde_json::Value::Array(messages));
        assert_eq!(body["stream"], true);
        assert_eq!(body["max_tokens"], 64);
        assert!(body.get("tools").is_none());
        assert!(body.get("tool_choice").is_none());
    }

    #[test]
    fn parse_visible_text_chunk() {
        let p = OpenAIProfile;
        let payload = serde_json::json!({
            "choices": [{"delta": {"content": "Hello"}}]
        });
        let events = p.parse_sse_payload(&payload);
        assert_eq!(events.len(), 1);
        match &events[0] {
            ChatStreamEvent::VisibleText(t) => assert_eq!(t, "Hello"),
            other => panic!("expected VisibleText, got {:?}", other),
        }
    }

    #[test]
    fn parse_reasoning_text_chunk() {
        let p = OpenAIProfile;
        let payload = serde_json::json!({
            "choices": [{"delta": {"reasoning_content": "thinking..."}}]
        });
        let events = p.parse_sse_payload(&payload);
        assert!(matches!(events[0], ChatStreamEvent::ReasoningText(_)));
    }

    #[test]
    fn parse_tool_call_delta() {
        let p = OpenAIProfile;
        let payload = serde_json::json!({
            "choices": [{"delta": {"tool_calls": [{
                "index": 0,
                "id": "call_abc",
                "function": {"name": "say", "arguments": "{\"text\":"}
            }]}}]
        });
        let events = p.parse_sse_payload(&payload);
        match &events[0] {
            ChatStreamEvent::ToolCallDelta {
                index,
                id,
                name,
                arguments_chunk,
            } => {
                assert_eq!(*index, 0);
                assert_eq!(id.as_deref(), Some("call_abc"));
                assert_eq!(name.as_deref(), Some("say"));
                assert_eq!(arguments_chunk.as_deref(), Some("{\"text\":"));
            }
            other => panic!("expected ToolCallDelta, got {:?}", other),
        }
    }
}
