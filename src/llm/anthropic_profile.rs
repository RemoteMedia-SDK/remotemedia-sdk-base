//! Anthropic Messages API profile.
//!
//! Wire-shapes a vendor-agnostic [`ChatRequest`] (built around the
//! OpenAI chat-completions message shape) into Anthropic's
//! `/v1/messages` request body, and parses Anthropic's SSE stream
//! into [`ChatStreamEvent`]s the [`crate::llm::ChatBackend`] handles
//! uniformly.
//!
//! Differences vs. [`crate::llm::OpenAIProfile`]:
//!
//! - Endpoint: `POST {base}/messages` (not `/chat/completions`).
//! - Auth: `x-api-key: <key>` plus `anthropic-version: 2023-06-01`,
//!   not `Authorization: Bearer …`.
//! - Body: `system` is a top-level field, not a `system` role
//!   message; `messages` only carries user/assistant.
//! - Tools: `tools[]` items are flat `{name, description, input_schema}`
//!   objects (no `type:"function"` envelope).
//! - Tool calls in responses arrive as `content_block_start` events
//!   carrying `tool_use` blocks, with arguments streamed as
//!   `input_json_delta` events.
//! - History: assistant messages carry mixed-content arrays of
//!   `text` and `tool_use` blocks; user messages with tool results
//!   carry `tool_result` blocks. The translator below converts the
//!   OpenAI-shape history entries the backend gives it into this
//!   shape on the fly — the backend itself stays vendor-neutral.

use crate::llm::provider::{ChatRequest, ChatStreamEvent, ProviderProfile};
use serde_json::Value;

/// Anthropic Messages API profile.
#[derive(Debug, Default, Clone, Copy)]
pub struct AnthropicProfile;

impl AnthropicProfile {
    /// Translate one OpenAI-shape history message into the
    /// content-blocks shape Anthropic expects on the wire. The backend
    /// stores history as OpenAI-shape messages (assistant+tool_calls
    /// followed by tool-role results); on each request we re-shape
    /// them here so the same in-memory history powers both vendors.
    fn shape_message(msg: &Value) -> Option<Value> {
        let role = msg.get("role").and_then(Value::as_str)?;
        match role {
            "system" => None, // hoisted to top-level `system` field
            "user" => Some(Self::shape_user(msg)),
            "assistant" => Some(Self::shape_assistant(msg)),
            // OpenAI-style `tool` messages get folded into the *next*
            // user message's content as `tool_result` blocks. We
            // collapse them at the messages-array level in
            // `shape_request` instead of here.
            "tool" => None,
            _ => None,
        }
    }

    fn shape_user(msg: &Value) -> Value {
        // OpenAI user content can be either a string or an array of
        // content-parts. Anthropic accepts the array form natively;
        // string content gets wrapped.
        let content = msg.get("content").cloned().unwrap_or(Value::Null);
        match content {
            Value::String(s) => serde_json::json!({
                "role": "user",
                "content": [{"type": "text", "text": s}],
            }),
            Value::Array(parts) => {
                // Re-shape OpenAI content parts into Anthropic content
                // blocks: `text` passes through; `image_url` becomes a
                // `source` block with base64 data; `input_audio` is
                // not supported on Anthropic Messages API and is
                // dropped with a warning.
                let blocks: Vec<Value> = parts
                    .into_iter()
                    .filter_map(Self::shape_user_part)
                    .collect();
                serde_json::json!({"role": "user", "content": blocks})
            }
            _ => serde_json::json!({"role": "user", "content": []}),
        }
    }

    fn shape_user_part(part: Value) -> Option<Value> {
        let kind = part.get("type").and_then(Value::as_str)?;
        match kind {
            "text" => part
                .get("text")
                .and_then(Value::as_str)
                .map(|t| serde_json::json!({"type": "text", "text": t})),
            "image_url" => {
                let url = part.pointer("/image_url/url").and_then(Value::as_str)?;
                if let Some(suffix) = url.strip_prefix("data:") {
                    // `image/png;base64,XXXX` → media_type + data.
                    let mut parts = suffix.splitn(2, ";base64,");
                    let media_type = parts.next()?;
                    let data = parts.next()?;
                    Some(serde_json::json!({
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": data,
                        },
                    }))
                } else {
                    Some(serde_json::json!({
                        "type": "image",
                        "source": {"type": "url", "url": url},
                    }))
                }
            }
            "input_audio" => {
                tracing::warn!(
                    "[anthropic] input_audio content parts are not supported by the Messages API; dropping"
                );
                None
            }
            _ => None,
        }
    }

    fn shape_assistant(msg: &Value) -> Value {
        // OpenAI assistant message can have:
        //   - content: "..." (plain reply)
        //   - content: null + tool_calls: [...] (tools-only)
        //   - both (rare)
        let mut blocks: Vec<Value> = Vec::new();
        if let Some(s) = msg.get("content").and_then(Value::as_str) {
            if !s.is_empty() {
                blocks.push(serde_json::json!({"type": "text", "text": s}));
            }
        }
        if let Some(tcs) = msg.get("tool_calls").and_then(Value::as_array) {
            for tc in tcs {
                let id = tc.get("id").and_then(Value::as_str).unwrap_or("");
                let name = tc
                    .pointer("/function/name")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                // Anthropic wants the input as a JSON object, not a
                // stringified one. Fall back to an empty object on
                // parse failure so we don't wedge the request.
                let input: Value = tc
                    .pointer("/function/arguments")
                    .and_then(Value::as_str)
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
                blocks.push(serde_json::json!({
                    "type": "tool_use",
                    "id": id,
                    "name": name,
                    "input": input,
                }));
            }
        }
        serde_json::json!({"role": "assistant", "content": blocks})
    }

    /// Build the messages array from OpenAI-shape history, folding
    /// `tool` role results into the next user message as
    /// `tool_result` blocks.
    fn shape_messages(messages: &[Value]) -> Vec<Value> {
        let mut out: Vec<Value> = Vec::new();
        let mut pending_results: Vec<Value> = Vec::new();
        for msg in messages {
            let role = msg.get("role").and_then(Value::as_str).unwrap_or("");
            if role == "tool" {
                let id = msg
                    .get("tool_call_id")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let content = msg.get("content").and_then(Value::as_str).unwrap_or("");
                pending_results.push(serde_json::json!({
                    "type": "tool_result",
                    "tool_use_id": id,
                    "content": content,
                }));
                continue;
            }
            if role == "user" && !pending_results.is_empty() {
                // Inject the pending tool_result blocks at the front
                // of the next user message's content.
                let mut shaped = Self::shape_user(msg);
                if let Some(arr) = shaped["content"].as_array_mut() {
                    let mut combined: Vec<Value> = pending_results.drain(..).collect();
                    combined.append(arr);
                    *arr = combined;
                }
                out.push(shaped);
                continue;
            }
            if let Some(shaped) = Self::shape_message(msg) {
                out.push(shaped);
            }
        }
        // Trailing tool results without a following user turn — wrap
        // them in a synthetic user message so Anthropic accepts the
        // request. Should be rare; means the backend committed
        // tool_results but no follow-up user turn exists yet (which
        // can happen for return_value tools).
        if !pending_results.is_empty() {
            out.push(serde_json::json!({
                "role": "user",
                "content": pending_results,
            }));
        }
        out
    }

    fn shape_tools(specs: &[crate::nodes::tool_spec::ToolSpec]) -> Value {
        let arr: Vec<Value> = specs
            .iter()
            .map(|s| {
                serde_json::json!({
                    "name": s.name,
                    "description": s.description,
                    "input_schema": s.parameters,
                })
            })
            .collect();
        Value::Array(arr)
    }
}

impl ProviderProfile for AnthropicProfile {
    fn name(&self) -> &'static str {
        "anthropic"
    }

    fn endpoint(&self, base_url: &str) -> String {
        format!("{}/messages", base_url)
    }

    fn apply_auth(
        &self,
        req: reqwest::RequestBuilder,
        api_key: Option<&str>,
    ) -> reqwest::RequestBuilder {
        let mut req = req.header("anthropic-version", "2023-06-01");
        if let Some(k) = api_key {
            if !k.is_empty() {
                req = req.header("x-api-key", k);
            }
        }
        req
    }

    fn shape_request(&self, req: &ChatRequest<'_>) -> Value {
        // Pull out the system prompt (if any) — Anthropic puts it in a
        // top-level `system` field, not the messages array.
        let system: Option<&str> = req
            .messages
            .iter()
            .find(|m| m["role"] == "system")
            .and_then(|m| m["content"].as_str());

        let messages = Self::shape_messages(&req.messages);

        let mut body = serde_json::json!({
            "model": req.model,
            "messages": messages,
            "stream": req.streaming,
            // Anthropic requires max_tokens. Fall back to a generous
            // default so a manifest that omits it still works.
            "max_tokens": req.max_tokens.unwrap_or(4096),
        });
        if let Some(s) = system {
            body["system"] = Value::String(s.to_string());
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
            body["tools"] = Self::shape_tools(req.tools);
            if let Some(tc) = req.tool_choice {
                body["tool_choice"] = tc.clone();
            }
        }
        body
    }

    fn parse_sse_payload(&self, payload: &Value) -> Vec<ChatStreamEvent> {
        // Anthropic streams a fixed set of event types via SSE:
        // `message_start`, `content_block_start`, `content_block_delta`,
        // `content_block_stop`, `message_delta`, `message_stop`.
        // We only emit events for `content_block_start` (when the
        // block is a `tool_use`), `content_block_delta` (text or
        // input_json), and `message_stop` (Done).
        let event_type = payload.get("type").and_then(Value::as_str).unwrap_or("");
        let index = payload.get("index").and_then(Value::as_u64).unwrap_or(0);
        match event_type {
            "content_block_start" => {
                let block = payload.get("content_block");
                let kind = block
                    .and_then(|b| b.get("type"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if kind == "tool_use" {
                    let id = block
                        .and_then(|b| b.get("id"))
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    let name = block
                        .and_then(|b| b.get("name"))
                        .and_then(Value::as_str)
                        .map(str::to_string);
                    return vec![ChatStreamEvent::ToolCallDelta {
                        index,
                        id,
                        name,
                        arguments_chunk: None,
                    }];
                }
                Vec::new()
            }
            "content_block_delta" => {
                let delta = match payload.get("delta") {
                    Some(d) => d,
                    None => return Vec::new(),
                };
                let dtype = delta.get("type").and_then(Value::as_str).unwrap_or("");
                match dtype {
                    "text_delta" => delta
                        .get("text")
                        .and_then(Value::as_str)
                        .filter(|s| !s.is_empty())
                        .map(|s| vec![ChatStreamEvent::VisibleText(s.to_string())])
                        .unwrap_or_default(),
                    "input_json_delta" => {
                        let chunk = delta
                            .get("partial_json")
                            .and_then(Value::as_str)
                            .map(str::to_string);
                        vec![ChatStreamEvent::ToolCallDelta {
                            index,
                            id: None,
                            name: None,
                            arguments_chunk: chunk,
                        }]
                    }
                    _ => Vec::new(),
                }
            }
            "message_stop" => vec![ChatStreamEvent::Done],
            _ => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_appends_messages() {
        let p = AnthropicProfile;
        assert_eq!(
            p.endpoint("https://api.anthropic.com/v1"),
            "https://api.anthropic.com/v1/messages"
        );
    }

    #[test]
    fn apply_auth_sets_required_headers() {
        let client = reqwest::Client::new();
        let req = client.post("https://api.anthropic.com/v1/messages");
        let p = AnthropicProfile;
        let req = p.apply_auth(req, Some("sk-ant-test"));
        // RequestBuilder doesn't expose headers directly; build it.
        let built = req.build().unwrap();
        assert_eq!(
            built.headers().get("anthropic-version").unwrap(),
            "2023-06-01"
        );
        assert_eq!(built.headers().get("x-api-key").unwrap(), "sk-ant-test");
    }

    #[test]
    fn system_prompt_hoisted_to_top_level() {
        let p = AnthropicProfile;
        let messages = vec![
            serde_json::json!({"role":"system","content":"Be helpful."}),
            serde_json::json!({"role":"user","content":"Hi"}),
        ];
        let req = ChatRequest {
            model: "claude-3-5-haiku-latest",
            messages,
            tools: &[],
            tool_choice: None,
            max_tokens: Some(256),
            temperature: None,
            top_p: None,
            streaming: true,
        };
        let body = p.shape_request(&req);
        assert_eq!(body["system"], "Be helpful.");
        let msgs = body["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["role"], "user");
    }

    #[test]
    fn image_part_is_shaped_as_base64_source() {
        let p = AnthropicProfile;
        let messages = vec![serde_json::json!({
            "role": "user",
            "content": [
                {"type": "text", "text": "what is this"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAAA"}}
            ]
        })];
        let req = ChatRequest {
            model: "claude-3-5-haiku-latest",
            messages,
            tools: &[],
            tool_choice: None,
            max_tokens: Some(64),
            temperature: None,
            top_p: None,
            streaming: false,
        };
        let body = p.shape_request(&req);
        let blocks = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[1]["type"], "image");
        assert_eq!(blocks[1]["source"]["type"], "base64");
        assert_eq!(blocks[1]["source"]["media_type"], "image/png");
        assert_eq!(blocks[1]["source"]["data"], "AAAA");
    }

    #[test]
    fn assistant_tool_calls_become_tool_use_blocks() {
        let p = AnthropicProfile;
        let messages = vec![
            serde_json::json!({"role":"user","content":"go"}),
            serde_json::json!({
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "call_0",
                    "type": "function",
                    "function": {"name": "say", "arguments": "{\"text\":\"Hi!\"}"}
                }]
            }),
            serde_json::json!({"role":"tool","tool_call_id":"call_0","name":"say","content":""}),
            serde_json::json!({"role":"user","content":"again"}),
        ];
        let req = ChatRequest {
            model: "claude-3-5-haiku-latest",
            messages,
            tools: &[],
            tool_choice: None,
            max_tokens: Some(64),
            temperature: None,
            top_p: None,
            streaming: false,
        };
        let body = p.shape_request(&req);
        let msgs = body["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 3); // user, assistant, user (with tool_result prepended)
                                   // assistant block carries tool_use
        let asst_blocks = msgs[1]["content"].as_array().unwrap();
        assert_eq!(asst_blocks[0]["type"], "tool_use");
        assert_eq!(asst_blocks[0]["id"], "call_0");
        assert_eq!(asst_blocks[0]["name"], "say");
        assert_eq!(asst_blocks[0]["input"]["text"], "Hi!");
        // The follow-up user message carries the tool_result up front.
        let next_user_blocks = msgs[2]["content"].as_array().unwrap();
        assert_eq!(next_user_blocks[0]["type"], "tool_result");
        assert_eq!(next_user_blocks[0]["tool_use_id"], "call_0");
    }

    #[test]
    fn parse_text_delta() {
        let p = AnthropicProfile;
        let payload = serde_json::json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "Hello"}
        });
        let evs = p.parse_sse_payload(&payload);
        assert!(matches!(&evs[0], ChatStreamEvent::VisibleText(s) if s == "Hello"));
    }

    #[test]
    fn parse_tool_use_start_and_input_delta() {
        let p = AnthropicProfile;
        let start = serde_json::json!({
            "type": "content_block_start",
            "index": 1,
            "content_block": {"type": "tool_use", "id": "tu_1", "name": "say"}
        });
        let evs = p.parse_sse_payload(&start);
        match &evs[0] {
            ChatStreamEvent::ToolCallDelta {
                index,
                id,
                name,
                arguments_chunk,
            } => {
                assert_eq!(*index, 1);
                assert_eq!(id.as_deref(), Some("tu_1"));
                assert_eq!(name.as_deref(), Some("say"));
                assert!(arguments_chunk.is_none());
            }
            other => panic!("expected ToolCallDelta start, got {:?}", other),
        }

        let delta = serde_json::json!({
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "input_json_delta", "partial_json": "{\"text\":"}
        });
        let evs = p.parse_sse_payload(&delta);
        match &evs[0] {
            ChatStreamEvent::ToolCallDelta {
                arguments_chunk, ..
            } => assert_eq!(arguments_chunk.as_deref(), Some("{\"text\":")),
            other => panic!("expected ToolCallDelta delta, got {:?}", other),
        }
    }

    #[test]
    fn parse_message_stop_emits_done() {
        let p = AnthropicProfile;
        let payload = serde_json::json!({"type": "message_stop"});
        let evs = p.parse_sse_payload(&payload);
        assert!(matches!(evs[0], ChatStreamEvent::Done));
    }
}
