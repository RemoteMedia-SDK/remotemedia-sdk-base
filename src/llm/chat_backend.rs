//! Modality-agnostic streaming chat-completion backend.
//!
//! Owns the HTTP client, per-session conversation history, and the
//! request → SSE stream → tool-dispatch → history-commit pipeline.
//! The caller (a pipeline node) supplies a pre-shaped user message
//! (text-only or content-parts array) plus per-call config and a
//! callback that receives streaming `RuntimeData::Text` outputs.
//!
//! Vendor-specific wire shaping is delegated to [`ProviderProfile`].
//! v1 only ships [`OpenAIProfile`], but the trait surface is sized to
//! drop in Anthropic / Gemini profiles without touching this module.

use crate::data::{tag_text_str, RuntimeData};
use crate::error::Error;
use crate::llm::history::{window_start, HistoryEntry};
use crate::llm::provider::{ChatRequest, ChatStreamEvent, ProviderProfile};
use crate::llm::tool_dispatch::{dispatch_tool_call, ToolCallAccum};
use crate::nodes::tool_spec::ToolSpec;
use crate::transport::cancel_gate::{CancelGate, ProtectGuard};
use parking_lot::Mutex;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio_stream::StreamExt;

// ---------------------------------------------------------------------------
// ChatBackendConfig — per-call transport + sampling config.
//
// Held by the owning node (`OpenAIChatNode`, `MultimodalLLMNode`) and
// passed by reference into `ChatBackend::run`. Splitting it from the
// node config lets two nodes share a backend, and keeps the backend
// stateless about node-specific input shaping.
// ---------------------------------------------------------------------------

/// Per-call config the backend needs to issue one request.
#[derive(Debug, Clone)]
pub struct ChatBackendConfig {
    pub api_key: Option<String>,
    pub base_url: String,
    pub model: String,
    pub system_prompt: Option<String>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub history_turns: usize,
    pub streaming: bool,
    pub output_channel: String,
    pub reasoning_channel: Option<String>,
    pub tools: Vec<ToolSpec>,
    pub tool_choice: Option<Value>,
}

// ---------------------------------------------------------------------------
// ChatBackend
// ---------------------------------------------------------------------------

/// The shared transport layer for chat-completion-shaped LLMs.
///
/// One backend serves many concurrent sessions, each keyed by
/// `session_id` for history isolation.
pub struct ChatBackend {
    client: Arc<reqwest::Client>,
    history: Arc<Mutex<HashMap<String, Vec<HistoryEntry>>>>,
    profile: Arc<dyn ProviderProfile>,
}

impl ChatBackend {
    pub fn new(profile: Arc<dyn ProviderProfile>) -> Self {
        Self {
            client: Arc::new(
                reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(120))
                    // Disable Nagle: small SSE delta frames from a
                    // local LLM (vLLM/Ollama on 127.0.0.1) get held up
                    // ~40 ms each waiting for more bytes to coalesce
                    // otherwise — step-function token latency for
                    // short replies.
                    .tcp_nodelay(true)
                    // Force HTTP/1.1. Local LLM servers almost always
                    // speak h1; advertising h2 can trigger ALPN
                    // round-trips on connection setup. We pool
                    // connections via this shared client so there's no
                    // multiplexing benefit to gain.
                    .http1_only()
                    .build()
                    .expect("build reqwest client"),
            ),
            history: Arc::new(Mutex::new(HashMap::new())),
            profile,
        }
    }

    /// Append one history entry for `session_id`.
    pub fn append_history(&self, session_id: &str, entry: HistoryEntry) {
        let mut hist = self.history.lock();
        hist.entry(session_id.to_string())
            .or_insert_with(Vec::new)
            .push(entry);
    }

    /// Append a batch atomically. Used to commit an assistant +
    /// tool-result group at end-of-turn so a barge / drop never leaves
    /// a dangling `tool_call_id`.
    pub fn extend_history(&self, session_id: &str, entries: Vec<HistoryEntry>) {
        if entries.is_empty() {
            return;
        }
        let mut hist = self.history.lock();
        hist.entry(session_id.to_string())
            .or_insert_with(Vec::new)
            .extend(entries);
    }

    /// Build the full `messages` array (system + bounded history +
    /// new user message) for one request. Pure function over snapshot
    /// state; doesn't mutate history.
    fn build_messages(
        &self,
        session_id: &str,
        cfg: &ChatBackendConfig,
        user_message: &Value,
    ) -> Vec<Value> {
        let mut messages: Vec<Value> = Vec::new();

        if let Some(ref sys) = cfg.system_prompt {
            messages.push(serde_json::json!({
                "role": "system",
                "content": sys,
            }));
        }

        {
            let hist = self.history.lock();
            if let Some(entries) = hist.get(session_id) {
                let start = window_start(entries, cfg.history_turns);
                for entry in entries.iter().skip(start) {
                    messages.push(entry.message.clone());
                }
            }
        }

        messages.push(user_message.clone());
        messages
    }

    /// Test/inspection helper — `OpenAIChatNode`'s tests reach in for
    /// the constructed request body.
    #[cfg(test)]
    pub(crate) fn build_request_body_for_test(
        &self,
        session_id: &str,
        cfg: &ChatBackendConfig,
        user_message: &Value,
    ) -> Value {
        let messages = self.build_messages(session_id, cfg, user_message);
        let req = ChatRequest {
            model: &cfg.model,
            messages,
            tools: &cfg.tools,
            tool_choice: cfg.tool_choice.as_ref(),
            max_tokens: cfg.max_tokens,
            temperature: cfg.temperature,
            top_p: cfg.top_p,
            streaming: cfg.streaming,
        };
        self.profile.shape_request(&req)
    }

    /// Issue one request, drive the stream, dispatch tool calls, and
    /// commit history. `user_message` is a fully-shaped chat-completion
    /// message (e.g. `{role:"user",content:"hi"}` or
    /// `{role:"user",content:[parts…]}`).
    ///
    /// `cancel_gate` is the per-(session, node) cancellation primitive
    /// owned by the session router — pass `Some(ctx.cancel_gate.clone())`
    /// from production callers so the backend can take a `ProtectGuard`
    /// when a streamed tool call resolves to a [`ToolSpec`] with
    /// `cancelable=false`. Pass `None` from tests / paths where barge
    /// suppression is not required (the guard becomes a no-op).
    ///
    /// Returns the count of streamed tokens (visible text) — the
    /// existing `OpenAIChatNode` contract preserved.
    pub async fn run<F>(
        &self,
        session_id: &str,
        user_message: Value,
        cfg: &ChatBackendConfig,
        cancel_gate: Option<Arc<CancelGate>>,
        callback: &mut F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        // Snapshot history → build messages → record user turn.
        let messages = self.build_messages(session_id, cfg, &user_message);
        self.append_history(session_id, HistoryEntry::user_message(user_message));

        let request = ChatRequest {
            model: &cfg.model,
            messages,
            tools: &cfg.tools,
            tool_choice: cfg.tool_choice.as_ref(),
            max_tokens: cfg.max_tokens,
            temperature: cfg.temperature,
            top_p: cfg.top_p,
            streaming: cfg.streaming,
        };
        let body = self.profile.shape_request(&request);

        let tool_names: Vec<&str> = cfg.tools.iter().map(|t| t.name.as_str()).collect();
        tracing::info!(
            provider = self.profile.name(),
            model = %cfg.model,
            base_url = %cfg.base_url,
            streaming = cfg.streaming,
            tool_count = cfg.tools.len(),
            tools = ?tool_names,
            tool_choice = ?cfg.tool_choice,
            "[llm] sending chat completion request"
        );

        if cfg.streaming {
            self.run_streaming(session_id, cfg, body, cancel_gate, callback)
                .await
        } else {
            // Non-streaming response is a single shot — there's no
            // window in which barge could cancel it cooperatively, so
            // the gate is unused.
            self.run_blocking(session_id, cfg, body, callback).await
        }
    }

    async fn run_streaming<F>(
        &self,
        session_id: &str,
        cfg: &ChatBackendConfig,
        body: Value,
        cancel_gate: Option<Arc<CancelGate>>,
        callback: &mut F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let url = self.profile.endpoint(&cfg.base_url);
        let mut req = self.client.post(url).json(&body);
        req = self.profile.apply_auth(req, cfg.api_key.as_deref());
        let response = req
            .send()
            .await
            .map_err(|e| Error::Execution(format!("LLM HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let err_body = response.text().await.unwrap_or_else(|_| "<no body>".into());
            return Err(Error::Execution(format!(
                "LLM API error {}: {}",
                status, err_body
            )));
        }

        let mut full_text = String::new();
        let mut token_count = 0usize;
        let mut tool_calls: HashMap<u64, ToolCallAccum> = HashMap::new();

        // Barge suppression: holds one `ProtectGuard` per
        // non-cancelable tool we've seen named in a delta. The Vec is
        // dropped at the end of this function (or on early return /
        // panic) and each guard's Drop decrements the gate's
        // protection counter. A `HashSet` of already-protected names
        // keeps us from stacking guards if the model emits multiple
        // deltas for the same tool index.
        //
        // When `cancel_gate.is_none()` (test paths) the protection
        // call is a noop — we never construct the guard.
        let mut tool_guards: Vec<ProtectGuard> = Vec::new();
        let mut protected_tools: HashSet<String> = HashSet::new();

        let mut stream = response.bytes_stream();
        let mut buf = Vec::<u8>::new();
        let mut done = false;

        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result
                .map_err(|e| Error::Execution(format!("LLM stream read error: {}", e)))?;
            buf.extend_from_slice(&chunk);

            while let Some(newline_pos) = buf.iter().position(|&b| b == b'\n') {
                let line_bytes: Vec<u8> = buf.drain(..=newline_pos).collect();
                let line = match std::str::from_utf8(&line_bytes) {
                    Ok(s) => s.trim_end_matches(|c| c == '\n' || c == '\r'),
                    Err(_) => continue,
                };

                if !line.starts_with("data: ") {
                    continue;
                }
                let payload = &line[6..];
                if payload.trim() == "[DONE]" {
                    done = true;
                    break;
                }
                let json: Value = match serde_json::from_str(payload) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                for ev in self.profile.parse_sse_payload(&json) {
                    match ev {
                        ChatStreamEvent::ReasoningText(text) => {
                            if let Some(ref rc) = cfg.reasoning_channel {
                                if !rc.is_empty() {
                                    callback(RuntimeData::Text(tag_text_str(&text, rc)))?;
                                }
                            }
                        }
                        ChatStreamEvent::VisibleText(text) => {
                            full_text.push_str(&text);
                            token_count += 1;
                            callback(RuntimeData::Text(tag_text_str(&text, &cfg.output_channel)))?;
                        }
                        ChatStreamEvent::ToolCallDelta {
                            index,
                            id,
                            name,
                            arguments_chunk,
                        } => {
                            let entry = tool_calls.entry(index).or_default();
                            if let Some(id) = id {
                                entry.id = id;
                            }
                            if let Some(n) = name {
                                // First time we learn the tool's name
                                // for this delta-index. If the name
                                // resolves to a non-cancelable
                                // ToolSpec AND we have a real cancel
                                // gate from the runtime, take a
                                // ProtectGuard so subsequent barge_in
                                // envelopes are suppressed by the
                                // session router's filter task. The
                                // guard sticks around in `tool_guards`
                                // until this function returns.
                                if let Some(ref gate) = cancel_gate {
                                    if !protected_tools.contains(&n) {
                                        if let Some(spec) = cfg.tools.iter().find(|t| t.name == n) {
                                            if !spec.cancelable {
                                                tool_guards.push(gate.protect());
                                                protected_tools.insert(n.clone());
                                                tracing::info!(
                                                    tool = %n,
                                                    "[llm] barge suppressed for non-cancelable tool call"
                                                );
                                            }
                                        }
                                    }
                                }
                                entry.name = n;
                            }
                            if let Some(args) = arguments_chunk {
                                entry.arguments.push_str(&args);
                            }
                        }
                        ChatStreamEvent::Done => {
                            done = true;
                        }
                    }
                }
            }
            if done {
                break;
            }
        }

        // Dispatch tool calls + build history additions.
        let mut indices: Vec<u64> = tool_calls.keys().copied().collect();
        indices.sort_unstable();

        let received_tool_names: Vec<&str> = indices
            .iter()
            .filter_map(|i| tool_calls.get(i).map(|c| c.name.as_str()))
            .filter(|n| !n.is_empty())
            .collect();
        tracing::info!(
            provider = self.profile.name(),
            tool_calls_received = indices.len(),
            tool_names = ?received_tool_names,
            "[llm] tool calls received from model"
        );

        // Surface tool dispatch on the timeline tap so the UI can show
        // "🛠 perform_motion" instead of "BOT (no reply)" for tool-only
        // turns. One marker per tool name (perform_motion, say, etc).
        for name in &received_tool_names {
            crate::transport::session_control::publish_timeline_event(
                session_id,
                "tool_call",
                Some(name),
                "llm",
            );
        }

        let mut tool_calls_for_history: Vec<Value> = Vec::with_capacity(indices.len());
        let mut tool_result_entries: Vec<HistoryEntry> = Vec::with_capacity(indices.len());

        for idx in indices {
            let entry = match tool_calls.remove(&idx) {
                Some(e) => e,
                None => continue,
            };
            if entry.name.is_empty() {
                continue;
            }
            let call_id = if entry.id.is_empty() {
                format!("call_{}", idx)
            } else {
                entry.id.clone()
            };
            tool_calls_for_history.push(serde_json::json!({
                "id": call_id,
                "type": "function",
                "function": {
                    "name": entry.name,
                    "arguments": entry.arguments,
                },
            }));
            tool_result_entries.push(HistoryEntry::tool_result(&call_id, &entry.name, ""));
            dispatch_tool_call(&cfg.tools, &entry, &cfg.output_channel, callback)?;
        }

        let mut turn_entries: Vec<HistoryEntry> = Vec::new();
        if !tool_calls_for_history.is_empty() {
            turn_entries.push(HistoryEntry::assistant_with_tool_calls(
                if full_text.is_empty() {
                    None
                } else {
                    Some(&full_text)
                },
                Value::Array(tool_calls_for_history),
            ));
            turn_entries.extend(tool_result_entries);
        } else if !full_text.is_empty() {
            turn_entries.push(HistoryEntry::assistant_text(&full_text));
        }
        self.extend_history(session_id, turn_entries);

        // End-of-turn sentinel on the configured output channel.
        // ConversationCoordinatorNode flips to Idle (and emits the
        // `turn_state` envelope the UI watches) only when it sees
        // `<|text_end|>` arrive on its TTS-channel input. LlamaCpp
        // emits this directly; OpenAI-compatible providers don't, so
        // we synthesise it here. The coordinator drops a stale
        // sentinel that arrives outside AgentSpeaking, so emitting
        // unconditionally is safe even for tool-only or empty turns.
        callback(RuntimeData::Text(tag_text_str(
            "<|text_end|>",
            &cfg.output_channel,
        )))?;

        tracing::info!(
            provider = self.profile.name(),
            tokens = token_count,
            chars = full_text.len(),
            "[llm] streaming complete"
        );
        Ok(token_count)
    }

    async fn run_blocking<F>(
        &self,
        session_id: &str,
        cfg: &ChatBackendConfig,
        body: Value,
        callback: &mut F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let url = self.profile.endpoint(&cfg.base_url);
        let mut req = self.client.post(url).json(&body);
        req = self.profile.apply_auth(req, cfg.api_key.as_deref());
        let response = req
            .send()
            .await
            .map_err(|e| Error::Execution(format!("LLM HTTP request failed: {}", e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let err_body = response.text().await.unwrap_or_else(|_| "<no body>".into());
            return Err(Error::Execution(format!(
                "LLM API error {}: {}",
                status, err_body
            )));
        }

        let json: Value = response
            .json()
            .await
            .map_err(|e| Error::Execution(format!("LLM JSON parse error: {}", e)))?;

        let content = json["choices"][0]["message"]["content"]
            .as_str()
            .unwrap_or("")
            .to_string();
        if content.is_empty() {
            return Ok(0);
        }
        self.append_history(session_id, HistoryEntry::assistant_text(&content));
        callback(RuntimeData::Text(tag_text_str(
            &content,
            &cfg.output_channel,
        )))?;
        // See run_streaming for why we emit `<|text_end|>` ourselves.
        callback(RuntimeData::Text(tag_text_str(
            "<|text_end|>",
            &cfg.output_channel,
        )))?;
        tracing::info!(
            provider = self.profile.name(),
            chars = content.len(),
            "[llm] non-streaming response complete"
        );
        Ok(1)
    }
}

#[cfg(test)]
impl ChatBackend {
    /// Test-only access to history snapshot.
    pub(crate) fn history_snapshot(&self, session_id: &str) -> Vec<HistoryEntry> {
        self.history
            .lock()
            .get(session_id)
            .cloned()
            .unwrap_or_default()
    }
}
