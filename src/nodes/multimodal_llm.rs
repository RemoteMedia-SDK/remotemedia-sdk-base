//! Multimodal LLM Node
//!
//! Streams chat-completion requests against an OpenAI-shape API and
//! accepts `Text`, `Image`, and `Json` inputs which are normalised
//! into a `content`-parts array on a single `user` message:
//!
//! ```json
//! {"role":"user","content":[
//!   {"type":"text","text":"What's in this image?"},
//!   {"type":"image_url","image_url":{"url":"data:image/png;base64,..."}}
//! ]}
//! ```
//!
//! Two aggregation modes:
//!
//! - `PerInput` (default) — every input frame becomes its own one-part
//!   user turn. Good for "describe this single image" pipelines.
//! - `CoalesceUntil { sentinel }` — accumulates parts in a per-session
//!   buffer; flushes as one user turn when a `Text` input matching the
//!   sentinel arrives. Required for "N images + a question" or
//!   "audio utterance + question" pipelines.
//!
//! Transport (HTTP, SSE, tool dispatch, history) is shared with
//! [`crate::nodes::openai_chat::OpenAIChatNode`] via
//! [`crate::llm::ChatBackend`]. Tool calls (`say`/`show` and
//! user-defined side-effect tools) work identically.

use crate::data::{RuntimeData, TEXT_CHANNEL_DEFAULT};
use crate::error::Error;
use crate::llm::audio_encode::audio_to_wav_base64;
use crate::llm::{data_url::image_to_data_url, ChatBackend, ChatBackendConfig, ProviderKind};
use crate::nodes::AsyncStreamingNode;
use parking_lot::Mutex;
use remotemedia_traits::runtime_context::{InitializeContextRead, NodeRuntimeContextRead};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// How input frames are grouped into chat-completion `user` messages.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AggregationMode {
    /// Each input frame is its own user turn (one chat-completion
    /// request per frame). Default.
    PerInput,
    /// Accumulate parts in a per-session buffer; flush when one of
    /// these triggers fires:
    ///
    /// 1. A `Text` input matching `sentinel` arrives.
    /// 2. The buffer reaches `max_parts` (overflow flush).
    ///
    /// `sentinel` defaults to `"<|input_end|>"`, matching the
    /// conversation coordinator's end-of-turn marker. `max_parts`
    /// caps memory growth on stuck pipelines (32 by default).
    CoalesceUntil {
        #[serde(default = "default_sentinel")]
        sentinel: String,
        /// Maximum content-parts buffered per session before forcing
        /// a flush. Prevents runaway memory if the sentinel never
        /// arrives. Defaults to 32 — well above realistic per-turn
        /// frame counts but well below "OOM the process".
        #[serde(default = "default_max_parts")]
        max_parts: usize,
    },
}

fn default_sentinel() -> String {
    "<|input_end|>".to_string()
}

fn default_max_parts() -> usize {
    32
}

impl Default for AggregationMode {
    fn default() -> Self {
        AggregationMode::PerInput
    }
}

/// Output mode for the MultimodalLLMNode.
///
/// Controls what data the node emits alongside the primary text output.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum LlmOutputMode {
    /// Default: emit only `RuntimeData::Text` with the LLM response.
    #[default]
    Text,
    /// Emit text + `RuntimeData::Json` metadata (token counts, usage,
    /// model info, timestamps). Enables downstream analysis without
    /// changing the primary text stream.
    TextWithMetadata,
    /// Emit text + metadata JSON + `RuntimeData::Tensor` with a
    /// text embedding of the LLM response. Requires the LLM endpoint
    /// to support the `/embeddings` API (OpenAI, vLLM, local models
    /// with embedding support). The embedding model defaults to the
    /// chat model; override with `embedding_model` in config.
    ///
    /// The tensor output has shape `[embedding_dim]` with dtype
    /// float32 (dtype=0). Downstream nodes can use this for:
    /// - Emotion vector analysis (cosine similarity against
    ///   pre-extracted emotion direction vectors)
    /// - Semantic clustering / topic detection
    /// - Retrieval-augmented generation (RAG) indexing
    TextWithEmbedding,
}

/// Configuration for [`MultimodalLLMNode`]. Mirrors
/// [`crate::nodes::openai_chat::OpenAIChatConfig`] field-for-field
/// where they overlap; new fields are documented inline.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct MultimodalLLMConfig {
    #[serde(alias = "apiKey")]
    pub api_key: Option<String>,
    #[serde(alias = "baseUrl")]
    pub base_url: Option<String>,
    #[serde(alias = "model")]
    pub model: Option<String>,
    #[serde(alias = "systemPrompt")]
    pub system_prompt: Option<String>,
    #[serde(alias = "outputChannel")]
    pub output_channel: Option<String>,
    #[serde(alias = "reasoningChannel")]
    pub reasoning_channel: Option<String>,
    #[serde(alias = "maxTokens")]
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
    #[serde(alias = "topP")]
    pub top_p: Option<f32>,
    #[serde(alias = "historyTurns")]
    pub history_turns: usize,
    pub streaming: bool,

    // ── Tool calling (same shape as OpenAIChatConfig) ───────────────
    #[serde(alias = "enableSayTool")]
    pub enable_say_tool: bool,
    #[serde(alias = "enableShowTool")]
    pub enable_show_tool: bool,
    #[serde(default, alias = "tools")]
    pub tools: Vec<crate::nodes::tool_spec::ToolSpec>,
    #[serde(default, alias = "activeTools")]
    pub active_tools: Option<Vec<String>>,
    #[serde(default, alias = "toolChoice")]
    pub tool_choice: Option<Value>,

    // ── Multimodal-only ─────────────────────────────────────────────
    /// Aggregation policy. See [`AggregationMode`].
    #[serde(alias = "aggregation")]
    pub aggregation: AggregationMode,

    /// LLM vendor profile. Default `OpenAI`. Set to `anthropic` to
    /// target the Messages API; the node will reshape image and tool
    /// content automatically.
    #[serde(default, alias = "provider")]
    pub provider: ProviderKind,

    // ── Output mode (vector / embedding support) ────────────────────
    /// Output mode. Default `text` (text only). Set to
    /// `text_with_metadata` to also emit JSON metadata (token counts,
    /// model info). Set to `text_with_embedding` to also emit a
    /// `RuntimeData::Tensor` with the response embedding for
    /// downstream emotion-vector analysis or semantic clustering.
    #[serde(default, alias = "outputMode")]
    pub output_mode: LlmOutputMode,

    /// Embedding model for `text_with_embedding` output mode.
    /// Defaults to the chat model if the endpoint supports it.
    /// Set to a dedicated embedding model (e.g. `text-embedding-3-small`)
    /// for better semantic representations.
    #[serde(default, alias = "embeddingModel")]
    pub embedding_model: Option<String>,

    /// Enable logprobs in the chat completion request. When true,
    /// the node emits token-level log probabilities as JSON metadata,
    /// enabling confidence analysis and token-level steering.
    #[serde(default, alias = "logprobs")]
    pub logprobs: bool,

    /// Number of top logprobs to return per token (requires `logprobs=true`).
    /// Default 0 (only the chosen token's logprob).
    #[serde(default, alias = "topLogprobs")]
    pub top_logprobs: u8,
}

impl Default for MultimodalLLMConfig {
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
            aggregation: AggregationMode::default(),
            provider: ProviderKind::default(),
            output_mode: LlmOutputMode::Text,
            embedding_model: None,
            logprobs: false,
            top_logprobs: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

/// Multimodal chat-completion node.
pub struct MultimodalLLMNode {
    config: MultimodalLLMConfig,
    backend: Arc<ChatBackend>,
    /// Per-session content-parts buffer used by the `CoalesceUntil`
    /// aggregation mode. `PerInput` mode never reads or writes this.
    coalesce_buffers: Arc<Mutex<HashMap<String, Vec<Value>>>>,
    /// HTTP client for embedding requests (lazy-initialized).
    embedding_client: std::sync::OnceLock<Arc<reqwest::Client>>,
}

impl MultimodalLLMNode {
    pub fn with_config(config: MultimodalLLMConfig) -> Self {
        let profile = config.provider.into_profile();
        Self {
            config,
            backend: Arc::new(ChatBackend::new(profile)),
            coalesce_buffers: Arc::new(Mutex::new(HashMap::new())),
            embedding_client: std::sync::OnceLock::new(),
        }
    }

    /// Get the embedding HTTP client (lazy-initialized).
    fn get_embedding_client(&self) -> &Arc<reqwest::Client> {
        self.embedding_client.get_or_init(|| {
            Arc::new(
                reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(60))
                    .build()
                    .expect("build embedding client"),
            )
        })
    }

    /// Fetch a text embedding from the LLM endpoint.
    ///
    /// Returns `RuntimeData::Tensor` with shape `[embedding_dim]` and
    /// dtype float32, or `None` if the endpoint doesn't support embeddings.
    async fn fetch_embedding(&self, text: &str) -> Option<RuntimeData> {
        let api_key = self.resolve_api_key()?;
        let base_url = self.resolve_base_url();
        let model = self
            .config
            .embedding_model
            .clone()
            .unwrap_or_else(|| self.resolve_model());

        let url = format!("{}/embeddings", base_url.trim_end_matches('/'));

        let body = serde_json::json!({
            "model": model,
            "input": text,
        });

        let client = self.get_embedding_client();
        let response = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await;

        let response = match response {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(
                    node = "MultimodalLLMNode",
                    "embedding request failed: {}",
                    e
                );
                return None;
            }
        };

        if !response.status().is_success() {
            tracing::debug!(
                node = "MultimodalLLMNode",
                "embedding API returned {}",
                response.status()
            );
            return None;
        }

        let json: Value = match response.json().await {
            Ok(j) => j,
            Err(e) => {
                tracing::debug!(node = "MultimodalLLMNode", "embedding JSON parse: {}", e);
                return None;
            }
        };

        // Extract embedding vector from response
        let embedding = json["data"][0]["embedding"]
            .as_array()
            .or_else(|| json["data"][0]["embedding"][0].as_array()); // some APIs nest an extra dimension

        let embedding = embedding?;

        // Convert to f32 bytes
        let data: Vec<u8> = embedding
            .iter()
            .filter_map(|v| v.as_f64())
            .flat_map(|v| (v as f32).to_le_bytes())
            .collect();

        Some(RuntimeData::Tensor {
            data,
            shape: vec![embedding.len() as i32],
            dtype: 0, // float32
            metadata: None,
        })
    }

    /// Build metadata JSON for the output.
    fn build_metadata(&self, token_count: usize, full_text: &str) -> RuntimeData {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);

        RuntimeData::Json(serde_json::json!({
            "node": "MultimodalLLMNode",
            "model": self.resolve_model(),
            "output_mode": match self.config.output_mode {
                LlmOutputMode::Text => "text",
                LlmOutputMode::TextWithMetadata => "text_with_metadata",
                LlmOutputMode::TextWithEmbedding => "text_with_embedding",
            },
            "token_count": token_count,
            "char_count": full_text.len(),
            "ts_ms": ts,
        }))
    }

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

    /// Convert one input frame into either a content-part or a sentinel
    /// signal (for `CoalesceUntil` mode). `None` means "drop this
    /// frame silently".
    fn input_to_part(&self, data: &RuntimeData) -> Result<Option<InputPart>, Error> {
        match data {
            RuntimeData::Text(t) => {
                let stripped = crate::data::split_text_str(t).1;
                if stripped.is_empty() {
                    return Ok(None);
                }
                if let AggregationMode::CoalesceUntil { sentinel, .. } = &self.config.aggregation {
                    if stripped == *sentinel || t == sentinel {
                        return Ok(Some(InputPart::Sentinel));
                    }
                }
                Ok(Some(InputPart::Part(serde_json::json!({
                    "type": "text",
                    "text": stripped,
                }))))
            }
            RuntimeData::Json(j) => {
                match j.get("content").or(j.get("text")).and_then(|v| v.as_str()) {
                    Some(s) if !s.is_empty() => Ok(Some(InputPart::Part(serde_json::json!({
                        "type": "text",
                        "text": s,
                    })))),
                    _ => {
                        tracing::debug!(
                            node = "MultimodalLLMNode",
                            "Dropping JSON input with no `content`/`text` field"
                        );
                        Ok(None)
                    }
                }
            }
            RuntimeData::Image { .. } => {
                let url = image_to_data_url(data)?;
                Ok(Some(InputPart::Part(serde_json::json!({
                    "type": "image_url",
                    "image_url": {"url": url},
                }))))
            }
            RuntimeData::Audio { .. } => {
                let b64 = audio_to_wav_base64(data)?;
                Ok(Some(InputPart::Part(serde_json::json!({
                    "type": "input_audio",
                    "input_audio": {"data": b64, "format": "wav"},
                }))))
            }
            other => Err(Error::Execution(format!(
                "MultimodalLLMNode does not accept {} input",
                other.data_type()
            ))),
        }
    }

    /// Build a `{role:"user", content:[...]}` message from one or more
    /// content-parts. OpenAI accepts a string `content` for text-only
    /// turns; we use the array form unconditionally for symmetry, and
    /// because gpt-4o-mini accepts both equally.
    fn user_message(parts: Vec<Value>) -> Value {
        serde_json::json!({"role": "user", "content": parts})
    }

    /// Push a part to the per-session buffer and return `Some(drained)`
    /// when the buffer has reached `max_parts` (caller flushes), or
    /// `None` to keep buffering. Synchronous — no HTTP, easy to test
    /// in isolation from the async path.
    fn push_and_check_overflow(&self, sid: &str, p: Value, max_parts: usize) -> Option<Vec<Value>> {
        let mut bufs = self.coalesce_buffers.lock();
        let buf = bufs.entry(sid.to_string()).or_insert_with(Vec::new);
        buf.push(p);
        if buf.len() >= max_parts {
            Some(std::mem::take(buf))
        } else {
            None
        }
    }
}

#[derive(Debug)]
enum InputPart {
    Part(Value),
    Sentinel,
}

#[async_trait::async_trait]
impl AsyncStreamingNode for MultimodalLLMNode {
    fn node_type(&self) -> &str {
        "MultimodalLLMNode"
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
            &format!("MultimodalLLMNode: model={}, endpoint={}", model, base_url),
        );
        tracing::info!(
            node = "MultimodalLLMNode",
            model = %model,
            base_url = %base_url,
            api_key = %masked_key,
            streaming = self.config.streaming,
            history_turns = self.config.history_turns,
            "Initializing MultimodalLLMNode"
        );
        ctx.emit_progress(
            "ready",
            &format!("MultimodalLLMNode ready (model={})", model),
        );
        Ok(())
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        // Single-shot: flatten the streaming output into the last frame.
        let mut outputs: Vec<RuntimeData> = Vec::new();
        let session_id = "default".to_string();
        let mut cb = |out: RuntimeData| -> Result<(), Error> {
            outputs.push(out);
            Ok(())
        };
        self.process_streaming(data, Some(session_id), &mut cb)
            .await?;
        outputs
            .into_iter()
            .last()
            .ok_or_else(|| Error::Execution("MultimodalLLMNode: no output generated".into()))
    }

    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        session_id: Option<String>,
        callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        // Legacy entry point — no ctx, so no cancel gate. The
        // streaming path inside `dispatch_streaming` therefore can't
        // suppress barge for non-cancelable tools. Production
        // dispatch goes through `process_streaming_with_ctx` below
        // which feeds the real gate.
        self.dispatch_streaming(data, session_id, None, callback)
            .await
    }

    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let sid = if ctx.session_id().is_empty() {
            None
        } else {
            Some(ctx.session_id().to_string())
        };
        // Forward the per-call cancel gate so streamed
        // non-cancelable tool calls can suppress barge_in for the
        // remainder of the call. Symmetric with
        // `OpenAIChatNode::process_streaming_with_ctx`. Reaches through
        // the read-trait `as_any` escape hatch — plugins can't do this
        // and don't need to.
        let cancel_gate = ctx
            .as_any()
            .downcast_ref::<crate::nodes::NodeRuntimeContext>()
            .map(|c| Arc::clone(&c.cancel_gate));
        self.dispatch_streaming(data, sid, cancel_gate, callback)
            .await
    }
}

impl MultimodalLLMNode {
    async fn dispatch_streaming<F>(
        &self,
        data: RuntimeData,
        session_id: Option<String>,
        cancel_gate: Option<Arc<crate::transport::CancelGate>>,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let sid = session_id.unwrap_or_else(|| "default".to_string());
        let needs_metadata = !matches!(self.config.output_mode, LlmOutputMode::Text);
        let needs_embedding = matches!(self.config.output_mode, LlmOutputMode::TextWithEmbedding);

        let part = match self.input_to_part(&data)? {
            Some(p) => p,
            None => return Ok(0),
        };

        let cfg = self.backend_config();

        // Wrap the callback to capture full text for metadata/embeddings
        let mut full_text = String::new();
        let mut token_count = 0usize;
        let mut wrapped_cb = |out: RuntimeData| -> Result<(), Error> {
            if needs_metadata || needs_embedding {
                if let RuntimeData::Text(ref t) = out {
                    full_text.push_str(t);
                    token_count += 1;
                }
            }
            callback(out)
        };

        let result = match (&self.config.aggregation, part) {
            (AggregationMode::PerInput, InputPart::Part(p)) => {
                let user_msg = Self::user_message(vec![p]);
                self.backend
                    .run(&sid, user_msg, &cfg, cancel_gate.clone(), &mut wrapped_cb)
                    .await
            }
            (AggregationMode::PerInput, InputPart::Sentinel) => {
                // PerInput mode has no buffer to flush — sentinels are
                // a no-op; treat them as a plain text message instead
                // would silently corrupt history if the user wired
                // CoalesceUntil and then switched modes. Drop with a
                // debug log.
                tracing::debug!(
                    node = "MultimodalLLMNode",
                    "received sentinel in PerInput mode — dropping (no buffer to flush)"
                );
                return Ok(0);
            }
            (AggregationMode::CoalesceUntil { max_parts, .. }, InputPart::Part(p)) => {
                // Push, then check overflow. Overflow flushes the
                // buffer as if a sentinel had arrived — same shape, no
                // dangling parts.
                let overflow_parts = self.push_and_check_overflow(&sid, p, *max_parts);
                if let Some(parts) = overflow_parts {
                    tracing::warn!(
                        node = "MultimodalLLMNode",
                        session = %sid,
                        max_parts = *max_parts,
                        "coalesce buffer reached max_parts; force-flushing"
                    );
                    let user_msg = Self::user_message(parts);
                    self.backend
                        .run(&sid, user_msg, &cfg, cancel_gate.clone(), &mut wrapped_cb)
                        .await
                } else {
                    return Ok(0);
                }
            }
            (AggregationMode::CoalesceUntil { .. }, InputPart::Sentinel) => {
                let parts = self
                    .coalesce_buffers
                    .lock()
                    .remove(&sid)
                    .unwrap_or_default();
                if parts.is_empty() {
                    tracing::debug!(
                        node = "MultimodalLLMNode",
                        "sentinel arrived with empty buffer; nothing to flush"
                    );
                    return Ok(0);
                }
                let user_msg = Self::user_message(parts);
                self.backend
                    .run(&sid, user_msg, &cfg, cancel_gate, &mut wrapped_cb)
                    .await
            }
        };

        // After the LLM response is complete, emit metadata and/or embedding
        if needs_metadata && !full_text.is_empty() {
            let metadata = self.build_metadata(token_count, &full_text);
            callback(metadata)?;
        }

        if needs_embedding && !full_text.is_empty() {
            if let Some(embedding) = self.fetch_embedding(&full_text).await {
                callback(embedding)?;
            }
        }

        result
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

pub struct MultimodalLLMNodeFactory;

impl crate::nodes::StreamingNodeFactory for MultimodalLLMNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn crate::nodes::StreamingNode>, Error> {
        let config: MultimodalLLMConfig =
            serde_json::from_value(params.clone()).unwrap_or_default();
        let node = MultimodalLLMNode::with_config(config);
        Ok(Box::new(crate::nodes::AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        "MultimodalLLMNode"
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{
            LatencyClass, NodeCapabilitiesSchema, NodeSchema, RuntimeDataType,
        };
        Some(
            NodeSchema::new("MultimodalLLMNode")
                .description(
                    "Streaming chat completion node accepting text, image, and audio input. \
                     Aggregates inputs into one user message with content-parts \
                     (text + image_url + input_audio). Targets OpenAI-shape APIs \
                     (cloud OpenAI, vLLM, modern llama.cpp + llava). \
                     Supports vector output modes for emotion analysis and \
                     semantic clustering via the `output_mode` config.",
                )
                .category("llm")
                .accepts([
                    RuntimeDataType::Text,
                    RuntimeDataType::Image,
                    RuntimeDataType::Audio,
                    RuntimeDataType::Json,
                ])
                .produces([
                    RuntimeDataType::Text,
                    RuntimeDataType::Json,
                    RuntimeDataType::Tensor,
                ])
                .capabilities(NodeCapabilitiesSchema {
                    parallelizable: false,
                    batch_aware: false,
                    supports_control: false,
                    latency_class: LatencyClass::Slow,
                })
                .config_schema_from::<MultimodalLLMConfig>(),
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::ImageFormat;
    use crate::nodes::{schema::RuntimeDataType, StreamingNodeFactory};

    fn img() -> RuntimeData {
        RuntimeData::Image {
            data: vec![1, 2, 3, 4],
            format: ImageFormat::Png,
            width: 1,
            height: 1,
            timestamp_us: None,
            stream_id: None,
            metadata: None,
        }
    }

    #[test]
    fn config_aggregation_default_is_per_input() {
        let cfg = MultimodalLLMConfig::default();
        assert!(matches!(cfg.aggregation, AggregationMode::PerInput));
    }

    #[test]
    fn config_camel_case_aggregation() {
        let params = serde_json::json!({
            "aggregation": {"kind": "coalesce_until", "sentinel": "<|done|>"}
        });
        let cfg: MultimodalLLMConfig = serde_json::from_value(params).unwrap();
        match cfg.aggregation {
            AggregationMode::CoalesceUntil {
                sentinel,
                max_parts,
            } => {
                assert_eq!(sentinel, "<|done|>");
                // omitted in JSON → default
                assert_eq!(max_parts, default_max_parts());
            }
            _ => panic!("expected CoalesceUntil"),
        }
    }

    #[test]
    fn coalesce_max_parts_force_flushes() {
        let cfg = MultimodalLLMConfig {
            aggregation: AggregationMode::CoalesceUntil {
                sentinel: "<|input_end|>".to_string(),
                max_parts: 3,
            },
            ..Default::default()
        };
        let node = MultimodalLLMNode::with_config(cfg);
        let part = || serde_json::json!({"type":"text","text":"x"});

        // First two parts buffer.
        assert!(node.push_and_check_overflow("s", part(), 3).is_none());
        assert_eq!(node.coalesce_buffers.lock().get("s").unwrap().len(), 1);
        assert!(node.push_and_check_overflow("s", part(), 3).is_none());
        assert_eq!(node.coalesce_buffers.lock().get("s").unwrap().len(), 2);

        // Third part triggers overflow flush — drained returned, buffer empty.
        let drained = node.push_and_check_overflow("s", part(), 3).unwrap();
        assert_eq!(drained.len(), 3);
        assert_eq!(
            node.coalesce_buffers.lock().get("s").unwrap().len(),
            0,
            "buffer should be drained after overflow flush"
        );
    }

    #[test]
    fn coalesce_per_session_buffer_isolation() {
        let cfg = MultimodalLLMConfig {
            aggregation: AggregationMode::CoalesceUntil {
                sentinel: "<|input_end|>".to_string(),
                max_parts: 32,
            },
            ..Default::default()
        };
        let node = MultimodalLLMNode::with_config(cfg);
        node.coalesce_buffers.lock().insert(
            "a".into(),
            vec![serde_json::json!({"type":"text","text":"A"})],
        );
        node.coalesce_buffers.lock().insert(
            "b".into(),
            vec![serde_json::json!({"type":"text","text":"B"})],
        );
        // Flushing session "a" must not touch session "b".
        let drained_a = node.coalesce_buffers.lock().remove("a").unwrap_or_default();
        assert_eq!(drained_a.len(), 1);
        assert_eq!(drained_a[0]["text"], "A");
        assert_eq!(
            node.coalesce_buffers
                .lock()
                .get("b")
                .map(|v| v.len())
                .unwrap_or(0),
            1
        );
    }

    #[test]
    fn text_input_builds_text_part() {
        let node = MultimodalLLMNode::with_config(MultimodalLLMConfig::default());
        let part = node
            .input_to_part(&RuntimeData::Text("hello".into()))
            .unwrap();
        match part {
            Some(InputPart::Part(v)) => {
                assert_eq!(v["type"], "text");
                assert_eq!(v["text"], "hello");
            }
            other => panic!("expected text Part, got {:?}", other),
        }
    }

    #[test]
    fn image_input_builds_image_url_part_with_data_url() {
        let node = MultimodalLLMNode::with_config(MultimodalLLMConfig::default());
        let part = node.input_to_part(&img()).unwrap();
        match part {
            Some(InputPart::Part(v)) => {
                assert_eq!(v["type"], "image_url");
                let url = v["image_url"]["url"].as_str().unwrap();
                assert!(url.starts_with("data:image/png;base64,"), "url={}", url);
            }
            other => panic!("expected image_url Part, got {:?}", other),
        }
    }

    #[test]
    fn empty_text_dropped_silently() {
        let node = MultimodalLLMNode::with_config(MultimodalLLMConfig::default());
        let part = node.input_to_part(&RuntimeData::Text("".into())).unwrap();
        assert!(part.is_none());
    }

    #[test]
    fn audio_input_emits_input_audio_part() {
        let node = MultimodalLLMNode::with_config(MultimodalLLMConfig::default());
        let audio = RuntimeData::Audio {
            samples: vec![0.0_f32; 16].into(),
            sample_rate: 16000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        };
        let part = node.input_to_part(&audio).unwrap();
        match part {
            Some(InputPart::Part(v)) => {
                assert_eq!(v["type"], "input_audio");
                assert_eq!(v["input_audio"]["format"], "wav");
                let data = v["input_audio"]["data"].as_str().unwrap();
                use base64::engine::general_purpose::STANDARD as B64;
                use base64::Engine;
                let buf = B64.decode(data).unwrap();
                assert_eq!(&buf[..4], b"RIFF");
                assert_eq!(&buf[8..12], b"WAVE");
            }
            other => panic!("expected input_audio Part, got {:?}", other),
        }
    }

    #[test]
    fn unsupported_input_still_rejected() {
        let node = MultimodalLLMNode::with_config(MultimodalLLMConfig::default());
        let bin = RuntimeData::Binary(vec![0u8; 4]);
        let err = node.input_to_part(&bin).unwrap_err();
        assert!(format!("{}", err).contains("does not accept"));
    }

    #[test]
    fn coalesce_buffers_until_sentinel() {
        let cfg = MultimodalLLMConfig {
            aggregation: AggregationMode::CoalesceUntil {
                sentinel: "<|input_end|>".to_string(),
                max_parts: default_max_parts(),
            },
            ..Default::default()
        };
        let node = MultimodalLLMNode::with_config(cfg);
        // First image goes to buffer.
        let p = node.input_to_part(&img()).unwrap().unwrap();
        if let InputPart::Part(v) = p {
            node.coalesce_buffers
                .lock()
                .entry("s".into())
                .or_insert_with(Vec::new)
                .push(v);
        }
        // Sentinel text is recognised.
        let s = node
            .input_to_part(&RuntimeData::Text("<|input_end|>".into()))
            .unwrap();
        assert!(matches!(s, Some(InputPart::Sentinel)));
        // Buffer is still populated; flush would empty it (covered by
        // the integration test below).
        assert_eq!(node.coalesce_buffers.lock().get("s").unwrap().len(), 1);
    }

    #[test]
    fn factory_schema_advertises_all_accepted_modalities() {
        let factory = MultimodalLLMNodeFactory;
        let schema = factory.schema().unwrap();
        assert!(schema.accepts.contains(&RuntimeDataType::Image));
        assert!(schema.accepts.contains(&RuntimeDataType::Audio));
        assert!(schema.accepts.contains(&RuntimeDataType::Text));
        assert!(schema.accepts.contains(&RuntimeDataType::Json));
        assert!(schema.produces.contains(&RuntimeDataType::Text));
        assert_eq!(schema.category, Some("llm".into()));
    }

    #[test]
    fn factory_creates_node() {
        let factory = MultimodalLLMNodeFactory;
        let params = serde_json::json!({"model": "gpt-4o-mini"});
        let node = factory.create("n1".into(), &params, None).unwrap();
        assert_eq!(node.node_type(), "MultimodalLLMNode");
    }

    /// End-to-end: a sentinel arriving with a non-empty buffer pulls
    /// the buffer through the backend's request builder. We can't
    /// actually issue HTTP in unit tests, but we can assert the
    /// per-session buffer is consumed by simulating the flush path —
    /// the actual `backend.run()` call is exercised by integration
    /// tests against a local llama.cpp.
    #[test]
    fn coalesce_flush_drains_buffer_on_sentinel() {
        let cfg = MultimodalLLMConfig {
            aggregation: AggregationMode::CoalesceUntil {
                sentinel: "<|input_end|>".to_string(),
                max_parts: default_max_parts(),
            },
            ..Default::default()
        };
        let node = MultimodalLLMNode::with_config(cfg);
        node.coalesce_buffers.lock().insert(
            "s".into(),
            vec![serde_json::json!({"type":"text","text":"x"})],
        );
        // Direct manipulation of the buffer that mirrors what
        // process_streaming does at the sentinel arm.
        let drained = node.coalesce_buffers.lock().remove("s").unwrap_or_default();
        assert_eq!(drained.len(), 1);
        let user_msg = MultimodalLLMNode::user_message(drained);
        assert_eq!(user_msg["role"], "user");
        assert_eq!(user_msg["content"][0]["type"], "text");
        assert!(node.coalesce_buffers.lock().get("s").is_none());
    }
}
