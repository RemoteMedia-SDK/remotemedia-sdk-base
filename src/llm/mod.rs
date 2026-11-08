//! LLM transport layer shared across chat-completion-shaped LLM nodes.
//!
//! Modality-agnostic: the backend transports a `messages` array plus
//! tools and emits streaming `RuntimeData::Text` outputs. The owning
//! node (e.g. [`crate::nodes::openai_chat::OpenAIChatNode`]) is
//! responsible for shaping each input modality into a chat-completions
//! `user` message before handing it off.
//!
//! Two layers:
//!
//! - [`ChatBackend`] — owns history, HTTP client, SSE parser, tool
//!   dispatch.
//! - [`ProviderProfile`] — vendor wire-shaping (endpoint URL, auth
//!   header, request body, SSE chunk parsing). v1 ships
//!   [`OpenAIProfile`]; Anthropic / Gemini land in step 5 of the
//!   multimodal-LLM plan.

pub mod anthropic_profile;
pub mod audio_encode;
pub mod chat_backend;
pub mod data_url;
pub mod gemini_profile;
pub mod history;
pub mod managed_server;
pub mod provider;
pub mod tool_dispatch;
pub mod traits;

pub use anthropic_profile::AnthropicProfile;
pub use chat_backend::{
    ChatBackend, ChatBackendConfig, ChatFailureKind, ChatTurnContext, LlmFallbackConfig,
    HISTORY_PROVIDER_KEY, LLM_FALLBACK_EVENT,
};
pub use gemini_profile::GeminiProfile;
pub use history::HistoryEntry;
pub use provider::{ChatRequest, ChatStreamEvent, OpenAIProfile, ProviderProfile};
pub use tool_dispatch::{dispatch_tool_call, ToolCallAccum};
pub use traits::{InterruptableBackend, StatefulConversationBackend, VoiceActivityDetectorBackend};

use std::sync::Arc;

/// Vendor selection for chat-completion-shaped LLM nodes.
///
/// Picks the [`ProviderProfile`] the [`ChatBackend`] uses. Default
/// is [`ProviderKind::OpenAI`] (cloud OpenAI, Azure, vLLM, modern
/// llama.cpp, Ollama). [`ProviderKind::Anthropic`] targets the
/// Messages API. Gemini exists as a stub but isn't selectable yet.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    /// Wire name `openai`; `open_a_i` (the old derived spelling) is
    /// still accepted.
    #[serde(rename = "openai", alias = "open_a_i")]
    OpenAI,
    Anthropic,
}

impl Default for ProviderKind {
    fn default() -> Self {
        ProviderKind::OpenAI
    }
}

impl ProviderKind {
    /// Materialise the profile trait object for this kind.
    pub fn into_profile(self) -> Arc<dyn ProviderProfile> {
        match self {
            ProviderKind::OpenAI => Arc::new(OpenAIProfile),
            ProviderKind::Anthropic => Arc::new(AnthropicProfile),
        }
    }

    /// Default base URL for this vendor's public API.
    pub fn default_base_url(self) -> &'static str {
        match self {
            ProviderKind::OpenAI => "https://api.openai.com/v1",
            ProviderKind::Anthropic => "https://api.anthropic.com/v1",
        }
    }
}

#[cfg(test)]
mod provider_kind_tests {
    use super::ProviderKind;

    #[test]
    fn provider_kind_accepts_openai_spelling() {
        for name in ["openai", "open_a_i"] {
            let kind: ProviderKind = serde_json::from_value(serde_json::json!(name)).unwrap();
            assert_eq!(kind, ProviderKind::OpenAI);
        }
        assert_eq!(
            serde_json::to_value(ProviderKind::OpenAI).unwrap(),
            serde_json::json!("openai")
        );
        let kind: ProviderKind = serde_json::from_value(serde_json::json!("anthropic")).unwrap();
        assert_eq!(kind, ProviderKind::Anthropic);
    }
}
