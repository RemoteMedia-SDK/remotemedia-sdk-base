//! Gemini Generative Language API profile (stub).
//!
//! Sized so the [`crate::llm::ProviderProfile`] trait surface is
//! verified against three vendors (OpenAI, Anthropic, Gemini) — but
//! the bodies are `unimplemented!()` and the profile is **not** wired
//! into [`ProviderKind`] yet. Lands so the abstraction layer is
//! clearly multi-vendor instead of OpenAI-shaped-by-accident.
//!
//! TODO(profile): the request shape is `POST {base}/models/{model}:streamGenerateContent`,
//! body is `{contents: [{role, parts: [...]}]}`, content parts use
//! `text` / `inline_data` (mime_type + base64) / `function_call`
//! shapes, and SSE chunks carry `candidates[].content.parts[].text`
//! plus tool calls. `system_instruction` is a top-level field.

use crate::llm::provider::{ChatRequest, ChatStreamEvent, ProviderProfile};
use serde_json::Value;

#[derive(Debug, Default, Clone, Copy)]
pub struct GeminiProfile;

impl ProviderProfile for GeminiProfile {
    fn name(&self) -> &'static str {
        "gemini"
    }

    fn endpoint(&self, _base_url: &str) -> String {
        unimplemented!("GeminiProfile not wired up yet — see TODO in gemini_profile.rs")
    }

    fn apply_auth(
        &self,
        _req: reqwest::RequestBuilder,
        _api_key: Option<&str>,
    ) -> reqwest::RequestBuilder {
        unimplemented!("GeminiProfile not wired up yet")
    }

    fn shape_request(&self, _req: &ChatRequest<'_>) -> Value {
        unimplemented!("GeminiProfile not wired up yet")
    }

    fn parse_sse_payload(&self, _payload: &Value) -> Vec<ChatStreamEvent> {
        unimplemented!("GeminiProfile not wired up yet")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_is_gemini() {
        assert_eq!(GeminiProfile.name(), "gemini");
    }
}
