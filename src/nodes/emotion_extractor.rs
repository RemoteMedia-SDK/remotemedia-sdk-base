//! `EmotionExtractorNode` — multi-output text→(text, json) tag extractor.
//!
//! Source-agnostic streaming node that lifts `[EMOTION:<emoji>]` tags out of
//! a text stream. Per spec [`docs/superpowers/specs/2026-04-27-live2d-audio2face-rvc-avatar-design.md`]
//! §3.1: emits one `RuntimeData::Text` per input (tags removed) plus one
//! `RuntimeData::Json` per matched tag (in source order).
//!
//! Mirrors the multi-output streaming pattern that `silero_vad` already uses
//! — the data-path edge owns the dispatch (TTS sees only the Text edge,
//! the renderer sees only the Json edge).
//!
//! ## Why a hand-rolled trait impl, not `#[node(...)]`
//!
//! The `#[node]` derive macro generates a fixed `new(config)` constructor
//! that doesn't compile the regex; we'd have to validate the pattern at
//! every `process_streaming` call instead of once at construction. Going
//! manual lets us return `Err(regex::Error)` from a fallible
//! `with_pattern` constructor — matching the spec's "construction with
//! malformed regex returns an error" requirement.
//!
//! ## `turn_id` forwarding (deferred)
//!
//! Spec §3.1 says `turn_id` is forwarded "if the upstream frame carries it
//! under the conventional `metadata.turn_id` key." `RuntimeData::Text` is a
//! tuple variant `Text(String)` with no metadata field, so there is no
//! place to read `turn_id` from when the input is plain Text. Forwarding
//! is deferred until either:
//!   1. upstream emits a Json envelope carrying both text and turn_id, or
//!   2. `RuntimeData::Text` gains a metadata field.
//! Tracked in the avatar plan; not a blocker for the M0 scope.

use crate::data::text_channel::{split_text_str, tag_text_str};
use crate::data::RuntimeData;
use crate::error::{Error, Result};
use crate::nodes::AsyncStreamingNode;
use async_trait::async_trait;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Default pattern — captures the inner emoji/alias of `[EMOTION:<x>]`.
pub const DEFAULT_EMOTION_PATTERN: &str = r"\[EMOTION:([^\]]+)\]";

/// Configuration for `EmotionExtractorNode`.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct EmotionExtractorConfig {
    /// Regex with one capture group; the captured text is the
    /// alias/emoji that gets emitted on the Json edge after alias
    /// substitution.
    pub pattern: String,

    /// Optional alias → canonical-emoji map. Applied before emit, so
    /// the Json carries the canonical emoji while preserving the
    /// alias for diagnostics.
    pub aliases: HashMap<String, String>,
}

impl Default for EmotionExtractorConfig {
    fn default() -> Self {
        Self {
            pattern: DEFAULT_EMOTION_PATTERN.to_string(),
            aliases: HashMap::new(),
        }
    }
}

/// Streaming node that strips `[EMOTION:…]` tags out of text and emits
/// structured emotion events.
pub struct EmotionExtractorNode {
    pattern: Regex,
    aliases: HashMap<String, String>,
}

impl EmotionExtractorNode {
    /// Build from config; fails if the pattern is malformed.
    pub fn new(config: EmotionExtractorConfig) -> std::result::Result<Self, regex::Error> {
        let pattern = Regex::new(&config.pattern)?;
        Ok(Self {
            pattern,
            aliases: config.aliases,
        })
    }

    /// Build with the default `[EMOTION:<x>]` pattern. The default
    /// pattern is a compile-time constant and is known to be valid,
    /// so this is infallible.
    pub fn with_default_pattern() -> Self {
        Self::new(EmotionExtractorConfig::default())
            .expect("default pattern is a compile-time constant and must always compile")
    }

    /// Build with a custom pattern. One capture group required.
    pub fn with_pattern(pattern: &str) -> std::result::Result<Self, regex::Error> {
        Self::new(EmotionExtractorConfig {
            pattern: pattern.to_string(),
            aliases: HashMap::new(),
        })
    }

    /// Builder: replace the alias map.
    pub fn with_aliases(mut self, aliases: HashMap<String, String>) -> Self {
        self.aliases = aliases;
        self
    }
}

#[async_trait]
impl AsyncStreamingNode for EmotionExtractorNode {
    fn node_type(&self) -> &str {
        "EmotionExtractorNode"
    }

    async fn process(&self, _data: RuntimeData) -> Result<RuntimeData> {
        // Multi-output node — single-output path is unsupported; mirrors
        // the silero_vad convention (and the #[node(multi_output)] codegen).
        Err(Error::Execution(
            "EmotionExtractorNode requires streaming mode — use process_streaming()".into(),
        ))
    }

    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        _session_id: Option<String>,
        mut callback: F,
    ) -> Result<usize>
    where
        F: FnMut(RuntimeData) -> Result<()> + Send,
    {
        let raw = match data {
            RuntimeData::Text(s) => s,
            other => {
                // Non-Text inputs pass through untouched. Rationale: the
                // node may sit on an edge that occasionally carries
                // control or status frames (e.g. ControlMessage, Json
                // envelopes) and shouldn't drop them. Mirrors silero_vad
                // pass-through for non-audio.
                callback(other)?;
                return Ok(1);
            }
        };

        let (channel, body) = split_text_str(&raw);
        let mut stripped = String::with_capacity(body.len());
        let mut json_outputs: Vec<serde_json::Value> = Vec::new();
        let mut last_end = 0usize;

        // One ts_ms per *input frame*, applied to every Json emitted from
        // it. The frame's pts in audio time is set by upstream; this is
        // wall-time on the extractor side, only useful for diagnostics.
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        for cap in self.pattern.captures_iter(body) {
            let whole = match cap.get(0) {
                Some(m) => m,
                None => continue,
            };
            stripped.push_str(&body[last_end..whole.start()]);

            let raw_match = cap
                .get(1)
                .map(|c| c.as_str())
                .unwrap_or_else(|| whole.as_str());

            let (emoji, alias) = match self.aliases.get(raw_match) {
                Some(canonical) => (canonical.clone(), Some(raw_match.to_string())),
                None => (raw_match.to_string(), None),
            };

            // `source_offset_chars` is char-count not byte-offset, since
            // emoji are multi-byte UTF-8 and offsets-in-chars are what
            // a downstream renderer would actually want to align to a
            // visible cursor.
            let offset_chars = body[..whole.start()].chars().count() as u64;

            let mut emotion_json = json!({
                "kind": "emotion",
                "emoji": emoji,
                "source_offset_chars": offset_chars,
                "ts_ms": ts_ms,
            });
            if let Some(a) = alias {
                emotion_json["alias"] = json!(a);
            }
            json_outputs.push(emotion_json);

            last_end = whole.end();
        }
        stripped.push_str(&body[last_end..]);

        // Always emit the Text frame, even when no tag matched, so
        // downstream TTS sees a continuous text stream. Channel survives
        // the strip via tag_text_str.
        let text_out = tag_text_str(&stripped, channel);
        callback(RuntimeData::Text(text_out))?;
        let mut emitted = 1usize;

        for j in json_outputs {
            callback(RuntimeData::Json(j))?;
            emitted += 1;
        }
        Ok(emitted)
    }
}

#[cfg(test)]
mod unit {
    use super::*;

    #[test]
    fn config_default_uses_canonical_pattern() {
        let cfg = EmotionExtractorConfig::default();
        assert_eq!(cfg.pattern, DEFAULT_EMOTION_PATTERN);
        assert!(cfg.aliases.is_empty());
    }

    #[test]
    fn with_default_pattern_compiles_without_panic() {
        let _ = EmotionExtractorNode::with_default_pattern();
    }

    #[test]
    fn malformed_pattern_returns_err() {
        assert!(EmotionExtractorNode::with_pattern("[unbalanced").is_err());
    }
}
