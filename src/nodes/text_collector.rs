/// Text Collector Node
///
/// Accumulates streaming text tokens and yields complete sentences based on punctuation.
/// Works with streaming text generation (e.g., LFM2-Audio text output).
///
/// Pipeline flow:
///   LFM2Audio → [Text tokens] → TextCollector → [Complete sentences] → Next node
///
/// The collector receives:
/// - Individual text tokens/chunks
/// - Special tokens like <|text_end|>
///
/// When a sentence boundary is detected (. ! ? , ; :), it outputs the accumulated text.
use crate::data::{split_text_str, tag_text_str, RuntimeData, TEXT_CHANNEL_DEFAULT};
use crate::error::Error;
use crate::nodes::SyncStreamingNode;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Text Collector Node configuration
///
/// Configuration for the text collector streaming node. Uses `#[serde(default)]` to allow
/// partial config, and `#[serde(alias)]` to accept both snake_case and camelCase.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct TextCollectorConfig {
    /// Split pattern for sentence boundaries (e.g., "[.!?;\\n]+")
    /// If not specified, uses default boundary chars: . ! ? ; \n
    #[serde(alias = "splitPattern")]
    pub split_pattern: Option<String>,

    /// Minimum sentence length before yielding (characters)
    #[serde(alias = "minSentenceLength")]
    #[schemars(range(min = 1, max = 1000))]
    pub min_sentence_length: usize,

    /// Yield partial sentences when <|text_end|> or <|audio_end|> is received
    #[serde(alias = "yieldPartialOnEnd")]
    pub yield_partial_on_end: bool,
}

impl Default for TextCollectorConfig {
    fn default() -> Self {
        Self {
            split_pattern: None,
            min_sentence_length: 3,
            yield_partial_on_end: true,
        }
    }
}

/// Buffer state for a single session
#[derive(Debug, Clone)]
struct TextBufferState {
    /// Accumulated text buffer
    accumulated_text: String,
    /// Total chunks accumulated
    chunks_accumulated: usize,
}

impl Default for TextBufferState {
    fn default() -> Self {
        Self {
            accumulated_text: String::new(),
            chunks_accumulated: 0,
        }
    }
}

/// Text Collector Node
pub struct TextCollectorNode {
    /// Sentence boundary characters (default: .!?,;:\n)
    boundary_chars: Vec<char>,

    /// Minimum sentence length before yielding (characters)
    min_sentence_length: usize,

    /// Yield partial sentences when <|text_end|> is received
    yield_partial_on_end: bool,

    /// Buffer states per session
    states: Arc<Mutex<std::collections::HashMap<String, TextBufferState>>>,
}

impl TextCollectorNode {
    /// Create a new TextCollectorNode with the given configuration
    pub fn with_config(config: TextCollectorConfig) -> Self {
        let boundary_chars = parse_boundary_chars(config.split_pattern.as_deref());

        Self {
            boundary_chars,
            min_sentence_length: config.min_sentence_length,
            yield_partial_on_end: config.yield_partial_on_end,
            states: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Create a new TextCollectorNode with optional parameters (legacy API)
    pub fn new(
        split_pattern: Option<String>,
        min_sentence_length: Option<usize>,
        yield_partial_on_end: Option<bool>,
    ) -> Result<Self, Error> {
        Ok(Self::with_config(TextCollectorConfig {
            split_pattern,
            min_sentence_length: min_sentence_length.unwrap_or(3),
            yield_partial_on_end: yield_partial_on_end.unwrap_or(true),
        }))
    }

    fn extract_complete_sentences(&self, buffer: &str) -> (Vec<String>, String) {
        extract_sentences(buffer, &self.boundary_chars, self.min_sentence_length)
    }
}

/// Extract complete sentences from a buffer using the given boundary chars.
///
/// Walks the buffer character by character; when a boundary char is seen,
/// consumes any run of further boundary chars and emits the accumulated
/// slice as one sentence (trimmed of leading whitespace only). Returns
/// `(sentences, remainder)` — callers keep `remainder` in their own
/// buffer and re-feed it on the next call.
///
/// Shared between `TextCollectorNode` and `ConversationCoordinatorNode`
/// so the sentence-boundary semantics stay in lockstep across both.
pub(crate) fn extract_sentences(
    buffer: &str,
    boundary_chars: &[char],
    min_sentence_length: usize,
) -> (Vec<String>, String) {
    let mut sentences = Vec::new();
    let mut current_sentence = String::new();
    let chars: Vec<char> = buffer.chars().collect();
    let is_boundary = |c: char| boundary_chars.contains(&c);

    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        current_sentence.push(c);

        if is_boundary(c) {
            while i + 1 < chars.len() && is_boundary(chars[i + 1]) {
                i += 1;
                current_sentence.push(chars[i]);
            }

            let sentence = current_sentence.trim_start().to_string();
            if sentence.len() >= min_sentence_length {
                sentences.push(sentence);
            }
            current_sentence.clear();
        }

        i += 1;
    }

    let remainder = current_sentence;
    (sentences, remainder)
}

/// Parse a split-pattern string like `"[.!?;\\n]+"` into the set of
/// boundary characters. Used by both `TextCollectorNode` and
/// `ConversationCoordinatorNode` so configs stay compatible.
pub(crate) fn parse_boundary_chars(pattern: Option<&str>) -> Vec<char> {
    if let Some(pattern) = pattern {
        pattern
            .chars()
            .filter(|c| !['[', ']', '+', '\\', 'n', 'r', 't'].contains(c))
            .collect()
    } else {
        vec!['.', '!', '?', ';', '\n']
    }
}

// Phase A-Wave 2: migrated to `SyncStreamingNode`. The body was
// already sync (parking_lot::Mutex, no `.await`); only the trait
// wrapping changes. Multi-output sentence emission preserved via the
// `SyncStreamingNode::process_streaming` hook.
impl SyncStreamingNode for TextCollectorNode {
    fn node_type(&self) -> &str {
        "TextCollectorNode"
    }

    fn process(&self, _data: RuntimeData) -> Result<RuntimeData, Error> {
        Err(Error::Execution(
            "TextCollectorNode requires streaming mode - \
             callers must use process_streaming() (the router does this \
             automatically when the factory declares is_multi_output_streaming=true)"
                .into(),
        ))
    }

    fn process_streaming(
        &self,
        data: RuntimeData,
        session_id: Option<&str>,
        callback: &mut dyn FnMut(RuntimeData) -> Result<(), Error>,
    ) -> Result<usize, Error> {
        let session_key = session_id.unwrap_or("default").to_string();

        // Only process text data
        let raw_text_chunk = match &data {
            RuntimeData::Text(text_string) => text_string.clone(),
            _ => {
                // Pass through non-text data unchanged
                tracing::debug!(
                    "[TextCollector] Passing through non-text data: {:?}",
                    data.data_type()
                );
                callback(data)?;
                return Ok(1);
            }
        };

        // Channel-aware routing. Only the default (`"tts"`) channel
        // gets sentence-split — UI/display channels are passthrough so
        // their markdown / code blocks reach downstream consumers
        // verbatim and don't get carved up mid-line by the sentence
        // boundary regex.
        let (channel, content) = split_text_str(&raw_text_chunk);
        if channel != TEXT_CHANNEL_DEFAULT {
            tracing::debug!(
                "[TextCollector] Session {}: passthrough channel={} ({} chars)",
                session_key,
                channel,
                content.len(),
            );
            // Re-attach the channel prefix so downstream nodes continue
            // to see the same tagged payload shape.
            callback(RuntimeData::Text(tag_text_str(content, channel)))?;
            return Ok(1);
        }
        let text_chunk = content.to_string();

        tracing::debug!(
            "[TextCollector] Session {}: Received text chunk: '{}'",
            session_key,
            text_chunk
        );

        // Collect outputs under the lock, release, then fire callbacks.
        // parking_lot::Mutex is safe here because we never hold it across
        // an `.await`.
        let has_text_end = text_chunk.contains("<|text_end|>");
        let has_audio_end = text_chunk.contains("<|audio_end|>");
        let cleaned_text = text_chunk
            .replace("<|text_end|>", "")
            .replace("<|audio_end|>", "")
            .to_string();

        let mut pending: Vec<RuntimeData> = Vec::new();
        {
            let mut states = self.states.lock();
            let state = states
                .entry(session_key.clone())
                .or_insert_with(TextBufferState::default);

            state.accumulated_text.push_str(&cleaned_text);
            state.chunks_accumulated += 1;

            tracing::debug!(
                "[TextCollector] Session {}: Buffer now: '{}' ({} chars)",
                session_key,
                state.accumulated_text,
                state.accumulated_text.len()
            );

            let (sentences, remainder) = self.extract_complete_sentences(&state.accumulated_text);
            for sentence in sentences {
                tracing::debug!(
                    "[TextCollector] Session {}: Yielding sentence: '{}'",
                    session_key,
                    sentence
                );
                pending.push(RuntimeData::Text(sentence));
            }
            state.accumulated_text = remainder;

            if has_text_end || has_audio_end {
                if self.yield_partial_on_end && !state.accumulated_text.is_empty() {
                    tracing::debug!(
                        "[TextCollector] Session {}: Yielding partial on end: '{}'",
                        session_key,
                        state.accumulated_text
                    );
                    pending.push(RuntimeData::Text(state.accumulated_text.clone()));
                    state.accumulated_text.clear();
                }
                if has_text_end {
                    pending.push(RuntimeData::Text("<|text_end|>".to_string()));
                }
                if has_audio_end {
                    pending.push(RuntimeData::Text("<|audio_end|>".to_string()));
                }
                state.accumulated_text.clear();
                state.chunks_accumulated = 0;
            }
        } // lock released before firing callbacks

        let output_count = pending.len();
        for out in pending {
            callback(out)?;
        }

        Ok(output_count)
    }
}
