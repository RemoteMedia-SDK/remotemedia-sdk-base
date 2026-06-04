//! Conversation Coordinator Node
//!
//! Sits between `llm` and `audio` (TTS) on the data path of a
//! speech-to-speech pipeline (see `qwen_s2s_webrtc_server.rs`) and owns
//! the authoritative turn-phase state machine. Responsibilities:
//!
//! - Absorbs the sentence-collection role (`TextCollectorNode`) for
//!   `channel="tts"` text; forwards `channel="ui"` text verbatim.
//! - Observes VAD `is_speech_start` / `is_speech_end` from a wired
//!   `vad → coordinator` edge and advances a monotonic `turn_id`.
//! - Gates TTS-bound text: if the user barges in while the agent is
//!   speaking, any LLM text still in flight for the cancelled turn is
//!   dropped before it reaches TTS (the coordinator IS the gate).
//! - Publishes a `turn_state` Json envelope on its own output. Because
//!   Rust streaming nodes have their outputs auto-tapped onto the
//!   control bus as `<node_id>.out`, the browser can subscribe to
//!   `coordinator.out` and get an authoritative state stream.
//! - Runs a lazy LLM-silence watchdog: if no LLM text arrives for
//!   `llm_silence_timeout_ms` while in AgentThinking/AgentSpeaking,
//!   synthesises a `<|text_end|>` downstream and returns to Idle.
//!
//! **Not in scope for iteration 1**: publishing `llm.in.barge_in` /
//! `audio.in.barge_in` / `flush_audio` from the server. The browser
//! keeps doing that unchanged (see `session.ts`). The coordinator's
//! gating role covers the race window where the LLM produces text
//! between "user spoke" and "LLM's own barge handler fired".

#[cfg(test)]
use crate::data::tag_text_str;

use crate::data::{split_text_str, RuntimeData, TEXT_CHANNEL_DEFAULT};
use crate::error::Error;
use crate::nodes::text_collector::{extract_sentences, parse_boundary_chars};
use crate::nodes::{StreamingNode, StreamingNodeFactory, SyncNodeWrapper, SyncStreamingNode};
use crate::transport::session_control::{global_bus, ControlAddress, AUX_PORT_ENVELOPE_KEY};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Configuration for the conversation coordinator.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct ConversationCoordinatorConfig {
    /// Split pattern (boundary chars) for sentence detection, e.g. `"[.!?;\\n]+"`.
    /// If None, uses `.!?;\n`.
    #[serde(alias = "splitPattern")]
    pub split_pattern: Option<String>,

    /// Minimum sentence length before yielding to TTS.
    #[serde(alias = "minSentenceLength")]
    pub min_sentence_length: usize,

    /// If true, flush any buffered partial sentence when `<|text_end|>`
    /// arrives (so a reply ending without punctuation still speaks).
    #[serde(alias = "yieldPartialOnEnd")]
    pub yield_partial_on_end: bool,

    /// Speculative first-chunk emission threshold (chars). On the
    /// **first** text chunk of an agent turn, flush as soon as the
    /// buffered LLM output reaches this length AND a soft boundary
    /// (space, comma, semicolon, colon) appears at or after the
    /// threshold — even if no hard sentence terminator (`. ! ?`) has
    /// arrived yet.
    ///
    /// This trades a small amount of prosody quality on the very first
    /// audio chunk for ~300–600 ms lower time-to-first-audio. Subsequent
    /// chunks revert to the normal `split_pattern` boundary detection.
    ///
    /// `0` (default) disables the feature — sentence emission waits
    /// for hard boundaries as before.
    #[serde(alias = "firstChunkMinChars")]
    pub first_chunk_min_chars: usize,

    /// LLM-silence watchdog (ms). If no LLM token arrives within this
    /// window while the agent is thinking/speaking, emit a synthetic
    /// `<|text_end|>` downstream and return to Idle.
    #[serde(alias = "llmSilenceTimeoutMs")]
    pub llm_silence_timeout_ms: u64,

    /// Minimum gap between two VAD `is_speech_start` events that each
    /// cause a turn advancement (ms). Below this gap the second event
    /// is ignored — a debounce to coalesce VAD flutter.
    #[serde(alias = "userSpeechDebounceMs")]
    pub user_speech_debounce_ms: u64,

    /// Node IDs to fan a user-barge to as aux-port publishes on
    /// `<node>.in.barge_in`. Fires only when VAD `is_speech_start`
    /// arrives in an agent phase (i.e. a real cut-off, not the opening
    /// of a fresh turn). Empty list disables server-side fanout and
    /// the client retains responsibility for publishing barge-in.
    ///
    /// Default `["llm", "audio"]` matches the `qwen_s2s_webrtc_server`
    /// manifest. For the single-node LFM2 pipeline set this to
    /// `["audio"]`.
    #[serde(alias = "bargeInTargets")]
    pub barge_in_targets: Vec<String>,
}

impl Default for ConversationCoordinatorConfig {
    fn default() -> Self {
        Self {
            split_pattern: None,
            min_sentence_length: 2,
            yield_partial_on_end: true,
            // 45s default. Qwen3-MLX first-token latency on M-series
            // is routinely 8-12s, and longer prompts or context
            // injections can push that further. A 12s watchdog fires
            // on the first legitimate token and kills the turn. 45s
            // still catches a genuinely wedged model within a
            // reasonable window.
            llm_silence_timeout_ms: 45_000,
            user_speech_debounce_ms: 150,
            barge_in_targets: vec!["llm".to_string(), "audio".to_string()],
            first_chunk_min_chars: 0,
        }
    }
}

/// Turn-phase state machine.
///
/// The original design document listed a transient `UserTurnClosed` state
/// between `UserSpeaking` and `AgentThinking`, but since the transition
/// is synchronous (on the VAD `is_speech_end` frame) we collapse
/// directly into `AgentThinking` and never actually observe it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Idle,
    UserSpeaking,
    AgentThinking,
    AgentSpeaking,
}

impl Phase {
    fn as_str(&self) -> &'static str {
        match self {
            Phase::Idle => "IDLE",
            Phase::UserSpeaking => "USER_SPEAKING",
            Phase::AgentThinking => "AGENT_THINKING",
            Phase::AgentSpeaking => "AGENT_SPEAKING",
        }
    }

    fn is_agent(&self) -> bool {
        matches!(self, Phase::AgentThinking | Phase::AgentSpeaking)
    }
}

/// Per-session mutable state.
#[derive(Debug)]
struct CoordinatorState {
    turn_id: u64,
    phase: Phase,
    text_buffer: String,
    /// Wall-clock ms of the last LLM activity (frame received in
    /// AgentThinking/AgentSpeaking). Zero when no turn is active.
    last_llm_activity_ms: u64,
    /// Wall-clock ms of the last VAD `is_speech_start` that was
    /// accepted (i.e. caused a state change). Used for debounce.
    last_user_start_ms: u64,
    /// Whether the speculative-first-chunk emission has fired for
    /// the current turn. Reset to `false` on every `text_buffer.clear()`
    /// site (turn end, barge, reset, silence timeout). Honoured only
    /// when `first_chunk_min_chars > 0`.
    first_chunk_emitted: bool,
    /// Set to `true` when we cancel a turn that was already in an
    /// agent phase (AgentThinking or AgentSpeaking). The cancelled
    /// LLM stream may still drain a `<|text_end|>` after the new turn
    /// has opened; honouring it would flip the new turn straight to
    /// Idle. The flag is consumed by the first `<|text_end|>` that
    /// arrives in the new turn's AgentThinking phase, or cleared on
    /// the AgentThinking → AgentSpeaking transition (any real new-turn
    /// token means the cancelled stream has stopped draining).
    expect_stale_text_end: bool,
}

impl Default for CoordinatorState {
    fn default() -> Self {
        Self {
            turn_id: 0,
            phase: Phase::Idle,
            text_buffer: String::new(),
            last_llm_activity_ms: 0,
            last_user_start_ms: 0,
            first_chunk_emitted: false,
            expect_stale_text_end: false,
        }
    }
}

/// Conversation coordinator node.
pub struct ConversationCoordinatorNode {
    boundary_chars: Vec<char>,
    min_sentence_length: usize,
    yield_partial_on_end: bool,
    first_chunk_min_chars: usize,
    llm_silence_timeout_ms: u64,
    user_speech_debounce_ms: u64,
    barge_in_targets: Vec<String>,
    /// Node ID this instance was registered under, used as the tap
    /// address when publishing `turn_state` envelopes on the side
    /// channel. Nodes don't normally need their own id, but the
    /// coordinator does because it publishes its control-plane stream
    /// out-of-band to avoid turn_state envelopes bleeding into the
    /// downstream data path (which would otherwise reach TTS and get
    /// spoken by any stringifying TTS node like Kokoro).
    node_id: String,
    states: Arc<Mutex<HashMap<String, CoordinatorState>>>,
}

impl ConversationCoordinatorNode {
    pub fn with_config(node_id: String, config: ConversationCoordinatorConfig) -> Self {
        Self {
            boundary_chars: parse_boundary_chars(config.split_pattern.as_deref()),
            min_sentence_length: config.min_sentence_length,
            yield_partial_on_end: config.yield_partial_on_end,
            first_chunk_min_chars: config.first_chunk_min_chars,
            llm_silence_timeout_ms: config.llm_silence_timeout_ms,
            user_speech_debounce_ms: config.user_speech_debounce_ms,
            barge_in_targets: config.barge_in_targets,
            node_id,
            states: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Speculative first-chunk extractor. If the buffer has at least
    /// `min_chars` UTF-8 characters AND a soft boundary (space, comma,
    /// semicolon, colon, em-dash) appears at or after the threshold,
    /// return `(prefix_to_emit, remaining_buffer)`. Otherwise `None`,
    /// leaving the buffer untouched.
    ///
    /// The threshold is in **chars**, not bytes — multibyte text (em-
    /// dashes, smart quotes) won't fire too early. We include the
    /// boundary character in the emitted prefix so TTS sees natural
    /// punctuation rather than a word fragment.
    fn try_split_first_chunk(buf: &str, min_chars: usize) -> Option<(String, String)> {
        if min_chars == 0 {
            return None;
        }
        let mut chars_seen = 0usize;
        let mut split_at: Option<usize> = None;
        for (byte_idx, c) in buf.char_indices() {
            chars_seen += 1;
            if chars_seen < min_chars {
                continue;
            }
            if c == ' ' || c == ',' || c == ';' || c == ':' || c == '\t' || c == '\n' || c == '—'
            {
                split_at = Some(byte_idx + c.len_utf8());
                break;
            }
        }
        let split_at = split_at?;
        let prefix = buf[..split_at].trim_start().to_string();
        if prefix.is_empty() {
            return None;
        }
        let remainder = buf[split_at..].to_string();
        Some((prefix, remainder))
    }

    /// Fan a server-side barge-in publish to each configured target
    /// node's `in.barge_in` aux port. Runs fire-and-forget — we don't
    /// want the sync `process_streaming` blocking on a wedged target.
    ///
    /// If there is no process-wide `SessionControlBus` installed
    /// (tests, degenerate configurations), or we're not in a tokio
    /// context, this is a silent no-op. In deployed pipelines that
    /// would mean the client-side fallback still works; in tests the
    /// state-machine transitions (which are what the tests cover) run
    /// unchanged.
    fn dispatch_barge(&self, session_id: &str) {
        if self.barge_in_targets.is_empty() {
            return;
        }
        let Some(bus) = global_bus() else {
            tracing::debug!("[Coordinator] no global SessionControlBus — skipping barge publish");
            return;
        };
        let Some(ctrl) = bus.get(session_id) else {
            tracing::debug!(
                "[Coordinator] no SessionControl registered for session {} — skipping barge",
                session_id
            );
            return;
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::debug!("[Coordinator] no tokio runtime — skipping barge publish");
            return;
        };
        for target in &self.barge_in_targets {
            let ctrl = ctrl.clone();
            let addr = ControlAddress::node_in(target).with_port("barge_in");
            let target = target.clone();
            handle.spawn(async move {
                if let Err(e) = ctrl.publish(&addr, RuntimeData::Text("barge".into())).await {
                    tracing::warn!(
                        "[Coordinator] barge publish to {}.in.barge_in failed: {}",
                        target,
                        e
                    );
                }
            });
        }

        // Ask the transport to drain any audio already queued to the
        // client. Without this, a barge halts generation at the LLM and
        // TTS but the listener still hears whatever was buffered in the
        // WebRTC ring (up to ~30 s) before the room goes silent. Hook
        // is absent on transports that don't need it (gRPC) — silent
        // no-op in that case. Fire-and-forget.
        let ctrl_for_flush = ctrl.clone();
        handle.spawn(async move {
            let _ = ctrl_for_flush.request_flush_audio().await;
        });
    }
}

/// Build a `turn_state` envelope ready to emit on `coordinator.out`.
fn turn_state_json(
    turn_id: u64,
    phase: Phase,
    cancelled_turn_id: Option<u64>,
    error: Option<&str>,
    ts_ms: u64,
) -> RuntimeData {
    RuntimeData::Json(serde_json::json!({
        "kind": "turn_state",
        "turn_id": turn_id,
        "phase": phase.as_str(),
        "cancelled_turn_id": cancelled_turn_id,
        "error": error,
        "ts_ms": ts_ms,
    }))
}

impl SyncStreamingNode for ConversationCoordinatorNode {
    fn node_type(&self) -> &str {
        "ConversationCoordinatorNode"
    }

    fn process(&self, _data: RuntimeData) -> Result<RuntimeData, Error> {
        Err(Error::Execution(
            "ConversationCoordinatorNode requires streaming mode — \
             callers must use process_streaming()"
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
        let now = Self::now_ms();

        // Collect emissions under the lock, release, then fire callbacks.
        // `pending` carries frames meant for the downstream data path
        // (text sentences, sentinels) — these also get tapped onto
        // `<node>.out` by the router, which is fine for subscribers.
        // `pending_tap` carries frames meant ONLY for the tap (e.g.
        // `turn_state` envelopes) and MUST NOT reach TTS — they're
        // delivered via `SessionControl::publish_tap` bypassing the
        // data path.
        let mut pending: Vec<RuntimeData> = Vec::new();
        let mut pending_tap: Vec<RuntimeData> = Vec::new();
        // Track whether this frame triggered a cancellation so we can
        // fan a server-side barge-in publish AFTER the lock is released
        // (dispatch_barge touches the tokio runtime and must not run
        // inside the parking_lot mutex).
        let mut fire_barge = false;

        {
            let mut states = self.states.lock();
            let state = states
                .entry(session_key.clone())
                .or_insert_with(CoordinatorState::default);

            // Refresh the activity counter BEFORE the watchdog check
            // when the incoming frame is itself LLM activity. Without
            // this, a model with a long first-token latency (Qwen3
            // on MLX is ~10s cold) trips the watchdog on the very
            // frame that proves the LLM is alive — emitting a
            // synthetic `<|text_end|>`, flipping to Idle, and then
            // dropping every subsequent real token because the phase
            // gate rejects them.
            if let RuntimeData::Text(raw) = &data {
                let (channel, _) = split_text_str(raw);
                if channel == TEXT_CHANNEL_DEFAULT && state.phase.is_agent() {
                    state.last_llm_activity_ms = now;
                }
            }

            // ── Watchdog ─────────────────────────────────────────────
            // Lazy check on every frame: if we've been in an agent
            // phase and no LLM activity happened within the timeout,
            // force the turn to end cleanly so TTS drains and the UI
            // returns to idle.
            if state.phase.is_agent()
                && state.last_llm_activity_ms > 0
                && now.saturating_sub(state.last_llm_activity_ms) >= self.llm_silence_timeout_ms
            {
                if self.yield_partial_on_end {
                    let trimmed = state.text_buffer.trim_start().to_string();
                    if !trimmed.is_empty() {
                        pending.push(RuntimeData::Text(trimmed));
                    }
                    state.text_buffer.clear();
                }
                state.first_chunk_emitted = false;
                pending.push(RuntimeData::Text("<|text_end|>".to_string()));
                state.phase = Phase::Idle;
                state.last_llm_activity_ms = 0;
                pending_tap.push(turn_state_json(
                    state.turn_id,
                    state.phase,
                    None,
                    Some("llm_silence_timeout"),
                    now,
                ));
            }

            // ── Dispatch by payload shape ────────────────────────────
            match &data {
                RuntimeData::Json(value) => {
                    // Aux-port envelope? (e.g. coordinator.in.reset)
                    if let Some(port) = value.get(AUX_PORT_ENVELOPE_KEY).and_then(Value::as_str) {
                        match port {
                            "reset" => {
                                let old_turn = state.turn_id;
                                state.turn_id += 1;
                                state.phase = Phase::Idle;
                                state.text_buffer.clear();
                                state.first_chunk_emitted = false;
                                state.last_llm_activity_ms = 0;
                                pending_tap.push(turn_state_json(
                                    state.turn_id,
                                    state.phase,
                                    Some(old_turn),
                                    Some("reset"),
                                    now,
                                ));
                            }
                            "barge_in" => {
                                // Manual barge-in, not tied to a VAD
                                // onset — e.g. user clicked a UI
                                // button. Same semantics as the
                                // speech_start path when firing from
                                // an agent phase: advance turn, drop
                                // buffer, fan to target nodes.
                                // From non-agent phases this is a
                                // no-op on the state side but still
                                // publishes downstream so any in-
                                // flight residual output gets halted.
                                let old_turn = state.turn_id;
                                let was_agent = state.phase.is_agent();
                                if was_agent {
                                    state.turn_id += 1;
                                    state.phase = Phase::UserSpeaking;
                                    state.text_buffer.clear();
                                    state.first_chunk_emitted = false;
                                    state.last_llm_activity_ms = 0;
                                    state.last_user_start_ms = now;
                                    state.expect_stale_text_end = true;
                                    pending_tap.push(turn_state_json(
                                        state.turn_id,
                                        state.phase,
                                        Some(old_turn),
                                        None,
                                        now,
                                    ));
                                }
                                fire_barge = true;
                            }
                            other => {
                                tracing::debug!(
                                    "[Coordinator] ignoring unknown aux port '{}'",
                                    other
                                );
                            }
                        }
                    } else if value.get("is_speech_start").is_some()
                        || value.get("is_speech_end").is_some()
                    {
                        // VAD event.
                        let is_start = value
                            .get("is_speech_start")
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                        let is_end = value
                            .get("is_speech_end")
                            .and_then(Value::as_bool)
                            .unwrap_or(false);

                        if is_start {
                            // Debounce only within an ongoing user
                            // turn — i.e. VAD flutter during a single
                            // onset should coalesce into one turn.
                            // A speech_start arriving while the agent
                            // is speaking is always treated as a
                            // legit barge-in and must not be suppressed
                            // by debounce.
                            let in_user_turn = state.phase == Phase::UserSpeaking;
                            let gap = now.saturating_sub(state.last_user_start_ms);
                            let debounced = in_user_turn
                                && state.last_user_start_ms != 0
                                && gap < self.user_speech_debounce_ms;
                            if !debounced {
                                let old_turn = state.turn_id;
                                let was_agent = state.phase.is_agent();
                                state.turn_id += 1;
                                state.phase = Phase::UserSpeaking;
                                state.text_buffer.clear();
                                state.first_chunk_emitted = false;
                                state.last_llm_activity_ms = 0;
                                state.last_user_start_ms = now;
                                pending_tap.push(turn_state_json(
                                    state.turn_id,
                                    state.phase,
                                    if was_agent { Some(old_turn) } else { None },
                                    None,
                                    now,
                                ));
                                // Only dispatch a server-side barge
                                // when we're actually cutting an
                                // active agent turn — firing on a
                                // fresh user turn from Idle would
                                // just be noise to the target nodes.
                                if was_agent {
                                    fire_barge = true;
                                    // Expect a possible drained
                                    // `<|text_end|>` from the cancelled
                                    // turn's LLM stream.
                                    state.expect_stale_text_end = true;
                                }
                            }
                        } else if is_end && state.phase == Phase::UserSpeaking {
                            state.phase = Phase::AgentThinking;
                            state.last_llm_activity_ms = now;
                            pending_tap.push(turn_state_json(
                                state.turn_id,
                                state.phase,
                                None,
                                None,
                                now,
                            ));
                        }
                        // Any other VAD event (has_speech with no
                        // start/end transition) is an observability
                        // frame — we don't forward it downstream.
                    } else {
                        // Non-VAD Json. We don't know what it is; the
                        // safe default is to DROP it rather than
                        // passing it through — downstream TTS nodes
                        // like Kokoro stringify whatever they see and
                        // would happily speak a JSON blob. If a future
                        // upstream node sends a legitimate Json frame
                        // that TTS needs, we can whitelist it here.
                        tracing::debug!(
                            "[Coordinator] dropping unrecognized Json frame: {}",
                            value
                        );
                    }
                }
                RuntimeData::Text(raw) => {
                    // Channel-aware LLM text handling.
                    let (channel, content) = split_text_str(raw);
                    let has_text_end = content.contains("<|text_end|>");
                    let cleaned = content.replace("<|text_end|>", "");

                    if channel != TEXT_CHANNEL_DEFAULT {
                        // UI / display / any non-default channel goes
                        // ONLY to the control-bus side channel as a
                        // structured envelope. If we forwarded it
                        // downstream, Kokoro would pass the tagged
                        // text through verbatim (including the
                        // `\0\u{2}ui` channel header), the browser's
                        // audio.out text subscriber would render it as
                        // a caption, and the UI would show garbage.
                        // The browser subscribes to `coordinator.out`
                        // for display_text instead.
                        //
                        // Heartbeat: any LLM text — including reasoning
                        // tokens emitted on the `think` channel by
                        // Qwen3/DeepSeek-style models — counts as
                        // activity for the silence watchdog. Without
                        // this, a model spending 30+ s inside a
                        // `<think>` block (no `delta.content` yet) was
                        // killed prematurely by the watchdog and
                        // produced no spoken reply.
                        if state.phase.is_agent() && !cleaned.is_empty() {
                            state.last_llm_activity_ms = now;
                        }
                        if !cleaned.is_empty() {
                            pending_tap.push(RuntimeData::Json(serde_json::json!({
                                "kind": "display_text",
                                "channel": channel,
                                "text": cleaned,
                                "turn_id": state.turn_id,
                                "ts_ms": now,
                            })));
                        }
                        // `<|text_end|>` carried on a non-default
                        // channel is the LLM signalling end-of-UI-
                        // stream. Swallow it — the coordinator
                        // emits its own authoritative `<|text_end|>`
                        // only when the tts-channel stream ends. We
                        // also never forward an empty text frame.
                    } else {
                        // TTS channel: gated by phase. Transition into
                        // AgentSpeaking on first meaningful text if we
                        // were in AgentThinking.
                        //
                        // Wake-on-text exception (tool-relay path): if
                        // we're sitting in IDLE and a fresh non-empty
                        // TTS chunk arrives, the audio LLM has started
                        // a second generation on the same logical turn
                        // — typically the v4.1 `tool_result_speak`
                        // relay fired by an instruct agent after the
                        // ack's `<|text_end|>` already closed the
                        // first generation. We re-arm into
                        // AgentThinking (keeping `turn_id` the same)
                        // so the relay's text flows through the normal
                        // sentence-splitter + final `<|text_end|>` →
                        // IDLE path, AND so a VAD speech_start during
                        // the relay still triggers a real barge (the
                        // barge branch only fires when leaving an
                        // agent phase). The SPA's coordinator handler
                        // defers `finalizeTurn` while a tool call is
                        // in flight, so this AgentThinking-from-IDLE
                        // transition doesn't open a phantom turn in
                        // the UI — it continues the same one.
                        //
                        // Stale TTS text from a cancelled turn could
                        // also trip this branch, but the
                        // `expect_stale_text_end` flag (set on barge)
                        // is already cleared on the AgentThinking →
                        // AgentSpeaking transition below, so a barged
                        // drain doesn't get mistaken for a relay.
                        if state.phase == Phase::Idle && !cleaned.is_empty() {
                            state.phase = Phase::AgentThinking;
                            state.last_llm_activity_ms = now;
                            pending_tap.push(turn_state_json(
                                state.turn_id,
                                state.phase,
                                None,
                                None,
                                now,
                            ));
                        }
                        let active = state.phase.is_agent();
                        if active {
                            state.last_llm_activity_ms = now;
                            if state.phase == Phase::AgentThinking && !cleaned.is_empty() {
                                state.phase = Phase::AgentSpeaking;
                                // Real new-turn token arrived; any
                                // cancelled-turn sentinel still in flight
                                // can be honoured normally now (the next
                                // text_end belongs to this turn).
                                state.expect_stale_text_end = false;
                                pending_tap.push(turn_state_json(
                                    state.turn_id,
                                    state.phase,
                                    None,
                                    None,
                                    now,
                                ));
                            }

                            state.text_buffer.push_str(&cleaned);

                            // Speculative first-chunk emission: trade
                            // a little prosody on the very first
                            // utterance for ~300–600 ms lower TTFA.
                            // Only fires once per turn and only if
                            // `extract_sentences` wouldn't already
                            // produce something on its own. Subsequent
                            // emissions fall through to the normal
                            // sentence boundary path below.
                            if self.first_chunk_min_chars > 0
                                && !state.first_chunk_emitted
                                && state.phase == Phase::AgentSpeaking
                            {
                                if let Some((prefix, remainder)) = Self::try_split_first_chunk(
                                    &state.text_buffer,
                                    self.first_chunk_min_chars,
                                ) {
                                    pending.push(RuntimeData::Text(prefix));
                                    state.text_buffer = remainder;
                                    state.first_chunk_emitted = true;
                                }
                            }

                            let (sentences, remainder) = extract_sentences(
                                &state.text_buffer,
                                &self.boundary_chars,
                                self.min_sentence_length,
                            );
                            for s in sentences {
                                pending.push(RuntimeData::Text(s));
                            }
                            state.text_buffer = remainder;

                            if has_text_end {
                                // Stale-text_end guard. A `<|text_end|>`
                                // arriving in AgentThinking after a
                                // barge from an agent phase is leftover
                                // from the cancelled turn's draining
                                // LLM stream. Honouring it would flip
                                // the new turn straight to Idle and the
                                // phase gate would silently drop every
                                // real reply token. Consume the flag and
                                // drop.
                                //
                                // Without that flag set, a text_end in
                                // AgentThinking is the genuine end of a
                                // tool-only or empty-response turn — flip
                                // to Idle so the UI can finalise the
                                // turn (otherwise it hangs until the
                                // silence watchdog fires minutes later).
                                let is_stale_drain = state.phase == Phase::AgentThinking
                                    && state.expect_stale_text_end;
                                if is_stale_drain {
                                    tracing::debug!(
                                        "[Coordinator] dropping stale <|text_end|> from cancelled turn in {:?}",
                                        state.phase
                                    );
                                    state.expect_stale_text_end = false;
                                } else if state.phase.is_agent() {
                                    if self.yield_partial_on_end {
                                        let trimmed = state.text_buffer.trim_start().to_string();
                                        if !trimmed.is_empty() {
                                            pending.push(RuntimeData::Text(trimmed));
                                        }
                                        state.text_buffer.clear();
                                    }
                                    state.first_chunk_emitted = false;
                                    state.expect_stale_text_end = false;
                                    pending.push(RuntimeData::Text("<|text_end|>".to_string()));
                                    state.phase = Phase::Idle;
                                    state.last_llm_activity_ms = 0;
                                    pending_tap.push(turn_state_json(
                                        state.turn_id,
                                        state.phase,
                                        None,
                                        None,
                                        now,
                                    ));
                                }
                            }
                        } else {
                            // Phase is Idle / User*, so we drop TTS
                            // text. If a `<|text_end|>` arrives in
                            // this state it's leftover from a
                            // cancelled turn — drop it too so TTS
                            // doesn't emit a spurious `<|audio_end|>`
                            // for a turn that was already cancelled.
                            tracing::debug!(
                                "[Coordinator] dropping tts text in phase {:?}: {} chars",
                                state.phase,
                                cleaned.len()
                            );
                        }
                    }
                }
                other => {
                    // DROP everything that isn't Text or Json. In
                    // particular, audio frames arrive here because
                    // `vad` fans out BOTH its JSON event AND its
                    // passthrough audio on every chunk, and the
                    // `vad → coordinator` manifest edge delivers
                    // both. Forwarding audio to the downstream TTS
                    // node would wake it on every 512-sample frame
                    // for no reason — constant IPC churn plus
                    // choppy synthesis as the TTS model thrashes
                    // between "processing" and "ready". Binary /
                    // control frames similarly have no consumer on
                    // this text path.
                    tracing::debug!(
                        "[Coordinator] dropping non-text frame: {:?}",
                        other.data_type()
                    );
                }
            }
        } // lock released

        if fire_barge {
            self.dispatch_barge(&session_key);
        }

        // Emit turn_state envelopes on the side channel so they reach
        // `coordinator.out` subscribers but NOT downstream TTS. Safe
        // no-op when no SessionControl is registered (e.g. unit tests).
        if !pending_tap.is_empty() {
            if let Some(ctrl) = global_bus().and_then(|b| b.get(&session_key)) {
                for frame in pending_tap {
                    ctrl.publish_tap(&self.node_id, None, frame);
                }
            } else {
                tracing::debug!(
                    "[Coordinator] no SessionControl for session {} — dropping {} turn_state envelopes",
                    session_key,
                    pending_tap.len()
                );
            }
        }

        let count = pending.len();
        for out in pending {
            callback(out)?;
        }
        Ok(count)
    }
}

/// Factory for `ConversationCoordinatorNode`.
pub struct ConversationCoordinatorNodeFactory;

impl StreamingNodeFactory for ConversationCoordinatorNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: ConversationCoordinatorConfig =
            serde_json::from_value(params.clone()).unwrap_or_default();
        let node = ConversationCoordinatorNode::with_config(node_id, config);
        Ok(Box::new(SyncNodeWrapper(node)))
    }

    fn node_type(&self) -> &str {
        "ConversationCoordinatorNode"
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("ConversationCoordinatorNode")
                .description(
                    "Authoritative turn-phase state machine between LLM and TTS. \
                     Absorbs the sentencer role, observes VAD to detect barge-in, \
                     and publishes turn_state envelopes on its output.",
                )
                .category("conversation")
                .accepts([RuntimeDataType::Text, RuntimeDataType::Json])
                .produces([RuntimeDataType::Text, RuntimeDataType::Json])
                .config_schema_from::<ConversationCoordinatorConfig>(),
        )
    }
}

// ─────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::session_control::{SessionControl, SessionControlBus};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEST_SESSION_COUNTER: AtomicUsize = AtomicUsize::new(0);

    /// Install a session control for the current test on the global
    /// bus, return the node id ("coordinator") and the unique
    /// session_id wired up. The returned receiver captures every
    /// `turn_state` envelope the coordinator publishes on its tap.
    fn test_rig() -> (
        ConversationCoordinatorNode,
        String,
        tokio::sync::broadcast::Receiver<RuntimeData>,
    ) {
        test_rig_with(ConversationCoordinatorConfig::default())
    }

    fn test_rig_with(
        cfg: ConversationCoordinatorConfig,
    ) -> (
        ConversationCoordinatorNode,
        String,
        tokio::sync::broadcast::Receiver<RuntimeData>,
    ) {
        // Ensure a global bus exists (first-writer-wins across tests).
        SessionControlBus::install_global(SessionControlBus::new());
        let bus = global_bus().expect("global bus installed");

        let n = TEST_SESSION_COUNTER.fetch_add(1, Ordering::Relaxed);
        let session_id = format!("test-session-{}", n);
        let ctrl = SessionControl::new(session_id.clone());
        bus.register(ctrl.clone());

        let rx = ctrl
            .subscribe(&ControlAddress::node_out("coordinator"))
            .expect("subscribe to coordinator.out");

        let node = ConversationCoordinatorNode::with_config("coordinator".to_string(), cfg);
        (node, session_id, rx)
    }

    fn drive_with(
        node: &ConversationCoordinatorNode,
        session_id: &str,
        inputs: Vec<RuntimeData>,
    ) -> Vec<RuntimeData> {
        let mut out = Vec::new();
        for input in inputs {
            let mut cb = |d: RuntimeData| -> Result<(), Error> {
                out.push(d);
                Ok(())
            };
            node.process_streaming(input, Some(session_id), &mut cb)
                .unwrap();
        }
        out
    }

    fn drain_turn_states(
        rx: &mut tokio::sync::broadcast::Receiver<RuntimeData>,
    ) -> Vec<serde_json::Value> {
        let mut v = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(RuntimeData::Json(j)) => v.push(j),
                Ok(_) => continue,
                Err(_) => break,
            }
        }
        v
    }

    fn vad_start() -> RuntimeData {
        RuntimeData::Json(serde_json::json!({
            "has_speech": true,
            "is_speech_start": true,
            "is_speech_end": false,
            "speech_probability": 0.9,
        }))
    }

    fn vad_end() -> RuntimeData {
        RuntimeData::Json(serde_json::json!({
            "has_speech": false,
            "is_speech_start": false,
            "is_speech_end": true,
            "speech_probability": 0.1,
        }))
    }

    fn llm(text: &str) -> RuntimeData {
        RuntimeData::Text(text.to_string())
    }

    fn find_texts(out: &[RuntimeData]) -> Vec<&str> {
        out.iter()
            .filter_map(|d| match d {
                RuntimeData::Text(s) => Some(s.as_str()),
                _ => None,
            })
            .collect()
    }

    fn find_jsons(out: &[RuntimeData]) -> Vec<&serde_json::Value> {
        out.iter()
            .filter_map(|d| match d {
                RuntimeData::Json(v) => Some(v),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn test_vad_start_advances_turn_id() {
        let (node, session_id, mut rx) = test_rig();
        let out = drive_with(&node, &session_id, vec![vad_start()]);
        assert!(
            find_jsons(&out).is_empty(),
            "turn_state must not leak to the data path (would be spoken by TTS)"
        );
        let turn_states = drain_turn_states(&mut rx);
        assert_eq!(turn_states.len(), 1);
        assert_eq!(turn_states[0]["turn_id"], 1);
        assert_eq!(turn_states[0]["phase"], "USER_SPEAKING");
    }

    #[test]
    fn test_llm_text_gated_by_phase() {
        let (node, session_id, mut rx) = test_rig();
        // Wake-on-text: text arriving in Idle transitions to AgentThinking
        // and flows through (tool-relay path). The phase gate still applies
        // during User* phases where text is dropped.
        let out = drive_with(&node, &session_id, vec![llm("hello there.")]);
        let texts = find_texts(&out);
        assert!(
            texts.iter().any(|t| t.contains("hello there.")),
            "expected text to flow via wake-on-text, got {:?}",
            texts
        );
        // Verify phase transitioned from Idle -> AgentThinking -> AgentSpeaking
        let turn_states = drain_turn_states(&mut rx);
        assert!(
            turn_states.iter().any(|ts| ts["phase"] == "AGENT_THINKING"),
            "expected AgentThinking phase transition, got {:?}",
            turn_states
        );
        assert!(
            turn_states.iter().any(|ts| ts["phase"] == "AGENT_SPEAKING"),
            "expected AgentSpeaking phase transition, got {:?}",
            turn_states
        );
    }

    #[test]
    fn test_sentencer_emits_complete_sentences() {
        let (node, session_id, _rx) = test_rig();
        let out = drive_with(
            &node,
            &session_id,
            vec![
                vad_start(),
                vad_end(),
                llm("Hello"),
                llm(" world"),
                llm("."),
            ],
        );
        let texts = find_texts(&out);
        assert!(
            texts.iter().any(|t| t.contains("Hello world.")),
            "expected 'Hello world.' in texts, got {:?}",
            texts
        );
        assert!(
            find_jsons(&out).is_empty(),
            "data path must not carry turn_state envelopes"
        );
    }

    #[test]
    fn test_cancel_drops_buffered_text() {
        let (node, session_id, mut rx) = test_rig();
        let _ = drive_with(
            &node,
            &session_id,
            vec![
                vad_start(),
                vad_end(),
                llm("Partial reply with no terminator"),
            ],
        );
        // Flush the turn_states from the first transitions so the
        // next drain reflects only what the barge produced.
        let _ = drain_turn_states(&mut rx);
        // User barges in.
        let out = drive_with(&node, &session_id, vec![vad_start()]);
        let texts = find_texts(&out);
        assert!(
            texts.is_empty(),
            "expected no forwarded text after barge, got {:?}",
            texts
        );
        let turn_states = drain_turn_states(&mut rx);
        assert_eq!(turn_states.len(), 1);
        assert_eq!(turn_states[0]["phase"], "USER_SPEAKING");
        assert_eq!(turn_states[0]["turn_id"], 2);
        assert_eq!(turn_states[0]["cancelled_turn_id"], 1);
    }

    #[test]
    fn test_ui_channel_routed_to_side_channel_only() {
        // Regression: UI-channel text (`show(content=...)` markdown)
        // MUST NOT flow downstream where Kokoro would pass it through
        // to the browser as garbled `\0\u{2}ui…` text. It belongs on
        // the `coordinator.out` side channel as a `display_text`
        // envelope the browser can render directly.
        let (node, session_id, mut rx) = test_rig();
        let ui = tag_text_str("# heading with no terminator", "ui");
        let out = drive_with(&node, &session_id, vec![RuntimeData::Text(ui.clone())]);
        assert!(
            find_texts(&out).is_empty(),
            "UI text leaked to data path: {:?}",
            find_texts(&out)
        );
        let taps = drain_turn_states_or_display(&mut rx);
        let display: Vec<&serde_json::Value> = taps
            .iter()
            .filter(|v| v["kind"] == "display_text")
            .collect();
        assert_eq!(display.len(), 1);
        assert_eq!(display[0]["channel"], "ui");
        assert_eq!(display[0]["text"], "# heading with no terminator");
    }

    /// Test helper: drain every RuntimeData::Json envelope (turn_state
    /// AND display_text) from the tap.
    fn drain_turn_states_or_display(
        rx: &mut tokio::sync::broadcast::Receiver<RuntimeData>,
    ) -> Vec<serde_json::Value> {
        let mut v = Vec::new();
        loop {
            match rx.try_recv() {
                Ok(RuntimeData::Json(j)) => v.push(j),
                Ok(_) => continue,
                Err(_) => break,
            }
        }
        v
    }

    #[test]
    fn test_text_end_flushes_partial() {
        let (node, session_id, mut rx) = test_rig();
        let out = drive_with(
            &node,
            &session_id,
            vec![
                vad_start(),
                vad_end(),
                llm("No period"),
                llm("<|text_end|>"),
            ],
        );
        let texts = find_texts(&out);
        assert!(texts.iter().any(|t| *t == "No period"), "got {:?}", texts);
        assert!(
            texts.iter().any(|t| *t == "<|text_end|>"),
            "got {:?}",
            texts
        );
        let turn_states = drain_turn_states(&mut rx);
        let last = turn_states.last().unwrap();
        assert_eq!(last["phase"], "IDLE");
    }

    #[test]
    fn test_watchdog_llm_silence() {
        let (node, session_id, mut rx) = test_rig_with(ConversationCoordinatorConfig {
            llm_silence_timeout_ms: 50,
            ..Default::default()
        });
        let _ = drive_with(&node, &session_id, vec![vad_start(), vad_end()]);
        std::thread::sleep(std::time::Duration::from_millis(80));
        let tick = RuntimeData::Json(serde_json::json!({
            "has_speech": false,
            "speech_probability": 0.0,
        }));
        let out = drive_with(&node, &session_id, vec![tick]);
        let turn_states = drain_turn_states(&mut rx);
        assert!(
            turn_states
                .iter()
                .any(|v| v["error"] == "llm_silence_timeout"),
            "expected llm_silence_timeout turn_state, got {:?}",
            turn_states
        );
        let texts = find_texts(&out);
        assert!(
            texts.iter().any(|t| *t == "<|text_end|>"),
            "expected synthetic <|text_end|>, got {:?}",
            texts
        );
    }

    #[test]
    fn test_server_side_barge_dispatch_noop_without_runtime() {
        // When there's no tokio runtime the dispatch_barge helper
        // must silently no-op. The state-machine transition
        // (cancelled_turn_id) still fires and reaches the tap.
        let (node, session_id, mut rx) = test_rig();
        let _ = drive_with(
            &node,
            &session_id,
            vec![vad_start(), vad_end(), llm("Reply")],
        );
        let _ = drain_turn_states(&mut rx);
        let _ = drive_with(&node, &session_id, vec![vad_start()]);
        let turn_states = drain_turn_states(&mut rx);
        assert_eq!(turn_states.len(), 1);
        assert_eq!(turn_states[0]["cancelled_turn_id"], 1);
    }

    #[test]
    fn test_vad_start_debounce() {
        let (node, session_id, mut rx) = test_rig_with(ConversationCoordinatorConfig {
            user_speech_debounce_ms: 10_000,
            ..Default::default()
        });
        let _ = drive_with(&node, &session_id, vec![vad_start(), vad_start()]);
        let turn_states = drain_turn_states(&mut rx);
        assert_eq!(
            turn_states.len(),
            1,
            "expected a single turn_state for two debounced starts, got {:?}",
            turn_states
        );
        assert_eq!(turn_states[0]["turn_id"], 1);
    }

    #[test]
    fn test_stale_text_end_from_cancelled_turn_does_not_idle_new_turn() {
        // Regression for the live trace where a cancelled turn's
        // `<|text_end|>` was still draining from the LLM, arrived in
        // the new turn's AgentThinking phase, flipped the coordinator
        // to Idle, and caused every subsequent real reply token to
        // be dropped.
        let (node, session_id, mut rx) = test_rig();
        // Turn 1 speaks a real reply.
        let _ = drive_with(
            &node,
            &session_id,
            vec![vad_start(), vad_end(), llm("Hi there.")],
        );
        // Barge — turn 2 opens.
        let _ = drive_with(&node, &session_id, vec![vad_start(), vad_end()]);
        let _ = drain_turn_states(&mut rx);

        // Stale <|text_end|> from cancelled turn 1 arrives. Must not
        // flip turn 2 to Idle.
        let _ = drive_with(&node, &session_id, vec![llm("<|text_end|>")]);
        // Real reply for turn 2.
        let out = drive_with(
            &node,
            &session_id,
            vec![llm("Fresh reply."), llm("<|text_end|>")],
        );
        let texts = find_texts(&out);
        assert!(
            texts.iter().any(|t| t.contains("Fresh reply.")),
            "turn 2 reply was dropped; got {:?}",
            texts
        );
        assert!(
            texts.iter().any(|t| *t == "<|text_end|>"),
            "expected final text_end forwarded, got {:?}",
            texts
        );
    }

    #[test]
    fn test_tool_only_turn_idles_on_text_end_in_agent_thinking() {
        // Tool-only LLM responses (e.g. perform_motion) emit no text on
        // the tts channel, so the coordinator never advances past
        // AgentThinking. After my chat_backend fix the LLM stream
        // always emits a `<|text_end|>` at end-of-stream — the
        // coordinator must honour it from AgentThinking when the turn
        // wasn't preceded by a cancellation, otherwise the UI hangs
        // until the silence watchdog fires.
        let (node, session_id, mut rx) = test_rig();
        // Open a fresh turn from Idle (not via barge) and send a
        // text_end before any spoken text arrives.
        let _ = drive_with(
            &node,
            &session_id,
            vec![vad_start(), vad_end(), llm("<|text_end|>")],
        );

        let states = drain_turn_states(&mut rx);
        let phases: Vec<_> = states.iter().filter_map(|v| v["phase"].as_str()).collect();
        assert!(
            phases.last() == Some(&"IDLE"),
            "expected final phase IDLE for tool-only turn, got {:?}",
            phases
        );
    }

    #[test]
    fn test_watchdog_does_not_drop_slow_first_token() {
        // Regression for the Qwen3-MLX ~10s first-token latency:
        // with a short watchdog, the first real LLM token used to
        // arrive AFTER the deadline, trip the watchdog, and get
        // dropped by the resulting phase flip. The fix refreshes
        // `last_llm_activity_ms` on the incoming LLM text BEFORE
        // the watchdog check, so a late-but-valid token is honoured.
        let (node, session_id, mut rx) = test_rig_with(ConversationCoordinatorConfig {
            llm_silence_timeout_ms: 30, // forces the problem instantly in test
            ..Default::default()
        });
        let _ = drive_with(&node, &session_id, vec![vad_start(), vad_end()]);
        // Sleep past the watchdog so `now - last_llm_activity_ms`
        // would exceed the timeout on the next frame.
        std::thread::sleep(std::time::Duration::from_millis(80));
        // Now the LLM's first token arrives. It MUST NOT be
        // dropped; the watchdog must not fire.
        let out = drive_with(
            &node,
            &session_id,
            vec![llm("Hello world."), llm("<|text_end|>")],
        );
        let texts = find_texts(&out);
        assert!(
            texts.iter().any(|t| t.contains("Hello world.")),
            "expected sentence forwarded, got {:?}",
            texts
        );
        // Drain and confirm we saw AGENT_SPEAKING (not the watchdog's
        // error envelope).
        let turn_states = drain_turn_states(&mut rx);
        let phases: Vec<&str> = turn_states
            .iter()
            .map(|v| v["phase"].as_str().unwrap_or(""))
            .collect();
        assert!(
            phases.contains(&"AGENT_SPEAKING"),
            "expected AGENT_SPEAKING in lifecycle, got {:?}",
            phases
        );
        for ts in &turn_states {
            assert_ne!(
                ts["error"], "llm_silence_timeout",
                "watchdog fired on a legitimate late-arriving token"
            );
        }
    }

    #[test]
    fn test_audio_frames_from_vad_fanout_are_dropped() {
        // The `vad → coordinator` edge delivers BOTH the VAD JSON
        // event AND the passthrough audio (see `silero_vad.rs`
        // emitting two outputs per input). The coordinator must drop
        // the audio — forwarding it to the TTS branch wakes Kokoro
        // on every 512-sample chunk and produces choppy output.
        let (node, session_id, _rx) = test_rig();
        let audio = RuntimeData::Audio {
            samples: vec![0.0_f32; 512].into(),
            sample_rate: 16000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        };
        let out = drive_with(&node, &session_id, vec![audio]);
        assert!(
            out.is_empty(),
            "audio frames must not pass through the coordinator: {:?}",
            out.iter().map(|d| d.data_type()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_turn_state_never_leaks_to_data_path() {
        // Regression guard: Kokoro TTS stringifies whatever it receives
        // and will speak any JSON envelope that reaches its input. The
        // coordinator MUST emit turn_state exclusively via publish_tap.
        let (node, session_id, mut rx) = test_rig();
        let out = drive_with(
            &node,
            &session_id,
            vec![
                vad_start(),
                vad_end(),
                llm("Hello world."),
                llm("<|text_end|>"),
            ],
        );
        for d in &out {
            match d {
                RuntimeData::Json(v) => panic!(
                    "turn_state / JSON envelope leaked into the data path: {}",
                    v
                ),
                RuntimeData::Text(_) => {} // expected: sentences + sentinels
                _ => {}
            }
        }
        // Tap must have observed the full phase lifecycle.
        let turn_states = drain_turn_states(&mut rx);
        assert!(turn_states.len() >= 3, "got {:?}", turn_states);
        let phases: Vec<&str> = turn_states
            .iter()
            .map(|v| v["phase"].as_str().unwrap_or(""))
            .collect();
        assert!(phases.contains(&"USER_SPEAKING"));
        assert!(phases.contains(&"AGENT_SPEAKING"));
        assert!(phases.contains(&"IDLE"));
    }
}
