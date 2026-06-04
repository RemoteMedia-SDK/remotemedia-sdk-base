//! `S2SCoordinatorNode` — joins audio + tool-decision per utterance,
//! then emits a typed-RPC `set_context` + `reset_history` sequence
//! followed by the audio frame on a single main-port edge to a
//! downstream audio LLM.
//!
//! See `openspec/changes/add-s2s-tool-orchestrator/design.md` for the
//! architectural justification. The short version: the audio LLM
//! cannot call tools; we run a separate text path (Whisper → tool
//! classifier → tool executor) in parallel and re-converge here.
//!
//! ## Wire shape
//!
//! The coordinator emits three frame *kinds* on its single output
//! edge, in order, per utterance where the tool path produced a
//! context string:
//!
//! ```text
//! 1. RuntimeData::Text "{\"__aux_port__\":\"set_context\",
//!                        \"payload\":{\"args\":[<ctx>],\"kwargs\":{}}}"
//! 2. RuntimeData::Text "{\"__aux_port__\":\"reset_history\",
//!                        \"payload\":{\"args\":[],\"kwargs\":{}}}"
//! 3. RuntimeData::Audio { samples, sample_rate, channels, ... }
//! ```
//!
//! When the tool path returns `null` (no tool needed, low confidence,
//! miss, or timeout) only the audio is emitted.
//!
//! Probe 0a / 0b in `examples/probes/` validated typed-RPC dispatch
//! and back-to-back FIFO ordering of these envelopes.

use crate::data::RuntimeData;
use crate::error::Error;
use crate::nodes::SyncStreamingNode;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub const DECISION_TIMEOUT_MS_DEFAULT: u64 = 600;
pub const BARGE_IN_WINDOW_MS_DEFAULT: u64 = 0;

/// Configuration for [`S2SCoordinatorNode`].
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct S2SCoordinatorConfig {
    /// Maximum time (ms) the coordinator waits for the tool decision
    /// after the matching audio has arrived. On expiry, the audio is
    /// forwarded with no context envelopes. Set to `0` to wait
    /// indefinitely.
    #[serde(alias = "decisionTimeoutMs")]
    pub decision_timeout_ms: u64,

    /// If a new utterance is ready to emit and the previous utterance
    /// was emitted less than this many ms ago, prepend a `barge_in`
    /// envelope to interrupt the audio LLM's in-flight generation.
    /// Set to `0` to disable barge-in.
    #[serde(alias = "bargeInWindowMs")]
    pub barge_in_window_ms: u64,
}

impl Default for S2SCoordinatorConfig {
    fn default() -> Self {
        Self {
            decision_timeout_ms: DECISION_TIMEOUT_MS_DEFAULT,
            barge_in_window_ms: BARGE_IN_WINDOW_MS_DEFAULT,
        }
    }
}

/// A pending audio utterance waiting for its matching decision.
#[derive(Debug)]
struct PendingAudio {
    audio: RuntimeData,
    /// When the audio arrived. Used for `decision_timeout_ms`.
    arrived_at: Instant,
    /// Monotonic order within this session, for tracing.
    utterance_id: u64,
}

/// A decision (context string or null) waiting for its matching audio.
#[derive(Debug)]
struct PendingDecision {
    /// `None` = "no context needed" (executor said miss / null / unknown).
    /// `Some(s)` = "set this as context before the audio".
    context: Option<String>,
}

/// Per-session state.
#[derive(Debug, Default)]
struct SessionState {
    audio_queue: VecDeque<PendingAudio>,
    decision_queue: VecDeque<PendingDecision>,
    /// Wall-clock at which we last emitted to the audio LLM. Used by
    /// the `barge_in_window_ms` heuristic.
    last_emit_at: Option<Instant>,
}

/// Coordinator that joins audio utterances with tool decisions and
/// drives a downstream audio LLM via typed-RPC envelopes.
pub struct S2SCoordinatorNode {
    cfg: S2SCoordinatorConfig,
    states: Arc<Mutex<HashMap<String, SessionState>>>,
    utterance_counter: AtomicU64,
}

impl S2SCoordinatorNode {
    pub fn with_config(cfg: S2SCoordinatorConfig) -> Self {
        Self {
            cfg,
            states: Arc::new(Mutex::new(HashMap::new())),
            utterance_counter: AtomicU64::new(0),
        }
    }

    pub fn new() -> Self {
        Self::with_config(S2SCoordinatorConfig::default())
    }

    /// Build the typed-RPC envelope as the wire shape the audio LLM
    /// dispatches against. See design.md "Typed-RPC envelope dispatch
    /// on the main edge".
    fn rpc_envelope(method: &str, args: Value) -> RuntimeData {
        let env = json!({
            "__aux_port__": method,
            "payload": {
                "args": args,
                "kwargs": {},
            }
        });
        RuntimeData::Text(env.to_string())
    }

    /// Classify an inbound `RuntimeData` as either an audio utterance,
    /// a tool decision, or something to ignore. Decisions are
    /// recognised by JSON shape (`{context: <str|null>}`); they may
    /// arrive as `RuntimeData::Json` or `RuntimeData::Text` (JSON
    /// string) since Python multiprocess nodes commonly emit
    /// JSON-as-text.
    fn classify_input(data: &RuntimeData) -> InputKind {
        match data {
            RuntimeData::Audio { .. } => InputKind::Audio,
            RuntimeData::Json(v) => Self::context_from_json(v)
                .map(InputKind::Decision)
                .unwrap_or(InputKind::Ignore),
            RuntimeData::Text(s) => {
                // Try to parse the text as a `{context: ...}` JSON.
                // Plain text or non-conforming JSON falls through to
                // Ignore — it's not an envelope shape we know.
                serde_json::from_str::<Value>(s)
                    .ok()
                    .and_then(|v| Self::context_from_json(&v))
                    .map(InputKind::Decision)
                    .unwrap_or(InputKind::Ignore)
            }
            _ => InputKind::Ignore,
        }
    }

    /// Pull a `context` field out of a JSON value. `null` and missing
    /// both map to `None`; a string maps to `Some(string)`; anything
    /// else returns `None` (treated as "no context"). Returns
    /// `Some(Option<String>)` only when the value carries a `context`
    /// key at all — that's how we tell decisions apart from random
    /// JSON.
    fn context_from_json(v: &Value) -> Option<Option<String>> {
        // Be lax about discrimination: ANY object that carries a
        // `context` key (even with a non-string value) is treated as
        // a decision envelope, since the spec says the executor
        // emits exactly `{context: <string|null>}`. Wrap-text
        // payloads (`{tool: ...}` from the classifier raw) are not
        // a decision shape and stay Ignore.
        let obj = v.as_object()?;
        if !obj.contains_key("context") {
            return None;
        }
        let inner = match obj.get("context") {
            Some(Value::Null) => None,
            Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
            // Anything else (including empty string, numbers, etc.) is treated as null.
            _ => None,
        };
        Some(inner)
    }

    /// Try to pair the head of each queue and emit. Also flushes
    /// audio that has waited longer than `decision_timeout_ms` with
    /// no matching decision.
    fn drain_session(
        &self,
        session_id: &str,
        state: &mut SessionState,
        callback: &mut dyn FnMut(RuntimeData) -> Result<(), Error>,
    ) -> Result<usize, Error> {
        let mut emitted = 0;

        loop {
            // Case 1: both queues have heads — pair them and emit.
            if !state.audio_queue.is_empty() && !state.decision_queue.is_empty() {
                let audio = state.audio_queue.pop_front().expect("checked non-empty");
                let decision = state.decision_queue.pop_front().expect("checked non-empty");
                emitted += self.emit_pair(session_id, &audio, &decision, state, callback)?;
                continue;
            }

            // Case 2: audio is waiting but no decision yet. Check timeout.
            if let Some(head_audio) = state.audio_queue.front() {
                let waited_ms = head_audio.arrived_at.elapsed().as_millis() as u64;
                if self.cfg.decision_timeout_ms > 0 && waited_ms >= self.cfg.decision_timeout_ms {
                    tracing::warn!(
                        target: "s2s::coordinator",
                        session = %session_id,
                        utterance_id = head_audio.utterance_id,
                        waited_ms,
                        "tool_path_timeout — emitting audio with no context"
                    );
                    let audio = state.audio_queue.pop_front().expect("checked non-empty");
                    let null_decision = PendingDecision { context: None };
                    emitted +=
                        self.emit_pair(session_id, &audio, &null_decision, state, callback)?;
                    continue;
                }
            }

            break;
        }

        Ok(emitted)
    }

    /// Emit one paired (audio, decision) sequence.
    fn emit_pair(
        &self,
        session_id: &str,
        audio: &PendingAudio,
        decision: &PendingDecision,
        state: &mut SessionState,
        callback: &mut dyn FnMut(RuntimeData) -> Result<(), Error>,
    ) -> Result<usize, Error> {
        let mut count = 0;

        // Barge-in: if a previous utterance was emitted within the
        // barge_in_window_ms, prepend a `barge_in` envelope.
        if self.cfg.barge_in_window_ms > 0 {
            if let Some(last) = state.last_emit_at {
                let since_ms = last.elapsed().as_millis() as u64;
                if since_ms < self.cfg.barge_in_window_ms {
                    tracing::debug!(
                        target: "s2s::coordinator",
                        session = %session_id,
                        utterance_id = audio.utterance_id,
                        since_last_emit_ms = since_ms,
                        "emitting barge_in envelope before next utterance"
                    );
                    callback(Self::rpc_envelope("barge_in", json!([])))?;
                    count += 1;
                }
            }
        }

        // Conditional set_context.
        if let Some(ctx) = &decision.context {
            callback(Self::rpc_envelope("set_context", json!([ctx])))?;
            count += 1;
            // Reset history is only meaningful when there's new context to apply.
            callback(Self::rpc_envelope("reset_history", json!([])))?;
            count += 1;
        }

        // The audio frame itself.
        callback(audio.audio.clone())?;
        count += 1;

        state.last_emit_at = Some(Instant::now());

        tracing::info!(
            target: "s2s::coordinator",
            session = %session_id,
            utterance_id = audio.utterance_id,
            with_context = decision.context.is_some(),
            frames_emitted = count,
            "utterance emitted to audio LLM"
        );

        Ok(count)
    }
}

enum InputKind {
    Audio,
    Decision(Option<String>),
    Ignore,
}

impl Default for S2SCoordinatorNode {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncStreamingNode for S2SCoordinatorNode {
    fn node_type(&self) -> &str {
        "S2SCoordinatorNode"
    }

    fn process(&self, _data: RuntimeData) -> Result<RuntimeData, Error> {
        Err(Error::Execution(
            "S2SCoordinatorNode requires streaming mode — \
             factory must declare is_multi_output_streaming=true"
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
        let kind = Self::classify_input(&data);

        let mut states = self.states.lock();
        let state = states.entry(session_key.clone()).or_default();

        match kind {
            InputKind::Audio => {
                let utterance_id = self.utterance_counter.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(
                    target: "s2s::coordinator",
                    session = %session_key,
                    utterance_id,
                    audio_pending = state.audio_queue.len() + 1,
                    decision_pending = state.decision_queue.len(),
                    "audio enqueued"
                );
                state.audio_queue.push_back(PendingAudio {
                    audio: data,
                    arrived_at: Instant::now(),
                    utterance_id,
                });
            }
            InputKind::Decision(context) => {
                tracing::debug!(
                    target: "s2s::coordinator",
                    session = %session_key,
                    has_context = context.is_some(),
                    audio_pending = state.audio_queue.len(),
                    decision_pending = state.decision_queue.len() + 1,
                    "decision enqueued"
                );
                state.decision_queue.push_back(PendingDecision { context });
            }
            InputKind::Ignore => {
                tracing::trace!(
                    target: "s2s::coordinator",
                    session = %session_key,
                    "ignoring unrecognised input"
                );
                return Ok(0);
            }
        }

        self.drain_session(&session_key, state, callback)
    }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

pub struct S2SCoordinatorNodeFactory;

impl crate::nodes::StreamingNodeFactory for S2SCoordinatorNodeFactory {
    fn create(
        &self,
        _node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn crate::nodes::StreamingNode>, Error> {
        use crate::nodes::SyncNodeWrapper;
        let cfg: S2SCoordinatorConfig = serde_json::from_value(params.clone())
            .map_err(|e| Error::Execution(format!("S2SCoordinatorNode params: {e}")))?;
        Ok(Box::new(SyncNodeWrapper(S2SCoordinatorNode::with_config(
            cfg,
        ))))
    }

    fn node_type(&self) -> &str {
        "S2SCoordinatorNode"
    }

    fn is_multi_output_streaming(&self) -> bool {
        // Per utterance: up to 4 frames (barge_in, set_context, reset_history, audio).
        true
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("S2SCoordinatorNode")
                .description(
                    "Joins audio utterances with tool decisions and \
                     emits typed-RPC set_context/reset_history envelopes \
                     followed by the audio frame to a downstream audio LLM.",
                )
                .category("s2s")
                .accepts([
                    RuntimeDataType::Audio,
                    RuntimeDataType::Json,
                    RuntimeDataType::Text,
                ])
                .produces([RuntimeDataType::Audio, RuntimeDataType::Text])
                .config_schema_from::<S2SCoordinatorConfig>(),
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn audio(sr: u32, len: usize) -> RuntimeData {
        RuntimeData::Audio {
            samples: vec![0.0_f32; len].into(),
            sample_rate: sr,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        }
    }

    fn decision_with(ctx: Option<&str>) -> RuntimeData {
        RuntimeData::Json(json!({ "context": ctx }))
    }

    fn step(node: &S2SCoordinatorNode, input: RuntimeData) -> Vec<RuntimeData> {
        let mut out = Vec::new();
        let mut cb = |d: RuntimeData| {
            out.push(d);
            Ok(())
        };
        node.process_streaming(input, Some("s"), &mut cb).unwrap();
        out
    }

    fn envelope_method(d: &RuntimeData) -> Option<String> {
        let RuntimeData::Text(t) = d else { return None };
        let v: Value = serde_json::from_str(t).ok()?;
        v.get("__aux_port__")?.as_str().map(|s| s.to_string())
    }

    #[test]
    fn audio_then_decision_emits_context_reset_audio() {
        let node = S2SCoordinatorNode::new();
        let out1 = step(&node, audio(24_000, 1024));
        assert!(out1.is_empty(), "audio alone must not emit");

        let out2 = step(&node, decision_with(Some("Patient is in Neurology Ward.")));
        assert_eq!(out2.len(), 3, "expect set_context + reset_history + audio");
        assert_eq!(envelope_method(&out2[0]).as_deref(), Some("set_context"));
        assert_eq!(envelope_method(&out2[1]).as_deref(), Some("reset_history"));
        assert!(matches!(out2[2], RuntimeData::Audio { .. }));
    }

    #[test]
    fn decision_then_audio_emits_in_same_order() {
        // Order of arrival can vary — decision can land first. Output
        // sequence must still be set_context → reset → audio.
        let node = S2SCoordinatorNode::new();
        let out1 = step(&node, decision_with(Some("ctx")));
        assert!(out1.is_empty(), "decision alone must not emit");

        let out2 = step(&node, audio(24_000, 1024));
        assert_eq!(out2.len(), 3);
        assert_eq!(envelope_method(&out2[0]).as_deref(), Some("set_context"));
        assert_eq!(envelope_method(&out2[1]).as_deref(), Some("reset_history"));
        assert!(matches!(out2[2], RuntimeData::Audio { .. }));
    }

    #[test]
    fn null_decision_emits_only_audio() {
        let node = S2SCoordinatorNode::new();
        step(&node, audio(24_000, 1024));
        let out = step(&node, decision_with(None));
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], RuntimeData::Audio { .. }));
    }

    #[test]
    fn utterances_serialise_in_order() {
        let node = S2SCoordinatorNode::new();
        // Two audios pile up, then two decisions arrive. Both
        // utterances should drain in arrival order.
        step(&node, audio(24_000, 1024));
        step(&node, audio(24_000, 2048));
        let out1 = step(&node, decision_with(Some("a")));
        assert_eq!(out1.len(), 3, "first utterance drains with context");
        let out2 = step(&node, decision_with(None));
        assert_eq!(out2.len(), 1, "second utterance drains as audio only");
        assert!(matches!(out2[0], RuntimeData::Audio { .. }));
    }

    #[test]
    fn text_decision_envelope_accepted() {
        // Python multiprocess emits JSON-as-text. Verify both shapes
        // are recognised.
        let node = S2SCoordinatorNode::new();
        step(&node, audio(24_000, 1024));
        let text_decision = RuntimeData::Text(r#"{"context": "from python"}"#.to_string());
        let out = step(&node, text_decision);
        assert_eq!(out.len(), 3);
        assert_eq!(envelope_method(&out[0]).as_deref(), Some("set_context"));
    }

    #[test]
    fn typed_rpc_envelope_payload_shape_is_correct() {
        let node = S2SCoordinatorNode::new();
        step(&node, audio(24_000, 1024));
        let out = step(&node, decision_with(Some("payload-check")));

        let RuntimeData::Text(t0) = &out[0] else {
            panic!("first frame must be set_context envelope")
        };
        let v: Value = serde_json::from_str(t0).unwrap();
        assert_eq!(v["__aux_port__"], "set_context");
        // Both args and kwargs MUST be present per the typed-RPC contract.
        assert!(v["payload"]["args"].is_array());
        assert!(v["payload"]["kwargs"].is_object());
        assert_eq!(v["payload"]["args"][0], "payload-check");

        let RuntimeData::Text(t1) = &out[1] else {
            panic!("second frame must be reset_history envelope")
        };
        let v: Value = serde_json::from_str(t1).unwrap();
        assert_eq!(v["__aux_port__"], "reset_history");
        assert!(v["payload"]["args"].is_array());
        assert!(v["payload"]["kwargs"].is_object());
    }

    #[test]
    fn timeout_fires_on_subsequent_input() {
        let node = S2SCoordinatorNode::with_config(S2SCoordinatorConfig {
            decision_timeout_ms: 10,
            barge_in_window_ms: 0,
        });
        step(&node, audio(24_000, 1024));
        std::thread::sleep(std::time::Duration::from_millis(30));
        // A second audio arrives — drain_session sees the first
        // audio has waited > 10ms and emits it without context.
        let out = step(&node, audio(24_000, 2048));
        // First emit is the timed-out audio with no envelopes.
        // (We expect 1 frame from the first audio; the second is
        // still queued because nothing came after it.)
        assert!(!out.is_empty(), "timeout must drain the first audio");
        assert!(matches!(out[0], RuntimeData::Audio { .. }));
    }

    #[test]
    fn barge_in_envelope_prepended_when_overlap() {
        let node = S2SCoordinatorNode::with_config(S2SCoordinatorConfig {
            decision_timeout_ms: 0,
            barge_in_window_ms: 60_000, // 60s — well above any test timing
        });
        // First utterance: ordinary 3-frame emit.
        step(&node, audio(24_000, 1024));
        let out1 = step(&node, decision_with(Some("first")));
        assert_eq!(out1.len(), 3);

        // Second utterance immediately after — within barge window.
        step(&node, audio(24_000, 1024));
        let out2 = step(&node, decision_with(Some("second")));
        assert_eq!(out2.len(), 4, "barge_in + set_context + reset + audio");
        assert_eq!(envelope_method(&out2[0]).as_deref(), Some("barge_in"));
        assert_eq!(envelope_method(&out2[1]).as_deref(), Some("set_context"));
        assert_eq!(envelope_method(&out2[2]).as_deref(), Some("reset_history"));
        assert!(matches!(out2[3], RuntimeData::Audio { .. }));
    }
}
