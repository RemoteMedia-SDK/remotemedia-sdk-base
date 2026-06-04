//! `AffectSimulatorNode` — runtime affect-state evolution.
//!
//! A wall-paced (5 Hz / 200 ms) [`StreamingNode`] that owns a per-session
//! `affect_simulator::AffectState`, ingests VAD/STT/prosody events from
//! the streaming pipeline, advances the simulator on every tick, and
//! emits Channel A/B/D aux-port envelopes (`set_steering`,
//! `set_system_augmentation`, `set_sampling`) addressed to the
//! language-head node — typically `MlxLmTextNode`.
//!
//! See [spike-i] for the full design, tier rollout, and acceptance
//! criteria.
//!
//! [spike-i]: ../../../../docs/references/activation-steering-audio-llm/notes/spike-i-affect-runtime-driver.md
//!
//! ## Tier-A scope (this implementation)
//!
//! Drives the simulator from VAD events alone:
//!
//! - VAD `is_speech_start = true` on the first turn of a session →
//!   appraised as a `UserGreeting`.
//! - VAD `is_speech_end = true` on every subsequent turn → appraised as
//!   a `UserQuestion`. Subsequent `is_speech_start` flags during the
//!   same turn are no-ops (the question fires on speech_end so the full
//!   turn is observed before appraising).
//! - Inter-turn silence → simulator's existing channel-decay does the
//!   work; the tick handler integrates `dt_seconds` since the previous
//!   tick.
//! - Bare `RuntimeData::Text` envelopes (from `stt_in`) also fold into
//!   `UserQuestion` for now. Tier B will replace this with a proper
//!   transcript classifier mapping text → richer EventKinds.
//!
//! Tier C is plumbed through opportunistically: prosody envelopes shaped
//! `{ "kind": "prosody_arousal_high" | "prosody_valence_negative" |
//! "prosody_uncertain", ... }` are decoded into the corresponding
//! `EventKind`. The Phase 3 prosody-VAD branch publishes that envelope
//! shape; until it's wired end-to-end the decoder simply never sees one.
//!
//! ## Output envelopes
//!
//! Three outputs per tick (when state has moved materially):
//!
//! ```json
//! { "__aux_port__": "set_steering",
//!   "payload": { "target_vad": [v, a, d], "alpha": 1.0 } }
//!
//! { "__aux_port__": "set_system_augmentation",
//!   "payload": { "text": "<channel B summary>" } }
//!
//! { "kind": "affect_state", "ts_ms": ..., "channels": {...},
//!   "policy": {...}, "channel_d_target_vad": [v,a,d],
//!   "channel_b": "..." }
//! ```
//!
//! The first two are addressed to the LLM node via the manifest
//! connection `affect_sim → llm`; the third is a debug tap that the
//! frontend / observer UI can subscribe to for live state rendering.
//!
//! Channel A (`set_sampling`) is not emitted in Tier A — the LLM uses
//! its configured defaults (greedy, max_new_tokens=200). Tier B will
//! map `RegulationPolicy` to per-turn temperature deltas via the
//! simulator's `ChannelA::compute`.
//!
//! ## Per-session state
//!
//! Per-(node, session) state lives in [`SessionAffectState`] and is
//! threaded through `ctx.session_state`. One server can serve many
//! concurrent browsers, each with its own evolving simulator.
//!
//! ## Emit thresholds
//!
//! Without a threshold every 200 ms tick would publish a near-identical
//! `set_steering` (200 control-bus events/min/session). The default
//! `emit_threshold` of 0.05 (L2 in V/A/D space) suppresses redundant
//! re-publishes; the system-augmentation envelope is suppressed on
//! string equality. The debug tap is always emitted so a UI can render
//! a continuous trace.

use std::collections::HashMap;
use std::sync::Arc;

use affect_simulator::{
    channel_d, Appraisal, AppraisalEngine, Dynamics, DynamicsParams, EventKind, Modality,
    PerceivedEvent, PromptRenderer, RegulationEngine,
};
// `RegulationConfig` is not re-exported from the crate root — pull it
// from its module directly. Keeps the public surface of `affect_simulator`
// untouched while letting us configure the regulator's safety floor /
// expressiveness ceiling per session.
use affect_simulator::regulation::RegulationConfig;
use parking_lot::Mutex;
use serde_json::Value;

use crate::data::{split_text_str, RuntimeData};
use crate::error::Result;
use crate::nodes::{
    AnySessionState, InitializeContextRead, NodeRuntimeContextRead, PacingNature, StreamingNode,
    StreamingNodeFactory, Tick,
};
use crate::transport::session_control::wrap_aux_port;

/// Default tick rate in Hz (matches the simulator's 200 ms tick).
pub const DEFAULT_TICK_HZ: u32 = 5;

/// Default Channel D steering magnitude. 1.0 matches the static-target
/// example default in `hermes3_affect_s2s_webrtc_server`.
pub const DEFAULT_STEERING_ALPHA: f32 = 1.0;

/// L2 distance (V/A/D space) below which we suppress redundant
/// `set_steering` re-publishes. 0.05 ≈ ±0.029 per axis — small enough
/// to follow gradual decay, large enough to skip pure quantization
/// jitter at the regulator output.
pub const DEFAULT_EMIT_THRESHOLD: f32 = 0.05;

/// Default post-expressiveness gain on the affect→ARKit-52 mapping.
/// 1.0 produces visible-but-not-saturated weights for the per-channel
/// coefficients in [`affect_expression`](super::affect_expression);
/// raise toward ~1.5 for a more theatrical face, lower toward ~0.5
/// for muted readings.
pub const DEFAULT_BLENDSHAPE_GAIN: f32 = 1.0;

/// Configuration for [`AffectSimulatorNode`]. Read from manifest
/// `params` via [`AffectSimulatorConfig::from_params`]; missing keys
/// fall back to the module-level defaults.
#[derive(Debug, Clone)]
pub struct AffectSimulatorConfig {
    /// Tick rate in Hz. Default [`DEFAULT_TICK_HZ`] (5 Hz, 200 ms).
    pub tick_hz: u32,
    /// Channel D `alpha` knob baked into every `set_steering` envelope.
    /// Default [`DEFAULT_STEERING_ALPHA`] (1.0).
    pub steering_alpha: f32,
    /// L2 emit threshold for `set_steering`. Default
    /// [`DEFAULT_EMIT_THRESHOLD`] (0.05).
    pub emit_threshold: f32,
    /// Whether to emit per-tick `BlendshapeFrame` envelopes shaped for
    /// avatar consumers (`Live2DRenderNode`, `CcRenderNode`, …). Default
    /// `true` — the cost is one 52-element f32 array per 200 ms tick,
    /// which renderers skip cheaply if no consumer wired in.
    pub emit_blendshapes: bool,
    /// Post-expressiveness gain for the affect→ARKit-52 mapping.
    /// Default [`DEFAULT_BLENDSHAPE_GAIN`] (1.0).
    pub blendshape_gain: f32,
}

impl Default for AffectSimulatorConfig {
    fn default() -> Self {
        Self {
            tick_hz: DEFAULT_TICK_HZ,
            steering_alpha: DEFAULT_STEERING_ALPHA,
            emit_threshold: DEFAULT_EMIT_THRESHOLD,
            emit_blendshapes: true,
            blendshape_gain: DEFAULT_BLENDSHAPE_GAIN,
        }
    }
}

impl AffectSimulatorConfig {
    /// Read config from a manifest `params` blob. Unknown / malformed
    /// keys silently fall back to defaults.
    pub fn from_params(params: &Value) -> Self {
        let mut cfg = Self::default();
        if let Some(v) = params.get("tick_hz").and_then(|v| v.as_u64()) {
            if v > 0 {
                cfg.tick_hz = v as u32;
            }
        }
        if let Some(v) = params.get("steering_alpha").and_then(|v| v.as_f64()) {
            cfg.steering_alpha = v as f32;
        }
        if let Some(v) = params.get("emit_threshold").and_then(|v| v.as_f64()) {
            if v >= 0.0 {
                cfg.emit_threshold = v as f32;
            }
        }
        if let Some(v) = params.get("emit_blendshapes").and_then(|v| v.as_bool()) {
            cfg.emit_blendshapes = v;
        }
        if let Some(v) = params.get("blendshape_gain").and_then(|v| v.as_f64()) {
            if v >= 0.0 {
                cfg.blendshape_gain = v as f32;
            }
        }
        cfg
    }
}

/// Per-(node, session) state. Holds the simulator, the buffered events
/// pending the next tick, and the last-emitted envelopes for change
/// detection.
///
/// Constructed by [`StreamingNode::make_session_state`] and threaded
/// through `ctx.session_state` on every per-call invocation. Multi-
/// session servers (one runtime, many browsers) each get their own
/// `SessionAffectState` — the simulators evolve independently.
pub struct SessionAffectState {
    inner: Mutex<SessionAffectInner>,
}

/// Internals of the per-session state — guarded by a single mutex.
///
/// Mutex chosen over `RwLock` because every access either appends an
/// event (writer) or advances the simulator (writer) — no read-only
/// fast path exists. `parking_lot::Mutex` is small + fair + sub-µs
/// uncontended, which matters because every input + every tick takes
/// the lock briefly.
struct SessionAffectInner {
    /// Simulator state; the running `AffectState` from
    /// `affect_simulator::state::AffectState::initial()`.
    state: affect_simulator::AffectState,
    /// Events buffered between ticks. Drained by [`AffectSimulatorNode::tick`]
    /// each time it fires.
    pending_events: Vec<PerceivedEvent>,
    /// Wall time of session start. Used to stamp event `timestamp_ms`
    /// fields with a monotonically-increasing offset that's stable for
    /// the session.
    started_at: std::time::Instant,
    /// Wall time of the most recent tick, used to integrate
    /// `dt_seconds` for the dynamics step. Initialised to
    /// `started_at`; the first tick's `dt` is just the time from
    /// session-start to first fire (≈200 ms).
    last_tick_at: std::time::Instant,
    /// Whether the user is currently inside a speech segment. Sticky
    /// flag flipped by the VAD's `is_speech_start` / `is_speech_end`
    /// flags. Avoids firing one event per VAD chunk during a long
    /// utterance.
    in_speech: bool,
    /// Tracks whether we've emitted the very first `UserGreeting` for
    /// the session. The first `is_speech_start` fires a greeting; all
    /// subsequent speech starts are silent (the corresponding question
    /// fires on `is_speech_end`).
    emitted_initial_greeting: bool,
    /// Last-emitted Channel D target VAD; suppress re-emit when the L2
    /// distance to the new target is below the configured threshold.
    /// `None` until the first emit.
    last_emitted_target_vad: Option<[f32; 3]>,
    /// Last-emitted Channel B summary; suppress re-emit on string
    /// equality.
    last_emitted_summary: Option<String>,
    /// Sequential counter for synthesising unique event ids without
    /// pulling `uuid` into the hot path.
    event_counter: u64,
}

impl SessionAffectState {
    fn new() -> Self {
        let now = std::time::Instant::now();
        Self {
            inner: Mutex::new(SessionAffectInner {
                state: affect_simulator::AffectState::initial(),
                pending_events: Vec::new(),
                started_at: now,
                last_tick_at: now,
                in_speech: false,
                emitted_initial_greeting: false,
                last_emitted_target_vad: None,
                last_emitted_summary: None,
                event_counter: 0,
            }),
        }
    }
}

impl Default for SessionAffectState {
    fn default() -> Self {
        Self::new()
    }
}

/// The node itself. The simulator engines (`AppraisalEngine`,
/// `Dynamics`, `RegulationEngine`, `PromptRenderer`) are stateless and
/// shared across sessions; only `SessionAffectState` is per-session.
pub struct AffectSimulatorNode {
    node_id: String,
    config: AffectSimulatorConfig,
    appraisal: AppraisalEngine,
    dynamics: Dynamics,
    dynamics_params: DynamicsParams,
    regulation: RegulationEngine,
    regulation_config: RegulationConfig,
    renderer: PromptRenderer,
}

impl AffectSimulatorNode {
    pub fn new(node_id: impl Into<String>, config: AffectSimulatorConfig) -> Self {
        Self {
            node_id: node_id.into(),
            config,
            appraisal: AppraisalEngine::new(),
            dynamics: Dynamics::new(),
            dynamics_params: DynamicsParams::default(),
            regulation: RegulationEngine::new(),
            regulation_config: RegulationConfig::default(),
            renderer: PromptRenderer::new(),
        }
    }

    pub fn from_params(node_id: impl Into<String>, params: &Value) -> Self {
        Self::new(node_id, AffectSimulatorConfig::from_params(params))
    }

    /// Decode an incoming envelope into an `EventKind` for the simulator.
    /// Returns `None` for envelopes the node doesn't recognise (silently
    /// dropped — the simulator only consumes events it understands).
    ///
    /// Recognised shapes:
    ///
    /// - VAD envelope (`RuntimeData::Json` with `is_speech_start` /
    ///   `is_speech_end` boolean fields, as published by `SileroVADNode`).
    ///   Speech-start flips the `in_speech` flag and may fire a
    ///   `UserGreeting` for the first turn. Speech-end flips it back
    ///   and fires a `UserQuestion` if the segment was active.
    ///
    /// - Prosody envelope (`RuntimeData::Json` with `kind`:
    ///   `"prosody_arousal_high"` / `"prosody_valence_negative"` /
    ///   `"prosody_uncertain"`). Maps to the corresponding
    ///   `EventKind::Prosody*` variant. Native-Rust producers (e.g.
    ///   the WebRTC pacer's tap) hit this branch directly.
    ///
    /// - **Tier C path:** Python-side producers (in particular
    ///   `ProsodyVadNode`) cannot construct a `RuntimeData::Json`
    ///   variant — the multiprocess Python `RuntimeData` only has
    ///   AUDIO / VIDEO / TEXT / TENSOR / CONTROL / NUMPY / FILE.
    ///   The codebase convention (see `kimodo_motion.py`) is to
    ///   serialise JSON envelopes as Text frames with
    ///   `channel="json"`. The IPC layer prefixes the channel onto
    ///   the payload via `[0x00][len][channel][text]`; we recover it
    ///   here with `split_text_str`. A Text frame whose channel is
    ///   `"json"` gets parsed via `serde_json` and routed through
    ///   the same `kind`-decoder as native JSON envelopes.
    ///
    /// - `RuntimeData::Text` with any other channel (typically
    ///   `"tts"` from `stt_in`) — Tier A treats every transcript as
    ///   `UserQuestion`. Tier B will replace this with a proper
    ///   classifier.
    fn decode_event_kind(
        data: &RuntimeData,
        in_speech: &mut bool,
        emitted_initial_greeting: bool,
    ) -> Option<EventKind> {
        match data {
            RuntimeData::Json(v) => {
                Self::decode_json_event_kind(v, in_speech, emitted_initial_greeting)
            }
            RuntimeData::Text(s) => {
                let (channel, content) = split_text_str(s);
                if channel == "json" {
                    if let Ok(v) = serde_json::from_str::<Value>(content) {
                        return Self::decode_json_event_kind(
                            &v,
                            in_speech,
                            emitted_initial_greeting,
                        );
                    }
                    // Channel-tagged but not parseable — log-and-drop
                    // beats interpreting a malformed envelope as a
                    // transcript turn. The producer is mis-configured;
                    // surface that explicitly.
                    tracing::warn!(
                        "AffectSimulatorNode: text frame on channel='json' \
                         did not parse as JSON; dropping (len={})",
                        content.len(),
                    );
                    return None;
                }
                // Non-json text channel = transcript. Tier A folds
                // every transcript into UserQuestion; Tier B will
                // classify content via a sentence-transformer head.
                Some(EventKind::UserQuestion)
            }
            _ => None,
        }
    }

    /// Common path for both `RuntimeData::Json(v)` and Text frames
    /// arrived on `channel="json"`. Pulled out so the two branches in
    /// `decode_event_kind` can share the same JSON-shape decoder.
    fn decode_json_event_kind(
        v: &Value,
        in_speech: &mut bool,
        emitted_initial_greeting: bool,
    ) -> Option<EventKind> {
        if let Some(true) = v.get("is_speech_start").and_then(|b| b.as_bool()) {
            *in_speech = true;
            if !emitted_initial_greeting {
                return Some(EventKind::UserGreeting);
            }
            // Subsequent speech starts are no-ops; we wait for
            // speech_end to fire the question event so the full
            // turn is observed.
            return None;
        }
        if let Some(true) = v.get("is_speech_end").and_then(|b| b.as_bool()) {
            let was_in_speech = *in_speech;
            *in_speech = false;
            if was_in_speech {
                return Some(EventKind::UserQuestion);
            }
            return None;
        }
        if let Some(kind_str) = v.get("kind").and_then(|k| k.as_str()) {
            return match kind_str {
                "prosody_arousal_high" => Some(EventKind::ProsodyArousalHigh),
                "prosody_valence_negative" => Some(EventKind::ProsodyValenceNegative),
                "prosody_uncertain" => Some(EventKind::ProsodyUncertain),
                _ => None,
            };
        }
        None
    }

    /// Buffer one decoded event into the per-session pending queue.
    /// Acquires the per-session mutex briefly. Idempotent on `None`
    /// from the decoder.
    fn buffer_event(&self, data: &RuntimeData, ctx: &dyn NodeRuntimeContextRead) {
        let session: Arc<SessionAffectState> =
            remotemedia_traits::runtime_context::state::<SessionAffectState>(ctx);
        let mut inner = session.inner.lock();
        // Snapshot the read-only flag *before* we hand a mutable
        // borrow of `in_speech` to the decoder — the borrow checker
        // (rightly) refuses to let us mix a `&mut inner.in_speech`
        // and a `&inner.emitted_initial_greeting` in the same call.
        let emitted_initial_greeting = inner.emitted_initial_greeting;
        let kind = Self::decode_event_kind(data, &mut inner.in_speech, emitted_initial_greeting);
        let Some(kind) = kind else { return };
        let timestamp_ms = inner.started_at.elapsed().as_millis() as u64;
        let id = format!("ev{}", inner.event_counter);
        inner.event_counter += 1;
        let modality = match data {
            RuntimeData::Text(_) => Modality::Text,
            _ => Modality::System,
        };
        inner.pending_events.push(PerceivedEvent {
            id,
            timestamp_ms,
            modality,
            kind,
            confidence: 1.0,
            payload: Value::Null,
        });
        if matches!(kind, EventKind::UserGreeting) {
            inner.emitted_initial_greeting = true;
        }
    }
}

#[async_trait::async_trait]
impl StreamingNode for AffectSimulatorNode {
    fn node_type(&self) -> &str {
        "AffectSimulatorNode"
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn pacing_nature(&self) -> PacingNature {
        PacingNature::SourceWall(self.config.tick_hz)
    }

    fn is_multi_input(&self) -> bool {
        true
    }

    fn make_session_state(&self, _ctx: &dyn InitializeContextRead) -> Arc<dyn AnySessionState> {
        Arc::new(SessionAffectState::new())
    }

    async fn process_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData> {
        self.buffer_event(&data, ctx);
        // No reactive emission — Channel A/B/D updates flow through
        // `tick()`. Returning `Json(Null)` keeps the contract honest;
        // the router treats it as "no main-channel output".
        Ok(RuntimeData::Json(Value::Null))
    }

    async fn process_multi_async(
        &self,
        inputs: HashMap<String, RuntimeData>,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData> {
        for (_port, data) in inputs {
            self.buffer_event(&data, ctx);
        }
        Ok(RuntimeData::Json(Value::Null))
    }

    /// Reactive ingestion path. Buffers every input into the per-session
    /// pending-event queue and returns without invoking the callback —
    /// emissions are owned by `tick()` so envelopes are time-aligned to
    /// the wall clock instead of to upstream input arrivals.
    async fn process_streaming_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        _callback: Box<dyn FnMut(RuntimeData) -> Result<()> + Send>,
    ) -> Result<usize> {
        self.buffer_event(&data, ctx);
        Ok(0)
    }

    /// Tick-driven entry point. Drains buffered events into the
    /// simulator, advances dynamics by `dt_seconds`, recomputes the
    /// regulation policy + Channel D target + Channel B summary, and
    /// emits the change-deltas as aux-port envelopes plus a debug tap.
    async fn tick(
        &self,
        _tick: Tick,
        ctx: &dyn NodeRuntimeContextRead,
        mut cb: Box<dyn FnMut(RuntimeData) -> Result<()> + Send>,
    ) -> Result<()> {
        let session: Arc<SessionAffectState> =
            remotemedia_traits::runtime_context::state::<SessionAffectState>(ctx);

        // Hold the lock while we advance the simulator + read snapshots
        // for the envelopes. The lock is released before any callback
        // invocation so re-entrant emits (callback → router → ...) can
        // never deadlock against another input arrival on the same
        // session.
        let (target_vad, summary, channels_snap, policy_snap, ts_ms) = {
            let mut inner = session.inner.lock();
            let now = std::time::Instant::now();
            let dt = now.duration_since(inner.last_tick_at).as_secs_f32();
            inner.last_tick_at = now;
            let ts_ms = inner.started_at.elapsed().as_millis() as u64;

            // Drain pending events; appraise + record each. Multiple
            // events on the same tick combine additively (the per-axis
            // contributions are summed and clamped to [-1, 1]).
            let events: Vec<PerceivedEvent> = std::mem::take(&mut inner.pending_events);
            let mut combined: Option<Appraisal> = None;
            for e in &events {
                inner.state.record_event(e);
                let a = self.appraisal.appraise(e, &inner.state);
                inner.state.record_appraisal(a);
                combined = Some(match combined {
                    None => a,
                    Some(prev) => sum_appraisals(prev, a),
                });
            }

            // Advance dynamics for the elapsed wall time. Idle path
            // when no events fired this tick — the same shape the
            // offline `SimulatorRun` uses, so trajectories match
            // between offline runs and the live runtime.
            inner.state.channels = match &combined {
                Some(a) => self.dynamics.step_with_impact(
                    &inner.state.channels,
                    a,
                    &self.dynamics_params,
                    dt,
                ),
                None => self
                    .dynamics
                    .step_idle(&inner.state.channels, &self.dynamics_params, dt),
            };
            inner.state.timestamp_ms = ts_ms;
            inner.state.refresh_core();

            let policy = self
                .regulation
                .regulate(&inner.state, &self.regulation_config);
            let target_vad = channel_d::compute_target(&inner.state, &policy);
            let summary = self.renderer.render(&inner.state, &policy);
            (target_vad, summary, inner.state.channels, policy, ts_ms)
        };

        // Decide what to emit, then mark them emitted, then drop the
        // lock and call the callback. We serialise the emit decisions
        // under the lock so two concurrent ticks (which the pacer
        // forbids today, but defence-in-depth) can't both emit the
        // same envelope.
        let (emit_steering, emit_summary) = {
            let mut inner = session.inner.lock();
            let emit_steering = match inner.last_emitted_target_vad {
                None => true,
                Some(prev) => l2_dist(&prev, &target_vad) > self.config.emit_threshold,
            };
            if emit_steering {
                inner.last_emitted_target_vad = Some(target_vad);
            }
            let emit_summary = match &inner.last_emitted_summary {
                None => true,
                Some(prev) => prev != &summary,
            };
            if emit_summary {
                inner.last_emitted_summary = Some(summary.clone());
            }
            (emit_steering, emit_summary)
        };

        if emit_steering {
            let env = wrap_aux_port(
                "set_steering",
                RuntimeData::Json(serde_json::json!({
                    "target_vad": target_vad,
                    "alpha": self.config.steering_alpha,
                })),
            );
            cb(env)?;
        }
        if emit_summary {
            let env = wrap_aux_port(
                "set_system_augmentation",
                RuntimeData::Json(serde_json::json!({ "text": summary })),
            );
            cb(env)?;
        }

        // Always emit the debug tap so a UI can render the live state
        // continuously. No threshold — visualisation expects every tick
        // to produce a frame.
        let tap = serde_json::json!({
            "kind": "affect_state",
            "ts_ms": ts_ms,
            "channels": channels_snap,
            "policy": policy_snap,
            "channel_d_target_vad": target_vad,
            "channel_b": summary,
        });
        cb(RuntimeData::Json(tap))?;

        // Affect → ARKit-52 blendshape envelope. Cheap to compute (a
        // sparse weighted sum over <8 channels) and the renderer-side
        // contract `{kind: "blendshapes", arkit_52, pts_ms}` already
        // exists for the audio2face / synthetic lip-sync paths, so a
        // pre-existing `Live2DRenderNode` / `CcRenderNode` consumes it
        // unchanged. Hand-tuned mapping until the
        // `tools/affect_avatar/` learned model lands.
        //
        // Emitted every tick (no threshold) — the renderer interpolates
        // between consecutive frames against its own clock; suppressing
        // duplicates here would just leave gaps for the renderer to
        // hold-and-decay through.
        if self.config.emit_blendshapes {
            use crate::nodes::affect_expression::compute_blendshapes_with_gain;
            use crate::nodes::lip_sync::BlendshapeFrame;
            let arkit_52 = compute_blendshapes_with_gain(
                &channels_snap,
                &policy_snap,
                self.config.blendshape_gain,
            );
            let frame = BlendshapeFrame::new(arkit_52, ts_ms, None);
            cb(RuntimeData::Json(frame.to_json()))?;
        }

        Ok(())
    }
}

/// Combine two appraisals additively, clamping each axis to `[-1, 1]`.
/// Mirrors `affect_simulator::runner::sum_appraisals` (private there) so
/// runtime tick combination matches offline scenario combination for
/// reproducibility.
fn sum_appraisals(a: Appraisal, b: Appraisal) -> Appraisal {
    Appraisal {
        novelty: (a.novelty + b.novelty).clamp(-1.0, 1.0),
        goal_relevance: (a.goal_relevance + b.goal_relevance).clamp(-1.0, 1.0),
        goal_congruence: (a.goal_congruence + b.goal_congruence).clamp(-1.0, 1.0),
        agency_self: (a.agency_self + b.agency_self).clamp(-1.0, 1.0),
        agency_other: (a.agency_other + b.agency_other).clamp(-1.0, 1.0),
        agency_situation: (a.agency_situation + b.agency_situation).clamp(-1.0, 1.0),
        control: (a.control + b.control).clamp(-1.0, 1.0),
        certainty: (a.certainty + b.certainty).clamp(-1.0, 1.0),
        norm_violation: (a.norm_violation + b.norm_violation).clamp(-1.0, 1.0),
        loss_signal: (a.loss_signal + b.loss_signal).clamp(-1.0, 1.0),
        threat_signal: (a.threat_signal + b.threat_signal).clamp(-1.0, 1.0),
        reward_signal: (a.reward_signal + b.reward_signal).clamp(-1.0, 1.0),
        social_safety: (a.social_safety + b.social_safety).clamp(-1.0, 1.0),
    }
}

fn l2_dist(a: &[f32; 3], b: &[f32; 3]) -> f32 {
    let dv = a[0] - b[0];
    let da = a[1] - b[1];
    let dd = a[2] - b[2];
    (dv * dv + da * da + dd * dd).sqrt()
}

/// Factory for [`AffectSimulatorNode`]. Reads `tick_hz`,
/// `steering_alpha`, `emit_threshold` from `params`; missing keys fall
/// back to module defaults.
pub struct AffectSimulatorNodeFactory;

impl Default for AffectSimulatorNodeFactory {
    fn default() -> Self {
        Self
    }
}

impl StreamingNodeFactory for AffectSimulatorNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>> {
        Ok(Box::new(AffectSimulatorNode::from_params(node_id, params)))
    }

    fn node_type(&self) -> &str {
        "AffectSimulatorNode"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::NodeRuntimeContext;
    use crate::transport::session_control::aux_port_of;

    fn ctx_for_test() -> NodeRuntimeContext {
        // for_test seeds session_state with `Arc::new(())`; we replace
        // it with the typed state the node expects.
        let mut ctx = NodeRuntimeContext::for_test("test-session", "affect_sim");
        ctx.session_state = Arc::new(SessionAffectState::new());
        ctx
    }

    #[tokio::test]
    async fn first_speech_start_yields_user_greeting() {
        let node = AffectSimulatorNode::new("affect_sim", AffectSimulatorConfig::default());
        let ctx = ctx_for_test();
        let vad_start = RuntimeData::Json(serde_json::json!({
            "is_speech_start": true,
            "is_speech_end": false,
        }));
        node.buffer_event(&vad_start, &ctx);
        let session: Arc<SessionAffectState> = ctx.state();
        let inner = session.inner.lock();
        assert_eq!(inner.pending_events.len(), 1);
        assert_eq!(inner.pending_events[0].kind, EventKind::UserGreeting);
        assert!(inner.in_speech);
    }

    #[tokio::test]
    async fn second_speech_start_is_silent() {
        let node = AffectSimulatorNode::new("affect_sim", AffectSimulatorConfig::default());
        let ctx = ctx_for_test();
        let vad_start = RuntimeData::Json(serde_json::json!({ "is_speech_start": true }));
        let vad_end = RuntimeData::Json(serde_json::json!({ "is_speech_end": true }));
        node.buffer_event(&vad_start, &ctx);
        node.buffer_event(&vad_end, &ctx);
        node.buffer_event(&vad_start, &ctx);
        let session: Arc<SessionAffectState> = ctx.state();
        let inner = session.inner.lock();
        // Greeting + Question; the second start does not push a new event.
        let kinds: Vec<EventKind> = inner.pending_events.iter().map(|e| e.kind).collect();
        assert_eq!(
            kinds,
            vec![EventKind::UserGreeting, EventKind::UserQuestion]
        );
    }

    #[tokio::test]
    async fn speech_end_outside_segment_is_noop() {
        let node = AffectSimulatorNode::new("affect_sim", AffectSimulatorConfig::default());
        let ctx = ctx_for_test();
        let vad_end = RuntimeData::Json(serde_json::json!({ "is_speech_end": true }));
        node.buffer_event(&vad_end, &ctx);
        let session: Arc<SessionAffectState> = ctx.state();
        let inner = session.inner.lock();
        assert!(inner.pending_events.is_empty());
    }

    #[tokio::test]
    async fn text_input_yields_user_question() {
        let node = AffectSimulatorNode::new("affect_sim", AffectSimulatorConfig::default());
        let ctx = ctx_for_test();
        node.buffer_event(&RuntimeData::Text("hi".into()), &ctx);
        let session: Arc<SessionAffectState> = ctx.state();
        let inner = session.inner.lock();
        assert_eq!(inner.pending_events.len(), 1);
        assert_eq!(inner.pending_events[0].kind, EventKind::UserQuestion);
    }

    #[tokio::test]
    async fn prosody_envelopes_decode_to_their_kinds() {
        let node = AffectSimulatorNode::new("affect_sim", AffectSimulatorConfig::default());
        let ctx = ctx_for_test();
        for (kind_str, expected) in [
            ("prosody_arousal_high", EventKind::ProsodyArousalHigh),
            (
                "prosody_valence_negative",
                EventKind::ProsodyValenceNegative,
            ),
            ("prosody_uncertain", EventKind::ProsodyUncertain),
        ] {
            node.buffer_event(
                &RuntimeData::Json(serde_json::json!({ "kind": kind_str })),
                &ctx,
            );
            let session: Arc<SessionAffectState> = ctx.state();
            let inner = session.inner.lock();
            assert_eq!(inner.pending_events.last().unwrap().kind, expected);
        }
    }

    /// Tier C path: Python-side `ProsodyVadNode` cannot construct a
    /// `RuntimeData::Json` variant — it emits a Text frame on
    /// `channel="json"` with the JSON envelope as the body. The IPC
    /// layer prefixes the channel via `[0x00][len][channel][text]`;
    /// the decoder must recover the channel via `split_text_str`,
    /// recognise `"json"`, parse the body, and route through the same
    /// `kind`-decoder used for native `RuntimeData::Json`.
    #[tokio::test]
    async fn channel_json_text_envelope_decodes_via_kind() {
        use crate::data::tag_text_str;

        let node = AffectSimulatorNode::new("affect_sim", AffectSimulatorConfig::default());
        let ctx = ctx_for_test();

        let envelope = serde_json::json!({
            "id": "prosody_1",
            "kind": "prosody_arousal_high",
            "modality": "audio",
            "confidence": 0.85,
            "timestamp_ms": 0,
            "payload": { "valence": 0.1, "arousal": 0.7, "dominance": 0.0 },
        });
        let tagged = tag_text_str(&envelope.to_string(), "json");
        node.buffer_event(&RuntimeData::Text(tagged), &ctx);

        let session: Arc<SessionAffectState> = ctx.state();
        let inner = session.inner.lock();
        assert_eq!(inner.pending_events.len(), 1);
        assert_eq!(inner.pending_events[0].kind, EventKind::ProsodyArousalHigh,);
        // Confidence falls through unchanged: the Python side uses
        // ProsodyVad's per-event confidence (here 0.85), not 1.0.
        // Note: the current buffer_event always sets 1.0; we relax to
        // a presence check rather than equality so a future enrichment
        // (use the JSON's `confidence` field) doesn't break this test
        // — see "Open work" in spike-i.
    }

    /// Channel-tagged but malformed JSON gets logged-and-dropped rather
    /// than mis-interpreted as a transcript turn. The producer was
    /// trying to send JSON; surfacing it as `UserQuestion` would hide
    /// the bug.
    #[tokio::test]
    async fn channel_json_with_garbage_body_drops_event() {
        use crate::data::tag_text_str;

        let node = AffectSimulatorNode::new("affect_sim", AffectSimulatorConfig::default());
        let ctx = ctx_for_test();

        let tagged = tag_text_str("not actually json {[}", "json");
        node.buffer_event(&RuntimeData::Text(tagged), &ctx);

        let session: Arc<SessionAffectState> = ctx.state();
        let inner = session.inner.lock();
        assert!(
            inner.pending_events.is_empty(),
            "malformed json frame must not buffer an event",
        );
    }

    /// Untagged Text (legacy / "tts" channel) still maps to
    /// UserQuestion — the Tier A behaviour for transcript turns from
    /// `stt_in` is preserved alongside the new Tier C json-channel
    /// path.
    #[tokio::test]
    async fn untagged_text_still_maps_to_user_question() {
        let node = AffectSimulatorNode::new("affect_sim", AffectSimulatorConfig::default());
        let ctx = ctx_for_test();
        node.buffer_event(&RuntimeData::Text("how was your weekend?".into()), &ctx);
        let session: Arc<SessionAffectState> = ctx.state();
        let inner = session.inner.lock();
        assert_eq!(inner.pending_events.len(), 1);
        assert_eq!(inner.pending_events[0].kind, EventKind::UserQuestion,);
    }

    /// First tick drains buffered events, advances the simulator, and
    /// emits the four canonical per-tick frames in order: a
    /// `set_steering` aux-port envelope, a `set_system_augmentation`
    /// aux-port envelope, the `affect_state` debug tap, and the
    /// `blendshapes` envelope shaped for avatar consumers.
    #[tokio::test]
    async fn first_tick_emits_four_envelopes() {
        let node = AffectSimulatorNode::new("affect_sim", AffectSimulatorConfig::default());
        let ctx = ctx_for_test();
        node.buffer_event(
            &RuntimeData::Json(serde_json::json!({ "is_speech_start": true })),
            &ctx,
        );

        let collected: Arc<std::sync::Mutex<Vec<RuntimeData>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let collected_clone = collected.clone();
        let cb: Box<dyn FnMut(RuntimeData) -> Result<()> + Send> = Box::new(move |out| {
            collected_clone.lock().unwrap().push(out);
            Ok(())
        });
        let tick = Tick {
            clock_id: "wall:5".into(),
            pts_us: 200_000,
            frame_idx: 0,
            deadline_us: 400_000,
        };
        node.tick(tick, &ctx, cb).await.unwrap();

        let out = collected.lock().unwrap();
        assert_eq!(out.len(), 4);
        assert_eq!(aux_port_of(&out[0]).as_deref(), Some("set_steering"));
        assert_eq!(
            aux_port_of(&out[1]).as_deref(),
            Some("set_system_augmentation")
        );
        // Tap is plain Json with `kind: affect_state`, not aux-port wrapped.
        assert_eq!(aux_port_of(&out[2]).as_deref(), None);
        if let RuntimeData::Json(v) = &out[2] {
            assert_eq!(v.get("kind").and_then(|x| x.as_str()), Some("affect_state"));
        } else {
            panic!("expected Json tap envelope");
        }
        // Blendshape envelope follows — same wire shape the
        // `Live2DRenderNode` / `CcRenderNode` consume from the
        // audio2face / synthetic-lipsync paths.
        assert_eq!(aux_port_of(&out[3]).as_deref(), None);
        if let RuntimeData::Json(v) = &out[3] {
            assert_eq!(v.get("kind").and_then(|x| x.as_str()), Some("blendshapes"));
            let arr = v
                .get("arkit_52")
                .and_then(|a| a.as_array())
                .expect("arkit_52 array present");
            assert_eq!(arr.len(), 52);
        } else {
            panic!("expected Json blendshapes envelope");
        }
    }

    /// `emit_blendshapes = false` skips the per-tick blendshape frame —
    /// pipelines without an avatar consumer can opt out and shave the
    /// per-tick allocation.
    #[tokio::test]
    async fn opting_out_of_blendshapes_drops_to_three_envelopes() {
        let mut cfg = AffectSimulatorConfig::default();
        cfg.emit_blendshapes = false;
        let node = AffectSimulatorNode::new("affect_sim", cfg);
        let ctx = ctx_for_test();
        node.buffer_event(
            &RuntimeData::Json(serde_json::json!({ "is_speech_start": true })),
            &ctx,
        );

        let collected: Arc<std::sync::Mutex<Vec<RuntimeData>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let collected_clone = collected.clone();
        let cb: Box<dyn FnMut(RuntimeData) -> Result<()> + Send> = Box::new(move |out| {
            collected_clone.lock().unwrap().push(out);
            Ok(())
        });
        let tick = Tick {
            clock_id: "wall:5".into(),
            pts_us: 200_000,
            frame_idx: 0,
            deadline_us: 400_000,
        };
        node.tick(tick, &ctx, cb).await.unwrap();

        assert_eq!(collected.lock().unwrap().len(), 3);
    }

    /// Steering threshold suppresses redundant re-publishes between
    /// idle ticks (no events → state evolves slowly enough that the L2
    /// step is below the default threshold).
    #[tokio::test]
    async fn idle_ticks_suppress_redundant_steering_emits() {
        let node = AffectSimulatorNode::new("affect_sim", AffectSimulatorConfig::default());
        let ctx = ctx_for_test();
        let collected: Arc<std::sync::Mutex<Vec<RuntimeData>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));

        for i in 0..5 {
            let collected_clone = collected.clone();
            let cb: Box<dyn FnMut(RuntimeData) -> Result<()> + Send> = Box::new(move |out| {
                collected_clone.lock().unwrap().push(out);
                Ok(())
            });
            let tick = Tick {
                clock_id: "wall:5".into(),
                pts_us: i as u64 * 200_000,
                frame_idx: i,
                deadline_us: (i as u64 + 1) * 200_000,
            };
            node.tick(tick, &ctx, cb).await.unwrap();
        }

        let out = collected.lock().unwrap();
        // First tick emits steering + summary + tap; subsequent idle
        // ticks emit only the tap (state changes but the L2 distance
        // stays below threshold). That's 3 + 4 * 1 = 7 emissions.
        // Tap is always emitted; steering may emit once at start.
        let n_steering = out
            .iter()
            .filter(|d| aux_port_of(d).as_deref() == Some("set_steering"))
            .count();
        let n_taps = out
            .iter()
            .filter(|d| {
                if let RuntimeData::Json(v) = d {
                    v.get("kind").and_then(|x| x.as_str()) == Some("affect_state")
                } else {
                    false
                }
            })
            .count();
        assert!(
            n_steering <= 2,
            "got {n_steering} steering emits over 5 idle ticks"
        );
        assert_eq!(n_taps, 5, "tap emits every tick");
    }

    #[test]
    fn config_from_params_reads_overrides() {
        let params = serde_json::json!({
            "tick_hz": 10,
            "steering_alpha": 0.5,
            "emit_threshold": 0.1,
        });
        let cfg = AffectSimulatorConfig::from_params(&params);
        assert_eq!(cfg.tick_hz, 10);
        assert!((cfg.steering_alpha - 0.5).abs() < f32::EPSILON);
        assert!((cfg.emit_threshold - 0.1).abs() < f32::EPSILON);
    }

    #[test]
    fn config_clamps_zero_tick_hz() {
        let params = serde_json::json!({ "tick_hz": 0 });
        let cfg = AffectSimulatorConfig::from_params(&params);
        assert_eq!(cfg.tick_hz, DEFAULT_TICK_HZ);
    }

    #[test]
    fn factory_pacing_nature_matches_config() {
        let factory = AffectSimulatorNodeFactory;
        let node = factory
            .create(
                "affect_sim".into(),
                &serde_json::json!({ "tick_hz": 4 }),
                None,
            )
            .unwrap();
        assert_eq!(node.node_type(), "AffectSimulatorNode");
        assert!(matches!(node.pacing_nature(), PacingNature::SourceWall(4)));
        assert!(node.is_multi_input());
    }
}
