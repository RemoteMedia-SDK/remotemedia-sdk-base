//! ActivationFaceNode — drive the avatar face from LLM hidden-state
//! projections.
//!
//! ## Pipeline summary
//!
//! ```text
//!   input_hidden  ──┐
//!                   ├─► ActivationProjector::project_normalised
//!   response_hidden ┘                       │
//!                                           ▼
//!                       (label → score) per side
//!                                           │
//!                                           ▼
//!                            blend(α·input + (1-α)·response)
//!                                           │
//!                                           ▼
//!                                   sigmoid → per-label weight
//!                                           │
//!                                           ▼
//!                       label-to-morph routing table
//!                       (built at init by intersecting NPZ labels
//!                        with this avatar's morph-target catalog)
//!                                           │
//!                                           ▼
//!                       ARKit map (resolved JSON) inverts
//!                       morph names → ARKit-52 indices
//!                                           │
//!                                           ▼
//!                            BlendshapeFrame { arkit_52, pts_ms }
//! ```
//!
//! ## Scope of this initial cut
//!
//! - Stateless `process(input_hidden, response_hidden, pts_ms)` — the
//!   wrapping streaming node lives in the example wire-up
//!   (`hermes3_affect_s2s_webrtc_server`) so this module stays test
//!   focused on the routing math.
//! - Static `morph_targets: Vec<String>` at construction. Runtime
//!   discovery from a loaded GLB is a follow-up (proposal §"Out of
//!   scope").
//! - Reuses the existing `BlendshapeFrame` envelope. Per-morph weights
//!   that route to a given ARKit index get max-folded so consumers
//!   see a single coherent pose.
//!
//! See `openspec/changes/add-activation-projection-face/` for the full
//! design.

#![cfg(feature = "activation-face")]

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::Deserialize;
use thiserror::Error;

use crate::data::RuntimeData;
use crate::error::Error;
use crate::nodes::activation_projection::{ActivationProjector, ActivationProjectorError};
use crate::nodes::lip_sync::{BlendshapeFrame, ARKIT_52, ARKIT_BLENDSHAPE_NAMES};
use crate::nodes::{
    AnySessionState, InitializeContext, InitializeContextRead, NodeRuntimeContext,
    NodeRuntimeContextRead, StreamingNode, StreamingNodeFactory,
};

/// Default blend factor: α=0.3 means the response side dominates.
/// Aligns with the proposal's "blend weighted toward response" decision.
pub const DEFAULT_BLEND_ALPHA: f32 = 0.3;

/// One entry of the resolved ARKit map JSON file
/// (`avatars/<name>.arkit_map.resolved.json`). Mirrors the
/// `pub(crate)` `MorphRef` type in
/// `crates/core/src/nodes/cc_render/bevy_app/assets.rs` — we keep our
/// own copy so this module stays decoupled from the cc_render feature
/// surface.
#[derive(Debug, Deserialize, Clone)]
struct ResolvedMorphRef {
    morph: String,
    /// Coefficient under the resolved JSON's authoring conventions.
    /// Multiplies the per-morph weight before maximum-folding into
    /// the ARKit-52 array.
    #[serde(default = "one")]
    weight: f32,
    #[allow(dead_code)]
    #[serde(default)]
    meshes: Vec<String>,
}

fn one() -> f32 {
    1.0
}

#[derive(Debug, Deserialize)]
struct ResolvedArkitMap {
    mapping: HashMap<String, Vec<ResolvedMorphRef>>,
}

/// Errors surfaced by [`ActivationFaceNode`] construction.
#[derive(Debug, Error)]
pub enum ActivationFaceError {
    #[error("io error reading {1}: {0}")]
    Io(#[source] std::io::Error, String),

    #[error("ARKit map JSON parse error in {1}: {0}")]
    ArkitMapParse(#[source] serde_json::Error, String),

    #[error(transparent)]
    Projector(#[from] ActivationProjectorError),

    #[error(
        "no direction labels matched any morph target — face will be \
         frozen; check that the NPZ labels and the avatar's morph names \
         have at least one substring or synonym overlap"
    )]
    NoLabelsMatched,
}

/// Static synonym map. Each entry says "if a direction label is
/// `from`, also accept `to` as a substring match against morph
/// names." Bidirectional — the matcher tries both directions.
/// Calibration vocabularies and CC4/CC5 morph naming conventions
/// don't always agree (e.g. NPZ says `"joy"`, CC5 ships `Mood_Happy`),
/// so a tiny synonym table closes the most common gaps without
/// requiring per-NPZ wiring.
const SYNONYMS: &[(&str, &str)] = &[
    ("joy", "happy"),
    ("happiness", "happy"),
    ("sadness", "sad"),
    ("anger", "angry"),
    ("fear", "afraid"),
    ("surprise", "surprised"),
    ("disgust", "disgusted"),
    ("frustration", "frustrated"),
    // Channel D V/A/D names map naturally to FACS-style mood
    // descriptors. These cover the case where the runtime NPZ is
    // valence/arousal/dominance but the avatar exposes mood-preset
    // morphs.
    ("valence", "happy"),
    ("arousal", "surprised"),
    ("dominance", "confident"),
];

/// True iff `morph_name` (lowercased) contains any normalised form of
/// `label` (label itself + every synonym).
fn matches_label(label: &str, morph_name: &str) -> bool {
    let label_lc = label.to_lowercase();
    let morph_lc = morph_name.to_lowercase();
    if morph_lc.contains(&label_lc) {
        return true;
    }
    for (a, b) in SYNONYMS {
        // For each synonym pair, if the label is `a` we also accept
        // `b`'s substring presence in the morph name (and vice versa).
        let a_lc = a.to_lowercase();
        let b_lc = b.to_lowercase();
        if label_lc == a_lc && morph_lc.contains(&b_lc) {
            return true;
        }
        if label_lc == b_lc && morph_lc.contains(&a_lc) {
            return true;
        }
    }
    false
}

#[inline]
fn sigmoid(x: f32) -> f32 {
    if x >= 0.0 {
        1.0 / (1.0 + (-x).exp())
    } else {
        let e = x.exp();
        e / (1.0 + e)
    }
}

/// Per-direction-label fan-out: which ARKit-52 indices does this
/// label drive? Each entry pairs an ARKit index with the resolved
/// map's per-morph weight (so a morph authored at 0.7 contributes
/// proportionally less than one authored at 1.0).
#[derive(Debug, Clone)]
struct ArkitFanout {
    arkit_index: usize,
    /// Morph-side coefficient from the resolved ARKit map JSON.
    morph_weight: f32,
}

/// Stateless face-projection node. Construct once with the loaded
/// projector + ARKit map + the avatar's morph-target catalog, then
/// call [`process`] each time you have a fresh tap to render.
///
/// Cheap to clone: all interior state is small enough that cloning
/// per-session is fine, but in practice one node serves all sessions.
#[derive(Debug)]
pub struct ActivationFaceNode {
    projector: ActivationProjector,
    /// Direction label → list of ARKit-52 fan-outs. Built at init by
    /// intersecting the NPZ's labels with the supplied morph-target
    /// catalog and the resolved ARKit map. Labels with no matching
    /// morph are absent from this map (silently no-op at emit time).
    routing: HashMap<String, Vec<ArkitFanout>>,
    /// Blend factor — α weighted toward the **input** side. Default
    /// 0.3, so the response side dominates.
    blend_alpha: f32,
}

impl ActivationFaceNode {
    /// Build a node from a calibrated projector, the path to a
    /// resolved ARKit map JSON, and the avatar's morph-target catalog.
    /// Logs the resolved routing table at `INFO`.
    pub fn new(
        projector: ActivationProjector,
        arkit_map_path: impl AsRef<Path>,
        morph_targets: &[String],
        blend_alpha: f32,
    ) -> Result<Self, ActivationFaceError> {
        let arkit_path = arkit_map_path.as_ref();
        let arkit_text = std::fs::read_to_string(arkit_path)
            .map_err(|e| ActivationFaceError::Io(e, arkit_path.display().to_string()))?;
        let arkit_map: ResolvedArkitMap = serde_json::from_str(&arkit_text)
            .map_err(|e| ActivationFaceError::ArkitMapParse(e, arkit_path.display().to_string()))?;
        Self::from_loaded_map(projector, &arkit_map, morph_targets, blend_alpha)
    }

    /// Build from an already-loaded ARKit map. Exposed for tests so
    /// they don't need a tempfile per case.
    fn from_loaded_map(
        projector: ActivationProjector,
        arkit_map: &ResolvedArkitMap,
        morph_targets: &[String],
        blend_alpha: f32,
    ) -> Result<Self, ActivationFaceError> {
        let routing = build_routing(&projector, arkit_map, morph_targets);

        // Diagnostic log (Spec: Operator Diagnostic at Initialization).
        // Emit one INFO line per label with the matched morphs and a
        // single trailing line listing unmatched labels. Operators who
        // see "0 labels routed" know the NPZ + morph catalog are
        // misaligned without reading the code.
        let mut total_arkit = 0usize;
        let mut matched = Vec::new();
        let mut unmatched = Vec::new();
        for axis in &projector.axes {
            match routing.get(&axis.label) {
                Some(entries) if !entries.is_empty() => {
                    total_arkit += entries.len();
                    let names: Vec<String> = entries
                        .iter()
                        .map(|fo| {
                            ARKIT_BLENDSHAPE_NAMES
                                .get(fo.arkit_index)
                                .map(|s| s.to_string())
                                .unwrap_or_else(|| format!("idx{}", fo.arkit_index))
                        })
                        .collect();
                    matched.push(format!("{} -> {}", axis.label, names.join(", ")));
                }
                _ => unmatched.push(axis.label.clone()),
            }
        }

        tracing::info!(
            n_labels = projector.axes.len(),
            n_matched = matched.len(),
            n_unmatched = unmatched.len(),
            n_arkit_targets = total_arkit,
            "ActivationFaceNode routing table built",
        );
        for line in &matched {
            tracing::info!(routing = %line, "  matched");
        }
        if !unmatched.is_empty() {
            tracing::info!(
                unmatched = %unmatched.join(", "),
                "  unmatched (NPZ has these directions but no avatar morph matches by substring or synonym)",
            );
        }

        if matched.is_empty() {
            return Err(ActivationFaceError::NoLabelsMatched);
        }

        Ok(Self {
            projector,
            routing,
            blend_alpha: blend_alpha.clamp(0.0, 1.0),
        })
    }

    /// Project two hidden states (one from the user-input forward
    /// pass, one from the response generation tap) and emit a
    /// [`BlendshapeFrame`] driving the matched morphs.
    ///
    /// Pass `None` for `input_hidden` when no input-side capture is
    /// available yet (e.g. very first response chunk before the user
    /// finishes speaking). The blend then falls back to
    /// response-only mode for that frame.
    pub fn process(
        &self,
        input_hidden: Option<&[f32]>,
        response_hidden: Option<&[f32]>,
        pts_ms: u64,
    ) -> Result<BlendshapeFrame, ActivationProjectorError> {
        let input_scores = match input_hidden {
            Some(h) => Some(self.projector.project_normalised(h)?),
            None => None,
        };
        let response_scores = match response_hidden {
            Some(h) => Some(self.projector.project_normalised(h)?),
            None => None,
        };

        // Build a label → blended-score map. Any label without a
        // contribution defaults to 0.0.
        let mut blended: HashMap<&str, f32> = HashMap::new();
        for axis in &self.projector.axes {
            let i = input_scores
                .as_ref()
                .and_then(|m| m.get(&axis.label))
                .copied()
                .unwrap_or(0.0);
            let r = response_scores
                .as_ref()
                .and_then(|m| m.get(&axis.label))
                .copied()
                .unwrap_or(0.0);
            // α weighted toward input. When only response_hidden is
            // present, treat the input side as 0.0 so the response
            // signal carries through scaled by (1-α). When only
            // input_hidden is present, mirror that.
            let score = match (input_scores.is_some(), response_scores.is_some()) {
                (true, true) => self.blend_alpha * i + (1.0 - self.blend_alpha) * r,
                (true, false) => i,
                (false, true) => r,
                (false, false) => 0.0,
            };
            blended.insert(axis.label.as_str(), score);
        }

        // Distribute sigmoid(score) into the ARKit-52 array via the
        // routing table. Multiple labels may route to the same ARKit
        // index — keep the maximum, never sum (that would double-count
        // a single semantic and saturate too easily).
        let mut arkit_52 = [0.0f32; ARKIT_52];
        for (label, fanouts) in &self.routing {
            let score = blended.get(label.as_str()).copied().unwrap_or(0.0);
            let weight = sigmoid(score);
            for fo in fanouts {
                let scaled = (weight * fo.morph_weight).clamp(0.0, 1.0);
                if scaled > arkit_52[fo.arkit_index] {
                    arkit_52[fo.arkit_index] = scaled;
                }
            }
        }

        Ok(BlendshapeFrame::new(arkit_52, pts_ms, None))
    }

    /// Read-only access to the resolved routing table — used by the
    /// example wire-up to print a friendly summary at startup, and by
    /// the unit tests to assert routing-construction correctness.
    pub fn routing_summary(&self) -> Vec<(String, Vec<&'static str>)> {
        let mut out = Vec::new();
        for axis in &self.projector.axes {
            let names: Vec<&'static str> = self
                .routing
                .get(&axis.label)
                .map(|entries| {
                    entries
                        .iter()
                        .filter_map(|fo| ARKIT_BLENDSHAPE_NAMES.get(fo.arkit_index).copied())
                        .collect()
                })
                .unwrap_or_default();
            out.push((axis.label.clone(), names));
        }
        out
    }

    /// Direct read of the configured blend alpha, useful for tests and
    /// for the example startup log.
    pub fn blend_alpha(&self) -> f32 {
        self.blend_alpha
    }
}

/// Walk the avatar's morph catalog × NPZ labels × the resolved ARKit
/// map and produce the `(label → [arkit_index, weight])` routing.
fn build_routing(
    projector: &ActivationProjector,
    arkit_map: &ResolvedArkitMap,
    morph_targets: &[String],
) -> HashMap<String, Vec<ArkitFanout>> {
    let morph_set: std::collections::HashSet<&str> =
        morph_targets.iter().map(String::as_str).collect();

    let mut routing: HashMap<String, Vec<ArkitFanout>> = HashMap::new();

    // For every ARKit name (52 of them), the resolved map says which
    // CC4/CC5 morphs it drives. We need the inverse routing: given a
    // direction label that matches a morph name, which ARKit indices
    // does it light up?
    for (arkit_idx, arkit_name) in ARKIT_BLENDSHAPE_NAMES.iter().enumerate() {
        let Some(refs) = arkit_map.mapping.get(*arkit_name) else {
            continue;
        };
        for entry in refs {
            // Skip morphs the avatar doesn't have. This is the silent
            // no-op gate for missing morphs (e.g. cap_morphs.py
            // dropped them).
            if !morph_set.contains(entry.morph.as_str()) {
                continue;
            }
            // For each NPZ label that matches this morph, record the
            // ARKit index in its fan-out list.
            for axis in &projector.axes {
                if !matches_label(&axis.label, &entry.morph) {
                    continue;
                }
                let fanout = ArkitFanout {
                    arkit_index: arkit_idx,
                    morph_weight: entry.weight,
                };
                let entries = routing.entry(axis.label.clone()).or_default();
                // Avoid duplicate (label, arkit_idx) pairs — multiple
                // matched morphs at the same ARKit index would
                // otherwise inflate the count without changing the
                // emitted weight (we max-fold downstream anyway).
                if !entries.iter().any(|fo| fo.arkit_index == arkit_idx) {
                    entries.push(fanout);
                }
            }
        }
    }

    routing
}

// ─── Streaming-node wrapper ───────────────────────────────────────────────
//
// `ActivationFaceNode` is a stateless function-style processor; the
// wrapper below adapts it to the `StreamingNode` trait so it's
// addressable from a pipeline manifest. The wrapper:
//   1. Receives `RuntimeData::Tensor` envelopes whose
//      `metadata.kind == "activation_tap"` (emitted upstream by either
//      the Rust llama-cpp tap on `LlamaCppGenerationNode` or the
//      Python MLX tap on `QwenTextMlxNode`).
//   2. Maintains per-(node, session) state: the most recent
//      `phase=input` hidden state. Subsequent `phase=response` taps
//      blend with that input.
//   3. Emits a canonical `BlendshapeFrame` JSON envelope per response
//      tap (and per input tap, with response treated as `None` — so
//      the avatar reacts to the user's prompt before the model has
//      generated anything).
//
// Non-tap envelopes (e.g. text chunks the same upstream emits) flow
// through as opaque pass-throughs would normally — but in practice
// the manifest connects only the tap-out port to this node, so other
// envelopes don't reach it.

/// Per-(node, session) blend state. Holds the most recent input-phase
/// hidden vector so each response-phase tap blends with it. Resets
/// implicitly when a new input-phase tap arrives.
#[derive(Debug, Default)]
pub struct SessionActivationFaceState {
    inner: Mutex<SessionActivationFaceInner>,
}

#[derive(Debug, Default)]
struct SessionActivationFaceInner {
    /// Most recent `phase=input` hidden state for this session, or
    /// `None` before the first input tap.
    last_input_hidden: Option<Vec<f32>>,
    /// Sequential turn counter, lazily incremented when an `input`
    /// tap arrives. Surfaced as `BlendshapeFrame.turn_id` so the
    /// renderer can group frames per turn for diagnostics.
    turn_id: u64,
}

impl SessionActivationFaceState {
    pub fn new() -> Self {
        Self::default()
    }
}

/// `ActivationFaceNode` adapted to the `StreamingNode` trait so it's
/// addressable from a manifest as `node_type: "ActivationFaceNode"`.
pub struct ActivationFaceStreamingNode {
    node_id: String,
    inner: Arc<ActivationFaceNode>,
}

impl ActivationFaceStreamingNode {
    pub fn new(node_id: impl Into<String>, inner: ActivationFaceNode) -> Self {
        Self {
            node_id: node_id.into(),
            inner: Arc::new(inner),
        }
    }

    /// Inspect the wrapped routing summary — handy in the example
    /// startup log.
    pub fn routing_summary(&self) -> Vec<(String, Vec<&'static str>)> {
        self.inner.routing_summary()
    }
}

#[async_trait::async_trait]
impl StreamingNode for ActivationFaceStreamingNode {
    fn node_type(&self) -> &str {
        "ActivationFaceNode"
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn is_multi_input(&self) -> bool {
        false
    }

    fn make_session_state(&self, _ctx: &dyn InitializeContextRead) -> Arc<dyn AnySessionState> {
        Arc::new(SessionActivationFaceState::new())
    }

    async fn process_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        // Use the inline-result helper so the unary path doesn't need
        // a `Box<FnMut>`-shaped closure that captures a mutable
        // local. Callers in the unary mode are rare (the streaming
        // path is the manifest connection), but we keep the surface
        // for trait completeness.
        match self.process_inline(data, ctx)? {
            Some(out) => Ok(out),
            None => Err(Error::Execution(
                "ActivationFaceNode produced no output (input was not an activation_tap envelope)"
                    .into(),
            )),
        }
    }

    async fn process_multi_async(
        &self,
        inputs: HashMap<String, RuntimeData>,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        if let Some((_, data)) = inputs.into_iter().next() {
            self.process_async(data, ctx).await
        } else {
            Err(Error::Execution("ActivationFaceNode: no input data".into()))
        }
    }

    async fn process_streaming_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<usize, Error> {
        match self.process_inline(data, ctx)? {
            Some(out) => {
                callback(out)?;
                Ok(1)
            }
            None => Ok(0),
        }
    }
}

impl ActivationFaceStreamingNode {
    /// Shared, callback-free implementation. Returns `Some(envelope)`
    /// for valid activation-tap inputs, `None` for non-tap envelopes
    /// (silent no-op so a misrouted manifest doesn't crash the
    /// pipeline). Both `process_async` and `process_streaming_async`
    /// route through this so the projection / blend / emit logic is
    /// authored once.
    fn process_inline(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<Option<RuntimeData>, Error> {
        let (data_bytes, shape, _dtype, metadata) = match data {
            RuntimeData::Tensor {
                data,
                shape,
                dtype,
                metadata,
            } => (data, shape, dtype, metadata),
            other => {
                // Not a tap envelope — silently no-op rather than
                // erroring. Manifest authors who route the wrong
                // port here learn quickly via "the face never moves";
                // a hard error would crash the pipeline on every
                // unrelated upstream chunk.
                tracing::trace!(
                    node = %self.node_id,
                    kind = other.data_type(),
                    "ActivationFaceStreamingNode: ignoring non-Tensor input",
                );
                return Ok(None);
            }
        };

        let extras = match &metadata {
            Some(v) => v,
            None => {
                tracing::trace!(
                    node = %self.node_id,
                    "ActivationFaceStreamingNode: tensor without extras — skipping"
                );
                return Ok(None);
            }
        };
        let kind = extras.get("kind").and_then(|k| k.as_str()).unwrap_or("");
        if kind != "activation_tap" {
            tracing::trace!(
                node = %self.node_id,
                kind = %kind,
                "ActivationFaceStreamingNode: ignoring non-activation-tap tensor",
            );
            return Ok(None);
        }
        let phase = extras
            .get("phase")
            .and_then(|p| p.as_str())
            .unwrap_or("response");

        // Parse the f32 hidden state out of the byte payload. Shape's
        // last dim is the hidden width; we accept 1-D or 2-D
        // (taking the last row of a 2-D for chat-style tensors).
        let hidden = decode_hidden(&data_bytes, &shape).ok_or_else(|| {
            Error::Execution(format!(
                "ActivationFaceNode: bad tensor shape/dtype (shape={:?}, bytes={})",
                shape,
                data_bytes.len()
            ))
        })?;

        let session_state =
            remotemedia_traits::runtime_context::state::<SessionActivationFaceState>(ctx);

        let pts_ms = extras
            .get("turn_offset_ms")
            .and_then(|n| n.as_u64())
            .unwrap_or(0);

        // Decide what to project on this tap. Input taps cache and
        // emit response=None; response taps blend against the cached
        // input. Both produce one BlendshapeFrame per call.
        let (input_for_proc, response_for_proc, turn_id) = {
            let mut inner = session_state.inner.lock();
            match phase {
                "input" => {
                    inner.last_input_hidden = Some(hidden.clone());
                    inner.turn_id = inner.turn_id.saturating_add(1);
                    (Some(hidden.clone()), None, inner.turn_id)
                }
                _ => {
                    let cached = inner.last_input_hidden.clone();
                    (cached, Some(hidden.clone()), inner.turn_id)
                }
            }
        };

        let mut frame = self
            .inner
            .process(
                input_for_proc.as_deref(),
                response_for_proc.as_deref(),
                pts_ms,
            )
            .map_err(|e| Error::Execution(format!("ActivationFaceNode projection: {e}")))?;
        frame.turn_id = Some(turn_id);

        Ok(Some(RuntimeData::Json(frame.to_json())))
    }
}

// ─── Factory + registry hook ──────────────────────────────────────────────

/// Factory for [`ActivationFaceStreamingNode`].
///
/// Manifest schema (under `params`):
///
/// ```json
/// {
///   "node_type": "ActivationFaceNode",
///   "params": {
///     "npz_path":         "tools/affect_calibration/artifacts/llm_directions/qwen3.5-9b/layer15.npz",
///     "labels":           ["valence", "arousal", "dominance"],
///     "arkit_map_path":   "avatars/assistant.arkit_map.resolved.json",
///     "morph_targets":    ["Mood_Happy", "Mood_Sad", "Mood_Surprised"],
///     "blend_alpha":      0.3
///   }
/// }
/// ```
///
/// `labels` defaults to [`crate::nodes::activation_projection::DEFAULT_VAD_LABELS`]
/// when omitted, matching the calibration script's `("valence",
/// "arousal", "dominance")` ordering.
pub struct ActivationFaceNodeFactory;

impl Default for ActivationFaceNodeFactory {
    fn default() -> Self {
        Self
    }
}

impl StreamingNodeFactory for ActivationFaceNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &serde_json::Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        use crate::nodes::activation_projection::{ActivationProjector, DEFAULT_VAD_LABELS};

        let npz_path = params
            .get("npz_path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Error::Execution("ActivationFaceNode: missing required `npz_path` in params".into())
            })?;

        // Labels: optional, fall through to V/A/D defaults.
        let owned_labels: Vec<String> = match params.get("labels") {
            Some(serde_json::Value::Array(items)) => items
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
            _ => DEFAULT_VAD_LABELS
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
        };
        let label_refs: Vec<&str> = owned_labels.iter().map(|s| s.as_str()).collect();

        let projector = ActivationProjector::load(npz_path, &label_refs).map_err(|e| {
            Error::Execution(format!(
                "ActivationFaceNode: load projector at {npz_path}: {e}"
            ))
        })?;

        let arkit_map_path = params
            .get("arkit_map_path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                Error::Execution(
                    "ActivationFaceNode: missing required `arkit_map_path` in params".into(),
                )
            })?;

        let morph_targets: Vec<String> = match params.get("morph_targets") {
            Some(serde_json::Value::Array(items)) => items
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
            _ => Vec::new(),
        };
        if morph_targets.is_empty() {
            return Err(Error::Execution(
                "ActivationFaceNode: `morph_targets` must be a non-empty \
                 array — supply your avatar's GLB morph-target catalog \
                 (runtime discovery is a follow-up; see proposal §Out of scope)"
                    .into(),
            ));
        }

        let blend_alpha = params
            .get("blend_alpha")
            .and_then(|v| v.as_f64())
            .map(|f| f as f32)
            .unwrap_or(DEFAULT_BLEND_ALPHA);

        let face = ActivationFaceNode::new(projector, arkit_map_path, &morph_targets, blend_alpha)
            .map_err(|e| Error::Execution(format!("ActivationFaceNode: construction: {e}")))?;

        Ok(Box::new(ActivationFaceStreamingNode::new(node_id, face)))
    }

    fn node_type(&self) -> &str {
        "ActivationFaceNode"
    }
}

/// Decode a row-major f32 byte payload into a `Vec<f32>` matching the
/// last dim of `shape`. Returns `None` on malformed input. Tolerant
/// of 1-D and 2-D shapes: 2-D is treated as a chat-style tensor
/// and the last row is returned (mirroring what the Rust llama-cpp
/// tap and Python MLX tap both ship).
fn decode_hidden(bytes: &[u8], shape: &[i32]) -> Option<Vec<f32>> {
    if shape.is_empty() || bytes.is_empty() {
        return None;
    }
    let n_embd = *shape.last()? as usize;
    if n_embd == 0 {
        return None;
    }
    if bytes.len() % 4 != 0 {
        return None;
    }
    let total_floats = bytes.len() / 4;
    if total_floats < n_embd {
        return None;
    }
    // Take the LAST n_embd floats (matches "last token of last batch"
    // semantics on both producer sides).
    let start = (total_floats - n_embd) * 4;
    let mut out = Vec::with_capacity(n_embd);
    for chunk in bytes[start..].chunks_exact(4) {
        out.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::activation_projection::NpyMatrix;
    use std::path::PathBuf;

    /// Minimal projector with two unit-aligned directions. Used by the
    /// routing/blend tests so we can rely on `project([1,0,...]) ==
    /// {label0: 1, label1: 0}`.
    fn synthetic_projector(labels: &[&str]) -> ActivationProjector {
        // n_axes × 4-dim. Axis i is the i-th basis vector.
        let n = labels.len();
        let mut data = vec![0.0f32; n * 4];
        for i in 0..n {
            data[i * 4 + i] = 1.0;
        }
        ActivationProjector::from_matrix(
            NpyMatrix {
                data,
                shape: vec![n, 4],
            },
            4,
            21,
            labels,
            PathBuf::from("synthetic.npz"),
        )
        .expect("from_matrix")
    }

    fn synthetic_arkit_map(entries: &[(&str, &[(&str, f32)])]) -> ResolvedArkitMap {
        let mut mapping = HashMap::new();
        for (arkit_name, morphs) in entries {
            let refs: Vec<ResolvedMorphRef> = morphs
                .iter()
                .map(|(morph, weight)| ResolvedMorphRef {
                    morph: morph.to_string(),
                    weight: *weight,
                    meshes: Vec::new(),
                })
                .collect();
            mapping.insert(arkit_name.to_string(), refs);
        }
        ResolvedArkitMap { mapping }
    }

    #[test]
    fn routing_substring_match_basic() {
        // NPZ has label "happy" — should match "Mood_Happy" by direct
        // substring.
        let proj = synthetic_projector(&["happy"]);
        let map = synthetic_arkit_map(&[(
            "mouthSmileLeft",
            &[("Mood_Happy", 1.0), ("Other_Morph", 1.0)],
        )]);
        let morphs: Vec<String> = vec!["Mood_Happy".to_string(), "Other_Morph".to_string()];
        let node =
            ActivationFaceNode::from_loaded_map(proj, &map, &morphs, 0.3).expect("construct");
        let summary = node.routing_summary();
        assert_eq!(summary.len(), 1);
        assert_eq!(summary[0].0, "happy");
        assert_eq!(summary[0].1, vec!["mouthSmileLeft"]);
    }

    #[test]
    fn routing_synonym_joy_matches_happy() {
        // NPZ says "joy" but morph is named "Mood_Happy" — synonym map
        // should bridge them.
        let proj = synthetic_projector(&["joy"]);
        let map = synthetic_arkit_map(&[("mouthSmileLeft", &[("Mood_Happy", 1.0)])]);
        let morphs = vec!["Mood_Happy".to_string()];
        let node =
            ActivationFaceNode::from_loaded_map(proj, &map, &morphs, 0.3).expect("construct");
        let summary = node.routing_summary();
        assert_eq!(summary[0].1, vec!["mouthSmileLeft"]);
    }

    #[test]
    fn routing_unmatched_label_silently_skips() {
        // NPZ has "happy" + "frustrated"; avatar only has Mood_Happy.
        // frustrated should be silent. (We need at least one matched
        // label for construction to succeed.)
        let proj = synthetic_projector(&["happy", "frustrated"]);
        let map = synthetic_arkit_map(&[("mouthSmileLeft", &[("Mood_Happy", 1.0)])]);
        let morphs = vec!["Mood_Happy".to_string()];
        let node =
            ActivationFaceNode::from_loaded_map(proj, &map, &morphs, 0.3).expect("construct");
        let summary = node.routing_summary();
        let happy = summary.iter().find(|(l, _)| l == "happy").unwrap();
        let frustrated = summary.iter().find(|(l, _)| l == "frustrated").unwrap();
        assert!(!happy.1.is_empty());
        assert!(frustrated.1.is_empty());
    }

    #[test]
    fn routing_morph_not_on_avatar_silently_skips() {
        // ARKit map entry references Mouth_Lips_Press_L but the avatar
        // only ships Mood_Happy. The press-lookup should fall through.
        let proj = synthetic_projector(&["happy", "angry"]);
        let map = synthetic_arkit_map(&[
            ("mouthSmileLeft", &[("Mood_Happy", 1.0)]),
            ("mouthPressLeft", &[("Mouth_Lips_Press_L", 1.0)]),
        ]);
        let morphs = vec!["Mood_Happy".to_string()];
        let node =
            ActivationFaceNode::from_loaded_map(proj, &map, &morphs, 0.3).expect("construct");
        let summary = node.routing_summary();
        // angry routes to Mouth_Lips_Press_L conceptually but the
        // morph isn't on this avatar — silent.
        let angry = summary.iter().find(|(l, _)| l == "angry").unwrap();
        assert!(angry.1.is_empty());
    }

    #[test]
    fn no_labels_matched_returns_error() {
        let proj = synthetic_projector(&["aurora", "borealis"]);
        let map = synthetic_arkit_map(&[("mouthSmileLeft", &[("Mood_Happy", 1.0)])]);
        let morphs = vec!["Mood_Happy".to_string()];
        let err =
            ActivationFaceNode::from_loaded_map(proj, &map, &morphs, 0.3).expect_err("should fail");
        assert!(matches!(err, ActivationFaceError::NoLabelsMatched));
    }

    #[test]
    fn process_emits_blendshapes_for_matched_label() {
        let proj = synthetic_projector(&["happy"]);
        let map = synthetic_arkit_map(&[("mouthSmileLeft", &[("Mood_Happy", 1.0)])]);
        let morphs = vec!["Mood_Happy".to_string()];
        let node =
            ActivationFaceNode::from_loaded_map(proj, &map, &morphs, 0.3).expect("construct");
        // Hidden state aligned to direction 0 ("happy").
        let hidden = vec![10.0_f32, 0.0, 0.0, 0.0];
        let frame = node.process(None, Some(&hidden), 1234).expect("process");
        assert_eq!(frame.pts_ms, 1234);
        // mouthSmileLeft idx is 23 in the canonical names. We don't
        // hardcode the index — just confirm exactly one ARKit slot is
        // non-zero and corresponds to mouthSmileLeft.
        let nonzero: Vec<(usize, f32)> = frame
            .arkit_52
            .iter()
            .enumerate()
            .filter(|(_, w)| **w > 1e-6)
            .map(|(i, w)| (i, *w))
            .collect();
        assert_eq!(nonzero.len(), 1);
        let smile_idx = ARKIT_BLENDSHAPE_NAMES
            .iter()
            .position(|n| *n == "mouthSmileLeft")
            .unwrap();
        assert_eq!(nonzero[0].0, smile_idx);
        // sigmoid(positive large value) ≈ 1.0; clamped weight is in
        // (0.5, 1.0] for any positive input.
        assert!(nonzero[0].1 > 0.5);
    }

    #[test]
    fn blend_alpha_one_uses_input_only() {
        let proj = synthetic_projector(&["happy"]);
        let map = synthetic_arkit_map(&[("mouthSmileLeft", &[("Mood_Happy", 1.0)])]);
        let morphs = vec!["Mood_Happy".to_string()];
        // α=1.0 → input dominates.
        let node =
            ActivationFaceNode::from_loaded_map(proj, &map, &morphs, 1.0).expect("construct");
        // Input pulls toward happy strongly; response pulls negative
        // strongly. With α=1.0 the response should be ignored.
        let happy_input = vec![10.0_f32, 0.0, 0.0, 0.0];
        let sad_response = vec![-10.0_f32, 0.0, 0.0, 0.0];
        let frame = node
            .process(Some(&happy_input), Some(&sad_response), 0)
            .expect("process");
        let smile_idx = ARKIT_BLENDSHAPE_NAMES
            .iter()
            .position(|n| *n == "mouthSmileLeft")
            .unwrap();
        // With α=1 we read input only → positive → sigmoid > 0.5.
        assert!(frame.arkit_52[smile_idx] > 0.5);
    }

    #[test]
    fn blend_alpha_zero_uses_response_only() {
        let proj = synthetic_projector(&["happy"]);
        let map = synthetic_arkit_map(&[("mouthSmileLeft", &[("Mood_Happy", 1.0)])]);
        let morphs = vec!["Mood_Happy".to_string()];
        // α=0.0 → response dominates.
        let node =
            ActivationFaceNode::from_loaded_map(proj, &map, &morphs, 0.0).expect("construct");
        let happy_input = vec![10.0_f32, 0.0, 0.0, 0.0];
        let sad_response = vec![-10.0_f32, 0.0, 0.0, 0.0];
        let frame = node
            .process(Some(&happy_input), Some(&sad_response), 0)
            .expect("process");
        let smile_idx = ARKIT_BLENDSHAPE_NAMES
            .iter()
            .position(|n| *n == "mouthSmileLeft")
            .unwrap();
        // α=0 → response only → negative → sigmoid < 0.5.
        assert!(frame.arkit_52[smile_idx] < 0.5);
    }

    #[test]
    fn missing_input_falls_through_to_response_only() {
        let proj = synthetic_projector(&["happy"]);
        let map = synthetic_arkit_map(&[("mouthSmileLeft", &[("Mood_Happy", 1.0)])]);
        let morphs = vec!["Mood_Happy".to_string()];
        let node =
            ActivationFaceNode::from_loaded_map(proj, &map, &morphs, 0.3).expect("construct");
        let response = vec![10.0_f32, 0.0, 0.0, 0.0];
        let frame_no_input = node.process(None, Some(&response), 0).unwrap();
        let smile_idx = ARKIT_BLENDSHAPE_NAMES
            .iter()
            .position(|n| *n == "mouthSmileLeft")
            .unwrap();
        // Falls through to response-only — positive input → sigmoid > 0.5.
        assert!(frame_no_input.arkit_52[smile_idx] > 0.5);
    }

    #[test]
    fn morph_weight_scales_arkit_output() {
        // Same direction but the resolved JSON authors the morph at
        // 0.5. The emitted ARKit weight should be sigmoid(score) × 0.5
        // (clamped to [0,1]).
        let proj = synthetic_projector(&["happy"]);
        let map = synthetic_arkit_map(&[("mouthSmileLeft", &[("Mood_Happy", 0.5)])]);
        let morphs = vec!["Mood_Happy".to_string()];
        let node =
            ActivationFaceNode::from_loaded_map(proj, &map, &morphs, 0.3).expect("construct");
        let hidden = vec![10.0_f32, 0.0, 0.0, 0.0];
        let frame = node.process(None, Some(&hidden), 0).unwrap();
        let smile_idx = ARKIT_BLENDSHAPE_NAMES
            .iter()
            .position(|n| *n == "mouthSmileLeft")
            .unwrap();
        // Pre-clamp the weight is sigmoid(big) ≈ 1.0; * 0.5 = 0.5.
        // Allow some slack for sigmoid not being exactly 1.0.
        let w = frame.arkit_52[smile_idx];
        assert!(w > 0.4 && w <= 0.5 + 1e-3, "weight = {w}");
    }

    #[test]
    fn multi_morph_max_fold_at_same_arkit_index() {
        // Two NPZ labels both route to mouthSmileLeft via the synonym
        // map — the resulting weight at that index should be the max,
        // not the sum.
        let proj = synthetic_projector(&["happy", "joy"]);
        let map = synthetic_arkit_map(&[("mouthSmileLeft", &[("Mood_Happy", 1.0)])]);
        let morphs = vec!["Mood_Happy".to_string()];
        let node =
            ActivationFaceNode::from_loaded_map(proj, &map, &morphs, 0.3).expect("construct");
        // Hidden aligned to the FIRST direction ("happy") only.
        let hidden = vec![10.0_f32, 0.0, 0.0, 0.0];
        let frame = node.process(None, Some(&hidden), 0).unwrap();
        let smile_idx = ARKIT_BLENDSHAPE_NAMES
            .iter()
            .position(|n| *n == "mouthSmileLeft")
            .unwrap();
        // Sigmoid(positive) for "happy" is large; "joy" projects to 0
        // (orthogonal direction), sigmoid(0)=0.5. Max is the "happy"
        // contribution (≈1.0).
        assert!(frame.arkit_52[smile_idx] > 0.6);
    }

    #[test]
    fn matches_label_substring_and_synonym() {
        // Direct substring: "happy" ⊂ "Mood_Happy".
        assert!(matches_label("happy", "Mood_Happy"));
        // Synonym: joy ↔ happy.
        assert!(matches_label("joy", "Mood_Happy"));
        // No relationship.
        assert!(!matches_label("happy", "Brow_Down_L"));
        // Case-insensitive.
        assert!(matches_label("HAPPY", "mood_happy"));
    }

    // ─── Streaming-node wrapper tests ────────────────────────────────

    fn build_streaming_node() -> ActivationFaceStreamingNode {
        let proj = synthetic_projector(&["happy"]);
        let map = synthetic_arkit_map(&[("mouthSmileLeft", &[("Mood_Happy", 1.0)])]);
        let morphs = vec!["Mood_Happy".to_string()];
        let face = ActivationFaceNode::from_loaded_map(proj, &map, &morphs, 0.3).expect("face");
        ActivationFaceStreamingNode::new("test_face", face)
    }

    /// Build a `RuntimeData::Tensor` envelope tagged like the Rust
    /// llama-cpp / Python MLX activation tap would.
    fn tap_envelope(hidden: &[f32], phase: &str, token_index: u32) -> crate::data::RuntimeData {
        let bytes: Vec<u8> = hidden.iter().flat_map(|f| f.to_le_bytes()).collect();
        crate::data::RuntimeData::Tensor {
            data: bytes,
            shape: vec![hidden.len() as i32],
            dtype: 1,
            metadata: Some(serde_json::json!({
                "kind": "activation_tap",
                "layer": 15,
                "phase": phase,
                "token_index": token_index,
                "turn_offset_ms": 0,
            })),
        }
    }

    fn ctx_for_streaming() -> NodeRuntimeContext {
        let mut ctx = NodeRuntimeContext::for_test("test-session", "test_face");
        ctx.session_state = Arc::new(SessionActivationFaceState::new());
        ctx
    }

    #[tokio::test]
    async fn streaming_input_tap_caches_and_emits_frame() {
        let node = build_streaming_node();
        let ctx = ctx_for_streaming();
        let env = tap_envelope(&[10.0, 0.0, 0.0, 0.0], "input", 0);
        let out = node.process_async(env, &ctx).await.expect("process");
        match out {
            crate::data::RuntimeData::Json(v) => {
                assert_eq!(v.get("kind").and_then(|k| k.as_str()), Some("blendshapes"));
                assert!(v.get("arkit_52").and_then(|a| a.as_array()).is_some());
                assert_eq!(v.get("turn_id").and_then(|t| t.as_u64()), Some(1));
            }
            other => panic!("unexpected variant: {:?}", other.data_type()),
        }
        // The cached input is now in session state.
        let st: Arc<SessionActivationFaceState> = ctx.state();
        let inner = st.inner.lock();
        assert!(inner.last_input_hidden.is_some());
        assert_eq!(inner.turn_id, 1);
    }

    #[tokio::test]
    async fn streaming_response_tap_blends_with_cached_input() {
        let node = build_streaming_node();
        let ctx = ctx_for_streaming();
        // Input phase: positive valence → smile high.
        let _ = node
            .process_async(tap_envelope(&[10.0, 0.0, 0.0, 0.0], "input", 0), &ctx)
            .await
            .unwrap();

        // Response phase: opposite (negative valence). Blend with α=0.3
        // means the output is 0.3·input + 0.7·response → net negative
        // → smile sigmoid below 0.5.
        let response_out = node
            .process_async(tap_envelope(&[-10.0, 0.0, 0.0, 0.0], "response", 32), &ctx)
            .await
            .unwrap();
        if let crate::data::RuntimeData::Json(v) = response_out {
            let arkit = v
                .get("arkit_52")
                .and_then(|a| a.as_array())
                .expect("arkit_52 array");
            let smile_idx = ARKIT_BLENDSHAPE_NAMES
                .iter()
                .position(|n| *n == "mouthSmileLeft")
                .unwrap();
            let smile = arkit[smile_idx].as_f64().unwrap() as f32;
            // 0.3·(+sigmoid_clamp_to_+1.5) + 0.7·(−clamp_to_−1.5) is
            // net negative — sigmoid below 0.5.
            assert!(smile < 0.5, "smile after blend = {smile}");
        } else {
            panic!("expected Json envelope from response phase");
        }
        // Same turn id as the input tap that opened it.
        let st: Arc<SessionActivationFaceState> = ctx.state();
        assert_eq!(st.inner.lock().turn_id, 1);
    }

    #[tokio::test]
    async fn streaming_non_tap_envelopes_are_silent_no_op() {
        let node = build_streaming_node();
        let ctx = ctx_for_streaming();
        // Plain Text envelope — should produce no output via the
        // streaming path (returns 0).
        let count = {
            let mut emitted = 0;
            let cb: Box<dyn FnMut(crate::data::RuntimeData) -> Result<(), Error> + Send> =
                Box::new(move |_d| {
                    emitted += 1;
                    Ok(())
                });
            node.process_streaming_async(crate::data::RuntimeData::Text("hello".into()), &ctx, cb)
                .await
                .unwrap()
        };
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn streaming_response_without_prior_input_falls_through_to_response_only() {
        // Edge case: the very first response tap of a session (no
        // input cached yet). The wrapper should still emit a frame —
        // `ActivationFaceNode::process` accepts `input=None` and
        // falls through to response-only mode.
        let node = build_streaming_node();
        let ctx = ctx_for_streaming();
        let out = node
            .process_async(tap_envelope(&[10.0, 0.0, 0.0, 0.0], "response", 32), &ctx)
            .await
            .unwrap();
        match out {
            crate::data::RuntimeData::Json(v) => {
                // turn_id is whatever's in session state (still 0 — no
                // input ever bumped it). The frame is still emitted.
                assert_eq!(v.get("turn_id").and_then(|t| t.as_u64()), Some(0));
            }
            other => panic!("unexpected variant: {:?}", other.data_type()),
        }
    }
}
