//! VAD → ARKit-52 blendshape RBF anchor lookup.
//!
//! Path B of the listener-face plan
//! ([`tools/affect_avatar/LISTENER_MODE_PLAN.md`](../../../../tools/affect_avatar/LISTENER_MODE_PLAN.md)
//! and [`tools/affect_avatar/INTEGRATION.md`](../../../../tools/affect_avatar/INTEGRATION.md)):
//! given a per-input `(V, A, D, intensity)` tuple, interpolate among
//! 21 emotion×intensity anchors derived from MEAD aggregates and emit
//! a `BlendshapeFrame`-shaped JSON envelope. Sub-millisecond inference,
//! zero audio path, no possible lip-sync. Companion to
//! [`WhisperToVadNode`](super::whisper_to_vad::WhisperToVadNode), which
//! produces the upstream V/A/D.
//!
//! ## Anchor format
//!
//! The node loads a JSON sidecar produced by
//! `tools/affect_avatar/scripts/10_build_vad_anchors.py` (its `.npz`
//! output is converted to JSON for tooling-free Rust loading). Schema:
//!
//! ```json
//! {
//!   "baseline": [54 floats],            // neutral L1 peak
//!   "anchors": {
//!     "happy_3":     [54 floats],       // emotion-delta, clipped [0, 1]
//!     "surprised_2": [54 floats],
//!     ...
//!   }
//! }
//! ```
//!
//! The 54 channels are MEAD_3D's MediaPipe-alphabetical order (without
//! `_neutral`); we remap to the canonical ARKit-52 used by every
//! avatar consumer in this workspace before emitting.
//!
//! ## RBF interpolation
//!
//! Same math as the Python prototype
//! ([`tools/affect_avatar/scripts/lib/vad_to_blendshape.py`](../../../../tools/affect_avatar/scripts/lib/vad_to_blendshape.py)):
//! Gaussian RBF with bandwidth `tau` over `(V, A, D, intensity_norm)`.
//! Listener-mode default `add_baseline=false` — emit the pure
//! emotion-delta over the renderer's neutral rest pose.

use crate::data::RuntimeData;
use crate::error::{Error, Result};
use crate::nodes::AsyncStreamingNode;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// Number of MEAD_3D blendshape channels in each anchor vector. The
/// dataset emits MediaPipe FaceLandmarker outputs with 3 trailing
/// pose-or-other extras at indices 51..54; we keep the full 54 so the
/// loader doesn't have to know about them.
const MEAD_54: usize = 54;

/// Canonical 8-emotion → V/A/D table.
///
/// Mirrors `MEAD_VAD` in
/// [`tools/affect_avatar/scripts/lib/vad_to_blendshape.py`](../../../../tools/affect_avatar/scripts/lib/vad_to_blendshape.py).
/// Same coordinates the DiT uses for emotion conditioning, so an
/// upstream V/A/D query that lands near these values yields the
/// expected anchor.
const MEAD_VAD: &[(&str, [f32; 3])] = &[
    ("neutral", [0.0, 0.0, 0.0]),
    ("happy", [0.80, 0.50, 0.40]),
    ("sad", [-0.70, -0.30, -0.40]),
    ("angry", [-0.50, 0.70, 0.50]),
    ("fear", [-0.60, 0.60, -0.50]),
    ("disgusted", [-0.60, 0.30, -0.20]),
    ("surprised", [0.30, 0.70, 0.0]),
    ("contempt", [-0.40, -0.10, 0.50]),
];

/// Per-ARKit-52-channel source index into the MEAD-54 anchor vector.
/// `-1` means "no source" — `tongueOut` (ARKit index 51) is the only
/// such channel; MediaPipe FaceLandmarker doesn't emit a tongue
/// activation, so we zero-fill it.
///
/// Derived once from the Python `mead3d_to_arkit.build_mead_to_arkit_map`
/// table; bake it as a const so the runtime never has to reconstruct
/// it. Order matches `crate::nodes::lip_sync::ARKIT_BLENDSHAPE_NAMES`.
const MEAD_TO_ARKIT_MAP: [i8; 52] = [
    8,  // 0  eyeBlinkLeft       ← MEAD 8
    10, // 1  eyeLookDownLeft    ← 10
    12, // 2  eyeLookInLeft      ← 12
    14, // 3  eyeLookOutLeft     ← 14
    16, // 4  eyeLookUpLeft      ← 16
    18, // 5  eyeSquintLeft      ← 18
    20, // 6  eyeWideLeft        ← 20
    9,  // 7  eyeBlinkRight      ← 9
    11, // 8  eyeLookDownRight   ← 11
    13, // 9  eyeLookInRight     ← 13
    15, // 10 eyeLookOutRight    ← 15
    17, // 11 eyeLookUpRight     ← 17
    19, // 12 eyeSquintRight     ← 19
    21, // 13 eyeWideRight       ← 21
    22, // 14 jawForward         ← 22
    23, // 15 jawLeft            ← 23
    25, // 16 jawRight           ← 25
    24, // 17 jawOpen            ← 24
    26, // 18 mouthClose         ← 26
    31, // 19 mouthFunnel        ← 31
    37, // 20 mouthPucker        ← 37
    32, // 21 mouthLeft          ← 32
    38, // 22 mouthRight         ← 38
    43, // 23 mouthSmileLeft     ← 43
    44, // 24 mouthSmileRight    ← 44
    29, // 25 mouthFrownLeft     ← 29
    30, // 26 mouthFrownRight    ← 30
    27, // 27 mouthDimpleLeft    ← 27
    28, // 28 mouthDimpleRight   ← 28
    45, // 29 mouthStretchLeft   ← 45
    46, // 30 mouthStretchRight  ← 46
    39, // 31 mouthRollLower     ← 39
    40, // 32 mouthRollUpper     ← 40
    41, // 33 mouthShrugLower    ← 41
    42, // 34 mouthShrugUpper    ← 42
    35, // 35 mouthPressLeft     ← 35
    36, // 36 mouthPressRight    ← 36
    33, // 37 mouthLowerDownLeft  ← 33
    34, // 38 mouthLowerDownRight ← 34
    47, // 39 mouthUpperUpLeft   ← 47
    48, // 40 mouthUpperUpRight  ← 48
    0,  // 41 browDownLeft       ← 0
    1,  // 42 browDownRight      ← 1
    2,  // 43 browInnerUp        ← 2
    3,  // 44 browOuterUpLeft    ← 3
    4,  // 45 browOuterUpRight   ← 4
    5,  // 46 cheekPuff          ← 5
    6,  // 47 cheekSquintLeft    ← 6
    7,  // 48 cheekSquintRight   ← 7
    49, // 49 noseSneerLeft      ← 49
    50, // 50 noseSneerRight     ← 50
    -1, // 51 tongueOut          (no MediaPipe source)
];

/// One loaded anchor: emotion name + numeric intensity (1–3) +
/// 4-D query coordinate `(V, A, D, intensity_norm)` + the 54-channel
/// delta. Stacked into [`AnchorSet`] for inference.
struct Anchor {
    coord: [f32; 4],
    delta: [f32; MEAD_54],
}

struct AnchorSet {
    anchors: Vec<Anchor>,
    #[allow(dead_code)]
    baseline: [f32; MEAD_54],
}

impl AnchorSet {
    fn load(path: &std::path::Path) -> Result<Self> {
        #[derive(Deserialize)]
        struct Raw {
            baseline: Vec<f32>,
            anchors: HashMap<String, Vec<f32>>,
        }

        let bytes = std::fs::read(path).map_err(|e| {
            Error::Execution(format!(
                "VadToFaceNode: failed to read anchors at {}: {e}",
                path.display()
            ))
        })?;
        let raw: Raw = serde_json::from_slice(&bytes).map_err(|e| {
            Error::Execution(format!(
                "VadToFaceNode: anchor JSON at {} is malformed: {e}",
                path.display()
            ))
        })?;
        if raw.baseline.len() != MEAD_54 {
            return Err(Error::Execution(format!(
                "VadToFaceNode: baseline has {} channels, expected {MEAD_54}",
                raw.baseline.len()
            )));
        }
        let mut baseline = [0.0_f32; MEAD_54];
        baseline.copy_from_slice(&raw.baseline);

        // Build a quick name → V/A/D lookup so we can drop anchor
        // entries with unknown emotion labels with a clear log line
        // rather than silently mis-coordinating them.
        let vad_table: HashMap<&str, [f32; 3]> = MEAD_VAD.iter().map(|(n, v)| (*n, *v)).collect();

        let mut anchors = Vec::with_capacity(raw.anchors.len());
        for (key, delta_vec) in raw.anchors.iter() {
            // Skip neutral_1 in the anchor pool — it's the baseline,
            // and including it as an anchor at the origin would
            // dominate near-rest queries.
            let (emo, lvl) = match key.rsplit_once('_') {
                Some((emo, lvl_str)) => match lvl_str.parse::<u32>() {
                    Ok(n) => (emo.to_string(), n),
                    Err(_) => {
                        tracing::warn!(
                            anchor = %key,
                            "VadToFaceNode: anchor key has non-numeric intensity suffix; skipping"
                        );
                        continue;
                    }
                },
                None => {
                    tracing::warn!(
                        anchor = %key,
                        "VadToFaceNode: anchor key missing `_<intensity>` suffix; skipping"
                    );
                    continue;
                }
            };
            if emo == "neutral" {
                continue;
            }
            let vad = match vad_table.get(emo.as_str()) {
                Some(v) => *v,
                None => {
                    tracing::warn!(
                        anchor = %key,
                        "VadToFaceNode: anchor emotion not in MEAD_VAD table; skipping"
                    );
                    continue;
                }
            };
            if delta_vec.len() != MEAD_54 {
                return Err(Error::Execution(format!(
                    "VadToFaceNode: anchor `{key}` has {} channels, expected {MEAD_54}",
                    delta_vec.len()
                )));
            }
            let mut delta = [0.0_f32; MEAD_54];
            delta.copy_from_slice(delta_vec);
            let intensity_norm = (lvl as f32) / 3.0;
            anchors.push(Anchor {
                coord: [vad[0], vad[1], vad[2], intensity_norm],
                delta,
            });
        }
        if anchors.is_empty() {
            return Err(Error::Execution(format!(
                "VadToFaceNode: no usable anchors loaded from {}",
                path.display()
            )));
        }
        tracing::info!(anchors = anchors.len(), "VadToFaceNode: loaded anchor set");
        Ok(Self { anchors, baseline })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct VadToFaceConfig {
    /// Path to the anchors JSON sidecar. Use
    /// `tools/affect_avatar/scripts/10_build_vad_anchors.py` to (re)generate.
    #[serde(alias = "anchorsPath")]
    pub anchors_path: PathBuf,

    /// Gaussian RBF bandwidth in 4-D `(V, A, D, intensity_norm)`
    /// space. Smaller = sharper transitions (more single-anchor at a
    /// time); larger = smoother blends. 0.25 chosen so a query near
    /// one anchor gets ~80% of its weight from that anchor.
    #[schemars(range(min = 0.001, max = 5.0))]
    pub tau: f32,

    /// Anchors below this normalized intensity get their delta
    /// scaled down. Stops L1 anchors from looking indistinguishable
    /// from rest.
    #[serde(alias = "intensityFloor")]
    #[schemars(range(min = 0.0, max = 1.0))]
    pub intensity_floor: f32,

    /// When true, emitted blendshape is `baseline + emotion_delta`.
    /// When false (default for listener mode), pure delta — no
    /// baseline pucker / squint bleeding through.
    #[serde(alias = "addBaseline")]
    pub add_baseline: bool,

    /// Soft clamp the per-channel emotion delta. Useful for tuning
    /// down listener-mode expressiveness without retraining anchors.
    #[schemars(range(min = 0.0, max = 1.0))]
    pub max_weight: f32,
}

impl Default for VadToFaceConfig {
    fn default() -> Self {
        Self {
            anchors_path: PathBuf::from("tools/affect_avatar/artifacts/vad_anchors_delta.json"),
            tau: 0.25,
            intensity_floor: 0.20,
            add_baseline: false,
            max_weight: 1.0,
        }
    }
}

pub struct VadToFaceNode {
    config: VadToFaceConfig,
    anchors: OnceCell<Arc<AnchorSet>>,
}

impl VadToFaceNode {
    pub fn with_config(config: VadToFaceConfig) -> Self {
        Self {
            config,
            anchors: OnceCell::new(),
        }
    }

    async fn get_or_load_anchors(&self) -> Result<&Arc<AnchorSet>> {
        self.anchors
            .get_or_try_init(|| async {
                let set = AnchorSet::load(&self.config.anchors_path)?;
                Ok(Arc::new(set))
            })
            .await
    }

    fn rbf_blend(&self, set: &AnchorSet, query: [f32; 4]) -> [f32; MEAD_54] {
        // Pairwise squared distance to every anchor.
        let mut weights = vec![0.0_f32; set.anchors.len()];
        let two_tau_sq = 2.0 * self.config.tau * self.config.tau;
        let mut sum_w = 0.0_f32;
        for (i, a) in set.anchors.iter().enumerate() {
            let mut d2 = 0.0_f32;
            for k in 0..4 {
                let dv = query[k] - a.coord[k];
                d2 += dv * dv;
            }
            let w = (-d2 / two_tau_sq).exp();
            weights[i] = w;
            sum_w += w;
        }
        let inv = if sum_w > 1e-8 { 1.0 / sum_w } else { 0.0 };

        let mut out = [0.0_f32; MEAD_54];
        for (i, a) in set.anchors.iter().enumerate() {
            let w = weights[i] * inv;
            for c in 0..MEAD_54 {
                out[c] += w * a.delta[c];
            }
        }

        // Intensity floor: low-intensity queries get a scaled-down
        // delta so the L1 anchors don't end up identical to neutral.
        let scale = query[3].max(self.config.intensity_floor);
        for x in out.iter_mut() {
            *x *= scale;
        }

        if self.config.add_baseline {
            for c in 0..MEAD_54 {
                out[c] += set.baseline[c];
            }
        }

        let cap = self.config.max_weight;
        for x in out.iter_mut() {
            if *x < 0.0 {
                *x = 0.0;
            } else if *x > cap {
                *x = cap;
            }
        }
        out
    }

    fn mead54_to_arkit52(mead: &[f32; MEAD_54]) -> [f32; 52] {
        let mut out = [0.0_f32; 52];
        for (i, &src) in MEAD_TO_ARKIT_MAP.iter().enumerate() {
            if src >= 0 {
                let idx = src as usize;
                if idx < MEAD_54 {
                    out[i] = mead[idx];
                }
            }
        }
        out
    }

    fn parse_vad_input(data: &RuntimeData) -> Result<Option<[f32; 4]>> {
        match data {
            RuntimeData::Json(v) => {
                let valence = v.get("valence").and_then(|x| x.as_f64());
                let arousal = v.get("arousal").and_then(|x| x.as_f64());
                let dominance = v.get("dominance").and_then(|x| x.as_f64());
                let intensity = v.get("intensity").and_then(|x| x.as_f64()).unwrap_or(0.4);
                match (valence, arousal, dominance) {
                    (Some(va), Some(ar), Some(do_)) => {
                        Ok(Some([va as f32, ar as f32, do_ as f32, intensity as f32]))
                    }
                    _ => Ok(None),
                }
            }
            RuntimeData::Tensor {
                data, shape, dtype, ..
            } => {
                if *dtype != 0 {
                    return Ok(None);
                }
                let total: usize = shape.iter().map(|d| *d as usize).product();
                if total != 3 && total != 4 {
                    return Ok(None);
                }
                if data.len() != total * 4 {
                    return Ok(None);
                }
                let vals: Vec<f32> = data
                    .chunks_exact(4)
                    .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                    .collect();
                let intensity = if total == 4 { vals[3] } else { 0.4 };
                Ok(Some([vals[0], vals[1], vals[2], intensity]))
            }
            _ => Ok(None),
        }
    }
}

#[async_trait]
impl AsyncStreamingNode for VadToFaceNode {
    fn node_type(&self) -> &str {
        "VadToFaceNode"
    }

    async fn process(&self, _data: RuntimeData) -> Result<RuntimeData> {
        Err(Error::Execution(
            "VadToFaceNode is streaming-only — use process_streaming()".into(),
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
        let query = match Self::parse_vad_input(&data)? {
            Some(q) => q,
            None => {
                // Pass non-VAD envelopes through so a misrouted
                // upstream chunk doesn't get silently swallowed.
                callback(data)?;
                return Ok(1);
            }
        };

        let set = self.get_or_load_anchors().await?.clone();
        let mead = self.rbf_blend(&set, query);
        let arkit = Self::mead54_to_arkit52(&mead);

        // Carry through pts_ms / turn_id from upstream metadata if present.
        let (pts_ms, turn_id) = match &data {
            RuntimeData::Json(v) => (
                v.get("pts_ms").and_then(|x| x.as_u64()),
                v.get("turn_id").and_then(|x| x.as_u64()),
            ),
            _ => (None, None),
        };

        let arkit_json: Vec<serde_json::Value> = arkit
            .iter()
            .map(|x| serde_json::json!((x * 1_000.0).round() / 1_000.0))
            .collect();

        let mut out = serde_json::json!({
            "kind": "blendshapes",
            "arkit_52": arkit_json,
            "source": "vad_to_face",
            "vad": [query[0], query[1], query[2]],
            "intensity": query[3],
        });
        if let Some(p) = pts_ms {
            out["pts_ms"] = serde_json::json!(p);
        }
        if let Some(t) = turn_id {
            out["turn_id"] = serde_json::json!(t);
        }
        callback(RuntimeData::Json(out))?;
        Ok(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Canonical ARKit-52 names. Mirrored inline (rather than imported
    /// from `crate::nodes::lip_sync`) so this test compiles when the
    /// `avatar-lipsync` feature isn't enabled. The
    /// `arkit_names_match_canonical_when_lipsync_on` test below pins
    /// the two against each other when the feature *is* enabled, so a
    /// rename on either side gets caught.
    const ARKIT_NAMES: [&str; 52] = [
        "eyeBlinkLeft",
        "eyeLookDownLeft",
        "eyeLookInLeft",
        "eyeLookOutLeft",
        "eyeLookUpLeft",
        "eyeSquintLeft",
        "eyeWideLeft",
        "eyeBlinkRight",
        "eyeLookDownRight",
        "eyeLookInRight",
        "eyeLookOutRight",
        "eyeLookUpRight",
        "eyeSquintRight",
        "eyeWideRight",
        "jawForward",
        "jawLeft",
        "jawRight",
        "jawOpen",
        "mouthClose",
        "mouthFunnel",
        "mouthPucker",
        "mouthLeft",
        "mouthRight",
        "mouthSmileLeft",
        "mouthSmileRight",
        "mouthFrownLeft",
        "mouthFrownRight",
        "mouthDimpleLeft",
        "mouthDimpleRight",
        "mouthStretchLeft",
        "mouthStretchRight",
        "mouthRollLower",
        "mouthRollUpper",
        "mouthShrugLower",
        "mouthShrugUpper",
        "mouthPressLeft",
        "mouthPressRight",
        "mouthLowerDownLeft",
        "mouthLowerDownRight",
        "mouthUpperUpLeft",
        "mouthUpperUpRight",
        "browDownLeft",
        "browDownRight",
        "browInnerUp",
        "browOuterUpLeft",
        "browOuterUpRight",
        "cheekPuff",
        "cheekSquintLeft",
        "cheekSquintRight",
        "noseSneerLeft",
        "noseSneerRight",
        "tongueOut",
    ];

    #[cfg(feature = "avatar-lipsync")]
    #[test]
    fn arkit_names_match_canonical_when_lipsync_on() {
        use crate::nodes::lip_sync::ARKIT_BLENDSHAPE_NAMES;
        for (i, n) in ARKIT_NAMES.iter().enumerate() {
            assert_eq!(
                *n, ARKIT_BLENDSHAPE_NAMES[i],
                "ARKit name drift at index {i}: local={n} canonical={}",
                ARKIT_BLENDSHAPE_NAMES[i]
            );
        }
    }

    /// Sanity-check the static MEAD→ARKit map against the canonical
    /// MediaPipe alphabetical names. A future reorder of either side
    /// can't silently mis-route weights.
    #[test]
    fn mead_to_arkit_map_round_trips_names() {
        // MediaPipe alphabetical order, sans `_neutral`. Mirrors
        // `tools/affect_avatar/scripts/lib/mead3d_to_arkit.py::MEDIAPIPE_ORDER`.
        const MEDIAPIPE_ORDER: &[&str] = &[
            "browDownLeft",
            "browDownRight",
            "browInnerUp",
            "browOuterUpLeft",
            "browOuterUpRight",
            "cheekPuff",
            "cheekSquintLeft",
            "cheekSquintRight",
            "eyeBlinkLeft",
            "eyeBlinkRight",
            "eyeLookDownLeft",
            "eyeLookDownRight",
            "eyeLookInLeft",
            "eyeLookInRight",
            "eyeLookOutLeft",
            "eyeLookOutRight",
            "eyeLookUpLeft",
            "eyeLookUpRight",
            "eyeSquintLeft",
            "eyeSquintRight",
            "eyeWideLeft",
            "eyeWideRight",
            "jawForward",
            "jawLeft",
            "jawOpen",
            "jawRight",
            "mouthClose",
            "mouthDimpleLeft",
            "mouthDimpleRight",
            "mouthFrownLeft",
            "mouthFrownRight",
            "mouthFunnel",
            "mouthLeft",
            "mouthLowerDownLeft",
            "mouthLowerDownRight",
            "mouthPressLeft",
            "mouthPressRight",
            "mouthPucker",
            "mouthRight",
            "mouthRollLower",
            "mouthRollUpper",
            "mouthShrugLower",
            "mouthShrugUpper",
            "mouthSmileLeft",
            "mouthSmileRight",
            "mouthStretchLeft",
            "mouthStretchRight",
            "mouthUpperUpLeft",
            "mouthUpperUpRight",
            "noseSneerLeft",
            "noseSneerRight",
        ];
        assert_eq!(MEDIAPIPE_ORDER.len(), 51);
        for (arkit_idx, arkit_name) in ARKIT_NAMES.iter().enumerate() {
            let expected_src: i8 = MEDIAPIPE_ORDER
                .iter()
                .position(|n| n == arkit_name)
                .map(|p| p as i8)
                .unwrap_or(-1);
            assert_eq!(
                MEAD_TO_ARKIT_MAP[arkit_idx], expected_src,
                "ARKit[{}]={} expected MEAD source {}, got {}",
                arkit_idx, arkit_name, expected_src, MEAD_TO_ARKIT_MAP[arkit_idx]
            );
        }
    }

    /// Loads the committed `vad_anchors_delta.json` and verifies the
    /// happy@(0.8,0.5,0.4,1.0) query produces a non-trivial smile.
    /// Skipped when the artifact isn't on disk so the test still
    /// passes in trimmed CI checkouts.
    #[tokio::test]
    async fn loads_real_anchors_and_blends_happy() {
        let path =
            std::path::PathBuf::from("../../tools/affect_avatar/artifacts/vad_anchors_delta.json");
        if !path.exists() {
            eprintln!("anchors not present at {} — skipping", path.display());
            return;
        }
        let node = VadToFaceNode::with_config(VadToFaceConfig {
            anchors_path: path,
            ..Default::default()
        });
        let set = node.get_or_load_anchors().await.expect("load anchors");
        let mead = node.rbf_blend(&set, [0.80, 0.50, 0.40, 1.0]);
        let arkit = VadToFaceNode::mead54_to_arkit52(&mead);
        // ARKit-52: 23 = mouthSmileLeft, 24 = mouthSmileRight.
        assert!(
            arkit[23] > 0.05 && arkit[24] > 0.05,
            "happy query should activate mouthSmile{{Left,Right}}; got L={} R={}",
            arkit[23],
            arkit[24]
        );
    }

    #[test]
    fn rbf_near_anchor_returns_anchor_delta() {
        // Hand-build a 2-anchor set with disjoint deltas; querying
        // exactly at one anchor's coord should produce ~that anchor's
        // delta (modulo intensity floor scaling and tau leakage from
        // the other anchor).
        let mut a = [0.0_f32; MEAD_54];
        a[24] = 1.0; // jawOpen-ish on MEAD side
        let mut b = [0.0_f32; MEAD_54];
        b[2] = 1.0; // browInnerUp on MEAD side
        let set = AnchorSet {
            baseline: [0.0; MEAD_54],
            anchors: vec![
                Anchor {
                    coord: [0.8, 0.5, 0.4, 1.0],
                    delta: a,
                },
                Anchor {
                    coord: [-0.7, -0.3, -0.4, 1.0],
                    delta: b,
                },
            ],
        };

        let node = VadToFaceNode::with_config(VadToFaceConfig {
            tau: 0.25,
            intensity_floor: 0.0,
            add_baseline: false,
            max_weight: 1.0,
            anchors_path: PathBuf::new(),
        });
        let out = node.rbf_blend(&set, [0.8, 0.5, 0.4, 1.0]);
        // jawOpen channel should dominate.
        assert!(
            out[24] > 0.9,
            "expected jawOpen-ish near 1.0, got {}",
            out[24]
        );
        assert!(
            out[2] < 0.1,
            "expected browInnerUp near 0.0, got {}",
            out[2]
        );
    }
}
