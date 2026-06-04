//! Hand-tuned affect → ARKit-52 blendshape mapping.
//!
//! Companion to [`affect_sim`](super::affect_sim) — translates one
//! `affect_simulator::AffectState` + `RegulationPolicy` snapshot into a
//! [`BlendshapeFrame`](crate::nodes::lip_sync::blendshape::BlendshapeFrame)-shaped
//! 52-element ARKit weight vector that any avatar consumer
//! (`Live2DRenderNode`, `CcRenderNode`, the Audio2Face stream) can render.
//!
//! ## Why hand-tuned for now
//!
//! A learned audio→blendshape model is the eventual right answer
//! (the `tools/affect_avatar/` blendshape-DiT track is exactly that), but
//! the first usable demo of the affect chain doesn't need it. The 8
//! emotion channels the simulator tracks (`anger`, `sadness`, `fear`,
//! `joy`, `calm`, `frustration`, `curiosity`, `empathy`) plus the
//! regulator's `expressiveness` / `safety_dampening` knobs map cleanly
//! to a small number of ARKit blendshapes via standard FACS shorthand:
//!
//! - **joy** → `mouthSmile{Left,Right}` + `cheekSquint{Left,Right}`
//!   (the Duchenne marker — eyes-corner crinkle is what distinguishes a
//!   genuine smile from a polite one)
//! - **sadness** → `mouthFrown{Left,Right}` + `browInnerUp` (the
//!   classic distress brow shape)
//! - **anger** → `browDown{Left,Right}` + `mouthPress{Left,Right}` +
//!   `noseSneer{Left,Right}` (lowered brow, set jaw, sneer)
//! - **fear** → `browInnerUp` + `eyeWide{Left,Right}` +
//!   `mouthStretch{Left,Right}` + slight `jawOpen`
//! - **curiosity** → `browOuterUp{Left,Right}` + `browInnerUp` +
//!   slight `eyeWide` (alert lift)
//! - **empathy** → `browInnerUp` + small symmetric `mouthSmile`
//!   (compassionate listening)
//! - **frustration** → muted **anger** (the mild form: tighter mouth,
//!   slight brow furrow, no sneer)
//! - **calm** → no contributions; calm is the *absence* of
//!   activation rather than its own facial signature.
//!
//! The contribution coefficients live in [`CHANNEL_COEFFICIENTS`].
//! Each row maps one emotion channel onto a small subset of the 52
//! blendshapes; per-frame we evaluate
//! `weight[i] = clamp01(Σ_channels(channel_value × coefficient[c][i])
//!              × expressiveness × gain)`.
//!
//! ## Regulator authority
//!
//! Two regulator knobs gate the raw channel sum:
//!
//! - `expressiveness ∈ [0, 1]` is the overall gain. A regulator under
//!   high `safety_dampening` (e.g. adversarial-provocation scenarios)
//!   computes low `expressiveness`, which mutes the entire face toward
//!   neutral. This matches what social-regulated affect display looks
//!   like: a competent adult under provocation has a composed face,
//!   not a furious one.
//! - The `anger` channel's contribution is additionally scaled by
//!   `(1 - 0.7 × safety_dampening)` so even a fast-onset anger spike
//!   gets visibly softened on the face when the regulator says
//!   "don't show this." Sadness / fear / empathy aren't gated this
//!   way — those are socially safe to display under most circumstances.
//!
//! ## Bounds + neutrality
//!
//! - All output values are clamped to `[0, 1]`. ARKit weights outside
//!   that range are technically allowed (Audio2Face occasionally emits
//!   negatives), but every renderer in this workspace clips to `[0, 1]`
//!   anyway. Clipping here keeps the wire format predictable.
//! - The all-zero pose is the **neutral expression** — the renderer's
//!   default rest face. We never emit a frame more neutral than
//!   neutral.
//!
//! ## Future work
//!
//! When the affect-avatar DiT lands
//! (`tools/affect_avatar/`), this module becomes obsolete: the model
//! will produce per-tick blendshape vectors directly conditioned on
//! V/A/D + audio. Until then this gets us a usable face on every
//! tick of the affect chain without standing up that whole training
//! pipeline.

use affect_simulator::regulation::RegulationPolicy;
use affect_simulator::state::EmotionChannels;

use crate::nodes::lip_sync::ARKIT_52;

// ─── ARKit-52 index constants ─────────────────────────────────────────────
//
// Indices match `crate::nodes::lip_sync::ARKIT_BLENDSHAPE_NAMES`. The
// `arkit_index_constants_match_canonical_names` test below pins each
// constant against the canonical name table so a future reorder of
// the enum can't silently misroute weights.
//
// Note: `lip_sync::synthetic.rs` ships its own `IDX_MOUTH_SMILE_*`
// constants pointing at 24/25 (off-by-one vs the canonical table).
// Its tests only exercise `IDX_JAW_OPEN = 17`, which is correct, so
// the smile/jaw fixture happens to mismatch only for the smile lobes.
// We keep our constants right and let synthetic catch up in a
// separate pass.

const IDX_EYE_LOOK_DOWN_LEFT: usize = 1;
const IDX_EYE_SQUINT_LEFT: usize = 5;
const IDX_EYE_WIDE_LEFT: usize = 6;
const IDX_EYE_LOOK_DOWN_RIGHT: usize = 8;
const IDX_EYE_SQUINT_RIGHT: usize = 12;
const IDX_EYE_WIDE_RIGHT: usize = 13;

const IDX_JAW_FORWARD: usize = 14;
const IDX_JAW_OPEN: usize = 17;

const IDX_MOUTH_SMILE_LEFT: usize = 23;
const IDX_MOUTH_SMILE_RIGHT: usize = 24;
const IDX_MOUTH_FROWN_LEFT: usize = 25;
const IDX_MOUTH_FROWN_RIGHT: usize = 26;
const IDX_MOUTH_DIMPLE_LEFT: usize = 27;
const IDX_MOUTH_DIMPLE_RIGHT: usize = 28;
const IDX_MOUTH_STRETCH_LEFT: usize = 29;
const IDX_MOUTH_STRETCH_RIGHT: usize = 30;
const IDX_MOUTH_SHRUG_LOWER: usize = 33;
const IDX_MOUTH_PRESS_LEFT: usize = 35;
const IDX_MOUTH_PRESS_RIGHT: usize = 36;
const IDX_MOUTH_LOWER_DOWN_LEFT: usize = 37;
const IDX_MOUTH_LOWER_DOWN_RIGHT: usize = 38;

const IDX_BROW_DOWN_LEFT: usize = 41;
const IDX_BROW_DOWN_RIGHT: usize = 42;
const IDX_BROW_INNER_UP: usize = 43;
const IDX_BROW_OUTER_UP_LEFT: usize = 44;
const IDX_BROW_OUTER_UP_RIGHT: usize = 45;

const IDX_CHEEK_SQUINT_LEFT: usize = 47;
const IDX_CHEEK_SQUINT_RIGHT: usize = 48;
const IDX_NOSE_SNEER_LEFT: usize = 49;
const IDX_NOSE_SNEER_RIGHT: usize = 50;

// ─── Per-channel contribution rows ───────────────────────────────────────
//
// Each row is a sparse list of `(arkit_index, coefficient)` pairs. A row
// represents how one emotion channel pushes weight onto specific ARKit
// blendshapes. Rows are intentionally short — we only touch the
// blendshapes that meaningfully participate in that emotion's
// canonical FACS pattern. Coefficients are tuned so a channel value
// of 1.0 produces visible (but not saturated) facial movement before
// expressiveness gating; expressiveness then scales the whole pose.

type Coeffs = &'static [(usize, f32)];

const COEFFS_JOY: Coeffs = &[
    (IDX_MOUTH_SMILE_LEFT, 0.7),
    (IDX_MOUTH_SMILE_RIGHT, 0.7),
    (IDX_CHEEK_SQUINT_LEFT, 0.5),
    (IDX_CHEEK_SQUINT_RIGHT, 0.5),
    (IDX_MOUTH_DIMPLE_LEFT, 0.3),
    (IDX_MOUTH_DIMPLE_RIGHT, 0.3),
    // Joy reduces eye-wide slightly — the cheek lift narrows the eye
    // aperture (Duchenne). We can't subtract from a clamped sum, so
    // we just don't add eye-wide here; the absence is enough.
];

const COEFFS_SADNESS: Coeffs = &[
    (IDX_MOUTH_FROWN_LEFT, 0.5),
    (IDX_MOUTH_FROWN_RIGHT, 0.5),
    (IDX_BROW_INNER_UP, 0.6),
    (IDX_MOUTH_SHRUG_LOWER, 0.3),
    (IDX_EYE_LOOK_DOWN_LEFT, 0.2),
    (IDX_EYE_LOOK_DOWN_RIGHT, 0.2),
    (IDX_MOUTH_LOWER_DOWN_LEFT, 0.2),
    (IDX_MOUTH_LOWER_DOWN_RIGHT, 0.2),
];

const COEFFS_ANGER: Coeffs = &[
    (IDX_BROW_DOWN_LEFT, 0.7),
    (IDX_BROW_DOWN_RIGHT, 0.7),
    (IDX_MOUTH_PRESS_LEFT, 0.4),
    (IDX_MOUTH_PRESS_RIGHT, 0.4),
    (IDX_JAW_FORWARD, 0.2),
    (IDX_NOSE_SNEER_LEFT, 0.3),
    (IDX_NOSE_SNEER_RIGHT, 0.3),
    (IDX_EYE_SQUINT_LEFT, 0.3),
    (IDX_EYE_SQUINT_RIGHT, 0.3),
];

const COEFFS_FEAR: Coeffs = &[
    (IDX_BROW_INNER_UP, 0.5),
    (IDX_BROW_OUTER_UP_LEFT, 0.4),
    (IDX_BROW_OUTER_UP_RIGHT, 0.4),
    (IDX_EYE_WIDE_LEFT, 0.6),
    (IDX_EYE_WIDE_RIGHT, 0.6),
    (IDX_MOUTH_STRETCH_LEFT, 0.4),
    (IDX_MOUTH_STRETCH_RIGHT, 0.4),
    (IDX_JAW_OPEN, 0.2),
];

// frustration is "anger lite" — narrower brow, set mouth, no sneer/jaw.
const COEFFS_FRUSTRATION: Coeffs = &[
    (IDX_BROW_DOWN_LEFT, 0.4),
    (IDX_BROW_DOWN_RIGHT, 0.4),
    (IDX_MOUTH_PRESS_LEFT, 0.3),
    (IDX_MOUTH_PRESS_RIGHT, 0.3),
    (IDX_JAW_FORWARD, 0.1),
];

const COEFFS_CURIOSITY: Coeffs = &[
    (IDX_BROW_OUTER_UP_LEFT, 0.3),
    (IDX_BROW_OUTER_UP_RIGHT, 0.3),
    (IDX_BROW_INNER_UP, 0.2),
    (IDX_EYE_WIDE_LEFT, 0.3),
    (IDX_EYE_WIDE_RIGHT, 0.3),
    (IDX_JAW_OPEN, 0.1),
];

const COEFFS_EMPATHY: Coeffs = &[
    (IDX_BROW_INNER_UP, 0.4),
    (IDX_MOUTH_SMILE_LEFT, 0.2),
    (IDX_MOUTH_SMILE_RIGHT, 0.2),
    (IDX_EYE_LOOK_DOWN_LEFT, 0.1),
    (IDX_EYE_LOOK_DOWN_RIGHT, 0.1),
];

// `calm` is intentionally absent — it has no per-channel ARKit
// contribution. Calm pulls the face toward neutral, which is what
// the all-zero baseline already represents.

/// Channel-name → coefficient-row table. Order doesn't matter
/// (contributions sum), but kept stable so test output is readable.
pub const CHANNEL_COEFFICIENTS: &[(&str, Coeffs)] = &[
    ("anger", COEFFS_ANGER),
    ("sadness", COEFFS_SADNESS),
    ("fear", COEFFS_FEAR),
    ("joy", COEFFS_JOY),
    ("frustration", COEFFS_FRUSTRATION),
    ("curiosity", COEFFS_CURIOSITY),
    ("empathy", COEFFS_EMPATHY),
];

/// Default scalar multiplier applied after summation + expressiveness
/// gating. Tuned so that a "moderate joy + moderate empathy" mix
/// produces visible-but-not-saturated weights (~0.4–0.5 on the active
/// blendshapes); raise via [`compute_blendshapes_with_gain`] if you
/// want a more emotive face.
pub const DEFAULT_GAIN: f32 = 1.0;

/// Compute ARKit-52 blendshape weights from one simulator state +
/// regulation snapshot.
///
/// See module docs for the contribution table and gating rules. Output
/// values are clamped to `[0, 1]`. The all-zero array is the
/// neutral pose — emitted whenever every channel is at baseline AND
/// `expressiveness` is non-zero (or whenever `expressiveness == 0`,
/// regardless of channels).
pub fn compute_blendshapes(
    channels: &EmotionChannels,
    policy: &RegulationPolicy,
) -> [f32; ARKIT_52] {
    compute_blendshapes_with_gain(channels, policy, DEFAULT_GAIN)
}

/// Same as [`compute_blendshapes`] but with a tunable post-expressiveness
/// gain. Use `1.0` for the default emote-strength; raise to ~1.5 for a
/// more theatrical face, lower toward 0.5 for muted readings.
pub fn compute_blendshapes_with_gain(
    channels: &EmotionChannels,
    policy: &RegulationPolicy,
    gain: f32,
) -> [f32; ARKIT_52] {
    let mut weights = [0.0f32; ARKIT_52];

    // Anger gets an extra safety-attenuation so an internally angry but
    // socially-regulated agent shows a softer brow/mouth than the raw
    // channel value would imply. Other channels (sadness / fear /
    // empathy) are socially-safe to display under most policies, so we
    // leave them ungated here.
    let safety = policy.safety_dampening.clamp(0.0, 1.0);
    let anger_attenuation = 1.0 - 0.7 * safety;

    // Helper: read a named channel from EmotionChannels. We could do
    // this via reflection but the explicit match is faster + matches
    // EmotionChannels::get_mut's signature shape.
    let channel_value = |name: &str| -> f32 {
        match name {
            "anger" => channels.anger * anger_attenuation,
            "sadness" => channels.sadness,
            "fear" => channels.fear,
            "joy" => channels.joy,
            "calm" => channels.calm,
            "frustration" => channels.frustration,
            "curiosity" => channels.curiosity,
            "empathy" => channels.empathy,
            _ => 0.0,
        }
        .clamp(0.0, 1.0)
    };

    for (name, coeffs) in CHANNEL_COEFFICIENTS {
        let value = channel_value(name);
        if value <= 0.0 {
            continue;
        }
        for (idx, coeff) in *coeffs {
            weights[*idx] += value * coeff;
        }
    }

    // Expressiveness gates the *displayed* intensity, not the felt
    // intensity. Under high safety_dampening the regulator computes a
    // low expressiveness, pulling the face toward neutral.
    let expressiveness = policy.expressiveness.clamp(0.0, 1.0);
    let scale = expressiveness * gain.max(0.0);

    for w in &mut weights {
        *w = (*w * scale).clamp(0.0, 1.0);
    }
    weights
}

/// Returns the top-k active blendshapes as (name, weight) pairs in
/// descending intensity order. Convenience for diagnostics / coach
/// CLI readouts; values below `min_threshold` are skipped.
pub fn top_active(
    weights: &[f32; ARKIT_52],
    k: usize,
    min_threshold: f32,
) -> Vec<(&'static str, f32)> {
    let names = crate::nodes::lip_sync::ARKIT_BLENDSHAPE_NAMES;
    let mut pairs: Vec<(usize, f32)> = weights
        .iter()
        .enumerate()
        .filter(|(_, &w)| w >= min_threshold)
        .map(|(i, &w)| (i, w))
        .collect();
    pairs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    pairs
        .into_iter()
        .take(k)
        .map(|(i, w)| (names[i], w))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn neutral_policy() -> RegulationPolicy {
        // expressiveness = 0.5 (neither stoic nor vivid), no safety
        // attenuation, baseline knobs.
        RegulationPolicy::neutral()
    }

    fn pure_channel(name: &str, val: f32) -> EmotionChannels {
        let mut c = EmotionChannels::default();
        if let Some(slot) = c.get_mut(name) {
            *slot = val.clamp(0.0, 1.0);
        }
        c
    }

    /// Index constants are sanity-checked against the canonical
    /// `ARKIT_BLENDSHAPE_NAMES` once — if the table is ever reordered
    /// every other test would silently start firing the wrong
    /// blendshape, which is the kind of bug that's painful to chase.
    #[test]
    fn arkit_index_constants_match_canonical_names() {
        let names = crate::nodes::lip_sync::ARKIT_BLENDSHAPE_NAMES;
        let pairs: &[(usize, &str)] = &[
            (IDX_EYE_LOOK_DOWN_LEFT, "eyeLookDownLeft"),
            (IDX_EYE_SQUINT_LEFT, "eyeSquintLeft"),
            (IDX_EYE_WIDE_LEFT, "eyeWideLeft"),
            (IDX_EYE_LOOK_DOWN_RIGHT, "eyeLookDownRight"),
            (IDX_EYE_SQUINT_RIGHT, "eyeSquintRight"),
            (IDX_EYE_WIDE_RIGHT, "eyeWideRight"),
            (IDX_JAW_FORWARD, "jawForward"),
            (IDX_JAW_OPEN, "jawOpen"),
            (IDX_MOUTH_SMILE_LEFT, "mouthSmileLeft"),
            (IDX_MOUTH_SMILE_RIGHT, "mouthSmileRight"),
            (IDX_MOUTH_FROWN_LEFT, "mouthFrownLeft"),
            (IDX_MOUTH_FROWN_RIGHT, "mouthFrownRight"),
            (IDX_MOUTH_DIMPLE_LEFT, "mouthDimpleLeft"),
            (IDX_MOUTH_DIMPLE_RIGHT, "mouthDimpleRight"),
            (IDX_MOUTH_STRETCH_LEFT, "mouthStretchLeft"),
            (IDX_MOUTH_STRETCH_RIGHT, "mouthStretchRight"),
            (IDX_MOUTH_SHRUG_LOWER, "mouthShrugLower"),
            (IDX_MOUTH_PRESS_LEFT, "mouthPressLeft"),
            (IDX_MOUTH_PRESS_RIGHT, "mouthPressRight"),
            (IDX_MOUTH_LOWER_DOWN_LEFT, "mouthLowerDownLeft"),
            (IDX_MOUTH_LOWER_DOWN_RIGHT, "mouthLowerDownRight"),
            (IDX_BROW_DOWN_LEFT, "browDownLeft"),
            (IDX_BROW_DOWN_RIGHT, "browDownRight"),
            (IDX_BROW_INNER_UP, "browInnerUp"),
            (IDX_BROW_OUTER_UP_LEFT, "browOuterUpLeft"),
            (IDX_BROW_OUTER_UP_RIGHT, "browOuterUpRight"),
            (IDX_CHEEK_SQUINT_LEFT, "cheekSquintLeft"),
            (IDX_CHEEK_SQUINT_RIGHT, "cheekSquintRight"),
            (IDX_NOSE_SNEER_LEFT, "noseSneerLeft"),
            (IDX_NOSE_SNEER_RIGHT, "noseSneerRight"),
        ];
        for (idx, name) in pairs {
            assert_eq!(
                names[*idx], *name,
                "IDX constant {} pointed at {:?}, expected {:?}",
                idx, names[*idx], name,
            );
        }
    }

    /// Pure neutral state with neutral policy → all-zero blendshapes
    /// (the renderer's rest pose).
    #[test]
    fn neutral_state_yields_zero_pose() {
        let weights = compute_blendshapes(&EmotionChannels::default(), &neutral_policy());
        for (i, w) in weights.iter().enumerate() {
            assert!(*w == 0.0, "weights[{}] = {} for neutral state", i, w);
        }
    }

    /// expressiveness = 0 mutes all output regardless of channel mix.
    #[test]
    fn zero_expressiveness_mutes_face() {
        let mut policy = neutral_policy();
        policy.expressiveness = 0.0;
        let weights = compute_blendshapes(&pure_channel("joy", 1.0), &policy);
        assert!(
            weights.iter().all(|&w| w == 0.0),
            "expressiveness=0 must produce zero pose; got max {}",
            weights.iter().cloned().fold(0.0f32, f32::max),
        );
    }

    /// Pure joy → both smile blendshapes (and Duchenne cheek squint)
    /// fire, no frowns, no anger brow.
    #[test]
    fn pure_joy_fires_smile_and_cheek_squint() {
        let mut policy = neutral_policy();
        policy.expressiveness = 1.0;
        let weights = compute_blendshapes(&pure_channel("joy", 1.0), &policy);
        assert!(
            weights[IDX_MOUTH_SMILE_LEFT] > 0.4,
            "smile_left = {}",
            weights[IDX_MOUTH_SMILE_LEFT]
        );
        assert!(
            weights[IDX_MOUTH_SMILE_RIGHT] > 0.4,
            "smile_right = {}",
            weights[IDX_MOUTH_SMILE_RIGHT]
        );
        assert!(
            weights[IDX_CHEEK_SQUINT_LEFT] > 0.3,
            "cheek_squint_left = {}",
            weights[IDX_CHEEK_SQUINT_LEFT]
        );
        assert!(
            weights[IDX_MOUTH_FROWN_LEFT] == 0.0,
            "frown_left should be zero, got {}",
            weights[IDX_MOUTH_FROWN_LEFT]
        );
        assert!(
            weights[IDX_BROW_DOWN_LEFT] == 0.0,
            "brow_down_left should be zero, got {}",
            weights[IDX_BROW_DOWN_LEFT]
        );
    }

    /// Pure sadness → frowns + sad-brow shape (browInnerUp), not
    /// anger-brow (browDown).
    #[test]
    fn pure_sadness_fires_frown_and_inner_brow() {
        let mut policy = neutral_policy();
        policy.expressiveness = 1.0;
        let weights = compute_blendshapes(&pure_channel("sadness", 1.0), &policy);
        assert!(
            weights[IDX_MOUTH_FROWN_LEFT] > 0.3,
            "frown_left = {}",
            weights[IDX_MOUTH_FROWN_LEFT]
        );
        assert!(
            weights[IDX_BROW_INNER_UP] > 0.3,
            "brow_inner_up = {}",
            weights[IDX_BROW_INNER_UP]
        );
        assert!(
            weights[IDX_BROW_DOWN_LEFT] == 0.0,
            "brow_down_left should be zero on sadness, got {}",
            weights[IDX_BROW_DOWN_LEFT]
        );
    }

    /// Pure anger → brow_down + sneer + mouth_press. Distinct from
    /// sadness (no inner_brow, no frown).
    #[test]
    fn pure_anger_fires_brow_down_and_sneer() {
        let mut policy = neutral_policy();
        policy.expressiveness = 1.0;
        let weights = compute_blendshapes(&pure_channel("anger", 1.0), &policy);
        assert!(
            weights[IDX_BROW_DOWN_LEFT] > 0.4,
            "brow_down_left = {}",
            weights[IDX_BROW_DOWN_LEFT]
        );
        assert!(
            weights[IDX_NOSE_SNEER_LEFT] > 0.1,
            "nose_sneer_left = {}",
            weights[IDX_NOSE_SNEER_LEFT]
        );
        assert!(
            weights[IDX_MOUTH_PRESS_LEFT] > 0.2,
            "mouth_press_left = {}",
            weights[IDX_MOUTH_PRESS_LEFT]
        );
        assert!(
            weights[IDX_BROW_INNER_UP] == 0.0,
            "brow_inner_up should be zero on pure anger, got {}",
            weights[IDX_BROW_INNER_UP]
        );
        assert!(
            weights[IDX_MOUTH_FROWN_LEFT] == 0.0,
            "frown should be zero on anger, got {}",
            weights[IDX_MOUTH_FROWN_LEFT]
        );
    }

    /// Safety dampening attenuates anger more than other channels.
    /// Sadness at the same level under safety_dampening keeps its
    /// face; anger softens.
    #[test]
    fn safety_dampening_softens_anger_specifically() {
        let mut policy = neutral_policy();
        policy.expressiveness = 1.0;
        policy.safety_dampening = 0.0;
        let anger_unsafe = compute_blendshapes(&pure_channel("anger", 1.0), &policy);
        let sad_unsafe = compute_blendshapes(&pure_channel("sadness", 1.0), &policy);

        policy.safety_dampening = 1.0;
        let anger_safe = compute_blendshapes(&pure_channel("anger", 1.0), &policy);
        let sad_safe = compute_blendshapes(&pure_channel("sadness", 1.0), &policy);

        // Anger's brow-down should drop materially (safety pulls it
        // toward neutral) — but not all the way to zero (we apply
        // 1 - 0.7×safety, not 1 - safety).
        assert!(
            anger_safe[IDX_BROW_DOWN_LEFT] < anger_unsafe[IDX_BROW_DOWN_LEFT],
            "anger brow_down should attenuate under safety; \
             unsafe={:.3} safe={:.3}",
            anger_unsafe[IDX_BROW_DOWN_LEFT],
            anger_safe[IDX_BROW_DOWN_LEFT],
        );
        // Sadness should be unaffected by safety_dampening.
        assert_eq!(
            sad_safe[IDX_MOUTH_FROWN_LEFT], sad_unsafe[IDX_MOUTH_FROWN_LEFT],
            "sadness frown shouldn't change under safety_dampening",
        );
    }

    /// Pure empathy yields a compassionate-listening pose: gentle
    /// smile + sad-brow shape + downward gaze. Distinct from joy
    /// (no Duchenne cheek squint) and from sadness (no frown).
    #[test]
    fn pure_empathy_yields_compassionate_listening_pose() {
        let mut policy = neutral_policy();
        policy.expressiveness = 1.0;
        let weights = compute_blendshapes(&pure_channel("empathy", 1.0), &policy);
        assert!(weights[IDX_BROW_INNER_UP] > 0.2);
        assert!(weights[IDX_MOUTH_SMILE_LEFT] > 0.05);
        assert!(weights[IDX_EYE_LOOK_DOWN_LEFT] > 0.05);
        assert_eq!(weights[IDX_CHEEK_SQUINT_LEFT], 0.0);
        assert_eq!(weights[IDX_MOUTH_FROWN_LEFT], 0.0);
    }

    /// Top-active diagnostic returns descending-by-weight, filters
    /// near-zero values.
    #[test]
    fn top_active_returns_top_k_descending() {
        let mut policy = neutral_policy();
        policy.expressiveness = 1.0;
        let weights = compute_blendshapes(&pure_channel("joy", 1.0), &policy);
        let top = top_active(&weights, 4, 0.05);
        assert!(!top.is_empty());
        // Smiles should outrank dimples (0.7 vs 0.3 coefficient).
        let names: Vec<&str> = top.iter().map(|(n, _)| *n).collect();
        let smile_idx = names.iter().position(|n| *n == "mouthSmileLeft");
        let dimple_idx = names.iter().position(|n| *n == "mouthDimpleLeft");
        if let (Some(si), Some(di)) = (smile_idx, dimple_idx) {
            assert!(si < di, "smile should outrank dimple in top_active");
        }
        // Top sequence is monotone non-increasing.
        for w in top.windows(2) {
            assert!(w[0].1 >= w[1].1);
        }
    }
}
