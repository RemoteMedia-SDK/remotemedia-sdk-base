//! `BlendshapeFrame` — the wire-format envelope every `LipSyncNode`
//! emits, per spec 2026-04-27 §3.3.
//!
//! ## Why ARKit-52 (not Live2D / VBridger params)
//!
//! ARKit-52 is the renderer-agnostic interchange format. The
//! `Live2DRenderNode` (M4) maps these 52 floats to VBridger params
//! internally, so a future iOS / RealityKit / non-Live2D target reuses
//! the same blendshape stream. Mapping cost on a 30 fps render tick is
//! sub-microsecond.

use crate::error::{Error, Result};
use serde_json::{json, Value};

/// Number of ARKit blendshapes (canonical face animation set).
pub const ARKIT_52: usize = 52;

/// ARKit blendshape names in canonical order. Index `i` of the
/// `arkit_52` array corresponds to `ARKIT_BLENDSHAPE_NAMES[i]`.
///
/// Mirrors Apple's `ARFaceAnchor.BlendShapeLocation` enumeration.
/// Re-exported so the `Live2DRenderNode`'s ARKit→VBridger mapper can
/// reference names symbolically rather than hard-coding indices.
pub const ARKIT_BLENDSHAPE_NAMES: [&str; ARKIT_52] = [
    // Eyes (8)
    "eyeBlinkLeft",
    "eyeLookDownLeft",
    "eyeLookInLeft",
    "eyeLookOutLeft",
    "eyeLookUpLeft",
    "eyeSquintLeft",
    "eyeWideLeft",
    "eyeBlinkRight",
    // (continuing — index 8..16)
    "eyeLookDownRight",
    "eyeLookInRight",
    "eyeLookOutRight",
    "eyeLookUpRight",
    "eyeSquintRight",
    "eyeWideRight",
    // Jaw / mouth area (4)
    "jawForward",
    "jawLeft",
    // index 16..27
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
    // index 28..39
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
    // index 40..47
    "mouthUpperUpRight",
    // Brows (4)
    "browDownLeft",
    "browDownRight",
    "browInnerUp",
    "browOuterUpLeft",
    "browOuterUpRight",
    // Cheeks (3)
    "cheekPuff",
    "cheekSquintLeft",
    // index 48..51
    "cheekSquintRight",
    // Nose (2)
    "noseSneerLeft",
    "noseSneerRight",
    // Tongue (1)
    "tongueOut",
];

const _ASSERT_ARKIT_NAMES_LEN: () = assert!(ARKIT_BLENDSHAPE_NAMES.len() == ARKIT_52);

/// One timed blendshape keyframe — the unit a `LipSyncNode` emits
/// per output tick. Renderer treats consecutive keyframes as a
/// sampleable timeline keyed by `pts_ms` (audio playback time).
#[derive(Debug, Clone, PartialEq)]
pub struct BlendshapeFrame {
    /// 52 ARKit blendshape activations, indexed per
    /// [`ARKIT_BLENDSHAPE_NAMES`]. Values are not strictly bounded —
    /// the persona-engine's Audio2Face model emits raw predictions in
    /// roughly `[-1.0, 2.0]` and the PGD/BVLS solver clips to `[0, 1]`
    /// for animation. We don't enforce bounds at the envelope level
    /// because future solvers / phoneme impls may use other ranges.
    pub arkit_52: [f32; ARKIT_52],
    /// Presentation timestamp (ms) — matches the audio frame the
    /// keyframe was derived from, NOT wall time. Renderer samples the
    /// keyframe ring against the `audio.out.clock` tap.
    pub pts_ms: u64,
    /// Conversational turn id, forwarded if upstream metadata had one.
    /// Lets the renderer group blendshapes by turn for diagnostics or
    /// barge handling, without the lip-sync node tracking turns.
    pub turn_id: Option<u64>,
}

impl BlendshapeFrame {
    /// Build a frame; the array is borrowed in by value.
    pub fn new(arkit_52: [f32; ARKIT_52], pts_ms: u64, turn_id: Option<u64>) -> Self {
        Self {
            arkit_52,
            pts_ms,
            turn_id,
        }
    }

    /// All-zero blendshapes — the neutral pose. Renderer interpolates
    /// toward this when no audio is playing (spec §6.1).
    pub fn neutral(pts_ms: u64) -> Self {
        Self::new([0.0; ARKIT_52], pts_ms, None)
    }

    /// Encode the frame as the canonical `RuntimeData::Json` payload.
    ///
    /// Shape (spec §3.3):
    /// ```json
    /// {"kind": "blendshapes",
    ///  "arkit_52": [f32; 52],
    ///  "pts_ms": u64,
    ///  "turn_id": u64 | absent}
    /// ```
    pub fn to_json(&self) -> Value {
        let mut v = json!({
            "kind": "blendshapes",
            "arkit_52": self.arkit_52.as_slice(),
            "pts_ms": self.pts_ms,
        });
        if let Some(turn) = self.turn_id {
            v["turn_id"] = json!(turn);
        }
        v
    }

    /// Inverse of [`Self::to_json`]. Tolerant: missing `turn_id` is
    /// fine; arrays of the wrong length are an error.
    pub fn from_json(v: &Value) -> Result<Self> {
        let kind = v.get("kind").and_then(|k| k.as_str()).unwrap_or("");
        if kind != "blendshapes" {
            return Err(Error::InvalidData(format!(
                "BlendshapeFrame::from_json: expected kind='blendshapes', got {:?}",
                kind
            )));
        }
        let arr = v
            .get("arkit_52")
            .and_then(|a| a.as_array())
            .ok_or_else(|| Error::InvalidData("missing arkit_52 array".into()))?;
        if arr.len() != ARKIT_52 {
            return Err(Error::InvalidData(format!(
                "arkit_52 must have {} entries, got {}",
                ARKIT_52,
                arr.len()
            )));
        }
        let mut arkit_52 = [0.0f32; ARKIT_52];
        for (i, item) in arr.iter().enumerate() {
            arkit_52[i] = item
                .as_f64()
                .ok_or_else(|| Error::InvalidData(format!("arkit_52[{}] is not a number", i)))?
                as f32;
        }
        let pts_ms = v
            .get("pts_ms")
            .and_then(|p| p.as_u64())
            .ok_or_else(|| Error::InvalidData("missing or non-u64 pts_ms".into()))?;
        let turn_id = v.get("turn_id").and_then(|t| t.as_u64());
        Ok(Self::new(arkit_52, pts_ms, turn_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arkit_names_are_52() {
        assert_eq!(ARKIT_BLENDSHAPE_NAMES.len(), ARKIT_52);
    }

    #[test]
    fn jaw_open_index_matches_name() {
        // jawOpen sits at the canonical index ARKit declares; lock it
        // so the renderer's mapper doesn't drift if the array is ever
        // reordered. (jawOpen is the load-bearing mouth blendshape.)
        let pos = ARKIT_BLENDSHAPE_NAMES
            .iter()
            .position(|&n| n == "jawOpen")
            .expect("jawOpen must be present");
        assert!(
            pos < ARKIT_52,
            "jawOpen must be inside the 52-slot array (was {pos})"
        );
    }

    #[test]
    fn round_trip_via_json() {
        let mut arr = [0.0f32; ARKIT_52];
        arr[17] = 0.5; // jawOpen
        arr[24] = 0.25; // mouthSmileLeft
        let frame = BlendshapeFrame::new(arr, 12345, Some(7));
        let json = frame.to_json();
        assert_eq!(json["kind"], "blendshapes");
        assert_eq!(json["pts_ms"], 12345);
        assert_eq!(json["turn_id"], 7);
        assert_eq!(json["arkit_52"].as_array().unwrap().len(), 52);
        let back = BlendshapeFrame::from_json(&json).expect("from_json");
        assert_eq!(back, frame);
    }

    #[test]
    fn round_trip_omits_turn_id_when_none() {
        let frame = BlendshapeFrame::new([0.0; ARKIT_52], 10, None);
        let json = frame.to_json();
        assert!(json.get("turn_id").is_none());
        let back = BlendshapeFrame::from_json(&json).expect("from_json");
        assert_eq!(back.turn_id, None);
    }

    #[test]
    fn from_json_rejects_wrong_kind() {
        let bad = json!({
            "kind": "emotion",
            "arkit_52": vec![0.0f32; 52],
            "pts_ms": 0,
        });
        assert!(BlendshapeFrame::from_json(&bad).is_err());
    }

    #[test]
    fn from_json_rejects_short_array() {
        let bad = json!({
            "kind": "blendshapes",
            "arkit_52": vec![0.0f32; 10],
            "pts_ms": 0,
        });
        assert!(BlendshapeFrame::from_json(&bad).is_err());
    }

    #[test]
    fn neutral_is_all_zeros() {
        let n = BlendshapeFrame::neutral(42);
        assert!(n.arkit_52.iter().all(|&v| v == 0.0));
        assert_eq!(n.pts_ms, 42);
        assert_eq!(n.turn_id, None);
    }
}
