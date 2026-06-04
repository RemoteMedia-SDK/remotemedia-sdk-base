//! `ArkitSmoother` — uniform exponential-moving-average over ARKit-52
//! blendshape vectors.
//!
//! Spec [§3.4] config knob: `smoothing_alpha`. The persona-engine ships
//! a more elaborate per-VBridger-param `ParamSmoother` (different
//! coefficient per axis) but it operates on *post-mapped* params,
//! which per spec §3.4 is renderer territory. The lip-sync node ships
//! a single uniform alpha applied to raw ARKit-52 — simpler, and any
//! per-axis tuning lives in the renderer's mapper.
//!
//! ## Math
//!
//! `out[i] = alpha · prev[i] + (1 - alpha) · in[i]` for each `i ∈ [0, 52)`.
//!
//! - `alpha = 0` → no smoothing (passthrough).
//! - `alpha = 1` → infinite smoothing (output = prev forever; degenerate).
//! - typical `alpha ∈ [0.1, 0.4]` matches persona-engine's per-axis tunings.
//!
//! First frame passes through unchanged (no `prev` to mix with).

use crate::nodes::lip_sync::blendshape::ARKIT_52;

/// Uniform EMA smoother over the 52-element ARKit blendshape vector.
pub struct ArkitSmoother {
    alpha: f32,
    prev: [f32; ARKIT_52],
    has_prev: bool,
}

impl ArkitSmoother {
    /// Build a smoother. `alpha` is clamped to `[0, 1]`.
    pub fn new(alpha: f32) -> Self {
        Self {
            alpha: alpha.clamp(0.0, 1.0),
            prev: [0.0; ARKIT_52],
            has_prev: false,
        }
    }

    /// Apply EMA to one frame. First call passes through unchanged.
    pub fn smooth(&mut self, input: &[f32; ARKIT_52]) -> [f32; ARKIT_52] {
        if !self.has_prev {
            self.has_prev = true;
            self.prev = *input;
            return *input;
        }
        let mut out = [0.0f32; ARKIT_52];
        let a = self.alpha;
        let inv = 1.0 - a;
        for i in 0..ARKIT_52 {
            out[i] = a * self.prev[i] + inv * input[i];
        }
        self.prev = out;
        out
    }

    /// Reset state — the next `smooth()` call will pass through.
    /// Used by `barge_in` so the smoother doesn't pull the new turn
    /// toward the prior turn's frozen pose.
    pub fn reset(&mut self) {
        self.prev = [0.0; ARKIT_52];
        self.has_prev = false;
    }

    /// Snapshot current state for save/restore (mirrors the persona-
    /// engine's API; useful for speculative replay).
    pub fn save(&self) -> ([f32; ARKIT_52], bool) {
        (self.prev, self.has_prev)
    }

    /// Restore from a snapshot.
    pub fn restore(&mut self, state: ([f32; ARKIT_52], bool)) {
        self.prev = state.0;
        self.has_prev = state.1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f32, b: f32, eps: f32) -> bool {
        (a - b).abs() < eps
    }

    #[test]
    fn first_frame_passes_through() {
        let mut s = ArkitSmoother::new(0.5);
        let mut input = [0.0f32; ARKIT_52];
        input[17] = 0.8; // jawOpen
        let out = s.smooth(&input);
        assert_eq!(out, input);
    }

    #[test]
    fn second_frame_blends_50_50_at_alpha_half() {
        let mut s = ArkitSmoother::new(0.5);
        let mut a = [0.0f32; ARKIT_52];
        a[17] = 1.0;
        let _ = s.smooth(&a);
        let mut b = [0.0f32; ARKIT_52];
        b[17] = 0.0;
        let out = s.smooth(&b);
        // 0.5 * 1.0 + 0.5 * 0.0 = 0.5
        assert!(approx(out[17], 0.5, 1e-6));
    }

    #[test]
    fn alpha_zero_is_passthrough() {
        let mut s = ArkitSmoother::new(0.0);
        let mut a = [0.0f32; ARKIT_52];
        a[17] = 0.5;
        let _ = s.smooth(&a);
        let mut b = [0.0f32; ARKIT_52];
        b[17] = 0.9;
        let out = s.smooth(&b);
        assert!(approx(out[17], 0.9, 1e-6), "expected 0.9, got {}", out[17]);
    }

    #[test]
    fn alpha_clamped_above_one() {
        // alpha=2.0 → should clamp to 1.0 (full prev hold).
        let mut s = ArkitSmoother::new(2.0);
        let mut a = [0.0f32; ARKIT_52];
        a[17] = 1.0;
        let _ = s.smooth(&a);
        let mut b = [0.0f32; ARKIT_52];
        b[17] = 0.0;
        let out = s.smooth(&b);
        // alpha=1 → out = prev = 1.0
        assert!(approx(out[17], 1.0, 1e-6));
    }

    #[test]
    fn reset_returns_to_first_frame_behavior() {
        let mut s = ArkitSmoother::new(0.5);
        let mut a = [0.0f32; ARKIT_52];
        a[17] = 1.0;
        let _ = s.smooth(&a);
        s.reset();
        // After reset, next frame passes through unchanged.
        let mut b = [0.0f32; ARKIT_52];
        b[17] = 0.7;
        let out = s.smooth(&b);
        assert_eq!(out, b);
    }

    #[test]
    fn save_restore_round_trip() {
        let mut s = ArkitSmoother::new(0.3);
        let mut a = [0.0f32; ARKIT_52];
        a[17] = 0.6;
        let _ = s.smooth(&a);
        let snap = s.save();
        // Advance state, then restore — the next smoothed output
        // should match what we'd have got without the advance.
        let mut b = [0.0f32; ARKIT_52];
        b[17] = 0.0;
        let _ = s.smooth(&b);
        s.restore(snap);
        let out = s.smooth(&b);
        // Without the intervening advance: 0.3 * 0.6 + 0.7 * 0.0 = 0.18
        assert!(approx(out[17], 0.18, 1e-6), "got {}", out[17]);
    }

    #[test]
    fn smoothing_decays_toward_input_over_time() {
        let mut s = ArkitSmoother::new(0.8); // strong smoothing
        let mut a = [0.0f32; ARKIT_52];
        a[17] = 0.0;
        let _ = s.smooth(&a);
        let mut b = [0.0f32; ARKIT_52];
        b[17] = 1.0;
        let mut current = 0.0f32;
        for _ in 0..30 {
            current = s.smooth(&b)[17];
        }
        // After 30 iterations with alpha=0.8, prev approaches 1.0 but
        // never reaches it. 0.8^30 ≈ 0.00124 → out ≈ 1 - 0.00124.
        assert!(
            current > 0.998,
            "expected >0.998 after 30 iters, got {current}"
        );
        assert!(current < 1.0, "should never reach exactly 1.0");
    }
}
