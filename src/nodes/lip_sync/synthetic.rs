//! `SyntheticLipSyncNode` — deterministic, dependency-free stand-in
//! for the real `Audio2FaceLipSyncNode`.
//!
//! ## Why ship a synthetic?
//!
//! The avatar plan's M2.7 integration test exercises EmotionExtractor +
//! LipSync + (eventually) Renderer end-to-end through the WebRTC
//! transport. Gating that on the real Audio2Face ONNX model — which
//! requires a license-walled download from the persona-engine
//! installer — would mean it never runs in CI. A deterministic
//! synthetic that satisfies the same `LipSyncNode` contract lets the
//! plumbing assertions (frames flow, pts_ms is monotonic, barge clears
//! the buffer, capabilities resolve) run on every host.
//!
//! ## Mapping
//!
//! Per output tick:
//!   - Compute RMS of the input audio chunk (`rms ∈ [0, 1]` after
//!     `min(rms * gain, 1)` clipping).
//!   - `arkit_52[jawOpen] = rms`
//!   - `arkit_52[mouthSmileLeft] = arkit_52[mouthSmileRight] = rms * 0.4`
//!   - All other slots = 0
//!   - `pts_ms` = cumulative input-audio milliseconds processed.
//!
//! That's a believable mouth-with-audio behavior — wide open on loud
//! frames, closed on silent ones — without any ML. Real Audio2Face
//! produces 52 phoneme-shaped activations; this only animates four.
//! Renderer e2e tests don't care which axes move, only that the
//! envelope is well-formed and timed.

use crate::data::RuntimeData;
use crate::error::{Error, Result};
use crate::nodes::lip_sync::blendshape::{BlendshapeFrame, ARKIT_52};
use crate::nodes::lip_sync::LipSyncNode;
use crate::nodes::AsyncStreamingNode;
use crate::transport::session_control::{aux_port_of, BARGE_IN_PORT};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// Configuration for [`SyntheticLipSyncNode`].
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, rename_all = "camelCase")]
pub struct SyntheticLipSyncConfig {
    /// Required input sample rate. Default 16 kHz to match what the
    /// real Audio2Face will require (so manifests stay portable when
    /// the real impl drops in).
    pub sample_rate: u32,
    /// Multiplier applied to RMS before clipping into [0, 1]. Higher
    /// values make the mouth more reactive to soft audio. Persona-
    /// engine's tuned default for line-level audio is around 6.0.
    pub gain: f32,
}

impl Default for SyntheticLipSyncConfig {
    fn default() -> Self {
        Self {
            sample_rate: 16_000,
            gain: 6.0,
        }
    }
}

// Indices we touch in the 52-vector. Kept here as constants rather
// than name-lookups so the hot path stays branch-free.
const IDX_JAW_OPEN: usize = 17;
const IDX_MOUTH_SMILE_LEFT: usize = 24;
const IDX_MOUTH_SMILE_RIGHT: usize = 25;

pub struct SyntheticLipSyncNode {
    config: SyntheticLipSyncConfig,
    /// Cumulative ms of input audio seen across all frames.
    /// Atomic so `process_streaming(&self, …)` (immutable receiver)
    /// can advance without a Mutex.
    cum_ms: AtomicU64,
}

impl SyntheticLipSyncNode {
    pub fn new(config: SyntheticLipSyncConfig) -> Self {
        Self {
            config,
            cum_ms: AtomicU64::new(0),
        }
    }

    pub fn with_default() -> Self {
        Self::new(SyntheticLipSyncConfig::default())
    }

    /// Reset the cumulative-ms clock — used by `barge_in` (M2.6).
    pub fn reset_clock(&self) {
        self.cum_ms.store(0, Ordering::Release);
    }

    fn build_frame(&self, samples: &[f32], pts_ms: u64) -> BlendshapeFrame {
        let mut arkit = [0.0f32; ARKIT_52];
        if !samples.is_empty() {
            let mut sum_sq = 0.0f64;
            for &s in samples {
                sum_sq += (s as f64) * (s as f64);
            }
            let rms = (sum_sq / samples.len() as f64).sqrt() as f32;
            let val = (rms * self.config.gain).clamp(0.0, 1.0);
            arkit[IDX_JAW_OPEN] = val;
            arkit[IDX_MOUTH_SMILE_LEFT] = val * 0.4;
            arkit[IDX_MOUTH_SMILE_RIGHT] = val * 0.4;
        }
        // turn_id forwarding is parked (see emotion_extractor docstring)
        // — Audio frames carry metadata; we *could* propagate from
        // `metadata.turn_id` but that wiring lives in the real
        // Audio2Face port to keep the synthetic stand-in narrow.
        BlendshapeFrame::new(arkit, pts_ms, None)
    }
}

#[async_trait]
impl AsyncStreamingNode for SyntheticLipSyncNode {
    fn node_type(&self) -> &str {
        "SyntheticLipSyncNode"
    }

    async fn process(&self, _data: RuntimeData) -> Result<RuntimeData> {
        // Synthetic emits per-chunk blendshapes via process_streaming.
        Err(Error::Execution(
            "SyntheticLipSyncNode requires streaming mode — use process_streaming()".into(),
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
        let (samples, sample_rate) = match data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                ..
            } => (samples, sample_rate),
            other => {
                // Pass-through non-audio (mirrors silero_vad / emotion_extractor).
                callback(other)?;
                return Ok(1);
            }
        };

        // Compute the *input* duration in ms — this is what `pts_ms`
        // should advance by, regardless of whether upstream sent us
        // 16 kHz or some other rate. The renderer reads `pts_ms` as
        // a clock tied to audio playback; we honor that.
        let chunk_ms = if sample_rate == 0 {
            0
        } else {
            (samples.len() as u64 * 1000) / sample_rate as u64
        };
        let pts_ms = self.cum_ms.fetch_add(chunk_ms, Ordering::AcqRel) + chunk_ms;

        let frame = self.build_frame(&samples, pts_ms);
        callback(RuntimeData::Json(frame.to_json()))?;
        Ok(1)
    }

    /// Runtime-dispatched control message handler. On `barge_in` we
    /// reset the cumulative-ms clock so the post-barge turn's `pts_ms`
    /// stream restarts at zero — keeping the wire-format contract
    /// identical between the synthetic stand-in and the real
    /// `Audio2FaceLipSyncNode`. See spec §3.4 + the avatar plan's M2.6.
    async fn process_control_message(
        &self,
        message: RuntimeData,
        _session_id: Option<String>,
    ) -> Result<bool> {
        if matches!(aux_port_of(&message).as_deref(), Some(BARGE_IN_PORT)) {
            self.reset_clock();
            return Ok(true);
        }
        Ok(false)
    }
}

impl LipSyncNode for SyntheticLipSyncNode {
    fn required_sample_rate(&self) -> u32 {
        self.config.sample_rate
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::audio_samples::AudioSamples;

    fn audio_chunk(samples: Vec<f32>, sample_rate: u32) -> RuntimeData {
        RuntimeData::Audio {
            samples: AudioSamples::Vec(samples),
            sample_rate,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        }
    }

    async fn drive(node: &SyntheticLipSyncNode, data: RuntimeData) -> Vec<RuntimeData> {
        let collected = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let cc = collected.clone();
        node.process_streaming(data, None, move |out| {
            cc.lock().unwrap().push(out);
            Ok(())
        })
        .await
        .expect("process_streaming");
        let v = collected.lock().unwrap().clone();
        v
    }

    #[tokio::test]
    async fn silent_audio_emits_neutral_mouth() {
        let node = SyntheticLipSyncNode::with_default();
        let outs = drive(&node, audio_chunk(vec![0.0; 1600], 16_000)).await;
        assert_eq!(outs.len(), 1);
        let frame =
            BlendshapeFrame::from_json(outs[0].as_json().expect("json")).expect("blendshape");
        assert!(frame.arkit_52.iter().all(|&v| v == 0.0));
    }

    #[tokio::test]
    async fn loud_audio_opens_jaw() {
        let node = SyntheticLipSyncNode::with_default();
        // 0.5 amplitude sine ≈ 0.354 RMS; gain=6 → clipped to 1.0.
        let samples: Vec<f32> = (0..1600)
            .map(|i| 0.5 * (2.0 * std::f32::consts::PI * 440.0 * i as f32 / 16_000.0).sin())
            .collect();
        let outs = drive(&node, audio_chunk(samples, 16_000)).await;
        let frame =
            BlendshapeFrame::from_json(outs[0].as_json().expect("json")).expect("blendshape");
        assert!(
            frame.arkit_52[IDX_JAW_OPEN] > 0.5,
            "jawOpen should be high for loud audio, got {}",
            frame.arkit_52[IDX_JAW_OPEN]
        );
    }

    #[tokio::test]
    async fn pts_ms_advances_with_input_duration() {
        let node = SyntheticLipSyncNode::with_default();
        // Two chunks of 100 ms (1600 samples @ 16 kHz) each.
        let outs1 = drive(&node, audio_chunk(vec![0.1; 1600], 16_000)).await;
        let outs2 = drive(&node, audio_chunk(vec![0.1; 1600], 16_000)).await;
        let pts1 = BlendshapeFrame::from_json(outs1[0].as_json().unwrap())
            .unwrap()
            .pts_ms;
        let pts2 = BlendshapeFrame::from_json(outs2[0].as_json().unwrap())
            .unwrap()
            .pts_ms;
        assert_eq!(pts1, 100);
        assert_eq!(pts2, 200);
    }

    #[tokio::test]
    async fn non_audio_passes_through() {
        let node = SyntheticLipSyncNode::with_default();
        let outs = drive(&node, RuntimeData::Text("hi".into())).await;
        assert_eq!(outs.len(), 1);
        assert!(matches!(&outs[0], RuntimeData::Text(_)));
    }

    #[tokio::test]
    async fn reset_clock_zeroes_pts_ms() {
        let node = SyntheticLipSyncNode::with_default();
        let _ = drive(&node, audio_chunk(vec![0.1; 1600], 16_000)).await;
        node.reset_clock();
        let outs = drive(&node, audio_chunk(vec![0.1; 1600], 16_000)).await;
        let pts = BlendshapeFrame::from_json(outs[0].as_json().unwrap())
            .unwrap()
            .pts_ms;
        assert_eq!(pts, 100, "reset_clock should restart the pts_ms counter");
    }

    /// M2.6: barge envelope dispatched to `process_control_message`
    /// resets the clock so the next frame's pts_ms starts at zero
    /// again. Mirrors the runtime's session_router routing — when the
    /// coordinator's `barge_in_targets` includes this node, the
    /// router will deliver the wrapped envelope here.
    #[tokio::test]
    async fn process_control_message_barge_in_resets_clock() {
        use crate::transport::session_control::{wrap_aux_port, BARGE_IN_PORT};

        let node = SyntheticLipSyncNode::with_default();
        // Advance the clock to 100 ms.
        let _ = drive(&node, audio_chunk(vec![0.1; 1600], 16_000)).await;

        // Deliver a wrapped barge envelope.
        let envelope = wrap_aux_port(BARGE_IN_PORT, RuntimeData::Json(serde_json::json!({})));
        let handled = node
            .process_control_message(envelope, None)
            .await
            .expect("control message ok");
        assert!(handled, "node must report it handled the barge envelope");

        // Next chunk's pts_ms should restart from 100 ms (just one
        // chunk's worth from zero).
        let outs = drive(&node, audio_chunk(vec![0.1; 1600], 16_000)).await;
        let pts = BlendshapeFrame::from_json(outs[0].as_json().unwrap())
            .unwrap()
            .pts_ms;
        assert_eq!(pts, 100, "barge should restart the cumulative-ms clock");
    }

    /// Non-barge control messages return `Ok(false)` (unhandled) so
    /// the runtime's universal fallback semantics still apply.
    #[tokio::test]
    async fn process_control_message_ignores_non_barge() {
        let node = SyntheticLipSyncNode::with_default();
        let unrelated = RuntimeData::Json(serde_json::json!({"kind": "weather", "city": "PDX"}));
        let handled = node
            .process_control_message(unrelated, None)
            .await
            .expect("ok");
        assert!(!handled);
    }

    #[test]
    fn lipsync_trait_required_sample_rate() {
        let node = SyntheticLipSyncNode::with_default();
        assert_eq!(node.required_sample_rate(), 16_000);
        assert_eq!(node.required_channels(), 1);
    }

    /// Convenience for tests inside this module.
    trait AsJson {
        fn as_json(&self) -> Option<&serde_json::Value>;
    }
    impl AsJson for RuntimeData {
        fn as_json(&self) -> Option<&serde_json::Value> {
            if let RuntimeData::Json(v) = self {
                Some(v)
            } else {
                None
            }
        }
    }
}
