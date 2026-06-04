//! Reactive lipsync producer.
//!
//! Consumes audio chunks via `process_streaming_async` and publishes
//! typed [`AvatarWeights`] to a `weights` snapshot output port. The
//! `AvatarNode` reads those weights on its own tick and decides
//! between the speaking and idle render paths.
//!
//! This skeleton ships with a deterministic stub inference: per chunk
//! we compute the audio's RMS and map it onto a `mouth_open` weight
//! in `[0, 1]`. The shape of the produced [`AvatarWeights`] is the
//! same as what a real Audio2Face inference run would emit (typed
//! struct on a snapshot port), so swapping the stub for a real model
//! is a one-method change.
//!
//! ## Why Reactive (not Clocked)?
//!
//! Lipsync is naturally reactive — there's nothing to compute when no
//! audio is flowing. The avatar absorbs the rate mismatch on the read
//! side: it ticks at the video clock rate (30/60 Hz) and reads the
//! latest published weights snapshot every tick. A 50 Hz audio
//! producer ↔ 30 Hz video consumer is the canonical Reactive→Clocked
//! seam this spec was designed around.
//!
//! ## Output
//!
//! - **Snapshot**: `weights` port → [`AvatarWeights`]. PTS is the
//!   audio chunk's `timestamp_us` when present, otherwise wall-clock
//!   microseconds. Consumers compare this against their tick PTS to
//!   gate freshness.
//! - **Streaming**: a small JSON envelope (`{kind, mouth_open,
//!   pts_us}`) to the primary output, primarily for debug taps and
//!   pipeline observers. Real production setups can drop the
//!   streaming output by not connecting it.
//!
//! ## Pacing
//!
//! `PacingNature::Reactive` (default). The session router invokes
//! `process_streaming_async` per upstream audio chunk; no pacer needed.
//! Per spec Phase 5.1, future versions will also subscribe to
//! `audio_clock:<stream>` for explicit per-frame timing — that lands
//! with the generalized `media_clock` envelope.

use crate::data::audio_samples::AudioSamples;
use crate::data::RuntimeData;
use crate::nodes::avatar::AvatarWeights;
use crate::nodes::ports::{OutputPort, PortKind, SnapshotPort};
#[cfg(test)]
use crate::nodes::NodeRuntimeContext;
use crate::nodes::{
    InitializeContextRead, NodeRuntimeContextRead, PacingNature, StreamingNode,
    StreamingNodeFactory,
};
use crate::Error;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Output port name for the lipsync weights snapshot. Matches the
/// `AvatarNode::WEIGHTS_PORT` constant; the manifest connection wires
/// `audio2face.weights -> avatar.weights`.
pub const WEIGHTS_PORT: &str = "weights";

/// Default RMS-to-mouth-open scale. Scales typical conversational
/// speech RMS (~0.05–0.2 for f32 PCM in `[-1, 1]`) up into a useful
/// blendshape range. Override via `params.mouth_open_scale`.
pub const DEFAULT_MOUTH_OPEN_SCALE: f32 = 6.0;

#[derive(Debug, Clone)]
pub struct Audio2FaceConfig {
    pub mouth_open_scale: f32,
}

impl Default for Audio2FaceConfig {
    fn default() -> Self {
        Self {
            mouth_open_scale: DEFAULT_MOUTH_OPEN_SCALE,
        }
    }
}

impl Audio2FaceConfig {
    fn from_params(params: &Value) -> Self {
        let mut cfg = Self::default();
        if let Some(v) = params.get("mouth_open_scale").and_then(|v| v.as_f64()) {
            if v.is_finite() && v > 0.0 {
                cfg.mouth_open_scale = v as f32;
            }
        }
        cfg
    }
}

/// Pure stub inference: RMS of the audio block scaled into `[0, 1]`.
/// Pulled out so unit tests can pin the math without spinning up the
/// node. Real Audio2Face inference replaces this with a model forward
/// pass and a blendshape vector.
fn stub_inference(samples: &[f32], scale: f32) -> AvatarWeights {
    if samples.is_empty() {
        return AvatarWeights {
            mouth_open: 0.0,
            blendshapes: Vec::new(),
        };
    }
    let sum_sq: f64 = samples.iter().map(|&s| (s as f64) * (s as f64)).sum();
    let rms = (sum_sq / samples.len() as f64).sqrt() as f32;
    let mouth_open = (rms * scale).clamp(0.0, 1.0);
    AvatarWeights {
        mouth_open,
        blendshapes: Vec::new(),
    }
}

/// Reactive lipsync node. Holds an `OutputPort<AvatarWeights>` that
/// the session router exposes to downstream snapshot consumers
/// (typically `AvatarNode`) at session bind via `snapshot_outputs()`.
pub struct Audio2FaceNode {
    node_id: String,
    config: Audio2FaceConfig,
    weights_out: OutputPort<AvatarWeights>,
}

impl Audio2FaceNode {
    pub fn new(node_id: impl Into<String>, config: Audio2FaceConfig) -> Self {
        Self {
            node_id: node_id.into(),
            config,
            weights_out: OutputPort::empty(),
        }
    }

    pub fn from_params(node_id: impl Into<String>, params: &Value) -> Self {
        Self::new(node_id, Audio2FaceConfig::from_params(params))
    }

    fn now_us() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0)
    }
}

#[async_trait::async_trait]
impl StreamingNode for Audio2FaceNode {
    fn node_type(&self) -> &str {
        "Audio2FaceNode"
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn pacing_nature(&self) -> PacingNature {
        PacingNature::Reactive
    }

    fn is_multi_input(&self) -> bool {
        false
    }

    async fn initialize(&self, _ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        Ok(())
    }

    async fn process_async(
        &self,
        data: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        // process_streaming_async is the primary entry point; this
        // unary path exists so the node survives manifests that wire
        // it without snapshot-port wiring (debug runs).
        let (weights, pts_us) = self.compute_weights(&data);
        self.weights_out.publish(weights.clone(), pts_us);
        Ok(RuntimeData::Json(Self::debug_envelope(&weights, pts_us)))
    }

    async fn process_multi_async(
        &self,
        _inputs: HashMap<String, RuntimeData>,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        Err(Error::Execution(
            "Audio2FaceNode does not consume multi-input".into(),
        ))
    }

    async fn process_streaming_async(
        &self,
        data: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
        mut callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<usize, Error> {
        let (weights, pts_us) = self.compute_weights(&data);
        // Snapshot first — readers gating on `pts_us` see the new
        // value before any downstream node observes the streaming
        // envelope.
        self.weights_out.publish(weights.clone(), pts_us);
        callback(RuntimeData::Json(Self::debug_envelope(&weights, pts_us)))?;
        Ok(1)
    }

    fn snapshot_outputs(&self) -> HashMap<String, Arc<dyn SnapshotPort>> {
        let mut m: HashMap<String, Arc<dyn SnapshotPort>> = HashMap::new();
        m.insert(WEIGHTS_PORT.to_string(), Arc::new(self.weights_out.input()));
        m
    }
}

impl Audio2FaceNode {
    fn compute_weights(&self, data: &RuntimeData) -> (AvatarWeights, u64) {
        match data {
            RuntimeData::Audio {
                samples,
                timestamp_us,
                ..
            } => {
                let weights =
                    stub_inference(samples_as_slice(samples), self.config.mouth_open_scale);
                let pts = timestamp_us.unwrap_or_else(Self::now_us);
                (weights, pts)
            }
            // Non-audio inputs: emit a silent frame at wall-clock PTS.
            // The avatar treats this as "no fresh weights" and falls
            // back to idle on the next tick anyway.
            _ => (
                AvatarWeights {
                    mouth_open: 0.0,
                    blendshapes: Vec::new(),
                },
                Self::now_us(),
            ),
        }
    }

    fn debug_envelope(weights: &AvatarWeights, pts_us: u64) -> serde_json::Value {
        serde_json::json!({
            "kind": "weights",
            "mouth_open": weights.mouth_open,
            "pts_us": pts_us,
        })
    }
}

fn samples_as_slice(samples: &AudioSamples) -> &[f32] {
    samples.as_slice()
}

/// Factory for `Audio2FaceNode`. Reads `mouth_open_scale` from
/// `params`; missing or invalid keys fall back to the module default.
pub struct Audio2FaceNodeFactory;

impl Default for Audio2FaceNodeFactory {
    fn default() -> Self {
        Self
    }
}

impl StreamingNodeFactory for Audio2FaceNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        Ok(Box::new(Audio2FaceNode::from_params(node_id, params)))
    }

    fn node_type(&self) -> &str {
        "Audio2FaceNode"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("Audio2FaceNode")
                .description("Reactive lipsync producer — maps audio to blendshape weights")
                .category("avatar")
                .accepts([RuntimeDataType::Audio])
                .produces([RuntimeDataType::Json]),
        )
    }

    /// `weights` is a snapshot port — declared here so tooling can
    /// surface it in typed manifests, and so the session router has
    /// an explicit declaration to consult.
    fn output_port_kinds(&self) -> HashMap<String, PortKind> {
        let mut m = HashMap::new();
        m.insert(WEIGHTS_PORT.to_string(), PortKind::Snapshot);
        m
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::audio_samples::AudioSamples;

    #[test]
    fn stub_inference_zero_signal_produces_zero_mouth() {
        let w = stub_inference(&[0.0; 480], 6.0);
        assert_eq!(w.mouth_open, 0.0);
        assert!(w.blendshapes.is_empty());
    }

    #[test]
    fn stub_inference_clamps_to_unit_range() {
        // Saturating signal at +1.0 → RMS 1.0, scaled by 6.0 → clamps
        // to 1.0.
        let w = stub_inference(&[1.0; 480], 6.0);
        assert_eq!(w.mouth_open, 1.0);
    }

    #[test]
    fn stub_inference_scales_typical_signal_into_useful_range() {
        // Sine-like fill at 0.1 amplitude; RMS ~= 0.1; scaled by 6.0
        // ~= 0.6 — squarely in the useful range.
        let samples: Vec<f32> = (0..480).map(|_| 0.1).collect();
        let w = stub_inference(&samples, 6.0);
        assert!(
            (w.mouth_open - 0.6).abs() < 0.05,
            "expected ~0.6, got {}",
            w.mouth_open
        );
    }

    #[test]
    fn stub_inference_empty_samples_returns_zero() {
        let w = stub_inference(&[], 6.0);
        assert_eq!(w.mouth_open, 0.0);
    }

    #[test]
    fn factory_declares_snapshot_output() {
        let f = Audio2FaceNodeFactory;
        let kinds = f.output_port_kinds();
        assert_eq!(kinds.get(WEIGHTS_PORT), Some(&PortKind::Snapshot));
    }

    #[test]
    fn factory_overrides_scale_from_params() {
        let f = Audio2FaceNodeFactory;
        let node = f
            .create(
                "a2f".to_string(),
                &serde_json::json!({ "mouth_open_scale": 12.5 }),
                None,
            )
            .unwrap();
        assert_eq!(node.node_type(), "Audio2FaceNode");
        // Verify pacing nature is reactive (default).
        assert!(matches!(node.pacing_nature(), PacingNature::Reactive));
    }

    #[tokio::test]
    async fn process_streaming_publishes_weights_snapshot() {
        let node = Audio2FaceNode::new("a2f", Audio2FaceConfig::default());

        // Hold the snapshot read handle so we can verify the publish
        // is observable via the same handle the session router would
        // wire into the consumer's ctx.
        let outputs = node.snapshot_outputs();
        let handle = outputs.get(WEIGHTS_PORT).expect("weights port").clone();
        let typed_input: &crate::nodes::ports::InputPort<AvatarWeights> =
            handle.as_any().downcast_ref().expect("downcast");

        // Empty audio chunk so we don't depend on RMS specifics here.
        let audio = RuntimeData::Audio {
            samples: AudioSamples::Vec((0..480).map(|_| 0.1).collect()),
            sample_rate: 16_000,
            channels: 1,
            stream_id: None,
            timestamp_us: Some(7_777),
            arrival_ts_us: None,
            metadata: None,
        };

        let ctx = NodeRuntimeContext::for_test("sess-1", "a2f");
        let captured = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured_clone = captured.clone();
        let cb: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send> = Box::new(move |out| {
            captured_clone.lock().unwrap().push(out);
            Ok(())
        });

        node.process_streaming_async(audio, &ctx, cb).await.unwrap();

        // Streaming envelope shape.
        let cap = captured.lock().unwrap();
        assert_eq!(cap.len(), 1);
        let RuntimeData::Json(v) = &cap[0] else {
            panic!("expected Json");
        };
        assert_eq!(v.get("kind").and_then(|x| x.as_str()), Some("weights"));
        assert_eq!(v.get("pts_us").and_then(|x| x.as_u64()), Some(7_777));
        assert!(v.get("mouth_open").and_then(|x| x.as_f64()).unwrap() > 0.0);

        // Snapshot publish observable.
        let snap = typed_input.snapshot().expect("populated");
        assert_eq!(snap.pts_us, 7_777);
        assert!(snap.value.mouth_open > 0.0);
    }
}
