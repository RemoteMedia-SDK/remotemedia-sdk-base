use crate::audio::buffer::AudioData;
/// Streaming wrapper for FastResampleNode
///
/// Adapts the synchronous FastResampleNode (FastAudioNode trait) to work
/// in the async streaming pipeline (AsyncStreamingNode trait).
use crate::data::RuntimeData;
use crate::error::{Error, Result};
use crate::nodes::{
    audio::{FastAudioNode, FastResampleNode, ResampleQuality},
    AsyncStreamingNode,
};
use async_trait::async_trait;
use remotemedia_traits::runtime_context::{InitializeContextRead, NodeRuntimeContextRead};
use serde_json::Value;
// `parking_lot::Mutex` chosen deliberately: all locking sites in this
// file cover pure synchronous CPU work (rubato resampling, atomic config
// field updates). We never hold these locks across an `.await`, so the
// async-aware `tokio::sync::Mutex` was pure overhead + priority-inversion
// risk. parking_lot is lock-free fast-path + much cheaper contended case.
use parking_lot::Mutex;

pub struct ResampleStreamingNode {
    inner: Mutex<FastResampleNode>,
    tail: Mutex<Option<RuntimeData>>,
    speech_epoch: Mutex<u64>,
    target_rate: u32,
    /// When set, force-stamps this `stream_id` onto every emitted Audio
    /// chunk regardless of the input's value. Used by the avatar pipeline
    /// to route the TTS sink's output to a dedicated WebRTC track. When
    /// `None`, the input's `stream_id` is preserved unchanged.
    output_stream_id: Option<String>,
}

impl ResampleStreamingNode {
    pub fn new(inner: FastResampleNode, target_rate: u32) -> Self {
        Self {
            inner: Mutex::new(inner),
            tail: Mutex::new(None),
            speech_epoch: Mutex::new(0),
            target_rate,
            output_stream_id: None,
        }
    }

    pub fn with_output_stream_id(mut self, stream_id: Option<String>) -> Self {
        self.output_stream_id = stream_id;
        self
    }
}

#[async_trait]
impl AsyncStreamingNode for ResampleStreamingNode {
    fn node_type(&self) -> &str {
        "FastResampleNode"
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        // if not audio, passthrough
        if data.data_type() != "audio" {
            return Ok(data);
        }
        // Extract audio data + carry-through fields. We move out of the
        // Audio variant so `stream_id`, `timestamp_us`, `arrival_ts_us`,
        // and `metadata` propagate to the resampled output. Without this
        // they were silently zeroed, which broke pts-anchored consumers
        // like Audio2FaceLipSyncNode (it reads `metadata.pts_us` to align
        // blendshape pts to upstream content time) and forced every
        // downstream chunk onto the `default` WebRTC track.
        let (
            f32_samples,
            input_sample_rate,
            input_channels,
            in_stream_id,
            in_ts_us,
            in_arrival_us,
            in_meta,
        ) = match data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                stream_id,
                timestamp_us,
                arrival_ts_us,
                metadata,
            } => (
                samples,
                sample_rate,
                channels,
                stream_id,
                timestamp_us,
                arrival_ts_us,
                metadata,
            ),
            other => return Ok(other),
        };
        let out_stream_id = self.output_stream_id.clone().or(in_stream_id);

        // The resampler already buffers arbitrary input sizes. Padding each
        // incoming chunk here lengthened speech and moved face timestamps backwards.
        let mut inner = self.inner.lock();
        if let Some(epoch) = in_meta
            .as_ref()
            .and_then(|m| m.get("speech_epoch"))
            .and_then(Value::as_u64)
        {
            let mut current = self.speech_epoch.lock();
            if epoch > *current {
                inner.reset();
                self.tail.lock().take();
                *current = epoch;
            } else if epoch < *current {
                return Ok(RuntimeData::Audio {
                    samples: Vec::new().into(),
                    sample_rate: self.target_rate,
                    channels: input_channels,
                    stream_id: out_stream_id,
                    timestamp_us: in_ts_us,
                    arrival_ts_us: in_arrival_us,
                    metadata: in_meta,
                });
            }
        }
        let buffered_us = inner.buffered_duration_us();
        let resampled = inner.process_audio(AudioData::new(
            crate::audio::buffer::AudioBuffer::new_f32(f32_samples.into_vec()),
            input_sample_rate,
            input_channels as usize,
        ))?;
        let samples = resampled.buffer.as_f32().unwrap().to_vec();
        let timestamp_us = in_ts_us.map(|pts| pts.saturating_sub(buffered_us));
        let mut metadata = in_meta;
        if let Some(meta) = metadata.as_mut() {
            if let Some(pts) = meta.get("pts_us").and_then(Value::as_u64) {
                meta["pts_us"] = pts.saturating_sub(buffered_us).into();
            }
        }
        let duration_us =
            samples.len() as u64 * 1_000_000 / input_channels as u64 / self.target_rate as u64;
        let mut tail_meta = metadata.clone();
        if let Some(meta) = tail_meta.as_mut() {
            if let Some(pts) = meta.get("pts_us").and_then(Value::as_u64) {
                meta["pts_us"] = (pts + duration_us).into();
            }
        }
        *self.tail.lock() = Some(RuntimeData::Audio {
            samples: Vec::new().into(),
            sample_rate: self.target_rate,
            channels: input_channels,
            stream_id: out_stream_id.clone(),
            timestamp_us: timestamp_us.map(|pts| pts + duration_us),
            arrival_ts_us: in_arrival_us,
            metadata: tail_meta,
        });
        Ok(RuntimeData::Audio {
            samples: samples.into(),
            sample_rate: self.target_rate,
            channels: input_channels,
            stream_id: out_stream_id,
            timestamp_us,
            arrival_ts_us: in_arrival_us,
            metadata,
        })
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
        let envelope = match &data {
            RuntimeData::Json(v) => Some(v.clone()),
            RuntimeData::Text(t) => {
                serde_json::from_str::<Value>(crate::data::split_text_str(t).1).ok()
            }
            _ => None,
        };
        let mut count = 0;
        if let Some(end) = envelope.filter(|v| v["kind"] == "speech_end") {
            // Keep epoch validation and flushing atomic with audio ingestion.
            // A delayed end marker must never flush a newer utterance.
            let tail = {
                let mut inner = self.inner.lock();
                let mut current = self.speech_epoch.lock();
                let epoch = end["speech_epoch"].as_u64().unwrap_or(*current);
                if epoch < *current {
                    return Ok(0);
                }
                if epoch > *current {
                    inner.reset();
                    self.tail.lock().take();
                    *current = epoch;
                }
                let output = inner.flush_audio()?;
                let mut tail = self.tail.lock().take();
                if let Some(RuntimeData::Audio { samples, .. }) = &mut tail {
                    *samples = output.buffer.as_f32().unwrap().to_vec().into();
                }
                tail
            };
            if let Some(tail) = tail {
                callback(tail)?;
                count += 1;
            }
            callback(data)?;
        } else {
            callback(self.process(data).await?)?;
        }
        Ok(count + 1)
    }
}

// =============================================================================
// AutoResampleStreamingNode - Lazy initialization with auto-configuration
// =============================================================================

/// Configuration for auto-resampling behavior
#[derive(Debug, Clone)]
pub struct AutoResampleConfig {
    /// Source sample rate (None = detect from first audio chunk)
    pub source_rate: Option<u32>,
    /// Target sample rate (None = use configured value or passthrough)
    pub target_rate: Option<u32>,
    /// Resampling quality
    pub quality: ResampleQuality,
    /// Number of channels (None = detect from first audio chunk)
    pub channels: Option<usize>,
    /// When set, force-stamps this `stream_id` onto every emitted Audio
    /// chunk regardless of the input's value. `None` preserves the
    /// input's `stream_id` unchanged. Used by the avatar pipeline to
    /// route the TTS sink onto a dedicated WebRTC track.
    pub output_stream_id: Option<String>,
}

impl Default for AutoResampleConfig {
    fn default() -> Self {
        Self {
            source_rate: None,
            target_rate: None,
            quality: ResampleQuality::Medium,
            channels: None,
            output_stream_id: None,
        }
    }
}

/// Auto-configuring resample node with lazy initialization.
///
/// This node supports automatic sample rate detection and configuration:
/// - `source_rate`: If None, detected from first incoming audio chunk
/// - `target_rate`: If None, defaults to passthrough (no resampling)
///
/// The resampler is created lazily on the first `process()` call when we
/// have enough information to configure it.
///
/// # Capability Behavior
///
/// This node has `Adaptive` capability behavior:
/// - Input: Accepts any sample rate (8kHz-192kHz)
/// - Output: Adapts to downstream requirements during capability resolution
///
/// When used without explicit `target_rate`, the resolver's reverse pass
/// will set the output rate based on what downstream nodes require.
pub struct AutoResampleStreamingNode {
    node_id: String,
    config: AutoResampleConfig,
    /// Lazily initialized resampler
    inner: Mutex<Option<FastResampleNode>>,
    /// Resolved target rate (set during initialization or from config)
    resolved_target_rate: Mutex<Option<u32>>,
    /// Resolved source rate (detected from first chunk)
    resolved_source_rate: Mutex<Option<u32>>,
    /// Resolved channels (detected from first chunk)
    resolved_channels: Mutex<Option<usize>>,
    /// `(source, target, channels)` already set up, so a passthrough
    /// (source == target, no resampler built) is decided once, not per chunk.
    set_up_for: Mutex<Option<(u32, u32, usize)>>,
}

impl AutoResampleStreamingNode {
    pub fn new(node_id: String, config: AutoResampleConfig) -> Self {
        let resolved_target = config.target_rate;
        let resolved_source = config.source_rate;
        let resolved_channels = config.channels;

        Self {
            node_id,
            config,
            inner: Mutex::new(None),
            resolved_target_rate: Mutex::new(resolved_target),
            resolved_source_rate: Mutex::new(resolved_source),
            resolved_channels: Mutex::new(resolved_channels),
            set_up_for: Mutex::new(None),
        }
    }

    /// Set the target sample rate (called during capability resolution).
    pub async fn set_target_rate(&self, rate: u32) {
        *self.resolved_target_rate.lock() = Some(rate);
    }

    /// Get the resolved target rate.
    pub async fn get_target_rate(&self) -> Option<u32> {
        *self.resolved_target_rate.lock()
    }

    /// Set the source sample rate from upstream discovery (spec 025).
    ///
    /// Called by `configure_from_upstream()` when upstream RuntimeDiscovered
    /// node reports its actual output sample rate.
    pub async fn set_source_rate(&self, rate: u32) {
        tracing::debug!(
            "[{}] Setting source rate from upstream: {}Hz",
            self.node_id,
            rate
        );
        *self.resolved_source_rate.lock() = Some(rate);
    }

    /// Get the resolved source rate.
    pub async fn get_source_rate(&self) -> Option<u32> {
        *self.resolved_source_rate.lock()
    }

    /// Set the channel count from upstream discovery (spec 025).
    pub async fn set_channels(&self, channels: usize) {
        *self.resolved_channels.lock() = Some(channels);
    }

    /// Initialize the resampler with detected/configured rates.
    async fn ensure_initialized(
        &self,
        source_rate: u32,
        target_rate: u32,
        channels: usize,
    ) -> Result<()> {
        let mut inner = self.inner.lock();
        if inner.is_some() || *self.set_up_for.lock() == Some((source_rate, target_rate, channels))
        {
            return Ok(()); // Already initialized
        }
        *self.set_up_for.lock() = Some((source_rate, target_rate, channels));

        // Store resolved values
        *self.resolved_source_rate.lock() = Some(source_rate);
        *self.resolved_channels.lock() = Some(channels);

        // If source and target are the same, we can skip resampling
        if source_rate == target_rate {
            tracing::info!(
                "[{}] No resampling needed (source == target = {}Hz)",
                self.node_id,
                source_rate
            );
            return Ok(());
        }

        tracing::info!(
            "[{}] Creating resampler: {}Hz -> {}Hz, {} channels, quality: {:?}",
            self.node_id,
            source_rate,
            target_rate,
            channels,
            self.config.quality
        );

        let resampler =
            FastResampleNode::new(source_rate, target_rate, self.config.quality, channels)?;

        *inner = Some(resampler);
        Ok(())
    }
}

#[async_trait]
impl AsyncStreamingNode for AutoResampleStreamingNode {
    fn node_type(&self) -> &str {
        "FastResampleNode"
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        // Passthrough non-audio data
        if data.data_type() != "audio" {
            return Ok(data);
        }

        // Extract audio data + carry-through fields. See ResampleStreamingNode
        // for why we move these out (preserves pts_us metadata for the
        // lipsync chain and lets us route by stream_id).
        let (
            f32_samples,
            input_sample_rate,
            input_channels,
            in_stream_id,
            in_ts_us,
            in_arrival_us,
            in_meta,
        ) = match data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                stream_id,
                timestamp_us,
                arrival_ts_us,
                metadata,
            } => (
                samples,
                sample_rate,
                channels as usize,
                stream_id,
                timestamp_us,
                arrival_ts_us,
                metadata,
            ),
            other => return Ok(other),
        };
        let out_stream_id = self.config.output_stream_id.clone().or(in_stream_id);

        // Determine source rate (from config or incoming data)
        let source_rate = self.config.source_rate.unwrap_or(input_sample_rate);

        // Determine target rate (from config, resolved value, or passthrough)
        let target_rate = {
            let resolved = *self.resolved_target_rate.lock();
            resolved
                .or(self.config.target_rate)
                .unwrap_or(input_sample_rate)
        };

        // Determine channels (from config or incoming data)
        let channels = self.config.channels.unwrap_or(input_channels);

        // Ensure resampler is initialized
        self.ensure_initialized(source_rate, target_rate, channels)
            .await?;

        // If no resampling needed, passthrough (carry-through fields)
        if source_rate == target_rate {
            return Ok(RuntimeData::Audio {
                samples: f32_samples,
                sample_rate: target_rate,
                channels: channels as u32,
                stream_id: out_stream_id,
                timestamp_us: in_ts_us,
                arrival_ts_us: in_arrival_us,
                metadata: in_meta,
            });
        }

        // Process through resampler
        let mut inner_guard = self.inner.lock();
        let inner = inner_guard.as_mut().ok_or_else(|| {
            Error::Execution(format!("[{}] Resampler not initialized", self.node_id))
        })?;

        let chunk_size = 1024;
        let total_samples = f32_samples.len();

        if total_samples <= chunk_size {
            // Small buffer - process directly
            let audio_data = AudioData::new(
                crate::audio::buffer::AudioBuffer::new_f32(f32_samples.into_vec()),
                input_sample_rate,
                channels,
            );

            let resampled = inner.process_audio(audio_data)?;
            let f32_samples = resampled
                .buffer
                .as_f32()
                .ok_or_else(|| Error::Execution("Resampler output must be F32".into()))?;

            return Ok(RuntimeData::Audio {
                samples: f32_samples.to_vec().into(),
                sample_rate: resampled.sample_rate,
                channels: resampled.channels as u32,
                stream_id: out_stream_id,
                timestamp_us: in_ts_us,
                arrival_ts_us: in_arrival_us,
                metadata: in_meta,
            });
        }

        // Large buffer - process in chunks
        let mut all_output_samples = Vec::new();

        for chunk_start in (0..total_samples).step_by(chunk_size) {
            let chunk_end = (chunk_start + chunk_size).min(total_samples);
            let chunk_samples = &f32_samples[chunk_start..chunk_end];

            let mut chunk_vec = chunk_samples.to_vec();
            if chunk_vec.len() < chunk_size {
                chunk_vec.resize(chunk_size, 0.0);
            }

            let chunk_data = AudioData::new(
                crate::audio::buffer::AudioBuffer::new_f32(chunk_vec),
                input_sample_rate,
                channels,
            );

            let resampled_chunk = inner.process_audio(chunk_data)?;
            let chunk_out = resampled_chunk
                .buffer
                .as_f32()
                .ok_or_else(|| Error::Execution("Resampler output must be F32".into()))?;

            all_output_samples.extend_from_slice(chunk_out);
        }

        Ok(RuntimeData::Audio {
            samples: all_output_samples.into(),
            sample_rate: target_rate,
            channels: channels as u32,
            stream_id: out_stream_id,
            timestamp_us: in_ts_us,
            arrival_ts_us: in_arrival_us,
            metadata: in_meta,
        })
    }
}

// =============================================================================
// AutoResampleStreamingNodeWrapper - StreamingNode wrapper with configure_from_upstream
// =============================================================================

use crate::capabilities::{ConstraintValue, MediaCapabilities, MediaConstraints};
use crate::nodes::streaming_node::StreamingNode;
use std::collections::HashMap;
use std::sync::Arc;

/// Wrapper that implements `StreamingNode` for `AutoResampleStreamingNode` with
/// capability re-propagation support (spec 025).
///
/// This wrapper enables the resample node to receive upstream capability updates
/// via `configure_from_upstream()` when a RuntimeDiscovered upstream node
/// (like MicInput) discovers its actual sample rate.
pub struct AutoResampleStreamingNodeWrapper {
    inner: Arc<AutoResampleStreamingNode>,
}

impl AutoResampleStreamingNodeWrapper {
    pub fn new(node: AutoResampleStreamingNode) -> Self {
        Self {
            inner: Arc::new(node),
        }
    }

    /// Get reference to inner node for capability queries.
    pub fn inner(&self) -> &AutoResampleStreamingNode {
        &self.inner
    }
}

#[async_trait]
impl StreamingNode for AutoResampleStreamingNodeWrapper {
    fn node_type(&self) -> &str {
        self.inner.node_type()
    }

    fn node_id(&self) -> &str {
        &self.inner.node_id
    }

    async fn initialize(&self, _ctx: &dyn InitializeContextRead) -> Result<()> {
        Ok(()) // Lazy initialization happens on first process()
    }

    async fn process_async(
        &self,
        data: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData> {
        self.inner.process(data).await
    }

    async fn process_multi_async(
        &self,
        inputs: HashMap<String, RuntimeData>,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData> {
        if let Some((_name, data)) = inputs.into_iter().next() {
            self.inner.process(data).await
        } else {
            Err(Error::Execution("No input data provided".into()))
        }
    }

    fn is_multi_input(&self) -> bool {
        false
    }

    /// Configure resample node based on upstream capabilities (spec 025).
    ///
    /// Called after a RuntimeDiscovered upstream node (e.g., MicInput) reports
    /// its actual output capabilities. Extracts sample rate and channels from
    /// the upstream output and configures this resample node accordingly.
    ///
    /// # Example
    ///
    /// If upstream MicInput discovers it outputs 48kHz stereo audio, this
    /// method will call `set_source_rate(48000)` and `set_channels(2)` on
    /// the inner resample node, ensuring proper resampling when data arrives.
    fn configure_from_upstream(&self, upstream_caps: &MediaCapabilities) -> Result<()> {
        // Set directly (parking_lot locks): the handshake runs before any
        // data flows, and a background task could lose the race with the
        // first chunk.
        if let Some(MediaConstraints::Audio(audio)) = upstream_caps.default_output() {
            if let Some(ConstraintValue::Exact(rate)) = &audio.sample_rate {
                *self.inner.resolved_source_rate.lock() = Some(*rate);
                tracing::info!(
                    "[{}] Configured source rate from upstream: {}Hz",
                    self.inner.node_id,
                    rate
                );
            }
            if let Some(ConstraintValue::Exact(channels)) = &audio.channels {
                *self.inner.resolved_channels.lock() = Some(*channels as usize);
                tracing::debug!(
                    "[{}] Configured channels from upstream: {}",
                    self.inner.node_id,
                    channels
                );
            }
        }
        Ok(())
    }

    /// Handshake: take the target rate from what downstream accepts,
    /// keeping the source rate when it is accepted (no needless resampling)
    /// and otherwise the nearest accepted rate. Channels pass through: this
    /// node does not convert them. An explicit `target_rate` param wins, and
    /// is an error when downstream does not accept it.
    fn configure_for_downstream(
        &self,
        accepted: &MediaCapabilities,
    ) -> Result<Option<MediaCapabilities>> {
        use crate::transport::caps_handshake::{choose, describe_constraint, AudioFormat};
        let Some(MediaConstraints::Audio(acc)) = accepted.default_input() else {
            return Ok(None);
        };
        let inner = &self.inner;
        let source = inner
            .config
            .source_rate
            .or(*inner.resolved_source_rate.lock());
        let channels = inner
            .config
            .channels
            .or(*inner.resolved_channels.lock())
            .map(|c| c as u32);
        let target = match (inner.config.target_rate, &acc.sample_rate) {
            (Some(t), Some(c)) if !c.satisfies(&t) => {
                return Err(Error::Execution(format!(
                    "target_rate {t} is set, but downstream accepts {}; set target_rate to one of \
                     those or leave it unset to follow downstream",
                    describe_constraint(c, "Hz")
                )))
            }
            (Some(t), _) => Some(t),
            (None, Some(c)) => choose(source, c),
            (None, None) => source,
        };
        if inner.config.target_rate.is_none() {
            if let Some(t) = target {
                *inner.resolved_target_rate.lock() = Some(t);
                tracing::info!(
                    "[{}] Target rate from downstream: {}Hz{}",
                    inner.node_id,
                    t,
                    if source == Some(t) {
                        " (passthrough)"
                    } else {
                        ""
                    }
                );
            }
        }
        Ok(Some(
            AudioFormat {
                sample_rate: target,
                channels,
            }
            .to_caps(),
        ))
    }

    fn capability_behavior(&self) -> crate::capabilities::CapabilityBehavior {
        crate::capabilities::CapabilityBehavior::Adaptive
    }
}

#[cfg(test)]
mod speech_timeline_tests {
    use super::*;

    async fn resample(chunks: &[usize]) -> (Vec<f32>, Vec<u64>) {
        let node = ResampleStreamingNode::new(
            FastResampleNode::new(24000, 16000, ResampleQuality::Medium, 1).unwrap(),
            16000,
        );
        let mut audio = Vec::new();
        let mut timestamps = Vec::new();
        let mut input_frames = 0usize;
        for &length in chunks {
            let input = RuntimeData::Audio {
                samples: (input_frames..input_frames + length)
                    .map(|i| (i as f32 * 0.031).sin())
                    .collect::<Vec<_>>()
                    .into(),
                sample_rate: 24000,
                channels: 1,
                stream_id: None,
                timestamp_us: None,
                arrival_ts_us: None,
                metadata: Some(
                    serde_json::json!({"pts_us":input_frames as u64 * 1_000_000 / 24000, "speech_epoch":0}),
                ),
            };
            input_frames += length;
            node.process_streaming(input, None, |out| {
                if let RuntimeData::Audio {
                    samples, metadata, ..
                } = out
                {
                    if !samples.is_empty() {
                        timestamps.push(metadata.unwrap()["pts_us"].as_u64().unwrap());
                    }
                    audio.extend_from_slice(samples.as_slice());
                }
                Ok(())
            })
            .await
            .unwrap();
        }
        node.process_streaming(
            RuntimeData::Json(serde_json::json!({"kind":"speech_end", "speech_epoch":0})),
            None,
            |out| {
                if let RuntimeData::Audio {
                    samples, metadata, ..
                } = out
                {
                    if !samples.is_empty() {
                        timestamps.push(metadata.unwrap()["pts_us"].as_u64().unwrap());
                    }
                    audio.extend_from_slice(samples.as_slice());
                }
                Ok(())
            },
        )
        .await
        .unwrap();
        (audio, timestamps)
    }

    #[tokio::test]
    async fn chunk_boundaries_do_not_pad_speech_or_drop_its_tail() {
        let (whole, _) = resample(&[10010]).await;
        let (chunked, pts) = resample(&[1001; 10]).await;
        assert_eq!(whole.len(), 10010 * 16000 / 24000);
        assert_eq!(whole, chunked);
        assert!(pts.windows(2).all(|p| p[0] <= p[1]));
    }

    #[tokio::test]
    async fn stale_end_marker_does_not_flush_new_epoch() {
        let node = ResampleStreamingNode::new(
            FastResampleNode::new(24000, 16000, ResampleQuality::Medium, 1).unwrap(),
            16000,
        );
        let audio = RuntimeData::Audio {
            samples: vec![0.25; 100].into(),
            sample_rate: 24000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: Some(serde_json::json!({"speech_epoch": 2, "pts_us": 0})),
        };
        node.process(audio).await.unwrap();
        let end = |epoch| {
            RuntimeData::Json(serde_json::json!({"kind":"speech_end", "speech_epoch":epoch}))
        };
        assert_eq!(
            node.process_streaming(end(1), None, |_| panic!("stale marker emitted output"))
                .await
                .unwrap(),
            0
        );
        let mut frames = 0;
        node.process_streaming(end(2), None, |out| {
            if let RuntimeData::Audio { samples, .. } = out {
                frames += samples.len();
            }
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(frames, 100 * 16000 / 24000);
    }
}
