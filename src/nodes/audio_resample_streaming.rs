/// Streaming wrapper for FastResampleNode
///
/// Adapts the synchronous FastResampleNode (FastAudioNode trait) to work
/// in the async streaming pipeline (AsyncStreamingNode trait).
use crate::data::RuntimeData;
use crate::error::{Error, Result};
use crate::nodes::{
    audio::{FastResampleNode, ResampleQuality},
    AsyncStreamingNode,
};
use async_trait::async_trait;
use remotemedia_traits::runtime_context::{InitializeContextRead, NodeRuntimeContextRead};
use remotemedia_types::{HeapOutputAllocator, OutputAllocator};
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

impl ResampleStreamingNode {
    fn process_allocated(
        &self,
        data: RuntimeData,
        allocator: &dyn OutputAllocator,
    ) -> Result<RuntimeData> {
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
                    samples: allocator
                        .allocate_audio(0)
                        .map_err(|e| Error::Execution(format!("resampler output allocation: {e}")))?
                        .freeze()
                        .into(),
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
        let samples = inner.process_shared(
            f32_samples,
            input_sample_rate,
            input_channels as usize,
            allocator,
        )?;
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

    fn stream_allocated<F>(
        &self,
        data: RuntimeData,
        allocator: &dyn OutputAllocator,
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
                let output = inner.flush_shared(allocator)?;
                let mut tail = self.tail.lock().take();
                if let Some(RuntimeData::Audio { samples, .. }) = &mut tail {
                    *samples = output.into();
                }
                tail
            };
            if let Some(tail) = tail {
                callback(tail)?;
                count += 1;
            }
            callback(data)?;
        } else {
            callback(self.process_allocated(data, allocator)?)?;
        }
        Ok(count + 1)
    }
}

#[async_trait]
impl AsyncStreamingNode for ResampleStreamingNode {
    fn node_type(&self) -> &str {
        "FastResampleNode"
    }
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        self.process_allocated(data, &HeapOutputAllocator)
    }
    async fn process_with_ctx(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData> {
        if !matches!(data, RuntimeData::Audio { .. }) {
            return Ok(data);
        }
        let allocator = ctx
            .output_allocator()
            .ok_or_else(|| Error::Execution("resampler requires an output allocator".into()))?;
        self.process_allocated(data, allocator.as_ref())
    }
    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        _session_id: Option<String>,
        callback: F,
    ) -> Result<usize>
    where
        F: FnMut(RuntimeData) -> Result<()> + Send,
    {
        self.stream_allocated(data, &HeapOutputAllocator, callback)
    }
    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        callback: F,
    ) -> Result<usize>
    where
        F: FnMut(RuntimeData) -> Result<()> + Send,
    {
        let allocator = ctx
            .output_allocator()
            .ok_or_else(|| Error::Execution("resampler requires an output allocator".into()))?;
        self.stream_allocated(data, allocator.as_ref(), callback)
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
    inner: Mutex<Option<ResampleStreamingNode>>,
    /// Resolved target rate (set during initialization or from config)
    resolved_target_rate: Mutex<Option<u32>>,
    /// Resolved source rate (detected from first chunk)
    resolved_source_rate: Mutex<Option<u32>>,
    /// Resolved channels (detected from first chunk)
    resolved_channels: Mutex<Option<usize>>,
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
    fn ensure_initialized(
        &self,
        source_rate: u32,
        target_rate: u32,
        channels: usize,
    ) -> Result<()> {
        let mut inner = self.inner.lock();
        if let Some(existing) = inner.as_ref() {
            if existing.target_rate == target_rate {
                return Ok(());
            }
            if !existing.inner.lock().can_reconfigure() {
                return Err(Error::Execution(
                    "resampler target rate changed with an unfinished utterance; flush before reconfiguration".into(),
                ));
            }
        }

        // Store resolved values
        *self.resolved_source_rate.lock() = Some(source_rate);
        *self.resolved_channels.lock() = Some(channels);

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

        *inner = Some(
            ResampleStreamingNode::new(resampler, target_rate)
                .with_output_stream_id(self.config.output_stream_id.clone()),
        );
        Ok(())
    }
}

impl AutoResampleStreamingNode {
    fn prepare(&self, data: &RuntimeData) -> Result<()> {
        if let RuntimeData::Audio {
            sample_rate,
            channels,
            ..
        } = data
        {
            let source = self.config.source_rate.unwrap_or(*sample_rate);
            let target = self
                .resolved_target_rate
                .lock()
                .or(self.config.target_rate)
                .unwrap_or(*sample_rate);
            let channels = self.config.channels.unwrap_or(*channels as usize);
            self.ensure_initialized(source, target, channels)?;
        }
        Ok(())
    }
    fn process_allocated(
        &self,
        data: RuntimeData,
        allocator: &dyn OutputAllocator,
    ) -> Result<RuntimeData> {
        self.prepare(&data)?;
        match self.inner.lock().as_ref() {
            Some(inner) => inner.process_allocated(data, allocator),
            None => Ok(data),
        }
    }
    fn stream_allocated<F>(
        &self,
        data: RuntimeData,
        allocator: &dyn OutputAllocator,
        mut callback: F,
    ) -> Result<usize>
    where
        F: FnMut(RuntimeData) -> Result<()> + Send,
    {
        self.prepare(&data)?;
        match self.inner.lock().as_ref() {
            Some(inner) => inner.stream_allocated(data, allocator, callback),
            None => {
                callback(data)?;
                Ok(1)
            }
        }
    }
}
#[async_trait]
impl AsyncStreamingNode for AutoResampleStreamingNode {
    fn node_type(&self) -> &str {
        "FastResampleNode"
    }
    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        self.process_allocated(data, &HeapOutputAllocator)
    }
    async fn process_with_ctx(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData> {
        if !matches!(data, RuntimeData::Audio { .. }) {
            return Ok(data);
        }
        let allocator = ctx
            .output_allocator()
            .ok_or_else(|| Error::Execution("resampler requires an output allocator".into()))?;
        self.process_allocated(data, allocator.as_ref())
    }
    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        _session_id: Option<String>,
        callback: F,
    ) -> Result<usize>
    where
        F: FnMut(RuntimeData) -> Result<()> + Send,
    {
        self.stream_allocated(data, &HeapOutputAllocator, callback)
    }
    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        callback: F,
    ) -> Result<usize>
    where
        F: FnMut(RuntimeData) -> Result<()> + Send,
    {
        let allocator = ctx
            .output_allocator()
            .ok_or_else(|| Error::Execution("resampler requires an output allocator".into()))?;
        self.stream_allocated(data, allocator.as_ref(), callback)
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
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData> {
        self.inner.process_with_ctx(data, ctx).await
    }

    async fn process_multi_async(
        &self,
        inputs: HashMap<String, RuntimeData>,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData> {
        if let Some((_name, data)) = inputs.into_iter().next() {
            self.inner.process_with_ctx(data, ctx).await
        } else {
            Err(Error::Execution("No input data provided".into()))
        }
    }

    async fn process_streaming_async(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        callback: Box<dyn FnMut(RuntimeData) -> Result<()> + Send>,
    ) -> Result<usize> {
        self.inner
            .process_streaming_with_ctx(data, ctx, callback)
            .await
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
        // Extract audio constraints from upstream output
        if let Some(MediaConstraints::Audio(audio)) = upstream_caps.default_output() {
            // Extract sample rate
            if let Some(ConstraintValue::Exact(rate)) = &audio.sample_rate {
                let inner = self.inner.clone();
                let rate = *rate;

                // Use tokio::spawn to run async code from sync context
                // This is safe because we're just setting values, not blocking
                tokio::spawn(async move {
                    inner.set_source_rate(rate).await;
                });

                tracing::info!(
                    "[{}] Configured source rate from upstream: {}Hz",
                    self.inner.node_id,
                    rate
                );
            }

            // Extract channel count
            if let Some(ConstraintValue::Exact(channels)) = &audio.channels {
                let inner = self.inner.clone();
                let channels = *channels as usize;

                tokio::spawn(async move {
                    inner.set_channels(channels).await;
                });

                tracing::debug!(
                    "[{}] Configured channels from upstream: {}",
                    self.inner.node_id,
                    channels
                );
            }
        }

        Ok(())
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
