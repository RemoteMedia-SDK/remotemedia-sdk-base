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
// `parking_lot::Mutex` chosen deliberately: all locking sites in this
// file cover pure synchronous CPU work (rubato resampling, atomic config
// field updates). We never hold these locks across an `.await`, so the
// async-aware `tokio::sync::Mutex` was pure overhead + priority-inversion
// risk. parking_lot is lock-free fast-path + much cheaper contended case.
use parking_lot::Mutex;

pub struct ResampleStreamingNode {
    inner: Mutex<FastResampleNode>,
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

        // Lock and process - need to chunk input for FftFixedIn resampler
        let mut inner = self.inner.lock();

        // FftFixedIn requires fixed chunk sizes - split input into chunks
        let chunk_size = 1024; // Match Medium quality chunk size
        let total_samples = f32_samples.len();

        if total_samples <= chunk_size {
            // Small enough to process directly
            let audio_data = AudioData::new(
                crate::audio::buffer::AudioBuffer::new_f32(f32_samples.into_vec()),
                input_sample_rate,
                input_channels as usize,
            );

            let resampled = inner.process_audio(audio_data)?;
            drop(inner); // Release lock early

            // Convert f32 samples to bytes
            let f32_samples = resampled
                .buffer
                .as_f32()
                .ok_or_else(|| Error::Execution("Resampler output must be F32".into()))?;

            let _num_samples = f32_samples.len() as u64; // TODO: Use for metadata

            let _bytes: Vec<u8> = f32_samples.iter().flat_map(|&f| f.to_le_bytes()).collect(); // TODO: Support byte format output

            // Return resampled audio as RuntimeData
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

        // Large buffer - process in chunks. Downgraded to `debug!` since
        // this fires once per long input and was otherwise noisy in hot
        // streaming paths.
        tracing::debug!(
            "Resampling large buffer: {} samples in chunks of {}",
            total_samples,
            chunk_size
        );
        // Pre-size the output Vec. FftFixedIn output length is
        // `chunk_size * target/source` per input chunk, rounded; we
        // estimate the total with ceil(total / chunk_size) * chunk_size
        // upper-bounded at input size * 2 to avoid pathological growth.
        // A small over-estimate is fine — `extend_from_slice` uses the
        // reserve, no reallocation in steady state.
        let num_chunks = (total_samples + chunk_size - 1) / chunk_size;
        let mut all_output_samples: Vec<f32> = Vec::with_capacity(num_chunks * chunk_size * 2);
        // Reusable scratch buffer for the padded last chunk. Only grows
        // to chunk_size once; subsequent iterations reuse the alloc.
        let mut chunk_vec: Vec<f32> = Vec::with_capacity(chunk_size);

        for chunk_start in (0..total_samples).step_by(chunk_size) {
            let chunk_end = (chunk_start + chunk_size).min(total_samples);
            let chunk_samples = &f32_samples[chunk_start..chunk_end];

            chunk_vec.clear();
            chunk_vec.extend_from_slice(chunk_samples);
            if chunk_vec.len() < chunk_size {
                chunk_vec.resize(chunk_size, 0.0);
            }

            // FastResampleNode::process_audio takes ownership of the
            // AudioBuffer, so we must hand it an owned Vec. Swap our
            // scratch Vec out and replace with an empty one that will
            // grow back to capacity on the next iteration; this keeps
            // the allocation churn to one realloc per call rather than
            // per chunk.
            let owned_chunk = std::mem::take(&mut chunk_vec);
            chunk_vec = Vec::with_capacity(chunk_size);

            let chunk_data = AudioData::new(
                crate::audio::buffer::AudioBuffer::new_f32(owned_chunk),
                input_sample_rate,
                input_channels as usize,
            );

            let resampled_chunk = inner.process_audio(chunk_data)?;
            let chunk_out = resampled_chunk
                .buffer
                .as_f32()
                .ok_or_else(|| Error::Execution("Resampler output must be F32".into()))?;

            all_output_samples.extend_from_slice(chunk_out);
        }

        drop(inner); // Release lock

        let num_samples = all_output_samples.len();
        tracing::debug!(
            "Resampling complete: {} input samples -> {} output samples",
            total_samples,
            num_samples
        );

        // Use stored target rate
        let target_rate = self.target_rate;

        // Return resampled audio (carry-through metadata + stream_id)
        Ok(RuntimeData::Audio {
            samples: all_output_samples.into(),
            sample_rate: target_rate,
            channels: input_channels,
            stream_id: out_stream_id,
            timestamp_us: in_ts_us,
            arrival_ts_us: in_arrival_us,
            metadata: in_meta,
        })
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
    async fn ensure_initialized(
        &self,
        source_rate: u32,
        target_rate: u32,
        channels: usize,
    ) -> Result<()> {
        let mut inner = self.inner.lock();
        if inner.is_some() {
            return Ok(()); // Already initialized
        }

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
