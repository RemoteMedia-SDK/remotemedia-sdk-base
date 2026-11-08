//! `MicInput`: a microphone as a self-driven audio source (feature
//! `microphone`).

use super::source_schema as io_schema;
use crate::audio_io::{AudioCapture, CaptureConfig, DeviceSelector};
use crate::capabilities::{
    AudioConstraints, AudioSampleFormat, CapabilityBehavior, ConstraintValue, MediaCapabilities,
    MediaConstraints,
};
use crate::data::{AudioSamples, RuntimeData};
use crate::nodes::streaming_node::{
    AsyncStreamingNode, InitializeContextRead, NodeRuntimeContextRead, StreamingNode,
    StreamingNodeFactory,
};
use crate::nodes::{PacingNature, Tick};
use crate::Error;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex;

/// Configuration for MicInputNode
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MicInputConfig {
    /// Sample rate in Hz
    #[serde(default = "default_sample_rate")]
    pub sample_rate: u32,

    /// Number of audio channels (1 = mono, 2 = stereo)
    #[serde(default = "default_channels")]
    pub channels: u16,

    /// Audio device name, index, or "default"
    #[serde(default)]
    pub device: Option<String>,

    /// Audio host/backend (alsa, pulse, coreaudio, wasapi)
    #[serde(default)]
    pub host: Option<String>,

    /// Number of samples per output chunk
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,

    /// Buffer size in milliseconds (affects latency)
    #[serde(default = "default_buffer_ms")]
    pub buffer_ms: u32,
}

fn default_sample_rate() -> u32 {
    16000
}

fn default_channels() -> u16 {
    1
}

fn default_chunk_size() -> usize {
    4000
}

fn default_buffer_ms() -> u32 {
    20
}

impl Default for MicInputConfig {
    fn default() -> Self {
        Self {
            sample_rate: default_sample_rate(),
            channels: default_channels(),
            device: None,
            host: None,
            chunk_size: default_chunk_size(),
            buffer_ms: default_buffer_ms(),
        }
    }
}

/// Streaming node for microphone input that actually captures audio
///
/// A self-driven source: it declares [`PacingNature::SourceWall`] at one tick
/// per `buffer_ms`, so the session runs it on its own clock from the moment
/// the session starts (no input needed). Each tick drains what the device
/// captured since the last one and emits it in `chunk_size` pieces; inputs
/// sent to it are ignored.
///
/// Implements RuntimeDiscovered capability behavior when device is not explicitly
/// specified. This enables two-phase capability resolution:
/// - Phase 1: Use potential_capabilities() with broad range for early validation
/// - Phase 2: After initialize(), use actual_capabilities() with discovered device caps
pub struct MicInputStreamingNode {
    node_id: String,
    config: MicInputConfig,
    capture: Mutex<Option<AudioCapture>>,
    /// Captured samples not yet emitted (less than one chunk).
    carry: std::sync::Mutex<Vec<f32>>,
    /// Device buffers lost because ticks fell behind.
    lost: std::sync::atomic::AtomicU64,
    /// Discovered device capabilities (populated during initialize)
    discovered_capabilities: Mutex<Option<MediaCapabilities>>,
}

impl MicInputStreamingNode {
    pub fn new(node_id: String, config: MicInputConfig) -> Self {
        Self {
            node_id,
            config,
            capture: Mutex::new(None),
            carry: std::sync::Mutex::new(Vec::new()),
            lost: std::sync::atomic::AtomicU64::new(0),
            discovered_capabilities: Mutex::new(None),
        }
    }

    /// Ticks per second: one per `buffer_ms` (10..=200 Hz).
    fn tick_hz(&self) -> u32 {
        (1000 / self.config.buffer_ms.max(1)).clamp(10, 200)
    }

    /// Samples per emitted chunk, rounded down to whole frames.
    fn chunk_samples(&self) -> usize {
        let ch = (self.config.channels as usize).max(1);
        let n = if self.config.chunk_size > 0 {
            self.config.chunk_size
        } else {
            4000
        };
        (n / ch).max(1) * ch
    }

    /// Append `captured` to the carry and take every whole chunk out of it.
    fn take_chunks(&self, captured: &[f32]) -> Vec<Vec<f32>> {
        let chunk = self.chunk_samples();
        let mut carry = self.carry.lock().unwrap_or_else(|p| p.into_inner());
        carry.extend_from_slice(captured);
        let whole = carry.len() / chunk * chunk;
        let out = carry[..whole].chunks(chunk).map(<[f32]>::to_vec).collect();
        carry.drain(..whole);
        out
    }

    fn audio(&self, samples: Vec<f32>) -> RuntimeData {
        RuntimeData::Audio {
            samples: AudioSamples::Vec(samples),
            sample_rate: self.config.sample_rate,
            channels: self.config.channels as u32,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        }
    }

    /// Check if this node uses RuntimeDiscovered behavior
    /// (when device is not explicitly specified or is "default")
    fn is_runtime_discovered(&self) -> bool {
        match &self.config.device {
            None => true,
            Some(d) => d.to_lowercase() == "default",
        }
    }

    /// Get potential capabilities for Phase 1 validation (broad range)
    fn potential_caps() -> MediaCapabilities {
        MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Range {
                min: 8000,
                max: 192000,
            }),
            channels: Some(ConstraintValue::Range { min: 1, max: 8 }),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        }))
    }

    /// Get configured capabilities when device is explicit
    fn configured_caps(config: &MicInputConfig) -> MediaCapabilities {
        MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(config.sample_rate)),
            channels: Some(ConstraintValue::Exact(config.channels as u32)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        }))
    }
}

#[async_trait::async_trait]
impl AsyncStreamingNode for MicInputStreamingNode {
    fn node_type(&self) -> &str {
        "MicInput"
    }

    async fn initialize(&self, _ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        let capture_config = CaptureConfig {
            device: self
                .config
                .device
                .as_ref()
                .map(|s| DeviceSelector::parse(s)),
            host: self.config.host.clone(),
            sample_rate: self.config.sample_rate,
            channels: self.config.channels,
            buffer_size_ms: self.config.buffer_ms,
        };

        let capture = AudioCapture::start(capture_config)
            .map_err(|e| Error::Execution(format!("Failed to start audio capture: {}", e)))?;

        // Store discovered capabilities for Phase 2 validation
        // The actual device may have different capabilities than requested
        let actual_sample_rate = capture.config().sample_rate;
        let actual_channels = capture.config().channels;

        let discovered =
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(actual_sample_rate)),
                channels: Some(ConstraintValue::Exact(actual_channels as u32)),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }));

        *self.discovered_capabilities.lock().await = Some(discovered);

        tracing::info!(
            "MicInput initialized: capturing from '{}' ({}Hz, {} ch)",
            capture.device_name(),
            actual_sample_rate,
            actual_channels
        );

        *self.capture.lock().await = Some(capture);
        Ok(())
    }

    async fn process(&self, _data: RuntimeData) -> Result<RuntimeData, Error> {
        // Get audio from capture
        let mut guard = self.capture.lock().await;
        let capture = guard
            .as_mut()
            .ok_or_else(|| Error::Execution("MicInput not initialized".into()))?;

        // Try to receive audio (non-blocking, return what's available)
        match capture.try_recv() {
            Some(samples) => Ok(RuntimeData::Audio {
                samples: AudioSamples::Vec(samples),
                sample_rate: self.config.sample_rate,
                channels: self.config.channels as u32,
                stream_id: None,
                timestamp_us: None,
                arrival_ts_us: None,
                metadata: None,
            }),
            None => {
                // No audio available yet - return empty audio
                Ok(RuntimeData::Audio {
                    samples: AudioSamples::default(),
                    sample_rate: self.config.sample_rate,
                    channels: self.config.channels as u32,
                    stream_id: None,
                    timestamp_us: None,
                    arrival_ts_us: None,
                    metadata: None,
                })
            }
        }
    }

    async fn process_streaming<F>(
        &self,
        _data: RuntimeData,
        _session_id: Option<String>,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        // Continuous audio capture loop
        let mut guard = self.capture.lock().await;
        let capture = guard
            .as_mut()
            .ok_or_else(|| Error::Execution("MicInput not initialized".into()))?;

        let chunk_size = if self.config.chunk_size > 0 {
            self.config.chunk_size
        } else {
            4000
        };
        let mut buffer = Vec::with_capacity(chunk_size);
        let mut chunks_sent = 0;

        // Receive audio and chunk it
        while let Some(samples) = capture.recv().await {
            buffer.extend(samples);

            // Send complete chunks
            while buffer.len() >= chunk_size {
                let chunk: Vec<f32> = buffer.drain(..chunk_size).collect();
                callback(RuntimeData::Audio {
                    samples: AudioSamples::Vec(chunk),
                    sample_rate: self.config.sample_rate,
                    channels: self.config.channels as u32,
                    stream_id: None,
                    timestamp_us: None,
                    arrival_ts_us: None,
                    metadata: None,
                })?;
                chunks_sent += 1;
            }
        }

        // Send remaining samples if any
        if !buffer.is_empty() {
            callback(RuntimeData::Audio {
                samples: AudioSamples::Vec(buffer),
                sample_rate: self.config.sample_rate,
                channels: self.config.channels as u32,
                stream_id: None,
                timestamp_us: None,
                arrival_ts_us: None,
                metadata: None,
            })?;
            chunks_sent += 1;
        }

        Ok(chunks_sent)
    }
}

// Implement StreamingNode directly for MicInputStreamingNode to support capability methods
#[async_trait::async_trait]
impl StreamingNode for MicInputStreamingNode {
    fn node_type(&self) -> &str {
        "MicInput"
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }

    async fn initialize(&self, ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        <Self as AsyncStreamingNode>::initialize(self, ctx).await
    }

    async fn process_async(
        &self,
        data: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        <Self as AsyncStreamingNode>::process(self, data).await
    }

    async fn process_multi_async(
        &self,
        inputs: std::collections::HashMap<String, RuntimeData>,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        if let Some((_name, data)) = inputs.into_iter().next() {
            self.process_async(data, ctx).await
        } else {
            Err(Error::Execution("No input data provided".into()))
        }
    }

    fn is_multi_input(&self) -> bool {
        false
    }

    /// Inputs are ignored: the node runs on its own clock ([`Self::tick`]).
    async fn process_streaming_async(
        &self,
        _data: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
        _callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<usize, Error> {
        tracing::debug!(node = %self.node_id, "MicInput ignores inputs; it captures on its own clock");
        Ok(0)
    }

    fn pacing_nature(&self) -> PacingNature {
        PacingNature::SourceWall(self.tick_hz())
    }

    async fn tick(
        &self,
        _tick: Tick,
        _ctx: &dyn NodeRuntimeContextRead,
        mut cb: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<(), Error> {
        let (captured, lost) = {
            let mut guard = self.capture.lock().await;
            let capture = guard
                .as_mut()
                .ok_or_else(|| Error::Execution("MicInput not initialized".into()))?;
            capture.drain()
        };
        if lost > 0 {
            let total = self
                .lost
                .fetch_add(lost, std::sync::atomic::Ordering::Relaxed)
                + lost;
            tracing::warn!(
                node = %self.node_id,
                lost,
                total,
                "MicInput fell behind the device; captured buffers were dropped"
            );
        }
        for samples in self.take_chunks(&captured) {
            cb(self.audio(samples))?;
        }
        Ok(())
    }

    // Capability Resolution Methods (spec 023)

    fn media_capabilities(&self) -> Option<MediaCapabilities> {
        if self.is_runtime_discovered() {
            // For RuntimeDiscovered, return None here - use potential/actual instead
            None
        } else {
            Some(Self::configured_caps(&self.config))
        }
    }

    fn capability_behavior(&self) -> CapabilityBehavior {
        if self.is_runtime_discovered() {
            CapabilityBehavior::RuntimeDiscovered
        } else {
            CapabilityBehavior::Configured
        }
    }

    fn potential_capabilities(&self) -> Option<MediaCapabilities> {
        if self.is_runtime_discovered() {
            Some(Self::potential_caps())
        } else {
            Some(Self::configured_caps(&self.config))
        }
    }

    fn actual_capabilities(&self) -> Option<MediaCapabilities> {
        // Try to get discovered capabilities (set during initialize)
        // This is a sync method, so we can't await the lock
        // Use try_lock and fall back to configured caps
        if let Ok(guard) = self.discovered_capabilities.try_lock() {
            if let Some(ref caps) = *guard {
                return Some(caps.clone());
            }
        }
        // Fall back to configured capabilities
        Some(Self::configured_caps(&self.config))
    }
}

/// Factory for creating MicInput streaming nodes
pub struct MicInputNodeFactory;

impl StreamingNodeFactory for MicInputNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &serde_json::Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: MicInputConfig = serde_json::from_value(params.clone()).unwrap_or_default();
        Ok(Box::new(MicInputStreamingNode::new(node_id, config)))
    }

    fn node_type(&self) -> &str {
        "MicInput"
    }

    fn schema(&self) -> Option<remotemedia_core::nodes::schema::NodeSchema> {
        use remotemedia_core::nodes::schema::RuntimeDataType;
        Some(io_schema("MicInput", "Microphone capture source: produces f32 audio at sample_rate (default 16000 Hz) on its own clock from session start, in chunk_size pieces. No inputs; it starts the pipeline.", "io", &[], &[RuntimeDataType::Audio], json!({
                "sample_rate": {"type": "integer", "minimum": 8000, "maximum": 192000, "default": 16000, "description": "Capture rate in Hz"},
                "channels": {"type": "integer", "minimum": 1, "maximum": 8, "default": 1},
                "device": {"type": ["string", "null"], "description": "Device name or index; omit or \"default\" to discover the format at runtime"},
                "host": {"type": ["string", "null"], "description": "Audio backend: alsa, pulse, coreaudio, wasapi"},
                "chunk_size": {"type": "integer", "minimum": 1, "default": 4000, "description": "Samples per output chunk (all channels, interleaved)"},
                "buffer_ms": {"type": "integer", "minimum": 1, "default": 20, "description": "How often captured audio is collected, in ms"}
            })))
    }

    fn media_capabilities(&self, params: &Value) -> Option<MediaCapabilities> {
        let config: MicInputConfig = serde_json::from_value(params.clone()).unwrap_or_default();
        // Always return configured capabilities - this is used for resolution
        Some(MicInputStreamingNode::configured_caps(&config))
    }

    fn capability_behavior(&self) -> CapabilityBehavior {
        // Return Configured as the default behavior
        // The node determines RuntimeDiscovered based on whether device is "default"
        CapabilityBehavior::Configured
    }

    fn potential_capabilities(&self, _params: &Value) -> Option<MediaCapabilities> {
        // Return broad range for Phase 1 validation (used by RuntimeDiscovered nodes)
        Some(MicInputStreamingNode::potential_caps())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mic(params: Value) -> MicInputStreamingNode {
        MicInputStreamingNode::new("mic".into(), serde_json::from_value(params).unwrap())
    }

    #[test]
    fn mic_input_is_a_self_driven_source_paced_by_buffer_ms() {
        assert_eq!(
            StreamingNode::pacing_nature(&mic(json!({}))),
            PacingNature::SourceWall(50)
        );
        assert_eq!(
            StreamingNode::pacing_nature(&mic(json!({"buffer_ms": 10}))),
            PacingNature::SourceWall(100)
        );
        assert_eq!(
            StreamingNode::pacing_nature(&mic(json!({"buffer_ms": 5000}))),
            PacingNature::SourceWall(10)
        );
    }

    #[test]
    fn captured_audio_is_emitted_in_whole_frame_chunks_and_the_rest_carried() {
        // 3 channels, chunk_size 10 -> 9 samples (3 whole frames) per chunk.
        let m = mic(json!({"channels": 3, "chunk_size": 10}));
        let first = m.take_chunks(&[0.0; 20]);
        assert_eq!(first.iter().map(Vec::len).collect::<Vec<_>>(), vec![9, 9]);
        assert!(m.take_chunks(&[0.0; 6]).is_empty()); // carry: 2 + 6 = 8 < 9
        assert_eq!(m.take_chunks(&[0.0; 1]).len(), 1); // carry reaches 9
    }
}
