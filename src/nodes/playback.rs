//! `SpeakerOutput`: plays incoming audio on a speaker (feature `speaker`).
//!
//! A passthrough sink: audio is queued to the device and forwarded
//! unchanged. A device with another channel layout (a 7.1 card offers only
//! 8 channels in shared mode) plays mono or stereo on its own channels; see
//! [`crate::audio_io::map_frame`].

use crate::audio_io::{AudioPlayback, DeviceSelector, PlaybackConfig};
use crate::capabilities::{
    AudioConstraints, AudioSampleFormat, CapabilityBehavior, ConstraintValue, MediaCapabilities,
    MediaConstraints,
};
use crate::data::RuntimeData;
use crate::nodes::streaming_node::{
    AsyncStreamingNode, InitializeContextRead, NodeRuntimeContextRead, StreamingNode,
    StreamingNodeFactory,
};
use crate::Error;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Mutex;

/// Configuration for SpeakerOutputNode
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeakerOutputConfig {
    /// Sample rate in Hz (should match input audio)
    #[serde(default = "default_sample_rate")]
    pub sample_rate: u32,

    /// Number of audio channels (should match input audio)
    #[serde(default = "default_channels")]
    pub channels: u16,

    /// Audio device name, index, or "default"
    #[serde(default)]
    pub device: Option<String>,

    /// Audio host/backend (alsa, pulse, coreaudio, wasapi)
    #[serde(default)]
    pub host: Option<String>,
}

fn default_sample_rate() -> u32 {
    16000
}

fn default_channels() -> u16 {
    1
}

impl Default for SpeakerOutputConfig {
    fn default() -> Self {
        Self {
            sample_rate: default_sample_rate(),
            channels: default_channels(),
            device: None,
            host: None,
        }
    }
}

/// Schema of a playback sink: audio in, the same audio out, closed params.
fn playback_schema(
    node_type: &str,
    description: &str,
    category: &str,
    accepts: &[crate::nodes::schema::RuntimeDataType],
    produces: &[crate::nodes::schema::RuntimeDataType],
    properties: serde_json::Value,
) -> crate::nodes::schema::NodeSchema {
    crate::nodes::schema::NodeSchema::new(node_type)
        .description(description)
        .category(category)
        .accepts(accepts.iter().cloned())
        .produces(produces.iter().cloned())
        .config_schema(serde_json::json!({
            "type": "object",
            "properties": properties,
            "additionalProperties": false
        }))
}

/// Streaming node for speaker output that actually plays audio
///
/// Implements Passthrough capability behavior - it accepts whatever audio format
/// it receives from upstream and attempts to play it on the output device.
pub struct SpeakerOutputStreamingNode {
    node_id: String,
    config: SpeakerOutputConfig,
    playback: Mutex<Option<AudioPlayback>>,
    /// Capabilities inherited from upstream (set during resolution)
    inherited_capabilities: Mutex<Option<MediaCapabilities>>,
}

impl SpeakerOutputStreamingNode {
    pub fn new(node_id: String, config: SpeakerOutputConfig) -> Self {
        Self {
            node_id,
            config,
            playback: Mutex::new(None),
            inherited_capabilities: Mutex::new(None),
        }
    }
}

#[async_trait]
impl AsyncStreamingNode for SpeakerOutputStreamingNode {
    fn node_type(&self) -> &str {
        "SpeakerOutput"
    }

    async fn initialize(&self, _ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        let playback_config = PlaybackConfig {
            device: self
                .config
                .device
                .as_ref()
                .map(|s| DeviceSelector::parse(s)),
            host: self.config.host.clone(),
            sample_rate: self.config.sample_rate,
            channels: self.config.channels,
        };

        let playback = AudioPlayback::start(playback_config)
            .map_err(|e| Error::Execution(format!("Failed to start audio playback: {}", e)))?;

        tracing::info!(
            "SpeakerOutput initialized: playing to '{}' ({}Hz, {} ch)",
            playback.device_name(),
            playback.config().sample_rate,
            playback.config().channels
        );

        *self.playback.lock().await = Some(playback);
        Ok(())
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        // Play audio to speaker
        if let RuntimeData::Audio {
            samples,
            sample_rate,
            ..
        } = &data
        {
            let mut guard = self.playback.lock().await;
            let playback = guard
                .as_mut()
                .ok_or_else(|| Error::Execution("SpeakerOutput not initialized".into()))?;

            // Resample if needed (simple case: just warn if mismatch)
            if *sample_rate != playback.config().sample_rate {
                tracing::warn!(
                    "Sample rate mismatch: input {} Hz, output {} Hz",
                    sample_rate,
                    playback.config().sample_rate
                );
            }

            playback.queue(samples);
            tracing::trace!("Played {} samples to speaker", samples.len());
        }

        // Sink node returns the input unchanged (for pipeline passthrough)
        Ok(data)
    }
}

// Implement StreamingNode directly to support capability methods
#[async_trait]
impl StreamingNode for SpeakerOutputStreamingNode {
    fn node_type(&self) -> &str {
        "SpeakerOutput"
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

    // Capability Resolution Methods (spec 023)

    fn media_capabilities(&self) -> Option<MediaCapabilities> {
        // Passthrough node - capabilities are inherited from upstream
        // Return None to signal passthrough behavior
        None
    }

    fn capability_behavior(&self) -> CapabilityBehavior {
        CapabilityBehavior::Passthrough
    }

    fn potential_capabilities(&self) -> Option<MediaCapabilities> {
        // Accept any audio format
        Some(MediaCapabilities::with_input(MediaConstraints::Audio(
            AudioConstraints {
                sample_rate: Some(ConstraintValue::Range {
                    min: 8000,
                    max: 192000,
                }),
                channels: Some(ConstraintValue::Range { min: 1, max: 8 }),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            },
        )))
    }

    fn actual_capabilities(&self) -> Option<MediaCapabilities> {
        // Return inherited capabilities if set, otherwise potential capabilities
        if let Ok(guard) = self.inherited_capabilities.try_lock() {
            if let Some(ref caps) = *guard {
                return Some(caps.clone());
            }
        }
        self.potential_capabilities()
    }
}

/// Factory for creating SpeakerOutput streaming nodes
pub struct SpeakerOutputNodeFactory;

impl StreamingNodeFactory for SpeakerOutputNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &serde_json::Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config: SpeakerOutputConfig =
            serde_json::from_value(params.clone()).unwrap_or_default();
        Ok(Box::new(SpeakerOutputStreamingNode::new(node_id, config)))
    }

    fn node_type(&self) -> &str {
        "SpeakerOutput"
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::RuntimeDataType;
        Some(playback_schema(
            "SpeakerOutput",
            "Speaker playback sink: plays incoming audio and passes it through.",
            "io",
            &[RuntimeDataType::Audio],
            &[RuntimeDataType::Audio],
            json!({
                "sample_rate": {"type": "integer", "minimum": 8000, "maximum": 192000, "description": "Should match the incoming audio"},
                "channels": {"type": "integer", "minimum": 1, "maximum": 8},
                "device": {"type": ["string", "null"]},
                "host": {"type": ["string", "null"]}
            }),
        ))
    }

    fn media_capabilities(&self, _params: &Value) -> Option<MediaCapabilities> {
        // SpeakerOutput is a passthrough sink - it accepts whatever audio it receives
        // Capabilities are inherited from upstream during resolution
        None
    }

    fn capability_behavior(&self) -> CapabilityBehavior {
        // Output matches input (passthrough behavior)
        CapabilityBehavior::Passthrough
    }
}
