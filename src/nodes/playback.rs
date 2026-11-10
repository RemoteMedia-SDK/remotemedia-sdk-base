//! `SpeakerOutput`: plays incoming audio on a speaker (feature `speaker`).
//!
//! A passthrough sink: audio is queued to the device and forwarded
//! unchanged. A device with another channel layout (a 7.1 card offers only
//! 8 channels in shared mode) plays mono or stereo on its own channels; see
//! [`crate::audio_io::map_frame`].
//!
//! Playback retains the incoming packet's shared storage until the device
//! callback has copied it out (the one counted `DeviceTransfer`) and hands
//! it back off the real-time thread; the passed-through packet shares that
//! storage. Under a strict zero-copy policy the device copy is refused.

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
use remotemedia_types::{CopyContext, CopyReason};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Mutex;

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

/// Streaming node for speaker output that actually plays audio.
///
/// Takes part in the runtime capability handshake: `initialize()` asks the
/// device what it plays (without opening it) and reports that as this
/// node's accepted input (`actual_capabilities()`): the device's rates, and
/// 1 up to its channel count, since fewer channels are mapped onto the
/// device's own. The device is opened in the format the handshake settles
/// (`configure_from_upstream`), or else in the format of the first audio
/// that arrives, and reopened when the incoming format changes: mono plays
/// as mono, stereo as stereo. The `sample_rate` / `channels` params are not
/// used to pick the format any more.
pub struct SpeakerOutputStreamingNode {
    node_id: String,
    config: SpeakerOutputConfig,
    /// Open in the current format (`None` until the first format is known).
    playback: Mutex<Option<AudioPlayback>>,
    /// What the device accepts, from `initialize()`.
    accepts: Mutex<Option<AudioConstraints>>,
    /// A format the device refused and why, so every chunk does not retry
    /// opening it (and repeats the full reason).
    refused: Mutex<Option<(u32, u16, String)>>,
    /// Inputs that were not audio (nothing to play), for the warning.
    ignored: std::sync::atomic::AtomicU64,
}

/// 1, 10, 100, ...
fn is_power_of_ten(mut n: u64) -> bool {
    while n >= 10 && n % 10 == 0 {
        n /= 10;
    }
    n == 1
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

impl SpeakerOutputStreamingNode {
    pub fn new(node_id: String, config: SpeakerOutputConfig) -> Self {
        Self {
            node_id,
            config,
            playback: Mutex::new(None),
            accepts: Mutex::new(None),
            refused: Mutex::new(None),
            ignored: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Warn that non-audio input reached the speaker: on the first one,
    /// then at 10, 100, 1000, ... so the node logs (and the telemetry's log
    /// counts) show it without a record per chunk.
    fn note_ignored(&self, data: &RuntimeData) {
        let n = self
            .ignored
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1;
        if is_power_of_ten(n) {
            tracing::warn!(
                "SpeakerOutput '{}' received {} ({n} non-audio input(s) so far): nothing was \
                 played. It plays audio only; connect it to a node that outputs audio.",
                self.node_id,
                data.data_type()
            );
        }
    }

    fn selector(&self) -> Option<DeviceSelector> {
        self.config
            .device
            .as_ref()
            .map(|s| DeviceSelector::parse(s))
    }

    /// Play `sample_rate` / `channels` audio from now on, (re)opening the
    /// device when the format changes. `copies` is the session's copy
    /// authority for borrowed compatibility input.
    fn ensure_format(
        &self,
        sample_rate: u32,
        channels: u16,
        copies: &CopyContext,
    ) -> Result<(), Error> {
        if let Some(pb) = lock(&self.playback).as_ref() {
            if pb.config().sample_rate == sample_rate && pb.config().channels == channels {
                return Ok(());
            }
        }
        if let Some((r, c, why)) = lock(&self.refused).as_ref() {
            if (*r, *c) == (sample_rate, channels) {
                return Err(Error::Execution(why.clone()));
            }
        }
        let playback = AudioPlayback::start_with_context(
            PlaybackConfig {
                device: self.selector(),
                host: self.config.host.clone(),
                sample_rate,
                channels,
            },
            copies.clone(),
        )
        .map_err(|e| {
            let why = format!("Failed to start audio playback: {e:#}");
            *lock(&self.refused) = Some((sample_rate, channels, why.clone()));
            Error::Execution(why)
        })?;
        tracing::info!(
            "SpeakerOutput playing to '{}' at {} Hz, {} ch",
            playback.device_name(),
            sample_rate,
            channels
        );
        *lock(&self.playback) = Some(playback);
        Ok(())
    }

    /// Queue `data`'s audio by sharing its storage, and pass the packet on.
    /// A full device queue is backpressure: wait for the callback to make
    /// room, up to the queued audio's duration plus slack.
    async fn play(
        &self,
        mut data: RuntimeData,
        copies: &CopyContext,
    ) -> Result<RuntimeData, Error> {
        let RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            ..
        } = &mut data
        else {
            self.note_ignored(&data);
            return Ok(data);
        };
        let channels = (*channels).max(1) as u16;
        if samples.len() % usize::from(channels) != 0 {
            return Err(Error::Execution(
                "SpeakerOutput input has an incomplete frame".into(),
            ));
        }
        if samples.is_empty() {
            return Ok(data);
        }
        let bytes = u64::try_from(samples.len())
            .ok()
            .and_then(|n| n.checked_mul(4))
            .ok_or_else(|| Error::Execution("SpeakerOutput byte count overflow".into()))?;
        copies
            .check(CopyReason::DeviceTransfer, bytes)
            .map_err(playback_error)?;
        self.ensure_format(*sample_rate, channels, copies)?;
        // Move Vec-backed samples into shared storage without copying, so
        // playback and the passed-through packet own the same buffer.
        let shared = std::mem::take(samples).into_shared();
        *samples = shared.clone().into();
        let mut deadline = None;
        loop {
            let queued = {
                let guard = lock(&self.playback);
                let pb = guard
                    .as_ref()
                    .ok_or_else(|| Error::Execution("SpeakerOutput has stopped".into()))?;
                match pb.queue_shared(shared.clone(), copies) {
                    Err(e) if is_full(&e) => Err(pb.drain_timeout()),
                    other => Ok(other.map_err(playback_error)),
                }
            };
            match queued {
                Ok(result) => {
                    result?;
                    break;
                }
                Err(drain) => {
                    let deadline =
                        *deadline.get_or_insert_with(|| std::time::Instant::now() + drain);
                    if std::time::Instant::now() >= deadline {
                        return Err(Error::Execution(
                            "SpeakerOutput: the device queue stayed full; is the device playing?"
                                .into(),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                }
            }
        }
        tracing::trace!("Queued {} samples to speaker", shared.len());
        Ok(data)
    }

    /// Play out what is queued, then close the device.
    async fn finish(&self) -> Result<(), Error> {
        let playback = lock(&self.playback).take();
        if let Some(mut playback) = playback {
            let timeout = playback.drain_timeout();
            playback.finish(timeout).await.map_err(playback_error)?;
        }
        Ok(())
    }

    /// Use `playback` as the open device (a test fixture).
    #[cfg(test)]
    fn set_playback(&self, playback: AudioPlayback) {
        *lock(&self.playback) = Some(playback);
    }
}

fn playback_error(error: impl std::fmt::Display) -> Error {
    Error::Execution(format!("SpeakerOutput: {error:#}"))
}

/// The device queue has no free slot (backpressure, not a failure).
fn is_full(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<crate::rt_bridge::PlaybackError>(),
        Some(crate::rt_bridge::PlaybackError::Full)
    )
}

/// The audio a device with `formats` (`(channels, min, max)`) accepts
/// through `SpeakerOutput`: its rates, and 1 up to its channel count.
fn device_accepts(formats: &[(u16, u32, u32)]) -> Option<AudioConstraints> {
    let max_channels = formats.iter().map(|f| f.0).max()?;
    let exact: std::collections::BTreeSet<u32> =
        formats.iter().filter(|f| f.1 == f.2).map(|f| f.1).collect();
    let sample_rate = if formats.iter().all(|f| f.1 == f.2) {
        let v: Vec<u32> = exact.into_iter().collect();
        if v.len() == 1 {
            ConstraintValue::Exact(v[0])
        } else {
            ConstraintValue::Set(v)
        }
    } else {
        ConstraintValue::Range {
            min: formats.iter().map(|f| f.1).min()?,
            max: formats.iter().map(|f| f.2).max()?,
        }
    };
    Some(AudioConstraints {
        sample_rate: Some(sample_rate),
        channels: Some(ConstraintValue::Range {
            min: 1,
            max: max_channels as u32,
        }),
        format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
    })
}

#[async_trait]
impl AsyncStreamingNode for SpeakerOutputStreamingNode {
    fn node_type(&self) -> &str {
        "SpeakerOutput"
    }

    async fn initialize(&self, _ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        let selector = self.selector();
        let host = self.config.host.clone();
        let (name, formats) = tokio::task::spawn_blocking(move || {
            crate::audio_io::output_device_formats(selector.as_ref(), host.as_deref())
        })
        .await
        .map_err(|e| Error::Execution(format!("SpeakerOutput: device query: {e}")))?
        .map_err(|e| Error::Execution(format!("Failed to find the output device: {e}")))?;
        let accepts = device_accepts(&formats).ok_or_else(|| {
            Error::Execution(format!(
                "Output device '{name}' reports no supported formats"
            ))
        })?;
        tracing::info!(
            "SpeakerOutput: '{}' plays {}, up to {} channels; it opens in the incoming format",
            name,
            accepts
                .sample_rate
                .as_ref()
                .map(|c| crate::transport::caps_handshake::describe_constraint(c, "Hz"))
                .unwrap_or_default(),
            formats.iter().map(|f| f.0).max().unwrap_or(0)
        );
        *lock(&self.accepts) = Some(accepts);
        Ok(())
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        self.play(data, CopyContext::legacy()).await
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
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        match ctx.copy_context() {
            Some(copies) => self.play(data, &copies).await,
            None => self.play(data, CopyContext::legacy()).await,
        }
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

    /// `{"__aux_port__": "shutdown"}` plays out what is queued and closes the
    /// device.
    async fn process_control_message(
        &self,
        message: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<bool, Error> {
        if matches!(message, RuntimeData::Json(ref value) if value["__aux_port__"] == "shutdown") {
            self.finish().await?;
            return Ok(true);
        }
        Ok(false)
    }

    // Capability Resolution Methods (spec 023)

    /// Nothing declared: what this node accepts is discovered from the
    /// device ([`Self::actual_capabilities`]).
    fn media_capabilities(&self) -> Option<MediaCapabilities> {
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

    /// After `initialize()`: what the device accepts.
    fn actual_capabilities(&self) -> Option<MediaCapabilities> {
        match lock(&self.accepts).clone() {
            Some(a) => Some(MediaCapabilities::with_input(MediaConstraints::Audio(a))),
            None => self.potential_capabilities(),
        }
    }

    /// The handshake's answer: open the device in the settled format now,
    /// before audio arrives.
    fn configure_from_upstream(&self, upstream_caps: &MediaCapabilities) -> Result<(), Error> {
        if let Some(MediaConstraints::Audio(a)) = upstream_caps.default_output() {
            if let (Some(ConstraintValue::Exact(rate)), Some(ConstraintValue::Exact(ch))) =
                (&a.sample_rate, &a.channels)
            {
                self.ensure_format(*rate, (*ch).max(1) as u16, CopyContext::legacy())?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_device_accepts_its_rates_and_up_to_its_channels() {
        let x4 = device_accepts(&[(8, 48000, 48000)]).unwrap();
        assert_eq!(x4.sample_rate, Some(ConstraintValue::Exact(48000)));
        assert_eq!(x4.channels, Some(ConstraintValue::Range { min: 1, max: 8 }));
        let two = device_accepts(&[(2, 44100, 44100), (2, 48000, 48000)]).unwrap();
        assert_eq!(
            two.sample_rate,
            Some(ConstraintValue::Set(vec![44100, 48000]))
        );
        let ranged = device_accepts(&[(2, 8000, 96000)]).unwrap();
        assert_eq!(
            ranged.sample_rate,
            Some(ConstraintValue::Range {
                min: 8000,
                max: 96000
            })
        );
        assert!(device_accepts(&[]).is_none());
    }

    #[tokio::test]
    async fn non_audio_input_is_counted_and_passed_on_without_opening_the_device() {
        let n = SpeakerOutputStreamingNode::new("spkr".into(), SpeakerOutputConfig::default());
        for _ in 0..3 {
            let out = n.process(RuntimeData::Json(json!({"rms_db": -40.0}))).await;
            assert!(matches!(out, Ok(RuntimeData::Json(_))));
        }
        assert_eq!(n.ignored.load(std::sync::atomic::Ordering::Relaxed), 3);
        assert!(lock(&n.playback).is_none());
        let warned: Vec<u64> = (1..=1000).filter(|i| is_power_of_ten(*i)).collect();
        assert_eq!(warned, vec![1, 10, 100, 1000]);
    }

    #[tokio::test]
    async fn audio_is_played_from_and_passed_on_in_the_same_shared_storage() {
        use crate::audio_io::playback::tests::test_playback;
        use crate::data::AudioSamples;
        use crate::nodes::NodeRuntimeContext;
        use remotemedia_types::CopyPolicy;

        let copies = CopyContext::new(CopyPolicy::AllowCopies, "speaker-node");
        let ctx = NodeRuntimeContext::for_test("s", "spkr").with_copy_context(copies.clone());
        let n = SpeakerOutputStreamingNode::new("spkr".into(), SpeakerOutputConfig::default());
        // The fixture device plays 48 kHz mono, the format of this input.
        let (playback, device) = test_playback(copies.clone());
        n.set_playback(playback);
        let input = RuntimeData::Audio {
            samples: AudioSamples::Vec(vec![0.25, 0.5, 0.75]),
            sample_rate: 48000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        };
        let out = StreamingNode::process_async(&n, input, &ctx).await.unwrap();
        let RuntimeData::Audio { samples, .. } = &out else {
            panic!("audio passes through");
        };
        assert!(matches!(samples, AudioSamples::Shared(_)));
        assert_eq!(device.render(4), [0.25, 0.5, 0.75, 0.0]);
        n.finish().await.unwrap();
        drop(out);
        let report = copies.report();
        let device_bytes: u64 = report
            .entries
            .iter()
            .filter(|(reason, _)| *reason == CopyReason::DeviceTransfer)
            .map(|(_, e)| e.copied_bytes)
            .sum();
        let all: u64 = report.entries.iter().map(|(_, e)| e.copied_bytes).sum();
        assert_eq!(device_bytes, 12, "the device copy is the only one");
        assert_eq!(all, 12);
    }

    #[tokio::test]
    async fn strict_policy_refuses_the_device_copy_before_opening() {
        use crate::data::AudioSamples;
        use crate::nodes::NodeRuntimeContext;
        use remotemedia_types::CopyPolicy;

        let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "speaker-strict");
        let ctx = NodeRuntimeContext::for_test("s", "spkr").with_copy_context(copies);
        let n = SpeakerOutputStreamingNode::new("spkr".into(), SpeakerOutputConfig::default());
        let input = RuntimeData::Audio {
            samples: AudioSamples::Vec(vec![0.0; 4]),
            sample_rate: 48000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        };
        assert!(StreamingNode::process_async(&n, input, &ctx).await.is_err());
        assert!(lock(&n.playback).is_none());
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
            "Speaker playback sink: plays incoming audio and passes it through. Plays in the incoming audio's format (mono stays mono); the device's rates are reported to the pipeline so an upstream FastResampleNode without target_rate converts to them. A device with another channel layout plays mono or stereo on its own channels.",
            "io",
            &[RuntimeDataType::Audio],
            &[RuntimeDataType::Audio],
            json!({
                "sample_rate": {"type": "integer", "minimum": 8000, "maximum": 192000, "description": "Unused: playback follows the incoming audio (kept for older manifests)"},
                "channels": {"type": "integer", "minimum": 1, "maximum": 8, "description": "Unused: playback follows the incoming audio (kept for older manifests)"},
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
