//! `MicInput`: a microphone as a self-driven audio source (feature
//! `microphone`).
//!
//! The capture format: `sample_rate` / `channels` where set (the device must
//! offer them, or initialization fails naming what it does offer). Left
//! unset, the node opens the device's own format, and takes part in the
//! runtime capability handshake as an `Adaptive` source: when the pipeline
//! downstream needs a format (a speaker's rates, a 16 kHz mono STT node), it
//! reopens in the nearest format the device offers, keeping the device's own
//! value per dimension where that fits.
//!
//! Capture is zero-copy past the device: the cpal callback copies its
//! borrowed samples once (a counted `BorrowedInput`) straight into
//! `chunk_size`-sample loans from the session's output allocator (heap or a
//! mapped pool), and each full loan is emitted as a shared packet. The device
//! opens on the first tick, with that session's allocator and copy authority.

use super::source_schema as io_schema;
use crate::audio_io::{
    input_device_formats, AudioCapture, CaptureConfig, DeviceSelector, InputDeviceFormats,
};
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
    /// Sample rate in Hz; unset = the device's own (or what the pipeline
    /// downstream needs, when the device offers it).
    #[serde(default)]
    pub sample_rate: Option<u32>,

    /// Number of audio channels (1 = mono, 2 = stereo); unset as for
    /// `sample_rate`.
    #[serde(default)]
    pub channels: Option<u16>,

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

fn default_chunk_size() -> usize {
    4000
}

fn default_buffer_ms() -> u32 {
    20
}

/// The defaults a manifest gets: format from the device, `chunk_size` 4000,
/// `buffer_ms` 20.
impl Default for MicInputConfig {
    fn default() -> Self {
        Self {
            sample_rate: None,
            channels: None,
            device: None,
            host: None,
            chunk_size: default_chunk_size(),
            buffer_ms: default_buffer_ms(),
        }
    }
}

impl MicInputConfig {
    /// The node picks (part of) its format: `sample_rate` or `channels` is
    /// unset.
    pub fn picks_format(&self) -> bool {
        self.sample_rate.is_none() || self.channels.is_none()
    }

    /// The declared output: exact where set, the full range where unset
    /// (the handshake settles it at runtime).
    pub fn declared_caps(&self) -> MediaCapabilities {
        MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(match self.sample_rate {
                Some(r) => ConstraintValue::Exact(r),
                None => ConstraintValue::Range {
                    min: 8000,
                    max: 192000,
                },
            }),
            channels: Some(match self.channels {
                Some(c) => ConstraintValue::Exact(c as u32),
                None => ConstraintValue::Range { min: 1, max: 8 },
            }),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        }))
    }
}

/// Capture rates tried inside a device's rate ranges.
const COMMON_RATES: &[u32] = &[
    8000, 11025, 16000, 22050, 24000, 32000, 44100, 48000, 88200, 96000, 176400, 192000,
];

/// The `(channels, sample_rate)` to open an input device in.
///
/// `channels` / `sample_rate` are kept where set. Of the formats the device
/// offers (`formats`: `(channels, min_rate, max_rate)`), the pick is the one
/// `demand` (what the pipeline downstream accepts) allows in the most
/// dimensions, then the closest to the device's own format (`own`): the
/// device's value is kept where it fits, else the nearest (the higher rate
/// on a tie). `None` when no offered format has the set values.
pub fn pick_format(
    formats: &[(u16, u32, u32)],
    own: Option<(u16, u32)>,
    channels: Option<u16>,
    sample_rate: Option<u32>,
    demand: Option<&AudioConstraints>,
) -> Option<(u16, u32)> {
    let (own_ch, own_rate) = own.or_else(|| {
        formats
            .first()
            .map(|&(c, lo, hi)| (c, 48000u32.clamp(lo, hi)))
    })?;
    let mut wanted: Vec<u32> = COMMON_RATES.to_vec();
    wanted.push(own_rate);
    wanted.extend(sample_rate);
    if let Some(r) = demand.and_then(|d| d.sample_rate.as_ref()) {
        match r {
            ConstraintValue::Exact(v) => wanted.push(*v),
            ConstraintValue::Set(s) => wanted.extend(s.iter().copied()),
            ConstraintValue::Range { min, max } => wanted.extend([*min, *max]),
        }
    }
    let misses = |ch: u16, rate: u32| -> u8 {
        let Some(d) = demand else { return 0 };
        let rate_miss = d.sample_rate.as_ref().is_some_and(|c| !c.satisfies(&rate));
        let ch_miss = d
            .channels
            .as_ref()
            .is_some_and(|c| !c.satisfies(&(ch as u32)));
        rate_miss as u8 + ch_miss as u8
    };
    formats
        .iter()
        .filter(|(c, _, _)| channels.is_none_or(|want| want == *c))
        .flat_map(|&(c, lo, hi)| {
            wanted
                .iter()
                .copied()
                .chain([lo, hi])
                .filter(move |r| (lo..=hi).contains(r))
                .map(move |r| (c, r))
        })
        .filter(|&(_, r)| sample_rate.is_none_or(|want| want == r))
        .min_by_key(|&(c, r)| {
            (
                misses(c, r),
                c.abs_diff(own_ch),
                r.abs_diff(own_rate),
                std::cmp::Reverse(r),
            )
        })
}

/// Streaming node for microphone input that actually captures audio
///
/// A self-driven source: it declares [`PacingNature::SourceWall`] at one tick
/// per `buffer_ms`, so the session runs it on its own clock from the moment
/// the session starts (no input needed). Each tick drains what the device
/// captured since the last one and emits it in `chunk_size` pieces; inputs
/// sent to it are ignored.
///
/// Capabilities: with `sample_rate` and `channels` set and an explicit
/// device it is `Configured`; with both set on the default device,
/// `RuntimeDiscovered` (the device is found at runtime). With either unset
/// it is `Adaptive` (see the module docs): `actual_capabilities()` is the
/// format the device is open in.
pub struct MicInputStreamingNode {
    node_id: String,
    config: MicInputConfig,
    /// The running capture; `None` until the first read (it needs the
    /// session's allocator) and after a format change.
    capture: Mutex<Option<AudioCapture>>,
    /// `(channels, sample_rate)` the device opens in.
    format: std::sync::Mutex<Option<(u16, u32)>>,
    /// What the device offers, when the node picks its format.
    device: std::sync::Mutex<Option<InputDeviceFormats>>,
    /// Discovered device capabilities (populated during initialize)
    discovered_capabilities: std::sync::Mutex<Option<MediaCapabilities>>,
}

fn lock<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

impl MicInputStreamingNode {
    pub fn new(node_id: String, config: MicInputConfig) -> Self {
        Self {
            node_id,
            config,
            capture: Mutex::new(None),
            format: std::sync::Mutex::new(None),
            device: std::sync::Mutex::new(None),
            discovered_capabilities: std::sync::Mutex::new(None),
        }
    }

    /// Ticks per second: one per `buffer_ms` (10..=200 Hz).
    fn tick_hz(&self) -> u32 {
        (1000 / self.config.buffer_ms.max(1)).clamp(10, 200)
    }

    /// `(channels, sample_rate)` of the emitted audio: the open format, or
    /// the configured one before the device is open.
    fn current_format(&self) -> (u16, u32) {
        lock(&self.format).unwrap_or((
            self.config.channels.unwrap_or(1),
            self.config.sample_rate.unwrap_or(16000),
        ))
    }

    /// Samples per emitted chunk, rounded down to whole frames.
    fn chunk_samples(&self) -> usize {
        let ch = (self.current_format().0 as usize).max(1);
        let n = if self.config.chunk_size > 0 {
            self.config.chunk_size
        } else {
            4000
        };
        (n / ch).max(1) * ch
    }

    fn audio(&self, samples: Vec<f32>) -> RuntimeData {
        let (channels, sample_rate) = self.current_format();
        RuntimeData::Audio {
            samples: AudioSamples::Vec(samples),
            sample_rate,
            channels: channels as u32,
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

    fn selector(&self) -> Option<DeviceSelector> {
        self.config
            .device
            .as_ref()
            .map(|s| DeviceSelector::parse(s))
    }

    fn capture_config(&self, channels: u16, sample_rate: u32) -> CaptureConfig {
        CaptureConfig {
            device: self.selector(),
            host: self.config.host.clone(),
            sample_rate,
            channels,
            buffer_size_ms: self.config.buffer_ms,
        }
    }

    /// Settle on `channels` x `sample_rate`: check the device offers it (the
    /// error names what it does offer), close any running capture, and record
    /// the format. The device opens on the next read.
    fn open(
        &self,
        guard: &mut Option<AudioCapture>,
        channels: u16,
        sample_rate: u32,
    ) -> Result<(), Error> {
        *guard = None; // stops and joins the previous stream
        AudioCapture::validate_config(&self.capture_config(channels, sample_rate))
            .map_err(|e| Error::Execution(format!("Failed to start audio capture: {e:#}")))?;
        *lock(&self.format) = Some((channels, sample_rate));
        *lock(&self.discovered_capabilities) = Some(MediaCapabilities::with_output(
            MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(sample_rate)),
                channels: Some(ConstraintValue::Exact(channels as u32)),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }),
        ));
        tracing::info!(
            node = %self.node_id,
            "MicInput initialized: capturing at {}Hz, {} ch",
            sample_rate,
            channels
        );
        Ok(())
    }

    /// The running capture, started on first use: into the session's
    /// allocator with its copy authority (`ctx`), or a heap provider with
    /// legacy authority for context-free callers.
    fn started<'a>(
        &self,
        guard: &'a mut Option<AudioCapture>,
        ctx: Option<&dyn NodeRuntimeContextRead>,
    ) -> Result<&'a mut AudioCapture, Error> {
        if guard.is_none() {
            let (channels, sample_rate) = self.current_format();
            let config = self.capture_config(channels, sample_rate);
            let samples = self.chunk_samples();
            let capture = match ctx {
                Some(ctx) => AudioCapture::start_with_context(config, samples, ctx),
                None => AudioCapture::start_with_context(
                    config,
                    samples,
                    &AudioCapture::legacy_context(),
                ),
            }
            .map_err(capture_error)?;
            *guard = Some(capture);
        }
        Ok(guard.as_mut().expect("capture started"))
    }

    /// One ready packet, or empty audio when none is ready yet.
    async fn read_once(
        &self,
        ctx: Option<&dyn NodeRuntimeContextRead>,
    ) -> Result<RuntimeData, Error> {
        let mut guard = self.capture.lock().await;
        let capture = self.started(&mut guard, ctx)?;
        Ok(capture
            .try_recv_packet()
            .map_err(capture_error)?
            .unwrap_or_else(|| self.audio(Vec::new())))
    }

    /// Use `capture` as the running capture (a test fixture).
    #[cfg(test)]
    pub(crate) async fn set_capture(&self, capture: AudioCapture) {
        *self.capture.lock().await = Some(capture);
    }
}

fn capture_error(error: anyhow::Error) -> Error {
    Error::Execution(format!("MicInput: {error:#}"))
}

#[async_trait::async_trait]
impl AsyncStreamingNode for MicInputStreamingNode {
    fn node_type(&self) -> &str {
        "MicInput"
    }

    async fn initialize(&self, _ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        let (channels, sample_rate) = match (self.config.channels, self.config.sample_rate) {
            (Some(c), Some(r)) => (c, r),
            (channels, sample_rate) => {
                let dev =
                    input_device_formats(self.selector().as_ref(), self.config.host.as_deref())
                        .map_err(|e| {
                            Error::Execution(format!("Failed to query the input device: {e:#}"))
                        })?;
                // No offered format has the set value: open it anyway, for
                // the error that lists what the device does offer.
                let own = dev.default.unwrap_or((1, 48000));
                let picked = pick_format(&dev.formats, dev.default, channels, sample_rate, None)
                    .unwrap_or((channels.unwrap_or(own.0), sample_rate.unwrap_or(own.1)));
                tracing::debug!(
                    node = %self.node_id,
                    "MicInput: '{}' offers {:?} (own format {:?}); opening {} ch at {} Hz",
                    dev.name, dev.formats, dev.default, picked.0, picked.1
                );
                *lock(&self.device) = Some(dev);
                picked
            }
        };
        let mut guard = self.capture.lock().await;
        self.open(&mut guard, channels, sample_rate)
    }

    async fn process(&self, _data: RuntimeData) -> Result<RuntimeData, Error> {
        self.read_once(None).await
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
        let mut guard = self.capture.lock().await;
        self.started(&mut guard, None)?;
        // Owned locally: a cancelled future drops (stops and joins) the
        // capture rather than leaving the microphone running.
        let mut capture = guard.take().expect("capture started");
        drop(guard);
        let mut sent = 0;
        while let Some(packet) = capture.recv_packet().await.map_err(capture_error)? {
            callback(packet)?;
            sent += 1;
        }
        Ok(sent)
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
        _data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        self.read_once(Some(ctx)).await
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
        ctx: &dyn NodeRuntimeContextRead,
        mut cb: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<(), Error> {
        let mut guard = self.capture.lock().await;
        let capture = self.started(&mut guard, Some(ctx))?;
        // Every full chunk captured since the last tick, as shared packets.
        // Device or queue losses are terminal errors, not silent gaps.
        while let Some(packet) = capture.try_recv_packet().map_err(capture_error)? {
            cb(packet)?;
        }
        Ok(())
    }

    /// `{"__aux_port__": "shutdown"}` stops the device; chunks already
    /// captured still drain on later reads.
    async fn process_control_message(
        &self,
        message: RuntimeData,
        _ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<bool, Error> {
        if matches!(message, RuntimeData::Json(ref value) if value["__aux_port__"] == "shutdown") {
            if let Some(capture) = self.capture.lock().await.as_ref() {
                capture.request_stop();
            }
            return Ok(true);
        }
        Ok(false)
    }

    // Capability Resolution Methods (spec 023)

    fn media_capabilities(&self) -> Option<MediaCapabilities> {
        if self.config.picks_format() || self.is_runtime_discovered() {
            // Settled at runtime: see potential/actual.
            None
        } else {
            Some(self.config.declared_caps())
        }
    }

    fn capability_behavior(&self) -> CapabilityBehavior {
        if self.config.picks_format() {
            CapabilityBehavior::Adaptive
        } else if self.is_runtime_discovered() {
            CapabilityBehavior::RuntimeDiscovered
        } else {
            CapabilityBehavior::Configured
        }
    }

    fn potential_capabilities(&self) -> Option<MediaCapabilities> {
        Some(self.config.declared_caps())
    }

    fn actual_capabilities(&self) -> Option<MediaCapabilities> {
        lock(&self.discovered_capabilities)
            .clone()
            .or_else(|| Some(self.config.declared_caps()))
    }

    /// The handshake's demand: reopen in the device format nearest to what
    /// the pipeline downstream accepts (when this node picks its format).
    /// A demand the device cannot meet keeps the nearest format it can; the
    /// handshake then names the connection and the converter to insert.
    fn configure_for_downstream(
        &self,
        accepted: &MediaCapabilities,
    ) -> Result<Option<MediaCapabilities>, Error> {
        if !self.config.picks_format() {
            return Ok(None);
        }
        let Some(demand) = crate::transport::caps_handshake::accepted_audio(accepted) else {
            return Ok(None);
        };
        let picked = {
            let dev = lock(&self.device);
            let Some(dev) = dev.as_ref() else {
                return Ok(None);
            };
            pick_format(
                &dev.formats,
                dev.default,
                self.config.channels,
                self.config.sample_rate,
                Some(&demand),
            )
        };
        let Some((channels, sample_rate)) = picked else {
            return Ok(None);
        };
        if *lock(&self.format) != Some((channels, sample_rate)) {
            let mut guard = self.capture.try_lock().map_err(|_| {
                Error::Execution("MicInput is capturing; cannot change its format now".into())
            })?;
            tracing::info!(
                node = %self.node_id,
                "MicInput: reopening at {sample_rate} Hz, {channels} ch for what the pipeline downstream accepts"
            );
            self.open(&mut guard, channels, sample_rate)?;
        }
        Ok(Some(MediaCapabilities::with_output(
            MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(sample_rate)),
                channels: Some(ConstraintValue::Exact(channels as u32)),
                format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
            }),
        )))
    }
}

/// Factory for creating MicInput streaming nodes
pub struct MicInputNodeFactory;

fn config_from(params: &Value) -> MicInputConfig {
    if params.is_null() {
        return MicInputConfig::default();
    }
    serde_json::from_value(params.clone()).unwrap_or_else(|_| MicInputConfig::default())
}

impl StreamingNodeFactory for MicInputNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &serde_json::Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        Ok(Box::new(MicInputStreamingNode::new(
            node_id,
            config_from(params),
        )))
    }

    fn node_type(&self) -> &str {
        "MicInput"
    }

    fn schema(&self) -> Option<remotemedia_core::nodes::schema::NodeSchema> {
        use remotemedia_core::nodes::schema::RuntimeDataType;
        Some(io_schema("MicInput", "Microphone capture source: produces f32 audio on its own clock from session start, in chunk_size pieces. No inputs; it starts the pipeline. Leave sample_rate and channels unset to capture in the device's own format, or in the one the pipeline downstream needs (a speaker, a 16 kHz STT node) when the device offers it.", "io", &[], &[RuntimeDataType::Audio], json!({
                "sample_rate": {"type": "integer", "minimum": 8000, "maximum": 192000, "description": "Capture rate in Hz; unset = the device's own (or what downstream needs, if the device offers it). A set rate the device does not offer fails the start, naming what it offers."},
                "channels": {"type": "integer", "minimum": 1, "maximum": 8, "description": "Channel count; unset as for sample_rate"},
                "device": {"type": ["string", "null"], "description": "Device name or index; omit or \"default\" for the default input device"},
                "host": {"type": ["string", "null"], "description": "Audio backend: alsa, pulse, coreaudio, wasapi"},
                "chunk_size": {"type": "integer", "minimum": 1, "default": 4000, "description": "Samples per output chunk (all channels, interleaved)"},
                "buffer_ms": {"type": "integer", "minimum": 1, "default": 20, "description": "How often captured audio is collected, in ms"}
            })))
    }

    fn media_capabilities(&self, params: &Value) -> Option<MediaCapabilities> {
        // Exact where set, ranges where the runtime picks.
        Some(config_from(params).declared_caps())
    }

    fn capability_behavior(&self) -> CapabilityBehavior {
        // Return Configured as the default behavior
        // The node determines RuntimeDiscovered based on whether device is "default"
        CapabilityBehavior::Configured
    }

    fn potential_capabilities(&self, params: &Value) -> Option<MediaCapabilities> {
        Some(config_from(params).declared_caps())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mic(params: Value) -> MicInputStreamingNode {
        MicInputStreamingNode::new("mic".into(), config_from(&params))
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
    fn chunks_are_whole_frames() {
        // 3 channels, chunk_size 10 -> 9 samples (3 whole frames) per packet.
        assert_eq!(
            mic(json!({"channels": 3, "chunk_size": 10})).chunk_samples(),
            9
        );
        assert_eq!(mic(json!({"channels": 2})).chunk_samples(), 4000);
        assert_eq!(
            mic(json!({"channels": 3, "chunk_size": 2})).chunk_samples(),
            3
        );
    }

    #[tokio::test]
    async fn ticks_emit_shared_capture_packets_without_staging_copies() {
        use crate::audio_io::capture::tests::fixture;
        use crate::nodes::NodeRuntimeContext;
        use remotemedia_types::{CopyContext, CopyPolicy, HeapOutputAllocator};
        use std::sync::{atomic::AtomicBool, Arc};

        let copies = CopyContext::new(CopyPolicy::AllowCopies, "mic-tick");
        let ctx = NodeRuntimeContext::for_test("s", "mic")
            .with_output_allocator(Arc::new(HeapOutputAllocator))
            .with_copy_context(copies.clone());
        let node = mic(json!({"channels": 2, "sample_rate": 48000}));
        // The fixture captures 142 samples in odd-sized callbacks into
        // 64-sample loans: two full packets and a 14-sample tail.
        node.set_capture(fixture(ctx.clone(), Arc::new(AtomicBool::new(false))))
            .await;
        let mut lens = Vec::new();
        for _ in 0..2000 {
            let out = Arc::new(std::sync::Mutex::new(Vec::new()));
            let sink = out.clone();
            StreamingNode::tick(
                &node,
                Tick {
                    clock_id: "wall:50".into(),
                    pts_us: 0,
                    frame_idx: 0,
                    deadline_us: 20_000,
                },
                &ctx,
                Box::new(move |packet| {
                    sink.lock().unwrap().push(packet);
                    Ok(())
                }),
            )
            .await
            .unwrap();
            for packet in out.lock().unwrap().drain(..) {
                let RuntimeData::Audio {
                    samples, channels, ..
                } = packet
                else {
                    panic!("audio packet");
                };
                assert_eq!(channels, 2);
                assert!(matches!(samples, AudioSamples::Shared(_)));
                lens.push(samples.len());
            }
            if lens.iter().sum::<usize>() == 142 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert_eq!(lens, [64, 64, 14]);
        drop(node);
        let report = copies.report();
        let staged: u64 = report.entries.iter().map(|(_, e)| e.copied_bytes).sum();
        assert_eq!(staged, 142 * 4, "only the device callback copy is counted");
    }

    fn audio(rate: ConstraintValue<u32>, ch: ConstraintValue<u32>) -> AudioConstraints {
        AudioConstraints {
            sample_rate: Some(rate),
            channels: Some(ch),
            format: None,
        }
    }

    #[test]
    fn unset_format_is_the_devices_own() {
        // A stereo-48k-only USB mic (the HyperX case) and a flexible one.
        let usb = [(2, 48000, 48000)];
        assert_eq!(
            pick_format(&usb, Some((2, 48000)), None, None, None),
            Some((2, 48000))
        );
        let flexible = [(1, 8000, 96000), (2, 8000, 96000)];
        assert_eq!(
            pick_format(&flexible, Some((2, 44100)), None, None, None),
            Some((2, 44100))
        );
        // One dimension set: the other follows the device.
        assert_eq!(
            pick_format(&flexible, Some((2, 44100)), Some(1), None, None),
            Some((1, 44100))
        );
        // A set value the device does not offer: no pick (start fails
        // with the device's list).
        assert_eq!(
            pick_format(&usb, Some((2, 48000)), Some(1), None, None),
            None
        );
    }

    #[test]
    fn a_downstream_demand_picks_the_nearest_offered_format() {
        use ConstraintValue::{Exact, Range};
        let flexible = [(1, 8000, 96000), (2, 8000, 96000)];
        // A 16 kHz mono STT node: the device can, so it opens that.
        let stt = audio(Exact(16000), Exact(1));
        assert_eq!(
            pick_format(&flexible, Some((2, 48000)), None, None, Some(&stt)),
            Some((1, 16000))
        );
        // A speaker at 48 kHz with 1..8 channels: keep the device's stereo.
        let speaker = audio(Exact(48000), Range { min: 1, max: 8 });
        assert_eq!(
            pick_format(&flexible, Some((2, 44100)), None, None, Some(&speaker)),
            Some((2, 48000))
        );
        // The device cannot meet it: its own format stays (the handshake
        // then names the converter to insert).
        let usb = [(2, 48000, 48000)];
        assert_eq!(
            pick_format(&usb, Some((2, 48000)), None, None, Some(&stt)),
            Some((2, 48000))
        );
        // A set rate wins over the demand.
        assert_eq!(
            pick_format(&flexible, Some((2, 48000)), None, Some(48000), Some(&stt)),
            Some((1, 48000))
        );
    }

    #[test]
    fn unset_format_is_adaptive_and_declared_as_ranges() {
        let picks = mic(json!({}));
        assert_eq!(
            StreamingNode::capability_behavior(&picks),
            CapabilityBehavior::Adaptive
        );
        assert!(StreamingNode::media_capabilities(&picks).is_none());
        let declared = MicInputNodeFactory
            .media_capabilities(&json!({"channels": 2}))
            .unwrap();
        match declared.default_output().unwrap() {
            MediaConstraints::Audio(a) => {
                assert_eq!(a.channels, Some(ConstraintValue::Exact(2)));
                assert_eq!(
                    a.sample_rate,
                    Some(ConstraintValue::Range {
                        min: 8000,
                        max: 192000
                    })
                );
            }
            other => panic!("{other:?}"),
        }
        let fixed = mic(json!({"channels": 1, "sample_rate": 16000, "device": "hw:0"}));
        assert_eq!(
            StreamingNode::capability_behavior(&fixed),
            CapabilityBehavior::Configured
        );
        let on_default = mic(json!({"channels": 1, "sample_rate": 16000}));
        assert_eq!(
            StreamingNode::capability_behavior(&on_default),
            CapabilityBehavior::RuntimeDiscovered
        );
        // Not open yet: emits the configured format (16 kHz mono fallback).
        assert_eq!(picks.current_format(), (1, 16000));
    }
}
