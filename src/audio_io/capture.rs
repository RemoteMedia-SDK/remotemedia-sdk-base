//! Microphone capture into prepared output loans.
//!
//! The device callback copies its borrowed samples once, directly into a
//! fixed-size loan from the session's selected output allocator (heap or
//! mapped pool); the loan is published as a shared `RuntimeData::Audio`
//! packet. Packets are read with [`AudioCapture::recv_packet`] /
//! [`AudioCapture::try_recv_packet`]; the `Vec<f32>` reads are a counted
//! compatibility materialization.
//!
//! ```no_run
//! use remotemedia_sdk_base::audio_io::{AudioCapture, CaptureConfig, DeviceSelector};
//!
//! # async fn example() -> anyhow::Result<()> {
//! let config = CaptureConfig {
//!     device: Some(DeviceSelector::Name("USB Microphone".into())),
//!     sample_rate: 48000,
//!     ..Default::default()
//! };
//! let mut capture = AudioCapture::start(config)?;
//! while let Some(packet) = capture.recv_packet().await? {
//!     // Forward the shared packet...
//! }
//! # Ok(())
//! # }
//! ```

use crate::{
    data::RuntimeData,
    nodes::{NodeRuntimeContext, NodeRuntimeContextRead},
};
use anyhow::Result;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::Device;
use remotemedia_types::{CopyContext, CopyReason, HeapOutputAllocator};
use std::sync::mpsc;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread::JoinHandle;
use tokio::sync::mpsc as async_mpsc;

mod control;
mod prepared;
mod recording;
use control::CaptureRun;
use prepared::{CaptureCallback, CaptureCounters, CaptureWorker};
pub use recording::{capture_sync, capture_sync_shared};

use super::{find_input_device, get_host, pick_stream_config, DeviceSelector};

/// Audio capture configuration
#[derive(Debug, Clone)]
pub struct CaptureConfig {
    /// Device selector (name, index, or default)
    pub device: Option<DeviceSelector>,
    /// Audio host/backend name (platform-specific)
    pub host: Option<String>,
    /// Sample rate in Hz
    pub sample_rate: u32,
    /// Number of channels
    pub channels: u16,
    /// Buffer size in milliseconds
    pub buffer_size_ms: u32,
}

impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            device: None,
            host: None,
            sample_rate: 48000,
            channels: 1,
            buffer_size_ms: 20,
        }
    }
}

impl CaptureConfig {
    /// Interleaved samples in `duration_ms` of this layout.
    pub fn samples_for_duration(&self, duration_ms: u32) -> Result<usize> {
        anyhow::ensure!(
            self.sample_rate > 0 && self.channels > 0,
            "invalid capture layout"
        );
        let frames = u64::from(self.sample_rate) * u64::from(duration_ms) / 1000;
        let samples = frames
            .checked_mul(u64::from(self.channels))
            .ok_or_else(|| anyhow::anyhow!("capture layout overflow"))?;
        Ok(usize::try_from(samples)?)
    }
}

enum CaptureMode {
    Stream(usize),
    Recording(usize),
}

/// Audio capture handle (Send + Sync safe)
///
/// The actual cpal::Stream runs on a dedicated thread, and this handle
/// communicates via channels. This makes it safe to use in async contexts.
pub struct AudioCapture {
    receiver: async_mpsc::Receiver<RuntimeData>,
    device_name: String,
    config: CaptureConfig,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<Result<()>>>,
    completion: Option<Result<(), String>>,
    copies: CopyContext,
}

impl AudioCapture {
    /// Compatibility entry point. Owned Vec reads explicitly materialize samples;
    /// use recv_packet/try_recv_packet to preserve shared payload storage.
    pub fn start(config: CaptureConfig) -> Result<Self> {
        let samples = config.samples_for_duration(config.buffer_size_ms)?;
        Self::start_with_context(config, samples, &Self::legacy_context())
    }

    /// Heap provider with legacy (allow, uncounted-owner) copy authority, for
    /// callers without a session context.
    pub fn legacy_context() -> NodeRuntimeContext {
        NodeRuntimeContext::for_test("legacy-capture", "microphone")
            .with_output_allocator(Arc::new(HeapOutputAllocator))
            .with_copy_context(CopyContext::legacy().clone())
    }

    /// Retain this call's selected provider and copy authority. Missing authority
    /// fails; strict device adaptation is rejected before opening a stream.
    pub fn start_with_context(
        config: CaptureConfig,
        samples: usize,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<Self> {
        Self::start_controlled(
            config,
            samples,
            Self::capture_context(ctx)?,
            Arc::new(AtomicBool::new(false)),
        )
    }

    /// The session's selected output allocator and copy authority, detached from
    /// `ctx` so the capture threads can own them.
    pub fn capture_context(ctx: &dyn NodeRuntimeContextRead) -> Result<NodeRuntimeContext> {
        let provider = ctx
            .output_allocator()
            .ok_or_else(|| anyhow::anyhow!("capture requires an output allocator"))?;
        let copies = ctx
            .copy_context()
            .ok_or_else(|| anyhow::anyhow!("capture requires copy authority"))?;
        Ok(
            NodeRuntimeContext::for_test(ctx.session_id(), ctx.node_id())
                .with_output_allocator(provider)
                .with_copy_context(copies),
        )
    }

    /// Start streaming `samples`-sample packets; `stop` ends capture (queued
    /// packets and the partial tail stay readable).
    pub fn start_controlled(
        config: CaptureConfig,
        samples: usize,
        context: NodeRuntimeContext,
        stop: Arc<AtomicBool>,
    ) -> Result<Self> {
        Self::start_backend(
            config,
            CaptureMode::Stream(samples),
            context,
            stop,
            Self::open_device,
        )
    }

    fn open_device(
        config: &CaptureConfig,
        mut callback: CaptureCallback,
        counters: Arc<CaptureCounters>,
    ) -> Result<(cpal::Stream, String)> {
        let (device, supported) = Self::device_config(config)?;
        let name = device.name().unwrap_or_else(|_| "Unknown".to_owned());
        let stream = device.build_input_stream(
            &supported.into(),
            move |data: &[f32], _: &cpal::InputCallbackInfo| callback.capture(data),
            move |_error| {
                counters.device_error.store(true, Ordering::Release);
            },
            None,
        )?;
        stream.play()?;
        Ok((stream, name))
    }

    // The backend is created on the owner thread, so its stream need not be Send.
    fn start_backend<S: 'static>(
        config: CaptureConfig,
        mode: CaptureMode,
        mut context: NodeRuntimeContext,
        stop: Arc<AtomicBool>,
        open: impl FnOnce(&CaptureConfig, CaptureCallback, Arc<CaptureCounters>) -> Result<(S, String)>
            + Send
            + 'static,
    ) -> Result<Self> {
        let (samples, recording) = match mode {
            CaptureMode::Stream(samples) => (samples, false),
            CaptureMode::Recording(samples) => (samples, true),
        };
        let bytes = u64::try_from(samples)?
            .checked_mul(4)
            .ok_or_else(|| anyhow::anyhow!("capture byte count overflow"))?;
        context
            .materialization
            .check(CopyReason::BorrowedInput, bytes)?;
        if recording {
            context = recording::prepare(context, samples)?;
        }
        let copies = context.materialization.clone();
        let audio = crate::rt_bridge::PreparedAudioConfig {
            samples,
            sample_rate: config.sample_rate,
            channels: config.channels,
            ready_capacity: if recording { 1 } else { 4 },
        };
        let (output, receiver) = async_mpsc::channel(100);
        let (started, startup) = mpsc::sync_channel(1);
        let run = CaptureRun {
            config: config.clone(),
            context,
            audio,
            stop: stop.clone(),
            output,
            recording,
        };
        let thread = std::thread::Builder::new()
            .name("microphone-control".into())
            .spawn(move || {
                let result = run.run(&started, open);
                if let Err(ref error) = result {
                    let _ = started.send(Err(error.to_string()));
                }
                result
            })?;
        match startup.recv() {
            Ok(Ok(device_name)) => {
                tracing::info!(
                    "Starting audio capture from '{}' ({}Hz, {} ch)",
                    device_name,
                    config.sample_rate,
                    config.channels
                );
                Ok(Self {
                    receiver,
                    device_name,
                    config,
                    stop,
                    thread: Some(thread),
                    completion: None,
                    copies,
                })
            }
            result => {
                stop.store(true, Ordering::Release);
                let _ = thread.join();
                anyhow::bail!(
                    "capture startup failed: {}",
                    match result {
                        Ok(Err(error)) => error,
                        _ => "control thread exited".into(),
                    }
                );
            }
        }
    }

    /// Validate exact requested f32 layout without starting the microphone.
    pub fn validate_config(config: &CaptureConfig) -> Result<()> {
        Self::device_config(config).map(|_| ())
    }

    fn device_config(config: &CaptureConfig) -> Result<(Device, cpal::SupportedStreamConfig)> {
        anyhow::ensure!(
            config.channels != 0 && config.sample_rate != 0,
            "invalid capture layout"
        );
        let device = match &config.device {
            Some(selector) => find_input_device(selector, config.host.as_deref())?,
            None => get_host(config.host.as_deref())?
                .default_input_device()
                .ok_or_else(|| anyhow::anyhow!("No default input device available"))?,
        };
        let name = device.name().unwrap_or_else(|_| "Unknown".to_string());
        let supported = pick_stream_config(
            device.supported_input_configs()?,
            &name,
            config.channels,
            config.sample_rate,
            " Resample with FastResampleNode where another node needs a different rate or channel count.",
        )?;
        anyhow::ensure!(
            supported.sample_format() == cpal::SampleFormat::F32,
            "Device '{name}' offers {} channel(s) at {} Hz only as {:?} samples; capture needs f32",
            config.channels,
            config.sample_rate,
            supported.sample_format()
        );
        Ok((device, supported))
    }

    /// Receive a shared packet. After draining, return the terminal worker error
    /// or clean EOF. No payload conversion or broadcast cloning occurs.
    pub async fn recv_packet(&mut self) -> Result<Option<RuntimeData>> {
        if let Some(packet) = self.receiver.recv().await {
            return Ok(Some(packet));
        }
        self.join()?;
        Ok(None)
    }

    pub fn try_recv_packet(&mut self) -> Result<Option<RuntimeData>> {
        match self.receiver.try_recv() {
            Ok(packet) => Ok(Some(packet)),
            Err(async_mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(async_mpsc::error::TryRecvError::Disconnected) => {
                self.join()?;
                Ok(None)
            }
        }
    }

    /// Request stop; queued packets and the partial tail remain available to read.
    pub fn request_stop(&self) {
        self.stop.store(true, Ordering::Release);
    }

    /// Stop and join the device/bridge workers. Does not discard queued output.
    pub fn stop(&mut self) -> Result<()> {
        self.request_stop();
        self.join()
    }

    fn join(&mut self) -> Result<()> {
        if let Some(thread) = self.thread.take() {
            self.completion = Some(match thread.join() {
                Ok(result) => result.map_err(|error| error.to_string()),
                Err(_) => Err("capture control thread panicked".into()),
            });
        }
        match &self.completion {
            Some(Err(error)) => Err(anyhow::anyhow!(error.clone())),
            _ => Ok(()),
        }
    }

    /// Legacy owned-buffer read. Errors terminate the legacy stream; packet APIs
    /// preserve errors explicitly and avoid this requested Vec materialization.
    pub async fn recv(&mut self) -> Option<Vec<f32>> {
        let packet = self.recv_packet().await;
        self.legacy_samples(packet)
    }

    pub fn try_recv(&mut self) -> Option<Vec<f32>> {
        let packet = self.try_recv_packet();
        self.legacy_samples(packet)
    }

    /// Take every packet captured so far as one counted `Vec`, without
    /// blocking. Capture losses are terminal errors (see
    /// [`Self::try_recv_packet`]), so the lost-buffer count is always zero;
    /// an error ends the drain and is logged.
    pub fn drain(&mut self) -> (Vec<f32>, u64) {
        let mut out = Vec::new();
        while let Some(samples) = self.try_recv() {
            if samples.is_empty() {
                break;
            }
            out.extend_from_slice(&samples);
        }
        (out, 0)
    }

    fn legacy_samples(&self, packet: Result<Option<RuntimeData>>) -> Option<Vec<f32>> {
        let result = packet.and_then(|packet| match packet {
            Some(RuntimeData::Audio { samples, .. }) => {
                Ok(Some(samples.try_into_vec(&self.copies)?))
            }
            _ => Ok(None),
        });
        match result {
            Ok(samples) => samples,
            Err(error) => {
                tracing::error!("capture failed: {error}");
                None
            }
        }
    }

    pub fn device_name(&self) -> &str {
        &self.device_name
    }
    pub fn config(&self) -> &CaptureConfig {
        &self.config
    }
}

/// Test hook: the callback a cpal input stream would drive, for fake devices
/// in tests outside this crate (see [`AudioCapture::start_with_test_device`]).
#[doc(hidden)]
pub struct TestCaptureDevice(CaptureCallback);

impl TestCaptureDevice {
    /// Deliver one device callback's interleaved samples.
    pub fn capture(&mut self, samples: &[f32]) {
        self.0.capture(samples)
    }
}

impl AudioCapture {
    /// Test hook: capture `samples`-sample packets from a fake device.
    /// `open` gets the device callback and returns the "stream"; dropping
    /// the stream must stop the device and drop the callback, as cpal does.
    #[doc(hidden)]
    pub fn start_with_test_device<S: 'static>(
        config: CaptureConfig,
        samples: usize,
        context: NodeRuntimeContext,
        stop: Arc<AtomicBool>,
        open: impl FnOnce(TestCaptureDevice) -> S + Send + 'static,
    ) -> Result<Self> {
        Self::start_backend(
            config,
            CaptureMode::Stream(samples),
            context,
            stop,
            move |_, callback, _| Ok((open(TestCaptureDevice(callback)), "test device".into())),
        )
    }
}

impl Drop for AudioCapture {
    fn drop(&mut self) {
        self.request_stop();
        if let Err(error) = self.join() {
            tracing::debug!("capture teardown: {error}");
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
