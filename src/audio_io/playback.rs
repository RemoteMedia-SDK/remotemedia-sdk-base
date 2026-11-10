//! Speaker playback that retains shared sample owners.
//!
//! Packets are admitted by reference ([`AudioPlayback::queue_shared`]); the
//! device callback copies them into its output buffer (one counted
//! `DeviceTransfer`) and hands the owners back to a control thread, which
//! releases them off the real-time thread. Borrowed `&[f32]` input
//! ([`AudioPlayback::try_queue`]) costs one additional counted copy.

use anyhow::Result;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::Device;
use std::sync::{Arc, Mutex};

use super::{find_output_device, get_host, pick_output_config, DeviceSelector};

/// Audio playback configuration
#[derive(Debug, Clone)]
pub struct PlaybackConfig {
    /// Device selector (name, index, or default)
    pub device: Option<DeviceSelector>,
    /// Audio host/backend name (platform-specific)
    pub host: Option<String>,
    /// Sample rate in Hz
    pub sample_rate: u32,
    /// Number of channels
    pub channels: u16,
}

impl Default for PlaybackConfig {
    fn default() -> Self {
        Self {
            device: None,
            host: None,
            sample_rate: 48000,
            channels: 1,
        }
    }
}

/// Joined playback controller with bounded shared-owner admission.
/// Only admission/control code locks; the device callback uses SPSC queues.
pub struct AudioPlayback {
    queue: Arc<Mutex<crate::rt_bridge::PlaybackQueue>>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    error: Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<Result<()>>>,
    completion: Option<std::result::Result<(), String>>,
    copies: remotemedia_types::CopyContext,
    device_name: String,
    config: PlaybackConfig,
}

mod control;
#[cfg(test)]
pub(crate) mod tests;

impl AudioPlayback {
    pub fn start(config: PlaybackConfig) -> Result<Self> {
        Self::start_with_context(config, remotemedia_types::CopyContext::legacy().clone())
    }

    /// Device output necessarily copies samples. Strict callers fail before opening it.
    pub fn start_with_context(
        config: PlaybackConfig,
        copies: remotemedia_types::CopyContext,
    ) -> Result<Self> {
        Self::start_backend(config, copies, Self::open_device)
    }

    fn open_device(
        config: &PlaybackConfig,
        mut callback: control::PlaybackCallback,
    ) -> Result<(cpal::Stream, String)> {
        let (device, supported, device_channels) = Self::device_config(config)?;
        let name = device.name().unwrap_or_else(|_| "Unknown".into());
        // A device without `config.channels` channels (a 7.1 card offers only
        // 8 in shared mode) plays them mapped onto its own; see `map_frame`.
        callback.map_channels(config.channels, device_channels);
        let error = callback.error.clone();
        let stream = device.build_output_stream(
            &supported.into(),
            move |output: &mut [f32], _: &cpal::OutputCallbackInfo| callback.render(output),
            move |_| {
                error.store(true, std::sync::atomic::Ordering::Release);
            },
            None,
        )?;
        stream.play()?;
        Ok((stream, name))
    }

    fn start_backend<S: 'static>(
        config: PlaybackConfig,
        copies: remotemedia_types::CopyContext,
        open: impl FnOnce(&PlaybackConfig, control::PlaybackCallback) -> Result<(S, String)>
            + Send
            + 'static,
    ) -> Result<Self> {
        use std::sync::{atomic::AtomicBool, mpsc};
        anyhow::ensure!(
            config.channels > 0 && config.sample_rate > 0,
            "invalid playback layout"
        );
        anyhow::ensure!(
            copies.policy() != remotemedia_types::CopyPolicy::RequireZeroCopy,
            "CPAL playback requires a device transfer; strict zero-copy is unsupported"
        );
        let (queue, consumer, retired) = crate::rt_bridge::playback_queue(64, config.channels)?;
        let queue = Arc::new(Mutex::new(queue));
        let stop = Arc::new(AtomicBool::new(false));
        let error = Arc::new(AtomicBool::new(false));
        let worker_queue = queue.clone();
        let worker_stop = stop.clone();
        let worker_error = error.clone();
        let (started, startup) = mpsc::sync_channel(1);
        let backend_config = config.clone();
        let thread = std::thread::Builder::new()
            .name("speaker-control".into())
            .spawn(move || {
                let (mut worker, callback) = control::PlaybackWorker::new(
                    worker_queue,
                    consumer,
                    retired,
                    worker_stop,
                    worker_error,
                );
                let result = worker.run(&backend_config, callback, &started, open);
                if let Err(ref error) = result {
                    let _ = started.send(Err(error.to_string()));
                }
                result
            })?;
        let mut playback = Self {
            queue,
            stop,
            error,
            thread: Some(thread),
            completion: None,
            copies,
            device_name: String::new(),
            config,
        };
        playback.device_name = match startup.recv() {
            Ok(Ok(name)) => name,
            status => {
                anyhow::bail!(
                    "playback startup failed: {}",
                    match status {
                        Ok(Err(error)) => error,
                        _ => "control thread exited".into(),
                    }
                );
            }
        };
        Ok(playback)
    }

    fn device_config(
        config: &PlaybackConfig,
    ) -> Result<(Device, cpal::SupportedStreamConfig, u16)> {
        anyhow::ensure!(
            config.channels > 0 && config.sample_rate > 0,
            "invalid playback layout"
        );
        let device = match &config.device {
            Some(selector) => find_output_device(selector, config.host.as_deref())?,
            None => get_host(config.host.as_deref())?
                .default_output_device()
                .ok_or_else(|| anyhow::anyhow!("No default output device available"))?,
        };
        let name = device.name().unwrap_or_else(|_| "Unknown".into());
        let (supported, device_channels) = pick_output_config(
            device.supported_output_configs()?,
            &name,
            config.channels,
            config.sample_rate,
        )?;
        anyhow::ensure!(
            supported.sample_format() == cpal::SampleFormat::F32,
            "Device '{name}' offers {device_channels} channel(s) at {} Hz only as {:?} samples; playback needs f32",
            config.sample_rate,
            supported.sample_format()
        );
        Ok((device, supported, device_channels))
    }

    /// Compatibility API; use try_queue to handle queue-full/device errors.
    pub fn queue(&self, samples: &[f32]) {
        if let Err(error) = self.try_queue(samples) {
            tracing::error!(%error, "playback admission failed");
        }
    }

    /// Borrowed compatibility input requires one counted ownership copy.
    pub fn try_queue(&self, samples: &[f32]) -> Result<()> {
        use remotemedia_types::CopyReason;
        self.check_running()?;
        if samples.is_empty() {
            return Ok(());
        }
        anyhow::ensure!(
            samples.len() % usize::from(self.config.channels) == 0,
            "incomplete audio frame"
        );
        let bytes = u64::try_from(samples.len())?
            .checked_mul(4)
            .ok_or_else(|| anyhow::anyhow!("playback byte count overflow"))?;
        self.copies.check(CopyReason::DeviceTransfer, bytes)?;
        let owned = self
            .copies
            .materialize(CopyReason::BorrowedInput, bytes, || samples.to_vec())?;
        self.queue_shared(owned.into(), &self.copies)
    }

    /// Preserve the packet's backing storage and this call's copy authority.
    pub fn queue_shared(
        &self,
        samples: remotemedia_types::SharedBuffer<f32>,
        copies: &remotemedia_types::CopyContext,
    ) -> Result<()> {
        let mut queue = self
            .queue
            .lock()
            .map_err(|_| anyhow::anyhow!("playback admission lock poisoned"))?;
        self.check_running()?;
        queue.try_queue(samples, copies)?;
        Ok(())
    }

    pub fn try_clear(&self) -> Result<()> {
        let mut queue = self
            .queue
            .lock()
            .map_err(|_| anyhow::anyhow!("playback admission lock poisoned"))?;
        self.check_running()?;
        queue.clear()?;
        Ok(())
    }

    pub fn clear(&self) {
        if let Err(error) = self.try_clear() {
            tracing::error!(%error, "playback clear failed");
        }
    }

    /// Current-generation device writes and retirement, not hardware audible completion.
    pub fn is_complete(&self) -> bool {
        self.stats().pending_packets == 0
    }
    pub fn position(&self) -> usize {
        self.stats().rendered_samples
    }
    pub fn total_samples(&self) -> usize {
        self.stats().queued_samples
    }
    pub fn stats(&self) -> crate::rt_bridge::PlaybackStats {
        self.queue
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .stats()
    }
    pub fn device_name(&self) -> &str {
        &self.device_name
    }
    pub fn config(&self) -> &PlaybackConfig {
        &self.config
    }

    pub fn check_running(&self) -> Result<()> {
        use std::sync::atomic::Ordering;
        anyhow::ensure!(
            !self.error.load(Ordering::Acquire),
            "audio output callback/device failed"
        );
        anyhow::ensure!(
            !self.stop.load(Ordering::Acquire)
                && self.thread.as_ref().is_some_and(|t| !t.is_finished()),
            "audio playback stopped"
        );
        Ok(())
    }

    /// Remaining audio duration plus two seconds for callback scheduling/retirement.
    pub fn drain_timeout(&self) -> std::time::Duration {
        let stats = self.stats();
        let remaining = stats.queued_samples.saturating_sub(stats.rendered_samples);
        std::time::Duration::from_secs_f64(
            remaining as f64 / f64::from(self.config.sample_rate) / f64::from(self.config.channels),
        ) + std::time::Duration::from_secs(2)
    }

    /// Drain with a caller-specified deadline. Timeout/device failure still joins.
    pub async fn finish(&mut self, timeout: std::time::Duration) -> Result<()> {
        let drained = async {
            let deadline = std::time::Instant::now()
                .checked_add(timeout)
                .ok_or_else(|| anyhow::anyhow!("playback drain deadline overflow"))?;
            loop {
                self.check_running()?;
                if self.is_complete() {
                    return Ok(());
                }
                anyhow::ensure!(
                    std::time::Instant::now() < deadline,
                    "audio playback drain timed out"
                );
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        }
        .await;
        let stopped = self.stop();
        drained.and(stopped)
    }

    /// Stop admission, join callbacks and release all queued owners and ledger operations.
    pub fn stop(&mut self) -> Result<()> {
        self.stop.store(true, std::sync::atomic::Ordering::Release);
        if let Some(thread) = self.thread.take() {
            self.completion = Some(match thread.join() {
                Ok(result) => result.map_err(|error| error.to_string()),
                Err(_) => Err("playback control thread panicked".into()),
            });
        }
        self.completion
            .as_ref()
            .cloned()
            .unwrap_or(Ok(()))
            .map_err(anyhow::Error::msg)
    }
}

/// Test hook: the callback a cpal output stream would drive, for fake
/// devices in tests outside this crate (see
/// [`AudioPlayback::start_with_test_device`]).
#[doc(hidden)]
pub struct TestPlaybackDevice(control::PlaybackCallback);

impl TestPlaybackDevice {
    /// Fill one device buffer.
    pub fn render(&mut self, output: &mut [f32]) {
        self.0.render(output)
    }

    /// Report a device error, as cpal's error callback would.
    pub fn fail(&self) {
        self.0
            .error
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

impl AudioPlayback {
    /// Test hook: play to a fake device. `open` gets the device callback and
    /// returns the "stream"; dropping the stream must stop the device and
    /// drop the callback, as cpal does.
    #[doc(hidden)]
    pub fn start_with_test_device<S: 'static>(
        config: PlaybackConfig,
        copies: remotemedia_types::CopyContext,
        open: impl FnOnce(TestPlaybackDevice) -> S + Send + 'static,
    ) -> Result<Self> {
        Self::start_backend(config, copies, move |_, callback| {
            Ok((open(TestPlaybackDevice(callback)), "test output".into()))
        })
    }
}

impl Drop for AudioPlayback {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
/// Play audio samples synchronously
pub fn play_sync(samples: &[f32], config: PlaybackConfig) -> Result<()> {
    let mut playback = AudioPlayback::start(config)?;
    playback.try_queue(samples)?;
    let deadline = std::time::Instant::now()
        .checked_add(playback.drain_timeout())
        .ok_or_else(|| anyhow::anyhow!("playback drain deadline overflow"))?;

    // Wait for playback to complete
    while !playback.is_complete() {
        playback.check_running()?;
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "audio playback drain timed out"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    // Preserve the compatibility settling delay; this does not certify audible completion.
    std::thread::sleep(std::time::Duration::from_millis(50));

    playback.stop()
}
