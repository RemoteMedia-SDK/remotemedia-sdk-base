//! Audio device I/O on cpal (feature `microphone`): device selection, stream
//! config picking that never panics on an unsupported format, and live
//! capture. Used by the `MicInput` node and the CLI's audio commands.

use anyhow::{Context, Result};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{Device, Host, SupportedStreamConfig};
use std::sync::mpsc;
use tokio::sync::broadcast;

/// Which audio device to open: parsed from a `device` param or CLI argument
/// (`default`, an index, `hw:X,Y`, or a name substring).
#[derive(Debug, Clone)]
pub enum DeviceSelector {
    /// Use system default device
    Default,
    /// Select by exact name
    Name(String),
    /// Select by index (0-based)
    Index(usize),
    /// ALSA-style hardware specifier (hw:X,Y)
    AlsaHw { card: u32, device: u32 },
}

impl DeviceSelector {
    /// Parse a device selector from a string
    pub fn parse(s: &str) -> Self {
        let s = s.trim();

        // Handle explicit default
        if s.eq_ignore_ascii_case("default") || s.is_empty() {
            return DeviceSelector::Default;
        }

        // Try parsing as index
        if let Ok(idx) = s.parse::<usize>() {
            return DeviceSelector::Index(idx);
        }

        // Try parsing ALSA-style hw:X,Y or hw:X
        if s.starts_with("hw:") {
            let parts: Vec<&str> = s[3..].split(',').collect();
            if let Some(card) = parts.first().and_then(|p| p.parse::<u32>().ok()) {
                let device = parts
                    .get(1)
                    .and_then(|p| p.parse::<u32>().ok())
                    .unwrap_or(0);
                return DeviceSelector::AlsaHw { card, device };
            }
        }

        // Treat as device name
        DeviceSelector::Name(s.to_string())
    }
}

impl std::fmt::Display for DeviceSelector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeviceSelector::Default => write!(f, "default"),
            DeviceSelector::Name(name) => write!(f, "{}", name),
            DeviceSelector::Index(idx) => write!(f, "{}", idx),
            DeviceSelector::AlsaHw { card, device } => write!(f, "hw:{},{}", card, device),
        }
    }
}

/// Pick the stream config with `channels` channels at `sample_rate` from a
/// device's supported ranges (`supported_input_configs()` /
/// `supported_output_configs()`), preferring `f32` samples.
///
/// cpal's `with_sample_rate` panics on a rate the range does not cover (a
/// 48 kHz-only USB mic asked for 16 kHz); this returns an error naming the
/// rates the device does support instead. `channel_hint` is appended to the
/// error when no range has `channels` channels.
pub fn pick_stream_config(
    ranges: impl Iterator<Item = cpal::SupportedStreamConfigRange>,
    device_name: &str,
    channels: u16,
    sample_rate: u32,
    channel_hint: &str,
) -> Result<SupportedStreamConfig> {
    let all: Vec<_> = ranges.collect();
    let mut matching: Vec<_> = all
        .iter()
        .filter(|r| r.channels() == channels)
        .cloned()
        .collect();
    if matching.is_empty() {
        let counts: std::collections::BTreeSet<u16> = all.iter().map(|r| r.channels()).collect();
        let counts: Vec<String> = counts.iter().map(u16::to_string).collect();
        if counts.is_empty() {
            anyhow::bail!("Device '{device_name}' reports no supported stream configs.");
        }
        anyhow::bail!(
            "Device '{device_name}' doesn't support {channels} channel(s); it supports {} \
             channel(s).{channel_hint}",
            counts.join(", ")
        );
    }
    matching.sort_by_key(|r| r.sample_format() != cpal::SampleFormat::F32);
    let rates: Vec<String> = matching
        .iter()
        .map(|r| {
            let (lo, hi) = (r.min_sample_rate().0, r.max_sample_rate().0);
            if lo == hi {
                format!("{lo}")
            } else {
                format!("{lo}..{hi}")
            }
        })
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    matching
        .into_iter()
        .find_map(|r| r.try_with_sample_rate(cpal::SampleRate(sample_rate)))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Device '{device_name}' doesn't support {sample_rate} Hz with {channels} channel(s); \
                 it supports {} Hz. Set sample_rate to a supported rate (and resample with \
                 FastResampleNode where a node needs another rate).",
                rates.join(", ")
            )
        })
}

/// Get a specific audio host by name
pub fn get_host(name: Option<&str>) -> Result<Host> {
    match name {
        None => Ok(cpal::default_host()),
        Some(name) => {
            let name_lower = name.to_lowercase();

            for host_id in cpal::available_hosts() {
                let id_str = format!("{:?}", host_id).to_lowercase();
                if id_str.contains(&name_lower) {
                    return cpal::host_from_id(host_id)
                        .context(format!("Failed to initialize audio host: {}", name));
                }
            }

            anyhow::bail!(
                "Unknown audio host '{}'. Available hosts: {:?}",
                name,
                cpal::available_hosts()
            )
        }
    }
}

/// Find an input device by selector
pub fn find_input_device(selector: &DeviceSelector, host_name: Option<&str>) -> Result<Device> {
    let host = get_host(host_name)?;

    match selector {
        DeviceSelector::Default => host
            .default_input_device()
            .ok_or_else(|| anyhow::anyhow!("No default input device available")),
        DeviceSelector::Name(name) => {
            for device in host.input_devices()? {
                if let Ok(device_name) = device.name() {
                    if device_name.eq_ignore_ascii_case(name) || device_name.contains(name) {
                        return Ok(device);
                    }
                }
            }
            anyhow::bail!("Input device '{}' not found", name)
        }
        DeviceSelector::Index(idx) => host
            .input_devices()?
            .nth(*idx)
            .ok_or_else(|| anyhow::anyhow!("Input device index {} out of range", idx)),
        DeviceSelector::AlsaHw { card, device } => {
            // ALSA-style selector: look for device containing card/device number
            let pattern = format!("hw:{},{}", card, device);
            let alt_pattern = format!("card {}", card);

            for dev in host.input_devices()? {
                if let Ok(name) = dev.name() {
                    if name.contains(&pattern) || name.contains(&alt_pattern) {
                        return Ok(dev);
                    }
                }
            }
            anyhow::bail!("ALSA input device hw:{},{} not found", card, device)
        }
    }
}

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

/// Audio capture handle (Send + Sync safe)
///
/// The actual cpal::Stream runs on a dedicated thread, and this handle
/// communicates via channels. This makes it safe to use in async contexts.
pub struct AudioCapture {
    /// Receiver for audio data from the capture thread
    receiver: broadcast::Receiver<Vec<f32>>,
    /// Device name for logging
    device_name: String,
    /// Original config
    config: CaptureConfig,
    /// Handle to stop the capture thread
    _stop_tx: mpsc::Sender<()>,
}

// Ensure AudioCapture is Send + Sync
unsafe impl Send for AudioCapture {}
unsafe impl Sync for AudioCapture {}

impl AudioCapture {
    /// Start capturing from the specified or default input device
    pub fn start(config: CaptureConfig) -> Result<Self> {
        let device = Self::get_device(&config)?;
        let device_name = device.name().unwrap_or_else(|_| "Unknown".to_string());

        tracing::info!(
            "Starting audio capture from '{}' ({}Hz, {} ch)",
            device_name,
            config.sample_rate,
            config.channels
        );

        let supported_config = pick_stream_config(
            device.supported_input_configs()?,
            &device_name,
            config.channels,
            config.sample_rate,
            " Set `channels` to a count it supports (2 for a stereo-only USB mic).",
        )?;

        let (audio_tx, audio_rx) = broadcast::channel(100);
        let (stop_tx, stop_rx) = mpsc::channel();

        // Spawn a dedicated thread to own the cpal::Stream (which is !Send)
        let device_name_clone = device_name.clone();
        std::thread::spawn(move || {
            let stream = match device.build_input_stream(
                &supported_config.into(),
                move |data: &[f32], _: &cpal::InputCallbackInfo| {
                    let _ = audio_tx.send(data.to_vec());
                },
                |err| {
                    tracing::error!("Audio capture error: {}", err);
                },
                None,
            ) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Failed to build input stream: {}", e);
                    return;
                }
            };

            if let Err(e) = stream.play() {
                tracing::error!("Failed to start stream: {}", e);
                return;
            }

            tracing::debug!("Audio capture thread started for '{}'", device_name_clone);

            // Keep the thread alive until stop signal
            let _ = stop_rx.recv();

            tracing::debug!("Audio capture thread stopping for '{}'", device_name_clone);
            // Stream is dropped here, stopping capture
        });

        Ok(Self {
            receiver: audio_rx,
            device_name,
            config,
            _stop_tx: stop_tx,
        })
    }

    /// Get the device based on configuration
    fn get_device(config: &CaptureConfig) -> Result<Device> {
        match &config.device {
            Some(selector) => find_input_device(selector, config.host.as_deref()),
            None => {
                let host = get_host(config.host.as_deref())?;
                host.default_input_device()
                    .ok_or_else(|| anyhow::anyhow!("No default input device available"))
            }
        }
    }

    /// Receive the next audio buffer (async)
    pub async fn recv(&mut self) -> Option<Vec<f32>> {
        self.receiver.recv().await.ok()
    }

    /// Try to receive audio without blocking
    pub fn try_recv(&mut self) -> Option<Vec<f32>> {
        self.receiver.try_recv().ok()
    }

    /// Take every buffer captured so far, without blocking. Returns the
    /// samples and how many buffers were lost because the reader fell more
    /// than the channel's capacity behind (those are skipped, not fatal).
    pub fn drain(&mut self) -> (Vec<f32>, u64) {
        use tokio::sync::broadcast::error::TryRecvError;
        let mut out = Vec::new();
        let mut lost = 0u64;
        loop {
            match self.receiver.try_recv() {
                Ok(buf) => out.extend_from_slice(&buf),
                Err(TryRecvError::Lagged(n)) => lost += n,
                Err(TryRecvError::Empty) | Err(TryRecvError::Closed) => break,
            }
        }
        (out, lost)
    }

    /// Get the device name being used
    pub fn device_name(&self) -> &str {
        &self.device_name
    }

    /// Get the capture configuration
    pub fn config(&self) -> &CaptureConfig {
        &self.config
    }
}
