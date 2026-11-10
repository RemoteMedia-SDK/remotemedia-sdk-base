//! Audio device I/O on cpal (features `microphone` / `speaker`): device
//! selection, stream config picking that never panics on an unsupported
//! format, channel mapping for speakers with another layout, live capture
//! and playback. Used by `MicInput`, `SpeakerOutput` and the CLI's audio
//! commands.

use anyhow::{Context, Result};
use cpal::traits::{DeviceTrait, HostTrait};
use cpal::{Device, Host, SupportedStreamConfig};

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
    hint: &str,
) -> Result<SupportedStreamConfig> {
    let all: Vec<_> = ranges.collect();
    if all.is_empty() {
        anyhow::bail!("Device '{device_name}' reports no supported stream configs.");
    }
    let mut matching: Vec<_> = all
        .iter()
        .filter(|r| r.channels() == channels)
        .cloned()
        .collect();
    matching.sort_by_key(|r| r.sample_format() != cpal::SampleFormat::F32);
    if let Some(c) = matching.iter().find_map(|r| {
        r.clone()
            .try_with_sample_rate(cpal::SampleRate(sample_rate))
    }) {
        return Ok(c);
    }
    Err(anyhow::anyhow!(unsupported_config_message(
        &all,
        device_name,
        channels,
        sample_rate,
        hint
    )))
}

/// Everything wrong with `channels` x `sample_rate` on a device, and every
/// combination it does support, in one message: an agent or user fixing the
/// params should not have to learn the constraints one failed start at a
/// time.
fn unsupported_config_message(
    all: &[cpal::SupportedStreamConfigRange],
    device_name: &str,
    channels: u16,
    sample_rate: u32,
    hint: &str,
) -> String {
    use std::collections::{BTreeMap, BTreeSet};
    let covers = |r: &cpal::SupportedStreamConfigRange| {
        r.min_sample_rate().0 <= sample_rate && sample_rate <= r.max_sample_rate().0
    };
    // channels -> the rates (or rate ranges) offered with that count.
    let mut by_channels: BTreeMap<u16, BTreeSet<(u32, u32)>> = BTreeMap::new();
    for r in all {
        by_channels
            .entry(r.channels())
            .or_default()
            .insert((r.min_sample_rate().0, r.max_sample_rate().0));
    }
    let rates_text = |rates: &BTreeSet<(u32, u32)>| {
        rates
            .iter()
            .map(|&(lo, hi)| {
                if lo == hi {
                    format!("{lo}")
                } else {
                    format!("{lo}..{hi}")
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut wrong = Vec::new();
    if !by_channels.contains_key(&channels) {
        wrong.push(format!("channels {channels} is not supported"));
    }
    if !all.iter().any(covers) {
        wrong.push(format!("sample_rate {sample_rate} is not supported"));
    } else if by_channels.contains_key(&channels) {
        wrong.push(format!(
            "sample_rate {sample_rate} is not supported with {channels} channel(s)"
        ));
    }
    let supported = by_channels
        .iter()
        .map(|(c, rates)| format!("{c} channel(s) at {} Hz", rates_text(rates)))
        .collect::<Vec<_>>()
        .join("; ");
    // One fixed combination: say exactly what to set.
    let only = match (by_channels.len(), by_channels.iter().next()) {
        (1, Some((c, rates))) if rates.len() == 1 => {
            let &(lo, hi) = rates.iter().next().expect("one rate");
            (lo == hi).then(|| format!(" Set channels: {c}, sample_rate: {lo}."))
        }
        _ => None,
    }
    .unwrap_or_else(|| " Set channels and sample_rate to one of these.".into());
    format!(
        "Device '{device_name}' cannot open {channels} channel(s) at {sample_rate} Hz: {}. \
         It supports {supported}.{only}{hint}",
        wrong.join(", ")
    )
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

pub(crate) mod capture;
#[doc(hidden)]
pub use capture::TestCaptureDevice;
pub use capture::{capture_sync, capture_sync_shared, AudioCapture, CaptureConfig};

/// Like [`pick_stream_config`] for an output device, but when it has no
/// config with `channels` channels (a sound card set to 7.1 offers only 8 in
/// shared mode), open it with the fewest channels above `channels` (else the
/// most) and let the caller map channels: see [`map_frame`]. Returns the
/// config and the device's channel count.
pub fn pick_output_config(
    ranges: impl Iterator<Item = cpal::SupportedStreamConfigRange>,
    device_name: &str,
    channels: u16,
    sample_rate: u32,
) -> Result<(SupportedStreamConfig, u16)> {
    let all: Vec<_> = ranges.collect();
    if all.iter().any(|r| r.channels() == channels) {
        let c = pick_stream_config(all.into_iter(), device_name, channels, sample_rate, "")?;
        return Ok((c, channels));
    }
    let counts: std::collections::BTreeSet<u16> = all.iter().map(|r| r.channels()).collect();
    let device_channels = counts
        .iter()
        .copied()
        .find(|&c| c > channels)
        .or_else(|| counts.iter().next_back().copied())
        .ok_or_else(|| {
            anyhow::anyhow!("Device '{device_name}' reports no supported stream configs.")
        })?;
    // The node maps `channels` onto the device's own, so only the rate can
    // be wrong here: do not tell the caller to change its channel count.
    let c = pick_stream_config(
        all.iter().cloned(),
        device_name,
        device_channels,
        sample_rate,
        "",
    )
    .map_err(|_| {
        anyhow::anyhow!(mapped_rate_message(
            &all,
            device_name,
            channels,
            device_channels,
            sample_rate
        ))
    })?;
    tracing::info!(
        "Device '{device_name}' has no {channels}-channel output; playing on its {device_channels} channels"
    );
    Ok((c, device_channels))
}

/// The error for an output device that has no `channels`-channel config
/// (so `channels` would be mapped onto its `device_channels`) and no
/// `sample_rate` either.
fn mapped_rate_message(
    all: &[cpal::SupportedStreamConfigRange],
    device_name: &str,
    channels: u16,
    device_channels: u16,
    sample_rate: u32,
) -> String {
    let rates: std::collections::BTreeSet<(u32, u32)> = all
        .iter()
        .filter(|r| r.channels() == device_channels)
        .map(|r| (r.min_sample_rate().0, r.max_sample_rate().0))
        .collect();
    let text = rates
        .iter()
        .map(|&(lo, hi)| {
            if lo == hi {
                format!("{lo}")
            } else {
                format!("{lo}..{hi}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let set = match rates.iter().next() {
        Some(&(lo, hi)) if rates.len() == 1 && lo == hi => format!("Set sample_rate: {lo}"),
        _ => "Set sample_rate to one of these".to_string(),
    };
    format!(
        "Device '{device_name}' cannot play at {sample_rate} Hz: sample_rate {sample_rate} is not \
         supported. It plays {channels}-channel audio on its {device_channels} channels at {text} Hz. \
         {set} (resample upstream with FastResampleNode where the audio has another rate)."
    )
}

/// Write one interleaved input frame (`input.len()` channels) to an output
/// frame with a different channel count: mono goes to the first two (front
/// left/right) channels, otherwise channels are copied in order and the rest
/// stay silent.
pub fn map_frame(input: &[f32], output: &mut [f32]) {
    let mono = input.len() == 1;
    for (d, out) in output.iter_mut().enumerate() {
        *out = if mono {
            if d < 2 {
                input[0]
            } else {
                0.0
            }
        } else {
            input.get(d).copied().unwrap_or(0.0)
        };
    }
}

/// An output device's name and the formats it offers, as
/// `(channels, min_rate, max_rate)` ranges, without opening a stream.
/// `selector` `None` is the host's default output device.
pub fn output_device_formats(
    selector: Option<&DeviceSelector>,
    host_name: Option<&str>,
) -> Result<(String, Vec<(u16, u32, u32)>)> {
    let device = match selector {
        Some(s) => find_output_device(s, host_name)?,
        None => get_host(host_name)?
            .default_output_device()
            .ok_or_else(|| anyhow::anyhow!("No default output device available"))?,
    };
    let name = device.name().unwrap_or_else(|_| "Unknown".to_string());
    let formats = device
        .supported_output_configs()?
        .map(|r| (r.channels(), r.min_sample_rate().0, r.max_sample_rate().0))
        .collect();
    Ok((name, formats))
}

/// An input device's name, the formats it offers as `(channels, min_rate,
/// max_rate)` ranges, and its own default `(channels, rate)`, without
/// opening a stream. `selector` `None` is the host's default input device.
pub fn input_device_formats(
    selector: Option<&DeviceSelector>,
    host_name: Option<&str>,
) -> Result<InputDeviceFormats> {
    let device = match selector {
        Some(s) => find_input_device(s, host_name)?,
        None => get_host(host_name)?
            .default_input_device()
            .ok_or_else(|| anyhow::anyhow!("No default input device available"))?,
    };
    let name = device.name().unwrap_or_else(|_| "Unknown".to_string());
    let formats = device
        .supported_input_configs()?
        .map(|r| (r.channels(), r.min_sample_rate().0, r.max_sample_rate().0))
        .collect();
    let default = device
        .default_input_config()
        .ok()
        .map(|c| (c.channels(), c.sample_rate().0));
    Ok(InputDeviceFormats {
        name,
        formats,
        default,
    })
}

/// What [`input_device_formats`] reports.
#[derive(Debug, Clone, PartialEq)]
pub struct InputDeviceFormats {
    pub name: String,
    /// `(channels, min_rate, max_rate)` per supported range.
    pub formats: Vec<(u16, u32, u32)>,
    /// The device's own `(channels, rate)` (its shared-mode format).
    pub default: Option<(u16, u32)>,
}

/// Find an output device by selector
pub fn find_output_device(selector: &DeviceSelector, host_name: Option<&str>) -> Result<Device> {
    let host = get_host(host_name)?;

    match selector {
        DeviceSelector::Default => host
            .default_output_device()
            .ok_or_else(|| anyhow::anyhow!("No default output device available")),
        DeviceSelector::Name(name) => {
            for device in host.output_devices()? {
                if let Ok(device_name) = device.name() {
                    if device_name.eq_ignore_ascii_case(name) || device_name.contains(name) {
                        return Ok(device);
                    }
                }
            }
            anyhow::bail!("Output device '{}' not found", name)
        }
        DeviceSelector::Index(idx) => host
            .output_devices()?
            .nth(*idx)
            .ok_or_else(|| anyhow::anyhow!("Output device index {} out of range", idx)),
        DeviceSelector::AlsaHw { card, device } => {
            let pattern = format!("hw:{},{}", card, device);
            let alt_pattern = format!("card {}", card);

            for dev in host.output_devices()? {
                if let Ok(name) = dev.name() {
                    if name.contains(&pattern) || name.contains(&alt_pattern) {
                        return Ok(dev);
                    }
                }
            }
            anyhow::bail!("ALSA output device hw:{},{} not found", card, device)
        }
    }
}

pub(crate) mod playback;
#[doc(hidden)]
pub use playback::TestPlaybackDevice;
pub use playback::{play_sync, AudioPlayback, PlaybackConfig};

#[cfg(test)]
mod tests {
    use super::*;
    use cpal::{SampleFormat, SampleRate, SupportedBufferSize, SupportedStreamConfigRange};

    fn range(ch: u16, lo: u32, hi: u32) -> SupportedStreamConfigRange {
        SupportedStreamConfigRange::new(
            ch,
            SampleRate(lo),
            SampleRate(hi),
            SupportedBufferSize::Unknown,
            SampleFormat::F32,
        )
    }

    fn err(ranges: Vec<SupportedStreamConfigRange>, ch: u16, rate: u32) -> String {
        pick_stream_config(ranges.into_iter(), "Mic", ch, rate, "")
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn a_stereo_48k_only_mic_names_both_wrong_params_and_the_fix_at_once() {
        let msg = err(vec![range(2, 48000, 48000)], 1, 16000);
        assert!(msg.contains("channels 1 is not supported"), "{msg}");
        assert!(msg.contains("sample_rate 16000 is not supported"), "{msg}");
        assert!(
            msg.contains("It supports 2 channel(s) at 48000 Hz."),
            "{msg}"
        );
        assert!(
            msg.contains("Set channels: 2, sample_rate: 48000."),
            "{msg}"
        );
        assert!(!msg.contains("  "), "no stray whitespace: {msg}");
    }

    #[test]
    fn a_rate_only_valid_with_other_channels_says_so_and_lists_every_combination() {
        let msg = err(
            vec![range(1, 8000, 48000), range(2, 48000, 48000)],
            2,
            16000,
        );
        assert!(
            msg.contains("sample_rate 16000 is not supported with 2 channel(s)"),
            "{msg}"
        );
        assert!(!msg.contains("channels 2 is not supported"), "{msg}");
        assert!(
            msg.contains("1 channel(s) at 8000..48000 Hz; 2 channel(s) at 48000 Hz"),
            "{msg}"
        );
        assert!(
            msg.contains("Set channels and sample_rate to one of these."),
            "{msg}"
        );
    }

    #[test]
    fn a_speaker_with_another_layout_is_only_asked_for_a_rate() {
        let err = pick_output_config(vec![range(8, 48000, 48000)].into_iter(), "X4", 2, 16000)
            .unwrap_err()
            .to_string();
        assert!(err.contains("cannot play at 16000 Hz"), "{err}");
        assert!(
            err.contains("plays 2-channel audio on its 8 channels at 48000 Hz"),
            "{err}"
        );
        assert!(err.contains("Set sample_rate: 48000"), "{err}");
        assert!(!err.contains("Set channels"), "{err}");
        assert!(!err.contains("  "), "no stray whitespace: {err}");
    }

    #[test]
    fn a_supported_combination_opens() {
        let c = pick_stream_config(
            vec![range(2, 44100, 48000)].into_iter(),
            "Mic",
            2,
            48000,
            "",
        )
        .unwrap();
        assert_eq!((c.channels(), c.sample_rate().0), (2, 48000));
    }
}
