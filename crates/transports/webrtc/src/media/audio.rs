//! Audio codec support (Opus)
//!
//! Opus codec is always enabled for WebRTC audio support.

// Phase 4 (US2) audio infrastructure - allow unused until multi-peer sync is implemented
#![allow(dead_code)]

use crate::{Error, Result};

/// Audio frame duration in milliseconds (Opus standard)
pub const FRAME_DURATION_MS: u32 = 20;

/// Number of audio frames per second (1000ms / 20ms per frame)
pub const FRAMES_PER_SECOND: usize = 50;

/// Default ring buffer duration in seconds.
///
/// Sized for *jitter absorption*, not for "queue an entire response."
/// TTS engines (Kokoro, Coqui, Piper, etc.) commonly synthesise audio
/// faster than real-time and emit chunks in bursts. The audio sender
/// transmits at strict real-time pace (50 fps × 20 ms = 1× content
/// rate), so a too-deep buffer turns those bursts into multi-second
/// playback delays — the listener hears each new sentence further and
/// further behind generation. Producers respect back-pressure
/// (`AudioRingBuffer::push().await`), so a tight cap forces them to
/// emit at real-time pace and keeps perceived latency low.
///
/// Two seconds is enough to ride out short OS scheduling hiccups
/// without audible underflow while keeping the gen-to-play delay
/// under one chunk for typical TTS chunk sizes (~1–3 s).
pub const DEFAULT_BUFFER_SECONDS: usize = 2;

/// Default ring buffer capacity (frames). 50 fps × 2 s = 100 frames.
pub const DEFAULT_RING_BUFFER_CAPACITY: usize = FRAMES_PER_SECOND * DEFAULT_BUFFER_SECONDS;

/// Sample rates accepted by the standard libopus encoder/decoder API.
///
/// Opus RTP/WebRTC commonly advertises a 48 kHz RTP clock, but libopus can
/// encode and decode PCM provided at these rates.
pub const OPUS_SUPPORTED_SAMPLE_RATES: &[u32] = &[8_000, 12_000, 16_000, 24_000, 48_000];

fn is_supported_opus_sample_rate(sample_rate: u32) -> bool {
    OPUS_SUPPORTED_SAMPLE_RATES.contains(&sample_rate)
}

/// Audio encoder configuration
#[derive(Debug, Clone)]
pub struct AudioEncoderConfig {
    /// Sample rate in Hz (typically 48000)
    pub sample_rate: u32,
    /// Number of channels (1 = mono, 2 = stereo)
    pub channels: u16,
    /// Bitrate in bits per second (typically 32000-128000)
    pub bitrate: u32,
    /// Complexity (0-10, higher = better quality but slower)
    pub complexity: u32,
    /// Ring buffer capacity in frames. Default: `DEFAULT_RING_BUFFER_CAPACITY`
    /// (~2 s of audio). Sized for **jitter absorption**, not bulk
    /// queuing — see the constant's docs. Raise only when you have a
    /// good reason (e.g. a non-real-time export pipeline that wants to
    /// pre-render long TTS outputs); for interactive/avatar use cases
    /// keep this small so the producer back-pressures at real-time
    /// pace and the listener hears audio close to generation time.
    pub ring_buffer_capacity: usize,
}

impl Default for AudioEncoderConfig {
    fn default() -> Self {
        Self {
            sample_rate: 48000,
            channels: 1,
            bitrate: 64000,
            complexity: 10,
            ring_buffer_capacity: DEFAULT_RING_BUFFER_CAPACITY,
        }
    }
}

/// Audio encoder (Opus)
pub struct AudioEncoder {
    pub(crate) config: AudioEncoderConfig,
    encoder: opus::Encoder,
}

// SAFETY: Opus encoder is actually thread-safe despite raw pointers in FFI.
// Each encoder instance is independent and mutations are protected by RwLock in AudioTrack.
unsafe impl Send for AudioEncoder {}
unsafe impl Sync for AudioEncoder {}

impl AudioEncoder {
    /// Create a new audio encoder
    pub fn new(config: AudioEncoderConfig) -> Result<Self> {
        // Validate configuration
        if !is_supported_opus_sample_rate(config.sample_rate) {
            return Err(Error::InvalidConfig(
                "Opus sample rate must be one of 8000, 12000, 16000, 24000, or 48000 Hz"
                    .to_string(),
            ));
        }

        if config.channels != 1 && config.channels != 2 {
            return Err(Error::InvalidConfig(
                "Opus supports 1 (mono) or 2 (stereo) channels".to_string(),
            ));
        }

        if config.complexity > 10 {
            return Err(Error::InvalidConfig(
                "Opus complexity must be 0-10".to_string(),
            ));
        }

        // Create Opus encoder
        let channels = match config.channels {
            1 => opus::Channels::Mono,
            2 => opus::Channels::Stereo,
            _ => unreachable!(),
        };

        let mut encoder = opus::Encoder::new(config.sample_rate, channels, opus::Application::Voip)
            .map_err(|e| Error::EncodingError(format!("Failed to create Opus encoder: {:?}", e)))?;

        // Configure encoder
        encoder
            .set_bitrate(opus::Bitrate::Bits(config.bitrate as i32))
            .map_err(|e| Error::EncodingError(format!("Failed to set bitrate: {:?}", e)))?;

        Ok(Self { config, encoder })
    }

    /// Encode audio samples to Opus format
    ///
    /// # Arguments
    ///
    /// * `samples` - Input audio samples as f32 (range -1.0 to 1.0)
    ///
    /// # Returns
    ///
    /// Encoded Opus packet as bytes (RTP payload)
    pub fn encode(&mut self, samples: &[f32]) -> Result<Vec<u8>> {
        // Opus expects samples in range -1.0 to 1.0
        // Allocate buffer for encoded output (max Opus packet size)
        const MAX_PACKET_SIZE: usize = 4000;
        let mut output = vec![0u8; MAX_PACKET_SIZE];

        // Encode the samples
        let len = self
            .encoder
            .encode_float(samples, &mut output)
            .map_err(|e| Error::EncodingError(format!("Opus encoding failed: {}", e)))?;

        // Truncate to actual encoded size
        output.truncate(len);
        Ok(output)
    }
}

/// Audio decoder (Opus)
pub struct AudioDecoder {
    config: AudioEncoderConfig,
    decoder: opus::Decoder,
}

// SAFETY: Opus decoder is actually thread-safe despite raw pointers in FFI.
// Each decoder instance is independent and mutations are protected by RwLock in AudioTrack.
unsafe impl Send for AudioDecoder {}
unsafe impl Sync for AudioDecoder {}

impl AudioDecoder {
    /// Create a new audio decoder
    pub fn new(config: AudioEncoderConfig) -> Result<Self> {
        // Validate configuration
        if !is_supported_opus_sample_rate(config.sample_rate) {
            return Err(Error::InvalidConfig(
                "Opus sample rate must be one of 8000, 12000, 16000, 24000, or 48000 Hz"
                    .to_string(),
            ));
        }

        if config.channels != 1 && config.channels != 2 {
            return Err(Error::InvalidConfig(
                "Opus supports 1 (mono) or 2 (stereo) channels".to_string(),
            ));
        }

        // Create Opus decoder
        let channels = match config.channels {
            1 => opus::Channels::Mono,
            2 => opus::Channels::Stereo,
            _ => unreachable!(),
        };

        let decoder = opus::Decoder::new(config.sample_rate, channels)
            .map_err(|e| Error::EncodingError(format!("Failed to create Opus decoder: {:?}", e)))?;

        Ok(Self { config, decoder })
    }

    /// Decode Opus packet to audio samples
    ///
    /// # Arguments
    ///
    /// * `payload` - Encoded Opus packet (RTP payload)
    ///
    /// # Returns
    ///
    /// Decoded audio samples as f32 (range -1.0 to 1.0) at the configured sample rate.
    pub fn decode(&mut self, payload: &[u8]) -> Result<Vec<f32>> {
        // Opus frame size: typically 2.5, 5, 10, 20, 40, or 60 ms
        // Max frame size for Opus is 120ms @ 48kHz = 5760 samples per channel
        const MAX_FRAME_SIZE: usize = 5760;
        let mut output = vec![0f32; MAX_FRAME_SIZE * self.config.channels as usize];

        // Decode the packet
        let len = self
            .decoder
            .decode_float(payload, &mut output, false)
            .map_err(|e| Error::EncodingError(format!("Opus decoding failed: {:?}", e)))?;

        // Truncate to actual decoded size
        output.truncate(len * self.config.channels as usize);
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_audio_encoder_config_default() {
        let config = AudioEncoderConfig::default();
        assert_eq!(config.sample_rate, 48000);
        assert_eq!(config.channels, 1);
        assert_eq!(config.bitrate, 64000);
    }

    #[test]
    fn test_audio_encoder_creation() {
        let config = AudioEncoderConfig::default();
        let encoder = AudioEncoder::new(config);
        assert!(encoder.is_ok());
    }

    #[test]
    fn test_audio_encoder_accepts_libopus_sample_rates() {
        for sample_rate in OPUS_SUPPORTED_SAMPLE_RATES {
            let config = AudioEncoderConfig {
                sample_rate: *sample_rate,
                ..Default::default()
            };
            let mut encoder = AudioEncoder::new(config).unwrap();
            let frame_samples = (*sample_rate as usize * FRAME_DURATION_MS as usize) / 1000;
            let encoded = encoder.encode(&vec![0.0; frame_samples]).unwrap();
            assert!(!encoded.is_empty(), "sample_rate={sample_rate}");
        }
    }

    #[test]
    fn test_audio_encoder_invalid_sample_rate() {
        let config = AudioEncoderConfig {
            sample_rate: 44100, // Not supported by Opus
            ..Default::default()
        };
        let encoder = AudioEncoder::new(config);
        assert!(encoder.is_err());
    }

    #[test]
    fn test_audio_decoder_creation() {
        let config = AudioEncoderConfig::default();
        let decoder = AudioDecoder::new(config);
        assert!(decoder.is_ok());
    }

    #[test]
    fn test_audio_decoder_accepts_libopus_sample_rates() {
        for sample_rate in OPUS_SUPPORTED_SAMPLE_RATES {
            let config = AudioEncoderConfig {
                sample_rate: *sample_rate,
                ..Default::default()
            };
            assert!(
                AudioDecoder::new(config).is_ok(),
                "sample_rate={sample_rate}"
            );
        }
    }
}
