//! Codec conversion primitives.

use crate::{AudioCodec, Error, Result};
use std::collections::HashMap;

/// Describes decoded linear PCM handed to the pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedAudioFormat {
    /// Source codec.
    pub source_codec: AudioCodec,
    /// PCM sample rate in Hz.
    pub sample_rate_hz: u32,
    /// Channel count.
    pub channels: u16,
}

/// RTP payload mapping for one negotiated codec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodecPayload {
    /// Codec represented by the payload type.
    pub codec: AudioCodec,
    /// RTP payload type.
    pub payload_type: u8,
    /// RTP clock rate.
    pub clock_rate_hz: u32,
}

/// Mapping from payload types to codecs for one call session.
#[derive(Debug, Clone, Default)]
pub struct CodecMap {
    by_payload: HashMap<u8, CodecPayload>,
}

impl CodecMap {
    /// Insert a payload mapping.
    pub fn insert(&mut self, payload_type: u8, codec: AudioCodec) -> Result<()> {
        if payload_type > 127 {
            return Err(Error::Codec(format!(
                "RTP payload type must fit in 7 bits, got {payload_type}"
            )));
        }

        self.by_payload.insert(
            payload_type,
            CodecPayload {
                codec,
                payload_type,
                clock_rate_hz: codec.clock_rate_hz(),
            },
        );
        Ok(())
    }

    /// Lookup codec mapping by RTP payload type.
    pub fn get(&self, payload_type: u8) -> Option<&CodecPayload> {
        self.by_payload.get(&payload_type)
    }
}

/// Decode one G.711 mu-law byte into normalized f32 PCM.
pub fn decode_pcmu_byte(byte: u8) -> f32 {
    let u = !byte;
    let sign = u & 0x80;
    let exponent = (u >> 4) & 0x07;
    let mantissa = u & 0x0f;
    let sample = (((i16::from(mantissa) << 3) + 0x84) << exponent) - 0x84;
    let signed = if sign != 0 { -sample } else { sample };
    f32::from(signed) / 32768.0
}

/// Decode one G.711 A-law byte into normalized f32 PCM.
pub fn decode_pcma_byte(byte: u8) -> f32 {
    let a = byte ^ 0x55;
    let sign = a & 0x80;
    let exponent = (a >> 4) & 0x07;
    let mantissa = a & 0x0f;
    let sample = if exponent == 0 {
        (i16::from(mantissa) << 4) + 8
    } else {
        ((i16::from(mantissa) << 4) + 0x108) << (exponent - 1)
    };
    let signed = if sign == 0 { -sample } else { sample };
    f32::from(signed) / 32768.0
}

/// Decode a G.711 payload into normalized f32 PCM samples.
pub fn decode_g711(codec: AudioCodec, payload: &[u8]) -> Result<Vec<f32>> {
    match codec {
        AudioCodec::Pcmu => Ok(payload.iter().copied().map(decode_pcmu_byte).collect()),
        AudioCodec::Pcma => Ok(payload.iter().copied().map(decode_pcma_byte).collect()),
        AudioCodec::Opus => Err(Error::Codec(
            "Opus decoding is negotiated but not implemented in the G.711 helper".to_string(),
        )),
    }
}

/// Encode normalized f32 PCM samples into a G.711 payload.
pub fn encode_g711(codec: AudioCodec, samples: &[f32]) -> Result<Vec<u8>> {
    match codec {
        AudioCodec::Pcmu => Ok(samples.iter().copied().map(encode_pcmu_sample).collect()),
        AudioCodec::Pcma => Ok(samples.iter().copied().map(encode_pcma_sample).collect()),
        AudioCodec::Opus => Err(Error::Codec(
            "Opus encoding is not implemented in the G.711 helper".to_string(),
        )),
    }
}

/// Opus encoder/decoder pair for telephony RTP payloads.
pub struct OpusCodec {
    encoder: opus::Encoder,
    decoder: opus::Decoder,
    channels: u16,
}

impl std::fmt::Debug for OpusCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpusCodec")
            .field("channels", &self.channels)
            .finish_non_exhaustive()
    }
}

// SAFETY: Each OpusCodec owns independent encoder/decoder state and callers
// require `&mut self` for encode/decode operations.
unsafe impl Send for OpusCodec {}
unsafe impl Sync for OpusCodec {}

impl OpusCodec {
    /// Create an Opus codec for mono or stereo audio.
    pub fn new(sample_rate_hz: u32, channels: u16) -> Result<Self> {
        if !matches!(sample_rate_hz, 8_000 | 12_000 | 16_000 | 24_000 | 48_000) {
            return Err(Error::Codec(format!(
                "unsupported Opus sample rate: {sample_rate_hz}"
            )));
        }
        let opus_channels = match channels {
            1 => opus::Channels::Mono,
            2 => opus::Channels::Stereo,
            _ => {
                return Err(Error::Codec(format!(
                    "Opus supports mono or stereo, got {channels} channels"
                )));
            }
        };
        let mut encoder =
            opus::Encoder::new(sample_rate_hz, opus_channels, opus::Application::Voip)
                .map_err(|e| Error::Codec(format!("failed to create Opus encoder: {e:?}")))?;
        encoder
            .set_bitrate(opus::Bitrate::Bits(32_000))
            .map_err(|e| Error::Codec(format!("failed to set Opus bitrate: {e:?}")))?;
        let decoder = opus::Decoder::new(sample_rate_hz, opus_channels)
            .map_err(|e| Error::Codec(format!("failed to create Opus decoder: {e:?}")))?;
        Ok(Self {
            encoder,
            decoder,
            channels,
        })
    }

    /// Encode f32 PCM samples into one Opus packet.
    pub fn encode(&mut self, samples: &[f32]) -> Result<Vec<u8>> {
        let mut out = vec![0_u8; 4000];
        let len = self
            .encoder
            .encode_float(samples, &mut out)
            .map_err(|e| Error::Codec(format!("Opus encoding failed: {e:?}")))?;
        out.truncate(len);
        Ok(out)
    }

    /// Decode one Opus packet to f32 PCM samples.
    pub fn decode(&mut self, payload: &[u8]) -> Result<Vec<f32>> {
        let mut out = vec![0_f32; 5760 * usize::from(self.channels)];
        let len = self
            .decoder
            .decode_float(payload, &mut out, false)
            .map_err(|e| Error::Codec(format!("Opus decoding failed: {e:?}")))?;
        out.truncate(len * usize::from(self.channels));
        Ok(out)
    }
}

fn encode_pcmu_sample(sample: f32) -> u8 {
    let pcm = f32_to_i16(sample);
    let sign = if pcm < 0 { 0x80 } else { 0 };
    let magnitude = pcm.unsigned_abs().min(32635) as i16 + 0x84;
    let exponent = if (magnitude & 0x4000) != 0 {
        7
    } else if (magnitude & 0x2000) != 0 {
        6
    } else if (magnitude & 0x1000) != 0 {
        5
    } else if (magnitude & 0x0800) != 0 {
        4
    } else if (magnitude & 0x0400) != 0 {
        3
    } else if (magnitude & 0x0200) != 0 {
        2
    } else if (magnitude & 0x0100) != 0 {
        1
    } else {
        0
    };
    let mantissa = (magnitude >> (exponent + 3)) & 0x0f;
    !(sign | (exponent << 4) | mantissa as u8)
}

fn encode_pcma_sample(sample: f32) -> u8 {
    let pcm = f32_to_i16(sample);
    let sign = if pcm >= 0 { 0x80 } else { 0 };
    let magnitude = pcm.unsigned_abs().min(32635) as i16;
    let (exponent, mantissa) = if magnitude < 256 {
        (0, (magnitude >> 4) & 0x0f)
    } else if (magnitude & 0x4000) != 0 {
        (7, (magnitude >> 10) & 0x0f)
    } else if (magnitude & 0x2000) != 0 {
        (6, (magnitude >> 9) & 0x0f)
    } else if (magnitude & 0x1000) != 0 {
        (5, (magnitude >> 8) & 0x0f)
    } else if (magnitude & 0x0800) != 0 {
        (4, (magnitude >> 7) & 0x0f)
    } else if (magnitude & 0x0400) != 0 {
        (3, (magnitude >> 6) & 0x0f)
    } else if (magnitude & 0x0200) != 0 {
        (2, (magnitude >> 5) & 0x0f)
    } else {
        (1, (magnitude >> 4) & 0x0f)
    };
    (sign | (exponent << 4) | mantissa as u8) ^ 0x55
}

fn f32_to_i16(sample: f32) -> i16 {
    (sample.clamp(-1.0, 1.0) * 32767.0).round() as i16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_payload_types() {
        let mut map = CodecMap::default();
        map.insert(0, AudioCodec::Pcmu).unwrap();
        map.insert(8, AudioCodec::Pcma).unwrap();
        assert_eq!(map.get(0).unwrap().codec, AudioCodec::Pcmu);
        assert_eq!(map.get(8).unwrap().clock_rate_hz, 8_000);
    }

    #[test]
    fn decodes_g711_payloads() {
        let pcmu = decode_g711(AudioCodec::Pcmu, &[0xff, 0x7f]).unwrap();
        let pcma = decode_g711(AudioCodec::Pcma, &[0xd5, 0x55]).unwrap();
        assert_eq!(pcmu.len(), 2);
        assert_eq!(pcma.len(), 2);
        assert!(pcmu.iter().all(|s| (-1.0..=1.0).contains(s)));
        assert!(pcma.iter().all(|s| (-1.0..=1.0).contains(s)));
    }

    #[test]
    fn encodes_g711_payloads() {
        let samples = [-0.25, 0.0, 0.25];
        let pcmu = encode_g711(AudioCodec::Pcmu, &samples).unwrap();
        let pcma = encode_g711(AudioCodec::Pcma, &samples).unwrap();
        assert_eq!(pcmu.len(), samples.len());
        assert_eq!(pcma.len(), samples.len());
    }

    #[test]
    fn opus_round_trip_silence() {
        let mut codec = OpusCodec::new(48_000, 1).unwrap();
        let encoded = codec.encode(&vec![0.0; 960]).unwrap();
        let decoded = codec.decode(&encoded).unwrap();
        assert!(!encoded.is_empty());
        assert_eq!(decoded.len(), 960);
    }

    #[test]
    fn pcmu_round_trip_precision() {
        for i in -100..=100 {
            let sample = (i as f32) / 100.0;
            let encoded = encode_pcmu_sample(sample);
            let decoded = decode_pcmu_byte(encoded);
            let error = (sample - decoded).abs();
            // Maximum error for G.711 PCMU (8-bit log) should be quite small
            assert!(
                error < 0.05,
                "PCMU error too high at sample {}: decoded {}, error {}",
                sample,
                decoded,
                error
            );
        }
    }

    #[test]
    fn pcma_round_trip_precision() {
        for i in -100..=100 {
            let sample = (i as f32) / 100.0;
            let encoded = encode_pcma_sample(sample);
            let decoded = decode_pcma_byte(encoded);
            let error = (sample - decoded).abs();
            // Maximum error for G.711 PCMA should also be small
            assert!(
                error < 0.05,
                "PCMA error too high at sample {}: decoded {}, error {}",
                sample,
                decoded,
                error
            );
        }
    }
}
