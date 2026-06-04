//! Minimal WAV + base64 encoding for `RuntimeData::Audio` going to
//! OpenAI-shape vision/audio LLMs.
//!
//! gpt-4o-audio (and the equivalent vLLM / llama.cpp adapters) accept
//! audio content-parts shaped as
//! `{type:"input_audio", input_audio:{data:"<base64>", format:"wav"}}`.
//! This helper builds the 44-byte RIFF/WAVE header in front of the
//! IEEE-754-LE f32 PCM samples that `RuntimeData::Audio` already
//! carries, then base64-encodes the whole blob.
//!
//! Resampling is **not** done here — the multimodal node declares an
//! audio-input capability and the spec-023 capability resolver auto-
//! inserts an upstream resampler when there's a mismatch. Encoding to
//! WAV is purely a container wrap.

use crate::data::{AudioSamples, RuntimeData};
use crate::error::Error;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;

const RIFF_HEADER: usize = 44;
/// IEEE 754 32-bit float PCM. Per Microsoft's WAVE spec
/// (`WAVE_FORMAT_IEEE_FLOAT`).
const WAVE_FORMAT_IEEE_FLOAT: u16 = 0x0003;
const BITS_PER_SAMPLE: u16 = 32;

/// Build a base64-encoded WAV blob from an audio sample slice.
pub fn samples_to_wav_base64(
    samples: &[f32],
    sample_rate: u32,
    channels: u32,
) -> Result<String, Error> {
    if channels == 0 {
        return Err(Error::Execution(
            "samples_to_wav_base64: channels must be ≥ 1".into(),
        ));
    }
    let channels_u16 = u16::try_from(channels).map_err(|_| {
        Error::Execution(format!(
            "samples_to_wav_base64: channels {} > u16::MAX",
            channels
        ))
    })?;

    let pcm_bytes_len = samples
        .len()
        .checked_mul(4)
        .ok_or_else(|| Error::Execution("samples_to_wav_base64: pcm length overflow".into()))?;
    let mut buf: Vec<u8> = Vec::with_capacity(RIFF_HEADER + pcm_bytes_len);

    let byte_rate = sample_rate * channels * (BITS_PER_SAMPLE as u32 / 8);
    let block_align: u16 = channels_u16 * (BITS_PER_SAMPLE / 8);
    // RIFF/WAVE chunk sizes are 32-bit; saturate rather than panic on
    // implausibly long buffers.
    let data_chunk_size = u32::try_from(pcm_bytes_len).unwrap_or(u32::MAX);
    let riff_size = data_chunk_size.saturating_add(36);

    buf.extend_from_slice(b"RIFF");
    buf.extend_from_slice(&riff_size.to_le_bytes());
    buf.extend_from_slice(b"WAVE");

    // `fmt ` sub-chunk (16 bytes for PCM).
    buf.extend_from_slice(b"fmt ");
    buf.extend_from_slice(&16u32.to_le_bytes());
    buf.extend_from_slice(&WAVE_FORMAT_IEEE_FLOAT.to_le_bytes());
    buf.extend_from_slice(&channels_u16.to_le_bytes());
    buf.extend_from_slice(&sample_rate.to_le_bytes());
    buf.extend_from_slice(&byte_rate.to_le_bytes());
    buf.extend_from_slice(&block_align.to_le_bytes());
    buf.extend_from_slice(&BITS_PER_SAMPLE.to_le_bytes());

    // `data` sub-chunk.
    buf.extend_from_slice(b"data");
    buf.extend_from_slice(&data_chunk_size.to_le_bytes());
    for s in samples {
        buf.extend_from_slice(&s.to_le_bytes());
    }

    Ok(BASE64.encode(&buf))
}

/// Build a base64 WAV blob from a `RuntimeData::Audio` frame.
pub fn audio_to_wav_base64(data: &RuntimeData) -> Result<String, Error> {
    match data {
        RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            ..
        } => {
            let slice: &[f32] = samples_slice(samples);
            samples_to_wav_base64(slice, *sample_rate, *channels)
        }
        other => Err(Error::Execution(format!(
            "audio_to_wav_base64: expected RuntimeData::Audio, got {}",
            other.data_type()
        ))),
    }
}

fn samples_slice(samples: &AudioSamples) -> &[f32] {
    samples.as_ref()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_is_44_bytes_and_well_formed() {
        let samples = vec![0.0f32; 16];
        let b64 = samples_to_wav_base64(&samples, 16000, 1).unwrap();
        let buf = BASE64.decode(b64).unwrap();
        assert_eq!(&buf[..4], b"RIFF");
        assert_eq!(&buf[8..12], b"WAVE");
        assert_eq!(&buf[12..16], b"fmt ");
        // Sub-chunk1 size = 16
        assert_eq!(u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]), 16);
        // Audio format = 3 (IEEE float)
        assert_eq!(u16::from_le_bytes([buf[20], buf[21]]), 3);
        // Channels = 1
        assert_eq!(u16::from_le_bytes([buf[22], buf[23]]), 1);
        // Sample rate
        assert_eq!(
            u32::from_le_bytes([buf[24], buf[25], buf[26], buf[27]]),
            16000
        );
        // Bits per sample
        assert_eq!(u16::from_le_bytes([buf[34], buf[35]]), 32);
        assert_eq!(&buf[36..40], b"data");
        // data size = 16 samples * 4 bytes = 64
        assert_eq!(u32::from_le_bytes([buf[40], buf[41], buf[42], buf[43]]), 64);
        assert_eq!(buf.len(), 44 + 64);
    }

    #[test]
    fn pcm_payload_round_trips() {
        let samples = vec![0.5f32, -0.25, 1.0, -1.0];
        let b64 = samples_to_wav_base64(&samples, 8000, 1).unwrap();
        let buf = BASE64.decode(b64).unwrap();
        let pcm = &buf[44..];
        for (i, expected) in samples.iter().enumerate() {
            let bytes = [pcm[i * 4], pcm[i * 4 + 1], pcm[i * 4 + 2], pcm[i * 4 + 3]];
            let got = f32::from_le_bytes(bytes);
            assert!((got - expected).abs() < f32::EPSILON, "i={}", i);
        }
    }

    #[test]
    fn audio_to_wav_base64_handles_runtime_data() {
        let audio = RuntimeData::Audio {
            samples: vec![0.0f32; 8].into(),
            sample_rate: 16000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        };
        let b64 = audio_to_wav_base64(&audio).unwrap();
        let buf = BASE64.decode(b64).unwrap();
        assert_eq!(buf.len(), 44 + 8 * 4);
    }

    #[test]
    fn non_audio_input_rejected() {
        let err = audio_to_wav_base64(&RuntimeData::Text("hi".into())).unwrap_err();
        assert!(format!("{}", err).contains("expected RuntimeData::Audio"));
    }
}
