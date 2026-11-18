//! `AudioFileDecoderNode`: an encoded audio file in, its audio out.
//!
//! Takes the bytes of a container (MP3, AAC/M4A, FLAC, Ogg Vorbis or Opus,
//! WebM, WAV) as `RuntimeData::Binary` and emits the decoded audio as one
//! `RuntimeData::Audio` (interleaved f32 at the source rate and channel
//! count). It uses the runtime's FFmpeg, the same decode `FileIngestPlugin`
//! uses (`ingestion::file::decode_audio_with_ffmpeg`), so it exists only in
//! builds with the `video` feature. What it can decode is what the vendored
//! FFmpeg was built with.
//!
//! Used by the AI SDK provider's `transcribe` for compressed input
//! (openspec: extend-ai-sdk-media-streaming, design D9).

use crate::data::RuntimeData;
use crate::error::Error;
use crate::nodes::schema::{NodeSchema, RuntimeDataType};
use crate::nodes::{StreamingNode, StreamingNodeFactory, SyncNodeWrapper, SyncStreamingNode};
use serde_json::Value;

/// The node type name in manifests.
pub const AUDIO_FILE_DECODER_NODE: &str = "AudioFileDecoderNode";

/// See the module documentation.
pub struct AudioFileDecoderNode;

impl AudioFileDecoderNode {
    /// Decode one file's bytes.
    pub fn decode(bytes: &[u8]) -> Result<RuntimeData, Error> {
        let decoded = crate::ingestion::file::decode_audio_with_ffmpeg(
            std::io::Cursor::new(bytes.to_vec()),
            "the audio file",
        )?;
        if decoded.samples.is_empty() {
            return Err(Error::Execution(format!(
                "{AUDIO_FILE_DECODER_NODE}: the file has an audio stream but no samples were decoded"
            )));
        }
        let mut metadata = serde_json::Map::new();
        if let Some(codec) = decoded.codec {
            metadata.insert("codec".into(), Value::String(codec));
        }
        Ok(RuntimeData::Audio {
            samples: decoded.samples.into(),
            sample_rate: decoded.sample_rate,
            channels: decoded.channels,
            stream_id: None,
            timestamp_us: Some(0),
            arrival_ts_us: None,
            metadata: Some(Value::Object(metadata)),
        })
    }
}

impl SyncStreamingNode for AudioFileDecoderNode {
    fn node_type(&self) -> &str {
        AUDIO_FILE_DECODER_NODE
    }

    fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        match data {
            RuntimeData::Binary(bytes) => Self::decode(bytes.as_slice()),
            other => Err(Error::Execution(format!(
                "{AUDIO_FILE_DECODER_NODE} takes the bytes of an audio file (binary), not {:?}",
                other.data_type()
            ))),
        }
    }
}

/// Factory for [`AudioFileDecoderNode`].
pub struct AudioFileDecoderNodeFactory;

impl StreamingNodeFactory for AudioFileDecoderNodeFactory {
    fn create(
        &self,
        _node_id: String,
        _params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        Ok(Box::new(SyncNodeWrapper(AudioFileDecoderNode)))
    }

    fn node_type(&self) -> &str {
        AUDIO_FILE_DECODER_NODE
    }

    fn schema(&self) -> Option<NodeSchema> {
        Some(
            NodeSchema::new(AUDIO_FILE_DECODER_NODE)
                .description(
                    "Decodes an audio file (MP3, AAC/M4A, FLAC, Ogg Vorbis/Opus, WebM, WAV) with the runtime's FFmpeg",
                )
                .category("audio")
                .accepts([RuntimeDataType::Binary])
                .produces([RuntimeDataType::Audio]),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A 16-bit mono WAV of a 440 Hz tone.
    fn wav(rate: u32, seconds: f32) -> Vec<u8> {
        let n = (rate as f32 * seconds) as usize;
        let pcm: Vec<i16> = (0..n)
            .map(|i| {
                ((i as f32 * 440.0 * std::f32::consts::TAU / rate as f32).sin() * 12000.0) as i16
            })
            .collect();
        let data_len = (pcm.len() * 2) as u32;
        let mut out = Vec::new();
        out.extend_from_slice(b"RIFF");
        out.extend_from_slice(&(36 + data_len).to_le_bytes());
        out.extend_from_slice(b"WAVEfmt ");
        out.extend_from_slice(&16u32.to_le_bytes());
        out.extend_from_slice(&1u16.to_le_bytes());
        out.extend_from_slice(&1u16.to_le_bytes());
        out.extend_from_slice(&rate.to_le_bytes());
        out.extend_from_slice(&(rate * 2).to_le_bytes());
        out.extend_from_slice(&2u16.to_le_bytes());
        out.extend_from_slice(&16u16.to_le_bytes());
        out.extend_from_slice(b"data");
        out.extend_from_slice(&data_len.to_le_bytes());
        for s in pcm {
            out.extend_from_slice(&s.to_le_bytes());
        }
        out
    }

    #[test]
    fn decodes_a_file_from_memory() {
        let out = AudioFileDecoderNode
            .process(RuntimeData::Binary(wav(16_000, 0.5).into()))
            .unwrap();
        let RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            ..
        } = out
        else {
            panic!("expected audio");
        };
        assert_eq!((sample_rate, channels), (16_000, 1));
        assert_eq!(samples.len(), 8_000);
        let peak = samples.iter().fold(0.0f32, |a, s| a.max(s.abs()));
        assert!((peak - 12000.0 / 32768.0).abs() < 0.01, "peak {peak}");
    }

    /// Compressed one-second 440 Hz tones made with FFmpeg
    /// (`tests/fixtures/audio_file_decoder`): (file, rate, channels, peak).
    /// The peaks are FFmpeg's own `astats` measurement of each file.
    const FIXTURES: &[(&str, u32, u32, f32)] = &[
        ("tone-stereo.mp3", 44_100, 2, 0.0332),
        ("tone.m4a", 48_000, 1, 0.0508),
        ("tone.flac", 16_000, 1, 0.0462),
        ("tone.ogg", 48_000, 1, 0.0474),
        ("tone-vorbis.ogg", 44_100, 2, 0.0332),
        ("tone.webm", 48_000, 1, 0.0474),
    ];

    #[test]
    fn decodes_compressed_formats_with_every_channel() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/audio_file_decoder");
        for &(name, rate, chans, expected) in FIXTURES {
            let bytes = std::fs::read(dir.join(name)).unwrap();
            let out = AudioFileDecoderNode
                .process(RuntimeData::Binary(bytes.into()))
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            let RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                ..
            } = out
            else {
                panic!("{name}: expected audio");
            };
            assert_eq!((sample_rate, channels), (rate, chans), "{name}");
            let seconds = samples.len() as f32 / (rate * chans) as f32;
            assert!((0.95..1.15).contains(&seconds), "{name}: {seconds} s");
            // Every channel carries the tone (planar output is interleaved).
            for c in 0..chans as usize {
                let peak = samples
                    .iter()
                    .skip(c)
                    .step_by(chans as usize)
                    .fold(0.0f32, |a, s| a.max(s.abs()));
                assert!(
                    (peak - expected).abs() < 0.004,
                    "{name} channel {c}: peak {peak}, FFmpeg says {expected}"
                );
            }
        }
    }

    #[test]
    fn refuses_what_is_not_an_audio_file() {
        let error = AudioFileDecoderNode
            .process(RuntimeData::Binary(b"not audio at all".to_vec().into()))
            .unwrap_err();
        assert!(error.to_string().contains("FFmpeg"), "{error}");
        let error = AudioFileDecoderNode
            .process(RuntimeData::Text("x".into()))
            .unwrap_err();
        assert!(error.to_string().contains("binary"), "{error}");
    }
}
