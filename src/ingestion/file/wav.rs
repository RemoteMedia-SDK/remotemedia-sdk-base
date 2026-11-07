//! Bounded RIFF discovery and direct WAV production. File reads and sample
//! conversion are source operations; published payloads need no staging copy.

use super::*;
use remotemedia_types::OutputAllocator;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::Path;

#[derive(Clone, Copy)]
struct WavFormat {
    channels: u16,
    rate: u32,
    bytes: usize,
    float: bool,
    channel_mask: Option<u32>,
}

pub(super) struct WavInput {
    file: File,
    format: WavFormat,
    frames: u64,
    position: u64,
    max_frames: usize,
}

fn invalid(message: impl std::fmt::Display) -> Error {
    Error::Other(format!("Direct WAV ingestion: {message}"))
}

fn le16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn le32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
}

impl WavFormat {
    fn parse(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < 16 {
            return Err(invalid("short fmt chunk"));
        }
        let mut tag = le16(bytes, 0);
        let bits = le16(bytes, 14);
        let mut channel_mask = None;
        if tag == 0xfffe {
            if bytes.len() < 40 || le16(bytes, 16) < 22 {
                return Err(invalid("short extensible fmt chunk"));
            }
            // Standard PCM/IEEE-float subtype GUID suffix in little-endian RIFF.
            if bytes[26..40] != [0, 0, 0, 0, 16, 0, 128, 0, 0, 170, 0, 56, 155, 113] {
                return Err(invalid("unsupported WAV subtype GUID"));
            }
            tag = le16(bytes, 24);
            let valid = le16(bytes, 18);
            if valid == 0 || valid > bits || (tag == 3 && valid != bits) {
                return Err(invalid("invalid valid-bit count"));
            }
            // Integer valid bits are left aligned, so full-container scaling
            // preserves their value without a second conversion buffer.
            channel_mask = Some(le32(bytes, 20));
        }
        let supported = matches!((tag, bits), (1, 8 | 16 | 24 | 32) | (3, 32));
        let channels = le16(bytes, 2);
        let rate = le32(bytes, 4);
        let block = u32::from(channels) * u32::from(bits / 8);
        if !supported
            || channels == 0
            || rate == 0
            || block != u32::from(le16(bytes, 12))
            || block.checked_mul(rate) != Some(le32(bytes, 8))
        {
            return Err(invalid("unsupported or inconsistent WAV format"));
        }
        if channel_mask.is_some_and(|mask| mask != 0 && mask.count_ones() != u32::from(channels)) {
            return Err(invalid("channel mask does not match channel count"));
        }
        Ok(Self {
            channels,
            rate,
            bytes: usize::from(bits / 8),
            float: tag == 3,
            channel_mask,
        })
    }

    fn validate_config(self, config: &IngestConfig) -> Result<(), Error> {
        if config.video.is_some() {
            return Err(invalid("video output is unavailable for WAV"));
        }
        if let Some(audio) = &config.audio {
            if audio.sample_rate != self.rate || audio.channels != self.channels {
                return Err(invalid(
                    "requested resampling/remixing requires a downstream DSP node",
                ));
            }
        }
        if let TrackSelection::Specific(selectors) = &config.track_selection {
            let selected = selectors.iter().any(|s| {
                s.media_type == MediaType::Audio
                    && s.index.unwrap_or(0) == 0
                    && s.language.is_none()
            });
            if !selected {
                return Err(invalid(
                    "track selection does not include the WAV audio track",
                ));
            }
        }
        Ok(())
    }

    fn fill(self, file: &mut File, output: &mut [f32]) -> Result<(), Error> {
        if self.float {
            // All f32 bit patterns are valid. On little-endian hosts this is
            // a direct file read into the final allocation, including NaN bits.
            file.read_exact(bytemuck::cast_slice_mut(output))
                .map_err(invalid)?;
            #[cfg(target_endian = "big")]
            for value in output {
                *value = f32::from_bits(value.to_bits().swap_bytes());
            }
            return Ok(());
        }
        // Bounded source-format scratch; conversion writes each final sample
        // once. No decoded Vec or same-format intermediate payload exists.
        let mut encoded = [0u8; 4096];
        for target in output.chunks_mut(encoded.len() / self.bytes) {
            let source = &mut encoded[..target.len() * self.bytes];
            file.read_exact(source).map_err(invalid)?;
            for (value, bytes) in target.iter_mut().zip(source.chunks_exact(self.bytes)) {
                *value = match self.bytes {
                    1 => (f32::from(bytes[0]) - 128.0) / 128.0,
                    2 => f32::from(i16::from_le_bytes(bytes.try_into().unwrap())) / 32768.0,
                    3 => {
                        (i32::from_le_bytes([0, bytes[0], bytes[1], bytes[2]]) >> 8) as f32
                            / 8388608.0
                    }
                    4 => i32::from_le_bytes(bytes.try_into().unwrap()) as f32 / 2147483648.0,
                    _ => unreachable!("validated PCM width"),
                };
            }
        }
        Ok(())
    }
}

fn riff_end(file: &mut File) -> Result<u64, Error> {
    let mut riff = [0u8; 12];
    file.read_exact(&mut riff).map_err(invalid)?;
    if &riff[..4] != b"RIFF" || &riff[8..] != b"WAVE" {
        return Err(invalid(
            "requires RIFF/WAVE; compressed files and stdin have no direct allocator binding",
        ));
    }
    let end = u64::from(le32(&riff, 4)) + 8;
    if end < 12 || end > file.metadata().map_err(invalid)?.len() {
        return Err(invalid("RIFF length exceeds file bounds"));
    }
    Ok(end)
}

fn read_format(file: &mut File, len: u64) -> Result<WavFormat, Error> {
    if len < 16 {
        return Err(invalid("short fmt chunk"));
    }
    let mut bytes = [0u8; 40];
    let count = len.min(40) as usize;
    file.read_exact(&mut bytes[..count]).map_err(invalid)?;
    if len > 16 && (len < 18 || u64::from(le16(&bytes, 16)) + 18 > len) {
        return Err(invalid("invalid fmt extension length"));
    }
    WavFormat::parse(&bytes[..count])
}

/// Walk chunk headers with seeks, never buffering unknown chunks or audio.
fn discover(file: &mut File) -> Result<(WavFormat, u64), Error> {
    let end = riff_end(file)?;
    let mut format = None;
    while file.stream_position().map_err(invalid)? + 8 <= end {
        let mut header = [0u8; 8];
        file.read_exact(&mut header).map_err(invalid)?;
        let len = u64::from(le32(&header, 4));
        let next = file.stream_position().map_err(invalid)? + len + (len & 1);
        if next > end {
            return Err(invalid("chunk exceeds RIFF bounds"));
        }
        match &header[..4] {
            b"fmt " => {
                if format.is_some() {
                    return Err(invalid("duplicate fmt chunk"));
                }
                format = Some(read_format(file, len)?);
            }
            b"data" => {
                let format = format.ok_or_else(|| invalid("data precedes fmt"))?;
                let frame_bytes = u64::from(format.channels) * format.bytes as u64;
                if len % frame_bytes != 0 {
                    return Err(invalid("partial audio frame"));
                }
                return Ok((format, len / frame_bytes));
            }
            _ => (),
        }
        file.seek(SeekFrom::Start(next)).map_err(invalid)?;
    }
    Err(invalid("missing data chunk"))
}

impl WavInput {
    pub(super) fn open(
        path: &Path,
        config: &IngestConfig,
        max_frames: usize,
    ) -> Result<Self, Error> {
        if path == Path::new("-") || !std::fs::metadata(path).map_err(invalid)?.is_file() {
            return Err(invalid("direct ingestion requires a regular WAV file"));
        }
        let mut file = File::open(path).map_err(invalid)?;
        if !file.metadata().map_err(invalid)?.is_file() {
            return Err(invalid("direct ingestion requires a regular WAV file"));
        }
        let (format, frames) = discover(&mut file)?;
        format.validate_config(config)?;
        max_frames
            .checked_mul(usize::from(format.channels))
            .and_then(|samples| samples.checked_mul(4))
            .filter(|bytes| *bytes <= isize::MAX as usize)
            .ok_or_else(|| invalid("output frame bound exceeds addressable storage"))?;
        Ok(Self {
            file,
            format,
            frames,
            position: 0,
            max_frames,
        })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.position == self.frames
    }

    pub(super) fn metadata(&self) -> IngestMetadata {
        IngestMetadata {
            tracks: vec![TrackInfo {
                media_type: MediaType::Audio,
                index: 0,
                stream_id: "audio:0".into(),
                language: None,
                codec: Some(
                    if self.format.float {
                        "pcm_f32le"
                    } else {
                        "pcm"
                    }
                    .into(),
                ),
                properties: TrackProperties::Audio(AudioTrackProperties {
                    sample_rate: self.format.rate,
                    channels: self.format.channels,
                    bit_depth: Some((self.format.bytes * 8) as u16),
                }),
            }],
            format: Some("wav".into()),
            duration_ms: Some(self.frames * 1000 / u64::from(self.format.rate)),
            bitrate: (self.format.rate
                * u32::from(self.format.channels)
                * self.format.bytes as u32)
                .checked_mul(8),
        }
    }

    pub(super) fn read_packet(
        &mut self,
        allocator: &dyn OutputAllocator,
    ) -> Result<RuntimeData, Error> {
        let frames = (self.frames - self.position).min(self.max_frames as u64) as usize;
        let mut loan = allocator
            .allocate_audio(frames * usize::from(self.format.channels))
            .map_err(invalid)?;
        self.format.fill(&mut self.file, loan.as_mut_slice())?;
        let timestamp_us = self.position * 1_000_000 / u64::from(self.format.rate);
        self.position += frames as u64;
        Ok(RuntimeData::Audio {
            samples: loan.freeze().into(),
            sample_rate: self.format.rate,
            channels: u32::from(self.format.channels),
            stream_id: Some("audio:0".into()),
            timestamp_us: Some(timestamp_us),
            arrival_ts_us: None,
            metadata: self
                .format
                .channel_mask
                .map(|mask| serde_json::json!({"wav_channel_mask":mask})),
        })
    }
}
