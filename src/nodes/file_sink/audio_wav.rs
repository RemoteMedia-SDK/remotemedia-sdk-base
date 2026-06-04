//! `AudioFileWriterNode` — appends incoming `RuntimeData::Audio`
//! samples to a WAV file on disk.
//!
//! Hand-rolled WAV emitter (no external deps): writes a RIFF/WAVE
//! IEEE-float header on first sample, streams every subsequent
//! sample directly into the file, and fixes the placeholder size
//! fields on `Drop`.
//!
//! ## Format
//!
//! 32-bit IEEE float, channel + sample-rate from the **first**
//! audio frame received. Subsequent frames must match — mismatched
//! frames are dropped with a `tracing::warn!`. Mixing rates would
//! require resampling, which is the [`FastResampleNode`]'s job.
//!
//! ## Lifecycle
//!
//! - First Audio frame: writes WAV header (placeholder sizes) and
//!   captures the format.
//! - Each Audio frame: appends raw f32 LE samples to `data` chunk.
//! - On `Drop`: re-opens the file, seeks to header offsets, and
//!   patches the RIFF chunk size + data chunk size. If the process
//!   exits abnormally without dropping (e.g. a panic in another
//!   task), the file is left with the placeholder sizes —
//!   well-formed enough that `ffmpeg` and `mpv` recover by
//!   scanning to EOF.

use crate::data::RuntimeData;
use crate::error::Result;
use crate::nodes::AsyncStreamingNode;
use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

/// Configuration for [`AudioFileWriterNode`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioFileWriterConfig {
    /// Output WAV path. Parent directory is auto-created.
    pub output_path: PathBuf,
}

/// Streaming node that writes incoming Audio frames to a `.wav`
/// file. Pass-through: emits the same Audio frames it received so
/// it can sit on a tap edge without breaking the data flow.
pub struct AudioFileWriterNode {
    config: AudioFileWriterConfig,
    state: Arc<Mutex<WriterState>>,
}

struct WriterState {
    /// `None` until the first audio frame establishes the format.
    /// `Some(file)` afterward; closed on Drop.
    file: Option<File>,
    /// Captured at first-frame time so the header (and subsequent
    /// frame validation) stays consistent.
    sample_rate: u32,
    channels: u16,
    /// Total sample count written across all channels — used to
    /// fixup the data chunk size on Drop.
    samples_written: u64,
    /// Output path; cached for the Drop-time fixup.
    path: PathBuf,
}

impl AudioFileWriterNode {
    /// Build a node with the given output path.
    pub fn new(config: AudioFileWriterConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(WriterState {
                file: None,
                sample_rate: 0,
                channels: 0,
                samples_written: 0,
                path: config.output_path.clone(),
            })),
            config,
        }
    }
}

impl std::fmt::Debug for AudioFileWriterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.state.lock();
        f.debug_struct("AudioFileWriterNode")
            .field("output_path", &self.config.output_path)
            .field("samples_written", &s.samples_written)
            .field("sample_rate", &s.sample_rate)
            .finish()
    }
}

#[async_trait]
impl AsyncStreamingNode for AudioFileWriterNode {
    fn node_type(&self) -> &str {
        "AudioFileWriterNode"
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
        write_one(&self.state, &self.config, &data)?;
        Ok(data)
    }
}

fn write_one(
    state: &Mutex<WriterState>,
    config: &AudioFileWriterConfig,
    data: &RuntimeData,
) -> Result<()> {
    let RuntimeData::Audio {
        samples,
        sample_rate,
        channels,
        ..
    } = data
    else {
        return Ok(()); // pass-through non-audio
    };
    let mut s = state.lock();

    if s.file.is_none() {
        // First Audio frame — open the file + write the header.
        if let Some(parent) = config.output_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let mut f = File::create(&config.output_path).map_err(|e| {
            crate::Error::Execution(format!(
                "AudioFileWriterNode: open {:?}: {e}",
                config.output_path
            ))
        })?;
        write_wav_header(&mut f, *sample_rate, *channels as u16)?;
        s.file = Some(f);
        s.sample_rate = *sample_rate;
        s.channels = *channels as u16;
    }

    if *sample_rate != s.sample_rate || (*channels as u16) != s.channels {
        tracing::warn!(
            "AudioFileWriterNode: dropping frame with mismatched format \
             ({}Hz/{}ch vs initial {}Hz/{}ch)",
            sample_rate,
            channels,
            s.sample_rate,
            s.channels
        );
        return Ok(());
    }

    // Append raw f32 LE samples to the data chunk.
    let f = s.file.as_mut().expect("file open after init");
    let bytes: &[u8] = bytemuck::cast_slice(samples.as_slice());
    f.write_all(bytes)
        .map_err(|e| crate::Error::Execution(format!("AudioFileWriterNode: write: {e}")))?;
    s.samples_written += samples.len() as u64;
    Ok(())
}

impl Drop for AudioFileWriterNode {
    fn drop(&mut self) {
        let state = self.state.lock();
        if state.file.is_none() {
            return;
        }
        // Fix up RIFF + data chunk sizes. Drop the held file
        // handle first (re-open in r/w) so we can seek + write.
        drop(state);
        let mut s = self.state.lock();
        let path = s.path.clone();
        let samples_written = s.samples_written;
        let channels = s.channels as u64;
        let _ = s.file.take(); // close
        drop(s);

        if let Err(e) = patch_wav_sizes(&path, samples_written, channels) {
            tracing::warn!(
                "AudioFileWriterNode: failed to patch sizes in {:?}: {e}",
                path
            );
        }
    }
}

/// WAV header (RIFF + fmt + data) for IEEE 32-bit float samples
/// with placeholder size fields. Sizes are patched on Drop.
fn write_wav_header(f: &mut File, sample_rate: u32, channels: u16) -> Result<()> {
    let bits_per_sample: u16 = 32;
    let byte_rate: u32 = sample_rate * channels as u32 * (bits_per_sample / 8) as u32;
    let block_align: u16 = channels * (bits_per_sample / 8);

    let mut buf: Vec<u8> = Vec::with_capacity(44);
    buf.extend_from_slice(b"RIFF");
    buf.extend_from_slice(&0u32.to_le_bytes()); // RIFF chunk size — fixed up on Drop
    buf.extend_from_slice(b"WAVE");
    buf.extend_from_slice(b"fmt ");
    buf.extend_from_slice(&16u32.to_le_bytes()); // fmt chunk size = 16
    buf.extend_from_slice(&3u16.to_le_bytes()); // PCM-IEEE-float = 3
    buf.extend_from_slice(&channels.to_le_bytes());
    buf.extend_from_slice(&sample_rate.to_le_bytes());
    buf.extend_from_slice(&byte_rate.to_le_bytes());
    buf.extend_from_slice(&block_align.to_le_bytes());
    buf.extend_from_slice(&bits_per_sample.to_le_bytes());
    buf.extend_from_slice(b"data");
    buf.extend_from_slice(&0u32.to_le_bytes()); // data chunk size — fixed up on Drop
    debug_assert_eq!(buf.len(), 44);

    f.write_all(&buf)
        .map_err(|e| crate::Error::Execution(format!("WAV header write: {e}")))?;
    Ok(())
}

/// Re-open the file and patch the RIFF chunk size + data chunk
/// size now that we know the final sample count. Robust to either
/// `samples_written = 0` (header-only file, sizes stay 0) or the
/// nominal case.
fn patch_wav_sizes(path: &std::path::Path, total_samples: u64, _channels: u64) -> Result<()> {
    let data_bytes: u64 = total_samples * 4; // f32 = 4 bytes/sample
                                             // RIFF chunk size = (file size - 8). After header + data:
                                             // file size = 44 + data_bytes; RIFF size = 36 + data_bytes.
    let riff_size: u32 = (data_bytes + 36).try_into().unwrap_or(u32::MAX);
    let data_size: u32 = data_bytes.try_into().unwrap_or(u32::MAX);

    let mut f = OpenOptions::new()
        .write(true)
        .read(true)
        .open(path)
        .map_err(|e| crate::Error::Execution(format!("WAV reopen: {e}")))?;

    // RIFF size is at offset 4.
    f.seek(SeekFrom::Start(4))
        .map_err(|e| crate::Error::Execution(format!("WAV seek: {e}")))?;
    f.write_all(&riff_size.to_le_bytes())
        .map_err(|e| crate::Error::Execution(format!("WAV write riff size: {e}")))?;

    // data size is at offset 40 (12+8+16+4 = 40).
    f.seek(SeekFrom::Start(40))
        .map_err(|e| crate::Error::Execution(format!("WAV seek2: {e}")))?;
    f.write_all(&data_size.to_le_bytes())
        .map_err(|e| crate::Error::Execution(format!("WAV write data size: {e}")))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::audio_samples::AudioSamples;

    fn audio(samples: Vec<f32>, sample_rate: u32) -> RuntimeData {
        RuntimeData::Audio {
            samples: AudioSamples::Vec(samples),
            sample_rate,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        }
    }

    #[tokio::test]
    async fn writes_a_valid_wav_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.wav");
        let node = AudioFileWriterNode::new(AudioFileWriterConfig {
            output_path: path.clone(),
        });
        // Send a few audio frames.
        node.process(audio(vec![0.1, 0.2, 0.3], 16_000))
            .await
            .unwrap();
        node.process(audio(vec![0.4, 0.5], 16_000)).await.unwrap();
        drop(node); // triggers size fixup

        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.len() >= 44 + 5 * 4, "WAV file too small");
        assert_eq!(&bytes[0..4], b"RIFF");
        assert_eq!(&bytes[8..12], b"WAVE");
        assert_eq!(&bytes[12..16], b"fmt ");
        // PCM-IEEE-float code = 3.
        assert_eq!(u16::from_le_bytes([bytes[20], bytes[21]]), 3);
        // 1 channel.
        assert_eq!(u16::from_le_bytes([bytes[22], bytes[23]]), 1);
        // 16 kHz sample rate.
        assert_eq!(
            u32::from_le_bytes([bytes[24], bytes[25], bytes[26], bytes[27]]),
            16_000
        );
        assert_eq!(&bytes[36..40], b"data");
        let data_size = u32::from_le_bytes([bytes[40], bytes[41], bytes[42], bytes[43]]);
        assert_eq!(
            data_size,
            5 * 4,
            "data chunk size should be 5 samples × 4 bytes"
        );
    }

    #[tokio::test]
    async fn drops_mismatched_format_frames() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("b.wav");
        let node = AudioFileWriterNode::new(AudioFileWriterConfig {
            output_path: path.clone(),
        });
        node.process(audio(vec![0.1; 4], 16_000)).await.unwrap();
        // Different sample rate — dropped with a warn.
        node.process(audio(vec![0.2; 4], 48_000)).await.unwrap();
        node.process(audio(vec![0.3; 4], 16_000)).await.unwrap();
        drop(node);

        let bytes = std::fs::read(&path).unwrap();
        let data_size = u32::from_le_bytes([bytes[40], bytes[41], bytes[42], bytes[43]]);
        // Only the matching frames (4 + 4 = 8 samples × 4 bytes) hit disk.
        assert_eq!(data_size, 8 * 4);
    }

    #[tokio::test]
    async fn non_audio_input_is_passthrough_no_op() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("never.wav");
        let node = AudioFileWriterNode::new(AudioFileWriterConfig {
            output_path: path.clone(),
        });
        node.process(RuntimeData::Text("hi".into())).await.unwrap();
        drop(node);
        // No audio frames → file was never created.
        assert!(
            !path.exists(),
            "file should not be created without audio input"
        );
    }
}
