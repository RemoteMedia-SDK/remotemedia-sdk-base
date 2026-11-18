//! Built-in file ingestion plugin
//!
//! This module provides the default `FileIngestPlugin` for ingesting media
//! from local files and stdin. It uses `ac-ffmpeg` for demuxing and decoding.
//!
//! # Supported Schemes
//!
//! - `file://` - Explicit file URI
//! - Bare paths (`./local.wav`, `/path/to/file.mp4`)
//! - `-` - Standard input (stdin)
//!
//! # Usage
//!
//! The FileIngestPlugin is automatically registered with the global registry:
//!
//! ```ignore
//! use remotemedia_sdk_base::ingestion::{global_ingest_registry, IngestConfig};
//!
//! let config = IngestConfig::from_url("./test.wav");
//! let source = global_ingest_registry().create_from_uri(&config)?;
//! ```

use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot, RwLock};

use crate::data::RuntimeData;
use crate::Error;

use super::config::{AudioConfig, TrackSelection, VideoConfig};
use super::status::{IngestStatus, MediaType};
use super::stream::{
    AudioTrackProperties, IngestMetadata, IngestStream, TrackInfo, TrackProperties,
};
use super::{IngestConfig, IngestPlugin, IngestSource};

mod direct;
mod wav;

/// File ingest plugin
///
/// Handles local file and stdin ingestion with `file://`, bare paths, and `-`.
pub struct FileIngestPlugin;

impl IngestPlugin for FileIngestPlugin {
    fn name(&self) -> &'static str {
        "file"
    }

    fn schemes(&self) -> &'static [&'static str] {
        // Empty string matches bare paths (./file, /path/to/file)
        // "-" matches stdin
        &["file", "", "-"]
    }

    fn create(&self, config: &IngestConfig) -> Result<Box<dyn IngestSource>, Error> {
        Ok(Box::new(FileIngestSource::new(config.clone())?))
    }

    fn validate(&self, config: &IngestConfig) -> Result<(), Error> {
        let path = url_to_path(&config.url)?;

        // Special case: stdin is always valid
        if path.to_string_lossy() == "-" {
            return Ok(());
        }

        // Check file exists
        if !path.exists() {
            return Err(Error::IngestFileNotFound {
                path: path.to_string_lossy().to_string(),
            });
        }

        // Check it's a file (not a directory)
        if !path.is_file() {
            return Err(Error::ConfigError(format!(
                "Path is not a file: {}",
                path.display()
            )));
        }

        Ok(())
    }
}

/// Convert URL string to local path
///
/// Handles:
/// - `file:///path/to/file` → `/path/to/file`
/// - `file://path/to/file` → `path/to/file`
/// - `./relative/path` → `./relative/path`
/// - `/absolute/path` → `/absolute/path`
/// - `-` → `-` (stdin)
pub fn url_to_path(url: &str) -> Result<PathBuf, Error> {
    // Handle stdin
    if url == "-" {
        return Ok(PathBuf::from("-"));
    }

    // Handle file:// URLs
    if let Some(stripped) = url.strip_prefix("file://") {
        // file:///path → /path (Unix)
        // file:///C:/path → C:/path (Windows)
        if stripped.starts_with('/') {
            // Unix absolute path or Windows path with drive letter
            #[cfg(windows)]
            {
                // On Windows, file:///C:/path should become C:/path; a
                // drive-less file:///path stays rooted (\path), not relative.
                let rest = &stripped[1..];
                let b = rest.as_bytes();
                let has_drive = b.len() >= 2 && b[0].is_ascii_alphabetic() && b[1] == b':';
                let path = if has_drive { rest } else { stripped };
                return Ok(PathBuf::from(path));
            }
            #[cfg(not(windows))]
            {
                return Ok(PathBuf::from(stripped));
            }
        } else {
            // file://relative/path (unusual but valid)
            return Ok(PathBuf::from(stripped));
        }
    }

    // Bare path (relative or absolute)
    Ok(PathBuf::from(url))
}

/// File ingest source
///
/// Active ingestion from a local file or stdin.
pub struct FileIngestSource {
    /// Configuration
    config: IngestConfig,

    /// Resolved file path
    path: PathBuf,

    /// Current status
    status: Arc<RwLock<IngestStatus>>,

    /// Discovered metadata
    metadata: Arc<RwLock<Option<IngestMetadata>>>,

    /// Stop signal sender
    stop_tx: Option<oneshot::Sender<()>>,

    /// Stopped flag
    stopped: Arc<AtomicBool>,

    /// Joined worker for allocator-backed file production.
    direct_task: Option<tokio::task::JoinHandle<Result<(), Error>>>,
}

impl FileIngestSource {
    /// Create a new file ingest source
    pub fn new(config: IngestConfig) -> Result<Self, Error> {
        let path = url_to_path(&config.url)?;

        Ok(Self {
            config,
            path,
            status: Arc::new(RwLock::new(IngestStatus::Idle)),
            metadata: Arc::new(RwLock::new(None)),
            stop_tx: None,
            stopped: Arc::new(AtomicBool::new(false)),
            direct_task: None,
        })
    }

    /// Get the file path
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Internal: Set status
    async fn set_status(&self, status: IngestStatus) {
        let mut guard = self.status.write().await;
        *guard = status;
    }
}

#[async_trait]
impl IngestSource for FileIngestSource {
    async fn start_with_output_allocator(
        &mut self,
        allocator: Arc<dyn remotemedia_types::OutputAllocator>,
        max_frames: usize,
    ) -> Result<IngestStream, Error> {
        self.start_direct(allocator, max_frames).await
    }

    async fn start(&mut self) -> Result<IngestStream, Error> {
        // Check if already started
        if self.stop_tx.is_some() || self.direct_task.is_some() {
            return Err(Error::ConfigError(
                "Source already started. Call stop() first.".to_string(),
            ));
        }

        // Reset stopped flag
        self.stopped.store(false, Ordering::SeqCst);

        // Create stop signal channel
        let (stop_tx, stop_rx) = oneshot::channel();
        self.stop_tx = Some(stop_tx);

        // Create data channel
        let (data_tx, data_rx) = mpsc::channel::<RuntimeData>(32);

        // Update status to Connecting
        self.set_status(IngestStatus::Connecting).await;

        // Clone values for the decode task
        let path = self.path.clone();
        let audio_config = self.config.audio.clone();
        let video_config = self.config.video.clone();
        let track_selection = self.config.track_selection.clone();
        let status = Arc::clone(&self.status);
        let metadata_store = Arc::clone(&self.metadata);
        let stopped = Arc::clone(&self.stopped);

        // Spawn the decode task
        tokio::spawn(async move {
            tracing::debug!("Starting file decode task");
            let result = decode_file(
                path,
                audio_config,
                video_config,
                track_selection,
                data_tx,
                stop_rx,
                status.clone(),
                metadata_store.clone(),
                stopped,
            )
            .await;

            // Update final status
            let final_status = match &result {
                Ok(()) => {
                    tracing::debug!("File decode completed successfully");
                    IngestStatus::Disconnected
                }
                Err(e) => {
                    tracing::error!("File decode failed: {}", e);
                    IngestStatus::error(e.to_string())
                }
            };
            let mut guard = status.write().await;
            *guard = final_status;
        });

        // Wait briefly for connection
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Return stream with placeholder metadata
        let metadata = {
            let guard = self.metadata.read().await;
            guard.clone().unwrap_or_default()
        };

        Ok(IngestStream::new(data_rx, metadata))
    }

    async fn stop(&mut self) -> Result<(), Error> {
        // Set stopped flag
        self.stopped.store(true, Ordering::SeqCst);

        // Send stop signal
        if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }

        // Keep the handle in self across await: cancelling stop() must not
        // detach the worker or allow a second producer to start over it.
        if let Some(task) = self.direct_task.as_mut() {
            let result = task.await;
            self.direct_task = None;
            result.map_err(|e| Error::Other(format!("File producer task failed: {e}")))??;
        }

        // Update status
        self.set_status(IngestStatus::Disconnected).await;

        Ok(())
    }

    fn status(&self) -> IngestStatus {
        self.status
            .try_read()
            .map(|guard| guard.clone())
            .unwrap_or(IngestStatus::Idle)
    }

    fn metadata(&self) -> Option<&IngestMetadata> {
        None // Use IngestStream::metadata() instead
    }
}

/// Decode file and send chunks to channel
#[allow(clippy::too_many_arguments)]
async fn decode_file(
    path: PathBuf,
    audio_config: Option<AudioConfig>,
    video_config: Option<VideoConfig>,
    track_selection: TrackSelection,
    data_tx: mpsc::Sender<RuntimeData>,
    mut stop_rx: oneshot::Receiver<()>,
    status: Arc<RwLock<IngestStatus>>,
    metadata_store: Arc<RwLock<Option<IngestMetadata>>>,
    stopped: Arc<AtomicBool>,
) -> Result<(), Error> {
    tracing::debug!("decode_file: starting for path {:?}", path);

    // Run the blocking decode in spawn_blocking
    let path_clone = path.clone();
    let audio_config_clone = audio_config.clone();
    let video_config_clone = video_config.clone();
    let track_selection_clone = track_selection.clone();

    tracing::debug!("decode_file: spawning blocking decode task");

    // Spawn blocking decoder
    let decode_handle = tokio::task::spawn_blocking(move || {
        tracing::debug!("decode_file_sync: starting synchronous decode");
        let result = decode_file_sync(
            &path_clone,
            audio_config_clone.as_ref(),
            video_config_clone.as_ref(),
            &track_selection_clone,
        );
        tracing::debug!(
            "decode_file_sync: completed with result: {:?}",
            result.as_ref().map(|(m, c)| (m, c.len()))
        );
        result
    });

    // Wait for decode to complete or stop signal
    tokio::select! {
        biased;

        _ = &mut stop_rx => {
            tracing::info!("File decode stopped by request");
            return Ok(());
        }

        result = decode_handle => {
            match result {
                Ok(Ok((metadata, chunks))) => {
                    // Store metadata
                    {
                        let mut guard = metadata_store.write().await;
                        *guard = Some(metadata);
                    }

                    // Update status to connected
                    {
                        let mut guard = status.write().await;
                        *guard = IngestStatus::Connected;
                    }

                    // Send all chunks
                    for chunk in chunks {
                        if stopped.load(Ordering::SeqCst) {
                            break;
                        }

                        if data_tx.send(chunk).await.is_err() {
                            tracing::warn!("Data channel closed");
                            break;
                        }

                        // Small yield to allow stop signal to be processed
                        tokio::task::yield_now().await;
                    }

                    Ok(())
                }
                Ok(Err(e)) => Err(e),
                Err(e) => Err(Error::Execution(format!("Decode task panicked: {}", e))),
            }
        }
    }
}

/// Synchronous file decode implementation
fn decode_file_sync(
    path: &PathBuf,
    audio_config: Option<&AudioConfig>,
    video_config: Option<&VideoConfig>,
    track_selection: &TrackSelection,
) -> Result<(IngestMetadata, Vec<RuntimeData>), Error> {
    use std::fs::File;

    let _ = (video_config, track_selection); // TODO: Use these

    // Handle stdin
    if path.to_string_lossy() == "-" {
        return decode_stdin(audio_config);
    }

    // Open file
    let file = File::open(path).map_err(|e| Error::IngestFileNotFound {
        path: format!("{}: {}", path.display(), e),
    })?;

    // Try to decode with ac-ffmpeg if available
    #[cfg(feature = "video")]
    {
        return decode_with_ffmpeg(file, path, audio_config);
    }

    #[cfg(not(feature = "video"))]
    {
        // Fallback: Try WAV format
        decode_wav_file(file, path, audio_config)
    }
}

/// Decode stdin as raw PCM
fn decode_stdin(
    audio_config: Option<&AudioConfig>,
) -> Result<(IngestMetadata, Vec<RuntimeData>), Error> {
    let sample_rate = audio_config.map(|c| c.sample_rate).unwrap_or(16000);
    let channels = audio_config.map(|c| c.channels).unwrap_or(1);

    // Read all stdin data
    let mut buffer = Vec::new();
    std::io::stdin()
        .read_to_end(&mut buffer)
        .map_err(|e| Error::Io(e))?;

    // Interpret as f32 samples
    let samples: Vec<f32> = buffer
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect();

    let metadata = IngestMetadata {
        tracks: vec![TrackInfo {
            media_type: MediaType::Audio,
            index: 0,
            stream_id: "audio:0".to_string(),
            language: None,
            codec: Some("pcm_f32le".to_string()),
            properties: TrackProperties::Audio(AudioTrackProperties {
                sample_rate,
                channels,
                bit_depth: Some(32),
            }),
        }],
        format: Some("raw".to_string()),
        duration_ms: Some((samples.len() as u64 * 1000) / (sample_rate as u64 * channels as u64)),
        bitrate: None,
    };

    // Create single audio chunk, moved into shared storage (no copy).
    let chunk = RuntimeData::Audio {
        samples: crate::data::audio_samples::AudioSamples::Shared(samples.into()),
        sample_rate,
        channels: channels as u32,
        stream_id: Some("audio:0".to_string()),
        timestamp_us: Some(0),
        arrival_ts_us: Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0),
        ),
        metadata: None,
    };

    Ok((metadata, vec![chunk]))
}

/// Audio decoded by FFmpeg: interleaved f32 samples at the source rate and
/// channel count.
#[cfg(feature = "video")]
#[derive(Debug, Clone)]
pub struct DecodedAudio {
    pub samples: Vec<f32>,
    pub sample_rate: u32,
    pub channels: u32,
    /// Name of the decoder FFmpeg chose (`mp3float`, `opus`, ...).
    pub codec: Option<String>,
}

/// Decode the first audio stream of a container from any seekable reader
/// (a file, or bytes in memory) with the runtime's FFmpeg. Every frame is
/// converted to interleaved f32 at the source rate and
/// channel layout, so planar decoder output (MP3, AAC, Opus, Vorbis) keeps
/// every channel. `label` names the source in errors.
#[cfg(feature = "video")]
pub fn decode_audio_with_ffmpeg<R>(source: R, label: &str) -> Result<DecodedAudio, Error>
where
    R: std::io::Read + std::io::Seek,
{
    use ac_ffmpeg::codec::audio::{AudioDecoder, AudioFrame};
    use ac_ffmpeg::codec::Decoder;
    use ac_ffmpeg::format::demuxer::Demuxer;
    use ac_ffmpeg::format::io::IO;

    let io = IO::from_seekable_read_stream(source);
    let mut demuxer = Demuxer::builder()
        .build(io)
        .map_err(|e| Error::Other(format!("FFmpeg cannot open {label}: {e}")))?
        .find_stream_info(None)
        .map_err(|(_, e)| Error::Other(format!("FFmpeg cannot read the streams of {label}: {e}")))?;

    let (stream_index, stream) = demuxer
        .streams()
        .iter()
        .enumerate()
        .find(|(_, s)| s.codec_parameters().is_audio_codec())
        .ok_or_else(|| Error::Other(format!("No audio stream found in {label}")))?;
    let codec_params = stream
        .codec_parameters()
        .into_audio_codec_parameters()
        .ok_or_else(|| Error::Other(format!("{label}: no audio codec parameters")))?;
    let codec = codec_params.decoder_name().map(|s| s.to_string());
    let mut decoder = AudioDecoder::from_stream(stream)
        .map_err(|e| Error::Other(format!("{label}: no decoder for the audio stream: {e}")))?
        .build()
        .map_err(|e| Error::Other(format!("{label}: the audio decoder failed to start: {e}")))?;

    let mut samples: Vec<f32> = Vec::new();
    let mut sample_rate = codec_params.sample_rate();
    let mut channels = codec_params.channel_layout().channels();
    let mut first = true;

    let mut convert = |frame: AudioFrame, samples: &mut Vec<f32>| -> Result<(), Error> {
        if first {
            first = false;
            sample_rate = frame.sample_rate();
            channels = frame.channel_layout().channels();
        }
        append_interleaved_f32(&frame, samples)
            .map_err(|format| Error::Other(format!("{label}: unsupported sample format {format}")))
    };

    loop {
        match demuxer.take() {
            Ok(Some(packet)) => {
                if packet.stream_index() != stream_index {
                    continue;
                }
                decoder
                    .push(packet)
                    .map_err(|e| Error::Other(format!("{label}: decode error: {e}")))?;
                while let Some(frame) = decoder
                    .take()
                    .map_err(|e| Error::Other(format!("{label}: decode error: {e}")))?
                {
                    convert(frame, &mut samples)?;
                }
            }
            Ok(None) => break,
            Err(e) => return Err(Error::Other(format!("{label}: demuxer error: {e}"))),
        }
    }
    decoder
        .flush()
        .map_err(|e| Error::Other(format!("{label}: decoder flush error: {e}")))?;
    while let Some(frame) = decoder
        .take()
        .map_err(|e| Error::Other(format!("{label}: decoder flush error: {e}")))?
    {
        convert(frame, &mut samples)?;
    }

    tracing::debug!(
        "FFmpeg decode of {label}: {} samples, {}Hz {} ch",
        samples.len(),
        sample_rate,
        channels
    );
    Ok(DecodedAudio {
        samples,
        sample_rate,
        channels,
        codec,
    })
}

/// Append one decoded frame as interleaved f32: packed or planar `u8`,
/// `s16`, `s32`, `flt` or `dbl` (FFmpeg's decoder output formats). Planar
/// frames hold one plane per channel and are interleaved here. Returns the
/// format name when it is none of these.
#[cfg(feature = "video")]
fn append_interleaved_f32(
    frame: &ac_ffmpeg::codec::audio::AudioFrame,
    out: &mut Vec<f32>,
) -> Result<(), &'static str> {
    let format = frame.sample_format();
    let name = format.name();
    let (bytes_per_sample, read): (usize, fn(&[u8]) -> f32) = match name.trim_end_matches('p') {
        "u8" => (1, |b| (b[0] as f32 - 128.0) / 128.0),
        "s16" => (2, |b| i16::from_le_bytes([b[0], b[1]]) as f32 / 32768.0),
        "s32" => (4, |b| i32::from_le_bytes([b[0], b[1], b[2], b[3]]) as f32 / 2147483648.0),
        "flt" => (4, |b| f32::from_le_bytes([b[0], b[1], b[2], b[3]])),
        "dbl" => (8, |b| {
            f64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]) as f32
        }),
        _ => return Err(name),
    };
    let count = frame.samples();
    let channels = frame.channel_layout().channels() as usize;
    let planes = frame.planes();
    if format.is_planar() {
        out.reserve(count * channels);
        for i in 0..count {
            for c in 0..channels.min(planes.len()) {
                let data = planes[c].data();
                let at = i * bytes_per_sample;
                if at + bytes_per_sample <= data.len() {
                    out.push(read(&data[at..at + bytes_per_sample]));
                }
            }
        }
    } else if let Some(plane) = planes.first() {
        // The plane may carry padding past the samples.
        let len = (count * channels * bytes_per_sample).min(plane.data().len());
        out.extend(plane.data()[..len].chunks_exact(bytes_per_sample).map(read));
    }
    Ok(())
}

/// Decode with ac-ffmpeg (when video feature is enabled)
#[cfg(feature = "video")]
fn decode_with_ffmpeg(
    file: std::fs::File,
    path: &PathBuf,
    audio_config: Option<&AudioConfig>,
) -> Result<(IngestMetadata, Vec<RuntimeData>), Error> {
    // We don't resample here - the pipeline can handle format conversion via
    // FastResampleNode/AutoResampleStreamingNode if needed.
    let _ = audio_config; // Config is informational only for FFmpeg decode

    let decoded = decode_audio_with_ffmpeg(file, &path.display().to_string())?;
    let output_sample_rate = decoded.sample_rate;
    let output_channels = decoded.channels;

    let metadata = IngestMetadata {
        tracks: vec![TrackInfo {
            media_type: MediaType::Audio,
            index: 0,
            stream_id: "audio:0".to_string(),
            language: None,
            codec: decoded.codec.clone(),
            properties: TrackProperties::Audio(AudioTrackProperties {
                sample_rate: output_sample_rate,
                channels: output_channels as u16,
                bit_depth: None,
            }),
        }],
        format: Some(
            path.extension()
                .and_then(|e| e.to_str())
                .unwrap_or("unknown")
                .to_string(),
        ),
        duration_ms: None,
        bitrate: None,
    };

    // Create chunks (split into ~100ms chunks for streaming)
    let chunk_size = (output_sample_rate as usize * output_channels as usize) / 10; // 100ms
    let mut chunks = Vec::new();
    let mut timestamp_us = 0u64;
    let samples_per_second = output_sample_rate as u64 * output_channels as u64;

    // Promote the decoded buffer once; every chunk is a shared view of it.
    let all_samples = remotemedia_types::SharedBuffer::from(decoded.samples);
    let step = chunk_size.max(1);
    for start in (0..all_samples.len()).step_by(step) {
        let chunk_samples = all_samples
            .slice(start..(start + step).min(all_samples.len()))
            .expect("chunk range lies within the decoded buffer");
        let chunk_len = chunk_samples.len();
        let chunk = RuntimeData::Audio {
            samples: crate::data::audio_samples::AudioSamples::Shared(chunk_samples),
            sample_rate: output_sample_rate,
            channels: output_channels,
            stream_id: Some("audio:0".to_string()),
            timestamp_us: Some(timestamp_us),
            arrival_ts_us: Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros() as u64)
                    .unwrap_or(0),
            ),
            metadata: None,
        };
        chunks.push(chunk);
        if samples_per_second > 0 {
            timestamp_us += (chunk_len as u64 * 1_000_000) / samples_per_second;
        }
    }

    Ok((metadata, chunks))
}

/// Fallback WAV decoder (when video feature is disabled)
#[cfg(not(feature = "video"))]
fn decode_wav_file(
    mut file: std::fs::File,
    path: &PathBuf,
    audio_config: Option<&AudioConfig>,
) -> Result<(IngestMetadata, Vec<RuntimeData>), Error> {
    // Simple WAV header parsing
    let mut header = [0u8; 44];
    file.read_exact(&mut header)
        .map_err(|e| Error::Other(format!("Failed to read WAV header: {}", e)))?;

    // Validate RIFF/WAVE
    if &header[0..4] != b"RIFF" || &header[8..12] != b"WAVE" {
        return Err(Error::Other(format!(
            "Not a valid WAV file: {}",
            path.display()
        )));
    }

    // Parse format chunk
    let channels = u16::from_le_bytes([header[22], header[23]]);
    let sample_rate = u32::from_le_bytes([header[24], header[25], header[26], header[27]]);
    let bits_per_sample = u16::from_le_bytes([header[34], header[35]]);

    // Read all sample data
    let mut data = Vec::new();
    file.read_to_end(&mut data)
        .map_err(|e| Error::Other(format!("Failed to read WAV data: {}", e)))?;

    // Convert to f32
    let samples: Vec<f32> = match bits_per_sample {
        16 => data
            .chunks_exact(2)
            .map(|chunk| {
                let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
                sample as f32 / 32768.0
            })
            .collect(),
        32 => data
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect(),
        _ => {
            return Err(Error::Other(format!(
                "Unsupported WAV bit depth: {}",
                bits_per_sample
            )));
        }
    };

    let target_sample_rate = audio_config.map(|c| c.sample_rate).unwrap_or(sample_rate);
    let target_channels = audio_config
        .map(|c| c.channels as u32)
        .unwrap_or(channels as u32);

    let metadata = IngestMetadata {
        tracks: vec![TrackInfo {
            media_type: MediaType::Audio,
            index: 0,
            stream_id: "audio:0".to_string(),
            language: None,
            codec: Some("pcm".to_string()),
            properties: TrackProperties::Audio(AudioTrackProperties {
                sample_rate: target_sample_rate,
                channels: target_channels as u16,
                bit_depth: Some(bits_per_sample),
            }),
        }],
        format: Some("wav".to_string()),
        duration_ms: Some((samples.len() as u64 * 1000) / (sample_rate as u64 * channels as u64)),
        bitrate: None,
    };

    // Note: No resampling in fallback mode
    let chunk = RuntimeData::Audio {
        // Move the decoded Vec into shared storage (no copy) so later clones
        // share it instead of deep-copying.
        samples: crate::data::audio_samples::AudioSamples::Shared(samples.into()),
        sample_rate: target_sample_rate,
        channels: target_channels,
        stream_id: Some("audio:0".to_string()),
        timestamp_us: Some(0),
        arrival_ts_us: Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0),
        ),
        metadata: None,
    };

    Ok((metadata, vec![chunk]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_plugin_validates_existing_file() {
        let plugin = FileIngestPlugin;

        // Test with stdin which is always valid
        let config = IngestConfig::from_url("-");
        assert!(plugin.validate(&config).is_ok());

        // Test with the Cargo.toml file which should always exist in the workspace
        let config = IngestConfig::from_url("Cargo.toml");
        // This may or may not work depending on working directory,
        // so we just check it doesn't panic
        let _ = plugin.validate(&config);
    }

    #[test]
    fn test_file_plugin_rejects_nonexistent_file() {
        let plugin = FileIngestPlugin;

        let config = IngestConfig::from_url("/nonexistent/path/to/file.wav");
        let result = plugin.validate(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_url_to_path_handles_file_protocol() {
        assert_eq!(
            url_to_path("file:///path/to/file.wav").unwrap(),
            PathBuf::from("/path/to/file.wav")
        );

        assert_eq!(
            url_to_path("file://relative/path.wav").unwrap(),
            PathBuf::from("relative/path.wav")
        );
    }

    #[cfg(windows)]
    #[test]
    fn test_url_to_path_handles_windows_drive_urls() {
        assert_eq!(
            url_to_path("file:///C:/media/file.wav").unwrap(),
            PathBuf::from("C:/media/file.wav")
        );
    }

    #[test]
    fn test_url_to_path_handles_bare_paths() {
        assert_eq!(
            url_to_path("./local.wav").unwrap(),
            PathBuf::from("./local.wav")
        );

        assert_eq!(
            url_to_path("/absolute/path.mp4").unwrap(),
            PathBuf::from("/absolute/path.mp4")
        );
    }

    #[test]
    fn test_url_to_path_handles_stdin() {
        assert_eq!(url_to_path("-").unwrap(), PathBuf::from("-"));
    }

    #[test]
    fn test_file_plugin_schemes() {
        let plugin = FileIngestPlugin;
        let schemes = plugin.schemes();
        assert!(schemes.contains(&"file"));
        assert!(schemes.contains(&"")); // bare paths
        assert!(schemes.contains(&"-")); // stdin
    }

    /// The legacy `start()` path decodes a whole file, then chunks it. Chunks
    /// must be views of one shared allocation, not per-chunk copies.
    #[cfg(feature = "video")]
    #[test]
    fn ffmpeg_decode_chunks_share_one_allocation() {
        use crate::data::audio_samples::AudioSamples;
        let samples: Vec<i16> = (0..4000)
            .map(|i| ((i % 200) * 100 - 10_000) as i16)
            .collect();
        let mut wav = Vec::new();
        let data_len = (samples.len() * 2) as u32;
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&(36 + data_len).to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16u32.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes()); // PCM
        wav.extend_from_slice(&1u16.to_le_bytes()); // mono
        wav.extend_from_slice(&16_000u32.to_le_bytes());
        wav.extend_from_slice(&32_000u32.to_le_bytes());
        wav.extend_from_slice(&2u16.to_le_bytes());
        wav.extend_from_slice(&16u16.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&data_len.to_le_bytes());
        for s in &samples {
            wav.extend_from_slice(&s.to_le_bytes());
        }
        let path = std::env::temp_dir().join(format!(
            "remotemedia-ingest-chunks-{}.wav",
            std::process::id()
        ));
        std::fs::write(&path, &wav).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let (_, chunks) = decode_with_ffmpeg(file, &path, None).unwrap();
        let _ = std::fs::remove_file(&path);

        assert!(chunks.len() >= 2, "expected several 100 ms chunks");
        let views: Vec<_> = chunks
            .iter()
            .map(|chunk| match chunk {
                RuntimeData::Audio {
                    samples: AudioSamples::Shared(view),
                    ..
                } => view.clone(),
                other => panic!("expected shared audio, got {other:?}"),
            })
            .collect();
        for view in &views[1..] {
            assert!(
                view.shares_storage(&views[0]),
                "chunks must share one allocation"
            );
        }
        let total: usize = views.iter().map(|v| v.len()).sum();
        assert_eq!(total, samples.len());
        let first = samples[0] as f32 / 32768.0;
        assert!((views[0][0] - first).abs() < 1e-4);
    }
}
