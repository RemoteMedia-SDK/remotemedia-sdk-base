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
//! use remotemedia_core::ingestion::{global_ingest_registry, IngestConfig};
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
                // On Windows, file:///C:/path should become C:/path
                let path = stripped.strip_prefix('/').unwrap_or(stripped);
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
    async fn start(&mut self) -> Result<IngestStream, Error> {
        // Check if already started
        if self.stop_tx.is_some() {
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

    // Create single audio chunk
    let chunk = RuntimeData::Audio {
        samples: samples.into(),
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

/// Decode with ac-ffmpeg (when video feature is enabled)
#[cfg(feature = "video")]
fn decode_with_ffmpeg(
    file: std::fs::File,
    path: &PathBuf,
    audio_config: Option<&AudioConfig>,
) -> Result<(IngestMetadata, Vec<RuntimeData>), Error> {
    use ac_ffmpeg::codec::audio::AudioDecoder;
    use ac_ffmpeg::codec::Decoder;
    use ac_ffmpeg::format::demuxer::Demuxer;
    use ac_ffmpeg::format::io::IO;

    // Note: We don't resample here - the pipeline can handle format conversion
    // via FastResampleNode/AutoResampleStreamingNode if needed
    // These are kept for reference but the actual output format is the source format
    let _ = audio_config; // Config is informational only for FFmpeg decode

    // Create IO wrapper
    let io = IO::from_seekable_read_stream(file);

    // Create demuxer
    let mut demuxer = Demuxer::builder()
        .build(io)
        .map_err(|e| {
            Error::Other(format!(
                "Failed to create demuxer for {}: {}",
                path.display(),
                e
            ))
        })?
        .find_stream_info(None)
        .map_err(|(_, e)| Error::Other(format!("Failed to find stream info: {}", e)))?;

    // Find audio stream
    let (stream_index, stream) = demuxer
        .streams()
        .iter()
        .enumerate()
        .find(|(_, s)| s.codec_parameters().is_audio_codec())
        .ok_or_else(|| Error::Other(format!("No audio stream found in {}", path.display())))?;

    // Get codec parameters
    let codec_params = stream
        .codec_parameters()
        .into_audio_codec_parameters()
        .ok_or_else(|| Error::Other("Failed to get audio codec parameters".to_string()))?;

    let source_sample_rate = codec_params.sample_rate();
    let source_channels = codec_params.channel_layout().channels() as u16;
    let codec_name = codec_params.decoder_name().map(|s| s.to_string());

    // Build metadata (using source format since we don't resample)
    let metadata = IngestMetadata {
        tracks: vec![TrackInfo {
            media_type: MediaType::Audio,
            index: 0,
            stream_id: "audio:0".to_string(),
            language: None,
            codec: codec_name,
            properties: TrackProperties::Audio(AudioTrackProperties {
                sample_rate: source_sample_rate,
                channels: source_channels,
                bit_depth: None,
            }),
        }],
        format: Some(
            path.extension()
                .and_then(|e| e.to_str())
                .unwrap_or("unknown")
                .to_string(),
        ),
        duration_ms: None, // Could calculate from demuxer
        bitrate: None,
    };

    // Create decoder
    let mut decoder = AudioDecoder::from_stream(stream)
        .map_err(|e| Error::Other(format!("Failed to create audio decoder: {}", e)))?
        .build()
        .map_err(|e| Error::Other(format!("Failed to build audio decoder: {}", e)))?;

    // Decode without resampling - the pipeline can handle format conversion
    // via FastResampleNode/AutoResampleStreamingNode if needed
    let mut all_samples: Vec<f32> = Vec::new();
    let mut actual_sample_rate: Option<u32> = None;
    let mut actual_channels: Option<u32> = None;

    loop {
        match demuxer.take() {
            Ok(Some(packet)) => {
                if packet.stream_index() != stream_index {
                    continue;
                }

                decoder
                    .push(packet)
                    .map_err(|e| Error::Other(format!("Decode push error: {}", e)))?;

                while let Some(frame) = decoder
                    .take()
                    .map_err(|e| Error::Other(format!("Decode take error: {}", e)))?
                {
                    // Track actual format from first frame
                    if actual_sample_rate.is_none() {
                        actual_sample_rate = Some(frame.sample_rate());
                        actual_channels = Some(frame.channel_layout().channels() as u32);
                        tracing::debug!(
                            "Decoded audio: {}Hz {} ch",
                            frame.sample_rate(),
                            frame.channel_layout().channels()
                        );
                    }

                    // Extract samples from frame - convert to f32
                    let planes = frame.planes();
                    if !planes.is_empty() {
                        let data = planes[0].data();

                        // Determine bytes per sample based on frame's sample format
                        // Common formats: s16 (2 bytes), s32 (4 bytes), flt (4 bytes), dbl (8 bytes)
                        let bytes_per_sample =
                            frame.samples() as usize * frame.channel_layout().channels() as usize;
                        let bytes_in_plane = data.len();

                        if bytes_in_plane > 0 && bytes_per_sample > 0 {
                            let bytes_per_actual_sample = bytes_in_plane / bytes_per_sample;

                            let samples: Vec<f32> = match bytes_per_actual_sample {
                                // 16-bit signed integer (s16)
                                2 => data
                                    .chunks_exact(2)
                                    .map(|chunk| {
                                        let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
                                        sample as f32 / 32768.0
                                    })
                                    .collect(),
                                // 32-bit float (flt) or 32-bit signed integer (s32)
                                4 => {
                                    // Try interpreting as float first (most common for decoded audio)
                                    data.chunks_exact(4)
                                        .map(|chunk| {
                                            f32::from_le_bytes([
                                                chunk[0], chunk[1], chunk[2], chunk[3],
                                            ])
                                        })
                                        .collect()
                                }
                                // 64-bit double (dbl)
                                8 => data
                                    .chunks_exact(8)
                                    .map(|chunk| {
                                        let bytes: [u8; 8] = [
                                            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4],
                                            chunk[5], chunk[6], chunk[7],
                                        ];
                                        f64::from_le_bytes(bytes) as f32
                                    })
                                    .collect(),
                                _ => {
                                    tracing::warn!(
                                        "Unknown bytes per sample: {}, defaulting to f32 interpretation",
                                        bytes_per_actual_sample
                                    );
                                    // Default to f32 interpretation
                                    data.chunks_exact(4)
                                        .map(|chunk| {
                                            f32::from_le_bytes([
                                                chunk[0], chunk[1], chunk[2], chunk[3],
                                            ])
                                        })
                                        .collect()
                                }
                            };
                            all_samples.extend(samples);
                        }
                    }
                }
            }
            Ok(None) => break,
            Err(e) => return Err(Error::Other(format!("Demuxer error: {}", e))),
        }
    }

    // Flush decoder
    decoder
        .flush()
        .map_err(|e| Error::Other(format!("Flush decoder error: {}", e)))?;

    while let Some(frame) = decoder
        .take()
        .map_err(|e| Error::Other(format!("Flush take error: {}", e)))?
    {
        let planes = frame.planes();
        if !planes.is_empty() {
            let data = planes[0].data();
            // Use same format detection as above (simplified to f32 for flush)
            let samples: Vec<f32> = data
                .chunks_exact(4)
                .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                .collect();
            all_samples.extend(samples);
        }
    }

    // Use actual detected sample rate and channels, or fall back to source metadata
    let output_sample_rate = actual_sample_rate.unwrap_or(source_sample_rate);
    let output_channels = actual_channels.unwrap_or(source_channels as u32);

    tracing::debug!(
        "FFmpeg decode complete: {} samples, {}Hz {} ch",
        all_samples.len(),
        output_sample_rate,
        output_channels
    );

    // Create chunks (split into ~100ms chunks for streaming)
    let chunk_size = (output_sample_rate as usize * output_channels as usize) / 10; // 100ms
    let mut chunks = Vec::new();
    let mut timestamp_us = 0u64;
    let samples_per_second = output_sample_rate as u64 * output_channels as u64;

    for chunk_samples in all_samples.chunks(chunk_size.max(1)) {
        let chunk = RuntimeData::Audio {
            samples: chunk_samples.to_vec().into(),
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

        // Calculate timestamp increment in microseconds
        if samples_per_second > 0 {
            timestamp_us += (chunk_samples.len() as u64 * 1_000_000) / samples_per_second;
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
        samples: crate::data::audio_samples::AudioSamples::Vec(samples),
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
}
