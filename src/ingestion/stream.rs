//! Stream types for ingestion
//!
//! This module defines the async stream wrapper and metadata types
//! for receiving decoded media data from ingest sources.

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::data::RuntimeData;

use super::status::MediaType;

/// Async stream of RuntimeData from an ingest source
///
/// Wraps an mpsc receiver for receiving decoded media chunks.
/// The stream is created by `IngestSource::start()`.
///
/// # Example
///
/// ```ignore
/// let mut stream = source.start().await?;
///
/// while let Some(data) = stream.recv().await {
///     // Process RuntimeData (audio, video, etc.)
///     println!("Received: {:?}", data.data_type());
/// }
/// ```
pub struct IngestStream {
    /// Channel receiver for RuntimeData chunks
    receiver: mpsc::Receiver<RuntimeData>,

    /// Discovered stream metadata
    metadata: IngestMetadata,
}

impl IngestStream {
    /// Create a new ingest stream with the given receiver and metadata
    pub fn new(receiver: mpsc::Receiver<RuntimeData>, metadata: IngestMetadata) -> Self {
        Self { receiver, metadata }
    }

    /// Create a stream with default metadata
    ///
    /// Used during initial connection before metadata is discovered.
    pub fn with_receiver(receiver: mpsc::Receiver<RuntimeData>) -> Self {
        Self {
            receiver,
            metadata: IngestMetadata::default(),
        }
    }

    /// Receive the next data chunk
    ///
    /// Returns `None` when the stream is closed (EOF or disconnected).
    pub async fn recv(&mut self) -> Option<RuntimeData> {
        self.receiver.recv().await
    }

    /// Try to receive without blocking
    ///
    /// Returns `Ok(data)` if data is available, `Err(TryRecvError)` otherwise.
    pub fn try_recv(&mut self) -> Result<RuntimeData, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }

    /// Get the discovered stream metadata
    pub fn metadata(&self) -> &IngestMetadata {
        &self.metadata
    }

    /// Update metadata (called after stream discovery)
    pub fn set_metadata(&mut self, metadata: IngestMetadata) {
        self.metadata = metadata;
    }

    /// Check if the stream is closed
    pub fn is_closed(&self) -> bool {
        self.receiver.is_closed()
    }
}

impl std::fmt::Debug for IngestStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestStream")
            .field("metadata", &self.metadata)
            .field("is_closed", &self.is_closed())
            .finish()
    }
}

/// Metadata discovered from an ingest source
///
/// Contains information about the tracks, format, and duration
/// discovered after connecting to the source.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IngestMetadata {
    /// All discovered tracks
    #[serde(default)]
    pub tracks: Vec<TrackInfo>,

    /// Container format (e.g., "mp4", "flv", "ts", "wav")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,

    /// Total duration in milliseconds (None for live streams)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,

    /// Overall bitrate in bits per second
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bitrate: Option<u32>,
}

impl IngestMetadata {
    /// Check if this is a live stream (no duration)
    pub fn is_live(&self) -> bool {
        self.duration_ms.is_none()
    }

    /// Get the first audio track info
    pub fn first_audio(&self) -> Option<&TrackInfo> {
        self.tracks
            .iter()
            .find(|t| t.media_type == MediaType::Audio)
    }

    /// Get the first video track info
    pub fn first_video(&self) -> Option<&TrackInfo> {
        self.tracks
            .iter()
            .find(|t| t.media_type == MediaType::Video)
    }

    /// Get all audio tracks
    pub fn audio_tracks(&self) -> impl Iterator<Item = &TrackInfo> {
        self.tracks
            .iter()
            .filter(|t| t.media_type == MediaType::Audio)
    }

    /// Get all video tracks
    pub fn video_tracks(&self) -> impl Iterator<Item = &TrackInfo> {
        self.tracks
            .iter()
            .filter(|t| t.media_type == MediaType::Video)
    }

    /// Get track by stream_id
    pub fn track_by_id(&self, stream_id: &str) -> Option<&TrackInfo> {
        self.tracks.iter().find(|t| t.stream_id == stream_id)
    }
}

/// Information about a single media track
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackInfo {
    /// Track type (audio, video, subtitle, data)
    pub media_type: MediaType,

    /// Track index within the source (0-based)
    pub index: u32,

    /// Unique identifier used in RuntimeData.stream_id
    /// Format: "{media_type}:{index}" (e.g., "audio:0", "video:1")
    pub stream_id: String,

    /// Language code (ISO 639-1/639-2, e.g., "eng", "spa")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,

    /// Codec name (e.g., "aac", "opus", "h264", "vp8")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,

    /// Type-specific properties
    pub properties: TrackProperties,
}

impl TrackInfo {
    /// Create audio track info
    pub fn audio(index: u32, sample_rate: u32, channels: u16) -> Self {
        Self {
            media_type: MediaType::Audio,
            index,
            stream_id: MediaType::Audio.stream_id(index),
            language: None,
            codec: None,
            properties: TrackProperties::Audio(AudioTrackProperties {
                sample_rate,
                channels,
                bit_depth: None,
            }),
        }
    }

    /// Create video track info
    pub fn video(index: u32, width: u32, height: u32, framerate: f64) -> Self {
        Self {
            media_type: MediaType::Video,
            index,
            stream_id: MediaType::Video.stream_id(index),
            language: None,
            codec: None,
            properties: TrackProperties::Video(VideoTrackProperties {
                width,
                height,
                framerate,
                pixel_format: None,
            }),
        }
    }

    /// Builder: set language
    pub fn with_language(mut self, lang: impl Into<String>) -> Self {
        self.language = Some(lang.into());
        self
    }

    /// Builder: set codec
    pub fn with_codec(mut self, codec: impl Into<String>) -> Self {
        self.codec = Some(codec.into());
        self
    }
}

/// Type-specific track properties
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TrackProperties {
    /// Audio track properties
    Audio(AudioTrackProperties),

    /// Video track properties
    Video(VideoTrackProperties),

    /// Subtitle track properties
    Subtitle(SubtitleTrackProperties),

    /// Data track properties
    Data(DataTrackProperties),
}

/// Audio-specific track properties
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioTrackProperties {
    /// Sample rate in Hz
    pub sample_rate: u32,

    /// Number of channels
    pub channels: u16,

    /// Bits per sample (e.g., 16, 24, 32)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bit_depth: Option<u16>,
}

/// Video-specific track properties
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoTrackProperties {
    /// Frame width in pixels
    pub width: u32,

    /// Frame height in pixels
    pub height: u32,

    /// Framerate in FPS
    pub framerate: f64,

    /// Pixel format (e.g., "yuv420p", "rgb24")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pixel_format: Option<String>,
}

/// Subtitle-specific track properties
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubtitleTrackProperties {
    /// Subtitle format (e.g., "srt", "webvtt", "ass")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

/// Data-specific track properties
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DataTrackProperties {
    /// Data format description
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingest_metadata_queries() {
        let metadata = IngestMetadata {
            tracks: vec![
                TrackInfo::audio(0, 48000, 2).with_codec("aac"),
                TrackInfo::audio(1, 16000, 1)
                    .with_codec("opus")
                    .with_language("eng"),
                TrackInfo::video(0, 1920, 1080, 30.0).with_codec("h264"),
            ],
            format: Some("mp4".to_string()),
            duration_ms: Some(120_000),
            bitrate: Some(5_000_000),
        };

        assert!(!metadata.is_live());
        assert!(metadata.first_audio().is_some());
        assert!(metadata.first_video().is_some());
        assert_eq!(metadata.audio_tracks().count(), 2);
        assert_eq!(metadata.video_tracks().count(), 1);
        assert!(metadata.track_by_id("audio:1").is_some());
        assert!(metadata.track_by_id("video:0").is_some());
        assert!(metadata.track_by_id("audio:5").is_none());
    }

    #[test]
    fn test_track_info_builders() {
        let audio = TrackInfo::audio(0, 44100, 2)
            .with_codec("flac")
            .with_language("jpn");

        assert_eq!(audio.stream_id, "audio:0");
        assert_eq!(audio.codec, Some("flac".to_string()));
        assert_eq!(audio.language, Some("jpn".to_string()));

        if let TrackProperties::Audio(props) = &audio.properties {
            assert_eq!(props.sample_rate, 44100);
            assert_eq!(props.channels, 2);
        } else {
            panic!("Expected audio properties");
        }
    }

    #[test]
    fn test_metadata_serialization() {
        let metadata = IngestMetadata {
            tracks: vec![
                TrackInfo::audio(0, 48000, 2),
                TrackInfo::video(0, 1280, 720, 25.0),
            ],
            format: Some("ts".to_string()),
            duration_ms: None,
            bitrate: Some(2_500_000),
        };

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: IngestMetadata = serde_json::from_str(&json).unwrap();

        assert!(deserialized.is_live());
        assert_eq!(deserialized.tracks.len(), 2);
        assert_eq!(deserialized.format, Some("ts".to_string()));
    }

    #[test]
    fn test_track_properties_serialization() {
        let video_props = TrackProperties::Video(VideoTrackProperties {
            width: 1920,
            height: 1080,
            framerate: 29.97,
            pixel_format: Some("yuv420p".to_string()),
        });

        let json = serde_json::to_string(&video_props).unwrap();
        assert!(json.contains("\"type\":\"video\""));

        let deserialized: TrackProperties = serde_json::from_str(&json).unwrap();
        if let TrackProperties::Video(props) = deserialized {
            assert_eq!(props.width, 1920);
            assert_eq!(props.framerate, 29.97);
        } else {
            panic!("Expected video properties");
        }
    }
}
