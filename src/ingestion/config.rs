//! Configuration types for ingestion sources
//!
//! This module defines all configuration types needed to create and configure
//! ingestion sources from various protocols (file, RTMP, SRT, WebRTC, etc.)

use serde::{Deserialize, Serialize};

use super::status::MediaType;

/// Configuration for creating an ingest source
///
/// # Example
///
/// ```
/// use remotemedia_core::ingestion::{IngestConfig, TrackSelection, AudioConfig, VideoConfig, ReconnectConfig};
///
/// let config = IngestConfig {
///     url: "rtmp://localhost:1935/live/stream".to_string(),
///     audio: Some(AudioConfig::default()),
///     video: Some(VideoConfig::default()),
///     track_selection: TrackSelection::FirstAudioVideo,
///     reconnect: ReconnectConfig::default(),
///     extra: None,
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestConfig {
    /// Source URL (e.g., "rtmp://...", "file:///...", "./local.wav", "-")
    pub url: String,

    /// Audio output configuration (resample/remix)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audio: Option<AudioConfig>,

    /// Video output configuration (scale/convert)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub video: Option<VideoConfig>,

    /// Which tracks to ingest from multi-track sources
    #[serde(default)]
    pub track_selection: TrackSelection,

    /// Reconnection behavior for live streams
    #[serde(default)]
    pub reconnect: ReconnectConfig,

    /// Protocol-specific extra options (JSON object)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra: Option<serde_json::Value>,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            audio: None,
            video: None,
            track_selection: TrackSelection::default(),
            reconnect: ReconnectConfig::default(),
            extra: None,
        }
    }
}

impl IngestConfig {
    /// Create a new config with just a URL
    pub fn from_url(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }

    /// Set audio configuration
    pub fn with_audio(mut self, audio: AudioConfig) -> Self {
        self.audio = Some(audio);
        self
    }

    /// Set video configuration
    pub fn with_video(mut self, video: VideoConfig) -> Self {
        self.video = Some(video);
        self
    }

    /// Set track selection
    pub fn with_track_selection(mut self, selection: TrackSelection) -> Self {
        self.track_selection = selection;
        self
    }

    /// Set reconnect configuration
    pub fn with_reconnect(mut self, reconnect: ReconnectConfig) -> Self {
        self.reconnect = reconnect;
        self
    }
}

/// Audio output configuration
///
/// Specifies the target sample rate and channel count for decoded audio.
/// The ingest source will resample/remix as needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AudioConfig {
    /// Target sample rate in Hz (e.g., 16000, 44100, 48000)
    pub sample_rate: u32,

    /// Target channel count (1 = mono, 2 = stereo)
    pub channels: u16,
}

impl Default for AudioConfig {
    fn default() -> Self {
        Self {
            sample_rate: 16000, // Common for speech processing
            channels: 1,        // Mono for speech
        }
    }
}

impl AudioConfig {
    /// Create audio config for speech processing (16kHz mono)
    pub fn speech() -> Self {
        Self {
            sample_rate: 16000,
            channels: 1,
        }
    }

    /// Create audio config for music (48kHz stereo)
    pub fn music() -> Self {
        Self {
            sample_rate: 48000,
            channels: 2,
        }
    }

    /// Create audio config for telephony (8kHz mono)
    pub fn telephony() -> Self {
        Self {
            sample_rate: 8000,
            channels: 1,
        }
    }
}

/// Video output configuration
///
/// Specifies the target dimensions and pixel format for decoded video.
/// The ingest source will scale/convert as needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoConfig {
    /// Target width in pixels (None = source width)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<u32>,

    /// Target height in pixels (None = source height)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub height: Option<u32>,

    /// Target pixel format (e.g., "yuv420p", "rgb24")
    /// None = source format or decoder default
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pixel_format: Option<String>,
}

impl Default for VideoConfig {
    fn default() -> Self {
        Self {
            width: None,
            height: None,
            pixel_format: None,
        }
    }
}

impl VideoConfig {
    /// Create video config for 720p output
    pub fn hd720() -> Self {
        Self {
            width: Some(1280),
            height: Some(720),
            pixel_format: Some("yuv420p".to_string()),
        }
    }

    /// Create video config for 1080p output
    pub fn hd1080() -> Self {
        Self {
            width: Some(1920),
            height: Some(1080),
            pixel_format: Some("yuv420p".to_string()),
        }
    }

    /// Create video config for analysis (smaller frames)
    pub fn analysis() -> Self {
        Self {
            width: Some(640),
            height: Some(480),
            pixel_format: Some("rgb24".to_string()),
        }
    }
}

/// Reconnection behavior for live stream sources
///
/// Configures automatic reconnection with exponential backoff for
/// transient network failures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconnectConfig {
    /// Whether to attempt reconnection on disconnect
    pub enabled: bool,

    /// Maximum number of reconnection attempts (0 = unlimited)
    pub max_attempts: u32,

    /// Initial delay between reconnection attempts (milliseconds)
    pub delay_ms: u64,

    /// Multiplier for exponential backoff (e.g., 2.0 = double each time)
    pub backoff_multiplier: f32,

    /// Maximum delay between attempts (milliseconds)
    pub max_delay_ms: u64,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_attempts: 5,
            delay_ms: 1000,
            backoff_multiplier: 2.0,
            max_delay_ms: 30_000,
        }
    }
}

impl ReconnectConfig {
    /// Create config with reconnection disabled
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Create config for aggressive reconnection (many fast retries)
    pub fn aggressive() -> Self {
        Self {
            enabled: true,
            max_attempts: 10,
            delay_ms: 500,
            backoff_multiplier: 1.5,
            max_delay_ms: 10_000,
        }
    }

    /// Create config for persistent connection (unlimited retries)
    pub fn persistent() -> Self {
        Self {
            enabled: true,
            max_attempts: 0, // unlimited
            delay_ms: 1000,
            backoff_multiplier: 2.0,
            max_delay_ms: 60_000,
        }
    }

    /// Calculate the delay for a given attempt number
    pub fn delay_for_attempt(&self, attempt: u32) -> u64 {
        if !self.enabled || (self.max_attempts > 0 && attempt >= self.max_attempts) {
            return 0;
        }

        let delay = self.delay_ms as f64 * self.backoff_multiplier.powi(attempt as i32) as f64;
        (delay as u64).min(self.max_delay_ms)
    }
}

/// Track selection mode for multi-track sources
///
/// Controls which audio, video, and subtitle tracks are ingested from
/// sources that contain multiple tracks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TrackSelection {
    /// Ingest all available tracks (audio, video, subtitles)
    All,

    /// Ingest first audio and first video track only (default)
    FirstAudioVideo,

    /// Ingest specific tracks by selector
    Specific(Vec<TrackSelector>),
}

impl Default for TrackSelection {
    fn default() -> Self {
        Self::FirstAudioVideo
    }
}

/// Selector for choosing specific tracks from multi-track sources
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TrackSelector {
    /// Track type to select
    pub media_type: MediaType,

    /// Track index (None = first of this type)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<u32>,

    /// Language filter (None = any language)
    /// Uses ISO 639-1/639-2 codes (e.g., "eng", "spa", "jpn")
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
}

impl TrackSelector {
    /// Select the first audio track
    pub fn first_audio() -> Self {
        Self {
            media_type: MediaType::Audio,
            index: None,
            language: None,
        }
    }

    /// Select the first video track
    pub fn first_video() -> Self {
        Self {
            media_type: MediaType::Video,
            index: None,
            language: None,
        }
    }

    /// Select audio track with specific language
    pub fn audio_language(lang: impl Into<String>) -> Self {
        Self {
            media_type: MediaType::Audio,
            index: None,
            language: Some(lang.into()),
        }
    }

    /// Select audio track by index
    pub fn audio_index(index: u32) -> Self {
        Self {
            media_type: MediaType::Audio,
            index: Some(index),
            language: None,
        }
    }

    /// Select video track by index
    pub fn video_index(index: u32) -> Self {
        Self {
            media_type: MediaType::Video,
            index: Some(index),
            language: None,
        }
    }

    /// Select subtitle track with specific language
    pub fn subtitle_language(lang: impl Into<String>) -> Self {
        Self {
            media_type: MediaType::Subtitle,
            index: None,
            language: Some(lang.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingest_config_serialization_roundtrip() {
        let config = IngestConfig {
            url: "rtmp://localhost:1935/live/stream".to_string(),
            audio: Some(AudioConfig::speech()),
            video: Some(VideoConfig::hd720()),
            track_selection: TrackSelection::FirstAudioVideo,
            reconnect: ReconnectConfig::default(),
            extra: Some(serde_json::json!({"key": "value"})),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: IngestConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config.url, deserialized.url);
        assert_eq!(config.track_selection, deserialized.track_selection);
    }

    #[test]
    fn test_track_selection_default_is_first_audio_video() {
        let selection = TrackSelection::default();
        assert_eq!(selection, TrackSelection::FirstAudioVideo);
    }

    #[test]
    fn test_reconnect_delay_calculation() {
        let config = ReconnectConfig {
            enabled: true,
            max_attempts: 5,
            delay_ms: 1000,
            backoff_multiplier: 2.0,
            max_delay_ms: 10_000,
        };

        assert_eq!(config.delay_for_attempt(0), 1000); // 1000 * 2^0 = 1000
        assert_eq!(config.delay_for_attempt(1), 2000); // 1000 * 2^1 = 2000
        assert_eq!(config.delay_for_attempt(2), 4000); // 1000 * 2^2 = 4000
        assert_eq!(config.delay_for_attempt(3), 8000); // 1000 * 2^3 = 8000
        assert_eq!(config.delay_for_attempt(4), 10_000); // clamped to max
        assert_eq!(config.delay_for_attempt(5), 0); // past max_attempts
    }

    #[test]
    fn test_audio_config_presets() {
        let speech = AudioConfig::speech();
        assert_eq!(speech.sample_rate, 16000);
        assert_eq!(speech.channels, 1);

        let music = AudioConfig::music();
        assert_eq!(music.sample_rate, 48000);
        assert_eq!(music.channels, 2);

        let telephony = AudioConfig::telephony();
        assert_eq!(telephony.sample_rate, 8000);
        assert_eq!(telephony.channels, 1);
    }

    #[test]
    fn test_video_config_presets() {
        let hd720 = VideoConfig::hd720();
        assert_eq!(hd720.width, Some(1280));
        assert_eq!(hd720.height, Some(720));

        let analysis = VideoConfig::analysis();
        assert_eq!(analysis.width, Some(640));
        assert_eq!(analysis.height, Some(480));
        assert_eq!(analysis.pixel_format, Some("rgb24".to_string()));
    }

    #[test]
    fn test_track_selector_builders() {
        let audio = TrackSelector::first_audio();
        assert_eq!(audio.media_type, MediaType::Audio);
        assert!(audio.index.is_none());

        let eng_audio = TrackSelector::audio_language("eng");
        assert_eq!(eng_audio.language, Some("eng".to_string()));

        let video_1 = TrackSelector::video_index(1);
        assert_eq!(video_1.media_type, MediaType::Video);
        assert_eq!(video_1.index, Some(1));
    }
}
