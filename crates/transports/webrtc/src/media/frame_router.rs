//! Frame Router for Dynamic Multi-Track Streaming (Spec 013)
//!
//! Routes incoming RuntimeData to appropriate tracks based on stream_id.
//! Supports automatic track creation on first frame and fallback to default track.

use crate::{Error, Result};
use remotemedia_core::data::RuntimeData;
use std::sync::Arc;
use tracing::{debug, info};

use super::track_registry::{TrackRegistry, DEFAULT_STREAM_ID};
use super::tracks::{AudioTrack, VideoTrack};

/// Callback type for creating new tracks dynamically
pub type AudioTrackCreator = Box<dyn Fn(&str) -> Result<Arc<AudioTrack>> + Send + Sync>;
pub type VideoTrackCreator = Box<dyn Fn(&str) -> Result<Arc<VideoTrack>> + Send + Sync>;

/// Frame router for stream_id based routing (T008-T009)
///
/// Routes RuntimeData::Audio and RuntimeData::Video to their respective tracks
/// based on the stream_id field. Supports:
/// - Direct routing to existing tracks
/// - Automatic track creation on first frame (FR-008)
/// - Fallback to default track when stream_id is None (FR-006)
pub struct FrameRouter {
    /// Track registry for looking up and managing tracks
    registry: Arc<TrackRegistry<AudioTrack, VideoTrack>>,

    /// Optional callback for creating audio tracks dynamically
    audio_track_creator: Option<AudioTrackCreator>,

    /// Optional callback for creating video tracks dynamically
    video_track_creator: Option<VideoTrackCreator>,

    /// Peer ID for logging
    peer_id: String,

    /// Whether to auto-create tracks on first frame (FR-008)
    auto_create_tracks: bool,
}

impl FrameRouter {
    /// Create a new frame router
    ///
    /// # Arguments
    ///
    /// * `registry` - Track registry for managing tracks
    /// * `peer_id` - Peer identifier for logging
    pub fn new(registry: Arc<TrackRegistry<AudioTrack, VideoTrack>>, peer_id: String) -> Self {
        Self {
            registry,
            audio_track_creator: None,
            video_track_creator: None,
            peer_id,
            auto_create_tracks: true, // Default to auto-create
        }
    }

    /// Set the callback for creating audio tracks dynamically
    pub fn with_audio_track_creator(mut self, creator: AudioTrackCreator) -> Self {
        self.audio_track_creator = Some(creator);
        self
    }

    /// Set the callback for creating video tracks dynamically
    pub fn with_video_track_creator(mut self, creator: VideoTrackCreator) -> Self {
        self.video_track_creator = Some(creator);
        self
    }

    /// Enable or disable auto-creation of tracks
    pub fn with_auto_create(mut self, enabled: bool) -> Self {
        self.auto_create_tracks = enabled;
        self
    }

    /// Route a RuntimeData frame to the appropriate track
    ///
    /// # Arguments
    ///
    /// * `data` - RuntimeData to route (Audio or Video)
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Frame was routed successfully
    /// * `Err` - Track not found and auto-create disabled/failed
    ///
    /// # Behavior
    ///
    /// 1. Extract stream_id from data (or use DEFAULT_STREAM_ID if None)
    /// 2. Look up track in registry
    /// 3. If not found and auto-create enabled, create track dynamically
    /// 4. Send frame to track
    pub async fn route(&self, data: RuntimeData) -> Result<()> {
        // Extract stream_id first without borrowing data
        let (is_audio, is_video, stream_id_owned) = match &data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                stream_id,
                ..
            } => {
                let sid = stream_id
                    .as_deref()
                    .unwrap_or(DEFAULT_STREAM_ID)
                    .to_string();
                debug!(
                    "Routing audio frame to stream '{}' ({} samples, {}Hz)",
                    sid,
                    samples.len(),
                    sample_rate
                );
                (true, false, sid)
            }
            RuntimeData::Video {
                width,
                height,
                stream_id,
                ..
            } => {
                let sid = stream_id
                    .as_deref()
                    .unwrap_or(DEFAULT_STREAM_ID)
                    .to_string();
                debug!(
                    "Routing video frame to stream '{}' ({}x{})",
                    sid, width, height
                );
                (false, true, sid)
            }
            _ => {
                // Non-media data types are passed through without routing
                debug!("Non-media data type, skipping routing");
                return Ok(());
            }
        };

        // Now route with owned stream_id
        if is_audio {
            self.route_audio(data, &stream_id_owned).await
        } else if is_video {
            self.route_video(data, &stream_id_owned).await
        } else {
            Ok(())
        }
    }

    /// Route audio data to the appropriate track
    async fn route_audio(&self, data: RuntimeData, stream_id: &str) -> Result<()> {
        // Get or create track
        let track = self.get_or_create_audio_track(stream_id).await?;

        // Extract audio data
        if let RuntimeData::Audio {
            samples,
            sample_rate,
            ..
        } = data
        {
            // Send audio through the track.
            // `send_audio` currently takes `Arc<Vec<f32>>`; convert from
            // `AudioSamples` here. `into_vec` reuses the allocation for
            // the `Vec` variant; the `Arc` / `Pooled` variants allocate
            // once. Upgrading `send_audio` to accept `Arc<[f32]>` would
            // make the `Arc` variant zero-copy end-to-end.
            track
                .send_audio(Arc::new(samples.into_vec()), sample_rate)
                .await
                .map_err(|e| Error::MediaTrackError(format!("Failed to send audio: {}", e)))?;

            // Record frame for activity tracking
            self.registry.record_audio_frame(stream_id).await;

            Ok(())
        } else {
            Err(Error::InvalidData(
                "Expected audio data for audio routing".to_string(),
            ))
        }
    }

    /// Route video data to the appropriate track
    async fn route_video(&self, data: RuntimeData, stream_id: &str) -> Result<()> {
        // Get or create track
        let track = self.get_or_create_video_track(stream_id).await?;

        // Send video through the track
        track
            .send_video_runtime_data(data)
            .await
            .map_err(|e| Error::MediaTrackError(format!("Failed to send video: {}", e)))?;

        // Record frame for activity tracking
        self.registry.record_video_frame(stream_id).await;

        Ok(())
    }

    /// Get existing audio track or create new one if auto-create is enabled
    async fn get_or_create_audio_track(&self, stream_id: &str) -> Result<Arc<AudioTrack>> {
        // Try to get existing track
        if let Some(track) = self.registry.get_audio_track(stream_id).await {
            return Ok(track);
        }

        // Track doesn't exist - check if we can create it
        if !self.auto_create_tracks {
            return Err(Error::TrackNotFound(format!(
                "Audio track '{}' not found and auto-create is disabled",
                stream_id
            )));
        }

        // Create track dynamically
        if let Some(ref creator) = self.audio_track_creator {
            info!(
                "Auto-creating audio track '{}' for peer {}",
                stream_id, self.peer_id
            );

            let track = creator(stream_id)?;

            // Register the new track
            self.registry
                .register_audio_track(stream_id, Arc::clone(&track))
                .await?;

            Ok(track)
        } else {
            Err(Error::TrackNotFound(format!(
                "Audio track '{}' not found and no creator configured",
                stream_id
            )))
        }
    }

    /// Get existing video track or create new one if auto-create is enabled
    async fn get_or_create_video_track(&self, stream_id: &str) -> Result<Arc<VideoTrack>> {
        // Try to get existing track
        if let Some(track) = self.registry.get_video_track(stream_id).await {
            return Ok(track);
        }

        // Track doesn't exist - check if we can create it
        if !self.auto_create_tracks {
            return Err(Error::TrackNotFound(format!(
                "Video track '{}' not found and auto-create is disabled",
                stream_id
            )));
        }

        // Create track dynamically
        if let Some(ref creator) = self.video_track_creator {
            info!(
                "Auto-creating video track '{}' for peer {}",
                stream_id, self.peer_id
            );

            let track = creator(stream_id)?;

            // Register the new track
            self.registry
                .register_video_track(stream_id, Arc::clone(&track))
                .await?;

            Ok(track)
        } else {
            Err(Error::TrackNotFound(format!(
                "Video track '{}' not found and no creator configured",
                stream_id
            )))
        }
    }

    /// Get the underlying track registry
    pub fn registry(&self) -> &Arc<TrackRegistry<AudioTrack, VideoTrack>> {
        &self.registry
    }

    /// Get peer ID
    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }
}

/// Helper to extract stream_id from RuntimeData
pub fn extract_stream_id(data: &RuntimeData) -> Option<&str> {
    match data {
        RuntimeData::Audio { stream_id, .. } => stream_id.as_deref(),
        RuntimeData::Video { stream_id, .. } => stream_id.as_deref(),
        _ => None,
    }
}

/// Helper to set stream_id on RuntimeData (creates new instance)
pub fn with_stream_id(data: RuntimeData, new_stream_id: Option<String>) -> RuntimeData {
    match data {
        RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            ..
        } => RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            stream_id: new_stream_id,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        },
        RuntimeData::Video {
            pixel_data,
            width,
            height,
            format,
            codec,
            frame_number,
            timestamp_us,
            is_keyframe,
            ..
        } => RuntimeData::Video {
            pixel_data,
            width,
            height,
            format,
            codec,
            frame_number,
            timestamp_us,
            is_keyframe,
            stream_id: new_stream_id,
            arrival_ts_us: None,
        },
        other => other, // Non-media types pass through unchanged
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_stream_id_audio() {
        let data = RuntimeData::Audio {
            samples: vec![0.0; 100].into(),
            sample_rate: 48000,
            channels: 1,
            stream_id: Some("voice".to_string()),
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        };

        assert_eq!(extract_stream_id(&data), Some("voice"));
    }

    #[test]
    fn test_extract_stream_id_audio_none() {
        let data = RuntimeData::Audio {
            samples: vec![0.0; 100].into(),
            sample_rate: 48000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        };

        assert_eq!(extract_stream_id(&data), None);
    }

    #[test]
    fn test_extract_stream_id_video() {
        use remotemedia_core::data::video::PixelFormat;

        let data = RuntimeData::Video {
            pixel_data: vec![0u8; 100],
            width: 640,
            height: 480,
            format: PixelFormat::Yuv420p,
            codec: None,
            frame_number: 0,
            timestamp_us: 0,
            is_keyframe: false,
            stream_id: Some("camera".to_string()),
            arrival_ts_us: None,
        };

        assert_eq!(extract_stream_id(&data), Some("camera"));
    }

    #[test]
    fn test_extract_stream_id_non_media() {
        let data = RuntimeData::Text("hello".to_string());
        assert_eq!(extract_stream_id(&data), None);
    }

    #[test]
    fn test_with_stream_id_audio() {
        let data = RuntimeData::Audio {
            samples: vec![1.0, 2.0, 3.0].into(),
            sample_rate: 48000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        };

        let modified = with_stream_id(data, Some("voice".to_string()));

        if let RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            stream_id,
            ..
        } = modified
        {
            assert_eq!(samples.as_slice(), &[1.0, 2.0, 3.0]);
            assert_eq!(sample_rate, 48000);
            assert_eq!(channels, 1);
            assert_eq!(stream_id, Some("voice".to_string()));
        } else {
            panic!("Expected Audio variant");
        }
    }

    #[test]
    fn test_with_stream_id_video() {
        use remotemedia_core::data::video::PixelFormat;

        let data = RuntimeData::Video {
            pixel_data: vec![128u8; 100],
            width: 640,
            height: 480,
            format: PixelFormat::Rgb24,
            codec: None,
            frame_number: 42,
            timestamp_us: 1000,
            is_keyframe: true,
            stream_id: None,
            arrival_ts_us: None,
        };

        let modified = with_stream_id(data, Some("screen".to_string()));

        if let RuntimeData::Video {
            pixel_data,
            width,
            height,
            frame_number,
            is_keyframe,
            stream_id,
            ..
        } = modified
        {
            assert_eq!(pixel_data.len(), 100);
            assert_eq!(width, 640);
            assert_eq!(height, 480);
            assert_eq!(frame_number, 42);
            assert!(is_keyframe);
            assert_eq!(stream_id, Some("screen".to_string()));
        } else {
            panic!("Expected Video variant");
        }
    }

    #[test]
    fn test_with_stream_id_non_media() {
        let data = RuntimeData::Text("unchanged".to_string());
        let modified = with_stream_id(data, Some("ignored".to_string()));

        if let RuntimeData::Text(s) = modified {
            assert_eq!(s, "unchanged");
        } else {
            panic!("Expected Text variant");
        }
    }
}
