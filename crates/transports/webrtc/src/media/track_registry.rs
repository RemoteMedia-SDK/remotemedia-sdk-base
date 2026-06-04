//! Track Registry for Dynamic Multi-Track Streaming (Spec 013)
//!
//! Central coordinator for managing multiple audio and video tracks per peer connection.
//! Supports dynamic track creation on first frame arrival with duplicate prevention.

use crate::{Error, Result};
use remotemedia_core::transport::session_control::SessionControl;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, info};

use super::audio_sender::MediaClockPublisher;
use super::validate_stream_id;
use super::MediaClockAttach;

/// Maximum number of audio tracks per peer (FR-018)
pub const MAX_AUDIO_TRACKS_PER_PEER: usize = 10;

/// Maximum number of video tracks per peer (FR-019)
pub const MAX_VIDEO_TRACKS_PER_PEER: usize = 5;

/// Default stream ID used when no stream_id is specified (backward compatibility)
pub const DEFAULT_STREAM_ID: &str = "default";

/// Track lifecycle information
#[derive(Debug, Clone)]
pub struct TrackInfo {
    /// Stream identifier
    pub stream_id: String,

    /// When the track was created
    pub created_at: Instant,

    /// Last frame timestamp (for activity monitoring - FR-028)
    pub last_frame_at: Instant,

    /// Total frames sent through this track
    pub frame_count: u64,
}

impl TrackInfo {
    /// Create new track info
    pub fn new(stream_id: String) -> Self {
        let now = Instant::now();
        Self {
            stream_id,
            created_at: now,
            last_frame_at: now,
            frame_count: 0,
        }
    }

    /// Update last frame timestamp and increment counter
    pub fn record_frame(&mut self) {
        self.last_frame_at = Instant::now();
        self.frame_count += 1;
    }

    /// Check if track is inactive (no frames for specified duration)
    pub fn is_inactive(&self, inactive_threshold: std::time::Duration) -> bool {
        self.last_frame_at.elapsed() > inactive_threshold
    }
}

/// Generic track handle that wraps the actual track with metadata
pub struct TrackHandle<T> {
    /// The actual track (AudioTrack or VideoTrack)
    pub track: Arc<T>,

    /// Track lifecycle information
    pub info: TrackInfo,
}

impl<T> TrackHandle<T> {
    /// Create a new track handle
    pub fn new(track: Arc<T>, stream_id: String) -> Self {
        Self {
            track,
            info: TrackInfo::new(stream_id),
        }
    }
}

/// Registry for managing multiple tracks per peer connection
///
/// Thread-safe with separate registries for audio and video tracks.
/// Supports dynamic track creation with duplicate prevention.
pub struct TrackRegistry<A, V> {
    /// Audio tracks keyed by stream_id
    audio_tracks: Arc<RwLock<HashMap<String, TrackHandle<A>>>>,

    /// Video tracks keyed by stream_id
    video_tracks: Arc<RwLock<HashMap<String, TrackHandle<V>>>>,

    /// Peer ID for logging
    peer_id: String,

    /// Optional `SessionControl` for the pipeline session bound to
    /// this peer. When set via [`Self::attach_session_control`],
    /// every subsequent `register_*_track` call automatically
    /// attaches a [`MediaClockPublisher`] keyed at
    /// `<medium>:<stream_id>` so wire-bound pacers
    /// (`PacingNature::ClockedToOutboundMedia`) tick at the actual
    /// outbound RTP cadence.
    ///
    /// When `None` (back-compat / pre-bind), tracks register without
    /// publishers — same behavior as before Phase 5 wiring.
    session_control: Arc<RwLock<Option<Arc<SessionControl>>>>,
}

impl<A, V> TrackRegistry<A, V> {
    /// Create a new track registry for a peer
    pub fn new(peer_id: String) -> Self {
        info!("Creating TrackRegistry for peer {}", peer_id);
        Self {
            audio_tracks: Arc::new(RwLock::new(HashMap::new())),
            video_tracks: Arc::new(RwLock::new(HashMap::new())),
            peer_id,
            session_control: Arc::new(RwLock::new(None)),
        }
    }

    /// Attach the per-session [`SessionControl`] so subsequent
    /// `register_*_track` calls automatically wire a
    /// `MediaClockPublisher` for the registered track. Called once
    /// per `(peer, session)` after `executor.create_session(...)`
    /// returns, before any track registration.
    ///
    /// Idempotent — replaces any prior attached control. Existing
    /// already-registered tracks are *not* retroactively wired
    /// (callers should attach control before registering tracks).
    pub async fn attach_session_control(&self, control: Arc<SessionControl>) {
        *self.session_control.write().await = Some(control);
    }

    /// Currently attached `SessionControl`, if any. Used internally by
    /// `register_*_track` to decide whether to build a publisher.
    async fn current_control(&self) -> Option<Arc<SessionControl>> {
        self.session_control.read().await.clone()
    }

    // =========================================================================
    // Audio Track Management
    // =========================================================================

    /// Get an audio track by stream_id (returns None if not found)
    pub async fn get_audio_track(&self, stream_id: &str) -> Option<Arc<A>> {
        let tracks = self.audio_tracks.read().await;
        tracks.get(stream_id).map(|h| Arc::clone(&h.track))
    }

    /// Check if an audio track exists for the given stream_id
    pub async fn has_audio_track(&self, stream_id: &str) -> bool {
        let tracks = self.audio_tracks.read().await;
        tracks.contains_key(stream_id)
    }

    /// Get the number of audio tracks
    pub async fn audio_track_count(&self) -> usize {
        let tracks = self.audio_tracks.read().await;
        tracks.len()
    }

    /// Get all audio stream IDs
    pub async fn audio_stream_ids(&self) -> Vec<String> {
        let tracks = self.audio_tracks.read().await;
        tracks.keys().cloned().collect()
    }

    /// Register a new audio track (T007: duplicate prevention)
    ///
    /// # Arguments
    ///
    /// * `stream_id` - Stream identifier (validated)
    /// * `track` - The audio track to register
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Track was registered successfully
    /// * `Ok(false)` - Track already exists for this stream_id
    /// * `Err` - Validation error or track limit reached
    pub async fn register_audio_track(&self, stream_id: &str, track: Arc<A>) -> Result<bool>
    where
        A: MediaClockAttach,
    {
        // Validate stream_id (FR-003, FR-004, FR-005)
        validate_stream_id(stream_id).map_err(|e| Error::InvalidStreamId(e.to_string()))?;

        // Pre-resolve duplicate / limit / control under the existing
        // write lock; we'll release it before the async
        // `set_media_clock` call so we don't hold the registry lock
        // across an arbitrary track-side await.
        let do_attach = {
            let mut tracks = self.audio_tracks.write().await;

            // Check for duplicate (FR-009)
            if tracks.contains_key(stream_id) {
                debug!(
                    "Audio track already exists for stream_id '{}' on peer {}",
                    stream_id, self.peer_id
                );
                return Ok(false);
            }

            // Check track limit (FR-018)
            if tracks.len() >= MAX_AUDIO_TRACKS_PER_PEER {
                return Err(Error::TrackLimitExceeded(format!(
                    "Cannot create audio track: limit of {} reached for peer {}",
                    MAX_AUDIO_TRACKS_PER_PEER, self.peer_id
                )));
            }

            // Register the track
            let handle = TrackHandle::new(Arc::clone(&track), stream_id.to_string());
            tracks.insert(stream_id.to_string(), handle);

            info!(
                "Registered audio track '{}' for peer {} (total: {})",
                stream_id,
                self.peer_id,
                tracks.len()
            );

            true
        };

        // Phase 5.1 wiring: if a SessionControl is attached, build a
        // MediaClockPublisher addressed at `audio:<stream_id>` and
        // attach it to the track's inner sender. ClockedToOutboundMedia
        // nodes binding to that address now receive ticks at the Opus
        // egress cadence.
        if do_attach {
            if let Some(control) = self.current_control().await {
                let publisher = MediaClockPublisher::new(control, "audio", stream_id);
                track.set_media_clock(publisher).await;
            }
        }

        Ok(do_attach)
    }

    /// Remove an audio track by stream_id (FR-027)
    pub async fn remove_audio_track(&self, stream_id: &str) -> Option<Arc<A>>
    where
        A: MediaClockAttach,
    {
        let mut tracks = self.audio_tracks.write().await;
        let removed = tracks.remove(stream_id).map(|h| h.track);
        let count = tracks.len();
        drop(tracks); // release before the async detach

        if let Some(track) = removed.as_ref() {
            // Phase 5.9: stop the wire clock so subscribed pacers exit
            // cleanly. No-op if no publisher was attached.
            track.detach_media_clock().await;
            info!(
                "Removed audio track '{}' from peer {} (remaining: {})",
                stream_id, self.peer_id, count
            );
        }

        removed
    }

    /// Record a frame being sent on an audio track (FR-028)
    pub async fn record_audio_frame(&self, stream_id: &str) {
        let mut tracks = self.audio_tracks.write().await;
        if let Some(handle) = tracks.get_mut(stream_id) {
            handle.info.record_frame();
        }
    }

    // =========================================================================
    // Video Track Management
    // =========================================================================

    /// Get a video track by stream_id (returns None if not found)
    pub async fn get_video_track(&self, stream_id: &str) -> Option<Arc<V>> {
        let tracks = self.video_tracks.read().await;
        tracks.get(stream_id).map(|h| Arc::clone(&h.track))
    }

    /// Check if a video track exists for the given stream_id
    pub async fn has_video_track(&self, stream_id: &str) -> bool {
        let tracks = self.video_tracks.read().await;
        tracks.contains_key(stream_id)
    }

    /// Get the number of video tracks
    pub async fn video_track_count(&self) -> usize {
        let tracks = self.video_tracks.read().await;
        tracks.len()
    }

    /// Get all video stream IDs
    pub async fn video_stream_ids(&self) -> Vec<String> {
        let tracks = self.video_tracks.read().await;
        tracks.keys().cloned().collect()
    }

    /// Register a new video track (T007: duplicate prevention)
    ///
    /// # Arguments
    ///
    /// * `stream_id` - Stream identifier (validated)
    /// * `track` - The video track to register
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Track was registered successfully
    /// * `Ok(false)` - Track already exists for this stream_id
    /// * `Err` - Validation error or track limit reached
    pub async fn register_video_track(&self, stream_id: &str, track: Arc<V>) -> Result<bool>
    where
        V: MediaClockAttach,
    {
        // Validate stream_id (FR-003, FR-004, FR-005)
        validate_stream_id(stream_id).map_err(|e| Error::InvalidStreamId(e.to_string()))?;

        let do_attach = {
            let mut tracks = self.video_tracks.write().await;

            // Check for duplicate (FR-009)
            if tracks.contains_key(stream_id) {
                debug!(
                    "Video track already exists for stream_id '{}' on peer {}",
                    stream_id, self.peer_id
                );
                return Ok(false);
            }

            // Check track limit (FR-019)
            if tracks.len() >= MAX_VIDEO_TRACKS_PER_PEER {
                return Err(Error::TrackLimitExceeded(format!(
                    "Cannot create video track: limit of {} reached for peer {}",
                    MAX_VIDEO_TRACKS_PER_PEER, self.peer_id
                )));
            }

            // Register the track
            let handle = TrackHandle::new(Arc::clone(&track), stream_id.to_string());
            tracks.insert(stream_id.to_string(), handle);

            info!(
                "Registered video track '{}' for peer {} (total: {})",
                stream_id,
                self.peer_id,
                tracks.len()
            );

            true
        };

        // Phase 5.2 wiring (Option C): publish media_clock at
        // `video:<stream_id>` per encoded RTP write. AvatarNode and
        // other ClockedToOutboundMedia consumers binding to that
        // address tick at the actual outbound video cadence.
        if do_attach {
            if let Some(control) = self.current_control().await {
                let publisher = MediaClockPublisher::new(control, "video", stream_id);
                track.set_media_clock(publisher).await;
            }
        }

        Ok(true)
    }

    /// Remove a video track by stream_id (FR-027)
    pub async fn remove_video_track(&self, stream_id: &str) -> Option<Arc<V>>
    where
        V: MediaClockAttach,
    {
        let mut tracks = self.video_tracks.write().await;
        let removed = tracks.remove(stream_id).map(|h| h.track);
        let count = tracks.len();
        drop(tracks);

        if let Some(track) = removed.as_ref() {
            // Phase 5.9: stop the wire clock so subscribed pacers exit
            // cleanly. No-op if no publisher was attached.
            track.detach_media_clock().await;
            info!(
                "Removed video track '{}' from peer {} (remaining: {})",
                stream_id, self.peer_id, count
            );
        }

        removed
    }

    /// Record a frame being sent on a video track (FR-028)
    pub async fn record_video_frame(&self, stream_id: &str) {
        let mut tracks = self.video_tracks.write().await;
        if let Some(handle) = tracks.get_mut(stream_id) {
            handle.info.record_frame();
        }
    }

    // =========================================================================
    // Cross-Track Operations
    // =========================================================================

    /// Get total track count (audio + video)
    pub async fn total_track_count(&self) -> usize {
        let audio_count = self.audio_tracks.read().await.len();
        let video_count = self.video_tracks.read().await.len();
        audio_count + video_count
    }

    /// Get inactive tracks (FR-029)
    ///
    /// Returns stream IDs of tracks that haven't received frames within the threshold.
    pub async fn get_inactive_tracks(
        &self,
        threshold: std::time::Duration,
    ) -> (Vec<String>, Vec<String>) {
        let audio_tracks = self.audio_tracks.read().await;
        let video_tracks = self.video_tracks.read().await;

        let inactive_audio: Vec<String> = audio_tracks
            .iter()
            .filter(|(_, h)| h.info.is_inactive(threshold))
            .map(|(id, _)| id.clone())
            .collect();

        let inactive_video: Vec<String> = video_tracks
            .iter()
            .filter(|(_, h)| h.info.is_inactive(threshold))
            .map(|(id, _)| id.clone())
            .collect();

        (inactive_audio, inactive_video)
    }

    /// Clear all tracks (for cleanup on peer disconnect).
    ///
    /// Detaches every track's `MediaClockPublisher` first so subscribed
    /// wire pacers exit cleanly (Phase 5.9). The track's `Arc<>` may
    /// still be held by other components (the peer connection, the
    /// frame router); we only drop the registry's references plus the
    /// inner publishers.
    pub async fn clear(&self)
    where
        A: MediaClockAttach,
        V: MediaClockAttach,
    {
        let audio_handles: Vec<Arc<A>> = {
            let mut t = self.audio_tracks.write().await;
            t.drain().map(|(_, h)| h.track).collect()
        };
        let video_handles: Vec<Arc<V>> = {
            let mut t = self.video_tracks.write().await;
            t.drain().map(|(_, h)| h.track).collect()
        };
        let audio_count = audio_handles.len();
        let video_count = video_handles.len();

        for track in &audio_handles {
            track.detach_media_clock().await;
        }
        for track in &video_handles {
            track.detach_media_clock().await;
        }

        info!(
            "Cleared all tracks for peer {}: {} audio, {} video",
            self.peer_id, audio_count, video_count
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Simple test track type
    struct MockTrack {
        id: String,
    }

    impl MockTrack {
        fn new(id: &str) -> Self {
            Self { id: id.to_string() }
        }
    }

    // No-op MediaClockAttach impl for tests. The registry's
    // attach/detach paths are short-circuited when no SessionControl
    // is wired, but the trait bound on register_*_track still needs
    // to be satisfied by the mock.
    #[async_trait::async_trait]
    impl super::MediaClockAttach for MockTrack {
        async fn set_media_clock(&self, _: MediaClockPublisher) {}
        async fn detach_media_clock(&self) {}
    }

    #[tokio::test]
    async fn test_register_audio_track() {
        let registry: TrackRegistry<MockTrack, MockTrack> = TrackRegistry::new("peer1".to_string());

        let track = Arc::new(MockTrack::new("track1"));
        let result = registry.register_audio_track("voice", track).await;

        assert!(result.is_ok());
        assert!(result.unwrap()); // true = newly registered
        assert_eq!(registry.audio_track_count().await, 1);
    }

    #[tokio::test]
    async fn test_duplicate_prevention() {
        let registry: TrackRegistry<MockTrack, MockTrack> = TrackRegistry::new("peer1".to_string());

        let track1 = Arc::new(MockTrack::new("track1"));
        let track2 = Arc::new(MockTrack::new("track2"));

        // First registration succeeds
        let result1 = registry.register_audio_track("voice", track1).await;
        assert!(result1.is_ok());
        assert!(result1.unwrap());

        // Second registration with same stream_id returns false
        let result2 = registry.register_audio_track("voice", track2).await;
        assert!(result2.is_ok());
        assert!(!result2.unwrap()); // false = already exists

        // Still only one track
        assert_eq!(registry.audio_track_count().await, 1);
    }

    #[tokio::test]
    async fn test_audio_track_limit() {
        let registry: TrackRegistry<MockTrack, MockTrack> = TrackRegistry::new("peer1".to_string());

        // Register up to limit
        for i in 0..MAX_AUDIO_TRACKS_PER_PEER {
            let track = Arc::new(MockTrack::new(&format!("track{}", i)));
            let result = registry
                .register_audio_track(&format!("stream{}", i), track)
                .await;
            assert!(result.is_ok());
        }

        // Next one should fail
        let track = Arc::new(MockTrack::new("overflow"));
        let result = registry.register_audio_track("overflow", track).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_video_track_limit() {
        let registry: TrackRegistry<MockTrack, MockTrack> = TrackRegistry::new("peer1".to_string());

        // Register up to limit
        for i in 0..MAX_VIDEO_TRACKS_PER_PEER {
            let track = Arc::new(MockTrack::new(&format!("track{}", i)));
            let result = registry
                .register_video_track(&format!("stream{}", i), track)
                .await;
            assert!(result.is_ok());
        }

        // Next one should fail
        let track = Arc::new(MockTrack::new("overflow"));
        let result = registry.register_video_track("overflow", track).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_stream_id_validation() {
        let registry: TrackRegistry<MockTrack, MockTrack> = TrackRegistry::new("peer1".to_string());

        let track = Arc::new(MockTrack::new("track"));

        // Valid stream IDs
        assert!(registry
            .register_audio_track("camera", Arc::clone(&track))
            .await
            .is_ok());
        assert!(registry
            .register_audio_track("screen_share", Arc::clone(&track))
            .await
            .is_ok());
        assert!(registry
            .register_audio_track("audio-1", Arc::clone(&track))
            .await
            .is_ok());

        // Invalid stream IDs
        let track2 = Arc::new(MockTrack::new("track"));
        assert!(registry
            .register_video_track("has space", track2)
            .await
            .is_err());

        let track3 = Arc::new(MockTrack::new("track"));
        assert!(registry.register_video_track("", track3).await.is_err());
    }

    #[tokio::test]
    async fn test_remove_track() {
        let registry: TrackRegistry<MockTrack, MockTrack> = TrackRegistry::new("peer1".to_string());

        let track = Arc::new(MockTrack::new("track1"));
        registry.register_audio_track("voice", track).await.unwrap();

        assert_eq!(registry.audio_track_count().await, 1);

        let removed = registry.remove_audio_track("voice").await;
        assert!(removed.is_some());
        assert_eq!(registry.audio_track_count().await, 0);

        // Removing non-existent track returns None
        let removed_again = registry.remove_audio_track("voice").await;
        assert!(removed_again.is_none());
    }

    #[tokio::test]
    async fn test_separate_audio_video_registries() {
        let registry: TrackRegistry<MockTrack, MockTrack> = TrackRegistry::new("peer1".to_string());

        // Same stream_id can be used for both audio and video (FR-013)
        let audio_track = Arc::new(MockTrack::new("audio"));
        let video_track = Arc::new(MockTrack::new("video"));

        assert!(registry
            .register_audio_track("main", audio_track)
            .await
            .is_ok());
        assert!(registry
            .register_video_track("main", video_track)
            .await
            .is_ok());

        assert_eq!(registry.audio_track_count().await, 1);
        assert_eq!(registry.video_track_count().await, 1);
        assert_eq!(registry.total_track_count().await, 2);
    }
}
