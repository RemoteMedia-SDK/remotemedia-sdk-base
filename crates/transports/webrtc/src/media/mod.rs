//! Media codec integration (Opus audio, VP9/H264 video)
//!
//! Handles encoding/decoding and media track management.

pub mod audio;
pub mod audio_sender;
pub mod frame_router;
pub mod track_registry;
pub mod tracks;
pub mod video;

// Re-export AudioTrack and VideoTrack for public use
pub use audio_sender::MediaClockPublisher;
pub use tracks::{AudioTrack, VideoTrack};

/// Capability for attaching a per-stream `MediaClockPublisher` to a
/// media track. Implemented by `AudioTrack` and `VideoTrack`; consumed
/// by `TrackRegistry` so the registry can wire publishers
/// automatically at registration time without the caller knowing
/// which concrete track type it holds.
///
/// `set_media_clock` is async because `AudioTrack`'s inner
/// `AudioSender` lives behind a `tokio::sync::RwLock` (the existing
/// async-locked container). `VideoTrack` is sync but conforms to the
/// async signature for trait uniformity.
#[async_trait::async_trait]
pub trait MediaClockAttach: Send + Sync {
    /// Attach a publisher; subsequent outbound frames fire one
    /// `MediaClock` event each. Idempotent — replaces a prior
    /// publisher (the prior publisher's broadcast remains open until
    /// the caller calls `stop` on it).
    async fn set_media_clock(&self, publisher: MediaClockPublisher);

    /// Detach and stop the publisher, if any. Closes the broadcast so
    /// subscribed wire pacers exit cleanly. Safe to call when none is
    /// attached.
    async fn detach_media_clock(&self);
}

#[async_trait::async_trait]
impl MediaClockAttach for AudioTrack {
    async fn set_media_clock(&self, publisher: MediaClockPublisher) {
        self.set_media_clock(publisher).await;
    }
    async fn detach_media_clock(&self) {
        self.detach_media_clock().await;
    }
}

#[async_trait::async_trait]
impl MediaClockAttach for VideoTrack {
    async fn set_media_clock(&self, publisher: MediaClockPublisher) {
        // `VideoTrack::set_media_clock` is sync; lift into async for
        // the trait. No actual await — instant return.
        VideoTrack::set_media_clock(self, publisher);
    }
    async fn detach_media_clock(&self) {
        VideoTrack::detach_media_clock(self);
    }
}

// Re-export track registry types for external use
pub use track_registry::{
    TrackHandle, TrackInfo, TrackRegistry, DEFAULT_STREAM_ID, MAX_AUDIO_TRACKS_PER_PEER,
    MAX_VIDEO_TRACKS_PER_PEER,
};

// Re-export frame router types
pub use frame_router::{extract_stream_id, with_stream_id, FrameRouter};

// =============================================================================
// Stream ID Validation (Spec 013: Dynamic Multi-Track Streaming)
// =============================================================================

/// Maximum length for stream identifiers (FR-005)
pub const STREAM_ID_MAX_LENGTH: usize = 64;

/// Valid characters for stream identifiers: alphanumeric, hyphens, underscores (FR-003)
/// Pattern: ^[a-zA-Z0-9_-]+$
const STREAM_ID_VALID_CHARS: &str =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";

/// Error type for stream ID validation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamIdError {
    /// Stream ID is empty
    Empty,
    /// Stream ID exceeds maximum length
    TooLong { length: usize, max: usize },
    /// Stream ID contains invalid characters
    InvalidCharacters { invalid_char: char, position: usize },
}

impl std::fmt::Display for StreamIdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamIdError::Empty => write!(f, "stream_id cannot be empty"),
            StreamIdError::TooLong { length, max } => {
                write!(f, "stream_id too long: {} chars (max {})", length, max)
            }
            StreamIdError::InvalidCharacters {
                invalid_char,
                position,
            } => {
                write!(
                    f,
                    "stream_id contains invalid character '{}' at position {}",
                    invalid_char, position
                )
            }
        }
    }
}

impl std::error::Error for StreamIdError {}

/// Validate a stream identifier string (FR-003, FR-004, FR-005)
///
/// Stream identifiers must:
/// - Not be empty
/// - Be at most 64 characters (STREAM_ID_MAX_LENGTH)
/// - Contain only alphanumeric characters, hyphens, and underscores
///
/// # Arguments
///
/// * `stream_id` - The stream identifier to validate
///
/// # Returns
///
/// * `Ok(())` if the stream_id is valid
/// * `Err(StreamIdError)` describing the validation failure
///
/// # Examples
///
/// ```
/// use remotemedia_webrtc::media::validate_stream_id;
///
/// assert!(validate_stream_id("camera").is_ok());
/// assert!(validate_stream_id("screen_share").is_ok());
/// assert!(validate_stream_id("audio-main").is_ok());
/// assert!(validate_stream_id("track_123").is_ok());
///
/// // Invalid examples
/// assert!(validate_stream_id("").is_err());  // Empty
/// assert!(validate_stream_id("has space").is_err());  // Contains space
/// assert!(validate_stream_id("has.dot").is_err());  // Contains dot
/// ```
pub fn validate_stream_id(stream_id: &str) -> Result<(), StreamIdError> {
    // Check empty
    if stream_id.is_empty() {
        return Err(StreamIdError::Empty);
    }

    // Check length (FR-005)
    if stream_id.len() > STREAM_ID_MAX_LENGTH {
        return Err(StreamIdError::TooLong {
            length: stream_id.len(),
            max: STREAM_ID_MAX_LENGTH,
        });
    }

    // Check valid characters (FR-003, FR-004)
    for (pos, c) in stream_id.chars().enumerate() {
        if !STREAM_ID_VALID_CHARS.contains(c) {
            return Err(StreamIdError::InvalidCharacters {
                invalid_char: c,
                position: pos,
            });
        }
    }

    Ok(())
}

/// Check if a character is valid in a stream identifier
#[inline]
pub fn is_valid_stream_id_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == '-'
}

/// Generate a deterministic track ID from a stream_id (FR-020)
///
/// This creates a reproducible track ID that can be used in SDP.
/// The format is: `{media_type}_{stream_id}` (e.g., "video_camera", "audio_voice")
///
/// # Arguments
///
/// * `media_type` - Either "audio" or "video"
/// * `stream_id` - The stream identifier
///
/// # Returns
///
/// A deterministic track ID string
pub fn generate_track_id(media_type: &str, stream_id: &str) -> String {
    format!("{}_{}", media_type, stream_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_stream_id_valid() {
        // Valid stream IDs
        assert!(validate_stream_id("camera").is_ok());
        assert!(validate_stream_id("screen").is_ok());
        assert!(validate_stream_id("audio_main").is_ok());
        assert!(validate_stream_id("video-1").is_ok());
        assert!(validate_stream_id("Track_123").is_ok());
        assert!(validate_stream_id("a").is_ok()); // Single char
        assert!(validate_stream_id("UPPERCASE").is_ok());
        assert!(validate_stream_id("MixedCase123").is_ok());
        assert!(validate_stream_id("_underscore_start").is_ok());
        assert!(validate_stream_id("-hyphen-start").is_ok());
    }

    #[test]
    fn test_validate_stream_id_empty() {
        let result = validate_stream_id("");
        assert!(matches!(result, Err(StreamIdError::Empty)));
    }

    #[test]
    fn test_validate_stream_id_too_long() {
        let long_id = "a".repeat(65);
        let result = validate_stream_id(&long_id);
        assert!(matches!(
            result,
            Err(StreamIdError::TooLong {
                length: 65,
                max: 64
            })
        ));

        // Exactly 64 chars should be OK
        let max_id = "a".repeat(64);
        assert!(validate_stream_id(&max_id).is_ok());
    }

    #[test]
    fn test_validate_stream_id_invalid_chars() {
        // Space
        let result = validate_stream_id("has space");
        assert!(matches!(
            result,
            Err(StreamIdError::InvalidCharacters {
                invalid_char: ' ',
                position: 3
            })
        ));

        // Dot
        let result = validate_stream_id("has.dot");
        assert!(matches!(
            result,
            Err(StreamIdError::InvalidCharacters {
                invalid_char: '.',
                position: 3
            })
        ));

        // Special chars
        assert!(validate_stream_id("track@1").is_err());
        assert!(validate_stream_id("track#1").is_err());
        assert!(validate_stream_id("track$1").is_err());
        assert!(validate_stream_id("track/1").is_err());
        assert!(validate_stream_id("track\\1").is_err());
    }

    #[test]
    fn test_is_valid_stream_id_char() {
        // Valid chars
        assert!(is_valid_stream_id_char('a'));
        assert!(is_valid_stream_id_char('Z'));
        assert!(is_valid_stream_id_char('0'));
        assert!(is_valid_stream_id_char('9'));
        assert!(is_valid_stream_id_char('_'));
        assert!(is_valid_stream_id_char('-'));

        // Invalid chars
        assert!(!is_valid_stream_id_char(' '));
        assert!(!is_valid_stream_id_char('.'));
        assert!(!is_valid_stream_id_char('@'));
        assert!(!is_valid_stream_id_char('/'));
    }

    #[test]
    fn test_generate_track_id() {
        assert_eq!(generate_track_id("video", "camera"), "video_camera");
        assert_eq!(generate_track_id("audio", "voice"), "audio_voice");
        assert_eq!(
            generate_track_id("video", "screen_share"),
            "video_screen_share"
        );
    }
}
