//! Status types for ingestion sources
//!
//! This module defines the connection status enum and media type enum
//! used to track ingest source lifecycle and track types.

use serde::{Deserialize, Serialize};

/// Connection status of an ingest source
///
/// Represents the lifecycle state of an ingest connection, from idle
/// through connected to terminated states.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestStatus {
    /// Source created but not started
    Idle,

    /// Attempting to connect to the source
    Connecting,

    /// Successfully connected and receiving data
    Connected,

    /// Connection lost, attempting to reconnect
    Reconnecting {
        /// Current reconnection attempt number (1-based)
        attempt: u32,
        /// Maximum attempts configured (0 = unlimited)
        max_attempts: u32,
    },

    /// Intentionally disconnected (via stop())
    Disconnected,

    /// Error state - cannot continue without intervention
    Error {
        /// Error message describing the failure
        message: String,
    },
}

impl IngestStatus {
    /// Check if the source is actively receiving data
    pub fn is_connected(&self) -> bool {
        matches!(self, IngestStatus::Connected)
    }

    /// Check if the source is in a terminal state (disconnected or error)
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            IngestStatus::Disconnected | IngestStatus::Error { .. }
        )
    }

    /// Check if the source is attempting to connect
    pub fn is_connecting(&self) -> bool {
        matches!(
            self,
            IngestStatus::Connecting | IngestStatus::Reconnecting { .. }
        )
    }

    /// Get error message if in error state
    pub fn error_message(&self) -> Option<&str> {
        match self {
            IngestStatus::Error { message } => Some(message),
            _ => None,
        }
    }

    /// Create an error status
    pub fn error(message: impl Into<String>) -> Self {
        IngestStatus::Error {
            message: message.into(),
        }
    }
}

impl Default for IngestStatus {
    fn default() -> Self {
        IngestStatus::Idle
    }
}

impl std::fmt::Display for IngestStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestStatus::Idle => write!(f, "idle"),
            IngestStatus::Connecting => write!(f, "connecting"),
            IngestStatus::Connected => write!(f, "connected"),
            IngestStatus::Reconnecting {
                attempt,
                max_attempts,
            } => {
                if *max_attempts == 0 {
                    write!(f, "reconnecting (attempt {})", attempt)
                } else {
                    write!(f, "reconnecting (attempt {}/{})", attempt, max_attempts)
                }
            }
            IngestStatus::Disconnected => write!(f, "disconnected"),
            IngestStatus::Error { message } => write!(f, "error: {}", message),
        }
    }
}

/// Type of media track
///
/// Used to identify and select tracks from multi-track sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaType {
    /// Audio track (PCM, AAC, Opus, etc.)
    Audio,

    /// Video track (H.264, VP8, AV1, etc.)
    Video,

    /// Subtitle/caption track (SRT, WebVTT, etc.)
    Subtitle,

    /// Data track (metadata, timecode, etc.)
    Data,
}

impl MediaType {
    /// Get the stream ID prefix for this media type
    ///
    /// Used to construct stream_id values like "audio:0", "video:1"
    pub fn stream_id_prefix(&self) -> &'static str {
        match self {
            MediaType::Audio => "audio",
            MediaType::Video => "video",
            MediaType::Subtitle => "subtitle",
            MediaType::Data => "data",
        }
    }

    /// Create a stream ID for this media type and track index
    ///
    /// # Example
    /// ```
    /// use remotemedia_core::ingestion::MediaType;
    ///
    /// let stream_id = MediaType::Audio.stream_id(0);
    /// assert_eq!(stream_id, "audio:0");
    ///
    /// let stream_id = MediaType::Video.stream_id(2);
    /// assert_eq!(stream_id, "video:2");
    /// ```
    pub fn stream_id(&self, index: u32) -> String {
        format!("{}:{}", self.stream_id_prefix(), index)
    }
}

impl std::fmt::Display for MediaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.stream_id_prefix())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ingest_status_transitions() {
        let status = IngestStatus::Idle;
        assert!(!status.is_connected());
        assert!(!status.is_terminal());
        assert!(!status.is_connecting());

        let status = IngestStatus::Connecting;
        assert!(!status.is_connected());
        assert!(!status.is_terminal());
        assert!(status.is_connecting());

        let status = IngestStatus::Connected;
        assert!(status.is_connected());
        assert!(!status.is_terminal());
        assert!(!status.is_connecting());

        let status = IngestStatus::Reconnecting {
            attempt: 1,
            max_attempts: 5,
        };
        assert!(!status.is_connected());
        assert!(!status.is_terminal());
        assert!(status.is_connecting());

        let status = IngestStatus::Disconnected;
        assert!(!status.is_connected());
        assert!(status.is_terminal());
        assert!(!status.is_connecting());

        let status = IngestStatus::error("test error");
        assert!(!status.is_connected());
        assert!(status.is_terminal());
        assert!(!status.is_connecting());
        assert_eq!(status.error_message(), Some("test error"));
    }

    #[test]
    fn test_ingest_status_display() {
        assert_eq!(format!("{}", IngestStatus::Idle), "idle");
        assert_eq!(format!("{}", IngestStatus::Connecting), "connecting");
        assert_eq!(format!("{}", IngestStatus::Connected), "connected");
        assert_eq!(
            format!(
                "{}",
                IngestStatus::Reconnecting {
                    attempt: 2,
                    max_attempts: 5
                }
            ),
            "reconnecting (attempt 2/5)"
        );
        assert_eq!(
            format!(
                "{}",
                IngestStatus::Reconnecting {
                    attempt: 3,
                    max_attempts: 0
                }
            ),
            "reconnecting (attempt 3)"
        );
        assert_eq!(format!("{}", IngestStatus::Disconnected), "disconnected");
        assert_eq!(
            format!("{}", IngestStatus::error("connection refused")),
            "error: connection refused"
        );
    }

    #[test]
    fn test_media_type_stream_id() {
        assert_eq!(MediaType::Audio.stream_id(0), "audio:0");
        assert_eq!(MediaType::Audio.stream_id(1), "audio:1");
        assert_eq!(MediaType::Video.stream_id(0), "video:0");
        assert_eq!(MediaType::Subtitle.stream_id(2), "subtitle:2");
        assert_eq!(MediaType::Data.stream_id(0), "data:0");
    }

    #[test]
    fn test_media_type_display() {
        assert_eq!(format!("{}", MediaType::Audio), "audio");
        assert_eq!(format!("{}", MediaType::Video), "video");
        assert_eq!(format!("{}", MediaType::Subtitle), "subtitle");
        assert_eq!(format!("{}", MediaType::Data), "data");
    }

    #[test]
    fn test_status_serialization() {
        let status = IngestStatus::Reconnecting {
            attempt: 3,
            max_attempts: 5,
        };
        let json = serde_json::to_string(&status).unwrap();
        let deserialized: IngestStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(status, deserialized);
    }
}
