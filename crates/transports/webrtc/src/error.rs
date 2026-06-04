//! Error types for WebRTC transport

/// Result type alias using WebRTC Error
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in WebRTC transport operations
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid configuration parameter
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Signaling connection error
    #[error("Signaling error: {0}")]
    SignalingError(String),

    /// Peer not found
    #[error("Peer not found: {0}")]
    PeerNotFound(String),

    /// NAT traversal failed (ICE connection failure)
    #[error("NAT traversal failed: {0}")]
    NatTraversalFailed(String),

    /// Media encoding/decoding error
    #[error("Encoding error: {0}")]
    EncodingError(String),

    /// Session not found
    #[error("Session not found: {0}")]
    SessionNotFound(String),

    /// Session management error
    #[error("Session error: {0}")]
    SessionError(String),

    /// Invalid data format
    #[error("Invalid data: {0}")]
    InvalidData(String),

    /// Operation timeout
    #[error("Operation timeout: {0}")]
    OperationTimeout(String),

    /// WebRTC peer connection error
    #[error("Peer connection error: {0}")]
    PeerConnectionError(String),

    /// ICE candidate error
    #[error("ICE candidate error: {0}")]
    IceCandidateError(String),

    /// SDP negotiation error
    #[error("SDP negotiation error: {0}")]
    SdpError(String),

    /// Data channel error
    #[error("Data channel error: {0}")]
    DataChannelError(String),

    /// Media track error
    #[error("Media track error: {0}")]
    MediaTrackError(String),

    /// Synchronization error (audio/video sync)
    #[error("Synchronization error: {0}")]
    SyncError(String),

    /// Pipeline integration error
    #[error("Pipeline error: {0}")]
    PipelineError(String),

    /// Invalid stream_id (Spec 013: Dynamic Multi-Track Streaming)
    #[error("Invalid stream_id: {0}")]
    InvalidStreamId(String),

    /// Track limit exceeded (Spec 013: FR-018, FR-019)
    #[error("Track limit exceeded: {0}")]
    TrackLimitExceeded(String),

    /// Track not found (Spec 013)
    #[error("Track not found: {0}")]
    TrackNotFound(String),

    /// WebSocket error
    #[error("WebSocket error: {0}")]
    WebSocketError(String),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Internal error (should not occur in normal operation)
    #[error("Internal error: {0}")]
    InternalError(String),

    /// WebRTC library error
    #[error("WebRTC error: {0}")]
    WebRtcError(String),

    /// I/O error
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Any other error
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Error {
    /// Check if this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Error::SignalingError(_)
                | Error::NatTraversalFailed(_)
                | Error::OperationTimeout(_)
                | Error::WebSocketError(_)
                | Error::IoError(_)
        )
    }

    /// Check if this error is a configuration error
    pub fn is_config_error(&self) -> bool {
        matches!(self, Error::InvalidConfig(_))
    }

    /// Check if this error is a peer-related error
    pub fn is_peer_error(&self) -> bool {
        matches!(
            self,
            Error::PeerNotFound(_)
                | Error::PeerConnectionError(_)
                | Error::IceCandidateError(_)
                | Error::SdpError(_)
        )
    }

    /// Get a suggested recovery action for this error
    ///
    /// Returns a human-readable suggestion for how to recover from or
    /// prevent this error.
    ///
    /// # Example
    ///
    /// ```
    /// use remotemedia_webrtc::Error;
    ///
    /// let err = Error::NatTraversalFailed("ICE timeout".to_string());
    /// println!("Error: {}", err);
    /// println!("Suggestion: {}", err.recovery_suggestion());
    /// ```
    pub fn recovery_suggestion(&self) -> &'static str {
        match self {
            Error::InvalidConfig(_) => {
                "Check your configuration parameters. Use WebRtcTransportConfig::validate() \
                 to catch configuration errors early. Consider using a preset like \
                 WebRtcTransportConfig::low_latency_preset() for common use cases."
            }
            Error::SignalingError(_) => {
                "Ensure the signaling server is running and accessible. Check the \
                 signaling_url in your configuration. Verify network connectivity \
                 and firewall rules allow WebSocket connections."
            }
            Error::PeerNotFound(_) => {
                "The peer may have disconnected or never connected. Use \
                 transport.list_peers() to see available peers before attempting \
                 operations."
            }
            Error::NatTraversalFailed(_) => {
                "ICE connection failed. This usually indicates NAT/firewall issues. \
                 Try these solutions:\n\
                 1. Configure TURN servers for relay fallback\n\
                 2. Verify STUN server is accessible\n\
                 3. Check firewall allows UDP traffic on ports 49152-65535\n\
                 4. Use WebRtcTransportConfig::mobile_network_preset() with TURN servers"
            }
            Error::EncodingError(_) => {
                "Media encoding/decoding failed. Ensure the 'codecs' feature is enabled \
                 and codec libraries are installed. Check that input data format matches \
                 expected format (e.g., f32 samples for audio, I420/NV12 for video)."
            }
            Error::SessionNotFound(_) => {
                "The session may have been removed or never created. Use \
                 transport.has_session(id) to check if a session exists before \
                 accessing it."
            }
            Error::SessionError(_) => {
                "Session operation failed. Check that peers are properly associated \
                 with the session using add_peer_to_session(). Verify the session \
                 state using get_session()."
            }
            Error::InvalidData(_) => {
                "Data format is invalid. Check that:\n\
                 1. Audio samples are f32 arrays at the expected sample rate\n\
                 2. Video frames use a supported pixel format (I420, NV12)\n\
                 3. Data channel messages are within size limits (16 MB max)"
            }
            Error::OperationTimeout(_) => {
                "Operation timed out. This may indicate network issues or a busy \
                 server. Try increasing timeout values or check network connectivity. \
                 For ICE timeouts, consider adding more STUN/TURN servers."
            }
            Error::PeerConnectionError(_) => {
                "WebRTC peer connection failed. Check that:\n\
                 1. Both peers have compatible codecs enabled\n\
                 2. ICE candidates are being exchanged properly\n\
                 3. Network allows peer-to-peer connections\n\
                 Consider using peer.reconnect() to re-establish the connection."
            }
            Error::IceCandidateError(_) => {
                "ICE candidate processing failed. Verify that:\n\
                 1. STUN servers are accessible\n\
                 2. Candidate format is valid\n\
                 3. Candidates are being exchanged via signaling\n\
                 Try collecting more candidates by adding multiple STUN servers."
            }
            Error::SdpError(_) => {
                "SDP negotiation failed. This may indicate:\n\
                 1. Incompatible codec support between peers\n\
                 2. Invalid SDP format from signaling\n\
                 3. Mismatched offer/answer sequence\n\
                 Ensure both peers are using compatible WebRTC configurations."
            }
            Error::DataChannelError(_) => {
                "Data channel operation failed. Check that:\n\
                 1. Data channels are enabled in configuration\n\
                 2. The peer connection is in 'connected' state\n\
                 3. Message size is within limits (16 MB max)\n\
                 For reliability, use DataChannelMode::Reliable."
            }
            Error::MediaTrackError(_) => {
                "Media track operation failed. Verify that:\n\
                 1. The track type (audio/video) is supported\n\
                 2. Codec is enabled for the track type\n\
                 3. Track hasn't been removed from the peer connection"
            }
            Error::SyncError(_) => {
                "Audio/video synchronization failed. Check that:\n\
                 1. RTCP sender reports are being received\n\
                 2. Jitter buffer size is appropriate for network conditions\n\
                 3. Both audio and video tracks are active\n\
                 Consider increasing jitter_buffer_size_ms for unstable networks."
            }
            Error::PipelineError(_) => {
                "Pipeline integration failed. Verify that:\n\
                 1. Pipeline manifest is valid YAML/JSON\n\
                 2. All referenced node types exist\n\
                 3. Node connections form a valid graph\n\
                 Use manifest.validate() to check pipeline before execution."
            }
            Error::InvalidStreamId(_) => {
                "Invalid stream_id format. Stream IDs must:\n\
                 1. Be non-empty and max 64 characters\n\
                 2. Contain only alphanumeric, underscore, and hyphen characters\n\
                 3. Start with a letter or underscore\n\
                 Use validate_stream_id() to check IDs before use."
            }
            Error::TrackLimitExceeded(_) => {
                "Maximum number of tracks exceeded. Current limits:\n\
                 1. Max 8 video tracks per peer\n\
                 2. Max 8 audio tracks per peer\n\
                 3. Max 16 total tracks per peer\n\
                 Consider removing unused tracks before adding new ones."
            }
            Error::TrackNotFound(_) => {
                "The requested track was not found. Verify that:\n\
                 1. The stream_id matches an existing track\n\
                 2. The track hasn't been removed\n\
                 3. Use get_video_streams() or get_audio_streams() to list available tracks."
            }
            Error::WebSocketError(_) => {
                "WebSocket connection failed. Check that:\n\
                 1. Signaling server URL is correct (ws:// or wss://)\n\
                 2. Server is running and accepting connections\n\
                 3. Network/firewall allows WebSocket traffic"
            }
            Error::SerializationError(_) => {
                "Data serialization failed. Ensure that:\n\
                 1. Data types implement Serialize/Deserialize\n\
                 2. JSON/binary data is properly formatted\n\
                 3. No circular references in data structures"
            }
            Error::InternalError(_) => {
                "An internal error occurred. This is likely a bug. Please report this \
                 issue with full error details and reproduction steps."
            }
            Error::WebRtcError(_) => {
                "WebRTC library error. Check that:\n\
                 1. All WebRTC dependencies are correctly installed\n\
                 2. Platform-specific requirements are met\n\
                 3. No conflicting WebRTC instances are running"
            }
            Error::IoError(_) => {
                "I/O error occurred. Check file permissions, disk space, and that \
                 required files/directories exist."
            }
            Error::Other(_) => {
                "An unexpected error occurred. Check the error details for more \
                 information and consider filing a bug report if the issue persists."
            }
        }
    }

    /// Get a brief error code for logging and metrics
    pub fn error_code(&self) -> &'static str {
        match self {
            Error::InvalidConfig(_) => "INVALID_CONFIG",
            Error::SignalingError(_) => "SIGNALING_ERROR",
            Error::PeerNotFound(_) => "PEER_NOT_FOUND",
            Error::NatTraversalFailed(_) => "NAT_TRAVERSAL_FAILED",
            Error::EncodingError(_) => "ENCODING_ERROR",
            Error::SessionNotFound(_) => "SESSION_NOT_FOUND",
            Error::SessionError(_) => "SESSION_ERROR",
            Error::InvalidData(_) => "INVALID_DATA",
            Error::OperationTimeout(_) => "OPERATION_TIMEOUT",
            Error::PeerConnectionError(_) => "PEER_CONNECTION_ERROR",
            Error::IceCandidateError(_) => "ICE_CANDIDATE_ERROR",
            Error::SdpError(_) => "SDP_ERROR",
            Error::DataChannelError(_) => "DATA_CHANNEL_ERROR",
            Error::MediaTrackError(_) => "MEDIA_TRACK_ERROR",
            Error::SyncError(_) => "SYNC_ERROR",
            Error::PipelineError(_) => "PIPELINE_ERROR",
            Error::InvalidStreamId(_) => "INVALID_STREAM_ID",
            Error::TrackLimitExceeded(_) => "TRACK_LIMIT_EXCEEDED",
            Error::TrackNotFound(_) => "TRACK_NOT_FOUND",
            Error::WebSocketError(_) => "WEBSOCKET_ERROR",
            Error::SerializationError(_) => "SERIALIZATION_ERROR",
            Error::InternalError(_) => "INTERNAL_ERROR",
            Error::WebRtcError(_) => "WEBRTC_ERROR",
            Error::IoError(_) => "IO_ERROR",
            Error::Other(_) => "OTHER_ERROR",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::InvalidConfig("test".to_string());
        assert_eq!(err.to_string(), "Invalid configuration: test");
    }

    #[test]
    fn test_error_is_retryable() {
        assert!(Error::SignalingError("test".to_string()).is_retryable());
        assert!(Error::OperationTimeout("test".to_string()).is_retryable());
        assert!(!Error::InvalidConfig("test".to_string()).is_retryable());
    }

    #[test]
    fn test_error_is_config_error() {
        assert!(Error::InvalidConfig("test".to_string()).is_config_error());
        assert!(!Error::SignalingError("test".to_string()).is_config_error());
    }

    #[test]
    fn test_error_is_peer_error() {
        assert!(Error::PeerNotFound("test".to_string()).is_peer_error());
        assert!(Error::PeerConnectionError("test".to_string()).is_peer_error());
        assert!(!Error::InvalidConfig("test".to_string()).is_peer_error());
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = Error::from(io_err);
        assert!(matches!(err, Error::IoError(_)));
    }

    #[test]
    fn test_recovery_suggestion_nat_traversal() {
        let err = Error::NatTraversalFailed("ICE timeout".to_string());
        let suggestion = err.recovery_suggestion();
        assert!(suggestion.contains("TURN servers"));
        assert!(suggestion.contains("NAT"));
    }

    #[test]
    fn test_recovery_suggestion_config() {
        let err = Error::InvalidConfig("bad value".to_string());
        let suggestion = err.recovery_suggestion();
        assert!(suggestion.contains("validate()"));
        assert!(suggestion.contains("preset"));
    }

    #[test]
    fn test_recovery_suggestion_signaling() {
        let err = Error::SignalingError("connection refused".to_string());
        let suggestion = err.recovery_suggestion();
        assert!(suggestion.contains("signaling server"));
        assert!(suggestion.contains("WebSocket"));
    }

    #[test]
    fn test_error_code() {
        assert_eq!(
            Error::InvalidConfig("test".to_string()).error_code(),
            "INVALID_CONFIG"
        );
        assert_eq!(
            Error::NatTraversalFailed("test".to_string()).error_code(),
            "NAT_TRAVERSAL_FAILED"
        );
        assert_eq!(
            Error::DataChannelError("test".to_string()).error_code(),
            "DATA_CHANNEL_ERROR"
        );
    }

    #[test]
    fn test_all_errors_have_recovery_suggestions() {
        // Ensure all error variants have non-empty recovery suggestions
        let errors = vec![
            Error::InvalidConfig("test".to_string()),
            Error::SignalingError("test".to_string()),
            Error::PeerNotFound("test".to_string()),
            Error::NatTraversalFailed("test".to_string()),
            Error::EncodingError("test".to_string()),
            Error::SessionNotFound("test".to_string()),
            Error::SessionError("test".to_string()),
            Error::InvalidData("test".to_string()),
            Error::OperationTimeout("test".to_string()),
            Error::PeerConnectionError("test".to_string()),
            Error::IceCandidateError("test".to_string()),
            Error::SdpError("test".to_string()),
            Error::DataChannelError("test".to_string()),
            Error::MediaTrackError("test".to_string()),
            Error::SyncError("test".to_string()),
            Error::PipelineError("test".to_string()),
            Error::InvalidStreamId("test".to_string()),
            Error::TrackLimitExceeded("test".to_string()),
            Error::TrackNotFound("test".to_string()),
            Error::WebSocketError("test".to_string()),
            Error::SerializationError("test".to_string()),
            Error::InternalError("test".to_string()),
            Error::WebRtcError("test".to_string()),
        ];

        for err in errors {
            let suggestion = err.recovery_suggestion();
            assert!(
                !suggestion.is_empty(),
                "Error {:?} has empty recovery suggestion",
                err
            );
            let code = err.error_code();
            assert!(!code.is_empty(), "Error {:?} has empty error code", err);
        }
    }
}
