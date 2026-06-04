//! Error types for the telephony transport.

/// Result type for telephony transport operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur in telephony transport operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid configuration parameter.
    #[error("invalid telephony configuration: {0}")]
    InvalidConfig(String),

    /// SIP signaling failed.
    #[error("SIP error: {0}")]
    Sip(String),

    /// SDP negotiation failed.
    #[error("SDP error: {0}")]
    Sdp(String),

    /// RTP media handling failed.
    #[error("RTP error: {0}")]
    Rtp(String),

    /// Codec conversion failed.
    #[error("codec error: {0}")]
    Codec(String),

    /// Call/session lifecycle error.
    #[error("session error: {0}")]
    Session(String),

    /// Pipeline integration failed.
    #[error("pipeline error: {0}")]
    Pipeline(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization or deserialization failed.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}
