//! WebRTC error types for FFI
//!
//! Provides a unified error type that can be converted to language-specific
//! exceptions/errors in both Node.js and Python.

use super::config::ConfigValidationError;
use super::events::ErrorCode;
use thiserror::Error;

/// WebRTC error type for FFI layer
#[derive(Debug, Error)]
pub enum WebRtcError {
    /// Configuration validation failed
    #[error("Configuration error: {0}")]
    Config(#[from] ConfigValidationError),

    /// Signaling connection or protocol error
    #[error("Signaling error: {0}")]
    Signaling(String),

    /// Peer connection or communication error
    #[error("Peer error: {0}")]
    Peer(String),

    /// Pipeline execution error
    #[error("Pipeline error: {0}")]
    Pipeline(String),

    /// Maximum peer limit reached
    #[error("Maximum peers reached: {0}")]
    MaxPeersReached(u32),

    /// Session not found
    #[error("Session not found: {0}")]
    SessionNotFound(String),

    /// Peer not found
    #[error("Peer not found: {0}")]
    PeerNotFound(String),

    /// Server state error (e.g., trying to start when already running)
    #[error("Invalid server state: {0}")]
    InvalidState(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON serialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

impl WebRtcError {
    /// Get the error code for this error
    pub fn code(&self) -> ErrorCode {
        match self {
            WebRtcError::Config(_) => ErrorCode::ConfigError,
            WebRtcError::Signaling(_) => ErrorCode::SignalingError,
            WebRtcError::Peer(_) => ErrorCode::PeerError,
            WebRtcError::Pipeline(_) => ErrorCode::PipelineError,
            WebRtcError::MaxPeersReached(_) => ErrorCode::MaxPeersReached,
            WebRtcError::SessionNotFound(_) => ErrorCode::SessionNotFound,
            WebRtcError::PeerNotFound(_) => ErrorCode::PeerNotFound,
            WebRtcError::InvalidState(_) => ErrorCode::InternalError,
            WebRtcError::Internal(_) => ErrorCode::InternalError,
            WebRtcError::Io(_) => ErrorCode::InternalError,
            WebRtcError::Json(_) => ErrorCode::ConfigError,
        }
    }

    /// Create a signaling error
    pub fn signaling(msg: impl Into<String>) -> Self {
        WebRtcError::Signaling(msg.into())
    }

    /// Create a peer error
    pub fn peer(msg: impl Into<String>) -> Self {
        WebRtcError::Peer(msg.into())
    }

    /// Create a pipeline error
    pub fn pipeline(msg: impl Into<String>) -> Self {
        WebRtcError::Pipeline(msg.into())
    }

    /// Create an internal error
    pub fn internal(msg: impl Into<String>) -> Self {
        WebRtcError::Internal(msg.into())
    }

    /// Create an invalid state error
    pub fn invalid_state(msg: impl Into<String>) -> Self {
        WebRtcError::InvalidState(msg.into())
    }

    /// Create a configuration error (wraps as Internal since Config requires ConfigValidationError)
    pub fn config(msg: impl Into<String>) -> Self {
        WebRtcError::Internal(format!("Configuration: {}", msg.into()))
    }
}

/// Result type for WebRTC operations
pub type WebRtcResult<T> = Result<T, WebRtcError>;

#[cfg(feature = "napi")]
impl From<WebRtcError> for napi::Error {
    fn from(err: WebRtcError) -> Self {
        napi::Error::from_reason(err.to_string())
    }
}

#[cfg(feature = "python")]
impl From<WebRtcError> for pyo3::PyErr {
    fn from(err: WebRtcError) -> Self {
        use pyo3::exceptions::PyRuntimeError;
        PyRuntimeError::new_err(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_codes() {
        assert_eq!(
            WebRtcError::signaling("test").code(),
            ErrorCode::SignalingError
        );
        assert_eq!(WebRtcError::peer("test").code(), ErrorCode::PeerError);
        assert_eq!(
            WebRtcError::pipeline("test").code(),
            ErrorCode::PipelineError
        );
        assert_eq!(
            WebRtcError::MaxPeersReached(10).code(),
            ErrorCode::MaxPeersReached
        );
        assert_eq!(
            WebRtcError::SessionNotFound("test".to_string()).code(),
            ErrorCode::SessionNotFound
        );
        assert_eq!(
            WebRtcError::PeerNotFound("test".to_string()).code(),
            ErrorCode::PeerNotFound
        );
    }

    #[test]
    fn test_error_display() {
        let err = WebRtcError::signaling("Connection refused");
        assert_eq!(err.to_string(), "Signaling error: Connection refused");
    }

    #[test]
    fn test_config_error_conversion() {
        let config_err = ConfigValidationError::NoStunServers;
        let webrtc_err = WebRtcError::from(config_err);
        assert_eq!(webrtc_err.code(), ErrorCode::ConfigError);
    }
}
