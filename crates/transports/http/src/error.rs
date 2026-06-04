//! HTTP transport error types

use thiserror::Error;

/// HTTP transport error types
#[derive(Debug, Error)]
pub enum Error {
    /// HTTP request failed
    #[error("HTTP request failed: {0}")]
    RequestFailed(String),

    /// SSE stream error
    #[error("SSE stream error: {0}")]
    StreamError(String),

    /// Connection error
    #[error("Connection error: {0}")]
    ConnectionError(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    /// HTTP client error
    #[error("HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),

    /// Server error
    #[error("Server error: {0}")]
    ServerError(String),

    /// Session not found
    #[error("Session not found: {0}")]
    SessionNotFound(String),

    /// Invalid session state
    #[error("Invalid session state: {0}")]
    InvalidSessionState(String),

    /// Timeout error
    #[error("Operation timed out: {0}")]
    Timeout(String),
}

/// Result type for HTTP transport operations
pub type Result<T> = std::result::Result<T, Error>;
