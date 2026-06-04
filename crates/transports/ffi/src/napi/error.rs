//! Error types for Node.js FFI bindings
//!
//! Provides error types that convert cleanly to JavaScript exceptions.

use napi::bindgen_prelude::*;
use thiserror::Error;

/// Errors that can occur in the napi FFI layer
#[derive(Debug, Error)]
pub enum IpcError {
    /// Failed to create or access iceoryx2 node
    #[error("IPC node error: {0}")]
    NodeError(String),

    /// Failed to create or access channel
    #[error("Channel error: {0}")]
    ChannelError(String),

    /// Failed to create publisher
    #[error("Publisher error: {0}")]
    PublisherError(String),

    /// Failed to create subscriber
    #[error("Subscriber error: {0}")]
    SubscriberError(String),

    /// Sample lifecycle error (already consumed, etc.)
    #[error("Sample error: {0}")]
    SampleError(String),

    /// Session management error
    #[error("Session error: {0}")]
    SessionError(String),

    /// Type mismatch between publisher and subscriber
    #[error("Incompatible types: {0}")]
    IncompatibleTypes(String),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Resource exhausted (loan pool, etc.)
    #[error("Resource exhausted: {0}")]
    ResourceExhausted(String),

    /// Invalid argument
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<IpcError> for napi::Error {
    fn from(err: IpcError) -> Self {
        napi::Error::from_reason(err.to_string())
    }
}

/// Result type for IPC operations
pub type IpcResult<T> = std::result::Result<T, IpcError>;

/// Convert iceoryx2 errors to IpcError
impl From<iceoryx2::node::NodeCreationFailure> for IpcError {
    fn from(err: iceoryx2::node::NodeCreationFailure) -> Self {
        IpcError::NodeError(format!("Failed to create iceoryx2 node: {:?}", err))
    }
}

/// Helper trait for converting Results to napi::Result
pub trait IntoNapiResult<T> {
    fn into_napi(self) -> napi::Result<T>;
}

impl<T> IntoNapiResult<T> for IpcResult<T> {
    fn into_napi(self) -> napi::Result<T> {
        self.map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = IpcError::ChannelError("test".to_string());
        assert_eq!(err.to_string(), "Channel error: test");
    }

    #[test]
    fn test_into_napi_error() {
        let err = IpcError::NodeError("failed".to_string());
        let napi_err: napi::Error = err.into();
        assert!(napi_err.reason.contains("IPC node error"));
    }
}
