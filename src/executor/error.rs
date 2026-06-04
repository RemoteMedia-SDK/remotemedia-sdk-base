//! Execution error extensions
//!
//! Extends the root Error type with execution-specific helpers.

use crate::error::Error;

/// Extension methods for Error related to execution
pub trait ExecutionErrorExt {
    /// Check if error is retryable
    fn is_retryable(&self) -> bool;

    /// Create an execution error
    fn execution(msg: impl Into<String>) -> Error;

    /// Create a timeout error
    fn timeout(msg: impl Into<String>) -> Error;
}

impl ExecutionErrorExt for Error {
    fn is_retryable(&self) -> bool {
        match self {
            Error::Io(_) | Error::Transport(_) | Error::Wasm(_) => true,
            // Timeout errors are retryable (they contain "Timeout:" prefix)
            Error::Execution(msg) if msg.starts_with("Timeout:") => true,
            _ => false,
        }
    }

    fn execution(msg: impl Into<String>) -> Error {
        Error::Execution(msg.into())
    }

    fn timeout(msg: impl Into<String>) -> Error {
        Error::Execution(format!("Timeout: {}", msg.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retryable_errors() {
        let io_err = Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "test"));
        assert!(io_err.is_retryable());

        let manifest_err = Error::Manifest("test".into());
        assert!(!manifest_err.is_retryable());
    }
}
