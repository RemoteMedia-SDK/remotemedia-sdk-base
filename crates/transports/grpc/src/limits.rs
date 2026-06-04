//! Resource limit enforcement for gRPC service
//!
//! Implements memory cap, timeout enforcement, buffer size validation.
//! Prevents resource exhaustion and denial-of-service attacks.

use std::time::Duration;

/// Resource limit configuration
#[derive(Clone, Debug)]
pub struct ResourceLimits {
    /// Maximum memory allocation per execution (bytes)
    pub max_memory_bytes: u64,

    /// Maximum execution timeout
    pub max_timeout: Duration,

    /// Maximum audio buffer size (total samples across all channels)
    pub max_audio_samples: u64,

    /// Maximum concurrent executions
    pub max_concurrent_executions: usize,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: 100_000_000,       // 100 MB default
            max_timeout: Duration::from_secs(5), // 5 second default
            max_audio_samples: 10_000_000,       // ~200MB stereo F32
            max_concurrent_executions: 1000,
        }
    }
}

impl ResourceLimits {
    /// Create new resource limits with custom values
    pub fn new(
        max_memory_bytes: u64,
        max_timeout: Duration,
        max_audio_samples: u64,
        max_concurrent_executions: usize,
    ) -> Self {
        Self {
            max_memory_bytes,
            max_timeout,
            max_audio_samples,
            max_concurrent_executions,
        }
    }

    /// Validate memory usage against limit
    pub fn check_memory(&self, bytes: u64) -> Result<(), LimitError> {
        if bytes > self.max_memory_bytes {
            return Err(LimitError::MemoryExceeded {
                used: bytes,
                limit: self.max_memory_bytes,
            });
        }
        Ok(())
    }

    /// Validate timeout against limit
    pub fn check_timeout(&self, duration: Duration) -> Result<(), LimitError> {
        if duration > self.max_timeout {
            return Err(LimitError::TimeoutExceeded {
                requested: duration,
                limit: self.max_timeout,
            });
        }
        Ok(())
    }

    /// Validate audio buffer size against limit
    pub fn check_audio_samples(&self, samples: u64) -> Result<(), LimitError> {
        if samples > self.max_audio_samples {
            return Err(LimitError::AudioBufferTooLarge {
                samples,
                limit: self.max_audio_samples,
            });
        }
        Ok(())
    }

    /// Convert to protobuf ResourceLimits
    pub fn to_proto(&self) -> crate::ResourceLimits {
        crate::ResourceLimits {
            max_memory_bytes: self.max_memory_bytes,
            max_timeout_ms: self.max_timeout.as_millis() as u64,
            max_audio_samples: self.max_audio_samples,
        }
    }

    /// Create from protobuf ResourceLimits
    pub fn from_proto(proto: &crate::ResourceLimits) -> Self {
        Self {
            max_memory_bytes: proto.max_memory_bytes,
            max_timeout: Duration::from_millis(proto.max_timeout_ms),
            max_audio_samples: proto.max_audio_samples,
            max_concurrent_executions: 1000, // Not in proto, use default
        }
    }
}

/// Resource limit errors
#[derive(Debug, thiserror::Error)]
pub enum LimitError {
    #[error("Memory limit exceeded: used {used} bytes, limit {limit} bytes")]
    MemoryExceeded { used: u64, limit: u64 },

    #[error("Timeout exceeded: requested {requested:?}, limit {limit:?}")]
    TimeoutExceeded {
        requested: Duration,
        limit: Duration,
    },

    #[error("Audio buffer too large: {samples} samples, limit {limit} samples")]
    AudioBufferTooLarge { samples: u64, limit: u64 },

    #[error("Too many concurrent executions")]
    TooManyConcurrentExecutions,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_limits() {
        let limits = ResourceLimits::default();
        assert_eq!(limits.max_memory_bytes, 100_000_000);
        assert_eq!(limits.max_timeout, Duration::from_secs(5));
        assert_eq!(limits.max_audio_samples, 10_000_000);
    }

    #[test]
    fn test_memory_check_pass() {
        let limits = ResourceLimits::default();
        assert!(limits.check_memory(50_000_000).is_ok());
    }

    #[test]
    fn test_memory_check_fail() {
        let limits = ResourceLimits::default();
        let result = limits.check_memory(200_000_000);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            LimitError::MemoryExceeded { .. }
        ));
    }

    #[test]
    fn test_timeout_check_pass() {
        let limits = ResourceLimits::default();
        assert!(limits.check_timeout(Duration::from_secs(2)).is_ok());
    }

    #[test]
    fn test_timeout_check_fail() {
        let limits = ResourceLimits::default();
        let result = limits.check_timeout(Duration::from_secs(10));
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            LimitError::TimeoutExceeded { .. }
        ));
    }

    #[test]
    fn test_audio_samples_check_pass() {
        let limits = ResourceLimits::default();
        assert!(limits.check_audio_samples(5_000_000).is_ok());
    }

    #[test]
    fn test_audio_samples_check_fail() {
        let limits = ResourceLimits::default();
        let result = limits.check_audio_samples(20_000_000);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            LimitError::AudioBufferTooLarge { .. }
        ));
    }

    #[test]
    fn test_proto_conversion() {
        let limits = ResourceLimits::new(50_000_000, Duration::from_secs(3), 5_000_000, 500);

        let proto = limits.to_proto();
        assert_eq!(proto.max_memory_bytes, 50_000_000);
        assert_eq!(proto.max_timeout_ms, 3000);
        assert_eq!(proto.max_audio_samples, 5_000_000);

        let restored = ResourceLimits::from_proto(&proto);
        assert_eq!(restored.max_memory_bytes, 50_000_000);
        assert_eq!(restored.max_timeout, Duration::from_secs(3));
    }
}
