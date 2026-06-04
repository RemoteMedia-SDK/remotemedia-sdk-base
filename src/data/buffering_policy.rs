//! Buffering policy configuration
//!
//! Configuration for auto-buffering wrapper behavior, including batch size,
//! timeout, and merge strategies for combining multiple inputs.

use serde::{Deserialize, Serialize};

/// Configuration for auto-buffering wrapper behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferingPolicy {
    /// Minimum inputs to batch before processing
    pub min_batch_size: usize, // Default: 2-5

    /// Maximum wait time before processing (microseconds)
    pub max_wait_us: u64, // Default: 75000-150000 (75-150ms)

    /// Maximum buffer size (memory limit)
    pub max_buffer_size: usize, // Default: 100

    /// Strategy for merging buffered inputs
    pub merge_strategy: MergeStrategy,
}

/// Strategy for merging buffered inputs
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum MergeStrategy {
    /// Concatenate text with separator
    ConcatenateText { separator: String },

    /// Concatenate audio samples in time order
    ConcatenateAudio {
        ensure_continuity: bool, // Check sample rate/channels match
        max_gap_ms: u64,         // Max silence gap before separate segment
    },

    /// Keep inputs separate (no merging)
    KeepSeparate,

    /// Custom merge function (not serializable, runtime only)
    /// Note: This variant cannot be serialized, used for runtime configuration
    #[serde(skip)]
    Custom,
}

impl BufferingPolicy {
    /// Create default policy for text processing (TTS)
    pub fn text_default() -> Self {
        Self {
            min_batch_size: 3,
            max_wait_us: 100_000, // 100ms
            max_buffer_size: 50,
            merge_strategy: MergeStrategy::ConcatenateText {
                separator: " ".to_string(),
            },
        }
    }

    /// Create default policy for audio processing
    pub fn audio_default() -> Self {
        Self {
            min_batch_size: 1,   // No batching
            max_wait_us: 10_000, // 10ms
            max_buffer_size: 10,
            merge_strategy: MergeStrategy::KeepSeparate,
        }
    }

    /// Create policy that keeps inputs separate (no batching)
    pub fn no_batching() -> Self {
        Self {
            min_batch_size: 1,
            max_wait_us: 1_000, // 1ms
            max_buffer_size: 10,
            merge_strategy: MergeStrategy::KeepSeparate,
        }
    }

    /// Validate buffering policy
    pub fn validate(&self) -> Result<(), String> {
        if self.min_batch_size == 0 {
            return Err("min_batch_size must be >= 1".to_string());
        }

        if self.max_wait_us == 0 {
            return Err("max_wait_us must be > 0 (timeout required)".to_string());
        }

        if self.max_buffer_size < self.min_batch_size {
            return Err(format!(
                "max_buffer_size ({}) must be >= min_batch_size ({})",
                self.max_buffer_size, self.min_batch_size
            ));
        }

        // Validate merge strategy-specific constraints
        match &self.merge_strategy {
            MergeStrategy::ConcatenateAudio { max_gap_ms, .. } => {
                if *max_gap_ms > 100 {
                    return Err(format!(
                        "ConcatenateAudio: max_gap_ms ({}) should be <= 100ms",
                        max_gap_ms
                    ));
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Check if this policy actually enables batching
    pub fn is_batching_enabled(&self) -> bool {
        self.min_batch_size > 1 || !matches!(self.merge_strategy, MergeStrategy::KeepSeparate)
    }
}

impl Default for BufferingPolicy {
    fn default() -> Self {
        Self::no_batching()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_text_default_policy() {
        let policy = BufferingPolicy::text_default();

        assert_eq!(policy.min_batch_size, 3);
        assert_eq!(policy.max_wait_us, 100_000);
        assert!(policy.is_batching_enabled());

        match policy.merge_strategy {
            MergeStrategy::ConcatenateText { ref separator } => {
                assert_eq!(separator, " ");
            }
            _ => panic!("Expected ConcatenateText strategy"),
        }

        assert!(policy.validate().is_ok());
    }

    #[test]
    fn test_audio_default_policy() {
        let policy = BufferingPolicy::audio_default();

        assert_eq!(policy.min_batch_size, 1);
        assert!(!policy.is_batching_enabled());

        match policy.merge_strategy {
            MergeStrategy::KeepSeparate => {}
            _ => panic!("Expected KeepSeparate strategy"),
        }

        assert!(policy.validate().is_ok());
    }

    #[test]
    fn test_no_batching_policy() {
        let policy = BufferingPolicy::no_batching();

        assert_eq!(policy.min_batch_size, 1);
        assert!(!policy.is_batching_enabled());
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn test_policy_validation_success() {
        let policy = BufferingPolicy {
            min_batch_size: 3,
            max_wait_us: 100_000,
            max_buffer_size: 50,
            merge_strategy: MergeStrategy::ConcatenateText {
                separator: " ".to_string(),
            },
        };

        assert!(policy.validate().is_ok());
    }

    #[test]
    fn test_policy_validation_fails_zero_batch_size() {
        let policy = BufferingPolicy {
            min_batch_size: 0,
            max_wait_us: 100_000,
            max_buffer_size: 50,
            merge_strategy: MergeStrategy::KeepSeparate,
        };

        let result = policy.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be >= 1"));
    }

    #[test]
    fn test_policy_validation_fails_zero_timeout() {
        let policy = BufferingPolicy {
            min_batch_size: 3,
            max_wait_us: 0,
            max_buffer_size: 50,
            merge_strategy: MergeStrategy::KeepSeparate,
        };

        let result = policy.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be > 0"));
    }

    #[test]
    fn test_policy_validation_fails_buffer_smaller_than_batch() {
        let policy = BufferingPolicy {
            min_batch_size: 10,
            max_wait_us: 100_000,
            max_buffer_size: 5, // Smaller than min_batch_size
            merge_strategy: MergeStrategy::KeepSeparate,
        };

        let result = policy.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be >="));
    }

    #[test]
    fn test_audio_merge_strategy_validation() {
        let policy = BufferingPolicy {
            min_batch_size: 2,
            max_wait_us: 50_000,
            max_buffer_size: 10,
            merge_strategy: MergeStrategy::ConcatenateAudio {
                ensure_continuity: true,
                max_gap_ms: 50,
            },
        };

        assert!(policy.validate().is_ok());

        // Invalid max_gap_ms
        let invalid_policy = BufferingPolicy {
            merge_strategy: MergeStrategy::ConcatenateAudio {
                ensure_continuity: true,
                max_gap_ms: 200, // Too large
            },
            ..policy
        };

        let result = invalid_policy.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("max_gap_ms"));
    }

    #[test]
    fn test_is_batching_enabled() {
        // Batching enabled with min_batch_size > 1
        let policy1 = BufferingPolicy {
            min_batch_size: 3,
            max_wait_us: 100_000,
            max_buffer_size: 50,
            merge_strategy: MergeStrategy::KeepSeparate,
        };
        assert!(policy1.is_batching_enabled());

        // Batching enabled with merge strategy (even if min_batch_size=1)
        let policy2 = BufferingPolicy {
            min_batch_size: 1,
            max_wait_us: 100_000,
            max_buffer_size: 50,
            merge_strategy: MergeStrategy::ConcatenateText {
                separator: " ".to_string(),
            },
        };
        assert!(policy2.is_batching_enabled());

        // Batching disabled
        let policy3 = BufferingPolicy::no_batching();
        assert!(!policy3.is_batching_enabled());
    }

    #[test]
    fn test_merge_strategy_variants() {
        let strategies = vec![
            MergeStrategy::ConcatenateText {
                separator: "\n".to_string(),
            },
            MergeStrategy::ConcatenateAudio {
                ensure_continuity: true,
                max_gap_ms: 50,
            },
            MergeStrategy::KeepSeparate,
        ];

        for strategy in strategies {
            let policy = BufferingPolicy {
                min_batch_size: 2,
                max_wait_us: 100_000,
                max_buffer_size: 50,
                merge_strategy: strategy.clone(),
            };

            // Ensure policy can be created and validated
            assert!(policy.validate().is_ok());
        }
    }

    #[test]
    fn test_policy_serialization_roundtrip() {
        let original = BufferingPolicy::text_default();

        // Serialize to JSON
        let json = serde_json::to_string(&original).expect("Failed to serialize");

        // Deserialize back
        let deserialized: BufferingPolicy =
            serde_json::from_str(&json).expect("Failed to deserialize");

        assert_eq!(original.min_batch_size, deserialized.min_batch_size);
        assert_eq!(original.max_wait_us, deserialized.max_wait_us);
        assert_eq!(original.merge_strategy, deserialized.merge_strategy);
    }
}
