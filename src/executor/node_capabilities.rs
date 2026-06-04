//! Node capabilities and execution characteristics
//!
//! Metadata describing a node's execution characteristics for runtime optimization.
//! Used by the executor to determine batching, queue management, and scheduling strategies.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Metadata describing a node's execution characteristics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCapabilities {
    /// Node type identifier (matches registry)
    pub node_type: String,

    /// Can this node process multiple inputs concurrently?
    pub parallelizable: bool,

    /// Does this node benefit from batched inputs?
    pub batch_aware: bool,

    /// Average processing time (microseconds), updated via EMA
    pub avg_processing_us: f64,

    /// Recommended queue capacity
    pub queue_capacity: usize,

    /// Overflow policy for bounded queue
    pub overflow_policy: OverflowPolicy,

    /// Does this node support control messages?
    pub supports_control_messages: bool,
}

/// Policy for handling queue overflow
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Drop oldest item when queue full (real-time audio)
    DropOldest,

    /// Reject new item, keep queue intact
    DropNewest,

    /// Block until space available (backpressure)
    Block,

    /// Apply merge strategy to reduce queue depth
    MergeOnOverflow,
}

impl NodeCapabilities {
    /// Create default capabilities for a node type
    pub fn new(node_type: impl Into<String>) -> Self {
        Self {
            node_type: node_type.into(),
            parallelizable: true,
            batch_aware: false,
            avg_processing_us: 1000.0, // Default 1ms
            queue_capacity: 50,
            overflow_policy: OverflowPolicy::DropOldest,
            supports_control_messages: false,
        }
    }

    /// Update average processing time using exponential moving average (EMA)
    ///
    /// Alpha = 0.1 gives more weight to recent measurements while maintaining stability
    pub fn update_avg_processing_us(&mut self, new_measurement_us: u64) {
        const ALPHA: f64 = 0.1;
        let new_value = new_measurement_us as f64;
        self.avg_processing_us = ALPHA * new_value + (1.0 - ALPHA) * self.avg_processing_us;
    }

    /// Check if this node should be auto-wrapped with BufferedProcessor
    pub fn should_auto_wrap(&self) -> bool {
        !self.parallelizable && self.batch_aware
    }

    /// Validate capabilities configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.queue_capacity == 0 {
            return Err("queue_capacity must be > 0".to_string());
        }

        if self.queue_capacity > 1000 {
            return Err(format!(
                "queue_capacity ({}) is unusually large (>1000)",
                self.queue_capacity
            ));
        }

        if self.avg_processing_us < 0.0 {
            return Err("avg_processing_us cannot be negative".to_string());
        }

        Ok(())
    }
}

/// Default capabilities by node type
impl Default for NodeCapabilities {
    fn default() -> Self {
        Self::new("unknown")
    }
}

/// Registry for node capabilities (singleton, thread-safe)
pub struct NodeCapabilitiesRegistry {
    capabilities: Arc<RwLock<HashMap<String, NodeCapabilities>>>,
}

impl NodeCapabilitiesRegistry {
    /// Create a new registry
    pub fn new() -> Self {
        let mut registry = Self {
            capabilities: Arc::new(RwLock::new(HashMap::new())),
        };

        // Register default capabilities for known node types
        registry.register_defaults();

        registry
    }

    /// Register default capabilities for well-known node types
    fn register_defaults(&mut self) {
        let defaults = vec![
            // Audio processing nodes (parallelizable, real-time)
            NodeCapabilities {
                node_type: "AudioResample".to_string(),
                parallelizable: true,
                batch_aware: false,
                avg_processing_us: 500.0,
                queue_capacity: 50,
                overflow_policy: OverflowPolicy::DropOldest,
                supports_control_messages: true,
            },
            // VAD nodes (parallelizable, real-time)
            NodeCapabilities {
                node_type: "SileroVAD".to_string(),
                parallelizable: true,
                batch_aware: false,
                avg_processing_us: 2000.0,
                queue_capacity: 50,
                overflow_policy: OverflowPolicy::DropOldest,
                supports_control_messages: true,
            },
            // Speculative VAD gate (parallelizable, supports control messages)
            NodeCapabilities {
                node_type: "SpeculativeVADGate".to_string(),
                parallelizable: true,
                batch_aware: false,
                avg_processing_us: 100.0,
                queue_capacity: 100,
                overflow_policy: OverflowPolicy::DropOldest,
                supports_control_messages: true,
            },
            // TTS nodes (NOT parallelizable, batch-aware)
            NodeCapabilities {
                node_type: "TTS".to_string(),
                parallelizable: false,
                batch_aware: true,
                avg_processing_us: 150000.0, // 150ms
                queue_capacity: 20,
                overflow_policy: OverflowPolicy::MergeOnOverflow,
                supports_control_messages: true,
            },
            // Text collector (parallelizable, batch-aware)
            NodeCapabilities {
                node_type: "TextCollector".to_string(),
                parallelizable: true,
                batch_aware: true,
                avg_processing_us: 50.0,
                queue_capacity: 50,
                overflow_policy: OverflowPolicy::Block,
                supports_control_messages: false,
            },
            // Buffered processor wrapper (NOT parallelizable, batch-aware)
            NodeCapabilities {
                node_type: "BufferedProcessor".to_string(),
                parallelizable: false,
                batch_aware: true,
                avg_processing_us: 1000.0,
                queue_capacity: 100,
                overflow_policy: OverflowPolicy::MergeOnOverflow,
                supports_control_messages: true,
            },
        ];

        for cap in defaults {
            self.register(cap.node_type.clone(), cap);
        }
    }

    /// Register capabilities for a node type
    pub fn register(&mut self, node_type: String, capabilities: NodeCapabilities) {
        self.capabilities
            .write()
            .expect("Failed to acquire write lock on capabilities registry")
            .insert(node_type, capabilities);
    }

    /// Get capabilities for a node type
    ///
    /// Returns default capabilities if node type not registered
    pub fn get(&self, node_type: &str) -> NodeCapabilities {
        self.capabilities
            .read()
            .expect("Failed to acquire read lock on capabilities registry")
            .get(node_type)
            .cloned()
            .unwrap_or_else(|| NodeCapabilities::new(node_type))
    }

    /// Update average processing time for a node type
    pub fn update_avg_processing(&mut self, node_type: &str, measurement_us: u64) {
        let mut caps = self
            .capabilities
            .write()
            .expect("Failed to acquire write lock");

        if let Some(cap) = caps.get_mut(node_type) {
            cap.update_avg_processing_us(measurement_us);
        }
    }

    /// Check if a node type should be auto-wrapped
    pub fn should_auto_wrap(&self, node_type: &str) -> bool {
        self.get(node_type).should_auto_wrap()
    }
}

impl Default for NodeCapabilitiesRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_node_capabilities() {
        let cap = NodeCapabilities::new("TestNode");

        assert_eq!(cap.node_type, "TestNode");
        assert!(cap.parallelizable); // Default
        assert!(!cap.batch_aware); // Default
        assert!(cap.validate().is_ok());
    }

    #[test]
    fn test_update_avg_processing_us() {
        let mut cap = NodeCapabilities::new("TestNode");
        cap.avg_processing_us = 1000.0;

        // Update with new measurement (2000us)
        cap.update_avg_processing_us(2000);

        // EMA with alpha=0.1: 0.1 * 2000 + 0.9 * 1000 = 1100
        assert!((cap.avg_processing_us - 1100.0).abs() < 0.1);
    }

    #[test]
    fn test_should_auto_wrap() {
        let mut cap = NodeCapabilities::new("TTS");
        cap.parallelizable = false;
        cap.batch_aware = true;

        assert!(cap.should_auto_wrap());

        // Parallelizable nodes should not be auto-wrapped
        cap.parallelizable = true;
        assert!(!cap.should_auto_wrap());

        // Non-batch-aware nodes should not be auto-wrapped
        cap.parallelizable = false;
        cap.batch_aware = false;
        assert!(!cap.should_auto_wrap());
    }

    #[test]
    fn test_capabilities_validation_success() {
        let cap = NodeCapabilities::new("TestNode");
        assert!(cap.validate().is_ok());
    }

    #[test]
    fn test_capabilities_validation_fails_zero_queue() {
        let mut cap = NodeCapabilities::new("TestNode");
        cap.queue_capacity = 0;

        let result = cap.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be > 0"));
    }

    #[test]
    fn test_capabilities_validation_warns_large_queue() {
        let mut cap = NodeCapabilities::new("TestNode");
        cap.queue_capacity = 2000;

        let result = cap.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unusually large"));
    }

    #[test]
    fn test_registry_default_capabilities() {
        let registry = NodeCapabilitiesRegistry::new();

        // Check TTS node (should be non-parallelizable, batch-aware)
        let tts_cap = registry.get("TTS");
        assert_eq!(tts_cap.node_type, "TTS");
        assert!(!tts_cap.parallelizable);
        assert!(tts_cap.batch_aware);
        assert_eq!(tts_cap.overflow_policy, OverflowPolicy::MergeOnOverflow);

        // Check AudioResample node (should be parallelizable)
        let resample_cap = registry.get("AudioResample");
        assert!(resample_cap.parallelizable);
        assert!(!resample_cap.batch_aware);
        assert_eq!(resample_cap.overflow_policy, OverflowPolicy::DropOldest);
    }

    #[test]
    fn test_registry_unknown_node_type_returns_default() {
        let registry = NodeCapabilitiesRegistry::new();

        let unknown_cap = registry.get("UnknownNodeType");
        assert_eq!(unknown_cap.node_type, "UnknownNodeType");
        assert!(unknown_cap.parallelizable); // Default
    }

    #[test]
    fn test_registry_register_custom_capabilities() {
        let mut registry = NodeCapabilitiesRegistry::new();

        let custom_cap = NodeCapabilities {
            node_type: "CustomNode".to_string(),
            parallelizable: false,
            batch_aware: true,
            avg_processing_us: 5000.0,
            queue_capacity: 10,
            overflow_policy: OverflowPolicy::Block,
            supports_control_messages: true,
        };

        registry.register("CustomNode".to_string(), custom_cap.clone());

        let retrieved = registry.get("CustomNode");
        assert_eq!(retrieved.node_type, "CustomNode");
        assert!(!retrieved.parallelizable);
        assert_eq!(retrieved.queue_capacity, 10);
    }

    #[test]
    fn test_registry_should_auto_wrap() {
        let registry = NodeCapabilitiesRegistry::new();

        // TTS should be auto-wrapped (non-parallelizable + batch-aware)
        assert!(registry.should_auto_wrap("TTS"));

        // AudioResample should NOT be auto-wrapped (parallelizable)
        assert!(!registry.should_auto_wrap("AudioResample"));
    }

    #[test]
    fn test_overflow_policy_variants() {
        let policies = vec![
            OverflowPolicy::DropOldest,
            OverflowPolicy::DropNewest,
            OverflowPolicy::Block,
            OverflowPolicy::MergeOnOverflow,
        ];

        // Ensure all variants can be created and compared
        for policy in policies {
            let cap = NodeCapabilities {
                overflow_policy: policy,
                ..NodeCapabilities::new("test")
            };
            assert_eq!(cap.overflow_policy, policy);
        }
    }
}
