//! Custom node registration helpers for WebRTC transport
//!
//! This module provides utilities for registering custom streaming nodes
//! beyond the built-in ones provided by core.
//!
//! # Examples
//!
//! ## Registering a Custom Python Node
//!
//! ```
//! use remotemedia_webrtc::custom_nodes::create_custom_registry;
//!
//! // Create a registry with custom Python nodes
//! let registry = create_custom_registry(&[
//!     ("MyCustomASR", true),  // (name, is_multi_output)
//!     ("MyCustomTTS", true),
//! ]);
//!
//! // Verify nodes are registered
//! assert!(registry.has_node_type("MyCustomASR"));
//! assert!(registry.has_node_type("MyCustomTTS"));
//! ```
//!
//! ## Registering a Custom Factory
//!
//! ```
//! use remotemedia_webrtc::custom_nodes::{create_custom_registry_with_factories, PythonNodeFactory};
//! use std::sync::Arc;
//!
//! // Create registry with custom factories
//! let registry = create_custom_registry_with_factories(vec![
//!     Arc::new(PythonNodeFactory::new("MyPythonNode", true)),
//! ]);
//!
//! assert!(registry.has_node_type("MyPythonNode"));
//! ```

use remotemedia_core::nodes::python_streaming::PythonStreamingNode;
use remotemedia_core::nodes::streaming_registry::create_default_streaming_registry;
use remotemedia_core::nodes::{
    AsyncNodeWrapper, StreamingNode, StreamingNodeFactory, StreamingNodeRegistry,
};
use remotemedia_core::Error;
use serde_json::Value;
use std::sync::Arc;

/// Helper factory for creating Python streaming nodes
///
/// This is a convenience wrapper that simplifies registering Python nodes.
/// It uses `PythonStreamingNode` under the hood which connects to Python
/// processes via the multiprocess executor.
pub struct PythonNodeFactory {
    node_type_name: String,
    is_multi_output: bool,
}

impl PythonNodeFactory {
    /// Create a new Python node factory
    ///
    /// # Arguments
    ///
    /// * `node_type_name` - The Python class name (e.g., "MyCustomASR")
    /// * `is_multi_output` - Whether the node yields multiple outputs per input
    ///
    /// # Examples
    ///
    /// ```
    /// use remotemedia_webrtc::custom_nodes::PythonNodeFactory;
    /// use remotemedia_core::nodes::StreamingNodeFactory;
    ///
    /// let factory = PythonNodeFactory::new("MyCustomASR", false);
    /// assert_eq!(factory.node_type(), "MyCustomASR");
    /// assert!(factory.is_python_node());
    /// assert!(!factory.is_multi_output_streaming());
    /// ```
    pub fn new(node_type_name: impl Into<String>, is_multi_output: bool) -> Self {
        Self {
            node_type_name: node_type_name.into(),
            is_multi_output,
        }
    }
}

impl StreamingNodeFactory for PythonNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let node = if let Some(sid) = session_id {
            PythonStreamingNode::with_session(node_id, &self.node_type_name, params, sid)?
        } else {
            PythonStreamingNode::new(node_id, &self.node_type_name, params)?
        };
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        &self.node_type_name
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        self.is_multi_output
    }
}

/// Create a custom streaming registry with default nodes + custom Python nodes
///
/// This is a convenience function for quickly registering Python nodes.
/// It starts with all built-in nodes and adds your custom Python nodes.
///
/// # Arguments
///
/// * `python_nodes` - Slice of tuples: (node_type_name, is_multi_output)
///
/// # Returns
///
/// A `StreamingNodeRegistry` with default + custom nodes registered
///
/// # Examples
///
/// ```
/// use remotemedia_webrtc::custom_nodes::create_custom_registry;
///
/// let registry = create_custom_registry(&[
///     ("WhisperASR", false),
///     ("GPT4TTS", true),
///     ("CustomFilter", false),
/// ]);
///
/// assert!(registry.has_node_type("WhisperASR"));
/// assert!(registry.has_node_type("GPT4TTS"));
/// assert!(registry.has_node_type("CustomFilter"));
/// ```
pub fn create_custom_registry(python_nodes: &[(&str, bool)]) -> StreamingNodeRegistry {
    let mut registry = create_default_streaming_registry();

    for (node_type, is_multi_output) in python_nodes {
        registry.register(Arc::new(PythonNodeFactory::new(
            *node_type,
            *is_multi_output,
        )));
    }

    registry
}

/// Create a custom streaming registry with default nodes + custom factories
///
/// This provides full control over node registration, allowing you to register
/// both Rust and Python nodes with custom factory implementations.
///
/// # Arguments
///
/// * `factories` - Vector of custom node factories to register
///
/// # Returns
///
/// A `StreamingNodeRegistry` with default + custom nodes registered
///
/// # Examples
///
/// ```
/// use remotemedia_webrtc::custom_nodes::{create_custom_registry_with_factories, PythonNodeFactory};
/// use std::sync::Arc;
///
/// let registry = create_custom_registry_with_factories(vec![
///     Arc::new(PythonNodeFactory::new("CustomNode1", false)),
/// ]);
///
/// assert!(registry.has_node_type("CustomNode1"));
/// ```
pub fn create_custom_registry_with_factories(
    factories: Vec<Arc<dyn StreamingNodeFactory>>,
) -> StreamingNodeRegistry {
    let mut registry = create_default_streaming_registry();

    for factory in factories {
        registry.register(factory);
    }

    registry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_python_node_factory_creation() {
        let factory = PythonNodeFactory::new("TestNode", true);
        assert_eq!(factory.node_type(), "TestNode");
        assert!(factory.is_python_node());
        assert!(factory.is_multi_output_streaming());
    }

    #[test]
    fn test_create_custom_registry() {
        let registry = create_custom_registry(&[("Node1", false), ("Node2", true)]);

        assert!(registry.has_node_type("Node1"));
        assert!(registry.has_node_type("Node2"));
        assert!(registry.is_python_node("Node1"));
        assert!(registry.is_multi_output_streaming("Node2"));
    }

    #[test]
    fn test_custom_registry_includes_defaults() {
        let registry = create_custom_registry(&[("CustomNode", false)]);

        // Should include default nodes
        assert!(registry.has_node_type("PassThrough"));
        assert!(registry.has_node_type("AudioChunkerNode"));

        // Should include custom node
        assert!(registry.has_node_type("CustomNode"));
    }
}
