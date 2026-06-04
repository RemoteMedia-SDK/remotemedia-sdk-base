//! Fluent builder API for constructing `StreamingNodeRegistry` instances.
//!
//! This module provides an ergonomic way to build node registries with
//! type-safe registration methods.
//!
//! # Example
//!
//! ```ignore
//! use remotemedia_core::nodes::StreamingNodeRegistry;
//!
//! let registry = StreamingNodeRegistry::builder()
//!     // Start with all default nodes from registered providers
//!     .with_defaults()
//!     
//!     // Add custom Python nodes
//!     .python("MyCustomASR")
//!     .python_multi_output("MyStreamingTTS")
//!     
//!     // Batch registration
//!     .python_batch(&["Node1", "Node2", "Node3"])
//!     
//!     // Add a custom factory
//!     .factory(Arc::new(MyCustomFactory))
//!     
//!     .build();
//! ```

use crate::nodes::provider::NodeProvider;
use crate::nodes::{AsyncNodeWrapper, StreamingNodeFactory, StreamingNodeRegistry};
use crate::Error;
use serde_json::Value;
use std::sync::Arc;

#[cfg(feature = "multiprocess")]
use crate::nodes::python_streaming::PythonStreamingNode;

/// Builder for constructing `StreamingNodeRegistry` instances with a fluent API.
///
/// # Example
///
/// ```ignore
/// let registry = StreamingNodeRegistryBuilder::new()
///     .with_defaults()
///     .python("CustomNode")
///     .build();
/// ```
pub struct StreamingNodeRegistryBuilder {
    registry: StreamingNodeRegistry,
    include_defaults: bool,
}

impl Default for StreamingNodeRegistryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamingNodeRegistryBuilder {
    /// Create a new empty builder.
    ///
    /// Call `.with_defaults()` to include nodes from registered `NodeProvider`s.
    pub fn new() -> Self {
        Self {
            registry: StreamingNodeRegistry::new(),
            include_defaults: false,
        }
    }

    /// Include all nodes from registered `NodeProvider` implementations.
    ///
    /// This collects nodes from all providers registered via the `inventory` crate,
    /// sorted by priority (lower priority number = registered first).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let registry = StreamingNodeRegistry::builder()
    ///     .with_defaults()  // Includes CoreNodes, PythonNodes, CandleNodes, etc.
    ///     .python("MyExtraNode")
    ///     .build();
    /// ```
    pub fn with_defaults(mut self) -> Self {
        self.include_defaults = true;
        self
    }

    /// Register nodes from a specific `NodeProvider`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// struct MyProvider;
    /// impl NodeProvider for MyProvider {
    ///     fn register(&self, registry: &mut StreamingNodeRegistry) {
    ///         registry.register(Arc::new(MyFactory));
    ///     }
    ///     fn provider_name(&self) -> &'static str { "my-provider" }
    /// }
    ///
    /// let registry = StreamingNodeRegistry::builder()
    ///     .provider(&MyProvider)
    ///     .build();
    /// ```
    pub fn provider(mut self, provider: &dyn NodeProvider) -> Self {
        provider.register(&mut self.registry);
        self
    }

    /// Register a streaming node factory directly.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let registry = StreamingNodeRegistry::builder()
    ///     .factory(Arc::new(MyCustomFactory))
    ///     .build();
    /// ```
    pub fn factory(mut self, factory: Arc<dyn StreamingNodeFactory>) -> Self {
        self.registry.register(factory);
        self
    }

    /// Register a Python node by type name (single output per input).
    ///
    /// The Python class must be registered in the multiprocess node registry
    /// before pipeline execution.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let registry = StreamingNodeRegistry::builder()
    ///     .python("WhisperASR")
    ///     .python("SentimentAnalyzer")
    ///     .build();
    /// ```
    #[cfg(feature = "multiprocess")]
    pub fn python(self, node_type: &'static str) -> Self {
        self.factory(Arc::new(SimplePythonNodeFactory {
            node_type,
            multi_output: false,
        }))
    }

    /// Register a Python node that yields multiple outputs per input.
    ///
    /// Use this for streaming/generative nodes like TTS or chunking nodes.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let registry = StreamingNodeRegistry::builder()
    ///     .python_multi_output("StreamingTTS")  // Yields audio chunks
    ///     .python_multi_output("VADSegmenter")  // Yields speech segments
    ///     .build();
    /// ```
    #[cfg(feature = "multiprocess")]
    pub fn python_multi_output(self, node_type: &'static str) -> Self {
        self.factory(Arc::new(SimplePythonNodeFactory {
            node_type,
            multi_output: true,
        }))
    }

    /// Register multiple Python nodes at once (single output each).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let registry = StreamingNodeRegistry::builder()
    ///     .python_batch(&["Node1", "Node2", "Node3"])
    ///     .build();
    /// ```
    #[cfg(feature = "multiprocess")]
    pub fn python_batch(mut self, node_types: &[&'static str]) -> Self {
        for &node_type in node_types {
            self = self.python(node_type);
        }
        self
    }

    /// Register multiple Python multi-output nodes at once.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let registry = StreamingNodeRegistry::builder()
    ///     .python_multi_output_batch(&["TTS1", "TTS2"])
    ///     .build();
    /// ```
    #[cfg(feature = "multiprocess")]
    pub fn python_multi_output_batch(mut self, node_types: &[&'static str]) -> Self {
        for &node_type in node_types {
            self = self.python_multi_output(node_type);
        }
        self
    }

    /// Build the final `StreamingNodeRegistry`.
    ///
    /// If `.with_defaults()` was called, this collects nodes from all
    /// registered `NodeProvider` implementations first, then adds any
    /// additional nodes registered via the builder.
    pub fn build(mut self) -> StreamingNodeRegistry {
        if self.include_defaults {
            // Collect from all registered providers first
            let mut default_registry = StreamingNodeRegistry::new();

            // Get providers sorted by priority
            let mut providers: Vec<_> = crate::nodes::provider::iter_providers().collect();
            providers.sort_by_key(|p| p.priority());

            for provider in providers {
                tracing::debug!(
                    provider = provider.provider_name(),
                    priority = provider.priority(),
                    "Builder: registering nodes from provider"
                );
                provider.register(&mut default_registry);
            }

            // Merge: default nodes first, then builder additions (which can override)
            default_registry.merge_from(&mut self.registry);

            default_registry
        } else {
            self.registry
        }
    }
}

// Extension methods for StreamingNodeRegistry — keep builder() here since it
// is a builder concern.
impl StreamingNodeRegistry {
    /// Create a builder for constructing a `StreamingNodeRegistry`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let registry = StreamingNodeRegistry::builder()
    ///     .with_defaults()
    ///     .python("CustomNode")
    ///     .build();
    /// ```
    pub fn builder() -> StreamingNodeRegistryBuilder {
        StreamingNodeRegistryBuilder::new()
    }
}

/// Simple factory for Python nodes created via the builder.
#[cfg(feature = "multiprocess")]
struct SimplePythonNodeFactory {
    node_type: &'static str,
    multi_output: bool,
}

#[cfg(feature = "multiprocess")]
impl StreamingNodeFactory for SimplePythonNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn crate::nodes::StreamingNode>, Error> {
        // Use with_session if session_id provided, otherwise new()
        let node = if let Some(sid) = session_id {
            PythonStreamingNode::with_session(node_id, self.node_type, params, sid)?
        } else {
            PythonStreamingNode::new(node_id, self.node_type, params)?
        };

        // Wrap with AsyncNodeWrapper to implement StreamingNode trait
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        self.node_type
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        self.multi_output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_empty() {
        let registry = StreamingNodeRegistry::builder().build();
        assert!(registry.list_types().is_empty());
    }

    #[test]
    fn test_builder_with_defaults() {
        let registry = StreamingNodeRegistry::builder().with_defaults().build();

        // Should have at least some core nodes
        let types = registry.list_types();
        assert!(!types.is_empty(), "Registry should have default nodes");

        // Should have CalculatorNode from CoreNodesProvider
        assert!(
            registry.has_node_type("CalculatorNode"),
            "Should have CalculatorNode from defaults"
        );
    }

    #[test]
    fn test_builder_custom_factory() {
        struct TestFactory;
        impl StreamingNodeFactory for TestFactory {
            fn create(
                &self,
                _node_id: String,
                _params: &Value,
                _session_id: Option<String>,
            ) -> Result<Box<dyn crate::nodes::StreamingNode>, Error> {
                Err(Error::Execution("test".into()))
            }
            fn node_type(&self) -> &str {
                "TestNode"
            }
        }

        let registry = StreamingNodeRegistry::builder()
            .factory(Arc::new(TestFactory))
            .build();

        assert!(registry.has_node_type("TestNode"));
    }

    #[cfg(feature = "multiprocess")]
    #[test]
    fn test_builder_python_nodes() {
        let registry = StreamingNodeRegistry::builder()
            .python("SingleOutputNode")
            .python_multi_output("MultiOutputNode")
            .build();

        assert!(registry.has_node_type("SingleOutputNode"));
        assert!(registry.has_node_type("MultiOutputNode"));
        assert!(registry.is_python_node("SingleOutputNode"));
        assert!(registry.is_python_node("MultiOutputNode"));
        assert!(!registry.is_multi_output_streaming("SingleOutputNode"));
        assert!(registry.is_multi_output_streaming("MultiOutputNode"));
    }

    #[cfg(feature = "multiprocess")]
    #[test]
    fn test_builder_python_batch() {
        let registry = StreamingNodeRegistry::builder()
            .python_batch(&["Node1", "Node2", "Node3"])
            .build();

        assert!(registry.has_node_type("Node1"));
        assert!(registry.has_node_type("Node2"));
        assert!(registry.has_node_type("Node3"));
    }

    #[test]
    fn test_builder_override_defaults() {
        struct OverrideFactory;
        impl StreamingNodeFactory for OverrideFactory {
            fn create(
                &self,
                _node_id: String,
                _params: &Value,
                _session_id: Option<String>,
            ) -> Result<Box<dyn crate::nodes::StreamingNode>, Error> {
                Err(Error::Execution("overridden".into()))
            }
            fn node_type(&self) -> &str {
                "CalculatorNode" // Override the default
            }
        }

        let registry = StreamingNodeRegistry::builder()
            .with_defaults()
            .factory(Arc::new(OverrideFactory)) // This should override
            .build();

        // Should have the node type
        assert!(registry.has_node_type("CalculatorNode"));

        // Creating should use our override (which returns an error)
        let result = registry.create_node("CalculatorNode", "test".into(), &Value::Null, None);
        assert!(result.is_err());

        // Check the error message contains "overridden"
        let err = result.err().unwrap();
        assert!(
            err.to_string().contains("overridden"),
            "Expected 'overridden' in error: {}",
            err
        );
    }
}
