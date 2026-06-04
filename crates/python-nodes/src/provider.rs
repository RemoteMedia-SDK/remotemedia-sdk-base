//! Python nodes provider using dynamic registration
//!
//! This provider creates factories dynamically from the Python node registry,
//! rather than hardcoding factory definitions.

use crate::registry::{register_default_python_nodes, PythonNodeConfig, PYTHON_NODE_REGISTRY};
use remotemedia_core::nodes::provider::NodeProvider;
use remotemedia_core::nodes::python_streaming::PythonStreamingNode;
use remotemedia_core::nodes::schema::{NodeSchema, RuntimeDataType};
use remotemedia_core::nodes::streaming_node::{
    AsyncNodeWrapper, StreamingNode, StreamingNodeFactory, StreamingNodeRegistry,
};
use remotemedia_core::Error;
use serde_json::Value;
use std::sync::Arc;

/// A dynamic factory that creates Python nodes based on registry configuration
struct DynamicPythonNodeFactory {
    config: PythonNodeConfig,
}

impl DynamicPythonNodeFactory {
    fn new(config: PythonNodeConfig) -> Self {
        Self { config }
    }
}

impl StreamingNodeFactory for DynamicPythonNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let node = if let Some(sid) = session_id {
            PythonStreamingNode::with_session(node_id, &self.config.python_class, params, sid)?
        } else {
            PythonStreamingNode::new(node_id, &self.config.python_class, params)?
        };
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }

    fn node_type(&self) -> &str {
        &self.config.node_type
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        self.config.is_multi_output
    }

    fn schema(&self) -> Option<NodeSchema> {
        let mut schema = NodeSchema::new(&self.config.node_type);

        if let Some(ref desc) = self.config.description {
            schema = schema.description(desc);
        }

        if let Some(ref cat) = self.config.category {
            schema = schema.category(cat);
        }

        // Convert string types to RuntimeDataType
        let accepts: Vec<RuntimeDataType> = self
            .config
            .accepts
            .iter()
            .filter_map(|t| match t.as_str() {
                "audio" => Some(RuntimeDataType::Audio),
                "text" => Some(RuntimeDataType::Text),
                "json" => Some(RuntimeDataType::Json),
                "video" => Some(RuntimeDataType::Video),
                "binary" | "bytes" => Some(RuntimeDataType::Binary),
                "tensor" => Some(RuntimeDataType::Tensor),
                "numpy" => Some(RuntimeDataType::Numpy),
                _ => None,
            })
            .collect();

        let produces: Vec<RuntimeDataType> = self
            .config
            .produces
            .iter()
            .filter_map(|t| match t.as_str() {
                "audio" => Some(RuntimeDataType::Audio),
                "text" => Some(RuntimeDataType::Text),
                "json" => Some(RuntimeDataType::Json),
                "video" => Some(RuntimeDataType::Video),
                "binary" | "bytes" => Some(RuntimeDataType::Binary),
                "tensor" => Some(RuntimeDataType::Tensor),
                "numpy" => Some(RuntimeDataType::Numpy),
                _ => None,
            })
            .collect();

        if !accepts.is_empty() {
            schema = schema.accepts(accepts);
        }

        if !produces.is_empty() {
            schema = schema.produces(produces);
        }

        Some(schema)
    }
}

/// Provider for dynamically registered Python nodes.
///
/// This provider:
/// 1. Registers default Python nodes on first use
/// 2. Creates factories dynamically from the registry
/// 3. Supports runtime registration of additional nodes
///
/// Priority is 500 (below core nodes at 1000, above user nodes at 100).
pub struct PythonNodesProvider;

impl NodeProvider for PythonNodesProvider {
    fn register(&self, registry: &mut StreamingNodeRegistry) {
        // Ensure default nodes are registered
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            register_default_python_nodes();
        });

        // Create factories for all registered Python nodes
        let nodes = PYTHON_NODE_REGISTRY.get_all();
        for config in nodes {
            let node_type = config.node_type.clone();
            registry.register(Arc::new(DynamicPythonNodeFactory::new(config)));
            tracing::debug!(node_type = %node_type, "Registered dynamic Python node factory");
        }
    }

    fn provider_name(&self) -> &'static str {
        "python-nodes"
    }

    fn node_count(&self) -> usize {
        PYTHON_NODE_REGISTRY.len()
    }

    fn priority(&self) -> i32 {
        // Below core nodes (1000), above user nodes (100)
        500
    }
}

// Auto-register the Python nodes provider
inventory::submit! {
    &PythonNodesProvider as &'static dyn NodeProvider
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{clear_registry, register_python_node, PythonNodeConfig};

    #[test]
    fn test_dynamic_factory_creation() {
        clear_registry();

        register_python_node(
            PythonNodeConfig::new("TestDynamicNode")
                .with_python_class("test.TestDynamicNode")
                .with_multi_output(true)
                .with_description("A test node")
                .with_category("test")
                .accepts(["audio"])
                .produces(["text"]),
        );

        let mut registry = StreamingNodeRegistry::new();
        let provider = PythonNodesProvider;
        provider.register(&mut registry);

        assert!(registry.has_node_type("TestDynamicNode"));
        assert!(registry.is_python_node("TestDynamicNode"));
    }

    #[test]
    fn test_provider_metadata() {
        let provider = PythonNodesProvider;
        assert_eq!(provider.provider_name(), "python-nodes");
        assert_eq!(provider.priority(), 500);
    }

    #[test]
    fn test_schema_generation() {
        let config = PythonNodeConfig::new("SchemaTestNode")
            .with_description("Test description")
            .with_category("test")
            .accepts(["audio", "text"])
            .produces(["json"]);

        let factory = DynamicPythonNodeFactory::new(config);
        let schema = factory.schema().unwrap();

        assert_eq!(schema.node_type, "SchemaTestNode");
    }
}
