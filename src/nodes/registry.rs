//! Node registry for runtime-based node creation (Phase 5: T061-T064)
//!
//! The NodeRegistry manages node factories and handles runtime hint resolution
//! to select between Rust-native and Python-fallback implementations.

use crate::error::{Error, Result};
use crate::executor::node_executor::NodeExecutor;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Runtime hint for node selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeHint {
    /// Automatically select best available runtime (Rust if available, else Python)
    Auto,
    /// Force Rust native implementation
    Rust,
    /// Force Python implementation
    Python,
}

impl RuntimeHint {
    /// Parse runtime hint from string
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "rust" => RuntimeHint::Rust,
            "python" => RuntimeHint::Python,
            _ => RuntimeHint::Auto,
        }
    }
}

impl Default for RuntimeHint {
    fn default() -> Self {
        RuntimeHint::Auto
    }
}

/// Factory trait for creating node executors
pub trait NodeFactory: Send + Sync {
    /// Create a new node executor with given parameters
    fn create(&self, params: Value) -> Result<Box<dyn NodeExecutor>>;

    /// Get the node type name
    fn node_type(&self) -> &str;

    /// Check if this is a Rust-native implementation
    fn is_rust_native(&self) -> bool {
        true
    }

    /// Get the node schema for type generation.
    ///
    /// Override this to provide schema metadata (description, accepts/produces,
    /// config parameters) that gets exported to TypeScript types via NAPI.
    ///
    /// Returns None by default - nodes without schema info are still usable
    /// but won't have typed configs in generated TypeScript.
    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        None
    }
}

/// Node registry for managing node factories
pub struct NodeRegistry {
    /// Rust-native factories by node type
    rust_factories: HashMap<String, Arc<dyn NodeFactory>>,

    /// Python fallback factories by node type
    python_factories: HashMap<String, Arc<dyn NodeFactory>>,
}

/// Multi-tier composite registry that chains multiple registries
///
/// Searches registries in order (first match wins):
/// 1. User registry (custom nodes, highest priority)
/// 2. Audio registry (audio processing nodes)
/// 3. System registry (built-in nodes, lowest priority)
///
/// This allows layering and override semantics:
/// - User can override system nodes
/// - Audio nodes isolated from user nodes
/// - Clean separation of concerns
pub struct CompositeRegistry {
    /// Ordered list of registries (searched first-to-last)
    registries: Vec<Arc<NodeRegistry>>,

    /// Optional names for debugging
    names: Vec<String>,
}

impl NodeRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            rust_factories: HashMap::new(),
            python_factories: HashMap::new(),
        }
    }

    /// Register a Rust-native node factory
    pub fn register_rust(&mut self, factory: Arc<dyn NodeFactory>) {
        let node_type = factory.node_type().to_string();
        self.rust_factories.insert(node_type, factory);
    }

    /// Register a Python fallback factory
    pub fn register_python(&mut self, factory: Arc<dyn NodeFactory>) {
        let node_type = factory.node_type().to_string();
        self.python_factories.insert(node_type, factory);
    }

    /// Create a node executor with runtime hint resolution
    ///
    /// # Arguments
    /// * `node_type` - The type of node to create
    /// * `hint` - Runtime hint for implementation selection
    /// * `params` - Node initialization parameters
    ///
    /// # Returns
    /// A boxed NodeExecutor implementation
    pub fn create_node(
        &self,
        node_type: &str,
        hint: RuntimeHint,
        params: Value,
    ) -> Result<Box<dyn NodeExecutor>> {
        match hint {
            RuntimeHint::Rust => {
                // Force Rust implementation
                let factory = self.rust_factories.get(node_type).ok_or_else(|| {
                    Error::Execution(format!(
                        "Rust implementation not available for node type: {}",
                        node_type
                    ))
                })?;
                factory.create(params)
            }
            RuntimeHint::Python => {
                // Force Python implementation
                let factory = self.python_factories.get(node_type).ok_or_else(|| {
                    Error::Execution(format!(
                        "Python implementation not available for node type: {}",
                        node_type
                    ))
                })?;
                factory.create(params)
            }
            RuntimeHint::Auto => {
                // Auto-select: prefer Rust, fallback to Python
                if let Some(factory) = self.rust_factories.get(node_type) {
                    factory.create(params)
                } else if let Some(factory) = self.python_factories.get(node_type) {
                    factory.create(params)
                } else {
                    Err(Error::Execution(format!(
                        "No implementation available for node type: {}",
                        node_type
                    )))
                }
            }
        }
    }

    /// Check if a Rust implementation is available for a node type
    pub fn has_rust_impl(&self, node_type: &str) -> bool {
        self.rust_factories.contains_key(node_type)
    }

    /// Check if a Python implementation is available for a node type
    pub fn has_python_impl(&self, node_type: &str) -> bool {
        self.python_factories.contains_key(node_type)
    }

    /// List all registered node types
    pub fn list_node_types(&self) -> Vec<String> {
        let mut types: Vec<String> = self
            .rust_factories
            .keys()
            .chain(self.python_factories.keys())
            .cloned()
            .collect();
        types.sort();
        types.dedup();
        types
    }
}

impl Default for NodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl CompositeRegistry {
    /// Create empty composite registry
    pub fn new() -> Self {
        Self {
            registries: Vec::new(),
            names: Vec::new(),
        }
    }

    /// Add a registry with optional name
    pub fn add_registry(&mut self, registry: Arc<NodeRegistry>, name: Option<&str>) {
        self.registries.push(registry);
        self.names.push(name.unwrap_or("unnamed").to_string());
    }

    /// Create node from first registry that has the type
    pub fn create_node(
        &self,
        node_type: &str,
        hint: RuntimeHint,
        params: Value,
    ) -> Result<Box<dyn NodeExecutor>> {
        for (_idx, registry) in self.registries.iter().enumerate() {
            // Check if this registry has the node type
            let has_node = match hint {
                RuntimeHint::Rust => registry.has_rust_impl(node_type),
                RuntimeHint::Python => registry.has_python_impl(node_type),
                RuntimeHint::Auto => {
                    registry.has_rust_impl(node_type) || registry.has_python_impl(node_type)
                }
            };

            if has_node {
                return registry.create_node(node_type, hint, params);
            }
        }

        Err(Error::Execution(format!(
            "No implementation available for node type: {} (searched {} registries)",
            node_type,
            self.registries.len()
        )))
    }

    /// List all node types from all registries (deduplicated)
    pub fn list_node_types(&self) -> Vec<String> {
        let mut types: Vec<String> = self
            .registries
            .iter()
            .flat_map(|reg| reg.list_node_types())
            .collect();
        types.sort();
        types.dedup();
        types
    }

    /// Check if any registry has Rust implementation
    pub fn has_rust_impl(&self, node_type: &str) -> bool {
        self.registries
            .iter()
            .any(|reg| reg.has_rust_impl(node_type))
    }

    /// Check if any registry has Python implementation
    pub fn has_python_impl(&self, node_type: &str) -> bool {
        self.registries
            .iter()
            .any(|reg| reg.has_python_impl(node_type))
    }

    /// Get registry names for debugging
    pub fn registry_names(&self) -> &[String] {
        &self.names
    }
}

impl Default for CompositeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    // Mock node executor for testing
    struct MockExecutor {
        node_type: String,
    }

    #[async_trait]
    impl NodeExecutor for MockExecutor {
        async fn initialize(
            &mut self,
            _ctx: &crate::executor_compat::node_executor::NodeContext,
        ) -> Result<()> {
            Ok(())
        }

        async fn process(&mut self, input: Value) -> Result<Vec<Value>> {
            Ok(vec![input])
        }

        async fn cleanup(&mut self) -> Result<()> {
            Ok(())
        }
    }

    // Mock factory for testing
    struct MockFactory {
        node_type: String,
        is_rust: bool,
    }

    impl NodeFactory for MockFactory {
        fn create(&self, _params: Value) -> Result<Box<dyn NodeExecutor>> {
            Ok(Box::new(MockExecutor {
                node_type: self.node_type.clone(),
            }))
        }

        fn node_type(&self) -> &str {
            &self.node_type
        }

        fn is_rust_native(&self) -> bool {
            self.is_rust
        }
    }

    #[test]
    fn test_registry_creation() {
        let registry = NodeRegistry::new();
        assert_eq!(registry.list_node_types().len(), 0);
    }

    #[test]
    fn test_register_rust_factory() {
        let mut registry = NodeRegistry::new();
        let factory = Arc::new(MockFactory {
            node_type: "test_node".to_string(),
            is_rust: true,
        });

        registry.register_rust(factory);
        assert!(registry.has_rust_impl("test_node"));
        assert!(!registry.has_python_impl("test_node"));
    }

    #[test]
    fn test_register_python_factory() {
        let mut registry = NodeRegistry::new();
        let factory = Arc::new(MockFactory {
            node_type: "test_node".to_string(),
            is_rust: false,
        });

        registry.register_python(factory);
        assert!(!registry.has_rust_impl("test_node"));
        assert!(registry.has_python_impl("test_node"));
    }

    #[tokio::test]
    async fn test_create_node_rust_hint() {
        let mut registry = NodeRegistry::new();
        let factory = Arc::new(MockFactory {
            node_type: "test_node".to_string(),
            is_rust: true,
        });
        registry.register_rust(factory);

        let mut node = registry
            .create_node("test_node", RuntimeHint::Rust, Value::Null)
            .unwrap();

        // Node should be created successfully
        let ctx = crate::executor_compat::node_executor::NodeContext {
            node_id: "test".to_string(),
            node_type: "test_node".to_string(),
            params: Value::Null,
            session_id: None,
            metadata: HashMap::new(),
        };
        assert!(node.initialize(&ctx).await.is_ok());
    }

    #[tokio::test]
    async fn test_create_node_auto_prefers_rust() {
        let mut registry = NodeRegistry::new();

        // Register both Rust and Python implementations
        let rust_factory = Arc::new(MockFactory {
            node_type: "test_node".to_string(),
            is_rust: true,
        });
        let python_factory = Arc::new(MockFactory {
            node_type: "test_node".to_string(),
            is_rust: false,
        });

        registry.register_rust(rust_factory);
        registry.register_python(python_factory);

        // Auto should prefer Rust
        let mut node = registry
            .create_node("test_node", RuntimeHint::Auto, Value::Null)
            .unwrap();

        let ctx = crate::executor_compat::node_executor::NodeContext {
            node_id: "test".to_string(),
            node_type: "test_node".to_string(),
            params: Value::Null,
            session_id: None,
            metadata: HashMap::new(),
        };
        assert!(node.initialize(&ctx).await.is_ok());
    }

    #[test]
    fn test_create_node_auto_fallback_to_python() {
        let mut registry = NodeRegistry::new();

        // Register only Python implementation
        let factory = Arc::new(MockFactory {
            node_type: "test_node".to_string(),
            is_rust: false,
        });
        registry.register_python(factory);

        // Auto should fallback to Python
        let result = registry.create_node("test_node", RuntimeHint::Auto, Value::Null);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_node_not_found() {
        let registry = NodeRegistry::new();

        let result = registry.create_node("nonexistent", RuntimeHint::Auto, Value::Null);
        assert!(result.is_err());
    }

    #[test]
    fn test_runtime_hint_from_str() {
        assert_eq!(RuntimeHint::from_str("rust"), RuntimeHint::Rust);
        assert_eq!(RuntimeHint::from_str("RUST"), RuntimeHint::Rust);
        assert_eq!(RuntimeHint::from_str("python"), RuntimeHint::Python);
        assert_eq!(RuntimeHint::from_str("Python"), RuntimeHint::Python);
        assert_eq!(RuntimeHint::from_str("auto"), RuntimeHint::Auto);
        assert_eq!(RuntimeHint::from_str("unknown"), RuntimeHint::Auto);
    }

    #[test]
    fn test_list_node_types() {
        let mut registry = NodeRegistry::new();

        let rust_factory = Arc::new(MockFactory {
            node_type: "node_a".to_string(),
            is_rust: true,
        });
        let python_factory = Arc::new(MockFactory {
            node_type: "node_b".to_string(),
            is_rust: false,
        });

        registry.register_rust(rust_factory);
        registry.register_python(python_factory);

        let types = registry.list_node_types();
        assert_eq!(types.len(), 2);
        assert!(types.contains(&"node_a".to_string()));
        assert!(types.contains(&"node_b".to_string()));
    }
}
