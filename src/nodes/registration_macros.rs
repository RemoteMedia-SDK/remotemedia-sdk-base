//! Registration macros for ergonomic node registration
//!
//! This module provides declarative macros that simplify node registration
//! by automatically generating factory structs and handling Arc wrapping.
//!
//! # Macros (Compile-time, for known node names)
//!
//! - `register_python_node!` - Register a single Python node by class name
//! - `register_python_nodes!` - Register multiple Python nodes in batch
//! - `register_rust_node!` - Register a Rust node with custom initialization
//! - `register_rust_node_default!` - Register a Rust node using Default trait
//!
//! # Runtime Registration (for dynamic/file-based nodes)
//!
//! For registering Python nodes from file paths at runtime, use the Python API:
//!
//! ```python
//! from remotemedia import register_python_node
//!
//! # Register node(s) from a file
//! register_python_node("./my_nodes/custom_ml.py")
//!
//! # Register with options
//! register_python_node(
//!     "./my_nodes/my_tts.py",
//!     node_type="MyTTS",
//!     multi_output=True
//! )
//! ```
//!
//! The Python registration integrates with the multiprocess node registry,
//! making nodes available to the Rust runtime automatically.
//!
//! # Example (Compile-time)
//!
//! ```
//! use remotemedia_core::nodes::registry::NodeRegistry;
//! use remotemedia_core::register_python_node;
//!
//! let mut registry = NodeRegistry::new();
//! register_python_node!(registry, "OmniASRNode");
//! ```

/// Register a single Python node by class name
///
/// This macro simplifies Python node registration by automatically generating
/// the factory boilerplate. It reduces registration from ~40 lines to 1 line.
///
/// # Syntax
///
/// `register_python_node!(registry, "NodeClassName");`
///
/// # Parameters
///
/// - `registry`: Mutable reference to `NodeRegistry`
/// - `node_name`: String literal - must match Python class name exactly
///
/// # Example
///
/// ```
/// use remotemedia_core::nodes::registry::NodeRegistry;
/// use remotemedia_core::register_python_node;
///
/// let mut registry = NodeRegistry::new();
/// register_python_node!(registry, "OmniASRNode");
///
/// assert!(registry.has_python_impl("OmniASRNode"));
/// ```
///
/// # Generated Code
///
/// The macro generates an anonymous factory struct that implements `NodeFactory`,
/// creating a `PythonNodeExecutor` for the given node name.
#[macro_export]
macro_rules! register_python_node {
    ($registry:expr, $node_name:literal) => {{
        struct Factory;
        impl $crate::nodes::registry::NodeFactory for Factory {
            fn create(
                &self,
                _params: $crate::serde_json::Value,
            ) -> $crate::Result<Box<dyn $crate::executor::node_executor::NodeExecutor>>
            {
                Ok(Box::new(
                    $crate::executor::node_executor::PythonNodeExecutor::new($node_name),
                ))
            }

            fn node_type(&self) -> &str {
                $node_name
            }

            fn is_rust_native(&self) -> bool {
                false
            }
        }
        $registry.register_python(std::sync::Arc::new(Factory));
    }};
}

/// Register multiple Python nodes in a single batch operation
///
/// This macro simplifies batch registration of Python nodes by expanding to
/// multiple `register_python_node!` calls.
///
/// # Syntax
///
/// `register_python_nodes!(registry, ["Node1", "Node2", "Node3"]);`
///
/// # Parameters
///
/// - `registry`: Mutable reference to `NodeRegistry`
/// - `node_names`: Array of string literals (supports trailing comma)
///
/// # Example
///
/// ```
/// use remotemedia_core::nodes::registry::NodeRegistry;
/// use remotemedia_core::register_python_nodes;
///
/// let mut registry = NodeRegistry::new();
/// register_python_nodes!(registry, [
///     "OmniASRNode",
///     "KokoroTTSNode",
///     "SimplePyTorchNode",
/// ]);
///
/// assert_eq!(registry.list_node_types().len(), 3);
/// ```
///
/// # Behavior
///
/// Expands to multiple `register_python_node!` invocations.
/// Duplicate names are allowed (last registration wins).
#[macro_export]
macro_rules! register_python_nodes {
    ($registry:expr, [$($node_name:literal),* $(,)?]) => {{
        $(
            $crate::register_python_node!($registry, $node_name);
        )*
    }};
}

/// Register a Rust node with custom initialization closure
///
/// This macro simplifies Rust node registration by automatically generating
/// the factory boilerplate while allowing custom initialization logic.
///
/// # Syntax
///
/// `register_rust_node!(registry, NodeType, |params| { NodeType::new(params) });`
///
/// # Parameters
///
/// - `registry`: Mutable reference to `NodeRegistry`
/// - `node_type`: Rust type implementing `NodeHandler` trait
/// - `factory`: Closure `Fn(Value) -> Result<NodeType>` - custom initialization logic
///
/// # Example
///
/// See `register_rust_node_default!` for simpler usage with Default-implementing types.
///
/// # Guarantees
///
/// - Node name derived from type via `stringify!` (typo-proof)
/// - Closure invoked on each `create()` call (fresh instance)
/// - Closure can capture environment (must be `'static`)
/// - Factory errors propagate as `Result::Err`
#[macro_export]
macro_rules! register_rust_node {
    ($registry:expr, $node_type:ty, $factory:expr) => {{
        struct Factory;
        impl $crate::nodes::registry::NodeFactory for Factory {
            fn create(
                &self,
                params: $crate::serde_json::Value,
            ) -> $crate::Result<Box<dyn $crate::executor::node_executor::NodeExecutor>>
            {
                let handler: $node_type = ($factory)(params)?;
                Ok(Box::new(
                    $crate::executor::node_executor::RustNodeExecutor::new(
                        stringify!($node_type),
                        Box::new(handler),
                    ),
                ))
            }

            fn node_type(&self) -> &str {
                stringify!($node_type)
            }

            fn is_rust_native(&self) -> bool {
                true
            }
        }
        $registry.register_rust(std::sync::Arc::new(Factory));
    }};
}

/// Register a Rust node using Default trait
///
/// This macro is the simplest form for registering Rust nodes that implement
/// the `Default` trait. It delegates to `register_rust_node!` with a default closure.
///
/// # Syntax
///
/// `register_rust_node_default!(registry, NodeType);`
///
/// # Parameters
///
/// - `registry`: Mutable reference to `NodeRegistry`
/// - `node_type`: Rust type implementing `NodeHandler + Default`
///
/// # Example
///
/// Types must implement both `Default` and `NodeHandler` traits.
///
/// # Compile-time Requirements
///
/// The node type MUST implement the `Default` trait. If it doesn't, you'll get
/// a compile error. Use `register_rust_node!` instead for custom initialization.
#[macro_export]
macro_rules! register_rust_node_default {
    ($registry:expr, $node_type:ty) => {{
        $crate::register_rust_node!(
            $registry,
            $node_type,
            |_params: $crate::serde_json::Value| -> $crate::Result<$node_type> {
                Ok(<$node_type>::default())
            }
        );
    }};
}
