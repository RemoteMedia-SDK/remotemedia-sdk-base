//! Dynamic Python node registration for RemoteMedia pipelines
//!
//! This crate provides a dynamic registration system for Python nodes.
//! Instead of hardcoding factory definitions, Python nodes register themselves
//! and are discovered at runtime.
//!
//! # Usage
//!
//! ## From Python (recommended)
//!
//! Register Python nodes from file paths:
//!
//! ```python
//! from remotemedia import register_python_node
//!
//! # Register all MultiprocessNode classes from a file
//! register_python_node("./my_nodes/custom_ml.py")
//!
//! # Register with options
//! register_python_node(
//!     "./my_nodes/my_tts.py",
//!     node_type="MyTTS",
//!     multi_output=True,
//!     category="tts"
//! )
//! ```
//!
//! Or use the `@streaming_node` decorator:
//!
//! ```python
//! from remotemedia.nodes import streaming_node
//!
//! @streaming_node(
//!     node_type="KokoroTTSNode",
//!     multi_output=True,
//!     accepts=["text"],
//!     produces=["audio"]
//! )
//! class KokoroTTSNode(MultiprocessNode):
//!     async def process(self, data):
//!         # ... implementation
//! ```
//!
//! ## From Rust
//!
//! You can also register Python nodes directly from Rust:
//!
//! ```ignore
//! use remotemedia_python_nodes::{register_python_node, PythonNodeConfig};
//!
//! // Register by node type (Python class must be importable)
//! register_python_node(PythonNodeConfig::new("MyPythonNode"));
//!
//! // Register with full configuration
//! register_python_node(
//!     PythonNodeConfig::new("MyTTSNode")
//!         .with_multi_output(true)
//!         .with_category("tts")
//!         .accepts(["text"])
//!         .produces(["audio"])
//! );
//! ```

#[cfg(feature = "inprocess")]
mod inprocess_node;
mod provider;
mod registry;

pub use provider::PythonNodesProvider;
pub use registry::{
    clear_registry, get_registered_nodes, register_default_python_nodes, register_python_node,
    PythonExecutionMode, PythonNodeConfig, PythonNodeRegistry, PYTHON_NODE_REGISTRY,
};

// Re-export the provider trait for convenience
pub use remotemedia_core::nodes::NodeProvider;
