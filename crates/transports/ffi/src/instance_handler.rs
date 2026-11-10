//! Python Node Instance Handler
//!
//! This module provides support for executing Python Node instances directly
//! in the Rust runtime, bypassing JSON manifest serialization.
//!
//! # Architecture
//!
//! - **InstanceExecutor**: Wraps a Python Node instance (Py<PyAny>) and provides
//!   a Rust-friendly execution interface
//! - **Lifecycle Management**: Calls Python's initialize(), process(), cleanup() methods
//! - **GIL Management**: Uses Python::attach() for all Python method calls
//! - **Memory Safety**: Py<PyAny> handles reference counting automatically
//!
//! # Usage
//!
//! ```
//! use pyo3::prelude::*;
//! use pyo3::ffi::c_str;
//! use remotemedia_ffi::instance_handler::InstanceExecutor;
//!
//! Python::attach(|py| {
//!     // InstanceExecutor wraps a Python Node instance for lifecycle management
//!     // Create a node class and instance:
//!     let code = c_str!(r#"
//! class TestNode:
//!     def initialize(self): pass
//!     def process(self, data): return data
//!     def cleanup(self): pass
//! "#);
//!     py.run(code, None, None).unwrap();
//!     let node = py.eval(c_str!("TestNode()"), None, None).unwrap();
//!     let executor = InstanceExecutor::new(node.unbind(), "test_node".to_string()).unwrap();
//! });
//! ```

use pyo3::prelude::*;
use remotemedia_core::data::RuntimeData;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{debug, error, warn};

/// Wrapper for executing Python Node instances from Rust
///
/// This struct holds a reference to a Python Node instance and provides
/// methods to call its lifecycle methods (initialize, process, cleanup)
/// from Rust code using PyO3.
///
/// # Thread Safety
///
/// `Py<PyAny>` implements `Send`, making this struct safe to pass between
/// async tasks. However, all Python method calls must acquire the GIL via
/// `Python::attach()`.
pub struct InstanceExecutor {
    /// Python Node instance reference (GIL-independent)
    node_instance: Py<PyAny>,

    /// Node identifier for logging/debugging
    node_id: String,

    /// Whether this node supports streaming execution
    is_streaming: bool,
    /// Claim cleanup before attempting initialization; never retry cleanup on drop.
    cleanup_pending: AtomicBool,
}

impl InstanceExecutor {
    /// Create a new instance executor from a Python Node object
    ///
    /// # Arguments
    ///
    /// * `node_instance` - Python Node instance (Py<PyAny>)
    /// * `node_id` - Unique identifier for this node
    ///
    /// # Returns
    ///
    /// `PyResult<InstanceExecutor>` - Executor if validation passes
    ///
    /// # Errors
    ///
    /// Returns PyErr if:
    /// - Node instance is missing required `process` method
    /// - Node instance is missing required `initialize` method
    ///
    /// # Example
    ///
    /// ```
    /// use pyo3::prelude::*;
    /// use pyo3::ffi::c_str;
    /// use remotemedia_ffi::instance_handler::InstanceExecutor;
    ///
    /// Python::attach(|py| {
    ///     // Create a Python node with required methods
    ///     let code = c_str!(r#"
    /// class TestNode:
    ///     def initialize(self): pass
    ///     def process(self, data): return data
    /// "#);
    ///     py.run(code, None, None).unwrap();
    ///     let node = py.eval(c_str!("TestNode()"), None, None).unwrap();
    ///     let executor = InstanceExecutor::new(node.unbind(), "test".to_string()).unwrap();
    /// });
    /// ```
    pub fn new(node_instance: Py<PyAny>, node_id: String) -> PyResult<Self> {
        // T005: PyO3 Py<PyAny> storage pattern - node_instance stored directly
        // T009: Validate required methods exist
        Python::attach(|py| {
            let node_ref = node_instance.bind(py);

            let inspect = py.import("inspect")?;
            for name in ["initialize", "process", "cleanup"] {
                let method = node_ref.getattr(name)?;
                if !method.is_callable() {
                    return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                        "Node '{}' {} must be callable",
                        node_id, name
                    )));
                }
                for kind in [
                    "iscoroutinefunction",
                    "isasyncgenfunction",
                    "isgeneratorfunction",
                ] {
                    if inspect.call_method1(kind, (&method,))?.is_truthy()? {
                        return Err(pyo3::exceptions::PyNotImplementedError::new_err(
                            "Direct instance execution supports synchronous methods; use registered inline nodes for async or streaming execution",
                        ));
                    }
                }
            }

            // Detect is_streaming attribute (optional)
            let is_streaming = node_ref
                .getattr("is_streaming")
                .ok()
                .and_then(|v| v.extract::<bool>().ok())
                .unwrap_or(false);

            debug!(
                "Created InstanceExecutor for node: {} (streaming: {})",
                node_id, is_streaming
            );

            Ok(InstanceExecutor {
                node_instance,
                node_id,
                is_streaming,
                cleanup_pending: AtomicBool::new(false),
            })
        })
    }

    /// Initialize the Python Node instance before processing
    ///
    /// Calls the Python `node.initialize()` method with GIL acquired.
    ///
    /// # Returns
    ///
    /// `PyResult<()>` - Ok if initialization succeeds
    ///
    /// # Errors
    ///
    /// Returns PyErr if Python initialize() method raises an exception
    pub fn initialize(&self) -> PyResult<()> {
        if self.cleanup_pending.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        Python::attach(|py| {
            let result = self.node_instance.call_method0(py, "initialize")?;
            require_sync_result(py, result)?;
            Ok(())
        })
    }

    /// Forward an owning Python reference without packing, serialization, or a
    /// RuntimeData conversion. Private arrays retain their Python mutability;
    /// this sequential lane is not an immutable core-session transport.
    pub fn process_python(&self, input: Py<PyAny>) -> PyResult<Option<Py<PyAny>>> {
        Python::attach(|py| {
            let result = self.node_instance.call_method1(py, "process", (input,))?;
            let result = require_sync_result(py, result)?;
            Ok(if result.is_none(py) {
                None
            } else {
                Some(result)
            })
        })
    }

    /// Process input data through the Python Node instance
    ///
    /// Calls the Python `node.process(data)` method with GIL acquired.
    ///
    /// # Arguments
    ///
    /// * `input` - RuntimeData to process
    ///
    /// # Returns
    ///
    /// `PyResult<Vec<RuntimeData>>` - Processed outputs (vec to support multiple outputs)
    ///
    /// # Errors
    ///
    /// Returns PyErr if Python process() method raises an exception
    pub fn process(&self, input: RuntimeData) -> PyResult<Vec<RuntimeData>> {
        // T006, T016: Python::attach() method calling pattern for process()
        Python::attach(|py| {
            // Convert RuntimeData to Python object (requires T007)
            let py_input = super::marshal::runtime_data_into_python(py, input)?;

            // Call process method
            let result = self
                .node_instance
                .call_method1(py, "process", (py_input,))
                .map_err(|e| {
                    error!("Node '{}' process() failed: {}", self.node_id, e);
                    e
                })?;

            // Handle None returns (no output)
            if result.is_none(py) {
                debug!("Node '{}' returned None (no output)", self.node_id);
                return Ok(vec![]);
            }

            // Convert result back to RuntimeData (requires T008)
            let runtime_data = super::marshal::python_to_runtime_data(py, result.bind(py))?;
            debug!("Node '{}' produced output", self.node_id);
            Ok(vec![runtime_data])
        })
    }

    /// Clean up resources held by the Python Node instance
    ///
    /// Calls the Python `node.cleanup()` method with GIL acquired.
    ///
    /// # Returns
    ///
    /// `PyResult<()>` - Ok if cleanup succeeds
    ///
    /// # Errors
    ///
    /// Returns PyErr if Python cleanup() method raises an exception
    pub fn cleanup(&self) -> PyResult<()> {
        if !self.cleanup_pending.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        // T006, T017: Python::attach() method calling pattern for cleanup()
        Python::attach(|py| {
            let result = self
                .node_instance
                .call_method0(py, "cleanup")
                .map_err(|e| {
                    warn!("Failed to cleanup node '{}': {}", self.node_id, e);
                    e
                })?;
            require_sync_result(py, result)?;

            debug!("Cleaned up node: {}", self.node_id);
            Ok(())
        })
    }

    /// Get the node ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Check if this node supports streaming
    pub fn is_streaming(&self) -> bool {
        self.is_streaming
    }
}

/// Catch dynamically returned async work as well as async methods found at construction.
fn require_sync_result(py: Python<'_>, result: Py<PyAny>) -> PyResult<Py<PyAny>> {
    let inspect = py.import("inspect")?;
    for kind in ["isawaitable", "isasyncgen", "isgenerator"] {
        if inspect
            .call_method1(kind, (result.bind(py),))?
            .is_truthy()?
        {
            if inspect
                .call_method1("iscoroutine", (result.bind(py),))?
                .is_truthy()?
            {
                result.call_method0(py, "close")?;
            }
            return Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "Direct instance execution requires synchronous results; async and generator results are unsupported",
            ));
        }
    }
    Ok(result)
}

impl Drop for InstanceExecutor {
    /// Ensure cleanup is called when InstanceExecutor is dropped
    ///
    /// # Note
    ///
    /// Py<PyAny> automatically decrements Python refcount when dropped.
    /// We explicitly call cleanup() to release Python-side resources.
    fn drop(&mut self) {
        // T018: Drop trait implementation for cleanup
        if let Err(e) = self.cleanup() {
            warn!("Cleanup during drop failed for '{}': {}", self.node_id, e);
        }

        debug!("Dropping InstanceExecutor for node: {}", self.node_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stub() {
        // TODO: Add tests in T022
        assert!(true);
    }
}
