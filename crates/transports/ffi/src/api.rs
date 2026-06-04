//! Python FFI functions for calling Rust runtime from Python
//!
//! This module provides the bridge between Python and Rust, allowing
//! Python code to execute pipelines using the Rust runtime.
//!
//! Uses PipelineExecutor from core for transport-agnostic execution.
//! (Migrated from PipelineRunner per spec 026)

use super::marshal::{python_to_runtime_data, runtime_data_to_python};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use remotemedia_core::{
    data::RuntimeData,
    manifest::Manifest,
    transport::{PipelineExecutor, TransportData},
};
use std::sync::Arc;

/// Map runtime errors to appropriate Python exceptions
///
/// Provides consistent error handling across FFI, with special handling for:
/// - Validation errors -> ValueError with structured error details
/// - Manifest errors -> ValueError
/// - Execution errors -> RuntimeError
fn map_runtime_error(e: remotemedia_core::Error) -> PyErr {
    match e {
        remotemedia_core::Error::Validation(ref validation_errors) => {
            // Format validation errors as structured JSON for Python consumers
            let errors_json =
                serde_json::to_string_pretty(validation_errors).unwrap_or_else(|_| e.to_string());
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Parameter validation failed ({} error(s)):\n{}",
                validation_errors.len(),
                errors_json
            ))
        }
        remotemedia_core::Error::Manifest(msg) | remotemedia_core::Error::InvalidManifest(msg) => {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid manifest: {}", msg))
        }
        remotemedia_core::Error::InvalidData(msg) => {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("Invalid data: {}", msg))
        }
        remotemedia_core::Error::InvalidInput {
            message, node_id, ..
        } => PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Invalid input for node '{}': {}",
            node_id, message
        )),
        _ => PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Execution failed: {}", e)),
    }
}

/// Execute a pipeline from a JSON manifest
///
/// # Arguments
/// * `manifest_json` - JSON string containing the pipeline manifest
///
/// # Returns
/// Python coroutine that resolves to execution results
///
/// # Example (Python)
/// ```python
/// import asyncio
/// from remotemedia._remotemedia_runtime import execute_pipeline
///
/// async def main():
///     manifest = '{"version": "v1", ...}'
///     results = await execute_pipeline(manifest)
///     print(results)
///
/// asyncio.run(main())
/// ```
#[pyfunction]
pub fn execute_pipeline(
    py: Python<'_>,
    manifest_json: String,
    enable_metrics: Option<bool>,
) -> PyResult<Bound<'_, PyAny>> {
    // Capture the asyncio loop / context so inline-async dispatch deep
    // inside the executor can find them (session-router tokio spawns
    // don't inherit task-locals). Must happen on the calling Python
    // thread, before `future_into_py` moves us into tokio.
    super::inline_python_node::set_task_locals(pyo3_async_runtimes::tokio::get_current_locals(py)?);
    future_into_py(py, async move {
        // Parse manifest
        let manifest: Manifest = serde_json::from_str(&manifest_json).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Failed to parse manifest: {}",
                e
            ))
        })?;
        let manifest = Arc::new(manifest);

        // Create PipelineExecutor (spec 026 migration)
        let executor = PipelineExecutor::new().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create executor: {}",
                e
            ))
        })?;

        // Execute using PipelineExecutor (no input data for basic execution)
        let input = TransportData::new(RuntimeData::Text(String::new()));
        let output = executor
            .execute_unary(manifest, input)
            .await
            .map_err(map_runtime_error)?;

        // Convert output to Python
        Python::attach(|py| {
            // Use runtime_data_to_python for direct conversion (zero-copy for numpy!)
            // This avoids JSON serialization and converts RuntimeData::Numpy directly to numpy arrays
            let outputs_py = runtime_data_to_python(py, &output.data)?;

            // Include metrics if requested - now includes scheduler metrics from PipelineExecutor
            if enable_metrics.unwrap_or(false) {
                let dict = pyo3::types::PyDict::new(py);
                dict.set_item("outputs", &outputs_py)?;
                dict.set_item("metrics", "{}")?; // Placeholder - metrics exposed via executor.prometheus_metrics()
                Ok(dict.into_any().unbind())
            } else {
                Ok(outputs_py) // runtime_data_to_python already returns PyObject (unbound)
            }
        })
    })
}

/// Execute a pipeline with input data
///
/// # Arguments
/// * `manifest_json` - JSON string containing the pipeline manifest
/// * `input_data` - List of input items to process
///
/// # Returns
/// Python coroutine that resolves to list of results
///
/// # Example (Python)
/// ```python
/// manifest = pipeline.serialize()
/// results = await execute_pipeline_with_input(manifest, [1, 2, 3])
/// ```
#[pyfunction]
pub fn execute_pipeline_with_input<'py>(
    py: Python<'py>,
    manifest_json: String,
    input_data: Vec<Bound<'py, PyAny>>,
    enable_metrics: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    // Capture asyncio task-locals for inline-async dispatch (see
    // `execute_pipeline` for rationale).
    super::inline_python_node::set_task_locals(pyo3_async_runtimes::tokio::get_current_locals(py)?);

    // Convert input_data to RuntimeData directly (zero-copy for numpy!)
    let rust_input: Vec<RuntimeData> = input_data
        .iter()
        .map(|obj| python_to_runtime_data(py, obj))
        .collect::<PyResult<Vec<_>>>()?;

    future_into_py(py, async move {
        // Parse manifest
        let manifest: Manifest = serde_json::from_str(&manifest_json).map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Failed to parse manifest: {}",
                e
            ))
        })?;
        let manifest = Arc::new(manifest);

        // Create PipelineExecutor (spec 026 migration)
        let executor = PipelineExecutor::new().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to create executor: {}",
                e
            ))
        })?;

        // Use first input item or empty text
        let input_data = if let Some(first) = rust_input.first() {
            first.clone()
        } else {
            RuntimeData::Text(String::new())
        };

        // Execute using PipelineExecutor - uses map_runtime_error for proper validation error handling
        let input = TransportData::new(input_data);
        let output = executor
            .execute_unary(manifest, input)
            .await
            .map_err(map_runtime_error)?;

        // Convert output to Python
        Python::attach(|py| {
            // Use runtime_data_to_python for direct conversion (zero-copy for numpy!)
            // This avoids JSON serialization and converts RuntimeData::Numpy directly to numpy arrays
            let outputs_py = runtime_data_to_python(py, &output.data)?;

            // Include metrics if requested - now includes scheduler metrics from PipelineExecutor
            if enable_metrics.unwrap_or(false) {
                let dict = pyo3::types::PyDict::new(py);
                dict.set_item("outputs", &outputs_py)?;
                dict.set_item("metrics", "{}")?; // Placeholder - metrics exposed via executor.prometheus_metrics()
                Ok(dict.into_any().unbind())
            } else {
                Ok(outputs_py) // runtime_data_to_python already returns PyObject (unbound)
            }
        })
    })
}

/// Execute pipeline directly with Python Node instances (Feature 011)
///
/// This function bypasses the node registry and executes Node instances
/// directly using InstanceExecutor. This enables custom Python nodes to
/// run without registration.
///
/// # Arguments
/// * `node_instances` - List of Python Node instances to execute in sequence
/// * `input_data` - Optional input data for the first node
///
/// # Returns
/// Python coroutine that resolves to execution results
#[pyfunction]
pub fn execute_pipeline_with_instances<'py>(
    py: Python<'py>,
    node_instances: Vec<Bound<'py, PyAny>>,
    input_data: Option<Bound<'py, PyAny>>,
    enable_metrics: Option<bool>,
) -> PyResult<Bound<'py, PyAny>> {
    use super::instance_handler::InstanceExecutor;
    use super::marshal::{python_to_runtime_data, runtime_data_to_python};

    // Convert to Py<PyAny> before async block (Bound is not Send)
    let node_refs: Vec<Py<PyAny>> = node_instances
        .into_iter()
        .map(|node| node.unbind())
        .collect();

    let input_ref: Option<RuntimeData> = if let Some(input) = input_data {
        Some(python_to_runtime_data(py, &input)?)
    } else {
        None
    };

    future_into_py(py, async move {
        // Convert node instances to InstanceExecutor wrappers
        let executors: Vec<InstanceExecutor> = node_refs
            .into_iter()
            .enumerate()
            .map(|(i, node)| {
                let node_id = format!("instance_{}", i);
                InstanceExecutor::new(node, node_id)
            })
            .collect::<PyResult<Vec<_>>>()?;

        if executors.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Cannot execute empty pipeline",
            ));
        }

        // Initialize all nodes
        for executor in &executors {
            executor.initialize()?;
        }

        // Get initial input data
        let mut current_data = input_ref.unwrap_or(RuntimeData::Text(String::new()));

        // Execute nodes in sequence
        for executor in &executors {
            let outputs = executor.process(current_data)?;

            // Take first output as input for next node
            current_data = outputs.into_iter().next().ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Node '{}' produced no output",
                    executor.node_id()
                ))
            })?;
        }

        // Cleanup all nodes
        for executor in &executors {
            executor.cleanup()?;
        }

        // Convert final output to Python
        Python::attach(|py| {
            let py_output = runtime_data_to_python(py, &current_data)?;

            if enable_metrics.unwrap_or(false) {
                let dict = pyo3::types::PyDict::new(py);
                dict.set_item("outputs", py_output)?;
                dict.set_item("metrics", "{}")?;
                Ok(dict.into())
            } else {
                Ok(py_output)
            }
        })
    })
}

/// Run the reusable RemoteMedia benchmark harness from Python.
///

/// Get runtime version information
#[pyfunction]
pub fn get_runtime_version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}

/// Check if Rust runtime is available
#[pyfunction]
pub fn is_available() -> bool {
    true
}

// ─────────────────────────────────────────────────────────────────────
// Late node registration (Design B + inline support)
//
// Three bindings:
//   - register_inline_node_class(cls, ...)       → in-process via PyO3
//   - register_python_node_type(node_type, ...)  → multiprocess via subprocess import
//   - unregister_node_type(node_type)            → remove from either registry
//   - list_registered_node_types()               → all known types
// ─────────────────────────────────────────────────────────────────────

/// Register a Python class as an inline node — instantiated and called
/// in-process via PyO3. Use this for classes defined inline / in
/// `__main__` / in a REPL where the multiprocess subprocess can't
/// import them by dotted path.
///
/// The class must be a subclass of `remotemedia.core.node.Node` (the
/// PyO3 layer does no type check; the runtime calls `cls(**params)`
/// then `instance.process(data)`).
#[pyfunction]
#[pyo3(signature = (cls, node_type=None, multi_output=false,
                    description=None, category=None,
                    accepts=None, produces=None))]
pub fn register_inline_node_class(
    cls: Bound<'_, PyAny>,
    node_type: Option<String>,
    multi_output: bool,
    description: Option<String>,
    category: Option<String>,
    accepts: Option<Vec<String>>,
    produces: Option<Vec<String>>,
) -> PyResult<String> {
    let resolved_type = match node_type {
        Some(t) => t,
        None => cls.getattr("__name__")?.extract::<String>().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("cls has no __name__: {}", e))
        })?,
    };
    let config = super::inline_python_node::InlineNodeConfig {
        node_type: resolved_type.clone(),
        cls: cls.unbind(),
        multi_output,
        description,
        category,
        accepts: accepts.unwrap_or_default(),
        produces: produces.unwrap_or_default(),
    };
    super::inline_python_node::register_inline_node(config);
    Ok(resolved_type)
}

/// Register a Python class as a dotted-path node — instantiated in a
/// multiprocess subprocess via `importlib.import_module`. The class
/// **must** be importable from the subprocess's `PYTHONPATH`.
///
/// Use this for classes defined in modules on disk. Heavy-compute
/// nodes benefit from this path because each instance gets its own
/// process / GIL.
#[pyfunction]
#[pyo3(signature = (node_type, python_class=None, multi_output=false,
                    description=None, category=None,
                    accepts=None, produces=None))]
pub fn register_python_node_type(
    node_type: String,
    python_class: Option<String>,
    multi_output: bool,
    description: Option<String>,
    category: Option<String>,
    accepts: Option<Vec<String>>,
    produces: Option<Vec<String>>,
) -> PyResult<()> {
    use remotemedia_python_nodes::{register_python_node, PythonNodeConfig};
    let mut cfg = PythonNodeConfig::new(node_type.clone())
        .with_python_class(python_class.unwrap_or_else(|| node_type.clone()))
        .with_multi_output(multi_output);
    if let Some(c) = category {
        cfg = cfg.with_category(c);
    }
    if let Some(d) = description {
        cfg = cfg.with_description(d);
    }
    if let Some(a) = accepts {
        cfg = cfg.accepts(a);
    }
    if let Some(p) = produces {
        cfg = cfg.produces(p);
    }
    register_python_node(cfg);
    Ok(())
}

/// Remove a node type from both inline and dotted-path registries.
/// Returns true if anything was removed.
#[pyfunction]
pub fn unregister_node_type(node_type: &str) -> PyResult<bool> {
    let inline = super::inline_python_node::unregister_inline_node(node_type);
    let py = remotemedia_python_nodes::PYTHON_NODE_REGISTRY.remove(node_type);
    Ok(inline || py)
}

/// List every node type currently visible to the Rust runtime: built-in
/// Rust nodes, built-in Python multiprocess nodes, late-registered
/// dotted-path nodes, and inline nodes. Returns a Python coroutine that
/// resolves to a sorted, deduplicated list of node type names.
#[pyfunction]
pub fn list_registered_node_types(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    future_into_py(py, async move {
        use remotemedia_core::transport::PipelineExecutor;
        let executor = PipelineExecutor::new().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Failed to construct executor: {}",
                e
            ))
        })?;
        let mut types = executor.list_node_types().await;
        types.sort();
        types.dedup();
        Ok(types)
    })
}

/// Python module initialization
///
/// This is now defined in lib.rs to properly set up the PyO3 module structure

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        let version = get_runtime_version();
        assert!(!version.is_empty());
    }

    #[test]
    fn test_availability() {
        assert!(is_available());
    }
}
