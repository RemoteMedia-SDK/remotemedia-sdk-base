//! Python FFI functions for calling Rust runtime from Python
//!
//! This module provides the bridge between Python and Rust, allowing
//! Python code to execute pipelines using the Rust runtime.
//!
//! Uses PipelineExecutor from core for transport-agnostic execution.
//! (Migrated from PipelineRunner per spec 026)

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

/// Execute one unary request with the real session's transport copy authority.
/// All supplied inputs are validated; the historical API executes only the first.
async fn execute_unary_python(
    manifest_json: String,
    input_data: Vec<Py<PyAny>>,
    enable_metrics: bool,
    pipeline_buffers: Option<crate::python::buffer_config::PipelineBufferConfig>,
) -> PyResult<Py<PyAny>> {
    let manifest: Manifest = serde_json::from_str(&manifest_json).map_err(|e| {
        pyo3::exceptions::PyValueError::new_err(format!("Failed to parse manifest: {}", e))
    })?;
    let config = pipeline_buffers
        .as_ref()
        .map(crate::python::buffer_config::PipelineBufferConfig::to_executor_config)
        .transpose()?
        .unwrap_or_default();
    let executor = PipelineExecutor::with_config(config).map_err(map_runtime_error)?;
    for factory in crate::plugins::collect_registered_factories() {
        executor.register_factory(factory).await;
    }
    let mut session = executor
        .create_session(Arc::new(manifest))
        .await
        .map_err(map_runtime_error)?;
    let copies = session.transport_copy_context();
    let node_copies = session.node_materialization_copies();
    let result = async {
        let inputs = Python::attach(|py| {
            input_data
                .iter()
                .map(|obj| {
                    crate::marshal::python_to_runtime_data_with_context(py, obj.bind(py), &copies)
                })
                .collect::<PyResult<Vec<_>>>()
        })?;
        let input = inputs
            .into_iter()
            .next()
            .unwrap_or_else(|| RuntimeData::Text(String::new()));
        session
            .send_input(TransportData::new(input))
            .await
            .map_err(map_runtime_error)?;
        session.signal_input_complete();
        session
            .recv_output()
            .await
            .map_err(map_runtime_error)?
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("No output from pipeline"))
    }
    .await;
    // Join cleanup even when input conversion or execution failed. Preserve the
    // original operation error, and surface cleanup failures on successful runs.
    let closed = session.close().await.map_err(map_runtime_error);
    let joined = session.wait().await.map_err(map_runtime_error);
    let output = result?;
    closed?;
    joined?;
    Python::attach(|py| {
        let outputs = crate::marshal::runtime_data_to_python_with_context(
            py,
            &output.data.into_shared(),
            &copies,
        )?;
        if !enable_metrics {
            return Ok(outputs);
        }
        let dict = pyo3::types::PyDict::new(py);
        dict.set_item("outputs", outputs)?;
        dict.set_item("metrics", "{}")?;
        // Includes output decoding above; this is the instrumented transport
        // ledger, not a claim about arbitrary model or device internals.
        let report = serde_json::to_value(copies.report())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        dict.set_item("copy_report", crate::marshal::json_to_python(py, &report)?)?;
        let nodes = serde_json::to_value(node_copies.reports())
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        dict.set_item(
            "node_copy_reports",
            crate::marshal::json_to_python(py, &nodes)?,
        )?;
        Ok(dict.into_any().unbind())
    })
}

/// Execute a unary pipeline with an empty text input.
/// `pipeline_buffers` configures the same copy policy and budgets as streaming.
/// With metrics enabled, returns outputs, the legacy metrics field, and copy_report.
#[pyfunction]
#[pyo3(signature = (manifest_json, enable_metrics=None, *, pipeline_buffers=None))]
pub fn execute_pipeline(
    py: Python<'_>,
    manifest_json: String,
    enable_metrics: Option<bool>,
    pipeline_buffers: Option<crate::python::buffer_config::PipelineBufferConfig>,
) -> PyResult<Bound<'_, PyAny>> {
    super::inline_python_node::set_task_locals(pyo3_async_runtimes::tokio::get_current_locals(py)?);
    future_into_py(
        py,
        execute_unary_python(
            manifest_json,
            Vec::new(),
            enable_metrics.unwrap_or(false),
            pipeline_buffers,
        ),
    )
}

/// Validate all input items and execute the first (or empty text for an empty list).
/// Owners remain alive through execution; compatibility copies use the session ledger.
#[pyfunction]
#[pyo3(signature = (manifest_json, input_data, enable_metrics=None, *, pipeline_buffers=None))]
pub fn execute_pipeline_with_input<'py>(
    py: Python<'py>,
    manifest_json: String,
    input_data: Vec<Bound<'py, PyAny>>,
    enable_metrics: Option<bool>,
    pipeline_buffers: Option<crate::python::buffer_config::PipelineBufferConfig>,
) -> PyResult<Bound<'py, PyAny>> {
    super::inline_python_node::set_task_locals(pyo3_async_runtimes::tokio::get_current_locals(py)?);
    let inputs = input_data.into_iter().map(Bound::unbind).collect();
    future_into_py(
        py,
        execute_unary_python(
            manifest_json,
            inputs,
            enable_metrics.unwrap_or(false),
            pipeline_buffers,
        ),
    )
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
#[pyo3(signature = (node_instances, input_data=None, enable_metrics=None, *, pipeline_buffers=None))]
pub fn execute_pipeline_with_instances<'py>(
    py: Python<'py>,
    node_instances: Vec<Bound<'py, PyAny>>,
    input_data: Option<Bound<'py, PyAny>>,
    enable_metrics: Option<bool>,
    pipeline_buffers: Option<crate::python::buffer_config::PipelineBufferConfig>,
) -> PyResult<Bound<'py, PyAny>> {
    use super::instance_handler::InstanceExecutor;
    if pipeline_buffers.is_some() {
        return Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "Direct instance execution has no core session; pipeline_buffers requires manifest-based execution",
        ));
    }
    let node_refs: Vec<Py<PyAny>> = node_instances.into_iter().map(Bound::unbind).collect();
    let input = input_data
        .map(Bound::unbind)
        .unwrap_or_else(|| pyo3::types::PyString::new(py, "").into_any().unbind());
    future_into_py(py, async move {
        let executors = node_refs
            .into_iter()
            .enumerate()
            .map(|(i, node)| InstanceExecutor::new(node, format!("instance_{}", i)))
            .collect::<PyResult<Vec<_>>>()?;
        if executors.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Cannot execute empty pipeline",
            ));
        }
        let result = (|| {
            for executor in &executors {
                executor.initialize()?;
            }
            let mut value = input;
            for executor in &executors {
                value = executor.process_python(value)?.ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err(format!(
                        "Node '{}' produced no output",
                        executor.node_id()
                    ))
                })?;
            }
            Ok::<_, PyErr>(value)
        })();
        // Visit every initialized node, even when processing or another cleanup
        // fails. Drop sees the cleared cleanup claim and cannot call it twice.
        let mut cleanup_error = None;
        for executor in executors.iter().rev() {
            if let Err(error) = executor.cleanup() {
                cleanup_error.get_or_insert(error);
            }
        }
        let output = result?;
        if let Some(error) = cleanup_error {
            return Err(error);
        }
        Python::attach(|py| {
            if !enable_metrics.unwrap_or(false) {
                return Ok(output);
            }
            let dict = pyo3::types::PyDict::new(py);
            dict.set_item("outputs", output)?;
            dict.set_item("metrics", "{}")?;
            Ok(dict.into_any().unbind())
        })
    })
}
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
                    accepts=None, produces=None, *, buffer_contract_json=None))]
pub fn register_inline_node_class(
    cls: Bound<'_, PyAny>,
    node_type: Option<String>,
    multi_output: bool,
    description: Option<String>,
    category: Option<String>,
    accepts: Option<Vec<String>>,
    produces: Option<Vec<String>>,
    buffer_contract_json: Option<String>,
) -> PyResult<String> {
    let resolved_type = match node_type {
        Some(t) => t,
        None => cls.getattr("__name__")?.extract::<String>().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("cls has no __name__: {}", e))
        })?,
    };
    let buffer_contract = buffer_contract_json
        .as_deref()
        .map(|json| super::inline_python_node::parse_buffer_contract(&cls, json))
        .transpose()?;
    let config = super::inline_python_node::InlineNodeConfig {
        node_type: resolved_type.clone(),
        cls: cls.unbind(),
        multi_output,
        description,
        category,
        accepts: accepts.unwrap_or_default(),
        produces: produces.unwrap_or_default(),
        buffer_contract,
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
