//! Python bindings for GrpcServer.
//! Exposes a class allowing users to host the gRPC control plane and pipeline execution service.

use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use remotemedia_core::transport::PipelineExecutor;
use remotemedia_grpc::{GrpcServer as CoreGrpcServer, ServiceConfig};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

/// gRPC Server for hosting the RemoteMedia control plane, streaming, and execution services.
#[pyclass(name = "GrpcServer")]
pub struct PyGrpcServer {
    bind_address: String,
    running: Arc<AtomicBool>,
    shutdown_flag: Arc<AtomicBool>,
    handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

#[pymethods]
impl PyGrpcServer {
    /// Create a new gRPC server.
    ///
    /// Args:
    ///     bind_address: Socket address to bind to (e.g. "0.0.0.0:50051").
    #[new]
    #[pyo3(signature = (bind_address = "0.0.0.0:50051"))]
    fn new(bind_address: &str) -> Self {
        Self {
            bind_address: bind_address.to_string(),
            running: Arc::new(AtomicBool::new(false)),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            handle: Arc::new(Mutex::new(None)),
        }
    }

    /// Start the gRPC server asynchronously.
    fn start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let running = self.running.clone();
        let shutdown_flag = self.shutdown_flag.clone();
        let handle_lock = self.handle.clone();
        let bind_address = self.bind_address.clone();

        future_into_py(py, async move {
            if running.load(Ordering::SeqCst) {
                return Ok(());
            }

            let executor = PipelineExecutor::new().map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to build PipelineExecutor: {}",
                    e
                ))
            })?;

            #[cfg(feature = "python")]
            // Register all custom Python nodes and FFI dynamic library factories
            for factory in crate::plugins::collect_registered_factories() {
                executor.register_factory(factory).await;
            }

            let executor = Arc::new(executor);
            let mut grpc_config = ServiceConfig::default();
            grpc_config.bind_address = bind_address;

            let server = CoreGrpcServer::new(grpc_config, executor).map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to construct GrpcServer: {}",
                    e
                ))
            })?;

            running.store(true, Ordering::SeqCst);
            shutdown_flag.store(false, Ordering::SeqCst);
            let shutdown_clone = shutdown_flag.clone();

            let handle = tokio::spawn(async move {
                if let Err(e) = server.serve_with_shutdown_flag(shutdown_clone).await {
                    tracing::error!("gRPC server error: {}", e);
                }
            });

            *handle_lock.lock().await = Some(handle);
            Ok(())
        })
    }

    /// Gracefully shutdown the gRPC server.
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let running = self.running.clone();
        let shutdown_flag = self.shutdown_flag.clone();
        let handle_lock = self.handle.clone();

        future_into_py(py, async move {
            running.store(false, Ordering::SeqCst);
            shutdown_flag.store(true, Ordering::SeqCst);
            if let Some(h) = handle_lock.lock().await.take() {
                let _ = h.await;
            }
            Ok(())
        })
    }

    /// Context manager support: async with GrpcServer(...)
    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let server = slf.clone_ref(py);
        future_into_py(py, async move {
            let start_fut = Python::attach(|py| {
                let s = server.bind(py);
                s.borrow().start(py).map(|b| b.unbind())
            })?;

            let rust_fut = Python::attach(|py| {
                let bound = start_fut.bind(py);
                pyo3_async_runtimes::tokio::into_future(bound.clone())
            })?;

            rust_fut.await?;
            Ok(server)
        })
    }

    /// Context manager support: cleanup on exit
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Bound<'py, PyAny>>,
        _exc_val: Option<Bound<'py, PyAny>>,
        _exc_tb: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let shutdown_fut = self.shutdown(py)?.unbind();
        future_into_py(py, async move {
            let rust_fut = Python::attach(|py| {
                let bound = shutdown_fut.bind(py);
                pyo3_async_runtimes::tokio::into_future(bound.clone())
            })?;

            rust_fut.await?;
            Ok(false) // Don't suppress exceptions
        })
    }
}
