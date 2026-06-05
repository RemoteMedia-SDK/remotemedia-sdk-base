//! WebRtcServer Python binding
//!
//! Implements the Python WebRtcServer class with decorator-based callbacks
//! using Py<PyAny> for callback storage and Python::attach() for invocation.

use super::config::{PeerInfo, SessionInfo, WebRtcServerConfig};
use super::events::{
    DataReceivedEvent, ErrorEvent, PeerConnectedEvent, PeerDisconnectedEvent, PipelineOutputEvent,
    SessionEvent,
};
use crate::webrtc::core::WebRtcServerCore;
use crate::webrtc::events::WebRtcEvent;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict};
use pyo3::IntoPyObjectExt;
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

/// WebRTC server for Python
///
/// Provides a WebRTC media server with embedded or external signaling.
/// Uses decorator-based callback registration for Pythonic API.
#[pyclass]
pub struct WebRtcServer {
    /// Core server implementation
    core: Arc<WebRtcServerCore>,

    /// Peer connected callbacks
    on_peer_connected_callbacks: Arc<Mutex<Vec<Py<PyAny>>>>,

    /// Peer disconnected callbacks
    on_peer_disconnected_callbacks: Arc<Mutex<Vec<Py<PyAny>>>>,

    /// Pipeline output callbacks
    on_pipeline_output_callbacks: Arc<Mutex<Vec<Py<PyAny>>>>,

    /// Data received callbacks
    on_data_callbacks: Arc<Mutex<Vec<Py<PyAny>>>>,

    /// Error callbacks
    on_error_callbacks: Arc<Mutex<Vec<Py<PyAny>>>>,

    /// Session event callbacks
    on_session_callbacks: Arc<Mutex<Vec<Py<PyAny>>>>,

    /// Event loop running flag
    event_loop_running: Arc<AtomicBool>,

    /// Event loop task handle
    event_loop_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

#[pymethods]
impl WebRtcServer {
    /// Create a new WebRTC server with embedded signaling
    ///
    /// Args:
    ///     config: Server configuration with port set
    ///
    /// Returns:
    ///     WebRtcServer instance
    #[staticmethod]
    fn create(py: Python<'_>, config: WebRtcServerConfig) -> PyResult<Bound<'_, PyAny>> {
        future_into_py(py, async move {
            let core_config = config.to_core_config()?;

            if core_config.port.is_none() {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Port is required for embedded signaling. Use connect() for external signaling.",
                ));
            }

            let core = WebRtcServerCore::new(core_config)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            Python::attach(|py| {
                Py::new(
                    py,
                    WebRtcServer {
                        core: Arc::new(core),
                        on_peer_connected_callbacks: Arc::new(Mutex::new(Vec::new())),
                        on_peer_disconnected_callbacks: Arc::new(Mutex::new(Vec::new())),
                        on_pipeline_output_callbacks: Arc::new(Mutex::new(Vec::new())),
                        on_data_callbacks: Arc::new(Mutex::new(Vec::new())),
                        on_error_callbacks: Arc::new(Mutex::new(Vec::new())),
                        on_session_callbacks: Arc::new(Mutex::new(Vec::new())),
                        event_loop_running: Arc::new(AtomicBool::new(false)),
                        event_loop_handle: Arc::new(Mutex::new(None)),
                    },
                )
            })
        })
    }

    /// Connect to an external signaling server
    ///
    /// Args:
    ///     config: Server configuration with signaling_url set
    ///
    /// Returns:
    ///     WebRtcServer instance
    #[staticmethod]
    fn connect(py: Python<'_>, config: WebRtcServerConfig) -> PyResult<Bound<'_, PyAny>> {
        future_into_py(py, async move {
            let core_config = config.to_core_config()?;

            if core_config.signaling_url.is_none() {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "signaling_url is required for external signaling. Use create() for embedded signaling.",
                ));
            }

            let core = WebRtcServerCore::new(core_config)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            Python::attach(|py| {
                Py::new(
                    py,
                    WebRtcServer {
                        core: Arc::new(core),
                        on_peer_connected_callbacks: Arc::new(Mutex::new(Vec::new())),
                        on_peer_disconnected_callbacks: Arc::new(Mutex::new(Vec::new())),
                        on_pipeline_output_callbacks: Arc::new(Mutex::new(Vec::new())),
                        on_data_callbacks: Arc::new(Mutex::new(Vec::new())),
                        on_error_callbacks: Arc::new(Mutex::new(Vec::new())),
                        on_session_callbacks: Arc::new(Mutex::new(Vec::new())),
                        event_loop_running: Arc::new(AtomicBool::new(false)),
                        event_loop_handle: Arc::new(Mutex::new(None)),
                    },
                )
            })
        })
    }

    /// Get the server ID
    #[getter]
    fn id(&self) -> String {
        self.core.id().to_string()
    }

    /// Get the current server state
    fn get_state<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        future_into_py(py, async move {
            let state = core.state().await;
            let state_str = match state {
                crate::webrtc::config::ServerState::Created => "created",
                crate::webrtc::config::ServerState::Starting => "starting",
                crate::webrtc::config::ServerState::Running => "running",
                crate::webrtc::config::ServerState::Stopping => "stopping",
                crate::webrtc::config::ServerState::Stopped => "stopped",
            };
            Ok(state_str.to_string())
        })
    }

    /// Get connected peers
    fn get_peers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        future_into_py(py, async move {
            let peers = core.peers().await;
            let peer_infos: Vec<PeerInfo> = peers.into_values().map(Into::into).collect();
            Python::attach(|py| {
                let list = pyo3::types::PyList::empty(py);
                for peer in peer_infos {
                    list.append(Py::new(py, peer)?)?;
                }
                Ok(list.unbind())
            })
        })
    }

    /// Get active sessions
    fn get_sessions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        future_into_py(py, async move {
            let sessions = core.sessions().await;
            let session_infos: Vec<SessionInfo> = sessions.into_values().map(Into::into).collect();
            Python::attach(|py| {
                let list = pyo3::types::PyList::empty(py);
                for session in session_infos {
                    list.append(Py::new(py, session)?)?;
                }
                Ok(list.unbind())
            })
        })
    }

    /// Register a callback for peer connected events (decorator pattern)
    ///
    /// Usage:
    ///     @server.on_peer_connected
    ///     async def handle_peer(event: PeerConnectedEvent):
    ///         print(f"Peer {event.peer_id} connected")
    fn on_peer_connected(&self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let callbacks = self.on_peer_connected_callbacks.clone();
        let callback_clone = callback.clone_ref(py);
        tokio::spawn(async move {
            callbacks.lock().await.push(callback_clone);
        });
        // Return the callback for decorator chaining
        Ok(callback)
    }

    /// Register a callback for peer disconnected events (decorator pattern)
    fn on_peer_disconnected(&self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let callbacks = self.on_peer_disconnected_callbacks.clone();
        let callback_clone = callback.clone_ref(py);
        tokio::spawn(async move {
            callbacks.lock().await.push(callback_clone);
        });
        Ok(callback)
    }

    /// Register a callback for pipeline output events (decorator pattern)
    fn on_pipeline_output(&self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let callbacks = self.on_pipeline_output_callbacks.clone();
        let callback_clone = callback.clone_ref(py);
        tokio::spawn(async move {
            callbacks.lock().await.push(callback_clone);
        });
        Ok(callback)
    }

    /// Register a callback for raw data events (decorator pattern)
    fn on_data(&self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let callbacks = self.on_data_callbacks.clone();
        let callback_clone = callback.clone_ref(py);
        tokio::spawn(async move {
            callbacks.lock().await.push(callback_clone);
        });
        Ok(callback)
    }

    /// Register a callback for error events (decorator pattern)
    fn on_error(&self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let callbacks = self.on_error_callbacks.clone();
        let callback_clone = callback.clone_ref(py);
        tokio::spawn(async move {
            callbacks.lock().await.push(callback_clone);
        });
        Ok(callback)
    }

    /// Register a callback for session events (decorator pattern)
    fn on_session(&self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let callbacks = self.on_session_callbacks.clone();
        let callback_clone = callback.clone_ref(py);
        tokio::spawn(async move {
            callbacks.lock().await.push(callback_clone);
        });
        Ok(callback)
    }

    /// Start the server
    ///
    /// For embedded mode: binds to the configured port
    /// For external mode: connects to the signaling server
    fn start<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let event_loop_running = self.event_loop_running.clone();
        let event_loop_handle = self.event_loop_handle.clone();
        let on_peer_connected = self.on_peer_connected_callbacks.clone();
        let on_peer_disconnected = self.on_peer_disconnected_callbacks.clone();
        let on_pipeline_output = self.on_pipeline_output_callbacks.clone();
        let on_data = self.on_data_callbacks.clone();
        let on_error = self.on_error_callbacks.clone();
        let on_session = self.on_session_callbacks.clone();

        future_into_py(py, async move {
            // Start the core server
            core.start()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            // Start event loop if not already running
            if !event_loop_running.load(Ordering::SeqCst) {
                // Take the event receiver
                let event_rx = core.take_event_receiver().await.ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("Event receiver already taken")
                })?;

                event_loop_running.store(true, Ordering::SeqCst);

                // Spawn the event loop
                let running = event_loop_running.clone();
                let handle = tokio::spawn(async move {
                    run_event_loop(
                        event_rx,
                        running,
                        on_peer_connected,
                        on_peer_disconnected,
                        on_pipeline_output,
                        on_data,
                        on_error,
                        on_session,
                    )
                    .await;
                });

                *event_loop_handle.lock().await = Some(handle);
            }

            Ok(())
        })
    }

    /// Stop the server gracefully
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let event_loop_running = self.event_loop_running.clone();
        let event_loop_handle = self.event_loop_handle.clone();

        future_into_py(py, async move {
            // Stop event loop
            event_loop_running.store(false, Ordering::SeqCst);

            // Wait for event loop to finish
            if let Some(handle) = event_loop_handle.lock().await.take() {
                let _ = handle.await;
            }

            // Shutdown core
            core.shutdown()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            Ok(())
        })
    }

    /// Send data to a specific peer
    ///
    /// Args:
    ///     peer_id: Target peer ID
    ///     data: Data to send (bytes)
    fn send_to_peer<'py>(
        &self,
        py: Python<'py>,
        peer_id: String,
        data: Bound<'py, PyBytes>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let data_vec = data.as_bytes().to_vec();

        future_into_py(py, async move {
            core.send_to_peer(&peer_id, data_vec)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Broadcast data to all connected peers
    ///
    /// Args:
    ///     data: Data to broadcast (bytes)
    fn broadcast<'py>(
        &self,
        py: Python<'py>,
        data: Bound<'py, PyBytes>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let data_vec = data.as_bytes().to_vec();

        future_into_py(py, async move {
            core.broadcast(data_vec)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Disconnect a specific peer
    ///
    /// Args:
    ///     peer_id: Peer to disconnect
    ///     reason: Optional disconnect reason
    #[pyo3(signature = (peer_id, reason=None))]
    fn disconnect_peer<'py>(
        &self,
        py: Python<'py>,
        peer_id: String,
        reason: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();

        future_into_py(py, async move {
            core.disconnect_peer(&peer_id, reason.as_deref())
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Create a new session (room) and return a WebRtcSession object
    ///
    /// Args:
    ///     session_id: Unique session identifier
    ///     metadata: Optional session metadata (dict)
    ///
    /// Returns:
    ///     WebRtcSession instance for managing the room
    #[pyo3(signature = (session_id, metadata=None))]
    fn create_session<'py>(
        &self,
        py: Python<'py>,
        session_id: String,
        metadata: Option<Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();

        // Convert metadata dict to HashMap
        let metadata_map: Option<HashMap<String, String>> = metadata.map(|dict| {
            dict.iter()
                .filter_map(|(k, v)| {
                    let key: Option<String> = k.extract().ok();
                    let value: Option<String> = v.extract().ok();
                    key.zip(value)
                })
                .collect()
        });

        let session_id_clone = session_id.clone();

        future_into_py(py, async move {
            // Create the session in core
            let _session = core
                .create_session(&session_id_clone, metadata_map)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            // Return a WebRtcSession wrapper
            let py_session = super::session::WebRtcSession::new(core, session_id_clone);
            Python::attach(|py| Py::new(py, py_session))
        })
    }

    /// Create a new session (room) and return SessionInfo
    ///
    /// Args:
    ///     session_id: Unique session identifier
    ///     metadata: Optional session metadata (dict)
    ///
    /// Returns:
    ///     SessionInfo with session details
    #[pyo3(signature = (session_id, metadata=None))]
    fn create_session_info<'py>(
        &self,
        py: Python<'py>,
        session_id: String,
        metadata: Option<Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();

        // Convert metadata dict to HashMap
        let metadata_map: Option<HashMap<String, String>> = metadata.map(|dict| {
            dict.iter()
                .filter_map(|(k, v)| {
                    let key: Option<String> = k.extract().ok();
                    let value: Option<String> = v.extract().ok();
                    key.zip(value)
                })
                .collect()
        });

        future_into_py(py, async move {
            let session = core
                .create_session(&session_id, metadata_map)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

            let session_info: SessionInfo = session.into();
            Python::attach(|py| Py::new(py, session_info))
        })
    }

    /// Get an existing session
    ///
    /// Args:
    ///     session_id: Session identifier
    ///
    /// Returns:
    ///     SessionInfo or None
    fn get_session<'py>(&self, py: Python<'py>, session_id: String) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();

        future_into_py(py, async move {
            match core.get_session(&session_id).await {
                Some(session) => {
                    let session_info: SessionInfo = session.into();
                    Python::attach(|py| Py::new(py, session_info).map(|o| o.into_any()))
                }
                None => Python::attach(|py| Ok(py.None())),
            }
        })
    }

    /// Delete a session
    ///
    /// Args:
    ///     session_id: Session to delete
    fn delete_session<'py>(
        &self,
        py: Python<'py>,
        session_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();

        future_into_py(py, async move {
            core.delete_session(&session_id)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Context manager support: async with server
    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let server = slf.clone_ref(py);
        future_into_py(py, async move {
            // Start the server when entering context
            let core = Python::attach(|py| {
                let s = server.bind(py);
                s.borrow().core.clone()
            });

            core.start()
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

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
        let core = self.core.clone();
        let event_loop_running = self.event_loop_running.clone();
        let event_loop_handle = self.event_loop_handle.clone();

        future_into_py(py, async move {
            // Stop event loop
            event_loop_running.store(false, Ordering::SeqCst);

            if let Some(handle) = event_loop_handle.lock().await.take() {
                let _ = handle.await;
            }

            // Shutdown core
            let _ = core.shutdown().await;

            Ok(false) // Don't suppress exceptions
        })
    }
}

// ============ Event Loop ============

async fn run_event_loop(
    mut event_rx: tokio::sync::mpsc::Receiver<WebRtcEvent>,
    running: Arc<AtomicBool>,
    on_peer_connected: Arc<Mutex<Vec<Py<PyAny>>>>,
    on_peer_disconnected: Arc<Mutex<Vec<Py<PyAny>>>>,
    on_pipeline_output: Arc<Mutex<Vec<Py<PyAny>>>>,
    on_data: Arc<Mutex<Vec<Py<PyAny>>>>,
    on_error: Arc<Mutex<Vec<Py<PyAny>>>>,
    on_session: Arc<Mutex<Vec<Py<PyAny>>>>,
) {
    while running.load(Ordering::SeqCst) {
        tokio::select! {
            Some(event) = event_rx.recv() => {
                match event {
                    WebRtcEvent::PeerConnected(e) => {
                        let py_event: PeerConnectedEvent = e.into();
                        let callbacks = on_peer_connected.lock().await;
                        invoke_callbacks(&callbacks, py_event).await;
                    }
                    WebRtcEvent::PeerDisconnected(e) => {
                        let py_event: PeerDisconnectedEvent = e.into();
                        let callbacks = on_peer_disconnected.lock().await;
                        invoke_callbacks(&callbacks, py_event).await;
                    }
                    WebRtcEvent::PipelineOutput(e) => {
                        let py_event: PipelineOutputEvent = e.into();
                        let callbacks = on_pipeline_output.lock().await;
                        invoke_callbacks(&callbacks, py_event).await;
                    }
                    WebRtcEvent::DataReceived(e) => {
                        let py_event: DataReceivedEvent = e.into();
                        let callbacks = on_data.lock().await;
                        invoke_callbacks(&callbacks, py_event).await;
                    }
                    WebRtcEvent::Error(e) => {
                        let py_event: ErrorEvent = e.into();
                        let callbacks = on_error.lock().await;
                        invoke_callbacks(&callbacks, py_event).await;
                    }
                    WebRtcEvent::Session(e) => {
                        let py_event: SessionEvent = e.into();
                        let callbacks = on_session.lock().await;
                        invoke_callbacks(&callbacks, py_event).await;
                    }
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                // Periodic wake up to check the running flag in the loop condition
            }
            else => {
                break;
            }
        }
    }

    tracing::debug!("Python WebRTC event loop stopped");
}

async fn invoke_callbacks<T>(callbacks: &[Py<PyAny>], event: T)
where
    T: for<'a> IntoPyObjectExt<'a> + Clone + Send + 'static,
{
    for callback in callbacks.iter() {
        let event = event.clone();

        // Invoke callback in a way that handles both sync and async functions
        if let Err(e) = Python::attach(|py| -> PyResult<()> {
            let cb = callback.bind(py);
            // Convert event to a PyAny object
            let py_event = event.into_py_any(py)?;

            // Call the callback with the bound Python object
            let result = cb.call1((py_event,))?;

            // If the result is a coroutine, we need to schedule it
            if result.hasattr("__await__")? || result.hasattr("send")? {
                // It's a coroutine - get the asyncio module and schedule it
                let asyncio = py.import("asyncio")?;
                let _ = asyncio.call_method1("create_task", (result,))?;
            }

            Ok(())
        }) {
            tracing::warn!("Error invoking Python callback: {}", e);
        }
    }
}
