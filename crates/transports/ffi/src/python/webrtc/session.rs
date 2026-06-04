//! WebRtcSession Python binding
//!
//! Implements the Python WebRtcSession class for room/group management
//! with decorator-based callback registration for peer join/leave events.

use crate::webrtc::core::WebRtcServerCore;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::sync::Mutex;

/// WebRTC session (room) for Python
///
/// Provides logical grouping of peers with session-specific events.
/// Created via WebRtcServer.create_session().
#[pyclass]
pub struct WebRtcSession {
    /// Reference to the parent server core
    core: Arc<WebRtcServerCore>,

    /// Session ID
    session_id: String,

    /// Peer joined callbacks
    on_peer_joined_callbacks: Arc<Mutex<Vec<Py<PyAny>>>>,

    /// Peer left callbacks
    on_peer_left_callbacks: Arc<Mutex<Vec<Py<PyAny>>>>,
}

impl WebRtcSession {
    /// Create a new WebRtcSession (internal constructor)
    pub(crate) fn new(core: Arc<WebRtcServerCore>, session_id: String) -> Self {
        Self {
            core,
            session_id,
            on_peer_joined_callbacks: Arc::new(Mutex::new(Vec::new())),
            on_peer_left_callbacks: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[pymethods]
impl WebRtcSession {
    /// Get the session ID
    #[getter]
    fn session_id(&self) -> String {
        self.session_id.clone()
    }

    /// Get the peer IDs in this session
    fn get_peers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let session_id = self.session_id.clone();

        future_into_py(py, async move {
            match core.get_session(&session_id).await {
                Some(session) => Python::attach(|py| {
                    let list = pyo3::types::PyList::empty(py);
                    for peer_id in session.peers {
                        list.append(peer_id)?;
                    }
                    Ok(list.unbind())
                }),
                None => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Session not found: {}",
                    session_id
                ))),
            }
        })
    }

    /// Get the session creation timestamp
    fn get_created_at<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let session_id = self.session_id.clone();

        future_into_py(py, async move {
            match core.get_session(&session_id).await {
                Some(session) => Ok(session.created_at as i64),
                None => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Session not found: {}",
                    session_id
                ))),
            }
        })
    }

    /// Get the session metadata
    fn get_metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let session_id = self.session_id.clone();

        future_into_py(py, async move {
            match core.get_session(&session_id).await {
                Some(session) => Python::attach(|py| {
                    let dict = pyo3::types::PyDict::new(py);
                    for (k, v) in session.metadata {
                        dict.set_item(k, v)?;
                    }
                    Ok(dict.unbind())
                }),
                None => Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Session not found: {}",
                    session_id
                ))),
            }
        })
    }

    /// Register a callback for peer joined events (decorator pattern)
    ///
    /// Usage:
    ///     @session.on_peer_joined
    ///     async def handle_join(peer_id: str):
    ///         print(f"Peer {peer_id} joined")
    fn on_peer_joined(&self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let callbacks = self.on_peer_joined_callbacks.clone();
        let callback_clone = callback.clone_ref(py);
        tokio::spawn(async move {
            callbacks.lock().await.push(callback_clone);
        });
        // Return the callback for decorator chaining
        Ok(callback)
    }

    /// Register a callback for peer left events (decorator pattern)
    ///
    /// Usage:
    ///     @session.on_peer_left
    ///     async def handle_leave(peer_id: str):
    ///         print(f"Peer {peer_id} left")
    fn on_peer_left(&self, py: Python<'_>, callback: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let callbacks = self.on_peer_left_callbacks.clone();
        let callback_clone = callback.clone_ref(py);
        tokio::spawn(async move {
            callbacks.lock().await.push(callback_clone);
        });
        Ok(callback)
    }

    /// Broadcast data to all peers in the session
    ///
    /// Args:
    ///     data: Data to broadcast (bytes)
    fn broadcast<'py>(
        &self,
        py: Python<'py>,
        data: Bound<'py, PyBytes>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let session_id = self.session_id.clone();
        let data_vec = data.as_bytes().to_vec();

        future_into_py(py, async move {
            core.broadcast_to_session(&session_id, data_vec)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Send data to a specific peer in the session
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
        let session_id = self.session_id.clone();
        let data_vec = data.as_bytes().to_vec();

        future_into_py(py, async move {
            // Verify peer is in this session
            let session = core.get_session(&session_id).await.ok_or_else(|| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Session not found: {}",
                    session_id
                ))
            })?;

            if !session.peers.contains(&peer_id) {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Peer {} is not in session {}",
                    peer_id, session_id
                )));
            }

            core.send_to_peer(&peer_id, data_vec)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Add a peer to this session
    ///
    /// Args:
    ///     peer_id: Peer ID to add
    fn add_peer<'py>(&self, py: Python<'py>, peer_id: String) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let session_id = self.session_id.clone();

        future_into_py(py, async move {
            core.add_peer_to_session(&session_id, &peer_id)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }

    /// Remove a peer from this session
    ///
    /// Args:
    ///     peer_id: Peer ID to remove
    fn remove_peer<'py>(&self, py: Python<'py>, peer_id: String) -> PyResult<Bound<'py, PyAny>> {
        let core = self.core.clone();
        let session_id = self.session_id.clone();

        future_into_py(py, async move {
            core.remove_peer_from_session(&session_id, &peer_id)
                .await
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))
        })
    }
}
