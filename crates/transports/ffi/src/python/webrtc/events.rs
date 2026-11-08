//! Python event types for WebRTC callbacks
//!
//! Provides Python-accessible event types for the decorator-based callback pattern.

use super::config::PeerCapabilities;
use crate::marshal::buffers::storage;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use remotemedia_core::data::RuntimeData;
use remotemedia_types::{CopyContext, CopyReason, PayloadStorage, SharedBytes};
use std::collections::HashMap;

#[cfg(test)]
#[path = "event_owner_tests.rs"]
mod owner_tests;

/// Event fired when a peer connects
#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct PeerConnectedEvent {
    /// Unique peer identifier
    pub peer_id: String,
    /// Peer's media capabilities
    pub capabilities: PeerCapabilities,
    /// Custom peer metadata
    metadata_map: HashMap<String, String>,
}

#[pymethods]
impl PeerConnectedEvent {
    /// Get metadata as Python dict
    #[getter]
    fn metadata(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let dict = PyDict::new(py);
        for (k, v) in &self.metadata_map {
            dict.set_item(k, v)?;
        }
        Ok(dict.into())
    }

    fn __repr__(&self) -> String {
        format!(
            "PeerConnectedEvent(peer_id='{}', capabilities={})",
            self.peer_id,
            format!(
                "audio={}, video={}, data={}",
                self.capabilities.audio, self.capabilities.video, self.capabilities.data
            )
        )
    }
}

impl From<crate::webrtc::events::PeerConnectedEvent> for PeerConnectedEvent {
    fn from(e: crate::webrtc::events::PeerConnectedEvent) -> Self {
        Self {
            peer_id: e.peer_id,
            capabilities: PeerCapabilities {
                audio: e.capabilities.audio,
                video: e.capabilities.video,
                data: e.capabilities.data,
            },
            metadata_map: e.metadata,
        }
    }
}

/// Event fired when a peer disconnects
#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct PeerDisconnectedEvent {
    /// Unique peer identifier
    pub peer_id: String,
    /// Disconnect reason (if any)
    pub reason: Option<String>,
}

#[pymethods]
impl PeerDisconnectedEvent {
    fn __repr__(&self) -> String {
        if let Some(ref reason) = self.reason {
            format!(
                "PeerDisconnectedEvent(peer_id='{}', reason='{}')",
                self.peer_id, reason
            )
        } else {
            format!("PeerDisconnectedEvent(peer_id='{}')", self.peer_id)
        }
    }
}

impl From<crate::webrtc::events::PeerDisconnectedEvent> for PeerDisconnectedEvent {
    fn from(e: crate::webrtc::events::PeerDisconnectedEvent) -> Self {
        Self {
            peer_id: e.peer_id,
            reason: e.reason,
        }
    }
}

/// Event fired when pipeline produces output.
/// `runtime_data` exports retained read-only bulk views; `data` is the
/// compatibility JSON-bytes materialization, performed only when requested.
#[pyclass]
#[derive(Clone, Debug)]
pub struct PipelineOutputEvent {
    #[pyo3(get)]
    pub peer_id: String,
    runtime_data: RuntimeData,
    serialized: std::sync::Arc<std::sync::OnceLock<Vec<u8>>>,
    /// Existing timestamp value, preserved from the event bridge.
    #[pyo3(get)]
    pub timestamp: u64,
}

#[pymethods]
impl PipelineOutputEvent {
    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        self.data_with_context(py, CopyContext::legacy())
    }

    /// Canonical Python runtime value with owning read-only bulk views.
    #[getter]
    fn runtime_data(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        crate::marshal::runtime_data_to_python(py, &self.runtime_data)
    }

    fn __repr__(&self) -> String {
        format!(
            "PipelineOutputEvent(peer_id='{}', data_type='{}', timestamp={})",
            self.peer_id,
            self.runtime_data.data_type(),
            self.timestamp
        )
    }
}

impl PipelineOutputEvent {
    fn data_with_context<'py>(
        &self,
        py: Python<'py>,
        copies: &CopyContext,
    ) -> PyResult<Bound<'py, PyBytes>> {
        if self.serialized.get().is_none() {
            let encoded = storage::materialize(
                copies,
                CopyReason::PortableSerialization,
                self.runtime_data.bulk_payload_bytes(),
                || {
                    serde_json::to_vec(&self.runtime_data)
                        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
                },
            )?;
            // Racing callers may each serialize (and each is counted), but the
            // cache is immutable and shared by event clones once initialized.
            let _ = self.serialized.set(encoded);
        }
        let encoded = self.serialized.get().expect("initialized JSON cache");
        storage::materialize(
            copies,
            CopyReason::VecMaterialization,
            encoded.len(),
            || Ok(PyBytes::new(py, &encoded)),
        )
    }
}

impl From<crate::webrtc::events::PipelineOutputEvent> for PipelineOutputEvent {
    fn from(e: crate::webrtc::events::PipelineOutputEvent) -> Self {
        Self {
            peer_id: e.peer_id,
            runtime_data: e.data.into_shared(),
            serialized: Default::default(),
            timestamp: e.timestamp,
        }
    }
}

/// Event fired when raw data is received from a peer.
/// `view` retains the immutable receive owner. `data` explicitly copies to bytes.
#[pyclass]
#[derive(Clone, Debug)]
pub struct DataReceivedEvent {
    #[pyo3(get)]
    pub peer_id: String,
    data_bytes: SharedBytes,
    #[pyo3(get)]
    pub timestamp: u64,
}

#[pymethods]
impl DataReceivedEvent {
    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        self.data_with_context(py, CopyContext::legacy())
    }

    /// Read-only memoryview retaining the original receive allocation.
    #[getter]
    fn view(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        storage::view(py, PayloadStorage::Bytes(self.data_bytes.clone()))
    }

    fn __repr__(&self) -> String {
        format!(
            "DataReceivedEvent(peer_id='{}', data_len={}, timestamp={})",
            self.peer_id,
            self.data_bytes.len(),
            self.timestamp
        )
    }
}

impl DataReceivedEvent {
    fn data_with_context<'py>(
        &self,
        py: Python<'py>,
        copies: &CopyContext,
    ) -> PyResult<Bound<'py, PyBytes>> {
        storage::materialize(
            copies,
            CopyReason::VecMaterialization,
            self.data_bytes.len(),
            || Ok(PyBytes::new(py, &self.data_bytes)),
        )
    }
}

impl From<crate::webrtc::events::DataReceivedEvent> for DataReceivedEvent {
    fn from(e: crate::webrtc::events::DataReceivedEvent) -> Self {
        Self {
            peer_id: e.peer_id,
            data_bytes: e.data,
            timestamp: e.timestamp,
        }
    }
}

/// Event fired when an error occurs
#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct ErrorEvent {
    /// Error code
    pub code: String,
    /// Error message
    pub message: String,
    /// Associated peer ID (if any)
    pub peer_id: Option<String>,
}

#[pymethods]
impl ErrorEvent {
    fn __repr__(&self) -> String {
        if let Some(ref peer_id) = self.peer_id {
            format!(
                "ErrorEvent(code='{}', message='{}', peer_id='{}')",
                self.code, self.message, peer_id
            )
        } else {
            format!(
                "ErrorEvent(code='{}', message='{}')",
                self.code, self.message
            )
        }
    }
}

impl From<crate::webrtc::events::ErrorEvent> for ErrorEvent {
    fn from(e: crate::webrtc::events::ErrorEvent) -> Self {
        Self {
            code: e.code.to_string(),
            message: e.message,
            peer_id: e.peer_id,
        }
    }
}

/// Event fired for session-related changes
#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct SessionEvent {
    /// Session ID
    pub session_id: String,
    /// Event type (peer_joined, peer_left, session_created, session_deleted)
    pub event_type: String,
    /// Associated peer ID (for peer_joined/peer_left)
    pub peer_id: Option<String>,
}

#[pymethods]
impl SessionEvent {
    fn __repr__(&self) -> String {
        if let Some(ref peer_id) = self.peer_id {
            format!(
                "SessionEvent(session_id='{}', event_type='{}', peer_id='{}')",
                self.session_id, self.event_type, peer_id
            )
        } else {
            format!(
                "SessionEvent(session_id='{}', event_type='{}')",
                self.session_id, self.event_type
            )
        }
    }
}

impl From<crate::webrtc::events::SessionEvent> for SessionEvent {
    fn from(e: crate::webrtc::events::SessionEvent) -> Self {
        Self {
            session_id: e.session_id,
            event_type: e.event_type.to_string(),
            peer_id: Some(e.peer_id),
        }
    }
}
