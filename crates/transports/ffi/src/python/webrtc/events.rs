//! Python event types for WebRTC callbacks
//!
//! Provides Python-accessible event types for the decorator-based callback pattern.

use super::config::PeerCapabilities;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use std::collections::HashMap;

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

/// Event fired when pipeline produces output
#[pyclass]
#[derive(Clone, Debug)]
pub struct PipelineOutputEvent {
    /// Peer ID that triggered this output
    #[pyo3(get)]
    pub peer_id: String,
    /// Output data as bytes
    data_bytes: Vec<u8>,
    /// Unix timestamp in milliseconds
    #[pyo3(get)]
    pub timestamp: u64,
}

#[pymethods]
impl PipelineOutputEvent {
    /// Get data as Python bytes
    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.data_bytes)
    }

    fn __repr__(&self) -> String {
        format!(
            "PipelineOutputEvent(peer_id='{}', data_len={}, timestamp={})",
            self.peer_id,
            self.data_bytes.len(),
            self.timestamp
        )
    }
}

impl From<crate::webrtc::events::PipelineOutputEvent> for PipelineOutputEvent {
    fn from(e: crate::webrtc::events::PipelineOutputEvent) -> Self {
        // Serialize RuntimeData to bytes for Python consumption
        let data_bytes = match serde_json::to_vec(&e.data) {
            Ok(bytes) => bytes,
            Err(_) => Vec::new(),
        };
        Self {
            peer_id: e.peer_id,
            data_bytes,
            timestamp: e.timestamp,
        }
    }
}

/// Event fired when raw data is received from a peer
#[pyclass]
#[derive(Clone, Debug)]
pub struct DataReceivedEvent {
    /// Peer ID that sent the data
    #[pyo3(get)]
    pub peer_id: String,
    /// Raw data bytes
    data_bytes: Vec<u8>,
    /// Unix timestamp in milliseconds
    #[pyo3(get)]
    pub timestamp: u64,
}

#[pymethods]
impl DataReceivedEvent {
    /// Get data as Python bytes
    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.data_bytes)
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
