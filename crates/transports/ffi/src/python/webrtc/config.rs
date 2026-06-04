//! Python WebRtcServerConfig and related types
//!
//! Provides Python dataclass-like types for WebRTC server configuration.

use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde_json;
use std::collections::HashMap;

/// TURN server configuration for Python
#[pyclass(get_all, set_all)]
#[derive(Clone, Debug)]
pub struct TurnServer {
    /// TURN server URL (turn:host:port or turns:host:port)
    pub url: String,
    /// Authentication username
    pub username: String,
    /// Authentication credential
    pub credential: String,
}

#[pymethods]
impl TurnServer {
    #[new]
    fn new(url: String, username: String, credential: String) -> Self {
        Self {
            url,
            username,
            credential,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "TurnServer(url='{}', username='{}', ...)",
            self.url, self.username
        )
    }
}

impl From<&TurnServer> for crate::webrtc::config::TurnServerConfig {
    fn from(ts: &TurnServer) -> Self {
        crate::webrtc::config::TurnServerConfig {
            url: ts.url.clone(),
            username: ts.username.clone(),
            credential: ts.credential.clone(),
        }
    }
}

/// WebRTC server configuration for Python
#[pyclass]
#[derive(Clone, Debug)]
pub struct WebRtcServerConfig {
    /// Port for embedded gRPC signaling server (mutually exclusive with signaling_url)
    #[pyo3(get, set)]
    pub port: Option<u16>,

    /// URL for external signaling server (mutually exclusive with port)
    #[pyo3(get, set)]
    pub signaling_url: Option<String>,

    /// Pipeline manifest as JSON dict
    manifest_json: String,

    /// STUN server URLs
    #[pyo3(get, set)]
    pub stun_servers: Vec<String>,

    /// TURN server configurations
    turn_servers_inner: Vec<TurnServer>,

    /// Maximum concurrent peers (1-10)
    #[pyo3(get, set)]
    pub max_peers: u32,

    /// Audio codec (only 'opus' supported)
    #[pyo3(get, set)]
    pub audio_codec: String,

    /// Video codec ('vp8', 'vp9', 'h264')
    #[pyo3(get, set)]
    pub video_codec: String,
}

#[pymethods]
impl WebRtcServerConfig {
    #[new]
    #[pyo3(signature = (port=None, signaling_url=None, manifest=None, stun_servers=None, turn_servers=None, max_peers=10, audio_codec="opus", video_codec="vp9"))]
    fn new(
        py: Python<'_>,
        port: Option<u16>,
        signaling_url: Option<String>,
        manifest: Option<Bound<'_, PyAny>>,
        stun_servers: Option<Vec<String>>,
        turn_servers: Option<Vec<TurnServer>>,
        max_peers: Option<u32>,
        audio_codec: Option<&str>,
        video_codec: Option<&str>,
    ) -> PyResult<Self> {
        // Convert manifest to JSON string
        let manifest_json = if let Some(m) = manifest {
            python_dict_to_json_string(py, &m)?
        } else {
            r#"{"nodes": [], "connections": []}"#.to_string()
        };

        // Default STUN servers
        let stun_servers =
            stun_servers.unwrap_or_else(|| vec!["stun:stun.l.google.com:19302".to_string()]);

        Ok(Self {
            port,
            signaling_url,
            manifest_json,
            stun_servers,
            turn_servers_inner: turn_servers.unwrap_or_default(),
            max_peers: max_peers.unwrap_or(10),
            audio_codec: audio_codec.unwrap_or("opus").to_string(),
            video_codec: video_codec.unwrap_or("vp9").to_string(),
        })
    }

    /// Get manifest as Python dict
    #[getter]
    fn manifest(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value: serde_json::Value = serde_json::from_str(&self.manifest_json)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        json_to_python(py, &value)
    }

    /// Set manifest from Python dict
    #[setter]
    fn set_manifest(&mut self, py: Python<'_>, value: Bound<'_, PyAny>) -> PyResult<()> {
        self.manifest_json = python_dict_to_json_string(py, &value)?;
        Ok(())
    }

    /// Get TURN servers
    #[getter]
    fn turn_servers(&self) -> Vec<TurnServer> {
        self.turn_servers_inner.clone()
    }

    /// Set TURN servers
    #[setter]
    fn set_turn_servers(&mut self, servers: Vec<TurnServer>) {
        self.turn_servers_inner = servers;
    }

    fn __repr__(&self) -> String {
        if let Some(port) = self.port {
            format!(
                "WebRtcServerConfig(port={}, max_peers={})",
                port, self.max_peers
            )
        } else if let Some(ref url) = self.signaling_url {
            format!(
                "WebRtcServerConfig(signaling_url='{}', max_peers={})",
                url, self.max_peers
            )
        } else {
            format!("WebRtcServerConfig(max_peers={})", self.max_peers)
        }
    }
}

impl WebRtcServerConfig {
    /// Convert to core config for validation and internal use
    pub fn to_core_config(&self) -> PyResult<crate::webrtc::config::WebRtcServerConfig> {
        let manifest: serde_json::Value =
            serde_json::from_str(&self.manifest_json).map_err(|e| {
                pyo3::exceptions::PyValueError::new_err(format!("Invalid manifest: {}", e))
            })?;

        let video_codec = match self.video_codec.to_lowercase().as_str() {
            "vp8" => crate::webrtc::config::VideoCodec::Vp8,
            "vp9" => crate::webrtc::config::VideoCodec::Vp9,
            "h264" => crate::webrtc::config::VideoCodec::H264,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid video codec: {}. Must be vp8, vp9, or h264",
                    self.video_codec
                )));
            }
        };

        let config = crate::webrtc::config::WebRtcServerConfig {
            port: self.port,
            signaling_url: self.signaling_url.clone(),
            manifest,
            stun_servers: self.stun_servers.clone(),
            turn_servers: self.turn_servers_inner.iter().map(Into::into).collect(),
            max_peers: self.max_peers,
            audio_codec: crate::webrtc::config::AudioCodec::Opus,
            video_codec,
            reconnect: crate::webrtc::config::ReconnectConfig::default(),
        };

        // Validate the config
        config
            .validate()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        Ok(config)
    }
}

/// Peer capabilities for Python
#[pyclass(get_all)]
#[derive(Clone, Debug, Default)]
pub struct PeerCapabilities {
    /// Supports audio tracks
    pub audio: bool,
    /// Supports video tracks
    pub video: bool,
    /// Supports data channels
    pub data: bool,
}

#[pymethods]
impl PeerCapabilities {
    #[new]
    #[pyo3(signature = (audio=false, video=false, data=false))]
    fn new(audio: bool, video: bool, data: bool) -> Self {
        Self { audio, video, data }
    }

    fn __repr__(&self) -> String {
        format!(
            "PeerCapabilities(audio={}, video={}, data={})",
            self.audio, self.video, self.data
        )
    }
}

impl From<crate::webrtc::config::PeerCapabilities> for PeerCapabilities {
    fn from(c: crate::webrtc::config::PeerCapabilities) -> Self {
        Self {
            audio: c.audio,
            video: c.video,
            data: c.data,
        }
    }
}

/// Peer information for Python
#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct PeerInfo {
    /// Unique peer identifier
    pub peer_id: String,
    /// Peer's audio/video/data capabilities
    pub capabilities: PeerCapabilities,
    /// Custom peer metadata
    metadata_map: HashMap<String, String>,
    /// Connection state
    pub state: String,
    /// Unix timestamp when peer connected
    pub connected_at: u64,
}

#[pymethods]
impl PeerInfo {
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
            "PeerInfo(peer_id='{}', state='{}', connected_at={})",
            self.peer_id, self.state, self.connected_at
        )
    }
}

impl From<crate::webrtc::config::PeerInfo> for PeerInfo {
    fn from(p: crate::webrtc::config::PeerInfo) -> Self {
        let state = match p.state {
            crate::webrtc::config::PeerState::Connecting => "connecting",
            crate::webrtc::config::PeerState::Connected => "connected",
            crate::webrtc::config::PeerState::Disconnecting => "disconnecting",
            crate::webrtc::config::PeerState::Disconnected => "disconnected",
        };

        Self {
            peer_id: p.peer_id,
            capabilities: p.capabilities.into(),
            metadata_map: p.metadata,
            state: state.to_string(),
            connected_at: p.connected_at,
        }
    }
}

/// Session information for Python
#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct SessionInfo {
    /// Unique session identifier
    pub session_id: String,
    /// Session metadata
    metadata_map: HashMap<String, String>,
    /// Connected peer IDs
    pub peer_ids: Vec<String>,
    /// Unix timestamp when session was created
    pub created_at: u64,
}

#[pymethods]
impl SessionInfo {
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
            "SessionInfo(session_id='{}', peers={})",
            self.session_id,
            self.peer_ids.len()
        )
    }
}

impl From<crate::webrtc::core::SessionInfo> for SessionInfo {
    fn from(s: crate::webrtc::core::SessionInfo) -> Self {
        Self {
            session_id: s.session_id,
            metadata_map: s.metadata,
            peer_ids: s.peers,
            created_at: s.created_at,
        }
    }
}

// ============ Helper functions ============

fn python_dict_to_json_string(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<String> {
    let json_module = py.import("json")?;
    let json_str: String = json_module.call_method1("dumps", (obj,))?.extract()?;
    Ok(json_str)
}

fn json_to_python(py: Python<'_>, value: &serde_json::Value) -> PyResult<Py<PyAny>> {
    match value {
        serde_json::Value::Null => Ok(py.None()),
        serde_json::Value::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.into_any().unbind())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.into_any().unbind())
            } else {
                Ok(py.None())
            }
        }
        serde_json::Value::String(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
        serde_json::Value::Array(arr) => {
            let list = pyo3::types::PyList::empty(py);
            for item in arr {
                list.append(json_to_python(py, item)?)?;
            }
            Ok(list.unbind().into_any())
        }
        serde_json::Value::Object(obj) => {
            let dict = PyDict::new(py);
            for (k, v) in obj {
                dict.set_item(k, json_to_python(py, v)?)?;
            }
            Ok(dict.unbind().into_any())
        }
    }
}
