//! Python TelephonyServerConfig and related types
//!
//! Provides Python classes for configuring the native Telephony SIP/RTP gateway.

use pyo3::prelude::*;
use pyo3::types::PyAny;
use serde_json;

/// Telephony server configuration for Python
#[pyclass]
#[derive(Clone, Debug)]
pub struct TelephonyServerConfig {
    /// Address for the SIP listener (e.g. 0.0.0.0:5060)
    #[pyo3(get, set)]
    pub sip_bind_address: String,

    /// Optional advertised address in SDP media answers
    #[pyo3(get, set)]
    pub advertised_media_address: Option<String>,

    /// First UDP port available for RTP sockets
    #[pyo3(get, set)]
    pub rtp_port_start: u16,

    /// Last UDP port available for RTP sockets
    #[pyo3(get, set)]
    pub rtp_port_end: u16,

    /// Codec preference order (e.g. ['opus', 'pcmu', 'pcma'])
    #[pyo3(get, set)]
    pub codec_preferences: Vec<String>,

    /// Audio frame duration in milliseconds
    #[pyo3(get, set)]
    pub frame_duration_ms: u16,

    /// Maximum concurrent calls allowed
    #[pyo3(get, set)]
    pub max_active_calls: u32,

    /// Allow-list of SIP peer IPs or domains
    #[pyo3(get, set)]
    pub allowed_peers: Vec<String>,

    /// Access mode: "allowlist" (deny by default) or "denylist" (allow by default)
    #[pyo3(get, set)]
    pub access_mode: String,

    /// Deny-list of SIP peer IPs or domains
    #[pyo3(get, set)]
    pub blocked_peers: Vec<String>,

    /// Enable rate limiting
    #[pyo3(get, set)]
    pub rate_limit_enabled: bool,

    /// Rate limit: max requests per window
    #[pyo3(get, set)]
    pub rate_limit_max: u32,

    /// Rate limit: window duration in seconds
    #[pyo3(get, set)]
    pub rate_limit_window: u64,

    /// Rate limit: ban duration in seconds
    #[pyo3(get, set)]
    pub rate_limit_ban_duration: u64,

    /// Enable SIPREC mirrored-call ingestion
    #[pyo3(get, set)]
    pub enable_siprec: bool,

    /// Optional gRPC control plane port to listen on (e.g. 50051)
    #[pyo3(get, set)]
    pub control_plane_port: Option<u16>,

    /// Pipeline manifest as JSON string
    pub manifest_json: String,
}

#[pymethods]
impl TelephonyServerConfig {
    #[new]
    #[pyo3(signature = (
        sip_bind_address = "0.0.0.0:5060",
        advertised_media_address = None,
        rtp_port_start = 16384,
        rtp_port_end = 32767,
        codec_preferences = None,
        frame_duration_ms = 20,
        max_active_calls = 128,
        allowed_peers = None,
        access_mode = "allowlist",
        blocked_peers = None,
        rate_limit_enabled = false,
        rate_limit_max = 10,
        rate_limit_window = 60,
        rate_limit_ban_duration = 60,
        enable_siprec = false,
        manifest = None,
        control_plane_port = None
    ))]
    fn new(
        py: Python<'_>,
        sip_bind_address: &str,
        advertised_media_address: Option<String>,
        rtp_port_start: u16,
        rtp_port_end: u16,
        codec_preferences: Option<Vec<String>>,
        frame_duration_ms: u16,
        max_active_calls: u32,
        allowed_peers: Option<Vec<String>>,
        access_mode: &str,
        blocked_peers: Option<Vec<String>>,
        rate_limit_enabled: bool,
        rate_limit_max: u32,
        rate_limit_window: u64,
        rate_limit_ban_duration: u64,
        enable_siprec: bool,
        manifest: Option<Bound<'_, PyAny>>,
        control_plane_port: Option<u16>,
    ) -> PyResult<Self> {
        let manifest_json = if let Some(m) = manifest {
            python_dict_to_json_string(py, &m)?
        } else {
            r#"{"version": "v1", "metadata": {"name": "empty-telephony"}, "nodes": [], "connections": []}"#.to_string()
        };

        let codec_preferences = codec_preferences
            .unwrap_or_else(|| vec!["opus".to_string(), "pcmu".to_string(), "pcma".to_string()]);

        Ok(Self {
            sip_bind_address: sip_bind_address.to_string(),
            advertised_media_address,
            rtp_port_start,
            rtp_port_end,
            codec_preferences,
            frame_duration_ms,
            max_active_calls,
            allowed_peers: allowed_peers.unwrap_or_default(),
            access_mode: access_mode.to_string(),
            blocked_peers: blocked_peers.unwrap_or_default(),
            rate_limit_enabled,
            rate_limit_max,
            rate_limit_window,
            rate_limit_ban_duration,
            enable_siprec,
            manifest_json,
            control_plane_port,
        })
    }

    /// Get pipeline manifest as Python dict
    #[getter]
    fn manifest(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value: serde_json::Value = serde_json::from_str(&self.manifest_json)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        json_to_python(py, &value)
    }

    /// Set pipeline manifest from Python dict
    #[setter]
    fn set_manifest(&mut self, py: Python<'_>, value: Bound<'_, PyAny>) -> PyResult<()> {
        self.manifest_json = python_dict_to_json_string(py, &value)?;
        Ok(())
    }

    fn __repr__(&self) -> String {
        format!(
            "TelephonyServerConfig(sip_bind_address='{}', rtp_port_range={}-{}, max_active_calls={})",
            self.sip_bind_address, self.rtp_port_start, self.rtp_port_end, self.max_active_calls
        )
    }
}

impl TelephonyServerConfig {
    /// Convert to core config used by the Rust transport crate
    pub fn to_core_config(&self) -> PyResult<remotemedia_telephony::TelephonyTransportConfig> {
        let mut preferences = Vec::new();
        for codec_str in &self.codec_preferences {
            match codec_str.to_lowercase().as_str() {
                "opus" => preferences.push(remotemedia_telephony::AudioCodec::Opus),
                "pcmu" => preferences.push(remotemedia_telephony::AudioCodec::Pcmu),
                "pcma" => preferences.push(remotemedia_telephony::AudioCodec::Pcma),
                other => {
                    return Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "Invalid audio codec preference: '{}'. Must be opus, pcmu, or pcma",
                        other
                    )));
                }
            }
        }

        let config = remotemedia_telephony::TelephonyTransportConfig {
            sip_bind_address: self.sip_bind_address.clone(),
            advertised_media_address: self.advertised_media_address.clone(),
            rtp_port_start: self.rtp_port_start,
            rtp_port_end: self.rtp_port_end,
            codec_preferences: preferences,
            frame_duration_ms: self.frame_duration_ms,
            max_active_calls: self.max_active_calls,
            max_rtp_sessions: self.max_active_calls * 2,
            allowed_peers: self.allowed_peers.clone(),
            access_mode: match self.access_mode.as_str() {
                "denylist" | "deny_list" => remotemedia_telephony::SipAccessMode::DenyList,
                _ => remotemedia_telephony::SipAccessMode::AllowList,
            },
            blocked_peers: self.blocked_peers.clone(),
            rate_limit: remotemedia_telephony::SipRateLimitConfig {
                enabled: self.rate_limit_enabled,
                max_requests_per_window: self.rate_limit_max,
                window_seconds: self.rate_limit_window,
                ban_duration_seconds: self.rate_limit_ban_duration,
            },
            enable_siprec: self.enable_siprec,
            ..Default::default()
        };

        config
            .validate()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

        Ok(config)
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
        serde_json::Value::Bool(b) => {
            let py_val = b.into_pyobject(py)?;
            Ok(py_val.to_owned().into_any().unbind())
        }
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                let py_val = i.into_pyobject(py)?;
                Ok(py_val.to_owned().into_any().unbind())
            } else if let Some(f) = n.as_f64() {
                let py_val = f.into_pyobject(py)?;
                Ok(py_val.to_owned().into_any().unbind())
            } else {
                Ok(py.None())
            }
        }
        serde_json::Value::String(s) => {
            let py_val = s.into_pyobject(py)?;
            Ok(py_val.to_owned().into_any().unbind())
        }
        serde_json::Value::Array(arr) => {
            let list = pyo3::types::PyList::empty(py);
            for item in arr {
                list.append(json_to_python(py, item)?)?;
            }
            Ok(list.unbind().into_any())
        }
        serde_json::Value::Object(obj) => {
            let dict = pyo3::types::PyDict::new(py);
            for (k, v) in obj {
                dict.set_item(k, json_to_python(py, v)?)?;
            }
            Ok(dict.unbind().into_any())
        }
    }
}
