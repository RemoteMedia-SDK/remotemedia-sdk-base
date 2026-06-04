//! Node.js-friendly WebRTC configuration types
//!
//! These types are exposed to JavaScript with appropriate conversions
//! for JS-native types (Object instead of HashMap, etc.)

use crate::webrtc::config as core_config;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::collections::HashMap;

/// TURN server configuration for Node.js
#[napi(object)]
#[derive(Debug, Clone)]
pub struct TurnServer {
    /// TURN server URL (turn:host:port or turns:host:port)
    pub url: String,
    /// Authentication username
    pub username: String,
    /// Authentication credential
    pub credential: String,
}

impl From<TurnServer> for core_config::TurnServerConfig {
    fn from(ts: TurnServer) -> Self {
        core_config::TurnServerConfig {
            url: ts.url,
            username: ts.username,
            credential: ts.credential,
        }
    }
}

impl From<core_config::TurnServerConfig> for TurnServer {
    fn from(ts: core_config::TurnServerConfig) -> Self {
        TurnServer {
            url: ts.url,
            username: ts.username,
            credential: ts.credential,
        }
    }
}

/// WebRTC server configuration for Node.js
#[napi(object)]
#[derive(Debug, Clone)]
pub struct WebRtcServerConfig {
    /// Port for embedded gRPC signaling server (mutually exclusive with signaling_url)
    pub port: Option<u32>,

    /// URL for external signaling server (mutually exclusive with port)
    pub signaling_url: Option<String>,

    /// Pipeline manifest as JSON string (will be parsed)
    pub manifest: String,

    /// STUN server URLs (at least one required)
    pub stun_servers: Vec<String>,

    /// TURN server configurations (optional)
    pub turn_servers: Option<Vec<TurnServer>>,

    /// Maximum concurrent peers (1-10, default 10)
    pub max_peers: Option<u32>,

    /// Audio codec (only "opus" supported)
    pub audio_codec: Option<String>,

    /// Video codec ("vp8", "vp9", or "h264")
    pub video_codec: Option<String>,
}

impl WebRtcServerConfig {
    /// Convert to core configuration
    pub fn to_core_config(&self) -> Result<core_config::WebRtcServerConfig> {
        let video_codec = match self.video_codec.as_deref() {
            Some("vp8") => core_config::VideoCodec::Vp8,
            Some("vp9") | None => core_config::VideoCodec::Vp9,
            Some("h264") => core_config::VideoCodec::H264,
            Some(other) => {
                return Err(Error::from_reason(format!(
                    "Invalid video codec: {}. Valid options: vp8, vp9, h264",
                    other
                )))
            }
        };

        let audio_codec = match self.audio_codec.as_deref() {
            Some("opus") | None => core_config::AudioCodec::Opus,
            Some(other) => {
                return Err(Error::from_reason(format!(
                    "Invalid audio codec: {}. Only 'opus' is supported for WebRTC",
                    other
                )))
            }
        };

        // Parse manifest from JSON string
        let manifest: serde_json::Value = serde_json::from_str(&self.manifest)
            .map_err(|e| Error::from_reason(format!("Invalid manifest JSON: {}", e)))?;

        let config = core_config::WebRtcServerConfig {
            port: self.port.map(|p| p as u16),
            signaling_url: self.signaling_url.clone(),
            manifest,
            stun_servers: self.stun_servers.clone(),
            turn_servers: self
                .turn_servers
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
            max_peers: self.max_peers.unwrap_or(10),
            audio_codec,
            video_codec,
            reconnect: core_config::ReconnectConfig::default(),
        };

        // Validate the config
        config
            .validate()
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(config)
    }
}

/// Peer capabilities exposed to Node.js
#[napi(object)]
#[derive(Debug, Clone, Default)]
pub struct PeerCapabilities {
    /// Supports audio tracks
    pub audio: bool,
    /// Supports video tracks
    pub video: bool,
    /// Supports data channels
    pub data: bool,
}

impl From<core_config::PeerCapabilities> for PeerCapabilities {
    fn from(caps: core_config::PeerCapabilities) -> Self {
        PeerCapabilities {
            audio: caps.audio,
            video: caps.video,
            data: caps.data,
        }
    }
}

impl From<PeerCapabilities> for core_config::PeerCapabilities {
    fn from(caps: PeerCapabilities) -> Self {
        core_config::PeerCapabilities {
            audio: caps.audio,
            video: caps.video,
            data: caps.data,
        }
    }
}

/// Peer information exposed to Node.js
#[napi(object)]
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Unique peer identifier
    pub peer_id: String,
    /// Peer's audio/video/data capabilities
    pub capabilities: PeerCapabilities,
    /// Custom peer metadata as key-value pairs
    pub metadata: HashMap<String, String>,
    /// Connection state
    pub state: String,
    /// Unix timestamp when peer connected
    pub connected_at: f64,
}

impl From<core_config::PeerInfo> for PeerInfo {
    fn from(info: core_config::PeerInfo) -> Self {
        let state = match info.state {
            core_config::PeerState::Connecting => "connecting",
            core_config::PeerState::Connected => "connected",
            core_config::PeerState::Disconnecting => "disconnecting",
            core_config::PeerState::Disconnected => "disconnected",
        };

        PeerInfo {
            peer_id: info.peer_id,
            capabilities: info.capabilities.into(),
            metadata: info.metadata,
            state: state.to_string(),
            connected_at: info.connected_at as f64,
        }
    }
}

/// Session information exposed to Node.js
#[napi(object)]
#[derive(Debug, Clone)]
pub struct SessionInfo {
    /// Unique session identifier
    pub session_id: String,
    /// Peer IDs in this session
    pub peer_ids: Vec<String>,
    /// Unix timestamp of creation
    pub created_at: f64,
    /// Custom session metadata as key-value pairs
    pub metadata: HashMap<String, String>,
}

impl From<crate::webrtc::core::SessionInfo> for SessionInfo {
    fn from(info: crate::webrtc::core::SessionInfo) -> Self {
        SessionInfo {
            session_id: info.session_id,
            peer_ids: info.peers,
            created_at: info.created_at as f64,
            metadata: info.metadata,
        }
    }
}
