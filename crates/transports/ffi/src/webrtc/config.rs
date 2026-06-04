//! WebRTC server configuration types
//!
//! This module provides language-agnostic configuration types that are
//! converted to/from language-specific types in the napi and python modules.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// TURN server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnServerConfig {
    /// TURN server URL (turn:host:port or turns:host:port)
    pub url: String,
    /// Authentication username
    pub username: String,
    /// Authentication credential
    pub credential: String,
}

/// Video codec selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum VideoCodec {
    Vp8,
    #[default]
    Vp9,
    H264,
}

impl VideoCodec {
    /// Convert to remotemedia-webrtc VideoCodec
    #[cfg(feature = "webrtc")]
    pub fn to_webrtc_codec(&self) -> remotemedia_webrtc::config::VideoCodec {
        match self {
            VideoCodec::Vp8 => remotemedia_webrtc::config::VideoCodec::VP8,
            VideoCodec::Vp9 => remotemedia_webrtc::config::VideoCodec::VP9,
            VideoCodec::H264 => remotemedia_webrtc::config::VideoCodec::H264,
        }
    }
}

/// Audio codec selection (only Opus is supported for WebRTC)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum AudioCodec {
    #[default]
    Opus,
}

/// Reconnection configuration for external signaling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconnectConfig {
    /// Maximum number of reconnection attempts (0 = disabled)
    #[serde(default = "default_max_reconnect_attempts")]
    pub max_attempts: u32,

    /// Initial backoff delay in milliseconds
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,

    /// Maximum backoff delay in milliseconds
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,

    /// Backoff multiplier (exponential backoff)
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
}

fn default_max_reconnect_attempts() -> u32 {
    5
}

fn default_initial_backoff_ms() -> u64 {
    1000
}

fn default_max_backoff_ms() -> u64 {
    30000
}

fn default_backoff_multiplier() -> f64 {
    2.0
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            max_attempts: default_max_reconnect_attempts(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            backoff_multiplier: default_backoff_multiplier(),
        }
    }
}

/// WebRTC server configuration
///
/// Used to create a WebRTC server with either embedded or external signaling.
/// Either `port` (embedded) or `signaling_url` (external) must be set, not both.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebRtcServerConfig {
    /// Port for embedded gRPC signaling server (mutually exclusive with signaling_url)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,

    /// URL for external signaling server (mutually exclusive with port)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signaling_url: Option<String>,

    /// Pipeline manifest (JSON object)
    pub manifest: serde_json::Value,

    /// STUN server URLs (at least one required)
    pub stun_servers: Vec<String>,

    /// TURN server configurations (optional)
    #[serde(default)]
    pub turn_servers: Vec<TurnServerConfig>,

    /// Maximum concurrent peers (1-10, default 10)
    #[serde(default = "default_max_peers")]
    pub max_peers: u32,

    /// Audio codec (only opus supported)
    #[serde(default)]
    pub audio_codec: AudioCodec,

    /// Video codec (vp8, vp9, h264)
    #[serde(default)]
    pub video_codec: VideoCodec,

    /// Reconnection configuration for external signaling mode
    #[serde(default)]
    pub reconnect: ReconnectConfig,
}

fn default_max_peers() -> u32 {
    10
}

impl Default for WebRtcServerConfig {
    fn default() -> Self {
        Self {
            port: None,
            signaling_url: None,
            manifest: serde_json::json!({"nodes": [], "connections": []}),
            stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
            turn_servers: Vec::new(),
            max_peers: 10,
            audio_codec: AudioCodec::Opus,
            video_codec: VideoCodec::Vp9,
            reconnect: ReconnectConfig::default(),
        }
    }
}

impl WebRtcServerConfig {
    /// Validate the configuration
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        // Check mutual exclusivity of port and signaling_url
        match (&self.port, &self.signaling_url) {
            (None, None) => {
                return Err(ConfigValidationError::MissingSignalingConfig);
            }
            (Some(_), Some(_)) => {
                return Err(ConfigValidationError::ConflictingSignalingConfig);
            }
            _ => {}
        }

        // Validate port range
        if let Some(port) = self.port {
            if port == 0 {
                return Err(ConfigValidationError::InvalidPort(port));
            }
        }

        // Validate signaling URL format
        if let Some(ref url) = self.signaling_url {
            if !url.starts_with("grpc://") && !url.starts_with("grpcs://") {
                return Err(ConfigValidationError::InvalidSignalingUrl(url.clone()));
            }
        }

        // Validate STUN servers
        if self.stun_servers.is_empty() {
            return Err(ConfigValidationError::NoStunServers);
        }

        for stun in &self.stun_servers {
            if !stun.starts_with("stun:") && !stun.starts_with("stuns:") {
                return Err(ConfigValidationError::InvalidStunUrl(stun.clone()));
            }
        }

        // Validate TURN servers
        for turn in &self.turn_servers {
            if !turn.url.starts_with("turn:") && !turn.url.starts_with("turns:") {
                return Err(ConfigValidationError::InvalidTurnUrl(turn.url.clone()));
            }
            if turn.username.is_empty() {
                return Err(ConfigValidationError::MissingTurnUsername);
            }
            if turn.credential.is_empty() {
                return Err(ConfigValidationError::MissingTurnCredential);
            }
        }

        // Validate max_peers
        if self.max_peers == 0 || self.max_peers > 10 {
            return Err(ConfigValidationError::InvalidMaxPeers(self.max_peers));
        }

        Ok(())
    }

    /// Check if this config uses embedded signaling
    pub fn is_embedded(&self) -> bool {
        self.port.is_some()
    }

    /// Check if this config uses external signaling
    pub fn is_external(&self) -> bool {
        self.signaling_url.is_some()
    }

    /// Convert to remotemedia-webrtc WebRtcTransportConfig
    #[cfg(feature = "webrtc")]
    pub fn to_webrtc_config(&self) -> remotemedia_webrtc::config::WebRtcTransportConfig {
        use remotemedia_webrtc::config::{
            ConfigOptions, DataChannelMode, WebRtcTransportConfig as WebRtcConfig,
        };

        let signaling_url = if let Some(ref url) = self.signaling_url {
            url.clone()
        } else if let Some(port) = self.port {
            format!("grpc://127.0.0.1:{}", port)
        } else {
            "grpc://127.0.0.1:50051".to_string()
        };

        let turn_servers = self
            .turn_servers
            .iter()
            .map(|t| remotemedia_webrtc::config::TurnServerConfig {
                url: t.url.clone(),
                username: t.username.clone(),
                credential: t.credential.clone(),
            })
            .collect();

        WebRtcConfig {
            signaling_url,
            stun_servers: self.stun_servers.clone(),
            turn_servers,
            peer_id: None, // Auto-generated
            max_peers: self.max_peers,
            audio_codec: remotemedia_webrtc::config::AudioCodec::Opus,
            video_codec: self.video_codec.to_webrtc_codec(),
            enable_data_channel: true,
            data_channel_mode: DataChannelMode::Reliable,
            jitter_buffer_size_ms: 100,
            enable_rtcp: true,
            rtcp_interval_ms: 1000,
            options: ConfigOptions::default(),
            shared_session_key: None,
        }
    }
}


/// Configuration validation errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum ConfigValidationError {
    #[error("Either port (embedded) or signaling_url (external) must be specified")]
    MissingSignalingConfig,

    #[error("Cannot specify both port and signaling_url - choose embedded or external signaling")]
    ConflictingSignalingConfig,

    #[error("Invalid port: {0} (must be non-zero)")]
    InvalidPort(u16),

    #[error("Invalid signaling URL: {0} (must start with grpc:// or grpcs://)")]
    InvalidSignalingUrl(String),

    #[error("At least one STUN server is required")]
    NoStunServers,

    #[error("Invalid STUN URL: {0} (must start with stun: or stuns:)")]
    InvalidStunUrl(String),

    #[error("Invalid TURN URL: {0} (must start with turn: or turns:)")]
    InvalidTurnUrl(String),

    #[error("TURN server username is required")]
    MissingTurnUsername,

    #[error("TURN server credential is required")]
    MissingTurnCredential,

    #[error("Invalid max_peers: {0} (must be 1-10)")]
    InvalidMaxPeers(u32),
}

/// Peer capabilities (audio/video/data support)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PeerCapabilities {
    /// Supports audio tracks
    pub audio: bool,
    /// Supports video tracks
    pub video: bool,
    /// Supports data channels
    pub data: bool,
}

/// Peer connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PeerState {
    Connecting,
    Connected,
    Disconnecting,
    Disconnected,
}

impl Default for PeerState {
    fn default() -> Self {
        Self::Connecting
    }
}

/// Information about a connected peer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Unique peer identifier
    pub peer_id: String,
    /// Peer's audio/video/data capabilities
    pub capabilities: PeerCapabilities,
    /// Custom peer metadata
    pub metadata: HashMap<String, String>,
    /// Connection state
    pub state: PeerState,
    /// Unix timestamp when peer connected
    pub connected_at: u64,
}

/// Server state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServerState {
    Created,
    Starting,
    Running,
    Stopping,
    Stopped,
}

impl Default for ServerState {
    fn default() -> Self {
        Self::Created
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_embedded_config() {
        let config = WebRtcServerConfig {
            port: Some(50051),
            signaling_url: None,
            manifest: serde_json::json!({}),
            stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
            turn_servers: vec![],
            max_peers: 10,
            audio_codec: AudioCodec::Opus,
            video_codec: VideoCodec::Vp9,
            reconnect: ReconnectConfig::default(),
        };
        assert!(config.validate().is_ok());
        assert!(config.is_embedded());
        assert!(!config.is_external());
    }

    #[test]
    fn test_validate_external_config() {
        let config = WebRtcServerConfig {
            port: None,
            signaling_url: Some("grpc://signaling.example.com:50051".to_string()),
            manifest: serde_json::json!({}),
            stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
            turn_servers: vec![],
            max_peers: 5,
            audio_codec: AudioCodec::Opus,
            video_codec: VideoCodec::Vp8,
            reconnect: ReconnectConfig::default(),
        };
        assert!(config.validate().is_ok());
        assert!(!config.is_embedded());
        assert!(config.is_external());
    }

    #[test]
    fn test_validate_missing_signaling() {
        let config = WebRtcServerConfig {
            port: None,
            signaling_url: None,
            manifest: serde_json::json!({}),
            stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigValidationError::MissingSignalingConfig)
        ));
    }

    #[test]
    fn test_validate_conflicting_signaling() {
        let config = WebRtcServerConfig {
            port: Some(50051),
            signaling_url: Some("grpc://example.com:50051".to_string()),
            manifest: serde_json::json!({}),
            stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigValidationError::ConflictingSignalingConfig)
        ));
    }

    #[test]
    fn test_validate_no_stun_servers() {
        let config = WebRtcServerConfig {
            port: Some(50051),
            signaling_url: None,
            manifest: serde_json::json!({}),
            stun_servers: vec![],
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigValidationError::NoStunServers)
        ));
    }

    #[test]
    fn test_validate_invalid_max_peers() {
        let config = WebRtcServerConfig {
            port: Some(50051),
            signaling_url: None,
            manifest: serde_json::json!({}),
            stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
            max_peers: 100,
            ..Default::default()
        };
        assert!(matches!(
            config.validate(),
            Err(ConfigValidationError::InvalidMaxPeers(100))
        ));
    }
}
