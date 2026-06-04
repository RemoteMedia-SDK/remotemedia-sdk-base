//! Configuration types for WebRTC transport

use serde::{Deserialize, Serialize};

/// Main configuration for WebRtcTransport
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebRtcTransportConfig {
    /// WebSocket signaling server URL (ws:// or wss://)
    pub signaling_url: String,

    /// STUN server URLs (at least one required)
    pub stun_servers: Vec<String>,

    /// TURN server configurations (optional)
    pub turn_servers: Vec<TurnServerConfig>,

    /// Local peer ID (auto-generated if None)
    pub peer_id: Option<String>,

    /// Maximum peers in mesh (default: 10, max: 10)
    pub max_peers: u32,

    /// Audio codec preference (default: Opus)
    pub audio_codec: AudioCodec,

    /// Video codec preference (default: VP9)
    pub video_codec: VideoCodec,

    /// Enable data channel (default: true)
    pub enable_data_channel: bool,

    /// Data channel mode (default: Reliable)
    pub data_channel_mode: DataChannelMode,

    /// Optional logical key for sharing one core pipeline session across transports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared_session_key: Option<String>,

    /// Jitter buffer size in milliseconds (default: 50ms, range: 50-200ms)
    pub jitter_buffer_size_ms: u32,

    /// Enable RTCP Sender Reports (default: true, required for A/V sync)
    pub enable_rtcp: bool,

    /// RTCP report interval in milliseconds (default: 5000ms)
    pub rtcp_interval_ms: u32,

    /// Additional configuration options
    pub options: ConfigOptions,
}

/// TURN server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnServerConfig {
    /// TURN server URL (turn:// or turns://)
    pub url: String,

    /// Username for TURN authentication
    pub username: String,

    /// Credential for TURN authentication
    pub credential: String,
}

/// Additional configuration options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigOptions {
    /// Enable adaptive bitrate adjustment (default: true)
    pub adaptive_bitrate_enabled: bool,

    /// Target bitrate in kbps (default: 2000)
    pub target_bitrate_kbps: u32,

    /// Maximum video resolution (default: 720p)
    pub max_video_resolution: VideoResolution,

    /// Video framerate in fps (default: 30)
    pub video_framerate_fps: u32,

    /// ICE connection timeout in seconds (default: 30)
    pub ice_timeout_secs: u32,

    /// Maximum reconnection attempts (default: 5)
    pub max_reconnect_retries: u32,

    /// Initial reconnection backoff in milliseconds (default: 1000)
    pub reconnect_backoff_initial_ms: u64,

    /// Maximum reconnection backoff in milliseconds (default: 30000)
    pub reconnect_backoff_max_ms: u64,

    /// Reconnection backoff multiplier (default: 2.0)
    pub reconnect_backoff_multiplier: f64,
}

/// Supported audio codecs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AudioCodec {
    /// Opus codec (default, required for WebRTC)
    Opus,
}

/// Supported video codecs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VideoCodec {
    /// VP8 codec (WebRTC standard, wide compatibility)
    VP8,
    /// VP9 codec (better compression, modern browsers)
    VP9,
    /// H.264 codec (universal compatibility)
    H264,
}

/// Video resolution options
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VideoResolution {
    /// 640x480
    #[serde(rename = "480p")]
    P480,
    /// 1280x720
    #[serde(rename = "720p")]
    P720,
    /// 1920x1080
    #[serde(rename = "1080p")]
    P1080,
}

/// Data channel mode (T159)
///
/// Determines the reliability of message delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataChannelMode {
    /// Reliable, ordered delivery (default)
    ///
    /// Messages are guaranteed to arrive in order and without loss.
    /// Best for control messages and configuration.
    Reliable,
    /// Unreliable, unordered delivery (low latency)
    ///
    /// Messages may arrive out of order or be lost.
    /// Best for real-time data where latency is critical.
    Unreliable,
}

impl DataChannelMode {
    /// Get the ordered setting for webrtc-rs
    pub fn ordered(&self) -> bool {
        match self {
            DataChannelMode::Reliable => true,
            DataChannelMode::Unreliable => false,
        }
    }

    /// Get the max retransmits setting for webrtc-rs
    pub fn max_retransmits(&self) -> Option<u16> {
        match self {
            DataChannelMode::Reliable => None,      // Unlimited retransmits
            DataChannelMode::Unreliable => Some(0), // No retransmits
        }
    }
}

impl Default for WebRtcTransportConfig {
    fn default() -> Self {
        Self {
            signaling_url: "ws://localhost:8080".to_string(),
            stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
            turn_servers: Vec::new(),
            peer_id: None,
            shared_session_key: None,
            max_peers: 10,
            audio_codec: AudioCodec::Opus,
            video_codec: VideoCodec::VP9,
            enable_data_channel: true,
            data_channel_mode: DataChannelMode::Reliable,
            jitter_buffer_size_ms: 50,
            enable_rtcp: true,
            rtcp_interval_ms: 5000,
            options: ConfigOptions::default(),
        }
    }
}

impl Default for ConfigOptions {
    fn default() -> Self {
        Self {
            adaptive_bitrate_enabled: true,
            target_bitrate_kbps: 2000,
            max_video_resolution: VideoResolution::P720,
            video_framerate_fps: 30,
            ice_timeout_secs: 30,
            max_reconnect_retries: 5,
            reconnect_backoff_initial_ms: 1000,
            reconnect_backoff_max_ms: 30000,
            reconnect_backoff_multiplier: 2.0,
        }
    }
}

impl WebRtcTransportConfig {
    /// Validate configuration parameters
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `stun_servers` is empty
    /// - `max_peers` is not in range 1-10
    /// - `jitter_buffer_size_ms` is not in range 50-200
    /// - `rtcp_interval_ms` is not in range 1000-10000
    /// - `signaling_url` is not a valid WebSocket URL
    pub fn validate(&self) -> crate::Result<()> {
        use crate::Error;

        // Validate STUN servers
        if self.stun_servers.is_empty() {
            return Err(Error::InvalidConfig(
                "At least one STUN server is required".to_string(),
            ));
        }

        // Validate max peers
        if self.max_peers == 0 || self.max_peers > 10 {
            return Err(Error::InvalidConfig(format!(
                "max_peers must be in range 1-10, got {}",
                self.max_peers
            )));
        }

        // Validate jitter buffer size
        if self.jitter_buffer_size_ms < 50 || self.jitter_buffer_size_ms > 200 {
            return Err(Error::InvalidConfig(format!(
                "jitter_buffer_size_ms must be in range 50-200, got {}",
                self.jitter_buffer_size_ms
            )));
        }

        // Validate RTCP interval
        if self.rtcp_interval_ms < 1000 || self.rtcp_interval_ms > 10000 {
            return Err(Error::InvalidConfig(format!(
                "rtcp_interval_ms must be in range 1000-10000, got {}",
                self.rtcp_interval_ms
            )));
        }

        // Validate signaling URL
        if !self.signaling_url.starts_with("ws://") && !self.signaling_url.starts_with("wss://") {
            return Err(Error::InvalidConfig(format!(
                "signaling_url must start with ws:// or wss://, got {}",
                self.signaling_url
            )));
        }

        Ok(())
    }

    /// Create a configuration preset optimized for low latency
    ///
    /// Best for real-time audio/video applications where minimal delay
    /// is more important than quality.
    ///
    /// Settings:
    /// - Jitter buffer: 50ms (minimum)
    /// - RTCP interval: 2000ms (faster feedback)
    /// - Adaptive bitrate: enabled
    /// - Data channel: unreliable mode (lower latency)
    ///
    /// # Example
    ///
    /// ```
    /// use remotemedia_webrtc::config::WebRtcTransportConfig;
    ///
    /// let config = WebRtcTransportConfig::low_latency_preset("ws://localhost:8080");
    /// assert_eq!(config.jitter_buffer_size_ms, 50);
    /// assert_eq!(config.rtcp_interval_ms, 2000);
    /// ```
    pub fn low_latency_preset(signaling_url: &str) -> Self {
        Self {
            signaling_url: signaling_url.to_string(),
            stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
            turn_servers: Vec::new(),
            peer_id: None,
            shared_session_key: None,
            max_peers: 10,
            audio_codec: AudioCodec::Opus,
            video_codec: VideoCodec::VP8, // VP8 has lower encoding latency than VP9
            enable_data_channel: true,
            data_channel_mode: DataChannelMode::Unreliable, // Lower latency
            jitter_buffer_size_ms: 50,                      // Minimum jitter buffer
            enable_rtcp: true,
            rtcp_interval_ms: 2000, // Faster RTCP feedback
            options: ConfigOptions {
                adaptive_bitrate_enabled: true,
                target_bitrate_kbps: 1500, // Moderate bitrate for fast encoding
                max_video_resolution: VideoResolution::P720,
                video_framerate_fps: 30,
                ice_timeout_secs: 15, // Faster timeout for quick reconnects
                // Aggressive reconnection for low latency scenarios
                max_reconnect_retries: 10,
                reconnect_backoff_initial_ms: 500,
                reconnect_backoff_max_ms: 10000,
                reconnect_backoff_multiplier: 1.5,
            },
        }
    }

    /// Create a configuration preset optimized for high quality
    ///
    /// Best for applications where video/audio quality is more important
    /// than latency, such as recording or broadcasting.
    ///
    /// Settings:
    /// - Jitter buffer: 100ms (smoother playback)
    /// - RTCP interval: 5000ms (standard)
    /// - Higher bitrate: 4000 kbps
    /// - Video: VP9 codec, 1080p resolution
    ///
    /// # Example
    ///
    /// ```
    /// use remotemedia_webrtc::config::WebRtcTransportConfig;
    ///
    /// let config = WebRtcTransportConfig::high_quality_preset("ws://localhost:8080");
    /// assert_eq!(config.jitter_buffer_size_ms, 100);
    /// assert_eq!(config.options.target_bitrate_kbps, 4000);
    /// ```
    pub fn high_quality_preset(signaling_url: &str) -> Self {
        Self {
            signaling_url: signaling_url.to_string(),
            stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
            turn_servers: Vec::new(),
            peer_id: None,
            shared_session_key: None,
            max_peers: 10,
            audio_codec: AudioCodec::Opus,
            video_codec: VideoCodec::VP9, // Better compression for higher quality
            enable_data_channel: true,
            data_channel_mode: DataChannelMode::Reliable,
            jitter_buffer_size_ms: 100, // Larger buffer for smoother playback
            enable_rtcp: true,
            rtcp_interval_ms: 5000,
            options: ConfigOptions {
                adaptive_bitrate_enabled: true,
                target_bitrate_kbps: 4000, // Higher bitrate for quality
                max_video_resolution: VideoResolution::P1080, // Full HD
                video_framerate_fps: 30,
                ice_timeout_secs: 30,
                // Standard reconnection for high quality scenarios
                max_reconnect_retries: 5,
                reconnect_backoff_initial_ms: 1000,
                reconnect_backoff_max_ms: 30000,
                reconnect_backoff_multiplier: 2.0,
            },
        }
    }

    /// Create a configuration preset optimized for mobile networks
    ///
    /// Best for applications running on cellular or unstable networks
    /// where connectivity is unreliable.
    ///
    /// Settings:
    /// - Jitter buffer: 150ms (larger buffer for network jitter)
    /// - Lower bitrate: 800 kbps (bandwidth conservation)
    /// - Video: 480p resolution, VP8 codec (hardware decoding)
    /// - ICE timeout: 45 seconds (more time for cellular handoffs)
    /// - Requires TURN servers (set via `with_turn_servers()`)
    ///
    /// # Example
    ///
    /// ```
    /// use remotemedia_webrtc::config::{WebRtcTransportConfig, TurnServerConfig};
    ///
    /// let config = WebRtcTransportConfig::mobile_network_preset("ws://localhost:8080")
    ///     .with_turn_servers(vec![
    ///         TurnServerConfig {
    ///             url: "turn:turn.example.com:3478".to_string(),
    ///             username: "user".to_string(),
    ///             credential: "pass".to_string(),
    ///         }
    ///     ]);
    /// assert_eq!(config.jitter_buffer_size_ms, 150);
    /// ```
    pub fn mobile_network_preset(signaling_url: &str) -> Self {
        Self {
            signaling_url: signaling_url.to_string(),
            stun_servers: vec![
                "stun:stun.l.google.com:19302".to_string(),
                "stun:stun1.l.google.com:19302".to_string(), // Backup STUN
            ],
            turn_servers: Vec::new(), // Set via with_turn_servers()
            peer_id: None,
            shared_session_key: None,
            max_peers: 5, // Fewer peers for bandwidth conservation
            audio_codec: AudioCodec::Opus,
            video_codec: VideoCodec::VP8, // Better hardware decoding support
            enable_data_channel: true,
            data_channel_mode: DataChannelMode::Reliable, // Ensure delivery over flaky connections
            jitter_buffer_size_ms: 150,                   // Larger buffer for network jitter
            enable_rtcp: true,
            rtcp_interval_ms: 3000, // More frequent feedback for adaptation
            options: ConfigOptions {
                adaptive_bitrate_enabled: true,
                target_bitrate_kbps: 800, // Lower bitrate for bandwidth conservation
                max_video_resolution: VideoResolution::P480, // Lower resolution
                video_framerate_fps: 24,  // Lower framerate saves bandwidth
                ice_timeout_secs: 45,     // More time for cellular handoffs
                // Conservative reconnection for unreliable mobile networks
                max_reconnect_retries: 15, // More retries for flaky connections
                reconnect_backoff_initial_ms: 2000, // Longer initial backoff
                reconnect_backoff_max_ms: 60000, // Up to 1 minute between retries
                reconnect_backoff_multiplier: 1.5, // Slower backoff growth
            },
        }
    }

    /// Add TURN servers to this configuration
    ///
    /// Useful for chaining with preset methods.
    ///
    /// # Example
    ///
    /// ```
    /// use remotemedia_webrtc::config::{WebRtcTransportConfig, TurnServerConfig};
    ///
    /// let config = WebRtcTransportConfig::mobile_network_preset("ws://localhost:8080")
    ///     .with_turn_servers(vec![
    ///         TurnServerConfig {
    ///             url: "turn:turn.example.com:3478".to_string(),
    ///             username: "user".to_string(),
    ///             credential: "pass".to_string(),
    ///         }
    ///     ]);
    /// ```
    pub fn with_turn_servers(mut self, turn_servers: Vec<TurnServerConfig>) -> Self {
        self.turn_servers = turn_servers;
        self
    }

    /// Set the peer ID for this configuration
    ///
    /// Useful for chaining with preset methods.
    pub fn with_peer_id(mut self, peer_id: &str) -> Self {
        self.peer_id = Some(peer_id.to_string());
        self
    }

    /// Set the maximum number of peers
    ///
    /// Useful for chaining with preset methods.
    pub fn with_max_peers(mut self, max_peers: u32) -> Self {
        self.max_peers = max_peers;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_valid() {
        let config = WebRtcTransportConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_empty_stun_servers_fails() {
        let mut config = WebRtcTransportConfig::default();
        config.stun_servers.clear();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_max_peers_fails() {
        let mut config = WebRtcTransportConfig::default();
        config.max_peers = 0;
        assert!(config.validate().is_err());

        config.max_peers = 11;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_jitter_buffer_size_fails() {
        let mut config = WebRtcTransportConfig::default();
        config.jitter_buffer_size_ms = 49;
        assert!(config.validate().is_err());

        config.jitter_buffer_size_ms = 201;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_invalid_signaling_url_fails() {
        let mut config = WebRtcTransportConfig::default();
        config.signaling_url = "http://localhost:8080".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_serialization() {
        let config = WebRtcTransportConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: WebRtcTransportConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.signaling_url, deserialized.signaling_url);
    }

    #[test]
    fn test_low_latency_preset() {
        let config = WebRtcTransportConfig::low_latency_preset("ws://localhost:8080");
        assert!(config.validate().is_ok());
        assert_eq!(config.jitter_buffer_size_ms, 50);
        assert_eq!(config.rtcp_interval_ms, 2000);
        assert_eq!(config.data_channel_mode, DataChannelMode::Unreliable);
        assert_eq!(config.video_codec, VideoCodec::VP8);
        assert_eq!(config.options.ice_timeout_secs, 15);
    }

    #[test]
    fn test_high_quality_preset() {
        let config = WebRtcTransportConfig::high_quality_preset("ws://localhost:8080");
        assert!(config.validate().is_ok());
        assert_eq!(config.jitter_buffer_size_ms, 100);
        assert_eq!(config.options.target_bitrate_kbps, 4000);
        assert_eq!(config.options.max_video_resolution, VideoResolution::P1080);
        assert_eq!(config.video_codec, VideoCodec::VP9);
    }

    #[test]
    fn test_mobile_network_preset() {
        let config = WebRtcTransportConfig::mobile_network_preset("ws://localhost:8080");
        assert!(config.validate().is_ok());
        assert_eq!(config.jitter_buffer_size_ms, 150);
        assert_eq!(config.options.target_bitrate_kbps, 800);
        assert_eq!(config.options.max_video_resolution, VideoResolution::P480);
        assert_eq!(config.max_peers, 5);
        assert_eq!(config.options.ice_timeout_secs, 45);
        assert_eq!(config.stun_servers.len(), 2); // Backup STUN
    }

    #[test]
    fn test_preset_with_turn_servers() {
        let config = WebRtcTransportConfig::mobile_network_preset("ws://localhost:8080")
            .with_turn_servers(vec![TurnServerConfig {
                url: "turn:turn.example.com:3478".to_string(),
                username: "user".to_string(),
                credential: "pass".to_string(),
            }]);
        assert!(config.validate().is_ok());
        assert_eq!(config.turn_servers.len(), 1);
        assert_eq!(config.turn_servers[0].url, "turn:turn.example.com:3478");
    }

    #[test]
    fn test_preset_builder_chain() {
        let config = WebRtcTransportConfig::low_latency_preset("ws://localhost:8080")
            .with_peer_id("my-peer")
            .with_max_peers(5);
        assert!(config.validate().is_ok());
        assert_eq!(config.peer_id, Some("my-peer".to_string()));
        assert_eq!(config.max_peers, 5);
    }
}
