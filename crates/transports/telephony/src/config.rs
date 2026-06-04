//! Configuration for the SIP/RTP telephony transport.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, ToSocketAddrs};

const DEFAULT_SIP_BIND_ADDRESS: &str = "0.0.0.0:5060";
const DEFAULT_RTP_PORT_START: u16 = 16_384;
const DEFAULT_RTP_PORT_END: u16 = 32_767;
const DEFAULT_JITTER_BUFFER_MS: u16 = 60;
const DEFAULT_MAX_JITTER_BUFFER_MS: u16 = 200;
const DEFAULT_FRAME_DURATION_MS: u16 = 20;
const DEFAULT_MAX_ACTIVE_CALLS: u32 = 128;
const DEFAULT_MAX_RTP_SESSIONS: u32 = 256;
const DEFAULT_MAX_SIP_DATAGRAM_BYTES: usize = 65_507;
const DEFAULT_RATE_LIMIT_MAX_REQUESTS: u32 = 10;
const DEFAULT_RATE_LIMIT_WINDOW_SECONDS: u64 = 60;
const DEFAULT_RATE_LIMIT_BAN_DURATION_SECONDS: u64 = 300;

/// Peer access control mode for SIP connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SipAccessMode {
    /// Only allow peers explicitly listed in `allowed_peers`.
    /// If `allowed_peers` is empty, reject all incoming traffic.
    AllowList,
    /// Reject peers explicitly listed in `blocked_peers`.
    /// If `blocked_peers` is empty, allow all incoming traffic.
    DenyList,
}

impl Default for SipAccessMode {
    fn default() -> Self {
        Self::AllowList
    }
}

/// Rate limiting configuration for per-peer SIP request throttling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SipRateLimitConfig {
    /// Maximum SIP requests per peer per time window.
    pub max_requests_per_window: u32,
    /// Sliding window duration in seconds.
    pub window_seconds: u64,
    /// How long a banned peer remains banned, in seconds.
    pub ban_duration_seconds: u64,
    /// Whether rate limiting is active.
    pub enabled: bool,
}

impl Default for SipRateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests_per_window: DEFAULT_RATE_LIMIT_MAX_REQUESTS,
            window_seconds: DEFAULT_RATE_LIMIT_WINDOW_SECONDS,
            ban_duration_seconds: DEFAULT_RATE_LIMIT_BAN_DURATION_SECONDS,
            enabled: false,
        }
    }
}

/// Audio codecs supported by the telephony transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AudioCodec {
    /// G.711 mu-law, usually RTP payload type 0 at 8 kHz.
    Pcmu,
    /// G.711 A-law, usually RTP payload type 8 at 8 kHz.
    Pcma,
    /// Opus, usually negotiated as dynamic RTP payload type at 48 kHz.
    Opus,
}

impl AudioCodec {
    /// RTP clock rate for the codec.
    pub fn clock_rate_hz(self) -> u32 {
        match self {
            AudioCodec::Pcmu | AudioCodec::Pcma => 8_000,
            AudioCodec::Opus => 48_000,
        }
    }
}

/// Jitter-buffer and packet-loss handling configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JitterConfig {
    /// Initial jitter buffer target in milliseconds.
    pub target_ms: u16,
    /// Maximum jitter buffer growth in milliseconds.
    pub max_ms: u16,
    /// Enable packet loss concealment when packets arrive too late or are missing.
    pub packet_loss_concealment: bool,
}

impl Default for JitterConfig {
    fn default() -> Self {
        Self {
            target_ms: DEFAULT_JITTER_BUFFER_MS,
            max_ms: DEFAULT_MAX_JITTER_BUFFER_MS,
            packet_loss_concealment: true,
        }
    }
}

/// Conference media routing configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConferenceConfig {
    /// Enable active media injection into multiple call legs.
    pub enabled: bool,
    /// Maximum call legs that may participate in one conference session.
    pub max_legs: u8,
    /// Prevent injected bot audio from being reflected back into STT channels.
    pub suppress_injected_audio_feedback: bool,
}

impl Default for ConferenceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_legs: 3,
            suppress_injected_audio_feedback: true,
        }
    }
}

/// Hermes tool gateway integration configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HermesToolConfig {
    /// Optional Hermes gateway endpoint.
    pub endpoint: String,
    /// Optional authentication token.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
}

/// Top-level telephony transport configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelephonyTransportConfig {
    /// Address for the SIP listener.
    pub sip_bind_address: String,
    /// Optional address advertised in SDP media answers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub advertised_media_address: Option<String>,
    /// First UDP port available for RTP sockets.
    pub rtp_port_start: u16,
    /// Last UDP port available for RTP sockets.
    pub rtp_port_end: u16,
    /// Codec preference order for SDP negotiation.
    pub codec_preferences: Vec<AudioCodec>,
    /// Normalized audio frame duration handed to the pipeline.
    pub frame_duration_ms: u16,
    /// Jitter-buffer configuration.
    pub jitter: JitterConfig,
    /// Maximum active SIP call sessions.
    pub max_active_calls: u32,
    /// Maximum active RTP media sessions.
    pub max_rtp_sessions: u32,
    /// Maximum accepted SIP UDP datagram size.
    pub max_sip_datagram_bytes: usize,
    /// Optional allow-list of SIP peer addresses or CIDR labels.
    pub allowed_peers: Vec<String>,
    /// Access control mode for SIP peers.
    #[serde(default)]
    pub access_mode: SipAccessMode,
    /// Optional block-list of SIP peer addresses or CIDR labels.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocked_peers: Vec<String>,
    /// Per-peer SIP rate-limiting configuration.
    #[serde(default)]
    pub rate_limit: SipRateLimitConfig,
    /// Enable SIPREC mirrored-call ingestion.
    pub enable_siprec: bool,
    /// Optional logical key for sharing one core pipeline session across transports.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shared_session_key: Option<String>,
    /// Conference and media-injection behavior.
    pub conference: ConferenceConfig,
    /// Optional Hermes tool gateway integration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hermes: Option<HermesToolConfig>,
}

impl Default for TelephonyTransportConfig {
    fn default() -> Self {
        Self {
            sip_bind_address: DEFAULT_SIP_BIND_ADDRESS.to_string(),
            advertised_media_address: None,
            rtp_port_start: DEFAULT_RTP_PORT_START,
            rtp_port_end: DEFAULT_RTP_PORT_END,
            codec_preferences: vec![AudioCodec::Opus, AudioCodec::Pcmu, AudioCodec::Pcma],
            frame_duration_ms: DEFAULT_FRAME_DURATION_MS,
            jitter: JitterConfig::default(),
            max_active_calls: DEFAULT_MAX_ACTIVE_CALLS,
            max_rtp_sessions: DEFAULT_MAX_RTP_SESSIONS,
            max_sip_datagram_bytes: DEFAULT_MAX_SIP_DATAGRAM_BYTES,
            allowed_peers: Vec::new(),
            access_mode: SipAccessMode::AllowList,
            blocked_peers: Vec::new(),
            rate_limit: SipRateLimitConfig::default(),
            enable_siprec: false,
            shared_session_key: None,
            conference: ConferenceConfig::default(),
            hermes: None,
        }
    }
}

impl TelephonyTransportConfig {
    /// Build a config using a generic transport server bind address.
    pub fn from_bind_address(address: impl Into<String>) -> Self {
        Self {
            sip_bind_address: address.into(),
            ..Self::default()
        }
    }

    /// Parse config from transport plugin extra config.
    pub fn from_json(value: &serde_json::Value) -> Result<Self> {
        if value.is_null() || value.as_object().is_some_and(serde_json::Map::is_empty) {
            return Ok(Self::default());
        }

        let config: Self = serde_json::from_value(value.clone())?;
        config.validate()?;
        Ok(config)
    }

    /// Validate configuration before listeners or sessions are created.
    pub fn validate(&self) -> Result<()> {
        parse_socket_addr(&self.sip_bind_address, "sip_bind_address")?;

        if let Some(address) = &self.advertised_media_address {
            if address.trim().is_empty() {
                return Err(Error::InvalidConfig(
                    "advertised_media_address must not be empty when set".to_string(),
                ));
            }
        }

        if self.rtp_port_start == 0 || self.rtp_port_end == 0 {
            return Err(Error::InvalidConfig(
                "RTP ports must be non-zero UDP ports".to_string(),
            ));
        }

        if self.rtp_port_start > self.rtp_port_end {
            return Err(Error::InvalidConfig(format!(
                "rtp_port_start ({}) must be <= rtp_port_end ({})",
                self.rtp_port_start, self.rtp_port_end
            )));
        }

        let port_count = u32::from(self.rtp_port_end) - u32::from(self.rtp_port_start) + 1;
        if port_count < 2 {
            return Err(Error::InvalidConfig(
                "RTP port range must contain at least two ports".to_string(),
            ));
        }

        if self.codec_preferences.is_empty() {
            return Err(Error::InvalidConfig(
                "at least one codec preference is required".to_string(),
            ));
        }

        if self.frame_duration_ms != DEFAULT_FRAME_DURATION_MS {
            return Err(Error::InvalidConfig(format!(
                "frame_duration_ms must be {} for the initial telephony transport",
                DEFAULT_FRAME_DURATION_MS
            )));
        }

        if self.jitter.target_ms == 0 {
            return Err(Error::InvalidConfig(
                "jitter.target_ms must be greater than zero".to_string(),
            ));
        }

        if self.jitter.target_ms > self.jitter.max_ms {
            return Err(Error::InvalidConfig(format!(
                "jitter.target_ms ({}) must be <= jitter.max_ms ({})",
                self.jitter.target_ms, self.jitter.max_ms
            )));
        }

        if self.max_active_calls == 0 {
            return Err(Error::InvalidConfig(
                "max_active_calls must be greater than zero".to_string(),
            ));
        }

        if self.max_rtp_sessions < self.max_active_calls {
            return Err(Error::InvalidConfig(
                "max_rtp_sessions must be >= max_active_calls".to_string(),
            ));
        }

        if self.max_sip_datagram_bytes < 512 {
            return Err(Error::InvalidConfig(
                "max_sip_datagram_bytes must be at least 512".to_string(),
            ));
        }

        if self.rate_limit.enabled {
            if self.rate_limit.max_requests_per_window == 0 {
                return Err(Error::InvalidConfig(
                    "rate_limit.max_requests_per_window must be greater than zero when rate limiting is enabled".to_string(),
                ));
            }
            if self.rate_limit.window_seconds == 0 {
                return Err(Error::InvalidConfig(
                    "rate_limit.window_seconds must be greater than zero when rate limiting is enabled".to_string(),
                ));
            }
            if self.rate_limit.ban_duration_seconds == 0 {
                return Err(Error::InvalidConfig(
                    "rate_limit.ban_duration_seconds must be greater than zero when rate limiting is enabled".to_string(),
                ));
            }
        }

        if self.conference.enabled && self.conference.max_legs < 2 {
            return Err(Error::InvalidConfig(
                "conference.max_legs must be at least 2 when conferencing is enabled".to_string(),
            ));
        }

        if let Some(hermes) = &self.hermes {
            if hermes.endpoint.trim().is_empty() {
                return Err(Error::InvalidConfig(
                    "hermes.endpoint must not be empty when Hermes integration is configured"
                        .to_string(),
                ));
            }
        }

        Ok(())
    }
}

fn parse_socket_addr(value: &str, field: &str) -> Result<SocketAddr> {
    value
        .to_socket_addrs()
        .map_err(|e| Error::InvalidConfig(format!("{field} is invalid: {e}")))?
        .next()
        .ok_or_else(|| Error::InvalidConfig(format!("{field} did not resolve to an address")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_config_is_valid() {
        TelephonyTransportConfig::default().validate().unwrap();
    }

    #[test]
    fn parses_config_from_json() {
        let config = TelephonyTransportConfig::from_json(&json!({
            "sip_bind_address": "127.0.0.1:5060",
            "rtp_port_start": 20000,
            "rtp_port_end": 20100,
            "codec_preferences": ["pcmu", "opus"],
            "frame_duration_ms": 20,
            "jitter": {
                "target_ms": 40,
                "max_ms": 120,
                "packet_loss_concealment": true
            },
            "max_active_calls": 10,
            "max_rtp_sessions": 20,
            "max_sip_datagram_bytes": 4096,
            "allowed_peers": ["127.0.0.1"],
            "enable_siprec": true,
            "conference": {
                "enabled": true,
                "max_legs": 3,
                "suppress_injected_audio_feedback": true
            },
            "hermes": {
                "endpoint": "http://127.0.0.1:8787"
            }
        }))
        .unwrap();

        assert_eq!(config.sip_bind_address, "127.0.0.1:5060");
        assert_eq!(
            config.codec_preferences,
            vec![AudioCodec::Pcmu, AudioCodec::Opus]
        );
        assert!(config.enable_siprec);
        assert!(config.conference.enabled);
    }

    #[test]
    fn rejects_invalid_rtp_range() {
        let config = TelephonyTransportConfig {
            rtp_port_start: 30_000,
            rtp_port_end: 20_000,
            ..TelephonyTransportConfig::default()
        };

        assert!(matches!(config.validate(), Err(Error::InvalidConfig(_))));
    }

    #[test]
    fn rejects_empty_codec_preferences() {
        let config = TelephonyTransportConfig {
            codec_preferences: Vec::new(),
            ..TelephonyTransportConfig::default()
        };

        assert!(matches!(config.validate(), Err(Error::InvalidConfig(_))));
    }

    #[test]
    fn default_access_mode_is_allow_list() {
        let config = TelephonyTransportConfig::default();
        assert_eq!(config.access_mode, SipAccessMode::AllowList);
        assert!(config.allowed_peers.is_empty());
    }

    #[test]
    fn default_rate_limit_is_disabled() {
        let config = TelephonyTransportConfig::default();
        assert!(!config.rate_limit.enabled);
        assert_eq!(config.rate_limit.max_requests_per_window, 10);
        assert_eq!(config.rate_limit.window_seconds, 60);
        assert_eq!(config.rate_limit.ban_duration_seconds, 300);
    }

    #[test]
    fn parses_access_mode_and_rate_limit_from_json() {
        let config = TelephonyTransportConfig::from_json(&json!({
            "sip_bind_address": "127.0.0.1:5060",
            "rtp_port_start": 20000,
            "rtp_port_end": 20100,
            "codec_preferences": ["pcmu"],
            "frame_duration_ms": 20,
            "jitter": {
                "target_ms": 60,
                "max_ms": 200,
                "packet_loss_concealment": true
            },
            "max_active_calls": 10,
            "max_rtp_sessions": 20,
            "max_sip_datagram_bytes": 4096,
            "allowed_peers": ["127.0.0.1"],
            "access_mode": "deny_list",
            "blocked_peers": ["10.0.0.0/24"],
            "rate_limit": {
                "enabled": true,
                "max_requests_per_window": 20,
                "window_seconds": 30,
                "ban_duration_seconds": 120
            },
            "enable_siprec": false,
            "conference": {
                "enabled": false,
                "max_legs": 3,
                "suppress_injected_audio_feedback": true
            }
        }))
        .unwrap();

        assert_eq!(config.access_mode, SipAccessMode::DenyList);
        assert_eq!(config.blocked_peers, vec!["10.0.0.0/24"]);
        assert!(config.rate_limit.enabled);
        assert_eq!(config.rate_limit.max_requests_per_window, 20);
    }

    #[test]
    fn rejects_zero_rate_limit_max_when_enabled() {
        let config = TelephonyTransportConfig {
            rate_limit: SipRateLimitConfig {
                enabled: true,
                max_requests_per_window: 0,
                window_seconds: 60,
                ban_duration_seconds: 300,
            },
            ..TelephonyTransportConfig::default()
        };

        assert!(matches!(config.validate(), Err(Error::InvalidConfig(_))));
    }

    #[test]
    fn rejects_zero_rate_limit_window_when_enabled() {
        let config = TelephonyTransportConfig {
            rate_limit: SipRateLimitConfig {
                enabled: true,
                max_requests_per_window: 10,
                window_seconds: 0,
                ban_duration_seconds: 300,
            },
            ..TelephonyTransportConfig::default()
        };

        assert!(matches!(config.validate(), Err(Error::InvalidConfig(_))));
    }
}
