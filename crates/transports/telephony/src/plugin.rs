//! Telephony transport plugin implementation.

use crate::{TelephonyTransport, TelephonyTransportConfig};
use async_trait::async_trait;
use remotemedia_core::transport::{
    ClientConfig, PipelineClient, PipelineExecutor, PipelineTransport, ServerConfig,
    TransportPlugin,
};
use remotemedia_core::{Error as CoreError, Result as CoreResult};
use std::sync::Arc;

/// SIP/RTP telephony transport plugin.
pub struct TelephonyTransportPlugin;

#[async_trait]
impl TransportPlugin for TelephonyTransportPlugin {
    fn name(&self) -> &'static str {
        "telephony"
    }

    async fn create_client(&self, _config: &ClientConfig) -> CoreResult<Box<dyn PipelineClient>> {
        Err(CoreError::Transport(
            "telephony transport is server-side only; use SIP/RTP clients or an upstream PBX/SBC"
                .to_string(),
        ))
    }

    async fn create_server(
        &self,
        config: &ServerConfig,
        executor: Arc<PipelineExecutor>,
    ) -> CoreResult<Box<dyn PipelineTransport>> {
        let telephony_config = TelephonyTransportConfig::from_bind_address(config.address.clone());
        let transport = TelephonyTransport::new(telephony_config, executor)
            .map_err(|e| CoreError::Transport(e.to_string()))?;
        Ok(Box::new(transport))
    }

    fn validate_config(&self, extra_config: &serde_json::Value) -> CoreResult<()> {
        TelephonyTransportConfig::from_json(extra_config)
            .map(|_| ())
            .map_err(|e| CoreError::Transport(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use remotemedia_core::transport::TransportPlugin;
    use serde_json::json;

    #[test]
    fn plugin_name_is_telephony() {
        let plugin = TelephonyTransportPlugin;
        assert_eq!(plugin.name(), "telephony");
    }

    #[test]
    fn validates_extra_config() {
        let plugin = TelephonyTransportPlugin;

        assert!(plugin.validate_config(&json!({})).is_ok());
        assert!(plugin
            .validate_config(&json!({
                "sip_bind_address": "127.0.0.1:5060",
                "rtp_port_start": 10000,
                "rtp_port_end": 10010,
                "codec_preferences": ["opus"],
                "frame_duration_ms": 20,
                "jitter": {
                    "target_ms": 40,
                    "max_ms": 120,
                    "packet_loss_concealment": true
                },
                "max_active_calls": 4,
                "max_rtp_sessions": 8,
                "max_sip_datagram_bytes": 4096,
                "allowed_peers": [],
                "enable_siprec": false,
                "conference": {
                    "enabled": false,
                    "max_legs": 3,
                    "suppress_injected_audio_feedback": true
                }
            }))
            .is_ok());
    }

    #[test]
    fn rejects_invalid_extra_config() {
        let plugin = TelephonyTransportPlugin;

        assert!(plugin
            .validate_config(&json!({
                "sip_bind_address": "127.0.0.1:5060",
                "rtp_port_start": 10010,
                "rtp_port_end": 10000,
                "codec_preferences": ["opus"],
                "frame_duration_ms": 20,
                "jitter": {
                    "target_ms": 40,
                    "max_ms": 120,
                    "packet_loss_concealment": true
                },
                "max_active_calls": 4,
                "max_rtp_sessions": 8,
                "max_sip_datagram_bytes": 4096,
                "allowed_peers": [],
                "enable_siprec": false,
                "conference": {
                    "enabled": false,
                    "max_legs": 3,
                    "suppress_injected_audio_feedback": true
                }
            }))
            .is_err());
    }
}
