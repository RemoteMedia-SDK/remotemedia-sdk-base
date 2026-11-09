//! WebRTC transport plugin implementation

use async_trait::async_trait;
use remotemedia_core::transport::{
    ClientConfig, PipelineClient, PipelineExecutor, ServerConfig, TransportPlugin,
};
use remotemedia_core::Result;
use std::sync::Arc;

/// WebRTC transport plugin
pub struct WebRtcTransportPlugin;

#[async_trait]
impl TransportPlugin for WebRtcTransportPlugin {
    fn name(&self) -> &'static str {
        "webrtc"
    }

    async fn create_client(&self, config: &ClientConfig) -> Result<Box<dyn PipelineClient>> {
        use crate::client::WebRtcPipelineClient;

        // Extract ice_servers from extra_config if present
        let ice_servers = if let Some(extra) = &config.extra_config {
            if let Some(servers) = extra.get("ice_servers") {
                serde_json::from_value(servers.clone()).map_err(|e| {
                    remotemedia_core::Error::ConfigError(format!(
                        "Invalid ice_servers format: {}",
                        e
                    ))
                })?
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        let client = WebRtcPipelineClient::new(
            config.address.clone(),
            ice_servers,
            config.auth_token.clone(),
        )
        .await
        .map_err(|e| remotemedia_core::Error::ConfigError(e.to_string()))?;

        Ok(Box::new(client))
    }

    async fn create_server(
        &self,
        config: &ServerConfig,
        executor: Arc<PipelineExecutor>,
    ) -> Result<Box<dyn remotemedia_core::transport::PipelineTransport>> {
        Ok(Box::new(server_transport(config, &executor)?))
    }

    fn validate_config(&self, extra_config: &serde_json::Value) -> Result<()> {
        // If no extra config, that's OK (will use defaults)
        if extra_config.is_null() {
            return Ok(());
        }

        // Check ice_servers if present
        if let Some(ice_servers) = extra_config.get("ice_servers") {
            // Must be an array
            if !ice_servers.is_array() {
                return Err(remotemedia_core::Error::ConfigError(
                    "ice_servers must be an array".to_string(),
                ));
            }

            // Validate each server URL
            let servers = ice_servers.as_array().unwrap();
            for (i, server) in servers.iter().enumerate() {
                if !server.is_string() {
                    return Err(remotemedia_core::Error::ConfigError(format!(
                        "ice_servers[{}] must be a string",
                        i
                    )));
                }

                let url = server.as_str().unwrap();
                if !url.starts_with("stun:")
                    && !url.starts_with("turn:")
                    && !url.starts_with("turns:")
                {
                    return Err(remotemedia_core::Error::ConfigError(format!(
                        "ice_servers[{}] must be a valid STUN/TURN URL (got: {})",
                        i, url
                    )));
                }
            }
        }

        Ok(())
    }
}

/// Build the server transport. WebRtcTransport creates its own sessions in
/// execute()/stream(); the executor supplies the transport copy policy so a
/// strict executor never yields a permissive, unattributed transport ledger.
fn server_transport(
    config: &ServerConfig,
    executor: &PipelineExecutor,
) -> Result<crate::transport::WebRtcTransport> {
    use crate::config::WebRtcTransportConfig;
    use crate::transport::WebRtcTransport;

    let copies = remotemedia_core::data::CopyContext::new(
        executor.transport_materialization_policy(),
        "webrtc/server/transport/materialization",
    );

    // Use default WebRTC configuration
    // TODO: ServerConfig currently doesn't have extra_config field.
    // When it's added, extract WebRTC-specific settings like:
    // - signaling_url from config.address or extra_config
    // - stun_servers from extra_config
    // - turn_servers from extra_config
    // - max_peers from extra_config
    let mut webrtc_config = WebRtcTransportConfig::default();

    // Use config.address as signaling_url if provided and valid
    if !config.address.is_empty()
        && (config.address.starts_with("ws://") || config.address.starts_with("wss://"))
    {
        webrtc_config.signaling_url = config.address.clone();
    }

    // Create WebRTC transport (note: constructor is sync, not async)
    WebRtcTransport::new_with_copy_context(webrtc_config, copies).map_err(|e| {
        remotemedia_core::Error::Transport(format!("Failed to create WebRTC transport: {}", e))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use remotemedia_core::data::CopyPolicy;
    use remotemedia_core::transport::ExecutorConfig;

    #[tokio::test]
    async fn server_transport_inherits_executor_copy_policy() {
        for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
            let executor = PipelineExecutor::with_config(ExecutorConfig {
                transport_materialization_policy: policy,
                ..Default::default()
            })
            .unwrap();
            let server = ServerConfig {
                address: String::new(),
                tls_config: None,
            };
            let transport = server_transport(&server, &executor).unwrap();
            assert_eq!(transport.copy_context().policy(), policy);
            assert_eq!(
                transport.copy_context().boundary(),
                "webrtc/server/transport/materialization"
            );
        }
    }
}
