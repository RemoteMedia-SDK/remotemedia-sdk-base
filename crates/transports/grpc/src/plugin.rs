//! gRPC transport plugin implementation
//!
//! This module provides the `GrpcTransportPlugin` which implements the `TransportPlugin`
//! trait for the gRPC transport. It enables dynamic registration and creation of gRPC
//! clients and servers through the plugin registry.
//!
//! # Usage
//!
//! ```
//! use remotemedia_grpc::plugin::GrpcTransportPlugin;
//! use remotemedia_core::transport::TransportPluginRegistry;
//! use std::sync::Arc;
//!
//! let registry = TransportPluginRegistry::new();
//! registry.register(Arc::new(GrpcTransportPlugin)).unwrap();
//! ```

use async_trait::async_trait;
use remotemedia_core::transport::{
    ClientConfig, PipelineClient, PipelineExecutor, PipelineTransport, ServerConfig,
    TransportPlugin,
};
use remotemedia_core::Result;
use std::sync::Arc;

/// gRPC transport plugin
///
/// Provides gRPC-based client and server implementations for the RemoteMedia pipeline
/// execution framework. This plugin enables remote execution of pipelines via gRPC.
///
/// # Features
///
/// - Unary RPC execution (single request/response)
/// - Bidirectional streaming
/// - Authentication via metadata tokens
/// - Connection pooling and health checks
/// - Automatic retry and circuit breaker patterns
pub struct GrpcTransportPlugin;

#[async_trait]
impl TransportPlugin for GrpcTransportPlugin {
    /// Returns the name of the transport plugin
    ///
    /// This name is used for registration and lookup in the plugin registry.
    fn name(&self) -> &'static str {
        "grpc"
    }

    /// Create a gRPC pipeline client
    ///
    /// # Arguments
    ///
    /// * `config` - Client configuration containing endpoint and authentication settings
    ///
    /// # Returns
    ///
    /// A boxed `PipelineClient` implementation for gRPC
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The endpoint URL is invalid or empty
    /// - The authentication token format is invalid
    /// - Connection to the endpoint fails (during first use)
    ///
    async fn create_client(&self, config: &ClientConfig) -> Result<Box<dyn PipelineClient>> {
        use crate::client::GrpcPipelineClient;

        let client =
            GrpcPipelineClient::new(config.address.clone(), config.auth_token.clone()).await?;

        Ok(Box::new(client))
    }

    /// Create a gRPC pipeline server
    ///
    /// # Arguments
    ///
    /// * `config` - Server configuration containing bind address and authentication settings
    /// * `executor` - Pipeline executor for executing pipelines (spec 026 migration)
    ///
    /// # Returns
    ///
    /// A boxed `PipelineTransport` implementation for gRPC server
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The bind address format is invalid
    /// - The port is already in use
    /// - TLS configuration is invalid (if using HTTPS)
    async fn create_server(
        &self,
        config: &ServerConfig,
        executor: Arc<PipelineExecutor>,
    ) -> Result<Box<dyn PipelineTransport>> {
        use crate::server::GrpcServer;
        use crate::ServiceConfig;

        // Convert ServerConfig to ServiceConfig
        let service_config = ServiceConfig {
            bind_address: config.address.clone(),
            // TODO: Extract auth settings from config.extra_config if needed
            auth: crate::auth::AuthConfig::default(),
            limits: crate::limits::ResourceLimits::default(),
            json_logging: true,
        };

        // Create GrpcServer with PipelineExecutor (spec 026 migration)
        let server = GrpcServer::new(service_config, executor)
            .map_err(|e| remotemedia_core::Error::Transport(e.to_string()))?;

        Ok(Box::new(server))
    }

    /// Validate gRPC-specific configuration
    ///
    /// gRPC transport does not require any extra configuration beyond the standard
    /// ClientConfig and ServerConfig fields (endpoint, auth_token, bind_address, etc.).
    ///
    /// # Arguments
    ///
    /// * `_extra_config` - Additional configuration (unused for gRPC)
    ///
    /// # Returns
    ///
    /// Always returns `Ok(())` as gRPC has no extra validation requirements.
    fn validate_config(&self, _extra_config: &serde_json::Value) -> Result<()> {
        // gRPC has no extra configuration beyond standard ClientConfig/ServerConfig
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_name() {
        let plugin = GrpcTransportPlugin;
        assert_eq!(plugin.name(), "grpc");
    }

    #[test]
    fn test_validate_config() {
        let plugin = GrpcTransportPlugin;
        let empty_config = serde_json::json!({});
        assert!(plugin.validate_config(&empty_config).is_ok());

        let arbitrary_config = serde_json::json!({"foo": "bar"});
        assert!(plugin.validate_config(&arbitrary_config).is_ok());
    }
}
