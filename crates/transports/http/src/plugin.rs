//! HTTP transport plugin implementation
//!
//! This module provides the `HttpTransportPlugin` which implements the `TransportPlugin`
//! trait for the HTTP transport. It enables dynamic registration and creation of HTTP
//! clients and servers through the plugin registry.
//!
//! # Usage
//!
//! ```ignore
//! use remotemedia_http::HttpTransportPlugin;
//! use remotemedia_core::transport::TransportPluginRegistry;
//!
//! let mut registry = TransportPluginRegistry::new();
//! registry.register(Arc::new(HttpTransportPlugin));
//! ```

use async_trait::async_trait;
use remotemedia_core::transport::{
    ClientConfig, PipelineClient, PipelineExecutor, PipelineTransport, ServerConfig,
    TransportPlugin,
};
use remotemedia_core::Result;
use std::sync::Arc;

/// HTTP transport plugin
///
/// Provides HTTP/REST-based client and server implementations for the RemoteMedia pipeline
/// execution framework. This plugin enables remote execution of pipelines via HTTP with
/// Server-Sent Events (SSE) for streaming.
///
/// # Features
///
/// - Unary execution via POST /execute
/// - Streaming sessions via POST /stream
/// - SSE for continuous output streaming
/// - Health checks via GET /health
/// - Authentication via Bearer tokens
pub struct HttpTransportPlugin;

#[async_trait]
impl TransportPlugin for HttpTransportPlugin {
    /// Returns the name of the transport plugin
    ///
    /// This name is used for registration and lookup in the plugin registry.
    fn name(&self) -> &'static str {
        "http"
    }

    /// Create an HTTP pipeline client
    ///
    /// # Arguments
    ///
    /// * `config` - Client configuration containing endpoint and authentication settings
    ///
    /// # Returns
    ///
    /// A boxed `PipelineClient` implementation for HTTP
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The endpoint URL is invalid or empty
    /// - The URL scheme is not http:// or https://
    /// - The authentication token format is invalid
    async fn create_client(&self, config: &ClientConfig) -> Result<Box<dyn PipelineClient>> {
        use crate::client::HttpPipelineClient;

        let client = HttpPipelineClient::new(config.address.clone(), config.auth_token.clone())
            .await
            .map_err(|e| remotemedia_core::Error::Transport(e.to_string()))?;

        Ok(Box::new(client))
    }

    /// Create an HTTP pipeline server
    ///
    /// # Arguments
    ///
    /// * `config` - Server configuration containing bind address
    /// * `executor` - Pipeline executor for executing pipelines (spec 026 migration)
    ///
    /// # Returns
    ///
    /// A boxed `PipelineTransport` implementation for HTTP server
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The bind address format is invalid
    /// - The port is already in use
    async fn create_server(
        &self,
        config: &ServerConfig,
        executor: Arc<PipelineExecutor>,
    ) -> Result<Box<dyn PipelineTransport>> {
        use crate::server::HttpServer;

        let server = HttpServer::new(config.address.clone(), executor)
            .await
            .map_err(|e| remotemedia_core::Error::Transport(e.to_string()))?;

        Ok(Box::new(server))
    }

    /// Validate HTTP-specific configuration
    ///
    /// HTTP transport accepts optional extra configuration for:
    /// - Custom headers
    /// - Retry policies
    /// - Timeout overrides
    /// - Connection pooling settings
    ///
    /// # Arguments
    ///
    /// * `_extra_config` - Additional configuration (validated as needed)
    ///
    /// # Returns
    ///
    /// Always returns `Ok(())` as HTTP accepts flexible configuration.
    fn validate_config(&self, _extra_config: &serde_json::Value) -> Result<()> {
        // HTTP accepts any extra configuration (headers, timeouts, etc.)
        // Validation can be added here if specific config schema is needed
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plugin_name() {
        let plugin = HttpTransportPlugin;
        assert_eq!(plugin.name(), "http");
    }

    #[test]
    fn test_validate_config() {
        let plugin = HttpTransportPlugin;
        let empty_config = serde_json::json!({});
        assert!(plugin.validate_config(&empty_config).is_ok());

        let custom_config = serde_json::json!({
            "headers": {"X-Custom": "value"},
            "timeout_ms": 5000,
            "retry_count": 3
        });
        assert!(plugin.validate_config(&custom_config).is_ok());
    }
}
