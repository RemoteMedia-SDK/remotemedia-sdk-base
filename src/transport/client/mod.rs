//! Transport client abstractions for remote pipeline execution
//!
//! This module defines client-side interfaces for executing pipelines on remote servers.
//! It mirrors the server-side `PipelineTransport` trait but from the client perspective.
//!
//! # Architecture
//!
//! ```text
//! Local Pipeline                    Remote Server
//! ┌─────────────┐                  ┌──────────────┐
//! │ RemoteNode  │  PipelineClient  │ gRPC/WebRTC  │
//! │             │ ───────────────> │ Server       │
//! │             │                  │              │
//! │             │ <─────────────── │ PipelineExecutor│
//! └─────────────┘                  └──────────────┘
//! ```
//!
//! # Usage
//!
//! ```
//! use remotemedia_core::transport::client::TransportType;
//!
//! // Transport types for client configuration
//! let grpc = TransportType::Grpc;
//! let webrtc = TransportType::Webrtc;
//! let http = TransportType::Http;
//! ```

use crate::manifest::Manifest;
use crate::transport::TransportData;
use crate::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Re-export submodules
pub mod circuit_breaker;
pub mod load_balancer;
pub mod retry;

// Re-export key types
pub use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitState};
pub use load_balancer::{Endpoint, EndpointPool, LoadBalanceStrategy};
pub use retry::{RetryConfig, RetryExecutor};

/// Transport protocol type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransportType {
    /// gRPC transport
    Grpc,
    /// WebRTC transport
    Webrtc,
    /// HTTP/REST transport
    Http,
}

impl std::fmt::Display for TransportType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportType::Grpc => write!(f, "grpc"),
            TransportType::Webrtc => write!(f, "webrtc"),
            TransportType::Http => write!(f, "http"),
        }
    }
}

/// Client-side pipeline execution interface
///
/// This trait defines the interface for executing pipelines on remote servers.
/// It's the client-side counterpart to the server-side `PipelineTransport` trait.
///
/// # Thread Safety
///
/// Implementations must be Send + Sync to allow concurrent access from
/// multiple async tasks.
///
/// # Cancellation
///
/// Methods should respect tokio cancellation and clean up resources appropriately.
#[async_trait]
pub trait PipelineClient: Send + Sync {
    /// Execute a pipeline with unary semantics (single request → single response)
    ///
    /// Sends the manifest and input data to a remote server, waits for processing,
    /// and returns the result.
    ///
    /// # Arguments
    ///
    /// * `manifest` - Pipeline configuration to execute on remote server
    /// * `input` - Input data for the pipeline
    ///
    /// # Returns
    ///
    /// * `Ok(TransportData)` - Pipeline output from remote execution
    /// * `Err(Error)` - Network failure, timeout, or remote execution error
    ///
    /// # Errors
    ///
    /// * `Error::RemoteExecutionFailed` - Remote pipeline execution failed
    /// * `Error::RemoteTimeout` - Execution exceeded timeout
    /// * `Error::Transport` - Network or protocol error
    async fn execute_unary(
        &self,
        manifest: Arc<Manifest>,
        input: TransportData,
    ) -> Result<TransportData>;

    /// Create a streaming session with the remote server
    ///
    /// Establishes a long-lived bidirectional connection for continuous data streaming.
    ///
    /// # Arguments
    ///
    /// * `manifest` - Pipeline configuration (shared across session)
    ///
    /// # Returns
    ///
    /// * `Ok(Box<dyn ClientStreamSession>)` - Session handle for streaming I/O
    /// * `Err(Error)` - Failed to establish connection
    ///
    /// # Errors
    ///
    /// * `Error::Transport` - Connection failed
    /// * `Error::InvalidManifest` - Server rejected manifest
    async fn create_stream_session(
        &self,
        manifest: Arc<Manifest>,
    ) -> Result<Box<dyn ClientStreamSession>>;

    /// Check if remote endpoint is healthy
    ///
    /// Performs a lightweight health check to determine if the endpoint is responsive.
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Endpoint is healthy
    /// * `Ok(false)` - Endpoint is unhealthy
    /// * `Err(Error)` - Health check failed (network error)
    async fn health_check(&self) -> Result<bool>;
}

/// Client-side streaming session handle
///
/// Represents an active bidirectional streaming connection to a remote server.
/// Allows sending multiple inputs and receiving multiple outputs over the
/// session lifetime.
///
/// # Lifecycle
///
/// 1. Create via `PipelineClient::create_stream_session()`
/// 2. Send inputs via `send()`
/// 3. Receive outputs via `receive()`
/// 4. Close gracefully via `close()`
///
/// # Example
///
/// See `tests/fixtures/mock_transport_plugin.rs` for a complete example of
/// implementing a streaming session.
#[async_trait]
pub trait ClientStreamSession: Send + Sync {
    /// Get unique session identifier
    fn session_id(&self) -> &str;

    /// Send input data to the remote pipeline
    ///
    /// # Arguments
    ///
    /// * `data` - Input data to process
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Data sent successfully
    /// * `Err(Error)` - Send failed (connection closed, buffer full, etc.)
    async fn send(&mut self, data: TransportData) -> Result<()>;

    /// Receive output data from the remote pipeline
    ///
    /// This is a blocking call that waits for the next output.
    ///
    /// # Returns
    ///
    /// * `Ok(Some(data))` - Received output data
    /// * `Ok(None)` - Stream closed (no more outputs)
    /// * `Err(Error)` - Receive failed (connection error, timeout, etc.)
    async fn receive(&mut self) -> Result<Option<TransportData>>;

    /// Close the session gracefully
    ///
    /// Flushes any pending data and closes the connection.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Session closed successfully
    /// * `Err(Error)` - Failed to close (already closed, network error, etc.)
    async fn close(&mut self) -> Result<()>;

    /// Check if session is still active
    ///
    /// # Returns
    ///
    /// * `true` - Session is active and can send/receive
    /// * `false` - Session is closed or failed
    fn is_active(&self) -> bool;
}

/// Transport factory configuration
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// Transport type
    pub transport_type: TransportType,
    /// Endpoint URL/address
    pub endpoint: String,
    /// Optional authentication token
    pub auth_token: Option<String>,
    /// Transport-specific configuration (for WebRTC: ICE servers, signaling URL, etc.)
    pub extra_config: Option<serde_json::Value>,
}

/// Create a transport client based on transport type
///
/// # Deprecated
///
/// This function is deprecated in favor of the plugin registry system.
/// Use `global_registry().get(name).create_client(config)` instead.
///
/// See `docs/MIGRATION_TO_PLUGINS.md` for migration guide.
///
/// # Arguments
///
/// * `config` - Transport configuration
///
/// # Returns
///
/// * `Ok(Box<dyn PipelineClient>)` - Transport client
/// * `Err(Error)` - Failed to create client
///
/// # Example
///
/// ```
/// use remotemedia_core::transport::client::{TransportConfig, TransportType};
///
/// // Create transport configuration
/// let config = TransportConfig {
///     transport_type: TransportType::Http,
///     endpoint: "http://localhost:8080".to_string(),
///     auth_token: None,
///     extra_config: None,
/// };
/// ```
#[deprecated(
    since = "0.5.0",
    note = "Use TransportPluginRegistry instead. See docs/MIGRATION_TO_PLUGINS.md"
)]
pub async fn create_transport_client(
    config: TransportConfig,
) -> crate::Result<Box<dyn PipelineClient>> {
    match config.transport_type {
        TransportType::Grpc => {
            Err(crate::Error::ConfigError(
                "gRPC client moved to remotemedia-grpc crate - use TransportPluginRegistry instead. See docs/MIGRATION_TO_PLUGINS.md".to_string(),
            ))
        }
        TransportType::Http => {
            Err(crate::Error::ConfigError(
                "HTTP client moved to remotemedia-http crate - use TransportPluginRegistry instead. See docs/MIGRATION_TO_PLUGINS.md".to_string(),
            ))
        }
        TransportType::Webrtc => {
            Err(crate::Error::ConfigError(
                "WebRTC client moved to remotemedia-webrtc crate - use TransportPluginRegistry instead. See docs/MIGRATION_TO_PLUGINS.md".to_string(),
            ))
        }
    }
}

/// Validate transport-specific configuration
///
/// Performs transport-specific validation checks:
/// - gRPC: Validate endpoint format
/// - HTTP: Validate base URL format
/// - WebRTC: Validate signaling URL and ICE server configuration
///
/// # Arguments
///
/// * `config` - Transport configuration
///
/// # Returns
///
/// * `Ok(())` - Configuration is valid
/// * `Err(Error)` - Configuration validation failed
pub fn validate_transport_config(config: &TransportConfig) -> crate::Result<()> {
    match config.transport_type {
        TransportType::Grpc => {
            // gRPC endpoint validation
            if config.endpoint.is_empty() {
                return Err(crate::Error::ConfigError(
                    "gRPC endpoint cannot be empty".to_string(),
                ));
            }
            // Basic format check
            if config.endpoint.contains("://") && !config.endpoint.starts_with("http") {
                return Err(crate::Error::ConfigError(format!(
                    "gRPC endpoint has invalid scheme: {}",
                    config.endpoint
                )));
            }
        }
        TransportType::Http => {
            // HTTP base URL validation
            if config.endpoint.is_empty() {
                return Err(crate::Error::ConfigError(
                    "HTTP base_url cannot be empty".to_string(),
                ));
            }
            if !config.endpoint.starts_with("http://") && !config.endpoint.starts_with("https://") {
                return Err(crate::Error::ConfigError(format!(
                    "HTTP base_url must start with http:// or https://, got: {}",
                    config.endpoint
                )));
            }
        }
        TransportType::Webrtc => {
            // WebRTC signaling URL validation
            if config.endpoint.is_empty() {
                return Err(crate::Error::ConfigError(
                    "WebRTC signaling_url cannot be empty".to_string(),
                ));
            }
            if !config.endpoint.starts_with("ws://") && !config.endpoint.starts_with("wss://") {
                return Err(crate::Error::ConfigError(format!(
                    "WebRTC signaling_url must start with ws:// or wss://, got: {}",
                    config.endpoint
                )));
            }

            // Validate ICE servers if present
            if let Some(extra) = &config.extra_config {
                if let Some(servers) = extra.get("ice_servers") {
                    if !servers.is_array() {
                        return Err(crate::Error::ConfigError(
                            "WebRTC ice_servers must be an array".to_string(),
                        ));
                    }
                }
            }
        }
    }

    Ok(())
}
