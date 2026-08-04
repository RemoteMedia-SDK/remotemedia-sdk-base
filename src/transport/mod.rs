//! Transport abstraction layer
//!
//! This module defines traits that transport implementations must satisfy.
//! The core runtime knows nothing about specific transports (gRPC, WebRTC, FFI).
//!
//! # Architecture
//!
//! Transport implementations (gRPC, FFI, WebRTC) depend on core and
//! implement the `PipelineTransport` trait. This inverts the dependency from
//! the original monolithic design where transports were embedded in the runtime.
//!
//! # Example
//!
//! ```
//! use remotemedia_core::transport::{PipelineExecutor, TransportData};
//! use remotemedia_core::data::RuntimeData;
//!
//! let executor = PipelineExecutor::new().unwrap();
//! let input = TransportData::new(RuntimeData::Text("hello".into()));
//! // Use executor.execute_unary(manifest, input).await for execution
//! ```

use crate::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Re-export submodules
pub mod cancel_gate;
pub mod client;
pub mod control_transport;
pub mod data;
pub mod embedded_python_env;
pub mod pacer;
pub mod perf_aggregator;
pub mod plugin_registry;
pub mod session;
pub mod session_control;
pub mod session_recorder;
pub mod session_router;
pub mod shared_session;
pub mod warm_session_pool;

// PipelineExecutor facade (spec 026)
pub mod executor;

// Re-export key types for convenience
pub use cancel_gate::{CancelGate, ProtectGuard};
pub use client::{ClientStreamSession, PipelineClient, TransportType};
pub use control_transport::{
    ControlTransport, InProcControlTransport, InterceptReplyHandle, InterceptRequest,
    InterceptSession,
};
pub use data::{Participant, TransportData};
pub use executor::{
    ExecutorConfig, ParticipantSessionHandle, PipelineExecutor, PipelineSessionHost, SessionHandle,
    SessionInputSender,
};
pub use plugin_registry::TransportPluginRegistry;
pub use session::{StreamSession, StreamSessionHandle};
pub use session_router::{
    ClientOutputReceivers, ClientOutputRouter, DataPacket, SessionRouter,
    DEFAULT_ROUTER_INPUT_CAPACITY, DEFAULT_ROUTER_OUTPUT_CAPACITY,
};
pub use shared_session::{
    SharedPipelineOutputReceivers, SharedPipelineSession, SharedPipelineStreamSession,
};
pub use warm_session_pool::{WarmPoolConfig, WarmSessionPool};

/// Configuration for creating a transport client
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    /// Base URL or address for the client connection
    pub address: String,
    /// Optional authentication token
    pub auth_token: Option<String>,
    /// Connection timeout in milliseconds
    pub timeout_ms: Option<u64>,
    /// Transport-specific configuration (JSON)
    ///
    /// Different transports may require additional configuration:
    /// - **gRPC**: No extra config needed
    /// - **WebRTC**: `{"ice_servers": ["stun:..."]}`
    /// - **HTTP**: `{"retry_count": 3, "headers": {...}}`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra_config: Option<serde_json::Value>,
}

impl ClientConfig {
    /// Create ClientConfig from manifest parameters
    ///
    /// Extracts transport configuration from pipeline manifest node parameters.
    ///
    /// # Arguments
    ///
    /// * `params` - Node parameters from manifest (contains endpoint, auth_token, etc.)
    ///
    /// # Returns
    ///
    /// ClientConfig with extracted values
    pub fn from_manifest_params(params: &serde_json::Value) -> crate::Result<Self> {
        let address = params
            .get("endpoint")
            .or_else(|| params.get("address"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                crate::Error::ConfigError(
                    "Missing 'endpoint' or 'address' in transport config".to_string(),
                )
            })?
            .to_string();

        let auth_token = params
            .get("auth_token")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let timeout_ms = params.get("timeout_ms").and_then(|v| v.as_u64());

        let extra_config = params.get("extra_config").cloned();

        Ok(Self {
            address,
            auth_token,
            timeout_ms,
            extra_config,
        })
    }
}

/// Configuration for creating a transport server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Server bind address
    pub address: String,
    /// Optional TLS/SSL configuration
    pub tls_config: Option<TlsConfig>,
}

/// TLS/SSL configuration for servers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Path to certificate file
    pub cert_path: String,
    /// Path to private key file
    pub key_path: String,
}

/// Transport plugin trait for registering transport implementations
///
/// This trait allows transport implementations (gRPC, WebRTC, HTTP, etc.)
/// to be dynamically registered and instantiated by name.
#[async_trait]
pub trait TransportPlugin: Send + Sync {
    /// Get the unique name of this transport plugin (e.g., "grpc", "webrtc")
    fn name(&self) -> &'static str;

    /// Create a client for this transport
    ///
    /// # Arguments
    ///
    /// * `config` - Client configuration including address and auth
    ///
    /// # Returns
    ///
    /// * `Ok(Box<dyn PipelineClient>)` - Configured client instance
    /// * `Err(Error)` - Client creation failed
    async fn create_client(&self, config: &ClientConfig) -> Result<Box<dyn PipelineClient>>;

    /// Create a server for this transport
    ///
    /// # Arguments
    ///
    /// * `config` - Server configuration including bind address
    /// * `executor` - Pipeline executor for executing pipelines (spec 026 migration)
    ///
    /// # Returns
    ///
    /// * `Ok(Box<dyn PipelineTransport>)` - Configured server instance
    /// * `Err(Error)` - Server creation failed
    async fn create_server(
        &self,
        config: &ServerConfig,
        executor: Arc<PipelineExecutor>,
    ) -> Result<Box<dyn PipelineTransport>>;

    /// Validate transport-specific configuration
    ///
    /// This method should be called before creating clients or servers to
    /// validate any transport-specific configuration in ClientConfig or ServerConfig.
    ///
    /// # Arguments
    ///
    /// * `extra_config` - Transport-specific configuration as JSON
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Configuration is valid
    /// * `Err(Error)` - Configuration validation failed
    ///
    /// # Example
    ///
    /// For WebRTC, this might validate ice_servers structure:
    /// ```json
    /// {
    ///   "ice_servers": ["stun:stun.l.google.com:19302"]
    /// }
    /// ```
    fn validate_config(&self, extra_config: &serde_json::Value) -> Result<()> {
        // Default implementation: no validation needed
        let _ = extra_config;
        Ok(())
    }
}

/// Transport-agnostic pipeline execution interface
///
/// All transport implementations (gRPC, FFI, WebRTC, custom) must implement
/// this trait to integrate with the RemoteMedia runtime core.
///
/// # Thread Safety
///
/// Implementations must be Send + Sync to allow concurrent access from
/// multiple async tasks.
///
/// # Cancellation
///
/// Methods should respect tokio cancellation (tokio::select! or similar)
/// and clean up resources appropriately.
#[async_trait]
pub trait PipelineTransport: Send + Sync {
    /// Execute a pipeline with unary semantics (single request → single response)
    ///
    /// This method is suitable for batch processing or simple request/response
    /// scenarios where the entire input is available upfront.
    ///
    /// # Arguments
    ///
    /// * `manifest` - Pipeline configuration defining nodes and connections
    /// * `input` - Input data wrapped in transport-agnostic container
    ///
    /// # Returns
    ///
    /// * `Ok(TransportData)` - Pipeline output after all nodes execute
    /// * `Err(Error)` - Pipeline execution failed (see Error for details)
    ///
    /// # Errors
    ///
    /// * `Error::InvalidManifest` - Manifest parsing or validation failed
    /// * `Error::NodeExecutionFailed` - A node in the pipeline failed
    /// * `Error::InvalidData` - Input data format incompatible with pipeline
    async fn execute(
        &self,
        manifest: Arc<crate::manifest::Manifest>,
        input: TransportData,
    ) -> Result<TransportData>;

    /// Start a streaming pipeline session (multiple requests/responses)
    ///
    /// This method creates a stateful session for continuous data streaming.
    /// The transport can send multiple inputs and receive multiple outputs
    /// over the lifetime of the session.
    ///
    /// # Arguments
    ///
    /// * `manifest` - Pipeline configuration (shared across session)
    ///
    /// # Returns
    ///
    /// * `Ok(Box<dyn StreamSession>)` - Session handle for streaming I/O
    /// * `Err(Error)` - Session creation failed
    ///
    /// # Errors
    ///
    /// * `Error::InvalidManifest` - Manifest parsing or validation failed
    /// * `Error::ResourceLimit` - Too many concurrent sessions
    async fn stream(
        &self,
        manifest: Arc<crate::manifest::Manifest>,
    ) -> Result<Box<dyn StreamSession>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_client_config_from_manifest_params() {
        let params = json!({
            "endpoint": "localhost:50051",
            "auth_token": "test-token",
            "timeout_ms": 5000,
            "extra_config": {
                "ice_servers": ["stun:stun.l.google.com:19302"]
            }
        });

        let config = ClientConfig::from_manifest_params(&params).unwrap();

        assert_eq!(config.address, "localhost:50051");
        assert_eq!(config.auth_token, Some("test-token".to_string()));
        assert_eq!(config.timeout_ms, Some(5000));
        assert!(config.extra_config.is_some());
    }

    #[test]
    fn test_client_config_from_manifest_params_minimal() {
        let params = json!({
            "endpoint": "localhost:50051"
        });

        let config = ClientConfig::from_manifest_params(&params).unwrap();

        assert_eq!(config.address, "localhost:50051");
        assert_eq!(config.auth_token, None);
        assert_eq!(config.timeout_ms, None);
        assert_eq!(config.extra_config, None);
    }

    #[test]
    fn test_client_config_from_manifest_params_missing_endpoint() {
        let params = json!({
            "auth_token": "test-token"
        });

        let result = ClientConfig::from_manifest_params(&params);
        assert!(result.is_err());
    }

    #[test]
    fn test_client_config_from_manifest_params_address_fallback() {
        // Test that "address" works as fallback for "endpoint"
        let params = json!({
            "address": "localhost:8080"
        });

        let config = ClientConfig::from_manifest_params(&params).unwrap();
        assert_eq!(config.address, "localhost:8080");
    }
}
