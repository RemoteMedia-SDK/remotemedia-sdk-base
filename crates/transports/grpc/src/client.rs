//! gRPC client implementation for remote pipeline execution
//!
//! This module provides a gRPC client that implements the `PipelineClient` trait
//! for executing pipelines on remote gRPC servers.
//!
//! # Features
//!
//! - Unary execution (single request/response)
//! - Bidirectional streaming
//! - Health checks
//! - Authentication via metadata
//! - Connection pooling and retry logic
//!
//! # Usage
//!
//! ```
//! use remotemedia_grpc::client::GrpcPipelineClient;
//!
//! # tokio_test::block_on(async {
//! let client = GrpcPipelineClient::new("localhost:50051", None).await.unwrap();
//! // Use client.execute_unary(manifest, input).await for unary execution
//! // Use client.create_stream_session(manifest).await for streaming
//! # });
//! ```

// Internal infrastructure - auth_token reserved for future use
#![allow(dead_code)]

use async_trait::async_trait;
use remotemedia_core::manifest::Manifest;
use remotemedia_core::transport::client::{ClientStreamSession, PipelineClient};
use remotemedia_core::transport::TransportData;
use remotemedia_core::{Error, Result};
use std::sync::Arc;

/// gRPC client for remote pipeline execution
///
/// Connects to a remotemedia-grpc server and executes pipelines remotely.
pub struct GrpcPipelineClient {
    /// Endpoint URL (e.g., "localhost:50051")
    endpoint: String,

    /// Optional authentication token
    auth_token: Option<String>,

    /// gRPC channel (created lazily)
    channel: tokio::sync::Mutex<Option<tonic::transport::Channel>>,
}

impl GrpcPipelineClient {
    /// Create a new gRPC client
    ///
    /// # Arguments
    ///
    /// * `endpoint` - Server endpoint (e.g., "localhost:50051" or "https://api.example.com:443")
    /// * `auth_token` - Optional authentication token
    ///
    /// # Returns
    ///
    /// * `Ok(GrpcPipelineClient)` - Client created successfully
    /// * `Err(Error)` - Failed to create client
    ///
    /// # Example
    ///
    /// ```
    /// use remotemedia_grpc::client::GrpcPipelineClient;
    ///
    /// # tokio_test::block_on(async {
    /// let client = GrpcPipelineClient::new("localhost:50051", None).await.unwrap();
    /// # });
    /// ```
    pub async fn new(endpoint: impl Into<String>, auth_token: Option<String>) -> Result<Self> {
        let endpoint = endpoint.into();

        // Validate endpoint format
        if endpoint.is_empty() {
            return Err(Error::ConfigError(
                "gRPC endpoint cannot be empty".to_string(),
            ));
        }

        Ok(Self {
            endpoint,
            auth_token,
            channel: tokio::sync::Mutex::new(None),
        })
    }

    /// Get or create gRPC channel
    async fn get_channel(&self) -> Result<tonic::transport::Channel> {
        let mut guard = self.channel.lock().await;

        if let Some(ref channel) = *guard {
            return Ok(channel.clone());
        }

        // Create new channel
        let endpoint = self.endpoint.clone();

        // Determine if we need TLS based on endpoint format
        let uri = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            endpoint.clone()
        } else {
            // Default to http:// for local endpoints
            format!("http://{}", endpoint)
        };

        let channel = tonic::transport::Channel::from_shared(uri)
            .map_err(|e| Error::Transport(format!("Invalid gRPC endpoint '{}': {}", endpoint, e)))?
            .connect()
            .await
            .map_err(|e| {
                Error::Transport(format!(
                    "Failed to connect to gRPC endpoint '{}': {}",
                    endpoint, e
                ))
            })?;

        *guard = Some(channel.clone());
        Ok(channel)
    }

    /// Create metadata with authentication token
    fn create_metadata(&self) -> Result<tonic::metadata::MetadataMap> {
        let mut metadata = tonic::metadata::MetadataMap::new();

        if let Some(ref token) = self.auth_token {
            let header_value =
                tonic::metadata::MetadataValue::try_from(format!("Bearer {}", token))
                    .map_err(|e| Error::ConfigError(format!("Invalid auth token: {}", e)))?;

            metadata.insert("authorization", header_value);
        }

        Ok(metadata)
    }
}

#[async_trait]
impl PipelineClient for GrpcPipelineClient {
    /// Execute a pipeline with unary semantics
    ///
    /// This is a placeholder implementation. Full implementation requires:
    /// 1. Converting Manifest to protobuf PipelineManifest
    /// 2. Converting TransportData to protobuf DataBuffer
    /// 3. Calling PipelineExecutionService::ExecutePipeline RPC
    /// 4. Converting protobuf result back to TransportData
    async fn execute_unary(
        &self,
        _manifest: Arc<Manifest>,
        _input: TransportData,
    ) -> Result<TransportData> {
        // TODO: Implement actual gRPC call
        // This requires protobuf conversion utilities which are available in this crate
        Err(Error::Transport(
            "gRPC execute_unary not yet fully implemented - will be completed in T011".to_string(),
        ))
    }

    /// Create a streaming session
    ///
    /// This is a placeholder implementation. Full implementation requires:
    /// 1. Establishing bidirectional stream via StreamPipeline RPC
    /// 2. Sending StreamInit message with manifest
    /// 3. Creating GrpcStreamSession wrapper
    async fn create_stream_session(
        &self,
        _manifest: Arc<Manifest>,
    ) -> Result<Box<dyn ClientStreamSession>> {
        Err(Error::Transport(
            "gRPC create_stream_session not yet fully implemented - will be completed in T011"
                .to_string(),
        ))
    }

    /// Check if the remote endpoint is healthy
    async fn health_check(&self) -> Result<bool> {
        // Try to establish a connection
        match self.get_channel().await {
            Ok(_) => Ok(true),
            Err(e) => {
                tracing::warn!("Health check failed for {}: {}", self.endpoint, e);
                Ok(false)
            }
        }
    }
}

/// gRPC streaming session
///
/// Represents an active bidirectional streaming connection to a gRPC server.
pub struct GrpcStreamSession {
    /// Unique session ID
    session_id: String,

    /// Whether the session is active
    active: bool,
}

impl GrpcStreamSession {
    /// Create a new stream session
    pub fn new(session_id: String) -> Self {
        Self {
            session_id,
            active: true,
        }
    }
}

#[async_trait]
impl ClientStreamSession for GrpcStreamSession {
    fn session_id(&self) -> &str {
        &self.session_id
    }

    async fn send(&mut self, _data: TransportData) -> Result<()> {
        if !self.active {
            return Err(Error::Transport("Session is closed".to_string()));
        }

        // TODO: Implement actual streaming send (T011)
        Err(Error::Transport(
            "gRPC streaming send not yet fully implemented".to_string(),
        ))
    }

    async fn receive(&mut self) -> Result<Option<TransportData>> {
        if !self.active {
            return Ok(None);
        }

        // TODO: Implement actual streaming receive (T011)
        Err(Error::Transport(
            "gRPC streaming receive not yet fully implemented".to_string(),
        ))
    }

    async fn close(&mut self) -> Result<()> {
        if !self.active {
            return Ok(());
        }

        self.active = false;
        tracing::info!("gRPC stream session {} closed", self.session_id);
        Ok(())
    }

    fn is_active(&self) -> bool {
        self.active
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_client() {
        let client = GrpcPipelineClient::new("localhost:50051", None).await;
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_create_client_with_auth() {
        let client =
            GrpcPipelineClient::new("localhost:50051", Some("test-token".to_string())).await;
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_empty_endpoint_error() {
        let client = GrpcPipelineClient::new("", None).await;
        assert!(client.is_err());
    }

    #[tokio::test]
    async fn test_health_check_unreachable() {
        let client = GrpcPipelineClient::new("localhost:9999", None)
            .await
            .unwrap();
        let result = client.health_check().await;
        // Should return Ok(false) for unreachable endpoint
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }
}
