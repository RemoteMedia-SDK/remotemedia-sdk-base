//! gRPC client implementation for remote pipeline execution
//!
//! This module provides a gRPC client that implements the `PipelineClient` trait
//! for executing pipelines on remote gRPC servers.
//!
//! # Features
//!
//! - Unary execution (single request/response) over the lossless
//!   `manifest_json` wire path
//! - Bidirectional streaming with monotonic sequence numbers and a
//!   half-close (`CLOSE`) handshake
//! - Health checks
//! - Authentication via metadata
//! - Connection pooling and retry logic
//!
//! # Wire contract
//!
//! The client populates [`PipelineManifest::manifest_json`] with the canonical
//! serde JSON of the runtime [`Manifest`]. This is the lossless path the
//! server's `decode_manifest` treats as authoritative (plugins,
//! `is_output_node`, etc. survive the round trip). The legacy structured
//! protobuf fields are intentionally left empty to avoid the server's
//! conflicting-forms rejection.
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
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::adapters::{
    data_buffer_to_transport_data, transport_data_to_data_buffer,
};
use crate::generated::{
    execute_response::Outcome, pipeline_execution_service_client::PipelineExecutionServiceClient,
    stream_control::Command, stream_request::Request as StreamRequestKind,
    stream_response::Response as StreamResponseKind, streaming_pipeline_service_client::
        StreamingPipelineServiceClient, DataChunk, EmbeddedPluginBlob, ExecuteRequest, ExecuteResponse,
    ExecutionStatus, PipelineManifest, StreamControl, StreamInit, StreamRequest, StreamResponse,
};

/// Bound on the outbound streaming chunk queue. Sends block when the server
/// falls behind, surfacing transport-level backpressure to the caller.
const STREAM_OUTBOUND_BOUND: usize = 16;

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

    /// Serialize a runtime manifest into the lossless `manifest_json` proto
    /// field, leaving the legacy structured projection empty.
    fn manifest_to_proto(manifest: &Manifest) -> Result<PipelineManifest> {
        let manifest_json = serde_json::to_vec(manifest)
            .map_err(|e| Error::ConfigError(format!("Failed to serialize manifest: {}", e)))?;
        Ok(PipelineManifest {
            manifest_json,
            ..Default::default()
        })
    }

    /// Resolve the single input node id for `data_inputs` routing.
    ///
    /// Prefers a node that has no incoming connection (a source/entry node).
    /// Bails with [`Error::InvalidManifest`] when more than one candidate
    /// is ambiguous so inputs are never silently misrouted.
    fn input_node_id(manifest: &Manifest) -> Result<String> {
        let targets: std::collections::HashSet<&str> =
            manifest.connections.iter().map(|c| c.to.as_str()).collect();
        let candidates: Vec<&str> = manifest
            .nodes
            .iter()
            .filter(|n| !targets.contains(n.id.as_str()))
            .map(|n| n.id.as_str())
            .collect();
        match candidates.len() {
            1 => Ok(candidates[0].to_string()),
            0 => Ok(manifest
                .nodes
                .first()
                .ok_or_else(|| Error::InvalidManifest("manifest has no nodes".to_string()))?
                .id
                .clone()),
            _ => Err(Error::InvalidManifest(format!(
                "ambiguous input node: several source nodes found ({})",
                candidates.join(", ")
            ))),
        }
    }
}

#[async_trait]
impl PipelineClient for GrpcPipelineClient {
    /// Execute a pipeline with unary semantics over the lossless
    /// `manifest_json` wire path.
    async fn execute_unary(
        &self,
        manifest: Arc<Manifest>,
        input: TransportData,
        embedded_plugins: &[(String, Vec<u8>)],
    ) -> Result<TransportData> {
        let proto_manifest = Self::manifest_to_proto(&manifest)?;
        let input_node = Self::input_node_id(&manifest)?;

        let mut data_inputs = std::collections::HashMap::new();
        data_inputs.insert(input_node, transport_data_to_data_buffer(&input));

        let mut embedded = Vec::with_capacity(embedded_plugins.len());
        for (digest, content) in embedded_plugins {
            embedded.push(EmbeddedPluginBlob {
                digest: digest.clone(),
                content: content.clone(),
            });
        }

        let request = ExecuteRequest {
            manifest: Some(proto_manifest),
            data_inputs,
            resource_limits: None,
            client_version: "v1".to_string(),
            embedded_plugins: embedded,
        };

        let channel = self.get_channel().await?;
        let mut client = PipelineExecutionServiceClient::new(channel);

        let mut tonic_request = tonic::Request::new(request);
        if self.auth_token.is_some() {
            *tonic_request.metadata_mut() = self.create_metadata()?;
        }

        let response: ExecuteResponse = client
            .execute_pipeline(tonic_request)
            .await
            .map_err(|s| Error::Transport(format!("ExecutePipeline RPC failed: {}", s)))?
            .into_inner();

        match response.outcome {
            Some(Outcome::Result(result)) => map_execution_result(result),
            Some(Outcome::Error(err)) => Err(server_error_to_core(
                err,
                "ExecutePipeline returned an error outcome",
            )),
            None => Err(Error::Transport(
                "ExecutePipeline response had no outcome".to_string(),
            )),
        }
    }

    /// Create a bidirectional streaming session over the lossless
    /// `manifest_json` wire path.
    async fn create_stream_session(
        &self,
        manifest: Arc<Manifest>,
    ) -> Result<Box<dyn ClientStreamSession>> {
        let proto_manifest = Self::manifest_to_proto(&manifest)?;
        let input_node = Self::input_node_id(&manifest)?;

        let init = StreamInit {
            manifest: Some(proto_manifest),
            data_inputs: std::collections::HashMap::new(),
            resource_limits: None,
            client_version: "v1".to_string(),
            expected_chunk_size: 0,
            output_taps: Vec::new(),
        };

        let channel = self.get_channel().await?;
        let mut client = StreamingPipelineServiceClient::new(channel);

        let (tx, rx) = mpsc::channel::<StreamRequest>(STREAM_OUTBOUND_BOUND);

        // Emit Init immediately as the first stream message.
        tx.send(StreamRequest {
            request: Some(StreamRequestKind::Init(init)),
        })
        .await
        .map_err(|_| Error::Transport("stream closed before Init could be sent".to_string()))?;

        let mut tonic_request = tonic::Request::new(ReceiverStream::new(rx));
        if self.auth_token.is_some() {
            *tonic_request.metadata_mut() = self.create_metadata()?;
        }

        let response_stream = client
            .stream_pipeline(tonic_request)
            .await
            .map_err(|s| Error::Transport(format!("StreamPipeline RPC failed: {}", s)))?;

        let session = GrpcStreamSession::new(input_node, tx, response_stream.into_inner());
        Ok(Box::new(session))
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
/// The session lazily forwards outbound [`TransportData`]s as `DataChunk`
/// messages with monotonic sequence numbers and half-closes by emitting
/// `StreamControl(CLOSE)`.
pub struct GrpcStreamSession {
    /// Input node id used to route `DataChunk`s back to the manifest source.
    input_node: String,
    /// Outbound chunk/control channel, shared with the driver task.
    outbound: mpsc::Sender<StreamRequest>,
    /// Server response stream (drained by `receive`).
    inbound: tonic::codec::Streaming<StreamResponse>,
    /// Monotonic outbound sequence counter.
    sequence: u64,
    /// Whether the session is still active (not yet half-closed).
    active: bool,
    /// Whether a graceful `Closed` response has been observed after a close.
    closed: bool,
    /// Session id negotiated via `StreamReady` (for diagnostics).
    session_id: String,
}

impl GrpcStreamSession {
    /// Create a new streaming session wrapping a live tonic stream and the
    /// outbound request channel owned by the caller. `session_id` is
    /// discovered from the first `StreamReady` response by `receive`.
    fn new(
        input_node: String,
        outbound: mpsc::Sender<StreamRequest>,
        inbound: tonic::codec::Streaming<StreamResponse>,
    ) -> Self {
        Self {
            input_node,
            outbound,
            inbound,
            sequence: 0,
            active: true,
            closed: false,
            session_id: String::new(),
        }
    }
}

#[async_trait]
impl ClientStreamSession for GrpcStreamSession {
    fn session_id(&self) -> &str {
        &self.session_id
    }

    async fn send(&mut self, data: TransportData) -> Result<()> {
        if !self.active {
            return Err(Error::Transport("Session is closed".to_string()));
        }

        let buffer = transport_data_to_data_buffer(&data);
        let sequence = self.sequence;
        self.sequence += 1;

        let chunk = DataChunk {
            node_id: self.input_node.clone(),
            buffer: Some(buffer),
            named_buffers: std::collections::HashMap::new(),
            sequence,
            timestamp_ms: 0,
        };

        self.outbound
            .send(StreamRequest {
                request: Some(StreamRequestKind::DataChunk(chunk)),
            })
            .await
            .map_err(|_| Error::Transport("failed to send DataChunk: stream closed".to_string()))?;
        Ok(())
    }

    async fn receive(&mut self) -> Result<Option<TransportData>> {
        loop {
            if self.closed {
                return Ok(None);
            }
            let message = self
                .inbound
                .message()
                .await
                .map_err(|s| Error::Transport(format!("StreamPipeline receive failed: {}", s)))?
                .ok_or_else(|| {
                    Error::Transport("stream ended without StreamClosed".to_string())
                })?;

            match message.response {
                Some(StreamResponseKind::Ready(ready)) => {
                    self.session_id = ready.session_id;
                    continue;
                }
                Some(StreamResponseKind::Result(result)) => {
                    if let Some(data) = map_chunk_result(result) {
                        return Ok(Some(data));
                    }
                    continue;
                }
                Some(StreamResponseKind::Metrics(_)) => continue,
                Some(StreamResponseKind::Closed(closed)) => {
                    self.session_id = closed.session_id;
                    self.closed = true;
                    self.active = false;
                    return Ok(None);
                }
                Some(StreamResponseKind::Error(err)) => {
                    return Err(server_error_to_core(
                        err,
                        "StreamPipeline returned an error response",
                    ));
                }
                None => continue,
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        if !self.active {
            return Ok(());
        }
        self.active = false;
        // Half-close handshake: emit CLOSE so the server flushes and returns
        // StreamClosed. Subsequent `receive` calls drain until Closed.
        let control = StreamRequest {
            request: Some(StreamRequestKind::Control(StreamControl {
                command: Command::Close as i32,
            })),
        };
        let _ = self.outbound.send(control).await;
        tracing::info!("gRPC stream session {} close requested", self.session_id);
        Ok(())
    }

    fn is_active(&self) -> bool {
        self.active || !self.closed
    }
}

/// Convert an `ExecutionResult` (server output is keyed under "output") into
/// a [`TransportData`].
fn map_execution_result(
    result: crate::generated::ExecutionResult,
) -> Result<TransportData> {
    if result.status != ExecutionStatus::Success as i32 {
        return Err(Error::RemoteExecutionFailed(format!(
            "pipeline execution status was not Success: {}",
            result.status
        )));
    }
    result
        .data_outputs
        .get("output")
        .and_then(data_buffer_to_transport_data)
        .ok_or_else(|| {
            Error::Transport(
                "ExecutePipeline returned no \"output\" entry in data_outputs".to_string(),
            )
        })
}

/// Convert a streaming `ChunkResult` (server may key outputs under the sink
/// node id, an `is_output_node`, or a `__tap__.*` key) into a
/// [`TransportData`]. The first convertible buffer is returned.
fn map_chunk_result(result: crate::generated::ChunkResult) -> Option<TransportData> {
    result
        .data_outputs
        .into_values()
        .find_map(|buffer| data_buffer_to_transport_data(&buffer))
}

/// Map a server [`ErrorResponse`] (and its surrounding context) into a core
/// [`Error::RemoteExecutionFailed`], preserving the failing node id and the
/// server's diagnostic message.
fn server_error_to_core(err: crate::generated::ErrorResponse, context: &str) -> Error {
    let node = if err.failing_node_id.is_empty() {
        String::new()
    } else {
        format!(" (node '{}')", err.failing_node_id)
    };
    let ctx = if err.context.is_empty() {
        String::new()
    } else {
        format!(": {}", err.context)
    };
    Error::RemoteExecutionFailed(format!("{}{}{}; {}", err.message, node, ctx, context))
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

    #[test]
    fn test_input_node_id_picks_source() {
        let manifest: Manifest = serde_json::from_str(
            r#"{ "version": "v1", "metadata": {}, "nodes": [
                { "id": "src", "node_type": "Echo" },
                { "id": "sink", "node_type": "Echo" }
            ], "connections": [ { "from": "src", "to": "sink" } ] }"#,
        )
        .unwrap();
        assert_eq!(GrpcPipelineClient::input_node_id(&manifest).unwrap(), "src");
    }

    #[test]
    fn test_input_node_id_rejects_ambiguous() {
        let manifest: Manifest = serde_json::from_str(
            r#"{ "version": "v1", "metadata": {}, "nodes": [
                { "id": "a", "node_type": "Echo" },
                { "id": "b", "node_type": "Echo" }
            ], "connections": [] }"#,
        )
        .unwrap();
        assert!(GrpcPipelineClient::input_node_id(&manifest).is_err());
    }
}
