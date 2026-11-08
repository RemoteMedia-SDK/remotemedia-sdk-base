//! HTTP/REST client implementation with SSE streaming support
//!
//! This module provides an HTTP client that implements the `PipelineClient` trait
//! for executing pipelines on remote HTTP/REST servers.
//!
//! # Features
//!
//! - Unary execution via POST /execute
//! - SSE streaming via POST /stream (create session) and GET /stream/:id/output
//! - Health checks via GET /health
//! - Authentication via Authorization header
//! - JSON serialization of manifests and data
//!
//! # Usage
//!
//! ```ignore
//! use remotemedia_http::HttpPipelineClient;
//!
//! let client = HttpPipelineClient::new("http://localhost:8080", None).await?;
//! let result = client.execute_unary(manifest, input).await?;
//! ```

use crate::error::{Error, Result};
use crate::{codec, sse};
use async_trait::async_trait;
use futures::StreamExt;
use remotemedia_core::manifest::Manifest;
use remotemedia_core::transport::client::EmbeddedPythonEnv;
use remotemedia_core::transport::{ClientStreamSession, PipelineClient, TransportData};
use remotemedia_types::{CopyContext, CopyPolicy};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;

/// HTTP client for remote pipeline execution
///
/// Connects to a remotemedia HTTP/REST server and executes pipelines remotely.
pub struct HttpPipelineClient {
    /// Base URL (e.g., "http://localhost:8080" or "https://api.example.com")
    base_url: String,

    /// Optional authentication token
    auth_token: Option<String>,

    /// Reqwest HTTP client
    client: reqwest::Client,
    copies: CopyContext,
}

impl HttpPipelineClient {
    /// Create a new HTTP client
    ///
    /// # Arguments
    ///
    /// * `base_url` - Server base URL (e.g., "http://localhost:8080")
    /// * `auth_token` - Optional authentication token
    ///
    /// # Returns
    ///
    /// * `Ok(HttpPipelineClient)` - Client created successfully
    /// * `Err(Error)` - Failed to create client
    ///
    /// # Example
    ///
    /// ```ignore
    /// let client = HttpPipelineClient::new("http://localhost:8080", None).await?;
    /// ```
    pub async fn new(base_url: impl Into<String>, auth_token: Option<String>) -> Result<Self> {
        let base_url = base_url.into();

        // Validate base URL format
        if base_url.is_empty() {
            return Err(Error::ConnectionError(
                "HTTP base_url cannot be empty".to_string(),
            ));
        }

        // Ensure base URL starts with http:// or https://
        if !base_url.starts_with("http://") && !base_url.starts_with("https://") {
            return Err(Error::ConnectionError(format!(
                "HTTP base_url must start with http:// or https://, got: {}",
                base_url
            )));
        }

        // Bound individual RPC requests; SSE connections remain open until closed.
        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| Error::ConnectionError(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            base_url,
            auth_token,
            client,
            copies: CopyContext::new(
                CopyPolicy::AllowCopies,
                format!("http/client/{}", uuid::Uuid::new_v4()),
            ),
        })
    }

    /// Bind explicit authority for all subsequent requests and stream handles.
    /// Retain a clone to inspect accounting after the client/session is dropped.
    pub fn with_copy_context(mut self, copies: CopyContext) -> Self {
        self.copies = copies;
        self
    }

    /// Retain this client's compatibility-transfer ledger.
    pub fn copy_context(&self) -> CopyContext {
        self.copies.clone()
    }

    /// Build authorization header if token is present
    fn build_auth_header(&self) -> Option<String> {
        self.auth_token
            .as_ref()
            .map(|token| format!("Bearer {}", token))
    }
}

/// Request body for /execute endpoint
#[derive(Debug, Serialize)]
struct ExecuteRequest {
    /// Pipeline manifest
    manifest: serde_json::Value,
    /// Input data
    input: TransportData,
}

/// Response body from /execute endpoint
#[derive(Debug, Deserialize)]
struct ExecuteResponse {
    /// Output data
    output: TransportData,
}

/// Request body for POST /stream (create session)
#[derive(Debug, Serialize)]
struct CreateStreamRequest {
    /// Pipeline manifest
    manifest: serde_json::Value,
}

/// Response from POST /stream
#[derive(Debug, Deserialize)]
struct CreateStreamResponse {
    /// Unique session ID
    session_id: String,
}

/// Request body for POST /stream/:id/input
#[derive(Debug, Serialize)]
struct StreamInputRequest {
    /// Input data
    data: TransportData,
}

#[async_trait]
impl PipelineClient for HttpPipelineClient {
    /// Execute a pipeline with unary semantics
    ///
    /// Sends POST request to /execute endpoint with manifest and input data.
    async fn execute_unary(
        &self,
        manifest: Arc<Manifest>,
        input: TransportData,
        _embedded_plugins: &[(String, Vec<u8>)],
        _embedded_python_env: Option<&EmbeddedPythonEnv>,
    ) -> remotemedia_core::Result<TransportData> {
        let url = format!("{}/execute", self.base_url);

        // Serialize manifest to JSON
        let manifest_json = serde_json::to_value(manifest.as_ref()).map_err(|e| {
            remotemedia_core::Error::Transport(format!("Failed to serialize manifest: {}", e))
        })?;

        let request_body = ExecuteRequest {
            manifest: manifest_json,
            input,
        };

        // Build request
        let bytes = codec::encode(
            &request_body,
            request_body.input.data.bulk_payload_bytes(),
            &self.copies,
        )?;
        let mut request = self
            .client
            .post(&url)
            .timeout(std::time::Duration::from_secs(30))
            .header("content-type", "application/json")
            .body(bytes);

        // Add auth header if present
        if let Some(auth_header) = self.build_auth_header() {
            request = request.header("authorization", auth_header);
        }

        // Send request
        let response = request.send().await.map_err(|e| {
            remotemedia_core::Error::RemoteExecutionFailed(format!("HTTP request failed: {}", e))
        })?;

        // Check status
        let response = checked_response(response)
            .await
            .map_err(remotemedia_core::Error::RemoteExecutionFailed)?;

        // Parse response
        let bytes = read_response(response, &self.copies).await?;
        let execute_response: ExecuteResponse = codec::decode(&bytes, &self.copies)?;

        Ok(execute_response.output)
    }

    /// Create a streaming session with SSE
    ///
    /// Creates a session on the server and returns a handle for sending inputs
    /// and receiving outputs via Server-Sent Events.
    async fn create_stream_session(
        &self,
        manifest: Arc<Manifest>,
        _embedded_plugins: &[(String, Vec<u8>)],
        _embedded_python_env: Option<&EmbeddedPythonEnv>,
    ) -> remotemedia_core::Result<Box<dyn ClientStreamSession>> {
        let url = format!("{}/stream", self.base_url);

        // Serialize manifest
        let manifest_json = serde_json::to_value(manifest.as_ref()).map_err(|e| {
            remotemedia_core::Error::Transport(format!("Failed to serialize manifest: {}", e))
        })?;

        let request_body = CreateStreamRequest {
            manifest: manifest_json,
        };

        // Build request
        let mut request = self
            .client
            .post(&url)
            .timeout(std::time::Duration::from_secs(30))
            .json(&request_body);
        if let Some(auth_header) = self.build_auth_header() {
            request = request.header("authorization", auth_header);
        }

        // Send request
        let response = request.send().await.map_err(|e| {
            remotemedia_core::Error::RemoteExecutionFailed(format!(
                "Failed to create session: {}",
                e
            ))
        })?;

        let response = checked_response(response)
            .await
            .map_err(remotemedia_core::Error::RemoteExecutionFailed)?;

        // Parse response
        let create_response: CreateStreamResponse = response.json().await.map_err(|e| {
            remotemedia_core::Error::RemoteExecutionFailed(format!(
                "Failed to parse response: {}",
                e
            ))
        })?;

        // Create stream session
        let session = HttpStreamSession::new(
            create_response.session_id.clone(),
            self.base_url.clone(),
            self.auth_token.clone(),
            self.client.clone(),
            self.copies.clone(),
        )
        .await;
        match session {
            Ok(session) => Ok(Box::new(session)),
            Err(error) => {
                let cleanup = delete_session(
                    &self.client,
                    &self.base_url,
                    &create_response.session_id,
                    self.auth_token.as_deref(),
                )
                .await;
                let detail = cleanup
                    .err()
                    .map(|e| format!("; session cleanup failed: {e}"))
                    .unwrap_or_default();
                Err(codec::error(format!("{error}{detail}")))
            }
        }
    }

    /// Check if the remote endpoint is healthy
    ///
    /// Sends GET request to /health endpoint.
    async fn health_check(&self) -> remotemedia_core::Result<bool> {
        let url = format!("{}/health", self.base_url);

        // Build request
        let mut request = self.client.get(&url);

        // Add auth header if present (optional for health checks)
        if let Some(auth_header) = self.build_auth_header() {
            request = request.header("authorization", auth_header);
        }

        // Send request with short timeout
        match tokio::time::timeout(std::time::Duration::from_secs(5), request.send()).await {
            Ok(Ok(response)) => Ok(response.status().is_success()),
            Ok(Err(e)) => {
                tracing::warn!("Health check failed for {}: {}", self.base_url, e);
                Ok(false)
            }
            Err(_) => {
                tracing::warn!("Health check timeout for {}", self.base_url);
                Ok(false)
            }
        }
    }
}

/// HTTP streaming session using SSE for output
///
/// Uses Server-Sent Events (SSE) for receiving outputs from the server.
/// Inputs are sent via POST requests.
pub struct HttpStreamSession {
    /// Unique session ID
    session_id: String,

    /// Base URL
    base_url: String,

    /// Auth token
    auth_token: Option<String>,

    /// HTTP client
    client: reqwest::Client,

    /// Receiver for output data from SSE stream.
    ///
    /// Bounded — the background SSE parser task `.await`s its send, so
    /// a slow consumer back-pressures the SSE stream parsing rather
    /// than buffering events unboundedly in memory.
    output_rx: mpsc::Receiver<remotemedia_core::Result<TransportData>>,
    copies: CopyContext,

    /// Whether the session is active
    active: bool,

    /// Handle to the background SSE task
    sse_task: Option<tokio::task::JoinHandle<()>>,
}

impl HttpStreamSession {
    /// Create a new HTTP stream session with SSE
    async fn new(
        session_id: String,
        base_url: String,
        auth_token: Option<String>,
        client: reqwest::Client,
        copies: CopyContext,
    ) -> Result<Self> {
        // Create bounded channel for SSE outputs. Small capacity because
        // SSE events are typically text/JSON frames and we want to
        // back-pressure the stream quickly when the consumer falls behind.
        // Override via `REMOTEMEDIA_HTTP_CLIENT_OUTPUT_CAPACITY`.
        let capacity: usize = std::env::var("REMOTEMEDIA_HTTP_CLIENT_OUTPUT_CAPACITY")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&n: &usize| n > 0)
            .unwrap_or(16);
        let (output_tx, output_rx) = mpsc::channel(capacity);

        // Establish the subscription before returning, so the first input cannot
        // race the server's subscriber registration. HTTP failures remain visible.
        let sse_url = format!("{}/stream/{}/output", base_url, session_id);
        let mut request = client.get(sse_url);
        if let Some(token) = &auth_token {
            request = request.bearer_auth(token);
        }
        let response = tokio::time::timeout(std::time::Duration::from_secs(30), request.send())
            .await
            .map_err(|e| Error::ConnectionError(e.to_string()))??
            .error_for_status()?;
        let stream_copies = copies.clone();
        let sse_task = tokio::spawn(async move {
            if let Err(error) = receive_sse(response, &output_tx, &stream_copies).await {
                let _ = output_tx.send(Err(error)).await;
            }
        });

        Ok(Self {
            session_id,
            base_url,
            auth_token,
            client,
            output_rx,
            active: true,
            sse_task: Some(sse_task),
            copies,
        })
    }
}

#[async_trait]
impl ClientStreamSession for HttpStreamSession {
    fn session_id(&self) -> &str {
        &self.session_id
    }

    async fn send(&mut self, data: TransportData) -> remotemedia_core::Result<()> {
        if !self.active {
            return Err(remotemedia_core::Error::Transport(
                "Session is closed".to_string(),
            ));
        }

        let url = format!("{}/stream/{}/input", self.base_url, self.session_id);

        let request_body = StreamInputRequest { data };

        // Build request
        let bytes = codec::encode(
            &request_body,
            request_body.data.data.bulk_payload_bytes(),
            &self.copies,
        )?;
        let mut request = self
            .client
            .post(&url)
            .timeout(std::time::Duration::from_secs(30))
            .header("content-type", "application/json")
            .body(bytes);
        if let Some(auth_header) = self.build_auth_header() {
            request = request.header("authorization", auth_header);
        }

        // Send request
        let response = request.send().await.map_err(|e| {
            remotemedia_core::Error::Transport(format!("Failed to send data: {}", e))
        })?;

        let _response = checked_response(response)
            .await
            .map_err(remotemedia_core::Error::Transport)?;

        Ok(())
    }

    async fn receive(&mut self) -> remotemedia_core::Result<Option<TransportData>> {
        if !self.active {
            return Ok(None);
        }

        // Receive from SSE stream channel
        match self.output_rx.recv().await {
            Some(Ok(data)) => Ok(Some(data)),
            Some(Err(error)) => {
                self.active = false;
                Err(error)
            }
            None => {
                // SSE stream ended
                self.active = false;
                Ok(None)
            }
        }
    }

    async fn close(&mut self) -> remotemedia_core::Result<()> {
        self.active = false;
        self.output_rx.close();
        if let Some(task) = self.sse_task.take() {
            task.abort();
            // Join releases retained wire chunks, including a blocked output send.
            let _ = task.await;
        }
        while self.output_rx.try_recv().is_ok() {}
        delete_session(
            &self.client,
            &self.base_url,
            &self.session_id,
            self.auth_token.as_deref(),
        )
        .await
    }

    fn is_active(&self) -> bool {
        self.active
    }
}

impl HttpStreamSession {
    fn build_auth_header(&self) -> Option<String> {
        self.auth_token
            .as_ref()
            .map(|token| format!("Bearer {}", token))
    }
}

impl Drop for HttpStreamSession {
    fn drop(&mut self) {
        if let Some(task) = &self.sse_task {
            task.abort();
        }
    }
}

async fn receive_sse(
    response: reqwest::Response,
    outputs: &mpsc::Sender<remotemedia_core::Result<TransportData>>,
    copies: &CopyContext,
) -> remotemedia_core::Result<()> {
    let mut stream = response.bytes_stream();
    let mut frames = sse::Frames::default();
    while let Some(chunk) = stream.next().await {
        frames.push(chunk.map_err(codec::error)?);
        while let Some(frame) = frames.next(copies)? {
            if let Some(data) = sse::decode(&frame, copies)? {
                if outputs.send(Ok(data)).await.is_err() {
                    return Ok(());
                }
            }
        }
    }
    if !frames.is_empty() {
        return Err(codec::error(
            "SSE connection ended with an incomplete event",
        ));
    }
    Ok(())
}

async fn checked_response(
    response: reqwest::Response,
) -> std::result::Result<reqwest::Response, String> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let detail = response
        .text()
        .await
        .map_err(|error| format!("HTTP {status}: failed to read error response: {error}"))?;
    Err(format!(
        "HTTP {} {}: {}",
        status.as_u16(),
        status.canonical_reason().unwrap_or("Unknown"),
        detail
    ))
}

async fn delete_session(
    client: &reqwest::Client,
    base_url: &str,
    session_id: &str,
    auth: Option<&str>,
) -> remotemedia_core::Result<()> {
    let mut request = client
        .delete(format!("{base_url}/stream/{session_id}"))
        .timeout(std::time::Duration::from_secs(30));
    if let Some(token) = auth {
        request = request.bearer_auth(token);
    }
    let response = request.send().await.map_err(codec::error)?;
    if response.status() != reqwest::StatusCode::NOT_FOUND {
        response.error_for_status().map_err(codec::error)?;
    }
    Ok(())
}

async fn read_response(
    response: reqwest::Response,
    copies: &CopyContext,
) -> remotemedia_core::Result<bytes::Bytes> {
    let mut stream = response.bytes_stream();
    let mut chunks = Vec::new();
    let mut size = 0usize;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(codec::error)?;
        size = size
            .checked_add(chunk.len())
            .ok_or_else(|| codec::error("HTTP body size overflow"))?;
        if !chunk.is_empty() {
            chunks.push(chunk);
        }
    }
    if chunks.len() <= 1 {
        return Ok(chunks.pop().unwrap_or_default());
    }
    copies
        .materialize(
            remotemedia_types::CopyReason::VecMaterialization,
            size as u64,
            || {
                let mut bytes = Vec::with_capacity(size);
                for chunk in chunks {
                    bytes.extend_from_slice(&chunk);
                }
                bytes::Bytes::from(bytes)
            },
        )
        .map_err(codec::error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_client() {
        let client = HttpPipelineClient::new("http://localhost:8080", None).await;
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_create_client_with_auth() {
        let client =
            HttpPipelineClient::new("https://api.example.com", Some("test-token".to_string()))
                .await;
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_empty_url_error() {
        let client = HttpPipelineClient::new("", None).await;
        assert!(client.is_err());
    }

    #[tokio::test]
    async fn test_invalid_url_scheme() {
        let client = HttpPipelineClient::new("ftp://invalid.com", None).await;
        assert!(client.is_err());
    }

    #[tokio::test]
    async fn test_health_check_unreachable() {
        let client = HttpPipelineClient::new("http://localhost:9999", None)
            .await
            .unwrap();
        let result = client.health_check().await;
        // Should return Ok(false) for unreachable endpoint
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }
}
