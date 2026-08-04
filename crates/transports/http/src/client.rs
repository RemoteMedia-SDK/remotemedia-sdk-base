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
use async_trait::async_trait;
use futures::StreamExt;
use remotemedia_core::manifest::Manifest;
use remotemedia_core::transport::{ClientStreamSession, PipelineClient, TransportData};
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

        // Create reqwest client with timeout
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| Error::ConnectionError(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            base_url,
            auth_token,
            client,
        })
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

/// SSE event data
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Fields used during deserialization
struct SseEvent {
    /// Output data
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
        let mut request = self.client.post(&url).json(&request_body);

        // Add auth header if present
        if let Some(auth_header) = self.build_auth_header() {
            request = request.header("authorization", auth_header);
        }

        // Send request
        let response = request.send().await.map_err(|e| {
            remotemedia_core::Error::RemoteExecutionFailed(format!("HTTP request failed: {}", e))
        })?;

        // Check status
        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(remotemedia_core::Error::RemoteExecutionFailed(format!(
                "HTTP {} {}: {}",
                status.as_u16(),
                status.canonical_reason().unwrap_or("Unknown"),
                error_text
            )));
        }

        // Parse response
        let execute_response: ExecuteResponse = response.json().await.map_err(|e| {
            remotemedia_core::Error::RemoteExecutionFailed(format!(
                "Failed to parse response: {}",
                e
            ))
        })?;

        Ok(execute_response.output)
    }

    /// Create a streaming session with SSE
    ///
    /// Creates a session on the server and returns a handle for sending inputs
    /// and receiving outputs via Server-Sent Events.
    async fn create_stream_session(
        &self,
        manifest: Arc<Manifest>,
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
        let mut request = self.client.post(&url).json(&request_body);
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

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(remotemedia_core::Error::RemoteExecutionFailed(format!(
                "HTTP {} {}: {}",
                status.as_u16(),
                status.canonical_reason().unwrap_or("Unknown"),
                error_text
            )));
        }

        // Parse response
        let create_response: CreateStreamResponse = response.json().await.map_err(|e| {
            remotemedia_core::Error::RemoteExecutionFailed(format!(
                "Failed to parse response: {}",
                e
            ))
        })?;

        // Create stream session
        let session = HttpStreamSession::new(
            create_response.session_id,
            self.base_url.clone(),
            self.auth_token.clone(),
            self.client.clone(),
        )
        .await
        .map_err(|e| remotemedia_core::Error::Transport(e.to_string()))?;

        Ok(Box::new(session))
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
    output_rx: mpsc::Receiver<TransportData>,

    /// Whether the session is active
    active: bool,

    /// Handle to the background SSE task
    _sse_task: tokio::task::JoinHandle<()>,
}

impl HttpStreamSession {
    /// Create a new HTTP stream session with SSE
    async fn new(
        session_id: String,
        base_url: String,
        auth_token: Option<String>,
        client: reqwest::Client,
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

        // Start SSE stream in background task
        let sse_url = format!("{}/stream/{}/output", base_url, session_id);
        let sse_client = client.clone();
        let sse_auth = auth_token.clone();

        let sse_task = tokio::spawn(async move {
            let mut request = sse_client.get(&sse_url);
            if let Some(auth_header) = sse_auth.as_ref().map(|t| format!("Bearer {}", t)) {
                request = request.header("authorization", auth_header);
            }

            match request.send().await {
                Ok(response) => {
                    let mut stream = response.bytes_stream();

                    // Simple SSE parsing: read "data: {json}\n\n" events
                    let mut buffer = String::new();

                    while let Some(chunk_result) = stream.next().await {
                        match chunk_result {
                            Ok(chunk) => {
                                if let Ok(text) = std::str::from_utf8(&chunk) {
                                    buffer.push_str(text);

                                    // Process complete SSE events
                                    while let Some(event_end) = buffer.find("\n\n") {
                                        let event = buffer[..event_end].to_string();
                                        buffer.drain(..event_end + 2);

                                        // Parse "data: {json}" format
                                        if let Some(data_line) = event.strip_prefix("data: ") {
                                            if let Ok(transport_data) =
                                                serde_json::from_str::<TransportData>(
                                                    data_line.trim(),
                                                )
                                            {
                                                // Bounded: .await applies backpressure to SSE parsing
                                                // when the consumer falls behind.
                                                if output_tx.send(transport_data).await.is_err() {
                                                    tracing::debug!(
                                                        "SSE receiver dropped, stopping stream"
                                                    );
                                                    return;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!("SSE stream error: {}", e);
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to connect to SSE stream: {}", e);
                }
            }
        });

        Ok(Self {
            session_id,
            base_url,
            auth_token,
            client,
            output_rx,
            active: true,
            _sse_task: sse_task,
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
        let mut request = self.client.post(&url).json(&request_body);
        if let Some(auth_header) = self.build_auth_header() {
            request = request.header("authorization", auth_header);
        }

        // Send request
        let response = request.send().await.map_err(|e| {
            remotemedia_core::Error::Transport(format!("Failed to send data: {}", e))
        })?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            return Err(remotemedia_core::Error::Transport(format!(
                "HTTP {} {}: {}",
                status.as_u16(),
                status.canonical_reason().unwrap_or("Unknown"),
                error_text
            )));
        }

        Ok(())
    }

    async fn receive(&mut self) -> remotemedia_core::Result<Option<TransportData>> {
        if !self.active {
            return Ok(None);
        }

        // Receive from SSE stream channel
        match self.output_rx.recv().await {
            Some(data) => Ok(Some(data)),
            None => {
                // SSE stream ended
                self.active = false;
                Ok(None)
            }
        }
    }

    async fn close(&mut self) -> remotemedia_core::Result<()> {
        if !self.active {
            return Ok(());
        }

        // Send DELETE request to close session
        let url = format!("{}/stream/{}", self.base_url, self.session_id);
        let mut request = self.client.delete(&url);
        if let Some(auth_header) = self.build_auth_header() {
            request = request.header("authorization", auth_header);
        }

        let _ = request.send().await; // Ignore errors on close

        self.active = false;
        tracing::info!("HTTP stream session {} closed", self.session_id);
        Ok(())
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
