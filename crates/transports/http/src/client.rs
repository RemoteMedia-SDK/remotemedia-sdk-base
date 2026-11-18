//! HTTP/REST client implementation with streamed protobuf output
//!
//! This module provides an HTTP client that implements the `PipelineClient` trait
//! for executing pipelines on remote HTTP/REST servers.
//!
//! # Features
//!
//! - Unary execution via POST /execute
//! - Streaming via POST /stream (create session) and GET /stream/:id/output
//! - Health checks via GET /health
//! - Authentication via Authorization header
//! - Frames as protobuf `DataBuffer` (see `codec`); manifests as JSON
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
use crate::{codec, stream};
use async_trait::async_trait;
use futures::StreamExt;
use remotemedia_core::manifest::Manifest;
use remotemedia_core::transport::client::EmbeddedPythonEnv;
use remotemedia_core::transport::{ClientStreamSession, PipelineClient, TransportData};
use remotemedia_types::{CopyContext, CopyPolicy};
use serde::{Deserialize, Serialize};

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// How long the client waits on the server.
///
/// Creating a session (`POST /stream`) and a unary run (`POST /execute`)
/// answer only after the server has built the pipeline, which for a cold
/// pipeline means loading its models (and possibly building their Python
/// environments): minutes, not seconds. Every other request (sending an
/// input, opening the output stream, closing a session) is bounded by
/// `request`. Connecting is always bounded by [`HttpTimeouts::CONNECT`], so
/// an unreachable server still fails fast.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HttpTimeouts {
    /// Session creation and unary execution; `None` waits indefinitely.
    pub session_setup: Option<Duration>,
    /// Every other request.
    pub request: Duration,
}

impl HttpTimeouts {
    /// Bound on establishing a connection, whatever the request.
    pub const CONNECT: Duration = Duration::from_secs(10);
}

impl Default for HttpTimeouts {
    fn default() -> Self {
        Self {
            session_setup: Some(Duration::from_secs(600)),
            request: Duration::from_secs(30),
        }
    }
}

/// Apply an optional timeout to a request.
fn bounded(request: reqwest::RequestBuilder, timeout: Option<Duration>) -> reqwest::RequestBuilder {
    match timeout {
        Some(timeout) => request.timeout(timeout),
        None => request,
    }
}

/// An error message naming a timed-out request's limit, so a caller can tell
/// a slow server from a broken one.
fn send_error(what: &str, error: reqwest::Error, limit: Option<Duration>) -> String {
    match (error.is_timeout(), limit) {
        (true, Some(limit)) => format!(
            "{what}: no response within {limit:?} (raise the limit with HttpPipelineClient::with_timeouts)"
        ),
        _ => format!("{what}: {error}"),
    }
}

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
    timeouts: HttpTimeouts,
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

        // Requests are bounded per call (`HttpTimeouts`); output streams stay
        // open until closed.
        let client = reqwest::Client::builder()
            .connect_timeout(HttpTimeouts::CONNECT)
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
            timeouts: HttpTimeouts::default(),
        })
    }

    /// Set how long requests wait on the server (see [`HttpTimeouts`]).
    /// Stream sessions created afterwards keep these limits.
    pub fn with_timeouts(mut self, timeouts: HttpTimeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// The limits requests wait on the server.
    pub fn timeouts(&self) -> HttpTimeouts {
        self.timeouts
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

        // The manifest is configuration (JSON); the input is a protobuf frame.
        let manifest_json = serde_json::to_vec(manifest.as_ref()).map_err(|e| {
            remotemedia_core::Error::Transport(format!("Failed to serialize manifest: {}", e))
        })?;
        let bytes = codec::encode_execute_request(&manifest_json, &input, &self.copies)?;
        let mut request = bounded(self.client.post(&url), self.timeouts.session_setup)
            .header("content-type", codec::PROTOBUF)
            .header("accept", codec::PROTOBUF)
            .body(bytes);

        // Add auth header if present
        if let Some(auth_header) = self.build_auth_header() {
            request = request.header("authorization", auth_header);
        }

        // Send request
        let response = request.send().await.map_err(|e| {
            remotemedia_core::Error::RemoteExecutionFailed(send_error(
                "HTTP request failed",
                e,
                self.timeouts.session_setup,
            ))
        })?;

        // Check status
        let response = checked_response(response)
            .await
            .map_err(remotemedia_core::Error::RemoteExecutionFailed)?;

        let body = crate::body::Body::collect(response.bytes_stream(), usize::MAX).await?;
        codec::frame_from_buffer(body.decode()?, &self.copies)
    }

    /// Create a streaming session
    ///
    /// Creates a session on the server and returns a handle for sending inputs
    /// and receiving outputs on a stream of delimited protobuf events.
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
        // The server answers once the pipeline is built (models loaded).
        let mut request =
            bounded(self.client.post(&url), self.timeouts.session_setup).json(&request_body);
        if let Some(auth_header) = self.build_auth_header() {
            request = request.header("authorization", auth_header);
        }

        // Send request
        let response = request.send().await.map_err(|e| {
            remotemedia_core::Error::RemoteExecutionFailed(send_error(
                "Failed to create session",
                e,
                self.timeouts.session_setup,
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
            self.timeouts,
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
                    self.timeouts.request,
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
        match tokio::time::timeout(Duration::from_secs(5), request.send()).await {
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

/// HTTP streaming session: inputs are POSTed, outputs arrive on one stream
///
/// of delimited protobuf events.
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

    /// Receiver for output data from the output stream.
    ///
    /// Bounded — the background parser task `.await`s its send, so
    /// a slow consumer back-pressures stream parsing rather
    /// than buffering events unboundedly in memory.
    output_rx: mpsc::Receiver<remotemedia_core::Result<TransportData>>,
    copies: CopyContext,
    timeouts: HttpTimeouts,

    /// Whether the session is active
    active: bool,

    /// Handle to the background output-stream task
    output_task: Option<tokio::task::JoinHandle<()>>,
}

impl HttpStreamSession {
    /// Create a new HTTP stream session
    async fn new(
        session_id: String,
        base_url: String,
        auth_token: Option<String>,
        client: reqwest::Client,
        copies: CopyContext,
        timeouts: HttpTimeouts,
    ) -> Result<Self> {
        // Bounded channel for stream outputs, small so a consumer that falls
        // behind back-pressures the stream quickly.
        // Override via `REMOTEMEDIA_HTTP_CLIENT_OUTPUT_CAPACITY`.
        let capacity: usize = std::env::var("REMOTEMEDIA_HTTP_CLIENT_OUTPUT_CAPACITY")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&n: &usize| n > 0)
            .unwrap_or(16);
        let (output_tx, output_rx) = mpsc::channel(capacity);

        // Establish the subscription before returning, so the first input cannot
        // race the server's subscriber registration. HTTP failures remain visible.
        let output_url = format!("{}/stream/{}/output", base_url, session_id);
        let mut request = client
            .get(output_url)
            .header("accept", codec::PROTOBUF_STREAM);
        if let Some(token) = &auth_token {
            request = request.bearer_auth(token);
        }
        let response = tokio::time::timeout(timeouts.request, request.send())
            .await
            .map_err(|e| Error::ConnectionError(e.to_string()))??
            .error_for_status()?;
        let stream_copies = copies.clone();
        let output_task = tokio::spawn(async move {
            if let Err(error) = receive_stream(response, &output_tx, &stream_copies).await {
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
            output_task: Some(output_task),
            copies,
            timeouts,
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

        let bytes = codec::encode_frame(&data, &self.copies)?;
        let mut request = self
            .client
            .post(&url)
            .timeout(self.timeouts.request)
            .header("content-type", codec::PROTOBUF)
            .body(bytes);
        if let Some(auth_header) = self.build_auth_header() {
            request = request.header("authorization", auth_header);
        }

        // Send request
        let response = request.send().await.map_err(|e| {
            remotemedia_core::Error::Transport(send_error(
                "Failed to send data",
                e,
                Some(self.timeouts.request),
            ))
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

        // Receive from the output stream channel
        match self.output_rx.recv().await {
            Some(Ok(data)) => Ok(Some(data)),
            Some(Err(error)) => {
                self.active = false;
                Err(error)
            }
            None => {
                // Output stream ended
                self.active = false;
                Ok(None)
            }
        }
    }

    async fn close(&mut self) -> remotemedia_core::Result<()> {
        self.active = false;
        self.output_rx.close();
        if let Some(task) = self.output_task.take() {
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
            self.timeouts.request,
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
        if let Some(task) = &self.output_task {
            task.abort();
        }
    }
}

async fn receive_stream(
    response: reqwest::Response,
    outputs: &mpsc::Sender<remotemedia_core::Result<TransportData>>,
    copies: &CopyContext,
) -> remotemedia_core::Result<()> {
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    if !codec::is_protobuf(content_type) {
        return Err(codec::error(format!(
            "output stream has content-type {content_type:?}, expected {}",
            codec::PROTOBUF_STREAM
        )));
    }
    let mut body = response.bytes_stream();
    let mut events = stream::Events::default();
    while let Some(chunk) = body.next().await {
        events.push(chunk.map_err(codec::error)?);
        while let Some(event) = events.next(copies)? {
            if let Some(data) = stream::decode(event, copies)? {
                if outputs.send(Ok(data)).await.is_err() {
                    return Ok(());
                }
            }
        }
    }
    if !events.is_empty() {
        return Err(codec::error("output stream ended with an incomplete event"));
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
    timeout: Duration,
) -> remotemedia_core::Result<()> {
    let mut request = client
        .delete(format!("{base_url}/stream/{session_id}"))
        .timeout(timeout);
    if let Some(token) = auth {
        request = request.bearer_auth(token);
    }
    let response = request.send().await.map_err(codec::error)?;
    if response.status() != reqwest::StatusCode::NOT_FOUND {
        response.error_for_status().map_err(codec::error)?;
    }
    Ok(())
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

    /// A server that takes `delay` to create a session (as when it loads a
    /// pipeline's models), then serves an output stream with one keepalive.
    async fn slow_setup_server(delay: Duration) -> (String, tokio::task::JoinHandle<()>) {
        use axum::routing::{delete, get, post};
        let router = axum::Router::new()
            .route(
                "/stream",
                post(move || async move {
                    tokio::time::sleep(delay).await;
                    axum::Json(serde_json::json!({"session_id": "slow"}))
                }),
            )
            .route(
                "/stream/slow/output",
                get(|| async {
                    (
                        [("content-type", codec::PROTOBUF_STREAM)],
                        codec::KEEPALIVE.to_vec(),
                    )
                }),
            )
            .route(
                "/stream/slow",
                delete(|| async { axum::http::StatusCode::OK }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (url, task)
    }

    fn manifest() -> Arc<Manifest> {
        Arc::new(
            serde_json::from_value(serde_json::json!({
                "version":"v1", "metadata":{}, "nodes":[], "connections":[]
            }))
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn session_setup_may_take_longer_than_a_request() {
        let (url, task) = slow_setup_server(Duration::from_millis(1500)).await;
        // Setup outlasting the per-request limit still succeeds.
        let client = HttpPipelineClient::new(&url, None)
            .await
            .unwrap()
            .with_timeouts(HttpTimeouts {
                session_setup: Some(Duration::from_secs(10)),
                request: Duration::from_millis(500),
            });
        let mut session = client
            .create_stream_session(manifest(), &[], None)
            .await
            .unwrap();
        session.close().await.unwrap();
        // So does waiting without a limit.
        let client = client.with_timeouts(HttpTimeouts {
            session_setup: None,
            request: Duration::from_millis(500),
        });
        let mut session = client
            .create_stream_session(manifest(), &[], None)
            .await
            .unwrap();
        session.close().await.unwrap();
        task.abort();
    }

    #[tokio::test]
    async fn session_setup_limit_is_reported_as_a_timeout() {
        let (url, task) = slow_setup_server(Duration::from_secs(5)).await;
        let client = HttpPipelineClient::new(&url, None)
            .await
            .unwrap()
            .with_timeouts(HttpTimeouts {
                session_setup: Some(Duration::from_millis(300)),
                ..HttpTimeouts::default()
            });
        let error = match client.create_stream_session(manifest(), &[], None).await {
            Ok(_) => panic!("session setup should time out"),
            Err(error) => error.to_string(),
        };
        assert!(error.contains("no response within 300ms"), "{error}");
        assert!(error.contains("with_timeouts"), "{error}");
        task.abort();
    }

    #[test]
    fn default_setup_limit_allows_cold_pipelines() {
        let timeouts = HttpTimeouts::default();
        assert!(timeouts.session_setup.unwrap() >= Duration::from_secs(300));
        assert_eq!(timeouts.request, Duration::from_secs(30));
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
