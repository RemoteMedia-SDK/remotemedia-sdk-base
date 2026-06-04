//! HTTP/REST server implementation with SSE streaming
//!
//! Provides HTTP endpoints for pipeline execution:
//! - POST /execute - Unary execution
//! - POST /stream - Create streaming session
//! - POST /stream/:id/input - Send input to session
//! - GET /stream/:id/output - Receive outputs via SSE
//! - DELETE /stream/:id - Close session
//! - GET /health - Health check

use crate::error::{Error, Result};
use async_trait::async_trait;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::sse::{Event, KeepAlive, Sse},
    routing::{delete, get, post},
    Json, Router,
};
use futures::stream::Stream;
use remotemedia_core::manifest::Manifest;
use remotemedia_core::transport::{
    Participant, PipelineExecutor, PipelineTransport, SharedPipelineStreamSession, StreamSession,
    TransportData,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt as _;

/// HTTP server state shared across handlers
#[derive(Clone)]
struct ServerState {
    /// Pipeline executor for executing pipelines (spec 026 migration)
    executor: Arc<PipelineExecutor>,
    /// Active streaming sessions
    sessions: Arc<RwLock<HashMap<String, SessionHandle>>>,
}

/// Handle to a streaming session
#[allow(dead_code)] // Fields used internally
struct SessionHandle {
    /// Session ID
    session_id: String,
    /// Stream session from executor (spec 026 migration)
    session: SessionHandleWrapper,
    /// Broadcast channel for sending outputs to multiple SSE subscribers
    output_tx: broadcast::Sender<TransportData>,
}

/// HTTP server with SSE streaming support
pub struct HttpServer {
    /// Server bind address
    bind_address: String,
    /// Shared server state
    state: ServerState,
}

impl HttpServer {
    /// Create a new HTTP server
    ///
    /// # Arguments
    ///
    /// * `bind_address` - Address to bind to (e.g., "127.0.0.1:8080")
    /// * `executor` - Pipeline executor for executing pipelines (spec 026 migration)
    ///
    /// # Returns
    ///
    /// * `Ok(HttpServer)` - Server created successfully
    /// * `Err(Error)` - Failed to create server
    pub async fn new(bind_address: String, executor: Arc<PipelineExecutor>) -> Result<Self> {
        let state = ServerState {
            executor,
            sessions: Arc::new(RwLock::new(HashMap::new())),
        };

        Ok(Self {
            bind_address,
            state,
        })
    }

    /// Build the router with all endpoints
    fn build_router(&self) -> Router {
        Router::new()
            .route("/health", get(health_handler))
            .route("/execute", post(execute_handler))
            .route("/stream", post(create_stream_handler))
            .route("/stream/:session_id/input", post(stream_input_handler))
            .route("/stream/:session_id/output", get(stream_output_handler))
            .route("/stream/:session_id", delete(close_stream_handler))
            .with_state(self.state.clone())
            .layer(
                tower::ServiceBuilder::new()
                    .layer(tower_http::trace::TraceLayer::new_for_http())
                    .layer(tower_http::cors::CorsLayer::permissive()),
            )
    }

    /// Start the HTTP server
    ///
    /// This method blocks until the server is shut down.
    pub async fn serve(self) -> Result<()> {
        let addr: std::net::SocketAddr = self
            .bind_address
            .parse()
            .map_err(|e| Error::ServerError(format!("Invalid bind address: {}", e)))?;

        tracing::info!("Starting HTTP server on {}", addr);

        let router = self.build_router();

        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| Error::ServerError(format!("Failed to bind: {}", e)))?;

        axum::serve(listener, router)
            .await
            .map_err(|e| Error::ServerError(format!("Server error: {}", e)))?;

        Ok(())
    }

    /// Start or join a streaming pipeline session shared by logical key.
    ///
    /// This opt-in API lets HTTP entry points participate in a multi-transport
    /// room while preserving the existing `/stream` fresh-session behavior.
    pub async fn shared_stream(
        &self,
        key: impl Into<String>,
        manifest: Arc<Manifest>,
        participant: Participant,
    ) -> remotemedia_core::Result<Box<dyn StreamSession>> {
        let shared = self
            .state
            .executor
            .get_or_create_shared_session(key, manifest)
            .await?;
        Ok(Box::new(SharedPipelineStreamSession::new(
            &shared,
            participant,
        )))
    }
}

#[async_trait]
impl PipelineTransport for HttpServer {
    async fn execute(
        &self,
        manifest: Arc<Manifest>,
        input: TransportData,
    ) -> remotemedia_core::Result<TransportData> {
        self.state.executor.execute_unary(manifest, input).await
    }

    async fn stream(
        &self,
        manifest: Arc<Manifest>,
    ) -> remotemedia_core::Result<Box<dyn remotemedia_core::transport::StreamSession>> {
        let session = self.state.executor.create_session(manifest).await?;
        // Convert SessionHandle to Box<dyn StreamSession>
        // Note: SessionHandle implements the streaming interface
        Ok(Box::new(SessionHandleWrapper(session)))
    }
}

/// Wrapper to adapt PipelineExecutor's SessionHandle to StreamSession trait
struct SessionHandleWrapper(remotemedia_core::transport::SessionHandle);

#[async_trait]
impl StreamSession for SessionHandleWrapper {
    async fn send_input(&mut self, data: TransportData) -> remotemedia_core::Result<()> {
        self.0.send_input(data).await
    }

    async fn recv_output(&mut self) -> remotemedia_core::Result<Option<TransportData>> {
        self.0.recv_output().await
    }

    async fn close(&mut self) -> remotemedia_core::Result<()> {
        self.0.close().await
    }

    fn session_id(&self) -> &str {
        &self.0.session_id
    }

    fn is_active(&self) -> bool {
        self.0.is_active()
    }
}

// Handler implementations

/// Health check endpoint
async fn health_handler() -> StatusCode {
    StatusCode::OK
}

/// Request body for /execute
#[derive(Debug, Deserialize)]
struct ExecuteRequest {
    manifest: serde_json::Value,
    input: TransportData,
}

/// Response body for /execute
#[derive(Debug, Serialize)]
struct ExecuteResponse {
    output: TransportData,
}

/// Error response body for structured error responses
#[derive(Debug, Serialize)]
struct ErrorResponse {
    /// Error type (e.g., "validation", "execution", "internal")
    error_type: String,
    /// Human-readable error message
    message: String,
    /// Structured validation errors (only for validation errors)
    #[serde(skip_serializing_if = "Option::is_none")]
    validation_errors: Option<serde_json::Value>,
}

/// Map runtime errors to appropriate HTTP status codes and structured responses
fn map_runtime_error(e: remotemedia_core::Error) -> (StatusCode, Json<ErrorResponse>) {
    match e {
        remotemedia_core::Error::Validation(ref validation_errors) => {
            let errors_json = serde_json::to_value(validation_errors).ok();
            (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error_type: "validation".to_string(),
                    message: format!(
                        "{} validation error(s) in node parameters",
                        validation_errors.len()
                    ),
                    validation_errors: errors_json,
                }),
            )
        }
        remotemedia_core::Error::Manifest(msg) | remotemedia_core::Error::InvalidManifest(msg) => (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error_type: "manifest".to_string(),
                message: msg,
                validation_errors: None,
            }),
        ),
        remotemedia_core::Error::InvalidData(msg)
        | remotemedia_core::Error::InvalidInput { message: msg, .. } => (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error_type: "input".to_string(),
                message: msg,
                validation_errors: None,
            }),
        ),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error_type: "execution".to_string(),
                message: e.to_string(),
                validation_errors: None,
            }),
        ),
    }
}

/// POST /execute - Unary pipeline execution
async fn execute_handler(
    State(state): State<ServerState>,
    Json(request): Json<ExecuteRequest>,
) -> std::result::Result<Json<ExecuteResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Parse manifest
    let manifest: Manifest = serde_json::from_value(request.manifest).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error_type: "manifest".to_string(),
                message: format!("Invalid manifest: {}", e),
                validation_errors: None,
            }),
        )
    })?;

    // Execute pipeline - uses map_runtime_error for proper validation error handling
    let output = state
        .executor
        .execute_unary(Arc::new(manifest), request.input)
        .await
        .map_err(map_runtime_error)?;

    Ok(Json(ExecuteResponse { output }))
}

/// Request body for POST /stream
#[derive(Debug, Deserialize)]
struct CreateStreamRequest {
    manifest: serde_json::Value,
}

/// Response body for POST /stream
#[derive(Debug, Serialize)]
struct CreateStreamResponse {
    session_id: String,
}

/// POST /stream - Create a streaming session
async fn create_stream_handler(
    State(state): State<ServerState>,
    Json(request): Json<CreateStreamRequest>,
) -> std::result::Result<Json<CreateStreamResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Parse manifest
    let manifest: Manifest = serde_json::from_value(request.manifest).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error_type: "manifest".to_string(),
                message: format!("Invalid manifest: {}", e),
                validation_errors: None,
            }),
        )
    })?;

    // Create stream session - uses map_runtime_error for proper validation error handling
    let session = state
        .executor
        .create_session(Arc::new(manifest))
        .await
        .map_err(map_runtime_error)?;

    let session_id = session.session_id.clone();

    // Create broadcast channel for SSE (supports multiple subscribers)
    // Capacity of 100 means it can buffer up to 100 outputs before dropping oldest
    let (output_tx, _output_rx) = broadcast::channel(100);

    // Store session (wrap in SessionHandleWrapper for trait compatibility)
    let handle = SessionHandle {
        session_id: session_id.clone(),
        session: SessionHandleWrapper(session),
        output_tx,
    };

    state
        .sessions
        .write()
        .await
        .insert(session_id.clone(), handle);

    tracing::info!("Created streaming session: {}", session_id);

    Ok(Json(CreateStreamResponse { session_id }))
}

/// Request body for POST /stream/:session_id/input
#[derive(Debug, Deserialize)]
struct StreamInputRequest {
    data: TransportData,
}

/// POST /stream/:session_id/input - Send input to streaming session
async fn stream_input_handler(
    State(state): State<ServerState>,
    Path(session_id): Path<String>,
    Json(request): Json<StreamInputRequest>,
) -> std::result::Result<StatusCode, (StatusCode, String)> {
    let mut sessions = state.sessions.write().await;

    let handle = sessions.get_mut(&session_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("Session not found: {}", session_id),
        )
    })?;

    // Send input to session
    handle.session.send_input(request.data).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to send input: {}", e),
        )
    })?;

    // Poll for outputs and broadcast to all SSE subscribers
    while let Ok(Some(output)) = handle.session.recv_output().await {
        // Broadcast ignores send errors (no subscribers) which is fine
        let _ = handle.output_tx.send(output);
    }

    Ok(StatusCode::OK)
}

/// GET /stream/:session_id/output - SSE stream of outputs
async fn stream_output_handler(
    State(state): State<ServerState>,
    Path(session_id): Path<String>,
) -> std::result::Result<
    Sse<impl Stream<Item = std::result::Result<Event, Infallible>>>,
    (StatusCode, String),
> {
    // Get session and subscribe to broadcast channel
    let rx = {
        let sessions = state.sessions.read().await;
        let handle = sessions.get(&session_id).ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                format!("Session not found: {}", session_id),
            )
        })?;

        // Subscribe to broadcast channel (supports multiple SSE connections)
        handle.output_tx.subscribe()
    };

    // Convert broadcast receiver to SSE stream
    let session_id_clone = session_id.clone();
    let stream = BroadcastStream::new(rx).filter_map(move |result| {
        // Filter out lagged messages (when buffer is full)
        match result {
            Ok(data) => {
                let json = serde_json::to_string(&data).unwrap_or_default();
                Some(Ok(Event::default().data(json)))
            }
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)) => {
                tracing::warn!(
                    "SSE stream lagged by {} messages for session {}",
                    n,
                    session_id_clone
                );
                None
            }
        }
    });

    tracing::debug!("SSE connection established for session {}", session_id);

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

/// DELETE /stream/:session_id - Close streaming session
async fn close_stream_handler(
    State(state): State<ServerState>,
    Path(session_id): Path<String>,
) -> std::result::Result<StatusCode, (StatusCode, String)> {
    let mut sessions = state.sessions.write().await;

    let mut handle = sessions.remove(&session_id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("Session not found: {}", session_id),
        )
    })?;

    // Close session
    handle.session.close().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to close session: {}", e),
        )
    })?;

    tracing::info!("Closed streaming session: {}", session_id);

    Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_check() {
        let response = health_handler().await;
        assert_eq!(response, StatusCode::OK);
    }

    #[tokio::test]
    async fn shared_stream_reuses_pipeline_session_by_key() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let server = HttpServer::new("127.0.0.1:0".into(), executor)
            .await
            .unwrap();
        let manifest = Arc::new(Manifest {
            version: "v1".to_string(),
            metadata: Default::default(),
            nodes: Vec::new(),
            connections: Vec::new(),
            python_env: None,
            plugins: Vec::new(),
        });

        let first = server
            .shared_stream(
                "room-a",
                Arc::clone(&manifest),
                Participant::new("http-a", "client"),
            )
            .await
            .unwrap();
        let second = server
            .shared_stream("room-a", manifest, Participant::new("http-b", "client"))
            .await
            .unwrap();

        assert_eq!(first.session_id(), second.session_id());
    }
}
