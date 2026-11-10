//! HTTP/REST server implementation with SSE streaming
//!
//! Provides HTTP endpoints for pipeline execution:
//! - POST /execute - Unary execution
//! - POST /stream - Create streaming session
//! - POST /stream/:id/input - Send input to session
//! - GET /stream/:id/output - Receive outputs via SSE
//! - DELETE /stream/:id - Close session
//! - GET /health - Health check

mod output;
mod session;
use crate::body::JsonBody;
use crate::{
    codec,
    error::{Error, Result},
};
use async_trait::async_trait;
use axum::{
    extract::{Path, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use remotemedia_core::manifest::Manifest;
use remotemedia_core::transport::{
    Participant, PipelineExecutor, PipelineTransport, SharedPipelineStreamSession, StreamSession,
    TransportData,
};
use serde::{Deserialize, Serialize};
use session::{Guard, HttpSession};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// HTTP server state shared across handlers
#[derive(Clone)]
struct ServerState {
    /// Pipeline executor for executing pipelines (spec 026 migration)
    executor: Arc<PipelineExecutor>,
    copy_observer: Option<Arc<dyn Fn(&str, remotemedia_types::CopyContext) + Send + Sync>>,
    /// Active streaming sessions
    sessions: Arc<RwLock<HashMap<String, Arc<HttpSession>>>>,
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
            copy_observer: None,
            sessions: Arc::new(RwLock::new(HashMap::new())),
        };

        Ok(Self {
            bind_address,
            state,
        })
    }

    /// Observe each unary/streaming session's authority before bulk conversion.
    /// Retain the context to inspect reports after cleanup. The callback runs
    /// synchronously; the host controls report retention and should return promptly.
    pub fn with_copy_observer(
        mut self,
        observer: impl Fn(&str, remotemedia_types::CopyContext) + Send + Sync + 'static,
    ) -> Self {
        self.state.copy_observer = Some(Arc::new(observer));
        self
    }

    /// Retain an active HTTP session's exact core transport authority/report.
    pub async fn session_copy_context(&self, id: &str) -> Option<remotemedia_types::CopyContext> {
        self.state
            .sessions
            .read()
            .await
            .get(id)
            .map(|session| session.copies.clone())
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
    request: Request,
) -> std::result::Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let body = read_body(request).await?;
    let manifest = body.manifest().map_err(map_input_error)?;
    let session = state
        .executor
        .create_session(Arc::new(manifest))
        .await
        .map_err(map_runtime_error)?;
    let mut guard = Guard::new(session);
    let copies = guard.core().transport_copy_context();
    if let Some(observer) = &state.copy_observer {
        observer(&guard.core().session_id, copies.clone());
    }
    let outcome = async {
        let mut errors = session::subscribe_errors(&state.executor, guard.core())?;
        let request: ExecuteRequest = body
            .decode(&copies)
            .map_err(|error| remotemedia_core::Error::InvalidData(error.to_string()))?;
        guard.core().send_input(request.input).await?;
        let output = tokio::select! {
            biased;
            error = session::failure(&mut errors) => return Err(error),
            output = guard.core().recv_output() => output?,
        }
        .ok_or_else(|| codec::error("No output from pipeline"))?;
        let size = output.data.bulk_payload_bytes();
        codec::encode(&ExecuteResponse { output }, size, &copies)
    }
    .await;
    let cleanup = guard.close().await;
    let wire = outcome.map_err(map_runtime_error)?;
    cleanup.map_err(map_runtime_error)?;
    Ok((
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        wire,
    )
        .into_response())
}

fn map_input_error(error: remotemedia_core::Error) -> (StatusCode, Json<ErrorResponse>) {
    map_runtime_error(remotemedia_core::Error::InvalidData(error.to_string()))
}

async fn read_body(
    request: Request,
) -> std::result::Result<JsonBody, (StatusCode, Json<ErrorResponse>)> {
    let content_type = request
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim();
    if content_type != "application/json"
        && !(content_type.starts_with("application/") && content_type.ends_with("+json"))
    {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(ErrorResponse {
                error_type: "input".into(),
                message: "Expected application/json".into(),
                validation_errors: None,
            }),
        ));
    }
    // Preserve Axum Json's default 2 MiB request bound without coalescing chunks.
    JsonBody::collect(request.into_body().into_data_stream(), 2 * 1024 * 1024)
        .await
        .map_err(map_input_error)
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

    let handle = HttpSession::start(session, &state.executor).map_err(map_runtime_error)?;
    if let Some(observer) = &state.copy_observer {
        observer(&session_id, handle.copies.clone());
    }
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
    request: Request,
) -> std::result::Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let handle = find_session(&state, &session_id).await?;
    let input = handle.input().map_err(map_runtime_error)?;
    let body = read_body(request).await?;
    let request: StreamInputRequest = body.decode(&handle.copies).map_err(map_input_error)?;
    input.send(request.data).await.map_err(map_runtime_error)?;
    Ok(StatusCode::OK)
}

async fn find_session(
    state: &ServerState,
    id: &str,
) -> std::result::Result<Arc<HttpSession>, (StatusCode, Json<ErrorResponse>)> {
    state
        .sessions
        .read()
        .await
        .get(id)
        .cloned()
        .ok_or_else(|| not_found(id))
}

fn not_found(id: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error_type: "session".into(),
            message: format!("Session not found: {id}"),
            validation_errors: None,
        }),
    )
}

/// GET output subscribes without holding the session map during delivery.
async fn stream_output_handler(
    State(state): State<ServerState>,
    Path(session_id): Path<String>,
) -> std::result::Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let session = find_session(&state, &session_id).await?;
    Ok(output::response(&session))
}

/// Remove admission first, then signal and join output/core cleanup outside the map lock.
async fn close_stream_handler(
    State(state): State<ServerState>,
    Path(session_id): Path<String>,
) -> std::result::Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let handle = state
        .sessions
        .write()
        .await
        .remove(&session_id)
        .ok_or_else(|| not_found(&session_id))?;
    handle.close().await.map_err(map_runtime_error)?;
    Ok(StatusCode::OK)
}

#[cfg(test)]
mod baseline_tests {
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

#[cfg(test)]
mod tests;
