//! Bidirectional streaming RPC handler for StreamPipeline
//!
//! This module implements the StreamingPipelineService trait for real-time
//! chunk-by-chunk audio processing with <50ms latency per chunk.
//!
//! # Architecture
//!
//! - **StreamingServiceImpl**: Main service implementation with session management
//! - **StreamSession**: Per-session state (manifest, executor, metrics, sequence tracking)
//! - **stream_pipeline**: Bidirectional stream handler loop
//!
//! # Flow
//!
//! 1. Client sends StreamInit with manifest → Server responds with StreamReady
//! 2. Client sends AudioChunk messages → Server processes and returns ChunkResult
//! 3. Periodic StreamMetrics sent every 10 chunks
//! 4. Client sends StreamControl::CLOSE → Server flushes and sends StreamClosed
//!
//! # Performance
//!
//! - Target: <50ms average latency per chunk (User Story 3)
//! - Bounded buffer to prevent memory bloat
//! - Backpressure via STREAM_ERROR_BUFFER_OVERFLOW

// Internal infrastructure - some fields/methods for future use
#![allow(dead_code)]

use crate::generated::{
    stream_control::Command, stream_request::Request as StreamRequestType,
    stream_response::Response as StreamResponseType, ChunkResult, ErrorResponse, ErrorType,
    ExecutionMetrics, StreamClosed, StreamControl, StreamInit, StreamMetrics, StreamReady,
    StreamRequest, StreamResponse,
};
use crate::manifest_wire::{decode_manifest, PluginPolicy};
use crate::metrics::ServiceMetrics;
use crate::session_router::{DataPacket, SessionRouter};
use crate::ServiceError;
#[cfg(feature = "multiprocess")]
use remotemedia_core::nodes::python_streaming::PythonStreamingNode;
#[cfg(feature = "multiprocess")]
use remotemedia_core::python::multiprocess::MultiprocessExecutor;
use remotemedia_core::{
    data::RuntimeData,
    manifest::Manifest,
    nodes::{StreamingNode, StreamingNodeRegistry},
    transport::{
        session_control::{ControlAddress, SessionControl},
        PipelineExecutor,
    },
};
/// Python streaming nodes only exist with the `multiprocess` feature;
/// without it the node cache's Python slot is always `None`.
#[cfg(not(feature = "multiprocess"))]
type PythonStreamingNode = std::convert::Infallible;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Maximum number of chunks buffered before backpressure
const MAX_BUFFER_CHUNKS: usize = 10;

/// Maximum session idle time before timeout (seconds)
const SESSION_TIMEOUT_SECS: u64 = 300; // 5 minutes

/// Global node cache TTL (seconds) - how long to keep cached nodes after last use
const GLOBAL_NODE_CACHE_TTL_SECS: u64 = 600; // 10 minutes

/// Interval for cache cleanup checks (seconds)
const CACHE_CLEANUP_INTERVAL_SECS: u64 = 60; // 1 minute

/// Frequency of metrics updates (every N chunks)
const METRICS_UPDATE_INTERVAL: u64 = 10;

/// Optional `StreamPipeline` request header naming who opened the session
/// (for example `agent:claude-code`). It is recorded as the session's
/// discovery origin (`SessionSummary.origin`) so observers can tell agent-
/// created sessions apart. Values are trimmed, must be printable ASCII and at
/// most [`MAX_STREAM_ORIGIN_LEN`] bytes; anything else is ignored.
pub const STREAM_ORIGIN_HEADER: &str = "x-remotemedia-origin";

/// Upper bound on an accepted [`STREAM_ORIGIN_HEADER`] value.
pub const MAX_STREAM_ORIGIN_LEN: usize = 128;

/// The accepted origin label from a `StreamPipeline` request's metadata.
pub fn stream_origin(metadata: &tonic::metadata::MetadataMap) -> Option<String> {
    let v = metadata.get(STREAM_ORIGIN_HEADER)?.to_str().ok()?.trim();
    (!v.is_empty()
        && v.len() <= MAX_STREAM_ORIGIN_LEN
        && v.bytes().all(|b| b.is_ascii_graphic() || b == b' '))
    .then(|| v.to_string())
}

/// Node cache entry with timestamp for TTL management
struct CachedNode {
    node: Arc<Box<dyn StreamingNode>>,
    /// For Python streaming nodes, store the unwrapped instance to access process_streaming()
    py_streaming_node: Option<Arc<PythonStreamingNode>>,
    last_used: Instant,
}

/// Streaming pipeline service implementation
pub struct StreamingServiceImpl {
    /// Active streaming sessions (keyed by session_id)
    sessions: Arc<RwLock<HashMap<String, Arc<Mutex<StreamSession>>>>>,

    /// Authentication configuration
    auth_config: crate::auth::AuthConfig,

    /// Resource limits
    limits: crate::limits::ResourceLimits,

    /// Prometheus metrics
    metrics: Arc<ServiceMetrics>,

    /// Pipeline executor (encapsulates scheduler, node registry, and drift metrics)
    /// (Migrated from PipelineRunner per spec 026)
    executor: Arc<PipelineExecutor>,
    plugin_policy: PluginPolicy,

    /// Global node cache (shared across all sessions)
    /// Key: "{node_type}:{json_params_hash}", Value: cached node with timestamp
    global_node_cache: Arc<RwLock<HashMap<String, CachedNode>>>,

    /// Multiprocess executor for Python nodes (when multiprocess feature enabled)
    #[cfg(feature = "multiprocess")]
    multiprocess_executor: Option<Arc<MultiprocessExecutor>>,
}

impl StreamingServiceImpl {
    /// Create new streaming service instance
    pub fn new(
        auth_config: crate::auth::AuthConfig,
        limits: crate::limits::ResourceLimits,
        metrics: Arc<ServiceMetrics>,
        executor: Arc<PipelineExecutor>,
    ) -> Self {
        Self::new_with_policy(
            auth_config,
            limits,
            metrics,
            executor,
            PluginPolicy::permissive(),
        )
    }

    pub fn new_with_policy(
        auth_config: crate::auth::AuthConfig,
        limits: crate::limits::ResourceLimits,
        metrics: Arc<ServiceMetrics>,
        executor: Arc<PipelineExecutor>,
        plugin_policy: PluginPolicy,
    ) -> Self {
        let global_node_cache: Arc<RwLock<HashMap<String, CachedNode>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let cache_for_cleanup = global_node_cache.clone();
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(CACHE_CLEANUP_INTERVAL_SECS));
            loop {
                interval.tick().await;
                let mut cache = cache_for_cleanup.write().await;
                let before_count = cache.len();
                cache.retain(|key, cached_node| {
                    let age_secs = cached_node.last_used.elapsed().as_secs();
                    let keep = age_secs < GLOBAL_NODE_CACHE_TTL_SECS;
                    if !keep {
                        info!("🗑️ Expired cached node '{}' (idle for {}s)", key, age_secs);
                    }
                    keep
                });
                let removed_count = before_count - cache.len();
                if removed_count > 0 {
                    info!(
                        "🧹 Cache cleanup: removed {} expired nodes ({} remaining)",
                        removed_count,
                        cache.len()
                    );
                }
            }
        });

        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            auth_config,
            limits,
            metrics,
            executor,
            plugin_policy,
            global_node_cache,
            #[cfg(feature = "multiprocess")]
            multiprocess_executor: None,
        }
    }

    /// Set the multiprocess executor for Python node support
    #[cfg(feature = "multiprocess")]
    pub fn with_multiprocess_executor(mut self, executor: Arc<MultiprocessExecutor>) -> Self {
        self.multiprocess_executor = Some(executor);
        self
    }

    /// Get number of active sessions
    pub async fn active_session_count(&self) -> usize {
        self.sessions.read().await.len()
    }
}

/// Per-session state for streaming execution
pub(crate) struct StreamSession {
    /// Unique session identifier
    pub(crate) session_id: String,

    /// Parsed pipeline manifest
    pub(crate) manifest: Manifest,

    /// Guard keeping any inline plugin blobs (shipped via `embedded_plugins`)
    /// materialized on disk for the session lifetime. Held so the dlopened
    /// plugin stays valid across chunks.
    pub(crate) embedded_cas_guard: Option<crate::embedded_plugin::CasGuard>,

    /// Guard keeping a materialized embedded Python venv (shipped via
    /// `embedded_python_env`) on disk for the session lifetime.
    pub(crate) embedded_python_env_guard: Option<crate::embedded_plugin::PythonEnvGuard>,

    /// Expected next sequence number
    next_sequence: u64,

    /// Total chunks processed
    chunks_processed: u64,

    /// Total items processed (samples, frames, tokens, etc.)
    total_items: u64,

    /// Data type distribution
    data_type_counts: HashMap<String, u64>,

    /// Total chunks dropped (backpressure)
    chunks_dropped: u64,

    /// Cumulative processing time (milliseconds)
    cumulative_processing_time_ms: f64,

    /// Peak memory usage (bytes)
    peak_memory_bytes: u64,

    /// Current buffer occupancy (items)
    buffer_items: u64,

    /// Session creation time
    created_at: Instant,

    /// Last activity time (for timeout detection)
    last_activity: Instant,

    /// Recommended chunk size (samples)
    recommended_chunk_size: u64,

    /// Node cache: reuses initialized nodes across chunks
    /// Key: node_id, Value: cached StreamingNode instance
    /// This prevents expensive re-initialization (e.g., ML model loading)
    pub(crate) node_cache: HashMap<String, Arc<Box<dyn StreamingNode>>>,

    /// Cache hits for this session (Feature 005)
    pub(crate) cache_hits: u64,

    /// Cache misses for this session (Feature 005)
    pub(crate) cache_misses: u64,

    /// Bounded input sender to feed chunks to the session router.
    ///
    /// Sends use `.await` and block the gRPC handler when the pipeline is
    /// behind — this is the transport-level backpressure surface.
    pub(crate) router_input: Option<tokio::sync::mpsc::Sender<DataPacket>>,

    /// Session transport ledger for gRPC wire decode of client chunks.
    pub(crate) transport_copies: Option<remotemedia_core::data::CopyContext>,

    /// Router task handle
    pub(crate) router_task: Option<JoinHandle<()>>,

    /// Router shutdown signal sender
    pub(crate) router_shutdown: Option<tokio::sync::mpsc::Sender<()>>,
}

impl StreamSession {
    /// Create new session from StreamInit request
    pub(crate) fn new(
        session_id: String,
        manifest: Manifest,
        recommended_chunk_size: u64,
        embedded_cas_guard: Option<crate::embedded_plugin::CasGuard>,
        embedded_python_env_guard: Option<crate::embedded_plugin::PythonEnvGuard>,
    ) -> Self {
        let now = Instant::now();
        Self {
            session_id,
            manifest,
            embedded_cas_guard,
            embedded_python_env_guard,
            next_sequence: 0,
            chunks_processed: 0,
            total_items: 0,
            data_type_counts: HashMap::new(),
            chunks_dropped: 0,
            cumulative_processing_time_ms: 0.0,
            peak_memory_bytes: 0,
            buffer_items: 0,
            created_at: now,
            last_activity: now,
            recommended_chunk_size,
            node_cache: HashMap::new(),
            cache_hits: 0,
            cache_misses: 0,
            router_input: None,
            transport_copies: None,
            router_task: None,
            router_shutdown: None,
        }
    }

    /// Update last activity timestamp
    fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    /// Check if session has timed out
    fn is_timed_out(&self) -> bool {
        self.last_activity.elapsed().as_secs() > SESSION_TIMEOUT_SECS
    }

    /// Get number of cached nodes
    fn cached_nodes_count(&self) -> usize {
        self.node_cache.len()
    }

    /// Clear node cache (called on session cleanup)
    fn clear_node_cache(&mut self) {
        let count = self.node_cache.len();
        self.node_cache.clear();
        if count > 0 {
            info!(
                "🗑️ Cleared {} cached nodes for session {}",
                count, self.session_id
            );
        }
    }

    /// Shutdown the router and all node processing
    async fn shutdown_router(&mut self) {
        info!(
            "[ROUTER-SHUTDOWN] Starting shutdown for session '{}'",
            self.session_id
        );

        // Drop the input sender to close the router's input channel
        info!("[ROUTER-SHUTDOWN] Dropping router input channel...");
        self.router_input.take();

        // Send shutdown signal to router
        if let Some(shutdown_tx) = self.router_shutdown.take() {
            info!("[ROUTER-SHUTDOWN] Sending shutdown signal to router...");
            let _ = shutdown_tx.send(()).await;
            info!("[ROUTER-SHUTDOWN] Shutdown signal sent");
        } else {
            info!("[ROUTER-SHUTDOWN] No shutdown channel available");
        }

        // Wait for router task to complete
        if let Some(task) = self.router_task.take() {
            info!("[ROUTER-SHUTDOWN] Waiting for router task to complete...");
            match tokio::time::timeout(std::time::Duration::from_millis(500), task).await {
                Ok(Ok(_)) => info!(
                    "[ROUTER-SHUTDOWN] ✅ Router task completed for session '{}'",
                    self.session_id
                ),
                Ok(Err(e)) => error!(
                    "[ROUTER-SHUTDOWN] Router task failed for session '{}': {}",
                    self.session_id, e
                ),
                Err(_) => warn!(
                    "[ROUTER-SHUTDOWN] ⏱️ Router task timeout for session '{}', continuing anyway",
                    self.session_id
                ),
            }
        } else {
            info!("[ROUTER-SHUTDOWN] No router task to wait for");
        }

        info!(
            "[ROUTER-SHUTDOWN] Shutdown complete for session '{}'",
            self.session_id
        );
    }

    /// Validate sequence number (detect gaps or out-of-order)
    fn validate_sequence(&mut self, sequence: u64) -> Result<(), ServiceError> {
        if sequence < self.next_sequence {
            return Err(ServiceError::Validation(format!(
                "Out-of-order chunk: expected sequence {}, got {}",
                self.next_sequence, sequence
            )));
        }

        if sequence > self.next_sequence {
            let gap = sequence - self.next_sequence;
            warn!(
                session_id = %self.session_id,
                expected = self.next_sequence,
                received = sequence,
                gap = gap,
                "Missing chunks detected"
            );
            // For now, accept the chunk but log the gap
            // In production, might want to return STREAM_ERROR_INVALID_SEQUENCE
        }

        self.next_sequence = sequence + 1;
        Ok(())
    }

    /// Record processing metrics for a chunk
    fn record_chunk_metrics(
        &mut self,
        processing_time_ms: f64,
        items: u64,
        memory_bytes: u64,
        data_type: &str,
    ) {
        self.chunks_processed += 1;
        self.total_items += items;
        self.cumulative_processing_time_ms += processing_time_ms;

        // Update data type breakdown
        *self
            .data_type_counts
            .entry(data_type.to_string())
            .or_insert(0) += 1;

        if memory_bytes > self.peak_memory_bytes {
            self.peak_memory_bytes = memory_bytes;
        }

        self.touch();
    }

    /// Calculate average latency across all processed chunks
    fn average_latency_ms(&self) -> f64 {
        if self.chunks_processed == 0 {
            0.0
        } else {
            self.cumulative_processing_time_ms / self.chunks_processed as f64
        }
    }

    /// Generate StreamMetrics message
    fn create_metrics(&self, cached_nodes_count: u64) -> StreamMetrics {
        let cache_total = self.cache_hits + self.cache_misses;
        let cache_hit_rate = if cache_total > 0 {
            self.cache_hits as f64 / cache_total as f64
        } else {
            0.0
        };

        StreamMetrics {
            session_id: self.session_id.clone(),
            chunks_processed: self.chunks_processed,
            average_latency_ms: self.average_latency_ms(),
            total_items: self.total_items,
            buffer_items: self.buffer_items,
            chunks_dropped: self.chunks_dropped,
            peak_memory_bytes: self.peak_memory_bytes,
            data_type_breakdown: self.data_type_counts.clone(),
            cache_hits: self.cache_hits,
            cache_misses: self.cache_misses,
            cached_nodes_count,
            cache_hit_rate,
        }
    }

    /// Generate final ExecutionMetrics for StreamClosed
    fn create_final_metrics(&self) -> ExecutionMetrics {
        ExecutionMetrics {
            wall_time_ms: self.created_at.elapsed().as_secs_f64() * 1000.0,
            cpu_time_ms: self.cumulative_processing_time_ms, // Approximate
            memory_used_bytes: self.peak_memory_bytes,
            node_metrics: HashMap::new(), // TODO: Populate from executor
            serialization_time_ms: 0.0,   // Not tracked for streaming
            proto_to_runtime_ms: 0.0,     // Not tracked yet
            runtime_to_proto_ms: 0.0,     // Not tracked yet
            data_type_breakdown: self.data_type_counts.clone(),
        }
    }
}

#[tonic::async_trait]
impl crate::StreamingPipelineService for StreamingServiceImpl {
    type StreamPipelineStream =
        tokio_stream::wrappers::ReceiverStream<Result<StreamResponse, Status>>;

    async fn stream_pipeline(
        &self,
        request: Request<Streaming<StreamRequest>>,
    ) -> Result<Response<Self::StreamPipelineStream>, Status> {
        info!("StreamPipeline RPC invoked");

        // Preview feature header validation (from initial request metadata)
        // Note: Feature flag validation removed - configure via ServiceConfig if needed
        if let Some(_hdr_val) = request.metadata().get("x-preview-features") {
            // Preview features can be enabled/disabled via ServiceConfig
            // For now, we allow all preview features
            info!("Preview features requested (validation skipped)");
        }

        let origin = stream_origin(request.metadata());
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let mut stream = request.into_inner();
        let sessions = self.sessions.clone();
        let metrics = self.metrics.clone();
        #[cfg(feature = "multiprocess")]
        let multiprocess_executor = self.multiprocess_executor.clone();

        let executor = self.executor.clone();
        let plugin_policy = self.plugin_policy.clone();
        let global_node_cache = self.global_node_cache.clone();

        // Spawn async task to handle bidirectional streaming
        tokio::spawn(async move {
            #[cfg(feature = "multiprocess")]
            let result = handle_stream(
                &mut stream,
                tx.clone(),
                sessions,
                metrics,
                executor,
                plugin_policy,
                global_node_cache,
                origin,
                multiprocess_executor,
            )
            .await;

            #[cfg(not(feature = "multiprocess"))]
            let result = handle_stream(
                &mut stream,
                tx.clone(),
                sessions,
                metrics,
                executor,
                plugin_policy,
                global_node_cache,
                origin,
            )
            .await;

            if let Err(e) = result {
                error!(error = %e, "Stream handling error");
                let error_response = ErrorResponse {
                    error_type: ErrorType::Internal as i32,
                    message: e.to_string(),
                    failing_node_id: String::new(),
                    context: String::new(),
                    stack_trace: String::new(),
                };
                let response = StreamResponse {
                    response: Some(StreamResponseType::Error(error_response)),
                };
                let _ = tx.send(Ok(response)).await;
            }
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}

/// Forward the session control bus's injected packets into a gRPC
/// [`SessionRouter`]'s input. Ends when the router input closes or the
/// control (and its sender) is dropped.
async fn attach_control_input(
    control: &Arc<SessionControl>,
    router_input: tokio::sync::mpsc::Sender<DataPacket>,
) {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<remotemedia_core::transport::DataPacket>(64);
    control.attach_input_sender(tx).await;
    tokio::spawn(async move {
        while let Some(p) = rx.recv().await {
            let packet = DataPacket {
                data: p.data,
                from_node: p.from_node,
                to_node: p.to_node,
                session_id: p.session_id,
                sequence: p.sequence,
                sub_sequence: p.sub_sequence,
            };
            if router_input.send(packet).await.is_err() {
                break;
            }
        }
    });
}

/// Handle bidirectional stream (runs in async task)
///
/// Note: Audio chunks are now processed through the session router like data chunks,
/// rather than using the legacy execute_fast_pipeline path. This aligns with spec 026
/// PipelineExecutor migration.
async fn handle_stream(
    stream: &mut Streaming<StreamRequest>,
    tx: tokio::sync::mpsc::Sender<Result<StreamResponse, Status>>,
    sessions: Arc<RwLock<HashMap<String, Arc<Mutex<StreamSession>>>>>,
    metrics: Arc<ServiceMetrics>,
    executor: Arc<PipelineExecutor>,
    plugin_policy: PluginPolicy,
    global_node_cache: Arc<RwLock<HashMap<String, CachedNode>>>,
    origin: Option<String>,
    #[cfg(feature = "multiprocess")] multiprocess_executor: Option<Arc<MultiprocessExecutor>>,
) -> Result<(), ServiceError> {
    let mut session: Option<Arc<Mutex<StreamSession>>> = None;
    let mut session_id = String::new();

    // Main stream loop. Its errors (including a client that vanishes
    // mid-stream) still run the cleanup below, so the session never
    // outlives its stream on the control bus.
    let result: Result<(), ServiceError> = async {
        while let Some(request_result) = stream
            .message()
            .await
            .map_err(|e| ServiceError::Internal(format!("Stream receive error: {}", e)))?
        {
            match request_result.request {
                Some(StreamRequestType::Init(init)) => {
                    // Handle StreamInit (must be first message)
                    if session.is_some() {
                        return Err(ServiceError::Validation(
                            "StreamInit already received".to_string(),
                        ));
                    }

                    debug!("Processing StreamInit");
                    if !init.shared_session_key.is_empty() {
                        // The rest of this stream belongs to a shared session.
                        return run_shared_stream(init, stream, tx.clone(), &executor, &plugin_policy, &metrics)
                            .await;
                    }
                    let output_taps = validate_output_taps(&init.output_taps)?;
                    let (new_session_id, ready) =
                        handle_stream_init(init, &sessions, &executor, &plugin_policy).await?;
                    session_id = new_session_id.clone();
                    session = Some(sessions.read().await.get(&session_id).unwrap().clone());

                    // Manifest-declared plugins are resolved before taking the
                    // per-session registry snapshot used by SessionRouter.
                    let streaming_registry = {
                        let registry = executor.registry();
                        let guard = registry.read().await;
                        Arc::new(guard.clone())
                    };

                    // Create and start the SessionRouter for this session
                    let sess = session.as_ref().unwrap();

                    // Create the session router with graph validation (spec 021)
                    // This validates the pipeline graph (cycles, missing nodes) before streaming starts
                    let (mut router, shutdown_tx) = SessionRouter::new(
                        session_id.clone(),
                        streaming_registry.clone(),
                        sess.clone(),
                        tx.clone(),
                    )
                    .await
                    .map_err(|e| {
                        error!("Failed to create session router: {}", e);
                        ServiceError::Validation(format!("Pipeline graph validation failed: {}", e))
                    })?;

                    // Set multiprocess executor if available
                    #[cfg(feature = "multiprocess")]
                    if let Some(ref mp_executor) = multiprocess_executor {
                        router.set_multiprocess_executor(mp_executor.clone());
                    }

                    // This streaming path constructs SessionRouter directly rather
                    // than through PipelineExecutor::cold_build_session, so it must
                    // install the per-session control bus itself. Do this before
                    // node initialization: source-Python nodes can publish blips
                    // and progress while initialize() is still running, and output
                    // taps must be attachable as soon as StreamReady is emitted.
                    // The control bus shares the router's transport ledger, so wire
                    // encode/decode on taps, publish and intercepts is attributed.
                    let control = SessionControl::with_transport_copy_context(
                        session_id.clone(),
                        router.transport_copy_context(),
                    );
                    router.attach_control(control.clone());
                    // Discovery metadata (openspec: add-live-pipeline-observer);
                    // the router attached its descriptor + telemetry above.
                    // [`STREAM_ORIGIN_HEADER`] labels who opened it (e.g. an agent).
                    control.set_transport_info("grpc", origin.clone());
                    // Control-plane injection (`publish` to `<node>.in[.<port>]`,
                    // typed RPC to nodes without a dedicated lane) reaches this
                    // router through a bridge: the bus speaks core `DataPacket`s,
                    // this router its own.
                    attach_control_input(&control, router.get_input_sender()).await;
                    executor.control_bus().register(control.clone());

                    // 🔥 Pre-initialize all nodes before streaming starts
                    // CRITICAL: Do this WITHOUT holding the session lock to avoid deadlock
                    // (get_or_create_node needs to acquire the lock)
                    info!("🔥 Pre-initializing nodes for session '{}'", session_id);
                    router.pre_initialize_all_nodes().await.map_err(|e| {
                        executor
                            .control_bus()
                            .unregister_with(&session_id, Some(e.to_string()));
                        error!("Failed to pre-initialize nodes: {}", e);
                        ServiceError::Internal(format!("Node pre-initialization failed: {}", e))
                    })?;
                    control.set_phase(remotemedia_core::transport::session_control::SessionPhase::Active);
                    info!("✅ All nodes ready, starting router");

                    // Get the input sender before starting
                    let input_sender = router.get_input_sender();

                    // Start the router task
                    let task = router.start();

                    // Now acquire lock to store router state
                    {
                        let mut sess_guard = sess.lock().await;
                        sess_guard.router_task = Some(task);

                        // Store the input sender for feeding chunks
                        sess_guard.router_input = Some(input_sender);
                        sess_guard.transport_copies = Some(control.transport_copy_context());

                        // Store the shutdown sender for cleanup
                        sess_guard.router_shutdown = Some(shutdown_tx);

                        info!("🚀 SessionRouter started for session '{}'", session_id);
                    }

                    // Record metrics
                    metrics.record_stream_start();

                    // Send StreamReady response
                    let response = StreamResponse {
                        response: Some(StreamResponseType::Ready(ready)),
                    };
                    tx.send(Ok(response)).await.map_err(|_| {
                        ServiceError::Internal("Failed to send StreamReady".to_string())
                    })?;

                    if !output_taps.is_empty() {
                        let control = executor.control_bus().get(&session_id).ok_or_else(|| {
                            ServiceError::Internal(format!(
                                "session control unavailable for '{}'",
                                session_id
                            ))
                        })?;
                        for tap in output_taps {
                            let tap_tx = tx.clone();
                            let tap_copies = control.transport_copy_context();
                            let output_key = format!("__tap__.{tap}");
                            if tap == "__system__" {
                                let mut receiver = control.subscribe_system();
                                tokio::spawn(async move {
                                    while let Some(data) = receiver.recv().await {
                                        if send_tap_result(&tap_tx, &output_key, data, &tap_copies).await.is_err() {
                                            break;
                                        }
                                    }
                                });
                            } else {
                                let mut receiver = control
                                    .subscribe(&ControlAddress::node_out(tap.clone()))
                                    .map_err(|error| {
                                        ServiceError::Validation(format!(
                                            "could not subscribe output tap '{tap}': {error}"
                                        ))
                                    })?;
                                tokio::spawn(async move {
                                    loop {
                                        match receiver.recv().await {
                                            Ok(data) => {
                                                if send_tap_result(&tap_tx, &output_key, data, &tap_copies)
                                                    .await
                                                    .is_err()
                                                {
                                                    break;
                                                }
                                            }
                                            Err(tokio::sync::broadcast::error::RecvError::Lagged(
                                                count,
                                            )) => {
                                                warn!(tap = %output_key, dropped = count, "gRPC output tap lagged");
                                            }
                                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                                break
                                            }
                                        }
                                    }
                                });
                            }
                        }
                    }
                }

                Some(StreamRequestType::DataChunk(data_chunk)) => {
                    // Handle DataChunk (generic streaming)
                    let sess = session.as_ref().ok_or_else(|| {
                        ServiceError::Validation("StreamInit required before DataChunk".to_string())
                    })?;

                    let chunk_start = Instant::now();
                    debug!(sequence = data_chunk.sequence, "Processing DataChunk");

                    // Feed the chunk to the session router. Clone the sender and
                    // release the session lock before the (bounded, awaited) send:
                    // the router takes this lock to lazily create node tasks, so
                    // holding it across a full-queue send deadlocks the session.
                    let (router_input, transport_copies) = {
                        let guard = sess.lock().await;
                        (guard.router_input.clone(), guard.transport_copies.clone())
                    };
                    if let Some(router_input) = router_input {
                        // Convert DataBuffer to RuntimeData with arrival timestamp (spec 026).
                        // The chunk owns its buffer: bulk bytes move into shared storage
                        // and audio conversion is charged to the session ledger.
                        use crate::adapters::{data_buffer_into_runtime_data_with_context, now_micros};
                        let arrival_ts = Some(now_micros());
                        let copies = transport_copies.ok_or_else(|| {
                            ServiceError::Internal("session transport ledger unavailable".to_string())
                        })?;
                        let decode = |buffer| {
                            data_buffer_into_runtime_data_with_context(buffer, arrival_ts, &copies)
                                .map_err(|rejected| ServiceError::Validation(rejected.to_string()))?
                                .ok_or_else(|| {
                                    ServiceError::Validation("Data conversion failed".to_string())
                                })
                        };

                        let runtime_data = if let Some(buffer) = data_chunk.buffer {
                            decode(buffer)?
                        } else if !data_chunk.named_buffers.is_empty() {
                            // For multi-input, just use the first buffer for now
                            let (_, buffer) =
                                data_chunk.named_buffers.into_iter().next().ok_or_else(|| {
                                    ServiceError::Validation("No input data provided".to_string())
                                })?;
                            decode(buffer)?
                        } else {
                            return Err(ServiceError::Validation(
                                "DataChunk must have buffer or named_buffers".to_string(),
                            ));
                        };

                        // Create DataPacket - the data should be sent TO the node specified in data_chunk.node_id
                        // not FROM it. We use "client" as the source since this is input from the client.
                        let packet = DataPacket {
                            data: runtime_data,
                            from_node: "client".to_string(), // Data comes from client
                            to_node: Some(data_chunk.node_id.clone()), // Send TO this node for processing
                            session_id: session_id.clone(),
                            sequence: data_chunk.sequence,
                            sub_sequence: 0,
                        };

                        // Bounded router-input: .await applies real backpressure.
                        router_input.send(packet).await.map_err(|e| {
                            ServiceError::Internal(format!("Failed to send to router: {}", e))
                        })?;

                        sess.lock().await.chunks_processed += 1;

                        let latency = chunk_start.elapsed().as_secs_f64();
                        metrics.record_chunk_processed(&session_id, latency);

                        debug!("Fed DataChunk to session router");

                        // Send periodic metrics
                        let sess_lock = sess.lock().await;
                        if sess_lock.chunks_processed % METRICS_UPDATE_INTERVAL == 0 {
                            let cached_nodes_count = global_node_cache.read().await.len() as u64;
                            let stream_metrics = sess_lock.create_metrics(cached_nodes_count);
                            drop(sess_lock);

                            let metrics_response = StreamResponse {
                                response: Some(StreamResponseType::Metrics(stream_metrics)),
                            };
                            tx.send(Ok(metrics_response)).await.map_err(|_| {
                                ServiceError::Internal("Failed to send StreamMetrics".to_string())
                            })?;
                        }
                    } else {
                        return Err(ServiceError::Internal(
                            "Session router not initialized".to_string(),
                        ));
                    }
                }

                Some(StreamRequestType::Control(control)) => {
                    // Handle StreamControl (CLOSE or CANCEL)
                    let sess = session.as_ref().ok_or_else(|| {
                        ServiceError::Validation("StreamInit required before StreamControl".to_string())
                    })?;

                    debug!(command = control.command, "Processing StreamControl");
                    let closed = handle_stream_control(control, sess.clone()).await?;

                    // Send StreamClosed response
                    let response = StreamResponse {
                        response: Some(StreamResponseType::Closed(closed)),
                    };
                    tx.send(Ok(response)).await.map_err(|_| {
                        ServiceError::Internal("Failed to send StreamClosed".to_string())
                    })?;

                    // Cleanup session and metrics
                    if let Some(session_arc) = sessions.write().await.remove(&session_id) {
                        // Shutdown router and all node processing
                        let mut sess_guard = session_arc.lock().await;
                        sess_guard.shutdown_router().await;
                        sess_guard.clear_node_cache();
                    }
                    metrics.record_stream_end();
                    info!(session_id = %session_id, "Session closed");
                    break; // Exit stream loop
                }

                None => {
                    warn!("Received empty StreamRequest");
                }
            }
        }
        Ok(())
    }
    .await;

    // If we exit loop without explicit close (or on error), cleanup
    if !session_id.is_empty() {
        if let Some(session_arc) = sessions.write().await.remove(&session_id) {
            // Shutdown router and all node processing
            let mut sess_guard = session_arc.lock().await;
            sess_guard.shutdown_router().await;
            sess_guard.clear_node_cache();
        }
        executor
            .control_bus()
            .unregister_with(&session_id, result.as_ref().err().map(|e| e.to_string()));
        metrics.record_stream_end();
        info!(session_id = %session_id, "Session disconnected");
    }

    result
}

/// Streams currently attached to each shared session key, so the last one to
/// leave closes the session.
fn shared_stream_clients() -> &'static std::sync::Mutex<HashMap<String, usize>> {
    static CLIENTS: std::sync::OnceLock<std::sync::Mutex<HashMap<String, usize>>> =
        std::sync::OnceLock::new();
    CLIENTS.get_or_init(Default::default)
}

/// Output key of shared-stream results: core sessions do not name the sink
/// that produced an output.
pub const SHARED_STREAM_OUTPUT_KEY: &str = "output";

/// `StreamPipeline` on a shared session (openspec:
/// add-multi-participant-sessions): join the core shared session for
/// `init.shared_session_key` as `init.participant`, feed this stream's chunks
/// in as that participant, and stream every session output back with its
/// `participant.*` attribution in `DataBuffer.metadata`. Leaves the session
/// when the stream ends; the last stream on a key closes the session.
async fn run_shared_stream(
    init: StreamInit,
    stream: &mut Streaming<StreamRequest>,
    tx: tokio::sync::mpsc::Sender<Result<StreamResponse, Status>>,
    executor: &Arc<PipelineExecutor>,
    plugin_policy: &PluginPolicy,
    metrics: &Arc<ServiceMetrics>,
) -> Result<(), ServiceError> {
    use crate::adapters::{data_buffer_into_transport_data_with_context, participant_metadata};
    use remotemedia_core::transport::{data::participant, Participant};

    if init.client_version.is_empty() {
        return Err(ServiceError::Validation("client_version required".to_string()));
    }
    let identity = init.participant.as_ref().map(participant_metadata).unwrap_or_default();
    let Some(participant_id) = identity.get(participant::ID).cloned() else {
        return Err(ServiceError::Validation(
            "participant.id required with shared_session_key".to_string(),
        ));
    };
    if !init.embedded_plugins.is_empty() || init.embedded_python_env.is_some() {
        return Err(ServiceError::Validation(
            "embedded plugins and Python environments are not supported on shared sessions".to_string(),
        ));
    }
    if !init.output_taps.is_empty() {
        return Err(ServiceError::Validation(
            "output_taps are not supported on shared sessions; attach to the control bus instead".to_string(),
        ));
    }
    let manifest_proto = init
        .manifest
        .ok_or_else(|| ServiceError::Validation("manifest required in StreamInit".to_string()))?;
    let manifest = decode_manifest(&manifest_proto, plugin_policy)?;
    executor
        .ensure_plugins_loaded(&manifest)
        .await
        .map_err(|error| ServiceError::Validation(error.to_string()))?;

    let key = format!("grpc:{}", init.shared_session_key);
    let shared = executor
        .get_or_create_shared_session(key.clone(), Arc::new(manifest))
        .await
        .map_err(|error| ServiceError::Internal(format!("shared session: {error}")))?;
    *shared_stream_clients().lock().unwrap().entry(key.clone()).or_default() += 1;
    let info = init.participant.unwrap_or_default();
    let mut who = Participant::new(participant_id.clone(), if info.role.is_empty() { participant::role::CLIENT.to_string() } else { info.role.clone() });
    if !info.track_id.is_empty() {
        who = who.with_track_id(info.track_id.clone());
    }
    if !info.modality.is_empty() {
        who = who.with_modality(info.modality.clone());
    }
    if !info.display_name.is_empty() {
        who = who.with_display_name(info.display_name.clone());
    }
    let handle = shared.participant_handle(who);
    let copies = shared.transport_copy_context().await;
    info!(
        session_id = %shared.session_id(),
        key = %init.shared_session_key,
        participant = %participant_id,
        "gRPC stream joined a shared session"
    );

    // Every output of the session goes to every stream on the key.
    let (audio_rx, video_rx, data_rx) = shared.subscribe_outputs().split();
    let mut forwarders = Vec::new();
    for mut rx in [audio_rx, video_rx, data_rx] {
        let tx = tx.clone();
        let copies = copies.clone();
        forwarders.push(tokio::spawn(async move {
            loop {
                let output = match rx.recv().await {
                    Ok(output) => output,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "shared gRPC stream lagged; outputs dropped");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                };
                // The output's metadata (participant.*) rides in DataBuffer.metadata.
                let buffer = match crate::adapters::transport_data_to_data_buffer_with_context(&output, &copies) {
                    Ok(buffer) => buffer,
                    Err(error) => {
                        warn!(%error, "shared gRPC output not encoded");
                        continue;
                    }
                };
                let mut data_outputs = HashMap::new();
                data_outputs.insert(SHARED_STREAM_OUTPUT_KEY.to_string(), buffer);
                let response = StreamResponse {
                    response: Some(StreamResponseType::Result(ChunkResult {
                        sequence: output.sequence.unwrap_or(0),
                        data_outputs,
                        processing_time_ms: 0.0,
                        total_items_processed: 0,
                    })),
                };
                if tx.send(Ok(response)).await.is_err() {
                    break;
                }
            }
        }));
    }

    metrics.record_stream_start();
    tx.send(Ok(StreamResponse {
        response: Some(StreamResponseType::Ready(StreamReady {
            session_id: shared.session_id().to_string(),
            recommended_chunk_size: if init.expected_chunk_size > 0 { init.expected_chunk_size } else { 4096 },
            max_buffer_latency_ms: 100,
        })),
    }))
    .await
    .map_err(|_| ServiceError::Internal("Failed to send StreamReady".to_string()))?;

    let result: Result<(), ServiceError> = async {
        while let Some(request) = stream
            .message()
            .await
            .map_err(|e| ServiceError::Internal(format!("Stream receive error: {}", e)))?
        {
            match request.request {
                Some(StreamRequestType::DataChunk(chunk)) => {
                    let buffer = match (chunk.buffer, chunk.named_buffers.into_iter().next()) {
                        (Some(buffer), _) => buffer,
                        (None, Some((_, buffer))) => buffer,
                        (None, None) => {
                            return Err(ServiceError::Validation(
                                "DataChunk must have buffer or named_buffers".to_string(),
                            ))
                        }
                    };
                    let mut input = data_buffer_into_transport_data_with_context(buffer, &copies)
                        .map_err(|rejected| ServiceError::Validation(rejected.to_string()))?
                        .ok_or_else(|| ServiceError::Validation("Data conversion failed".to_string()))?;
                    input.sequence = Some(chunk.sequence);
                    handle
                        .send(input)
                        .await
                        .map_err(|e| ServiceError::Internal(format!("shared session input: {e}")))?;
                }
                Some(StreamRequestType::Control(_)) => {
                    tx.send(Ok(StreamResponse {
                        response: Some(StreamResponseType::Closed(StreamClosed {
                            session_id: handle.session_id().to_string(),
                            final_metrics: None,
                            reason: "client closed".to_string(),
                        })),
                    }))
                    .await
                    .map_err(|_| ServiceError::Internal("Failed to send StreamClosed".to_string()))?;
                    break;
                }
                Some(StreamRequestType::Init(_)) => {
                    return Err(ServiceError::Validation("StreamInit already received".to_string()))
                }
                None => warn!("Received empty StreamRequest"),
            }
        }
        Ok(())
    }
    .await;

    // This participant leaves; its per-participant node instances stop.
    let _ = handle.leave().await;
    for forwarder in forwarders {
        forwarder.abort();
    }
    let last = {
        let mut clients = shared_stream_clients().lock().unwrap();
        let count = clients.entry(key.clone()).or_default();
        *count = count.saturating_sub(1);
        let last = *count == 0;
        if last {
            clients.remove(&key);
        }
        last
    };
    if last {
        if let Err(error) = shared.close().await {
            warn!(key = %key, "closing shared session: {error}");
        }
    }
    metrics.record_stream_end();
    info!(participant = %participant_id, "gRPC stream left its shared session");
    result
}

fn validate_output_taps(taps: &[String]) -> Result<Vec<String>, ServiceError> {
    if taps.len() > 16 {
        return Err(ServiceError::Validation(
            "at most 16 output_taps may be requested".to_string(),
        ));
    }
    let mut unique = Vec::new();
    for tap in taps {
        let tap = tap.trim();
        if tap.is_empty()
            || tap.len() > 128
            || !tap
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        {
            return Err(ServiceError::Validation(format!(
                "invalid output tap '{tap}'"
            )));
        }
        if !unique.iter().any(|existing| existing == tap) {
            unique.push(tap.to_string());
        }
    }
    Ok(unique)
}

async fn send_tap_result(
    tx: &tokio::sync::mpsc::Sender<Result<StreamResponse, Status>>,
    output_key: &str,
    data: RuntimeData,
    copies: &remotemedia_core::data::CopyContext,
) -> Result<(), ()> {
    let mut outputs = HashMap::new();
    let buffer = match crate::adapters::runtime_data_to_data_buffer_with_context(&data, copies) {
        Ok(buffer) => buffer,
        Err(rejected) => {
            // Strict policy refuses the copying wire encode; skip this event.
            warn!(tap = %output_key, error = %rejected, "gRPC output tap not encoded");
            return Ok(());
        }
    };
    outputs.insert(output_key.to_string(), buffer);
    tx.send(Ok(StreamResponse {
        response: Some(StreamResponseType::Result(ChunkResult {
            sequence: 0,
            data_outputs: outputs,
            processing_time_ms: 0.0,
            total_items_processed: 0,
        })),
    }))
    .await
    .map_err(|_| ())
}

/// Handle StreamInit message
async fn handle_stream_init(
    init: StreamInit,
    sessions: &Arc<RwLock<HashMap<String, Arc<Mutex<StreamSession>>>>>,
    executor: &Arc<PipelineExecutor>,
    plugin_policy: &PluginPolicy,
) -> Result<(String, StreamReady), ServiceError> {
    // Validate client version (basic check)
    if init.client_version.is_empty() {
        return Err(ServiceError::Validation(
            "client_version required".to_string(),
        ));
    }

    // Deserialize manifest
    let manifest_proto = init
        .manifest
        .ok_or_else(|| ServiceError::Validation("manifest required in StreamInit".to_string()))?;

    let mut manifest = decode_manifest(&manifest_proto, plugin_policy)?;

    // Materialize any inline plugin blobs shipped with the init (portable
    // pipeline bundles) into a content-addressed temp dir and rewrite
    // `embedded:<digest>` plugin specs, so the executor can dlopen the plugin
    // without a prior deploy. Held for the session lifetime.
    let embedded_cas_guard = if !init.embedded_plugins.is_empty() {
        Some(
            crate::embedded_plugin::materialize_embedded_plugins(
                &init.embedded_plugins,
                &mut manifest,
            )
            .map_err(|e| ServiceError::Validation(e))?,
        )
    } else {
        None
    };

    // Materialize an embedded (frozen) Python wheelhouse shipped inline with
    // the init into a venv and record it on the manifest, so Python nodes run
    // against the prebuilt env (no network, no resolution). Held for the
    // session lifetime.
    let embedded_python_env_guard = if let Some(py) = init.embedded_python_env.as_ref() {
        let decoded = crate::embedded_plugin::decode_embedded_python_env(py);
        let (wheels_dir, guard) =
            crate::embedded_plugin::materialize_embedded_python_env(&decoded)
                .map_err(ServiceError::Validation)?;
        // The guard wires `UV_FIND_LINKS` for the session lifetime and restores
        // the previous value when the session (and guard) is dropped.
        let python_version =
            Some(decoded.interpreter.version.clone()).filter(|v| !v.trim().is_empty());
        manifest.python_env = Some(remotemedia_core::manifest::ManifestPythonEnv {
            python_version,
            scope: Some(remotemedia_core::python::env_manager::EnvScope::PerPipeline),
            find_links: vec![wheels_dir],
            ..Default::default()
        });
        Some(guard)
    } else {
        None
    };

    executor
        .ensure_plugins_loaded(&manifest)
        .await
        .map_err(|error| ServiceError::Validation(error.to_string()))?;

    // Generate unique session ID
    let session_id = Uuid::new_v4().to_string();

    // Determine recommended chunk size (use client's suggestion or default)
    let recommended_chunk_size = if init.expected_chunk_size > 0 {
        init.expected_chunk_size
    } else {
        4096 // Default: 4096 samples (~256ms at 16kHz)
    };

    // Create session
    let session = Arc::new(Mutex::new(StreamSession::new(
        session_id.clone(),
        manifest,
        recommended_chunk_size,
        embedded_cas_guard,
        embedded_python_env_guard,
    )));

    // Store session
    sessions.write().await.insert(session_id.clone(), session);

    info!(
        session_id = %session_id,
        chunk_size = recommended_chunk_size,
        "StreamSession created"
    );

    // Return StreamReady
    let ready = StreamReady {
        session_id: session_id.clone(),
        recommended_chunk_size,
        max_buffer_latency_ms: 100, // 100ms max buffer latency
    };

    Ok((session_id, ready))
}

/// Recursively route output data through the pipeline
async fn route_to_downstream(
    output_data: RuntimeData,
    from_node_id: String,
    session: Arc<Mutex<StreamSession>>,
    streaming_registry: Arc<StreamingNodeRegistry>,
    tx: tokio::sync::mpsc::Sender<Result<StreamResponse, Status>>,
    session_id: String,
    base_sequence: u64,
) -> Result<(), ServiceError> {
    // USE THE NEW ASYNC ROUTER FOR TRUE STREAMING
    use crate::async_router::route_to_downstream_async;

    return route_to_downstream_async(
        output_data,
        from_node_id,
        session,
        streaming_registry,
        tx,
        session_id,
        base_sequence,
    )
    .await
    .map_err(|e| ServiceError::Internal(e.to_string()));
}

/// Create and initialize a Python streaming node, returning the boxed
/// streaming node plus the unwrapped instance kept in the node cache.
#[cfg(feature = "multiprocess")]
async fn create_python_streaming_node(
    node_id: &str,
    node_type: &str,
    params: &serde_json::Value,
    session_id: &str,
) -> Result<(Box<dyn StreamingNode>, Arc<PythonStreamingNode>), ServiceError> {
    use remotemedia_core::nodes::AsyncNodeWrapper;

    info!(
        "🐍 Creating Python streaming node: {} with session {}",
        node_type, session_id
    );

    let py_node = PythonStreamingNode::with_session(
        node_id.to_string(),
        node_type,
        params,
        session_id.to_string(),
    )
    .map_err(|e| {
        ServiceError::Internal(format!("Failed to create Python streaming node: {}", e))
    })?;

    // Initialize the node immediately to load the model into memory
    py_node
        .ensure_initialized()
        .await
        .map_err(|e| ServiceError::Internal(format!("Failed to initialize Python node: {}", e)))?;

    let py_node_arc = Arc::new(py_node);
    let wrapped: Box<dyn StreamingNode> = Box::new(AsyncNodeWrapper(Arc::clone(&py_node_arc)));
    Ok((wrapped, py_node_arc))
}

/// Without the `multiprocess` feature there is no Python runtime to host
/// the node, so creating one is a clear error rather than a build failure.
#[cfg(not(feature = "multiprocess"))]
async fn create_python_streaming_node(
    _node_id: &str,
    node_type: &str,
    _params: &serde_json::Value,
    _session_id: &str,
) -> Result<(Box<dyn StreamingNode>, Arc<PythonStreamingNode>), ServiceError> {
    Err(ServiceError::Internal(format!(
        "Python node '{}' requires remotemedia-grpc to be built with the `multiprocess` feature",
        node_type
    )))
}

/// Handle DataChunk message with multi-output support (for streaming generators)
async fn handle_data_chunk_multi(
    chunk: crate::generated::DataChunk,
    session: Arc<Mutex<StreamSession>>,
    streaming_registry: Arc<StreamingNodeRegistry>,
    metrics: Arc<ServiceMetrics>,
    tx: tokio::sync::mpsc::Sender<Result<StreamResponse, Status>>,
    global_node_cache: Arc<RwLock<HashMap<String, CachedNode>>>,
) -> Result<usize, ServiceError> {
    let start_time = Instant::now();

    // Extract session_id for passing to Python nodes
    let session_id = {
        let sess = session.lock().await;
        sess.session_id.clone()
    };

    // Get or create node from cache (global cache with TTL)
    let (node, _py_streaming_node): (
        Arc<Box<dyn StreamingNode>>,
        Option<Arc<PythonStreamingNode>>,
    ) = {
        let mut sess = session.lock().await;
        sess.validate_sequence(chunk.sequence)?;

        // Get node info from manifest
        let node_spec = sess
            .manifest
            .nodes
            .iter()
            .find(|n| n.id == chunk.node_id)
            .ok_or_else(|| {
                ServiceError::Validation(format!("Node '{}' not found in manifest", chunk.node_id))
            })?;

        let node_type = node_spec.node_type.clone();
        let params = node_spec.params.clone();

        // Create cache key: "{node_type}:{params_hash}"
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        params.hash(&mut hasher);
        let cache_key = format!("{}:{:x}", node_type, hasher.finish());

        // Check global cache first (read lock)
        let cache_entry = {
            let global_cache = global_node_cache.read().await;
            global_cache.get(&cache_key).map(|cached| {
                // info!("♻️ Reusing globally cached node: {} (key: {})", chunk.node_id, cache_key);
                (Arc::clone(&cached.node), cached.py_streaming_node.clone())
            })
        };

        let (node, py_streaming_node) = if let Some((cached_node, cached_py_node)) = cache_entry {
            // CACHE HIT!
            sess.cache_hits += 1;
            metrics.record_cache_hit(&node_type);
            // info!("✅ Cache HIT for node type '{}' (session hits: {}, misses: {})", node_type, sess.cache_hits, sess.cache_misses);

            // Update timestamp in global cache (write lock)
            let mut global_cache = global_node_cache.write().await;
            if let Some(cached) = global_cache.get_mut(&cache_key) {
                cached.last_used = Instant::now();
            }

            // Also store in session cache for quick lookup
            sess.node_cache
                .insert(chunk.node_id.clone(), Arc::clone(&cached_node));
            (cached_node, cached_py_node)
        } else {
            // CACHE MISS!
            sess.cache_misses += 1;
            metrics.record_cache_miss(&node_type);
            // info!("❌ Cache MISS for node type '{}' (session hits: {}, misses: {})", node_type, sess.cache_hits, sess.cache_misses);

            // Node not cached - create new instance
            // info!("🆕 Creating new node: {} (type: {}, key: {})", chunk.node_id, node_type, cache_key);

            // Check if this is a Python node via the registry
            // Python nodes need special handling to preserve the unwrapped instance for caching
            let (new_node, py_streaming_node) = if streaming_registry.is_python_node(&node_type) {
                let (wrapped, py_node) =
                    create_python_streaming_node(&chunk.node_id, &node_type, &params, &session_id)
                        .await?;
                (wrapped, Some(py_node))
            } else {
                // Regular Rust nodes - use registry normally
                // info!("🦀 Creating Rust streaming node: {}", node_type);
                let node = streaming_registry
                    .create_node(
                        &node_type,
                        chunk.node_id.clone(),
                        &params,
                        Some(session_id.clone()),
                    )
                    .map_err(|e| ServiceError::Internal(format!("Failed to create node: {}", e)))?;
                (node, None)
            };

            // Wrap in Arc
            let arc_node = Arc::new(new_node);

            // Store in global cache with timestamp
            let mut global_cache = global_node_cache.write().await;
            global_cache.insert(
                cache_key.clone(),
                CachedNode {
                    node: Arc::clone(&arc_node),
                    py_streaming_node: py_streaming_node.clone(),
                    last_used: Instant::now(),
                },
            );

            // Update Prometheus gauge for cached nodes count
            metrics.set_cached_nodes_count(global_cache.len() as i64);

            // Also store in session cache for quick lookup
            sess.node_cache
                .insert(chunk.node_id.clone(), Arc::clone(&arc_node));

            // info!("💾 Globally cached node '{}' (type: {}, key: {}, total cached: {})", chunk.node_id, node_type, cache_key, global_cache.len());
            (arc_node, py_streaming_node)
        };

        (node, py_streaming_node)
    };

    // Convert DataBuffer(s) to RuntimeData
    use crate::adapters::data_buffer_to_runtime_data;

    let (runtime_data_map, data_type, item_count) = if !chunk.named_buffers.is_empty() {
        // Multi-input mode
        let mut map = HashMap::new();
        let mut total_items = 0u64;
        let mut types = Vec::new();

        for (name, data_buffer) in chunk.named_buffers {
            let runtime_data = data_buffer_to_runtime_data(&data_buffer).ok_or_else(|| {
                ServiceError::Validation(format!("Data conversion failed for '{}'", name))
            })?;

            types.push(runtime_data.data_type().to_string());
            total_items += runtime_data.item_count() as u64;
            map.insert(name, runtime_data);
        }

        let combined_type = if types.len() == 1 {
            types[0].to_string()
        } else {
            format!("multi[{}]", types.join("+"))
        };

        (map, combined_type, total_items)
    } else if let Some(data_buffer) = chunk.buffer {
        // Single-input mode
        let runtime_data = data_buffer_to_runtime_data(&data_buffer)
            .ok_or_else(|| ServiceError::Validation("Data conversion failed".to_string()))?;

        let data_type = runtime_data.data_type().to_string();
        let item_count = runtime_data.item_count() as u64;

        let mut map = HashMap::new();
        map.insert("input".to_string(), runtime_data);

        (map, data_type, item_count)
    } else {
        return Err(ServiceError::Validation(
            "DataChunk must have either 'buffer' or 'named_buffers' set".to_string(),
        ));
    };

    // Extract input data for single-input nodes
    let input_data = runtime_data_map
        .get("input")
        .or_else(|| runtime_data_map.values().next())
        .ok_or_else(|| ServiceError::Validation("No input data provided".to_string()))?
        .clone();

    // Check if this is a streaming node (Python or Rust)
    let node_type = node.node_type();
    let output_count: usize;

    // Check if this is a multi-output streaming node
    let is_streaming = streaming_registry.is_multi_output_streaming(&node_type);

    // Use streaming path for multi-output streaming nodes (both Python and Rust)
    if is_streaming {
        // Multi-yield streaming node - use callback for incremental sending
        info!(
            "🎙️ Detected multi-yield streaming node '{}', using streaming iteration",
            node_type
        );

        // USE THE CACHED NODE instead of creating a new one!
        // The 'node' variable already contains our globally cached instance
        // which preserves the Python object and the loaded Kokoro model

        // Create a bounded channel for chunks from the streaming node's
        // sync process callback into Rust's async world. Sized at the
        // default gRPC loopback capacity (256 slots) — generous so normal
        // operation is try_send-fast-path, tight enough to surface a
        // genuinely stalled downstream. Overflow drops with a warn and
        // relies on the client_tx bounded channel downstream as the true
        // backpressure surface.
        let (chunk_tx, mut chunk_rx) = tokio::sync::mpsc::channel::<RuntimeData>(
            crate::session_router::DEFAULT_GRPC_LOOPBACK_CAPACITY,
        );

        // Spawn async task to send chunks as they arrive from the channel
        let tx_clone = tx.clone();
        let session_clone = session.clone();
        let streaming_registry_clone = streaming_registry.clone();
        let chunk_node_id = chunk.node_id.clone();
        let base_sequence = chunk.sequence;
        let _data_type_clone = data_type.clone();
        let session_id_clone = session_id.clone();

        let send_task = tokio::spawn(async move {
            info!("📡 Send task started - waiting for chunks...");
            let mut chunk_idx = 0u64;

            while let Some(output_data) = chunk_rx.recv().await {
                info!(
                    "🎯 Received chunk {} from streaming node '{}' - immediately routing to client",
                    chunk_idx + 1,
                    chunk_node_id
                );

                // Use recursive routing
                if let Err(e) = route_to_downstream(
                    output_data,
                    chunk_node_id.clone(),
                    session_clone.clone(),
                    streaming_registry_clone.clone(),
                    tx_clone.clone(),
                    session_id_clone.clone(),
                    base_sequence + chunk_idx,
                )
                .await
                {
                    error!("Routing failed: {}", e);
                }

                chunk_idx += 1;
            }

            chunk_idx
        });

        // Process streaming with callback that just enqueues chunks
        // Use the unified trait method for both Python and Rust nodes

        // Spawn the streaming processing as a separate task so it doesn't block
        let chunk_tx_clone = chunk_tx.clone();
        let node_clone = Arc::clone(&node); // Clone the Arc
        let ctx = remotemedia_core::nodes::NodeRuntimeContext::for_test(
            session_id.clone(),
            chunk.node_id.clone(),
        );

        // Start the process_task IMMEDIATELY without blocking
        let process_task = tokio::spawn(async move {
            info!("🚀 Process task started");
            let result = node_clone
                .process_streaming_async(
                    input_data,
                    &ctx,
                    Box::new(move |output_data| {
                        info!("📨 Callback called - sending chunk to channel");
                        // Sync callback cannot .await. try_send on a
                        // bounded channel keeps the fast path allocation-
                        // free; on overflow we log-and-drop rather than
                        // hang, because `client_tx` downstream is the
                        // true backpressure surface for the gRPC stream.
                        match chunk_tx_clone.try_send(output_data) {
                            Ok(()) => {
                                info!("📨 Chunk sent to channel successfully");
                                Ok(())
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                tracing::warn!(
                                    "Streaming chunk channel full — dropping output (downstream saturated)"
                                );
                                Ok(())
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                error!("Streaming chunk channel closed");
                                Err(remotemedia_core::Error::Execution(
                                    "Failed to enqueue chunk".to_string(),
                                ))
                            }
                        }
                    }),
                )
                .await;
            info!("🏁 Process task completed");
            result
        });

        // Drop our reference to chunk_tx so it closes when process_task completes
        drop(chunk_tx);

        // CRITICAL: Don't wait for process_task to complete before starting to send!
        // The tasks should run truly concurrently

        // Wait for both tasks to complete
        let (send_result, process_result) = tokio::join!(send_task, process_task);

        // Check process task result
        process_result
            .map_err(|e| ServiceError::Internal(format!("Process task panicked: {}", e)))?
            .map_err(|e| ServiceError::Internal(format!("Multi-chunk streaming failed: {}", e)))?;

        // Get output count from send task
        output_count = send_result
            .map_err(|e| ServiceError::Internal(format!("Send task failed: {}", e)))?
            as usize;

        debug!("✅ Completed streaming {} chunks", output_count);
    } else {
        // Regular node - single output
        use crate::adapters::runtime_data_to_data_buffer;

        let ctx = remotemedia_core::nodes::NodeRuntimeContext::for_test(
            session_id.clone(),
            chunk.node_id.clone(),
        );
        let output = node
            .process_async(input_data, &ctx)
            .await
            .map_err(|e| ServiceError::Internal(format!("Node execution failed: {}", e)))?;

        let output_buffer = runtime_data_to_data_buffer(&output);
        let mut data_outputs = HashMap::new();
        data_outputs.insert(chunk.node_id.clone(), output_buffer);

        let total_items = {
            let mut sess = session.lock().await;
            sess.record_chunk_metrics(0.0, item_count, 0, &data_type);
            sess.total_items
        };

        let chunk_result = ChunkResult {
            sequence: chunk.sequence,
            data_outputs,
            processing_time_ms: 0.0,
            total_items_processed: total_items,
        };

        let response = StreamResponse {
            response: Some(StreamResponseType::Result(chunk_result)),
        };
        tx.send(Ok(response))
            .await
            .map_err(|_| ServiceError::Internal("Failed to send ChunkResult".to_string()))?;

        output_count = 1;
    }

    let processing_time_ms = start_time.elapsed().as_secs_f64() * 1000.0;
    debug!(
        "Total processing time: {:.2}ms for {} chunks",
        processing_time_ms, output_count
    );

    Ok(output_count)
}

/// Handle StreamControl message
async fn handle_stream_control(
    control: StreamControl,
    session: Arc<Mutex<StreamSession>>,
) -> Result<StreamClosed, ServiceError> {
    let sess = session.lock().await;

    let command = Command::try_from(control.command)
        .map_err(|_| ServiceError::Validation(format!("Invalid command: {}", control.command)))?;

    let reason = match command {
        Command::Close => "Client requested close",
        Command::Cancel => "Client requested cancel",
        Command::Unspecified => "Unspecified",
    };

    let closed = StreamClosed {
        session_id: sess.session_id.clone(),
        final_metrics: Some(sess.create_final_metrics()),
        reason: reason.to_string(),
    };

    info!(
        session_id = %sess.session_id,
        chunks_processed = sess.chunks_processed,
        reason = reason,
        "Stream closing"
    );

    Ok(closed)
}

#[cfg(test)]
mod origin_tests {
    use super::*;

    #[test]
    fn stream_origin_header_is_sanitised() {
        let mut md = tonic::metadata::MetadataMap::new();
        assert_eq!(stream_origin(&md), None);
        md.insert(STREAM_ORIGIN_HEADER, "  agent:claude-code ".parse().unwrap());
        assert_eq!(stream_origin(&md).as_deref(), Some("agent:claude-code"));
        md.insert(STREAM_ORIGIN_HEADER, "".parse().unwrap());
        assert_eq!(stream_origin(&md), None);
        let long = "a".repeat(MAX_STREAM_ORIGIN_LEN + 1);
        md.insert(STREAM_ORIGIN_HEADER, long.parse().unwrap());
        assert_eq!(stream_origin(&md), None);
    }
}
