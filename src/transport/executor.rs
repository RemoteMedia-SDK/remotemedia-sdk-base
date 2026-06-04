//! PipelineExecutor - Unified facade for transport layers
//!
//! This module provides:
//! - SessionHandle for streaming sessions
//! - PipelineExecutor as unified entry point
//! - Unary and streaming execution modes
//! - Factory registration support
//!
//! # Usage
//!
//! ```ignore
//! let executor = PipelineExecutor::new()?;
//! let result = executor.execute_unary(manifest, input).await?;
//! ```
//!
//! # Architecture
//!
//! PipelineExecutor wraps SessionRouter with StreamingScheduler to provide:
//! - Production-grade execution with timeout/retry/circuit breaker
//! - DriftMetrics for stream health monitoring
//! - Unified API for all transports (HTTP, gRPC, WebRTC, FFI)
//!
//! # Spec Reference
//!
//! See `/specs/026-streaming-scheduler-migration/` for full specification.

use crate::capabilities::{negotiate_manifest, strict_capabilities_enabled};
use crate::data::RuntimeData;
use crate::executor::streaming_scheduler::{SchedulerConfig, StreamingScheduler};
use crate::executor::DriftThresholds;
use crate::manifest::Manifest;
use crate::nodes::{StreamingNodeFactory, StreamingNodeRegistry};
use crate::transport::data::Participant;
use crate::transport::session_control::{SessionControl, SessionControlBus};
use crate::transport::session_router::{DataPacket, SessionRouter};
use crate::transport::shared_session::SharedPipelineSession;
use crate::transport::TransportData;
use crate::{Error, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;

/// Configuration for PipelineExecutor
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Scheduler configuration
    pub scheduler_config: SchedulerConfig,
    /// Drift metrics thresholds
    pub drift_thresholds: DriftThresholds,
    /// Enable drift metrics collection
    pub enable_drift_metrics: bool,
    /// Session ID prefix for generated sessions
    pub session_id_prefix: String,
    /// Anchor for relative paths in
    /// [`crate::manifest::Manifest::plugins`] specs.
    ///
    /// When `None`, the resolver uses the process current working
    /// directory. Callers that load a manifest from a file should set
    /// this to the manifest's parent directory so `"./plugins/foo.so"`
    /// resolves relative to where the manifest lives, not where the
    /// program was launched from.
    pub manifest_base_dir: Option<std::path::PathBuf>,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            scheduler_config: SchedulerConfig::default(),
            drift_thresholds: DriftThresholds::default(),
            enable_drift_metrics: true,
            session_id_prefix: "session".to_string(),
            manifest_base_dir: None,
        }
    }
}

/// Handle to an active streaming session
///
/// SessionHandle provides a type-safe interface for streaming data
/// through a pipeline session. It owns the channels and task handle.
pub struct SessionHandle {
    /// Unique session identifier
    pub session_id: String,
    /// Channel for sending input data to the session (None after input complete).
    ///
    /// Bounded — see `DEFAULT_ROUTER_INPUT_CAPACITY` in `session_router`.
    input_tx: Option<mpsc::Sender<DataPacket>>,
    /// Per-kind receivers for sink output. See [`ClientOutputRouter`] for
    /// why kinds are split. Wrapped in `Option` so `take_output_receivers`
    /// can consume them — needed when a transport (e.g. WebRTC peer) wants
    /// to drain each kind on its own task instead of multiplexing through
    /// `recv_output`.
    output_audio_rx: Option<mpsc::Receiver<RuntimeData>>,
    output_video_rx: Option<mpsc::Receiver<RuntimeData>>,
    output_data_rx: Option<mpsc::Receiver<RuntimeData>>,
    /// Shutdown signal sender
    shutdown_tx: mpsc::Sender<()>,
    /// Handle to the session router task
    task_handle: JoinHandle<Result<()>>,
    /// Whether the session is still active
    is_active: bool,
    /// Active trace recorder, if `REMOTEMEDIA_RECORD_DIR` was set when
    /// the session was created. Held here so its tap subscriptions +
    /// writer task live exactly as long as the session does; on drop
    /// the writer finishes the JSONL file and the tap relays exit.
    _recorder: Option<crate::transport::session_recorder::SessionRecorder>,
    /// Per-session perf aggregator. `Some` whenever `REMOTEMEDIA_PERF_TAP`
    /// was set at session-construction time. External tooling calls
    /// [`PerfAggregator::peek_snapshot`] at end-of-run to read merged
    /// HDR-histogram percentiles without racing the periodic flush.
    perf_aggregator: Option<Arc<crate::transport::perf_aggregator::PerfAggregator>>,
}

impl SessionHandle {
    /// Send input data to the session.
    ///
    /// The data will be processed through the pipeline and outputs
    /// will be available via `recv_output()`.
    ///
    /// # **REAL-TIME UNSAFE**
    ///
    /// This method is `async` and awaits on a bounded tokio channel. It
    /// must not be called from a real-time-priority thread (Core Audio
    /// HAL IO proc, AU render callback, JACK process callback, AAudio
    /// data callback, etc.) — `.await` returns control to the tokio
    /// scheduler, and a full queue parks the caller. For RT audio hosts,
    /// use the [`remotemedia-rt-bridge`] crate, which pumps data from
    /// RT threads into the async pipeline through pinned-thread SPSC
    /// rings, or call [`crate::nodes::process_sync`] directly on a
    /// [`crate::nodes::SyncStreamingNode`] to skip the executor entirely.
    pub async fn send_input(&self, data: TransportData) -> Result<()> {
        if !self.is_active {
            return Err(crate::Error::Execution("Session is closed".to_string()));
        }

        let packet = DataPacket {
            data: data.data,
            from_node: "client".to_string(),
            to_node: None, // Route to sources
            session_id: self.session_id.clone(),
            sequence: data.sequence.unwrap_or(0),
            sub_sequence: data.sequence.unwrap_or(0),
            metadata: data.metadata,
        };

        let tx = self.input_tx.as_ref().ok_or_else(|| {
            crate::Error::Execution("Input channel closed (input complete signalled)".to_string())
        })?;
        // Bounded channel: this `.await` is the ingress backpressure point.
        // When the router's input queue is full, the producer stalls here
        // rather than growing memory unboundedly.
        tx.send(packet)
            .await
            .map_err(|e| crate::Error::Execution(format!("Failed to send input: {}", e)))?;

        Ok(())
    }

    /// Receive output data from the session.
    ///
    /// Multiplexes across the three kind-split receivers (Audio / Video /
    /// Data). For high-throughput WebRTC peers that want to drain kinds in
    /// parallel, prefer [`Self::take_output_receivers`] and run a task per
    /// kind — `recv_output` here goes through a single `select!` and is
    /// strictly slower than the parallel path.
    ///
    /// Returns `None` if all three channels are closed.
    pub async fn recv_output(&mut self) -> Result<Option<TransportData>> {
        // Take temporary &mut references to whatever receivers are still
        // present. If `take_output_receivers` has consumed them, `select!`
        // sees an empty futures set and would panic — guard with a
        // try-recv fast path that returns None instead.
        let audio = self.output_audio_rx.as_mut();
        let video = self.output_video_rx.as_mut();
        let data = self.output_data_rx.as_mut();
        if audio.is_none() && video.is_none() && data.is_none() {
            return Ok(None);
        }
        // `tokio::select!` requires concrete futures; build them
        // conditionally with `Option::map(|rx| rx.recv())` and unwrap to
        // pending when absent so the select! arm is effectively disabled.
        let audio_fut = async {
            match audio {
                Some(rx) => rx.recv().await,
                None => std::future::pending().await,
            }
        };
        let video_fut = async {
            match video {
                Some(rx) => rx.recv().await,
                None => std::future::pending().await,
            }
        };
        let data_fut = async {
            match data {
                Some(rx) => rx.recv().await,
                None => std::future::pending().await,
            }
        };
        tokio::pin!(audio_fut, video_fut, data_fut);
        tokio::select! {
            v = &mut audio_fut => Ok(v.map(TransportData::new)),
            v = &mut video_fut => Ok(v.map(TransportData::new)),
            v = &mut data_fut => Ok(v.map(TransportData::new)),
        }
    }

    /// Try to receive output data without blocking.
    ///
    /// Polls all three kind channels in turn; returns the first message
    /// found (Audio first, then Video, then Data) or `None` if all are
    /// empty. Callers that care about per-kind ordering should use the
    /// per-kind receivers obtained from [`Self::take_output_receivers`]
    /// instead.
    pub fn try_recv_output(&mut self) -> Result<Option<TransportData>> {
        if let Some(rx) = self.output_audio_rx.as_mut() {
            if let Ok(data) = rx.try_recv() {
                return Ok(Some(TransportData::new(data)));
            }
        }
        if let Some(rx) = self.output_video_rx.as_mut() {
            if let Ok(data) = rx.try_recv() {
                return Ok(Some(TransportData::new(data)));
            }
        }
        if let Some(rx) = self.output_data_rx.as_mut() {
            if let Ok(data) = rx.try_recv() {
                return Ok(Some(TransportData::new(data)));
            }
        }
        Ok(None)
    }

    /// Consume the per-kind receivers so the caller can drain each on its
    /// own task. After calling this, `recv_output` / `try_recv_output`
    /// always return `Ok(None)`.
    ///
    /// Used by WebRTC peers to run independent drainers for Audio /
    /// Video / Data — the architectural fix that eliminates head-of-line
    /// blocking when one consumer (e.g. CPU video encoder) is slower than
    /// another. See `crates/transports/webrtc/src/peer/server_peer.rs`.
    pub fn take_output_receivers(
        &mut self,
    ) -> Option<crate::transport::session_router::ClientOutputReceivers> {
        let audio = self.output_audio_rx.take()?;
        let video = self.output_video_rx.take()?;
        let data = self.output_data_rx.take()?;
        Some(crate::transport::session_router::ClientOutputReceivers {
            audio_rx: audio,
            video_rx: video,
            data_rx: data,
        })
    }

    /// Check if the session is still active
    pub fn is_active(&self) -> bool {
        self.is_active && !self.task_handle.is_finished()
    }

    /// Per-session perf aggregator, if `REMOTEMEDIA_PERF_TAP` was set
    /// when this session was created. Performance tooling uses this to read
    /// merged HDR-histogram percentiles at end-of-run without racing
    /// the periodic 1 s flush task.
    pub fn perf_aggregator(
        &self,
    ) -> Option<&Arc<crate::transport::perf_aggregator::PerfAggregator>> {
        self.perf_aggregator.as_ref()
    }

    /// Signal that no more input will be sent
    ///
    /// This closes the input channel, allowing the session router to detect
    /// end-of-input and shut down gracefully after processing remaining data.
    /// Outputs can still be received via `recv_output()` after calling this.
    pub fn signal_input_complete(&mut self) {
        self.input_tx = None;
    }

    /// Clone-able, send-only handle onto this session's input.
    ///
    /// Transport adapters (WebRTC, gRPC) need to forward inputs on one
    /// task while draining outputs on another — without this split, a
    /// full router input channel blocks the same task that's supposed
    /// to be pulling outputs, which can deadlock if the router's
    /// output channel is also full (classic bounded-channel ring
    /// deadlock). This returns a lightweight handle that owns a clone
    /// of the input `Sender` and can be moved into its own task.
    ///
    /// Returns `None` after `signal_input_complete()` has been called.
    pub fn input_sender(&self) -> Option<SessionInputSender> {
        self.input_tx.as_ref().map(|tx| SessionInputSender {
            tx: tx.clone(),
            session_id: self.session_id.clone(),
        })
    }

    /// Create a participant-scoped ingress handle for this session.
    ///
    /// The returned handle shares this pipeline session's input channel and
    /// stamps every frame with canonical participant metadata before it enters
    /// the router.
    pub fn join_participant(&self, participant: Participant) -> Option<ParticipantSessionHandle> {
        self.input_sender()
            .map(|input| ParticipantSessionHandle::new(input, participant))
    }

    /// Close the session gracefully
    pub async fn close(&mut self) -> Result<()> {
        if !self.is_active {
            return Ok(());
        }

        self.is_active = false;

        // Send shutdown signal
        let _ = self.shutdown_tx.send(()).await;

        Ok(())
    }

    /// Wait for the session to complete
    pub async fn wait(self) -> Result<()> {
        self.task_handle
            .await
            .map_err(|e| crate::Error::Execution(format!("Session task panicked: {}", e)))?
    }
}

/// Clone-able, send-only side of a [`SessionHandle`].
///
/// See [`SessionHandle::input_sender`] for why this exists.
#[derive(Clone)]
pub struct SessionInputSender {
    tx: mpsc::Sender<DataPacket>,
    session_id: String,
}

impl SessionInputSender {
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Send input data. Blocks when the router input channel is full
    /// — call from a dedicated task so a full queue doesn't wedge
    /// the output-drain loop.
    pub async fn send(&self, data: TransportData) -> Result<()> {
        let packet = DataPacket {
            data: data.data,
            from_node: "client".to_string(),
            to_node: None,
            session_id: self.session_id.clone(),
            sequence: data.sequence.unwrap_or(0),
            sub_sequence: data.sequence.unwrap_or(0),
            metadata: data.metadata,
        };
        self.tx
            .send(packet)
            .await
            .map_err(|e| crate::Error::Execution(format!("Failed to send input: {}", e)))
    }

    pub(crate) fn from_data_packet_sender(
        session_id: impl Into<String>,
        tx: mpsc::Sender<DataPacket>,
    ) -> Self {
        Self {
            tx,
            session_id: session_id.into(),
        }
    }
}

/// Participant-scoped ingress handle for a shared pipeline session.
#[derive(Clone)]
pub struct ParticipantSessionHandle {
    input: SessionInputSender,
    participant: Participant,
}

impl ParticipantSessionHandle {
    pub fn new(input: SessionInputSender, participant: Participant) -> Self {
        Self { input, participant }
    }

    /// Pipeline session ID this participant is attached to.
    pub fn session_id(&self) -> &str {
        self.input.session_id()
    }

    /// Participant descriptor used to stamp outgoing frames.
    pub fn participant(&self) -> &Participant {
        &self.participant
    }

    /// Send input from this participant into the shared pipeline session.
    pub async fn send(&self, data: TransportData) -> Result<()> {
        self.input
            .send(data.with_participant_descriptor(&self.participant))
            .await
    }
}

// Implement StreamSession for SessionHandle to allow use in PipelineTransport
#[async_trait::async_trait]
impl crate::transport::StreamSession for SessionHandle {
    fn session_id(&self) -> &str {
        &self.session_id
    }

    async fn send_input(&mut self, data: TransportData) -> Result<()> {
        // SessionHandle::send_input takes &self, so we can call it with &mut self
        <SessionHandle>::send_input(self, data).await
    }

    async fn recv_output(&mut self) -> Result<Option<TransportData>> {
        <SessionHandle>::recv_output(self).await
    }

    async fn close(&mut self) -> Result<()> {
        <SessionHandle>::close(self).await
    }

    fn is_active(&self) -> bool {
        <SessionHandle>::is_active(self)
    }
}

/// Trait representing a host that can spawn pipeline sessions.
/// This trait abstracts the pipeline execution host and decouples transports from
/// concrete executor implementations.
pub trait PipelineSessionHost: Send + Sync {
    /// Create a new session for the pipeline manifest
    fn create_session(
        &self,
        manifest: Arc<Manifest>,
    ) -> futures::future::BoxFuture<'_, crate::Result<SessionHandle>>;

    /// Get or create a shared session for this pipeline
    fn get_or_create_shared_session(
        &self,
        key: String,
        manifest: Arc<Manifest>,
    ) -> futures::future::BoxFuture<'_, crate::Result<Arc<SharedPipelineSession>>>;

    /// Access the control bus of this host
    fn control_bus(&self) -> Arc<SessionControlBus>;

    /// Access the node registry of this host
    fn registry(&self) -> Arc<RwLock<StreamingNodeRegistry>>;
}

impl PipelineSessionHost for PipelineExecutor {
    fn create_session(
        &self,
        manifest: Arc<Manifest>,
    ) -> futures::future::BoxFuture<'_, crate::Result<SessionHandle>> {
        Box::pin(async move { self.create_session(manifest).await })
    }

    fn get_or_create_shared_session(
        &self,
        key: String,
        manifest: Arc<Manifest>,
    ) -> futures::future::BoxFuture<'_, crate::Result<Arc<SharedPipelineSession>>> {
        Box::pin(async move { self.get_or_create_shared_session(key, manifest).await })
    }

    fn control_bus(&self) -> Arc<SessionControlBus> {
        self.control_bus.clone()
    }

    fn registry(&self) -> Arc<RwLock<StreamingNodeRegistry>> {
        self.registry.clone()
    }
}

/// Unified facade for transport pipeline execution
///
/// PipelineExecutor provides a clean API with production-grade execution features.
///
/// # Features
///
/// - **Unary execution**: Single input → single output
/// - **Streaming sessions**: Multiple inputs → multiple outputs via SessionHandle
/// - **Factory registration**: Custom node type registration
/// - **Schema validation**: Manifest validation before execution
/// - **Metrics**: Prometheus-format metrics export
pub struct PipelineExecutor {
    /// Configuration
    config: ExecutorConfig,
    /// Node registry for creating nodes (wrapped in RwLock for mutable access)
    registry: Arc<RwLock<StreamingNodeRegistry>>,
    /// Streaming scheduler for node execution
    scheduler: Arc<StreamingScheduler>,
    /// Session counter for ID generation
    session_counter: std::sync::atomic::AtomicU64,
    /// Shared pipeline sessions keyed by logical room/session key.
    shared_sessions: Arc<RwLock<HashMap<String, Arc<SharedPipelineSession>>>>,
    /// Process-wide control bus for client-side pub/sub/intercept.
    ///
    /// Populated automatically for every session created via
    /// [`Self::create_session`]. Transport layers (gRPC, WebRTC) look
    /// up a session here when a client sends an `Attach(session_id)`
    /// control frame.
    control_bus: Arc<SessionControlBus>,
    /// Optional weak reference to a [`WarmSessionPool`] that
    /// `create_session` consults before falling back to cold-build.
    ///
    /// `Weak` is intentional (D2 in design.md): the pool stores
    /// `Arc<PipelineExecutor>` to call `cold_build_session` for
    /// new entries, so the executor must NOT hold a strong reference
    /// back, or both would leak in a cycle. Drop the pool to disable
    /// delegation cleanly — `Weak::upgrade()` returns `None` and the
    /// executor falls back to cold-build.
    default_pool: std::sync::Mutex<
        Option<std::sync::Weak<crate::transport::warm_session_pool::WarmSessionPool>>,
    >,
    /// Plugins resolved from manifests + dlopen'd into this process.
    ///
    /// Keyed by canonical absolute path so the same `.so` referenced
    /// from two manifests is loaded exactly once. The [`LoadableNodeBundle`]
    /// keeps the underlying library mapped + the `Arc<dyn StreamingNodeFactory>`
    /// factories alive — dropping the bundle would defeat the
    /// per-session registry snapshot.
    ///
    /// Populated by [`Self::ensure_plugins_loaded`] (called from
    /// `cold_build_session` before manifest validation).
    ///
    /// Gated on the `loadable` feature — when disabled (e.g. for
    /// `default-features = false` consumers like the silero-vad and
    /// whisper loadable plugins, which dlopen-from is irrelevant to)
    /// the field is omitted entirely and `ensure_plugins_loaded` is a
    /// no-op stub.
    #[cfg(feature = "loadable")]
    loaded_plugins: tokio::sync::Mutex<
        std::collections::HashMap<std::path::PathBuf, crate::loadable::factory::LoadableNodeBundle>,
    >,
}

impl PipelineExecutor {
    /// Create a new PipelineExecutor with default configuration
    pub fn new() -> Result<Self> {
        Self::with_config(ExecutorConfig::default())
    }

    /// Create a new PipelineExecutor with custom configuration
    pub fn with_config(config: ExecutorConfig) -> Result<Self> {
        let scheduler = Arc::new(StreamingScheduler::new(config.scheduler_config.clone()));
        // Use the default registry with all built-in nodes registered
        let registry = Arc::new(RwLock::new(
            crate::nodes::streaming_registry::create_default_streaming_registry(),
        ));

        // Reuse the process-wide SessionControlBus if one already exists
        // (typically installed by an earlier PipelineExecutor::new call or by
        // a host that called install_global explicitly). This avoids per-
        // executor bus fragmentation: every executor in the process shares
        // the same bus so InProcControlTransport::open(session_id) can find
        // any registered session regardless of which executor created it.
        //
        // If no bus is yet installed, construct one and install it — same
        // first-writer-wins semantics as before, just lifted out of an
        // unconditional new+install pair.
        let control_bus = match crate::transport::session_control::global_bus() {
            Some(existing) => existing,
            None => {
                let fresh = SessionControlBus::new();
                SessionControlBus::install_global(fresh.clone());
                fresh
            }
        };

        Ok(Self {
            config,
            registry,
            scheduler,
            session_counter: std::sync::atomic::AtomicU64::new(0),
            shared_sessions: Arc::new(RwLock::new(HashMap::new())),
            control_bus,
            default_pool: std::sync::Mutex::new(None),
            #[cfg(feature = "loadable")]
            loaded_plugins: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// Resolve every entry in `manifest.plugins`, dlopen any not
    /// already loaded, and register their factories into the executor's
    /// registry. Idempotent — re-running with the same manifest is a
    /// no-op once the bundles are loaded.
    ///
    /// Called automatically by [`Self::cold_build_session`] before
    /// manifest validation runs, so any node types contributed by
    /// plugins are visible when validate checks `node.node_type` against
    /// the registry.
    ///
    /// Gated on the `loadable` feature. When the feature is disabled
    /// (e.g. when a Rust plugin author depends on `remotemedia-core`
    /// with `default-features = false` to keep the cdylib size down,
    /// since they have no need to *consume* other plugins themselves),
    /// the function is a no-op stub that surfaces a precise error if
    /// the manifest actually tries to declare any plugins.
    #[cfg(feature = "loadable")]
    pub async fn ensure_plugins_loaded(&self, manifest: &Manifest) -> Result<()> {
        if manifest.plugins.is_empty() {
            return Ok(());
        }
        // Anchor relative paths at the configured manifest base dir, or
        // process CWD when none was set.
        let base_dir: std::path::PathBuf = self
            .config
            .manifest_base_dir
            .clone()
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| std::path::PathBuf::from("."));

        let resolver = crate::loadable::resolver::PluginResolver::new(&base_dir);

        for spec in &manifest.plugins {
            // `resolve_async` handles local paths + GitHub releases +
            // direct URLs + Python source-load. Local-path-only call
            // sites can use the sync `resolve` instead, but executor
            // always takes the async path so a Manifest with mixed
            // local + remote + source specs Just Works.
            let resolved = resolver
                .resolve_async(spec)
                .await
                .map_err(|e| Error::Manifest(format!("plugin resolution failed: {e}")))?;
            match resolved {
                crate::loadable::resolver::ResolvedPlugin::Cdylib { path } => {
                    self.load_cdylib_plugin(path).await?;
                }
                crate::loadable::resolver::ResolvedPlugin::SourcePython {
                    plugin_toml,
                    module_root,
                    hash,
                } => {
                    self.load_source_python_plugin(plugin_toml, module_root, hash)
                        .await?;
                }
            }
        }
        Ok(())
    }

    /// Stub when the `loadable` feature is disabled. Surfaces a precise
    /// build-time error if the manifest declares any plugins, instead
    /// of silently ignoring them (which would later manifest as
    /// confusing "unknown node type" validation errors).
    #[cfg(not(feature = "loadable"))]
    pub async fn ensure_plugins_loaded(&self, manifest: &Manifest) -> Result<()> {
        if manifest.plugins.is_empty() {
            return Ok(());
        }
        Err(Error::Manifest(format!(
            "manifest declares {} plugin(s) but remotemedia-core was \
             built without the `loadable` feature. Rebuild with \
             `--features loadable` to enable plugin resolution.",
            manifest.plugins.len()
        )))
    }

    /// Dlopen a cdylib + register its factories. Idempotent by
    /// canonical path — see `loaded_plugins`.
    #[cfg(feature = "loadable")]
    async fn load_cdylib_plugin(&self, path: std::path::PathBuf) -> Result<()> {
        let canonical = path.canonicalize().unwrap_or(path.clone());
        {
            let loaded = self.loaded_plugins.lock().await;
            if loaded.contains_key(&canonical) {
                return Ok(());
            }
        }
        let bundle =
            crate::loadable::factory::LoadableNodeBundle::load(&canonical).map_err(|e| {
                Error::Manifest(format!("loadable plugin {canonical:?} failed to load: {e}"))
            })?;
        {
            let mut registry = self.registry.write().await;
            bundle.register_into(&mut registry);
        }
        self.loaded_plugins.lock().await.insert(canonical, bundle);
        Ok(())
    }

    /// Register a `SourcePythonFactory` per `node_types` entry from the
    /// resolved plugin.toml. Each factory spawns its own Python
    /// subprocess on `create()`.
    #[cfg(all(feature = "loadable", feature = "python-source-plugin"))]
    async fn load_source_python_plugin(
        &self,
        plugin_toml: crate::loadable::plugin_toml::PluginToml,
        module_root: std::path::PathBuf,
        hash: String,
    ) -> Result<()> {
        let py = plugin_toml.python.as_ref().ok_or_else(|| {
            Error::Manifest(format!(
                "source-python plugin '{}' missing [python] table at registration time",
                plugin_toml.plugin.name
            ))
        })?;
        let mut registry = self.registry.write().await;
        for node_type in &py.node_types {
            let plugin = crate::loadable::source_python::SourcePythonPlugin {
                node_type: node_type.clone(),
                entry_module: py.entry_module.clone(),
                module_root: module_root.clone(),
                requires: py.requires.clone(),
                hash: hash.clone(),
            };
            let factory = std::sync::Arc::new(
                crate::loadable::source_python::SourcePythonFactory::new(plugin),
            );
            registry.register(factory);
        }
        Ok(())
    }

    /// Stub when `python-source-plugin` feature is disabled — surfaces
    /// the precise build-config error instead of a confusing "node
    /// type not found" downstream.
    #[cfg(all(feature = "loadable", not(feature = "python-source-plugin")))]
    async fn load_source_python_plugin(
        &self,
        plugin_toml: crate::loadable::plugin_toml::PluginToml,
        _module_root: std::path::PathBuf,
        _hash: String,
    ) -> Result<()> {
        Err(Error::Manifest(format!(
            "plugin '{}' is a Python source plugin but the `python-source-plugin` \
             feature on remotemedia-core was not enabled at build time. \
             Rebuild with `--features python-source-plugin`.",
            plugin_toml.plugin.name
        )))
    }

    /// Access the process-wide [`SessionControlBus`].
    ///
    /// Transport servers (gRPC `PipelineControl`, WebRTC control data
    /// channel) use this to look up a [`SessionControl`] by `session_id`
    /// when a client opens a control-plane attach.
    pub fn control_bus(&self) -> Arc<SessionControlBus> {
        self.control_bus.clone()
    }

    /// Get the scheduler reference
    pub fn scheduler(&self) -> &Arc<StreamingScheduler> {
        &self.scheduler
    }

    /// Get the node registry reference (wrapped in RwLock)
    pub fn registry(&self) -> &Arc<RwLock<StreamingNodeRegistry>> {
        &self.registry
    }

    /// Generate a unique session ID
    pub fn generate_session_id(&self) -> String {
        let count = self
            .session_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        format!("{}_{}", self.config.session_id_prefix, count)
    }

    /// Register a custom node factory
    ///
    /// # Arguments
    ///
    /// * `factory` - Factory for creating node instances (includes node_type internally)
    ///
    /// # Example
    ///
    /// ```ignore
    /// executor.register_factory(Arc::new(MyCustomNodeFactory)).await;
    /// ```
    pub async fn register_factory(&self, factory: Arc<dyn StreamingNodeFactory>) {
        let mut registry = self.registry.write().await;
        registry.register(factory);
    }

    /// Attach a [`WarmSessionPool`] as this executor's default pool.
    ///
    /// After this call, [`Self::create_session`] consults the pool first
    /// (via [`WarmSessionPool::acquire`]) and falls back to
    /// [`Self::cold_build_session`] when the pool's `Weak` upgrade returns
    /// `None` (i.e., the pool's last `Arc` has been dropped).
    ///
    /// The executor stores `Arc::downgrade(&pool)`. The caller retains the
    /// only strong reference, so dropping the caller's `Arc<WarmSessionPool>`
    /// transparently disables delegation without further executor calls.
    ///
    /// Calling `set_default_pool` again replaces the previous pool. Use
    /// [`Self::clear_default_pool`] to detach without replacing.
    pub fn set_default_pool(
        &self,
        pool: std::sync::Arc<crate::transport::warm_session_pool::WarmSessionPool>,
    ) {
        *self
            .default_pool
            .lock()
            .expect("default_pool mutex poisoned") = Some(std::sync::Arc::downgrade(&pool));
    }

    /// Detach the default pool without dropping it.
    ///
    /// Subsequent [`Self::create_session`] calls cold-build via
    /// [`Self::cold_build_session`]. The pool's `Arc` count is unaffected —
    /// the caller may still hold and use the pool directly via
    /// [`crate::transport::warm_session_pool::WarmSessionPool::acquire`].
    pub fn clear_default_pool(&self) {
        *self
            .default_pool
            .lock()
            .expect("default_pool mutex poisoned") = None;
    }

    /// List all registered node types
    ///
    /// Returns a sorted list of node type names that can be used in pipelines.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let executor = PipelineExecutor::new()?;
    /// let types = executor.list_node_types().await;
    /// for node_type in types {
    ///     println!("Available: {}", node_type);
    /// }
    /// ```
    pub async fn list_node_types(&self) -> Vec<String> {
        let registry = self.registry.read().await;
        registry.list_types()
    }

    /// Validate a manifest before execution
    ///
    /// Checks:
    /// - All referenced node types are registered
    /// - Connection graph is valid (no cycles, all endpoints exist)
    /// - Node parameters are valid
    pub async fn validate_manifest(&self, manifest: &Manifest) -> Result<()> {
        // Build the graph to validate connections
        crate::executor::PipelineGraph::from_manifest(manifest)?;

        // Verify all node types are registered
        let registry = self.registry.read().await;
        for node in &manifest.nodes {
            if !registry.has_node_type(&node.node_type) {
                return Err(crate::Error::Execution(format!(
                    "Unknown node type '{}' for node '{}'",
                    node.node_type, node.id
                )));
            }
        }

        Ok(())
    }

    /// Run capability negotiation over a manifest and return a possibly-rewritten copy.
    ///
    /// Behavior is driven by manifest metadata + the `REMOTEMEDIA_STRICT_CAPS`
    /// env var:
    ///
    /// * `metadata.auto_negotiate = true` — unbridgeable mismatches still
    ///   surface as warnings, but anything fixable becomes an inserted
    ///   `FastResampleNode` in the returned manifest.
    /// * `metadata.strict_capabilities = true` (or env override) — any
    ///   *remaining* unresolved mismatch is fatal. Otherwise mismatches are
    ///   logged as `tracing::warn!` and the session proceeds with the
    ///   original wiring.
    ///
    /// Returns the manifest the caller should use for session construction.
    /// When auto-insertion ran, this is a new `Arc` whose `nodes` and
    /// `connections` include the synthesized conversion hops.
    pub(crate) async fn negotiate_capabilities(
        &self,
        manifest: Arc<Manifest>,
    ) -> Result<Arc<Manifest>> {
        let registry = self.registry.read().await;
        let outcome = negotiate_manifest(manifest.clone(), &registry);
        drop(registry);

        if !outcome.inserted_nodes.is_empty() {
            tracing::info!(
                "Capability negotiation inserted {} conversion node(s): {}",
                outcome.inserted_nodes.len(),
                outcome
                    .inserted_nodes
                    .iter()
                    .map(|n| format!(
                        "{}({} between {}→{})",
                        n.node_type, n.id, n.between.0, n.between.1
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }

        if outcome.has_unresolved() {
            let summary = outcome
                .warnings
                .iter()
                .map(|m| m.to_string())
                .collect::<Vec<_>>()
                .join("; ");

            if strict_capabilities_enabled(&outcome.manifest) {
                return Err(crate::Error::Execution(format!(
                    "Capability validation failed (strict mode): {}",
                    summary
                )));
            } else {
                tracing::warn!(
                    "Capability validation reported {} unresolved mismatch(es) (pipeline will still attempt to run; set metadata.strict_capabilities or REMOTEMEDIA_STRICT_CAPS to fail fast): {}",
                    outcome.warnings.len(),
                    summary
                );
            }
        }

        Ok(outcome.manifest)
    }

    /// Execute a pipeline with unary semantics (single input → single output)
    ///
    /// This creates a temporary session, processes the input, and returns the output.
    /// For multiple inputs/outputs, use `create_session()` instead.
    ///
    /// # Arguments
    ///
    /// * `manifest` - Pipeline configuration
    /// * `input` - Input data to process
    ///
    /// # Returns
    ///
    /// The output from the pipeline's sink nodes
    pub async fn execute_unary(
        &self,
        manifest: Arc<Manifest>,
        input: TransportData,
    ) -> Result<TransportData> {
        // Validate manifest (cycles, unknown node types) then run capability
        // negotiation. The returned manifest may have FastResampleNode entries
        // spliced in by negotiation when `metadata.auto_negotiate` is set.
        self.validate_manifest(&manifest).await?;
        let manifest = self.negotiate_capabilities(manifest).await?;

        // Create a temporary session
        let mut session = self.create_session(manifest).await?;

        // Send input
        session.send_input(input).await?;

        // Wait for output BEFORE closing (close() shuts down the router)
        let output = session.recv_output().await?;

        // Close session after receiving output
        session.close().await?;

        match output {
            Some(output) => Ok(output),
            None => Err(crate::Error::Execution(
                "No output from pipeline".to_string(),
            )),
        }
    }

    /// Create a streaming session for multiple inputs/outputs.
    ///
    /// If a [`WarmSessionPool`] has been attached via
    /// [`Self::set_default_pool`], this method delegates to
    /// [`crate::transport::warm_session_pool::WarmSessionPool::acquire`],
    /// which returns a parked warm session if available and falls back to
    /// [`Self::cold_build_session`] otherwise. When no pool is attached
    /// (or the pool's last `Arc` has been dropped), this is equivalent
    /// to calling `cold_build_session` directly.
    ///
    /// # Arguments
    ///
    /// * `manifest` - Pipeline configuration
    ///
    /// # Returns
    ///
    /// A SessionHandle for sending inputs and receiving outputs
    pub async fn create_session(&self, manifest: Arc<Manifest>) -> Result<SessionHandle> {
        // Try the default pool first when one is attached. The Weak
        // upgrade returns None if the caller has dropped their last
        // Arc<WarmSessionPool> — fall back to cold-build cleanly in
        // that case (D2/D6 in design.md).
        let pool = self
            .default_pool
            .lock()
            .expect("default_pool mutex poisoned")
            .as_ref()
            .and_then(|weak| weak.upgrade());
        if let Some(pool) = pool {
            return pool.acquire(manifest).await;
        }
        self.cold_build_session(manifest).await
    }

    /// Get or create a pipeline session shared by multiple transports.
    ///
    /// The `key` is a logical room/session key chosen by the host. WebRTC,
    /// telephony, HTTP, gRPC, and FFI transports that use the same executor and
    /// key will share one core session and attach as participants.
    pub async fn get_or_create_shared_session(
        &self,
        key: impl Into<String>,
        manifest: Arc<Manifest>,
    ) -> Result<Arc<SharedPipelineSession>> {
        let key = key.into();
        if let Some(existing) = self.shared_sessions.read().await.get(&key).cloned() {
            if existing.is_active().await {
                return Ok(existing);
            }
        }

        let session_handle = self.create_session(manifest).await?;
        let shared = SharedPipelineSession::new(key.clone(), session_handle)?;

        let mut sessions = self.shared_sessions.write().await;
        if let Some(existing) = sessions.get(&key).cloned() {
            if existing.is_active().await {
                drop(sessions);
                shared.close().await?;
                return Ok(existing);
            }
        }

        sessions.insert(key, Arc::clone(&shared));
        Ok(shared)
    }

    /// Return an existing shared session by key.
    pub async fn shared_session(&self, key: &str) -> Option<Arc<SharedPipelineSession>> {
        self.shared_sessions.read().await.get(key).cloned()
    }

    /// Close and remove a shared session by key.
    pub async fn close_shared_session(&self, key: &str) -> Result<()> {
        let shared = self.shared_sessions.write().await.remove(key);
        if let Some(shared) = shared {
            shared.close().await?;
        }
        Ok(())
    }

    /// Join an existing shared pipeline session as a named participant.
    pub async fn join_shared_session(
        &self,
        key: &str,
        participant: Participant,
    ) -> Result<ParticipantSessionHandle> {
        let shared = self
            .shared_session(key)
            .await
            .ok_or_else(|| Error::Execution(format!("shared pipeline session not found: {key}")))?;
        Ok(shared.participant_handle(participant))
    }

    /// Join an already-running pipeline session as a named participant.
    ///
    /// This is the transport-neutral hook for "one pipeline, many clients":
    /// WebRTC peers, telephony call legs, gRPC streams, and control clients can
    /// all attach to the same `session_id` and send frames tagged with their
    /// participant identity/role.
    pub async fn join_session(
        &self,
        session_id: &str,
        participant: Participant,
    ) -> Result<ParticipantSessionHandle> {
        let control = self.control_bus.get(session_id).ok_or_else(|| {
            crate::Error::Execution(format!("pipeline session not found: {session_id}"))
        })?;
        let input_tx = control.input_sender().await.ok_or_else(|| {
            crate::Error::Execution(format!("pipeline session input closed: {session_id}"))
        })?;

        Ok(ParticipantSessionHandle::new(
            SessionInputSender::from_data_packet_sender(session_id, input_tx),
            participant,
        ))
    }

    /// Build a fresh session bypassing any attached warm pool. Used by
    /// [`Self::create_session`] when no pool is attached, and by
    /// `WarmSessionPool` to construct entries it parks. Visibility is
    /// `pub(crate)` so the future warm-pool module (in the same crate)
    /// can call it.
    pub(crate) async fn cold_build_session(
        &self,
        manifest: Arc<Manifest>,
    ) -> Result<SessionHandle> {
        // Resolve + load any plugin dependencies the manifest declares
        // BEFORE validation runs — otherwise unknown-node-type errors
        // would fire even though the plugin would have contributed
        // exactly that type. Idempotent: bundles cache by canonical path.
        self.ensure_plugins_loaded(&manifest).await?;
        // Validate manifest (cycles, unknown node types) then run capability
        // negotiation. The returned manifest may have FastResampleNode entries
        // spliced in by negotiation when `metadata.auto_negotiate` is set.
        self.validate_manifest(&manifest).await?;
        let manifest = self.negotiate_capabilities(manifest).await?;

        let session_id = self.generate_session_id();

        // Create kind-split output router. Each kind (Audio / Video /
        // Data) gets its own bounded channel — see
        // [`crate::transport::session_router::ClientOutputRouter`] for the
        // motivation. Each channel is sized at
        // `DEFAULT_ROUTER_OUTPUT_CAPACITY`; capacity is per-kind, so total
        // headroom is 3× the constant.
        let (output_router, output_receivers) =
            crate::transport::session_router::ClientOutputRouter::new(
                crate::transport::session_router::DEFAULT_ROUTER_OUTPUT_CAPACITY,
            );

        // Get a snapshot of the registry for the session
        let registry_snapshot = {
            let registry = self.registry.read().await;
            Arc::new(registry.clone())
        };

        // Create session router with scheduler config and drift thresholds
        let (mut router, shutdown_tx) = SessionRouter::with_config(
            session_id.clone(),
            manifest.clone(),
            registry_snapshot,
            output_router,
            Some(self.config.scheduler_config.clone()),
            if self.config.enable_drift_metrics {
                Some(self.config.drift_thresholds.clone())
            } else {
                None
            },
        )?;

        // Create and attach the per-session control bus. Must happen before
        // `start()` consumes the router's input_tx.
        let control = SessionControl::new(session_id.clone());
        router.attach_control(control.clone()).await;
        self.control_bus.register(control.clone());

        // Trace recorder: if `REMOTEMEDIA_RECORD_DIR` is set, attach
        // now so the taps are in place BEFORE the router starts —
        // otherwise we'd miss the first few frames. Failures log and
        // degrade to "no recording" (they must never take the
        // session out). The recorder handle is moved into
        // SessionHandle below so its lifetime matches the session.
        let recorder = crate::transport::session_recorder::SessionRecorder::maybe_attach_from_env(
            session_id.clone(),
            control.clone(),
            &manifest,
        )
        .await;

        // Get input sender before moving router
        let input_tx = router.get_input_sender();
        // Snapshot the perf aggregator handle before the router is
        // moved into the spawned task — performance tooling reads
        // merged HDR-histogram percentiles off this at end-of-run.
        let perf_aggregator =
            if crate::transport::perf_aggregator::PerfAggregator::enabled_from_env() {
                Some(router.perf_aggregator())
            } else {
                None
            };

        // Run node initialization SYNCHRONOUSLY before returning the
        // SessionHandle. Without this, `cold_build_session` would
        // return `Ok` immediately after spawning the router task,
        // and `initialize_nodes()` would run asynchronously inside
        // that task — meaning `WarmSessionPool::prewarm` (which
        // awaits `cold_build_session`) would also return `Ok` while
        // heavy nodes (LlamaCpp 27B GGUF, ONNX, Python multiprocess
        // spawn) were still loading, defeating the entire prewarm
        // contract.
        //
        // On init failure: unregister the half-attached control
        // entry from the process-wide bus so late attaches cleanly
        // see SessionNotFound. The router itself drops at the end
        // of this scope.
        if let Err(e) = router.initialize_nodes().await {
            self.control_bus.unregister(&session_id);
            return Err(e);
        }

        // Spawn router task. When the router exits, remove the session
        // entry from the bus so late attaches cleanly see SessionNotFound.
        // Note: we spawn `run_after_init` (not `run_public`) because
        // `initialize_nodes` already completed above.
        let bus = self.control_bus.clone();
        let unregister_sid = session_id.clone();
        let task_handle = tokio::spawn(async move {
            let result = router.run_after_init().await;
            if let Err(ref e) = result {
                tracing::error!(
                    session_id = %unregister_sid,
                    error = %e,
                    "Session router task exited with error"
                );
            }
            bus.unregister(&unregister_sid);
            result
        });

        let crate::transport::session_router::ClientOutputReceivers {
            audio_rx,
            video_rx,
            data_rx,
        } = output_receivers;
        Ok(SessionHandle {
            session_id,
            input_tx: Some(input_tx),
            output_audio_rx: Some(audio_rx),
            output_video_rx: Some(video_rx),
            output_data_rx: Some(data_rx),
            shutdown_tx,
            task_handle,
            is_active: true,
            _recorder: recorder,
            perf_aggregator,
        })
    }

    /// Get scheduler metrics in Prometheus format
    pub async fn prometheus_metrics(&self) -> String {
        self.scheduler.to_prometheus().await
    }

    /// Get scheduler statistics for all nodes
    pub async fn get_node_stats(
        &self,
    ) -> std::collections::HashMap<String, crate::executor::streaming_scheduler::NodeStats> {
        self.scheduler.get_all_node_stats().await
    }
}

impl Default for PipelineExecutor {
    fn default() -> Self {
        Self::new().expect("Failed to create default PipelineExecutor")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{Connection, ManifestMetadata, NodeManifest};

    fn create_test_manifest() -> Manifest {
        Manifest {
            version: "v1".to_string(),
            metadata: ManifestMetadata {
                name: "test-pipeline".to_string(),
                ..Default::default()
            },
            nodes: vec![NodeManifest {
                id: "test_node".to_string(),
                node_type: "PassthroughNode".to_string(),
                params: serde_json::json!({}),
                ..Default::default()
            }],
            connections: vec![],
            python_env: None,
            plugins: Vec::new(),
        }
    }

    /// Build a minimal [`Manifest`] using `CalculatorNode`, which is part of
    /// the default streaming registry installed by `PipelineExecutor::new()`.
    /// `CalculatorNode::new` does not validate params at construction time,
    /// so an empty params object is enough for `cold_build_session` to
    /// succeed without us actually pumping data through the pipeline.
    ///
    /// Returns an `Arc<Manifest>` directly — used by warm-pool delegation
    /// tests that need to share the manifest between executor and pool.
    fn create_test_manifest_arc() -> Arc<Manifest> {
        Arc::new(Manifest {
            version: "v1".to_string(),
            metadata: ManifestMetadata {
                name: "executor-warm-pool-test".to_string(),
                ..Default::default()
            },
            nodes: vec![NodeManifest {
                id: "calc".to_string(),
                node_type: "CalculatorNode".to_string(),
                params: serde_json::json!({}),
                ..Default::default()
            }],
            connections: vec![],
            python_env: None,
            plugins: Vec::new(),
        })
    }

    #[test]
    fn test_executor_config_default() {
        let config = ExecutorConfig::default();
        assert!(config.enable_drift_metrics);
        assert_eq!(config.session_id_prefix, "session");
    }

    #[test]
    fn test_executor_creation() {
        let executor = PipelineExecutor::new().unwrap();
        assert!(executor.scheduler().config.max_concurrency > 0);
    }

    #[test]
    fn test_session_id_generation() {
        let executor = PipelineExecutor::new().unwrap();
        let id1 = executor.generate_session_id();
        let id2 = executor.generate_session_id();

        assert_ne!(id1, id2);
        assert!(id1.starts_with("session_"));
    }

    #[test]
    fn test_executor_with_custom_config() {
        let config = ExecutorConfig {
            scheduler_config: SchedulerConfig::with_concurrency(16),
            enable_drift_metrics: false,
            session_id_prefix: "custom".to_string(),
            ..Default::default()
        };

        let executor = PipelineExecutor::with_config(config).unwrap();
        assert_eq!(executor.scheduler().config.max_concurrency, 16);

        let session_id = executor.generate_session_id();
        assert!(session_id.starts_with("custom_"));
    }

    #[tokio::test]
    async fn test_validate_manifest_unknown_node() {
        let executor = PipelineExecutor::new().unwrap();
        let manifest = create_test_manifest();

        // Should fail because PassthroughNode isn't registered
        let result = executor.validate_manifest(&manifest).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unknown node type"));
    }

    #[tokio::test]
    async fn test_validate_manifest_cycle_detection() {
        let executor = PipelineExecutor::new().unwrap();

        let manifest = Manifest {
            version: "v1".to_string(),
            metadata: ManifestMetadata {
                name: "cyclic-pipeline".to_string(),
                ..Default::default()
            },
            nodes: vec![
                NodeManifest {
                    id: "A".to_string(),
                    node_type: "TestNode".to_string(),
                    params: serde_json::json!({}),
                    ..Default::default()
                },
                NodeManifest {
                    id: "B".to_string(),
                    node_type: "TestNode".to_string(),
                    params: serde_json::json!({}),
                    ..Default::default()
                },
            ],
            connections: vec![
                Connection {
                    from: "A".to_string(),
                    to: "B".to_string(),
                    ..Default::default()
                },
                Connection {
                    from: "B".to_string(),
                    to: "A".to_string(),
                    ..Default::default()
                },
            ],
            python_env: None,
            plugins: Vec::new(),
        };

        let result = executor.validate_manifest(&manifest).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cycle"));
    }

    #[tokio::test]
    async fn test_registry_access() {
        let executor = PipelineExecutor::new().unwrap();

        // Registry should be accessible
        let registry = executor.registry();
        let reg_guard = registry.read().await;
        assert!(!reg_guard.has_node_type("NonExistentNode"));
    }

    #[tokio::test]
    async fn create_session_delegates_to_attached_pool() {
        use crate::transport::warm_session_pool::WarmSessionPool;
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let pool = WarmSessionPool::new(executor.clone());
        let manifest = create_test_manifest_arc();
        pool.prewarm(manifest.clone()).await.unwrap();
        assert_eq!(pool.pool_size(&*manifest).await, 1);

        executor.set_default_pool(pool.clone());

        // create_session should consume the warm entry — pool size drops
        let _session = executor.create_session(manifest.clone()).await.unwrap();
        assert_eq!(
            pool.pool_size(&*manifest).await,
            0,
            "create_session should consume the warm pool entry via delegation"
        );
    }

    #[tokio::test]
    async fn create_session_cold_builds_when_pool_dropped() {
        use crate::transport::warm_session_pool::WarmSessionPool;
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let pool = WarmSessionPool::new(executor.clone());
        let manifest = create_test_manifest_arc();
        executor.set_default_pool(pool.clone());

        // Drop the only Arc<WarmSessionPool> — Weak upgrade now returns None
        drop(pool);

        let _session = executor.create_session(manifest).await.unwrap();
        // No assertion needed beyond "didn't panic, returned Ok" — the
        // executor must fall back to cold-build cleanly when the pool
        // is gone. (If the upgrade were unwrapped instead of pattern-matched,
        // this test would panic.)
    }

    #[tokio::test]
    async fn join_session_returns_participant_handles_for_same_pipeline_session() {
        let executor = PipelineExecutor::new().unwrap();
        let manifest = create_test_manifest_arc();
        let mut session = executor.create_session(manifest).await.unwrap();
        let session_id = session.session_id.clone();

        let alice = executor
            .join_session(
                &session_id,
                Participant::new("alice", crate::transport::data::participant::role::USER)
                    .with_track_id("alice-mic")
                    .with_modality(crate::transport::data::participant::modality::AUDIO),
            )
            .await
            .unwrap();
        let bob = session
            .join_participant(
                Participant::new("bob", crate::transport::data::participant::role::CLIENT)
                    .with_track_id("bob-mic")
                    .with_modality(crate::transport::data::participant::modality::AUDIO),
            )
            .unwrap();

        assert_eq!(alice.session_id(), session_id);
        assert_eq!(bob.session_id(), session_id);
        assert_eq!(alice.participant().id, "alice");
        assert_eq!(bob.participant().role, "client");

        session.close().await.unwrap();
    }

    #[tokio::test]
    async fn shared_sessions_reuse_key_and_broadcast_outputs() {
        let executor = PipelineExecutor::new().unwrap();
        let manifest = Arc::new(Manifest {
            version: "v1".to_string(),
            metadata: ManifestMetadata {
                name: "shared-session-test".to_string(),
                ..Default::default()
            },
            nodes: vec![NodeManifest {
                id: "pass".to_string(),
                node_type: "PassThrough".to_string(),
                params: serde_json::json!({}),
                ..Default::default()
            }],
            connections: vec![],
            python_env: None,
            plugins: Vec::new(),
        });

        let shared = executor
            .get_or_create_shared_session("room-a", Arc::clone(&manifest))
            .await
            .unwrap();
        let same = executor
            .get_or_create_shared_session("room-a", manifest)
            .await
            .unwrap();

        assert_eq!(shared.session_id(), same.session_id());
        assert_eq!(shared.key(), "room-a");

        let mut first_outputs = shared.subscribe_outputs();
        let mut second_outputs = same.subscribe_outputs();
        let participant = Participant::new(
            "webrtc-client",
            crate::transport::data::participant::role::CLIENT,
        )
        .with_track_id("webrtc-client:data")
        .with_modality(crate::transport::data::participant::modality::CONTROL);
        shared
            .participant_handle(participant)
            .send(TransportData::new(RuntimeData::Text("hello".to_string())))
            .await
            .unwrap();

        let first = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            first_outputs.recv_output(),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        let second = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            second_outputs.recv_output(),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();

        assert!(matches!(first.data, RuntimeData::Text(ref text) if text == "hello"));
        assert!(matches!(second.data, RuntimeData::Text(ref text) if text == "hello"));

        shared.close().await.unwrap();
    }

    #[tokio::test]
    async fn clear_default_pool_detaches_without_dropping_pool() {
        use crate::transport::warm_session_pool::WarmSessionPool;
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let pool = WarmSessionPool::new(executor.clone());
        let manifest = create_test_manifest_arc();
        pool.prewarm(manifest.clone()).await.unwrap();
        assert_eq!(pool.pool_size(&*manifest).await, 1);

        executor.set_default_pool(pool.clone());
        executor.clear_default_pool();

        // create_session should NOT consume the pool entry — delegation is detached
        let _session = executor.create_session(manifest.clone()).await.unwrap();
        assert_eq!(
            pool.pool_size(&*manifest).await,
            1,
            "clear_default_pool should detach delegation; pool entry remains"
        );

        // Pool is still usable directly
        let _session = pool.acquire(manifest.clone()).await.unwrap();
        assert_eq!(
            pool.pool_size(&*manifest).await,
            0,
            "direct pool.acquire still works after clear_default_pool"
        );
    }
}
