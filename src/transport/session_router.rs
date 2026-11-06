//! Session-level Async Router for Pipeline Streaming
//!
//! This module provides a graph-aware routing layer that processes data through
//! multi-node pipelines with proper topological ordering, fan-in/fan-out support,
//! and cycle detection.
//!
//! # Architecture
//!
//! The SessionRouter sits between transport implementations and the node execution
//! layer. It:
//!
//! 1. Builds a `PipelineGraph` at session creation (validates connections, detects cycles)
//! 2. Executes nodes in topological order
//! 3. Routes data between nodes based on manifest connections
//! 4. Sends outputs from terminal nodes (sinks) to the client
//!
//! # Usage
//!
//! Transport implementations should use `SessionRouter` instead of implementing
//! their own routing logic:
//!
//! ```ignore
//! use remotemedia_core::transport::SessionRouter;
//!
//! let router = SessionRouter::new(
//!     session_id,
//!     manifest,
//!     streaming_registry,
//!     output_tx,
//! )?;
//!
//! // Start the router (runs until shutdown)
//! let handle = router.start();
//!
//! // Feed data through the pipeline
//! router.send_input(input_data).await?;
//! ```

mod snapshot_bindings;
use snapshot_bindings::SnapshotBindings;
mod tasks;
use tasks::TaskGroup;
mod children;
use children::Children;
#[cfg(test)]
mod task_failure_tests;

use crate::capabilities::{CapabilityBehavior, CapabilityResolver, ResolutionContext};
use crate::data::RuntimeData;
use crate::executor::{
    DriftMetrics, DriftThresholds, NodeStats, PipelineGraph, SchedulerConfig, StreamingScheduler,
};
use crate::manifest::Manifest;
use crate::nodes::{InitializeContext, StreamingNode, StreamingNodeRegistry};
use crate::telemetry::{approx_payload_bytes, QueueCapacity, SessionDescriptor, SessionTelemetry};
use crate::transport::perf_aggregator::PerfAggregator;
use crate::transport::session_control::{aux_port_of, CloseReason, SessionControl, BARGE_IN_PORT};
use crate::{Error, Result};
use parking_lot::RwLock as DriftRwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Default capacity for the router's internal input channel.
///
/// Sized for ~160 ms of headroom at 48 kHz / 20 ms frames (8 frames × 20 ms).
/// Bounded channels apply block-producer backpressure on the transport side,
/// which is the desired behavior for real-time media pipelines — we'd rather
/// stall the ingress than grow memory unboundedly.
///
/// Override at session creation via `REMOTEMEDIA_ROUTER_INPUT_CAPACITY`.
pub const DEFAULT_ROUTER_INPUT_CAPACITY: usize = 8;

/// Default capacity for per-session client output channels.
///
/// Callers (`PipelineExecutor::create_session`, transport streaming handlers)
/// create the output channel and pass the sender in; this constant is the
/// recommended default. Same sizing rationale as the input capacity.
pub const DEFAULT_ROUTER_OUTPUT_CAPACITY: usize = 256;

/// Kind-split client output: one bounded channel per `RuntimeData` kind
/// (Audio, Video, everything-else-as-data).
///
/// **Why this exists.** A single shared `mpsc::Sender<RuntimeData>` for sink
/// outputs serialises every Audio, Video, and Json/Text frame through one
/// FIFO. When the consumer is slow on one kind (e.g. CPU h264 encode at
/// ~16 fps versus a 30 fps source) the FIFO backs up and Audio sits behind
/// Video for tens of seconds. Splitting by kind gives each path its own
/// queue so a slow video drain can't head-of-line-block audio. Within a
/// kind, ordering across stream_ids is still FIFO; the per-stream
/// dispatcher in `webrtc/server_peer.rs` handles the within-kind split.
#[derive(Clone)]
pub struct ClientOutputRouter {
    audio_tx: mpsc::Sender<RuntimeData>,
    video_tx: mpsc::Sender<RuntimeData>,
    data_tx: mpsc::Sender<RuntimeData>,
}

/// Owning consumer end of a [`ClientOutputRouter`]: three receivers, one per
/// kind. Kept together because they're created as a unit and almost always
/// handed to the same consumer (e.g. `SessionHandle`, the WebRTC peer's
/// per-kind drainers).
pub struct ClientOutputReceivers {
    pub audio_rx: mpsc::Receiver<RuntimeData>,
    pub video_rx: mpsc::Receiver<RuntimeData>,
    pub data_rx: mpsc::Receiver<RuntimeData>,
}

impl ClientOutputRouter {
    /// Create a new router with all three kind channels sized at `capacity`.
    /// Returns the cloneable sender side and the matching receiver triple.
    pub fn new(capacity: usize) -> (Self, ClientOutputReceivers) {
        let (audio_tx, audio_rx) = mpsc::channel(capacity);
        let (video_tx, video_rx) = mpsc::channel(capacity);
        let (data_tx, data_rx) = mpsc::channel(capacity);
        (
            Self {
                audio_tx,
                video_tx,
                data_tx,
            },
            ClientOutputReceivers {
                audio_rx,
                video_rx,
                data_rx,
            },
        )
    }

    /// Route one piece of sink output to the channel for its kind.
    ///
    /// Awaits when that kind's channel is full — backpressure is per-kind,
    /// so a stalled video consumer no longer holds back audio.
    pub async fn send(
        &self,
        data: RuntimeData,
    ) -> std::result::Result<(), mpsc::error::SendError<RuntimeData>> {
        match &data {
            RuntimeData::Audio { .. } => self.audio_tx.send(data).await,
            RuntimeData::Video { .. } => self.video_tx.send(data).await,
            _ => self.data_tx.send(data).await,
        }
    }

    /// Non-blocking variant of [`Self::send`].
    pub fn try_send(
        &self,
        data: RuntimeData,
    ) -> std::result::Result<(), mpsc::error::TrySendError<RuntimeData>> {
        match &data {
            RuntimeData::Audio { .. } => self.audio_tx.try_send(data),
            RuntimeData::Video { .. } => self.video_tx.try_send(data),
            _ => self.data_tx.try_send(data),
        }
    }
}

/// Read the input-channel capacity from the environment, falling back to
/// [`DEFAULT_ROUTER_INPUT_CAPACITY`].
fn input_capacity_from_env() -> usize {
    std::env::var("REMOTEMEDIA_ROUTER_INPUT_CAPACITY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_ROUTER_INPUT_CAPACITY)
}

fn node_trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("REMOTEMEDIA_NODE_TRACE")
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    })
}

fn describe_runtime_data(data: &RuntimeData) -> String {
    fn push_json_field(parts: &mut Vec<String>, value: &serde_json::Value, key: &str) {
        if let Some(field) = value.get(key) {
            let rendered = match field {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                _ => return,
            };
            parts.push(format!("{key}={rendered}"));
        }
    }

    fn push_json_fields(parts: &mut Vec<String>, value: &serde_json::Value, prefix: Option<&str>) {
        let keys = [
            "kind",
            "type",
            "event",
            "is_speech",
            "is_speech_start",
            "is_speech_end",
            "speech_prob",
            "probability",
            "score",
        ];

        for key in keys {
            let before = parts.len();
            push_json_field(parts, value, key);
            if let Some(prefix) = prefix {
                if parts.len() > before {
                    if let Some(last) = parts.last_mut() {
                        *last = format!("{prefix}.{last}");
                    }
                }
            }
        }
    }

    match data {
        RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            stream_id,
            timestamp_us,
            ..
        } => format!(
            "audio samples={} rate={}Hz channels={} stream={} ts_us={}",
            samples.len(),
            sample_rate,
            channels,
            stream_id.as_deref().unwrap_or("default"),
            timestamp_us
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_string())
        ),
        RuntimeData::Video {
            pixel_data,
            width,
            height,
            frame_number,
            timestamp_us,
            ..
        } => format!(
            "video {}x{} bytes={} frame={} ts_us={}",
            width,
            height,
            pixel_data.len(),
            frame_number,
            timestamp_us
        ),
        RuntimeData::Text(text) => format!("text chars={}", text.chars().count()),
        RuntimeData::Json(value) => {
            let mut parts = Vec::new();
            push_json_fields(&mut parts, value, None);

            for envelope_key in ["payload", "data", "event"] {
                if let Some(nested) = value.get(envelope_key) {
                    push_json_fields(&mut parts, nested, Some(envelope_key));
                }
            }

            if parts.is_empty() {
                if let Some(object) = value.as_object() {
                    let keys = object
                        .keys()
                        .take(8)
                        .map(String::as_str)
                        .collect::<Vec<_>>()
                        .join(",");
                    format!("json keys=[{keys}]")
                } else {
                    "json value".to_string()
                }
            } else {
                format!("json {}", parts.join(" "))
            }
        }
        other => other.data_type().to_string(),
    }
}

/// Data packet flowing through the pipeline
#[derive(Clone, Debug)]
pub struct DataPacket {
    /// The actual data
    pub data: RuntimeData,
    /// Source node ID (where this data came from)
    pub from_node: String,
    /// Target node ID (optional - for direct routing from client to specific node)
    pub to_node: Option<String>,
    /// Session ID
    pub session_id: String,
    /// Sequence number for ordering
    pub sequence: u64,
    /// Sub-sequence for streaming outputs (multiple outputs per input)
    pub sub_sequence: u64,
    /// Transport envelope metadata preserved until router ingress.
    pub metadata: std::collections::HashMap<String, String>,
}

/// Session-persistent router that processes data through the pipeline graph
///
/// This router:
/// - Validates the pipeline graph at creation (cycle detection, missing nodes)
/// - Caches node instances for the session lifetime
/// - Executes nodes in topological order
/// - Handles fan-in (multiple inputs) and fan-out (multiple outputs)
/// - Only sends outputs from sink nodes to the client
/// - Integrates StreamingScheduler for timeout, retry, and circuit breaker (spec 026)
/// - Tracks per-stream drift metrics for health monitoring (spec 026)
pub struct SessionRouter {
    bound_output_allocators: HashMap<String, Arc<remotemedia_types::BoundOutputAllocator>>,
    buffer_contracts: Option<
        std::collections::BTreeMap<
            String,
            Option<remotemedia_traits::buffer_contract::BufferContract>,
        >,
    >,
    node_materialization: super::NodeMaterializationCopies,
    /// Storage selected for producer outputs in this session. Nodes allocate
    /// through their context; routing never converts existing private buffers.
    output_allocator: Arc<dyn remotemedia_types::OutputAllocator>,
    /// Session ID
    session_id: String,

    /// Pipeline manifest
    manifest: Arc<Manifest>,

    /// Pipeline graph (topological order, sources, sinks)
    graph: PipelineGraph,

    /// Registry for creating nodes
    registry: Arc<StreamingNodeRegistry>,

    /// Cached node instances (created once per session)
    cached_nodes: HashMap<String, Box<dyn StreamingNode>>,
    snapshot_bindings: Option<SnapshotBindings>,

    /// Kind-split sink output. See [`ClientOutputRouter`] for why we don't
    /// share a single FIFO across Audio/Video/Json. Cloneable; sinks each
    /// hold a clone via their fan-out drain task.
    output_router: ClientOutputRouter,

    /// Channel to receive inputs from client.
    ///
    /// Bounded — see [`DEFAULT_ROUTER_INPUT_CAPACITY`].
    ///
    /// Wrapped in `std::sync::Mutex` so the router struct stays `Sync`
    /// once it is shared across per-packet spawn tasks (`tokio::spawn`
    /// requires `Arc<Self>: Send`, which requires `Self: Sync`). The
    /// receiver is `take()`-n once at the top of `run()` and used
    /// locally from then on, so lock contention is irrelevant.
    input_rx: std::sync::Mutex<Option<mpsc::Receiver<DataPacket>>>,

    /// Channel to send inputs to router (held by external code, dropped in run()).
    input_tx: Option<mpsc::Sender<DataPacket>>,

    /// Shutdown signal receiver. See [`Self::input_rx`] for why the
    /// receiver is wrapped in a `std::sync::Mutex`.
    shutdown_rx: std::sync::Mutex<Option<mpsc::Receiver<()>>>,

    /// Shutdown signal sender (held externally)
    _shutdown_tx: mpsc::Sender<()>,

    /// Capability resolution context (spec 025)
    /// Stores pending capability updates for downstream nodes
    resolution_ctx: Option<ResolutionContext>,

    /// StreamingScheduler for node execution with timeout, retry, circuit breaker (spec 026)
    scheduler: Arc<StreamingScheduler>,

    /// Per-stream drift metrics for health monitoring (spec 026).
    ///
    /// Map is `DashMap` (lock-free sharded hashmap). Phase B1 swaps
    /// the inner `tokio::sync::RwLock` for `parking_lot::RwLock` —
    /// no `.await` is ever held across the lock (writes finish in
    /// `record_sample` before any await), so the async RwLock was
    /// pure overhead on the per-packet path. `parking_lot::RwLock`
    /// uncontended fast path is a single CAS.
    ///
    /// Key: stream_id (from RuntimeData.stream_id or "default").
    drift_metrics: Arc<dashmap::DashMap<String, Arc<DriftRwLock<DriftMetrics>>>>,

    /// Real-time latency probes (Phase 0 of the tokio-off-data-plane
    /// migration). Records into `ingress` and `egress` today; `route_in`,
    /// `node_in`, `node_out` are reserved for follow-up wiring inside
    /// `process_input`.
    ///
    /// Accessed out-of-band via [`Self::probe_snapshots`]. Shared via
    /// `Arc` because consumers may want to poll from a separate task.
    probes: Arc<crate::metrics::RtProbeSet>,

    /// Drift thresholds for new streams
    drift_thresholds: DriftThresholds,

    /// Optional per-session control bus (client-side pub/sub/intercept).
    /// Attached via [`Self::attach_control`] after construction, before
    /// `start()`. When `None`, the router's hot path skips the control
    /// hook entirely — no overhead for sessions without attaches.
    control: Option<Arc<SessionControl>>,

    /// Per-session performance aggregator (slice 1 of the perf-tap
    /// feature). Records dispatch-site I/O timings into HDR
    /// histograms; a periodic flush task publishes a `PerfSnapshot`
    /// on the `__perf__` tap. Disabled by default — set
    /// `REMOTEMEDIA_PERF_TAP=1` to enable. When disabled, every
    /// `record_input`/`record_output` is an inline atomic-load + early
    /// return, so production pays no cost.
    perf: Arc<PerfAggregator>,

    /// Running-topology descriptor (openspec: add-live-pipeline-observer).
    /// Refined after node init (pacing) and pushed to the control bus.
    descriptor: SessionDescriptor,

    /// Lock-free per-node / per-edge counters, shared with node tasks and
    /// the telemetry publisher.
    telemetry: Arc<SessionTelemetry>,
}

/// Bounded capacity of each node's input channel.
///
/// Sized the same as the router's own ingress: enough to absorb a small burst
/// without drops, not so large that a stalled node hides backpressure. If a
/// node is a bottleneck, we want the upstream producer to block at `send`
/// rather than silently queuing megabytes of buffered audio.
const NODE_INPUT_CAPACITY: usize = 16;

/// Bounded capacity of each node's internal fan-out (callback → drain task).
///
/// Larger than the input because one input packet can produce many output
/// chunks (e.g. a TTS sentence → ~20 audio frames). `try_send` overflow
/// drops a chunk with a warning; sizing this generously avoids drops under
/// normal TTS/VAD burst patterns while still bounding memory growth.
const NODE_FANOUT_CAPACITY: usize = 1024;

const RELAXED: std::sync::atomic::Ordering = std::sync::atomic::Ordering::Relaxed;

/// Per-node task handles + input sender map, owned by the router for the
/// lifetime of the session. Built in [`SessionRouter::spawn_pipeline_tasks`]
/// and torn down in [`SessionRouter::teardown_pipeline_tasks`].
struct PipelineTasks {
    /// Input sender for each node (keyed by node id). The router pushes
    /// source-bound and `to_node`-addressed packets through these.
    input_txs: HashMap<String, mpsc::Sender<RuntimeData>>,
    /// Telemetry ingress edge index for each source node.
    ingress_edges: HashMap<String, usize>,
    /// All spawned tasks (main + fan-out per node). Awaited on shutdown.
    tasks: TaskGroup,
}

/// One stream successor of a node: its input channel plus the telemetry edge
/// index of the connection feeding it.
struct Successor {
    tx: mpsc::Sender<RuntimeData>,
    edge: Option<usize>,
}

/// Everything a node's tasks need to update telemetry.
struct NodeTelemetryHandle {
    tel: Arc<SessionTelemetry>,
    idx: usize,
    /// Egress edge (sink → client), when this node forwards to the client.
    egress_edge: Option<usize>,
    /// Weak handle on the node's router-side input channel (queue probe).
    input_weak: mpsc::WeakSender<RuntimeData>,
}

impl SessionRouter {
    /// Create a new session router
    ///
    /// # Arguments
    ///
    /// * `session_id` - Unique identifier for this session
    /// * `manifest` - Pipeline manifest defining nodes and connections
    /// * `registry` - Registry for creating streaming nodes
    /// * `output_router` - Kind-split client output (Audio/Video/Data). See
    ///   [`ClientOutputRouter::new`] — callers typically pair it with a
    ///   [`ClientOutputReceivers`] held by the consumer-side `SessionHandle`.
    ///
    /// # Returns
    ///
    /// * `Ok((router, shutdown_tx))` - Router and shutdown signal sender
    /// * `Err` - Graph validation failed (cycles, missing nodes, etc.)
    pub fn new(
        session_id: String,
        manifest: Arc<Manifest>,
        registry: Arc<StreamingNodeRegistry>,
        output_router: ClientOutputRouter,
    ) -> Result<(Self, mpsc::Sender<()>)> {
        Self::with_config(session_id, manifest, registry, output_router, None, None)
    }

    /// Create a new session router with optional scheduler and drift threshold configuration
    ///
    /// # Arguments
    ///
    /// * `session_id` - Unique identifier for this session
    /// * `manifest` - Pipeline manifest defining nodes and connections
    /// * `registry` - Registry for creating streaming nodes
    /// * `output_router` - Kind-split client output ([`ClientOutputRouter`]).
    /// * `scheduler_config` - Optional scheduler configuration (uses defaults if None)
    /// * `drift_thresholds` - Optional drift threshold configuration (uses defaults if None)
    ///
    /// # Returns
    ///
    /// * `Ok((router, shutdown_tx))` - Router and shutdown signal sender
    /// * `Err` - Graph validation failed (cycles, missing nodes, etc.)
    pub fn with_config(
        session_id: String,
        manifest: Arc<Manifest>,
        registry: Arc<StreamingNodeRegistry>,
        output_router: ClientOutputRouter,
        scheduler_config: Option<SchedulerConfig>,
        drift_thresholds: Option<DriftThresholds>,
    ) -> Result<(Self, mpsc::Sender<()>)> {
        // Build and validate the pipeline graph
        let graph = PipelineGraph::from_manifest(&manifest)?;
        tracing::info!(
            "Session {}: Built pipeline graph with {} nodes, execution_order: {:?}, sources: {:?}, sinks: {:?}",
            session_id,
            graph.node_count(),
            graph.execution_order,
            graph.sources,
            graph.sinks
        );

        // Create input/shutdown channels.
        //
        // The input channel is bounded so that transport ingress applies
        // backpressure to the upstream (gRPC/WebRTC/etc.) when the pipeline
        // falls behind. Capacity is configurable via
        // `REMOTEMEDIA_ROUTER_INPUT_CAPACITY` (default 8 frames ≈ 160 ms at
        // 48 kHz/20 ms) — see [`DEFAULT_ROUTER_INPUT_CAPACITY`].
        let (input_tx, input_rx) = mpsc::channel(input_capacity_from_env());
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let shutdown_tx_clone = shutdown_tx.clone();

        // Create scheduler config, extracting fast_path nodes from manifest
        let mut config = scheduler_config.unwrap_or_default();
        for node_def in &manifest.nodes {
            if node_def.fast_path {
                config.fast_path_nodes.insert(node_def.id.clone());
                tracing::debug!(
                    "Session {}: Node '{}' configured for fast path execution",
                    session_id,
                    node_def.id
                );
            }
        }

        // Create scheduler with config
        let scheduler = Arc::new(StreamingScheduler::new(config));

        let perf = Arc::new(PerfAggregator::with_sample_stride(
            session_id.clone(),
            PerfAggregator::enabled_from_env(),
            PerfAggregator::window_ms_from_env(),
            PerfAggregator::sample_stride_from_env(),
        ));

        let descriptor = SessionDescriptor::build(
            &session_id,
            &manifest,
            &graph,
            &registry,
            QueueCapacity {
                input: NODE_INPUT_CAPACITY as u32,
                filter: NODE_FANOUT_CAPACITY as u32,
                fanout: NODE_FANOUT_CAPACITY as u32,
            },
        );
        let telemetry = Arc::new(SessionTelemetry::new(
            &descriptor,
            scheduler.clone(),
            perf.clone(),
        ));

        let router = Self {
            bound_output_allocators: HashMap::new(),
            buffer_contracts: None,
            node_materialization: super::NodeMaterializationCopies::new(
                &session_id,
                manifest.nodes.iter().map(|node| node.id.as_str()),
                remotemedia_types::CopyPolicy::AllowCopies,
            ),
            output_allocator: Arc::new(remotemedia_types::HeapOutputAllocator),
            session_id,
            manifest,
            graph,
            registry,
            cached_nodes: HashMap::new(),
            snapshot_bindings: None,
            output_router,
            input_rx: std::sync::Mutex::new(Some(input_rx)),
            input_tx: Some(input_tx),
            shutdown_rx: std::sync::Mutex::new(Some(shutdown_rx)),
            _shutdown_tx: shutdown_tx,
            resolution_ctx: None,
            scheduler,
            drift_metrics: Arc::new(dashmap::DashMap::new()),
            probes: Arc::new(crate::metrics::RtProbeSet::new()),
            drift_thresholds: drift_thresholds.unwrap_or_default(),
            control: None,
            perf,
            descriptor,
            telemetry,
        };

        Ok((router, shutdown_tx_clone))
    }

    /// Attach a [`SessionControl`] bus to this router.
    ///
    /// After this call, the control can:
    ///   - see every node output via `on_node_output` (tap + intercept)
    ///   - inject inputs via `publish` (the bus forwards to the router's
    ///     own input channel, with `to_node` set).
    ///
    /// Must be called before `start()` / `run()`. Safe to skip entirely —
    /// a router with `control = None` has zero control-bus overhead.
    pub async fn attach_control(&mut self, control: Arc<SessionControl>) {
        let input_tx = self
            .input_tx
            .clone()
            .expect("attach_control must be called before run() consumes input_tx");
        control.attach_input_sender(input_tx).await;
        control.set_descriptor(Arc::new(self.descriptor.clone()));
        control.set_telemetry(self.telemetry.clone());
        self.control = Some(control);
    }

    /// Select output storage before starting the session. This does not adapt
    /// existing input buffers or make legacy node allocations shareable.
    pub fn set_output_allocator(&mut self, allocator: Arc<dyn remotemedia_types::OutputAllocator>) {
        self.output_allocator = allocator;
    }

    pub(crate) fn set_buffer_contracts(
        &mut self,
        report: &super::buffer_preflight::BufferPreflightReport,
    ) {
        self.buffer_contracts = Some(report.nodes.clone());
    }

    fn validate_effective_buffer_contract(
        &self,
        node: &crate::manifest::NodeManifest,
        params: &serde_json::Value,
    ) -> Result<()> {
        if let Some(contracts) = &self.buffer_contracts {
            let expected = contracts.get(&node.id).ok_or_else(|| {
                Error::ConfigError(format!("missing buffer preflight for node '{}'", node.id))
            })?;
            let actual = self
                .registry
                .get_factory(&node.node_type)
                .and_then(|factory| factory.buffer_contract(params));
            if &actual != expected {
                return Err(Error::ConfigError(format!("buffer contract node '{}': effective runtime parameters changed the preflight declaration", node.id)));
            }
        }
        Ok(())
    }

    /// Select explicit host-node materialization policy before starting execution.
    /// The returned ledgers survive session shutdown. This does not certify
    /// foreign adapters, arbitrary node internals or whole-graph zero-copy.
    pub fn set_node_materialization_policy(
        &mut self,
        policy: remotemedia_types::CopyPolicy,
    ) -> super::NodeMaterializationCopies {
        self.node_materialization = super::NodeMaterializationCopies::new(
            &self.session_id,
            self.manifest.nodes.iter().map(|node| node.id.as_str()),
            policy,
        );
        self.node_materialization.clone()
    }

    /// Flag nodes that were spliced into the manifest by capability
    /// negotiation (or other manifest rewriting) so observers can tell them
    /// apart from author-written nodes.
    pub fn mark_inserted_nodes(&mut self, ids: &std::collections::HashSet<String>) {
        self.descriptor.mark_inserted(ids);
        if let Some(ctrl) = &self.control {
            ctrl.set_descriptor(Arc::new(self.descriptor.clone()));
        }
    }

    /// The session's running-topology descriptor.
    pub fn descriptor(&self) -> &SessionDescriptor {
        &self.descriptor
    }

    /// The session's telemetry handle. Clone before `run*` consumes the
    /// router to read counters out-of-band.
    pub fn telemetry(&self) -> Arc<SessionTelemetry> {
        self.telemetry.clone()
    }

    /// Get the input sender for feeding data to the router.
    ///
    /// The returned sender is bounded; callers should `.send(...).await`
    /// and treat the back-pressure as real (drop-on-deadline is a policy
    /// decision for the transport ingress, not the router).
    pub fn get_input_sender(&self) -> mpsc::Sender<DataPacket> {
        self.input_tx
            .clone()
            .expect("input_tx not yet consumed by run()")
    }

    /// Get the pipeline graph
    pub fn graph(&self) -> &PipelineGraph {
        &self.graph
    }

    /// Get the execution order
    pub fn execution_order(&self) -> &[String] {
        &self.graph.execution_order
    }

    /// Get the source nodes (nodes with no inputs)
    pub fn sources(&self) -> &[String] {
        &self.graph.sources
    }

    /// Get the sink nodes (nodes with no outputs)
    pub fn sinks(&self) -> &[String] {
        &self.graph.sinks
    }

    /// Get the per-session performance aggregator. Lets performance
    /// tooling call [`PerfAggregator::peek_snapshot`] at the end of a
    /// run to capture an entire execution window in one merged HDR
    /// histogram (instead of trying to combine percentile-of-percentiles
    /// across the periodic flush task's 1 s windows).
    pub fn perf_aggregator(&self) -> Arc<PerfAggregator> {
        self.perf.clone()
    }

    /// Shape the exact parameters factories receive, including resolved Python
    /// environment metadata. Buffer preflight must be rechecked on this value.
    fn effective_node_parameters(&self, node: &crate::manifest::NodeManifest) -> serde_json::Value {
        let mut params = node.params.clone();
        let Some(obj) = params.as_object_mut() else {
            return params;
        };
        if let Some(deps) = &node.python_deps {
            obj.insert("__python_deps__".into(), serde_json::json!(deps));
        }
        if let Some(env) = &self.manifest.python_env {
            if !env.extra_deps.is_empty() {
                obj.insert(
                    "__python_extra_deps__".into(),
                    serde_json::json!(env.extra_deps),
                );
            }
            if let Some(version) = &env.python_version {
                obj.insert("__python_version__".into(), serde_json::json!(version));
            }
            // A materialized frozen venv overrides dependency resolution.
            if let Some(venv) = env.explicit_venv.as_ref().filter(|path| !path.is_empty()) {
                obj.insert("__python_explicit_venv__".into(), serde_json::json!(venv));
            }
            // Preserve the frozen wheelhouse for offline ABI-compatible installs.
            if !env.find_links.is_empty() {
                obj.insert(
                    "__python_find_links__".into(),
                    serde_json::json!(env.find_links),
                );
            }
        }
        let scope = node
            .python_env
            .as_ref()
            .and_then(|env| env.scope.as_ref())
            .or_else(|| {
                self.manifest
                    .python_env
                    .as_ref()
                    .and_then(|env| env.scope.as_ref())
            });
        if let Some(scope) = scope {
            obj.insert(
                "__python_env_scope__".into(),
                serde_json::to_value(scope).unwrap_or_else(|_| serde_json::Value::Null),
            );
            use crate::python::env_manager::EnvScope;
            let context = match scope {
                EnvScope::Global => None,
                EnvScope::PerPipeline => Some(format!("session:{}", self.session_id)),
                EnvScope::PerNode => Some(format!(
                    "session:{};node:{};type:{}",
                    self.session_id, node.id, node.node_type
                )),
            };
            if let Some(context) = context {
                obj.insert(
                    "__python_env_scope_context__".into(),
                    serde_json::json!(context),
                );
            }
        }
        params
    }

    /// Initialize all nodes in the pipeline
    ///
    /// This pre-creates and caches all nodes before streaming starts,
    /// eliminating cold-start latency.
    pub async fn initialize_nodes(&mut self) -> Result<()> {
        use futures::future::try_join_all;
        self.snapshot_bindings = None;
        self.bound_output_allocators.clear();

        let total = self.manifest.nodes.len();
        tracing::info!(
            "Session {}: Initializing {} nodes (parallel)",
            self.session_id,
            total
        );
        self.emit_loading_event("initializing", Some(format!("Starting {} nodes", total)));

        // Validate every effective declaration before creating the first node.
        let prepared = self
            .manifest
            .nodes
            .iter()
            .map(|node_spec| {
                let params = self.effective_node_parameters(node_spec);
                self.validate_effective_buffer_contract(node_spec, &params)?;
                Ok((node_spec, params))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut created: Vec<(String, String, Box<dyn StreamingNode>)> = Vec::with_capacity(total);
        for (node_spec, params) in prepared {
            let mut node = self.registry.create_node(
                &node_spec.node_type,
                node_spec.id.clone(),
                &params,
                Some(self.session_id.clone()),
            )?;
            if let Some(contract) = self
                .buffer_contracts
                .as_ref()
                .and_then(|contracts| contracts.get(&node_spec.id))
                .and_then(Option::as_ref)
            {
                let allocator = Arc::new(remotemedia_types::BoundOutputAllocator::new(
                    self.output_allocator.clone(),
                ));
                self.bound_output_allocators
                    .insert(node_spec.id.clone(), allocator.clone());
                node = Box::new(super::buffer_validation::BufferCheckedNode::new(
                    node_spec.id.clone(),
                    contract.clone(),
                    node,
                    Some(allocator),
                ));
            }
            created.push((node_spec.id.clone(), node_spec.node_type.clone(), node));
        }

        // Phase 2: emit `loading_node` for every node up front so the
        // UI sees the full initialising set immediately. With parallel
        // init the order in which they finish is non-deterministic, so
        // observers must track per-node lifecycle, not a singleton
        // "currently loading" pointer.
        for (id, ty, _) in &created {
            self.emit_node_loading_event("loading_node", id, &format!("Loading {} ({})", id, ty));
        }

        // Phase 3: drive every node's initialize() concurrently.
        //
        // Why this is a big win: each Python multiprocess node spawns
        // its own OS process and waits independently for its
        // post-init READY signal (see node._publish_ready_signal).
        // Sequential init time = sum(individual); parallel = max. For
        // the avatar pipeline that turns ~150s of cold start into
        // ~140s gated entirely on Kimodo, with whisper / kokoro /
        // audio2face folded inside that window for free.
        //
        // try_join_all short-circuits on the first failure. Other
        // futures are dropped, which drops their owned
        // Box<dyn StreamingNode>; for Python multiprocess nodes that
        // chains into ProcessManager::Drop and SIGKILLs the spawned
        // child, so we don't leak partially-initialised processes.
        let session_id = self.session_id.clone();
        let control = self.control.clone();
        let init_futures = created.into_iter().map(|(id, ty, node)| {
            let init_ctx = InitializeContext {
                session_id: session_id.clone(),
                node_id: id.clone(),
                control: control.clone(),
            };
            async move {
                match node.initialize(&init_ctx).await {
                    Ok(()) => Ok((id, ty, node)),
                    Err(e) => Err((id, ty, e)),
                }
            }
        });

        let initialised = match try_join_all(init_futures).await {
            Ok(v) => v,
            Err((id, ty, e)) => {
                tracing::error!(
                    session_id = %self.session_id,
                    node_id = %id,
                    node_type = %ty,
                    error = %e,
                    "Node initialization failed; aborting session startup"
                );
                self.emit_node_loading_event(
                    "error",
                    &id,
                    &format!("Node '{}' ({}) failed to initialize: {}", id, ty, e),
                );
                return Err(Error::Execution(format!(
                    "Node '{}' ({}) failed to initialize: {}",
                    id, ty, e
                )));
            }
        };

        // Phase 4: stamp authoritative `node_ready` for each
        // successfully initialised node, then move it into
        // `cached_nodes`. The router emits `node_ready` itself so the
        // frontend has a reliable transition signal even if a node
        // doesn't bother calling `ctx.emit_progress("ready", …)`.
        for (id, ty, node) in initialised {
            self.emit_node_loading_event(
                "node_ready",
                &id,
                &format!("Node '{}' ({}) ready", id, ty),
            );
            tracing::debug!(
                "Session {}: Initialized node '{}' (type: {})",
                self.session_id,
                id,
                ty
            );
            self.cached_nodes.insert(id, node);
        }

        tracing::info!(
            "Session {}: All {} nodes initialized",
            self.session_id,
            self.cached_nodes.len()
        );

        self.snapshot_bindings = Some(self.bind_snapshot_inputs()?);

        // Refine the descriptor now that node instances exist: pacing is a
        // property of the instance, not the factory.
        let pacing: HashMap<String, String> = self
            .cached_nodes
            .iter()
            .map(|(id, node)| {
                (
                    id.clone(),
                    crate::telemetry::descriptor::pacing_label(node.pacing_nature()),
                )
            })
            .collect();
        self.descriptor.set_pacing(&pacing);
        if let Some(ctrl) = &self.control {
            ctrl.set_descriptor(Arc::new(self.descriptor.clone()));
        }

        // Pipeline-level "ready": every node is up.
        self.emit_loading_event("ready", Some("Pipeline ready".to_string()));

        // Spec 025: After initialization, propagate actual capabilities from
        // RuntimeDiscovered nodes to downstream Adaptive/Passthrough nodes
        self.propagate_runtime_capabilities().await?;

        Ok(())
    }

    /// Emit a pipeline-level loading event on the control bus.
    ///
    /// Clients subscribed to `__system__.out` receive these as JSON events
    /// with `kind: "loading"`. Used for `initializing` (start), `ready`
    /// (all nodes done) — both with no `node` field, signalling
    /// pipeline-level transitions.
    fn emit_loading_event(&self, status: &str, message: Option<String>) {
        if let Some(ctrl) = &self.control {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let event = RuntimeData::Json(serde_json::json!({
                "kind": "loading",
                "status": status,
                "message": message,
                "ts_ms": ts,
            }));
            ctrl.publish_tap("__system__", None, event);
        }
    }

    /// Emit a per-node loading event on the control bus.
    ///
    /// Same wire shape as [`emit_loading_event`] but with a `node` field
    /// so the frontend can track lifecycle state per node. Used for:
    /// - `loading_node` — node init kicked off
    /// - `node_ready`   — node init returned `Ok` (authoritative — the
    ///                    router emits this regardless of whether the node
    ///                    itself called `ctx.emit_progress("ready", …)`)
    /// - `error`        — node init returned `Err`
    fn emit_node_loading_event(&self, status: &str, node_id: &str, message: &str) {
        if let Some(ctrl) = &self.control {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let event = RuntimeData::Json(serde_json::json!({
                "kind": "loading",
                "status": status,
                "node": node_id,
                "message": message,
                "ts_ms": ts,
            }));
            ctrl.publish_tap("__system__", None, event);
        }
    }

    /// Propagate capabilities from RuntimeDiscovered nodes after initialization (spec 025).
    ///
    /// This method is called after all nodes are initialized. For each node with
    /// `RuntimeDiscovered` behavior that reports actual capabilities, we:
    /// 1. Call `revalidate_and_propagate()` to update resolution context
    /// 2. Apply pending updates to downstream nodes via `configure_from_upstream()`
    async fn propagate_runtime_capabilities(&mut self) -> Result<()> {
        // Build resolution context from manifest if not already present
        if self.resolution_ctx.is_none() {
            let mut ctx = ResolutionContext::new();

            // Add node types and connections from manifest
            for node_spec in &self.manifest.nodes {
                ctx.node_types
                    .insert(node_spec.id.clone(), node_spec.node_type.clone());
            }
            for conn in &self.manifest.connections {
                ctx.add_connection(&conn.from, &conn.to);
            }

            // Set behaviors from cached nodes
            for (node_id, node) in &self.cached_nodes {
                ctx.set_behavior(node_id, node.capability_behavior());
            }

            self.resolution_ctx = Some(ctx);
        }

        let ctx = self.resolution_ctx.as_mut().unwrap();
        let resolver = CapabilityResolver::new(&self.registry);

        // Find RuntimeDiscovered nodes that have actual capabilities
        let runtime_discovered_nodes: Vec<String> = self
            .cached_nodes
            .iter()
            .filter(|(_, node)| {
                matches!(
                    node.capability_behavior(),
                    CapabilityBehavior::RuntimeDiscovered
                )
            })
            .map(|(id, _)| id.clone())
            .collect();

        for node_id in runtime_discovered_nodes {
            if let Some(node) = self.cached_nodes.get(&node_id) {
                // Get actual capabilities from the node after initialization
                if let Some(actual_caps) = node.actual_capabilities() {
                    tracing::info!(
                        "Session {}: Node '{}' reported actual capabilities, propagating to downstream nodes",
                        self.session_id,
                        node_id
                    );

                    // Update resolution context with actual capabilities and create pending updates
                    if let Err(e) = resolver.revalidate_and_propagate(ctx, &node_id, actual_caps) {
                        tracing::warn!(
                            "Session {}: Failed to propagate capabilities from '{}': {}",
                            self.session_id,
                            node_id,
                            e
                        );
                    }
                }
            }
        }

        // Apply pending updates to downstream nodes
        self.apply_pending_updates().await?;

        Ok(())
    }

    /// Apply pending capability updates to downstream nodes (spec 025).
    ///
    /// Iterates through all pending updates in the resolution context and calls
    /// `configure_from_upstream()` on each target node.
    pub async fn apply_pending_updates(&mut self) -> Result<()> {
        let ctx = match &mut self.resolution_ctx {
            Some(ctx) => ctx,
            None => return Ok(()), // No context, nothing to apply
        };

        if !ctx.has_pending_updates() {
            return Ok(());
        }

        // Get the list of nodes with pending updates
        let nodes_to_update: Vec<String> = ctx
            .nodes_with_pending_updates()
            .iter()
            .map(|s| s.to_string())
            .collect();

        tracing::debug!(
            "Session {}: Applying {} pending capability updates",
            self.session_id,
            nodes_to_update.len()
        );

        for node_id in nodes_to_update {
            // Take the pending update (removes from context)
            if let Some(notification) = ctx.take_pending_update(&node_id) {
                if let Some(node) = self.cached_nodes.get(&node_id) {
                    tracing::debug!(
                        "Session {}: Configuring '{}' from upstream '{}'",
                        self.session_id,
                        node_id,
                        notification.upstream_node_id
                    );

                    // Apply the upstream capabilities to this node
                    if let Err(e) = node.configure_from_upstream(&notification.upstream_output) {
                        tracing::warn!(
                            "Session {}: Failed to configure '{}' from upstream: {}",
                            self.session_id,
                            node_id,
                            e
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Start the router - runs until shutdown signal
    pub fn start(self) -> JoinHandle<()> {
        let session_id = self.session_id.clone();
        tracing::info!("Session {}: Starting session router", session_id);

        tokio::spawn(async move {
            if let Err(e) = self.run().await {
                tracing::error!("Session {}: Router error: {}", session_id, e);
            }
            tracing::info!("Session {}: Router stopped", session_id);
        })
    }

    /// Main routing loop
    /// Run the session router main loop
    ///
    /// This is the public entry point for starting the session router.
    /// It takes ownership of the input/shutdown channels and processes
    /// data through the pipeline graph.
    pub async fn run_public(self) -> Result<()> {
        self.run().await
    }

    async fn run(mut self) -> Result<()> {
        // Initialize all nodes before processing starts
        self.initialize_nodes().await?;
        self.run_after_init().await
    }

    /// Run the streaming loop assuming [`Self::initialize_nodes`] has
    /// already been called. Used by
    /// [`crate::transport::PipelineExecutor::cold_build_session`] so
    /// init completes synchronously before a `SessionHandle` is
    /// returned — without this split, `WarmSessionPool::prewarm`
    /// would return `Ok` while heavy nodes were still loading models
    /// in the background.
    pub async fn run_after_init(mut self) -> Result<()> {
        // Spawn one task per node, wired through mpsc channels along the
        // manifest connections. From here on, every node runs concurrently:
        // an audio chunk yielded by node A reaches node B's input_rx on the
        // same scheduler tick, instead of being batched into a `Vec` until
        // A's `process_streaming_async` returns. Sink yields go straight to
        // the client `output_tx`. See `spawn_pipeline_tasks` for the wiring.
        let mut pipeline = self.spawn_pipeline_tasks()?;

        // Spawn the telemetry publisher (openspec: add-live-pipeline-observer).
        // Replaces the old conditional `__perf__` flush task: it runs for
        // every session, is idle (two atomic loads per tick) when nobody
        // subscribes, turns latency histograms on while observed, and keeps
        // publishing the historical `__perf__` schema for existing HUDs.
        let publisher_shutdown = Arc::new(tokio::sync::Notify::new());
        let publisher_handle = crate::telemetry::publisher::spawn_publisher(
            self.telemetry.clone(),
            self.control.clone(),
            publisher_shutdown.clone(),
        );

        // Drop router's own input_tx so the transport channel closes when
        // all external senders (SessionHandle) are dropped.
        self.input_tx.take();

        let mut input_rx =
            self.input_rx.lock().unwrap().take().ok_or_else(|| {
                crate::Error::Execution("Input channel already taken".to_string())
            })?;

        let mut shutdown_rx =
            self.shutdown_rx.lock().unwrap().take().ok_or_else(|| {
                crate::Error::Execution("Shutdown channel already taken".to_string())
            })?;

        tracing::info!(
            "Session {}: Router running, waiting for input...",
            self.session_id
        );
        if node_trace_enabled() {
            log::info!(
                "Session {}: Router running, sources={:?}, sinks={:?}",
                self.session_id,
                self.graph.sources,
                self.graph.sinks
            );
        }

        // Single-threaded ingress loop. We no longer spawn per-packet tasks
        // because work is performed by per-node tasks; the router's only
        // job here is to shovel input packets into the right source node's
        // input channel. Each node's bounded input mpsc provides natural
        // backpressure back through `input_rx`.
        let explicit_close = loop {
            let ingress_start = std::time::Instant::now();
            tokio::select! {
                biased;

                _ = pipeline.tasks.wait_for_failure() => break true,

                result = input_rx.recv() => {
                    match result {
                        Some(packet) => {
                            self.probes.ingress.record_since(ingress_start);
                            tokio::select! {
                                biased;
                                _ = pipeline.tasks.wait_for_failure() => break true,
                                _ = shutdown_rx.recv() => break true,
                                _ = self.route_input(packet, &pipeline.input_txs, &pipeline.ingress_edges) => {}
                            }
                        }
                        None => {
                            tracing::warn!(
                                "Session {}: Input channel closed (all senders dropped), shutting down pipeline",
                                self.session_id
                            );
                            break false;
                        }
                    }
                }
                result = shutdown_rx.recv() => {
                    match result {
                        Some(()) => {
                            tracing::info!(
                                "Session {}: Shutdown signal received, closing pipeline",
                                self.session_id
                            );
                        }
                        None => {
                            tracing::warn!(
                                "Session {}: Shutdown channel closed (sender dropped), closing pipeline",
                                self.session_id
                            );
                        }
                    }
                    break true;
                }
            }
        };

        // Graceful teardown. Dropping every source input_tx cascades through
        // the pipeline: each node's main task exits when its input_rx
        // closes, which drops its fan_tx, which closes the next hop.
        let teardown = Self::teardown_pipeline_tasks(pipeline, explicit_close).await;

        // Stop the publisher before the control bus is signalled closed. It
        // publishes one final snapshot (`final: true`) — node tasks have
        // stopped by now, so observers see the end state.
        publisher_shutdown.notify_one();
        let _ = publisher_handle.await;

        // Wake every attached control client so they drain and exit.
        // Idempotent — harmless if no control is attached.
        if let Some(ctrl) = &self.control {
            ctrl.signal_close(match &teardown {
                Ok(()) => CloseReason::Normal,
                Err(error) => CloseReason::Error(error.to_string()),
            });
        }

        teardown
    }

    /// Build the per-node task pipeline.
    ///
    /// For every node in the graph we create a bounded input mpsc. We then
    /// walk the manifest connections to decide each node's successor set.
    /// Sinks additionally get a send-to-client hop.
    ///
    /// Drains `self.cached_nodes` — the node instances are moved into their
    /// owning tasks, so we can no longer borrow them from `self`. That's
    /// fine; after this call the router's only remaining role is shovelling
    /// packets into the source nodes' input channels.
    fn spawn_pipeline_tasks(&mut self) -> Result<PipelineTasks> {
        let SnapshotBindings {
            inputs: mut snapshot_inputs,
            edges: snapshot_edges,
        } = match self.snapshot_bindings.take() {
            Some(bindings) => bindings,
            None => self.bind_snapshot_inputs()?,
        };
        let (shutdown, _) = tokio::sync::watch::channel(false);
        let mut input_txs: HashMap<String, mpsc::Sender<RuntimeData>> = HashMap::new();
        let mut input_rxs: HashMap<String, mpsc::Receiver<RuntimeData>> = HashMap::new();

        // Pass 1: create an input channel for every node we have cached.
        for node_id in self.cached_nodes.keys() {
            let (tx, rx) = mpsc::channel::<RuntimeData>(NODE_INPUT_CAPACITY);
            input_txs.insert(node_id.clone(), tx);
            input_rxs.insert(node_id.clone(), rx);
        }

        // Use the validated snapshot readers; other edges become stream channels.
        let mut stream_successors: HashMap<String, Vec<Successor>> = HashMap::new();
        for node_id in self.cached_nodes.keys() {
            stream_successors.insert(node_id.clone(), Vec::new());
        }
        for (conn_idx, conn) in self.manifest.connections.iter().enumerate() {
            if snapshot_edges.contains(&conn_idx) {
                continue;
            }
            if let Some(tx) = input_txs.get(&conn.to) {
                stream_successors
                    .entry(conn.from.clone())
                    .or_default()
                    .push(Successor {
                        tx: tx.clone(),
                        edge: (conn_idx < self.telemetry.edge_count()).then_some(conn_idx),
                    });
            }
        }

        let sinks: std::collections::HashSet<String> = self.graph.sinks.iter().cloned().collect();

        // Drain node instances out of the cache. Each task owns its node.
        let nodes = std::mem::take(&mut self.cached_nodes);

        let mut handles = Vec::with_capacity(nodes.len() * 2);

        for (node_id, node) in nodes {
            let input_rx = match input_rxs.remove(&node_id) {
                Some(rx) => rx,
                None => {
                    tracing::error!(
                        "Session {}: node '{}' has no input channel (bug)",
                        self.session_id,
                        node_id
                    );
                    continue;
                }
            };
            let succ_txs = stream_successors.remove(&node_id).unwrap_or_default();
            let Some(node_idx) = self.telemetry.node_index(&node_id) else {
                tracing::error!(
                    "Session {}: node '{}' missing from telemetry index (bug)",
                    self.session_id,
                    node_id
                );
                continue;
            };
            let input_weak = input_txs
                .get(&node_id)
                .map(|tx| tx.downgrade())
                .expect("input channel created in pass 1");
            let egress_edge = self.descriptor.egress_edge(&node_id);
            let snap_inputs = snapshot_inputs.remove(&node_id).unwrap_or_default();
            let is_sink = sinks.contains(&node_id);
            let is_output_node = self
                .manifest
                .nodes
                .iter()
                .any(|n| n.id == node_id && n.is_output_node);

            let (main_handle, fan_handle) = Self::spawn_node_pipeline(
                self.bound_output_allocators
                    .get(&node_id)
                    .map(|allocator| {
                        allocator.clone() as Arc<dyn remotemedia_types::OutputAllocator>
                    })
                    .unwrap_or_else(|| self.output_allocator.clone()),
                self.node_materialization.context(&node_id)?,
                node_id.clone(),
                node,
                input_rx,
                succ_txs,
                snap_inputs,
                if is_sink || is_output_node {
                    Some(self.output_router.clone())
                } else {
                    None
                },
                self.session_id.clone(),
                self.scheduler.clone(),
                self.control.clone(),
                self.probes.clone(),
                self.perf.clone(),
                NodeTelemetryHandle {
                    tel: self.telemetry.clone(),
                    idx: node_idx,
                    egress_edge,
                    input_weak,
                },
                shutdown.subscribe(),
            );

            handles.push((format!("{node_id} main"), main_handle));
            handles.push((format!("{node_id} fan-out"), fan_handle));
        }

        let ingress_edges = self
            .graph
            .sources
            .iter()
            .filter_map(|s| self.descriptor.ingress_edge(s).map(|e| (s.clone(), e)))
            .collect();

        Ok(PipelineTasks {
            input_txs,
            ingress_edges,
            tasks: TaskGroup::new(shutdown, handles),
        })
    }

    /// Spawn the two tasks that drive a single node:
    ///
    /// 1. **main**: owns the node instance, consumes `input_rx`, and calls
    ///    `process_streaming_async` once per input. The scheduler wraps the
    ///    call so timeouts, retries, circuit breakers, and metrics still
    ///    apply. The sync callback passed into the node `try_send`s each
    ///    yield into an internal fan-out channel (`fan_tx`).
    ///
    /// 2. **fan_out**: drains `fan_rx`, applies the control-bus hook (tap +
    ///    intercept), and forwards surviving outputs to every successor's
    ///    input channel AND — for sinks — to the client `output_tx`. The
    ///    hook must run async, so it cannot live inside the sync callback;
    ///    that's why we need the second task.
    ///
    /// Returns both `JoinHandle`s so the router can await clean shutdown.
    fn spawn_node_pipeline(
        output_allocator: Arc<dyn remotemedia_types::OutputAllocator>,
        materialization: remotemedia_types::CopyContext,
        node_id: String,
        node: Box<dyn StreamingNode>,
        mut input_rx: mpsc::Receiver<RuntimeData>,
        successor_txs: Vec<Successor>,
        snapshot_inputs: HashMap<String, Arc<dyn crate::nodes::ports::SnapshotPort>>,
        client_tx: Option<ClientOutputRouter>,
        session_id: String,
        scheduler: Arc<StreamingScheduler>,
        control: Option<Arc<SessionControl>>,
        probes: Arc<crate::metrics::RtProbeSet>,
        perf: Arc<PerfAggregator>,
        nt: NodeTelemetryHandle,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> (JoinHandle<Result<()>>, JoinHandle<Result<()>>) {
        let (fan_tx, mut fan_rx) = mpsc::channel::<RuntimeData>(NODE_FANOUT_CAPACITY);
        let NodeTelemetryHandle {
            tel,
            idx: node_idx,
            egress_edge,
            input_weak,
        } = nt;

        // ── Fan-out drain task ─────────────────────────────────────────
        let fan_node_id = node_id.clone();
        let fan_session_id = session_id.clone();
        let fan_control = control.clone();
        let fan_probes = probes.clone();
        let fan_tel = tel.clone();
        let mut fan_shutdown = shutdown.clone();
        let fan_handle = tokio::spawn(async move {
            let forward = async move {
                let tel = fan_tel;
                while let Some(out) = fan_rx.recv().await {
                    tel.node(node_idx).outputs.fetch_add(1, RELAXED);
                    let kept = match &fan_control {
                        Some(ctrl) => ctrl.on_node_output(&fan_node_id, None, out).await,
                        None => Some(out.into_shared()),
                    };
                    let Some(kept) = kept else { continue };
                    // One size estimate per output, shared by every edge.
                    let kept_bytes = approx_payload_bytes(&kept);
                    let kept_desc = if node_trace_enabled() {
                        Some(describe_runtime_data(&kept))
                    } else {
                        None
                    };

                    // Fan out to successors first. Bounded `send` awaits on
                    // full, providing real backpressure all the way back to
                    // the node's callback (via `fan_tx` filling up).
                    for succ in &successor_txs {
                        if let Some(desc) = kept_desc.as_deref() {
                            tracing::info!(
                                session_id = %fan_session_id,
                                node_id = %fan_node_id,
                                output = %desc,
                                "node output forwarded to successor"
                            );
                            log::info!(
                                "Session {}: node '{}' output forwarded to successor: {}",
                                fan_session_id,
                                fan_node_id,
                                desc
                            );
                        }
                        // try_send first so a full successor is observable as a
                        // stall; then fall back to the same awaited send as
                        // before, so delivery and backpressure are unchanged.
                        let edge = succ.edge.and_then(|e| tel.edge(e));
                        let delivered = match succ.tx.try_send(kept.clone()) {
                            Ok(()) => true,
                            Err(mpsc::error::TrySendError::Full(v)) => {
                                let stall_start = std::time::Instant::now();
                                if let Some(e) = edge {
                                    e.begin_stall();
                                }
                                let ok = succ.tx.send(v).await.is_ok();
                                if let Some(e) = edge {
                                    e.end_stall(stall_start.elapsed().as_micros() as u64);
                                }
                                ok
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => false,
                        };
                        match (delivered, edge) {
                            (true, Some(e)) => e.record_frame(kept_bytes),
                            (false, Some(e)) => e.record_drop(),
                            _ => {}
                        }
                        if !delivered {
                            tracing::debug!(
                                "Session {}: node '{}' successor closed; drop",
                                fan_session_id,
                                fan_node_id
                            );
                        }
                    }

                    // Sinks: also forward to the client. Routed by RuntimeData
                    // kind so a slow Video consumer never head-of-line-blocks
                    // Audio (or vice versa) — see [`ClientOutputRouter`].
                    if let Some(ref out_router) = client_tx {
                        if let Some(desc) = kept_desc.as_deref() {
                            tracing::info!(
                                session_id = %fan_session_id,
                                node_id = %fan_node_id,
                                output = %desc,
                                "sink output forwarded to client"
                            );
                            log::info!(
                                "Session {}: sink '{}' output forwarded to client: {}",
                                fan_session_id,
                                fan_node_id,
                                desc
                            );
                        }
                        let egress_start = std::time::Instant::now();
                        let edge = egress_edge.and_then(|e| tel.edge(e));
                        let res = match out_router.try_send(kept) {
                            Ok(()) => Ok(()),
                            Err(mpsc::error::TrySendError::Full(v)) => {
                                if let Some(e) = edge {
                                    e.begin_stall();
                                }
                                let r = out_router.send(v).await.map_err(|_| ());
                                if let Some(e) = edge {
                                    e.end_stall(egress_start.elapsed().as_micros() as u64);
                                }
                                r
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => Err(()),
                        };
                        fan_probes.egress.record_since(egress_start);
                        match (&res, edge) {
                            (Ok(()), Some(e)) => e.record_frame(kept_bytes),
                            (Err(_), Some(e)) => e.record_drop(),
                            _ => {}
                        }
                        if res.is_err() {
                            tracing::warn!(
                                "Session {}: sink '{}' client channel closed",
                                fan_session_id,
                                fan_node_id
                            );
                        }
                    }
                }
            };
            tokio::select! {
                biased;
                _ = fan_shutdown.changed() => {},
                _ = forward => {},
            }
            Ok(())
        });

        // ── Aux-port filter task ───────────────────────────────────────
        //
        // Sits between the router-level `input_rx` and the main node
        // task. It does TWO things every node benefits from for free:
        //
        //   1. `barge_in` envelopes are intercepted here. They never
        //      reach the node's `process_*`. Instead they fire
        //      `cancel.notify_waiters()` which aborts whatever the
        //      node is currently doing (see select! below). This is
        //      the universal cancellation primitive — every node
        //      becomes preemptible without any per-node code.
        //
        //   2. All other frames (normal data + non-barge aux ports
        //      like `context` for nodes that opt into them) are
        //      forwarded unchanged. Nodes that consume aux ports
        //      (e.g. lfm2_text, qwen_tts_mlx) keep working.
        //
        // Putting this filter in the runtime — rather than asking
        // every node to recognise barge envelopes — was a direct
        // response to barge frames leaking into LLM prompts and TTS
        // synthesis whenever a node forgot to filter them.
        // Per-(session, node) cancel gate. Replaces a bare `Notify`
        // because nodes need to be able to TEMPORARILY suppress barge
        // cancellation — e.g. while a non-cancelable LLM tool call is
        // streaming, where dropping the HTTP future mid-tool-call
        // loses irreversible work. See `crate::transport::CancelGate`
        // for the protection-counter mechanics; nodes opt in by
        // taking a `ProtectGuard` from `ctx.cancel_gate.protect()`.
        let cancel = Arc::new(crate::transport::CancelGate::new());
        let (filt_tx, mut filt_rx) = mpsc::channel::<(u64, RuntimeData)>(NODE_FANOUT_CAPACITY);
        // Weak handles only: telemetry must never keep a channel open.
        tel.install_queues(
            node_idx,
            Box::new(input_weak),
            Box::new(filt_tx.downgrade()),
            Box::new(fan_tx.downgrade()),
        );
        let (control_tx, mut control_rx) = mpsc::channel::<RuntimeData>(16);
        let rpc_rx = if node.rpc_methods().is_empty() {
            None
        } else {
            control
                .as_ref()
                .map(|control| control.register_rpc(&node_id))
        };
        let input_generation = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let filter_generation = input_generation.clone();
        let filter_cancel = Arc::clone(&cancel);
        let filter_node_id = node_id.clone();
        let filter_session_id = session_id.clone();
        let children = Children::default();
        let filter_handle = tokio::spawn(async move {
            while let Some(input) = input_rx.recv().await {
                if let Some(port) = aux_port_of(&input) {
                    if port == BARGE_IN_PORT {
                        // Invalidate queued work before waking the active call.
                        // ProtectGuard preserves an irreversible in-flight call,
                        // but cancellation still supersedes its queued inputs.
                        filter_generation.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                        // Try to fire the cancel. If a node currently
                        // holds a `ProtectGuard` (e.g. the chat
                        // backend mid-non-cancelable-tool-call) the
                        // gate is closed and `try_cancel` returns
                        // false; we still forward the envelope to
                        // `process_control_message` so any &self
                        // state-clearing logic runs.
                        let cancelled = filter_cancel.try_cancel();
                        if cancelled {
                            tracing::debug!(
                                session_id = %filter_session_id,
                                node_id = %filter_node_id,
                                "Runtime: barge_in received — cancelling in-flight call \
                                 and forwarding to node.process_control_message"
                            );
                        } else {
                            tracing::info!(
                                session_id = %filter_session_id,
                                node_id = %filter_node_id,
                                "Runtime: barge_in suppressed by active ProtectGuard \
                                 (non-cancelable tool call in progress); envelope still \
                                 forwarded to process_control_message"
                            );
                        }
                        // Also forward the barge envelope to the main
                        // task so the node's `process_control_message`
                        // override can clear `&self`-held state
                        // (audio buffers, GRU state, smoother, etc.).
                        // For nodes whose only meaningful response to
                        // barge is in-flight cancellation (e.g. the
                        // OpenAIChatNode HTTP-stream-drop path), the
                        // default `process_control_message` returns
                        // `Ok(false)`, so this is a free no-op.
                        // Avatar M2.6 wires this through to the
                        // Audio2FaceLipSyncNode + SyntheticLipSyncNode.
                        if control_tx.send(input).await.is_err() {
                            break;
                        }
                        continue;
                    }
                    // Other aux ports fall through — node-side aux
                    // handlers (e.g. `_handle_aux_port` in Python
                    // nodes) still receive them.
                }
                let generation = filter_generation.load(std::sync::atomic::Ordering::Acquire);
                if filt_tx.send((generation, input)).await.is_err() {
                    break;
                }
            }
        });

        // ── Main node task ─────────────────────────────────────────────
        children.add("input filter", filter_handle);
        let main_node_id = node_id.clone();
        let main_session_id = session_id.clone();
        let main_cancel = Arc::clone(&cancel);
        let main_tel = tel.clone();
        let supervised_children = children.clone();
        let main_body = async move {
            let tel = main_tel;
            let cell = tel.node(node_idx);
            tel.set_alive(node_idx, true);
            // Read node state from the telemetry cache (one atomic load)
            // when it mirrors this router's control bus.
            let use_state_cache = control
                .as_ref()
                .and_then(|c| c.telemetry())
                .map(|t| Arc::ptr_eq(&t, &tel))
                .unwrap_or(false);
            // Convert the Box<dyn StreamingNode> into an Arc so capability
            // forwarder tasks can hold their own refs alongside the main
            // dispatch path. The Arc is cheap and the trait object's
            // vtable handles `&self` dispatch the same as Box.
            let node: Arc<dyn StreamingNode> = Arc::from(node);
            let node_ref: &dyn StreamingNode = &*node;

            // Construct the per-(node, session) NodeRuntimeContext once at
            // bind time. Cloning it per dispatch is cheap — every field is
            // an `Arc`. `make_session_state` is invoked exactly once per
            // session so heavy state (e.g. LlamaCpp's per-session
            // ChatState) is not reallocated on every input.
            let init_ctx_for_state = crate::nodes::InitializeContext {
                session_id: main_session_id.clone(),
                node_id: main_node_id.clone(),
                control: control.clone(),
            };
            let session_state = node_ref.make_session_state(&init_ctx_for_state);
            let ctx_control = control
                .clone()
                .unwrap_or_else(|| SessionControl::new(main_session_id.clone()));
            let bound_ctx = crate::nodes::NodeRuntimeContext::with_input_snapshots(
                main_session_id.clone(),
                main_node_id.clone(),
                Arc::clone(&ctx_control),
                session_state,
                snapshot_inputs,
            )
            // Hand the node-side ctx the SAME `Arc<CancelGate>` the
            // filter task owns above. Without this every ctx would
            // start with a fresh, never-fired gate and `protect()`
            // calls inside the node would have no effect on the real
            // barge path.
            .with_cancel_gate(Arc::clone(&cancel))
            .with_output_allocator(output_allocator)
            .with_copy_context(materialization);

            if let Some(rx) = rpc_rx {
                let (rpc_shutdown, rpc_stop) = tokio::sync::watch::channel(false);
                children.add_cooperative(
                    "RPC",
                    rpc_shutdown,
                    tokio::spawn(super::rpc_dispatch::run(
                        node.clone(),
                        bound_ctx.clone(),
                        rx,
                        rpc_stop,
                    )),
                );
            }

            // Capability transient channel: subscribe to every address the
            // node declared interest in, drain into `on_capability_update`.
            // Default impl writes the value to `ctx.capabilities`, so the
            // node's per-call methods can read via `ctx.capability(addr)`
            // without overriding anything else. Spawned tasks hold their
            // own `Arc<dyn StreamingNode>` clone + ctx clone so they keep
            // running even after the main loop drops `node_ref`.
            let cap_addrs = node_ref.capability_subscriptions();
            for addr in cap_addrs {
                let mut rx = ctx_control.subscribe_capability(&addr);
                // Seed: deliver the retained value synchronously so
                // `ctx.capability(addr)` returns it on the very next input
                // dispatch, even if no fresh publish lands first.
                if let Some(seed) = ctx_control.current_capability(&addr) {
                    let _ = node_ref.on_capability_update(&addr, seed, &bound_ctx).await;
                }
                let cap_node = Arc::clone(&node);
                let cap_ctx = bound_ctx.clone();
                let cap_addr = addr.clone();
                children.add(
                    format!("capability {addr:?}"),
                    tokio::spawn(async move {
                        loop {
                            match rx.recv().await {
                                Ok(caps) => {
                                    let _ = cap_node
                                        .on_capability_update(&cap_addr, caps, &cap_ctx)
                                        .await;
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                    // Slow consumer fell behind. Re-seed from
                                    // the retained value so we can't observe
                                    // a stale capability and drop a real one
                                    // — `current_capability` is always the
                                    // latest publish.
                                    if let Some(seed) =
                                        cap_ctx.control.current_capability(&cap_addr)
                                    {
                                        let _ = cap_node
                                            .on_capability_update(&cap_addr, seed, &cap_ctx)
                                            .await;
                                    }
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                            }
                        }
                    }),
                );
            }

            // Pacer wiring: delegate to `crate::transport::pacer`.
            //
            // - `SourceWall(hz)` → `WallTickSource(hz)`. Headless,
            //   self-paced (idle generators, debug demos).
            // - `ClockedToOutboundMedia` with `clocked_media_addr =
            //   Some(medium, stream_id)` → `WireTickSource` subscribed
            //   to the WebRTC sender's media_clock at
            //   `<medium>:<stream_id>`. Avatar render bound to the
            //   outbound video clock.
            // - `ClockedToOutboundMedia` with `clocked_media_addr =
            //   None` → fallback `WallTickSource(fallback_rate_hz)`
            //   so the node still ticks during headless tests / before
            //   a peer connects (Phase 5.9 fallback).
            //
            // Outputs flow into the same `fan_tx` reactive emits use,
            // so downstream wiring is identical regardless of pacing.
            let pacer_handle: Option<tokio::task::JoinHandle<()>> = {
                use crate::nodes::PacingNature;
                use crate::transport::pacer::{
                    OnMiss, Pacer, TickSource, WallTickSource, WireTickSource,
                };
                let source: Option<Box<dyn TickSource>> = match node_ref.pacing_nature() {
                    PacingNature::SourceWall(hz) if hz > 0 => {
                        Some(Box::new(WallTickSource::new(hz)))
                    }
                    PacingNature::ClockedToOutboundMedia => match node_ref.clocked_media_addr() {
                        Some((medium, stream_id)) => {
                            tracing::debug!(
                                pacer_node = %main_node_id,
                                medium = %medium,
                                stream_id = %stream_id,
                                "Subscribing ClockedToOutboundMedia node to wire clock"
                            );
                            Some(Box::new(WireTickSource::subscribe(
                                &ctx_control,
                                &medium,
                                &stream_id,
                            )))
                        }
                        None => {
                            let hz = node_ref.fallback_rate_hz().max(1);
                            tracing::warn!(
                                pacer_node = %main_node_id,
                                fallback_hz = hz,
                                "ClockedToOutboundMedia node has no wire clock binding; falling back to wall pacer"
                            );
                            Some(Box::new(WallTickSource::new(hz)))
                        }
                    },
                    PacingNature::SourceWall(_) | PacingNature::Reactive => None,
                };
                source.map(|src| {
                    let pacer = Pacer::new(
                        main_node_id.clone(),
                        Arc::clone(&node),
                        bound_ctx.clone(),
                        fan_tx.clone(),
                        src,
                        OnMiss::default(),
                    )
                    .with_perf(perf.clone());
                    pacer.spawn()
                })
            };

            if let Some(handle) = pacer_handle {
                children.add("pacer", handle);
            }
            loop {
                let input = tokio::select! {
                    biased;
                    _ = shutdown.changed() => break,
                    Some(control) = control_rx.recv() => control,
                    input = filt_rx.recv() => match input {
                        Some((generation, input)) if generation == input_generation.load(std::sync::atomic::Ordering::Acquire) => input,
                        Some(_) => {
                            // Superseded by a barge-in.
                            cell.discarded.fetch_add(1, RELAXED);
                            continue;
                        }
                        None => break,
                    },
                };
                if node_trace_enabled() {
                    tracing::info!(
                        session_id = %main_session_id,
                        node_id = %main_node_id,
                        input = %describe_runtime_data(&input),
                        "node input received"
                    );
                    log::info!(
                        "Session {}: node '{}' input received: {}",
                        main_session_id,
                        main_node_id,
                        describe_runtime_data(&input)
                    );
                }
                // Barge-in dispatch. The filter task forwards barge
                // envelopes here in addition to firing
                // `cancel.notify_waiters()`. We route the envelope to
                // `process_control_message` so nodes can clear
                // `&self`-held state (e.g. audio buffers, GRU). This
                // does NOT flow through `process_streaming_async` —
                // barge envelopes never appear in the data path. The
                // result is dropped: barge handling is fire-and-forget
                // and a node returning `Ok(false)` (or an error) just
                // means it had no state to clear.
                if let Some(port) = aux_port_of(&input) {
                    if port == BARGE_IN_PORT {
                        if let Err(e) = node_ref.process_control_message(input, &bound_ctx).await {
                            tracing::debug!(
                                node_id = %main_node_id,
                                "process_control_message(barge_in) error (ignored): {}",
                                e
                            );
                        }
                        continue;
                    }
                }

                // Node-state gate (Bypass / Disabled). Per-input, so a
                // runtime control-bus toggle takes effect on the next
                // packet.
                {
                    use crate::transport::session_control::NodeState;
                    let state = if use_state_cache {
                        cell.node_state()
                    } else if let Some(ctrl) = &control {
                        ctrl.node_state(&main_node_id)
                    } else {
                        NodeState::Enabled
                    };
                    match state {
                        NodeState::Enabled => {}
                        NodeState::Bypass => {
                            cell.bypassed.fetch_add(1, RELAXED);
                            if fan_tx.send(input).await.is_err() {
                                break;
                            }
                            continue;
                        }
                        NodeState::Disabled => {
                            cell.discarded.fetch_add(1, RELAXED);
                            continue;
                        }
                    }
                }
                cell.inputs.fetch_add(1, RELAXED);

                // Perf instrumentation: record one input event,
                // and per-output latency via the wrapped callback.
                // When the perf aggregator is disabled, every
                // `record_*` call is an inline atomic-load + early
                // return.
                perf.record_input(&main_node_id);
                let perf_node_id = main_node_id.clone();
                let perf_clone = perf.clone();
                let dispatch_start_for_perf = std::time::Instant::now();
                let first_emit_seen =
                    std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
                let output_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

                // Per-input sync callback: try_send first; if the
                // fan-out channel is full, fall back to a blocking
                // send so backpressure propagates to the producer
                // instead of silently dropping the frame. This was
                // load-bearing for the avatar pipeline where
                // audio2face emits 30 blendshapes/sec and the
                // renderer consumes ~16/sec — without this fallback,
                // ~25% of blendshapes were dropped, breaking
                // lip-sync. The blocking_send runs in the calling
                // task's tokio worker; on the multi-thread runtime
                // other tasks keep running on other workers, and
                // the producer task simply stalls until the consumer
                // catches up.
                let cb_fan_tx = fan_tx.clone();
                let cb_node_id = main_node_id.clone();
                let cb_session_id = main_session_id.clone();
                let cb_first_emit = first_emit_seen.clone();
                let cb_output_count = output_count.clone();
                let cb = Box::new(move |out: RuntimeData| {
                    // Latency from input arrival to this output.
                    let lat_us = dispatch_start_for_perf.elapsed().as_micros() as u64;
                    let is_first = !cb_first_emit.swap(true, std::sync::atomic::Ordering::Relaxed);
                    perf_clone.record_output(&perf_node_id, lat_us, is_first);
                    let emit_index =
                        cb_output_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;

                    if node_trace_enabled() {
                        tracing::info!(
                            session_id = %cb_session_id,
                            node_id = %cb_node_id,
                            output_index = emit_index,
                            latency_us = lat_us,
                            output = %describe_runtime_data(&out),
                            "node output emitted"
                        );
                        log::info!(
                            "Session {}: node '{}' output #{} emitted after {}us: {}",
                            cb_session_id,
                            cb_node_id,
                            emit_index,
                            lat_us,
                            describe_runtime_data(&out)
                        );
                    }

                    crate::transport::output_delivery::send_output(&cb_fan_tx, &cb_node_id, out)
                });

                let use_fast = scheduler.config.is_fast_path(&main_node_id);
                let node_dispatch_start = std::time::Instant::now();
                // +1 so that 0 keeps meaning "idle".
                cell.in_flight_since_us.store(tel.now_us() + 1, RELAXED);

                // Cheap clone of the per-(node, session) ctx built above.
                // Every field is `Arc`-backed; this keeps the per-dispatch
                // session state stable while still letting closures own
                // their own clone for spawning.
                let ctx = bound_ctx.clone();

                // Build the dispatch future (not yet awaited). Once
                // we await it inside the select!, dropping it on
                // cancel will run all destructors — including HTTP
                // streams, multiprocess IPC sends in flight, etc. —
                // so cancellation is genuine, not just gated output.
                let dispatch_fut = async {
                    if use_fast {
                        let input_clone = input.clone();
                        let ctx_clone = ctx.clone();
                        scheduler
                            .execute_streaming_node_fast(&main_node_id, || async move {
                                node_ref
                                    .process_streaming_async(input_clone, &ctx_clone, cb)
                                    .await
                                    .map(|_| ())
                            })
                            .await
                    } else {
                        // Full path retries the closure on transient failures,
                        // so each retry rebuilds the callback + input clone.
                        let input_outer = input.clone();
                        let ctx_outer = ctx.clone();
                        let cb_outer = std::sync::Mutex::new(Some(cb));
                        scheduler
                            .execute_streaming_node(&main_node_id, || {
                                let input_clone = input_outer.clone();
                                let ctx_clone = ctx_outer.clone();
                                // Take the callback for the first attempt;
                                // subsequent retries rebuild a fresh one
                                // against the same fan_tx so yields from a
                                // retry still reach the drain task.
                                let cb_taken = cb_outer.lock().unwrap().take();
                                let cb_for_attempt: Box<
                                    dyn FnMut(RuntimeData) -> Result<()> + Send,
                                > = match cb_taken {
                                    Some(c) => c,
                                    None => {
                                        let fan_tx = fan_tx.clone();
                                        let id = main_node_id.clone();
                                        Box::new(move |out| {
                                            // try_send with blocking_send
                                            // fallback — see the primary
                                            // cb above for rationale.
                                            match fan_tx.try_send(out) {
                                                Ok(()) => Ok(()),
                                                Err(tokio::sync::mpsc::error::TrySendError::Full(out)) => {
                                                    tokio::task::block_in_place(|| {
                                                        fan_tx.blocking_send(out)
                                                    })
                                                    .map_err(|_| {
                                                        tracing::warn!(
                                                            "node '{}' fan_tx receiver dropped during retry blocking send",
                                                            id
                                                        );
                                                        crate::error::Error::Execution(
                                                            "fan_tx receiver dropped".into(),
                                                        )
                                                    })?;
                                                    Ok(())
                                                }
                                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                                    tracing::warn!(
                                                        "node '{}' fan_tx closed; dropping output",
                                                        id
                                                    );
                                                    Ok(())
                                                }
                                            }
                                        })
                                    }
                                };
                                async move {
                                    node_ref
                                        .process_streaming_async(
                                            input_clone,
                                            &ctx_clone,
                                            cb_for_attempt,
                                        )
                                        .await
                                        .map(|_| ())
                                }
                            })
                            .await
                    }
                };

                let cancelled = tokio::select! {
                    biased;
                    _ = shutdown.changed() => break,
                    _ = main_cancel.notified() => {
                        tracing::info!(
                            session_id = %main_session_id,
                            node_id = %main_node_id,
                            "Runtime: in-flight call cancelled by barge_in"
                        );
                        // Dropping the dispatch_fut here cascades drop
                        // through every awaited future inside the
                        // node call (HTTP streams, IPC sends, etc.),
                        // which is the cancellation mechanism.
                        true
                    }
                    r = dispatch_fut => {
                        if let Err(e) = r {
                            if let Some(ev) = tel.record_error(node_idx, &e.to_string()) {
                                if let Some(ctrl) = &control {
                                    crate::telemetry::publisher::publish_node_error(
                                        ctrl,
                                        &main_node_id,
                                        &ev.message,
                                        ev.count,
                                    );
                                }
                            }
                            tracing::error!(
                                "Session {}: node '{}' execution error: {}",
                                main_session_id,
                                main_node_id,
                                e
                            );
                            if node_trace_enabled() {
                                log::error!(
                                    "Session {}: node '{}' execution error: {}",
                                    main_session_id,
                                    main_node_id,
                                    e
                                );
                            }
                        }
                        false
                    }
                };
                if node_trace_enabled() {
                    tracing::info!(
                        session_id = %main_session_id,
                        node_id = %main_node_id,
                        cancelled = cancelled,
                        outputs = output_count.load(std::sync::atomic::Ordering::Relaxed),
                        elapsed_ms = node_dispatch_start.elapsed().as_millis(),
                        "node execution finished"
                    );
                    log::info!(
                        "Session {}: node '{}' execution finished: cancelled={}, outputs={}, elapsed_ms={}",
                        main_session_id,
                        main_node_id,
                        cancelled,
                        output_count.load(std::sync::atomic::Ordering::Relaxed),
                        node_dispatch_start.elapsed().as_millis()
                    );
                }
                let _ = cancelled;

                cell.busy_us
                    .fetch_add(node_dispatch_start.elapsed().as_micros() as u64, RELAXED);
                cell.in_flight_since_us.store(0, RELAXED);
                probes.node_out.record_since(node_dispatch_start);
            }
            // input_rx closed: drop fan_tx so the drain task exits once
            // it finishes forwarding any already-queued outputs.
            drop(fan_tx);
            // Capability forwarders block on a broadcast::Receiver, so
            // they need an explicit abort when the node task exits —
            // dropping the Arc<SessionControl> doesn't close the
            // broadcast (other subscribers may still hold it).
            let mut failures = children.stop().await;
            if let Err(error) = node_ref
                .process_control_message(
                    crate::transport::session_control::wrap_aux_port(
                        "shutdown",
                        RuntimeData::Json(serde_json::json!({})),
                    ),
                    &bound_ctx,
                )
                .await
            {
                tracing::error!(node = %main_node_id, %error, "node shutdown failed");
                failures.push(format!("shutdown: {error}"));
            }
            if failures.is_empty() {
                Ok(())
            } else {
                Err(Error::Execution(format!(
                    "node {main_node_id}: {}",
                    failures.join("; ")
                )))
            }
        };
        let main_handle = tokio::spawn(async move {
            let result = children::supervise(supervised_children, main_body).await;
            tel.set_alive(node_idx, false);
            result
        });

        (main_handle, fan_handle)
    }

    /// Dispatch an incoming [`DataPacket`] to the right source/target
    /// node's input channel. Stamps the arrival timestamp and records a
    /// drift sample if the payload is timed media.
    async fn route_input(
        &self,
        packet: DataPacket,
        input_txs: &HashMap<String, mpsc::Sender<RuntimeData>>,
        ingress_edges: &HashMap<String, usize>,
    ) {
        let arrival_ts_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        let mut input_data = crate::transport::data::apply_transport_metadata_to_runtime(
            packet.data.into_shared(),
            &packet.metadata,
        );
        input_data.set_arrival_timestamp(arrival_ts_us);

        if input_data.is_timed_media() {
            self.record_drift_sample(&input_data).await;
        }

        let targets: Vec<&str> = if let Some(ref target) = packet.to_node {
            vec![target.as_str()]
        } else {
            self.graph.sources.iter().map(|s| s.as_str()).collect()
        };

        for target in targets {
            let Some(tx) = input_txs.get(target) else {
                tracing::warn!(
                    "Session {}: input routed to unknown node '{}'",
                    self.session_id,
                    target
                );
                continue;
            };
            if node_trace_enabled() {
                tracing::info!(
                    session_id = %self.session_id,
                    target_node = %target,
                    input = %describe_runtime_data(&input_data),
                    "router input routed to source"
                );
                log::info!(
                    "Session {}: router routed input to '{}': {}",
                    self.session_id,
                    target,
                    describe_runtime_data(&input_data)
                );
            }
            let ingress = ingress_edges
                .get(target)
                .and_then(|&e| self.telemetry.edge(e));
            let delivered = match tx.try_send(input_data.clone()) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(v)) => {
                    let stall_start = std::time::Instant::now();
                    if let Some(e) = ingress {
                        e.begin_stall();
                    }
                    let ok = tx.send(v).await.is_ok();
                    if let Some(e) = ingress {
                        e.end_stall(stall_start.elapsed().as_micros() as u64);
                    }
                    ok
                }
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            };
            if let Some(e) = ingress {
                if delivered {
                    e.record_frame(approx_payload_bytes(&input_data));
                } else {
                    e.record_drop();
                }
            }
            if !delivered {
                tracing::warn!(
                    "Session {}: node '{}' input channel closed; drop packet",
                    self.session_id,
                    target
                );
            }
        }
    }

    /// Await every spawned task after dropping the router's own input_txs.
    async fn teardown_pipeline_tasks(pipeline: PipelineTasks, explicit_close: bool) -> Result<()> {
        let PipelineTasks {
            input_txs,
            ingress_edges: _,
            tasks,
        } = pipeline;
        // Drop the router's copies of every node's input sender; each source
        // node sees `input_rx.recv()` return `None` and exits, cascading
        // through the graph.
        drop(input_txs);
        tasks.join(explicit_close).await
    }

    /// Snapshot all RT latency probes in declaration order:
    /// `ingress, route_in, node_in, node_out, egress`.
    ///
    /// `ingress`, `egress`, and `node_out` are actively recorded;
    /// `route_in` and `node_in` will be wired as A-wave migrations
    /// land and the dispatch path gets more inspectable.
    pub fn probe_snapshots(&self) -> [(&'static str, crate::metrics::ProbeSnapshot); 5] {
        self.probes.snapshot_all()
    }

    /// Snapshot the router's operational counters (`spawn_count`,
    /// `loopback_depth`). Core router doesn't currently spawn per
    /// packet, so `spawn_count` stays at 0 — useful as a baseline and
    /// to flag regressions if any future code adds a per-packet spawn.
    pub fn operational_snapshot(&self) -> crate::metrics::OperationalSnapshot {
        self.probes.operational_snapshot()
    }

    /// Clone the router's probe handle.
    ///
    /// [`Self::start`] consumes the router, so anything that wants to
    /// read probe state *after* the router is running (admin
    /// endpoints, test harnesses) needs the `Arc` before the `start`
    /// call. `probes().snapshot_all()` / `probes().operational_snapshot()`
    /// is the out-of-band equivalent of the `&self` accessors above.
    pub fn probes(&self) -> Arc<crate::metrics::RtProbeSet> {
        Arc::clone(&self.probes)
    }

    // The old `process_input` (topological per-packet loop with per-node
    // Vec batching) has been deleted. Per-node concurrent execution now
    // lives in `spawn_pipeline_tasks` + `spawn_node_pipeline`; ingress
    // routing is `route_input`.

    /// Record drift sample for timed media (spec 026).
    ///
    /// Phase B1: inner lock is `parking_lot::RwLock`; no `.await` is
    /// held across it. `async` kept on the method only so callers
    /// that already use `.await` here don't need to change.
    async fn record_drift_sample(&self, data: &RuntimeData) {
        let (media_ts_us, arrival_ts_us) = data.timing();

        // Need both timestamps to record drift
        let (media_ts, arrival_ts) = match (media_ts_us, arrival_ts_us) {
            (Some(m), Some(a)) => (m, a),
            _ => return,
        };

        // Get or create drift metrics for this stream
        let stream_id = data.stream_id().unwrap_or("default").to_string();

        // Lock-free per-packet lookup via DashMap. `entry().or_insert_with`
        // atomically gets-or-creates the per-stream metrics without the
        // read-upgrade-write double-check pattern the old RwLock needed.
        let metrics = self
            .drift_metrics
            .entry(stream_id.clone())
            .or_insert_with(|| {
                Arc::new(DriftRwLock::new(DriftMetrics::new(
                    stream_id.clone(),
                    self.drift_thresholds.clone(),
                )))
            })
            .clone();

        // Sync write lock — parking_lot, uncontended CAS fast path.
        let mut metrics_guard = metrics.write();

        // Use appropriate method based on media type
        let alert_changed = if data.is_audio() {
            metrics_guard.record_audio_sample(media_ts, arrival_ts)
        } else if data.is_video() {
            // For video, we'd ideally compute a content hash, but for now use None
            metrics_guard.record_video_sample(media_ts, arrival_ts, None)
        } else {
            metrics_guard.record_sample(media_ts, arrival_ts, None)
        };

        if alert_changed {
            let alerts = metrics_guard.alerts();
            tracing::warn!(
                "Session {}: Stream '{}' alert state changed: {:?}, health_score: {:.2}",
                self.session_id,
                stream_id,
                alerts,
                metrics_guard.health_score()
            );
        }
    }

    // ==================== Metrics API (spec 026) ====================

    /// Get scheduler metrics (node execution stats)
    pub async fn get_scheduler_metrics(&self) -> crate::executor::metrics::PipelineMetrics {
        self.scheduler.get_metrics().await
    }

    /// Get node-level execution statistics
    pub async fn get_node_stats(&self, node_id: &str) -> Option<NodeStats> {
        self.scheduler.get_node_stats(node_id).await
    }

    /// Get all node statistics
    pub async fn get_all_node_stats(&self) -> HashMap<String, NodeStats> {
        self.scheduler.get_all_node_stats().await
    }

    /// Get drift metrics for a specific stream.
    ///
    /// `async` is preserved for API stability; inner `read()` is sync
    /// parking_lot (B1).
    pub async fn get_drift_metrics(&self, stream_id: &str) -> Option<serde_json::Value> {
        let metrics = self.drift_metrics.get(stream_id)?.clone();
        let m = metrics.read();
        Some(m.to_debug_json())
    }

    /// Get all stream IDs with drift metrics
    pub async fn get_stream_ids(&self) -> Vec<String> {
        self.drift_metrics.iter().map(|e| e.key().clone()).collect()
    }

    /// Export all metrics in Prometheus format
    pub async fn prometheus_metrics(&self) -> String {
        let mut output = String::new();

        // Scheduler metrics
        output.push_str(&self.scheduler.to_prometheus().await);

        // Drift metrics (aggregated — per-stream detail available via debug endpoint).
        // Snapshot via DashMap::iter (no global lock) so we don't hold
        // the shard guard across the per-stream reads.
        let stream_metrics: Vec<Arc<DriftRwLock<DriftMetrics>>> = self
            .drift_metrics
            .iter()
            .map(|e| e.value().clone())
            .collect();
        if !stream_metrics.is_empty() {
            output.push_str(&format!(
                "session_router_active_streams{{session_id=\"{}\"}} {}\n",
                self.session_id,
                stream_metrics.len()
            ));

            // Aggregate health score (minimum across streams). Sync
            // reads via parking_lot.
            let mut min_health = 1.0f64;
            for metrics in &stream_metrics {
                let m = metrics.read();
                min_health = min_health.min(m.health_score());
            }
            output.push_str(&format!(
                "session_router_min_health_score{{session_id=\"{}\"}} {:.6}\n",
                self.session_id, min_health
            ));
        }

        output
    }

    /// Export per-stream debug metrics as JSON
    pub async fn debug_stream_metrics(&self) -> serde_json::Value {
        // Snapshot pairs first so the per-stream reads happen off the
        // DashMap shard guard (even with sync reads now, we keep the
        // clone-and-iterate pattern).
        let pairs: Vec<(String, Arc<DriftRwLock<DriftMetrics>>)> = self
            .drift_metrics
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        let stream_count = pairs.len();
        let mut streams = serde_json::Map::new();
        for (stream_id, metrics) in pairs {
            let m = metrics.read();
            streams.insert(stream_id, m.to_debug_json());
        }

        serde_json::json!({
            "session_id": self.session_id,
            "stream_count": stream_count,
            "streams": streams,
            "scheduler": {
                "max_concurrency": self.scheduler.config.max_concurrency,
                "available_permits": "N/A", // Can't get this synchronously
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) struct BlockingCloseNode {
        pub(super) started: Arc<tokio::sync::Notify>,
        pub(super) stopped: Arc<std::sync::atomic::AtomicBool>,
    }

    struct ShutdownResultNode {
        cleaned: Arc<std::sync::atomic::AtomicUsize>,
        fail: bool,
    }
    #[async_trait::async_trait]
    impl crate::nodes::AsyncStreamingNode for ShutdownResultNode {
        fn node_type(&self) -> &str {
            "ShutdownResultNode"
        }
        async fn process(&self, data: RuntimeData) -> Result<RuntimeData> {
            Ok(data)
        }
        async fn process_control_message(
            &self,
            data: RuntimeData,
            _: Option<String>,
        ) -> Result<bool> {
            if aux_port_of(&data).as_deref() != Some("shutdown") {
                return Ok(false);
            }
            // Another node's failure must not short-circuit this cleanup.
            tokio::task::yield_now().await;
            self.cleaned
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.fail {
                Err(Error::Execution("cleanup sentinel".into()))
            } else {
                Ok(true)
            }
        }
    }

    #[tokio::test]
    async fn shutdown_failure_reaches_router_result_and_control_after_all_nodes_join() {
        let manifest = create_test_manifest(
            vec![
                ("bad", "ShutdownResultNode"),
                ("good", "ShutdownResultNode"),
            ],
            vec![],
        );
        let (output, _received) = ClientOutputRouter::new(1);
        let (mut router, shutdown) = SessionRouter::new(
            "shutdown-failure".into(),
            Arc::new(manifest),
            Arc::new(StreamingNodeRegistry::new()),
            output,
        )
        .unwrap();
        let cleaned = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for (id, fail) in [("bad", true), ("good", false)] {
            router.cached_nodes.insert(
                id.into(),
                Box::new(crate::nodes::AsyncNodeWrapper(Arc::new(
                    ShutdownResultNode {
                        cleaned: cleaned.clone(),
                        fail,
                    },
                ))),
            );
        }
        let control = SessionControl::new("shutdown-failure");
        let mut close = control.close_subscriber();
        router.attach_control(control).await;
        shutdown.send(()).await.unwrap();
        let error =
            tokio::time::timeout(std::time::Duration::from_secs(2), router.run_after_init())
                .await
                .unwrap()
                .unwrap_err()
                .to_string();
        assert!(
            error.contains("bad main") && error.contains("cleanup sentinel"),
            "{error}"
        );
        assert_eq!(cleaned.load(std::sync::atomic::Ordering::SeqCst), 2);
        let CloseReason::Error(reason) = close.recv().await.unwrap() else {
            panic!("normal close hid failure")
        };
        assert_eq!(reason, error);
    }

    #[tokio::test]
    async fn teardown_observes_panics_and_cancellation_without_abandoning_other_tasks() {
        let (shutdown, mut waiting) = tokio::sync::watch::channel(false);
        let joined = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let finished = joined.clone();
        let blocked = tokio::spawn(async move {
            waiting.changed().await.unwrap();
            tokio::task::yield_now().await;
            finished.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        });
        let panicked = tokio::spawn(async { panic!("teardown panic sentinel") });
        let cancelled = tokio::spawn(std::future::pending::<Result<()>>());
        cancelled.abort();
        let pipeline = PipelineTasks {
            input_txs: HashMap::new(),
            ingress_edges: HashMap::new(),
            tasks: TaskGroup::new(
                shutdown,
                vec![
                    ("blocked".into(), blocked),
                    ("panicked".into(), panicked),
                    ("cancelled".into(), cancelled),
                ],
            ),
        };
        let error = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            SessionRouter::teardown_pipeline_tasks(pipeline, false),
        )
        .await
        .unwrap()
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("panicked") && error.contains("teardown panic sentinel"),
            "{error}"
        );
        assert!(error.contains("cancelled"), "{error}");
        assert!(joined.load(std::sync::atomic::Ordering::SeqCst));
    }
    #[async_trait::async_trait]
    impl crate::nodes::AsyncStreamingNode for BlockingCloseNode {
        fn node_type(&self) -> &str {
            "BlockingCloseNode"
        }
        async fn process(&self, data: RuntimeData) -> crate::Result<RuntimeData> {
            Ok(data)
        }
        async fn process_streaming<F>(
            &self,
            data: RuntimeData,
            _session: Option<String>,
            mut callback: F,
        ) -> crate::Result<usize>
        where
            F: FnMut(RuntimeData) -> crate::Result<()> + Send,
        {
            if matches!(&data, RuntimeData::Text(text) if text != "start") {
                callback(data)?;
                return Ok(1);
            }
            for _ in 0..8 {
                callback(RuntimeData::Json(serde_json::json!({"pending":true})))?;
            }
            self.started.notify_one();
            std::future::pending().await
        }
        async fn process_control_message(
            &self,
            data: RuntimeData,
            _session: Option<String>,
        ) -> crate::Result<bool> {
            if matches!(
                aux_port_of(&data).as_deref(),
                Some("shutdown" | BARGE_IN_PORT)
            ) {
                self.stopped
                    .store(true, std::sync::atomic::Ordering::Release);
                return Ok(true);
            }
            Ok(false)
        }
    }

    #[tokio::test]
    async fn barge_preempts_queued_inputs_and_preserves_fresh_input() {
        let manifest = create_test_manifest(vec![("A", "BlockingCloseNode")], vec![]);
        let (output, mut received) = ClientOutputRouter::new(32);
        let (mut router, _) = SessionRouter::new(
            "barge-queue".into(),
            Arc::new(manifest),
            Arc::new(StreamingNodeRegistry::new()),
            output,
        )
        .unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        router.cached_nodes.insert(
            "A".into(),
            Box::new(crate::nodes::AsyncNodeWrapper(Arc::new(
                BlockingCloseNode {
                    started: started.clone(),
                    stopped: stopped.clone(),
                },
            ))),
        );
        let pipeline = router.spawn_pipeline_tasks().unwrap();
        let input = &pipeline.input_txs["A"];
        input.send(RuntimeData::Text("start".into())).await.unwrap();
        started.notified().await;
        for _ in 0..8 {
            input
                .send(RuntimeData::Text("obsolete speech".into()))
                .await
                .unwrap();
        }
        input
            .send(crate::transport::session_control::wrap_aux_port(
                BARGE_IN_PORT,
                RuntimeData::Json(serde_json::json!({})),
            ))
            .await
            .unwrap();
        input
            .send(RuntimeData::Text("fresh speech".into()))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let packet = received.data_rx.recv().await.unwrap();
                if let RuntimeData::Text(text) = packet {
                    assert_eq!(text, "fresh speech", "cancelled queue must not replay");
                    assert!(
                        stopped.load(std::sync::atomic::Ordering::Acquire),
                        "control precedes fresh work"
                    );
                    break;
                }
            }
        })
        .await
        .unwrap();
        SessionRouter::teardown_pipeline_tasks(pipeline, true)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn explicit_close_cancels_generation_and_full_sink_then_runs_shutdown() {
        let manifest = create_test_manifest(vec![("A", "BlockingCloseNode")], vec![]);
        let (output, _unread) = ClientOutputRouter::new(1);
        let (mut router, _) = SessionRouter::new(
            "close-test".into(),
            Arc::new(manifest),
            Arc::new(StreamingNodeRegistry::new()),
            output,
        )
        .unwrap();
        let started = Arc::new(tokio::sync::Notify::new());
        let stopped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        router.cached_nodes.insert(
            "A".into(),
            Box::new(crate::nodes::AsyncNodeWrapper(Arc::new(
                BlockingCloseNode {
                    started: started.clone(),
                    stopped: stopped.clone(),
                },
            ))),
        );
        let pipeline = router.spawn_pipeline_tasks().unwrap();
        pipeline.input_txs["A"]
            .send(RuntimeData::Text("start".into()))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), started.notified())
            .await
            .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            SessionRouter::teardown_pipeline_tasks(pipeline, true),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(stopped.load(std::sync::atomic::Ordering::Acquire));
    }
    use crate::manifest::{Connection, ManifestMetadata, NodeManifest};

    pub(super) fn create_test_manifest(
        nodes: Vec<(&str, &str)>,
        connections: Vec<(&str, &str)>,
    ) -> Manifest {
        Manifest {
            version: "v1".to_string(),
            metadata: ManifestMetadata {
                name: "test-pipeline".to_string(),
                ..Default::default()
            },
            nodes: nodes
                .into_iter()
                .map(|(id, node_type)| NodeManifest {
                    id: id.to_string(),
                    node_type: node_type.to_string(),
                    params: serde_json::json!({}),
                    ..Default::default()
                })
                .collect(),
            connections: connections
                .into_iter()
                .map(|(from, to)| Connection {
                    from: from.to_string(),
                    to: to.to_string(),
                    ..Default::default()
                })
                .collect(),
            python_env: None,
            plugins: Vec::new(),
        }
    }

    #[test]
    fn test_session_router_graph_validation() {
        // Create a valid linear pipeline
        let manifest = create_test_manifest(
            vec![("A", "TestNode"), ("B", "TestNode"), ("C", "TestNode")],
            vec![("A", "B"), ("B", "C")],
        );

        let registry = Arc::new(StreamingNodeRegistry::new());
        let (output_tx, _output_rxs) = ClientOutputRouter::new(DEFAULT_ROUTER_OUTPUT_CAPACITY);

        let result = SessionRouter::new(
            "test-session".to_string(),
            Arc::new(manifest),
            registry,
            output_tx,
        );

        assert!(result.is_ok());
        let (router, _shutdown_tx) = result.unwrap();

        assert_eq!(router.execution_order(), &["A", "B", "C"]);
        assert_eq!(router.sources(), &["A"]);
        assert_eq!(router.sinks(), &["C"]);
    }

    #[test]
    fn test_session_router_cycle_detection() {
        // Create a cyclic pipeline (should fail)
        let manifest = create_test_manifest(
            vec![("A", "TestNode"), ("B", "TestNode"), ("C", "TestNode")],
            vec![("A", "B"), ("B", "C"), ("C", "A")], // Cycle!
        );

        let registry = Arc::new(StreamingNodeRegistry::new());
        let (output_tx, _output_rxs) = ClientOutputRouter::new(DEFAULT_ROUTER_OUTPUT_CAPACITY);

        let result = SessionRouter::new(
            "test-session".to_string(),
            Arc::new(manifest),
            registry,
            output_tx,
        );

        assert!(result.is_err());
        let error = result.err().unwrap().to_string();
        assert!(
            error.contains("cycle"),
            "Error should mention cycle: {}",
            error
        );
    }

    #[test]
    fn test_session_router_fan_out() {
        // A -> B, A -> C (fan-out)
        let manifest = create_test_manifest(
            vec![("A", "TestNode"), ("B", "TestNode"), ("C", "TestNode")],
            vec![("A", "B"), ("A", "C")],
        );

        let registry = Arc::new(StreamingNodeRegistry::new());
        let (output_tx, _output_rxs) = ClientOutputRouter::new(DEFAULT_ROUTER_OUTPUT_CAPACITY);

        let (router, _shutdown_tx) = SessionRouter::new(
            "test-session".to_string(),
            Arc::new(manifest),
            registry,
            output_tx,
        )
        .unwrap();

        assert_eq!(router.sources(), &["A"]);
        assert_eq!(router.sinks().len(), 2);
        assert!(router.sinks().contains(&"B".to_string()));
        assert!(router.sinks().contains(&"C".to_string()));
    }

    #[test]
    fn test_session_router_fan_in() {
        // A -> C, B -> C (fan-in)
        let manifest = create_test_manifest(
            vec![("A", "TestNode"), ("B", "TestNode"), ("C", "TestNode")],
            vec![("A", "C"), ("B", "C")],
        );

        let registry = Arc::new(StreamingNodeRegistry::new());
        let (output_tx, _output_rxs) = ClientOutputRouter::new(DEFAULT_ROUTER_OUTPUT_CAPACITY);

        let (router, _shutdown_tx) = SessionRouter::new(
            "test-session".to_string(),
            Arc::new(manifest),
            registry,
            output_tx,
        )
        .unwrap();

        assert_eq!(router.sources().len(), 2);
        assert!(router.sources().contains(&"A".to_string()));
        assert!(router.sources().contains(&"B".to_string()));
        assert_eq!(router.sinks(), &["C"]);
    }

    #[test]
    fn test_session_router_diamond() {
        // A -> B, A -> C, B -> D, C -> D (diamond)
        let manifest = create_test_manifest(
            vec![
                ("A", "TestNode"),
                ("B", "TestNode"),
                ("C", "TestNode"),
                ("D", "TestNode"),
            ],
            vec![("A", "B"), ("A", "C"), ("B", "D"), ("C", "D")],
        );

        let registry = Arc::new(StreamingNodeRegistry::new());
        let (output_tx, _output_rxs) = ClientOutputRouter::new(DEFAULT_ROUTER_OUTPUT_CAPACITY);

        let (router, _shutdown_tx) = SessionRouter::new(
            "test-session".to_string(),
            Arc::new(manifest),
            registry,
            output_tx,
        )
        .unwrap();

        assert_eq!(router.sources(), &["A"]);
        assert_eq!(router.sinks(), &["D"]);

        // Verify execution order respects dependencies
        let order = router.execution_order();
        let a_idx = order.iter().position(|x| x == "A").unwrap();
        let b_idx = order.iter().position(|x| x == "B").unwrap();
        let c_idx = order.iter().position(|x| x == "C").unwrap();
        let d_idx = order.iter().position(|x| x == "D").unwrap();

        assert!(a_idx < b_idx);
        assert!(a_idx < c_idx);
        assert!(b_idx < d_idx);
        assert!(c_idx < d_idx);
    }

    // ==================== Spec 026 Integration Tests ====================

    #[test]
    fn test_session_router_with_scheduler_config() {
        // Test creating router with custom scheduler config
        let manifest =
            create_test_manifest(vec![("A", "TestNode"), ("B", "TestNode")], vec![("A", "B")]);

        let registry = Arc::new(StreamingNodeRegistry::new());
        let (output_tx, _output_rxs) = ClientOutputRouter::new(DEFAULT_ROUTER_OUTPUT_CAPACITY);

        let scheduler_config = SchedulerConfig::with_concurrency(8)
            .with_timeout(5000)
            .with_circuit_breaker_threshold(3);

        let result = SessionRouter::with_config(
            "test-session".to_string(),
            Arc::new(manifest),
            registry,
            output_tx,
            Some(scheduler_config),
            None,
        );

        assert!(result.is_ok());
        let (router, _shutdown_tx) = result.unwrap();
        assert_eq!(router.scheduler.config.max_concurrency, 8);
        assert_eq!(router.scheduler.config.default_timeout_ms, 5000);
        assert_eq!(router.scheduler.config.circuit_breaker_threshold, 3);
    }

    #[test]
    fn test_session_router_with_drift_thresholds() {
        // Test creating router with custom drift thresholds
        let manifest = create_test_manifest(vec![("A", "TestNode")], vec![]);

        let registry = Arc::new(StreamingNodeRegistry::new());
        let (output_tx, _output_rxs) = ClientOutputRouter::new(DEFAULT_ROUTER_OUTPUT_CAPACITY);

        let drift_thresholds = DriftThresholds {
            slope_threshold_ms_per_s: 10.0,
            av_skew_threshold_us: 100_000,
            ..Default::default()
        };

        let result = SessionRouter::with_config(
            "test-session".to_string(),
            Arc::new(manifest),
            registry,
            output_tx,
            None,
            Some(drift_thresholds.clone()),
        );

        assert!(result.is_ok());
        let (router, _shutdown_tx) = result.unwrap();
        assert_eq!(router.drift_thresholds.slope_threshold_ms_per_s, 10.0);
        assert_eq!(router.drift_thresholds.av_skew_threshold_us, 100_000);
    }

    #[tokio::test]
    async fn test_session_router_drift_metrics_creation() {
        // Test that drift metrics are created for streams
        let manifest = create_test_manifest(vec![("A", "TestNode")], vec![]);

        let registry = Arc::new(StreamingNodeRegistry::new());
        let (output_tx, _output_rxs) = ClientOutputRouter::new(DEFAULT_ROUTER_OUTPUT_CAPACITY);

        let (router, _shutdown_tx) = SessionRouter::new(
            "test-session".to_string(),
            Arc::new(manifest),
            registry,
            output_tx,
        )
        .unwrap();

        // Initially no streams
        assert!(router.get_stream_ids().await.is_empty());

        // Record a drift sample manually by accessing internals
        let audio_data = RuntimeData::Audio {
            samples: vec![0.1; 100].into(),
            sample_rate: 16000,
            channels: 1,
            stream_id: Some("stream_1".to_string()),
            timestamp_us: Some(1_000_000),
            arrival_ts_us: Some(1_001_000),
            metadata: None,
        };

        router.record_drift_sample(&audio_data).await;

        // Now we should have one stream
        let stream_ids = router.get_stream_ids().await;
        assert_eq!(stream_ids.len(), 1);
        assert!(stream_ids.contains(&"stream_1".to_string()));

        // Get drift metrics for the stream
        let metrics = router.get_drift_metrics("stream_1").await;
        assert!(metrics.is_some());
        let metrics_json = metrics.unwrap();
        assert_eq!(metrics_json["stream_id"], "stream_1");
    }

    #[tokio::test]
    async fn test_session_router_prometheus_metrics() {
        // Test Prometheus metrics export
        let manifest = create_test_manifest(vec![("A", "TestNode")], vec![]);

        let registry = Arc::new(StreamingNodeRegistry::new());
        let (output_tx, _output_rxs) = ClientOutputRouter::new(DEFAULT_ROUTER_OUTPUT_CAPACITY);

        let (router, _shutdown_tx) = SessionRouter::new(
            "test-session".to_string(),
            Arc::new(manifest),
            registry,
            output_tx,
        )
        .unwrap();

        // Record a sample to create stream metrics
        let audio_data = RuntimeData::Audio {
            samples: vec![0.1; 100].into(),
            sample_rate: 16000,
            channels: 1,
            stream_id: Some("test_stream".to_string()),
            timestamp_us: Some(1_000_000),
            arrival_ts_us: Some(1_000_000),
            metadata: None,
        };
        router.record_drift_sample(&audio_data).await;

        let prom = router.prometheus_metrics().await;

        // Should contain scheduler metrics
        assert!(prom.contains("streaming_scheduler_max_concurrency"));

        // Should contain router metrics since we have a stream
        assert!(prom.contains("session_router_active_streams"));
        assert!(prom.contains("session_router_min_health_score"));
    }

    #[tokio::test]
    async fn test_session_router_debug_stream_metrics() {
        // Test debug JSON metrics export
        let manifest = create_test_manifest(vec![("A", "TestNode")], vec![]);

        let registry = Arc::new(StreamingNodeRegistry::new());
        let (output_tx, _output_rxs) = ClientOutputRouter::new(DEFAULT_ROUTER_OUTPUT_CAPACITY);

        let (router, _shutdown_tx) = SessionRouter::new(
            "test-session".to_string(),
            Arc::new(manifest),
            registry,
            output_tx,
        )
        .unwrap();

        // Record samples from two streams
        let audio_1 = RuntimeData::Audio {
            samples: vec![0.1; 100].into(),
            sample_rate: 16000,
            channels: 1,
            stream_id: Some("audio_1".to_string()),
            timestamp_us: Some(1_000_000),
            arrival_ts_us: Some(1_000_500),
            metadata: None,
        };
        let audio_2 = RuntimeData::Audio {
            samples: vec![0.2; 100].into(),
            sample_rate: 16000,
            channels: 1,
            stream_id: Some("audio_2".to_string()),
            timestamp_us: Some(2_000_000),
            arrival_ts_us: Some(2_001_000),
            metadata: None,
        };

        router.record_drift_sample(&audio_1).await;
        router.record_drift_sample(&audio_2).await;

        let debug_json = router.debug_stream_metrics().await;

        assert_eq!(debug_json["session_id"], "test-session");
        assert_eq!(debug_json["stream_count"], 2);
        assert!(debug_json["streams"]["audio_1"].is_object());
        assert!(debug_json["streams"]["audio_2"].is_object());
    }

    #[tokio::test]
    async fn test_session_router_node_stats() {
        // Test node statistics retrieval
        let manifest = create_test_manifest(vec![("A", "TestNode")], vec![]);

        let registry = Arc::new(StreamingNodeRegistry::new());
        let (output_tx, _output_rxs) = ClientOutputRouter::new(DEFAULT_ROUTER_OUTPUT_CAPACITY);

        let (router, _shutdown_tx) = SessionRouter::new(
            "test-session".to_string(),
            Arc::new(manifest),
            registry,
            output_tx,
        )
        .unwrap();

        // Initially no node stats (no executions yet)
        let stats = router.get_node_stats("A").await;
        assert!(stats.is_none());

        // Get all node stats (empty initially)
        let all_stats = router.get_all_node_stats().await;
        assert!(all_stats.is_empty());
    }
}

#[cfg(test)]
#[path = "session_router_telemetry_tests.rs"]
mod telemetry_tests;
