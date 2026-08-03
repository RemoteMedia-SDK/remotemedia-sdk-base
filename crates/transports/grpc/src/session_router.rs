//! Session-level Async Router for Persistent Streaming
//!
//! This module implements a persistent router that runs for the entire session,
//! continuously processing chunks from the client and routing them through the pipeline.
//!
//! # Graph Integration (spec 021)
//!
//! This router uses `PipelineGraph` from core for:
//! - Cycle detection at session creation (early validation)
//! - Access to execution_order, sources, and sinks
//! - Graph topology information (fan-in/fan-out)
//!
//! The actual routing still uses per-node tasks for transport-specific features
//! like status updates, multiprocess IPC setup, and gRPC streaming.

// Internal infrastructure - some methods reserved for future use
#![allow(dead_code)]
// StreamSession is intentionally kept private for internal use
#![allow(private_interfaces)]

use crate::adapters::runtime_data_to_data_buffer;
use crate::generated::{
    stream_response::Response as StreamResponseType, ChunkResult, StreamResponse,
};
use crate::streaming::StreamSession;
use remotemedia_core::data::RuntimeData;
use remotemedia_core::executor::PipelineGraph;
use remotemedia_core::metrics::RtProbeSet;
use remotemedia_core::nodes::{StreamingNode, StreamingNodeRegistry};
use remotemedia_core::transport::session_control::SessionControl;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tonic::Status;
use tracing::{debug, error, info, warn};

/// Capacity of the client-ingress channel (client → router).
///
/// Small by design: this is the actual backpressure knob for the gRPC
/// streaming handler. At 48 kHz / 20 ms frames this is ~160 ms of headroom.
///
/// Override via `REMOTEMEDIA_GRPC_ROUTER_INPUT_CAPACITY`.
pub const DEFAULT_GRPC_CLIENT_INPUT_CAPACITY: usize = 8;

/// Capacity of the node-to-router loopback channel.
///
/// Sized large (32× client-ingress) so that in normal operation the
/// loopback never fills — client-ingress backpressure is the only
/// bound that should ever matter. A full loopback indicates a real
/// pipeline problem (runaway fan-out or a stalled sink consumer).
///
/// Override via `REMOTEMEDIA_GRPC_ROUTER_LOOPBACK_CAPACITY`.
pub const DEFAULT_GRPC_LOOPBACK_CAPACITY: usize = 256;

fn grpc_client_input_capacity() -> usize {
    std::env::var("REMOTEMEDIA_GRPC_ROUTER_INPUT_CAPACITY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_GRPC_CLIENT_INPUT_CAPACITY)
}

fn grpc_loopback_capacity() -> usize {
    std::env::var("REMOTEMEDIA_GRPC_ROUTER_LOOPBACK_CAPACITY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_GRPC_LOOPBACK_CAPACITY)
}

/// Capacity of each per-node input channel.
///
/// Deadlock-safety relationship: `loopback_capacity ≥ node_input_capacity ×
/// max_expected_fanout`. With the defaults (loopback 256, node-input 32)
/// the router tolerates up to ~8× fan-out per node without the router
/// task blocking on `node_input.send().await` while a node is blocked on
/// `loopback_tx.send().await`. If your pipeline has bigger fan-out, raise
/// `REMOTEMEDIA_GRPC_ROUTER_LOOPBACK_CAPACITY` accordingly.
///
/// Override via `REMOTEMEDIA_GRPC_NODE_INPUT_CAPACITY`.
pub const DEFAULT_GRPC_NODE_INPUT_CAPACITY: usize = 32;

fn grpc_node_input_capacity() -> usize {
    std::env::var("REMOTEMEDIA_GRPC_NODE_INPUT_CAPACITY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_GRPC_NODE_INPUT_CAPACITY)
}

#[cfg(feature = "multiprocess")]
use remotemedia_core::python::multiprocess::MultiprocessExecutor;

/// Represents a data packet flowing through the pipeline
#[derive(Clone, Debug)]
pub struct DataPacket {
    /// The actual data
    pub data: RuntimeData,
    /// Source node ID
    pub from_node: String,
    /// Target node ID (if specified)
    pub to_node: Option<String>,
    /// Session ID
    pub session_id: String,
    /// Sequence number for ordering
    pub sequence: u64,
    /// Sub-sequence for streaming outputs
    pub sub_sequence: u64,
}

/// Session-persistent router that runs for the entire streaming session
pub struct SessionRouter {
    /// Session ID
    session_id: String,

    /// Registry for creating nodes
    registry: Arc<StreamingNodeRegistry>,

    /// Session state
    session: Arc<Mutex<StreamSession>>,

    /// Pipeline graph (from spec 021) - provides validated topology
    /// Used for: cycle detection, execution order, sources/sinks identification
    graph: PipelineGraph,

    /// Channel to send results to client
    client_tx: mpsc::Sender<Result<StreamResponse, Status>>,

    /// Bounded channel receiving new chunks from the client (transport ingress).
    ///
    /// Full → the gRPC handler's `.await send` stalls, applying real
    /// backpressure to the client stream.
    client_input_rx: mpsc::Receiver<DataPacket>,

    /// Sender half of the client-ingress channel. Cloned and handed to the
    /// transport handler via `get_input_sender()`.
    client_input_tx: mpsc::Sender<DataPacket>,

    /// Bounded channel receiving node outputs routed back for further
    /// dispatch (the self-loop). Separate from `client_input_*` so that
    /// bounding ingress doesn't risk deadlocking the node → router path.
    loopback_rx: mpsc::Receiver<DataPacket>,

    /// Sender half of the loopback channel. Cloned into each node task.
    loopback_tx: mpsc::Sender<DataPacket>,

    /// Channel to receive shutdown signal
    shutdown_rx: mpsc::Receiver<()>,

    /// Channel to send shutdown signal (held externally)
    _shutdown_tx: mpsc::Sender<()>,

    /// Active node tasks
    node_tasks: HashMap<String, JoinHandle<()>>,

    /// Node input channels
    /// Per-node bounded input channels. Capacity set from
    /// [`DEFAULT_GRPC_NODE_INPUT_CAPACITY`] — see the constant's doc for
    /// the loopback-capacity relationship that keeps `route_packet` from
    /// deadlocking with a node task that's blocked on `loopback_tx.send`.
    node_inputs: HashMap<String, mpsc::Sender<DataPacket>>,

    /// Whether the router is running
    running: bool,

    /// Per-session control bus used by source-Python progress, node output
    /// taps, and coordinator/tool relay events.
    control: Option<Arc<SessionControl>>,

    /// Multiprocess executor for IPC communication (optional)
    #[cfg(feature = "multiprocess")]
    multiprocess_executor: Option<Arc<MultiprocessExecutor>>,

    /// Phase B0 instrumentation: per-session latency histograms plus
    /// `spawn_count` and `loopback_depth`. Shared via `Arc` with each
    /// spawned node task so they can record samples without
    /// needing a handle back to the router.
    probes: Arc<RtProbeSet>,
}

impl SessionRouter {
    /// Create a new session router with graph validation
    ///
    /// Returns (router, shutdown_sender) - the shutdown_sender should be stored to trigger shutdown
    ///
    /// # Errors
    ///
    /// Returns `Status::invalid_argument` if the pipeline graph is invalid:
    /// - Cycles detected in connections
    /// - References to non-existent nodes
    /// - Other graph validation errors
    pub async fn new(
        session_id: String,
        registry: Arc<StreamingNodeRegistry>,
        session: Arc<Mutex<StreamSession>>,
        client_tx: mpsc::Sender<Result<StreamResponse, Status>>,
    ) -> Result<(Self, mpsc::Sender<()>), Status> {
        // Build and validate the pipeline graph (spec 021)
        let graph = {
            let session_guard = session.lock().await;
            PipelineGraph::from_manifest(&session_guard.manifest).map_err(|e| {
                error!(
                    "Session {}: Pipeline graph validation failed: {}",
                    session_id, e
                );
                Status::invalid_argument(format!("Invalid pipeline graph: {}", e))
            })?
        };

        info!(
            "Session {}: Built pipeline graph - {} nodes, order: {:?}, sources: {:?}, sinks: {:?}",
            session_id,
            graph.node_count(),
            graph.execution_order,
            graph.sources,
            graph.sinks
        );

        // Client ingress is bounded small — this is the real backpressure
        // surface toward the gRPC client stream.
        let (client_input_tx, client_input_rx) = mpsc::channel(grpc_client_input_capacity());
        // Loopback is bounded but generous. Sized so that client-ingress
        // backpressure is the limiting factor in practice; a full loopback
        // logs a warning and indicates a pipeline design problem.
        let (loopback_tx, loopback_rx) = mpsc::channel(grpc_loopback_capacity());
        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);
        let shutdown_tx_clone = shutdown_tx.clone();

        let router = Self {
            session_id,
            registry,
            session,
            graph,
            client_tx,
            client_input_rx,
            client_input_tx,
            loopback_rx,
            loopback_tx,
            shutdown_rx,
            _shutdown_tx: shutdown_tx,
            node_tasks: HashMap::new(),
            node_inputs: HashMap::new(),
            running: false,
            control: None,
            #[cfg(feature = "multiprocess")]
            multiprocess_executor: None,
            probes: Arc::new(RtProbeSet::new()),
        };

        Ok((router, shutdown_tx_clone))
    }

    /// Get the bounded input sender for feeding chunks from the client.
    ///
    /// Callers should `.await` the send — the channel applies real
    /// backpressure when the pipeline is behind.
    pub fn get_input_sender(&self) -> mpsc::Sender<DataPacket> {
        self.client_input_tx.clone()
    }

    /// Attach the control bus before node initialization and router start.
    pub fn attach_control(&mut self, control: Arc<SessionControl>) {
        self.control = Some(control);
    }

    /// Snapshot every RT latency probe in declaration order:
    /// `ingress, route_in, node_in, node_out, egress`.
    pub fn probe_snapshots(&self) -> [(&'static str, remotemedia_core::metrics::ProbeSnapshot); 5] {
        self.probes.snapshot_all()
    }

    /// Snapshot the router's operational counters. `spawn_count`
    /// climbs once per per-packet streaming-node spawn (B0 state);
    /// `loopback_depth` is the current fill of the loopback channel.
    pub fn operational_snapshot(&self) -> remotemedia_core::metrics::OperationalSnapshot {
        self.probes.operational_snapshot()
    }

    /// Get the pipeline graph
    pub fn graph(&self) -> &PipelineGraph {
        &self.graph
    }

    /// Get the execution order from the graph
    pub fn execution_order(&self) -> &[String] {
        &self.graph.execution_order
    }

    /// Get the source nodes (nodes with no inputs)
    pub fn sources(&self) -> &[String] {
        &self.graph.sources
    }

    /// Get the sink nodes (nodes with no outputs - terminal nodes)
    pub fn sinks(&self) -> &[String] {
        &self.graph.sinks
    }

    /// Check if a node is a terminal node (sink)
    pub fn is_terminal_node(&self, node_id: &str) -> bool {
        self.graph.sinks.contains(&node_id.to_string())
    }

    /// Set the multiprocess executor for IPC communication
    #[cfg(feature = "multiprocess")]
    pub fn set_multiprocess_executor(&mut self, executor: Arc<MultiprocessExecutor>) {
        self.multiprocess_executor = Some(executor);
    }

    /// Pre-initialize all nodes in the manifest before streaming starts
    ///
    /// This eliminates cold-start latency by loading all models upfront.
    /// Any initialization errors are caught early before streaming begins.
    ///
    /// Sends real-time status updates to the client during initialization.
    pub async fn pre_initialize_all_nodes(&mut self) -> Result<(), Status> {
        let node_specs: Vec<(String, String)> = {
            let session = self.session.lock().await;
            session
                .manifest
                .nodes
                .iter()
                .map(|n| (n.id.clone(), n.node_type.clone()))
                .collect()
        };

        let total_nodes = node_specs.len();
        info!(
            "🔥 Pre-initializing {} nodes for session '{}'...",
            total_nodes, self.session_id
        );
        info!(
            "   Node list: {:?}",
            node_specs
                .iter()
                .map(|(id, _)| id.as_str())
                .collect::<Vec<_>>()
        );

        // Send initialization start message to client (non-blocking fire-and-forget)
        let _ = self.client_tx.try_send(Ok({
            use crate::generated::{
                stream_response::Response as StreamResponseType, StreamResponse,
            };
            use remotemedia_core::data::RuntimeData;

            let status_text = format!(
                "[_system] status=initializing message=Initializing {} nodes...",
                total_nodes
            );
            let status_data = RuntimeData::Text(status_text);
            let proto_data = runtime_data_to_data_buffer(&status_data);

            let mut data_outputs = HashMap::new();
            data_outputs.insert("_status".to_string(), proto_data);

            StreamResponse {
                response: Some(StreamResponseType::Result(ChunkResult {
                    sequence: 0,
                    data_outputs,
                    processing_time_ms: 0.0,
                    total_items_processed: 0,
                })),
            }
        }));

        for (idx, (node_id, node_type)) in node_specs.iter().enumerate() {
            let progress = ((idx + 1) * 100) / total_nodes;

            info!(
                "   📦 [{}/{}] Initializing {} (type: {})...",
                idx + 1,
                total_nodes,
                node_id,
                node_type
            );

            // Send "initializing" status to client
            self.send_status_update(
                &node_id,
                "initializing",
                &format!("Loading {} ({}/{})", node_type, idx + 1, total_nodes),
            );

            match self.get_or_create_node(&node_id).await {
                Ok(node) => {
                    info!(
                        "   📦 [{}/{}] Node created, calling initialize()...",
                        idx + 1,
                        total_nodes
                    );

                    // 🔥 Actually call initialize() to load models
                    let init_ctx = remotemedia_core::nodes::InitializeContext {
                        session_id: self.session_id.clone(),
                        node_id: node_id.clone(),
                        control: self.control.clone(),
                    };
                    match node.initialize(&init_ctx).await {
                        Ok(_) => {
                            info!(
                                "   ✅ [{}/{}] {} initialized successfully ({}% complete)",
                                idx + 1,
                                total_nodes,
                                node_id,
                                progress
                            );

                            // Query node status
                            let status = node.get_status();

                            // Send "ready" status to client
                            self.send_status_update(
                                &node_id,
                                status.as_str(),
                                &format!("{} ready ({}/{})", node_type, idx + 1, total_nodes),
                            );
                        }
                        Err(init_err) => {
                            error!(
                                "   ❌ [{}/{}] Failed to initialize {}: {}",
                                idx + 1,
                                total_nodes,
                                node_id,
                                init_err
                            );

                            // Send "error" status to client
                            self.send_status_update(
                                &node_id,
                                "error",
                                &format!("Initialization failed: {}", init_err),
                            );

                            return Err(Status::internal(format!(
                                "Failed to initialize node '{}': {}",
                                node_id, init_err
                            )));
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "   ❌ [{}/{}] Failed to initialize {}: {}",
                        idx + 1,
                        total_nodes,
                        node_id,
                        e
                    );

                    // Send "error" status to client
                    self.send_status_update(
                        &node_id,
                        "error",
                        &format!("Failed to initialize {}: {}", node_type, e),
                    );

                    return Err(Status::internal(format!(
                        "Failed to pre-initialize node '{}': {}",
                        node_id, e
                    )));
                }
            }
        }

        info!(
            "✅ All {} nodes pre-initialized and ready for streaming",
            total_nodes
        );

        // Send completion message to client
        self.send_status_update(
            "_system",
            "ready",
            &format!("All {} nodes ready for streaming", total_nodes),
        );

        Ok(())
    }

    /// Send a status update message to the client (non-blocking)
    fn send_status_update(&self, node_id: &str, status: &str, message: &str) {
        use crate::generated::{stream_response::Response as StreamResponseType, StreamResponse};
        use remotemedia_core::data::RuntimeData;

        // Create status message as text
        let status_text = format!("[{}] status={} message={}", node_id, status, message);
        let status_data = RuntimeData::Text(status_text);

        // Convert to proto
        let proto_data = runtime_data_to_data_buffer(&status_data);

        // Create ChunkResult with status info
        let mut data_outputs = HashMap::new();
        data_outputs.insert("_status".to_string(), proto_data);

        let chunk_result = ChunkResult {
            sequence: 0, // Status updates use sequence 0
            data_outputs,
            processing_time_ms: 0.0,
            total_items_processed: 0,
        };

        let response = StreamResponse {
            response: Some(StreamResponseType::Result(chunk_result)),
        };

        // Send to client (non-blocking, ignore errors)
        let _ = self.client_tx.try_send(Ok(response));
    }

    /// Start the router - this runs until the session ends
    pub fn start(mut self) -> JoinHandle<()> {
        info!(
            "🚀 Starting session router for session '{}'",
            self.session_id
        );

        self.running = true;
        let session_id = self.session_id.clone();

        // Spawn the main routing task
        tokio::spawn(async move {
            if let Err(e) = self.run().await {
                error!("Session router failed: {}", e);
            }
            info!("🛑 Session router stopped for session '{}'", session_id);
        })
    }

    /// Main routing loop - runs until session ends
    async fn run(&mut self) -> Result<(), Status> {
        info!("📡 Session router running - waiting for chunks from client...");

        // Process incoming packets from client ingress OR from node loopback,
        // plus the shutdown signal. Keeping client and loopback as separate
        // bounded channels lets us apply real backpressure to the client
        // (small ingress capacity) without risking deadlock on the node →
        // router loopback (generous capacity).
        loop {
            // Phase B0 probe: sample the current loopback depth before
            // we suspend on `select!`. If B1 (spawn removal) works, this
            // should flatten — a slow node no longer backs packets up in
            // the loopback queue because the router drains directly.
            self.probes
                .loopback_depth
                .set(self.loopback_rx.len() as i64);
            tokio::select! {
                packet = self.client_input_rx.recv() => {
                    match packet {
                        Some(packet) => {
                            debug!("📥 Router received CLIENT packet from '{}' (seq: {})",
                                   packet.from_node, packet.sequence);
                            if let Err(e) = self.route_packet(packet).await {
                                error!("Failed to route packet: {}", e);
                            }
                        }
                        None => {
                            info!("✅ Session router client-input channel closed - shutting down");
                            break;
                        }
                    }
                }
                packet = self.loopback_rx.recv() => {
                    match packet {
                        Some(packet) => {
                            debug!("🔁 Router received LOOPBACK packet from '{}' (seq: {})",
                                   packet.from_node, packet.sequence);
                            if let Err(e) = self.route_packet(packet).await {
                                error!("Failed to route packet: {}", e);
                            }
                        }
                        None => {
                            // Loopback senders live with node tasks and the
                            // router itself. This branch triggers only at
                            // full session teardown — fine to treat as EOS.
                            info!("✅ Session router loopback channel closed - shutting down");
                            break;
                        }
                    }
                }
                _ = self.shutdown_rx.recv() => {
                    info!("🛑 Session router received shutdown signal - stopping all processing");
                    break;
                }
            }
        }

        // Shutdown all node tasks
        self.shutdown_nodes().await;

        Ok(())
    }

    /// Route a packet through the pipeline
    async fn route_packet(&mut self, packet: DataPacket) -> Result<(), Status> {
        // Node outputs pass through SessionControl before normal graph fan-out.
        // This publishes requested taps (including tool/blip/system events)
        // and applies any installed intercept without touching client ingress.
        let packet = if packet.to_node.is_none() {
            if let Some(control) = &self.control {
                let DataPacket {
                    data,
                    from_node,
                    to_node,
                    session_id,
                    sequence,
                    sub_sequence,
                } = packet;
                let Some(data) = control.on_node_output(&from_node, None, data).await else {
                    return Ok(());
                };
                DataPacket {
                    data,
                    from_node,
                    to_node,
                    session_id,
                    sequence,
                    sub_sequence,
                }
            } else {
                packet
            }
        } else {
            packet
        };

        // If to_node is specified, route directly to that node (for client input)
        let downstream_nodes = if let Some(ref to_node) = packet.to_node {
            vec![to_node.clone()]
        } else {
            // Find downstream nodes for this packet based on manifest connections
            let nodes = self.get_downstream_nodes(&packet.from_node).await?;
            if nodes.is_empty() {
                // Terminal node - send to client
                self.send_to_client(packet).await?;
                return Ok(());
            }
            nodes
        };

        // Route to downstream nodes
        for next_node_id in downstream_nodes {
            // info!("🔀 Routing from '{}' → '{}'", packet.from_node, next_node_id);

            // Get or create the node task
            if !self.node_tasks.contains_key(&next_node_id) {
                self.start_node_task(next_node_id.clone()).await?;
            }

            // Send via in-memory channel to the node task
            // The node task will handle both native and multiprocess nodes
            // Multiprocess nodes will internally use the IPC thread via process_streaming_async()
            if let Some(node_input) = self.node_inputs.get(&next_node_id) {
                let packet_clone = packet.clone();
                // Bounded per-node input: `.await` stalls the router task
                // when a node is saturated. Loopback capacity is sized
                // (32× node-input) so the node task can drain onto the
                // loopback even when its input is full — preventing the
                // mutual-await deadlock a naive bounding would create.
                if let Err(e) = node_input.send(packet_clone).await {
                    error!("Failed to send packet to node '{}': {}", next_node_id, e);
                }
            } else {
                error!("No input channel for node '{}'", next_node_id);
            }
        }

        Ok(())
    }

    /// Start a task for a node
    async fn start_node_task(&mut self, node_id: String) -> Result<(), Status> {
        info!("🎯 Starting task for node '{}'", node_id);

        // Get or create the node
        let node = self.get_or_create_node(&node_id).await?;
        // Loadable-plugin factories cannot currently advertise the
        // multi-output capability across the stable FFI factory ABI.  The
        // manifest is therefore authoritative when it explicitly marks a
        // node as streaming; built-in factories retain their capability-based
        // fallback for legacy manifests.
        let manifest_streaming = {
            let session = self.session.lock().await;
            session
                .manifest
                .nodes
                .iter()
                .find(|spec| spec.id == node_id)
                .is_some_and(|spec| spec.is_streaming)
        };
        let is_streaming =
            manifest_streaming || self.registry.is_multi_output_streaming(&node.node_type());

        // Create bounded input channel for this node. See
        // DEFAULT_GRPC_NODE_INPUT_CAPACITY for deadlock-safety sizing
        // (loopback must be ≥ node_input × max_fanout).
        let (input_tx, mut input_rx) = mpsc::channel::<DataPacket>(grpc_node_input_capacity());
        self.node_inputs.insert(node_id.clone(), input_tx);

        // Clone what we need for the task
        let node_id_clone = node_id.clone();
        let session_id = self.session_id.clone();
        let router_tx = self.loopback_tx.clone(); // Bounded loopback: node → router
        let probes = self.probes.clone(); // Phase B0 instrumentation
        let control = self
            .control
            .clone()
            .unwrap_or_else(|| SessionControl::new(session_id.clone()));
        let init_ctx = remotemedia_core::nodes::InitializeContext {
            session_id: session_id.clone(),
            node_id: node_id_clone.clone(),
            control: Some(control.clone()),
        };
        let runtime_ctx = remotemedia_core::nodes::NodeRuntimeContext::for_session(
            session_id.clone(),
            node_id_clone.clone(),
            control,
            node.make_session_state(&init_ctx),
        );

        // Check if this is a multiprocess node and set up continuous output draining
        #[cfg(feature = "multiprocess")]
        let multiprocess_executor = self.multiprocess_executor.clone();
        #[cfg(feature = "multiprocess")]
        let node_type = self.get_node_type(&node_id).await;
        #[cfg(feature = "multiprocess")]
        let is_multiprocess_node = self.registry.is_python_node(&node_type);

        // Spawn the node task
        let task = tokio::spawn(async move {
            info!(
                "⚡ Node '{}' task started (streaming: {})",
                node_id_clone, is_streaming
            );

            // For multiprocess nodes, set up continuous output draining
            #[cfg(feature = "multiprocess")]
            if is_multiprocess_node {
                if let Some(ref executor) = multiprocess_executor {
                    // Create bounded channel for continuous output forwarding.
                    //
                    // The IPC thread `blocking_send`s onto this channel, so a
                    // slow drain back-pressures the iceoryx2 subscriber loop
                    // and then the Python publisher. Capacity of 8 gives ~160
                    // ms headroom at 48 kHz / 20 ms frames — enough to absorb
                    // async scheduling jitter, tight enough to surface real
                    // consumer stalls quickly.
                    let (output_tx, mut output_rx) = tokio::sync::mpsc::channel(
                        remotemedia_core::transport::DEFAULT_ROUTER_OUTPUT_CAPACITY,
                    );

                    // Register callback with IPC thread
                    if let Err(e) = executor
                        .register_output_callback(&node_id_clone, &session_id, output_tx)
                        .await
                    {
                        error!(
                            "Failed to register output callback for node '{}': {}",
                            node_id_clone, e
                        );
                    } else {
                        info!(
                            "✅ Registered continuous output callback for multiprocess node '{}'",
                            node_id_clone
                        );

                        // Spawn background task to drain outputs and forward to router
                        let router_tx_for_drain = router_tx.clone();
                        let node_id_for_drain = node_id_clone.clone();
                        let session_id_for_drain = session_id.clone();

                        tokio::spawn(async move {
                            let mut sub_sequence = 0;
                            while let Some(ipc_output) = output_rx.recv().await {
                                // Skip EndOfInput sentinels — they carry no
                                // payload; only `process_runtime_data_streaming`
                                // uses them to shortcut its wait loop.
                                // WireDataType::EndOfInput = 8
                                if !ipc_output.is_empty() && ipc_output[0] == 8 {
                                    continue;
                                }
                                sub_sequence += 1;

                                // Convert IPC wire bytes to RuntimeData
                                match MultiprocessExecutor::from_ipc_runtime_data(ipc_output) {
                                    Ok(output_data) => {
                                        let output_packet = DataPacket {
                                            data: output_data,
                                            from_node: node_id_for_drain.clone(),
                                            to_node: None,
                                            session_id: session_id_for_drain.clone(),
                                            sequence: 0, // Continuous outputs don't have input sequence
                                            sub_sequence,
                                        };

                                        // Bounded loopback: .await applies
                                        // backpressure when the router is
                                        // behind on dispatching.
                                        if let Err(e) =
                                            router_tx_for_drain.send(output_packet).await
                                        {
                                            error!(
                                                "Failed to forward output from '{}': {}",
                                                node_id_for_drain, e
                                            );
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        error!(
                                            "Failed to convert IPC output for node '{}': {}",
                                            node_id_for_drain, e
                                        );
                                    }
                                }
                            }
                            info!(
                                "Output draining task ended for node '{}'",
                                node_id_for_drain
                            );
                        });
                    }
                }
            }

            while let Some(packet) = input_rx.recv().await {
                debug!(
                    "📦 Node '{}' processing packet (seq: {})",
                    node_id_clone, packet.sequence
                );

                if is_streaming {
                    // Streaming node — inline dispatch.
                    //
                    // Phase B1: the previous implementation
                    // `tokio::spawn`ed this call "for pipelined
                    // execution", but the callback is already sync
                    // (`try_send`, not `.await`), so there was nothing
                    // genuinely async to concurrently drive. The spawn
                    // cost ~one task allocation + one work-stealer
                    // wakeup per packet. Removing it keeps ordering
                    // (outputs are emitted in-sequence) and cedes the
                    // per-packet-spawn budget back.
                    //
                    // Backpressure semantics unchanged: the node task
                    // is the sole driver for this node's input ring;
                    // if the node is slow, its input backs up, and the
                    // router's `route_packet` stalls on
                    // `node_input.send().await` as it did before.
                    let packet_sequence = packet.sequence;
                    let packet_data = packet.data;
                    let node_id_for_cb = node_id_clone.clone();
                    let session_id_for_cb = session_id.clone();
                    let router_tx_for_cb = router_tx.clone();
                    let mut output_count = 0_u64;

                    let node_dispatch_start = std::time::Instant::now();
                    let result = node
                        .process_streaming_async(
                            packet_data,
                            &runtime_ctx,
                            Box::new(move |output| {
                                output_count += 1;
                                let output_packet = DataPacket {
                                    data: output,
                                    from_node: node_id_for_cb.clone(),
                                    to_node: None,
                                    session_id: session_id_for_cb.clone(),
                                    sequence: packet_sequence,
                                    sub_sequence: output_count,
                                };

                                // Sync callback — cannot `.await`.
                                // See comment on the loopback sizing in
                                // `DEFAULT_GRPC_LOOPBACK_CAPACITY`.
                                match router_tx_for_cb.try_send(output_packet) {
                                    Ok(()) => Ok(()),
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                        warn!(
                                            "Loopback full — dropping output from '{}' (session saturated)",
                                            node_id_for_cb
                                        );
                                        Ok(())
                                    }
                                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                        error!(
                                            "Loopback closed while sending output from '{}'",
                                            node_id_for_cb
                                        );
                                        Err(remotemedia_core::Error::Execution(
                                            "Channel closed".into(),
                                        ))
                                    }
                                }
                            }),
                        )
                        .await;
                    probes.node_out.record_since(node_dispatch_start);

                    match result {
                        Ok(count) => {
                            debug!("✅ Node '{}' produced {} outputs", node_id_clone, count);
                        }
                        Err(e) => {
                            error!("Streaming node '{}' failed: {}", node_id_clone, e);
                        }
                    }
                } else {
                    // Non-streaming node - single output.
                    // Phase B0: record total dispatch latency inline (no spawn).
                    let node_dispatch_start = std::time::Instant::now();
                    let process_result = node.process_async(packet.data, &runtime_ctx).await;
                    probes.node_out.record_since(node_dispatch_start);
                    match process_result {
                        Ok(output) => {
                            let output_packet = DataPacket {
                                data: output,
                                from_node: node_id_clone.clone(),
                                to_node: None,
                                session_id: session_id.clone(),
                                sequence: packet.sequence,
                                sub_sequence: 0,
                            };

                            // Send output back to router via bounded loopback.
                            // `.await` is safe: we're already in async context.
                            if let Err(e) = router_tx.send(output_packet).await {
                                error!("Failed to send output from '{}': {}", node_id_clone, e);
                            }
                        }
                        Err(e) => {
                            error!("Node '{}' failed: {}", node_id_clone, e);
                        }
                    }
                }
            }

            info!("⚡ Node '{}' task completed", node_id_clone);
        });

        self.node_tasks.insert(node_id, task);
        Ok(())
    }

    /// Get or create a node
    async fn get_or_create_node(
        &self,
        node_id: &str,
    ) -> Result<Arc<Box<dyn StreamingNode>>, Status> {
        let mut session = self.session.lock().await;

        // Check cache first
        if let Some(cached) = session.node_cache.get(node_id) {
            let cached_node = cached.clone();
            session.cache_hits += 1;
            return Ok(cached_node);
        }

        // Create new node
        session.cache_misses += 1;
        let spec = session
            .manifest
            .nodes
            .iter()
            .find(|n| n.id == node_id)
            .ok_or_else(|| Status::internal(format!("Node spec not found for '{}'", node_id)))?;

        // Pass session_id for multiprocess execution
        let session_id = Some(session.session_id.clone());
        let node = self
            .registry
            .create_node(
                &spec.node_type,
                node_id.to_string(),
                &spec.params,
                session_id,
            )
            .map_err(|e| Status::internal(format!("Failed to create node '{}': {}", node_id, e)))?;

        let arc_node = Arc::new(node);
        session
            .node_cache
            .insert(node_id.to_string(), arc_node.clone());

        Ok(arc_node)
    }

    /// Get downstream nodes for a given node
    async fn get_downstream_nodes(&self, from_node_id: &str) -> Result<Vec<String>, Status> {
        let session = self.session.lock().await;

        let downstream: Vec<String> = session
            .manifest
            .connections
            .iter()
            .filter(|c| c.from == from_node_id)
            .map(|c| c.to.clone())
            .collect();

        Ok(downstream)
    }

    /// Get node type from node ID
    async fn get_node_type(&self, node_id: &str) -> String {
        let session = self.session.lock().await;
        session
            .manifest
            .nodes
            .iter()
            .find(|n| n.id == node_id)
            .map(|n| n.node_type.clone())
            .unwrap_or_else(|| "unknown".to_string())
    }

    /// Send a packet to the client
    async fn send_to_client(&self, packet: DataPacket) -> Result<(), Status> {
        debug!(
            "📤 Sending to client from '{}' (seq: {}.{})",
            packet.from_node, packet.sequence, packet.sub_sequence
        );

        let output_buffer = runtime_data_to_data_buffer(&packet.data);

        let mut data_outputs = HashMap::new();
        data_outputs.insert(packet.from_node, output_buffer);

        let chunk_result = ChunkResult {
            sequence: packet.sequence,
            data_outputs,
            processing_time_ms: 0.0,
            total_items_processed: packet.sub_sequence,
        };

        let response = StreamResponse {
            response: Some(StreamResponseType::Result(chunk_result)),
        };

        self.client_tx
            .send(Ok(response))
            .await
            .map_err(|_| Status::internal("Failed to send to client"))?;

        Ok(())
    }

    /// Shutdown all node tasks
    async fn shutdown_nodes(&mut self) {
        info!("Shutting down {} node tasks", self.node_tasks.len());

        // Close all node input channels
        self.node_inputs.clear();

        // Wait for all tasks to complete
        for (node_id, task) in self.node_tasks.drain() {
            debug!("Waiting for node '{}' to shutdown", node_id);
            let _ = task.await;
        }

        info!("All node tasks shut down");
    }

    /// Feed a chunk from the client into the router.
    ///
    /// Async because the client-ingress channel is bounded — a full queue
    /// stalls the caller until the router catches up.
    pub async fn feed_chunk(
        &self,
        data: RuntimeData,
        from_node_id: String,
        sequence: u64,
    ) -> Result<(), String> {
        let packet = DataPacket {
            data,
            from_node: from_node_id,
            to_node: None,
            session_id: self.session_id.clone(),
            sequence,
            sub_sequence: 0,
        };

        // Bounded client-ingress: `.await` applies real backpressure.
        self.client_input_tx
            .send(packet)
            .await
            .map_err(|e| format!("Failed to feed chunk: {}", e))
    }
}
