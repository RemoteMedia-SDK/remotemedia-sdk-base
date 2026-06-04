//! Async Router for Streaming Pipeline
//!
//! This module replaces the blocking route_to_downstream with a truly
//! asynchronous implementation that processes each yielded item immediately.

// Internal infrastructure - some fields reserved for future use
#![allow(dead_code)]
// StreamSession is intentionally kept private for internal use
#![allow(private_interfaces)]

use crate::adapters::runtime_data_to_data_buffer;
use crate::generated::{
    stream_response::Response as StreamResponseType, ChunkResult, StreamResponse,
};
use crate::streaming::StreamSession;
use remotemedia_core::data::RuntimeData;
use remotemedia_core::nodes::{StreamingNode, StreamingNodeRegistry};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tonic::Status;
use tracing::{debug, error, info};

/// Result type for routing operations
type RouterResult = Result<(), Status>;

/// Represents a data packet flowing through the pipeline
#[derive(Clone, Debug)]
struct DataPacket {
    /// The actual data
    data: RuntimeData,
    /// Source node ID
    from_node: String,
    /// Session ID if any
    session_id: Option<String>,
    /// Sequence number for ordering
    sequence: u64,
    /// Sub-sequence for streaming outputs
    sub_sequence: u64,
}

/// Node runner that processes items asynchronously
struct NodeRunner {
    node_id: String,
    node: Arc<Box<dyn StreamingNode>>,
    is_streaming: bool,
    session_id: Option<String>,
}

impl NodeRunner {
    /// Process a single packet and emit outputs
    async fn process_packet(
        &self,
        packet: DataPacket,
        output_tx: mpsc::Sender<DataPacket>,
    ) -> RouterResult {
        if self.is_streaming {
            // Streaming node - emit multiple outputs
            info!(
                "🔄 Streaming node '{}' processing packet (seq: {})",
                self.node_id, packet.sequence
            );

            let mut output_count = 0;
            let node_id = self.node_id.clone();
            let base_sequence = packet.sequence;
            let session_id = packet.session_id.clone();

            let ctx = remotemedia_core::nodes::NodeRuntimeContext::for_test(
                session_id.clone().unwrap_or_default(),
                self.node_id.clone(),
            );
            let result = self
                .node
                .process_streaming_async(
                    packet.data,
                    &ctx,
                    Box::new(move |output| {
                        output_count += 1;
                        let output_packet = DataPacket {
                            data: output,
                            from_node: node_id.clone(),
                            session_id: session_id.clone(),
                            sequence: base_sequence,
                            sub_sequence: output_count,
                        };

                        debug!(
                            "📤 Node '{}' yielding output {} (seq: {}.{})",
                            node_id, output_count, base_sequence, output_count
                        );

                        // Sync streaming callback can't .await on the bounded
                        // channel. Use try_send; on overflow the async router's
                        // sizing (DEFAULT_GRPC_LOOPBACK_CAPACITY, 256) has been
                        // exceeded — log and drop rather than hang.
                        match output_tx.try_send(output_packet) {
                            Ok(()) => Ok(()),
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                tracing::warn!(
                                    "Async router queue full — dropping output from '{}'",
                                    node_id
                                );
                                Ok(())
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                error!("Output channel closed for '{}'", node_id);
                                Err(remotemedia_core::Error::Execution("Channel closed".into()))
                            }
                        }
                    }),
                )
                .await;

            match result {
                Ok(count) => {
                    info!("✅ Node '{}' streamed {} outputs", self.node_id, count);
                    Ok(())
                }
                Err(e) => {
                    error!("Streaming node '{}' failed: {}", self.node_id, e);
                    Err(Status::internal(format!("Node failed: {}", e)))
                }
            }
        } else {
            // Non-streaming node - single output
            let ctx = remotemedia_core::nodes::NodeRuntimeContext::for_test(
                packet.session_id.clone().unwrap_or_default(),
                self.node_id.clone(),
            );
            match self.node.process_async(packet.data, &ctx).await {
                Ok(output) => {
                    let output_packet = DataPacket {
                        data: output,
                        from_node: self.node_id.clone(),
                        session_id: packet.session_id,
                        sequence: packet.sequence,
                        sub_sequence: 0,
                    };

                    debug!(
                        "📤 Node '{}' emitting single output (seq: {})",
                        self.node_id, packet.sequence
                    );

                    // Bounded channel in async context: `.await` is safe.
                    output_tx
                        .send(output_packet)
                        .await
                        .map_err(|e| Status::internal(format!("Send failed: {}", e)))?;
                    Ok(())
                }
                Err(e) => {
                    error!("Node '{}' failed: {}", self.node_id, e);
                    Err(Status::internal(format!("Node failed: {}", e)))
                }
            }
        }
    }
}

/// Async router that manages the pipeline execution
pub struct AsyncRouter {
    /// Registry for creating nodes
    registry: Arc<StreamingNodeRegistry>,
    /// Active sessions
    session: Arc<Mutex<StreamSession>>,
    /// Client output sender
    client_tx: mpsc::Sender<Result<StreamResponse, Status>>,
    /// Running tasks
    tasks: Vec<JoinHandle<()>>,
}

impl AsyncRouter {
    /// Create a new async router
    pub fn new(
        registry: Arc<StreamingNodeRegistry>,
        session: Arc<Mutex<StreamSession>>,
        client_tx: mpsc::Sender<Result<StreamResponse, Status>>,
    ) -> Self {
        Self {
            registry,
            session,
            client_tx,
            tasks: Vec::new(),
        }
    }

    /// Start routing from a node - this is the main entry point
    pub async fn route_from_node(
        &mut self,
        initial_data: RuntimeData,
        from_node_id: String,
        session_id: Option<String>,
        base_sequence: u64,
    ) -> RouterResult {
        info!("🚀 Starting async routing from node '{}'", from_node_id);

        // Create a bounded internal routing channel. Matches the sibling
        // `SessionRouter` loopback sizing so a runaway fan-out is
        // surfaced rather than buffered unboundedly. See
        // `DEFAULT_GRPC_LOOPBACK_CAPACITY` in session_router.rs.
        let (router_tx, mut router_rx) =
            mpsc::channel::<DataPacket>(crate::session_router::DEFAULT_GRPC_LOOPBACK_CAPACITY);

        // Start initial packet
        let initial_packet = DataPacket {
            data: initial_data,
            from_node: from_node_id.clone(),
            session_id: session_id.clone(),
            sequence: base_sequence,
            sub_sequence: 0,
        };

        // Process the initial packet and any resulting packets.
        // `.await` on a bounded channel is safe here: nothing else is
        // holding the sender, so a full queue only means the loop below
        // needs to drain first.
        router_tx
            .send(initial_packet)
            .await
            .map_err(|e| Status::internal(format!("Failed to send initial packet: {}", e)))?;

        // Process packets as they arrive (including those generated by streaming nodes)
        while let Some(packet) = router_rx.recv().await {
            // Find downstream node for this packet
            let downstream_node = self.get_downstream_node(&packet.from_node).await?;

            if let Some((next_node_id, next_node)) = downstream_node {
                // There's a downstream node - process through it
                info!(
                    "🔀 Routing from '{}' → '{}'",
                    packet.from_node, next_node_id
                );

                // Check if it's a streaming node
                let is_streaming = self
                    .registry
                    .is_multi_output_streaming(&next_node.node_type());

                // Create a node runner
                let runner = NodeRunner {
                    node_id: next_node_id.clone(),
                    node: next_node,
                    is_streaming,
                    session_id: packet.session_id.clone(),
                };

                // Clone the channel for the async task - outputs will be sent back to router_rx
                let router_tx_clone = router_tx.clone();
                let packet_clone = packet.clone();

                // Process in a separate task so we don't block receiving more packets
                // This task will emit outputs back to router_tx, which we'll process in this loop
                let task = tokio::spawn(async move {
                    if let Err(e) = runner.process_packet(packet_clone, router_tx_clone).await {
                        error!("Node '{}' failed: {}", runner.node_id, e);
                    }
                });

                self.tasks.push(task);
            } else {
                // No downstream - this is a terminal node, send to client
                info!(
                    "📤 Terminal output from '{}' (seq: {}.{})",
                    packet.from_node, packet.sequence, packet.sub_sequence
                );

                self.send_to_client(packet).await?;
            }
        }

        // Wait for all tasks to complete
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }

        info!("✅ Async routing complete");
        Ok(())
    }

    /// Get the downstream node for a given node ID
    async fn get_downstream_node(
        &self,
        from_node_id: &str,
    ) -> Result<Option<(String, Arc<Box<dyn StreamingNode>>)>, Status> {
        let mut session = self.session.lock().await;

        // Find connection in manifest
        let next_node_id = session
            .manifest
            .connections
            .iter()
            .find(|c| c.from == from_node_id)
            .map(|c| c.to.clone());

        if let Some(next_id) = next_node_id {
            // Check cache first
            if let Some(cached) = session.node_cache.get(&next_id) {
                let cached_node = cached.clone();
                session.cache_hits += 1;
                Ok(Some((next_id, cached_node)))
            } else {
                // Create new node
                session.cache_misses += 1;
                let spec = session
                    .manifest
                    .nodes
                    .iter()
                    .find(|n| n.id == next_id)
                    .ok_or_else(|| {
                        Status::internal(format!("Node spec not found for '{}'", next_id))
                    })?;

                // Pass session_id for multiprocess execution
                let session_id = Some(session.session_id.clone());
                let node = self
                    .registry
                    .create_node(&spec.node_type, next_id.clone(), &spec.params, session_id)
                    .map_err(|e| {
                        Status::internal(format!("Failed to create node '{}': {}", next_id, e))
                    })?;

                let arc_node = Arc::new(node);
                session.node_cache.insert(next_id.clone(), arc_node.clone());
                Ok(Some((next_id, arc_node)))
            }
        } else {
            Ok(None)
        }
    }

    /// Send a packet to the client
    async fn send_to_client(&self, packet: DataPacket) -> RouterResult {
        debug!("Converting RuntimeData to proto...");
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

        debug!("✅ Sent to client");
        Ok(())
    }
}

/// Replacement for the old route_to_downstream function
pub async fn route_to_downstream_async(
    output_data: RuntimeData,
    from_node_id: String,
    session: Arc<Mutex<StreamSession>>,
    streaming_registry: Arc<StreamingNodeRegistry>,
    tx: mpsc::Sender<Result<StreamResponse, Status>>,
    session_id: String,
    base_sequence: u64,
) -> Result<(), Status> {
    info!(
        "🚀 Starting async route_to_downstream from '{}'",
        from_node_id
    );

    let mut router = AsyncRouter::new(streaming_registry, session, tx);
    router
        .route_from_node(output_data, from_node_id, Some(session_id), base_sequence)
        .await
}
