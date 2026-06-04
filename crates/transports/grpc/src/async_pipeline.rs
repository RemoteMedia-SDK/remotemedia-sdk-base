//! Async Pipeline Architecture
//!
//! This module implements a truly asynchronous pipeline where each node
//! runs in its own task and communicates via async channels/queues.
//! This allows each node to process as many inputs as it can handle
//! without blocking other nodes.

use remotemedia_core::data::RuntimeData;
use remotemedia_core::nodes::StreamingNode;
use remotemedia_core::{Error, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

/// Represents a single item flowing through the pipeline
#[derive(Clone, Debug)]
pub struct PipelineItem {
    /// Unique ID for this item (for tracing)
    pub id: String,
    /// The actual data
    pub data: RuntimeData,
    /// Session ID if any
    pub session_id: Option<String>,
    /// Sequence number for ordering
    pub sequence: u64,
}

/// A node wrapper that runs in its own task with input/output queues
pub struct AsyncNodeExecutor {
    /// Node ID
    pub node_id: String,
    /// The actual node implementation
    pub node: Arc<dyn StreamingNode>,
    /// Input queue receiver
    pub input_rx: UnboundedReceiver<PipelineItem>,
    /// Output queue senders (can have multiple downstream nodes)
    pub output_txs: Vec<UnboundedSender<PipelineItem>>,
    /// Whether this node produces multiple outputs per input (streaming)
    pub is_streaming: bool,
}

impl AsyncNodeExecutor {
    /// Create a new async node executor
    pub fn new(
        node_id: String,
        node: Arc<dyn StreamingNode>,
        input_rx: UnboundedReceiver<PipelineItem>,
        output_txs: Vec<UnboundedSender<PipelineItem>>,
        is_streaming: bool,
    ) -> Self {
        Self {
            node_id,
            node,
            input_rx,
            output_txs,
            is_streaming,
        }
    }

    /// Run the node executor in its own task
    /// This continuously processes items from the input queue
    pub async fn run(mut self) -> Result<()> {
        info!(
            "🚀 AsyncNodeExecutor '{}' starting (streaming: {})",
            self.node_id, self.is_streaming
        );

        while let Some(item) = self.input_rx.recv().await {
            debug!(
                "📥 Node '{}' received item {} (seq: {})",
                self.node_id, item.id, item.sequence
            );

            if self.is_streaming {
                // Streaming node - use callback to emit multiple outputs
                let output_txs = self.output_txs.clone();
                let node_id = self.node_id.clone();
                let item_id = item.id.clone();
                let sequence = item.sequence;

                let mut output_count = 0;
                let ctx = remotemedia_core::nodes::NodeRuntimeContext::for_test(
                    item.session_id.clone().unwrap_or_default(),
                    self.node_id.clone(),
                );
                let result = self
                    .node
                    .process_streaming_async(
                        item.data,
                        &ctx,
                        Box::new(move |output| {
                            output_count += 1;
                            let output_item = PipelineItem {
                                id: format!("{}_output_{}", item_id, output_count),
                                data: output,
                                session_id: item.session_id.clone(),
                                sequence: sequence * 1000 + output_count as u64, // Sub-sequence for ordering
                            };

                            debug!(
                                "📤 Node '{}' emitting output {} (seq: {})",
                                node_id, output_item.id, output_item.sequence
                            );

                            // Send to all downstream nodes
                            for tx in &output_txs {
                                if let Err(e) = tx.send(output_item.clone()) {
                                    error!("Failed to send output from '{}': {}", node_id, e);
                                    return Err(Error::Execution(format!(
                                        "Channel send failed: {}",
                                        e
                                    )));
                                }
                            }
                            Ok(())
                        }),
                    )
                    .await;

                if let Err(e) = result {
                    error!(
                        "Streaming node '{}' failed processing item {}: {}",
                        self.node_id, item.id, e
                    );
                }

                info!(
                    "✅ Node '{}' streamed {} outputs for item {}",
                    self.node_id, output_count, item.id
                );
            } else {
                // Non-streaming node - single output
                let ctx = remotemedia_core::nodes::NodeRuntimeContext::for_test(
                    item.session_id.clone().unwrap_or_default(),
                    self.node_id.clone(),
                );
                match self.node.process_async(item.data, &ctx).await {
                    Ok(output) => {
                        let output_item = PipelineItem {
                            id: format!("{}_output", item.id),
                            data: output,
                            session_id: item.session_id,
                            sequence: item.sequence,
                        };

                        debug!(
                            "📤 Node '{}' emitting single output {} (seq: {})",
                            self.node_id, output_item.id, output_item.sequence
                        );

                        // Send to all downstream nodes
                        for tx in &self.output_txs {
                            if let Err(e) = tx.send(output_item.clone()) {
                                error!("Failed to send output from '{}': {}", self.node_id, e);
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Node '{}' failed processing item {}: {}",
                            self.node_id, item.id, e
                        );
                    }
                }
            }
        }

        info!(
            "🛑 AsyncNodeExecutor '{}' shutting down (input channel closed)",
            self.node_id
        );
        Ok(())
    }
}

/// Pipeline builder for constructing async pipelines
pub struct AsyncPipelineBuilder {
    /// All nodes in the pipeline
    nodes: HashMap<String, Arc<dyn StreamingNode>>,
    /// Connections between nodes (from_node -> vec of to_nodes)
    connections: HashMap<String, Vec<String>>,
    /// Terminal nodes (nodes with no downstream)
    terminal_nodes: Vec<String>,
    /// Streaming node types
    streaming_types: Vec<String>,
}

impl AsyncPipelineBuilder {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            connections: HashMap::new(),
            terminal_nodes: Vec::new(),
            streaming_types: Vec::new(),
        }
    }

    /// Add a node to the pipeline
    pub fn add_node(&mut self, node_id: String, node: Arc<dyn StreamingNode>) -> &mut Self {
        self.nodes.insert(node_id, node);
        self
    }

    /// Connect two nodes
    pub fn connect(&mut self, from: String, to: String) -> &mut Self {
        self.connections
            .entry(from)
            .or_insert_with(Vec::new)
            .push(to);
        self
    }

    /// Mark a node as terminal (output goes to client)
    pub fn mark_terminal(&mut self, node_id: String) -> &mut Self {
        self.terminal_nodes.push(node_id);
        self
    }

    /// Register a streaming node type
    pub fn register_streaming_type(&mut self, node_type: String) -> &mut Self {
        self.streaming_types.push(node_type);
        self
    }

    /// Build and start the pipeline
    pub fn build(self) -> AsyncPipeline {
        let mut node_inputs: HashMap<String, UnboundedReceiver<PipelineItem>> = HashMap::new();
        let mut node_outputs: HashMap<String, Vec<UnboundedSender<PipelineItem>>> = HashMap::new();
        #[allow(unused_assignments)]
        let mut client_output_rx = None;

        // Create channels for all nodes
        for (node_id, _) in &self.nodes {
            let (_tx, rx) = unbounded_channel();
            node_inputs.insert(node_id.clone(), rx);

            // Store the sender for upstream nodes to use
            if let Some(_upstream_nodes) = self
                .connections
                .iter()
                .filter(|(_, downstream)| downstream.contains(node_id))
                .map(|(upstream, _)| upstream.clone())
                .collect::<Vec<_>>()
                .first()
            {
                // Node has upstream connections
            } else {
                // This is an input node - we'll need to keep its sender
            }
        }

        // Create output channel for terminal nodes
        let (client_tx, client_rx) = unbounded_channel();
        client_output_rx = Some(client_rx);

        // Wire up connections
        for (from_node, to_nodes) in &self.connections {
            let mut outputs = Vec::new();
            for to_node in to_nodes {
                if let Some(_rx) = node_inputs.remove(to_node) {
                    let (tx, new_rx) = unbounded_channel();
                    outputs.push(tx);
                    node_inputs.insert(to_node.clone(), new_rx);
                }
            }
            node_outputs.insert(from_node.clone(), outputs);
        }

        // Add client output for terminal nodes
        for terminal in &self.terminal_nodes {
            node_outputs
                .entry(terminal.clone())
                .or_insert_with(Vec::new)
                .push(client_tx.clone());
        }

        // Start all node executors
        let mut tasks = Vec::new();
        for (node_id, node) in self.nodes {
            if let Some(input_rx) = node_inputs.remove(&node_id) {
                let output_txs = node_outputs.remove(&node_id).unwrap_or_default();
                let is_streaming = self
                    .streaming_types
                    .iter()
                    .any(|t| node.node_type().contains(t));

                let executor = AsyncNodeExecutor::new(
                    node_id.clone(),
                    node,
                    input_rx,
                    output_txs,
                    is_streaming,
                );

                let task = tokio::spawn(async move {
                    if let Err(e) = executor.run().await {
                        error!("Node executor failed: {}", e);
                    }
                });

                tasks.push(task);
            }
        }

        AsyncPipeline {
            tasks,
            client_output: client_output_rx.unwrap(),
            input_senders: HashMap::new(), // Would need to track input nodes
        }
    }
}

/// Represents a running async pipeline
pub struct AsyncPipeline {
    /// All running node tasks
    tasks: Vec<JoinHandle<()>>,
    /// Client output receiver
    client_output: UnboundedReceiver<PipelineItem>,
    /// Input senders for feeding data into the pipeline
    input_senders: HashMap<String, UnboundedSender<PipelineItem>>,
}

impl AsyncPipeline {
    /// Send input to the pipeline
    pub async fn send_input(&self, node_id: &str, item: PipelineItem) -> Result<()> {
        if let Some(tx) = self.input_senders.get(node_id) {
            tx.send(item)
                .map_err(|e| Error::Execution(format!("Failed to send input: {}", e)))?;
        } else {
            return Err(Error::Execution(format!(
                "No input sender for node '{}'",
                node_id
            )));
        }
        Ok(())
    }

    /// Receive output from the pipeline
    pub async fn recv_output(&mut self) -> Option<PipelineItem> {
        self.client_output.recv().await
    }

    /// Shutdown the pipeline
    pub async fn shutdown(self) {
        // Dropping input senders will cause nodes to exit
        drop(self.input_senders);

        // Wait for all tasks to complete
        for task in self.tasks {
            let _ = task.await;
        }
    }
}
