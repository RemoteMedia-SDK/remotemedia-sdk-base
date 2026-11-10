//! Unary `executePipeline` on a real `PipelineExecutor` session (task 6.2).
//!
//! The legacy executor this replaces created nodes with test contexts and no
//! copy authority. A unary call now runs the same session as
//! `createStreamingSession`: node initialization, buffer preflight, strict or
//! compatibility policy, attributed copy ledgers and joined cleanup.
//!
//! Its result keeps the unary contract: the last output of every sink, keyed by
//! node id. Session client output does not identify the producing node, so each
//! sink gets a hidden in-process collector that retains that sink's last output
//! by owner (no copy). Inputs address source nodes by id; input to a node with
//! upstream connections is rejected rather than silently merged.

use super::buffer_config::PipelineBufferConfig;
use napi::{Error, Result};
use remotemedia_core::data::RuntimeData;
use remotemedia_core::manifest::Manifest;
use remotemedia_core::nodes::{NodeRuntimeContextRead, StreamingNode, StreamingNodeFactory};
use remotemedia_core::transport::{NodeMaterializationCopies, TransportData};
use remotemedia_traits::buffer_contract::{
    BufferContract, BufferPortContract, BufferProduction, BufferRetention,
};
use remotemedia_types::CopyContext;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

const COLLECTOR: &str = "__UnaryOutputCollector";
const COLLECTOR_PREFIX: &str = "__unary_output__";

type Collected = Arc<Mutex<HashMap<String, RuntimeData>>>;

/// Outputs and the copy authority that governed them.
pub(crate) struct UnaryResult {
    pub outputs: HashMap<String, RuntimeData>,
    pub transport: CopyContext,
    pub nodes: NodeMaterializationCopies,
}

struct Collector {
    sink: String,
    collected: Collected,
}

#[async_trait::async_trait]
impl StreamingNode for Collector {
    fn node_type(&self) -> &str {
        COLLECTOR
    }
    fn is_multi_input(&self) -> bool {
        false
    }
    async fn process_async(
        &self,
        data: RuntimeData,
        _: &dyn NodeRuntimeContextRead,
    ) -> std::result::Result<RuntimeData, remotemedia_core::Error> {
        // Never reached: the router dispatches through process_streaming_async.
        Ok(data)
    }
    async fn process_multi_async(
        &self,
        _: HashMap<String, RuntimeData>,
        _: &dyn NodeRuntimeContextRead,
    ) -> std::result::Result<RuntimeData, remotemedia_core::Error> {
        Err(remotemedia_core::Error::Execution(
            "unary output collector is single-input".into(),
        ))
    }
    async fn process_streaming_async(
        &self,
        data: RuntimeData,
        _: &dyn NodeRuntimeContextRead,
        _: Box<dyn FnMut(RuntimeData) -> std::result::Result<(), remotemedia_core::Error> + Send>,
    ) -> std::result::Result<usize, remotemedia_core::Error> {
        // Retain the sink's latest output by owner; emit nothing.
        self.collected
            .lock()
            .unwrap()
            .insert(self.sink.clone(), data);
        Ok(0)
    }
}

/// Builds one collector per sink. Its buffer contract accepts exactly what its
/// sink's default output port declares, so strict preflight still checks the
/// whole graph.
struct CollectorFactory {
    collected: Collected,
    contracts: HashMap<String, Option<BufferContract>>,
}

impl StreamingNodeFactory for CollectorFactory {
    fn node_type(&self) -> &str {
        COLLECTOR
    }
    fn buffer_contract(&self, params: &serde_json::Value) -> Option<BufferContract> {
        let sink = params["sink"].as_str()?;
        let sink_contract = self.contracts.get(sink)?.as_ref()?;
        let output = sink_contract.outputs.get("")?.clone();
        let metadata = BufferPortContract {
            layouts: Vec::new(),
            domains: Vec::new(),
            max_payload_bytes: 0,
        };
        let bytes = output.max_payload_bytes;
        Some(BufferContract {
            version: sink_contract.version,
            protocol: sink_contract.protocol.clone(),
            inputs: BTreeMap::from([(String::new(), output)]),
            outputs: BTreeMap::from([(String::new(), metadata)]),
            production: BufferProduction::PrivateStorage,
            retention: Some(BufferRetention { owners: 1, bytes }),
        })
    }
    fn create(
        &self,
        _node_id: String,
        params: &serde_json::Value,
        _session_id: Option<String>,
    ) -> std::result::Result<Box<dyn StreamingNode>, remotemedia_core::Error> {
        let sink = params["sink"]
            .as_str()
            .ok_or_else(|| remotemedia_core::Error::ConfigError("collector needs a sink".into()))?;
        Ok(Box::new(Collector {
            sink: sink.to_string(),
            collected: self.collected.clone(),
        }))
    }
}

fn reason(message: impl std::fmt::Display) -> Error {
    Error::from_reason(message.to_string())
}

/// Execute `manifest_json` once: each entry of `inputs` is sent to the named
/// source node, input completes, and the session drains and joins.
pub(crate) async fn execute(
    manifest_json: &str,
    inputs: HashMap<String, RuntimeData>,
    session_prefix: Option<String>,
    pipeline_buffers: Option<&PipelineBufferConfig>,
) -> Result<UnaryResult> {
    let mut graph: serde_json::Value = serde_json::from_str(manifest_json)
        .map_err(|e| reason(format!("Failed to parse manifest: {e}")))?;
    let manifest: Manifest = serde_json::from_value(graph.clone())
        .map_err(|e| reason(format!("Failed to parse manifest: {e}")))?;
    let ids: HashSet<&str> = manifest.nodes.iter().map(|n| n.id.as_str()).collect();
    let with_upstream: HashSet<&str> = manifest.connections.iter().map(|c| c.to.as_str()).collect();
    let with_downstream: HashSet<&str> = manifest
        .connections
        .iter()
        .map(|c| c.from.as_str())
        .collect();
    for node in inputs.keys() {
        if !ids.contains(node.as_str()) {
            return Err(reason(format!("Input for unknown node '{node}'")));
        }
        if with_upstream.contains(node.as_str()) {
            return Err(reason(format!(
                "Unary input for '{node}' must target a source node; it has upstream connections"
            )));
        }
    }
    for node in &manifest.nodes {
        if !with_upstream.contains(node.id.as_str()) && !inputs.contains_key(&node.id) {
            return Err(reason(format!("No inputs for node '{}'", node.id)));
        }
    }

    let executor =
        super::streaming_session::plugin_executor(pipeline_buffers, session_prefix).await?;
    let sinks: Vec<NodeRef> = manifest
        .nodes
        .iter()
        .filter(|n| !with_downstream.contains(n.id.as_str()))
        .map(|n| NodeRef::new(&n.id, &n.node_type, &n.params))
        .collect();
    let contracts = {
        let registry = executor.registry().read().await;
        let mut contracts = HashMap::new();
        for sink in &sinks {
            // An unknown type or invalid parameters fail at session creation,
            // with the registry's own diagnostic.
            let contract = match registry.get_factory(&sink.node_type) {
                Some(factory) => factory.try_buffer_contract(&sink.params).unwrap_or(None),
                None => None,
            };
            contracts.insert(sink.id.clone(), contract);
        }
        contracts
    };
    let collected = Collected::default();
    executor
        .register_factory(Arc::new(CollectorFactory {
            collected: collected.clone(),
            contracts,
        }))
        .await;
    let nodes = graph["nodes"]
        .as_array_mut()
        .ok_or_else(|| reason("manifest nodes must be an array"))?;
    for sink in &sinks {
        nodes.push(serde_json::json!({
            "id": format!("{COLLECTOR_PREFIX}{}", sink.id),
            "node_type": COLLECTOR,
            "params": {"sink": sink.id},
        }));
    }
    if graph.get("connections").is_none() {
        graph["connections"] = serde_json::json!([]);
    }
    let connections = graph["connections"]
        .as_array_mut()
        .ok_or_else(|| reason("manifest connections must be an array"))?;
    for sink in &sinks {
        connections.push(serde_json::json!({
            "from": sink.id,
            "to": format!("{COLLECTOR_PREFIX}{}", sink.id),
        }));
    }
    let manifest: Manifest = serde_json::from_value(graph)
        .map_err(|e| reason(format!("Failed to parse manifest: {e}")))?;

    let mut session = executor
        .create_session(Arc::new(manifest))
        .await
        .map_err(|e| reason(format!("Pipeline execution failed: {e}")))?;
    let transport = session.transport_copy_context();
    let node_copies = session.node_materialization_copies();
    let control = executor.control_bus().get(&session.session_id);
    let sent = async {
        for (node, data) in inputs {
            session
                .send_input_to(&node, TransportData::new(data))
                .await
                .map_err(|e| reason(format!("Pipeline execution failed: {e}")))?;
        }
        Ok::<(), Error>(())
    }
    .await;
    session.signal_input_complete();
    // Join even when sending failed, so node cleanup always runs.
    let joined = session
        .wait()
        .await
        .map_err(|e| reason(format!("Pipeline execution failed: {e}")));
    sent?;
    joined?;
    if let Some(telemetry) = control.as_ref().and_then(|c| c.telemetry()) {
        let errors = telemetry.node_errors();
        if !errors.is_empty() {
            let detail: Vec<String> = errors
                .into_iter()
                .map(|(node, error)| format!("node '{node}': {}", error.message))
                .collect();
            return Err(reason(format!(
                "Pipeline execution failed: {}",
                detail.join("; ")
            )));
        }
    }
    let outputs = std::mem::take(&mut *collected.lock().unwrap());
    Ok(UnaryResult {
        outputs,
        transport,
        nodes: node_copies,
    })
}

struct NodeRef {
    id: String,
    node_type: String,
    params: serde_json::Value,
}

impl NodeRef {
    fn new(id: &str, node_type: &str, params: &serde_json::Value) -> Self {
        Self {
            id: id.to_string(),
            node_type: node_type.to_string(),
            params: params.clone(),
        }
    }
}
