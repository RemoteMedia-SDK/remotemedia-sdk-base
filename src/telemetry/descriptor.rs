//! Serializable description of a running session's topology.

use super::identity::{params_hash, NodeImplementation};
use super::redact::redact_params;
use crate::capabilities::MediaCapabilities;
use crate::executor::PipelineGraph;
use crate::manifest::Manifest;
use crate::nodes::ports::PortKind;
use crate::nodes::StreamingNodeRegistry;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Virtual node id used as the far end of `ingress` / `egress` edges.
pub const CLIENT_NODE_ID: &str = "__client__";

/// Running topology of one session. Built from the post-negotiation manifest
/// so auto-inserted conversion nodes are visible.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct SessionDescriptor {
    pub schema_version: u16,
    pub session_id: String,
    pub pipeline_name: String,
    pub created_at_ms: u64,
    pub nodes: Vec<NodeDescriptor>,
    /// Edges. `edges[i].idx == i`. The first `manifest.connections.len()`
    /// entries follow manifest connection order; ingress / egress edges to
    /// the virtual [`CLIENT_NODE_ID`] node follow.
    pub edges: Vec<EdgeDescriptor>,
    pub sources: Vec<String>,
    pub sinks: Vec<String>,
    /// Post-negotiation manifest with sensitive params redacted.
    pub manifest: serde_json::Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct NodeDescriptor {
    pub id: String,
    pub node_type: String,
    pub is_streaming: bool,
    pub is_output_node: bool,
    /// `reactive`, `clocked_to_outbound_media`, `source_wall(30)`; `None`
    /// until nodes are initialized.
    pub pacing: Option<String>,
    /// `true` for nodes spliced in by capability negotiation (or other
    /// manifest rewriting) rather than written by the manifest author.
    pub inserted_by_negotiation: bool,
    pub input_ports: Vec<PortDescriptor>,
    /// Statically declared / configured media capabilities, when known.
    pub capabilities: Option<MediaCapabilities>,
    /// Node params with sensitive values redacted.
    pub params: serde_json::Value,
    pub queue_capacity: QueueCapacity,
    /// Which build of the node type this session runs.
    pub implementation: NodeImplementation,
    /// `sha256:<hex>` of the effective params (post-override, key-order
    /// independent; redacted values contribute only the marker).
    pub params_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct QueueCapacity {
    /// Router → filter-task channel.
    pub input: u32,
    /// Filter-task → main-task channel (where a slow node's backlog grows).
    pub filter: u32,
    /// Node emit callback → fan-out task channel.
    pub fanout: u32,
}

impl Default for QueueCapacity {
    fn default() -> Self {
        Self {
            input: 16,
            filter: 1024,
            fanout: 1024,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct PortDescriptor {
    pub name: String,
    /// `stream` or `snapshot`.
    pub kind: String,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EdgeKind {
    #[default]
    Stream,
    Snapshot,
    /// Client / transport → source node.
    Ingress,
    /// Sink / output node → client / transport.
    Egress,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct EdgeDescriptor {
    pub idx: u32,
    pub from: String,
    pub from_port: Option<String>,
    pub to: String,
    pub to_port: Option<String>,
    pub kind: EdgeKind,
}

impl SessionDescriptor {
    /// Build a descriptor for `manifest` as wired by the session router.
    ///
    /// `queue_capacity` is the per-node channel sizing the router uses.
    pub fn build(
        session_id: &str,
        manifest: &Manifest,
        graph: &PipelineGraph,
        registry: &StreamingNodeRegistry,
        queue_capacity: QueueCapacity,
    ) -> Self {
        let nodes = manifest
            .nodes
            .iter()
            .map(|n| {
                let factory = registry.get_factory(&n.node_type);
                let mut input_ports: Vec<PortDescriptor> = factory
                    .map(|f| {
                        f.input_port_kinds()
                            .into_iter()
                            .map(|(name, kind)| PortDescriptor {
                                name,
                                kind: port_kind_str(kind).to_string(),
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                input_ports.sort_by(|a, b| a.name.cmp(&b.name));
                let capabilities = registry
                    .get_media_capabilities(&n.node_type, &n.params)
                    .or_else(|| n.media_capabilities.clone());
                NodeDescriptor {
                    id: n.id.clone(),
                    node_type: n.node_type.clone(),
                    is_streaming: n.is_streaming,
                    is_output_node: n.is_output_node,
                    pacing: None,
                    inserted_by_negotiation: false,
                    input_ports,
                    capabilities,
                    params: redact_params(&n.params),
                    queue_capacity: queue_capacity.clone(),
                    implementation: registry.implementation(&n.node_type),
                    params_hash: params_hash(&n.params),
                }
            })
            .collect();

        let mut edges: Vec<EdgeDescriptor> = Vec::with_capacity(manifest.connections.len() + 4);
        for conn in &manifest.connections {
            let to_port_name = conn.to_port.clone().unwrap_or_else(|| "default".into());
            let to_kind = graph
                .nodes
                .get(&conn.to)
                .and_then(|gn| registry.get_factory(&gn.node_type))
                .and_then(|f| f.input_port_kinds().get(&to_port_name).copied())
                .unwrap_or(PortKind::Stream);
            edges.push(EdgeDescriptor {
                idx: edges.len() as u32,
                from: conn.from.clone(),
                from_port: conn.from_port.clone(),
                to: conn.to.clone(),
                to_port: conn.to_port.clone(),
                kind: match to_kind {
                    PortKind::Stream => EdgeKind::Stream,
                    PortKind::Snapshot => EdgeKind::Snapshot,
                },
            });
        }
        for src in &graph.sources {
            edges.push(EdgeDescriptor {
                idx: edges.len() as u32,
                from: CLIENT_NODE_ID.into(),
                from_port: None,
                to: src.clone(),
                to_port: None,
                kind: EdgeKind::Ingress,
            });
        }
        let sinks: HashSet<&String> = graph.sinks.iter().collect();
        for n in &manifest.nodes {
            if sinks.contains(&n.id) || n.is_output_node {
                edges.push(EdgeDescriptor {
                    idx: edges.len() as u32,
                    from: n.id.clone(),
                    from_port: None,
                    to: CLIENT_NODE_ID.into(),
                    to_port: None,
                    kind: EdgeKind::Egress,
                });
            }
        }

        let manifest_json = serde_json::to_value(manifest)
            .map(|mut v| {
                if let Some(nodes) = v.get_mut("nodes").and_then(|n| n.as_array_mut()) {
                    for node in nodes {
                        if let Some(params) = node.get_mut("params") {
                            *params = redact_params(params);
                        }
                    }
                }
                v
            })
            .unwrap_or(serde_json::Value::Null);

        Self {
            schema_version: super::SCHEMA_VERSION,
            session_id: session_id.to_string(),
            pipeline_name: manifest.metadata.name.clone(),
            created_at_ms: super::unix_ms(),
            nodes,
            edges,
            sources: graph.sources.clone(),
            sinks: graph.sinks.clone(),
            manifest: manifest_json,
        }
    }

    /// Flag the given node ids as inserted by negotiation.
    pub fn mark_inserted(&mut self, ids: &HashSet<String>) {
        for n in &mut self.nodes {
            n.inserted_by_negotiation = ids.contains(&n.id);
        }
    }

    /// Fill in per-node pacing labels (known only once nodes exist).
    pub fn set_pacing(&mut self, pacing: &HashMap<String, String>) {
        for n in &mut self.nodes {
            if let Some(p) = pacing.get(&n.id) {
                n.pacing = Some(p.clone());
            }
        }
    }

    /// Index of the ingress edge feeding `node_id`, if it is a source.
    pub fn ingress_edge(&self, node_id: &str) -> Option<usize> {
        self.edges
            .iter()
            .position(|e| e.kind == EdgeKind::Ingress && e.to == node_id)
    }

    /// Index of the egress edge leaving `node_id`, if it is a sink.
    pub fn egress_edge(&self, node_id: &str) -> Option<usize> {
        self.edges
            .iter()
            .position(|e| e.kind == EdgeKind::Egress && e.from == node_id)
    }
}

fn port_kind_str(kind: PortKind) -> &'static str {
    match kind {
        PortKind::Stream => "stream",
        PortKind::Snapshot => "snapshot",
    }
}

/// Human-readable label for a node's pacing nature.
pub fn pacing_label(p: crate::nodes::PacingNature) -> String {
    use crate::nodes::PacingNature;
    match p {
        PacingNature::Reactive => "reactive".into(),
        PacingNature::ClockedToOutboundMedia => "clocked_to_outbound_media".into(),
        PacingNature::SourceWall(hz) => format!("source_wall({hz})"),
    }
}
