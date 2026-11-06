//! Static buffer-contract inspection before factory creation. This is one gate,
//! not whole-pipeline zero-copy certification: runtime layouts/provenance, router
//! reservations, observers and external transports still need independent checks.
use crate::{
    manifest::{Manifest, NodeManifest, RuntimeHint},
    nodes::StreamingNodeRegistry,
    Error,
};
use remotemedia_traits::buffer_contract::*;
use remotemedia_types::CopyPolicy;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct BufferPreflightConfig {
    /// Strict rejects static incompatibilities before creating nodes. This does
    /// not replace node_materialization_policy or mapped_input_policy.
    pub policy: CopyPolicy,
    /// Budget for the sum of declared node retention, not a pool reservation.
    pub node_retention_budget: BufferRetention,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BufferPreflightIssue {
    pub boundary: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BufferPreflightReport {
    pub nodes: BTreeMap<String, Option<BufferContract>>,
    pub issues: Vec<BufferPreflightIssue>,
    /// None if any retention declaration is absent or arithmetic overflowed.
    pub declared_node_retention: Option<BufferRetention>,
}

impl BufferPreflightReport {
    fn issue(&mut self, boundary: impl Into<String>, reason: impl Into<String>) {
        self.issues.push(BufferPreflightIssue {
            boundary: boundary.into(),
            reason: reason.into(),
        });
    }

    pub fn enforce(&self, policy: CopyPolicy) -> Result<(), Error> {
        if policy == CopyPolicy::RequireZeroCopy {
            if let Some(issue) = self.issues.first() {
                return Err(Error::ConfigError(format!(
                    "buffer preflight at '{}': {}",
                    issue.boundary, issue.reason
                )));
            }
        }
        Ok(())
    }
}

/// Inspect only the supplied registry and manifest: never load a plugin, create
/// a node, provision Python or spawn a worker. Unknown contracts stay visible.
pub fn inspect_buffer_sharing(
    manifest: &Manifest,
    registry: &StreamingNodeRegistry,
    budget: BufferRetention,
) -> BufferPreflightReport {
    let mut report = BufferPreflightReport {
        nodes: BTreeMap::new(),
        issues: Vec::new(),
        declared_node_retention: Some(BufferRetention {
            owners: 0,
            bytes: 0,
        }),
    };
    for node in &manifest.nodes {
        if report.nodes.contains_key(&node.id) {
            report.issue(
                &node.id,
                "duplicate node identity makes buffer contracts ambiguous",
            );
            report.declared_node_retention = None;
            continue;
        }
        let contract = registry
            .get_factory(&node.node_type)
            .and_then(|f| f.buffer_contract(&node.params));
        if let Some(contract) = &contract {
            inspect_node(&mut report, node, contract);
        } else {
            report.issue(&node.id, format!("{} has no buffer contract; declare allocation, layout, protocol and retention support", node.node_type));
            report.declared_node_retention = None;
        }
        report.nodes.insert(node.id.clone(), contract);
    }
    inspect_edges(&mut report, manifest);
    if let Some(retained) = report.declared_node_retention {
        if retained.owners > budget.owners || retained.bytes > budget.bytes {
            report.issue(
                "session",
                format!(
                    "declared node retention needs {} owners / {} bytes; budget is {} / {}",
                    retained.owners, retained.bytes, budget.owners, budget.bytes
                ),
            );
        }
    }
    report
}

fn inspect_edges(report: &mut BufferPreflightReport, manifest: &Manifest) {
    for edge in &manifest.connections {
        let boundary = format!(
            "{}:{} -> {}:{}",
            edge.from,
            edge.from_port.as_deref().unwrap_or("<default>"),
            edge.to,
            edge.to_port.as_deref().unwrap_or("<default>")
        );
        let output = report
            .nodes
            .get(&edge.from)
            .and_then(Option::as_ref)
            .and_then(|c| c.outputs.get(edge.from_port.as_deref().unwrap_or("")));
        let input = report
            .nodes
            .get(&edge.to)
            .and_then(Option::as_ref)
            .and_then(|c| c.inputs.get(edge.to_port.as_deref().unwrap_or("")));
        let reason = match (output, input) {
            (Some(output), Some(input)) => incompatible_ports(output, input),
            _ => Some("missing exact input/output port contract"),
        };
        if let Some(reason) = reason {
            report.issue(boundary, reason);
        }
    }
}

fn inspect_node(
    report: &mut BufferPreflightReport,
    node: &NodeManifest,
    contract: &BufferContract,
) {
    if contract.version != BUFFER_CONTRACT_VERSION {
        report.issue(
            &node.id,
            format!("unsupported buffer contract version {}", contract.version),
        );
    }
    if let Some(reason) = local_placement_issue(node) {
        report.issue(&node.id, reason);
    }
    match &contract.protocol {
        BufferProtocol::RustOwners => {},
        BufferProtocol::Serialized => report.issue(&node.id, "serialized protocol requires payload materialization"),
        protocol => report.issue(&node.id, format!("transport contract {protocol:?} has no verified factory binding in this preflight version")),
    }
    inspect_ports(report, &node.id, contract);
    inspect_forwarding(report, &node.id, contract);
    record_retention(report, &node.id, contract);
}

fn inspect_ports(report: &mut BufferPreflightReport, node_id: &str, contract: &BufferContract) {
    for (name, port) in contract.inputs.iter().chain(contract.outputs.iter()) {
        if !port.layouts.is_empty() && (port.domains.is_empty() || port.max_payload_bytes == 0) {
            report.issue(
                format!("{}:{name}", node_id),
                "bulk port needs a memory domain and a nonzero payload bound",
            );
        }
        if port
            .domains
            .iter()
            .any(|d| !matches!(d, BufferDomain::Host))
        {
            report.issue(
                format!("{}:{name}", node_id),
                "mapped/device domain requires allocator/transport binding verification",
            );
        }
    }
}

fn inspect_forwarding(
    report: &mut BufferPreflightReport,
    node_id: &str,
    contract: &BufferContract,
) {
    if contract.production != BufferProduction::ForwardInput {
        return;
    }
    for (name, output) in &contract.outputs {
        let possible = output.layouts.iter().all(|layout| {
            output.domains.iter().all(|domain| {
                contract
                    .inputs
                    .values()
                    .any(|input| input.layouts.contains(layout) && input.domains.contains(domain))
            })
        });
        if !possible {
            report.issue(
                format!("{node_id}:{name}"),
                "forwarding output declares a layout/domain unavailable from every input",
            );
        }
    }
}

fn record_retention(report: &mut BufferPreflightReport, node_id: &str, contract: &BufferContract) {
    match contract.retention {
        None => {
            report.issue(node_id, "node retention is unbounded or undeclared");
            report.declared_node_retention = None;
        }
        Some(retained) => {
            let max_payload = contract
                .inputs
                .values()
                .chain(contract.outputs.values())
                .map(|port| port.max_payload_bytes)
                .max()
                .unwrap_or(0);
            if max_payload > 0 && (retained.owners == 0 || retained.bytes < max_payload) {
                report.issue(
                    node_id,
                    "node retention cannot cover one declared maximum payload",
                );
            }
            if let Some(total) = report.declared_node_retention {
                report.declared_node_retention = total
                    .owners
                    .checked_add(retained.owners)
                    .zip(total.bytes.checked_add(retained.bytes))
                    .map(|(owners, bytes)| BufferRetention { owners, bytes });
                if report.declared_node_retention.is_none() {
                    report.issue(node_id, "node retention sum overflows u64");
                }
            }
        }
    }
}

fn incompatible_ports(
    output: &BufferPortContract,
    input: &BufferPortContract,
) -> Option<&'static str> {
    if !output
        .layouts
        .iter()
        .all(|layout| input.layouts.contains(layout))
    {
        return Some("consumer cannot accept every producer layout without conversion");
    }
    if output.layouts.is_empty() {
        return None;
    }
    if !output
        .domains
        .iter()
        .all(|domain| input.domains.contains(domain))
    {
        return Some("producer memory domain requires a copy/transfer at the consumer");
    }
    if output.max_payload_bytes > input.max_payload_bytes {
        return Some("producer payload bound exceeds consumer bound");
    }
    None
}

#[derive(Clone, Copy)]
pub(crate) enum LocalPlacementConflict {
    Host,
    Execution,
    Runtime,
    #[cfg(feature = "docker")]
    Docker,
}

fn local_placement_issue(node: &NodeManifest) -> Option<&'static str> {
    local_placement_conflict(node).map(|conflict| match conflict {
        LocalPlacementConflict::Host => "host routing is unsupported for same-host buffer sharing",
        LocalPlacementConflict::Execution => {
            "execution placement/fallback conflicts with local buffer sharing"
        }
        LocalPlacementConflict::Runtime => {
            "runtime_hint requires an explicit buffer-sharing adapter"
        }
        #[cfg(feature = "docker")]
        LocalPlacementConflict::Docker => {
            "Docker placement requires an explicit buffer-sharing adapter"
        }
    })
}

pub(crate) fn local_placement_conflict(node: &NodeManifest) -> Option<LocalPlacementConflict> {
    if node.host.is_some() {
        return Some(LocalPlacementConflict::Host);
    }
    if let Some(execution) = &node.execution {
        if !matches!(
            execution.placement.as_str(),
            "" | "local" | "prefer_local" | "auto"
        ) || execution.fallback.is_some()
        {
            return Some(LocalPlacementConflict::Execution);
        }
    }
    if !matches!(node.runtime_hint, None | Some(RuntimeHint::Auto)) {
        return Some(LocalPlacementConflict::Runtime);
    }
    #[cfg(feature = "docker")]
    if node.docker.is_some() {
        return Some(LocalPlacementConflict::Docker);
    }
    None
}
