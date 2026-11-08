//! Bounded execution policy for automatically inserted built-in audio hops.
//!
//! Endpoint factory contracts supply ceilings, not a prediction of DSP output
//! size. Runtime payload, allocator provenance and live retention checks still
//! apply. In particular, resampling expansion or retained small input chunks can
//! exhaust these limits; negotiation never authorizes staging or truncation.
use super::*;
use remotemedia_traits::buffer_contract::{BufferContract, BufferDomain, BufferLayout};
use serde_json::json;

pub(super) fn bind(
    manifest: &Manifest,
    registry: &StreamingNodeRegistry,
    inserted: &mut [InsertedNode],
) {
    let mut chains: HashMap<_, Vec<usize>> = HashMap::new();
    for (index, node) in inserted.iter().enumerate() {
        chains.entry(node.between.clone()).or_default().push(index);
    }
    for ((from, to), indices) in chains {
        // The media negotiator groups by node pair, not by port. Never attach
        // one port's limits to a chain shared by parallel edges.
        let mut edges = manifest
            .connections
            .iter()
            .filter(|c| c.from == from && c.to == to);
        let Some(edge) = edges.next() else { continue };
        if edges.next().is_some()
            || indices.iter().any(|&i| {
                !matches!(
                    inserted[i].node_type.as_str(),
                    "FastResampleNode" | "FastFormatConverterNode"
                )
            })
        {
            continue;
        }
        let Some(limits) = edge_limits(manifest, registry, edge) else {
            continue;
        };
        for (position, index) in indices.iter().copied().enumerate() {
            let mut params = inserted[index].params.clone();
            let Some(object) = params.as_object_mut() else {
                continue;
            };
            let mut hop = limits.clone();
            let intermediate = limits["max_input_bytes"]
                .as_u64()
                .unwrap()
                .max(limits["max_output_bytes"].as_u64().unwrap());
            if position > 0 {
                hop["max_input_bytes"] = json!(intermediate);
            }
            if position + 1 < indices.len() {
                hop["max_output_bytes"] = json!(intermediate);
            }
            object.insert("buffer_limits".into(), hop);
            // Ask the actual registered factory to accept the generated params.
            // This is not an inline manifest promise on behalf of a plugin.
            if registry
                .get_factory(&inserted[index].node_type)
                .and_then(|f| f.try_buffer_contract(&params).ok().flatten())
                .is_some()
            {
                inserted[index].params = params;
            }
        }
    }
}

fn endpoint(
    manifest: &Manifest,
    registry: &StreamingNodeRegistry,
    id: &str,
) -> Option<BufferContract> {
    let node = manifest.nodes.iter().find(|node| node.id == id)?;
    registry
        .get_factory(&node.node_type)?
        .try_buffer_contract(&node.params)
        .ok()
        .flatten()
}

fn edge_limits(
    manifest: &Manifest,
    registry: &StreamingNodeRegistry,
    edge: &Connection,
) -> Option<serde_json::Value> {
    let producer = endpoint(manifest, registry, &edge.from)?;
    let consumer = endpoint(manifest, registry, &edge.to)?;
    let output = producer
        .outputs
        .get(edge.from_port.as_deref().unwrap_or(""))?;
    let input = consumer.inputs.get(edge.to_port.as_deref().unwrap_or(""))?;
    output.validate_definition().ok()?;
    input.validate_definition().ok()?;
    // A converter cannot select which domain the producer will actually emit.
    // Require one concrete domain and audio-only production; extra accepted
    // consumer layouts/domains do not require a conversion.
    if output.layouts != [BufferLayout::AudioInterleavedF32]
        || !input.layouts.contains(&BufferLayout::AudioInterleavedF32)
        || output.domains.len() != 1
        || !input.domains.contains(&output.domains[0])
        || !matches!(
            output.domains[0],
            BufferDomain::Host | BufferDomain::SessionMapped
        )
        || output.max_payload_bytes.max(input.max_payload_bytes) > isize::MAX as u64
    {
        return None;
    }
    // Reserve both adjacent nodes' declared allowances for each inserted hop.
    // This covers retained input owners and output capacity without an unbounded
    // allowance. The session preflight includes every inserted reservation.
    let source = producer.retention?;
    let target = consumer.retention?;
    let owners = source.owners.checked_add(target.owners)?;
    let bytes = source.bytes.checked_add(target.bytes)?;
    Some(json!({
        "domain":output.domains[0],
        "max_input_bytes":output.max_payload_bytes,
        "max_output_bytes":input.max_payload_bytes,
        "retention":{"owners":owners,"bytes":bytes}
    }))
}
