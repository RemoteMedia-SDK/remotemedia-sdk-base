//! Self-driven sources declare their output capabilities (openspec:
//! add-source-node-lifecycle).
//!
//! A node with no upstream normally inherits nothing and is treated as
//! having no constraints. For a self-driven source that is wrong twice: it
//! has no upstream to inherit from, and nothing the client sends ever tells
//! the pipeline what flows on its edge. Such a node must say what it puts
//! out, or downstream format checks pass against nothing.

use crate::manifest::{Manifest, NodeManifest};
use crate::nodes::{StreamingNodeFactory, StreamingNodeRegistry};
use crate::Error;
use serde_json::Value;

/// Whether `factory` declares an output for a node with `params`: in its
/// capabilities (final or potential), or as the produced data types of its
/// schema (what a Python registration's `produces` sets).
pub fn declares_output(factory: &dyn StreamingNodeFactory, params: &Value) -> bool {
    let has_output = |capabilities: Option<super::MediaCapabilities>| {
        capabilities.is_some_and(|capabilities| !capabilities.outputs.is_empty())
    };
    has_output(factory.media_capabilities(params))
        || has_output(factory.potential_capabilities(params))
        || factory.schema().is_some_and(|schema| !schema.produces.is_empty())
}

/// The error for a self-driven source that declares no output.
pub fn undeclared_source(node_id: &str, node_type: &str) -> Error {
    Error::ConfigError(format!(
        "node '{node_id}' ({node_type}) is a self-driven source and does not declare its output \
         capabilities. Declare them: `output_caps` on a Rust node (`#[node(source, output_caps = \
         \"...\")]` or `source_factory`), `describe` on a loadable plugin factory, `produces` on \
         a Python registration, or `media_capabilities` on the manifest node"
    ))
}

/// Whether the manifest node is known, before any node is created, to be
/// self-driven: the manifest says so, or its factory does and the manifest
/// does not override it.
fn known_self_driven(node: &NodeManifest, factory: Option<&dyn StreamingNodeFactory>) -> bool {
    match node.session_input {
        Some(takes_input) => !takes_input,
        None => factory.and_then(|factory| factory.self_driven()).unwrap_or(false),
    }
}

/// Fail for the first self-driven node of `manifest` that has no upstream
/// and declares no output. Nodes whose self-driven nature is only known
/// once they run (a Python class that reports it at READY) are not covered.
pub fn check_self_driven_sources(
    manifest: &Manifest,
    registry: &StreamingNodeRegistry,
) -> Result<(), Error> {
    for node in &manifest.nodes {
        if manifest.connections.iter().any(|connection| connection.to == node.id) {
            continue;
        }
        let factory = registry.get_factory(&node.node_type).map(|factory| factory.as_ref());
        if !known_self_driven(node, factory) {
            continue;
        }
        let in_manifest = node
            .media_capabilities
            .as_ref()
            .is_some_and(|capabilities| !capabilities.outputs.is_empty());
        let by_factory = factory.is_some_and(|factory| declares_output(factory, &node.params));
        if !in_manifest && !by_factory {
            return Err(undeclared_source(&node.id, &node.node_type));
        }
    }
    Ok(())
}
