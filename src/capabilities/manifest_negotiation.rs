//! Manifest-level capability negotiation entry point.
//!
//! Bridges the static [`negotiate_pipeline`] algorithm in [`super::negotiation`]
//! to the runtime [`StreamingNodeRegistry`] and [`Manifest`] types. Two
//! responsibilities:
//!
//! 1. **Collect declared capabilities** for every node in the manifest, asking
//!    each registered factory's `media_capabilities(params)` and falling back
//!    to any `media_capabilities` block the manifest itself supplied.
//! 2. **Optionally rewrite the manifest** to splice in conversion nodes when
//!    `metadata.auto_negotiate` is true and the negotiation produces a path.
//!
//! Call sites are expected to keep the original manifest around for telemetry
//! and pass the returned [`NegotiationOutcome::manifest`] to downstream
//! construction (session router, graph builder, etc.).
//!
//! ## Strictness
//!
//! Mismatches that can't be bridged surface in [`NegotiationOutcome::warnings`].
//! Whether they cause a hard error is decided by the caller, which reads
//! `metadata.strict_capabilities` (plus the `REMOTEMEDIA_STRICT_CAPS` env var
//! as an override).

use std::collections::HashMap;
use std::sync::Arc;

use crate::manifest::{Connection, Manifest, NodeManifest};
use crate::nodes::streaming_node::StreamingNodeRegistry;

use super::constraints::MediaCapabilities;
use super::negotiation::{negotiate_pipeline_default, InsertedNode, NegotiationResult};
use super::validation::CapabilityMismatch;

/// Result of negotiating a manifest against the live registry.
#[derive(Debug, Clone)]
pub struct NegotiationOutcome {
    /// The (possibly rewritten) manifest the caller should hand to downstream
    /// construction. Always non-None even when no auto-insertion happened —
    /// in that case it's `Arc::clone()` of the input.
    pub manifest: Arc<Manifest>,
    /// Conversion nodes spliced into `manifest` by negotiation. Empty when
    /// `auto_negotiate` was off or no insertions were needed.
    pub inserted_nodes: Vec<InsertedNode>,
    /// Capability mismatches that were detected. With `auto_negotiate=true`
    /// these are only the *unresolved* ones (anything fixable becomes an
    /// inserted node instead). With `auto_negotiate=false` every mismatch
    /// shows up here.
    pub warnings: Vec<CapabilityMismatch>,
}

impl NegotiationOutcome {
    pub fn has_unresolved(&self) -> bool {
        !self.warnings.is_empty()
    }
}

/// Negotiate `manifest` against `registry`, optionally rewriting it.
///
/// Pure function: never logs, never panics, never errors. The caller decides
/// what to do with the outcome based on `metadata.strict_capabilities` and any
/// environment flags.
pub fn negotiate_manifest(
    manifest: Arc<Manifest>,
    registry: &StreamingNodeRegistry,
) -> NegotiationOutcome {
    // Build node_id -> MediaCapabilities by asking factories first
    // (`get_media_capabilities` reads `f.media_capabilities(params)`), then
    // falling back to any caps the manifest declared inline. Nodes that
    // declare neither are absent from the map, which `negotiate_pipeline`
    // treats as "accepts any" — same shape as the existing behavior.
    let mut node_caps: HashMap<String, MediaCapabilities> = HashMap::new();
    for node in &manifest.nodes {
        if let Some(caps) = registry.get_media_capabilities(&node.node_type, &node.params) {
            node_caps.insert(node.id.clone(), caps);
        } else if let Some(caps) = &node.media_capabilities {
            node_caps.insert(node.id.clone(), caps.clone());
        }
    }

    let connections: Vec<(String, String)> = manifest
        .connections
        .iter()
        .map(|c| (c.from.clone(), c.to.clone()))
        .collect();

    let auto_insert = manifest.metadata.auto_negotiate;
    let result = negotiate_pipeline_default(&node_caps, &connections, auto_insert);

    match result {
        NegotiationResult::Valid(_) => NegotiationOutcome {
            manifest,
            inserted_nodes: Vec::new(),
            warnings: Vec::new(),
        },
        NegotiationResult::Negotiated(caps) => {
            let rewritten = splice_inserted_nodes(&manifest, &caps.inserted_nodes);
            NegotiationOutcome {
                manifest: Arc::new(rewritten),
                inserted_nodes: caps.inserted_nodes,
                warnings: Vec::new(),
            }
        }
        NegotiationResult::Failed(mismatches) => NegotiationOutcome {
            manifest,
            inserted_nodes: Vec::new(),
            warnings: mismatches,
        },
    }
}

/// Read the strict-mode flag from either the manifest or the
/// `REMOTEMEDIA_STRICT_CAPS` env override. The env wins so operators can flip
/// it on across an entire deployment without re-authoring manifests.
pub fn strict_capabilities_enabled(manifest: &Manifest) -> bool {
    if manifest.metadata.strict_capabilities {
        return true;
    }
    match std::env::var("REMOTEMEDIA_STRICT_CAPS") {
        Ok(v) => matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        Err(_) => false,
    }
}

/// Build a new `Manifest` that contains every original node plus the inserted
/// conversion nodes, with the affected connections re-wired to chain through
/// them. Connections we don't touch are preserved in order.
fn splice_inserted_nodes(manifest: &Manifest, inserted: &[InsertedNode]) -> Manifest {
    // Group insertions by their (source, target) edge so we can chain multiple
    // converters (e.g., resample → channel mix) cleanly. The negotiation
    // algorithm appends in path order, which is the order we want to chain in.
    let mut by_edge: HashMap<(String, String), Vec<&InsertedNode>> = HashMap::new();
    for node in inserted {
        by_edge.entry(node.between.clone()).or_default().push(node);
    }

    let mut out = manifest.clone();

    // Append synthesized nodes. NodeManifest::default() gives us sane fields;
    // we only override the ones that matter for execution.
    for node in inserted {
        out.nodes.push(NodeManifest {
            id: node.id.clone(),
            node_type: node.node_type.clone(),
            params: node.params.clone(),
            ..Default::default()
        });
    }

    // Rewrite connections: for each original edge that has insertions, replace
    // its single `from → to` with the chain `from → ins1 → ins2 → … → to`.
    let mut new_connections: Vec<Connection> = Vec::with_capacity(out.connections.len());
    for conn in &out.connections {
        let key = (conn.from.clone(), conn.to.clone());
        match by_edge.get(&key) {
            None => new_connections.push(conn.clone()),
            Some(chain) => {
                // Preserve the original from_port/to_port on the outermost
                // hops so port-aware routing still works after splicing.
                let mut prev = conn.from.clone();
                let mut prev_port = conn.from_port.clone();
                for inserted_node in chain {
                    new_connections.push(Connection {
                        from: prev,
                        to: inserted_node.id.clone(),
                        from_port: prev_port,
                        to_port: None,
                    });
                    prev = inserted_node.id.clone();
                    prev_port = None;
                }
                new_connections.push(Connection {
                    from: prev,
                    to: conn.to.clone(),
                    from_port: prev_port,
                    to_port: conn.to_port.clone(),
                });
            }
        }
    }

    out.connections = new_connections;
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{Manifest, ManifestMetadata, NodeManifest};
    use crate::nodes::streaming_node::StreamingNodeRegistry;
    use serde_json::json;

    fn manifest_with(
        metadata: ManifestMetadata,
        nodes: Vec<NodeManifest>,
        conns: Vec<Connection>,
    ) -> Arc<Manifest> {
        Arc::new(Manifest {
            version: "v1".into(),
            metadata,
            nodes,
            connections: conns,
            python_env: None,
            plugins: Vec::new(),
        })
    }

    #[test]
    fn strict_env_var_overrides_metadata() {
        let m = Manifest {
            version: "v1".into(),
            metadata: ManifestMetadata::default(),
            nodes: vec![],
            connections: vec![],
            python_env: None,
            plugins: Vec::new(),
        };

        std::env::remove_var("REMOTEMEDIA_STRICT_CAPS");
        assert!(!strict_capabilities_enabled(&m));

        std::env::set_var("REMOTEMEDIA_STRICT_CAPS", "1");
        assert!(strict_capabilities_enabled(&m));
        std::env::remove_var("REMOTEMEDIA_STRICT_CAPS");
    }

    #[test]
    fn negotiate_with_unknown_nodes_returns_input_unchanged() {
        // No factories registered → no caps → no mismatches → Valid path.
        let registry = StreamingNodeRegistry::new();
        let m = manifest_with(
            ManifestMetadata::default(),
            vec![
                NodeManifest {
                    id: "a".into(),
                    node_type: "Unknown".into(),
                    params: json!({}),
                    ..Default::default()
                },
                NodeManifest {
                    id: "b".into(),
                    node_type: "Unknown".into(),
                    params: json!({}),
                    ..Default::default()
                },
            ],
            vec![Connection {
                from: "a".into(),
                to: "b".into(),
                from_port: None,
                to_port: None,
            }],
        );

        let out = negotiate_manifest(m.clone(), &registry);
        assert!(out.inserted_nodes.is_empty());
        assert!(out.warnings.is_empty());
        assert_eq!(out.manifest.nodes.len(), 2);
        assert_eq!(out.manifest.connections.len(), 1);
    }

    #[test]
    fn splice_chains_multiple_inserts_between_same_edge() {
        // Synthesize a manifest with one a→b edge and two pre-baked
        // InsertedNodes assigned to that edge; the rewritten connections
        // should form a→ins1→ins2→b chain in that exact order.
        let manifest = Manifest {
            version: "v1".into(),
            metadata: ManifestMetadata::default(),
            nodes: vec![
                NodeManifest {
                    id: "a".into(),
                    node_type: "A".into(),
                    ..Default::default()
                },
                NodeManifest {
                    id: "b".into(),
                    node_type: "B".into(),
                    ..Default::default()
                },
            ],
            connections: vec![Connection {
                from: "a".into(),
                to: "b".into(),
                from_port: Some("audio_out".into()),
                to_port: Some("audio_in".into()),
            }],
            python_env: None,
            plugins: Vec::new(),
        };

        let inserted = vec![
            InsertedNode {
                id: "_auto_convert_1".into(),
                node_type: "FastResampleNode".into(),
                between: ("a".into(), "b".into()),
                params: json!({}),
                input_caps: crate::capabilities::MediaConstraints::Audio(
                    crate::capabilities::AudioConstraints::default(),
                ),
                output_caps: crate::capabilities::MediaConstraints::Audio(
                    crate::capabilities::AudioConstraints::default(),
                ),
            },
            InsertedNode {
                id: "_auto_convert_2".into(),
                node_type: "FastResampleNode".into(),
                between: ("a".into(), "b".into()),
                params: json!({}),
                input_caps: crate::capabilities::MediaConstraints::Audio(
                    crate::capabilities::AudioConstraints::default(),
                ),
                output_caps: crate::capabilities::MediaConstraints::Audio(
                    crate::capabilities::AudioConstraints::default(),
                ),
            },
        ];

        let rewritten = splice_inserted_nodes(&manifest, &inserted);
        assert_eq!(rewritten.nodes.len(), 4); // a, b + 2 inserted
        assert_eq!(rewritten.connections.len(), 3); // a→ins1, ins1→ins2, ins2→b

        // First hop preserves source's from_port; last hop preserves target's
        // to_port; the chain interior is unported.
        assert_eq!(rewritten.connections[0].from, "a");
        assert_eq!(rewritten.connections[0].to, "_auto_convert_1");
        assert_eq!(
            rewritten.connections[0].from_port.as_deref(),
            Some("audio_out")
        );
        assert_eq!(rewritten.connections[0].to_port, None);

        assert_eq!(rewritten.connections[1].from, "_auto_convert_1");
        assert_eq!(rewritten.connections[1].to, "_auto_convert_2");
        assert_eq!(rewritten.connections[1].from_port, None);
        assert_eq!(rewritten.connections[1].to_port, None);

        assert_eq!(rewritten.connections[2].from, "_auto_convert_2");
        assert_eq!(rewritten.connections[2].to, "b");
        assert_eq!(rewritten.connections[2].from_port, None);
        assert_eq!(
            rewritten.connections[2].to_port.as_deref(),
            Some("audio_in")
        );
    }
}
