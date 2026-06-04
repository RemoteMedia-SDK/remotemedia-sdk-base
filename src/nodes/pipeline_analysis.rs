//! Pipeline analysis module
//!
//! Provides functions to analyze pipeline manifests against the node registry,
//! returning detailed information about each node without hardcoded mappings.

use crate::manifest::Manifest;
use crate::nodes::streaming_node::StreamingNodeRegistry;
use serde::{Deserialize, Serialize};

/// Information about a node in a pipeline (from analysis)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineNodeInfo {
    /// Node ID from the manifest
    pub id: String,
    /// Node type (e.g., "KokoroTTSNode", "SileroVADNode")
    pub node_type: String,
    /// Whether this node is registered in the registry
    pub is_registered: bool,
    /// Whether this is a Python-based node
    pub is_python: bool,
    /// Whether this node produces multiple outputs
    pub is_multi_output: bool,
    /// Node parameters from the manifest
    pub params: serde_json::Value,
    /// Whether this is marked as a streaming node in the manifest
    pub is_streaming: bool,
}

/// Result of analyzing a pipeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineAnalysis {
    /// Pipeline name from metadata
    pub name: String,
    /// Pipeline description
    pub description: Option<String>,
    /// All nodes in the pipeline
    pub nodes: Vec<PipelineNodeInfo>,
    /// Node types that are registered (available)
    pub registered_types: Vec<String>,
    /// Node types that are NOT registered (missing)
    pub missing_types: Vec<String>,
    /// Python node types used in this pipeline
    pub python_node_types: Vec<String>,
    /// Rust node types used in this pipeline
    pub rust_node_types: Vec<String>,
    /// Whether all nodes are available
    pub is_valid: bool,
    /// Validation errors (if any)
    pub errors: Vec<String>,
}

/// Analyze a pipeline manifest against a node registry
///
/// Returns detailed information about each node, including:
/// - Whether the node type is registered
/// - Whether it's a Python or Rust node
/// - Whether it's multi-output streaming
///
/// # Arguments
/// * `manifest` - The pipeline manifest to analyze
/// * `registry` - The streaming node registry to check against
///
/// # Returns
/// A `PipelineAnalysis` containing node information and validation status
pub fn analyze_pipeline(manifest: &Manifest, registry: &StreamingNodeRegistry) -> PipelineAnalysis {
    let mut nodes = Vec::new();
    let mut registered_types = Vec::new();
    let mut missing_types = Vec::new();
    let mut python_node_types = Vec::new();
    let mut rust_node_types = Vec::new();
    let mut errors = Vec::new();

    // Track unique node types we've already processed
    let mut seen_types = std::collections::HashSet::new();

    for node_manifest in &manifest.nodes {
        let node_type = &node_manifest.node_type;
        let is_registered = registry.has_node_type(node_type);
        let is_python = if is_registered {
            registry.is_python_node(node_type)
        } else {
            false
        };
        let is_multi_output = if is_registered {
            registry.is_multi_output_streaming(node_type)
        } else {
            false
        };

        // Track node type categorization (only once per type)
        if !seen_types.contains(node_type) {
            seen_types.insert(node_type.clone());

            if is_registered {
                registered_types.push(node_type.clone());
                if is_python {
                    python_node_types.push(node_type.clone());
                } else {
                    rust_node_types.push(node_type.clone());
                }
            } else {
                missing_types.push(node_type.clone());
                errors.push(format!(
                    "Node type '{}' is not registered. Available types: {:?}",
                    node_type,
                    registry.list_types()
                ));
            }
        }

        nodes.push(PipelineNodeInfo {
            id: node_manifest.id.clone(),
            node_type: node_type.clone(),
            is_registered,
            is_python,
            is_multi_output,
            params: node_manifest.params.clone(),
            is_streaming: node_manifest.is_streaming,
        });
    }

    PipelineAnalysis {
        name: manifest.metadata.name.clone(),
        description: manifest.metadata.description.clone(),
        nodes,
        registered_types,
        missing_types: missing_types.clone(),
        python_node_types,
        rust_node_types,
        is_valid: missing_types.is_empty(),
        errors,
    }
}

/// Get all registered node types from a registry
///
/// Useful for listing available nodes for tooling/IDE completion.
pub fn list_all_node_types(registry: &StreamingNodeRegistry) -> Vec<String> {
    registry.list_types()
}

/// Get information about a specific node type from the registry
///
/// Returns None if the node type is not registered.
pub fn get_node_type_info(
    registry: &StreamingNodeRegistry,
    node_type: &str,
) -> Option<NodeTypeInfo> {
    if !registry.has_node_type(node_type) {
        return None;
    }

    Some(NodeTypeInfo {
        node_type: node_type.to_string(),
        is_python: registry.is_python_node(node_type),
        is_multi_output: registry.is_multi_output_streaming(node_type),
        schema: registry
            .get_factory(node_type)
            .and_then(|f| f.schema())
            .map(|s| serde_json::to_value(s).ok())
            .flatten(),
    })
}

/// Information about a node type from the registry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTypeInfo {
    /// The node type name
    pub node_type: String,
    /// Whether this is a Python-based node
    pub is_python: bool,
    /// Whether this node produces multiple outputs
    pub is_multi_output: bool,
    /// JSON schema for node parameters (if available)
    pub schema: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::NodeManifest;
    use crate::nodes::streaming_registry::create_default_streaming_registry;

    #[test]
    fn test_analyze_pipeline_with_registry() {
        let registry = create_default_streaming_registry();

        // Create a simple manifest
        let manifest = Manifest {
            version: "v1".to_string(),
            metadata: crate::manifest::ManifestMetadata {
                name: "test-pipeline".to_string(),
                description: Some("Test pipeline".to_string()),
                ..Default::default()
            },
            nodes: vec![NodeManifest {
                id: "node1".to_string(),
                node_type: "PassThrough".to_string(),
                params: serde_json::json!({}),
                ..Default::default()
            }],
            connections: vec![],
            python_env: None,
            plugins: Vec::new(),
        };

        let analysis = analyze_pipeline(&manifest, &registry);

        assert!(analysis.is_valid);
        assert_eq!(analysis.nodes.len(), 1);
        assert!(analysis.nodes[0].is_registered);
        assert!(!analysis.nodes[0].is_python);
    }

    #[test]
    fn test_analyze_pipeline_missing_node() {
        let registry = create_default_streaming_registry();

        let manifest = Manifest {
            version: "v1".to_string(),
            metadata: crate::manifest::ManifestMetadata {
                name: "test-pipeline".to_string(),
                ..Default::default()
            },
            nodes: vec![NodeManifest {
                id: "node1".to_string(),
                node_type: "NonExistentNode".to_string(),
                params: serde_json::json!({}),
                ..Default::default()
            }],
            connections: vec![],
            python_env: None,
            plugins: Vec::new(),
        };

        let analysis = analyze_pipeline(&manifest, &registry);

        assert!(!analysis.is_valid);
        assert_eq!(analysis.missing_types.len(), 1);
        assert_eq!(analysis.missing_types[0], "NonExistentNode");
    }

    #[test]
    fn test_list_all_node_types() {
        let registry = create_default_streaming_registry();
        let types = list_all_node_types(&registry);

        // Should have at least some built-in nodes
        assert!(!types.is_empty());
        assert!(types.contains(&"PassThrough".to_string()));
    }
}
