//! Capability negotiation algorithm (spec 022)
//!
//! This module provides the negotiation algorithm for automatically
//! resolving capability mismatches and optionally inserting conversion nodes.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use super::constraints::{MediaCapabilities, MediaConstraints};
use super::registry::{ConversionRegistry, DefaultConversionRegistry};
use super::validation::{validate_connection, CapabilityMismatch};

// =============================================================================
// Conversion Types (Phase 5: US3)
// =============================================================================

/// Single step in a conversion path.
///
/// Represents one conversion node to be inserted between source and target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversionStep {
    /// Type of conversion node to insert (e.g., "AudioResample")
    pub node_type: String,
    /// Configuration parameters for the conversion node
    pub params: serde_json::Value,
    /// Expected input constraints
    pub input_caps: MediaConstraints,
    /// Produced output constraints
    pub output_caps: MediaConstraints,
}

/// Sequence of conversion nodes to resolve a capability mismatch (FR-015).
///
/// The path is selected to have the fewest nodes (shortest path).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversionPath {
    /// Ordered conversion operations
    pub steps: Vec<ConversionStep>,
    /// Total number of nodes to insert
    pub total_nodes: usize,
    /// Estimated added latency in microseconds (if available from LatencyMetrics)
    pub estimated_latency_us: Option<u64>,
}

impl ConversionPath {
    /// Create a single-step conversion path.
    pub fn single(step: ConversionStep) -> Self {
        Self {
            steps: vec![step],
            total_nodes: 1,
            estimated_latency_us: None,
        }
    }

    /// Create an empty (no-op) path.
    pub fn empty() -> Self {
        Self {
            steps: Vec::new(),
            total_nodes: 0,
            estimated_latency_us: Some(0),
        }
    }

    /// Check if this is an empty (no-op) path.
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }
}

// =============================================================================
// Inserted Node Tracking (Phase 5: US3)
// =============================================================================

/// Record of an auto-inserted conversion node.
///
/// Used for introspection of the negotiated pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertedNode {
    /// Generated unique ID for the inserted node
    pub id: String,
    /// Type of the inserted node
    pub node_type: String,
    /// Source and target nodes this was inserted between
    pub between: (String, String),
    /// Configuration parameters
    pub params: serde_json::Value,
    /// Input capabilities of the inserted node
    pub input_caps: MediaConstraints,
    /// Output capabilities of the inserted node
    pub output_caps: MediaConstraints,
}

// =============================================================================
// Negotiated Capabilities (Phase 7: US5)
// =============================================================================

/// Result of capability negotiation for introspection (FR-016).
///
/// Contains the resolved format for each connection and any
/// conversion nodes that were inserted.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NegotiatedCapabilities {
    /// Resolved format for each connection: (source_id, target_id) → format
    pub connections: HashMap<(String, String), MediaConstraints>,
    /// List of auto-inserted conversion nodes
    pub inserted_nodes: Vec<InsertedNode>,
}

impl NegotiatedCapabilities {
    /// Get the negotiated format for a specific connection.
    pub fn get_connection_format(&self, source: &str, target: &str) -> Option<&MediaConstraints> {
        self.connections
            .get(&(source.to_string(), target.to_string()))
    }

    /// Check if any conversion nodes were inserted.
    pub fn has_conversions(&self) -> bool {
        !self.inserted_nodes.is_empty()
    }

    /// Get the number of inserted conversion nodes.
    pub fn conversion_count(&self) -> usize {
        self.inserted_nodes.len()
    }
}

// =============================================================================
// Negotiation Result
// =============================================================================

/// Result of capability negotiation.
#[derive(Debug, Clone)]
pub enum NegotiationResult {
    /// Pipeline is valid as-is, no changes needed
    Valid(NegotiatedCapabilities),
    /// Pipeline was modified with conversion nodes
    Negotiated(NegotiatedCapabilities),
    /// Negotiation failed - unresolvable mismatches
    Failed(Vec<CapabilityMismatch>),
}

impl NegotiationResult {
    /// Check if negotiation succeeded (either valid or negotiated).
    pub fn is_success(&self) -> bool {
        matches!(
            self,
            NegotiationResult::Valid(_) | NegotiationResult::Negotiated(_)
        )
    }

    /// Get the negotiated capabilities if successful.
    pub fn capabilities(&self) -> Option<&NegotiatedCapabilities> {
        match self {
            NegotiationResult::Valid(caps) | NegotiationResult::Negotiated(caps) => Some(caps),
            NegotiationResult::Failed(_) => None,
        }
    }

    /// Get the mismatches if negotiation failed.
    pub fn mismatches(&self) -> Option<&Vec<CapabilityMismatch>> {
        match self {
            NegotiationResult::Failed(mismatches) => Some(mismatches),
            _ => None,
        }
    }
}

// =============================================================================
// Flexible Node Resolution (Phase 6: US4)
// =============================================================================

/// Resolve a flexible constraint to match a fixed target constraint.
///
/// When a source has a flexible constraint (Range or Set) and the target
/// has a fixed requirement (Exact), this function selects the value from
/// the source's range/set that matches the target.
///
/// Returns `None` if no compatible value can be found.
pub fn resolve_flexible_to_fixed<T>(
    source: &super::constraints::ConstraintValue<T>,
    target: &super::constraints::ConstraintValue<T>,
) -> Option<T>
where
    T: Clone + PartialOrd + Ord,
{
    use super::constraints::ConstraintValue;

    match (source, target) {
        // Source flexible, target exact → select target value if in source range/set
        (ConstraintValue::Range { min, max }, ConstraintValue::Exact(val)) => {
            if val >= min && val <= max {
                Some(val.clone())
            } else {
                None
            }
        }
        (ConstraintValue::Set(set), ConstraintValue::Exact(val)) => {
            if set.contains(val) {
                Some(val.clone())
            } else {
                None
            }
        }
        // Both exact → must match
        (ConstraintValue::Exact(src), ConstraintValue::Exact(tgt)) => {
            if src == tgt {
                Some(src.clone())
            } else {
                None
            }
        }
        // Source exact, target flexible → source value if in target range/set
        (ConstraintValue::Exact(val), ConstraintValue::Range { min, max }) => {
            if val >= min && val <= max {
                Some(val.clone())
            } else {
                None
            }
        }
        (ConstraintValue::Exact(val), ConstraintValue::Set(set)) => {
            if set.contains(val) {
                Some(val.clone())
            } else {
                None
            }
        }
        // Both flexible → prefer passthrough (first common value)
        (
            ConstraintValue::Range {
                min: min1,
                max: max1,
            },
            ConstraintValue::Range {
                min: min2,
                max: max2,
            },
        ) => {
            // Return the lower bound of the intersection
            let lower = if min1 > min2 { min1 } else { min2 };
            let upper = if max1 < max2 { max1 } else { max2 };
            if lower <= upper {
                Some(lower.clone())
            } else {
                None
            }
        }
        (ConstraintValue::Set(set1), ConstraintValue::Set(set2)) => {
            // Return first common value
            set1.iter().find(|v| set2.contains(v)).cloned()
        }
        (ConstraintValue::Range { min, max }, ConstraintValue::Set(set)) => {
            // Return first set value in range
            set.iter().find(|v| *v >= min && *v <= max).cloned()
        }
        (ConstraintValue::Set(set), ConstraintValue::Range { min, max }) => {
            // Return first set value in range
            set.iter().find(|v| *v >= min && *v <= max).cloned()
        }
    }
}

// =============================================================================
// Negotiation Algorithm (Phase 5: US3)
// =============================================================================

/// Negotiate capabilities for a pipeline, optionally inserting conversion nodes.
///
/// # Arguments
///
/// * `node_capabilities` - Map of node ID to its MediaCapabilities
/// * `connections` - List of (source_id, target_id) connections
/// * `auto_insert` - If true, attempt to insert conversion nodes for mismatches
/// * `registry` - Registry of available conversion nodes
///
/// # Returns
///
/// * `NegotiationResult::Valid` - Pipeline is valid as-is
/// * `NegotiationResult::Negotiated` - Pipeline was modified with conversions
/// * `NegotiationResult::Failed` - Unresolvable mismatches found
pub fn negotiate_pipeline(
    node_capabilities: &HashMap<String, MediaCapabilities>,
    connections: &[(String, String)],
    auto_insert: bool,
    registry: &dyn ConversionRegistry,
) -> NegotiationResult {
    let mut negotiated = NegotiatedCapabilities::default();
    let mut unresolved_mismatches = Vec::new();
    let mut insertion_counter = 0;

    for (source_id, target_id) in connections {
        // Get capabilities, defaulting to empty if not specified
        let source_caps = node_capabilities
            .get(source_id)
            .cloned()
            .unwrap_or_default();
        let target_caps = node_capabilities
            .get(target_id)
            .cloned()
            .unwrap_or_default();

        // Check for mismatches
        let mismatches = validate_connection(
            source_id,
            &source_caps,
            target_id,
            &target_caps,
            "default",
            "default",
        );

        if mismatches.is_empty() {
            // Connection is compatible - record the resolved format
            if let Some(output) = source_caps.default_output() {
                negotiated
                    .connections
                    .insert((source_id.clone(), target_id.clone()), output.clone());
            }
        } else if auto_insert {
            // Try to find conversion path
            let source_constraint = source_caps.default_output();
            let target_constraint = target_caps.default_input();

            if let (Some(src), Some(tgt)) = (source_constraint, target_constraint) {
                if let Some(path) = registry.find_conversion_path(src, tgt) {
                    // Insert conversion nodes
                    for step in &path.steps {
                        insertion_counter += 1;
                        let inserted_id = format!("_auto_convert_{}", insertion_counter);

                        negotiated.inserted_nodes.push(InsertedNode {
                            id: inserted_id,
                            node_type: step.node_type.clone(),
                            between: (source_id.clone(), target_id.clone()),
                            params: step.params.clone(),
                            input_caps: step.input_caps.clone(),
                            output_caps: step.output_caps.clone(),
                        });
                    }

                    // Record the final resolved format
                    if let Some(last_step) = path.steps.last() {
                        negotiated.connections.insert(
                            (source_id.clone(), target_id.clone()),
                            last_step.output_caps.clone(),
                        );
                    }
                } else {
                    // No conversion path available
                    unresolved_mismatches.extend(mismatches);
                }
            } else {
                unresolved_mismatches.extend(mismatches);
            }
        } else {
            // Auto-insert disabled, report mismatches
            unresolved_mismatches.extend(mismatches);
        }
    }

    if !unresolved_mismatches.is_empty() {
        NegotiationResult::Failed(unresolved_mismatches)
    } else if negotiated.has_conversions() {
        NegotiationResult::Negotiated(negotiated)
    } else {
        NegotiationResult::Valid(negotiated)
    }
}

/// Negotiate capabilities using the default conversion registry.
pub fn negotiate_pipeline_default(
    node_capabilities: &HashMap<String, MediaCapabilities>,
    connections: &[(String, String)],
    auto_insert: bool,
) -> NegotiationResult {
    let registry = DefaultConversionRegistry::new();
    negotiate_pipeline(node_capabilities, connections, auto_insert, &registry)
}

#[cfg(test)]
mod tests {
    use super::super::constraints::{AudioConstraints, ConstraintValue};
    use super::*;

    #[test]
    fn test_conversion_path_single() {
        let step = ConversionStep {
            node_type: "AudioResample".to_string(),
            params: serde_json::json!({"target_rate": 16000}),
            input_caps: MediaConstraints::Audio(AudioConstraints::default()),
            output_caps: MediaConstraints::Audio(AudioConstraints::default()),
        };

        let path = ConversionPath::single(step);
        assert_eq!(path.total_nodes, 1);
        assert!(!path.is_empty());
    }

    #[test]
    fn test_conversion_path_empty() {
        let path = ConversionPath::empty();
        assert_eq!(path.total_nodes, 0);
        assert!(path.is_empty());
    }

    #[test]
    fn test_negotiated_capabilities_has_conversions() {
        let mut caps = NegotiatedCapabilities::default();
        assert!(!caps.has_conversions());
        assert_eq!(caps.conversion_count(), 0);

        caps.inserted_nodes.push(InsertedNode {
            id: "_auto_convert_1".to_string(),
            node_type: "AudioResample".to_string(),
            between: ("a".to_string(), "b".to_string()),
            params: serde_json::json!({}),
            input_caps: MediaConstraints::Audio(AudioConstraints::default()),
            output_caps: MediaConstraints::Audio(AudioConstraints::default()),
        });

        assert!(caps.has_conversions());
        assert_eq!(caps.conversion_count(), 1);
    }

    #[test]
    fn test_resolve_flexible_to_fixed_range_to_exact() {
        let source = ConstraintValue::Range {
            min: 16000u32,
            max: 48000,
        };
        let target = ConstraintValue::Exact(32000u32);

        let result = resolve_flexible_to_fixed(&source, &target);
        assert_eq!(result, Some(32000));
    }

    #[test]
    fn test_resolve_flexible_to_fixed_range_to_exact_out_of_range() {
        let source = ConstraintValue::Range {
            min: 16000u32,
            max: 48000,
        };
        let target = ConstraintValue::Exact(8000u32);

        let result = resolve_flexible_to_fixed(&source, &target);
        assert_eq!(result, None);
    }

    #[test]
    fn test_resolve_flexible_to_fixed_set_to_exact() {
        let source = ConstraintValue::Set(vec![16000u32, 44100, 48000]);
        let target = ConstraintValue::Exact(44100u32);

        let result = resolve_flexible_to_fixed(&source, &target);
        assert_eq!(result, Some(44100));
    }

    #[test]
    fn test_negotiate_pipeline_valid() {
        let mut caps = HashMap::new();
        caps.insert(
            "a".to_string(),
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(48000)),
                channels: None,
                format: None,
            })),
        );
        caps.insert(
            "b".to_string(),
            MediaCapabilities::with_input(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(48000)),
                channels: None,
                format: None,
            })),
        );

        let connections = vec![("a".to_string(), "b".to_string())];
        let result = negotiate_pipeline_default(&caps, &connections, false);

        assert!(result.is_success());
        assert!(matches!(result, NegotiationResult::Valid(_)));
    }

    #[test]
    fn test_negotiate_pipeline_mismatch_no_auto() {
        let mut caps = HashMap::new();
        caps.insert(
            "a".to_string(),
            MediaCapabilities::with_output(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(48000)),
                channels: None,
                format: None,
            })),
        );
        caps.insert(
            "b".to_string(),
            MediaCapabilities::with_input(MediaConstraints::Audio(AudioConstraints {
                sample_rate: Some(ConstraintValue::Exact(16000)),
                channels: None,
                format: None,
            })),
        );

        let connections = vec![("a".to_string(), "b".to_string())];
        let result = negotiate_pipeline_default(&caps, &connections, false);

        assert!(!result.is_success());
        assert!(matches!(result, NegotiationResult::Failed(_)));
    }

    #[test]
    fn test_negotiation_result_accessors() {
        let valid = NegotiationResult::Valid(NegotiatedCapabilities::default());
        assert!(valid.is_success());
        assert!(valid.capabilities().is_some());
        assert!(valid.mismatches().is_none());

        let failed = NegotiationResult::Failed(vec![]);
        assert!(!failed.is_success());
        assert!(failed.capabilities().is_none());
        assert!(failed.mismatches().is_some());
    }
}
