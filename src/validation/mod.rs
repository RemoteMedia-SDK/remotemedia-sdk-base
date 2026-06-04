//! Node parameter validation module
//!
//! Provides JSON Schema validation for node parameters in pipeline manifests.
//! Validation happens at the core level before any node instantiation,
//! ensuring all transports (gRPC, HTTP, FFI, WebRTC) receive the same behavior.
//!
//! # Usage
//!
//! ```ignore
//! use remotemedia_core::validation::{SchemaValidator, validate_manifest};
//! use remotemedia_core::nodes::schema::NodeSchemaRegistry;
//!
//! // Create validator from schema registry (typically done at startup)
//! let registry = NodeSchemaRegistry::default();
//! let validator = SchemaValidator::from_registry(&registry)?;
//!
//! // Validate a manifest before execution
//! let result = validate_manifest(&manifest, &validator);
//! match result {
//!     ValidationResult::Valid => { /* proceed with execution */ }
//!     ValidationResult::PartiallyValid { warnings } => { /* log warnings, proceed */ }
//!     ValidationResult::Invalid { errors } => { /* return errors to caller */ }
//! }
//! ```

mod error;
mod schema_validator;

pub use error::{ValidationConstraint, ValidationError, ValidationWarning, WarningType};
pub use schema_validator::SchemaValidator;

// Re-export capability validation types for convenience
pub use crate::capabilities::validation::CapabilityMismatch;

use crate::capabilities::{
    validation::{validate_pipeline as validate_caps_pipeline, CapabilityValidationResult},
    MediaCapabilities,
};
use crate::manifest::Manifest;

/// Result of validating a manifest
#[derive(Debug, Clone)]
pub enum ValidationResult {
    /// All parameters are valid
    Valid,
    /// Valid with warnings (e.g., missing schemas for some nodes)
    PartiallyValid { warnings: Vec<ValidationWarning> },
    /// One or more validation errors
    Invalid { errors: Vec<ValidationError> },
}

impl ValidationResult {
    /// Check if validation passed (Valid or PartiallyValid)
    pub fn is_ok(&self) -> bool {
        !matches!(self, ValidationResult::Invalid { .. })
    }

    /// Get errors if invalid, empty vec otherwise
    pub fn errors(&self) -> Vec<ValidationError> {
        match self {
            ValidationResult::Invalid { errors } => errors.clone(),
            _ => Vec::new(),
        }
    }

    /// Get warnings if any
    pub fn warnings(&self) -> Vec<ValidationWarning> {
        match self {
            ValidationResult::PartiallyValid { warnings } => warnings.clone(),
            _ => Vec::new(),
        }
    }
}

/// Validate an entire manifest before execution
///
/// This is the primary entry point for validation. It:
/// 1. Iterates over all nodes in the manifest
/// 2. Validates each node's parameters against its schema
/// 3. Collects all errors and warnings
/// 4. Returns aggregated result
///
/// # Arguments
/// * `manifest` - The pipeline manifest to validate
/// * `validator` - Pre-compiled schema validator
///
/// # Returns
/// * `ValidationResult::Valid` - All nodes valid
/// * `ValidationResult::PartiallyValid` - Valid with warnings
/// * `ValidationResult::Invalid` - One or more errors
pub fn validate_manifest(manifest: &Manifest, validator: &SchemaValidator) -> ValidationResult {
    let mut errors: Vec<ValidationError> = Vec::new();
    let mut warnings: Vec<ValidationWarning> = Vec::new();

    for node in &manifest.nodes {
        // Check if schema exists for this node type
        if !validator.has_schema(&node.node_type) {
            warnings.push(ValidationWarning {
                node_id: node.id.clone(),
                node_type: node.node_type.clone(),
                warning_type: WarningType::MissingSchema,
                message: format!(
                    "Node '{}' ({}) has no parameter schema - skipping validation",
                    node.id, node.node_type
                ),
            });
            continue;
        }

        // Validate node parameters against schema
        if let Err(node_errors) = validator.validate_node(&node.id, &node.node_type, &node.params) {
            errors.extend(node_errors);
        }
    }

    if !errors.is_empty() {
        ValidationResult::Invalid { errors }
    } else if !warnings.is_empty() {
        ValidationResult::PartiallyValid { warnings }
    } else {
        ValidationResult::Valid
    }
}

/// Get parameter schema for a node type
///
/// # Arguments
/// * `node_type` - Type of the node
/// * `validator` - Schema validator containing compiled schemas
///
/// # Returns
/// * `Some(schema)` - JSON Schema for the node's parameters
/// * `None` - Node type not found or has no schema
pub fn get_node_schema(node_type: &str, validator: &SchemaValidator) -> Option<serde_json::Value> {
    validator.get_schema(node_type)
}

/// Get all registered node schemas
///
/// Returns a map of node_type -> JSON Schema for all registered nodes.
pub fn get_all_schemas(
    validator: &SchemaValidator,
) -> std::collections::HashMap<String, serde_json::Value> {
    validator.get_all_schemas()
}

/// Validate media capabilities compatibility in a manifest (spec 022).
///
/// Checks that all connections between nodes have compatible media capabilities.
/// This validates:
/// - Media types match between connected nodes
/// - Specific constraints (sample rate, channels, format, etc.) are compatible
///
/// # Arguments
/// * `manifest` - The pipeline manifest to validate
///
/// # Returns
/// * `Ok(())` if all capabilities are compatible
/// * `Err(mismatches)` if incompatibilities are found
pub fn validate_capabilities(manifest: &Manifest) -> Result<(), Vec<CapabilityMismatch>> {
    // Build a map of node ID -> MediaCapabilities
    let mut node_caps: std::collections::HashMap<String, MediaCapabilities> =
        std::collections::HashMap::new();

    for node in &manifest.nodes {
        if let Some(caps) = &node.media_capabilities {
            node_caps.insert(node.id.clone(), caps.clone());
        }
        // Nodes without media_capabilities use empty (accept any)
    }

    // Build connections list
    let connections: Vec<(String, String)> = manifest
        .connections
        .iter()
        .map(|c| (c.from.clone(), c.to.clone()))
        .collect();

    // Validate using the capabilities validation module
    match validate_caps_pipeline(&node_caps, &connections) {
        CapabilityValidationResult::Valid => Ok(()),
        CapabilityValidationResult::Invalid(mismatches) => Err(mismatches),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_result_is_ok() {
        assert!(ValidationResult::Valid.is_ok());
        assert!(ValidationResult::PartiallyValid { warnings: vec![] }.is_ok());
        assert!(!ValidationResult::Invalid { errors: vec![] }.is_ok());
    }

    #[test]
    fn test_validation_result_errors() {
        let errors = vec![ValidationError {
            node_id: "test".to_string(),
            node_type: "TestNode".to_string(),
            path: "/param".to_string(),
            constraint: ValidationConstraint::Type,
            expected: "number".to_string(),
            received: "\"string\"".to_string(),
            message: "Test error".to_string(),
        }];

        let result = ValidationResult::Invalid {
            errors: errors.clone(),
        };
        assert_eq!(result.errors().len(), 1);

        let valid = ValidationResult::Valid;
        assert!(valid.errors().is_empty());
    }
}
