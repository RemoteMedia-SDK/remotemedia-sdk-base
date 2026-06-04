//! Schema validator for node parameters
//!
//! Wraps the jsonschema crate to provide efficient, pre-compiled validation
//! of node parameters against their declared JSON Schemas.

use std::collections::HashMap;

use jsonschema::{ValidationError as JsonSchemaError, Validator};
use serde_json::Value;
use tracing::warn;

use crate::nodes::schema::NodeSchemaRegistry;
use crate::Error;

use super::error::{ValidationConstraint, ValidationError};

/// Compiled schema entry for a node type
struct CompiledSchema {
    /// Original JSON Schema
    schema: Value,
    /// Pre-compiled validator
    compiled: Validator,
}

/// Schema validator with pre-compiled schemas for efficient validation
pub struct SchemaValidator {
    /// Compiled schemas indexed by node type
    schemas: HashMap<String, CompiledSchema>,
}

impl SchemaValidator {
    /// Create a new validator from the schema registry
    ///
    /// Pre-compiles all schemas for efficient validation.
    ///
    /// # Arguments
    /// * `registry` - NodeSchemaRegistry containing all node schemas
    ///
    /// # Returns
    /// * `Ok(SchemaValidator)` - Ready to validate
    /// * `Err` - Schema compilation failed
    pub fn from_registry(registry: &NodeSchemaRegistry) -> Result<Self, Error> {
        let mut schemas = HashMap::new();

        for node_schema in registry.iter() {
            if let Some(config_schema) = &node_schema.config_schema {
                // Compile the schema using draft 7 (what schemars generates)
                match jsonschema::draft7::new(config_schema) {
                    Ok(compiled) => {
                        schemas.insert(
                            node_schema.node_type.clone(),
                            CompiledSchema {
                                schema: config_schema.clone(),
                                compiled,
                            },
                        );
                    }
                    Err(e) => {
                        // Log warning but continue - invalid schema shouldn't prevent startup
                        warn!(
                            "Failed to compile schema for node type '{}': {}",
                            node_schema.node_type, e
                        );
                    }
                }
            }
        }

        Ok(Self { schemas })
    }

    /// Create an empty validator (for testing or when no schemas are available)
    pub fn empty() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    /// Add a schema for a specific node type
    ///
    /// Used for testing or dynamic schema registration.
    pub fn add_schema(&mut self, node_type: &str, schema: Value) -> Result<(), Error> {
        let compiled = jsonschema::draft7::new(&schema).map_err(|e| {
            Error::Validation(vec![ValidationError::new(
                "",
                node_type,
                "",
                ValidationConstraint::Other("schema_compilation".to_string()),
                "valid JSON Schema",
                e.to_string(),
            )])
        })?;

        self.schemas
            .insert(node_type.to_string(), CompiledSchema { schema, compiled });

        Ok(())
    }

    /// Check if a node type has a schema
    pub fn has_schema(&self, node_type: &str) -> bool {
        self.schemas.contains_key(node_type)
    }

    /// Get the raw schema for a node type
    pub fn get_schema(&self, node_type: &str) -> Option<Value> {
        self.schemas.get(node_type).map(|s| s.schema.clone())
    }

    /// Get all schemas as a map
    pub fn get_all_schemas(&self) -> HashMap<String, Value> {
        self.schemas
            .iter()
            .map(|(k, v)| (k.clone(), v.schema.clone()))
            .collect()
    }

    /// Validate a single node's parameters
    ///
    /// # Arguments
    /// * `node_id` - ID of the node (for error reporting)
    /// * `node_type` - Type of the node
    /// * `params` - Parameters to validate
    ///
    /// # Returns
    /// * `Ok(())` - Parameters are valid
    /// * `Err(Vec<ValidationError>)` - Validation errors
    pub fn validate_node(
        &self,
        node_id: &str,
        node_type: &str,
        params: &Value,
    ) -> Result<(), Vec<ValidationError>> {
        let compiled = match self.schemas.get(node_type) {
            Some(schema) => schema,
            None => return Ok(()), // No schema = no validation (handled as warning elsewhere)
        };

        // Validate against the compiled schema
        let result = compiled.compiled.validate(params);

        if result.is_ok() {
            return Ok(());
        }

        // Collect all validation errors
        let errors: Vec<ValidationError> = compiled
            .compiled
            .iter_errors(params)
            .map(|error| self.convert_jsonschema_error(node_id, node_type, error))
            .collect();

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Convert a jsonschema error to our ValidationError type
    fn convert_jsonschema_error(
        &self,
        node_id: &str,
        node_type: &str,
        error: JsonSchemaError,
    ) -> ValidationError {
        // Extract path from error instance_path
        let path = format!("/{}", error.instance_path);

        // Determine constraint type from error kind
        let (constraint, expected, received) = self.extract_constraint_info(&error);

        ValidationError::new(node_id, node_type, path, constraint, expected, received)
    }

    /// Extract constraint type and expected/received values from jsonschema error
    fn extract_constraint_info(
        &self,
        error: &JsonSchemaError,
    ) -> (ValidationConstraint, String, String) {
        use jsonschema::error::ValidationErrorKind;

        let instance_str = error.instance.to_string();

        match &error.kind {
            ValidationErrorKind::Type { kind } => {
                let expected_type = format!("{:?}", kind);
                (
                    ValidationConstraint::Type,
                    expected_type.to_lowercase(),
                    self.describe_value_type(&error.instance),
                )
            }
            ValidationErrorKind::Required { property } => (
                ValidationConstraint::Required,
                format!("property '{}'", property),
                "(missing)".to_string(),
            ),
            ValidationErrorKind::Minimum { limit } => (
                ValidationConstraint::Minimum,
                limit.to_string(),
                instance_str,
            ),
            ValidationErrorKind::Maximum { limit } => (
                ValidationConstraint::Maximum,
                limit.to_string(),
                instance_str,
            ),
            ValidationErrorKind::ExclusiveMinimum { limit } => (
                ValidationConstraint::ExclusiveMinimum,
                limit.to_string(),
                instance_str,
            ),
            ValidationErrorKind::ExclusiveMaximum { limit } => (
                ValidationConstraint::ExclusiveMaximum,
                limit.to_string(),
                instance_str,
            ),
            ValidationErrorKind::Enum { options } => {
                // options is a serde_json::Value, format it nicely
                let options_str = if let Some(arr) = options.as_array() {
                    arr.iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                } else {
                    options.to_string()
                };
                (ValidationConstraint::Enum, options_str, instance_str)
            }
            ValidationErrorKind::Pattern { pattern } => (
                ValidationConstraint::Pattern,
                pattern.to_string(),
                instance_str,
            ),
            ValidationErrorKind::MinLength { limit } => (
                ValidationConstraint::MinLength,
                limit.to_string(),
                format!("{} characters", instance_str.len().saturating_sub(2)), // -2 for quotes
            ),
            ValidationErrorKind::MaxLength { limit } => (
                ValidationConstraint::MaxLength,
                limit.to_string(),
                format!("{} characters", instance_str.len().saturating_sub(2)),
            ),
            ValidationErrorKind::MinItems { limit } => {
                let count = error.instance.as_array().map(|a| a.len()).unwrap_or(0);
                (
                    ValidationConstraint::MinItems,
                    limit.to_string(),
                    format!("{} items", count),
                )
            }
            ValidationErrorKind::MaxItems { limit } => {
                let count = error.instance.as_array().map(|a| a.len()).unwrap_or(0);
                (
                    ValidationConstraint::MaxItems,
                    limit.to_string(),
                    format!("{} items", count),
                )
            }
            ValidationErrorKind::AdditionalProperties { unexpected } => (
                ValidationConstraint::AdditionalProperties,
                "no additional properties".to_string(),
                unexpected.join(", "),
            ),
            ValidationErrorKind::Format { format } => (
                ValidationConstraint::Format,
                format.to_string(),
                instance_str,
            ),
            _ => (
                ValidationConstraint::Other(format!("{:?}", error.kind)),
                "constraint".to_string(),
                instance_str,
            ),
        }
    }

    /// Get a human-readable description of a JSON value's type
    fn describe_value_type(&self, value: &Value) -> String {
        match value {
            Value::Null => "null".to_string(),
            Value::Bool(_) => "boolean".to_string(),
            Value::Number(n) => {
                if n.is_i64() || n.is_u64() {
                    "integer".to_string()
                } else {
                    "number".to_string()
                }
            }
            Value::String(_) => "string".to_string(),
            Value::Array(_) => "array".to_string(),
            Value::Object(_) => "object".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_test_validator() -> SchemaValidator {
        let mut validator = SchemaValidator::empty();

        // Add a test schema
        let schema = json!({
            "type": "object",
            "properties": {
                "threshold": {
                    "type": "number",
                    "minimum": 0.0,
                    "maximum": 1.0
                },
                "count": {
                    "type": "integer",
                    "minimum": 1
                },
                "mode": {
                    "type": "string",
                    "enum": ["fast", "accurate"]
                }
            },
            "required": ["threshold"]
        });

        validator.add_schema("TestNode", schema).unwrap();
        validator
    }

    #[test]
    fn test_valid_params() {
        let validator = create_test_validator();

        let params = json!({
            "threshold": 0.5,
            "count": 10,
            "mode": "fast"
        });

        let result = validator.validate_node("test", "TestNode", &params);
        assert!(result.is_ok());
    }

    #[test]
    fn test_type_mismatch() {
        let validator = create_test_validator();

        let params = json!({
            "threshold": "high"  // Should be number
        });

        let result = validator.validate_node("test", "TestNode", &params);
        assert!(result.is_err());

        let errors = result.unwrap_err();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].constraint, ValidationConstraint::Type);
        assert!(errors[0].message.contains("threshold"));
    }

    #[test]
    fn test_required_missing() {
        let validator = create_test_validator();

        let params = json!({
            "count": 5  // Missing required "threshold"
        });

        let result = validator.validate_node("test", "TestNode", &params);
        assert!(result.is_err());

        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.constraint == ValidationConstraint::Required));
    }

    #[test]
    fn test_range_violation_minimum() {
        let validator = create_test_validator();

        let params = json!({
            "threshold": -0.5  // Below minimum 0.0
        });

        let result = validator.validate_node("test", "TestNode", &params);
        assert!(result.is_err());

        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.constraint == ValidationConstraint::Minimum));
    }

    #[test]
    fn test_range_violation_maximum() {
        let validator = create_test_validator();

        let params = json!({
            "threshold": 1.5  // Above maximum 1.0
        });

        let result = validator.validate_node("test", "TestNode", &params);
        assert!(result.is_err());

        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.constraint == ValidationConstraint::Maximum));
    }

    #[test]
    fn test_enum_violation() {
        let validator = create_test_validator();

        let params = json!({
            "threshold": 0.5,
            "mode": "invalid"  // Not in enum
        });

        let result = validator.validate_node("test", "TestNode", &params);
        assert!(result.is_err());

        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| e.constraint == ValidationConstraint::Enum));
    }

    #[test]
    fn test_unknown_node_type() {
        let validator = create_test_validator();

        let params = json!({"anything": "goes"});

        // Unknown node type should pass (no schema = no validation)
        let result = validator.validate_node("test", "UnknownNode", &params);
        assert!(result.is_ok());
    }

    #[test]
    fn test_has_schema() {
        let validator = create_test_validator();

        assert!(validator.has_schema("TestNode"));
        assert!(!validator.has_schema("UnknownNode"));
    }

    #[test]
    fn test_get_schema() {
        let validator = create_test_validator();

        let schema = validator.get_schema("TestNode");
        assert!(schema.is_some());

        let schema = schema.unwrap();
        assert!(schema.get("properties").is_some());
    }

    #[test]
    fn test_multiple_errors() {
        let validator = create_test_validator();

        let params = json!({
            "threshold": "not a number",
            "count": "also not a number"
        });

        let result = validator.validate_node("test", "TestNode", &params);
        assert!(result.is_err());

        let errors = result.unwrap_err();
        assert!(errors.len() >= 2); // At least two type errors
    }

    #[test]
    fn test_nested_object_validation() {
        let mut validator = SchemaValidator::empty();

        let schema = json!({
            "type": "object",
            "properties": {
                "config": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "number"}
                    },
                    "required": ["value"]
                }
            }
        });

        validator.add_schema("NestedNode", schema).unwrap();

        let params = json!({
            "config": {
                "value": "string"  // Should be number
            }
        });

        let result = validator.validate_node("test", "NestedNode", &params);
        assert!(result.is_err());

        let errors = result.unwrap_err();
        assert!(errors[0].path.contains("config"));
    }
}
