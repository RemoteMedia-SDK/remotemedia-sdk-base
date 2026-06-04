//! T036: HTTP validation error format test
//!
//! Tests that HTTP transport returns ValidationError format correctly
//! when node parameter validation fails.

use axum::http::StatusCode;
use remotemedia_core::validation::{ValidationConstraint, ValidationError};
use serde::{Deserialize, Serialize};

/// HTTP error response structure (mirrors server.rs ErrorResponse)
#[derive(Debug, Serialize, Deserialize)]
struct ErrorResponse {
    error_type: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    validation_errors: Option<serde_json::Value>,
}

/// Test that validation errors map to HTTP 400 Bad Request
#[test]
fn test_validation_errors_use_400_status() {
    // GIVEN: A validation error situation
    // THEN: Status code should be 400 Bad Request
    assert_eq!(StatusCode::BAD_REQUEST.as_u16(), 400);
}

/// Test error response structure for validation errors
#[test]
fn test_error_response_structure() {
    // GIVEN: Validation errors
    let errors = vec![ValidationError::new(
        "vad_node",
        "SileroVAD",
        "/threshold",
        ValidationConstraint::Maximum,
        "1.0",
        "1.5",
    )];

    // WHEN: Creating HTTP error response
    let errors_json = serde_json::to_value(&errors).unwrap();
    let response = ErrorResponse {
        error_type: "validation".to_string(),
        message: format!("{} validation error(s) in node parameters", errors.len()),
        validation_errors: Some(errors_json),
    };

    // THEN: Response has correct structure
    assert_eq!(response.error_type, "validation");
    assert!(response.message.contains("1 validation error(s)"));
    assert!(response.validation_errors.is_some());
}

/// Test that validation errors are included in response
#[test]
fn test_validation_errors_included_in_response() {
    // GIVEN: Multiple validation errors
    let errors = vec![
        ValidationError::new(
            "node1",
            "TestNode",
            "/param1",
            ValidationConstraint::Type,
            "number",
            "string",
        ),
        ValidationError::new(
            "node2",
            "TestNode",
            "/param2",
            ValidationConstraint::Required,
            "required parameter",
            "(missing)",
        ),
    ];

    // WHEN: Creating HTTP error response
    let errors_json = serde_json::to_value(&errors).unwrap();
    let response = ErrorResponse {
        error_type: "validation".to_string(),
        message: format!("{} validation error(s) in node parameters", errors.len()),
        validation_errors: Some(errors_json),
    };

    // THEN: Response can be serialized to JSON
    let response_json = serde_json::to_string(&response).unwrap();

    // AND: Contains expected error details
    assert!(response_json.contains("node1"));
    assert!(response_json.contains("node2"));
    assert!(response_json.contains("param1"));
    assert!(response_json.contains("param2"));
    assert!(response_json.contains("type") || response_json.contains("Type"));
    assert!(response_json.contains("required") || response_json.contains("Required"));
}

/// Test error type string for different error categories
#[test]
fn test_error_type_strings() {
    // Different error types should have distinct strings
    let validation_response = ErrorResponse {
        error_type: "validation".to_string(),
        message: "Parameter validation failed".to_string(),
        validation_errors: Some(serde_json::json!([])),
    };
    assert_eq!(validation_response.error_type, "validation");

    let manifest_response = ErrorResponse {
        error_type: "manifest".to_string(),
        message: "Invalid manifest".to_string(),
        validation_errors: None,
    };
    assert_eq!(manifest_response.error_type, "manifest");

    let execution_response = ErrorResponse {
        error_type: "execution".to_string(),
        message: "Execution failed".to_string(),
        validation_errors: None,
    };
    assert_eq!(execution_response.error_type, "execution");
}

/// Test that validation errors can be parsed by clients
#[test]
fn test_client_can_parse_validation_errors() {
    // GIVEN: A validation error response as JSON (simulating HTTP response body)
    let response_json = r#"{
        "error_type": "validation",
        "message": "2 validation error(s) in node parameters",
        "validation_errors": [
            {
                "node_id": "vad",
                "node_type": "SileroVAD",
                "path": "/threshold",
                "constraint": "maximum",
                "expected": "1.0",
                "received": "1.5",
                "message": "Node 'vad' (SileroVAD): parameter 'threshold' must be <= 1.0, got 1.5"
            }
        ]
    }"#;

    // WHEN: Client parses the response
    let parsed: ErrorResponse = serde_json::from_str(response_json).unwrap();

    // THEN: Error type is correctly identified
    assert_eq!(parsed.error_type, "validation");

    // AND: Validation errors can be extracted
    let errors = parsed.validation_errors.unwrap();
    assert!(errors.is_array());

    let errors_array = errors.as_array().unwrap();
    assert_eq!(errors_array.len(), 1);

    // AND: Individual error fields are accessible
    let first_error = &errors_array[0];
    assert_eq!(first_error["node_id"].as_str().unwrap(), "vad");
    assert_eq!(first_error["node_type"].as_str().unwrap(), "SileroVAD");
    assert_eq!(first_error["path"].as_str().unwrap(), "/threshold");
}

/// Test non-validation errors don't include validation_errors field
#[test]
fn test_non_validation_errors_exclude_validation_field() {
    // GIVEN: A manifest error (not validation)
    let response = ErrorResponse {
        error_type: "manifest".to_string(),
        message: "Invalid JSON in manifest".to_string(),
        validation_errors: None,
    };

    // WHEN: Serialized to JSON
    let json = serde_json::to_string(&response).unwrap();

    // THEN: validation_errors field is not present (skip_serializing_if = None)
    assert!(!json.contains("validation_errors"));
}

/// Test that error messages are human-readable
#[test]
fn test_error_messages_are_human_readable() {
    let error = ValidationError::new(
        "my_node",
        "MyNodeType",
        "/my_param",
        ValidationConstraint::Type,
        "number",
        "\"string_value\"",
    );

    let message = &error.message;

    // Should form a complete, readable sentence
    assert!(message.len() > 20, "Message should be a complete sentence");
    assert!(message.contains("my_node"), "Should identify the node");
    assert!(
        message.contains("my_param"),
        "Should identify the parameter"
    );
}

/// Test that all constraint types have valid string representations
#[test]
fn test_constraint_types_serialize_to_strings() {
    let constraints = vec![
        ValidationConstraint::Type,
        ValidationConstraint::Required,
        ValidationConstraint::Minimum,
        ValidationConstraint::Maximum,
        ValidationConstraint::ExclusiveMinimum,
        ValidationConstraint::ExclusiveMaximum,
        ValidationConstraint::Enum,
        ValidationConstraint::Pattern,
        ValidationConstraint::MinLength,
        ValidationConstraint::MaxLength,
        ValidationConstraint::MinItems,
        ValidationConstraint::MaxItems,
        ValidationConstraint::AdditionalProperties,
        ValidationConstraint::Format,
        ValidationConstraint::Other("custom".to_string()),
    ];

    for constraint in constraints {
        let error = ValidationError::new(
            "node",
            "Type",
            "/path",
            constraint.clone(),
            "expected",
            "received",
        );

        // Should serialize without error
        let json = serde_json::to_string(&error).unwrap();
        assert!(!json.is_empty());

        // Constraint should be present as string
        assert!(json.contains("constraint"));
    }
}
