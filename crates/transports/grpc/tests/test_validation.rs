//! T035: gRPC validation error format test
//!
//! Tests that gRPC transport returns ValidationError format correctly
//! when node parameter validation fails.

use remotemedia_core::validation::{ValidationConstraint, ValidationError};
use remotemedia_grpc::generated::{ErrorResponse, ErrorType};

/// Test that validation errors serialize to the expected gRPC format
#[test]
fn test_validation_error_serializes_to_json() {
    // GIVEN: A validation error
    let error = ValidationError::new(
        "test_node",
        "SileroVAD",
        "/threshold",
        ValidationConstraint::Maximum,
        "1.0",
        "1.5",
    );

    // WHEN: Serialized to JSON (as gRPC message field)
    let errors = vec![error];
    let json_result = serde_json::to_string(&errors);

    // THEN: Serialization succeeds
    assert!(json_result.is_ok(), "Errors must be JSON serializable");

    let json_str = json_result.unwrap();

    // AND: Contains expected fields
    assert!(json_str.contains("test_node"));
    assert!(json_str.contains("SileroVAD"));
    assert!(json_str.contains("threshold"));
    assert!(json_str.contains("maximum") || json_str.contains("Maximum"));
}

/// Test that ErrorType::Validation maps to correct value
#[test]
fn test_error_type_validation_value() {
    // gRPC ErrorType::Validation should be 1 per proto definition
    assert_eq!(
        ErrorType::Validation as i32,
        1,
        "ErrorType::Validation should map to 1"
    );
}

/// Test that ErrorResponse structure can hold validation errors
#[test]
fn test_error_response_structure() {
    // GIVEN: Validation errors
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

    // WHEN: Creating ErrorResponse with validation errors
    let errors_json = serde_json::to_string(&errors).unwrap();
    let error_response = ErrorResponse {
        error_type: ErrorType::Validation as i32,
        message: errors_json.clone(),
        failing_node_id: String::new(),
        context: format!("{} validation error(s) in node parameters", errors.len()),
        stack_trace: String::new(),
    };

    // THEN: Response has correct structure
    assert_eq!(error_response.error_type, 1);
    assert!(error_response.message.contains("node1"));
    assert!(error_response.message.contains("node2"));
    assert!(error_response.context.contains("2 validation error(s)"));
}

/// Test that validation errors can be deserialized back from gRPC response
#[test]
fn test_validation_errors_roundtrip() {
    // GIVEN: Original validation errors
    let original_errors = vec![
        ValidationError::new(
            "vad_node",
            "SileroVAD",
            "/threshold",
            ValidationConstraint::Maximum,
            "1.0",
            "1.5",
        ),
        ValidationError::new(
            "vad_node",
            "SileroVAD",
            "/mode",
            ValidationConstraint::Enum,
            "fast, balanced, accurate",
            "invalid",
        ),
    ];

    // WHEN: Serialized as would be in gRPC message
    let json_str = serde_json::to_string(&original_errors).unwrap();

    // THEN: Can be deserialized back
    let parsed_errors: Vec<ValidationError> = serde_json::from_str(&json_str).unwrap();

    // AND: All fields are preserved
    assert_eq!(parsed_errors.len(), original_errors.len());
    assert_eq!(parsed_errors[0].node_id, "vad_node");
    assert_eq!(parsed_errors[0].node_type, "SileroVAD");
    assert_eq!(parsed_errors[0].path, "/threshold");
    assert_eq!(parsed_errors[0].constraint, ValidationConstraint::Maximum);
    assert_eq!(parsed_errors[1].constraint, ValidationConstraint::Enum);
}

/// Test validation error message is human-readable
#[test]
fn test_error_message_is_human_readable() {
    let error = ValidationError::new(
        "my_vad",
        "SileroVAD",
        "/threshold",
        ValidationConstraint::Maximum,
        "1.0",
        "1.5",
    );

    // Message should be self-explanatory
    let message = &error.message;
    assert!(message.contains("my_vad"), "Should contain node_id");
    assert!(message.contains("SileroVAD"), "Should contain node_type");
    assert!(
        message.contains("threshold"),
        "Should contain parameter name"
    );
    assert!(message.contains("1.0"), "Should contain expected value");
    assert!(message.contains("1.5"), "Should contain received value");
}

/// Test multiple error types are distinguishable
#[test]
fn test_different_constraint_types_are_distinguishable() {
    let type_error = ValidationError::new(
        "n1",
        "Test",
        "/p1",
        ValidationConstraint::Type,
        "number",
        "string",
    );

    let required_error = ValidationError::new(
        "n2",
        "Test",
        "/p2",
        ValidationConstraint::Required,
        "required",
        "(missing)",
    );

    let range_error = ValidationError::new(
        "n3",
        "Test",
        "/p3",
        ValidationConstraint::Maximum,
        "100",
        "150",
    );

    let enum_error = ValidationError::new(
        "n4",
        "Test",
        "/p4",
        ValidationConstraint::Enum,
        "a, b, c",
        "d",
    );

    // Serialize all
    let errors = vec![type_error, required_error, range_error, enum_error];
    let json = serde_json::to_string(&errors).unwrap();

    // All constraint types should be present and distinguishable
    let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.len(), 4);

    // Each should have distinct constraint field
    let constraints: Vec<String> = parsed
        .iter()
        .map(|e| e["constraint"].as_str().unwrap_or("").to_string())
        .collect();

    assert!(constraints.contains(&"type".to_string()));
    assert!(constraints.contains(&"required".to_string()));
    assert!(constraints.contains(&"maximum".to_string()));
    assert!(
        constraints.contains(&"enum".to_string()),
        "Expected 'enum' but got {:?}",
        constraints
    );
}
