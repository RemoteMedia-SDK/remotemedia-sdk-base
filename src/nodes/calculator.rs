//! CalculatorNode - JSON processing demonstration node
//! Feature: 004-generic-streaming
//!
//! This node demonstrates JSON data processing by accepting calculator
//! requests (operation + operands) and returning JSON results.
//!
//! Input: JSON with structure:
//!   { "operation": "add"|"subtract"|"multiply"|"divide", "operands": [a, b] }
//!
//! Output: JSON with structure:
//!   { "result": number, "operation": string }

use crate::data::RuntimeData;
use crate::nodes::SyncStreamingNode;
use crate::Error;
use serde_json::json;

/// CalculatorNode for JSON processing
pub struct CalculatorNode {
    /// Node ID
    pub id: String,
}

impl CalculatorNode {
    /// Create new calculator node
    pub fn new(id: String, _params: &str) -> Result<Self, Error> {
        Ok(Self { id })
    }

    /// Process JSON calculator request
    fn process_internal(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        // Extract JSON data
        let json_value = match data {
            RuntimeData::Json(value) => value,
            _ => {
                return Err(Error::InvalidInput {
                    message: "CalculatorNode expects JSON input".into(),
                    node_id: self.id.clone(),
                    context: format!("Received {:?}", data.data_type()),
                });
            }
        };

        // Parse operation and operands
        let operation = json_value
            .get("operation")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::InvalidInput {
                message: "Missing 'operation' field".into(),
                node_id: self.id.clone(),
                context: "Expected: add, subtract, multiply, divide".into(),
            })?;

        let operands = json_value
            .get("operands")
            .and_then(|v| v.as_array())
            .ok_or_else(|| Error::InvalidInput {
                message: "Missing 'operands' array".into(),
                node_id: self.id.clone(),
                context: "Expected: [number, number]".into(),
            })?;

        if operands.len() != 2 {
            return Err(Error::InvalidInput {
                message: format!("Expected 2 operands, got {}", operands.len()),
                node_id: self.id.clone(),
                context: "Operands array must contain exactly 2 numbers".into(),
            });
        }

        let a = operands[0].as_f64().ok_or_else(|| Error::InvalidInput {
            message: "First operand must be a number".into(),
            node_id: self.id.clone(),
            context: format!("Got: {:?}", operands[0]),
        })?;

        let b = operands[1].as_f64().ok_or_else(|| Error::InvalidInput {
            message: "Second operand must be a number".into(),
            node_id: self.id.clone(),
            context: format!("Got: {:?}", operands[1]),
        })?;

        // Perform calculation
        let result = match operation {
            "add" => a + b,
            "subtract" => a - b,
            "multiply" => a * b,
            "divide" => {
                if b == 0.0 {
                    return Err(Error::Execution("Division by zero".into()));
                }
                a / b
            }
            _ => {
                return Err(Error::InvalidInput {
                    message: format!("Unknown operation: {}", operation),
                    node_id: self.id.clone(),
                    context: "Supported: add, subtract, multiply, divide".into(),
                });
            }
        };

        // Return JSON result
        let output = json!({
            "result": result,
            "operation": operation,
            "operands": [a, b]
        });

        Ok(RuntimeData::Json(output))
    }
}

impl SyncStreamingNode for CalculatorNode {
    fn node_type(&self) -> &str {
        "CalculatorNode"
    }

    fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        self.process_internal(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::SyncStreamingNode;
    use serde_json::json;

    #[test]
    fn test_addition() {
        let node = CalculatorNode::new("calc".to_string(), "").unwrap();
        let input = RuntimeData::Json(json!({
            "operation": "add",
            "operands": [10, 20]
        }));

        let result = SyncStreamingNode::process(&node, input).unwrap();
        match result {
            RuntimeData::Json(value) => {
                assert_eq!(value["result"], 30.0);
                assert_eq!(value["operation"], "add");
            }
            _ => panic!("Expected JSON output"),
        }
    }

    #[test]
    fn test_multiplication() {
        let node = CalculatorNode::new("calc".to_string(), "").unwrap();
        let input = RuntimeData::Json(json!({
            "operation": "multiply",
            "operands": [5, 7]
        }));

        let result = SyncStreamingNode::process(&node, input).unwrap();
        match result {
            RuntimeData::Json(value) => {
                assert_eq!(value["result"], 35.0);
            }
            _ => panic!("Expected JSON output"),
        }
    }

    #[test]
    fn test_division() {
        let node = CalculatorNode::new("calc".to_string(), "").unwrap();
        let input = RuntimeData::Json(json!({
            "operation": "divide",
            "operands": [100, 4]
        }));

        let result = SyncStreamingNode::process(&node, input).unwrap();
        match result {
            RuntimeData::Json(value) => {
                assert_eq!(value["result"], 25.0);
            }
            _ => panic!("Expected JSON output"),
        }
    }

    #[test]
    fn test_division_by_zero() {
        let node = CalculatorNode::new("calc".to_string(), "").unwrap();
        let input = RuntimeData::Json(json!({
            "operation": "divide",
            "operands": [10, 0]
        }));

        let result = SyncStreamingNode::process(&node, input);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_operation() {
        let node = CalculatorNode::new("calc".to_string(), "").unwrap();
        let input = RuntimeData::Json(json!({
            "operation": "power",
            "operands": [2, 3]
        }));

        let result = SyncStreamingNode::process(&node, input);
        assert!(result.is_err());
    }

    #[test]
    fn test_wrong_data_type() {
        let node = CalculatorNode::new("calc".to_string(), "").unwrap();
        let input = RuntimeData::Text("not json".to_string());

        let result = SyncStreamingNode::process(&node, input);
        assert!(result.is_err());
    }
}
