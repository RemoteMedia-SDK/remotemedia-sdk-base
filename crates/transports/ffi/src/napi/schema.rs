//! Node schema registry NAPI bindings
//!
//! Exposes the node schema registry to Node.js for:
//! - Runtime introspection of available node types
//! - TypeScript type generation
//! - Pipeline validation
//!
//! # Example
//!
//! ```javascript
//! const { getNodeSchemas, getNodeSchema, generateTypescript } = require('@matbee/remotemedia-native');
//!
//! // Get all schemas as JSON array
//! const schemas = getNodeSchemas();
//! console.log('Available nodes:', schemas.map(s => s.nodeType));
//!
//! // Get specific node schema
//! const ttsSchema = getNodeSchema('KokoroTTSNode');
//! console.log('TTS accepts:', ttsSchema.accepts);
//!
//! // Generate TypeScript definitions
//! const typescriptDefs = generateTypescript();
//! fs.writeFileSync('node-types.d.ts', typescriptDefs);
//! ```

use napi_derive::napi;
use remotemedia_core::nodes::schema::{
    create_builtin_schema_registry, LatencyClass, NodeCapabilitiesSchema, NodeParameter,
    NodeSchema, NodeSchemaRegistry, ParameterType, RuntimeDataType,
};
use serde_json::Value;

/// RuntimeData type variant for Node.js
#[napi]
pub enum NapiRuntimeDataType {
    Audio = 0,
    Video = 1,
    Json = 2,
    Text = 3,
    Binary = 4,
    Tensor = 5,
    Numpy = 6,
    ControlMessage = 7,
}

impl From<RuntimeDataType> for NapiRuntimeDataType {
    fn from(t: RuntimeDataType) -> Self {
        match t {
            RuntimeDataType::Audio => NapiRuntimeDataType::Audio,
            RuntimeDataType::Image => NapiRuntimeDataType::Video,
            RuntimeDataType::Video => NapiRuntimeDataType::Video,
            RuntimeDataType::Json => NapiRuntimeDataType::Json,
            RuntimeDataType::Text => NapiRuntimeDataType::Text,
            RuntimeDataType::Binary => NapiRuntimeDataType::Binary,
            RuntimeDataType::Tensor => NapiRuntimeDataType::Tensor,
            RuntimeDataType::Numpy => NapiRuntimeDataType::Numpy,
            RuntimeDataType::ControlMessage => NapiRuntimeDataType::ControlMessage,
        }
    }
}

/// Latency class for Node.js
#[napi]
pub enum NapiLatencyClass {
    /// Sub-millisecond (< 1ms)
    Realtime = 0,
    /// Fast (1-10ms)
    Fast = 1,
    /// Medium (10-100ms)
    Medium = 2,
    /// Slow (100ms-1s)
    Slow = 3,
    /// Very slow (> 1s)
    Batch = 4,
}

impl From<LatencyClass> for NapiLatencyClass {
    fn from(l: LatencyClass) -> Self {
        match l {
            LatencyClass::Realtime => NapiLatencyClass::Realtime,
            LatencyClass::Fast => NapiLatencyClass::Fast,
            LatencyClass::Medium => NapiLatencyClass::Medium,
            LatencyClass::Slow => NapiLatencyClass::Slow,
            LatencyClass::Batch => NapiLatencyClass::Batch,
        }
    }
}

/// Node capabilities for scheduling hints
#[napi(object)]
pub struct NapiNodeCapabilities {
    /// Can process inputs in parallel
    pub parallelizable: bool,
    /// Benefits from batched inputs
    pub batch_aware: bool,
    /// Supports control messages
    pub supports_control: bool,
    /// Latency class (0=realtime, 1=fast, 2=medium, 3=slow, 4=batch)
    pub latency_class: u8,
}

impl From<&NodeCapabilitiesSchema> for NapiNodeCapabilities {
    fn from(c: &NodeCapabilitiesSchema) -> Self {
        NapiNodeCapabilities {
            parallelizable: c.parallelizable,
            batch_aware: c.batch_aware,
            supports_control: c.supports_control,
            latency_class: match c.latency_class {
                LatencyClass::Realtime => 0,
                LatencyClass::Fast => 1,
                LatencyClass::Medium => 2,
                LatencyClass::Slow => 3,
                LatencyClass::Batch => 4,
            },
        }
    }
}

/// Parameter type for node configuration
#[napi]
pub enum NapiParameterType {
    String = 0,
    Number = 1,
    Integer = 2,
    Boolean = 3,
    Array = 4,
    Object = 5,
}

impl From<&ParameterType> for NapiParameterType {
    fn from(t: &ParameterType) -> Self {
        match t {
            ParameterType::String => NapiParameterType::String,
            ParameterType::Number => NapiParameterType::Number,
            ParameterType::Integer => NapiParameterType::Integer,
            ParameterType::Boolean => NapiParameterType::Boolean,
            ParameterType::Array => NapiParameterType::Array,
            ParameterType::Object => NapiParameterType::Object,
        }
    }
}

/// A single configuration parameter for a node
#[napi(object)]
pub struct NapiNodeParameter {
    /// Parameter name
    pub name: String,
    /// Parameter type as string (string, number, integer, boolean, array, object)
    pub param_type: String,
    /// Human-readable description
    pub description: Option<String>,
    /// Default value as JSON string
    pub default_value: Option<String>,
    /// Whether the parameter is required
    pub required: bool,
    /// Allowed values as JSON array string (for enum constraints)
    pub enum_values: Option<String>,
    /// Minimum value (for numbers)
    pub minimum: Option<f64>,
    /// Maximum value (for numbers)
    pub maximum: Option<f64>,
}

impl From<&NodeParameter> for NapiNodeParameter {
    fn from(p: &NodeParameter) -> Self {
        NapiNodeParameter {
            name: p.name.clone(),
            param_type: match p.param_type {
                ParameterType::String => "string".to_string(),
                ParameterType::Number => "number".to_string(),
                ParameterType::Integer => "integer".to_string(),
                ParameterType::Boolean => "boolean".to_string(),
                ParameterType::Array => "array".to_string(),
                ParameterType::Object => "object".to_string(),
            },
            description: p.description.clone(),
            default_value: p.default.as_ref().map(|v| v.to_string()),
            required: p.required,
            enum_values: if p.enum_values.is_empty() {
                None
            } else {
                Some(serde_json::to_string(&p.enum_values).unwrap_or_default())
            },
            minimum: p.minimum,
            maximum: p.maximum,
        }
    }
}

/// Node schema exposed to Node.js
#[napi(object)]
pub struct NapiNodeSchema {
    /// Node type identifier
    pub node_type: String,
    /// Human-readable description
    pub description: Option<String>,
    /// Category (audio, video, ml, text, utility)
    pub category: Option<String>,
    /// Accepted input RuntimeData types as strings
    pub accepts: Vec<String>,
    /// Produced output RuntimeData types as strings
    pub produces: Vec<String>,
    /// Configuration parameters (extracted from config_schema)
    pub parameters: Vec<NapiNodeParameter>,
    /// Configuration JSON Schema (as JSON string)
    pub config_schema: Option<String>,
    /// Default configuration (as JSON string)
    pub config_defaults: Option<String>,
    /// Whether this is a Python node
    pub is_python: bool,
    /// Whether this node supports streaming
    pub streaming: bool,
    /// Whether this node produces multiple outputs per input
    pub multi_output: bool,
    /// Execution capabilities (optional)
    pub capabilities: Option<NapiNodeCapabilities>,
}

impl From<&NodeSchema> for NapiNodeSchema {
    fn from(s: &NodeSchema) -> Self {
        NapiNodeSchema {
            node_type: s.node_type.clone(),
            description: s.description.clone(),
            category: s.category.clone(),
            accepts: s
                .accepts
                .iter()
                .map(|t| format!("{:?}", t).to_lowercase())
                .collect(),
            produces: s
                .produces
                .iter()
                .map(|t| format!("{:?}", t).to_lowercase())
                .collect(),
            parameters: s.get_parameters().iter().map(|p| p.into()).collect(),
            config_schema: s.config_schema.as_ref().map(|v| v.to_string()),
            config_defaults: s.config_defaults.as_ref().map(|v| v.to_string()),
            is_python: s.is_python,
            streaming: s.streaming,
            multi_output: s.multi_output,
            capabilities: s.capabilities.as_ref().map(|c| c.into()),
        }
    }
}

// Thread-safe global registry
use std::sync::OnceLock;
static SCHEMA_REGISTRY: OnceLock<NodeSchemaRegistry> = OnceLock::new();

fn get_registry() -> &'static NodeSchemaRegistry {
    SCHEMA_REGISTRY.get_or_init(create_builtin_schema_registry)
}

/// Get all registered node schemas
///
/// Returns an array of node schema objects with metadata about each node type.
///
/// # Example
///
/// ```javascript
/// const schemas = getNodeSchemas();
/// schemas.forEach(schema => {
///   console.log(`${schema.nodeType}: ${schema.description}`);
///   console.log(`  Accepts: ${schema.accepts.join(', ')}`);
///   console.log(`  Produces: ${schema.produces.join(', ')}`);
/// });
/// ```
#[napi]
pub fn get_node_schemas() -> Vec<NapiNodeSchema> {
    get_registry().iter().map(|s| s.into()).collect()
}

/// Get schema for a specific node type
///
/// Returns the schema for the given node type, or null if not found.
///
/// # Arguments
///
/// * `node_type` - The node type identifier (e.g., "KokoroTTSNode", "AudioResample")
///
/// # Example
///
/// ```javascript
/// const schema = getNodeSchema('KokoroTTSNode');
/// if (schema) {
///   console.log('Config schema:', schema.configSchema);
/// }
/// ```
#[napi]
pub fn get_node_schema(node_type: String) -> Option<NapiNodeSchema> {
    get_registry().get(&node_type).map(|s| s.into())
}

/// Get configuration parameters for a specific node type
///
/// Returns an array of parameter definitions, or empty array if node not found.
///
/// # Arguments
///
/// * `node_type` - The node type identifier (e.g., "KokoroTTSNode")
///
/// # Example
///
/// ```javascript
/// const params = getNodeParameters('KokoroTTSNode');
/// params.forEach(p => {
///   console.log(`${p.name} (${p.paramType}): ${p.description}`);
///   if (p.enumValues) {
///     console.log(`  Allowed values: ${JSON.parse(p.enumValues).join(', ')}`);
///   }
/// });
/// ```
#[napi]
pub fn get_node_parameters(node_type: String) -> Vec<NapiNodeParameter> {
    get_registry()
        .get(&node_type)
        .map(|s| s.get_parameters().iter().map(|p| p.into()).collect())
        .unwrap_or_default()
}

/// Get all node schemas as JSON array
///
/// Returns the complete schema registry as a JSON string for serialization.
///
/// # Example
///
/// ```javascript
/// const json = getNodeSchemasJson();
/// const schemas = JSON.parse(json);
/// ```
#[napi]
pub fn get_node_schemas_json() -> String {
    let registry = get_registry();
    registry.to_json().to_string()
}

/// Get schema for a specific node type as JSON
///
/// # Arguments
///
/// * `node_type` - The node type identifier
///
/// # Example
///
/// ```javascript
/// const json = getNodeSchemaJson('KokoroTTSNode');
/// if (json) {
///   const schema = JSON.parse(json);
/// }
/// ```
#[napi]
pub fn get_node_schema_json(node_type: String) -> Option<String> {
    get_registry()
        .get(&node_type)
        .map(|s| serde_json::to_string(s).unwrap_or_default())
}

/// Get all registered node type names
///
/// Returns a simple array of node type identifiers.
///
/// # Example
///
/// ```javascript
/// const nodeTypes = getNodeTypes();
/// console.log('Available nodes:', nodeTypes.join(', '));
/// ```
#[napi]
pub fn get_node_types() -> Vec<String> {
    get_registry().iter().map(|s| s.node_type.clone()).collect()
}

/// Get node types by category
///
/// Returns node types filtered by category (e.g., "audio", "ml", "video").
///
/// # Arguments
///
/// * `category` - Category to filter by
///
/// # Example
///
/// ```javascript
/// const audioNodes = getNodeTypesByCategory('audio');
/// console.log('Audio nodes:', audioNodes);
/// ```
#[napi]
pub fn get_node_types_by_category(category: String) -> Vec<String> {
    get_registry()
        .iter()
        .filter(|s| s.category.as_deref() == Some(&category))
        .map(|s| s.node_type.clone())
        .collect()
}

/// Check if a node type is registered
///
/// # Arguments
///
/// * `node_type` - The node type identifier to check
///
/// # Example
///
/// ```javascript
/// if (hasNodeType('CustomNode')) {
///   // Node exists
/// }
/// ```
#[napi]
pub fn has_node_type(node_type: String) -> bool {
    get_registry().get(&node_type).is_some()
}

/// Get categories of all registered nodes
///
/// Returns unique category names from all registered nodes.
///
/// # Example
///
/// ```javascript
/// const categories = getNodeCategories();
/// // ['audio', 'ml', 'video', 'text', 'utility']
/// ```
#[napi]
pub fn get_node_categories() -> Vec<String> {
    let mut categories: Vec<String> = get_registry()
        .iter()
        .filter_map(|s| s.category.clone())
        .collect();
    categories.sort();
    categories.dedup();
    categories
}

/// Validate a pipeline manifest against node schemas
///
/// Returns an array of validation errors, empty if valid.
///
/// # Arguments
///
/// * `manifest_json` - Pipeline manifest as JSON string
///
/// # Example
///
/// ```javascript
/// const errors = validateManifest(manifestJson);
/// if (errors.length > 0) {
///   console.error('Validation errors:', errors);
/// }
/// ```
#[napi]
pub fn validate_manifest(manifest_json: String) -> napi::Result<Vec<String>> {
    let manifest: Value = serde_json::from_str(&manifest_json)
        .map_err(|e| napi::Error::from_reason(format!("Invalid JSON: {}", e)))?;

    let mut errors = Vec::new();
    let registry = get_registry();

    // Validate nodes array
    if let Some(nodes) = manifest.get("nodes").and_then(|n| n.as_array()) {
        for (i, node) in nodes.iter().enumerate() {
            // Check node_type exists
            if let Some(node_type) = node
                .get("node_type")
                .or(node.get("nodeType"))
                .and_then(|t| t.as_str())
            {
                if registry.get(node_type).is_none() {
                    errors.push(format!("Node {}: Unknown node type '{}'", i, node_type));
                }
            } else {
                errors.push(format!("Node {}: Missing 'node_type' field", i));
            }

            // Check id exists
            if node.get("id").and_then(|id| id.as_str()).is_none() {
                errors.push(format!("Node {}: Missing 'id' field", i));
            }
        }
    } else {
        errors.push("Missing 'nodes' array in manifest".to_string());
    }

    // Validate connections reference valid node IDs
    if let Some(connections) = manifest.get("connections").and_then(|c| c.as_array()) {
        let node_ids: Vec<&str> = manifest
            .get("nodes")
            .and_then(|n| n.as_array())
            .map(|nodes| {
                nodes
                    .iter()
                    .filter_map(|n| n.get("id").and_then(|id| id.as_str()))
                    .collect()
            })
            .unwrap_or_default();

        for (i, conn) in connections.iter().enumerate() {
            if let Some(source) = conn.get("source").and_then(|s| s.as_str()) {
                if !node_ids.contains(&source) {
                    errors.push(format!(
                        "Connection {}: Source '{}' not found in nodes",
                        i, source
                    ));
                }
            }
            if let Some(dest) = conn
                .get("destination")
                .or(conn.get("dest"))
                .and_then(|d| d.as_str())
            {
                if !node_ids.contains(&dest) {
                    errors.push(format!(
                        "Connection {}: Destination '{}' not found in nodes",
                        i, dest
                    ));
                }
            }
        }
    }

    Ok(errors)
}

/// Generate TypeScript type definitions for all node configs
///
/// Returns a complete `.d.ts` file with interfaces for all registered node types.
///
/// # Example
///
/// ```javascript
/// const fs = require('fs');
/// const { generateTypescript } = require('@matbee/remotemedia-native');
///
/// // Generate and save TypeScript definitions
/// const typescriptDefs = generateTypescript();
/// fs.writeFileSync('node-configs.d.ts', typescriptDefs);
/// ```
#[napi]
pub fn generate_typescript() -> String {
    let registry = get_registry();
    remotemedia_core::nodes::schema::generate_typescript(registry)
}

/// Generate TypeScript type definitions including auto-registered configs
///
/// This version merges the builtin schemas with any configs registered
/// via `#[derive(NodeConfig)]` and inventory.
///
/// # Example
///
/// ```javascript
/// const { generateTypescriptWithRegistered } = require('@matbee/remotemedia-native');
/// const fullDefs = generateTypescriptWithRegistered();
/// ```
#[napi]
pub fn generate_typescript_with_registered() -> String {
    // Start with builtin schemas
    let mut registry = create_builtin_schema_registry();

    // Merge in auto-registered configs
    let registered = remotemedia_core::nodes::schema::collect_registered_configs();
    for schema in registered.iter() {
        registry.register(schema.clone());
    }

    remotemedia_core::nodes::schema::generate_typescript(&registry)
}

// Note: Tests for this module should be run via Node.js Jest tests
// since they require NAPI bindings to be loaded in a Node.js environment.
// See transports/ffi/nodejs/__tests__/schema.test.ts
