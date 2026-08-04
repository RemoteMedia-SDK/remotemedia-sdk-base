//! Remote pipeline execution node
//!
//! This module implements a node that delegates execution to remote pipeline servers.
//! It acts as a proxy, forwarding RuntimeData to remote servers and returning results.
//!
//! # Use Cases
//!
//! - **GPU Offloading**: Run STT/TTS on GPU servers while keeping VAD/preprocessing local
//! - **Load Distribution**: Route to nearest/healthiest server from geographic pool
//! - **Microservices**: Compose pipelines from independently-deployed services
//!
//! # Example Configuration
//!
//! ```json
//! {
//!   "id": "remote_tts",
//!   "node_type": "RemotePipelineNode",
//!   "params": {
//!     "transport": "grpc",
//!     "endpoint": "tts-server.example.com:50051",
//!     "manifest": {
//!       "version": "v1",
//!       "nodes": [{
//!         "id": "kokoro",
//!         "node_type": "KokoroTTS",
//!         "params": {"voice": "af_bella"}
//!       }]
//!     },
//!     "timeout_ms": 10000,
//!     "retry": {
//!       "max_retries": 3,
//!       "backoff_ms": 1000
//!     }
//!   }
//! }
//! ```

use crate::manifest::Manifest;
use crate::{Error, Result};
use remotemedia_traits::runtime_context::InitializeContextRead;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Source of the remote pipeline manifest
///
/// Specifies where to load the pipeline configuration that will be executed remotely.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ManifestSource {
    /// Inline manifest JSON
    ///
    /// The complete manifest is embedded in the node configuration.
    ///
    /// # Example
    ///
    /// ```json
    /// {
    ///   "manifest": {
    ///     "version": "v1",
    ///     "nodes": [{"id": "tts", "node_type": "KokoroTTS", "params": {}}]
    ///   }
    /// }
    /// ```
    Inline {
        /// Manifest JSON object
        manifest: serde_json::Value,
    },

    /// Remote manifest URL
    ///
    /// Fetch the manifest from an HTTP(S) URL.
    ///
    /// # Example
    ///
    /// ```json
    /// {
    ///   "manifest_url": "https://api.example.com/pipelines/tts-v2.json",
    ///   "auth_header": "Bearer ${API_TOKEN}"
    /// }
    /// ```
    Url {
        /// URL to fetch manifest from
        manifest_url: String,
        /// Optional authentication header value (supports env var substitution)
        #[serde(skip_serializing_if = "Option::is_none")]
        auth_header: Option<String>,
    },

    /// Predefined pipeline name
    ///
    /// Reference a pipeline by name that the remote server knows about.
    /// The server maintains a registry of named pipelines.
    ///
    /// # Example
    ///
    /// ```json
    /// {
    ///   "pipeline_name": "whisper-large-v3",
    ///   "manifest_endpoint": "https://api.example.com",
    ///   "auth_header": "Bearer ${API_TOKEN}"
    /// }
    /// ```
    Name {
        /// Pipeline name known to remote server
        pipeline_name: String,
        /// Optional endpoint to fetch manifest from (e.g., "https://api.example.com")
        /// If specified, fetches from: {manifest_endpoint}/manifests/{pipeline_name}
        /// If not specified, creates minimal manifest for server-side resolution
        #[serde(skip_serializing_if = "Option::is_none")]
        manifest_endpoint: Option<String>,
        /// Optional authentication header value (supports env var substitution)
        #[serde(skip_serializing_if = "Option::is_none")]
        auth_header: Option<String>,
    },
}

/// Configuration for remote pipeline execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemotePipelineConfig {
    /// Transport protocol to use
    pub transport: String,

    /// Primary endpoint URL
    ///
    /// For single endpoint: "localhost:50051"
    /// For load balancing: first endpoint in `endpoints` array
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    /// Multiple endpoints for load balancing
    ///
    /// If specified, takes precedence over `endpoint` field
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoints: Option<Vec<String>>,

    /// Manifest source (inline, URL, or predefined name)
    #[serde(flatten)]
    pub manifest_source: ManifestSource,

    /// Execution timeout in milliseconds
    ///
    /// Default: 30000ms (30 seconds)
    #[serde(default = "default_timeout")]
    pub timeout_ms: u64,

    /// Retry configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryConfig>,

    /// Circuit breaker configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub circuit_breaker: Option<CircuitBreakerConfig>,

    /// Load balancing strategy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_balance_strategy: Option<LoadBalanceStrategy>,

    /// Health check configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health_check: Option<HealthCheckConfig>,

    /// Authentication token
    ///
    /// Supports environment variable substitution: "${API_TOKEN}"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,

    /// Additional metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
}

fn default_timeout() -> u64 {
    30000
}

/// Retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    ///
    /// Default: 3
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Initial backoff duration in milliseconds
    ///
    /// Backoff increases exponentially: backoff_ms * 2^attempt
    /// Default: 1000ms (1 second)
    #[serde(default = "default_backoff_ms")]
    pub backoff_ms: u64,
}

fn default_max_retries() -> u32 {
    3
}

fn default_backoff_ms() -> u64 {
    1000
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            backoff_ms: default_backoff_ms(),
        }
    }
}

/// Circuit breaker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening circuit
    ///
    /// Default: 5
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,

    /// Number of consecutive successes to close circuit from half-open
    ///
    /// Default: 2
    #[serde(default = "default_success_threshold")]
    pub success_threshold: u32,

    /// Duration to wait in open state before transitioning to half-open (milliseconds)
    ///
    /// Default: 60000ms (60 seconds)
    #[serde(default = "default_reset_timeout")]
    pub reset_timeout_ms: u64,
}

fn default_failure_threshold() -> u32 {
    5
}

fn default_success_threshold() -> u32 {
    2
}

fn default_reset_timeout() -> u64 {
    60000
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: default_failure_threshold(),
            success_threshold: default_success_threshold(),
            reset_timeout_ms: default_reset_timeout(),
        }
    }
}

/// Load balancing strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadBalanceStrategy {
    /// Round-robin selection
    RoundRobin,
    /// Select endpoint with fewest active connections
    LeastConnections,
    /// Random selection
    Random,
}

impl Default for LoadBalanceStrategy {
    fn default() -> Self {
        LoadBalanceStrategy::RoundRobin
    }
}

/// Health check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    /// Health check interval in milliseconds
    ///
    /// Default: 5000ms (5 seconds)
    #[serde(default = "default_health_interval")]
    pub interval_ms: u64,

    /// Timeout for individual health checks in milliseconds
    ///
    /// Default: 2000ms (2 seconds)
    #[serde(default = "default_health_timeout")]
    pub timeout_ms: u64,
}

fn default_health_interval() -> u64 {
    5000
}

fn default_health_timeout() -> u64 {
    2000
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            interval_ms: default_health_interval(),
            timeout_ms: default_health_timeout(),
        }
    }
}

/// Runtime execution context for remote pipeline execution
///
/// Tracks state during a remote execution attempt
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    /// Number of attempts made so far
    pub attempt_count: u32,

    /// Total elapsed time in milliseconds
    pub elapsed_ms: u64,

    /// Selected endpoint for this execution
    pub selected_endpoint: String,

    /// Authentication context (resolved token)
    pub auth_context: Option<String>,

    /// Start time (for timing)
    pub start_time: std::time::Instant,
}

impl ExecutionContext {
    /// Create new execution context
    pub fn new(endpoint: String, auth_token: Option<String>) -> Self {
        Self {
            attempt_count: 0,
            elapsed_ms: 0,
            selected_endpoint: endpoint,
            auth_context: auth_token,
            start_time: std::time::Instant::now(),
        }
    }

    /// Update elapsed time
    pub fn update_elapsed(&mut self) {
        self.elapsed_ms = self.start_time.elapsed().as_millis() as u64;
    }

    /// Increment attempt count
    pub fn increment_attempt(&mut self) {
        self.attempt_count += 1;
    }
}

//
// Manifest loading utilities (T018-T020)
//

/// Load manifest from configured source
///
/// Supports three loading modes:
/// - Inline: Use embedded JSON directly
/// - URL: Fetch from HTTP(S) endpoint with optional auth
/// - Name: Fetch from {manifest_endpoint}/manifests/{name} or create minimal manifest
pub async fn load_manifest_from_source(source: &ManifestSource) -> Result<Manifest> {
    match source {
        ManifestSource::Inline { manifest } => {
            // Parse inline JSON value directly to Manifest
            serde_json::from_value(manifest.clone()).map_err(|e| {
                Error::InvalidManifest(format!("Failed to parse inline manifest: {}", e))
            })
        }

        ManifestSource::Url {
            manifest_url,
            auth_header,
        } => {
            // Build request with optional auth header
            let client = reqwest::Client::new();
            let mut request = client.get(manifest_url);

            // Add auth header if present (with env var substitution)
            if let Some(auth) = auth_header {
                let resolved_auth = substitute_env_vars(auth)?;
                request = request.header("authorization", resolved_auth);
            }

            // Fetch from URL
            let response = request
                .send()
                .await
                .map_err(|e| Error::ManifestFetchFailed {
                    url: manifest_url.clone(),
                    reason: format!("HTTP request failed: {}", e),
                })?;

            if !response.status().is_success() {
                return Err(Error::ManifestFetchFailed {
                    url: manifest_url.clone(),
                    reason: format!(
                        "HTTP {} {}",
                        response.status().as_u16(),
                        response.status().canonical_reason().unwrap_or("Unknown")
                    ),
                });
            }

            let json_str = response
                .text()
                .await
                .map_err(|e| Error::ManifestFetchFailed {
                    url: manifest_url.clone(),
                    reason: format!("Failed to read response body: {}", e),
                })?;

            serde_json::from_str(&json_str).map_err(|e| Error::ManifestFetchFailed {
                url: manifest_url.clone(),
                reason: format!("Failed to parse manifest JSON: {}", e),
            })
        }

        ManifestSource::Name {
            pipeline_name,
            manifest_endpoint,
            auth_header,
        } => {
            // If manifest_endpoint is specified, fetch from /manifests/{name}
            if let Some(endpoint) = manifest_endpoint {
                let url = format!(
                    "{}/manifests/{}",
                    endpoint.trim_end_matches('/'),
                    pipeline_name
                );

                // Build request with optional auth header
                let client = reqwest::Client::new();
                let mut request = client.get(&url);

                // Add auth header if present (with env var substitution)
                if let Some(auth) = auth_header {
                    let resolved_auth = substitute_env_vars(auth)?;
                    request = request.header("authorization", resolved_auth);
                }

                // Fetch manifest
                let response = request
                    .send()
                    .await
                    .map_err(|e| Error::ManifestFetchFailed {
                        url: url.clone(),
                        reason: format!("HTTP request failed: {}", e),
                    })?;

                if !response.status().is_success() {
                    return Err(Error::ManifestFetchFailed {
                        url: url.clone(),
                        reason: format!(
                            "HTTP {} {}",
                            response.status().as_u16(),
                            response.status().canonical_reason().unwrap_or("Unknown")
                        ),
                    });
                }

                let json_str = response
                    .text()
                    .await
                    .map_err(|e| Error::ManifestFetchFailed {
                        url: url.clone(),
                        reason: format!("Failed to read response body: {}", e),
                    })?;

                serde_json::from_str(&json_str).map_err(|e| Error::ManifestFetchFailed {
                    url: url.clone(),
                    reason: format!("Failed to parse manifest JSON: {}", e),
                })
            } else {
                // Create minimal manifest with pipeline name
                // Remote server will resolve the actual pipeline
                let json = format!(
                    r#"{{"version":"v1","metadata":{{"name":"{}"}}}}"#,
                    pipeline_name
                );

                serde_json::from_str(&json).map_err(|e| {
                    Error::InvalidManifest(format!(
                        "Failed to create manifest for pipeline '{}': {}",
                        pipeline_name, e
                    ))
                })
            }
        }
    }
}

/// Substitute environment variables in auth tokens
///
/// Supports syntax: "${VAR_NAME}" or "$VAR_NAME"
///
/// # Example
///
/// ```
/// use remotemedia_core::nodes::remote_pipeline::substitute_env_vars;
///
/// // When value has no env vars, returns as-is
/// let simple = substitute_env_vars("simple_token").unwrap();
/// assert_eq!(simple, "simple_token");
/// ```
pub fn substitute_env_vars(value: &str) -> Result<String> {
    if !value.contains('$') {
        // No substitution needed
        return Ok(value.to_string());
    }

    let mut result = value.to_string();

    // Handle ${VAR} syntax
    let re = regex::Regex::new(r"\$\{([A-Z_][A-Z0-9_]*)\}").unwrap();
    for cap in re.captures_iter(value) {
        let var_name = &cap[1];
        let var_value = std::env::var(var_name).map_err(|_| {
            Error::ConfigError(format!(
                "Environment variable '{}' not found (referenced in auth_token)",
                var_name
            ))
        })?;
        result = result.replace(&format!("${{{}}}", var_name), &var_value);
    }

    // Handle $VAR syntax (without braces)
    let re = regex::Regex::new(r"\$([A-Z_][A-Z0-9_]*)").unwrap();
    for cap in re.captures_iter(&result.clone()) {
        let var_name = &cap[1];
        if !value.contains(&format!("${{{}}}", var_name)) {
            // Only substitute if not already done with braces
            let var_value = std::env::var(var_name).map_err(|_| {
                Error::ConfigError(format!(
                    "Environment variable '{}' not found (referenced in auth_token)",
                    var_name
                ))
            })?;
            result = result.replace(&format!("${}", var_name), &var_value);
        }
    }

    Ok(result)
}

/// Validate manifest compatibility
///
/// Checks:
/// - Version is supported ("v1")
/// - Node types are reasonable (not empty, valid identifiers)
/// - No obvious circular references in manifest itself
///
/// Note: Full circular dependency detection happens at execution time
/// when traversing node connections.
pub fn validate_manifest(manifest: &Manifest) -> Result<()> {
    // Check version
    if manifest.version != "v1" {
        return Err(Error::InvalidManifest(format!(
            "Unsupported manifest version: '{}' (expected 'v1')",
            manifest.version
        )));
    }

    // Check that nodes array is not empty
    if manifest.nodes.is_empty() {
        return Err(Error::InvalidManifest(
            "Manifest must contain at least one node".to_string(),
        ));
    }

    // Validate node IDs (no duplicates, valid identifiers)
    let mut seen_ids = std::collections::HashSet::new();
    for node in &manifest.nodes {
        if !seen_ids.insert(&node.id) {
            return Err(Error::InvalidManifest(format!(
                "Duplicate node ID: '{}'",
                node.id
            )));
        }

        if node.id.is_empty() {
            return Err(Error::InvalidManifest(
                "Node ID cannot be empty".to_string(),
            ));
        }

        if node.node_type.is_empty() {
            return Err(Error::InvalidManifest(format!(
                "Node '{}' has empty node_type",
                node.id
            )));
        }
    }

    // Validate connections reference existing nodes
    for conn in &manifest.connections {
        if !seen_ids.contains(&conn.from) {
            return Err(Error::InvalidManifest(format!(
                "Connection references unknown source node: '{}'",
                conn.from
            )));
        }
        if !seen_ids.contains(&conn.to) {
            return Err(Error::InvalidManifest(format!(
                "Connection references unknown target node: '{}'",
                conn.to
            )));
        }
    }

    Ok(())
}

//
// RemotePipelineNode Implementation (T023-T036)
//

/// Remote pipeline execution node
///
/// This node delegates processing to a remote gRPC server running a pipeline.
/// It acts as a client, sending RuntimeData to the server and returning results.
///
/// # Configuration
///
/// The node is configured via `params` in the manifest, which must deserialize to
/// `RemotePipelineConfig`. See `RemotePipelineConfig` documentation for details.
///
/// # Example
///
/// ```json
/// {
///   "id": "remote_tts",
///   "node_type": "RemotePipelineNode",
///   "params": {
///     "transport": "grpc",
///     "endpoint": "localhost:50051",
///     "manifest": {
///       "version": "v1",
///       "nodes": [...]
///     },
///     "timeout_ms": 10000
///   }
/// }
/// ```
pub struct RemotePipelineNode {
    /// Node ID from manifest
    pub node_id: String,

    /// Parsed configuration
    pub config: RemotePipelineConfig,

    /// Resolved manifest to execute remotely
    pub manifest: Option<Manifest>,
}

impl RemotePipelineNode {
    /// Create new RemotePipelineNode from configuration
    ///
    /// # Arguments
    ///
    /// * `node_id` - Unique node identifier
    /// * `params` - Configuration as serde_json::Value (will be deserialized to RemotePipelineConfig)
    ///
    /// # Returns
    ///
    /// * `Ok(RemotePipelineNode)` - Successfully created node
    /// * `Err(Error)` - Failed to parse configuration
    ///
    /// # Example
    ///
    /// ```
    /// use remotemedia_core::nodes::remote_pipeline::RemotePipelineNode;
    ///
    /// let params = serde_json::json!({
    ///     "transport": "grpc",
    ///     "endpoint": "localhost:50051",
    ///     "manifest": { "version": "v1", "nodes": [] }
    /// });
    /// let node = RemotePipelineNode::new("remote1".into(), params).unwrap();
    /// ```
    pub fn new(node_id: String, params: serde_json::Value) -> crate::Result<Self> {
        // Parse configuration
        let config: RemotePipelineConfig = serde_json::from_value(params).map_err(|e| {
            crate::Error::ConfigError(format!("Failed to parse RemotePipelineConfig: {}", e))
        })?;

        // Validate transport type
        if config.transport != "grpc" && config.transport != "http" && config.transport != "webrtc"
        {
            return Err(crate::Error::ConfigError(format!(
                "Unsupported transport type: '{}' (supported: grpc, http, webrtc)",
                config.transport
            )));
        }

        // Ensure at least one endpoint is specified
        if config.endpoint.is_none() && config.endpoints.is_none() {
            return Err(crate::Error::ConfigError(
                "At least one endpoint must be specified (via 'endpoint' or 'endpoints')".into(),
            ));
        }

        // Resolve auth token environment variables if present
        let mut config = config;
        if let Some(ref token) = config.auth_token {
            config.auth_token = Some(substitute_env_vars(token)?);
        }

        Ok(Self {
            node_id,
            config,
            manifest: None,
        })
    }

    /// Get the primary endpoint from configuration
    ///
    /// Returns the first endpoint from `endpoints` array, or the single `endpoint` field.
    fn get_primary_endpoint(&self) -> String {
        if let Some(ref endpoints) = self.config.endpoints {
            endpoints.first().cloned().unwrap_or_default()
        } else {
            self.config.endpoint.clone().unwrap_or_default()
        }
    }

    /// Initialize the node (load manifest from source)
    ///
    /// This is called once before any processing.
    pub async fn initialize(&mut self) -> crate::Result<()> {
        // Load manifest from configured source
        let manifest = load_manifest_from_source(&self.config.manifest_source).await?;

        // Validate the manifest
        validate_manifest(&manifest)?;

        self.manifest = Some(manifest);

        tracing::info!(
            "RemotePipelineNode '{}' initialized with transport={}, endpoint={}",
            self.node_id,
            self.config.transport,
            self.get_primary_endpoint()
        );

        Ok(())
    }

    /// Get or create the transport client using the plugin registry
    #[cfg(feature = "grpc-client")]
    async fn get_client(&self) -> crate::Result<Box<dyn crate::transport::client::PipelineClient>> {
        use crate::transport::plugin_registry::global_registry;
        use crate::transport::ClientConfig;

        // Look up transport plugin by name
        let transport_name = &self.config.transport;
        let plugin = global_registry().get(transport_name).ok_or_else(|| {
            let available = global_registry().list();
            crate::Error::ConfigError(format!(
                "Transport '{}' not found. Available transports: {}. \
                     Make sure to register the transport plugin at application startup.",
                transport_name,
                available.join(", ")
            ))
        })?;

        // Build ClientConfig from manifest params
        // We need to reconstruct params as a JSON object with endpoint and optional fields
        let mut params = serde_json::Map::new();
        params.insert(
            "endpoint".to_string(),
            serde_json::Value::String(self.get_primary_endpoint()),
        );

        if let Some(ref token) = self.config.auth_token {
            params.insert(
                "auth_token".to_string(),
                serde_json::Value::String(token.clone()),
            );
        }

        params.insert(
            "timeout_ms".to_string(),
            serde_json::Value::Number(self.config.timeout_ms.into()),
        );

        let params_value = serde_json::Value::Object(params);
        let client_config = ClientConfig::from_manifest_params(&params_value)?;

        // Validate transport-specific config
        if let Some(extra) = &client_config.extra_config {
            plugin.validate_config(extra)?;
        }

        // Create client via plugin
        plugin.create_client(&client_config).await
    }

    /// Execute with retry logic
    #[cfg_attr(not(feature = "grpc-client"), allow(unused_variables))]
    async fn execute_with_retry(
        &self,
        manifest: std::sync::Arc<Manifest>,
        input: crate::transport::TransportData,
    ) -> crate::Result<crate::transport::TransportData> {
        let retry_config = self.config.retry.as_ref();
        let max_retries = retry_config.map(|r| r.max_retries).unwrap_or(0);
        let backoff_ms = retry_config.map(|r| r.backoff_ms).unwrap_or(1000);

        #[allow(unused_mut)]
        let mut last_error = None;
        let mut ctx =
            ExecutionContext::new(self.get_primary_endpoint(), self.config.auth_token.clone());

        for attempt in 0..=max_retries {
            ctx.increment_attempt();

            // Apply exponential backoff (skip on first attempt)
            if attempt > 0 {
                let delay = backoff_ms * 2u64.pow(attempt - 1);
                tracing::debug!(
                    "RemotePipelineNode '{}': Retry attempt {} after {}ms",
                    self.node_id,
                    attempt,
                    delay
                );
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }

            // Get client
            #[cfg(feature = "grpc-client")]
            let client = match self.get_client().await {
                Ok(c) => c,
                Err(e) => {
                    last_error = Some(e);
                    continue;
                }
            };

            #[cfg(not(feature = "grpc-client"))]
            {
                return Err(crate::Error::ConfigError(
                    "gRPC client not enabled - compile with 'grpc-client' feature".into(),
                ));
            }

            #[cfg(feature = "grpc-client")]
            {
                // Execute with timeout
                let timeout_ms = self.config.timeout_ms;
                let timeout_duration = std::time::Duration::from_millis(timeout_ms);

                let result = tokio::time::timeout(
                    timeout_duration,
                    client.execute_unary(manifest.clone(), input.clone(), &[], None),
                )
                .await;

                match result {
                    Ok(Ok(output)) => {
                        ctx.update_elapsed();
                        tracing::info!(
                            "RemotePipelineNode '{}': Success after {} attempts, {}ms elapsed",
                            self.node_id,
                            ctx.attempt_count,
                            ctx.elapsed_ms
                        );
                        return Ok(output);
                    }
                    Ok(Err(e)) => {
                        last_error = Some(e);
                        tracing::warn!(
                            "RemotePipelineNode '{}': Attempt {} failed: {}",
                            self.node_id,
                            attempt + 1,
                            last_error.as_ref().unwrap()
                        );
                    }
                    Err(_elapsed) => {
                        last_error = Some(crate::Error::RemoteTimeout {
                            timeout_ms,
                            context: format!(
                                "Node '{}' timed out after {}ms",
                                self.node_id, timeout_ms
                            ),
                        });
                        tracing::warn!(
                            "RemotePipelineNode '{}': Attempt {} timed out after {}ms",
                            self.node_id,
                            attempt + 1,
                            timeout_ms
                        );
                    }
                }
            }
        }

        // All retries exhausted
        ctx.update_elapsed();
        Err(last_error.unwrap_or_else(|| {
            crate::Error::RemoteExecutionFailed(format!(
                "Node '{}' failed after {} attempts",
                self.node_id, ctx.attempt_count
            ))
        }))
    }
}

/// Factory for creating RemotePipelineNode instances
pub struct RemotePipelineNodeFactory;

impl crate::nodes::StreamingNodeFactory for RemotePipelineNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &serde_json::Value,
        _session_id: Option<String>,
    ) -> crate::Result<Box<dyn crate::nodes::StreamingNode>> {
        // Create node
        let mut node = RemotePipelineNode::new(node_id, params.clone())?;

        // Note: We can't call async initialize() here in sync context
        // The node will be initialized later by the executor or before first use
        // For now, we load the manifest synchronously using blocking
        if let ManifestSource::Inline { manifest } = &node.config.manifest_source {
            // For inline manifests, we can load synchronously
            let manifest_obj: Manifest = serde_json::from_value(manifest.clone()).map_err(|e| {
                crate::Error::InvalidManifest(format!("Failed to parse inline manifest: {}", e))
            })?;
            validate_manifest(&manifest_obj)?;
            node.manifest = Some(manifest_obj);
        }
        // For URL and Name sources, manifest will be loaded on first process() call
        // or during async initialize()

        Ok(Box::new(crate::nodes::AsyncNodeWrapper(
            std::sync::Arc::new(node),
        )))
    }

    fn node_type(&self) -> &str {
        "RemotePipelineNode"
    }

    fn is_python_node(&self) -> bool {
        false
    }

    fn is_multi_output_streaming(&self) -> bool {
        false // Remote nodes use unary execution for now
    }

    fn schema(&self) -> Option<crate::nodes::schema::NodeSchema> {
        use crate::nodes::schema::{NodeSchema, RuntimeDataType};
        Some(
            NodeSchema::new("RemotePipelineNode")
                .description(
                    "Proxies requests to a remote pipeline over gRPC, HTTP, or in-process Python",
                )
                .category("network")
                .accepts([
                    RuntimeDataType::Audio,
                    RuntimeDataType::Text,
                    RuntimeDataType::Json,
                ])
                .produces([
                    RuntimeDataType::Audio,
                    RuntimeDataType::Text,
                    RuntimeDataType::Json,
                ]),
        )
    }
}

// Implement AsyncStreamingNode for RemotePipelineNode
#[async_trait::async_trait]
impl crate::nodes::AsyncStreamingNode for RemotePipelineNode {
    fn node_type(&self) -> &str {
        "RemotePipelineNode"
    }

    async fn initialize(&self, _ctx: &dyn InitializeContextRead) -> crate::Result<()> {
        // The initialization is done in RemotePipelineNode::initialize which is &mut
        // For now, we'll just log that init was called
        tracing::debug!(
            "RemotePipelineNode '{}' AsyncStreamingNode::initialize called",
            self.node_id
        );
        Ok(())
    }

    async fn process(
        &self,
        data: crate::data::RuntimeData,
    ) -> crate::Result<crate::data::RuntimeData> {
        // Get the manifest
        let manifest = self.manifest.as_ref().ok_or_else(|| {
            crate::Error::Execution(format!(
                "RemotePipelineNode '{}' not initialized - manifest is None",
                self.node_id
            ))
        })?;

        // Convert RuntimeData to TransportData
        let input = crate::transport::TransportData::new(data);

        // Execute with retry and timeout
        let output = self
            .execute_with_retry(std::sync::Arc::new(manifest.clone()), input)
            .await?;

        // Extract RuntimeData from TransportData
        Ok(output.data)
    }

    async fn process_streaming<F>(
        &self,
        data: crate::data::RuntimeData,
        _session_id: Option<String>,
        mut callback: F,
    ) -> crate::Result<usize>
    where
        F: FnMut(crate::data::RuntimeData) -> crate::Result<()> + Send,
    {
        // For unary execution, just call process and invoke callback once
        let output = self.process(data).await?;
        callback(output)?;
        Ok(1)
    }

    async fn process_control_message(
        &self,
        message: crate::data::RuntimeData,
        _session_id: Option<String>,
    ) -> crate::Result<bool> {
        // Forward control messages to remote transport
        // The transport layer will handle serialization (JSON for gRPC/WebRTC/HTTP)

        match &message {
            crate::data::RuntimeData::ControlMessage { .. } => {
                tracing::debug!(
                    "RemotePipelineNode '{}' forwarding control message to remote transport",
                    self.node_id
                );

                // Convert to TransportData and send via remote transport
                // Note: For now, control messages are sent as data (will be handled by transport)
                // TODO: Once T042-T045 are complete, this will use proper transport control message APIs
                let transport_data = crate::transport::TransportData::new(message);

                // Get manifest
                let manifest = self.manifest.as_ref().ok_or_else(|| {
                    crate::Error::Execution(format!(
                        "RemotePipelineNode '{}' not initialized - manifest is None",
                        self.node_id
                    ))
                })?;

                // Execute via transport (control messages flow through the same pipeline)
                let _result = self
                    .execute_with_retry(std::sync::Arc::new(manifest.clone()), transport_data)
                    .await?;

                Ok(true) // Message was handled (forwarded)
            }
            _ => Ok(false), // Not a control message
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_source_inline_deserialization() {
        let json = r#"{"manifest": {"version": "v1", "nodes": []}}"#;
        let source: ManifestSource = serde_json::from_str(json).unwrap();

        match source {
            ManifestSource::Inline { .. } => (),
            _ => panic!("Expected Inline variant"),
        }
    }

    #[test]
    fn test_manifest_source_url_deserialization() {
        let json = r#"{"manifest_url": "https://example.com/pipeline.json"}"#;
        let source: ManifestSource = serde_json::from_str(json).unwrap();

        match source {
            ManifestSource::Url { manifest_url, .. } => {
                assert_eq!(manifest_url, "https://example.com/pipeline.json");
            }
            _ => panic!("Expected Url variant"),
        }
    }

    #[test]
    fn test_manifest_source_name_deserialization() {
        let json = r#"{"pipeline_name": "whisper-large-v3"}"#;
        let source: ManifestSource = serde_json::from_str(json).unwrap();

        match source {
            ManifestSource::Name { pipeline_name, .. } => {
                assert_eq!(pipeline_name, "whisper-large-v3");
            }
            _ => panic!("Expected Name variant"),
        }
    }

    #[test]
    fn test_env_var_substitution() {
        std::env::set_var("TEST_TOKEN", "secret123");

        let result = substitute_env_vars("Bearer ${TEST_TOKEN}").unwrap();
        assert_eq!(result, "Bearer secret123");

        let result2 = substitute_env_vars("Bearer $TEST_TOKEN").unwrap();
        assert_eq!(result2, "Bearer secret123");

        std::env::remove_var("TEST_TOKEN");
    }

    #[test]
    fn test_env_var_missing_error() {
        let result = substitute_env_vars("${NONEXISTENT_VAR}");
        assert!(result.is_err());
    }
}

//
// Manifest caching (T085)
//

/// Cached manifest entry with TTL
#[derive(Debug, Clone)]
struct CachedManifest {
    /// The cached manifest
    manifest: Manifest,
    /// When this entry was created
    cached_at: Instant,
}

/// In-memory TTL cache for manifests
///
/// This cache prevents repeated fetches of the same manifest from remote URLs.
/// Entries expire after a configurable TTL (default 60 seconds).
///
/// # Thread Safety
///
/// The cache is wrapped in Arc<RwLock<>> for concurrent access from multiple nodes.
pub struct ManifestCache {
    /// Cache storage (key = manifest URL or name, value = cached manifest with timestamp)
    cache: RwLock<HashMap<String, CachedManifest>>,
    /// Time-to-live for cache entries
    ttl: Duration,
}

impl ManifestCache {
    /// Create a new manifest cache with default TTL (60 seconds)
    pub fn new() -> Self {
        Self::with_ttl(Duration::from_secs(60))
    }

    /// Create a new manifest cache with custom TTL
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            ttl,
        }
    }

    /// Get manifest from cache if present and not expired
    pub fn get(&self, key: &str) -> Option<Manifest> {
        let cache = self.cache.read().ok()?;
        let entry = cache.get(key)?;

        // Check if entry has expired
        if entry.cached_at.elapsed() > self.ttl {
            drop(cache); // Release read lock
            self.remove(key); // Clean up expired entry
            return None;
        }

        Some(entry.manifest.clone())
    }

    /// Store manifest in cache
    pub fn put(&self, key: String, manifest: Manifest) {
        let entry = CachedManifest {
            manifest,
            cached_at: Instant::now(),
        };

        if let Ok(mut cache) = self.cache.write() {
            cache.insert(key, entry);
        }
    }

    /// Remove manifest from cache
    pub fn remove(&self, key: &str) {
        if let Ok(mut cache) = self.cache.write() {
            cache.remove(key);
        }
    }

    /// Clear all entries from cache
    pub fn clear(&self) {
        if let Ok(mut cache) = self.cache.write() {
            cache.clear();
        }
    }

    /// Remove expired entries from cache
    pub fn cleanup_expired(&self) {
        if let Ok(mut cache) = self.cache.write() {
            let now = Instant::now();
            cache.retain(|_, entry| now.duration_since(entry.cached_at) <= self.ttl);
        }
    }
}

impl Default for ManifestCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Global manifest cache instance
///
/// This is a singleton cache shared across all RemotePipelineNode instances.
static MANIFEST_CACHE: std::sync::OnceLock<Arc<ManifestCache>> = std::sync::OnceLock::new();

/// Get the global manifest cache
pub fn manifest_cache() -> Arc<ManifestCache> {
    MANIFEST_CACHE
        .get_or_init(|| Arc::new(ManifestCache::new()))
        .clone()
}

/// Load manifest with caching support
///
/// This wraps `load_manifest_from_source` and adds caching for URL and Name sources.
/// Inline manifests are not cached (they're already embedded in configuration).
pub async fn load_manifest_from_source_cached(source: &ManifestSource) -> Result<Manifest> {
    let cache = manifest_cache();

    // Generate cache key based on source
    let cache_key = match source {
        ManifestSource::Inline { .. } => {
            // Don't cache inline manifests
            return load_manifest_from_source(source).await;
        }
        ManifestSource::Url { manifest_url, .. } => manifest_url.clone(),
        ManifestSource::Name {
            pipeline_name,
            manifest_endpoint,
            ..
        } => {
            if let Some(endpoint) = manifest_endpoint {
                format!("{}/manifests/{}", endpoint, pipeline_name)
            } else {
                format!("name:{}", pipeline_name)
            }
        }
    };

    // Check cache first
    if let Some(cached) = cache.get(&cache_key) {
        tracing::debug!("Manifest cache hit for: {}", cache_key);
        return Ok(cached);
    }

    // Cache miss - load from source
    tracing::debug!("Manifest cache miss for: {}", cache_key);
    let manifest = load_manifest_from_source(source).await?;

    // Store in cache
    cache.put(cache_key, manifest.clone());

    Ok(manifest)
}

//
// Circular dependency detection (T086-T089)
//

use std::collections::HashSet;

/// Maximum recursion depth for manifest loading (prevents infinite loops)
const MAX_RECURSION_DEPTH: usize = 10;

/// Detect circular dependencies in remote pipeline references
///
/// This performs a depth-first search through manifest references, tracking visited
/// manifests to detect cycles. It also enforces a maximum recursion depth limit.
///
/// # Arguments
///
/// * `manifest` - The manifest to check
/// * `visited` - Set of already-visited manifest identifiers
/// * `path` - Current path of manifests (for error reporting)
/// * `depth` - Current recursion depth
///
/// # Returns
///
/// * `Ok(())` - No circular dependencies detected
/// * `Err(Error::CircularDependency)` - Circular dependency detected
///
/// # Example
///
/// ```
/// use remotemedia_core::nodes::remote_pipeline::detect_circular_dependencies;
/// use remotemedia_core::manifest::{Manifest, ManifestMetadata};
/// use std::collections::HashSet;
///
/// let manifest = Manifest {
///     version: "v1".into(),
///     metadata: ManifestMetadata { name: "test".into(), ..Default::default() },
///     nodes: vec![],
///     connections: vec![],
///     python_env: None,
///     plugins: vec![],
/// };
/// let mut visited = HashSet::new();
/// let mut path = Vec::new();
/// detect_circular_dependencies(&manifest, &mut visited, &mut path, 0).unwrap();
/// ```
pub fn detect_circular_dependencies(
    manifest: &Manifest,
    visited: &mut HashSet<String>,
    path: &mut Vec<String>,
    depth: usize,
) -> Result<()> {
    // Check recursion depth limit
    if depth > MAX_RECURSION_DEPTH {
        return Err(Error::CircularDependency {
            chain: path.clone(),
            reason: format!(
                "Maximum recursion depth ({}) exceeded - possible circular dependency",
                MAX_RECURSION_DEPTH
            ),
        });
    }

    // Generate manifest identifier (use metadata.name or hash of manifest)
    let manifest_id = manifest.metadata.name.clone();

    // Check if we've already visited this manifest in current path
    if path.contains(&manifest_id) {
        // Circular dependency detected!
        path.push(manifest_id.clone());
        return Err(Error::CircularDependency {
            chain: path.clone(),
            reason: format!(
                "Circular dependency detected: manifest '{}' references itself",
                manifest_id
            ),
        });
    }

    // Add to visited set (globally)
    if visited.contains(&manifest_id) {
        // Already processed this manifest in a different branch - safe to skip
        return Ok(());
    }

    visited.insert(manifest_id.clone());
    path.push(manifest_id.clone());

    // Check all nodes in manifest for RemotePipelineNode references
    for node in &manifest.nodes {
        if node.node_type == "RemotePipelineNode" {
            // Try to parse the params as RemotePipelineConfig
            if let Ok(config) = serde_json::from_value::<RemotePipelineConfig>(node.params.clone())
            {
                // Check the manifest source
                match &config.manifest_source {
                    ManifestSource::Inline {
                        manifest: nested_manifest,
                    } => {
                        // Recursively check nested manifest
                        if let Ok(nested) =
                            serde_json::from_value::<Manifest>(nested_manifest.clone())
                        {
                            detect_circular_dependencies(&nested, visited, path, depth + 1)?;
                        }
                    }
                    ManifestSource::Url { .. } | ManifestSource::Name { .. } => {
                        // For URL/Name sources, we can't check without fetching
                        // This would be done at runtime
                        // For now, just log a warning
                        tracing::warn!(
                            "Cannot statically check circular dependencies for URL/Name manifest sources in node '{}'",
                            node.id
                        );
                    }
                }
            }
        }
    }

    // Remove from path (backtrack)
    path.pop();

    Ok(())
}

/// Validate manifest for circular dependencies (convenience wrapper)
///
/// This is the main entry point for circular dependency checking.
pub fn validate_no_circular_dependencies(manifest: &Manifest) -> Result<()> {
    let mut visited = HashSet::new();
    let mut path = Vec::new();
    detect_circular_dependencies(manifest, &mut visited, &mut path, 0)
}
