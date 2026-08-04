//! Unary RPC handler for pipeline execution
//!
//! Implements PipelineExecutionService trait for ExecutePipeline RPC.
//! Provides manifest-to-runtime conversion and result serialization using PipelineExecutor.
//! (Migrated from PipelineRunner per spec 026)

// Internal infrastructure - some fields reserved for future use
#![allow(dead_code)]

use crate::{
    adapters::{data_buffer_to_runtime_data, runtime_data_to_data_buffer},
    auth::{check_auth, AuthConfig},
    generated::{
        pipeline_execution_service_server::PipelineExecutionService, ErrorResponse, ErrorType,
        ExecuteRequest, ExecuteResponse, ExecutionMetrics as ProtoExecutionMetrics,
        ExecutionResult as ProtoExecutionResult, ExecutionStatus, VersionInfo, VersionRequest,
        VersionResponse,
    },
    limits::ResourceLimits,
    manifest_wire::{decode_manifest, PluginPolicy},
    metrics::ServiceMetrics,
    ServiceError,
};

use remotemedia_core::{
    manifest::Manifest,
    transport::{PipelineExecutor, TransportData},
};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{error, info};

use crate::embedded_plugin::materialize_embedded_plugins;

/// ExecutePipeline service implementation
pub struct ExecutionServiceImpl {
    auth_config: AuthConfig,
    limits: ResourceLimits,
    metrics: Arc<ServiceMetrics>,
    /// Pipeline executor encapsulates scheduler, node registry, and drift metrics
    executor: Arc<PipelineExecutor>,
    plugin_policy: PluginPolicy,
}

impl ExecutionServiceImpl {
    /// Create new execution service with pipeline executor (spec 026 migration)
    pub fn new(
        auth_config: AuthConfig,
        limits: ResourceLimits,
        metrics: Arc<ServiceMetrics>,
        executor: Arc<PipelineExecutor>,
    ) -> Self {
        tracing::info!("ExecutionServiceImpl initialized with PipelineExecutor");

        Self::new_with_policy(
            auth_config,
            limits,
            metrics,
            executor,
            PluginPolicy::permissive(),
        )
    }

    pub fn new_with_policy(
        auth_config: AuthConfig,
        limits: ResourceLimits,
        metrics: Arc<ServiceMetrics>,
        executor: Arc<PipelineExecutor>,
        plugin_policy: PluginPolicy,
    ) -> Self {
        Self {
            auth_config,
            limits,
            metrics,
            executor,
            plugin_policy,
        }
    }

    /// Validate manifest structure
    ///
    /// Note: Node parameter validation is performed by PipelineExecutor during execution.
    /// This method handles transport-specific validation only (e.g., size limits).
    fn validate_manifest(&self, _manifest: &Manifest) -> Result<(), ServiceError> {
        // Transport-specific validation (size limits, rate limits, etc.)
        // Node parameter validation is performed by PipelineExecutor
        Ok(())
    }

    /// Collect execution metrics
    fn collect_metrics(
        &self,
        start_time: std::time::Instant,
        memory_bytes: u64,
    ) -> ProtoExecutionMetrics {
        ProtoExecutionMetrics {
            wall_time_ms: start_time.elapsed().as_millis() as f64,
            cpu_time_ms: 0.0, // TODO: Measure actual CPU time
            memory_used_bytes: memory_bytes,
            serialization_time_ms: 0.0,
            node_metrics: HashMap::new(), // TODO: Get per-node metrics from runner
            proto_to_runtime_ms: 0.0,     // TODO: Measure conversion overhead
            runtime_to_proto_ms: 0.0,     // TODO: Measure conversion overhead
            data_type_breakdown: HashMap::new(),
        }
    }
}

#[tonic::async_trait]
impl PipelineExecutionService for ExecutionServiceImpl {
    async fn execute_pipeline(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let start_time = std::time::Instant::now();
        self.metrics.record_request_start("ExecutePipeline");

        // Check authentication
        check_auth(&request, &self.auth_config)?;

        let req = request.into_inner();

        // Validate request
        let proto_manifest = req
            .manifest
            .ok_or_else(|| Status::invalid_argument("Manifest is required"))?;

        // Deserialize manifest
        let mut manifest = match decode_manifest(&proto_manifest, &self.plugin_policy) {
            Ok(m) => m,
            Err(e) => {
                self.metrics
                    .record_request_end("ExecutePipeline", "error", start_time);
                self.metrics.record_error("validation");

                let error_response = ErrorResponse {
                    error_type: ErrorType::Validation as i32,
                    message: e.to_string(),
                    failing_node_id: String::new(),
                    context: "Manifest deserialization failed".to_string(),
                    stack_trace: String::new(),
                };

                let response = ExecuteResponse {
                    outcome: Some(crate::generated::execute_response::Outcome::Error(
                        error_response,
                    )),
                };

                return Ok(Response::new(response));
            }
        };

        // Materialize any inline plugin blobs shipped with the request
        // (portable pipeline bundles) into a content-addressed temp dir and
        // rewrite `embedded:<digest>` plugin specs to point at them, so the
        // executor can dlopen the plugin without a prior deploy.
        let _cas_guard = if !req.embedded_plugins.is_empty() {
            match materialize_embedded_plugins(&req.embedded_plugins, &mut manifest) {
                Ok(guard) => Some(guard),
                Err(e) => {
                    self.metrics
                        .record_request_end("ExecutePipeline", "error", start_time);
                    self.metrics.record_error("plugin_load");
                    let error_response = ErrorResponse {
                        error_type: ErrorType::Validation as i32,
                        message: e.to_string(),
                        failing_node_id: String::new(),
                        context: "Embedded plugin materialization failed".to_string(),
                        stack_trace: String::new(),
                    };
                    let response = ExecuteResponse {
                        outcome: Some(crate::generated::execute_response::Outcome::Error(
                            error_response,
                        )),
                    };
                    return Ok(Response::new(response));
                }
            }
        } else {
            None
        };

        // Materialize an embedded (frozen) Python wheelhouse shipped inline
        // with the request into a venv, and record it on the manifest so the
        // multiprocess executor uses it directly (no network, no resolution).
        let _py_env_guard = if let Some(py) = req.embedded_python_env.as_ref() {
            let decoded = crate::embedded_plugin::decode_embedded_python_env(py);
            match crate::embedded_plugin::materialize_embedded_python_env(&decoded) {
                Ok((venv_python, guard)) => {
                    manifest.python_env = Some(remotemedia_core::manifest::ManifestPythonEnv {
                        explicit_venv: Some(venv_python),
                        scope: Some(remotemedia_core::python::env_manager::EnvScope::PerPipeline),
                        ..Default::default()
                    });
                    Some(guard)
                }
                Err(e) => {
                    self.metrics
                        .record_request_end("ExecutePipeline", "error", start_time);
                    self.metrics.record_error("python_env");
                    let error_response = ErrorResponse {
                        error_type: ErrorType::Validation as i32,
                        message: e,
                        failing_node_id: String::new(),
                        context: "Embedded Python environment materialization failed".to_string(),
                        stack_trace: String::new(),
                    };
                    let response = ExecuteResponse {
                        outcome: Some(crate::generated::execute_response::Outcome::Error(
                            error_response,
                        )),
                    };
                    return Ok(Response::new(response));
                }
            }
        } else {
            None
        };

        let manifest = Arc::new(manifest);

        // Validate manifest
        if let Err(e) = self.validate_manifest(&manifest) {
            self.metrics
                .record_request_end("ExecutePipeline", "error", start_time);
            self.metrics.record_error("validation");

            let error_response = ErrorResponse {
                error_type: ErrorType::Validation as i32,
                message: e.to_string(),
                failing_node_id: String::new(),
                context: "Manifest validation failed".to_string(),
                stack_trace: String::new(),
            };

            let response = ExecuteResponse {
                outcome: Some(crate::generated::execute_response::Outcome::Error(
                    error_response,
                )),
            };

            return Ok(Response::new(response));
        }

        // Load any manifest-declared plugins (cdylib dlopen / source-python)
        // before executing, so node types like RustWhisperNode are
        // registered. Without this the unary path never loads plugins and
        // reports "Unknown node type". The streaming path does the same in
        // streaming.rs.
        if let Err(e) = self.executor.ensure_plugins_loaded(&manifest).await {
            self.metrics
                .record_request_end("ExecutePipeline", "error", start_time);
            self.metrics.record_error("plugin_load");

            let error_response = ErrorResponse {
                error_type: ErrorType::Validation as i32,
                message: e.to_string(),
                failing_node_id: String::new(),
                context: "Plugin loading failed".to_string(),
                stack_trace: String::new(),
            };

            let response = ExecuteResponse {
                outcome: Some(crate::generated::execute_response::Outcome::Error(
                    error_response,
                )),
            };

            return Ok(Response::new(response));
        }

        // Convert first data input to TransportData
        // For unary execution, we expect exactly one input
        let input = if let Some((node_id, data_buffer)) = req.data_inputs.into_iter().next() {
            match data_buffer_to_runtime_data(&data_buffer) {
                Some(runtime_data) => TransportData::new(runtime_data),
                None => {
                    self.metrics
                        .record_request_end("ExecutePipeline", "error", start_time);
                    self.metrics.record_error("validation");

                    let error_response = ErrorResponse {
                        error_type: ErrorType::Validation as i32,
                        message: format!("Input data conversion failed for node '{}'", node_id),
                        failing_node_id: node_id.clone(),
                        context: "Data buffer conversion failed".to_string(),
                        stack_trace: String::new(),
                    };

                    let response = ExecuteResponse {
                        outcome: Some(crate::generated::execute_response::Outcome::Error(
                            error_response,
                        )),
                    };

                    return Ok(Response::new(response));
                }
            }
        } else {
            self.metrics
                .record_request_end("ExecutePipeline", "error", start_time);
            self.metrics.record_error("validation");

            let error_response = ErrorResponse {
                error_type: ErrorType::Validation as i32,
                message: "No input data provided".to_string(),
                failing_node_id: String::new(),
                context: "ExecutePipeline requires at least one input".to_string(),
                stack_trace: String::new(),
            };

            let response = ExecuteResponse {
                outcome: Some(crate::generated::execute_response::Outcome::Error(
                    error_response,
                )),
            };

            return Ok(Response::new(response));
        };

        // Execute pipeline using PipelineExecutor (spec 026 migration)
        info!(
            nodes = manifest.nodes.len(),
            connections = manifest.connections.len(),
            "Executing pipeline"
        );

        let output = match self.executor.execute_unary(manifest.clone(), input).await {
            Ok(result) => result,
            Err(e) => {
                error!(error = %e, "Pipeline execution failed");
                self.metrics
                    .record_request_end("ExecutePipeline", "error", start_time);

                // Detect validation errors and map to appropriate error type
                let (error_type, message, context) =
                    if let remotemedia_core::Error::Validation(ref validation_errors) = e {
                        self.metrics.record_error("validation");
                        // Format validation errors as structured JSON for clients
                        let errors_json = serde_json::to_string(validation_errors)
                            .unwrap_or_else(|_| e.to_string());
                        (
                            ErrorType::Validation as i32,
                            errors_json,
                            format!(
                                "{} validation error(s) in node parameters",
                                validation_errors.len()
                            ),
                        )
                    } else {
                        self.metrics.record_error("execution");
                        (
                            ErrorType::NodeExecution as i32,
                            e.to_string(),
                            String::new(),
                        )
                    };

                let error_response = ErrorResponse {
                    error_type,
                    message,
                    failing_node_id: String::new(),
                    context,
                    stack_trace: String::new(),
                };

                let response = ExecuteResponse {
                    outcome: Some(crate::generated::execute_response::Outcome::Error(
                        error_response,
                    )),
                };

                return Ok(Response::new(response));
            }
        };

        // Convert output to protobuf format
        let output_buffer = runtime_data_to_data_buffer(&output.data);
        let mut data_outputs = HashMap::new();
        data_outputs.insert("output".to_string(), output_buffer);

        info!(
            outputs = data_outputs.len(),
            duration_ms = start_time.elapsed().as_millis(),
            "Pipeline execution completed"
        );

        // Collect metrics
        let metrics = self.collect_metrics(start_time, 0); // TODO: Get actual memory usage

        let exec_result_proto = ProtoExecutionResult {
            data_outputs,
            metrics: Some(metrics),
            node_results: vec![], // TODO: Include per-node results
            status: ExecutionStatus::Success as i32,
        };

        let response = ExecuteResponse {
            outcome: Some(crate::generated::execute_response::Outcome::Result(
                exec_result_proto,
            )),
        };

        self.metrics
            .record_request_end("ExecutePipeline", "success", start_time);

        Ok(Response::new(response))
    }

    async fn get_version(
        &self,
        _request: Request<VersionRequest>,
    ) -> Result<Response<VersionResponse>, Status> {
        // Return static version info
        let version_info = VersionInfo {
            protocol_version: "v1".to_string(),
            runtime_version: env!("CARGO_PKG_VERSION").to_string(),
            supported_node_types: vec![], // TODO: Get from PipelineExecutor
            supported_protocols: vec!["v1".to_string()],
            build_timestamp: "unknown".to_string(), // TODO: Add actual build timestamp
        };

        Ok(Response::new(VersionResponse {
            version_info: Some(version_info),
            compatible: true,
            compatibility_message: String::from("Compatible"),
        }))
    }
}

#[cfg(test)]
#[cfg(test)]
mod tests {
    #[test]
    fn test_placeholder() {
        // Placeholder test - gRPC integration tests are in tests/grpc_integration/
        assert!(true);
    }
}
