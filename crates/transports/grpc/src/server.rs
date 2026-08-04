//! Tonic server setup and configuration for gRPC service
//!
//! Implements server builder with middleware stack (auth, metrics, logging).
//! Provides graceful shutdown and health check support.

use crate::{
    auth::AuthConfig,
    control::ControlServiceImpl,
    deployment::BundleDeploymentServiceImpl,
    execution::ExecutionServiceImpl,
    generated::{
        bundle_deployment_service_server::BundleDeploymentServiceServer,
        pipeline_control_server::PipelineControlServer,
        pipeline_execution_service_server::PipelineExecutionServiceServer,
        streaming_pipeline_service_server::StreamingPipelineServiceServer,
    },
    metrics::ServiceMetrics,
    streaming::StreamingServiceImpl,
    ServiceConfig,
};

use async_trait::async_trait;
use remotemedia_bundle::{
    AcceleratorBackend, CompatibilityRange, RuntimeCapabilities, BUNDLE_SCHEMA_VERSION,
};
use remotemedia_bundle_deployment::{
    ActivationRegistry, ContentStore, DeploymentService, ReqwestExternalAssetTransport,
    TokenAuthenticator,
};
use remotemedia_core::manifest::Manifest;
use remotemedia_core::transport::{
    Participant, PipelineExecutor, PipelineTransport, SharedPipelineStreamSession, StreamSession,
    TransportData,
};
use std::sync::Arc;
use tonic::{service::LayerExt as _, transport::Server};
use tracing::info;

/// gRPC server builder with middleware
pub struct GrpcServer {
    config: ServiceConfig,
    metrics: Arc<ServiceMetrics>,
    executor: Arc<PipelineExecutor>,
    deployment: Option<BundleDeploymentServiceImpl>,
}

impl GrpcServer {
    /// Create new server with configuration and pipeline executor
    ///
    /// The executor encapsulates all scheduler, node registry, and drift metrics.
    /// The server is only responsible for the gRPC transport layer.
    /// (Migrated from PipelineRunner per spec 026)
    pub fn new(
        config: ServiceConfig,
        executor: Arc<PipelineExecutor>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let metrics = Arc::new(ServiceMetrics::with_default_registry()?);

        let deployment = deployment_service_from_env(Arc::clone(&executor))?;
        Ok(Self {
            config,
            metrics,
            executor,
            deployment,
        })
    }

    /// Get metrics for use in service implementations
    pub fn metrics(&self) -> Arc<ServiceMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Get auth config for use in service implementations
    pub fn auth_config(&self) -> &AuthConfig {
        &self.config.auth
    }

    /// Build and run the server
    pub async fn serve(self) -> Result<(), Box<dyn std::error::Error>> {
        let addr: std::net::SocketAddr = self.config.bind_address.parse()?;

        info!(
            %addr,
            auth_required = self.config.auth.require_auth,
            max_memory_mb = self.config.limits.max_memory_bytes / 1_000_000,
            "Starting gRPC server"
        );

        // Create service implementations with PipelineExecutor (spec 026 migration)
        let execution_service = ExecutionServiceImpl::new(
            self.config.auth.clone(),
            self.config.limits.clone(),
            Arc::clone(&self.metrics),
            Arc::clone(&self.executor),
        );

        let streaming_service = StreamingServiceImpl::new(
            self.config.auth.clone(),
            self.config.limits.clone(),
            Arc::clone(&self.metrics),
            Arc::clone(&self.executor),
        );

        // Session Control Bus — per-session pub/sub/intercept/node-state.
        let control_service = ControlServiceImpl::new(self.executor.control_bus());

        // Wrap services with gRPC-Web and CORS support using tower ServiceBuilder
        let execution_service = tower::ServiceBuilder::new()
            .layer(tower_http::cors::CorsLayer::permissive())
            .layer(tonic_web::GrpcWebLayer::new())
            .into_inner()
            .named_layer(
                PipelineExecutionServiceServer::new(execution_service)
                    .max_decoding_message_size(10 * 1024 * 1024) // 10MB for large video frames
                    .max_encoding_message_size(10 * 1024 * 1024), // 10MB
            );

        let streaming_service = tower::ServiceBuilder::new()
            .layer(tower_http::cors::CorsLayer::permissive())
            .layer(tonic_web::GrpcWebLayer::new())
            .into_inner()
            .named_layer(
                StreamingPipelineServiceServer::new(streaming_service)
                    .max_decoding_message_size(10 * 1024 * 1024) // 10MB for large video frames
                    .max_encoding_message_size(10 * 1024 * 1024), // 10MB
            );

        let control_service = tower::ServiceBuilder::new()
            .layer(tower_http::cors::CorsLayer::permissive())
            .layer(tonic_web::GrpcWebLayer::new())
            .into_inner()
            .named_layer(PipelineControlServer::new(control_service));

        // T037: Configure connection pooling and HTTP/2 keepalive for concurrent clients
        let deployment_service = self.deployment.map(|service| {
            BundleDeploymentServiceServer::new(service)
                .max_decoding_message_size(2 * 1024 * 1024)
                .max_encoding_message_size(16 * 1024 * 1024)
        });
        let server = Server::builder()
            // Allow many concurrent requests per connection
            .concurrency_limit_per_connection(256)
            // TCP keepalive to detect dead connections
            .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
            .tcp_nodelay(true)
            // HTTP/2 keepalive ping to keep connections alive
            .http2_keepalive_interval(Some(std::time::Duration::from_secs(30)))
            .http2_keepalive_timeout(Some(std::time::Duration::from_secs(10)))
            // Connection timeouts
            .timeout(std::time::Duration::from_secs(60))
            // Enable HTTP/1.1 for gRPC-Web
            .accept_http1(true)
            // Tracing
            .trace_fn(|_| tracing::info_span!("grpc_request"))
            .add_service(execution_service)
            .add_service(streaming_service)
            .add_service(control_service);
        let server = server.add_optional_service(deployment_service);

        // TODO: Add graceful shutdown on Ctrl+C
        // Requires tokio signal feature which may not be available on all platforms
        info!("gRPC server listening on {}", addr);

        server.serve(addr).await?;

        Ok(())
    }

    /// Build and run the server with external shutdown flag (for robust Ctrl+C handling)
    pub async fn serve_with_shutdown_flag(
        self,
        shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let addr: std::net::SocketAddr = self.config.bind_address.parse()?;

        info!(
            %addr,
            auth_required = self.config.auth.require_auth,
            max_memory_mb = self.config.limits.max_memory_bytes / 1_000_000,
            "Starting gRPC server with shutdown flag"
        );

        // Create service implementations with PipelineExecutor (spec 026 migration)
        let execution_service = ExecutionServiceImpl::new(
            self.config.auth.clone(),
            self.config.limits.clone(),
            Arc::clone(&self.metrics),
            Arc::clone(&self.executor),
        );

        let streaming_service = StreamingServiceImpl::new(
            self.config.auth.clone(),
            self.config.limits.clone(),
            Arc::clone(&self.metrics),
            Arc::clone(&self.executor),
        );

        // Session Control Bus — per-session pub/sub/intercept/node-state.
        let control_service = ControlServiceImpl::new(self.executor.control_bus());

        // Wrap services with gRPC-Web and CORS support using tower ServiceBuilder
        let execution_service = tower::ServiceBuilder::new()
            .layer(tower_http::cors::CorsLayer::permissive())
            .layer(tonic_web::GrpcWebLayer::new())
            .into_inner()
            .named_layer(
                PipelineExecutionServiceServer::new(execution_service)
                    .max_decoding_message_size(10 * 1024 * 1024) // 10MB for large video frames
                    .max_encoding_message_size(10 * 1024 * 1024), // 10MB
            );

        let streaming_service = tower::ServiceBuilder::new()
            .layer(tower_http::cors::CorsLayer::permissive())
            .layer(tonic_web::GrpcWebLayer::new())
            .into_inner()
            .named_layer(
                StreamingPipelineServiceServer::new(streaming_service)
                    .max_decoding_message_size(10 * 1024 * 1024) // 10MB for large video frames
                    .max_encoding_message_size(10 * 1024 * 1024), // 10MB
            );

        let control_service = tower::ServiceBuilder::new()
            .layer(tower_http::cors::CorsLayer::permissive())
            .layer(tonic_web::GrpcWebLayer::new())
            .into_inner()
            .named_layer(PipelineControlServer::new(control_service));

        // T037: Configure connection pooling and HTTP/2 keepalive for concurrent clients
        let deployment_service = self.deployment.map(|service| {
            BundleDeploymentServiceServer::new(service)
                .max_decoding_message_size(2 * 1024 * 1024)
                .max_encoding_message_size(16 * 1024 * 1024)
        });
        let server = Server::builder()
            // Allow many concurrent requests per connection
            .concurrency_limit_per_connection(256)
            // TCP keepalive to detect dead connections
            .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
            .tcp_nodelay(true)
            // HTTP/2 keepalive ping to keep connections alive
            .http2_keepalive_interval(Some(std::time::Duration::from_secs(30)))
            .http2_keepalive_timeout(Some(std::time::Duration::from_secs(10)))
            // Connection timeouts
            .timeout(std::time::Duration::from_secs(60))
            // Enable HTTP/1.1 for gRPC-Web
            .accept_http1(true)
            // Tracing
            .trace_fn(|_| tracing::info_span!("grpc_request"))
            .add_service(execution_service)
            .add_service(streaming_service)
            .add_service(control_service);
        let server = server.add_optional_service(deployment_service);

        info!("gRPC server listening on {}", addr);

        // Monitor shutdown flag and trigger graceful shutdown
        // Poll frequently (10ms) to ensure responsive shutdown
        let shutdown_future = async move {
            let mut check_count = 0u64;
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                check_count += 1;

                if shutdown_flag.load(std::sync::atomic::Ordering::SeqCst) {
                    info!(
                        "[SHUTDOWN] Flag detected after {} checks ({}ms)",
                        check_count,
                        check_count * 10
                    );
                    info!("[SHUTDOWN] Initiating graceful shutdown of gRPC server...");
                    break;
                }

                // Log periodically to show we're still checking
                if check_count % 6000 == 0 {
                    // Every minute
                    info!(
                        "[HEALTH] Server running, checked shutdown flag {} times",
                        check_count
                    );
                }
            }
            info!("[SHUTDOWN] Shutdown future completed, server will now close connections");
        };

        info!("[SERVER] Calling serve_with_shutdown, will block until shutdown signal...");
        let serve_result = server.serve_with_shutdown(addr, shutdown_future).await;
        info!(
            "[SERVER] serve_with_shutdown returned: {:?}",
            serve_result.is_ok()
        );

        serve_result?;

        info!("[SHUTDOWN] gRPC server shutdown complete");

        Ok(())
    }

    /// Expose Prometheus metrics as HTTP endpoint
    ///
    /// Returns metrics text for /metrics endpoint
    pub fn metrics_text(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.metrics.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }

    /// Start or join a streaming pipeline session shared by logical key.
    ///
    /// This is the gRPC transport's opt-in entry point for one pipeline with
    /// many clients. Existing `PipelineTransport::stream` behavior remains a
    /// fresh session per call.
    pub async fn shared_stream(
        &self,
        key: impl Into<String>,
        manifest: Arc<Manifest>,
        participant: Participant,
    ) -> remotemedia_core::Result<Box<dyn StreamSession>> {
        let shared = self
            .executor
            .get_or_create_shared_session(key, manifest)
            .await?;
        Ok(Box::new(SharedPipelineStreamSession::new(
            &shared,
            participant,
        )))
    }
}

fn deployment_service_from_env(
    executor: Arc<PipelineExecutor>,
) -> Result<Option<BundleDeploymentServiceImpl>, Box<dyn std::error::Error>> {
    let Ok(token) = std::env::var("REMOTEMEDIA_DEPLOY_TOKEN") else {
        return Ok(None);
    };
    if token.is_empty() {
        return Err("REMOTEMEDIA_DEPLOY_TOKEN must not be empty".into());
    }
    let root = std::env::var("REMOTEMEDIA_DEPLOY_ROOT")
        .unwrap_or_else(|_| ".remotemedia/deployment".to_owned());
    let memory_bytes = env_u64("REMOTEMEDIA_RUNTIME_MEMORY_BYTES", 0)?;
    let cache_bytes = env_u64("REMOTEMEDIA_CACHE_AVAILABLE_BYTES", 0)?;
    let capabilities = RuntimeCapabilities {
        schema_version: BUNDLE_SCHEMA_VERSION.to_owned(),
        os: std::env::consts::OS.to_owned(),
        architecture: runtime_architecture(),
        native_abi: std::env::var("REMOTEMEDIA_NATIVE_ABI").ok(),
        manifest_schemas: vec!["v1".to_owned()],
        plugin_abi: CompatibilityRange {
            minimum: env!("CARGO_PKG_VERSION").to_owned(),
            maximum_exclusive: None,
        },
        python: Vec::new(),
        accelerators: vec![AcceleratorBackend::Cpu],
        memory_bytes,
        available_cache_bytes: cache_bytes,
        media_devices: env_list("REMOTEMEDIA_MEDIA_DEVICES"),
        runtime_features: env_list("REMOTEMEDIA_RUNTIME_FEATURES"),
    };
    let content = ContentStore::open(std::path::Path::new(&root).join("cas"))?;
    let registry = ActivationRegistry::open(std::path::Path::new(&root).join("state"))?;
    let mut external_transport = ReqwestExternalAssetTransport::new()?;
    if let Ok(token) = std::env::var("REMOTEMEDIA_EXTERNAL_ASSET_BEARER_TOKEN") {
        external_transport = external_transport.with_bearer_token(token)?;
    }
    let service = DeploymentService::new(
        TokenAuthenticator::new(token.as_bytes()),
        capabilities,
        content,
        registry,
    )
    .with_external_asset_transport(std::sync::Arc::new(external_transport));
    Ok(Some(BundleDeploymentServiceImpl::new(
        AuthConfig::new(vec![token], true),
        service,
        executor,
    )))
}

fn env_u64(name: &str, default: u64) -> Result<u64, Box<dyn std::error::Error>> {
    match std::env::var(name) {
        Ok(value) => Ok(value
            .parse()
            .map_err(|_| format!("{name} must be an unsigned integer"))?),
        Err(_) => Ok(default),
    }
}

fn runtime_architecture() -> String {
    match std::env::consts::ARCH {
        "x86_64" => "amd64".to_owned(),
        "aarch64" => "arm64".to_owned(),
        architecture => architecture.to_owned(),
    }
}

fn env_list(name: &str) -> Vec<String> {
    std::env::var(name)
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_linux_x86_architecture_for_bundle_profiles() {
        assert_eq!(runtime_architecture(), "amd64");
    }

    #[test]
    fn test_server_creation() {
        let config = ServiceConfig::default();
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let server = GrpcServer::new(config, executor);
        assert!(server.is_ok());
    }

    #[test]
    fn test_metrics_access() {
        let config = ServiceConfig::default();
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let server = GrpcServer::new(config, executor).unwrap();
        let metrics = server.metrics();

        // Test metrics are accessible
        metrics.active_connections.inc();
        assert_eq!(metrics.active_connections.get(), 1);
    }

    #[test]
    fn test_metrics_text_export() {
        let config = ServiceConfig::default();
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let server = GrpcServer::new(config, executor).unwrap();

        let text = server.metrics_text();
        assert!(text.contains("remotemedia_grpc"));
    }

    #[test]
    fn test_auth_config_access() {
        let mut config = ServiceConfig::default();
        config.auth.require_auth = false;

        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let server = GrpcServer::new(config, executor).unwrap();
        assert!(!server.auth_config().require_auth);
    }

    #[tokio::test]
    async fn shared_stream_reuses_pipeline_session_by_key() {
        let config = ServiceConfig::default();
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let server = GrpcServer::new(config, executor).unwrap();
        let manifest = Arc::new(Manifest {
            version: "v1".to_string(),
            metadata: Default::default(),
            nodes: Vec::new(),
            connections: Vec::new(),
            python_env: None,
            plugins: Vec::new(),
        });

        let first = server
            .shared_stream(
                "room-a",
                Arc::clone(&manifest),
                Participant::new("grpc-a", "client"),
            )
            .await
            .unwrap();
        let second = server
            .shared_stream("room-a", manifest, Participant::new("grpc-b", "client"))
            .await
            .unwrap();

        assert_eq!(first.session_id(), second.session_id());
    }
}

/// Implement PipelineTransport for GrpcServer
///
/// This allows GrpcServer to be used as a transport server that can execute
/// pipelines via the PipelineExecutor. (Migrated from PipelineRunner per spec 026)
#[async_trait]
impl PipelineTransport for GrpcServer {
    /// Execute a pipeline with unary semantics
    ///
    /// Delegates to the PipelineExecutor to execute the pipeline synchronously.
    async fn execute(
        &self,
        manifest: Arc<Manifest>,
        input: TransportData,
    ) -> remotemedia_core::Result<TransportData> {
        self.executor.execute_unary(manifest, input).await
    }

    /// Start a streaming pipeline session
    ///
    /// Delegates to the PipelineExecutor to create a streaming session.
    async fn stream(
        &self,
        manifest: Arc<Manifest>,
    ) -> remotemedia_core::Result<Box<dyn StreamSession>> {
        let session = self.executor.create_session(manifest).await?;
        // Wrap in SessionHandleWrapper for StreamSession trait compatibility
        Ok(Box::new(SessionHandleWrapper(session)))
    }
}

/// Wrapper to adapt PipelineExecutor's SessionHandle to StreamSession trait
struct SessionHandleWrapper(remotemedia_core::transport::SessionHandle);

#[async_trait]
impl StreamSession for SessionHandleWrapper {
    async fn send_input(&mut self, data: TransportData) -> remotemedia_core::Result<()> {
        self.0.send_input(data).await
    }

    async fn recv_output(&mut self) -> remotemedia_core::Result<Option<TransportData>> {
        self.0.recv_output().await
    }

    async fn close(&mut self) -> remotemedia_core::Result<()> {
        self.0.close().await
    }

    fn session_id(&self) -> &str {
        &self.0.session_id
    }

    fn is_active(&self) -> bool {
        self.0.is_active()
    }
}
