//! CLI integration for gRPC transport (requires `cli` feature)

use crate::builder::GrpcServerBuilder;
use clap::Args;
use remotemedia_core::transport::PipelineExecutor;
use std::sync::Arc;

/// CLI arguments for serving a gRPC transport server
#[derive(Args, Debug, Clone)]
pub struct GrpcServeArgs {
    /// gRPC server bind address
    #[arg(long, default_value = "0.0.0.0:50051", env = "GRPC_BIND_ADDRESS")]
    pub grpc_bind: String,

    /// Comma-separated list of valid API tokens
    #[arg(long, env = "GRPC_AUTH_TOKENS")]
    pub grpc_auth_tokens: Option<String>,

    /// Require authentication (auto-enabled when tokens are provided)
    #[arg(long, env = "GRPC_REQUIRE_AUTH")]
    pub grpc_require_auth: bool,

    /// Maximum memory per execution in MB
    #[arg(long, default_value_t = 100, env = "GRPC_MAX_MEMORY_MB")]
    pub grpc_max_memory_mb: u64,

    /// Maximum execution timeout in seconds
    #[arg(long, default_value_t = 5, env = "GRPC_MAX_TIMEOUT_SEC")]
    pub grpc_max_timeout_secs: u64,

    /// Enable JSON structured logging
    #[arg(long, env = "GRPC_JSON_LOGGING")]
    pub grpc_json_logging: bool,
}

impl GrpcServeArgs {
    /// Run the gRPC server with these arguments
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        self.run_with_executor(None).await
    }

    /// Run the gRPC server with an optional pre-configured executor
    pub async fn run_with_executor(
        self,
        executor: Option<Arc<PipelineExecutor>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut builder = GrpcServerBuilder::new()
            .bind(&self.grpc_bind)
            .max_memory_mb(self.grpc_max_memory_mb)
            .max_timeout_secs(self.grpc_max_timeout_secs)
            .json_logging(self.grpc_json_logging);

        if let Some(tokens) = self.grpc_auth_tokens {
            let token_list: Vec<String> = tokens.split(',').map(|s| s.trim().to_string()).collect();
            builder = builder.auth_tokens(token_list);
        }

        if self.grpc_require_auth {
            builder = builder.require_auth(true);
        }

        if let Some(exec) = executor {
            builder = builder.executor(exec);
        }

        builder.build()?.run().await
    }
}
