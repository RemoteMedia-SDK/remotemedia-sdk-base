//! CLI integration for HTTP transport (requires `cli` feature)

use crate::builder::HttpServerBuilder;
use clap::Args;
use remotemedia_core::transport::PipelineExecutor;
use std::sync::Arc;

/// CLI arguments for serving an HTTP transport server
#[derive(Args, Debug, Clone)]
pub struct HttpServeArgs {
    /// HTTP server bind address
    #[arg(long, default_value = "127.0.0.1:8080", env = "HTTP_BIND_ADDRESS")]
    pub http_bind: String,
}

impl HttpServeArgs {
    /// Run the HTTP server with these arguments
    pub async fn run(self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.run_with_executor(None).await
    }

    /// Run the HTTP server with an optional pre-configured executor
    pub async fn run_with_executor(
        self,
        executor: Option<Arc<PipelineExecutor>>,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let mut builder = HttpServerBuilder::new().bind(&self.http_bind);

        if let Some(exec) = executor {
            builder = builder.executor(exec);
        }

        builder.build().await?.run().await
    }
}
