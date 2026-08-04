//! gRPC server binary entry point
//!
//! Starts the RemoteMedia gRPC service for high-performance remote pipeline execution.
//!
//! # Usage
//!
//! ```bash
//! # Start with defaults (localhost:50051, auth required)
//! cargo run -p remotemedia-grpc-server
//!
//! # Start with custom address
//! GRPC_BIND_ADDRESS="0.0.0.0:50051" cargo run -p remotemedia-grpc-server
//!
//! # Start without authentication (dev mode)
//! GRPC_REQUIRE_AUTH=false cargo run -p remotemedia-grpc-server
//!
//! # Start with API tokens
//! GRPC_AUTH_TOKENS="token1,token2" cargo run -p remotemedia-grpc-server
//!
//! # Configure resource limits
//! GRPC_MAX_MEMORY_MB=200 GRPC_MAX_TIMEOUT_SEC=10 cargo run -p remotemedia-grpc-server
//! ```
//!
//! # Environment Variables
//!
//! - `GRPC_BIND_ADDRESS`: Server bind address (default: `0.0.0.0:50051`)
//! - `GRPC_AUTH_TOKENS`: Comma-separated list of valid API tokens
//! - `GRPC_REQUIRE_AUTH`: Enable/disable authentication (default: `false`)
//! - `GRPC_MAX_MEMORY_MB`: Maximum memory per execution in MB (default: `100`)
//! - `GRPC_MAX_TIMEOUT_SEC`: Maximum execution timeout in seconds (default: `5`)
//! - `GRPC_JSON_LOGGING`: Enable JSON structured logging (default: `true`)
//! - `GRPC_PLUGIN_POLICY`: `permissive` or `local-only` (default: `permissive`)
//! - `GRPC_PLUGIN_ROOTS`: Platform-separated allowed roots in local-only mode
//! - `GRPC_MANIFEST_BASE_DIR`: Base directory for relative manifest plugin paths
//! - `RUST_LOG`: Logging level (default: `info`)

use remotemedia_grpc::{init_tracing, GrpcServerBuilder};
#[allow(unused_imports)]
use remotemedia_python_nodes as _python_nodes_link;
use tracing::info;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Build the server (including the bundle-deployment service init) in a
    // synchronous context. The deployment service constructs a
    // `reqwest::blocking::Client`, which spins up and tears down a tokio
    // runtime internally; doing that inside an existing async runtime panics
    // with "Cannot drop a runtime in a context where blocking is not allowed".
    // Building here — before the server runtime exists — avoids that.
    let server = GrpcServerBuilder::new().from_env().build()?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .thread_name("remotemedia-worker")
        .enable_all()
        .build()?;

    runtime.block_on(async {
        // Initialize tracing after from_env() so we can respect GRPC_JSON_LOGGING
        // Note: init_tracing reads GRPC_JSON_LOGGING env var directly
        let json_logging = std::env::var("GRPC_JSON_LOGGING")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(true);
        init_tracing(json_logging);

        info!(
            version = env!("CARGO_PKG_VERSION"),
            protocol = "v1",
            "RemoteMedia gRPC Server starting"
        );

        server.run().await
    })
}
