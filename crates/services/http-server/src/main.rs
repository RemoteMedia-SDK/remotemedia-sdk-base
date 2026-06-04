//! HTTP server binary entry point
//!
//! Starts the RemoteMedia HTTP/REST service with SSE streaming support.
//!
//! # Usage
//!
//! ```bash
//! # Start with defaults (localhost:8080)
//! cargo run -p remotemedia-http-server
//!
//! # Start with custom address
//! HTTP_BIND_ADDRESS="0.0.0.0:8080" cargo run -p remotemedia-http-server
//!
//! # With logging
//! RUST_LOG=debug cargo run -p remotemedia-http-server
//! ```
//!
//! # Environment Variables
//!
//! - `HTTP_BIND_ADDRESS`: Server bind address (default: `127.0.0.1:8080`)
//! - `RUST_LOG`: Logging level (default: `info`)

use remotemedia_http::HttpServerBuilder;
use tracing::info;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!(
        version = env!("CARGO_PKG_VERSION"),
        "RemoteMedia HTTP Server starting"
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .thread_name("remotemedia-http")
        .enable_all()
        .build()?;

    runtime.block_on(async {
        HttpServerBuilder::new()
            .from_env()
            .build()
            .await?
            .run()
            .await
    })
}
