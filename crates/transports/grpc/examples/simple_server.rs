//! Simple gRPC Server Example
//!
//! This example demonstrates how to set up a basic RemoteMedia gRPC server
//! using the new transport decoupling architecture.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example simple_server --package remotemedia-grpc
//! ```
//!
//! The server will start on `0.0.0.0:50051` by default.
//!
//! # Environment Variables
//!
//! - `GRPC_BIND_ADDRESS`: Server address (default: "0.0.0.0:50051")
//! - `GRPC_REQUIRE_AUTH`: Enable authentication (default: false)
//! - `GRPC_JSON_LOGGING`: Enable JSON logging (default: true)
//! - `RUST_LOG`: Logging level (default: "info")

use remotemedia_core::transport::PipelineExecutor;
use remotemedia_grpc::{init_tracing, GrpcServer, ServiceConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration from environment variables
    let config = ServiceConfig::from_env();

    // Initialize logging
    init_tracing(config.json_logging);

    println!("🚀 RemoteMedia gRPC Server Example");
    println!("📍 Address: {}", config.bind_address);
    println!(
        "🔐 Auth: {}",
        if config.auth.require_auth {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!();

    // Create pipeline runner
    // PipelineExecutor encapsulates all executor and node registry logic
    let runner = PipelineExecutor::new()?;
    let runner = Arc::new(runner);

    println!("✅ PipelineExecutor initialized");
    println!("   All nodes registered and ready");
    println!();

    // Create and start server
    let server = GrpcServer::new(config, runner)?;

    println!("🎧 Starting server...");
    println!("   Press Ctrl+C to shutdown");
    println!();

    // Run server (blocks until shutdown)
    server.serve().await?;

    println!("👋 Server shutdown complete");
    Ok(())
}
