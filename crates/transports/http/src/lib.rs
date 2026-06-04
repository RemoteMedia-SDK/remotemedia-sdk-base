//! HTTP/REST transport for RemoteMedia pipelines with SSE streaming
//!
//! Provides HTTP-based transport with Server-Sent Events (SSE) for
//! bidirectional streaming support.
//!
//! # Features
//!
//! - **Unary execution**: Simple request/response via POST /execute
//! - **Streaming sessions**: Create sessions via POST /stream
//! - **SSE output**: Continuous output streaming via GET /stream/:id/output
//! - **Input submission**: Send inputs via POST /stream/:id/input
//! - **Health checks**: Monitor server health via GET /health
//!
//! # Usage
//!
//! ## Client
//!
//! ```ignore
//! use remotemedia_http::HttpPipelineClient;
//! use remotemedia_core::transport::PipelineClient;
//!
//! let client = HttpPipelineClient::new("http://localhost:8080", None).await?;
//!
//! // Unary execution
//! let output = client.execute_unary(manifest, input).await?;
//!
//! // Streaming session
//! let mut session = client.create_stream_session(manifest).await?;
//! session.send(input).await?;
//! while let Some(output) = session.receive().await? {
//!     // Process output
//! }
//! session.close().await?;
//! ```
//!
//! ## Server
//!
//! ```ignore
//! use remotemedia_http::HttpServer;
//! use remotemedia_core::transport::PipelineExecutor;
//!
//! let executor = Arc::new(PipelineExecutor::new()?);
//! let server = HttpServer::new("127.0.0.1:8080".to_string(), executor).await?;
//! server.serve().await?;
//! ```
//!
//! ## Plugin Registration
//!
//! ```ignore
//! use remotemedia_http::HttpTransportPlugin;
//! use remotemedia_core::transport::TransportPluginRegistry;
//!
//! let mut registry = TransportPluginRegistry::new();
//! registry.register(Arc::new(HttpTransportPlugin));
//!
//! // Create client via registry
//! let client = registry.create_client("http", &config).await?;
//! ```

pub mod builder;
#[cfg(feature = "cli")]
pub mod cli;
pub mod client;
pub mod error;
pub mod plugin;
pub mod server;

// Re-export main types
pub use builder::{HttpServerBuilder, HttpTransportServer};
#[cfg(feature = "cli")]
pub use cli::HttpServeArgs;
pub use client::{HttpPipelineClient, HttpStreamSession};
pub use error::{Error, Result};
pub use plugin::HttpTransportPlugin;
pub use server::HttpServer;
