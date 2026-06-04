//! Builder pattern for constructing and running an HTTP transport server.

use crate::server::HttpServer;
use remotemedia_core::transport::PipelineExecutor;
use std::sync::Arc;

/// Builder for configuring and creating an [`HttpTransportServer`].
///
/// # Example
///
/// ```ignore
/// use remotemedia_http::HttpServerBuilder;
/// use remotemedia_core::transport::PipelineExecutor;
/// use std::sync::Arc;
///
/// let executor = Arc::new(PipelineExecutor::new()?);
/// let server = HttpServerBuilder::new()
///     .bind("0.0.0.0:9090")
///     .executor(executor)
///     .build()?;
/// server.run().await?;
/// ```
pub struct HttpServerBuilder {
    bind_address: Option<String>,
    executor: Option<Arc<PipelineExecutor>>,
}

impl HttpServerBuilder {
    /// Create a new builder with default values.
    ///
    /// Defaults:
    /// - `bind_address`: `"127.0.0.1:8080"`
    /// - `executor`: `None` (must be provided before calling `build`)
    pub fn new() -> Self {
        Self {
            bind_address: None,
            executor: None,
        }
    }

    /// Set the address the server will bind to.
    ///
    /// If not called, defaults to `"127.0.0.1:8080"`.
    pub fn bind(mut self, addr: impl Into<String>) -> Self {
        self.bind_address = Some(addr.into());
        self
    }

    /// Set the pipeline executor used by the server.
    pub fn executor(mut self, executor: Arc<PipelineExecutor>) -> Self {
        self.executor = Some(executor);
        self
    }

    /// Read configuration from environment variables.
    ///
    /// Currently reads:
    /// - `HTTP_BIND_ADDRESS` - overrides the bind address
    pub fn from_env(mut self) -> Self {
        if let Ok(addr) = std::env::var("HTTP_BIND_ADDRESS") {
            self.bind_address = Some(addr);
        }
        self
    }

    /// Build the [`HttpTransportServer`].
    ///
    /// # Errors
    ///
    /// Returns an error if the executor has not been set.
    pub async fn build(
        self,
    ) -> std::result::Result<HttpTransportServer, Box<dyn std::error::Error>> {
        let executor = self
            .executor
            .ok_or("executor is required — call .executor() before .build()")?;

        let bind_address = self
            .bind_address
            .unwrap_or_else(|| "127.0.0.1:8080".to_string());

        let server = HttpServer::new(bind_address, executor).await?;

        Ok(HttpTransportServer { server })
    }
}

impl Default for HttpServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A configured HTTP transport server ready to run.
///
/// Created via [`HttpServerBuilder::build`].
pub struct HttpTransportServer {
    server: HttpServer,
}

impl HttpTransportServer {
    /// Run the server, blocking until shutdown.
    pub async fn run(self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.server.serve().await?;
        Ok(())
    }
}
