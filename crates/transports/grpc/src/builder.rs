//! Typed builder for creating and running a gRPC transport server
//!
//! Provides a fluent API for configuring and launching a gRPC server
//! with sensible defaults, environment variable overrides, and
//! built-in graceful shutdown handling.
//!
//! # Example
//!
//! ```no_run
//! use remotemedia_grpc::GrpcServerBuilder;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! GrpcServerBuilder::new()
//!     .bind("0.0.0.0:50051")
//!     .auth_tokens(vec!["my-token".into()])
//!     .max_memory_mb(200)
//!     .build()?
//!     .run()
//!     .await?;
//! # Ok(())
//! # }
//! ```

use crate::auth::AuthConfig;
use crate::limits::ResourceLimits as ServiceResourceLimits;
use crate::server::GrpcServer;
use crate::ServiceConfig;

use remotemedia_core::transport::PipelineExecutor;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// Typed builder for creating a gRPC transport server.
///
/// All fields are optional and fall back to [`ServiceConfig::default()`] values
/// unless explicitly set or populated via [`from_env`](Self::from_env).
pub struct GrpcServerBuilder {
    bind_address: Option<String>,
    executor: Option<Arc<PipelineExecutor>>,
    auth_tokens: Vec<String>,
    require_auth: Option<bool>,
    max_memory_mb: Option<u64>,
    max_timeout_secs: Option<u64>,
    json_logging: Option<bool>,
}

impl GrpcServerBuilder {
    /// Create a new builder with default values.
    pub fn new() -> Self {
        Self {
            bind_address: None,
            executor: None,
            auth_tokens: Vec::new(),
            require_auth: None,
            max_memory_mb: None,
            max_timeout_secs: None,
            json_logging: None,
        }
    }

    /// Set the server bind address (e.g. `"0.0.0.0:50051"`).
    pub fn bind(mut self, addr: impl Into<String>) -> Self {
        self.bind_address = Some(addr.into());
        self
    }

    /// Provide a pre-built [`PipelineExecutor`].
    ///
    /// If not set, [`build`](Self::build) will create one via
    /// [`PipelineExecutor::new()`].
    pub fn executor(mut self, e: Arc<PipelineExecutor>) -> Self {
        self.executor = Some(e);
        self
    }

    /// Set the list of valid authentication tokens.
    ///
    /// When tokens are provided and [`require_auth`](Self::require_auth) has
    /// not been explicitly set, authentication will be enabled automatically.
    pub fn auth_tokens(mut self, tokens: Vec<String>) -> Self {
        self.auth_tokens = tokens;
        self
    }

    /// Explicitly enable or disable authentication.
    ///
    /// When not called, authentication is enabled if
    /// [`auth_tokens`](Self::auth_tokens) is non-empty.
    pub fn require_auth(mut self, require: bool) -> Self {
        self.require_auth = Some(require);
        self
    }

    /// Set the maximum memory per execution in megabytes.
    pub fn max_memory_mb(mut self, mb: u64) -> Self {
        self.max_memory_mb = Some(mb);
        self
    }

    /// Set the maximum execution timeout in seconds.
    pub fn max_timeout_secs(mut self, secs: u64) -> Self {
        self.max_timeout_secs = Some(secs);
        self
    }

    /// Enable or disable JSON structured logging.
    pub fn json_logging(mut self, enabled: bool) -> Self {
        self.json_logging = Some(enabled);
        self
    }

    /// Populate any unset fields from environment variables.
    ///
    /// Reads the same environment variables as [`ServiceConfig::from_env()`]:
    ///
    /// - `GRPC_BIND_ADDRESS`
    /// - `GRPC_AUTH_TOKENS` (comma-separated)
    /// - `GRPC_REQUIRE_AUTH`
    /// - `GRPC_MAX_MEMORY_MB`
    /// - `GRPC_MAX_TIMEOUT_SEC`
    /// - `GRPC_JSON_LOGGING`
    ///
    /// Fields that have already been set via builder methods are **not**
    /// overwritten.
    pub fn from_env(mut self) -> Self {
        if self.bind_address.is_none() {
            if let Ok(addr) = std::env::var("GRPC_BIND_ADDRESS") {
                self.bind_address = Some(addr);
            }
        }

        if self.auth_tokens.is_empty() {
            if let Ok(tokens) = std::env::var("GRPC_AUTH_TOKENS") {
                self.auth_tokens = tokens.split(',').map(|s| s.trim().to_string()).collect();
            }
        }

        if self.require_auth.is_none() {
            if let Ok(val) = std::env::var("GRPC_REQUIRE_AUTH") {
                self.require_auth = Some(val.to_lowercase() == "true");
            }
        }

        if self.max_memory_mb.is_none() {
            if let Ok(val) = std::env::var("GRPC_MAX_MEMORY_MB") {
                if let Ok(mb) = val.parse::<u64>() {
                    self.max_memory_mb = Some(mb);
                }
            }
        }

        if self.max_timeout_secs.is_none() {
            if let Ok(val) = std::env::var("GRPC_MAX_TIMEOUT_SEC") {
                if let Ok(secs) = val.parse::<u64>() {
                    self.max_timeout_secs = Some(secs);
                }
            }
        }

        if self.json_logging.is_none() {
            if let Ok(val) = std::env::var("GRPC_JSON_LOGGING") {
                self.json_logging = Some(val.to_lowercase() == "true");
            }
        }

        self
    }

    /// Build the server, resolving all defaults.
    ///
    /// If no [`executor`](Self::executor) was provided, a new
    /// [`PipelineExecutor`] is created. Returns an error if executor
    /// creation or server initialization fails.
    pub fn build(self) -> Result<GrpcTransportServer, Box<dyn std::error::Error>> {
        let defaults = ServiceConfig::default();

        let bind_address = self.bind_address.unwrap_or(defaults.bind_address);

        let require_auth = self.require_auth.unwrap_or(!self.auth_tokens.is_empty());

        let auth = AuthConfig::new(self.auth_tokens, require_auth);

        let max_memory_bytes = self
            .max_memory_mb
            .map(|mb| mb * 1_000_000)
            .unwrap_or(defaults.limits.max_memory_bytes);

        let max_timeout = self
            .max_timeout_secs
            .map(Duration::from_secs)
            .unwrap_or(defaults.limits.max_timeout);

        let limits = ServiceResourceLimits {
            max_memory_bytes,
            max_timeout,
            ..Default::default()
        };

        let json_logging = self.json_logging.unwrap_or(defaults.json_logging);

        let config = ServiceConfig {
            bind_address,
            auth,
            limits,
            json_logging,
        };

        let executor = match self.executor {
            Some(e) => e,
            None => Arc::new(PipelineExecutor::new()?),
        };

        let server = GrpcServer::new(config, executor)?;

        Ok(GrpcTransportServer { server })
    }
}

impl Default for GrpcServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A fully configured gRPC transport server ready to run.
///
/// Created via [`GrpcServerBuilder::build`]. Call [`run`](Self::run) to
/// start serving with automatic Ctrl+C / graceful shutdown handling.
pub struct GrpcTransportServer {
    server: GrpcServer,
}

impl GrpcTransportServer {
    /// Run the server, blocking until shutdown.
    ///
    /// Sets up an [`AtomicBool`] shutdown flag and spawns a background
    /// task that listens for Ctrl+C via [`tokio::signal::ctrl_c`]. On the
    /// first signal the flag is set and the server begins graceful
    /// shutdown. A watchdog thread will force-exit after 1 second if
    /// graceful shutdown stalls.
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let shutdown_flag_clone = Arc::clone(&shutdown_flag);

        // Spawn a task that waits for Ctrl+C and sets the shutdown flag
        tokio::spawn(async move {
            loop {
                if tokio::signal::ctrl_c().await.is_ok() {
                    let was_already_set = shutdown_flag_clone.swap(true, Ordering::SeqCst);
                    if was_already_set {
                        eprintln!("\nShutdown already in progress, forcing immediate exit");
                        std::process::exit(0);
                    }

                    info!("Ctrl+C received, initiating graceful shutdown");

                    // Watchdog: force-exit after 1 second if graceful shutdown stalls
                    std::thread::spawn(move || {
                        std::thread::sleep(Duration::from_secs(1));
                        eprintln!("Graceful shutdown timeout (1s), forcing exit");
                        std::process::exit(0);
                    });
                    break;
                }
            }
        });

        self.server.serve_with_shutdown_flag(shutdown_flag).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_defaults() {
        let server = GrpcServerBuilder::new().build();
        assert!(server.is_ok());
    }

    #[test]
    fn test_builder_with_bind_address() {
        let server = GrpcServerBuilder::new().bind("127.0.0.1:9999").build();
        assert!(server.is_ok());
    }

    #[test]
    fn test_builder_with_auth_tokens_enables_auth() {
        // When auth_tokens are provided and require_auth is not explicitly set,
        // authentication should be enabled automatically.
        let server = GrpcServerBuilder::new()
            .auth_tokens(vec!["token1".into()])
            .build();
        assert!(server.is_ok());
    }

    #[test]
    fn test_builder_with_explicit_require_auth_false() {
        let server = GrpcServerBuilder::new()
            .auth_tokens(vec!["token1".into()])
            .require_auth(false)
            .build();
        assert!(server.is_ok());
    }

    #[test]
    fn test_builder_with_resource_limits() {
        let server = GrpcServerBuilder::new()
            .max_memory_mb(256)
            .max_timeout_secs(30)
            .build();
        assert!(server.is_ok());
    }

    #[test]
    fn test_builder_with_executor() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let server = GrpcServerBuilder::new().executor(executor).build();
        assert!(server.is_ok());
    }

    #[test]
    fn test_builder_json_logging() {
        let server = GrpcServerBuilder::new().json_logging(false).build();
        assert!(server.is_ok());
    }

    #[test]
    fn test_builder_full_chain() {
        let executor = Arc::new(PipelineExecutor::new().unwrap());
        let server = GrpcServerBuilder::new()
            .bind("0.0.0.0:8080")
            .executor(executor)
            .auth_tokens(vec!["secret".into()])
            .require_auth(true)
            .max_memory_mb(512)
            .max_timeout_secs(60)
            .json_logging(false)
            .build();
        assert!(server.is_ok());
    }
}
