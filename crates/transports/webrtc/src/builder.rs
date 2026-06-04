//! Builder pattern for constructing WebRTC transports.
//!
//! Provides two builders for the two primary modes of operation:
//!
//! - [`WebRtcServerBuilder`]: Creates a [`WebRtcTransport`] that connects to a
//!   WebSocket signaling server as a client.
//! - [`WebRtcSignalingServerBuilder`]: Creates a gRPC signaling server that
//!   accepts WebRTC peer connections and routes media through a pipeline.
//!   Only available with the `grpc-signaling` feature.

use crate::config::WebRtcTransportConfig;
use crate::transport::WebRtcTransport;

// ---------------------------------------------------------------------------
// WebRtcServerBuilder (WebSocket client mode)
// ---------------------------------------------------------------------------

/// Builder for creating a [`WebRtcTransport`] that connects to a WebSocket
/// signaling server.
///
/// # Example
///
/// ```no_run
/// use remotemedia_webrtc::WebRtcServerBuilder;
///
/// # async fn example() -> std::result::Result<(), Box<dyn std::error::Error>> {
/// let handle = WebRtcServerBuilder::new()
///     .signaling_url("ws://localhost:9090")
///     .max_peers(4)
///     .build()?;
///
/// handle.run().await?;
/// # Ok(())
/// # }
/// ```
pub struct WebRtcServerBuilder {
    signaling_url: Option<String>,
    stun_servers: Vec<String>,
    max_peers: Option<u32>,
    enable_data_channel: Option<bool>,
    jitter_buffer_ms: Option<u32>,
}

impl WebRtcServerBuilder {
    /// Create a new builder with default values.
    pub fn new() -> Self {
        Self {
            signaling_url: None,
            stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
            max_peers: None,
            enable_data_channel: None,
            jitter_buffer_ms: None,
        }
    }

    /// Set the WebSocket signaling server URL.
    ///
    /// Defaults to `"ws://localhost:8080"` if not specified.
    pub fn signaling_url(mut self, url: impl Into<String>) -> Self {
        self.signaling_url = Some(url.into());
        self
    }

    /// Set the STUN server URLs.
    ///
    /// Defaults to `["stun:stun.l.google.com:19302"]`.
    pub fn stun_servers(mut self, servers: Vec<String>) -> Self {
        self.stun_servers = servers;
        self
    }

    /// Set the maximum number of peers allowed in the mesh.
    ///
    /// Defaults to `10`. Must be in the range 1-10.
    pub fn max_peers(mut self, n: u32) -> Self {
        self.max_peers = Some(n);
        self
    }

    /// Enable or disable the data channel.
    ///
    /// Defaults to `true`.
    pub fn enable_data_channel(mut self, enabled: bool) -> Self {
        self.enable_data_channel = Some(enabled);
        self
    }

    /// Set the jitter buffer size in milliseconds.
    ///
    /// Defaults to `100`. Must be in the range 50-200.
    pub fn jitter_buffer_ms(mut self, ms: u32) -> Self {
        self.jitter_buffer_ms = Some(ms);
        self
    }

    /// Build and validate the transport, returning a [`WebRtcTransportHandle`].
    pub fn build(self) -> std::result::Result<WebRtcTransportHandle, Box<dyn std::error::Error>> {
        let mut config = WebRtcTransportConfig::default();

        if let Some(url) = self.signaling_url {
            config.signaling_url = url;
        }
        config.stun_servers = self.stun_servers;
        if let Some(max) = self.max_peers {
            config.max_peers = max;
        }
        if let Some(dc) = self.enable_data_channel {
            config.enable_data_channel = dc;
        }
        if let Some(jb) = self.jitter_buffer_ms {
            config.jitter_buffer_size_ms = jb;
        }

        config.validate()?;

        let transport = WebRtcTransport::new(config)?;

        Ok(WebRtcTransportHandle { transport })
    }
}

impl Default for WebRtcServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// WebRtcTransportHandle
// ---------------------------------------------------------------------------

/// Handle wrapping a fully-configured [`WebRtcTransport`].
///
/// Call [`run`](Self::run) to start the transport and block until a shutdown
/// signal (ctrl-c) is received.
pub struct WebRtcTransportHandle {
    transport: WebRtcTransport,
}

impl WebRtcTransportHandle {
    /// Start the transport and run until a ctrl-c signal is received.
    ///
    /// On shutdown the transport is gracefully stopped.
    pub async fn run(self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        self.transport.start().await?;

        tokio::signal::ctrl_c().await?;

        self.transport.shutdown().await?;

        Ok(())
    }

    /// Return a reference to the underlying [`WebRtcTransport`].
    pub fn transport(&self) -> &WebRtcTransport {
        &self.transport
    }

    /// Consume the handle and return the inner [`WebRtcTransport`].
    pub fn into_transport(self) -> WebRtcTransport {
        self.transport
    }
}

// ---------------------------------------------------------------------------
// WebRtcSignalingServerBuilder (gRPC signaling server mode)
// ---------------------------------------------------------------------------

#[cfg(feature = "grpc-signaling")]
mod grpc_builder {
    use super::*;
    use crate::signaling::grpc::WebRtcSignalingService;
    use remotemedia_core::{manifest::Manifest, transport::PipelineSessionHost};
    use std::sync::Arc;

    /// Builder for creating a gRPC-based WebRTC signaling server.
    ///
    /// The server accepts WebRTC peer connections and routes media through a
    /// [`PipelineExecutor`] driven by a [`Manifest`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// use remotemedia_webrtc::WebRtcSignalingServerBuilder;
    /// use remotemedia_core::{manifest::Manifest, transport::PipelineExecutor};
    /// use std::sync::Arc;
    ///
    /// # async fn example() -> std::result::Result<(), Box<dyn std::error::Error>> {
    /// let executor = Arc::new(PipelineExecutor::new()?);
    /// let manifest: Arc<Manifest> = todo!();
    ///
    /// let server = WebRtcSignalingServerBuilder::new()
    ///     .bind("0.0.0.0:50052")
    ///     .executor(executor)
    ///     .manifest(manifest)
    ///     .build()?;
    ///
    /// server.run().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub struct WebRtcSignalingServerBuilder {
        bind_address: Option<String>,
        executor: Option<Arc<dyn PipelineSessionHost>>,
        manifest: Option<Arc<Manifest>>,
        stun_servers: Vec<String>,
        max_peers: Option<u32>,
        enable_data_channel: Option<bool>,
        jitter_buffer_ms: Option<u32>,
    }

    impl WebRtcSignalingServerBuilder {
        /// Create a new builder with default values.
        pub fn new() -> Self {
            Self {
                bind_address: None,
                executor: None,
                manifest: None,
                stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
                max_peers: None,
                enable_data_channel: None,
                jitter_buffer_ms: None,
            }
        }

        /// Set the address the gRPC server binds to.
        ///
        /// Defaults to `"0.0.0.0:50051"`.
        pub fn bind(mut self, addr: impl Into<String>) -> Self {
            self.bind_address = Some(addr.into());
            self
        }

        /// Set the pipeline executor.
        ///
        /// This is **required** -- [`build`](Self::build) will fail if not set.
        pub fn executor(mut self, executor: Arc<dyn PipelineSessionHost>) -> Self {
            self.executor = Some(executor);
            self
        }

        /// Set the pipeline manifest.
        ///
        /// This is **required** -- [`build`](Self::build) will fail if not set.
        pub fn manifest(mut self, manifest: Arc<Manifest>) -> Self {
            self.manifest = Some(manifest);
            self
        }

        /// Load a manifest from a file on disk (JSON or YAML).
        ///
        /// Detects format by file extension (`.yaml`/`.yml` for YAML, otherwise JSON).
        /// Replaces any previously set manifest.
        pub fn manifest_from_file(
            mut self,
            path: impl AsRef<std::path::Path>,
        ) -> std::result::Result<Self, Box<dyn std::error::Error>> {
            let path = path.as_ref();
            let contents = std::fs::read_to_string(path)?;
            let manifest: Manifest = match path.extension().and_then(|e| e.to_str()) {
                Some("yaml" | "yml") => {
                    serde_json::from_value(serde_yaml::from_str::<serde_json::Value>(&contents)?)?
                }
                _ => serde_json::from_str(&contents)?,
            };
            self.manifest = Some(Arc::new(manifest));
            Ok(self)
        }

        /// Set the STUN server URLs.
        ///
        /// Defaults to `["stun:stun.l.google.com:19302"]`.
        pub fn stun_servers(mut self, servers: Vec<String>) -> Self {
            self.stun_servers = servers;
            self
        }

        /// Set the maximum number of peers allowed.
        ///
        /// Defaults to `10`.
        pub fn max_peers(mut self, n: u32) -> Self {
            self.max_peers = Some(n);
            self
        }

        /// Enable or disable the data channel.
        ///
        /// Defaults to `true`.
        pub fn enable_data_channel(mut self, enabled: bool) -> Self {
            self.enable_data_channel = Some(enabled);
            self
        }

        /// Set the jitter buffer size in milliseconds.
        ///
        /// Defaults to `100`. Must be in the range 50-200.
        pub fn jitter_buffer_ms(mut self, ms: u32) -> Self {
            self.jitter_buffer_ms = Some(ms);
            self
        }

        /// Build and validate the server configuration.
        ///
        /// Returns an error if `executor` or `manifest` has not been set, or
        /// if the WebRTC configuration fails validation.
        pub fn build(
            self,
        ) -> std::result::Result<WebRtcSignalingServer, Box<dyn std::error::Error>> {
            let executor = self
                .executor
                .ok_or("executor is required for WebRtcSignalingServerBuilder")?;
            let manifest = self
                .manifest
                .ok_or("manifest is required for WebRtcSignalingServerBuilder")?;

            let bind_address = self
                .bind_address
                .unwrap_or_else(|| "0.0.0.0:50051".to_string());

            let mut config = WebRtcTransportConfig::default();
            config.stun_servers = self.stun_servers;
            if let Some(max) = self.max_peers {
                config.max_peers = max;
            }
            if let Some(dc) = self.enable_data_channel {
                config.enable_data_channel = dc;
            }
            if let Some(jb) = self.jitter_buffer_ms {
                config.jitter_buffer_size_ms = jb;
            }

            config.validate()?;

            let service = WebRtcSignalingService::new(Arc::new(config), executor, manifest);

            Ok(WebRtcSignalingServer {
                bind_address,
                service,
            })
        }
    }

    impl Default for WebRtcSignalingServerBuilder {
        fn default() -> Self {
            Self::new()
        }
    }

    // -----------------------------------------------------------------------
    // WebRtcSignalingServer
    // -----------------------------------------------------------------------

    /// A fully-configured gRPC signaling server ready to run.
    ///
    /// Created via [`WebRtcSignalingServerBuilder::build`].
    pub struct WebRtcSignalingServer {
        bind_address: String,
        service: WebRtcSignalingService,
    }

    impl WebRtcSignalingServer {
        /// Run the gRPC server, blocking until a ctrl-c signal is received.
        pub async fn run(self) -> std::result::Result<(), Box<dyn std::error::Error>> {
            let addr: std::net::SocketAddr = self.bind_address.parse()?;

            tracing::info!("WebRTC signaling server listening on {}", addr);

            let grpc_service = self.service.into_server();

            tonic::transport::Server::builder()
                .add_service(grpc_service)
                .serve_with_shutdown(addr, async {
                    tokio::signal::ctrl_c()
                        .await
                        .expect("failed to listen for ctrl-c");
                    tracing::info!("Shutdown signal received, stopping signaling server");
                })
                .await
                .map_err(|e| format!("Failed to start server on {}: {}", addr, e))?;

            Ok(())
        }
    }
}

#[cfg(feature = "grpc-signaling")]
pub use grpc_builder::{WebRtcSignalingServer, WebRtcSignalingServerBuilder};
