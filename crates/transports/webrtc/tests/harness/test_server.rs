//! Embedded test server for WebRTC E2E testing
//!
//! Wraps the gRPC signaling server for use in integration tests.

use super::{HarnessError, HarnessResult};
use remotemedia_core::manifest::Manifest;
use remotemedia_core::transport::{PipelineExecutor, PipelineSessionHost};
use remotemedia_webrtc::signaling::WebRtcSignalingService;
use remotemedia_webrtc::WebRtcTransportConfig;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, RwLock};
use tonic::transport::Server;
use tracing::{error, info};

/// Embedded gRPC signaling server for testing
pub struct TestServer {
    /// Server address (127.0.0.1:port)
    addr: SocketAddr,

    /// Shutdown signal sender
    shutdown_tx: mpsc::Sender<()>,

    /// Server task handle
    server_handle: tokio::task::JoinHandle<()>,

    /// Running flag
    running: Arc<AtomicBool>,

    /// Shared server-side pipeline session ID, populated after the first peer offer.
    shared_pipeline_session_id: Arc<RwLock<Option<String>>>,
}

impl TestServer {
    /// Create and start a new test server on a random available port
    ///
    /// # Arguments
    ///
    /// * `manifest` - Pipeline manifest for processing
    ///
    /// # Returns
    ///
    /// Running server instance
    pub async fn new(manifest: Manifest) -> HarnessResult<Self> {
        // Bind to random port
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| HarnessError::ServerError(format!("Failed to bind: {}", e)))?;

        let addr = listener
            .local_addr()
            .map_err(|e| HarnessError::ServerError(format!("Failed to get local addr: {}", e)))?;

        info!("Test server binding to {}", addr);

        // Create shutdown channel
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        let running = Arc::new(AtomicBool::new(true));
        let running_clone = Arc::clone(&running);

        // Create pipeline runner
        let runner: Arc<dyn PipelineSessionHost> =
            Arc::new(PipelineExecutor::new().map_err(|e| {
                HarnessError::ServerError(format!("Failed to create PipelineExecutor: {}", e))
            })?);

        // Create WebRTC config
        let config = Arc::new(WebRtcTransportConfig {
            stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
            max_peers: 100,
            enable_data_channel: true,
            ..Default::default()
        });

        // Create signaling service
        let manifest = Arc::new(manifest);
        let signaling_service = WebRtcSignalingService::new(
            Arc::clone(&config),
            Arc::clone(&runner),
            Arc::clone(&manifest),
        );

        // Register virtual server peer
        let (server_peer_tx, mut _server_peer_rx) = mpsc::channel(128);
        signaling_service.register_server_peer(server_peer_tx).await;
        let shared_pipeline_session_id = signaling_service.shared_pipeline_session_id_handle();

        let grpc_service = signaling_service.into_server();

        // Convert TcpListener to incoming stream
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        // Spawn server task
        let server_handle = tokio::spawn(async move {
            info!("Starting test gRPC server");

            let shutdown_future = async move {
                let _ = shutdown_rx.recv().await;
                info!("Test server received shutdown signal");
            };

            let result = Server::builder()
                .add_service(grpc_service)
                .serve_with_incoming_shutdown(incoming, shutdown_future)
                .await;

            running_clone.store(false, Ordering::SeqCst);

            if let Err(e) = result {
                error!("Test server error: {}", e);
            }

            info!("Test server stopped");
        });

        // Wait briefly for server to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        Ok(Self {
            addr,
            shutdown_tx,
            server_handle,
            running,
            shared_pipeline_session_id,
        })
    }

    /// Get the server port
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Get the full server address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Check if server is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Return the shared server-side pipeline session ID, if it has been created.
    pub async fn shared_pipeline_session_id(&self) -> Option<String> {
        self.shared_pipeline_session_id.read().await.clone()
    }

    /// Wait for server to be ready (performs health check)
    pub async fn wait_for_ready(&self, timeout: Duration) -> HarnessResult<()> {
        use remotemedia_webrtc::generated::webrtc::{
            web_rtc_signaling_client::WebRtcSignalingClient, HealthCheckRequest,
        };

        let deadline = tokio::time::Instant::now() + timeout;
        let addr = format!("http://{}", self.addr);

        while tokio::time::Instant::now() < deadline {
            match WebRtcSignalingClient::connect(addr.clone()).await {
                Ok(mut client) => match client.health_check(HealthCheckRequest {}).await {
                    Ok(response) => {
                        let health = response.into_inner();
                        if health.status == "healthy" {
                            info!("Test server is ready (uptime: {}s)", health.uptime_seconds);
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        tracing::debug!("Health check failed: {}, retrying...", e);
                    }
                },
                Err(e) => {
                    tracing::debug!("Connection failed: {}, retrying...", e);
                }
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Err(HarnessError::Timeout(format!(
            "Server not ready after {:?}",
            timeout
        )))
    }

    /// Shutdown the server gracefully
    pub async fn shutdown(&self) -> HarnessResult<()> {
        info!("Shutting down test server");

        // Send shutdown signal
        let _ = self.shutdown_tx.send(()).await;

        // Wait for server to stop (with timeout)
        let timeout = Duration::from_secs(5);
        let deadline = tokio::time::Instant::now() + timeout;

        while self.is_running() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        if self.is_running() {
            return Err(HarnessError::Timeout(
                "Server did not shut down in time".to_string(),
            ));
        }

        Ok(())
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Abort server task if still running
        if self.is_running() {
            self.server_handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::PASSTHROUGH_MANIFEST;

    #[tokio::test]
    async fn test_server_starts_and_binds() {
        let manifest: Manifest = serde_json::from_str(PASSTHROUGH_MANIFEST).unwrap();
        let server = TestServer::new(manifest).await.unwrap();

        assert!(server.port() > 0);
        assert!(server.is_running());

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_server_health_check() {
        let manifest: Manifest = serde_json::from_str(PASSTHROUGH_MANIFEST).unwrap();
        let server = TestServer::new(manifest).await.unwrap();

        let result = server.wait_for_ready(Duration::from_secs(5)).await;
        assert!(result.is_ok());

        server.shutdown().await.unwrap();
    }
}
