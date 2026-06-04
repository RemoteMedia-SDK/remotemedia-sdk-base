//! WebRTC End-to-End Test Harness
//!
//! Provides infrastructure for integration testing of WebRTC pipelines with:
//! - Embedded gRPC signaling server on random port
//! - Multiple concurrent test clients
//! - Synthetic media generation (audio/video)
//! - Output validation helpers
//!
//! # Example
//!
//! See the integration tests in `tests/` for complete usage examples.
//!
//! Basic usage pattern:
//!
//! 1. Create a `WebRtcTestHarness` with a pipeline manifest
//! 2. Create test clients using `harness.create_client()`
//! 3. Send media data using client methods
//! 4. Validate outputs using harness assertion helpers
//! 5. Call `harness.shutdown()` to clean up

pub mod media;
pub mod test_client;
pub mod test_server;
pub mod validator;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use remotemedia_core::manifest::Manifest;
use tokio::sync::RwLock;
use tracing::{info, warn};

pub use media::MediaGenerator;
pub use test_client::TestClient;
pub use test_server::TestServer;
pub use validator::OutputValidator;

/// Result type for test harness operations
pub type HarnessResult<T> = Result<T, HarnessError>;

/// Error type for test harness operations
#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    #[error("Server error: {0}")]
    ServerError(String),

    #[error("Client error: {0}")]
    ClientError(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Media error: {0}")]
    MediaError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

/// Main WebRTC test harness
///
/// Orchestrates server and multiple clients for end-to-end testing.
pub struct WebRtcTestHarness {
    /// Embedded test server
    server: Arc<TestServer>,

    /// Connected test clients (peer_id -> client)
    clients: Arc<RwLock<HashMap<String, Arc<TestClient>>>>,

    /// Media generator for synthetic test data
    pub media_generator: MediaGenerator,

    /// Output validator for assertions
    output_validator: OutputValidator,

    /// Client counter for auto-generated peer IDs
    client_counter: Arc<RwLock<u32>>,
}

impl WebRtcTestHarness {
    /// Create a new test harness with the given manifest
    ///
    /// # Arguments
    ///
    /// * `manifest_json` - Pipeline manifest as JSON string
    ///
    /// # Returns
    ///
    /// Initialized harness with server running on random port
    pub async fn new(manifest_json: &str) -> HarnessResult<Self> {
        info!("Creating WebRTC test harness");

        // Parse manifest
        let manifest: Manifest = serde_json::from_str(manifest_json)
            .map_err(|e| HarnessError::ServerError(format!("Invalid manifest: {}", e)))?;

        // Create and start server
        let server = TestServer::new(manifest).await?;
        let server = Arc::new(server);

        info!("Test server started on port {}", server.port());

        Ok(Self {
            server,
            clients: Arc::new(RwLock::new(HashMap::new())),
            media_generator: MediaGenerator::new(),
            output_validator: OutputValidator::new(),
            client_counter: Arc::new(RwLock::new(0)),
        })
    }

    /// Create a new test harness with manifest from file
    pub async fn from_file(manifest_path: &str) -> HarnessResult<Self> {
        let manifest_json = tokio::fs::read_to_string(manifest_path)
            .await
            .map_err(|e| HarnessError::IoError(e))?;
        Self::new(&manifest_json).await
    }

    /// Get the server port
    pub fn server_port(&self) -> u16 {
        self.server.port()
    }

    /// Get the server address (e.g., "http://127.0.0.1:12345")
    pub fn server_addr(&self) -> String {
        format!("http://127.0.0.1:{}", self.server.port())
    }

    /// Create and connect a new test client
    ///
    /// # Arguments
    ///
    /// * `peer_id` - Unique identifier for this client
    ///
    /// # Returns
    ///
    /// Connected test client ready to send/receive media
    pub async fn create_client(&self, peer_id: &str) -> HarnessResult<Arc<TestClient>> {
        info!("Creating test client: {}", peer_id);

        let client = TestClient::new(peer_id.to_string(), self.server.port()).await?;
        let client = Arc::new(client);

        // Connect to server
        client.connect().await?;

        // Store client
        self.clients
            .write()
            .await
            .insert(peer_id.to_string(), Arc::clone(&client));

        info!("Test client {} connected successfully", peer_id);

        Ok(client)
    }

    /// Create a client with auto-generated peer ID
    pub async fn create_client_auto(&self) -> HarnessResult<Arc<TestClient>> {
        let mut counter = self.client_counter.write().await;
        *counter += 1;
        let peer_id = format!("test-client-{}", *counter);
        drop(counter);

        self.create_client(&peer_id).await
    }

    /// Create multiple clients concurrently
    ///
    /// # Arguments
    ///
    /// * `count` - Number of clients to create
    ///
    /// # Returns
    ///
    /// Vector of connected test clients
    pub async fn create_clients(&self, count: usize) -> HarnessResult<Vec<Arc<TestClient>>> {
        let mut handles = Vec::with_capacity(count);

        for i in 0..count {
            let peer_id = format!("test-client-{}", i + 1);
            let port = self.server.port();

            handles.push(tokio::spawn(async move {
                let client = TestClient::new(peer_id.clone(), port).await?;
                let client = Arc::new(client);
                client.connect().await?;
                Ok::<_, HarnessError>(client)
            }));
        }

        let mut clients = Vec::with_capacity(count);
        for handle in handles {
            let client = handle
                .await
                .map_err(|e| HarnessError::ClientError(format!("Join error: {}", e)))??;
            let peer_id = client.peer_id().to_string();
            self.clients
                .write()
                .await
                .insert(peer_id, Arc::clone(&client));
            clients.push(client);
        }

        info!("Created {} test clients", clients.len());

        Ok(clients)
    }

    /// Get a client by peer ID
    pub async fn get_client(&self, peer_id: &str) -> Option<Arc<TestClient>> {
        self.clients.read().await.get(peer_id).cloned()
    }

    /// Get all connected clients
    pub async fn get_all_clients(&self) -> Vec<Arc<TestClient>> {
        self.clients.read().await.values().cloned().collect()
    }

    /// Get number of connected clients
    pub async fn client_count(&self) -> usize {
        self.clients.read().await.len()
    }

    /// Return the server-side shared pipeline session ID, if any client has attached.
    pub async fn shared_pipeline_session_id(&self) -> Option<String> {
        self.server.shared_pipeline_session_id().await
    }

    /// Remove and disconnect a client
    pub async fn remove_client(&self, peer_id: &str) -> HarnessResult<()> {
        if let Some(client) = self.clients.write().await.remove(peer_id) {
            client.disconnect().await?;
        }
        Ok(())
    }

    // ========================================================================
    // Media Generation (delegated to MediaGenerator)
    // ========================================================================

    /// Generate a sine wave audio signal
    pub fn generate_sine_wave(
        &self,
        frequency: f32,
        duration_secs: f32,
        sample_rate: u32,
    ) -> Vec<f32> {
        self.media_generator
            .generate_sine_wave(frequency, duration_secs, sample_rate)
    }

    /// Generate silence
    pub fn generate_silence(&self, duration_secs: f32, sample_rate: u32) -> Vec<f32> {
        self.media_generator
            .generate_silence(duration_secs, sample_rate)
    }

    /// Generate a solid color video frame (YUV420P)
    pub fn generate_solid_frame(&self, width: u32, height: u32, y: u8, u: u8, v: u8) -> Vec<u8> {
        self.media_generator
            .generate_solid_frame(width, height, y, u, v)
    }

    /// Generate a test pattern video frame
    pub fn generate_test_pattern(&self, width: u32, height: u32) -> Vec<u8> {
        self.media_generator.generate_test_pattern(width, height)
    }

    // ========================================================================
    // Output Validation (delegated to OutputValidator)
    // ========================================================================

    /// Wait for audio output from a client
    pub async fn expect_audio_output(
        &self,
        client: &TestClient,
        timeout: Duration,
    ) -> HarnessResult<Vec<u8>> {
        self.output_validator
            .expect_audio_output(client, timeout)
            .await
    }

    /// Wait for video output from a client
    pub async fn expect_video_output(
        &self,
        client: &TestClient,
        timeout: Duration,
    ) -> HarnessResult<test_client::ReceivedVideoFrame> {
        self.output_validator
            .expect_video_output(client, timeout)
            .await
    }

    /// Assert that two audio signals are similar within tolerance
    pub fn assert_audio_similar(&self, a: &[f32], b: &[f32], tolerance: f32) -> HarnessResult<()> {
        self.output_validator.assert_audio_similar(a, b, tolerance)
    }

    /// Assert video frame dimensions
    pub fn assert_frame_dimensions(
        &self,
        frame: &[u8],
        width: u32,
        height: u32,
    ) -> HarnessResult<()> {
        self.output_validator
            .assert_frame_dimensions(frame, width, height)
    }

    // ========================================================================
    // Lifecycle Management
    // ========================================================================

    /// Shutdown all clients and the server
    pub async fn shutdown(&self) {
        info!("Shutting down test harness");

        // Disconnect all clients
        let clients: Vec<_> = self.clients.write().await.drain().collect();
        for (peer_id, client) in clients {
            if let Err(e) = client.disconnect().await {
                warn!("Error disconnecting client {}: {}", peer_id, e);
            }
        }

        // Stop server
        if let Err(e) = self.server.shutdown().await {
            warn!("Error shutting down server: {}", e);
        }

        info!("Test harness shutdown complete");
    }

    /// Wait for server to be ready (health check)
    pub async fn wait_for_server_ready(&self, timeout: Duration) -> HarnessResult<()> {
        self.server.wait_for_ready(timeout).await
    }
}

impl Drop for WebRtcTestHarness {
    fn drop(&mut self) {
        // Note: async drop not supported, clients should call shutdown() explicitly
        tracing::debug!("WebRtcTestHarness dropped");
    }
}

// ============================================================================
// Common Test Manifests
// ============================================================================

/// Simple passthrough manifest - input goes directly to output
pub const PASSTHROUGH_MANIFEST: &str = r#"
{
    "version": "1.0",
    "metadata": {
        "name": "passthrough",
        "description": "Simple passthrough for testing"
    },
    "nodes": [
        {
            "id": "passthrough",
            "node_type": "PassThrough",
            "params": {}
        }
    ],
    "connections": []
}
"#;

/// Audio-only loopback manifest
pub const AUDIO_LOOPBACK_MANIFEST: &str = r#"
{
    "version": "1.0",
    "metadata": {
        "name": "audio-loopback",
        "description": "Audio loopback for testing"
    },
    "nodes": [
        {
            "id": "passthrough",
            "node_type": "PassThrough",
            "params": {}
        }
    ],
    "connections": []
}
"#;

/// VAD pipeline manifest
pub const VAD_MANIFEST: &str = r#"
{
    "version": "1.0",
    "metadata": {
        "name": "vad-test",
        "description": "VAD pipeline for testing"
    },
    "nodes": [
        {
            "id": "vad",
            "node_type": "SileroVADNode",
            "params": {
                "threshold": 0.5,
                "sample_rate": 16000
            }
        }
    ],
    "connections": []
}
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_passthrough_manifest_parses() {
        let manifest: Manifest = serde_json::from_str(PASSTHROUGH_MANIFEST).unwrap();
        assert_eq!(manifest.metadata.name, "passthrough");
    }

    #[test]
    fn test_vad_manifest_parses() {
        let manifest: Manifest = serde_json::from_str(VAD_MANIFEST).unwrap();
        assert_eq!(manifest.metadata.name, "vad-test");
    }
}
