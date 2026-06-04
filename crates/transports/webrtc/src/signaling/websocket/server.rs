//! WebSocket signaling server
//!
//! Provides a WebSocket server for JSON-RPC 2.0 signaling protocol.
//!
//! This server runs on a dedicated thread with its own tokio runtime to ensure
//! the accept loop is always polled, even when used from FFI contexts like
//! napi-rs where the main runtime may not continuously poll spawned tasks.

use super::events::WebRtcEventBridge;
use super::handler::{handle_connection, SharedState};
use crate::config::WebRtcTransportConfig;
use remotemedia_core::{manifest::Manifest, transport::{PipelineExecutor, PipelineSessionHost}};
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{error, info};

/// WebSocket signaling server
pub struct WebSocketSignalingServer {
    /// Server address
    addr: SocketAddr,

    /// Shared state
    state: Arc<SharedState>,
}

impl WebSocketSignalingServer {
    /// Create a new WebSocket signaling server without event forwarding
    pub fn new(
        port: u16,
        config: Arc<WebRtcTransportConfig>,
        runner: Arc<dyn PipelineSessionHost>,
        manifest: Arc<Manifest>,
    ) -> Self {
        Self::new_with_events(port, config, runner, manifest, None)
    }

    /// Create a new WebSocket signaling server with event forwarding
    ///
    /// When `event_tx` is provided, peer connect/disconnect and pipeline events
    /// are forwarded through the channel for FFI integration.
    pub fn new_with_events(
        port: u16,
        config: Arc<WebRtcTransportConfig>,
        runner: Arc<dyn PipelineSessionHost>,
        manifest: Arc<Manifest>,
        event_tx: Option<mpsc::Sender<WebRtcEventBridge>>,
    ) -> Self {
        let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
        let state = Arc::new(SharedState::new_with_events(
            config, runner, manifest, event_tx,
        ));

        Self { addr, state }
    }

    /// Get shared state (for external access)
    pub fn state(&self) -> Arc<SharedState> {
        Arc::clone(&self.state)
    }

    /// Start the WebSocket server
    ///
    /// Spawns a dedicated thread with its own tokio runtime to ensure the accept
    /// loop is always polled. Returns a handle that can be used to shut down the server.
    pub async fn start(self) -> Result<WebSocketServerHandle, std::io::Error> {
        let addr = self.addr;
        let state = Arc::clone(&self.state);

        // Channel to receive the startup result
        let (startup_tx, startup_rx) = oneshot::channel::<Result<(), std::io::Error>>();

        // Shutdown channel
        let (shutdown_tx, _) = broadcast::channel::<()>(1);
        let shutdown_tx_clone = shutdown_tx.clone();

        eprintln!(
            "[WS-SERVER] Starting WebSocket signaling server thread for {}",
            addr
        );
        info!("Starting WebSocket signaling server thread for {}", addr);

        // Spawn a dedicated thread with its own tokio runtime
        let thread_handle = thread::spawn(move || {
            eprintln!("[WS-SERVER] Thread spawned, creating tokio runtime...");

            // Create a new tokio runtime for this thread
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("[WS-SERVER] Failed to create tokio runtime: {}", e);
                    let _ = startup_tx.send(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to create tokio runtime: {}", e),
                    )));
                    return;
                }
            };

            eprintln!("[WS-SERVER] Tokio runtime created, entering block_on...");
            rt.block_on(async move {
                eprintln!("[WS-SERVER] Inside block_on, binding to {}", addr);
                info!("WebSocket server thread started, binding to {}", addr);

                // Bind the TCP listener
                let listener = match TcpListener::bind(&addr).await {
                    Ok(l) => {
                        eprintln!("[WS-SERVER] TcpListener::bind succeeded!");
                        l
                    },
                    Err(e) => {
                        eprintln!("[WS-SERVER] TcpListener::bind FAILED: {}", e);
                        error!("Failed to bind WebSocket server to {}: {}", addr, e);
                        let _ = startup_tx.send(Err(e));
                        return;
                    }
                };

                let local_addr = match listener.local_addr() {
                    Ok(a) => a,
                    Err(e) => {
                        let _ = startup_tx.send(Err(e));
                        return;
                    }
                };

                eprintln!("[WS-SERVER] Bound and listening on ws://{}/ws", local_addr);
                info!(
                    "WebSocket signaling server bound and listening on ws://{}/ws",
                    local_addr
                );

                // Signal successful startup
                eprintln!("[WS-SERVER] Sending startup success signal...");
                let _ = startup_tx.send(Ok(()));

                // Subscribe to shutdown
                let mut shutdown_rx = shutdown_tx_clone.subscribe();

                // Accept loop
                loop {
                    tokio::select! {
                        result = listener.accept() => {
                            match result {
                                Ok((stream, peer_addr)) => {
                                    info!("Accepted WebSocket connection from {}", peer_addr);
                                    let state_clone = Arc::clone(&state);
                                    tokio::spawn(async move {
                                        if let Err(e) = handle_connection(stream, state_clone).await {
                                            error!("WebSocket connection error from {}: {}", peer_addr, e);
                                        }
                                    });
                                }
                                Err(e) => {
                                    error!("Failed to accept WebSocket connection: {}", e);
                                }
                            }
                        }
                        _ = shutdown_rx.recv() => {
                            info!("WebSocket signaling server received shutdown signal");
                            break;
                        }
                    }
                }

                info!("WebSocket signaling server accept loop exited");
            });

            info!("WebSocket server thread exiting");
        });

        // Wait for startup to complete
        match startup_rx.await {
            Ok(Ok(())) => {
                info!("WebSocket signaling server started successfully");
                Ok(WebSocketServerHandle {
                    thread: Some(thread_handle),
                    shutdown_tx,
                })
            }
            Ok(Err(e)) => {
                error!("WebSocket signaling server failed to start: {}", e);
                Err(e)
            }
            Err(_) => {
                error!("WebSocket signaling server startup channel closed unexpectedly");
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Server startup channel closed unexpectedly",
                ))
            }
        }
    }

    /// Get the server address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

/// Handle for controlling the WebSocket server
pub struct WebSocketServerHandle {
    thread: Option<thread::JoinHandle<()>>,
    shutdown_tx: broadcast::Sender<()>,
}

impl WebSocketServerHandle {
    /// Shut down the server
    pub async fn shutdown(mut self) {
        info!("Sending shutdown signal to WebSocket server");
        let _ = self.shutdown_tx.send(());

        // Wait for the thread to finish
        if let Some(handle) = self.thread.take() {
            info!("Waiting for WebSocket server thread to exit...");
            let _ = handle.join();
            info!("WebSocket server thread exited");
        }
    }
}
