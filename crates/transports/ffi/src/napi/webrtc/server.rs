//! WebRtcServer napi binding
//!
//! Implements the Node.js WebRtcServer class with Event Emitter pattern
//! using ThreadsafeFunction for async event callbacks.

use super::config::{PeerInfo, SessionInfo, WebRtcServerConfig};
use super::events::{
    DataReceivedData, ErrorData, PeerConnectedData, PeerDisconnectedData, PipelineOutputData,
};
use crate::webrtc::core::WebRtcServerCore;
use crate::webrtc::events::WebRtcEvent;
use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use remotemedia_core::manifest::Manifest;
use remotemedia_core::transport::PipelineExecutor;
use remotemedia_webrtc::signaling::websocket::WebSocketServerHandle;
use remotemedia_webrtc::signaling::WebSocketSignalingServer;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Type aliases for callback storage
type PeerConnectedCallback = ThreadsafeFunction<PeerConnectedData, ErrorStrategy::Fatal>;
type PeerDisconnectedCallback = ThreadsafeFunction<PeerDisconnectedData, ErrorStrategy::Fatal>;
type PipelineOutputCallback = ThreadsafeFunction<PipelineOutputData, ErrorStrategy::Fatal>;
type DataCallback = ThreadsafeFunction<DataReceivedData, ErrorStrategy::Fatal>;
type ErrorCallback = ThreadsafeFunction<ErrorData, ErrorStrategy::Fatal>;

/// WebRTC server for Node.js
///
/// Provides a WebRTC media server with embedded or external signaling.
/// Uses Event Emitter pattern for callbacks.
#[napi]
pub struct WebRtcServer {
    /// Core server implementation
    core: Arc<WebRtcServerCore>,

    /// Peer connected callbacks
    on_peer_connected: Arc<Mutex<Vec<PeerConnectedCallback>>>,

    /// Peer disconnected callbacks
    on_peer_disconnected: Arc<Mutex<Vec<PeerDisconnectedCallback>>>,

    /// Pipeline output callbacks
    on_pipeline_output: Arc<Mutex<Vec<PipelineOutputCallback>>>,

    /// Raw data callbacks
    on_data: Arc<Mutex<Vec<DataCallback>>>,

    /// Error callbacks
    on_error: Arc<Mutex<Vec<ErrorCallback>>>,

    /// Event loop running flag
    event_loop_running: Arc<AtomicBool>,

    /// Event loop task handle
    event_loop_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,

    /// WebSocket signaling server handle (for explicit startSignalingServer)
    ws_server_handle: Arc<Mutex<Option<WebSocketServerHandle>>>,
}

#[napi]
impl WebRtcServer {
    /// Create a new WebRTC server with embedded signaling
    ///
    /// @param config - Server configuration with port set
    /// @returns Promise resolving to WebRtcServer instance
    #[napi(factory)]
    pub async fn create(config: WebRtcServerConfig) -> Result<Self> {
        let core_config = config.to_core_config()?;

        if core_config.port.is_none() {
            return Err(Error::from_reason(
                "Port is required for embedded signaling. Use connect() for external signaling.",
            ));
        }

        let core =
            WebRtcServerCore::new(core_config).map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(Self {
            core: Arc::new(core),
            on_peer_connected: Arc::new(Mutex::new(Vec::new())),
            on_peer_disconnected: Arc::new(Mutex::new(Vec::new())),
            on_pipeline_output: Arc::new(Mutex::new(Vec::new())),
            on_data: Arc::new(Mutex::new(Vec::new())),
            on_error: Arc::new(Mutex::new(Vec::new())),
            event_loop_running: Arc::new(AtomicBool::new(false)),
            event_loop_handle: Arc::new(Mutex::new(None)),
            ws_server_handle: Arc::new(Mutex::new(None)),
        })
    }

    /// Connect to an external signaling server
    ///
    /// @param config - Server configuration with signaling_url set
    /// @returns Promise resolving to WebRtcServer instance
    #[napi(factory)]
    pub async fn connect(config: WebRtcServerConfig) -> Result<Self> {
        let core_config = config.to_core_config()?;

        if core_config.signaling_url.is_none() {
            return Err(Error::from_reason(
                "signaling_url is required for external signaling. Use create() for embedded signaling.",
            ));
        }

        let core =
            WebRtcServerCore::new(core_config).map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(Self {
            core: Arc::new(core),
            on_peer_connected: Arc::new(Mutex::new(Vec::new())),
            on_peer_disconnected: Arc::new(Mutex::new(Vec::new())),
            on_pipeline_output: Arc::new(Mutex::new(Vec::new())),
            on_data: Arc::new(Mutex::new(Vec::new())),
            on_error: Arc::new(Mutex::new(Vec::new())),
            event_loop_running: Arc::new(AtomicBool::new(false)),
            event_loop_handle: Arc::new(Mutex::new(None)),
            ws_server_handle: Arc::new(Mutex::new(None)),
        })
    }

    /// Get the server ID
    #[napi(getter)]
    pub fn id(&self) -> String {
        self.core.id().to_string()
    }

    /// Get the current server state
    #[napi(getter)]
    pub async fn state(&self) -> String {
        let state = self.core.state().await;
        match state {
            crate::webrtc::config::ServerState::Created => "created".to_string(),
            crate::webrtc::config::ServerState::Starting => "starting".to_string(),
            crate::webrtc::config::ServerState::Running => "running".to_string(),
            crate::webrtc::config::ServerState::Stopping => "stopping".to_string(),
            crate::webrtc::config::ServerState::Stopped => "stopped".to_string(),
        }
    }

    /// Get connected peers as a Map
    #[napi]
    pub async fn get_peers(&self) -> Result<Vec<PeerInfo>> {
        let peers = self.core.peers().await;
        Ok(peers.into_values().map(Into::into).collect())
    }

    /// Get active sessions
    #[napi]
    pub async fn get_sessions(&self) -> Result<Vec<SessionInfo>> {
        let sessions = self.core.sessions().await;
        Ok(sessions.into_values().map(Into::into).collect())
    }

    /// Register an event listener
    ///
    /// @param event - Event name ('peer_connected', 'peer_disconnected', 'pipeline_output', 'data', 'error')
    /// @param callback - Event handler function
    #[napi]
    pub fn on(&self, _env: Env, event: String, callback: JsFunction) -> Result<()> {
        match event.as_str() {
            "peer_connected" => {
                let tsfn: PeerConnectedCallback = callback.create_threadsafe_function(
                    0,
                    |ctx: napi::threadsafe_function::ThreadSafeCallContext<PeerConnectedData>| {
                        Ok(vec![ctx.value])
                    },
                )?;
                let callbacks = self.on_peer_connected.clone();
                tokio::spawn(async move {
                    callbacks.lock().await.push(tsfn);
                });
            }
            "peer_disconnected" => {
                let tsfn: PeerDisconnectedCallback = callback.create_threadsafe_function(
                    0,
                    |ctx: napi::threadsafe_function::ThreadSafeCallContext<
                        PeerDisconnectedData,
                    >| { Ok(vec![ctx.value]) },
                )?;
                let callbacks = self.on_peer_disconnected.clone();
                tokio::spawn(async move {
                    callbacks.lock().await.push(tsfn);
                });
            }
            "pipeline_output" => {
                let tsfn: PipelineOutputCallback = callback.create_threadsafe_function(
                    0,
                    |ctx: napi::threadsafe_function::ThreadSafeCallContext<PipelineOutputData>| {
                        Ok(vec![ctx.value])
                    },
                )?;
                let callbacks = self.on_pipeline_output.clone();
                tokio::spawn(async move {
                    callbacks.lock().await.push(tsfn);
                });
            }
            "data" => {
                let tsfn: DataCallback = callback.create_threadsafe_function(
                    0,
                    |ctx: napi::threadsafe_function::ThreadSafeCallContext<DataReceivedData>| {
                        Ok(vec![ctx.value])
                    },
                )?;
                let callbacks = self.on_data.clone();
                tokio::spawn(async move {
                    callbacks.lock().await.push(tsfn);
                });
            }
            "error" => {
                let tsfn: ErrorCallback = callback.create_threadsafe_function(
                    0,
                    |ctx: napi::threadsafe_function::ThreadSafeCallContext<ErrorData>| {
                        Ok(vec![ctx.value])
                    },
                )?;
                let callbacks = self.on_error.clone();
                tokio::spawn(async move {
                    callbacks.lock().await.push(tsfn);
                });
            }
            _ => {
                return Err(Error::from_reason(format!(
                    "Unknown event: {}. Valid events: peer_connected, peer_disconnected, pipeline_output, data, error",
                    event
                )));
            }
        }
        Ok(())
    }

    /// Start the server
    ///
    /// For embedded mode: binds to the configured port
    /// For external mode: connects to the signaling server
    #[napi]
    pub async fn start(&self) -> Result<()> {
        // Start the core server
        self.core
            .start()
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;

        // Start the event loop
        self.start_event_loop().await?;

        Ok(())
    }

    /// Start the WebSocket signaling server explicitly
    ///
    /// This method starts the WebSocket JSON-RPC 2.0 signaling server on a
    /// dedicated thread with its own tokio runtime. It blocks until the server
    /// is confirmed to be listening on the port.
    ///
    /// @param port - Port number to listen on (e.g., 50100)
    /// @returns Promise that resolves when the server is ready to accept connections
    #[napi]
    pub async fn start_signaling_server(&self, port: u32) -> Result<()> {
        eprintln!("[NAPI] startSignalingServer called with port {}", port);

        // Check if already running
        if self.ws_server_handle.lock().await.is_some() {
            return Err(Error::from_reason("Signaling server already started"));
        }

        // Get the config from core
        let config = self.core.config();

        // Parse the pipeline manifest
        let manifest: Manifest = serde_json::from_value(config.manifest.clone())
            .map_err(|e| Error::from_reason(format!("Invalid manifest: {}", e)))?;
        let manifest = Arc::new(manifest);

        // Create PipelineExecutor
        let runner = Arc::new(PipelineExecutor::new().map_err(|e| {
            Error::from_reason(format!("Failed to create PipelineExecutor: {}", e))
        })?);

        // Build WebRTC transport configuration
        let transport_config = Arc::new(config.to_webrtc_config());

        eprintln!("[NAPI] Creating WebSocketSignalingServer on port {}", port);

        // Create event channel for forwarding events from signaling to NAPI callbacks
        let (bridge_tx, mut bridge_rx) =
            tokio::sync::mpsc::channel::<remotemedia_webrtc::signaling::WebRtcEventBridge>(1024);

        let runner_host: Arc<dyn remotemedia_core::transport::PipelineSessionHost> = runner.clone();

        // Create WebSocket signaling server WITH event forwarding
        let ws_server = WebSocketSignalingServer::new_with_events(
            port as u16,
            Arc::clone(&transport_config),
            runner_host,
            Arc::clone(&manifest),
            Some(bridge_tx),
        );

        eprintln!("[NAPI] Starting WebSocketSignalingServer...");

        // Start the WebSocket server - this returns when the server is actually listening
        let ws_handle = ws_server
            .start()
            .await
            .map_err(|e| Error::from_reason(format!("Failed to start WebSocket server: {}", e)))?;

        eprintln!(
            "[NAPI] WebSocketSignalingServer started successfully on port {}",
            port
        );

        // Store the server handle
        *self.ws_server_handle.lock().await = Some(ws_handle);

        // Spawn bridge event forwarder task
        let on_peer_connected = self.on_peer_connected.clone();
        let on_peer_disconnected = self.on_peer_disconnected.clone();
        let on_pipeline_output = self.on_pipeline_output.clone();
        let on_data = self.on_data.clone();
        let on_error = self.on_error.clone();
        let event_loop_running = self.event_loop_running.clone();

        event_loop_running.store(true, Ordering::SeqCst);

        let bridge_handle = tokio::spawn(async move {
            eprintln!("[NAPI] Event bridge task started");
            while let Some(bridge_event) = bridge_rx.recv().await {
                eprintln!(
                    "[NAPI] Received bridge event: {:?}",
                    std::mem::discriminant(&bridge_event)
                );
                match bridge_event {
                    remotemedia_webrtc::signaling::WebRtcEventBridge::PeerConnected {
                        peer_id,
                        capabilities,
                        metadata,
                    } => {
                        eprintln!(
                            "[NAPI] Processing PeerConnected event for peer: {}",
                            peer_id
                        );
                        let data = PeerConnectedData {
                            peer_id: peer_id.clone(),
                            capabilities: super::config::PeerCapabilities {
                                audio: capabilities.contains(&"audio".to_string()),
                                video: capabilities.contains(&"video".to_string()),
                                data: capabilities.contains(&"data".to_string()),
                            },
                            metadata: metadata
                                .iter()
                                .map(|(k, v)| (k.clone(), v.to_string()))
                                .collect(),
                        };
                        let callbacks = on_peer_connected.lock().await;
                        eprintln!(
                            "[NAPI] Calling {} peer_connected callbacks",
                            callbacks.len()
                        );
                        for callback in callbacks.iter() {
                            let _ = callback.call(
                                data.clone(),
                                napi::threadsafe_function::ThreadsafeFunctionCallMode::Blocking,
                            );
                        }
                    }
                    remotemedia_webrtc::signaling::WebRtcEventBridge::PeerDisconnected {
                        peer_id,
                        reason,
                    } => {
                        eprintln!(
                            "[NAPI] Processing PeerDisconnected event for peer: {}",
                            peer_id
                        );
                        let data = PeerDisconnectedData {
                            peer_id: peer_id.clone(),
                            reason,
                        };
                        let callbacks = on_peer_disconnected.lock().await;
                        eprintln!(
                            "[NAPI] Calling {} peer_disconnected callbacks",
                            callbacks.len()
                        );
                        for callback in callbacks.iter() {
                            let _ = callback.call(
                                data.clone(),
                                napi::threadsafe_function::ThreadsafeFunctionCallMode::Blocking,
                            );
                        }
                    }
                    remotemedia_webrtc::signaling::WebRtcEventBridge::PipelineOutput {
                        peer_id,
                        data_json,
                        timestamp_ns,
                    } => {
                        // Parse the data_json to extract data type
                        let (data_type, data_str) = if let Ok(runtime_data) =
                            serde_json::from_str::<remotemedia_core::data::RuntimeData>(&data_json)
                        {
                            use remotemedia_core::data::RuntimeData;
                            match &runtime_data {
                                RuntimeData::Audio { samples, sample_rate, channels, .. } => (
                                    "audio".to_string(),
                                    serde_json::json!({ "sample_rate": sample_rate, "channels": channels, "num_samples": samples.len() }).to_string(),
                                ),
                                RuntimeData::Text(content) => ("text".to_string(), serde_json::json!({ "content": content }).to_string()),
                                RuntimeData::Video { width, height, format, .. } => (
                                    "video".to_string(),
                                    serde_json::json!({ "width": width, "height": height, "format": format!("{:?}", format) }).to_string(),
                                ),
                                RuntimeData::Binary(data) => ("binary".to_string(), serde_json::json!({ "size": data.len() }).to_string()),
                                RuntimeData::Json(value) => ("json".to_string(), value.to_string()),
                                _ => ("unknown".to_string(), "{}".to_string()),
                            }
                        } else {
                            ("unknown".to_string(), data_json)
                        };
                        let data = PipelineOutputData {
                            peer_id: peer_id.clone(),
                            data_type,
                            data: data_str,
                            timestamp: timestamp_ns as f64,
                        };
                        let callbacks = on_pipeline_output.lock().await;
                        for callback in callbacks.iter() {
                            let _ = callback.call(
                                data.clone(),
                                napi::threadsafe_function::ThreadsafeFunctionCallMode::Blocking,
                            );
                        }
                    }
                    remotemedia_webrtc::signaling::WebRtcEventBridge::DataReceived {
                        peer_id,
                        data,
                        timestamp_ns,
                    } => {
                        let data = DataReceivedData {
                            peer_id: peer_id.clone(),
                            size: data.len() as u32,
                            timestamp: timestamp_ns as f64,
                        };
                        let callbacks = on_data.lock().await;
                        for callback in callbacks.iter() {
                            let _ = callback.call(
                                data.clone(),
                                napi::threadsafe_function::ThreadsafeFunctionCallMode::Blocking,
                            );
                        }
                    }
                    remotemedia_webrtc::signaling::WebRtcEventBridge::Error {
                        code,
                        message,
                        peer_id,
                    } => {
                        let error_code = match code {
                            remotemedia_webrtc::signaling::WebRtcErrorCode::SignalingError => {
                                "signaling_error"
                            }
                            remotemedia_webrtc::signaling::WebRtcErrorCode::PeerError => {
                                "peer_error"
                            }
                            remotemedia_webrtc::signaling::WebRtcErrorCode::PipelineError => {
                                "pipeline_error"
                            }
                            remotemedia_webrtc::signaling::WebRtcErrorCode::InternalError => {
                                "internal_error"
                            }
                        };
                        let data = ErrorData {
                            code: error_code.to_string(),
                            message,
                            peer_id,
                        };
                        let callbacks = on_error.lock().await;
                        for callback in callbacks.iter() {
                            let _ = callback.call(
                                data.clone(),
                                napi::threadsafe_function::ThreadsafeFunctionCallMode::Blocking,
                            );
                        }
                    }
                }
            }
            eprintln!("[NAPI] Event bridge task ended");
        });

        // Store the bridge handle (so it can be cancelled on shutdown)
        *self.event_loop_handle.lock().await = Some(bridge_handle);

        Ok(())
    }

    /// Stop the server gracefully
    ///
    /// Disconnects all peers and cleans up resources.
    #[napi]
    pub async fn shutdown(&self) -> Result<()> {
        // Stop event loop
        self.event_loop_running.store(false, Ordering::SeqCst);

        // Wait for event loop to finish
        if let Some(handle) = self.event_loop_handle.lock().await.take() {
            let _ = handle.await;
        }

        // Shutdown explicit signaling server if running
        if let Some(handle) = self.ws_server_handle.lock().await.take() {
            eprintln!("[NAPI] Shutting down explicit signaling server...");
            handle.shutdown().await;
            eprintln!("[NAPI] Explicit signaling server shut down");
        }

        // Shutdown core
        self.core
            .shutdown()
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(())
    }

    /// Send data to a specific peer
    ///
    /// @param peer_id - Target peer ID
    /// @param data - Data to send (Buffer)
    #[napi]
    pub async fn send_to_peer(&self, peer_id: String, data: Buffer) -> Result<()> {
        self.core
            .send_to_peer(&peer_id, data.to_vec())
            .await
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Broadcast data to all connected peers
    ///
    /// @param data - Data to broadcast (Buffer)
    #[napi]
    pub async fn broadcast(&self, data: Buffer) -> Result<()> {
        self.core
            .broadcast(data.to_vec())
            .await
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Disconnect a specific peer
    ///
    /// @param peer_id - Peer to disconnect
    /// @param reason - Optional disconnect reason
    #[napi]
    pub async fn disconnect_peer(&self, peer_id: String, reason: Option<String>) -> Result<()> {
        self.core
            .disconnect_peer(&peer_id, reason.as_deref())
            .await
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    /// Create a new session (room) and return a WebRtcSession object
    ///
    /// @param session_id - Unique session identifier
    /// @param metadata - Optional session metadata as key-value pairs
    /// @returns WebRtcSession instance for managing the room
    #[napi]
    pub async fn create_session(
        &self,
        session_id: String,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<crate::napi::webrtc::session::WebRtcSession> {
        // Create the session in the core
        let _session = self
            .core
            .create_session(&session_id, metadata)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;

        // Return a WebRtcSession wrapper
        Ok(crate::napi::webrtc::session::WebRtcSession::new(
            self.core.clone(),
            session_id,
        ))
    }

    /// Create a new session (room) and return SessionInfo
    ///
    /// @param session_id - Unique session identifier
    /// @param metadata - Optional session metadata as key-value pairs
    /// @returns SessionInfo with session details
    #[napi]
    pub async fn create_session_info(
        &self,
        session_id: String,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<SessionInfo> {
        let session = self
            .core
            .create_session(&session_id, metadata)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;

        Ok(session.into())
    }

    /// Get an existing session
    ///
    /// @param session_id - Session identifier
    /// @returns SessionInfo or undefined
    #[napi]
    pub async fn get_session(&self, session_id: String) -> Result<Option<SessionInfo>> {
        Ok(self.core.get_session(&session_id).await.map(Into::into))
    }

    /// Delete a session
    ///
    /// @param session_id - Session to delete
    #[napi]
    pub async fn delete_session(&self, session_id: String) -> Result<()> {
        self.core
            .delete_session(&session_id)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    // ============ Internal methods ============

    async fn start_event_loop(&self) -> Result<()> {
        if self.event_loop_running.load(Ordering::SeqCst) {
            return Ok(()); // Already running
        }

        // Take the event receiver
        let event_rx = self
            .core
            .take_event_receiver()
            .await
            .ok_or_else(|| Error::from_reason("Event receiver already taken"))?;

        self.event_loop_running.store(true, Ordering::SeqCst);

        // Clone references for the event loop
        let running = self.event_loop_running.clone();
        let on_peer_connected = self.on_peer_connected.clone();
        let on_peer_disconnected = self.on_peer_disconnected.clone();
        let on_pipeline_output = self.on_pipeline_output.clone();
        let on_data = self.on_data.clone();
        let on_error = self.on_error.clone();

        let handle = tokio::spawn(async move {
            let mut event_rx = event_rx;

            while running.load(Ordering::SeqCst) {
                tokio::select! {
                    biased;

                    // Periodic check for shutdown (100ms timeout)
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                        // Just check the running flag
                        continue;
                    }
                    Some(event) = event_rx.recv() => {
                        match event {
                            WebRtcEvent::PeerConnected(e) => {
                                let data: PeerConnectedData = e.into();
                                let callbacks = on_peer_connected.lock().await;
                                for tsfn in callbacks.iter() {
                                    let _ = tsfn.call(data.clone(), ThreadsafeFunctionCallMode::NonBlocking);
                                }
                            }
                            WebRtcEvent::PeerDisconnected(e) => {
                                let data: PeerDisconnectedData = e.into();
                                let callbacks = on_peer_disconnected.lock().await;
                                for tsfn in callbacks.iter() {
                                    let _ = tsfn.call(data.clone(), ThreadsafeFunctionCallMode::NonBlocking);
                                }
                            }
                            WebRtcEvent::PipelineOutput(e) => {
                                let data: PipelineOutputData = e.into();
                                let callbacks = on_pipeline_output.lock().await;
                                for tsfn in callbacks.iter() {
                                    let _ = tsfn.call(data.clone(), ThreadsafeFunctionCallMode::NonBlocking);
                                }
                            }
                            WebRtcEvent::DataReceived(e) => {
                                let data: DataReceivedData = e.into();
                                let callbacks = on_data.lock().await;
                                for tsfn in callbacks.iter() {
                                    let _ = tsfn.call(data.clone(), ThreadsafeFunctionCallMode::NonBlocking);
                                }
                            }
                            WebRtcEvent::Error(e) => {
                                let data: ErrorData = e.into();
                                let callbacks = on_error.lock().await;
                                for tsfn in callbacks.iter() {
                                    let _ = tsfn.call(data.clone(), ThreadsafeFunctionCallMode::NonBlocking);
                                }
                            }
                            WebRtcEvent::Session(_) => {
                                // Session events are handled at the session level
                            }
                        }
                    }
                    else => {
                        break;
                    }
                }
            }

            tracing::debug!("WebRTC event loop stopped");
        });

        *self.event_loop_handle.lock().await = Some(handle);

        Ok(())
    }
}
