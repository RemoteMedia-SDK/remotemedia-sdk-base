//! Multiprocess executor for Python nodes
//!
//! Implements NodeExecutor trait to manage Python nodes running in separate processes
//! with iceoryx2 shared memory IPC for zero-copy data transfer.

use crate::executor::node_executor::{
    NodeContext as ExecutorNodeContext, NodeExecutor as ExecutorNodeExecutor,
};
use crate::nodes::{NodeContext as NodesNodeContext, NodeExecutor as NodesNodeExecutor};
use crate::{Error, Result};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock;

#[cfg(feature = "multiprocess")]
use super::data_transfer::{from_python_wire, to_python_wire};
#[cfg(feature = "multiprocess")]
use super::health_monitor::{HealthMonitor, ProcessEvent};
#[cfg(feature = "multiprocess")]
use super::ipc_channel::{ChannelHandle, ChannelRegistry};
#[cfg(feature = "multiprocess")]
use super::process_manager::{ExitReason, ProcessHandle, ProcessManager};

#[cfg(feature = "docker")]
use super::docker_support::DockerSupport;

// NOTE: Publisher caching attempts and why they failed:
//
// ATTEMPT 1: Global static cache with Mutex
//   - Problem: Publisher contains Rc<> which is !Send + !Sync
//   - Error: "cannot be sent/shared between threads safely"
//   - Code: static PUBLISHER_CACHE: OnceLock<Mutex<HashMap<String, Publisher>>> = ...
//
// ATTEMPT 2: Thread-local storage (TLS)
//   - Problem: Publisher has lifetime tied to ChannelRegistry (&'a ChannelRegistry)
//   - Error: "does not live long enough" - registry lives in spawn_blocking closure
//   - Publishers can't outlive the registry that created them
//   - Code: thread_local! { static PUBLISHER_CACHE: RefCell<HashMap<...>> = ... }
//
// ATTEMPT 3: Store publishers in SessionState
//   - Problem: Publisher is !Send, can't be stored in async-accessible SessionState
//   - Would require wrapping in unsafe or redesigning entire session storage
//
// ROOT CAUSE:
// iceoryx2::Publisher is !Send + !Sync and has lifetime bounds to its registry.
// This makes it impossible to cache in Rust's standard patterns (static, TLS, Arc).
//
// CURRENT SOLUTION:
// Create a new publisher for each send with a 50ms delay for iceoryx2 routing.
// This is correct but adds latency. Future optimization would require:
// - Restructuring to create publishers during initialization
// - Storing them in a non-Send-friendly way (e.g., blocking thread with channels)
// - Or contributing to iceoryx2 to make Publisher Send+Sync

/// Global session storage
/// Maps session_id -> HashMap<node_id, IPC thread command sender>
/// This is shared across all MultiprocessExecutor instances to ensure
/// session_router can find IPC threads regardless of which executor instance it uses
#[cfg(feature = "multiprocess")]
static GLOBAL_SESSIONS: OnceLock<
    Arc<RwLock<HashMap<String, HashMap<String, tokio::sync::mpsc::Sender<IpcCommand>>>>>,
> = OnceLock::new();

#[cfg(feature = "multiprocess")]
fn global_sessions(
) -> Arc<RwLock<HashMap<String, HashMap<String, tokio::sync::mpsc::Sender<IpcCommand>>>>> {
    GLOBAL_SESSIONS
        .get_or_init(|| Arc::new(RwLock::new(HashMap::new())))
        .clone()
}

/// Commands sent to the dedicated IPC thread
#[cfg(feature = "multiprocess")]
enum IpcCommand {
    /// Send data to the node's input channel (serialized Python wire bytes)
    SendData { bytes: Vec<u8> },
    /// Register a bounded callback channel for continuous output forwarding.
    ///
    /// The IPC thread uses `blocking_send` on this channel, so a full queue
    /// will back-pressure the iceoryx2 subscriber loop — which in turn back-
    /// pressures the Python process via iceoryx2's own publisher limits.
    RegisterOutputCallback {
        callback_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    },
    /// Request graceful shutdown of the IPC thread
    Shutdown,
}

/// Responses from the dedicated IPC thread
#[cfg(feature = "multiprocess")]
enum IpcResponse {
    /// Data received from the node's output channel (serialized Python wire bytes)
    OutputData(Vec<u8>),
    /// Acknowledgment that data was sent successfully
    SendComplete,
    /// Error occurred during IPC operation
    Error(String),
}

/// Handle a single runtime control message read from a Python node's
/// control IPC channel (post-READY traffic from `publish_progress` /
/// `publish_to_node_port`).
///
/// Two wire formats:
///
/// 1. `PROGRESS:{"status": "...", "message": "..."}` — tap onto the
///    session's `__system__` topic so SPA / observer subscribers see
///    the event. Same shape produced at init time by
///    `wait_for_ready_signal_ipc`.
///
/// 2. `PUBLISH:{"target":"<node>","port":"<port>","payload":<value>}`
///    — route via [`SessionControl::publish`] to a sibling node's
///    aux port. The router wraps `<value>` in the standard aux
///    envelope (`{"__aux_port__": "<port>", "payload": <value>}`)
///    and delivers it to the target node's main input. This is the
///    sibling-publish primitive used to wire blip → instruct
///    triggering without introducing graph cycles.
///
/// Unknown prefixes are logged and dropped. Failures (missing
/// session bus, invalid JSON, router send error) are logged and
/// dropped — control-channel delivery is best-effort.
#[cfg(feature = "multiprocess")]
fn handle_runtime_control_message(
    bytes: &[u8],
    session_id: &str,
    node_id: &str,
    rt: &tokio::runtime::Runtime,
) {
    use crate::transport::session_control::{global_bus, ControlAddress};

    if bytes.starts_with(b"PROGRESS:") {
        let json_str = std::str::from_utf8(&bytes[9..]).unwrap_or("{}");
        let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) else {
            tracing::warn!(
                "[ctrl] node {} PROGRESS: payload was not valid JSON: {:?}",
                node_id,
                json_str
            );
            return;
        };
        let Some(bus) = global_bus() else { return };
        let Some(ctrl) = bus.get(session_id) else {
            return;
        };
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let status = json
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("loading");
        let message = json.get("message").and_then(|v| v.as_str()).unwrap_or("");
        tracing::info!(
            "[{}] Node {} runtime progress: {} - {}",
            session_id,
            node_id,
            status,
            message
        );
        ctrl.publish_tap(
            "__system__",
            None,
            crate::data::RuntimeData::Json(serde_json::json!({
                "kind": "loading",
                "status": status,
                "node": node_id,
                "message": message,
                "ts_ms": ts,
            })),
        );
    } else if bytes.starts_with(b"PUBLISH:") {
        let json_str = std::str::from_utf8(&bytes[8..]).unwrap_or("{}");
        let Ok(json) = serde_json::from_str::<serde_json::Value>(json_str) else {
            tracing::warn!(
                "[ctrl] node {} PUBLISH: payload was not valid JSON: {:?}",
                node_id,
                json_str
            );
            return;
        };
        let target = match json.get("target").and_then(|v| v.as_str()) {
            Some(t) if !t.is_empty() => t.to_string(),
            _ => {
                tracing::warn!(
                    "[ctrl] node {} PUBLISH missing/empty 'target': {}",
                    node_id,
                    json
                );
                return;
            }
        };
        let port = match json.get("port").and_then(|v| v.as_str()) {
            Some(p) if !p.is_empty() => p.to_string(),
            _ => {
                tracing::warn!(
                    "[ctrl] node {} PUBLISH missing/empty 'port': {}",
                    node_id,
                    json
                );
                return;
            }
        };
        let payload = json
            .get("payload")
            .cloned()
            .unwrap_or(serde_json::Value::Null);

        let Some(bus) = global_bus() else {
            tracing::debug!(
                "[ctrl] node {} PUBLISH dropped — no global SessionControlBus",
                node_id
            );
            return;
        };
        let Some(ctrl) = bus.get(session_id) else {
            tracing::debug!(
                "[ctrl] node {} PUBLISH dropped — no SessionControl for session {}",
                node_id,
                session_id
            );
            return;
        };

        let addr = ControlAddress::node_in(target.clone()).with_port(port.clone());
        let data = crate::data::RuntimeData::Json(payload);
        let publish_res = rt.block_on(async { ctrl.publish(&addr, data).await });
        match publish_res {
            Ok(()) => {
                tracing::debug!(
                    "[ctrl] node {} PUBLISH -> {}.in.{} delivered",
                    node_id,
                    target,
                    port
                );
            }
            Err(e) => {
                tracing::warn!(
                    "[ctrl] node {} PUBLISH -> {}.in.{} failed: {}",
                    node_id,
                    target,
                    port,
                    e
                );
            }
        }
    } else {
        tracing::trace!(
            "[ctrl] node {} unknown control message: {:?}",
            node_id,
            std::str::from_utf8(bytes).unwrap_or("<invalid utf8>")
        );
    }
}

/// Handle to a node's dedicated IPC thread
#[cfg(feature = "multiprocess")]
pub struct NodeIpcThread {
    /// Channel to send commands to the IPC thread
    command_tx: tokio::sync::mpsc::Sender<IpcCommand>,
    /// Channel to receive responses from the IPC thread
    response_rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<IpcResponse>>>,
    /// Handle to the OS thread (for cleanup)
    thread_handle: Option<std::thread::JoinHandle<()>>,
    /// Node ID this thread serves
    node_id: String,
}

#[cfg(feature = "multiprocess")]
impl std::fmt::Debug for NodeIpcThread {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeIpcThread")
            .field("node_id", &self.node_id)
            .field("thread_handle", &self.thread_handle.is_some())
            .finish()
    }
}

/// Configuration for multiprocess executor
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MultiprocessConfig {
    /// Maximum processes per session (None = unlimited)
    #[serde(default = "default_max_processes")]
    pub max_processes_per_session: Option<usize>,

    /// Channel buffer capacity (number of messages)
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,

    /// Process initialization timeout in seconds
    #[serde(default = "default_init_timeout")]
    pub init_timeout_secs: u64,

    /// Python executable path.
    /// When set explicitly, this overrides any managed environment resolution.
    #[serde(default = "default_python_executable")]
    pub python_executable: std::path::PathBuf,

    /// Enable backpressure on channels
    #[serde(default = "default_backpressure")]
    pub enable_backpressure: bool,

    /// Docker fallback policy when Docker is unavailable
    #[serde(default = "default_docker_fallback_policy")]
    pub docker_fallback_policy: DockerFallbackPolicy,

    /// Additional Python path entries for module discovery
    #[serde(default)]
    pub python_path: Vec<std::path::PathBuf>,

    /// Python environment management configuration.
    /// Controls whether the SDK auto-provisions venvs via uv.
    #[serde(default)]
    pub python_env: crate::python::env_manager::PythonEnvConfig,
}

/// Policy for handling Docker unavailability
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DockerFallbackPolicy {
    /// Allow fallback to native multiprocess with warning (default)
    Allow,
    /// Deny fallback and return error if Docker is unavailable
    Deny,
    /// Allow fallback with explicit warning log
    AllowWithWarning,
}

// Default value functions for serde
fn default_max_processes() -> Option<usize> {
    Some(10)
}

fn default_channel_capacity() -> usize {
    1000 // Increased from 100 to handle audio flood during initialization (50 packets/sec * 5sec init = 250+)
}

fn default_init_timeout() -> u64 {
    300 // 5 minutes for model loading (e.g., Kokoro TTS)
}

fn default_python_executable() -> std::path::PathBuf {
    if let Ok(python_path) = std::env::var("PYTHON_EXECUTABLE") {
        return std::path::PathBuf::from(python_path);
    }

    #[cfg(windows)]
    {
        std::path::PathBuf::from("python")
    }
    #[cfg(not(windows))]
    {
        std::path::PathBuf::from("python3")
    }
}

fn default_backpressure() -> bool {
    true
}

fn default_docker_fallback_policy() -> DockerFallbackPolicy {
    DockerFallbackPolicy::AllowWithWarning
}

impl Default for MultiprocessConfig {
    fn default() -> Self {
        Self {
            max_processes_per_session: Some(10),
            channel_capacity: 100,
            init_timeout_secs: 300, // 5 minutes for model loading
            python_executable: default_python_executable(),
            enable_backpressure: true,
            docker_fallback_policy: DockerFallbackPolicy::AllowWithWarning,
            python_path: Vec::new(),
            python_env: Default::default(),
        }
    }
}

impl MultiprocessConfig {
    /// Load configuration from a TOML file
    ///
    /// # Arguments
    /// * `path` - Path to the TOML configuration file
    ///
    /// # Returns
    /// * `Ok(MultiprocessConfig)` - Loaded configuration with defaults for missing fields
    /// * `Err(Error)` - If file cannot be read or parsed
    ///
    /// # Example
    /// ```
    /// use remotemedia_core::python::multiprocess::multiprocess_executor::MultiprocessConfig;
    /// use std::path::Path;
    ///
    /// // Use from_toml_str for inline configuration
    /// let config = MultiprocessConfig::default();
    /// ```
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path).map_err(|e| {
            Error::ConfigError(format!("Failed to read config file {:?}: {}", path, e))
        })?;

        Self::from_toml_str(&contents)
    }

    /// Load configuration from a TOML string
    ///
    /// # Arguments
    /// * `toml_str` - TOML configuration string
    ///
    /// # Returns
    /// * `Ok(MultiprocessConfig)` - Parsed configuration with defaults for missing fields
    /// * `Err(Error)` - If TOML cannot be parsed
    ///
    /// # Example
    /// ```
    /// use remotemedia_core::python::multiprocess::multiprocess_executor::MultiprocessConfig;
    ///
    /// let config_str = r#"
    ///     max_processes_per_session = 20
    ///     channel_capacity = 200
    ///     init_timeout_secs = 60
    ///     python_executable = "/usr/bin/python3"
    ///     enable_backpressure = true
    /// "#;
    ///
    /// let config = MultiprocessConfig::from_toml_str(config_str).unwrap();
    /// assert_eq!(config.channel_capacity, 200);
    /// ```
    pub fn from_toml_str(toml_str: &str) -> Result<Self> {
        // Parse the TOML string
        let config: MultiprocessConfig = toml::from_str(toml_str)
            .map_err(|e| Error::ConfigError(format!("Failed to parse TOML config: {}", e)))?;

        Ok(config)
    }

    /// Load configuration from runtime.toml in the current directory
    ///
    /// Falls back to default configuration if the file doesn't exist.
    ///
    /// # Returns
    /// * `Ok(MultiprocessConfig)` - Loaded or default configuration
    /// * `Err(Error)` - If file exists but cannot be parsed
    pub fn from_default_file() -> Result<Self> {
        let path = std::path::Path::new("runtime.toml");

        let mut config = if path.exists() {
            Self::from_file(path)?
        } else {
            Self::default()
        };

        // Apply environment variable overrides
        if let Ok(python_path) = std::env::var("PYTHON_EXECUTABLE") {
            config.python_executable = std::path::PathBuf::from(python_path);
        }

        config.python_env.apply_env_overrides();

        // Init timeout override. The default (300s) is per the existing
        // serde default; this exists because cold first-run init for ML
        // nodes can legitimately exceed it: uv venv provisioning of
        // transformers/torch (~2-3 min), HuggingFace model download
        // (multi-GB on cold cache), torch import, GPU upload + JIT
        // warmup. Prewarm is fundamentally different from per-call
        // inference — it should not be bound by the same time budget,
        // but until a separate prewarm-timeout knob lands this env
        // override gives operators a way to raise the ceiling.
        if let Ok(s) = std::env::var("REMOTEMEDIA_INIT_TIMEOUT_SECS") {
            if let Ok(secs) = s.parse::<u64>() {
                config.init_timeout_secs = secs;
            }
        }

        Ok(config)
    }
}

/// Session state for tracking pipeline execution
#[derive(Debug)]
pub struct SessionState {
    /// Session identifier
    pub session_id: String,

    /// Active node processes
    pub node_processes: HashMap<String, ProcessHandle>,

    /// IPC channels for this session
    pub channels: HashMap<String, ChannelHandle>,

    /// Dedicated IPC threads for each node (one thread per node)
    /// These threads own the persistent publishers/subscribers
    #[cfg(feature = "multiprocess")]
    pub ipc_threads: HashMap<String, NodeIpcThread>,

    /// Session status
    pub status: SessionStatus,

    /// Creation timestamp
    pub created_at: std::time::Instant,

    /// Initialization progress for each node (node_id -> progress)
    pub init_progress: HashMap<String, InitProgress>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SessionStatus {
    Initializing,
    Ready,
    Running,
    Terminating,
    Terminated,
    Error(String),
}

/// Initialization progress information for a node
#[derive(Debug, Clone)]
pub struct InitProgress {
    /// Node identifier
    pub node_id: String,

    /// Node type
    pub node_type: String,

    /// Current initialization status
    pub status: InitStatus,

    /// Progress percentage (0.0 to 1.0)
    pub progress: f32,

    /// Human-readable progress message
    pub message: String,

    /// Timestamp of this progress update
    pub timestamp: std::time::Instant,
}

/// Node initialization status
#[derive(Debug, Clone, PartialEq)]
pub enum InitStatus {
    /// Node process is starting
    Starting,

    /// Loading model files or dependencies
    LoadingModel,

    /// Connecting to IPC channels
    Connecting,

    /// Node is ready for execution
    Ready,

    /// Initialization failed
    Failed(String),
}

/// Multiprocess executor for Python nodes
#[derive(Clone)]
pub struct MultiprocessExecutor {
    /// Process manager
    process_manager: Arc<ProcessManager>,

    /// Channel registry
    channel_registry: Arc<ChannelRegistry>,

    /// Health monitor
    #[cfg(feature = "multiprocess")]
    health_monitor: Arc<HealthMonitor>,

    /// Active sessions
    sessions: Arc<RwLock<HashMap<String, SessionState>>>,

    /// Configuration
    config: MultiprocessConfig,

    /// Current node context
    current_context: Option<ExecutorNodeContext>,

    /// Docker support
    #[cfg(feature = "docker")]
    docker_support: Option<Arc<DockerSupport>>,

    /// Python environment manager for auto-provisioning venvs.
    /// None when PythonEnvMode::System (default / backward-compatible).
    env_manager: Option<Arc<crate::python::env_manager::PythonEnvManager>>,

    /// Optional control bus handle for forwarding progress events from
    /// Python nodes during initialization. Set via `set_control()`.
    control: Arc<std::sync::Mutex<Option<Arc<crate::transport::session_control::SessionControl>>>>,
}

impl MultiprocessExecutor {
    /// Process RuntimeData with streaming callback via IPC channels
    #[cfg(feature = "multiprocess")]
    pub async fn process_runtime_data_streaming<F>(
        &self, // Changed from &mut self - all state is behind Arc/RwLock
        input: crate::data::RuntimeData,
        session_id: Option<String>,
        mut callback: F,
    ) -> Result<usize>
    where
        F: FnMut(crate::data::RuntimeData) -> Result<()> + Send,
    {
        let ctx = self
            .current_context
            .as_ref()
            .ok_or_else(|| Error::Execution("Node not initialized".to_string()))?;

        let session_id = session_id
            .or_else(|| ctx.session_id.clone())
            .unwrap_or_else(|| format!("default_{}", ctx.node_id));

        tracing::debug!(
            "MultiprocessExecutor::process_runtime_data_streaming for node {} (session: {})",
            ctx.node_id,
            session_id
        );

        // Convert input to Python IPC wire bytes
        let ipc_data = Self::to_ipc_runtime_data(&input, &session_id);

        tracing::debug!(
            "[Multiprocess] Converted input data for node '{}': {} bytes",
            ctx.node_id,
            ipc_data.len()
        );

        // Get the IPC thread for this node
        let sessions = self.sessions.read().await;
        let session = sessions
            .get(&session_id)
            .ok_or_else(|| Error::Execution(format!("Session {} not found", session_id)))?;

        let ipc_thread_cmd_tx = session
            .ipc_threads
            .get(&ctx.node_id)
            .ok_or_else(|| {
                Error::Execution(format!("IPC thread not found for node {}", ctx.node_id))
            })?
            .command_tx
            .clone();

        let ipc_thread_resp_rx = session
            .ipc_threads
            .get(&ctx.node_id)
            .ok_or_else(|| {
                Error::Execution(format!("IPC thread not found for node {}", ctx.node_id))
            })?
            .response_rx
            .clone();

        drop(sessions); // Release read lock

        // Send data to IPC thread (fast, no blocking!)
        tracing::debug!(
            "[Multiprocess] Sending data to IPC thread for node '{}'",
            ctx.node_id
        );
        ipc_thread_cmd_tx
            .send(IpcCommand::SendData { bytes: ipc_data })
            .await
            .map_err(|e| Error::Execution(format!("Failed to send to IPC thread: {}", e)))?;

        // Wait for send acknowledgment
        let mut resp_rx = ipc_thread_resp_rx.lock().await;
        match tokio::time::timeout(std::time::Duration::from_secs(5), resp_rx.recv()).await {
            Ok(Some(IpcResponse::SendComplete)) => {
                tracing::debug!("Data sent successfully to node: {}", ctx.node_id);
            }
            Ok(Some(IpcResponse::Error(e))) => {
                return Err(Error::Execution(format!("IPC send error: {}", e)));
            }
            Ok(None) | Err(_) => {
                return Err(Error::Execution(format!(
                    "Timeout waiting for send confirmation"
                )));
            }
            _ => {}
        }

        // Collect outputs from IPC thread continuously
        // The callback will control when to stop (e.g., based on end markers)
        let mut output_count = 0;

        tracing::debug!(
            "[Multiprocess] Continuously collecting output from node '{}'",
            ctx.node_id
        );

        // Upper bound only. The hot path is the `EndOfInput` sentinel — Python
        // emits it after each `process()` call, so we return immediately.
        // This fallback fires only if the Python node crashes or never signals
        // completion (e.g. an older client without the sentinel).
        let hard_timeout = std::time::Duration::from_secs(300);

        loop {
            tracing::debug!(
                "[Multiprocess] Waiting for response from IPC thread for node '{}'",
                ctx.node_id
            );

            match tokio::time::timeout(hard_timeout, resp_rx.recv()).await {
                Err(_) => {
                    // Timeout — no more outputs from the node for this input
                    tracing::debug!(
                        "[Multiprocess] Output receive timeout for node '{}' after {} outputs",
                        ctx.node_id,
                        output_count
                    );
                    return Ok(output_count);
                }
                Ok(recv_result) => match recv_result {
                    Some(IpcResponse::OutputData(ipc_output)) => {
                        // EndOfInput is the Python side's "done for this input"
                        // signal. Check the first byte (wire data type) directly.
                        // WireDataType::EndOfInput = 8
                        if !ipc_output.is_empty() && ipc_output[0] == 8 {
                            tracing::debug!(
                            "[Multiprocess] Received EndOfInput from node '{}' after {} outputs",
                            ctx.node_id,
                            output_count
                        );
                            return Ok(output_count);
                        }

                        tracing::debug!(
                            "[Multiprocess] Received OutputData from node '{}': {} bytes",
                            ctx.node_id,
                            ipc_output.len()
                        );

                        let output_data = Self::from_ipc_runtime_data(ipc_output)?;

                        tracing::debug!(
                        "[Multiprocess] Converted to RuntimeData, invoking callback for node '{}'",
                        ctx.node_id
                    );

                        // Forward all outputs - the callback decides when to stop
                        match callback(output_data) {
                            Ok(_) => {
                                output_count += 1;
                                tracing::debug!("[Multiprocess] Callback succeeded for node '{}', total outputs: {}", ctx.node_id, output_count);
                            }
                            Err(e) => {
                                // Callback returned error - stop collecting
                                tracing::debug!(
                                    "Callback stopped collection for node {} after {} outputs: {}",
                                    ctx.node_id,
                                    output_count,
                                    e
                                );
                                return Ok(output_count);
                            }
                        }
                    }
                    Some(IpcResponse::SendComplete) => {
                        tracing::debug!("[Multiprocess] Received SendComplete from node '{}', continuing to poll for outputs", ctx.node_id);
                        // Acknowledgment - ignore and continue polling for outputs
                        continue;
                    }
                    Some(IpcResponse::Error(e)) => {
                        tracing::error!(
                            "[Multiprocess] IPC error from node '{}': {}",
                            ctx.node_id,
                            e
                        );
                        return Err(Error::Execution(format!("IPC error: {}", e)));
                    }
                    None => {
                        // IPC thread disconnected
                        tracing::debug!(
                            "IPC thread disconnected for node {} after {} outputs",
                            ctx.node_id,
                            output_count
                        );
                        return Ok(output_count);
                    }
                }, // match recv_result
            } // match timeout
        }
    }

    /// Register a bounded callback channel for continuous output forwarding
    /// from a node's IPC thread. The callback will receive ALL outputs from
    /// the node, independent of any input processing.
    ///
    /// The sender is bounded — the caller chooses the capacity. The IPC
    /// thread uses `blocking_send`, so a full receiver applies real
    /// backpressure all the way back to the Python process.
    #[cfg(feature = "multiprocess")]
    pub async fn register_output_callback(
        &self,
        node_id: &str,
        session_id: &str,
        callback_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
    ) -> Result<()> {
        // Get the IPC thread from global sessions storage
        let global_sessions = global_sessions();
        let sessions = global_sessions.read().await;

        let session = sessions
            .get(session_id)
            .ok_or_else(|| Error::Execution(format!("Session {} not found", session_id)))?;

        let ipc_thread_cmd_tx = session
            .get(node_id)
            .ok_or_else(|| Error::Execution(format!("IPC thread not found for node {}", node_id)))?
            .clone();

        drop(sessions);

        // Send register command to IPC thread
        ipc_thread_cmd_tx
            .send(IpcCommand::RegisterOutputCallback { callback_tx })
            .await
            .map_err(|e| {
                Error::Execution(format!(
                    "Failed to register callback for node {}: {}",
                    node_id, e
                ))
            })?;

        tracing::debug!(
            "Registered output callback for node '{}' in session '{}'",
            node_id,
            session_id
        );
        Ok(())
    }

    /// Send data to a specific node without collecting outputs
    /// Use this when a background output draining task is already registered
    #[cfg(feature = "multiprocess")]
    pub async fn send_data_to_node(
        &self,
        node_id: &str,
        session_id: &str,
        input: crate::data::RuntimeData,
    ) -> Result<()> {
        tracing::info!(
            "[send_data_to_node] START: node_id='{}', session_id='{}'",
            node_id,
            session_id
        );

        // Convert input to Python IPC wire bytes
        let ipc_data = Self::to_ipc_runtime_data(&input, session_id);

        tracing::info!(
            "[send_data_to_node] Converted RuntimeData to IPC wire: {} bytes",
            ipc_data.len()
        );

        // Get the IPC thread from global sessions storage
        let global_sessions = global_sessions();
        let sessions = global_sessions.read().await;

        let session = sessions.get(session_id).ok_or_else(|| {
            let available: Vec<_> = sessions.keys().collect();
            Error::Execution(format!(
                "Session {} not found. Available: {:?}",
                session_id, available
            ))
        })?;

        let ipc_thread_cmd_tx = session
            .get(node_id)
            .ok_or_else(|| {
                let available: Vec<_> = session.keys().collect();
                Error::Execution(format!(
                    "IPC thread not found for node {}. Available: {:?}",
                    node_id, available
                ))
            })?
            .clone();

        drop(sessions);

        // Send data to IPC thread (no waiting for outputs)
        tracing::info!(
            "[send_data_to_node] Queuing data to IPC thread for node '{}'",
            node_id
        );

        match ipc_thread_cmd_tx
            .send(IpcCommand::SendData { bytes: ipc_data })
            .await
        {
            Ok(_) => {
                tracing::info!(
                    "[send_data_to_node] Successfully queued data to IPC thread for node '{}'",
                    node_id
                );
                Ok(())
            }
            Err(e) => {
                tracing::error!(
                    "[send_data_to_node] Failed to send to IPC thread for node '{}': {}",
                    node_id,
                    e
                );
                Err(Error::Execution(format!(
                    "Failed to send to IPC thread for node {}: {}",
                    node_id, e
                )))
            }
        }
    }

    /// Send data to a node's IPC thread without waiting for response (fire-and-forget)
    /// This is used for routing data between nodes in the pipeline
    #[cfg(feature = "multiprocess")]
    pub async fn send_to_node_async(
        &self,
        node_id: &str,
        session_id: &str,
        data: crate::data::RuntimeData,
    ) -> Result<()> {
        // Convert to Python IPC wire bytes
        let ipc_data = Self::to_ipc_runtime_data(&data, session_id);

        // Get the IPC thread from global sessions storage
        let global_sessions = global_sessions();
        let sessions = global_sessions.read().await;

        let session = sessions.get(session_id).ok_or_else(|| {
            let available: Vec<_> = sessions.keys().collect();
            Error::Execution(format!(
                "Session {} not found in global sessions. Available: {:?}",
                session_id, available
            ))
        })?;

        let ipc_thread_cmd_tx = session
            .get(node_id)
            .ok_or_else(|| {
                let available: Vec<_> = session.keys().collect();
                Error::Execution(format!(
                    "IPC thread not found for node {} in session {}. Available nodes: {:?}",
                    node_id, session_id, available
                ))
            })?
            .clone();

        drop(sessions); // Release read lock

        // Send data to IPC thread (no waiting for response)
        tracing::debug!(
            "send_to_node_async: Sending data to node '{}' in session '{}'",
            node_id,
            session_id
        );
        ipc_thread_cmd_tx
            .send(IpcCommand::SendData { bytes: ipc_data })
            .await
            .map_err(|e| {
                Error::Execution(format!(
                    "Failed to send to IPC thread for node {}: {}",
                    node_id, e
                ))
            })?;

        Ok(())
    }

    /// Convert main RuntimeData to Python IPC wire bytes
    #[cfg(feature = "multiprocess")]
    pub fn to_ipc_runtime_data(data: &crate::data::RuntimeData, session_id: &str) -> Vec<u8> {
        to_python_wire(data, session_id)
    }

    /// Convert Python IPC wire bytes back to main RuntimeData
    #[cfg(feature = "multiprocess")]
    pub fn from_ipc_runtime_data(bytes: Vec<u8>) -> Result<crate::data::RuntimeData> {
        from_python_wire(&bytes)
            .map_err(|e| Error::Execution(format!("Invalid IPC wire data: {e}")))
    }

    /// Create a new multiprocess executor
    pub fn new(config: MultiprocessConfig) -> Self {
        #[cfg(feature = "multiprocess")]
        let health_monitor = Arc::new(HealthMonitor::new(config.init_timeout_secs));

        // Use the global shared channel registry to ensure all executors
        // share the same iceoryx2 Node instance
        let channel_registry = ChannelRegistry::global();

        // Initialize Docker support if feature is enabled
        // Note: Docker initialization is deferred to avoid blocking in constructors
        // Use docker_support() method which will lazily initialize if needed
        #[cfg(feature = "docker")]
        let docker_support = None;

        // Initialize Python environment manager if mode is not System
        let env_manager = if config.python_env.mode
            != crate::python::env_manager::PythonEnvMode::System
        {
            match crate::python::env_manager::PythonEnvManager::new(config.python_env.clone()) {
                Ok(mgr) => {
                    tracing::info!(
                        mode = ?config.python_env.mode,
                        "Python environment manager initialized"
                    );
                    Some(Arc::new(mgr))
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "Failed to initialize Python environment manager, falling back to system Python"
                    );
                    None
                }
            }
        } else {
            None
        };

        let executor = Self {
            process_manager: Arc::new(ProcessManager::new(config.clone())),
            channel_registry,
            #[cfg(feature = "multiprocess")]
            health_monitor: health_monitor.clone(),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            config,
            current_context: None,
            #[cfg(feature = "docker")]
            docker_support,
            env_manager,
            control: Arc::new(std::sync::Mutex::new(None)),
        };

        // Setup pipeline termination on node failure
        #[cfg(feature = "multiprocess")]
        executor.setup_failure_handling();

        executor
    }

    /// Set the control bus handle for forwarding progress events from
    /// Python nodes during initialization.
    pub fn set_control(&self, control: Arc<crate::transport::session_control::SessionControl>) {
        *self.control.lock().unwrap() = Some(control);
    }

    /// Get a clone of the control bus handle (if set).
    fn get_control(&self) -> Option<Arc<crate::transport::session_control::SessionControl>> {
        self.control.lock().unwrap().clone()
    }

    /// Register a Python module for node registration
    ///
    /// This module will be imported before looking up node types,
    /// allowing tests and custom applications to register nodes dynamically.
    ///
    /// # Arguments
    /// * `module` - Fully qualified Python module name (e.g., "myapp.nodes.custom")
    ///
    /// # Example
    /// ```ignore
    /// let mut executor = MultiprocessExecutor::new(config);
    /// executor.register_module("remotemedia.nodes.test_nodes").await;
    /// ```
    pub async fn register_module(&self, module: &str) {
        self.process_manager
            .register_module(module.to_string())
            .await;
    }

    /// Get the list of registered modules
    pub async fn get_registered_modules(&self) -> Vec<String> {
        self.process_manager.get_registered_modules().await
    }

    /// Get the channel registry for IPC communication
    pub fn channel_registry(&self) -> &Arc<ChannelRegistry> {
        &self.channel_registry
    }

    /// Discover Python package requirements declared by a node class via `@python_requires`.
    ///
    /// Runs a lightweight Python probe using the base Python interpreter (before venv setup)
    /// to import the node class and read its `__python_requires__` attribute. This allows
    /// the managed environment to include node-declared deps in the initial venv creation,
    /// avoiding a failed first spawn.
    ///
    /// Returns an empty Vec if discovery fails (e.g., node not found, Python not available).
    async fn discover_node_deps(&self, node_type: &str) -> Vec<String> {
        let python = self.config.python_executable.clone();

        // The probe no longer imports `remotemedia` (see comment below
        // for why), so any `register_modules` configured for the
        // process manager are intentionally NOT replayed here. The
        // probe is a pure source-tree AST scan.
        let mut python_path_env = {
            let spawn_config = self.process_manager.spawn_config().read().await;

            if !self.config.python_path.is_empty() {
                self.config
                    .python_path
                    .iter()
                    .map(|p| p.to_string_lossy().to_string())
                    .collect::<Vec<_>>()
                    .join(if cfg!(windows) { ";" } else { ":" })
            } else if !spawn_config.python_path.is_empty() {
                spawn_config
                    .python_path
                    .iter()
                    .map(|p| p.to_string_lossy().to_string())
                    .collect::<Vec<_>>()
                    .join(if cfg!(windows) { ";" } else { ":" })
            } else {
                String::new()
            }
        };

        // Auto-include REMOTEMEDIA_PYTHON_SRC on PYTHONPATH for the probe.
        // The probe runs against the *base* Python interpreter (the venv
        // doesn't exist yet), so it picks up whatever `remotemedia` is in
        // that interpreter's site-packages. On a dev box that's almost
        // always older than the working tree — which means PEP 723
        // metadata added under `clients/python/` is invisible, the venv
        // is built with stale `@python_requires` only, and CUDA-wheel
        // pins (or any other PEP 723 dep) never reach uv.
        //
        // Prepending the source root makes the editable source win over
        // installed site-packages without requiring the user to set
        // PYTHONPATH explicitly.
        if let Ok(src) = std::env::var("REMOTEMEDIA_PYTHON_SRC") {
            let trimmed = src.trim();
            if !trimmed.is_empty() {
                let sep = if cfg!(windows) { ";" } else { ":" };
                python_path_env = if python_path_env.is_empty() {
                    trimmed.to_string()
                } else {
                    format!("{trimmed}{sep}{python_path_env}")
                };
                tracing::info!(
                    "[discover_node_deps] auto-prepended REMOTEMEDIA_PYTHON_SRC={} to PYTHONPATH",
                    trimmed
                );
            }
        }

        // The probe used to print bare JSON to stdout and swallow every
        // failure with `print("[]")`, sending stderr to /dev/null. That
        // hid the actual cause of empty results.
        //
        // We then tried `from remotemedia.core.multiprocessing import
        // get_node_requirements`, but importing the `remotemedia`
        // top-level package eagerly pulls in every node submodule
        // (`from .nodes import *` chains into `from .ml import
        // LFM2AudioNode`, which then triggers
        // `remotemedia.nodes.ml.__init__` which iterates every ML
        // submodule including ones that hit torch / CUDA at module
        // import time). That import chain easily blows past the 15 s
        // probe budget on a cold Windows box with no GPU driver
        // warmed up, leaving the probe to return `[]` and the venv
        // to be built without torch — which is the bug we're trying
        // to fix.
        //
        // Final design: the probe is a *standalone* script that does
        // NOT import the `remotemedia` package at all. Instead it
        // walks the source tree at `REMOTEMEDIA_PYTHON_SRC`
        // (preferred) or `PYTHONPATH` entries looking for a class
        // decorated with `@register_node("<node_type>")`, then AST-
        // parses that file for `@python_requires([...])` literals and
        // PEP 723 inline metadata. Pure source inspection, no
        // execution.
        //
        //   * stdout always carries a single line starting with
        //     `RMDEPS:` followed by JSON
        //     `{"deps": [...], "error": null | "..."}`.
        //   * stderr is captured (not nulled) and forwarded to
        //     tracing so the user can see what the probe did.
        //   * The 15 s budget now only covers AST work over the
        //     source tree — comfortably under a second on a real
        //     filesystem.
        let script = format!(
            r##"
import ast, json, os, sys, traceback
from pathlib import Path

NODE_TYPE = "{node_type}"
RESULT = {{"deps": [], "error": None, "source_file": None, "roots_searched": []}}

def _candidate_roots():
    roots = []
    src = os.environ.get("REMOTEMEDIA_PYTHON_SRC", "").strip()
    if src:
        roots.append(Path(src))
    for entry in sys.path:
        if not entry:
            continue
        p = Path(entry)
        if (p / "remotemedia").is_dir():
            roots.append(p)
    seen = set()
    deduped = []
    for r in roots:
        try:
            key = r.resolve()
        except OSError:
            key = r
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    return deduped

def _str_value(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None

def _list_of_strs(node):
    if not isinstance(node, ast.List):
        return None
    out = []
    for elt in node.elts:
        s = _str_value(elt)
        if s is None:
            return None
        out.append(s)
    return out

def _dec_name(dec):
    func = dec.func if isinstance(dec, ast.Call) else dec
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None

def _scan_file(path, want_node_type):
    """Return (deps_list, found) where `deps_list` is the
    @python_requires([...]) literal on the class registered as
    `<want_node_type>`. Class registration is recognised via either
    `@register_node(<target>)` (MultiprocessNode pattern, used by
    `lfm2_text.py`, `tool_classifier.py`, etc.) OR
    `@streaming_node(node_type=<target>)` (the older registration
    decorator, still used by e.g. `tts.py::KokoroTTSNode`). Returns
    `(None, False)` if no matching class is found."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None, False
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return None, False
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        matches = False
        deps = None
        for dec in node.decorator_list:
            if not isinstance(dec, ast.Call):
                continue
            name = _dec_name(dec)
            if name in ("register_node", "streaming_node"):
                target = None
                if dec.args:
                    target = _str_value(dec.args[0])
                for kw in dec.keywords:
                    if kw.arg in ("node_type", "name"):
                        target = _str_value(kw.value) or target
                if target == want_node_type:
                    matches = True
            elif name == "python_requires":
                arg = None
                if dec.args:
                    arg = _list_of_strs(dec.args[0])
                for kw in dec.keywords:
                    if kw.arg == "deps":
                        arg = _list_of_strs(kw.value) or arg
                if arg is not None:
                    deps = arg
        if matches:
            return (deps or []), True
    return None, False

def _extract_pep723(path):
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return []
    try:
        import tomllib
    except ImportError:
        return []
    inside = False
    metadata_lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if not inside and stripped == "# /// script":
            inside = True
            continue
        if inside and stripped == "# ///":
            break
        if inside:
            if not stripped.startswith("#"):
                continue
            content = stripped[1:]
            if content.startswith(" "):
                content = content[1:]
            metadata_lines.append(content)
    if not metadata_lines:
        return []
    try:
        metadata = tomllib.loads("\n".join(metadata_lines))
    except Exception:
        return []
    deps = metadata.get("dependencies", [])
    if not isinstance(deps, list):
        return []
    return [d for d in deps if isinstance(d, str)]

def _merge_dedup(declared, pep723):
    if not pep723:
        return list(declared)
    seen = set()
    out = []
    for d in list(declared) + list(pep723):
        if d in seen:
            continue
        seen.add(d)
        out.append(d)
    return out

try:
    roots = _candidate_roots()
    RESULT["roots_searched"] = [str(r) for r in roots]
    matched = False
    for root in roots:
        pkg_root = root / "remotemedia"
        if not pkg_root.is_dir():
            continue
        for py_path in pkg_root.rglob("*.py"):
            # Skip giant test trees if any
            if "tests" in py_path.parts or "__pycache__" in py_path.parts:
                continue
            deps, found = _scan_file(py_path, NODE_TYPE)
            if found:
                pep723 = _extract_pep723(py_path)
                RESULT["deps"] = _merge_dedup(deps or [], pep723)
                RESULT["source_file"] = str(py_path)
                matched = True
                break
        if matched:
            break
    if not matched:
        RESULT["error"] = f"no class found with @register_node({{NODE_TYPE!r}}) under any candidate root"
except BaseException as exc:
    RESULT["error"] = f"{{type(exc).__name__}}: {{exc}}"
    traceback.print_exc(file=sys.stderr)

sys.stdout.write("RMDEPS:" + json.dumps(RESULT) + "\n")
sys.stdout.flush()
"##,
        );

        let mut cmd = tokio::process::Command::new(python);
        cmd.args(["-c", &script]);
        if !python_path_env.is_empty() {
            cmd.env("PYTHONPATH", &python_path_env);
        }
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let emit_stderr = |node_type: &str, stderr_bytes: &[u8]| {
            if stderr_bytes.is_empty() {
                return;
            }
            let stderr_text = String::from_utf8_lossy(stderr_bytes);
            for line in stderr_text.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                tracing::warn!(
                    node_type = %node_type,
                    "[discover_node_deps stderr] {}",
                    trimmed
                );
            }
        };

        let parse_marker = |stdout_bytes: &[u8]| -> Option<(Vec<String>, Option<String>)> {
            let text = String::from_utf8_lossy(stdout_bytes);
            for line in text.lines() {
                let trimmed = line.trim();
                if let Some(rest) = trimmed.strip_prefix("RMDEPS:") {
                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(rest) {
                        let deps = val
                            .get("deps")
                            .and_then(|d| d.as_array())
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default();
                        let err = val
                            .get("error")
                            .and_then(|e| e.as_str())
                            .map(|s| s.to_string());
                        return Some((deps, err));
                    }
                }
            }
            None
        };

        match tokio::time::timeout(std::time::Duration::from_secs(15), cmd.output()).await {
            Ok(Ok(output)) if output.status.success() => {
                emit_stderr(node_type, &output.stderr);
                match parse_marker(&output.stdout) {
                    Some((deps, err)) => {
                        if let Some(err) = err {
                            tracing::warn!(
                                node_type = %node_type,
                                error = %err,
                                "Node deps discovery probe reported an error; using empty deps"
                            );
                        }
                        if !deps.is_empty() {
                            tracing::info!(
                                node_type = %node_type,
                                deps = ?deps,
                                "Discovered node-declared Python requirements"
                            );
                        } else {
                            tracing::info!(
                                node_type = %node_type,
                                "Node deps discovery probe returned empty deps"
                            );
                        }
                        deps
                    }
                    None => {
                        let stdout_text = String::from_utf8_lossy(&output.stdout);
                        tracing::warn!(
                            node_type = %node_type,
                            stdout = %stdout_text.trim(),
                            "Node deps discovery probe did not emit RMDEPS marker"
                        );
                        Vec::new()
                    }
                }
            }
            Ok(Ok(output)) => {
                emit_stderr(node_type, &output.stderr);
                tracing::warn!(
                    node_type = %node_type,
                    exit_status = ?output.status,
                    "Node deps discovery probe exited with non-zero status"
                );
                Vec::new()
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    node_type = %node_type,
                    error = %e,
                    "Failed to run node deps discovery probe"
                );
                Vec::new()
            }
            Err(_) => {
                tracing::warn!(
                    node_type = %node_type,
                    "Node deps discovery probe timed out after 15s"
                );
                Vec::new()
            }
        }
    }

    /// Get the Docker support instance if available
    #[cfg(feature = "docker")]
    pub fn docker_support(&self) -> Option<&Arc<DockerSupport>> {
        self.docker_support.as_ref()
    }

    /// Initialize Docker support asynchronously
    /// This should be called once during executor initialization to enable Docker functionality
    #[cfg(feature = "docker")]
    pub async fn initialize_docker_support(&mut self) -> Result<()> {
        if self.docker_support.is_some() {
            // Already initialized
            return Ok(());
        }

        match DockerSupport::new().await {
            Ok(ds) => {
                tracing::debug!("Docker support initialized successfully");
                self.docker_support = Some(Arc::new(ds));
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    "Docker support unavailable: {}. Falling back to regular multiprocess.",
                    e
                );
                // Don't fail - just continue without Docker support
                Ok(())
            }
        }
    }

    /// Setup failure handling for pipeline termination
    #[cfg(feature = "multiprocess")]
    fn setup_failure_handling(&self) {
        let sessions = self.sessions.clone();
        let health_monitor = self.health_monitor.clone();

        // Register process exit handler
        let process_manager = self.process_manager.clone();
        let process_manager_weak = Arc::downgrade(&process_manager);
        let sessions_weak = Arc::downgrade(&sessions);
        let health_monitor_for_handler = health_monitor.clone();

        tokio::spawn(async move {
            process_manager
                .on_process_exit(move |pid, reason| {
                    let sessions_weak = sessions_weak.clone();
                    let health_monitor = health_monitor_for_handler.clone();
                    let process_manager_weak = process_manager_weak.clone();

                    tokio::spawn(async move {
                        // Upgrade Weak pointers to break memory cycles
                        let Some(sessions) = sessions_weak.upgrade() else {
                            return;
                        };
                        let Some(process_manager_clone) = process_manager_weak.upgrade() else {
                            return;
                        };

                        // Find which session this process belongs to
                        let session_info = {
                            let sessions_guard = sessions.read().await;
                            sessions_guard
                                .values()
                                .find(|s| s.node_processes.values().any(|p| p.id == pid))
                                .map(|s| {
                                    (
                                        s.session_id.clone(),
                                        s.node_processes
                                            .iter()
                                            .find(|(_, p)| p.id == pid)
                                            .map(|(node_id, _)| node_id.clone()),
                                    )
                                })
                        };

                        if let Some((session_id, Some(node_id))) = session_info {
                            // Handle process exit
                            let _ = health_monitor
                                .handle_process_exit(
                                    pid,
                                    reason.clone(),
                                    Some(session_id.clone()),
                                    Some(node_id.clone()),
                                )
                                .await;

                            // Terminate pipeline on error
                            match reason {
                                ExitReason::Error(_) | ExitReason::Killed | ExitReason::Timeout => {
                                    tracing::error!(
                                        "Node {} (PID {}) failed, terminating pipeline {}",
                                        node_id,
                                        pid,
                                        session_id
                                    );

                                    // Terminate the entire session
                                    if let Err(e) = Self::terminate_session_static(
                                        sessions.clone(),
                                        process_manager_clone,
                                        &session_id,
                                    )
                                    .await
                                    {
                                        tracing::error!(
                                            "Failed to terminate session {}: {}",
                                            session_id,
                                            e
                                        );
                                    }
                                }
                                ExitReason::Normal => {
                                    tracing::debug!(
                                        "Node {} (PID {}) exited normally",
                                        node_id,
                                        pid
                                    );
                                }
                            }
                        }
                    });
                })
                .await;
        });

        // Register health event handler for pipeline termination
        let sessions_weak_events = Arc::downgrade(&sessions);

        tokio::spawn(async move {
            health_monitor
                .on_event(move |event| {
                    if let ProcessEvent::PipelineTerminated {
                        session_id,
                        failed_node,
                        reason,
                        ..
                    } = event
                    {
                        tracing::error!(
                            "Pipeline {} terminated: node {} failed - {}",
                            session_id,
                            failed_node,
                            reason
                        );

                        let sessions_weak = sessions_weak_events.clone();
                        tokio::spawn(async move {
                            if let Some(sessions) = sessions_weak.upgrade() {
                                // Update session status
                                if let Some(session) = sessions.write().await.get_mut(&session_id) {
                                    session.status = SessionStatus::Error(reason);
                                }
                            }
                        });
                    }
                })
                .await;
        });
    }

    /// Static version of terminate_session for use in closures
    #[cfg(feature = "multiprocess")]
    async fn terminate_session_static(
        sessions: Arc<RwLock<HashMap<String, SessionState>>>,
        process_manager: Arc<ProcessManager>,
        session_id: &str,
    ) -> Result<()> {
        let mut sessions_guard = sessions.write().await;

        if let Some(mut session) = sessions_guard.remove(session_id) {
            session.status = SessionStatus::Terminating;

            // Terminate all processes in the session
            for (_, process) in session.node_processes.drain() {
                process_manager
                    .terminate_process(process, std::time::Duration::from_secs(5))
                    .await?;
            }

            // Channels will be cleaned up when processes exit
        }

        Ok(())
    }

    /// Create a new session for pipeline execution
    pub async fn create_session(&self, session_id: String) -> Result<()> {
        let mut sessions = self.sessions.write().await;

        // Check if session already exists
        if sessions.contains_key(&session_id) {
            return Err(Error::Execution(format!(
                "Session {} already exists",
                session_id
            )));
        }

        // Create new session state
        let session = SessionState {
            session_id: session_id.clone(),
            node_processes: HashMap::new(),
            channels: HashMap::new(),
            #[cfg(feature = "multiprocess")]
            ipc_threads: HashMap::new(),
            status: SessionStatus::Initializing,
            created_at: std::time::Instant::now(),
            init_progress: HashMap::new(),
        };

        sessions.insert(session_id, session);
        Ok(())
    }

    /// Wait for all nodes in a session to complete initialization
    pub async fn wait_for_initialization(
        &self,
        session_id: &str,
        timeout: std::time::Duration,
    ) -> Result<()> {
        let start = std::time::Instant::now();

        loop {
            // Check if timeout expired
            if start.elapsed() > timeout {
                return Err(Error::Execution(format!(
                    "Session {} initialization timeout after {}s",
                    session_id,
                    timeout.as_secs()
                )));
            }

            // Check session status
            let sessions = self.sessions.read().await;
            let session = sessions
                .get(session_id)
                .ok_or_else(|| Error::Execution(format!("Session {} not found", session_id)))?;

            // Check if any nodes failed initialization
            for (node_id, progress) in &session.init_progress {
                if let InitStatus::Failed(reason) = &progress.status {
                    return Err(Error::Execution(format!(
                        "Node {} failed to initialize: {}",
                        node_id, reason
                    )));
                }
            }

            // Check if all nodes are ready
            if !session.node_processes.is_empty() {
                let all_ready = session.node_processes.keys().all(|node_id| {
                    session
                        .init_progress
                        .get(node_id)
                        .map(|p| p.status == InitStatus::Ready)
                        .unwrap_or(false)
                });

                if all_ready {
                    // Update session status to Ready
                    drop(sessions);
                    let mut sessions = self.sessions.write().await;
                    if let Some(session) = sessions.get_mut(session_id) {
                        session.status = SessionStatus::Ready;
                    }
                    return Ok(());
                }
            }

            drop(sessions);

            // Yield to allow other tasks to run before checking again
            tokio::task::yield_now().await;
        }
    }

    /// Update initialization progress for a node
    pub async fn update_init_progress(
        &self,
        session_id: &str,
        node_id: &str,
        status: InitStatus,
        progress: f32,
        message: String,
    ) -> Result<()> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| Error::Execution(format!("Session {} not found", session_id)))?;

        // Get node type from process handle
        let node_type = session
            .node_processes
            .get(node_id)
            .map(|p| p.node_type.clone())
            .unwrap_or_else(|| "unknown".to_string());

        // Create or update progress entry
        let init_progress = InitProgress {
            node_id: node_id.to_string(),
            node_type,
            status,
            progress: progress.clamp(0.0, 1.0),
            message,
            timestamp: std::time::Instant::now(),
        };

        session
            .init_progress
            .insert(node_id.to_string(), init_progress.clone());

        // Log progress
        tracing::debug!(
            "Session {}, Node {}: {} ({}%)",
            session_id,
            node_id,
            init_progress.message,
            (init_progress.progress * 100.0) as u8
        );

        Ok(())
    }

    /// Get initialization progress for all nodes in a session
    pub async fn get_init_progress(&self, session_id: &str) -> Result<Vec<InitProgress>> {
        let sessions = self.sessions.read().await;
        let session = sessions
            .get(session_id)
            .ok_or_else(|| Error::Execution(format!("Session {} not found", session_id)))?;

        Ok(session.init_progress.values().cloned().collect())
    }

    /// Check if Docker is available and handle fallback according to policy
    ///
    /// Returns `Ok(true)` if Docker should be used, `Ok(false)` if fallback to multiprocess,
    /// or `Err` if Docker is unavailable and fallback is denied by policy.
    #[cfg(feature = "docker")]
    async fn should_use_docker(&self, node_id: &str) -> Result<bool> {
        if let Some(docker_support) = &self.docker_support {
            // Docker support exists, check if daemon is responsive
            if docker_support.check_availability().await {
                tracing::debug!("Docker is available for node '{}'", node_id);
                Ok(true)
            } else {
                tracing::warn!(
                    "Docker daemon is not responsive for node '{}'. Applying fallback policy: {:?}",
                    node_id,
                    self.config.docker_fallback_policy
                );
                self.handle_docker_unavailable_fallback(node_id)
            }
        } else {
            tracing::warn!(
                "Docker support not initialized for node '{}'. Applying fallback policy: {:?}",
                node_id,
                self.config.docker_fallback_policy
            );
            self.handle_docker_unavailable_fallback(node_id)
        }
    }

    /// Handle Docker unavailable scenario according to fallback policy
    #[cfg(feature = "docker")]
    fn handle_docker_unavailable_fallback(&self, node_id: &str) -> Result<bool> {
        match self.config.docker_fallback_policy {
            DockerFallbackPolicy::Deny => {
                Err(Error::Execution(format!(
                    "Docker is unavailable for node '{}' and fallback is denied by policy. \
                     Possible causes:\n\
                     - Docker daemon is not running (start: 'systemctl start docker' or open Docker Desktop)\n\
                     - Docker is not installed (install: https://docs.docker.com/get-docker/)\n\
                     - Permission denied (fix: 'sudo usermod -aG docker $USER', then log out/in)\n\
                     \nTo allow fallback to native multiprocess, set docker_fallback_policy to 'Allow' or 'AllowWithWarning'.",
                    node_id
                )))
            }
            DockerFallbackPolicy::Allow => {
                tracing::debug!(
                    "Docker unavailable for node '{}', falling back to native multiprocess execution",
                    node_id
                );
                Ok(false)
            }
            DockerFallbackPolicy::AllowWithWarning => {
                tracing::warn!(
                    "Docker unavailable for node '{}', falling back to native multiprocess execution. \
                     This may result in different behavior than Docker-isolated execution.",
                    node_id
                );
                Ok(false)
            }
        }
    }

    /// Wait for a Python process to signal it's ready via iceoryx2 control channel
    async fn wait_for_ready_signal_ipc(
        &self,
        session_id: &str,
        node_id: &str,
        timeout: std::time::Duration,
    ) -> Result<bool> {
        let start = std::time::Instant::now();
        let control_channel_name = format!("control/{}_{}", session_id, node_id);

        tracing::debug!("Subscribing to control channel: {}", control_channel_name);

        // Create subscriber for the control channel in a blocking task
        let registry = self.channel_registry.clone();
        let channel_name = control_channel_name.clone();
        let node_id_clone = node_id.to_string();
        let session_id_clone = session_id.to_string();
        let control = self.get_control();

        // Control channel was already created in initialize() before spawning Python
        // Now poll for the READY signal - need direct iceoryx2 subscriber (not RuntimeData wrapper)
        let ready = tokio::task::spawn_blocking(move || -> Result<bool> {
            let handle = tokio::runtime::Handle::current();

            // Create raw subscriber for control channel (bypasses RuntimeData deserialization)
            let subscriber = handle.block_on(registry.create_raw_subscriber(&channel_name))?;

            tracing::debug!("Control channel subscriber created, polling for READY signal...");
            let mut poll_count = 0;

            loop {
                // Check timeout
                if start.elapsed() > timeout {
                    tracing::warn!(
                        "Timeout waiting for READY signal from node {} after {} polls",
                        node_id_clone,
                        poll_count
                    );
                    return Ok(false);
                }

                poll_count += 1;
                if poll_count % 100 == 0 {
                    tracing::debug!(
                        "Control channel poll #{} - still waiting for READY",
                        poll_count
                    );
                }

                // Try to receive READY signal (raw bytes, not RuntimeData)
                match subscriber
                    .receive()
                    .map_err(|e| Error::Execution(format!("Receive error: {:?}", e)))?
                {
                    Some(sample) => {
                        let bytes = sample.payload();

                        // Debug: log what we received
                        tracing::debug!(
                            "Received control message #{} - {} bytes: {:?}, as_str: {:?}",
                            poll_count,
                            bytes.len(),
                            bytes,
                            std::str::from_utf8(bytes).unwrap_or("<invalid utf8>")
                        );

                        // Check if it's the READY signal
                        if bytes == b"READY" {
                            tracing::debug!("Node {} signaled READY via iceoryx2", node_id_clone);
                            return Ok(true);
                        } else if bytes.starts_with(b"DEPS:") {
                            // Node is reporting its Python package requirements.
                            // In managed mode these were already pre-discovered and
                            // installed before spawning, so this is informational only.
                            let deps_json = std::str::from_utf8(&bytes[5..]).unwrap_or("[]");
                            tracing::debug!(
                                "Node {} declared dependencies: {}",
                                node_id_clone,
                                deps_json
                            );
                        } else if bytes.starts_with(b"PROGRESS:") {
                            // Node is reporting initialization progress.
                            // Forward to the control bus so the frontend can show it.
                            if let Some(ctrl) = &control {
                                let json_str = std::str::from_utf8(&bytes[9..]).unwrap_or("{}");
                                if let Ok(json) =
                                    serde_json::from_str::<serde_json::Value>(json_str)
                                {
                                    let ts = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .map(|d| d.as_millis() as u64)
                                        .unwrap_or(0);
                                    let status = json
                                        .get("status")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("loading");
                                    let message =
                                        json.get("message").and_then(|v| v.as_str()).unwrap_or("");
                                    tracing::info!(
                                        "[{}] Node {} progress: {} - {}",
                                        session_id_clone,
                                        node_id_clone,
                                        status,
                                        message
                                    );
                                    ctrl.publish_tap(
                                        "__system__",
                                        None,
                                        crate::data::RuntimeData::Json(serde_json::json!({
                                            "kind": "loading",
                                            "status": status,
                                            "node": node_id_clone,
                                            "message": message,
                                            "ts_ms": ts,
                                        })),
                                    );
                                }
                            }
                        } else {
                            tracing::warn!(
                                "Received unexpected control message from node {}: {:?}",
                                node_id_clone,
                                std::str::from_utf8(bytes).unwrap_or("<invalid utf8>")
                            );
                        }
                    }
                    None => {
                        // No data yet, yield to scheduler
                        std::thread::yield_now();
                    }
                }
            }
        })
        .await
        .map_err(|e| Error::Execution(format!("Join error: {}", e)))??;

        Ok(ready)
    }

    /// Spawn a dedicated IPC thread for a node
    /// This thread owns persistent publishers/subscribers and never recreates them
    #[cfg(feature = "multiprocess")]
    async fn spawn_ipc_thread(
        &self,
        node_id: &str,
        session_id: &str,
        input_channel_name: &str,
        output_channel_name: &str,
        control_channel_name: &str,
    ) -> Result<NodeIpcThread> {
        // Create channels for async <-> thread communication
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel::<IpcCommand>(100);
        let (resp_tx, resp_rx) = tokio::sync::mpsc::channel::<IpcResponse>(100);

        let registry = self.channel_registry.clone();
        let input_ch = input_channel_name.to_string();
        let output_ch = output_channel_name.to_string();
        let control_ch = control_channel_name.to_string();
        let node_id_clone = node_id.to_string();
        let session_id_clone = session_id.to_string();

        // Spawn dedicated OS thread
        let handle = std::thread::spawn(move || {
            tracing::debug!("IPC thread starting for node: {}", node_id_clone);

            // Create tokio runtime for this thread (needed for async create_publisher/subscriber)
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to create runtime for IPC thread");

            // Create persistent publishers/subscribers ONCE
            // Use create_subscriber_fresh to drain any stale messages from previous sessions
            let (publisher, subscriber, control_subscriber) = rt.block_on(async {
                let pub_result = registry.create_publisher(&input_ch).await;
                // Use fresh subscriber to drain any stale messages from crashed/uncleaned previous sessions
                let sub_result = registry.create_subscriber_fresh(&output_ch).await;
                // Runtime control-channel subscriber. Lives alongside the
                // init-only subscriber owned by `wait_for_ready_signal_ipc`
                // — both can coexist on the same iceoryx2 service.
                // Used to deliver `PROGRESS:` taps and `PUBLISH:`-routed
                // aux envelopes at runtime; init-time READY/DEPS/PROGRESS
                // are still handled by the wait_for_ready subscriber.
                // Failure here is non-fatal: nodes that never call
                // publish_progress / publish_to_node_port keep working.
                let ctrl_result = registry.create_raw_subscriber(&control_ch).await;
                (pub_result, sub_result, ctrl_result)
            });

            let publisher = match publisher {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!("Failed to create publisher: {}", e);
                    return;
                }
            };

            let subscriber = match subscriber {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Failed to create subscriber: {}", e);
                    return;
                }
            };

            let control_subscriber = match control_subscriber {
                Ok(s) => Some(s),
                Err(e) => {
                    tracing::warn!(
                        "Failed to create runtime control subscriber for node {}: {} \
                         — publish_progress() / publish_to_node_port() will be silently dropped",
                        node_id_clone,
                        e
                    );
                    None
                }
            };

            // CRITICAL: One-time delay for iceoryx2 routing to stabilize
            // std::thread::sleep(std::time::Duration::from_millis(50));
            tracing::debug!(
                "IPC thread ready for node: {} (publishers created)",
                node_id_clone
            );

            // Optional bounded callback for continuous output forwarding.
            // Uses blocking_send from this sync OS thread so a full queue
            // naturally stalls the iceoryx2 subscriber loop below.
            let mut output_callback: Option<tokio::sync::mpsc::Sender<Vec<u8>>> = None;

            // Main loop: process commands using persistent publishers/subscribers
            loop {
                // Check for commands (non-blocking poll)
                match cmd_rx.try_recv() {
                    Ok(IpcCommand::SendData { bytes }) => {
                        // Per-frame hot path: keep tracing at trace!/debug! to avoid
                        // allocating a format-args record per audio frame. Use
                        // `RUST_LOG=remotemedia_core::python::multiprocess=trace` to
                        // re-enable for diagnostics.
                        tracing::trace!(
                            "[IPC Thread] Node '{}': Received SendData command, {} bytes",
                            node_id_clone,
                            bytes.len()
                        );

                        match publisher.send(&bytes) {
                            Ok(_) => {
                                tracing::trace!(
                                    "[IPC Thread] Node '{}': Successfully published data to iceoryx2",
                                    node_id_clone
                                );
                            }
                            Err(e) => {
                                tracing::error!(
                                    "[IPC Thread] Node '{}': Publish failed: {}",
                                    node_id_clone,
                                    e
                                );
                                let _ = resp_tx.blocking_send(IpcResponse::Error(format!(
                                    "Publish failed: {}",
                                    e
                                )));
                                continue;
                            }
                        }

                        // Acknowledge send
                        let _ = resp_tx.blocking_send(IpcResponse::SendComplete);

                        // Continue polling in the main loop - don't break out!
                        // The subscriber will be polled continuously even between commands
                        // to ensure we capture all output from streaming nodes
                    }
                    Ok(IpcCommand::RegisterOutputCallback { callback_tx }) => {
                        tracing::debug!(
                            "IPC thread registered output callback for node: {}",
                            node_id_clone
                        );
                        output_callback = Some(callback_tx);
                    }
                    Ok(IpcCommand::Shutdown) => {
                        tracing::debug!("IPC thread shutting down for node: {}", node_id_clone);
                        break;
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                        // Runtime control-channel drain (PROGRESS:/PUBLISH:).
                        // Polled *before* the data subscriber so a slow data
                        // path doesn't starve out control flow.
                        if let Some(ref ctrl_sub) = control_subscriber {
                            match ctrl_sub.receive() {
                                Ok(Some(sample)) => {
                                    handle_runtime_control_message(
                                        sample.payload(),
                                        &session_id_clone,
                                        &node_id_clone,
                                        &rt,
                                    );
                                    // Loop again — there may be more control
                                    // messages stacked up; we don't want them
                                    // to wait behind a data-subscriber yield.
                                    continue;
                                }
                                Ok(None) => { /* no control message — fall through */ }
                                Err(e) => {
                                    tracing::error!(
                                        "Control receive error for node {}: {}",
                                        node_id_clone,
                                        e
                                    );
                                }
                            }
                        }

                        // No command, poll subscriber for incoming data
                        // Use receive_bytes() for Python IPC — the wire format is not bincode
                        match subscriber.receive_bytes() {
                            Ok(Some(output_data)) => {
                                // If we have a registered callback, use it (for continuous forwarding).
                                // blocking_send applies backpressure: when the downstream
                                // consumer is slow the send blocks this IPC thread, which
                                // stalls `subscriber.receive()` below, which back-pressures
                                // the Python publisher via iceoryx2.
                                if let Some(ref cb) = output_callback {
                                    if let Err(e) = cb.blocking_send(output_data.clone()) {
                                        tracing::error!(
                                            "Failed to send output via callback for node {}: {}",
                                            node_id_clone,
                                            e
                                        );
                                        // Callback channel closed, clear it
                                        output_callback = None;
                                    }
                                }
                                // Also send via response channel (for backwards compat)
                                let _ = resp_tx.blocking_send(IpcResponse::OutputData(output_data));
                            }
                            Ok(None) => {
                                // No data, yield to scheduler (avoids 100% CPU but minimal latency)
                                std::thread::yield_now();
                            }
                            Err(e) => {
                                tracing::error!("Receive error for node {}: {}", node_id_clone, e);
                            }
                        }
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        tracing::debug!("Command channel closed for node: {}", node_id_clone);
                        break;
                    }
                }
            }

            tracing::debug!("IPC thread exited for node: {}", node_id_clone);
        });

        Ok(NodeIpcThread {
            command_tx: cmd_tx,
            response_rx: Arc::new(tokio::sync::Mutex::new(resp_rx)),
            thread_handle: Some(handle),
            node_id: node_id.to_string(),
        })
    }

    /// Terminate a session and cleanup all associated resources
    ///
    /// This method performs comprehensive cleanup in the following order:
    /// 1. Shuts down IPC threads and removes from global sessions storage
    /// 2. Cleans up Docker containers (if Docker support is enabled)
    /// 3. Terminates all Python node processes
    /// 4. Cleans up iceoryx2 IPC channels
    ///
    /// # Arguments
    /// * `session_id` - The unique session identifier to terminate
    ///
    /// # Returns
    /// * `Ok(())` - Session successfully terminated and resources cleaned up
    /// * `Err(Error)` - If session not found or process termination fails
    ///
    /// # Docker Cleanup (T019)
    /// When Docker support is enabled, this method automatically:
    /// - Lists all containers labeled with `remotemedia.session_id=<session_id>`
    /// - Gracefully stops running containers with 5-second timeout
    /// - Forcefully removes containers and associated volumes
    /// - Logs warnings for any containers that fail to clean up (doesn't fail the overall cleanup)
    ///
    /// # IPC Cleanup
    /// The method ensures:
    /// - All IPC threads receive shutdown commands
    /// - Channels are properly destroyed
    /// - Global session storage is updated
    ///
    /// # Error Handling
    /// - Docker cleanup errors are logged as warnings but do not fail the entire session termination
    /// - Process termination errors are propagated as failures
    /// - This ensures robust cleanup even if some resources are unavailable
    pub async fn terminate_session(&self, session_id: &str) -> Result<()> {
        let mut sessions = self.sessions.write().await;

        if let Some(mut session) = sessions.remove(session_id) {
            session.status = SessionStatus::Terminating;

            // Shutdown IPC threads first
            #[cfg(feature = "multiprocess")]
            {
                for (node_id, mut ipc_thread) in session.ipc_threads.drain() {
                    tracing::debug!("Shutting down IPC thread for node: {}", node_id);

                    // Send shutdown command
                    let _ = ipc_thread.command_tx.send(IpcCommand::Shutdown).await;

                    // Wait for thread to exit (with timeout)
                    if let Some(handle) = ipc_thread.thread_handle.take() {
                        std::thread::spawn(move || {
                            let _ = handle.join();
                        });
                    }
                }

                // Remove session from global sessions storage
                let global_sessions = global_sessions();
                let mut global_sessions_guard = global_sessions.write().await;
                global_sessions_guard.remove(session_id);
                tracing::debug!(
                    "Removed session {} from global sessions storage",
                    session_id
                );
                drop(global_sessions_guard);
            }

            // Cleanup Docker containers before terminating processes
            #[cfg(feature = "docker")]
            {
                if let Some(docker_support) = &self.docker_support {
                    tracing::debug!("Cleaning up Docker containers for session: {}", session_id);

                    // Use the built-in cleanup_session_containers which is more efficient
                    // It uses Docker labels to find and clean up all containers for this session
                    match docker_support.cleanup_session_containers(session_id).await {
                        Ok(removed_containers) => {
                            if !removed_containers.is_empty() {
                                tracing::debug!(
                                    "Cleaned up {} Docker containers for session {}",
                                    removed_containers.len(),
                                    session_id
                                );
                                for container_id in removed_containers {
                                    tracing::debug!(
                                        "Removed Docker container: {} from session {}",
                                        container_id,
                                        session_id
                                    );
                                }
                            } else {
                                tracing::debug!(
                                    "No Docker containers found for session {} to clean up",
                                    session_id
                                );
                            }
                        }
                        Err(e) => {
                            // Log warning but don't fail the entire cleanup process
                            // Docker cleanup is important but should not block session termination
                            tracing::warn!(
                                "Failed to clean up Docker containers for session {}: {}. \
                                 This may leave orphaned containers that require manual cleanup.",
                                session_id,
                                e
                            );
                        }
                    }
                }
            }

            // Then terminate processes
            #[cfg(feature = "multiprocess")]
            {
                for (_, process) in session.node_processes.drain() {
                    self.process_manager
                        .terminate_process(process, std::time::Duration::from_secs(5))
                        .await?;
                }

                // CRITICAL: Drain and cleanup channels to prevent stale messages
                // This ensures no old inputs persist in iceoryx2 shared memory
                for (channel_name, channel) in session.channels.drain() {
                    // Drain any pending messages first
                    if let Err(e) = self.channel_registry.drain_channel(&channel_name).await {
                        tracing::warn!(
                            "Failed to drain channel {} during session cleanup: {}",
                            channel_name,
                            e
                        );
                    }

                    // Then destroy the channel (removes service and cleans up shared memory)
                    if let Err(e) = self.channel_registry.destroy_channel(channel).await {
                        tracing::warn!(
                            "Failed to destroy channel {} during session cleanup: {}",
                            channel_name,
                            e
                        );
                    }
                }
            }

            Ok(())
        } else {
            Err(Error::Execution(format!(
                "Session {} not found",
                session_id
            )))
        }
    }

    /// Connect two nodes with a channel for data transfer
    #[cfg(feature = "multiprocess")]
    pub async fn connect_nodes(
        &self,
        session_id: &str,
        from_node: &str,
        to_node: &str,
        channel_name: Option<&str>,
    ) -> Result<()> {
        let mut sessions = self.sessions.write().await;

        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| Error::Execution(format!("Session {} not found", session_id)))?;

        // Generate channel name if not provided
        let channel_name = channel_name
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("{}_to_{}", from_node, to_node));

        // Verify both nodes exist in the session
        if !session.node_processes.contains_key(from_node) {
            return Err(Error::Execution(format!(
                "Source node {} not found in session",
                from_node
            )));
        }
        if !session.node_processes.contains_key(to_node) {
            return Err(Error::Execution(format!(
                "Destination node {} not found in session",
                to_node
            )));
        }

        // Create the channel
        let channel = self
            .channel_registry
            .create_channel(
                &channel_name,
                self.config.channel_capacity,
                self.config.enable_backpressure,
            )
            .await?;

        // Store channel in session
        session.channels.insert(channel_name.clone(), channel);

        tracing::debug!(
            "Connected nodes {} -> {} via channel {}",
            from_node,
            to_node,
            channel_name
        );

        Ok(())
    }

    /// Get channel statistics for a session
    #[cfg(feature = "multiprocess")]
    pub async fn get_channel_stats(
        &self,
        session_id: &str,
        channel_name: &str,
    ) -> Result<super::ipc_channel::ChannelStats> {
        let sessions = self.sessions.read().await;

        let session = sessions
            .get(session_id)
            .ok_or_else(|| Error::Execution(format!("Session {} not found", session_id)))?;

        let channel = session
            .channels
            .get(channel_name)
            .ok_or_else(|| Error::Execution(format!("Channel {} not found", channel_name)))?;

        let stats = channel.stats.read().await;
        Ok(stats.clone())
    }
}

#[async_trait]
impl ExecutorNodeExecutor for MultiprocessExecutor {
    async fn initialize(&mut self, ctx: &ExecutorNodeContext) -> Result<()> {
        tracing::debug!(
            "Initializing multiprocess node: {} ({})",
            ctx.node_id,
            ctx.node_type
        );

        // Store context for later use
        self.current_context = Some(ctx.clone());

        // Get or create session
        let session_id = ctx
            .session_id
            .clone()
            .unwrap_or_else(|| format!("default_{}", ctx.node_id));

        // Ensure session exists
        if !self.sessions.read().await.contains_key(&session_id) {
            self.create_session(session_id.clone()).await?;
        }

        // Clean up stale iceoryx2 state from previous crashed sessions.
        // Orphaned node_monitor / node_monitor_owner_lock files cause
        // InternalError when a new Python process tries to create its
        // IPC node. We clean before creating channels so the runtime
        // starts in a known-good state.
        #[cfg(all(feature = "multiprocess", not(target_os = "android")))]
        {
            ChannelRegistry::cleanup_stale_iceoryx2_state();
        }

        // Determine execution mode from context metadata
        #[cfg(feature = "docker")]
        let use_docker = ctx
            .metadata
            .get("use_docker")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        #[cfg(feature = "docker")]
        let docker_config = if use_docker {
            // Parse Docker configuration from metadata
            ctx.metadata.get("docker_config").and_then(|v| {
                serde_json::from_value::<super::docker_support::DockerNodeConfig>(v.clone()).ok()
            })
        } else {
            None
        };

        // Initialize Docker support if needed and not already initialized
        #[cfg(feature = "docker")]
        if use_docker && self.docker_support.is_none() {
            tracing::debug!("Docker mode requested, initializing Docker support...");
            match DockerSupport::new().await {
                Ok(ds) => {
                    // Additional availability check to ensure Docker daemon is responsive
                    if ds.check_availability().await {
                        tracing::debug!("Docker support initialized and daemon is responsive");
                        self.docker_support = Some(Arc::new(ds));
                    } else {
                        tracing::warn!(
                            "Docker daemon is not responsive. Docker support will not be available."
                        );
                        // Don't set docker_support - will trigger fallback below
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to initialize Docker support: {}. Docker will not be available.",
                        e
                    );
                    // Don't fail immediately - we'll handle this below when checking docker_support
                }
            }
        }

        // Create IPC channels BEFORE spawning process (must exist when Python connects)
        // Prefix with session_id to avoid conflicts and make cleanup easier
        #[cfg(feature = "multiprocess")]
        let (input_channel_name, output_channel_name, input_channel, output_channel) = {
            let input_channel_name = format!("{}_{}_input", session_id, ctx.node_id);
            let output_channel_name = format!("{}_{}_output", session_id, ctx.node_id);
            let control_channel_name = format!("control/{}_{}", session_id, ctx.node_id);

            let input_channel = self
                .channel_registry
                .create_channel(
                    &input_channel_name,
                    self.config.channel_capacity,
                    self.config.enable_backpressure,
                )
                .await?;

            let output_channel = self
                .channel_registry
                .create_channel(
                    &output_channel_name,
                    self.config.channel_capacity,
                    self.config.enable_backpressure,
                )
                .await?;

            // Create control channel BEFORE spawning process to avoid race condition
            // Python will open this channel to send READY signal
            self.channel_registry
                .create_channel(
                    &control_channel_name,
                    10,    // Small capacity for control messages
                    false, // No backpressure for control
                )
                .await?;

            tracing::debug!(
                "Pre-created IPC channels for node {}: {}, {}, {}",
                ctx.node_id,
                input_channel_name,
                output_channel_name,
                control_channel_name
            );

            (
                input_channel_name,
                output_channel_name,
                input_channel,
                output_channel,
            )
        };

        // Resolve Python environment if managed mode is active.
        // This updates the process manager's spawn config with the venv python
        // before any spawn calls below.
        if let Some(ref env_mgr) = self.env_manager {
            // Honor the manifest's python_env (python_version + find_links) for
            // this node. The executor's `PythonEnvManager` is built from the
            // server-wide `MultiprocessConfig.python_env` (runtime.toml), which
            // does NOT carry the manifest's `python_version`. If we used it as-is,
            // the venv would be built with the default interpreter (e.g. 3.11)
            // instead of the manifest's (e.g. 3.12). A version skew there makes
            // `iceoryx2` resolve to an ABI-incompatible build -> the Python
            // subprocess opens the server's shared-memory service with the wrong
            // layout and crashes with `ServiceInCorruptedState`. So override the
            // per-node manager config from the manifest when present.
            let manifest_python_version = ctx
                .params
                .get("__python_version__")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
                .map(String::from);
            let manifest_find_links: Vec<std::path::PathBuf> = ctx
                .params
                .get("__python_find_links__")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str())
                        .map(std::path::PathBuf::from)
                        .collect()
                })
                .unwrap_or_default();

            // Build a per-node manager with the manifest overrides applied on top
            // of the server-wide config. Falls back to the server-wide manager
            // when the manifest omits the keys.
            let node_env_mgr: std::sync::Arc<crate::python::env_manager::PythonEnvManager> = {
                let mut cfg = self.config.python_env.clone();
                let mut overridden = false;
                if let Some(pv) = &manifest_python_version {
                    cfg.python_version = pv.clone();
                    overridden = true;
                }
                if !manifest_find_links.is_empty() {
                    cfg.find_links = manifest_find_links.clone();
                    overridden = true;
                }
                if overridden {
                    match crate::python::env_manager::PythonEnvManager::new(cfg) {
                        Ok(mgr) => std::sync::Arc::new(mgr),
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                node_id = %ctx.node_id,
                                "Failed to build per-node Python env manager from manifest; using server-wide config"
                            );
                            env_mgr.clone()
                        }
                    }
                } else {
                    env_mgr.clone()
                }
            };
            let env_mgr = node_env_mgr;

            // Pre-discover node-declared deps (via @python_requires decorator)
            // by running a lightweight Python probe BEFORE spawning the real process.
            // This ensures the managed venv includes all required packages upfront.
            let node_deps = self.discover_node_deps(&ctx.node_type).await;

            // Collect deps from:
            // 1. Node class __python_requires__ (discovered via probe above)
            // 2. Node params __python_deps__ (injected from manifest's python_deps)
            // 3. Manifest-level extra_deps (via __python_extra_deps__)
            let manifest_deps: Vec<String> = ctx
                .params
                .get("__python_deps__")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            let extra_deps: Vec<String> = ctx
                .params
                .get("__python_extra_deps__")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            tracing::info!(
                node_type = %ctx.node_type,
                node_deps = ?node_deps,
                manifest_deps = ?manifest_deps,
                extra_deps = ?extra_deps,
                "[DEBUG] inputs to merge_deps"
            );

            let merged =
                crate::python::env_manager::merge_deps(&node_deps, &manifest_deps, &extra_deps);

            tracing::info!(
                node_type = %ctx.node_type,
                merged_deps = ?merged,
                "[DEBUG] merged deps about to hit ensure_env"
            );

            let scope_context = ctx
                .params
                .get("__python_env_scope_context__")
                .and_then(|value| value.as_str());

            // A prebuilt venv materialized by the server from an embedded
            // (frozen) wheelhouse short-circuits provisioning entirely: use it
            // as-is, with no venv creation, resolution, or network access.
            let explicit_venv = ctx
                .params
                .get("__python_explicit_venv__")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty());

            let env_result = if let Some(explicit) = explicit_venv {
                let raw = std::path::PathBuf::from(explicit);
                // Accept either the venv root or the python binary inside it.
                let (root, python_executable) = if raw.is_dir() {
                    let rel = if cfg!(windows) {
                        "Scripts/python.exe"
                    } else {
                        "bin/python"
                    };
                    (raw.clone(), raw.join(rel))
                } else {
                    let root = raw
                        .parent()
                        .and_then(|p| p.parent())
                        .map(|p| p.to_path_buf())
                        .unwrap_or_else(|| raw.clone());
                    (root, raw.clone())
                };
                tracing::info!(
                    node_id = %ctx.node_id,
                    python = %python_executable.display(),
                    "Using prebuilt embedded Python environment (no provisioning)"
                );
                Ok(crate::python::env_manager::VenvInfo {
                    path: root,
                    python_executable,
                    cache_key: "embedded-explicit-venv".to_string(),
                })
            } else {
                env_mgr.ensure_env_scoped(&merged, scope_context).await
            };

            match env_result {
                Ok(venv_info) => {
                    tracing::info!(
                        node_id = %ctx.node_id,
                        python = %venv_info.python_executable.display(),
                        cache_key = %venv_info.cache_key,
                        "Using managed Python environment"
                    );
                    // Update spawn config with the resolved venv python
                    let mut spawn_cfg = self.process_manager.spawn_config().write().await;
                    spawn_cfg.python_executable = venv_info.python_executable;

                    // Ensure the remotemedia Python client is importable in the venv.
                    // Add the existing python_path entries (which typically include the
                    // clients/python directory) so the runner module can be found.
                    if spawn_cfg.python_path.is_empty() {
                        // Add default paths: the system PYTHONPATH entries that point
                        // to the remotemedia client package
                        for path in &self.config.python_path {
                            if !spawn_cfg.python_path.contains(path) {
                                spawn_cfg.python_path.push(path.clone());
                            }
                        }
                    }

                    // Auto-include REMOTEMEDIA_PYTHON_SRC on the spawned subprocess's
                    // PYTHONPATH. The managed venv only contains the node's declared
                    // ML deps (transformers, torch, …) from `@python_requires` /
                    // `python_deps` — it does NOT contain the `remotemedia` package
                    // itself, so `python -m remotemedia.core.multiprocessing.runner`
                    // fails with ModuleNotFoundError unless the source root is on
                    // PYTHONPATH. The discover_node_deps probe already does this for
                    // the dep-discovery probe (line ~1347); mirror it here so the
                    // actual runner subprocess gets the same treatment.
                    if let Ok(src) = std::env::var("REMOTEMEDIA_PYTHON_SRC") {
                        let trimmed = src.trim();
                        if !trimmed.is_empty() {
                            let p = std::path::PathBuf::from(trimmed);
                            if !spawn_cfg.python_path.contains(&p) {
                                tracing::info!(
                                    "Auto-prepending REMOTEMEDIA_PYTHON_SRC={} to managed-venv subprocess PYTHONPATH",
                                    trimmed
                                );
                                spawn_cfg.python_path.insert(0, p);
                            }
                        }
                    }

                    // Clear LD_LIBRARY_PATH for managed venvs so that pip-installed
                    // nvidia-* packages (cuDNN, CUDA runtime, etc.) resolve their
                    // bundled native libs without interference from potentially
                    // incompatible system libraries on the host LD_LIBRARY_PATH.
                    if std::env::var("LD_LIBRARY_PATH").is_ok() {
                        tracing::debug!(
                            "Clearing inherited LD_LIBRARY_PATH for managed venv subprocess"
                        );
                        spawn_cfg
                            .env_vars
                            .insert("LD_LIBRARY_PATH".to_string(), String::new());
                    }

                    // Lock out the Python user-site (e.g. ~/.local) for managed-venv
                    // subprocesses. A stale `iceoryx2` in the host user-site (e.g.
                    // 0.9.0) would shadow the venv's pinned `iceoryx2` (0.9.3) on
                    // `import iceoryx2`. Any version skew between the venv's iceoryx2
                    // and the Rust server's iceoryx2 corrupts the shared-memory
                    // service the server pre-creates, surfacing as
                    // `ServiceInCorruptedState` on the node's first channel open.
                    // PYTHONNOUSERSITE=1 makes the venv hermetic so only the pinned
                    // iceoryx2 is ever imported.
                    spawn_cfg
                        .env_vars
                        .insert("PYTHONNOUSERSITE".to_string(), "1".to_string());
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        node_id = %ctx.node_id,
                        "Failed to provision managed Python environment, using system Python"
                    );
                }
            }
        }

        // NOW spawn process or container (channels already exist)
        #[cfg(feature = "multiprocess")]
        {
            let process = {
                #[cfg(feature = "docker")]
                {
                    if use_docker {
                        // Check Docker availability and apply fallback policy
                        let should_use_docker = self.should_use_docker(&ctx.node_id).await?;

                        if should_use_docker {
                            // Docker is available, proceed with container spawn
                            let docker_support = self.docker_support.as_ref().expect(
                                "Docker support should be Some when should_use_docker returns true",
                            );

                            if let Some(docker_config) = docker_config {
                                // Validate Docker configuration
                                docker_config.validate()?;

                                tracing::debug!(
                                    "Spawning Docker container for node '{}' with config: {:?}",
                                    ctx.node_id,
                                    docker_config
                                );

                                // Spawn Docker container
                                self.process_manager
                                    .spawn_docker_container(
                                        &ctx.node_type,
                                        &ctx.node_id,
                                        &ctx.params,
                                        &session_id,
                                        docker_support,
                                        &docker_config,
                                    )
                                    .await?
                            } else {
                                return Err(Error::Execution(
                                    "Docker mode enabled but no docker_config provided".to_string(),
                                ));
                            }
                        } else {
                            // Fallback to regular multiprocess execution
                            tracing::debug!(
                                "Using native multiprocess execution for node '{}' (Docker fallback applied)",
                                ctx.node_id
                            );
                            self.process_manager
                                .spawn_node(&ctx.node_type, &ctx.node_id, &ctx.params, &session_id)
                                .await?
                        }
                    } else {
                        // Regular multiprocess execution (Docker not requested)
                        self.process_manager
                            .spawn_node(&ctx.node_type, &ctx.node_id, &ctx.params, &session_id)
                            .await?
                    }
                }
                #[cfg(not(feature = "docker"))]
                {
                    // Regular multiprocess execution (Docker feature not enabled)
                    self.process_manager
                        .spawn_node(&ctx.node_type, &ctx.node_id, &ctx.params, &session_id)
                        .await?
                }
            };

            // Spawn dedicated IPC thread for this node.
            // The control channel name mirrors the one pre-created above
            // (`control/<session>_<node>`) — used at runtime to drain
            // `publish_progress` / `publish_to_node_port` from Python.
            let control_channel_name = format!("control/{}_{}", session_id, ctx.node_id);
            let ipc_thread = self
                .spawn_ipc_thread(
                    &ctx.node_id,
                    &session_id,
                    &input_channel_name,
                    &output_channel_name,
                    &control_channel_name,
                )
                .await?;

            // Register IPC thread in global sessions storage
            let global_sessions = global_sessions();
            let mut global_sessions_guard = global_sessions.write().await;
            global_sessions_guard
                .entry(session_id.clone())
                .or_insert_with(HashMap::new)
                .insert(ctx.node_id.clone(), ipc_thread.command_tx.clone());
            drop(global_sessions_guard);

            // Add process, channels, and IPC thread to local session
            let mut sessions = self.sessions.write().await;
            if let Some(session) = sessions.get_mut(&session_id) {
                session.node_processes.insert(ctx.node_id.clone(), process);
                session
                    .channels
                    .insert(input_channel_name.clone(), input_channel);
                session
                    .channels
                    .insert(output_channel_name.clone(), output_channel);
                session.ipc_threads.insert(ctx.node_id.clone(), ipc_thread);

                tracing::debug!(
                    "Created IPC thread and channels for node {}: {}, {}",
                    ctx.node_id,
                    input_channel_name,
                    output_channel_name
                );
            }
        }

        // Wait for Python process to signal READY
        #[cfg(feature = "multiprocess")]
        {
            tracing::debug!("Waiting for Python process to signal READY via iceoryx2...");

            // Wait for Python to signal it's ready via iceoryx2 control channel
            // Python creates its input subscriber BEFORE sending READY, so when we receive
            // READY signal, Python is fully prepared to receive data
            // Wait for READY signal with configured timeout (allows time for heavy model loading)
            let ready = self
                .wait_for_ready_signal_ipc(
                    &session_id,
                    &ctx.node_id,
                    std::time::Duration::from_secs(self.config.init_timeout_secs),
                )
                .await?;
            if !ready {
                return Err(Error::Execution(format!(
                    "Node {} failed to signal ready within timeout",
                    ctx.node_id
                )));
            }

            tracing::debug!(
                "✅ Received READY signal - Python subscriber is ready to receive data"
            );

            // Small delay to ensure Python subscriber is fully registered with iceoryx2's routing tables
            // The subscriber creation and READY signal are very close in time, but iceoryx2 needs a moment
            // to complete the internal pub/sub connection registration
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

            tracing::debug!(
                "✅ Node initialization complete, IPC thread ready with persistent publishers"
            );
        }

        Ok(())
    }

    async fn process(&mut self, input: Value) -> Result<Vec<Value>> {
        #[cfg(feature = "multiprocess")]
        {
            // Get current context
            let ctx = self
                .current_context
                .as_ref()
                .ok_or_else(|| Error::Execution("Node not initialized".to_string()))?;

            let _session_id = ctx
                .session_id
                .clone()
                .unwrap_or_else(|| format!("default_{}", ctx.node_id));

            // Send input to node process via IPC
            // TODO: Use session_id for IPC channel routing
            // This is a placeholder - actual IPC implementation will be in ipc_channel.rs
            tracing::debug!("Processing input in multiprocess node: {}", ctx.node_id);

            // For now, just pass through
            Ok(vec![input])
        }

        #[cfg(not(feature = "multiprocess"))]
        {
            Err(Error::Execution(
                "Multiprocess support not enabled".to_string(),
            ))
        }
    }

    async fn cleanup(&mut self) -> Result<()> {
        if let Some(ctx) = &self.current_context {
            let session_id = ctx
                .session_id
                .clone()
                .unwrap_or_else(|| format!("default_{}", ctx.node_id));

            // Terminate the session
            self.terminate_session(&session_id).await?;
        }

        self.current_context = None;
        Ok(())
    }

    fn is_streaming(&self) -> bool {
        // Multiprocess nodes can support streaming
        true
    }

    async fn finish_streaming(&mut self) -> Result<Vec<Value>> {
        // Flush any remaining data from channels
        Ok(Vec::new())
    }
}

// Implement the old nodes::NodeExecutor trait for compatibility with PythonStreamingNode
#[async_trait]
impl NodesNodeExecutor for MultiprocessExecutor {
    async fn initialize(&mut self, ctx: &NodesNodeContext) -> Result<()> {
        // Convert to new NodeContext format
        let executor_ctx = ExecutorNodeContext {
            node_id: ctx.node_id.clone(),
            node_type: ctx.node_type.clone(),
            params: ctx.params.clone(),
            session_id: ctx.session_id.clone(),
            metadata: ctx.metadata.clone(),
        };

        // Delegate to new trait implementation
        ExecutorNodeExecutor::initialize(self, &executor_ctx).await
    }

    async fn process(&mut self, input: Value) -> Result<Vec<Value>> {
        // Delegate to new trait implementation
        ExecutorNodeExecutor::process(self, input).await
    }

    async fn cleanup(&mut self) -> Result<()> {
        // Delegate to new trait implementation
        ExecutorNodeExecutor::cleanup(self).await
    }

    fn is_streaming(&self) -> bool {
        // Multiprocess nodes can support streaming
        ExecutorNodeExecutor::is_streaming(self)
    }

    async fn finish_streaming(&mut self) -> Result<Vec<Value>> {
        // Delegate to new trait implementation
        ExecutorNodeExecutor::finish_streaming(self).await
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_multiprocess_executor_creation() {
        let config = MultiprocessConfig::default();
        let executor = MultiprocessExecutor::new(config);

        // Create a test session
        executor
            .create_session("test_session".to_string())
            .await
            .unwrap();

        // Verify session exists
        let sessions = executor.sessions.read().await;
        assert!(sessions.contains_key("test_session"));

        // Cleanup
        drop(sessions);
        executor.terminate_session("test_session").await.unwrap();
    }

    /// Minimal diagnostic: verify Python can open a channel Rust created.
    /// This is an integration test that depends on a working iceoryx2 IPC
    /// runtime, so it is skipped in CI unless explicitly enabled.
    #[tokio::test]
    async fn test_iceoryx2_python_channel_connect() {
        if std::env::var("REMOTEMEDIA_RUN_IPC_INTEGRATION_TESTS")
            .ok()
            .as_deref()
            != Some("1")
        {
            eprintln!(
                "skipping test_iceoryx2_python_channel_connect; \
                 set REMOTEMEDIA_RUN_IPC_INTEGRATION_TESTS=1 to enable"
            );
            return;
        }

        #[cfg(feature = "multiprocess")]
        {
            use super::super::ipc_channel::ChannelRegistry;
            use std::fs;

            // Check state BEFORE cleanup
            if let Ok(entries) = fs::read_dir("/tmp/iceoryx2/nodes") {
                let nodes: Vec<_> = entries
                    .flatten()
                    .map(|e| e.file_name().to_string_lossy().to_string())
                    .collect();
                eprintln!("[DIAG] Nodes BEFORE cleanup: {:?}", nodes);
            }

            ChannelRegistry::cleanup_stale_iceoryx2_state();

            // Check state AFTER cleanup
            if let Ok(entries) = fs::read_dir("/tmp/iceoryx2/nodes") {
                let nodes: Vec<_> = entries
                    .flatten()
                    .map(|e| e.file_name().to_string_lossy().to_string())
                    .collect();
                eprintln!("[DIAG] Nodes AFTER cleanup: {:?}", nodes);
            }

            let registry = ChannelRegistry::global();
            eprintln!("[DIAG] GLOBAL_REGISTRY initialized");

            let channel_name = "control/diag_test_diag_node";
            registry
                .create_channel(channel_name, 10, false)
                .await
                .expect("Failed to create channel");
            eprintln!("[DIAG] Control channel created: {}", channel_name);

            // Spawn Python child that tries to open the same channel
            let child_code = r#"
import iceoryx2 as iox2
import ctypes

node = iox2.NodeBuilder.new().create(iox2.ServiceType.Ipc)
svc_name = iox2.ServiceName.new("control/diag_test_diag_node")

try:
    service = (
        node.service_builder(svc_name)
        .publish_subscribe(iox2.Slice[ctypes.c_uint8])
        .open()
    )
    print("[DIAG_CHILD] Service opened successfully")
except Exception as e:
    print("[DIAG_CHILD] ERROR:", type(e).__name__, ":", e)
"#;

            let output = std::process::Command::new("python3")
                .arg("-c")
                .arg(child_code)
                .output()
                .expect("Failed to spawn Python");
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);

            eprintln!("[DIAG] Child stdout:\n{}", stdout);
            let start = stderr.len().saturating_sub(500);
            eprintln!(
                "[DIAG] Child stderr (last 500 chars):\n{}",
                &stderr[start..]
            );

            assert!(
                stdout.contains("[DIAG_CHILD] Service opened successfully"),
                "Python child should be able to open the control channel"
            );
        }
    }

    /// Minimal diagnostic: verify Python can open a channel Rust created.
    /// This is an integration test that depends on a working iceoryx2 IPC
    /// runtime, so it is skipped in CI unless explicitly enabled.
    #[tokio::test]
    async fn test_node_initialization() {
        if std::env::var("REMOTEMEDIA_RUN_IPC_INTEGRATION_TESTS")
            .ok()
            .as_deref()
            != Some("1")
        {
            eprintln!(
                "skipping test_node_initialization; \
                 set REMOTEMEDIA_RUN_IPC_INTEGRATION_TESTS=1 to enable"
            );
            return;
        }
        // Clean up stale iceoryx2 state from previous test runs — stale node
        // monitor files and shared memory segments cause InternalError when
        // the Python subprocess tries to create its IPC node.
        #[cfg(feature = "multiprocess")]
        ChannelRegistry::cleanup_stale_iceoryx2_state();

        // Use a shorter timeout for tests (30 seconds instead of 5 minutes)
        let mut config = MultiprocessConfig::default();
        config.init_timeout_secs = 30;

        // Check for PYTHON_EXECUTABLE env var (e.g., for conda environments)
        if let Ok(python_path) = std::env::var("PYTHON_EXECUTABLE") {
            config.python_executable = std::path::PathBuf::from(python_path);
        }

        // Set python_path to the clients/python directory for package discovery
        // This is needed because the spawned subprocess doesn't inherit the dev environment
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let python_client_path = manifest_dir
            .parent() // crates
            .and_then(|p| p.parent()) // remotemedia-sdk
            .map(|p| p.join("clients/python"));
        if let Some(path) = python_client_path {
            if path.exists() {
                config.python_path.push(path);
            }
        }

        // Eagerly initialize the GLOBAL_REGISTRY to see if it creates the node
        let _registry = ChannelRegistry::global();
        eprintln!("[TEST] GLOBAL_REGISTRY initialized");

        let mut executor = MultiprocessExecutor::new(config);

        // Register the test nodes module so test_processor is available
        executor
            .register_module("remotemedia.nodes.multiprocess_test_nodes")
            .await;

        // Use unique session/node IDs to avoid conflicts with other tests
        let session_id = format!("test_node_init_{}", std::process::id());
        let ctx = ExecutorNodeContext {
            node_id: format!("test_node_{}", std::process::id()),
            node_type: "test_processor".to_string(),
            params: serde_json::json!({"param": "value"}),
            session_id: Some(session_id),
            metadata: HashMap::new(),
        };

        // Initialize should create session and prepare for process spawn
        ExecutorNodeExecutor::initialize(&mut executor, &ctx)
            .await
            .unwrap_or_else(|e| {
                eprintln!("[TEST] Initialize failed: {}", e);
                // Give time for any pending tokio tasks to log Python subprocess output
                std::thread::sleep(std::time::Duration::from_secs(1));
                panic!("Initialize failed: {}", e);
            });

        // Cleanup
        ExecutorNodeExecutor::cleanup(&mut executor).await.unwrap();
    }

    #[tokio::test]
    async fn test_session_termination_with_cleanup() {
        let config = MultiprocessConfig::default();
        let executor = MultiprocessExecutor::new(config);

        // Create a test session
        let session_id = "test_cleanup_session";
        executor
            .create_session(session_id.to_string())
            .await
            .expect("Failed to create session");

        // Verify session exists
        {
            let sessions = executor.sessions.read().await;
            assert!(
                sessions.contains_key(session_id),
                "Session should exist after creation"
            );
        }

        // Terminate the session - this should trigger cleanup including Docker containers
        // if Docker support is enabled (graceful handling if not available)
        let result = executor.terminate_session(session_id).await;
        assert!(
            result.is_ok(),
            "Session termination should succeed: {:?}",
            result
        );

        // Verify session is removed
        {
            let sessions = executor.sessions.read().await;
            assert!(
                !sessions.contains_key(session_id),
                "Session should be removed after termination"
            );
        }
    }

    #[tokio::test]
    async fn test_terminate_nonexistent_session() {
        let config = MultiprocessConfig::default();
        let executor = MultiprocessExecutor::new(config);

        // Try to terminate a non-existent session
        let result = executor.terminate_session("nonexistent_session").await;

        // Should return an error indicating session not found
        assert!(
            result.is_err(),
            "Terminating non-existent session should return an error"
        );
        assert!(
            result.unwrap_err().to_string().contains("not found"),
            "Error should indicate session not found"
        );
    }
}
