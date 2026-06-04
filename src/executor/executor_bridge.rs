//! Executor Bridge for unified node execution interface
//!
//! Provides a unified interface for executing nodes across different executor types
//! (Native, Multiprocess, WASM) with transparent routing and data conversion.

use crate::executor::node_executor::NodeExecutor;
use crate::{Error, Result};
use async_trait::async_trait;
use futures;
use remotemedia_types::RuntimeData;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[cfg(feature = "inprocess-python")]
use remotemedia_plugin_sdk::PythonNodeHandle;
#[cfg(feature = "inprocess-python")]
use crate::python::multiprocess::data_transfer;

/// Unified interface for node execution across different executor types
#[async_trait]
pub trait ExecutorBridge: Send + Sync {
    /// Execute a node using the appropriate executor
    async fn execute_node(
        &self,
        node_id: &str,
        node_type: &str,
        input_data: Vec<u8>, // RuntimeData serialized
        params: &Value,
    ) -> Result<Vec<u8>>; // RuntimeData serialized

    /// Execute a node with streaming output
    async fn execute_node_streaming(
        &self,
        node_id: &str,
        node_type: &str,
        input_data: Vec<u8>, // RuntimeData serialized
        params: &Value,
    ) -> Result<Box<dyn futures::Stream<Item = Result<RuntimeData>> + Send + Unpin>>;

    /// Initialize a node (for stateful nodes like AI models)
    async fn initialize_node(&self, node_id: &str, node_type: &str, params: &Value) -> Result<()>;

    /// Cleanup node resources
    async fn cleanup_node(&self, node_id: &str) -> Result<()>;

    /// Get executor type name for this bridge
    fn executor_type_name(&self) -> &str;
}

/// Native executor bridge (Rust nodes in same process)
pub struct NativeExecutorBridge {
    /// Reference to the main executor
    #[allow(dead_code)] // Reserved for accessing executor features in future phases
    executor: Arc<crate::executor::Executor>,

    /// Active node instances for this bridge
    node_instances: Arc<RwLock<HashMap<String, Box<dyn NodeExecutor>>>>,
}

impl NativeExecutorBridge {
    /// Create a new native executor bridge
    pub fn new(executor: Arc<crate::executor::Executor>) -> Self {
        Self {
            executor,
            node_instances: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl ExecutorBridge for NativeExecutorBridge {
    async fn execute_node(
        &self,
        node_id: &str,
        _node_type: &str,
        input_data: Vec<u8>,
        _params: &Value,
    ) -> Result<Vec<u8>> {
        // For MVP: Simply pass through data
        // Full implementation in later phases will use actual node execution
        tracing::debug!("NativeExecutorBridge: Executing node {}", node_id);

        // Native nodes would process the data here
        // For MVP, we're focusing on multiprocess routing, so native nodes
        // will continue using the existing execution path

        Ok(input_data)
    }

    async fn execute_node_streaming(
        &self,
        _node_id: &str,
        _node_type: &str,
        _input_data: Vec<u8>,
        _params: &Value,
    ) -> Result<Box<dyn futures::Stream<Item = Result<RuntimeData>> + Send + Unpin>> {
        Err(Error::Execution("NativeExecutorBridge streaming not implemented".into()))
    }

    async fn initialize_node(&self, node_id: &str, node_type: &str, _params: &Value) -> Result<()> {
        tracing::info!(
            "NativeExecutorBridge: Initializing node {} (type: {})",
            node_id,
            node_type
        );

        // For native nodes, initialization happens per-execution
        // TODO: Use params for node configuration once persistent state is implemented
        // No persistent state needed for the MVP
        Ok(())
    }

    async fn cleanup_node(&self, node_id: &str) -> Result<()> {
        tracing::debug!("NativeExecutorBridge: Cleaning up node {}", node_id);

        // Remove node instance if it exists
        let mut instances = self.node_instances.write().await;
        instances.remove(node_id);

        Ok(())
    }

    fn executor_type_name(&self) -> &str {
        "native"
    }
}

/// Multiprocess executor bridge (Python nodes in separate processes)
#[cfg(feature = "multiprocess")]
pub struct MultiprocessExecutorBridge {
    /// Reference to multiprocess executor
    executor: Arc<crate::python::multiprocess::MultiprocessExecutor>,

    /// Session ID for this bridge
    session_id: String,
}

#[cfg(feature = "multiprocess")]
impl MultiprocessExecutorBridge {
    /// Create a new multiprocess executor bridge
    pub fn new(
        executor: Arc<crate::python::multiprocess::MultiprocessExecutor>,
        session_id: String,
    ) -> Self {
        Self {
            executor,
            session_id,
        }
    }
}

#[cfg(feature = "multiprocess")]
#[async_trait]
impl ExecutorBridge for MultiprocessExecutorBridge {
    async fn execute_node(
        &self,
        node_id: &str,
        node_type: &str,
        input_data: Vec<u8>,
        _params: &Value, // Reserved for runtime parameter overrides
    ) -> Result<Vec<u8>> {
        tracing::debug!(
            "MultiprocessExecutorBridge: Executing node {} (type: {}) in session {}",
            node_id,
            node_type,
            self.session_id
        );

        // For MVP: The multiprocess executor handles the actual execution
        // TODO: Use params for runtime configuration in Phase 4 (US2)
        // Data conversion between executors will be added in Phase 4 (US2)
        // For now, we verify the node can be executed via multiprocess

        // Return input data (actual execution happens through existing pipeline)
        Ok(input_data)
    }

    async fn execute_node_streaming(
        &self,
        _node_id: &str,
        _node_type: &str,
        _input_data: Vec<u8>,
        _params: &Value,
    ) -> Result<Box<dyn futures::Stream<Item = Result<RuntimeData>> + Send + Unpin>> {
        Err(Error::Execution("MultiprocessExecutorBridge streaming not implemented".into()))
    }

    async fn initialize_node(&self, node_id: &str, node_type: &str, _params: &Value) -> Result<()> {
        tracing::info!(
            "MultiprocessExecutorBridge: Initializing Python node {} (type: {}) in session {}",
            node_id,
            node_type,
            self.session_id
        );

        // TODO: Pass params to multiprocess executor for node configuration
        // Update init progress to track initialization
        // The actual process spawning happens when the pipeline executes
        // This is because MultiprocessExecutor::initialize requires &mut self
        // but we're working with Arc<MultiprocessExecutor>
        self.executor
            .update_init_progress(
                &self.session_id,
                node_id,
                crate::python::multiprocess::InitStatus::Starting,
                0.0,
                format!("Initializing {} node", node_type),
            )
            .await
            .map_err(|e| Error::Execution(format!("Failed to track init progress: {}", e)))?;

        tracing::info!(
            "MultiprocessExecutorBridge: Node {} initialization tracked",
            node_id
        );
        Ok(())
    }

    async fn cleanup_node(&self, node_id: &str) -> Result<()> {
        tracing::debug!(
            "MultiprocessExecutorBridge: Cleaning up node {} in session {}",
            node_id,
            self.session_id
        );

        // Cleanup handled by session termination in multiprocess executor
        Ok(())
    }

    fn executor_type_name(&self) -> &str {
        "multiprocess"
    }
}

/// In-process executor bridge (Python nodes via PyO3 in same process)
/// Used on Android and opt-in via REMOTEMEDIA_EXECUTION_STRATEGY=inprocess
#[cfg(feature = "inprocess-python")]
pub struct InProcessExecutorBridge {
    /// Loaded Python node handles
    node_handles: Arc<RwLock<HashMap<String, PythonNodeHandle>>>,
}

#[cfg(feature = "inprocess-python")]
impl InProcessExecutorBridge {
    /// Create a new in-process executor bridge
    pub fn new() -> Self {
        Self {
            node_handles: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[cfg(feature = "inprocess-python")]
#[async_trait]
impl ExecutorBridge for InProcessExecutorBridge {
    async fn execute_node(
        &self,
        node_id: &str,
        node_type: &str,
        input_data: Vec<u8>,
        _params: &Value,
    ) -> Result<Vec<u8>> {
        tracing::debug!(
            "InProcessExecutorBridge: Executing node {} (type: {})",
            node_id,
            node_type
        );

        // Get the Python node handle
        let handles = self.node_handles.read().await;
        let handle = handles
            .get(node_id)
            .ok_or_else(|| Error::Execution(format!("Python node not loaded: {}", node_id)))?;

        // Deserialize input data
        let runtime_data = data_transfer::from_bytes(&input_data)
            .map_err(|e| Error::Execution(format!("Failed to deserialize input: {}", e)))?;

        // Execute the Python node
        let output_data = handle.process(&runtime_data)
            .map_err(|e| Error::Execution(format!("Python node process failed: {:?}", e)))?;

        // Serialize output
        let output_bytes = data_transfer::to_bytes(&output_data)
            .map_err(|e| Error::Execution(format!("Failed to serialize output: {}", e)))?;

        Ok(output_bytes)
    }

    async fn initialize_node(&self, node_id: &str, node_type: &str, params: &Value) -> Result<()> {
        tracing::info!(
            "InProcessExecutorBridge: Initializing Python node {} (type: {})",
            node_id,
            node_type
        );

        // Parse config from params
        let config: HashMap<String, serde_json::Value> = serde_json::from_value(params.clone())
            .map_err(|e| Error::Execution(format!("Failed to parse config: {}", e)))?;

        // For in-process, we need to specify the module and class based on node_type
        // This maps node type to Python module/class - configurable via params
        let (module_path, class_name) = Self::resolve_python_plugin(node_type);

        // Load and initialize the Python plugin
        let handle = PythonNodeHandle::load(&module_path, &class_name)
            .map_err(|e| Error::Execution(format!("Failed to load Python plugin: {:?}", e)))?;

        handle.initialize(&config)
            .map_err(|e| Error::Execution(format!("Failed to initialize Python plugin: {:?}", e)))?;

        // Store the handle
        let mut handles = self.node_handles.write().await;
        handles.insert(node_id.to_string(), handle);

        Ok(())
    }

    async fn cleanup_node(&self, node_id: &str) -> Result<()> {
        tracing::debug!(
            "InProcessExecutorBridge: Cleaning up node {}",
            node_id
        );

        let mut handles = self.node_handles.write().await;
        if let Some(handle) = handles.remove(node_id) {
            handle.finalize()
                .map_err(|e| Error::Execution(format!("Failed to finalize Python plugin: {:?}", e)))?;
        }

        Ok(())
    }

    fn executor_type_name(&self) -> &str {
        "inprocess"
    }

    async fn execute_node_streaming(
        &self,
        node_id: &str,
        node_type: &str,
        input_data: Vec<u8>,
        _params: &Value,
    ) -> Result<Box<dyn futures::Stream<Item = Result<RuntimeData>> + Send + Unpin>> {
        tracing::debug!(
            "InProcessExecutorBridge: Streaming execution for node {} (type: {})",
            node_id,
            node_type
        );

        // Get the Python node handle
        let handles = self.node_handles.read().await;
        let handle = handles
            .get(node_id)
            .ok_or_else(|| Error::Execution(format!("Python node not loaded: {}", node_id)))?;

        // Deserialize input data
        let runtime_data = data_transfer::from_bytes(&input_data)
            .map_err(|e| Error::Execution(format!("Failed to deserialize input: {}", e)))?;

        // Execute streaming
        let outputs = handle.process_streaming(&runtime_data)
            .map_err(|e| Error::Execution(format!("Python node streaming failed: {:?}", e)))?;

        // Convert to stream
        let stream = futures::stream::iter(outputs.into_iter().map(|o| Ok::<RuntimeData, Error>(o)));
        Ok(Box::new(stream))
    }
}

#[cfg(feature = "inprocess-python")]
impl InProcessExecutorBridge {
    /// Set control bus for progress reporting during initialization
    pub fn set_control(&mut self, _control: Arc<crate::transport::session_control::SessionControl>) {
        // Control bus integration would go here if needed
        // For now, we don't use it for in-process execution since
        // there's no separate process to send progress from
    }

    /// Resolve node type to Python module and class
    /// This can be extended to read from params for full configurability
    fn resolve_python_plugin(node_type: &str) -> (String, String) {
        match node_type {
            "python_whisper" => ("remotemedia_nodes.whisper".to_string(), "WhisperSTTNode".to_string()),
            "python_llm" => ("remotemedia_nodes.llm".to_string(), "LLMNode".to_string()),
            "python_tts" => ("remotemedia_nodes.tts".to_string(), "TTSNode".to_string()),
            _ => {
                // Default: infer from node_type
                let module = format!("remotemedia_nodes.{}", node_type);
                let class = node_type
                    .split('_')
                    .map(|s| {
                        let mut c = s.chars();
                        match c.next() {
                            None => String::new(),
                            Some(first) => first.to_uppercase().collect::<String>() + c.as_str(),
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("");
                (module, class + "Node")
            }
        }
    }
}