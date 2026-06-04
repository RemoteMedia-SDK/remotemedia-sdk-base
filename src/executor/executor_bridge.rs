//! Executor Bridge for unified node execution interface
//!
//! Provides a unified interface for executing nodes across different executor types
//! (Native, Multiprocess, WASM) with transparent routing and data conversion.

use crate::executor::node_executor::NodeExecutor;
use crate::{Error, Result};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

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
