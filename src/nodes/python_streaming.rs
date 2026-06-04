//! Python StreamingNode wrapper
//!
//! This module provides a wrapper that allows Python nodes to participate
//! in streaming pipelines via the AsyncStreamingNode trait.
//!
//! With multiprocess feature enabled (spec 002), Python AI nodes can run in
//! separate processes with independent GILs for concurrent execution.

use crate::data::RuntimeData;
use crate::nodes::{AsyncStreamingNode, InitializeContextRead, NodeContext, NodeExecutor};
use crate::transport::session_control::SessionControl;
use crate::Error;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Wrapper that adapts a Python node to the AsyncStreamingNode trait
pub struct PythonStreamingNode {
    node_id: String,
    node_type: String,
    params: Value,
    executor: Mutex<Option<Box<dyn NodeExecutor>>>,
    /// Session ID for multiprocess execution (spec 002)
    session_id: Option<String>,
    /// Control bus handle for progress reporting during initialization.
    /// Set by `initialize()` from the `InitializeContext`.
    control: Mutex<Option<Arc<SessionControl>>>,
}

impl PythonStreamingNode {
    /// Create a new Python streaming node
    pub fn new(node_id: String, node_type: &str, params: &Value) -> Result<Self, Error> {
        // Extract session_id from params if present (spec 002)
        let session_id = params
            .get("__session_id__")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        if session_id.is_some() {
            tracing::info!(
                "Python node {} (type: {}) created with multiprocess session: {:?}",
                node_id,
                node_type,
                session_id
            );
        }

        Ok(Self {
            node_id,
            node_type: node_type.to_string(),
            params: params.clone(),
            executor: Mutex::new(None),
            session_id,
            control: Mutex::new(None),
        })
    }

    /// Create with session ID for multiprocess execution (spec 002)
    pub fn with_session(
        node_id: String,
        node_type: &str,
        params: &Value,
        session_id: String,
    ) -> Result<Self, Error> {
        Ok(Self {
            node_id,
            node_type: node_type.to_string(),
            params: params.clone(),
            executor: Mutex::new(None),
            session_id: Some(session_id),
            control: Mutex::new(None),
        })
    }

    /// Create from an existing executor (to reuse cached Python instances)
    pub fn from_executor(
        node_id: String,
        node_type: &str,
        params: &Value,
        executor: Box<dyn NodeExecutor>,
    ) -> Result<Self, Error> {
        Ok(Self {
            node_id,
            node_type: node_type.to_string(),
            params: params.clone(),
            executor: Mutex::new(Some(executor)),
            session_id: None,
            control: Mutex::new(None),
        })
    }

    pub async fn ensure_initialized(&self) -> Result<(), Error> {
        let mut executor_guard = self.executor.lock().await;
        if executor_guard.is_none() {
            // Always use multiprocess executor for all Python nodes (spec 002)
            tracing::debug!(
                "Using MULTIPROCESS execution for Python node {} (type: {})",
                self.node_id,
                self.node_type
            );

            // Load config from runtime.toml or use defaults
            let config = crate::python::multiprocess::MultiprocessConfig::from_default_file()
                .unwrap_or_default();

            let mut py_executor: Box<dyn NodeExecutor> = Box::new(
                crate::python::multiprocess::MultiprocessExecutor::new(config),
            );

            // Pass the control bus handle so progress events from Python
            // nodes during initialization can be forwarded to the frontend.
            if let Some(ctrl) = self.control.lock().await.as_ref() {
                py_executor
                    .as_any_mut()
                    .downcast_mut::<crate::python::multiprocess::MultiprocessExecutor>()
                    .map(|mp| mp.set_control(Arc::clone(ctrl)));
            }

            // Create context for initialization
            let context = NodeContext {
                node_id: self.node_id.clone(),
                node_type: self.node_type.clone(),
                params: self.params.clone(),
                session_id: self.session_id.clone(),
                metadata: std::collections::HashMap::new(),
            };

            // Initialize
            py_executor.initialize(&context).await?;

            *executor_guard = Some(py_executor);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncStreamingNode for PythonStreamingNode {
    fn node_type(&self) -> &str {
        &self.node_type
    }

    async fn initialize(&self, ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        // Store the control bus handle so ensure_initialized can pass it
        // to the MultiprocessExecutor for progress forwarding. Reach
        // through the read trait's `as_any` escape hatch to grab the
        // concrete `Option<Arc<SessionControl>>` field — plugins can't
        // do this and don't need to (they use `emit_progress`); this
        // is in-tree code wiring legacy IPC plumbing.
        if let Some(init) = ctx
            .as_any()
            .downcast_ref::<crate::nodes::streaming_node::InitializeContext>()
        {
            if let Some(ctrl) = &init.control {
                *self.control.lock().await = Some(Arc::clone(ctrl));
            }
        }
        // Delegate to ensure_initialized which loads the Python node
        self.ensure_initialized().await
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        // IMPORTANT: For single-output nodes, use the streaming iteration and take first item
        // This avoids complex GIL deadlock issues with loop.run_until_complete()

        // Ensure the Python node is initialized
        self.ensure_initialized().await?;

        // Get the executor and process through it
        let mut executor_guard = self.executor.lock().await;
        let executor = executor_guard
            .as_mut()
            .ok_or_else(|| Error::Execution("Python node not initialized".to_string()))?;

        // Downcast to MultiprocessExecutor
        let mp_executor = executor
            .as_any_mut()
            .downcast_mut::<crate::python::multiprocess::MultiprocessExecutor>()
            .ok_or_else(|| {
                Error::Execution("Failed to downcast to MultiprocessExecutor".to_string())
            })?;

        // Use streaming iteration but only take the first item
        let mut result: Option<RuntimeData> = None;
        mp_executor
            .process_runtime_data_streaming(data, None, |output| {
                result = Some(output);
                // Return error to stop iteration after first item
                Err(crate::Error::Execution("StopIteration".to_string()))
            })
            .await
            .ok(); // Ignore the error since we use it to stop

        result.ok_or_else(|| Error::Execution("No output from Python node".to_string()))
    }

    // Override the trait's process_streaming method to use true streaming
    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        session_id: Option<String>,
        callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        // Ensure the Python node is initialized
        self.ensure_initialized().await?;

        tracing::info!(
            "[PythonStreamingNode] process_streaming called for node {} (type: {}) session {:?}, data type: {}",
            self.node_id, self.node_type, session_id, data.data_type()
        );

        // Get session_id (from parameter or from node)
        let session_id_opt = session_id.or_else(|| self.session_id.clone());
        tracing::info!(
            "[PythonStreamingNode] Node {}: resolved session_id to {:?}",
            self.node_id,
            session_id_opt
        );

        // Timeline marker: when a JSON intent arrives at the kimodo
        // motion node, publish "motion_intent" so the UI can show that
        // the diffusion request was dispatched. The matching
        // "motion_visible" event would come later from cc_render once
        // the first pose actually drives the avatar — that one's a
        // future addition; this one is enough to bracket the slowest
        // typical bottleneck (LLM tool call → Kimodo daemon RPC).
        if self.node_type == "KimodoMotionNode" && matches!(data, RuntimeData::Json(_)) {
            if let Some(sid) = session_id_opt.as_deref() {
                crate::transport::session_control::publish_timeline_event(
                    sid,
                    "motion_intent",
                    None,
                    &self.node_id,
                );
            }
        }

        // CRITICAL: MultiprocessExecutor methods don't need &mut self anymore (as of the multiprocess redesign)
        // They use Arc/RwLock internally, so we can safely get a shared reference and release the lock
        // This allows concurrent requests to be processed in parallel
        let executor = {
            let mut executor_guard = self.executor.lock().await;
            let executor = executor_guard
                .as_mut()
                .ok_or_else(|| Error::Execution("Python node not initialized".to_string()))?;

            // Downcast to MultiprocessExecutor
            let mp_executor = executor
                .as_any_mut()
                .downcast_mut::<crate::python::multiprocess::MultiprocessExecutor>()
                .ok_or_else(|| {
                    Error::Execution("Failed to downcast to MultiprocessExecutor".to_string())
                })?;

            // Clone the Arc to get a shared reference we can use outside the lock
            // SAFETY: MultiprocessExecutor's process_runtime_data_streaming only needs &self
            // because all mutable state is behind Arc<RwLock<...>>
            mp_executor as *const crate::python::multiprocess::MultiprocessExecutor
        };

        // Release the lock here before calling process_runtime_data_streaming
        // This allows other requests to proceed concurrently

        // Use process_runtime_data_streaming which waits for outputs and invokes callback
        tracing::info!(
            "[PythonStreamingNode] Node {}: calling mp_executor.process_runtime_data_streaming",
            self.node_id
        );
        let result = unsafe {
            // SAFETY: The executor pointer is valid for the lifetime of self because:
            // 1. PythonStreamingNode owns the Box<dyn PythonExecutor> in self.executor
            // 2. The MultiprocessExecutor will not be dropped while self exists
            // 3. process_runtime_data_streaming only needs &self (all state is Arc/RwLock)
            (*executor)
                .process_runtime_data_streaming(data, session_id_opt, callback)
                .await
        };

        tracing::info!("[PythonStreamingNode] Node {}: mp_executor.process_runtime_data_streaming returned {:?}", self.node_id, result);

        tracing::debug!(
            "Node {}: process_runtime_data_streaming returned {:?}",
            self.node_id,
            result.as_ref().map(|_| "Ok").unwrap_or("Err")
        );
        result
    }
}
