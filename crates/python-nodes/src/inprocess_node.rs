//! In-process Python node wrapper using PyO3 (Android / opt-in)
//!
//! This module provides a wrapper that allows Python nodes to run in-process
//! via the PyO3 bridge, avoiding the overhead of multiprocess IPC.
//!
//! Requires the `inprocess` feature which enables PyO3 integration.
mod contracts;
#[cfg(test)]
mod tests;

use remotemedia_core::data::RuntimeData;
use remotemedia_core::nodes::{AsyncStreamingNode, InitializeContextRead, NodeRuntimeContextRead};
use remotemedia_core::python::multiprocess::data_transfer;
use remotemedia_core::Error;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::Mutex;

#[cfg(feature = "inprocess")]
use remotemedia_core::executor::executor_bridge::{ExecutorBridge, InProcessExecutorBridge};

/// Wrapper that adapts an in-process Python node to the AsyncStreamingNode trait
#[cfg(feature = "inprocess")]
pub struct InProcessPythonNode {
    node_id: String,
    node_type: String,
    python_class: String,
    params: Value,
    executor: Mutex<Option<InProcessExecutorBridge>>,
    shared_buffers: bool,
    multi_output: bool,
    source_rate_hz: Option<u32>,
    session_id: Option<String>,
    control: Mutex<Option<Arc<remotemedia_core::transport::session_control::SessionControl>>>,
}

#[cfg(feature = "inprocess")]
impl InProcessPythonNode {
    /// Create a new in-process Python node
    pub fn new(node_id: String, python_class: &str, params: &Value) -> Result<Self, Error> {
        Ok(Self {
            node_id,
            node_type: python_class.to_string(),
            python_class: python_class.to_string(),
            params: params.clone(),
            executor: Mutex::new(None),
            shared_buffers: false,
            multi_output: false,
            source_rate_hz: None,
            session_id: None,
            control: Mutex::new(None),
        })
    }

    /// Create with session ID (for compatibility with multiprocess interface)
    pub fn with_session(
        node_id: String,
        python_class: &str,
        params: &Value,
        session_id: String,
    ) -> Result<Self, Error> {
        let mut node = Self::new(node_id, python_class, params)?;
        node.session_id = Some(session_id);
        Ok(node)
    }

    pub async fn ensure_initialized(&self) -> Result<(), Error> {
        let mut executor_guard = self.executor.lock().await;
        if executor_guard.is_none() {
            tracing::debug!(
                "Using IN-PROCESS execution for Python node {} (class: {})",
                self.node_id,
                self.python_class
            );

            // Create the in-process executor bridge
            let mut bridge = InProcessExecutorBridge::new();

            // Pass control bus if available
            if let Some(ctrl) = self.control.lock().await.as_ref() {
                bridge.set_control(Arc::clone(ctrl));
            }

            // Initialize the bridge using initialize_node
            bridge
                .initialize_node(&self.node_id, &self.node_type, &self.params)
                .await?;

            *executor_guard = Some(bridge);
        }
        Ok(())
    }

    async fn initialized_executor(
        &self,
    ) -> Result<tokio::sync::MappedMutexGuard<'_, InProcessExecutorBridge>, Error> {
        self.ensure_initialized().await?;
        tokio::sync::MutexGuard::try_map(self.executor.lock().await, Option::as_mut)
            .map_err(|_| Error::Execution("Python executor unavailable".into()))
    }

    fn validate_call(&self, ctx: &dyn NodeRuntimeContextRead) -> Result<(), Error> {
        if ctx.node_id() != self.node_id
            || self
                .session_id
                .as_deref()
                .is_some_and(|id| id != ctx.session_id())
        {
            return Err(Error::Execution(
                "In-process Python call identity mismatch".into(),
            ));
        }
        if self.shared_buffers {
            if ctx.output_allocator().is_none() {
                return Err(Error::Execution(
                    "Shared in-process Python requires an output allocator".into(),
                ));
            }
            Ok(())
        } else {
            InProcessExecutorBridge::validate_legacy_context(ctx)
        }
    }
}

#[cfg(feature = "inprocess")]
#[async_trait::async_trait]
impl AsyncStreamingNode for InProcessPythonNode {
    fn pacing_nature(&self) -> remotemedia_traits::streaming::PacingNature {
        self.source_rate_hz.map_or(
            remotemedia_traits::streaming::PacingNature::Reactive,
            remotemedia_traits::streaming::PacingNature::SourceWall,
        )
    }

    async fn tick_with_ctx(
        &self,
        tick: remotemedia_traits::streaming::Tick,
        ctx: &dyn NodeRuntimeContextRead,
        callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<(), Error> {
        if self.source_rate_hz.is_none() {
            return Ok(());
        }
        self.validate_call(ctx)?;
        let executor = self.initialized_executor().await?;
        executor
            .execute_tick_shared(&self.node_id, tick, ctx, callback)
            .await?;
        Ok(())
    }

    async fn tick(
        &self,
        _tick: remotemedia_traits::streaming::Tick,
        _session_id: Option<String>,
        _callback: Box<dyn FnMut(RuntimeData) -> Result<(), Error> + Send>,
    ) -> Result<(), Error> {
        if self.source_rate_hz.is_some() {
            return Err(Error::Execution(
                "Shared Python source requires a runtime context".into(),
            ));
        }
        Ok(())
    }

    fn node_type(&self) -> &str {
        &self.node_type
    }

    async fn initialize(&self, ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        // Store the control bus handle
        if let Some(init) = ctx
            .as_any()
            .downcast_ref::<remotemedia_core::nodes::InitializeContext>()
        {
            if let Some(ctrl) = &init.control {
                *self.control.lock().await = Some(Arc::clone(ctrl));
            }
        }
        self.ensure_initialized().await
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        if self.shared_buffers {
            return Err(Error::Execution(
                "Shared in-process Python requires a runtime context".into(),
            ));
        }
        self.ensure_initialized().await?;

        let executor_guard = self.executor.lock().await;
        let Some(executor) = executor_guard.as_ref() else {
            return Err(Error::Execution(
                "InProcessExecutorBridge not initialized".into(),
            ));
        };

        // Serialize input data for the bridge
        let input_bytes = data_transfer::to_bytes(&data)
            .map_err(|e| Error::Execution(format!("Failed to serialize input: {}", e)))?;

        // Execute via the bridge
        let output_bytes = executor
            .execute_node(&self.node_id, &self.node_type, input_bytes, &self.params)
            .await?;

        // Deserialize output
        let output = data_transfer::from_bytes(&output_bytes)
            .map_err(|e| Error::Execution(format!("Failed to deserialize output: {}", e)))?;

        Ok(output)
    }

    async fn process_with_ctx(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        self.validate_call(ctx)?;
        if !self.shared_buffers {
            return self.process(data).await;
        }
        let executor = self.initialized_executor().await?;
        executor.execute_shared(&self.node_id, data, ctx).await
    }

    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        self.validate_call(ctx)?;
        if !self.shared_buffers {
            return self
                .process_streaming(data, Some(ctx.session_id().to_owned()), callback)
                .await;
        }
        if !self.multi_output {
            callback(self.process_with_ctx(data, ctx).await?)?;
            return Ok(1);
        }
        let executor = self.initialized_executor().await?;
        executor
            .execute_streaming_shared(&self.node_id, data, ctx, callback)
            .await
    }

    async fn process_streaming<F>(
        &self,
        data: RuntimeData,
        _session_id: Option<String>,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        if self.shared_buffers {
            return Err(Error::Execution(
                "Shared in-process Python requires a runtime context".into(),
            ));
        }
        self.ensure_initialized().await?;

        let executor_guard = self.executor.lock().await;
        let Some(executor) = executor_guard.as_ref() else {
            return Err(Error::Execution(
                "InProcessExecutorBridge not initialized".into(),
            ));
        };

        // Serialize input data for the bridge
        let input_bytes = data_transfer::to_bytes(&data)
            .map_err(|e| Error::Execution(format!("Failed to serialize input: {}", e)))?;

        // Get streaming output
        let mut stream = executor
            .execute_node_streaming(&self.node_id, &self.node_type, input_bytes, &self.params)
            .await?;

        let mut count = 0;
        while let Some(result) = futures::StreamExt::next(&mut stream).await {
            match result {
                Ok(output) => {
                    callback(output)?;
                    count += 1;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(count)
    }
}

/// Factory for in-process Python nodes
#[cfg(feature = "inprocess")]
pub struct DynamicInProcessPythonNodeFactory {
    config: crate::registry::PythonNodeConfig,
}

#[cfg(feature = "inprocess")]
impl DynamicInProcessPythonNodeFactory {
    pub fn new(config: crate::registry::PythonNodeConfig) -> Self {
        Self { config }
    }
}

#[cfg(feature = "inprocess")]
impl remotemedia_core::nodes::StreamingNodeFactory for DynamicInProcessPythonNodeFactory {
    fn try_buffer_contract(
        &self,
        _params: &Value,
    ) -> Result<Option<remotemedia_traits::buffer_contract::BufferContract>, Error> {
        contracts::inspect(&self.config)
    }

    fn supports_buffer_protocol(
        &self,
        protocol: &remotemedia_traits::buffer_contract::BufferProtocol,
    ) -> bool {
        self.config.execution_mode == crate::registry::PythonExecutionMode::InProcessShared
            && protocol == &contracts::protocol()
    }

    fn create(
        &self,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn remotemedia_core::nodes::StreamingNode>, Error> {
        self.try_buffer_contract(params)?;
        let mut node = if let Some(sid) = session_id {
            InProcessPythonNode::with_session(node_id, &self.config.python_class, params, sid)?
        } else {
            InProcessPythonNode::new(node_id, &self.config.python_class, params)?
        };
        node.shared_buffers =
            self.config.execution_mode == crate::registry::PythonExecutionMode::InProcessShared;
        node.multi_output = self.config.is_multi_output;
        node.source_rate_hz = self.config.shared_source_rate_hz;
        Ok(Box::new(remotemedia_core::nodes::AsyncNodeWrapper(
            Arc::new(node),
        )))
    }

    fn node_type(&self) -> &str {
        &self.config.node_type
    }

    fn is_python_node(&self) -> bool {
        true
    }

    fn is_multi_output_streaming(&self) -> bool {
        self.config.is_multi_output
    }

    fn schema(&self) -> Option<remotemedia_core::nodes::schema::NodeSchema> {
        let mut schema = remotemedia_core::nodes::schema::NodeSchema::new(&self.config.node_type);

        if let Some(ref desc) = self.config.description {
            schema = schema.description(desc);
        }

        if let Some(ref cat) = self.config.category {
            schema = schema.category(cat);
        }

        // Convert string types to RuntimeDataType
        let accepts: Vec<remotemedia_core::nodes::schema::RuntimeDataType> = self
            .config
            .accepts
            .iter()
            .filter_map(|t| match t.as_str() {
                "audio" => Some(remotemedia_core::nodes::schema::RuntimeDataType::Audio),
                "text" => Some(remotemedia_core::nodes::schema::RuntimeDataType::Text),
                "json" => Some(remotemedia_core::nodes::schema::RuntimeDataType::Json),
                "video" => Some(remotemedia_core::nodes::schema::RuntimeDataType::Video),
                "binary" | "bytes" => {
                    Some(remotemedia_core::nodes::schema::RuntimeDataType::Binary)
                }
                "tensor" => Some(remotemedia_core::nodes::schema::RuntimeDataType::Tensor),
                "numpy" => Some(remotemedia_core::nodes::schema::RuntimeDataType::Numpy),
                _ => None,
            })
            .collect();

        let produces: Vec<remotemedia_core::nodes::schema::RuntimeDataType> = self
            .config
            .produces
            .iter()
            .filter_map(|t| match t.as_str() {
                "audio" => Some(remotemedia_core::nodes::schema::RuntimeDataType::Audio),
                "text" => Some(remotemedia_core::nodes::schema::RuntimeDataType::Text),
                "json" => Some(remotemedia_core::nodes::schema::RuntimeDataType::Json),
                "video" => Some(remotemedia_core::nodes::schema::RuntimeDataType::Video),
                "binary" | "bytes" => {
                    Some(remotemedia_core::nodes::schema::RuntimeDataType::Binary)
                }
                "tensor" => Some(remotemedia_core::nodes::schema::RuntimeDataType::Tensor),
                "numpy" => Some(remotemedia_core::nodes::schema::RuntimeDataType::Numpy),
                _ => None,
            })
            .collect();

        if !accepts.is_empty() {
            schema = schema.accepts(accepts);
        }

        if !produces.is_empty() {
            schema = schema.produces(produces);
        }

        Some(schema)
    }
}
