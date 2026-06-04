//! Node executor trait and implementations
//!
//! Defines the interface for executing different types of nodes.

use crate::{Error, Result};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

/// Context for node execution
#[derive(Debug, Clone)]
pub struct NodeContext {
    /// Node ID
    pub node_id: String,

    /// Node type
    pub node_type: String,

    /// Configuration parameters
    pub params: Value,

    /// Session ID for stateful execution
    pub session_id: Option<String>,

    /// Session metadata
    pub metadata: HashMap<String, Value>,
}

/// Node executor trait
///
/// All nodes (Rust, Python, WASM) must implement this trait.
#[async_trait]
pub trait NodeExecutor: Send + Sync {
    /// Initialize the node
    async fn initialize(&mut self, ctx: &NodeContext) -> Result<()>;

    /// Process input data
    ///
    /// Returns a vector of output values (supports streaming/batching).
    async fn process(&mut self, input: Value) -> Result<Vec<Value>>;

    /// Cleanup resources
    async fn cleanup(&mut self) -> Result<()>;

    /// Check if this is a streaming node
    fn is_streaming(&self) -> bool {
        false
    }

    /// Finish streaming and flush buffers
    ///
    /// Called after all inputs have been processed for streaming nodes.
    async fn finish_streaming(&mut self) -> Result<Vec<Value>> {
        Ok(Vec::new())
    }
}

/// Rust node executor
///
/// Executes native Rust nodes via dynamic dispatch.
pub struct RustNodeExecutor {
    node_type: String,
    handler: Box<dyn NodeHandler>,
}

/// Handler trait for Rust nodes
#[async_trait]
pub trait NodeHandler: Send + Sync {
    async fn initialize(&mut self, params: &Value) -> Result<()>;
    async fn process(&mut self, input: Value) -> Result<Vec<Value>>;
    async fn cleanup(&mut self) -> Result<()>;
}

impl RustNodeExecutor {
    /// Create a new Rust node executor
    pub fn new(node_type: impl Into<String>, handler: Box<dyn NodeHandler>) -> Self {
        Self {
            node_type: node_type.into(),
            handler,
        }
    }
}

#[async_trait]
impl NodeExecutor for RustNodeExecutor {
    async fn initialize(&mut self, ctx: &NodeContext) -> Result<()> {
        tracing::info!(
            "Initializing Rust node: {} ({})",
            ctx.node_id,
            self.node_type
        );
        self.handler.initialize(&ctx.params).await
    }

    async fn process(&mut self, input: Value) -> Result<Vec<Value>> {
        self.handler.process(input).await
    }

    async fn cleanup(&mut self) -> Result<()> {
        self.handler.cleanup().await
    }
}

/// Python node executor wrapper
///
/// Delegates to MultiprocessExecutor from the python module.
pub struct PythonNodeExecutor {
    #[allow(dead_code)] // Reserved for node identification/logging
    node_type: String,
    #[cfg(feature = "multiprocess")]
    inner: Option<crate::python::multiprocess::MultiprocessExecutor>,
}

impl PythonNodeExecutor {
    /// Create a new Python node executor
    pub fn new(node_type: impl Into<String>) -> Self {
        Self {
            node_type: node_type.into(),
            #[cfg(feature = "multiprocess")]
            inner: None,
        }
    }
}

#[async_trait]
impl NodeExecutor for PythonNodeExecutor {
    async fn initialize(&mut self, ctx: &NodeContext) -> Result<()> {
        #[cfg(feature = "multiprocess")]
        {
            // Load config from runtime.toml or use defaults
            let config = crate::python::multiprocess::MultiprocessConfig::from_default_file()
                .unwrap_or_default();
            let mut executor = crate::python::multiprocess::MultiprocessExecutor::new(config);
            executor.initialize(ctx).await?;
            self.inner = Some(executor);
            Ok(())
        }

        #[cfg(not(feature = "multiprocess"))]
        {
            Err(Error::Execution(
                "Multiprocess support not enabled".to_string(),
            ))
        }
    }

    async fn process(&mut self, input: Value) -> Result<Vec<Value>> {
        #[cfg(feature = "multiprocess")]
        {
            if let Some(ref mut executor) = self.inner {
                executor.process(input).await
            } else {
                Err(Error::Execution("Node not initialized".to_string()))
            }
        }

        #[cfg(not(feature = "multiprocess"))]
        {
            Err(Error::Execution(
                "Multiprocess support not enabled".to_string(),
            ))
        }
    }

    async fn cleanup(&mut self) -> Result<()> {
        #[cfg(feature = "multiprocess")]
        {
            if let Some(ref mut executor) = self.inner {
                executor.cleanup().await?;
            }
            self.inner = None;
            Ok(())
        }

        #[cfg(not(feature = "multiprocess"))]
        {
            Ok(())
        }
    }

    fn is_streaming(&self) -> bool {
        #[cfg(feature = "multiprocess")]
        {
            self.inner
                .as_ref()
                .map(|e| e.is_streaming())
                .unwrap_or(false)
        }

        #[cfg(not(feature = "multiprocess"))]
        {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestHandler {
        multiplier: i64,
    }

    #[async_trait]
    impl NodeHandler for TestHandler {
        async fn initialize(&mut self, params: &Value) -> Result<()> {
            if let Some(mult) = params.get("multiplier").and_then(|v| v.as_i64()) {
                self.multiplier = mult;
            }
            Ok(())
        }

        async fn process(&mut self, input: Value) -> Result<Vec<Value>> {
            if let Some(num) = input.as_i64() {
                Ok(vec![Value::from(num * self.multiplier)])
            } else {
                Ok(vec![input])
            }
        }

        async fn cleanup(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_rust_node_executor() {
        let handler = Box::new(TestHandler { multiplier: 1 });
        let mut executor = RustNodeExecutor::new("test", handler);

        let ctx = NodeContext {
            node_id: "test_node".to_string(),
            node_type: "test".to_string(),
            params: serde_json::json!({"multiplier": 3}),
            session_id: None,
            metadata: HashMap::new(),
        };

        executor.initialize(&ctx).await.unwrap();

        let result = executor.process(Value::from(10)).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], Value::from(30));

        executor.cleanup().await.unwrap();
    }
}
