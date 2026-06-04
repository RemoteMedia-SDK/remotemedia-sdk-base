//! iceoryx2 Node wrapper for Node.js
//!
//! Provides management of iceoryx2 node instances for IPC.

use napi_derive::napi;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// iceoryx2 Node wrapper for IPC management
///
/// Each Node represents an iceoryx2 participant in the IPC mesh.
/// Nodes are used to create publishers and subscribers.
#[napi]
pub struct IpcNode {
    /// Node name for identification
    name: String,
    /// Whether the node is still valid
    is_valid: Arc<AtomicBool>,
}

#[napi]
impl IpcNode {
    /// Create a new IPC node
    ///
    /// # Arguments
    ///
    /// * `name` - Optional node name (auto-generated if not provided)
    #[napi(constructor)]
    pub fn new(name: Option<String>) -> napi::Result<Self> {
        let node_name = name.unwrap_or_else(|| format!("node_js_{}", std::process::id()));

        // Note: Actual iceoryx2 node creation is deferred to when we need
        // to create publishers/subscribers, because iceoryx2 nodes are !Send
        // and need to live on dedicated threads.

        Ok(Self {
            name: node_name,
            is_valid: Arc::new(AtomicBool::new(true)),
        })
    }

    /// Get the node name
    #[napi(getter)]
    pub fn name(&self) -> String {
        self.name.clone()
    }

    /// Check if the node is still valid
    #[napi(getter)]
    pub fn is_valid(&self) -> bool {
        self.is_valid.load(Ordering::SeqCst)
    }

    /// Close the node and release resources
    #[napi]
    pub fn close(&self) {
        self.is_valid.store(false, Ordering::SeqCst);
    }
}

/// Create a new IPC node
///
/// This is the recommended way to create a node for IPC operations.
#[napi]
pub fn create_ipc_node(name: Option<String>) -> napi::Result<IpcNode> {
    IpcNode::new(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_creation() {
        // Note: This test doesn't actually create iceoryx2 resources
        // because we defer that to publisher/subscriber creation
        let node = IpcNode::new(Some("test_node".to_string())).unwrap();
        assert_eq!(node.name(), "test_node");
        assert!(node.is_valid());

        node.close();
        assert!(!node.is_valid());
    }

    #[test]
    fn test_auto_generated_name() {
        let node = IpcNode::new(None).unwrap();
        assert!(node.name().starts_with("node_js_"));
    }
}
