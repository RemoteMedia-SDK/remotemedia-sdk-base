//! Host-side streaming-node surface: `InitializeContext`, the registry,
//! and re-exports of the trait declarations now living in
//! `remotemedia-traits`.
//!
//! # Trait moved (Task A4)
//!
//! `StreamingNode`, `SyncStreamingNode`, `AsyncStreamingNode`,
//! `StreamingNodeFactory`, `SyncNodeWrapper`, and `AsyncNodeWrapper`
//! were lifted into `remotemedia-traits` so loadable-node plugins can
//! depend on the trait surface without inheriting the host's heavy
//! transport / control / IPC deps. The historical `crate::nodes::*`
//! paths still work via the re-exports below.
//!
//! # What stays here
//!
//! - [`InitializeContext`] — the concrete struct holding
//!   `Arc<SessionControl>` for the session bus. Plugins read it
//!   through the `InitializeContextRead` trait in `remotemedia-traits`.
//! - [`StreamingNodeRegistry`] — the `HashMap<String,
//!   Arc<dyn StreamingNodeFactory>>` registry the executor consults.
//!   Built from in-tree `inventory::submit!` collectors.
//!
//! # Capability Resolution (spec 023)
//!
//! Nodes can declare their media capabilities via trait methods:
//! - `media_capabilities()` - Returns input/output constraints
//! - `capability_behavior()` - Returns how capabilities are determined
//! - `potential_capabilities()` - For RuntimeDiscovered nodes (Phase 1)
//! - `actual_capabilities()` - For RuntimeDiscovered nodes (Phase 2)

use crate::capabilities::{CapabilityBehavior, MediaCapabilities};
use crate::data::RuntimeData;
use crate::transport::session_control::SessionControl;
use crate::Error;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

// Re-export the trait declarations + plain-data primitives from
// `remotemedia-traits`. Historical `crate::nodes::*` paths keep working.
pub use remotemedia_traits::runtime_context::{InitializeContextRead, NodeRuntimeContextRead};
pub use remotemedia_traits::streaming::{
    AsyncNodeWrapper, AsyncStreamingNode, NodeStatus, PacingNature, StreamingNode,
    StreamingNodeFactory, SyncNodeWrapper, SyncStreamingNode, Tick,
};

/// Context passed to [`StreamingNode::initialize`].
///
/// Gives nodes access to the session's [`SessionControl`] bus so they can
/// emit progress events (e.g. "downloading model", "loading voice") while
/// loading resources. Nodes that don't need progress reporting can ignore
/// this parameter entirely.
#[derive(Clone)]
pub struct InitializeContext {
    /// Session ID for this pipeline run.
    pub session_id: String,
    /// Node ID (from the manifest).
    pub node_id: String,
    /// Optional control bus handle. `None` when the session was created
    /// without a control bus (e.g. unit tests, gRPC unary mode).
    pub control: Option<Arc<SessionControl>>,
}

impl InitializeContext {
    /// Emit a progress event on the control bus.
    ///
    /// Clients subscribed to `__system__.out` receive these as JSON events
    /// with `kind: "loading"`. No-op if `control` is `None`.
    ///
    /// # Arguments
    /// * `status` - One of `"loading_node"`, `"downloading"`, `"loading_model"`, etc.
    /// * `message` - Human-readable description shown in the UI.
    pub fn emit_progress(&self, status: &str, message: &str) {
        if let Some(ctrl) = &self.control {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            ctrl.publish_tap(
                "__system__",
                None,
                RuntimeData::Json(serde_json::json!({
                    "kind": "loading",
                    "status": status,
                    "node": self.node_id,
                    "message": message,
                    "ts_ms": ts,
                })),
            );
        }
    }
}

// R1 fallback: read-trait impl for plugin-side code that holds a
// `&dyn InitializeContextRead` instead of the concrete struct.
impl remotemedia_traits::runtime_context::InitializeContextRead for InitializeContext {
    fn session_id(&self) -> &str {
        &self.session_id
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn emit_progress(&self, status: &str, message: &str) {
        // Forward to the inherent method — same control-bus publish path.
        InitializeContext::emit_progress(self, status, message)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Registry for streaming nodes
#[derive(Clone)]
pub struct StreamingNodeRegistry {
    factories: HashMap<String, Arc<dyn StreamingNodeFactory>>,
}

impl StreamingNodeRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }

    /// Register a streaming node factory
    pub fn register(&mut self, factory: Arc<dyn StreamingNodeFactory>) {
        let node_type = factory.node_type().to_string();
        self.factories.insert(node_type, factory);
    }

    /// Drain the factories from another registry, transferring ownership.
    ///
    /// This is the internal primitive for `merge_from()` — it exists so
    /// factories move rather than clone (avoiding Arc::clone on every node).
    #[doc(hidden)]
    pub(crate) fn drain_factories(
        &mut self,
    ) -> std::collections::hash_map::Drain<'_, String, Arc<dyn StreamingNodeFactory>> {
        self.factories.drain()
    }

    /// Merge the factories from `other` into `self`, consuming `other`.
    ///
    /// Factories from `other` override any existing entries in `self` with
    /// the same node type.
    pub(crate) fn merge_from(&mut self, other: &mut StreamingNodeRegistry) {
        for (k, v) in other.drain_factories() {
            self.factories.insert(k, v);
        }
    }

    /// Create a streaming node by type
    ///
    /// # Arguments
    /// * `node_type` - The type of node to create
    /// * `node_id` - Unique identifier for this node instance
    /// * `params` - Node initialization parameters
    /// * `session_id` - Optional session ID for multiprocess execution
    pub fn create_node(
        &self,
        node_type: &str,
        node_id: String,
        params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let factory = self.factories.get(node_type).ok_or_else(|| {
            Error::Execution(format!(
                "No streaming node factory registered for type '{}'. Available types: {:?}",
                node_type,
                self.list_types()
            ))
        })?;

        factory.create(node_id, params, session_id)
    }

    /// Check if a node type is registered
    pub fn has_node_type(&self, node_type: &str) -> bool {
        self.factories.contains_key(node_type)
    }

    /// Check if a node type is a Python-based node
    pub fn is_python_node(&self, node_type: &str) -> bool {
        self.factories
            .get(node_type)
            .map(|factory| factory.is_python_node())
            .unwrap_or(false)
    }

    /// Check if a node type is a multi-output streaming node
    pub fn is_multi_output_streaming(&self, node_type: &str) -> bool {
        self.factories
            .get(node_type)
            .map(|factory| factory.is_multi_output_streaming())
            .unwrap_or(false)
    }

    /// List all registered node types
    pub fn list_types(&self) -> Vec<String> {
        let mut types: Vec<String> = self.factories.keys().cloned().collect();
        types.sort();
        types
    }

    /// Collect all schemas from registered factories.
    ///
    /// This allows building a schema registry dynamically from factory metadata
    /// rather than maintaining a separate manual registry.
    pub fn collect_schemas(&self) -> Vec<crate::nodes::schema::NodeSchema> {
        self.factories
            .values()
            .filter_map(|factory| factory.schema())
            .collect()
    }

    // =========================================================================
    // Capability Resolution Methods (spec 023)
    // =========================================================================

    /// Get the factory for a node type.
    ///
    /// Used by `CapabilityResolver` to access factory methods during resolution.
    pub fn get_factory(&self, node_type: &str) -> Option<&Arc<dyn StreamingNodeFactory>> {
        self.factories.get(node_type)
    }

    /// Get capability behavior for a node type.
    ///
    /// Returns `Passthrough` if the node type is not registered.
    pub fn get_capability_behavior(&self, node_type: &str) -> CapabilityBehavior {
        self.factories
            .get(node_type)
            .map(|f| f.capability_behavior())
            .unwrap_or(CapabilityBehavior::Passthrough)
    }

    /// Get media capabilities for a node type with params.
    ///
    /// Returns `None` if the node type is not registered or has no declared capabilities.
    pub fn get_media_capabilities(
        &self,
        node_type: &str,
        params: &Value,
    ) -> Option<MediaCapabilities> {
        self.factories
            .get(node_type)
            .and_then(|f| f.media_capabilities(params))
    }

    /// Get potential capabilities for RuntimeDiscovered nodes (Phase 1).
    ///
    /// Returns broad capabilities for early validation before device initialization.
    /// For non-RuntimeDiscovered nodes, returns `media_capabilities(params)`.
    pub fn get_potential_capabilities(
        &self,
        node_type: &str,
        params: &Value,
    ) -> Option<MediaCapabilities> {
        self.factories
            .get(node_type)
            .and_then(|f| f.potential_capabilities(params))
    }
}

impl Default for StreamingNodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod runtime_context_default_tests {
    //! Verifies the default impls of `make_session_state`,
    //! `on_capability_update`, and `capability_subscriptions` on a
    //! minimal `StreamingNode` that only provides the *required* trait
    //! methods. Confirms the lazy-read pattern works for nodes that opt
    //! out of overriding either hook.

    use super::*;
    use crate::capabilities::{
        AudioConstraints, AudioSampleFormat, ConstraintValue, MediaCapabilities, MediaConstraints,
    };
    use crate::nodes::{AnySessionState, NodeRuntimeContext};
    use crate::transport::session_control::SessionControl;
    use dashmap::DashMap;
    use remotemedia_traits::runtime_context::NodeRuntimeContextRead;
    use std::collections::HashMap;
    use std::sync::Arc;

    /// Minimal `StreamingNode` impl that overrides only the required
    /// methods. Inherits both new defaults verbatim.
    struct MinimalNode;

    #[async_trait::async_trait]
    impl StreamingNode for MinimalNode {
        fn node_type(&self) -> &str {
            "MinimalNode"
        }

        async fn process_async(
            &self,
            data: RuntimeData,
            _ctx: &dyn NodeRuntimeContextRead,
        ) -> Result<RuntimeData, Error> {
            Ok(data)
        }

        async fn process_multi_async(
            &self,
            inputs: HashMap<String, RuntimeData>,
            _ctx: &dyn NodeRuntimeContextRead,
        ) -> Result<RuntimeData, Error> {
            inputs
                .into_iter()
                .next()
                .map(|(_, v)| v)
                .ok_or_else(|| Error::Execution("MinimalNode: no input".into()))
        }

        fn is_multi_input(&self) -> bool {
            false
        }
    }

    fn ctx_for(node_id: &str, session_id: &str) -> NodeRuntimeContext {
        NodeRuntimeContext {
            session_id: Arc::from(session_id),
            node_id: Arc::from(node_id),
            control: SessionControl::new(session_id),
            capabilities: Arc::new(DashMap::new()),
            input_snapshots: Arc::new(HashMap::new()),
            session_state: Arc::new(()),
            cancel_gate: Arc::new(crate::transport::CancelGate::new()),
        }
    }

    /// Default `make_session_state` returns `Arc::new(())` and downcasts
    /// successfully to `()` from a `NodeRuntimeContext` populated with
    /// the result.
    #[test]
    fn default_make_session_state_returns_unit() {
        let node = MinimalNode;
        // We can't pass a real `InitializeContext` cheaply (it carries
        // an `Arc<SessionControl>`), so build a minimal one. The
        // default impl ignores the parameter anyway.
        let init_ctx = InitializeContext {
            session_id: "sess-1".into(),
            node_id: "node-x".into(),
            control: Some(SessionControl::new("sess-1")),
        };
        let state: Arc<dyn AnySessionState> = node.make_session_state(&init_ctx);

        // Downcast to () round-trips. We do this by constructing a
        // NodeRuntimeContext and using its `state::<()>()` accessor.
        let mut ctx = ctx_for("node-x", "sess-1");
        ctx.session_state = state;
        let _: Arc<()> = ctx.state();
    }

    /// Default `on_capability_update` writes the published value into
    /// `ctx.capabilities`. A subsequent `ctx.capability(addr)` call
    /// from any other method then returns the latest value.
    #[tokio::test]
    async fn default_on_capability_update_writes_cache() {
        let node = MinimalNode;
        let ctx = ctx_for("node-x", "sess-1");

        assert!(ctx.capability("video:default").is_none());

        let caps = MediaCapabilities::with_input(MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(48_000)),
            channels: Some(ConstraintValue::Exact(1)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        }));

        node.on_capability_update("video:default", caps.clone(), &ctx)
            .await
            .expect("default impl returns Ok");

        let observed = ctx
            .capability("video:default")
            .expect("default impl wrote the cache");
        assert_eq!(format!("{:?}", observed), format!("{:?}", caps));
    }

    /// Re-publishing replaces the cached value. Documents the
    /// retain-latest semantic and confirms the default impl handles
    /// updates, not just first-writes.
    #[tokio::test]
    async fn default_on_capability_update_replaces_cached_value() {
        let node = MinimalNode;
        let ctx = ctx_for("node-x", "sess-1");

        let caps_a = MediaCapabilities::with_input(MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(48_000)),
            channels: Some(ConstraintValue::Exact(1)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        }));
        node.on_capability_update("audio:default", caps_a.clone(), &ctx)
            .await
            .unwrap();

        let caps_b = MediaCapabilities::with_input(MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(16_000)),
            channels: Some(ConstraintValue::Exact(1)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        }));
        node.on_capability_update("audio:default", caps_b.clone(), &ctx)
            .await
            .unwrap();

        let observed = ctx.capability("audio:default").expect("populated");
        assert_eq!(format!("{:?}", observed), format!("{:?}", caps_b));
        assert_ne!(format!("{:?}", observed), format!("{:?}", caps_a));
    }

    /// Default `capability_subscriptions` returns an empty list — a node
    /// that doesn't override it tells the runtime "I don't care about
    /// any transport capabilities" and skips the auto-subscribe wiring.
    #[test]
    fn default_capability_subscriptions_is_empty() {
        let node = MinimalNode;
        assert!(node.capability_subscriptions().is_empty());
    }

    /// End-to-end: publish to `SessionControl`'s capability bus, drain
    /// once via the node's `on_capability_update`, verify
    /// `ctx.capability(addr)` returns the published value. This is the
    /// path the session router runs at session bind: subscribe → drain →
    /// invoke `on_capability_update` → cache populated.
    #[tokio::test]
    async fn capability_bus_to_node_cache_round_trip() {
        let node = MinimalNode;
        let ctx = ctx_for("node-x", "sess-1");

        let caps = MediaCapabilities::with_input(MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(48_000)),
            channels: Some(ConstraintValue::Exact(1)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        }));

        // Subscribe before publishing so the broadcast channel sees the
        // event (matches the session router's wiring order).
        let mut rx = ctx.control.subscribe_capability("audio:default");
        ctx.control
            .publish_capability("audio:default", caps.clone());

        // Drain one event off the bus and run it through the default
        // `on_capability_update` impl, just like the session router's
        // forwarder task does.
        let received = rx.recv().await.expect("publish reaches subscriber");
        node.on_capability_update("audio:default", received, &ctx)
            .await
            .unwrap();

        let observed = ctx.capability("audio:default").expect("cache populated");
        assert_eq!(format!("{:?}", observed), format!("{:?}", caps));
    }

    /// `InitializeContextRead` accessors forward to the host struct's
    /// inherent fields/methods. Plugin code holding only a
    /// `&dyn InitializeContextRead` reads the same `session_id`, `node_id`
    /// the concrete struct exposes, and `emit_progress` is a no-op when
    /// there's no control bus attached.
    #[test]
    fn read_trait_initialize_context_accessors() {
        use remotemedia_traits::runtime_context::InitializeContextRead;
        let init_ctx = InitializeContext {
            session_id: "sess-init".into(),
            node_id: "node-init".into(),
            control: None, // emit_progress is a no-op without a bus
        };
        let dyn_ctx: &dyn InitializeContextRead = &init_ctx;
        assert_eq!(dyn_ctx.session_id(), "sess-init");
        assert_eq!(dyn_ctx.node_id(), "node-init");
        // Should not panic; control is None so this is a no-op.
        dyn_ctx.emit_progress("loading_node", "test message");
    }
}
