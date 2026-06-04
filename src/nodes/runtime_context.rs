//! Per-call runtime context for node invocations.
//!
//! `NodeRuntimeContext` is the per-call companion to
//! [`InitializeContext`](crate::nodes::streaming_node::InitializeContext).
//! Whereas `InitializeContext` is handed to a node once at session bind
//! (when the node loads models or allocates resources),
//! `NodeRuntimeContext` is handed to the node on **every** per-call
//! invocation: `process_streaming`, `tick`, `on_capability_update`, and
//! `terminate_*` lifecycle hooks.
//!
//! # Three concentric scopes
//!
//! A node has access to three storage scopes via this context:
//!
//! | Scope | Lifetime | Created by | Use for |
//! |---|---|---|---|
//! | Process-global (`OnceLock`/static) | Process | Factory at first init | Loaded models, GPU contexts, embeddings DB |
//! | Per-(node, session) | Session | `make_session_state` | Shared session state across peers |
//! | Per-(node, session, peer) | Peer connection | `make_peer_state` (follow-up plan) | Conversation history, voice settings |
//!
//! Today this module ships the first two scopes. The third
//! (`peer_states`) lands with the `plan-multi-peer` follow-up.
//!
//! # Lifecycle
//!
//! - **Construction**: the session router constructs one
//!   `NodeRuntimeContext` per `(node instance, session)` pair when a
//!   session binds, holding it on the session for the lifetime of that
//!   session.
//! - **Cloning**: `NodeRuntimeContext` is `Clone`. All field clones are
//!   `Arc::clone`s, so cloning is cheap and safe to spawn into tasks.
//! - **Destruction**: when the session terminates, the router drops the
//!   per-(node, session) context, which drops the `Arc<dyn AnySessionState>`
//!   and lets the node's per-session resources free.
//!
//! # Capability cache
//!
//! The default
//! [`StreamingNode::on_capability_update`](crate::nodes::streaming_node::StreamingNode::on_capability_update)
//! impl writes published capabilities into `ctx.capabilities`. A node
//! that doesn't override the hook can still read the latest negotiated
//! values from any of its methods via [`Self::capability`].
//!
//! # Per-session state
//!
//! Nodes that need session-isolated state (e.g.
//! `LlamaCppGenerationNode`'s `ChatState`) override
//! `StreamingNode::make_session_state` to return a typed `Arc`. The
//! runtime stores it as `Arc<dyn AnySessionState>` and the node
//! downcasts on access via [`Self::state`].

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;

use crate::capabilities::MediaCapabilities;
use crate::transport::cancel_gate::CancelGate;
use crate::transport::session_control::SessionControl;

// `AnySessionState` moved to `remotemedia-traits` in Task A4 — re-export
// to keep the historical `crate::nodes::AnySessionState` path working.
pub use remotemedia_traits::AnySessionState;

// R1 fallback: read-trait surface plus the typed free helpers
// (`runtime_context::snapshot::<T>`, `state::<S>`, `try_state::<S>`)
// live in the traits crate. The struct in this module implements the
// read trait; plugin-side code calls through the trait + helpers
// without depending on the concrete struct's heavy fields.
use remotemedia_traits::runtime_context::NodeRuntimeContextRead;

// Snapshot port primitives live in `crate::nodes::ports`; re-exported
// from `crate::nodes` so `ctx.input_snapshots` consumers don't have to
// reach across module boundaries.
pub use crate::nodes::ports::SnapshotPort;

/// Per-call runtime context handed to every per-call `StreamingNode`
/// method.
///
/// Constructed once per `(node instance, session)` at session bind and
/// re-handed to every invocation. Cheap to clone (all fields are `Arc`).
///
/// See the module-level docs for lifecycle, scopes, and lifecycle.
#[derive(Clone)]
pub struct NodeRuntimeContext {
    /// Pipeline session this call belongs to. Stored as `Arc<str>`
    /// because we frequently clone it into spawned tasks and across
    /// thread boundaries.
    pub session_id: Arc<str>,

    /// Node id (from manifest). Same `Arc<str>` rationale as
    /// `session_id`.
    pub node_id: Arc<str>,

    /// Session control bus. **Same instance** is shared by every node
    /// in the same session; `Arc::ptr_eq` would return `true` for two
    /// nodes' contexts within one session.
    ///
    /// Replaces the current pattern of nodes hoarding
    /// `Arc<SessionControl>` from `InitializeContext` in their own
    /// state. Nodes are free to use `ctx.control` directly from any
    /// per-call method.
    pub control: Arc<SessionControl>,

    /// Per-(node, session) capability cache. The default
    /// `StreamingNode::on_capability_update` impl writes here. Nodes
    /// read on demand via [`Self::capability`].
    ///
    /// Address keys follow the `<medium>:<stream_id>` convention
    /// (e.g. `"video:default"`, `"audio:default"`). Population happens
    /// when the WebRTC transport publishes negotiated capabilities to
    /// the session control bus and the runtime auto-subscribes
    /// connected nodes.
    pub capabilities: Arc<DashMap<String, MediaCapabilities>>,

    /// Read handles to upstream snapshot ports keyed by input port
    /// name. **Empty until the pacing/snapshot PRs land** — this field
    /// is published now so the type signature of per-call methods is
    /// stable across the upcoming pacing migration.
    pub input_snapshots: Arc<HashMap<String, Arc<dyn SnapshotPort>>>,

    /// Per-(node, session) opaque state slot. The mechanism that lets
    /// one node instance serve multiple concurrent sessions with
    /// isolated state. Constructed once per session bind by
    /// `StreamingNode::make_session_state`; downcast by the node via
    /// [`Self::state`].
    ///
    /// Stateless nodes get an `Arc::new(())` from the default factory
    /// impl and never call `state()`.
    pub session_state: Arc<dyn AnySessionState>,

    /// Per-(node, session) cancellation primitive backing the runtime's
    /// barge-in / shutdown signal.
    ///
    /// The session router holds the same `Arc<CancelGate>` and fires
    /// `try_cancel()` on it when a `barge_in` envelope reaches this
    /// node. Nodes that want to suppress cancellation for a window
    /// (e.g. while a non-cancelable LLM tool call is mid-flight) call
    /// [`CancelGate::protect`] to take a `ProtectGuard`; while any
    /// guard is held, `try_cancel` is a no-op.
    ///
    /// Most nodes never touch this field — they're cancelled by the
    /// universal future-drop mechanism the router applies. Only nodes
    /// that own irreversible work mid-call (chat backend tool
    /// dispatch, partial commits, etc.) should take a guard.
    pub cancel_gate: Arc<CancelGate>,
}

impl NodeRuntimeContext {
    /// Construct a context for a (node, session) bind.
    ///
    /// Capabilities cache and snapshot port handles are initialised empty;
    /// they are populated by the runtime as the WebRTC transport publishes
    /// negotiated capabilities and the pacing layer wires up snapshot ports.
    ///
    /// `session_state` is whatever the node's
    /// [`crate::nodes::StreamingNode::make_session_state`] returned for this
    /// session — `Arc::new(())` for stateless nodes (the default).
    pub fn for_session(
        session_id: impl Into<Arc<str>>,
        node_id: impl Into<Arc<str>>,
        control: Arc<SessionControl>,
        session_state: Arc<dyn AnySessionState>,
    ) -> Self {
        Self::with_input_snapshots(session_id, node_id, control, session_state, HashMap::new())
    }

    /// Construct a context for a (node, session) bind with pre-wired
    /// snapshot input ports.
    ///
    /// Same as [`Self::for_session`] but populates `input_snapshots` with
    /// the read handles the session router resolved from the manifest's
    /// snapshot connections at session start. The consumer node reads
    /// each port via `remotemedia_traits::runtime_context::snapshot::<T>(ctx, port_name)`.
    pub fn with_input_snapshots(
        session_id: impl Into<Arc<str>>,
        node_id: impl Into<Arc<str>>,
        control: Arc<SessionControl>,
        session_state: Arc<dyn AnySessionState>,
        input_snapshots: HashMap<String, Arc<dyn SnapshotPort>>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            node_id: node_id.into(),
            control,
            capabilities: Arc::new(DashMap::new()),
            input_snapshots: Arc::new(input_snapshots),
            session_state,
            cancel_gate: Arc::new(CancelGate::new()),
        }
    }

    /// Builder: replace the per-call cancel gate.
    ///
    /// The session router uses this to install the same `Arc<CancelGate>`
    /// it owns into every node-context for the session, so a barge
    /// envelope arriving at the filter task cancels the right
    /// dispatch future.
    pub fn with_cancel_gate(mut self, cancel_gate: Arc<CancelGate>) -> Self {
        self.cancel_gate = cancel_gate;
        self
    }

    /// Convenience constructor for tests, executor unit-tests, and
    /// non-streaming call sites that don't yet have a real
    /// `Arc<SessionControl>` and don't need session-isolated state.
    ///
    /// Builds an empty `SessionControl` keyed to `session_id`, an empty
    /// capability cache, no snapshot ports, and `Arc::new(())` for
    /// session_state. Equivalent to what an `AsyncNodeWrapper` /
    /// `SyncNodeWrapper` legacy node would observe via the bridge anyway.
    pub fn for_test(session_id: impl Into<Arc<str>>, node_id: impl Into<Arc<str>>) -> Self {
        let session_id: Arc<str> = session_id.into();
        let control = SessionControl::new(session_id.as_ref());
        Self::for_session(session_id, node_id, control, Arc::new(()))
    }

    /// Read the latest published capability for `addr`, or `None` if no
    /// publisher has emitted yet.
    ///
    /// Atomic load against the per-(node, session) capability cache.
    /// Never blocks. Safe to call from any node method.
    pub fn capability(&self, addr: &str) -> Option<MediaCapabilities> {
        self.capabilities.get(addr).map(|r| r.value().clone())
    }

    /// Inherent accessor matching `NodeRuntimeContextRead::session_id`.
    ///
    /// Lets in-tree call sites that have a concrete `&NodeRuntimeContext`
    /// use the same `ctx.session_id()` method-call form that trait-bound
    /// generic / `&dyn` code uses. The field is also still public.
    #[inline]
    pub fn session_id(&self) -> &str {
        self.session_id.as_ref()
    }

    /// Inherent accessor matching `NodeRuntimeContextRead::node_id`.
    #[inline]
    pub fn node_id(&self) -> &str {
        self.node_id.as_ref()
    }

    /// Read the latest snapshot on the named input port, downcast to
    /// the consumer's expected type.
    ///
    /// Atomic load against the producer's snapshot slot — never blocks.
    /// Returns `None` when:
    ///   - the port name isn't registered for this node, OR
    ///   - the producer hasn't published anything yet, OR
    ///   - the consumer's `T` doesn't match the producer's type
    ///     (silent type mismatch — the runtime can't enforce typed
    ///     wiring at session-bind time without a schema, so this is a
    ///     safety net rather than a panic).
    ///
    /// `T: Clone` because snapshots are deep-copied on read; this is
    /// usually fine because snapshot payloads are small (blendshape
    /// weight vectors, idle pose tuples). If your snapshot is large,
    /// box it: `OutputPort<Arc<MyBigStruct>>` then clone the `Arc`.
    pub fn snapshot<T>(&self, port: &str) -> Option<crate::nodes::ports::TimestampedSnapshot<T>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let handle = self.input_snapshots.get(port)?;
        let typed: &crate::nodes::ports::InputPort<T> = handle.as_any().downcast_ref()?;
        typed.snapshot()
    }

    /// Downcast the per-session state to the node's expected type.
    ///
    /// Returns `None` when the runtime constructed the context with a
    /// different state type (e.g. `Arc::new(())` for `for_test` paths
    /// that don't yet invoke `make_session_state`). Use [`Self::state`]
    /// when the caller guarantees the right type and wants a panic on
    /// mismatch.
    pub fn try_state<S>(&self) -> Option<Arc<S>>
    where
        S: AnySessionState,
    {
        downcast_session_state::<S>(Arc::clone(&self.session_state))
    }

    /// Downcast the per-session state to the node's expected type.
    ///
    /// Returns the same `Arc<S>` allocation across multiple calls for
    /// the same context (verifiable via `Arc::ptr_eq`).
    ///
    /// # Panics
    ///
    /// Panics with a descriptive message that names the requested
    /// type, the node id, and the session id when the stored state's
    /// runtime type does not match `S`. This is a programming bug —
    /// either `make_session_state` returned the wrong type or the
    /// caller passed the wrong type parameter — and silent `None` would
    /// hide it.
    pub fn state<S>(&self) -> Arc<S>
    where
        S: AnySessionState,
    {
        downcast_session_state::<S>(Arc::clone(&self.session_state)).unwrap_or_else(|| {
            panic!(
                "NodeRuntimeContext::state::<{type_name}>(): wrong type for node {node} \
                 session {session} — check make_session_state on the factory",
                type_name = std::any::type_name::<S>(),
                node = &self.node_id,
                session = &self.session_id,
            )
        })
    }
}

// ─── R1 fallback: read-trait impl over the concrete struct ──────────
//
// Implementations forward to the existing accessor methods. The
// erased `*_any` methods round-trip an `Arc<dyn Any + Send + Sync>`
// for the typed free helpers in `remotemedia_traits::runtime_context`.
//
// Auto-coercion handles call sites that already hold a
// `&NodeRuntimeContext` and want to pass `&dyn NodeRuntimeContextRead`
// — no edits needed at the call site for the common case.

impl NodeRuntimeContextRead for NodeRuntimeContext {
    fn session_id(&self) -> &str {
        self.session_id.as_ref()
    }

    fn node_id(&self) -> &str {
        self.node_id.as_ref()
    }

    fn capability(&self, addr: &str) -> Option<MediaCapabilities> {
        // Forward to the inherent accessor — same DashMap read path.
        NodeRuntimeContext::capability(self, addr)
    }

    fn insert_capability(&self, addr: &str, caps: MediaCapabilities) {
        self.capabilities.insert(addr.to_string(), caps);
    }

    fn snapshot_any(&self, port: &str) -> Option<Arc<dyn Any + Send + Sync>> {
        // Producer's `Arc<TimestampedSnapshot<T>>` erased to
        // `Arc<dyn Any + Send + Sync>`. The free helper in traits
        // downcasts back to the consumer's `T`.
        let handle = self.input_snapshots.get(port)?;
        handle.snapshot_any()
    }

    fn state_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        // `AnySessionState::into_any_arc` upcasts the trait-object Arc
        // to `Arc<dyn Any + Send + Sync>` via the blanket impl's
        // monomorphization for the concrete type. The session_state
        // field is always populated (host uses `Arc::new(())` for
        // stateless nodes), so we always return `Some`.
        Some(AnySessionState::into_any_arc(Arc::clone(
            &self.session_state,
        )))
    }

    fn try_state_any(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        Some(AnySessionState::into_any_arc(Arc::clone(
            &self.session_state,
        )))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Round-trip an `Arc<dyn AnySessionState>` into a typed `Arc<S>` when
/// the runtime type matches.
///
/// Returns `None` on type mismatch. Callers in this module convert the
/// `None` into a panic with full diagnostic context; if you reach for
/// this helper from outside, decide for yourself how to handle the
/// mismatch.
fn downcast_session_state<S>(any: Arc<dyn AnySessionState>) -> Option<Arc<S>>
where
    S: AnySessionState,
{
    // Trait upcasting (`&dyn AnySessionState` → `&dyn Any`) is stable
    // in Rust 1.86+. `Any::is::<S>` then checks the concrete type id.
    let any_ref: &dyn Any = &*any;
    if any_ref.is::<S>() {
        // SAFETY: the type-id check above confirms the underlying
        // allocation is `S`. `Arc::into_raw` strips the Arc wrapper
        // without changing the refcount; casting the resulting fat
        // pointer (`*const dyn AnySessionState`) to `*const S` keeps
        // the data pointer (the only part `Arc::from_raw` reads). The
        // refcount lives on the same allocation, so the resulting
        // `Arc<S>` shares it with any other live clones — same trick
        // `Arc::downcast` uses internally.
        let raw = Arc::into_raw(any) as *const S;
        Some(unsafe { Arc::from_raw(raw) })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `NodeRuntimeContext` itself is `Send + Sync + Clone`. Compile-only
    /// check; the trait bounds make this trivial but worth pinning so a
    /// future field addition doesn't silently regress thread-safety.
    #[test]
    fn ctx_is_send_sync_clone() {
        fn assert_send_sync_clone<T: Send + Sync + Clone>() {}
        assert_send_sync_clone::<NodeRuntimeContext>();
    }

    /// Same-type downcast returns the same underlying allocation. We
    /// verify both `Arc::ptr_eq` and a strong-count delta to confirm
    /// the round-trip didn't allocate.
    #[test]
    fn downcast_returns_same_allocation() {
        struct MyState {
            value: u32,
        }

        let original: Arc<MyState> = Arc::new(MyState { value: 42 });
        let original_count_before = Arc::strong_count(&original);

        // Erase to the trait object.
        let erased: Arc<dyn AnySessionState> = original.clone();
        assert_eq!(Arc::strong_count(&original), original_count_before + 1);

        let recovered: Arc<MyState> = downcast_session_state(erased).expect("type matches");
        assert_eq!(recovered.value, 42);
        // Both arcs point at the same allocation as `original`.
        assert!(Arc::ptr_eq(&original, &recovered));
        // strong_count: original + recovered = 2.
        assert_eq!(Arc::strong_count(&original), 2);
    }

    /// Wrong-type downcast returns `None` — the helper itself doesn't
    /// panic; the panic is an opt-in policy applied by `state::<S>()`.
    #[test]
    fn downcast_wrong_type_returns_none() {
        struct A;
        struct B;
        let erased: Arc<dyn AnySessionState> = Arc::new(A);
        assert!(downcast_session_state::<B>(erased).is_none());
    }

    /// `state::<S>()` panics with a message that names the requested
    /// type, the node id, and the session id when the type doesn't
    /// match.
    #[test]
    #[should_panic(expected = "NodeRuntimeContext::state::")]
    fn state_panics_on_wrong_type_with_descriptive_message() {
        struct A;
        struct B;
        let ctx = NodeRuntimeContext {
            session_id: Arc::from("sess-1"),
            node_id: Arc::from("node-x"),
            control: SessionControl::new("sess-1"),
            capabilities: Arc::new(DashMap::new()),
            input_snapshots: Arc::new(HashMap::new()),
            session_state: Arc::new(A),
            cancel_gate: Arc::new(CancelGate::new()),
        };
        // Should panic; `should_panic` only matches the prefix to keep
        // the test resilient to future message tweaks.
        let _ = ctx.state::<B>();
    }

    /// `capability(addr)` returns `None` before publish, then the
    /// latest value, then a newer value after a second publish.
    #[test]
    fn capability_cache_round_trip() {
        use crate::capabilities::{
            AudioConstraints, AudioSampleFormat, ConstraintValue, MediaCapabilities,
            MediaConstraints,
        };

        let ctx = NodeRuntimeContext {
            session_id: Arc::from("sess-1"),
            node_id: Arc::from("node-x"),
            control: SessionControl::new("sess-1"),
            capabilities: Arc::new(DashMap::new()),
            input_snapshots: Arc::new(HashMap::new()),
            session_state: Arc::new(()),
            cancel_gate: Arc::new(CancelGate::new()),
        };

        assert!(ctx.capability("audio:default").is_none());

        let caps_a = MediaCapabilities::with_input(MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(48_000)),
            channels: Some(ConstraintValue::Exact(1)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        }));
        ctx.capabilities
            .insert("audio:default".to_string(), caps_a.clone());
        assert!(ctx.capability("audio:default").is_some());

        // Replace with a new value; readers see the new value
        // immediately (DashMap is internally synchronized).
        let caps_b = MediaCapabilities::with_input(MediaConstraints::Audio(AudioConstraints {
            sample_rate: Some(ConstraintValue::Exact(16_000)),
            channels: Some(ConstraintValue::Exact(1)),
            format: Some(ConstraintValue::Exact(AudioSampleFormat::F32)),
        }));
        ctx.capabilities.insert("audio:default".to_string(), caps_b);
        let observed = ctx.capability("audio:default").expect("populated");
        // Sanity: the observed value is the *new* one, not the
        // original. We compare via the Debug repr — MediaCapabilities
        // doesn't impl PartialEq across all its inner enums, so this
        // is the cheapest stable check.
        assert_ne!(format!("{:?}", observed), format!("{:?}", caps_a));
    }

    /// Multiple `state` calls return Arcs that share the underlying
    /// allocation. Stronger than the helper test because it exercises
    /// the full path through the public method.
    #[test]
    fn state_calls_share_allocation() {
        struct MyState {
            counter: std::sync::atomic::AtomicU32,
        }

        let state = Arc::new(MyState {
            counter: std::sync::atomic::AtomicU32::new(0),
        });
        let ctx = NodeRuntimeContext {
            session_id: Arc::from("sess-1"),
            node_id: Arc::from("node-x"),
            control: SessionControl::new("sess-1"),
            capabilities: Arc::new(DashMap::new()),
            input_snapshots: Arc::new(HashMap::new()),
            session_state: state.clone(),
            cancel_gate: Arc::new(CancelGate::new()),
        };

        let a: Arc<MyState> = ctx.state();
        let b: Arc<MyState> = ctx.state();
        assert!(Arc::ptr_eq(&a, &b));
        // Mutating through `a` is visible through `b` — same allocation.
        a.counter.store(7, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(b.counter.load(std::sync::atomic::Ordering::SeqCst), 7);
    }

    /// `remotemedia_traits::runtime_context::snapshot::<T>(ctx, port)` round-trips: producer publishes via
    /// `OutputPort`, the read handle is registered in the ctx's
    /// `input_snapshots`, the consumer downcasts and reads the latest
    /// value. This is the exact path the session router will run when
    /// wiring snapshot connections at session bind.
    #[test]
    fn snapshot_helper_returns_typed_latest_value() {
        use crate::nodes::ports::{OutputPort, SnapshotPort};

        // Producer side: publish two values; the latest must win.
        let out: OutputPort<u32> = OutputPort::empty();
        out.publish(1, 1000);
        out.publish(2, 2000);

        // Wiring: register the input read-handle keyed by the consumer's
        // port name. Mimics what the session router will do.
        let mut snapshots: HashMap<String, Arc<dyn SnapshotPort>> = HashMap::new();
        snapshots.insert("weights".to_string(), Arc::new(out.input()));

        let ctx = NodeRuntimeContext {
            session_id: Arc::from("sess-1"),
            node_id: Arc::from("node-x"),
            control: SessionControl::new("sess-1"),
            capabilities: Arc::new(DashMap::new()),
            input_snapshots: Arc::new(snapshots),
            session_state: Arc::new(()),
            cancel_gate: Arc::new(CancelGate::new()),
        };

        let snap = ctx.snapshot::<u32>("weights").expect("populated");
        assert_eq!(snap.value, 2);
        assert_eq!(snap.pts_us, 2000);
    }

    /// Asking for the wrong type returns `None` rather than panicking —
    /// the consumer can choose how to handle a port-name hit with a
    /// type mismatch (typically: log, fall back to idle path).
    #[test]
    fn snapshot_helper_wrong_type_returns_none() {
        use crate::nodes::ports::{OutputPort, SnapshotPort};

        let out: OutputPort<u32> = OutputPort::empty();
        out.publish(42, 0);

        let mut snapshots: HashMap<String, Arc<dyn SnapshotPort>> = HashMap::new();
        snapshots.insert("port".to_string(), Arc::new(out.input()));

        let ctx = NodeRuntimeContext {
            session_id: Arc::from("sess-1"),
            node_id: Arc::from("node-x"),
            control: SessionControl::new("sess-1"),
            capabilities: Arc::new(DashMap::new()),
            input_snapshots: Arc::new(snapshots),
            session_state: Arc::new(()),
            cancel_gate: Arc::new(CancelGate::new()),
        };

        assert!(ctx.snapshot::<String>("port").is_none());
    }

    /// Asking for a port that isn't registered returns `None`.
    #[test]
    fn snapshot_helper_unknown_port_returns_none() {
        let ctx = NodeRuntimeContext::for_test("sess-1", "node-x");
        assert!(ctx.snapshot::<u32>("never-bound").is_none());
    }

    // ─── R1 fallback: read-trait + free-helper round-trips ─────────────
    //
    // The host's concrete `NodeRuntimeContext` implements
    // `remotemedia_traits::runtime_context::NodeRuntimeContextRead`.
    // These tests verify that plugin-side code holding only a
    // `&dyn NodeRuntimeContextRead` can recover the same values via
    // the typed free helpers in the traits crate.

    use remotemedia_traits::runtime_context::{
        snapshot as trait_snapshot, state as trait_state, try_state as trait_try_state,
        NodeRuntimeContextRead,
    };

    /// `runtime_context::state::<S>(&dyn NodeRuntimeContextRead)`
    /// recovers the same `Arc<S>` allocation the host's `state::<S>`
    /// returns.
    #[test]
    fn read_trait_state_round_trip() {
        struct MyState {
            value: u32,
        }
        let original = Arc::new(MyState { value: 99 });
        let mut ctx = NodeRuntimeContext::for_test("sess-1", "node-x");
        ctx.session_state = original.clone();

        // Erase to the trait-object form a plugin would receive.
        let dyn_ctx: &dyn NodeRuntimeContextRead = &ctx;
        let recovered: Arc<MyState> = trait_state(dyn_ctx);
        assert_eq!(recovered.value, 99);
        assert!(Arc::ptr_eq(&original, &recovered));
    }

    /// `runtime_context::try_state::<S>` returns `None` on type
    /// mismatch when accessed through the trait object.
    #[test]
    fn read_trait_try_state_wrong_type_returns_none() {
        struct A;
        struct B;
        let mut ctx = NodeRuntimeContext::for_test("sess-1", "node-x");
        ctx.session_state = Arc::new(A);
        let dyn_ctx: &dyn NodeRuntimeContextRead = &ctx;
        assert!(trait_try_state::<B>(dyn_ctx).is_none());
    }

    /// `runtime_context::snapshot::<T>` reads the latest value through
    /// the trait + the snapshot port's `snapshot_any` impl.
    #[test]
    fn read_trait_snapshot_round_trip() {
        use crate::nodes::ports::{OutputPort, SnapshotPort};
        let out: OutputPort<u32> = OutputPort::empty();
        out.publish(11, 1111);
        out.publish(22, 2222);

        let mut snapshots: HashMap<String, Arc<dyn SnapshotPort>> = HashMap::new();
        snapshots.insert("weights".into(), Arc::new(out.input()));
        let ctx = NodeRuntimeContext {
            session_id: Arc::from("sess-1"),
            node_id: Arc::from("node-x"),
            control: SessionControl::new("sess-1"),
            capabilities: Arc::new(DashMap::new()),
            input_snapshots: Arc::new(snapshots),
            session_state: Arc::new(()),
            cancel_gate: Arc::new(CancelGate::new()),
        };

        let dyn_ctx: &dyn NodeRuntimeContextRead = &ctx;
        let snap = trait_snapshot::<u32>(dyn_ctx, "weights").expect("populated");
        assert_eq!(snap.value, 22);
        assert_eq!(snap.pts_us, 2222);
    }

    /// `session_id` / `node_id` accessors pass through unchanged.
    #[test]
    fn read_trait_id_accessors() {
        let ctx = NodeRuntimeContext::for_test("sess-abc", "node-zzz");
        let dyn_ctx: &dyn NodeRuntimeContextRead = &ctx;
        assert_eq!(dyn_ctx.session_id(), "sess-abc");
        assert_eq!(dyn_ctx.node_id(), "node-zzz");
    }
}
