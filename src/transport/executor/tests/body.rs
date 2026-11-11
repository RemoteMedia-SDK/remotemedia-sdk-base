//! Node bodies and the start gate (openspec: add-source-node-lifecycle): the
//! runtime runs a node's `background`, cancels it when the instance stops,
//! reports its failure, and does not start it before the session starts.
use super::*;
use crate::nodes::{
    AsyncNodeWrapper, AsyncStreamingNode, Emitter, InitializeContextRead, InstanceScope,
    NodeRuntimeContextRead, StreamingNode, StreamingNodeFactory,
};
use crate::transport::data::participant;
use crate::transport::WarmSessionPool;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// What the bodies of a test did, in order.
#[derive(Default)]
struct Probe {
    events: Mutex<Vec<String>>,
    /// When each node finished `initialize` / started its body.
    initialized: Mutex<Vec<(String, Instant)>>,
    body_started: Mutex<Vec<Instant>>,
}

impl Probe {
    fn note(&self, event: impl Into<String>) {
        self.events.lock().unwrap().push(event.into());
    }
    fn events(&self) -> Vec<String> {
        self.events.lock().unwrap().clone()
    }
    fn bodies(&self) -> usize {
        self.body_started.lock().unwrap().len()
    }
}

/// Records `body-cancelled` when the body future is dropped mid-run.
struct CancelGuard(Arc<Probe>, bool);

impl Drop for CancelGuard {
    fn drop(&mut self) {
        if !self.1 {
            self.0.note("body-cancelled");
        }
    }
}

struct BodyNode {
    /// `return`: emit `frames` and return. `run`: emit until cancelled.
    /// `error` / `panic`: fail after `frames`. `none`: no body.
    /// `missing`: reports a body it does not have.
    mode: String,
    frames: u64,
    init_ms: u64,
    self_driven: Option<bool>,
    scope: Option<InstanceScope>,
    probe: Arc<Probe>,
}

fn text(value: impl Into<String>) -> RuntimeData {
    RuntimeData::Text(value.into())
}

#[async_trait::async_trait]
impl AsyncStreamingNode for BodyNode {
    fn node_type(&self) -> &str {
        "BodyNode"
    }

    async fn initialize(&self, ctx: &dyn InitializeContextRead) -> crate::Result<()> {
        if self.init_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.init_ms)).await;
        }
        self.probe
            .initialized
            .lock()
            .unwrap()
            .push((ctx.node_id().to_string(), Instant::now()));
        Ok(())
    }

    fn self_driven(&self) -> Option<bool> {
        self.self_driven
    }

    fn instance_scope(&self) -> Option<InstanceScope> {
        self.scope
    }

    fn has_background(&self) -> bool {
        self.mode != "none"
    }

    async fn background(&self, ctx: &dyn NodeRuntimeContextRead) -> crate::Result<()> {
        if self.mode == "missing" {
            return Err(remotemedia_traits::streaming::missing_background(self.node_type()));
        }
        self.probe.body_started.lock().unwrap().push(Instant::now());
        let emitter = Emitter::for_call(ctx);
        let mut guard = CancelGuard(self.probe.clone(), false);
        let mut n = 0u64;
        while self.mode == "run" || n < self.frames {
            if let Err(error) = emitter.emit(text(format!("body-{n}"))).await {
                self.probe.note(format!("emit-failed: {error}"));
                return Err(error);
            }
            n += 1;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        guard.1 = true;
        match self.mode.as_str() {
            "error" => Err(crate::Error::Execution("body sentinel error".into())),
            "panic" => panic!("body sentinel panic"),
            _ => Ok(()),
        }
    }

    async fn process(&self, data: RuntimeData) -> crate::Result<RuntimeData> {
        Ok(data)
    }

    /// Emits a copy of the input, then replies with it.
    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: F,
    ) -> crate::Result<usize>
    where
        F: FnMut(RuntimeData) -> crate::Result<()> + Send,
    {
        if let RuntimeData::Text(body) = &data {
            Emitter::for_call(ctx).emit(text(format!("emitted-{body}"))).await?;
        }
        callback(data)?;
        Ok(1)
    }

    async fn process_control_message(
        &self,
        message: RuntimeData,
        _session_id: Option<String>,
    ) -> crate::Result<bool> {
        if crate::transport::session_control::aux_port_of(&message).as_deref() == Some("shutdown") {
            self.probe.note("shutdown");
        }
        Ok(false)
    }
}

struct BodyFactory {
    node_type: &'static str,
    scope: Option<InstanceScope>,
    probe: Arc<Probe>,
}

impl StreamingNodeFactory for BodyFactory {
    fn create(
        &self,
        _node_id: String,
        params: &serde_json::Value,
        _session_id: Option<String>,
    ) -> crate::Result<Box<dyn StreamingNode>> {
        Ok(Box::new(AsyncNodeWrapper(Arc::new(BodyNode {
            mode: params.get("mode").and_then(|v| v.as_str()).unwrap_or("none").to_string(),
            frames: params.get("frames").and_then(|v| v.as_u64()).unwrap_or(0),
            init_ms: params.get("init_ms").and_then(|v| v.as_u64()).unwrap_or(0),
            self_driven: params.get("self_driven").and_then(|v| v.as_bool()),
            scope: self.scope,
            probe: self.probe.clone(),
        }))))
    }
    fn node_type(&self) -> &str {
        self.node_type
    }
}

async fn executor() -> (Arc<PipelineExecutor>, Arc<Probe>) {
    let executor = Arc::new(PipelineExecutor::new().unwrap());
    let probe = Arc::new(Probe::default());
    for (node_type, scope) in [
        ("BodyNode", None),
        ("BodyPerParticipant", Some(InstanceScope::PerParticipant)),
    ] {
        executor
            .register_factory(Arc::new(BodyFactory {
                node_type,
                scope,
                probe: probe.clone(),
            }))
            .await;
    }
    (executor, probe)
}

fn manifest(nodes: &[(&str, &str, serde_json::Value)]) -> Arc<Manifest> {
    Arc::new(Manifest {
        version: "v1".to_string(),
        metadata: ManifestMetadata {
            name: "body-test".to_string(),
            ..Default::default()
        },
        nodes: nodes
            .iter()
            .map(|(id, node_type, params)| NodeManifest {
                id: id.to_string(),
                node_type: node_type.to_string(),
                params: params.clone(),
                ..Default::default()
            })
            .collect(),
        connections: Vec::new(),
        python_env: None,
        plugins: Vec::new(),
    })
}

fn source(mode: &str, frames: u64) -> Arc<Manifest> {
    manifest(&[(
        "source",
        "BodyNode",
        serde_json::json!({"mode": mode, "frames": frames, "self_driven": true}),
    )])
}

async fn next(session: &mut SessionHandle) -> (String, Option<String>) {
    let output = tokio::time::timeout(Duration::from_secs(5), session.recv_output())
        .await
        .expect("output in time")
        .unwrap()
        .expect("session open");
    let RuntimeData::Text(body) = &output.data else {
        panic!("expected text, got {:?}", output.data)
    };
    (body.clone(), output.metadata.get(participant::ID).cloned())
}

async fn failure(session: SessionHandle) -> String {
    tokio::time::timeout(Duration::from_secs(5), session.wait())
        .await
        .expect("the session ended")
        .expect_err("the session failed")
        .to_string()
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    for _ in 0..250 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_emits_with_no_input_and_its_return_ends_a_source_only_session() {
    let (executor, _probe) = executor().await;
    let mut session = executor.create_session(source("return", 3)).await.unwrap();
    for n in 0..3 {
        assert_eq!(next(&mut session).await, (format!("body-{n}"), None));
    }
    let end = tokio::time::timeout(Duration::from_secs(5), session.recv_output()).await;
    assert!(matches!(end, Ok(Ok(None))), "the session did not end with its source: {end:?}");
    assert!(session.wait().await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_with_a_body_that_is_not_self_driven_takes_input_and_keeps_its_emitter() {
    let (executor, probe) = executor().await;
    let mut session = executor
        .create_session(manifest(&[("node", "BodyNode", serde_json::json!({"mode": "return", "frames": 1}))]))
        .await
        .unwrap();
    assert_eq!(next(&mut session).await.0, "body-0");
    // The body has returned. The node still takes client input, though it
    // has no upstream, and still emits.
    until("the body to return", || probe.bodies() == 1).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    session.send_input(TransportData::new(text("hello"))).await.unwrap();
    assert_eq!(next(&mut session).await.0, "emitted-hello");
    assert_eq!(next(&mut session).await.0, "hello");
    session.close().await.unwrap();
    assert!(session.wait().await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_that_returns_an_error_fails_the_node() {
    let (executor, _probe) = executor().await;
    let session = executor.create_session(source("error", 1)).await.unwrap();
    let error = failure(session).await;
    assert!(error.contains("source") && error.contains("body sentinel error"), "{error}");
    assert!(error.contains("background"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_that_panics_fails_the_node_and_the_session_does_not_wait_for_it() {
    let (executor, _probe) = executor().await;
    let session = executor.create_session(source("panic", 0)).await.unwrap();
    let error = failure(session).await;
    assert!(error.contains("source") && error.contains("body sentinel panic"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_that_reports_a_body_it_does_not_have_fails_and_is_named() {
    let (executor, _probe) = executor().await;
    let session = executor.create_session(source("missing", 0)).await.unwrap();
    let error = failure(session).await;
    assert!(error.contains("BodyNode") && error.contains("does not override background"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_body_is_cancelled_before_shutdown_and_sees_no_failed_emit() {
    let (executor, probe) = executor().await;
    let mut session = executor.create_session(source("run", 0)).await.unwrap();
    assert_eq!(next(&mut session).await.0, "body-0");
    session.close().await.unwrap();
    // Outputs still queued are of no interest; the router's result is.
    let result = tokio::time::timeout(Duration::from_secs(5), session.wait()).await.unwrap();
    assert!(result.is_ok(), "{result:?}");
    assert_eq!(probe.events(), ["body-cancelled", "shutdown"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_per_participant_instance_runs_its_own_body_from_join_to_leave() {
    let (executor, probe) = executor().await;
    let mut session = executor
        .create_session(manifest(&[(
            "source",
            "BodyPerParticipant",
            serde_json::json!({"mode": "run", "self_driven": true}),
        )]))
        .await
        .unwrap();
    let alice = session
        .join_participant(Participant::new("alice", participant::role::USER))
        .unwrap();
    alice.join().await.unwrap();
    // Her instance's body emits as her.
    assert_eq!(next(&mut session).await, ("body-0".to_string(), Some("alice".to_string())));
    assert_eq!(probe.bodies(), 1);
    alice.leave().await.unwrap();
    until("alice's body to be cancelled", || probe.events().contains(&"body-cancelled".to_string())).await;
    session.close().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), session.wait()).await.unwrap();
    assert!(result.is_ok(), "{result:?}");
    assert!(!probe.events().iter().any(|event| event.starts_with("emit-failed")), "{:?}", probe.events());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_body_does_not_start_while_a_sibling_node_initializes() {
    let (executor, probe) = executor().await;
    let mut session = executor
        .create_session(manifest(&[
            ("source", "BodyNode", serde_json::json!({"mode": "return", "frames": 1, "self_driven": true})),
            ("slow", "BodyNode", serde_json::json!({"init_ms": 400})),
        ]))
        .await
        .unwrap();
    assert_eq!(next(&mut session).await.0, "body-0");
    let slow_ready = probe
        .initialized
        .lock()
        .unwrap()
        .iter()
        .find(|(id, _)| id == "slow")
        .map(|(_, at)| *at)
        .expect("slow initialized");
    let body_started = probe.body_started.lock().unwrap()[0];
    assert!(body_started >= slow_ready, "the body started before its sibling was ready");
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_source_in_a_parked_warm_session_produces_nothing_until_acquired() {
    let (executor, probe) = executor().await;
    let pool = WarmSessionPool::new(executor.clone());
    let manifest = source("return", 2);
    pool.prewarm(manifest.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(probe.bodies(), 0, "the body ran in a parked session");

    let mut session = pool.acquire(manifest).await.unwrap();
    assert!(session.is_started());
    assert_eq!(next(&mut session).await.0, "body-0");
    assert_eq!(next(&mut session).await.0, "body-1");
    assert_eq!(probe.bodies(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_task_a_node_spawned_itself_waits_for_the_start_of_a_parked_session() {
    // `Emitter::started()` and emits resolve at the same gate.
    let executor = Arc::new(PipelineExecutor::new().unwrap());
    let pool = WarmSessionPool::new(executor.clone());
    let manifest = Arc::new(Manifest {
        version: "v1".to_string(),
        metadata: ManifestMetadata { name: "parked-interval".to_string(), ..Default::default() },
        nodes: vec![NodeManifest {
            id: "clock".into(),
            node_type: "IntervalSource".into(),
            params: serde_json::json!({"interval_ms": 10, "count": 2}),
            ..Default::default()
        }],
        connections: Vec::new(),
        python_env: None,
        plugins: Vec::new(),
    });
    pool.prewarm(manifest.clone()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut session = pool.acquire(manifest).await.unwrap();
    let acquired = Instant::now();
    let output = tokio::time::timeout(Duration::from_secs(5), session.recv_output())
        .await
        .unwrap()
        .unwrap()
        .expect("a tick");
    let RuntimeData::Json(tick) = &output.data else { panic!("expected json") };
    assert_eq!(tick["tick"], 0);
    // Counted from the start, not from the build 200 ms earlier.
    assert!(tick["elapsed_ms"].as_u64().unwrap() < 150, "{tick}");
    assert!(acquired.elapsed() < Duration::from_secs(2));
}

// ── Rust source nodes (trait and attribute) ─────────────────────────────

struct Counter {
    count: u64,
}

#[async_trait::async_trait]
impl remotemedia_traits::SourceNode for Counter {
    fn node_type(&self) -> &str {
        "Counter"
    }
    async fn background(&self, ctx: &dyn NodeRuntimeContextRead) -> crate::Result<()> {
        let emitter = Emitter::for_call(ctx);
        for n in 0..self.count {
            emitter.emit(text(format!("count-{n}"))).await?;
        }
        Ok(())
    }
}

/// The same source, declared with the attribute. In its own module: the
/// macro's output names `Result` with two parameters.
mod attribute {
    use crate::data::RuntimeData;
    use crate::nodes::{Emitter, NodeRuntimeContextRead};

    #[crate::node(source, node_type = "MacroCounter", output_caps = "text")]
    pub struct MacroCounterNode {
        #[config(default = 3)]
        pub count: u64,
    }

    impl MacroCounterNode {
        async fn background_impl(
            &self,
            ctx: &dyn NodeRuntimeContextRead,
        ) -> Result<(), crate::Error> {
            let emitter = Emitter::for_call(ctx);
            for n in 0..self.config.count {
                emitter.emit(RuntimeData::Text(format!("count-{n}"))).await?;
            }
            Ok(())
        }
    }
}
use attribute::MacroCounterNode;

/// Declares itself self-driven and says nothing about its output.
struct SilentFactory;

impl StreamingNodeFactory for SilentFactory {
    fn create(
        &self,
        _node_id: String,
        _params: &serde_json::Value,
        _session_id: Option<String>,
    ) -> crate::Result<Box<dyn StreamingNode>> {
        Ok(remotemedia_traits::SourceNodeWrapper::boxed(Counter { count: 1 }))
    }
    fn node_type(&self) -> &str {
        "Silent"
    }
    fn self_driven(&self) -> Option<bool> {
        Some(true)
    }
}

async fn source_executor() -> Arc<PipelineExecutor> {
    let (executor, _probe) = executor().await;
    executor
        .register_factory(Arc::new(remotemedia_traits::source_factory(
            "Counter",
            crate::capabilities::MediaConstraints::Text(Default::default()),
            |params: &serde_json::Value| {
                Ok(Counter {
                    count: params.get("count").and_then(|v| v.as_u64()).unwrap_or(3),
                })
            },
        )))
        .await;
    executor.register_factory(MacroCounterNode::source_factory()).await;
    executor.register_factory(Arc::new(SilentFactory)).await;
    executor
}

async fn runs_as_a_source(node_type: &str) {
    let executor = source_executor().await;
    let mut session = executor
        .create_session(manifest(&[("source", node_type, serde_json::json!({"count": 3}))]))
        .await
        .unwrap();
    // Nothing is sent. Three frames, then the session ends with its source.
    for n in 0..3 {
        assert_eq!(next(&mut session).await, (format!("count-{n}"), None));
    }
    let end = tokio::time::timeout(Duration::from_secs(5), session.recv_output()).await;
    assert!(matches!(end, Ok(Ok(None))), "{node_type}: the session did not end: {end:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_type_that_implements_source_node_is_a_source() {
    runs_as_a_source("Counter").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_struct_with_the_source_attribute_is_a_source() {
    runs_as_a_source("MacroCounter").await;
    let factory = MacroCounterNode::source_factory();
    assert_eq!(factory.self_driven(), Some(true));
    assert!(factory
        .media_capabilities(&serde_json::Value::Null)
        .is_some_and(|capabilities| capabilities.default_output().is_some()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_self_driven_source_that_declares_no_output_fails_validation_and_is_named() {
    let executor = source_executor().await;
    // The factory says the node is self-driven.
    let error = executor
        .create_session(manifest(&[("mute", "Silent", serde_json::json!({}))]))
        .await
        .err()
        .expect("validation fails")
        .to_string();
    assert!(error.contains("'mute'") && error.contains("output capabilities"), "{error}");
    assert!(error.contains("output_caps") && error.contains("media_capabilities"), "{error}");

    // The manifest says so, of a node whose factory declares nothing.
    let mut by_manifest = (*manifest(&[("quiet", "BodyNode", serde_json::json!({"mode": "return"}))])).clone();
    by_manifest.nodes[0].session_input = Some(false);
    let error = executor
        .create_session(Arc::new(by_manifest.clone()))
        .await
        .err()
        .expect("validation fails")
        .to_string();
    assert!(error.contains("'quiet'"), "{error}");

    // Declared on the manifest node: accepted.
    by_manifest.nodes[0].media_capabilities = Some(crate::capabilities::MediaCapabilities::with_output(
        crate::capabilities::MediaConstraints::Text(Default::default()),
    ));
    let mut session = executor.create_session(Arc::new(by_manifest)).await.unwrap();
    session.close().await.unwrap();

    // With an upstream node it is not a source: nothing to declare.
    let mut fed = (*manifest(&[
        ("in", "BodyNode", serde_json::json!({})),
        ("mute", "Silent", serde_json::json!({})),
    ]))
    .clone();
    fed.connections.push(Connection {
        from: "in".into(),
        to: "mute".into(),
        ..Default::default()
    });
    let mut session = executor.create_session(Arc::new(fed)).await.unwrap();
    session.close().await.unwrap();
}

#[test]
fn the_resolver_rejects_an_undeclared_self_driven_source() {
    let mut registry = crate::nodes::StreamingNodeRegistry::new();
    registry.register(Arc::new(SilentFactory));
    registry.register(MacroCounterNode::source_factory());
    let resolver = crate::capabilities::resolver::CapabilityResolver::new(&registry);
    let error = resolver
        .resolve(&[("mute".into(), "Silent".into())], &[], &Default::default())
        .err()
        .expect("resolution fails")
        .to_string();
    assert!(error.contains("'mute'") && error.contains("Silent"), "{error}");
    assert!(resolver
        .resolve(&[("clock".into(), "MacroCounter".into())], &[], &Default::default())
        .is_ok());
}

// ── Paced sources follow the source rules ───────────────────────────────

/// Ticks `ticks` times, then finishes. Answers input it receives.
struct PacedNode {
    ticks: u64,
    done: std::sync::atomic::AtomicU64,
}

#[async_trait::async_trait]
impl AsyncStreamingNode for PacedNode {
    fn node_type(&self) -> &str {
        "Paced"
    }
    fn pacing_nature(&self) -> crate::nodes::PacingNature {
        crate::nodes::PacingNature::SourceWall(100)
    }
    async fn process(&self, data: RuntimeData) -> crate::Result<RuntimeData> {
        match data {
            RuntimeData::Text(body) => Ok(text(format!("paced-heard:{body}"))),
            other => Ok(other),
        }
    }
    async fn tick_with_ctx(
        &self,
        _tick: crate::nodes::Tick,
        ctx: &dyn NodeRuntimeContextRead,
        mut output: Box<dyn FnMut(RuntimeData) -> crate::Result<()> + Send>,
    ) -> crate::Result<()> {
        let n = self.done.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if n >= self.ticks {
            return Ok(());
        }
        output(text(format!("tick-{n}")))?;
        if n + 1 == self.ticks {
            ctx.finish_source()?;
        }
        Ok(())
    }
}

struct PacedFactory;

impl StreamingNodeFactory for PacedFactory {
    fn create(
        &self,
        _node_id: String,
        params: &serde_json::Value,
        _session_id: Option<String>,
    ) -> crate::Result<Box<dyn StreamingNode>> {
        Ok(Box::new(AsyncNodeWrapper(Arc::new(PacedNode {
            ticks: params.get("ticks").and_then(|v| v.as_u64()).unwrap_or(u64::MAX),
            done: Default::default(),
        }))))
    }
    fn node_type(&self) -> &str {
        "Paced"
    }
}

async fn paced_executor() -> Arc<PipelineExecutor> {
    let executor = source_executor().await;
    executor.register_factory(Arc::new(PacedFactory)).await;
    executor
}

/// Outputs until `quiet` passes with none, sorted.
async fn drain(session: &mut SessionHandle, quiet: Duration) -> Vec<String> {
    let mut seen = Vec::new();
    while let Ok(Ok(Some(output))) = tokio::time::timeout(quiet, session.recv_output()).await {
        if let RuntimeData::Text(body) = &output.data {
            seen.push(body.clone());
        }
    }
    seen.sort();
    seen
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_input_is_not_delivered_to_a_paced_source() {
    let executor = paced_executor().await;
    let mut session = executor
        .create_session(manifest(&[
            ("paced", "Paced", serde_json::json!({"ticks": 2})),
            ("echo", "BodyNode", serde_json::json!({})),
        ]))
        .await
        .unwrap();
    session.send_input(TransportData::new(text("hello"))).await.unwrap();
    // The ordinary source answers once; the paced source only ticks.
    let seen = drain(&mut session, Duration::from_millis(400)).await;
    assert_eq!(seen, ["emitted-hello", "hello", "tick-0", "tick-1"]);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_manifest_can_keep_a_paced_source_taking_client_input() {
    let executor = paced_executor().await;
    let mut manifest = (*manifest(&[("paced", "Paced", serde_json::json!({"ticks": 1}))])).clone();
    manifest.nodes[0].session_input = Some(true);
    let mut session = executor.create_session(Arc::new(manifest)).await.unwrap();
    session.send_input(TransportData::new(text("hello"))).await.unwrap();
    let seen = drain(&mut session, Duration::from_millis(400)).await;
    assert_eq!(seen, ["paced-heard:hello", "tick-0"]);
    session.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_of_paced_and_emitter_based_sources_ends_when_both_have_finished() {
    let executor = paced_executor().await;
    let mut session = executor
        .create_session(manifest(&[
            ("paced", "Paced", serde_json::json!({"ticks": 2})),
            ("counter", "Counter", serde_json::json!({"count": 2})),
        ]))
        .await
        .unwrap();
    // No node takes client input, and nothing is sent.
    let mut seen = Vec::new();
    let ended = loop {
        match tokio::time::timeout(Duration::from_secs(5), session.recv_output()).await {
            Ok(Ok(Some(output))) => {
                if let RuntimeData::Text(body) = &output.data {
                    seen.push(body.clone());
                }
            }
            Ok(Ok(None)) => break true,
            other => panic!("the session did not end by itself: {other:?}"),
        }
    };
    assert!(ended);
    seen.sort();
    assert_eq!(seen, ["count-0", "count-1", "tick-0", "tick-1"]);
    assert!(session.wait().await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paced_source_that_has_not_finished_keeps_its_session_open() {
    let executor = paced_executor().await;
    let mut session = executor
        .create_session(manifest(&[
            ("paced", "Paced", serde_json::json!({})),
            ("counter", "Counter", serde_json::json!({"count": 1})),
        ]))
        .await
        .unwrap();
    // The emitter-based source finished long ago; the paced one still ticks.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut ticks = 0;
    while let Ok(Ok(Some(_))) =
        tokio::time::timeout(Duration::from_millis(100), session.recv_output()).await
    {
        ticks += 1;
        if ticks > 5 {
            break;
        }
    }
    assert!(ticks > 5, "the session ended while a paced source was running");
    session.close().await.unwrap();
}
