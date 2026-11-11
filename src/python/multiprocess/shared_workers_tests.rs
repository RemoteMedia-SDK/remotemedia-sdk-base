//! Shared model workers end to end: real executors, a real Python shared
//! worker and the `SharedEchoNode` test node.
//!
//! Needs a Python with iceoryx2 matching this build and the remotemedia
//! client, so it runs only with `REMOTEMEDIA_RUN_IPC_INTEGRATION_TESTS=1` and
//! `REMOTEMEDIA_TEST_PYTHON_CLIENT=<remotemedia-sdk>/clients/python`
//! (`PYTHON_EXECUTABLE` selects the interpreter).
//!
//! `SharedEchoNode` answers a text input with `{session}:{count}:{model}:{text}`
//! and appends one line to `load_log` per model load.

use crate::python::multiprocess::shared_workers::{pool, WorkerSharingConfig, AT_CAPACITY};
use super::*;
use std::path::PathBuf;
use std::time::Duration;

fn enabled() -> Option<PathBuf> {
    if std::env::var("REMOTEMEDIA_RUN_IPC_INTEGRATION_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping: set REMOTEMEDIA_RUN_IPC_INTEGRATION_TESTS=1 and REMOTEMEDIA_TEST_PYTHON_CLIENT");
        return None;
    }
    Some(PathBuf::from(
        std::env::var("REMOTEMEDIA_TEST_PYTHON_CLIENT").expect("REMOTEMEDIA_TEST_PYTHON_CLIENT"),
    ))
}

struct Fixture {
    config: MultiprocessConfig,
    model: String,
    load_log: PathBuf,
}

impl Fixture {
    fn new(client: PathBuf, sharing: WorkerSharingConfig) -> Self {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
        let mut config = MultiprocessConfig::default();
        config.init_timeout_secs = 60;
        if let Ok(python) = std::env::var("PYTHON_EXECUTABLE") {
            config.python_executable = python.into();
        }
        config.python_path = vec![client.clone(), client.join("tests").join("support")];
        config.worker_sharing = sharing;
        // Each test uses its own model value, so its own sharing key (the
        // pool is process-wide and tests run concurrently).
        let model = format!("m{}", &uuid::Uuid::new_v4().simple().to_string()[..8]);
        let load_log = std::env::temp_dir().join(format!("shared-echo-{model}.log"));
        Self { config, model, load_log }
    }

    async fn session(&self, session_id: &str) -> Result<MultiprocessExecutor> {
        let mut executor = MultiprocessExecutor::new(self.config.clone());
        executor.register_module("shared_echo_node").await;
        let ctx = ExecutorNodeContext {
            node_id: "echo".into(),
            node_type: "SharedEchoNode".into(),
            params: serde_json::json!({"model": self.model, "load_log": self.load_log}),
            session_id: Some(session_id.into()),
            metadata: HashMap::new(),
        };
        ExecutorNodeExecutor::initialize(&mut executor, &ctx).await?;
        Ok(executor)
    }

    fn loads(&self) -> usize {
        std::fs::read_to_string(&self.load_log).map(|s| s.lines().count()).unwrap_or(0)
    }

    async fn workers(&self) -> Vec<crate::python::multiprocess::shared_workers::WorkerSnapshot> {
        let mut pids = Vec::new();
        for worker in pool().snapshot().await {
            if worker.node_type == "SharedEchoNode" && worker.alive {
                pids.push(worker);
            }
        }
        pids
    }
}

fn session_id(label: &str) -> String {
    format!("{label}{}", &uuid::Uuid::new_v4().simple().to_string()[..8])
}

async fn ask(executor: &MultiprocessExecutor, session: &str, text: &str) -> String {
    let replies = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = replies.clone();
    executor
        .process_runtime_data_streaming(
            crate::data::RuntimeData::Text(text.into()),
            Some(session.into()),
            move |output| {
                if let crate::data::RuntimeData::Text(reply) = output {
                    sink.lock().unwrap().push(reply);
                }
                Ok(())
            },
        )
        .await
        .expect("turn");
    let replies = replies.lock().unwrap();
    assert_eq!(replies.len(), 1, "{replies:?}");
    replies[0].clone()
}

async fn lane_worker_pid(executor: &MultiprocessExecutor, session: &str) -> Option<u32> {
    executor
        .sessions
        .read()
        .await
        .get(session)
        .and_then(|s| s.shared_lanes.get("echo"))
        .map(|lease| lease.worker.process.id)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_sessions_share_one_worker_and_model() {
    let Some(client) = enabled() else { return };
    let sharing = WorkerSharingConfig { enabled: true, idle_ttl_secs: 1, ..Default::default() };
    let fixture = Fixture::new(client, sharing);
    let (a_id, b_id) = (session_id("sa"), session_id("sb"));

    // Concurrent first sessions start one worker between them.
    let (a, b) = tokio::join!(fixture.session(&a_id), fixture.session(&b_id));
    let (mut a, mut b) = (a.expect("session a"), b.expect("session b"));
    let pid = lane_worker_pid(&a, &a_id).await.expect("a is shared");
    assert_eq!(lane_worker_pid(&b, &b_id).await, Some(pid));
    assert_eq!(fixture.loads(), 1, "one model load for both sessions");

    let m = &fixture.model;
    assert_eq!(ask(&a, &a_id, "one").await, format!("{a_id}:1:{m}:one"));
    assert_eq!(ask(&b, &b_id, "two").await, format!("{b_id}:1:{m}:two"));
    assert_eq!(ask(&a, &a_id, "three").await, format!("{a_id}:2:{m}:three"));

    // Ending A detaches it; the worker keeps serving B.
    ExecutorNodeExecutor::cleanup(&mut a).await.unwrap();
    let worker = fixture.workers().await.into_iter().find(|w| w.pid == pid).expect("worker alive");
    assert_eq!(worker.attached_sessions, 1);
    assert_eq!(ask(&b, &b_id, "four").await, format!("{b_id}:2:{m}:four"));

    // A new session reuses the loaded worker without loading again.
    let c_id = session_id("sc");
    let mut c = fixture.session(&c_id).await.expect("session c");
    assert_eq!(lane_worker_pid(&c, &c_id).await, Some(pid));
    assert_eq!(fixture.loads(), 1);
    assert_eq!(ask(&c, &c_id, "five").await, format!("{c_id}:1:{m}:five"));

    // With no sessions left, the idle TTL shuts the worker down.
    ExecutorNodeExecutor::cleanup(&mut b).await.unwrap();
    ExecutorNodeExecutor::cleanup(&mut c).await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while fixture.workers().await.iter().any(|w| w.pid == pid) {
        assert!(std::time::Instant::now() < deadline, "idle worker was not shut down");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_workers_refuse_with_at_capacity() {
    let Some(client) = enabled() else { return };
    let sharing = WorkerSharingConfig {
        enabled: true,
        max_sessions_per_worker: 1,
        max_workers_per_key: 2,
        idle_ttl_secs: 1,
        ..Default::default()
    };
    let fixture = Fixture::new(client, sharing);
    let (a_id, b_id) = (session_id("ca"), session_id("cb"));
    let mut a = fixture.session(&a_id).await.expect("session a");
    eprintln!("[capacity] a attached");
    // A full worker causes a second worker for the same key...
    let mut b = fixture.session(&b_id).await.expect("session b");
    eprintln!("[capacity] b attached");
    let (pa, pb) = (lane_worker_pid(&a, &a_id).await.unwrap(), lane_worker_pid(&b, &b_id).await.unwrap());
    assert_ne!(pa, pb);
    // ...up to the per-key limit.
    let error = fixture.session(&session_id("cc")).await.err().expect("third session refused");
    assert!(error.to_string().contains(AT_CAPACITY), "{error}");
    eprintln!("[capacity] c refused");
    // Detaching frees a place on the existing worker.
    ExecutorNodeExecutor::cleanup(&mut a).await.unwrap();
    eprintln!("[capacity] a detached");
    let d_id = session_id("cd");
    let mut d = fixture.session(&d_id).await.expect("session after detach");
    assert_eq!(lane_worker_pid(&d, &d_id).await, Some(pa));
    ExecutorNodeExecutor::cleanup(&mut b).await.unwrap();
    ExecutorNodeExecutor::cleanup(&mut d).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_crash_ends_every_attached_session_and_respawns() {
    let Some(client) = enabled() else { return };
    let sharing = WorkerSharingConfig { enabled: true, idle_ttl_secs: 1, ..Default::default() };
    let fixture = Fixture::new(client, sharing);
    let (a_id, b_id) = (session_id("ka"), session_id("kb"));
    let a = fixture.session(&a_id).await.expect("session a");
    let b = fixture.session(&b_id).await.expect("session b");
    let pid = lane_worker_pid(&a, &a_id).await.expect("shared");

    let process = a.sessions.read().await[&a_id].node_processes["echo"].clone();
    process.kill().await.unwrap();
    eprintln!("[crash] worker {pid} killed");

    // Both sessions are terminated, as for a crashed unshared node.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let a_gone = !a.sessions.read().await.contains_key(&a_id);
        let b_gone = !b.sessions.read().await.contains_key(&b_id);
        if a_gone && b_gone {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "sessions not terminated: a gone {a_gone}, b gone {b_gone}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    eprintln!("[crash] sessions terminated");
    // The next session starts a new worker.
    let c_id = session_id("kc");
    let mut c = fixture.session(&c_id).await.expect("session after crash");
    let new_pid = lane_worker_pid(&c, &c_id).await.expect("shared");
    assert_ne!(new_pid, pid);
    assert_eq!(ask(&c, &c_id, "hi").await, format!("{c_id}:1:{}:hi", fixture.model));
    ExecutorNodeExecutor::cleanup(&mut c).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sharing_off_spawns_a_process_per_session() {
    let Some(client) = enabled() else { return };
    let fixture = Fixture::new(client, WorkerSharingConfig::default());
    let (a_id, b_id) = (session_id("oa"), session_id("ob"));
    let mut a = fixture.session(&a_id).await.expect("session a");
    let mut b = fixture.session(&b_id).await.expect("session b");
    assert_eq!(lane_worker_pid(&a, &a_id).await, None);
    let pa = a.sessions.read().await[&a_id].node_processes["echo"].id;
    let pb = b.sessions.read().await[&b_id].node_processes["echo"].id;
    assert_ne!(pa, pb);
    assert_eq!(fixture.loads(), 2);
    let m = &fixture.model;
    assert_eq!(ask(&a, &a_id, "x").await, format!("{a_id}:1:{m}:x"));
    ExecutorNodeExecutor::cleanup(&mut a).await.unwrap();
    ExecutorNodeExecutor::cleanup(&mut b).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn undeclared_node_is_never_shared() {
    let Some(client) = enabled() else { return };
    let sharing = WorkerSharingConfig { enabled: true, ..Default::default() };
    let fixture = Fixture::new(client, sharing);
    let (a_id, b_id) = (session_id("pa"), session_id("pb"));
    let start = |session: String| {
        let config = fixture.config.clone();
        let params = serde_json::json!({"model": fixture.model, "load_log": fixture.load_log});
        async move {
            let mut executor = MultiprocessExecutor::new(config);
            executor.register_module("shared_echo_node").await;
            let ctx = ExecutorNodeContext {
                node_id: "echo".into(),
                node_type: "PlainEchoNode".into(),
                params,
                session_id: Some(session),
                metadata: HashMap::new(),
            };
            ExecutorNodeExecutor::initialize(&mut executor, &ctx).await.map(|_| executor)
        }
    };
    let mut a = start(a_id.clone()).await.expect("session a");
    let mut b = start(b_id.clone()).await.expect("session b");
    assert_eq!(lane_worker_pid(&a, &a_id).await, None);
    assert_eq!(lane_worker_pid(&b, &b_id).await, None);
    assert_eq!(fixture.loads(), 2);
    ExecutorNodeExecutor::cleanup(&mut a).await.unwrap();
    ExecutorNodeExecutor::cleanup(&mut b).await.unwrap();
}

// ---------------------------------------------------------------------------
// Per-participant Python nodes (openspec: add-multi-participant-sessions 2.2,
// 2.8): the router runs one instance (one IPC lane) per participant; with
// worker sharing the lanes attach to one worker process.
// ---------------------------------------------------------------------------

mod participant_lanes {
    use super::*;
    use crate::manifest::{Connection, Manifest, ManifestMetadata, NodeManifest};
    use crate::nodes::{
        AsyncNodeWrapper, AsyncStreamingNode, InitializeContextRead, InstanceScope, StreamingNode,
        StreamingNodeFactory,
    };
    use crate::transport::data::participant;
    use crate::transport::session_control::aux_port_of;
    use crate::transport::{Participant, PipelineExecutor, TransportData};
    use std::collections::{BTreeMap, HashSet};

    type Lanes = Arc<std::sync::Mutex<Vec<Arc<tokio::sync::Mutex<Option<MultiprocessExecutor>>>>>>;

    /// `ScopedEchoNode` run through a `MultiprocessExecutor` that imports the
    /// test module; reports the scope the worker declared.
    struct Lane {
        node_id: String,
        config: MultiprocessConfig,
        params: Value,
        executor: Arc<tokio::sync::Mutex<Option<MultiprocessExecutor>>>,
        session: OnceLock<String>,
        scope: OnceLock<InstanceScope>,
    }

    #[async_trait]
    impl AsyncStreamingNode for Lane {
        fn node_type(&self) -> &str {
            "ScopedEchoNode"
        }

        async fn initialize(&self, ctx: &dyn InitializeContextRead) -> Result<()> {
            let mut executor = MultiprocessExecutor::new(self.config.clone());
            executor.register_module("shared_echo_node").await;
            let node_ctx = ExecutorNodeContext {
                node_id: self.node_id.clone(),
                node_type: "ScopedEchoNode".into(),
                params: self.params.clone(),
                session_id: Some(ctx.session_id().to_string()),
                metadata: HashMap::new(),
            };
            ExecutorNodeExecutor::initialize(&mut executor, &node_ctx).await?;
            if let Some(scope) = executor.declared_instance_scope() {
                let _ = self.scope.set(scope);
            }
            let _ = self.session.set(ctx.session_id().to_string());
            *self.executor.lock().await = Some(executor);
            Ok(())
        }

        async fn process(&self, data: crate::data::RuntimeData) -> Result<crate::data::RuntimeData> {
            let guard = self.executor.lock().await;
            let executor = guard.as_ref().ok_or_else(|| Error::Execution("lane stopped".into()))?;
            let mut replies = Vec::new();
            executor
                .process_runtime_data_streaming(data, self.session.get().cloned(), |out| {
                    replies.push(out);
                    Ok(())
                })
                .await?;
            replies.pop().ok_or_else(|| Error::Execution("no reply".into()))
        }

        async fn process_control_message(
            &self,
            data: crate::data::RuntimeData,
            _session_id: Option<String>,
        ) -> Result<bool> {
            if aux_port_of(&data).as_deref() == Some("shutdown") {
                if let Some(mut executor) = self.executor.lock().await.take() {
                    let _ = ExecutorNodeExecutor::cleanup(&mut executor).await;
                }
                return Ok(true);
            }
            Ok(false)
        }

        fn instance_scope(&self) -> Option<InstanceScope> {
            self.scope.get().copied()
        }
    }

    struct LaneFactory {
        config: MultiprocessConfig,
        lanes: Lanes,
    }

    impl StreamingNodeFactory for LaneFactory {
        fn create(
            &self,
            node_id: String,
            params: &Value,
            _session_id: Option<String>,
        ) -> Result<Box<dyn StreamingNode>> {
            let executor = Arc::new(tokio::sync::Mutex::new(None));
            self.lanes.lock().unwrap().push(executor.clone());
            Ok(Box::new(AsyncNodeWrapper(Arc::new(Lane {
                node_id,
                config: self.config.clone(),
                params: params.clone(),
                executor,
                session: OnceLock::new(),
                scope: OnceLock::new(),
            }))))
        }
        fn node_type(&self) -> &str {
            "ScopedEchoNode"
        }
    }

    /// Three participants, two rounds each, through a per-participant Python
    /// node (scope declared only by the Python class). Returns each
    /// participant's `(instance, count)` replies and every lane's worker pid.
    async fn three_participants(fixture: &Fixture) -> (BTreeMap<String, Vec<(String, u32)>>, Vec<u32>) {
        let executor = PipelineExecutor::new().unwrap();
        let lanes: Lanes = Arc::default();
        executor
            .register_factory(Arc::new(LaneFactory { config: fixture.config.clone(), lanes: lanes.clone() }))
            .await;
        let manifest = Arc::new(Manifest {
            version: "v1".into(),
            metadata: ManifestMetadata { name: "python-lanes".into(), ..Default::default() },
            nodes: vec![NodeManifest {
                id: "echo".into(),
                node_type: "ScopedEchoNode".into(),
                params: serde_json::json!({"model": fixture.model, "load_log": fixture.load_log}),
                ..Default::default()
            }],
            connections: Vec::<Connection>::new(),
            python_env: None,
            plugins: Vec::new(),
        });
        let mut session = executor.create_session(manifest).await.expect("session");
        let users: Vec<_> = ["ann", "ben", "cat"]
            .iter()
            .map(|id| session.join_participant(Participant::new(*id, participant::role::USER)).unwrap())
            .collect();
        for round in 1..=2 {
            for user in &users {
                user.send(TransportData::new(crate::data::RuntimeData::Text(format!("r{round}"))))
                    .await
                    .unwrap();
            }
        }
        let mut replies: BTreeMap<String, Vec<(String, u32)>> = BTreeMap::new();
        for _ in 0..6 {
            let output = tokio::time::timeout(Duration::from_secs(60), session.recv_output())
                .await
                .expect("reply in time")
                .unwrap()
                .expect("session open");
            let crate::data::RuntimeData::Text(body) = &output.data else { panic!("{:?}", output.data) };
            let mut parts = body.splitn(3, ':');
            let instance = parts.next().unwrap().to_string();
            let count = parts.next().unwrap().parse().unwrap();
            let who = output.metadata.get(participant::ID).cloned().expect("attributed");
            replies.entry(who).or_default().push((instance, count));
        }
        let mut pids = Vec::new();
        let session_id = session.session_id.clone();
        let all: Vec<_> = lanes.lock().unwrap().clone();
        for lane in all {
            let guard = lane.lock().await;
            if let Some(executor) = guard.as_ref() {
                let sessions = executor.sessions.read().await;
                let state = &sessions[&session_id];
                let pid = state
                    .shared_lanes
                    .values()
                    .next()
                    .map(|lease| lease.worker.process.id)
                    .or_else(|| state.node_processes.values().next().map(|p| p.id))
                    .expect("lane process");
                pids.push(pid);
            }
        }
        session.close().await.unwrap();
        let _ = session.wait().await;
        let remaining: Vec<_> = lanes.lock().unwrap().drain(..).collect();
        for lane in remaining {
            if let Some(mut executor) = lane.lock().await.take() {
                let _ = ExecutorNodeExecutor::cleanup(&mut executor).await;
            }
        }
        (replies, pids)
    }

    fn assert_isolated(replies: &BTreeMap<String, Vec<(String, u32)>>) {
        assert_eq!(replies.len(), 3, "{replies:?}");
        let mut instances = HashSet::new();
        for (who, seen) in replies {
            assert_eq!(seen.len(), 2, "{who}: {seen:?}");
            assert_eq!(seen[0].0, seen[1].0, "{who} stayed on one instance");
            assert_eq!((seen[0].1, seen[1].1), (1, 2), "{who} has its own count");
            instances.insert(seen[0].0.clone());
        }
        assert_eq!(instances.len(), 3, "one instance per participant: {replies:?}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn python_per_participant_node_gets_a_lane_per_participant() {
        let Some(client) = enabled() else { return };
        let fixture = Fixture::new(client, WorkerSharingConfig::default());
        let (replies, pids) = three_participants(&fixture).await;
        assert_isolated(&replies);
        let distinct: HashSet<_> = pids.iter().collect();
        assert_eq!((pids.len(), distinct.len()), (3, 3), "a process per lane: {pids:?}");
        assert_eq!(fixture.loads(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_workers_serve_every_participant_lane_from_one_process() {
        let Some(client) = enabled() else { return };
        let sharing = WorkerSharingConfig { enabled: true, idle_ttl_secs: 1, ..Default::default() };
        let fixture = Fixture::new(client, sharing);
        let (replies, pids) = three_participants(&fixture).await;
        assert_isolated(&replies);
        let distinct: HashSet<_> = pids.iter().collect();
        assert_eq!((pids.len(), distinct.len()), (3, 1), "lanes share one worker: {pids:?}");
        assert_eq!(fixture.loads(), 1, "one model load for three participants");
    }
}
