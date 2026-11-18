//! A session's iceoryx2 services must not outlive it.
//!
//! Real executors and a real Python worker (`SharedEchoNode`, not shared), so
//! these run only with `REMOTEMEDIA_RUN_IPC_INTEGRATION_TESTS=1` and
//! `REMOTEMEDIA_TEST_PYTHON_CLIENT=<remotemedia-sdk>/clients/python`
//! (`PYTHON_EXECUTABLE` selects the interpreter), like the shared-worker
//! tests. `REMOTEMEDIA_IPC_RELEASE_SESSIONS` sets how many sessions the
//! repeated-session test runs (default 4); `REMOTEMEDIA_IPC_RELEASE_HANDLES=1`
//! also prints the process's handle count after each one.

use super::*;
use crate::multiprocess::ipc_channel::wake_event_name;
use iceoryx2::prelude::{ipc, MessagingPattern, ServiceName};
use iceoryx2::service::Service;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const NODE: &str = "echo";

fn enabled() -> Option<PathBuf> {
    if std::env::var("REMOTEMEDIA_RUN_IPC_INTEGRATION_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping: set REMOTEMEDIA_RUN_IPC_INTEGRATION_TESTS=1 and REMOTEMEDIA_TEST_PYTHON_CLIENT");
        return None;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    Some(PathBuf::from(
        std::env::var("REMOTEMEDIA_TEST_PYTHON_CLIENT").expect("REMOTEMEDIA_TEST_PYTHON_CLIENT"),
    ))
}

fn config(client: &std::path::Path) -> MultiprocessConfig {
    let mut config = MultiprocessConfig::default();
    config.init_timeout_secs = 60;
    if let Ok(python) = std::env::var("PYTHON_EXECUTABLE") {
        config.python_executable = python.into();
    }
    config.python_path = vec![client.to_path_buf(), client.join("tests").join("support")];
    config
}

fn context(session: &str, node_type: &str) -> ExecutorNodeContext {
    let load_log = std::env::temp_dir().join(format!("ipc-release-{session}.log"));
    ExecutorNodeContext {
        node_id: NODE.into(),
        node_type: node_type.into(),
        params: serde_json::json!({"model": "m", "load_log": load_log}),
        session_id: Some(session.into()),
        metadata: HashMap::new(),
    }
}

fn session_id(label: &str) -> String {
    format!(
        "{label}{}",
        &uuid::Uuid::new_v4().simple().to_string()[..12]
    )
}

/// The services a session opens for one node, with their pattern.
fn session_services(session: &str) -> Vec<(String, MessagingPattern)> {
    let input = format!("{session}_{NODE}_input");
    let output = format!("{session}_{NODE}_output");
    vec![
        (wake_event_name(&input), MessagingPattern::Event),
        (wake_event_name(&output), MessagingPattern::Event),
        (input, MessagingPattern::PublishSubscribe),
        (output, MessagingPattern::PublishSubscribe),
        (
            format!("control/{session}_{NODE}"),
            MessagingPattern::PublishSubscribe,
        ),
    ]
}

fn existing(session: &str) -> Vec<String> {
    session_services(session)
        .into_iter()
        .filter(|(name, pattern)| {
            let name = ServiceName::new(name).unwrap();
            ipc::Service::does_exist(&name, iceoryx2::config::Config::global_config(), *pattern)
                .unwrap_or(true)
        })
        .map(|(name, _)| name)
        .collect()
}

/// Wait until none of the session's services exists; the ones left after
/// `within` otherwise.
async fn left_after(session: &str, within: Duration) -> Vec<String> {
    let deadline = Instant::now() + within;
    loop {
        let left = existing(session);
        if left.is_empty() || Instant::now() >= deadline {
            return left;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn registered(session: &str) -> usize {
    let registry = ChannelRegistry::global();
    let mut count = 0;
    for (name, _) in session_services(session) {
        count += usize::from(registry.holds(&name).await);
    }
    count + usize::from(global_sessions().read().await.contains_key(session))
}

/// With sharing on, the worker outlives the session: the lane's services
/// must go when the lane detaches, not when the worker exits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detached_shared_lane_releases_its_services() {
    use crate::python::multiprocess::shared_workers::WorkerSharingConfig;
    let Some(client) = enabled() else { return };
    let mut config = config(&client);
    config.worker_sharing = WorkerSharingConfig {
        enabled: true,
        idle_ttl_secs: 30,
        ..Default::default()
    };
    let model = session_id("m");
    let mut sessions = Vec::new();
    // The first session keeps the worker alive while the second one ends.
    for label in ["keep", "lane"] {
        let session = session_id(label);
        let mut ctx = context(&session, "SharedEchoNode");
        ctx.params["model"] = model.clone().into();
        let mut executor = MultiprocessExecutor::new(config.clone());
        executor.register_module("shared_echo_node").await;
        ExecutorNodeExecutor::initialize(&mut executor, &ctx)
            .await
            .expect("session");
        assert!(executor.sessions.read().await[&session]
            .shared_lanes
            .contains_key(NODE));
        echo(&executor, &session).await;
        sessions.push((session, executor));
    }
    let (session, executor) = sessions.pop().unwrap();
    drop(executor);
    let left = left_after(&session, Duration::from_secs(15)).await;
    assert!(
        left.is_empty(),
        "a detached lane left services behind: {left:?}"
    );
    assert_eq!(registered(&session).await, 0);
    let (keep, mut executor) = sessions.pop().unwrap();
    assert_eq!(
        existing(&keep).len(),
        5,
        "the other session keeps its services"
    );
    echo(&executor, &keep).await;
    ExecutorNodeExecutor::cleanup(&mut executor).await.unwrap();
    let left = left_after(&keep, Duration::from_secs(15)).await;
    assert!(
        left.is_empty(),
        "cleanup of a shared lane left services behind: {left:?}"
    );
}

async fn echo(executor: &MultiprocessExecutor, session: &str) {
    let replies = Arc::new(std::sync::Mutex::new(Vec::new()));
    let sink = replies.clone();
    executor
        .process_runtime_data_streaming(
            crate::data::RuntimeData::Text("x".into()),
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
    assert_eq!(replies.lock().unwrap().len(), 1);
}

async fn start(client: &std::path::Path, session: &str) -> MultiprocessExecutor {
    let mut executor = MultiprocessExecutor::new(config(client));
    executor.register_module("shared_echo_node").await;
    ExecutorNodeExecutor::initialize(&mut executor, &context(session, "SharedEchoNode"))
        .await
        .expect("session");
    assert_eq!(existing(session).len(), 5, "a node opens five services");
    echo(&executor, session).await;
    executor
}

fn services_on_disk() -> Option<usize> {
    let root = iceoryx2::config::Config::global_config()
        .global
        .root_path()
        .to_string();
    std::fs::read_dir(std::path::Path::new(&root).join("services"))
        .ok()
        .map(|entries| entries.flatten().count())
}

/// This process's open handle count (Windows, and only when
/// `REMOTEMEDIA_IPC_RELEASE_HANDLES=1`: it asks PowerShell).
fn open_handles() -> Option<u32> {
    if !cfg!(windows) || std::env::var("REMOTEMEDIA_IPC_RELEASE_HANDLES").as_deref() != Ok("1") {
        return None;
    }
    let script = format!("(Get-Process -Id {}).HandleCount", std::process::id());
    let output = std::process::Command::new("powershell")
        .args(["-NoProfile", "-Command", &script])
        .output()
        .ok()?;
    String::from_utf8_lossy(&output.stdout).trim().parse().ok()
}

/// A session closes by dropping its nodes; nothing calls `cleanup()`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_sessions_release_their_services() {
    let Some(client) = enabled() else { return };
    let sessions: usize = std::env::var("REMOTEMEDIA_IPC_RELEASE_SESSIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(4);
    let before = services_on_disk();
    for index in 0..sessions {
        let session = session_id("drop");
        let executor = start(&client, &session).await;
        drop(executor);
        let left = left_after(&session, Duration::from_secs(15)).await;
        eprintln!(
            "session {}/{sessions}: services on disk {:?} (before the run {:?}), handles {:?}",
            index + 1,
            services_on_disk(),
            before,
            open_handles()
        );
        assert!(
            left.is_empty(),
            "session {index} left services behind: {left:?}"
        );
        assert_eq!(
            registered(&session).await,
            0,
            "host registry still holds session {index}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_cleanup_releases_every_service() {
    let Some(client) = enabled() else { return };
    let session = session_id("clean");
    let mut executor = start(&client, &session).await;
    ExecutorNodeExecutor::cleanup(&mut executor).await.unwrap();
    let left = left_after(&session, Duration::from_secs(15)).await;
    assert!(left.is_empty(), "cleanup left services behind: {left:?}");
    assert_eq!(registered(&session).await, 0);
}

/// A worker killed mid-session runs no cleanup of its own: its iceoryx2 node
/// stays registered as a dead node in every service it had open, with its
/// ports and shared memory. Ending the session must remove all of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_worker_leaves_no_dead_node_behind() {
    use iceoryx2::node::{NodeState, NodeView};
    use iceoryx2::prelude::CallbackProgression;
    let Some(client) = enabled() else { return };
    let session = session_id("kill");
    let executor = start(&client, &session).await;
    let worker = executor.sessions.read().await[&session].node_processes[NODE].clone();
    worker.kill().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while worker.exited_now() != Some(true) {
        assert!(Instant::now() < deadline, "the killed worker did not exit");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    drop(executor);
    let left = left_after(&session, Duration::from_secs(20)).await;
    assert!(
        left.is_empty(),
        "a killed worker's session left services behind: {left:?}"
    );
    assert_eq!(registered(&session).await, 0);
    let mut workers_nodes = Vec::new();
    iceoryx2::node::Node::<ipc::Service>::list(
        iceoryx2::config::Config::global_config(),
        |state| {
            let (kind, details) = match &state {
                NodeState::Alive(view) => ("alive", view.details().clone()),
                NodeState::Dead(view) => ("dead", view.details().clone()),
                _ => return CallbackProgression::Continue,
            };
            if details.is_some_and(|d| d.process_id().value() as u32 == worker.id) {
                workers_nodes.push(kind);
            }
            CallbackProgression::Continue
        },
    )
    .unwrap();
    assert!(
        workers_nodes.is_empty(),
        "the killed worker's node is still registered: {workers_nodes:?}"
    );
}

/// The worker exits before READY: initialization fails after the channels
/// were created.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_initialization_releases_its_services() {
    let Some(client) = enabled() else { return };
    let session = session_id("fail");
    let mut executor = MultiprocessExecutor::new(config(&client));
    let result =
        ExecutorNodeExecutor::initialize(&mut executor, &context(&session, "NoSuchPythonNode"))
            .await;
    assert!(result.is_err(), "an unknown node type must not initialize");
    drop(executor);
    let left = left_after(&session, Duration::from_secs(15)).await;
    assert!(
        left.is_empty(),
        "failed initialization left services behind: {left:?}"
    );
    assert_eq!(registered(&session).await, 0);
}
