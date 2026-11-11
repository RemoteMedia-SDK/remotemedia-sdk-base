//! Shared model workers (openspec: add-shared-model-workers).
//!
//! A shared worker is one Python process (`runner --shared-worker-id W`) that
//! hosts one node type for several sessions. Each session keeps its ordinary
//! per-(session, node) lane: its own channels, wake events, IPC thread, READY
//! and copy reports. The worker runs one node instance per attached session;
//! instances share the interpreter, the CUDA context and any model loaded
//! through `MultiprocessNode.shared_resource`.
//!
//! The host talks to a worker over two worker-scoped channels it creates
//! before spawning it: `worker/W/commands` (attach, detach, shutdown) and
//! `worker/W/events` (ready, attached, detached, failed), each with a wake
//! event. See `clients/python/remotemedia/core/multiprocessing/shared_worker.py`.
//!
//! Sharing needs both the node class's `@shareable(key=...)` declaration and
//! the host policy ([`WorkerSharingConfig`]), and is off by default. Two
//! sessions share a worker only when the node type, the Python environment, the
//! sharing scope and the values of the declared key parameters all match.
//!
//! The pool is process-wide: executors are created per node, and a worker
//! outlives the sessions attached to it until it has been idle for
//! `idle_ttl_secs`.

use super::multiprocess_executor::MultiprocessConfig;
use super::process_manager::{ProcessHandle, ProcessManager, SpawnConfig};
use crate::multiprocess::ipc_channel::{ChannelHandle, ChannelRegistry};
use crate::{Error, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;
use tokio::sync::{oneshot, watch};

/// Capacity of the worker command and event channels (messages).
const CONTROL_CAPACITY: usize = 64;
/// How long the control thread blocks on the event wake before it sends
/// queued commands; bounds the latency of attach and detach commands.
const CONTROL_TICK: Duration = Duration::from_millis(5);
/// How long a detach may take before the lane is given up on.
const DETACH_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a worker may take to exit after `shutdown` before it is killed.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// Host policy for shared model workers (`[worker_sharing]` in runtime.toml).
///
/// `REMOTEMEDIA_SHARE_WORKERS=1|0` overrides `enabled`, and
/// `REMOTEMEDIA_SHARE_NODE_TYPES=A,B` overrides `node_types`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct WorkerSharingConfig {
    /// Master switch. Off by default for every node.
    pub enabled: bool,
    /// Share only these node types. Empty: every node type that declares
    /// `@shareable`.
    pub node_types: Vec<String>,
    /// Sessions one worker hosts; a full worker causes another worker for the
    /// same key.
    pub max_sessions_per_worker: usize,
    /// Workers per sharing key; beyond this a new session fails with
    /// `at_capacity`.
    pub max_workers_per_key: usize,
    /// How long a worker with no sessions stays loaded.
    pub idle_ttl_secs: u64,
    /// Whether a manifest node may turn sharing on for itself with
    /// `__share__: true` while `enabled` is off. Turn this off on hosts that
    /// accept client-supplied manifests.
    pub allow_manifest_opt_in: bool,
}

impl Default for WorkerSharingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_types: Vec::new(),
            max_sessions_per_worker: 8,
            max_workers_per_key: 4,
            idle_ttl_secs: 300,
            allow_manifest_opt_in: true,
        }
    }
}

impl WorkerSharingConfig {
    /// This policy with the `REMOTEMEDIA_SHARE_*` environment overrides applied.
    pub fn with_env_overrides(mut self) -> Self {
        if let Ok(value) = std::env::var("REMOTEMEDIA_SHARE_WORKERS") {
            self.enabled = matches!(value.trim(), "1" | "true" | "yes" | "on");
        }
        if let Ok(value) = std::env::var("REMOTEMEDIA_SHARE_MANIFEST_OPT_IN") {
            self.allow_manifest_opt_in = matches!(value.trim(), "1" | "true" | "yes" | "on");
        }
        if let Ok(value) = std::env::var("REMOTEMEDIA_SHARE_NODE_TYPES") {
            self.node_types = value
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
        }
        self
    }

    /// Whether the policy allows sharing this node. The node class must still
    /// declare `@shareable`; that is learned from its first worker. A manifest
    /// node opts out with `__share__: false`, and opts in with `__share__: true`
    /// when `allow_manifest_opt_in` permits it (the node-type list still applies).
    pub fn applies_to(&self, node_type: &str, params: &Value) -> bool {
        let listed =
            self.node_types.is_empty() || self.node_types.iter().any(|t| t == node_type);
        match params.get("__share__").and_then(Value::as_bool) {
            Some(false) => false,
            Some(true) => listed && (self.enabled || self.allow_manifest_opt_in),
            None => listed && self.enabled,
        }
    }
}

/// Error message prefix of a session refused because every worker is full.
pub const AT_CAPACITY: &str = "at_capacity";

/// The part of the key fixed by the node type and its Python environment.
fn stem_key(node_type: &str, spawn: &SpawnConfig) -> String {
    let mut env: Vec<_> = spawn.env_vars.iter().collect();
    env.sort();
    format!(
        "{node_type}\u{0}{:?}\u{0}{:?}\u{0}{:?}\u{0}{:?}\u{0}{:?}",
        spawn.python_executable, spawn.python_path, env, spawn.register_modules, spawn.working_dir
    )
}

/// The full sharing key: the stem, the sharing scope (`__share_scope__`, for
/// example a tenant; default the whole executor) and the declared parameters.
fn sharing_key(stem: &str, names: &[String], params: &Value) -> String {
    let scope = params.get("__share_scope__").map(Value::to_string).unwrap_or_default();
    let values: Vec<Value> = names
        .iter()
        .map(|name| params.get(name).cloned().unwrap_or(Value::Null))
        .collect();
    format!("{stem}\u{0}{scope}\u{0}{}", Value::Array(values))
}

/// Per (node type, Python environment): what the node declares, and its workers.
#[derive(Default)]
struct Stem {
    /// `None` until a worker reported it; then the class's `@shareable` key
    /// names, or `None` when the class does not opt in.
    names: Option<Option<Vec<String>>>,
    workers: HashMap<String, Vec<Arc<Worker>>>,
}

/// One worker's attached-session count, for diagnostics and telemetry.
#[derive(Debug, Clone, serde::Serialize)]
pub struct WorkerSnapshot {
    pub worker_id: String,
    pub node_type: String,
    pub pid: u32,
    pub attached_sessions: usize,
    pub alive: bool,
}

/// The process-wide pool of shared workers.
pub struct SharedWorkerPool {
    stems: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<Stem>>>>,
    next_worker: AtomicU64,
}

/// The process-wide pool.
pub fn pool() -> &'static SharedWorkerPool {
    static POOL: OnceLock<SharedWorkerPool> = OnceLock::new();
    POOL.get_or_init(|| SharedWorkerPool {
        stems: std::sync::Mutex::new(HashMap::new()),
        next_worker: AtomicU64::new(0),
    })
}

impl SharedWorkerPool {
    /// Reserve a place for one session on a worker for `node_type` with
    /// `params`, starting a worker if none has room. Returns `None` when the
    /// node class does not declare `@shareable` (run it unshared), and an
    /// `at_capacity` error when every allowed worker for the key is full.
    pub(crate) async fn reserve(
        &self,
        node_type: &str,
        params: &Value,
        spawn: SpawnConfig,
        policy: &WorkerSharingConfig,
        config: &MultiprocessConfig,
    ) -> Result<Option<Arc<Worker>>> {
        let stem_id = stem_key(node_type, &spawn);
        let stem = self
            .stems
            .lock()
            .unwrap()
            .entry(stem_id.clone())
            .or_default()
            .clone();
        // Held while a worker starts (Python import, not the model load), so
        // concurrent first sessions of one node type start one worker.
        let mut guard = stem.lock().await;
        let names = match &guard.names {
            Some(None) => return Ok(None),
            Some(Some(names)) => names.clone(),
            None => {
                let worker = self.start(node_type, String::new(), &stem, spawn.clone(), config).await?;
                let Some(names) = worker.shareable.clone() else {
                    tracing::info!("{node_type} does not declare @shareable; running it unshared");
                    guard.names = Some(None);
                    worker.shutdown().await;
                    return Ok(None);
                };
                guard.names = Some(Some(names.clone()));
                let key = sharing_key(&stem_id, &names, params);
                worker.set_key(key.clone());
                guard.workers.entry(key).or_default().push(worker);
                names
            }
        };
        let key = sharing_key(&stem_id, &names, params);
        let workers = guard.workers.entry(key.clone()).or_default();
        workers.retain(|worker| worker.alive());
        if let Some(worker) = workers
            .iter()
            .find(|worker| worker.try_reserve(policy.max_sessions_per_worker))
        {
            return Ok(Some(worker.clone()));
        }
        if workers.len() >= policy.max_workers_per_key {
            return Err(Error::Execution(format!(
                "{AT_CAPACITY}: all {} shared {node_type} workers are hosting {} sessions",
                workers.len(),
                policy.max_sessions_per_worker
            )));
        }
        let worker = self.start(node_type, key, &stem, spawn, config).await?;
        if !worker.try_reserve(policy.max_sessions_per_worker) {
            return Err(Error::Execution("shared worker refused its first session".into()));
        }
        // `guard` borrows from `stem`; re-borrow the list after the await.
        guard.workers.get_mut(&worker.key()).unwrap().push(worker.clone());
        Ok(Some(worker))
    }

    async fn start(
        &self,
        node_type: &str,
        key: String,
        stem: &Arc<tokio::sync::Mutex<Stem>>,
        spawn: SpawnConfig,
        config: &MultiprocessConfig,
    ) -> Result<Arc<Worker>> {
        let id = format!(
            "{}-{}-{}",
            std::process::id(),
            self.next_worker.fetch_add(1, Ordering::Relaxed),
            uuid::Uuid::new_v4().simple()
        );
        Worker::start(id, node_type, key, Arc::downgrade(stem), spawn, config).await
    }

    /// Attached-session counts of every live worker.
    pub async fn snapshot(&self) -> Vec<WorkerSnapshot> {
        let stems: Vec<_> = self.stems.lock().unwrap().values().cloned().collect();
        let mut out = Vec::new();
        for stem in stems {
            for worker in stem.lock().await.workers.values().flatten() {
                out.push(worker.snapshot());
            }
        }
        out
    }
}

/// Per-lane waiters inside a worker, resolved by the worker's events.
struct Lane {
    attached: Option<oneshot::Sender<std::result::Result<(), String>>>,
    detached: Option<oneshot::Sender<()>>,
    failure: watch::Sender<Option<String>>,
}

/// A running shared worker.
pub struct Worker {
    pub id: String,
    pub node_type: String,
    key: std::sync::Mutex<String>,
    stem: Weak<tokio::sync::Mutex<Stem>>,
    /// The worker process. Attached sessions record it as their node's process.
    pub process: ProcessHandle,
    manager: Arc<ProcessManager>,
    shareable: Option<Vec<String>>,
    attached: AtomicUsize,
    /// Bumped on every reservation, so an idle timer knows if it is stale.
    generation: AtomicU64,
    idle_ttl: Duration,
    commands: std::sync::Mutex<Option<std::sync::mpsc::Sender<Vec<u8>>>>,
    lanes: std::sync::Mutex<HashMap<(String, String), Lane>>,
    /// Set to the exit reason when the process exits.
    death: watch::Receiver<Option<String>>,
    stop_control: Arc<AtomicBool>,
    channels: std::sync::Mutex<Vec<ChannelHandle>>,
}

impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("id", &self.id)
            .field("node_type", &self.node_type)
            .field("pid", &self.process.id)
            .field("attached", &self.attached.load(Ordering::Relaxed))
            .finish()
    }
}

impl Worker {
    async fn start(
        id: String,
        node_type: &str,
        key: String,
        stem: Weak<tokio::sync::Mutex<Stem>>,
        spawn: SpawnConfig,
        config: &MultiprocessConfig,
    ) -> Result<Arc<Self>> {
        let registry = ChannelRegistry::global();
        let commands_name = format!("worker/{id}/commands");
        let events_name = format!("worker/{id}/events");
        let mut channels = Vec::new();
        for name in [&commands_name, &events_name] {
            channels.push(registry.create_channel(name, CONTROL_CAPACITY, false).await?);
            registry.create_wake_event(name).await?;
        }

        // The control thread owns the !Send iceoryx2 ports.
        let (command_tx, command_rx) = std::sync::mpsc::channel::<Vec<u8>>();
        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
        let stop_control = Arc::new(AtomicBool::new(false));
        spawn_control_thread(
            registry.clone(),
            commands_name,
            events_name,
            command_rx,
            event_tx,
            stop_control.clone(),
            id.clone(),
        )?;

        let manager = Arc::new(ProcessManager::with_spawn_config(config.clone(), spawn));
        let (death_tx, death) = watch::channel(None);
        let death_tx = Arc::new(death_tx);
        {
            let death_tx = death_tx.clone();
            manager
                .on_process_exit(move |_pid, reason| {
                    death_tx.send_replace(Some(format!("shared worker exited: {reason:?}")));
                })
                .await;
        }
        let process = match manager.spawn_shared_worker(node_type, &id).await {
            Ok(process) => process,
            Err(error) => {
                stop_control.store(true, Ordering::Release);
                for channel in channels {
                    let _ = registry.destroy_channel(channel).await;
                }
                return Err(error);
            }
        };

        // Wait for `ready`, which carries the class's sharing declaration.
        let ready = {
            let mut death = death.clone();
            let wait_ready = async {
                while let Some(bytes) = event_rx.recv().await {
                    match serde_json::from_slice::<Value>(&bytes) {
                        Ok(event) if event.get("event").and_then(Value::as_str) == Some("ready") => {
                            return Ok(event);
                        }
                        Ok(event) => tracing::warn!("shared worker {id}: event before ready: {event}"),
                        Err(error) => tracing::warn!("shared worker {id}: malformed event: {error}"),
                    }
                }
                Err("event channel closed".to_string())
            };
            tokio::select! {
                result = tokio::time::timeout(Duration::from_secs(config.init_timeout_secs), wait_ready) => {
                    result.map_err(|_| "no ready event".to_string()).and_then(|r| r)
                }
                died = death.wait_for(Option::is_some) => {
                    Err(died.map(|reason| reason.clone().unwrap_or_default())
                        .unwrap_or_else(|_| "worker exited".to_string()))
                }
            }
        };
        let ready = match ready {
            Ok(ready) => ready,
            Err(error) => {
                let cause = process
                    .last_error_line()
                    .map(|l| format!("; last stderr line: {l}"))
                    .unwrap_or_default();
                let _ = process.kill().await;
                stop_control.store(true, Ordering::Release);
                for channel in channels {
                    let _ = registry.destroy_channel(channel).await;
                }
                return Err(Error::Execution(format!(
                    "shared {node_type} worker did not start: {error}{cause}"
                )));
            }
        };
        let shareable = ready.get("shareable").and_then(Value::as_array).map(|names| {
            names
                .iter()
                .filter_map(|n| n.as_str().map(String::from))
                .collect::<Vec<_>>()
        });

        let worker = Arc::new(Self {
            id: id.clone(),
            node_type: node_type.to_string(),
            key: std::sync::Mutex::new(key),
            stem,
            process,
            manager,
            shareable,
            attached: AtomicUsize::new(0),
            generation: AtomicU64::new(0),
            idle_ttl: Duration::from_secs(
                config.worker_sharing.clone().with_env_overrides().idle_ttl_secs,
            ),
            commands: std::sync::Mutex::new(Some(command_tx)),
            lanes: std::sync::Mutex::new(HashMap::new()),
            death,
            stop_control,
            channels: std::sync::Mutex::new(channels),
        });

        // Dispatch events to the lanes they belong to.
        let weak = Arc::downgrade(&worker);
        tokio::spawn(async move {
            while let Some(bytes) = event_rx.recv().await {
                let Some(worker) = weak.upgrade() else { break };
                match serde_json::from_slice::<Value>(&bytes) {
                    Ok(event) => worker.dispatch(&event),
                    Err(error) => tracing::warn!("shared worker {}: malformed event: {error}", worker.id),
                }
            }
        });
        // Fail every attached lane when the process dies.
        let weak = Arc::downgrade(&worker);
        let mut death = worker.death.clone();
        tokio::spawn(async move {
            if death.wait_for(Option::is_some).await.is_err() {
                return;
            }
            let reason = death.borrow().clone().unwrap_or_default();
            if let Some(worker) = weak.upgrade() {
                tracing::error!("shared worker {} ({}) died: {reason}", worker.id, worker.node_type);
                for lane in worker.lanes.lock().unwrap().values_mut() {
                    if let Some(attached) = lane.attached.take() {
                        let _ = attached.send(Err(reason.clone()));
                    }
                    if let Some(detached) = lane.detached.take() {
                        let _ = detached.send(());
                    }
                    lane.failure.send_replace(Some(reason.clone()));
                }
                worker.stop_control.store(true, Ordering::Release);
            }
        });

        tracing::info!(
            "shared worker {id} for {node_type} started (pid {}, shareable: {:?})",
            worker.process.id,
            worker.shareable
        );
        Ok(worker)
    }

    fn key(&self) -> String {
        self.key.lock().unwrap().clone()
    }

    fn set_key(&self, key: String) {
        *self.key.lock().unwrap() = key;
    }

    /// Whether the worker process is still running.
    pub fn alive(&self) -> bool {
        self.death.borrow().is_none()
    }

    fn try_reserve(&self, max_sessions: usize) -> bool {
        if !self.alive() {
            return false;
        }
        let reserved = self
            .attached
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| (n < max_sessions).then_some(n + 1))
            .is_ok();
        if reserved {
            self.generation.fetch_add(1, Ordering::AcqRel);
        }
        reserved
    }

    /// Give back a reservation. A worker left with no sessions is shut down
    /// after its idle TTL unless a session reserves it first.
    fn release(self: &Arc<Self>) {
        if self.attached.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        let generation = self.generation.load(Ordering::Acquire);
        let weak = Arc::downgrade(self);
        let ttl = self.idle_ttl;
        tokio::spawn(async move {
            tokio::time::sleep(ttl).await;
            let Some(worker) = weak.upgrade() else { return };
            let Some(stem) = worker.stem.upgrade() else { return };
            let mut guard = stem.lock().await;
            if worker.attached.load(Ordering::Acquire) != 0
                || worker.generation.load(Ordering::Acquire) != generation
            {
                return;
            }
            if let Some(list) = guard.workers.get_mut(&worker.key()) {
                list.retain(|w| !Arc::ptr_eq(w, &worker));
            }
            drop(guard);
            tracing::info!("shared worker {} idle for {:?}; shutting it down", worker.id, ttl);
            worker.shutdown().await;
        });
    }

    fn send(&self, command: Value) -> Result<()> {
        let sender = self.commands.lock().unwrap();
        sender
            .as_ref()
            .ok_or_else(|| Error::Execution(format!("shared worker {} is stopped", self.id)))?
            .send(command.to_string().into_bytes())
            .map_err(|_| Error::Execution(format!("shared worker {} control thread is gone", self.id)))
    }

    fn dispatch(&self, event: &Value) {
        let kind = event.get("event").and_then(Value::as_str).unwrap_or_default();
        let lane_id = (
            event.get("session").and_then(Value::as_str).unwrap_or_default().to_string(),
            event.get("node").and_then(Value::as_str).unwrap_or_default().to_string(),
        );
        let mut lanes = self.lanes.lock().unwrap();
        let Some(lane) = lanes.get_mut(&lane_id) else {
            tracing::debug!("shared worker {}: {kind} event for unknown lane {lane_id:?}", self.id);
            return;
        };
        match kind {
            "attached" => {
                if let Some(attached) = lane.attached.take() {
                    let _ = attached.send(Ok(()));
                }
            }
            "detached" => {
                if let Some(detached) = lane.detached.take() {
                    let _ = detached.send(());
                }
            }
            "failed" => {
                let error = event.get("error").and_then(Value::as_str).unwrap_or("lane failed").to_string();
                tracing::error!("shared worker {}: lane {lane_id:?} failed: {error}", self.id);
                if let Some(attached) = lane.attached.take() {
                    let _ = attached.send(Err(error.clone()));
                }
                if let Some(detached) = lane.detached.take() {
                    let _ = detached.send(());
                }
                lane.failure.send_replace(Some(error));
            }
            other => tracing::warn!("shared worker {}: unknown event {other:?}", self.id),
        }
    }

    /// Attach a session lane to this reserved worker. The lane's channels and
    /// wake events must already exist; the node signals READY on the lane's
    /// own control channel once its `initialize()` completes.
    pub(crate) async fn attach(
        self: &Arc<Self>,
        session_id: &str,
        node_id: &str,
        params: &Value,
        copy_authority: Option<super::copy_authority::CopyAuthority>,
        timeout: Duration,
    ) -> Result<Lease> {
        let lane_id = (session_id.to_string(), node_id.to_string());
        let (attached_tx, attached_rx) = oneshot::channel();
        let (failure_tx, failure) = watch::channel(None);
        {
            let mut lanes = self.lanes.lock().unwrap();
            if lanes.contains_key(&lane_id) {
                self.release();
                return Err(Error::Execution(format!(
                    "session {session_id} node {node_id} is already attached to shared worker {}",
                    self.id
                )));
            }
            lanes.insert(
                lane_id.clone(),
                Lane { attached: Some(attached_tx), detached: None, failure: failure_tx },
            );
        }
        let bootstrap = copy_authority
            .as_ref()
            .map(|a| serde_json::to_value(a.bootstrap()).unwrap_or(Value::Null))
            .unwrap_or(Value::Null);
        let started = std::time::Instant::now();
        let result = match self.send(serde_json::json!({
            "op": "attach",
            "session": session_id,
            "node": node_id,
            "node_type": self.node_type,
            "params": params,
            "copy_authority": bootstrap,
        })) {
            Ok(()) => match tokio::time::timeout(timeout, attached_rx).await {
                Ok(Ok(Ok(()))) => Ok(()),
                Ok(Ok(Err(error))) => Err(error),
                Ok(Err(_)) => Err("worker dropped the attach".to_string()),
                Err(_) => Err(format!("no reply within {timeout:?}")),
            },
            Err(error) => Err(error.to_string()),
        };
        let lease = Lease {
            worker: self.clone(),
            session_id: session_id.to_string(),
            node_id: node_id.to_string(),
            failure,
            copy_authority,
            released: false,
        };
        match result {
            Ok(()) => {
                tracing::info!(
                    "session {session_id} node {node_id} attached to shared worker {} in {:?} ({} sessions)",
                    self.id,
                    started.elapsed(),
                    self.attached.load(Ordering::Acquire)
                );
                Ok(lease)
            }
            Err(error) => {
                drop(lease); // detaches (best effort) and releases the reservation
                Err(Error::Execution(format!(
                    "attaching session {session_id} to shared {} worker {} failed: {error}",
                    self.node_type, self.id
                )))
            }
        }
    }

    /// Ask the worker to exit; kill it if it does not within the grace period.
    async fn shutdown(&self) {
        let _ = self.send(serde_json::json!({"op": "shutdown"}));
        let deadline = std::time::Instant::now() + SHUTDOWN_GRACE;
        while self.process.exit_status().await.is_none() && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        if let Err(error) = self.manager.terminate_process(self.process.clone(), Duration::from_secs(1)).await {
            tracing::warn!("shared worker {}: {error}", self.id);
        }
        self.commands.lock().unwrap().take();
        self.stop_control.store(true, Ordering::Release);
        let channels: Vec<_> = std::mem::take(&mut *self.channels.lock().unwrap());
        let registry = ChannelRegistry::global();
        for channel in channels {
            let _ = registry.destroy_channel(channel).await;
        }
    }

    fn snapshot(&self) -> WorkerSnapshot {
        WorkerSnapshot {
            worker_id: self.id.clone(),
            node_type: self.node_type.clone(),
            pid: self.process.id,
            attached_sessions: self.attached.load(Ordering::Acquire),
            alive: self.alive(),
        }
    }
}

/// One session lane attached to a shared worker. Detach it with
/// [`Lease::detach`]; dropping it detaches without waiting.
pub struct Lease {
    pub worker: Arc<Worker>,
    session_id: String,
    node_id: String,
    failure: watch::Receiver<Option<String>>,
    copy_authority: Option<super::copy_authority::CopyAuthority>,
    released: bool,
}

impl std::fmt::Debug for Lease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lease")
            .field("worker", &self.worker.id)
            .field("session", &self.session_id)
            .field("node", &self.node_id)
            .finish()
    }
}

impl Lease {
    /// Why the lane failed, if it has (its node stopped, or the worker died).
    pub fn failure(&self) -> Option<String> {
        self.failure.borrow().clone().or_else(|| self.worker.death.borrow().clone())
    }

    /// A receiver that resolves when the lane fails or the worker dies.
    pub fn failure_watch(&self) -> (watch::Receiver<Option<String>>, watch::Receiver<Option<String>>) {
        (self.failure.clone(), self.worker.death.clone())
    }

    /// Detach the lane: the worker runs the node's `on_session_detach()` and
    /// `cleanup()`, closes the lane's ports and copy reporting. The worker
    /// stays up for other sessions (and for its idle TTL).
    pub async fn detach(mut self) {
        self.released = true;
        let lane_id = (self.session_id.clone(), self.node_id.clone());
        let mut detached = None;
        if self.worker.alive() && self.failure.borrow().is_none() {
            let (tx, rx) = oneshot::channel();
            if let Some(lane) = self.worker.lanes.lock().unwrap().get_mut(&lane_id) {
                lane.detached = Some(tx);
            }
            if self
                .worker
                .send(serde_json::json!({"op": "detach", "session": self.session_id, "node": self.node_id}))
                .is_ok()
            {
                detached = Some(rx);
            }
        } else if self.failure.borrow().is_some() && self.worker.alive() {
            // The lane already stopped; tell the worker to forget it.
            let _ = self.worker.send(serde_json::json!({"op": "detach", "session": self.session_id, "node": self.node_id}));
        }
        if let Some(rx) = detached {
            if tokio::time::timeout(DETACH_TIMEOUT, rx).await.is_err() {
                tracing::warn!(
                    "shared worker {}: session {} node {} did not detach within {:?}",
                    self.worker.id, self.session_id, self.node_id, DETACH_TIMEOUT
                );
            }
        }
        self.worker.lanes.lock().unwrap().remove(&lane_id);
        if let Some(authority) = self.copy_authority.take() {
            let node = self.node_id.clone();
            let finished = tokio::task::spawn_blocking(move || authority.finish()).await;
            if !matches!(finished, Ok(Ok(()))) {
                tracing::error!("copy accounting for shared lane {node} did not close cleanly");
            }
        }
        self.worker.release();
        tracing::info!(
            "session {} node {} detached from shared worker {} ({} sessions left)",
            self.session_id,
            self.node_id,
            self.worker.id,
            self.worker.attached.load(Ordering::Acquire)
        );
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        let lane_id = (self.session_id.clone(), self.node_id.clone());
        self.worker.lanes.lock().unwrap().remove(&lane_id);
        if self.worker.alive() {
            let _ = self
                .worker
                .send(serde_json::json!({"op": "detach", "session": self.session_id, "node": self.node_id}));
        }
        // `release` schedules the idle timer on the runtime.
        if tokio::runtime::Handle::try_current().is_ok() {
            self.worker.release();
        } else {
            self.worker.attached.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// The thread that owns a worker's command publisher and event subscriber.
/// It sends queued commands, then blocks on the event wake for
/// [`CONTROL_TICK`] and forwards every event received.
fn spawn_control_thread(
    registry: Arc<ChannelRegistry>,
    commands_name: String,
    events_name: String,
    commands: std::sync::mpsc::Receiver<Vec<u8>>,
    events: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    stop: Arc<AtomicBool>,
    worker_id: String,
) -> Result<()> {
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<std::result::Result<(), String>>();
    std::thread::Builder::new()
        .name(format!("shared-worker-{worker_id}"))
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread().build() {
                Ok(rt) => rt,
                Err(error) => {
                    let _ = ready_tx.send(Err(error.to_string()));
                    return;
                }
            };
            let ports = rt.block_on(async {
                Ok::<_, Error>((
                    registry.create_publisher(&commands_name).await?,
                    registry.create_wake_notifier(&commands_name).await?,
                    registry.create_raw_subscriber(&events_name).await?,
                    registry.create_wake_listener(&events_name).await?,
                ))
            });
            let (publisher, notifier, subscriber, listener) = match ports {
                Ok(ports) => ports,
                Err(error) => {
                    let _ = ready_tx.send(Err(error.to_string()));
                    return;
                }
            };
            let _ = ready_tx.send(Ok(()));
            loop {
                let mut disconnected = false;
                loop {
                    match commands.try_recv() {
                        Ok(command) => {
                            if let Err(error) = publisher.send(&command) {
                                tracing::error!("shared worker {worker_id}: command not sent: {error}");
                            }
                            let _ = notifier.notify();
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => break,
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            disconnected = true;
                            break;
                        }
                    }
                }
                if disconnected || stop.load(Ordering::Acquire) {
                    break;
                }
                if let Err(error) = listener.timed_wait(|_| {}, CONTROL_TICK) {
                    tracing::debug!("shared worker {worker_id}: event wait failed: {error:?}");
                    std::thread::sleep(CONTROL_TICK);
                }
                loop {
                    match subscriber.receive() {
                        Ok(Some(sample)) => {
                            if events.send(sample.payload().to_vec()).is_err() {
                                return;
                            }
                        }
                        Ok(None) => break,
                        Err(error) => {
                            tracing::warn!("shared worker {worker_id}: event receive failed: {error:?}");
                            break;
                        }
                    }
                }
            }
        })
        .map_err(|error| Error::Execution(format!("cannot start shared worker control thread: {error}")))?;
    match ready_rx.recv() {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(Error::IpcError(format!("shared worker control ports: {error}"))),
        Err(_) => Err(Error::IpcError("shared worker control thread exited".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_is_off_by_default_and_honours_opt_out() {
        let policy = WorkerSharingConfig::default();
        assert!(!policy.applies_to("KokoroTTSNode", &serde_json::json!({})));
        let policy = WorkerSharingConfig { enabled: true, ..Default::default() };
        assert!(policy.applies_to("KokoroTTSNode", &serde_json::json!({})));
        assert!(!policy.applies_to("KokoroTTSNode", &serde_json::json!({"__share__": false})));
        let policy = WorkerSharingConfig {
            enabled: true,
            node_types: vec!["WhisperSTTNode".into()],
            ..Default::default()
        };
        assert!(!policy.applies_to("KokoroTTSNode", &serde_json::json!({})));
        assert!(policy.applies_to("WhisperSTTNode", &serde_json::json!({})));
    }

    #[test]
    fn key_uses_declared_parameters_and_scope_only() {
        let spawn = SpawnConfig::default();
        let stem = stem_key("KokoroTTSNode", &spawn);
        let names = vec!["model".to_string(), "device".to_string()];
        let base = serde_json::json!({"model": "a", "device": "cuda", "voice": "x"});
        let other_voice = serde_json::json!({"model": "a", "device": "cuda", "voice": "y"});
        let other_device = serde_json::json!({"model": "a", "device": "cpu", "voice": "x"});
        let tenant = serde_json::json!({"model": "a", "device": "cuda", "__share_scope__": "t1"});
        assert_eq!(sharing_key(&stem, &names, &base), sharing_key(&stem, &names, &other_voice));
        assert_ne!(sharing_key(&stem, &names, &base), sharing_key(&stem, &names, &other_device));
        assert_ne!(sharing_key(&stem, &names, &base), sharing_key(&stem, &names, &tenant));
        let other_env = SpawnConfig { python_executable: "other/python".into(), ..Default::default() };
        assert_ne!(stem, stem_key("KokoroTTSNode", &other_env));
    }
}
