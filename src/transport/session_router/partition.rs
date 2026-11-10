//! Per-participant node instances (openspec: add-multi-participant-sessions,
//! capability `participant-scoped-nodes`).
//!
//! A node whose effective instance scope is `per_participant` keeps its one
//! router input queue, so upstream wiring is unchanged. In place of the node
//! task, a [`Dispatcher`] reads that queue and hands each frame to the instance
//! for the frame's participant ([`Routed::origin`]):
//!
//! - The instance created at session start serves the first participant
//!   (or unattributed frames, whichever arrives first).
//! - Every later participant gets a new instance, created and initialized on
//!   its own task so other participants never wait for it. Its queue exists
//!   at once, so the participant's frames buffer during initialization.
//! - Frames without a participant go to one default instance. Control
//!   envelopes without a participant (e.g. `barge_in`) go to every instance.
//! - A participant that leaves (see [`ParticipantEvent::Left`]) has its
//!   instance's queue closed: the instance drains, runs its shutdown and exits.
//! - At most `max_instances` instances run; frames of further participants
//!   are dropped and one error per participant is published.
//!
//! All instances share the node's successors, its control-bus identity (state,
//! taps, intercepts) and its telemetry slot.

use super::{NODE_INPUT_CAPACITY, StreamingNode};
use crate::transport::origin::Routed;
use crate::transport::session_control::{aux_port_of, SessionControl};
use crate::{Error, Result};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;

/// Instances per per-participant node when the manifest sets no
/// `max_instances` (`REMOTEMEDIA_MAX_PARTICIPANT_INSTANCES` overrides).
pub(super) const DEFAULT_MAX_INSTANCES: usize = 32;

pub(super) fn default_max_instances() -> usize {
    std::env::var("REMOTEMEDIA_MAX_PARTICIPANT_INSTANCES")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &usize| *value > 0)
        .unwrap_or(DEFAULT_MAX_INSTANCES)
}

/// Session-level participant lifecycle, broadcast to every dispatcher.
#[derive(Debug, Clone)]
pub(crate) enum ParticipantEvent {
    /// The participant with this id left the session.
    Left(String),
}

/// Starts the tasks of one node instance on the given input queue. `primary`
/// is the instance that owns the node's liveness telemetry and RPC handlers.
pub(super) type SpawnInstance = Arc<
    dyn Fn(Box<dyn StreamingNode>, mpsc::Receiver<Routed>, bool) -> (JoinHandle<Result<()>>, JoinHandle<Result<()>>)
        + Send
        + Sync,
>;

/// Creates and initializes further instances of one node.
pub(super) struct InstanceFactory {
    pub(super) registry: Arc<crate::nodes::StreamingNodeRegistry>,
    pub(super) node_type: String,
    pub(super) params: serde_json::Value,
    pub(super) session_id: String,
    pub(super) control: Option<Arc<SessionControl>>,
    pub(super) copies: remotemedia_types::CopyContext,
    pub(super) contract: Option<remotemedia_traits::buffer_contract::BufferContract>,
    pub(super) allocator: Option<Arc<remotemedia_types::BoundOutputAllocator>>,
    #[cfg(all(feature = "native-worker", any(windows, target_os = "linux")))]
    pub(super) mapped_pool: Option<crate::multiprocess::native_worker::SharedPool>,
}

impl InstanceFactory {
    /// Create and initialize an instance under `instance_id` (which names its
    /// worker channels, so it must differ from every other instance's).
    async fn create(&self, instance_id: &str) -> Result<Box<dyn StreamingNode>> {
        let mut node = self.registry.create_node(
            &self.node_type,
            instance_id.to_string(),
            &self.params,
            Some(self.session_id.clone()),
        )?;
        if let Some(contract) = &self.contract {
            let checked = crate::transport::buffer_validation::BufferCheckedNode::new(
                instance_id.to_string(),
                contract.clone(),
                node,
                self.allocator.clone(),
            );
            #[cfg(all(feature = "native-worker", any(windows, target_os = "linux")))]
            let checked = match &self.mapped_pool {
                Some(pool) => checked.with_mapped_pool(pool.clone()),
                None => checked,
            };
            node = Box::new(checked);
        }
        let ctx = crate::nodes::InitializeContext {
            session_id: self.session_id.clone(),
            node_id: instance_id.to_string(),
            control: self.control.clone(),
        }
        .with_copy_context(self.copies.clone());
        node.initialize(&ctx).await?;
        Ok(node)
    }
}

/// The key of frames that carry no participant.
const DEFAULT_KEY: &str = "";

pub(super) struct Dispatcher {
    pub(super) node_id: String,
    pub(super) session_id: String,
    pub(super) input_rx: mpsc::Receiver<Routed>,
    /// The instance created at session start; serves the first participant.
    pub(super) primary: Option<Box<dyn StreamingNode>>,
    pub(super) factory: Arc<InstanceFactory>,
    pub(super) spawn: SpawnInstance,
    pub(super) max_instances: usize,
    pub(super) control: Option<Arc<SessionControl>>,
    pub(super) telemetry: Arc<crate::telemetry::SessionTelemetry>,
    pub(super) node_index: usize,
    pub(super) lifecycle: broadcast::Receiver<ParticipantEvent>,
    pub(super) shutdown: watch::Receiver<bool>,
}

struct Instance {
    tx: mpsc::Sender<Routed>,
}

impl Dispatcher {
    pub(super) async fn run(mut self) -> Result<()> {
        let mut instances: HashMap<String, Instance> = HashMap::new();
        let mut tasks: Vec<(String, JoinHandle<Result<()>>)> = Vec::new();
        let mut refused: HashSet<String> = HashSet::new();
        let mut created = 0usize;
        loop {
            tokio::select! {
                biased;
                _ = self.shutdown.changed() => break,
                event = self.lifecycle.recv() => match event {
                    Ok(ParticipantEvent::Left(id)) => {
                        if instances.remove(&id).is_some() {
                            tracing::info!(
                                session_id = %self.session_id,
                                node_id = %self.node_id,
                                participant = %id,
                                "participant left; its node instance drains and stops"
                            );
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => {}
                },
                frame = self.input_rx.recv() => {
                    let Some(frame) = frame else { break };
                    self.route(frame, &mut instances, &mut tasks, &mut refused, &mut created).await;
                }
            }
        }
        // Closing the instance queues lets each instance drain and stop.
        drop(instances);
        let mut failures = Vec::new();
        for (label, task) in tasks {
            match task.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(format!("{label}: {error}")),
                Err(error) => failures.push(format!("{label}: {error}")),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(Error::Execution(format!(
                "node {} instances: {}",
                self.node_id,
                failures.join("; ")
            )))
        }
    }

    async fn route(
        &mut self,
        frame: Routed,
        instances: &mut HashMap<String, Instance>,
        tasks: &mut Vec<(String, JoinHandle<Result<()>>)>,
        refused: &mut HashSet<String>,
        created: &mut usize,
    ) {
        // A control envelope from nobody in particular (barge-in, shutdown
        // hints, ...) concerns every participant's instance.
        if frame.origin.is_none() && aux_port_of(&frame.data).is_some() && !instances.is_empty() {
            for instance in instances.values() {
                let _ = instance.tx.send(frame.clone()).await;
            }
            return;
        }
        let key = frame
            .origin
            .as_ref()
            .map(|origin| origin.id().to_string())
            .unwrap_or_else(|| DEFAULT_KEY.to_string());
        if !instances.contains_key(&key) {
            if instances.len() >= self.max_instances {
                if refused.insert(key.clone()) {
                    self.refuse(&key);
                }
                return;
            }
            let instance = self.open(&key, tasks, created);
            instances.insert(key.clone(), instance);
        }
        let tx = instances[&key].tx.clone();
        if tx.send(frame).await.is_err() {
            // The instance failed to start or stopped; its error was reported.
            instances.remove(&key);
        }
    }

    fn open(
        &mut self,
        key: &str,
        tasks: &mut Vec<(String, JoinHandle<Result<()>>)>,
        created: &mut usize,
    ) -> Instance {
        let (tx, rx) = mpsc::channel(NODE_INPUT_CAPACITY);
        let who = if key == DEFAULT_KEY { "unattributed input" } else { key };
        if let Some(node) = self.primary.take() {
            tracing::info!(
                session_id = %self.session_id,
                node_id = %self.node_id,
                participant = %who,
                "per-participant node: startup instance serves this participant"
            );
            let (main, fan) = (self.spawn)(node, rx, true);
            tasks.push((format!("{} ({who}) main", self.node_id), main));
            tasks.push((format!("{} ({who}) fan-out", self.node_id), fan));
            return Instance { tx };
        }
        *created += 1;
        let instance_id = format!("{}__p{}", self.node_id, created);
        tracing::info!(
            session_id = %self.session_id,
            node_id = %self.node_id,
            instance = %instance_id,
            participant = %who,
            "per-participant node: starting an instance"
        );
        let factory = Arc::clone(&self.factory);
        let spawn = Arc::clone(&self.spawn);
        let control = self.control.clone();
        let telemetry = Arc::clone(&self.telemetry);
        let node_index = self.node_index;
        let node_id = self.node_id.clone();
        let label = format!("{} ({who})", self.node_id);
        let who = who.to_string();
        let task = tokio::spawn(async move {
            let node = match factory.create(&instance_id).await {
                Ok(node) => node,
                Err(error) => {
                    let message = format!("instance for participant '{who}' failed to start: {error}");
                    tracing::error!(node_id = %node_id, instance = %instance_id, "{message}");
                    crate::telemetry::publisher::record_node_error(
                        &telemetry,
                        Some(node_index),
                        control.as_deref(),
                        &node_id,
                        &message,
                    );
                    // Dropping `rx` makes the dispatcher forget this participant.
                    return Err(Error::Execution(message));
                }
            };
            let (main, fan) = spawn(node, rx, false);
            let main = main.await.map_err(|e| Error::Execution(e.to_string()))?;
            let fan = fan.await.map_err(|e| Error::Execution(e.to_string()))?;
            main.and(fan)
        });
        tasks.push((label, task));
        Instance { tx }
    }

    fn refuse(&self, key: &str) {
        let message = format!(
            "per-participant node reached max_instances = {}; dropping frames from participant '{}'",
            self.max_instances, key
        );
        tracing::error!(session_id = %self.session_id, node_id = %self.node_id, "{message}");
        crate::telemetry::publisher::record_node_error(
            &self.telemetry,
            Some(self.node_index),
            self.control.as_deref(),
            &self.node_id,
            &message,
        );
    }
}
