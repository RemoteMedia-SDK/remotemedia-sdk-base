//! Releasing a session's iceoryx2 resources when the session ends.
//!
//! A node opens five services (input, output and control channels, and the
//! wake events of the first two). A service exists for as long as any node
//! on the machine has it open, and each one costs the host handles from
//! iceoryx2's bounded tables. Three parties hold a session's services:
//!
//! - the process-wide [`ChannelRegistry`], which keeps the port factories;
//! - the node's IPC thread and its wake thread, which own the ports;
//! - the worker process. A worker that is killed, or that exits without
//!   closing its ports, stays registered in its services as a dead node until
//!   something removes it.
//!
//! [`SessionIpc::release`] lets go of all three. It runs when a session is
//! terminated explicitly, when a worker crash ends it, and when the last
//! clone of its executor is dropped, which is how a streaming session closes
//! and what happens to a node whose initialization failed part-way.

use super::*;
use iceoryx2::prelude::{ipc, MessagingPattern, ServiceName};
use iceoryx2::service::Service;
use std::time::{Duration, Instant};

/// How long an IPC thread gets to take its shutdown command and exit.
const THREAD_EXIT_WAIT: Duration = Duration::from_secs(5);

/// How long past the stop grace period a worker is waited for.
const WORKER_EXIT_MARGIN: Duration = Duration::from_secs(3);

/// How long the services get to disappear once every holder has let go,
/// before dead nodes are looked for.
const SERVICE_SETTLE: Duration = Duration::from_millis(500);

/// What a session holds in iceoryx2, taken out of its [`SessionState`].
pub(super) struct SessionIpc {
    session_id: String,
    threads: Vec<NodeIpcThread>,
    channels: Vec<String>,
    /// The session's own worker processes. A shared worker is not waited
    /// for: it closes the lane's ports when the lane detaches and lives on.
    workers: Vec<ProcessHandle>,
}

impl SessionIpc {
    /// Take the IPC threads and channels out of `session`. Its processes and
    /// shared-worker leases stay where they are.
    pub(super) fn take(session: &mut SessionState) -> Self {
        Self {
            session_id: session.session_id.clone(),
            threads: session.ipc_threads.drain().map(|(_, thread)| thread).collect(),
            channels: session.channels.drain().map(|(name, _)| name).collect(),
            workers: session
                .node_processes
                .iter()
                .filter(|(node, _)| !session.shared_lanes.contains_key(*node))
                .map(|(_, process)| process.clone())
                .collect(),
        }
    }

    fn is_empty(&self) -> bool {
        self.threads.is_empty() && self.channels.is_empty()
    }

    /// Release on a thread of its own. For callers that must not block: a
    /// drop, or a task on the async runtime.
    pub(super) fn release_in_background(self, registry: Arc<ChannelRegistry>) {
        if self.is_empty() {
            return;
        }
        let session_id = self.session_id.clone();
        let spawned = std::thread::Builder::new()
            .name("py-ipc-release".into())
            .spawn(move || self.release(&registry));
        if let Err(error) = spawned {
            tracing::error!(
                "Session {session_id}: no thread to release its IPC channels on ({error}); its iceoryx2 services stay open"
            );
        }
    }

    /// Stop the IPC threads, drop the registry's handles, wait for the
    /// workers to exit, and remove what a dead worker left registered.
    ///
    /// Blocks; call it from a thread that is not running async tasks.
    pub(super) fn release(self, registry: &ChannelRegistry) {
        let Self { session_id, threads, channels, workers } = self;

        // No new command reaches the threads. A session's nodes each have
        // their own executor: only this one's routes are removed.
        {
            let routes = global_sessions();
            let mut routes = routes.blocking_write();
            if let Some(nodes) = routes.get_mut(&session_id) {
                for thread in &threads {
                    if nodes
                        .get(&thread.node_id)
                        .is_some_and(|route| route.same_channel(&thread.command_tx))
                    {
                        nodes.remove(&thread.node_id);
                    }
                }
                if nodes.is_empty() {
                    routes.remove(&session_id);
                }
            }
        }

        let stop_by = Instant::now() + THREAD_EXIT_WAIT;
        let mut joins = Vec::new();
        for mut thread in threads {
            request_shutdown(&thread, stop_by);
            if let Some(handle) = thread.thread_handle.take() {
                joins.push((thread.node_id.clone(), handle));
            }
            // Dropping the handle closes the response lane, which unblocks a
            // thread waiting to deliver an output nobody reads any more.
        }

        // The factories. Ports still open on an IPC thread keep their
        // service alive until that thread exits.
        for channel in &channels {
            registry.release_channel_blocking(channel);
        }

        for (node_id, handle) in joins {
            while !handle.is_finished() && Instant::now() < stop_by {
                std::thread::sleep(Duration::from_millis(5));
            }
            if handle.is_finished() {
                let _ = handle.join();
            } else {
                tracing::warn!(
                    "Session {session_id}: IPC thread of node {node_id} did not stop within {THREAD_EXIT_WAIT:?}; its ports stay open until it does"
                );
            }
        }

        // The workers were asked to stop by whoever ended the session. One
        // that closes its ports on the way out removes the services itself.
        let exit_by = Instant::now() + super::super::process_manager::stop_grace() + WORKER_EXIT_MARGIN;
        while workers.iter().any(|worker| worker.exited_now() != Some(true))
            && Instant::now() < exit_by
        {
            std::thread::sleep(Duration::from_millis(20));
        }

        let settle_by = Instant::now() + SERVICE_SETTLE;
        let mut left = existing_services(&channels);
        while !left.is_empty() && Instant::now() < settle_by {
            std::thread::sleep(Duration::from_millis(20));
            left = existing_services(&channels);
        }
        if left.is_empty() {
            tracing::debug!("Session {session_id}: released {} IPC channel(s)", channels.len());
            return;
        }

        if workers.is_empty() {
            // Only a shared worker had these lanes open, and it is alive:
            // there is no dead node to remove. A worker whose client does not
            // close a detached lane's ports keeps them until it exits.
            tracing::debug!(
                "Session {session_id}: {} service(s) are still open in a shared worker: {left:?}",
                left.len()
            );
            return;
        }

        // A worker that died with its ports open is still registered in the
        // services. Remove the dead nodes; the services go with the last one.
        let (removed, failed) = crate::multiprocess::runner::cleanup_dead_nodes(
            iceoryx2::config::Config::global_config(),
        );
        let still_left = existing_services(&channels);
        if still_left.is_empty() {
            tracing::debug!(
                "Session {session_id}: {} service(s) were held by dead nodes; removed {removed} dead node(s)",
                left.len()
            );
        } else {
            tracing::warn!(
                "Session {session_id}: {} iceoryx2 service(s) still exist after its release (dead nodes removed: {removed}, not removable: {failed}): {still_left:?}",
                still_left.len()
            );
        }
    }
}

/// Send the shutdown command, retrying while the command queue is full.
fn request_shutdown(thread: &NodeIpcThread, give_up: Instant) {
    use tokio::sync::mpsc::error::TrySendError;
    loop {
        match thread.command_tx.try_send(IpcCommand::Shutdown) {
            Ok(()) | Err(TrySendError::Closed(_)) => return,
            Err(TrySendError::Full(_)) if Instant::now() < give_up => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(TrySendError::Full(_)) => return,
        }
    }
}

/// The services of `channels` (each channel and its wake event) that exist
/// on this machine.
pub(super) fn existing_services(channels: &[String]) -> Vec<String> {
    let config = iceoryx2::config::Config::global_config();
    let exists = |name: &str, pattern: MessagingPattern| {
        ServiceName::new(name)
            .ok()
            .and_then(|service| ipc::Service::does_exist(&service, config, pattern).ok())
            .unwrap_or(false)
    };
    let mut existing = Vec::new();
    for channel in channels {
        if exists(channel, MessagingPattern::PublishSubscribe) {
            existing.push(channel.clone());
        }
        let wake = crate::multiprocess::ipc_channel::wake_event_name(channel);
        if exists(&wake, MessagingPattern::Event) {
            existing.push(wake);
        }
    }
    existing
}

/// Releases the IPC resources of every session an executor still has when
/// its last clone is dropped.
pub(super) struct ReleaseOnDrop {
    pub(super) sessions: Arc<RwLock<HashMap<String, SessionState>>>,
    pub(super) registry: Arc<ChannelRegistry>,
}

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        // Runs on async worker threads and during runtime shutdown: never
        // block here. The work happens on a thread of its own.
        let take = |sessions: &mut HashMap<String, SessionState>| -> Vec<SessionIpc> {
            sessions
                .values_mut()
                .map(SessionIpc::take)
                .filter(|ipc| !ipc.is_empty())
                .collect()
        };
        let registry = self.registry.clone();
        let spawned = match self.sessions.try_write() {
            Ok(mut sessions) => {
                let taken = take(&mut sessions);
                drop(sessions);
                if taken.is_empty() {
                    return;
                }
                std::thread::Builder::new().name("py-ipc-release".into()).spawn(move || {
                    for ipc in taken {
                        ipc.release(&registry);
                    }
                })
            }
            // A task is using the session table; wait for it on the thread.
            Err(_) => {
                let sessions = self.sessions.clone();
                std::thread::Builder::new().name("py-ipc-release".into()).spawn(move || {
                    let taken = take(&mut sessions.blocking_write());
                    for ipc in taken {
                        ipc.release(&registry);
                    }
                })
            }
        };
        if let Err(error) = spawned {
            tracing::error!(
                "No thread to release a dropped executor's IPC channels on ({error}); its iceoryx2 services stay open"
            );
        }
    }
}
