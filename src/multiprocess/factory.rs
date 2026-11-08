//! `StreamingNodeFactory` that runs a native binary plugin in its
//! own process, communicating via iceoryx2 shared memory.
//!
//! Usage from host code:
//!
//! ```no_run
//! use std::path::PathBuf;
//! use std::sync::Arc;
//! use remotemedia_core::multiprocess::factory::MultiprocessNativeFactory;
//! use remotemedia_core::nodes::StreamingNodeRegistry;
//!
//! let mut registry = StreamingNodeRegistry::new();
//! registry.register(Arc::new(MultiprocessNativeFactory {
//!     binary_path: PathBuf::from("./my-plugin"),
//!     node_type: "MyPluginNode".to_string(),
//! }));
//!
//! // From here on, `MyPluginNode` works like any other registered node.
//! ```
//!
//! # Limitations of this prototype
//!
//! - The plugin is assumed ready ~500 ms after spawn. Production
//!   would handshake on a control channel (READY pattern from the
//!   Python multiprocess executor).
//! - A nonresponding/crashed plugin can leave a call pending until node drop.
//!   Dropping the node cancels the receive loop and kills/reaps its child.
//! - Strict contexts reject this copying protocol. Compatibility host copies
//!   use the caller ledger; worker copies still use the worker legacy ledger.
//! - One outstanding request per node — the IPC thread handles them
//!   serially. Concurrent calls queue on the mpsc command channel.

use crate::data::RuntimeData;
use crate::nodes::{
    AsyncNodeWrapper, AsyncStreamingNode, InitializeContextRead, NodeRuntimeContextRead,
    StreamingNode, StreamingNodeFactory,
};
use crate::Error;
use async_trait::async_trait;
use remotemedia_types::{CopyContext, CopyPolicy};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// Spawns a native binary plugin per `create()`.
///
/// Each created node owns its own child process and IPC channel pair.
pub struct MultiprocessNativeFactory {
    /// Path to the plugin binary. The binary should call
    /// `multiprocess::runner::Plugin::from_env().run(...)`.
    pub binary_path: PathBuf,
    /// Node type as it should appear in pipeline manifests.
    pub node_type: String,
}

impl StreamingNodeFactory for MultiprocessNativeFactory {
    fn node_type(&self) -> &str {
        &self.node_type
    }

    fn create(
        &self,
        node_id: String,
        _params: &Value,
        session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let session_id = session_id.unwrap_or_else(|| "default".to_string());
        let node = MultiprocessNativeNode::spawn(
            &self.binary_path,
            &self.node_type,
            &node_id,
            &session_id,
        )?;
        Ok(Box::new(AsyncNodeWrapper(Arc::new(node))))
    }
}

/// One spawned plugin process + the dedicated IPC thread that owns
/// the !Send iceoryx2 publisher/subscriber.
pub struct MultiprocessNativeNode {
    node_type: String,
    node_id: String,
    session_id: String,
    stopping: Arc<AtomicBool>,
    cmd_tx: mpsc::Sender<IpcCommand>,
    /// Held so the child is killed on drop.
    child: Arc<Mutex<Child>>,
}

enum IpcCommand {
    Round {
        data: RuntimeData,
        copies: CopyContext,
        reply: oneshot::Sender<Result<RuntimeData, String>>,
    },
}

impl MultiprocessNativeNode {
    fn spawn(
        binary_path: &PathBuf,
        node_type: &str,
        node_id: &str,
        session_id: &str,
    ) -> Result<Self, Error> {
        Self::spawn_command(Command::new(binary_path), node_type, node_id, session_id)
    }

    fn spawn_command(
        mut command: Command,
        node_type: &str,
        node_id: &str,
        session_id: &str,
    ) -> Result<Self, Error> {
        let in_channel = format!("{session_id}_{node_id}_input");
        let out_channel = format!("{session_id}_{node_id}_output");

        let mut child = command
            .env("RM_SESSION_ID", session_id)
            .env("RM_NODE_ID", node_id)
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| Error::Execution(format!("spawn native compatibility plugin: {e}")))?;

        // Plugin needs a moment to attach to iceoryx2 services. A
        // production implementation would wait on a control channel
        // READY signal instead of sleeping.
        std::thread::sleep(Duration::from_millis(500));

        let stopping = Arc::new(AtomicBool::new(false));
        let cmd_tx = match spawn_ipc_thread(in_channel, out_channel, stopping.clone()) {
            Ok(sender) => sender,
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error);
            }
        };

        Ok(Self {
            node_type: node_type.to_string(),
            node_id: node_id.to_string(),
            session_id: session_id.to_string(),
            stopping,
            cmd_tx,
            child: Arc::new(Mutex::new(child)),
        })
    }
    fn call_authority(
        &self,
        node: &str,
        session: &str,
        copies: Option<CopyContext>,
    ) -> Result<CopyContext, Error> {
        if node != self.node_id || session != self.session_id {
            return Err(Error::Execution(
                "Native compatibility call identity mismatch".into(),
            ));
        }
        let copies = copies.ok_or_else(|| {
            Error::Execution("Native compatibility calls require copy authority".into())
        })?;
        if copies.policy() != CopyPolicy::AllowCopies {
            return Err(Error::Execution("Native MessagePack compatibility requires allow_copies; use the mapped native-worker protocol for strict calls".into()));
        }
        Ok(copies)
    }

    async fn process_with_copies(
        &self,
        data: RuntimeData,
        copies: CopyContext,
    ) -> Result<RuntimeData, Error> {
        let (reply, response) = oneshot::channel();
        self.cmd_tx
            .send(IpcCommand::Round {
                data,
                copies,
                reply,
            })
            .await
            .map_err(|error| Error::IpcError(format!("ipc cmd send: {error}")))?;
        response
            .await
            .map_err(|error| Error::IpcError(format!("ipc reply recv: {error}")))?
            .map_err(Error::IpcError)
    }
}

impl Drop for MultiprocessNativeNode {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        // Closing the cmd_tx makes the IPC thread's blocking_recv()
        // return None and the thread exits cleanly.
        // Kill the child explicitly — the plugin runs an infinite
        // loop so it won't notice the channel close on its own.
        if let Ok(mut child) = self.child.lock() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[async_trait]
impl AsyncStreamingNode for MultiprocessNativeNode {
    fn node_type(&self) -> &str {
        &self.node_type
    }

    async fn initialize(&self, ctx: &dyn InitializeContextRead) -> Result<(), Error> {
        self.call_authority(ctx.node_id(), ctx.session_id(), ctx.copy_context())?;
        Ok(())
    }

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        self.process_with_copies(data, CopyContext::legacy().clone())
            .await
    }

    async fn process_with_ctx(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        let copies = self.call_authority(ctx.node_id(), ctx.session_id(), ctx.copy_context())?;
        self.process_with_copies(data, copies).await
    }

    async fn process_multi_with_ctx(
        &self,
        inputs: HashMap<String, RuntimeData>,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> Result<RuntimeData, Error> {
        let copies = self.call_authority(ctx.node_id(), ctx.session_id(), ctx.copy_context())?;
        let data = inputs
            .into_values()
            .next()
            .ok_or_else(|| Error::Execution("No input data provided".into()))?;
        self.process_with_copies(data, copies).await
    }

    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let output = self.process_with_ctx(data, ctx).await?;
        callback(output)?;
        Ok(1)
    }
}

/// Spawn the dedicated OS thread that owns the iceoryx2 publisher
/// and subscriber (those types are `!Send`). Returns the mpsc sender
/// used by async callers to enqueue round-trips.
///
/// The thread blocks on `blocking_recv()` between calls and exits
/// when all senders are dropped (i.e. when the owning
/// `MultiprocessNativeNode` is dropped).
fn spawn_ipc_thread(
    in_channel: String,
    out_channel: String,
    stopping: Arc<AtomicBool>,
) -> Result<mpsc::Sender<IpcCommand>, Error> {
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<IpcCommand>(64);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();

    std::thread::Builder::new()
        .name(format!("rm-mp-ipc-{in_channel}"))
        .spawn(move || {
            let (publisher, subscriber) =
                match super::runner::open_channels(&in_channel, &out_channel) {
                    Ok(p) => {
                        let _ = ready_tx.send(Ok(()));
                        p
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                };

            while let Some(cmd) = cmd_rx.blocking_recv() {
                match cmd {
                    IpcCommand::Round {
                        data,
                        copies,
                        reply,
                    } => {
                        if reply.is_closed() {
                            continue;
                        }
                        let result = round_trip(&publisher, &subscriber, &data, &copies, &stopping);
                        let failed = result.is_err();
                        let _ = reply.send(result);
                        // A late response must never be mistaken for the next request.
                        if failed {
                            break;
                        }
                    }
                }
            }
        })
        .map_err(|e| Error::Execution(format!("spawn ipc thread: {e}")))?;

    ready_rx
        .recv()
        .map_err(|e| Error::IpcError(format!("ipc thread ready signal lost: {e}")))?
        .map_err(Error::IpcError)?;

    Ok(cmd_tx)
}

fn round_trip<P: PublishApi, S: ReceiveApi>(
    publisher: &P,
    subscriber: &S,
    data: &RuntimeData,
    copies: &CopyContext,
    stopping: &AtomicBool,
) -> Result<RuntimeData, String> {
    if stopping.load(Ordering::Acquire) {
        return Err("native compatibility IPC stopped".into());
    }
    publisher.publish_data(data, copies)?;
    loop {
        if stopping.load(Ordering::Acquire) {
            return Err("native compatibility IPC stopped".into());
        }
        if let Some(data) = subscriber.receive_data(copies)? {
            return Ok(data);
        }
        std::thread::yield_now();
    }
}

trait PublishApi {
    fn publish_data(&self, data: &RuntimeData, copies: &CopyContext) -> Result<(), String>;
}
trait ReceiveApi {
    fn receive_data(&self, copies: &CopyContext) -> Result<Option<RuntimeData>, String>;
}

impl PublishApi for super::runner::NativePublisher {
    fn publish_data(&self, data: &RuntimeData, copies: &CopyContext) -> Result<(), String> {
        super::runner::publish_runtime_data(self, data, copies)
    }
}

impl ReceiveApi for super::runner::NativeSubscriber {
    fn receive_data(&self, copies: &CopyContext) -> Result<Option<RuntimeData>, String> {
        match self
            .receive()
            .map_err(|error| format!("receive: {error:?}"))?
        {
            // Decode while the native loan is alive, eliminating the private wire snapshot.
            Some(sample) => {
                super::data_transfer::from_bytes_with_copy_context(sample.payload(), copies)
                    .map(Some)
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
#[path = "factory_copy_tests.rs"]
mod copy_tests;
