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
//! - Crash of the plugin process surfaces as an iceoryx2 receive
//!   error on the next call but is otherwise unrecovered. A real
//!   integration would respawn / mark the node failed.
//! - One outstanding request per node — the IPC thread handles them
//!   serially. Concurrent calls queue on the mpsc command channel.

use crate::data::RuntimeData;
use crate::nodes::{AsyncNodeWrapper, AsyncStreamingNode, StreamingNode, StreamingNodeFactory};
use crate::Error;
use async_trait::async_trait;
use iceoryx2::prelude::*;
use serde_json::Value;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

const MAX_SLICE_LEN: usize = 1024 * 1024;

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
    cmd_tx: mpsc::Sender<IpcCommand>,
    /// Held so the child is killed on drop.
    child: Arc<Mutex<Child>>,
}

enum IpcCommand {
    Round {
        req_bytes: Vec<u8>,
        reply: oneshot::Sender<Result<Vec<u8>, String>>,
    },
}

impl MultiprocessNativeNode {
    fn spawn(
        binary_path: &PathBuf,
        node_type: &str,
        node_id: &str,
        session_id: &str,
    ) -> Result<Self, Error> {
        let in_channel = format!("{session_id}_{node_id}_input");
        let out_channel = format!("{session_id}_{node_id}_output");

        let child = Command::new(binary_path)
            .env("RM_SESSION_ID", session_id)
            .env("RM_NODE_ID", node_id)
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|e| {
                Error::Execution(format!("spawn plugin {}: {e}", binary_path.display()))
            })?;

        // Plugin needs a moment to attach to iceoryx2 services. A
        // production implementation would wait on a control channel
        // READY signal instead of sleeping.
        std::thread::sleep(Duration::from_millis(500));

        let cmd_tx = spawn_ipc_thread(in_channel, out_channel)?;

        Ok(Self {
            node_type: node_type.to_string(),
            cmd_tx,
            child: Arc::new(Mutex::new(child)),
        })
    }
}

impl Drop for MultiprocessNativeNode {
    fn drop(&mut self) {
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

    async fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        // Serialize native RuntimeData to bytes via msgpack
        let req_bytes = super::data_transfer::to_bytes(&data).map_err(Error::Execution)?;

        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(IpcCommand::Round {
                req_bytes,
                reply: reply_tx,
            })
            .await
            .map_err(|e| Error::IpcError(format!("ipc cmd send: {e}")))?;

        let resp_bytes = reply_rx
            .await
            .map_err(|e| Error::IpcError(format!("ipc reply recv: {e}")))?
            .map_err(Error::IpcError)?;

        // Deserialize response bytes back to native RuntimeData via msgpack
        super::data_transfer::from_bytes(&resp_bytes)
            .map_err(|e| Error::IpcError(format!("decode response: {e}")))
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
) -> Result<mpsc::Sender<IpcCommand>, Error> {
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<IpcCommand>(64);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();

    std::thread::Builder::new()
        .name(format!("rm-mp-ipc-{in_channel}"))
        .spawn(move || {
            // All iceoryx2 setup happens on this thread because
            // Publisher and Subscriber are !Send.
            let setup = || -> Result<_, String> {
                let node = NodeBuilder::new()
                    .create::<ipc::Service>()
                    .map_err(|e| format!("node create: {e:?}"))?;

                let in_service = node
                    .service_builder(
                        &ServiceName::new(&in_channel)
                            .map_err(|e| format!("input service name: {e:?}"))?,
                    )
                    .publish_subscribe::<[u8]>()
                    .max_publishers(2)
                    .max_subscribers(2)
                    .subscriber_max_buffer_size(64)
                    .open_or_create()
                    .map_err(|e| format!("input service: {e:?}"))?;

                let out_service = node
                    .service_builder(
                        &ServiceName::new(&out_channel)
                            .map_err(|e| format!("output service name: {e:?}"))?,
                    )
                    .publish_subscribe::<[u8]>()
                    .max_publishers(2)
                    .max_subscribers(2)
                    .subscriber_max_buffer_size(64)
                    .open_or_create()
                    .map_err(|e| format!("output service: {e:?}"))?;

                let publisher = in_service
                    .publisher_builder()
                    .initial_max_slice_len(MAX_SLICE_LEN)
                    .create()
                    .map_err(|e| format!("publisher create: {e:?}"))?;
                let subscriber = out_service
                    .subscriber_builder()
                    .create()
                    .map_err(|e| format!("subscriber create: {e:?}"))?;
                Ok((publisher, subscriber))
            };

            let (publisher, subscriber) = match setup() {
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
                    IpcCommand::Round { req_bytes, reply } => {
                        let result = round_trip(&publisher, &subscriber, &req_bytes);
                        let _ = reply.send(result);
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

fn round_trip<P, S>(publisher: &P, subscriber: &S, req: &[u8]) -> Result<Vec<u8>, String>
where
    P: PublishApi,
    S: ReceiveApi,
{
    publisher.publish_bytes(req)?;
    loop {
        if let Some(payload) = subscriber.receive_bytes()? {
            return Ok(payload);
        }
        std::thread::yield_now();
    }
}

// Wrappers so the round_trip helper isn't tied to a specific iceoryx2
// generic signature in the function's where-clause (keeps the trait
// surface tiny and the borrows explicit).
trait PublishApi {
    fn publish_bytes(&self, bytes: &[u8]) -> Result<(), String>;
}
trait ReceiveApi {
    fn receive_bytes(&self) -> Result<Option<Vec<u8>>, String>;
}

impl PublishApi for iceoryx2::port::publisher::Publisher<ipc::Service, [u8], ()> {
    fn publish_bytes(&self, bytes: &[u8]) -> Result<(), String> {
        let sample = self
            .loan_slice_uninit(bytes.len())
            .map_err(|e| format!("loan: {e:?}"))?;
        let sample = sample.write_from_slice(bytes);
        sample.send().map_err(|e| format!("send: {e:?}"))?;
        Ok(())
    }
}

impl ReceiveApi for iceoryx2::port::subscriber::Subscriber<ipc::Service, [u8], ()> {
    fn receive_bytes(&self) -> Result<Option<Vec<u8>>, String> {
        match self.receive().map_err(|e| format!("receive: {e:?}"))? {
            Some(sample) => Ok(Some(sample.payload().to_vec())),
            None => Ok(None),
        }
    }
}

// Native RuntimeData flows directly between host and plugin via
// msgpack serialization — no conversion layer needed.
