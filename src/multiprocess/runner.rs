//! Plugin-side helper for native Rust binaries acting as a
//! multiprocess node.
//!
//! This is the copying compatibility runner. For direct mapped input/output
//! ownership, enable `native-worker` and use `multiprocess::native_worker::run`
//! with an existing `StreamingNode` implementation.
//!
//! Replaces the hand-rolled iceoryx2 boilerplate every plugin binary
//! would otherwise reinvent. Channel naming and wire format remain compatible.
//! Factory-launched workers negotiate copy reporting v2 (call ids and copy
//! totals carried in the samples); standalone workers retain the legacy READY log.
//!
//! # Example
//!
//! ```no_run
//! use remotemedia_core::multiprocess::runner::Plugin;
//! use remotemedia_types::RuntimeData;
//!
//! fn main() -> Result<(), String> {
//!     Plugin::from_env()?.run(|input| {
//!         // Echo input as uppercase if it's text.
//!         match input {
//!             RuntimeData::Text(s) => {
//!                 Ok(RuntimeData::Text(s.to_uppercase()))
//!             }
//!             other => Ok(other),
//!         }
//!     })
//! }
//! ```
//!
//! The host launches the binary with `RM_SESSION_ID` and `RM_NODE_ID`
//! environment variables; `Plugin::from_env` reads them and binds to
//! the standard `{session}_{node}_input/output` iceoryx2 services.

pub(super) use super::native_wire::publish_runtime_data;
use iceoryx2::prelude::*;
use remotemedia_types::{CopyContext, CopyLedger, CopyPolicy, RuntimeData};

use super::data_transfer::MAX_SLICE_LEN;

/// One-shot plugin runner. Wraps service setup + the receive/process/send
/// loop so plugin authors only write the handler.
pub struct Plugin {
    session_id: String,
    node_id: String,
}

impl Plugin {
    /// Construct from explicit session + node identifiers.
    pub fn new(session_id: impl Into<String>, node_id: impl Into<String>) -> Self {
        Self {
            session_id: session_id.into(),
            node_id: node_id.into(),
        }
    }

    /// Construct by reading `RM_SESSION_ID` and `RM_NODE_ID` from the
    /// environment — the contract the host uses when spawning a
    /// plugin binary.
    pub fn from_env() -> Result<Self, String> {
        let session_id = std::env::var("RM_SESSION_ID")
            .map_err(|_| "RM_SESSION_ID env var not set".to_string())?;
        let node_id =
            std::env::var("RM_NODE_ID").map_err(|_| "RM_NODE_ID env var not set".to_string())?;
        Ok(Self::new(session_id, node_id))
    }

    /// Standard channel names this plugin will bind to.
    pub fn input_channel(&self) -> String {
        format!("{}_{}_input", self.session_id, self.node_id)
    }
    pub fn output_channel(&self) -> String {
        format!("{}_{}_output", self.session_id, self.node_id)
    }

    /// Run the IPC loop until the host closes the channels (or an
    /// unrecoverable iceoryx2 error). `handler` is called once per
    /// input message and returns a single response.
    ///
    /// With host reporting, errors are returned to the host in-band and stop the loop.
    /// Standalone errors returned by `handler` are logged and the message is dropped — the loop continues. iceoryx2 errors abort
    /// the loop.
    ///
    /// # Output
    ///
    /// Logs an `[plugin] READY` line to stderr after services are
    /// open — the host can use this as a liveness signal.
    pub fn run<F>(self, mut handler: F) -> Result<(), String>
    where
        F: FnMut(RuntimeData) -> Result<RuntimeData, String>,
    {
        let in_name = self.input_channel();
        let out_name = self.output_channel();
        eprintln!("[plugin] in={in_name}  out={out_name}");

        let (publisher, subscriber) = open_channels(&out_name, &in_name)?;

        if let Some(mut worker) =
            super::native_copy_reporting::Worker::from_env(&self.node_id, &self.session_id)
                .map_err(|error| error.to_string())?
        {
            return run_reported(&publisher, &subscriber, &mut worker, &mut handler);
        }
        eprintln!("[plugin] READY");

        loop {
            match subscriber
                .receive()
                .map_err(|e| format!("subscriber receive: {e:?}"))?
            {
                Some(sample) => {
                    let bytes: &[u8] = sample.payload();
                    let input = match super::data_transfer::from_bytes(bytes) {
                        Ok(d) => d,
                        Err(e) => {
                            eprintln!("[plugin] decode error: {e}");
                            continue;
                        }
                    };

                    let output = match handler(input) {
                        Ok(o) => o,
                        Err(e) => {
                            eprintln!("[plugin] handler error: {e}");
                            continue;
                        }
                    };

                    publish_runtime_data(&publisher, &output, CopyContext::legacy())?;
                }
                None => {
                    // Hot-poll: matches the latency tuning in the
                    // existing Python multiprocess executor.
                    std::thread::yield_now();
                }
            }
        }
    }
}

/// Copy reporting v2 call loop. Each input frame carries the call id and the
/// host's ledger boundary; each output frame returns the call's per-reason copy
/// totals with the output (or the error), so a call needs no control-channel
/// round trip. A handler or framing error is returned to the host in-band and
/// then ends the worker.
fn run_reported<F>(
    publisher: &NativePublisher,
    subscriber: &NativeSubscriber,
    worker: &mut super::native_copy_reporting::Worker,
    handler: &mut F,
) -> Result<(), String>
where
    F: FnMut(RuntimeData) -> Result<RuntimeData, String>,
{
    use super::native_copy_reporting::frame;
    loop {
        let sample = worker.wait(|| {
            subscriber
                .receive()
                .map_err(|error| format!("subscriber receive: {error:?}"))
        })?;
        // Decode while the input loan is alive, then release it. The worker's
        // ledger for this call is named after the host's boundary.
        let (call, copies, decoded) = {
            let input = frame::parse_input(sample.payload())?;
            worker.accept(input.call)?;
            let copies = CopyContext::new(CopyPolicy::AllowCopies, input.boundary);
            let decoded = super::data_transfer::from_bytes_with_copy_context(input.body, &copies);
            (input.call, copies, decoded)
        };
        drop(sample);
        let result = decoded.and_then(|data| {
            let output = handler(data)?;
            super::native_wire::encode_framed(
                publisher,
                &output,
                &copies,
                super::data_transfer::MAX_SLICE_LEN - frame::OUTPUT_PREFIX,
                frame::OUTPUT_PREFIX,
                |prefix| frame::write_output(prefix, call, true, &copies.counts()),
            )
        });
        match result {
            Ok(output) => output.send().map_err(|error| format!("send: {error:?}"))?,
            Err(error) => {
                publish_error(publisher, call, &copies, &error)?;
                return Err(error);
            }
        };
    }
}

/// Return a failed call to the host with the copies it made before failing.
fn publish_error(
    publisher: &NativePublisher,
    call: u64,
    copies: &CopyContext,
    error: &str,
) -> Result<(), String> {
    use super::native_copy_reporting::frame;
    let message: String = error.chars().take(frame::MAX_ERROR_CHARS).collect();
    let mut sample = publisher
        .loan_slice_uninit(frame::OUTPUT_PREFIX + message.len())
        .map_err(|error| format!("loan: {error:?}"))?;
    let (head, tail) = sample.payload_mut().split_at_mut(frame::OUTPUT_PREFIX);
    frame::write_output(head, call, false, &copies.counts())?;
    for (out, byte) in tail.iter_mut().zip(message.as_bytes()) {
        out.write(*byte);
    }
    // SAFETY: the prefix and every message byte were written above.
    unsafe { sample.assume_init() }
        .send()
        .map_err(|error| format!("send: {error:?}"))?;
    Ok(())
}

fn service_name(name: &str) -> Result<ServiceName, String> {
    ServiceName::new(name).map_err(|e| format!("invalid service name '{name}': {e:?}"))
}

/// Native endpoints shared by both sides of the copying compatibility protocol.
pub(super) type NativePublisher = iceoryx2::port::publisher::Publisher<ipc::Service, [u8], ()>;
pub(super) type NativeSubscriber = iceoryx2::port::subscriber::Subscriber<ipc::Service, [u8], ()>;

/// iceoryx2 config for nodes opened by native compatibility workers and their
/// host IPC threads, with dead-node sweeps off.
///
/// By default every iceoryx2 node scans all nodes on the machine and removes
/// the ones it judges dead, when it is created and when it is dropped. On
/// Windows that liveness check emulates F_GETLK by briefly taking an exclusive
/// lock, so processes checking the same node at once can judge a live node dead
/// and iceoryx2 fatal-panics ("the Node is still alive"); every sweep also
/// rescans all stale node directories, slowing startup. The host sweeps once,
/// per process, before its first spawn, instead ([`sweep_dead_nodes_once`]).
/// Returns a copy; the process-wide global config is unchanged.
pub(super) fn node_config_without_dead_node_sweeps() -> iceoryx2::config::Config {
    let mut config = iceoryx2::config::Config::global_config().clone();
    config.global.node.cleanup_dead_nodes_on_creation = false;
    config.global.node.cleanup_dead_nodes_on_destruction = false;
    config
}

/// Host-side replacement for the per-node sweeps: remove dead iceoryx2 nodes
/// once per process, before the first worker spawn. Concurrent first spawns
/// wait for that single sweep. A sweep per spawn would rescan, serially, every
/// node directory that iceoryx2 cannot delete on Windows.
pub(super) fn sweep_dead_nodes_once() {
    static SWEEP: std::sync::Once = std::sync::Once::new();
    SWEEP.call_once(sweep_dead_nodes);
}

fn sweep_dead_nodes() {
    let state = iceoryx2::node::Node::<ipc::Service>::try_cleanup_dead_nodes(
        iceoryx2::config::Config::global_config(),
    );
    if state.cleanups + state.failed_cleanups > 0 {
        tracing::info!(
            "iceoryx2: removed {} dead node(s), {} could not be removed",
            state.cleanups,
            state.failed_cleanups
        );
    }
}

pub(super) fn open_channels(
    publish_name: &str,
    subscribe_name: &str,
) -> Result<(NativePublisher, NativeSubscriber), String> {
    let node = NodeBuilder::new()
        .config(&node_config_without_dead_node_sweeps())
        .create::<ipc::Service>()
        .map_err(|e| format!("iceoryx2 node create: {e:?}"))?;
    let publish_service = node
        .service_builder(&service_name(publish_name)?)
        .publish_subscribe::<[u8]>()
        .max_publishers(2)
        .max_subscribers(2)
        .subscriber_max_buffer_size(64)
        .open_or_create()
        .map_err(|e| format!("publish service: {e:?}"))?;
    let subscribe_service = node
        .service_builder(&service_name(subscribe_name)?)
        .publish_subscribe::<[u8]>()
        .max_publishers(2)
        .max_subscribers(2)
        .subscriber_max_buffer_size(64)
        .open_or_create()
        .map_err(|e| format!("subscribe service: {e:?}"))?;
    let publisher = publish_service
        .publisher_builder()
        .initial_max_slice_len(MAX_SLICE_LEN)
        .create()
        .map_err(|e| format!("publisher create: {e:?}"))?;
    let subscriber = subscribe_service
        .subscriber_builder()
        .create()
        .map_err(|e| format!("subscriber create: {e:?}"))?;
    Ok((publisher, subscriber))
}
