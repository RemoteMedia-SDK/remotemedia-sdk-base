//! Plugin-side helper for native Rust binaries acting as a
//! multiprocess node.
//!
//! This is the copying compatibility runner. For direct mapped input/output
//! ownership, enable `native-worker` and use `multiprocess::native_worker::run`
//! with an existing `StreamingNode` implementation.
//!
//! Replaces the hand-rolled iceoryx2 boilerplate every plugin binary
//! would otherwise reinvent. Same channel naming, same wire format,
//! same READY signal as the Python multiprocess runner.
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

use iceoryx2::prelude::*;
use remotemedia_types::{CopyContext, CopyReason, RuntimeData};

/// Matches the global iceoryx2 config the channel registry uses.
const MAX_SLICE_LEN: usize = 1024 * 1024;

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
    /// Errors returned by `handler` are logged to stderr and the
    /// message is dropped — the loop continues. iceoryx2 errors abort
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

fn service_name(name: &str) -> Result<ServiceName, String> {
    ServiceName::new(name).map_err(|e| format!("invalid service name '{name}': {e:?}"))
}

/// Native endpoints shared by both sides of the copying compatibility protocol.
pub(super) type NativePublisher = iceoryx2::port::publisher::Publisher<ipc::Service, [u8], ()>;
pub(super) type NativeSubscriber = iceoryx2::port::subscriber::Subscriber<ipc::Service, [u8], ()>;

pub(super) fn open_channels(
    publish_name: &str,
    subscribe_name: &str,
) -> Result<(NativePublisher, NativeSubscriber), String> {
    let node = NodeBuilder::new()
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

/// Serialize once, then account the separate transfer into the native loan.
/// Completion describes copied bytes even if subsequent publication fails.
pub(super) fn publish_runtime_data(
    publisher: &NativePublisher,
    data: &RuntimeData,
    copies: &CopyContext,
) -> Result<(), String> {
    let bytes = super::data_transfer::to_bytes_with_copy_context(data, copies)?;
    let sample = copies
        .materialize_result(CopyReason::PrivateIpcIngress, bytes.len() as u64, || {
            let sample = publisher
                .loan_slice_uninit(bytes.len())
                .map_err(|e| format!("loan: {e:?}"))?;
            Ok::<_, String>(sample.write_from_slice(&bytes))
        })
        .map_err(|error| error.to_string())??;
    sample.send().map_err(|error| format!("send: {error:?}"))?;
    Ok(())
}
