//! Plugin-side helper for native Rust binaries acting as a
//! multiprocess node.
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
use remotemedia_types::RuntimeData;

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

        let node = NodeBuilder::new()
            .create::<ipc::Service>()
            .map_err(|e| format!("iceoryx2 node create: {e:?}"))?;

        let in_service = node
            .service_builder(&service_name(&in_name)?)
            .publish_subscribe::<[u8]>()
            .max_publishers(2)
            .max_subscribers(2)
            .subscriber_max_buffer_size(64)
            .open_or_create()
            .map_err(|e| format!("input service open_or_create: {e:?}"))?;

        let out_service = node
            .service_builder(&service_name(&out_name)?)
            .publish_subscribe::<[u8]>()
            .max_publishers(2)
            .max_subscribers(2)
            .subscriber_max_buffer_size(64)
            .open_or_create()
            .map_err(|e| format!("output service open_or_create: {e:?}"))?;

        let subscriber = in_service
            .subscriber_builder()
            .create()
            .map_err(|e| format!("subscriber create: {e:?}"))?;
        let publisher = out_service
            .publisher_builder()
            .initial_max_slice_len(MAX_SLICE_LEN)
            .create()
            .map_err(|e| format!("publisher create: {e:?}"))?;

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

                    let out_bytes = super::data_transfer::to_bytes(&output)
                        .map_err(|e| format!("msgpack serialize: {e}"))?;
                    let sample = publisher
                        .loan_slice_uninit(out_bytes.len())
                        .map_err(|e| format!("loan_slice_uninit: {e:?}"))?;
                    let sample = sample.write_from_slice(&out_bytes);
                    sample
                        .send()
                        .map_err(|e| format!("publisher send: {e:?}"))?;
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
