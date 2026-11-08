//! Versioned, metadata-only accounting for the copying native protocol.
//! Media remains in iceoryx. The host selects authority for each serialized call.
use super::control_frames::{invalid, read_frame, write_frame};
use remotemedia_types::{
    CopyContext, CopyCounts, CopyEvent, CopyLedger, CopyPolicy, CopyReason, COPY_REASONS,
};
use serde::{Deserialize, Serialize};
use std::{
    io,
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    time::Duration,
};

mod host;
mod state;
mod worker;
pub(super) use host::Host;
pub(super) use worker::Worker;

pub(super) const PROTOCOL_ENV: &str = "RM_NATIVE_COPY_PROTOCOL";
const VERSION: u8 = 1;
const IO_TIMEOUT: Duration = Duration::from_secs(5);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Bootstrap {
    version: u8,
    address: String,
    token: String,
    node: String,
    session: String,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum HostMessage {
    Ack { seq: u64 },
    Start { call: u64, boundary: String },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Report {
    seq: u64,
    call: u64,
    event: Event,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Event {
    Copy { reason: CopyReason, event: WireCopy },
    Finished { error: Option<String> },
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum WireCopy {
    Started,
    Completed { bytes: u64 },
    Failed { bytes: u64 },
    Rejected { bytes: u64 },
}

impl From<CopyEvent> for WireCopy {
    fn from(event: CopyEvent) -> Self {
        match event {
            CopyEvent::Started => Self::Started,
            CopyEvent::Completed(bytes) => Self::Completed { bytes },
            CopyEvent::Failed(bytes) => Self::Failed { bytes },
            CopyEvent::Rejected(bytes) => Self::Rejected { bytes },
        }
    }
}

#[cfg(test)]
mod tests;
