//! Versioned, metadata-only accounting for the copying native protocol.
//! Media remains in iceoryx. The host selects authority for each serialized call.
//!
//! Version 2 carries each call's grant and copy totals in-band, in the input and
//! output samples themselves ([`frame`]). The loopback control connection is
//! used only for the authenticated handshake, for detecting a worker or host
//! exit, and as a one-byte doorbell that wakes an idle worker; no per-call
//! request, report or acknowledgement crosses it.
use super::control_frames::{invalid, read_frame, write_frame};
use remotemedia_types::{CopyContext, CopyCounts, CopyPolicy, CopyReason, COPY_REASONS};
use serde::{Deserialize, Serialize};
use std::{
    io,
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    time::Duration,
};

pub(super) mod frame;
mod host;
mod worker;
pub(super) use host::Host;
pub(super) use worker::Worker;

pub(super) const PROTOCOL_ENV: &str = "RM_NATIVE_COPY_PROTOCOL";
/// Microseconds a worker keeps polling for the next input after a call before
/// it blocks on the doorbell. Busy streams never block; idle workers do not spin.
pub(super) const IDLE_SPIN_ENV: &str = "RM_NATIVE_IDLE_SPIN_US";
const DEFAULT_IDLE_SPIN: Duration = Duration::from_micros(1000);
pub(super) const VERSION: u8 = 2;
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
    /// Handshake acceptance; the only framed message after the bootstrap.
    Ack { seq: u64 },
}

#[cfg(test)]
mod tests;
