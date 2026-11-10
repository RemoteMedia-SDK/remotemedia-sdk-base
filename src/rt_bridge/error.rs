//! Error types for the RT bridge.

use crate::data::RuntimeData;

/// Result of a `try_push` attempt.
///
/// Returned by [`crate::rt_bridge::RtInputProducer::try_push`] when the bridge's
/// input ring is full — i.e., the RT producer is submitting faster than
/// the worker thread can process. The caller retains ownership of the
/// data and can decide whether to drop it (keep audio latency bounded)
/// or retry in a later callback (at the cost of introducing jitter).
///
/// The bridge itself never blocks the RT thread.
#[derive(Debug)]
pub enum TryPushError {
    /// The input ring is full. The unpushed data is returned so the
    /// caller can drop it, log it, or stash it for the next callback.
    Full(RuntimeData),
}

impl std::fmt::Display for TryPushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryPushError::Full(_) => write!(f, "rt-bridge input ring is full"),
        }
    }
}

impl std::error::Error for TryPushError {}

/// Errors that can occur while spawning the bridge worker thread.
#[derive(Debug)]
pub enum SpawnError {
    /// Preparing the fixed audio layout or its selected allocator failed.
    PreparedAudio(String),
    /// OS refused to spawn the worker thread.
    ThreadSpawn(std::io::Error),
    /// Reserved for a strict real-time setup mode. The current `realtime`
    /// feature is best-effort and logs refusals instead of returning this.
    RealtimeSetup(String),
}

impl std::fmt::Display for SpawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpawnError::PreparedAudio(msg) => write!(f, "failed to prepare audio: {msg}"),
            SpawnError::ThreadSpawn(e) => write!(f, "failed to spawn worker thread: {e}"),
            SpawnError::RealtimeSetup(msg) => {
                write!(f, "failed to apply realtime settings: {msg}")
            }
        }
    }
}

impl std::error::Error for SpawnError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SpawnError::ThreadSpawn(e) => Some(e),
            SpawnError::RealtimeSetup(_) | SpawnError::PreparedAudio(_) => None,
        }
    }
}
