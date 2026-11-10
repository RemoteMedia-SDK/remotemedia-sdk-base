//! `realtime` feature: apply thread priority and core affinity on the
//! bridge worker thread after spawn.
//!
//! This module is only compiled with `--features realtime`. It is a
//! best-effort helper — if the OS refuses to elevate priority or pin
//! the core (missing capability, restricted cgroup, etc.) we log and
//! continue rather than killing the process. That keeps the bridge
//! usable in unprivileged test environments; production deployments
//! should verify capabilities out-of-band.

use thread_priority::{set_current_thread_priority, ThreadPriority};

pub(crate) fn apply(priority: Option<ThreadPriority>, core_id: Option<core_affinity::CoreId>) {
    if let Some(p) = priority {
        match set_current_thread_priority(p) {
            Ok(()) => tracing::info!(?p, "rt-bridge worker priority applied"),
            Err(e) => tracing::warn!(
                error = ?e,
                "rt-bridge worker could not apply priority; continuing at default",
            ),
        }
    }

    if let Some(c) = core_id {
        if core_affinity::set_for_current(c) {
            tracing::info!(core = ?c, "rt-bridge worker pinned to core");
        } else {
            tracing::warn!(core = ?c, "rt-bridge worker could not be pinned");
        }
    }
}
