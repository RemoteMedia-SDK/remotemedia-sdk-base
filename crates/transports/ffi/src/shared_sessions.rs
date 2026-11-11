//! Shared streaming sessions across FFI handles (openspec:
//! add-multi-participant-sessions, multi-participant-ingress).
//!
//! `create_shared_streaming_session` (Python and Node.js) must return handles
//! to the *same* pipeline session for the same key within a process. The core
//! executor already shares sessions by key; this module keeps one executor per
//! executor configuration for the whole process (previously every call built
//! its own executor, so nothing was ever shared), and counts the handles
//! attached to each shared session so the last one to close shuts it down.
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use remotemedia_core::transport::{data::participant, ExecutorConfig, PipelineExecutor, SharedPipelineSession};

/// The process-wide executor for `config`. Calls with an equal configuration
/// share one executor, and with it their shared sessions.
pub(crate) fn shared_executor(config: ExecutorConfig) -> remotemedia_core::Result<Arc<PipelineExecutor>> {
    static EXECUTORS: OnceLock<Mutex<HashMap<String, Arc<PipelineExecutor>>>> = OnceLock::new();
    let key = format!("{config:?}");
    let mut executors = EXECUTORS.get_or_init(Default::default).lock().unwrap();
    if let Some(executor) = executors.get(&key) {
        return Ok(Arc::clone(executor));
    }
    let executor = Arc::new(PipelineExecutor::with_config(config)?);
    executors.insert(key, Arc::clone(&executor));
    Ok(executor)
}

fn attached() -> &'static Mutex<HashMap<String, usize>> {
    static ATTACHED: OnceLock<Mutex<HashMap<String, usize>>> = OnceLock::new();
    ATTACHED.get_or_init(Default::default)
}

/// One FFI handle's membership of a shared session.
pub(crate) struct SharedMembership {
    session: Arc<SharedPipelineSession>,
}

impl SharedMembership {
    pub(crate) fn join(session: Arc<SharedPipelineSession>) -> Self {
        *attached().lock().unwrap().entry(session.session_id().to_string()).or_default() += 1;
        Self { session }
    }

    /// Detach this handle; the last handle of the session closes it.
    pub(crate) async fn leave(self) {
        let id = self.session.session_id().to_string();
        let last = {
            let mut attached = attached().lock().unwrap();
            let count = attached.entry(id.clone()).or_default();
            *count = count.saturating_sub(1);
            let last = *count == 0;
            if last {
                attached.remove(&id);
            }
            last
        };
        if last {
            if let Err(error) = self.session.close().await {
                tracing::warn!(session_id = %id, "closing shared session: {error}");
            }
        }
    }
}

/// The producing participant of an output, as `(field, value)` pairs without
/// the `participant.` prefix (`id`, `role`, `track_id`, `modality`,
/// `display_name`); `None` for an unattributed output.
pub(crate) fn participant_fields(metadata: &HashMap<String, String>) -> Option<Vec<(&'static str, String)>> {
    metadata.get(participant::ID)?;
    Some(
        [
            ("id", participant::ID),
            ("role", participant::ROLE),
            ("track_id", participant::TRACK_ID),
            ("modality", participant::MODALITY),
            ("display_name", participant::DISPLAY_NAME),
        ]
        .into_iter()
        .filter_map(|(field, key)| metadata.get(key).map(|value| (field, value.clone())))
        .collect(),
    )
}
