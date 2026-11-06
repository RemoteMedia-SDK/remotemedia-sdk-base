//! Child task ownership survives unwinding of the node's main body.
use crate::{Error, Result};
use futures::FutureExt;
use parking_lot::Mutex;
use std::{future::Future, panic::AssertUnwindSafe, sync::Arc};
use tokio::{sync::watch, task::JoinHandle};

struct Child {
    name: String,
    handle: JoinHandle<()>,
    shutdown: Option<watch::Sender<bool>>,
}
impl Drop for Child {
    fn drop(&mut self) {
        // Also prevent detachment if the supervising future itself is dropped.
        // Joined termination is established by stop(), not by this fallback.
        self.handle.abort();
    }
}

#[derive(Clone, Default)]
pub(super) struct Children(Arc<Mutex<Vec<Child>>>);
impl Children {
    pub(super) fn add(&self, name: impl Into<String>, handle: JoinHandle<()>) {
        self.0.lock().push(Child {
            name: name.into(),
            handle,
            shutdown: None,
        });
    }

    pub(super) fn add_cooperative(
        &self,
        name: impl Into<String>,
        shutdown: watch::Sender<bool>,
        handle: JoinHandle<()>,
    ) {
        self.0.lock().push(Child {
            name: name.into(),
            handle,
            shutdown: Some(shutdown),
        });
    }

    /// Call only after this node body has stopped creating child tasks.
    pub(super) async fn stop(&self) -> Vec<String> {
        let pending = std::mem::take(&mut *self.0.lock());
        for child in &pending {
            if let Some(shutdown) = &child.shutdown {
                // A lane with its own children must join them before finishing.
                shutdown.send_replace(true);
            } else {
                child.handle.abort();
            }
        }
        let mut failures = Vec::new();
        for mut child in pending {
            if let Err(error) = (&mut child.handle).await {
                if !error.is_cancelled() {
                    failures.push(format!("{}: {error}", child.name));
                }
            }
        }
        failures
    }
}

/// Never resume a panicked node body. First settle its children, then report the
/// panic as a task failure to the pipeline's existing active observer.
pub(super) async fn supervise(
    children: Children,
    body: impl Future<Output = Result<()>>,
) -> Result<()> {
    let outcome = AssertUnwindSafe(body).catch_unwind().await;
    let mut failures = children.stop().await;
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(error)) => failures.push(error.to_string()),
        Err(payload) => {
            let message = payload
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic payload");
            failures.push(format!("node task panicked: {message}"));
        }
    }
    if failures.is_empty() {
        return Ok(());
    }
    Err(Error::Execution(failures.join("; ")))
}
