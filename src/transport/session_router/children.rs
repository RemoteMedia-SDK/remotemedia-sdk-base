//! Child task ownership survives unwinding of the node's main body.
use crate::{Error, Result};
use futures::{stream::FuturesUnordered, task::AtomicWaker, FutureExt, StreamExt};
use parking_lot::Mutex;
use std::{
    future::{poll_fn, Future},
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::{sync::watch, task::JoinHandle};
mod task;
use task::Task;

struct Child {
    name: String,
    handle: Option<Task>,
    failure: Option<String>,
    shutdown: Shutdown,
}

enum Shutdown {
    Abort,
    Cooperative(watch::Sender<bool>),
    Drain(watch::Sender<bool>),
}
impl Child {
    fn poll_failure(&mut self, cx: &mut Context<'_>) -> bool {
        let Some(handle) = self.handle.as_mut() else {
            return self.failure.is_some();
        };
        let Poll::Ready(result) = Pin::new(handle).poll(cx) else {
            return false;
        };
        // JoinHandle must never be polled again after completion. Keep any
        // failure for stop(), including cancellation while the lane was active.
        self.handle = None;
        self.failure = task::error(result, false).map(|error| format!("{}: {error}", self.name));
        self.failure.is_some()
    }

    async fn join_failure(&mut self, interrupted: &AtomicBool) -> Option<String> {
        if let Some(failure) = self.failure.take() {
            return Some(failure);
        }
        let handle = self.handle.as_mut()?;
        let outcome = handle.await;
        // Graceful producers should complete normally. Their cancellation is
        // expected only after stop() or another child's failure interrupted drain.
        let expected =
            !matches!(self.shutdown, Shutdown::Drain(_)) || interrupted.load(Ordering::Acquire);
        task::error(outcome, expected).map(|error| format!("{}: {error}", self.name))
    }
}
impl Drop for Child {
    fn drop(&mut self) {
        // Also prevent detachment if the supervising future itself is dropped.
        // Joined termination is established by stop(), not by this fallback.
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

#[derive(Default)]
struct ChildState {
    tasks: Mutex<Vec<Child>>,
    observer: AtomicWaker,
}

#[derive(Clone, Default)]
pub(super) struct Children(Arc<ChildState>);
impl Children {
    pub(super) fn add(&self, name: impl Into<String>, handle: JoinHandle<()>) {
        self.insert(name.into(), Task::Plain(handle), Shutdown::Abort);
    }

    pub(super) fn add_fallible(&self, name: impl Into<String>, handle: JoinHandle<Result<()>>) {
        self.insert(name.into(), Task::Fallible(handle), Shutdown::Abort);
    }

    pub(super) fn add_draining(
        &self,
        name: impl Into<String>,
        stop: watch::Sender<bool>,
        handle: JoinHandle<Result<()>>,
    ) {
        self.insert(name.into(), Task::Fallible(handle), Shutdown::Drain(stop));
    }

    pub(super) fn add_cooperative(
        &self,
        name: impl Into<String>,
        shutdown: watch::Sender<bool>,
        handle: JoinHandle<()>,
    ) {
        self.insert(
            name.into(),
            Task::Plain(handle),
            Shutdown::Cooperative(shutdown),
        );
    }

    fn insert(&self, name: String, handle: Task, shutdown: Shutdown) {
        self.0.tasks.lock().push(Child {
            name,
            handle: Some(handle),
            failure: None,
            shutdown,
        });
        self.0.observer.wake();
    }

    async fn wait_for_failure(&self) {
        poll_fn(|cx| {
            // Register before checking the list so a concurrently added child
            // cannot miss waking an already suspended observer.
            self.0.observer.register(cx.waker());
            if self
                .0
                .tasks
                .lock()
                .iter_mut()
                .any(|child| child.poll_failure(cx))
            {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }

    /// Call only after this node body has stopped creating child tasks.
    pub(super) async fn stop(&self) -> Vec<String> {
        self.finish(false).await
    }

    /// Finish an active producer before closing its output channel. Producers
    /// must also observe session shutdown so explicit close can interrupt drain.
    pub(super) async fn drain(&self) -> Vec<String> {
        self.finish(true).await
    }

    async fn finish(&self, drain: bool) -> Vec<String> {
        let pending = std::mem::take(&mut *self.0.tasks.lock());
        for child in &pending {
            let cooperative = match &child.shutdown {
                Shutdown::Cooperative(stop) => Some(stop),
                Shutdown::Drain(stop) if drain => Some(stop),
                _ => None,
            };
            if let Some(shutdown) = cooperative {
                // A lane with its own children must join them before finishing.
                shutdown.send_replace(true);
            } else if let Some(handle) = &child.handle {
                handle.abort();
            }
        }
        // Draining takes ownership out of the active observer. Poll all joins
        // here so a failed lane cannot hide behind another blocked producer.
        let abort_drains: Vec<_> = pending
            .iter()
            .filter_map(|child| {
                matches!(child.shutdown, Shutdown::Drain(_))
                    .then(|| child.handle.as_ref().map(Task::abort_handle))
                    .flatten()
            })
            .collect();
        let interrupted = AtomicBool::new(!drain);
        let mut joining: FuturesUnordered<_> = pending
            .into_iter()
            .map(|mut child| {
                let interrupted = &interrupted;
                async move { child.join_failure(interrupted).await }
            })
            .collect();
        let mut failures = Vec::new();
        while let Some(failure) = joining.next().await {
            if let Some(failure) = failure {
                interrupted.store(true, Ordering::Release);
                for handle in &abort_drains {
                    handle.abort();
                }
                failures.push(failure);
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
    let outcome = tokio::select! {
        outcome = AssertUnwindSafe(body).catch_unwind() => Some(outcome),
        () = children.wait_for_failure() => None,
    };
    // The selected futures (including an interrupted body) have been dropped
    // before stop(): no body can create new children during joined teardown.
    let mut failures = children.stop().await;
    match outcome {
        None | Some(Ok(Ok(()))) => {}
        Some(Ok(Err(error))) => failures.push(error.to_string()),
        Some(Err(payload)) => {
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
