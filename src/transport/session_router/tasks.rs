//! Retain and observe the same task joins during execution and teardown.
use crate::{Error, Result};
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::{sync::watch, task::JoinHandle};

pub(super) struct TaskGroup {
    pending: FuturesUnordered<BoxFuture<'static, (String, Result<()>)>>,
    failures: Vec<String>,
    shutdown: watch::Sender<bool>,
}

impl TaskGroup {
    pub(super) fn shutdown_signal(&self) -> watch::Sender<bool> {
        self.shutdown.clone()
    }

    pub(super) fn new(
        shutdown: watch::Sender<bool>,
        handles: Vec<(String, JoinHandle<Result<()>>)>,
    ) -> Self {
        let pending = handles
            .into_iter()
            .map(|(name, handle)| {
                async move {
                    let result = handle
                        .await
                        .unwrap_or_else(|error| Err(Error::Execution(error.to_string())));
                    (name, result)
                }
                .boxed()
            })
            .collect();
        Self {
            pending,
            failures: Vec::new(),
            shutdown,
        }
    }

    /// Safe to cancel at an ingress select: completed results are recorded
    /// without an intervening await, and remaining joins stay in this group.
    pub(super) async fn wait_for_failure(&mut self) {
        if !self.failures.is_empty() {
            return;
        }
        while let Some((name, result)) = self.pending.next().await {
            if self.record(name, result) {
                return;
            }
        }
        // Normal task completion is not yet the pipeline's finite-source EOS
        // protocol. Do not spin or cancel an ingress packet on a successful join.
        std::future::pending::<()>().await;
    }

    fn record(&mut self, name: String, result: Result<()>) -> bool {
        let Err(error) = result else {
            return false;
        };
        self.shutdown.send_replace(true);
        self.failures.push(format!("{name}: {error}"));
        true
    }

    pub(super) async fn join(mut self, explicit_close: bool) -> Result<()> {
        if explicit_close {
            self.shutdown.send_replace(true);
        }
        while let Some((name, result)) = self.pending.next().await {
            self.record(name, result);
        }
        if self.failures.is_empty() {
            return Ok(());
        }
        self.failures.sort();
        Err(Error::Execution(format!(
            "pipeline teardown failed: {}",
            self.failures.join("; ")
        )))
    }
}
