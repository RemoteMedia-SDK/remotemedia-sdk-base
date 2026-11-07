//! Own either child result shape without spawning a wrapper that can detach it.
use crate::Result;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::task::{JoinError, JoinHandle};

pub(super) type Outcome = std::result::Result<Result<()>, JoinError>;

pub(super) enum Task {
    Plain(JoinHandle<()>),
    Fallible(JoinHandle<Result<()>>),
}

impl Task {
    pub(super) fn abort_handle(&self) -> tokio::task::AbortHandle {
        match self {
            Self::Plain(handle) => handle.abort_handle(),
            Self::Fallible(handle) => handle.abort_handle(),
        }
    }

    pub(super) fn abort(&self) {
        match self {
            Self::Plain(handle) => handle.abort(),
            Self::Fallible(handle) => handle.abort(),
        }
    }
}

impl Future for Task {
    type Output = Outcome;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Outcome> {
        match self.get_mut() {
            Self::Plain(handle) => Pin::new(handle)
                .poll(cx)
                .map(|result| result.map(|()| Ok(()))),
            Self::Fallible(handle) => Pin::new(handle).poll(cx),
        }
    }
}

pub(super) fn error(outcome: Outcome, expected_cancellation: bool) -> Option<String> {
    match outcome {
        Ok(result) => result.err().map(|error| error.to_string()),
        Err(error) if expected_cancellation && error.is_cancelled() => None,
        Err(error) => Some(error.to_string()),
    }
}
