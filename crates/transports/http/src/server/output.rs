//! SSE framing uses separate owning chunks; payload bytes are never recopied.
use super::session::{Completion, HttpSession};
use axum::{
    body::Body,
    http::header,
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use std::{collections::VecDeque, convert::Infallible};
use tokio::sync::{broadcast, watch};

#[cfg(test)]
mod tests;

struct Output {
    rx: broadcast::Receiver<Bytes>,
    completion: watch::Receiver<Completion>,
    heartbeat: tokio::time::Interval,
    pending: VecDeque<Bytes>,
    terminal: bool,
}

pub(super) fn response(session: &HttpSession) -> Response {
    let state = Output {
        rx: session.output.subscribe(),
        completion: session.completion.clone(),
        heartbeat: tokio::time::interval(std::time::Duration::from_secs(15)),
        pending: VecDeque::new(),
        terminal: false,
    };
    let stream = futures::stream::unfold(state, |mut state| async move {
        state
            .next()
            .await
            .map(|bytes| (Ok::<_, Infallible>(bytes), state))
    });
    (
        [
            (header::CONTENT_TYPE, "text/event-stream"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        Body::from_stream(stream),
    )
        .into_response()
}

impl Output {
    fn error(&mut self, message: &str) {
        // Error metadata is JSON escaped so arbitrary messages cannot inject SSE fields.
        self.pending.extend([
            Bytes::from_static(b"event: error\ndata: "),
            Bytes::from(serde_json::to_vec(&message).expect("string serialization is infallible")),
            Bytes::from_static(b"\n\n"),
        ]);
        self.terminal = true;
    }

    async fn next(&mut self) -> Option<Bytes> {
        loop {
            if let Some(bytes) = self.pending.pop_front() {
                return Some(bytes);
            }
            if self.terminal {
                return None;
            }
            if self.rx.is_empty() {
                let completion = self.completion.borrow().clone();
                if let Some(result) = completion {
                    match result {
                        Ok(()) => return None,
                        Err(error) => {
                            self.error(&error);
                            continue;
                        }
                    }
                }
            }
            tokio::select! {
                biased;
                event = self.rx.recv() => match event {
                    Ok(wire) => self.pending.extend([
                        Bytes::from_static(b"data: "), wire, Bytes::from_static(b"\n\n"),
                    ]),
                    Err(broadcast::error::RecvError::Lagged(count)) => self.error(&format!("SSE subscriber missed {count} outputs")),
                    Err(broadcast::error::RecvError::Closed) => return None,
                },
                changed = self.completion.changed() => {
                    if changed.is_err() { self.error("HTTP output drainer ended without a completion report"); }
                },
                _ = self.heartbeat.tick() => return Some(Bytes::from_static(b": keepalive\n\n")),
            }
        }
    }
}
