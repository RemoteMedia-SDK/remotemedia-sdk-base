//! One legacy Python request/response turn over a node's shared response lane.
//!
//! The legacy worker processes inputs serially and ends each input with an
//! `EndOfInput` wire frame. Every turn shares one response receiver, so a turn
//! that ends before its `EndOfInput` (cancelled future, callback stop, receive
//! timeout) leaves the remainder of that turn queued. The next turn must not
//! treat those frames as its own: it first discards the abandoned turns'
//! responses, through each turn's `EndOfInput`, without decoding their payloads.
use super::multiprocess_executor::IpcResponse;
use crate::data::RuntimeData;
use crate::{Error, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::mpsc::Receiver;

/// `WireDataType::EndOfInput` discriminant: the first wire byte.
const END_OF_INPUT: u8 = 8;

fn is_end_of_input(frame: &[u8]) -> bool {
    frame.first() == Some(&END_OF_INPUT)
}

/// Turns that ended before their `EndOfInput`, shared by every turn on a node.
#[derive(Debug, Default)]
pub(super) struct AbandonedTurns(AtomicUsize);

impl AbandonedTurns {
    #[cfg(test)]
    pub(super) fn pending(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
}

/// Counts the current turn as abandoned unless it reached a terminal response.
struct TurnGuard<'a> {
    turns: &'a AbandonedTurns,
    armed: bool,
}

impl TurnGuard<'_> {
    fn finish(mut self) {
        self.armed = false;
    }
}

impl Drop for TurnGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.turns.0.fetch_add(1, Ordering::AcqRel);
        }
    }
}

pub(super) struct Timeouts {
    pub send_ack: Duration,
    pub output: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            send_ack: Duration::from_secs(5),
            // Upper bound only: the hot path is the per-input EndOfInput.
            output: Duration::from_secs(300),
        }
    }
}

/// Discard queued responses of abandoned turns, oldest first. Payloads are
/// dropped without decoding, so no copy is attributed to the next turn's ledger.
/// A drain that cannot complete fails this turn and resets the count, so a hung
/// or crashed worker reports an error instead of wedging every later turn.
async fn drain_abandoned(
    responses: &mut Receiver<IpcResponse>,
    turns: &AbandonedTurns,
    timeout: Duration,
    node_id: &str,
) -> Result<()> {
    while turns.0.load(Ordering::Acquire) > 0 {
        let terminal = loop {
            match tokio::time::timeout(timeout, responses.recv()).await {
                Ok(Some(IpcResponse::OutputData(frame))) if is_end_of_input(&frame) => break true,
                Ok(Some(IpcResponse::Error(_))) => break true,
                Ok(Some(_)) => continue,
                Ok(None) | Err(_) => break false,
            }
        };
        if !terminal {
            turns.0.store(0, Ordering::Release);
            return Err(Error::Execution(format!(
                "legacy Python node '{node_id}' did not finish an abandoned input; its stale outputs cannot be separated from new input"
            )));
        }
        turns.0.fetch_sub(1, Ordering::AcqRel);
    }
    Ok(())
}

/// Run one turn: discard abandoned responses, submit through `send`, await its
/// acknowledgement, then forward decoded outputs until this input's EndOfInput.
pub(super) async fn run<S, F>(
    responses: &mut Receiver<IpcResponse>,
    turns: &AbandonedTurns,
    timeouts: Timeouts,
    node_id: &str,
    send: S,
    mut decode: impl FnMut(remotemedia_types::SharedBytes) -> Result<RuntimeData>,
    mut callback: F,
) -> Result<usize>
where
    S: std::future::Future<Output = Result<()>>,
    F: FnMut(RuntimeData) -> Result<()> + Send,
{
    drain_abandoned(responses, turns, timeouts.output, node_id).await?;
    send.await?;
    // From here on the worker owns this input and will end it with EndOfInput.
    let guard = TurnGuard { turns, armed: true };

    loop {
        match tokio::time::timeout(timeouts.send_ack, responses.recv()).await {
            Ok(Some(IpcResponse::SendComplete)) => break,
            Ok(Some(IpcResponse::Error(e))) => {
                // A failed publication never reached the worker.
                guard.finish();
                return Err(Error::Execution(format!("IPC send error: {e}")));
            }
            // Frames acknowledged before this input was published belong to
            // background output, never to this turn.
            Ok(Some(IpcResponse::OutputData(_))) => continue,
            Ok(None) => {
                guard.finish();
                return Err(Error::Execution(format!(
                    "IPC thread for node '{node_id}' closed before acknowledging input"
                )));
            }
            Err(_) => {
                return Err(Error::Execution(
                    "Timeout waiting for send confirmation".into(),
                ))
            }
        }
    }

    let mut output_count = 0;
    loop {
        match tokio::time::timeout(timeouts.output, responses.recv()).await {
            Err(_) => {
                tracing::debug!(
                    "[Multiprocess] Output receive timeout for node '{}' after {} outputs",
                    node_id,
                    output_count
                );
                return Ok(output_count);
            }
            Ok(Some(IpcResponse::OutputData(frame))) => {
                if is_end_of_input(&frame) {
                    guard.finish();
                    return Ok(output_count);
                }
                let output = decode(frame)?;
                if let Err(e) = callback(output) {
                    // The callback chose to stop; the rest of this turn is
                    // discarded before the next turn starts.
                    tracing::debug!(
                        "Callback stopped collection for node {} after {} outputs: {}",
                        node_id,
                        output_count,
                        e
                    );
                    return Ok(output_count);
                }
                output_count += 1;
            }
            Ok(Some(IpcResponse::SendComplete)) => continue,
            Ok(Some(IpcResponse::Error(e))) => {
                guard.finish();
                return Err(Error::Execution(format!("IPC error: {e}")));
            }
            Ok(None) => {
                guard.finish();
                return Ok(output_count);
            }
        }
    }
}

#[cfg(test)]
#[path = "legacy_call_tests.rs"]
mod tests;
