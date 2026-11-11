//! One legacy Python request/response turn over a node's shared response lane.
//!
//! The legacy worker processes inputs serially and ends each input with an
//! `EndOfInput` wire frame. Every turn shares one response receiver, so a turn
//! that ends before its `EndOfInput` (cancelled future, callback stop, receive
//! timeout) leaves the remainder of that turn queued. The next turn must not
//! treat those frames as its own: it first discards the abandoned turns'
//! responses, through each turn's `EndOfInput`, without decoding their payloads.
//!
//! A worker that tags its frames (openspec: add-node-initiated-output) removes
//! the guesswork: every output names the input it answers, so a turn keeps the
//! frames with its own sequence number and drops the others on sight, and a
//! frame with sequence number 0 is one the node emitted on its own. Emitted
//! frames are forwarded to the node's emitter in the order they arrived,
//! during a turn by the turn itself and between turns by
//! [`read_between_turns`].
use super::multiprocess_executor::IpcResponse;
use crate::data::RuntimeData;
use crate::multiprocess::python_wire::EmitTag;
use crate::nodes::{EmitOrigin, Emitter};
use crate::{Error, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;

/// Where frames a Python node emitted go: the node instance's emitter.
#[derive(Clone)]
pub(super) struct EmitSink {
    pub emitter: Emitter,
    pub copies: remotemedia_types::CopyContext,
    pub node_id: String,
}

impl EmitSink {
    /// Decode and emit one frame. Waits while the emit queue is full, which
    /// holds back the node's later output. A frame that cannot be emitted is
    /// reported and dropped.
    pub(super) async fn deliver(&self, frame: remotemedia_types::SharedBytes, origin: EmitTag) {
        let data = match crate::multiprocess::data_transfer::from_owned_python_wire_with_copy_context(
            frame,
            &self.copies,
        ) {
            Ok(data) => data,
            Err(error) => {
                tracing::error!(node_id = %self.node_id, "emitted frame could not be decoded: {error}");
                return;
            }
        };
        let origin = match origin {
            EmitTag::Default => EmitOrigin::Default,
            EmitTag::Unattributed => EmitOrigin::Unattributed,
            EmitTag::Participant(id) => EmitOrigin::Participant(id),
        };
        if let Err(error) = self.emitter.emit_as(data, origin).await {
            tracing::error!(node_id = %self.node_id, "emitted frame was not delivered: {error}");
        }
    }
}

/// The tagged turn being run: its sequence number and where emitted frames go.
pub(super) struct Turn<'a> {
    pub seq: u64,
    pub emits: Option<&'a EmitSink>,
}

/// Hands a node's response lane back and forth between its turns and the
/// reader that forwards emitted frames between turns. A turn has priority.
#[derive(Default)]
pub(super) struct LaneGate {
    wanted: AtomicUsize,
    wanted_changed: tokio::sync::Notify,
    released: tokio::sync::Notify,
}

impl LaneGate {
    /// Ask for the lane on behalf of a turn; held until the turn is over.
    pub(super) fn want(&self) -> Wanted<'_> {
        self.wanted.fetch_add(1, Ordering::AcqRel);
        self.wanted_changed.notify_waiters();
        Wanted(self)
    }
}

pub(super) struct Wanted<'a>(&'a LaneGate);

impl Drop for Wanted<'_> {
    fn drop(&mut self) {
        self.0.wanted.fetch_sub(1, Ordering::AcqRel);
        self.0.released.notify_waiters();
    }
}

/// Read a tagging worker's response lane while no turn runs: forward the
/// frames the node emitted, drop what an abandoned turn left behind. Ends
/// when the lane closes.
pub(super) async fn read_between_turns(
    lane: Arc<tokio::sync::Mutex<Receiver<IpcResponse>>>,
    gate: Arc<LaneGate>,
    sink: EmitSink,
) {
    loop {
        loop {
            let released = gate.released.notified();
            if gate.wanted.load(Ordering::Acquire) == 0 {
                break;
            }
            released.await;
        }
        let mut responses = lane.lock().await;
        loop {
            let wanted = gate.wanted_changed.notified();
            tokio::pin!(wanted);
            wanted.as_mut().enable();
            if gate.wanted.load(Ordering::Acquire) > 0 {
                break;
            }
            tokio::select! {
                biased;
                _ = &mut wanted => break,
                response = responses.recv() => match response {
                    None => return,
                    Some(IpcResponse::Tagged { seq: 0, origin, frame }) => {
                        sink.deliver(frame, origin).await;
                    }
                    Some(_) => {}
                },
            }
        }
    }
}

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
                Ok(Some(IpcResponse::Tagged { frame, .. })) if is_end_of_input(&frame) => break true,
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
    decode: impl FnMut(remotemedia_types::SharedBytes) -> Result<RuntimeData>,
    callback: F,
) -> Result<usize>
where
    S: std::future::Future<Output = Result<()>>,
    F: FnMut(RuntimeData) -> Result<()> + Send,
{
    run_turn(responses, turns, timeouts, node_id, send, decode, callback, None).await
}

/// One received response, as this turn sees it.
enum Seen {
    /// A response of this turn (or an untagged one), handled as before.
    Mine(IpcResponse),
    /// Emitted by the node, or left behind by another turn: already handled.
    NotMine,
}

/// Sort a response: forward an emitted frame, drop another turn's frame.
async fn sort(response: IpcResponse, turn: Option<&Turn<'_>>) -> Seen {
    match response {
        IpcResponse::Tagged { seq: 0, origin, frame } => {
            if let Some(sink) = turn.and_then(|turn| turn.emits) {
                sink.deliver(frame, origin).await;
            }
            Seen::NotMine
        }
        IpcResponse::Tagged { seq, frame, .. } => match turn {
            Some(turn) if turn.seq != seq => Seen::NotMine,
            _ => Seen::Mine(IpcResponse::OutputData(frame)),
        },
        other => Seen::Mine(other),
    }
}

/// [`run`], for a worker that may tag its frames. With `turn` set, the input
/// was sent with that sequence number: frames of other turns are dropped as
/// they arrive instead of being counted through their `EndOfInput`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_turn<S, F>(
    responses: &mut Receiver<IpcResponse>,
    turns: &AbandonedTurns,
    timeouts: Timeouts,
    node_id: &str,
    send: S,
    mut decode: impl FnMut(remotemedia_types::SharedBytes) -> Result<RuntimeData>,
    mut callback: F,
    turn: Option<Turn<'_>>,
) -> Result<usize>
where
    S: std::future::Future<Output = Result<()>>,
    F: FnMut(RuntimeData) -> Result<()> + Send,
{
    if turn.is_none() {
        drain_abandoned(responses, turns, timeouts.output, node_id).await?;
    }
    send.await?;
    // From here on the worker owns this input and will end it with EndOfInput.
    // A tagged turn that ends early needs no bookkeeping: its frames carry
    // its sequence number.
    let guard = TurnGuard { turns, armed: turn.is_none() };

    loop {
        let response = match tokio::time::timeout(timeouts.send_ack, responses.recv()).await {
            Ok(Some(response)) => match sort(response, turn.as_ref()).await {
                Seen::Mine(response) => Ok(Some(response)),
                Seen::NotMine => continue,
            },
            other => other,
        };
        match response {
            Ok(Some(IpcResponse::SendComplete)) => break,
            Ok(Some(IpcResponse::Error(e))) => {
                // A failed publication never reached the worker.
                guard.finish();
                return Err(Error::Execution(format!("IPC send error: {e}")));
            }
            // Frames acknowledged before this input was published belong to
            // background output, never to this turn.
            Ok(Some(IpcResponse::OutputData(_) | IpcResponse::Tagged { .. })) => continue,
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
        let response = match tokio::time::timeout(timeouts.output, responses.recv()).await {
            Ok(Some(response)) => match sort(response, turn.as_ref()).await {
                Seen::Mine(response) => Ok(Some(response)),
                Seen::NotMine => continue,
            },
            other => other,
        };
        match response {
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
            // `sort` turned every tagged frame into one of the cases above.
            Ok(Some(IpcResponse::Tagged { .. })) => continue,
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
