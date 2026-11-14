//! Bounded, metadata-only copy reporting to one retained host authority.
//! The private capability is delivered over the child's stdin, never argv/logs.
//!
//! - **Version 1** (any policy other than `allow_copies`): every copy is
//!   authorized by an acknowledged begin request and closed by a complete or
//!   fail request, so a strict policy rejects before the copy happens.
//! - **Version 2** (`allow_copies`): the worker authorizes locally and keeps
//!   per-reason totals. It reports them in the end-of-input frame it already
//!   sends after each input ([`replay_end_of_input`]) and, for copies after
//!   the last input, in its close request. The connection carries only the
//!   handshake and the close, and its loss marks the worker as failed. A copy
//!   that was unreported when the worker died is recorded as one failed
//!   operation: its reason and size are unknown to the host.
use crate::multiprocess::control_frames::MAX_FRAME;
use crate::multiprocess::control_frames::{invalid, read_frame, write_frame};
use remotemedia_types::{CopyContext, CopyEvent, CopyLedger, CopyOperation, CopyPolicy, CopyReason};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::io::Write;
use std::{
    io,
    net::{Shutdown, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Bootstrap {
    version: u8,
    address: String,
    token: String,
    node: String,
    session: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Hello {
    version: u8,
    token: String,
    node: String,
    session: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    seq: u64,
    action: Action,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    Begin { reason: CopyReason, bytes: u64 },
    Complete { bytes: u64 },
    Fail {},
    Close {
        /// Version 2 only: totals not yet carried by an end-of-input frame.
        #[serde(default)]
        totals: Option<Totals>,
    },
}

/// Version 2 per-reason totals since the previous report, one entry per reason:
/// `[reason, [completed, copied_bytes, failed, failed_requested_bytes]]`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Totals {
    v: u8,
    reasons: Vec<(CopyReason, [u64; 4])>,
}

/// Bound on operations per reason per report, so a malformed report cannot
/// make the host replay an unbounded number of ledger events.
const MAX_OPERATIONS: u64 = 1 << 20;

/// The reasons a legacy Python worker may report.
const WORKER_REASONS: [CopyReason; 3] = [
    CopyReason::BorrowedInput,
    CopyReason::PrivateIpcIngress,
    CopyReason::PortableSerialization,
];

impl Totals {
    fn validate(&self) -> Result<(), String> {
        if self.v != 2 {
            return Err("unsupported copy report version".into());
        }
        for (index, (reason, [completed, bytes, failed, failed_bytes])) in self.reasons.iter().enumerate() {
            if self.reasons[..index].iter().any(|(seen, _)| seen == reason) {
                return Err(format!("copy reason {reason:?} is reported twice"));
            }
            if !WORKER_REASONS.contains(reason) {
                return Err(format!("copy reason {reason:?} is not reportable by a worker"));
            }
            if *completed > MAX_OPERATIONS || *failed > MAX_OPERATIONS {
                return Err("copy report exceeds its operation bound".into());
            }
            if (*completed == 0 && *bytes != 0) || (*failed == 0 && *failed_bytes != 0) {
                return Err("copy report has bytes without operations".into());
            }
        }
        Ok(())
    }

    /// Replay into the ledger. Ledgers keep per-reason sums, so the bytes of
    /// `n` operations are recorded on the first and zero on the rest.
    fn replay(&self, copies: &CopyContext) {
        for (reason, [completed, bytes, failed, failed_bytes]) in &self.reasons {
            for index in 0..*completed {
                copies.record(*reason, CopyEvent::Started);
                copies.record(*reason, CopyEvent::Completed(if index == 0 { *bytes } else { 0 }));
            }
            for index in 0..*failed {
                copies.record(*reason, CopyEvent::Started);
                copies.record(*reason, CopyEvent::Failed(if index == 0 { *failed_bytes } else { 0 }));
            }
        }
    }
}

/// `WireDataType::EndOfInput`, the first byte of a worker's end-of-input frame.
pub(crate) const END_OF_INPUT: u8 = 8;

/// Replay the copy totals a version 2 worker carries in an end-of-input frame
/// (`type | session_len u16 | session | timestamp u64 | payload_len u32 |
/// payload`, little-endian). An empty payload reports nothing. Call this
/// before the frame completes its turn, so the accounting is applied with the
/// turn it describes. Returns an error, after recording one failed operation,
/// when the frame or its report is malformed.
pub(crate) fn replay_end_of_input(copies: &CopyContext, frame: &[u8]) -> Result<(), String> {
    let result = parse_end_of_input(frame).and_then(|totals| match totals {
        Some(totals) => totals.validate().map(|()| Some(totals)),
        None => Ok(None),
    });
    match result {
        Ok(Some(totals)) => {
            totals.replay(copies);
            Ok(())
        }
        Ok(None) => Ok(()),
        Err(error) => {
            record_unreported(copies);
            Err(error)
        }
    }
}

fn parse_end_of_input(frame: &[u8]) -> Result<Option<Totals>, String> {
    let truncated = || "end-of-input frame is truncated".to_string();
    if frame.first() != Some(&END_OF_INPUT) {
        return Err("not an end-of-input frame".into());
    }
    let session = u16::from_le_bytes(frame.get(1..3).ok_or_else(truncated)?.try_into().unwrap()) as usize;
    let at = 3 + session + 8;
    let size = u32::from_le_bytes(frame.get(at..at + 4).ok_or_else(truncated)?.try_into().unwrap()) as usize;
    let payload = frame.get(at + 4..).ok_or_else(truncated)?;
    if payload.len() != size {
        return Err("end-of-input payload length mismatch".into());
    }
    if payload.is_empty() {
        return Ok(None);
    }
    if payload.len() > MAX_FRAME {
        return Err("copy report exceeds its frame bound".into());
    }
    serde_json::from_slice(payload)
        .map(Some)
        .map_err(|error| format!("invalid copy report: {error}"))
}

/// A version 2 worker's copies that never reached the host: one failed
/// operation, because their reason and size are unknown.
fn record_unreported(copies: &CopyContext) {
    copies.record(CopyReason::BorrowedInput, CopyEvent::Started);
    copies.record(CopyReason::BorrowedInput, CopyEvent::Failed(0));
}

#[derive(Serialize)]
struct Reply {
    seq: u64,
    ok: bool,
}

#[derive(Default)]
struct State {
    stopping: AtomicBool,
    failed: AtomicBool,
    stream: Mutex<Option<TcpStream>>,
}

struct JoinState {
    task: Option<JoinHandle<io::Result<()>>>,
    result: Option<Result<(), String>>,
}

pub(crate) struct CopyAuthority {
    bootstrap: Bootstrap,
    state: Arc<State>,
    joined: Mutex<JoinState>,
}

impl CopyAuthority {
    pub(crate) fn start(copies: CopyContext, node: &str, session: &str) -> io::Result<Self> {
        // Version 2 needs no per-copy authorization, so only allow_copies uses it.
        let version = if copies.policy() == CopyPolicy::AllowCopies { 2 } else { 1 };
        Self::start_with_version(copies, node, session, version)
    }

    pub(crate) fn start_with_version(
        copies: CopyContext,
        node: &str,
        session: &str,
        version: u8,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
        listener.set_nonblocking(true)?;
        let bootstrap = Bootstrap {
            version,
            address: listener.local_addr()?.to_string(),
            token: uuid::Uuid::new_v4().to_string(),
            node: node.to_owned(),
            session: session.to_owned(),
        };
        let state = Arc::new(State::default());
        let worker_state = state.clone();
        let expected = bootstrap.clone();
        let task = thread::Builder::new()
            .name("python-copy-authority".into())
            .spawn(move || {
                let result = accept_worker(listener, &expected, &worker_state, copies);
                worker_state
                    .failed
                    .store(result.is_err(), Ordering::Release);
                worker_state.stream.lock().unwrap().take();
                result
            })?;
        Ok(Self {
            bootstrap,
            state,
            joined: Mutex::new(JoinState {
                task: Some(task),
                result: None,
            }),
        })
    }

    pub(crate) fn bootstrap(&self) -> &Bootstrap {
        &self.bootstrap
    }

    pub(crate) fn failed(&self) -> bool {
        self.state.failed.load(Ordering::Acquire)
    }

    /// After confirmed worker exit, briefly drain buffered terminal reports.
    /// A missing close, disconnect or forced stop is an accounting failure.
    pub(crate) fn finish(&self) -> io::Result<()> {
        self.finish_within(Duration::from_millis(250))
    }

    /// [`finish`](Self::finish) for a worker that is still stopping: wait up
    /// to `grace` for its terminal report before treating it as missing.
    pub(crate) fn finish_within(&self, grace: Duration) -> io::Result<()> {
        let mut joined = self.joined.lock().unwrap();
        if joined.result.is_none() {
            let task = joined.task.take().unwrap();
            let deadline = Instant::now() + grace;
            while !task.is_finished() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(2));
            }
            self.stop();
            let result = task
                .join()
                .map_err(|_| invalid("copy reporter panicked"))
                .and_then(|result| result)
                .map_err(|error| error.to_string());
            joined.result = Some(result);
        }
        joined.result.clone().unwrap().map_err(io::Error::other)
    }

    fn stop(&self) {
        self.state.stopping.store(true, Ordering::Release);
        if let Some(stream) = self.state.stream.lock().unwrap().as_ref() {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }
}

impl Drop for CopyAuthority {
    fn drop(&mut self) {
        self.stop();
        if let Some(task) = self.joined.get_mut().unwrap().task.take() {
            let _ = task.join();
        }
    }
}

fn accept_worker(
    listener: TcpListener,
    expected: &Bootstrap,
    state: &State,
    copies: CopyContext,
) -> io::Result<()> {
    while !state.stopping.load(Ordering::Acquire) {
        let (mut stream, _) = match listener.accept() {
            Ok(value) => value,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(2));
                continue;
            }
            Err(error) => return Err(error),
        };
        *state.stream.lock().unwrap() = Some(stream.try_clone()?);
        if state.stopping.load(Ordering::Acquire) {
            break;
        }
        // Windows can inherit the listener's nonblocking mode. Framed reads
        // require a blocking stream; cancellation uses the retained shutdown handle.
        stream.set_nonblocking(false)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        let authenticated = read_frame::<Hello>(&mut stream).is_ok_and(|hello| {
            hello.version == expected.version
                && hello.token == expected.token
                && hello.node == expected.node
                && hello.session == expected.session
        });
        if !authenticated {
            state.stream.lock().unwrap().take();
            continue;
        }
        // One authenticated connection, with no reconnection or replay window.
        drop(listener);
        stream.set_read_timeout(None)?;
        write_frame(&mut stream, &Reply { seq: 0, ok: true })?;
        if expected.version == 2 {
            let result = serve_v2(&mut stream, &copies);
            if result.is_err() {
                record_unreported(&copies);
            }
            return result;
        }
        return serve(&mut stream, copies);
    }
    Err(invalid("copy reporter stopped before authenticated close"))
}

/// Version 2: the only request is the final close, carrying the totals no
/// end-of-input frame reported. Anything else ends the lane as failed.
fn serve_v2(stream: &mut TcpStream, copies: &CopyContext) -> io::Result<()> {
    let request: Request = read_frame(stream)?;
    let Action::Close { totals } = request.action else {
        return Err(invalid("version 2 copy reporting accepts only close"));
    };
    if request.seq != 1 {
        return Err(invalid("copy report sequence mismatch"));
    }
    if let Some(totals) = totals {
        totals
            .validate()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        totals.replay(copies);
    }
    write_frame(stream, &Reply { seq: 1, ok: true })
}

fn serve(stream: &mut TcpStream, copies: CopyContext) -> io::Result<()> {
    let mut next = 1u64;
    let mut pending: Option<CopyOperation> = None;
    loop {
        let request: Request = read_frame(stream)?;
        if request.seq != next {
            return Err(invalid("copy report sequence mismatch"));
        }
        next = next
            .checked_add(1)
            .ok_or_else(|| invalid("copy report sequence exhausted"))?;
        let mut ok = true;
        let mut close = false;
        match request.action {
            Action::Begin { reason, bytes } => {
                if pending.is_some()
                    || !matches!(
                        reason,
                        CopyReason::BorrowedInput
                            | CopyReason::PrivateIpcIngress
                            | CopyReason::PortableSerialization
                    )
                {
                    return Err(invalid("invalid copy begin"));
                }
                match copies.begin(reason, bytes) {
                    Ok(operation) => pending = Some(operation),
                    Err(_) => ok = false,
                }
            }
            Action::Complete { bytes } => {
                pending
                    .take()
                    .ok_or_else(|| invalid("copy completion without begin"))?
                    .complete_bytes(bytes)
                    .map_err(|_| invalid("copy completion exceeds authorization"))?;
            }
            Action::Fail {} => {
                drop(
                    pending
                        .take()
                        .ok_or_else(|| invalid("copy failure without begin"))?,
                );
            }
            Action::Close { totals } => {
                if totals.is_some() {
                    return Err(invalid("version 1 close carries no totals"));
                }
                if pending.is_some() {
                    return Err(invalid("copy close with unfinished operation"));
                }
                close = true;
            }
        }
        // Terminal accounting occurs before acknowledgement. Any error drops an
        // outstanding CopyOperation as failed; it is never credited as zero.
        write_frame(
            stream,
            &Reply {
                seq: request.seq,
                ok,
            },
        )?;
        if close {
            return Ok(());
        }
    }
}

#[cfg(test)]
#[path = "copy_authority_tests.rs"]
mod tests;
