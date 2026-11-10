//! Bounded, acknowledged metadata-only reporting to one retained host authority.
//! The private capability is delivered over the child's stdin, never argv/logs.
#[cfg(test)]
use crate::multiprocess::control_frames::MAX_FRAME;
use crate::multiprocess::control_frames::{invalid, read_frame, write_frame};
use remotemedia_types::{CopyContext, CopyOperation, CopyReason};
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
    Close {},
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
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
        listener.set_nonblocking(true)?;
        let bootstrap = Bootstrap {
            version: 1,
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
        let mut joined = self.joined.lock().unwrap();
        if joined.result.is_none() {
            let task = joined.task.take().unwrap();
            let deadline = Instant::now() + Duration::from_millis(250);
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
            hello.version == 1
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
        return serve(&mut stream, copies);
    }
    Err(invalid("copy reporter stopped before authenticated close"))
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
            Action::Close {} => {
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
