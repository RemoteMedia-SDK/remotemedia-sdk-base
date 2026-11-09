use super::*;
use std::{
    io::Write,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
    time::Instant,
};

struct Shared {
    failure: Mutex<Option<String>>,
    /// Fast-path flag for `failure`, polled on every spin of a round.
    failed: AtomicBool,
    doorbell: Mutex<Option<TcpStream>>,
    stopping: AtomicBool,
}
impl Shared {
    fn fail(&self, error: impl ToString) {
        let mut failure = self.failure.lock().unwrap();
        if failure.is_none() {
            *failure = Some(error.to_string());
        }
        self.failed.store(true, Ordering::Release);
    }
}

pub(in crate::multiprocess) struct Host {
    bootstrap: Bootstrap,
    shared: Arc<Shared>,
    last_call: AtomicU64,
    ready: Mutex<Option<mpsc::Receiver<Result<(), String>>>>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl Host {
    pub(in crate::multiprocess) fn start(node: &str, session: &str) -> io::Result<Arc<Self>> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))?;
        listener.set_nonblocking(true)?;
        let bootstrap = Bootstrap {
            version: VERSION,
            address: listener.local_addr()?.to_string(),
            token: uuid::Uuid::new_v4().to_string(),
            node: node.into(),
            session: session.into(),
        };
        let shared = Arc::new(Shared {
            failure: Mutex::new(None),
            failed: AtomicBool::new(false),
            doorbell: Mutex::new(None),
            stopping: AtomicBool::new(false),
        });
        let (ready_tx, ready_rx) = mpsc::channel();
        let state = shared.clone();
        let expected = bootstrap.clone();
        let thread = thread::Builder::new()
            .name("native-copy-watch".into())
            .spawn(move || {
                let error = serve(listener, &expected, &state, &ready_tx);
                state.fail(&error);
                let _ = ready_tx.send(Err(error.to_string()));
                if let Some(stream) = state.doorbell.lock().unwrap().take() {
                    let _ = stream.shutdown(Shutdown::Both);
                }
            })?;
        Ok(Arc::new(Self {
            bootstrap,
            shared,
            last_call: AtomicU64::new(0),
            ready: Mutex::new(Some(ready_rx)),
            thread: Mutex::new(Some(thread)),
        }))
    }

    pub(in crate::multiprocess) fn bootstrap(&self, writer: &mut impl io::Write) -> io::Result<()> {
        write_frame(writer, &self.bootstrap)
    }

    pub(in crate::multiprocess) fn wait_ready(
        &self,
        mut alive: impl FnMut() -> io::Result<bool>,
    ) -> io::Result<()> {
        let ready = self
            .ready
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| invalid("native reporter readiness already consumed"))?;
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            match ready.recv_timeout(Duration::from_millis(20)) {
                Ok(result) => return result.map_err(io::Error::other),
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(invalid("native copy reporter stopped before readiness"))
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
            }
            if !alive()? || Instant::now() >= deadline {
                return Err(not_negotiated());
            }
        }
    }

    /// Fails once the worker has disconnected, broken the protocol, or the
    /// host closed the lane.
    pub(in crate::multiprocess) fn check(&self) -> io::Result<()> {
        if !self.shared.failed.load(Ordering::Acquire) {
            return Ok(());
        }
        let failure = self.shared.failure.lock().unwrap();
        Err(io::Error::other(
            failure.clone().unwrap_or_else(|| "native copy reporter failed".into()),
        ))
    }

    /// Grant the next call id. Calls are serial per lane, and any failed round
    /// closes the lane, so ids reach the worker without gaps.
    pub(in crate::multiprocess) fn next_call(
        &self,
        copies: &CopyContext,
    ) -> io::Result<u64> {
        self.check()?;
        if copies.policy() != CopyPolicy::AllowCopies {
            return Err(invalid(
                "native compatibility reporting requires allow_copies",
            ));
        }
        let call = self.last_call.load(Ordering::Acquire);
        let call = call
            .checked_add(1)
            .ok_or_else(|| invalid("native copy call ids exhausted"))?;
        self.last_call.store(call, Ordering::Release);
        Ok(call)
    }

    /// Wake the worker if it is blocked waiting for input. Written after the
    /// input sample is published, so a worker that drains its doorbell before
    /// a final poll and then blocks can never miss a published call.
    pub(in crate::multiprocess) fn ring(&self) -> io::Result<()> {
        let mut doorbell = self.shared.doorbell.lock().unwrap();
        let stream = doorbell
            .as_mut()
            .ok_or_else(|| invalid("native copy reporter is not connected"))?;
        stream.write_all(&[1]).inspect_err(|error| self.shared.fail(error))
    }

    pub(in crate::multiprocess) fn close(&self) {
        self.shared.stopping.store(true, Ordering::Release);
        self.shared.fail("native copy reporter closed");
        if let Some(stream) = self.shared.doorbell.lock().unwrap().as_ref() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        if let Some(thread) = self.thread.lock().unwrap().take() {
            let _ = thread.join();
        }
    }
}
impl Drop for Host {
    fn drop(&mut self) {
        self.close();
    }
}

fn authenticate(
    listener: TcpListener,
    expected: &Bootstrap,
    shared: &Shared,
) -> io::Result<TcpStream> {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    while !shared.stopping.load(Ordering::Acquire) && Instant::now() < deadline {
        let (mut stream, _) = match listener.accept() {
            Ok(value) => value,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(2));
                continue;
            }
            Err(error) => return Err(error),
        };
        stream.set_nonblocking(false)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        stream.set_write_timeout(Some(IO_TIMEOUT))?;
        if !read_frame::<Bootstrap>(&mut stream).is_ok_and(|hello| hello == *expected) {
            continue;
        }
        stream.set_read_timeout(Some(Duration::from_millis(100)))?;
        *shared.doorbell.lock().unwrap() = Some(stream.try_clone()?);
        // close() may race the authenticated connection becoming visible.
        if shared.stopping.load(Ordering::Acquire) {
            let _ = stream.shutdown(Shutdown::Both);
            return Err(invalid("native copy reporter closed during handshake"));
        }
        return Ok(stream);
    }
    Err(not_negotiated())
}

fn not_negotiated() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("native worker did not negotiate copy reporting v{VERSION}"),
    )
}

/// Handshake, then watch the connection until the worker exits. In v2 the
/// worker never writes after the handshake, so any byte is a protocol error.
/// Returns the error that ends the lane.
fn serve(
    listener: TcpListener,
    expected: &Bootstrap,
    shared: &Shared,
    ready: &mpsc::Sender<Result<(), String>>,
) -> io::Error {
    let mut stream = match authenticate(listener, expected, shared) {
        Ok(stream) => stream,
        Err(error) => return error,
    };
    {
        let mut doorbell = shared.doorbell.lock().unwrap();
        if let Err(error) = doorbell
            .as_mut()
            .map_or(Err(invalid("native copy reporter closed")), |stream| {
                write_frame(stream, &HostMessage::Ack { seq: 0 })
            })
        {
            return error;
        }
    }
    let _ = ready.send(Ok(()));
    let mut byte = [0u8; 1];
    match io::Read::read(
        &mut StoppableRead {
            stream: &mut stream,
            stopping: &shared.stopping,
        },
        &mut byte,
    ) {
        Ok(0) => invalid("native worker disconnected"),
        Ok(_) => invalid("native worker wrote to the v2 control connection"),
        Err(error) => error,
    }
}

/// A cloned Windows socket's shutdown need not wake its pending read while the
/// peer stays open. Retry idle timeouts inside Read and check cancellation
/// before each retry so close can always join the watcher.
struct StoppableRead<'a> {
    stream: &'a mut TcpStream,
    stopping: &'a AtomicBool,
}
impl io::Read for StoppableRead<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.stopping.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "native copy reader stopped",
                ));
            }
            match io::Read::read(self.stream, buffer) {
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    continue
                }
                result => return result,
            }
        }
    }
}
