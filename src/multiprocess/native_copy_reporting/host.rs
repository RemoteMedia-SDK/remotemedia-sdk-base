use super::state::State;
use super::*;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
    time::Instant,
};

struct Shared {
    state: Mutex<State>,
    writer: Mutex<Option<TcpStream>>,
    stopping: AtomicBool,
}

pub(in crate::multiprocess) struct Host {
    bootstrap: Bootstrap,
    shared: Arc<Shared>,
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
            state: Mutex::new(State::default()),
            writer: Mutex::new(None),
            stopping: AtomicBool::new(false),
        });
        let (ready_tx, ready_rx) = mpsc::channel();
        let state = shared.clone();
        let expected = bootstrap.clone();
        let thread = thread::Builder::new()
            .name("native-copy-reporter".into())
            .spawn(move || {
                let result = serve(listener, &expected, &state, &ready_tx);
                if let Err(error) = result {
                    state.state.lock().unwrap().fail(&error);
                    let _ = ready_tx.send(Err(error.to_string()));
                }
                if let Some(stream) = state.writer.lock().unwrap().take() {
                    let _ = stream.shutdown(Shutdown::Both);
                }
            })?;
        Ok(Arc::new(Self {
            bootstrap,
            shared,
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
                return Err(invalid("native worker did not negotiate copy reporting v1"));
            }
        }
    }

    pub(in crate::multiprocess) fn begin(&self, copies: CopyContext) -> io::Result<u64> {
        let mut state = self.shared.state.lock().unwrap();
        let boundary = copies.boundary().to_string();
        let call = state.begin(copies)?;
        if let Err(error) = send(&self.shared, &HostMessage::Start { call, boundary }) {
            state.fail(&error);
            return Err(error);
        }
        Ok(call)
    }

    pub(in crate::multiprocess) fn status(&self, call: u64) -> io::Result<bool> {
        self.shared.state.lock().unwrap().status(call)
    }

    pub(in crate::multiprocess) fn close(&self) {
        self.shared.stopping.store(true, Ordering::Release);
        self.shared
            .state
            .lock()
            .unwrap()
            .fail("native copy reporter closed");
        if let Some(stream) = self.shared.writer.lock().unwrap().as_ref() {
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

fn send(shared: &Shared, message: &HostMessage) -> io::Result<()> {
    let mut writer = shared.writer.lock().unwrap();
    write_frame(
        writer
            .as_mut()
            .ok_or_else(|| invalid("native copy reporter is not connected"))?,
        message,
    )
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
        *shared.writer.lock().unwrap() = Some(stream.try_clone()?);
        // close() may race the authenticated connection becoming visible.
        if shared.stopping.load(Ordering::Acquire) {
            let _ = stream.shutdown(Shutdown::Both);
            return Err(invalid("native copy reporter closed during handshake"));
        }
        return Ok(stream);
    }
    Err(invalid("native worker did not negotiate copy reporting v1"))
}

fn serve(
    listener: TcpListener,
    expected: &Bootstrap,
    shared: &Shared,
    ready: &mpsc::Sender<Result<(), String>>,
) -> io::Result<()> {
    let mut stream = authenticate(listener, expected, shared)?;
    send(shared, &HostMessage::Ack { seq: 0 })?;
    let _ = ready.send(Ok(()));
    loop {
        let report: Report = read_frame(&mut StoppableRead {
            stream: &mut stream,
            stopping: &shared.stopping,
        })?;
        let seq = report.seq;
        // Keep completion invisible to the media thread until its ACK is sent.
        // This also orders ACK before any Start for a subsequent call.
        let mut state = shared.state.lock().unwrap();
        state.apply(report)?;
        send(shared, &HostMessage::Ack { seq })?;
    }
}

/// A cloned Windows socket's shutdown need not wake its pending read while the
/// peer stays open. Retry idle timeouts inside Read, preserving partial frames,
/// and check cancellation before each retry so close can always join the reader.
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
