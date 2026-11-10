use super::*;
use std::{io::Read, time::Instant};

/// Doorbell bytes accumulate while a busy worker never blocks; drain them at
/// least this often so the host's one-byte writes can never fill the socket.
const DRAIN_EVERY_CALLS: u32 = 1024;

/// Worker end of copy reporting v2: the authenticated control connection and
/// the in-band call sequence. Per-call accounting travels in the samples.
pub(in crate::multiprocess) struct Worker {
    pub(super) stream: TcpStream,
    last_call: u64,
    pub(super) idle_spin: Duration,
    calls_since_drain: u32,
}

impl Worker {
    pub(in crate::multiprocess) fn from_env(node: &str, session: &str) -> io::Result<Option<Self>> {
        match std::env::var(PROTOCOL_ENV) {
            Err(std::env::VarError::NotPresent) => return Ok(None),
            Ok(value) if value == VERSION.to_string() => {}
            _ => return Err(invalid("unsupported native copy reporting version")),
        }
        let bootstrap: Bootstrap = read_frame(&mut io::stdin().lock())?;
        if bootstrap.version != VERSION || bootstrap.node != node || bootstrap.session != session {
            return Err(invalid("native copy bootstrap identity/version mismatch"));
        }
        let mut worker = Self::connect(bootstrap)?;
        if let Some(micros) = std::env::var(IDLE_SPIN_ENV)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
        {
            worker.idle_spin = Duration::from_micros(micros);
        }
        Ok(Some(worker))
    }

    pub(super) fn connect(bootstrap: Bootstrap) -> io::Result<Self> {
        let address: std::net::SocketAddr = bootstrap
            .address
            .parse()
            .map_err(|_| invalid("invalid native copy address"))?;
        if !address.ip().is_loopback() {
            return Err(invalid("native copy address must be loopback"));
        }
        let mut stream = TcpStream::connect_timeout(&address, IO_TIMEOUT)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(IO_TIMEOUT))?;
        stream.set_write_timeout(Some(IO_TIMEOUT))?;
        write_frame(&mut stream, &bootstrap)?;
        if !matches!(
            read_frame::<HostMessage>(&mut stream)?,
            HostMessage::Ack { seq: 0 }
        ) {
            return Err(invalid("native copy handshake rejected"));
        }
        stream.set_read_timeout(None)?;
        Ok(Self {
            stream,
            last_call: 0,
            idle_spin: DEFAULT_IDLE_SPIN,
            calls_since_drain: 0,
        })
    }

    /// Wait for the next input. Polls without blocking for the idle-spin window,
    /// then drains pending doorbells, polls once more and blocks until the host
    /// rings or disconnects. The host rings only after publishing, so the
    /// drain → poll → block order cannot miss a call. Returns an error when the
    /// host has closed the lane.
    pub(in crate::multiprocess) fn wait<T>(
        &mut self,
        mut poll: impl FnMut() -> Result<Option<T>, String>,
    ) -> Result<T, String> {
        loop {
            let deadline = Instant::now() + self.idle_spin;
            loop {
                if let Some(value) = poll()? {
                    return self.received(value);
                }
                if Instant::now() >= deadline {
                    break;
                }
                std::thread::yield_now();
            }
            self.drain().map_err(|error| error.to_string())?;
            if let Some(value) = poll()? {
                return self.received(value);
            }
            let mut byte = [0u8; 64];
            match self.stream.read(&mut byte) {
                Ok(0) => return Err("native host closed the control connection".into()),
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(format!("native control connection: {error}")),
            }
        }
    }

    fn received<T>(&mut self, value: T) -> Result<T, String> {
        self.calls_since_drain += 1;
        if self.calls_since_drain >= DRAIN_EVERY_CALLS {
            self.drain().map_err(|error| error.to_string())?;
        }
        Ok(value)
    }

    /// Discard pending doorbells without blocking; fails if the host closed.
    fn drain(&mut self) -> io::Result<()> {
        self.calls_since_drain = 0;
        self.stream.set_nonblocking(true)?;
        let result = (|| {
            let mut buffer = [0u8; 256];
            loop {
                match self.stream.read(&mut buffer) {
                    Ok(0) => {
                        return Err(io::Error::new(
                            io::ErrorKind::ConnectionAborted,
                            "native host closed the control connection",
                        ))
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) => return Err(error),
                }
            }
        })();
        self.stream.set_nonblocking(false)?;
        result
    }

    /// Accept the call id from an input frame; ids must arrive in sequence.
    pub(in crate::multiprocess) fn accept(&mut self, call: u64) -> Result<(), String> {
        if self.last_call.checked_add(1) != Some(call) {
            return Err(format!(
                "native call sequence mismatch: expected {}, received {call}",
                self.last_call.wrapping_add(1)
            ));
        }
        self.last_call = call;
        Ok(())
    }
}
