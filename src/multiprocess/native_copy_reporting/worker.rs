use super::*;

struct Client {
    stream: TcpStream,
    seq: u64,
    failed: Option<String>,
}
impl Client {
    fn report(&mut self, call: u64, event: Event) -> io::Result<()> {
        self.check()?;
        self.seq = self
            .seq
            .checked_add(1)
            .ok_or_else(|| invalid("native copy sequence exhausted"))?;
        write_frame(
            &mut self.stream,
            &Report {
                seq: self.seq,
                call,
                event,
            },
        )?;
        match read_frame::<HostMessage>(&mut self.stream)? {
            HostMessage::Ack { seq } if seq == self.seq => Ok(()),
            _ => Err(invalid("native copy acknowledgement mismatch")),
        }
    }
    fn check(&self) -> io::Result<()> {
        match &self.failed {
            Some(error) => Err(io::Error::other(error.clone())),
            None => Ok(()),
        }
    }
    fn fail(&mut self, error: impl ToString) {
        self.failed = Some(error.to_string());
        let _ = self.stream.shutdown(Shutdown::Both);
    }
}

pub(in crate::multiprocess) struct Worker {
    client: Arc<Mutex<Client>>,
    last_call: u64,
}
pub(in crate::multiprocess) struct Call {
    client: Arc<Mutex<Client>>,
    call: u64,
    pub(in crate::multiprocess) copies: CopyContext,
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
        Self::connect(bootstrap).map(Some)
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
        Ok(Self {
            client: Arc::new(Mutex::new(Client {
                stream,
                seq: 0,
                failed: None,
            })),
            last_call: 0,
        })
    }

    pub(in crate::multiprocess) fn next(&mut self) -> io::Result<Call> {
        let mut client = self.client.lock().unwrap();
        client.check()?;
        client.stream.set_read_timeout(None)?;
        let message: HostMessage = read_frame(&mut client.stream)?;
        client.stream.set_read_timeout(Some(IO_TIMEOUT))?;
        let HostMessage::Start { call, boundary } = message else {
            return Err(invalid("expected native call grant"));
        };
        if self.last_call.checked_add(1) != Some(call) {
            return Err(invalid("native call grant sequence mismatch"));
        }
        self.last_call = call;
        let ledger = Arc::new(RemoteLedger {
            client: self.client.clone(),
            call,
            local: CopyContext::new(CopyPolicy::AllowCopies, boundary.clone()),
        });
        Ok(Call {
            client: self.client.clone(),
            call,
            copies: CopyContext::from_ledger(CopyPolicy::AllowCopies, boundary, ledger),
        })
    }
}

impl Call {
    pub(in crate::multiprocess) fn check(&self) -> io::Result<()> {
        self.client.lock().unwrap().check()
    }
    pub(in crate::multiprocess) fn finish(&self, result: Result<(), String>) -> io::Result<()> {
        let error = result.err().map(|error| error.chars().take(256).collect());
        let mut client = self.client.lock().unwrap();
        if let Err(error) = client.report(self.call, Event::Finished { error }) {
            client.fail(&error);
            return Err(error);
        }
        Ok(())
    }
}

struct RemoteLedger {
    client: Arc<Mutex<Client>>,
    call: u64,
    local: CopyContext,
}
impl CopyLedger for RemoteLedger {
    fn counts(&self) -> [CopyCounts; COPY_REASONS.len()] {
        self.local.counts()
    }
    fn record(&self, reason: CopyReason, event: CopyEvent) {
        self.local.record(reason, event);
        let mut client = self.client.lock().unwrap();
        if let Err(error) = client.report(
            self.call,
            Event::Copy {
                reason,
                event: event.into(),
            },
        ) {
            client.fail(error);
        }
    }
}
