use super::state::State;
use super::*;
use std::io::Write;
use std::time::Instant;

fn copies(boundary: &str) -> CopyContext {
    CopyContext::new(CopyPolicy::AllowCopies, boundary)
}
fn count(context: &CopyContext) -> CopyCounts {
    context.counts()[CopyReason::PortableDeserialization as usize]
}
fn event(seq: u64, call: u64, event: WireCopy) -> Report {
    Report {
        seq,
        call,
        event: Event::Copy {
            reason: CopyReason::PortableDeserialization,
            event,
        },
    }
}
fn terminal(seq: u64, call: u64) -> Report {
    Report {
        seq,
        call,
        event: Event::Finished { error: None },
    }
}
fn bootstrap(host: &Host) -> Bootstrap {
    let mut bytes = Vec::new();
    host.bootstrap(&mut bytes).unwrap();
    read_frame(&mut bytes.as_slice()).unwrap()
}
fn connected() -> (Arc<Host>, Worker) {
    let host = Host::start("node", "session").unwrap();
    let worker = Worker::connect(bootstrap(&host)).unwrap();
    host.wait_ready(|| Ok(true)).unwrap();
    (host, worker)
}

#[test]
fn reports_bind_exact_context_and_enforce_transitions() {
    let mut state = State::default();
    assert!(state
        .begin(CopyContext::new(CopyPolicy::RequireZeroCopy, "strict"))
        .is_err());
    let a = copies("first");
    let b = copies("second");
    let call = state.begin(a.clone()).unwrap();
    assert!(state.begin(b.clone()).is_err());
    assert!(state.apply(event(2, call, WireCopy::Started)).is_err());
    assert!(state.apply(event(1, call + 1, WireCopy::Started)).is_err());
    assert!(state
        .apply(event(1, call, WireCopy::Completed { bytes: 7 }))
        .is_err());
    state.apply(event(1, call, WireCopy::Started)).unwrap();
    assert!(state.apply(event(1, call, WireCopy::Started)).is_err());
    assert!(state.apply(event(2, call, WireCopy::Started)).is_err());
    assert!(state.apply(terminal(2, call)).is_err());
    state
        .apply(event(2, call, WireCopy::Completed { bytes: 7 }))
        .unwrap();
    state.apply(terminal(3, call)).unwrap();
    assert!(state.status(call).unwrap());
    assert!(state.apply(event(4, call, WireCopy::Started)).is_err());
    let next = state.begin(b.clone()).unwrap();
    state.apply(event(4, next, WireCopy::Started)).unwrap();
    state
        .apply(event(5, next, WireCopy::Failed { bytes: 9 }))
        .unwrap();
    state
        .apply(event(6, next, WireCopy::Rejected { bytes: 11 }))
        .unwrap();
    state.apply(terminal(7, next)).unwrap();
    assert_eq!(count(&a).copied_bytes, 7);
    assert_eq!(count(&a).failed, 0);
    assert_eq!(count(&b).copied_bytes, 0);
    assert_eq!(count(&b).failed_requested_bytes, 9);
    assert_eq!(count(&b).rejected_bytes, 11);
    assert_eq!(count(&b).in_flight, 0);
}

#[test]
fn interrupted_reports_are_failed_not_completed_or_zero_copy_proof() {
    let mut state = State::default();
    let copies = copies("disconnect");
    let call = state.begin(copies.clone()).unwrap();
    state.apply(event(1, call, WireCopy::Started)).unwrap();
    assert_eq!(count(&copies).in_flight, 1);
    state.fail("disconnected");
    state.fail("again");
    assert!(state.status(call).is_err());
    assert!(state.begin(copies.clone()).is_err());
    let counts = count(&copies);
    assert_eq!(counts.in_flight, 0);
    assert_eq!(counts.failed, 1);
    assert_eq!(counts.completed, 0);
    assert_eq!(counts.copied_bytes, 0);
}

#[test]
fn acknowledged_reports_and_terminal_precede_success_and_next_call() {
    let (host, mut worker) = connected();
    for length in [17, 31, 7] {
        let context = copies(&format!("call-{length}"));
        let id = host.begin(context.clone()).unwrap();
        let call = worker.next().unwrap();
        assert_eq!(call.copies.boundary(), context.boundary());
        assert!(!host.status(id).unwrap());
        call.copies
            .materialize(CopyReason::PortableDeserialization, length, || ())
            .unwrap();
        assert_eq!(count(&context).copied_bytes, length);
        assert_eq!(count(&context).in_flight, 0);
        call.finish(Ok(())).unwrap();
        assert!(host.status(id).unwrap());
    }
    let close_started = Instant::now();
    host.close();
    assert!(close_started.elapsed() < Duration::from_secs(1));
    assert!(worker.next().is_err());
}

#[test]
fn malformed_report_disconnect_closes_pending_accounting() {
    let host = Host::start("node", "session").unwrap();
    let hello = bootstrap(&host);
    let mut peer = TcpStream::connect(&hello.address).unwrap();
    peer.set_read_timeout(Some(IO_TIMEOUT)).unwrap();
    write_frame(&mut peer, &hello).unwrap();
    assert!(matches!(
        read_frame::<HostMessage>(&mut peer).unwrap(),
        HostMessage::Ack { seq: 0 }
    ));
    host.wait_ready(|| Ok(true)).unwrap();
    let context = copies("broken");
    let id = host.begin(context.clone()).unwrap();
    assert!(matches!(
        read_frame::<HostMessage>(&mut peer).unwrap(),
        HostMessage::Start { .. }
    ));
    write_frame(&mut peer, &event(1, id, WireCopy::Started)).unwrap();
    assert!(matches!(
        read_frame::<HostMessage>(&mut peer).unwrap(),
        HostMessage::Ack { seq: 1 }
    ));
    peer.write_all(&0u32.to_be_bytes()).unwrap();
    assert!(read_frame::<HostMessage>(&mut peer).is_err());
    let deadline = Instant::now() + IO_TIMEOUT;
    while host.status(id).is_ok() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(host.status(id).is_err());
    assert_eq!(count(&context).failed, 1);
    assert_eq!(count(&context).in_flight, 0);
}

#[test]
fn wrong_capability_or_version_cannot_claim_reporting_channel() {
    let host = Host::start("node", "session").unwrap();
    for bad_version in [false, true] {
        let mut hello = bootstrap(&host);
        if bad_version {
            hello.version += 1;
        } else {
            hello.token = "wrong".into();
        }
        let mut peer = TcpStream::connect(&hello.address).unwrap();
        peer.set_read_timeout(Some(IO_TIMEOUT)).unwrap();
        write_frame(&mut peer, &hello).unwrap();
        assert!(read_frame::<HostMessage>(&mut peer).is_err());
    }
    let _worker = Worker::connect(bootstrap(&host)).unwrap();
    host.wait_ready(|| Ok(true)).unwrap();
}

#[test]
fn failed_terminal_is_preserved_after_worker_disconnect() {
    let (host, mut worker) = connected();
    let id = host.begin(copies("failed")).unwrap();
    let call = worker.next().unwrap();
    call.finish(Err("handler failed".into())).unwrap();
    drop(call);
    drop(worker);
    assert!(host
        .status(id)
        .unwrap_err()
        .to_string()
        .contains("handler failed"));
    host.close();
    assert!(host
        .status(id)
        .unwrap_err()
        .to_string()
        .contains("handler failed"));
}

#[test]
fn close_during_handshake_joins_reporter() {
    for _ in 0..12 {
        let host = Host::start("node", "session").unwrap();
        let hello = bootstrap(&host);
        let peer = std::thread::spawn(move || Worker::connect(hello));
        host.close();
        drop(peer.join().unwrap());
    }
}

#[test]
fn close_joins_with_partial_frame_and_open_peer() {
    let host = Host::start("node", "session").unwrap();
    let hello = bootstrap(&host);
    let mut peer = TcpStream::connect(&hello.address).unwrap();
    peer.set_read_timeout(Some(IO_TIMEOUT)).unwrap();
    write_frame(&mut peer, &hello).unwrap();
    assert!(matches!(
        read_frame::<HostMessage>(&mut peer).unwrap(),
        HostMessage::Ack { seq: 0 }
    ));
    host.wait_ready(|| Ok(true)).unwrap();
    peer.write_all(&[0, 0]).unwrap();
    // Let the reader enter a partial frame before cancellation.
    std::thread::sleep(Duration::from_millis(150));
    let start = Instant::now();
    host.close();
    assert!(start.elapsed() < Duration::from_secs(1));
    assert!(read_frame::<HostMessage>(&mut peer).is_err());
}
