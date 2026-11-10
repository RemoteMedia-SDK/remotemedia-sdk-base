use super::frame::{self, OUTPUT_PREFIX};
use super::*;
use remotemedia_types::{CopyEvent, CopyLedger};
use std::io::Write;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

fn copies(boundary: &str) -> CopyContext {
    CopyContext::new(CopyPolicy::AllowCopies, boundary)
}
fn count(context: &CopyContext, reason: CopyReason) -> CopyCounts {
    context.counts()[reason as usize]
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
fn input_frame(call: u64, boundary: &str, body: &[u8]) -> Vec<u8> {
    let prefix = frame::input_prefix_len(boundary).unwrap();
    let mut head = vec![MaybeUninit::uninit(); prefix];
    frame::write_input(&mut head, call, boundary).unwrap();
    // SAFETY: write_input initializes every byte of an exact-length prefix.
    let mut bytes: Vec<u8> = head.into_iter().map(|b| unsafe { b.assume_init() }).collect();
    bytes.extend_from_slice(body);
    bytes
}
fn output_frame(call: u64, ok: bool, ledger: &CopyContext, body: &[u8]) -> Vec<u8> {
    let mut head = vec![MaybeUninit::uninit(); OUTPUT_PREFIX];
    frame::write_output(&mut head, call, ok, &ledger.counts()).unwrap();
    // SAFETY: write_output initializes every byte of an exact-length prefix.
    let mut bytes: Vec<u8> = head.into_iter().map(|b| unsafe { b.assume_init() }).collect();
    bytes.extend_from_slice(body);
    bytes
}

#[test]
fn input_frames_carry_call_boundary_and_body() {
    let bytes = input_frame(7, "session:a/node", b"payload");
    let input = frame::parse_input(&bytes).unwrap();
    assert_eq!(input.call, 7);
    assert_eq!(input.boundary, "session:a/node");
    assert_eq!(input.body, b"payload");
    assert!(frame::parse_input(&bytes[..10]).is_err());
    let mut legacy = bytes.clone();
    legacy[0] = b'X';
    assert!(frame::parse_input(&legacy).is_err());
    assert!(frame::input_prefix_len(&"b".repeat(2000)).is_err());
    let truncated_name = &bytes[..16];
    assert!(frame::parse_input(truncated_name).is_err());
}

#[test]
fn output_totals_replay_to_the_same_counts_as_direct_recording() {
    // Worker-side ledger with completed, failed and rejected operations.
    let worker = copies("worker");
    for bytes in [17, 31] {
        worker.record(CopyReason::PortableDeserialization, CopyEvent::Started);
        worker.record(CopyReason::PortableDeserialization, CopyEvent::Completed(bytes));
    }
    worker.record(CopyReason::PortableSerialization, CopyEvent::Started);
    worker.record(CopyReason::PortableSerialization, CopyEvent::Failed(9));
    worker.record(CopyReason::VecMaterialization, CopyEvent::Rejected(11));
    let bytes = output_frame(3, true, &worker, b"body");
    let output = frame::parse_output(&bytes).unwrap();
    assert_eq!((output.call, output.ok, output.body), (3, true, &b"body"[..]));
    let host = copies("host");
    frame::replay(&host, &output.totals);
    assert_eq!(host.counts(), worker.counts());
    assert_eq!(count(&host, CopyReason::PortableDeserialization).copied_bytes, 48);
    assert_eq!(count(&host, CopyReason::PortableDeserialization).completed, 2);
    assert!(host.counts().iter().all(|c| c.in_flight == 0));
}

#[test]
fn malformed_or_incoherent_output_frames_are_rejected() {
    let worker = copies("worker");
    worker.record(CopyReason::PortableSerialization, CopyEvent::Started);
    worker.record(CopyReason::PortableSerialization, CopyEvent::Completed(5));
    let good = output_frame(1, false, &worker, b"handler failed");
    let output = frame::parse_output(&good).unwrap();
    assert!(!output.ok);
    assert_eq!(output.body, b"handler failed");
    assert!(frame::parse_output(&good[..OUTPUT_PREFIX - 1]).is_err());
    let field = |reason: CopyReason, field: usize| 16 + (reason as usize * 6 + field) * 8;
    for corrupt in [
        |b: &mut Vec<u8>| b[0] = b'X',  // magic
        |b: &mut Vec<u8>| b[12] = 2,    // status
        |b: &mut Vec<u8>| b[13] += 1,   // reason count
        |b: &mut Vec<u8>| b[14] = 1,    // reserved
    ] {
        let mut bytes = good.clone();
        corrupt(&mut bytes);
        assert!(frame::parse_output(&bytes).is_err());
    }
    // Bytes without operations, and unbounded operation counts.
    let mut bytes = good.clone();
    let at = field(CopyReason::VecMaterialization, 1);
    bytes[at..at + 8].copy_from_slice(&3u64.to_le_bytes());
    assert!(frame::parse_output(&bytes).is_err());
    let mut bytes = good.clone();
    let at = field(CopyReason::VecMaterialization, 0);
    bytes[at..at + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(frame::parse_output(&bytes).is_err());
    // A worker cannot report a terminal with copies still in flight.
    worker.record(CopyReason::PortableSerialization, CopyEvent::Started);
    let mut head = vec![MaybeUninit::uninit(); OUTPUT_PREFIX];
    assert!(frame::write_output(&mut head, 2, true, &worker.counts()).is_err());
}

#[test]
fn interrupted_calls_are_failed_not_completed_or_zero_copy_proof() {
    let context = copies("disconnect");
    frame::record_interrupted(&context);
    let counts = count(&context, CopyReason::PortableDeserialization);
    assert_eq!(counts.failed, 1);
    assert_eq!(counts.completed, 0);
    assert_eq!(counts.copied_bytes, 0);
    assert_eq!(counts.in_flight, 0);
}

#[test]
fn calls_are_granted_in_sequence_under_allow_copies_only() {
    let (host, mut worker) = connected();
    assert!(host
        .next_call(&CopyContext::new(CopyPolicy::RequireZeroCopy, "strict"))
        .is_err());
    for expected in 1..=3 {
        let call = host.next_call(&copies("call")).unwrap();
        assert_eq!(call, expected);
        worker.accept(call).unwrap();
    }
    assert!(worker.accept(5).is_err());
    assert!(worker.accept(3).is_err());
    worker.accept(4).unwrap();
}

#[test]
fn doorbell_wakes_an_idle_worker_and_host_close_ends_its_wait() {
    let (host, mut worker) = connected();
    // No idle spin: the worker blocks on the doorbell immediately.
    worker.idle_spin = Duration::ZERO;
    let published = Arc::new(AtomicBool::new(false));
    let flag = published.clone();
    let waiter = std::thread::spawn(move || {
        let value = worker.wait(|| Ok(flag.load(Ordering::Acquire).then_some(42)));
        (worker, value)
    });
    std::thread::sleep(Duration::from_millis(100));
    // Publish, then ring: the order the host uses.
    published.store(true, Ordering::Release);
    host.ring().unwrap();
    let (mut worker, value) = waiter.join().unwrap();
    assert_eq!(value.unwrap(), 42);
    // Rings while the worker is not waiting are drained, not mistaken for calls.
    for _ in 0..3 {
        host.ring().unwrap();
    }
    let closer = {
        let host = host.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            host.close();
        })
    };
    let started = Instant::now();
    assert!(worker.wait(|| Ok(None::<()>)).is_err());
    assert!(started.elapsed() < Duration::from_secs(2));
    closer.join().unwrap();
}

#[test]
fn worker_exit_or_stray_bytes_fail_the_lane() {
    let (host, worker) = connected();
    host.check().unwrap();
    drop(worker);
    let deadline = Instant::now() + IO_TIMEOUT;
    while host.check().is_ok() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(host.check().unwrap_err().to_string().contains("disconnected"));
    assert!(host.next_call(&copies("after")).is_err());

    let (host, mut worker) = connected();
    worker.stream.write_all(&[0]).unwrap();
    let deadline = Instant::now() + IO_TIMEOUT;
    while host.check().is_ok() && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(host.check().is_err());
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
fn close_joins_while_the_worker_stays_connected() {
    let (host, _worker) = connected();
    let start = Instant::now();
    host.close();
    assert!(start.elapsed() < Duration::from_secs(1));
    assert!(host.check().is_err());
    assert!(host.ring().is_err());
}
