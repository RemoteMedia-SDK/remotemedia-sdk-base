use super::*;
use remotemedia_types::CopyPolicy;

pub(crate) use crate::rt_bridge::test_support::{allocations, mapped};

struct FixtureStream(Option<JoinHandle<()>>);
impl Drop for FixtureStream {
    fn drop(&mut self) {
        self.0.take().unwrap().join().unwrap();
    }
}

pub(super) fn start_fixture(
    context: NodeRuntimeContext,
    mode: CaptureMode,
    stop: Arc<AtomicBool>,
    run: impl FnOnce(CaptureCallback, Arc<CaptureCounters>) + Send + 'static,
) -> AudioCapture {
    AudioCapture::start_backend(
        CaptureConfig {
            sample_rate: 48000,
            channels: 2,
            ..Default::default()
        },
        mode,
        context,
        stop,
        move |_, callback, counters| {
            let thread = std::thread::spawn(move || run(callback, counters));
            Ok((FixtureStream(Some(thread)), "fixture".into()))
        },
    )
    .unwrap()
}

pub(crate) fn live_fixture(
    context: NodeRuntimeContext,
    stop: Arc<AtomicBool>,
    exited: Arc<AtomicBool>,
) -> AudioCapture {
    let callback_stop = stop.clone();
    let (ready, started) = mpsc::sync_channel(1);
    let capture = start_fixture(
        context,
        CaptureMode::Stream(64),
        stop,
        move |mut callback, _| {
            allocations::callback(|| callback.capture(&[0.5; 14]));
            ready.send(()).unwrap();
            while !callback_stop.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            allocations::callback(|| drop(callback));
            exited.store(true, Ordering::Release);
        },
    );
    started.recv().unwrap();
    capture
}

pub(crate) fn fixture(context: NodeRuntimeContext, stop: Arc<AtomicBool>) -> AudioCapture {
    let callback_stop = stop.clone();
    start_fixture(
        context,
        CaptureMode::Stream(64),
        stop,
        move |mut callback, _| {
            for size in [10, 46, 18, 60, 8] {
                allocations::callback(|| callback.capture(&[0.25; 64][..size]));
            }
            allocations::callback(|| drop(callback));
            callback_stop.store(true, Ordering::Release);
        },
    )
}

#[tokio::test]
async fn shared_capture_drains_partial_tail_and_joins_before_eof() {
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "capture/source");
    let context = NodeRuntimeContext::for_test("capture", "mic").with_copy_context(copies.clone());
    let mut capture = fixture(context, Arc::new(AtomicBool::new(false)));
    let mut lengths = Vec::new();
    while let Some(packet) = capture.recv_packet().await.unwrap() {
        let RuntimeData::Audio { samples, .. } = packet else {
            panic!("audio");
        };
        assert!(matches!(
            samples,
            remotemedia_types::AudioSamples::Shared(_)
        ));
        assert!(samples.iter().all(|value| *value == 0.25));
        lengths.push(samples.len());
    }
    assert_eq!(lengths, [64, 64, 14]);
    assert!(capture.thread.is_none());
    assert_eq!(copies.report().entries[3].1.copied_bytes, 142 * 4);
    assert_eq!(copies.report().entries[0].1.copied_bytes, 0);
    assert_eq!(copies.report().entries[3].1.in_flight, 0);
}

#[test]
fn strict_capture_rejects_before_backend_open() {
    let context = NodeRuntimeContext::for_test("capture", "mic").with_copy_context(
        CopyContext::new(CopyPolicy::RequireZeroCopy, "capture/strict"),
    );
    let result = AudioCapture::start_backend::<()>(
        CaptureConfig::default(),
        CaptureMode::Stream(64),
        context,
        Arc::new(AtomicBool::new(false)),
        |_, _, _| panic!("strict policy must reject before device open"),
    );
    assert!(result.err().unwrap().to_string().contains("rejects 256"));
}

#[test]
fn startup_failure_returns_error_and_releases_prepared_owners() {
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "capture/startup");
    let context = NodeRuntimeContext::for_test("capture", "mic").with_copy_context(copies.clone());
    let result = AudioCapture::start_backend::<()>(
        CaptureConfig::default(),
        CaptureMode::Stream(64),
        context,
        Arc::new(AtomicBool::new(false)),
        |_, _, _| anyhow::bail!("device unavailable"),
    );
    assert!(result
        .err()
        .unwrap()
        .to_string()
        .contains("device unavailable"));
    assert_eq!(copies.report().entries[3].1.copied_bytes, 0);
    assert_eq!(copies.report().entries[3].1.in_flight, 0);
}

#[tokio::test]
async fn device_error_is_reported_after_drain() {
    let mut capture = AudioCapture::start_backend(
        CaptureConfig::default(),
        CaptureMode::Stream(64),
        NodeRuntimeContext::for_test("capture", "mic"),
        Arc::new(AtomicBool::new(false)),
        |_, callback, counters| {
            counters.device_error.store(true, Ordering::Release);
            Ok((callback, "error fixture".into()))
        },
    )
    .unwrap();
    let error = capture.recv_packet().await.unwrap_err().to_string();
    assert!(error.contains("device_error=true"), "{error}");
    assert!(capture.thread.is_none());
}

#[test]
fn terminal_error_includes_losses_while_draining_the_partial_tail() {
    let (output, _receiver) = async_mpsc::channel(1);
    let (started, _startup) = mpsc::sync_channel(1);
    let run = CaptureRun {
        config: CaptureConfig::default(),
        context: NodeRuntimeContext::for_test("capture", "loss"),
        audio: crate::rt_bridge::PreparedAudioConfig {
            samples: 64,
            sample_rate: 48000,
            channels: 2,
            ready_capacity: 4,
        },
        stop: Arc::new(AtomicBool::new(false)),
        output,
        recording: false,
    };
    let error = run
        .run(&started, |_, mut callback, _| {
            callback.capture(&[0.25; 142]);
            Ok((callback, "loss fixture".into()))
        })
        .unwrap_err();
    assert!(error.to_string().contains("output_packets=2"), "{error}");
}

/// Explicit local-device validation; never runs as part of an ordinary test run.
#[cfg(windows)]
#[tokio::test]
#[ignore = "opens the default local input device; set REMOTEMEDIA_CAPTURE_SMOKE=1"]
async fn local_device_capture_preserves_mapped_storage_and_exact_copy_counts() {
    assert_eq!(
        std::env::var("REMOTEMEDIA_CAPTURE_SMOKE").as_deref(),
        Ok("1")
    );
    use remotemedia_shared_memory::{PoolConfig, SessionId, SharedPool, SizeClass};
    let host = cpal::default_host();
    let device = host.default_input_device().expect("default input device");
    let supported = device.default_input_config().unwrap();
    let config = CaptureConfig {
        sample_rate: supported.sample_rate().0,
        channels: supported.channels(),
        ..Default::default()
    };
    let chunk_samples = config.sample_rate as usize / 50 * usize::from(config.channels);
    let slot_bytes = (chunk_samples * 4).next_power_of_two();
    let pool = SharedPool::new(
        SessionId([62; 16]),
        PoolConfig {
            classes: vec![SizeClass {
                bytes: slot_bytes,
                slots: 16,
            }],
            max_bytes: slot_bytes * 16,
            max_leases: 32,
        },
    )
    .unwrap();
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "device/smoke");
    let context = NodeRuntimeContext::for_test("device-smoke", "mic")
        .with_output_allocator(Arc::new(pool.clone()))
        .with_copy_context(copies.clone());
    let mut capture =
        AudioCapture::start_with_context(config.clone(), chunk_samples, &context).unwrap();
    let mut samples_received = 0;
    let mut packets = 0;
    let mut retained = None;
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while let Some(packet) = capture.recv_packet().await.unwrap() {
            let RuntimeData::Audio { samples, .. } = packet else {
                panic!("audio");
            };
            let shared = samples.into_shared();
            assert!(pool.owns_buffer(&shared));
            assert!(shared.as_slice().iter().all(|sample| sample.is_finite()));
            samples_received += shared.len();
            packets += 1;
            if retained.is_none() {
                retained = Some(shared);
            }
            if packets >= 8 {
                capture.request_stop();
            }
        }
    })
    .await
    .expect("device capture deadline");
    assert!(packets >= 8);
    assert!(capture.thread.is_none());
    let device_name = capture.device_name().to_owned();
    drop(capture);
    drop(context);
    drop(pool);
    let retained = retained.unwrap();
    assert!(!retained.is_empty());
    assert!(retained.as_slice().iter().all(|sample| sample.is_finite()));
    let report = copies.report();
    assert_eq!(
        report.entries[3].1.copied_bytes,
        samples_received as u64 * 4
    );
    assert_eq!(report.entries[3].1.in_flight, 0);
    assert_eq!(report.entries[0].1.copied_bytes, 0);
    println!("CPAL={} input={:?} rate={} channels={} packets={} samples={} copied_bytes={} vec_bytes=0 joined=true retained_mapped=true",
        host.id().name(), device_name, config.sample_rate, config.channels, packets, samples_received, report.entries[3].1.copied_bytes);
}
