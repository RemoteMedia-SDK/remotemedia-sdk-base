use super::*;
use crate::audio_io::capture::tests::{allocations, mapped, start_fixture};
use remotemedia_types::CopyPolicy;
use std::sync::atomic::AtomicUsize;

struct TrackedAllocator {
    provider: Arc<dyn OutputAllocator>,
    allocations: AtomicUsize,
    pointer: AtomicUsize,
}

impl OutputAllocator for TrackedAllocator {
    fn allocate_bytes(&self, len: usize) -> Result<BufferLoan<u8>, BufferAllocationError> {
        self.provider.allocate_bytes(len)
    }

    fn allocate_audio(&self, samples: usize) -> Result<BufferLoan<f32>, BufferAllocationError> {
        self.allocations.fetch_add(1, Ordering::Relaxed);
        let mut loan = self.provider.allocate_audio(samples)?;
        self.pointer
            .store(loan.as_mut_slice().as_ptr() as usize, Ordering::Relaxed);
        Ok(loan)
    }

    fn prepared_realtime_support(&self) -> remotemedia_types::PreparedRealtimeSupport {
        self.provider.prepared_realtime_support()
    }
}

fn fixture(context: NodeRuntimeContext, sizes: &'static [usize], fail: bool) -> AudioCapture {
    let stop = Arc::new(AtomicBool::new(false));
    let completed = stop.clone();
    start_fixture(
        context,
        CaptureMode::Recording(64),
        stop,
        move |mut callback, counters| {
            for &size in sizes {
                allocations::callback(|| callback.capture(&[0.25; 128][..size]));
            }
            counters.device_error.store(fail, Ordering::Release);
            allocations::callback(|| drop(callback));
            completed.store(true, Ordering::Release);
        },
    )
}

#[tokio::test]
async fn heap_recording_recovers_the_only_output_loan_without_an_aggregation_copy() {
    let allocator = Arc::new(TrackedAllocator {
        provider: Arc::new(HeapOutputAllocator),
        allocations: AtomicUsize::new(0),
        pointer: AtomicUsize::new(0),
    });
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "recording/heap");
    let context = NodeRuntimeContext::for_test("recording", "mic")
        .with_copy_context(copies.clone())
        .with_output_allocator(allocator.clone());
    let capture = fixture(context, &[10, 46, 18, 60, 8], false);
    // This executes in a Tokio runtime; collection must not call blocking_recv.
    let output = collect(capture, 64, Duration::from_secs(1)).unwrap();
    assert_eq!(output.as_slice(), &[0.25; 64]);
    assert_eq!(allocator.allocations.load(Ordering::Relaxed), 1);
    assert_eq!(
        output.as_slice().as_ptr() as usize,
        allocator.pointer.load(Ordering::Relaxed)
    );
    let vector = output.try_into_vec(&copies).unwrap();
    assert_eq!(
        vector.as_ptr() as usize,
        allocator.pointer.load(Ordering::Relaxed)
    );
    assert_eq!(copies.report().entries[3].1.copied_bytes, 64 * 4);
    assert_eq!(copies.report().entries[3].1.in_flight, 0);
    assert_eq!(copies.report().entries[0].1.copied_bytes, 0);
}

#[test]
fn mapped_recording_retains_the_only_pool_slot_after_shutdown() {
    let pool = mapped::pool(1);
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "recording/mapped");
    let context = NodeRuntimeContext::for_test("recording", "mic")
        .with_output_allocator(Arc::new(pool.clone()))
        .with_copy_context(copies.clone());
    let capture = fixture(context, &[128, 128], false);
    let output = collect(capture, 64, Duration::from_secs(1)).unwrap();
    assert!(pool.owns_buffer(&output));
    assert_eq!(output.as_slice(), &[0.25; 64]);
    assert!(pool.allocate_audio(64).is_err());
    assert_eq!(copies.report().entries[3].1.copied_bytes, 256);
    assert_eq!(copies.report().entries[3].1.in_flight, 0);
    drop(output);
    assert!(pool.allocate_audio(64).is_ok());
}

#[test]
fn short_recordings_and_terminal_device_errors_are_not_successful_recordings() {
    for (sizes, fail, expected) in [
        (&[14][..], false, "ended early"),
        (&[64][..], true, "device_error=true"),
        (&[][..], false, "without audio"),
        (&[15][..], false, "dropped_samples=15"),
    ] {
        let capture = fixture(
            NodeRuntimeContext::for_test("recording", "mic"),
            sizes,
            fail,
        );
        let error = collect(capture, 64, Duration::from_secs(1)).unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[test]
fn timed_out_recording_joins_callbacks_and_closes_accounting() {
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "recording/timeout");
    let context =
        NodeRuntimeContext::for_test("recording", "mic").with_copy_context(copies.clone());
    let exited = Arc::new(AtomicBool::new(false));
    let capture = crate::audio_io::capture::tests::live_fixture(
        context,
        Arc::new(AtomicBool::new(false)),
        exited.clone(),
    );
    let error = collect(capture, 64, Duration::from_millis(10)).unwrap_err();
    assert!(error.to_string().contains("timed out"), "{error}");
    assert!(exited.load(Ordering::Acquire));
    assert_eq!(copies.report().entries[3].1.copied_bytes, 56);
    assert_eq!(copies.report().entries[3].1.in_flight, 0);
}

#[test]
fn duration_counts_whole_interleaved_frames_without_u32_overflow() {
    let mut config = CaptureConfig {
        channels: 2,
        sample_rate: 44100,
        ..Default::default()
    };
    assert_eq!(config.samples_for_duration(1).unwrap(), 88);
    assert_eq!(config.samples_for_duration(1000).unwrap(), 88200);
    assert_eq!(config.samples_for_duration(100_000).unwrap(), 8_820_000);
    config.channels = 0;
    assert!(config.samples_for_duration(1).is_err());
    config.channels = 1;
    config.sample_rate = 0;
    assert!(config.samples_for_duration(1).is_err());
    config.sample_rate = u32::MAX;
    config.channels = u16::MAX;
    assert!(config.samples_for_duration(u32::MAX).is_err());
}

#[test]
fn zero_duration_allows_strict_policy_without_opening_a_device() {
    let config = CaptureConfig {
        device: Some(DeviceSelector::Name("nonexistent fixture".into())),
        ..Default::default()
    };
    let context = NodeRuntimeContext::for_test("recording", "mic").with_copy_context(
        CopyContext::new(CopyPolicy::RequireZeroCopy, "recording/empty"),
    );
    assert!(capture_sync_shared(config.clone(), 0, &context)
        .unwrap()
        .is_empty());
    assert!(capture_sync(config, 0).unwrap().is_empty());
}

/// Explicit real-device check; never opens a device in the ordinary test suite.
#[cfg(windows)]
#[test]
#[ignore = "opens the default local input device; set REMOTEMEDIA_CAPTURE_SMOKE=1"]
fn local_device_records_exact_frames_into_one_mapped_loan() {
    use remotemedia_shared_memory::{PoolConfig, SessionId, SharedPool, SizeClass};
    assert_eq!(
        std::env::var("REMOTEMEDIA_CAPTURE_SMOKE").as_deref(),
        Ok("1")
    );
    let device = cpal::default_host()
        .default_input_device()
        .expect("default input device");
    let supported = device.default_input_config().unwrap();
    let config = CaptureConfig {
        sample_rate: supported.sample_rate().0,
        channels: supported.channels(),
        ..Default::default()
    };
    let samples = config.samples_for_duration(50).unwrap();
    let bytes = (samples * 4).next_power_of_two();
    let pool = SharedPool::new(
        SessionId([63; 16]),
        PoolConfig {
            classes: vec![SizeClass { bytes, slots: 1 }],
            max_bytes: bytes,
            max_leases: 4,
        },
    )
    .unwrap();
    let allocator = Arc::new(TrackedAllocator {
        provider: Arc::new(pool.clone()),
        allocations: AtomicUsize::new(0),
        pointer: AtomicUsize::new(0),
    });
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "device/finite");
    let context = NodeRuntimeContext::for_test("device-finite", "mic")
        .with_output_allocator(allocator.clone())
        .with_copy_context(copies.clone());
    let output = capture_sync_shared(config.clone(), 50, &context).unwrap();
    assert_eq!(output.len(), samples);
    assert_eq!(allocator.allocations.load(Ordering::Relaxed), 1);
    assert_eq!(
        output.as_slice().as_ptr() as usize,
        allocator.pointer.load(Ordering::Relaxed)
    );
    assert!(pool.owns_buffer(&output));
    assert!(pool.allocate_audio(samples).is_err());
    assert_eq!(
        copies.report().entries[3].1.copied_bytes,
        (samples * 4) as u64
    );
    assert_eq!(copies.report().entries[3].1.in_flight, 0);
    assert_eq!(copies.report().entries[0].1.copied_bytes, 0);
    println!("finite device={}, rate={}, channels={}, samples={}, copied_bytes={}, provider_allocations=1",
        device.name().unwrap(), config.sample_rate, config.channels, samples, samples * 4);
    drop(context);
    drop(allocator);
    drop(pool);
    assert!(output.as_slice().iter().all(|sample| sample.is_finite()));
}

#[test]
fn strict_recording_rejects_before_provider_allocation_and_device_open() {
    let allocator = Arc::new(TrackedAllocator {
        provider: Arc::new(HeapOutputAllocator),
        allocations: AtomicUsize::new(0),
        pointer: AtomicUsize::new(0),
    });
    let context = NodeRuntimeContext::for_test("recording", "mic")
        .with_output_allocator(allocator.clone())
        .with_copy_context(CopyContext::new(
            CopyPolicy::RequireZeroCopy,
            "recording/strict",
        ));
    let result = AudioCapture::start_backend::<()>(
        CaptureConfig::default(),
        CaptureMode::Recording(64),
        context,
        Arc::new(AtomicBool::new(false)),
        |_, _, _| panic!("strict device open"),
    );
    assert!(result.err().unwrap().to_string().contains("rejects 256"));
    assert_eq!(allocator.allocations.load(Ordering::Relaxed), 0);
}
