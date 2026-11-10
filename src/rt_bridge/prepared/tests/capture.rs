use super::*;
use remotemedia_types::{CopyContext, CopyPolicy};

#[test]
fn variable_callbacks_publish_full_and_partial_packets_without_intermediate_copies() {
    let mapped = pool(8);
    for provider in [
        Arc::new(HeapOutputAllocator) as Arc<dyn OutputAllocator>,
        Arc::new(mapped.clone()),
    ] {
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "device/capture");
        let (bridge, mut producer, mut consumer) = RtBridge::spawn_prepared_capture(
            ContextEcho,
            RtBridgeConfig::default(),
            config(4),
            context(provider).with_copy_context(copies.clone()),
        )
        .unwrap();
        let input: Vec<f32> = (0..142).map(|x| x as f32).collect();
        let mut offset = 0;
        for count in [10, 46, 18, 60, 8] {
            let result = callback(|| {
                producer.try_capture(&input[offset..offset + count], Some(offset as u64 * 1000))
            });
            assert_eq!(result.accepted_samples, count);
            assert_eq!(result.error, None);
            offset += count;
        }
        // Capture the partial loan address before worker-side freeze/slicing.
        let partial_pointer = producer.producer.current.as_ref().unwrap().loan.as_ptr() as usize;
        producer.finish().unwrap();
        producer.finish().unwrap();
        assert_eq!(
            callback(|| producer.try_capture(&[0.0; 2], None)).error,
            Some(PreparedAudioError::Stopped)
        );
        let mut retained = Vec::new();
        for _ in 0..3 {
            retained.push(until_ready(|| {
                callback(|| consumer.try_read(|data| data.clone())).unwrap()
            }));
        }
        drop(producer);
        drop(consumer);
        bridge.shutdown();
        assert_capture_output(&retained, &input, partial_pointer);
        let counts = copies.report().entries[CopyReason::BorrowedInput as usize].1;
        assert_eq!(counts.copied_bytes, 142 * 4);
        assert_eq!(counts.failed, 0);
        assert_eq!(counts.in_flight, 0);
    }
}

fn assert_capture_output(retained: &[RuntimeData], input: &[f32], partial_pointer: usize) {
    let mut output = Vec::new();
    for (index, packet) in retained.iter().enumerate() {
        let RuntimeData::Audio {
            samples,
            timestamp_us,
            ..
        } = packet
        else {
            panic!("audio");
        };
        let timestamps = [0, 56083, 74562];
        assert_eq!(*timestamp_us, Some(timestamps[index]));
        if index == 2 {
            assert_eq!(samples.len(), 14);
            assert_eq!(samples.as_ptr() as usize, partial_pointer);
        } else {
            assert_eq!(samples.len(), 64);
        }
        output.extend_from_slice(samples.as_ref());
    }
    assert_eq!(output, input);
}

#[test]
fn capture_pressure_reports_accepted_prefix_and_release_accounts_it_off_callback() {
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "device/pressure");
    let stopped = Arc::new(AtomicBool::new(false));
    let (_, output_rx) = rtrb::RingBuffer::new(2);
    let (mut worker, producer, consumer) = PreparedWorker::prepare(
        context(Arc::new(HeapOutputAllocator)).with_copy_context(copies.clone()),
        config(4),
        3,
        output_rx,
        stopped.clone(),
        true,
    )
    .unwrap();
    let mut producer = PreparedCaptureProducer {
        producer,
        channels: 2,
        sample_rate: 48000,
        finished: false,
    };
    let result = callback(|| producer.try_capture(&[0.5; 200], None));
    assert_eq!(result.accepted_samples, 128);
    assert_eq!(result.error, Some(PreparedAudioError::Full));
    assert_eq!(
        copies.report().entries[3].1.copied_bytes,
        0,
        "ledger work is deferred"
    );
    drop(worker.pop().unwrap());
    let partial = callback(|| producer.try_capture(&[0.25; 12], None));
    assert_eq!(partial.accepted_samples, 12);
    assert_eq!(partial.error, None);
    let invalid = callback(|| producer.try_capture(&[0.0; 3], None));
    assert_eq!(invalid.accepted_samples, 0);
    assert_eq!(invalid.error, Some(PreparedAudioError::InvalidLayout));
    // Abandon both queued and partially captured samples. Known copies still
    // count even when no node processes the packet, and unused loans count zero.
    stopped.store(true, Ordering::Release);
    assert_eq!(producer.finish(), Err(PreparedAudioError::Stopped));
    assert_eq!(
        callback(|| producer.try_capture(&[0.0; 2], None)).accepted_samples,
        0
    );
    drop(producer);
    drop(consumer);
    drop(worker);
    let counts = copies.report().entries[3].1;
    assert_eq!(counts.copied_bytes, 140 * 4);
    assert_eq!(counts.in_flight, 0);
    assert_eq!(counts.failed, 0);
}

#[test]
fn capture_drop_flushes_partial_and_exhaustion_never_falls_back_to_heap() {
    let mapped = pool(1);
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "device/exhaustion");
    let (_, output_rx) = rtrb::RingBuffer::new(2);
    let (mut worker, producer, consumer) = PreparedWorker::prepare(
        context(Arc::new(mapped.clone())).with_copy_context(copies.clone()),
        config(1),
        3,
        output_rx,
        Arc::new(AtomicBool::new(false)),
        true,
    )
    .unwrap();
    let mut producer = PreparedCaptureProducer {
        producer,
        channels: 2,
        sample_rate: 48000,
        finished: false,
    };
    let progress = callback(|| producer.try_capture(&[1.0; 70], None));
    assert_eq!(progress.accepted_samples, 64);
    assert_eq!(progress.error, Some(PreparedAudioError::Unavailable));
    let packet = worker.pop().unwrap();
    worker.service();
    assert_eq!(
        callback(|| producer.try_capture(&[0.5; 6], None)).error,
        Some(PreparedAudioError::Unavailable)
    );
    drop(packet);
    worker.service();
    assert_eq!(
        callback(|| producer.try_capture(&[0.5; 6], Some(77))).accepted_samples,
        6
    );
    drop(producer);
    let RuntimeData::Audio {
        samples,
        timestamp_us,
        ..
    } = worker.pop().unwrap()
    else {
        panic!("audio");
    };
    assert_eq!(samples.as_ref(), &[0.5; 6]);
    assert_eq!(timestamp_us, Some(77));
    assert!(mapped.owns_buffer(&samples.into_shared()));
    drop(consumer);
    drop(worker);
    let counts = copies.report().entries[3].1;
    assert_eq!(counts.copied_bytes, 70 * 4);
    assert_eq!(counts.in_flight, 0);
}

#[test]
fn strict_device_capture_is_rejected_before_preparing_loans() {
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "device/strict");
    let result = RtBridge::spawn_prepared_capture(
        ContextEcho,
        RtBridgeConfig::default(),
        config(1),
        context(opted_in_invalid()).with_copy_context(copies.clone()),
    );
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("strict capture must reject"),
    };
    assert!(error
        .to_string()
        .contains("zero-copy boundary 'device/strict' rejects 256"));
    let counts = copies.report().entries[3].1;
    assert_eq!(counts.rejected_bytes, 256);
    assert_eq!(counts.in_flight, 0);
}

#[test]
fn joined_shutdown_reports_final_counts_and_drains_only_unread_output() {
    let (bridge, mut producer, mut consumer) = RtBridge::spawn_prepared_capture(
        ContextEcho,
        RtBridgeConfig::default(),
        config(4),
        context(Arc::new(HeapOutputAllocator)),
    )
    .unwrap();
    assert_eq!(
        producer.try_capture(&[0.5; 142], None).accepted_samples,
        142
    );
    producer.finish().unwrap();
    until_ready(|| (bridge.stats().processed == 3).then_some(()));
    let first = consumer
        .try_read(|packet| {
            let RuntimeData::Audio { samples, .. } = packet else {
                panic!("audio");
            };
            samples.len()
        })
        .unwrap();
    assert_eq!(first, Some(64));
    drop(producer);
    let stats = bridge.shutdown_checked().unwrap();
    assert_eq!(stats.processed, 3);
    assert_eq!(stats.output_overflows, 0);
    let remaining: Vec<_> = consumer
        .drain_after_shutdown()
        .map(|packet| {
            let RuntimeData::Audio { samples, .. } = packet else {
                panic!("audio");
            };
            samples.len()
        })
        .collect();
    assert_eq!(remaining, [64, 14]);
}

struct PanicNode;
impl SyncStreamingNode for PanicNode {
    fn node_type(&self) -> &str {
        "PanicNode"
    }
    fn process(&self, _: RuntimeData) -> crate::Result<RuntimeData> {
        panic!("worker failure fixture")
    }
}

#[test]
fn checked_shutdown_preserves_worker_panics() {
    let (bridge, mut producer, consumer) = RtBridge::spawn_prepared_audio(
        PanicNode,
        RtBridgeConfig::default(),
        config(1),
        context(Arc::new(HeapOutputAllocator)),
    )
    .unwrap();
    producer
        .try_fill(None, |samples| samples.fill(1.0))
        .unwrap();
    drop(producer);
    assert!(bridge.shutdown_checked().is_err());
    assert_eq!(consumer.drain_after_shutdown().count(), 0);
}
