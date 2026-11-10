use super::*;
use crate::nodes::{NodeRuntimeContextRead, SyncStreamingNode};
use crate::rt_bridge::{RtBridge, RtBridgeConfig};
use remotemedia_types::{BufferAllocationError, HeapOutputAllocator, OutputAllocator};

use crate::rt_bridge::test_support::allocations;
use allocations::callback;

use crate::rt_bridge::test_support::poll;
use poll::until_ready;

fn config(ready_capacity: usize) -> PreparedAudioConfig {
    PreparedAudioConfig {
        samples: 64,
        sample_rate: 48000,
        channels: 2,
        ready_capacity,
    }
}

use crate::rt_bridge::test_support::mapped;
use mapped::pool;

mod capture;

fn context(allocator: Arc<dyn OutputAllocator>) -> NodeRuntimeContext {
    NodeRuntimeContext::for_test("rt-prepared", "node").with_output_allocator(allocator)
}

#[test]
fn callback_backpressure_empty_and_stop_never_allocate_or_release() {
    let stopped = Arc::new(AtomicBool::new(false));
    let (mut output, output_rx) = rtrb::RingBuffer::new(2);
    let (mut worker, mut producer, mut consumer) = PreparedWorker::new(
        context(Arc::new(HeapOutputAllocator)),
        config(4),
        2,
        output_rx,
        stopped.clone(),
    )
    .unwrap();
    assert!(callback(|| consumer.try_read(|_| ())).unwrap().is_none());
    for _ in 0..2 {
        callback(|| producer.try_fill(None, |samples| samples.fill(0.25))).unwrap();
    }
    assert_eq!(
        callback(|| producer.try_fill(None, |_| panic!("full"))),
        Err(PreparedAudioError::Full)
    );
    let packet = worker.pop().unwrap();
    for _ in 0..4 {
        output.push(packet.clone()).unwrap();
        // Two returns fit, the third stays in the consumer; the fourth read blocks.
        let result = callback(|| consumer.try_read(|_| ()));
        if consumer.current.is_some() {
            assert!(result.unwrap().is_some());
            break;
        }
    }
    assert_eq!(
        callback(|| consumer.try_read(|_| panic!("return queue full"))),
        Err(PreparedAudioError::Full)
    );
    worker.service();
    assert!(callback(|| consumer.try_read(|_| ())).unwrap().is_none());
    stopped.store(true, Ordering::Release);
    assert_eq!(
        callback(|| producer.try_fill(None, |_| panic!("stopped"))),
        Err(PreparedAudioError::Stopped)
    );
    assert_eq!(
        callback(|| consumer.try_read(|_| ())),
        Err(PreparedAudioError::Stopped)
    );
}

#[test]
fn mapped_retained_views_exhaust_provider_without_heap_fallback() {
    let pool = pool(1);
    let (_, output_rx) = rtrb::RingBuffer::new(2);
    let (mut worker, mut producer, _consumer) = PreparedWorker::new(
        context(Arc::new(pool.clone())),
        config(1),
        2,
        output_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    let mut pointer = 0;
    callback(|| {
        producer.try_fill(Some(123), |samples| {
            pointer = samples.as_ptr() as usize;
            samples.fill(0.75);
        })
    })
    .unwrap();
    let packet = worker.pop().unwrap();
    let RuntimeData::Audio {
        samples,
        timestamp_us,
        ..
    } = packet
    else {
        panic!("audio")
    };
    assert_eq!(samples.as_ptr() as usize, pointer);
    assert_eq!(timestamp_us, Some(123));
    let shared = samples.into_shared();
    assert!(pool.owns_buffer(&shared));
    let retained = shared.slice(8..16).unwrap();
    drop(shared);
    worker.service();
    assert_eq!(
        callback(|| producer.try_fill(None, |_| panic!("exhausted"))),
        Err(PreparedAudioError::Unavailable)
    );
    assert_eq!(retained.as_slice(), &[0.75; 8]);
    drop(retained);
    worker.service();
    callback(|| producer.try_fill(None, |samples| samples.fill(0.5))).unwrap();
}

#[test]
fn callback_panics_retain_input_and_output_owners() {
    let pool = pool(1);
    let (mut output, output_rx) = rtrb::RingBuffer::new(2);
    let (mut worker, mut producer, mut consumer) = PreparedWorker::new(
        context(Arc::new(pool.clone())),
        config(1),
        2,
        output_rx,
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = producer.try_fill(None, |_| panic!("producer panic"));
    }));
    assert!(failed.is_err());
    assert!(producer.current.is_some());
    assert!(pool.allocate_audio(64).is_err());
    callback(|| producer.try_fill(None, |samples| samples.fill(1.0))).unwrap();
    output.push(worker.pop().unwrap()).unwrap();
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = consumer.try_read::<()>(|_| panic!("consumer panic"));
    }));
    assert!(failed.is_err());
    assert!(consumer.current.is_some());
    assert!(pool.allocate_audio(64).is_err());
    callback(|| consumer.try_read(|_| ())).unwrap();
    worker.service();
    callback(|| producer.try_fill(None, |_| ())).unwrap();
}

struct ContextEcho;
impl SyncStreamingNode for ContextEcho {
    fn node_type(&self) -> &str {
        "ContextEcho"
    }
    fn process(&self, _: RuntimeData) -> crate::Result<RuntimeData> {
        panic!("prepared bridge must preserve the execution context")
    }
    fn process_with_ctx(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> crate::Result<RuntimeData> {
        assert_eq!(ctx.session_id(), "rt-prepared");
        Ok(data)
    }
}

#[test]
fn real_worker_keeps_heap_and_mapped_sample_addresses() {
    let mapped = pool(4);
    let providers: [Arc<dyn OutputAllocator>; 2] =
        [Arc::new(HeapOutputAllocator), Arc::new(mapped)];
    for provider in providers {
        let (bridge, mut producer, mut consumer) = RtBridge::spawn_prepared_audio(
            ContextEcho,
            RtBridgeConfig::default(),
            config(2),
            context(provider),
        )
        .unwrap();
        for sequence in 0..128 {
            let mut pointer = 0;
            until_ready(|| {
                callback(|| {
                    producer.try_fill(Some(sequence), |samples| {
                        pointer = samples.as_ptr() as usize;
                        samples.fill(sequence as f32);
                    })
                })
                .ok()
            });
            until_ready(|| {
                callback(|| {
                    consumer.try_read(|data| {
                        let RuntimeData::Audio {
                            samples,
                            timestamp_us,
                            ..
                        } = data
                        else {
                            panic!("audio")
                        };
                        assert_eq!(samples.as_ptr() as usize, pointer);
                        assert_eq!(*timestamp_us, Some(sequence));
                        assert!(samples.iter().all(|&x| x == sequence as f32));
                    })
                })
                .unwrap()
            });
        }
        drop(producer);
        drop(consumer);
        bridge.shutdown();
    }
}

#[test]
fn preparation_rejects_invalid_layout_and_exhaustion() {
    let mut invalid = config(1);
    invalid.samples = 63;
    assert!(RtBridge::spawn_prepared_audio(
        ContextEcho,
        RtBridgeConfig::default(),
        invalid,
        context(Arc::new(HeapOutputAllocator))
    )
    .is_err());
    let mapped = pool(1);
    assert!(RtBridge::spawn_prepared_audio(
        ContextEcho,
        RtBridgeConfig::default(),
        config(2),
        context(Arc::new(mapped.clone()))
    )
    .is_err());
    assert!(
        mapped.allocate_audio(64).is_ok(),
        "failed setup must release partial preparation"
    );
}

struct InvalidProvider;
impl OutputAllocator for InvalidProvider {
    fn allocate_bytes(&self, _: usize) -> Result<BufferLoan<u8>, BufferAllocationError> {
        Err(BufferAllocationError::Unavailable)
    }
    fn allocate_audio(&self, _: usize) -> Result<BufferLoan<f32>, BufferAllocationError> {
        Ok(vec![0.0; 1].into())
    }
}

/// Opt the fixture in so the length check (not the capability check) fails.
fn opted_in_invalid() -> Arc<dyn OutputAllocator> {
    Arc::new(remotemedia_types::AssumePreparedRealtime(Arc::new(
        InvalidProvider,
    )))
}

#[test]
fn wrong_provider_length_is_rejected_before_capture() {
    let error = match RtBridge::spawn_prepared_audio(
        ContextEcho,
        RtBridgeConfig::default(),
        config(1),
        context(opted_in_invalid()),
    ) {
        Err(error) => error.to_string(),
        Ok(_) => panic!("wrong-length provider must be rejected"),
    };
    assert!(!error.contains("not validated"), "{error}");
}

/// Heap-backed, but its prepared real-time behaviour is not declared.
struct UndeclaredProvider;
impl OutputAllocator for UndeclaredProvider {
    fn allocate_bytes(&self, len: usize) -> Result<BufferLoan<u8>, BufferAllocationError> {
        HeapOutputAllocator.allocate_bytes(len)
    }
    fn allocate_audio(&self, samples: usize) -> Result<BufferLoan<f32>, BufferAllocationError> {
        HeapOutputAllocator.allocate_audio(samples)
    }
}

#[test]
fn unverified_provider_is_rejected_unless_explicitly_assumed() {
    let error = match RtBridge::spawn_prepared_audio(
        ContextEcho,
        RtBridgeConfig::default(),
        config(1),
        context(Arc::new(UndeclaredProvider)),
    ) {
        Err(error) => error.to_string(),
        Ok(_) => panic!("unverified provider must be rejected"),
    };
    assert!(
        error.contains("not validated for prepared real-time use"),
        "{error}"
    );

    // The explicit opt-in is accepted and still prepares through the provider.
    let (bridge, producer, consumer) = RtBridge::spawn_prepared_audio(
        ContextEcho,
        RtBridgeConfig::default(),
        config(1),
        context(Arc::new(remotemedia_types::AssumePreparedRealtime(
            Arc::new(UndeclaredProvider),
        ))),
    )
    .expect("assumed provider is accepted");
    drop((producer, consumer));
    bridge.shutdown();
}

#[test]
fn budgeted_binding_inherits_its_provider_support() {
    use remotemedia_types::{BoundOutputAllocator, PreparedRealtimeSupport};
    let verified = BoundOutputAllocator::new(Arc::new(HeapOutputAllocator));
    assert_eq!(
        verified.prepared_realtime_support(),
        PreparedRealtimeSupport::Verified
    );
    let unknown = BoundOutputAllocator::new(Arc::new(UndeclaredProvider));
    assert_eq!(
        unknown.prepared_realtime_support(),
        PreparedRealtimeSupport::Unverified
    );
}

struct ContextTransform;
impl SyncStreamingNode for ContextTransform {
    fn node_type(&self) -> &str {
        "ContextTransform"
    }
    fn process(&self, _: RuntimeData) -> crate::Result<RuntimeData> {
        panic!("context required")
    }
    fn process_with_ctx(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
    ) -> crate::Result<RuntimeData> {
        let RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            timestamp_us,
            ..
        } = data
        else {
            panic!("audio")
        };
        let mut output = ctx.allocate_output_audio(samples.len()).unwrap();
        for (out, input) in output.iter_mut().zip(samples.iter()) {
            *out = input * 2.0;
        }
        Ok(RuntimeData::Audio {
            samples: output.freeze().into(),
            sample_rate,
            channels,
            timestamp_us,
            stream_id: None,
            arrival_ts_us: None,
            metadata: None,
        })
    }
}

#[test]
fn transform_context_and_shutdown_preserve_mapped_provider() {
    let mapped = pool(4);
    let (bridge, mut producer, mut consumer) = RtBridge::spawn_prepared_audio(
        ContextTransform,
        RtBridgeConfig::default(),
        config(1),
        context(Arc::new(mapped.clone())),
    )
    .unwrap();
    callback(|| producer.try_fill(Some(88), |samples| samples.fill(0.25))).unwrap();
    let retained = until_ready(|| {
        callback(|| {
            consumer.try_read(|data| {
                let RuntimeData::Audio { samples, .. } = data else {
                    panic!("audio")
                };
                assert_eq!(samples.as_ref(), &[0.5; 64]);
                samples.clone().into_shared()
            })
        })
        .unwrap()
    });
    assert!(mapped.owns_buffer(&retained));
    bridge.shutdown(); // Also exercise early shutdown with callback handles alive.
    assert_eq!(
        callback(|| producer.try_fill(None, |_| panic!("stopped"))),
        Err(PreparedAudioError::Stopped)
    );
    assert_eq!(
        callback(|| consumer.try_read(|_| ())),
        Err(PreparedAudioError::Stopped)
    );
    drop(producer);
    drop(consumer);
    assert_eq!(retained.as_slice(), &[0.5; 64]);
    let other = (0..3)
        .map(|_| mapped.allocate_audio(64).unwrap())
        .collect::<Vec<_>>();
    assert!(mapped.allocate_audio(64).is_err());
    drop(retained);
    assert!(mapped.allocate_audio(64).is_ok());
    drop(other);
}
