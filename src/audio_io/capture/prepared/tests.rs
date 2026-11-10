use super::*;
use remotemedia_types::{CopyContext, CopyPolicy, HeapOutputAllocator};

#[test]
fn handback_defers_partial_owner_release_and_accounting_off_callback_thread() {
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "capture/handback");
    let context = NodeRuntimeContext::for_test("capture", "mic").with_copy_context(copies.clone());
    let (worker, mut callback) = CaptureWorker::prepare(
        context,
        PreparedAudioConfig {
            samples: 64,
            sample_rate: 48000,
            channels: 2,
            ready_capacity: 4,
        },
        None,
    )
    .unwrap();
    std::thread::spawn(move || {
        crate::audio_io::capture::tests::allocations::callback(|| callback.capture(&[0.75; 14]));
        crate::audio_io::capture::tests::allocations::callback(|| drop(callback));
    })
    .join()
    .unwrap();
    assert_eq!(copies.report().entries[3].1.copied_bytes, 0);
    let (output, mut receiver) = mpsc::channel(4);
    worker.finish(&output).unwrap();
    let RuntimeData::Audio { samples, .. } = receiver.try_recv().unwrap() else {
        panic!("audio");
    };
    assert_eq!(samples.as_ref(), &[0.75; 14]);
    let count = copies.report().entries[3].1;
    assert_eq!(count.copied_bytes, 56);
    assert_eq!(count.in_flight, 0);
}

#[test]
fn full_output_queue_is_an_error_after_final_drain() {
    let context = NodeRuntimeContext::for_test("capture", "mic")
        .with_output_allocator(Arc::new(HeapOutputAllocator));
    let (worker, mut callback) = CaptureWorker::prepare(
        context,
        PreparedAudioConfig {
            samples: 64,
            sample_rate: 48000,
            channels: 2,
            ready_capacity: 4,
        },
        None,
    )
    .unwrap();
    callback.capture(&[1.0; 128]);
    drop(callback);
    let (output, _receiver) = mpsc::channel(1);
    let error = worker.finish(&output).unwrap_err().to_string();
    assert!(error.contains("output_packets=1"), "{error}");
}
