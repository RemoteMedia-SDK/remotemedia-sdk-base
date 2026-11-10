use super::*;
use remotemedia_types::{BufferBudget, RuntimeData};
use std::task::Poll;

#[tokio::test]
async fn audio_stop_cancels_pending_transport_work_and_releases_its_owner() {
    let queue = AudioRingBuffer::new(2);
    let budget = BufferBudget::new(1, 32);
    let frame = owned_frame(&budget, 1);
    let mut work = Box::pin(queue.run_until_closed(async move {
        std::future::pending::<()>().await;
        drop(frame);
    }));
    assert!(matches!(futures::poll!(&mut work), Poll::Pending));
    assert_eq!(budget.usage().owners, 1);
    queue.signal_shutdown();
    assert!(tokio::time::timeout(Duration::from_secs(1), work)
        .await
        .unwrap()
        .is_none());
    assert_eq!(budget.usage().owners, 0);
    assert!(queue
        .run_until_closed(async { panic!("closed work must not run") })
        .await
        .is_none());
}

fn owned_frame(budget: &BufferBudget, value: u8) -> AudioFrame {
    let RuntimeData::Binary(owner) = budget
        .admit(RuntimeData::Binary(vec![value; 32].into()))
        .unwrap()
    else {
        unreachable!()
    };
    AudioFrame {
        data: bytes::Bytes::from_owner(owner),
        sample_count: 960,
        duration: Duration::from_millis(20),
    }
}

#[test]
fn audio_queue_owners_keep_identity_and_credit_through_pop_flush_and_shutdown() {
    let budget = BufferBudget::new(2, 64);
    let queue = AudioRingBuffer::new(3);
    let frame = owned_frame(&budget, 7);
    let pointer = frame.data.as_ptr();
    queue.try_push(frame).unwrap();
    queue.try_push(owned_frame(&budget, 9)).unwrap();
    let popped = queue.try_pop().unwrap();
    assert_eq!(popped.data.as_ptr(), pointer);
    let retained = popped.data.slice(1..8);
    let clone = popped.clone();
    assert_eq!(clone.data.as_ptr(), pointer);
    assert_eq!(queue.clear(), 1);
    queue.signal_shutdown();
    assert_eq!(budget.usage().owners, 1);
    assert_eq!(retained.as_ref(), &[7; 7]);
    drop((popped, clone));
    assert_eq!(budget.usage().bytes, 32);
    drop(retained);
    assert_eq!(budget.usage().owners, 0);
}

#[tokio::test]
async fn audio_queue_owners_backpressure_cancellation_and_clear_release_exact_owners() {
    let budget = BufferBudget::new(3, 96);
    let queue = AudioRingBuffer::new(2);
    queue.try_push(owned_frame(&budget, 1)).unwrap();
    let mut cancelled = Box::pin(queue.push(owned_frame(&budget, 2)));
    assert!(matches!(futures::poll!(&mut cancelled), Poll::Pending));
    assert_eq!(budget.usage().owners, 2);
    drop(cancelled);
    assert_eq!(budget.usage().owners, 1);
    let next = owned_frame(&budget, 3);
    let pointer = next.data.as_ptr();
    let mut waiting = Box::pin(queue.push(next));
    assert!(matches!(futures::poll!(&mut waiting), Poll::Pending));
    assert_eq!(queue.clear(), 1);
    tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(queue.try_pop().unwrap().data.as_ptr(), pointer);
    assert_eq!(budget.usage().owners, 0);
}

#[tokio::test]
async fn audio_queue_owners_shutdown_wakes_all_waiters_and_never_reopens() {
    let budget = BufferBudget::new(10, 320);
    let queue = AudioRingBuffer::new(2);
    queue.try_push(owned_frame(&budget, 0)).unwrap();
    let mut waiting = Vec::new();
    for value in 1..9 {
        let mut future = Box::pin(queue.push(owned_frame(&budget, value)));
        assert!(matches!(futures::poll!(&mut future), Poll::Pending));
        waiting.push(future);
    }
    queue.signal_shutdown();
    for future in waiting {
        assert!(tokio::time::timeout(Duration::from_secs(2), future)
            .await
            .unwrap()
            .is_err());
    }
    assert_eq!(budget.usage().owners, 0);
    assert!(queue.push(owned_frame(&budget, 9)).await.is_err());
    assert!(queue.try_push(owned_frame(&budget, 9)).is_err());
    assert_eq!(budget.usage().owners, 0);
    assert_eq!(queue.len(), 0);
}

#[test]
fn audio_queue_owners_concurrent_producers_consumer_and_flush_release_every_owner() {
    let queue = Arc::new(AudioRingBuffer::new(9));
    let budget = BufferBudget::new(10000, 320000);
    let barrier = Arc::new(std::sync::Barrier::new(6));
    std::thread::scope(|scope| {
        for value in 0..4 {
            let (queue, budget, barrier) = (queue.clone(), budget.clone(), barrier.clone());
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..1000 {
                    let _ = queue.try_push(owned_frame(&budget, value));
                }
            });
        }
        let (reader, start) = (queue.clone(), barrier.clone());
        scope.spawn(move || {
            start.wait();
            for _ in 0..4000 {
                if let Some(frame) = reader.try_pop() {
                    assert_eq!(frame.data.len(), 32);
                    assert!(frame.data.iter().all(|&v| v == frame.data[0]));
                }
            }
        });
        barrier.wait();
        for _ in 0..1000 {
            queue.clear();
        }
    });
    queue.signal_shutdown();
    assert_eq!(budget.usage().owners, 0);
    assert_eq!(budget.usage().bytes, 0);
}

fn sender() -> AudioSender {
    use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
    AudioSender::new(
        Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: "audio/opus".into(),
                ..Default::default()
            },
            "audio".into(),
            "stream".into(),
        )),
        3,
    )
}

#[tokio::test]
async fn audio_queue_owners_sender_immediate_shutdown_joins_and_last_drop_closes() {
    let sender = sender();
    let budget = BufferBudget::new(1, 32);
    let RuntimeData::Binary(owner) = budget
        .admit(RuntimeData::Binary(vec![1; 32].into()))
        .unwrap()
    else {
        unreachable!()
    };
    sender
        .enqueue_owned_frame(owner, 960, Duration::from_millis(20))
        .await
        .unwrap();
    // No runtime yield has occurred: the thread handle must already be installed.
    assert!(sender.thread_handle.try_lock().unwrap().is_some());
    sender.shutdown().await.unwrap();
    assert!(sender.thread_handle.lock().await.is_none());
    assert_eq!(budget.usage().owners, 0);
    let other = self::sender();
    let queue = other.buffer.clone();
    let handle = other.thread_handle.try_lock().unwrap().take().unwrap();
    let clone = other.clone();
    drop(other);
    assert!(!queue.shutdown.load(Ordering::Acquire));
    drop(clone);
    assert!(queue.shutdown.load(Ordering::Acquire));
    handle.join().unwrap();
}

#[test]
fn audio_queue_owners_degenerate_capacity_still_admits_one_frame() {
    for capacity in [0, 1, 2] {
        let budget = BufferBudget::new(2, 64);
        let queue = AudioRingBuffer::new(capacity);
        queue.try_push(owned_frame(&budget, 1)).unwrap();
        assert!(queue.try_push(owned_frame(&budget, 2)).is_err());
        assert_eq!(queue.len(), 1);
        queue.signal_shutdown();
        assert_eq!(budget.usage().owners, 0);
    }
}

#[test]
fn audio_queue_owners_concurrent_final_sender_drops_request_stop_once() {
    let sender = sender();
    let queue = sender.buffer.clone();
    let stopped = sender.shutdown.clone();
    let handle = sender.thread_handle.try_lock().unwrap().take().unwrap();
    let second = sender.clone();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    std::thread::scope(|scope| {
        let start = barrier.clone();
        scope.spawn(move || {
            start.wait();
            drop(sender);
        });
        barrier.wait();
        drop(second);
    });
    assert!(stopped.load(Ordering::Acquire));
    assert!(queue.shutdown.load(Ordering::Acquire));
    handle.join().unwrap();
}
