use super::*;
use remotemedia_types::{BufferBudget, CopyPolicy};
use std::task::Poll;
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;

#[tokio::test]
async fn audio_stop_closed_track_rejects_partial_input_without_retaining_owner() {
    use remotemedia_types::{BoundOutputAllocator, HeapOutputAllocator, OutputAllocator};
    let track = track();
    let sender = track.sender.read().await.as_ref().unwrap().clone();
    sender.request_shutdown();
    let budget = BufferBudget::new(1, 4);
    let allocator =
        BoundOutputAllocator::new(Arc::new(HeapOutputAllocator)).with_budget(budget.clone());
    let input = allocator.allocate_audio(1).unwrap().freeze();
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "closed-track");
    assert!(track
        .send_audio_with_context(input, 48000, &copies)
        .await
        .is_err());
    assert_eq!(budget.usage().owners, 0);
    track.shutdown().await.unwrap();
    assert!(track.sender.read().await.is_none());
}

fn track() -> AudioTrack {
    AudioTrack::new(
        Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: "audio/opus".into(),
                ..Default::default()
            },
            "audio".into(),
            "stream".into(),
        )),
        AudioEncoderConfig {
            ring_buffer_capacity: 2,
            ..Default::default()
        },
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audio_stop_interrupts_backpressured_producer_and_releases_pcm_owner() {
    let track = track();
    let sender = track.sender.read().await.as_ref().unwrap().clone();
    sender
        .enqueue_frame(vec![0xf8, 0xff, 0xfe], 960, Duration::from_secs(2))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while sender.timestamp() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let budget = BufferBudget::new(1, 960 * 3 * 4);
    let RuntimeData::Audio { samples, .. } = budget
        .admit(RuntimeData::Audio {
            samples: vec![0.25; 960 * 3].into(),
            sample_rate: 48000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        })
        .unwrap()
    else {
        unreachable!()
    };
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "shutdown-owner");
    let mut producer =
        Box::pin(track.send_audio_with_context(samples.into_shared(), 48000, &copies));
    assert!(matches!(futures::poll!(&mut producer), Poll::Pending));
    assert_eq!(budget.usage().owners, 1);
    let stopped = tokio::time::timeout(Duration::from_millis(300), async {
        tokio::join!(producer.as_mut(), track.shutdown())
    })
    .await;
    // Cleanup also runs on the old implementation so the failing baseline
    // records a joined sender rather than leaving a transmission thread behind.
    drop(producer);
    sender.shutdown().await.unwrap();
    track.shutdown().await.unwrap();
    assert_eq!(budget.usage().owners, 0);
    let (production, shutdown) = stopped.expect("shutdown waited behind a backpressured producer");
    assert!(production.is_err());
    shutdown.unwrap();
    assert!(copies
        .report()
        .entries
        .iter()
        .all(|(_, counts)| counts.copied_bytes == 0));
}
