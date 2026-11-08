use super::*;
use crate::media::tracks::video_copy_tests::{counts, frame, track, YUV_BYTES};
use remotemedia_core::data::video::PixelFormat;
use remotemedia_types::{BufferBudget, CopyPolicy};
use tokio::time::{timeout, Duration};

#[tokio::test]
async fn video_copy_attached_dispatcher_uses_core_authority_and_releases_source() {
    for policy in [CopyPolicy::RequireZeroCopy, CopyPolicy::AllowCopies] {
        let (_executor, handle, _control) = crate::test_support::core_session(policy).await;
        let copies = handle.transport_copy_context();
        let core = CoreSharedPipelineSession::new("video-dispatch", handle).unwrap();
        let shared = SharedPipelineSession::new(core, None, false).await.unwrap();
        let registry = Arc::new(TrackRegistry::new("peer".into()));
        registry
            .register_video_track(DEFAULT_STREAM_ID, track())
            .await
            .unwrap();
        shared
            .attach_peer(
                "peer".into(),
                registry,
                Arc::new(RwLock::new(None)),
                #[cfg(feature = "ws-signaling")]
                None,
            )
            .await;
        let dispatcher = shared.output_targets.read().await["peer"]
            .dispatcher
            .clone();
        assert!(dispatcher.copies.same_authority(&copies));
        let budget = BufferBudget::new(1, YUV_BYTES);
        let data = budget.admit(frame(PixelFormat::Yuv420p)).unwrap();
        dispatcher.dispatch(TransportData::new(data)).await;
        timeout(Duration::from_secs(10), async {
            loop {
                let c = counts(&copies);
                if c.completed + c.rejected == 1 && budget.usage().owners == 0 {
                    if policy == CopyPolicy::RequireZeroCopy {
                        assert_eq!((c.rejected_bytes, c.copied_bytes), (YUV_BYTES, 0));
                    } else {
                        assert_eq!((c.copied_bytes, c.rejected_bytes), (YUV_BYTES, 0));
                    }
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        shared.detach_peer("peer").await;
        drop(dispatcher);
        shared.close().await.unwrap();
        assert_eq!(budget.usage().owners, 0);
    }
}
