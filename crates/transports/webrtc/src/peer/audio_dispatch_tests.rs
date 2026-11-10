use super::*;
use crate::media::audio::AudioEncoderConfig;
use remotemedia_types::{BufferBudget, CopyContext, CopyPolicy, CopyReason};
use std::time::Duration;
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;

async fn session_with_policy(policy: CopyPolicy) -> (Arc<SharedPipelineSession>, CopyContext) {
    let (_executor, handle, _control) = crate::test_support::core_session(policy).await;
    let copies = handle.transport_copy_context();
    let core = CoreSharedPipelineSession::new("audio-dispatch", handle).unwrap();
    let shared = SharedPipelineSession::new(core, None, false).await.unwrap();
    (shared, copies)
}

#[tokio::test]
async fn audio_owners_attached_dispatcher_preserves_session_policy_and_releases_inputs() {
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        let (shared, copies) = session_with_policy(policy).await;
        let registry = Arc::new(TrackRegistry::new("peer".into()));
        let track = Arc::new(
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
                    sample_rate: 48000,
                    channels: 1,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        registry
            .register_audio_track(DEFAULT_STREAM_ID.into(), track.clone())
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
        let budget = BufferBudget::new(2, 960 * 4);
        for value in [0.25, 0.5] {
            let data = budget
                .admit(RuntimeData::Audio {
                    samples: vec![value; 480].into(),
                    sample_rate: 48000,
                    channels: 1,
                    stream_id: None,
                    timestamp_us: None,
                    arrival_ts_us: None,
                    metadata: None,
                })
                .unwrap();
            dispatcher.dispatch(TransportData::new(data)).await;
        }
        wait_for_assembly_accounting(&copies, policy).await;
        shared.detach_peer("peer").await;
        drop(dispatcher);
        tokio::time::timeout(Duration::from_secs(5), track.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(budget.usage().owners, 0);
        shared.close().await.unwrap();
    }
}

async fn wait_for_assembly_accounting(copies: &CopyContext, policy: CopyPolicy) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let report = copies.report();
            let counts = report
                .entries
                .iter()
                .find(|(r, _)| *r == CopyReason::VecMaterialization);
            if let Some((_, counts)) = counts {
                if policy == CopyPolicy::RequireZeroCopy && counts.rejected_bytes == 480 * 4 {
                    assert_eq!(counts.copied_bytes, 0);
                    break;
                }
                if policy == CopyPolicy::AllowCopies && counts.copied_bytes == 960 * 4 {
                    assert_eq!(counts.rejected_bytes, 0);
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
