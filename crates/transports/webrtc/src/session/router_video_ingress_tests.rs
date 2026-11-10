use super::*;
use remotemedia_core::nodes::video::{VideoEncoderConfig as CoreConfig, VideoEncoderNode};
use remotemedia_types::{
    BoundOutputAllocator, BufferBudget, CopyReason, HeapOutputAllocator, SharedBytes,
};

#[tokio::test]
async fn video_ingress_router_preserves_sample_owners_policy_and_selected_output() {
    let encoder = VideoEncoderNode::new(CoreConfig {
        codec: remotemedia_core::data::video::VideoCodec::H264,
        hardware_accel: false,
        ..Default::default()
    })
    .unwrap();
    let RuntimeData::Video {
        pixel_data: packet, ..
    } = encoder
        .encode_frame_with_copy_context(
            frame(PixelFormat::Yuv420p),
            &CopyContext::new(CopyPolicy::AllowCopies, "fixture"),
        )
        .await
        .unwrap()
    else {
        panic!("expected encoded packet")
    };
    drop(encoder);
    assert!(!packet.is_empty());
    // This snapshot emulates a network-owned encoded sample, outside the measured path.
    let wire: SharedBytes = packet.as_slice().to_vec().into();
    let peer = Arc::new(
        PeerConnection::new("video-ingress".into(), &WebRtcTransportConfig::default())
            .await
            .unwrap(),
    );
    peer.add_video_track(VideoEncoderConfig {
        width: 64,
        height: 64,
        ..Default::default()
    })
    .await
    .unwrap();
    let peers = Arc::new(PeerManager::new(1).unwrap());
    peers
        .add_peer("video-ingress".into(), peer.clone())
        .await
        .unwrap();

    // Full native packet owners need no padding copy. Network owners need one;
    // a borrowed slice also needs an ownership snapshot before the async decoder.
    for kind in 0..3 {
        for policy in [CopyPolicy::RequireZeroCopy, CopyPolicy::AllowCopies] {
            let copies = CopyContext::new(policy, "video-ingress");
            let budget = BufferBudget::new(1, YUV_BYTES);
            let allocator = Arc::new(
                BoundOutputAllocator::new(Arc::new(HeapOutputAllocator))
                    .with_budget(budget.clone()),
            );
            let router = SessionRouter::new(
                "video".into(),
                Arc::new(Session::new("video".into())),
                peers.clone(),
            )
            .unwrap()
            .with_copy_context(copies.clone())
            .with_output_allocator(allocator.clone());
            let result = match kind {
                0 => {
                    router
                        .route_incoming_video_owned("video-ingress", packet.clone())
                        .await
                }
                1 => {
                    router
                        .route_incoming_video_owned("video-ingress", wire.clone())
                        .await
                }
                _ => router.route_incoming("video-ingress", &wire, false).await,
            };
            let report = copies.report();
            let foreign = report
                .entries
                .iter()
                .find(|(r, _)| *r == CopyReason::ForeignPoolIngress)
                .unwrap()
                .1;
            let borrowed = report
                .entries
                .iter()
                .find(|(r, _)| *r == CopyReason::BorrowedInput)
                .unwrap()
                .1;
            if policy == CopyPolicy::RequireZeroCopy {
                assert!(result.unwrap_err().to_string().contains("video-ingress"));
                assert_eq!(budget.usage().owners, 0);
                assert!(report.entries.iter().all(|(_, c)| c.copied_bytes == 0));
                if kind == 2 {
                    assert_eq!(borrowed.rejected_bytes, wire.len() as u64);
                    assert_eq!(foreign.rejected, 0);
                } else {
                    assert_eq!(
                        foreign.rejected_bytes,
                        if kind == 0 {
                            YUV_BYTES
                        } else {
                            wire.len() as u64
                        }
                    );
                    assert_eq!(borrowed.rejected, 0);
                }
                assert_eq!(router.metrics.read().await.video_frames_in, 0);
                continue;
            }
            let output = result.unwrap();
            assert!(allocator.owns_payload(&output));
            let RuntimeData::Video {
                pixel_data,
                width,
                height,
                codec,
                arrival_ts_us,
                ..
            } = output
            else {
                panic!("expected decoded frame")
            };
            assert_eq!((width, height, codec), (64, 64, None));
            assert!(arrival_ts_us.unwrap() > 0);
            assert_eq!(pixel_data.len() as u64, YUV_BYTES);
            assert!(pixel_data.iter().all(|&value| value == 128));
            assert_eq!(
                foreign.copied_bytes,
                YUV_BYTES + if kind > 0 { wire.len() as u64 } else { 0 }
            );
            assert_eq!(
                borrowed.copied_bytes,
                if kind == 2 { wire.len() as u64 } else { 0 }
            );
            assert_eq!(router.metrics.read().await.video_frames_in, 1);
            if kind == 0 {
                let retained = pixel_data.slice(0..16).unwrap();
                drop(pixel_data);
                assert!(router
                    .route_incoming_video_owned("video-ingress", packet.clone())
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("output allocat"));
                assert_eq!(counts(&copies).failed, 1);
                assert_eq!(budget.usage().bytes, YUV_BYTES);
                drop(retained);
                let retry = router
                    .route_incoming_video_owned("video-ingress", packet.clone())
                    .await
                    .unwrap();
                assert!(allocator.owns_payload(&retry));
                let RuntimeData::Video { pixel_data, .. } = retry else {
                    unreachable!()
                };
                let retained = pixel_data.slice(0..16).unwrap();
                drop((pixel_data, router, allocator));
                assert_eq!(budget.usage().bytes, YUV_BYTES);
                std::thread::spawn(move || assert!(retained.iter().all(|&v| v == 128)))
                    .join()
                    .unwrap();
                assert_eq!(budget.usage().owners, 0);
            } else {
                drop(pixel_data);
                assert_eq!(budget.usage().owners, 0);
            }
        }
    }
    peer.close().await.unwrap();
}
