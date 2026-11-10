use super::*;
use remotemedia_types::{CopyCounts, CopyPolicy, CopyReason};
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;

pub(crate) const YUV_BYTES: u64 = 64 * 64 * 3 / 2;

pub(crate) fn frame(format: PixelFormat) -> RuntimeData {
    RuntimeData::Video {
        pixel_data: vec![
            128;
            if format == PixelFormat::Rgb24 {
                64 * 64 * 3
            } else {
                YUV_BYTES as usize
            }
        ]
        .into(),
        width: 64,
        height: 64,
        format,
        codec: None,
        frame_number: 0,
        timestamp_us: 33_333,
        is_keyframe: true,
        stream_id: None,
        arrival_ts_us: None,
    }
}

pub(crate) fn track() -> Arc<VideoTrack> {
    Arc::new(
        VideoTrack::new(
            Arc::new(TrackLocalStaticSample::new(
                RTCRtpCodecCapability {
                    mime_type: "video/H264".into(),
                    clock_rate: 90_000,
                    ..Default::default()
                },
                "video".into(),
                "stream".into(),
            )),
            VideoCodec::H264,
            64,
            64,
            250_000,
            30,
        )
        .unwrap(),
    )
}

pub(crate) fn counts(copies: &CopyContext) -> CopyCounts {
    copies
        .report()
        .entries
        .into_iter()
        .find(|(reason, _)| *reason == CopyReason::ForeignPoolIngress)
        .unwrap()
        .1
}

#[tokio::test]
async fn video_copy_track_preserves_policy_and_recovers_after_rejected_yuv() {
    let track = track();
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "video-strict");
    let allowed = CopyContext::new(CopyPolicy::AllowCopies, "video-allowed");
    let error = track
        .send_video_runtime_data_with_context(frame(PixelFormat::Yuv420p), &strict)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("video-strict"));
    assert_eq!(counts(&strict).rejected_bytes, YUV_BYTES);
    assert_eq!(
        track.frame_count.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert!(track.prev_pts_us.read().is_none());

    track
        .send_video_runtime_data_with_context(frame(PixelFormat::Yuv420p), &allowed)
        .await
        .unwrap();
    assert_eq!(
        (counts(&allowed).completed, counts(&allowed).copied_bytes),
        (1, YUV_BYTES)
    );
    // Color conversion computes directly into native planes; it is not a planar
    // ownership copy. A strict call after rejection must still be usable.
    track
        .send_video_runtime_data_with_context(frame(PixelFormat::Rgb24), &strict)
        .await
        .unwrap();
    assert_eq!(
        track.frame_count.load(std::sync::atomic::Ordering::Relaxed),
        2
    );
    assert_eq!(counts(&strict).rejected, 1);
    assert!(strict
        .report()
        .entries
        .iter()
        .all(|(_, c)| c.copied_bytes == 0));
}

#[tokio::test]
async fn video_copy_frame_router_uses_selected_authority() {
    use crate::media::{FrameRouter, TrackRegistry, DEFAULT_STREAM_ID};
    let registry = Arc::new(TrackRegistry::new("video-router".into()));
    registry
        .register_video_track(DEFAULT_STREAM_ID, track())
        .await
        .unwrap();
    for policy in [CopyPolicy::RequireZeroCopy, CopyPolicy::AllowCopies] {
        let copies = CopyContext::new(policy, "frame-router-video");
        let router = FrameRouter::new(registry.clone(), "video-router".into())
            .with_copy_context(copies.clone());
        let result = router.route(frame(PixelFormat::Yuv420p)).await;
        if policy == CopyPolicy::RequireZeroCopy {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("frame-router-video"));
            assert_eq!(counts(&copies).rejected_bytes, YUV_BYTES);
            assert_eq!(counts(&copies).copied_bytes, 0);
        } else {
            result.unwrap();
            assert_eq!(counts(&copies).copied_bytes, YUV_BYTES);
        }
    }
}
