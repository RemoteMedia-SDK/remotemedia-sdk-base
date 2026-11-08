use super::*;
use crate::config::WebRtcTransportConfig;
use crate::media::tracks::video_copy_tests::{counts, frame, YUV_BYTES};
use crate::media::video::VideoEncoderConfig;
use crate::peer::PeerConnection;
use remotemedia_types::{CopyContext, CopyPolicy};

#[tokio::test]
async fn video_copy_session_router_retains_policy_through_legacy_frame_adapter() {
    let peer = Arc::new(
        PeerConnection::new("video-policy".into(), &WebRtcTransportConfig::default())
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
        .add_peer("video-policy".into(), peer.clone())
        .await
        .unwrap();
    for policy in [CopyPolicy::RequireZeroCopy, CopyPolicy::AllowCopies] {
        let copies = CopyContext::new(policy, "session-video");
        let router = SessionRouter::new(
            "video".into(),
            Arc::new(Session::new("video".into())),
            peers.clone(),
        )
        .unwrap()
        .with_copy_context(copies.clone());
        let result = router
            .send_to_peer("video-policy", &frame(PixelFormat::Yuv420p))
            .await;
        if policy == CopyPolicy::RequireZeroCopy {
            assert!(result.unwrap_err().to_string().contains("session-video"));
            assert_eq!(counts(&copies).rejected_bytes, YUV_BYTES);
            assert_eq!(counts(&copies).copied_bytes, 0);
        } else {
            result.unwrap();
            assert_eq!(counts(&copies).copied_bytes, YUV_BYTES);
        }
    }
    peer.close().await.unwrap();
}
