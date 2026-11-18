//! The media plan on signalling and no encoding for unreceived video
//! (openspec: extend-ai-sdk-media-streaming, tasks 1.3 and 5.3).
//!
//! The answer tests negotiate two real webrtc-rs peer connections: the
//! server registers its planned tracks as `ServerPeer` does, the client
//! offers receive-only sections as the browser client does.

use super::*;
use crate::config::WebRtcTransportConfig;
use crate::peer::connection::PeerConnection as WebRtcPeer;
use crate::peer::scanner::{
    answered_media_json, AnsweredMedia, AudioStreamSpec, MediaKind, MediaStreamPlan,
    VideoStreamSpec,
};
use webrtc::rtp_transceiver::rtp_codec::RTPCodecType;
use webrtc::rtp_transceiver::rtp_transceiver_direction::RTCRtpTransceiverDirection;
use webrtc::rtp_transceiver::RTCRtpTransceiverInit;

fn video(stream: &str) -> VideoStreamSpec {
    VideoStreamSpec {
        stream_id: stream.into(),
        width: 64,
        height: 64,
        framerate: 30,
    }
}

fn plan() -> MediaStreamPlan {
    MediaStreamPlan {
        audio_outputs: vec![AudioStreamSpec::default_named("default")],
        video_outputs: vec![video("avatar"), video("screen")],
    }
}

/// Register the plan's tracks on a server peer, in the order given, and
/// return them as `ServerPeer` records them.
async fn server(
    audio: &[AudioStreamSpec],
    videos: &[VideoStreamSpec],
) -> (WebRtcPeer, Vec<PlannedSender>) {
    let server = WebRtcPeer::new("server".into(), &WebRtcTransportConfig::default())
        .await
        .unwrap();
    let mut planned = Vec::new();
    for spec in audio {
        let (_, sender) = server
            .add_audio_track_with_stream_id(&spec.stream_id, Default::default())
            .await
            .unwrap();
        planned.push((MediaKind::Audio, spec.stream_id.clone(), sender));
    }
    for spec in videos {
        let cfg = crate::media::video::VideoEncoderConfig {
            width: spec.width,
            height: spec.height,
            framerate: spec.framerate,
            bitrate: 250_000,
            keyframe_interval: 60,
        };
        let (_, sender) = server
            .add_video_track_with_stream_id(&spec.stream_id, cfg)
            .await
            .unwrap();
        planned.push((MediaKind::Video, spec.stream_id.clone(), sender));
    }
    (server, planned)
}

/// The browser's offer: one receive-only audio section, `video_sections`
/// receive-only video sections, and the data channel.
async fn client_offer(video_sections: usize) -> String {
    let client = WebRtcPeer::new("client".into(), &WebRtcTransportConfig::default())
        .await
        .unwrap();
    let pc = client.peer_connection();
    let recvonly = || {
        Some(RTCRtpTransceiverInit {
            direction: RTCRtpTransceiverDirection::Recvonly,
            send_encodings: vec![],
        })
    };
    pc.add_transceiver_from_kind(RTPCodecType::Audio, recvonly())
        .await
        .unwrap();
    for _ in 0..video_sections {
        pc.add_transceiver_from_kind(RTPCodecType::Video, recvonly())
            .await
            .unwrap();
    }
    pc.create_data_channel("pipeline", None).await.unwrap();
    let offer = pc.create_offer(None).await.unwrap();
    pc.set_local_description(offer.clone()).await.unwrap();
    offer.sdp
}

async fn answer(server: &WebRtcPeer, planned: &[PlannedSender], offer: String) -> Vec<AnsweredMedia> {
    let pc = server.peer_connection();
    pc.set_remote_description(RTCSessionDescription::offer(offer).unwrap())
        .await
        .unwrap();
    let answer = pc.create_answer(None).await.unwrap();
    pc.set_local_description(answer).await.unwrap();
    answered_outputs(pc, planned).await
}

fn streams(media: &[AnsweredMedia], kind: MediaKind) -> Vec<String> {
    media
        .iter()
        .filter(|m| m.kind == kind)
        .map(|m| m.stream.clone())
        .collect()
}

#[test]
fn announce_lists_the_plan_with_the_default_audio_fallback() {
    let audio_only = MediaStreamPlan::default().with_default_audio();
    assert_eq!(
        audio_only.to_signaling_json(),
        serde_json::json!({ "v": 1, "audio": [{ "stream": "default" }], "video": [] })
    );
    let avatar = MediaStreamPlan {
        audio_outputs: vec![],
        video_outputs: vec![VideoStreamSpec {
            stream_id: "avatar".into(),
            width: 1024,
            height: 768,
            framerate: 25,
        }],
    }
    .with_default_audio();
    assert_eq!(
        avatar.to_signaling_json(),
        serde_json::json!({
            "v": 1,
            "audio": [{ "stream": "default" }],
            "video": [{ "stream": "avatar", "width": 1024, "height": 768, "framerate": 25 }]
        })
    );
}

#[test]
fn receive_list_orders_the_requested_outputs_first() {
    let planned = plan().video_outputs;
    let ids = |specs: Vec<VideoStreamSpec>| -> Vec<String> {
        specs.into_iter().map(|s| s.stream_id).collect()
    };
    assert_eq!(ids(order_video_outputs(&planned, None)), ["avatar", "screen"]);
    assert_eq!(
        ids(order_video_outputs(&planned, Some(&["screen".to_string()]))),
        ["screen", "avatar"]
    );
    assert_eq!(
        video_receive_from_params(&serde_json::json!({ "receive": { "video": ["screen"] } })),
        Some(vec!["screen".to_string()])
    );
    assert_eq!(
        video_receive_from_params(&serde_json::json!({ "receive": { "video": false } })),
        Some(vec![])
    );
    assert_eq!(video_receive_from_params(&serde_json::json!({ "sdp": "v=0" })), None);
    assert_eq!(
        video_receive_from_params(&serde_json::json!({ "receive": { "video": true } })),
        None
    );
}

#[tokio::test]
async fn two_video_outputs_answered_on_two_receive_only_sections() {
    let plan = plan();
    let (server, planned) = server(&plan.audio_outputs, &plan.video_outputs).await;
    let media = answer(&server, &planned, client_offer(2).await).await;
    assert_eq!(streams(&media, MediaKind::Audio), ["default"]);
    assert_eq!(streams(&media, MediaKind::Video), ["avatar", "screen"]);
    // Each output has a section of its own.
    let mids: std::collections::HashSet<_> = media.iter().map(|m| m.mid.clone()).collect();
    assert_eq!(mids.len(), 3);
    let json = answered_media_json(&media);
    assert_eq!(json[1]["kind"], "video");
    assert_eq!(json[1]["stream"], "avatar");
    assert!(json[1]["mid"].is_string());
}

#[tokio::test]
async fn an_offer_without_video_sections_answers_no_video() {
    let plan = plan();
    let (server, planned) = server(&plan.audio_outputs, &plan.video_outputs).await;
    let media = answer(&server, &planned, client_offer(0).await).await;
    assert_eq!(streams(&media, MediaKind::Audio), ["default"]);
    assert!(streams(&media, MediaKind::Video).is_empty());
}

#[tokio::test]
async fn one_video_section_answers_only_the_first_planned_output() {
    let plan = plan();
    let (server, planned) = server(&plan.audio_outputs, &plan.video_outputs).await;
    let media = answer(&server, &planned, client_offer(1).await).await;
    assert_eq!(streams(&media, MediaKind::Video), ["avatar"]);
}

#[tokio::test]
async fn a_requested_output_is_registered_first_and_answered() {
    let plan = plan();
    let wanted = ["screen".to_string()];
    let ordered = order_video_outputs(&plan.video_outputs, Some(&wanted));
    let (server, planned) = server(&plan.audio_outputs, &ordered).await;
    let media = answer(&server, &planned, client_offer(1).await).await;
    assert_eq!(streams(&media, MediaKind::Video), ["screen"]);
}

// ── No encoding for unreceived video (task 5.3) ──────────────────────────

#[tokio::test]
async fn video_is_encoded_only_for_the_member_that_receives_it() {
    use crate::media::tracks::video_copy_tests::{frame, track};
    use remotemedia_core::data::video::PixelFormat;
    use remotemedia_types::CopyPolicy;

    // Two members on one track plan: one offered video, the other did not.
    let receiving = track();
    let declined = track();
    receiving.set_receiving(true);
    declined.set_receiving(false);
    let copies = remotemedia_types::CopyContext::new(CopyPolicy::AllowCopies, "unreceived");
    for _ in 0..3 {
        receiving
            .send_video_runtime_data_with_context(frame(PixelFormat::Yuv420p), &copies)
            .await
            .unwrap();
        declined
            .send_video_runtime_data_with_context(frame(PixelFormat::Yuv420p), &copies)
            .await
            .unwrap();
    }
    assert_eq!(receiving.frames_unreceived(), 0);
    assert_eq!(declined.frames_unreceived(), 3);
    // Nothing reached the encoder for the member that declined.
    assert_eq!(declined.frames_encoded(), 0);
    assert_eq!(receiving.frames_encoded(), 3);
}

#[tokio::test]
async fn a_track_that_starts_being_received_asks_for_a_keyframe() {
    use crate::media::tracks::video_copy_tests::track;
    let t = track();
    assert_eq!(t.keyframes_requested(), 0);
    t.set_receiving(true);
    assert_eq!(t.keyframes_requested(), 1);
    t.set_receiving(false);
    assert_eq!(t.keyframes_requested(), 1);
}

#[test]
fn picture_loss_and_full_intra_request_ask_for_a_keyframe() {
    use webrtc::rtcp::payload_feedbacks::full_intra_request::FullIntraRequest;
    use webrtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
    use webrtc::rtcp::receiver_report::ReceiverReport;
    assert!(is_picture_loss(&PictureLossIndication::default()));
    assert!(is_picture_loss(&FullIntraRequest::default()));
    assert!(!is_picture_loss(&ReceiverReport::default()));
}

#[tokio::test]
async fn after_picture_loss_the_next_frame_is_a_keyframe() {
    use remotemedia_core::data::video::{PixelFormat, VideoCodec};
    use remotemedia_core::nodes::video::encoder::{VideoEncoderConfig, VideoEncoderNode};

    let encoder = VideoEncoderNode::new(VideoEncoderConfig {
        codec: VideoCodec::H264,
        bitrate: 250_000,
        framerate: 30,
        keyframe_interval: 300,
        hardware_accel: false,
        ..Default::default()
    })
    .unwrap();
    let frame = |n: u64| RuntimeData::Video {
        pixel_data: vec![(n * 7 % 255) as u8; 64 * 64 * 3 / 2].into(),
        width: 64,
        height: 64,
        format: PixelFormat::Yuv420p,
        codec: None,
        frame_number: n,
        timestamp_us: n * 33_333,
        is_keyframe: false,
        stream_id: None,
        arrival_ts_us: None,
    };
    let keyframe = |data: &RuntimeData| matches!(data, RuntimeData::Video { is_keyframe: true, .. });
    let encoded_size = |data: &RuntimeData| match data {
        RuntimeData::Video { pixel_data, .. } => pixel_data.len(),
        _ => 0,
    };

    // The stream starts with a keyframe, then predicted frames.
    let mut sizes = Vec::new();
    for n in 0..6 {
        let out = encoder.encode_frame(frame(n)).await.unwrap();
        sizes.push((keyframe(&out), encoded_size(&out)));
    }
    let later_keyframes = sizes[1..].iter().filter(|(k, size)| *k && *size > 0).count();
    // A keyframe is requested (RTCP PLI); the next frame is one.
    encoder.request_keyframe();
    let out = encoder.encode_frame(frame(6)).await.unwrap();
    assert!(
        keyframe(&out) || encoded_size(&out) == 0,
        "the frame after a keyframe request is not a keyframe (earlier: {sizes:?}, {later_keyframes} later keyframes)"
    );
}
