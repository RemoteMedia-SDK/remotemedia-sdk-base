//! `media_ready` over real peer connections on this host
//! (openspec: add-ai-sdk-voice-library, tasks 2.1 and 2.4).
use super::*;
use remotemedia_core::manifest::NodeManifest;
use remotemedia_core::transport::turn_audio::MediaEvent;
use remotemedia_types::CopyPolicy;
use std::time::Duration;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::APIBuilder;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtp_transceiver::rtp_codec::RTPCodecType;

/// A browser-like client: one audio transceiver, a data channel, a complete
/// (non-trickle) offer.
async fn client() -> RTCPeerConnection {
    let mut media = MediaEngine::default();
    media.register_default_codecs().unwrap();
    let rtc = APIBuilder::new()
        .with_media_engine(media)
        .build()
        .new_peer_connection(Default::default())
        .await
        .unwrap();
    rtc.add_transceiver_from_kind(RTPCodecType::Audio, None)
        .await
        .unwrap();
    rtc.create_data_channel("data", None).await.unwrap();
    rtc
}

/// Connect a new client to a new server peer for `member` on the session.
async fn connect(
    member: &str,
    config: &WebRtcTransportConfig,
    executor: Arc<remotemedia_core::transport::PipelineExecutor>,
    manifest: Arc<Manifest>,
    shared: Arc<SharedPipelineSession>,
) -> (Arc<ServerPeer>, RTCPeerConnection) {
    let peer = Arc::new(
        ServerPeer::new(member.into(), config, executor, manifest)
            .await
            .unwrap(),
    );
    let rtc = client().await;
    let mut gathered = rtc.gathering_complete_promise().await;
    rtc.set_local_description(rtc.create_offer(None).await.unwrap())
        .await
        .unwrap();
    gathered.recv().await;
    let offer = rtc.local_description().await.unwrap().sdp;
    peer.handle_offer_with_shared_session(offer, shared)
        .await
        .unwrap();
    // The answer with the server's candidates, as trickle would deliver them.
    let server = peer.peer_connection().peer_connection();
    let mut gathered = server.gathering_complete_promise().await;
    gathered.recv().await;
    rtc.set_remote_description(server.local_description().await.unwrap())
        .await
        .unwrap();
    (peer, rtc)
}

/// Collect `media_ready` / `media_lost` events until `done` holds.
async fn events_until(
    events: &mut tokio::sync::broadcast::Receiver<RuntimeData>,
    seen: &mut Vec<(String, String)>,
    done: impl Fn(&[(String, String)]) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !done(seen) {
            match MediaEvent::from_runtime(&events.recv().await.unwrap()) {
                Some(MediaEvent::MediaReady { member, .. }) => seen.push(("ready".into(), member)),
                Some(MediaEvent::MediaLost { member, .. }) => seen.push(("lost".into(), member)),
                _ => {}
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out; media events so far: {seen:?}"));
}

fn count(seen: &[(String, String)], kind: &str, member: &str) -> usize {
    seen.iter()
        .filter(|(k, m)| k == kind && m == member)
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn media_ready_fires_once_per_member_and_again_after_reconnect() {
    let manifest = Arc::new(Manifest {
        nodes: vec![NodeManifest {
            id: "pass".into(),
            node_type: "PassThrough".into(),
            params: serde_json::json!({}),
            ..Default::default()
        }],
        ..Default::default()
    });
    let (executor, handle, control) =
        crate::test_support::core_session_with_manifest(CopyPolicy::AllowCopies, manifest.clone())
            .await;
    let executor = Arc::new(executor);
    let core = CoreSharedPipelineSession::new("media-ready", handle).unwrap();
    let shared = SharedPipelineSession::new(core, Some(control.clone()), false)
        .await
        .unwrap();
    let config = WebRtcTransportConfig {
        stun_servers: Vec::new(),
        turn_servers: Vec::new(),
        ..Default::default()
    };
    let mut events = control.subscribe_media();
    let mut seen = Vec::new();

    // Not ready before anyone is connected.
    assert!(control.media_ready_members().is_empty());

    let (alice_peer, alice) =
        connect("alice", &config, executor.clone(), manifest.clone(), shared.clone()).await;
    events_until(&mut events, &mut seen, |s| count(s, "ready", "alice") >= 1).await;
    assert!(control.is_media_ready("alice"));

    let (bob_peer, bob) =
        connect("bob", &config, executor.clone(), manifest.clone(), shared.clone()).await;
    events_until(&mut events, &mut seen, |s| count(s, "ready", "bob") >= 1).await;

    // Once per member while they stay connected.
    tokio::time::sleep(Duration::from_millis(500)).await;
    while let Ok(event) = events.try_recv() {
        if let Some(MediaEvent::MediaReady { member, .. }) = MediaEvent::from_runtime(&event) {
            seen.push(("ready".into(), member));
        }
    }
    assert_eq!(count(&seen, "ready", "alice"), 1, "{seen:?}");
    assert_eq!(count(&seen, "ready", "bob"), 1, "{seen:?}");

    // Alice drops and comes back on a new connection.
    alice.close().await.unwrap();
    alice_peer.shutdown().await.unwrap();
    events_until(&mut events, &mut seen, |s| count(s, "lost", "alice") >= 1).await;
    assert!(!control.is_media_ready("alice"));
    assert!(control.is_media_ready("bob"));

    let (alice_peer, alice) =
        connect("alice", &config, executor.clone(), manifest.clone(), shared.clone()).await;
    events_until(&mut events, &mut seen, |s| count(s, "ready", "alice") >= 2).await;
    assert_eq!(count(&seen, "ready", "alice"), 2, "{seen:?}");
    assert_eq!(count(&seen, "ready", "bob"), 1, "{seen:?}");
    assert!(control.is_media_ready("alice"));

    for (peer, rtc) in [(alice_peer, alice), (bob_peer, bob)] {
        rtc.close().await.unwrap();
        peer.shutdown().await.unwrap();
    }
    shared.close().await.unwrap();
}
