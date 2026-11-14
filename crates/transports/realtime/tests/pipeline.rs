//! The realtime socket joined to a real shared pipeline session (no stub):
//! two members of one room, a per-participant pass-through node standing in
//! for `RealtimeEventsNode`. It echoes what arrives on its `realtime` port,
//! which shows the path a frame takes to the member's node instance and
//! back, and that the transport keeps internal frames and other members'
//! frames off the socket.

mod common;

use common::*;
use remotemedia_core::data::{AudioSamples, RuntimeData};
use remotemedia_core::transport::session_control::ControlAddress;
use remotemedia_core::transport::TransportData;
use remotemedia_realtime::RealtimeConfig;
use remotemedia_webrtc::WebRtcTransportConfig;
use serde_json::json;
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_members_of_a_real_session_each_get_only_their_own_frames() {
    let state = shared_state(passthrough_manifest(), WebRtcTransportConfig::default());
    state.set_admission(Tickets::new(&[
        ("t-alice", member("alice")),
        ("t-bob", member("bob")),
    ]));
    remotemedia_realtime::mount(
        &state,
        RealtimeConfig {
            // The pass-through plays the events node.
            events_node: Some("echo".into()),
            pipeline_sample_rate: 16_000,
            ..Default::default()
        },
    );
    let addr = listen(state.clone()).await;
    let url = format!("ws://{addr}/realtime");

    let mut alice = connect_url(&url, &["remotemedia.realtime.v1", "remotemedia.ticket.t-alice"])
        .await
        .expect("alice is admitted");
    assert_eq!(alice.next().await["type"], "session-created");
    let mut bob = connect_url(&url, &["remotemedia.realtime.v1", "remotemedia.ticket.t-bob"])
        .await
        .expect("bob is admitted");
    assert_eq!(bob.next().await["type"], "session-created");

    // Both are in the room's one session, and can hear output.
    let room = state.rooms.lock().await.get("room").cloned().expect("the room's session");
    let control = state
        .runner
        .control_bus()
        .get(room.session_id())
        .expect("session control");
    let ready_by = Instant::now() + Duration::from_secs(5);
    while !(control.is_media_ready("alice") && control.is_media_ready("bob")) {
        assert!(Instant::now() < ready_by, "members are media-ready after joining");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // A client event reaches alice's node instance on its `realtime` port
    // and what the instance writes to that port comes back to alice, and to
    // nobody else. The node also echoes the transport's `joined` and
    // `released` frames: those never reach a socket.
    let event = json!({ "type": "response-create", "options": { "metadata": { "n": 1 } } });
    alice.send(event.clone()).await;
    assert_eq!(alice.next().await, event);
    assert!(bob.next_before(Instant::now() + Duration::from_millis(300)).await.is_none());

    // The member's audio enters the pipeline at the pipeline's rate, as
    // that member's input.
    let mut outputs = control
        .subscribe_attributed(&ControlAddress::node_out("echo"))
        .unwrap();
    for frame in sine_pcm16(24_000, 440.0, 4_800).chunks(480) {
        bob.send(json!({ "type": "input-audio-append", "audio": pcm16_base64(frame) }))
            .await;
    }
    let (who, rate) = loop {
        let (who, data) = tokio::time::timeout(Duration::from_secs(5), outputs.recv())
            .await
            .expect("bob's audio reached the pipeline")
            .unwrap();
        if let RuntimeData::Audio { sample_rate, .. } = data {
            break (who, sample_rate);
        }
    };
    assert_eq!(who.as_deref(), Some("bob"));
    assert_eq!(rate, 16_000);
    // Untagged audio (the echo of bob's own voice) is not an audio-delta.
    assert!(bob.next_before(Instant::now() + Duration::from_millis(300)).await.is_none());

    // A turn's audio for alice, as TTS output reaches the sink: tagged with
    // the turn and attributed to her.
    let speech = TransportData::new(RuntimeData::Audio {
        samples: AudioSamples::from(sine_f32(16_000, 440.0, 3_200)),
        sample_rate: 16_000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    })
    .with_participant("alice", "client")
    .with_metadata("participant.turn_id".into(), "t1".into())
    .with_metadata("participant.chunk".into(), "0".into());
    room.input_sender().send(speech).await.unwrap();

    let mut played_ms = 0.0;
    while let Some(frame) = alice.next_before(Instant::now() + Duration::from_millis(500)).await {
        assert_eq!(frame["type"], "audio-delta", "{frame}");
        assert_eq!(frame["responseId"], "t1");
        assert_eq!(frame["itemId"], "t1:assistant");
        played_ms += delta_ms(&frame, 24_000);
    }
    assert!((195.0..=200.0).contains(&played_ms), "200 ms of speech, got {played_ms} ms");
    assert!(
        bob.next_before(Instant::now() + Duration::from_millis(200)).await.is_none(),
        "alice's turn is not bob's"
    );

    // Hanging up is leaving: the member is no longer media-ready.
    alice.close().await;
    let gone_by = Instant::now() + Duration::from_secs(5);
    while control.is_media_ready("alice") {
        assert!(Instant::now() < gone_by, "alice left when her socket closed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(control.is_media_ready("bob"));
}
