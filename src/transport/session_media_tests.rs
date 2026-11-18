//! Media readiness, targeted flush and injected speech on `SessionControl`
//! (openspec: add-ai-sdk-voice-library, tasks 2.1, 2.3, 2.3a).
use super::*;
use crate::transport::data::participant;
use crate::transport::turn_audio::{keys, TurnTag};

fn media_events(rx: &mut broadcast::Receiver<RuntimeData>) -> Vec<MediaEvent> {
    let mut events = Vec::new();
    while let Ok(frame) = rx.try_recv() {
        events.extend(MediaEvent::from_runtime(&frame));
    }
    events
}

fn ready_count(events: &[MediaEvent], who: &str) -> usize {
    events
        .iter()
        .filter(|e| matches!(e, MediaEvent::MediaReady { member, .. } if member == who))
        .count()
}

#[tokio::test]
async fn media_ready_is_published_once_per_member_and_again_after_reconnect() {
    let control = SessionControl::new("media");
    let mut rx = control.subscribe_media();

    assert!(control.publish_media_ready("alice", "webrtc", "c1"));
    // The same connection reporting again publishes nothing.
    assert!(!control.publish_media_ready("alice", "webrtc", "c1"));
    assert!(control.publish_media_ready("bob", "telephony", "call-7"));
    let events = media_events(&mut rx);
    assert_eq!(ready_count(&events, "alice"), 1);
    assert_eq!(ready_count(&events, "bob"), 1);
    assert_eq!(events.len(), 2);
    assert!(control.is_media_ready("alice"));

    // Lost, then ready again on the same connection (ICE restart).
    assert!(control.publish_media_lost("alice", "webrtc", "c1"));
    assert!(!control.is_media_ready("alice"));
    assert!(control.publish_media_ready("alice", "webrtc", "c1"));

    // A new connection replaces the old one without a loss in between, and
    // the old connection's late loss does not un-ready the member.
    assert!(control.publish_media_ready("alice", "webrtc", "c2"));
    assert!(!control.publish_media_lost("alice", "webrtc", "c1"));
    assert!(control.is_media_ready("alice"));
    let events = media_events(&mut rx);
    assert_eq!(ready_count(&events, "alice"), 2);
    assert_eq!(events.len(), 3, "{events:?}");

    let mut members = control.media_ready_members();
    members.sort();
    assert_eq!(members, ["alice", "bob"]);
}

#[tokio::test]
async fn targeted_flush_reaches_its_hooks_and_leaves_the_session_wide_flush_alone() {
    let control = SessionControl::new("flush");
    let target = FlushTarget {
        members: vec!["alice".into()],
        turn_id: "t1".into(),
        drop_rest: true,
    };
    // No transport: both report that nothing took the request.
    assert!(!control.request_flush_audio_for(target.clone()));
    assert!(!control.request_flush_audio().await);

    let (wide_tx, mut wide_rx) = mpsc::channel::<()>(4);
    control.install_flush_audio_hook(wide_tx).await;
    let (webrtc_tx, mut webrtc_rx) = mpsc::channel::<FlushTarget>(4);
    let (telephony_tx, mut telephony_rx) = mpsc::channel::<FlushTarget>(4);
    control.install_flush_audio_for_hook(webrtc_tx);
    control.install_flush_audio_for_hook(telephony_tx);

    // A targeted request goes to every transport's hook and to no
    // session-wide hook.
    assert!(control.request_flush_audio_for(target.clone()));
    assert_eq!(webrtc_rx.try_recv().unwrap(), target);
    assert_eq!(telephony_rx.try_recv().unwrap(), target);
    assert!(wide_rx.try_recv().is_err());

    // The session-wide flush (ConversationCoordinator's barge) behaves as
    // before: one ping on its own hook, nothing targeted.
    assert!(control.request_flush_audio().await);
    assert!(wide_rx.try_recv().is_ok());
    assert!(webrtc_rx.try_recv().is_err());
    assert!(telephony_rx.try_recv().is_err());

    // A transport that went away is skipped.
    drop(webrtc_rx);
    assert!(control.request_flush_audio_for(target.clone()));
    assert_eq!(telephony_rx.try_recv().unwrap(), target);
}

fn speech(text: &str, to: &[&str]) -> SpeechRequest {
    SpeechRequest {
        node: "tts".into(),
        text: text.into(),
        to: to.iter().map(|m| m.to_string()).collect(),
        turn_id: None,
        wait_for_media: true,
    }
}

fn text_of(packet: &DataPacket) -> &str {
    match &packet.data {
        RuntimeData::Text(text) => text,
        other => panic!("speech is text, got {}", other.data_type()),
    }
}

#[tokio::test]
async fn speech_is_queued_until_media_ready_and_enters_tts_with_a_turn_id() {
    let (tx, mut rx) = mpsc::channel::<DataPacket>(8);
    let control = SessionControl::new("speak");
    control.attach_input_sender(tx).await;

    // Alice is not connected yet: both greetings wait, in order.
    let first = control
        .speak(speech("Hi Alice.", &["alice"]))
        .await
        .unwrap();
    let second = control
        .speak(speech("How can I help?", &["alice"]))
        .await
        .unwrap();
    assert!(first.queued && second.queued);
    assert_ne!(first.turn_id, second.turn_id);
    assert!(
        rx.try_recv().is_err(),
        "nothing reaches TTS before media_ready"
    );

    // Bob becoming ready does not release speech for Alice.
    control.publish_media_ready("bob", "webrtc", "c-bob");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(rx.try_recv().is_err());

    control.publish_media_ready("alice", "webrtc", "c-alice");
    let packet = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(text_of(&packet), "Hi Alice.");
    assert_eq!(packet.to_node.as_deref(), Some("tts"));
    // Tagged like a turn and addressed to its listener: the router carries
    // `participant.*` to the TTS audio, and the sender reports and flushes
    // it by this turn id.
    assert_eq!(
        TurnTag::from_metadata(&packet.metadata),
        Some(TurnTag::new(&first.turn_id, 0))
    );
    assert_eq!(packet.metadata[keys::TURN_ID], first.turn_id);
    assert_eq!(packet.metadata[participant::TO], "alice");
    assert_eq!(packet.metadata[participant::ID], "alice");
    let packet = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(text_of(&packet), "How can I help?");
    assert_eq!(packet.metadata[keys::TURN_ID], second.turn_id);

    // Ready members hear speech at once, under the caller's turn id.
    let mut request = speech("One more thing.", &["alice", "bob"]);
    request.turn_id = Some("greeting-3".into());
    let third = control.speak(request).await.unwrap();
    assert_eq!(
        (third.turn_id.as_str(), third.queued),
        ("greeting-3", false)
    );
    let packet = rx.try_recv().unwrap();
    assert_eq!(packet.metadata[participant::TO], "alice,bob");
    assert_eq!(packet.metadata[keys::TURN_ID], "greeting-3");
}

#[tokio::test]
async fn speech_through_the_control_bus_port_is_the_same_request() {
    let (tx, mut rx) = mpsc::channel::<DataPacket>(8);
    let control = SessionControl::new("speak-port");
    control.attach_input_sender(tx).await;
    control.publish_media_ready("alice", "webrtc", "c1");

    // What a control client sends: a publish to `tts.in.__speak__`.
    let addr = ControlAddress::node_in("tts").with_port(SPEAK_PORT);
    control
        .publish(
            &addr,
            RuntimeData::Json(serde_json::json!({
                "text": "Hello.", "to": ["alice"], "turn_id": "hello-1"
            })),
        )
        .await
        .unwrap();
    let packet = rx.try_recv().unwrap();
    assert_eq!(text_of(&packet), "Hello.");
    assert_eq!(packet.to_node.as_deref(), Some("tts"));
    assert_eq!(packet.metadata[keys::TURN_ID], "hello-1");
    assert_eq!(packet.metadata[participant::TO], "alice");

    // Bare text speaks to everyone, with a generated turn id.
    control
        .publish(&addr, RuntimeData::Text("Welcome, all.".into()))
        .await
        .unwrap();
    let packet = rx.try_recv().unwrap();
    assert_eq!(text_of(&packet), "Welcome, all.");
    assert!(packet.metadata[keys::TURN_ID].starts_with("speak-"));
    assert!(!packet.metadata.contains_key(participant::TO));

    // `wait_for_media: false` does not wait for anyone.
    control
        .publish(
            &addr,
            RuntimeData::Json(serde_json::json!({
                "text": "Now.", "to": ["nobody-yet"], "wait_for_media": false
            })),
        )
        .await
        .unwrap();
    assert_eq!(text_of(&rx.try_recv().unwrap()), "Now.");
}

#[tokio::test]
async fn speech_leaves_the_pipeline_with_its_turn_and_audience() {
    use crate::manifest::{Manifest, NodeManifest};
    // A pass-through stands in for TTS: what matters is that the router
    // carries the tag from the injected text to the node's output.
    let manifest = Arc::new(Manifest {
        nodes: vec![NodeManifest {
            id: "tts".into(),
            node_type: "PassThrough".into(),
            params: serde_json::json!({}),
            ..Default::default()
        }],
        ..Default::default()
    });
    let executor = crate::transport::PipelineExecutor::with_config(Default::default()).unwrap();
    let mut session = executor.create_session(manifest).await.unwrap();
    let control = executor.control_bus().get(&session.session_id).unwrap();

    let mut request = speech("Hello.", &["alice"]);
    request.turn_id = Some("t9".into());
    request.wait_for_media = false;
    control.speak(request).await.unwrap();

    let output = tokio::time::timeout(Duration::from_secs(10), session.recv_output())
        .await
        .expect("no output")
        .unwrap()
        .expect("session ended");
    assert!(matches!(&output.data, RuntimeData::Text(text) if text == "Hello."));
    assert_eq!(
        TurnTag::from_metadata(&output.metadata),
        Some(TurnTag::new("t9", 0))
    );
    assert_eq!(output.metadata[participant::TO], "alice");
}
