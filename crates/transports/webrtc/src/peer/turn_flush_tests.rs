//! Targeted flush and release reports through a shared WebRTC session
//! (openspec: add-ai-sdk-voice-library, tasks 2.2 and 2.3a).
use super::*;
use crate::media::audio::AudioEncoderConfig;
use remotemedia_core::transport::turn_audio::{keys, FlushTarget, MediaEvent};
use remotemedia_types::CopyPolicy;
use std::time::Duration;
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;

struct Room {
    shared: Arc<SharedPipelineSession>,
    control: Arc<SessionControl>,
    tracks: HashMap<&'static str, Arc<AudioTrack>>,
}

/// A shared session with members `alice` and `bob`, each with one audio track.
async fn room() -> Room {
    let (_executor, handle, control) = crate::test_support::core_session(CopyPolicy::AllowCopies).await;
    let core = CoreSharedPipelineSession::new("turn-flush", handle).unwrap();
    let shared = SharedPipelineSession::new(core, Some(control.clone()), false)
        .await
        .unwrap();
    let mut tracks = HashMap::new();
    for member in ["alice", "bob"] {
        let registry = Arc::new(TrackRegistry::new(member.into()));
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
        track
            .set_release_reporter(crate::media::audio_sender::ReleaseReporter::session(
                control.clone(),
                member,
                DEFAULT_STREAM_ID,
            ))
            .await;
        registry
            .register_audio_track(DEFAULT_STREAM_ID, track.clone())
            .await
            .unwrap();
        shared
            .attach_peer(
                member.into(),
                registry,
                Arc::new(RwLock::new(None)),
                #[cfg(feature = "ws-signaling")]
                None,
            )
            .await;
        tracks.insert(member, track);
    }
    Room {
        shared,
        control,
        tracks,
    }
}

/// `ms` of TTS audio of a turn, optionally addressed, as the router emits it.
fn turn_audio(turn: &str, chunk: u64, ms: usize, to: Option<&str>) -> TransportData {
    let mut output = TransportData::new(RuntimeData::Audio {
        samples: vec![0.1f32; 48 * ms].into(),
        sample_rate: 48000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    });
    output.metadata.insert(keys::TURN_ID.into(), turn.into());
    output.metadata.insert(keys::CHUNK.into(), chunk.to_string());
    if let Some(to) = to {
        output.metadata.insert(participant::TO.into(), to.into());
    }
    output
}

impl Room {
    async fn dispatch(&self, member: &str, output: TransportData) {
        let dispatcher = self.shared.output_targets.read().await[member]
            .dispatcher
            .clone();
        dispatcher.dispatch(output).await;
    }

    async fn queued(&self, member: &str, turn: &str) -> usize {
        self.tracks[member].queued_frames_for_turn(Some(turn)).await
    }

    /// Wait until the member's consumer task has queued `frames` of `turn`.
    async fn wait_queued(&self, member: &str, turn: &str, frames: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.queued(member, turn).await < frames {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{member} never queued {frames} frames of {turn}"));
    }

    async fn close(self) {
        for track in self.tracks.values() {
            track.shutdown().await.unwrap();
        }
        self.shared.close().await.unwrap();
    }
}

#[tokio::test]
async fn flushing_member_a_for_turn_t_leaves_b_and_a_later_turn_intact() {
    let room = room().await;
    for member in ["alice", "bob"] {
        room.dispatch(member, turn_audio("t", 0, 400, None)).await;
    }
    room.dispatch("alice", turn_audio("u", 0, 400, None)).await;
    room.wait_queued("alice", "u", 20).await;
    room.wait_queued("bob", "t", 8).await;
    let bob_before = room.queued("bob", "t").await;

    assert!(room.control.request_flush_audio_for(FlushTarget {
        members: vec!["alice".into()],
        turn_id: "t".into(),
        drop_rest: true,
    }));
    tokio::time::timeout(Duration::from_secs(5), async {
        while room.queued("alice", "t").await > 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("alice's frames of t were not flushed");
    // Intact; it is next in line, so its first frames may have been sent.
    let later = room.queued("alice", "u").await;
    assert!((17..=20).contains(&later), "alice's later turn: {later}");
    assert!(room.queued("bob", "t").await + 3 >= bob_before, "bob's queue");

    // The rest of t is dropped for alice and still plays for bob.
    for member in ["alice", "bob"] {
        room.dispatch(member, turn_audio("t", 1, 200, None)).await;
    }
    room.dispatch("alice", turn_audio("v", 0, 100, None)).await;
    room.wait_queued("alice", "v", 5).await;
    assert_eq!(room.queued("alice", "t").await, 0);
    assert!(room.queued("bob", "t").await >= 10);
    room.close().await;
}

#[tokio::test]
async fn an_empty_member_list_flushes_the_turn_for_every_member() {
    let room = room().await;
    for member in ["alice", "bob"] {
        room.dispatch(member, turn_audio("t", 0, 400, None)).await;
        room.dispatch(member, turn_audio("u", 0, 200, None)).await;
        room.wait_queued(member, "u", 10).await;
    }
    assert!(room.control.request_flush_audio_for(FlushTarget {
        members: Vec::new(),
        turn_id: "t".into(),
        drop_rest: false,
    }));
    tokio::time::timeout(Duration::from_secs(5), async {
        while room.queued("alice", "t").await + room.queued("bob", "t").await > 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("t was not flushed for everyone");
    for member in ["alice", "bob"] {
        let later = room.queued(member, "u").await;
        assert!((7..=10).contains(&later), "{member}'s later turn: {later}");
    }
    room.close().await;
}

#[tokio::test]
async fn session_wide_flush_behaves_as_before() {
    let room = room().await;
    for member in ["alice", "bob"] {
        room.dispatch(member, turn_audio("t", 0, 400, None)).await;
        room.dispatch(member, turn_audio("u", 0, 200, None)).await;
        room.wait_queued(member, "u", 10).await;
    }
    // What ConversationCoordinator calls on a barge.
    assert!(room.control.request_flush_audio().await);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut left = 0;
            for member in ["alice", "bob"] {
                left += room.queued(member, "t").await + room.queued(member, "u").await;
            }
            if left == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("the session-wide flush drops every member's every turn");
    room.close().await;
}

#[tokio::test]
async fn released_audio_is_reported_per_member_on_the_session_bus() {
    let room = room().await;
    let mut events = room.control.subscribe_media();
    room.dispatch("alice", turn_audio("t", 0, 200, Some("alice"))).await;
    let report = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let event = events.recv().await.unwrap();
            if let Some(MediaEvent::AudioReleased(report)) = MediaEvent::from_runtime(&event) {
                if report.released_ms >= 100 {
                    return report;
                }
            }
        }
    })
    .await
    .expect("no audio_released event");
    assert_eq!(report.member, "alice");
    assert_eq!(report.turn_id, "t");
    assert_eq!(report.stream_id.as_deref(), Some(DEFAULT_STREAM_ID));
    room.close().await;
}
