//! Spoken-so-far reports and targeted flush on one audio track
//! (openspec: add-ai-sdk-voice-library, tasks 2.2 and 2.3a).
use super::*;
use crate::media::audio_sender::ReleaseReporter;
use remotemedia_core::transport::turn_audio::{AudioReleased, TurnTag};
use std::sync::Mutex;
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;

type Reports = Arc<Mutex<Vec<AudioReleased>>>;

pub(crate) fn track() -> Arc<AudioTrack> {
    Arc::new(
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
    )
}

async fn reporting_track(member: &str) -> (Arc<AudioTrack>, Reports) {
    let track = track();
    let reports: Reports = Default::default();
    let sink = reports.clone();
    track
        .set_release_reporter(ReleaseReporter::new(member, "default", move |report| {
            sink.lock().unwrap().push(report)
        }))
        .await;
    (track, reports)
}

/// `ms` of 48 kHz mono audio tagged with a turn and chunk.
pub(crate) async fn speak(track: &AudioTrack, turn: &str, chunk: u64, ms: usize) {
    track
        .send_audio_tagged(
            vec![0.1f32; 48 * ms].into(),
            48000,
            CopyContext::legacy(),
            Some(TurnTag::new(turn, chunk)),
        )
        .await
        .unwrap();
}

async fn wait_for(reports: &Reports, what: impl Fn(&AudioReleased) -> bool) -> AudioReleased {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(found) = reports.lock().unwrap().iter().find(|r| what(r)) {
                return found.clone();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("no matching audio_released report")
}

#[tokio::test]
async fn interrupted_turn_reports_a_strict_prefix_and_no_queued_frames() {
    let (track, reports) = reporting_track("alice").await;
    // Three text chunks of 400 ms each: 1.2 s queued, sent at real time.
    for chunk in 0..3 {
        speak(&track, "t1", chunk, 400).await;
    }
    // Let part of the turn leave: into the second chunk, not past it.
    wait_for(&reports, |r| r.released_ms >= 440).await;
    let dropped = track.flush_turn("t1", true).await;
    assert!(dropped > 0, "frames were still queued at the flush");

    let after = wait_for(&reports, |r| r.flushed).await;
    assert_eq!(after.member, "alice");
    assert_eq!(after.turn_id, "t1");
    assert!(after.released_ms < 1200, "a prefix: {after:?}");
    assert!(after.chunks_released < 3, "a strict prefix: {after:?}");
    assert!(after.chunks_released >= 1, "the first chunk was sent: {after:?}");
    // Exactly what was written to the track: the RTP clock advances 960
    // samples per written 20 ms frame, and dropped frames never advance it.
    assert_eq!(after.released_ms, u64::from(track.timestamp().await) / 48);
    assert_eq!(after.released_ms + dropped as u64 * 20, 1200);
    // A chunk counts once audio of a later chunk has been written.
    assert_eq!(after.chunks_released, (after.released_ms - 20) / 400);

    // Nothing of the turn is sent or reported after the flush.
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(u64::from(track.timestamp().await) / 48, after.released_ms);
    let last = reports.lock().unwrap().last().cloned().unwrap();
    assert_eq!(last.released_ms, after.released_ms);
    // Reports while sending were at most one per 100 ms (plus the flush).
    let count = reports.lock().unwrap().len() as u64;
    assert!(count <= after.released_ms / 100 + 3, "{count} reports");
    track.shutdown().await.unwrap();
}

#[tokio::test]
async fn completed_turn_reports_every_chunk_at_its_end_mark() {
    let (track, reports) = reporting_track("alice").await;
    speak(&track, "t1", 0, 60).await;
    speak(&track, "t1", 1, 60).await;
    track
        .finish_audio_tagged(Some(TurnTag::new("t1", 1)))
        .await
        .unwrap();
    let done = wait_for(&reports, |r| r.chunks_released == 2).await;
    assert_eq!(done.released_ms, 120);
    assert!(!done.flushed);
    track.shutdown().await.unwrap();
}

#[tokio::test]
async fn flushing_a_turn_leaves_the_later_turn_and_other_tracks_intact() {
    let (alice, _) = reporting_track("alice").await;
    let (bob, _) = reporting_track("bob").await;
    speak(&alice, "t", 0, 400).await;
    speak(&alice, "u", 0, 400).await;
    speak(&bob, "t", 0, 400).await;
    let bob_before = bob.queued_frames_for_turn(Some("t")).await;

    let dropped = alice.flush_turn("t", true).await;
    assert!(dropped >= 5, "alice's queued frames of t: {dropped}");
    assert_eq!(alice.queued_frames_for_turn(Some("t")).await, 0);
    // The later turn is intact; it is next, so its first frames may already
    // have been sent.
    let later = alice.queued_frames_for_turn(Some("u")).await;
    assert!((17..=20).contains(&later), "alice's later turn: {later}");
    let bob_after = bob.queued_frames_for_turn(Some("t")).await;
    assert!(bob_after + 2 >= bob_before, "bob only lost what was sent");

    // drop_rest: later audio of the flushed turn is refused for alice only.
    speak(&alice, "t", 1, 100).await;
    speak(&bob, "t", 1, 100).await;
    assert_eq!(alice.queued_frames_for_turn(Some("t")).await, 0);
    assert!(bob.queued_frames_for_turn(Some("t")).await >= bob_after + 4);
    // Other turns still play for alice.
    speak(&alice, "v", 0, 100).await;
    assert_eq!(alice.queued_frames_for_turn(Some("v")).await, 5);
    alice.shutdown().await.unwrap();
    bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn flush_without_drop_rest_accepts_later_audio_of_the_turn() {
    let (alice, _) = reporting_track("alice").await;
    speak(&alice, "t", 0, 200).await;
    assert!(alice.flush_turn("t", false).await > 0);
    speak(&alice, "t", 1, 100).await;
    // Accepted again (the sender may already have taken the first frames).
    let queued = alice.queued_frames_for_turn(Some("t")).await;
    assert!((3..=5).contains(&queued), "{queued}");
    alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn session_wide_flush_still_drops_every_turn_and_untagged_audio() {
    let (alice, reports) = reporting_track("alice").await;
    speak(&alice, "t", 0, 200).await;
    speak(&alice, "u", 0, 200).await;
    alice
        .send_audio(Arc::new(vec![0.1f32; 4800]), 48000)
        .await
        .unwrap();
    wait_for(&reports, |r| r.turn_id == "t").await;
    assert!(alice.flush_send_buffer().await >= 8);
    assert_eq!(alice.queued_frames_for_turn(None).await, 0);
    assert_eq!(alice.queued_frames_for_turn(Some("u")).await, 0);
    // The turn being sent is reported once after the flush.
    let after = wait_for(&reports, |r| r.flushed).await;
    assert_eq!(after.turn_id, "t");
    assert!(after.released_ms < 200, "{after:?}");
    alice.shutdown().await.unwrap();
}
