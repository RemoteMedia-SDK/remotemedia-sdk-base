//! Spoken-so-far reports and targeted flush on a call's outbound RTP
//! (openspec: add-ai-sdk-voice-library, tasks 2.2 and 2.3a).
use super::*;
use remotemedia_core::transport::turn_audio::{FlushTarget, TurnTag};

fn outbound() -> RtpOutboundMediaSession {
    RtpOutboundMediaSession::new(AudioCodec::Pcmu, 0, 1, 0, 0, 20).unwrap()
}

/// `ms` of 8 kHz mono audio of a turn's chunk.
fn speak(session: &mut RtpOutboundMediaSession, turn: &str, chunk: u64, ms: usize) {
    session
        .queue_audio_tagged(
            vec![0.1f32; 8 * ms].into(),
            8000,
            1,
            Some(TurnTag::new(turn, chunk)),
        )
        .unwrap();
}

fn take_packets(session: &mut RtpOutboundMediaSession, limit: usize) -> usize {
    let mut taken = 0;
    while taken < limit && session.next_audio_packet().unwrap().is_some() {
        taken += 1;
    }
    taken
}

#[test]
fn an_interrupted_turn_reports_a_strict_prefix_and_no_queued_audio() {
    let mut call = outbound();
    for chunk in 0..3 {
        speak(&mut call, "t1", chunk, 400);
    }
    // 27 packets leave: all of chunk 0 and 140 ms of chunk 1.
    assert_eq!(take_packets(&mut call, 27), 27);
    let dropped = call.flush_turn("t1", true);
    assert_eq!(dropped, 8 * (1200 - 540), "everything still queued");
    let reports = call.take_release_reports("call-1:caller");
    let last = reports.last().unwrap();
    assert!(last.flushed);
    assert_eq!(last.member, "call-1:caller");
    assert_eq!(last.turn_id, "t1");
    assert_eq!(last.released_ms, 540);
    assert_eq!(
        last.chunks_released, 1,
        "chunk 1 was cut and chunk 2 never left"
    );
    // Nothing of the turn is left, and its later audio is refused.
    speak(&mut call, "t1", 2, 100);
    assert_eq!(take_packets(&mut call, 100), 0);
    assert!(call
        .take_release_reports("call-1:caller")
        .iter()
        .all(|report| report.released_ms == 540));
}

#[test]
fn flushing_one_turn_keeps_the_turn_queued_behind_it() {
    let mut call = outbound();
    speak(&mut call, "t", 0, 400);
    speak(&mut call, "u", 0, 400);
    assert_eq!(take_packets(&mut call, 5), 5);
    call.flush_turn("t", false);
    // All of u still plays.
    assert_eq!(take_packets(&mut call, 100), 20);
    let reports = call.take_release_reports("leg");
    let u = reports.iter().rev().find(|r| r.turn_id == "u").unwrap();
    assert_eq!(u.released_ms, 400);
    let t = reports.iter().rev().find(|r| r.turn_id == "t").unwrap();
    assert_eq!((t.released_ms, t.flushed), (100, true));
    // Without drop_rest, later audio of t is accepted again.
    speak(&mut call, "t", 1, 100);
    assert_eq!(take_packets(&mut call, 100), 5);
}

#[test]
fn a_session_flush_applies_per_call() {
    let mut alice = outbound();
    let mut bob = outbound();
    for call in [&mut alice, &mut bob] {
        speak(call, "t", 0, 200);
    }
    let target = FlushTarget {
        members: vec!["alice:caller".into()],
        turn_id: "t".into(),
        drop_rest: true,
    };
    assert_eq!(alice.apply_flush(&target, "alice:caller"), Some(8 * 200));
    assert_eq!(bob.apply_flush(&target, "bob:caller"), None);
    assert_eq!(take_packets(&mut alice, 100), 0);
    assert_eq!(take_packets(&mut bob, 100), 10);
    // No members named: every call.
    let everyone = FlushTarget {
        members: Vec::new(),
        turn_id: "u".into(),
        drop_rest: false,
    };
    assert_eq!(bob.apply_flush(&everyone, "bob:caller"), Some(0));
}

#[test]
fn an_end_mark_completes_the_chunk_and_untagged_audio_is_unchanged() {
    let mut call = outbound();
    speak(&mut call, "t", 0, 100);
    call.mark_chunk_end(Some(TurnTag::new("t", 0)));
    assert_eq!(take_packets(&mut call, 100), 5);
    let reports = call.take_release_reports("leg");
    assert_eq!(reports.last().unwrap().chunks_released, 1);
    assert_eq!(reports.last().unwrap().released_ms, 100);

    // Untagged audio: queued and packetized as before, never reported.
    call.queue_audio(vec![0.1f32; 800].into(), 8000, 1).unwrap();
    assert_eq!(take_packets(&mut call, 100), 5);
    call.clear_pending_audio();
    assert!(call
        .take_release_reports("leg")
        .iter()
        .all(|report| report.turn_id == "t"));
}
