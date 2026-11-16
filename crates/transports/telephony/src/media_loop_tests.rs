//! The call media loop: `audio_released` on the session bus, targeted and
//! session-wide flush, pacing (openspec: add-ai-sdk-voice-library, tasks 2.2
//! and 2.3a, telephony).
use super::*;
use crate::codec::CodecMap;
use crate::rtp::{RtpPacket, RtpPacketInfo};
use async_trait::async_trait;
use remotemedia_core::transport::turn_audio::{AudioReleased, MediaEvent};
use std::time::Instant;

const MEMBER: &str = "call-1:caller";

fn outbound() -> RtpOutboundMediaSession {
    RtpOutboundMediaSession::new(AudioCodec::Pcmu, 0, 1, 0, 0, 20).unwrap()
}

fn sender(control: &Arc<SessionControl>) -> CallAudioSender {
    CallAudioSender::new(outbound(), MEMBER, Some(Arc::clone(control)))
}

fn tagged(output: TransportData, turn: Option<(&str, u64)>) -> TransportData {
    match turn {
        Some((turn, chunk)) => {
            let mut output = output;
            TurnTag::new(turn, chunk).write_to(&mut output.metadata);
            output
        }
        None => output,
    }
}

/// `ms` of 8 kHz mono audio as a pipeline output, of a turn's chunk or of
/// no turn.
fn audio(turn: Option<(&str, u64)>, ms: usize) -> TransportData {
    tagged(
        TransportData::new(RuntimeData::Audio {
            samples: vec![0.1f32; 8 * ms].into(),
            sample_rate: 8000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        }),
        turn,
    )
}

/// The TTS node's end mark for a text chunk.
fn speech_end(turn: &str, chunk: u64) -> TransportData {
    tagged(
        TransportData::new(RuntimeData::Json(serde_json::json!({ "kind": "speech_end" }))),
        Some((turn, chunk)),
    )
}

fn take(sender: &mut CallAudioSender, limit: usize) -> usize {
    let mut taken = 0;
    while taken < limit && sender.next_datagram().is_some() {
        taken += 1;
    }
    taken
}

/// The `audio_released` reports published on the session bus so far.
fn released(events: &mut broadcast::Receiver<RuntimeData>) -> Vec<AudioReleased> {
    let mut reports = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let Some(MediaEvent::AudioReleased(report)) = MediaEvent::from_runtime(&event) {
            reports.push(report);
        }
    }
    reports
}

/// The session's targeted flush as the call receives it.
fn flush_for(
    control: &Arc<SessionControl>,
    requests: &mut mpsc::Receiver<FlushTarget>,
    members: &[&str],
    turn_id: &str,
) -> FlushTarget {
    assert!(control.request_flush_audio_for(FlushTarget {
        members: members.iter().map(|member| member.to_string()).collect(),
        turn_id: turn_id.into(),
        drop_rest: true,
    }));
    requests.try_recv().expect("the call's flush request")
}

#[tokio::test]
async fn a_tagged_turn_is_reported_on_the_bus_and_a_targeted_flush_leaves_a_strict_prefix() {
    let control = SessionControl::new("s-prefix");
    let mut events = control.subscribe_media();
    let (hook, mut requests) = mpsc::channel(4);
    control.install_flush_audio_for_hook(hook);
    let mut call = sender(&control);

    for chunk in 0..3 {
        call.push_output(audio(Some(("t1", chunk)), 400)).unwrap();
        call.push_output(speech_end("t1", chunk)).unwrap();
    }
    // 27 packets leave: all of chunk 0 and 140 ms of chunk 1.
    assert_eq!(take(&mut call, 27), 27);
    let reports = released(&mut events);
    assert!(reports.len() >= 2, "{reports:?}");
    assert!(reports
        .iter()
        .all(|report| report.member == MEMBER && report.turn_id == "t1" && !report.flushed));
    assert!(
        reports.iter().any(|report| report.chunks_released == 1 && report.released_ms == 400),
        "chunk 0's end mark is reported when its last packet is taken: {reports:?}"
    );
    assert!(reports.windows(2).all(|pair| pair[0].released_ms <= pair[1].released_ms));
    assert!(reports.last().unwrap().released_ms <= 540);

    let target = flush_for(&control, &mut requests, &[MEMBER], "t1");
    assert_eq!(call.apply_flush(&target), Some(8 * (1200 - 540)));
    let reports = released(&mut events);
    assert_eq!(reports.len(), 1, "{reports:?}");
    let last = &reports[0];
    assert!(last.flushed);
    assert_eq!((last.member.as_str(), last.turn_id.as_str()), (MEMBER, "t1"));
    assert_eq!(last.released_ms, 540, "what left, not what was generated (1200 ms)");
    assert_eq!(last.chunks_released, 1, "chunk 1 was cut and chunk 2 never left");

    // Nothing of the turn is left, and its later audio and end mark are refused.
    assert_eq!(take(&mut call, 100), 0);
    call.push_output(audio(Some(("t1", 2)), 100)).unwrap();
    call.push_output(speech_end("t1", 2)).unwrap();
    assert_eq!(take(&mut call, 100), 0);
    assert!(released(&mut events).iter().all(|report| report.released_ms == 540));
}

#[tokio::test]
async fn flushing_one_turn_leaves_a_later_turn_and_other_calls_queued() {
    let control = SessionControl::new("s-later");
    let mut events = control.subscribe_media();
    let (hook, mut requests) = mpsc::channel(4);
    control.install_flush_audio_for_hook(hook);
    let mut call = sender(&control);
    let mut other = CallAudioSender::new(outbound(), "call-2:caller", Some(Arc::clone(&control)));

    for call in [&mut call, &mut other] {
        call.push_output(audio(Some(("t", 0)), 400)).unwrap();
        call.push_output(audio(Some(("u", 0)), 400)).unwrap();
        call.push_output(speech_end("u", 0)).unwrap();
    }
    assert_eq!(take(&mut call, 5), 5);
    let target = flush_for(&control, &mut requests, &[MEMBER], "t");
    assert_eq!(call.apply_flush(&target), Some(8 * 300));
    assert_eq!(other.apply_flush(&target), None, "the flush names another call");

    // All of u still plays on the flushed call; the other call plays both.
    assert_eq!(take(&mut call, 100), 20);
    assert_eq!(take(&mut other, 100), 40);
    let reports = released(&mut events);
    let last = |member: &str, turn: &str| {
        reports
            .iter()
            .rev()
            .find(|report| report.member == member && report.turn_id == turn)
            .cloned()
            .unwrap_or_else(|| panic!("no report of {turn} for {member}: {reports:?}"))
    };
    let t = last(MEMBER, "t");
    assert_eq!((t.released_ms, t.flushed), (100, true));
    let u = last(MEMBER, "u");
    assert_eq!((u.released_ms, u.chunks_released, u.flushed), (400, 1, false));
    assert_eq!(last("call-2:caller", "t").released_ms, 400);
    assert_eq!(last("call-2:caller", "u").released_ms, 400);
}

#[tokio::test]
async fn the_session_wide_flush_drops_everything_queued_and_refuses_nothing() {
    let control = SessionControl::new("s-wide");
    let mut events = control.subscribe_media();
    let mut call = sender(&control);

    call.push_output(audio(Some(("t", 0)), 400)).unwrap();
    call.push_output(audio(Some(("u", 0)), 400)).unwrap();
    call.push_output(audio(None, 400)).unwrap();
    assert_eq!(take(&mut call, 5), 5);
    call.flush_all();
    assert_eq!(take(&mut call, 100), 0, "every turn and the untagged audio are gone");
    let reports = released(&mut events);
    let last = reports.last().unwrap();
    assert_eq!((last.turn_id.as_str(), last.released_ms, last.flushed), ("t", 100, true));
    assert!(reports.iter().all(|report| report.turn_id == "t"), "u never left: {reports:?}");

    // Unlike a targeted flush with `drop_rest`, later audio is accepted,
    // tagged or not, as before.
    call.push_output(audio(Some(("t", 1)), 100)).unwrap();
    call.push_output(audio(None, 100)).unwrap();
    assert_eq!(take(&mut call, 100), 10);
}

#[tokio::test]
async fn untagged_audio_is_sent_and_never_reported_and_addressed_audio_goes_to_its_member() {
    let control = SessionControl::new("s-untagged");
    let mut events = control.subscribe_media();
    let mut call = sender(&control);

    call.push_output(audio(None, 200)).unwrap();
    assert_eq!(take(&mut call, 100), 10);
    assert!(released(&mut events).is_empty());

    let to = |member: &str| {
        audio(Some(("t", 0)), 200).with_metadata(participant::TO.to_string(), member.to_string())
    };
    call.push_output(to("call-2:caller")).unwrap();
    assert_eq!(take(&mut call, 100), 0, "addressed to another member");
    assert!(released(&mut events).is_empty());
    call.push_output(to(MEMBER)).unwrap();
    assert_eq!(take(&mut call, 100), 10);
    assert_eq!(released(&mut events).last().unwrap().released_ms, 200);
}

// ─── The loop itself, over UDP on loopback ───────────────────────────────

struct FakeStream {
    outputs: mpsc::Receiver<TransportData>,
    inputs: mpsc::UnboundedSender<TransportData>,
}

#[async_trait]
impl StreamSession for FakeStream {
    fn session_id(&self) -> &str {
        "s-loop"
    }

    async fn send_input(&mut self, data: TransportData) -> remotemedia_core::Result<()> {
        let _ = self.inputs.send(data);
        Ok(())
    }

    async fn recv_output(&mut self) -> remotemedia_core::Result<Option<TransportData>> {
        Ok(self.outputs.recv().await)
    }

    async fn close(&mut self) -> remotemedia_core::Result<()> {
        Ok(())
    }

    fn is_active(&self) -> bool {
        true
    }
}

struct Rig {
    control: Arc<SessionControl>,
    /// The caller's RTP socket.
    caller: UdpSocket,
    call_addr: SocketAddr,
    outputs: mpsc::Sender<TransportData>,
    inputs: mpsc::UnboundedReceiver<TransportData>,
    session_flush: broadcast::Sender<()>,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

async fn rig(session: &str) -> Rig {
    let control = SessionControl::new(session);
    let caller = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let call_addr = socket.local_addr().unwrap();
    let (outputs, stream_outputs) = mpsc::channel(64);
    let (stream_inputs, inputs) = mpsc::unbounded_channel();
    let mut codec_map = CodecMap::default();
    codec_map.insert(0, AudioCodec::Pcmu).unwrap();
    let mut media = CallMedia::new(
        "call-1",
        Box::new(FakeStream {
            outputs: stream_outputs,
            inputs: stream_inputs,
        }),
        socket,
        caller.local_addr().unwrap(),
        RtpMediaSession::new("call-1".into(), MEMBER.into(), ParticipantRole::User, codec_map, 4),
        sender(&control),
        Duration::from_millis(20),
    );
    let (hook, requests) = mpsc::channel(8);
    control.install_flush_audio_for_hook(hook);
    media.flush_requests = Some(requests);
    let (session_flush, flushes) = broadcast::channel(8);
    media.session_flushes = Some(flushes);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(media.run(stopped));
    Rig {
        control,
        caller,
        call_addr,
        outputs,
        inputs,
        session_flush,
        stop,
        task,
    }
}

impl Rig {
    /// Receive RTP at the caller until none arrives for `quiet`; the arrival
    /// time of each packet.
    async fn packets_until_quiet(&self, quiet: Duration) -> Vec<Instant> {
        let mut arrivals = Vec::new();
        let mut buf = [0u8; 1500];
        while let Ok(Ok((len, _))) =
            tokio::time::timeout(quiet, self.caller.recv_from(&mut buf)).await
        {
            RtpPacket::parse(&buf[..len]).expect("an RTP packet");
            arrivals.push(Instant::now());
        }
        arrivals
    }

    async fn stop(self) {
        let _ = self.stop.send(());
        tokio::time::timeout(Duration::from_secs(2), self.task)
            .await
            .expect("the loop stops")
            .unwrap();
    }
}

#[tokio::test]
async fn the_loop_paces_packets_and_publishes_what_it_released() {
    let rig = rig("s-loop-paced").await;
    let mut events = rig.control.subscribe_media();
    let started = Instant::now();
    rig.outputs.send(audio(Some(("t1", 0)), 400)).await.unwrap();
    rig.outputs.send(speech_end("t1", 0)).await.unwrap();

    let arrivals = rig.packets_until_quiet(Duration::from_millis(300)).await;
    assert_eq!(arrivals.len(), 20, "400 ms is 20 packets of 20 ms");
    let took = *arrivals.last().unwrap() - started;
    assert!(
        took >= Duration::from_millis(300),
        "20 packets left in {took:?}: they were sent in a burst, not paced"
    );

    let reports = released(&mut events);
    assert!(reports.len() >= 3, "a report about every 100 ms: {reports:?}");
    assert!(reports.windows(2).all(|pair| pair[0].released_ms <= pair[1].released_ms));
    let last = reports.last().unwrap();
    assert_eq!((last.member.as_str(), last.turn_id.as_str()), (MEMBER, "t1"));
    assert_eq!((last.released_ms, last.chunks_released), (400, 1));
    rig.stop().await;
}

#[tokio::test]
async fn a_targeted_flush_reaches_the_loop_and_cuts_only_its_turn() {
    let rig = rig("s-loop-flush").await;
    let mut events = rig.control.subscribe_media();
    rig.outputs.send(audio(Some(("t1", 0)), 1000)).await.unwrap();
    rig.outputs.send(audio(Some(("u", 0)), 200)).await.unwrap();
    rig.outputs.send(speech_end("u", 0)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(rig.control.request_flush_audio_for(FlushTarget {
        members: vec![MEMBER.into()],
        turn_id: "t1".into(),
        drop_rest: true,
    }));
    let arrivals = rig.packets_until_quiet(Duration::from_millis(300)).await;

    let reports = released(&mut events);
    let t1 = reports
        .iter()
        .rev()
        .find(|report| report.turn_id == "t1")
        .expect("a report of t1");
    assert!(t1.flushed, "{reports:?}");
    assert!(
        t1.released_ms > 0 && t1.released_ms < 1000,
        "a strict prefix of the 1000 ms generated: {t1:?}"
    );
    assert_eq!(t1.chunks_released, 0, "its only chunk was cut");
    let u = reports.iter().rev().find(|report| report.turn_id == "u").expect("a report of u");
    assert_eq!((u.released_ms, u.chunks_released, u.flushed), (200, 1, false));
    // What the caller received is what was reported: t1's prefix and all of u.
    assert_eq!(arrivals.len() as u64 * 20, t1.released_ms + 200);
    rig.stop().await;
}

#[tokio::test]
async fn the_session_wide_flush_reaches_the_loop() {
    let rig = rig("s-loop-wide").await;
    let mut events = rig.control.subscribe_media();
    rig.outputs.send(audio(Some(("t1", 0)), 1000)).await.unwrap();
    rig.outputs.send(audio(None, 1000)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;
    rig.session_flush.send(()).unwrap();
    let arrivals = rig.packets_until_quiet(Duration::from_millis(300)).await;
    assert!(
        !arrivals.is_empty() && arrivals.len() < 50,
        "{} packets: the queue was not dropped",
        arrivals.len()
    );
    let reports = released(&mut events);
    let last = reports.last().unwrap();
    assert!(last.flushed && last.turn_id == "t1", "{reports:?}");
    assert_eq!(last.released_ms, arrivals.len() as u64 * 20);

    // Audio after the flush plays.
    rig.outputs.send(audio(None, 100)).await.unwrap();
    assert_eq!(rig.packets_until_quiet(Duration::from_millis(300)).await.len(), 5);
    rig.stop().await;
}

#[tokio::test]
async fn caller_rtp_reaches_the_pipeline_and_makes_the_member_media_ready() {
    let mut rig = rig("s-loop-inbound").await;
    assert!(!rig.control.is_media_ready(MEMBER));
    for sequence in 1..=8u16 {
        let packet = RtpPacket {
            info: RtpPacketInfo {
                payload_type: 0,
                sequence,
                timestamp: u32::from(sequence) * 160,
                ssrc: 9,
                marker: false,
            },
            payload: vec![0xff; 160].into(),
        };
        rig.caller.send_to(&packet.write(), rig.call_addr).await.unwrap();
    }
    let frame = tokio::time::timeout(Duration::from_secs(2), rig.inputs.recv())
        .await
        .expect("decoded caller audio")
        .unwrap();
    assert!(matches!(frame.data, RuntimeData::Audio { sample_rate: 8000, .. }));
    assert!(rig.control.is_media_ready(MEMBER), "the caller's media is flowing");
    rig.stop().await;
}
