use super::*;
use remotemedia_types::SharedBytes;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::sync::mpsc;

fn frame(tag: u8) -> IpcResponse {
    // Any non-EndOfInput discriminant; the test decoder keeps the bytes.
    IpcResponse::OutputData(SharedBytes::from(vec![3, tag]))
}
fn end() -> IpcResponse {
    IpcResponse::OutputData(SharedBytes::from(vec![END_OF_INPUT]))
}

fn quick() -> Timeouts {
    Timeouts {
        send_ack: Duration::from_millis(200),
        output: Duration::from_millis(200),
    }
}

struct Turn {
    decoded: Arc<AtomicUsize>,
    outputs: Vec<u8>,
}

async fn turn(
    responses: &mut Receiver<IpcResponse>,
    turns: &AbandonedTurns,
    stop_after: Option<usize>,
) -> (Result<usize>, Turn) {
    let decoded = Arc::new(AtomicUsize::new(0));
    let counter = decoded.clone();
    let mut outputs = Vec::new();
    let result = run(
        responses,
        turns,
        quick(),
        "fixture",
        async { Ok(()) },
        move |bytes| {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(RuntimeData::Binary(bytes))
        },
        |data| {
            let RuntimeData::Binary(bytes) = data else {
                unreachable!()
            };
            outputs.push(bytes[1]);
            if stop_after == Some(outputs.len()) {
                return Err(Error::Execution("stop".into()));
            }
            Ok(())
        },
    )
    .await;
    (result, Turn { decoded, outputs })
}

#[tokio::test]
async fn completed_turn_delivers_only_its_outputs() {
    let (tx, mut rx) = mpsc::channel(16);
    let turns = AbandonedTurns::default();
    for response in [IpcResponse::SendComplete, frame(1), frame(2), end()] {
        tx.send(response).await.unwrap();
    }
    let (result, out) = turn(&mut rx, &turns, None).await;
    assert_eq!(result.unwrap(), 2);
    assert_eq!(out.outputs, [1, 2]);
    assert_eq!(turns.pending(), 0);
}

#[tokio::test]
async fn cancelled_turn_outputs_are_discarded_before_the_next_turn() {
    let (tx, mut rx) = mpsc::channel(16);
    let turns = AbandonedTurns::default();
    tx.send(IpcResponse::SendComplete).await.unwrap();
    tx.send(frame(1)).await.unwrap();
    // Barge-in: the router drops the in-flight turn after its first output.
    let cancelled = turn(&mut rx, &turns, None);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), cancelled)
            .await
            .is_err(),
        "turn is still waiting for EndOfInput"
    );
    assert_eq!(turns.pending(), 1);
    // The serial worker finishes the cancelled input, then answers the next.
    for response in [
        frame(2),
        frame(3),
        end(),
        IpcResponse::SendComplete,
        frame(9),
        end(),
    ] {
        tx.send(response).await.unwrap();
    }
    let (result, out) = turn(&mut rx, &turns, None).await;
    assert_eq!(result.unwrap(), 1);
    assert_eq!(out.outputs, [9], "stale outputs leaked into the next turn");
    assert_eq!(
        out.decoded.load(Ordering::SeqCst),
        1,
        "abandoned frames must be dropped without decoding"
    );
    assert_eq!(turns.pending(), 0);
}

#[tokio::test]
async fn callback_stop_and_output_timeout_abandon_their_remainder() {
    let (tx, mut rx) = mpsc::channel(16);
    let turns = AbandonedTurns::default();
    for response in [IpcResponse::SendComplete, frame(1), frame(2), end()] {
        tx.send(response).await.unwrap();
    }
    let (result, out) = turn(&mut rx, &turns, Some(1)).await;
    assert_eq!(result.unwrap(), 0);
    assert_eq!(out.outputs, [1]);
    assert_eq!(turns.pending(), 1);
    // A turn whose worker stays silent past the output bound is abandoned too.
    tx.send(IpcResponse::SendComplete).await.unwrap();
    let (result, _) = turn(&mut rx, &turns, None).await;
    assert_eq!(result.unwrap(), 0);
    assert_eq!(
        turns.pending(),
        1,
        "first drained, timed-out turn now pending"
    );
    for response in [end(), IpcResponse::SendComplete, frame(7), end()] {
        tx.send(response).await.unwrap();
    }
    let (result, out) = turn(&mut rx, &turns, None).await;
    assert_eq!(result.unwrap(), 1);
    assert_eq!(out.outputs, [7]);
    assert_eq!(turns.pending(), 0);
}

#[tokio::test]
async fn undrainable_abandoned_turn_fails_once_then_recovers() {
    let (tx, mut rx) = mpsc::channel(16);
    let turns = AbandonedTurns::default();
    turns.0.store(1, Ordering::Release);
    let (result, out) = turn(&mut rx, &turns, None).await;
    let error = result.unwrap_err().to_string();
    assert!(error.contains("abandoned input"), "{error}");
    assert!(out.outputs.is_empty());
    assert_eq!(turns.pending(), 0, "a failed drain does not wedge the node");
    for response in [IpcResponse::SendComplete, frame(4), end()] {
        tx.send(response).await.unwrap();
    }
    let (result, out) = turn(&mut rx, &turns, None).await;
    assert_eq!(result.unwrap(), 1);
    assert_eq!(out.outputs, [4]);
}

#[tokio::test]
async fn failed_send_and_pre_ack_frames_do_not_create_stale_turns() {
    let (tx, mut rx) = mpsc::channel(16);
    let turns = AbandonedTurns::default();
    tx.send(IpcResponse::Error("publish failed".into()))
        .await
        .unwrap();
    let (result, _) = turn(&mut rx, &turns, None).await;
    assert!(result.unwrap_err().to_string().contains("publish failed"));
    assert_eq!(turns.pending(), 0, "an unpublished input has no EndOfInput");
    // A background frame queued before the acknowledgement is not this turn's.
    for response in [frame(5), IpcResponse::SendComplete, frame(6), end()] {
        tx.send(response).await.unwrap();
    }
    let (result, out) = turn(&mut rx, &turns, None).await;
    assert_eq!(result.unwrap(), 1);
    assert_eq!(out.outputs, [6]);
    // A send that fails before submission leaves nothing to drain.
    let failed = run(
        &mut rx,
        &turns,
        quick(),
        "fixture",
        async { Err(Error::Execution("closed".into())) },
        |bytes| Ok(RuntimeData::Binary(bytes)),
        |_| Ok(()),
    )
    .await;
    assert!(failed.is_err());
    assert_eq!(turns.pending(), 0);
}

// ── Tagged turns (openspec: add-node-initiated-output) ──────────────────

fn tagged(seq: u64, tag: u8) -> IpcResponse {
    IpcResponse::Tagged {
        seq,
        origin: EmitTag::Default,
        frame: SharedBytes::from(vec![3, tag]),
    }
}

fn tagged_end(seq: u64) -> IpcResponse {
    IpcResponse::Tagged {
        seq,
        origin: EmitTag::Default,
        frame: SharedBytes::from(vec![END_OF_INPUT]),
    }
}

/// A frame the node emitted: real wire bytes, since the sink decodes them.
fn emitted(body: &str, origin: EmitTag) -> IpcResponse {
    let wire = crate::multiprocess::data_transfer::to_python_wire(
        &RuntimeData::Text(body.into()),
        "session",
    )
    .unwrap();
    IpcResponse::Tagged {
        seq: 0,
        origin,
        frame: SharedBytes::from(wire),
    }
}

/// An emit sink whose emitter feeds the returned queue.
fn sink() -> (EmitSink, mpsc::Receiver<crate::transport::origin::Routed>) {
    let (tx, rx) = mpsc::channel(16);
    let emitter = Emitter::pending("fixture");
    emitter.bind(tx, None, Default::default(), None);
    let sink = EmitSink {
        emitter,
        copies: remotemedia_types::CopyContext::legacy().clone(),
        node_id: "fixture".into(),
    };
    (sink, rx)
}

async fn tagged_turn(
    responses: &mut Receiver<IpcResponse>,
    seq: u64,
    emits: Option<&EmitSink>,
) -> (Result<usize>, Vec<u8>) {
    let mut outputs = Vec::new();
    let result = run_turn(
        responses,
        &AbandonedTurns::default(),
        quick(),
        "fixture",
        async { Ok(()) },
        |bytes| Ok(RuntimeData::Binary(bytes)),
        |data| {
            let RuntimeData::Binary(bytes) = data else {
                unreachable!()
            };
            outputs.push(bytes[1]);
            Ok(())
        },
        Some(super::Turn { seq, emits }),
    )
    .await;
    (result, outputs)
}

fn emitted_text(frame: crate::transport::origin::Routed) -> String {
    match frame.data {
        RuntimeData::Text(body) => body,
        other => panic!("expected text, got {other:?}"),
    }
}

#[tokio::test]
async fn a_tagged_turn_keeps_its_own_frames_and_drops_another_turns() {
    let (tx, mut rx) = mpsc::channel(16);
    // Turn 4 was abandoned; its remaining output and end are still queued,
    // some of it arriving after turn 5 was acknowledged.
    for response in [
        tagged(4, 40),
        IpcResponse::SendComplete,
        tagged(4, 41),
        tagged_end(4),
        tagged(5, 50),
        tagged(5, 51),
        tagged_end(5),
    ] {
        tx.send(response).await.unwrap();
    }
    let (result, outputs) = tagged_turn(&mut rx, 5, None).await;
    assert_eq!(result.unwrap(), 2);
    assert_eq!(outputs, [50, 51]);
}

#[tokio::test]
async fn frames_emitted_during_a_turn_go_to_the_emitter_not_to_the_turn() {
    let (tx, mut rx) = mpsc::channel(16);
    let (sink, mut emits) = sink();
    for response in [
        IpcResponse::SendComplete,
        tagged(1, 10),
        emitted("on its own", EmitTag::Default),
        tagged(1, 11),
        tagged_end(1),
    ] {
        tx.send(response).await.unwrap();
    }
    let (result, outputs) = tagged_turn(&mut rx, 1, Some(&sink)).await;
    assert_eq!(result.unwrap(), 2, "an emitted frame is not an output of the turn");
    assert_eq!(outputs, [10, 11]);
    assert_eq!(emitted_text(emits.recv().await.unwrap()), "on its own");
}

#[tokio::test]
async fn emits_between_turns_are_forwarded_and_a_turn_takes_the_lane() {
    let (tx, rx) = mpsc::channel(16);
    let lane = Arc::new(tokio::sync::Mutex::new(rx));
    let gate = Arc::new(LaneGate::default());
    let (sink, mut emits) = sink();
    let reader = tokio::spawn(read_between_turns(lane.clone(), gate.clone(), sink.clone()));

    // No turn is running: an emitted frame is forwarded, and what an
    // abandoned turn left behind is dropped.
    tx.send(tagged(3, 30)).await.unwrap();
    tx.send(tagged_end(3)).await.unwrap();
    tx.send(emitted("idle", EmitTag::Default)).await.unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(2), emits.recv())
        .await
        .expect("forwarded while idle")
        .unwrap();
    assert_eq!(emitted_text(frame), "idle");

    // A turn gets the lane from the reader, and sees its own frames.
    {
        let _wanted = gate.want();
        let mut responses = tokio::time::timeout(Duration::from_secs(2), lane.lock())
            .await
            .expect("the reader gives the lane up");
        for response in [IpcResponse::SendComplete, tagged(4, 40), tagged_end(4)] {
            tx.send(response).await.unwrap();
        }
        let (result, outputs) = tagged_turn(&mut responses, 4, Some(&sink)).await;
        assert_eq!(result.unwrap(), 1);
        assert_eq!(outputs, [40]);
    }

    // After the turn the reader forwards again, until the lane closes.
    tx.send(emitted("idle again", EmitTag::Default)).await.unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(2), emits.recv())
        .await
        .expect("forwarded after the turn")
        .unwrap();
    assert_eq!(emitted_text(frame), "idle again");
    drop(tx);
    tokio::time::timeout(Duration::from_secs(2), reader)
        .await
        .expect("reader ends with the lane")
        .unwrap();
}
