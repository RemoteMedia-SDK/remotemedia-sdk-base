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
