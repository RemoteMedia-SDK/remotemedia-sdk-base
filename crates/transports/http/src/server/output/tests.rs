use super::*;

#[tokio::test]
async fn framing_retains_wire_owner_and_drains_before_completion() {
    let (tx, rx) = broadcast::channel(2);
    let (finished, completion) = watch::channel(None);
    let mut output = Output {
        rx,
        completion,
        heartbeat: tokio::time::interval(std::time::Duration::from_secs(15)),
        pending: VecDeque::new(),
        terminal: false,
    };
    let wire = Bytes::from(vec![b'x'; 1024]);
    let pointer = wire.as_ptr();
    tx.send(wire).unwrap();
    finished.send_replace(Some(Ok(())));
    assert_eq!(output.next().await.unwrap(), "data: ");
    let retained = output.next().await.unwrap();
    assert_eq!(retained.as_ptr(), pointer);
    assert_eq!(output.next().await.unwrap(), "\n\n");
    assert!(output.next().await.is_none());
    drop(output);
    drop(tx);
    assert!(retained.iter().all(|byte| *byte == b'x'));
}

#[tokio::test]
async fn lagged_subscriber_receives_terminal_error_instead_of_partial_history() {
    let (tx, rx) = broadcast::channel(2);
    let (_finished, completion) = watch::channel(None);
    let mut output = Output {
        rx,
        completion,
        heartbeat: tokio::time::interval(std::time::Duration::from_secs(15)),
        pending: VecDeque::new(),
        terminal: false,
    };
    for _ in 0..3 {
        tx.send(Bytes::from_static(b"payload")).unwrap();
    }
    assert_eq!(output.next().await.unwrap(), "event: error\ndata: ");
    let error: String = serde_json::from_slice(&output.next().await.unwrap()).unwrap();
    assert!(error.contains("missed 1 outputs"), "{error}");
    assert_eq!(output.next().await.unwrap(), "\n\n");
    assert!(output.next().await.is_none());
    assert_eq!(tx.len(), 2);
    drop(output);
    assert_eq!(tx.len(), 0);
}
