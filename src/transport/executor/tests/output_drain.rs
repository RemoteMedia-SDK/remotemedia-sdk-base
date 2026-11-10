use super::*;
use std::time::Duration;

#[tokio::test]
async fn drains_every_buffered_kind_after_other_kinds_close() {
    let executor = PipelineExecutor::new().unwrap();
    let mut session = executor
        .create_session(create_test_manifest_arc())
        .await
        .unwrap();
    session.close().await.unwrap();

    // Exercise each kind as the sole remaining buffered lane, including missing
    // receivers. Keep an external owner to prove receive preserves storage.
    for lane in 0..3 {
        for absent_others in [false, true] {
            let (tx, rx) = mpsc::channel(32);
            let bytes: remotemedia_types::SharedBytes = vec![1, 2, 3, 4].into();
            for _ in 0..32 {
                tx.send(TransportData::new(RuntimeData::Binary(bytes.clone())))
                    .await
                    .unwrap();
            }
            drop(tx);
            let mut receivers = [None, None, None];
            for (index, receiver) in receivers.iter_mut().enumerate() {
                if index != lane && !absent_others {
                    let (closed, rx) = mpsc::channel(1);
                    drop(closed);
                    *receiver = Some(rx);
                }
            }
            receivers[lane] = Some(rx);
            [
                session.output_audio_rx,
                session.output_video_rx,
                session.output_data_rx,
            ] = receivers;
            for _ in 0..32 {
                let output = tokio::time::timeout(Duration::from_secs(1), session.recv_output())
                    .await
                    .unwrap()
                    .unwrap()
                    .expect("buffered output lost at kind closure");
                let RuntimeData::Binary(output) = output.data else {
                    panic!("unexpected payload")
                };
                assert_eq!(output.as_ptr(), bytes.as_ptr());
            }
            for _ in 0..2 {
                assert!(
                    tokio::time::timeout(Duration::from_secs(1), session.recv_output())
                        .await
                        .unwrap()
                        .unwrap()
                        .is_none()
                );
            }
        }
    }
    session.wait().await.unwrap();
}

#[tokio::test]
async fn waits_for_live_kind_and_preserves_cancelled_receive() {
    let executor = PipelineExecutor::new().unwrap();
    let mut session = executor
        .create_session(create_test_manifest_arc())
        .await
        .unwrap();
    session.close().await.unwrap();
    let (tx, rx) = mpsc::channel(1);
    session.output_audio_rx = Some(rx);
    // The other lanes are closed, but an empty live lane must keep waiting.
    assert!(
        tokio::time::timeout(Duration::from_millis(20), session.recv_output())
            .await
            .is_err()
    );
    tx.send(TransportData::new(RuntimeData::Text("after cancellation".into())))
        .await
        .unwrap();
    drop(tx);
    let output = tokio::time::timeout(Duration::from_secs(1), session.recv_output())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(output.data, RuntimeData::Text(text) if text == "after cancellation"));
    // Draining must not partially consume the receiver handles themselves.
    let _receivers = session.take_output_receivers().unwrap();
    assert!(session.recv_output().await.unwrap().is_none());
    session.wait().await.unwrap();
}
