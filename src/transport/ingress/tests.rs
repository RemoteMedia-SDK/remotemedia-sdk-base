use super::*;
use crate::data::RuntimeData;
use remotemedia_types::BufferBudgetUsage;
use std::collections::HashMap;

fn packet(data: RuntimeData) -> DataPacket {
    DataPacket {
        data,
        from_node: "test".into(),
        to_node: Some("node".into()),
        session_id: "ingress".into(),
        sequence: 42,
        sub_sequence: 7,
        metadata: HashMap::from([("tag".into(), "value".into())]),
    }
}
fn binary() -> DataPacket {
    packet(RuntimeData::Binary(vec![7; 8].into()))
}
fn text() -> DataPacket {
    packet(RuntimeData::Text("metadata".into()))
}
fn channel(
    owners: u64,
) -> (
    SessionPacketSender,
    mpsc::Receiver<DataPacket>,
    BufferBudget,
) {
    let (tx, rx) = mpsc::channel(1);
    let sender = SessionPacketSender::from(tx);
    let budget = BufferBudget::new(owners, owners * 8);
    sender.bind_budget(budget.clone()).unwrap();
    (sender, rx, budget)
}
fn owners(budget: &BufferBudget, n: u64) {
    assert_eq!(
        budget.usage(),
        BufferBudgetUsage {
            owners: n,
            bytes: n * 8
        }
    );
}

#[tokio::test]
async fn exhausted_send_and_reserved_send_preserve_packet_for_retry() {
    let (sender, mut rx, budget) = channel(1);
    sender.send(binary()).await.unwrap();
    let held = rx.recv().await.unwrap();
    let original = binary();
    let RuntimeData::Binary(view) = &original.data else {
        panic!()
    };
    let pointer = view.as_ptr();
    let error = sender.clone().send(original).await.unwrap_err();
    assert!(matches!(&error, IngressSendError::Budget { .. }));
    let retry = error.into_inner();
    assert_eq!(retry.sequence, 42);
    assert_eq!(retry.metadata["tag"], "value");
    let RuntimeData::Binary(view) = &retry.data else {
        panic!()
    };
    assert_eq!(view.as_ptr(), pointer);
    let error = sender.reserve().await.unwrap().send(retry).unwrap_err();
    assert!(matches!(&error, IngressSendError::Budget { .. }));
    assert_eq!(
        sender.capacity(),
        1,
        "failed permit send returns channel credit"
    );
    drop(held);
    sender
        .reserve()
        .await
        .unwrap()
        .send(error.into_inner())
        .unwrap();
    owners(&budget, 1);
    let received = rx.recv().await.unwrap();
    let RuntimeData::Binary(view) = &received.data else {
        panic!()
    };
    assert_eq!(view.as_ptr(), pointer);
    drop(received);
    owners(&budget, 0);
}

#[tokio::test]
async fn full_and_closed_errors_own_receipts_until_retry_or_drop() {
    let (sender, mut rx, budget) = channel(1);
    sender.try_send(text()).unwrap();
    let error = sender.try_send(binary()).unwrap_err();
    assert!(matches!(&error, IngressSendError::Full(_)));
    owners(&budget, 1);
    drop(rx.recv().await);
    sender.try_send(error.into_inner()).unwrap();
    drop(rx.recv().await);
    owners(&budget, 0);
    drop(rx);
    let error = sender.send(binary()).await.unwrap_err();
    assert!(matches!(&error, IngressSendError::Closed(_)));
    owners(&budget, 1);
    drop(error);
    owners(&budget, 0);
    assert!(sender.is_closed());
    sender.closed().await;
}

#[tokio::test]
async fn cancelling_a_blocked_ingress_send_restores_credit() {
    let (sender, mut rx, budget) = channel(2);
    sender.send(binary()).await.unwrap();
    let mut pending = Box::pin(sender.send(binary()));
    assert!(matches!(
        futures::poll!(pending.as_mut()),
        std::task::Poll::Pending
    ));
    owners(&budget, 2);
    drop(pending);
    owners(&budget, 1);
    drop(rx.recv().await);
    owners(&budget, 0);
}

#[tokio::test]
async fn binding_seals_all_clones_before_first_use_and_guards_auxiliary_bulk() {
    let (tx, mut rx) = mpsc::channel(1);
    let sender = SessionPacketSender::from(tx);
    let early_clone = sender.clone();
    sender.bind_budget(BufferBudget::new(0, 0)).unwrap();
    assert!(early_clone.send(binary()).await.is_err());
    early_clone.send(text()).await.unwrap();
    drop(rx.recv().await);
    assert!(sender.bind_budget(BufferBudget::new(1, 8)).is_err());
    assert!(sender
        .control_payload(Some("aux"), RuntimeData::Binary(Vec::new().into()))
        .is_err());
    assert!(sender
        .control_payload(Some("aux"), RuntimeData::Text("ok".into()))
        .is_ok());
    let (tx, _rx) = mpsc::channel(1);
    let legacy = SessionPacketSender::from(tx);
    legacy.try_send(text()).unwrap();
    assert!(legacy.bind_budget(BufferBudget::new(1, 8)).is_err());
}
