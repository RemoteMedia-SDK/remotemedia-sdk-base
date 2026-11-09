use super::*;
use crate::data::RuntimeData;
use remotemedia_types::{BufferBudgetUsage, CopyContext, CopyPolicy, CopyReason};
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
    let copies = CopyContext::legacy();
    assert!(sender
        .control_payload(Some("aux"), RuntimeData::Binary(Vec::new().into()), copies)
        .is_err());
    assert!(sender
        .control_payload(Some("aux"), RuntimeData::Text("ok".into()), copies)
        .is_ok());
    let (tx, _rx) = mpsc::channel(1);
    let legacy = SessionPacketSender::from(tx);
    legacy.try_send(text()).unwrap();
    assert!(legacy.bind_budget(BufferBudget::new(1, 8)).is_err());
}

fn counts_for(copies: &CopyContext, reason: CopyReason) -> remotemedia_types::CopyCounts {
    copies
        .report()
        .entries
        .into_iter()
        .find(|(entry, _)| *entry == reason)
        .map(|(_, counts)| counts)
        .unwrap_or_default()
}

/// (completed operations, copied bytes, rejected operations) across all reasons.
fn activity(copies: &CopyContext) -> (u64, u64, u64) {
    copies
        .report()
        .entries
        .iter()
        .fold((0, 0, 0), |(done, bytes, rejected), (_, counts)| {
            (
                done + counts.completed,
                bytes + counts.copied_bytes,
                rejected + counts.rejected,
            )
        })
}

#[test]
fn auxiliary_envelopes_reject_typed_media_and_count_legacy_binary_encoding() {
    let (tx, _rx) = mpsc::channel(1);
    let sender = SessionPacketSender::from(tx);
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "aux-envelope-test");
    let audio = RuntimeData::Audio {
        samples: vec![0.25_f32; 4].into(),
        sample_rate: 16_000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    };
    let tensor = RuntimeData::Tensor {
        data: vec![1_u8; 8].into(),
        shape: vec![2],
        dtype: 0,
        metadata: None,
    };
    for payload in [audio, tensor] {
        let error = sender
            .control_payload(Some("context"), payload, &copies)
            .expect_err("typed media must not be replaced by a type tag");
        assert!(error.to_string().contains("main input"), "{error}");
    }
    assert_eq!(
        activity(&copies),
        (0, 0, 0),
        "rejected media copied nothing"
    );

    let main = sender
        .control_payload(None, RuntimeData::Binary(vec![9_u8; 6].into()), &copies)
        .unwrap();
    assert!(matches!(main, RuntimeData::Binary(ref b) if b.len() == 6));
    assert_eq!(activity(&copies), (0, 0, 0), "main rail forwards the owner");

    let wrapped = sender
        .control_payload(
            Some("context"),
            RuntimeData::Binary(vec![9_u8; 6].into()),
            &copies,
        )
        .unwrap();
    let RuntimeData::Json(envelope) = wrapped else {
        panic!("expected envelope")
    };
    assert_eq!(envelope["payload"]["binary_b64"], "CQkJCQkJ");
    let counts = counts_for(&copies, CopyReason::PortableSerialization);
    assert_eq!((counts.completed, counts.copied_bytes), (1, 6));
    assert_eq!(
        activity(&copies),
        (1, 6, 0),
        "only the binary encoding is counted"
    );

    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "aux-envelope-strict");
    assert!(sender
        .control_payload(
            Some("context"),
            RuntimeData::Binary(vec![1_u8; 3].into()),
            &strict
        )
        .is_err());
    let counts = counts_for(&strict, CopyReason::PortableSerialization);
    assert_eq!((counts.rejected, counts.rejected_bytes), (1, 3));
    assert_eq!(counts.completed, 0, "strict rejection precedes encoding");
    assert!(sender
        .control_payload(Some("context"), RuntimeData::Text("ok".into()), &strict)
        .is_ok());
}
