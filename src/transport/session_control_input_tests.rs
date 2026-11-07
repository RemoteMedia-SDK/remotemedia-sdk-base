use super::*;
use remotemedia_types::BufferBudget;

#[tokio::test]
async fn input_completion_releases_internal_sender_but_preserves_admission() {
    let (tx, mut rx) = mpsc::channel(1);
    let sender = super::super::SessionPacketSender::from(tx);
    let budget = BufferBudget::new(1, 8);
    sender.bind_budget(budget.clone()).unwrap();
    let control = SessionControl::new("drain");
    control.attach_input_sender(sender).await;
    assert!(control.input_sender().await.is_some());
    control.finish_input();
    control.finish_input();
    assert!(control.input_sender().await.is_none());
    assert!(rx.recv().await.is_none());
    let retained = control
        .admit_external_control(RuntimeData::Binary(vec![7; 8].into()))
        .unwrap();
    assert_eq!(budget.usage().owners, 1);
    let error = control
        .admit_external_control(RuntimeData::Binary(vec![9; 8].into()))
        .unwrap_err();
    assert!(error.to_string().contains("control ingress buffer budget"));
    let error = control
        .publish(
            &ControlAddress::node_in("source"),
            RuntimeData::Text("late".into()),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("input is complete"));
    drop(retained);
    assert_eq!(budget.usage().owners, 0);
    assert!(control
        .admit_external_control(RuntimeData::Binary(vec![9; 8].into()))
        .is_ok());
}

#[tokio::test]
async fn input_completion_preserves_independent_sender_lifetimes() {
    let (tx, mut rx) = mpsc::channel(1);
    let control = SessionControl::new("drain");
    control.attach_input_sender(tx).await;
    let participant = control.input_sender().await.unwrap();
    control.finish_input();
    assert!(!participant.is_closed());
    assert!(control.input_sender().await.is_none());
    drop(participant);
    assert!(rx.recv().await.is_none());
}
