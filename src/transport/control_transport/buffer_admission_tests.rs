use super::*;

async fn budgeted_control() -> (Arc<SessionControl>, remotemedia_types::BufferBudget) {
    let control = SessionControl::new("budgeted-control");
    let (tx, _rx) = mpsc::channel(1);
    let sender = crate::transport::SessionPacketSender::from(tx);
    let budget = remotemedia_types::BufferBudget::new(1, 8);
    sender.bind_budget(budget.clone()).unwrap();
    control.attach_input_sender(sender).await;
    (control, budget)
}

#[tokio::test]
async fn intercept_handle_reports_budget_rejection_and_allows_pass() {
    let (control, budget) = budgeted_control().await;
    let transport = InProcControlTransport {
        control: control.clone(),
    };
    let mut intercept = transport
        .intercept(&ControlAddress::node_out("echo"), Duration::from_secs(5))
        .await
        .unwrap();
    let held = budget
        .admit(RuntimeData::Binary(vec![1; 8].into()))
        .unwrap();
    let output = control.on_node_output("echo", None, RuntimeData::Text("original".into()));
    tokio::pin!(output);
    assert!(futures::poll!(output.as_mut()).is_pending());
    let request = intercept.requests.recv().await.unwrap();
    let error = intercept
        .reply
        .complete(
            request.correlation_id,
            InterceptDecision::Replace(RuntimeData::Binary(vec![2; 8].into())),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("control ingress buffer budget"));
    intercept
        .reply
        .complete(request.correlation_id, InterceptDecision::Pass)
        .await
        .unwrap();
    assert!(matches!(output.await, Some(RuntimeData::Text(text)) if text == "original"));
    assert_eq!(budget.usage().owners, 1);
    drop(held);
    assert_eq!(budget.usage().owners, 0);
}

#[tokio::test]
async fn cancelled_intercepts_release_admitted_replacements_and_drop_needs_no_credit() {
    let (control, budget) = budgeted_control().await;
    let mut requests = control
        .intercept(
            &ControlAddress::node_out("echo"),
            Some(Duration::from_secs(5)),
        )
        .unwrap();
    let mut output =
        Box::pin(control.on_node_output("echo", None, RuntimeData::Text("cancelled".into())));
    assert!(futures::poll!(output.as_mut()).is_pending());
    let session_control::ControlEvent::InterceptRequest { correlation_id, .. } =
        requests.recv().await.unwrap()
    else {
        panic!("request expected")
    };
    drop(output);
    let error = control
        .complete_intercept(
            correlation_id,
            InterceptDecision::Replace(RuntimeData::Binary(vec![2; 8].into())),
        )
        .unwrap_err();
    assert!(error.to_string().contains("intercept receiver closed"));
    assert_eq!(budget.usage().owners, 0);
    let held = budget
        .admit(RuntimeData::Binary(vec![1; 8].into()))
        .unwrap();
    let output = control.on_node_output("echo", None, RuntimeData::Text("drop".into()));
    tokio::pin!(output);
    assert!(futures::poll!(output.as_mut()).is_pending());
    let session_control::ControlEvent::InterceptRequest { correlation_id, .. } =
        requests.recv().await.unwrap()
    else {
        panic!("request expected")
    };
    control
        .complete_intercept(correlation_id, InterceptDecision::Drop)
        .unwrap();
    assert!(output.await.is_none());
    drop(held);
    assert_eq!(budget.usage().owners, 0);
}
