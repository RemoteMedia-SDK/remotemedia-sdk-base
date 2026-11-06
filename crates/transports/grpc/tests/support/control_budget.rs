use super::*;

async fn replace(tx: &mpsc::Sender<ControlFrame>, correlation_id: u64) {
    let data = remotemedia_grpc::adapters::runtime_data_to_data_buffer(&RuntimeData::Binary(
        vec![7; 8].into(),
    ));
    tx.send(ControlFrame {
        op: Some(PbOp::InterceptReply(InterceptReply {
            correlation_id,
            decision: Some(PbInterceptDecision {
                decision: Some(PbDecision::Replace(data)),
            }),
        })),
    })
    .await
    .unwrap();
}

async fn request_intercept(
    tx: &mpsc::Sender<ControlFrame>,
    events: &mut tonic::Streaming<ControlEvent>,
) -> u64 {
    tx.send(ControlFrame {
        op: Some(PbOp::Intercept(Intercept {
            addr: Some(addr("calc", ControlDirection::Out)),
            deadline_ms: 5000,
        })),
    })
    .await
    .unwrap();
    // These frames share one ordered attach, so no timing sleep is needed.
    publish_calc(tx, 1.0, 2.0).await;
    let Some(PbEvent::InterceptRequest(request)) = next_event(events, Duration::from_secs(3))
        .await
        .unwrap()
        .event
    else {
        panic!("intercept request expected")
    };
    request.correlation_id
}

#[tokio::test]
async fn exhausted_replacement_reports_a_wire_error_and_retries_after_release() {
    let config = remotemedia_core::transport::ExecutorConfig {
        ingress_retention: Some(
            serde_json::from_value(serde_json::json!({"owners": 1, "bytes": 8})).unwrap(),
        ),
        ..Default::default()
    };
    let (url, executor, server) = start_server_with_config(config).await;
    let mut session = executor
        .create_session(Arc::new(calc_manifest()))
        .await
        .unwrap();
    let budget = session.input_sender().unwrap().buffer_budget().unwrap();
    let held = session.output_allocator().allocate_bytes(8).unwrap();
    let mut client = connect_control(&url).await;
    let (tx, mut events) = open_attach(&mut client, &session.session_id).await;
    let correlation = request_intercept(&tx, &mut events).await;
    replace(&tx, correlation).await;
    let Some(PbEvent::Error(error)) = next_event(&mut events, Duration::from_secs(3))
        .await
        .unwrap()
        .event
    else {
        panic!("budget rejection must reach the wire client")
    };
    assert!(format!("{error:?}").contains("control ingress buffer budget"));
    assert_eq!(budget.usage().owners, 1);
    drop(held);
    replace(&tx, correlation).await;
    let output = tokio::time::timeout(Duration::from_secs(3), session.recv_output())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let RuntimeData::Binary(view) = output.data else {
        panic!("binary replacement expected")
    };
    assert_eq!(&*view, &[7; 8]);
    assert_eq!(budget.usage().bytes, 8);
    session.close().await.unwrap();
    session.wait().await.unwrap();
    drop(view);
    assert_eq!(budget.usage().owners, 0);
    drop(tx);
    server.abort();
    let _ = server.await;
}
