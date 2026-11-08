use super::*;
use futures::StreamExt;

fn without_arrival(mut data: RuntimeData) -> RuntimeData {
    match &mut data {
        RuntimeData::Audio { arrival_ts_us, .. } | RuntimeData::Video { arrival_ts_us, .. } => {
            *arrival_ts_us = None
        }
        _ => {}
    }
    data
}

#[tokio::test]
async fn every_bulk_type_roundtrips_or_rejects_under_real_session_policy() {
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        let fixture = Fixture::new(policy).await;
        let client = fixture.client().await;
        let mut stream = bounded(client.create_stream_session(manifest(), &[], None))
            .await
            .unwrap();
        let mut total = 0;
        for data in crate::copy_tests::bulk_cases() {
            let payload = data.bulk_payload_bytes() as u64;
            total += payload;
            let unary = bounded(client.execute_unary(
                manifest(),
                TransportData::new(data.clone()),
                &[],
                None,
            ))
            .await;
            let sent = bounded(stream.send(TransportData::new(data.clone()))).await;
            if policy == CopyPolicy::RequireZeroCopy {
                assert!(unary.is_err());
                assert!(sent.is_err());
            } else {
                assert_eq!(without_arrival(unary.unwrap().data), data);
                sent.unwrap();
                let output = bounded(stream.receive()).await.unwrap().unwrap();
                assert_eq!(without_arrival(output.data), data);
                let reports = fixture.reports.lock().unwrap();
                let unary = &reports.last().unwrap().1;
                assert_eq!(bytes(unary, CopyReason::PortableDeserialization), payload);
                assert_eq!(bytes(unary, CopyReason::PortableSerialization), payload);
            }
        }
        let context = fixture
            .server
            .session_copy_context(stream.session_id())
            .await
            .unwrap();
        let expected = if policy == CopyPolicy::AllowCopies {
            total
        } else {
            0
        };
        assert_eq!(
            bytes(&context, CopyReason::PortableDeserialization),
            expected
        );
        assert_eq!(bytes(&context, CopyReason::PortableSerialization), expected);
        assert_eq!(bytes(&context, CopyReason::VecMaterialization), 0);
        assert_eq!(
            fixture.calls.load(Ordering::SeqCst),
            if expected == 0 { 0 } else { 12 }
        );
        bounded(stream.close()).await.unwrap();
        fixture.finish().await;
    }
}

#[tokio::test]
async fn two_network_subscribers_receive_one_serialized_output() {
    let fixture = Fixture::new(CopyPolicy::AllowCopies).await;
    let mut first = fixture
        .client()
        .await
        .create_stream_session(manifest(), &[], None)
        .await
        .unwrap();
    let response = bounded(
        reqwest::Client::new()
            .get(format!(
                "{}/stream/{}/output",
                fixture.url,
                first.session_id(),
            ))
            .send(),
    )
    .await
    .unwrap()
    .error_for_status()
    .unwrap();
    let mut wire = response.bytes_stream();
    bounded(first.send(audio())).await.unwrap();
    assert_audio(bounded(first.receive()).await.unwrap().unwrap());
    let mut frames = crate::sse::Frames::default();
    let ledger = CopyContext::new(CopyPolicy::AllowCopies, "second-network-subscriber");
    let second = bounded(async {
        loop {
            frames.push(wire.next().await.unwrap().unwrap());
            while let Some(frame) = frames.next(&ledger).unwrap() {
                if let Some(data) = crate::sse::decode(&frame, &ledger).unwrap() {
                    return data;
                }
            }
        }
    })
    .await;
    assert_audio(second);
    let context = fixture
        .server
        .session_copy_context(first.session_id())
        .await
        .unwrap();
    assert_eq!(bytes(&context, CopyReason::PortableSerialization), 16);
    bounded(first.close()).await.unwrap();
    // A heartbeat can already be in flight when deletion finishes. Drain to EOF
    // and require complete framing with no further payload, independent of TCP
    // chunk boundaries and scheduling on Windows/Linux.
    bounded(async {
        loop {
            while let Some(frame) = frames.next(&ledger).unwrap() {
                assert!(crate::sse::decode(&frame, &ledger).unwrap().is_none());
            }
            match wire.next().await {
                Some(chunk) => frames.push(chunk.unwrap()),
                None => break,
            }
        }
        assert!(frames.is_empty());
    })
    .await;
    fixture.finish().await;
}

#[tokio::test]
async fn unary_node_and_encoding_failures_return_errors_and_join_cleanup() {
    for (policy, input, expected) in [
        (CopyPolicy::AllowCopies, "fail", "node failure sentinel"),
        (CopyPolicy::RequireZeroCopy, "produce", "copy"),
    ] {
        let fixture = Fixture::new(policy).await;
        let error = bounded(fixture.client().await.execute_unary(
            manifest(),
            TransportData::new(RuntimeData::Text(input.into())),
            &[],
            None,
        ))
        .await
        .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        fixture.finish().await;
    }
}

#[tokio::test]
async fn malformed_typed_input_returns_bad_request_and_cleans_created_core() {
    let fixture = Fixture::new(CopyPolicy::AllowCopies).await;
    let response = bounded(reqwest::Client::new().post(format!("{}/execute", fixture.url))
        .json(&serde_json::json!({"manifest":manifest().as_ref(), "input":{"data":{"Unknown":0}}}))
        .send()).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.reports.lock().unwrap().len(), 1);
    fixture.finish().await;
}

#[tokio::test]
async fn fragmented_network_request_uses_session_accounting_without_body_staging() {
    let fixture = Fixture::new(CopyPolicy::AllowCopies).await;
    let wire = serde_json::to_vec(&serde_json::json!({
        "input":audio(), "manifest":manifest().as_ref(),
    }))
    .unwrap();
    let chunks: Vec<_> = wire
        .chunks(3)
        .map(|chunk| Ok::<_, Infallible>(bytes::Bytes::copy_from_slice(chunk)))
        .collect();
    let response = bounded(
        reqwest::Client::new()
            .post(format!("{}/execute", fixture.url))
            .header("content-type", "application/json")
            .body(reqwest::Body::wrap_stream(futures::stream::iter(chunks)))
            .send(),
    )
    .await
    .unwrap()
    .error_for_status()
    .unwrap();
    let body = JsonBody::collect(response.bytes_stream(), usize::MAX)
        .await
        .unwrap();
    #[derive(Deserialize)]
    struct Reply {
        output: TransportData,
    }
    assert_audio(body.decode::<Reply>(CopyContext::legacy()).unwrap().output);
    let context = fixture.reports.lock().unwrap()[0].1.clone();
    assert_eq!(bytes(&context, CopyReason::PortableDeserialization), 16);
    assert_eq!(bytes(&context, CopyReason::VecMaterialization), 0);
    fixture.finish().await;
}

#[tokio::test]
async fn closing_stream_cancels_blocked_node_and_releases_core() {
    let fixture = Fixture::new(CopyPolicy::AllowCopies).await;
    let mut stream = fixture
        .client()
        .await
        .create_stream_session(manifest(), &[], None)
        .await
        .unwrap();
    bounded(stream.send(TransportData::new(RuntimeData::Text("block".into()))))
        .await
        .unwrap();
    bounded(fixture.blocked.notified()).await;
    bounded(stream.close()).await.unwrap();
    bounded(stream.close()).await.unwrap();
    fixture.finish().await;
}
