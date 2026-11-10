use super::*;
use pyo3::types::{PyByteArray, PyBytes};
use remotemedia_core::transport::ExecutorConfig;
use remotemedia_types::{CopyContext, CopyCounts, CopyPolicy, CopyReason};

fn manifest() -> String {
    serde_json::json!({"version": "v1", "metadata": {}, "nodes": [
        {"id": "echo", "node_type": "PassThrough", "params": {}}
    ], "connections": []})
    .to_string()
}

fn counts(copies: &CopyContext) -> CopyCounts {
    copies
        .report()
        .entries
        .into_iter()
        .find(|(reason, _)| *reason == CopyReason::BorrowedInput)
        .unwrap()
        .1
}

async fn send(session: &PyStreamingSession, strict: bool) -> PyResult<()> {
    let future = Python::attach(|py| {
        let input = PyByteArray::new(py, &[1, 2, 3, 4]).into_any();
        pyo3_async_runtimes::tokio::into_future(session.send_input(py, input, strict)?)
    })?;
    future.await?;
    Ok(())
}

async fn finish(session: &PyStreamingSession) {
    *session.input_sender.lock().await = None;
    *session.audio_rx.lock().await = None;
    *session.video_rx.lock().await = None;
    *session.data_rx.lock().await = None;
    if let Some(mut handle) = session.handle.lock().await.take() {
        handle.close().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), handle.wait())
            .await
            .unwrap()
            .unwrap();
    }
}

#[test]
fn ffi_input_policy_strict_session_cannot_be_weakened_by_call_flag() {
    Python::attach(|py| {
        pyo3_async_runtimes::tokio::run(py, async {
            for unified in [false, true] {
                let executor = Arc::new(
                    PipelineExecutor::with_config(ExecutorConfig {
                        pipeline_copy_policy: unified.then_some(CopyPolicy::RequireZeroCopy),
                        buffer_preflight: unified.then_some(
                            remotemedia_core::transport::buffer_preflight::BufferPreflightConfig {
                                policy: CopyPolicy::AllowCopies,
                                node_retention_budget:
                                    remotemedia_traits::buffer_contract::BufferRetention {
                                        owners: 16,
                                        bytes: 4096,
                                    },
                            },
                        ),
                        transport_materialization_policy: CopyPolicy::RequireZeroCopy,
                        ..Default::default()
                    })
                    .unwrap(),
                );
                let session = build_session(executor, manifest()).await?;
                let copies = session
                    .handle
                    .lock()
                    .await
                    .as_ref()
                    .unwrap()
                    .transport_copy_context();
                for strict in [false, true] {
                    let error = send(&session, strict).await.unwrap_err();
                    Python::attach(|py| {
                        assert!(
                            error.is_instance_of::<pyo3::exceptions::PyBufferError>(py),
                            "{error}"
                        )
                    });
                    assert!(error.to_string().contains(copies.boundary()));
                }
                assert_eq!(counts(&copies).rejected_bytes, 8);
                assert_eq!(counts(&copies).copied_bytes, 0);
                finish(&session).await;
                assert_eq!(counts(&copies).rejected, 2);
                assert_eq!(
                    session.input_copies.inherited.policy(),
                    CopyPolicy::RequireZeroCopy
                );
            }
            Ok(())
        })
        .unwrap()
    });
}

#[test]
fn ffi_input_policy_in_flight_and_failed_operations_retain_session_accounting() {
    let authority = CopyContext::new(CopyPolicy::AllowCopies, "ffi-test-authority");
    let selectors = InputCopies::new(authority.clone());
    let operation = selectors
        .inherited
        .begin(CopyReason::BorrowedInput, 12)
        .unwrap();
    let local = selectors.inherited.clone();
    assert!(selectors.session.same_authority(&authority));
    assert_eq!(counts(&authority).in_flight, 1);
    assert_eq!(counts(&local).in_flight, 1);
    assert_eq!(counts(&selectors.strict), CopyCounts::default());
    drop(selectors);
    std::thread::spawn(move || drop(operation)).join().unwrap();
    assert_eq!(counts(&authority).failed_requested_bytes, 12);
    assert_eq!(counts(&authority).failed, 1);
    assert_eq!(counts(&authority).in_flight, 0);
    assert_eq!(counts(&local), counts(&authority));
}

#[test]
fn ffi_input_policy_compatibility_conversion_is_attributed_to_session() {
    Python::attach(|py| {
        pyo3_async_runtimes::tokio::run(py, async {
            let session =
                build_session(Arc::new(PipelineExecutor::new().unwrap()), manifest()).await?;
            let copies = session
                .handle
                .lock()
                .await
                .as_ref()
                .unwrap()
                .transport_copy_context();
            send(&session, false).await?;
            assert!(send(&session, true).await.is_err());
            let output = session
                .data_rx
                .lock()
                .await
                .as_mut()
                .unwrap()
                .recv()
                .await
                .unwrap();
            assert!(matches!(output.data, RuntimeData::Binary(bytes) if &*bytes == [1, 2, 3, 4]));
            assert_eq!(counts(&copies).copied_bytes, 4);
            assert_eq!(counts(&copies).rejected_bytes, 4);
            finish(&session).await;
            Python::attach(|py| {
                let report = session.input_copy_report(py).unwrap();
                let report = report.bind(py);
                assert_eq!(
                    report
                        .get_item("session")
                        .unwrap()
                        .get_item("boundary")
                        .unwrap()
                        .extract::<String>()
                        .unwrap(),
                    copies.boundary()
                );
                assert_eq!(
                    report
                        .get_item("allow_copies")
                        .unwrap()
                        .get_item("policy")
                        .unwrap()
                        .extract::<String>()
                        .unwrap(),
                    "allow_copies"
                );
            });
            Ok(())
        })
        .unwrap()
    });
}

#[test]
fn ffi_input_policy_shared_participants_use_one_authority_and_keep_owners() {
    Python::attach(|py| {
        pyo3_async_runtimes::tokio::run(py, async {
            for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
                let executor = Arc::new(
                    PipelineExecutor::with_config(ExecutorConfig {
                        transport_materialization_policy: policy,
                        ..Default::default()
                    })
                    .unwrap(),
                );
                let shared = executor
                    .get_or_create_shared_session(
                        "input-policy-room".to_owned(),
                        Arc::new(serde_json::from_str(&manifest()).unwrap()),
                    )
                    .await
                    .unwrap();
                let copies = shared.transport_copy_context().await;
                let mut participants = Vec::new();
                for id in ["alice", "bob"] {
                    let session = build_shared_session(
                        executor.clone(),
                        manifest(),
                        "input-policy-room".to_owned(),
                        id.into(),
                        "user".into(),
                        None,
                        None,
                        None,
                    )
                    .await?;
                    for strict in [false, true] {
                        assert_eq!(
                            send(&session, strict).await.is_ok(),
                            !strict && policy == CopyPolicy::AllowCopies
                        );
                    }
                    participants.push(session);
                }
                let count = counts(&copies);
                assert_eq!(
                    count.copied_bytes,
                    if policy == CopyPolicy::AllowCopies {
                        8
                    } else {
                        0
                    }
                );
                assert_eq!(
                    count.rejected_bytes,
                    if policy == CopyPolicy::AllowCopies {
                        8
                    } else {
                        16
                    }
                );
                // Immutable Python storage is retained even when strict conversion applies.
                let (future, pointer) = Python::attach(|py| {
                    let input = PyBytes::new(py, &[9, 8, 7, 6, 5]);
                    let pointer = input.as_bytes().as_ptr() as usize;
                    Ok::<_, PyErr>((
                        pyo3_async_runtimes::tokio::into_future(participants[0].send_input(
                            py,
                            input.into_any(),
                            false,
                        )?)?,
                        pointer,
                    ))
                })?;
                future.await?;
                let retained = loop {
                    let output = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                        participants[0]
                            .data_rx
                            .lock()
                            .await
                            .as_mut()
                            .unwrap()
                            .recv()
                            .await
                            .unwrap()
                    })
                    .await
                    .unwrap();
                    if let RuntimeData::Binary(bytes) = output.data {
                        if bytes.len() == 5 {
                            break bytes;
                        }
                    }
                };
                assert_eq!(retained.as_ptr() as usize, pointer);
                for session in &participants {
                    finish(session).await;
                }
                shared.close().await.unwrap();
                assert_eq!(&*retained, &[9, 8, 7, 6, 5]);
                assert_eq!(counts(&copies).copied_bytes, count.copied_bytes);
                assert_eq!(counts(&copies).rejected_bytes, count.rejected_bytes);
            }
            Ok(())
        })
        .unwrap()
    });
}
