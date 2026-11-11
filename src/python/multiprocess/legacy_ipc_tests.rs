//! Focused tests of the production legacy IPC thread and fan-out.
use super::*;

#[tokio::test]
async fn legacy_ipc_initial_binding_rejects_rebind_and_retroactive_attribution() {
    use remotemedia_types::CopyPolicy;
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "initial");
    let mut executor = MultiprocessExecutor::new(MultiprocessConfig::default());
    assert!(executor
        .bind_initial_copy_context(CopyContext::new(CopyPolicy::RequireZeroCopy, "strict"))
        .is_err());
    assert!(executor.initial_copies.is_none());
    executor.bind_initial_copy_context(copies.clone()).unwrap();
    executor.bind_initial_copy_context(copies.clone()).unwrap();
    assert!(executor
        .bind_initial_copy_context(CopyContext::new(CopyPolicy::AllowCopies, "initial"))
        .is_err());
    let prior = ExecutorNodeContext {
        node_id: "first".into(),
        node_type: "NoSuchPythonNode".into(),
        session_id: Some("session".into()),
        params: serde_json::json!({}),
        metadata: HashMap::new(),
    };
    executor.initial_scope = Some((prior.node_id.clone(), prior.session_id.clone().unwrap()));
    // Cleanup clears current execution state, but must retain the bound identity.
    ExecutorNodeExecutor::cleanup(&mut executor).await.unwrap();
    let mut other = prior.clone();
    other.node_id = "other".into();
    assert!(ExecutorNodeExecutor::initialize(&mut executor, &other)
        .await
        .unwrap_err()
        .to_string()
        .contains("another node/session"));
    let mut legacy = MultiprocessExecutor::new(MultiprocessConfig::default());
    legacy.current_context = Some(prior);
    assert!(legacy
        .bind_initial_copy_context(copies)
        .unwrap_err()
        .to_string()
        .contains("retroactively"));
}

#[tokio::test(flavor = "current_thread")]
async fn legacy_ipc_thread_keeps_background_authority_separate_from_requests() {
    verify_background_authority(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn legacy_ipc_initial_authority_covers_output_before_registration() {
    verify_background_authority(true).await;
}

async fn verify_background_authority(bind_before_spawn: bool) {
    use futures::FutureExt;
    use remotemedia_types::{CopyPolicy, CopyReason, RuntimeData};
    use std::panic::AssertUnwindSafe;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    let mut executor = MultiprocessExecutor::new(MultiprocessConfig::default());
    let session = format!(
        "ledger-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let node = "fixture";
    let output_copies =
        CopyContext::new(CopyPolicy::AllowCopies, format!("session/{session}/output"));
    if bind_before_spawn {
        executor
            .bind_initial_copy_context(output_copies.clone())
            .unwrap();
    }
    let registry = executor.channel_registry().clone();
    let mut channels = Vec::new();
    for suffix in ["input", "output", "control"] {
        channels.push(
            registry
                .create_channel(&format!("{session}/{suffix}"), 8, false)
                .await
                .unwrap(),
        );
    }
    let output_publisher = registry.create_publisher(&channels[1].name).await.unwrap();
    let mut thread = executor
        .spawn_ipc_thread(
            node,
            &session,
            &channels[0].name,
            &channels[1].name,
            &channels[2].name,
        )
        .await
        .unwrap();
    global_sessions()
        .write()
        .await
        .entry(session.clone())
        .or_default()
        .insert(node.into(), thread.command_tx.clone());
    let (callback_tx, mut callback_rx) = tokio::sync::mpsc::channel(1);
    let requests = [
        CopyContext::new(CopyPolicy::AllowCopies, "request/a"),
        CopyContext::new(CopyPolicy::AllowCopies, "request/b"),
    ];
    let value = RuntimeData::Tensor {
        data: vec![5; 1024].into(),
        shape: vec![1024],
        dtype: 5,
        metadata: None,
    };
    let wire = crate::multiprocess::data_transfer::to_python_wire(&value, &session).unwrap();
    // Always close receivers/remove routing and join the actual IPC thread, even
    // when a test assertion fails. No Python worker or model is started here.
    let result = AssertUnwindSafe(async {
        let mut responses = thread.response_rx.lock().await;
        if bind_before_spawn {
            let startup_input = CopyContext::new(CopyPolicy::AllowCopies, "startup-input");
            executor
                .send_data_to_node_with_copy_context(
                    node,
                    &session,
                    RuntimeData::Text("ready".into()),
                    &startup_input,
                )
                .await
                .unwrap();
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(5), responses.recv())
                    .await
                    .unwrap(),
                Some(IpcResponse::SendComplete)
            ));
            output_publisher.send(&wire).unwrap();
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(5), responses.recv())
                    .await
                    .unwrap(),
                Some(IpcResponse::OutputData(_))
            ));
        }
        let callback_copies = if bind_before_spawn {
            CopyContext::new(CopyPolicy::AllowCopies, "later-callback")
        } else {
            output_copies.clone()
        };
        executor
            .register_output_callback_with_copy_context(
                node,
                &session,
                callback_tx,
                &callback_copies,
            )
            .await
            .unwrap();
        let mut retained = None;
        for index in 0..3 {
            if index < requests.len() {
                executor
                    .send_data_to_node_with_copy_context(
                        node,
                        &session,
                        RuntimeData::Text("input".into()),
                        &requests[index],
                    )
                    .await
                    .unwrap();
                // FIFO SendComplete also proves registration and port setup have
                // completed before the fixture publishes output.
                assert!(matches!(
                    tokio::time::timeout(Duration::from_secs(5), responses.recv())
                        .await
                        .unwrap(),
                    Some(IpcResponse::SendComplete)
                ));
            }
            // Third output is autonomous: no input request changes its authority.
            output_publisher.send(&wire).unwrap();
            let callback = tokio::time::timeout(Duration::from_secs(5), callback_rx.recv())
                .await
                .unwrap()
                .unwrap();
            let Some(IpcResponse::OutputData(response)) =
                tokio::time::timeout(Duration::from_secs(5), responses.recv())
                    .await
                    .unwrap()
            else {
                panic!("missing response output")
            };
            assert_eq!(callback.as_slice().as_ptr(), response.as_slice().as_ptr());
            assert_eq!(callback.as_slice(), wire);
            let data = MultiprocessExecutor::from_ipc_runtime_data_with_copy_context(
                callback,
                &output_copies,
            )
            .unwrap();
            let RuntimeData::Tensor { data, .. } = data else {
                panic!("missing tensor")
            };
            assert_eq!(
                data.as_slice().as_ptr(),
                response.as_slice()[wire.len() - 1024..].as_ptr()
            );
            retained = Some(data.slice(7..23).unwrap());
        }
        let copied = |context: &CopyContext, reason| {
            context
                .report()
                .entries
                .into_iter()
                .find(|(r, _)| *r == reason)
                .unwrap()
                .1
        };
        assert_eq!(
            copied(&output_copies, CopyReason::PrivateIpcIngress).copied_bytes,
            (wire.len() * if bind_before_spawn { 4 } else { 3 }) as u64
        );
        assert_eq!(
            copied(&output_copies, CopyReason::PortableDeserialization).copied_bytes,
            0
        );
        assert_eq!(
            copied(&output_copies, CopyReason::BorrowedInput).copied_bytes,
            0
        );
        if bind_before_spawn {
            assert_eq!(
                copied(&callback_copies, CopyReason::PrivateIpcIngress).copied_bytes,
                0
            );
        }
        for request in &requests {
            assert_eq!(copied(request, CopyReason::BorrowedInput).completed, 1);
            assert_eq!(
                copied(request, CopyReason::PrivateIpcIngress).copied_bytes,
                0
            );
        }
        retained.unwrap()
    })
    .catch_unwind()
    .await;
    drop(callback_rx);
    global_sessions().write().await.remove(&session);
    let _ = thread.command_tx.send(IpcCommand::Shutdown).await;
    thread.thread_handle.take().unwrap().join().unwrap();
    drop((thread, output_publisher));
    for channel in channels {
        registry.destroy_channel(channel).await.unwrap();
    }
    match result {
        Ok(retained) => assert_eq!(retained.as_slice(), &[5; 16]),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[test]
fn legacy_ipc_fanout_retains_one_owner_until_last_slice() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Owner(Vec<u8>, Arc<AtomicUsize>);
    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }
    impl Drop for Owner {
        fn drop(&mut self) {
            self.1.fetch_add(1, Ordering::SeqCst);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let owner = Owner(vec![7; 65536], drops.clone());
    let pointer = owner.0.as_ptr();
    let bytes = SharedBytes::from_owner(owner);
    let (callback_tx, mut callback_rx) = tokio::sync::mpsc::channel(1);
    let (response_tx, mut response_rx) = tokio::sync::mpsc::channel(1);
    deliver_ipc_output(bytes, None, &mut Some(callback_tx), &response_tx);
    let callback = callback_rx.blocking_recv().unwrap();
    let IpcResponse::OutputData(response) = response_rx.blocking_recv().unwrap() else {
        panic!("missing output")
    };
    assert_eq!(callback.as_slice().as_ptr(), pointer);
    assert_eq!(response.as_slice().as_ptr(), pointer);
    let tail = response.slice(5..19).unwrap();
    drop((response, callback, callback_rx, response_rx, response_tx));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(tail.as_slice(), &[7; 14]);
    drop(tail);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn legacy_ipc_callback_pressure_then_close_preserves_response() {
    let (callback_tx, callback_rx) = tokio::sync::mpsc::channel(1);
    callback_tx.blocking_send(vec![1].into()).unwrap();
    let (response_tx, mut response_rx) = tokio::sync::mpsc::channel(1);
    let bytes = vec![2; 4096];
    let pointer = bytes.as_ptr() as usize;
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut callback = Some(callback_tx);
        started_tx.send(()).unwrap();
        deliver_ipc_output(bytes.into(), None, &mut callback, &response_tx);
        done_tx.send(callback.is_none()).unwrap();
    });
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    assert!(done_rx
        .recv_timeout(std::time::Duration::from_millis(30))
        .is_err());
    assert!(matches!(
        response_rx.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
    drop(callback_rx);
    assert!(done_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap());
    worker.join().unwrap();
    let IpcResponse::OutputData(response) = response_rx.blocking_recv().unwrap() else {
        panic!("missing output")
    };
    assert_eq!(response.as_slice().as_ptr() as usize, pointer);
    assert_eq!(response.as_slice(), &[2; 4096]);
}

#[test]
fn legacy_ipc_closed_response_does_not_discard_callback_output() {
    let (callback_tx, mut callback_rx) = tokio::sync::mpsc::channel(1);
    let (response_tx, response_rx) = tokio::sync::mpsc::channel(1);
    drop(response_rx);
    let bytes = vec![3; 256];
    let pointer = bytes.as_ptr();
    deliver_ipc_output(bytes.into(), None, &mut Some(callback_tx), &response_tx);
    let callback = callback_rx.blocking_recv().unwrap();
    assert_eq!(callback.as_slice().as_ptr(), pointer);
    assert_eq!(callback.as_slice(), &[3; 256]);
}
