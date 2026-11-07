use super::*;
use crate::registry::PythonNodeConfig;
use pyo3::{prelude::*, types::PyModule};
use remotemedia_core::nodes::{NodeRuntimeContext, StreamingNode, StreamingNodeFactory};
use remotemedia_shared_memory::{PoolConfig, SessionId, SharedPool, SizeClass};
use remotemedia_types::{CopyContext, CopyPolicy};

mod contracts;
mod executor;
mod guards;
mod router;

fn node(mode: &str) -> Box<dyn StreamingNode> {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        Python::attach(|py| {
            let code = std::ffi::CString::new(include_str!("fixture.py")).unwrap();
            let module = PyModule::from_code(
                py,
                &code,
                c"inprocess_runtime_fixture.py",
                c"inprocess_runtime_fixture",
            )
            .unwrap();
            py.import("sys")
                .unwrap()
                .getattr("modules")
                .unwrap()
                .set_item("inprocess_runtime_fixture", module)
                .unwrap();
        })
    });
    DynamicInProcessPythonNodeFactory::new(
        PythonNodeConfig::new("SharedFixture")
            .with_python_class("inprocess_runtime_fixture.SharedNode")
            .with_multi_output(true)
            .with_shared_buffers(),
    )
    .create(
        "python".into(),
        &serde_json::json!({"mode": mode}),
        Some("session".into()),
    )
    .unwrap()
}

fn context() -> (NodeRuntimeContext, Arc<SharedPool>, CopyContext) {
    let pool = Arc::new(
        SharedPool::new(
            SessionId([91; 16]),
            PoolConfig {
                classes: vec![SizeClass {
                    bytes: 4096,
                    slots: 1,
                }],
                max_bytes: 4096,
                max_leases: 4,
            },
        )
        .unwrap(),
    );
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "inprocess-runtime-test");
    let ctx = NodeRuntimeContext::for_test("session", "python")
        .with_output_allocator(pool.clone())
        .with_copy_context(copies.clone());
    (ctx, pool, copies)
}

fn audio() -> RuntimeData {
    RuntimeData::Audio {
        samples: vec![1.0; 4].into(),
        sample_rate: 48_000,
        channels: 1,
        stream_id: Some("voice".into()),
        timestamp_us: Some(7),
        arrival_ts_us: Some(9),
        metadata: None,
    }
}

fn samples(data: &RuntimeData) -> &remotemedia_types::AudioSamples {
    match data {
        RuntimeData::Audio { samples, .. } => samples,
        _ => panic!("expected audio"),
    }
}

async fn state(node: &dyn StreamingNode, ctx: &NodeRuntimeContext) -> Value {
    match node
        .process_async(RuntimeData::Json(serde_json::json!({})), ctx)
        .await
        .unwrap()
    {
        RuntimeData::Json(value) => value,
        _ => panic!("expected state"),
    }
}

#[tokio::test]
async fn factory_pass_through_keeps_pointer_identity_and_call_identity() {
    let node = node("pass");
    let (ctx, _pool, copies) = context();
    let input = audio();
    let pointer = samples(&input).as_ptr();
    let output = node.process_async(input, &ctx).await.unwrap();
    assert_eq!(samples(&output).as_ptr(), pointer);
    let state = state(node.as_ref(), &ctx).await;
    assert_eq!(state["session"], "session");
    assert_eq!(state["node"], "python");
    assert!(copies
        .report()
        .entries
        .iter()
        .all(|(_, c)| c.completed == 0 && c.failed == 0 && c.in_flight == 0));
    drop(node);
    assert_eq!(samples(&output).as_slice(), &[1.0; 4]);
}

#[tokio::test]
async fn producer_uses_selected_pool_and_retained_output_reclaims_on_last_drop() {
    let node = node("gain");
    let (ctx, pool, _) = context();
    let output = node.process_async(audio(), &ctx).await.unwrap();
    let RuntimeData::Audio {
        samples, metadata, ..
    } = output
    else {
        panic!()
    };
    assert_eq!(
        samples.as_ptr() as u64,
        metadata.unwrap()["producer_address"].as_u64().unwrap()
    );
    let owner = samples.into_shared();
    drop(
        pool.export(&owner, remotemedia_shared_memory::PeerId(1))
            .unwrap(),
    );
    assert!(pool.allocate::<f32>(4).is_err());
    drop(node);
    drop(ctx);
    assert_eq!(owner.as_slice(), &[0.5; 4]);
    drop(owner);
    assert!(pool.allocate::<f32>(4).is_ok());
}

#[tokio::test]
async fn callback_failure_stops_generator_and_closes_retained_iterator() {
    let node = node("pass");
    let (ctx, pool, _) = context();
    let retained = Arc::new(std::sync::Mutex::new(None));
    let received = retained.clone();
    let error = node
        .process_streaming_async(
            audio(),
            &ctx,
            Box::new(move |output| {
                *received.lock().unwrap() = Some(output);
                Err(Error::Execution("consumer-stop".into()))
            }),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("consumer-stop"));
    let state = state(node.as_ref(), &ctx).await;
    assert_eq!(state["produced"], 1);
    assert_eq!(state["closed"], true);
    assert_eq!(
        samples(retained.lock().unwrap().as_ref().unwrap()).as_slice(),
        &[0.0; 4]
    );
    assert!(pool.allocate::<f32>(4).is_err());
    drop(retained);
    assert!(pool.allocate::<f32>(4).is_ok());
}

#[tokio::test]
async fn shared_private_output_and_mismatched_session_are_rejected() {
    let node = node("private");
    let (ctx, _, _) = context();
    assert!(node.process_async(audio(), &ctx).await.is_err());
    let wrong = NodeRuntimeContext::for_test("other-session", "python");
    assert!(node
        .process_async(audio(), &wrong)
        .await
        .unwrap_err()
        .to_string()
        .contains("identity mismatch"));
}

#[tokio::test]
async fn streaming_delivers_before_next_allocation_with_a_single_slot() {
    let node = node("pass");
    let (ctx, pool, _) = context();
    let values = Arc::new(std::sync::Mutex::new(Vec::new()));
    let received = values.clone();
    let count = node
        .process_streaming_async(
            audio(),
            &ctx,
            Box::new(move |output| {
                received.lock().unwrap().push(samples(&output)[0]);
                Ok(())
            }),
        )
        .await
        .unwrap();
    assert_eq!(count, 3);
    assert_eq!(*values.lock().unwrap(), vec![0.0, 1.0, 2.0]);
    assert_eq!(state(node.as_ref(), &ctx).await["closed"], true);
    assert!(pool.allocate::<f32>(4).is_ok());
}

#[tokio::test]
async fn shared_calls_without_context_do_not_invent_heap_allocation() {
    let mut shared = InProcessPythonNode::new(
        "python".into(),
        "missing_module.Missing",
        &serde_json::json!({}),
    )
    .unwrap();
    shared.shared_buffers = true;
    assert!(shared
        .process(audio())
        .await
        .unwrap_err()
        .to_string()
        .contains("requires a runtime context"));
    assert!(shared
        .process_streaming(audio(), None, |_| Ok(()))
        .await
        .unwrap_err()
        .to_string()
        .contains("requires a runtime context"));
    assert!(shared.executor.lock().await.is_none());
}

#[tokio::test]
async fn legacy_strict_call_is_rejected_before_importing_python() {
    let legacy = InProcessPythonNode::new(
        "python".into(),
        "missing_module.Missing",
        &serde_json::json!({}),
    )
    .unwrap();
    let (ctx, _, _) = context();
    assert!(legacy
        .process_with_ctx(audio(), &ctx)
        .await
        .unwrap_err()
        .to_string()
        .contains("requires allow_copies"));
    assert!(legacy.executor.lock().await.is_none());
}
