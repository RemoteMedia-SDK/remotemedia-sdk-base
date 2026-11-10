use super::{contracts::*, *};
use remotemedia_core::transport::{
    buffer_preflight::BufferPreflightConfig, ExecutorConfig, PipelineExecutor, SessionHandle,
    TransportData,
};
use remotemedia_traits::buffer_contract::*;
use std::time::Duration;

#[cfg(any(windows, target_os = "linux"))]
mod mapped;
mod source;

#[tokio::test]
async fn metadata_only_python_node_needs_no_bulk_budget_or_allocation_code() {
    drop(node("pass"));
    let executor = PipelineExecutor::with_config(ExecutorConfig {
        buffer_preflight: Some(BufferPreflightConfig {
            policy: CopyPolicy::RequireZeroCopy,
            node_retention_budget: BufferRetention {
                owners: 0,
                bytes: 0,
            },
        }),
        node_materialization_policy: CopyPolicy::RequireZeroCopy,
        ..Default::default()
    })
    .unwrap();
    executor
        .register_factory(Arc::new(DynamicInProcessPythonNodeFactory::new(
            PythonNodeConfig::new("TextNode")
                .with_python_class("inprocess_runtime_fixture.TextNode")
                .with_shared_buffer_contract(BufferContract::metadata_only(
                    super::super::contracts::protocol(),
                )),
        )))
        .await;
    let manifest = manifest(
        serde_json::json!([{"id":"text","node_type":"TextNode"}]),
        serde_json::json!([]),
    );
    let mut session = executor.create_session(manifest).await.unwrap();
    assert_eq!(
        session.buffer_preflight().unwrap().declared_node_retention,
        Some(BufferRetention {
            owners: 0,
            bytes: 0
        })
    );
    let mut outputs = session.take_output_receivers().unwrap();
    session
        .send_input(TransportData::new(RuntimeData::Text("simple text".into())))
        .await
        .unwrap();
    let output = receive(&mut outputs.data_rx, &mut session).await;
    session.signal_input_complete();
    tokio::time::timeout(Duration::from_secs(10), session.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output, RuntimeData::Text("SIMPLE TEXT".into()));
}

async fn executor(contract: BufferContract) -> PipelineExecutor {
    drop(node("pass")); // install the fixture module before execution
    let executor = PipelineExecutor::with_config(ExecutorConfig {
        buffer_preflight: Some(BufferPreflightConfig {
            policy: CopyPolicy::RequireZeroCopy,
            node_retention_budget: BufferRetention {
                owners: 4,
                bytes: 64,
            },
        }),
        node_materialization_policy: CopyPolicy::RequireZeroCopy,
        ..Default::default()
    })
    .unwrap();
    executor.register_factory(Arc::new(factory(contract))).await;
    executor
}

pub(super) async fn receive(
    rx: &mut tokio::sync::mpsc::Receiver<RuntimeData>,
    session: &mut SessionHandle,
) -> RuntimeData {
    match tokio::time::timeout(Duration::from_secs(10), rx.recv()).await {
        Ok(Some(data)) => data,
        result => {
            let _ = session.close().await;
            panic!("missing executor output: {result:?}");
        }
    }
}

#[tokio::test]
async fn strict_executor_binds_python_producer_and_forwarder_contracts() {
    let executor = executor(declaration(BufferProduction::ForwardOrOutputAllocator)).await;
    let manifest = manifest(
        serde_json::json!([
            {"id":"produce", "node_type":"SharedNode", "params":{"mode":"gain"}},
            {"id":"forward", "node_type":"SharedNode", "params":{"mode":"pass"}}
        ]),
        serde_json::json!([{"from":"produce", "to":"forward"}]),
    );
    let mut session = executor.create_session(manifest).await.unwrap();
    assert!(session.buffer_preflight().unwrap().issues.is_empty());
    let ledger = session.node_materialization_copies();
    let mut outputs = session.take_output_receivers().unwrap();
    session
        .send_input(TransportData::new(audio()))
        .await
        .unwrap();
    let output = receive(&mut outputs.audio_rx, &mut session).await;
    session.signal_input_complete();
    tokio::time::timeout(Duration::from_secs(10), session.wait())
        .await
        .unwrap()
        .unwrap();
    let RuntimeData::Audio {
        samples, metadata, ..
    } = output
    else {
        panic!()
    };
    assert_eq!(samples.as_slice(), &[0.5; 4]);
    assert_eq!(
        samples.as_ptr() as u64,
        metadata.unwrap()["producer_address"].as_u64().unwrap()
    );
    for report in ledger.reports().values() {
        assert!(report
            .entries
            .iter()
            .all(|(_, c)| c.completed == 0 && c.failed == 0 && c.in_flight == 0));
    }
}

#[tokio::test]
async fn strict_executor_rejects_budget_before_importing_unknown_python_class() {
    let executor = PipelineExecutor::with_config(ExecutorConfig {
        buffer_preflight: Some(BufferPreflightConfig {
            policy: CopyPolicy::RequireZeroCopy,
            node_retention_budget: BufferRetention {
                owners: 1,
                bytes: 16,
            },
        }),
        ..Default::default()
    })
    .unwrap();
    executor
        .register_factory(Arc::new(DynamicInProcessPythonNodeFactory::new(
            PythonNodeConfig::new("SharedNode")
                .with_python_class("does_not_exist.Node")
                .with_shared_buffer_contract(declaration(BufferProduction::ForwardInput)),
        )))
        .await;
    let manifest = manifest(
        serde_json::json!([{"id":"python", "node_type":"SharedNode"}]),
        serde_json::json!([]),
    );
    let error = match executor.create_session(manifest).await {
        Err(error) => error,
        Ok(mut session) => {
            session.close().await.unwrap();
            panic!("accepted insufficient retention budget")
        }
    };
    assert!(
        error.to_string().contains("declared node retention"),
        "{error}"
    );
}

#[tokio::test]
async fn retained_python_slice_exhausts_live_budget_until_explicit_release() {
    let mut contract = declaration(BufferProduction::ForwardInput);
    contract.retention = Some(BufferRetention {
        owners: 1,
        bytes: 16,
    });
    let executor = executor(contract).await;
    let manifest = manifest(
        serde_json::json!([
            {"id":"python", "node_type":"SharedNode", "params":{"mode":"retain"}}
        ]),
        serde_json::json!([]),
    );
    let mut session = executor.create_session(manifest).await.unwrap();
    let mut outputs = session.take_output_receivers().unwrap();
    session
        .send_input(TransportData::new(audio()))
        .await
        .unwrap();
    drop(receive(&mut outputs.audio_rx, &mut session).await);
    // Only Python's escaped slice retains the first input now.
    session
        .send_input(TransportData::new(audio()))
        .await
        .unwrap();
    session
        .send_input(TransportData::new(RuntimeData::Json(serde_json::json!({}))))
        .await
        .unwrap();
    let state = receive(&mut outputs.data_rx, &mut session).await;
    assert_eq!(
        state,
        RuntimeData::Json(serde_json::json!({
            "produced":0, "closed":false, "retained":1,
            "node":"python", "session":session.session_id.as_str(),
        }))
    );
    assert!(outputs.audio_rx.try_recv().is_err());
    session
        .send_input(TransportData::new(RuntimeData::Json(
            serde_json::json!({"clear":true}),
        )))
        .await
        .unwrap();
    let RuntimeData::Json(state) = receive(&mut outputs.data_rx, &mut session).await else {
        panic!()
    };
    assert_eq!(state["retained"], 0);
    session
        .send_input(TransportData::new(audio()))
        .await
        .unwrap();
    let retained = receive(&mut outputs.audio_rx, &mut session).await;
    session.signal_input_complete();
    tokio::time::timeout(Duration::from_secs(10), session.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(samples(&retained).as_slice(), &[1.0; 4]);
}
