use super::*;
use remotemedia_core::nodes::StreamingNodeRegistry;
use remotemedia_core::transport::session_router::{ClientOutputRouter, DataPacket, SessionRouter};
use std::time::Duration;

#[tokio::test]
async fn real_router_preserves_python_producer_allocation_through_python_forwarder() {
    // Install the fixture module; the router creates separate node instances.
    drop(node("pass"));
    let manifest = serde_json::from_value(serde_json::json!({
        "version": "v1", "metadata": {"name": "inprocess-shared-runtime"},
        "nodes": [
            {"id": "produce", "node_type": "SharedNode", "params": {"mode": "gain"}},
            {"id": "forward", "node_type": "SharedNode", "params": {"mode": "pass"}}
        ],
        "connections": [{"from": "produce", "to": "forward"}]
    }))
    .unwrap();
    let mut registry = StreamingNodeRegistry::new();
    registry.register(Arc::new(DynamicInProcessPythonNodeFactory::new(
        PythonNodeConfig::new("SharedNode")
            .with_python_class("inprocess_runtime_fixture.SharedNode")
            .with_shared_buffers(),
    )));
    let (output, mut outputs) = ClientOutputRouter::new(2);
    let (mut router, shutdown) = SessionRouter::new(
        "graph-session".into(),
        Arc::new(manifest),
        Arc::new(registry),
        output,
    )
    .unwrap();
    let (_, pool, _) = context();
    router.set_output_allocator(pool.clone());
    let copies = router.set_node_materialization_policy(CopyPolicy::RequireZeroCopy);
    let input = router.get_input_sender();
    let mut running = tokio::spawn(router.run_public());
    input
        .send(DataPacket {
            data: audio(),
            from_node: "client".into(),
            to_node: Some("produce".into()),
            session_id: "graph-session".into(),
            sequence: 1,
            sub_sequence: 0,
            metadata: Default::default(),
        })
        .await
        .unwrap();
    let received = tokio::time::timeout(Duration::from_secs(10), outputs.audio_rx.recv()).await;
    drop(input);
    if received.is_err() {
        let _ = shutdown.send(()).await;
    }
    let joined = tokio::time::timeout(Duration::from_secs(10), &mut running).await;
    if joined.is_err() {
        running.abort();
        let _ = running.await;
        panic!("in-process router failed to join");
    }
    joined.unwrap().unwrap().unwrap();
    let output = received
        .expect("missing audio output")
        .expect("output channel closed");
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
    let owner = samples.into_shared();
    drop(
        pool.export(&owner, remotemedia_shared_memory::PeerId(1))
            .unwrap(),
    );
    for report in copies.reports().values() {
        assert!(report
            .entries
            .iter()
            .all(|(_, c)| c.completed == 0 && c.failed == 0 && c.in_flight == 0));
    }
    assert!(pool.allocate::<f32>(4).is_err());
    drop(owner);
    assert!(pool.allocate::<f32>(4).is_ok());
}
