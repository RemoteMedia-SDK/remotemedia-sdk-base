use super::*;

#[tokio::test]
async fn strict_executor_uses_exact_session_mapping_for_python_outputs() {
    drop(node("pass"));
    let mut contract = declaration(BufferProduction::ForwardOrOutputAllocator);
    for port in contract
        .inputs
        .values_mut()
        .chain(contract.outputs.values_mut())
    {
        port.domains = vec![BufferDomain::SessionMapped];
    }
    contract.retention = Some(BufferRetention {
        owners: 2,
        bytes: 8192,
    });
    let executor = PipelineExecutor::with_config(ExecutorConfig {
        buffer_preflight: Some(BufferPreflightConfig {
            policy: CopyPolicy::RequireZeroCopy,
            node_retention_budget: BufferRetention {
                owners: 4,
                bytes: 16384,
            },
        }),
        node_materialization_policy: CopyPolicy::RequireZeroCopy,
        mapped_pool: Some(PoolConfig {
            classes: vec![SizeClass {
                bytes: 4096,
                slots: 4,
            }],
            max_bytes: 16384,
            max_leases: 8,
        }),
        ..Default::default()
    })
    .unwrap();
    executor.register_factory(Arc::new(factory(contract))).await;
    let manifest = manifest(
        serde_json::json!([
            {"id":"produce", "node_type":"SharedNode", "params":{"mode":"gain"}},
            {"id":"forward", "node_type":"SharedNode", "params":{"mode":"pass"}}
        ]),
        serde_json::json!([{"from":"produce", "to":"forward"}]),
    );
    let mut session = executor.create_session(manifest).await.unwrap();
    assert!(session.buffer_preflight().unwrap().issues.is_empty());
    let pool = session.mapped_pool().unwrap().clone();
    let mut loan = session.output_allocator().allocate_audio(4).unwrap();
    loan.as_mut_slice().fill(1.0);
    let mut input = audio();
    let RuntimeData::Audio { samples, .. } = &mut input else {
        panic!()
    };
    *samples = loan.freeze().into();
    let mut outputs = session.take_output_receivers().unwrap();
    session.send_input(TransportData::new(input)).await.unwrap();
    let output = receive(&mut outputs.audio_rx, &mut session).await;
    assert!(pool.owns_payload(&output));
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
    assert_eq!(
        samples.as_ptr() as u64,
        metadata.unwrap()["producer_address"].as_u64().unwrap()
    );
    assert_eq!(samples.as_slice(), &[0.5; 4]);
}
