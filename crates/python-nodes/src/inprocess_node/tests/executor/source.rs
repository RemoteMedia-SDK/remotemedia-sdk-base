use super::*;
use remotemedia_traits::streaming::{PacingNature, Tick};

fn source_config() -> PythonNodeConfig {
    PythonNodeConfig::new("SourceNode")
        .with_python_class("inprocess_runtime_fixture.SourceNode")
        .with_shared_source(20)
}

#[test]
fn source_configuration_rejects_invalid_rates_and_mode_downgrade() {
    for config in [
        source_config().with_shared_source(0),
        source_config().with_shared_source(1_000_001),
        source_config().with_inprocess(false),
    ] {
        let factory = DynamicInProcessPythonNodeFactory::new(config);
        assert!(factory.try_buffer_contract(&Value::Null).is_err());
        assert!(factory.create("source".into(), &Value::Null, None).is_err());
    }
}

#[tokio::test]
async fn source_rejects_missing_context_allocator_and_wrong_identity_before_import() {
    let mut source = InProcessPythonNode::with_session(
        "python".into(),
        "missing_module.Missing",
        &Value::Null,
        "session".into(),
    )
    .unwrap();
    source.shared_buffers = true;
    source.source_rate_hz = Some(20);
    let tick = Tick {
        clock_id: "wall:20".into(),
        pts_us: 0,
        frame_idx: 0,
        deadline_us: 50_000,
    };
    assert!(source
        .tick(tick.clone(), None, Box::new(|_| Ok(())))
        .await
        .unwrap_err()
        .to_string()
        .contains("requires a runtime context"));
    let wrong = NodeRuntimeContext::for_test("other", "python");
    for (ctx, message) in [
        (
            &UnboundContext as &dyn NodeRuntimeContextRead,
            "requires an output allocator",
        ),
        (&wrong, "identity mismatch"),
    ] {
        assert!(source
            .tick_with_ctx(tick.clone(), ctx, Box::new(|_| Ok(())))
            .await
            .unwrap_err()
            .to_string()
            .contains(message));
    }
    assert!(source.executor.lock().await.is_none());
}

async fn source_executor(
    mapped: bool,
    mode: &str,
) -> (PipelineExecutor, Arc<remotemedia_core::manifest::Manifest>) {
    drop(node("pass"));
    let mut source = declaration(BufferProduction::OutputAllocator);
    let mut forward = declaration(BufferProduction::ForwardInput);
    if mapped {
        for contract in [&mut source, &mut forward] {
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
        }
    }
    let executor = PipelineExecutor::with_config(ExecutorConfig {
        buffer_preflight: Some(BufferPreflightConfig {
            policy: CopyPolicy::RequireZeroCopy,
            node_retention_budget: BufferRetention {
                owners: 4,
                bytes: if mapped { 16384 } else { 64 },
            },
        }),
        node_materialization_policy: CopyPolicy::RequireZeroCopy,
        mapped_pool: mapped.then(|| PoolConfig {
            classes: vec![SizeClass {
                bytes: 4096,
                slots: 1,
            }],
            max_bytes: 4096,
            max_leases: 8,
        }),
        ..Default::default()
    })
    .unwrap();
    executor
        .register_factory(Arc::new(DynamicInProcessPythonNodeFactory::new(
            source_config().with_shared_buffer_contract(source),
        )))
        .await;
    executor.register_factory(Arc::new(factory(forward))).await;
    let manifest = manifest(
        serde_json::json!([
            {"id":"source", "node_type":"SourceNode", "params":{"mode":mode}},
            {"id":"forward", "node_type":"SharedNode"}
        ]),
        serde_json::json!([{"from":"source", "to":"forward"}]),
    );
    (executor, manifest)
}

#[tokio::test]
async fn strict_source_pacer_keeps_host_and_mapped_allocation_identity() {
    for mapped in [false, true] {
        let (executor, manifest) = source_executor(mapped, "source").await;
        let mut session = executor.create_session(manifest).await.unwrap();
        assert!(session.buffer_preflight().unwrap().issues.is_empty());
        let identity = session.session_id.clone();
        let pool = session.mapped_pool().cloned();
        let ledger = session.node_materialization_copies();
        let mut outputs = session.take_output_receivers().unwrap();
        let output = receive(&mut outputs.audio_rx, &mut session).await;
        if let Some(pool) = &pool {
            assert!(pool.owns_payload(&output));
            assert!(pool.allocate::<f32>(4).is_err());
        }
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
        let metadata = metadata.unwrap();
        assert_eq!(
            samples.as_ptr() as u64,
            metadata["producer_address"].as_u64().unwrap()
        );
        assert_eq!(samples.as_slice(), &[0.25; 4]);
        assert_eq!(metadata["frame_idx"], 1); // tick zero was quiet, not EOS
        assert_eq!(metadata["clock_id"], "wall:20");
        assert_eq!(metadata["session"], identity);
        assert_eq!(metadata["node"], "source");
        assert!(metadata["deadline_us"].as_u64().unwrap() >= metadata["pts_us"].as_u64().unwrap());
        drop(samples);
        if let Some(pool) = pool {
            assert!(pool.allocate::<f32>(4).is_ok());
        }
        for report in ledger.reports().values() {
            assert!(report
                .entries
                .iter()
                .all(|(_, c)| c.completed == 0 && c.failed == 0 && c.in_flight == 0));
        }
    }
}

#[tokio::test]
async fn source_tick_failure_reaches_session_wait() {
    let (executor, manifest) = source_executor(false, "fail").await;
    let session = executor.create_session(manifest).await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(10), session.wait())
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("source-tick-failure"), "{error}");
}

#[tokio::test]
async fn source_callback_rejection_closes_generator_and_preserves_retained_output() {
    drop(node("pass"));
    let source = DynamicInProcessPythonNodeFactory::new(source_config())
        .create(
            "python".into(),
            &serde_json::json!({"mode":"many"}),
            Some("session".into()),
        )
        .unwrap();
    assert_eq!(source.pacing_nature(), PacingNature::SourceWall(20));
    let (ctx, pool, _) = context();
    let retained = Arc::new(std::sync::Mutex::new(None));
    let received = retained.clone();
    let error = source
        .tick(
            Tick {
                clock_id: "wall:20".into(),
                pts_us: 50_000,
                frame_idx: 1,
                deadline_us: 100_000,
            },
            &ctx,
            Box::new(move |output| {
                *received.lock().unwrap() = Some(output);
                Err(Error::Execution("source-consumer-stop".into()))
            }),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("source-consumer-stop"));
    let state = state(source.as_ref(), &ctx).await;
    assert_eq!(state["produced"], 1);
    assert_eq!(state["closed"], true);
    assert!(pool.allocate::<f32>(4).is_err());
    drop(source);
    assert_eq!(
        samples(retained.lock().unwrap().as_ref().unwrap()).as_slice(),
        &[0.25; 4]
    );
    drop(retained);
    assert!(pool.allocate::<f32>(4).is_ok());
}
