use super::*;
use remotemedia_types::{CopyCounts, CopyReason, ImageFormat, PixelFormat};

fn legacy(mode: &str) -> InProcessPythonNode {
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| install_fixture(include_str!("legacy_fixture.py"), c"legacy_fixture"));
    InProcessPythonNode::with_session(
        "python".into(),
        "legacy_fixture.LegacyNode",
        &serde_json::json!({"mode":mode}),
        "session".into(),
    )
    .unwrap()
}

fn allowed(label: &str) -> (NodeRuntimeContext, CopyContext) {
    let copies = CopyContext::new(CopyPolicy::AllowCopies, label);
    (
        NodeRuntimeContext::for_test("session", "python").with_copy_context(copies.clone()),
        copies,
    )
}
fn counts(copies: &CopyContext, reason: CopyReason) -> CopyCounts {
    copies
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == reason)
        .unwrap()
        .1
}
fn check(copies: &CopyContext, exported: u64, imported: u64) {
    assert_eq!(
        counts(copies, CopyReason::BorrowedInput).copied_bytes,
        exported
    );
    assert_eq!(
        counts(copies, CopyReason::VecMaterialization).copied_bytes,
        imported
    );
    assert_eq!(
        counts(copies, CopyReason::PortableSerialization).copied_bytes,
        0
    );
    assert_eq!(
        counts(copies, CopyReason::PortableDeserialization).copied_bytes,
        0
    );
    assert!(copies
        .report()
        .entries
        .iter()
        .all(|(_, c)| c.in_flight == 0));
}
fn bulk_cases() -> Vec<RuntimeData> {
    vec![
        audio(),
        RuntimeData::Binary(vec![5u8; 16].into()),
        RuntimeData::Video {
            pixel_data: vec![7u8; 6].into(),
            width: 2,
            height: 1,
            format: PixelFormat::Rgb24,
            codec: None,
            frame_number: 3,
            is_keyframe: true,
            timestamp_us: 100,
            stream_id: Some("camera".into()),
            arrival_ts_us: Some(120),
        },
        RuntimeData::Image {
            data: vec![9u8; 6].into(),
            format: ImageFormat::Raw {
                pixel_format: PixelFormat::Rgb24,
            },
            width: 2,
            height: 1,
            timestamp_us: Some(200),
            stream_id: Some("photo".into()),
            metadata: None,
        },
        RuntimeData::Tensor {
            data: vec![0u8; 16].into(),
            shape: vec![2, 2],
            dtype: 1,
            metadata: None,
        },
        RuntimeData::Numpy {
            data: vec![0u8; 16].into(),
            shape: vec![2, 2],
            dtype: "float32".into(),
            strides: vec![8, 4],
            c_contiguous: true,
            f_contiguous: false,
        },
    ]
}

#[tokio::test]
async fn legacy_calls_account_all_bulk_types_without_portable_wire_copies() {
    let node = legacy("echo");
    for data in bulk_cases() {
        let (ctx, copies) = allowed("legacy-single");
        let bytes = data.bulk_payload_bytes() as u64;
        let output = node.process_with_ctx(data.clone(), &ctx).await.unwrap();
        assert_eq!(output, data);
        check(&copies, bytes, bytes);
    }
}

#[tokio::test]
async fn legacy_streaming_accounts_each_output_under_the_callers_ledger() {
    let node = legacy("echo");
    for data in bulk_cases() {
        let (ctx, copies) = allowed("legacy-stream");
        let bytes = data.bulk_payload_bytes() as u64;
        let mut outputs = Vec::new();
        assert_eq!(
            node.process_streaming_with_ctx(data.clone(), &ctx, |output| {
                outputs.push(output);
                Ok(())
            })
            .await
            .unwrap(),
            3
        );
        assert_eq!(outputs, vec![data; 3]);
        check(&copies, bytes, bytes * 3);
    }
}

#[tokio::test]
async fn legacy_numpy_output_converts_strided_samples_with_exact_accounting() {
    let node = legacy("numpy");
    let (ctx, copies) = allowed("numpy-output");
    let mut input = audio();
    if let RuntimeData::Audio { samples, .. } = &mut input {
        *samples = vec![1.0, 2.0, 3.0, 4.0].into();
    }
    let output = node.process_with_ctx(input, &ctx).await.unwrap();
    assert_eq!(samples(&output).as_slice(), &[4.0, 3.0, 2.0, 1.0]);
    check(&copies, 16, 16);
}

#[tokio::test]
async fn legacy_repeated_and_concurrent_calls_do_not_rebind_or_mix_ledgers() {
    let node = legacy("echo");
    let (first, first_copies) = allowed("same-label");
    let (second, second_copies) = allowed("same-label");
    let a = node.process_with_ctx(RuntimeData::Binary(vec![1; 3].into()), &first);
    let b = node.process_with_ctx(RuntimeData::Binary(vec![2; 7].into()), &second);
    let (a, b) = tokio::join!(a, b);
    assert!(a.is_ok() && b.is_ok());
    check(&first_copies, 3, 3);
    check(&second_copies, 7, 7);
    node.process_with_ctx(RuntimeData::Binary(vec![3; 2].into()), &first)
        .await
        .unwrap();
    check(&first_copies, 5, 5);
    check(&second_copies, 7, 7);
}

#[tokio::test]
async fn legacy_failures_preserve_completed_and_failed_transfer_accounting() {
    for (mode, message, failed_bytes) in [
        ("error", "legacy process sentinel", 0),
        ("bad_samples", "TypeError", 12),
        ("changing", "length changed", 4),
    ] {
        let node = legacy(mode);
        let (ctx, copies) = allowed(mode);
        let error = node.process_with_ctx(audio(), &ctx).await.unwrap_err();
        assert!(error.to_string().contains(message), "{error}");
        check(&copies, 16, 0);
        let failed = counts(&copies, CopyReason::VecMaterialization);
        assert_eq!(failed.failed_requested_bytes, failed_bytes);
        assert_eq!(failed.failed, u64::from(failed_bytes > 0));
    }
    let node = legacy("stream_error");
    let (ctx, copies) = allowed("stream-error");
    assert!(node
        .process_streaming_with_ctx(audio(), &ctx, |_| Ok(()))
        .await
        .unwrap_err()
        .to_string()
        .contains("legacy stream sentinel"));
    check(&copies, 16, 16);
}

#[tokio::test]
async fn legacy_missing_strict_and_wrong_identity_reject_before_python_initialization() {
    let node = legacy("echo");
    let missing = UnboundContext;
    let (strict, _, _) = context();
    for ctx in [&missing as &dyn NodeRuntimeContextRead, &strict] {
        assert!(node
            .process_with_ctx(audio(), ctx)
            .await
            .unwrap_err()
            .to_string()
            .contains("requires allow_copies"));
        assert!(node
            .process_streaming_with_ctx(audio(), ctx, |_| Ok(()))
            .await
            .is_err());
        assert!(node
            .process_multi_with_ctx([("input".into(), audio())].into_iter().collect(), ctx)
            .await
            .is_err());
        assert!(node.executor.lock().await.is_none());
    }
    let (_, copies) = allowed("wrong");
    let wrong = NodeRuntimeContext::for_test("other", "python").with_copy_context(copies.clone());
    assert!(node
        .process_with_ctx(audio(), &wrong)
        .await
        .unwrap_err()
        .to_string()
        .contains("identity mismatch"));
    check(&copies, 0, 0);
    assert!(node.executor.lock().await.is_none());
}

#[tokio::test]
async fn legacy_named_input_fallback_preserves_the_callers_accounting() {
    let node = legacy("echo");
    let (ctx, copies) = allowed("named-input");
    let output = node
        .process_multi_with_ctx([("input".into(), audio())].into_iter().collect(), &ctx)
        .await
        .unwrap();
    assert_eq!(output, audio());
    check(&copies, 16, 16);
}

#[tokio::test]
async fn real_session_reports_legacy_python_transfers_under_its_node_authority() {
    use remotemedia_core::transport::{ExecutorConfig, PipelineExecutor, TransportData};
    drop(legacy("echo")); // Install the real Python fixture before session startup.
    let executor = PipelineExecutor::with_config(ExecutorConfig {
        node_materialization_policy: CopyPolicy::AllowCopies,
        ..Default::default()
    })
    .unwrap();
    executor
        .register_factory(Arc::new(DynamicInProcessPythonNodeFactory::new(
            PythonNodeConfig::new("LegacyFixture")
                .with_python_class("legacy_fixture.LegacyNode")
                .with_inprocess(true)
                .with_multi_output(true),
        )))
        .await;
    let manifest = super::contracts::manifest(
        serde_json::json!([{"id":"legacy","node_type":"LegacyFixture"}]),
        serde_json::json!([]),
    );
    let mut session = executor.create_session(manifest).await.unwrap();
    let copies = session
        .node_materialization_copies()
        .context("legacy")
        .unwrap();
    let mut outputs = session.take_output_receivers().unwrap();
    session
        .send_input(TransportData::new(audio()))
        .await
        .unwrap();
    for _ in 0..3 {
        let output = super::executor::receive(&mut outputs.audio_rx, &mut session).await;
        // Session ingress assigns its own arrival timestamp.
        assert_eq!(samples(&output).as_slice(), &[1.0; 4]);
    }
    session.signal_input_complete();
    tokio::time::timeout(std::time::Duration::from_secs(10), session.wait())
        .await
        .unwrap()
        .unwrap();
    check(&copies, 16, 48);
    assert!(copies
        .report()
        .boundary
        .contains("/node/legacy/materialization"));
}
