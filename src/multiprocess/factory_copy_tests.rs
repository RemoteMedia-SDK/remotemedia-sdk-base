use super::*;
use crate::nodes::{InitializeContext, NodeRuntimeContext};
use remotemedia_types::CopyReason;

#[test]
#[ignore = "child-process entrypoint, launched by native compatibility tests"]
fn native_compat_echo_fixture() {
    super::super::runner::Plugin::from_env()
        .unwrap()
        .run(|data| match data {
            RuntimeData::Text(text) if text == "copy-report" => Ok(RuntimeData::Json(
                serde_json::to_value(CopyContext::legacy().report()).unwrap(),
            )),
            RuntimeData::Text(text) if text == "stall" => {
                std::thread::sleep(Duration::from_secs(30));
                Ok(RuntimeData::Text(text))
            }
            RuntimeData::Text(text) if text == "handler-error" => {
                Err("fixture handler rejected input".into())
            }
            RuntimeData::Text(text) if text == "exit" => std::process::exit(29),
            RuntimeData::Text(text) if text == "delay" => {
                std::thread::sleep(Duration::from_millis(150));
                Ok(RuntimeData::Binary(vec![4; 16].into()))
            }
            other => Ok(other),
        })
        .unwrap();
}

fn fixture() -> MultiprocessNativeNode {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args([
        "--exact",
        "multiprocess::factory::copy_tests::native_compat_echo_fixture",
        "--ignored",
        "--nocapture",
    ]);
    MultiprocessNativeNode::spawn_command(
        command,
        "Echo",
        "echo",
        &format!("compat_{}", uuid::Uuid::new_v4().simple()),
    )
    .unwrap()
}

fn context(node: &MultiprocessNativeNode, copies: CopyContext) -> NodeRuntimeContext {
    NodeRuntimeContext::for_test(node.session_id.clone(), node.node_id.clone())
        .with_copy_context(copies)
}

fn counts(copies: &CopyContext, reason: CopyReason) -> remotemedia_types::CopyCounts {
    copies
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == reason)
        .unwrap()
        .1
}

#[tokio::test]
async fn native_compat_context_hooks_reject_strict_before_transport() {
    let node = fixture();
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "native-strict");
    let ctx = context(&node, copies.clone());
    let init = InitializeContext {
        session_id: node.session_id.clone(),
        node_id: node.node_id.clone(),
        control: None,
    }
    .with_copy_context(copies.clone());
    assert!(node
        .initialize(&init)
        .await
        .unwrap_err()
        .to_string()
        .contains("mapped native-worker"));
    for input in [
        RuntimeData::Text("metadata-only".into()),
        RuntimeData::Binary(vec![7; 32].into()),
    ] {
        assert!(node.process_with_ctx(input.clone(), &ctx).await.is_err());
        assert!(node
            .process_multi_with_ctx(HashMap::from([("input".into(), input.clone())]), &ctx)
            .await
            .is_err());
        assert!(node
            .process_streaming_with_ctx(input, &ctx, |_| panic!("strict callback must not run"))
            .await
            .is_err());
    }
    assert!(copies
        .report()
        .entries
        .iter()
        .all(|(_, c)| c.completed == 0 && c.in_flight == 0));
    assert!(node
        .call_authority(&node.node_id, &node.session_id, None)
        .is_err());
    let allow = CopyContext::new(CopyPolicy::AllowCopies, "native-allow");
    assert!(node
        .call_authority("other", &node.session_id, Some(allow.clone()))
        .is_err());
    assert!(node
        .call_authority(&node.node_id, "other", Some(allow))
        .is_err());
}

#[tokio::test]
async fn native_compat_real_worker_attributes_each_call_and_drops_wire_snapshot() {
    let node = fixture();
    let input = RuntimeData::Binary((0..64).collect::<Vec<u8>>().into());
    let a = CopyContext::new(CopyPolicy::AllowCopies, "session-a");
    let b = CopyContext::new(CopyPolicy::AllowCopies, "session-b");
    let ctx_a = context(&node, a.clone());
    let ctx_b = context(&node, b.clone());
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        node.process_with_ctx(input.clone(), &ctx_a),
    )
    .await
    .unwrap()
    .unwrap();
    let multi = tokio::time::timeout(
        Duration::from_secs(5),
        node.process_multi_with_ctx(HashMap::from([("input".into(), input.clone())]), &ctx_b),
    )
    .await
    .unwrap()
    .unwrap();
    let mut streamed = None;
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(5),
            node.process_streaming_with_ctx(input, &ctx_b, |data| {
                streamed = Some(data);
                Ok(())
            })
        )
        .await
        .unwrap()
        .unwrap(),
        1
    );
    for (copies, calls) in [(&a, 1), (&b, 2)] {
        assert_eq!(
            counts(copies, CopyReason::PortableSerialization).copied_bytes,
            128 * calls
        );
        assert_eq!(
            counts(copies, CopyReason::PortableDeserialization).copied_bytes,
            128 * calls
        );
        assert_eq!(
            counts(copies, CopyReason::PrivateIpcIngress).copied_bytes,
            0
        );
        assert_eq!(
            counts(copies, CopyReason::VecMaterialization).copied_bytes,
            0
        );
        assert!(copies
            .report()
            .entries
            .iter()
            .all(|(_, c)| c.failed == 0 && c.in_flight == 0));
    }
    // Framework work must no longer escape into the worker legacy ledger.
    let report_context = context(
        &node,
        CopyContext::new(CopyPolicy::AllowCopies, "report-query"),
    );
    let RuntimeData::Json(report) = tokio::time::timeout(
        Duration::from_secs(5),
        node.process_with_ctx(RuntimeData::Text("copy-report".into()), &report_context),
    )
    .await
    .unwrap()
    .unwrap() else {
        panic!("expected worker report")
    };
    for (reason, expected) in [
        ("portable_serialization", 0),
        ("portable_deserialization", 0),
        ("private_ipc_ingress", 0),
    ] {
        let entry = report["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row[0] == reason)
            .unwrap();
        assert_eq!(entry[1]["copied_bytes"].as_u64(), Some(expected));
        assert_eq!(entry[1]["in_flight"].as_u64(), Some(0));
    }
    let child = node.child.clone();
    let stopping = node.stopping.clone();
    drop(node);
    assert!(stopping.load(Ordering::Acquire));
    assert!(child.lock().unwrap().try_wait().unwrap().is_some());
    for result in [Some(output), Some(multi), streamed] {
        let RuntimeData::Binary(bytes) = result.unwrap() else {
            panic!("expected binary echo")
        };
        assert_eq!(bytes.as_slice(), &(0..64).collect::<Vec<u8>>());
    }
}

#[test]
fn native_compat_stopped_round_does_not_access_input_or_transport() {
    struct Never;
    impl PublishApi for Never {
        fn publish_call(&self, _: u64, _: &RuntimeData, _: &CopyContext) -> Result<(), String> {
            panic!("publish after stop")
        }
    }
    impl ReceiveApi for Never {
        fn receive_call(
            &self,
            _: u64,
            _: &CopyContext,
        ) -> Result<Option<Result<RuntimeData, String>>, String> {
            panic!("receive after stop")
        }
    }
    assert!(round_trip(
        &Never,
        &Never,
        &RuntimeData::Binary(vec![1; 8].into()),
        CopyContext::legacy(),
        &AtomicBool::new(true),
        &Host::start("stopped", "test").unwrap()
    )
    .is_err());
}

#[tokio::test]
async fn native_compat_drop_stops_pending_round_and_reaps_worker() {
    let node = fixture();
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "cancel-round");
    let ctx = context(&node, copies.clone());
    assert!(tokio::time::timeout(
        Duration::from_millis(100),
        node.process_with_ctx(RuntimeData::Text("stall".into()), &ctx)
    )
    .await
    .is_err());
    node.reporter.check().unwrap(); // The stalled call is still in progress.
    let commands = node.cmd_tx.clone();
    let child = node.child.clone();
    drop(node);
    tokio::time::timeout(Duration::from_secs(3), commands.closed())
        .await
        .unwrap();
    assert!(child.lock().unwrap().try_wait().unwrap().is_some());
}

#[tokio::test]
async fn native_compat_worker_failure_closes_lane_and_reaps_child() {
    for message in ["handler-error", "exit"] {
        let node = fixture();
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "failed-call");
        let ctx = context(&node, copies.clone());
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            node.process_with_ctx(RuntimeData::Text(message.into()), &ctx),
        )
        .await
        .unwrap()
        .unwrap_err()
        .to_string();
        if message == "handler-error" {
            assert!(error.contains("fixture handler rejected input"), "{error}");
        }
        assert!(node.child.lock().unwrap().try_wait().unwrap().is_some());
        assert!(node
            .process_with_ctx(RuntimeData::Text("after-error".into()), &ctx)
            .await
            .is_err());
        assert!(copies
            .report()
            .entries
            .iter()
            .all(|(_, c)| c.in_flight == 0));
        // A handler error returns its terminal frame (no worker copies for
        // metadata-only text). A worker exit has none, so the call is recorded
        // as interrupted rather than as zero copies.
        let worker_side = counts(&copies, CopyReason::PortableDeserialization);
        assert_eq!(worker_side.completed, 0);
        assert_eq!(worker_side.failed, u64::from(message == "exit"));
    }
}

#[tokio::test]
async fn native_compat_abandoned_call_drains_before_next_authority() {
    let node = fixture();
    let abandoned = CopyContext::new(CopyPolicy::AllowCopies, "abandoned");
    let next = CopyContext::new(CopyPolicy::AllowCopies, "next");
    let a = context(&node, abandoned.clone());
    let b = context(&node, next.clone());
    assert!(tokio::time::timeout(
        Duration::from_millis(50),
        node.process_with_ctx(RuntimeData::Text("delay".into()), &a)
    )
    .await
    .is_err());
    let output = tokio::time::timeout(
        Duration::from_secs(5),
        node.process_with_ctx(RuntimeData::Binary(vec![9; 32].into()), &b),
    )
    .await
    .unwrap()
    .unwrap();
    let RuntimeData::Binary(bytes) = output else {
        panic!("expected binary");
    };
    assert_eq!(bytes.as_slice(), &[9; 32]);
    assert_eq!(
        counts(&abandoned, CopyReason::PortableSerialization).copied_bytes,
        16
    );
    assert_eq!(
        counts(&abandoned, CopyReason::PortableDeserialization).copied_bytes,
        16
    );
    assert_eq!(
        counts(&next, CopyReason::PortableSerialization).copied_bytes,
        64
    );
    assert_eq!(
        counts(&next, CopyReason::PortableDeserialization).copied_bytes,
        64
    );
}

#[test]
fn native_compat_exited_worker_cannot_silently_skip_negotiation() {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.arg("--list"); // Exits without reading the private bootstrap.
    let start = std::time::Instant::now();
    let result = MultiprocessNativeNode::spawn_command(command, "Old", "old", "missing_reporting");
    assert!(result.is_err());
    assert!(start.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn native_compat_all_bulk_variants_report_both_sides_before_return() {
    use remotemedia_types::{ImageFormat, PixelFormat};
    let node = fixture();
    let inputs = [
        (
            RuntimeData::Audio {
                samples: vec![0.1, 0.2, 0.3].into(),
                sample_rate: 16000,
                channels: 1,
                stream_id: None,
                timestamp_us: None,
                arrival_ts_us: None,
                metadata: None,
            },
            12,
        ),
        (RuntimeData::Binary(vec![1; 7].into()), 7),
        (
            RuntimeData::Video {
                pixel_data: vec![2; 12].into(),
                width: 2,
                height: 2,
                format: PixelFormat::Rgb24,
                codec: None,
                frame_number: 1,
                timestamp_us: 8,
                is_keyframe: true,
                stream_id: None,
                arrival_ts_us: None,
            },
            12,
        ),
        (
            RuntimeData::Image {
                data: vec![3; 16].into(),
                format: ImageFormat::Raw {
                    pixel_format: PixelFormat::Rgba32,
                },
                width: 2,
                height: 2,
                timestamp_us: None,
                stream_id: None,
                metadata: None,
            },
            16,
        ),
        (
            RuntimeData::Tensor {
                data: vec![4; 8].into(),
                shape: vec![8],
                dtype: 0,
                metadata: None,
            },
            8,
        ),
        (
            RuntimeData::Numpy {
                data: vec![0; 32].into(),
                shape: vec![4, 2],
                dtype: "float32".into(),
                strides: vec![8, 4],
                c_contiguous: true,
                f_contiguous: false,
            },
            32,
        ),
    ];
    for (input, payload_bytes) in inputs {
        let wire = super::super::data_transfer::to_bytes(&input).unwrap();
        let copies = CopyContext::new(CopyPolicy::AllowCopies, input.data_type());
        let ctx = context(&node, copies.clone());
        let output =
            tokio::time::timeout(Duration::from_secs(5), node.process_with_ctx(input, &ctx))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            super::super::data_transfer::to_bytes(&output).unwrap(),
            wire
        );
        assert_eq!(
            counts(&copies, CopyReason::PortableSerialization).copied_bytes,
            2 * payload_bytes
        );
        assert_eq!(
            counts(&copies, CopyReason::PortableDeserialization).copied_bytes,
            2 * payload_bytes
        );
        assert_eq!(
            counts(&copies, CopyReason::PrivateIpcIngress).copied_bytes,
            0
        );
        assert!(copies
            .report()
            .entries
            .iter()
            .all(|(_, c)| c.failed == 0 && c.in_flight == 0));
    }
}
