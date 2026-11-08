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
    let wire_len = super::super::data_transfer::to_bytes(&input).unwrap().len() as u64;
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
            64 * calls
        );
        assert_eq!(
            counts(copies, CopyReason::PortableDeserialization).copied_bytes,
            64 * calls
        );
        assert_eq!(
            counts(copies, CopyReason::PrivateIpcIngress).copied_bytes,
            wire_len * calls
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
    // Query the fixture's process-local ledger. This is test evidence, not the
    // production host-report negotiation still required for session attribution.
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
        ("portable_serialization", 192),
        ("portable_deserialization", 192),
        ("private_ipc_ingress", wire_len * 3),
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
        fn publish_data(&self, _: &RuntimeData, _: &CopyContext) -> Result<(), String> {
            panic!("publish after stop")
        }
    }
    impl ReceiveApi for Never {
        fn receive_data(&self, _: &CopyContext) -> Result<Option<RuntimeData>, String> {
            panic!("receive after stop")
        }
    }
    assert!(round_trip(
        &Never,
        &Never,
        &RuntimeData::Binary(vec![1; 8].into()),
        CopyContext::legacy(),
        &AtomicBool::new(true)
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
    assert!(counts(&copies, CopyReason::PrivateIpcIngress).copied_bytes > 0);
    let commands = node.cmd_tx.clone();
    let child = node.child.clone();
    drop(node);
    tokio::time::timeout(Duration::from_secs(3), commands.closed())
        .await
        .unwrap();
    assert!(child.lock().unwrap().try_wait().unwrap().is_some());
}
