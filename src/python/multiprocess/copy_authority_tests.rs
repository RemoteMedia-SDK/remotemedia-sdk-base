use super::*;
use remotemedia_types::CopyPolicy;
use serde_json::{json, Value};

fn connect(server: &CopyAuthority) -> TcpStream {
    let mut stream = TcpStream::connect(&server.bootstrap.address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let mut hello = serde_json::to_value(&server.bootstrap).unwrap();
    hello.as_object_mut().unwrap().remove("address");
    write_frame(&mut stream, &hello).unwrap();
    assert_eq!(
        read_frame::<Value>(&mut stream).unwrap(),
        json!({"seq":0,"ok":true})
    );
    stream
}

fn request(stream: &mut TcpStream, seq: u64, action: Value) -> Value {
    write_frame(stream, &json!({"seq":seq,"action":action})).unwrap();
    read_frame(stream).unwrap()
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

#[test]
fn legacy_copy_protocol_retains_host_authority_and_failed_disconnect() {
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "original");
    let server = CopyAuthority::start_with_version(copies.clone(), "node", "session", 1).unwrap();
    let mut client = connect(&server);
    assert_eq!(
        request(
            &mut client,
            1,
            json!({"op":"begin","reason":"borrowed_input","bytes":32})
        )["ok"],
        true
    );
    assert_eq!(counts(&copies, CopyReason::BorrowedInput).in_flight, 1);
    request(&mut client, 2, json!({"op":"complete","bytes":16}));
    assert_eq!(counts(&copies, CopyReason::BorrowedInput).copied_bytes, 16);
    request(
        &mut client,
        3,
        json!({"op":"begin","reason":"private_ipc_ingress","bytes":77}),
    );
    drop(client);
    assert!(server.finish().is_err());
    let failed = counts(&copies, CopyReason::PrivateIpcIngress);
    assert_eq!(
        (
            failed.failed,
            failed.failed_requested_bytes,
            failed.in_flight
        ),
        (1, 77, 0)
    );
}

#[test]
fn legacy_copy_protocol_rejects_strict_before_authorization_and_closes_cleanly() {
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "strict");
    let server = CopyAuthority::start_with_version(copies.clone(), "node", "session", 1).unwrap();
    let mut client = connect(&server);
    assert_eq!(
        request(
            &mut client,
            1,
            json!({"op":"begin","reason":"borrowed_input","bytes":9})
        )["ok"],
        false
    );
    request(&mut client, 2, json!({"op":"close"}));
    server.finish().unwrap();
    let result = counts(&copies, CopyReason::BorrowedInput);
    assert_eq!(
        (result.rejected_bytes, result.in_flight, result.copied_bytes),
        (9, 0, 0)
    );
}

#[test]
fn legacy_copy_protocol_faults_preserve_unfinished_attempts() {
    for (seq, action) in [
        (1, json!({"op":"complete","bytes":4})), // duplicate sequence
        (2, json!({"op":"complete","bytes":5})), // overrun
        (2, json!({"op":"close"})),
        (2, json!({"op":"begin","reason":"borrowed_input","bytes":1})),
        (2, json!({"op":"fail","unexpected":true})),
    ] {
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "fault");
        let server = CopyAuthority::start_with_version(copies.clone(), "node", "session", 1).unwrap();
        let mut client = connect(&server);
        request(
            &mut client,
            1,
            json!({"op":"begin","reason":"borrowed_input","bytes":4}),
        );
        write_frame(&mut client, &json!({"seq":seq,"action":action})).unwrap();
        let reply = read_frame::<Value>(&mut client);
        assert!(
            reply.is_err(),
            "accepted invalid action {action}: {reply:?}"
        );
        assert!(server.finish().is_err());
        let result = counts(&copies, CopyReason::BorrowedInput);
        assert_eq!(
            (
                result.failed,
                result.failed_requested_bytes,
                result.copied_bytes
            ),
            (1, 4, 0)
        );
    }
}

#[test]
fn legacy_copy_protocol_authentication_and_frame_bounds_precede_ledger_access() {
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "authenticated");
    let server = CopyAuthority::start_with_version(copies.clone(), "node", "session", 1).unwrap();
    let mut imposter = TcpStream::connect(&server.bootstrap.address).unwrap();
    imposter
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    write_frame(
        &mut imposter,
        &json!({"version":1,"token":"wrong","node":"node","session":"session"}),
    )
    .unwrap();
    assert!(read_frame::<Value>(&mut imposter).is_err());
    let mut client = connect(&server);
    client
        .write_all(&((MAX_FRAME + 1) as u32).to_be_bytes())
        .unwrap();
    assert!(read_frame::<Value>(&mut client).is_err());
    assert!(server.finish().is_err());
    assert!(copies
        .report()
        .entries
        .iter()
        .all(|(_, c)| c.completed == 0 && c.failed == 0 && c.in_flight == 0));
}

#[test]
fn legacy_copy_python_hooks_report_to_real_host_context() {
    use std::process::{Command, Stdio};
    let python = std::env::var("LEGACY_WIRE_PYTHON")
        .expect("set LEGACY_WIRE_PYTHON for real Python reporting tests");
    let source =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../remotemedia-sdk/clients/python");
    for mode in ["success", "abandon", "strict"] {
        let policy = if mode == "strict" {
            CopyPolicy::RequireZeroCopy
        } else {
            CopyPolicy::AllowCopies
        };
        let copies = CopyContext::new(policy, "host/session/node");
        let server = CopyAuthority::start(copies.clone(), "node", "session").unwrap();
        let mut child = Command::new(&python)
            .args(["-c", include_str!("copy_authority_peer.py")])
            .arg(&source)
            .arg(mode)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child
            .stdin
            .take()
            .unwrap()
            .write_all(&serde_json::to_vec(server.bootstrap()).unwrap())
            .unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{mode}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let result = server.finish();
        let ingress = counts(&copies, CopyReason::PrivateIpcIngress);
        match mode {
            "success" => {
                result.unwrap();
                assert_eq!(ingress.copied_bytes, 4);
                assert_eq!(
                    counts(&copies, CopyReason::PortableSerialization).copied_bytes,
                    24
                );
                assert_eq!(
                    counts(&copies, CopyReason::BorrowedInput).failed_requested_bytes,
                    5
                );
            }
            "abandon" => {
                // Version 2: the abandoned copy was never reported, so the host
                // records one failed operation of unknown reason and size.
                assert!(result.is_err());
                assert_eq!(ingress.in_flight, 0);
                let unreported = counts(&copies, CopyReason::BorrowedInput);
                assert_eq!((unreported.failed, unreported.in_flight), (1, 0));
            }
            "strict" => {
                assert!(result.is_err());
                assert_eq!((ingress.rejected_bytes, ingress.copied_bytes), (4, 0));
            }
            _ => unreachable!(),
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn legacy_copy_native_launcher_accounts_startup_input_output_and_eof() {
    run_native_launcher(false).await;
}

#[tokio::test(flavor = "current_thread")]
async fn legacy_copy_native_launcher_terminates_idle_worker_after_reporting_disconnect() {
    run_native_launcher(true).await;
}

async fn run_native_launcher(abort_reporting: bool) {
    use crate::multiprocess::ipc_channel::ChannelRegistry;
    use crate::python::multiprocess::{
        process_manager::{ProcessManager, ProcessStatus},
        MultiprocessConfig,
    };
    use futures::FutureExt;
    use std::panic::AssertUnwindSafe;
    let source =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../remotemedia-sdk/clients/python");
    let config = MultiprocessConfig {
        python_executable: std::env::var("LEGACY_WIRE_PYTHON")
            .expect("set LEGACY_WIRE_PYTHON")
            .into(),
        python_path: vec![source.clone(), source.join("tests/support")],
        ..Default::default()
    };
    let manager = ProcessManager::new(config);
    manager.register_module("legacy_copy_node".into()).await;
    let session = format!("copy_{}", uuid::Uuid::new_v4().simple());
    let node = "echo";
    let registry = ChannelRegistry::global();
    let mut channels = Vec::new();
    for name in [
        format!("{session}_{node}_input"),
        format!("{session}_{node}_output"),
        format!("control/{session}_{node}"),
    ] {
        channels.push(registry.create_channel(&name, 128, false).await.unwrap());
    }
    let input = registry.create_publisher(&channels[0].name).await.unwrap();
    let output = registry.create_subscriber(&channels[1].name).await.unwrap();
    let control = registry.create_subscriber(&channels[2].name).await.unwrap();
    let copies = CopyContext::new(CopyPolicy::AllowCopies, format!("{session}/{node}"));
    let process = manager
        .spawn_node_with_copy_context(
            "LegacyCopyEcho",
            node,
            &json!({"configured":17,"abort_reporting":abort_reporting}),
            &session,
            Some(copies.clone()),
        )
        .await
        .unwrap();
    let result = AssertUnwindSafe(async {
        let deadline = Instant::now() + Duration::from_secs(45);
        if abort_reporting {
            loop {
                if matches!(*process.status.read().await, ProcessStatus::Error(_)) {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "idle worker survived reporting disconnect"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            // Version 2: the copy begun before the disconnect was never
            // reported; the host records one failed operation of unknown size.
            let failed = counts(&copies, CopyReason::BorrowedInput);
            assert_eq!(
                (
                    failed.failed,
                    failed.failed_requested_bytes,
                    failed.in_flight
                ),
                (1, 0, 0)
            );
            return Vec::new();
        }
        loop {
            if control.receive_bytes().unwrap().as_deref() == Some(b"READY") {
                break;
            }
            assert!(
                !matches!(*process.status.read().await, ProcessStatus::Error(_)),
                "worker failed before READY: {:?}",
                process.status.read().await
            );
            assert!(
                Instant::now() < deadline,
                "worker did not become ready: {:?}",
                process.status.read().await
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let data = remotemedia_types::RuntimeData::Tensor {
            data: vec![7; 16].into(),
            shape: vec![16],
            dtype: 5,
            metadata: None,
        };
        let wire = crate::multiprocess::data_transfer::to_python_wire(&data, &session).unwrap();
        input.send(&wire).unwrap();
        let mut outputs = Vec::new();
        loop {
            while let Some(frame) = output.receive_bytes().unwrap() {
                // The host's IPC thread does this in production.
                if frame[0] == END_OF_INPUT {
                    replay_end_of_input(&copies, &frame).unwrap();
                }
                outputs.push(frame);
            }
            if *process.status.read().await == ProcessStatus::Stopped {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "worker did not close: {:?}",
                process.status.read().await
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        while let Some(frame) = output.receive_bytes().unwrap() {
            if frame[0] == END_OF_INPUT {
                replay_end_of_input(&copies, &frame).unwrap();
            }
            outputs.push(frame);
        }
        assert_eq!(
            outputs.iter().map(|frame| frame[0]).collect::<Vec<_>>(),
            vec![4, 4, 8]
        );
        assert_eq!(
            counts(&copies, CopyReason::PrivateIpcIngress).copied_bytes,
            wire.len() as u64
        );
        let sent = counts(&copies, CopyReason::BorrowedInput);
        assert_eq!(
            (sent.completed, sent.copied_bytes),
            (3, outputs.iter().map(|frame| frame.len() as u64).sum())
        );
        assert_eq!(
            counts(&copies, CopyReason::PortableSerialization).copied_bytes,
            0
        );
        assert!(copies
            .report()
            .entries
            .iter()
            .all(|(_, entry)| entry.in_flight == 0 && entry.failed == 0));
        outputs
    })
    .catch_unwind()
    .await;
    manager
        .terminate_process(process.clone(), Duration::from_millis(100))
        .await
        .unwrap();
    assert!(!process.is_alive().await);
    drop((input, output, control));
    for channel in channels {
        registry.destroy_channel(channel).await.unwrap();
    }
    match result {
        Ok(outputs) => {
            if !abort_reporting {
                assert!(outputs[1].ends_with(&[7; 16]));
            }
        }
        Err(error) => std::panic::resume_unwind(error),
    }
}

fn end_of_input(report: &[u8]) -> Vec<u8> {
    let mut frame = vec![END_OF_INPUT];
    frame.extend(7u16.to_le_bytes());
    frame.extend(b"session");
    frame.extend(42u64.to_le_bytes());
    frame.extend((report.len() as u32).to_le_bytes());
    frame.extend(report);
    frame
}

#[test]
fn copy_report_v2_close_replays_totals() {
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "v2");
    let server = CopyAuthority::start(copies.clone(), "node", "session").unwrap();
    assert_eq!(server.bootstrap().version, 2);
    let mut client = connect(&server);
    let reply = request(
        &mut client,
        1,
        json!({"op":"close","totals":{"v":2,"reasons":[
            ["borrowed_input",[3,30,1,5]],["private_ipc_ingress",[1,7,0,0]]]}}),
    );
    assert_eq!(reply, json!({"seq":1,"ok":true}));
    server.finish().unwrap();
    let sent = counts(&copies, CopyReason::BorrowedInput);
    assert_eq!(
        (sent.completed, sent.copied_bytes, sent.failed, sent.failed_requested_bytes, sent.in_flight),
        (3, 30, 1, 5, 0)
    );
    assert_eq!(counts(&copies, CopyReason::PrivateIpcIngress).copied_bytes, 7);
}

#[test]
fn copy_report_v2_accepts_only_close_and_records_unreported_loss() {
    for action in [
        json!({"op":"begin","reason":"borrowed_input","bytes":4}),
        json!({"op":"close","totals":{"v":2,"reasons":[["vec_materialization",[1,1,0,0]]]}}),
    ] {
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "v2");
        let server = CopyAuthority::start(copies.clone(), "node", "session").unwrap();
        let mut client = connect(&server);
        write_frame(&mut client, &json!({"seq":1,"action":action})).unwrap();
        assert!(read_frame::<Value>(&mut client).is_err(), "accepted {action}");
        assert!(server.finish().is_err());
        let lost = counts(&copies, CopyReason::BorrowedInput);
        assert_eq!((lost.failed, lost.copied_bytes, lost.in_flight), (1, 0, 0));
    }
    // A worker that disconnects without closing loses its unreported copies.
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "v2");
    let server = CopyAuthority::start(copies.clone(), "node", "session").unwrap();
    drop(connect(&server));
    assert!(server.finish().is_err());
    assert_eq!(counts(&copies, CopyReason::BorrowedInput).failed, 1);
}

#[test]
fn copy_report_v2_end_of_input_replays_or_rejects() {
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "v2");
    replay_end_of_input(&copies, &end_of_input(b"")).unwrap();
    assert!(copies.report().entries.iter().all(|(_, c)| c.completed == 0 && c.failed == 0));
    replay_end_of_input(
        &copies,
        &end_of_input(br#"{"v":2,"reasons":[["portable_serialization",[2,24,0,0]]]}"#),
    )
    .unwrap();
    let serialized = counts(&copies, CopyReason::PortableSerialization);
    assert_eq!((serialized.completed, serialized.copied_bytes, serialized.in_flight), (2, 24, 0));

    for malformed in [
        end_of_input(br#"{"v":3,"reasons":[]}"#),
        end_of_input(br#"{"v":2,"reasons":[["borrowed_input",[0,5,0,0]]]}"#),
        end_of_input(br#"{"v":2,"reasons":[["borrowed_input",[1,1,0,0]],["borrowed_input",[1,1,0,0]]]}"#),
        end_of_input(br#"{"v":2,"reasons":[["borrowed_input",[2000000,1,0,0]]]}"#),
        end_of_input(b"not json"),
        end_of_input(b"{}")[..20].to_vec(),
    ] {
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "v2");
        assert!(replay_end_of_input(&copies, &malformed).is_err());
        let lost = counts(&copies, CopyReason::BorrowedInput);
        assert_eq!((lost.failed, lost.completed, lost.in_flight), (1, 0, 0));
    }
}
