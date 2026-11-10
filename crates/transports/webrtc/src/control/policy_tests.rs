use super::*;
use crate::generated::{Hello, Intercept, Publish, Subscribe};
use crate::test_support::core_session;
use futures::FutureExt;
use remotemedia_types::{CopyContext, CopyPolicy, CopyReason};
use tokio::time::timeout;

#[tokio::test]
async fn live_control_channel_delivers_counted_taps_or_explicit_policy_errors() {
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        live_control_case(policy).await;
    }
}

async fn live_control_case(policy: CopyPolicy) {
    let (executor, mut session, control) = core_session(policy).await;
    let copies = session.transport_copy_context();
    let offer = crate::test_support::peer().await;
    let answer = crate::test_support::peer().await;
    let sender = offer
        .create_data_channel(CONTROL_CHANNEL_LABEL, None)
        .await
        .unwrap();
    let bus = executor.control_bus();
    answer.on_data_channel(Box::new(move |dc| {
        let bus = bus.clone();
        Box::pin(async move {
            attach_control_channel(dc, bus).await;
        })
    }));
    let (tx, mut rx) = mpsc::unbounded_channel();
    sender.on_message(Box::new(move |message| {
        let tx = tx.clone();
        Box::pin(async move {
            let _ = tx.send(ControlEvent::decode(message.data).unwrap());
        })
    }));
    let work = async {
        crate::test_support::negotiate(&offer, &answer).await;
        while sender.ready_state()
            != webrtc::data_channel::data_channel_state::RTCDataChannelState::Open
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        for op in [
            PbOp::Hello(Hello {
                session_id: session.session_id.clone(),
                attach_id: "copy-test".into(),
            }),
            PbOp::Subscribe(Subscribe {
                addr: Some(address()),
            }),
        ] {
            sender
                .send(&ControlFrame { op: Some(op) }.encode_to_vec().into())
                .await
                .unwrap();
        }
        assert!(matches!(
            rx.recv().await.unwrap().event,
            Some(PbEvent::Attached(_))
        ));
        while control.tap_receiver_count("calc", None) == 0 {
            tokio::task::yield_now().await;
        }
        control.publish_tap("calc", None, RuntimeData::Binary(vec![42; 4096].into()));
        let event = rx.recv().await.unwrap();
        match (policy, event.event) {
            (CopyPolicy::AllowCopies, Some(PbEvent::Tap(tap))) => {
                let RuntimeData::Binary(data) =
                    pb_to_runtime_data(tap.data.unwrap(), &copies).unwrap()
                else {
                    panic!("binary tap")
                };
                assert_eq!(data.as_ref(), &[42; 4096]);
            }
            (CopyPolicy::RequireZeroCopy, Some(PbEvent::Error(error))) => {
                assert_eq!(error.code, ControlErrorCode::Internal as i32);
                assert!(error.message.contains(&session.session_id));
            }
            other => panic!("unexpected control response: {other:?}"),
        }
        let report = copies.report();
        let c = &report
            .entries
            .iter()
            .find(|(r, _)| *r == CopyReason::PortableSerialization)
            .unwrap()
            .1;
        assert_eq!(
            (c.copied_bytes, c.rejected_bytes),
            if policy == CopyPolicy::AllowCopies {
                (4096, 0)
            } else {
                (0, 4096)
            }
        );
    };
    let result = std::panic::AssertUnwindSafe(timeout(Duration::from_secs(25), work))
        .catch_unwind()
        .await;
    sender.close().await.unwrap();
    offer.close().await.unwrap();
    answer.close().await.unwrap();
    session.close().await.unwrap();
    timeout(Duration::from_secs(5), session.wait())
        .await
        .unwrap()
        .unwrap();
    result.unwrap().expect("local control channel timed out");
}

fn address() -> PbAddress {
    core_addr_to_pb(&CoreAddress::node_out("calc"))
}

#[tokio::test]
async fn control_forwarders_keep_session_authority_and_owners_after_shutdown() {
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        let (executor, mut session, control) = core_session(policy).await;
        let copies = session.transport_copy_context();
        let (tx, mut rx) = mpsc::channel(8);
        let tasks = Arc::new(Mutex::new(Vec::new()));
        let scope = RpcCallScope::default();
        for op in [
            PbOp::Subscribe(Subscribe {
                addr: Some(address()),
            }),
            PbOp::Intercept(Intercept {
                addr: Some(address()),
                deadline_ms: 5000,
            }),
        ] {
            dispatch_frame(ControlFrame { op: Some(op) }, &control, &tx, &tasks, &scope)
                .await
                .unwrap();
        }
        let bytes = vec![23; 32];
        let pointer = bytes.as_ptr();
        let producer_control = control.clone();
        let producer = tokio::spawn(async move {
            producer_control
                .on_node_output("calc", None, RuntimeData::Binary(bytes.into()))
                .await
        });
        let mut retained = Vec::new();
        for _ in 0..2 {
            let event = timeout(Duration::from_secs(5), rx.recv())
                .await
                .unwrap()
                .unwrap();
            let Outbound::Payload {
                header,
                data,
                copies: authority,
            } = &event
            else {
                panic!("expected owning event")
            };
            assert!(authority.same_authority(&copies));
            let RuntimeData::Binary(owner) = data else {
                panic!("lost payload")
            };
            assert_eq!(owner.as_ptr(), pointer);
            if let output::PayloadHeader::Intercept(header) = header {
                control
                    .complete_intercept(header.correlation_id, CoreDecision::Pass)
                    .unwrap();
            }
            retained.push(event);
        }
        let forwarded = timeout(Duration::from_secs(5), producer)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let RuntimeData::Binary(owner) = forwarded else {
            panic!("lost original")
        };
        assert_eq!(owner.as_ptr(), pointer);
        for task in tasks.lock().await.drain(..) {
            task.abort();
            let _ = task.await;
        }
        session.close().await.unwrap();
        timeout(Duration::from_secs(5), session.wait())
            .await
            .unwrap()
            .unwrap();
        drop(control);
        drop(executor);
        for event in retained {
            assert_eq!(event.encode().is_ok(), policy == CopyPolicy::AllowCopies);
        }
        let report = copies.report();
        let c = &report
            .entries
            .iter()
            .find(|(r, _)| *r == CopyReason::PortableSerialization)
            .unwrap()
            .1;
        assert_eq!(
            (c.copied_bytes, c.rejected_bytes),
            if policy == CopyPolicy::AllowCopies {
                (64, 0)
            } else {
                (0, 64)
            }
        );
    }
}

#[test]
fn owning_control_publish_decode_retains_wire_range_and_replacement_policy() {
    let data =
        crate::adapters::runtime_data_to_data_buffer(&RuntimeData::Binary(vec![42; 128].into()));
    let wire = bytes::Bytes::from(
        ControlFrame {
            op: Some(PbOp::Publish(Publish {
                addr: Some(address()),
                data: Some(data),
            })),
        }
        .encode_to_vec(),
    );
    let start = wire.as_ptr() as usize;
    let end = start + wire.len();
    let ControlFrame {
        op: Some(PbOp::Publish(publish)),
    } = ControlFrame::decode(wire).unwrap()
    else {
        panic!("publish")
    };
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "owning-control-input");
    let RuntimeData::Binary(owner) = pb_to_runtime_data(publish.data.unwrap(), &copies).unwrap()
    else {
        panic!("binary")
    };
    assert!((start..end).contains(&(owner.as_ptr() as usize)));
    assert_eq!(owner.as_ref(), &[42; 128]);
    let audio = crate::generated::DataBuffer {
        data_type: Some(crate::generated::data_buffer::DataType::Audio(
            crate::generated::AudioBuffer {
                samples: vec![0; 16].into(),
                sample_rate: 48000,
                channels: 1,
                num_samples: 4,
                format: crate::generated::AudioFormat::F32 as i32,
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    assert!(pb_to_runtime_data(audio.clone(), &copies).is_err());
    assert!(pb_decision_to_core(
        Some(crate::generated::InterceptDecision {
            decision: Some(PbDecision::Replace(audio.clone()))
        }),
        &copies
    )
    .is_err());
    let allow = CopyContext::new(CopyPolicy::AllowCopies, "control-conversion");
    assert!(pb_to_runtime_data(audio, &allow).is_ok());
    assert_eq!(
        copies
            .report()
            .entries
            .iter()
            .map(|(_, c)| c.rejected_bytes)
            .sum::<u64>(),
        32
    );
    assert_eq!(
        allow
            .report()
            .entries
            .iter()
            .map(|(_, c)| c.copied_bytes)
            .sum::<u64>(),
        16
    );
}

#[tokio::test]
async fn rejected_intercept_serialization_preserves_configured_timeout_passthrough() {
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "intercept-timeout");
    let control = SessionControl::with_transport_copy_context("timeout", copies.clone());
    let (tx, mut rx) = mpsc::channel(1);
    let tasks = Arc::new(Mutex::new(Vec::new()));
    dispatch_frame(
        ControlFrame {
            op: Some(PbOp::Intercept(Intercept {
                addr: Some(address()),
                deadline_ms: 20,
            })),
        },
        &control,
        &tx,
        &tasks,
        &RpcCallScope::default(),
    )
    .await
    .unwrap();
    let bytes = vec![9; 16];
    let pointer = bytes.as_ptr();
    let producer_control = control.clone();
    let producer = tokio::spawn(async move {
        producer_control
            .on_node_output("calc", None, RuntimeData::Binary(bytes.into()))
            .await
    });
    let event = timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(event.encode().is_err());
    let original = timeout(Duration::from_secs(5), producer)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let RuntimeData::Binary(owner) = original else {
        panic!("timeout lost original")
    };
    assert_eq!(owner.as_ptr(), pointer);
    for task in tasks.lock().await.drain(..) {
        task.abort();
        let _ = task.await;
    }
    assert_eq!(
        copies
            .report()
            .entries
            .iter()
            .map(|(_, c)| c.rejected_bytes)
            .sum::<u64>(),
        16
    );
}
