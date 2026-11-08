use super::*;
use remotemedia_types::{BufferBudget, CopyCounts, CopyPolicy, CopyReason};
use std::sync::atomic::Ordering;

fn address() -> PbAddress {
    PbAddress {
        node_id: "producer".into(),
        port: "audio".into(),
        direction: ControlDirection::Out as i32,
    }
}

fn counts(copies: &CopyContext) -> CopyCounts {
    copies
        .report()
        .entries
        .into_iter()
        .find(|(reason, _)| *reason == CopyReason::PortableSerialization)
        .unwrap()
        .1
}

fn audio(samples: Vec<f32>) -> RuntimeData {
    RuntimeData::Audio {
        samples: samples.into(),
        sample_rate: 48000,
        channels: 2,
        stream_id: Some("voice".into()),
        timestamp_us: Some(123),
        arrival_ts_us: None,
        metadata: None,
    }
}

#[test]
fn nested_control_wire_matches_generated_encoding_without_audio_staging() {
    for len in [0, 1, 31, 32, 127, 128, 4095, 4096] {
        for data in [
            RuntimeData::Binary(vec![42; len].into()),
            audio(
                (0..len)
                    .map(|i| f32::from_bits([0, 0x80000000, 0x7fc01234, 1][i % 4]))
                    .collect(),
            ),
        ] {
            for intercept in [false, true] {
                assert_nested_encoding(&data, intercept);
            }
        }
    }
}

fn assert_nested_encoding(data: &RuntimeData, intercept: bool) {
    let expected = crate::adapters::runtime_data_to_data_buffer(data);
    let byte_len = data.bulk_payload_bytes();
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "control-wire");
    let reference = ControlEvent {
        event: Some(if intercept {
            PbEvent::InterceptRequest(PbInterceptRequest {
                addr: Some(address()),
                correlation_id: u64::MAX,
                data: Some(expected.clone()),
            })
        } else {
            PbEvent::Tap(TapEvent {
                addr: Some(address()),
                data: Some(expected.clone()),
            })
        }),
    };
    let queued = if intercept {
        Outbound::intercept(address(), u64::MAX, data.clone(), &copies)
    } else {
        Outbound::tap(address(), data.clone(), &copies)
    };
    assert_eq!(queued.encode().unwrap().as_ref(), reference.encode_to_vec());
    assert_eq!(counts(&copies).copied_bytes, byte_len as u64);
    assert_eq!(counts(&copies).completed, u64::from(byte_len != 0));
}

#[test]
fn queued_payload_retains_owner_budget_and_authority_until_drop() {
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        let copies = CopyContext::new(policy, "queued-control");
        let budget = BufferBudget::new(1, 64);
        let data = budget
            .admit(RuntimeData::Binary(vec![7; 64].into()))
            .unwrap();
        let RuntimeData::Binary(ref owner) = data else {
            unreachable!()
        };
        let pointer = owner.as_ptr();
        let queued = Outbound::tap(address(), data, &copies);
        let Outbound::Payload {
            data: RuntimeData::Binary(ref retained),
            copies: ref authority,
            ..
        } = queued
        else {
            unreachable!()
        };
        assert_eq!(retained.as_ptr(), pointer);
        assert!(authority.same_authority(&copies));
        assert_eq!(budget.usage().owners, 1);
        assert_eq!(counts(&copies), CopyCounts::default());
        let result = queued.encode();
        assert_eq!(result.is_ok(), policy == CopyPolicy::AllowCopies);
        let c = counts(&copies);
        assert_eq!(
            (c.copied_bytes, c.rejected_bytes),
            if result.is_ok() { (64, 0) } else { (0, 64) }
        );
        drop(queued);
        assert_eq!(budget.usage().owners, 0);
    }
}

#[test]
fn strict_control_rejects_before_reading_queued_external_owners() {
    let (binary, audio, deny) = crate::test_support::guarded_payloads();
    deny.store(true, Ordering::SeqCst);
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "strict-control");
    for data in [binary, audio] {
        assert!(Outbound::tap(address(), data.clone(), &copies)
            .encode()
            .is_err());
        assert!(Outbound::intercept(address(), 1, data, &copies)
            .encode()
            .is_err());
    }
    assert_eq!(counts(&copies).rejected_bytes, 64);
    assert_eq!(counts(&copies).copied_bytes, 0);
}

#[test]
fn control_headers_cannot_smuggle_unattributed_payloads() {
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "headers");
    let header = TapEvent {
        addr: Some(address()),
        data: Some(crate::adapters::runtime_data_to_data_buffer(
            &RuntimeData::Binary(vec![1; 16].into()),
        )),
    };
    assert!(Outbound::from(ControlEvent {
        event: Some(PbEvent::Tap(header.clone()))
    })
    .encode()
    .is_err());
    let queued = Outbound::Payload {
        header: PayloadHeader::Tap(header),
        data: RuntimeData::Text("metadata".into()),
        copies: copies.clone(),
    };
    assert!(queued.encode().is_err());
    assert_eq!(counts(&copies), CopyCounts::default());
    let metadata = ControlEvent {
        event: Some(PbEvent::Attached(Attached::default())),
    };
    assert_eq!(
        Outbound::from(metadata.clone()).encode().unwrap().as_ref(),
        metadata.encode_to_vec()
    );
}

#[tokio::test]
async fn tap_forwarder_preserves_reply_lag_errors_and_releases_closed_queue_owners() {
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "forwarder");
    let (source, rx) = tokio::sync::broadcast::channel(1);
    let (tx, mut events) = mpsc::channel(1);
    for _ in 0..2 {
        source.send(RuntimeData::Text("reply".into())).unwrap();
    }
    let mut addr = CoreAddress::node_out("calc");
    addr.port = Some("__reply__".into());
    forward_taps(rx, tx, addr.clone(), copies.clone()).await;
    let decoded = ControlEvent::decode(events.recv().await.unwrap().encode().unwrap()).unwrap();
    let Some(PbEvent::Error(error)) = decoded.event else {
        panic!("lost lag error")
    };
    assert!(error
        .message
        .contains("RPC reply subscription lagged by 1 frames"));
    assert_eq!(error.addr, Some(core_addr_to_pb(&addr)));

    let budget = BufferBudget::new(1, 16);
    let (source, rx) = tokio::sync::broadcast::channel(1);
    let (tx, events) = mpsc::channel(1);
    drop(events);
    source
        .send(
            budget
                .admit(RuntimeData::Binary(vec![1; 16].into()))
                .unwrap(),
        )
        .unwrap();
    forward_taps(rx, tx, addr, copies.clone()).await;
    assert_eq!(budget.usage().owners, 0);
    assert_eq!(counts(&copies), CopyCounts::default());
}
