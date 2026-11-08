use super::*;
use remotemedia_types::{CopyLedger, CopyPolicy};

fn allow() -> CopyContext {
    CopyContext::new(CopyPolicy::AllowCopies, "direct-native-wire")
}
fn serialized(copies: &CopyContext) -> remotemedia_types::CopyCounts {
    copies.counts()[CopyReason::PortableSerialization as usize]
}

#[test]
fn direct_encoding_matches_variable_width_messagepack_without_staging_transfer() {
    let inputs = [
        RuntimeData::Binary(vec![0, 127, 128, 254, 255].into()),
        RuntimeData::Binary(Vec::new().into()),
        RuntimeData::Audio {
            samples: vec![0.1, -2.5, f32::NAN, f32::INFINITY].into(),
            sample_rate: 16000,
            channels: 1,
            stream_id: Some("audio".into()),
            timestamp_us: Some(128),
            arrival_ts_us: None,
            metadata: Some(serde_json::json!({"nested": [null, 128, 65536, -1, "metadata"]})),
        },
        RuntimeData::Json(serde_json::json!({"text":"unicode \u{1f600}","nested":{"value":false}})),
    ];
    for input in inputs {
        let expected = data_transfer::to_bytes(&input).unwrap();
        let copies = allow();
        let length = encoded_len(&input, &copies, data_transfer::MAX_SLICE_LEN).unwrap();
        assert_eq!(length, expected.len());
        assert_eq!(serialized(&copies).completed, 0); // Sizing retains no wire bytes.
        let mut destination = vec![MaybeUninit::uninit(); length];
        encode_into(&mut destination, &input, &copies).unwrap();
        // SAFETY: successful encode_into initialized the whole exact destination.
        let actual =
            unsafe { std::slice::from_raw_parts(destination.as_ptr().cast::<u8>(), length) };
        assert_eq!(actual, expected);
        assert_eq!(
            serialized(&copies).copied_bytes,
            input.bulk_payload_bytes() as u64
        );
        assert_eq!(
            copies.counts()[CopyReason::PrivateIpcIngress as usize].copied_bytes,
            0
        );
    }
}

#[test]
fn strict_policy_rejects_before_sizing_or_destination_writes() {
    let data = RuntimeData::Binary(vec![255; 8].into());
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "strict-wire");
    assert!(encoded_len(&data, &copies, usize::MAX)
        .unwrap_err()
        .contains("rejects"));
    assert_eq!(serialized(&copies).rejected_bytes, 8);
    let mut destination = [MaybeUninit::new(0xa5); 128];
    assert!(encode_into(&mut destination, &data, &copies).is_err());
    // SAFETY: the sentinel initialized every byte before the rejected write.
    assert!(destination
        .iter()
        .all(|byte| unsafe { byte.assume_init() } == 0xa5));
    assert_eq!(serialized(&copies).rejected_bytes, 16);
    assert_eq!(serialized(&copies).completed, 0);
}

#[test]
fn size_limits_and_changed_lengths_never_mark_uninitialized_storage_publishable() {
    let data = RuntimeData::Binary(vec![255; 8].into());
    let copies = allow();
    let length = encoded_len(&data, &copies, usize::MAX).unwrap();
    assert!(encoded_len(&data, &copies, length - 1).is_err());
    assert_eq!(serialized(&copies).completed, 0);
    let mut short = vec![MaybeUninit::uninit(); length - 1];
    assert!(encode_into(&mut short, &data, &copies).is_err());
    assert_eq!(serialized(&copies).failed, 1);
    assert_eq!(serialized(&copies).in_flight, 0);
    let mut long = vec![MaybeUninit::uninit(); length + 1];
    assert!(encode_into(&mut long, &data, &copies)
        .unwrap_err()
        .contains("length changed"));
    // Successful serialization happened, but its unmatched loan cannot publish.
    assert_eq!(serialized(&copies).copied_bytes, 8);
    let mut counter = WireSize {
        bytes: usize::MAX,
        limit: usize::MAX,
    };
    assert!(counter.write(&[1]).is_err());
}

#[test]
fn loan_writer_rejects_overflow_without_touching_unwritten_tail() {
    let mut bytes = [MaybeUninit::new(0xa5); 3];
    let mut writer = LoanWriter {
        destination: &mut bytes,
        written: 0,
    };
    writer.write_all(&[1, 2]).unwrap();
    assert!(writer.write(&[3, 4]).is_err());
    assert!(writer.finish().is_err());
    // SAFETY: all bytes were initialized to the sentinel before writes.
    assert_eq!(
        bytes.map(|byte| unsafe { byte.assume_init() }),
        [1, 2, 0xa5]
    );
}

#[test]
fn native_receiver_observes_exact_wire_and_retains_loan_after_publisher_drop() {
    let id = uuid::Uuid::new_v4().simple();
    let input_name = format!("direct_{id}_input");
    let output_name = format!("direct_{id}_output");
    let (publisher, unused_sub) =
        super::super::runner::open_channels(&input_name, &output_name).unwrap();
    let (unused_pub, subscriber) =
        super::super::runner::open_channels(&output_name, &input_name).unwrap();
    let data = RuntimeData::Binary((0..=255).cycle().take(4096).collect::<Vec<u8>>().into());
    let expected = data_transfer::to_bytes(&data).unwrap();
    let copies = allow();
    publish_runtime_data(&publisher, &data, &copies).unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let sample = loop {
        if let Some(sample) = subscriber.receive().unwrap() {
            break sample;
        }
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    };
    assert_eq!(sample.payload(), expected);
    assert_eq!(serialized(&copies).copied_bytes, 4096);
    assert_eq!(
        copies.counts()[CopyReason::PrivateIpcIngress as usize].copied_bytes,
        0
    );
    drop(publisher);
    drop(unused_pub);
    drop(unused_sub);
    assert_eq!(sample.payload(), expected);
    assert_eq!(data_transfer::from_bytes(sample.payload()).unwrap(), data);
}

#[test]
fn oversized_native_message_fails_without_publishing_or_claiming_copy_completion() {
    let id = uuid::Uuid::new_v4().simple();
    let input_name = format!("oversize_{id}_input");
    let output_name = format!("oversize_{id}_output");
    let (publisher, _unused_sub) =
        super::super::runner::open_channels(&input_name, &output_name).unwrap();
    let (_unused_pub, subscriber) =
        super::super::runner::open_channels(&output_name, &input_name).unwrap();
    let copies = allow();
    let data = RuntimeData::Binary(vec![0; data_transfer::MAX_SLICE_LEN].into());
    let error = publish_runtime_data(&publisher, &data, &copies).unwrap_err();
    assert!(error.contains("loan limit"), "{error}");
    assert!(subscriber.receive().unwrap().is_none());
    assert_eq!(serialized(&copies).completed, 0);
    assert_eq!(serialized(&copies).in_flight, 0);
    publish_runtime_data(&publisher, &RuntimeData::Binary(vec![1; 4].into()), &copies).unwrap();
    assert_eq!(serialized(&copies).copied_bytes, 4);
}
