use super::*;
use remotemedia_types::{CopyCounts, CopyPolicy};
use std::sync::atomic::Ordering;

fn counts(copies: &CopyContext, reason: CopyReason) -> CopyCounts {
    copies
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == reason)
        .unwrap()
        .1
}

fn audio(samples: Vec<f32>) -> RuntimeData {
    RuntimeData::Audio {
        samples: samples.into(),
        sample_rate: 48000,
        channels: 2,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    }
}

#[test]
fn websocket_bulk_output_matches_json_schema_and_counts_one_transfer() {
    for len in [0, 1, 2, 3, 127, 128, 1025, 65536] {
        let samples: Vec<f32> = (0..len)
            .map(|i| f32::from_bits([0, 0x80000000, 0x7fc01234, 1][i % 4]))
            .collect();
        let sample_bytes: Vec<u8> = samples.iter().flat_map(|s| s.to_le_bytes()).collect();
        for (data, expected, field) in [
            (
                RuntimeData::Binary(vec![42; len].into()),
                vec![42; len],
                "b64",
            ),
            (audio(samples), sample_bytes, "samples_b64"),
        ] {
            let copies = CopyContext::new(CopyPolicy::AllowCopies, "ws-output");
            let encoded = encode_event("node.out.\"\n", &data, true, &copies).unwrap();
            let mut wire: Value = serde_json::from_str(&encoded).unwrap();
            assert_eq!(wire["params"]["payload"][field], STANDARD.encode(&expected));
            let stamp = wire["params"]["ts"].clone();
            wire["params"]["payload"]
                .as_object_mut()
                .unwrap()
                .remove(field);
            let mut metadata = event_params("node.out.\"\n", &data);
            metadata["ts"] = stamp;
            assert_eq!(
                wire,
                json!({"jsonrpc":"2.0","method":"control.event","params":metadata})
            );
            let c = counts(&copies, CopyReason::PortableSerialization);
            assert_eq!(
                (c.copied_bytes, c.completed),
                (expected.len() as u64, u64::from(!expected.is_empty()))
            );
            assert_eq!(
                counts(&copies, CopyReason::VecMaterialization),
                CopyCounts::default()
            );
        }
    }
}

#[test]
fn websocket_strict_metadata_audio_does_not_read_samples_and_bulk_rejects() {
    let (binary, data, deny) = crate::test_support::guarded_payloads();
    deny.store(true, Ordering::SeqCst);
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "ws-strict");
    let metadata: Value =
        serde_json::from_str(&encode_event("audio.out", &data, false, &strict).unwrap()).unwrap();
    assert_eq!(
        metadata["params"]["payload"],
        json!({"size":4,"sample_rate":48000,"channels":2})
    );
    assert!(encode_event("audio.out", &data, true, &strict).is_err());
    assert!(encode_event("binary.out", &binary, false, &strict).is_err());
    let c = counts(&strict, CopyReason::PortableSerialization);
    assert_eq!((c.copied_bytes, c.rejected_bytes), (0, 32));
}

#[test]
fn websocket_publish_borrows_raw_tokens_before_attributed_base64_decode() {
    for token in [r#""/w==""#, r#""\/w==""#, r#""\u002fw==""#, r#""""#] {
        for raw_first in [false, true] {
            let params = format!(r#"{{"payload":{{"binary_b64":{token}}},"topic":"node.in"}}"#);
            let text = if raw_first {
                format!(
                    r#"{{"params":{params},"id":7,"method":"control.publish","jsonrpc":"2.0"}}"#
                )
            } else {
                format!(
                    r#"{{"jsonrpc":"2.0","method":"control.publish","id":7,"params":{params}}}"#
                )
            };
            let request: Request<'_> = serde_json::from_str(&text).unwrap();
            assert_eq!(request.params.get(), params);
            let p = request.params.get().as_ptr() as usize;
            assert!((text.as_ptr() as usize..text.as_ptr() as usize + text.len()).contains(&p));
            let fields: BTreeMap<String, &RawValue> =
                serde_json::from_str(request.params.get()).unwrap();
            let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "ws-input-strict");
            assert_eq!(
                coerce_payload(fields["payload"], &strict).is_ok(),
                token == r#""""#
            );
            assert_eq!(
                counts(&strict, CopyReason::PortableDeserialization).copied_bytes,
                0
            );
            let copies = CopyContext::new(CopyPolicy::AllowCopies, "ws-input");
            let RuntimeData::Binary(data) = coerce_payload(fields["payload"], &copies).unwrap()
            else {
                panic!("binary")
            };
            assert_eq!(
                data.as_ref(),
                if token == r#""""# {
                    &[][..]
                } else {
                    &[255][..]
                }
            );
            assert_eq!(
                counts(&copies, CopyReason::PortableDeserialization).copied_bytes,
                if token == r#""""# {
                    0
                } else if token.contains('\\') {
                    5
                } else {
                    1
                }
            );
        }
    }
}

#[test]
fn websocket_publish_preserves_payload_precedence_and_invalid_input_errors() {
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "metadata-input");
    for (wire, expected) in [
        (
            r#"{"text":"hello","json":{},"binary_b64":"not base64"}"#,
            RuntimeData::Text("hello".into()),
        ),
        (
            r#"{"json":null,"binary_b64":"not base64"}"#,
            RuntimeData::Json(Value::Null),
        ),
        (r#""hello""#, RuntimeData::Text("hello".into())),
        (r#"[1,2,3]"#, RuntimeData::Json(json!([1, 2, 3]))),
        (
            r#"{"binary_b64":null}"#,
            RuntimeData::Json(json!({"binary_b64":null})),
        ),
    ] {
        let actual = coerce_payload(serde_json::from_str(wire).unwrap(), &strict).unwrap();
        assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
    }
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "invalid-base64");
    assert!(coerce_payload(
        serde_json::from_str(r#"{"binary_b64":"!!"}"#).unwrap(),
        &copies
    )
    .is_err());
    assert_eq!(
        counts(&copies, CopyReason::PortableDeserialization).failed,
        1
    );
    assert!(coerce_payload(serde_json::from_str("42").unwrap(), &strict).is_err());
}

#[cfg(target_endian = "little")]
#[test]
fn websocket_audio_encoding_borrows_native_sample_storage() {
    let samples = [f32::from_bits(0x7fc01234), -0.0, 0.25];
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "pcm-view");
    let bytes = audio_bytes(&samples, &copies).unwrap();
    assert!(matches!(bytes, std::borrow::Cow::Borrowed(_)));
    assert_eq!(bytes.as_ptr(), samples.as_ptr().cast::<u8>());
    assert_eq!(
        counts(&copies, CopyReason::VecMaterialization),
        CopyCounts::default()
    );
}
