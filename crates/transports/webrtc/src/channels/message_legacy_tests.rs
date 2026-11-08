use super::*;
use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use remotemedia_types::{CopyCounts, CopyPolicy, CopyReason};

fn wire(kind: &str, encoded: &str, payload_first: bool) -> Vec<u8> {
    let payload = if kind == "Chunk" {
        serde_json::json!({"stream_id":"s", "message_id":"m", "chunk_index":0,
            "total_chunks":1, "is_final":true, "content_type":"application/test",
            "data_type_hint":"binary", "data":encoded})
        .to_string()
    } else {
        serde_json::to_string(encoded).unwrap()
    };
    if payload_first {
        format!(r#"{{"payload":{payload},"type":"{kind}"}}"#).into_bytes()
    } else {
        format!(r#"{{"type":"{kind}","payload":{payload}}}"#).into_bytes()
    }
}

fn counts(copies: &CopyContext) -> CopyCounts {
    copies
        .report()
        .entries
        .into_iter()
        .find(|(reason, _)| *reason == CopyReason::PortableDeserialization)
        .unwrap()
        .1
}

#[test]
fn legacy_bulk_variants_count_exact_decoded_bytes_and_reject_before_materialization() {
    for size in [0, 1, 2, 3, 4, 65536] {
        let payload = vec![37u8; size];
        let encoded = STANDARD.encode(&payload);
        for kind in ["Binary", "RuntimeData", "Chunk"] {
            for payload_first in [false, true] {
                let input = wire(kind, &encoded, payload_first);
                let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "legacy-strict");
                let result =
                    DataChannelMessage::decode_owned_with_context(input.clone().into(), &strict);
                if size == 0 {
                    assert!(result.is_ok());
                } else {
                    assert!(matches!(
                        result,
                        Err(DataChannelDecodeError::CopyRejected(_))
                    ));
                }
                assert_eq!(
                    (counts(&strict).copied_bytes, counts(&strict).rejected_bytes),
                    (0, size as u64)
                );
                let allowed = CopyContext::new(CopyPolicy::AllowCopies, "legacy-compatible");
                let decoded =
                    DataChannelMessage::decode_owned_with_context(input.into(), &allowed).unwrap();
                let bytes = match decoded {
                    DataChannelMessage::Binary { data, .. } => data,
                    DataChannelMessage::RuntimeData { data_buffer, .. } => data_buffer,
                    DataChannelMessage::Chunk { chunk, .. } => {
                        assert_eq!(
                            (chunk.stream_id.as_str(), chunk.message_id.as_str()),
                            ("s", "m")
                        );
                        assert_eq!(chunk.content_type.as_deref(), Some("application/test"));
                        assert_eq!(chunk.data_type_hint.as_deref(), Some("binary"));
                        assert_eq!(
                            (chunk.chunk_index, chunk.total_chunks, chunk.is_final),
                            (0, Some(1), true)
                        );
                        chunk.data
                    }
                    _ => panic!("legacy bulk kind changed"),
                };
                assert_eq!(&*bytes, &payload);
                let report = counts(&allowed);
                assert_eq!(
                    (
                        report.copied_bytes,
                        report.failed,
                        report.rejected,
                        report.in_flight
                    ),
                    (size as u64, 0, 0, 0)
                );
            }
        }
    }
}

#[test]
fn legacy_metadata_and_empty_payloads_do_not_require_bulk_copies() {
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "legacy-metadata");
    let text = br#"{"payload":"hello\nworld","type":"Text"}"#;
    let DataChannelMessage::Text { text, .. } =
        DataChannelMessage::decode_owned_with_context(Bytes::from_static(text), &strict).unwrap()
    else {
        panic!("wrong kind")
    };
    assert_eq!(text, "hello\nworld");
    let json = br#"{"type":"Json","payload":{"action":"ping","value":42}}"#;
    let DataChannelMessage::Control { action, json, .. } =
        DataChannelMessage::decode_owned_with_context(Bytes::from_static(json), &strict).unwrap()
    else {
        panic!("wrong kind")
    };
    assert_eq!(action, "ping");
    assert_eq!(json["value"], 42);
    assert_eq!(counts(&strict), CopyCounts::default());
}

#[test]
fn legacy_escaped_base64_is_guarded_before_json_unescaping() {
    for token in [r#""\/w==""#, r#""\u002fw==""#] {
        let input = format!(r#"{{"payload":{token},"type":"Binary"}}"#);
        let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "escaped-strict");
        assert!(matches!(
            DataChannelMessage::decode_owned_with_context(Bytes::from(input.clone()), &strict),
            Err(DataChannelDecodeError::CopyRejected(_))
        ));
        assert_eq!(
            (counts(&strict).rejected_bytes, counts(&strict).completed),
            (token.len() as u64, 0)
        );
        let allowed = CopyContext::new(CopyPolicy::AllowCopies, "escaped-compatible");
        let DataChannelMessage::Binary { data, .. } =
            DataChannelMessage::decode_owned_with_context(Bytes::from(input), &allowed).unwrap()
        else {
            panic!("wrong kind")
        };
        assert_eq!(&*data, &[255]);
        assert_eq!(
            (counts(&allowed).copied_bytes, counts(&allowed).completed),
            (5, 2)
        );
    }
}

#[test]
fn legacy_invalid_base64_is_a_failed_attempt_and_preserves_error_kind() {
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "malformed");
    let error = DataChannelMessage::decode_owned_with_context(
        wire("Binary", "!!!!", false).into(),
        &copies,
    )
    .unwrap_err();
    assert!(matches!(error, DataChannelDecodeError::Base64(_)));
    assert_eq!(
        (
            counts(&copies).failed,
            counts(&copies).failed_requested_bytes,
            counts(&copies).completed,
            counts(&copies).in_flight
        ),
        (1, 3, 0, 0)
    );
}

#[test]
fn legacy_raw_payload_token_borrows_wire_in_either_field_order() {
    for payload_first in [false, true] {
        let input = wire("Binary", "YWJjZA==", payload_first);
        let envelope: Envelope<'_> = serde_json::from_slice(&input).unwrap();
        let token = envelope.payload.get();
        let text: &str = serde_json::from_str(token).unwrap();
        assert!(token.as_ptr() as usize >= input.as_ptr() as usize);
        assert!((token.as_ptr() as usize + token.len()) <= input.as_ptr() as usize + input.len());
        assert_eq!(text.as_ptr(), token[1..].as_ptr());
        assert_eq!(text, "YWJjZA==");
    }
}
