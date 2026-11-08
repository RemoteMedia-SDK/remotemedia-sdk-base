use super::*;
use remotemedia_types::CopyPolicy;

fn bulk(message: &DataChannelMessage) -> &[u8] {
    message
        .as_binary()
        .or_else(|| message.as_runtime_data())
        .or_else(|| message.as_chunk().map(|chunk| chunk.data.as_ref()))
        .unwrap_or_default()
}

fn counts(copies: &CopyContext) -> remotemedia_types::CopyCounts {
    copies
        .report()
        .entries
        .into_iter()
        .find(|(reason, _)| *reason == CopyReason::BorrowedInput)
        .unwrap()
        .1
}

#[test]
fn borrowed_payload_snapshot_is_counted_and_outlives_mutated_source() {
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "borrowed");
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "borrowed-strict");
    let data = vec![0, 255, 1, 128];
    let messages = [
        DataChannelMessage::binary(data.clone()),
        DataChannelMessage::runtime_data(data.clone()),
        DataChannelMessage::chunk(ChunkInfo::new("stream", "id", 0, Some(1), true, data)),
    ];
    for expected in messages {
        let mut wire = expected.encode().unwrap();
        assert!(matches!(
            DataChannelMessage::decode_with_context(&wire, &strict),
            Err(DataChannelDecodeError::CopyRejected(CopyRejected {
                reason: CopyReason::BorrowedInput,
                bytes: 4,
                ..
            }))
        ));
        let decoded = DataChannelMessage::decode_with_context(&wire, &copies).unwrap();
        let start = wire.as_ptr() as usize;
        let pointer = bulk(&decoded).as_ptr() as usize;
        assert!(pointer < start || pointer >= start + wire.len());
        wire.fill(0);
        drop(wire);
        assert_eq!(decoded, expected);
    }
    assert_eq!(
        (counts(&copies).completed, counts(&copies).copied_bytes),
        (3, 12)
    );
    assert_eq!(
        (counts(&strict).rejected, counts(&strict).rejected_bytes),
        (3, 12)
    );
    assert_eq!(counts(&strict).copied_bytes, 0);
}

#[test]
fn borrowed_metadata_and_empty_payloads_pass_strict_without_bulk_charges() {
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "metadata");
    for message in [
        DataChannelMessage::text("hello"),
        DataChannelMessage::json(&serde_json::json!({"control": [1, 2, 3]})).unwrap(),
        DataChannelMessage::binary(Bytes::new()),
        DataChannelMessage::runtime_data(Bytes::new()),
        DataChannelMessage::chunk(ChunkInfo::new("s", "m", 0, Some(1), true, Bytes::new())),
    ] {
        assert_eq!(
            DataChannelMessage::decode_with_context(&message.encode().unwrap(), &strict).unwrap(),
            message
        );
    }
    assert!(strict
        .report()
        .entries
        .iter()
        .all(|(_, c)| c.copied_bytes == 0 && c.rejected == 0));
}

// Deliberately noncanonical wire: generated encoders cannot produce repeated
// singular/oneof fields. Owning Prost decoding is the compatibility oracle.
fn merged_wires() -> Vec<Vec<u8>> {
    vec![
        b"\x10\x03\x62\x06\x0a\x01a\x0a\x01b".to_vec(), // last scalar wins
        b"\x10\x03\x62\x03\x0a\x01a\x62\x03\x12\x01m".to_vec(), // same oneof merges
        b"\x10\x03\x62\x03\x0a\x01a\x62\x00".to_vec(),  // empty message preserves data
        b"\x10\x03\x62\x03\x0a\x01a\x62\x02\x0a\x00".to_vec(), // empty data replaces
        b"\x10\x03\x62\x03\x0a\x01a\x6a\x03\x0a\x01b\x62\x00".to_vec(), // switch resets
        b"\x10\x02\x62\x03\x0a\x01a\x5a\x03\x0a\x01t".to_vec(), // metadata replaces
        b"\x10\x03\x62\x03\x0a\x01a\x5a\x00\x62\x00".to_vec(), // metadata resets
        b"\x10\x04\x6a\x03\x0a\x01a\x6a\x03\x0a\x01b".to_vec(),
        b"\x10\x05\x72\x06\x0a\x01s\x42\x01a\x72\x03\x12\x01m".to_vec(),
        b"\x10\x03\x62\x03\x0a\x01a\x7a\x03\x0a\x01x".to_vec(), // unknown envelope bytes
        b"\x10\x03\x62\x07\x0a\x01a\x7b\x08\x01\x7c".to_vec(),  // unknown inner group
    ]
}

#[test]
fn borrowed_decoder_materializes_only_final_merged_field() {
    for wire in merged_wires() {
        let expected = DataChannelMessage::decode_owned(Bytes::copy_from_slice(&wire)).unwrap();
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "merged");
        let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "merged-strict");
        assert_eq!(
            DataChannelMessage::decode_with_context(&wire, &copies).unwrap(),
            expected
        );
        let len = bulk(&expected).len() as u64;
        assert_eq!(counts(&copies).copied_bytes, len);
        assert_eq!(counts(&copies).completed, u64::from(len != 0));
        assert_eq!(
            DataChannelMessage::decode_with_context(&wire, &strict).is_ok(),
            len == 0
        );
        assert_eq!(counts(&strict).rejected_bytes, len);
    }
}

fn assert_matches_owning(wire: &[u8]) {
    let expected = DataChannelMessage::decode_owned(Bytes::copy_from_slice(wire));
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "differential");
    let actual = DataChannelMessage::decode_with_context(wire, &copies);
    match (expected, actual) {
        (Ok(expected), Ok(actual)) => {
            assert_eq!(actual, expected, "{wire:?}");
            assert_eq!(counts(&copies).copied_bytes, bulk(&expected).len() as u64);
        }
        (Err(expected), Err(actual)) => {
            assert_eq!(
                std::mem::discriminant(&actual),
                std::mem::discriminant(&expected),
                "{wire:?}"
            );
            assert_eq!(counts(&copies).copied_bytes, 0);
        }
        (expected, actual) => panic!("decoder mismatch for {wire:?}: {expected:?} vs {actual:?}"),
    }
}

#[test]
fn borrowed_decoder_matches_prost_for_truncations_and_single_byte_mutations() {
    for wire in merged_wires() {
        for len in 0..wire.len() {
            assert_matches_owning(&wire[..len]);
        }
        for position in 0..wire.len() {
            for byte in 0..=255 {
                let mut mutated = wire.clone();
                mutated[position] = byte;
                assert_matches_owning(&mutated);
            }
        }
    }
}

#[test]
fn malformed_and_mismatched_envelopes_reject_before_bulk_materialization() {
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "malformed");
    for wire in [
        b"\x10\x03\x62\x03\x0a\x01a\xff".as_slice(), // malformed trailing key
        b"\x10\x03\x62\x02\x0a\x01a",                // field exceeds nested length
        b"\x10\x03\x62\x0b\x0a\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01", // enormous length
        b"\x10\x03\x62\x02\x08\x01",                 // wrong wire type for bulk
        b"\x10\x03\x60\x01",                         // wrong wire type for frame
        b"\x10\x04\x62\x03\x0a\x01a",                // kind/payload mismatch
        b"\x10\x7f\x62\x03\x0a\x01a",                // unknown kind
    ] {
        assert_matches_owning(wire);
        assert!(!matches!(
            DataChannelMessage::decode_with_context(wire, &strict),
            Ok(_) | Err(DataChannelDecodeError::CopyRejected(_))
        ));
    }
    assert_eq!(counts(&strict).rejected, 0);
}

#[test]
fn borrowed_decoder_preserves_unknown_group_recursion_limit() {
    for depth in [1, 98, 99, 100, 101, 150] {
        let mut frame = vec![0x7b; depth];
        frame.extend_from_slice(&[0x08, 1]);
        frame.extend(std::iter::repeat(0x7c).take(depth));
        frame.extend_from_slice(&[0x0a, 1, b'a']);
        let mut wire = vec![0x10, 3, 0x62];
        prost::encoding::encode_varint(frame.len() as u64, &mut wire);
        wire.extend(frame);
        assert_matches_owning(&wire);
    }
}

#[test]
fn borrowed_legacy_fallback_retains_portable_decoder_policy() {
    let wire = br#"{"type":"Binary","payload":"AAEC/w=="}"#;
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "legacy-borrowed");
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "legacy-borrowed-strict");
    assert_eq!(
        bulk(&DataChannelMessage::decode_with_context(wire, &copies).unwrap()),
        &[0, 1, 2, 255]
    );
    assert!(matches!(
        DataChannelMessage::decode_with_context(wire, &strict),
        Err(DataChannelDecodeError::CopyRejected(CopyRejected {
            reason: CopyReason::PortableDeserialization,
            bytes: 4,
            ..
        }))
    ));
    assert_eq!(counts(&copies).copied_bytes, 0);
    assert_eq!(
        copies
            .report()
            .entries
            .iter()
            .map(|(_, c)| c.copied_bytes)
            .sum::<u64>(),
        4
    );
}
