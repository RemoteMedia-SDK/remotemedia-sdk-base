use super::*;
use remotemedia_types::{BufferBudget, CopyPolicy, RuntimeData};

fn fixtures() -> Vec<(DataChannelMessage, &'static [u8])> {
    vec![
        (DataChannelMessage::binary(vec![0, 1, 2, 255]),
         b"\x08\x01\x10\x03\x1a\x06binary\x62\x06\x0a\x04\x00\x01\x02\xff"),
        (DataChannelMessage::runtime_data(vec![0, 1, 2, 255]),
         b"\x08\x01\x10\x04\x1a\x0bnode_output\x6a\x06\x0a\x04\x00\x01\x02\xff"),
        (DataChannelMessage::chunk(ChunkInfo::new("s", "m", 0, Some(1), true, vec![0, 1, 2, 255])),
         b"\x08\x01\x10\x05\x1a\x12runtime_data_chunk\x72\x10\x0a\x01s\x12\x01m\x20\x01\x28\x01\x42\x04\x00\x01\x02\xff"),
    ]
}

fn payload(message: &DataChannelMessage) -> &Bytes {
    match message {
        DataChannelMessage::Binary { data, .. } => data,
        DataChannelMessage::RuntimeData { data_buffer, .. } => data_buffer,
        DataChannelMessage::Chunk { chunk, .. } => &chunk.data,
        _ => unreachable!(),
    }
}

#[test]
fn channel_envelope_wire_matches_existing_schema_for_all_bulk_kinds() {
    for (message, wire) in fixtures() {
        assert_eq!(message.encode().unwrap(), wire);
        assert_eq!(DataChannelMessage::decode(wire).unwrap(), message);
        assert_eq!(
            DataChannelMessage::decode_owned(Bytes::from_static(wire)).unwrap(),
            message
        );
    }
}

#[test]
fn channel_envelope_decoded_ranges_and_clones_retain_original_wire_budget() {
    for (_, wire) in fixtures() {
        let budget = BufferBudget::new(1, wire.len() as u64);
        let RuntimeData::Binary(source) = budget
            .admit(RuntimeData::Binary(wire.to_vec().into()))
            .unwrap()
        else {
            unreachable!()
        };
        let pointer = source.as_ptr() as usize;
        let message = DataChannelMessage::decode_owned(Bytes::from_owner(source)).unwrap();
        let clone = message.clone();
        assert_eq!(payload(&clone).as_ptr(), payload(&message).as_ptr());
        let data = payload(&message).slice(1..3);
        assert!(data.as_ptr() as usize >= pointer);
        assert!(data.as_ptr() as usize + data.len() <= pointer + wire.len());
        drop((message, clone));
        assert_eq!(&data[..], &[1, 2]);
        assert_eq!(budget.usage().owners, 1);
        std::thread::spawn(move || drop(data)).join().unwrap();
        assert_eq!(budget.usage().owners, 0);
    }
}

#[test]
fn channel_envelope_encoding_counts_one_bulk_materialization_and_rejects_strict() {
    let allowed = CopyContext::new(CopyPolicy::AllowCopies, "envelope");
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "envelope-strict");
    for (message, wire) in fixtures() {
        assert_eq!(message.encode_with_context(&allowed).unwrap(), wire);
        assert!(matches!(
            message.encode_with_context(&strict),
            Err(DataChannelEncodeError::CopyRejected(CopyRejected {
                bytes: 4,
                ..
            }))
        ));
    }
    let allowed = allowed
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == CopyReason::PortableSerialization)
        .unwrap()
        .1;
    assert_eq!(
        (allowed.completed, allowed.copied_bytes, allowed.failed),
        (3, 12, 0)
    );
    let strict = strict
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == CopyReason::PortableSerialization)
        .unwrap()
        .1;
    assert_eq!(
        (strict.rejected, strict.rejected_bytes, strict.completed),
        (3, 12, 0)
    );
}

#[test]
fn channel_envelope_legacy_base64_and_malformed_input_remain_explicit() {
    let wire = Bytes::from_static(br#"{"type":"Binary","payload":"AAEC/w=="}"#);
    assert_eq!(
        &payload(&DataChannelMessage::decode_owned(wire).unwrap())[..],
        &[0, 1, 2, 255]
    );
    for wire in [b"\xff".as_slice(), b"\x08\x01\x10\x63", b"\x08\x01\x10\x03"] {
        assert!(DataChannelMessage::decode_owned(Bytes::copy_from_slice(wire)).is_err());
    }
}
