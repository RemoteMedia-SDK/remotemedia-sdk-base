use super::*;
use crate::channels::{ChunkInfo, DataChannelSplitter};
use remotemedia_types::{BufferBudget, CopyCounts, CopyPolicy, CopyReason};

// Independent common.proto wire fixture: field 6 BinaryBuffer, field 1 bytes.
const BINARY: &[u8] = b"\x32\x06\x0a\x04abcd";

#[test]
fn raw_envelope_and_single_chunk_retain_wire_owner_through_pipeline_delivery() {
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "ingress");
    for encoded in [
        BINARY.to_vec(),
        DataChannelMessage::runtime_data(Bytes::from_static(BINARY))
            .encode()
            .unwrap(),
        DataChannelMessage::chunk(ChunkInfo::new(
            "s",
            "m",
            0,
            Some(1),
            true,
            Bytes::from_static(BINARY),
        ))
        .encode()
        .unwrap(),
    ] {
        // Envelope encoding may reserve more than the visible wire length.
        let budget = BufferBudget::new(1, encoded.capacity() as u64);
        let RuntimeData::Binary(wire) = budget.admit(RuntimeData::Binary(encoded.into())).unwrap()
        else {
            unreachable!()
        };
        let start = wire.as_ptr() as usize;
        let end = start + wire.len();
        let mut pending = DataChannelReassembler::new();
        let output = decode_pipeline_input(Bytes::from_owner(wire), &mut pending, &strict)
            .unwrap()
            .unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(output).unwrap();
        drop((tx, pending));
        let RuntimeData::Binary(data) = rx.try_recv().unwrap() else {
            unreachable!()
        };
        assert_eq!(&*data, b"abcd");
        assert!((data.as_ptr() as usize) >= start && (data.as_ptr() as usize) + data.len() <= end);
        let retained = data.slice(1..3).unwrap();
        drop((data, rx));
        assert_eq!(budget.usage().owners, 1);
        assert_eq!(&*retained, b"bc");
        drop(retained);
        assert_eq!(budget.usage().owners, 0);
    }
    assert!(strict
        .report()
        .entries
        .iter()
        .all(|(_, c)| *c == CopyCounts::default()));
}

#[test]
fn fragmented_ingress_counts_only_contiguous_assembly_and_rejects_strict() {
    let chunks = DataChannelSplitter::with_max_chunk_size("s", "m", 4)
        .split_owned(Bytes::from_static(BINARY))
        .unwrap();
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        let copies = CopyContext::new(policy, "fragmented");
        let mut pending = DataChannelReassembler::new();
        let first = DataChannelMessage::chunk(chunks[1].clone())
            .encode()
            .unwrap();
        assert!(decode_pipeline_input(first.into(), &mut pending, &copies)
            .unwrap()
            .is_none());
        let last = DataChannelMessage::chunk(chunks[0].clone())
            .encode()
            .unwrap();
        let output = decode_pipeline_input(last.into(), &mut pending, &copies);
        if policy == CopyPolicy::AllowCopies {
            let RuntimeData::Binary(data) = output.unwrap().unwrap() else {
                unreachable!()
            };
            assert_eq!(&*data, b"abcd");
        } else {
            assert!(output.is_err());
        }
        assert_eq!((pending.memory_used(), pending.pending_count()), (0, 0));
        let report = copies.report();
        let c = &report
            .entries
            .iter()
            .find(|(r, _)| *r == CopyReason::VecMaterialization)
            .unwrap()
            .1;
        if policy == CopyPolicy::AllowCopies {
            assert_eq!((c.completed, c.copied_bytes), (1, BINARY.len() as u64));
        } else {
            assert_eq!(
                (c.rejected, c.rejected_bytes, c.completed),
                (1, BINARY.len() as u64, 0)
            );
        }
        assert!(report
            .entries
            .iter()
            .filter(|(r, _)| *r != CopyReason::VecMaterialization)
            .all(|(_, c)| *c == CopyCounts::default()));
    }
}

fn audio_wire() -> Bytes {
    use crate::generated::{data_buffer::DataType, AudioBuffer, AudioFormat};
    let buffer = DataBuffer {
        data_type: Some(DataType::Audio(AudioBuffer {
            samples: Bytes::from_static(&[0, 0, 128, 62, 0, 0, 0, 191]),
            sample_rate: 48000,
            channels: 1,
            num_samples: 2,
            format: AudioFormat::F32 as i32,
            ..Default::default()
        })),
        ..Default::default()
    };
    Bytes::from(buffer.encode_to_vec())
}

#[test]
fn audio_ingress_authorizes_only_the_aligned_element_conversion() {
    let wire = audio_wire();
    let mut pending = DataChannelReassembler::new();
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "audio-ingress-strict");
    assert!(decode_pipeline_input(wire.clone(), &mut pending, &strict).is_err());
    let allowed = CopyContext::new(CopyPolicy::AllowCopies, "audio-ingress");
    let RuntimeData::Audio {
        samples,
        sample_rate,
        channels,
        ..
    } = decode_pipeline_input(wire, &mut pending, &allowed)
        .unwrap()
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(samples.as_slice(), &[0.25, -0.5]);
    assert_eq!((sample_rate, channels), (48000, 1));
    for (context, copied, rejected) in [(&allowed, 8, 0), (&strict, 0, 8)] {
        let report = context.report();
        let c = &report
            .entries
            .iter()
            .find(|(r, _)| *r == CopyReason::PortableDeserialization)
            .unwrap()
            .1;
        assert_eq!(
            (c.copied_bytes, c.rejected_bytes, c.failed),
            (copied, rejected, 0)
        );
    }
}

#[tokio::test]
async fn session_policy_reaches_audio_ingress_and_survives_join() {
    use remotemedia_core::manifest::{Manifest, NodeManifest};
    use remotemedia_core::transport::{ExecutorConfig, PipelineExecutor};
    use std::sync::Arc;
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        let executor = PipelineExecutor::with_config(ExecutorConfig {
            transport_materialization_policy: policy,
            ..Default::default()
        })
        .unwrap();
        let manifest = Arc::new(Manifest {
            nodes: vec![NodeManifest {
                id: "calc".into(),
                node_type: "CalculatorNode".into(),
                params: serde_json::json!({}),
                ..Default::default()
            }],
            ..Default::default()
        });
        let mut session = executor.create_session(manifest).await.unwrap();
        let copies = session.transport_copy_context();
        let result =
            decode_pipeline_input(audio_wire(), &mut DataChannelReassembler::new(), &copies);
        session.close().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), session.wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.is_ok(), policy == CopyPolicy::AllowCopies);
        if policy == CopyPolicy::AllowCopies {
            let RuntimeData::Audio { samples, .. } = result.unwrap().unwrap() else {
                panic!("audio ingress returned the wrong payload kind");
            };
            assert_eq!(samples.as_slice(), &[0.25, -0.5]);
        }
        let report = copies.report();
        let counts = report
            .entries
            .iter()
            .find(|(reason, _)| *reason == CopyReason::PortableDeserialization)
            .unwrap()
            .1;
        assert_eq!(
            (counts.copied_bytes, counts.rejected_bytes),
            if policy == CopyPolicy::AllowCopies {
                (8, 0)
            } else {
                (0, 8)
            }
        );
        assert_eq!((counts.failed, counts.in_flight), (0, 0));
    }
}

#[test]
fn non_pipeline_messages_and_invalid_buffers_do_not_invent_runtime_input() {
    let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "non-pipeline");
    let mut pending = DataChannelReassembler::new();
    for message in [
        DataChannelMessage::text("hello"),
        DataChannelMessage::binary(vec![1, 2, 3]),
        DataChannelMessage::json(&serde_json::json!({"action":"ping"})).unwrap(),
    ] {
        assert!(
            decode_pipeline_input(message.encode().unwrap().into(), &mut pending, &copies)
                .unwrap()
                .is_none()
        );
    }
    for wire in [
        b"\xff".as_slice(),
        b"\x32\x07\x0a\x04abcd",
        b"",
        b"\x0a\x05\x0a\x03abc",
    ] {
        assert!(
            decode_pipeline_input(Bytes::copy_from_slice(wire), &mut pending, &copies).is_err()
        );
    }
    assert!(copies
        .report()
        .entries
        .iter()
        .all(|(_, c)| *c == CopyCounts::default()));
}
