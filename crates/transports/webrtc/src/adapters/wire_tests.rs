use super::*;
use crate::test_support::Guarded;
use remotemedia_types::{CopyCounts, CopyPolicy, SharedBuffer};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

fn audio(samples: Vec<f32>) -> RuntimeData {
    RuntimeData::Audio {
        samples: samples.into(),
        sample_rate: 48000,
        channels: 2,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: Some(serde_json::json!({"speaker": "test"})),
    }
}

fn counts(copies: &CopyContext) -> CopyCounts {
    copies
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == CopyReason::PortableSerialization)
        .unwrap()
        .1
}

#[test]
fn wire_output_audio_matches_generated_encoder_at_varint_boundaries() {
    for len in [0, 1, 31, 32, 4095, 4096] {
        let values = [0, 0x80000000, 0x7fc01234, 1, 0x7f800000, 0xbf000000];
        let runtime = audio(
            (0..len)
                .map(|i| f32::from_bits(values[i % values.len()]))
                .collect(),
        );
        let reference = runtime_data_to_data_buffer(&runtime).encode_to_vec();
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "direct-audio-wire");
        let wire = encode_runtime_data_with_context(&runtime, &copies).unwrap();
        assert_eq!(wire.as_ref(), reference);
        assert_eq!(counts(&copies).copied_bytes, (len * 4) as u64);
        assert_eq!(
            DataBuffer::decode(wire).unwrap(),
            runtime_data_to_data_buffer(&runtime)
        );
    }
}

#[test]
fn wire_output_bulk_variants_match_generated_encoder_and_count_once() {
    for data in super::super::ownership_tests::cases() {
        let runtime = data_buffer_into_runtime_data(DataBuffer {
            data_type: Some(data),
            ..Default::default()
        })
        .unwrap();
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "bulk-wire");
        let wire = encode_runtime_data_with_context(&runtime, &copies).unwrap();
        assert_eq!(
            wire.as_ref(),
            runtime_data_to_data_buffer(&runtime).encode_to_vec()
        );
        assert_eq!(counts(&copies).copied_bytes, 16);
        assert_eq!(counts(&copies).completed, 1);
        let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "strict-bulk-wire");
        assert_eq!(
            encode_runtime_data_with_context(&runtime, &strict)
                .unwrap_err()
                .bytes,
            16
        );
        assert_eq!(counts(&strict).copied_bytes, 0);
    }
}

#[test]
fn wire_output_strict_rejects_before_accessing_external_payload_storage() {
    let deny = Arc::new(AtomicBool::new(false));
    let samples = SharedBuffer::from_owner(Guarded {
        values: vec![0.25f32; 4],
        deny: deny.clone(),
    });
    let mut runtime_audio = audio(Vec::new());
    if let RuntimeData::Audio {
        samples: target, ..
    } = &mut runtime_audio
    {
        *target = samples.into();
    }
    let binary = RuntimeData::Binary(SharedBuffer::from_owner(Guarded {
        values: vec![42u8; 16],
        deny: deny.clone(),
    }));
    deny.store(true, Ordering::SeqCst);
    for runtime in [runtime_audio, binary] {
        let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "read-barrier");
        assert_eq!(
            encode_runtime_data_with_context(&runtime, &strict)
                .unwrap_err()
                .bytes,
            16
        );
        assert_eq!(counts(&strict).copied_bytes, 0);
    }
}

#[test]
fn wire_output_metadata_and_empty_payloads_pass_strict_policy() {
    for runtime in [
        RuntimeData::Text("hello".into()),
        RuntimeData::Json(serde_json::json!({"done":true})),
        RuntimeData::Binary(Vec::new().into()),
        audio(Vec::new()),
    ] {
        let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "metadata-wire");
        let wire = encode_runtime_data_with_context(&runtime, &strict).unwrap();
        assert_eq!(
            wire.as_ref(),
            runtime_data_to_data_buffer(&runtime).encode_to_vec()
        );
        assert_eq!(counts(&strict), CopyCounts::default());
    }
}

#[test]
fn wire_output_image_fallback_counts_visible_bytes() {
    let runtime = RuntimeData::Image {
        data: vec![7; 16].into(),
        format: remotemedia_types::ImageFormat::Png,
        width: 2,
        height: 2,
        timestamp_us: Some(10),
        stream_id: Some("image".into()),
        metadata: None,
    };
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "image-wire");
    let wire = encode_runtime_data_with_context(&runtime, &copies).unwrap();
    assert_eq!(
        wire.as_ref(),
        runtime_data_to_data_buffer(&runtime).encode_to_vec()
    );
    assert_eq!(counts(&copies).copied_bytes, 16);
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "image-strict");
    assert_eq!(
        encode_runtime_data_with_context(&runtime, &strict)
            .unwrap_err()
            .bytes,
        16
    );
}

#[test]
fn wire_output_file_references_and_control_keep_metadata_semantics() {
    let cases = [
        RuntimeData::File {
            path: "not-read-by-serializer".into(),
            filename: None,
            mime_type: None,
            size: Some(123),
            offset: Some(4),
            length: Some(8),
            stream_id: None,
        },
        RuntimeData::ControlMessage {
            message_type: remotemedia_types::ControlMessageType::BatchHint {
                suggested_batch_size: 4,
            },
            segment_id: Some("segment".into()),
            timestamp_ms: 20,
            metadata: serde_json::json!({}),
        },
    ];
    for runtime in cases {
        let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "metadata-wire");
        let wire = encode_runtime_data_with_context(&runtime, &strict).unwrap();
        assert_eq!(
            wire.as_ref(),
            runtime_data_to_data_buffer(&runtime).encode_to_vec()
        );
        assert_eq!(counts(&strict), CopyCounts::default());
    }
}
