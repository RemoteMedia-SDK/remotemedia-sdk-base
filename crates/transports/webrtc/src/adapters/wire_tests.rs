use super::*;
use remotemedia_types::{CopyCounts, CopyPolicy};
use std::sync::atomic::Ordering;

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
    let (binary, runtime_audio, deny) = crate::test_support::guarded_payloads();
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

#[test]
fn transport_metadata_rides_in_data_buffer_metadata_for_every_payload() {
    use crate::generated::data_buffer::DataType;
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "wire-metadata");
    let metadata: std::collections::HashMap<String, String> = [
        ("participant.id".to_string(), "alice".to_string()),
        ("participant.role".to_string(), "client".to_string()),
    ]
    .into_iter()
    .collect();
    for data in [RuntimeData::Text("hello".into()), audio(vec![0.25, -0.5, 1.0, 0.0])] {
        let transport = remotemedia_core::transport::TransportData {
            data: data.clone(),
            sequence: None,
            metadata: metadata.clone(),
        };
        let wire = encode_transport_data_with_context(&transport, &copies).unwrap();
        let decoded = DataBuffer::decode(wire.clone()).unwrap();
        assert_eq!(decoded.metadata, metadata);
        // The payload is the same as without metadata.
        let bare = DataBuffer::decode(encode_runtime_data_with_context(&data, &copies).unwrap()).unwrap();
        assert_eq!(decoded.data_type, bare.data_type);
        assert!(matches!(decoded.data_type, Some(DataType::Text(_)) | Some(DataType::Audio(_))));
        // No metadata: identical to the plain encoder.
        let plain = remotemedia_core::transport::TransportData {
            data: data.clone(),
            sequence: None,
            metadata: Default::default(),
        };
        assert_eq!(
            encode_transport_data_with_context(&plain, &copies).unwrap(),
            encode_runtime_data_with_context(&data, &copies).unwrap()
        );
    }
}
