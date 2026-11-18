use super::*;
use remotemedia_types::CopyPolicy;

fn every_variant() -> Vec<RuntimeData> {
    vec![
        RuntimeData::Audio {
            samples: vec![0.25f32, -1.0, 0.5, f32::MIN_POSITIVE].into(),
            sample_rate: 48_000,
            channels: 2,
            stream_id: Some("mic".into()),
            timestamp_us: Some(10),
            arrival_ts_us: Some(11),
            metadata: Some(serde_json::json!({ "speaker": 1 })),
        },
        RuntimeData::Audio {
            samples: Vec::<f32>::new().into(),
            sample_rate: 16_000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        },
        RuntimeData::Video {
            pixel_data: vec![1, 2, 3].into(),
            width: 2,
            height: 2,
            format: PixelFormat::Encoded,
            codec: Some(VideoCodec::H264),
            frame_number: 7,
            timestamp_us: 99,
            is_keyframe: true,
            stream_id: Some("cam".into()),
            arrival_ts_us: Some(5),
        },
        RuntimeData::Image {
            data: vec![9, 8].into(),
            format: ImageFormat::Raw {
                pixel_format: PixelFormat::Rgba32,
            },
            width: 1,
            height: 1,
            timestamp_us: Some(3),
            stream_id: Some("still".into()),
            metadata: Some(serde_json::json!({ "alt": "x" })),
        },
        RuntimeData::Image {
            data: vec![0xff, 0xd8].into(),
            format: ImageFormat::Jpeg,
            width: 640,
            height: 480,
            timestamp_us: None,
            stream_id: None,
            metadata: None,
        },
        RuntimeData::Tensor {
            data: vec![0; 8].into(),
            shape: vec![2, 3, 4],
            dtype: 3,
            metadata: Some(serde_json::json!({ "layout": "nchw" })),
        },
        RuntimeData::Numpy {
            data: vec![0; 16].into(),
            shape: vec![2, 2],
            dtype: "float32".into(),
            strides: vec![8, -4],
            c_contiguous: true,
            f_contiguous: false,
        },
        RuntimeData::Json(serde_json::json!({ "a": [1, 2], "b": null })),
        RuntimeData::Text("plain".into()),
        RuntimeData::Text(tag_text_str("hello", "ui")),
        RuntimeData::Binary(vec![0, 255].into()),
        RuntimeData::ControlMessage {
            message_type: ControlMessageType::CancelSpeculation {
                from_timestamp: 1,
                to_timestamp: 2,
            },
            segment_id: Some("seg".into()),
            timestamp_ms: 3,
            metadata: serde_json::json!({ "reason": "barge" }),
        },
        RuntimeData::ControlMessage {
            message_type: ControlMessageType::BatchHint {
                suggested_batch_size: 4,
            },
            segment_id: None,
            timestamp_ms: 0,
            metadata: serde_json::Value::Null,
        },
        RuntimeData::ControlMessage {
            message_type: ControlMessageType::DeadlineWarning { deadline_us: 500 },
            segment_id: None,
            timestamp_ms: 9,
            metadata: serde_json::json!(false),
        },
        RuntimeData::File {
            path: "/tmp/x".into(),
            filename: Some("x".into()),
            mime_type: Some("text/plain".into()),
            size: Some(10),
            offset: None,
            length: Some(4),
            stream_id: Some("files".into()),
        },
    ]
}

fn round_trip(data: &RuntimeData, copies: &CopyContext) -> Frame {
    let metadata = HashMap::from([("participant.id".to_string(), "ana".to_string())]);
    let wire = encode_frame(data, &metadata, Some(42), copies).unwrap();
    decode_frame(Bytes::from(wire), copies).unwrap()
}

#[test]
fn every_variant_round_trips_with_its_envelope() {
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "test");
    for data in every_variant() {
        let frame = round_trip(&data, &copies);
        assert_eq!(frame.data, data);
        assert_eq!(frame.sequence, Some(42));
        assert_eq!(frame.metadata["participant.id"], "ana");
    }
}

#[test]
fn embedded_data_buffer_matches_frame_encoding() {
    let copies = CopyContext::new(CopyPolicy::AllowCopies, "test");
    for data in every_variant() {
        let buffer = runtime_data_to_data_buffer(&data, &copies).unwrap();
        let direct = encode_frame(&data, &HashMap::new(), None, &copies).unwrap();
        assert_eq!(buffer.encode_to_vec(), direct, "{}", data.data_type());
        assert_eq!(
            data_buffer_into_runtime_data(buffer, &copies).unwrap(),
            data
        );
    }
}

/// Audio crosses as 4 raw bytes per sample, bit-exact: no decimal text.
#[test]
fn audio_is_raw_f32_le_and_bit_exact() {
    let samples = vec![0.1f32, -0.333_333_34, f32::MIN_POSITIVE, 1.0];
    let data = RuntimeData::Audio {
        samples: samples.clone().into(),
        sample_rate: 8_000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    };
    let wire = encode_frame(&data, &HashMap::new(), None, CopyContext::legacy()).unwrap();
    let raw: Vec<u8> = samples.iter().flat_map(|s| s.to_le_bytes()).collect();
    assert!(wire.windows(raw.len()).any(|window| window == raw));
    assert!(wire.len() < raw.len() + 16, "{} bytes", wire.len());
    let RuntimeData::Audio { samples: got, .. } =
        decode_frame(Bytes::from(wire), CopyContext::legacy())
            .unwrap()
            .data
    else {
        panic!("expected audio");
    };
    let bits = |s: &[f32]| s.iter().map(|v| v.to_bits()).collect::<Vec<_>>();
    assert_eq!(bits(&got), bits(&samples));
}

/// Decoded bulk payloads point into the received buffer.
#[test]
fn decoded_bulk_payloads_share_the_wire_buffer() {
    let data = RuntimeData::Binary(vec![7u8; 4096].into());
    let wire =
        Bytes::from(encode_frame(&data, &HashMap::new(), None, CopyContext::legacy()).unwrap());
    let range = wire.as_ptr_range();
    let RuntimeData::Binary(bytes) = decode_frame(wire.clone(), CopyContext::legacy())
        .unwrap()
        .data
    else {
        panic!("expected binary");
    };
    assert!(range.contains(&bytes.as_ref().as_ptr()));
}

#[test]
fn strict_context_rejects_before_encoding_bulk_data() {
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "strict");
    let data = RuntimeData::Binary(vec![1, 2, 3].into());
    assert!(matches!(
        encode_frame(&data, &HashMap::new(), None, &strict),
        Err(WireError::Rejected(_))
    ));
    // Metadata-only frames still cross.
    let text = RuntimeData::Text("ok".into());
    assert!(encode_frame(&text, &HashMap::new(), None, &strict).is_ok());
}

#[test]
fn tensor_dimensions_are_unsigned_on_the_wire() {
    let negative = RuntimeData::Tensor {
        data: vec![0; 4].into(),
        shape: vec![-1, 4],
        dtype: 1,
        metadata: None,
    };
    assert!(matches!(
        encode_frame(&negative, &HashMap::new(), None, CopyContext::legacy()),
        Err(WireError::Malformed(_))
    ));
    let oversized = DataBuffer {
        data_type: Some(DataType::Tensor(TensorBuffer {
            data: Bytes::from_static(&[1]),
            shape: vec![u64::MAX],
            ..Default::default()
        })),
        ..Default::default()
    };
    assert!(decode_frame(
        Bytes::from(oversized.encode_to_vec()),
        CopyContext::legacy()
    )
    .is_err());
}

#[test]
fn malformed_frames_are_errors_not_data() {
    let copies = CopyContext::legacy();
    let decode = |buffer: DataBuffer| decode_frame(Bytes::from(buffer.encode_to_vec()), copies);
    assert!(decode(DataBuffer::default()).is_err());
    assert!(decode(DataBuffer {
        data_type: Some(DataType::Audio(AudioBuffer {
            samples: Bytes::from_static(&[0, 0, 0]),
            format: AudioFormat::F32 as i32,
            ..Default::default()
        })),
        ..Default::default()
    })
    .is_err());
    assert!(decode(DataBuffer {
        data_type: Some(DataType::Audio(AudioBuffer {
            samples: Bytes::from_static(&[0, 0]),
            format: AudioFormat::I16 as i32,
            ..Default::default()
        })),
        ..Default::default()
    })
    .is_err());
    assert!(decode(DataBuffer {
        data_type: Some(DataType::Video(VideoFrame {
            codec: 42,
            ..Default::default()
        })),
        ..Default::default()
    })
    .is_err());
    assert!(decode(DataBuffer {
        data_type: Some(DataType::Control(ControlMessage::default())),
        ..Default::default()
    })
    .is_err());
    assert!(decode_frame(Bytes::from_static(&[0xff, 0xff]), copies).is_err());
}
