use super::*;
use remotemedia_types::{CopyCounts, CopyPolicy, SharedBytes};

pub(super) fn cases() -> Vec<DataType> {
    vec![
        DataType::Binary(BinaryBuffer {
            data: vec![7; 16].into(),
            ..Default::default()
        }),
        DataType::Tensor(TensorBuffer {
            data: vec![7; 16].into(),
            shape: vec![16],
            dtype: 1,
            ..Default::default()
        }),
        DataType::Numpy(NumpyBuffer {
            data: vec![7; 16].into(),
            shape: vec![16],
            dtype: "uint8".into(),
            strides: vec![1],
            c_contiguous: true,
            f_contiguous: true,
        }),
        DataType::Video(VideoFrame {
            pixel_data: vec![7; 16].into(),
            width: 2,
            height: 2,
            format: ProtoPixelFormat::Rgba32 as i32,
            frame_number: 12,
            timestamp_us: 34,
            ..Default::default()
        }),
    ]
}
fn raw(data: &DataType) -> &[u8] {
    match data {
        DataType::Binary(b) => &b.data,
        DataType::Tensor(t) => &t.data,
        DataType::Numpy(n) => &n.data,
        DataType::Video(v) => &v.pixel_data,
        _ => unreachable!(),
    }
}
fn bytes(data: RuntimeData) -> SharedBytes {
    match data {
        RuntimeData::Binary(data)
        | RuntimeData::Tensor { data, .. }
        | RuntimeData::Numpy { data, .. } => data,
        RuntimeData::Video {
            pixel_data,
            frame_number,
            timestamp_us,
            arrival_ts_us,
            ..
        } => {
            assert_eq!((frame_number, timestamp_us), (12, 34));
            assert!(arrival_ts_us.is_some());
            pixel_data
        }
        _ => unreachable!(),
    }
}
fn counts(context: &CopyContext, reason: CopyReason) -> CopyCounts {
    context
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == reason)
        .unwrap()
        .1
}

#[test]
fn consuming_protobuf_bytes_preserve_pointer_and_retained_slices_in_strict_mode() {
    let context = CopyContext::new(CopyPolicy::RequireZeroCopy, "webrtc-decoded-owner");
    for data in cases() {
        let pointer = raw(&data).as_ptr();
        let buffer = DataBuffer {
            data_type: Some(data),
            ..Default::default()
        };
        let owner = bytes(
            data_buffer_into_runtime_data_with_context(buffer, &context)
                .unwrap()
                .unwrap(),
        );
        assert_eq!(owner.as_ptr(), pointer);
        let slice = owner.slice(2..6).unwrap();
        drop(owner);
        assert_eq!(slice.as_ptr(), pointer.wrapping_add(2));
        assert_eq!(&*slice, &[7; 4]);
    }
    assert!(context
        .report()
        .entries
        .iter()
        .all(|(_, counts)| *counts == CopyCounts::default()));
}

#[test]
fn borrowed_and_outgoing_protobuf_fields_retain_owners_in_strict_mode() {
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "webrtc-protobuf-strict");
    for data in cases() {
        let buffer = DataBuffer {
            data_type: Some(data),
            ..Default::default()
        };
        let pointer = raw(buffer.data_type.as_ref().unwrap()).as_ptr();
        let runtime = data_buffer_to_runtime_data_with_context(&buffer, &strict)
            .unwrap()
            .unwrap();
        assert_eq!(bytes(runtime.clone()).as_ptr(), pointer);
        let output = runtime_data_to_data_buffer_with_context(&runtime, &strict).unwrap();
        drop((buffer, runtime));
        assert_eq!(raw(output.data_type.as_ref().unwrap()).as_ptr(), pointer);
        assert_eq!(raw(output.data_type.as_ref().unwrap()), &[7; 16]);
    }
    assert!(strict
        .report()
        .entries
        .iter()
        .all(|(_, c)| *c == CopyCounts::default()));
}

#[test]
fn owned_audio_conversion_counts_samples_and_rejects_malformed_payloads() {
    let context = CopyContext::new(CopyPolicy::AllowCopies, "webrtc-audio");
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "webrtc-audio-strict");
    let audio = AudioBuffer {
        samples: [0.25f32, -0.5]
            .into_iter()
            .flat_map(f32::to_le_bytes)
            .collect(),
        sample_rate: 48000,
        channels: 1,
        num_samples: 2,
        format: AudioFormat::F32 as i32,
        ..Default::default()
    };
    let mut buffer = DataBuffer {
        data_type: Some(DataType::Audio(audio)),
        ..Default::default()
    };
    assert!(data_buffer_into_runtime_data_with_context(buffer.clone(), &strict).is_err());
    let RuntimeData::Audio { samples, .. } =
        data_buffer_into_runtime_data_with_context(buffer.clone(), &context)
            .unwrap()
            .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(samples.as_slice(), &[0.25, -0.5]);
    assert_eq!(
        counts(&context, CopyReason::PortableDeserialization).copied_bytes,
        8
    );
    let Some(DataType::Audio(audio)) = &mut buffer.data_type else {
        unreachable!()
    };
    let valid_samples = audio.samples.clone();
    let mut malformed = valid_samples.to_vec();
    malformed.push(0);
    audio.samples = malformed.into();
    assert!(data_buffer_to_runtime_data_with_context(&buffer, &context)
        .unwrap()
        .is_none());
    let Some(DataType::Audio(audio)) = &mut buffer.data_type else {
        unreachable!()
    };
    audio.samples = valid_samples;
    audio.format = i32::MAX;
    assert!(data_buffer_to_runtime_data_with_context(&buffer, &context)
        .unwrap()
        .is_none());
    assert_eq!(
        counts(&context, CopyReason::PortableDeserialization).copied_bytes,
        8
    );
}

#[test]
fn all_bulk_protobuf_fields_preserve_common_proto_wire_bytes() {
    use prost::Message;
    let payload = bytes::Bytes::from_static(&[1, 2, 3, 4]);
    for (data, tag) in [
        (
            DataType::Audio(AudioBuffer {
                samples: payload.clone(),
                ..Default::default()
            }),
            0x0a,
        ),
        (
            DataType::Video(VideoFrame {
                pixel_data: payload.clone(),
                ..Default::default()
            }),
            0x12,
        ),
        (
            DataType::Tensor(TensorBuffer {
                data: payload.clone(),
                ..Default::default()
            }),
            0x1a,
        ),
        (
            DataType::Binary(BinaryBuffer {
                data: payload.clone(),
                ..Default::default()
            }),
            0x32,
        ),
        (
            DataType::Numpy(NumpyBuffer {
                data: payload.clone(),
                ..Default::default()
            }),
            0x42,
        ),
    ] {
        let buffer = DataBuffer {
            data_type: Some(data),
            ..Default::default()
        };
        let wire = [tag, 6, 0x0a, 4, 1, 2, 3, 4];
        assert_eq!(buffer.encode_to_vec(), wire);
        assert_eq!(DataBuffer::decode(wire.as_slice()).unwrap(), buffer);
        assert_eq!(
            DataBuffer::decode(bytes::Bytes::copy_from_slice(&wire)).unwrap(),
            buffer
        );
    }
}

#[test]
fn protobuf_wire_decode_retains_budget_through_runtime_and_outgoing_fields() {
    use prost::Message;
    use remotemedia_types::BufferBudget;
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "wire-owner");
    for data in cases() {
        let encoded = DataBuffer {
            data_type: Some(data),
            ..Default::default()
        }
        .encode_to_vec();
        let budget = BufferBudget::new(1, encoded.len() as u64);
        let RuntimeData::Binary(wire) = budget.admit(RuntimeData::Binary(encoded.into())).unwrap()
        else {
            unreachable!()
        };
        let start = wire.as_ptr() as usize;
        let end = start + wire.len();
        let decoded = DataBuffer::decode(bytes::Bytes::from_owner(wire)).unwrap();
        let pointer = raw(decoded.data_type.as_ref().unwrap()).as_ptr();
        assert!((pointer as usize) >= start && (pointer as usize) + 16 <= end);
        let runtime = data_buffer_into_runtime_data_with_context(decoded, &strict)
            .unwrap()
            .unwrap();
        let outgoing = runtime_data_to_data_buffer_with_context(&runtime, &strict).unwrap();
        assert_eq!(raw(outgoing.data_type.as_ref().unwrap()).as_ptr(), pointer);
        let view = bytes(runtime).slice(3..7).unwrap();
        drop(outgoing);
        assert_eq!(view.as_ptr(), pointer.wrapping_add(3));
        assert_eq!(&*view, &[7; 4]);
        assert_eq!(budget.usage().owners, 1);
        std::thread::spawn(move || drop(view)).join().unwrap();
        assert_eq!(budget.usage().owners, 0);
    }
    assert!(strict
        .report()
        .entries
        .iter()
        .all(|(_, c)| *c == CopyCounts::default()));
}

#[test]
fn outgoing_audio_conversion_is_counted_and_strictly_rejected() {
    let context = CopyContext::new(CopyPolicy::AllowCopies, "audio-encode");
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "audio-encode-strict");
    let runtime = RuntimeData::Audio {
        samples: vec![0.25, -0.5].into(),
        sample_rate: 48000,
        channels: 1,
        stream_id: None,
        timestamp_us: None,
        arrival_ts_us: None,
        metadata: None,
    };
    assert_eq!(
        runtime_data_to_data_buffer_with_context(&runtime, &strict)
            .unwrap_err()
            .bytes,
        8
    );
    let encoded = runtime_data_to_data_buffer_with_context(&runtime, &context).unwrap();
    let Some(DataType::Audio(audio)) = encoded.data_type else {
        unreachable!()
    };
    assert_eq!(audio.samples.as_ref(), &[0, 0, 128, 62, 0, 0, 0, 191]);
    assert_eq!(
        counts(&context, CopyReason::PortableSerialization).copied_bytes,
        8
    );
    assert_eq!(
        counts(&strict, CopyReason::PortableSerialization).rejected_bytes,
        8
    );
}

#[test]
fn oversized_tensor_dimensions_are_not_truncated_during_adoption() {
    let tensor = TensorBuffer {
        data: vec![1].into(),
        shape: vec![u64::MAX],
        ..Default::default()
    };
    let data = DataBuffer {
        data_type: Some(DataType::Tensor(tensor)),
        ..Default::default()
    };
    assert!(data_buffer_into_runtime_data(data).is_none());
}
