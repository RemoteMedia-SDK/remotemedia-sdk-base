use super::*;
use remotemedia_types::{CopyCounts, CopyPolicy, SharedBytes};

fn cases() -> Vec<DataType> {
    vec![
        DataType::Binary(BinaryBuffer {
            data: vec![7; 16],
            ..Default::default()
        }),
        DataType::Tensor(TensorBuffer {
            data: vec![7; 16],
            shape: vec![16],
            dtype: 1,
            ..Default::default()
        }),
        DataType::Numpy(NumpyBuffer {
            data: vec![7; 16],
            shape: vec![16],
            dtype: "uint8".into(),
            strides: vec![1],
            c_contiguous: true,
            f_contiguous: true,
        }),
        DataType::Video(VideoFrame {
            pixel_data: vec![7; 16],
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
fn borrowed_and_outgoing_protobuf_copies_are_counted_and_strictly_rejected() {
    let context = CopyContext::new(CopyPolicy::AllowCopies, "webrtc-protobuf");
    let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "webrtc-protobuf-strict");
    for data in cases() {
        let buffer = DataBuffer {
            data_type: Some(data),
            ..Default::default()
        };
        assert!(data_buffer_to_runtime_data_with_context(&buffer, &strict).is_err());
        let runtime = data_buffer_to_runtime_data_with_context(&buffer, &context)
            .unwrap()
            .unwrap();
        assert_ne!(
            bytes(runtime.clone()).as_ptr(),
            raw(buffer.data_type.as_ref().unwrap()).as_ptr()
        );
        assert!(runtime_data_to_data_buffer_with_context(&runtime, &strict).is_err());
        let output = runtime_data_to_data_buffer_with_context(&runtime, &context).unwrap();
        assert_eq!(raw(output.data_type.as_ref().unwrap()), &[7; 16]);
    }
    for reason in [
        CopyReason::PortableSerialization,
        CopyReason::PortableDeserialization,
    ] {
        let copied = counts(&context, reason);
        assert_eq!(
            (copied.completed, copied.copied_bytes, copied.failed),
            (4, 64, 0)
        );
        let denied = counts(&strict, reason);
        assert_eq!(
            (denied.rejected, denied.rejected_bytes, denied.completed),
            (4, 64, 0)
        );
    }
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
    audio.samples.push(0);
    assert!(data_buffer_to_runtime_data_with_context(&buffer, &context)
        .unwrap()
        .is_none());
    let Some(DataType::Audio(audio)) = &mut buffer.data_type else {
        unreachable!()
    };
    audio.samples.pop();
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
fn oversized_tensor_dimensions_are_not_truncated_during_adoption() {
    let tensor = TensorBuffer {
        data: vec![1],
        shape: vec![u64::MAX],
        ..Default::default()
    };
    let data = DataBuffer {
        data_type: Some(DataType::Tensor(tensor)),
        ..Default::default()
    };
    assert!(data_buffer_into_runtime_data(data).is_none());
}
