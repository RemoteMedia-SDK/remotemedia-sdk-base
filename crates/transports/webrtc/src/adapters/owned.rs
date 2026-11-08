//! Retain decoded protobuf Bytes owners in runtime buffers. Owning protobuf
//! decode preserves wire ranges; native f32 audio still needs aligned conversion.
use super::*;

pub fn data_buffer_into_runtime_data(buffer: DataBuffer) -> Option<RuntimeData> {
    data_buffer_into_runtime_data_with_context(buffer, CopyContext::legacy())
        .expect("legacy policy permits audio materialization")
}

pub fn data_buffer_into_runtime_data_with_context(
    mut buffer: DataBuffer,
    context: &CopyContext,
) -> Result<Option<RuntimeData>, CopyRejected> {
    match buffer.data_type.take() {
        Some(
            data @ (DataType::Video(_)
            | DataType::Tensor(_)
            | DataType::Binary(_)
            | DataType::Numpy(_)),
        ) => Ok(decode_bytes(data, Some(now_micros()))),
        other => {
            buffer.data_type = other;
            data_buffer_to_runtime_data_with_context(&buffer, context)
        }
    }
}

pub(super) fn decode_bytes(data: DataType, arrival_ts_us: Option<u64>) -> Option<RuntimeData> {
    match data {
        DataType::Video(video) => Some(RuntimeData::Video {
            pixel_data: remotemedia_types::SharedBytes::from_owner(video.pixel_data),
            width: video.width,
            height: video.height,
            format: proto_to_pixel_format(video.format),
            frame_number: video.frame_number,
            timestamp_us: video.timestamp_us,
            codec: proto_to_video_codec(video.codec),
            is_keyframe: video.is_keyframe,
            stream_id: None,
            arrival_ts_us,
        }),
        DataType::Tensor(tensor) => Some(RuntimeData::Tensor {
            data: remotemedia_types::SharedBytes::from_owner(tensor.data),
            shape: tensor
                .shape
                .into_iter()
                .map(i32::try_from)
                .collect::<Result<_, _>>()
                .ok()?,
            dtype: tensor.dtype,
            metadata: None,
        }),
        DataType::Binary(binary) => Some(RuntimeData::Binary(
            remotemedia_types::SharedBytes::from_owner(binary.data),
        )),
        DataType::Numpy(numpy) => Some(RuntimeData::Numpy {
            data: remotemedia_types::SharedBytes::from_owner(numpy.data),
            shape: numpy
                .shape
                .into_iter()
                .map(usize::try_from)
                .collect::<Result<_, _>>()
                .ok()?,
            dtype: numpy.dtype,
            strides: numpy
                .strides
                .into_iter()
                .map(isize::try_from)
                .collect::<Result<_, _>>()
                .ok()?,
            c_contiguous: numpy.c_contiguous,
            f_contiguous: numpy.f_contiguous,
        }),
        _ => None,
    }
}
