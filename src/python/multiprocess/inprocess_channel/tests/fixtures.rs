use remotemedia_types::RuntimeData;

pub(super) fn bulk_cases() -> [RuntimeData; 6] {
    use remotemedia_types::{ImageFormat, PixelFormat};
    [
        RuntimeData::Audio {
            samples: vec![0.25; 4].into(),
            sample_rate: 16000,
            channels: 1,
            timestamp_us: Some(123),
            arrival_ts_us: None,
            stream_id: None,
            metadata: None,
        },
        RuntimeData::Binary(vec![1u8, 2, 3].into()),
        RuntimeData::Video {
            pixel_data: vec![0u8; 6].into(),
            width: 2,
            height: 1,
            format: PixelFormat::Rgb24,
            codec: None,
            frame_number: 3,
            is_keyframe: true,
            timestamp_us: 100,
            stream_id: None,
            arrival_ts_us: None,
        },
        RuntimeData::Image {
            data: vec![0u8; 6].into(),
            format: ImageFormat::Raw {
                pixel_format: PixelFormat::Rgb24,
            },
            width: 2,
            height: 1,
            timestamp_us: None,
            stream_id: None,
            metadata: None,
        },
        RuntimeData::Tensor {
            data: vec![0u8; 16].into(),
            shape: vec![2, 2],
            dtype: 1,
            metadata: None,
        },
        RuntimeData::Numpy {
            data: vec![0u8; 16].into(),
            shape: vec![2, 2],
            dtype: "float32".into(),
            strides: vec![8, 4],
            c_contiguous: true,
            f_contiguous: false,
        },
    ]
}
