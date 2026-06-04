//! Adapters for converting between RuntimeData and Protobuf types
//!
//! This module provides conversion functions between core's RuntimeData
//! and the gRPC Protobuf DataBuffer types.

// Phase 4/5 adapter infrastructure - some functions for future integration
#![allow(dead_code)]

use crate::generated::data_buffer::DataType;
use crate::generated::{
    AudioBuffer, AudioFormat, BatchHint, BinaryBuffer, CancelSpeculation, ControlMessage,
    DataBuffer, DeadlineWarning, FileBuffer, JsonData, NumpyBuffer,
    PixelFormat as ProtoPixelFormat, TensorBuffer, TextBuffer, VideoCodec as ProtoVideoCodec,
    VideoFrame,
};
use remotemedia_core::data::{split_text_str, PixelFormat, RuntimeData, VideoCodec};
use remotemedia_core::transport::TransportData;

/// Convert core PixelFormat to protobuf PixelFormat
fn pixel_format_to_proto(format: PixelFormat) -> i32 {
    match format {
        PixelFormat::Unspecified => ProtoPixelFormat::Unspecified as i32,
        PixelFormat::Yuv420p => ProtoPixelFormat::Yuv420p as i32,
        PixelFormat::I420 => ProtoPixelFormat::I420 as i32,
        PixelFormat::NV12 => ProtoPixelFormat::Nv12 as i32,
        PixelFormat::Rgb24 => ProtoPixelFormat::Rgb24 as i32,
        PixelFormat::Rgba32 => ProtoPixelFormat::Rgba32 as i32,
        PixelFormat::Encoded => ProtoPixelFormat::Encoded as i32,
    }
}

/// Convert protobuf PixelFormat (i32) to core PixelFormat
fn proto_to_pixel_format(format: i32) -> PixelFormat {
    match ProtoPixelFormat::try_from(format) {
        Ok(ProtoPixelFormat::Unspecified) => PixelFormat::Unspecified,
        Ok(ProtoPixelFormat::Rgb24) => PixelFormat::Rgb24,
        Ok(ProtoPixelFormat::Rgba32) => PixelFormat::Rgba32,
        Ok(ProtoPixelFormat::Yuv420p) => PixelFormat::Yuv420p,
        Ok(ProtoPixelFormat::Gray8) => PixelFormat::Unspecified, // Map to Unspecified (not in core)
        Ok(ProtoPixelFormat::I420) => PixelFormat::I420,
        Ok(ProtoPixelFormat::Nv12) => PixelFormat::NV12,
        Ok(ProtoPixelFormat::Encoded) => PixelFormat::Encoded,
        _ => PixelFormat::Unspecified,
    }
}

/// Convert core VideoCodec to protobuf VideoCodec
fn video_codec_to_proto(codec: Option<VideoCodec>) -> i32 {
    match codec {
        None => ProtoVideoCodec::Unspecified as i32,
        Some(VideoCodec::Vp8) => ProtoVideoCodec::Vp8 as i32,
        Some(VideoCodec::H264) => ProtoVideoCodec::H264 as i32,
        Some(VideoCodec::Av1) => ProtoVideoCodec::Av1 as i32,
    }
}

/// Convert protobuf VideoCodec (i32) to core VideoCodec
fn proto_to_video_codec(codec: i32) -> Option<VideoCodec> {
    match ProtoVideoCodec::try_from(codec) {
        Ok(ProtoVideoCodec::Unspecified) => None,
        Ok(ProtoVideoCodec::Vp8) => Some(VideoCodec::Vp8),
        Ok(ProtoVideoCodec::H264) => Some(VideoCodec::H264),
        Ok(ProtoVideoCodec::Av1) => Some(VideoCodec::Av1),
        _ => None,
    }
}

/// Convert core RuntimeData to Protobuf DataBuffer
pub fn runtime_data_to_data_buffer(data: &RuntimeData) -> DataBuffer {
    let data_type = match data {
        RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            metadata,
            stream_id: _,     // stream_id not included in protobuf (yet)
            timestamp_us: _,  // spec 026: not included in protobuf
            arrival_ts_us: _, // spec 026: not included in protobuf
        } => DataType::Audio(AudioBuffer {
            samples: samples.iter().flat_map(|f| f.to_le_bytes()).collect(),
            sample_rate: *sample_rate,
            channels: *channels,
            format: AudioFormat::F32 as i32,
            num_samples: samples.len() as u64,
            metadata_json: metadata
                .as_ref()
                .map(|m| serde_json::to_vec(m).unwrap_or_default()),
        }),
        RuntimeData::Video {
            pixel_data,
            width,
            height,
            format,
            frame_number,
            timestamp_us,
            codec,
            is_keyframe,
            stream_id: _,     // stream_id not included in protobuf (yet)
            arrival_ts_us: _, // spec 026: not included in protobuf
        } => DataType::Video(VideoFrame {
            pixel_data: pixel_data.clone(),
            width: *width,
            height: *height,
            format: pixel_format_to_proto(*format),
            frame_number: *frame_number,
            timestamp_us: *timestamp_us,
            codec: video_codec_to_proto(*codec),
            is_keyframe: *is_keyframe,
        }),
        RuntimeData::Tensor {
            data, shape, dtype, ..
        } => {
            DataType::Tensor(TensorBuffer {
                data: data.clone(),
                shape: shape.iter().map(|&s| s as u64).collect(),
                dtype: *dtype,
                layout: String::new(), // Empty layout - nodes document expected layout
            })
        }
        RuntimeData::Json(value) => DataType::Json(JsonData {
            json_payload: serde_json::to_string(value).unwrap_or_default(),
            schema_type: String::new(),
        }),
        RuntimeData::Text(s) => {
            // Strip the `\x00<len><channel>` routing header and
            // forward the channel as a structured protobuf field.
            // The browser / any gRPC consumer gets clean UTF-8 plus an
            // explicit `channel` it can dispatch on.
            let (channel, content) = split_text_str(s);
            DataType::Text(TextBuffer {
                text_data: content.as_bytes().to_vec(),
                encoding: "utf-8".to_string(),
                language: String::new(),
                channel: channel.to_string(),
            })
        }
        RuntimeData::Binary(bytes) => DataType::Binary(BinaryBuffer {
            data: bytes.clone(),
            mime_type: "application/octet-stream".to_string(),
        }),
        // Image-over-WebRTC fallback: ship as Binary with a mime-type
        // hint. A first-class `ImageBuffer` proto entry is a follow-up
        // spec; vision-LLM pipelines today run image input in-process.
        RuntimeData::Image { data, format, .. } => DataType::Binary(BinaryBuffer {
            data: data.clone(),
            mime_type: format
                .mime_type()
                .unwrap_or("application/octet-stream")
                .to_string(),
        }),
        RuntimeData::Numpy {
            data,
            shape,
            dtype,
            strides,
            c_contiguous,
            f_contiguous,
        } => DataType::Numpy(NumpyBuffer {
            data: data.clone(),
            shape: shape.iter().map(|&s| s as u64).collect(),
            dtype: dtype.clone(),
            strides: strides.iter().map(|&s| s as i64).collect(),
            c_contiguous: *c_contiguous,
            f_contiguous: *f_contiguous,
        }),
        RuntimeData::ControlMessage {
            message_type,
            segment_id,
            timestamp_ms,
            metadata,
        } => {
            use crate::generated::control_message::MessageType;
            use remotemedia_core::data::ControlMessageType;

            // Convert ControlMessageType to protobuf oneof
            let proto_message_type = match message_type {
                ControlMessageType::CancelSpeculation {
                    from_timestamp,
                    to_timestamp,
                } => MessageType::CancelSpeculation(CancelSpeculation {
                    from_timestamp: *from_timestamp,
                    to_timestamp: *to_timestamp,
                }),
                ControlMessageType::BatchHint {
                    suggested_batch_size,
                } => MessageType::BatchHint(BatchHint {
                    suggested_batch_size: *suggested_batch_size as u32,
                }),
                ControlMessageType::DeadlineWarning { deadline_us } => {
                    MessageType::DeadlineWarning(DeadlineWarning {
                        deadline_us: *deadline_us,
                    })
                }
            };

            DataType::Control(ControlMessage {
                message_type: Some(proto_message_type),
                segment_id: segment_id.clone().unwrap_or_default(),
                timestamp_ms: *timestamp_ms,
                metadata: serde_json::to_string(metadata).unwrap_or_default(),
            })
        }
        RuntimeData::File {
            path,
            filename,
            mime_type,
            size,
            offset,
            length,
            stream_id,
        } => DataType::File(FileBuffer {
            path: path.clone(),
            filename: filename.clone().unwrap_or_default(),
            mime_type: mime_type.clone().unwrap_or_default(),
            size: size.unwrap_or(0),
            offset: offset.unwrap_or(0),
            length: length.unwrap_or(0),
            stream_id: stream_id.clone().unwrap_or_default(),
        }),
    };

    DataBuffer {
        data_type: Some(data_type),
        metadata: std::collections::HashMap::new(),
    }
}

/// Get current timestamp in microseconds for arrival time stamping (spec 026)
fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

/// Convert Protobuf DataBuffer to core RuntimeData
///
/// Note: arrival_ts_us is stamped at conversion time per spec 026
pub fn data_buffer_to_runtime_data(buffer: &DataBuffer) -> Option<RuntimeData> {
    // Stamp arrival time at transport ingest (spec 026)
    let arrival_ts_us = Some(now_micros());

    match &buffer.data_type {
        Some(DataType::Audio(audio)) => {
            // Convert bytes back to f32 samples
            let samples: Vec<f32> = audio
                .samples
                .chunks_exact(4)
                .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                .collect();

            Some(RuntimeData::Audio {
                samples: samples.into(),
                sample_rate: audio.sample_rate,
                channels: audio.channels,
                stream_id: None,
                timestamp_us: None,
                arrival_ts_us,
                metadata: audio
                    .metadata_json
                    .as_ref()
                    .and_then(|bytes| serde_json::from_slice(bytes).ok()),
            })
        }
        Some(DataType::Video(video)) => Some(RuntimeData::Video {
            pixel_data: video.pixel_data.clone(),
            width: video.width,
            height: video.height,
            format: proto_to_pixel_format(video.format),
            frame_number: video.frame_number,
            timestamp_us: video.timestamp_us,
            codec: proto_to_video_codec(video.codec),
            is_keyframe: video.is_keyframe,
            stream_id: None, // stream_id not in protobuf (yet)
            arrival_ts_us,
        }),
        Some(DataType::Tensor(tensor)) => Some(RuntimeData::Tensor {
            data: tensor.data.clone(),
            shape: tensor.shape.iter().map(|&s| s as i32).collect(),
            dtype: tensor.dtype,
            metadata: None,
        }),
        Some(DataType::Json(json)) => serde_json::from_str(&json.json_payload)
            .ok()
            .map(RuntimeData::Json),
        Some(DataType::Text(text)) => String::from_utf8(text.text_data.clone())
            .ok()
            .map(RuntimeData::Text),
        Some(DataType::Binary(bin)) => Some(RuntimeData::Binary(bin.data.clone())),
        Some(DataType::Numpy(numpy)) => Some(RuntimeData::Numpy {
            data: numpy.data.clone(),
            shape: numpy.shape.iter().map(|&s| s as usize).collect(),
            dtype: numpy.dtype.clone(),
            strides: numpy.strides.iter().map(|&s| s as isize).collect(),
            c_contiguous: numpy.c_contiguous,
            f_contiguous: numpy.f_contiguous,
        }),
        Some(DataType::File(file)) => Some(RuntimeData::File {
            path: file.path.clone(),
            filename: if file.filename.is_empty() {
                None
            } else {
                Some(file.filename.clone())
            },
            mime_type: if file.mime_type.is_empty() {
                None
            } else {
                Some(file.mime_type.clone())
            },
            size: if file.size == 0 {
                None
            } else {
                Some(file.size)
            },
            offset: if file.offset == 0 {
                None
            } else {
                Some(file.offset)
            },
            length: if file.length == 0 {
                None
            } else {
                Some(file.length)
            },
            stream_id: if file.stream_id.is_empty() {
                None
            } else {
                Some(file.stream_id.clone())
            },
        }),
        _ => None,
    }
}

/// Convert TransportData to Protobuf DataBuffer
pub fn transport_data_to_data_buffer(data: &TransportData) -> DataBuffer {
    let mut buffer = runtime_data_to_data_buffer(&data.data);

    // Add sequence number to metadata if present
    if let Some(seq) = data.sequence {
        buffer
            .metadata
            .insert("sequence".to_string(), seq.to_string());
    }

    // Add transport metadata
    for (key, value) in &data.metadata {
        buffer.metadata.insert(key.clone(), value.clone());
    }

    buffer
}

/// Convert Protobuf DataBuffer to TransportData
pub fn data_buffer_to_transport_data(buffer: &DataBuffer) -> Option<TransportData> {
    let runtime_data = data_buffer_to_runtime_data(buffer)?;
    let mut transport_data = TransportData::new(runtime_data);

    // Extract sequence number from metadata if present
    if let Some(seq_str) = buffer.metadata.get("sequence") {
        if let Ok(seq) = seq_str.parse::<u64>() {
            transport_data.sequence = Some(seq);
        }
    }

    // Copy metadata
    for (key, value) in &buffer.metadata {
        if key != "sequence" {
            // Don't duplicate sequence in metadata
            transport_data.metadata.insert(key.clone(), value.clone());
        }
    }

    Some(transport_data)
}

#[cfg(test)]
mod tests {
    use super::*;

    // File adapter roundtrip tests (Spec 001: RuntimeData.File)

    #[test]
    fn test_file_roundtrip_with_all_fields() {
        let file = RuntimeData::File {
            path: "/data/input/video.mp4".to_string(),
            filename: Some("video.mp4".to_string()),
            mime_type: Some("video/mp4".to_string()),
            size: Some(104_857_600),
            offset: Some(1_048_576),
            length: Some(65_536),
            stream_id: Some("video_track".to_string()),
        };

        let buffer = runtime_data_to_data_buffer(&file);
        let recovered = data_buffer_to_runtime_data(&buffer).unwrap();

        match recovered {
            RuntimeData::File {
                path,
                filename,
                mime_type,
                size,
                offset,
                length,
                stream_id,
            } => {
                assert_eq!(path, "/data/input/video.mp4");
                assert_eq!(filename, Some("video.mp4".to_string()));
                assert_eq!(mime_type, Some("video/mp4".to_string()));
                assert_eq!(size, Some(104_857_600));
                assert_eq!(offset, Some(1_048_576));
                assert_eq!(length, Some(65_536));
                assert_eq!(stream_id, Some("video_track".to_string()));
            }
            _ => panic!("Expected RuntimeData::File"),
        }
    }

    #[test]
    fn test_file_roundtrip_minimal() {
        let file = RuntimeData::File {
            path: "/tmp/output.bin".to_string(),
            filename: None,
            mime_type: None,
            size: None,
            offset: None,
            length: None,
            stream_id: None,
        };

        let buffer = runtime_data_to_data_buffer(&file);
        let recovered = data_buffer_to_runtime_data(&buffer).unwrap();

        match recovered {
            RuntimeData::File {
                path,
                filename,
                mime_type,
                size,
                offset,
                length,
                stream_id,
            } => {
                assert_eq!(path, "/tmp/output.bin");
                assert_eq!(filename, None);
                assert_eq!(mime_type, None);
                assert_eq!(size, None);
                assert_eq!(offset, None);
                assert_eq!(length, None);
                assert_eq!(stream_id, None);
            }
            _ => panic!("Expected RuntimeData::File"),
        }
    }

    #[test]
    fn test_file_roundtrip_byte_range() {
        let file = RuntimeData::File {
            path: "/data/large_file.bin".to_string(),
            filename: None,
            mime_type: None,
            size: Some(1_073_741_824),
            offset: Some(10 * 1024 * 1024),
            length: Some(64 * 1024),
            stream_id: None,
        };

        let buffer = runtime_data_to_data_buffer(&file);
        let recovered = data_buffer_to_runtime_data(&buffer).unwrap();

        match recovered {
            RuntimeData::File {
                path,
                size,
                offset,
                length,
                ..
            } => {
                assert_eq!(path, "/data/large_file.bin");
                assert_eq!(size, Some(1_073_741_824));
                assert_eq!(offset, Some(10 * 1024 * 1024));
                assert_eq!(length, Some(64 * 1024));
            }
            _ => panic!("Expected RuntimeData::File"),
        }
    }

    #[test]
    fn test_file_transport_data_roundtrip() {
        let file = RuntimeData::File {
            path: "/data/video.mp4".to_string(),
            filename: Some("video.mp4".to_string()),
            mime_type: Some("video/mp4".to_string()),
            size: Some(50_000_000),
            offset: None,
            length: None,
            stream_id: None,
        };

        let mut transport = TransportData::new(file);
        transport.sequence = Some(42);
        transport
            .metadata
            .insert("source".to_string(), "webrtc".to_string());

        let buffer = transport_data_to_data_buffer(&transport);
        let recovered = data_buffer_to_transport_data(&buffer).unwrap();

        assert_eq!(recovered.sequence, Some(42));
        assert_eq!(
            recovered.metadata.get("source"),
            Some(&"webrtc".to_string())
        );

        match recovered.data {
            RuntimeData::File { path, size, .. } => {
                assert_eq!(path, "/data/video.mp4");
                assert_eq!(size, Some(50_000_000));
            }
            _ => panic!("Expected RuntimeData::File"),
        }
    }
}
