//! Adapters for converting between RuntimeData and Protobuf types
//!
//! This module provides conversion functions between core's RuntimeData
//! and the gRPC Protobuf DataBuffer types.

use crate::generated::data_buffer::DataType;
use crate::generated::{
    AudioBuffer, AudioFormat, BatchHint, BinaryBuffer, CancelSpeculation, ControlMessage,
    DataBuffer, DeadlineWarning, FileBuffer, JsonData, NumpyBuffer, TensorBuffer, TextBuffer,
    VideoFrame,
};
use remotemedia_core::data::{split_text_str, tag_text_str, RuntimeData};
use remotemedia_core::transport::TransportData;

/// Get current timestamp in microseconds for arrival time stamping
pub fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}
/// Convert core RuntimeData to Protobuf DataBuffer
///
/// This portable adapter materializes owned protobuf bytes. It is a copying
/// transport boundary, even when the runtime payload has shared ownership.
pub fn runtime_data_to_data_buffer(data: &RuntimeData) -> DataBuffer {
    let mut metadata_map = std::collections::HashMap::new();
    let data_type = match data {
        RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            metadata,
            ..
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
            codec,
            frame_number,
            timestamp_us,
            is_keyframe,
            ..
        } => {
            DataType::Video(VideoFrame {
                pixel_data: pixel_data.to_vec(),
                width: *width,
                height: *height,
                format: *format as i32, // Convert PixelFormat enum to i32
                frame_number: *frame_number,
                timestamp_us: *timestamp_us,
                codec: codec.map(|c| c as i32).unwrap_or(0), // Convert VideoCodec to i32
                is_keyframe: *is_keyframe,
            })
        }
        RuntimeData::Tensor {
            data, shape, dtype, ..
        } => {
            DataType::Tensor(TensorBuffer {
                data: data.to_vec(),
                shape: shape.iter().map(|&s| s as u64).collect(),
                dtype: *dtype,
                layout: String::new(), // Empty layout - nodes document expected layout
            })
        }
        RuntimeData::Numpy {
            data,
            shape,
            dtype,
            strides,
            c_contiguous,
            f_contiguous,
        } => DataType::Numpy(NumpyBuffer {
            data: data.to_vec(),
            shape: shape.iter().map(|&s| s as u64).collect(),
            dtype: dtype.clone(),
            strides: strides.iter().map(|&s| s as i64).collect(),
            c_contiguous: *c_contiguous,
            f_contiguous: *f_contiguous,
        }),
        RuntimeData::DeviceTensor { .. } => return legacy_device_buffer(data),
        RuntimeData::Json(value) => {
            let mut val = value.clone();
            if let Some(obj) = val.as_object_mut() {
                let keys: Vec<String> = obj
                    .keys()
                    .filter(|k| k.starts_with("participant."))
                    .cloned()
                    .collect();
                for k in keys {
                    if let Some(serde_json::Value::String(s)) = obj.remove(&k) {
                        metadata_map.insert(k, s);
                    }
                }
            }
            DataType::Json(JsonData {
                json_payload: serde_json::to_string(&val).unwrap_or_default(),
                schema_type: String::new(),
            })
        }
        RuntimeData::Text(s) => {
            // Outbound: peel the routing channel out of the tagged
            // payload and ship it as a structured field; the wire
            // carries clean UTF-8 content.
            let (channel, content) = split_text_str(s);
            DataType::Text(TextBuffer {
                text_data: content.as_bytes().to_vec(),
                encoding: "utf-8".to_string(),
                language: String::new(),
                channel: channel.to_string(),
            })
        }
        RuntimeData::Binary(bytes) => DataType::Binary(BinaryBuffer {
            data: bytes.to_vec(),
            mime_type: "application/octet-stream".to_string(),
        }),
        // Image-over-gRPC fallback: ship as Binary with a mime-type
        // hint. A first-class `ImageBuffer` proto entry is a follow-up
        // spec; the typical multimodal-LLM path runs in-process or
        // WebRTC, not gRPC. The round-trip degrades to Binary on the
        // receiving side (format/width/height are lost).
        RuntimeData::Image { data, format, .. } => DataType::Binary(BinaryBuffer {
            data: data.to_vec(),
            mime_type: format
                .mime_type()
                .unwrap_or("application/octet-stream")
                .to_string(),
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
        metadata: metadata_map,
    }
}

/// A device tensor reaching the legacy (no copy authority) encoder: charge the
/// host transfer to the legacy context. A failed transfer is reported in band
/// as a JSON error rather than silently dropped.
fn legacy_device_buffer(data: &RuntimeData) -> DataBuffer {
    match data
        .clone()
        .device_to_host(remotemedia_core::data::CopyContext::legacy())
    {
        Ok(host) => runtime_data_to_data_buffer(&host),
        Err(error) => {
            tracing::error!(%error, "device tensor not encoded for gRPC");
            runtime_data_to_data_buffer(&RuntimeData::Json(
                serde_json::json!({ "error": format!("device tensor not encoded: {error}") }),
            ))
        }
    }
}

/// Encode for the gRPC wire under an explicit copy authority. Protobuf fields
/// own their bytes, so the visible bulk payload is a counted
/// `PortableSerialization`; strict policy rejects before encoding. A device
/// tensor first needs a counted `DeviceTransfer` to host, which strict policy
/// also rejects.
pub fn runtime_data_to_data_buffer_with_context(
    data: &RuntimeData,
    copies: &remotemedia_core::data::CopyContext,
) -> Result<DataBuffer, remotemedia_core::data::TransferError> {
    let host;
    let data = if data.is_device() {
        host = data.clone().device_to_host(copies)?;
        &host
    } else {
        data
    };
    Ok(copies.materialize(
        remotemedia_core::data::CopyReason::PortableSerialization,
        data.bulk_payload_bytes() as u64,
        || runtime_data_to_data_buffer(data),
    )?)
}

/// Decode an owned DataBuffer under an explicit copy authority.
///
/// Video, tensor, NumPy and binary bytes move into shared storage without a
/// copy. Audio arrives as little-endian bytes and must be converted to f32
/// samples; that conversion is a counted `PortableDeserialization`, rejected
/// under strict policy before it runs.
/// `participant.*` metadata for a [`crate::generated::ParticipantInfo`]
/// (empty fields are left out; an empty id yields no entries).
pub fn participant_metadata(
    info: &crate::generated::ParticipantInfo,
) -> std::collections::HashMap<String, String> {
    use remotemedia_core::transport::data::participant;
    let mut metadata = std::collections::HashMap::new();
    if info.id.is_empty() {
        return metadata;
    }
    for (key, value) in [
        (participant::ID, &info.id),
        (participant::ROLE, &info.role),
        (participant::TRACK_ID, &info.track_id),
        (participant::MODALITY, &info.modality),
        (participant::DISPLAY_NAME, &info.display_name),
    ] {
        if !value.is_empty() {
            metadata.insert(key.to_string(), value.clone());
        }
    }
    metadata
}

pub fn data_buffer_into_runtime_data_with_context(
    mut buffer: DataBuffer,
    arrival_ts_us: Option<u64>,
    copies: &remotemedia_core::data::CopyContext,
) -> Result<Option<RuntimeData>, remotemedia_core::data::CopyRejected> {
    let owned = match buffer.data_type.as_mut() {
        Some(DataType::Video(video)) => Some(std::mem::take(&mut video.pixel_data)),
        Some(DataType::Tensor(tensor)) => Some(std::mem::take(&mut tensor.data)),
        Some(DataType::Binary(binary)) => Some(std::mem::take(&mut binary.data)),
        Some(DataType::Numpy(numpy)) => Some(std::mem::take(&mut numpy.data)),
        _ => None,
    };
    let audio_bytes = match &buffer.data_type {
        Some(DataType::Audio(audio)) => audio.samples.len() as u64,
        _ => 0,
    };
    let mut decoded = copies.materialize(
        remotemedia_core::data::CopyReason::PortableDeserialization,
        audio_bytes,
        || data_buffer_to_runtime_data_with_arrival(&buffer, arrival_ts_us),
    )?;
    if let (Some(bytes), Some(data)) = (owned, decoded.as_mut()) {
        let shared: remotemedia_core::data::SharedBytes = bytes.into();
        match data {
            RuntimeData::Video { pixel_data, .. } => *pixel_data = shared,
            RuntimeData::Tensor { data, .. } => *data = shared,
            RuntimeData::Numpy { data, .. } => *data = shared,
            RuntimeData::Binary(data) => *data = shared,
            _ => {}
        }
    }
    // `DataBuffer.metadata` (e.g. `participant.*` on audio) reaches nodes
    // through the payload's own metadata where the payload has one.
    if !buffer.metadata.is_empty() {
        decoded = decoded.map(|data| {
            remotemedia_core::transport::data::apply_transport_metadata_to_runtime(data, &buffer.metadata)
        });
    }
    Ok(decoded)
}

/// Convert Protobuf DataBuffer to core RuntimeData
///
/// Note: For spec 026 drift monitoring, use `data_buffer_to_runtime_data_with_arrival()`
/// to stamp the arrival timestamp at transport ingest time.
pub fn data_buffer_to_runtime_data(buffer: &DataBuffer) -> Option<RuntimeData> {
    data_buffer_to_runtime_data_with_arrival(buffer, None)
}

/// Convert Protobuf DataBuffer to core RuntimeData with arrival timestamp stamping
///
/// # Arguments
/// * `buffer` - The protobuf DataBuffer to convert
/// * `stamp_arrival` - If true, stamps arrival_ts_us with current time (spec 026)
///
/// This should be called at the transport ingest boundary to ensure accurate
/// drift monitoring timestamps.
pub fn data_buffer_to_runtime_data_with_arrival(
    buffer: &DataBuffer,
    arrival_ts_us: Option<u64>,
) -> Option<RuntimeData> {
    // If caller wants arrival stamping but didn't provide a value, generate one now
    let arrival_ts = arrival_ts_us.or_else(|| None);

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
                arrival_ts_us: arrival_ts,
                metadata: audio
                    .metadata_json
                    .as_ref()
                    .and_then(|bytes| serde_json::from_slice(bytes).ok()),
            })
        }
        Some(DataType::Video(video)) => {
            use remotemedia_core::data::video::{PixelFormat, VideoCodec};

            // Convert i32 format to PixelFormat enum
            let format = match video.format {
                0 => PixelFormat::Unspecified,
                1 => PixelFormat::Rgb24,
                2 => PixelFormat::Rgba32,
                3 => PixelFormat::Yuv420p,
                4 => PixelFormat::Unspecified, // GRAY8 not mapped
                5 => PixelFormat::I420,
                6 => PixelFormat::NV12,
                255 => PixelFormat::Encoded,
                _ => PixelFormat::Unspecified,
            };

            // Convert i32 codec to VideoCodec enum
            let codec = match video.codec {
                0 => None, // Unspecified = raw frame
                1 => Some(VideoCodec::Vp8),
                2 => Some(VideoCodec::H264),
                3 => Some(VideoCodec::Av1),
                _ => None,
            };

            Some(RuntimeData::Video {
                pixel_data: video.pixel_data.clone().into(),
                width: video.width,
                height: video.height,
                format,
                codec,
                frame_number: video.frame_number,
                timestamp_us: video.timestamp_us,
                is_keyframe: video.is_keyframe,
                stream_id: None,
                arrival_ts_us: arrival_ts,
            })
        }
        Some(DataType::Tensor(tensor)) => Some(RuntimeData::Tensor {
            data: tensor.data.clone().into(),
            shape: tensor.shape.iter().map(|&s| s as i32).collect(),
            dtype: tensor.dtype,
            metadata: None,
        }),
        Some(DataType::Json(json)) => {
            let mut value: serde_json::Value = serde_json::from_str(&json.json_payload).ok()?;
            if let Some(obj) = value.as_object_mut() {
                for (k, v) in &buffer.metadata {
                    obj.insert(k.clone(), serde_json::Value::String(v.clone()));
                }
            }
            Some(RuntimeData::Json(value))
        }
        Some(DataType::Text(text)) => String::from_utf8(text.text_data.clone())
            .ok()
            .map(|content| RuntimeData::Text(tag_text_str(&content, &text.channel))),
        Some(DataType::Binary(bin)) => Some(RuntimeData::Binary(bin.data.clone().into())),
        Some(DataType::Numpy(numpy)) => Some(RuntimeData::Numpy {
            data: numpy.data.clone().into(),
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
/// Counted variant of [`transport_data_to_data_buffer`].
pub fn transport_data_to_data_buffer_with_context(
    data: &TransportData,
    copies: &remotemedia_core::data::CopyContext,
) -> Result<DataBuffer, remotemedia_core::data::TransferError> {
    if data.data.is_device() {
        let mut host = data.clone();
        host.data = data.data.clone().device_to_host(copies)?;
        return transport_data_to_data_buffer_with_context(&host, copies);
    }
    Ok(copies.materialize(
        remotemedia_core::data::CopyReason::PortableSerialization,
        data.data.bulk_payload_bytes() as u64,
        || transport_data_to_data_buffer(data),
    )?)
}

/// Owning, counted variant of [`data_buffer_to_transport_data`]: bulk bytes move
/// into shared storage; audio conversion is charged to `copies`.
pub fn data_buffer_into_transport_data_with_context(
    buffer: DataBuffer,
    copies: &remotemedia_core::data::CopyContext,
) -> Result<Option<TransportData>, remotemedia_core::data::CopyRejected> {
    let metadata = buffer.metadata.clone();
    let Some(runtime_data) = data_buffer_into_runtime_data_with_context(buffer, None, copies)?
    else {
        return Ok(None);
    };
    let mut transport_data = TransportData::new(runtime_data);
    if let Some(seq) = metadata.get("sequence").and_then(|s| s.parse::<u64>().ok()) {
        transport_data.sequence = Some(seq);
    }
    for (key, value) in metadata {
        if key != "sequence" {
            transport_data.metadata.insert(key, value);
        }
    }
    Ok(Some(transport_data))
}

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
                // Empty strings in protobuf become None
                assert_eq!(filename, None);
                assert_eq!(mime_type, None);
                // 0 values become None
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
            size: Some(1_073_741_824),      // 1 GB
            offset: Some(10 * 1024 * 1024), // 10 MB offset
            length: Some(64 * 1024),        // 64 KB chunk
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
    fn test_file_to_protobuf_structure() {
        let file = RuntimeData::File {
            path: "/test/path.txt".to_string(),
            filename: Some("path.txt".to_string()),
            mime_type: Some("text/plain".to_string()),
            size: Some(1024),
            offset: Some(0),
            length: Some(512),
            stream_id: Some("main".to_string()),
        };

        let buffer = runtime_data_to_data_buffer(&file);

        match buffer.data_type {
            Some(DataType::File(file_buf)) => {
                assert_eq!(file_buf.path, "/test/path.txt");
                assert_eq!(file_buf.filename, "path.txt");
                assert_eq!(file_buf.mime_type, "text/plain");
                assert_eq!(file_buf.size, 1024);
                assert_eq!(file_buf.offset, 0);
                assert_eq!(file_buf.length, 512);
                assert_eq!(file_buf.stream_id, "main");
            }
            _ => panic!("Expected DataType::File"),
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
            .insert("source".to_string(), "upload".to_string());

        let buffer = transport_data_to_data_buffer(&transport);
        let recovered = data_buffer_to_transport_data(&buffer).unwrap();

        assert_eq!(recovered.sequence, Some(42));
        assert_eq!(
            recovered.metadata.get("source"),
            Some(&"upload".to_string())
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

#[cfg(test)]
mod copy_authority_tests {
    use super::*;
    use remotemedia_core::data::{CopyContext, CopyPolicy, CopyReason};

    fn counts(copies: &CopyContext, reason: CopyReason) -> (u64, u64, u64) {
        copies
            .report()
            .entries
            .into_iter()
            .find(|(entry, _)| *entry == reason)
            .map(|(_, c)| (c.completed, c.copied_bytes, c.rejected))
            .unwrap_or_default()
    }

    #[test]
    fn owned_decode_moves_bulk_bytes_and_counts_audio_conversion() {
        let copies = CopyContext::new(CopyPolicy::AllowCopies, "grpc-adapter-test");
        let bytes = vec![7_u8; 4096];
        let original = bytes.as_ptr();
        let buffer = DataBuffer {
            data_type: Some(DataType::Binary(BinaryBuffer {
                data: bytes,
                mime_type: "application/octet-stream".into(),
            })),
            metadata: Default::default(),
        };
        let decoded = data_buffer_into_runtime_data_with_context(buffer, None, &copies)
            .unwrap()
            .unwrap();
        let RuntimeData::Binary(shared) = &decoded else {
            panic!("expected binary")
        };
        assert_eq!(shared.as_ptr(), original, "owned decode must not copy");
        assert_eq!(counts(&copies, CopyReason::PortableDeserialization).1, 0);

        let audio = RuntimeData::Audio {
            samples: vec![0.5_f32; 16].into(),
            sample_rate: 16_000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        };
        let wire = runtime_data_to_data_buffer_with_context(&audio, &copies).unwrap();
        assert_eq!(
            counts(&copies, CopyReason::PortableSerialization),
            (1, 64, 0)
        );
        let back = data_buffer_into_runtime_data_with_context(wire, None, &copies)
            .unwrap()
            .unwrap();
        assert!(matches!(back, RuntimeData::Audio { ref samples, .. } if samples.len() == 16));
        assert_eq!(
            counts(&copies, CopyReason::PortableDeserialization),
            (1, 64, 0)
        );
    }

    #[test]
    fn strict_ledgers_reject_wire_copies_before_converting() {
        let strict = CopyContext::new(CopyPolicy::RequireZeroCopy, "grpc-adapter-strict");
        let binary = RuntimeData::Binary(vec![1_u8; 8].into());
        assert!(runtime_data_to_data_buffer_with_context(&binary, &strict).is_err());
        let wire = runtime_data_to_data_buffer(&RuntimeData::Audio {
            samples: vec![0.25_f32; 4].into(),
            sample_rate: 8_000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        });
        assert!(data_buffer_into_runtime_data_with_context(wire, None, &strict).is_err());
        let (_, _, rejected) = counts(&strict, CopyReason::PortableSerialization);
        assert_eq!(rejected, 1);
        // Metadata-only payloads need no copy and pass under strict.
        let text =
            runtime_data_to_data_buffer_with_context(&RuntimeData::Text("hi".into()), &strict);
        assert!(text.is_ok());
    }
}
