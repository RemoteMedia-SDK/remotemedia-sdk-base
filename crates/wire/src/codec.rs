//! `RuntimeData` <-> `DataBuffer`, lossless in both directions.
use crate::generated::control_message::MessageType;
use crate::generated::data_buffer::DataType;
use crate::generated::{
    AudioBuffer, AudioFormat, BatchHint, BinaryBuffer, CancelSpeculation, ControlMessage,
    DataBuffer, DeadlineWarning, FileBuffer, ImageBuffer, ImageEncoding, JsonData, NumpyBuffer,
    PixelFormat as ProtoPixelFormat, TensorBuffer, TextBuffer, VideoCodec as ProtoVideoCodec,
    VideoFrame,
};
use bytes::Bytes;
use prost::encoding::{encode_key, encode_varint, encoded_len_varint, key_len, WireType};
use prost::Message;
use remotemedia_types::{
    split_text_str, tag_text_str, ControlMessageType, CopyContext, CopyReason, CopyRejected,
    ImageFormat, PixelFormat, RuntimeData, SharedBytes, TransferError, VideoCodec,
    TEXT_CHANNEL_DEFAULT,
};
use std::collections::HashMap;

/// One frame as it crosses the wire: the payload and its transport envelope.
#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    pub data: RuntimeData,
    pub metadata: HashMap<String, String>,
    pub sequence: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error(transparent)]
    Rejected(#[from] CopyRejected),
    #[error("device tensor could not be copied to host memory: {0}")]
    Device(String),
    #[error("invalid DataBuffer: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("malformed DataBuffer: {0}")]
    Malformed(String),
}

fn malformed(what: impl Into<String>) -> WireError {
    WireError::Malformed(what.into())
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

enum Payload<'a> {
    /// Audio samples are written straight into the wire buffer as f32 LE,
    /// without an intermediate byte vector.
    Audio {
        samples: &'a [f32],
        header: AudioBuffer,
    },
    Other(DataBuffer),
}

/// A frame ready to be written: policy checked, sizes known. Reserve
/// [`encoded_len`](Self::encoded_len) bytes, then [`append_to`](Self::append_to).
pub struct PreparedFrame<'a> {
    payload: Payload<'a>,
    /// `metadata` and `sequence`, written after the payload (protobuf merges
    /// fields of a message spread over the buffer).
    envelope: DataBuffer,
    bulk_bytes: usize,
    copies: &'a CopyContext,
}

impl<'a> PreparedFrame<'a> {
    pub fn new(
        data: &'a RuntimeData,
        metadata: &HashMap<String, String>,
        sequence: Option<u64>,
        copies: &'a CopyContext,
    ) -> Result<Self, WireError> {
        check_encodable(data)?;
        copies.check(
            CopyReason::PortableSerialization,
            data.bulk_payload_bytes() as u64,
        )?;
        let (payload, bulk_bytes) = match data {
            RuntimeData::DeviceTensor { .. } => {
                let host = data
                    .clone()
                    .device_to_host(copies)
                    .map_err(|error| match error {
                        TransferError::Rejected(rejected) => WireError::Rejected(rejected),
                        other => WireError::Device(other.to_string()),
                    })?;
                let bytes = host.bulk_payload_bytes();
                (Payload::Other(host_data_buffer(&host)), bytes)
            }
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                stream_id,
                timestamp_us,
                arrival_ts_us,
                metadata,
            } => (
                Payload::Audio {
                    samples: samples.as_slice(),
                    header: AudioBuffer {
                        samples: Bytes::new(),
                        sample_rate: *sample_rate,
                        channels: *channels,
                        format: AudioFormat::F32 as i32,
                        num_samples: samples.len() as u64,
                        metadata_json: metadata.as_ref().map(json_bytes),
                        stream_id: stream_id.clone(),
                        timestamp_us: *timestamp_us,
                        arrival_ts_us: *arrival_ts_us,
                    },
                },
                samples.len() * 4,
            ),
            other => (
                Payload::Other(host_data_buffer(other)),
                other.bulk_payload_bytes(),
            ),
        };
        Ok(Self {
            payload,
            envelope: DataBuffer {
                data_type: None,
                metadata: metadata.clone(),
                sequence,
            },
            bulk_bytes,
            copies,
        })
    }

    pub fn encoded_len(&self) -> usize {
        let payload = match &self.payload {
            Payload::Audio { header, .. } => field_len(audio_len(self.bulk_bytes, header)),
            Payload::Other(buffer) => buffer.encoded_len(),
        };
        payload + self.envelope.encoded_len()
    }

    /// Append the frame. Fails only when the copy policy rejects it.
    pub fn append_to(&self, wire: &mut Vec<u8>) -> Result<(), WireError> {
        wire.reserve(self.encoded_len());
        self.copies.materialize(
            CopyReason::PortableSerialization,
            self.bulk_bytes as u64,
            || {
                match &self.payload {
                    Payload::Audio { samples, header } => {
                        // DataBuffer.audio and AudioBuffer.samples are both tag 1.
                        encode_key(1, WireType::LengthDelimited, wire);
                        encode_varint(audio_len(self.bulk_bytes, header) as u64, wire);
                        if self.bulk_bytes != 0 {
                            encode_key(1, WireType::LengthDelimited, wire);
                            encode_varint(self.bulk_bytes as u64, wire);
                            for sample in *samples {
                                wire.extend_from_slice(&sample.to_le_bytes());
                            }
                        }
                        header.encode_raw(wire);
                    }
                    Payload::Other(buffer) => buffer.encode_raw(wire),
                }
                self.envelope.encode_raw(wire);
            },
        )?;
        Ok(())
    }
}

/// Encode one frame as a `DataBuffer`.
pub fn encode_frame(
    data: &RuntimeData,
    metadata: &HashMap<String, String>,
    sequence: Option<u64>,
    copies: &CopyContext,
) -> Result<Vec<u8>, WireError> {
    let mut wire = Vec::new();
    encode_frame_into(&mut wire, data, metadata, sequence, copies)?;
    Ok(wire)
}

/// [`encode_frame`] appending to `wire`.
pub fn encode_frame_into(
    wire: &mut Vec<u8>,
    data: &RuntimeData,
    metadata: &HashMap<String, String>,
    sequence: Option<u64>,
    copies: &CopyContext,
) -> Result<(), WireError> {
    PreparedFrame::new(data, metadata, sequence, copies)?.append_to(wire)
}

/// `RuntimeData` as a `DataBuffer` message, for embedding in another
/// message. Bulk fields share the runtime buffers; audio is converted to
/// f32 LE bytes (counted as `PortableSerialization`).
pub fn runtime_data_to_data_buffer(
    data: &RuntimeData,
    copies: &CopyContext,
) -> Result<DataBuffer, WireError> {
    check_encodable(data)?;
    if data.is_device() {
        let host = data
            .clone()
            .device_to_host(copies)
            .map_err(|error| match error {
                TransferError::Rejected(rejected) => WireError::Rejected(rejected),
                other => WireError::Device(other.to_string()),
            })?;
        return Ok(host_data_buffer(&host));
    }
    let audio_bytes = match data {
        RuntimeData::Audio { samples, .. } => samples.len() * 4,
        _ => 0,
    };
    Ok(copies.materialize(
        CopyReason::PortableSerialization,
        audio_bytes as u64,
        || host_data_buffer(data),
    )?)
}

/// Tensor dimensions are unsigned on the wire: a tensor that carries data
/// has concrete dimensions.
fn check_encodable(data: &RuntimeData) -> Result<(), WireError> {
    match data {
        RuntimeData::Tensor { shape, .. } if shape.iter().any(|&dim| dim < 0) => Err(malformed(
            format!("tensor shape {shape:?} has a negative dimension"),
        )),
        _ => Ok(()),
    }
}

fn field_len(bytes: usize) -> usize {
    key_len(1) + encoded_len_varint(bytes as u64) + bytes
}

fn audio_len(bytes: usize, header: &AudioBuffer) -> usize {
    (if bytes == 0 { 0 } else { field_len(bytes) }) + header.encoded_len()
}

fn json_bytes(value: &serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(value).expect("a JSON value serializes")
}

fn shared(bytes: &SharedBytes) -> Bytes {
    Bytes::from_owner(bytes.clone())
}

/// The `DataBuffer` of host-memory data (callers transfer device tensors
/// first).
fn host_data_buffer(data: &RuntimeData) -> DataBuffer {
    let data_type = match data {
        RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            stream_id,
            timestamp_us,
            arrival_ts_us,
            metadata,
        } => DataType::Audio(AudioBuffer {
            samples: {
                let mut bytes = Vec::with_capacity(samples.len() * 4);
                for sample in samples.as_slice() {
                    bytes.extend_from_slice(&sample.to_le_bytes());
                }
                bytes.into()
            },
            sample_rate: *sample_rate,
            channels: *channels,
            format: AudioFormat::F32 as i32,
            num_samples: samples.len() as u64,
            metadata_json: metadata.as_ref().map(json_bytes),
            stream_id: stream_id.clone(),
            timestamp_us: *timestamp_us,
            arrival_ts_us: *arrival_ts_us,
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
            stream_id,
            arrival_ts_us,
        } => DataType::Video(VideoFrame {
            pixel_data: shared(pixel_data),
            width: *width,
            height: *height,
            format: pixel_format_to_proto(*format),
            frame_number: *frame_number,
            timestamp_us: *timestamp_us,
            codec: video_codec_to_proto(*codec),
            is_keyframe: *is_keyframe,
            stream_id: stream_id.clone(),
            arrival_ts_us: *arrival_ts_us,
        }),
        RuntimeData::Image {
            data,
            format,
            width,
            height,
            timestamp_us,
            stream_id,
            metadata,
        } => {
            let (encoding, pixel_format) = match format {
                ImageFormat::Jpeg => (ImageEncoding::Jpeg, ProtoPixelFormat::Unspecified as i32),
                ImageFormat::Png => (ImageEncoding::Png, ProtoPixelFormat::Unspecified as i32),
                ImageFormat::WebP => (ImageEncoding::Webp, ProtoPixelFormat::Unspecified as i32),
                ImageFormat::Raw { pixel_format } => {
                    (ImageEncoding::Raw, pixel_format_to_proto(*pixel_format))
                }
            };
            DataType::Image(ImageBuffer {
                data: shared(data),
                encoding: encoding as i32,
                pixel_format,
                width: *width,
                height: *height,
                timestamp_us: *timestamp_us,
                stream_id: stream_id.clone(),
                metadata_json: metadata.as_ref().map(json_bytes),
            })
        }
        RuntimeData::Tensor {
            data,
            shape,
            dtype,
            metadata,
        } => DataType::Tensor(TensorBuffer {
            data: shared(data),
            // Checked non-negative by `check_encodable`.
            shape: shape.iter().map(|&dim| dim as u64).collect(),
            dtype: *dtype,
            layout: String::new(),
            metadata_json: metadata.as_ref().map(json_bytes),
        }),
        RuntimeData::Numpy {
            data,
            shape,
            dtype,
            strides,
            c_contiguous,
            f_contiguous,
        } => DataType::Numpy(NumpyBuffer {
            data: shared(data),
            shape: shape.iter().map(|&dim| dim as u64).collect(),
            dtype: dtype.clone(),
            strides: strides.iter().map(|&stride| stride as i64).collect(),
            c_contiguous: *c_contiguous,
            f_contiguous: *f_contiguous,
        }),
        RuntimeData::Json(value) => DataType::Json(JsonData {
            json_payload: value.to_string(),
            schema_type: String::new(),
        }),
        RuntimeData::Text(text) => {
            let (channel, content) = split_text_str(text);
            DataType::Text(TextBuffer {
                text_data: content.as_bytes().to_vec(),
                encoding: "utf-8".into(),
                language: String::new(),
                channel: channel.to_string(),
            })
        }
        RuntimeData::Binary(bytes) => DataType::Binary(BinaryBuffer {
            data: shared(bytes),
            mime_type: "application/octet-stream".into(),
        }),
        RuntimeData::ControlMessage {
            message_type,
            segment_id,
            timestamp_ms,
            metadata,
        } => DataType::Control(ControlMessage {
            message_type: Some(control_to_proto(message_type)),
            segment_id: segment_id.clone().unwrap_or_default(),
            timestamp_ms: *timestamp_ms,
            metadata: if metadata.is_null() {
                String::new()
            } else {
                metadata.to_string()
            },
        }),
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
        RuntimeData::DeviceTensor { .. } => {
            unreachable!("device tensors are transferred to host memory before encoding")
        }
    };
    DataBuffer {
        data_type: Some(data_type),
        metadata: HashMap::new(),
        sequence: None,
    }
}

fn control_to_proto(message_type: &ControlMessageType) -> MessageType {
    match message_type {
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
            suggested_batch_size: u32::try_from(*suggested_batch_size).unwrap_or(u32::MAX),
        }),
        ControlMessageType::DeadlineWarning { deadline_us } => {
            MessageType::DeadlineWarning(DeadlineWarning {
                deadline_us: *deadline_us,
            })
        }
    }
}

fn pixel_format_to_proto(format: PixelFormat) -> i32 {
    (match format {
        PixelFormat::Unspecified => ProtoPixelFormat::Unspecified,
        PixelFormat::Yuv420p => ProtoPixelFormat::Yuv420p,
        PixelFormat::I420 => ProtoPixelFormat::I420,
        PixelFormat::NV12 => ProtoPixelFormat::Nv12,
        PixelFormat::Rgb24 => ProtoPixelFormat::Rgb24,
        PixelFormat::Rgba32 => ProtoPixelFormat::Rgba32,
        PixelFormat::Encoded => ProtoPixelFormat::Encoded,
    }) as i32
}

fn video_codec_to_proto(codec: Option<VideoCodec>) -> i32 {
    (match codec {
        None => ProtoVideoCodec::Unspecified,
        Some(VideoCodec::Vp8) => ProtoVideoCodec::Vp8,
        Some(VideoCodec::H264) => ProtoVideoCodec::H264,
        Some(VideoCodec::Av1) => ProtoVideoCodec::Av1,
    }) as i32
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// Decode one `DataBuffer` frame. Bulk payloads keep `bytes` as their
/// storage; audio samples are converted (counted as
/// `PortableDeserialization`).
pub fn decode_frame(bytes: Bytes, copies: &CopyContext) -> Result<Frame, WireError> {
    data_buffer_into_frame(DataBuffer::decode(bytes)?, copies)
}

/// The frame a decoded `DataBuffer` carries (e.g. one embedded in another
/// message), with its metadata and sequence.
pub fn data_buffer_into_frame(
    mut buffer: DataBuffer,
    copies: &CopyContext,
) -> Result<Frame, WireError> {
    let metadata = std::mem::take(&mut buffer.metadata);
    let sequence = buffer.sequence;
    Ok(Frame {
        data: data_buffer_into_runtime_data(buffer, copies)?,
        metadata,
        sequence,
    })
}

/// The `RuntimeData` a `DataBuffer` carries (its metadata and sequence are
/// ignored; see [`decode_frame`]).
pub fn data_buffer_into_runtime_data(
    buffer: DataBuffer,
    copies: &CopyContext,
) -> Result<RuntimeData, WireError> {
    Ok(
        match buffer.data_type.ok_or_else(|| malformed("no payload"))? {
            DataType::Audio(audio) => {
                if audio.format != AudioFormat::F32 as i32 {
                    return Err(malformed(format!(
                        "audio format {} (only F32 is supported)",
                        audio.format
                    )));
                }
                if audio.samples.len() % 4 != 0 {
                    return Err(malformed("audio sample bytes are not a multiple of 4"));
                }
                let samples = copies.materialize(
                    CopyReason::PortableDeserialization,
                    audio.samples.len() as u64,
                    || {
                        audio
                            .samples
                            .chunks_exact(4)
                            .map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
                            .collect::<Vec<f32>>()
                    },
                )?;
                RuntimeData::Audio {
                    samples: samples.into(),
                    sample_rate: audio.sample_rate,
                    channels: audio.channels,
                    stream_id: audio.stream_id,
                    timestamp_us: audio.timestamp_us,
                    arrival_ts_us: audio.arrival_ts_us,
                    metadata: parse_metadata(audio.metadata_json.as_deref())?,
                }
            }
            DataType::Video(video) => RuntimeData::Video {
                format: pixel_format_from_proto(video.format)?,
                codec: video_codec_from_proto(video.codec)?,
                pixel_data: SharedBytes::from_owner(video.pixel_data),
                width: video.width,
                height: video.height,
                frame_number: video.frame_number,
                timestamp_us: video.timestamp_us,
                is_keyframe: video.is_keyframe,
                stream_id: video.stream_id,
                arrival_ts_us: video.arrival_ts_us,
            },
            DataType::Image(image) => RuntimeData::Image {
                format: match ImageEncoding::try_from(image.encoding) {
                    Ok(ImageEncoding::Jpeg) => ImageFormat::Jpeg,
                    Ok(ImageEncoding::Png) => ImageFormat::Png,
                    Ok(ImageEncoding::Webp) => ImageFormat::WebP,
                    Ok(ImageEncoding::Raw) => ImageFormat::Raw {
                        pixel_format: pixel_format_from_proto(image.pixel_format)?,
                    },
                    _ => return Err(malformed(format!("image encoding {}", image.encoding))),
                },
                metadata: parse_metadata(image.metadata_json.as_deref())?,
                data: SharedBytes::from_owner(image.data),
                width: image.width,
                height: image.height,
                timestamp_us: image.timestamp_us,
                stream_id: image.stream_id,
            },
            DataType::Tensor(tensor) => RuntimeData::Tensor {
                shape: tensor
                    .shape
                    .iter()
                    .map(|&dim| i32::try_from(dim))
                    .collect::<Result<_, _>>()
                    .map_err(|_| malformed("tensor dimension out of range"))?,
                metadata: parse_metadata(tensor.metadata_json.as_deref())?,
                data: SharedBytes::from_owner(tensor.data),
                dtype: tensor.dtype,
            },
            DataType::Numpy(numpy) => RuntimeData::Numpy {
                shape: numpy
                    .shape
                    .iter()
                    .map(|&dim| usize::try_from(dim))
                    .collect::<Result<_, _>>()
                    .map_err(|_| malformed("numpy dimension out of range"))?,
                strides: numpy
                    .strides
                    .iter()
                    .map(|&stride| isize::try_from(stride))
                    .collect::<Result<_, _>>()
                    .map_err(|_| malformed("numpy stride out of range"))?,
                data: SharedBytes::from_owner(numpy.data),
                dtype: numpy.dtype,
                c_contiguous: numpy.c_contiguous,
                f_contiguous: numpy.f_contiguous,
            },
            DataType::Json(json) => RuntimeData::Json(
                serde_json::from_str(&json.json_payload)
                    .map_err(|error| malformed(format!("JSON payload: {error}")))?,
            ),
            DataType::Text(text) => {
                let content = String::from_utf8(text.text_data)
                    .map_err(|_| malformed("text is not UTF-8"))?;
                if text.channel.is_empty() || text.channel == TEXT_CHANNEL_DEFAULT {
                    RuntimeData::Text(content)
                } else {
                    RuntimeData::Text(tag_text_str(&content, &text.channel))
                }
            }
            DataType::Binary(binary) => RuntimeData::Binary(SharedBytes::from_owner(binary.data)),
            DataType::Control(control) => RuntimeData::ControlMessage {
                message_type: match control
                    .message_type
                    .ok_or_else(|| malformed("control message without a type"))?
                {
                    MessageType::CancelSpeculation(c) => ControlMessageType::CancelSpeculation {
                        from_timestamp: c.from_timestamp,
                        to_timestamp: c.to_timestamp,
                    },
                    MessageType::BatchHint(b) => ControlMessageType::BatchHint {
                        suggested_batch_size: b.suggested_batch_size as usize,
                    },
                    MessageType::DeadlineWarning(d) => ControlMessageType::DeadlineWarning {
                        deadline_us: d.deadline_us,
                    },
                },
                segment_id: non_empty(control.segment_id),
                timestamp_ms: control.timestamp_ms,
                metadata: if control.metadata.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_str(&control.metadata)
                        .map_err(|error| malformed(format!("control metadata: {error}")))?
                },
            },
            DataType::File(file) => RuntimeData::File {
                path: file.path,
                filename: non_empty(file.filename),
                mime_type: non_empty(file.mime_type),
                size: non_zero(file.size),
                offset: non_zero(file.offset),
                length: non_zero(file.length),
                stream_id: non_empty(file.stream_id),
            },
        },
    )
}

fn parse_metadata(json: Option<&[u8]>) -> Result<Option<serde_json::Value>, WireError> {
    json.map(|bytes| {
        serde_json::from_slice(bytes).map_err(|error| malformed(format!("metadata: {error}")))
    })
    .transpose()
}

fn non_empty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

fn non_zero(value: u64) -> Option<u64> {
    (value != 0).then_some(value)
}

fn pixel_format_from_proto(value: i32) -> Result<PixelFormat, WireError> {
    Ok(match ProtoPixelFormat::try_from(value) {
        Ok(ProtoPixelFormat::Unspecified) => PixelFormat::Unspecified,
        Ok(ProtoPixelFormat::Rgb24) => PixelFormat::Rgb24,
        Ok(ProtoPixelFormat::Rgba32) => PixelFormat::Rgba32,
        Ok(ProtoPixelFormat::Yuv420p) => PixelFormat::Yuv420p,
        Ok(ProtoPixelFormat::I420) => PixelFormat::I420,
        Ok(ProtoPixelFormat::Nv12) => PixelFormat::NV12,
        Ok(ProtoPixelFormat::Encoded) => PixelFormat::Encoded,
        _ => return Err(malformed(format!("pixel format {value}"))),
    })
}

fn video_codec_from_proto(value: i32) -> Result<Option<VideoCodec>, WireError> {
    Ok(match ProtoVideoCodec::try_from(value) {
        Ok(ProtoVideoCodec::Unspecified) => None,
        Ok(ProtoVideoCodec::Vp8) => Some(VideoCodec::Vp8),
        Ok(ProtoVideoCodec::H264) => Some(VideoCodec::H264),
        Ok(ProtoVideoCodec::Av1) => Some(VideoCodec::Av1),
        _ => return Err(malformed(format!("video codec {value}"))),
    })
}

#[cfg(test)]
#[path = "codec_tests.rs"]
mod tests;
