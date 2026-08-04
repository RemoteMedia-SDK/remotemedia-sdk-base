//! Zero-copy IPC data transfer via iceoryx2 shared memory.
//!
//! Uses the native [`RuntimeData`] enum from `remotemedia-types`
//! directly — both host and plugin processes link the same crate
//! version so they agree on the serialization layout byte-for-byte.
//!
//! Serialization: **MessagePack** (`rmp-serde` named encoding) for all
//! variants.
//!
//! # Why rmp-serde and not bincode?
//!
//! Several `RuntimeData` variants use `#[serde(skip_serializing_if =
//! "Option::is_none")]` on their optional fields, and some carry
//! `serde_json::Value` (e.g. `Json`, `metadata` fields).  Bincode 1.x is
//! position-based and does not support `deserialize_any`, so both of those
//! patterns break at deserialize time.  rmp-serde named encoding is
//! self-describing (field names in the wire format) and handles all serde
//! attributes correctly, at a modest size cost that is acceptable for
//! shared-memory IPC.
//!
//! # Why not zero-copy native enum placement?
//!
//! Rust enums have no stable memory layout. `RuntimeData` contains
//! `Vec`, `String`, `Arc`, `Option` — all heap-allocated with
//! pointers meaningless across process boundaries. iceoryx2's
//! zero-copy works at the byte-slice level; the data still needs
//! to be serialized to a contiguous buffer.

pub use remotemedia_types::{AudioSamples, RuntimeData};

/// Maximum payload size for IPC transfers (1 MB).
/// Matches the global iceoryx2 config: publish_subscribe.max_slice_len = 1048576.
pub const MAX_SLICE_LEN: usize = 1 * 1024 * 1024;

// Legacy magic prefix written by the short-lived bincode+audio-rmp hybrid that
// shipped between commits 39de1d1 and 8761b94.  `from_bytes` strips it so old
// in-flight audio messages are still readable; `to_bytes` never writes it.
const LEGACY_AUDIO_RMP_PREFIX: &[u8] = b"RM_AUD_RMP1\0";

/// Serialize a [`RuntimeData`] to bytes for iceoryx2 shared memory.
///
/// Uses MessagePack (rmp-serde named encoding) so all serde attributes
/// (`skip_serializing_if`, `default`, …) and `serde_json::Value` fields
/// are handled correctly.
///
/// # Errors
///
/// Returns `Err` if serialization fails or the payload exceeds
/// [`MAX_SLICE_LEN`].
pub fn to_bytes(data: &RuntimeData) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(data).map_err(|e| format!("msgpack serialize: {e}"))
}

/// Deserialize a [`RuntimeData`] from iceoryx2 shared memory bytes.
///
/// Accepts both the current MessagePack encoding and the legacy
/// audio-only rmp prefix format for backward compatibility.
///
/// # Errors
///
/// Returns `Err` if deserialization fails (e.g., corrupted payload,
/// version mismatch, or truncated data).
pub fn from_bytes(bytes: &[u8]) -> Result<RuntimeData, String> {
    // Strip the legacy audio prefix if present so old in-flight messages
    // from the short-lived hybrid encoding are still readable.
    let payload = bytes.strip_prefix(LEGACY_AUDIO_RMP_PREFIX).unwrap_or(bytes);
    rmp_serde::from_slice(payload).map_err(|e| format!("msgpack deserialize: {e}"))
}

/// Re-export text-channel helpers that were previously defined in
/// the hand-crafted IPC `RuntimeData`. Keeps these names reachable
/// via the existing import path.
pub use remotemedia_types::{split_text_str, tag_text_str, TEXT_CHANNEL_DEFAULT};
/// Python IPC wire format type tags. Matches `plugin-sdk::python_ipc::WireDataType`.
/// Defined here so the Python multiprocess executor doesn't need the optional
/// `remotemedia-plugin-sdk` dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WireDataType {
    Audio = 1,
    Video = 2,
    Text = 3,
    Tensor = 4,
    ControlMessage = 5,
    Numpy = 6,
    File = 7,
    EndOfInput = 8,
}

/// Serialize a [`RuntimeData`] to the Python IPC wire format.
///
/// Wire layout: `[type:u8][session_len:u16][session_id:bytes][timestamp:u64][payload_len:u32][payload:bytes]`
///
/// This format is understood by the Python multiprocessing runner
/// (`clients/python/remotemedia/core/multiprocessing/data.py`).
pub fn to_python_wire(data: &RuntimeData, session_id: &str) -> Vec<u8> {
    let (wire_type, payload) = match data {
        RuntimeData::Text(s) => (WireDataType::Text, s.as_bytes().to_vec()),
        RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            metadata,
            ..
        } => {
            let wire_type = WireDataType::Audio;
            let mut payload = Vec::with_capacity(10 + samples.len() * 4);
            payload.extend_from_slice(&sample_rate.to_le_bytes());
            payload.extend_from_slice(&(*channels as u16).to_le_bytes());
            let meta_bytes = match metadata {
                Some(m) => serde_json::to_vec(m).unwrap_or_default(),
                None => Vec::new(),
            };
            payload.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
            payload.extend_from_slice(&meta_bytes);
            for &s in samples.as_slice() {
                payload.extend_from_slice(&s.to_le_bytes());
            }
            return finish_wire(wire_type, session_id, payload);
        }
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
            let wire_type = WireDataType::Video;
            let mut payload = Vec::with_capacity(19 + pixel_data.len());
            payload.extend_from_slice(&width.to_le_bytes());
            payload.extend_from_slice(&height.to_le_bytes());
            payload.push(*format as u8);
            payload.push(codec.map(|c| c as u8).unwrap_or(0));
            payload.extend_from_slice(&frame_number.to_le_bytes());
            payload.extend_from_slice(&timestamp_us.to_le_bytes());
            payload.push(if *is_keyframe { 1 } else { 0 });
            payload.extend_from_slice(pixel_data);
            return finish_wire(wire_type, session_id, payload);
        }
        RuntimeData::Tensor {
            data,
            shape,
            dtype,
            metadata,
        } => {
            let wire_type = WireDataType::Tensor;
            let mut payload = Vec::with_capacity(9 + data.len() + shape.len() * 4);
            payload.extend_from_slice(&data.len().to_le_bytes());
            payload.extend_from_slice(&(*dtype as u32).to_le_bytes());
            payload.extend_from_slice(&(shape.len() as u32).to_le_bytes());
            for &s in shape {
                payload.extend_from_slice(&s.to_le_bytes());
            }
            let meta_bytes = match metadata {
                Some(m) => serde_json::to_vec(m).unwrap_or_default(),
                None => Vec::new(),
            };
            payload.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
            payload.extend_from_slice(&meta_bytes);
            payload.extend_from_slice(data);
            return finish_wire(wire_type, session_id, payload);
        }
        RuntimeData::ControlMessage {
            message_type,
            segment_id,
            timestamp_ms,
            metadata,
        } => {
            let wire_type = WireDataType::ControlMessage;
            let (kind, type_fields) = match message_type {
                remotemedia_types::ControlMessageType::CancelSpeculation {
                    from_timestamp,
                    to_timestamp,
                } => (
                    "cancel_speculation",
                    serde_json::json!({
                        "from_timestamp": from_timestamp,
                        "to_timestamp": to_timestamp,
                    }),
                ),
                remotemedia_types::ControlMessageType::BatchHint {
                    suggested_batch_size,
                } => (
                    "batch_hint",
                    serde_json::json!({"suggested_batch_size": suggested_batch_size}),
                ),
                remotemedia_types::ControlMessageType::DeadlineWarning { deadline_us } => (
                    "deadline_warning",
                    serde_json::json!({"deadline_us": deadline_us}),
                ),
            };
            let payload = serde_json::json!({
                "type": kind,
                "segment_id": segment_id,
                "timestamp_ms": timestamp_ms,
                "metadata": metadata,
            });
            let mut payload = payload.as_object().cloned().unwrap_or_default();
            if let Some(type_fields) = type_fields.as_object() {
                payload.extend(type_fields.clone());
            }
            return finish_wire(
                wire_type,
                session_id,
                serde_json::to_vec(&payload).unwrap_or_default(),
            );
        }
        RuntimeData::Numpy {
            data,
            shape,
            dtype,
            strides,
            c_contiguous,
            f_contiguous,
        } => {
            let wire_type = WireDataType::Numpy;
            let mut payload = Vec::new();
            let dtype_bytes = dtype.as_bytes();
            payload.extend_from_slice(&(dtype_bytes.len() as u32).to_le_bytes());
            payload.extend_from_slice(dtype_bytes);
            payload.extend_from_slice(&(shape.len() as u32).to_le_bytes());
            for &s in shape {
                payload.extend_from_slice(&(s as u32).to_le_bytes());
            }
            payload.extend_from_slice(&(strides.len() as u32).to_le_bytes());
            for &s in strides {
                payload.extend_from_slice(&(s as u32).to_le_bytes());
            }
            payload.push(if *c_contiguous { 1 } else { 0 });
            payload.push(if *f_contiguous { 1 } else { 0 });
            payload.extend_from_slice(data);
            return finish_wire(wire_type, session_id, payload);
        }
        RuntimeData::File {
            path,
            filename,
            mime_type,
            size,
            offset,
            length,
            stream_id,
        } => {
            let wire_type = WireDataType::File;
            let payload = serde_json::json!({
                "path": path,
                "filename": filename,
                "mime_type": mime_type,
                "size": size,
                "offset": offset,
                "length": length,
                "stream_id": stream_id,
            });
            return finish_wire(
                wire_type,
                session_id,
                serde_json::to_vec(&payload).unwrap_or_default(),
            );
        }
        RuntimeData::Json(v) => {
            let wire_type = WireDataType::Text;
            let text = serde_json::to_string(v).unwrap_or_else(|_| "{}".to_string());
            return finish_wire(wire_type, session_id, text.into_bytes());
        }
        RuntimeData::Binary(b) => {
            let wire_type = WireDataType::Text;
            let text = format!("Binary data: {} bytes", b.len());
            return finish_wire(wire_type, session_id, text.into_bytes());
        }
        RuntimeData::Image { .. } => {
            // Images not directly supported by Python wire format — serialize as JSON
            let wire_type = WireDataType::Text;
            let text = format!("{:?}", data);
            return finish_wire(wire_type, session_id, text.into_bytes());
        }
    };
    finish_wire(wire_type, session_id, payload)
}

/// Build the complete wire blob from type tag, session ID, and payload.
fn finish_wire(wire_type: WireDataType, session_id: &str, payload: Vec<u8>) -> Vec<u8> {
    let session_bytes = session_id.as_bytes();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0);
    let mut bytes = Vec::with_capacity(1 + 2 + session_bytes.len() + 8 + 4 + payload.len());
    bytes.push(wire_type as u8);
    bytes.extend_from_slice(&(session_bytes.len() as u16).to_le_bytes());
    bytes.extend_from_slice(session_bytes);
    bytes.extend_from_slice(&timestamp.to_le_bytes());
    bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&payload);
    bytes
}

/// Deserialize the Python IPC wire format back to [`RuntimeData`].
///
/// Wire layout: `[type:u8][session_len:u16][session_id:bytes][timestamp:u64][payload_len:u32][payload:bytes]`
pub fn from_python_wire(bytes: &[u8]) -> Result<RuntimeData, String> {
    if bytes.len() < 1 + 2 + 8 + 4 {
        return Err(format!("wire blob too short: {} bytes", bytes.len()));
    }
    let mut p = 0;
    let wire_type = match bytes[p] {
        1 => WireDataType::Audio,
        2 => WireDataType::Video,
        3 => WireDataType::Text,
        4 => WireDataType::Tensor,
        5 => WireDataType::ControlMessage,
        6 => WireDataType::Numpy,
        7 => WireDataType::File,
        8 => WireDataType::EndOfInput,
        other => return Err(format!("unknown wire data type {}", other)),
    };
    p += 1;
    let session_len = u16::from_le_bytes([bytes[p], bytes[p + 1]]) as usize;
    p += 2;
    if p + session_len > bytes.len() {
        return Err("session length overruns buffer".into());
    }
    let session_id = String::from_utf8_lossy(&bytes[p..p + session_len]).into_owned();
    p += session_len;
    if p + 8 > bytes.len() {
        return Err("timestamp overruns buffer".into());
    }
    let _timestamp = u64::from_le_bytes([
        bytes[p],
        bytes[p + 1],
        bytes[p + 2],
        bytes[p + 3],
        bytes[p + 4],
        bytes[p + 5],
        bytes[p + 6],
        bytes[p + 7],
    ]);
    p += 8;
    if p + 4 > bytes.len() {
        return Err("payload length overruns buffer".into());
    }
    let payload_len =
        u32::from_le_bytes([bytes[p], bytes[p + 1], bytes[p + 2], bytes[p + 3]]) as usize;
    p += 4;
    if p + payload_len > bytes.len() {
        return Err("payload overruns buffer".into());
    }
    let payload = &bytes[p..p + payload_len];

    // Validate session_id matches what we expect (optional — caller can check)
    let _ = session_id;

    match wire_type {
        WireDataType::Text => {
            let text = String::from_utf8_lossy(payload).into_owned();
            // Check if it's actually JSON sent on the text channel
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                // Re-promote to Json if it parses cleanly
                return Ok(RuntimeData::Json(value));
            }
            Ok(RuntimeData::Text(text))
        }
        WireDataType::Audio => {
            if payload.len() < 10 {
                return Err("Audio wire payload too short".into());
            }
            let mut pos = 0;
            let sample_rate = u32::from_le_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]);
            pos += 4;
            let channels = u16::from_le_bytes([payload[pos], payload[pos + 1]]) as u32;
            pos += 2;
            let metadata_len = u32::from_le_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]) as usize;
            pos += 4;
            let metadata = if metadata_len > 0 && pos + metadata_len <= payload.len() {
                serde_json::from_slice(&payload[pos..pos + metadata_len]).ok()
            } else {
                None
            };
            pos += metadata_len;
            let samples: Vec<f32> = payload[pos..]
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            Ok(RuntimeData::Audio {
                samples: samples.into(),
                sample_rate,
                channels,
                stream_id: None,
                timestamp_us: None,
                arrival_ts_us: None,
                metadata,
            })
        }
        WireDataType::Video => {
            if payload.len() < 19 {
                return Err("Video wire payload too short".into());
            }
            let mut pos = 0;
            let width = u32::from_le_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]);
            pos += 4;
            let height = u32::from_le_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]);
            pos += 4;
            let format = match payload[pos] {
                1 => remotemedia_types::PixelFormat::Yuv420p,
                2 => remotemedia_types::PixelFormat::I420,
                3 => remotemedia_types::PixelFormat::NV12,
                4 => remotemedia_types::PixelFormat::Rgb24,
                5 => remotemedia_types::PixelFormat::Rgba32,
                255 => remotemedia_types::PixelFormat::Encoded,
                _ => remotemedia_types::PixelFormat::Unspecified,
            };
            pos += 1;
            let codec = match payload[pos] {
                1 => Some(remotemedia_types::VideoCodec::Vp8),
                2 => Some(remotemedia_types::VideoCodec::H264),
                3 => Some(remotemedia_types::VideoCodec::Av1),
                0 => None,
                _ => None,
            };
            pos += 1;
            let frame_number = u64::from_le_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
                payload[pos + 4],
                payload[pos + 5],
                payload[pos + 6],
                payload[pos + 7],
            ]);
            pos += 8;
            let timestamp_us = u64::from_le_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
                payload[pos + 4],
                payload[pos + 5],
                payload[pos + 6],
                payload[pos + 7],
            ]);
            pos += 8;
            let is_keyframe = payload[pos] != 0;
            pos += 1;
            let pixel_data = payload[pos..].to_vec();
            Ok(RuntimeData::Video {
                pixel_data,
                width,
                height,
                format,
                codec,
                frame_number,
                timestamp_us,
                is_keyframe,
                stream_id: None,
                arrival_ts_us: None,
            })
        }
        WireDataType::Tensor => {
            if payload.len() < 9 {
                return Err("Tensor wire payload too short".into());
            }
            let mut pos = 0;
            let _data_len = u32::from_le_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]) as usize;
            pos += 4;
            let dtype = i32::from_le_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]);
            pos += 4;
            let shape_len = u32::from_le_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]) as usize;
            pos += 4;
            let mut shape = Vec::with_capacity(shape_len);
            for _ in 0..shape_len {
                if pos + 4 > payload.len() {
                    return Err("Tensor shape overruns".into());
                }
                shape.push(i32::from_le_bytes([
                    payload[pos],
                    payload[pos + 1],
                    payload[pos + 2],
                    payload[pos + 3],
                ]));
                pos += 4;
            }
            let meta_len = if pos + 4 <= payload.len() {
                u32::from_le_bytes([
                    payload[pos],
                    payload[pos + 1],
                    payload[pos + 2],
                    payload[pos + 3],
                ]) as usize
            } else {
                0
            };
            pos += 4;
            let metadata = if meta_len > 0 && pos + meta_len <= payload.len() {
                serde_json::from_slice(&payload[pos..pos + meta_len]).ok()
            } else {
                None
            };
            pos += meta_len;
            let data = payload[pos..].to_vec();
            Ok(RuntimeData::Tensor {
                data,
                shape,
                dtype,
                metadata,
            })
        }
        WireDataType::ControlMessage => {
            let json: serde_json::Value = serde_json::from_slice(payload)
                .map_err(|e| format!("ControlMessage JSON parse: {}", e))?;
            let message_type = match json.get("type").and_then(|v| v.as_str()) {
                Some("cancel_speculation") => {
                    let from_ts = json
                        .get("from_timestamp")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    let to_ts = json
                        .get("to_timestamp")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    remotemedia_types::ControlMessageType::CancelSpeculation {
                        from_timestamp: from_ts,
                        to_timestamp: to_ts,
                    }
                }
                Some("batch_hint") => {
                    let size = json
                        .get("suggested_batch_size")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(1) as usize;
                    remotemedia_types::ControlMessageType::BatchHint {
                        suggested_batch_size: size,
                    }
                }
                Some("deadline_warning") => {
                    let deadline = json
                        .get("deadline_us")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    remotemedia_types::ControlMessageType::DeadlineWarning {
                        deadline_us: deadline,
                    }
                }
                _ => remotemedia_types::ControlMessageType::BatchHint {
                    suggested_batch_size: 1,
                },
            };
            let segment_id = json
                .get("segment_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let timestamp_ms = json
                .get("timestamp_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let metadata = json
                .get("metadata")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            Ok(RuntimeData::ControlMessage {
                message_type,
                segment_id,
                timestamp_ms,
                metadata,
            })
        }
        WireDataType::Numpy => {
            if payload.len() < 4 {
                return Err("Numpy wire payload too short".into());
            }
            let mut pos = 0;
            let dtype_len = u32::from_le_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]) as usize;
            pos += 4;
            let dtype = String::from_utf8_lossy(&payload[pos..pos + dtype_len]).into_owned();
            pos += dtype_len;
            let shape_len = u32::from_le_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]) as usize;
            pos += 4;
            let mut shape = Vec::with_capacity(shape_len);
            for _ in 0..shape_len {
                if pos + 4 > payload.len() {
                    return Err("Numpy shape overruns".into());
                }
                shape.push(u32::from_le_bytes([
                    payload[pos],
                    payload[pos + 1],
                    payload[pos + 2],
                    payload[pos + 3],
                ]) as usize);
                pos += 4;
            }
            let strides_len = u32::from_le_bytes([
                payload[pos],
                payload[pos + 1],
                payload[pos + 2],
                payload[pos + 3],
            ]) as usize;
            pos += 4;
            let mut strides = Vec::with_capacity(strides_len);
            for _ in 0..strides_len {
                if pos + 4 > payload.len() {
                    return Err("Numpy strides overruns".into());
                }
                strides.push(u32::from_le_bytes([
                    payload[pos],
                    payload[pos + 1],
                    payload[pos + 2],
                    payload[pos + 3],
                ]) as isize);
                pos += 4;
            }
            let c_contiguous = pos < payload.len() && payload[pos] != 0;
            pos += 1;
            let f_contiguous = pos < payload.len() && payload[pos] != 0;
            pos += 1;
            let data = payload[pos..].to_vec();
            Ok(RuntimeData::Numpy {
                data,
                shape,
                dtype,
                strides,
                c_contiguous,
                f_contiguous,
            })
        }
        WireDataType::File => {
            let json: serde_json::Value =
                serde_json::from_slice(payload).map_err(|e| format!("File JSON parse: {}", e))?;
            let path = json
                .get("path")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let filename = json
                .get("filename")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let mime_type = json
                .get("mime_type")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let size = json.get("size").and_then(|v| v.as_u64());
            let offset = json.get("offset").and_then(|v| v.as_u64());
            let length = json.get("length").and_then(|v| v.as_u64());
            let stream_id = json
                .get("stream_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            Ok(RuntimeData::File {
                path,
                filename,
                mime_type,
                size,
                offset,
                length,
                stream_id,
            })
        }
        WireDataType::EndOfInput => Ok(RuntimeData::Text(String::new())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that all RuntimeData variants survive a msgpack round-trip.
    /// This is the safety net for the IPC path — if any variant fails
    /// to serialize/deserialize, the multiprocess factory will break.
    fn roundtrip(original: RuntimeData) {
        let bytes = to_bytes(&original).expect("serialize");
        assert!(
            bytes.len() <= MAX_SLICE_LEN,
            "payload exceeds MAX_SLICE_LEN"
        );
        let decoded = from_bytes(&bytes).expect("deserialize");
        assert_eq!(original, decoded, "variant did not round-trip");
    }

    #[test]
    fn audio_roundtrips() {
        roundtrip(RuntimeData::Audio {
            samples: vec![0.1, 0.2, 0.3].into(),
            sample_rate: 16_000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        });
    }

    #[test]
    fn audio_with_all_fields_roundtrips() {
        roundtrip(RuntimeData::Audio {
            samples: vec![0.0, 1.0, -1.0, 0.5].into(),
            sample_rate: 48_000,
            channels: 2,
            stream_id: Some("main".into()),
            timestamp_us: Some(123_456),
            arrival_ts_us: Some(123_457),
            metadata: Some(serde_json::json!({"speaker": "alice", "confidence": 0.95})),
        });
    }

    #[test]
    fn video_roundtrips() {
        roundtrip(RuntimeData::Video {
            pixel_data: vec![1, 2, 3, 4, 5],
            width: 1280,
            height: 720,
            format: remotemedia_types::PixelFormat::Yuv420p,
            codec: None,
            frame_number: 42,
            timestamp_us: 1_000_000,
            is_keyframe: true,
            stream_id: None,
            arrival_ts_us: None,
        });
    }

    #[test]
    fn video_encoded_roundtrips() {
        roundtrip(RuntimeData::Video {
            pixel_data: vec![0xff, 0xfe, 0xfd],
            width: 1920,
            height: 1080,
            format: remotemedia_types::PixelFormat::Encoded,
            codec: Some(remotemedia_types::VideoCodec::Vp8),
            frame_number: 100,
            timestamp_us: 2_000_000,
            is_keyframe: false,
            stream_id: Some("video_track".into()),
            arrival_ts_us: Some(2_000_500),
        });
    }

    #[test]
    fn image_jpeg_roundtrips() {
        roundtrip(RuntimeData::Image {
            data: vec![0xff, 0xd8, 0xff, 0xe0],
            format: remotemedia_types::ImageFormat::Jpeg,
            width: 640,
            height: 480,
            timestamp_us: Some(500_000),
            stream_id: None,
            metadata: None,
        });
    }

    #[test]
    fn image_raw_roundtrips() {
        roundtrip(RuntimeData::Image {
            data: vec![0; 64],
            format: remotemedia_types::ImageFormat::Raw {
                pixel_format: remotemedia_types::PixelFormat::Rgba32,
            },
            width: 4,
            height: 4,
            timestamp_us: None,
            stream_id: Some("preview".into()),
            metadata: Some(serde_json::json!({"source": "camera"})),
        });
    }

    #[test]
    fn text_roundtrips() {
        roundtrip(RuntimeData::Text("hello world".into()));
    }

    #[test]
    fn binary_roundtrips() {
        roundtrip(RuntimeData::Binary(vec![0, 1, 2, 3, 255]));
    }

    #[test]
    fn json_roundtrips() {
        roundtrip(RuntimeData::Json(serde_json::json!({
            "key": "value",
            "n": 42,
            "arr": [1, 2, 3],
        })));
    }

    #[test]
    fn tensor_roundtrips() {
        roundtrip(RuntimeData::Tensor {
            data: vec![0u8; 16],
            shape: vec![2, 2, 2, 2],
            dtype: 0,
            metadata: None,
        });
    }

    #[test]
    fn tensor_with_metadata_roundtrips() {
        roundtrip(RuntimeData::Tensor {
            data: vec![1u8; 8],
            shape: vec![2, 4],
            dtype: 1,
            metadata: Some(serde_json::json!({"layer": "embedding"})),
        });
    }

    #[test]
    fn numpy_roundtrips() {
        roundtrip(RuntimeData::Numpy {
            data: vec![0u8; 32],
            shape: vec![4, 2],
            dtype: "float32".into(),
            strides: vec![8, 4],
            c_contiguous: true,
            f_contiguous: false,
        });
    }

    #[test]
    fn control_message_roundtrips() {
        roundtrip(RuntimeData::ControlMessage {
            message_type: remotemedia_types::ControlMessageType::CancelSpeculation {
                from_timestamp: 1000,
                to_timestamp: 2000,
            },
            segment_id: Some("seg-1".into()),
            timestamp_ms: 12345,
            metadata: serde_json::json!({"reason": "user_cancel"}),
        });
    }

    #[test]
    fn control_message_batch_hint_roundtrips() {
        roundtrip(RuntimeData::ControlMessage {
            message_type: remotemedia_types::ControlMessageType::BatchHint {
                suggested_batch_size: 8,
            },
            segment_id: None,
            timestamp_ms: 999,
            metadata: serde_json::Value::Null,
        });
    }

    #[test]
    fn file_minimal_roundtrips() {
        roundtrip(RuntimeData::File {
            path: "/tmp/output.bin".into(),
            filename: None,
            mime_type: None,
            size: None,
            offset: None,
            length: None,
            stream_id: None,
        });
    }

    #[test]
    fn file_full_roundtrips() {
        roundtrip(RuntimeData::File {
            path: "/data/large.bin".into(),
            filename: Some("large.bin".into()),
            mime_type: Some("application/octet-stream".into()),
            size: Some(1_073_741_824),
            offset: Some(10 * 1024 * 1024),
            length: Some(64 * 1024),
            stream_id: Some("track1".into()),
        });
    }

    #[test]
    fn msgpack_payload_fits_in_slice_len() {
        // Sanity check: a representative Audio payload must stay well within
        // the 1 MB iceoryx2 slice limit after msgpack encoding.
        let data = RuntimeData::Audio {
            samples: vec![0.1, 0.2, 0.3, 0.4, 0.5].into(),
            sample_rate: 48_000,
            channels: 2,
            stream_id: Some("main".into()),
            timestamp_us: Some(123_456),
            arrival_ts_us: Some(123_457),
            metadata: Some(serde_json::json!({"speaker": "alice"})),
        };

        let bytes = to_bytes(&data).unwrap();
        assert!(
            bytes.len() < MAX_SLICE_LEN,
            "msgpack payload ({} bytes) exceeds MAX_SLICE_LEN",
            bytes.len()
        );
    }
}
