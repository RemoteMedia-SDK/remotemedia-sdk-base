//! RuntimeData serialization/deserialization for Node.js
//!
//! This module provides zero-copy deserialization of RuntimeData from
//! iceoryx2 shared memory buffers into JavaScript objects.
//!
//! # Wire Format
//!
//! The wire format matches `data_transfer.rs` in core:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │ Offset │ Field          │ Size     │ Description               │
//! ├────────┼────────────────┼──────────┼───────────────────────────┤
//! │ 0      │ data_type      │ 1 byte   │ Type discriminant (1-7)   │
//! │ 1      │ session_len    │ 2 bytes  │ Session ID length (LE)    │
//! │ 3      │ session_id     │ N bytes  │ UTF-8 session identifier  │
//! │ 3+N    │ timestamp_ns   │ 8 bytes  │ Unix timestamp (LE u64)   │
//! │ 11+N   │ payload_len    │ 4 bytes  │ Payload length (LE u32)   │
//! │ 15+N   │ payload        │ M bytes  │ Type-specific payload     │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use super::error::IpcError;
use napi::bindgen_prelude::*;
use napi_derive::napi;

/// Data type discriminants (matches Rust RuntimeData enum)
#[napi]
pub enum DataType {
    Audio = 1,
    Video = 2,
    Text = 3,
    Tensor = 4,
    ControlMessage = 5,
    Numpy = 6,
    File = 7,
}

/// Audio buffer header size (sample_rate + channels + padding + num_samples)
pub const AUDIO_HEADER_SIZE: usize = 16;

/// Video frame header size
pub const VIDEO_HEADER_SIZE: usize = 19;

/// Parsed RuntimeData from wire format
#[napi(object)]
pub struct ParsedRuntimeData {
    /// Data type discriminant
    pub data_type: u8,
    /// Session identifier
    pub session_id: String,
    /// Timestamp in nanoseconds
    pub timestamp_ns: i64,
    /// Payload offset in buffer
    pub payload_offset: u32,
    /// Payload length
    pub payload_len: u32,
}

/// Audio buffer metadata
#[napi(object)]
pub struct AudioMetadata {
    pub sample_rate: u32,
    pub channels: u16,
    pub num_samples: i64,
    pub samples_offset: u32,
}

/// Video frame metadata
#[napi(object)]
pub struct VideoMetadata {
    pub width: u32,
    pub height: u32,
    pub format: u8,
    pub codec: u8,
    pub frame_num: i64,
    pub is_keyframe: bool,
    pub pixel_data_offset: u32,
}

/// File reference metadata (Spec 001: RuntimeData.File)
///
/// Represents a file reference with optional metadata for byte range requests
/// and multi-track routing.
#[napi(object)]
pub struct FileMetadata {
    /// File path (absolute or relative)
    pub path: String,
    /// Original filename (optional)
    pub filename: Option<String>,
    /// MIME type hint (optional)
    pub mime_type: Option<String>,
    /// File size in bytes (optional, None if unknown)
    pub size: Option<i64>,
    /// Byte offset for range requests (optional, None for start)
    pub offset: Option<i64>,
    /// Length for range requests (optional, None for to-EOF)
    pub length: Option<i64>,
    /// Stream identifier for multi-track routing (optional)
    pub stream_id: Option<String>,
}

/// Parse RuntimeData header from buffer
///
/// Returns metadata without copying the payload, allowing zero-copy
/// access to the underlying data.
#[napi]
pub fn parse_runtime_data_header(buffer: Buffer) -> napi::Result<ParsedRuntimeData> {
    let bytes = buffer.as_ref();

    if bytes.len() < 15 {
        return Err(IpcError::SerializationError(
            "Buffer too small for RuntimeData header".to_string(),
        )
        .into());
    }

    // Read header fields
    let data_type = bytes[0];
    let session_len = u16::from_le_bytes([bytes[1], bytes[2]]) as usize;

    if bytes.len() < 3 + session_len + 12 {
        return Err(IpcError::SerializationError(
            "Buffer too small for session ID and timestamp".to_string(),
        )
        .into());
    }

    let session_id = String::from_utf8_lossy(&bytes[3..3 + session_len]).to_string();

    let header_offset = 3 + session_len;
    let timestamp_ns = i64::from_le_bytes([
        bytes[header_offset],
        bytes[header_offset + 1],
        bytes[header_offset + 2],
        bytes[header_offset + 3],
        bytes[header_offset + 4],
        bytes[header_offset + 5],
        bytes[header_offset + 6],
        bytes[header_offset + 7],
    ]);

    let payload_len = u32::from_le_bytes([
        bytes[header_offset + 8],
        bytes[header_offset + 9],
        bytes[header_offset + 10],
        bytes[header_offset + 11],
    ]);

    let payload_offset = (header_offset + 12) as u32;

    Ok(ParsedRuntimeData {
        data_type,
        session_id,
        timestamp_ns,
        payload_offset,
        payload_len,
    })
}

/// Parse audio buffer metadata from payload
#[napi]
pub fn parse_audio_metadata(buffer: Buffer, payload_offset: u32) -> napi::Result<AudioMetadata> {
    let bytes = buffer.as_ref();
    let offset = payload_offset as usize;

    if bytes.len() < offset + AUDIO_HEADER_SIZE {
        return Err(
            IpcError::SerializationError("Buffer too small for audio header".to_string()).into(),
        );
    }

    let sample_rate = u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ]);

    let channels = u16::from_le_bytes([bytes[offset + 4], bytes[offset + 5]]);

    // Skip 2 bytes padding
    let num_samples = i64::from_le_bytes([
        bytes[offset + 8],
        bytes[offset + 9],
        bytes[offset + 10],
        bytes[offset + 11],
        bytes[offset + 12],
        bytes[offset + 13],
        bytes[offset + 14],
        bytes[offset + 15],
    ]);

    let samples_offset = (offset + AUDIO_HEADER_SIZE) as u32;

    Ok(AudioMetadata {
        sample_rate,
        channels,
        num_samples,
        samples_offset,
    })
}

/// Parse video frame metadata from payload
#[napi]
pub fn parse_video_metadata(buffer: Buffer, payload_offset: u32) -> napi::Result<VideoMetadata> {
    let bytes = buffer.as_ref();
    let offset = payload_offset as usize;

    if bytes.len() < offset + VIDEO_HEADER_SIZE {
        return Err(
            IpcError::SerializationError("Buffer too small for video header".to_string()).into(),
        );
    }

    let width = u32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ]);

    let height = u32::from_le_bytes([
        bytes[offset + 4],
        bytes[offset + 5],
        bytes[offset + 6],
        bytes[offset + 7],
    ]);

    let format = bytes[offset + 8];
    let codec = bytes[offset + 9];

    let frame_num = i64::from_le_bytes([
        bytes[offset + 10],
        bytes[offset + 11],
        bytes[offset + 12],
        bytes[offset + 13],
        bytes[offset + 14],
        bytes[offset + 15],
        bytes[offset + 16],
        bytes[offset + 17],
    ]);

    let is_keyframe = bytes[offset + 18] == 1;
    let pixel_data_offset = (offset + VIDEO_HEADER_SIZE) as u32;

    Ok(VideoMetadata {
        width,
        height,
        format,
        codec,
        frame_num,
        is_keyframe,
        pixel_data_offset,
    })
}

/// Parse file reference metadata from payload (Spec 001: RuntimeData.File)
///
/// # Binary Format
/// ```text
/// path_len (2 bytes) | path (variable) |
/// filename_len (2 bytes) | filename (variable) |
/// mime_type_len (2 bytes) | mime_type (variable) |
/// size (8 bytes) | offset (8 bytes) | length (8 bytes) |
/// stream_id_len (2 bytes) | stream_id (variable)
/// ```
#[napi]
pub fn parse_file_metadata(buffer: Buffer, payload_offset: u32) -> napi::Result<FileMetadata> {
    let bytes = buffer.as_ref();
    let mut pos = payload_offset as usize;

    // Minimum size check: 2+0 + 2+0 + 2+0 + 8 + 8 + 8 + 2+0 = 32 bytes
    if bytes.len() < pos + 32 {
        return Err(
            IpcError::SerializationError("Buffer too small for file metadata".to_string()).into(),
        );
    }

    // Path (required)
    let path_len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
    pos += 2;
    if pos + path_len > bytes.len() {
        return Err(IpcError::SerializationError("Invalid path length".to_string()).into());
    }
    let path = String::from_utf8_lossy(&bytes[pos..pos + path_len]).to_string();
    pos += path_len;

    // Filename (optional)
    if pos + 2 > bytes.len() {
        return Err(
            IpcError::SerializationError("Invalid filename length field".to_string()).into(),
        );
    }
    let filename_len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
    pos += 2;
    let filename = if filename_len > 0 {
        if pos + filename_len > bytes.len() {
            return Err(IpcError::SerializationError("Invalid filename".to_string()).into());
        }
        Some(String::from_utf8_lossy(&bytes[pos..pos + filename_len]).to_string())
    } else {
        None
    };
    pos += filename_len;

    // MIME type (optional)
    if pos + 2 > bytes.len() {
        return Err(
            IpcError::SerializationError("Invalid mime_type length field".to_string()).into(),
        );
    }
    let mime_type_len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
    pos += 2;
    let mime_type = if mime_type_len > 0 {
        if pos + mime_type_len > bytes.len() {
            return Err(IpcError::SerializationError("Invalid mime_type".to_string()).into());
        }
        Some(String::from_utf8_lossy(&bytes[pos..pos + mime_type_len]).to_string())
    } else {
        None
    };
    pos += mime_type_len;

    // Size, Offset, Length (8 bytes each)
    if pos + 24 > bytes.len() {
        return Err(
            IpcError::SerializationError("Invalid size/offset/length fields".to_string()).into(),
        );
    }
    let size_val = u64::from_le_bytes([
        bytes[pos],
        bytes[pos + 1],
        bytes[pos + 2],
        bytes[pos + 3],
        bytes[pos + 4],
        bytes[pos + 5],
        bytes[pos + 6],
        bytes[pos + 7],
    ]);
    pos += 8;
    let offset_val = u64::from_le_bytes([
        bytes[pos],
        bytes[pos + 1],
        bytes[pos + 2],
        bytes[pos + 3],
        bytes[pos + 4],
        bytes[pos + 5],
        bytes[pos + 6],
        bytes[pos + 7],
    ]);
    pos += 8;
    let length_val = u64::from_le_bytes([
        bytes[pos],
        bytes[pos + 1],
        bytes[pos + 2],
        bytes[pos + 3],
        bytes[pos + 4],
        bytes[pos + 5],
        bytes[pos + 6],
        bytes[pos + 7],
    ]);
    pos += 8;

    // Stream ID (optional)
    if pos + 2 > bytes.len() {
        return Err(
            IpcError::SerializationError("Invalid stream_id length field".to_string()).into(),
        );
    }
    let stream_id_len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
    pos += 2;
    let stream_id = if stream_id_len > 0 {
        if pos + stream_id_len > bytes.len() {
            return Err(IpcError::SerializationError("Invalid stream_id".to_string()).into());
        }
        Some(String::from_utf8_lossy(&bytes[pos..pos + stream_id_len]).to_string())
    } else {
        None
    };

    // Convert 0 values to None for optional numeric fields
    let size = if size_val == 0 {
        None
    } else {
        Some(size_val as i64)
    };
    let offset = if offset_val == 0 {
        None
    } else {
        Some(offset_val as i64)
    };
    let length = if length_val == 0 {
        None
    } else {
        Some(length_val as i64)
    };

    Ok(FileMetadata {
        path,
        filename,
        mime_type,
        size,
        offset,
        length,
        stream_id,
    })
}

/// Check if data type is Audio
#[napi]
pub fn is_audio(data_type: u8) -> bool {
    data_type == DataType::Audio as u8
}

/// Check if data type is Video
#[napi]
pub fn is_video(data_type: u8) -> bool {
    data_type == DataType::Video as u8
}

/// Check if data type is Text
#[napi]
pub fn is_text(data_type: u8) -> bool {
    data_type == DataType::Text as u8
}

/// Check if data type is ControlMessage
#[napi]
pub fn is_control_message(data_type: u8) -> bool {
    data_type == DataType::ControlMessage as u8
}

/// Check if data type is File
#[napi]
pub fn is_file(data_type: u8) -> bool {
    data_type == DataType::File as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_values() {
        assert_eq!(DataType::Audio as u8, 1);
        assert_eq!(DataType::Video as u8, 2);
        assert_eq!(DataType::Text as u8, 3);
        assert_eq!(DataType::Tensor as u8, 4);
        assert_eq!(DataType::ControlMessage as u8, 5);
        assert_eq!(DataType::Numpy as u8, 6);
        assert_eq!(DataType::File as u8, 7);
    }

    #[test]
    fn test_type_guards() {
        assert!(is_audio(1));
        assert!(!is_audio(2));
        assert!(is_video(2));
        assert!(is_text(3));
        assert!(is_control_message(5));
        assert!(is_file(7));
        assert!(!is_file(1));
    }
}
