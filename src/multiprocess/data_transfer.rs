//! Portable compatibility encoding for IPC data transfer.
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
//! zero-copy works at the byte-slice level. This codec serializes the payload
//! into a contiguous buffer; it does not provide zero-copy payload transfer.
//! Sharing mapped allocations requires a separate descriptor/lease protocol.

pub use remotemedia_types::{AudioSamples, RuntimeData};
use remotemedia_types::{CopyContext, CopyReason};

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
/// Returns `Err` if serialization fails. The transport enforces its message-size
/// limit. Logical bulk serialization is recorded in the unattributed legacy ledger.
pub fn to_bytes(data: &RuntimeData) -> Result<Vec<u8>, String> {
    to_bytes_with_copy_context(data, CopyContext::legacy())
}

/// Encode through an explicit boundary policy. Strict mode rejects nonempty bulk
/// payloads before encoding. Metadata-only messages remain portable under either
/// policy. Counts are visible source payload bytes, not wire length or all copies
/// inside MessagePack/Vec growth; decoding and transport staging are separate.
pub fn to_bytes_with_copy_context(
    data: &RuntimeData,
    copies: &CopyContext,
) -> Result<Vec<u8>, String> {
    let mut output = Vec::new();
    write_with_copy_context(&mut output, data, copies)?;
    Ok(output)
}

/// Write portable MessagePack with the same policy/accounting as to_bytes.
/// On writer/codec failure, output may be partial and must not be published.
/// Failed requested bytes are recorded without claiming completed serialization.
pub fn write_with_copy_context(
    writer: impl std::io::Write,
    data: &RuntimeData,
    copies: &CopyContext,
) -> Result<(), String> {
    use serde::Serialize;
    copies
        .materialize_result(
            CopyReason::PortableSerialization,
            data.bulk_payload_bytes() as u64,
            || data.serialize(&mut rmp_serde::Serializer::new(writer).with_struct_map()),
        )
        .map_err(|e| e.to_string())?
        .map_err(|e| format!("msgpack serialize: {e}"))
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
    from_bytes_with_copy_context(bytes, CopyContext::legacy())
}

/// Decode under an explicit bulk materialization policy. Strict mode rejects
/// before decoding the first nonzero-size payload element or allocating its
/// destination buffer. Metadata and empty payloads remain portable. Counts cover
/// element storage, not metadata or codec internals; failures may be partial.
pub fn from_bytes_with_copy_context(
    bytes: &[u8],
    copies: &CopyContext,
) -> Result<RuntimeData, String> {
    // Strip the legacy audio prefix if present so old in-flight messages
    // from the short-lived hybrid encoding are still readable.
    let payload = bytes.strip_prefix(LEGACY_AUDIO_RMP_PREFIX).unwrap_or(bytes);
    copies
        .deserialize(&mut rmp_serde::Deserializer::from_read_ref(payload))
        .map_err(|e| format!("msgpack deserialize: {e}"))
}

/// Decode exactly one compatibility frame; trailing bytes are an error rather
/// than silently dropping another concatenated message. Existing stream-style
/// from_bytes callers retain their behavior.
pub fn from_frame_with_copy_context(
    bytes: &[u8],
    copies: &CopyContext,
) -> Result<RuntimeData, String> {
    let payload = bytes.strip_prefix(LEGACY_AUDIO_RMP_PREFIX).unwrap_or(bytes);
    let mut decoder = rmp_serde::Deserializer::new(std::io::Cursor::new(payload));
    let data = copies
        .deserialize(&mut decoder)
        .map_err(|e| format!("msgpack deserialize: {e}"))?;
    if decoder.position() != payload.len() as u64 {
        return Err("msgpack frame contains trailing bytes".into());
    }
    Ok(data)
}

/// Legacy Python wire helpers, distinct from the MessagePack codec above.
pub use super::python_wire::{
    from_owned_python_wire_with_copy_context, from_python_wire, from_python_wire_with_copy_context,
    to_python_wire, to_python_wire_with_copy_context, WireDataType,
};
/// Re-export text-channel helpers that were previously defined in
/// the hand-crafted IPC `RuntimeData`. Keeps these names reachable
/// via the existing import path.
pub use remotemedia_types::{split_text_str, tag_text_str, TEXT_CHANNEL_DEFAULT};

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
            pixel_data: vec![1, 2, 3, 4, 5].into(),
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
            pixel_data: vec![0xff, 0xfe, 0xfd].into(),
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
            data: vec![0xff, 0xd8, 0xff, 0xe0].into(),
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
            data: vec![0; 64].into(),
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
        roundtrip(RuntimeData::Binary(vec![0, 1, 2, 3, 255].into()));
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
            data: vec![0u8; 16].into(),
            shape: vec![2, 2, 2, 2],
            dtype: 0,
            metadata: None,
        });
    }

    #[test]
    fn tensor_with_metadata_roundtrips() {
        roundtrip(RuntimeData::Tensor {
            data: vec![1u8; 8].into(),
            shape: vec![2, 4],
            dtype: 1,
            metadata: Some(serde_json::json!({"layer": "embedding"})),
        });
    }

    #[test]
    fn numpy_roundtrips() {
        roundtrip(RuntimeData::Numpy {
            data: vec![0u8; 32].into(),
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
