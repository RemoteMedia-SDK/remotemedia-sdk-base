//! The HTTP transport's wire format. Every frame crosses as protobuf
//! `remotemedia.v1.DataBuffer` (`remotemedia-wire`): audio as raw f32 LE
//! bytes, other bulk payloads as their own bytes, never as JSON.
//!
//! ```proto
//! // POST /execute  (content-type: application/x-protobuf)
//! message ExecuteRequest {
//!   string manifest_json = 1;          // the pipeline manifest (configuration)
//!   remotemedia.v1.DataBuffer input = 2;
//! }
//! // Response: one DataBuffer.
//!
//! // POST /stream/:id/input: one DataBuffer.
//!
//! // GET /stream/:id/output (content-type: application/x-protobuf; delimited=true):
//! // StreamEvent messages, each preceded by its varint length. An empty
//! // event (length 0) is a keepalive; `error` ends the stream.
//! message StreamEvent {
//!   oneof event {
//!     remotemedia.v1.DataBuffer frame = 1;
//!     string error = 2;
//!   }
//! }
//! ```
//!
//! Copy accounting: encoding counts the frame's bulk bytes as
//! `PortableSerialization`; decoding counts only audio samples as
//! `PortableDeserialization` (other payloads keep the received buffer).
use bytes::Bytes;
use prost::encoding::{encode_key, encode_varint, encoded_len_varint, key_len, WireType};
use prost::Message;
use remotemedia_core::{transport::TransportData, Error, Result};
use remotemedia_types::CopyContext;
use remotemedia_wire::{DataBuffer, PreparedFrame, WireError};

/// Content type of a request or response body holding one protobuf message.
pub(crate) const PROTOBUF: &str = "application/x-protobuf";
/// Content type of the output stream: varint-delimited `StreamEvent`s.
pub(crate) const PROTOBUF_STREAM: &str = "application/x-protobuf; delimited=true";

/// Whether a `content-type` header value names a protobuf body.
pub(crate) fn is_protobuf(content_type: &str) -> bool {
    let essence = content_type.split(';').next().unwrap_or("").trim();
    essence.eq_ignore_ascii_case(PROTOBUF) || essence.eq_ignore_ascii_case("application/protobuf")
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct ExecuteRequest {
    #[prost(string, tag = "1")]
    pub manifest_json: String,
    #[prost(message, optional, tag = "2")]
    pub input: Option<DataBuffer>,
}

#[derive(Clone, PartialEq, Message)]
pub(crate) struct StreamEvent {
    #[prost(oneof = "stream_event::Event", tags = "1, 2")]
    pub event: Option<stream_event::Event>,
}

pub(crate) mod stream_event {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Event {
        #[prost(message, tag = "1")]
        Frame(remotemedia_wire::DataBuffer),
        #[prost(string, tag = "2")]
        Error(String),
    }
}

/// Encode one frame as a `DataBuffer` body.
pub(crate) fn encode_frame(data: &TransportData, copies: &CopyContext) -> Result<Vec<u8>> {
    remotemedia_wire::encode_frame(&data.data, &data.metadata, data.sequence, copies)
        .map_err(wire_error)
}

/// Encode an `ExecuteRequest` body. The policy check happens before any
/// payload is touched; audio is written straight into the body.
pub(crate) fn encode_execute_request(
    manifest_json: &[u8],
    input: &TransportData,
    copies: &CopyContext,
) -> Result<Vec<u8>> {
    let frame = PreparedFrame::new(&input.data, &input.metadata, input.sequence, copies)
        .map_err(wire_error)?;
    let frame_len = frame.encoded_len();
    let mut body = Vec::with_capacity(
        2 * key_len(1)
            + encoded_len_varint(manifest_json.len() as u64)
            + manifest_json.len()
            + encoded_len_varint(frame_len as u64)
            + frame_len,
    );
    encode_key(1, WireType::LengthDelimited, &mut body);
    encode_varint(manifest_json.len() as u64, &mut body);
    body.extend_from_slice(manifest_json);
    encode_key(2, WireType::LengthDelimited, &mut body);
    encode_varint(frame_len as u64, &mut body);
    frame.append_to(&mut body).map_err(wire_error)?;
    Ok(body)
}

/// The frame a decoded `DataBuffer` carries.
pub(crate) fn frame_from_buffer(buffer: DataBuffer, copies: &CopyContext) -> Result<TransportData> {
    let frame = remotemedia_wire::data_buffer_into_frame(buffer, copies).map_err(wire_error)?;
    Ok(TransportData {
        data: frame.data,
        sequence: frame.sequence,
        metadata: frame.metadata,
    })
}

/// Decode a `DataBuffer` body.
#[cfg(test)]
pub(crate) fn decode_frame(bytes: Bytes, copies: &CopyContext) -> Result<TransportData> {
    frame_from_buffer(DataBuffer::decode(bytes).map_err(error)?, copies)
}

/// The length prefix and field header that put an encoded `DataBuffer` into
/// a delimited `StreamEvent`; the frame bytes follow unchanged.
pub(crate) fn frame_event_header(frame_len: usize) -> Bytes {
    let event_len = key_len(1) + encoded_len_varint(frame_len as u64) + frame_len;
    let mut header = Vec::with_capacity(encoded_len_varint(event_len as u64) + 1 + 10);
    encode_varint(event_len as u64, &mut header);
    encode_key(1, WireType::LengthDelimited, &mut header);
    encode_varint(frame_len as u64, &mut header);
    Bytes::from(header)
}

/// A delimited `StreamEvent` carrying `message` as an error.
pub(crate) fn error_event(message: &str) -> Bytes {
    StreamEvent {
        event: Some(stream_event::Event::Error(message.to_string())),
    }
    .encode_length_delimited_to_vec()
    .into()
}

/// A delimited empty `StreamEvent`: a keepalive.
pub(crate) const KEEPALIVE: &[u8] = &[0];

fn wire_error(error: WireError) -> Error {
    match error {
        WireError::Malformed(_) | WireError::Decode(_) => Error::InvalidData(error.to_string()),
        other => self::error(other),
    }
}

pub(crate) fn error(error: impl std::fmt::Display) -> Error {
    Error::Transport(format!("HTTP transport: {error}"))
}
