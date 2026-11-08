//! Defer borrowed protobuf bulk storage until the final payload is validated.
//! Generated messages still decode metadata; Prost enforces wire types, nested
//! lengths, unknown fields and recursion limits. Offsets refer to the original
//! slice because Prost's message merger passes the same Buf through nested fields.
use super::*;
use bytes::{Buf, BufMut};
use prost::encoding::{self, DecodeContext, WireType};
use std::ops::Range;

pub(super) fn decode(
    bytes: &[u8],
    copies: &CopyContext,
) -> Result<DataChannelMessage, DataChannelDecodeError> {
    let (envelope, range) = match scan(bytes) {
        Ok(value) => value,
        Err(error) => {
            return legacy::decode(bytes, copies)?.ok_or(DataChannelDecodeError::Protobuf(error));
        }
    };
    let mut message = DataChannelMessage::from_envelope(envelope)?;
    let destination = match &mut message {
        DataChannelMessage::Binary { data, .. } => data,
        DataChannelMessage::RuntimeData { data_buffer, .. } => data_buffer,
        DataChannelMessage::Chunk { chunk, .. } => &mut chunk.data,
        _ => return Ok(message),
    };
    if let Some(range) = range {
        *destination = copies.materialize(CopyReason::BorrowedInput, range.len() as u64, || {
            Bytes::copy_from_slice(&bytes[range])
        })?;
    }
    Ok(message)
}

fn scan(bytes: &[u8]) -> Result<(DataChannelEnvelope, Option<Range<usize>>), prost::DecodeError> {
    let mut envelope = DataChannelEnvelope::default();
    let mut range = None;
    let mut input = bytes;
    while input.has_remaining() {
        let (tag, wire) = encoding::decode_key(&mut input)?;
        let ctx = DecodeContext::default();
        // A repeated oneof message of the same kind merges; changing kind drops
        // its old range. Unknown envelope fields leave the selected range intact.
        let replacement = match tag {
            12 => Payload::Binary(BinaryFrame::default()),
            13 => Payload::RuntimeData(RuntimeDataFrame::default()),
            14 => Payload::Chunk(ChunkFrame::default()),
            _ => {
                envelope.merge_field(tag, wire, &mut input, ctx)?;
                if matches!(tag, 10 | 11) {
                    range = None;
                }
                continue;
            }
        };
        if envelope.payload.as_ref().map(std::mem::discriminant)
            != Some(std::mem::discriminant(&replacement))
        {
            envelope.payload = Some(replacement);
            range = None;
        }
        match envelope.payload.as_mut() {
            Some(Payload::Binary(frame)) => {
                merge::<_, 1>(frame, wire, &mut input, &mut range, bytes.len())?
            }
            Some(Payload::RuntimeData(frame)) => {
                merge::<_, 1>(frame, wire, &mut input, &mut range, bytes.len())?
            }
            Some(Payload::Chunk(frame)) => {
                merge::<_, 8>(frame, wire, &mut input, &mut range, bytes.len())?
            }
            _ => unreachable!("bulk oneof selected above"),
        }
    }
    Ok((envelope, range))
}

fn merge<T: Message, const BULK_TAG: u32>(
    frame: &mut T,
    wire: WireType,
    input: &mut &[u8],
    range: &mut Option<Range<usize>>,
    wire_len: usize,
) -> Result<(), prost::DecodeError> {
    encoding::message::merge(
        wire,
        &mut Deferred::<_, BULK_TAG> {
            frame,
            range,
            wire_len,
        },
        input,
        DecodeContext::default(),
    )
}

#[derive(Debug)]
struct Deferred<'a, T, const BULK_TAG: u32> {
    frame: &'a mut T,
    range: &'a mut Option<Range<usize>>,
    wire_len: usize,
}

impl<T: Message, const BULK_TAG: u32> Message for Deferred<'_, T, BULK_TAG> {
    fn merge_field(
        &mut self,
        tag: u32,
        wire: WireType,
        input: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        if tag != BULK_TAG {
            return self.frame.merge_field(tag, wire, input, ctx);
        }
        encoding::check_wire_type(WireType::LengthDelimited, wire)?;
        let len = encoding::decode_varint(input)?;
        if len > input.remaining() as u64 {
            return Err(prost::DecodeError::new("bulk field exceeds remaining wire"));
        }
        let start = self.wire_len - input.remaining();
        *self.range = Some(start..start + len as usize);
        input.advance(len as usize);
        Ok(())
    }

    fn encode_raw(&self, output: &mut impl BufMut) {
        self.frame.encode_raw(output);
    }

    fn encoded_len(&self) -> usize {
        self.frame.encoded_len()
    }

    fn clear(&mut self) {
        self.frame.clear();
        *self.range = None;
    }
}
