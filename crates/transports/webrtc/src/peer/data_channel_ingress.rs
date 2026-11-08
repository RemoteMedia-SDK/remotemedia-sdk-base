//! Owning conversion shared by envelope, raw compatibility and chunk ingress.
//! Copy authority covers legacy payload decoding, reassembly and audio element
//! conversion. Native receive buffering remains a separate boundary; this is
//! not full network strict enforcement.

use bytes::Bytes;
use prost::Message;
use remotemedia_core::data::RuntimeData;
use remotemedia_types::CopyContext;
use tracing::info;

use crate::channels::{DataChannelDecodeError, DataChannelMessage, DataChannelReassembler};
use crate::generated::DataBuffer;

pub(super) fn decode_pipeline_input(
    wire: Bytes,
    reassembler: &mut DataChannelReassembler,
    copies: &CopyContext,
) -> crate::Result<Option<RuntimeData>> {
    let buffer = match DataChannelMessage::decode_owned_with_context(wire.clone(), copies) {
        Ok(DataChannelMessage::RuntimeData { data_buffer, .. }) => data_buffer,
        Ok(DataChannelMessage::Chunk { chunk, .. }) => {
            let Some(buffer) = reassembler
                .feed_chunk_with_context(&chunk, copies)
                .map_err(anyhow::Error::from)?
            else {
                return Ok(None);
            };
            buffer
        }
        Ok(DataChannelMessage::Control { action, json, .. }) => {
            info!(%action, %json, "Received control message on data-plane channel");
            return Ok(None);
        }
        Ok(DataChannelMessage::Text { action, text, .. }) => {
            info!(%action, %text, "Received text message on data-plane channel");
            return Ok(None);
        }
        Ok(DataChannelMessage::Binary { action, data, .. }) => {
            info!(%action, bytes = data.len(), "Received binary message on data-plane channel");
            return Ok(None);
        }
        // Raw DataBuffer compatibility is decoded once, through the same owning
        // path as an envelope. Do not decode-and-discard merely to detect it.
        Err(
            DataChannelDecodeError::Protobuf(_)
            | DataChannelDecodeError::InvalidKind(_)
            | DataChannelDecodeError::MissingPayload(_),
        ) => wire,
        // Policy, accounting and recognized legacy payload errors are terminal.
        // A raw-format retry must never turn a rejected operation into success.
        Err(error) => return Err(anyhow::Error::from(error).into()),
    };
    let decoded = DataBuffer::decode(buffer)
        .map_err(|error| crate::Error::InvalidData(format!("DataBuffer decode: {error}")))?;
    let runtime = crate::adapters::data_buffer_into_runtime_data_with_context(decoded, copies)
        .map_err(anyhow::Error::from)?
        .ok_or_else(|| crate::Error::InvalidData("unsupported or malformed DataBuffer".into()))?;
    Ok(Some(runtime))
}

#[cfg(test)]
mod tests;
