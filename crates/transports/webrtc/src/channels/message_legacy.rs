//! Deprecated JSON/base64 framing with explicit payload materialization policy.
use super::{ChunkInfo, DataChannelDecodeError, DataChannelMessage};
use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use remotemedia_types::{CopyContext, CopyReason};
use serde::Deserialize;
use serde_json::value::RawValue;
use std::borrow::Cow;

#[cfg(test)]
#[path = "message_legacy_tests.rs"]
mod tests;

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(rename = "type")]
    kind: String,
    #[serde(borrow)]
    payload: &'a RawValue,
}

#[derive(Deserialize)]
struct LegacyChunk<'a> {
    stream_id: String,
    message_id: String,
    chunk_index: u32,
    total_chunks: Option<u32>,
    is_final: bool,
    content_type: Option<String>,
    data_type_hint: Option<String>,
    #[serde(borrow)]
    data: &'a RawValue,
}

pub(super) fn decode(
    wire: &[u8],
    copies: &CopyContext,
) -> Result<Option<DataChannelMessage>, DataChannelDecodeError> {
    let Ok(envelope) = serde_json::from_slice::<Envelope<'_>>(wire) else {
        return Ok(None);
    };
    let message = match envelope.kind.as_str() {
        "Json" => {
            let json: serde_json::Value = serde_json::from_str(envelope.payload.get())?;
            let action = json
                .get("action")
                .and_then(|a| a.as_str())
                .unwrap_or("control")
                .to_string();
            DataChannelMessage::Control {
                action,
                headers: Default::default(),
                json,
            }
        }
        "Text" => DataChannelMessage::Text {
            action: "text".into(),
            headers: Default::default(),
            text: serde_json::from_str(envelope.payload.get())?,
        },
        "Binary" => DataChannelMessage::binary(decode_payload(envelope.payload, copies)?),
        "RuntimeData" => {
            DataChannelMessage::runtime_data(decode_payload(envelope.payload, copies)?)
        }
        "Chunk" => {
            let chunk: LegacyChunk<'_> = serde_json::from_str(envelope.payload.get())?;
            DataChannelMessage::chunk(ChunkInfo {
                stream_id: chunk.stream_id,
                message_id: chunk.message_id,
                chunk_index: chunk.chunk_index,
                total_chunks: chunk.total_chunks,
                is_final: chunk.is_final,
                content_type: chunk.content_type,
                data_type_hint: chunk.data_type_hint,
                data: decode_payload(chunk.data, copies)?,
            })
        }
        _ => return Ok(None),
    };
    tracing::warn!("Decoded legacy JSON/base64 DataChannelMessage (deprecated!)");
    Ok(Some(message))
}

fn decode_payload(raw: &RawValue, copies: &CopyContext) -> Result<Bytes, DataChannelDecodeError> {
    // RawValue borrows the token independently of JSON field ordering. Inspect
    // escapes before asking serde_json to build its unescaping scratch/string.
    let token = raw.get();
    let encoded = if token.as_bytes().contains(&b'\\') {
        let operation = copies.begin(CopyReason::PortableDeserialization, token.len() as u64)?;
        let value: String = serde_json::from_str(token)?;
        operation.complete_bytes(value.len() as u64)?;
        Cow::Owned(value)
    } else {
        Cow::Borrowed(serde_json::from_str::<&str>(token)?)
    };
    // Exact for canonical padded base64; malformed inputs use a nonzero bound
    // and record a failed attempt. Empty payloads require no bulk materialization.
    let padding = encoded
        .bytes()
        .rev()
        .take_while(|b| *b == b'=')
        .take(2)
        .count();
    let bound = base64::decoded_len_estimate(encoded.len())
        .saturating_sub(padding)
        .max(usize::from(!encoded.is_empty()));
    let operation = copies.begin(CopyReason::PortableDeserialization, bound as u64)?;
    let decoded = STANDARD.decode(encoded.as_bytes())?;
    operation.complete_bytes(decoded.len() as u64)?;
    Ok(decoded.into())
}
