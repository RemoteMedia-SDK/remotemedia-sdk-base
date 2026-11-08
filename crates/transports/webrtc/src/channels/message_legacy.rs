//! Deprecated JSON/base64 framing with explicit payload materialization policy.
use super::{ChunkInfo, DataChannelDecodeError, DataChannelMessage};
use crate::json_payload::decode_base64 as decode_payload;
use remotemedia_types::CopyContext;
use serde::Deserialize;
use serde_json::value::RawValue;

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
