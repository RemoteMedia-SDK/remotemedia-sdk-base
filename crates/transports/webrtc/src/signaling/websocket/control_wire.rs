//! Portable control JSON with no full-payload serialization staging.
use base64::{engine::general_purpose::STANDARD, Engine};
use remotemedia_core::data::{split_text_str, RuntimeData};
use remotemedia_types::{CopyContext, CopyReason};
use serde::Deserialize;
use serde_json::{json, value::RawValue, Value};
use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
#[path = "control_wire_tests.rs"]
mod tests;

/// Parse routing metadata while retaining the raw payload token in the WS text.
#[derive(Deserialize)]
pub(super) struct Request<'a> {
    pub jsonrpc: String,
    pub method: String,
    #[serde(borrow)]
    pub params: &'a RawValue,
    pub id: Option<Value>,
}

impl Request<'_> {
    pub(super) fn into_owned(
        self,
    ) -> Result<crate::signaling::protocol::JsonRpcRequest, serde_json::Error> {
        Ok(crate::signaling::protocol::JsonRpcRequest {
            jsonrpc: self.jsonrpc,
            method: self.method,
            params: serde_json::from_str(self.params.get())?,
            id: self.id,
        })
    }
}

pub(super) fn coerce_payload(raw: &RawValue, copies: &CopyContext) -> Result<RuntimeData, String> {
    let token = raw.get();
    if token.starts_with('{') {
        let fields: BTreeMap<String, &RawValue> =
            serde_json::from_str(token).map_err(|e| e.to_string())?;
        if let Some(text) = fields.get("text").filter(|v| v.get().starts_with('"')) {
            return serde_json::from_str(text.get())
                .map(RuntimeData::Text)
                .map_err(|e| e.to_string());
        }
        if let Some(json) = fields.get("json") {
            return serde_json::from_str(json.get())
                .map(RuntimeData::Json)
                .map_err(|e| e.to_string());
        }
        if let Some(binary) = fields
            .get("binary_b64")
            .filter(|v| v.get().starts_with('"'))
        {
            return crate::json_payload::decode_base64(binary, copies)
                .map(|bytes| {
                    RuntimeData::Binary(remotemedia_types::SharedBuffer::from_owner(bytes))
                })
                .map_err(|e| format!("bad base64 in binary_b64: {e}"));
        }
    }
    match token.as_bytes().first() {
        Some(b'"') => serde_json::from_str(token)
            .map(RuntimeData::Text)
            .map_err(|e| e.to_string()),
        Some(b'{' | b'[') => serde_json::from_str(token)
            .map(RuntimeData::Json)
            .map_err(|e| e.to_string()),
        _ => Err("control.publish payload: expected {text|json|binary_b64} object".into()),
    }
}

/// Encode bulk bytes into their final JSON destination. Metadata-only audio
/// events never access sample storage and remain valid under strict policy.
pub(super) fn encode_event(
    topic: &str,
    data: &RuntimeData,
    include_audio: bool,
    copies: &CopyContext,
) -> Result<String, String> {
    let bulk = matches!(data, RuntimeData::Binary(_))
        || (include_audio && matches!(data, RuntimeData::Audio { .. }));
    let bytes = if bulk { data.bulk_payload_bytes() } else { 0 };
    let operation = copies
        .begin(CopyReason::PortableSerialization, bytes as u64)
        .map_err(|e| e.to_string())?;
    let mut params = event_params(topic, data);
    if !bulk {
        let wire = serde_json::to_string(
            &json!({"jsonrpc":"2.0", "method":"control.event", "params":params}),
        )
        .map_err(|e| e.to_string())?;
        operation.complete();
        return Ok(wire);
    }
    let payload = params
        .as_object_mut()
        .expect("event params object")
        .remove("payload")
        .unwrap();
    let mut wire = br#"{"jsonrpc":"2.0","method":"control.event","params":"#.to_vec();
    serde_json::to_writer(&mut wire, &params).map_err(|e| e.to_string())?;
    wire.pop(); // Extend the metadata object before any payload is written.
    wire.extend_from_slice(b",\"payload\":");
    serde_json::to_writer(&mut wire, &payload).map_err(|e| e.to_string())?;
    wire.pop();
    let field: &[u8] = if matches!(data, RuntimeData::Audio { .. }) {
        b",\"samples_b64\":\""
    } else {
        b",\"b64\":\""
    };
    wire.extend_from_slice(field);
    let offset = wire.len();
    let encoded_len = base64::encoded_len(bytes, true).ok_or("base64 output length overflow")?;
    let end = offset
        .checked_add(encoded_len)
        .ok_or("control output length overflow")?;
    let final_len = end.checked_add(4).ok_or("control output length overflow")?;
    wire.resize(final_len, 0); // All growth occurs before accessing the payload.
    if bytes != 0 {
        match data {
            RuntimeData::Binary(bytes) => {
                STANDARD
                    .encode_slice(bytes.as_ref(), &mut wire[offset..end])
                    .map_err(|e| e.to_string())?;
            }
            RuntimeData::Audio { samples, .. } => {
                let bytes = audio_bytes(samples.as_slice(), copies)?;
                STANDARD
                    .encode_slice(bytes.as_ref(), &mut wire[offset..end])
                    .map_err(|e| e.to_string())?;
            }
            _ => unreachable!("bulk variant selected above"),
        }
    }
    wire[end..].copy_from_slice(b"\"}}}");
    let result = String::from_utf8(wire).map_err(|e| e.to_string())?;
    operation.complete();
    Ok(result)
}

fn audio_bytes<'a>(
    samples: &'a [f32],
    _copies: &CopyContext,
) -> Result<std::borrow::Cow<'a, [u8]>, String> {
    #[cfg(target_endian = "little")]
    {
        Ok(std::borrow::Cow::Borrowed(bytemuck::cast_slice(samples)))
    }
    #[cfg(target_endian = "big")]
    {
        _copies
            .materialize(
                CopyReason::VecMaterialization,
                std::mem::size_of_val(samples) as u64,
                || std::borrow::Cow::Owned(samples.iter().flat_map(|s| s.to_le_bytes()).collect()),
            )
            .map_err(|e| e.to_string())
    }
}

fn event_params(topic: &str, data: &RuntimeData) -> Value {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    match data {
        RuntimeData::Text(s) => {
            // Strip the `\x00<len><channel>` routing header. The
            // browser sees only clean content, plus an explicit
            // `channel` field so it can decide where to render
            // (transcript line, markdown pane, etc).
            let (channel, content) = split_text_str(s);
            json!({
                "topic": topic,
                "kind": "text",
                "payload": content,
                "channel": channel,
                "ts": ts,
            })
        }
        RuntimeData::Json(v) => json!({
            "topic": topic,
            "kind": "json",
            "payload": v,
            "ts": ts,
        }),
        RuntimeData::Binary(b) => {
            json!({
                "topic": topic,
                "kind": "binary",
                "payload": {
                    "size": b.len(),
                },
                "ts": ts,
            })
        }
        RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            ..
        } => {
            // Audio for the browser rides the WebRTC audio track. We
            // surface a liveness envelope here (size + sample rate) so
            // UIs can still draw "audio active" meters without paying
            // to re-encode PCM as base64 every frame. Clients that
            // really want the samples pass `include_audio: true`.
            let payload = json!({
                "size": samples.len(),
                "sample_rate": sample_rate,
                "channels": channels,
            });
            json!({
                "topic": topic,
                "kind": "audio",
                "payload": payload,
                "ts": ts,
            })
        }
        other => json!({
            "topic": topic,
            "kind": "other",
            "payload": { "debug": format!("{:?}", std::mem::discriminant(other)) },
            "ts": ts,
        }),
    }
}
