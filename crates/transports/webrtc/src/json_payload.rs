//! Shared policy-aware decoding of portable JSON/base64 payloads.
use crate::channels::DataChannelDecodeError;
use base64::{engine::general_purpose::STANDARD, Engine};
use bytes::Bytes;
use remotemedia_types::{CopyContext, CopyReason};
use serde_json::value::RawValue;
use std::borrow::Cow;

pub(crate) fn decode_base64(
    raw: &RawValue,
    copies: &CopyContext,
) -> Result<Bytes, DataChannelDecodeError> {
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
