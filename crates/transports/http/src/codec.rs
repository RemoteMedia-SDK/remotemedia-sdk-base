//! JSON compatibility boundary. Counts logical payload bytes separately from
//! explicitly staged wire bytes; neither is physical-copy certification.
use remotemedia_core::{Error, Result};
use remotemedia_types::{CopyContext, CopyReason};
use serde::{de::DeserializeOwned, Serialize};

pub(crate) fn encode<T: Serialize>(
    value: &T,
    payload_bytes: usize,
    copies: &CopyContext,
) -> Result<Vec<u8>> {
    copies
        .materialize_result(
            CopyReason::PortableSerialization,
            payload_bytes as u64,
            || serde_json::to_vec(value),
        )
        .map_err(error)?
        .map_err(error)
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8], copies: &CopyContext) -> Result<T> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let value = copies.deserialize(&mut decoder).map_err(error)?;
    decoder.end().map_err(error)?;
    Ok(value)
}

pub(crate) fn error(error: impl std::fmt::Display) -> Error {
    Error::Transport(format!("HTTP compatibility transfer: {error}"))
}
