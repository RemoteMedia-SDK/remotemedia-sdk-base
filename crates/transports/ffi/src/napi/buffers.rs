//! JavaScript buffers are mutable snapshots. Published runtime owners stay opaque.
mod input;
pub(super) use input::InputBuffer;

use napi::{Env, Error, JsBuffer, NapiRaw, NapiValue, Result};
use napi_derive::napi;
use remotemedia_types::{CopyContext, CopyPolicy, CopyReason, SharedBuffer, SharedBytes};
use std::sync::OnceLock;

/// Allocate final JS-owned storage, avoiding napi-rs's external-Vec fallback copy.
/// The caller must fill it before returning it to JS.
pub(super) fn output_buffer(env: Env, len: usize) -> Result<napi::bindgen_prelude::Buffer> {
    let output = if len == 0 {
        empty_buffer(env)?
    } else {
        env.create_buffer(len)?.into_raw()
    };
    // SAFETY: this is a fresh ordinary Node Buffer in the current callback
    // scope. No JS code runs and no alias escapes during the conversion.
    unsafe {
        <napi::bindgen_prelude::Buffer as napi::bindgen_prelude::FromNapiValue>::from_napi_value(
            env.raw(),
            output.raw(),
        )
    }
}

fn contexts() -> &'static [CopyContext; 2] {
    static CONTEXTS: OnceLock<[CopyContext; 2]> = OnceLock::new();
    CONTEXTS.get_or_init(|| {
        [
            CopyContext::new(CopyPolicy::AllowCopies, "nodejs-ffi-buffer"),
            CopyContext::new(CopyPolicy::RequireZeroCopy, "nodejs-ffi-buffer"),
        ]
    })
}

pub(super) fn materialize<T>(
    strict: Option<bool>,
    bytes: usize,
    operation: impl FnOnce() -> Result<T>,
) -> Result<T> {
    contexts()[usize::from(strict.unwrap_or(false))]
        .materialize_result(CopyReason::BorrowedInput, bytes as u64, operation)
        .map_err(|e| Error::from_reason(e.to_string()))?
}

/// Process-wide logical copies at the NAPI pipeline buffer boundary only.
#[napi]
pub fn nodejs_buffer_copy_report() -> Result<String> {
    serde_json::to_string(&serde_json::json!({
        "allow_copies": contexts()[0].report(),
        "require_zero_copy": contexts()[1].report(),
    }))
    .map_err(|e| Error::from_reason(e.to_string()))
}

pub(super) fn import_bytes(input: &[u8], strict: Option<bool>) -> Result<SharedBytes> {
    materialize(strict, input.len(), || Ok(input.to_vec().into()))
}

pub(super) fn import_audio(input: &[u8], strict: Option<bool>) -> Result<SharedBuffer<f32>> {
    if input.len() % 4 != 0 {
        return Err(Error::from_reason(
            "Buffer length must be multiple of 4 (f32 samples)",
        ));
    }
    materialize(strict, input.len(), || {
        Ok(input
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes(chunk.try_into().expect("four-byte chunk")))
            .collect::<Vec<_>>()
            .into())
    })
}

pub(super) fn export_bytes(env: Env, input: &[u8], strict: Option<bool>) -> Result<JsBuffer> {
    if input.is_empty() {
        return empty_buffer(env);
    }
    materialize(strict, input.len(), || {
        env.create_buffer_copy(input)
            .map(|buffer| buffer.into_raw())
    })
}

pub(super) fn export_audio(env: Env, input: &[f32], strict: Option<bool>) -> Result<JsBuffer> {
    if input.is_empty() {
        return empty_buffer(env);
    }
    let len = input
        .len()
        .checked_mul(4)
        .ok_or_else(|| Error::from_reason("audio byte length overflow"))?;
    materialize(strict, len, || {
        let mut output = env.create_buffer(len)?;
        for (sample, bytes) in input.iter().zip(output.as_mut().chunks_exact_mut(4)) {
            bytes.copy_from_slice(&sample.to_le_bytes());
        }
        Ok(output.into_raw())
    })
}

// napi-rs 2.16 Env::create_buffer[_copy](0) builds Vec::from_raw_parts
// from Node's null empty-data pointer. Create only the JS handle instead.
fn empty_buffer(env: Env) -> Result<JsBuffer> {
    let mut value = std::ptr::null_mut();
    // SAFETY: env is the current callback environment; value is a writable
    // out-parameter. No data pointer, Rust slice or external owner is created.
    let status =
        unsafe { napi::sys::napi_create_buffer(env.raw(), 0, std::ptr::null_mut(), &mut value) };
    if status != napi::sys::Status::napi_ok {
        return Err(Error::from_reason("failed to allocate empty JS buffer"));
    }
    // SAFETY: the successful Node-API call returned a Buffer in this environment.
    unsafe { JsBuffer::from_raw(env.raw(), value) }
}
