//! Validate native backing storage after all JS argument conversion.
use napi::{Error, Result};

/// Callback-local input restricted to non-shared ArrayBuffer storage.
/// Inspect the native backing object before creating any Rust byte slice.
pub struct InputBuffer(InputSource);
enum InputSource {
    Js {
        env: napi::sys::napi_env,
        value: napi::sys::napi_value,
        length: usize,
    },
    #[cfg(test)]
    Owned(napi::bindgen_prelude::Buffer),
}

impl napi::bindgen_prelude::TypeName for InputBuffer {
    fn type_name() -> &'static str {
        "Buffer"
    }
    fn value_type() -> napi::ValueType {
        napi::ValueType::Object
    }
}
impl napi::bindgen_prelude::ValidateNapiValue for InputBuffer {
    unsafe fn validate(
        env: napi::sys::napi_env,
        value: napi::sys::napi_value,
    ) -> Result<napi::sys::napi_value> {
        // SAFETY: forwarded unchanged from napi-rs's argument validator.
        unsafe {
            <napi::bindgen_prelude::Buffer as napi::bindgen_prelude::ValidateNapiValue>::validate(
                env, value,
            )
        }
    }
}
impl napi::bindgen_prelude::FromNapiValue for InputBuffer {
    unsafe fn from_napi_value(
        env: napi::sys::napi_env,
        value: napi::sys::napi_value,
    ) -> Result<Self> {
        let mut length = 0;
        // SAFETY: inspect only metadata; another argument's getter may still
        // resize/detach this buffer before the Rust callback body begins.
        let status = unsafe {
            napi::sys::napi_get_buffer_info(env, value, std::ptr::null_mut(), &mut length)
        };
        if status != napi::sys::Status::napi_ok {
            return Err(Error::from_reason("expected Buffer input"));
        }
        Ok(Self(InputSource::Js { env, value, length }))
    }
}
impl InputBuffer {
    pub(crate) fn into_buffer(self) -> Result<napi::bindgen_prelude::Buffer> {
        let (env, value, length) = match self.0 {
            InputSource::Js { env, value, length } => (env, value, length),
            #[cfg(test)]
            InputSource::Owned(buffer) => return Ok(buffer),
        };
        let mut backing = std::ptr::null_mut();
        // SAFETY: callback env/value and writable output pointer are supplied by
        // napi-rs. Other outputs are optional; no payload bytes are read.
        let status = unsafe {
            napi::sys::napi_get_typedarray_info(
                env,
                value,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut backing,
                std::ptr::null_mut(),
            )
        };
        if status != napi::sys::Status::napi_ok {
            return Err(Error::from_reason(
                "input must be a Buffer with ordinary ArrayBuffer storage",
            ));
        }
        let mut ordinary = false;
        // SAFETY: backing came from the successful Node-API inspection.
        let status = unsafe { napi::sys::napi_is_arraybuffer(env, backing, &mut ordinary) };
        if status != napi::sys::Status::napi_ok || !ordinary {
            return Err(Error::from_reason(
                "SharedArrayBuffer inputs are unsupported; snapshot them explicitly",
            ));
        }
        // SAFETY: only non-shared storage reaches this conversion, on the JS
        // callback thread. Synchronous conversion makes no calls into JS.
        let buffer = unsafe {
            <napi::bindgen_prelude::Buffer as napi::bindgen_prelude::FromNapiValue>::from_napi_value(env, value)?
        };
        if buffer.len() != length {
            return Err(Error::from_reason(
                "input buffer changed length during argument conversion",
            ));
        }
        Ok(buffer)
    }
}
#[cfg(test)]
impl From<napi::bindgen_prelude::Buffer> for InputBuffer {
    fn from(value: napi::bindgen_prelude::Buffer) -> Self {
        Self(InputSource::Owned(value))
    }
}
