//! Explicit compatibility codec. Base64 is a materialization, never a lease.
use super::{extract_numpy_metadata, NumpyArrayMeta};
use crate::marshal::materialize;
use base64::{engine::general_purpose::STANDARD, Engine};
use pyo3::{exceptions::PyValueError, prelude::*, pybacked::PyBackedBytes, types::PyBytes};
use remotemedia_types::{CopyContext, CopyReason, PayloadStorage, SharedBytes};
use serde_json::Value;

fn error(message: impl std::fmt::Display) -> PyErr {
    PyValueError::new_err(message.to_string())
}

fn itemsize(dtype: &Bound<'_, PyAny>) -> PyResult<usize> {
    if dtype.getattr("hasobject")?.extract::<bool>()?
        || !dtype.getattr("fields")?.is_none()
        || !dtype.getattr("subdtype")?.is_none()
    {
        return Err(error("JSON NumPy codec requires a plain non-object dtype"));
    }
    let size = dtype.getattr("itemsize")?.extract()?;
    if size == 0 {
        return Err(error("zero-size NumPy dtype is unsupported"));
    }
    Ok(size)
}

pub(super) fn encode(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    context: &CopyContext,
) -> PyResult<Value> {
    if !obj.get_type().is(&py.import("numpy")?.getattr("ndarray")?) {
        return Err(error("JSON NumPy input must be an exact numpy.ndarray"));
    }
    let dtype = obj.getattr("dtype")?;
    itemsize(&dtype)?;
    let mut meta = extract_numpy_metadata(py, obj)?;
    // dtype.name discards endian and datetime units. The plain dtype string
    // preserves both; structured/subarray types require a different schema.
    meta.dtype = dtype.getattr("str")?.extract()?;
    let size = obj.getattr("nbytes")?.extract()?;
    let retained = if meta.c_contiguous {
        // A byte view preserves the same storage and supports dtypes (such as
        // datetime) that cannot export their own Python buffer format.
        let view = obj
            .call_method1("reshape", ((-1,),))?
            .call_method1("view", ("u1",))?;
        _remotemedia_buffers::recover_bytes(&view).ok()
    } else {
        None
    };
    let bytes = match retained {
        Some(owner) => owner,
        None => materialize(context, CopyReason::BorrowedInput, size, || {
            let packed = obj
                .call_method1("tobytes", ("C",))?
                .cast_into::<PyBytes>()?;
            Ok(SharedBytes::from_owner(PyBackedBytes::from(packed)))
        })?,
    };
    let data = materialize(context, CopyReason::PortableSerialization, size, || {
        Ok(STANDARD.encode(bytes.as_ref()))
    })?;
    // The wire payload is packed C order even for strided/Fortran input.
    meta.c_contiguous = true;
    meta.f_contiguous =
        meta.shape.contains(&0) || meta.shape.iter().filter(|&&d| d > 1).count() <= 1;
    let mut array = serde_json::Map::new();
    array.insert("meta".into(), serde_json::to_value(meta).map_err(error)?);
    array.insert("data".into(), Value::String(data));
    let mut envelope = serde_json::Map::new();
    envelope.insert("__numpy__".into(), Value::Bool(true));
    envelope.insert("array".into(), Value::Object(array));
    Ok(Value::Object(envelope))
}

pub(super) fn decode<'py>(
    py: Python<'py>,
    value: &Value,
    context: &CopyContext,
) -> PyResult<Bound<'py, PyAny>> {
    let array = value
        .get("array")
        .ok_or_else(|| error("Missing 'array' field"))?;
    let meta: NumpyArrayMeta = serde_json::from_value(
        array
            .get("meta")
            .ok_or_else(|| error("Missing NumPy metadata"))?
            .clone(),
    )
    .map_err(error)?;
    let encoded = array
        .get("data")
        .and_then(Value::as_str)
        .ok_or_else(|| error("Missing base64 NumPy data"))?;
    let numpy = py.import("numpy")?;
    let dtype = numpy.call_method1("dtype", (&meta.dtype,))?;
    let width = itemsize(&dtype)?;
    let elements = meta
        .shape
        .iter()
        .try_fold(1usize, |n, d| n.checked_mul(*d))
        .ok_or_else(|| error("NumPy shape overflow"))?;
    if elements != meta.size {
        return Err(error("NumPy shape/size mismatch"));
    }
    let size = elements
        .checked_mul(width)
        .ok_or_else(|| error("NumPy byte size overflow"))?;
    let encoded_size =
        base64::encoded_len(size, true).ok_or_else(|| error("base64 size overflow"))?;
    if encoded.len() != encoded_size {
        return Err(error("NumPy payload length mismatch"));
    }
    let bytes = materialize(context, CopyReason::PortableDeserialization, size, || {
        let bytes = STANDARD.decode(encoded).map_err(error)?;
        if bytes.len() != size {
            return Err(error("NumPy decoded length mismatch"));
        }
        Ok(bytes)
    })?;
    // Move the decoder allocation into the existing owning buffer exporter.
    // frombuffer/reshape retain it; neither constructs a second payload copy.
    let view = crate::marshal::payload_view(py, PayloadStorage::Bytes(bytes.into()))?;
    numpy
        .call_method1("frombuffer", (view, dtype))?
        .call_method1("reshape", (meta.shape,))
}
