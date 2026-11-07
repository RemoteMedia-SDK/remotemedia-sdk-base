//! Bulk marshaling keeps storage separate from JSON-compatible metadata.
use super::*;
use remotemedia_types::{AudioSamples, PayloadStorage, RuntimeMetadata, TensorEncoding};

mod numpy;
pub(super) mod storage;

fn value_error(error: impl std::fmt::Display) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(error.to_string())
}

pub(super) fn metadata_json(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    let options = PyDict::new(py);
    options.set_item("allow_nan", false)?;
    let encoded: String = py
        .import("json")?
        .call_method("dumps", (obj,), Some(&options))?
        .extract()?;
    serde_json::from_str(&encoded).map_err(value_error)
}

fn bulk_kind(kind: &str) -> Option<(&'static str, &'static str)> {
    match kind {
        "audio" => Some(("Audio", "samples")),
        "video" => Some(("Video", "pixel_data")),
        "image" => Some(("Image", "data")),
        "tensor" => Some(("Tensor", "data")),
        "numpy" => Some(("Numpy", "data")),
        "binary" => Some(("Binary", "data")),
        _ => None,
    }
}

fn clone_for_export(data: &RuntimeData, context: &CopyContext) -> PyResult<RuntimeData> {
    if let RuntimeData::Audio {
        samples,
        sample_rate,
        channels,
        stream_id,
        timestamp_us,
        arrival_ts_us,
        metadata,
    } = data
    {
        let owner = match samples {
            AudioSamples::Shared(owner) => owner.clone(),
            AudioSamples::Arc(owner) => owner.clone().into(),
            AudioSamples::Vec(samples) => {
                storage::copy(context, samples.len() * 4, || Ok(samples.clone().into()))?
            }
        };
        return Ok(RuntimeData::Audio {
            samples: owner.into(),
            sample_rate: *sample_rate,
            channels: *channels,
            stream_id: stream_id.clone(),
            timestamp_us: *timestamp_us,
            arrival_ts_us: *arrival_ts_us,
            metadata: metadata.clone(),
        });
    }
    Ok(data.clone())
}

pub(super) fn export(
    py: Python<'_>,
    data: &RuntimeData,
    context: &CopyContext,
) -> PyResult<Option<Py<PyAny>>> {
    let Some((_, field)) = bulk_kind(data.data_type()) else {
        return Ok(None);
    };
    let (metadata, payload) = clone_for_export(data, context)?
        .into_shared_parts(TensorEncoding::Runtime)
        .map_err(value_error)?;
    let view = storage::view(py, payload)?;
    if matches!(metadata, RuntimeMetadata::Binary) {
        return Ok(Some(view));
    }
    if let RuntimeMetadata::Numpy {
        shape,
        dtype,
        strides,
        ..
    } = metadata
    {
        return numpy::export(py, view, shape, dtype, strides).map(Some);
    }
    let json = serde_json::to_value(metadata).map_err(value_error)?;
    let fields = json
        .as_object()
        .and_then(|map| map.values().next())
        .ok_or_else(|| value_error("bulk metadata is not an object"))?;
    let dict = json_to_python(py, fields)?.cast_into::<PyDict>()?;
    dict.set_item("type", data.data_type())?;
    dict.set_item(field, view)?;
    if let RuntimeData::Image { format, .. } = data {
        if let Some(mime) = format.mime_type() {
            dict.set_item("mime_type", mime)?;
        }
    }
    Ok(Some(dict.into_any().unbind()))
}

fn structured(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    kind: &str,
    variant: &str,
    field: &str,
    context: &CopyContext,
) -> PyResult<RuntimeData> {
    let input = dict
        .get_item(field)?
        .ok_or_else(|| value_error(format!("{kind} missing {field}")))?;
    let payload = if kind == "audio" {
        PayloadStorage::Audio(storage::audio(&input, context)?)
    } else {
        PayloadStorage::Bytes(storage::bytes(&input, context)?)
    };
    if kind == "binary" {
        return RuntimeMetadata::Binary
            .attach(payload, TensorEncoding::Runtime)
            .map_err(value_error);
    }
    let fields = dict.copy()?;
    fields.del_item("type")?;
    fields.del_item(field)?;
    if kind == "image" && fields.contains("mime_type")? {
        fields.del_item("mime_type")?;
    }
    if kind == "tensor" {
        fields.set_item("encoding", "Runtime")?;
    }
    // JSON's ordinary encoder rejects ndarray/buffer objects in metadata rather
    // than calling a payload materializer or stringifying hidden media.
    let fields = metadata_json(py, fields.as_any())?;
    let metadata: RuntimeMetadata =
        serde_json::from_value(serde_json::json!({variant: fields})).map_err(value_error)?;
    metadata
        .attach(payload, TensorEncoding::Runtime)
        .map_err(value_error)
}

pub(super) fn import(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    context: &CopyContext,
) -> PyResult<Option<RuntimeData>> {
    if crate::numpy_bridge::is_numpy_array(py, obj) {
        return numpy::import(py, obj, context).map(Some);
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        if let Some(kind) = dict.get_item("type")? {
            let kind: String = kind.extract()?;
            if let Some((variant, field)) = bulk_kind(&kind) {
                return structured(py, dict, &kind, variant, field, context).map(Some);
            }
        }
    }
    if storage::is_byte_input(obj) {
        return storage::bytes(obj, context)
            .map(RuntimeData::Binary)
            .map(Some);
    }
    Ok(None)
}
