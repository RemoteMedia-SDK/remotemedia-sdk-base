//! Data marshaling between Python and Rust types.
//!
//! This module provides conversion functions for:
//! - Python objects → Rust serde_json::Value
//! - Rust serde_json::Value → Python objects
//! - RuntimeData ↔ Python objects (Feature 011)

use base64::Engine;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use pyo3::IntoPyObjectExt;
use remotemedia_core::data::RuntimeData;
use remotemedia_types::CopyContext;
#[cfg(test)]
#[path = "marshal/buffer_tests.rs"]
mod buffer_tests;
mod buffers;
pub(crate) use buffers::storage::{materialize, view as payload_view};
#[cfg(test)]
#[path = "marshal/control_tests.rs"]
mod control_tests;
#[cfg(test)]
#[path = "marshal/json_tests.rs"]
mod json_tests;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Cache for Python objects to avoid unnecessary serialization
/// when passing data between Python nodes
#[derive(Clone)]
pub struct PyObjectCache {
    objects: Arc<Mutex<HashMap<String, Py<PyAny>>>>,
    next_id: Arc<Mutex<u64>>,
}

impl PyObjectCache {
    pub fn new() -> Self {
        Self {
            objects: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(Mutex::new(0)),
        }
    }

    /// Store a Python object and return its cache ID
    pub fn store(&self, obj: Py<PyAny>) -> String {
        let mut next_id = self.next_id.lock().unwrap();
        let id = format!("pyobj_{}", *next_id);
        *next_id += 1;

        let mut objects = self.objects.lock().unwrap();
        objects.insert(id.clone(), obj);

        tracing::info!("Stored Python object in cache with ID: {}", id);
        id
    }

    /// Retrieve a Python object by ID
    pub fn get(&self, id: &str) -> Option<Py<PyAny>> {
        let objects = self.objects.lock().unwrap();
        Python::attach(|py| objects.get(id).map(|obj| obj.clone_ref(py)))
    }

    /// Check if an ID exists in the cache
    pub fn contains(&self, id: &str) -> bool {
        let objects = self.objects.lock().unwrap();
        objects.contains_key(id)
    }

    /// Clear all cached objects
    pub fn clear(&self) {
        let mut objects = self.objects.lock().unwrap();
        objects.clear();
        tracing::info!("Cleared Python object cache");
    }
}

/// Convert a Python object to a JSON Value
///
/// Supports:
/// - None → Null
/// - bool → Bool
/// - int → Number
/// - float → Number
/// - str → String
/// - list → Array
/// - tuple → Array (JSON doesn't distinguish tuples from lists)
/// - dict → Object
///
/// If a cache is provided, complex objects that can't be serialized to JSON
/// will be stored in the cache and a reference will be returned instead of pickling.
pub fn python_to_json(py: Python, obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    python_to_json_with_context(py, obj, None, CopyContext::legacy())
}

/// Convert with optional cache support
pub fn python_to_json_with_cache(
    py: Python,
    obj: &Bound<'_, PyAny>,
    cache: Option<&PyObjectCache>,
) -> PyResult<Value> {
    python_to_json_with_context(py, obj, cache, CopyContext::legacy())
}

pub fn python_to_json_with_context(
    py: Python,
    obj: &Bound<'_, PyAny>,
    cache: Option<&PyObjectCache>,
    context: &CopyContext,
) -> PyResult<Value> {
    // None
    if obj.is_none() {
        return Ok(Value::Null);
    }

    // Check before scalar extraction: NumPy permits conversion of some
    // singleton arrays to scalars, which would discard their shape/dtype.
    #[cfg(feature = "native-numpy")]
    if crate::numpy_bridge::is_numpy_array(py, obj) {
        return crate::numpy_bridge::numpy_to_json_with_context(py, obj, context);
    }

    // Boolean (check before int, as bool is subclass of int in Python)
    if let Ok(val) = obj.extract::<bool>() {
        return Ok(Value::Bool(val));
    }

    // Integer
    if let Ok(val) = obj.extract::<i64>() {
        return Ok(Value::Number(val.into()));
    }

    // Float
    if let Ok(val) = obj.extract::<f64>() {
        if let Some(num) = serde_json::Number::from_f64(val) {
            return Ok(Value::Number(num));
        }
        // If float is NaN or Inf, convert to null
        return Ok(Value::Null);
    }

    // String
    if let Ok(val) = obj.extract::<String>() {
        return Ok(Value::String(val));
    }

    // List
    if let Ok(list) = obj.downcast::<PyList>() {
        let mut vec = Vec::new();
        for item in list.iter() {
            vec.push(python_to_json_with_context(py, &item, cache, context)?);
        }
        return Ok(Value::Array(vec));
    }

    // Tuple (convert to array, as JSON doesn't have tuples)
    if let Ok(tuple) = obj.downcast::<PyTuple>() {
        let mut vec = Vec::new();
        for item in tuple.iter() {
            vec.push(python_to_json_with_context(py, &item, cache, context)?);
        }
        return Ok(Value::Array(vec));
    }

    // Dict
    if let Ok(dict) = obj.downcast::<PyDict>() {
        let mut map = serde_json::Map::new();
        for (key, value) in dict.iter() {
            let key_str = key.extract::<String>()?;
            map.insert(
                key_str,
                python_to_json_with_context(py, &value, cache, context)?,
            );
        }
        return Ok(Value::Object(map));
    }

    // Unsupported type - try to cache it if cache is available, otherwise pickle
    tracing::info!(
        "Complex Python object encountered: {}",
        obj.get_type().name()?
    );

    if let Some(cache) = cache {
        // Cache is available - store object and return reference instead of pickling
        tracing::info!(
            "Caching {} object instead of pickling",
            obj.get_type().name()?
        );
        let py_copy = obj.clone().unbind();
        let cache_id = cache.store(py_copy);

        let mut ref_obj = serde_json::Map::new();
        ref_obj.insert("__pyobj__".to_string(), Value::Bool(true));
        ref_obj.insert("id".to_string(), Value::String(cache_id));
        ref_obj.insert(
            "type".to_string(),
            Value::String(obj.get_type().name()?.to_string()),
        );

        return Ok(Value::Object(ref_obj));
    }

    // An opaque serializer has no inspectable payload contract. Strict mode
    // cannot grant permission based on an unknown byte count or invoke reducers.
    if context.policy() == remotemedia_types::CopyPolicy::RequireZeroCopy {
        return Err(pyo3::exceptions::PyBufferError::new_err(
            "opaque Python serialization is unsupported under require_zero_copy",
        ));
    }

    // No cache - must pickle
    tracing::info!(
        "No cache available, attempting to pickle {}",
        obj.get_type().name()?
    );

    // Try cloudpickle first (handles more types like Cython objects), fall back to pickle
    let pickled_bytes = match py.import("cloudpickle") {
        Ok(cloudpickle) => {
            tracing::info!(
                "Using cloudpickle for serialization of {}",
                obj.get_type().name()?
            );
            match cloudpickle.call_method1("dumps", (obj,)) {
                Ok(bytes) => bytes,
                Err(e) => {
                    tracing::error!("CloudPickle failed for {}: {}", obj.get_type().name()?, e);
                    return Err(e);
                }
            }
        }
        Err(_) => {
            tracing::info!("cloudpickle not available, using standard pickle");
            let pickle = py.import("pickle")?;
            pickle.call_method1("dumps", (obj,))?
        }
    };

    let bytes: &[u8] = pickled_bytes.extract()?;

    // Base64 encode the pickled bytes
    let base64_encoded = base64::engine::general_purpose::STANDARD.encode(bytes);

    // Store as a special JSON object that signals this is pickled data
    let mut map = serde_json::Map::new();
    map.insert("__pickled__".to_string(), Value::Bool(true));
    map.insert("data".to_string(), Value::String(base64_encoded));
    map.insert(
        "type".to_string(),
        Value::String(obj.get_type().name()?.to_string()),
    );

    tracing::info!(
        "Successfully pickled object of type: {}",
        obj.get_type().name()?
    );
    Ok(Value::Object(map))
}

/// Convert a JSON Value to a Python object
///
/// Supports:
/// - Null → None
/// - Bool → bool
/// - Number → int or float
/// - String → str
/// - Array → list
/// - Object → dict
pub fn json_to_python<'py>(py: Python<'py>, value: &Value) -> PyResult<Bound<'py, PyAny>> {
    json_to_python_with_cache(py, value, None)
}

/// Convert with optional cache support for __pyobj__ references
pub fn json_to_python_with_cache<'py>(
    py: Python<'py>,
    value: &Value,
    cache: Option<&PyObjectCache>,
) -> PyResult<Bound<'py, PyAny>> {
    json_to_python_with_context(py, value, cache, CopyContext::legacy())
}

pub fn json_to_python_with_context<'py>(
    py: Python<'py>,
    value: &Value,
    cache: Option<&PyObjectCache>,
    context: &CopyContext,
) -> PyResult<Bound<'py, PyAny>> {
    match value {
        Value::Null => Ok(py.None().into_bound(py)),

        Value::Bool(b) => Ok(b.into_bound_py_any(py)?),

        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_bound_py_any(py)?)
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_bound_py_any(py)?)
            } else {
                // Fallback for u64 or other edge cases
                Ok(py.None().into_bound(py))
            }
        }

        Value::String(s) => Ok(s.into_bound_py_any(py)?),

        Value::Array(arr) => {
            let py_list = PyList::empty(py);
            for item in arr {
                let py_item = json_to_python_with_context(py, item, cache, context)?;
                py_list.append(py_item)?;
            }
            Ok(py_list.into_any())
        }

        Value::Object(obj) => {
            // Check if this is a numpy array (Phase 1.7.3) - only with native-numpy feature
            #[cfg(feature = "native-numpy")]
            {
                if crate::numpy_bridge::is_numpy_json(value) {
                    tracing::info!("Converting JSON back to numpy array");
                    return crate::numpy_bridge::json_to_numpy_with_context(py, value, context);
                }
            }

            // Check if this is a PyObject cache reference (Phase 1.10)
            if obj
                .get("__pyobj__")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                if let Some(cache) = cache {
                    if let Some(Value::String(id)) = obj.get("id") {
                        tracing::info!("Retrieving PyObject reference from cache: {}", id);
                        if let Some(cached_obj) = cache.get(id) {
                            return Ok(cached_obj.into_bound(py));
                        } else {
                            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                                "PyObject reference {} not found in cache",
                                id
                            )));
                        }
                    }
                }
                // No cache or invalid reference - return as regular dict
                tracing::warn!("__pyobj__ reference found but cache not available or ID missing");
            }

            // Check if this is a pickled Python object (Phase 1.7.4)
            if obj
                .get("__pickled__")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
            {
                if context.policy() == remotemedia_types::CopyPolicy::RequireZeroCopy {
                    return Err(pyo3::exceptions::PyBufferError::new_err(
                        "opaque Python deserialization is unsupported under require_zero_copy",
                    ));
                }
                if let Some(Value::String(base64_data)) = obj.get("data") {
                    tracing::info!(
                        "Unpickling Python object: {}",
                        obj.get("type")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown")
                    );

                    // Base64 decode
                    let pickled_bytes = base64::engine::general_purpose::STANDARD
                        .decode(base64_data)
                        .map_err(|e| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                                "Failed to decode base64: {}",
                                e
                            ))
                        })?;

                    // Unpickle using Python's pickle module
                    let pickle = py.import("pickle")?;
                    let py_bytes = pyo3::types::PyBytes::new(py, &pickled_bytes);
                    let unpickled = pickle.call_method1("loads", (py_bytes,))?;

                    tracing::info!("Successfully unpickled object");
                    return Ok(unpickled);
                }
            }

            // Regular dict - recursively convert values (may contain __pyobj__ references)
            let py_dict = PyDict::new(py);
            for (key, value) in obj {
                py_dict.set_item(key, json_to_python_with_context(py, value, cache, context)?)?;
            }
            Ok(py_dict.into_any())
        }
    }
}

/// Convert Rust RuntimeData to Python object for passing to Node instances
///
/// # Arguments
///
/// * `py` - Python GIL token
/// * `data` - RuntimeData to convert
///
/// # Returns
///
/// `PyResult<PyObject>` - Python representation of the data
///
/// # Example
///
/// ```
/// use pyo3::prelude::*;
/// use remotemedia_core::data::RuntimeData;
/// use remotemedia_ffi::marshal::runtime_data_to_python;
///
/// Python::attach(|py| {
///     let text_data = RuntimeData::Text("hello".into());
///     let py_obj = runtime_data_to_python(py, &text_data).unwrap();
///     // py_obj is now a Python string "hello"
/// });
/// ```
pub fn runtime_data_to_python(py: Python<'_>, data: &RuntimeData) -> PyResult<Py<PyAny>> {
    runtime_data_to_python_with_context(py, data, CopyContext::legacy())
}

/// Consume input storage so legacy Vec audio can be promoted without copying.
pub fn runtime_data_into_python(py: Python<'_>, data: RuntimeData) -> PyResult<Py<PyAny>> {
    runtime_data_to_python(py, &data.into_shared())
}

/// Export JSON metadata without cloning embedded encoded payload strings.
fn json_runtime_output(py: Python<'_>, v: &Value, context: &CopyContext) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("type", "json")?;

    let metadata_dict = PyDict::new(py);
    if let Some(obj) = v.as_object() {
        for (key, value) in obj
            .iter()
            .filter(|(key, _)| key.starts_with("participant."))
        {
            if let Some(text) = value.as_str() {
                metadata_dict.set_item(key, text)?;
            }
        }
    }
    let value = if let Some(obj) = v.as_object().filter(|obj| {
        let numpy = cfg!(feature = "native-numpy")
            && obj.get("__numpy__").and_then(Value::as_bool) == Some(true);
        let pickle = obj.get("__pickled__").and_then(Value::as_bool) == Some(true)
            && obj.get("data").and_then(Value::as_str).is_some();
        !numpy && !pickle
    }) {
        let fields = PyDict::new(py);
        for (key, value) in obj
            .iter()
            .filter(|(key, _)| !key.starts_with("participant."))
        {
            fields.set_item(key, json_to_python_with_context(py, value, None, context)?)?;
        }
        fields.into_any()
    } else {
        json_to_python_with_context(py, v, None, context)?
    };
    dict.set_item("value", value)?;
    if !metadata_dict.is_empty() {
        dict.set_item("metadata", metadata_dict)?;
    }
    Ok(dict.into())
}

pub fn runtime_data_to_python_with_context(
    py: Python<'_>,
    data: &RuntimeData,
    context: &CopyContext,
) -> PyResult<Py<PyAny>> {
    if let Some(value) = buffers::export(py, data, context)? {
        return Ok(value);
    }
    // T007: Convert RuntimeData to Python objects
    match data {
        RuntimeData::Text(s) => Ok(s.into_py_any(py)?),
        RuntimeData::Json(v) => json_runtime_output(py, v, context),
        RuntimeData::ControlMessage {
            message_type,
            segment_id,
            timestamp_ms,
            metadata,
        } => {
            let dict = PyDict::new(py);
            dict.set_item("type", "control_message")?;
            let kind = serde_json::to_value(message_type)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
            dict.set_item("message_type", buffers::metadata_to_python(py, &kind)?)?;
            dict.set_item("segment_id", segment_id)?;
            dict.set_item("timestamp_ms", timestamp_ms)?;
            dict.set_item("metadata", buffers::metadata_to_python(py, metadata)?)?;
            Ok(dict.into())
        }
        RuntimeData::File {
            path,
            filename,
            mime_type,
            size,
            offset,
            length,
            stream_id,
        } => {
            // Convert file reference to Python dict (Spec 001)
            let dict = PyDict::new(py);
            dict.set_item("type", "file")?;
            dict.set_item("path", path)?;
            if let Some(f) = filename {
                dict.set_item("filename", f)?;
            }
            if let Some(m) = mime_type {
                dict.set_item("mime_type", m)?;
            }
            if let Some(s) = size {
                dict.set_item("size", s)?;
            }
            if let Some(o) = offset {
                dict.set_item("offset", o)?;
            }
            if let Some(l) = length {
                dict.set_item("length", l)?;
            }
            if let Some(sid) = stream_id {
                dict.set_item("stream_id", sid)?;
            }
            Ok(dict.into())
        }
        _ => unreachable!("bulk variants handled by owning exporter"),
    }
}

/// Convert Python object to Rust RuntimeData for pipeline processing
///
/// # Arguments
///
/// * `py` - Python GIL token
/// * `obj` - Python object to convert
///
/// # Returns
///
/// `PyResult<RuntimeData>` - RuntimeData variant
///
/// # Example
///
/// ```
/// use pyo3::prelude::*;
/// use remotemedia_core::data::RuntimeData;
/// use remotemedia_ffi::marshal::python_to_runtime_data;
///
/// Python::with_gil(|py| {
///     let py_str = "hello".into_pyobject(py).unwrap();
///     let runtime_data = python_to_runtime_data(py, py_str.as_ref()).unwrap();
///     assert!(matches!(runtime_data, RuntimeData::Text(_)));
/// });
/// ```
pub fn python_to_runtime_data(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<RuntimeData> {
    python_to_runtime_data_with_context(py, obj, CopyContext::legacy())
}

pub fn python_to_runtime_data_with_context(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    context: &CopyContext,
) -> PyResult<RuntimeData> {
    if let Some(value) = buffers::import(py, obj, context)? {
        return Ok(value);
    }
    // T008: Convert Python objects to RuntimeData

    // Check if it's a dict with "type" field (structured data)
    if let Ok(dict) = obj.downcast::<PyDict>() {
        if let Ok(Some(type_val)) = dict.get_item("type") {
            let type_str: String = type_val.extract()?;
            match type_str.as_str() {
                "control_message" => return buffers::control(py, dict),
                "text" => {
                    let data: String = dict
                        .get_item("data")?
                        .ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "Text data missing 'data' field",
                            )
                        })?
                        .extract()?;
                    return Ok(RuntimeData::Text(data));
                }
                "json" => {
                    let value_obj = dict.get_item("value")?.ok_or_else(|| {
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Json data missing 'value' field",
                        )
                    })?;
                    let mut json_val =
                        if context.policy() == remotemedia_types::CopyPolicy::RequireZeroCopy {
                            buffers::metadata_json(py, &value_obj)?
                        } else {
                            python_to_json_with_context(py, &value_obj, None, context)?
                        };
                    if let Ok(Some(metadata_val)) = dict.get_item("metadata") {
                        if let Ok(metadata_dict) = metadata_val.downcast::<PyDict>() {
                            if let Some(obj) = json_val.as_object_mut() {
                                for (k, v) in metadata_dict {
                                    if let Ok(k_str) = k.extract::<String>() {
                                        if let Ok(v_str) = v.extract::<String>() {
                                            obj.insert(k_str, serde_json::Value::String(v_str));
                                        }
                                    }
                                }
                            }
                        }
                    }
                    return Ok(RuntimeData::Json(json_val));
                }
                "file" => {
                    // Spec 001: File reference support
                    let path: String = dict
                        .get_item("path")?
                        .ok_or_else(|| {
                            PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "File data missing 'path' field",
                            )
                        })?
                        .extract()?;
                    let filename: Option<String> =
                        dict.get_item("filename")?.and_then(|v| v.extract().ok());
                    let mime_type: Option<String> =
                        dict.get_item("mime_type")?.and_then(|v| v.extract().ok());
                    let size: Option<u64> = dict.get_item("size")?.and_then(|v| v.extract().ok());
                    let offset: Option<u64> =
                        dict.get_item("offset")?.and_then(|v| v.extract().ok());
                    let length: Option<u64> =
                        dict.get_item("length")?.and_then(|v| v.extract().ok());
                    let stream_id: Option<String> =
                        dict.get_item("stream_id")?.and_then(|v| v.extract().ok());
                    return Ok(RuntimeData::File {
                        path,
                        filename,
                        mime_type,
                        size,
                        offset,
                        length,
                        stream_id,
                    });
                }
                _ => {
                    // Unknown type - fall through to JSON conversion
                }
            }
        }
    }

    // Try as string
    if let Ok(s) = obj.extract::<String>() {
        return Ok(RuntimeData::Text(s));
    }

    // Default: Convert to JSON
    let json_val = if context.policy() == remotemedia_types::CopyPolicy::RequireZeroCopy {
        buffers::metadata_json(py, obj)?
    } else {
        python_to_json_with_context(py, obj, None, context)?
    };
    Ok(RuntimeData::Json(json_val))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_python_to_json_primitives() {
        Python::attach(|py| {
            // None
            let py_none = py.None().into_bound(py);
            assert_eq!(python_to_json(py, &py_none).unwrap(), Value::Null);

            // Boolean
            let py_bool = true.into_bound_py_any(py).unwrap();
            assert_eq!(python_to_json(py, &py_bool).unwrap(), Value::Bool(true));

            // Integer
            let py_int = 42.into_bound_py_any(py).unwrap();
            assert_eq!(python_to_json(py, &py_int).unwrap(), Value::from(42));

            // Float
            let py_float = 3.14.into_bound_py_any(py).unwrap();
            let result = python_to_json(py, &py_float).unwrap();
            assert!(result.is_number());

            // String
            let py_str = "hello".into_bound_py_any(py).unwrap();
            assert_eq!(python_to_json(py, &py_str).unwrap(), Value::from("hello"));
        });
    }

    #[test]
    fn test_python_to_json_list() {
        Python::attach(|py| {
            let py_list = vec![1, 2, 3].into_bound_py_any(py).unwrap();
            let result = python_to_json(py, &py_list).unwrap();

            assert_eq!(
                result,
                Value::Array(vec![Value::from(1), Value::from(2), Value::from(3)])
            );
        });
    }

    #[test]
    fn test_python_to_json_dict() {
        Python::attach(|py| {
            let py_dict = [("name", "Alice"), ("city", "NYC")]
                .into_iter()
                .collect::<std::collections::HashMap<_, _>>()
                .into_bound_py_any(py)
                .unwrap();

            let result = python_to_json(py, &py_dict).unwrap();
            assert!(result.is_object());

            let obj = result.as_object().unwrap();
            assert_eq!(obj.get("name").unwrap(), &Value::from("Alice"));
            assert_eq!(obj.get("city").unwrap(), &Value::from("NYC"));
        });
    }

    #[test]
    fn test_json_to_python_primitives() {
        Python::attach(|py| {
            // Null
            let json_null = Value::Null;
            let py_obj = json_to_python(py, &json_null).unwrap();
            assert!(py_obj.is_none());

            // Boolean
            let json_bool = Value::Bool(true);
            let py_obj = json_to_python(py, &json_bool).unwrap();
            assert_eq!(py_obj.extract::<bool>().unwrap(), true);

            // Integer
            let json_int = Value::from(42);
            let py_obj = json_to_python(py, &json_int).unwrap();
            assert_eq!(py_obj.extract::<i64>().unwrap(), 42);

            // String
            let json_str = Value::from("hello");
            let py_obj = json_to_python(py, &json_str).unwrap();
            assert_eq!(py_obj.extract::<String>().unwrap(), "hello");
        });
    }

    #[test]
    fn test_json_to_python_collections() {
        Python::attach(|py| {
            // Array
            let json_array = Value::Array(vec![Value::from(1), Value::from(2), Value::from(3)]);
            let py_obj = json_to_python(py, &json_array).unwrap();
            assert_eq!(py_obj.extract::<Vec<i64>>().unwrap(), vec![1, 2, 3]);

            // Object
            let mut map = serde_json::Map::new();
            map.insert("x".to_string(), Value::from(10));
            map.insert("y".to_string(), Value::from(20));
            let json_obj = Value::Object(map);

            let py_obj = json_to_python(py, &json_obj).unwrap();
            let py_dict = py_obj.downcast::<PyDict>().unwrap();

            let x_value = py_dict.get_item("x").unwrap().unwrap();
            let y_value = py_dict.get_item("y").unwrap().unwrap();
            assert_eq!(x_value.extract::<i64>().unwrap(), 10);
            assert_eq!(y_value.extract::<i64>().unwrap(), 20);
        });
    }

    #[test]
    fn test_round_trip() {
        Python::attach(|py| {
            // Create complex Python structure
            let py_list = PyList::empty(py);
            py_list.append(1.into_bound_py_any(py).unwrap()).unwrap();
            py_list
                .append("test".into_bound_py_any(py).unwrap())
                .unwrap();
            py_list
                .append(vec![2, 3, 4].into_bound_py_any(py).unwrap())
                .unwrap();

            // Python → JSON
            let json_val = python_to_json(py, py_list.as_any()).unwrap();

            // JSON → Python
            let result = json_to_python(py, &json_val).unwrap();

            // Verify it's a list with correct length
            let result_list = result.downcast::<PyList>().unwrap();
            assert_eq!(result_list.len(), 3);
        });
    }

    #[test]
    fn test_tuple_conversion() {
        Python::attach(|py| {
            use pyo3::types::PyTuple;

            // Simple tuple
            let tuple = PyTuple::new(py, &[1, 2, 3]).unwrap();
            let json_val = python_to_json(py, tuple.as_any()).unwrap();
            assert_eq!(
                json_val,
                Value::Array(vec![Value::from(1), Value::from(2), Value::from(3)])
            );

            // Nested tuple
            let inner1 = PyTuple::new(py, &[1, 2]).unwrap();
            let inner2 = PyTuple::new(py, &[3, 4]).unwrap();
            let outer = PyTuple::new(py, &[&inner1, &inner2]).unwrap();
            let json_val = python_to_json(py, outer.as_any()).unwrap();
            assert!(json_val.is_array());
            assert_eq!(json_val.as_array().unwrap().len(), 2);

            // Empty tuple
            let empty_tuple = PyTuple::empty(py);
            let json_val = python_to_json(py, empty_tuple.as_any()).unwrap();
            assert_eq!(json_val, Value::Array(vec![]));
        });
    }
}
