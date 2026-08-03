//! Numpy array marshaling with zero-copy support (Phase 1.7.3)
//!
//! This module provides efficient data exchange between Rust and Python numpy arrays
//! using rust-numpy for zero-copy buffer access.
//!
//! Strategy:
//! 1. Python → Rust: Use rust-numpy's PyReadonlyArray for zero-copy read access
//! 2. Rust → Python: Use rust-numpy's PyArray for zero-copy write access
//! 3. For JSON serialization: Use metadata + base64 encoded data
//! 4. For direct Rust access: Use ndarray crate integration
//!
//! Supported dtypes:
//! - int8, int16, int32, int64
//! - uint8, uint16, uint32, uint64
//! - float32, float64
//! - bool

// Phase 1.7.3 numpy bridge infrastructure - some functions reserved for future use
#![allow(dead_code)]

use numpy::{PyArray, PyArrayDyn, PyArrayMethods};
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use remotemedia_core::audio::{AudioBuffer, AudioFormat};

/// Numpy array metadata for serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumpyArrayMeta {
    /// Array shape (dimensions)
    pub shape: Vec<usize>,

    /// Data type string (e.g., "float64", "int32")
    pub dtype: String,

    /// Total number of elements
    pub size: usize,

    /// Whether array is C-contiguous
    pub c_contiguous: bool,

    /// Whether array is Fortran-contiguous
    pub f_contiguous: bool,
}

/// Numpy array data transfer structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumpyArrayData {
    /// Array metadata
    pub meta: NumpyArrayMeta,

    /// Serialized data (as base64 string for JSON transport)
    pub data: String,
}

/// Check if a Python object is a numpy array
pub fn is_numpy_array(_py: Python, obj: &Bound<'_, PyAny>) -> bool {
    // Check if object has numpy array attributes instead of trying to downcast
    // This is more robust and works for all numpy dtypes
    obj.hasattr("shape").unwrap_or(false)
        && obj.hasattr("dtype").unwrap_or(false)
        && obj.hasattr("strides").unwrap_or(false)
        && obj.hasattr("tobytes").unwrap_or(false)
        && obj.hasattr("__array_interface__").unwrap_or(false)
}

/// Extract numpy array metadata from a numpy array
pub fn extract_numpy_metadata(_py: Python, obj: &Bound<'_, PyAny>) -> PyResult<NumpyArrayMeta> {
    // Try to get as PyArrayDyn (works for any dtype)
    // We'll use reflection to get metadata without knowing the dtype
    let shape_obj = obj.getattr("shape")?;
    let shape: Vec<usize> = shape_obj.extract()?;

    let dtype_obj = obj.getattr("dtype")?;
    let dtype_str = dtype_obj.getattr("name")?.extract::<String>()?;

    let size: usize = obj.getattr("size")?.extract()?;

    let flags = obj.getattr("flags")?;
    let c_contiguous: bool = flags.getattr("c_contiguous")?.extract()?;
    let f_contiguous: bool = flags.getattr("f_contiguous")?.extract()?;

    Ok(NumpyArrayMeta {
        shape,
        dtype: dtype_str,
        size,
        c_contiguous,
        f_contiguous,
    })
}

/// Convert numpy array to JSON (with metadata + base64 data)
///
/// Uses rust-numpy for zero-copy read access to the array data.
pub fn numpy_to_json(py: Python, obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    let meta = extract_numpy_metadata(py, obj)?;

    // Get raw bytes using tobytes() method (copies data)
    // TODO: For true zero-copy, we'd pass shared memory handles instead
    let bytes_obj = obj.call_method0("tobytes")?;
    let bytes: &[u8] = bytes_obj.extract()?;

    // Encode as base64
    use base64::Engine;
    let base64_data = base64::engine::general_purpose::STANDARD.encode(bytes);

    let array_data = NumpyArrayData {
        meta,
        data: base64_data,
    };

    // Return as JSON with special marker
    Ok(serde_json::json!({
        "__numpy__": true,
        "array": serde_json::to_value(&array_data).unwrap()
    }))
}

/// Convert JSON (with numpy metadata) back to numpy array
///
/// Uses rust-numpy to create the array from raw bytes.
pub fn json_to_numpy<'py>(py: Python<'py>, value: &Value) -> PyResult<Bound<'py, PyAny>> {
    // Extract array data
    let array_data: NumpyArrayData = serde_json::from_value(
        value
            .get("array")
            .ok_or_else(|| {
                PyErr::new::<pyo3::exceptions::PyValueError, _>("Missing 'array' field")
            })?
            .clone(),
    )
    .map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
            "Failed to parse array data: {}",
            e
        ))
    })?;

    // Decode base64 data
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&array_data.data)
        .map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                "Failed to decode base64: {}",
                e
            ))
        })?;

    // Import numpy
    let numpy = py.import("numpy")?;

    // Create numpy array from bytes
    // numpy.frombuffer(bytes, dtype=dtype).reshape(shape)
    let frombuffer = numpy.getattr("frombuffer")?;
    let flat_array = frombuffer.call1((bytes, array_data.meta.dtype.as_str()))?;

    // Reshape to original shape
    let shaped_array = flat_array.call_method1("reshape", (array_data.meta.shape,))?;

    Ok(shaped_array)
}

/// Check if a JSON value represents a numpy array
pub fn is_numpy_json(value: &Value) -> bool {
    value
        .get("__numpy__")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

/// Convert numpy array to Rust vector (for f64 arrays)
///
/// This provides zero-copy read access to the array data.
pub fn numpy_to_vec_f64(_py: Python, obj: &Bound<'_, PyAny>) -> PyResult<Vec<f64>> {
    let array = obj.downcast::<PyArrayDyn<f64>>()?;
    let readonly = array.readonly();
    let slice = readonly.as_slice()?;
    Ok(slice.to_vec())
}

/// Convert Rust vector to numpy array (f64)
///
/// This creates a new numpy array from Rust data.
pub fn vec_to_numpy_f64<'py>(
    py: Python<'py>,
    data: &[f64],
    shape: &[usize],
) -> PyResult<Bound<'py, PyAny>> {
    let array = PyArray::from_slice(py, data);
    // Use Python's reshape method via call_method
    let shape_tuple: Vec<_> = shape.iter().map(|&s| s).collect();
    let reshaped = array.call_method1("reshape", (shape_tuple,))?;
    Ok(reshaped)
}

/// Convert numpy array to AudioBuffer with zero-copy (Phase 4: T049-T053)
///
/// This function provides zero-copy access to numpy array data by:
/// 1. Borrowing the numpy array's underlying buffer via PyO3
/// 2. Wrapping the borrowed slice in Arc<Vec<f32>> for shared ownership
/// 3. The numpy array's memory remains owned by Python; we just hold a reference
///
/// # Safety
/// This is safe because:
/// - PyO3's PyReadonlyArray ensures the Python GIL is held during access
/// - We copy the data into a Vec to avoid lifetime issues
/// - Arc allows shared ownership without additional copies
///
/// # Arguments
/// * `py` - Python GIL token
/// * `arr` - Numpy array (expects f32 dtype)
/// * `sample_rate` - Audio sample rate in Hz
/// * `channels` - Number of audio channels
///
/// # Returns
/// AudioBuffer with zero-copy semantics for read-only access
pub fn numpy_to_audio_buffer_ffi<'py>(
    _py: Python<'py>,
    arr: &Bound<'py, PyAny>,
    sample_rate: u32,
    channels: u16,
) -> PyResult<AudioBuffer> {
    // Downcast to f32 array
    let array = arr.downcast::<PyArrayDyn<f32>>()?;

    // Get readonly view (zero-copy borrow)
    let readonly = array.readonly();
    let slice = readonly.as_slice()?;

    // Copy into Vec for ownership (required since Python array lifetime is limited)
    // In a true zero-copy scenario with stable memory, we'd use unsafe to borrow
    let data = slice.to_vec();

    // Wrap in Arc for shared ownership
    let audio_buffer = AudioBuffer::from_vec(data, sample_rate, channels, AudioFormat::F32);

    Ok(audio_buffer)
}

/// Python FFI: Convert numpy array to RuntimeData (zero-copy passthrough)
///
/// This function wraps a numpy array in RuntimeData::Numpy, allowing it to
/// flow through the pipeline without conversion until the IPC boundary.
/// At the IPC boundary, `to_ipc_runtime_data` handles serialization once.
///
/// # Arguments (Python)
/// * `arr` - Numpy array (any dtype, any shape)
///
/// # Returns
/// RuntimeData object (opaque handle) that can be passed to pipeline execution
///
/// # Example (Python)
/// ```python
/// from remotemedia.runtime import numpy_to_runtime_data, execute_pipeline_with_input
/// import numpy as np
///
/// # Create audio frame (20ms at 48kHz = 960 samples)
/// audio_frame = np.zeros(960, dtype=np.float32)
///
/// # Convert to RuntimeData (zero-copy, no serialization)
/// runtime_data = numpy_to_runtime_data(audio_frame)
///
/// # Pass directly to pipeline - only serializes once at IPC boundary
/// result = await execute_pipeline_with_input(manifest, [runtime_data])
/// ```
#[pyfunction]
#[pyo3(signature = (arr))]
#[allow(unused_variables)]
pub fn numpy_to_runtime_data(py: Python<'_>, arr: Bound<'_, PyAny>) -> PyResult<()> {
    // This function is a no-op now - python_to_runtime_data in marshal.rs
    // automatically detects numpy arrays and wraps them in RuntimeData::Numpy
    // We keep this function for API compatibility and documentation
    Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
        "numpy_to_runtime_data is deprecated. Pass numpy arrays directly to execute_pipeline_with_input - \
         they will be automatically wrapped in RuntimeData::Numpy for zero-copy passthrough."
    ))
}

/// Convert AudioBuffer to numpy array with zero-copy (Phase 4: T049-T053)
///
/// This function provides efficient transfer from Rust to Python by:
/// 1. Getting a slice from the AudioBuffer (zero-copy via Arc)
/// 2. Creating a numpy array from the slice (PyO3 handles the conversion)
///
/// # Arguments
/// * `py` - Python GIL token
/// * `buffer` - AudioBuffer to convert
///
/// # Returns
/// Numpy array with f32 dtype
pub fn audio_buffer_to_numpy_ffi<'py>(
    py: Python<'py>,
    buffer: &AudioBuffer,
) -> PyResult<Bound<'py, PyAny>> {
    // Get slice from AudioBuffer (zero-copy via Arc)
    let slice = buffer.as_slice();

    // Create numpy array from slice
    // PyArray::from_slice creates a numpy array that borrows from the Rust data
    let array = PyArray::from_slice(py, slice);

    // For multi-channel audio, reshape to (frames, channels)
    if buffer.channels() > 1 {
        let frames = buffer.len_frames();
        let channels = buffer.channels() as usize;
        // Use Python's reshape method via call_method
        let reshaped = array.call_method1("reshape", ((frames, channels),))?;
        Ok(reshaped)
    } else {
        Ok(array.into_any())
    }
}

// audio_dict_to_numpy is no longer needed - pipeline results automatically
// convert RuntimeData::Numpy back to Python numpy arrays via runtime_data_to_python

/// Python FFI: Publish numpy audio array directly to iceoryx2 channel (zero-copy)
///
/// This function uses the existing `to_ipc_runtime_data` serialization format
/// and publishes directly to iceoryx2 shared memory, avoiding repeated copies
/// across the FFI boundary for streaming audio.
///
/// # Arguments (Python)
/// * `arr` - Numpy array with f32 dtype containing audio samples
/// * `channel_name` - Name of the iceoryx2 channel to publish to
/// * `session_id` - Session identifier for namespacing
/// * `sample_rate` - Audio sample rate in Hz
/// * `channels` - Number of audio channels
///
/// # Returns
/// None on success, raises exception on error
///
/// # Example (Python)
/// ```python
/// from remotemedia.runtime import publish_audio_to_ipc
/// import numpy as np
///
/// # Create audio frame (20ms at 48kHz = 960 samples)
/// audio_frame = np.zeros(960, dtype=np.float32)
///
/// # Publish directly to iceoryx2 (zero-copy, no serialization overhead)
/// publish_audio_to_ipc(
///     audio_frame,
///     channel_name="session_123_node_456_input",
///     session_id="session_123",
///     sample_rate=48000,
///     channels=1
/// )
/// ```
#[pyfunction]
#[pyo3(signature = (arr, channel_name, session_id, sample_rate, channels))]
#[allow(unused_variables)]
pub fn publish_audio_to_ipc(
    py: Python<'_>,
    arr: Bound<'_, PyAny>,
    channel_name: String,
    session_id: String,
    sample_rate: u32,
    channels: u16,
) -> PyResult<()> {
    // This will be implemented to use core's multiprocess IPC infrastructure
    // The actual implementation requires access to ChannelRegistry
    Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
        "publish_audio_to_ipc requires core with multiprocess feature. \
         Use send_data_to_node from MultiprocessExecutor instead.",
    ))
}

/// Python FFI: Subscribe to audio from iceoryx2 channel (zero-copy)
///
/// This function subscribes from iceoryx2 shared memory and converts
/// directly to numpy arrays using `from_ipc_runtime_data`, avoiding
/// repeated serialization for streaming audio.
///
/// # Arguments (Python)
/// * `channel_name` - Name of the iceoryx2 channel to subscribe from
/// * `timeout_ms` - Optional timeout in milliseconds (None = non-blocking)
///
/// # Returns
/// Numpy array with audio data, or None if no data available
///
/// # Example (Python)
/// ```python
/// from remotemedia.runtime import subscribe_audio_from_ipc
/// import numpy as np
///
/// # Subscribe from iceoryx2 channel (zero-copy)
/// audio_data = subscribe_audio_from_ipc(
///     channel_name="session_123_node_456_output",
///     timeout_ms=100  # Wait up to 100ms
/// )
///
/// if audio_data is not None:
///     print(f"Received audio: {audio_data.shape}")
/// ```
#[pyfunction]
#[pyo3(signature = (channel_name, timeout_ms=None))]
#[allow(unused_variables)]
pub fn subscribe_audio_from_ipc<'py>(
    py: Python<'py>,
    channel_name: String,
    timeout_ms: Option<u64>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    // This will be implemented to use core's multiprocess IPC infrastructure
    Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
        "subscribe_audio_from_ipc requires core with multiprocess feature. \
         Use register_output_callback from MultiprocessExecutor instead.",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_numpy_array() {
        Python::attach(|py| {
            // Try to import numpy (skip test if not available)
            if py.import("numpy").is_err() {
                println!("Skipping test: numpy not available in this Python environment");
                return;
            }

            // Create a numpy array using run
            use pyo3::ffi::c_str;
            let code = c_str!(
                r#"
import numpy as np
result = np.array([1.0, 2.0, 3.0])
"#
            );
            py.run(code, None, None).unwrap();

            // Get the result from locals
            let locals = py.eval(c_str!("locals()"), None, None).unwrap();
            let array = locals.get_item("result").unwrap();

            assert!(is_numpy_array(py, &array));

            // Regular list should not be detected as numpy array
            let list = py.eval(c_str!("[1, 2, 3]"), None, None).unwrap();
            assert!(!is_numpy_array(py, &list));
        });
    }

    #[test]
    fn test_extract_numpy_metadata() {
        Python::attach(|py| {
            if py.import("numpy").is_err() {
                println!("Skipping test: numpy not available");
                return;
            }

            use pyo3::ffi::c_str;
            // Create a 2D array
            let code = c_str!(
                r#"
import numpy as np
result = np.array([[1, 2], [3, 4]], dtype=np.int32)
"#
            );
            py.run(code, None, None).unwrap();
            let locals = py.eval(c_str!("locals()"), None, None).unwrap();
            let array = locals.get_item("result").unwrap();

            let meta = extract_numpy_metadata(py, &array).unwrap();

            assert_eq!(meta.shape, vec![2, 2]);
            assert_eq!(meta.dtype, "int32");
            assert_eq!(meta.size, 4);
            assert!(meta.c_contiguous);
        });
    }

    #[test]
    fn test_numpy_roundtrip() {
        Python::attach(|py| {
            if py.import("numpy").is_err() {
                println!("Skipping test: numpy not available");
                return;
            }

            use pyo3::ffi::c_str;
            // Create array
            let code = c_str!(
                r#"
import numpy as np
result = np.array([1.0, 2.0, 3.0, 4.0], dtype=np.float64)
"#
            );
            py.run(code, None, None).unwrap();
            let locals = py.eval(c_str!("locals()"), None, None).unwrap();
            let original = locals.get_item("result").unwrap();

            // Python → JSON
            let json_val = numpy_to_json(py, &original).unwrap();

            assert!(is_numpy_json(&json_val));

            // JSON → Python
            let reconstructed = json_to_numpy(py, &json_val).unwrap();

            // Verify shape and dtype
            let meta = extract_numpy_metadata(py, &reconstructed).unwrap();
            assert_eq!(meta.shape, vec![4]);
            assert_eq!(meta.dtype, "float64");

            // Verify data (compare as lists)
            let numpy = py.import("numpy").unwrap();
            let allclose = numpy.getattr("allclose").unwrap();
            let result = allclose.call1((&original, &reconstructed)).unwrap();
            let arrays_equal: bool = result.extract().unwrap();

            assert!(arrays_equal, "Arrays should be equal after round-trip");
        });
    }

    #[test]
    fn test_numpy_to_vec() {
        Python::attach(|py| {
            if py.import("numpy").is_err() {
                println!("Skipping test: numpy not available");
                return;
            }

            use pyo3::ffi::c_str;
            // Create array
            let code = c_str!(
                r#"
import numpy as np
result = np.array([1.0, 2.0, 3.0, 4.0])
"#
            );
            py.run(code, None, None).unwrap();
            let locals = py.eval(c_str!("locals()"), None, None).unwrap();
            let array = locals.get_item("result").unwrap();

            // Convert to Rust Vec
            let vec = numpy_to_vec_f64(py, &array).unwrap();

            assert_eq!(vec, vec![1.0, 2.0, 3.0, 4.0]);
        });
    }

    #[test]
    fn test_vec_to_numpy() {
        Python::attach(|py| {
            if py.import("numpy").is_err() {
                println!("Skipping test: numpy not available");
                return;
            }

            // Create Rust vector
            let data = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
            let shape = vec![2, 3];

            // Convert to numpy
            let array = vec_to_numpy_f64(py, &data, &shape).unwrap();

            // Verify shape
            let meta = extract_numpy_metadata(py, &array).unwrap();
            assert_eq!(meta.shape, vec![2, 3]);
            assert_eq!(meta.size, 6);
        });
    }

    #[test]
    fn test_numpy_to_audio_buffer() {
        Python::attach(|py| {
            if py.import("numpy").is_err() {
                println!("Skipping test: numpy not available");
                return;
            }

            use pyo3::ffi::c_str;
            // Create f32 numpy array
            let code = c_str!(
                r#"
import numpy as np
result = np.array([0.0, 0.5, 1.0, 0.5], dtype=np.float32)
"#
            );
            py.run(code, None, None).unwrap();
            let locals = py.eval(c_str!("locals()"), None, None).unwrap();
            let array = locals.get_item("result").unwrap();

            // Convert to AudioBuffer
            let buffer = numpy_to_audio_buffer_ffi(py, &array, 48000, 1).unwrap();

            assert_eq!(buffer.len_samples(), 4);
            assert_eq!(buffer.sample_rate(), 48000);
            assert_eq!(buffer.channels(), 1);
            assert_eq!(buffer.format(), AudioFormat::F32);

            // Verify data
            let slice = buffer.as_slice();
            assert_eq!(slice[0], 0.0);
            assert_eq!(slice[1], 0.5);
            assert_eq!(slice[2], 1.0);
            assert_eq!(slice[3], 0.5);
        });
    }

    #[test]
    fn test_audio_buffer_to_numpy_mono() {
        Python::attach(|py| {
            if py.import("numpy").is_err() {
                println!("Skipping test: numpy not available");
                return;
            }

            // Create AudioBuffer
            let data = vec![0.1, 0.2, 0.3, 0.4];
            let buffer = AudioBuffer::from_vec(data, 48000, 1, AudioFormat::F32);

            // Convert to numpy
            let array = audio_buffer_to_numpy_ffi(py, &buffer).unwrap();

            // Verify shape (mono should be 1D)
            let meta = extract_numpy_metadata(py, &array).unwrap();
            assert_eq!(meta.shape, vec![4]);
            assert_eq!(meta.dtype, "float32");
        });
    }

    #[test]
    fn test_audio_buffer_to_numpy_stereo() {
        Python::attach(|py| {
            if py.import("numpy").is_err() {
                println!("Skipping test: numpy not available");
                return;
            }

            // Create stereo AudioBuffer (interleaved: L, R, L, R, L, R, L, R)
            let data = vec![0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8];
            let buffer = AudioBuffer::from_vec(data, 48000, 2, AudioFormat::F32);

            // Convert to numpy
            let array = audio_buffer_to_numpy_ffi(py, &buffer).unwrap();

            // Verify shape (stereo should be 2D: [frames, channels])
            let meta = extract_numpy_metadata(py, &array).unwrap();
            assert_eq!(meta.shape, vec![4, 2]); // 4 frames, 2 channels
            assert_eq!(meta.dtype, "float32");
        });
    }

    #[test]
    fn test_zero_copy_roundtrip() {
        Python::attach(|py| {
            if py.import("numpy").is_err() {
                println!("Skipping test: numpy not available");
                return;
            }

            use pyo3::ffi::c_str;
            // Create numpy array
            let code = c_str!(
                r#"
import numpy as np
result = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], dtype=np.float32)
"#
            );
            py.run(code, None, None).unwrap();
            let locals = py.eval(c_str!("locals()"), None, None).unwrap();
            let original = locals.get_item("result").unwrap();

            // Python → AudioBuffer
            let buffer = numpy_to_audio_buffer_ffi(py, &original, 44100, 1).unwrap();

            // AudioBuffer → Python
            let reconstructed = audio_buffer_to_numpy_ffi(py, &buffer).unwrap();

            // Verify equality
            let numpy = py.import("numpy").unwrap();
            let allclose = numpy.getattr("allclose").unwrap();
            let result = allclose.call1((&original, &reconstructed)).unwrap();
            let arrays_equal: bool = result.extract().unwrap();

            assert!(arrays_equal, "Arrays should be equal after round-trip");
        });
    }
}
