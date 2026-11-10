use super::*;
use _remotemedia_buffers::recover_bytes;
use pyo3::types::PyBytes;

pub(super) fn export(
    py: Python<'_>,
    view: Py<PyAny>,
    shape: Vec<usize>,
    dtype: String,
    strides: Vec<isize>,
) -> PyResult<Py<PyAny>> {
    let kwargs = PyDict::new(py);
    kwargs.set_item("dtype", dtype)?;
    kwargs.set_item("buffer", view)?;
    kwargs.set_item("strides", strides)?;
    Ok(py
        .import("numpy")?
        .getattr("ndarray")?
        .call((shape,), Some(&kwargs))?
        .unbind())
}

fn packed_strides(shape: &[usize], itemsize: usize) -> PyResult<Vec<isize>> {
    let mut size = itemsize;
    let mut strides = vec![0; shape.len()];
    for (dimension, stride) in shape.iter().zip(strides.iter_mut()).rev() {
        *stride = isize::try_from(size).map_err(value_error)?;
        size = size
            .checked_mul(*dimension)
            .ok_or_else(|| value_error("array size overflow"))?;
    }
    Ok(strides)
}

pub(super) fn import(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    context: &CopyContext,
) -> PyResult<RuntimeData> {
    if !obj.get_type().is(&py.import("numpy")?.getattr("ndarray")?) {
        return Err(value_error(
            "bulk array input must be an exact numpy.ndarray",
        ));
    }
    let dtype = obj.getattr("dtype")?;
    if dtype.getattr("hasobject")?.extract::<bool>()?
        || !dtype.getattr("isnative")?.extract::<bool>()?
    {
        return Err(value_error(
            "object and non-native-endian arrays require explicit conversion",
        ));
    }
    let meta = crate::numpy_bridge::extract_numpy_metadata(py, obj)?;
    let (data, strides, c_contiguous, f_contiguous) = match recover_bytes(obj) {
        Ok(data) => (
            data,
            obj.getattr("strides")?.extract()?,
            meta.c_contiguous,
            meta.f_contiguous,
        ),
        Err(_) => {
            let nbytes: usize = obj.getattr("nbytes")?.extract()?;
            let data = storage::copy(context, nbytes, || {
                let packed = obj
                    .call_method1("tobytes", ("C",))?
                    .cast_into::<PyBytes>()?;
                Ok(storage::immutable_bytes(packed))
            })?;
            let itemsize: usize = dtype.getattr("itemsize")?.extract()?;
            let strides = packed_strides(&meta.shape, itemsize)?;
            let f_contiguous =
                meta.shape.contains(&0) || meta.shape.iter().filter(|&&d| d > 1).count() <= 1;
            (data, strides, true, f_contiguous)
        }
    };
    RuntimeMetadata::Numpy {
        shape: meta.shape,
        dtype: meta.dtype,
        strides,
        c_contiguous,
        f_contiguous,
    }
    .attach(PayloadStorage::Bytes(data), TensorEncoding::Runtime)
    .map_err(value_error)
}
