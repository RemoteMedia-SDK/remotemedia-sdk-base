use _remotemedia_buffers::{recover_audio, recover_bytes, BufferData, ReadBuffer};
use pyo3::{
    buffer::{PyBuffer, PyUntypedBuffer},
    prelude::*,
    pybacked::PyBackedBytes,
    types::{PyBytes, PyList, PyMemoryView, PyTuple},
};
use remotemedia_types::{CopyContext, CopyReason, PayloadStorage, SharedBuffer, SharedBytes};

pub(super) fn copy<T>(
    context: &CopyContext,
    bytes: usize,
    operation: impl FnOnce() -> PyResult<T>,
) -> PyResult<T> {
    materialize(context, CopyReason::BorrowedInput, bytes, operation)
}

pub(crate) fn materialize<T>(
    context: &CopyContext,
    reason: CopyReason,
    bytes: usize,
    operation: impl FnOnce() -> PyResult<T>,
) -> PyResult<T> {
    context
        .materialize_result(reason, bytes as u64, operation)
        .map_err(|error| pyo3::exceptions::PyBufferError::new_err(error.to_string()))?
}

pub(crate) fn view(py: Python<'_>, payload: PayloadStorage) -> PyResult<Py<PyAny>> {
    let data = match payload {
        PayloadStorage::Audio(data) => BufferData::Audio(data),
        PayloadStorage::Bytes(data) => BufferData::Bytes(data),
        PayloadStorage::None => return Err(super::value_error("missing bulk storage")),
    };
    let owner = Bound::new(py, ReadBuffer::new(data)?)?;
    Ok(PyMemoryView::from(owner.as_any())?.into_any().unbind())
}

pub(super) fn immutable_bytes(bytes: Bound<'_, PyBytes>) -> SharedBytes {
    SharedBytes::from_owner(PyBackedBytes::from(bytes))
}

pub(super) fn bytes(obj: &Bound<'_, PyAny>, context: &CopyContext) -> PyResult<SharedBytes> {
    if let Ok(owner) = recover_bytes(obj) {
        return Ok(owner);
    }
    // Python bytes are immutable even when privately allocated. PyO3's owning
    // wrapper safely keeps them alive across GIL release and Rust threads.
    if let Ok(bytes) = obj.cast::<PyBytes>() {
        return Ok(immutable_bytes(bytes.clone()));
    }
    if let Ok(buffer) = PyUntypedBuffer::get(obj) {
        return copy(context, buffer.len_bytes(), || {
            let view = PyMemoryView::from(obj)?;
            let packed = view.call_method0("tobytes")?.cast_into::<PyBytes>()?;
            Ok(immutable_bytes(packed))
        });
    }
    copy(context, obj.len()?, || Ok(obj.extract::<Vec<u8>>()?.into()))
}

pub(super) fn audio(obj: &Bound<'_, PyAny>, context: &CopyContext) -> PyResult<SharedBuffer<f32>> {
    if let Ok(owner) = recover_audio(obj) {
        return Ok(owner);
    }
    if let Ok(buffer) = PyBuffer::<f32>::get(obj) {
        return copy(context, buffer.len_bytes(), || {
            Ok(buffer.to_vec(obj.py())?.into())
        });
    }
    let bytes = obj
        .len()?
        .checked_mul(4)
        .ok_or_else(|| super::value_error("audio length overflow"))?;
    copy(context, bytes, || Ok(obj.extract::<Vec<f32>>()?.into()))
}

pub(super) fn is_byte_input(obj: &Bound<'_, PyAny>) -> bool {
    if PyUntypedBuffer::get(obj).is_ok() {
        return true;
    }
    if obj.is_instance_of::<PyList>() || obj.is_instance_of::<PyTuple>() {
        return obj
            .try_iter()
            .is_ok_and(|mut it| it.all(|item| item.is_ok_and(|v| v.extract::<u8>().is_ok())));
    }
    false
}
