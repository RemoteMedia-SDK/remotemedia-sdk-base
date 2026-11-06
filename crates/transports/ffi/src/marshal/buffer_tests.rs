use super::*;
use pyo3::{buffer::PyUntypedBuffer, types::PyBytes};
use remotemedia_types::{
    AudioSamples, BufferBudget, CopyCounts, CopyPolicy, CopyReason, SharedBytes,
};

fn context(policy: CopyPolicy) -> CopyContext {
    CopyContext::new(policy, "python-ffi-test")
}
fn counts(context: &CopyContext) -> CopyCounts {
    context
        .report()
        .entries
        .into_iter()
        .find(|(reason, _)| *reason == CopyReason::BorrowedInput)
        .unwrap()
        .1
}
fn audio(samples: AudioSamples) -> RuntimeData {
    RuntimeData::Audio {
        samples,
        sample_rate: 48000,
        channels: 1,
        stream_id: Some("source".into()),
        timestamp_us: Some(42),
        arrival_ts_us: Some(43),
        metadata: Some(serde_json::json!({"key": "value"})),
    }
}

#[test]
fn binary_view_keeps_pointer_and_budget_after_rust_message_drop() {
    Python::attach(|py| {
        let budget = BufferBudget::new(1, 16);
        let value = budget
            .admit(RuntimeData::Binary(vec![7; 16].into()))
            .unwrap();
        let RuntimeData::Binary(data) = &value else {
            unreachable!()
        };
        let pointer = data.as_ptr();
        let strict = context(CopyPolicy::RequireZeroCopy);
        let object = runtime_data_to_python_with_context(py, &value, &strict).unwrap();
        let view = PyUntypedBuffer::get(object.bind(py)).unwrap();
        assert_eq!(view.buf_ptr() as *const u8, pointer);
        assert!(view.readonly());
        drop(view);
        drop(value);
        assert_eq!(budget.usage().bytes, 16);
        let recovered = python_to_runtime_data_with_context(py, object.bind(py), &strict).unwrap();
        drop(object);
        assert_eq!(budget.usage().owners, 1);
        let RuntimeData::Binary(data) = recovered else {
            unreachable!()
        };
        assert_eq!(data.as_ptr(), pointer);
        assert_eq!(&*data, &[7; 16]);
        drop(data);
        assert_eq!(budget.usage().owners, 0);
        assert_eq!(counts(&strict), CopyCounts::default());
    });
}

#[test]
fn consuming_vec_audio_preserves_pointer_and_all_metadata() {
    Python::attach(|py| {
        let samples = vec![0.25f32, -0.5, 1.0];
        let pointer = samples.as_ptr();
        let object = runtime_data_into_python(py, audio(AudioSamples::Vec(samples))).unwrap();
        let strict = context(CopyPolicy::RequireZeroCopy);
        let result = python_to_runtime_data_with_context(py, object.bind(py), &strict).unwrap();
        let RuntimeData::Audio {
            samples,
            stream_id,
            timestamp_us,
            arrival_ts_us,
            metadata,
            ..
        } = result
        else {
            unreachable!()
        };
        assert_eq!(samples.as_ptr(), pointer);
        assert_eq!(stream_id.as_deref(), Some("source"));
        assert_eq!((timestamp_us, arrival_ts_us), (Some(42), Some(43)));
        assert_eq!(metadata, Some(serde_json::json!({"key": "value"})));
        assert_eq!(counts(&strict), CopyCounts::default());
    });
}

#[test]
fn borrowed_vec_audio_copies_once_or_rejects_before_export() {
    Python::attach(|py| {
        let data = audio(AudioSamples::Vec(vec![1.0; 4]));
        let strict = context(CopyPolicy::RequireZeroCopy);
        assert!(runtime_data_to_python_with_context(py, &data, &strict).is_err());
        assert_eq!(counts(&strict).rejected_bytes, 16);
        let allow = context(CopyPolicy::AllowCopies);
        let output = runtime_data_to_python_with_context(py, &data, &allow).unwrap();
        let recovered = python_to_runtime_data_with_context(py, output.bind(py), &strict).unwrap();
        let RuntimeData::Audio {
            samples: original, ..
        } = data
        else {
            unreachable!()
        };
        let RuntimeData::Audio {
            samples: copied, ..
        } = recovered
        else {
            unreachable!()
        };
        assert_ne!(original.as_ptr(), copied.as_ptr());
        assert_eq!(counts(&allow).copied_bytes, 16);
        assert_eq!(counts(&allow).completed, 1);
    });
}

#[test]
fn immutable_python_bytes_are_owned_without_copy_and_survive_gil_release() {
    let owner = Python::attach(|py| {
        let bytes = PyBytes::new(py, &[4; 32]);
        let pointer = bytes.as_bytes().as_ptr();
        let strict = context(CopyPolicy::RequireZeroCopy);
        let result = python_to_runtime_data_with_context(py, bytes.as_any(), &strict).unwrap();
        let RuntimeData::Binary(data) = result else {
            unreachable!()
        };
        assert_eq!(pointer, data.as_ptr());
        assert_eq!(counts(&strict), CopyCounts::default());
        data
    });
    std::thread::spawn(move || assert_eq!(&*owner, &[4; 32]))
        .join()
        .unwrap();
}

fn array<'py>(py: Python<'py>, expression: &str) -> Bound<'py, PyAny> {
    let locals = PyDict::new(py);
    locals.set_item("np", py.import("numpy").unwrap()).unwrap();
    py.eval(
        &std::ffi::CString::new(expression).unwrap(),
        None,
        Some(&locals),
    )
    .unwrap()
}

#[test]
fn private_strided_numpy_packs_once_with_correct_metadata() {
    Python::attach(|py| {
        let obj = array(py, "np.arange(24, dtype=np.int32).reshape(4, 6)[::2, ::2]");
        let strict = context(CopyPolicy::RequireZeroCopy);
        assert!(python_to_runtime_data_with_context(py, &obj, &strict).is_err());
        assert_eq!(counts(&strict).rejected_bytes, 24);
        let allow = context(CopyPolicy::AllowCopies);
        let data = python_to_runtime_data_with_context(py, &obj, &allow).unwrap();
        let RuntimeData::Numpy {
            shape,
            strides,
            c_contiguous,
            f_contiguous,
            ..
        } = &data
        else {
            unreachable!()
        };
        assert_eq!(shape, &[2, 3]);
        assert_eq!(strides, &[12, 4]);
        assert!(*c_contiguous && !*f_contiguous);
        assert_eq!(counts(&allow).copied_bytes, 24);
        assert_eq!(counts(&allow).completed, 1);
        let output = runtime_data_to_python_with_context(py, &data, &strict).unwrap();
        let equal: bool = py
            .import("numpy")
            .unwrap()
            .call_method1("array_equal", (&obj, output.bind(py)))
            .unwrap()
            .extract()
            .unwrap();
        assert!(equal);
        assert_eq!(
            output
                .bind(py)
                .getattr("flags")
                .unwrap()
                .getattr("writeable")
                .unwrap()
                .extract::<bool>()
                .unwrap(),
            false
        );
    });
}

#[test]
fn numpy_c_and_fortran_views_roundtrip_without_copy() {
    Python::attach(|py| {
        for fortran in [false, true] {
            let bytes: SharedBytes = (0u8..24).collect::<Vec<_>>().into();
            let pointer = bytes.as_ptr();
            let data = RuntimeData::Numpy {
                data: bytes,
                shape: vec![2, 3],
                dtype: "int32".into(),
                strides: if fortran { vec![4, 8] } else { vec![12, 4] },
                c_contiguous: !fortran,
                f_contiguous: fortran,
            };
            let strict = context(CopyPolicy::RequireZeroCopy);
            let object = runtime_data_to_python_with_context(py, &data, &strict).unwrap();
            drop(data);
            let recovered =
                python_to_runtime_data_with_context(py, object.bind(py), &strict).unwrap();
            let RuntimeData::Numpy {
                data, f_contiguous, ..
            } = recovered
            else {
                unreachable!()
            };
            assert_eq!(data.as_ptr(), pointer);
            assert_eq!(f_contiguous, fortran);
            assert_eq!(counts(&strict), CopyCounts::default());
        }
    });
}

#[test]
fn object_and_non_native_arrays_reject_before_materialization() {
    Python::attach(|py| {
        for expression in [
            "np.array([object()], dtype=object)",
            "np.arange(3, dtype=np.dtype('int32').newbyteorder('S'))",
        ] {
            let input = array(py, expression);
            let allow = context(CopyPolicy::AllowCopies);
            assert!(python_to_runtime_data_with_context(py, &input, &allow).is_err());
            assert_eq!(counts(&allow), CopyCounts::default());
        }
    });
}

#[test]
fn mutable_private_bytes_copy_once_and_cannot_alias_published_storage() {
    Python::attach(|py| {
        let input = py
            .import("builtins")
            .unwrap()
            .getattr("bytearray")
            .unwrap()
            .call1((vec![1u8, 2, 3, 4],))
            .unwrap();
        let strict = context(CopyPolicy::RequireZeroCopy);
        assert!(python_to_runtime_data_with_context(py, &input, &strict).is_err());
        let allow = context(CopyPolicy::AllowCopies);
        let RuntimeData::Binary(data) =
            python_to_runtime_data_with_context(py, &input, &allow).unwrap()
        else {
            unreachable!()
        };
        input.set_item(0, 9).unwrap();
        assert_eq!(&*data, &[1, 2, 3, 4]);
        assert_eq!(
            (counts(&allow).completed, counts(&allow).copied_bytes),
            (1, 4)
        );
        assert_eq!(counts(&strict).rejected_bytes, 4);
    });
}

#[test]
fn structured_bulk_variants_preserve_owner_and_metadata() {
    use remotemedia_types::{ImageFormat, PayloadStorage, PixelFormat, TensorEncoding};
    Python::attach(|py| {
        let bytes: SharedBytes = vec![1; 16].into();
        let pointer = bytes.as_ptr();
        let cases = [
            RuntimeData::Video {
                pixel_data: bytes.clone(),
                width: 2,
                height: 2,
                format: PixelFormat::Rgba32,
                codec: None,
                frame_number: 7,
                timestamp_us: 99,
                is_keyframe: true,
                stream_id: Some("camera".into()),
                arrival_ts_us: Some(100),
            },
            RuntimeData::Image {
                data: bytes.clone(),
                format: ImageFormat::Png,
                width: 2,
                height: 2,
                timestamp_us: Some(12),
                stream_id: Some("image".into()),
                metadata: Some(serde_json::json!({"id": 1})),
            },
            RuntimeData::Tensor {
                data: bytes,
                shape: vec![4],
                dtype: 0,
                metadata: Some(serde_json::json!({"axis": "x"})),
            },
        ];
        let strict = context(CopyPolicy::RequireZeroCopy);
        for input in cases {
            let expected = serde_json::to_value(
                input
                    .clone()
                    .into_shared_parts(TensorEncoding::Runtime)
                    .unwrap()
                    .0,
            )
            .unwrap();
            let object = runtime_data_to_python_with_context(py, &input, &strict).unwrap();
            drop(input);
            let output = python_to_runtime_data_with_context(py, object.bind(py), &strict).unwrap();
            let (metadata, storage) = output.into_shared_parts(TensorEncoding::Runtime).unwrap();
            assert_eq!(serde_json::to_value(metadata).unwrap(), expected);
            let PayloadStorage::Bytes(owner) = storage else {
                unreachable!()
            };
            assert_eq!(owner.as_ptr(), pointer);
        }
        assert_eq!(counts(&strict), CopyCounts::default());
    });
}

#[test]
fn strict_metadata_does_not_serialize_hidden_arrays() {
    Python::attach(|py| {
        let input = array(py, "np.arange(4, dtype=np.float32)");
        let strict = context(CopyPolicy::RequireZeroCopy);
        let dict = PyDict::new(py);
        dict.set_item("type", "json").unwrap();
        dict.set_item("value", &input).unwrap();
        assert!(python_to_runtime_data_with_context(py, dict.as_any(), &strict).is_err());
        dict.del_item("type").unwrap();
        assert!(python_to_runtime_data_with_context(py, dict.as_any(), &strict).is_err());
        assert_eq!(counts(&strict), CopyCounts::default());
    });
}
