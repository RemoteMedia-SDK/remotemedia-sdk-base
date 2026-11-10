use super::*;
use remotemedia_types::{CopyCounts, CopyPolicy, CopyReason};

fn ledger(policy: CopyPolicy) -> CopyContext {
    CopyContext::new(policy, "ffi-json-test")
}

fn counts(context: &CopyContext, reason: CopyReason) -> CopyCounts {
    context
        .report()
        .entries
        .into_iter()
        .find(|(r, _)| *r == reason)
        .unwrap()
        .1
}

#[cfg(feature = "native-numpy")]
use super::buffer_tests::array;

#[cfg(feature = "native-numpy")]
#[test]
fn numpy_json_counts_each_copy_and_retains_decoded_storage() {
    Python::attach(|py| {
        for expression in [
            "np.arange(12, dtype=np.float32).reshape(3,4)[:, ::2]",
            "np.asfortranarray(np.arange(6, dtype=np.int16).reshape(3,2))",
            "np.array([1, 256], dtype='>i4')",
            "np.array(['2026-11-06'], dtype='datetime64[D]')",
            "np.array(3.5, dtype=np.float64)",
        ] {
            let source = array(py, expression);
            let nbytes: u64 = source.getattr("nbytes").unwrap().extract().unwrap();
            let context = ledger(CopyPolicy::AllowCopies);
            let encoded = python_to_json_with_context(py, &source, None, &context).unwrap();
            assert_eq!(
                counts(&context, CopyReason::BorrowedInput).copied_bytes,
                nbytes
            );
            assert_eq!(
                counts(&context, CopyReason::PortableSerialization).copied_bytes,
                nbytes
            );
            let decoded = json_to_python_with_context(py, &encoded, None, &context).unwrap();
            assert_eq!(
                counts(&context, CopyReason::PortableDeserialization).copied_bytes,
                nbytes
            );
            assert!(py
                .import("numpy")
                .unwrap()
                .call_method1("array_equal", (&source, &decoded))
                .unwrap()
                .extract::<bool>()
                .unwrap());
            assert!(source
                .getattr("dtype")
                .unwrap()
                .eq(decoded.getattr("dtype").unwrap())
                .unwrap());
            // datetime64 has no Python buffer-format code. A uint8 view of
            // these contiguous decoded bytes can still recover the same owner.
            let bytes = decoded
                .call_method1("reshape", ((-1,),))
                .unwrap()
                .call_method1(
                    "view",
                    (py.import("numpy").unwrap().getattr("uint8").unwrap(),),
                )
                .unwrap();
            let owner = _remotemedia_buffers::recover_bytes(&bytes).unwrap();
            let interface = decoded.getattr("__array_interface__").unwrap();
            let (pointer, readonly): (usize, bool) =
                interface.get_item("data").unwrap().extract().unwrap();
            assert_eq!(pointer, owner.as_ptr() as usize);
            assert!(readonly);
            assert!(decoded.call_method1("setflags", (true,)).is_err());
            drop(encoded);
            drop(source);
            drop(owner);
            drop(bytes);
            py.import("gc").unwrap().call_method0("collect").unwrap();
            assert_eq!(
                decoded.getattr("nbytes").unwrap().extract::<u64>().unwrap(),
                nbytes
            );
            // Plain dtypes recover the decoder owner through byte views and
            // need only the unavoidable base64 encoding, not another pack.
            {
                let again = ledger(CopyPolicy::AllowCopies);
                python_to_json_with_context(py, &decoded, None, &again).unwrap();
                assert_eq!(
                    counts(&again, CopyReason::BorrowedInput),
                    CopyCounts::default()
                );
                assert_eq!(
                    counts(&again, CopyReason::PortableSerialization).copied_bytes,
                    nbytes
                );
                let strict = ledger(CopyPolicy::RequireZeroCopy);
                assert!(python_to_json_with_context(py, &decoded, None, &strict).is_err());
                assert_eq!(
                    counts(&strict, CopyReason::PortableSerialization).rejected_bytes,
                    nbytes
                );
            }
        }
    });
}

#[cfg(feature = "native-numpy")]
#[test]
fn recursive_json_and_runtime_export_keep_copy_context() {
    Python::attach(|py| {
        let source = array(py, "np.array([17], dtype=np.int32)");
        let wrapper = PyDict::new(py);
        wrapper.set_item("nested", (source,)).unwrap();
        wrapper.set_item("participant.id", "peer-1").unwrap();
        let allow = ledger(CopyPolicy::AllowCopies);
        let json = python_to_json_with_context(py, wrapper.as_any(), None, &allow).unwrap();
        assert!(json["nested"][0]["__numpy__"].as_bool().unwrap());
        let strict = ledger(CopyPolicy::RequireZeroCopy);
        assert!(python_to_json_with_context(py, wrapper.as_any(), None, &strict).is_err());
        assert_eq!(counts(&strict, CopyReason::BorrowedInput).rejected_bytes, 4);
        let data = RuntimeData::Json(json);
        assert!(runtime_data_to_python_with_context(py, &data, &strict).is_err());
        assert_eq!(
            counts(&strict, CopyReason::PortableDeserialization).rejected_bytes,
            4
        );
        let output = runtime_data_to_python_with_context(py, &data, &allow).unwrap();
        assert_eq!(
            output
                .bind(py)
                .get_item("metadata")
                .unwrap()
                .get_item("participant.id")
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "peer-1"
        );
        let result = output
            .bind(py)
            .get_item("value")
            .unwrap()
            .get_item("nested")
            .unwrap()
            .get_item(0)
            .unwrap();
        assert_eq!(
            result
                .getattr("shape")
                .unwrap()
                .extract::<Vec<usize>>()
                .unwrap(),
            [1]
        );
        assert_eq!(
            counts(&allow, CopyReason::PortableDeserialization).copied_bytes,
            4
        );
        let mut envelope = match &data {
            RuntimeData::Json(value) => value["nested"][0].clone(),
            _ => unreachable!(),
        };
        envelope["participant.id"] = Value::String("outer-peer".into());
        let root =
            runtime_data_to_python_with_context(py, &RuntimeData::Json(envelope), &allow).unwrap();
        assert_eq!(
            root.bind(py)
                .get_item("metadata")
                .unwrap()
                .get_item("participant.id")
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "outer-peer"
        );
        // A top-level reserved envelope also passes through the same decoder.
        let RuntimeData::Json(json) = data else {
            unreachable!()
        };
        assert!(runtime_data_to_python_with_context(
            py,
            &RuntimeData::Json(json["nested"][0].clone()),
            &strict
        )
        .is_err());
    });
}

#[cfg(feature = "native-numpy")]
#[test]
fn empty_json_arrays_allow_strict_without_copy() {
    Python::attach(|py| {
        let source = array(py, "np.empty((0,3), dtype=np.float32)");
        let strict = ledger(CopyPolicy::RequireZeroCopy);
        let encoded = python_to_json_with_context(py, &source, None, &strict).unwrap();
        let output = json_to_python_with_context(py, &encoded, None, &strict).unwrap();
        assert_eq!(
            output
                .getattr("shape")
                .unwrap()
                .extract::<Vec<usize>>()
                .unwrap(),
            [0, 3]
        );
        assert!(strict
            .report()
            .entries
            .iter()
            .all(|(_, c)| *c == CopyCounts::default()));
    });
}

#[cfg(feature = "native-numpy")]
#[test]
fn invalid_numpy_metadata_rejects_without_payload_materialization() {
    Python::attach(|py| {
        let allow = ledger(CopyPolicy::AllowCopies);
        let source = array(py, "np.array([1,2], dtype=np.int32)");
        let valid = python_to_json_with_context(py, &source, None, &allow).unwrap();
        for (field, value) in [
            ("dtype", serde_json::json!("object")),
            ("size", serde_json::json!(3)),
            ("shape", serde_json::json!([usize::MAX, 2])),
        ] {
            let mut invalid = valid.clone();
            invalid["array"]["meta"][field] = value;
            let context = ledger(CopyPolicy::AllowCopies);
            assert!(json_to_python_with_context(py, &invalid, None, &context).is_err());
            assert_eq!(
                counts(&context, CopyReason::PortableDeserialization),
                CopyCounts::default()
            );
        }
        let mut invalid = valid;
        invalid["array"]["data"] = Value::String("!!!!!!!!!!!!".into());
        let context = ledger(CopyPolicy::AllowCopies);
        assert!(json_to_python_with_context(py, &invalid, None, &context).is_err());
        let count = counts(&context, CopyReason::PortableDeserialization);
        assert_eq!(
            (count.completed, count.failed, count.failed_requested_bytes),
            (0, 1, 8)
        );
        for expression in [
            "np.array([object()], dtype=object)",
            "np.array([(1,)], dtype=[('x', 'i4')])",
        ] {
            assert!(python_to_json_with_context(py, &array(py, expression), None, &allow).is_err());
        }
    });
}

#[test]
fn strict_json_rejects_opaque_codecs_but_preserves_cached_identity() {
    Python::attach(|py| {
        let strict = ledger(CopyPolicy::RequireZeroCopy);
        let object = py
            .import("builtins")
            .unwrap()
            .call_method0("object")
            .unwrap();
        assert!(python_to_json_with_context(py, &object, None, &strict).is_err());
        let marker = serde_json::json!({"nested": [{"__pickled__": true, "data": "invalid"}]});
        let err = json_to_python_with_context(py, &marker, None, &strict).unwrap_err();
        assert!(err.to_string().contains("opaque Python deserialization"));
        let cache = PyObjectCache::new();
        let reference = python_to_json_with_context(py, &object, Some(&cache), &strict).unwrap();
        assert!(
            json_to_python_with_context(py, &reference, Some(&cache), &strict)
                .unwrap()
                .is(&object)
        );
    });
}

#[test]
fn compatibility_pickle_round_trip_charges_encoded_bytes_both_ways() {
    Python::attach(|py| {
        let context = ledger(CopyPolicy::AllowCopies);
        // No numeric/string conversion applies, so this object is pickled.
        let value = py
            .import("datetime")
            .unwrap()
            .getattr("date")
            .unwrap()
            .call1((2026, 11, 9))
            .unwrap();
        let encoded = python_to_json_with_context(py, &value, None, &context).unwrap();
        assert_eq!(encoded["__pickled__"], true);
        let serialized = counts(&context, CopyReason::PortableSerialization);
        assert_eq!(serialized.completed, 1);
        assert!(serialized.copied_bytes > 0);
        let decoded = json_to_python_with_context(py, &encoded, None, &context).unwrap();
        assert!(decoded.eq(&value).unwrap());
        let deserialized = counts(&context, CopyReason::PortableDeserialization);
        assert_eq!(
            (deserialized.completed, deserialized.copied_bytes),
            (1, serialized.copied_bytes),
            "decode charges exactly the pickled bytes it consumed"
        );
    });
}
