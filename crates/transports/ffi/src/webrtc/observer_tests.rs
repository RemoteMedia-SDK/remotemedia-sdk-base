use super::*;
use remotemedia_types::{BufferBudget, CopyContext, CopyPolicy, RuntimeData};

#[test]
fn observer_bridge_conversion_and_ffi_fanout_retain_payload_owners() {
    for raw in [false, true] {
        let copies = CopyContext::new(CopyPolicy::RequireZeroCopy, "observer-fanout");
        let budget = BufferBudget::new(1, 16);
        let RuntimeData::Binary(source) = budget
            .admit(RuntimeData::Binary(vec![5; 16].into()))
            .unwrap()
        else {
            unreachable!()
        };
        let pointer = source.as_ptr();
        let bridge = if raw {
            WebRtcEventBridge::data_received_with_context("peer".into(), source, 42, &copies)
        } else {
            WebRtcEventBridge::pipeline_output_with_context(
                "peer".into(),
                RuntimeData::Binary(source),
                42,
                &copies,
            )
        };
        let event = convert_bridge_to_ffi_event(&bridge);
        let clone = event.clone();
        drop((bridge, event));
        let bytes = match clone {
            WebRtcEvent::DataReceived(event) => {
                assert_eq!(event.timestamp, 42);
                assert!(event.copies.same_authority(&copies));
                event.data
            }
            WebRtcEvent::PipelineOutput(event) => {
                assert_eq!(event.timestamp, 42);
                assert!(event.copies.same_authority(&copies));
                let RuntimeData::Binary(bytes) = event.data else {
                    unreachable!()
                };
                bytes
            }
            _ => unreachable!(),
        };
        assert_eq!(bytes.as_ptr(), pointer);
        let retained = bytes.slice(2..5).unwrap();
        drop(bytes);
        assert_eq!(budget.usage().owners, 1);
        assert_eq!(&*retained, &[5; 3]);
        std::thread::spawn(move || drop(retained)).join().unwrap();
        assert_eq!(budget.usage().owners, 0);
    }
}

#[cfg(feature = "python")]
#[tokio::test]
async fn observer_python_getters_keep_session_policy_after_joined_shutdown() {
    use remotemedia_core::{manifest::NodeManifest, transport::ExecutorConfig};
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        for raw in [false, true] {
            let executor = PipelineExecutor::with_config(ExecutorConfig {
                transport_materialization_policy: policy,
                ..Default::default()
            })
            .unwrap();
            let mut handle = executor
                .create_session(Arc::new(Manifest {
                    nodes: vec![NodeManifest {
                        id: "calc".into(),
                        node_type: "CalculatorNode".into(),
                        params: serde_json::json!({}),
                        ..Default::default()
                    }],
                    ..Default::default()
                }))
                .await
                .unwrap();
            let copies = handle.transport_copy_context();
            let budget = BufferBudget::new(1, 16);
            let RuntimeData::Binary(source) = budget
                .admit(RuntimeData::Binary(vec![9; 16].into()))
                .unwrap()
            else {
                unreachable!()
            };
            let pointer = source.as_ptr();
            let bridge = if raw {
                WebRtcEventBridge::data_received_with_context("peer".into(), source, 42, &copies)
            } else {
                WebRtcEventBridge::pipeline_output_with_context(
                    "peer".into(),
                    RuntimeData::Binary(source),
                    42,
                    &copies,
                )
            };
            let event = convert_bridge_to_ffi_event(&bridge);
            drop(bridge);
            handle.close().await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), handle.wait())
                .await
                .unwrap()
                .unwrap();
            check_python_observer_after_shutdown(event, &copies, &budget, pointer, raw);
        }
    }
}

#[cfg(feature = "python")]
fn check_python_observer_after_shutdown(
    event: WebRtcEvent,
    copies: &CopyContext,
    budget: &BufferBudget,
    pointer: *const u8,
    raw: bool,
) {
    use pyo3::{buffer::PyUntypedBuffer, prelude::*, types::PySlice};
    Python::attach(|py| {
        let object = match event {
            WebRtcEvent::DataReceived(e) => Bound::new(
                py,
                crate::python::webrtc::events::DataReceivedEvent::from(e),
            )
            .unwrap()
            .into_any(),
            WebRtcEvent::PipelineOutput(e) => Bound::new(
                py,
                crate::python::webrtc::events::PipelineOutputEvent::from(e),
            )
            .unwrap()
            .into_any(),
            _ => unreachable!(),
        };
        assert_eq!(
            object
                .getattr("timestamp")
                .unwrap()
                .extract::<u64>()
                .unwrap(),
            42
        );
        let view = object
            .getattr(if raw { "view" } else { "runtime_data" })
            .unwrap();
        let buffer = PyUntypedBuffer::get(&view).unwrap();
        assert_eq!(buffer.buf_ptr() as *const u8, pointer);
        assert!(buffer.readonly());
        drop(buffer);
        check_python_observer_materialization(&object, copies, raw);
        let slice = view.get_item(PySlice::new(py, 2, 6, 1)).unwrap();
        drop((object, view));
        assert_eq!(budget.usage().owners, 1);
        let retained = PyUntypedBuffer::get(&slice).unwrap();
        assert_eq!(retained.buf_ptr() as usize, pointer as usize + 2);
        drop((retained, slice));
        assert_eq!(budget.usage().owners, 0);
    });
}

#[cfg(feature = "python")]
fn check_python_observer_materialization(
    object: &pyo3::Bound<'_, pyo3::PyAny>,
    copies: &CopyContext,
    raw: bool,
) {
    use pyo3::{prelude::*, types::PyBytes};
    use remotemedia_types::CopyReason;
    let strict = copies.policy() == CopyPolicy::RequireZeroCopy;
    let mut encoded_len = 0;
    for _ in 0..2 {
        let result = object.getattr("data");
        if strict {
            let error = result.unwrap_err().to_string();
            assert!(error.contains(copies.boundary()), "{error}");
        } else {
            let value = result.unwrap();
            let bytes = value.cast::<PyBytes>().unwrap().as_bytes();
            encoded_len = bytes.len() as u64;
            if raw {
                assert_eq!(bytes, &[9; 16]);
            } else {
                let decoded: RuntimeData = serde_json::from_slice(bytes).unwrap();
                assert!(matches!(decoded, RuntimeData::Binary(b) if &*b == &[9; 16]));
            }
        }
    }
    let report = copies.report();
    let expected = if strict {
        vec![(
            if raw {
                CopyReason::VecMaterialization
            } else {
                CopyReason::PortableSerialization
            },
            0,
            32,
        )]
    } else if raw {
        vec![(CopyReason::VecMaterialization, 32, 0)]
    } else {
        vec![
            (CopyReason::PortableSerialization, 16, 0),
            (CopyReason::VecMaterialization, 2 * encoded_len, 0),
        ]
    };
    for (reason, copied, rejected) in expected {
        let counts = &report.entries.iter().find(|(r, _)| *r == reason).unwrap().1;
        assert_eq!(
            (counts.copied_bytes, counts.rejected_bytes),
            (copied, rejected)
        );
    }
}
