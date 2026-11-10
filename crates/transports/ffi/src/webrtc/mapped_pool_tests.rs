use super::*;

fn config() -> WebRtcServerConfig {
    serde_json::from_value(serde_json::json!({
        "port": 50051, "signaling_url": null, "manifest": {}, "stun_servers": ["stun:localhost"],
        "pipeline_buffers": {"policy":"require_zero_copy", "node_retention_owners":64,
            "node_retention_bytes":4194304, "mapped_pool": {
                "classes":[{"bytes":3840,"slots":1}],"max_bytes":3840,"max_leases":4
            }}
    }))
    .unwrap()
}

#[test]
fn mapped_pool_policy_rejects_invalid_geometry_before_executor_creation() {
    let original = serde_json::to_value(config()).unwrap();
    for pool in [
        serde_json::json!({"classes":[],"max_bytes":8,"max_leases":1}),
        serde_json::json!({"classes":[{"bytes":0,"slots":1}],"max_bytes":8,"max_leases":1}),
        serde_json::json!({"classes":[{"bytes":8,"slots":0}],"max_bytes":8,"max_leases":1}),
        serde_json::json!({"classes":[{"bytes":8,"slots":2}],"max_bytes":8,"max_leases":1}),
        serde_json::json!({"classes":[{"bytes":8,"slots":1}],"max_bytes":8,"max_leases":0}),
        serde_json::json!({"classes":[{"bytes":u64::MAX,"slots":1}],"max_bytes":u64::MAX,"max_leases":1}),
        serde_json::json!({"classes":[{"bytes":i64::MAX,"slots":3}],"max_bytes":u64::MAX,"max_leases":1}),
        serde_json::json!({"classes":[{"bytes":1,"slots":u64::from(u32::MAX)+1}],"max_bytes":u64::MAX,"max_leases":1}),
    ] {
        let mut value = original.clone();
        value["pipeline_buffers"]["mapped_pool"] = pool;
        let config: WebRtcServerConfig = serde_json::from_value(value).unwrap();
        assert!(config.validate().is_err());
        assert!(config.to_executor_config().is_err());
    }
    for pool in [
        serde_json::json!({"classes":[{"bytes":8,"slots":1,"typo":0}],"max_bytes":8,"max_leases":1}),
        serde_json::json!({"classes":[{"bytes":-1,"slots":1}],"max_bytes":8,"max_leases":1}),
        serde_json::json!({"classes":[{"bytes":8,"slots":0.5}],"max_bytes":8,"max_leases":1}),
        serde_json::json!({"classes":[{"bytes":8,"slots":1}],"max_bytes":8}),
        serde_json::json!({"classes":[{"bytes":8,"slots":1}],"max_bytes":8,"max_leases":1,"typo":0}),
    ] {
        let mut value = original.clone();
        value["pipeline_buffers"]["mapped_pool"] = pool;
        assert!(serde_json::from_value::<WebRtcServerConfig>(value).is_err());
    }
}

#[test]
fn mapped_pool_policy_requires_explicit_backend_and_preserves_heap_default() {
    assert!(WebRtcServerConfig::default().to_executor_config().is_ok());
    let config = config();
    let available = cfg!(all(
        feature = "mapped-buffers",
        any(windows, target_os = "linux")
    ));
    assert_eq!(config.validate().is_ok(), available);
    assert_eq!(config.to_executor_config().is_ok(), available);
    if !available {
        assert!(config
            .to_executor_config()
            .err()
            .unwrap()
            .to_string()
            .contains("mapped-buffers"));
    }
}

#[cfg(all(feature = "mapped-buffers", any(windows, target_os = "linux")))]
#[tokio::test]
async fn mapped_pool_policy_opus_produces_in_session_pool_and_retains_slot_after_shutdown() {
    opus_session_roundtrip(false).await;
}

#[cfg(all(feature = "mapped-buffers", any(windows, target_os = "linux")))]
#[tokio::test]
#[ignore = "requires NATIVE_NODE_TEST_WORKER"]
async fn mapped_pool_policy_opus_crosses_actual_worker_without_staging() {
    opus_session_roundtrip(true).await;
}

#[cfg(all(feature = "mapped-buffers", any(windows, target_os = "linux")))]
async fn opus_session_roundtrip(worker: bool) {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();
    use remotemedia_core::{
        manifest::{Manifest, NodeManifest},
        transport::{PipelineExecutor, TransportData},
    };
    use remotemedia_types::RuntimeData;
    use remotemedia_webrtc::media::audio::{AudioDecoder, AudioEncoder, AudioEncoderConfig};
    use std::{sync::Arc, time::Duration};
    let executor = PipelineExecutor::with_config(config().to_executor_config().unwrap()).unwrap();
    if worker {
        use remotemedia_core::multiprocess::native_worker::NativeWorkerFactory;
        use remotemedia_traits::buffer_contract::*;
        use std::collections::BTreeMap;
        // SAFETY: this trusted fixture honors immutable publication and never delegates mappings.
        let factory = unsafe {
            NativeWorkerFactory::for_sessions(
                std::env::var("NATIVE_NODE_TEST_WORKER").unwrap().into(),
                "MappedNativeFixture".into(),
            )
        }
        .with_buffer_contract(|_, protocol| {
            let port = BufferPortContract {
                layouts: vec![BufferLayout::AudioInterleavedF32],
                domains: vec![BufferDomain::SessionMapped],
                max_payload_bytes: 3840,
            };
            Ok(Some(BufferContract {
                version: BUFFER_CONTRACT_VERSION,
                protocol: protocol.clone(),
                inputs: BTreeMap::from([(String::new(), port.clone())]),
                outputs: BTreeMap::from([(String::new(), port)]),
                production: BufferProduction::ForwardOrOutputAllocator,
                retention: Some(BufferRetention {
                    owners: 2,
                    bytes: 7680,
                }),
            }))
        });
        executor.register_session_worker(Arc::new(factory)).await;
    }
    let graph = Arc::new(Manifest {
        nodes: vec![NodeManifest {
            id: "pass".into(),
            node_type: if worker {
                "MappedNativeFixture"
            } else {
                "PassThrough"
            }
            .into(),
            params: serde_json::json!({"reactive":true,"multi_input":false,"transform_stream":true,"forward":true,
                "passthrough_layouts":["audio_interleaved_f32"],
                "buffer_limits":{"domain":"session_mapped","max_input_bytes":3840,"max_output_bytes":3840,
                    "retention":{"owners":2,"bytes":7680}}}),
            ..Default::default()
        }],
        ..Default::default()
    });
    let mut session = executor.create_session(graph.clone()).await.unwrap();
    let mut other = executor.create_session(graph).await.unwrap();
    let pool = session.mapped_pool().unwrap().clone();
    let foreign = other.mapped_pool().unwrap().clone();
    assert_ne!(pool.session_id(), foreign.session_id());
    let allocator = session.output_allocator();
    let copies = session.mapped_input_copies();
    let packet = AudioEncoder::new(AudioEncoderConfig::default())
        .unwrap()
        .encode(&vec![0.25; 960])
        .unwrap();
    let expected = AudioDecoder::new(AudioEncoderConfig::default())
        .unwrap()
        .decode(&packet)
        .unwrap();
    let mut decoder = AudioDecoder::new(AudioEncoderConfig::default()).unwrap();
    let pcm = decoder
        .decode_with_allocator(&packet, allocator.as_ref())
        .unwrap();
    assert!(pool.owns_buffer(&pcm));
    assert!(!foreign.owns_buffer(&pcm));
    assert_eq!(pcm.as_slice(), expected);
    let pointer = pcm.as_ptr();
    assert!(decoder
        .decode_with_allocator(&packet, allocator.as_ref())
        .is_err());
    session
        .send_input(TransportData::new(RuntimeData::Audio {
            samples: pcm.into(),
            sample_rate: 48000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        }))
        .await
        .unwrap();
    let output = tokio::time::timeout(Duration::from_secs(5), session.recv_output())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(pool.owns_payload(&output.data));
    let RuntimeData::Audio { samples, .. } = output.data else {
        panic!("audio expected")
    };
    assert_eq!(samples.as_ptr(), pointer);
    let retained = samples.into_shared().slice(1..8).unwrap();
    session.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), session.wait())
        .await
        .unwrap()
        .unwrap();
    other.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), other.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.as_slice(), &expected[1..8]);
    if worker {
        let reports = copies.reports();
        let report = &reports["pass"];
        assert_eq!(report.policy, CopyPolicy::RequireZeroCopy);
        assert!(report
            .entries
            .iter()
            .all(|(_, count)| count.completed == 0 && count.rejected == 0));
    }
    assert!(allocator.allocate_audio(960).is_err());
    drop(retained);
    let replacement = allocator.allocate_audio(960).unwrap().freeze();
    assert!(pool.owns_buffer(&replacement));
}

#[cfg(feature = "python")]
#[test]
fn mapped_pool_policy_python_registration_and_immutable_selection() {
    use pyo3::{
        prelude::*,
        types::{PyDict, PyModule},
    };
    Python::attach(|py| {
        let module = PyModule::new(py, "runtime").unwrap();
        crate::remotemedia_ffi(&module).unwrap();
        let rtc = module.getattr("webrtc").unwrap();
        let class = rtc
            .getattr("MappedSizeClass")
            .unwrap()
            .call1((3840, 1))
            .unwrap();
        assert!(class.setattr("bytes", 8).is_err());
        let pool = rtc
            .getattr("MappedPoolConfig")
            .unwrap()
            .call1((vec![class], 3840, 4));
        if !cfg!(all(
            feature = "mapped-buffers",
            any(windows, target_os = "linux")
        )) {
            assert!(pool.err().unwrap().to_string().contains("mapped-buffers"));
            return;
        }
        let pool = pool.unwrap();
        assert!(pool.setattr("max_bytes", 8).is_err());
        // A getter returns a detached list, never a writable alias of config state.
        pool.getattr("classes")
            .unwrap()
            .call_method0("clear")
            .unwrap();
        assert_eq!(pool.getattr("classes").unwrap().len().unwrap(), 1);
        let kwargs = PyDict::new(py);
        kwargs.set_item("mapped_pool", pool).unwrap();
        let buffers = rtc
            .getattr("PipelineBufferConfig")
            .unwrap()
            .call(("require_zero_copy", 64, 4194304), Some(&kwargs))
            .unwrap();
        let kwargs = PyDict::new(py);
        kwargs.set_item("port", 50051).unwrap();
        kwargs.set_item("pipeline_buffers", buffers).unwrap();
        let config = rtc
            .getattr("WebRtcServerConfig")
            .unwrap()
            .call((), Some(&kwargs))
            .unwrap();
        let config = config
            .extract::<PyRef<'_, crate::python::webrtc::config::WebRtcServerConfig>>()
            .unwrap()
            .to_core_config()
            .unwrap();
        assert!(config.to_executor_config().is_ok());
        assert_eq!(
            config
                .pipeline_buffers
                .unwrap()
                .mapped_pool
                .unwrap()
                .classes[0]
                .bytes,
            3840
        );
    });
}

#[cfg(feature = "napi")]
#[test]
fn mapped_pool_policy_node_rejects_unsafe_integer_geometry() {
    use crate::napi::webrtc::config::{MappedPoolConfig as NodePool, MappedSizeClass as NodeClass};
    for invalid in [-1.0, 0.5, f64::NAN, f64::INFINITY, 9007199254740992.0] {
        for field in 0..4 {
            let mut values = [8.0, 1.0, 8.0, 1.0];
            values[field] = invalid;
            let pool = NodePool {
                classes: vec![NodeClass {
                    bytes: values[0],
                    slots: values[1],
                }],
                max_bytes: values[2],
                max_leases: values[3],
            };
            assert!(pool
                .to_core_config()
                .unwrap_err()
                .to_string()
                .contains("safe integer"));
        }
    }
    let pool = NodePool {
        classes: vec![NodeClass {
            bytes: 3840.0,
            slots: 1.0,
        }],
        max_bytes: 3840.0,
        max_leases: 4.0,
    };
    assert_eq!(
        pool.to_core_config().is_ok(),
        cfg!(all(
            feature = "mapped-buffers",
            any(windows, target_os = "linux")
        ))
    );
}
