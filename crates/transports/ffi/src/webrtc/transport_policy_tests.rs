use super::*;

#[test]
fn transport_policy_serde_defaults_and_rejects_unknown_values() {
    let mut value = serde_json::to_value(WebRtcServerConfig::default()).unwrap();
    value
        .as_object_mut()
        .unwrap()
        .remove("transport_materialization_policy");
    let config: WebRtcServerConfig = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(
        config.transport_materialization_policy,
        CopyPolicy::AllowCopies
    );
    for (name, policy) in [
        ("allow_copies", CopyPolicy::AllowCopies),
        ("require_zero_copy", CopyPolicy::RequireZeroCopy),
    ] {
        value["transport_materialization_policy"] = name.into();
        let config: WebRtcServerConfig = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(config.transport_materialization_policy, policy);
        assert_eq!(parse_transport_copy_policy(name).unwrap(), policy);
    }
    for invalid in [
        serde_json::Value::Null,
        true.into(),
        "strict".into(),
        "".into(),
    ] {
        value["transport_materialization_policy"] = invalid;
        assert!(serde_json::from_value::<WebRtcServerConfig>(value.clone()).is_err());
    }
}

#[tokio::test]
async fn transport_policy_server_config_reaches_real_session_authority() {
    use remotemedia_core::{
        manifest::{Manifest, NodeManifest},
        transport::PipelineExecutor,
    };
    use remotemedia_types::CopyReason;
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        let config = WebRtcServerConfig {
            transport_materialization_policy: policy,
            ..Default::default()
        };
        let executor = PipelineExecutor::with_config(config.to_executor_config().unwrap()).unwrap();
        let mut session = executor
            .create_session(std::sync::Arc::new(Manifest {
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
        let copies = session.transport_copy_context();
        session.close().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), session.wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(copies.policy(), policy);
        let result = copies.materialize(CopyReason::VecMaterialization, 8, || vec![7u8; 8]);
        assert_eq!(result.is_ok(), policy == CopyPolicy::AllowCopies);
        let report = copies.report();
        let counts = &report
            .entries
            .iter()
            .find(|(r, _)| *r == CopyReason::VecMaterialization)
            .unwrap()
            .1;
        assert_eq!(
            (counts.copied_bytes, counts.rejected_bytes),
            if policy == CopyPolicy::AllowCopies {
                (8, 0)
            } else {
                (0, 8)
            }
        );
    }
}

#[cfg(feature = "python")]
#[test]
fn transport_policy_python_constructor_and_mutation_validate() {
    use crate::python::webrtc::config::WebRtcServerConfig as PyConfig;
    use pyo3::{prelude::*, types::PyDict};
    Python::attach(|py| {
        for (name, expected) in [
            (None, CopyPolicy::AllowCopies),
            (Some("allow_copies"), CopyPolicy::AllowCopies),
            (Some("require_zero_copy"), CopyPolicy::RequireZeroCopy),
        ] {
            let kwargs = PyDict::new(py);
            kwargs.set_item("port", 50051).unwrap();
            if let Some(name) = name {
                kwargs
                    .set_item("transport_materialization_policy", name)
                    .unwrap();
            }
            let object = py.get_type::<PyConfig>().call((), Some(&kwargs)).unwrap();
            let config = object
                .extract::<PyRef<'_, PyConfig>>()
                .unwrap()
                .to_core_config()
                .unwrap();
            assert_eq!(
                config
                    .to_executor_config()
                    .unwrap()
                    .transport_materialization_policy,
                expected
            );
            assert_eq!(
                object
                    .getattr("transport_materialization_policy")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                name.unwrap_or("allow_copies")
            );
            object
                .setattr("transport_materialization_policy", "typo")
                .unwrap();
            assert!(object
                .extract::<PyRef<'_, PyConfig>>()
                .unwrap()
                .to_core_config()
                .is_err());
        }
        let kwargs = PyDict::new(py);
        kwargs
            .set_item("transport_materialization_policy", "typo")
            .unwrap();
        let error = py
            .get_type::<PyConfig>()
            .call((), Some(&kwargs))
            .unwrap_err();
        assert!(error.is_instance_of::<pyo3::exceptions::PyValueError>(py));
        kwargs
            .set_item("transport_materialization_policy", py.None())
            .unwrap();
        assert!(py.get_type::<PyConfig>().call((), Some(&kwargs)).is_err());
    });
}

#[cfg(feature = "napi")]
#[test]
fn transport_policy_node_conversion_validates_and_preserves_selection() {
    use crate::napi::webrtc::config::WebRtcServerConfig as NodeConfig;
    let mut config = NodeConfig {
        port: Some(50051),
        signaling_url: None,
        manifest: r#"{"nodes": [], "connections": []}"#.into(),
        stun_servers: vec!["stun:localhost:3478".into()],
        turn_servers: None,
        max_peers: None,
        audio_codec: None,
        video_codec: None,
        transport_materialization_policy: None,
        pipeline_buffers: None,
    };
    for (value, expected) in [
        (None, CopyPolicy::AllowCopies),
        (Some("allow_copies"), CopyPolicy::AllowCopies),
        (Some("require_zero_copy"), CopyPolicy::RequireZeroCopy),
    ] {
        config.transport_materialization_policy = value.map(str::to_owned);
        assert_eq!(
            config
                .to_core_config()
                .unwrap()
                .to_executor_config()
                .unwrap()
                .transport_materialization_policy,
            expected
        );
    }
    for invalid in ["", "strict", "RequireZeroCopy"] {
        config.transport_materialization_policy = Some(invalid.into());
        assert!(config
            .to_core_config()
            .unwrap_err()
            .to_string()
            .contains("transport_materialization_policy"));
    }
}
