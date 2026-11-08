use super::*;

fn buffers(policy: CopyPolicy) -> PipelineBufferConfig {
    PipelineBufferConfig {
        policy,
        node_retention_owners: 64,
        node_retention_bytes: 4194304,
    }
}

#[test]
fn pipeline_policy_serde_requires_explicit_complete_budgets() {
    let mut value = serde_json::to_value(WebRtcServerConfig::default()).unwrap();
    assert!(serde_json::from_value::<WebRtcServerConfig>(value.clone())
        .unwrap()
        .pipeline_buffers
        .is_none());
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        value["pipeline_buffers"] = serde_json::to_value(buffers(policy)).unwrap();
        let config: WebRtcServerConfig = serde_json::from_value(value.clone()).unwrap();
        let executor = config.to_executor_config();
        assert_eq!(executor.pipeline_copy_policy, Some(policy));
        let preflight = executor.buffer_preflight.unwrap();
        assert_eq!(preflight.policy, policy);
        assert_eq!(
            (
                preflight.node_retention_budget.owners,
                preflight.node_retention_budget.bytes
            ),
            (64, 4194304)
        );
    }
    for invalid in [
        serde_json::json!({"policy":"require_zero_copy"}),
        serde_json::json!({"policy":"strict","node_retention_owners":1,"node_retention_bytes":8}),
        serde_json::json!({"policy":"require_zero_copy","node_retention_owners":-1,"node_retention_bytes":8}),
        serde_json::json!({"policy":"require_zero_copy","node_retention_owners":1,"node_retention_bytes":1.5}),
        serde_json::json!({"policy":"require_zero_copy","node_retention_owners":1,"node_retention_bytes":8,"typo":true}),
    ] {
        value["pipeline_buffers"] = invalid;
        assert!(serde_json::from_value::<WebRtcServerConfig>(value.clone()).is_err());
    }
}

fn manifest(
    node_type: &str,
    params: serde_json::Value,
) -> std::sync::Arc<remotemedia_core::manifest::Manifest> {
    use remotemedia_core::manifest::{Manifest, NodeManifest};
    std::sync::Arc::new(Manifest {
        nodes: vec![NodeManifest {
            id: "probe".into(),
            node_type: node_type.into(),
            params,
            ..Default::default()
        }],
        ..Default::default()
    })
}

#[tokio::test]
async fn pipeline_policy_controls_real_session_preflight_and_both_authorities() {
    use remotemedia_core::transport::PipelineExecutor;
    use remotemedia_types::CopyReason;
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        let config = WebRtcServerConfig {
            pipeline_buffers: Some(buffers(policy)),
            transport_materialization_policy: if policy == CopyPolicy::AllowCopies {
                CopyPolicy::RequireZeroCopy
            } else {
                CopyPolicy::AllowCopies
            },
            ..Default::default()
        };
        let executor = PipelineExecutor::with_config(config.to_executor_config()).unwrap();
        let mut session = executor
            .create_session(manifest("PassThrough", serde_json::json!({})))
            .await
            .unwrap();
        assert!(session.buffer_preflight().unwrap().issues.is_empty());
        let transport = session.transport_copy_context();
        let nodes = session.node_materialization_copies();
        assert_eq!(nodes.reports()["probe"].policy, policy);
        session.close().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), session.wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(transport.policy(), policy);
        assert_eq!(
            transport
                .materialize(CopyReason::VecMaterialization, 8, || vec![7u8; 8])
                .is_ok(),
            policy == CopyPolicy::AllowCopies
        );
        assert_eq!(nodes.reports()["probe"].policy, policy);
        let error = executor
            .create_session(manifest("CalculatorNode", serde_json::json!({})))
            .await;
        if policy == CopyPolicy::RequireZeroCopy {
            assert!(error
                .err()
                .unwrap()
                .to_string()
                .contains("no buffer contract"));
        } else {
            let mut session = error.unwrap();
            assert!(!session.buffer_preflight().unwrap().issues.is_empty());
            session.close().await.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(5), session.wait())
                .await
                .unwrap()
                .unwrap();
        }
    }
}

#[tokio::test]
async fn pipeline_policy_enforces_the_supplied_retention_budget() {
    use remotemedia_core::transport::PipelineExecutor;
    let config = WebRtcServerConfig {
        pipeline_buffers: Some(PipelineBufferConfig {
            policy: CopyPolicy::RequireZeroCopy,
            node_retention_owners: 1,
            node_retention_bytes: 8,
        }),
        ..Default::default()
    };
    let executor = PipelineExecutor::with_config(config.to_executor_config()).unwrap();
    let graph = manifest(
        "PassThrough",
        serde_json::json!({"passthrough_layouts":["binary"], "buffer_limits": {
            "domain":"host", "max_input_bytes":8, "max_output_bytes":8,
            "retention":{"owners":2,"bytes":16}
        }}),
    );
    let error = executor.create_session(graph).await.err().unwrap();
    assert!(error.to_string().contains("budget"), "{error}");
}

#[cfg(feature = "python")]
#[test]
fn pipeline_policy_python_nested_config_and_replacement() {
    use crate::python::webrtc::config::{
        PipelineBufferConfig as PyBuffers, WebRtcServerConfig as PyConfig,
    };
    use pyo3::{
        prelude::*,
        types::{PyDict, PyModule},
    };
    Python::attach(|py| {
        let module = PyModule::new(py, "runtime").unwrap();
        crate::remotemedia_ffi(&module).unwrap();
        assert!(module
            .getattr("webrtc")
            .unwrap()
            .getattr("PipelineBufferConfig")
            .unwrap()
            .is(py.get_type::<PyBuffers>()));
        let kwargs = PyDict::new(py);
        kwargs.set_item("port", 50051).unwrap();
        let object = py.get_type::<PyConfig>().call((), Some(&kwargs)).unwrap();
        assert!(object
            .extract::<PyRef<'_, PyConfig>>()
            .unwrap()
            .to_core_config()
            .unwrap()
            .pipeline_buffers
            .is_none());
        for policy in ["require_zero_copy", "allow_copies"] {
            let selection = py
                .get_type::<PyBuffers>()
                .call1((policy, 64u64, 4194304u64))
                .unwrap();
            assert!(selection.setattr("policy", "typo").is_err());
            object.setattr("pipeline_buffers", &selection).unwrap();
            let core = object
                .extract::<PyRef<'_, PyConfig>>()
                .unwrap()
                .to_core_config()
                .unwrap();
            assert_eq!(
                core.to_executor_config().pipeline_copy_policy,
                Some(parse_copy_policy(policy, "policy").unwrap())
            );
        }
        object.setattr("pipeline_buffers", py.None()).unwrap();
        assert!(object
            .extract::<PyRef<'_, PyConfig>>()
            .unwrap()
            .to_core_config()
            .unwrap()
            .pipeline_buffers
            .is_none());
        assert!(py
            .get_type::<PyBuffers>()
            .call1(("require_zero_copy",))
            .is_err());
        assert!(py
            .get_type::<PyBuffers>()
            .call1(("typo", 1u64, 8u64))
            .is_err());
        assert!(py
            .get_type::<PyBuffers>()
            .call1(("require_zero_copy", -1i64, 8u64))
            .is_err());
        assert!(py
            .get_type::<PyBuffers>()
            .call1(("require_zero_copy", 1u64, 1.5f64))
            .is_err());
        let max = py
            .get_type::<PyBuffers>()
            .call1(("allow_copies", u64::MAX, u64::MAX))
            .unwrap();
        kwargs.set_item("pipeline_buffers", max).unwrap();
        let object = py.get_type::<PyConfig>().call((), Some(&kwargs)).unwrap();
        assert_eq!(
            object
                .extract::<PyRef<'_, PyConfig>>()
                .unwrap()
                .to_core_config()
                .unwrap()
                .pipeline_buffers
                .unwrap()
                .node_retention_bytes,
            u64::MAX
        );
    });
}

#[cfg(feature = "napi")]
#[test]
fn pipeline_policy_node_nested_config_validates_safe_integer_budgets() {
    use crate::napi::webrtc::config::{
        PipelineBufferConfig as NodeBuffers, WebRtcServerConfig as NodeConfig,
    };
    let mut config = NodeConfig {
        port: Some(50051),
        signaling_url: None,
        manifest: r#"{"nodes":[],"connections":[]}"#.into(),
        stun_servers: vec!["stun:localhost:3478".into()],
        turn_servers: None,
        max_peers: None,
        audio_codec: None,
        video_codec: None,
        transport_materialization_policy: None,
        pipeline_buffers: None,
    };
    for policy in ["require_zero_copy", "allow_copies"] {
        config.pipeline_buffers = Some(NodeBuffers {
            policy: policy.into(),
            node_retention_owners: 64.0,
            node_retention_bytes: 4194304.0,
        });
        assert_eq!(
            config
                .to_core_config()
                .unwrap()
                .to_executor_config()
                .pipeline_copy_policy,
            Some(parse_copy_policy(policy, "policy").unwrap())
        );
    }
    for invalid in [-1.0, 0.5, f64::NAN, f64::INFINITY, 9007199254740992.0] {
        for owners in [true, false] {
            let selection = config.pipeline_buffers.as_mut().unwrap();
            selection.node_retention_owners = if owners { invalid } else { 64.0 };
            selection.node_retention_bytes = if owners { 4194304.0 } else { invalid };
            assert!(config
                .to_core_config()
                .unwrap_err()
                .to_string()
                .contains("safe integer"));
        }
    }
    let selection = config.pipeline_buffers.as_mut().unwrap();
    selection.node_retention_owners = 0.0;
    selection.node_retention_bytes = 9007199254740991.0;
    assert_eq!(
        config
            .to_core_config()
            .unwrap()
            .pipeline_buffers
            .unwrap()
            .node_retention_bytes,
        9007199254740991
    );
}
