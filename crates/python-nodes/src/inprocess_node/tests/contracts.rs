use super::*;
use remotemedia_core::transport::buffer_preflight::inspect_buffer_sharing;
use remotemedia_core::{manifest::Manifest, nodes::StreamingNodeRegistry};
use remotemedia_traits::buffer_contract::*;

pub(super) fn declaration(production: BufferProduction) -> BufferContract {
    let port = BufferPortContract {
        layouts: vec![BufferLayout::AudioInterleavedF32],
        domains: vec![BufferDomain::Host],
        max_payload_bytes: 16,
    };
    BufferContract {
        version: BUFFER_CONTRACT_VERSION,
        protocol: super::super::contracts::protocol(),
        inputs: [(String::new(), port.clone())].into(),
        outputs: [(String::new(), port)].into(),
        production,
        retention: Some(BufferRetention {
            owners: 2,
            bytes: 32,
        }),
    }
}

pub(super) fn factory(contract: BufferContract) -> DynamicInProcessPythonNodeFactory {
    DynamicInProcessPythonNodeFactory::new(
        PythonNodeConfig::new("SharedNode")
            .with_python_class("inprocess_runtime_fixture.SharedNode")
            .with_shared_buffer_contract(contract),
    )
}

pub(super) fn manifest(nodes: Value, connections: Value) -> Arc<Manifest> {
    Arc::new(
        serde_json::from_value(serde_json::json!({
            "version":"v1", "metadata":{"name":"shared-python-preflight"},
            "nodes":nodes, "connections":connections,
        }))
        .unwrap(),
    )
}

#[test]
fn preflight_accepts_bound_python_protocol_without_loading_python() {
    let config = PythonNodeConfig::new("SharedNode")
        .with_python_class("does_not_exist.Node")
        .with_shared_buffer_contract(declaration(BufferProduction::ForwardOrOutputAllocator));
    let factory = DynamicInProcessPythonNodeFactory::new(config);
    assert!(!factory.supports_buffer_protocol(&BufferProtocol::RustOwners));
    assert!(!factory.supports_buffer_protocol(&BufferProtocol::InProcessPython { buffers: 2 }));
    let mut registry = StreamingNodeRegistry::new();
    registry.register(Arc::new(factory));
    let manifest = manifest(
        serde_json::json!([{"id":"python","node_type":"SharedNode"}]),
        serde_json::json!([]),
    );
    let report = inspect_buffer_sharing(
        &manifest,
        &registry,
        BufferRetention {
            owners: 2,
            bytes: 32,
        },
    );
    assert!(report.issues.is_empty(), "{:?}", report.issues);
    report.enforce(CopyPolicy::RequireZeroCopy).unwrap();
}

#[test]
fn unsupported_adapter_promises_fail_both_inspection_and_creation() {
    let invalid: &[fn(&mut BufferContract)] = &[
        |c| c.version += 1,
        |c| c.protocol = BufferProtocol::InProcessPython { buffers: 2 },
        |c| c.protocol = BufferProtocol::RustOwners,
        |c| c.production = BufferProduction::PrivateStorage,
        |c| {
            let port = c.inputs.remove("").unwrap();
            c.inputs.insert("named".into(), port);
        },
        |c| c.outputs.clear(),
        |c| c.inputs.get_mut("").unwrap().domains = vec![BufferDomain::Device("cuda".into())],
        |c| c.inputs.get_mut("").unwrap().layouts = vec![BufferLayout::TensorPythonIpc],
        |c| c.inputs.get_mut("").unwrap().max_payload_bytes = 0,
        |c| c.inputs.get_mut("").unwrap().max_payload_bytes = u64::MAX,
    ];
    for mutate in invalid {
        let mut contract = declaration(BufferProduction::ForwardInput);
        mutate(&mut contract);
        let f = factory(contract);
        assert!(f.try_buffer_contract(&serde_json::json!({})).is_err());
        assert!(f
            .create("python".into(), &serde_json::json!({}), None)
            .is_err());
    }
}

#[test]
fn declaration_cannot_survive_switching_to_legacy_execution() {
    let config = PythonNodeConfig::new("SharedNode")
        .with_shared_buffer_contract(declaration(BufferProduction::ForwardInput))
        .with_inprocess(true);
    assert!(DynamicInProcessPythonNodeFactory::new(config)
        .try_buffer_contract(&serde_json::json!({}))
        .is_err());
}

#[test]
fn omitted_or_unbounded_declarations_remain_strict_preflight_errors() {
    let manifest = manifest(
        serde_json::json!([{"id":"python","node_type":"SharedNode"}]),
        serde_json::json!([]),
    );
    for config in [PythonNodeConfig::new("SharedNode").with_shared_buffers(), {
        let mut contract = declaration(BufferProduction::ForwardInput);
        contract.retention = None;
        PythonNodeConfig::new("SharedNode").with_shared_buffer_contract(contract)
    }] {
        let mut registry = StreamingNodeRegistry::new();
        registry.register(Arc::new(DynamicInProcessPythonNodeFactory::new(config)));
        let report = inspect_buffer_sharing(
            &manifest,
            &registry,
            BufferRetention {
                owners: 4,
                bytes: 64,
            },
        );
        assert!(report.enforce(CopyPolicy::RequireZeroCopy).is_err());
    }
}
