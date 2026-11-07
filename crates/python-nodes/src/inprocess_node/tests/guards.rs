use super::{contracts::*, *};
use remotemedia_core::transport::buffer_validation::BufferCheckedNode;
use remotemedia_traits::buffer_contract::*;
use remotemedia_types::{BoundOutputAllocator, BufferBudget, HeapOutputAllocator};

#[tokio::test]
async fn python_output_must_satisfy_the_declared_production_origin() {
    drop(node("pass"));
    for (mode, production, expected) in [
        (
            "gain",
            BufferProduction::ForwardInput,
            "does not retain an input",
        ),
        ("pass", BufferProduction::OutputAllocator, "no provenance"),
    ] {
        let contract = declaration(production);
        let node = factory(contract.clone())
            .create(
                "python".into(),
                &serde_json::json!({"mode":mode}),
                Some("session".into()),
            )
            .unwrap();
        let allocator = Arc::new(
            BoundOutputAllocator::new(Arc::new(HeapOutputAllocator))
                .with_budget(BufferBudget::new(2, 32)),
        );
        let checked =
            BufferCheckedNode::new("python".into(), contract, node, Some(allocator.clone()));
        let ctx =
            NodeRuntimeContext::for_test("session", "python").with_output_allocator(allocator);
        let error = checked.process_async(audio(), &ctx).await.unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[tokio::test]
async fn input_bounds_are_checked_before_an_unknown_python_class_is_loaded() {
    let contract = declaration(BufferProduction::ForwardInput);
    let factory = DynamicInProcessPythonNodeFactory::new(
        PythonNodeConfig::new("Unknown")
            .with_python_class("does_not_exist.Node")
            .with_shared_buffer_contract(contract.clone()),
    );
    let node = factory
        .create(
            "python".into(),
            &serde_json::json!({}),
            Some("session".into()),
        )
        .unwrap();
    let checked = BufferCheckedNode::new("python".into(), contract, node, None);
    let mut input = audio();
    let RuntimeData::Audio { samples, .. } = &mut input else {
        panic!()
    };
    *samples = vec![1.0; 8].into();
    let error = checked
        .process_async(input, &NodeRuntimeContext::for_test("session", "python"))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("input port"), "{error}");
    assert!(!error.to_string().contains("Failed to load"), "{error}");
}
