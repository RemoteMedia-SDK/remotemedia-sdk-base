//! Real shared-library ABI/registration check. Build the cdylib first, then set
//! RAG_PLUGIN_PATH and run `cargo test --test rag_plugin_load -- --ignored`.
use remotemedia_plugin_sdk::{abi_stable::library::RootModule, NodePluginRef};

#[test]
#[ignore = "requires the separately built cdylib in RAG_PLUGIN_PATH"]
fn rag_factory_loads_through_the_plugin_abi() {
    let path = std::env::var("RAG_PLUGIN_PATH").expect("RAG_PLUGIN_PATH");
    let module =
        NodePluginRef::load_from_file(std::path::Path::new(&path)).expect("plugin ABI load");
    let factories = (module.list_factories())();
    let names: Vec<String> = factories
        .iter()
        .map(|factory| factory.node_type().to_string())
        .collect();
    for expected in [
        "WhisperLiteRtNode",
        "GemmaAssistantNode",
        "RagAssistantNode",
        "KokoroTtsNode",
    ] {
        assert!(
            names.iter().any(|name| name == expected),
            "missing {expected}: {names:?}"
        );
    }
    let factory = factories
        .iter()
        .find(|factory| factory.node_type().as_str() == "RagAssistantNode")
        .unwrap();
    let error = match factory.create("{}".into()).into_result() {
        Err(error) => error.to_string(),
        Ok(_) => panic!("invalid configuration unexpectedly loaded"),
    };
    assert!(
        error.contains("rag_url"),
        "factory did not execute config validation: {error}"
    );
}
