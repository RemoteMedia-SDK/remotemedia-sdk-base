use remotemedia_sdk_base::manifest::*;
#[test]
fn legacy_manifest_parses_without_model_sources() {
    let json = r#"{"version":"v1","metadata":{"name":"legacy"},"nodes":[{"id":"n1","node_type":"WhisperNode","params":{"model_path":"/tmp/model.tflite"}}],"connections":[]}"#;
    let m: Manifest = serde_json::from_str(json).unwrap();
    assert_eq!(m.nodes.len(), 1);
    assert!(m.nodes[0].params.get("model_path").is_some());
    assert!(m.nodes[0].params.get("model_sources").is_none());
}
