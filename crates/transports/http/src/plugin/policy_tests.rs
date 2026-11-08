use super::*;
use remotemedia_core::{data::RuntimeData, transport::TransportData};
use remotemedia_types::CopyPolicy;

#[test]
fn policy_configuration_accepts_only_supported_values() {
    for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
        let config = serde_json::json!({"copy_policy": policy});
        assert_eq!(copy_policy(&config).unwrap(), policy);
        HttpTransportPlugin.validate_config(&config).unwrap();
    }
    for policy in [
        serde_json::json!("invalid"),
        serde_json::json!(false),
        serde_json::Value::Null,
    ] {
        assert!(HttpTransportPlugin
            .validate_config(&serde_json::json!({"copy_policy":policy}))
            .is_err());
    }
    assert_eq!(
        copy_policy(&serde_json::Value::Null).unwrap(),
        CopyPolicy::AllowCopies
    );
}

#[tokio::test]
async fn factory_client_strict_policy_rejects_bulk_before_network_request() {
    let config = ClientConfig {
        address: "http://127.0.0.1:1".into(),
        auth_token: None,
        timeout_ms: None,
        extra_config: Some(serde_json::json!({"copy_policy":"require_zero_copy"})),
    };
    let client = HttpTransportPlugin.create_client(&config).await.unwrap();
    let manifest = Arc::new(
        serde_json::from_value(serde_json::json!({
            "version":"v1", "metadata":{}, "nodes":[], "connections":[],
        }))
        .unwrap(),
    );
    let error = client
        .execute_unary(
            manifest,
            TransportData::new(RuntimeData::Binary(vec![1u8, 2, 3].into())),
            &[],
            None,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("copy"), "{error}");
    assert!(!error.to_string().contains("connect"), "{error}");
}
