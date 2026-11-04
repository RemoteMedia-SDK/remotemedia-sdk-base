//! Redaction of sensitive node params before they leave the process.

use serde_json::Value;

/// Marker substituted for redacted values.
pub const REDACTED: &str = "«redacted»";

const SENSITIVE: &[&str] = &[
    "key",
    "token",
    "secret",
    "password",
    "passwd",
    "auth",
    "credential",
    "bearer",
    "cookie",
];

/// Is `key` the name of a param whose value must not be exposed?
pub fn is_sensitive_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    // Media vocabulary that contains "key" but is not a secret.
    if k.contains("keyframe") || k.contains("key_frame") || k.contains("hotkey") {
        return false;
    }
    SENSITIVE.iter().any(|s| k.contains(s))
}

/// Recursively replace values of sensitive keys with [`REDACTED`].
pub fn redact_params(params: &Value) -> Value {
    match params {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| {
                    if is_sensitive_key(k) && !v.is_null() {
                        (k.clone(), Value::String(REDACTED.into()))
                    } else {
                        (k.clone(), redact_params(v))
                    }
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(redact_params).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn redacts_nested_secrets_and_keeps_rest() {
        let p = json!({
            "api_key": "sk-123",
            "model": "whisper",
            "auth_token": "t",
            "nested": {"password": "p", "sample_rate": 16000},
            "list": [{"secret": "s"}],
            "keyframe_interval": 30,
            "maybe_key": null
        });
        let r = redact_params(&p);
        assert_eq!(r["api_key"], REDACTED);
        assert_eq!(r["auth_token"], REDACTED);
        assert_eq!(r["nested"]["password"], REDACTED);
        assert_eq!(r["nested"]["sample_rate"], 16000);
        assert_eq!(r["list"][0]["secret"], REDACTED);
        assert_eq!(r["model"], "whisper");
        assert_eq!(r["keyframe_interval"], 30);
        assert!(r["maybe_key"].is_null());
    }
}
