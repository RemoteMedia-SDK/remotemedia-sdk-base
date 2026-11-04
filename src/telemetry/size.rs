//! Cheap payload-size estimation for edge byte counters.

use crate::data::RuntimeData;

/// Approximate payload size of `data` in bytes.
///
/// Must stay O(1): it runs once per node output on the hot path. Unlike
/// `RuntimeData::size_bytes`, JSON / control payloads are *not* serialized —
/// they are estimated from their top-level shape, which is good enough for a
/// throughput gauge.
#[inline]
pub fn approx_payload_bytes(data: &RuntimeData) -> u64 {
    let n = match data {
        RuntimeData::Audio { samples, .. } => samples.len() * 4,
        RuntimeData::Video { pixel_data, .. } => pixel_data.len(),
        RuntimeData::Image { data, .. } => data.len(),
        RuntimeData::Tensor { data, .. } => data.len(),
        RuntimeData::Numpy { data, .. } => data.len(),
        RuntimeData::Text(s) => s.len(),
        RuntimeData::Binary(b) => b.len(),
        RuntimeData::Json(v) => approx_json(v),
        RuntimeData::ControlMessage { .. } => 32,
        RuntimeData::File { length, size, .. } => length.or(*size).unwrap_or(0) as usize,
    };
    n as u64
}

fn approx_json(v: &serde_json::Value) -> usize {
    use serde_json::Value;
    match v {
        Value::Null | Value::Bool(_) => 4,
        Value::Number(_) => 8,
        Value::String(s) => s.len() + 2,
        // Shallow: count keys/elements and string leaves one level down.
        Value::Array(a) => 2 + a.len() * 8,
        Value::Object(m) => {
            2 + m
                .iter()
                .map(|(k, v)| {
                    k.len()
                        + 4
                        + match v {
                            Value::String(s) => s.len(),
                            Value::Array(a) => a.len() * 8,
                            Value::Object(o) => o.len() * 16,
                            _ => 8,
                        }
                })
                .sum::<usize>()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::AudioSamples;

    #[test]
    fn audio_and_text_sizes() {
        let a = RuntimeData::Audio {
            samples: AudioSamples::Vec(vec![0.0; 320]),
            sample_rate: 16000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        };
        assert_eq!(approx_payload_bytes(&a), 1280);
        assert_eq!(approx_payload_bytes(&RuntimeData::Text("hello".into())), 5);
        let j = RuntimeData::Json(serde_json::json!({"text": "abcd"}));
        assert!(approx_payload_bytes(&j) > 4);
    }
}
