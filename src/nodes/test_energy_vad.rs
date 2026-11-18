//! `TestEnergyVadNode`: a deterministic stand-in for a VAD node, for tests.
//!
//! It is not a speech detector: it reports "speech" whenever the input is
//! loud (RMS at or above `threshold` for `min_speech_ms`) and "silence" once
//! it has been quiet for `hangover_ms`. The events have the shape the real
//! VAD nodes emit (`has_speech`, `is_speech_start`, `is_speech_end`), which is
//! what `HostedTurnNode` and `RealtimeEventsNode` read on their `vad` ports.
//! Anything that is not audio is dropped, and the audio itself is not
//! forwarded.
//!
//! It exists so that barge-in can be exercised end to end, by a test that
//! plays a tone into the microphone, on a machine with no VAD model
//! installed. See also `TestToneTtsNode`.

use crate::data::RuntimeData;
use crate::error::Error;
use crate::nodes::schema::{NodeSchema, RuntimeDataType};
use crate::nodes::{
    InstanceScope, StreamingNode, StreamingNodeFactory, SyncNodeWrapper, SyncStreamingNode,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The node type name in manifests.
pub const TEST_ENERGY_VAD_NODE: &str = "TestEnergyVadNode";

/// `TestEnergyVadNode` parameters. None is required.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct TestEnergyVadConfig {
    /// RMS level, 0 to 1, at or above which a frame is loud.
    pub threshold: f32,
    /// Loud audio needed before a speech start is reported, in milliseconds.
    #[serde(alias = "minSpeechMs")]
    pub min_speech_ms: u32,
    /// Quiet audio needed before a speech end is reported, in milliseconds.
    #[serde(alias = "hangoverMs")]
    pub hangover_ms: u32,
}

impl Default for TestEnergyVadConfig {
    fn default() -> Self {
        Self {
            threshold: 0.05,
            min_speech_ms: 60,
            hangover_ms: 300,
        }
    }
}

#[derive(Debug, Default)]
struct State {
    speaking: bool,
    /// Milliseconds of loud audio in a row while silent, or of quiet audio
    /// in a row while speaking.
    run_ms: f64,
}

/// See the module documentation.
pub struct TestEnergyVadNode {
    config: TestEnergyVadConfig,
    state: Mutex<State>,
}

impl TestEnergyVadNode {
    pub fn new(config: TestEnergyVadConfig) -> Self {
        Self {
            config,
            state: Mutex::new(State::default()),
        }
    }
}

impl SyncStreamingNode for TestEnergyVadNode {
    fn node_type(&self) -> &str {
        TEST_ENERGY_VAD_NODE
    }

    /// The state is one speaker's.
    fn instance_scope(&self) -> Option<InstanceScope> {
        Some(InstanceScope::PerParticipant)
    }

    fn process(&self, _data: RuntimeData) -> Result<RuntimeData, Error> {
        Err(Error::Execution(
            "TestEnergyVadNode runs in a streaming session".into(),
        ))
    }

    fn process_streaming(
        &self,
        data: RuntimeData,
        _session_id: Option<&str>,
        callback: &mut dyn FnMut(RuntimeData) -> Result<(), Error>,
    ) -> Result<usize, Error> {
        let RuntimeData::Audio {
            samples,
            sample_rate,
            channels,
            ..
        } = data
        else {
            return Ok(0);
        };
        if samples.is_empty() {
            return Ok(0);
        }
        let sum: f64 = samples.iter().map(|s| f64::from(*s) * f64::from(*s)).sum();
        let rms = (sum / samples.len() as f64).sqrt();
        let frames = samples.len() as f64 / f64::from(channels.max(1));
        let ms = frames * 1000.0 / f64::from(sample_rate.max(1));
        let loud = rms >= f64::from(self.config.threshold);

        let mut state = self.state.lock();
        // A run is loud audio while silent, quiet audio while speaking.
        if loud != state.speaking {
            state.run_ms += ms;
        } else {
            state.run_ms = 0.0;
        }
        let needed = if state.speaking {
            self.config.hangover_ms
        } else {
            self.config.min_speech_ms
        };
        if state.run_ms < f64::from(needed) {
            return Ok(0);
        }
        state.speaking = !state.speaking;
        state.run_ms = 0.0;
        let start = state.speaking;
        drop(state);
        callback(RuntimeData::Json(json!({
            "has_speech": start,
            "is_speech_start": start,
            "is_speech_end": !start,
        })))?;
        Ok(1)
    }
}

/// Factory for [`TestEnergyVadNode`].
pub struct TestEnergyVadNodeFactory;

impl StreamingNodeFactory for TestEnergyVadNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let params = if params.is_null() {
            json!({})
        } else {
            params.clone()
        };
        let config: TestEnergyVadConfig = serde_json::from_value(params).map_err(|error| {
            Error::Execution(format!(
                "{TEST_ENERGY_VAD_NODE} '{node_id}': invalid params: {error}"
            ))
        })?;
        Ok(Box::new(SyncNodeWrapper(TestEnergyVadNode::new(config))))
    }

    fn node_type(&self) -> &str {
        TEST_ENERGY_VAD_NODE
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }

    fn schema(&self) -> Option<NodeSchema> {
        Some(
            NodeSchema::new(TEST_ENERGY_VAD_NODE)
                .description(
                    "Test stand-in for a VAD node, not a speech detector: reports a speech \
                     start when the input is loud and a speech end once it is quiet again.",
                )
                .category("testing")
                .accepts([RuntimeDataType::Audio])
                .produces([RuntimeDataType::Json])
                .config_schema_from::<TestEnergyVadConfig>(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 20 ms of audio at 48 kHz with the given amplitude.
    fn frame(amplitude: f32) -> RuntimeData {
        let samples: Vec<f32> = (0..960)
            .map(|n| if n % 2 == 0 { amplitude } else { -amplitude })
            .collect();
        RuntimeData::Audio {
            samples: samples.into(),
            sample_rate: 48000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        }
    }

    fn feed(node: &TestEnergyVadNode, data: RuntimeData) -> Vec<Value> {
        let mut out = Vec::new();
        node.process_streaming(data, None, &mut |item| {
            if let RuntimeData::Json(value) = item {
                out.push(value);
            }
            Ok(())
        })
        .unwrap();
        out
    }

    #[test]
    fn loud_audio_starts_speech_and_quiet_audio_ends_it() {
        let node = TestEnergyVadNode::new(TestEnergyVadConfig {
            threshold: 0.05,
            min_speech_ms: 60,
            hangover_ms: 100,
        });
        // Silence reports nothing.
        for _ in 0..10 {
            assert!(feed(&node, frame(0.0)).is_empty());
        }
        // 60 ms of loud audio: the start is reported on the third frame.
        assert!(feed(&node, frame(0.3)).is_empty());
        assert!(feed(&node, frame(0.3)).is_empty());
        assert_eq!(
            feed(&node, frame(0.3)),
            vec![json!({ "has_speech": true, "is_speech_start": true, "is_speech_end": false })]
        );
        // More loud audio reports nothing; a short gap does not end it.
        assert!(feed(&node, frame(0.3)).is_empty());
        assert!(feed(&node, frame(0.0)).is_empty());
        assert!(feed(&node, frame(0.3)).is_empty());
        // 100 ms of quiet: the end is reported on the fifth frame.
        for _ in 0..4 {
            assert!(feed(&node, frame(0.0)).is_empty());
        }
        assert_eq!(
            feed(&node, frame(0.0)),
            vec![json!({ "has_speech": false, "is_speech_start": false, "is_speech_end": true })]
        );
    }

    #[test]
    fn a_short_burst_is_not_speech_and_other_data_is_dropped() {
        let node = TestEnergyVadNode::new(TestEnergyVadConfig::default());
        assert!(feed(&node, frame(0.3)).is_empty());
        assert!(feed(&node, frame(0.0)).is_empty());
        assert!(feed(&node, frame(0.3)).is_empty());
        assert!(feed(&node, frame(0.0)).is_empty());
        assert!(feed(&node, RuntimeData::Text("hello".into())).is_empty());
        assert!(feed(
            &node,
            RuntimeData::Json(json!({ "v": 1, "type": "text", "text": "hi" }))
        )
        .is_empty());
    }

    #[test]
    fn the_node_is_per_participant() {
        let node = TestEnergyVadNode::new(TestEnergyVadConfig::default());
        assert_eq!(node.instance_scope(), Some(InstanceScope::PerParticipant));
        assert!(TestEnergyVadNodeFactory
            .create("vad".into(), &json!({ "hangoverMs": 200 }), None)
            .is_ok());
        assert!(TestEnergyVadNodeFactory
            .create("vad".into(), &json!({ "threshold": "loud" }), None)
            .is_err());
    }
}
