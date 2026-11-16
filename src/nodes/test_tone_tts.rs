//! `TestToneTtsNode`: a deterministic stand-in for a TTS node, for tests.
//!
//! It is not speech synthesis. Each text input becomes a sine tone whose
//! length is proportional to the text (`ms_per_char`), emitted as audio
//! frames of `frame_ms`, followed by the end mark a TTS node sends after a
//! text chunk's audio (`{ "kind": "speech_end" }`). The node adds no
//! metadata: the session router keeps the input's `participant.*` fields
//! (the turn id, chunk index and audience) on every output, exactly as it
//! does for a real TTS node.
//!
//! It exists so that a pipeline with `HostedTurnNode` and
//! `RealtimeEventsNode` can be exercised end to end (hearing a reply,
//! `speaking` status, barge-in with a spoken prefix, realtime `audio-delta`
//! and truncation) on a machine with no speech model installed.

use crate::data::RuntimeData;
use crate::error::Error;
use crate::nodes::schema::{NodeSchema, RuntimeDataType};
use crate::nodes::{StreamingNode, StreamingNodeFactory, SyncNodeWrapper, SyncStreamingNode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The node type name in manifests.
pub const TEST_TONE_TTS_NODE: &str = "TestToneTtsNode";

/// `TestToneTtsNode` parameters. None is required.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct TestToneTtsConfig {
    /// Output sample rate in Hz.
    #[serde(alias = "sampleRate")]
    #[schemars(range(min = 8000, max = 48000))]
    pub sample_rate: u32,
    /// Milliseconds of tone per character of text (whitespace included).
    #[serde(alias = "msPerChar")]
    pub ms_per_char: u32,
    /// Length of each emitted audio frame in milliseconds.
    #[serde(alias = "frameMs")]
    pub frame_ms: u32,
    /// Tone frequency in Hz.
    pub frequency: f32,
    /// Peak amplitude, 0 to 1.
    pub amplitude: f32,
}

impl Default for TestToneTtsConfig {
    fn default() -> Self {
        Self {
            sample_rate: 24000,
            ms_per_char: 50,
            frame_ms: 100,
            frequency: 440.0,
            amplitude: 0.2,
        }
    }
}

/// See the module documentation.
pub struct TestToneTtsNode {
    config: TestToneTtsConfig,
}

impl TestToneTtsNode {
    pub fn new(config: TestToneTtsConfig) -> Self {
        Self { config }
    }

    /// Milliseconds of tone rendered for `text`.
    pub fn duration_ms(&self, text: &str) -> u64 {
        text.chars().count() as u64 * u64::from(self.config.ms_per_char)
    }
}

/// An audio generator (openspec: add-generator-cancellation). It renders a
/// call's tone at once, without waiting on the clock, so nothing of a call
/// outlives it: `DropStopsWork`. Like a TTS node, an interrupt drops its
/// queued text, which belongs to the cancelled reply.
#[async_trait::async_trait]
impl remotemedia_traits::Cancellable for TestToneTtsNode {
    fn cancel_strategy(&self) -> remotemedia_traits::CancelStrategy {
        remotemedia_traits::CancelStrategy::DropStopsWork
    }

    fn default_disposition(
        &self,
        cancel: &remotemedia_traits::CancelContext,
    ) -> remotemedia_traits::Disposition {
        remotemedia_traits::Disposition {
            pending_input: remotemedia_traits::Retention::Discard,
            ..cancel.reason.default_disposition()
        }
    }
}
impl remotemedia_traits::AudioGenerator for TestToneTtsNode {}

impl SyncStreamingNode for TestToneTtsNode {
    fn node_type(&self) -> &str {
        TEST_TONE_TTS_NODE
    }

    fn generator(&self) -> Option<remotemedia_traits::GeneratorRole<'_>> {
        remotemedia_traits::GeneratorRole::audio(self)
    }

    fn process(&self, _data: RuntimeData) -> Result<RuntimeData, Error> {
        Err(Error::Execution(
            "TestToneTtsNode runs in a streaming session".into(),
        ))
    }

    fn process_streaming(
        &self,
        data: RuntimeData,
        _session_id: Option<&str>,
        callback: &mut dyn FnMut(RuntimeData) -> Result<(), Error>,
    ) -> Result<usize, Error> {
        // Only text is spoken; anything else (control frames) is dropped.
        let RuntimeData::Text(text) = data else {
            return Ok(0);
        };
        if text.trim().is_empty() {
            return Ok(0);
        }
        let rate = self.config.sample_rate.max(1);
        let total = (self.duration_ms(&text) * u64::from(rate) / 1000) as usize;
        let frame = ((u64::from(self.config.frame_ms.max(1)) * u64::from(rate) / 1000) as usize).max(1);
        let step = 2.0 * std::f32::consts::PI * self.config.frequency / rate as f32;
        let mut emitted = 0;
        let mut at = 0;
        while at < total {
            let len = frame.min(total - at);
            let samples: Vec<f32> = (at..at + len)
                .map(|n| (n as f32 * step).sin() * self.config.amplitude)
                .collect();
            callback(RuntimeData::Audio {
                samples: samples.into(),
                sample_rate: rate,
                channels: 1,
                stream_id: None,
                timestamp_us: None,
                arrival_ts_us: None,
                metadata: None,
            })?;
            emitted += 1;
            at += len;
        }
        // What a TTS node sends when a text chunk's audio is complete.
        callback(RuntimeData::Json(json!({ "kind": "speech_end" })))?;
        Ok(emitted + 1)
    }
}

/// Factory for [`TestToneTtsNode`].
pub struct TestToneTtsNodeFactory;

impl StreamingNodeFactory for TestToneTtsNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let params = if params.is_null() { json!({}) } else { params.clone() };
        let config: TestToneTtsConfig = serde_json::from_value(params).map_err(|error| {
            Error::Execution(format!("{TEST_TONE_TTS_NODE} '{node_id}': invalid params: {error}"))
        })?;
        Ok(Box::new(SyncNodeWrapper(TestToneTtsNode::new(config))))
    }

    fn node_type(&self) -> &str {
        TEST_TONE_TTS_NODE
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }

    fn schema(&self) -> Option<NodeSchema> {
        Some(
            NodeSchema::new(TEST_TONE_TTS_NODE)
                .description(
                    "Test stand-in for a TTS node, not speech synthesis: renders each text \
                     input as a sine tone whose length is proportional to the text, then a \
                     `speech_end` mark.",
                )
                .category("testing")
                .generator(
                    remotemedia_traits::GeneratedModality::Audio,
                    remotemedia_traits::CancelStrategy::DropStopsWork,
                )
                .accepts([RuntimeDataType::Text])
                .produces([RuntimeDataType::Audio, RuntimeDataType::Json])
                .config_schema_from::<TestToneTtsConfig>(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(node: &TestToneTtsNode, data: RuntimeData) -> Vec<RuntimeData> {
        let mut out = Vec::new();
        node.process_streaming(data, None, &mut |item| {
            out.push(item);
            Ok(())
        })
        .unwrap();
        out
    }

    #[test]
    fn text_becomes_a_tone_of_proportional_length_then_an_end_mark() {
        let node = TestToneTtsNode::new(TestToneTtsConfig {
            sample_rate: 16000,
            ms_per_char: 25,
            frame_ms: 100,
            ..Default::default()
        });
        // 10 characters -> 250 ms -> 4000 samples in frames of 1600.
        let out = run(&node, RuntimeData::Text("0123456789".into()));
        assert_eq!(out.len(), 4);
        let mut samples = 0;
        let mut peak = 0f32;
        for item in &out[..3] {
            let RuntimeData::Audio { samples: frame, sample_rate, channels, .. } = item else {
                panic!("expected audio");
            };
            assert_eq!((*sample_rate, *channels), (16000, 1));
            let frame: Vec<f32> = frame.to_vec();
            samples += frame.len();
            peak = frame.iter().fold(peak, |peak, s| peak.max(s.abs()));
        }
        assert_eq!(samples, 4000);
        assert!(peak > 0.15 && peak <= 0.2, "peak {peak}");
        assert!(matches!(&out[3], RuntimeData::Json(v) if v == &json!({ "kind": "speech_end" })));
    }

    #[test]
    fn anything_but_text_and_blank_text_produce_nothing() {
        let node = TestToneTtsNode::new(TestToneTtsConfig::default());
        assert!(run(&node, RuntimeData::Json(json!({ "v": 1, "type": "interrupt" }))).is_empty());
        assert!(run(&node, RuntimeData::Text("  ".into())).is_empty());
    }

    #[test]
    fn the_factory_reads_parameters_and_refuses_unreadable_ones() {
        let factory = TestToneTtsNodeFactory;
        assert!(factory.create("tts".into(), &json!({ "sampleRate": 24000, "ms_per_char": 10 }), None).is_ok());
        assert!(factory.create("tts".into(), &Value::Null, None).is_ok());
        assert!(factory.create("tts".into(), &json!({ "sample_rate": "fast" }), None).is_err());
        assert!(factory.is_multi_output_streaming());
    }
}
