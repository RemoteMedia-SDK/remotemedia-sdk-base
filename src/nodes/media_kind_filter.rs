//! `MediaKindFilterNode`: passes on only the kinds of data it is told to.
//!
//! A session delivers each member input to every source node of the
//! pipeline. A voice pipeline has two kinds of member input: microphone audio
//! for the transcription chain, and JSON frames typed in the browser for the
//! turn node. Without a filter the typed frame also travels down the audio
//! chain (resampler, chunker and VAD pass non-audio through) and reaches the
//! turn node a second time, and every audio frame also reaches the turn node.
//! One filter at the head of each path keeps the two apart:
//!
//! ```json
//! { "id": "audio_in", "node_type": "MediaKindFilterNode", "params": { "allow": ["audio"] } }
//! { "id": "text_in",  "node_type": "MediaKindFilterNode", "params": { "allow": ["json", "text"] } }
//! ```
//!
//! Data passes unchanged, so it keeps its origin (`participant.*`).

use crate::data::RuntimeData;
use crate::error::Error;
use crate::nodes::schema::{NodeSchema, RuntimeDataType};
use crate::nodes::{StreamingNode, StreamingNodeFactory, SyncNodeWrapper, SyncStreamingNode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The node type name in manifests.
pub const MEDIA_KIND_FILTER_NODE: &str = "MediaKindFilterNode";

/// `MediaKindFilterNode` parameters.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct MediaKindFilterConfig {
    /// Kinds to pass on: `audio`, `text`, `json`. Anything else is dropped.
    pub allow: Vec<String>,
}

/// See the module documentation.
pub struct MediaKindFilterNode {
    audio: bool,
    text: bool,
    json: bool,
}

impl MediaKindFilterNode {
    pub fn new(config: &MediaKindFilterConfig) -> Result<Self, String> {
        let mut node = Self {
            audio: false,
            text: false,
            json: false,
        };
        for kind in &config.allow {
            match kind.to_ascii_lowercase().as_str() {
                "audio" => node.audio = true,
                "text" => node.text = true,
                "json" => node.json = true,
                other => return Err(format!("unknown kind '{other}' (use audio, text or json)")),
            }
        }
        Ok(node)
    }

    fn allows(&self, data: &RuntimeData) -> bool {
        match data {
            RuntimeData::Audio { .. } => self.audio,
            RuntimeData::Text(_) => self.text,
            RuntimeData::Json(_) => self.json,
            _ => false,
        }
    }
}

impl SyncStreamingNode for MediaKindFilterNode {
    fn node_type(&self) -> &str {
        MEDIA_KIND_FILTER_NODE
    }

    fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        Ok(data)
    }

    fn process_streaming(
        &self,
        data: RuntimeData,
        _session_id: Option<&str>,
        callback: &mut dyn FnMut(RuntimeData) -> Result<(), Error>,
    ) -> Result<usize, Error> {
        if !self.allows(&data) {
            return Ok(0);
        }
        callback(data)?;
        Ok(1)
    }
}

/// Factory for [`MediaKindFilterNode`].
pub struct MediaKindFilterNodeFactory;

impl StreamingNodeFactory for MediaKindFilterNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let params = if params.is_null() { json!({}) } else { params.clone() };
        let config: MediaKindFilterConfig = serde_json::from_value(params).map_err(|error| {
            Error::Execution(format!("{MEDIA_KIND_FILTER_NODE} '{node_id}': invalid params: {error}"))
        })?;
        let node = MediaKindFilterNode::new(&config)
            .map_err(|error| Error::Execution(format!("{MEDIA_KIND_FILTER_NODE} '{node_id}': {error}")))?;
        Ok(Box::new(SyncNodeWrapper(node)))
    }

    fn node_type(&self) -> &str {
        MEDIA_KIND_FILTER_NODE
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }

    fn schema(&self) -> Option<NodeSchema> {
        Some(
            NodeSchema::new(MEDIA_KIND_FILTER_NODE)
                .description("Passes on only the listed kinds of data (audio, text, json), unchanged.")
                .category("utility")
                .accepts([RuntimeDataType::Audio, RuntimeDataType::Text, RuntimeDataType::Json])
                .produces([RuntimeDataType::Audio, RuntimeDataType::Text, RuntimeDataType::Json])
                .config_schema_from::<MediaKindFilterConfig>(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn audio() -> RuntimeData {
        RuntimeData::Audio {
            samples: vec![0.0f32; 480].into(),
            sample_rate: 48000,
            channels: 1,
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us: None,
            metadata: None,
        }
    }

    fn passed(node: &MediaKindFilterNode, data: RuntimeData) -> usize {
        let mut n = 0;
        node.process_streaming(data, None, &mut |_| {
            n += 1;
            Ok(())
        })
        .unwrap();
        n
    }

    #[test]
    fn an_audio_filter_drops_typed_frames() {
        let node = MediaKindFilterNode::new(&MediaKindFilterConfig { allow: vec!["audio".into()] }).unwrap();
        assert_eq!(passed(&node, audio()), 1);
        assert_eq!(passed(&node, RuntimeData::Json(json!({ "v": 1, "type": "text", "text": "hi" }))), 0);
        assert_eq!(passed(&node, RuntimeData::Text("hi".into())), 0);
    }

    #[test]
    fn a_text_filter_drops_audio() {
        let node =
            MediaKindFilterNode::new(&MediaKindFilterConfig { allow: vec!["json".into(), "Text".into()] }).unwrap();
        assert_eq!(passed(&node, audio()), 0);
        assert_eq!(passed(&node, RuntimeData::Json(json!({ "v": 1, "type": "interrupt" }))), 1);
        assert_eq!(passed(&node, RuntimeData::Text("hi".into())), 1);
    }

    #[test]
    fn an_unknown_kind_is_refused_by_name() {
        let error = MediaKindFilterNode::new(&MediaKindFilterConfig { allow: vec!["video".into()] })
            .err()
            .unwrap();
        assert!(error.contains("video"), "{error}");
    }

    #[test]
    fn with_nothing_allowed_everything_is_dropped() {
        let node = MediaKindFilterNode::new(&MediaKindFilterConfig::default()).unwrap();
        assert_eq!(passed(&node, audio()), 0);
    }
}
