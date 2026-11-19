//! `TestInputProbeNode`: says what reached it, for tests.
//!
//! For every input it emits one JSON value describing it: its kind, its
//! stream, and the facts of its payload (sample rate and length of audio,
//! size, format, codec and keyframe flag of video, length of text or binary),
//! together with how many inputs of that kind it has seen. The payload itself
//! is dropped.
//!
//! It exists so a test can check what a transport delivers *into* a pipeline
//! (a microphone track, a camera track, a data-channel message) the same way
//! it checks what comes out: by reading outputs. Nothing in a real pipeline
//! needs it.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::data::RuntimeData;
use crate::error::Error;
use crate::nodes::schema::{NodeSchema, RuntimeDataType};
use crate::nodes::{StreamingNode, StreamingNodeFactory, SyncNodeWrapper, SyncStreamingNode};
use serde_json::{json, Value};

/// The node type name in manifests.
pub const TEST_INPUT_PROBE_NODE: &str = "TestInputProbeNode";

/// See the module documentation.
#[derive(Default)]
pub struct TestInputProbeNode {
    /// Inputs seen so far, by kind.
    seen: Mutex<HashMap<String, u64>>,
}

impl TestInputProbeNode {
    /// The description of `data` as input number `count` of its kind.
    pub fn describe(data: &RuntimeData, count: u64) -> Value {
        let mut probe = json!({ "kind": data.data_type(), "count": count });
        if let Some(stream) = data.stream_id() {
            probe["stream_id"] = json!(stream);
        }
        match data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
                ..
            } => {
                probe["sample_rate"] = json!(sample_rate);
                probe["channels"] = json!(channels);
                probe["samples"] = json!(samples.len());
            }
            RuntimeData::Video {
                pixel_data,
                width,
                height,
                format,
                codec,
                is_keyframe,
                frame_number,
                ..
            } => {
                probe["width"] = json!(width);
                probe["height"] = json!(height);
                probe["format"] = json!(format!("{format:?}").to_lowercase());
                probe["codec"] = match codec {
                    Some(codec) => json!(format!("{codec:?}").to_lowercase()),
                    None => Value::Null,
                };
                probe["keyframe"] = json!(is_keyframe);
                probe["frame_number"] = json!(frame_number);
                probe["bytes"] = json!(pixel_data.len());
            }
            RuntimeData::Text(text) => probe["bytes"] = json!(text.len()),
            RuntimeData::Binary(bytes) => probe["bytes"] = json!(bytes.len()),
            _ => {}
        }
        json!({ "probe": probe })
    }
}

impl SyncStreamingNode for TestInputProbeNode {
    fn node_type(&self) -> &str {
        TEST_INPUT_PROBE_NODE
    }

    fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        let count = {
            let mut seen = self.seen.lock().expect("probe counts");
            let count = seen.entry(data.data_type().to_string()).or_insert(0);
            *count += 1;
            *count
        };
        Ok(RuntimeData::Json(Self::describe(&data, count)))
    }
}

/// Factory for [`TestInputProbeNode`].
pub struct TestInputProbeNodeFactory;

impl StreamingNodeFactory for TestInputProbeNodeFactory {
    fn create(
        &self,
        _node_id: String,
        _params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        Ok(Box::new(SyncNodeWrapper(TestInputProbeNode::default())))
    }

    fn node_type(&self) -> &str {
        TEST_INPUT_PROBE_NODE
    }

    fn schema(&self) -> Option<NodeSchema> {
        Some(
            NodeSchema::new(TEST_INPUT_PROBE_NODE)
                .description("Test probe: a JSON description of every input (kind, stream, size), without its payload")
                .category("utility")
                .accepts([
                    RuntimeDataType::Audio,
                    RuntimeDataType::Video,
                    RuntimeDataType::Text,
                    RuntimeDataType::Json,
                    RuntimeDataType::Binary,
                ])
                .produces([RuntimeDataType::Json]),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::video::{PixelFormat, VideoCodec};

    #[test]
    fn describes_each_input_and_counts_by_kind() {
        let node = TestInputProbeNode::default();
        let frame = |keyframe| RuntimeData::Video {
            pixel_data: vec![0u8; 10].into(),
            width: 320,
            height: 240,
            format: PixelFormat::Encoded,
            codec: Some(VideoCodec::H264),
            frame_number: 7,
            timestamp_us: 0,
            is_keyframe: keyframe,
            stream_id: Some("cam".into()),
            arrival_ts_us: None,
        };
        let RuntimeData::Json(first) = node.process(frame(true)).unwrap() else {
            panic!("json")
        };
        assert_eq!(
            first,
            json!({ "probe": {
                "kind": "video", "count": 1, "stream_id": "cam", "width": 320, "height": 240,
                "format": "encoded", "codec": "h264", "keyframe": true, "frame_number": 7,
                "bytes": 10
            }})
        );
        node.process(RuntimeData::Text("hi".into())).unwrap();
        let RuntimeData::Json(second) = node.process(frame(false)).unwrap() else {
            panic!("json")
        };
        assert_eq!(second["probe"]["count"], 2);
        assert_eq!(second["probe"]["keyframe"], false);
    }
}
