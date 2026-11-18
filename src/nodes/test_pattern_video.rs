//! `TestPatternVideoNode`: a deterministic stand-in for a node that sends
//! video (an avatar, a shared screen), for tests and examples.
//!
//! It is not a renderer. Whatever input arrives (the member's microphone
//! audio, typically), it emits a raw YUV 4:2:0 frame of a moving bar, at
//! most `framerate` times a second, under its `video_stream_id`. It declares
//! its video output, so the WebRTC transport plans one outbound track for it
//! and announces it in the media plan (openspec:
//! extend-ai-sdk-media-streaming). A pipeline with it can be exercised end to
//! end (a browser receiving the pipeline's video) on a machine with no
//! camera, no screen and no model.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::capabilities::{
    ConstraintValue, MediaCapabilities, MediaConstraints, PixelFormat as CapsPixelFormat,
    VideoConstraints,
};
use crate::data::PixelFormat;
use crate::data::RuntimeData;
use crate::error::Error;
use crate::nodes::schema::{NodeSchema, RuntimeDataType};
use crate::nodes::{StreamingNode, StreamingNodeFactory, SyncNodeWrapper, SyncStreamingNode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// The node type name in manifests.
pub const TEST_PATTERN_VIDEO_NODE: &str = "TestPatternVideoNode";

/// `TestPatternVideoNode` parameters. None is required.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct TestPatternVideoConfig {
    /// The stream id of the video output (the WebRTC track it is sent on).
    pub video_stream_id: String,
    /// Frame width in pixels (even).
    #[schemars(range(min = 16, max = 1920))]
    pub width: u32,
    /// Frame height in pixels (even).
    #[schemars(range(min = 16, max = 1080))]
    pub height: u32,
    /// Most frames per second.
    #[schemars(range(min = 1, max = 60))]
    pub framerate: u32,
}

impl Default for TestPatternVideoConfig {
    fn default() -> Self {
        Self {
            video_stream_id: "default".into(),
            width: 160,
            height: 120,
            framerate: 15,
        }
    }
}

struct State {
    started: Instant,
    last: Option<Instant>,
    frames: u64,
}

/// See the module documentation.
pub struct TestPatternVideoNode {
    config: TestPatternVideoConfig,
    state: Mutex<State>,
}

impl TestPatternVideoNode {
    pub fn new(mut config: TestPatternVideoConfig) -> Self {
        config.width = (config.width.max(16) + 1) & !1;
        config.height = (config.height.max(16) + 1) & !1;
        config.framerate = config.framerate.clamp(1, 60);
        Self {
            config,
            state: Mutex::new(State {
                started: Instant::now(),
                last: None,
                frames: 0,
            }),
        }
    }

    /// Frame `n`: grey background, a white bar that moves across, neutral chroma.
    pub fn frame(&self, n: u64, timestamp_us: u64) -> RuntimeData {
        let (w, h) = (self.config.width as usize, self.config.height as usize);
        let mut pixels = vec![64u8; w * h];
        let bar = (n as usize * 4) % w;
        for row in 0..h {
            for x in bar..(bar + w / 8).min(w) {
                pixels[row * w + x] = 235;
            }
        }
        pixels.resize(w * h + 2 * (w / 2) * (h / 2), 128);
        RuntimeData::Video {
            pixel_data: pixels.into(),
            width: w as u32,
            height: h as u32,
            format: PixelFormat::Yuv420p,
            codec: None,
            frame_number: n,
            timestamp_us,
            is_keyframe: false,
            stream_id: Some(self.config.video_stream_id.clone()),
            arrival_ts_us: None,
        }
    }

    /// Output capabilities: raw video of the configured size and rate.
    pub fn caps(config: &TestPatternVideoConfig) -> MediaCapabilities {
        MediaCapabilities::with_output(MediaConstraints::Video(VideoConstraints {
            width: Some(ConstraintValue::Exact(config.width)),
            height: Some(ConstraintValue::Exact(config.height)),
            framerate: Some(ConstraintValue::Exact(config.framerate as f32)),
            pixel_format: Some(ConstraintValue::Exact(CapsPixelFormat::YUV420)),
        }))
    }
}

impl SyncStreamingNode for TestPatternVideoNode {
    fn node_type(&self) -> &str {
        TEST_PATTERN_VIDEO_NODE
    }

    fn process(&self, _data: RuntimeData) -> Result<RuntimeData, Error> {
        let mut state = self.state.lock().expect("pattern state");
        let n = state.frames;
        state.frames += 1;
        let at = state.started.elapsed().as_micros() as u64;
        Ok(self.frame(n, at))
    }

    fn process_streaming(
        &self,
        _data: RuntimeData,
        _session_id: Option<&str>,
        callback: &mut dyn FnMut(RuntimeData) -> Result<(), Error>,
    ) -> Result<usize, Error> {
        let frame = {
            let mut state = self.state.lock().expect("pattern state");
            let now = Instant::now();
            let interval = Duration::from_micros(1_000_000 / u64::from(self.config.framerate));
            if state
                .last
                .is_some_and(|last| now.duration_since(last) < interval)
            {
                return Ok(0);
            }
            state.last = Some(now);
            let n = state.frames;
            state.frames += 1;
            self.frame(n, now.duration_since(state.started).as_micros() as u64)
        };
        callback(frame)?;
        Ok(1)
    }
}

/// Factory for [`TestPatternVideoNode`].
pub struct TestPatternVideoNodeFactory;

fn config_of(node_id: &str, params: &Value) -> Result<TestPatternVideoConfig, Error> {
    let params = if params.is_null() {
        json!({})
    } else {
        params.clone()
    };
    serde_json::from_value(params).map_err(|e| {
        Error::Execution(format!(
            "{TEST_PATTERN_VIDEO_NODE} '{node_id}': invalid params: {e}"
        ))
    })
}

impl StreamingNodeFactory for TestPatternVideoNodeFactory {
    fn create(
        &self,
        node_id: String,
        params: &Value,
        _session_id: Option<String>,
    ) -> Result<Box<dyn StreamingNode>, Error> {
        let config = config_of(&node_id, params)?;
        Ok(Box::new(SyncNodeWrapper(TestPatternVideoNode::new(config))))
    }

    fn node_type(&self) -> &str {
        TEST_PATTERN_VIDEO_NODE
    }

    fn is_multi_output_streaming(&self) -> bool {
        true
    }

    fn media_capabilities(&self, params: &Value) -> Option<MediaCapabilities> {
        config_of("", params)
            .ok()
            .map(|config| TestPatternVideoNode::caps(&config))
    }

    fn schema(&self) -> Option<NodeSchema> {
        Some(
            NodeSchema::new(TEST_PATTERN_VIDEO_NODE)
                .description("Test stand-in for a video source: a moving bar per input, at most `framerate` per second")
                .category("video")
                .accepts([RuntimeDataType::Audio, RuntimeDataType::Text, RuntimeDataType::Json])
                .produces([RuntimeDataType::Video])
                .config_schema_from::<TestPatternVideoConfig>(),
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

    #[test]
    fn emits_a_frame_under_its_stream_id_at_most_at_its_rate() {
        let node = TestPatternVideoNode::new(TestPatternVideoConfig {
            video_stream_id: "avatar".into(),
            width: 64,
            height: 48,
            framerate: 10,
        });
        let mut frames = Vec::new();
        for _ in 0..5 {
            node.process_streaming(audio(), None, &mut |f| {
                frames.push(f);
                Ok(())
            })
            .unwrap();
        }
        // Five inputs in a burst: one frame, the rest are within the interval.
        assert_eq!(frames.len(), 1);
        let RuntimeData::Video {
            width,
            height,
            format,
            stream_id,
            pixel_data,
            ..
        } = &frames[0]
        else {
            panic!("expected video");
        };
        assert_eq!((*width, *height, *format), (64, 48, PixelFormat::Yuv420p));
        assert_eq!(stream_id.as_deref(), Some("avatar"));
        assert_eq!(pixel_data.len(), 64 * 48 * 3 / 2);
        std::thread::sleep(Duration::from_millis(110));
        node.process_streaming(audio(), None, &mut |f| {
            frames.push(f);
            Ok(())
        })
        .unwrap();
        assert_eq!(frames.len(), 2);
    }

    #[test]
    fn declares_its_video_output_for_the_media_plan() {
        let caps = TestPatternVideoNodeFactory
            .media_capabilities(&json!({ "video_stream_id": "avatar", "width": 320, "height": 240, "framerate": 25 }))
            .unwrap();
        let video = caps
            .outputs
            .values()
            .find_map(|c| match c {
                MediaConstraints::Video(v) => Some(v.clone()),
                _ => None,
            })
            .unwrap();
        assert_eq!(video.width, Some(ConstraintValue::Exact(320)));
        assert_eq!(video.framerate, Some(ConstraintValue::Exact(25.0)));
    }
}
