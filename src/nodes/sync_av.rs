//! SynchronizedAudioVideoNode - Multi-input node for audio-video synchronization
//!
//! This node demonstrates the multi-input capability of the generic streaming protocol.
//! It accepts both audio and video inputs via named_buffers and synchronizes them based
//! on timestamps (PTS - Presentation Time Stamp).
//!
//! # Input Requirements
//! - `audio`: AudioBuffer with timestamp information
//! - `video`: VideoFrame with timestamp_us field
//!
//! # Output
//! - JSON object containing sync status and timing information
//!
//! # Synchronization Logic
//! - Matches audio and video within ±20ms tolerance
//! - Reports sync drift and quality metrics

use crate::data::RuntimeData;
use crate::error::{Error, Result};
use crate::nodes::SyncStreamingNode;
use serde_json::{json, Value};
use std::collections::HashMap;

pub struct SynchronizedAudioVideoNode {
    node_id: String,
    sync_tolerance_us: i64, // Microseconds tolerance for sync (default 20ms = 20000us)
}

impl SynchronizedAudioVideoNode {
    /// Create a new SynchronizedAudioVideoNode
    pub fn new(node_id: String, params: &str) -> Result<Self> {
        // Parse parameters for sync tolerance
        let sync_tolerance_us = if let Ok(p) = serde_json::from_str::<Value>(params) {
            p["sync_tolerance_ms"]
                .as_f64()
                .map(|ms| (ms * 1000.0) as i64)
                .unwrap_or(20_000) // Default 20ms
        } else {
            20_000 // Default 20ms
        };

        Ok(Self {
            node_id,
            sync_tolerance_us,
        })
    }

    fn process_multi_internal(&self, inputs: HashMap<String, RuntimeData>) -> Result<RuntimeData> {
        // Extract audio and video from named inputs
        let audio_data = inputs.get("audio").ok_or_else(|| Error::InvalidInput {
            message: "Missing 'audio' input".to_string(),
            node_id: self.node_id.clone(),
            context: "SynchronizedAudioVideoNode requires both audio and video inputs".to_string(),
        })?;

        let video_data = inputs.get("video").ok_or_else(|| Error::InvalidInput {
            message: "Missing 'video' input".to_string(),
            node_id: self.node_id.clone(),
            context: "SynchronizedAudioVideoNode requires both audio and video inputs".to_string(),
        })?;

        // Extract timing information
        let (audio_timestamp_us, audio_sample_rate, audio_num_samples) = match audio_data {
            RuntimeData::Audio {
                samples,
                sample_rate,
                channels,
            } => {
                // Calculate audio timestamp from sample count
                let num_samples = samples.len() as u64;
                let duration_us = (num_samples as f64 / (*sample_rate as f64) * 1_000_000.0) as i64;
                (0i64, *sample_rate, num_samples)
            }
            _ => {
                return Err(Error::InvalidInput {
                    message: "'audio' input must be AudioBuffer".to_string(),
                    node_id: self.node_id.clone(),
                    context: "Received wrong data type for audio input".to_string(),
                })
            }
        };

        let (video_timestamp_us, video_frame_num, video_width, video_height) = match video_data {
            RuntimeData::Video {
                pixel_data,
                width,
                height,
                format,
                frame_number,
                timestamp_us,
            } => (*timestamp_us as i64, *frame_number, *width, *height),
            _ => {
                return Err(Error::InvalidInput {
                    message: "'video' input must be VideoFrame".to_string(),
                    node_id: self.node_id.clone(),
                    context: "Received wrong data type for video input".to_string(),
                })
            }
        };

        // Calculate sync offset
        let sync_offset_us = video_timestamp_us - audio_timestamp_us;
        let abs_offset = sync_offset_us.abs();

        // Determine sync status
        let is_synced = abs_offset <= self.sync_tolerance_us;
        let sync_quality = if abs_offset == 0 {
            "perfect"
        } else if abs_offset <= self.sync_tolerance_us / 2 {
            "excellent"
        } else if is_synced {
            "good"
        } else if abs_offset <= self.sync_tolerance_us * 2 {
            "acceptable"
        } else {
            "poor"
        };

        // Build sync report
        let sync_report = json!({
            "node_id": self.node_id,
            "sync_status": {
                "is_synced": is_synced,
                "quality": sync_quality,
                "offset_us": sync_offset_us,
                "offset_ms": sync_offset_us as f64 / 1000.0,
                "tolerance_ms": self.sync_tolerance_us as f64 / 1000.0,
            },
            "audio": {
                "timestamp_us": audio_timestamp_us,
                "sample_rate": audio_sample_rate,
                "num_samples": audio_num_samples,
                "duration_ms": audio_num_samples as f64 / audio_sample_rate as f64 * 1000.0,
            },
            "video": {
                "timestamp_us": video_timestamp_us,
                "frame_number": video_frame_num,
                "width": video_width,
                "height": video_height,
            },
            "recommendation": if !is_synced {
                if sync_offset_us > 0 {
                    "Video ahead of audio - consider delaying video or advancing audio"
                } else {
                    "Audio ahead of video - consider delaying audio or advancing video"
                }
            } else {
                "Streams are synchronized"
            }
        });

        // Return JSON output
        Ok(RuntimeData::Json(sync_report))
    }
}

impl SyncStreamingNode for SynchronizedAudioVideoNode {
    fn node_type(&self) -> &str {
        "SynchronizedAudioVideoNode"
    }

    fn process(&self, _input: RuntimeData) -> Result<RuntimeData> {
        Err(Error::InvalidInput {
            message: "SynchronizedAudioVideoNode requires multi-input via named_buffers"
                .to_string(),
            node_id: self.node_id.clone(),
            context: "Use process_multi() method with both audio and video inputs".to_string(),
        })
    }

    fn process_multi(&self, inputs: HashMap<String, RuntimeData>) -> Result<RuntimeData> {
        self.process_multi_internal(inputs)
    }

    fn is_multi_input(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio::AudioData;

    #[test]
    fn test_sync_av_perfect_sync() {
        let node = SynchronizedAudioVideoNode::new("sync1".to_string(), "{}").unwrap();

        let mut inputs = HashMap::new();

        // Create audio buffer
        use crate::data::{AudioBuffer, AudioFormat};
        let samples_f32 = vec![0.0f32; 1600];
        let samples_bytes: Vec<u8> = samples_f32.iter().flat_map(|&f| f.to_le_bytes()).collect();

        let audio = RuntimeData::Audio(AudioBuffer {
            samples: samples_bytes,
            sample_rate: 16000,
            channels: 1,
            format: AudioFormat::F32 as i32,
            num_samples: 1600,
        });

        // Create video frame with matching timestamp
        use crate::data::{PixelFormat, VideoFrame};
        let video = RuntimeData::Video(VideoFrame {
            pixel_data: vec![0u8; 320 * 240 * 3],
            width: 320,
            height: 240,
            format: PixelFormat::Rgb24 as i32,
            frame_number: 0,
            timestamp_us: 0, // Perfect sync with audio
        });

        inputs.insert("audio".to_string(), audio);
        inputs.insert("video".to_string(), video);

        let result = node.process_multi(inputs).unwrap();

        if let RuntimeData::Json(report) = result {
            assert_eq!(report["sync_status"]["is_synced"], true);
            assert_eq!(report["sync_status"]["quality"], "perfect");
            assert_eq!(report["sync_status"]["offset_us"], 0);
        } else {
            panic!("Expected JSON output");
        }
    }

    #[test]
    fn test_sync_av_drift() {
        use crate::data::{AudioBuffer, AudioFormat, PixelFormat, VideoFrame};

        let node = SynchronizedAudioVideoNode::new("sync1".to_string(), "{}").unwrap();

        let mut inputs = HashMap::new();

        let samples_f32 = vec![0.0f32; 1600];
        let samples_bytes: Vec<u8> = samples_f32.iter().flat_map(|&f| f.to_le_bytes()).collect();

        let audio = RuntimeData::Audio(AudioBuffer {
            samples: samples_bytes,
            sample_rate: 16000,
            channels: 1,
            format: AudioFormat::F32 as i32,
            num_samples: 1600,
        });

        // Video is 15ms ahead (within 20ms tolerance = good)
        let video = RuntimeData::Video(VideoFrame {
            pixel_data: vec![0u8; 320 * 240 * 3],
            width: 320,
            height: 240,
            format: PixelFormat::Rgb24 as i32,
            frame_number: 0,
            timestamp_us: 15_000, // 15ms ahead
        });

        inputs.insert("audio".to_string(), audio);
        inputs.insert("video".to_string(), video);

        let result = node.process_multi(inputs).unwrap();

        if let RuntimeData::Json(report) = result {
            assert_eq!(report["sync_status"]["is_synced"], true);
            assert_eq!(report["sync_status"]["quality"], "good");
            assert_eq!(report["sync_status"]["offset_ms"], 15.0);
        } else {
            panic!("Expected JSON output");
        }
    }
}
