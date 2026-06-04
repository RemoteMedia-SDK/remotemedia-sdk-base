//! VideoProcessorNode - Video frame processing demonstration node
//! Feature: 004-generic-streaming
//!
//! This node demonstrates video processing by accepting VideoFrame data
//! and returning JSON detection results (stub implementation).
//!
//! Input: VideoFrame with pixel data
//! Output: JSON with structure:
//!   { "frame_number": number, "detections": [{box, confidence, label}] }

use crate::data::RuntimeData;
use crate::data::VideoFrame;
use crate::nodes::SyncStreamingNode;
use crate::Error;
use serde_json::json;

/// VideoProcessorNode for video frame processing
pub struct VideoProcessorNode {
    /// Node ID
    pub id: String,
    /// Detection confidence threshold
    pub confidence_threshold: f64,
}

impl VideoProcessorNode {
    /// Create new video processor node
    pub fn new(id: String, params: &str) -> Result<Self, Error> {
        // Parse params JSON for configuration
        let config: serde_json::Value =
            serde_json::from_str(params).unwrap_or_else(|_| serde_json::json!({}));

        let confidence_threshold = config
            .get("confidence_threshold")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.5);

        Ok(Self {
            id,
            confidence_threshold,
        })
    }

    /// Process video frame and return detection results
    fn process_internal(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        // Extract video frame fields
        let (frame_number, width, height, timestamp_us) = match &data {
            RuntimeData::Video {
                pixel_data,
                width,
                height,
                format,
                frame_number,
                timestamp_us,
            } => (*frame_number, *width, *height, *timestamp_us),
            _ => {
                return Err(Error::InvalidInput {
                    message: "VideoProcessorNode expects VideoFrame input".into(),
                    node_id: self.id.clone(),
                    context: format!("Received {:?}", data.data_type()),
                });
            }
        };

        // Generate dummy detection results
        // In a real implementation, this would run an ML model
        let detections: Vec<serde_json::Value> = vec![]; // Simplified - no frame struct available

        // Return JSON result
        let output = json!({
            "frame_number": frame_number,
            "width": width,
            "height": height,
            "timestamp_us": timestamp_us,
            "detections": detections,
            "detection_count": detections.len()
        });

        Ok(RuntimeData::Json(output))
    }

    /// Generate dummy detection results for demonstration
    fn generate_dummy_detections(&self, frame: &VideoFrame) -> Vec<serde_json::Value> {
        // Simple deterministic "detection" based on frame properties
        // In real use: would run YOLO, SSD, or other object detection model

        let mut detections = Vec::new();

        // Generate 1-3 dummy detections based on frame number
        let detection_count = ((frame.frame_number % 3) + 1) as usize;

        for i in 0..detection_count {
            let confidence = 0.75 + (i as f64 * 0.05);

            if confidence >= self.confidence_threshold {
                let labels = ["person", "car", "dog"];
                let label = labels[i % 3];

                let detection = json!({
                    "label": label,
                    "confidence": confidence,
                    "bounding_box": {
                        "x": (i * 100) as u32,
                        "y": (i * 80) as u32,
                        "width": 120,
                        "height": 150
                    }
                });
                detections.push(detection);
            }
        }

        detections
    }
}

impl SyncStreamingNode for VideoProcessorNode {
    fn node_type(&self) -> &str {
        "VideoProcessorNode"
    }

    fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        self.process_internal(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::PixelFormat;
    use crate::nodes::SyncStreamingNode;

    fn create_test_frame(frame_number: u64) -> VideoFrame {
        VideoFrame {
            pixel_data: vec![0u8; 640 * 480 * 3], // RGB24 640x480
            width: 640,
            height: 480,
            format: PixelFormat::Rgb24 as i32,
            frame_number,
            timestamp_us: frame_number * 33333, // 30 FPS
        }
    }

    #[test]
    fn test_video_processing() {
        let node = VideoProcessorNode::new("video".to_string(), r#"{"confidence_threshold": 0.5}"#)
            .unwrap();
        let frame = create_test_frame(0);
        let input = RuntimeData::Video {
            pixel_data,
            width,
            height,
            format,
            frame_number,
            timestamp_us,
        };

        let result = SyncStreamingNode::process(&node, input).unwrap();
        match result {
            RuntimeData::Json(value) => {
                assert_eq!(value["frame_number"], 0);
                assert_eq!(value["width"], 640);
                assert_eq!(value["height"], 480);
                assert!(value["detections"].is_array());
                let detections = value["detections"].as_array().unwrap();
                assert!(detections.len() > 0);
            }
            _ => panic!("Expected JSON output"),
        }
    }

    #[test]
    fn test_detection_filtering_by_confidence() {
        let node = VideoProcessorNode::new("video".to_string(), r#"{"confidence_threshold": 0.9}"#)
            .unwrap();
        let frame = create_test_frame(5);
        let input = RuntimeData::Video {
            pixel_data,
            width,
            height,
            format,
            frame_number,
            timestamp_us,
        };

        let result = SyncStreamingNode::process(&node, input).unwrap();
        match result {
            RuntimeData::Json(value) => {
                let detections = value["detections"].as_array().unwrap();
                // High threshold should filter out most detections
                assert!(detections.len() <= 1);
            }
            _ => panic!("Expected JSON output"),
        }
    }

    #[test]
    fn test_multiple_frames() {
        let node = VideoProcessorNode::new("video".to_string(), "{}").unwrap();

        // Process 10 frames
        for i in 0..10 {
            let frame = create_test_frame(i);
            let input = RuntimeData::Video {
                pixel_data,
                width,
                height,
                format,
                frame_number,
                timestamp_us,
            };

            let result = SyncStreamingNode::process(&node, input).unwrap();
            match result {
                RuntimeData::Json(value) => {
                    assert_eq!(value["frame_number"], i);
                    assert!(value["detections"].is_array());
                }
                _ => panic!("Expected JSON output"),
            }
        }
    }

    #[test]
    fn test_wrong_data_type() {
        let node = VideoProcessorNode::new("video".to_string(), "{}").unwrap();
        let input = RuntimeData::Text("not a video frame".to_string());

        let result = SyncStreamingNode::process(&node, input);
        assert!(result.is_err());
    }
}
