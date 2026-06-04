//! Video flip node for testing WebRTC video processing
//!
//! Flips video frames vertically (upside down) or horizontally.

use crate::data::RuntimeData;
use crate::nodes::streaming_node::SyncStreamingNode;
use crate::Error;
use serde::{Deserialize, Serialize};

/// Flip direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum FlipDirection {
    /// Flip vertically (upside down)
    Vertical,
    /// Flip horizontally (mirror)
    Horizontal,
    /// Flip both vertically and horizontally (180° rotation)
    Both,
}

impl Default for FlipDirection {
    fn default() -> Self {
        Self::Vertical
    }
}

/// Video flip node configuration
///
/// Configuration for the video flip streaming node. Uses `#[serde(default)]` to allow
/// partial config.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default)]
pub struct VideoFlipConfig {
    /// Direction to flip (vertical, horizontal, or both)
    pub direction: FlipDirection,
}

impl Default for VideoFlipConfig {
    fn default() -> Self {
        Self {
            direction: FlipDirection::Vertical,
        }
    }
}

/// Video flip node
///
/// Flips video frames for testing WebRTC video processing.
/// Supports RGB24 and I420 (YUV420P) formats.
pub struct VideoFlipNode {
    config: VideoFlipConfig,
}

impl VideoFlipNode {
    /// Create a new video flip node
    pub fn new(config: VideoFlipConfig) -> Self {
        Self { config }
    }

    /// Flip RGB24 image
    fn flip_rgb24(&self, data: &[u8], width: u32, height: u32) -> Result<Vec<u8>, Error> {
        let width = width as usize;
        let height = height as usize;
        let bytes_per_pixel = 3; // RGB24
        let row_bytes = width * bytes_per_pixel;

        if data.len() != height * row_bytes {
            return Err(Error::InvalidData(format!(
                "RGB24 data size mismatch: expected {}, got {}",
                height * row_bytes,
                data.len()
            )));
        }

        let mut flipped = vec![0u8; data.len()];

        match self.config.direction {
            FlipDirection::Vertical => {
                // Flip vertically: reverse row order
                for y in 0..height {
                    let src_row = &data[y * row_bytes..(y + 1) * row_bytes];
                    let dst_row =
                        &mut flipped[(height - 1 - y) * row_bytes..(height - y) * row_bytes];
                    dst_row.copy_from_slice(src_row);
                }
            }
            FlipDirection::Horizontal => {
                // Flip horizontally: reverse pixel order in each row
                for y in 0..height {
                    for x in 0..width {
                        let src_offset = (y * width + x) * bytes_per_pixel;
                        let dst_offset = (y * width + (width - 1 - x)) * bytes_per_pixel;
                        flipped[dst_offset..dst_offset + bytes_per_pixel]
                            .copy_from_slice(&data[src_offset..src_offset + bytes_per_pixel]);
                    }
                }
            }
            FlipDirection::Both => {
                // Flip both: reverse everything
                for y in 0..height {
                    for x in 0..width {
                        let src_offset = (y * width + x) * bytes_per_pixel;
                        let dst_offset =
                            ((height - 1 - y) * width + (width - 1 - x)) * bytes_per_pixel;
                        flipped[dst_offset..dst_offset + bytes_per_pixel]
                            .copy_from_slice(&data[src_offset..src_offset + bytes_per_pixel]);
                    }
                }
            }
        }

        Ok(flipped)
    }

    /// Flip I420 (YUV420P) image
    fn flip_i420(&self, data: &[u8], width: u32, height: u32) -> Result<Vec<u8>, Error> {
        let width = width as usize;
        let height = height as usize;

        // I420 format: Y plane (width x height), U plane (width/2 x height/2), V plane (width/2 x height/2)
        let y_size = width * height;
        let uv_size = (width / 2) * (height / 2);
        let expected_size = y_size + 2 * uv_size;

        if data.len() != expected_size {
            return Err(Error::InvalidData(format!(
                "I420 data size mismatch: expected {}, got {}",
                expected_size,
                data.len()
            )));
        }

        let mut flipped = vec![0u8; data.len()];

        // Split into Y, U, V planes
        let (y_plane, uv_planes) = data.split_at(y_size);
        let (u_plane, v_plane) = uv_planes.split_at(uv_size);

        let (y_dst, uv_dst) = flipped.split_at_mut(y_size);
        let (u_dst, v_dst) = uv_dst.split_at_mut(uv_size);

        match self.config.direction {
            FlipDirection::Vertical => {
                // Flip Y plane vertically
                for y in 0..height {
                    let src_row = &y_plane[y * width..(y + 1) * width];
                    let dst_row = &mut y_dst[(height - 1 - y) * width..(height - y) * width];
                    dst_row.copy_from_slice(src_row);
                }

                // Flip U and V planes vertically (half resolution)
                let uv_height = height / 2;
                let uv_width = width / 2;
                for y in 0..uv_height {
                    // U plane
                    let src_row = &u_plane[y * uv_width..(y + 1) * uv_width];
                    let dst_row =
                        &mut u_dst[(uv_height - 1 - y) * uv_width..(uv_height - y) * uv_width];
                    dst_row.copy_from_slice(src_row);

                    // V plane
                    let src_row = &v_plane[y * uv_width..(y + 1) * uv_width];
                    let dst_row =
                        &mut v_dst[(uv_height - 1 - y) * uv_width..(uv_height - y) * uv_width];
                    dst_row.copy_from_slice(src_row);
                }
            }
            FlipDirection::Horizontal => {
                // Flip Y plane horizontally
                for y in 0..height {
                    for x in 0..width {
                        let src_idx = y * width + x;
                        let dst_idx = y * width + (width - 1 - x);
                        y_dst[dst_idx] = y_plane[src_idx];
                    }
                }

                // Flip U and V planes horizontally (half resolution)
                let uv_height = height / 2;
                let uv_width = width / 2;
                for y in 0..uv_height {
                    for x in 0..uv_width {
                        let src_idx = y * uv_width + x;
                        let dst_idx = y * uv_width + (uv_width - 1 - x);
                        u_dst[dst_idx] = u_plane[src_idx];
                        v_dst[dst_idx] = v_plane[src_idx];
                    }
                }
            }
            FlipDirection::Both => {
                // Flip Y plane both ways
                for y in 0..height {
                    for x in 0..width {
                        let src_idx = y * width + x;
                        let dst_idx = (height - 1 - y) * width + (width - 1 - x);
                        y_dst[dst_idx] = y_plane[src_idx];
                    }
                }

                // Flip U and V planes both ways (half resolution)
                let uv_height = height / 2;
                let uv_width = width / 2;
                for y in 0..uv_height {
                    for x in 0..uv_width {
                        let src_idx = y * uv_width + x;
                        let dst_idx = (uv_height - 1 - y) * uv_width + (uv_width - 1 - x);
                        u_dst[dst_idx] = u_plane[src_idx];
                        v_dst[dst_idx] = v_plane[src_idx];
                    }
                }
            }
        }

        Ok(flipped)
    }
}

// Phase A-Wave 1: `SyncStreamingNode` — body is pure pixel flipping,
// no `.await`. Factory registration switches from `AsyncNodeWrapper`
// to `SyncNodeWrapper` in `streaming_registry.rs`.
impl SyncStreamingNode for VideoFlipNode {
    fn node_type(&self) -> &str {
        "VideoFlip"
    }

    fn process(&self, data: RuntimeData) -> Result<RuntimeData, Error> {
        match data {
            RuntimeData::Video {
                pixel_data,
                width,
                height,
                format,
                codec,
                frame_number,
                timestamp_us,
                is_keyframe,
                ..
            } => {
                use crate::data::video::PixelFormat;

                // Flip based on format
                let flipped_data = match format {
                    PixelFormat::Rgb24 => self.flip_rgb24(&pixel_data, width, height)?,
                    PixelFormat::Yuv420p | PixelFormat::I420 => {
                        self.flip_i420(&pixel_data, width, height)?
                    }
                    _ => {
                        return Err(Error::Execution(format!(
                            "VideoFlip only supports RGB24 and I420/YUV420P, got format={:?}",
                            format
                        )));
                    }
                };

                Ok(RuntimeData::Video {
                    pixel_data: flipped_data,
                    width,
                    height,
                    format,
                    codec,
                    frame_number,
                    timestamp_us,
                    is_keyframe,
                    stream_id: None,
                    arrival_ts_us: None,
                })
            }
            _ => Err(Error::Execution(
                "VideoFlip expects Video input".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::video::PixelFormat;

    #[tokio::test]
    async fn test_flip_rgb24_vertical() {
        let config = VideoFlipConfig {
            direction: FlipDirection::Vertical,
        };
        let node = VideoFlipNode::new(config);

        // Create a simple 2x2 RGB24 image
        // Top row: red, green
        // Bottom row: blue, white
        let input = vec![
            255, 0, 0, // red
            0, 255, 0, // green
            0, 0, 255, // blue
            255, 255, 255, // white
        ];

        let input_data = RuntimeData::Video {
            pixel_data: input,
            width: 2,
            height: 2,
            format: PixelFormat::Rgb24,
            codec: None,
            frame_number: 0,
            timestamp_us: 0,
            is_keyframe: false,
            stream_id: None,
            arrival_ts_us: None,
        };

        let output = node.process(input_data).unwrap();

        if let RuntimeData::Video { pixel_data, .. } = output {
            // After vertical flip:
            // Top row: blue, white
            // Bottom row: red, green
            assert_eq!(pixel_data[0..3], [0, 0, 255]); // blue
            assert_eq!(pixel_data[3..6], [255, 255, 255]); // white
            assert_eq!(pixel_data[6..9], [255, 0, 0]); // red
            assert_eq!(pixel_data[9..12], [0, 255, 0]); // green
        } else {
            panic!("Expected Video output");
        }
    }

    #[tokio::test]
    async fn test_flip_rgb24_horizontal() {
        let config = VideoFlipConfig {
            direction: FlipDirection::Horizontal,
        };
        let node = VideoFlipNode::new(config);

        // Create a simple 2x2 RGB24 image
        let input = vec![
            255, 0, 0, // red
            0, 255, 0, // green
            0, 0, 255, // blue
            255, 255, 255, // white
        ];

        let input_data = RuntimeData::Video {
            pixel_data: input,
            width: 2,
            height: 2,
            format: PixelFormat::Rgb24,
            codec: None,
            frame_number: 0,
            timestamp_us: 0,
            is_keyframe: false,
            stream_id: None,
            arrival_ts_us: None,
        };

        let output = node.process(input_data).unwrap();

        if let RuntimeData::Video { pixel_data, .. } = output {
            // After horizontal flip:
            // Top row: green, red
            // Bottom row: white, blue
            assert_eq!(pixel_data[0..3], [0, 255, 0]); // green
            assert_eq!(pixel_data[3..6], [255, 0, 0]); // red
            assert_eq!(pixel_data[6..9], [255, 255, 255]); // white
            assert_eq!(pixel_data[9..12], [0, 0, 255]); // blue
        }
    }

    #[tokio::test]
    async fn test_flip_i420_vertical() {
        let config = VideoFlipConfig {
            direction: FlipDirection::Vertical,
        };
        let node = VideoFlipNode::new(config);

        // Create a simple 4x4 I420 image (minimum size for I420)
        // Y plane: 4x4 = 16 bytes
        // U plane: 2x2 = 4 bytes
        // V plane: 2x2 = 4 bytes
        let mut input = vec![0u8; 24];

        // Set Y plane with gradient
        for i in 0..16 {
            input[i] = i as u8;
        }

        // U and V planes
        input[16..20].copy_from_slice(&[100, 101, 102, 103]);
        input[20..24].copy_from_slice(&[200, 201, 202, 203]);

        let input_data = RuntimeData::Video {
            pixel_data: input,
            width: 4,
            height: 4,
            format: PixelFormat::I420,
            codec: None,
            frame_number: 0,
            timestamp_us: 0,
            is_keyframe: false,
            stream_id: None,
            arrival_ts_us: None,
        };

        let output = node.process(input_data).unwrap();

        if let RuntimeData::Video {
            pixel_data,
            width,
            height,
            format,
            ..
        } = output
        {
            assert_eq!(width, 4);
            assert_eq!(height, 4);
            assert_eq!(format, PixelFormat::I420);
            assert_eq!(pixel_data.len(), 24);

            // Y plane should be flipped vertically
            assert_eq!(pixel_data[0..4], [12, 13, 14, 15]); // Last row becomes first
            assert_eq!(pixel_data[12..16], [0, 1, 2, 3]); // First row becomes last
        }
    }
}
