//! Video codec backend abstractions
//!
//! Provides traits and implementations for video encoding/decoding backends.
//! Primary backend: ac-ffmpeg (FFmpeg bindings)
//! Optional: rav1e (pure Rust AV1 encoder)
//!
//! ## ac-ffmpeg Integration Status
//!
//! The codec implementations below provide functional mock encoding/decoding
//! that enables end-to-end testing of the video pipeline architecture.
//!
//! To enable actual FFmpeg encoding/decoding, implement the TODOs in:
//! - FFmpegEncoder::encode() - Use ac-ffmpeg VideoEncoder
//! - FFmpegDecoder::decode() - Use ac-ffmpeg VideoDecoder
//!
//! FFmpeg 7.1.1 is installed with libvpx (VP8), libx264 (H.264), and libaom/librav1e (AV1).
//!
//! Key ac-ffmpeg API elements (from source inspection):
//! - VideoDecoder::new(codec_name) or VideoDecoder::builder(codec_name).build()
//! - VideoEncoder::builder(codec_name).pixel_format(fmt).width(w).height(h).time_base(tb).bit_rate(br).build()
//! - PixelFormat: parse from strings "yuv420p", "rgb24", "nv12", etc.
//! - VideoFrameMut::black(format, width, height) - Create frame, use planes_mut()[i].data_mut() to write pixels
//! - decoder.push(packet), decoder.take() -> Option<VideoFrame>
//! - encoder.push(frame), encoder.take() -> Option<Packet>
//! - Packet data access: packet is from demuxer or needs PacketMut construction
//!
//! References:
//! - Source: ~/.cargo/registry/src/.../ac-ffmpeg-0.19.0/src/codec/video/
//! - Docs: https://docs.rs/ac-ffmpeg/0.19.0
//! - Examples: https://github.com/angelcam/rust-ac-ffmpeg/tree/master/examples

use super::decoder::VideoDecoderConfig;
use super::encoder::VideoEncoderConfig;
use crate::data::video::{PixelFormat, VideoCodec};
use crate::data::RuntimeData;

/// Result type for codec operations
pub type Result<T> = std::result::Result<T, CodecError>;

/// Codec-specific errors
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("Codec not available: {0}")]
    NotAvailable(String),

    #[error("Encoding failed: {0}")]
    EncodingFailed(String),

    #[error("Decoding failed: {0}")]
    DecodingFailed(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),
}

/// Video encoder backend trait
///
/// Implementations provide codec-specific encoding (FFmpeg, rav1e, etc.)
pub trait VideoEncoderBackend: Send + Sync {
    /// Encode a raw video frame to compressed bitstream
    ///
    /// # Arguments
    /// * `input` - RuntimeData::Video with codec=None (raw frame)
    ///
    /// # Returns
    /// * `Ok(RuntimeData)` - Encoded frame with codec=Some(...)
    /// * `Err(CodecError)` - Encoding failure
    fn encode(&mut self, input: RuntimeData) -> Result<RuntimeData>;

    /// Get the codec this encoder produces
    fn codec(&self) -> VideoCodec;

    /// Reconfigure encoder (bitrate, quality, etc.)
    fn reconfigure(&mut self, config: &VideoEncoderConfig) -> Result<()>;
}

/// Video decoder backend trait
///
/// Implementations provide codec-specific decoding (FFmpeg, dav1d, etc.)
pub trait VideoDecoderBackend: Send + Sync {
    /// Decode a compressed bitstream to raw video frame
    ///
    /// # Arguments
    /// * `input` - RuntimeData::Video with codec=Some(...) (encoded frame)
    ///
    /// # Returns
    /// * `Ok(RuntimeData)` - Decoded raw frame with codec=None
    /// * `Err(CodecError)` - Decoding failure
    fn decode(&mut self, input: RuntimeData) -> Result<RuntimeData>;

    /// Get the codec this decoder handles
    fn codec(&self) -> VideoCodec;

    /// Get the output pixel format
    fn output_format(&self) -> PixelFormat;
}

/// FFmpeg encoder implementation
///
/// Currently returns mock encoded frames. To enable actual encoding:
/// 1. Use VideoEncoder::builder() from ac-ffmpeg
/// 2. Create VideoFrameMut from pixel_data
/// 3. Push frame, take packets
/// 4. Return bitstream as encoded RuntimeData
#[cfg(feature = "video")]
pub struct FFmpegEncoder {
    config: VideoEncoderConfig,
    frame_count: u64,
    encoder: Option<ac_ffmpeg::codec::video::VideoEncoder>,
}

#[cfg(feature = "video")]
impl FFmpegEncoder {
    pub fn new(config: VideoEncoderConfig) -> Result<Self> {
        Ok(Self {
            config,
            frame_count: 0,
            encoder: None, // Lazy initialization on first frame
        })
    }
}

#[cfg(feature = "video")]
impl VideoEncoderBackend for FFmpegEncoder {
    fn encode(&mut self, input: RuntimeData) -> Result<RuntimeData> {
        // Extract raw video frame
        let (pixel_data, width, height, format, frame_number, timestamp_us) = match input {
            RuntimeData::Video {
                pixel_data,
                width,
                height,
                format,
                codec: None,
                frame_number,
                timestamp_us,
                stream_id: _,
                ..
            } => (
                pixel_data,
                width,
                height,
                format,
                frame_number,
                timestamp_us,
            ),
            RuntimeData::Video { codec: Some(_), .. } => {
                return Err(CodecError::InvalidInput(
                    "Frame is already encoded".to_string(),
                ));
            }
            _ => {
                return Err(CodecError::InvalidInput("Expected video frame".to_string()));
            }
        };

        if format == PixelFormat::Encoded {
            return Err(CodecError::InvalidInput(
                "Cannot encode already-encoded frame".to_string(),
            ));
        }

        // Encoder requires even dimensions for YUV 4:2:0 chroma sub-sampling.
        if width % 2 != 0 || height % 2 != 0 {
            return Err(CodecError::InvalidInput(format!(
                "VideoEncoder requires even dimensions for YUV 4:2:0; got {}x{}",
                width, height
            )));
        }

        // Convert input to YUV420p planar (Y || U || V) once, regardless of
        // the source pixel format. Earlier revisions of this method assumed
        // pixel_data was already YUV420p and silently memcpy'd whatever bytes
        // came in into the Y/U/V planes — feeding RGB into the encoder
        // produced a green-tinted, horizontally banded output over WebRTC
        // (the Y4M sink worked because it converts RGB→YUV itself).
        let y_size = (width * height) as usize;
        let uv_size = ((width / 2) * (height / 2)) as usize;
        let yuv_buffer: Vec<u8> = match format {
            PixelFormat::Yuv420p | PixelFormat::I420 => {
                let expected = y_size + 2 * uv_size;
                if pixel_data.len() != expected {
                    return Err(CodecError::InvalidInput(format!(
                        "YUV420p frame buffer size mismatch: got {} bytes, expected {} for {}x{}",
                        pixel_data.len(),
                        expected,
                        width,
                        height
                    )));
                }
                pixel_data
            }
            PixelFormat::Rgb24 => {
                let expected = (width * height * 3) as usize;
                if pixel_data.len() != expected {
                    return Err(CodecError::InvalidInput(format!(
                        "RGB24 frame buffer size mismatch: got {} bytes, expected {} for {}x{}",
                        pixel_data.len(),
                        expected,
                        width,
                        height
                    )));
                }
                crate::data::video::rgb24_to_yuv420p(&pixel_data, width as usize, height as usize)
            }
            PixelFormat::Rgba32 => {
                let expected = (width * height * 4) as usize;
                if pixel_data.len() != expected {
                    return Err(CodecError::InvalidInput(format!(
                        "RGBA32 frame buffer size mismatch: got {} bytes, expected {} for {}x{}",
                        pixel_data.len(),
                        expected,
                        width,
                        height
                    )));
                }
                crate::data::video::rgba32_to_yuv420p(&pixel_data, width as usize, height as usize)
            }
            PixelFormat::NV12 => {
                return Err(CodecError::InvalidInput(
                    "NV12 input not yet supported by VideoEncoder; convert to Yuv420p upstream"
                        .to_string(),
                ));
            }
            PixelFormat::Encoded | PixelFormat::Unspecified => {
                return Err(CodecError::InvalidInput(format!(
                    "Unsupported pixel format for encoding: {:?}",
                    format
                )));
            }
        };

        // Lazy initialize encoder on first frame
        if self.encoder.is_none() {
            use ac_ffmpeg::codec::video::{frame::get_pixel_format, VideoEncoder};
            use ac_ffmpeg::time::TimeBase;

            let pixel_format = get_pixel_format("yuv420p");
            let time_base = TimeBase::new(1, self.config.framerate as i32);

            // Build the candidate codec list. When `hardware_accel` is set
            // we try the NVENC variant first and fall back to the software
            // encoder if init fails (driver missing, GPU busy, ffmpeg
            // compiled without nvenc, etc). Without `hardware_accel` we go
            // straight to the software encoder.
            //
            // VP8 has no NVENC equivalent; the list collapses to libvpx.
            let candidates: &[&str] = match (self.config.codec, self.config.hardware_accel) {
                (VideoCodec::H264, true) => &["h264_nvenc", "libx264"],
                (VideoCodec::H264, false) => &["libx264"],
                (VideoCodec::Av1, true) => &["av1_nvenc", "libaom-av1"],
                (VideoCodec::Av1, false) => &["libaom-av1"],
                (VideoCodec::Vp8, _) => &["libvpx"],
            };

            let bitrate = self.config.bitrate as u64;
            let w = width as usize;
            let h = height as usize;
            let preset = self.config.quality_preset.clone();

            let mut last_err: Option<String> = None;
            let mut built: Option<(&'static str, VideoEncoder)> = None;
            for &codec_name in candidates {
                let mut b = match VideoEncoder::builder(codec_name) {
                    Ok(b) => b,
                    Err(e) => {
                        last_err = Some(format!("builder({}): {}", codec_name, e));
                        continue;
                    }
                };
                b = b
                    .pixel_format(pixel_format)
                    .width(w)
                    .height(h)
                    .time_base(time_base)
                    .bit_rate(bitrate);

                // Codec-specific low-latency tuning. set_option is
                // best-effort: unknown opts are silently ignored by
                // ffmpeg, so this stays safe across codec boundaries.
                b = match codec_name {
                    // NVIDIA NVENC: P1 is the fastest preset on the new
                    // P1..P7 ladder; `tune=ull` is the ultra-low-latency
                    // tune that disables B-frames + reordering. CBR with
                    // zerolatency keeps the encoder from buffering frames
                    // for rate-control look-ahead — crucial for WebRTC.
                    "h264_nvenc" | "av1_nvenc" => b
                        .set_option("preset", "p1")
                        .set_option("tune", "ull")
                        .set_option("rc", "cbr")
                        .set_option("zerolatency", "1")
                        .set_option("delay", "2")
                        .set_option("gpu", "1"),
                    // libx264 fallback: explicit ultrafast + zerolatency.
                    // Default `medium` preset cannot keep up with 30fps at
                    // typical avatar resolutions on a single CPU core.
                    "libx264" => b
                        .set_option(
                            "preset",
                            if preset.is_empty() {
                                "ultrafast"
                            } else {
                                preset.as_str()
                            },
                        )
                        .set_option("tune", "zerolatency"),
                    _ => b,
                };

                match b.build() {
                    Ok(enc) => {
                        tracing::info!(
                            "VideoEncoder using codec '{}' ({}x{} @ {}fps, {} bps)",
                            codec_name,
                            w,
                            h,
                            self.config.framerate,
                            bitrate,
                        );
                        built = Some((codec_name, enc));
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(
                            "VideoEncoder '{}' init failed (will try fallback): {}",
                            codec_name,
                            e
                        );
                        last_err = Some(format!("build({}): {}", codec_name, e));
                    }
                }
            }

            let encoder = built.map(|(_, enc)| enc).ok_or_else(|| {
                CodecError::EncodingFailed(
                    last_err.unwrap_or_else(|| "no codec candidates available".into()),
                )
            })?;
            self.encoder = Some(encoder);
        }

        let encoder = self.encoder.as_mut().unwrap();

        // Create frame and copy YUV plane data
        use ac_ffmpeg::codec::{
            video::{frame::get_pixel_format, VideoFrameMut},
            Encoder,
        };
        use ac_ffmpeg::time::{TimeBase, Timestamp};

        let pixel_format = get_pixel_format("yuv420p");
        let time_base = TimeBase::new(1, self.config.framerate as i32);

        let mut frame = VideoFrameMut::black(pixel_format, width as usize, height as usize);

        {
            let mut planes_mut = frame.planes_mut();
            if !planes_mut.is_empty() {
                let y_plane = planes_mut[0].data_mut();
                let copy_len = y_plane.len().min(y_size);
                y_plane[..copy_len].copy_from_slice(&yuv_buffer[..copy_len]);
            }
            if planes_mut.len() > 1 {
                let u_plane = planes_mut[1].data_mut();
                let copy_len = u_plane.len().min(uv_size);
                u_plane[..copy_len].copy_from_slice(&yuv_buffer[y_size..y_size + copy_len]);
            }
            if planes_mut.len() > 2 {
                let v_plane = planes_mut[2].data_mut();
                let copy_len = v_plane.len().min(uv_size);
                v_plane[..copy_len]
                    .copy_from_slice(&yuv_buffer[y_size + uv_size..y_size + uv_size + copy_len]);
            }
        }

        // Set timestamp
        let pts = Timestamp::new(self.frame_count as i64, time_base);
        let frame = frame.with_time_base(time_base).with_pts(pts).freeze();

        // Encode frame
        encoder.push(frame).map_err(|e| {
            CodecError::EncodingFailed(format!("Failed to push frame to encoder: {}", e))
        })?;

        // Collect encoded packets
        let mut bitstream = Vec::new();
        let mut is_keyframe = false;

        while let Some(packet) = encoder.take().map_err(|e| {
            CodecError::EncodingFailed(format!("Failed to take packet from encoder: {}", e))
        })? {
            bitstream.extend_from_slice(packet.data());
            is_keyframe |= packet.is_key();
        }

        self.frame_count += 1;

        Ok(RuntimeData::Video {
            pixel_data: bitstream,
            width,
            height,
            format: PixelFormat::Encoded,
            codec: Some(self.config.codec),
            frame_number,
            timestamp_us,
            is_keyframe,
            stream_id: None,
            arrival_ts_us: None,
        })
    }

    fn codec(&self) -> VideoCodec {
        self.config.codec
    }

    fn reconfigure(&mut self, config: &VideoEncoderConfig) -> Result<()> {
        self.config = config.clone();
        Ok(())
    }
}

/// FFmpeg decoder implementation
///
/// Currently returns mock decoded frames. To enable actual decoding:
/// 1. Use VideoDecoder::new() from ac-ffmpeg
/// 2. Create Packet from bitstream (may need PacketMut)
/// 3. Push packet, take frames
/// 4. Copy frame planes to RuntimeData
#[cfg(feature = "video")]
pub struct FFmpegDecoder {
    config: VideoDecoderConfig,
    decoder: Option<ac_ffmpeg::codec::video::VideoDecoder>,
}

#[cfg(feature = "video")]
impl FFmpegDecoder {
    pub fn new(config: VideoDecoderConfig) -> Result<Self> {
        Ok(Self {
            config,
            decoder: None, // Lazy initialization on first frame
        })
    }
}

#[cfg(feature = "video")]
impl VideoDecoderBackend for FFmpegDecoder {
    fn decode(&mut self, input: RuntimeData) -> Result<RuntimeData> {
        // Extract encoded video frame
        let (pixel_data, _width, _height, codec, frame_number, timestamp_us) = match input {
            RuntimeData::Video {
                pixel_data,
                width,
                height,
                codec: Some(codec),
                frame_number,
                timestamp_us,
                stream_id: _,
                ..
            } => (pixel_data, width, height, codec, frame_number, timestamp_us),
            RuntimeData::Video { codec: None, .. } => {
                return Err(CodecError::InvalidInput("Frame is not encoded".to_string()));
            }
            _ => {
                return Err(CodecError::InvalidInput("Expected video frame".to_string()));
            }
        };

        if let Some(expected) = self.config.expected_codec {
            if codec != expected {
                return Err(CodecError::InvalidInput(format!(
                    "Codec mismatch: expected {:?}, got {:?}",
                    expected, codec
                )));
            }
        }

        // Lazy initialize decoder on first frame
        if self.decoder.is_none() {
            use ac_ffmpeg::codec::video::VideoDecoder;

            let codec_name = match codec {
                VideoCodec::Vp8 => "vp8",
                VideoCodec::H264 => "h264",
                VideoCodec::Av1 => "av1",
            };

            let decoder = VideoDecoder::builder(codec_name)
                .map_err(|e| {
                    CodecError::DecodingFailed(format!("Failed to create decoder builder: {}", e))
                })?
                .build()
                .map_err(|e| {
                    CodecError::DecodingFailed(format!("Failed to build decoder: {}", e))
                })?;

            self.decoder = Some(decoder);
        }

        let decoder = self.decoder.as_mut().unwrap();

        // Create packet from bitstream
        use ac_ffmpeg::codec::Decoder;
        use ac_ffmpeg::packet::PacketMut;

        let mut packet_mut = PacketMut::new(pixel_data.len());
        packet_mut.data_mut().copy_from_slice(&pixel_data);
        let packet = packet_mut.freeze();

        // Decode packet
        decoder.push(packet).map_err(|e| {
            CodecError::DecodingFailed(format!("Failed to push packet to decoder: {}", e))
        })?;

        // Take decoded frame
        if let Some(frame) = decoder.take().map_err(|e| {
            CodecError::DecodingFailed(format!("Failed to take frame from decoder: {}", e))
        })? {
            // Copy frame planes to output buffer (YUV420P format)
            let mut decoded_pixel_data = Vec::new();

            let planes = frame.planes();
            // Only access first 3 planes (Y, U, V for YUV420P)
            // Planes index 3 may be uninitialized/invalid
            for i in 0..3 {
                if i < planes.len() {
                    let plane_data = planes[i].data();
                    if !plane_data.is_empty() {
                        decoded_pixel_data.extend_from_slice(plane_data);
                    }
                }
            }

            Ok(RuntimeData::Video {
                pixel_data: decoded_pixel_data,
                width: frame.width() as u32,
                height: frame.height() as u32,
                format: self.config.output_format,
                codec: None,
                frame_number,
                timestamp_us,
                is_keyframe: false,
                stream_id: None,
                arrival_ts_us: None,
            })
        } else {
            // No frame available yet (decoder may need more packets)
            // Return empty frame to indicate no output
            Ok(RuntimeData::Video {
                pixel_data: vec![],
                width: 0,
                height: 0,
                format: PixelFormat::Unspecified,
                codec: None,
                frame_number: 0,
                timestamp_us: 0,
                is_keyframe: false,
                stream_id: None,
                arrival_ts_us: None,
            })
        }
    }

    fn codec(&self) -> VideoCodec {
        self.config.expected_codec.unwrap_or(VideoCodec::Vp8)
    }

    fn output_format(&self) -> PixelFormat {
        self.config.output_format
    }
}
