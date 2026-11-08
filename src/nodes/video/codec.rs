//! Video codec backend abstractions
//!
//! Provides traits and implementations for video encoding/decoding backends.
//! Primary backend: ac-ffmpeg (FFmpeg bindings)
//! Optional: rav1e (pure Rust AV1 encoder)
//!
//! The `video` feature runs real ac-ffmpeg backends; codec availability depends
//! on the linked FFmpeg build. Single encoded packets retain their immutable
//! FFmpeg owner. Input frame/packet staging and decoded frame packing remain
//! separate ownership and layout boundaries.

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

    #[error(transparent)]
    CopyRejected(#[from] remotemedia_types::CopyRejected),

    #[error(transparent)]
    OutputAllocation(#[from] remotemedia_types::BufferAllocationError),

    #[error("Invalid decoded output: {0}")]
    OutputLayout(String),

    #[error("Codec task failed: {0}")]
    TaskFailed(String),
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

    /// Execute with caller-owned copy authority. Backends must opt in explicitly.
    fn encode_with_copy_context(
        &mut self,
        _input: RuntimeData,
        _copies: &remotemedia_types::CopyContext,
    ) -> Result<RuntimeData> {
        Err(CodecError::NotAvailable(
            "encoder does not support contextual copy accounting".into(),
        ))
    }

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

    /// Preserve caller copy authority and selected output storage, or reject.
    fn decode_with_context(
        &mut self,
        _input: RuntimeData,
        _copies: &remotemedia_types::CopyContext,
        _allocator: &dyn remotemedia_types::OutputAllocator,
    ) -> Result<RuntimeData> {
        Err(CodecError::NotAvailable(
            "decoder does not support contextual output allocation".into(),
        ))
    }

    /// Get the codec this decoder handles
    fn codec(&self) -> VideoCodec;

    /// Get the output pixel format
    fn output_format(&self) -> PixelFormat;
}

/// FFmpeg encoder implementation
///
/// Lazily selects an available encoder and retains single-packet output storage.
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
        self.encode_with_copy_context(input, remotemedia_types::CopyContext::legacy())
    }

    fn encode_with_copy_context(
        &mut self,
        input: RuntimeData,
        copies: &remotemedia_types::CopyContext,
    ) -> Result<RuntimeData> {
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

        let frame = super::frames::encoder_frame(&pixel_data, width, height, format, copies)?;

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
            //
            // `h264_mf` (Windows Media Foundation) is the last H.264
            // resort: the MSVC FFmpeg build from setup-ffmpeg.ps1 has no
            // libx264/NVENC, while Windows always has MF's software encoder.
            let candidates: &[&str] = match (self.config.codec, self.config.hardware_accel) {
                (VideoCodec::H264, true) => &["h264_nvenc", "libx264", "h264_mf"],
                (VideoCodec::H264, false) => &["libx264", "h264_mf"],
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
                    // Media Foundation: CBR for the remote-display
                    // scenario, no B-frames. Software MFT: hardware MFTs
                    // (NVIDIA's included) only negotiate NV12, and this
                    // encoder is fed yuv420p.
                    "h264_mf" => b
                        .set_option("rate_control", "cbr")
                        .set_option("scenario", "display_remoting")
                        .set_option("bf", "0"),
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

        use ac_ffmpeg::codec::Encoder;
        use ac_ffmpeg::time::{TimeBase, Timestamp};
        let time_base = TimeBase::new(1, self.config.framerate as i32);

        // Set timestamp
        let pts = Timestamp::new(self.frame_count as i64, time_base);
        let frame = frame.with_time_base(time_base).with_pts(pts).freeze();

        // Encode frame
        encoder.push(frame).map_err(|e| {
            CodecError::EncodingFailed(format!("Failed to push frame to encoder: {}", e))
        })?;

        // Preserve a single packet's immutable FFmpeg owner. Multiple packets
        // require contiguous aggregation in the existing RuntimeData wire shape.
        let (bitstream, is_keyframe) = super::packets::take_encoded(encoder, copies)?;

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
/// Lazily creates a decoder and reuses complete FFmpeg packet owners. Arbitrary
/// packet ingress and raw frame output still require staging.
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
        self.decode_with_context(
            input,
            remotemedia_types::CopyContext::legacy(),
            &remotemedia_types::HeapOutputAllocator,
        )
    }

    fn decode_with_context(
        &mut self,
        input: RuntimeData,
        copies: &remotemedia_types::CopyContext,
        allocator: &dyn remotemedia_types::OutputAllocator,
    ) -> Result<RuntimeData> {
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

        // Reuse a complete FFmpeg packet owner when available. Arbitrary bytes
        // still require padded backend storage under the caller's authority.
        use ac_ffmpeg::codec::Decoder;
        let packet = super::packets::decoder_packet(pixel_data, copies)?;

        // Decode packet
        decoder.push(packet).map_err(|e| {
            CodecError::DecodingFailed(format!("Failed to push packet to decoder: {}", e))
        })?;

        // Take decoded frame
        if let Some(frame) = decoder.take().map_err(|e| {
            CodecError::DecodingFailed(format!("Failed to take frame from decoder: {}", e))
        })? {
            let decoded_pixel_data = super::decoded_frames::decoder_frame(
                &frame,
                self.config.output_format,
                copies,
                allocator,
            )?;

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
                pixel_data: Vec::new().into(),
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
