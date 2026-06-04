//! Media tracks for WebRTC
//!
//! Handles RTP track management, encoding, and decoding.

// Phase 4 (US2) media track infrastructure
#![allow(dead_code)]

use super::audio::{AudioEncoder, AudioEncoderConfig};
use super::audio_sender::{AudioSender, MediaClockPublisher};
use super::video::{VideoFormat, VideoFrame};
use crate::{Error, Result};
use parking_lot::RwLock;
use remotemedia_core::data::video::{PixelFormat, VideoCodec};
use remotemedia_core::data::RuntimeData;
use remotemedia_core::nodes::video::{
    VideoDecoderConfig, VideoDecoderNode, VideoEncoderConfig, VideoEncoderNode,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock as AsyncRwLock;
use webrtc::media::Sample;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;

/// Audio track for WebRTC
///
/// Manages audio encoding/decoding and RTP transmission.
pub struct AudioTrack {
    /// Underlying WebRTC track
    track: Arc<TrackLocalStaticSample>,

    /// Audio encoder. Sync `parking_lot::RwLock` — never held across
    /// an `.await` (see comment in `send_audio`).
    encoder: Arc<RwLock<AudioEncoder>>,

    /// Audio decoder (for receiving audio from remote peer). Sync.
    decoder: Arc<RwLock<super::audio::AudioDecoder>>,

    /// Audio sender with ring buffer. Stays `tokio::sync::RwLock`
    /// because the read-guard is held across `enqueue_frame(...).await`
    /// in `send_audio`; `parking_lot::RwLockReadGuard` is `!Send` and
    /// would break `tokio::spawn` shipping of the containing future.
    sender: Arc<AsyncRwLock<Option<AudioSender>>>,

    /// RTP timestamp (in sample units). Sync parking_lot — only
    /// `u32` reads/writes; no guard ever lives across an await.
    timestamp: Arc<RwLock<u32>>,
}

impl AudioTrack {
    /// Create a new audio track
    ///
    /// # Arguments
    ///
    /// * `track` - Underlying WebRTC track
    /// * `config` - Audio encoder configuration
    pub fn new(track: Arc<TrackLocalStaticSample>, config: AudioEncoderConfig) -> Result<Self> {
        println!("[AUDIOTRACK] Creating new AudioTrack with ring buffer support!");
        let encoder = Arc::new(RwLock::new(AudioEncoder::new(config.clone())?));
        let decoder = Arc::new(RwLock::new(super::audio::AudioDecoder::new(
            config.clone(),
        )?));

        // Create audio sender with ring buffer
        // Large buffer allows TTS to generate audio in bursts without blocking
        println!("[AUDIOTRACK] About to create AudioSender...");
        let sender = AudioSender::new(Arc::clone(&track), config.ring_buffer_capacity);
        println!("[AUDIOTRACK] AudioSender created!");

        Ok(Self {
            track,
            encoder,
            decoder,
            // Async RwLock: see field doc — guard held across await.
            sender: Arc::new(AsyncRwLock::new(Some(sender))),
            timestamp: Arc::new(RwLock::new(0)),
        })
    }

    /// Send audio samples over RTP
    ///
    /// # Arguments
    ///
    /// * `samples` - Audio samples as f32 (range -1.0 to 1.0)
    /// * `sample_rate` - Sample rate of the audio in Hz
    ///
    /// # Note
    ///
    /// This method encodes audio to Opus and enqueues frames into a ring buffer.
    /// A dedicated thread continuously dequeues frames and sends them at real-time pace.
    /// Opus requires specific frame sizes (2.5, 5, 10, 20, 40, or 60ms).
    /// We chunk the input into 20ms frames (320 @ 16kHz, 480 @ 24kHz, 960 @ 48kHz).
    /// The encoder will be recreated if the sample rate changes.
    ///
    /// ARCHITECTURE:
    /// - Production (this method): Encode frames as fast as possible, enqueue to ring buffer
    /// - Transmission (dedicated thread): Dequeue frames and send at real-time pace (20ms intervals)
    /// - This decouples TTS generation speed from playback speed, preventing interruptions
    pub async fn send_audio(&self, samples: Arc<Vec<f32>>, sample_rate: u32) -> Result<()> {
        use tracing::{info, trace};

        // Phase B2: encoder / decoder / sender / timestamp locks
        // swapped from `tokio::sync::RwLock` to `parking_lot::RwLock`.
        // None of these lock sites hold across an `.await` — the
        // `enqueue_frame(...).await` below happens *after* the
        // encoder write-lock is released inside the `encode(...)` call
        // on line 148, so parking_lot is safe and ~1 CAS per acquire
        // uncontended.

        // Check if encoder needs to be recreated for different sample rate
        {
            let encoder = self.encoder.read();
            if encoder.config.sample_rate != sample_rate {
                drop(encoder);
                let old_rate = self.encoder.read().config.sample_rate;
                info!(
                    "Sample rate changed from {} to {} Hz, recreating encoder",
                    old_rate, sample_rate
                );

                let mut encoder_write = self.encoder.write();
                let new_config = crate::media::audio::AudioEncoderConfig {
                    sample_rate,
                    channels: encoder_write.config.channels,
                    bitrate: encoder_write.config.bitrate,
                    complexity: encoder_write.config.complexity,
                    ring_buffer_capacity: encoder_write.config.ring_buffer_capacity,
                };
                *encoder_write = crate::media::audio::AudioEncoder::new(new_config)?;
                info!("Encoder recreated with sample rate: {} Hz", sample_rate);
            }
        }

        let frame_size = (sample_rate as usize * 20) / 1000; // 20ms frame
        let frame_duration = Duration::from_millis(20);

        trace!(
            "AudioTrack: Enqueuing {} samples as {}sample frames @ {}Hz (duration: {:.2}s)",
            samples.len(),
            frame_size,
            sample_rate,
            samples.len() as f64 / sample_rate as f64
        );

        // `sender` is `tokio::sync::RwLock` — the read-guard needs to
        // be held across `.enqueue_frame(...).await` below, and a
        // parking_lot guard wouldn't be `Send`, breaking `tokio::spawn`
        // shipping of the containing future.
        let sender_guard = self.sender.read().await;
        let sender = sender_guard
            .as_ref()
            .ok_or_else(|| Error::MediaTrackError("AudioSender not initialized".to_string()))?;

        let mut frames_enqueued = 0;

        // Process audio in chunks and enqueue frames
        for chunk in samples.chunks(frame_size) {
            // Opus requires exact frame sizes - pad last chunk if needed
            let samples_to_encode: Vec<f32> = if chunk.len() < frame_size {
                let mut padded = chunk.to_vec();
                padded.resize(frame_size, 0.0); // Pad with silence
                padded
            } else {
                chunk.to_vec()
            };

            // Encode this chunk. Write-lock is taken, `encode` is
            // synchronous, lock released before the `.await` below.
            let encoded = self.encoder.write().encode(&samples_to_encode)?;

            // Enqueue frame into ring buffer (non-blocking)
            sender
                .enqueue_frame(encoded, chunk.len() as u32, frame_duration)
                .await?;
            frames_enqueued += 1;
        }

        trace!(
            "AudioTrack: Enqueued {} frames into ring buffer (buffer size: {})",
            frames_enqueued,
            sender.buffer_len()
        );

        Ok(())
    }

    /// Get the underlying WebRTC track
    pub fn track(&self) -> Arc<TrackLocalStaticSample> {
        Arc::clone(&self.track)
    }

    /// Decode received RTP packet to audio samples
    ///
    /// # Arguments
    ///
    /// * `payload` - RTP payload (Opus encoded data)
    ///
    /// # Returns
    ///
    /// Decoded audio samples as f32 (range -1.0 to 1.0) at the decoder's configured sample rate.
    ///
    /// # Note
    ///
    /// This method decodes incoming Opus RTP payloads from the remote peer.
    /// Used for bidirectional audio (VAD, STT, etc.).
    pub async fn on_rtp_packet(&self, payload: &[u8]) -> Result<Vec<f32>> {
        use tracing::trace;

        trace!("Decoding RTP packet with {} bytes", payload.len());

        // Decode the Opus payload. Sync parking_lot lock; decoder
        // work is CPU-bound and doesn't await.
        let samples = self.decoder.write().decode(payload)?;

        trace!("Decoded {} samples from RTP packet", samples.len());

        Ok(samples)
    }

    /// Drain any queued TTS audio from the sender ring buffer
    /// without stopping the sender thread.
    ///
    /// Used on barge-in: the user started speaking while the
    /// assistant still had ~10 s of generated audio queued for
    /// playback. Without this the assistant keeps talking over
    /// the user until the ring buffer drains. Returns the number
    /// of frames that were dropped.
    pub async fn flush_send_buffer(&self) -> usize {
        if let Some(sender) = self.sender.read().await.as_ref() {
            sender.flush_buffer()
        } else {
            0
        }
    }

    /// Get current RTP timestamp
    pub async fn timestamp(&self) -> u32 {
        // Use the sender's timestamp if available (more accurate for
        // ring buffer approach). `sender` is still the tokio RwLock.
        if let Some(sender) = self.sender.read().await.as_ref() {
            sender.timestamp()
        } else {
            *self.timestamp.read()
        }
    }

    /// Shutdown the audio track and wait for sender thread to complete
    pub async fn shutdown(&self) -> Result<()> {
        // Take the sender under the async write-lock, then await the
        // actual shutdown (the `.take()` returns owned).
        let taken = self.sender.write().await.take();
        if let Some(sender) = taken {
            sender.shutdown().await?;
        }
        Ok(())
    }

    /// Attach a `MediaClockPublisher` to the inner `AudioSender` so it
    /// fires one `MediaClock` event per successful Opus frame send.
    /// No-op if the inner sender hasn't been initialized yet (the
    /// rare path where `send_audio` hasn't been called); the publisher
    /// can be re-attached later by re-calling this method.
    ///
    /// Phase 5.1 / Option-C wiring (TrackRegistry → tracks):
    /// `TrackRegistry::register_audio_track` calls this automatically
    /// when an `Arc<SessionControl>` has been attached to the
    /// registry, so callers don't typically invoke it directly.
    pub async fn set_media_clock(&self, publisher: MediaClockPublisher) {
        if let Some(sender) = self.sender.read().await.as_ref() {
            sender.set_media_clock(publisher);
        }
    }

    /// Detach + stop the publisher, if any. Closes the broadcast so
    /// subscribed wire pacers exit cleanly.
    pub async fn detach_media_clock(&self) {
        if let Some(sender) = self.sender.read().await.as_ref() {
            sender.detach_media_clock();
        }
    }
}

/// Video track for WebRTC
///
/// Manages video encoding/decoding and RTP transmission using core video nodes.
pub struct VideoTrack {
    /// Underlying WebRTC track
    track: Arc<TrackLocalStaticSample>,

    /// Video encoder (core VideoEncoderNode for VP8/H.264/AV1)
    encoder: Arc<VideoEncoderNode>,

    /// Video decoder (core VideoDecoderNode for VP8/H.264/AV1)
    decoder: Arc<VideoDecoderNode>,

    /// Previous source PTS (microseconds), used to compute per-Sample
    /// duration as the actual inter-frame interval. webrtc-rs advances
    /// the RTP timestamp clock by `sample.duration * 90_000`, so when
    /// a frame arrives N µs after the prior one the RTP delta is N×0.09
    /// — meaning RTP timestamps reflect *source* time and the browser
    /// can detect lag instead of replaying buffered frames at a fixed
    /// 30 fps cadence. `None` until the first frame.
    prev_pts_us: Arc<RwLock<Option<u64>>>,

    /// Monotonic frame counter, used by `should_force_keyframe()` to
    /// nudge a keyframe every 60 frames. Replaces the removed
    /// `sequence_number` field which was incremented for the same
    /// purpose but also misleadingly looked like the RTP sequence
    /// number (which webrtc-rs owns).
    frame_count: Arc<std::sync::atomic::AtomicU32>,

    /// Wall-clock instant assigned to source PTS = 0. Sample.timestamp
    /// (the SR/NTP capture time) is computed as `epoch + pts_us` so
    /// downstream RTCP sender reports describe when the frame was
    /// produced, not when the encoder finished it.
    epoch_wall_clock: std::time::SystemTime,

    /// Video codec being used
    codec: VideoCodec,

    /// Configured framerate, used as the fallback frame duration on
    /// the very first Sample (when there is no prior PTS to subtract)
    /// and to compute `wire_deadline_us` on the media_clock publisher.
    framerate: u32,

    /// Optional media-clock publisher (Phase 5.2). When attached via
    /// `set_media_clock`, every successful encoded RTP write fires one
    /// `MediaClock` event so subscribed wire pacers tick at the
    /// outbound video cadence. `parking_lot::RwLock` because it's
    /// set once at session bind and otherwise only read by the send
    /// path.
    media_clock: Arc<RwLock<Option<MediaClockPublisher>>>,
}

impl VideoTrack {
    /// Create a new video track with core encoder/decoder
    ///
    /// # Arguments
    ///
    /// * `track` - Underlying WebRTC track
    /// * `codec` - Video codec to use (VP8, H.264, or AV1)
    /// * `width` - Frame width in pixels
    /// * `height` - Frame height in pixels
    /// * `bitrate` - Target bitrate in bits/second
    /// * `framerate` - Target framerate
    pub fn new(
        track: Arc<TrackLocalStaticSample>,
        codec: VideoCodec,
        _width: u32,
        _height: u32,
        bitrate: u32,
        framerate: u32,
    ) -> Result<Self> {
        // Create core encoder
        let encoder_config = VideoEncoderConfig {
            codec,
            bitrate,
            framerate,
            keyframe_interval: 60,
            quality_preset: "medium".to_string(),
            hardware_accel: true,
            threads: 0,
        };
        let encoder = VideoEncoderNode::new(encoder_config)
            .map_err(|e| Error::EncodingError(format!("Failed to create video encoder: {}", e)))?;

        // Create core decoder
        let decoder_config = VideoDecoderConfig {
            expected_codec: Some(codec),
            output_format: PixelFormat::Yuv420p,
            hardware_accel: true,
            threads: 0,
            error_resilience: "lenient".to_string(),
        };
        let decoder = VideoDecoderNode::new(decoder_config)
            .map_err(|e| Error::EncodingError(format!("Failed to create video decoder: {}", e)))?;

        Ok(Self {
            track,
            encoder: Arc::new(encoder),
            decoder: Arc::new(decoder),
            prev_pts_us: Arc::new(RwLock::new(None)),
            epoch_wall_clock: std::time::SystemTime::now(),
            frame_count: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            codec,
            framerate,
            media_clock: Arc::new(RwLock::new(None)),
        })
    }

    /// Send video frame over RTP (legacy VideoFrame API)
    ///
    /// # Arguments
    ///
    /// * `frame` - WebRTC VideoFrame
    ///
    /// # Note
    ///
    /// Converts VideoFrame to RuntimeData, encodes with core, sends via RTP.
    /// For new code, use `send_video_runtime_data` directly with RuntimeData::Video.
    pub async fn send_video(&self, frame: &VideoFrame) -> Result<()> {
        // Convert VideoFrame to RuntimeData
        let runtime_data = RuntimeData::Video {
            pixel_data: frame.data.clone(),
            width: frame.width,
            height: frame.height,
            format: match frame.format {
                VideoFormat::I420 => PixelFormat::I420,
                VideoFormat::NV12 => PixelFormat::NV12,
                VideoFormat::RGB24 => PixelFormat::Rgb24,
            },
            codec: None, // Raw frame
            frame_number: 0,
            timestamp_us: frame.timestamp_us,
            is_keyframe: frame.is_keyframe,
            stream_id: None,
            arrival_ts_us: None,
        };

        self.send_video_runtime_data(runtime_data).await
    }

    /// Send video frame over RTP (encodes using core VideoEncoderNode)
    ///
    /// # Arguments
    ///
    /// * `runtime_data` - Raw video frame (RuntimeData::Video with codec=None)
    ///
    /// # Note
    ///
    /// Encodes raw frames using core VideoEncoderNode (VP8/H.264/AV1 via ac-ffmpeg),
    /// then sends via WebRTC RTP with proper timestamp handling (90kHz clock).
    ///
    /// Phase 5 T067, T078: RTP transmission with 90kHz timestamp mapping
    pub async fn send_video_runtime_data(&self, runtime_data: RuntimeData) -> Result<()> {
        use remotemedia_core::nodes::streaming_node::AsyncStreamingNode;

        // Encode raw frame using core VideoEncoderNode
        let encoded = self
            .encoder
            .process(runtime_data)
            .await
            .map_err(|e| Error::EncodingError(format!("Video encoding failed: {}", e)))?;

        // Extract encoded bitstream + source PTS. The source PTS is what
        // anchors RTP timestamps to wall-clock; without it the browser
        // can't detect encoder lag and stale buffered frames replay as if
        // current.
        let (bitstream, pts_us) = match &encoded {
            RuntimeData::Video {
                pixel_data,
                codec: Some(_),
                timestamp_us,
                ..
            } => (pixel_data.clone(), *timestamp_us),
            _ => {
                return Err(Error::EncodingError(
                    "Expected encoded video frame".to_string(),
                ))
            }
        };

        // Compute Sample.duration from the inter-frame interval in
        // *source* time. webrtc-rs advances the RTP timestamp clock by
        // `duration * 90_000` (clock_rate for video), so with this in
        // place the RTP timestamps reflect when the source produced the
        // frame, not when the encoder finished. Browsers can then
        // correctly judge "this frame is N seconds late" and skip ahead
        // instead of slogging through a buffered backlog at 30 fps.
        //
        // First frame falls back to nominal 1/framerate (no prior PTS
        // to subtract). Non-monotonic PTS (clock reset, missed frame)
        // also falls back so we never hand webrtc-rs a negative or
        // zero duration that would stall the packetizer.
        let frame_duration = {
            let mut prev = self.prev_pts_us.write();
            let dur = match *prev {
                Some(prev_pts) if pts_us > prev_pts => Duration::from_micros(pts_us - prev_pts),
                _ => Duration::from_micros(if self.framerate > 0 {
                    1_000_000 / self.framerate as u64
                } else {
                    33_333
                }),
            };
            *prev = Some(pts_us);
            dur
        };

        // Sample.timestamp is the SR/NTP capture time used by RTCP
        // sender reports. Tying it to `epoch + pts_us` keeps it aligned
        // with source production time across encoder lag.
        let capture_wall_clock = self.epoch_wall_clock + Duration::from_micros(pts_us);

        // Bump frame counter for `should_force_keyframe`'s 60-frame
        // cadence (matches the encoder config). Relaxed because we
        // never use it for synchronisation — only for the modulo check.
        self.frame_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Create WebRTC sample with encoded bitstream
        let sample = Sample {
            data: bitstream.into(),
            duration: frame_duration,
            timestamp: capture_wall_clock,
            ..Default::default()
        };

        // Send sample (webrtc-rs handles RTP packetization per RFC 7741/6184)
        self.track
            .write_sample(&sample)
            .await
            .map_err(|e| Error::MediaTrackError(format!("Failed to write RTP sample: {}", e)))?;

        // Phase 5.2: publish a media_clock tick after each successful
        // encoded RTP write. Subscribers (wire-bound `WireTickSource`s
        // driving `ClockedToOutboundMedia` nodes — e.g. AvatarNode)
        // read `pts_us` and `wire_deadline_us` to drive `tick()` at
        // the actual outbound video cadence. No-op when no publisher
        // is attached.
        let publisher = self.media_clock.read().clone();
        if let Some(publisher) = publisher {
            // Per-frame budget = 1s / framerate. Matches the
            // duration we set on the WebRTC sample above.
            let frame_duration = if self.framerate > 0 {
                Duration::from_micros(1_000_000 / self.framerate as u64)
            } else {
                Duration::from_millis(33)
            };
            publisher.publish(frame_duration);
        }

        Ok(())
    }

    /// Attach a [`MediaClockPublisher`]. After this call, every
    /// successful encoded RTP write fires one `MediaClock` event on
    /// the publisher's address. Idempotent — replaces any previously
    /// attached publisher (callers that want a clean handoff should
    /// call `stop` on the prior publisher first).
    ///
    /// Phase 5.2 of the pacing-domains spec.
    pub fn set_media_clock(&self, publisher: MediaClockPublisher) {
        *self.media_clock.write() = Some(publisher);
    }

    /// Detach and stop the media-clock publisher (if any) so
    /// subscribed wire pacers exit cleanly. Safe to call when no
    /// publisher is attached.
    ///
    /// Phase 5.9 of the pacing-domains spec.
    pub fn detach_media_clock(&self) {
        if let Some(publisher) = self.media_clock.write().take() {
            publisher.stop();
        }
    }

    /// Get the underlying WebRTC track
    pub fn track(&self) -> Arc<TrackLocalStaticSample> {
        Arc::clone(&self.track)
    }

    /// Decode received RTP packet to RuntimeData (uses core VideoDecoderNode)
    ///
    /// # Arguments
    ///
    /// * `payload` - RTP payload (encoded video bitstream)
    ///
    /// # Returns
    ///
    /// Decoded RuntimeData::Video frame
    ///
    /// # Note
    ///
    /// Decodes RTP payload using core VideoDecoderNode (VP8/H.264/AV1 via ac-ffmpeg).
    /// Phase 5 T068: RTP depacketization and decoding
    pub async fn decode_rtp_payload(&self, payload: &[u8]) -> Result<RuntimeData> {
        use remotemedia_core::nodes::streaming_node::AsyncStreamingNode;

        // Create encoded RuntimeData from RTP payload
        let encoded_data = RuntimeData::Video {
            pixel_data: payload.to_vec(),
            width: 1280, // Will be overridden by decoder
            height: 720,
            format: PixelFormat::Encoded,
            codec: Some(self.codec),
            frame_number: 0,
            timestamp_us: 0,
            is_keyframe: false,
            stream_id: None,
            arrival_ts_us: None,
        };

        // Decode using core VideoDecoderNode
        self.decoder
            .process(encoded_data)
            .await
            .map_err(|e| Error::EncodingError(format!("Video decoding failed: {}", e)))
    }

    /// Get the video codec being used
    pub fn codec(&self) -> VideoCodec {
        self.codec
    }

    /// Check if the next frame should be a keyframe.
    /// Keyframe every 60 frames (matches encoder config).
    pub async fn should_force_keyframe(&self) -> bool {
        let n = self.frame_count.load(std::sync::atomic::Ordering::Relaxed);
        n.is_multiple_of(60)
    }

    /// Legacy method: Decode received RTP packet to VideoFrame
    ///
    /// For new code, use `decode_rtp_payload` which returns RuntimeData::Video
    pub async fn on_rtp_packet(&self, payload: &[u8]) -> Result<VideoFrame> {
        let runtime_data = self.decode_rtp_payload(payload).await?;

        // Convert RuntimeData::Video back to VideoFrame for backward compatibility
        match runtime_data {
            RuntimeData::Video {
                pixel_data,
                width,
                height,
                format,
                ..
            } => {
                let video_format = match format {
                    PixelFormat::I420 | PixelFormat::Yuv420p => VideoFormat::I420,
                    PixelFormat::NV12 => VideoFormat::NV12,
                    PixelFormat::Rgb24 => VideoFormat::RGB24,
                    _ => VideoFormat::I420,
                };

                Ok(VideoFrame {
                    width,
                    height,
                    format: video_format,
                    data: pixel_data,
                    timestamp_us: 0,
                    is_keyframe: false,
                })
            }
            _ => Err(Error::EncodingError("Expected video frame".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;

    #[tokio::test]
    async fn test_audio_track_creation() {
        let track = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: "audio/opus".to_string(),
                ..Default::default()
            },
            "audio".to_string(),
            "stream".to_string(),
        ));

        let config = AudioEncoderConfig::default();
        let audio_track = AudioTrack::new(track, config);
        assert!(audio_track.is_ok());
    }

    #[tokio::test]
    async fn test_video_track_creation() {
        let track = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: "video/VP8".to_string(),
                ..Default::default()
            },
            "video".to_string(),
            "stream".to_string(),
        ));

        let video_track = VideoTrack::new(track, VideoCodec::Vp8, 1280, 720, 2_000_000, 30);
        assert!(video_track.is_ok());
    }
}

/// Convert RuntimeData to RTP-ready format (T072)
///
/// # Arguments
///
/// * `data` - Runtime data (Audio or Video)
/// * `audio_track` - Optional audio track for encoding
/// * `video_track` - Optional video track for encoding
///
/// # Returns
///
/// RTP payload bytes ready to send
///
/// # Note
///
/// This function encodes RuntimeData to the appropriate codec format (Opus for audio, VP9 for video).
pub async fn runtime_data_to_rtp(
    data: &RuntimeData,
    audio_track: Option<&AudioTrack>,
    video_track: Option<&VideoTrack>,
) -> Result<Vec<u8>> {
    match data {
        RuntimeData::Audio {
            samples,
            sample_rate,
            ..
        } => {
            let audio_track = audio_track.ok_or_else(|| {
                Error::MediaTrackError("No audio track available for encoding".to_string())
            })?;

            // Verify sample rate matches encoder config (48kHz expected)
            if *sample_rate != 48000 {
                return Err(Error::InvalidConfig(format!(
                    "Audio sample rate must be 48000 Hz, got {}",
                    sample_rate
                )));
            }

            // Encode the audio samples
            audio_track.encoder.write().encode(samples)
        }

        RuntimeData::Video {
            width,
            height,
            pixel_data,
            format,
            ..
        } => {
            let video_track = video_track.ok_or_else(|| {
                Error::MediaTrackError("No video track available for encoding".to_string())
            })?;

            // Convert PixelFormat enum to VideoFormat enum
            let video_format = match format {
                PixelFormat::Rgb24 => VideoFormat::RGB24,
                PixelFormat::Yuv420p | PixelFormat::I420 => VideoFormat::I420,
                _ => {
                    return Err(Error::EncodingError(format!(
                        "Unsupported video format: {:?}",
                        format
                    )))
                }
            };

            // Create VideoFrame
            let frame = VideoFrame {
                width: *width,
                height: *height,
                format: video_format,
                data: pixel_data.clone(),
                timestamp_us: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64,
                is_keyframe: video_track.should_force_keyframe().await,
            };

            // Encode using VideoTrack's send_video method
            video_track.send_video(&frame).await?;
            Ok(vec![]) // Return empty Vec since data is sent via track
        }

        _ => Err(Error::MediaTrackError(
            "Unsupported RuntimeData type for RTP encoding".to_string(),
        )),
    }
}

/// Get current timestamp in microseconds for arrival time stamping (spec 026)
fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

/// Convert RTP payload to RuntimeData (T073)
///
/// # Arguments
///
/// * `payload` - RTP payload bytes
/// * `is_audio` - True for audio (Opus), false for video (VP9)
/// * `audio_track` - Optional audio track for decoding
/// * `video_track` - Optional video track for decoding
///
/// # Returns
///
/// Decoded RuntimeData (Audio or Video) with arrival_ts_us stamped (spec 026)
///
/// # Note
///
/// This function decodes RTP payloads (Opus or VP9) back to RuntimeData format.
/// The arrival_ts_us is stamped at transport ingest for drift monitoring.
pub async fn rtp_to_runtime_data(
    payload: &[u8],
    is_audio: bool,
    audio_track: Option<&AudioTrack>,
    video_track: Option<&VideoTrack>,
) -> Result<RuntimeData> {
    // Stamp arrival time at transport ingest (spec 026)
    let arrival_ts_us = Some(now_micros());

    if is_audio {
        let audio_track = audio_track.ok_or_else(|| {
            Error::MediaTrackError("No audio track available for decoding".to_string())
        })?;

        // Decode Opus to f32 samples
        let samples = audio_track.on_rtp_packet(payload).await?;

        Ok(RuntimeData::Audio {
            samples: samples.into(),
            sample_rate: 48000, // Opus always decodes to 48kHz
            channels: 1,        // Assuming mono for now
            stream_id: None,
            timestamp_us: None,
            arrival_ts_us,
            metadata: None,
        })
    } else {
        let video_track = video_track.ok_or_else(|| {
            Error::MediaTrackError("No video track available for decoding".to_string())
        })?;

        // Decode VP9 to VideoFrame
        let frame = video_track.on_rtp_packet(payload).await?;

        // Convert VideoFormat to PixelFormat enum
        let format = match frame.format {
            VideoFormat::I420 => PixelFormat::I420,
            VideoFormat::NV12 => PixelFormat::NV12,
            VideoFormat::RGB24 => PixelFormat::Rgb24,
        };

        Ok(RuntimeData::Video {
            pixel_data: frame.data,
            width: frame.width,
            height: frame.height,
            format,
            codec: None,     // Raw frame from WebRTC
            frame_number: 0, // Will be set by caller if needed
            timestamp_us: frame.timestamp_us,
            is_keyframe: false, // Will be set by encoder
            stream_id: None,
            arrival_ts_us,
        })
    }
}
