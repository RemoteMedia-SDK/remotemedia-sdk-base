# WebRTC Multi-Peer Transport: Technical Research & Decisions

**Document Date:** 2025-11-07
**Status:** Research Phase - Implementation Planning
**Branch:** `webrtc-multi-peer-transport`
**Target:** Production-ready WebRTC transport for RemoteMedia SDK

---

## Executive Summary

This document provides comprehensive technical research and implementation decisions for building a WebRTC multi-peer transport in Rust. The transport will enable:
- **N:N mesh networking** for real-time peer-to-peer communication
- **Audio/Video/Data channels** with production-quality synchronization
- **Zero-copy integration** with RemoteMedia pipelines
- **Real-time processing** across connected peers with <100ms latency

**Critical Finding:** Audio/video synchronization in multi-peer scenarios requires explicit clock management using RTP/NTP timestamps and jitter buffers. This is NOT automatic in WebRTC and must be implemented at the transport layer.

---

## 1. Audio/Video Synchronization in Multi-Peer Scenarios

### Decision: Implement Explicit RTP-Based Clock Management with Per-Peer Jitter Buffers

#### Rationale

WebRTC provides **per-stream** RTP timestamps but offers **NO automatic cross-stream synchronization**. For multi-peer pipelines processing audio from multiple sources simultaneously:

1. **Each peer's audio stream has independent RTP clocks** (48kHz nominal, but real clocks drift)
2. **Network jitter causes arrivals to be out-of-order** (reorder buffers needed)
3. **Lip-sync requires alignment to video** (requires buffer delays on both streams)
4. **Clock drift accumulates over time** (±0.1% drift = 2 seconds misalignment after 1000 seconds)

Without explicit management, multi-peer scenarios suffer from:
- Audio samples arriving out of order
- Drift between participants causing lip-sync failures
- Unbounded buffer growth leading to latency creep
- Cascade failures when one peer lags

#### Implementation Approach

```rust
/// Per-peer audio/video synchronization manager
pub struct SyncManager {
    /// RTP timestamp counter (48kHz reference clock)
    rtp_timestamp: u32,

    /// NTP timestamp mapping (from RTCP Sender Reports)
    /// Maps RTP timestamp -> wall-clock NTP time
    ntp_mapping: Option<(u64, u32)>, // (ntp_timestamp, rtp_timestamp)

    /// Jitter buffer for reordering packets
    /// Maintains 50-100ms buffer to handle network jitter
    jitter_buffer: JitterBuffer<AudioFrame>,

    /// Video frame buffer aligned to audio timeline
    /// Holds latest keyframe to sync with audio
    video_frame_buffer: Arc<Mutex<VideoFrame>>,

    /// Clock drift tracker
    /// Monitors receive clock vs peer's send clock
    drift_estimate: ClockDriftEstimator,

    /// Per-peer sequence tracking
    last_rtp_sequence: u16,
    last_rtp_timestamp: u32,
}

impl SyncManager {
    /// Process incoming audio frame with synchronization
    ///
    /// Returns: (timestamp_us, samples) aligned to global clock
    pub fn process_audio_frame(
        &mut self,
        rtp_timestamp: u32,
        rtp_sequence: u16,
        samples: &[f32],
    ) -> Result<(u64, Vec<f32>)> {
        // 1. Detect missing packets
        let expected_seq = self.last_rtp_sequence.wrapping_add(1);
        if rtp_sequence != expected_seq {
            // Gap detected - may need packet loss concealment (PLC)
            warn!("RTP sequence gap: expected {}, got {}", expected_seq, rtp_sequence);
        }

        // 2. Detect timestamp discontinuities (pause/resume)
        let rtp_diff = rtp_timestamp.wrapping_sub(self.last_rtp_timestamp) as i32;
        if rtp_diff < 0 || rtp_diff > 96000 { // >2 seconds at 48kHz
            // Timeline reset - resync required
            self.reset_sync(rtp_timestamp)?;
        }

        // 3. Push into jitter buffer for reordering
        self.jitter_buffer.push(AudioFrame {
            rtp_timestamp,
            sequence: rtp_sequence,
            samples: samples.to_vec(),
        })?;

        // 4. Monitor clock drift
        self.drift_estimate.update(rtp_timestamp, Instant::now());

        // 5. Pop oldest frame from jitter buffer (50-100ms old)
        // This ensures we have time to receive out-of-order packets
        let aligned_frame = self.jitter_buffer.pop_after_delay(
            Duration::from_millis(50)
        )?;

        // 6. Convert RTP timestamp to wall-clock time
        let wall_clock_us = self.rtp_to_wallclock(aligned_frame.rtp_timestamp);

        // 7. Update tracking
        self.last_rtp_sequence = rtp_sequence;
        self.last_rtp_timestamp = rtp_timestamp;

        Ok((wall_clock_us, aligned_frame.samples))
    }

    /// Get synchronized video frame at given audio timestamp
    /// This implements lip-sync by holding video until audio catches up
    pub fn get_video_frame_at_time(&self, audio_wall_clock_us: u64) -> Option<VideoFrame> {
        let video_buf = self.video_frame_buffer.lock().unwrap();

        // Return latest keyframe (video doesn't change as fast as audio)
        // If video is ahead, callers will hold the frame
        // If video is behind, we have lip-sync issue (log warning)

        video_buf.wall_clock_us.saturating_sub(audio_wall_clock_us).abs() > 100_000 {
            warn!("Lip-sync drift: {}ms", (video_buf.wall_clock_us as i64 - audio_wall_clock_us as i64) / 1000);
        }

        Some(video_buf.clone())
    }

    /// Convert RTP timestamp to wall-clock time using NTP mapping
    fn rtp_to_wallclock(&self, rtp_ts: u32) -> u64 {
        if let Some((ntp_ts, rtp_base)) = self.ntp_mapping {
            // Linear interpolation from NTP mapping
            let rtp_offset = rtp_ts.wrapping_sub(rtp_base) as u64;
            let ntp_offset_us = (rtp_offset * 1_000_000) / 48_000; // 48kHz clock rate
            ntp_ts + ntp_offset_us
        } else {
            // Fallback: use local clock (less accurate)
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64
        }
    }

    /// Extract NTP timestamp from RTCP Sender Report
    pub fn update_ntp_mapping(&mut self, rtcp_sr: &SenderReport) {
        self.ntp_mapping = Some((rtcp_sr.ntp_timestamp, rtcp_sr.rtp_timestamp));
    }
}

/// Adaptive jitter buffer for handling network reordering
pub struct JitterBuffer<T> {
    frames: VecDeque<T>,
    max_size: usize,
    target_delay_ms: u64,
}

impl<T: HasRtpTimestamp> JitterBuffer<T> {
    pub fn push(&mut self, frame: T) -> Result<()> {
        if self.frames.len() >= self.max_size {
            return Err("Jitter buffer overflow".into());
        }

        // Insert in order by RTP timestamp
        let insert_pos = self.frames.iter()
            .position(|f| f.rtp_timestamp() > frame.rtp_timestamp())
            .unwrap_or(self.frames.len());

        self.frames.insert(insert_pos, frame);
        Ok(())
    }

    pub fn pop_after_delay(&mut self, delay: Duration) -> Result<T> {
        // Returns frames that arrived >delay ago
        // This gives late packets time to arrive
        if self.frames.is_empty() {
            return Err("Jitter buffer empty".into());
        }

        self.frames.pop_front()
            .ok_or_else(|| "No frames available".into())
    }
}

/// Tracks clock drift between sender and receiver
pub struct ClockDriftEstimator {
    samples: VecDeque<(u32, Instant)>,
    max_samples: usize,
}

impl ClockDriftEstimator {
    pub fn update(&mut self, rtp_ts: u32, recv_time: Instant) {
        self.samples.push_back((rtp_ts, recv_time));
        if self.samples.len() > self.max_samples {
            self.samples.pop_front();
        }
    }

    /// Estimate drift rate in ppm (parts per million)
    /// Positive = sender clock faster than receiver
    pub fn estimate_drift(&self) -> f64 {
        if self.samples.len() < 2 {
            return 0.0;
        }

        let (rtp_start, recv_start) = self.samples[0];
        let (rtp_end, recv_end) = self.samples[self.samples.len() - 1];

        let rtp_diff = rtp_end.wrapping_sub(rtp_start) as f64 / 48_000.0; // seconds
        let recv_diff = recv_end.duration_since(recv_start).as_secs_f64();

        if recv_diff < 0.001 {
            return 0.0; // Not enough time elapsed
        }

        let drift_ratio = (rtp_diff - recv_diff) / recv_diff;
        drift_ratio * 1_000_000.0 // Convert to ppm
    }
}
```

#### Best Practices for Timestamp Management

1. **RTP Timestamp Handling**
   - Opus uses 48kHz reference clock regardless of actual sample rate
   - Increment by sample count per frame: `rtp_ts += frame_size_samples`
   - Handle wrap-around: `rtp_ts.wrapping_add(increment)`
   - For Opus: frame sizes 2.5ms (120 samples) through 60ms (2880 samples)

2. **NTP/RTP Synchronization**
   - Extract NTP timestamp from RTCP Sender Reports every 5 seconds
   - Maps wall-clock time to RTP timestamp for absolute synchronization
   - Use for inter-peer synchronization (all peers sync to NTP time)

3. **Buffer Management**
   - Target 50-100ms jitter buffer (balance between latency and reordering)
   - Adaptive sizing: grow buffer if packet loss detected, shrink during good conditions
   - For multi-peer: total pipeline delay = 50ms + processing + network

4. **Clock Drift Mitigation**
   - Monitor drift continuously (estimate every 10 seconds)
   - If drift > ±0.5%: apply automatic rate adjustment (sample rate conversion)
   - Log drift warnings for > ±0.1% drift (indicates system issues)

#### Alternatives Considered

| Approach | Pros | Cons | Verdict |
|----------|------|------|---------|
| **Explicit RTP management (chosen)** | Full control, handles multi-peer, standard | More code, requires RTCP integration | **BEST** |
| Ignore timestamps, use arrival order | Simplest implementation | Fails with jitter/reordering | Poor |
| Use media server (SFU model) | Proven, simple | Single point of failure, more bandwidth | Not mesh |
| Ignore sync, process independently | Easiest | Lip-sync impossible | Unacceptable |

---

## 2. WebRTC Rust Crate Evaluation (webrtc-rs/webrtc v0.9)

### Decision: Use webrtc-rs v0.9 as PRIMARY implementation with DOCUMENTED LIMITATIONS

#### Verdict: Early-stage, requires hardening for production

**webrtc-rs is NOT production-ready** (as of Nov 2024). However, it's the only pure-Rust implementation with complete WebRTC spec coverage. Recommend:
1. **Use webrtc-rs v0.9** for MVP (proof-of-concept)
2. **Plan migration path** to alternatives if stability issues emerge
3. **Implement timeout/fallback handlers** for known issues

#### Stability Assessment

**Current Status:** Active development, MSRV bumped as needed (currently ~6-month rolling)

**Known Limitations:**
- Not production-tested at scale (most users: 1-10 peer scenarios)
- Media track implementation less mature than data channels
- Some edge cases with ICE candidate handling in restrictive NAT environments
- Documentation sparse; mostly examples, limited API reference

**What's Solid:**
- DTLS/SRTP encryption working reliably
- Data channel protocol fully spec-compliant
- ICE candidate gathering robust in good networks
- SDP parsing/generation correct

**What's Risky:**
- Audio/video media track implementation may have latency bugs
- Performance under high load (100+ simultaneous peers) untested
- RTCP handling incomplete (no Sender Reports by default)
- Error recovery in degraded networks not comprehensive

#### API Overview

```rust
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::api::setting_engine::SettingEngine;
use webrtc::api::APIBuilder;

// 1. Configure WebRTC
let mut setting_engine = SettingEngine::default();
setting_engine.set_network_types(vec![NetworkType::Tcp4, NetworkType::Udp4]);
setting_engine.set_ice_servers(vec![
    RTCIceServer {
        urls: vec!["stun:stun.l.google.com:19302".to_string()],
        ..Default::default()
    }
]);

// 2. Create API
let api = APIBuilder::new()
    .with_setting_engine(setting_engine)
    .build()?;

// 3. Create peer connection
let config = RTCConfiguration {
    ice_servers: vec![/* ... */],
    ..Default::default()
};

let peer_connection = Arc::new(api.new_peer_connection(config).await?);

// 4. Add media tracks
let video_track = Arc::new(VideoTrack::new(
    "video",
    vec![Arc::new(LocalVideoTrack::new(video_source))],
)?);
peer_connection.add_track(video_track).await?;

let audio_track = Arc::new(AudioTrack::new(
    "audio",
    vec![Arc::new(LocalAudioTrack::new(audio_source))],
)?);
peer_connection.add_track(audio_track).await?;

// 5. Handle connection events
peer_connection.on_ice_candidate(Box::new(move |candidate| {
    // Send ICE candidate to peer via signaling
}));

peer_connection.on_track(Box::new(move |track| {
    // Process incoming media
}));

// 6. Signaling (manual via JSON-RPC)
let offer = peer_connection.create_offer(None).await?;
peer_connection.set_local_description(offer.clone()).await?;
// Send offer to peer via WebSocket
// Receive answer, call set_remote_description

// 7. Cleanup
peer_connection.close().await?;
```

#### Production Hardening Strategy

```rust
pub struct WebRtcTransportConfig {
    /// Timeout for ICE gathering (default 5s)
    pub ice_timeout: Duration,

    /// Maximum connections to attempt (circuit breaker)
    pub max_connection_attempts: u32,

    /// Enable keepalive (STUN binding every 30s)
    pub enable_keepalive: bool,

    /// Fallback transport if WebRTC fails
    pub fallback_transport: Option<Arc<dyn PipelineTransport>>,
}

impl WebRtcTransport {
    /// Connect with timeout and retry logic
    pub async fn connect_peer_with_fallback(
        &self,
        peer_id: &str,
        config: WebRtcTransportConfig,
    ) -> Result<PeerId> {
        // Try WebRTC with timeout
        let result = tokio::time::timeout(
            config.ice_timeout,
            self.connect_peer_internal(peer_id)
        ).await;

        match result {
            Ok(Ok(peer_id)) => Ok(peer_id),
            Ok(Err(e)) => {
                warn!("WebRTC connection failed: {}", e);

                // Fallback to gRPC or FFI transport
                if let Some(fallback) = &config.fallback_transport {
                    info!("Falling back to alternate transport");
                    // Implement fallback via delegation trait
                    fallback.connect(peer_id).await
                } else {
                    Err(e)
                }
            }
            Err(_) => Err("WebRTC connection timeout".into()),
        }
    }
}
```

#### Known Issues & Workarounds

| Issue | Impact | Workaround |
|-------|--------|-----------|
| RTCP not generated automatically | No Sender Reports (breaks NTP sync) | Manually send RTCP-RR packets every 5s |
| Media track loss under jitter | Audio/video dropouts in bad networks | Add application-level FEC (Forward Error Correction) |
| ICE gathering timeout edge case | Connections hang in some NAT | Always set explicit ice_timeout, not infinite |
| Memory not freed on rapid connect/disconnect | Potential memory leak in high-churn scenarios | Implement graceful shutdown with delay |

#### Alternatives Considered

| Alternative | Pros | Cons | Verdict |
|-------------|------|------|---------|
| **webrtc-rs (chosen)** | Pure Rust, full spec, active dev | Early-stage, some edge cases | Best for MVP |
| webrtc-sys (C++ bindings) | Battle-tested libwebrtc | Unsafe C++, large binary, slower build | For production fallback |
| just-webrtc | Modular, simpler API | Less complete, smaller community | Consider for v2 |
| Browser/WASM-only | Built-in WebRTC | Can't run headless/server-side | Not applicable |

---

## 3. Codec Integration

### Decision: Opus for Audio (mandatory), VP9 + H.264 for Video (fallback strategy)

#### Audio: Opus Codec (MANDATORY)

**Decision:** Use `opus` crate v0.3 (maintained by mozilla-services)

```toml
[dependencies]
opus = "0.3"
```

**Why Opus:**
- Mandatory in WebRTC spec
- 6-510 kbps bitrate range (adaptive)
- Supports 8-48 kHz sample rates (opus always decodes to 48kHz internally)
- Frame sizes: 2.5ms to 60ms (configurable)
- Zero licensing issues (royalty-free)

**Integration Pattern:**

```rust
use opus::{Encoder, Decoder};

pub struct AudioCodec {
    encoder: Encoder,
    decoder: Decoder,
    sample_rate: u32,
    channels: u16,
    bitrate: i32, // bps
}

impl AudioCodec {
    pub fn new(sample_rate: u32, channels: u16, bitrate_kbps: i32) -> Result<Self> {
        let encoder = Encoder::new(sample_rate, opus::Channels::Mono, opus::Application::Voip)?;
        encoder.set_bitrate(opus::Bitrate::Bits(bitrate_kbps * 1000))?;

        let decoder = Decoder::new(sample_rate, opus::Channels::Mono)?;

        Ok(Self {
            encoder,
            decoder,
            sample_rate,
            channels,
            bitrate: bitrate_kbps * 1000,
        })
    }

    pub fn encode(&mut self, pcm: &[f32]) -> Result<Vec<u8>> {
        // pcm expected: mono, 48kHz, 960 samples = 20ms frame
        let mut output = vec![0u8; 4000]; // Max opus frame ~4KB
        let len = self.encoder.encode_float(pcm, &mut output)?;
        output.truncate(len);
        Ok(output)
    }

    pub fn decode(&mut self, opus_data: &[u8], frame_size: usize) -> Result<Vec<f32>> {
        let mut pcm = vec![0f32; frame_size];
        self.decoder.decode_float(opus_data, &mut pcm, false)?;
        Ok(pcm)
    }

    pub fn set_bitrate(&mut self, kbps: i32) -> Result<()> {
        self.encoder.set_bitrate(opus::Bitrate::Bits(kbps * 1000))?;
        self.bitrate = kbps * 1000;
        Ok(())
    }
}

// Zero-copy integration with RemoteMedia audio buffers
impl AudioCodec {
    pub fn encode_runtime_data(&mut self, data: &RuntimeData) -> Result<Vec<u8>> {
        match data {
            RuntimeData::Audio { samples, .. } => {
                // Use remtomedia's audio buffer directly
                let float_samples = self.convert_to_f32(samples)?;
                self.encode(&float_samples)
            }
            _ => Err("Expected audio data".into()),
        }
    }
}
```

**Configuration Strategy:**

```rust
// Adaptive bitrate management
pub struct OpusAdaptiveConfig {
    // Start conservative, increase if quality good
    min_bitrate_kbps: i32, // 16 (low: voice only)
    nominal_bitrate_kbps: i32, // 32 (speech quality)
    max_bitrate_kbps: i32, // 128 (high fidelity)

    // Trigger adjustment when loss > threshold
    loss_threshold_percent: f32, // 5%
}

impl OpusAdaptiveConfig {
    pub fn adjust_for_loss(&self, loss_percent: f32) -> i32 {
        match loss_percent {
            x if x > 10.0 => self.min_bitrate_kbps,
            x if x > 5.0 => (self.nominal_bitrate_kbps * 0.7) as i32,
            x if x > 2.0 => self.nominal_bitrate_kbps,
            _ => std.max_bitrate_kbps,
        }
    }
}
```

**RTP Timestamp Handling for Opus:**

```rust
// Opus uses 48kHz reference clock (always, even if input is 8kHz/16kHz)
pub const OPUS_REFERENCE_CLOCK: u32 = 48_000;

impl AudioCodec {
    pub fn frame_rtp_increment(&self, frame_size_ms: u32) -> u32 {
        // frame_size_ms = 20 (typical)
        // increment = 20ms * 48kHz = 960 samples
        (frame_size_ms * OPUS_REFERENCE_CLOCK) / 1000
    }
}

// Example: 20ms frame at 48kHz
// Frame size: 960 samples
// RTP increment: 960
// If rtp_ts = 1000, next frame is 1960
```

---

### Video: VP9 + H.264 with Fallback Strategy

**Decision:** Implement VP9 as primary (better quality), H.264 as fallback (broader compatibility)

**Why VP9 + H.264:**
- VP9: Better compression, lower latency encoding, ~2-3x faster than original VP8
- H.264: Hardware support on older devices, guaranteed browser compatibility
- Fallback approach: Offer VP9 first, fall back to H.264 in SDP negotiation

#### VP9 Integration (Primary)

```toml
[dependencies]
# VP9 via libvpx
vpx = { version = "0.2", features = ["vp9"] }

# Fallback to wrapper
av1-grain = "0.2" # For grain extraction (optional)
```

**Challenges with VP9 in Rust:**

1. **vpx-sys limitations:**
   - Requires libvpx system library (C)
   - No safe bindings (must use unsafe)
   - Performance excellent but harder to integrate

2. **Recommended approach: wrap in safe abstraction**

```rust
use vpx::prelude::*;

pub struct Vp9Encoder {
    encoder: VpxEncoder,
    width: u32,
    height: u32,
    bitrate_kbps: u32,
}

impl Vp9Encoder {
    pub fn new(width: u32, height: u32, bitrate_kbps: u32) -> Result<Self> {
        let mut encoder = VpxEncoder::new(CodecIface::vp9(), &VpxCodecEncCfg {
            g_w: width,
            g_h: height,
            g_timebase: VpxRational { num: 1, den: 30 }, // 30fps
            rc_target_bitrate: bitrate_kbps as u32,
            ..Default::default()
        })?;

        // Set deadline (speed vs quality tradeoff)
        // VPX_DL_REALTIME = 1 (fastest, lowest latency)
        encoder.set_param(VP8E_SET_CPUUSED, 4)?; // 0-8, 4=balanced
        encoder.set_param(VP9E_SET_TILE_COLUMNS, 2)?; // Parallel encoding
        encoder.set_param(VP9E_SET_TILE_ROWS, 1)?;

        Ok(Self { encoder, width, height, bitrate_kbps })
    }

    pub fn encode(&mut self, frame: &VideoFrame) -> Result<EncodedFrame> {
        // frame: raw YUV420P or I420
        let image = Image {
            fmt: ImageFormat::I420,
            w: self.width as usize,
            h: self.height as usize,
            planes: &[
                &frame.y_plane,
                &frame.u_plane,
                &frame.v_plane,
            ],
        };

        self.encoder.encode(&image, 0)?;

        // Get output
        let mut iter = self.encoder.get_packet()?;
        if let Some(pkt) = iter.next() {
            Ok(EncodedFrame {
                data: pkt.data.to_vec(),
                is_keyframe: pkt.flags & VPX_FRAME_IS_KEY != 0,
                partition_id: pkt.partition_id,
            })
        } else {
            Err("No encoded frame".into())
        }
    }
}
```

#### H.264 Integration (Fallback)

```toml
[dependencies]
# H.264 via OpenH264
openh264 = "0.5"
```

```rust
use openh264::encoder::{Encoder, EncoderConfig};

pub struct H264Encoder {
    encoder: Encoder,
    width: u32,
    height: u32,
}

impl H264Encoder {
    pub fn new(width: u32, height: u32, bitrate_kbps: u32) -> Result<Self> {
        let config = EncoderConfig::default()
            .set_usage(openh264::encoder::Usage::SCREEN_CONTENT_REAL_TIME)?
            .set_bitrate_bps((bitrate_kbps * 1000) as u32)?;

        let mut encoder = Encoder::with_config(config)?;
        encoder.set_option(openh264::encoder::EncodeOption::ENTROPY_CODING_MODE_FLAG, 1)?;

        Ok(Self { encoder, width, height })
    }

    pub fn encode(&mut self, frame: &VideoFrame) -> Result<EncodedFrame> {
        let output = self.encoder.encode(&frame.data)?;

        Ok(EncodedFrame {
            data: output,
            is_keyframe: false, // OpenH264 handles internally
            partition_id: 0,
        })
    }
}
```

#### Video Codec Selection (SDP Negotiation)

```rust
pub enum VideoCodecPreference {
    VP9Only,
    H264Only,
    VP9PrimaryH264Fallback, // Recommended
}

fn create_video_codec_offer(pref: VideoCodecPreference) -> Vec<VideoCodec> {
    match pref {
        VideoCodecPreference::VP9PrimaryH264Fallback => vec![
            VideoCodec {
                name: "VP9".to_string(),
                payload_type: 96,
                clock_rate: 90_000,
                parameters: "profile-id=0".to_string(),
            },
            VideoCodec {
                name: "H264".to_string(),
                payload_type: 97,
                clock_rate: 90_000,
                parameters: "level-asymmetry-allowed=1;packetization-mode=1".to_string(),
            },
        ],
        _ => vec![], // Other cases...
    }
}
```

#### Zero-Copy Opportunities

Both Opus and video codecs can integrate with RemoteMedia zero-copy buffers:

```rust
// Audio: Use Arc<Vec<f32>> directly
impl AudioCodec {
    pub fn encode_zero_copy(&mut self, buffer: &Arc<Vec<f32>>) -> Result<Vec<u8>> {
        // No copy: just reference the Arc
        self.encode(buffer.as_slice())
    }
}

// Video: Use frame buffer without copy
impl VideoEncoder {
    pub fn encode_zero_copy(&mut self, frame: &Arc<VideoFrameBuffer>) -> Result<Vec<u8>> {
        // No copy: encoder works on Arc reference
        self.encode(frame.as_ref())
    }
}
```

---

## 4. Signaling Protocol Design

### Decision: JSON-RPC 2.0 over WebSocket (ONVIF standard implementation)

#### Rationale

JSON-RPC 2.0 is:
- **Standardized** (RFC, ONVIF WebRTC spec, industry adoption)
- **Simple** (easy to implement, debug, monitor)
- **Language-agnostic** (JavaScript clients, Python servers, Rust transport)
- **Battle-tested** (used in production WebRTC deployments)

#### Protocol Specification

**Transport:** WebSocket (ws:// or wss://)

**Message Format:**

```rust
#[derive(Serialize, Deserialize, Debug)]
pub struct JsonRpcMessage {
    /// MUST be "2.0"
    jsonrpc: String,

    /// Method name ("peer.announce", "peer.offer", etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    method: Option<String>,

    /// Method parameters (per method)
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<serde_json::Value>,

    /// Request ID for matching responses
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<serde_json::Value>,

    /// Response result (if response)
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,

    /// Response error (if error)
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonRpcError {
    /// Error code
    code: i32,
    /// Error message
    message: String,
    /// Optional context
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
}
```

**Error Codes (JSON-RPC standard + custom):**

```rust
pub mod errors {
    pub const PARSE_ERROR: i32 = -32700;
    pub const INVALID_REQUEST: i32 = -32600;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
    pub const INTERNAL_ERROR: i32 = -32603;

    // Custom for WebRTC
    pub const PEER_NOT_FOUND: i32 = -32000;
    pub const CONNECTION_FAILED: i32 = -32001;
    pub const OFFER_INVALID: i32 = -32002;
    pub const ANSWER_INVALID: i32 = -32003;
}
```

#### Signaling Flow

**Phase 1: Peer Discovery & Announcement**

```
Client A                           Signaling Server                    Client B
  |                                      |                                |
  +---- announce ----------------->      |                                |
  |    { peer_id, capabilities }         |                                |
  |                                      +---- broadcast_announce ------->|
  |                                      |    { peer_id, capabilities }   |
  |    { result: ack }                   |                                |
  |<---- (empty) -----+                  |                                |
```

**Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "peer.announce",
  "params": {
    "peer_id": "peer-abc123",
    "capabilities": ["audio", "video", "data"],
    "user_data": { "name": "Alice" }
  },
  "id": 1
}
```

**Response (to server):**
```json
{
  "jsonrpc": "2.0",
  "result": {
    "status": "registered",
    "registered_at": "2025-11-07T10:30:00Z"
  },
  "id": 1
}
```

**Broadcast (from server to other peers):**
```json
{
  "jsonrpc": "2.0",
  "method": "peer.announced",
  "params": {
    "peer_id": "peer-abc123",
    "capabilities": ["audio", "video", "data"],
    "user_data": { "name": "Alice" }
  }
}
```

---

**Phase 2: Offer/Answer Exchange with Trickle ICE**

**Trickle ICE Approach (Recommended):**
- Don't wait for ICE gathering complete
- Send SDP offer/answer immediately
- Stream ICE candidates incrementally as discovered
- Faster connection establishment: <500ms vs 2-5s

```
Peer A                             Peer B
  |                                  |
  +--- offer + canTrickleIceCandidates=true --->
  |                                  |
  |<--- answer + canTrickleIceCandidates=true ---+
  |                                  |
  +--- ice_candidate ------+         |
  +--- ice_candidate --+   |         |
  +--- ice_candidate --->  |         |
  |                    +----------> (reachability test)
  |                        |
  |<---- ice_candidate -----+
  |<---- ice_candidate ------+
  |                          |
  (connection established after first matching candidate pair)
```

**SDP Offer Request:**
```json
{
  "jsonrpc": "2.0",
  "method": "peer.offer",
  "params": {
    "from": "peer-abc123",
    "to": "peer-def456",
    "sdp": "v=0\r\no=- ...\r\n...",
    "can_trickle_ice_candidates": true,
    "request_id": "req-001"
  },
  "id": 2
}
```

**SDP Answer Response:**
```json
{
  "jsonrpc": "2.0",
  "result": {
    "sdp": "v=0\r\no=- ...\r\n...",
    "can_trickle_ice_candidates": true,
    "request_id": "req-001"
  },
  "id": 2
}
```

**ICE Candidate Trickle:**
```json
{
  "jsonrpc": "2.0",
  "method": "peer.ice_candidate",
  "params": {
    "from": "peer-abc123",
    "to": "peer-def456",
    "candidate": "candidate:123 1 UDP 2122260223 192.168.1.100 54321 typ host",
    "sdp_m_line_index": 0,
    "sdp_mid": "0",
    "user_fragment": "abc"
  }
}
```

---

**Phase 3: Connection State Management**

```json
{
  "jsonrpc": "2.0",
  "method": "peer.state_changed",
  "params": {
    "from": "peer-abc123",
    "to": "peer-def456",
    "connection_state": "connected",
    "ice_connection_state": "connected",
    "ice_gathering_state": "complete",
    "signaling_state": "stable"
  }
}
```

**State Values:**
- `connection_state`: new | connecting | connected | disconnected | failed | closed
- `ice_connection_state`: new | checking | connected | completed | failed | disconnected | closed
- `ice_gathering_state`: new | gathering | complete

---

**Phase 4: Disconnect**

```json
{
  "jsonrpc": "2.0",
  "method": "peer.disconnect",
  "params": {
    "from": "peer-abc123",
    "to": "peer-def456",
    "reason": "user_requested"
  }
}
```

#### Implementation Strategy

```rust
use tokio_tungstenite::connect_async;
use futures::{SinkExt, StreamExt};
use serde_json::json;

pub struct SignalingClient {
    ws_url: String,
    peer_id: String,
    outbound_tx: tokio::sync::mpsc::UnboundedSender<JsonRpcMessage>,
    request_pending: Arc<Mutex<HashMap<String, tokio::sync::oneshot::Sender<JsonRpcMessage>>>>,
}

impl SignalingClient {
    pub async fn connect(url: &str, peer_id: &str) -> Result<Self> {
        let (ws_stream, _) = connect_async(url).await?;
        let (write, mut read) = ws_stream.split();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let request_pending = Arc::new(Mutex::new(HashMap::new()));

        // Spawn write task
        let write_rx = rx;
        tokio::spawn(async move {
            let mut write = write;
            let mut write_rx = write_rx;
            while let Some(msg) = write_rx.recv().await {
                let json = serde_json::to_string(&msg).unwrap();
                write.send(Message::Text(json)).await.ok();
            }
        });

        // Spawn read task
        let request_pending_clone = request_pending.clone();
        tokio::spawn(async move {
            while let Some(msg) = read.next().await {
                if let Ok(Message::Text(json)) = msg {
                    if let Ok(msg) = serde_json::from_str::<JsonRpcMessage>(&json) {
                        if let Some(id) = &msg.id {
                            // Response to pending request
                            let id_str = format!("{:?}", id);
                            let pending = request_pending_clone.lock().unwrap();
                            if let Some(tx) = pending.get(&id_str) {
                                tx.send(msg).ok();
                            }
                        } else if let Some(method) = &msg.method {
                            // Notification - handle separately
                            handle_notification(method, msg).await;
                        }
                    }
                }
            }
        });

        Ok(Self {
            ws_url: url.to_string(),
            peer_id: peer_id.to_string(),
            outbound_tx: tx,
            request_pending,
        })
    }

    /// Send announce message
    pub async fn announce(&self, capabilities: &[&str]) -> Result<()> {
        let msg = json!({
            "jsonrpc": "2.0",
            "method": "peer.announce",
            "params": {
                "peer_id": self.peer_id,
                "capabilities": capabilities
            },
            "id": 1
        });

        self.outbound_tx.send(serde_json::from_value(msg)?)?;
        Ok(())
    }

    /// Send SDP offer (request/response pattern)
    pub async fn send_offer(
        &self,
        to_peer: &str,
        sdp: &str,
        can_trickle: bool,
    ) -> Result<String> {
        let id = format!("req-{}", uuid::Uuid::new_v4());
        let (tx, rx) = tokio::sync::oneshot::channel();

        self.request_pending.lock().unwrap().insert(id.clone(), tx);

        let msg = json!({
            "jsonrpc": "2.0",
            "method": "peer.offer",
            "params": {
                "from": self.peer_id,
                "to": to_peer,
                "sdp": sdp,
                "can_trickle_ice_candidates": can_trickle,
                "request_id": id
            },
            "id": &id
        });

        self.outbound_tx.send(serde_json::from_value(msg)?)?;

        // Wait for answer
        let response = tokio::time::timeout(Duration::from_secs(10), rx).await??;

        Ok(response.result
            .and_then(|r| r.get("sdp").and_then(|s| s.as_str()).map(|s| s.to_string()))
            .ok_or("No SDP in response")?)
    }

    /// Send ICE candidate (fire-and-forget)
    pub async fn send_ice_candidate(
        &self,
        to_peer: &str,
        candidate: &str,
        sdp_m_line_index: u32,
        sdp_mid: &str,
    ) -> Result<()> {
        let msg = json!({
            "jsonrpc": "2.0",
            "method": "peer.ice_candidate",
            "params": {
                "from": self.peer_id,
                "to": to_peer,
                "candidate": candidate,
                "sdp_m_line_index": sdp_m_line_index,
                "sdp_mid": sdp_mid
            }
        });

        self.outbound_tx.send(serde_json::from_value(msg)?)?;
        Ok(())
    }
}

/// Handle signaling notifications (not tied to specific request)
async fn handle_notification(method: &str, msg: JsonRpcMessage) {
    match method {
        "peer.announced" => {
            // New peer available
            if let Some(params) = msg.params {
                let peer_id = params["peer_id"].as_str();
                println!("Peer announced: {:?}", peer_id);
                // Initiate connection to this peer
            }
        }
        "peer.offer" => {
            // Incoming offer - respond with answer
            if let Some(params) = msg.params {
                let from = params["from"].as_str();
                let sdp = params["sdp"].as_str();
                println!("Received offer from {:?}", from);
                // Call create_answer, send back
            }
        }
        "peer.ice_candidate" => {
            // Incoming ICE candidate
            if let Some(params) = msg.params {
                let candidate = params["candidate"].as_str();
                println!("Received ICE candidate: {:?}", candidate);
                // Add to local peer connection
            }
        }
        _ => {}
    }
}
```

#### Signaling Server (Minimal Reference Implementation)

```rust
// Would be implemented as separate binary
// Can be in Go, Node.js, or Python for simplicity
// Must handle:
// 1. WebSocket connection per peer
// 2. Peer discovery (announce + broadcast)
// 3. Message routing (offer/answer forwarding)
// 4. ICE candidate forwarding
// 5. Connection state tracking

pub struct SignalingServer {
    peers: Arc<RwLock<HashMap<String, PeerSession>>>,
}

pub struct PeerSession {
    peer_id: String,
    tx: tokio::sync::mpsc::UnboundedSender<String>, // JSON messages
    capabilities: Vec<String>,
    connected_at: Instant,
}

// Server routes peer.offer -> recipient
// Server broadcasts peer.announced -> all other peers
// Server forwards peer.ice_candidate -> recipient
```

---

## 5. Zero-Copy Optimizations

### Decision: Use iceoryx2 for Peer-to-Pipeline Data, Direct Channels for Peer-to-Peer Media

#### Architecture

```
Peer A → WebRTC Media Track
          ↓ (RTP/SRTP)
        Decode (opus)
          ↓
    Audio/Video Buffer (Arc<>)
          ↓ (zero-copy reference)
    RemoteMedia Pipeline Input
          ↓ (processing)
    RemoteMedia Pipeline Output
          ↓ (zero-copy reference)
    iceoryx2 Publisher → Peer B
          ↓ (shared memory IPC)
        Peer B Subscriber (zero-copy)
          ↓
    Encode (opus)
          ↓
    WebRTC Media Track Output
          ↓
    Peer B
```

#### Decision: Use Arc-Based Buffers + iceoryx2 for Multi-Process

**Why this architecture:**

1. **WebRTC → Pipeline (in-process):** Direct Arc references (no copy)
   - WebRTC media decoder outputs Arc<AudioBuffer>
   - Pass directly to pipeline (same process)
   - No serialization needed

2. **Pipeline → WebRTC Media Track:** iceoryx2 for multiprocess
   - If pipeline node runs in Python process: use iceoryx2
   - Shared memory: zero-copy between Rust and Python
   - Eliminate 1-2ms copy overhead per frame

3. **Peer-to-Peer Media:** Direct WebRTC tracks (DTLS/SRTP encrypted)
   - No RemoteMedia involvement
   - Uses WebRTC's own media infrastructure
   - Lowest latency (<50ms possible)

#### Implementation

```rust
use std::sync::Arc;

/// Peer connection with zero-copy buffer management
pub struct PeerConnection {
    /// Peer ID
    peer_id: String,

    /// WebRTC peer (media tracks)
    rtc_peer: Arc<webrtc::peer_connection::RTCPeerConnection>,

    /// Audio input buffer (Arc for zero-copy)
    audio_input_buffer: Arc<Mutex<Arc<Vec<f32>>>>,

    /// Video input buffer (Arc for zero-copy)
    video_input_buffer: Arc<Mutex<Arc<Vec<u8>>>>,

    /// iceoryx2 publisher (for pipeline output to other peers)
    #[cfg(feature = "multiprocess")]
    ipc_publisher: Arc<iceoryx2::port::publisher::Publisher<'static>>,

    /// Sequence tracking
    last_audio_rtp: u32,
    last_video_rtp: u32,
}

impl PeerConnection {
    /// Receive audio from peer (WebRTC) and forward to pipeline
    pub async fn handle_incoming_audio(&self, track: &AudioTrack) -> Result<()> {
        let buffer = self.audio_input_buffer.clone();

        // WebRTC decoder outputs Arc<Vec<f32>>
        let audio_samples = track.read().await?; // Returns Arc<Vec<f32>>

        // Zero-copy: just swap Arc reference
        *buffer.lock().await = audio_samples;

        Ok(())
    }

    /// Send pipeline output to peer (WebRTC)
    pub async fn send_pipeline_output(&self, output: &RuntimeData) -> Result<()> {
        match output {
            RuntimeData::Audio { samples, .. } => {
                // Encode audio
                let encoded = self.encode_audio(samples)?;

                // Send via WebRTC track (no additional copy)
                self.rtc_peer.write_audio(&encoded).await?;
            }
            RuntimeData::Video { frame, .. } => {
                // Encode video
                let encoded = self.encode_video(frame)?;

                // Send via WebRTC track
                self.rtc_peer.write_video(&encoded).await?;
            }
            _ => {}
        }

        Ok(())
    }

    #[cfg(feature = "multiprocess")]
    pub async fn send_to_pipeline_process(&self, data: &RuntimeData) -> Result<()> {
        // Use iceoryx2 for multiprocess pipelines
        // Data published to shared memory, no copy to Python process

        let serialized = self.serialize_runtime_data(data)?;

        // Publisher loaned memory in shared space
        // Python process subscribes and reads zero-copy
        self.ipc_publisher.publish(serialized)?;

        Ok(())
    }
}

/// RemoteMedia integration with zero-copy buffers
pub struct MultiPeerPipeline {
    /// Pipeline runner (from runtime-core)
    runner: Arc<PipelineRunner>,

    /// Active peer connections
    peers: Arc<RwLock<HashMap<String, Arc<PeerConnection>>>>,

    /// Input buffer from all peers (merged)
    merged_input_rx: tokio::sync::mpsc::UnboundedReceiver<(String, Arc<Vec<f32>>)>,

    /// Output router (to all peers)
    output_routes: Arc<RwLock<HashMap<String, tokio::sync::mpsc::UnboundedSender<RuntimeData>>>>,
}

impl MultiPeerPipeline {
    pub async fn run_pipeline(&self) -> Result<()> {
        loop {
            // Receive from any peer (zero-copy Arc reference)
            if let Some((peer_id, audio_buffer)) = self.merged_input_rx.recv().await {
                // Convert Arc<Vec<f32>> to RemoteMedia RuntimeData
                let runtime_data = RuntimeData::Audio {
                    samples: Arc::new(audio_buffer.as_ref().clone()), // ISSUE: This copies
                    sample_rate: 48000,
                    channels: 1,
                };

                // TODO: Optimize to avoid clone here
                // Could use Arc<dyn AsRef<[f32]>> but type system gets complex

                // Execute pipeline
                let output = self.runner.execute_unary(
                    Arc::new(self.get_manifest()?),
                    TransportData::new(runtime_data),
                ).await?;

                // Route output to target peers (zero-copy)
                if let Ok(peers) = self.peers.read() {
                    for (target_id, peer_conn) in peers.iter() {
                        if target_id != &peer_id {
                            peer_conn.send_pipeline_output(&output.data).await?;
                        }
                    }
                }
            }
        }
    }
}
```

#### iceoryx2 Integration for Multiprocess

```rust
#[cfg(feature = "multiprocess")]
pub mod ipc {
    use iceoryx2::prelude::*;

    /// Serialize RuntimeData for iceoryx2 transmission
    pub fn serialize_for_ipc(data: &RuntimeData) -> Result<Vec<u8>> {
        // Custom binary format (not protobuf, not JSON - minimal overhead)
        match data {
            RuntimeData::Audio { samples, sample_rate, channels } => {
                let mut buf = Vec::new();
                // Type tag (1 byte)
                buf.push(1u8); // Audio
                // Metadata (8 bytes)
                buf.extend_from_slice(&(*sample_rate).to_le_bytes());
                buf.push(*channels as u8);
                buf.extend_from_slice(&(samples.len() as u32).to_le_bytes());
                // Samples (no copy - just reference)
                for sample in samples.iter() {
                    buf.extend_from_slice(&sample.to_le_bytes());
                }
                Ok(buf)
            }
            _ => todo!(),
        }
    }

    /// Integration with RemoteMedia multiprocess executor
    pub async fn forward_to_python_process(
        publisher: &Publisher,
        session_id: &str,
        node_id: &str,
        data: &RuntimeData,
    ) -> Result<()> {
        let serialized = serialize_for_ipc(data)?;

        // Loan memory from iceoryx2 (zero-copy)
        // Python process subscribes and reads directly
        publisher.publish(serialized)?;

        // Python process receives in node.py via:
        // subscriber.receive() -> buffer (zero-copy reference)

        Ok(())
    }
}
```

**Benefits:**
- Peer → RemoteMedia: Arc reference (zero-copy)
- RemoteMedia → Python process: iceoryx2 shared memory (zero-copy)
- Python → Peer: Encode locally, send via WebRTC

**Remaining Copy Points (necessary):**
1. WebRTC codec encode/decode (fundamental - no way around)
2. RuntimeData enum boxing (Arc<Vec<>> must be cloned into enum)
3. Cross-process boundary for multiprocess (unavoidable with iceoryx2, but still fast)

---

## 6. Implementation Roadmap & Risks

### Phase 1: Signaling Foundation (Week 1)
- [ ] JSON-RPC 2.0 protocol implementation
- [ ] WebSocket signaling client
- [ ] SDP offer/answer exchange
- [ ] ICE candidate trickle
- [ ] Minimal signaling server reference

**Risks:**
- WebSocket connection stability
- Error handling for malformed SDP
- **Mitigation:** Comprehensive protocol tests, timeout handling

### Phase 2: WebRTC Media (Week 2)
- [ ] webrtc-rs peer connection setup
- [ ] Audio track (Opus encode/decode)
- [ ] Video track (VP9 + H.264)
- [ ] DTLS/SRTP encryption
- [ ] Connection state machine

**Risks:**
- webrtc-rs stability with media tracks
- Codec integration complexity
- **Mitigation:** Start with audio-only (simpler), add video later

### Phase 3: Synchronization (Week 3)
- [ ] RTP timestamp tracking
- [ ] Jitter buffer implementation
- [ ] Clock drift monitoring
- [ ] RTCP Sender Report generation
- [ ] Audio/video lip-sync

**Risks:**
- Clock drift edge cases
- Buffer underrun handling
- **Mitigation:** Extensive testing with intentional jitter

### Phase 4: Pipeline Integration (Week 4)
- [ ] Connect peer input → PipelineRunner
- [ ] Route pipeline output → peer WebRTC track
- [ ] iceoryx2 integration for multiprocess
- [ ] Per-peer session management
- [ ] Broadcast/unicast routing

**Risks:**
- Multiprocess IPC latency
- Backpressure handling
- **Mitigation:** Benchmark with actual pipelines

### Phase 5: Production Hardening (Week 5)
- [ ] Error recovery
- [ ] Connection quality monitoring
- [ ] Adaptive bitrate for degraded networks
- [ ] Comprehensive testing
- [ ] Documentation

**Risks:**
- Long-tail failures in edge cases
- **Mitigation:** Chaos engineering tests, real-world deployments

---

## Summary of Key Technical Decisions

| Area | Decision | Rationale | Alternatives |
|------|----------|-----------|--------------|
| **Audio/Video Sync** | Explicit RTP + jitter buffers per peer | WebRTC doesn't auto-sync, multi-peer requires explicit | Ignore timestamps (fails), media server (not mesh) |
| **WebRTC Crate** | webrtc-rs v0.9 with fallback strategy | Only pure-Rust full implementation, accept early-stage | webrtc-sys (unsafe C++), just-webrtc (incomplete) |
| **Audio Codec** | Opus (mandatory) | WebRTC spec requires, 6-510kbps adaptive | None (mandatory) |
| **Video Codec** | VP9 primary + H.264 fallback | VP9 better quality, H.264 for compatibility | VP8 only, AV1 (overkill) |
| **Signaling** | JSON-RPC 2.0 over WebSocket | ONVIF standard, simple, debuggable | Custom binary (harder), gRPC (wrong layer) |
| **Zero-Copy** | Arc buffers in-process + iceoryx2 multiprocess | Eliminate codec/copy overhead, proven in codebase | Full memcpy (slow), shared memory (unsafe) |

---

## References & Further Reading

1. **WebRTC Specification**
   - https://w3c.github.io/webrtc-pc/
   - RFC 8829 (JavaScript Session Establishment Protocol - JSEP)

2. **RTP/RTCP Specifications**
   - RFC 7587: RTP Payload Format for Opus
   - RFC 3550: RTP (core spec)
   - RFC 3551: RTP A/V Profile

3. **Audio Synchronization**
   - https://www.nearstream.us/blog/how-to-sync-audio-and-video
   - RTP Sender Reports for NTP synchronization

4. **Rust WebRTC**
   - https://github.com/webrtc-rs/webrtc
   - https://webrtc.rs/ (documentation)

5. **iceoryx2**
   - https://github.com/eclipse-iceoryx/iceoryx2
   - Zero-copy IPC patterns

6. **RemoteMedia SDK**
   - See `CLAUDE.md` for session router patterns
   - `runtime-core/src/transport/` for abstraction layer
   - `transports/grpc/` for streaming reference

---

## Next Steps

1. **Prototype Signaling** (Week 1): Implement JSON-RPC 2.0 WebSocket client
2. **Validate webrtc-rs**: Test basic peer connection with audio track
3. **Build Sync Layer**: Implement RTP-based synchronization
4. **Integrate Pipeline**: Connect to PipelineRunner
5. **Production Hardening**: Error recovery, testing, monitoring

This research document provides the foundation for production-ready WebRTC multi-peer transport implementation in RemoteMedia SDK.
