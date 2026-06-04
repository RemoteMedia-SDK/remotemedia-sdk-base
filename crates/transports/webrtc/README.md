# RemoteMedia WebRTC Transport

**Status**: ✅ Feature Complete (Phases 1-8) - 145 tests passing

A WebRTC-based real-time media streaming transport for RemoteMedia pipelines with multi-peer mesh networking support.

## Current Status

**Phases 1-8 Complete** (v0.4.0):
- ✅ Configuration and error handling
- ✅ JSON-RPC 2.0 signaling protocol (WebSocket)
- ✅ WebRTC peer connection management (webrtc-rs v0.14.0)
- ✅ Media track support (Opus audio, VP9 video)
- ✅ Session management and peer associations
- ✅ Send/broadcast API for audio/video
- ✅ A/V synchronization with jitter buffers and clock drift estimation
- ✅ Data channel communication (reliable/unreliable modes)
- ✅ Automatic reconnection with circuit breaker and quality monitoring

**What's Working**:
- Real WebRTC peer connections (not placeholders)
- Multi-peer mesh topology (configurable max peers)
- Audio/video track creation and RTP transmission
- Session lifecycle management with state snapshots
- Data channels with JSON/Binary/Text message types
- Automatic reconnection with exponential backoff and jitter
- Circuit breaker pattern for connection quality management
- Connection quality metrics (latency, packet loss, jitter)
- Configuration presets (low latency, high quality, mobile network)
- Comprehensive test coverage (145 tests)

**What's Next**:
- Production deployment examples
- Performance optimization
- Additional example applications

## Features

### Core Capabilities
- **Multi-peer mesh topology**: Configurable max peers (default: 10)
- **Real WebRTC**: Uses webrtc-rs v0.14.0 (Pure Rust implementation)
- **Media codecs**: Opus audio (48kHz), VP9/VP8/H.264 video
- **Session management**: Track streaming sessions with peer associations
- **JSON-RPC 2.0 signaling**: WebSocket-based peer discovery and SDP exchange
- **Async/await**: Built on Tokio runtime

### Audio/Video Synchronization
- **Per-peer sync managers**: Each peer has dedicated sync state
- **Jitter buffers**: Configurable 50-200ms for audio and video
- **Clock drift estimation**: Linear regression-based drift detection
- **RTCP Sender Reports**: NTP-to-RTP timestamp correlation

### Data Channels
- **Reliable mode**: Ordered, guaranteed delivery (TCP-like)
- **Unreliable mode**: Unordered, no retransmits (UDP-like)
- **Message types**: JSON, Binary (base64), Text
- **Control messages**: Pipeline start/stop/pause via data channel

### Reconnection & Quality
- **Automatic reconnection**: Exponential backoff with jitter
- **Circuit breaker pattern**: Closed/Open/HalfOpen states
- **Connection quality metrics**: Latency, packet loss, jitter, bandwidth
- **Adaptive bitrate**: Quality-based bitrate adjustment
- **Session state snapshots**: Persist and restore sessions

### Configuration Presets
- **Low latency preset**: 50ms jitter buffer, 2s RTCP, unreliable data channel
- **High quality preset**: 100ms jitter buffer, 4Mbps bitrate, 1080p video
- **Mobile network preset**: 150ms jitter buffer, 800kbps bitrate, TURN servers

## Architecture

```
┌────────────────────────────────────────────────────────┐
│  WebRTC Peers (Browser/Native)                         │
│  ↓ (WebRTC peer connections - mesh topology)           │
│  WebRtcTransport                                       │
│  ├─ SignalingClient (JSON-RPC 2.0 over WebSocket)     │
│  ├─ PeerManager (manages peer connections)            │
│  │   └─ PeerConnection (webrtc-rs)                    │
│  │       ├─ AudioTrack (Opus encoding)                │
│  │       ├─ VideoTrack (VP9/VP8/H.264 encoding)       │
│  │       ├─ DataChannel (reliable/unreliable)         │
│  │       ├─ ReconnectionManager (circuit breaker)     │
│  │       └─ ConnectionQualityMetrics                  │
│  ├─ SessionManager (pipeline session lifecycle)       │
│  │   └─ SessionStateSnapshot (persistence)            │
│  └─ SessionRouter (peers ↔ pipeline data routing)    │
│     ├─ SyncManager (per-peer A/V sync)               │
│     │   ├─ JitterBuffer (audio + video)              │
│     │   └─ ClockDriftEstimator                       │
│     └─ ReconnectionEventHandler                       │
│     ↓                                                   │
│  remotemedia-runtime-core::PipelineRunner              │
└────────────────────────────────────────────────────────┘
```

## Quick Start

### Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
remotemedia-webrtc = { path = "transports/webrtc" }
tokio = { version = "1.35", features = ["full"] }
```

### Basic Usage

```rust
use remotemedia_webrtc::{
    WebRtcTransport, WebRtcTransportConfig,
    media::audio::AudioEncoderConfig,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure transport
    let config = WebRtcTransportConfig {
        signaling_url: "ws://localhost:8080".to_string(),
        stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
        max_peers: 10,
        ..Default::default()
    };

    // Create and start transport
    let transport = WebRtcTransport::new(config)?;
    transport.start().await?;

    // Create a session
    let session = transport.create_session("session-1".to_string()).await?;

    // Connect to a peer
    let peer_id = transport.connect_peer("peer-remote".to_string()).await?;

    // Associate peer with session
    transport.add_peer_to_session("session-1", peer_id.clone()).await?;

    // Add audio track
    let peer = transport.peer_manager.get_peer(&peer_id).await?;
    peer.add_audio_track(AudioEncoderConfig::default()).await?;

    // Send audio
    let samples = Arc::new(vec![0.0f32; 960]); // 20ms @ 48kHz
    transport.send_audio(&peer_id, samples).await?;

    // Cleanup
    transport.shutdown().await?;

    Ok(())
}
```

## Building

### Requirements

- **Rust**: 1.87 or later (required for webrtc-rs v0.14.0)
- **Tokio**: Async runtime
- **Optional**: CMake (for codec feature flag)

### Build Commands

```bash
# Build the library
cd transports/webrtc
cargo build --release

# Run tests
cargo test

# Build with codec support (requires CMake)
cargo build --release --features codecs

# Build with all features
cargo build --release --features full
```

### Feature Flags

- `codecs` - Enable Opus/VP9 codecs (requires CMake)
- `h264` - Enable H.264 codec (requires native libraries)
- `full` - Enable all features

## API Reference

### Transport Configuration

```rust
use remotemedia_webrtc::WebRtcTransportConfig;

let config = WebRtcTransportConfig {
    signaling_url: "ws://localhost:8080".to_string(),
    stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
    turn_servers: vec![],
    max_peers: 10,
    audio_codec: AudioCodec::Opus,
    video_codec: VideoCodec::VP9,
    enable_data_channel: true,
    jitter_buffer_size_ms: 100,
    ..Default::default()
};

// Validate configuration
config.validate()?;
```

### Session Management

```rust
// Create session
let session = transport.create_session("session-id".to_string()).await?;

// Get session
let session = transport.get_session("session-id").await?;

// Check if session exists
if transport.has_session("session-id").await {
    // ...
}

// List all sessions
let session_ids = transport.list_sessions().await;

// Remove session
transport.remove_session("session-id").await?;
```

### Peer Management

```rust
// Connect to peer (initiates WebRTC connection)
let peer_id = transport.connect_peer("peer-remote".to_string()).await?;

// Disconnect from peer
transport.disconnect_peer(&peer_id).await?;

// List connected peers
let peers = transport.list_peers().await;

// List all peers (any state)
let all_peers = transport.list_all_peers().await;
```

### Media Streaming

```rust
use remotemedia_webrtc::media::{
    audio::AudioEncoderConfig,
    video::{VideoEncoderConfig, VideoFrame, VideoFormat},
};

// Add audio track to peer
let peer = transport.peer_manager.get_peer(&peer_id).await?;
let audio_config = AudioEncoderConfig {
    sample_rate: 48000,
    channels: 1,
    bitrate: 64000,
    complexity: 10,
};
peer.add_audio_track(audio_config).await?;

// Send audio to specific peer
let samples = Arc::new(vec![0.0f32; 960]); // 20ms @ 48kHz
transport.send_audio(&peer_id, samples.clone()).await?;

// Broadcast audio to all peers
transport.broadcast_audio(samples, None).await?;

// Add video track
let video_config = VideoEncoderConfig {
    width: 1280,
    height: 720,
    framerate: 30,
    bitrate: 2_000_000,
    keyframe_interval: 60,
};
peer.add_video_track(video_config).await?;

// Send video frame
let frame = VideoFrame {
    width: 1280,
    height: 720,
    format: VideoFormat::I420,
    data: vec![0u8; 1280 * 720 * 3 / 2],
    timestamp_us: 1000000,
    is_keyframe: true,
};
transport.send_video(&peer_id, &frame).await?;
```

### Configuration Presets

```rust
use remotemedia_webrtc::{WebRtcTransportConfig, TurnServerConfig};

// Low latency preset (real-time audio/video)
let config = WebRtcTransportConfig::low_latency_preset("ws://localhost:8080")
    .with_peer_id("my-peer")
    .with_max_peers(5);

// High quality preset (recording/broadcasting)
let config = WebRtcTransportConfig::high_quality_preset("ws://localhost:8080");

// Mobile network preset (unreliable connections)
let config = WebRtcTransportConfig::mobile_network_preset("ws://localhost:8080")
    .with_turn_servers(vec![
        TurnServerConfig {
            url: "turn:turn.example.com:3478".to_string(),
            username: "user".to_string(),
            credential: "pass".to_string(),
        }
    ]);
```

### Data Channels

```rust
use remotemedia_webrtc::channels::{DataChannel, DataChannelMessage, ControlMessage};
use serde_json::json;

// Create data channel on peer connection
let channel = peer.create_data_channel("control", DataChannelMode::Reliable).await?;

// Send JSON message
let msg = DataChannelMessage::Json(json!({ "type": "ping", "timestamp": 12345 }));
channel.send(&msg).await?;

// Send binary message
let binary = DataChannelMessage::Binary(vec![0x01, 0x02, 0x03]);
channel.send(&binary).await?;

// Send control message (pipeline control)
let control = ControlMessage::PipelineStart {
    session_id: "session-1".to_string(),
    manifest: None,
};
channel.send_control(&control).await?;

// Receive messages (with callback)
channel.on_message(|msg| async move {
    match msg {
        DataChannelMessage::Json(value) => println!("JSON: {}", value),
        DataChannelMessage::Binary(data) => println!("Binary: {} bytes", data.len()),
        DataChannelMessage::Text(text) => println!("Text: {}", text),
    }
}).await;
```

### Reconnection & Quality Monitoring

```rust
use remotemedia_webrtc::peer::lifecycle::{ReconnectionPolicy, ConnectionQualityMetrics};

// Configure reconnection policy
let policy = ReconnectionPolicy {
    max_retries: 5,
    backoff_initial_ms: 1000,
    backoff_max_ms: 30000,
    backoff_multiplier: 2.0,
    jitter_enabled: true,
};

// Get connection quality metrics
let metrics: ConnectionQualityMetrics = peer.get_metrics().await;
println!("Latency: {}ms", metrics.latency_ms);
println!("Packet loss: {:.2}%", metrics.packet_loss_rate * 100.0);
println!("Jitter: {}ms", metrics.jitter_ms);
println!("Quality score: {}/100", metrics.quality_score);

// Manual reconnection (ICE restart)
peer.reconnect().await?;

// Handle reconnection events in session router
router.on_reconnection(|event| async move {
    println!("Peer {} reconnected (full: {})",
             event.peer_id, event.is_full_reconnect);
    if let Some(duration) = event.previous_duration {
        println!("Previous connection lasted: {:?}", duration);
    }
}).await;
```

### Session State Snapshots

```rust
use remotemedia_webrtc::session::SessionStateSnapshot;

// Create snapshot for persistence
let snapshot: SessionStateSnapshot = router.create_snapshot().await;

// Store snapshot (e.g., in Redis/database)
let json = serde_json::to_string(&snapshot)?;

// Later: restore from snapshot
let restored: SessionStateSnapshot = serde_json::from_str(&json)?;
router.restore_from_snapshot(restored).await?;
```

## Testing

```bash
# Run all tests
cargo test

# Run specific test module
cargo test session
cargo test peer
cargo test transport

# Run with output
cargo test -- --nocapture

# Run tests with codec feature
cargo test --features codecs
```

**Test Coverage**: 145 tests passing
- Config validation: 11 tests (includes presets)
- Error handling: 5 tests
- Media codecs: 11 tests
- Peer management: 8 tests
- Session management: 11 tests
- Signaling protocol: 6 tests
- Transport integration: 10 tests
- A/V synchronization: 35 tests (jitter buffer, clock drift, sync manager)
- Data channels: 15 tests (messages, send/receive, modes)
- Reconnection: 18 tests (policy, circuit breaker, quality metrics)
- Core functionality: 15 tests

## Implementation Status

| Phase | Status | Description | Tests |
|-------|--------|-------------|-------|
| **Phase 1** | ✅ Complete | Crate structure, config, error types | 13 |
| **Phase 2** | ✅ Complete | Signaling protocol & peer connections | 39 |
| **Phase 3** | ✅ Complete | Media tracks, codecs (Opus/VP9) | 51 |
| **Phase 4** | ✅ Complete | Session management | 62 |
| **Phase 5** | ✅ Complete | Transport-session integration | 68 |
| **Phase 6** | ✅ Complete | A/V sync, jitter buffers, clock drift | 126 |
| **Phase 7** | ✅ Complete | Data channel communication | 134 |
| **Phase 8** | ✅ Complete | Reconnection & failover | 145 |
| **Phase 9** | 🚧 In Progress | Documentation, examples, polish | - |

See [DESIGN.md](DESIGN.md) for detailed design documentation and [specs/](../../specs/001-webrtc-multi-peer-transport/) for implementation specifications.

## Examples

### Point-to-Point Audio Streaming

```rust
use remotemedia_webrtc::{WebRtcTransport, WebRtcTransportConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create transport
    let config = WebRtcTransportConfig::default();
    let transport = WebRtcTransport::new(config)?;
    transport.start().await?;

    // Create session and connect peer
    let session = transport.create_session("audio-session".to_string()).await?;
    let peer_id = transport.connect_peer("peer-1".to_string()).await?;
    transport.add_peer_to_session("audio-session", peer_id.clone()).await?;

    // Setup audio track
    let peer = transport.peer_manager.get_peer(&peer_id).await?;
    let audio_config = AudioEncoderConfig::default();
    peer.add_audio_track(audio_config).await?;

    // Stream audio
    loop {
        let samples = generate_audio_samples(); // Your audio source
        transport.send_audio(&peer_id, Arc::new(samples)).await?;
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
    }
}
```

### Multi-Peer Broadcasting

```rust
use remotemedia_webrtc::{WebRtcTransport, WebRtcTransportConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = WebRtcTransportConfig::default();
    let transport = WebRtcTransport::new(config)?;
    transport.start().await?;

    // Connect multiple peers
    let peer1 = transport.connect_peer("peer-1".to_string()).await?;
    let peer2 = transport.connect_peer("peer-2".to_string()).await?;
    let peer3 = transport.connect_peer("peer-3".to_string()).await?;

    // Setup audio tracks for all peers
    for peer_id in &[&peer1, &peer2, &peer3] {
        let peer = transport.peer_manager.get_peer(peer_id).await?;
        peer.add_audio_track(AudioEncoderConfig::default()).await?;
    }

    // Broadcast audio to all peers
    let samples = Arc::new(vec![0.0f32; 960]);
    transport.broadcast_audio(samples, None).await?;

    Ok(())
}
```

## Signaling Server

The WebRTC transport requires an external signaling server implementing JSON-RPC 2.0 over WebSocket.

**Required Methods**:
- `announce` - Announce peer with capabilities
- `offer` - Send SDP offer to peer
- `answer` - Send SDP answer to peer
- `ice_candidate` - Exchange ICE candidates
- `disconnect` - Notify peer disconnection

**Example signaling server**: A complete Node.js implementation is provided in [examples/signaling_server/](examples/signaling_server/).

**Quick Start:**
```bash
cd examples/signaling_server
npm install
npm start  # Starts on ws://localhost:8080
```

See [examples/signaling_server/README.md](examples/signaling_server/README.md) for full protocol specification and deployment instructions.

## Running the WebRTC Server

A standalone server executable is provided in `crates/services/webrtc-server/`:

```bash
# Run with default configuration (WebSocket mode)
cargo run -p remotemedia-webrtc-server

# Run in gRPC signaling mode
cargo run -p remotemedia-webrtc-server -- --mode grpc --grpc-address 0.0.0.0:50051

# Or with custom configuration
WEBRTC_SIGNALING_URL="ws://signaling.example.com" \
WEBRTC_MAX_PEERS=20 \
cargo run -p remotemedia-webrtc-server
```

**Environment Variables:**
- `WEBRTC_SIGNALING_URL` - Signaling server URL (default: `ws://localhost:8080`)
- `WEBRTC_MAX_PEERS` - Maximum concurrent peers (default: `10`)
- `WEBRTC_STUN_SERVERS` - Comma-separated STUN servers
- `WEBRTC_TURN_SERVERS` - Comma-separated TURN servers (format: `turn:host:port:username:credential`)
- `WEBRTC_ENABLE_DATA_CHANNEL` - Enable data channels (default: `true`)
- `WEBRTC_JITTER_BUFFER_MS` - Jitter buffer size in milliseconds (default: `100`)
- `RUST_LOG` - Logging level (default: `info`)

For detailed deployment and integration instructions, see [INTEGRATION.md](INTEGRATION.md).

## gRPC Signaling (Alternative to WebSocket)

The WebRTC transport now supports **gRPC bidirectional streaming** as an alternative to WebSocket JSON-RPC 2.0 signaling. This provides:
- Type-safe protocol with Protocol Buffers
- Built-in signaling server (no separate server needed)
- Automatic server-side peer creation with pipeline integration
- Real SDP answer generation

### Building with gRPC Signaling

```bash
# The server binary is in crates/services/webrtc-server/
# It includes grpc-signaling by default
cargo build -p remotemedia-webrtc-server --release
```

### Running with gRPC Signaling

```bash
# Basic gRPC signaling server (port 50051)
cargo run -p remotemedia-webrtc-server --release -- \
  --mode grpc \
  --grpc-address 0.0.0.0:50051 \
  --manifest ./examples/loopback.yaml

# With STUN/TURN servers
cargo run -p remotemedia-webrtc-server --release -- \
  --mode grpc \
  --grpc-address 0.0.0.0:50051 \
  --stun-servers stun:stun.l.google.com:19302 \
  --manifest ./manifests/vad.yaml

# Full configuration with environment variables
WEBRTC_MAX_PEERS=20 \
WEBRTC_JITTER_BUFFER_MS=100 \
RUST_LOG=info \
cargo run -p remotemedia-webrtc-server --release -- \
  --mode grpc \
  --grpc-address 0.0.0.0:50051 \
  --manifest ./manifests/audio_processing.yaml
```

### gRPC Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `WEBRTC_ENABLE_GRPC_SIGNALING` | `false` | Enable gRPC signaling server |
| `GRPC_SIGNALING_ADDRESS` | `0.0.0.0:50051` | gRPC server bind address |
| `WEBRTC_PIPELINE_MANIFEST` | (required) | Path to pipeline manifest YAML/JSON |

### Architecture: gRPC vs WebSocket

#### WebSocket Signaling (Default)

```
┌─────────────────────────────────────────────────┐
│  Browser Client                                 │
│  ↓ (WebSocket JSON-RPC 2.0)                    │
│  External Signaling Server (Node.js/Python)    │
│  ↓ (SDP/ICE relay)                              │
│  WebRTC P2P Connections (mesh)                 │
│  ↓                                               │
│  WebRtcTransport + PipelineRunner (optional)   │
└─────────────────────────────────────────────────┘
```

**Pros**: Browser-native, no extra dependencies
**Cons**: Requires external signaling server, manual peer setup

#### gRPC Signaling (Optional)

```
┌─────────────────────────────────────────────────┐
│  Next.js Client (gRPC-Web)                     │
│  ↓ (gRPC bidirectional stream over HTTP/2)     │
│  WebRtcSignalingService (built-in, port 50051) │
│    │                                             │
│    ├─ Auto-spawn ServerPeer on client announce  │
│    │   └─ WebRTC + PipelineRunner + Session     │
│    │                                             │
│    ├─ Real SDP answer generation                │
│    └─ Media: Client ↔ WebRTC ↔ Pipeline ↔ Client │
└─────────────────────────────────────────────────┘
```

**Pros**: Built-in server, auto-peer creation, type-safe, pipeline integration
**Cons**: Requires `grpc-signaling` feature, gRPC-Web for browsers

### Comparison

| Feature | WebSocket (JSON-RPC 2.0) | gRPC (Protobuf) |
|---------|--------------------------|-----------------|
| **Protocol** | WebSocket over TCP | HTTP/2 |
| **Encoding** | JSON (text) | Protobuf (binary) |
| **Type Safety** | Runtime validation | Compile-time |
| **Browser Support** | Native | Via gRPC-Web |
| **Server** | Separate (Node.js/Python) | Built-in |
| **Auto-Peer Creation** | Manual | Automatic (ServerPeer) |
| **Pipeline Integration** | Manual | Automatic |
| **SDP Answers** | Relay from other peer | Real from WebRTC |
| **Use Case** | Browser P2P | Server-processed media |

### Client Example (Next.js with gRPC-Web)

```typescript
import { WebRtcSignalingClient } from './generated/remotemedia/v1/webrtc';

// Create gRPC client (gRPC-Web for browsers)
const client = new WebRtcSignalingClient('http://localhost:50051', {
  transport: 'grpc-web'
});

// Open bidirectional stream
const stream = client.signal();

// Announce peer
stream.write({
  requestId: '1',
  announce: {
    peerId: 'browser-client-123',
    capabilities: { audio: true, video: true, data: false }
  }
});

// Create WebRTC peer connection
const pc = new RTCPeerConnection();

// Add local media
const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
stream.getTracks().forEach(track => pc.addTrack(track, stream));

// Create and send offer
const offer = await pc.createOffer();
await pc.setLocalDescription(offer);

stream.write({
  requestId: '2',
  offer: {
    toPeerId: 'remotemedia-server',
    sdp: offer.sdp,
    type: 'offer'
  }
});

// Listen for server's answer
stream.on('data', async (response) => {
  if (response.notification?.answer) {
    await pc.setRemoteDescription({
      type: 'answer',
      sdp: response.notification.answer.sdp
    });
  }

  if (response.notification?.iceCandidate) {
    await pc.addIceCandidate(new RTCIceCandidate({
      candidate: response.notification.iceCandidate.candidate,
      sdpMid: response.notification.iceCandidate.sdpMid,
      sdpMLineIndex: response.notification.iceCandidate.sdpMlineIndex
    }));
  }
});

// Send ICE candidates to server
pc.onicecandidate = ({ candidate }) => {
  if (candidate) {
    stream.write({
      requestId: String(Date.now()),
      iceCandidate: {
        toPeerId: 'remotemedia-server',
        candidate: candidate.candidate,
        sdpMid: candidate.sdpMid,
        sdpMLineIndex: candidate.sdpMLineIndex
      }
    });
  }
};
```

### Pipeline Manifests for gRPC Mode

When using gRPC signaling, you must provide a pipeline manifest. The ServerPeer will process all media through this pipeline.

**Example: Audio Loopback** (`examples/loopback.yaml`):
```yaml
nodes:
  - id: input
    node_type: Input

  - id: output
    node_type: Output

connections:
  - from: input
    to: output
```

**Example: Voice Activity Detection** (`manifests/vad.yaml`):
```yaml
nodes:
  - id: input
    node_type: Input

  - id: vad
    node_type: VoiceActivityDetection
    params:
      threshold: 0.5
      min_speech_duration_ms: 300

  - id: output
    node_type: Output

connections:
  - from: input
    to: vad
  - from: vad
    to: output
```

### Protocol Buffers Definition

The gRPC protocol is defined in [`protos/webrtc_signaling.proto`](protos/webrtc_signaling.proto).

**Key Messages**:
- `SignalingRequest` - Client → Server (announce, offer, answer, ICE candidate)
- `SignalingResponse` - Server → Client (ack, peer list, notifications)
- `AnnounceRequest` - Register peer with capabilities
- `OfferRequest` - Send SDP offer
- `AnswerNotification` - Receive real SDP answer from ServerPeer
- `IceCandidateRequest/Notification` - Bidirectional ICE exchange

### When to Use Each Signaling Method

**Use WebSocket (JSON-RPC 2.0) when**:
- Building browser-to-browser P2P applications
- Need native browser support without extra libraries
- Want simple peer-to-peer connections
- Don't need server-side media processing

**Use gRPC (Protobuf) when**:
- Need server-side media processing through pipelines
- Want type-safe protocol with compile-time validation
- Building Next.js or other gRPC-enabled clients
- Need automatic server peer creation
- Want built-in signaling server (no separate deployment)

### Troubleshooting gRPC Signaling

**Issue**: `Error: 14 UNAVAILABLE: failed to connect to all addresses`

**Solutions**:
1. Ensure server was built with `--features grpc-signaling`
2. Check `GRPC_SIGNALING_ADDRESS` is correct
3. Verify port 50051 is not in use: `netstat -an | findstr 50051` (Windows) or `lsof -i :50051` (Linux/Mac)
4. Check firewall allows incoming connections on port 50051

**Issue**: `PipelineError: Failed to load manifest`

**Solutions**:
1. Ensure `WEBRTC_PIPELINE_MANIFEST` points to valid YAML/JSON file
2. Check manifest syntax and node types exist in runtime
3. Verify file path is accessible from server working directory

**Issue**: Browser can't connect (CORS or gRPC-Web)

**Solutions**:
1. Use gRPC-Web proxy (envoy) for browser clients
2. Or enable CORS in server configuration (already enabled in WebRtcSignalingService)
3. Check browser console for specific gRPC-Web errors

## Troubleshooting

### Common Issues

**1. Compilation Error: "use of unstable library feature"**
```bash
# Update Rust to 1.87 or later
rustup update stable
```

**2. Signaling Connection Failed**
```
Error: SignalingError("WebSocket connection failed")
```
- Ensure signaling server is running at configured URL
- Check firewall rules for WebSocket connections

**3. Peer Connection Timeout**
```
Error: NatTraversalFailed("ICE connection timeout")
```
- Verify STUN/TURN server configuration
- Check network connectivity between peers
- Ensure firewall allows UDP traffic

**4. Codec Feature Not Available**
```
Error: EncodingError("Opus encoding requires the 'codecs' feature flag")
```
- Build with `--features codecs`
- Install CMake if not available

## Performance

**Benchmarks** (Rust 1.91, Windows 11):
- Peer connection setup: ~500ms
- Session creation: <1ms
- Audio send (960 samples): <100μs
- Video frame send (720p): <500μs
- Memory per peer: ~2MB

## Dependencies

- **webrtc-rs** v0.14.0 - Pure Rust WebRTC implementation
- **tokio** v1.35 - Async runtime
- **tokio-tungstenite** v0.21 - WebSocket client
- **tracing** v0.1 - Logging
- **serde** v1.0 - Serialization

**Optional**:
- **opus** v0.3 - Audio codec (requires CMake)
- **vpx** v0.1 - VP9 video codec
- **openh264** v0.5 - H.264 video codec

## Documentation

- **[INTEGRATION.md](INTEGRATION.md)** - Complete integration guide with deployment examples
- **[DESIGN.md](DESIGN.md)** - Architecture and design decisions
- **[specs/001-webrtc-multi-peer-transport/](../../specs/001-webrtc-multi-peer-transport/)** - Implementation specifications

## Related Projects

- **[webrtc-rs](https://github.com/webrtc-rs/webrtc)** - Rust WebRTC implementation
- **RemoteMedia gRPC Transport** - Production gRPC-based transport
- **RemoteMedia FFI Transport** - Python SDK transport

## Contributing

This project uses [OpenSpec](../../openspec/) for planning changes. See [AGENTS.md](../../openspec/AGENTS.md) for details.

## License

MIT OR Apache-2.0 (same as parent project)

## Links

- **GitHub**: [https://github.com/matbeedotcom/remotemedia-sdk](https://github.com/matbeedotcom/remotemedia-sdk)
- **Docs**: [docs.rs/remotemedia-webrtc](https://docs.rs/remotemedia-webrtc) (when published)
- **Crate**: [crates.io/crates/remotemedia-webrtc](https://crates.io/crates/remotemedia-webrtc) (when published)
