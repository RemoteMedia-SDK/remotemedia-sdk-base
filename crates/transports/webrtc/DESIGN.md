# WebRTC Multi-Peer Transport Design

**Status:** Implementation in progress
**Worktree:** `remotemedia-sdk-webrtc/`
**Branch:** `webrtc-multi-peer-transport`
**Base:** `003-transport-decoupling`

## Overview

A production-ready WebRTC transport for RemoteMedia SDK that enables:
- **Multi-peer mesh networking** (N:N communication)
- **Audio/Video/Data channels** (full WebRTC feature set)
- **Real-time pipeline execution** across connected peers
- **Zero-copy** where possible via shared buffers
- **Automatic peer discovery** and connection management

## Architecture

### High-Level Design

```
┌────────────────────────────────────────────────────────────────┐
│  WebRTC Transport (implements PipelineTransport)               │
│                                                                 │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐│
│  │ Signaling Server │  │ Peer Manager     │  │ Session Mgr  ││
│  │ (WebSocket)      │  │ (Mesh Topology)  │  │ (Pipeline)   ││
│  └────────┬─────────┘  └────────┬─────────┘  └──────┬───────┘│
│           │                     │                     │        │
│           └─────────────────────┴─────────────────────┘        │
│                                 │                               │
│  ┌──────────────────────────────┴─────────────────────────┐   │
│  │ WebRTC Peer Connections (1 per remote peer)            │   │
│  │                                                          │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐       │   │
│  │  │ Audio Track│  │ Video Track│  │ Data Channel│       │   │
│  │  │ (opus)     │  │ (vp9/h264) │  │ (reliable)  │       │   │
│  │  └────────────┘  └────────────┘  └────────────┘       │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                 │                               │
│  ┌──────────────────────────────┴─────────────────────────┐   │
│  │ PipelineRunner (from runtime-core)                     │   │
│  │ • Execute pipelines on incoming media                  │   │
│  │ • Send processed output to peers                       │   │
│  └────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

#### 1. **SignalingServer** (`src/signaling.rs`)
- WebSocket-based signaling for SDP/ICE exchange
- Peer discovery (broadcast presence)
- Connection state coordination
- JSON-RPC 2.0 protocol

#### 2. **PeerManager** (`src/peer_manager.rs`)
- Maintains mesh topology (N:N connections)
- Manages RTCPeerConnection instances
- Handles connection lifecycle (offer/answer/ice)
- Automatic reconnection on failure

#### 3. **MediaChannels** (`src/channels.rs`)
- **AudioChannel:** Opus codec, real-time audio streaming
- **VideoChannel:** VP9/H264 codec, adaptive bitrate
- **DataChannel:** Reliable/ordered binary data transfer
- Separate channels per peer

#### 4. **SessionManager** (`src/session.rs`)
- Maps WebRTC streams to pipeline sessions
- Routes processed output to appropriate peers
- Handles session cleanup

#### 5. **WebRtcTransport** (`src/lib.rs`)
- Implements `PipelineTransport` trait
- Integrates all components
- Exposes public API

## Data Flow

### Incoming Media Processing

```
Remote Peer
  │
  ├─ Audio Track ──────────┐
  ├─ Video Track ──────────┤
  └─ Data Channel ─────────┤
                            │
                            ▼
                  WebRTC Peer Connection
                            │
                            ▼
                  Media Frame Decoder
                            │
                            ▼
                  TransportData::new()
                            │
                            ▼
            PipelineRunner::execute_unary()
                            │
                            ▼
                  Processed RuntimeData
                            │
                            ▼
                  Encode to WebRTC
                            │
                            ▼
              Send to target peer(s)
```

### Multi-Peer Routing

```
Peer A ──┐
Peer B ──┼──→ Local Node ──→ PipelineRunner ──┬──→ Peer C
Peer C ──┘                                      ├──→ Peer D
                                                └──→ Peer E
```

## API Design

### Core Transport Implementation

```rust
pub struct WebRtcTransport {
    signaling: Arc<SignalingServer>,
    peer_manager: Arc<PeerManager>,
    session_manager: Arc<SessionManager>,
    runner: PipelineRunner,
    config: WebRtcConfig,
}

impl WebRtcTransport {
    pub async fn new(config: WebRtcConfig) -> Result<Self>;

    pub async fn connect_peer(&self, peer_id: &str) -> Result<PeerId>;

    pub async fn disconnect_peer(&self, peer_id: PeerId) -> Result<()>;

    pub fn list_peers(&self) -> Vec<PeerInfo>;

    pub async fn send_to_peer(
        &self,
        peer_id: PeerId,
        data: TransportData,
    ) -> Result<()>;

    pub async fn broadcast(
        &self,
        data: TransportData,
        exclude: Vec<PeerId>,
    ) -> Result<()>;
}
```

### Configuration

```rust
pub struct WebRtcConfig {
    /// Signaling server URL (ws://... or wss://...)
    pub signaling_url: String,

    /// STUN servers for NAT traversal
    pub stun_servers: Vec<String>,

    /// TURN servers for relay (optional)
    pub turn_servers: Vec<TurnServer>,

    /// Local peer ID (auto-generated if None)
    pub peer_id: Option<String>,

    /// Max peers in mesh (default: 10)
    pub max_peers: usize,

    /// Audio codec preference (default: Opus)
    pub audio_codec: AudioCodec,

    /// Video codec preference (default: VP9)
    pub video_codec: VideoCodec,

    /// Enable data channel (default: true)
    pub enable_data_channel: bool,

    /// Data channel reliability (default: Reliable)
    pub data_channel_mode: DataChannelMode,
}
```

### Multi-Peer Session

```rust
pub struct MultiPeerSession {
    session_id: String,
    manifest: Arc<Manifest>,
    peers: HashMap<PeerId, PeerConnection>,
    router: StreamRouter,
}

impl MultiPeerSession {
    /// Send input to specific peer's pipeline
    pub async fn send_to_peer(
        &mut self,
        peer_id: PeerId,
        data: TransportData,
    ) -> Result<()>;

    /// Broadcast input to all peers
    pub async fn broadcast(&mut self, data: TransportData) -> Result<()>;

    /// Receive output from any peer
    pub async fn recv_from_any(&mut self) -> Result<Option<(PeerId, TransportData)>>;

    /// Receive output from specific peer
    pub async fn recv_from_peer(
        &mut self,
        peer_id: PeerId,
    ) -> Result<Option<TransportData>>;
}
```

## Signaling Protocol

### JSON-RPC 2.0 Messages

```json
// Peer announces presence
{
  "jsonrpc": "2.0",
  "method": "peer.announce",
  "params": {
    "peer_id": "peer-abc123",
    "capabilities": ["audio", "video", "data"]
  }
}

// Peer initiates connection (send offer)
{
  "jsonrpc": "2.0",
  "method": "peer.offer",
  "params": {
    "from": "peer-abc123",
    "to": "peer-def456",
    "sdp": "v=0\r\no=- ... (full SDP offer)"
  }
}

// Peer responds (send answer)
{
  "jsonrpc": "2.0",
  "method": "peer.answer",
  "params": {
    "from": "peer-def456",
    "to": "peer-abc123",
    "sdp": "v=0\r\no=- ... (full SDP answer)"
  }
}

// ICE candidate exchange
{
  "jsonrpc": "2.0",
  "method": "peer.ice_candidate",
  "params": {
    "from": "peer-abc123",
    "to": "peer-def456",
    "candidate": "candidate:... (ICE candidate)"
  }
}

// Peer disconnects
{
  "jsonrpc": "2.0",
  "method": "peer.disconnect",
  "params": {
    "peer_id": "peer-abc123"
  }
}
```

## Media Encoding/Decoding

### Audio (Opus)

```rust
pub struct AudioEncoder {
    encoder: opus::Encoder,
    sample_rate: u32,
    channels: u16,
}

impl AudioEncoder {
    pub fn encode(&mut self, samples: &[f32]) -> Result<Vec<u8>>;
}

pub struct AudioDecoder {
    decoder: opus::Decoder,
}

impl AudioDecoder {
    pub fn decode(&mut self, packet: &[u8]) -> Result<Vec<f32>>;
}
```

### Video (VP9/H264)

```rust
pub struct VideoEncoder {
    encoder: vpx::Encoder, // or openh264::Encoder
    width: u32,
    height: u32,
    bitrate: u32,
}

impl VideoEncoder {
    pub fn encode(&mut self, frame: &[u8]) -> Result<Vec<u8>>;
}
```

### Data Channel (Binary)

```rust
pub enum DataChannelMessage {
    Binary(Vec<u8>),      // Raw binary data
    Json(serde_json::Value), // Structured data
}

impl DataChannelMessage {
    pub fn encode(&self) -> Vec<u8>;
    pub fn decode(bytes: &[u8]) -> Result<Self>;
}
```

## Dependencies

```toml
[dependencies]
# Core runtime
remotemedia-runtime-core = { path = "../../runtime-core" }
async-trait = "0.1"
tokio = { version = "1.35", features = ["full"] }

# WebRTC
webrtc = "0.9"

# Signaling
tokio-tungstenite = "0.21"  # WebSocket client
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"

# Media codecs
opus = "0.3"         # Audio codec
vpx-sys = "0.2"      # VP9 video codec (optional)
openh264 = "0.5"     # H264 video codec (optional)

# Utils
uuid = { version = "1.6", features = ["v4"] }
tracing = "0.1"
anyhow = "1.0"
thiserror = "1.0"
```

## Implementation Phases

### Phase 1: Core Transport (Week 1)
- [x] Create crate structure
- [x] Design documentation
- [ ] Implement `WebRtcTransport` skeleton
- [ ] Implement `PipelineTransport` trait
- [ ] Basic signaling client (WebSocket)
- [ ] Single peer connection (1:1)

### Phase 2: Multi-Peer Mesh (Week 2)
- [ ] `PeerManager` implementation
- [ ] Mesh topology management
- [ ] Automatic peer discovery
- [ ] Connection state machine
- [ ] Reconnection logic

### Phase 3: Media Channels (Week 3)
- [ ] Audio track setup (Opus)
- [ ] Video track setup (VP9)
- [ ] Data channel setup
- [ ] Codec integration
- [ ] Frame encoding/decoding

### Phase 4: Pipeline Integration (Week 4)
- [ ] `SessionManager` implementation
- [ ] Route incoming media to pipeline
- [ ] Route pipeline output to peers
- [ ] Multi-peer session API
- [ ] Broadcast/unicast routing

### Phase 5: Production Readiness (Week 5)
- [ ] Error handling and recovery
- [ ] Connection quality monitoring
- [ ] Adaptive bitrate control
- [ ] Testing (unit + integration)
- [ ] Documentation and examples

## Usage Examples

### Simple 1:1 Video Call with Pipeline

```rust
use remotemedia_webrtc::{WebRtcTransport, WebRtcConfig};
use remotemedia_runtime_core::manifest::Manifest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure transport
    let config = WebRtcConfig {
        signaling_url: "ws://localhost:8080".to_string(),
        stun_servers: vec!["stun:stun.l.google.com:19302".to_string()],
        ..Default::default()
    };

    let transport = WebRtcTransport::new(config).await?;

    // Load pipeline (e.g., background blur)
    let manifest = Arc::new(Manifest::from_file("blur_pipeline.yaml")?);

    // Create session
    let mut session = transport.stream(manifest).await?;

    // Connect to peer
    let peer_id = transport.connect_peer("peer-remote").await?;

    // Receive video from peer, process, send back
    while let Some((from_peer, input)) = session.recv_from_any().await? {
        let output = transport.runner.execute_unary(manifest.clone(), input).await?;
        session.send_to_peer(from_peer, output).await?;
    }

    session.close().await?;
    Ok(())
}
```

### Multi-Peer Conference with Audio Mixing

```rust
// Conference room with 5 participants
let mut session = transport.stream(manifest).await?;

// Connect to all peers
for peer in &["peer-1", "peer-2", "peer-3", "peer-4"] {
    transport.connect_peer(peer).await?;
}

// Audio mixing loop
loop {
    // Collect audio from all peers
    let mut audio_inputs = Vec::new();

    for peer in transport.list_peers() {
        if let Some(audio) = session.recv_from_peer(peer.id).await? {
            audio_inputs.push(audio);
        }
    }

    // Mix audio via pipeline
    let mixed = transport.runner.execute_unary(
        manifest.clone(),
        TransportData::new(RuntimeData::AudioMix(audio_inputs)),
    ).await?;

    // Broadcast mixed audio to all peers
    session.broadcast(mixed).await?;
}
```

## Testing Strategy

### Unit Tests
- Signaling protocol parsing
- Peer connection state machine
- Media encoding/decoding
- Routing logic

### Integration Tests
- 2-peer connection setup
- 4-peer mesh network
- Media stream transmission
- Pipeline execution with WebRTC

### Performance Tests
- Latency measurement (target: <100ms end-to-end)
- Throughput testing (target: 30fps video)
- CPU usage monitoring
- Memory leak detection

## Security Considerations

1. **Encryption:** All WebRTC channels encrypted (DTLS-SRTP)
2. **Authentication:** Signaling server validates peer identity
3. **Authorization:** Peer-to-peer permissions (allow/deny lists)
4. **NAT Traversal:** STUN/TURN for firewall bypass
5. **DoS Protection:** Rate limiting on signaling server

## Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Connection Setup | <2s | SDP exchange + ICE |
| Audio Latency | <50ms | Opus encoding + network |
| Video Latency | <100ms | VP9 encoding + network |
| Max Peers | 10 | Mesh topology limit |
| CPU (720p) | <30% | Single core, 30fps |
| Memory | <100MB | Per peer connection |

## Future Enhancements

- [ ] SFU (Selective Forwarding Unit) mode for large conferences
- [ ] Screen sharing support
- [ ] File transfer via data channel
- [ ] Simulcast for adaptive quality
- [ ] WebRTC stats API integration
- [ ] Browser SDK (WASM)

## gRPC Signaling Integration (2025-01)

### Overview

In addition to WebSocket-based JSON-RPC 2.0 signaling, the WebRTC transport now supports **gRPC bidirectional streaming** for signaling. This provides type-safe, efficient signaling with automatic code generation via Protocol Buffers.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Next.js Client (or other gRPC-enabled client)              │
│  ↓ (gRPC bidirectional stream over HTTP/2)                  │
│  WebRtcSignalingService (in remotemedia-webrtc)             │
│    │                                                          │
│    ├─ Peer announces (client_id, capabilities)              │
│    ├─ Server auto-spawns ServerPeer                         │
│    │   └─ PeerConnection + PipelineRunner + StreamSession   │
│    │                                                          │
│    ├─ Client sends SDP offer                                 │
│    ├─ ServerPeer generates real SDP answer                   │
│    ├─ ICE candidate exchange                                 │
│    └─ Media flows: Client ↔ WebRTC ↔ Pipeline ↔ WebRTC ↔ Client │
└─────────────────────────────────────────────────────────────┘
```

### Key Differences: gRPC vs WebSocket Signaling

| Feature | WebSocket (JSON-RPC 2.0) | gRPC (Protocol Buffers) |
|---------|--------------------------|-------------------------|
| **Protocol** | WebSocket over TCP | HTTP/2 with multiplexing |
| **Encoding** | JSON (text) | Protobuf (binary) |
| **Type Safety** | Runtime validation | Compile-time types |
| **Code Generation** | Manual types | Automatic via tonic-build |
| **Performance** | Good | Better (binary encoding) |
| **Browser Support** | Native | Via grpc-web |
| **Use Case** | Browser-native clients | Server-to-server, typed clients |

### ServerPeer Concept

When a client announces via gRPC signaling, the server automatically creates a **ServerPeer** that:

1. **Creates WebRTC PeerConnection** with ICE servers configured
2. **Creates Pipeline Session** via `PipelineRunner::create_stream_session(manifest)`
3. **Bidirectional Media Routing**:
   - **Incoming**: RTP packets from client → RuntimeData → `session.send_input()`
   - **Outgoing**: `session.receive_output()` → RuntimeData → RTP packets to client
4. **Generates Real SDP Answer** (not dummy responses)
5. **Cleans up** when client disconnects

### Implementation Status

**Completed:**
- ✅ Moved gRPC signaling service from remotemedia-grpc to remotemedia-webrtc
- ✅ Added `grpc-signaling` feature flag
- ✅ Proto file and service implementation
- ✅ Build system (tonic-prost-build)

**In Progress:**
- 🚧 ServerPeer implementation (`src/peer/server_peer.rs`)
- 🚧 PipelineRunner integration
- 🚧 Signaling service callbacks for peer lifecycle

**Pending:**
- ⏳ webrtc_server binary with gRPC signaling
- ⏳ Real SDP answer generation
- ⏳ Bidirectional media routing
- ⏳ Testing with Next.js client

### Configuration

```bash
# Enable gRPC signaling
cargo build --bin webrtc_server --features grpc-signaling

# Environment variables
GRPC_SIGNALING_PORT=50051
GRPC_SIGNALING_ADDRESS="0.0.0.0:50051"
WEBRTC_PIPELINE_MANIFEST="./examples/loopback.yaml"
WEBRTC_STUN_SERVERS="stun:stun.l.google.com:19302"
```

### Proto Definition

See [`protos/webrtc_signaling.proto`](protos/webrtc_signaling.proto) for full protocol definition.

**Key Messages:**
- `AnnounceRequest` - Client registers with peer_id and capabilities
- `OfferRequest` - Client sends SDP offer
- `AnswerNotification` - Server sends real SDP answer (via ServerPeer)
- `IceCandidateRequest/Notification` - Bidirectional ICE exchange
- `SignalingResponse` - Unified response/notification envelope

### Client Example (TypeScript/Next.js)

```typescript
import { WebRtcSignalingClient } from '@grpc/grpc-js';

// Connect to gRPC signaling
const client = new WebRtcSignalingClient('http://localhost:50051');

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

// Listen for peer joined notifications
stream.on('data', (response) => {
  if (response.notification?.peerJoined) {
    console.log('Server peer ready:', response.notification.peerJoined.peerId);
  }
  if (response.notification?.answer) {
    // Set remote description with real SDP answer
    pc.setRemoteDescription({
      type: 'answer',
      sdp: response.notification.answer.sdp
    });
  }
});

// Create WebRTC offer
const offer = await pc.createOffer();
await pc.setLocalDescription(offer);

// Send offer to server
stream.write({
  requestId: '2',
  offer: {
    toPeerId: 'remotemedia-server',
    sdp: offer.sdp,
    type: 'offer'
  }
});
```

## Pacing Model (pacing-domains spec)

Tick-driven nodes (avatar render, idle generators) are paced by the
core `transport::pacer::Pacer` driven by a `TickSource`. Two source
shapes:

| Source | When | Where defined |
|---|---|---|
| `WallTickSource(hz)` | Headless / `SourceWall(hz)` nodes | `crates/core/src/transport/pacer.rs` (today) |
| Wire-bound `media_clock` source | `ClockedToOutboundMedia` nodes terminating into an outbound RTP stream | `crates/transports/webrtc/src/session/` (Phase 5.1/5.2 — pending) |

The wire-bound source will subscribe to a `media_clock` broadcast
event published by the audio sender (after each Opus packet) and the
video tracks (after each `send_video` call), carrying:

```
media_clock {
    medium: "audio" | "video",
    stream_id: String,        // matches capability bus `<medium>:<stream_id>` addr
    pts_us: u64,              // sender-side PTS at egress
    frame_idx: u64,
    wire_deadline_us: u64,    // when this frame must be queued by to hit RTP timing
}
```

Subscribing nodes plug into the same `Pacer::run` loop that drives
wall-paced nodes — only the `TickSource::next` impl differs. When the
wire pacer ceases (peer disconnect), the source returns `None` and
the Pacer exits cleanly; a `media_clock_stopped` event lets the
session router fall back to wall pacing for headless sessions if
configured (Phase 5.9 — pending).

**Capability bus integration.** Negotiated outbound capabilities
(framerate, sample rate, channel layout) are published on the per-stream
addresses `audio:<stream_id>` and `video:<stream_id>` after SDP
finalization. Nodes opt in via `capability_subscriptions()` and read
the latest published value via `ctx.capability(addr)` — the runtime
forwarder writes to a per-(node, session) cache so reads never block.
See [`docs/PACING_DOMAINS.md`](../../../docs/PACING_DOMAINS.md) and
[`docs/NODE_RUNTIME_CONTEXT.md`](../../../docs/NODE_RUNTIME_CONTEXT.md)
for the full surface.

## References

- [WebRTC Specification](https://w3c.github.io/webrtc-pc/)
- [CUSTOM_TRANSPORT_GUIDE.md](../../../docs/CUSTOM_TRANSPORT_GUIDE.md)
- [PACING_DOMAINS.md](../../../docs/PACING_DOMAINS.md) — pacing
  model, snapshot ports, Reactive→Clocked seam.
- [NODE_RUNTIME_CONTEXT.md](../../../docs/NODE_RUNTIME_CONTEXT.md) — per-call
  context (capabilities, snapshots, session state).
- [specs/003-transport-decoupling/](../../../specs/003-transport-decoupling/)
- [rust-webrtc crate](https://github.com/webrtc-rs/webrtc)
- [tonic gRPC framework](https://github.com/hyperium/tonic)
- [gRPC-Web](https://github.com/grpc/grpc-web)
