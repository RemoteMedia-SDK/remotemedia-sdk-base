# WebRTC Transport Examples

This directory contains example applications demonstrating the WebRTC transport capabilities.

## Prerequisites

1. **Rust toolchain**: 1.87 or later
2. **Signaling server** (for peer-to-peer examples): See [signaling_server/](signaling_server/)

## Examples

### 1. Simple Peer (`simple_peer.rs`)

Basic 1:1 video call demonstrating peer connection setup.

```bash
# Start signaling server first
cd signaling_server && npm start

# Terminal 1: Alice
cargo run --example simple_peer -- --peer-id alice

# Terminal 2: Bob (connects to Alice)
cargo run --example simple_peer -- --peer-id bob --connect alice
```

**Features demonstrated:**
- Low latency configuration preset
- Peer connection setup
- TURN server configuration (optional)

### 2. Conference (`conference.rs`)

Multi-peer audio conference with up to 5 participants.

```bash
# Start signaling server
cd signaling_server && npm start

# Multiple terminals
cargo run --example conference -- --room "team-meeting" --name "Alice"
cargo run --example conference -- --room "team-meeting" --name "Bob"
cargo run --example conference -- --room "team-meeting" --name "Charlie"
```

**Features demonstrated:**
- Multi-peer mesh topology
- Participant management
- Audio optimization settings

### 3. Data Channel Control (`data_channel_control.rs`)

Pipeline control via WebRTC data channels.

```bash
cargo run --example data_channel_control
```

**Features demonstrated:**
- JSON control messages
- Binary data transfer
- Reliable vs unreliable modes
- Pipeline start/stop/pause/resume

## Pipeline Manifests

The examples directory also contains pipeline manifest files:

| File | Description |
|------|-------------|
| `loopback.yaml` | Simple input→output loopback |
| `loopback.json` | Same as above in JSON format |
| `tts.json` | Text-to-speech pipeline |
| `vad_bidirectional.json` | Voice activity detection with bidirectional streaming |

### Using Pipeline Manifests

When running with gRPC signaling, specify a manifest:

```bash
WEBRTC_PIPELINE_MANIFEST="examples/vad_bidirectional.json" \
cargo run --bin webrtc_server --features grpc-signaling
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SIGNALING_URL` | `ws://localhost:8080` | WebSocket signaling server URL |
| `TURN_URL` | (none) | TURN server URL for NAT traversal |
| `TURN_USERNAME` | (none) | TURN server username |
| `TURN_CREDENTIAL` | (none) | TURN server credential |

## Configuration Presets

The examples use configuration presets for common scenarios:

```rust
// Real-time video calls (low latency)
let config = WebRtcTransportConfig::low_latency_preset("ws://localhost:8080");

// High quality streaming (recording/broadcast)
let config = WebRtcTransportConfig::high_quality_preset("ws://localhost:8080");

// Mobile/unreliable networks
let config = WebRtcTransportConfig::mobile_network_preset("ws://localhost:8080")
    .with_turn_servers(vec![...]);
```

## Signaling Server

A reference signaling server implementation is provided in [signaling_server/](signaling_server/).

```bash
cd signaling_server
npm install
npm start  # Starts on ws://localhost:8080
```

See [signaling_server/README.md](signaling_server/README.md) for protocol details.

## Troubleshooting

### ICE Connection Failed

If peers can't connect:

1. Check STUN server accessibility
2. Configure TURN servers for NAT traversal:
   ```bash
   TURN_URL="turn:turn.example.com:3478" \
   TURN_USERNAME="user" \
   TURN_CREDENTIAL="pass" \
   cargo run --example simple_peer
   ```

### Signaling Connection Failed

Ensure the signaling server is running:
```bash
curl -I ws://localhost:8080  # Should return 101 Switching Protocols
```

### Audio/Video Quality Issues

Adjust jitter buffer for network conditions:
```rust
let mut config = WebRtcTransportConfig::default();
config.jitter_buffer_size_ms = 150;  // Increase for unstable networks
```
