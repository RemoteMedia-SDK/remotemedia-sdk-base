# RemoteMedia FFI Transport

FFI (Foreign Function Interface) transport for RemoteMedia pipelines, providing fast Rust-accelerated pipeline execution for **Node.js** and **Python** applications, including **WebRTC** real-time communication support.

## Overview

This crate provides bindings to the `remotemedia-runtime-core` for multiple languages:

### Python FFI
- **Fast execution**: Native Rust performance for media processing
- **Zero-copy**: Direct numpy array integration for audio/video data
- **Async support**: Full Python asyncio integration via PyO3
- **Independent deployment**: Can be updated without touching runtime-core

### Node.js FFI (N-API)
- **Native performance**: Rust-powered media processing in Node.js
- **Async/Promise support**: Full async/await integration
- **Buffer handling**: Zero-copy Buffer operations
- **WebRTC support**: Built-in WebRTC transport layer

### WebRTC Transport (both languages)
- **Real-time communication**: Low-latency audio/video streaming
- **Multi-peer support**: Up to 10 concurrent peer connections
- **Session management**: Room/session-based peer grouping
- **Event-driven**: Callbacks for peer connections, disconnections, and data

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  Python Application                                 │
│  ↓                                                   │
│  remotemedia_ffi (PyO3 module)                      │
│  ├─ execute_pipeline()                              │
│  ├─ execute_pipeline_with_input()                   │
│  └─ marshal.py ↔ RuntimeData conversion             │
│     ↓                                                │
│  remotemedia-runtime-core (PipelineRunner)          │
│  ├─ Transport-agnostic execution                    │
│  ├─ Node registry                                   │
│  └─ Audio/video processing                          │
└─────────────────────────────────────────────────────┘
```

## Installation

### Development (Editable Install)

For local development with editable python-client:

```bash
# 1. Install python-client as editable
cd python-client
pip install -e . --no-deps

# 2. Build and link Rust runtime
cd ../transports/ffi
./dev-install.sh
```

The `dev-install.sh` script:
- Builds the Rust extension with maturin
- Creates a symlink in python-client/remotemedia/
- Auto-updates when you rebuild

### Production Install

```bash
# Install python-client normally
pip install python-client/

# Install Rust runtime from wheel
pip install remotemedia_ffi-0.4.0-cp310-abi3-macosx_11_0_arm64.whl
```

Or build the wheel yourself:

```bash
cd transports/ffi
pip install maturin
maturin build --release --features extension-module
# Wheel will be in: ../../target/wheels/
```

## Usage

### Basic Pipeline Execution

```python
import asyncio
import json
from remotemedia.runtime import execute_pipeline, is_available

# Check if Rust runtime is available
if is_available():
    print("Using Rust-accelerated runtime")

async def main():
    manifest = {
        "version": "v1",
        "metadata": {"name": "audio_processing"},
        "nodes": [
            {
                "id": "resample",
                "node_type": "AudioResample",
                "params": {"target_rate": 16000}
            }
        ],
        "connections": []
    }

    manifest_json = json.dumps(manifest)
    result = await execute_pipeline(manifest_json)
    print(result)

asyncio.run(main())
```

### With Input Data

```python
from remotemedia.runtime import execute_pipeline_with_input

result = await execute_pipeline_with_input(
    manifest_json,
    input_data=["Hello, world!"],
    enable_metrics=True
)
```

### Zero-Copy Numpy Integration

**NEW: Automatic numpy array handling!** Just pass numpy arrays directly - no conversion functions needed!

```python
import numpy as np
from remotemedia.runtime import execute_pipeline_with_input

# Create audio frames (e.g., 20ms at 48kHz = 960 samples)
audio_frame = np.zeros(960, dtype=np.float32)

# Pass numpy array directly - automatically wrapped in RuntimeData::Numpy
result = await execute_pipeline_with_input(manifest_json, [audio_frame])

# Results are automatically converted back to numpy arrays
if isinstance(result, np.ndarray):
    print(f"Received numpy array: {result.shape}")
```

**How it works (zero-copy architecture):**

1. **Python → Rust FFI**: `python_to_runtime_data` detects numpy arrays and wraps them in `RuntimeData::Numpy` (zero-copy via buffer protocol)
2. **Rust Pipeline**: `RuntimeData::Numpy` flows through pipeline without conversion
3. **IPC Boundary**: `to_ipc_runtime_data` serializes **once** to iceoryx2 shared memory format
4. **Multiprocess Node**: Python receives data via zero-copy iceoryx2
5. **Return Path**: `from_ipc_runtime_data` deserializes back to `RuntimeData::Numpy`
6. **Rust → Python FFI**: `runtime_data_to_python` converts back to numpy array

**Performance for streaming audio:**
- **Before**: Serialize on every FFI call (50+ times/sec for 20ms frames) = ~50ms total overhead
- **After**: Serialize **once** at IPC boundary = ~1ms total overhead
- **Speedup**: ~50x reduction in serialization overhead for streaming pipelines!

## API Reference

### `execute_pipeline(manifest_json: str, enable_metrics: bool = False) -> Any`

Execute a pipeline from a JSON manifest.

**Parameters:**
- `manifest_json`: JSON string containing pipeline definition
- `enable_metrics`: If True, include execution metrics in response

**Returns:** Pipeline execution results (format depends on pipeline output)

### `execute_pipeline_with_input(manifest_json: str, input_data: List[Any], enable_metrics: bool = False) -> Any`

Execute a pipeline with input data.

**Parameters:**
- `manifest_json`: JSON string containing pipeline definition
- `input_data`: List of input items to process
- `enable_metrics`: If True, include execution metrics in response

**Returns:** Pipeline execution results

### `get_runtime_version() -> str`

Get the version of the FFI transport.

### `is_available() -> bool`

Check if Rust runtime is available (always returns `True`).

### `numpy_to_audio_dict(arr: np.ndarray, sample_rate: int, channels: int) -> dict`

Convert a numpy array to an audio RuntimeData dictionary for use in pipelines.

**Parameters:**
- `arr`: Numpy array with `float32` dtype containing audio samples
- `sample_rate`: Audio sample rate in Hz (e.g., 16000, 44100, 48000)
- `channels`: Number of audio channels (1 for mono, 2 for stereo, etc.)

**Returns:** Dictionary with keys:
- `type`: "audio"
- `samples`: Audio sample data (list of float32)
- `sample_rate`: Sample rate in Hz
- `channels`: Number of channels

**Example:**
```python
import numpy as np
from remotemedia.runtime import numpy_to_audio_dict

# Create 1 second of 440Hz sine wave
t = np.linspace(0, 1, 48000, dtype=np.float32)
audio = np.sin(2 * np.pi * 440 * t)

# Convert to pipeline format
audio_dict = numpy_to_audio_dict(audio, sample_rate=48000, channels=1)

# Use in pipeline
result = await execute_pipeline_with_input(manifest, [audio_dict])
```

### `audio_dict_to_numpy(audio_dict: dict) -> np.ndarray`

Convert an audio RuntimeData dictionary back to a numpy array.

**Parameters:**
- `audio_dict`: Dictionary with keys: `samples`, `sample_rate`, `channels`

**Returns:** Numpy array with `float32` dtype. Shape is:
- 1D array `(samples,)` for mono audio
- 2D array `(frames, channels)` for multi-channel audio

**Example:**
```python
from remotemedia.runtime import audio_dict_to_numpy

# Get audio from pipeline result
result = await execute_pipeline_with_input(manifest, [audio_dict])

if result.get("type") == "audio":
    # Convert back to numpy for processing
    audio_array = audio_dict_to_numpy(result)
    
    # Now you can use numpy operations
    max_amplitude = np.max(np.abs(audio_array))
    print(f"Max amplitude: {max_amplitude}")
```

## Development

### Building

```bash
# Debug build
maturin develop

# Release build with optimizations
maturin develop --release

# Build wheel
maturin build --release
```

### Testing

```bash
# Run Rust tests
cargo test

# Run Python tests
pytest python/tests/
```

### Type Stubs

For better IDE support, generate type stubs:

```bash
maturin develop --release
stubgen -p remotemedia_ffi -o stubs/
```

## Performance Benefits

Compared to pure Python execution:
- **Audio processing**: 2-16x faster (depending on operation)
- **VAD (Voice Activity Detection)**: 8-12x faster
- **Resampling**: 4-6x faster
- **Zero-copy**: No data copying between Python and Rust

## Migration from v0.3

```python
# OLD (v0.3.x):
from remotemedia_runtime import execute_pipeline

# NEW (v0.4.x):
from remotemedia.runtime import execute_pipeline  # Same API
```

The API remains the same, but execution now goes through the decoupled `PipelineRunner`.

## Environment Variables

- `RUST_LOG`: Control logging level (default: "info")
  ```bash
  RUST_LOG=debug python my_app.py
  ```

## Troubleshooting

### Import Error

If you see `ModuleNotFoundError: No module named 'remotemedia_ffi'`:
1. Ensure maturin is installed: `pip install maturin`
2. Build the module: `maturin develop --release`
3. Check Python can find the module: `python -c "import remotemedia_ffi; print(remotemedia_ffi.__version__)"`

### Performance Issues

For maximum performance:
1. Use release builds: `maturin develop --release`
2. Enable CPU optimizations: `RUSTFLAGS="-C target-cpu=native" maturin develop --release`
3. Use zero-copy numpy integration where possible

## WebRTC FFI Transport

The WebRTC module provides real-time peer-to-peer communication with pipeline integration.

### Building with WebRTC Support

```bash
# Node.js bindings
cargo build --features napi-webrtc

# Python bindings
cargo build --features python-webrtc

# Both
cargo build --features napi-webrtc,python-webrtc
```

### Node.js WebRTC Usage

```typescript
// Import the native module
const native = require('./nodejs');

// Create WebRTC server with embedded signaling
const server = await native.WebRtcServer.create({
  port: 50051,                                    // WebSocket signaling port
  manifest: JSON.stringify({                      // Pipeline configuration
    nodes: [{ id: 'echo', type: 'Echo' }],
    connections: []
  }),
  stunServers: ['stun:stun.l.google.com:19302'], // Required: at least one
  turnServers: [{                                 // Optional TURN for NAT
    url: 'turn:turn.example.com:3478',
    username: 'user',
    credential: 'pass'
  }],
  maxPeers: 5,                                    // Max concurrent peers (1-10)
  videoCodec: 'vp9'                               // 'vp8', 'vp9', or 'h264'
});

// Register event handlers
server.on('peer_connected', (event) => {
  console.log(`Peer ${event.peerId} connected`);
  console.log(`Capabilities: audio=${event.capabilities.audio}, video=${event.capabilities.video}`);
});

server.on('peer_disconnected', (event) => {
  console.log(`Peer ${event.peerId} disconnected: ${event.reason || 'unknown'}`);
});

server.on('pipeline_output', (event) => {
  console.log(`Pipeline output for peer ${event.peerId}:`, event.data);
});

server.on('data', (event) => {
  console.log(`Data from peer ${event.peerId}:`, event.data);
});

server.on('error', (event) => {
  console.error(`Error ${event.code}: ${event.message}`);
});

// Start the server
await server.start();
console.log(`WebRTC server running on ws://localhost:50051/ws`);

// Session/room management
const session = await server.createSession('room-1', { name: 'My Room' });
const sessions = await server.getSessions();
const retrieved = await server.getSession('room-1');
await server.deleteSession('room-1');

// Peer messaging
await server.sendToPeer('peer-123', Buffer.from('Hello!'));
await server.broadcast(Buffer.from('Hello everyone!'));
await server.disconnectPeer('peer-123', 'kicked');

// Get connected peers
const peers = await server.getPeers();
for (const peer of peers) {
  console.log(`Peer ${peer.peerId}: state=${peer.state}`);
}

// Graceful shutdown
await server.shutdown();
```

### Python WebRTC Usage

```python
import asyncio
import json
from remotemedia.webrtc import WebRtcServer

async def main():
    # Create WebRTC server with embedded signaling
    server = await WebRtcServer.create({
        "port": 50051,
        "manifest": json.dumps({
            "nodes": [{"id": "echo", "type": "Echo"}],
            "connections": []
        }),
        "stun_servers": ["stun:stun.l.google.com:19302"],
        "turn_servers": [{
            "url": "turn:turn.example.com:3478",
            "username": "user",
            "credential": "pass"
        }],
        "max_peers": 5,
        "video_codec": "vp9"
    })

    # Register event handlers using decorators
    @server.on_peer_connected
    async def handle_peer_connected(event):
        print(f"Peer {event.peer_id} connected")
        print(f"Capabilities: audio={event.capabilities.audio}, video={event.capabilities.video}")

    @server.on_peer_disconnected
    async def handle_peer_disconnected(event):
        print(f"Peer {event.peer_id} disconnected: {event.reason or 'unknown'}")

    @server.on_pipeline_output
    async def handle_pipeline_output(event):
        print(f"Pipeline output for peer {event.peer_id}: {event.data}")

    @server.on_error
    async def handle_error(event):
        print(f"Error {event.code}: {event.message}")

    # Start the server
    await server.start()
    print(f"WebRTC server running on ws://localhost:50051/ws")

    # Session/room management
    session = await server.create_session("room-1", metadata={"name": "My Room"})
    sessions = await server.get_sessions()
    retrieved = await server.get_session("room-1")
    await server.delete_session("room-1")

    # Peer messaging
    await server.send_to_peer("peer-123", b"Hello!")
    await server.broadcast(b"Hello everyone!")
    await server.disconnect_peer("peer-123", reason="kicked")

    # Get connected peers
    peers = await server.get_peers()
    for peer in peers:
        print(f"Peer {peer.peer_id}: state={peer.state}")

    # Keep server running
    try:
        await asyncio.sleep(float('inf'))
    except KeyboardInterrupt:
        pass

    # Graceful shutdown
    await server.shutdown()

asyncio.run(main())
```

### External Signaling Mode

For distributed deployments, connect to an external signaling server:

**Node.js:**
```typescript
const server = await native.WebRtcServer.connect({
  signalingUrl: 'grpc://signaling.example.com:50051',
  manifest: JSON.stringify({ nodes: [], connections: [] }),
  stunServers: ['stun:stun.l.google.com:19302'],
  reconnect: {
    maxAttempts: 5,
    initialBackoffMs: 1000,
    maxBackoffMs: 30000,
    backoffMultiplier: 2.0
  }
});
```

**Python:**
```python
server = await WebRtcServer.connect({
    "signaling_url": "grpc://signaling.example.com:50051",
    "manifest": json.dumps({"nodes": [], "connections": []}),
    "stun_servers": ["stun:stun.l.google.com:19302"],
    "reconnect": {
        "max_attempts": 5,
        "initial_backoff_ms": 1000,
        "max_backoff_ms": 30000,
        "backoff_multiplier": 2.0
    }
})
```

### WebRTC Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `port` | u16 | - | Port for embedded WebSocket signaling (mutually exclusive with `signaling_url`) |
| `signaling_url` | string | - | URL for external signaling server (`grpc://` or `grpcs://`) |
| `manifest` | string | required | Pipeline manifest as JSON string |
| `stun_servers` | string[] | required | STUN server URLs (`stun:host:port`) |
| `turn_servers` | TurnServer[] | [] | TURN server configurations |
| `max_peers` | u32 | 10 | Maximum concurrent peers (1-10) |
| `audio_codec` | string | "opus" | Audio codec (only "opus" supported) |
| `video_codec` | string | "vp9" | Video codec ("vp8", "vp9", "h264") |
| `reconnect` | ReconnectConfig | default | Reconnection settings for external signaling |

### Events

| Event | Description | Data |
|-------|-------------|------|
| `peer_connected` | Peer completed WebRTC handshake | `{ peerId, capabilities, metadata }` |
| `peer_disconnected` | Peer disconnected | `{ peerId, reason? }` |
| `pipeline_output` | Pipeline produced output for peer | `{ peerId, data, timestamp }` |
| `data` | Raw data received from peer | `{ peerId, data, timestamp }` |
| `error` | Error occurred | `{ code, message, peerId? }` |
| `session` | Session lifecycle event | `{ sessionId, eventType, peerId }` |

### Error Codes

| Code | Description |
|------|-------------|
| `SIGNALING_ERROR` | Signaling connection failed |
| `PEER_ERROR` | Peer connection error |
| `PIPELINE_ERROR` | Pipeline execution error |
| `CONFIG_ERROR` | Invalid configuration |
| `MAX_PEERS_REACHED` | Maximum peer limit reached |
| `SESSION_NOT_FOUND` | Session not found |
| `PEER_NOT_FOUND` | Peer not found |
| `RECONNECT_ATTEMPT` | Reconnection attempt (informational) |
| `RECONNECT_FAILED` | Reconnection failed after max attempts |
| `INTERNAL_ERROR` | Internal error |

## See Also

- [Runtime Core](../../runtime-core/README.md) - Core execution engine
- [gRPC Transport](../remotemedia-grpc/README.md) - gRPC service transport
- [Python Client](../../python-client/README.md) - Python SDK documentation
- [Transport Decoupling Spec](../../specs/003-transport-decoupling/spec.md) - Architecture details
- [WebRTC Spec](../../specs/016-ffi-webrtc-bindings/spec.md) - WebRTC FFI design specification
