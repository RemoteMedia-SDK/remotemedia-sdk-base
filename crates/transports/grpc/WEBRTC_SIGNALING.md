# gRPC WebRTC Signaling

This document describes the gRPC-based WebRTC signaling service, which provides an alternative to WebSocket JSON-RPC 2.0 signaling.

## Overview

The gRPC signaling service offers the same functionality as the WebSocket signaling server but with the following advantages:

- **Type-safe**: Protobuf messages with compile-time validation
- **Built-in Authentication**: Leverages existing gRPC auth middleware
- **Integrated Infrastructure**: Uses the same gRPC server as pipeline execution
- **Bidirectional Streaming**: Full-duplex communication via gRPC streams
- **Load Balancing**: Compatible with gRPC load balancers (Envoy, etc.)
- **HTTP/2**: Benefits from multiplexing and header compression

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  gRPC Server (port 50051)                                    │
│  ├─ PipelineExecutionService (unary pipeline execution)     │
│  ├─ StreamingPipelineService (bidirectional streaming)      │
│  └─ WebRtcSignalingService (WebRTC signaling) ← NEW         │
│     ├─ Signal(stream) → Bidirectional signaling stream      │
│     ├─ GetPeers() → List connected peers (unary)            │
│     └─ HealthCheck() → Service health (unary)               │
└──────────────────────────────────────────────────────────────┘
```

## Protocol Definition

See [protos/webrtc_signaling.proto](protos/webrtc_signaling.proto) for the complete protocol definition.

### Service Methods

#### 1. Signal (Bidirectional Streaming)

Main signaling method for peer-to-peer communication.

```protobuf
rpc Signal(stream SignalingRequest) returns (stream SignalingResponse);
```

**Request Types:**
- `AnnounceRequest` - Register peer with capabilities
- `OfferRequest` - Send SDP offer to peer
- `AnswerRequest` - Send SDP answer to peer
- `IceCandidateRequest` - Exchange ICE candidates
- `DisconnectRequest` - Notify peer disconnection
- `ListPeersRequest` - Get list of connected peers

**Response Types:**
- `AckResponse` - Acknowledgment of successful request
- `PeerListResponse` - List of connected peers
- `SignalingNotification` - Server-initiated notifications
- `ErrorResponse` - Error details

#### 2. GetPeers (Unary)

One-time query for connected peers without establishing a stream.

```protobuf
rpc GetPeers(GetPeersRequest) returns (GetPeersResponse);
```

#### 3. HealthCheck (Unary)

Service health monitoring.

```protobuf
rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
```

## Usage

### Server Setup

The gRPC signaling service is integrated into the existing gRPC server:

```rust
use remotemedia_grpc::{
    GrpcServer,
    ServiceConfig,
    webrtc_signaling::WebRtcSignalingService,
};
use remotemedia_runtime_core::transport::PipelineRunner;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load config
    let config = ServiceConfig::from_env();

    // Create pipeline runner
    let runner = Arc::new(PipelineRunner::new()?);

    // Create gRPC server with WebRTC signaling
    let server = GrpcServer::new_with_signaling(config, runner)?;

    // Start server
    server.serve().await?;

    Ok(())
}
```

### Client Usage (Rust)

```rust
use remotemedia_grpc::generated::webrtc::{
    web_rtc_signaling_client::WebRtcSignalingClient,
    SignalingRequest,
    AnnounceRequest,
    PeerCapabilities,
    signaling_request,
};
use tonic::Request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to server
    let mut client = WebRtcSignalingClient::connect("http://localhost:50051").await?;

    // Open bidirectional stream
    let (tx, rx) = tokio::sync::mpsc::channel(128);

    let outbound = async_stream::stream! {
        // Announce peer
        let announce = SignalingRequest {
            request_id: "1".to_string(),
            request: Some(signaling_request::Request::Announce(AnnounceRequest {
                peer_id: "rust-client-1".to_string(),
                capabilities: Some(PeerCapabilities {
                    audio: true,
                    video: true,
                    data: true,
                    extensions: String::new(),
                }),
                metadata: Default::default(),
            })),
        };

        yield announce;

        // Wait for more messages from application
        while let Some(msg) = rx.recv().await {
            yield msg;
        }
    };

    let mut response_stream = client.signal(Request::new(outbound)).await?.into_inner();

    // Handle incoming messages
    while let Some(response) = response_stream.message().await? {
        println!("Received: {:?}", response);
    }

    Ok(())
}
```

### Client Usage (Python with grpcio)

```python
import grpc
import remotemedia_pb2
import remotemedia_pb2_grpc

async def main():
    # Connect to server
    async with grpc.aio.insecure_channel('localhost:50051') as channel:
        stub = remotemedia_pb2_grpc.WebRtcSignalingStub(channel)

        # Create request stream
        async def request_iterator():
            # Announce peer
            yield remotemedia_pb2.SignalingRequest(
                request_id="1",
                announce=remotemedia_pb2.AnnounceRequest(
                    peer_id="python-client-1",
                    capabilities=remotemedia_pb2.PeerCapabilities(
                        audio=True,
                        video=True,
                        data=True
                    )
                )
            )

            # Keep stream open for more messages
            # (application would yield more requests here)

        # Open bidirectional stream
        response_stream = stub.Signal(request_iterator())

        # Handle responses
        async for response in response_stream:
            print(f"Received: {response}")

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
```

### Client Usage (JavaScript/TypeScript with @grpc/grpc-js)

```typescript
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';

// Load proto definition
const packageDefinition = protoLoader.loadSync('webrtc_signaling.proto', {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});

const signalingProto = grpc.loadPackageDefinition(packageDefinition);

// Connect to server
const client = new signalingProto.remotemedia.v1.webrtc.WebRtcSignaling(
  'localhost:50051',
  grpc.credentials.createInsecure()
);

// Open bidirectional stream
const call = client.signal();

// Handle incoming messages
call.on('data', (response) => {
  console.log('Received:', response);

  if (response.notification?.offer) {
    // Handle offer
    const offer = response.notification.offer;
    console.log(`Received offer from ${offer.from_peer_id}`);

    // Send answer
    call.write({
      request_id: '2',
      answer: {
        to_peer_id: offer.from_peer_id,
        sdp: 'answer-sdp-here',
        type: 'answer'
      }
    });
  }
});

call.on('end', () => {
  console.log('Stream ended');
});

call.on('error', (error) => {
  console.error('Stream error:', error);
});

// Announce peer
call.write({
  request_id: '1',
  announce: {
    peer_id: 'js-client-1',
    capabilities: {
      audio: true,
      video: true,
      data: true
    }
  }
});
```

## Message Flow Examples

### Peer Connection Establishment

```
Client A                        Server                        Client B
   |                              |                              |
   |--- Announce(peer-A) -------->|                              |
   |<-- Ack(peers_count: 0) ------|                              |
   |                              |<--- Announce(peer-B) ---------|
   |<-- PeerJoined(peer-B) -------|                              |
   |                              |--- PeerJoined(peer-A) ------>|
   |                              |                              |
   |--- Offer(to: peer-B, SDP) -->|                              |
   |<-- Ack(offer_id) ------------|                              |
   |                              |--- Offer(from: peer-A) ------>|
   |                              |                              |
   |                              |<-- Answer(to: peer-A, SDP) ---|
   |<-- Answer(from: peer-B) -----|                              |
   |                              |--- Ack() -------------------->|
   |                              |                              |
   |--- ICE(to: peer-B) --------->|                              |
   |                              |--- ICE(from: peer-A) -------->|
   |<-- ICE(from: peer-B) --------|<-- ICE(to: peer-A) -----------|
   |                              |                              |
   [WebRTC Connection Established]
```

## Comparison: gRPC vs WebSocket Signaling

| Feature | gRPC Signaling | WebSocket JSON-RPC 2.0 |
|---------|----------------|------------------------|
| **Protocol** | HTTP/2 + Protobuf | WebSocket + JSON |
| **Type Safety** | ✅ Compile-time | ❌ Runtime only |
| **Auth** | ✅ gRPC metadata/interceptors | ⚠️ Custom implementation |
| **Load Balancing** | ✅ Standard gRPC LB | ⚠️ Requires sticky sessions |
| **Compression** | ✅ HTTP/2 built-in | ⚠️ Per-message compression |
| **Firewall** | ✅ Standard HTTP/2 port | ⚠️ May be blocked |
| **Browser Support** | ✅ gRPC-Web | ✅ Native WebSocket |
| **Message Size** | ⚠️ Protobuf (binary, smaller) | ⚠️ JSON (text, larger) |
| **Infrastructure** | ✅ Same as pipeline gRPC | ❌ Separate server |
| **Debugging** | ⚠️ Binary (tools needed) | ✅ Human-readable JSON |

## Integration with Pipeline Execution

The gRPC signaling service shares the same server and infrastructure as pipeline execution:

```
┌─────────────────────────────────────────────────────────┐
│  Single gRPC Server (port 50051)                        │
│  ├─ Auth Middleware (shared API tokens)                │
│  ├─ Metrics Middleware (Prometheus, shared)            │
│  ├─ Logging Middleware (tracing, shared)               │
│  │                                                       │
│  ├─ PipelineExecutionService                           │
│  │  └─ ExecutePipeline(manifest, input)                │
│  │                                                       │
│  ├─ StreamingPipelineService                           │
│  │  └─ StreamPipeline(manifest, input_stream)          │
│  │                                                       │
│  └─ WebRtcSignalingService ← NEW                       │
│     ├─ Signal(bidirectional_stream)                    │
│     ├─ GetPeers()                                       │
│     └─ HealthCheck()                                    │
└─────────────────────────────────────────────────────────┘
```

**Benefits:**
1. **Single Port**: Only need to expose port 50051
2. **Shared Auth**: Same API tokens for signaling and pipeline execution
3. **Unified Metrics**: Combined Prometheus metrics for monitoring
4. **Simplified Deployment**: One server to deploy and manage
5. **Consistent Logging**: All logs in same format and destination

## Configuration

### Environment Variables

The gRPC signaling service uses the same configuration as the gRPC server:

```bash
# Server address
GRPC_BIND_ADDRESS="0.0.0.0:50051"

# Authentication
GRPC_REQUIRE_AUTH=true
GRPC_AUTH_TOKENS="token1,token2,token3"

# Resource limits
GRPC_MAX_MEMORY_MB=200
GRPC_MAX_TIMEOUT_SEC=10

# Logging
GRPC_JSON_LOGGING=true
RUST_LOG=info
```

### Starting the Server

```bash
# Build server with WebRTC signaling support
cd transports/grpc
cargo build --bin grpc-server --release

# Run server
GRPC_BIND_ADDRESS="0.0.0.0:50051" \
GRPC_AUTH_TOKENS="your-api-token" \
./target/release/grpc-server
```

## Testing

### Using grpcurl

```bash
# Health check
grpcurl -plaintext localhost:50051 \
  remotemedia.v1.webrtc.WebRtcSignaling/HealthCheck

# Get peers (unary)
grpcurl -plaintext localhost:50051 \
  remotemedia.v1.webrtc.WebRtcSignaling/GetPeers

# Bidirectional stream (requires input file)
grpcurl -plaintext -d @ localhost:50051 \
  remotemedia.v1.webrtc.WebRtcSignaling/Signal <<EOF
{
  "request_id": "1",
  "announce": {
    "peer_id": "test-peer-1",
    "capabilities": {
      "audio": true,
      "video": true,
      "data": true
    }
  }
}
EOF
```

### Unit Tests

```bash
cd transports/grpc
cargo test webrtc_signaling
```

## Security Considerations

### Authentication

The gRPC signaling service uses the same authentication middleware as pipeline execution:

```rust
// Client includes API token in metadata
let mut request = Request::new(stream);
request.metadata_mut().insert(
    "authorization",
    format!("Bearer {}", api_token).parse().unwrap()
);

let response = client.signal(request).await?;
```

### Authorization

Peer IDs should be validated against authenticated user:

```rust
// Example: Ensure peer_id matches authenticated user
if announce.peer_id != authenticated_user_id {
    return Err(Status::permission_denied("Peer ID mismatch"));
}
```

### Rate Limiting

Consider adding rate limiting for signaling messages:

```rust
// Example using tower-governor
use tower_governor::{governor::GovernorConfigBuilder, GovernorLayer};

let governor_conf = GovernorConfigBuilder::default()
    .per_second(10)
    .burst_size(50)
    .finish()
    .unwrap();

let server = Server::builder()
    .layer(GovernorLayer { config: &governor_conf })
    .add_service(signaling_service)
    .serve(addr);
```

## Production Deployment

### Docker

```dockerfile
FROM rust:1.87 as builder

WORKDIR /app
COPY . .

RUN cargo build --release --bin grpc-server

FROM debian:bookworm-slim

COPY --from=builder /app/target/release/grpc-server /usr/local/bin/

EXPOSE 50051

CMD ["grpc-server"]
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: remotemedia-grpc
spec:
  replicas: 3
  selector:
    matchLabels:
      app: remotemedia-grpc
  template:
    metadata:
      labels:
        app: remotemedia-grpc
    spec:
      containers:
      - name: grpc-server
        image: remotemedia-grpc:latest
        env:
        - name: GRPC_BIND_ADDRESS
          value: "0.0.0.0:50051"
        - name: GRPC_AUTH_TOKENS
          valueFrom:
            secretKeyRef:
              name: grpc-secrets
              key: api-tokens
        ports:
        - containerPort: 50051
          protocol: TCP
---
apiVersion: v1
kind: Service
metadata:
  name: remotemedia-grpc
spec:
  type: LoadBalancer
  selector:
    app: remotemedia-grpc
  ports:
  - protocol: TCP
    port: 50051
    targetPort: 50051
```

## Monitoring

### Prometheus Metrics

The gRPC signaling service automatically exports Prometheus metrics:

```
# Connected peers
grpc_webrtc_signaling_peers_total 5

# Active streams
grpc_webrtc_signaling_streams_active 5

# Messages processed
grpc_webrtc_signaling_messages_total{type="offer"} 120
grpc_webrtc_signaling_messages_total{type="answer"} 115
grpc_webrtc_signaling_messages_total{type="ice_candidate"} 450

# Pending offers
grpc_webrtc_signaling_pending_offers 2
```

### Health Checks

```bash
# gRPC health check
grpcurl -plaintext localhost:50051 \
  remotemedia.v1.webrtc.WebRtcSignaling/HealthCheck

# Kubernetes liveness probe
livenessProbe:
  exec:
    command:
    - grpc-health-probe
    - -addr=:50051
    - -service=remotemedia.v1.webrtc.WebRtcSignaling
  initialDelaySeconds: 5
```

## Troubleshooting

### Connection Refused

```
Error: transport error: Connection refused
```

**Solution:** Ensure gRPC server is running on the specified port:
```bash
netstat -tlnp | grep 50051
```

### Authentication Failed

```
Error: status: Unauthenticated, message: "Invalid or missing API token"
```

**Solution:** Include API token in metadata:
```rust
request.metadata_mut().insert("authorization", token.parse()?);
```

### Peer Not Found

```
Error: status: NotFound, message: "Peer not found: peer-xyz"
```

**Solution:** Ensure target peer has called `announce` first.

## Related Documentation

- [gRPC Server Documentation](README.md)
- [WebSocket Signaling Server](../remotemedia-webrtc/examples/signaling_server/README.md)
- [WebRTC Transport Integration](../remotemedia-webrtc/INTEGRATION.md)
- [Protocol Buffers](protos/webrtc_signaling.proto)

## License

MIT OR Apache-2.0 (same as parent project)
