# remotemedia-http

HTTP/REST transport for RemoteMedia pipelines with Server-Sent Events (SSE) streaming support.

## Features

- **Unary Execution**: Simple request/response via `POST /execute`
- **Streaming Sessions**: Create persistent sessions via `POST /stream`
- **SSE Output**: Continuous output streaming via `GET /stream/:id/output`
- **Input Submission**: Send inputs via `POST /stream/:id/input`
- **Health Checks**: Monitor server health via `GET /health`
- **Multiple SSE Subscribers**: Each session supports multiple concurrent SSE connections

## Architecture

### Client-Side

The HTTP client implements the `PipelineClient` trait and provides:
- Unary execution with `execute_unary()`
- Streaming sessions with `create_stream_session()`
- Health checks with `health_check()`

### Server-Side

The HTTP server implements the `PipelineTransport` trait and exposes:

**Endpoints:**
- `GET /health` - Health check
- `POST /execute` - Unary pipeline execution
- `POST /stream` - Create streaming session (returns session_id)
- `POST /stream/:session_id/input` - Send input data to session
- `GET /stream/:session_id/output` - SSE stream of output data
- `DELETE /stream/:session_id` - Close streaming session

**SSE Implementation:**
- Uses `tokio::sync::broadcast` channel for multiple subscribers
- Each session can have multiple concurrent SSE connections
- Handles lagged subscribers gracefully (logs warning and skips)
- Keeps alive with periodic heartbeat

## Usage

### Client Example

```rust
use remotemedia_http::HttpPipelineClient;
use remotemedia_runtime_core::transport::PipelineClient;

// Create client
let client = HttpPipelineClient::new("http://localhost:8080", None).await?;

// Unary execution
let output = client.execute_unary(manifest, input).await?;

// Streaming session
let mut session = client.create_stream_session(manifest).await?;
session.send(input).await?;
while let Some(output) = session.receive().await? {
    // Process output
}
session.close().await?;
```

### Server Example

```rust
use remotemedia_http::HttpServer;
use remotemedia_runtime_core::transport::PipelineRunner;
use std::sync::Arc;

// Create pipeline runner
let runner = Arc::new(PipelineRunner::new()?);

// Create HTTP server
let server = HttpServer::new("127.0.0.1:8080".to_string(), runner).await?;

// Run server
server.serve().await?;
```

### Using the Server Binary

```bash
# Start HTTP server with defaults (127.0.0.1:8080)
cargo run -p remotemedia-http-server

# Start with custom address
HTTP_BIND_ADDRESS="0.0.0.0:8080" cargo run -p remotemedia-http-server

# With logging
RUST_LOG=debug cargo run -p remotemedia-http-server
```

The server binary is located in `crates/services/http-server/`.

## Plugin Registration

```rust
use remotemedia_http::HttpTransportPlugin;
use remotemedia_runtime_core::transport::TransportPluginRegistry;

let mut registry = TransportPluginRegistry::new();
registry.register(Box::new(HttpTransportPlugin));

// Create client via registry
let config = ClientConfig {
    address: "http://localhost:8080".to_string(),
    auth_token: None,
    timeout_ms: None,
    extra_config: None,
};
let client = registry.create_client("http", &config).await?;
```

## SSE Message Format

Server-Sent Events use the following format:

```
data: {"data":{"Text":"hello"},"sequence":1,"metadata":{}}

data: {"data":{"Audio":{"samples":[0.1,0.2],"sample_rate":16000,"channels":1,"timestamp":0}},"sequence":2,"metadata":{}}
```

Each event is prefixed with `data: ` and contains a JSON-serialized `TransportData` object.

## Differences from gRPC Transport

| Feature | HTTP/SSE | gRPC |
|---------|----------|------|
| Protocol | HTTP/1.1 or HTTP/2 | HTTP/2 only |
| Streaming | SSE (server→client), POST (client→server) | Bidirectional |
| Browser Support | ✅ Native | ❌ Requires grpc-web |
| Binary Efficiency | JSON | Protobuf |
| Connection Overhead | Higher (separate connections) | Lower (multiplexed) |
| Firewall Friendly | ✅ Standard HTTP | ⚠️ May need proxy |

## Performance Considerations

- **Latency**: Higher than gRPC due to JSON serialization and separate HTTP requests for inputs
- **Throughput**: Lower than gRPC for high-frequency streaming
- **Browser Compatibility**: Best option for web clients without WebRTC
- **Scalability**: Each SSE connection is a separate stream; consider connection limits

## Best Use Cases

- ✅ Web dashboards and monitoring interfaces
- ✅ Low-frequency streaming (< 10 messages/sec)
- ✅ Browser-based clients without WebRTC support
- ✅ Debugging and development (easy to test with curl)
- ❌ High-frequency audio streaming (use gRPC or WebRTC)
- ❌ Bidirectional real-time communication (use WebRTC)

## Testing with curl

```bash
# Health check
curl http://localhost:8080/health

# Unary execution
curl -X POST http://localhost:8080/execute \
  -H "Content-Type: application/json" \
  -d '{
    "manifest": {...},
    "input": {"data":{"Text":"hello"},"sequence":null,"metadata":{}}
  }'

# Create streaming session
SESSION_ID=$(curl -X POST http://localhost:8080/stream \
  -H "Content-Type: application/json" \
  -d '{"manifest":{...}}' | jq -r '.session_id')

# Send input
curl -X POST http://localhost:8080/stream/$SESSION_ID/input \
  -H "Content-Type: application/json" \
  -d '{"data":{"Text":"hello"}}'

# Receive outputs (SSE stream)
curl -N http://localhost:8080/stream/$SESSION_ID/output

# Close session
curl -X DELETE http://localhost:8080/stream/$SESSION_ID
```

## License

MIT OR Apache-2.0
