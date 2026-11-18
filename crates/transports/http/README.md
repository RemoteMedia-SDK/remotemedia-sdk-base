# remotemedia-http

HTTP/REST transport for RemoteMedia pipelines. Frames cross as protobuf
`remotemedia.v1.DataBuffer` (`proto/common.proto`, codec in `crates/wire`):
audio as raw f32 little-endian bytes, other bulk payloads as their own bytes.
No frame is ever JSON. Manifests (configuration) and error responses are JSON.

## Features

- **Unary Execution**: request/response via `POST /execute`
- **Streaming Sessions**: create persistent sessions via `POST /stream`
- **Streamed Output**: delimited protobuf events via `GET /stream/:id/output`
- **Input Submission**: send inputs via `POST /stream/:id/input`
- **Health Checks**: `GET /health`
- **Multiple Output Subscribers**: each session supports several concurrent output streams

## Wire format

| Endpoint | Body | Content type |
|----------|------|--------------|
| `POST /execute` | `ExecuteRequest` (below) | `application/x-protobuf` |
| `POST /execute` response | one `DataBuffer` | `application/x-protobuf` |
| `POST /stream` | `{"manifest": {...}}` | `application/json` |
| `POST /stream` response | `{"session_id": "..."}` | `application/json` |
| `POST /stream/:id/input` | one `DataBuffer` | `application/x-protobuf` |
| `GET /stream/:id/output` | delimited `StreamEvent`s | `application/x-protobuf; delimited=true` |
| errors (4xx/5xx) | `{"error_type", "message", "validation_errors"?}` | `application/json` |

```proto
message ExecuteRequest {
  string manifest_json = 1;              // the pipeline manifest
  remotemedia.v1.DataBuffer input = 2;
}

// The output stream is a sequence of StreamEvents, each preceded by its
// length as a varint (protobuf "length-delimited" framing).
message StreamEvent {
  oneof event {
    remotemedia.v1.DataBuffer frame = 1;
    string error = 2;                    // terminal: the stream ends
  }
}
// An empty event (a single 0x00 length byte) is a keepalive, sent every 15 s.
```

A frame's transport metadata (e.g. `participant.*`) travels in
`DataBuffer.metadata` and its sequence number in `DataBuffer.sequence`.
`DataBuffer` carries every `RuntimeData` variant losslessly, including stream
ids, timestamps, image encodings, tensor metadata and control messages.
JSON request bodies for frames are refused with `415 Unsupported Media Type`.

## Architecture

### Client-Side

The HTTP client implements the `PipelineClient` trait and provides:
- Unary execution with `execute_unary()`
- Streaming sessions with `create_stream_session()`
- Health checks with `health_check()`

### Copy accounting

The client owns an `AllowCopies` ledger by default; `client.copy_context()`
retains it for inspection. To bind your own authority, call
`client.with_copy_context(copies.clone())` before making requests. Stream handles
retain that exact authority after the client is dropped. The supplied context
covers all calls made through that client; give independent clients distinct
contexts when separate accounting is required.

Encoding a frame counts its bulk bytes as `PortableSerialization` and is refused
under `RequireZeroCopy` before any payload is read. Decoding counts only audio
samples (`PortableDeserialization`, the conversion to aligned `f32`s); video,
image, tensor, numpy and binary payloads keep the received network buffer as
their storage, so a strict context accepts them. Request and response bodies
keep their network chunks: a payload inside one chunk is a view of it, and only
a field split across chunks is assembled (codec framing, not counted). On the
output stream, an event split across chunks is assembled once and counted as
`VecMaterialization`, which a strict context refuses. Counts exclude metadata
allocations and HTTP/TLS/kernel transfers. This API does not negotiate or certify
zero-copy execution remotely. The transport plugin accepts
`extra_config: {"copy_policy":"require_zero_copy"}` (or `"allow_copies"`);
invalid policy values are rejected.

### Timeouts

Creating a session (`POST /stream`) and a unary run (`POST /execute`) are
answered only once the server has built the pipeline, which for a cold pipeline
means loading its models: these wait up to 10 minutes by default. Every other
request (sending an input, opening the output stream, closing a session) is
bounded by 30 s, and connecting by 10 s. Change them with
`client.with_timeouts(HttpTimeouts { session_setup, request })`
(`session_setup: None` waits indefinitely), or through the plugin:
`ClientConfig.timeout_ms` sets the per-request limit and
`extra_config: {"session_setup_timeout_ms": N}` the setup limit (0 = none).
A request that runs out of time says which limit it hit.

Output events are limited to 16 MiB. Malformed or truncated events, an output
stream with another content type, and explicit error events are returned by
`receive()`. Session creation waits for the output subscription to connect, and a
failed subscription attempts remote cleanup. `close()` aborts and joins local
receive work, releases queued outputs and requests remote deletion; deletion
errors are returned. Dropping a stream aborts local receive work but does not
perform an asynchronous remote deletion.

### Server-Side

The HTTP server implements the `PipelineTransport` trait and exposes:

**Endpoints:**
- `GET /health` - Health check
- `POST /execute` - Unary pipeline execution
- `POST /stream` - Create streaming session (returns session_id)
- `POST /stream/:session_id/input` - Send input data to session
- `GET /stream/:session_id/output` - Stream of output events
- `DELETE /stream/:session_id` - Close streaming session

**Output streaming:**
- Uses `tokio::sync::broadcast` for multiple subscribers
- Each session can have multiple concurrent output connections
- Reports lagged subscribers with a terminal error event instead of silently skipping
- Keeps alive with a periodic empty event

HTTP handlers use each core session's `transport_materialization_policy` for
frame decoding and encoding. Unary requests decode the manifest first, then
convert the input under the newly created session's authority. Request bodies
retain chunks, with a 2 MiB limit. `session_copy_context(id)` exposes active stream
accounting; `with_copy_observer` lets the host retain unary and streaming contexts
after closure. The synchronous observer runs before bulk conversion and must
return promptly; retaining or bounding report history is the host's responsibility.

An independent task drains pipeline outputs. Each output is encoded once into
immutable wire storage and broadcast by ownership, with capacity for 100 outputs.
Each event is written as a small header chunk followed by the encoded frame
itself, never recopied. Input handlers never wait for output while holding the
session map lock. Explicit deletion stops and joins the drainer and core session;
cancellation of a unary handler schedules the same cleanup. Disconnecting an
output stream alone does not delete a remotely owned session.

Encoding failures and observed core `node_error` events terminate the HTTP
session and reach output consumers as error events. The error subscription is
installed before HTTP input is accepted; it does not replay ephemeral errors
emitted before attachment by an autonomous source. The core's general node-error
policy is unchanged.

## Usage

### Client Example

```rust
use remotemedia_http::HttpPipelineClient;
use remotemedia_core::transport::PipelineClient;

// Create client
let client = HttpPipelineClient::new("http://localhost:8080", None).await?;

// Unary execution
let output = client.execute_unary(manifest, input, &[], None).await?;

// Streaming session
let mut session = client.create_stream_session(manifest, &[], None).await?;
session.send(input).await?;
while let Some(output) = session.receive().await? {
    // Process output
}
session.close().await?;
```

### Server Example

```rust
use remotemedia_http::HttpServer;
use remotemedia_core::transport::PipelineExecutor;
use std::sync::Arc;

let executor = Arc::new(PipelineExecutor::new()?);
let server = HttpServer::new("127.0.0.1:8080".to_string(), executor).await?;
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
use remotemedia_core::transport::TransportPluginRegistry;

let mut registry = TransportPluginRegistry::new();
registry.register(Arc::new(HttpTransportPlugin));

// Create client via registry
let config = ClientConfig {
    address: "http://localhost:8080".to_string(),
    auth_token: None,
    timeout_ms: None,
    extra_config: None,
};
let client = registry.create_client("http", &config).await?;
```

## Differences from gRPC Transport

| Feature | HTTP | gRPC |
|---------|------|------|
| Protocol | HTTP/1.1 or HTTP/2 | HTTP/2 only |
| Streaming | delimited protobuf (server→client), POST (client→server) | Bidirectional |
| Frame encoding | protobuf `DataBuffer` | protobuf `DataBuffer` |
| Connection Overhead | Higher (separate requests for inputs) | Lower (multiplexed) |
| Firewall Friendly | ✅ Standard HTTP | ⚠️ May need proxy |

## Performance Considerations

- **Latency**: higher than gRPC because each input is a separate HTTP request
- **Throughput**: lower than gRPC for high-frequency streaming
- **Scalability**: each output stream is a separate connection; consider connection limits

## Best Use Cases

- ✅ Environments where only plain HTTP is allowed
- ✅ Low-frequency streaming
- ❌ High-frequency audio streaming (use gRPC or WebRTC)
- ❌ Bidirectional real-time communication (use WebRTC)

## Testing by hand

```bash
# Health check
curl http://localhost:8080/health

# Create streaming session (the manifest is JSON configuration)
SESSION_ID=$(curl -X POST http://localhost:8080/stream \
  -H "Content-Type: application/json" \
  -d '{"manifest":{...}}' | jq -r '.session_id')

# Send input: a binary DataBuffer, e.g. written by remotemedia_wire::encode_frame
curl -X POST http://localhost:8080/stream/$SESSION_ID/input \
  -H "Content-Type: application/x-protobuf" \
  --data-binary @frame.bin

# Receive outputs (binary, length-delimited StreamEvents)
curl -N http://localhost:8080/stream/$SESSION_ID/output -o outputs.bin

# Close session
curl -X DELETE http://localhost:8080/stream/$SESSION_ID
```

## License

MIT OR Apache-2.0
