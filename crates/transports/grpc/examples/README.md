# RemoteMedia gRPC Examples

This directory contains examples demonstrating how to use the `remotemedia-grpc` transport crate.

## Examples

### Simple Server (`simple_server.rs`)

A basic gRPC server that demonstrates the minimal setup required to run a RemoteMedia pipeline server.

**Run:**
```bash
cargo run --example simple_server --package remotemedia-grpc
```

**Features:**
- Environment-based configuration
- PipelineRunner initialization
- Basic server setup

**Environment Variables:**
- `GRPC_BIND_ADDRESS` - Server bind address (default: "0.0.0.0:50051")
- `GRPC_REQUIRE_AUTH` - Enable authentication (default: false)
- `GRPC_JSON_LOGGING` - Enable JSON logging (default: true)
- `RUST_LOG` - Logging level (default: "info")

### Simple Client (`simple_client.rs`)

A basic gRPC client that connects to the server and executes a simple pass-through pipeline.

**Run:**
```bash
# First, start the server
cargo run --example simple_server --package remotemedia-grpc

# In another terminal, run the client
cargo run --example simple_client --package remotemedia-grpc
```

**Features:**
- Client connection setup
- Pipeline manifest creation
- Audio data buffer creation
- ExecutePipeline RPC invocation

**Environment Variables:**
- `GRPC_SERVER_ADDR` - Server address (default: "http://[::1]:50051")

## Architecture

These examples demonstrate the new decoupled transport architecture introduced in v0.4:

```rust
// Old (v0.3.x - monolithic)
use remotemedia_runtime::grpc_service::GrpcServer;
use remotemedia_runtime::executor::Executor;

// New (v0.4.x - decoupled)
use remotemedia_grpc::GrpcServer;
use remotemedia_runtime_core::transport::PipelineRunner;
```

### Key Changes

1. **PipelineRunner** - Encapsulates executor and node registries
   - No manual node registration needed
   - All nodes initialized internally
   - Clean separation of concerns

2. **ServiceConfig** - Environment-based configuration
   - `ServiceConfig::from_env()` reads from environment
   - Simplified server setup

3. **Separate Crates** - Transport independence
   - `remotemedia-runtime-core` - Core execution engine (no transport deps)
   - `remotemedia-grpc` - gRPC transport implementation
   - Each builds independently

## For More Information

- **Quickstart Guide**: `../specs/003-transport-decoupling/quickstart.md`
- **API Documentation**: `cargo doc --open --package remotemedia-grpc`
- **Integration Tests**: `../runtime/tests/grpc_integration/`

## Migration from v0.3

If you have existing code using the old monolithic structure:

```rust
// OLD (v0.3.x):
use remotemedia_runtime::grpc_service::GrpcServer;
use remotemedia_runtime::executor::Executor;

let mut executor = Executor::new();
// ... manual node registration ...
let executor = Arc::new(executor);
let server = GrpcServer::new(config, executor)?;

// NEW (v0.4.x):
use remotemedia_grpc::GrpcServer;
use remotemedia_runtime_core::transport::PipelineRunner;

let runner = PipelineRunner::new()?;  // Nodes registered automatically
let runner = Arc::new(runner);
let server = GrpcServer::new(config, runner)?;
```

See the backward compatibility section in `runtime-core/src/lib.rs` for gradual migration paths.
