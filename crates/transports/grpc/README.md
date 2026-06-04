# remotemedia-grpc

gRPC transport implementation for RemoteMedia pipelines.

## Overview

This crate provides a gRPC service that exposes RemoteMedia pipeline execution via the `remotemedia-runtime-core` library. It implements the `PipelineTransport` trait and handles Protobuf ↔ RuntimeData conversion.

## Status

**Phase 4 (In Progress)**: Core files have been extracted from `runtime/src/grpc_service/`. Full integration with `PipelineRunner` pending.

### Completed
- ✅ Directory structure created
- ✅ Cargo.toml with runtime-core dependency
- ✅ gRPC service files copied from runtime
- ✅ Adapters for DataBuffer ↔ RuntimeData conversion
- ✅ Added to workspace

### Remaining
- ⏸️ Update StreamingServiceImpl to use PipelineRunner
- ⏸️ Update ExecutionServiceImpl to use PipelineRunner
- ⏸️ Integration tests
- ⏸️ Deployment examples

## Dependencies

- `remotemedia-runtime-core` - Core execution engine (NO transport deps)
- `tonic`, `prost` - gRPC implementation
- `tower`, `tower-http` - Middleware
- `prometheus` - Metrics

## Usage

### As a Library

```rust
use remotemedia_grpc::{GrpcServer, ServiceConfig};
use remotemedia_core::transport::PipelineExecutor;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServiceConfig::default();
    let executor = Arc::new(PipelineExecutor::new()?);
    let server = GrpcServer::new(config, executor)?;

    server.serve().await?;
    Ok(())
}
```

### Using the Server Binary

```bash
# Start gRPC server with defaults (localhost:50051)
cargo run -p remotemedia-grpc-server

# With custom address
GRPC_BIND_ADDRESS="0.0.0.0:50051" cargo run -p remotemedia-grpc-server

# With logging
RUST_LOG=debug cargo run -p remotemedia-grpc-server
```

The server binary is located in `crates/services/grpc-server/`.

## Architecture

```
┌─────────────────────────────────────┐
│  remotemedia-grpc (this crate)      │
│                                      │
│  ┌────────────────────────────────┐ │
│  │  StreamingService              │ │
│  │  ExecutionService              │ │
│  │  ↓                              │ │
│  │  Adapters (DataBuffer ↔ Data)  │ │
│  └────────────┬───────────────────┘ │
└───────────────┼─────────────────────┘
                │
                ▼
┌─────────────────────────────────────┐
│  remotemedia-runtime-core           │
│  ├─ PipelineRunner                  │
│  ├─ StreamSession                   │
│  └─ TransportData                   │
└─────────────────────────────────────┘
```

## Files

- `src/lib.rs` - Module exports and public API
- `src/server.rs` - Tonic server setup
- `src/streaming.rs` - Bidirectional streaming RPC
- `src/execution.rs` - Unary RPC
- `src/adapters.rs` - Protobuf conversion
- `src/auth.rs` - Authentication middleware
- `src/metrics.rs` - Prometheus metrics
- `src/session_router.rs` - Session routing logic
- `protos/` - Protocol buffer definitions
- `build.rs` - Protobuf code generation

## Documentation

- Architecture: `docs/TRANSPORT_DECOUPLING_ARCHITECTURE.md`
- Migration plan: `specs/003-transport-decoupling/plan.md`
- API contracts: `specs/003-transport-decoupling/contracts/`

## License

MIT OR Apache-2.0
