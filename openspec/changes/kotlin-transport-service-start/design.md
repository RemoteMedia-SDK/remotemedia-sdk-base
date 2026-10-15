## Context

The Kotlin Android library (`kotlin-android`) offers an in-process JNI bridge to RemoteMedia pipeline execution. Separately, `crates/transports/grpc` already provides a standalone gRPC server that exposes pipeline execution plus streaming/control services. There is no Android-side API to control that transport from an app, so voice assistant apps can’t currently expose a remote control/execution surface to other clients without building ad-hoc Rust glue.

## Goals / Non-Goals

Goals:
- Add a minimal, typed Kotlin API for starting/stopping the gRPC transport from an Android app through the existing native bridge.
- Keep gRPC server logic in Rust; Android only passes configuration and lifecycle commands.
- Preserve current in-process behavior when the transport is not started.

Non-Goals:
- Expose all of `remotemedia-grpc` configuration in Kotlin in the first pass.
- Replace the in-process execution path with gRPC by default.
- Add WebRTC or other transports beyond gRPC in this change.

## Decisions

### 1. Kotlin owns config; Rust owns server lifecycle
Kotlin defines simple config models (`bindAddress`, `requireAuth`, `authTokens`, `maxMemoryMb`, `maxTimeoutSecs`). A new `PipelineManager` extension starts the service via `nativeStartGrpcService(...)` and stops it via `nativeStopGrpcService(...)`. Native code builds/runs the existing `GrpcServerBuilder` using the same semantics already used by the Rust CLI path.

Rationale:
- gRPC already has a complete, well-tested builder/server; duplicating it in Kotlin would drift.
- The app process cannot safely own the gRPC server because Rust already runs the executor/tokio runtime.

### 2. Config serialization via JSON over JNI
The new JNI APIs pass a small JSON string for optional config. This matches the existing convention in the codebase and avoids breaking binary compatibility by adding many new primitive JNI methods later.

Accepted shape in Rust:
```rust
GrpcServiceConfig { bind_address: String, require_auth: bool, auth_tokens: Vec<String>, max_memory_mb: u64, max_timeout_secs: u64 }
```

If a field is missing/empty on the Kotlin side, Rust uses its existing `ServiceConfig::default()` behavior.

### 3. State reporting via existing callbacks
Starting/stopping the transport emits state through the same `onStateChange` hook used for pipelines. No new callback mechanism is introduced in this phase.

### 4. Extend the example in-process native host, not publish a new Rust crate
Because this feature requires new native functions exposed to an Android app, the implementation lives in `examples/android-inprocess`’s Rust library. This avoids forcing a new published crate version before the API is validated on device.

## Sequence

1. Kotlin adds `GrpcServiceConfig` and `startGrpcService`/`stopGrpcService` on `PipelineManager`.
2. NativeInterface adds JNI declarations for the new methods.
3. `examples/android-inprocess/src/lib.rs` implements the JNI functions using `remotemedia_grpc::{GrpcServerBuilder, ServiceConfig}` and stores the running server handle in the executor state for later shutdown.
4. `AudioProcessingService` and `RemoteMediaApplication` are left unchanged; use of the new API is opt-in.

## Error Handling

- JNI functions return `Boolean` for start/stop to indicate feasibility where failure is non-recoverable from Kotlin (e.g., already running, executor not created).
- Detailed errors are logged on the Rust side with `tracing`; Kotlin receives `false` and can read logs or later versions can return error strings.
- `stopGrpcService` is idempotent.

## Platform Notes

- Kotlin code runs on the main/UI thread for API entry, but each suspend function uses `Dispatchers.IO` for native calls.
- gRPC server runs on tokio in the same process as the existing executor. Threading model remains unchanged.
- The existing `android_build_deploy_test.sh` path puts this feature behind Rust toolchain availability, which is already required for the in-process example.
