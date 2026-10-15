## Why

The Kotlin Android library currently only supports in-process pipeline execution via JNI into `libremotemedia_android_inprocess.so`. That covers local/offline use cases, but the same RemoteMedia SDK already exposes first-class remote transports — beginning with gRPC in `crates/transports/grpc`. Consumers of the Kotlin Android library have no way to start, stop, or manage those services from the app layer.

This change adds a transport/service-start layer to `kotlin-android` so an Android app can launch a `remotemedia-grpc` server (and future transports) without leaving the Kotlin API.

## What Changes

- New capability: `kotlin-android-transport-start` — start/stop remote transport services from `kotlin-android`
- New API surface in Kotlin:
  - `RemoteMediaService` sealed class / config model
  - `startGrpcService(...)` and `stopGrpcService(...)` on `PipelineManager`
  - Service lifecycle + state exposed via existing `PipelineState` callbacks
- New Rust JNI bindings on the in-process example/crate:
  - `nativeStartGrpcService(executorHandle, bindAddress, requireAuth, authTokensJson) -> Boolean`
  - `nativeStopGrpcService(executorHandle) -> Boolean`
- The Kotlin layer maps config into the existing `remotemedia_grpc::GrpcServerBuilder` equivalents via JNI rather than embedding gRPC in the app process directly.

## Capabilities

### New Capabilities
- `kotlin-android-transport-start`: Start/stop a `remotemedia-grpc` transport server from the Android app
- `kotlin-android-transport-config`: Typed Kotlin config classes for gRPC bind address, auth tokens, resource limits

### Modified Capabilities
- `android-pipeline-management`: `PipelineManager` gains transport lifecycle methods
- `android-native-bindings` / example JNI host: new native functions for gRPC server control

## Impact

- Kotlin library API extension only; existing in-process flow is unchanged
- Rust-side in-process example is extended with new JNI entrypoints
- No new Gradle/Maven dependencies for consumers; gRPC logic stays in Rust
