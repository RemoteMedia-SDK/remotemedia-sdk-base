## ADDED Requirements

### Requirement: Kotlin API starts gRPC transport through native bridge
The Kotlin library SHALL provide `PipelineManager.startGrpcService(config)` and `PipelineManager.stopGrpcService()` for lifecycle control of the gRPC transport.

#### Scenario: Start gRPC with defaults
- **WHEN** `startGrpcService(GrpcServiceConfig())` is called on an initialized `PipelineManager`
- **THEN** the native side starts a `remotemedia-grpc` server bound to `0.0.0.0:50051` and the transition is reported via `onStateChange`

#### Scenario: Start gRPC with custom config
- **WHEN** `startGrpcService(GrpcServiceConfig(bindAddress="0.0.0.0:50052", authTokens=listOf("t1"), maxMemoryMb=256))` is called
- **THEN** the native gRPC server uses the provided values and returns success

#### Scenario: Stop gRPC
- **WHEN** `stopGrpcService()` is called while the service is running
- **THEN** the server shuts down and the service state returns to ready/idle

#### Scenario: Idempotent stop
- **WHEN** `stopGrpcService()` is called when no service is running
- **THEN** the call succeeds with no change in state
