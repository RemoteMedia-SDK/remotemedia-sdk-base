#!/usr/bin/env pwsh
# Build and run WebRTC server with gRPC signaling

Write-Host "Building WebRTC server..." -ForegroundColor Cyan
cargo build --bin webrtc_server --features "grpc-signaling"

if ($LASTEXITCODE -ne 0) {
    Write-Host "Build failed!" -ForegroundColor Red
    exit 1
}

Write-Host "Build succeeded!" -ForegroundColor Green
Write-Host ""

$env:WEBRTC_ENABLE_GRPC_SIGNALING = "true"
$env:GRPC_SIGNALING_ADDRESS = "0.0.0.0:50051"
$env:WEBRTC_PIPELINE_MANIFEST = "./examples/simple_docker_node.json"
$env:RUST_LOG = "info"

Write-Host "Starting WebRTC server with gRPC signaling on port 50051..." -ForegroundColor Green
Write-Host "Pipeline: $env:WEBRTC_PIPELINE_MANIFEST" -ForegroundColor Cyan
Write-Host "Press Ctrl+C to stop" -ForegroundColor Yellow
Write-Host ""

# ..\..\target\debug\webrtc_server.exe
cargo run --bin webrtc_server --features grpc-signaling -- --mode grpc --grpc-address 0.0.0.0:50051 --manifest "C:\Users\mail\dev\personal\remotemedia-sdk-webrtc\examples\docker-node\simple_docker_node.json"

