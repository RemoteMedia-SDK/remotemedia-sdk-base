#!/usr/bin/env bash
# Build and run WebRTC server with gRPC signaling, multiprocessing, and Docker support
#
# Features enabled:
# - gRPC signaling (grpc-signaling)
# - Multiprocess Python nodes (multiprocess - default in runtime-core)
# - Docker executor support (docker - default in runtime-core)
# - Silero VAD (silero-vad - default in runtime-core)

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Default configuration
BUILD_MODE="${BUILD_MODE:-release}"
GRPC_ADDRESS="${GRPC_ADDRESS:-0.0.0.0:50051}"
MANIFEST="${MANIFEST:-./examples/tts.json}"
RUST_LOG="${RUST_LOG:-info,remotemedia=info}"
MAX_PEERS="${MAX_PEERS:-10}"
STUN_SERVERS="${STUN_SERVERS:-stun:stun.l.google.com:19302}"

# Parse command line arguments
SKIP_BUILD=false
SHOW_HELP=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --release)
            BUILD_MODE="release"
            shift
            ;;
        --debug)
            BUILD_MODE="debug"
            shift
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --grpc-address)
            GRPC_ADDRESS="$2"
            shift 2
            ;;
        --manifest)
            MANIFEST="$2"
            shift 2
            ;;
        --max-peers)
            MAX_PEERS="$2"
            shift 2
            ;;
        --stun-servers)
            STUN_SERVERS="$2"
            shift 2
            ;;
        --log-level)
            RUST_LOG="$2"
            shift 2
            ;;
        -h|--help)
            SHOW_HELP=true
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            SHOW_HELP=true
            shift
            ;;
    esac
done

# Show help if requested
if [ "$SHOW_HELP" = true ]; then
    cat << EOF
${CYAN}WebRTC Server with gRPC Signaling${NC}

Build and run the RemoteMedia WebRTC server with full feature support:
- gRPC bidirectional streaming signaling
- Multiprocess Python node execution (iceoryx2 IPC)
- Docker container executor
- Native Rust audio nodes (Silero VAD)

${YELLOW}Usage:${NC}
  ./run_grpc_server.sh [OPTIONS]

${YELLOW}Options:${NC}
  --release              Build in release mode (optimized, slower compile)
  --debug                Build in debug mode (default, faster compile)
  --skip-build           Skip the build step (use existing binary)
  --grpc-address ADDR    gRPC server address (default: 0.0.0.0:50051)
  --manifest PATH        Pipeline manifest path (default: ./examples/docker-node/simple_docker_node.json)
  --max-peers N          Maximum concurrent WebRTC peers (default: 10)
  --stun-servers URLS    STUN server URLs, comma-separated (default: stun:stun.l.google.com:19302)
  --log-level LEVEL      Rust log level (default: info,remotemedia=debug)
  -h, --help             Show this help message

${YELLOW}Environment Variables:${NC}
  BUILD_MODE             Build mode: debug or release
  GRPC_ADDRESS           gRPC server address
  MANIFEST               Pipeline manifest file path
  RUST_LOG               Logging level configuration
  MAX_PEERS              Maximum concurrent peers
  STUN_SERVERS           STUN/TURN server configuration

${YELLOW}Examples:${NC}
  # Quick development build with default settings
  ./run_grpc_server.sh

  # Production release build with custom manifest
  ./run_grpc_server.sh --release --manifest ./my-pipeline.yaml

  # Skip rebuild and run with custom gRPC port
  ./run_grpc_server.sh --skip-build --grpc-address 0.0.0.0:9090

  # Debug logging for specific modules
  ./run_grpc_server.sh --log-level debug,remotemedia_webrtc=trace,remotemedia_runtime_core=debug

${YELLOW}Features Enabled:${NC}
  ✓ grpc-signaling       gRPC bidirectional streaming (instead of WebSocket)
  ✓ multiprocess         Process-isolated Python nodes with iceoryx2 IPC
  ✓ docker      Execute nodes in Docker containers
  ✓ silero-vad           Native Rust voice activity detection

${YELLOW}Dependencies:${NC}
  - Rust toolchain (1.87+)
  - Docker daemon (for docker feature)
  - iceoryx2 system dependencies (for multiprocess feature)

${YELLOW}Pipeline Manifest Format:${NC}
  The manifest file can be JSON or YAML. Example:
  
  {
    "nodes": [
      {
        "id": "docker_node",
        "node_type": "CustomDockerNode",
        "executor": "docker",
        "config": {
          "image": "my-node:latest"
        }
      }
    ],
    "edges": []
  }

${YELLOW}Connecting Clients:${NC}
  Once running, clients can connect via gRPC at: ${GRPC_ADDRESS}
  
  Example with Python client:
    from remotemedia.webrtc import WebRTCClient
    client = WebRTCClient(grpc_address="${GRPC_ADDRESS}")
    await client.connect()

EOF
    exit 0
fi

# Print configuration
echo -e "${CYAN}========================================${NC}"
echo -e "${CYAN}WebRTC Server Configuration${NC}"
echo -e "${CYAN}========================================${NC}"
echo -e "Build Mode:      ${GREEN}${BUILD_MODE}${NC}"
echo -e "gRPC Address:    ${GREEN}${GRPC_ADDRESS}${NC}"
echo -e "Manifest:        ${GREEN}${MANIFEST}${NC}"
echo -e "Max Peers:       ${GREEN}${MAX_PEERS}${NC}"
echo -e "STUN Servers:    ${GREEN}${STUN_SERVERS}${NC}"
echo -e "Log Level:       ${GREEN}${RUST_LOG}${NC}"
echo -e "${CYAN}========================================${NC}"
echo ""

# Navigate to the webrtc transport directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Build the server
if [ "$SKIP_BUILD" = false ]; then
    echo -e "${CYAN}Building WebRTC server...${NC}"
    echo -e "${YELLOW}Features: grpc-signaling + runtime defaults (multiprocess, docker, silero-vad)${NC}"
    echo ""
    
    BUILD_CMD="cargo build --bin webrtc_server --features grpc-signaling"
    
    if [ "$BUILD_MODE" = "release" ]; then
        BUILD_CMD="$BUILD_CMD --release"
        echo -e "${YELLOW}⚙️  Release build (optimized, takes longer)...${NC}"
    else
        echo -e "${YELLOW}⚙️  Debug build (faster compilation)...${NC}"
    fi
    
    if ! eval "$BUILD_CMD"; then
        echo -e "${RED}❌ Build failed!${NC}"
        exit 1
    fi
    
    echo ""
    echo -e "${GREEN}✅ Build succeeded!${NC}"
    echo ""
else
    echo -e "${YELLOW}⏭️  Skipping build step${NC}"
    echo ""

fi

# Check if manifest file exists
if [ ! -f "$MANIFEST" ]; then
    echo -e "${YELLOW}⚠️  Warning: Manifest file not found: ${MANIFEST}${NC}"
    echo -e "${YELLOW}   Server will start but may fail if manifest is required${NC}"
    echo ""
fi

# Export environment variables for the server
export RUST_LOG
export WEBRTC_ENABLE_GRPC_SIGNALING=true
export GRPC_SIGNALING_ADDRESS="$GRPC_ADDRESS"
export WEBRTC_PIPELINE_MANIFEST="$MANIFEST"
export WEBRTC_MAX_PEERS="$MAX_PEERS"
export WEBRTC_STUN_SERVERS="$STUN_SERVERS"

# Determine binary path
if [ "$BUILD_MODE" = "release" ]; then
    BINARY="../../target/release/webrtc_server"
else
    BINARY="../../target/debug/webrtc_server"
fi

# Check if binary exists
if [ ! -f "$BINARY" ]; then
    echo -e "${RED}❌ Binary not found: ${BINARY}${NC}"
    echo -e "${RED}   Try running without --skip-build${NC}"
    exit 1
fi

# Print startup information
echo -e "${GREEN}🚀 Starting WebRTC Server with gRPC Signaling${NC}"
echo -e "${CYAN}────────────────────────────────────────${NC}"
echo -e "gRPC Endpoint:   ${GREEN}${GRPC_ADDRESS}${NC}"
echo -e "Pipeline:        ${GREEN}${MANIFEST}${NC}"
echo -e "Binary:          ${GREEN}${BINARY}${NC}"
echo -e "${CYAN}────────────────────────────────────────${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop${NC}"
echo -e "${CYAN}────────────────────────────────────────${NC}"
echo ""

# Run the server with command-line arguments
exec "$BINARY" --mode grpc --grpc-address "$GRPC_ADDRESS" --manifest "$MANIFEST"

