#!/bin/bash
#
# build-pip.sh - Build Python wheels for RemoteMedia FFI bindings
#
# This script builds Python wheels using maturin for multiple platforms
# and Python versions. It can produce:
# - Source distribution (sdist)
# - Platform-specific wheels
# - Universal wheels (abi3)
#
# Usage:
#   ./scripts/build-pip.sh              # Build for current platform
#   ./scripts/build-pip.sh --release    # Release build (optimized)
#   ./scripts/build-pip.sh --all        # Build for all supported platforms
#   ./scripts/build-pip.sh --sdist      # Build source distribution only
#   ./scripts/build-pip.sh --webrtc     # Include WebRTC support
# Feature flags:
#   By default, the script compiles with "extension-module,python-telephony,python-webrtc"
#   which embeds WebRTC, Telephony, and gRPC server interfaces.
#
# Environment variables:
#   MATURIN_PYPI_TOKEN  - PyPI API token for publishing
#   MATURIN_REPOSITORY  - PyPI repository (pypi or testpypi)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Script directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FFI_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$FFI_DIR/../../.." && pwd)"

# Output directory
DIST_DIR="$FFI_DIR/dist"

# Default options
BUILD_TYPE="release"
BUILD_ALL=false
SDIST_ONLY=false
VERBOSE=false
FEATURES="extension-module,python-telephony,python-webrtc"

print_usage() {
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --release       Build optimized release wheels (default)"
    echo "  --debug         Build debug wheels"
    echo "  --all           Build for all supported platforms (requires Docker)"
    echo "  --sdist         Build source distribution only"
    echo "  --webrtc        Include WebRTC support (enabled by default)"
    echo "  --telephony     Include Telephony/gRPC support (enabled by default)"
    echo "  --verbose       Show detailed build output"
    echo "  --help          Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  MATURIN_PYPI_TOKEN  - PyPI API token for publishing"
    echo ""
    echo "Examples:"
    echo "  $0                        # Build release wheel for current platform"
    echo "  $0 --all                  # Build wheels for all platforms"
    echo "  $0 --release              # Build optimized wheels with WebRTC/Telephony/gRPC"
    echo ""
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            print_usage
            exit 0
            ;;
        --release)
            BUILD_TYPE="release"
            shift
            ;;
        --debug)
            BUILD_TYPE="debug"
            shift
            ;;
        --all)
            BUILD_ALL=true
            shift
            ;;
        --sdist)
            SDIST_ONLY=true
            shift
            ;;
        --webrtc)
            # Already enabled by default
            shift
            ;;
        --telephony)
            # Already enabled by default
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        *)
            echo -e "${RED}Error: Unknown argument '$1'${NC}"
            print_usage
            exit 1
            ;;
    esac
done

echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  RemoteMedia Python Package Builder${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  Package:   ${GREEN}remotemedia-ffi${NC}"
echo -e "  Build:     ${GREEN}$BUILD_TYPE${NC}"
echo -e "  Features:  ${GREEN}$FEATURES${NC}"
echo -e "  Output:    ${GREEN}$DIST_DIR${NC}"
echo ""

# Check dependencies
check_dependencies() {
    echo -e "${CYAN}[1/4]${NC} Checking dependencies..."
    
    # Check Rust
    if ! command -v cargo &> /dev/null; then
        echo -e "${RED}Error: cargo not found. Please install Rust from https://rustup.rs/${NC}"
        exit 1
    fi
    echo -e "  ${GREEN}✓${NC} cargo $(cargo --version | cut -d' ' -f2)"
    
    # Check maturin
    if ! command -v maturin &> /dev/null; then
        echo -e "${YELLOW}Warning: maturin not found. Installing...${NC}"
        pip install maturin
    fi
    echo -e "  ${GREEN}✓${NC} maturin $(maturin --version | cut -d' ' -f2)"
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}Error: python3 not found.${NC}"
        exit 1
    fi
    echo -e "  ${GREEN}✓${NC} Python $(python3 --version | cut -d' ' -f2)"
    
    # Check Docker if building for all platforms
    if [ "$BUILD_ALL" = true ]; then
        if ! command -v docker &> /dev/null; then
            echo -e "${RED}Error: docker not found. Required for cross-platform builds.${NC}"
            exit 1
        fi
        echo -e "  ${GREEN}✓${NC} Docker $(docker --version | cut -d' ' -f3 | tr -d ',')"
    fi
}

# Clean previous builds
clean_dist() {
    echo -e "${CYAN}[2/4]${NC} Cleaning previous builds..."
    rm -rf "$DIST_DIR"
    mkdir -p "$DIST_DIR"
    echo -e "  ${GREEN}✓${NC} Cleaned $DIST_DIR"
}

# Build wheels
build_wheels() {
    echo -e "${CYAN}[3/4]${NC} Building Python wheels..."
    
    cd "$FFI_DIR"
    
    MATURIN_ARGS="--features $FEATURES --out $DIST_DIR"
    
    if [ "$BUILD_TYPE" = "release" ]; then
        MATURIN_ARGS="$MATURIN_ARGS --release"
    fi
    
    if [ "$VERBOSE" = true ]; then
        MATURIN_ARGS="$MATURIN_ARGS --verbose"
    fi
    
    if [ "$SDIST_ONLY" = true ]; then
        echo -e "  Building source distribution..."
        maturin sdist $MATURIN_ARGS
    elif [ "$BUILD_ALL" = true ]; then
        echo -e "  Building for all platforms (using Docker)..."
        
        # Read FFmpeg static linking configuration from host .cargo/config.toml
        FFMPEG_HAS_VPX=$(grep -E "^FFMPEG_LIBVPX\s*=" "$PROJECT_ROOT/.cargo/config.toml" | cut -d'"' -f2 || echo "1")
        FFMPEG_HAS_X264=$(grep -E "^FFMPEG_LIBX264\s*=" "$PROJECT_ROOT/.cargo/config.toml" | cut -d'"' -f2 || echo "1")
        FFMPEG_HAS_AOM=$(grep -E "^FFMPEG_LIBAOM\s*=" "$PROJECT_ROOT/.cargo/config.toml" | cut -d'"' -f2 || echo "1")

        # Docker container needs relative --out path so it writes into /io/crates/transports/ffi/dist
        DOCKER_MATURIN_ARGS="--features $FEATURES --out dist"
        if [ "$BUILD_TYPE" = "release" ]; then
            DOCKER_MATURIN_ARGS="$DOCKER_MATURIN_ARGS --release"
        fi
        if [ "$VERBOSE" = true ]; then
            DOCKER_MATURIN_ARGS="$DOCKER_MATURIN_ARGS --verbose"
        fi

        # Linux x86_64 (manylinux)
        echo -e "  ${BLUE}→${NC} Building linux-x86_64..."
        docker run --rm \
            -v "$PROJECT_ROOT":/io \
            -w /io/crates/transports/ffi \
            -e FFMPEG_INCLUDE_DIR=/io/vendor/ffmpeg/include \
            -e FFMPEG_LIB_DIR=/io/vendor/ffmpeg/lib \
            -e FFMPEG_LIBS_MODE=static \
            -e FFMPEG_LIBVPX="$FFMPEG_HAS_VPX" \
            -e FFMPEG_LIBX264="$FFMPEG_HAS_X264" \
            -e FFMPEG_LIBAOM="$FFMPEG_HAS_AOM" \
            -e FFMPEG_LIBVPX_LIB_DIR=/io/vendor/libvpx/lib \
            -e FFMPEG_LIBX264_LIB_DIR=/io/vendor/x264/lib \
            -e FFMPEG_LIBAOM_LIB_DIR=/io/vendor/aom/lib \
            --entrypoint bash \
            ghcr.io/pyo3/maturin \
            -c "yum install -y epel-release && (yum install -y perl-core xz-devel bzip2-devel libvdpau-devel || yum install -y perl-IPC-Cmd perl-Time-Piece xz-devel bzip2-devel libvdpau-devel) && maturin build $DOCKER_MATURIN_ARGS --target x86_64-unknown-linux-gnu --manylinux 2_28" || {
                echo -e "${YELLOW}  Warning: Failed to build linux-x86_64 via Docker, trying on host...${NC}"
                maturin build $MATURIN_ARGS --target x86_64-unknown-linux-gnu --manylinux 2_28 || {
                    echo -e "${YELLOW}  Warning: Failed to build linux-x86_64 with manylinux compliance, trying with zig...${NC}"
                    maturin build $MATURIN_ARGS --target x86_64-unknown-linux-gnu --zig || true
                }
            }
        
        # Linux aarch64 (manylinux)
        echo -e "  ${BLUE}→${NC} Building linux-aarch64..."
        docker run --rm \
            -v "$PROJECT_ROOT":/io \
            -w /io/crates/transports/ffi \
            -e FFMPEG_INCLUDE_DIR=/io/vendor/ffmpeg/include \
            -e FFMPEG_LIB_DIR=/io/vendor/ffmpeg/lib \
            -e FFMPEG_LIBS_MODE=static \
            -e FFMPEG_LIBVPX="$FFMPEG_HAS_VPX" \
            -e FFMPEG_LIBX264="$FFMPEG_HAS_X264" \
            -e FFMPEG_LIBAOM="$FFMPEG_HAS_AOM" \
            -e FFMPEG_LIBVPX_LIB_DIR=/io/vendor/libvpx/lib \
            -e FFMPEG_LIBX264_LIB_DIR=/io/vendor/x264/lib \
            -e FFMPEG_LIBAOM_LIB_DIR=/io/vendor/aom/lib \
            --entrypoint bash \
            ghcr.io/pyo3/maturin \
            -c "yum install -y epel-release && (yum install -y perl-core xz-devel bzip2-devel libvdpau-devel || yum install -y perl-IPC-Cmd perl-Time-Piece xz-devel bzip2-devel libvdpau-devel) && rustup target add aarch64-unknown-linux-gnu && maturin build $DOCKER_MATURIN_ARGS --target aarch64-unknown-linux-gnu --manylinux 2_28" || {
                echo -e "${YELLOW}  Warning: Failed to build linux-aarch64 via Docker, trying on host...${NC}"
                maturin build $MATURIN_ARGS --target aarch64-unknown-linux-gnu --manylinux 2_28 || {
                    echo -e "${YELLOW}  Warning: Failed to build linux-aarch64, trying with zig...${NC}"
                    maturin build $MATURIN_ARGS --target aarch64-unknown-linux-gnu --zig || true
                }
            }
        
        # macOS x86_64
        if [[ "$(uname)" == "Darwin" ]]; then
            echo -e "  ${BLUE}→${NC} Building macos-x86_64..."
            maturin build $MATURIN_ARGS --target x86_64-apple-darwin || true
            
            # macOS aarch64 (Apple Silicon)
            echo -e "  ${BLUE}→${NC} Building macos-aarch64..."
            maturin build $MATURIN_ARGS --target aarch64-apple-darwin || true
        fi
        
        # Also build sdist
        echo -e "  ${BLUE}→${NC} Building source distribution..."
        maturin sdist --out "$DIST_DIR"
    else
        # Build for current platform only
        echo -e "  Building for current platform..."
        maturin build $MATURIN_ARGS
        
        # Also build sdist
        maturin sdist --out "$DIST_DIR"
    fi
    
    cd "$PROJECT_ROOT"
}

# Summarize build
summarize_build() {
    echo -e "${CYAN}[4/4]${NC} Build summary..."
    
    if [ -d "$DIST_DIR" ]; then
        WHEEL_COUNT=$(ls -1 "$DIST_DIR"/*.whl 2>/dev/null | wc -l || echo "0")
        SDIST_COUNT=$(ls -1 "$DIST_DIR"/*.tar.gz 2>/dev/null | wc -l || echo "0")
        
        echo ""
        echo -e "  ${GREEN}Wheels built:${NC} $WHEEL_COUNT"
        echo -e "  ${GREEN}Source dists:${NC} $SDIST_COUNT"
        echo ""
        
        if [ -d "$DIST_DIR" ]; then
            echo -e "  ${CYAN}Files:${NC}"
            for f in "$DIST_DIR"/*; do
                if [ -f "$f" ]; then
                    SIZE=$(du -h "$f" | cut -f1)
                    echo -e "    ${GREEN}✓${NC} $(basename "$f") ($SIZE)"
                fi
            done
        fi
    else
        echo -e "${RED}Error: No build output found${NC}"
        exit 1
    fi
}

# Main execution
check_dependencies
clean_dist
build_wheels
summarize_build

echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  ✅ Python package build complete!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "Next steps:"
echo -e "  1. Test locally:     ${BLUE}pip install $DIST_DIR/*.whl${NC}"
echo -e "  2. Publish to PyPI:  ${BLUE}./scripts/publish-pip.sh${NC}"
echo -e "  3. Test from PyPI:   ${BLUE}pip install remotemedia-ffi${NC}"
echo ""
