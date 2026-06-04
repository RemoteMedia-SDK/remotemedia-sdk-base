#!/bin/bash
#
# build-npm.sh - Build Node.js native addon for RemoteMedia FFI bindings
#
# This script builds the Node.js native addon using napi-rs for multiple
# platforms. It produces platform-specific .node binaries.
#
# Usage:
#   ./scripts/build-npm.sh              # Build for current platform
#   ./scripts/build-npm.sh --release    # Release build (optimized)
#   ./scripts/build-npm.sh --all        # Build for all supported platforms
#   ./scripts/build-npm.sh --webrtc     # Include WebRTC support
#
# Feature flags:
#   Without --webrtc: cargo build --features napi --no-default-features
#   With --webrtc:    cargo build --features napi,webrtc --no-default-features
#
# Environment variables:
#   NPM_TOKEN  - npm auth token for publishing

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
PROJECT_ROOT="$(cd "$FFI_DIR/../.." && pwd)"
NODEJS_DIR="$FFI_DIR/nodejs"

# Default options
BUILD_TYPE="release"
BUILD_ALL=false
VERBOSE=false
FEATURES="napi"

print_usage() {
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --release       Build optimized release (default)"
    echo "  --debug         Build debug version"
    echo "  --all           Build for all supported platforms"
    echo "  --webrtc        Include WebRTC support"
    echo "  --verbose       Show detailed build output"
    echo "  --help          Show this help message"
    echo ""
    echo "Supported platforms:"
    echo "  - x86_64-unknown-linux-gnu    (Linux x64)"
    echo "  - aarch64-unknown-linux-gnu   (Linux ARM64)"
    echo "  - x86_64-apple-darwin         (macOS x64)"
    echo "  - aarch64-apple-darwin        (macOS ARM64 / Apple Silicon)"
    echo "  - x86_64-pc-windows-msvc      (Windows x64)"
    echo ""
    echo "Examples:"
    echo "  $0                        # Build release for current platform"
    echo "  $0 --all                  # Build for all platforms"
    echo "  $0 --webrtc --release     # Build with WebRTC support"
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
        --webrtc)
            FEATURES="napi,webrtc"
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
echo -e "${BLUE}  RemoteMedia Node.js Native Addon Builder${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  Package:   ${GREEN}@matbee/remotemedia-native${NC}"
echo -e "  Build:     ${GREEN}$BUILD_TYPE${NC}"
echo -e "  Features:  ${GREEN}$FEATURES${NC}"
echo -e "  Output:    ${GREEN}$NODEJS_DIR${NC}"
echo ""

# Check dependencies
check_dependencies() {
    echo -e "${CYAN}[1/5]${NC} Checking dependencies..."
    
    # Check Rust
    if ! command -v cargo &> /dev/null; then
        echo -e "${RED}Error: cargo not found. Please install Rust from https://rustup.rs/${NC}"
        exit 1
    fi
    echo -e "  ${GREEN}✓${NC} cargo $(cargo --version | cut -d' ' -f2)"
    
    # Check Node.js
    if ! command -v node &> /dev/null; then
        echo -e "${RED}Error: node not found. Please install Node.js >= 18.${NC}"
        exit 1
    fi
    NODE_VERSION=$(node --version | cut -d'v' -f2 | cut -d'.' -f1)
    if [ "$NODE_VERSION" -lt 18 ]; then
        echo -e "${RED}Error: Node.js >= 18 required. Found: $(node --version)${NC}"
        exit 1
    fi
    echo -e "  ${GREEN}✓${NC} Node.js $(node --version)"
    
    # Check npm
    if ! command -v npm &> /dev/null; then
        echo -e "${RED}Error: npm not found.${NC}"
        exit 1
    fi
    echo -e "  ${GREEN}✓${NC} npm $(npm --version)"
    
    # Check for cross-compilation tools if building for all platforms
    if [ "$BUILD_ALL" = true ]; then
        # Check for zig (for cross-compilation)
        if command -v zig &> /dev/null; then
            echo -e "  ${GREEN}✓${NC} zig $(zig version)"
        else
            echo -e "${YELLOW}  ⚠ zig not found - some cross-compilation targets may fail${NC}"
        fi
        
        # Check for cross (for cross-compilation)
        if command -v cross &> /dev/null; then
            echo -e "  ${GREEN}✓${NC} cross available"
        else
            echo -e "${YELLOW}  ⚠ cross not found - install with: cargo install cross${NC}"
        fi
    fi
}

# Install npm dependencies
install_deps() {
    echo -e "${CYAN}[2/5]${NC} Installing Node.js dependencies..."
    
    cd "$NODEJS_DIR"
    
    if [ ! -d "node_modules" ]; then
        npm install --silent
        echo -e "  ${GREEN}✓${NC} Dependencies installed"
    else
        echo -e "  ${GREEN}✓${NC} Dependencies already installed"
    fi
    
    cd "$FFI_DIR"
}

# Build native addon
build_native() {
    echo -e "${CYAN}[3/5]${NC} Building native addon..."
    
    cd "$FFI_DIR"
    
    CARGO_FLAGS="--features $FEATURES --no-default-features -p remotemedia-ffi"
    
    if [ "$BUILD_TYPE" = "release" ]; then
        CARGO_FLAGS="$CARGO_FLAGS --release"
        TARGET_DIR="release"
    else
        TARGET_DIR="debug"
    fi
    
    if [ "$VERBOSE" = true ]; then
        CARGO_FLAGS="$CARGO_FLAGS -v"
    fi
    
    if [ "$BUILD_ALL" = true ]; then
        # Build for multiple platforms
        TARGETS=(
            "x86_64-unknown-linux-gnu"
            "aarch64-unknown-linux-gnu"
        )
        
        # Add macOS targets if on macOS
        if [[ "$(uname)" == "Darwin" ]]; then
            TARGETS+=("x86_64-apple-darwin" "aarch64-apple-darwin")
        fi
        
        for TARGET in "${TARGETS[@]}"; do
            echo -e "  ${BLUE}→${NC} Building $TARGET..."
            
            # Ensure target is installed
            rustup target add "$TARGET" 2>/dev/null || true
            
            # Try building with cargo-zigbuild first, then cargo
            if command -v cargo-zigbuild &> /dev/null; then
                cargo zigbuild --target "$TARGET" $CARGO_FLAGS || {
                    echo -e "${YELLOW}    Warning: zigbuild failed for $TARGET, trying cargo...${NC}"
                    cargo build --target "$TARGET" $CARGO_FLAGS || true
                }
            elif command -v cross &> /dev/null; then
                cross build --target "$TARGET" $CARGO_FLAGS || {
                    echo -e "${YELLOW}    Warning: cross failed for $TARGET, trying cargo...${NC}"
                    cargo build --target "$TARGET" $CARGO_FLAGS || true
                }
            else
                cargo build --target "$TARGET" $CARGO_FLAGS || {
                    echo -e "${YELLOW}    Warning: cargo build failed for $TARGET${NC}"
                }
            fi
            
            # Copy to nodejs directory with proper name
            copy_native_lib "$TARGET" "$TARGET_DIR"
        done
    else
        # Build for current platform
        echo -e "  Building for current platform..."
        cargo build $CARGO_FLAGS
        
        # Determine current target
        CURRENT_TARGET=$(rustc -vV | grep host | cut -d' ' -f2)
        copy_native_lib "$CURRENT_TARGET" "$TARGET_DIR"
    fi
    
    cd "$PROJECT_ROOT"
}

# Copy native library to nodejs directory with proper naming
copy_native_lib() {
    local TARGET=$1
    local TARGET_DIR=$2
    
    # Determine source file based on OS
    case "$TARGET" in
        *linux*)
            SRC_FILE="$PROJECT_ROOT/target/$TARGET/$TARGET_DIR/libremotemedia_ffi.so"
            [ ! -f "$SRC_FILE" ] && SRC_FILE="$PROJECT_ROOT/target/$TARGET_DIR/libremotemedia_ffi.so"
            ;;
        *darwin*)
            SRC_FILE="$PROJECT_ROOT/target/$TARGET/$TARGET_DIR/libremotemedia_ffi.dylib"
            [ ! -f "$SRC_FILE" ] && SRC_FILE="$PROJECT_ROOT/target/$TARGET_DIR/libremotemedia_ffi.dylib"
            ;;
        *windows*)
            SRC_FILE="$PROJECT_ROOT/target/$TARGET/$TARGET_DIR/remotemedia_ffi.dll"
            [ ! -f "$SRC_FILE" ] && SRC_FILE="$PROJECT_ROOT/target/$TARGET_DIR/remotemedia_ffi.dll"
            ;;
    esac
    
    if [ -f "$SRC_FILE" ]; then
        DEST_FILE="$NODEJS_DIR/remotemedia-native.$TARGET.node"
        cp "$SRC_FILE" "$DEST_FILE"
        echo -e "    ${GREEN}✓${NC} Copied to $(basename "$DEST_FILE")"
        
        # Also copy to standard location for current platform
        if [ -z "$BUILD_ALL" ] || [ "$BUILD_ALL" = false ]; then
            cp "$SRC_FILE" "$PROJECT_ROOT/target/$TARGET_DIR/remotemedia_native.node"
        fi
    else
        echo -e "    ${YELLOW}⚠${NC} Build output not found: $SRC_FILE"
    fi
}

# Generate TypeScript types
generate_types() {
    echo -e "${CYAN}[4/5]${NC} Generating TypeScript types..."
    
    cd "$NODEJS_DIR"
    
    # Try to generate types if native module is available
    if node -e "require('.')" 2>/dev/null; then
        npm run generate-types 2>/dev/null || echo -e "  ${YELLOW}⚠${NC} Type generation skipped (native module required)"
    else
        echo -e "  ${YELLOW}⚠${NC} Type generation skipped (native module not loaded)"
    fi
    
    cd "$FFI_DIR"
}

# Summarize build
summarize_build() {
    echo -e "${CYAN}[5/5]${NC} Build summary..."
    
    echo ""
    echo -e "  ${CYAN}Built files:${NC}"
    
    # List .node files in nodejs directory
    for f in "$NODEJS_DIR"/*.node; do
        if [ -f "$f" ]; then
            SIZE=$(du -h "$f" | cut -f1)
            echo -e "    ${GREEN}✓${NC} $(basename "$f") ($SIZE)"
        fi
    done
    
    # List .node files in target directory
    for f in "$PROJECT_ROOT/target/$TARGET_DIR"/*.node; do
        if [ -f "$f" ]; then
            SIZE=$(du -h "$f" | cut -f1)
            echo -e "    ${GREEN}✓${NC} target/$(basename "$f") ($SIZE)"
        fi
    done
}

# Main execution
TARGET_DIR="release"
[ "$BUILD_TYPE" = "debug" ] && TARGET_DIR="debug"

check_dependencies
install_deps
build_native
generate_types
summarize_build

echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  ✅ Node.js native addon build complete!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "Next steps:"
echo -e "  1. Test locally:     ${BLUE}cd nodejs && npm test${NC}"
echo -e "  2. Publish to npm:   ${BLUE}./scripts/publish-npm.sh${NC}"
echo -e "  3. Install from npm: ${BLUE}npm install @matbee/remotemedia-native${NC}"
echo ""
