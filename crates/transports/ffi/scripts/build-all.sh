#!/bin/bash
#
# build-all.sh - Build all RemoteMedia FFI packages (Python + Node.js)
#
# This is the main CI/CD build script that produces both Python wheels
# and Node.js native addons for all supported platforms.
#
# Usage:
#   ./scripts/build-all.sh              # Build both packages
#   ./scripts/build-all.sh --release    # Release build
#   ./scripts/build-all.sh --python     # Build Python only
#   ./scripts/build-all.sh --nodejs     # Build Node.js only
#   ./scripts/build-all.sh --ci         # CI mode (non-interactive, all platforms)
#
# Environment variables:
#   MATURIN_PYPI_TOKEN  - PyPI API token
#   NPM_TOKEN           - npm auth token

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

# Script directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FFI_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT_ROOT="$(cd "$FFI_DIR/../.." && pwd)"

# Default options
BUILD_TYPE="release"
BUILD_PYTHON=true
BUILD_NODEJS=true
BUILD_ALL_PLATFORMS=false
CI_MODE=false
INCLUDE_WEBRTC=false
VERBOSE=false

print_header() {
    echo -e "${MAGENTA}"
    echo "╔═══════════════════════════════════════════════════════════════════╗"
    echo "║                                                                   ║"
    echo "║   ██████╗ ███████╗███╗   ███╗ ██████╗ ████████╗███████╗          ║"
    echo "║   ██╔══██╗██╔════╝████╗ ████║██╔═══██╗╚══██╔══╝██╔════╝          ║"
    echo "║   ██████╔╝█████╗  ██╔████╔██║██║   ██║   ██║   █████╗            ║"
    echo "║   ██╔══██╗██╔══╝  ██║╚██╔╝██║██║   ██║   ██║   ██╔══╝            ║"
    echo "║   ██║  ██║███████╗██║ ╚═╝ ██║╚██████╔╝   ██║   ███████╗          ║"
    echo "║   ╚═╝  ╚═╝╚══════╝╚═╝     ╚═╝ ╚═════╝    ╚═╝   ╚══════╝          ║"
    echo "║                                                                   ║"
    echo "║             FFI Package Builder for Python & Node.js             ║"
    echo "║                                                                   ║"
    echo "╚═══════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

print_usage() {
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Build options:"
    echo "  --release        Build optimized release (default)"
    echo "  --debug          Build debug version"
    echo "  --all            Build for all supported platforms"
    echo "  --webrtc         Include WebRTC support"
    echo "  --verbose        Show detailed build output"
    echo ""
    echo "Target selection:"
    echo "  --python         Build Python package only"
    echo "  --nodejs         Build Node.js package only"
    echo "  (default: build both)"
    echo ""
    echo "CI/CD options:"
    echo "  --ci             CI mode: non-interactive, all platforms"
    echo "  --publish        Build and publish packages"
    echo ""
    echo "Environment variables:"
    echo "  MATURIN_PYPI_TOKEN  - PyPI API token"
    echo "  NPM_TOKEN           - npm auth token"
    echo ""
    echo "Examples:"
    echo "  $0                           # Build both packages"
    echo "  $0 --all --webrtc            # Build all platforms with WebRTC"
    echo "  $0 --python --release        # Build Python only"
    echo "  $0 --ci                      # CI mode (all platforms)"
    echo ""
}

# Parse command line arguments
PUBLISH=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            print_header
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
            BUILD_ALL_PLATFORMS=true
            shift
            ;;
        --python)
            BUILD_NODEJS=false
            shift
            ;;
        --nodejs)
            BUILD_PYTHON=false
            shift
            ;;
        --webrtc)
            INCLUDE_WEBRTC=true
            shift
            ;;
        --ci)
            CI_MODE=true
            BUILD_ALL_PLATFORMS=true
            shift
            ;;
        --publish)
            PUBLISH=true
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

# Print header
print_header

# Build info
echo -e "${CYAN}Build Configuration:${NC}"
echo -e "  Build type:     ${GREEN}$BUILD_TYPE${NC}"
echo -e "  Python:         $([ "$BUILD_PYTHON" = true ] && echo -e "${GREEN}Yes${NC}" || echo -e "${YELLOW}No${NC}")"
echo -e "  Node.js:        $([ "$BUILD_NODEJS" = true ] && echo -e "${GREEN}Yes${NC}" || echo -e "${YELLOW}No${NC}")"
echo -e "  All platforms:  $([ "$BUILD_ALL_PLATFORMS" = true ] && echo -e "${GREEN}Yes${NC}" || echo -e "${YELLOW}No${NC}")"
echo -e "  WebRTC:         $([ "$INCLUDE_WEBRTC" = true ] && echo -e "${GREEN}Yes${NC}" || echo -e "${YELLOW}No${NC}")"
echo -e "  CI mode:        $([ "$CI_MODE" = true ] && echo -e "${GREEN}Yes${NC}" || echo -e "${YELLOW}No${NC}")"
echo -e "  Publish:        $([ "$PUBLISH" = true ] && echo -e "${GREEN}Yes${NC}" || echo -e "${YELLOW}No${NC}")"
echo ""

# Track build results
PYTHON_SUCCESS=false
NODEJS_SUCCESS=false
START_TIME=$(date +%s)

# Build Python package
if [ "$BUILD_PYTHON" = true ]; then
    echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  Building Python Package${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
    echo ""
    
    PYTHON_ARGS="--$BUILD_TYPE"
    [ "$BUILD_ALL_PLATFORMS" = true ] && PYTHON_ARGS="$PYTHON_ARGS --all"
    [ "$INCLUDE_WEBRTC" = true ] && PYTHON_ARGS="$PYTHON_ARGS --webrtc"
    [ "$VERBOSE" = true ] && PYTHON_ARGS="$PYTHON_ARGS --verbose"
    
    if "$SCRIPT_DIR/build-pip.sh" $PYTHON_ARGS; then
        PYTHON_SUCCESS=true
        echo -e "${GREEN}✓ Python build succeeded${NC}"
    else
        echo -e "${RED}✗ Python build failed${NC}"
        [ "$CI_MODE" = true ] && exit 1
    fi
    echo ""
fi

# Build Node.js package
if [ "$BUILD_NODEJS" = true ]; then
    echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  Building Node.js Package${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
    echo ""
    
    NODEJS_ARGS="--$BUILD_TYPE"
    [ "$BUILD_ALL_PLATFORMS" = true ] && NODEJS_ARGS="$NODEJS_ARGS --all"
    [ "$INCLUDE_WEBRTC" = true ] && NODEJS_ARGS="$NODEJS_ARGS --webrtc"
    [ "$VERBOSE" = true ] && NODEJS_ARGS="$NODEJS_ARGS --verbose"
    
    if "$SCRIPT_DIR/build-npm.sh" $NODEJS_ARGS; then
        NODEJS_SUCCESS=true
        echo -e "${GREEN}✓ Node.js build succeeded${NC}"
    else
        echo -e "${RED}✗ Node.js build failed${NC}"
        [ "$CI_MODE" = true ] && exit 1
    fi
    echo ""
fi

# Publish if requested
if [ "$PUBLISH" = true ]; then
    echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  Publishing Packages${NC}"
    echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
    echo ""
    
    if [ "$BUILD_PYTHON" = true ] && [ "$PYTHON_SUCCESS" = true ]; then
        echo -e "${CYAN}Publishing Python package to PyPI...${NC}"
        "$SCRIPT_DIR/publish-pip.sh" --skip-build || {
            echo -e "${RED}✗ Python publish failed${NC}"
            [ "$CI_MODE" = true ] && exit 1
        }
    fi
    
    if [ "$BUILD_NODEJS" = true ] && [ "$NODEJS_SUCCESS" = true ]; then
        echo -e "${CYAN}Publishing Node.js package to npm...${NC}"
        "$SCRIPT_DIR/publish-npm.sh" --skip-build || {
            echo -e "${RED}✗ Node.js publish failed${NC}"
            [ "$CI_MODE" = true ] && exit 1
        }
    fi
fi

# Calculate build time
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

# Summary
echo ""
echo -e "${MAGENTA}════════════════════════════════════════════════════════${NC}"
echo -e "${MAGENTA}  Build Summary${NC}"
echo -e "${MAGENTA}════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  Duration: ${GREEN}${MINUTES}m ${SECONDS}s${NC}"
echo ""

if [ "$BUILD_PYTHON" = true ]; then
    if [ "$PYTHON_SUCCESS" = true ]; then
        echo -e "  ${GREEN}✓${NC} Python:  Built successfully"
        if [ -d "$FFI_DIR/dist" ]; then
            WHEEL_COUNT=$(ls -1 "$FFI_DIR/dist"/*.whl 2>/dev/null | wc -l || echo "0")
            echo -e "           ${CYAN}$WHEEL_COUNT wheel(s) in dist/${NC}"
        fi
    else
        echo -e "  ${RED}✗${NC} Python:  Build failed"
    fi
fi

if [ "$BUILD_NODEJS" = true ]; then
    if [ "$NODEJS_SUCCESS" = true ]; then
        echo -e "  ${GREEN}✓${NC} Node.js: Built successfully"
        NODE_COUNT=$(ls -1 "$FFI_DIR/nodejs"/*.node 2>/dev/null | wc -l || echo "0")
        if [ "$NODE_COUNT" -gt 0 ]; then
            echo -e "           ${CYAN}$NODE_COUNT native addon(s)${NC}"
        fi
    else
        echo -e "  ${RED}✗${NC} Node.js: Build failed"
    fi
fi

echo ""

# Overall status
if [ "$BUILD_PYTHON" = true ] && [ "$PYTHON_SUCCESS" = false ]; then
    echo -e "${RED}Build completed with errors.${NC}"
    exit 1
fi

if [ "$BUILD_NODEJS" = true ] && [ "$NODEJS_SUCCESS" = false ]; then
    echo -e "${RED}Build completed with errors.${NC}"
    exit 1
fi

echo -e "${GREEN}All builds completed successfully!${NC}"
echo ""
echo -e "Next steps:"
if [ "$BUILD_PYTHON" = true ]; then
    echo -e "  Python:  ${BLUE}pip install $FFI_DIR/dist/*.whl${NC}"
fi
if [ "$BUILD_NODEJS" = true ]; then
    echo -e "  Node.js: ${BLUE}cd $FFI_DIR/nodejs && npm test${NC}"
fi
echo ""
