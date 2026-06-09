#!/bin/bash
# =============================================================================
# Build python-for-android distro with Hermes Agent for RemoteMedia Android
# =============================================================================
# This script creates a python-for-android distribution that includes hermes-agent
# and all its core dependencies, suitable for in-process execution via PyO3 on Android.
#
# Usage:
#   ./build_p4a_hermes.sh [--clean] [--arch arm64-v8a]
#
# Requirements:
#   - python-for-android installed: pip install python-for-android
#   - Android SDK and NDK configured
#   - Internet access to download packages from PyPI
#
# The output will be at ~/.local/share/python-for-android/dists/remotemedia_hermes/
# =============================================================================

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log() { echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC} $*"; }
warn() { echo -e "${YELLOW}[!]${NC} $*"; }
error() { echo -e "${RED}[ERR]${NC} $*"; }

# Default configuration
CLEAN_BUILD=false
ARCH="arm64-v8a"
DIST_NAME="remotemedia_hermes"
PYTHON_VERSION="3.11"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --clean)
            CLEAN_BUILD=true
            shift
            ;;
        --arch)
            ARCH="$2"
            shift 2
            ;;
        --python-version)
            PYTHON_VERSION="$2"
            shift 2
            ;;
        --dist-name)
            DIST_NAME="$2"
            shift 2
            ;;
        *)
            error "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Paths
P4A_ROOT="${HOME}/.local/share/python-for-android"
DIST_DIR="${P4A_ROOT}/dists/${DIST_NAME}"
BUILD_DIR="/tmp/p4a_build_${DIST_NAME}"
REQUIREMENTS_FILE="${BUILD_DIR}/requirements.txt"

log "Building python-for-android distro: ${DIST_NAME}"
log "Architecture: ${ARCH}"
log "Python version: ${PYTHON_VERSION}"

# Check python-for-android
if ! command -v p4a &> /dev/null; then
    error "python-for-android not found. Install with: pip install python-for-android"
    exit 1
fi
success "p4a found: $(command -v p4a)"

# Check NDK
if [[ -z "${ANDROID_NDK_ROOT:-}" && -z "${ANDROID_NDK_HOME:-}" ]]; then
    warn "ANDROID_NDK_ROOT not set, p4a will try to find it"
fi

# Create build directory
mkdir -p "${BUILD_DIR}"

# Create requirements.txt with hermes-agent core dependencies
log "Creating requirements.txt..."
cat > "${REQUIREMENTS_FILE}" << 'EOF'
# Core hermes-agent dependencies (from pyproject.toml)
openai==2.24.0
python-dotenv==1.2.2
fire==0.7.1
httpx[socks]==0.28.1
rich==14.3.3
tenacity==9.1.4
pyyaml==6.0.3
ruamel.yaml==0.18.17
requests==2.33.0
jinja2==3.1.6
pydantic==2.13.4
prompt_toolkit==3.0.52
croniter==6.0.0
Markdown==3.10.2
PyJWT[crypto]==2.12.1
psutil==7.2.2
pathspec==1.1.1
fastapi==0.133.1
uvicorn[standard]==0.41.0
Pillow==12.2.0

# CLI and cron extras
simple-term-menu==1.6.6

# MCP extra (for ACP integration)
mcp==1.26.0
starlette==1.0.1

# ACP extra
agent-client-protocol==0.9.0

# Web extra
# fastapi and uvicorn already included

# Google workspace (if needed)
google-api-python-client==2.194.0
google-auth-oauthlib==1.3.1
google-auth-httplib2==0.3.1

# YouTube transcript (if needed)
youtube-transcript-api==1.2.4

# NumPy (needed for many scientific packages)
numpy==2.4.3

# Ensure stdlib modules that p4a might miss
sqlite3
EOF

success "requirements.txt created at ${REQUIREMENTS_FILE}"

# Clean previous build if requested
if [[ "${CLEAN_BUILD}" == "true" ]]; then
    log "Cleaning previous build..."
    rm -rf "${DIST_DIR}"
    rm -rf "${BUILD_DIR}/.build"
fi

# Build the python-for-android distribution
log "Building python-for-android distribution..."
log "This may take 10-30 minutes depending on network and CPU..."

# Build with p4a
cd "${BUILD_DIR}"

# Use p4a to create a distribution
p4a create \
    --name "${DIST_NAME}" \
    --package "com.remotemedia.inprocess" \
    --version "0.1.0" \
    --bootstrap sdl2 \
    --requirements "$(cat ${REQUIREMENTS_FILE} | tr '\n' ',' | sed 's/,$//')" \
    --arch "${ARCH}" \
    --python-version "${PYTHON_VERSION}" \
    --ndk-api 24 \
    --permission INTERNET \
    --permission ACCESS_NETWORK_STATE \
    --permission RECORD_AUDIO \
    --permission MODIFY_AUDIO_SETTINGS \
    --private "${BUILD_DIR}/private" \
    --dist-dir "${DIST_DIR}" \
    2>&1 | tee "${BUILD_DIR}/p4a_build.log"

# Check if build succeeded
if [[ ${PIPESTATUS[0]} -eq 0 ]]; then
    success "python-for-android build completed successfully!"
    log "Distribution created at: ${DIST_DIR}"
    
    # Show what was built
    log "Built architectures:"
    find "${DIST_DIR}" -name "_python_bundle__${ARCH}" -type d 2>/dev/null | head -5
    
    log "Python libraries:"
    find "${DIST_DIR}" -name "libpython*.so" 2>/dev/null | head -5
    
    log "To use this in the Android build, set:"
    log "  export PYTHON_FOR_ANDROID_ROOT=${P4A_ROOT}"
    log "  export PYTHON_BUNDLE_SRC=${DIST_DIR}/_python_bundle__${ARCH}/_python_bundle"
    log "  export PYTHON_NATIVE_LIBS_SRC=${DIST_DIR}/_python_bundle__${ARCH}/libs/${ARCH}"
else
    error "python-for-android build failed. Check ${BUILD_DIR}/p4a_build.log for details."
    exit 1
fi