#!/bin/bash
# =============================================================================
# Simple python-for-android build script for Hermes Agent
# =============================================================================
# This script uses p4a's built-in pip support to create a distribution
# with hermes-agent and its dependencies.
#
# Usage:
#   ./build_p4a_hermes_simple.sh
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

# Configuration
DIST_NAME="remotemedia_hermes"
ARCH="arm64-v8a"
PYTHON_VERSION="3.11"
P4A_ROOT="${HOME}/.local/share/python-for-android"
DIST_DIR="${P4A_ROOT}/dists/${DIST_NAME}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="/tmp/p4a_build_${DIST_NAME}"
REQUIREMENTS_FILE="${SCRIPT_DIR}/requirements-hermes.txt"

log "Building python-for-android distro: ${DIST_NAME}"
log "Architecture: ${ARCH}"
log "Python version: ${PYTHON_VERSION}"

# Check python-for-android
if ! command -v p4a &> /dev/null; then
    error "python-for-android not found. Install with: pip install python-for-android"
    exit 1
fi
success "p4a found: $(command -v p4a)"
p4a --version

# Check requirements file
if [[ ! -f "${REQUIREMENTS_FILE}" ]]; then
    error "Requirements file not found: ${REQUIREMENTS_FILE}"
    exit 1
fi
success "Requirements file found: ${REQUIREMENTS_FILE}"

# Read requirements and join with commas
REQUIREMENTS=$(grep -v '^#' "${REQUIREMENTS_FILE}" | grep -v '^$' | tr '\n' ',' | sed 's/,$//')
log "Requirements: ${REQUIREMENTS}"

# Clean previous build
log "Cleaning previous build..."
rm -rf "${DIST_DIR}"
mkdir -p "${BUILD_DIR}"

# Build the distribution
log "Building python-for-android distribution..."
log "This may take 10-30 minutes depending on network and CPU..."

# Set Android SDK/NDK paths for p4a
export ANDROID_SDK_ROOT="/home/acidhax/Android/Sdk"
export ANDROID_NDK_ROOT="/home/acidhax/Android/Sdk/ndk/25.2.9519653"
export ANDROID_HOME="/home/acidhax/Android/Sdk"
export JAVA_HOME="/usr/lib/jvm/java-21-openjdk-amd64"
export PATH="/home/acidhax/Android/Sdk/platform-tools:${PATH}"

# Use p4a create with requirements
cd "${BUILD_DIR}"
p4a create \
    --name "${DIST_NAME}" \
    --package "com.remotemedia.inprocess" \
    --version "0.1.0" \
    --bootstrap sdl2 \
    --requirements "${REQUIREMENTS}" \
    --arch "${ARCH}" \
    --python-version "${PYTHON_VERSION}" \
    --ndk-api 24 \
    --android-api 34 \
    --ndk-dir "/home/acidhax/Android/Sdk/ndk/25.2.9519653" \
    --permission INTERNET \
    --permission ACCESS_NETWORK_STATE \
    --permission RECORD_AUDIO \
    --permission MODIFY_AUDIO_SETTINGS \
    --dist-dir "${P4A_ROOT}/dists" \
    2>&1 | tee "${SCRIPT_DIR}/p4a_build_hermes.log"

# Check if build succeeded
if [[ ${PIPESTATUS[0]} -eq 0 ]]; then
    success "python-for-android build completed successfully!"
    log "Distribution created at: ${DIST_DIR}"
    
    # Show what was built
    log "Built architectures:"
    find "${DIST_DIR}" -name "_python_bundle__${ARCH}" -type d 2>/dev/null | head -5
    
    log "Python libraries:"
    find "${DIST_DIR}" -name "libpython*.so" 2>/dev/null | head -5
    
    log ""
    log "To use this in the Android build, set these environment variables:"
    log "  export PYTHON_FOR_ANDROID_ROOT=${P4A_ROOT}"
    log "  export PYTHON_BUNDLE_SRC=${DIST_DIR}/_python_bundle__${ARCH}/_python_bundle"
    log "  export PYTHON_NATIVE_LIBS_SRC=${DIST_DIR}/_python_bundle__${ARCH}/libs/${ARCH}"
    log ""
    log "Then run the main build script:"
    log "  ./android_build_deploy_test.sh --device <IP:PORT>"
else
    error "python-for-android build failed. Check ${SCRIPT_DIR}/p4a_build_hermes.log for details."
    
    # Show last 50 lines of log for debugging
    log "Last 50 lines of build log:"
    tail -50 "${SCRIPT_DIR}/p4a_build_hermes.log"
    exit 1
fi