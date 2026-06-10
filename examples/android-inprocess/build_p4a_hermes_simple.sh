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
find_python_for_android_root() {
    if [[ -n "${P4A_ROOT:-}" && -d "$P4A_ROOT" ]]; then
        echo "$P4A_ROOT"
        return 0
    fi

    if [[ -d "${HOME}/.local/share/python-for-android" ]]; then
        echo "${HOME}/.local/share/python-for-android"
        return 0
    fi

    if [[ -d "${HOME}/snap/code/current/.local/share/python-for-android" ]]; then
        echo "${HOME}/snap/code/current/.local/share/python-for-android"
        return 0
    fi

    local candidate
    candidate=$(find "${HOME}/snap/code" -maxdepth 6 -type d -path '*/.local/share/python-for-android' 2>/dev/null | head -n1)
    if [[ -n "$candidate" ]]; then
        echo "$candidate"
        return 0
    fi

    echo "${HOME}/.local/share/python-for-android"
}
P4A_ROOT="$(find_python_for_android_root)"
DIST_DIR="${P4A_ROOT}/dists/${DIST_NAME}"
ACTUAL_DIST_DIR=""
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="/tmp/p4a_build_${DIST_NAME}"
REQUIREMENTS_FILE="${SCRIPT_DIR}/requirements-hermes.txt"

# Critical packages that must be present in the produced Python bundle.
# These map to distribution names in site-packages (dist-info directories).
REQUIRED_DISTROS=(
    "requests"
    "charset_normalizer"
    "idna"
    "pydantic"
    "python_dotenv"
    "urllib3"
    "certifi"
    "pyyaml"
    "httpx"
    "httpcore"
    "websockets"
    "typing_extensions"
)

# Critical module paths that must exist to avoid runtime import failures.
REQUIRED_MODULE_PATHS=(
    "httpx/_transports/__init__.py"
    "websockets/__init__.py"
    "charset_normalizer/__init__.py"
    "typing_extensions.py"
)

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
mapfile -t REQUIREMENTS_ARRAY < <(grep -v '^#' "${REQUIREMENTS_FILE}" | sed 's/#.*$//' | awk 'NF > 0 { print $0 }')
REQUIREMENTS=$(IFS=, ; echo "${REQUIREMENTS_ARRAY[*]}")
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
export ANDROID_NDK_ROOT="/home/acidhax/Android/Sdk/ndk/27.0.11718014"
export ANDROID_HOME="/home/acidhax/Android/Sdk"
export JAVA_HOME="/usr/lib/jvm/java-21-openjdk-amd64"
export PATH="/home/acidhax/Android/Sdk/platform-tools:${PATH}"

# Use p4a create with requirements. Recipes are hardcoded to 3.11.9 (hermes-agent requires <3.14)
cd "${BUILD_DIR}"
p4a create \
    --dist-name "${DIST_NAME}" \
    --package "com.remotemedia.inprocess" \
    --version "0.1.0" \
    --bootstrap sdl2 \
    --requirements "${REQUIREMENTS}" \
    --arch "${ARCH}" \
    --python-version "3.11" \
    --ndk-api 24 \
    --android-api 34 \
    --ndk-dir "/home/acidhax/Android/Sdk/ndk/27.0.11718014" \
    --blacklist-requirements ruamel.yaml,pydantic_core \
    --permission INTERNET \
    --permission ACCESS_NETWORK_STATE \
    --permission RECORD_AUDIO \
    --permission MODIFY_AUDIO_SETTINGS \
    --dist-dir "${P4A_ROOT}/dists" \
    2>&1 | tee "${SCRIPT_DIR}/p4a_build_hermes.log"

verify_required_python_dists() {
    local bundle_root="${ACTUAL_DIST_DIR}/_python_bundle__${ARCH}/_python_bundle"
    local site_packages=""

    if [[ -d "${bundle_root}/site-packages" ]]; then
        site_packages="${bundle_root}/site-packages"
    else
        # Fallback for alternate p4a layouts
        site_packages=$(find "${bundle_root}" -type d -name site-packages 2>/dev/null | head -1 || true)
    fi

    if [[ -z "${site_packages}" || ! -d "${site_packages}" ]]; then
        error "Could not locate site-packages in built distro under ${bundle_root}"
        return 1
    fi

    log "Verifying required Python distributions in: ${site_packages}"
    local missing=0

    for dist in "${REQUIRED_DISTROS[@]}"; do
        if compgen -G "${site_packages}/${dist}-*.dist-info" > /dev/null; then
            success "Found ${dist}"
        else
            error "Missing required distribution: ${dist}"
            missing=1
        fi
    done

    for rel_path in "${REQUIRED_MODULE_PATHS[@]}"; do
        if [[ -f "${site_packages}/${rel_path}" || -f "${site_packages}/${rel_path}c" ]]; then
            success "Found module path ${rel_path}"
        else
            error "Missing required module path: ${rel_path}"
            missing=1
        fi
    done

    if [[ "${missing}" -ne 0 ]]; then
        error "Required Python distributions are missing from the built p4a bundle"
        return 1
    fi

    success "All required Python distributions are present in the p4a bundle"
    return 0
}

resolve_actual_dist_dir() {
    # Prefer configured dist dir when present
    if [[ -d "${DIST_DIR}" ]]; then
        ACTUAL_DIST_DIR="${DIST_DIR}"
        return 0
    fi

    # Fallback: parse p4a output log
    local from_log
    from_log=$(sed -n 's/^\[INFO\]:    Dist can be found at (for now) //p' "${SCRIPT_DIR}/p4a_build_hermes.log" | tail -1)
    if [[ -n "${from_log}" && -d "${from_log}" ]]; then
        ACTUAL_DIST_DIR="${from_log}"
        return 0
    fi

    # Last resort: newest distro directory
    local newest
    newest=$(find "${P4A_ROOT}/dists" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2-)
    if [[ -n "${newest}" && -d "${newest}" ]]; then
        ACTUAL_DIST_DIR="${newest}"
        return 0
    fi

    return 1
}

# Check if build succeeded
if [[ ${PIPESTATUS[0]} -eq 0 ]]; then
    success "python-for-android build completed successfully!"

    if ! resolve_actual_dist_dir; then
        error "Unable to resolve built distro directory"
        exit 1
    fi

    log "Distribution created at: ${ACTUAL_DIST_DIR}"

    if ! verify_required_python_dists; then
        error "Built distro failed dependency verification"
        exit 1
    fi
    
    # Show what was built
    log "Built architectures:"
    find "${ACTUAL_DIST_DIR}" -name "_python_bundle__${ARCH}" -type d 2>/dev/null | head -5
    
    log "Python libraries:"
    find "${ACTUAL_DIST_DIR}" -name "libpython*.so" 2>/dev/null | head -5
    
    log ""
    log "To use this in the Android build, set these environment variables:"
    log "  export PYTHON_FOR_ANDROID_ROOT=${P4A_ROOT}"
    log "  export PYTHON_BUNDLE_SRC=${ACTUAL_DIST_DIR}/_python_bundle__${ARCH}/_python_bundle"
    log "  export PYTHON_NATIVE_LIBS_SRC=${ACTUAL_DIST_DIR}/_python_bundle__${ARCH}/libs/${ARCH}"
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