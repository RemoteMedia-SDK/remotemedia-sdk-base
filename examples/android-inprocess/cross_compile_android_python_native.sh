#!/bin/bash
# =============================================================================
# Cross-compile Android Python native modules (pydantic_core, jiter, etc.)
# =============================================================================
# This script cross-compiles Rust-based Python native modules for Android
# using the python-for-android toolchain's Python 3.14.
#
# Prerequisites:
# - Android NDK r25c+ (tested with 27.0.11718014)
# - Rust with aarch64-linux-android target
# - python-for-android distro with Python 3.14 built
# - Maturity: requires pydantic_core 2.46.4, jiter 0.15.0
#
# Output: Wheels placed in app/src/main/assets/python-wheels/
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

# =============================================================================
# Configuration
# =============================================================================
ANDROID_PROJECT="/home/acidhax/dev/personal/remotemedia/remotemedia-sdk-base/examples/android-inprocess"
WHEELS_DIR="${ANDROID_PROJECT}/app/src/main/assets/python-wheels"

# Python-for-Android paths (adjusts based on your p4a build)
P4A_ROOT="/home/acidhax/.local/share/python-for-android"
P4A_DIST="remotemedia_hermes"
P4A_ARCH="arm64-v8a"
P4A_NDK_API="24"

# p4a build directories use arm64-v8a (with hyphens) converted to arm64_v8a (underscores) in lib.android paths
P4A_ARCH_UNDERSCORE="arm64_v8a"
PYTHON_BUILD_DIR="${P4A_ROOT}/build/other_builds/python3/${P4A_ARCH}__ndk_target_${P4A_NDK_API}/python3"
PYTHON_ANDROID_BUILD="${PYTHON_BUILD_DIR}/android-build"
PYTHON_INCLUDE="${PYTHON_BUILD_DIR}/Include"
PYTHON_LIB_DIR="${PYTHON_ANDROID_BUILD}"
PYTHON_SYSCONFIGDATA="${PYTHON_ANDROID_BUILD}/build/lib.android-${P4A_NDK_API}-${P4A_ARCH_UNDERSCORE}-3.14/_sysconfigdata__android_aarch64-linux-android.py"
PYTHON_SYSCONFIGDATA_DEST="${PYTHON_ANDROID_BUILD}/_sysconfigdata__android_aarch64-linux-android.py"

# NDK paths
NDK_PATH="/home/acidhax/Android/Sdk/ndk/27.0.11718014"
TOOLCHAIN="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64"
TARGET=aarch64-linux-android24

export CC="${TOOLCHAIN}/bin/${TARGET}-clang"
export CXX="${TOOLCHAIN}/bin/${TARGET}-clang++"
export AR="${TOOLCHAIN}/bin/llvm-ar"
export RANLIB="${TOOLCHAIN}/bin/llvm-ranlib"
export STRIP="${TOOLCHAIN}/bin/llvm-strip"

export CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="${TOOLCHAIN}/bin/aarch64-linux-android24-clang"
export CC_aarch64_linux_android="${CC}"
export CXX_aarch64_linux_android="${CXX}"
export AR_aarch64_linux_android="${AR}"
export RANLIB_aarch64_linux_android="${RANLIB}"

# PyO3 cross-compilation environment
export PYO3_CROSS=1
export PYO3_CROSS_LIB_DIR="${PYTHON_ANDROID_BUILD}"
export PYO3_CROSS_PYTHON_VERSION="3.14"
export PYO3_CROSS_PYTHON_IMPLEMENTATION=CPython
export _PYTHON_SYSCONFIGDATA_NAME="_sysconfigdata__android_aarch64-linux-android"

# Cargo config for Android
export RUSTFLAGS="${RUSTFLAGS:-} -C link-arg=-L${PYTHON_LIB_DIR} -C link-arg=-lpython3.14"

# Create wheels directory
mkdir -p "${WHEELS_DIR}"

# =============================================================================
# Helper functions
# =============================================================================
verify_env() {
    log "Verifying build environment..."
    
    [[ -d "${PYTHON_ANDROID_BUILD}" ]] || error "Python Android build not found: ${PYTHON_ANDROID_BUILD}"
    [[ -f "${PYTHON_SYSCONFIGDATA}" ]] || error "_sysconfigdata not found: ${PYTHON_SYSCONFIGDATA}"
    [[ -f "${PYTHON_ANDROID_BUILD}/libpython3.14.so" ]] || error "libpython3.14.so not found"
    
    # Copy sysconfigdata to where PYO3 expects it
    cp "${PYTHON_SYSCONFIGDATA}" "${PYTHON_SYSCONFIGDATA_DEST}"
    log "Copied _sysconfigdata to ${PYTHON_SYSCONFIGDATA_DEST}"
    
    command -v cargo >/dev/null || error "cargo not found"
    command -v rustup >/dev/null || error "rustup not found"
    
    # Add Android target if not present
    rustup target list --installed | grep -q aarch64-linux-android || {
        log "Adding aarch64-linux-android target..."
        rustup target add aarch64-linux-android
    }
    
    success "Environment verified"
}

# =============================================================================
# Build pydantic_core
# =============================================================================
build_pydantic_core() {
    local VERSION="2.46.4"
    local OUT_DIR="/tmp/pydantic_core-${VERSION}"
    local WHEEL="${WHEELS_DIR}/pydantic_core-${VERSION}-android_cp314.whl"
    
    log "Building pydantic_core ${VERSION} for Android..."
    
    if [[ -f "${WHEEL}" ]]; then
        log "Wheel already exists: ${WHEEL}"
        return 0
    fi
    
    cd /tmp
    rm -rf "pydantic_core-${VERSION}"
    
    # Download source
    if [[ ! -d "pydantic_core-${VERSION}" ]]; then
        log "Downloading pydantic_core ${VERSION}..."
        curl -L "https://files.pythonhosted.org/packages/9d/56/921726b776ace8d8f5db44c4ef961006580d91dc52b803c489fafd1aa249/pydantic_core-${VERSION}.tar.gz" \
            -o "pydantic_core-${VERSION}.tar.gz"
        tar xzf "pydantic_core-${VERSION}.tar.gz"
    fi
    
    cd "${OUT_DIR}"
    
    log "Cross-compiling pydantic_core with cargo..."
    # Set PYTHONPATH so host python can find the android sysconfigdata
    PYTHONPATH="${PYTHON_ANDROID_BUILD}" cargo build --target aarch64-linux-android --release 2>&1 | tail -20
    
    local LIB="${OUT_DIR}/target/aarch64-linux-android/release/lib_pydantic_core.so"
    [[ -f "${LIB}" ]] || error "pydantic_core build failed: ${LIB} not found"
    
    log "Creating wheel..."
    local WHEEL_DIR="/tmp/pydantic_core_wheel_${VERSION}"
    rm -rf "${WHEEL_DIR}"
    mkdir -p "${WHEEL_DIR}"
    
    # Use existing wheel as template
    local TEMPLATE_WHEEL=$(find "${WHEELS_DIR}" -name "pydantic_core-*.whl" | head -1)
    if [[ -z "${TEMPLATE_WHEEL}" ]]; then
        error "No template pydantic_core wheel found in ${WHEELS_DIR}"
    fi
    
    cd /tmp
    rm -rf pydantic_core_wheel
    mkdir -p pydantic_core_wheel
    cd pydantic_core_wheel
    
    unzip -q "${TEMPLATE_WHEEL}"
    cp "${LIB}" pydantic_core/_pydantic_core.so
    
    # Update METADATA version if needed (optional)
    zip -r "${WHEEL}" . -x "*.git*"
    
    success "Created pydantic_core wheel: ${WHEEL}"
}

# =============================================================================
# Build jiter
# =============================================================================
build_jiter() {
    local VERSION="0.15.0"
    local OUT_DIR="/tmp/jiter-${VERSION}"
    local WHEEL="${WHEELS_DIR}/jiter-${VERSION}-android_cp314.whl"
    
    log "Building jiter ${VERSION} for Android..."
    
    if [[ -f "${WHEEL}" ]]; then
        log "Wheel already exists: ${WHEEL}"
        return 0
    fi
    
    cd /tmp
    
    # Use cached library if available (avoids rebuild)
    if [[ -f "/home/acidhax/jiter_python.so" ]]; then
        log "Using cached jiter library from /home/acidhax/jiter_python.so"
        LIB="/home/acidhax/jiter_python.so"
    else
        rm -rf "jiter-${VERSION}"
        
        # Download source from PyPI
        log "Downloading jiter ${VERSION} from PyPI..."
        # URL from PyPI JSON API
        curl -L "https://files.pythonhosted.org/packages/66/b5/55f06bb281d92fb3cc86d14e1def2bd908bb77693183e7cb1f5a3c388b0c/jiter-${VERSION}.tar.gz" \
            -o "jiter-${VERSION}.tar.gz"
        
        # Verify download is not empty
        if [[ $(stat -c%s "jiter-${VERSION}.tar.gz") -lt 100000 ]]; then
            error "jiter download failed - file too small"
        fi
        
        tar xzf "jiter-${VERSION}.tar.gz" || error "Failed to extract jiter source"
        
        cd "${OUT_DIR}"
        
        log "Cross-compiling jiter with cargo..."
        # Set PYTHONPATH so host python can find the android sysconfigdata
        PYTHONPATH="${PYTHON_ANDROID_BUILD}" cargo build --target aarch64-linux-android --release 2>&1 | tail -20
        
        local LIB="${OUT_DIR}/target/aarch64-linux-android/release/libjiter_python.so"
        [[ -f "${LIB}" ]] || error "jiter build failed: ${LIB} not found"
        
        cp "${LIB}" /home/acidhax/jiter_python.so
        LIB="/home/acidhax/jiter_python.so"
    fi
    
    LIB="/home/acidhax/jiter_python.so"
    [[ -f "${LIB}" ]] || error "jiter library not found at ${LIB}"
    
    log "Creating wheel..."
    cd /tmp
    rm -rf jiter_wheel
    mkdir -p jiter_wheel/jiter
    cd jiter_wheel
    
    # Use pydantic_core wheel as template for structure
    unzip -q "${WHEELS_DIR}/pydantic_core-2.46.4-android_cp314.whl"
    
    # Replace pydantic_core with jiter
    rm -rf pydantic_core pydantic_core-2.46.4.dist-info
    
    # Debug: check if LIB exists
    log "DEBUG: LIB = ${LIB}"
    log "DEBUG: ls -la ${LIB}:"
    ls -la "${LIB}" 2>&1 || log "DEBUG: ls failed"
    
    # Use cp instead of mv to avoid losing the cached file
    cp "${LIB}" jiter/jiter.cpython-314-aarch64-linux-android.so
    
    # Create minimal __init__.py
    cat > jiter/__init__.py << 'EOF'
from .jiter import from_json, iter_json
from .jiter import JsonValue, JsonType
from .jiter import IterFlags, PartialMode, JSONthPartialMode

__all__ = [
    "from_json",
    "iter_json",
    "JsonValue",
    "JsonType",
    "IterFlags",
    "PartialMode",
    "JSONthPartialMode",
]

__version__ = "0.15.0"
EOF
    
    # Create dist-info
    mkdir -p jiter-0.15.0.dist-info
    cat > jiter-0.15.0.dist-info/METADATA << 'EOF'
Metadata-Version: 2.1
Name: jiter
Version: 0.15.0
Summary: Fast iterable JSON parser with Python bindings
Home-page: https://github.com/PyO3/jiter
Author: PyO3 contributors
Author-email: pyo3@pyo3.rs
License: MIT OR Apache-2.0
Platform: any
Classifier: Programming Language :: Python :: 3
Classifier: Programming Language :: Python :: 3.11
Classifier: Programming Language :: Python :: 3.12
Classifier: Programming Language :: Python :: 3.13
Classifier: Programming Language :: Python :: 3.14
Classifier: Programming Language :: Rust
Classifier: License :: OSI Approved :: MIT License
Classifier: License :: OSI Approved :: Apache Software License
Classifier: Operating System :: POSIX :: Linux
Classifier: Intended Audience :: Developers
Requires-Python: >=3.9
Provides-Extra: test
EOF
    
    cat > jiter-0.15.0.dist-info/WHEEL << 'EOF'
Wheel-Version: 1.0
Generator: cross_compile_android_python_native.sh
Root-Is-Purelib: false
Tag: cp314-cp314-android_aarch64
EOF
    
    # Create RECORD (minimal)
    cat > jiter-0.15.0.dist-info/RECORD << 'EOF'
jiter/__init__.py,,
jiter/jiter.cpython-314-aarch64-linux-android.so,,
jiter-0.15.0.dist-info/METADATA,,
jiter-0.15.0.dist-info/WHEEL,,
jiter-0.15.0.dist-info/RECORD,,
EOF
    
    # Move the compiled library to the right place
    mkdir -p jiter
    mv "${LIB}" jiter/jiter.cpython-314-aarch64-linux-android.so
    
    # Create wheel
    zip -r "${WHEEL}" jiter jiter-0.15.0.dist-info
    
    success "Created jiter wheel: ${WHEEL}"
}

# =============================================================================
# Main
# =============================================================================
main() {
    log "Starting Android Python native module cross-compilation"
    log "NDK: ${NDK_PATH}"
    log "Python: ${PYTHON_ANDROID_BUILD}"
    log "Wheels output: ${WHEELS_DIR}"
    
    verify_env
    
    build_pydantic_core
    build_jiter
    
    log "All wheels built successfully in ${WHEELS_DIR}:"
    ls -la "${WHEELS_DIR}"/*.whl
    
    success "Cross-compilation complete!"
}

# Run main
main "$@"