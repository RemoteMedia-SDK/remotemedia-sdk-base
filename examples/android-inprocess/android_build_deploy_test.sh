#!/bin/bash
# =============================================================================
# Android In-Process Voice Assistant - Automated Build, Deploy & Test Script
# =============================================================================
# This script automates the complete build, deploy, and test cycle for the
# RemoteMedia Android In-Process Voice Assistant App.
#
# Prerequisites:
# - Android SDK (platform-tools, build-tools, platforms;android-34)
# - Android NDK r25c (25.2.9519653)
# - Rust 1.75+ with aarch64-linux-android and x86_64-linux-android targets
# - Gradle 8.7+ (via wrapper)
# - Kotlin 1.9.22+
# - Python 3.10+ with python-for-android (for PyO3 linkage)
# - Physical arm64 Android device with USB/WiFi debugging
#
# Usage: ./android_build_deploy_test.sh [--device IP:PORT] [--skip-build] [--skip-deploy]
# =============================================================================

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SDK_BASE_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORKSPACE_ROOT="${REMOTEMEDIA_WORKSPACE_ROOT:-$(cd "${SDK_BASE_ROOT}/.." && pwd)}"
ANDROID_PROJECT="${ANDROID_PROJECT:-${SCRIPT_DIR}}"
DEVICE_ADDRESS="${DEVICE_ADDRESS:-192.168.18.60:35713}"
SKIP_BUILD=false
SKIP_DEPLOY=false
TEST_PIPELINE="${TEST_PIPELINE:-voice-assistant-mobile.json}"
MODEL_SRC="${MODEL_SRC:-${WORKSPACE_ROOT}/litert-lm-loadable-plugin/gemma-4-E2B-it.litertlm}"
MODEL_STAGING_PATH="${MODEL_STAGING_PATH:-/data/local/tmp/gemma-4-E2B-it.litertlm}"
MODEL_DEVICE_PATH="${MODEL_DEVICE_PATH:-/data/data/com.remotemedia.inprocess/files/models/gemma-4-E2B-it.litertlm}"
WHISPER_MODEL_SRC="${WHISPER_MODEL_SRC:-${WORKSPACE_ROOT}/models/whisper/whisper_tiny_30s_f32.tflite}"
WHISPER_BASE_MODEL_SRC="${WHISPER_BASE_MODEL_SRC:-${WORKSPACE_ROOT}/models/whisper/whisper_base_30s_f32.tflite}"
WHISPER_TOKENIZER_SRC="${WHISPER_TOKENIZER_SRC:-${WORKSPACE_ROOT}/models/whisper/tokenizer.json}"
WHISPER_CONFIG_SRC="${WHISPER_CONFIG_SRC:-${WORKSPACE_ROOT}/models/whisper/config.json}"
WHISPER_STAGING_DIR="${WHISPER_STAGING_DIR:-/data/local/tmp/remotemedia-whisper}"
SILERO_VAD_MODEL_SRC="${SILERO_VAD_MODEL_SRC:-${WORKSPACE_ROOT}/silero-vad/silero_vad.onnx}"
SILERO_VAD_STAGING_DIR="${SILERO_VAD_STAGING_DIR:-/data/local/tmp/remotemedia-silero-vad}"
KOKORO_MODEL_SRC="${KOKORO_MODEL_SRC:-${WORKSPACE_ROOT}/models/kokoro/onnx/model_fp16.onnx}"
KOKORO_MODEL_NAME="${KOKORO_MODEL_NAME:-$(basename "$KOKORO_MODEL_SRC")}"
KOKORO_TOKENIZER_SRC="${KOKORO_TOKENIZER_SRC:-${WORKSPACE_ROOT}/models/kokoro/tokenizer.json}"
KOKORO_VOICE_SRC="${KOKORO_VOICE_SRC:-${WORKSPACE_ROOT}/models/kokoro/voices/af_bella.bin}"
KOKORO_STAGING_DIR="${KOKORO_STAGING_DIR:-/data/local/tmp/remotemedia-kokoro}"
MISAKI_G2P_RESOURCE_SRC="${MISAKI_G2P_RESOURCE_SRC:-${WORKSPACE_ROOT}/models/misaki-g2p}"
MISAKI_G2P_RESOURCE_FALLBACK_SRC="${MISAKI_G2P_RESOURCE_FALLBACK_SRC:-${WORKSPACE_ROOT}/misaki-g2p/resources}"
MISAKI_G2P_STAGING_DIR="${MISAKI_G2P_STAGING_DIR:-/data/local/tmp/remotemedia-misaki-g2p}"
LOG_OUTPUT="${LOG_OUTPUT:-${ANDROID_PROJECT}/android-inprocess-logcat.txt}"
# Python-for-Android distro with Hermes Agent (built with build_p4a_hermes_simple.sh)
PYTHON_FOR_ANDROID_ROOT="${PYTHON_FOR_ANDROID_ROOT:-${HOME}/.local/share/python-for-android/dists}"
PYTHON_BUNDLE_SRC="${PYTHON_BUNDLE_SRC:-${PYTHON_FOR_ANDROID_ROOT}/remotemedia_hermes/_python_bundle__arm64-v8a/_python_bundle}"
PYTHON_BUNDLE_FALLBACK_SRC="${PYTHON_BUNDLE_FALLBACK_SRC:-${PYTHON_FOR_ANDROID_ROOT}/remotemedia_numpy/_python_bundle__arm64-v8a/_python_bundle}"
PYTHON_NATIVE_LIBS_SRC="${PYTHON_NATIVE_LIBS_SRC:-${PYTHON_FOR_ANDROID_ROOT}/remotemedia_hermes/_python_bundle__arm64-v8a/libs/arm64-v8a}"
PYTHON_NATIVE_LIBS_FALLBACK_SRC="${PYTHON_NATIVE_LIBS_FALLBACK_SRC:-${PYTHON_FOR_ANDROID_ROOT}/remotemedia_numpy/libs/arm64-v8a}"
PYTHON_SRC="${PYTHON_SRC:-${WORKSPACE_ROOT}/remotemedia-sdk/clients/python}"
PYTHON_SRC_STAGING_LOCAL="${PYTHON_SRC_STAGING_LOCAL:-/tmp/remotemedia-inprocess-python-src}"
PYTHON_STAGING_PATH="${PYTHON_STAGING_PATH:-/data/local/tmp/remotemedia-inprocess-python}"

NDK_VERSION="${NDK_VERSION:-25.2.9519653}"
SDK_PATH="${ANDROID_SDK_ROOT:-${ANDROID_HOME:-${HOME}/Android/Sdk}}"
NDK_PATH="${ANDROID_NDK_ROOT:-${SDK_PATH}/ndk/${NDK_VERSION}}"
CARGO_CONFIG="${ANDROID_PROJECT}/.cargo/config.toml"

export ANDROID_HOME="$SDK_PATH"
export ANDROID_SDK_ROOT="$SDK_PATH"
export ANDROID_NDK_ROOT="$NDK_PATH"
export PATH="${SDK_PATH}/platform-tools:${PATH}"

log() { echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $*"; }
success() { echo -e "${GREEN}[OK]${NC} $*"; }
warn() { echo -e "${YELLOW}[!]${NC} $*"; }
error() { echo -e "${RED}[ERR]${NC} $*"; }

# =============================================================================
# STEP 1: Verify Environment
# =============================================================================
verify_environment() {
    log "Verifying build environment..."

    if [[ ! -d "$SDK_PATH" ]]; then
        error "Android SDK not found at $SDK_PATH"
        exit 1
    fi
    success "Android SDK found: $SDK_PATH"
    
    # Check NDK
    if [[ ! -d "$NDK_PATH" ]]; then
        error "NDK not found at $NDK_PATH"
        exit 1
    fi
    success "NDK found: $NDK_PATH"
    
    # Check cargo config
    if [[ ! -f "$CARGO_CONFIG" ]]; then
        error "Cargo config not found. Run setup_cargo_config first."
        exit 1
    fi
    success "Cargo config found"
    
    # Check Rust targets
    rustup target list --installed | grep -q "aarch64-linux-android" || {
        error "aarch64-linux-android target not installed"
        exit 1
    }
    success "Rust Android targets installed"
    
    # Check adb
    if ! command -v adb &> /dev/null; then
        error "adb not in PATH"
        exit 1
    fi
    success "adb available"

    cat > "${ANDROID_PROJECT}/local.properties" << EOF
sdk.dir=${SDK_PATH}
ndk.dir=${NDK_PATH}
EOF
    success "local.properties configured"
}

# =============================================================================
# STEP 2: Setup Cargo Config (run once)
# =============================================================================
setup_cargo_config() {
    log "Setting up .cargo/config.toml for NDK cross-compilation..."
    mkdir -p "${ANDROID_PROJECT}/.cargo"
    cat > "$CARGO_CONFIG" << EOF
[target.aarch64-linux-android]
linker = "${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang"

[target.x86_64-linux-android]
linker = "${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/x86_64-linux-android24-clang"
EOF
    success "Cargo config created"
}

# =============================================================================
# STEP 3: Setup Python Library Symlink (run once)
# =============================================================================
setup_python_symlink() {
    log "Setting up Python library symlink for PyO3..."
    PYTHON_DIST="$PYTHON_NATIVE_LIBS_FALLBACK_SRC"
    if [[ -f "$PYTHON_DIST/libpython3.14.so" && ! -f "$PYTHON_DIST/libpython3.10.so" ]]; then
        ln -sf libpython3.14.so "$PYTHON_DIST/libpython3.10.so"
        success "Symlink created: libpython3.10.so -> libpython3.14.so"
    else
        warn "Python symlink already exists or libpython3.14.so not found"
    fi
}

# =============================================================================
# STEP 4: Build Rust cdylib
# =============================================================================
build_rust() {
    log "Building Rust cdylib for arm64-v8a..."
    cd "$ANDROID_PROJECT"

    CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
    CC_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
    CXX_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang++" \
    AR_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ar" \
    RANLIB_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ranlib" \
    LITERT_LM_LIB_DIR="${WORKSPACE_ROOT}/litert-lm-loadable-plugin/lib/aarch64-linux-android" \
    cargo build --release --target aarch64-linux-android 2>&1 | tail -20

    SO_FILE="target/aarch64-linux-android/release/libremotemedia_android_inprocess.so"
    if [[ ! -f "$SO_FILE" ]]; then
        error "Build failed: $SO_FILE not found"
        exit 1
    fi
    success "Rust cdylib built: $SO_FILE"
    
    # Copy to jniLibs
    mkdir -p app/src/main/jniLibs/arm64-v8a
    install -m 0644 "$SO_FILE" app/src/main/jniLibs/arm64-v8a/
    success "Copied to jniLibs/arm64-v8a/"

    log "Building Whisper loadable plugin for arm64-v8a (LiteRT backend)..."
    cd "${WORKSPACE_ROOT}/whisper"
    CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
    CC_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
    CXX_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang++" \
    AR_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ar" \
    RANLIB_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ranlib" \
    LITERT_LM_LIB_DIR="${WORKSPACE_ROOT}/litert-lm-loadable-plugin/lib/aarch64-linux-android" \
    cargo build --target aarch64-linux-android 2>&1 | tail -40
    cd "$ANDROID_PROJECT"

    WHISPER_PLUGIN="${WORKSPACE_ROOT}/whisper/target/aarch64-linux-android/debug/libwhisper_loadable_plugin.so"
    if [[ ! -f "$WHISPER_PLUGIN" ]]; then
        error "Whisper loadable plugin not found: $WHISPER_PLUGIN"
        exit 1
    fi
    mkdir -p app/src/main/assets/plugins
    install -m 0644 "$WHISPER_PLUGIN" app/src/main/assets/plugins/
    success "Copied Whisper loadable plugin to assets/plugins/"

    log "Building Silero VAD loadable plugin for arm64-v8a..."
    cd "${WORKSPACE_ROOT}/silero-vad"
    CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
    CC_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
    CXX_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang++" \
    AR_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ar" \
    RANLIB_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ranlib" \
    cargo build --target aarch64-linux-android 2>&1 | tail -40
    cd "$ANDROID_PROJECT"

    SILERO_VAD_PLUGIN="${WORKSPACE_ROOT}/silero-vad/target/aarch64-linux-android/debug/libsilero_vad_loadable_plugin.so"
    if [[ ! -f "$SILERO_VAD_PLUGIN" ]]; then
        error "Silero VAD loadable plugin not found: $SILERO_VAD_PLUGIN"
        exit 1
    fi
    mkdir -p app/src/main/assets/plugins
    install -m 0644 "$SILERO_VAD_PLUGIN" app/src/main/assets/plugins/
    success "Copied Silero VAD loadable plugin to assets/plugins/"

    log "Building Kokoro ONNX loadable plugin for arm64-v8a..."
    cd "${WORKSPACE_ROOT}/kokoro-onnx"
    CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
    CC_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
    CXX_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang++" \
    AR_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ar" \
    RANLIB_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ranlib" \
    cargo build --target aarch64-linux-android 2>&1 | tail -40
    cd "$ANDROID_PROJECT"

    KOKORO_PLUGIN="${WORKSPACE_ROOT}/kokoro-onnx/target/aarch64-linux-android/debug/libkokoro_onnx_plugin.so"
    if [[ ! -f "$KOKORO_PLUGIN" ]]; then
        error "Kokoro ONNX loadable plugin not found: $KOKORO_PLUGIN"
        exit 1
    fi
    mkdir -p app/src/main/assets/plugins
    install -m 0644 "$KOKORO_PLUGIN" app/src/main/assets/plugins/
    success "Copied Kokoro ONNX loadable plugin to assets/plugins/"

    log "Building Misaki G2P loadable plugin for arm64-v8a..."
    cd "${WORKSPACE_ROOT}/misaki-g2p"
    CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
    CC_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
    CXX_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang++" \
    AR_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ar" \
    RANLIB_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ranlib" \
    cargo build --target aarch64-linux-android 2>&1 | tail -40
    cd "$ANDROID_PROJECT"

    MISAKI_PLUGIN="${WORKSPACE_ROOT}/misaki-g2p/target/aarch64-linux-android/debug/libmisaki_g2p_plugin.so"
    if [[ ! -f "$MISAKI_PLUGIN" ]]; then
        error "Misaki G2P loadable plugin not found: $MISAKI_PLUGIN"
        exit 1
    fi
    mkdir -p app/src/main/assets/plugins
    install -m 0644 "$MISAKI_PLUGIN" app/src/main/assets/plugins/
    success "Copied Misaki G2P loadable plugin to assets/plugins/"

    log "Building LiteRT-LM loadable plugin for arm64-v8a..."
    cd "${WORKSPACE_ROOT}/litert-lm-loadable-plugin"
    CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
    CC_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
    CXX_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang++" \
    AR_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ar" \
    RANLIB_aarch64_linux_android="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ranlib" \
    LITERT_LM_LIB_DIR="${WORKSPACE_ROOT}/litert-lm-loadable-plugin/lib/aarch64-linux-android" \
    cargo build --target aarch64-linux-android 2>&1 | tail -40
    cd "$ANDROID_PROJECT"

    local python_native_libs_src="$PYTHON_NATIVE_LIBS_SRC"
    if [[ ! -d "$python_native_libs_src" ]]; then
        warn "Preferred Python native libs not found: $python_native_libs_src"
        python_native_libs_src="$PYTHON_NATIVE_LIBS_FALLBACK_SRC"
    fi
    if [[ ! -d "$python_native_libs_src" ]]; then
        error "Python native libs not found: $python_native_libs_src"
        exit 1
    fi
    install -m 0644 "$python_native_libs_src"/*.so app/src/main/jniLibs/arm64-v8a/
    success "Copied Python native libraries from $python_native_libs_src"

    LITERT_NATIVE="${WORKSPACE_ROOT}/litert-lm-loadable-plugin/lib/aarch64-linux-android/liblitert_lm.so"
    if [[ ! -f "$LITERT_NATIVE" ]]; then
        error "LiteRT-LM native library not found: $LITERT_NATIVE"
        exit 1
    fi
    install -m 0644 "$LITERT_NATIVE" app/src/main/jniLibs/arm64-v8a/
    success "Copied LiteRT-LM native library to jniLibs/arm64-v8a/"

    GEMMA_PROVIDER="${WORKSPACE_ROOT}/LiteRT-LM/prebuilt/android_arm64/libGemmaModelConstraintProvider.so"
    if [[ ! -f "$GEMMA_PROVIDER" ]]; then
        error "LiteRT-LM dependency not found: $GEMMA_PROVIDER"
        exit 1
    fi
    install -m 0644 "$GEMMA_PROVIDER" app/src/main/jniLibs/arm64-v8a/
    success "Copied LiteRT-LM dependency to jniLibs/arm64-v8a/"

    NDK_CXX_SHARED="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/sysroot/usr/lib/aarch64-linux-android/libc++_shared.so"
    if [[ ! -f "$NDK_CXX_SHARED" ]]; then
        error "NDK libc++ runtime not found: $NDK_CXX_SHARED"
        exit 1
    fi
    install -m 0644 "$NDK_CXX_SHARED" app/src/main/jniLibs/arm64-v8a/
    success "Copied NDK libc++ runtime to jniLibs/arm64-v8a/"

    LOADABLE_PLUGIN="${WORKSPACE_ROOT}/litert-lm-loadable-plugin/target/aarch64-linux-android/debug/liblitert_lm_loadable_plugin.so"
    if [[ ! -f "$LOADABLE_PLUGIN" ]]; then
        error "RemoteMedia loadable plugin not found: $LOADABLE_PLUGIN"
        exit 1
    fi
    mkdir -p app/src/main/assets/plugins
    install -m 0644 "$LOADABLE_PLUGIN" app/src/main/assets/plugins/
    success "Copied RemoteMedia loadable plugin to assets/plugins/"
}

# =============================================================================
# STEP 5: Build Gradle APK
# =============================================================================
build_apk() {
    log "Building Gradle APK (Release)..."
    cd "$ANDROID_PROJECT"
    
    ./gradlew assembleRelease --no-daemon 2>&1 | tail -40
    
    APK_FILE="app/build/outputs/apk/release/app-release.apk"
    if [[ ! -f "$APK_FILE" ]]; then
        error "APK not found: $APK_FILE"
        exit 1
    fi
    
    APK_SIZE=$(du -h "$APK_FILE" | cut -f1)
    success "APK built: $APK_FILE ($APK_SIZE)"

    verify_apk_contents "$APK_FILE"
}

verify_apk_contents() {
    local apk_file="$1"
    log "Verifying APK embeds required manifests and native libraries..."

    local required_entries=(
        "assets/plugins/libsilero_vad_loadable_plugin.so"
        "assets/plugins/libwhisper_loadable_plugin.so"
        "assets/plugins/liblitert_lm_loadable_plugin.so"
        "assets/plugins/libkokoro_onnx_plugin.so"
        "assets/plugins/libmisaki_g2p_plugin.so"
        "assets/manifests/llm-mobile.json"
        "assets/manifests/voice-assistant-mobile.json"
        "assets/manifests/tts-mobile.json"
        "assets/manifests/transcribe-mobile.json"
        "assets/have_a_wonderful_day.wav"
        "lib/arm64-v8a/libremotemedia_android_inprocess.so"
        "lib/arm64-v8a/libc++_shared.so"
        "lib/arm64-v8a/libGemmaModelConstraintProvider.so"
        "lib/arm64-v8a/liblitert_lm.so"
    )

    local missing=0
    local apk_entries
    apk_entries="$(zipinfo -1 "$apk_file")"

    for entry in "${required_entries[@]}"; do
        if grep -Fxq "$entry" <<< "$apk_entries"; then
            success "APK contains $entry"
        else
            error "APK missing $entry"
            missing=1
        fi
    done

    if [[ "$missing" -ne 0 ]]; then
        error "APK embedding verification failed"
        exit 1
    fi
}

# =============================================================================
# STEP 6: Deploy to Device
# =============================================================================
deploy_to_device() {
    log "Deploying to device: $DEVICE_ADDRESS"

    # Connect if WiFi
    if [[ "$DEVICE_ADDRESS" == *":"* ]]; then
        adb connect "$DEVICE_ADDRESS" 2>&1 | tail -1
    fi

    if ! adb devices | grep -q "$DEVICE_ADDRESS"; then
        error "Device not connected: $DEVICE_ADDRESS"
        warn "Pair/connect manually, then rerun: adb pair <IP>:<PAIR_PORT> <CODE>; adb connect $DEVICE_ADDRESS"
        exit 1
    fi
    
    # Install APK
    adb -s "$DEVICE_ADDRESS" install -r "$ANDROID_PROJECT/app/build/outputs/apk/release/app-release.apk"
    success "APK installed"
    
    success "Plugin is embedded in APK assets; PipelineManager will extract it to app files dir"
    copy_model_to_device
    copy_silero_vad_assets_to_device
    copy_whisper_assets_to_device
    copy_kokoro_assets_to_device
    copy_misaki_g2p_assets_to_device
    copy_python_to_device
}

copy_silero_vad_assets_to_device() {
    log "Ensuring Silero VAD assets are present in app-private files"
    if ! adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess id >/dev/null 2>&1; then
        error "run-as failed. Release build must be debuggable to copy Silero VAD assets into app-private files."
        exit 1
    fi

    copy_one_model_asset "$SILERO_VAD_MODEL_SRC" "$SILERO_VAD_STAGING_DIR" "files/models/silero-vad" "silero_vad.onnx" "Silero VAD" "true"

    log "Silero VAD asset diagnostics:"
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess ls -lh files/models/silero-vad || true
}

copy_model_to_device() {
    if [[ ! -f "$MODEL_SRC" ]]; then
        error "LiteRT-LM model not found: $MODEL_SRC"
        exit 1
    fi

    local model_size
    model_size="$(stat -c%s "$MODEL_SRC")"

    log "Ensuring model is present in app-private files: $MODEL_DEVICE_PATH (${model_size} bytes)"
    if ! adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess id >/dev/null 2>&1; then
        error "run-as failed. Release build must be debuggable to copy the model into app-private files."
        exit 1
    fi

    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess mkdir -p files/models
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess mkdir -p files/cache/litert-lm

    local device_size
    device_size="$(adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess stat -c%s files/models/gemma-4-E2B-it.litertlm 2>/dev/null | tr -d '\r' || true)"
    device_size="${device_size:-0}"

    if [[ "$device_size" == "$model_size" ]]; then
        success "Model already present on device with matching size"
        return
    fi

    warn "App-private model size is ${device_size}; staging model for run-as copy"
    local staging_size
    staging_size="$(adb -s "$DEVICE_ADDRESS" shell "stat -c%s '$MODEL_STAGING_PATH' 2>/dev/null || echo 0" | tr -d '\r')"
    if [[ "$staging_size" != "$model_size" ]]; then
        adb -s "$DEVICE_ADDRESS" push "$MODEL_SRC" "$MODEL_STAGING_PATH"
    else
        success "Staged model already present with matching size"
    fi

    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess cp "$MODEL_STAGING_PATH" files/models/gemma-4-E2B-it.litertlm

    device_size="$(adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess stat -c%s files/models/gemma-4-E2B-it.litertlm 2>/dev/null | tr -d '\r' || true)"
    device_size="${device_size:-0}"
    if [[ "$device_size" != "$model_size" ]]; then
        error "Model push failed or size mismatch: local=${model_size}, device=${device_size}"
        exit 1
    fi

    success "Model copied to device"
}

copy_one_whisper_asset() {
    local src="$1"
    local dst_name="$2"
    local required="$3"

    if [[ ! -f "$src" ]]; then
        if [[ "$required" == "true" ]]; then
            error "Whisper asset not found: $src"
            exit 1
        fi
        warn "Optional Whisper asset not found: $src"
        return
    fi

    local src_size
    src_size="$(stat -c%s "$src")"
    local staged_path="${WHISPER_STAGING_DIR}/${dst_name}"

    log "Staging Whisper asset $dst_name (${src_size} bytes)"
    adb -s "$DEVICE_ADDRESS" shell "mkdir -p '$WHISPER_STAGING_DIR'"
    local staging_size
    staging_size="$(adb -s "$DEVICE_ADDRESS" shell "stat -c%s '$staged_path' 2>/dev/null || echo 0" | tr -d '\r')"
    if [[ "$staging_size" != "$src_size" ]]; then
        adb -s "$DEVICE_ADDRESS" push "$src" "$staged_path"
    else
        success "Staged Whisper asset already present: $dst_name"
    fi

    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess cp "$staged_path" "files/models/whisper/$dst_name"

    local device_size
    device_size="$(adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess stat -c%s "files/models/whisper/$dst_name" 2>/dev/null | tr -d '\r' || true)"
    device_size="${device_size:-0}"
    if [[ "$device_size" != "$src_size" ]]; then
        error "Whisper asset copy failed or size mismatch for $dst_name: local=${src_size}, device=${device_size}"
        exit 1
    fi
    success "Whisper asset copied: $dst_name"
}

copy_whisper_assets_to_device() {
    log "Ensuring LiteRT Whisper assets are present in app-private files"
    if ! adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess id >/dev/null 2>&1; then
        error "run-as failed. Release build must be debuggable to copy Whisper assets into app-private files."
        exit 1
    fi
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess mkdir -p files/models/whisper

    copy_one_whisper_asset "$WHISPER_MODEL_SRC" "whisper_tiny_30s_f32.tflite" "true"
    copy_one_whisper_asset "$WHISPER_BASE_MODEL_SRC" "whisper_base_30s_f32.tflite" "false"
    copy_one_whisper_asset "$WHISPER_TOKENIZER_SRC" "tokenizer.json" "true"
    copy_one_whisper_asset "$WHISPER_CONFIG_SRC" "config.json" "false"

    log "Whisper asset diagnostics:"
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess ls -lh files/models/whisper || true
}

copy_one_model_asset() {
    local src="$1"
    local staged_dir="$2"
    local device_dir="$3"
    local dst_name="$4"
    local label="$5"
    local required="$6"

    if [[ ! -f "$src" ]]; then
        if [[ "$required" == "true" ]]; then
            error "$label asset not found: $src"
            warn "Provide it at the path above or override the corresponding *_SRC environment variable."
            exit 1
        fi
        warn "Optional $label asset not found: $src"
        return
    fi

    local src_size
    src_size="$(stat -c%s "$src")"
    local staged_path="${staged_dir}/${dst_name}"

    log "Staging $label asset $dst_name (${src_size} bytes)"
    adb -s "$DEVICE_ADDRESS" shell "mkdir -p '$staged_dir'"
    local staging_size
    staging_size="$(adb -s "$DEVICE_ADDRESS" shell "stat -c%s '$staged_path' 2>/dev/null || echo 0" | tr -d '\r')"
    if [[ "$staging_size" != "$src_size" ]]; then
        adb -s "$DEVICE_ADDRESS" push "$src" "$staged_path"
    else
        success "Staged $label asset already present: $dst_name"
    fi

    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess mkdir -p "$device_dir"
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess cp "$staged_path" "${device_dir}/${dst_name}"

    local device_size
    device_size="$(adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess stat -c%s "${device_dir}/${dst_name}" 2>/dev/null | tr -d '\r' || true)"
    device_size="${device_size:-0}"
    if [[ "$device_size" != "$src_size" ]]; then
        error "$label asset copy failed or size mismatch for $dst_name: local=${src_size}, device=${device_size}"
        exit 1
    fi
    success "$label asset copied: $dst_name"
}

copy_kokoro_assets_to_device() {
    log "Ensuring Kokoro ONNX assets are present in app-private files"
    if ! adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess id >/dev/null 2>&1; then
        error "run-as failed. Release build must be debuggable to copy Kokoro assets into app-private files."
        exit 1
    fi

    copy_one_model_asset "$KOKORO_MODEL_SRC" "$KOKORO_STAGING_DIR/onnx" "files/models/kokoro/onnx" "$KOKORO_MODEL_NAME" "Kokoro" "true"
    copy_one_model_asset "$KOKORO_TOKENIZER_SRC" "$KOKORO_STAGING_DIR" "files/models/kokoro" "tokenizer.json" "Kokoro" "true"
    copy_one_model_asset "$KOKORO_VOICE_SRC" "$KOKORO_STAGING_DIR/voices" "files/models/kokoro/voices" "af_bella.bin" "Kokoro" "true"

    log "Kokoro asset diagnostics:"
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess ls -lh files/models/kokoro files/models/kokoro/onnx files/models/kokoro/voices || true
}

resolve_misaki_resource_src() {
    if [[ -d "$MISAKI_G2P_RESOURCE_SRC" ]]; then
        echo "$MISAKI_G2P_RESOURCE_SRC"
    elif [[ -d "$MISAKI_G2P_RESOURCE_FALLBACK_SRC" ]]; then
        warn "Using bundled Misaki G2P fixture resources: $MISAKI_G2P_RESOURCE_FALLBACK_SRC" >&2
        echo "$MISAKI_G2P_RESOURCE_FALLBACK_SRC"
    else
        error "Misaki G2P resources not found: $MISAKI_G2P_RESOURCE_SRC"
        warn "Provide production resources there or set MISAKI_G2P_RESOURCE_SRC."
        exit 1
    fi
}

copy_misaki_g2p_assets_to_device() {
    log "Ensuring Misaki G2P resources are present in app-private files"
    if ! adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess id >/dev/null 2>&1; then
        error "run-as failed. Release build must be debuggable to copy Misaki G2P assets into app-private files."
        exit 1
    fi

    local src_root
    src_root="$(resolve_misaki_resource_src)"
    copy_one_model_asset "${src_root}/en-US/gold.json" "$MISAKI_G2P_STAGING_DIR/en-US" "files/models/misaki-g2p/en-US" "gold.json" "Misaki G2P" "true"
    copy_one_model_asset "${src_root}/en-US/silver.json" "$MISAKI_G2P_STAGING_DIR/en-US" "files/models/misaki-g2p/en-US" "silver.json" "Misaki G2P" "true"
    copy_one_model_asset "${src_root}/en-GB/gold.json" "$MISAKI_G2P_STAGING_DIR/en-GB" "files/models/misaki-g2p/en-GB" "gold.json" "Misaki G2P" "false"
    copy_one_model_asset "${src_root}/en-GB/silver.json" "$MISAKI_G2P_STAGING_DIR/en-GB" "files/models/misaki-g2p/en-GB" "silver.json" "Misaki G2P" "false"

    log "Misaki G2P asset diagnostics:"
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess ls -lh files/models/misaki-g2p files/models/misaki-g2p/en-US || true
}

copy_python_to_device() {
    local python_bundle_src="$PYTHON_BUNDLE_SRC"
    if [[ ! -d "$python_bundle_src" ]]; then
        warn "Preferred Python-for-Android bundle not found: $python_bundle_src"
        python_bundle_src="$PYTHON_BUNDLE_FALLBACK_SRC"
    fi

    if [[ ! -d "$python_bundle_src" ]]; then
        error "Python-for-Android bundle not found: $python_bundle_src"
        exit 1
    fi

    if [[ ! -d "$PYTHON_SRC" ]]; then
        error "RemoteMedia Python sources not found: $PYTHON_SRC"
        exit 1
    fi

    log "Staging Python runtime and RemoteMedia Python sources for app-private execution..."
    log "Using Python bundle: $python_bundle_src"
    rm -rf "$PYTHON_SRC_STAGING_LOCAL"
    mkdir -p "$PYTHON_SRC_STAGING_LOCAL"
    cp -aL "$PYTHON_SRC"/. "$PYTHON_SRC_STAGING_LOCAL"/
    
    # Also stage Hermes Agent source for in-process imports
    log "Staging Hermes Agent source for in-process imports..."
    HERMES_SRC="${WORKSPACE_ROOT}/hermes-agent"
    if [[ -d "$HERMES_SRC" ]]; then
        # Copy the hermes_agent package directory
        cp -aL "$HERMES_SRC" "$PYTHON_SRC_STAGING_LOCAL/hermes_agent"
        # Copy all root-level Python files so absolute imports work
        cp "$HERMES_SRC"/*.py "$PYTHON_SRC_STAGING_LOCAL/" 2>/dev/null || true
        # Copy entire agent directory (already in hermes_agent/agent but ensure root-level agent imports work)
        cp -aL "$HERMES_SRC/agent" "$PYTHON_SRC_STAGING_LOCAL/" 2>/dev/null || true
        cp -aL "$HERMES_SRC/tools" "$PYTHON_SRC_STAGING_LOCAL/" 2>/dev/null || true
        success "Hermes Agent source staged at $PYTHON_SRC_STAGING_LOCAL/"
    else
        warn "Hermes Agent source not found at $HERMES_SRC - imports will fail"
    fi
    cat > "$PYTHON_SRC_STAGING_LOCAL/remotemedia/__init__.py" <<'PY'
"""
Android in-process staging initializer.

Keep package import side effects minimal so explicit node imports do not pull
desktop-only optional dependencies such as aiortc, av, torch, or kokoro.
"""

__path__ = __import__("pkgutil").extend_path(__path__, __name__)
__version__ = "0.1.0"
PY
    cat > "$PYTHON_SRC_STAGING_LOCAL/remotemedia/nodes/__init__.py" <<'PY'
"""
Android in-process staging initializer for node packages.

Explicit imports like `remotemedia.nodes.ml.whisper_stt.WhisperSTTNode` should
not import every desktop node module as a side effect.
"""

from .registration import (
    NodeRegistration,
    discover_and_register,
    export_to_json,
    export_to_rust,
    get_node_registration,
    get_registered_nodes,
    streaming_node,
)
from .loader import (
    get_loaded_nodes,
    get_node_class,
    register_node_class,
    register_python_node,
    register_python_nodes_from_config,
)

__all__ = [
    "NodeRegistration",
    "discover_and_register",
    "export_to_json",
    "export_to_rust",
    "get_node_registration",
    "get_registered_nodes",
    "streaming_node",
    "get_loaded_nodes",
    "get_node_class",
    "register_node_class",
    "register_python_node",
    "register_python_nodes_from_config",
]
PY
    cat > "$PYTHON_SRC_STAGING_LOCAL/remotemedia/nodes/android_inprocess.py" <<'PY'
"""
Android in-process adapters for the PyO3 bridge.

The bridge instantiates classes without constructor args, calls
initialize(config) synchronously, and passes plain Python values converted from
RuntimeData. These adapters intentionally avoid desktop-only node imports.
"""

import math
from types import SimpleNamespace


def _audio_object(data, samples=None, sample_rate=None, channels=None, metadata=None):
    if isinstance(data, dict):
        samples = data.get("samples", samples)
        sample_rate = data.get("sample_rate", sample_rate)
        channels = data.get("channels", channels)
        metadata = data.get("metadata", metadata)
        stream_id = data.get("stream_id")
        timestamp_us = data.get("timestamp_us")
        arrival_ts_us = data.get("arrival_ts_us")
    else:
        stream_id = getattr(data, "stream_id", None)
        timestamp_us = getattr(data, "timestamp_us", None)
        arrival_ts_us = getattr(data, "arrival_ts_us", None)

    out = SimpleNamespace(
        data_type="audio",
        samples=list(samples or []),
        sample_rate=int(sample_rate or 16000),
        channels=int(channels or 1),
    )
    if stream_id is not None:
        out.stream_id = stream_id
    if timestamp_us is not None:
        out.timestamp_us = int(timestamp_us)
    if arrival_ts_us is not None:
        out.arrival_ts_us = int(arrival_ts_us)
    if metadata is not None:
        out.metadata = metadata
    return out


def _audio_stats(data):
    if isinstance(data, dict):
        samples = data.get("samples") or []
        sample_rate = int(data.get("sample_rate") or 16000)
        channels = int(data.get("channels") or 1)
    else:
        samples = getattr(data, "samples", []) or []
        sample_rate = int(getattr(data, "sample_rate", 16000) or 16000)
        channels = int(getattr(data, "channels", 1) or 1)

    if not samples:
        return 0, sample_rate, channels, 0.0
    total = 0.0
    peak = 0.0
    for sample in samples[: min(len(samples), 48000)]:
        value = float(sample)
        total += value * value
        peak = max(peak, abs(value))
    rms = math.sqrt(total / min(len(samples), 48000))
    return len(samples), sample_rate, channels, max(rms, peak)


class VADNode:
    def initialize(self, config):
        self.config = dict(config or {})
        self.energy_threshold = float(self.config.get("energy_threshold", 0.02))
        self.frames_seen = 0
        print(
            "AndroidInProcess VADNode initialized "
            f"threshold={self.energy_threshold} config={self.config}",
            flush=True,
        )

    def process(self, data):
        self.frames_seen += 1
        sample_count, sample_rate, channels, energy = _audio_stats(data)
        if self.frames_seen <= 3 or self.frames_seen % 10 == 0:
            print(
                "AndroidInProcess VADNode process "
                f"frame={self.frames_seen} samples={sample_count} energy={energy:.5f}",
                flush=True,
            )

        if energy < self.energy_threshold:
            if self.frames_seen <= 3 or self.frames_seen % 10 == 0:
                print(
                    "AndroidInProcess VADNode suppressing silence "
                    f"frame={self.frames_seen} energy={energy:.5f}",
                    flush=True,
                )
            return ""

        metadata = {}
        if isinstance(data, dict) and isinstance(data.get("metadata"), dict):
            metadata.update(data["metadata"])
        metadata["android_inprocess_vad"] = {
            "is_speech": bool(energy >= self.energy_threshold),
            "energy": energy,
            "sample_count": sample_count,
        }
        return _audio_object(data, sample_rate=sample_rate, channels=channels, metadata=metadata)

    def process_streaming(self, data):
        yield self.process(data)


class WhisperSTTNode:
    def initialize(self, config):
        self.config = dict(config or {})
        self.frames_seen = 0
        print(
            "AndroidInProcess WhisperSTTNode compatibility adapter initialized. "
            "Use native WhisperNode from libwhisper_loadable_plugin.so for LiteRT ASR.",
            flush=True,
        )

    def process(self, data):
        self.frames_seen += 1
        sample_count, sample_rate, channels, energy = _audio_stats(data)
        print(
            "AndroidInProcess WhisperSTTNode compatibility adapter suppressed "
            f"frame={self.frames_seen} samples={sample_count} energy={energy:.5f}",
            flush=True,
        )
        return ""

    def process_streaming(self, data):
        output = self.process(data)
        if output:
            yield output


class DebugKokoroTTSNode:
    def initialize(self, config):
        self.config = dict(config or {})
        self.sample_rate = int(self.config.get("sample_rate", 24000))
        print(
            "AndroidInProcess DebugKokoroTTSNode debug adapter initialized. "
            "Desktop KokoroTTSNode requires async execution plus kokoro/soundfile "
            "and is not directly runnable by the current PyO3 bridge.",
            flush=True,
        )

    def process(self, data):
        text = data if isinstance(data, str) else str(data)
        print(
            "AndroidInProcess DebugKokoroTTSNode process "
            f"text_preview={text[:80]!r}",
            flush=True,
        )
        duration_s = 0.25
        count = int(self.sample_rate * duration_s)
        frequency = 660.0 if text.strip() else 330.0
        amplitude = 0.08
        samples = [
            amplitude * math.sin(2.0 * math.pi * frequency * (i / self.sample_rate))
            for i in range(count)
        ]
        return _audio_object(
            {},
            samples=samples,
            sample_rate=self.sample_rate,
            channels=1,
            metadata={
                "android_inprocess_tts": {
                    "debug": True,
                    "input_preview": text[:160],
                }
            },
        )

    def process_streaming(self, data):
        yield self.process(data)


class DataSinkNode:
    def initialize(self, config):
        self.config = dict(config or {})
        self.total_processed = 0
        print("AndroidInProcess DataSinkNode initialized", flush=True)

    def process(self, data):
        self.total_processed += 1
        print(
            "AndroidInProcess DataSinkNode processed "
            f"count={self.total_processed} type={type(data).__name__}",
            flush=True,
        )
        return data

    def process_streaming(self, data):
        yield self.process(data)


class HermesAgentTestPlugin:
    """Test plugin to verify Hermes Agent imports work in-process on Android."""
    
    def initialize(self, config):
        self.config = dict(config or {})
        self.imports_ok = False
        self.import_error = ""
        self.imported_modules = []
        try:
            # Test core Hermes Agent imports (with hermes_agent prefix since source is in hermes_agent/ dir)
            from hermes_agent.run_agent import AIAgent
            self.imported_modules.append("hermes_agent.run_agent.AIAgent")
            
            from hermes_agent.agent.conversation_loop import run_conversation_loop
            self.imported_modules.append("hermes_agent.agent.conversation_loop.run_conversation_loop")
            
            from hermes_agent.model_tools import get_tool_definitions, handle_function_call
            self.imported_modules.append("hermes_agent.model_tools.get_tool_definitions")
            self.imported_modules.append("hermes_agent.model_tools.handle_function_call")
            
            from hermes_agent.tools.registry import get_tool_definitions as get_registry_tools
            self.imported_modules.append("hermes_agent.tools.registry.get_tool_definitions")
            
            self.imports_ok = True
            print(f"HermesAgentTestPlugin imports OK: {self.imported_modules}", flush=True)
        except ImportError as e:
            self.imports_ok = False
            self.import_error = str(e)
            print(f"HermesAgentTestPlugin import failed: {e}", flush=True)
    
    def process(self, data):
        if self.imports_ok:
            return {
                "data_type": "text",
                "text": "Hermes Agent imports: OK",
                "modules": self.imported_modules,
            }
        else:
            return {
                "data_type": "text",
                "text": f"Hermes Agent imports: FAILED - {self.import_error}",
                "error": self.import_error,
            }
    
    def process_streaming(self, data):
        yield self.process(data)
    
    def finalize(self):
        return True

PY

    adb -s "$DEVICE_ADDRESS" shell "rm -rf '$PYTHON_STAGING_PATH' && mkdir -p '$PYTHON_STAGING_PATH'"
    adb -s "$DEVICE_ADDRESS" push "$python_bundle_src" "$PYTHON_STAGING_PATH/bundle"
    adb -s "$DEVICE_ADDRESS" push "$PYTHON_SRC_STAGING_LOCAL" "$PYTHON_STAGING_PATH/src"

    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess rm -rf files/python
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess mkdir -p files/python
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess cp -R "$PYTHON_STAGING_PATH/bundle" files/python/bundle
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess cp -R "$PYTHON_STAGING_PATH/src" files/python/src

    # Copy libpythonbin.so and all required .so libraries to the bundle directory so python3 wrapper can find them
    # The libraries are in the p4a distro's libs/arm64-v8a/ directory (at distro root, not inside _python_bundle__arm64-v8a)
    local DISTRO_ROOT="${python_bundle_src%%/_python_bundle__arm64-v8a*}"
    local PYTHON_LIBS_SRC="${DISTRO_ROOT}/libs/arm64-v8a"
    if [[ -f "$PYTHON_LIBS_SRC/libpythonbin.so" ]]; then
        log "Copying Python libraries to bundle directory..."
        # Copy all .so files from the libs directory
        for lib_file in "$PYTHON_LIBS_SRC"/*.so; do
            if [[ -f "$lib_file" ]]; then
                lib_name=$(basename "$lib_file")
                adb -s "$DEVICE_ADDRESS" push "$lib_file" "/data/local/tmp/$lib_name"
                adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess cp "/data/local/tmp/$lib_name" "files/python/bundle/$lib_name"
                adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess chmod +x "files/python/bundle/$lib_name"
            fi
        done
    fi

    # Verify Python runtime (Hermes Agent source is included in staging)
    adb -s "$DEVICE_ADDRESS" shell "run-as com.remotemedia.inprocess sh -c 'ls -lh files/python/bundle/stdlib.zip && ls -ld files/python/bundle/modules files/python/bundle/site-packages files/python/src/remotemedia/nodes files/python/src/hermes_agent && test -d files/python/bundle/site-packages/numpy && test -f files/python/src/remotemedia/nodes/ml/whisper_stt.py && test -f files/python/src/remotemedia/nodes/tts.py'" || {
        error "Python runtime staging verification failed"
        exit 1
    }

    # Install missing PyPI dependencies on device (PyYAML, etc.)
    log "Installing missing PyPI dependencies on device (PyYAML, etc.)..."
    
    cat > /tmp/python3_wrapper << 'PYEOF'
#!/system/bin/sh
# Set LD_LIBRARY_PATH so libpythonbin.so can find libpython3.14.so and other deps
export LD_LIBRARY_PATH="/data/data/com.remotemedia.inprocess/files/python/bundle:${LD_LIBRARY_PATH}"
# Set PYTHONHOME so Python can find stdlib.zip (encodings module, etc.)
export PYTHONHOME="/data/data/com.remotemedia.inprocess/files/python/bundle"
# Also set PYTHONPATH explicitly
export PYTHONPATH="/data/data/com.remotemedia.inprocess/files/python/bundle/stdlib.zip:/data/data/com.remotemedia.inprocess/files/python/bundle/modules:/data/data/com.remotemedia.inprocess/files/python/bundle/site-packages"
# Debug output
echo "DEBUG: PYTHONHOME=$PYTHONHOME" >&2
echo "DEBUG: PYTHONPATH=$PYTHONPATH" >&2
echo "DEBUG: LD_LIBRARY_PATH=$LD_LIBRARY_PATH" >&2
ls -la "$PYTHONHOME/" >&2
# Try multiple possible locations for libpythonbin.so
for libpath in \
    "/data/data/com.remotemedia.inprocess/lib/libpythonbin.so" \
    "/data/data/com.remotemedia.inprocess/lib/arm64/libpythonbin.so" \
    "/data/app/com.remotemedia.inprocess-*/lib/arm64/libpythonbin.so" \
    "/data/data/com.remotemedia.inprocess/lib/libpython3.14.so" \
    "/data/app/com.remotemedia.inprocess-*/lib/arm64/libpythonbin.so" \
    "/data/app/com.remotemedia.inprocess-*/lib/libpythonbin.so" \
    "/data/app/com.remotemedia.inprocess*/lib/arm64/libpythonbin.so" \
    "/data/data/com.remotemedia.inprocess/lib/main-*/libpythonbin.so" \
    "/data/app/~~*/com.remotemedia.inprocess*/lib/arm64/libpythonbin.so" \
    "/data/app/~~*/com.remotemedia.inprocess*/lib/libpythonbin.so" \
    "/data/app/~~*/com.remotemedia.inprocess*/lib/pythonbin.so" \
    "/data/user/0/com.remotemedia.inprocess/lib/arm64/libpythonbin.so" \
    "/data/data/com.remotemedia.inprocess/lib/libpython3.14.so" \
    "/data/data/com.remotemedia.inprocess/files/python/bundle/libpythonbin.so" \
    "/data/data/com.remotemedia.inprocess/files/python/bundle/libpython3.14.so"; do
    if [ -x "$libpath" ]; then
        exec "$libpath" "$@"
    fi
done
# Fallback: try to find it using find with wider search
for libpath in $(find /data/data/com.remotemedia.inprocess -name "libpythonbin.so" -type f 2>/dev/null; find /data/app/com.remotemedia.inprocess* -name "libpythonbin.so" -type f 2>/dev/null; find /data/app/~~* -name "libpythonbin.so" -type f 2>/dev/null; find /data/data/com.remotemedia.inprocess/files -name "libpythonbin.so" -type f 2>/dev/null); do
    if [ -x "$libpath" ]; then
        exec "$libpath" "$@"
    fi
done
echo "ERROR: libpythonbin.so not found in any location" >&2
exit 1
PYEOF
    adb -s "$DEVICE_ADDRESS" push /tmp/python3_wrapper /data/local/tmp/python3_wrapper 2>&1
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess cp /data/local/tmp/python3_wrapper /data/data/com.remotemedia.inprocess/files/python3 2>&1
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess chmod +x /data/data/com.remotemedia.inprocess/files/python3 2>&1
    # Also copy to bundle directory as python3 for direct invocation
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess cp /data/local/tmp/python3_wrapper /data/data/com.remotemedia.inprocess/files/python/bundle/python3 2>&1
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess chmod +x /data/data/com.remotemedia.inprocess/files/python/bundle/python3 2>&1
    rm -f /tmp/python3_wrapper
    


    # Write install script to device using Python to avoid shell escaping issues
    cat > /tmp/deps_install.py << 'PYEOF'
import subprocess
import sys
import os

env = {
    **os.environ,
    "PYTHONPATH": "/data/data/com.remotemedia.inprocess/files/python/bundle/site-packages:/data/data/com.remotemedia.inprocess/files/python/bundle/modules:/data/data/com.remotemedia.inprocess/files/python/src",
    "PYTHONHOME": "/data/data/com.remotemedia.inprocess/files/python/bundle",
    "PATH": "/data/data/com.remotemedia.inprocess/files/python/bundle:/system/bin:/vendor/bin",
    "TMPDIR": "/data/data/com.remotemedia.inprocess/files/tmp",
    "TMP": "/data/data/com.remotemedia.inprocess/files/tmp",
    "TEMP": "/data/data/com.remotemedia.inprocess/files/tmp",
    "PIP_CACHE_DIR": "/data/data/com.remotemedia.inprocess/files/tmp/pip-cache",
    "PIP_NO_CACHE_DIR": "1",
    "HOME": "/data/data/com.remotemedia.inprocess/files",
    "XDG_CACHE_HOME": "/data/data/com.remotemedia.inprocess/files/tmp",
}

# First, bootstrap pip using get-pip.py (p4a doesn't include ensurepip)
print("Bootstrapping pip with get-pip.py...", flush=True)
get_pip_path = "/data/local/tmp/get-pip.py"

# Run get-pip.py
result = subprocess.run([
    "/data/data/com.remotemedia.inprocess/files/python/bundle/python3",
    get_pip_path
], env=env, capture_output=True, text=True, timeout=120)
print(f"get-pip.py stdout: {result.stdout}", flush=True)
print(f"get-pip.py stderr: {result.stderr}", flush=True)
print(f"get-pip.py returncode: {result.returncode}", flush=True)

if result.returncode != 0:
    print("Failed to bootstrap pip", flush=True)
    sys.exit(result.returncode)

# Then install pip packages
packages = [
    "PyYAML==6.0.3",
    "requests==2.33.0",
    "urllib3==2.2.0",
    "charset-normalizer==3.3.0",
    "idna==3.6",
    "certifi==2024.2.2",
    "pydantic==2.13.4",
    "pydantic-core==2.18.4",
    "typing-extensions==4.11.0",
    "annotated-types==0.7.0",
    "python-dotenv==1.2.2",
]

print("Installing pip packages...", flush=True)
result = subprocess.run([
    "/data/data/com.remotemedia.inprocess/files/python/bundle/python3",
    "-m", "pip", "install", "--timeout", "300"
] + packages, env=env, capture_output=True, text=True, timeout=300)
print(f"pip install stdout: {result.stdout}", flush=True)
print(f"pip install stderr: {result.stderr}", flush=True)
print(f"pip install returncode: {result.returncode}", flush=True)

sys.exit(result.returncode)
PYEOF
    adb -s "$DEVICE_ADDRESS" push /tmp/deps_install.py /data/local/tmp/deps_install.py 2>&1
    rm -f /tmp/deps_install.py
    
    # Execute the Python install script via run-as - run from /data/local/tmp 
    log "Installing PyPI dependencies on device (with ensurepip bootstrap)..."
    adb -s "$DEVICE_ADDRESS" shell "run-as com.remotemedia.inprocess sh -c 'cd /data/local/tmp && PYTHONHOME=/data/data/com.remotemedia.inprocess/files/python/bundle PYTHONPATH=/data/data/com.remotemedia.inprocess/files/python/bundle/stdlib.zip:/data/data/com.remotemedia.inprocess/files/python/bundle/modules:/data/data/com.remotemedia.inprocess/files/python/bundle/site-packages /data/data/com.remotemedia.inprocess/files/python/bundle/python3 /data/local/tmp/deps_install.py 2>&1'" | tail -50
    
    # Verify PyYAML is importable
    log "Verifying PyYAML installation..."
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess sh -c '
        export PYTHONPATH="/data/data/com.remotemedia.inprocess/files/python/bundle/site-packages:/data/data/com.remotemedia.inprocess/files/python/bundle/modules:/data/data/com.remotemedia.inprocess/files/python/src"
        export PYTHONHOME="/data/data/com.remotemedia.inprocess/files/python/bundle"
        export PATH="/data/data/com.remotemedia.inprocess/files/python/bundle:$PATH"
        cd /data/data/com.remotemedia.inprocess/files/python/bundle
        /data/data/com.remotemedia.inprocess/files/python/bundle/python3 -c "import yaml; print(\"PyYAML OK\")" 2>&1
    ' 2>&1
    
    # Clean up
    adb -s "$DEVICE_ADDRESS" shell rm /data/local/tmp/deps_install.py
    
    success "Python runtime staged in app-private files"
}
# =============================================================================
# STEP 7: Run App & Test
# =============================================================================
run_and_test() {
    log "Starting app and running tests..."
    
    # Stop any existing instance
    adb -s "$DEVICE_ADDRESS" shell am force-stop com.remotemedia.inprocess
    sleep 1
    adb -s "$DEVICE_ADDRESS" logcat -c
    adb -s "$DEVICE_ADDRESS" shell pm grant com.remotemedia.inprocess android.permission.RECORD_AUDIO 2>/dev/null || true

    log "Verifying app-private model before launch..."
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess ls -lh files/models/gemma-4-E2B-it.litertlm files/cache/litert-lm || true
    log "Verifying app-private Whisper assets before launch..."
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess ls -lh files/models/whisper || true
    log "Verifying app-private Kokoro and Misaki G2P assets before launch..."
    adb -s "$DEVICE_ADDRESS" shell run-as com.remotemedia.inprocess ls -lh files/models/kokoro files/models/kokoro/onnx files/models/kokoro/voices files/models/misaki-g2p files/models/misaki-g2p/en-US || true
    log "Verifying app-private Python runtime before launch..."
    adb -s "$DEVICE_ADDRESS" shell "run-as com.remotemedia.inprocess sh -c 'ls -lh files/python/bundle/stdlib.zip && ls -ld files/python/bundle/modules files/python/bundle/site-packages files/python/bundle/site-packages/numpy files/python/src/remotemedia/nodes'" || true
    
    # Start MainActivity and ask it to start streaming as soon as the manifest is ready.
    adb -s "$DEVICE_ADDRESS" shell am start -n com.remotemedia.inprocess/.MainActivity --ez auto_start true --ez simulate_speech true --es pipeline "$TEST_PIPELINE"
    
    local wait_seconds="${TEST_WAIT_SECONDS:-45}"
    if [[ -z "${TEST_WAIT_SECONDS:-}" && "$TEST_PIPELINE" == "tts-mobile.json" ]]; then
        wait_seconds=130
    fi
    log "Waiting for auto-started pipeline execution (${wait_seconds}s)..."
    sleep "$wait_seconds"
    
    # Capture logs
    log "Capturing full logcat to $LOG_OUTPUT"
    adb -s "$DEVICE_ADDRESS" logcat -d > "$LOG_OUTPUT" 2>&1

    log "Current activity state:"
    adb -s "$DEVICE_ADDRESS" shell dumpsys activity activities 2>/dev/null \
        | grep -E "ResumedActivity|topResumedActivity|Hist #0|mFocusedApp|mCurrentFocus" \
        | tail -20 || true

    log "Focused debug logs:"
    grep -E "RemoteMedia|PipelineManager|MainActivity|NativeInterface|AudioRecorder|AudioPlayer|AndroidRuntime|libc|DEBUG|crash_dump|tombstone|LiteRT|litert|Whisper|Gemma|Kokoro|Misaki|G2P|Python|InProcess|Manifest|model|tokenizer|Node initialization|Failed|Fatal|FORTIFY|autoStart|Auto-start|Starting listening|Session manifest diagnostics" "$LOG_OUTPUT" \
        | tail -300 || true

    if grep -Fq "Session manifest diagnostics" "$LOG_OUTPUT"; then
        success "Session manifest diagnostics were captured"
    else
        warn "Session manifest diagnostics were not captured; nativeCreateSession was not reached"
    fi

    if grep -Eq "FATAL EXCEPTION|FORTIFY|crash_dump|AndroidRuntime" "$LOG_OUTPUT"; then
        warn "Crash-related log lines were captured in $LOG_OUTPUT"
    fi
    
    success "Test complete. Check logs above for pipeline execution results."
}

# =============================================================================
# STEP 8: Full Cycle Function
# =============================================================================
full_cycle() {
    log "=== Starting Full Build-Deploy-Test Cycle ==="
    
    verify_environment
    
    if [[ "$SKIP_BUILD" != "true" ]]; then
        build_rust
        build_apk
    else
        log "Skipping build (--skip-build)"
    fi
    
    if [[ "$SKIP_DEPLOY" != "true" ]]; then
        deploy_to_device
        run_and_test
    else
        log "Skipping deploy/test (--skip-deploy)"
    fi
    
    success "=== Cycle Complete ==="
}

# =============================================================================
# Main Entry Point
# =============================================================================
COMMAND="full"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --setup)
            COMMAND="setup"
            shift
            ;;
        --build)
            COMMAND="build"
            shift
            ;;
        --deploy)
            COMMAND="deploy"
            shift
            ;;
        --test)
            COMMAND="test"
            shift
            ;;
        --device)
            DEVICE_ADDRESS="$2"
            shift 2
            ;;
        --pipeline)
            TEST_PIPELINE="$2"
            shift 2
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --skip-deploy)
            SKIP_DEPLOY=true
            shift
            ;;
        *)
            DEVICE_ADDRESS="$1"
            shift
            ;;
    esac
done

case "$COMMAND" in
    setup)
        setup_cargo_config
        setup_python_symlink
        ;;
    build)
        verify_environment
        build_rust
        build_apk
        ;;
    deploy)
        deploy_to_device
        run_and_test
        ;;
    test)
        run_and_test
        ;;
    full)
        full_cycle
        ;;
esac
