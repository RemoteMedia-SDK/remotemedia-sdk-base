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
# Usage: ./android_build_deploy_test.sh [--device IP:PORT] [--pipeline NAME] [--skip-build] [--skip-deploy]
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
TEST_PIPELINE="${TEST_PIPELINE:-hermes-agent-test.json}"
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
find_python_for_android_root() {
    if [[ -n "${PYTHON_FOR_ANDROID_ROOT:-}" && -d "$PYTHON_FOR_ANDROID_ROOT" ]]; then
        echo "$PYTHON_FOR_ANDROID_ROOT"
        return 0
    fi

    local roots=()
    if [[ -d "${HOME}/snap/code/current/.local/share/python-for-android/dists" ]]; then
        roots+=("${HOME}/snap/code/current/.local/share/python-for-android/dists")
    fi
    while IFS= read -r candidate; do
        roots+=("$candidate")
    done < <(find "${HOME}/snap/code" -maxdepth 6 -type d -path '*/.local/share/python-for-android/dists' 2>/dev/null)
    if [[ -d "${HOME}/.local/share/python-for-android/dists" ]]; then
        roots+=("${HOME}/.local/share/python-for-android/dists")
    fi

    for root in "${roots[@]}"; do
        if [[ -d "$root/remotemedia_hermes" ]]; then
            echo "$root"
            return 0
        fi
    done

    for root in "${roots[@]}"; do
        if [[ -d "$root/remotemedia_numpy" ]]; then
            echo "$root"
            return 0
        fi
    done

    if [[ ${#roots[@]} -gt 0 ]]; then
        echo "${roots[0]}"
        return 0
    fi

    echo "${HOME}/.local/share/python-for-android/dists"
}
export PYTHON_FOR_ANDROID_ROOT="$(find_python_for_android_root)"
export PYTHON_BUNDLE_SRC="${PYTHON_BUNDLE_SRC:-${PYTHON_FOR_ANDROID_ROOT}/remotemedia_hermes/_python_bundle__arm64-v8a/_python_bundle}"
export PYTHON_BUNDLE_FALLBACK_SRC="${PYTHON_BUNDLE_FALLBACK_SRC:-${PYTHON_FOR_ANDROID_ROOT}/remotemedia_numpy/_python_bundle__arm64-v8a/_python_bundle}"
export PYTHON_NATIVE_LIBS_SRC="${PYTHON_NATIVE_LIBS_SRC:-${PYTHON_FOR_ANDROID_ROOT}/remotemedia_hermes/libs/arm64-v8a}"
export PYTHON_NATIVE_LIBS_FALLBACK_SRC="${PYTHON_NATIVE_LIBS_FALLBACK_SRC:-${PYTHON_FOR_ANDROID_ROOT}/remotemedia_hermes/_python_bundle__arm64-v8a/libs/arm64-v8a}"
PYTHON_SRC="${PYTHON_SRC:-${WORKSPACE_ROOT}/remotemedia-sdk/clients/python}"
PYTHON_SRC_STAGING_LOCAL="${PYTHON_SRC_STAGING_LOCAL:-/tmp/remotemedia-inprocess-python-src}"
PYTHON_STAGING_PATH="${PYTHON_STAGING_PATH:-/data/local/tmp/remotemedia-inprocess-python}"

# APK/AAR asset packaging configuration. The deploy phase must not push these
# resources over adb; it should install the APK and let the app extract assets
# into app-private storage on first launch.
APP_ASSETS_DIR="${APP_ASSETS_DIR:-${ANDROID_PROJECT}/app/src/main/assets}"
PYTHON_RUNTIME_ID="${PYTHON_RUNTIME_ID:-hermes}"
PYTHON_RUNTIME_ASSETS_DIR="${PYTHON_RUNTIME_ASSETS_DIR:-${APP_ASSETS_DIR}/python-runtimes/${PYTHON_RUNTIME_ID}}"
BUNDLE_SMALL_MODELS_IN_APK="${BUNDLE_SMALL_MODELS_IN_APK:-true}"
BUNDLE_LARGE_LLM_IN_APK="${BUNDLE_LARGE_LLM_IN_APK:-false}"

NDK_VERSION="${NDK_VERSION:-25.2.9519653}"
SDK_PATH="${ANDROID_SDK_ROOT:-${ANDROID_HOME:-${HOME}/Android/Sdk}}"
NDK_PATH="${ANDROID_NDK_ROOT:-${SDK_PATH}/ndk/${NDK_VERSION}}"
CARGO_CONFIG="${ANDROID_PROJECT}/.cargo/config.toml"

export ANDROID_HOME="$SDK_PATH"
export ANDROID_SDK_ROOT="$SDK_PATH"
export ANDROID_NDK_ROOT="$NDK_PATH"
export PATH="${SDK_PATH}/platform-tools:${PATH}"

NDK_TOOLCHAIN_BIN="${NDK_PATH}/toolchains/llvm/prebuilt/linux-x86_64/bin"
export CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="${NDK_TOOLCHAIN_BIN}/aarch64-linux-android24-clang"
export CC_aarch64_linux_android="${NDK_TOOLCHAIN_BIN}/aarch64-linux-android24-clang"
export CXX_aarch64_linux_android="${NDK_TOOLCHAIN_BIN}/aarch64-linux-android24-clang++"
export AR_aarch64_linux_android="${NDK_TOOLCHAIN_BIN}/llvm-ar"
export RANLIB_aarch64_linux_android="${NDK_TOOLCHAIN_BIN}/llvm-ranlib"

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
    local python_native_libs_src
    python_native_libs_src="$(resolve_python_native_libs_src)"

    if [[ -f "$python_native_libs_src/libpython3.14.so" ]]; then
        if [[ ! -f "$python_native_libs_src/libpython3.10.so" ]]; then
            ln -sf libpython3.14.so "$python_native_libs_src/libpython3.10.so"
            success "Created symlink: libpython3.10.so -> libpython3.14.so"
        fi
        if [[ ! -f "$python_native_libs_src/libpython3.11.so" ]]; then
            ln -sf libpython3.14.so "$python_native_libs_src/libpython3.11.so"
            success "Created symlink: libpython3.11.so -> libpython3.14.so"
        fi
    else
        warn "Python symlink not configured because libpython3.14.so was not found in $python_native_libs_src"
    fi
}


# =============================================================================
# STEP 3b: Package runtime assets into APK
# =============================================================================
find_all_python_bundles() {
    local root="$PYTHON_FOR_ANDROID_ROOT"
    local arch="arm64-v8a"
    if [[ ! -d "$root" ]]; then
        return 1
    fi

    find "$root" -type d -path "*/_python_bundle__${arch}/_python_bundle" 2>/dev/null
}

find_latest_python_bundle() {
    find_all_python_bundles | while IFS= read -r bundle; do
        if [[ -d "$bundle" ]]; then
            printf '%s %s\n' "$(stat -c '%Y' "$bundle")" "$bundle"
        fi
    done | sort -n | tail -1 | cut -d' ' -f2- || true
}

find_latest_python_native_libs() {
    local root="$PYTHON_FOR_ANDROID_ROOT"
    local arch="arm64-v8a"
    if [[ ! -d "$root" ]]; then
        return 1
    fi

    find "$root" -type d -path "*/libs/${arch}" 2>/dev/null | while IFS= read -r libs; do
        if [[ -d "$libs" ]]; then
            printf '%s %s\n' "$(stat -c '%Y' "$libs")" "$libs"
        fi
    done | sort -n | tail -1 | cut -d' ' -f2- || true
}

bundle_has_httpx() {
    local bundle="$1"
    [[ -f "$bundle/site-packages/httpx/_transports/__init__.py" || -f "$bundle/site-packages/httpx/_transports/__init__.pyc" ]]
}

bundle_has_websockets() {
    local bundle="$1"
    [[ -f "$bundle/site-packages/websockets/__init__.py" || -f "$bundle/site-packages/websockets/__init__.pyc" ]]
}

bundle_has_required_python_modules() {
    local bundle="$1"
    bundle_has_httpx "$bundle" && bundle_has_websockets "$bundle"
}

find_python_bundle_with_required_modules() {
    find_all_python_bundles | while IFS= read -r bundle; do
        if bundle_has_required_python_modules "$bundle"; then
            echo "$bundle"
            return 0
        fi
    done
    return 1
}

resolve_python_bundle_src() {
    local python_bundle_src="$PYTHON_BUNDLE_SRC"
    if [[ ! -d "$python_bundle_src" ]]; then
        warn "Preferred Python-for-Android bundle not found: $python_bundle_src" >&2
        if latest_bundle=$(find_latest_python_bundle); then
            warn "Using newest available Python-for-Android bundle: $latest_bundle" >&2
            python_bundle_src="$latest_bundle"
        else
            warn "Attempting fallback bundle: $PYTHON_BUNDLE_FALLBACK_SRC" >&2
            python_bundle_src="$PYTHON_BUNDLE_FALLBACK_SRC"
        fi
    fi

    if [[ -d "$python_bundle_src" ]] && ! bundle_has_required_python_modules "$python_bundle_src"; then
        warn "Selected bundle does not contain required Hermes Python modules (httpx._transports and websockets): $python_bundle_src" >&2
        if modules_bundle=$(find_python_bundle_with_required_modules); then
            warn "Switching to bundle with required Hermes Python modules: $modules_bundle" >&2
            python_bundle_src="$modules_bundle"
        else
            error "No available python-for-android bundle contains required Hermes Python modules (httpx._transports and websockets)." >&2
            warn "Rebuild with build_p4a_hermes_simple.sh after updating requirements-hermes.txt." >&2
            exit 1
        fi
    fi

    if [[ -d "$python_bundle_src" ]] && ! bundle_has_required_python_modules "$python_bundle_src"; then
        error "Resolved bundle still missing required Hermes Python modules: $python_bundle_src" >&2
        warn "Rebuild with build_p4a_hermes_simple.sh and verify site-packages contains httpx/_transports and websockets." >&2
        exit 1
    fi

    if [[ ! -d "$python_bundle_src" ]]; then
        error "Python-for-Android bundle not found: $python_bundle_src" >&2
        warn "Build it first with build_p4a_hermes_simple.sh or override PYTHON_BUNDLE_SRC." >&2
        exit 1
    fi

    echo "$python_bundle_src"
}

resolve_python_native_libs_src() {
    local python_native_libs_src="$PYTHON_NATIVE_LIBS_SRC"
    if [[ ! -d "$python_native_libs_src" ]]; then
        warn "Preferred Python native libs not found: $python_native_libs_src" >&2
        if latest_libs=$(find_latest_python_native_libs); then
            warn "Using newest available Python native libs: $latest_libs" >&2
            python_native_libs_src="$latest_libs"
        else
            warn "Attempting fallback native libs: $PYTHON_NATIVE_LIBS_FALLBACK_SRC" >&2
            python_native_libs_src="$PYTHON_NATIVE_LIBS_FALLBACK_SRC"
        fi
    fi

    if [[ ! -d "$python_native_libs_src" ]]; then
        error "Python native libs not found: $python_native_libs_src" >&2
        warn "Build the p4a distro first or override PYTHON_NATIVE_LIBS_SRC." >&2
        exit 1
    fi

    echo "$python_native_libs_src"
}

ensure_python_shared_lib_symlinks() {
    local python_native_libs_src
    python_native_libs_src="$(resolve_python_native_libs_src)"

    if [[ -f "$python_native_libs_src/libpython3.14.so" ]]; then
        if [[ ! -f "$python_native_libs_src/libpython3.10.so" ]]; then
            ln -sf libpython3.14.so "$python_native_libs_src/libpython3.10.so"
            success "Created symlink: libpython3.10.so -> libpython3.14.so"
        fi
        if [[ ! -f "$python_native_libs_src/libpython3.11.so" ]]; then
            ln -sf libpython3.14.so "$python_native_libs_src/libpython3.11.so"
            success "Created symlink: libpython3.11.so -> libpython3.14.so"
        fi
    fi
}

copy_required_asset_file() {
    local src="$1"
    local dst="$2"
    local label="$3"

    if [[ ! -f "$src" ]]; then
        error "$label not found: $src"
        exit 1
    fi

    mkdir -p "$(dirname "$dst")"
    install -m 0644 "$src" "$dst"
    success "Packaged $label: ${dst#${ANDROID_PROJECT}/}"
}

copy_optional_asset_file() {
    local src="$1"
    local dst="$2"
    local label="$3"

    if [[ ! -f "$src" ]]; then
        warn "Optional $label not found: $src"
        return
    fi

    mkdir -p "$(dirname "$dst")"
    install -m 0644 "$src" "$dst"
    success "Packaged optional $label: ${dst#${ANDROID_PROJECT}/}"
}

copy_required_asset_dir() {
    local src="$1"
    local dst="$2"
    local label="$3"

    if [[ ! -d "$src" ]]; then
        error "$label directory not found: $src"
        exit 1
    fi

    rm -rf "$dst"
    mkdir -p "$(dirname "$dst")"
    cp -aL "$src" "$dst"
    success "Packaged $label: ${dst#${ANDROID_PROJECT}/}"
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

stage_python_sources_for_apk() {
    local dst="$1"

    if [[ ! -d "$PYTHON_SRC" ]]; then
        error "RemoteMedia Python sources not found: $PYTHON_SRC"
        exit 1
    fi

    rm -rf "$PYTHON_SRC_STAGING_LOCAL"
    mkdir -p "$PYTHON_SRC_STAGING_LOCAL"
    cp -aL "$PYTHON_SRC"/. "$PYTHON_SRC_STAGING_LOCAL"/

    local hermes_src="${WORKSPACE_ROOT}/hermes-agent"
    if [[ -d "$hermes_src" ]]; then
        cp -aL "$hermes_src" "$PYTHON_SRC_STAGING_LOCAL/hermes_agent"
        cp "$hermes_src"/*.py "$PYTHON_SRC_STAGING_LOCAL/" 2>/dev/null || true
        cp -aL "$hermes_src/agent" "$PYTHON_SRC_STAGING_LOCAL/" 2>/dev/null || true
        cp -aL "$hermes_src/tools" "$PYTHON_SRC_STAGING_LOCAL/" 2>/dev/null || true
        success "Staged Hermes Agent source for APK packaging"
    else
        warn "Hermes Agent source not found at $hermes_src - Hermes imports may fail"
    fi

    mkdir -p "$PYTHON_SRC_STAGING_LOCAL/remotemedia/nodes"
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

These adapters intentionally avoid desktop-only node imports. Native Android
loadable plugins should be preferred for Whisper, VAD, Kokoro, Misaki G2P, and
LiteRT-LM when available.
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
    limit = min(len(samples), 48000)
    for sample in samples[:limit]:
        value = float(sample)
        total += value * value
        peak = max(peak, abs(value))
    rms = math.sqrt(total / limit)
    return len(samples), sample_rate, channels, max(rms, peak)


class VADNode:
    def initialize(self, config):
        self.config = dict(config or {})
        self.energy_threshold = float(self.config.get("energy_threshold", 0.02))
        self.frames_seen = 0
        print(f"AndroidInProcess VADNode initialized threshold={self.energy_threshold}", flush=True)

    def process(self, data):
        self.frames_seen += 1
        sample_count, sample_rate, channels, energy = _audio_stats(data)
        if energy < self.energy_threshold:
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
        print("AndroidInProcess WhisperSTTNode compatibility adapter initialized", flush=True)

    def process(self, data):
        return ""

    def process_streaming(self, data):
        output = self.process(data)
        if output:
            yield output


class DebugKokoroTTSNode:
    def initialize(self, config):
        self.config = dict(config or {})
        self.sample_rate = int(self.config.get("sample_rate", 24000))
        print("AndroidInProcess DebugKokoroTTSNode initialized", flush=True)

    def process(self, data):
        text = data if isinstance(data, str) else str(data)
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
            metadata={"android_inprocess_tts": {"debug": True, "input_preview": text[:160]}},
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
        except ImportError as e:
            self.import_error = str(e)

    def process(self, data):
        if self.imports_ok:
            return {"data_type": "text", "text": "Hermes Agent imports: OK", "modules": self.imported_modules}
        return {"data_type": "text", "text": f"Hermes Agent imports: FAILED - {self.import_error}", "error": self.import_error}

    def process_streaming(self, data):
        yield self.process(data)

    def finalize(self):
        return True
PY

    rm -rf "$dst"
    mkdir -p "$(dirname "$dst")"
    cp -aL "$PYTHON_SRC_STAGING_LOCAL" "$dst"
    success "Packaged Python sources: ${dst#${ANDROID_PROJECT}/}"
}

package_python_runtime_assets() {
    log "Packaging Python runtime into APK assets..."

    local python_bundle_src
    python_bundle_src="$(resolve_python_bundle_src)"
    local python_native_libs_src
    python_native_libs_src="$(resolve_python_native_libs_src)"

    rm -rf "$PYTHON_RUNTIME_ASSETS_DIR"
    mkdir -p "$PYTHON_RUNTIME_ASSETS_DIR"

    copy_required_asset_dir "$python_bundle_src" "$PYTHON_RUNTIME_ASSETS_DIR/bundle" "Python-for-Android bundle"

    # p4a native libraries are needed both as Android native libs and inside the
    # extracted runtime bundle for direct libpythonbin.so execution/wrappers.
    mkdir -p "$PYTHON_RUNTIME_ASSETS_DIR/bundle"
    install -m 0644 "$python_native_libs_src"/*.so "$PYTHON_RUNTIME_ASSETS_DIR/bundle/"
    success "Packaged Python native libraries into runtime bundle assets"

    stage_python_sources_for_apk "$PYTHON_RUNTIME_ASSETS_DIR/src"

    cat > "$PYTHON_RUNTIME_ASSETS_DIR/runtime.json" <<JSON
{
  "id": "${PYTHON_RUNTIME_ID}",
  "python": "3.11",
  "provider": "python-for-android",
  "abi": "arm64-v8a",
  "entrypoint": "bundle/python3",
  "python_home": "bundle",
  "python_path": [
    "bundle/stdlib.zip",
    "bundle/modules",
    "bundle/site-packages",
    "src"
  ],
  "native_library_dirs": [
    "bundle"
  ],
  "packages": {
    "remotemedia": "local",
    "hermes-agent": "local"
  }
}
JSON

    success "Packaged Python runtime manifest: ${PYTHON_RUNTIME_ASSETS_DIR#${ANDROID_PROJECT}/}/runtime.json"
}

package_small_model_assets() {
    if [[ "$BUNDLE_SMALL_MODELS_IN_APK" != "true" ]]; then
        warn "Skipping small model/resource APK packaging because BUNDLE_SMALL_MODELS_IN_APK=$BUNDLE_SMALL_MODELS_IN_APK"
        return
    fi

    log "Packaging small model/resource assets into APK..."

    copy_required_asset_file "$SILERO_VAD_MODEL_SRC" "$APP_ASSETS_DIR/models/silero-vad/silero_vad.onnx" "Silero VAD model"

    copy_required_asset_file "$WHISPER_MODEL_SRC" "$APP_ASSETS_DIR/models/whisper/whisper_tiny_30s_f32.tflite" "Whisper tiny model"
    copy_optional_asset_file "$WHISPER_BASE_MODEL_SRC" "$APP_ASSETS_DIR/models/whisper/whisper_base_30s_f32.tflite" "Whisper base model"
    copy_required_asset_file "$WHISPER_TOKENIZER_SRC" "$APP_ASSETS_DIR/models/whisper/tokenizer.json" "Whisper tokenizer"
    copy_optional_asset_file "$WHISPER_CONFIG_SRC" "$APP_ASSETS_DIR/models/whisper/config.json" "Whisper config"

    copy_required_asset_file "$KOKORO_MODEL_SRC" "$APP_ASSETS_DIR/models/kokoro/onnx/$KOKORO_MODEL_NAME" "Kokoro ONNX model"
    copy_required_asset_file "$KOKORO_TOKENIZER_SRC" "$APP_ASSETS_DIR/models/kokoro/tokenizer.json" "Kokoro tokenizer"
    copy_required_asset_file "$KOKORO_VOICE_SRC" "$APP_ASSETS_DIR/models/kokoro/voices/af_bella.bin" "Kokoro voice"

    local misaki_src
    misaki_src="$(resolve_misaki_resource_src)"
    copy_required_asset_file "$misaki_src/en-US/gold.json" "$APP_ASSETS_DIR/models/misaki-g2p/en-US/gold.json" "Misaki G2P en-US gold"
    copy_required_asset_file "$misaki_src/en-US/silver.json" "$APP_ASSETS_DIR/models/misaki-g2p/en-US/silver.json" "Misaki G2P en-US silver"
    copy_optional_asset_file "$misaki_src/en-GB/gold.json" "$APP_ASSETS_DIR/models/misaki-g2p/en-GB/gold.json" "Misaki G2P en-GB gold"
    copy_optional_asset_file "$misaki_src/en-GB/silver.json" "$APP_ASSETS_DIR/models/misaki-g2p/en-GB/silver.json" "Misaki G2P en-GB silver"
}

package_large_model_assets_if_requested() {
    if [[ "$BUNDLE_LARGE_LLM_IN_APK" != "true" ]]; then
        warn "Not packaging large LLM model in APK. The app/runtime manager should download it separately."
        return
    fi

    copy_required_asset_file "$MODEL_SRC" "$APP_ASSETS_DIR/models/gemma-4-E2B-it.litertlm" "LiteRT-LM model"
}

package_apk_runtime_assets() {
    log "Packaging runtime assets into APK assets directory..."
    mkdir -p "$APP_ASSETS_DIR"

    package_python_runtime_assets
    package_small_model_assets
    package_large_model_assets_if_requested

    success "APK runtime asset packaging complete"
}

# =============================================================================
# STEP 4: Build Rust cdylib
# =============================================================================
build_rust() {
    log "Building Rust cdylib for arm64-v8a..."
    cd "$ANDROID_PROJECT"

    ensure_python_shared_lib_symlinks

    local python_native_libs_src
    python_native_libs_src="$(resolve_python_native_libs_src)"
    export RUSTFLAGS="${RUSTFLAGS:-} -C link-arg=-L${python_native_libs_src} -C link-arg=-lpython3.14"

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
    cargo build --target aarch64-linux-android 2>&1 | tail -40
    cd "$ANDROID_PROJECT"

    local python_native_libs_src
    python_native_libs_src="$(resolve_python_native_libs_src)"
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

    package_apk_runtime_assets
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
        "assets/python-runtimes/${PYTHON_RUNTIME_ID}/runtime.json"
        "assets/python-runtimes/${PYTHON_RUNTIME_ID}/bundle/stdlib.zip"
        "assets/python-runtimes/${PYTHON_RUNTIME_ID}/src/remotemedia/__init__.py"
        "assets/python-runtimes/${PYTHON_RUNTIME_ID}/src/remotemedia/nodes/android_inprocess.py"
        "assets/models/silero-vad/silero_vad.onnx"
        "assets/models/whisper/whisper_tiny_30s_f32.tflite"
        "assets/models/whisper/tokenizer.json"
        "assets/models/kokoro/onnx/${KOKORO_MODEL_NAME}"
        "assets/models/kokoro/tokenizer.json"
        "assets/models/kokoro/voices/af_bella.bin"
        "assets/models/misaki-g2p/en-US/gold.json"
        "assets/models/misaki-g2p/en-US/silver.json"
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

    local optional_entries=(
        "assets/models/whisper/whisper_base_30s_f32.tflite"
        "assets/models/whisper/config.json"
        "assets/models/misaki-g2p/en-GB/gold.json"
        "assets/models/misaki-g2p/en-GB/silver.json"
    )

    if [[ "$BUNDLE_LARGE_LLM_IN_APK" == "true" ]]; then
        optional_entries+=("assets/models/gemma-4-E2B-it.litertlm")
    fi

    for entry in "${optional_entries[@]}"; do
        if grep -Fxq "$entry" <<< "$apk_entries"; then
            success "APK contains optional $entry"
        else
            warn "APK does not contain optional $entry"
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
    
    # Install APK. Do not adb-push runtime assets, Python, small models, or
    # plugins here: they must already be embedded in the APK and extracted by
    # the app/runtime manager.
    adb -s "$DEVICE_ADDRESS" install -r "$ANDROID_PROJECT/app/build/outputs/apk/release/app-release.apk"
    success "APK installed"
    success "Runtime assets are embedded in the APK; the app is responsible for first-run extraction."
}

# Legacy adb-push staging functions were removed intentionally. Runtime assets,
# Python distributions, plugins, and small models are now packaged into the APK.
# Large LLM/model artifacts should be downloaded by the app/runtime manager, not
# pushed by the deploy script.

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

    log "Runtime assets are expected to be extracted from APK assets by the app on first launch."
    log "No Python/model/resource files are adb-pushed by this script."

    # For the profile import test: push a minimal test archive to the app's
    # internal app files dir (accessible to the app without external storage issues)
    if [[ "$TEST_PIPELINE" == "hermes-profile-import-test.json" ]]; then
        local test_archive="${HERMES_TEST_PROFILE_ARCHIVE:-${SCRIPT_DIR}/../../../default.tar.gz}"
        local device_tmp="/data/local/tmp/hermes_test_profile.tar.gz"
        local device_internal="/data/data/com.remotemedia.inprocess/files/hermes_test_profile.tar.gz"
        if [[ -f "$test_archive" ]]; then
            log "Pushing hermes test profile archive to device internal files dir ($device_internal)..."
            adb -s "$DEVICE_ADDRESS" push "$test_archive" "$device_tmp"
            adb -s "$DEVICE_ADDRESS" shell "run-as com.remotemedia.inprocess sh -c 'cat /data/local/tmp/hermes_test_profile.tar.gz > files/hermes_test_profile.tar.gz && chmod 600 files/hermes_test_profile.tar.gz'"
            adb -s "$DEVICE_ADDRESS" shell rm -f "$device_tmp" 2>/dev/null || true
            success "Archive pushed ($(du -sh "$test_archive" | cut -f1))"
        else
            warn "HERMES_TEST_PROFILE_ARCHIVE not set or file not found: $test_archive"
            warn "Archive must already be present at $device_internal on device"
        fi
    fi
    
    # Start MainActivity and ask it to start streaming as soon as the manifest is ready.
    adb -s "$DEVICE_ADDRESS" shell am start -n com.remotemedia.inprocess/.MainActivity --ez auto_start true --ez simulate_speech true --es pipeline "$TEST_PIPELINE"
    
    local wait_seconds="${TEST_WAIT_SECONDS:-45}"
    if [[ -z "${TEST_WAIT_SECONDS:-}" && "$TEST_PIPELINE" == "tts-mobile.json" ]]; then
        wait_seconds=130
    fi
    
    # Start streaming logcat in background so logs appear in real-time
    log "Streaming logcat to console (also saving to $LOG_OUTPUT)..."
    adb -s "$DEVICE_ADDRESS" logcat -v threadtime 2>&1 | tee "$LOG_OUTPUT" &
    LOGCAT_PID=$!
    
    # Ensure logcat process is killed on exit
    trap 'kill $LOGCAT_PID 2>/dev/null; wait $LOGCAT_PID 2>/dev/null' EXIT
    
    log "Waiting for auto-started pipeline execution (${wait_seconds}s)..."
    sleep "$wait_seconds"
    
    # Stop streaming logcat
    kill $LOGCAT_PID 2>/dev/null
    wait $LOGCAT_PID 2>/dev/null
    trap - EXIT
    
    # Check extracted runtime assets after launch
    log "Checking extracted runtime assets after launch, if the app exposes them via run-as..."
    adb -s "$DEVICE_ADDRESS" shell "run-as com.remotemedia.inprocess sh -c 'ls -lh files/remotemedia/python-runtimes/${PYTHON_RUNTIME_ID}/runtime.json files/remotemedia/python-runtimes/${PYTHON_RUNTIME_ID}/bundle/stdlib.zip 2>/dev/null || true; ls -lh files/models/whisper files/models/kokoro files/models/misaki-g2p 2>/dev/null || true'" || true

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

    if grep -Fq "Loaded manifest: $TEST_PIPELINE" "$LOG_OUTPUT"; then
        success "PipelineManager loaded requested manifest: $TEST_PIPELINE"
    else
        error "PipelineManager did not load requested manifest: $TEST_PIPELINE"
        exit 1
    fi

    if [[ "$TEST_PIPELINE" == "hermes-agent-test.json" ]]; then
        if grep -Fq "Hermes Agent imports: OK" "$LOG_OUTPUT"; then
            success "HermesAgentTestPlugin validation passed (imports OK)"
        elif grep -Fq "Hermes Agent imports: FAILED" "$LOG_OUTPUT"; then
            error "HermesAgentTestPlugin validation failed"
            exit 1
        else
            error "HermesAgentTestPlugin output marker not found in logs"
            exit 1
        fi
    fi

    if [[ "$TEST_PIPELINE" == "hermes-profile-import-test.json" ]]; then
        if grep -Fq "Hermes profile import: OK" "$LOG_OUTPUT"; then
            success "HermesProfileImportPlugin validation passed (profile import OK)"
        elif grep -Fq "Hermes profile import: FAILED" "$LOG_OUTPUT"; then
            error "HermesProfileImportPlugin validation failed - check logs for error details"
            grep -F "Hermes profile import: FAILED" "$LOG_OUTPUT" | tail -3 || true
            exit 1
        else
            error "HermesProfileImportPlugin output marker not found in logs"
            exit 1
        fi
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
