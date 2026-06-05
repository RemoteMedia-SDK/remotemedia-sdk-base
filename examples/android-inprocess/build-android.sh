#!/bin/bash
# build-android.sh - Build script for RemoteMedia Android In-Process App
# This script builds the Rust cdylib and the Android APK

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
EXAMPLE_DIR="${SCRIPT_DIR}"
RUST_CRATE_DIR="${EXAMPLE_DIR}"  # The crate is at the example dir level

# Default values
TARGET_ARCH="aarch64-linux-android"
BUILD_TYPE="release"
RUN_BUNDLE_MODELS=false
SKIP_RUST_BUILD=false
SKIP_GRADLE_BUILD=false
UPLOAD_ARTIFACTS=false

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Build the RemoteMedia Android In-Process application.

OPTIONS:
    -a, --arch ARCH         Target architecture (aarch64-linux-android, x86_64-linux-android, both)
    -t, --type TYPE         Build type (debug, release) [default: release]
    -b, --bundle-models     Run model bundling script before building
    --skip-rust             Skip Rust cargo build
    --skip-gradle           Skip Gradle APK build
    --upload                Upload APK as artifact (for CI)
    -h, --help              Show this help message

EXAMPLES:
    $0                              # Build release for arm64
    $0 -a both                      # Build for both arm64 and x86_64
    $0 -t debug                     # Debug build
    $0 -b                           # Bundle models then build
    $0 --skip-rust                  # Only build APK (Rust already built)
    $0 -a x86_64-linux-android -t debug  # Debug build for emulator

ENVIRONMENT VARIABLES:
    ANDROID_SDK_ROOT       Android SDK path (or set in local.properties)
    ANDROID_NDK_ROOT       Android NDK path (or set in local.properties)
    RUSTUP_TOOLCHAIN       Rust toolchain to use (default: stable)
EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -a|--arch)
            TARGET_ARCH="$2"
            shift 2
            ;;
        -t|--type)
            BUILD_TYPE="$2"
            shift 2
            ;;
        -b|--bundle-models)
            RUN_BUNDLE_MODELS=true
            shift
            ;;
        --skip-rust)
            SKIP_RUST_BUILD=true
            shift
            ;;
        --skip-gradle)
            SKIP_GRADLE_BUILD=true
            shift
            ;;
        --upload)
            UPLOAD_ARTIFACTS=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Validate build type
if [[ "$BUILD_TYPE" != "debug" && "$BUILD_TYPE" != "release" ]]; then
    log_error "Invalid build type: $BUILD_TYPE (must be debug or release)"
    exit 1
fi

# Validate architecture
if [[ "$TARGET_ARCH" != "aarch64-linux-android" && "$TARGET_ARCH" != "x86_64-linux-android" && "$TARGET_ARCH" != "both" ]]; then
    log_error "Invalid architecture: $TARGET_ARCH"
    exit 1
fi

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if ! command -v cargo &> /dev/null; then
        log_error "cargo not found. Install Rust toolchain."
        exit 1
    fi
    
    if ! command -v rustup &> /dev/null; then
        log_warn "rustup not found. Using system rustc."
    else
        # Ensure target is installed
        if [[ "$TARGET_ARCH" == "both" ]]; then
            rustup target add aarch64-linux-android x86_64-linux-android || true
        else
            rustup target add "$TARGET_ARCH" || true
        fi
    fi
    
    # Check for Android SDK/NDK
    if [[ -z "${ANDROID_SDK_ROOT:-}" ]] && [[ -f "${EXAMPLE_DIR}/local.properties" ]]; then
        ANDROID_SDK_ROOT=$(grep '^sdk.dir=' "${EXAMPLE_DIR}/local.properties" | cut -d= -f2)
        export ANDROID_SDK_ROOT
    fi
    
    if [[ -z "${ANDROID_NDK_ROOT:-}" ]] && [[ -f "${EXAMPLE_DIR}/local.properties" ]]; then
        ANDROID_NDK_ROOT=$(grep '^ndk.dir=' "${EXAMPLE_DIR}/local.properties" | cut -d= -f2)
        export ANDROID_NDK_ROOT
    fi
    
    if [[ -z "${ANDROID_SDK_ROOT:-}" ]]; then
        log_warn "ANDROID_SDK_ROOT not set. Gradle may fail."
    fi
    
    log_success "Prerequisites check complete"
}

# Run model bundling script
run_model_bundling() {
    if [[ "$RUN_BUNDLE_MODELS" == true ]]; then
        log_info "Running model bundling script..."
        local bundle_script="${PROJECT_ROOT}/scripts/bundle_models.py"
        
        if [[ -f "$bundle_script" ]]; then
            python3 "$bundle_script" --output-dir "${EXAMPLE_DIR}/app/src/main/assets/models" \
                --manifest "${EXAMPLE_DIR}/app/src/main/assets/models/manifest.json"
            log_success "Model bundling complete"
        else
            log_warn "Model bundling script not found at $bundle_script. Skipping."
        fi
    fi
}

# Build Rust cdylib
build_rust() {
    if [[ "$SKIP_RUST_BUILD" == true ]]; then
        log_info "Skipping Rust build (--skip-rust)"
        return
    fi
    
    log_info "Building Rust cdylib for target(s): $TARGET_ARCH"
    
    cd "$RUST_CRATE_DIR"
    
    local cargo_args=("build")
    
    if [[ "$BUILD_TYPE" == "release" ]]; then
        cargo_args+=("--release")
    fi
    
    local workspace_root
    workspace_root=$(cd "${PROJECT_ROOT}/../.." && pwd)
    
    if [[ "$TARGET_ARCH" == "both" ]]; then
        log_info "Building for aarch64-linux-android..."
        CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
        CC_aarch64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
        CXX_aarch64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang++" \
        AR_aarch64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ar" \
        RANLIB_aarch64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ranlib" \
        LITERT_LM_LIB_DIR="${workspace_root}/litert-lm-loadable-plugin/lib/aarch64-linux-android" \
        cargo "${cargo_args[@]}" --target aarch64-linux-android
        
        log_info "Building for x86_64-linux-android..."
        CARGO_TARGET_X86_64_LINUX_ANDROID_LINKER="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/x86_64-linux-android24-clang" \
        CC_x86_64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/x86_64-linux-android24-clang" \
        CXX_x86_64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/x86_64-linux-android24-clang++" \
        AR_x86_64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ar" \
        RANLIB_x86_64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ranlib" \
        cargo "${cargo_args[@]}" --target x86_64-linux-android
    else
        if [[ "$TARGET_ARCH" == "aarch64-linux-android" ]]; then
            log_info "Building for aarch64-linux-android..."
            CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
            CC_aarch64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
            CXX_aarch64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang++" \
            AR_aarch64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ar" \
            RANLIB_aarch64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ranlib" \
            LITERT_LM_LIB_DIR="${workspace_root}/litert-lm-loadable-plugin/lib/aarch64-linux-android" \
            cargo "${cargo_args[@]}" --target "$TARGET_ARCH"
        elif [[ "$TARGET_ARCH" == "x86_64-linux-android" ]]; then
            log_info "Building for x86_64-linux-android..."
            CARGO_TARGET_X86_64_LINUX_ANDROID_LINKER="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/x86_64-linux-android24-clang" \
            CC_x86_64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/x86_64-linux-android24-clang" \
            CXX_x86_64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/x86_64-linux-android24-clang++" \
            AR_x86_64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ar" \
            RANLIB_x86_64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ranlib" \
            cargo "${cargo_args[@]}" --target "$TARGET_ARCH"
        else
            cargo "${cargo_args[@]}" --target "$TARGET_ARCH"
        fi
    fi
    
    # Verify the library was built
    local lib_name="libremotemedia_android_inprocess.so"
    if [[ "$TARGET_ARCH" == "both" ]]; then
        for arch in aarch64-linux-android x86_64-linux-android; do
            local lib_path="target/${arch}/$(echo $BUILD_TYPE | tr '[:upper:]' '[:lower:]')/${lib_name}"
            if [[ ! -f "$lib_path" ]]; then
                log_error "Rust library not found at $lib_path"
                exit 1
            fi
            log_success "Found $lib_path"
        done
    else
        local lib_path="target/${TARGET_ARCH}/$(echo $BUILD_TYPE | tr '[:upper:]' '[:lower:]')/${lib_name}"
        if [[ ! -f "$lib_path" ]]; then
            log_error "Rust library not found at $lib_path"
            exit 1
        fi
        log_success "Found $lib_path"
    fi
    
    # Now build and embed the loadable plugins (only for aarch64-linux-android as x86_64 has no prebuilt liblitert_lm.so)
    if [[ "$TARGET_ARCH" == "aarch64-linux-android" || "$TARGET_ARCH" == "both" ]]; then
        log_info "Building loadable plugins for aarch64-linux-android..."
        local workspace_root
        workspace_root=$(cd "${PROJECT_ROOT}/../.." && pwd)
        
        # Build whisper-loadable-plugin
        log_info "Building whisper-loadable-plugin..."
        cd "${workspace_root}/whisper"
        CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
        CC_aarch64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
        CXX_aarch64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang++" \
        AR_aarch64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ar" \
        RANLIB_aarch64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ranlib" \
        LITERT_LM_LIB_DIR="${workspace_root}/litert-lm-loadable-plugin/lib/aarch64-linux-android" \
        cargo "${cargo_args[@]}" --target aarch64-linux-android
        
        # Build litert-lm-loadable-plugin
        log_info "Building litert-lm-loadable-plugin..."
        cd "${workspace_root}/litert-lm-loadable-plugin"
        CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
        CC_aarch64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang" \
        CXX_aarch64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android24-clang++" \
        AR_aarch64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ar" \
        RANLIB_aarch64_linux_android="${ANDROID_NDK_ROOT}/toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-ranlib" \
        LITERT_LM_LIB_DIR="${workspace_root}/litert-lm-loadable-plugin/lib/aarch64-linux-android" \
        cargo "${cargo_args[@]}" --target aarch64-linux-android
        
        # Go back to example directory
        cd "$EXAMPLE_DIR"
        
        # Create directories in app if they don't exist
        mkdir -p app/src/main/assets/plugins
        mkdir -p app/src/main/jniLibs/arm64-v8a
        
        local build_mode_dir=$(echo "$BUILD_TYPE" | tr '[:upper:]' '[:lower:]')
        
        # Copy built loadable plugins
        cp "${workspace_root}/whisper/target/aarch64-linux-android/${build_mode_dir}/libwhisper_loadable_plugin.so" app/src/main/assets/plugins/
        cp "${workspace_root}/litert-lm-loadable-plugin/target/aarch64-linux-android/${build_mode_dir}/liblitert_lm_loadable_plugin.so" app/src/main/assets/plugins/
        log_success "Copied loadable plugins to assets/plugins/"
        
        # Copy dependency libraries
        cp "${workspace_root}/litert-lm-loadable-plugin/lib/aarch64-linux-android/liblitert_lm.so" app/src/main/jniLibs/arm64-v8a/
        cp "${workspace_root}/LiteRT-LM/prebuilt/android_arm64/libGemmaModelConstraintProvider.so" app/src/main/jniLibs/arm64-v8a/
        log_success "Copied prebuilt LiteRT dependency libraries to jniLibs/arm64-v8a/"
    else
        log_warn "Skipping loadable plugin embedding for non-arm64 architecture ($TARGET_ARCH)"
    fi
    
    log_success "Rust build complete"
}

# Build Android APK with Gradle
build_gradle() {
    if [[ "$SKIP_GRADLE_BUILD" == true ]]; then
        log_info "Skipping Gradle build (--skip-gradle)"
        return
    fi
    
    log_info "Building Android APK with Gradle..."
    
    cd "$EXAMPLE_DIR"
    
    # Ensure local.properties exists
    if [[ ! -f "local.properties" ]]; then
        log_info "Creating local.properties from template..."
        cp local.properties.template local.properties
        
        # Try to auto-detect SDK/NDK
        if [[ -n "${ANDROID_SDK_ROOT:-}" ]]; then
            echo "sdk.dir=${ANDROID_SDK_ROOT}" >> local.properties
        fi
        if [[ -n "${ANDROID_NDK_ROOT:-}" ]]; then
            echo "ndk.dir=${ANDROID_NDK_ROOT}" >> local.properties
        fi
    fi
    
    local gradle_args=("assemble${BUILD_TYPE^}")
    
    if command -v ./gradlew &> /dev/null; then
        ./gradlew "${gradle_args[@]}" --no-daemon
    else
        log_error "Gradle wrapper not found. Run 'gradle wrapper' first?"
        exit 1
    fi
    
    # Find the APK
    local apk_dir="app/build/outputs/apk/${BUILD_TYPE}"
    local apk_pattern="app-${BUILD_TYPE}.apk"
    local apk_path="${apk_dir}/${apk_pattern}"
    
    if [[ -f "$apk_path" ]]; then
        log_success "APK built at: $apk_path"
        
        # Copy to project root for easy access
        cp "$apk_path" "${EXAMPLE_DIR}/remotemedia-inprocess-${BUILD_TYPE}.apk"
        log_success "APK copied to: ${EXAMPLE_DIR}/remotemedia-inprocess-${BUILD_TYPE}.apk"
        
        if [[ "$UPLOAD_ARTIFACTS" == true ]]; then
            log_info "Uploading APK as artifact (CI environment)..."
            echo "APK_PATH=${EXAMPLE_DIR}/remotemedia-inprocess-${BUILD_TYPE}.apk" >> "${GITHUB_OUTPUT:-/dev/stdout}"
        fi
    else
        log_error "APK not found at expected location: $apk_path"
        exit 1
    fi
}

# Main execution
main() {
    log_info "Starting RemoteMedia Android In-Process build"
    log_info "Target: $TARGET_ARCH, Type: $BUILD_TYPE"
    
    check_prerequisites
    run_model_bundling
    build_rust
    build_gradle
    
    log_success "Build completed successfully!"
    
    if [[ "$BUILD_TYPE" == "release" ]]; then
        log_info "Release APK: ${EXAMPLE_DIR}/remotemedia-inprocess-release.apk"
    else
        log_info "Debug APK: ${EXAMPLE_DIR}/remotemedia-inprocess-debug.apk"
    fi
}

main "$@"