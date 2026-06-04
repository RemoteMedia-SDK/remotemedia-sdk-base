#!/bin/bash
# Development installation script for remotemedia-ffi.
#
# Builds the Rust extension via maturin and lays the artifacts out so
# `PYTHONPATH=$REPO/clients/python python …` Just Works without further
# environment plumbing. Stable compiler / linker pinning comes from
# `scripts/dev-env.sh` (sourced below).
#
# After this runs, `clients/python/remotemedia/` contains:
#   * `runtime.abi3.so` — symlink to the maturin build output, so future
#     `maturin develop` / `cargo build` invocations update it in place
#     without a re-install.
#   * `libonnxruntime_providers_*.so` — REAL FILES (not symlinks) copied
#     from `target/release/`. ORT's CUDA-EP loader resolves these via
#     `dirname(dlopen-path)`, which uses the path Python provided (not
#     realpath), so they must sit alongside the runtime .so in the
#     canonical Python package directory.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
PYTHON_CLIENT_DIR="$REPO_ROOT/clients/python/remotemedia"
FFI_SO_SRC="$SCRIPT_DIR/python/remotemedia/runtime.abi3.so"
RUNTIME_LINK="$PYTHON_CLIENT_DIR/runtime.abi3.so"
export CARGO_TARGET_DIR="$REPO_ROOT/target_local"
TARGET_RELEASE="$CARGO_TARGET_DIR/release"
DEV_ENV="$REPO_ROOT/scripts/dev-env.sh"

# 1. Toolchain env (CC / CXX / linker pinning). Sourced — not exec'd.
if [[ -f "$DEV_ENV" ]]; then
    echo "↪ sourcing $DEV_ENV"
    # shellcheck disable=SC1090
    source "$DEV_ENV"
fi

# 2. Build the extension.
echo "🔨 Building remotemedia-ffi with maturin..."
cd "$SCRIPT_DIR"
maturin develop --release --features "extension-module,python-telephony,python-webrtc"
cd "$REPO_ROOT"

if [[ ! -f "$FFI_SO_SRC" ]]; then
    echo "❌ Error: Build failed — $FFI_SO_SRC not found"
    exit 1
fi
echo "✓ Build successful: $FFI_SO_SRC"

# 3. runtime.abi3.so — keep as a symlink so subsequent rebuilds auto-update.
mkdir -p "$PYTHON_CLIENT_DIR"
if [[ -L "$RUNTIME_LINK" ]]; then
    :  # already a symlink, fine
elif [[ -e "$RUNTIME_LINK" ]]; then
    echo "⚠️  Regular file at $RUNTIME_LINK — replacing with symlink"
    rm -f "$RUNTIME_LINK"
fi
ln -sf "$FFI_SO_SRC" "$RUNTIME_LINK"
echo "✓ Symlinked runtime: $RUNTIME_LINK -> $FFI_SO_SRC"

# 4. ORT shared / provider libs — COPY (not symlink) so dlopen's dirname
#    lookup resolves them in clients/python/remotemedia/ regardless of
#    how the .so was reached. Patterns:
#      libonnxruntime_providers_shared.so   (always needed by ort@2.0-rc)
#      libonnxruntime_providers_cuda.so     (CUDA EP — pulled when manifests
#                                             use device: "cuda:N")
#      libonnxruntime_providers_tensorrt.so (TRT EP)
#      libonnxruntime_providers_nv_*.so     (TRT-RTX variant)
#    Globbing keeps this honest — anything ORT shipped this build cycle
#    lands next to the runtime .so.
shopt -s nullglob
copied_any=0
for so in "$TARGET_RELEASE"/libonnxruntime_providers_*.so; do
    dst="$PYTHON_CLIENT_DIR/$(basename "$so")"
    # Replace stale symlinks from older versions of this script.
    if [[ -L "$dst" ]]; then
        rm -f "$dst"
    fi
    cp -f -- "$so" "$dst"
    echo "✓ Copied $(basename "$so")"
    copied_any=1
done
shopt -u nullglob

if [[ $copied_any -eq 0 ]]; then
    echo "ℹ No libonnxruntime_providers_*.so found in $TARGET_RELEASE — skipped (ORT may not be enabled in this build)."
fi

echo ""
echo "✅ Development setup complete!"
echo ""
echo "Test (no CUDA needed):"
echo "  PYTHONPATH=$REPO_ROOT/clients/python python -c \\"
echo "      'from remotemedia.runtime import get_runtime_version; print(get_runtime_version())'"
echo ""
echo "Run a CUDA-using example (auto-probes your active env for cuDNN/CUDA):"
echo "  PYTHONPATH=$REPO_ROOT/clients/python $REPO_ROOT/scripts/with-cuda python <your_script.py>"
echo ""
echo "Or source the env into your current shell:"
echo "  source $REPO_ROOT/scripts/cuda-env.sh"
