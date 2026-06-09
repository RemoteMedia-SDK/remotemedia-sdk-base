#!/bin/bash
# Prepare Python wheels as APK assets for bundling
prepare_python_wheels_assets() {
    log "Preparing Python wheels as APK assets..."

    local REQUIREMENTS_FILE="${SCRIPT_DIR}/requirements-hermes.txt"
    local WHEEL_ASSETS_DIR="app/src/main/assets/python-wheels"
    local WHEEL_DIR="/tmp/remotemedia-wheels-arm64"

    # Clean and create assets directory
    rm -rf "$WHEEL_ASSETS_DIR"
    mkdir -p "$WHEEL_ASSETS_DIR"

    # Download wheels for arm64 if requirements file exists
    if [[ -f "$REQUIREMENTS_FILE" ]]; then
        rm -rf "$WHEEL_DIR"
        mkdir -p "$WHEEL_DIR"

        while IFS= read -r line; do
            # Skip comments and empty lines
            [[ -z "$line" || "$line" =~ ^# ]] && continue
            [[ "$line" =~ ^- ]] && continue
            pkg=$(echo "$line" | sed 's/#.*$//' | xargs)
            [[ -z "$pkg" ]] && continue

            # Extract package name and version
            if [[ "$pkg" =~ ^([^=<>!~]+)(.*)$ ]]; then
                pkg_name="${BASH_REMATCH[1]}"
                pkg_spec="${BASH_REMATCH[2]}"
                pkg_spec_clean=$(echo "$pkg_spec" | sed 's/^[=<>!~]*//')

                log "Downloading $pkg_name$pkg_spec for manylinux2014_aarch64..."
                pip download "$pkg_name==$pkg_spec_clean" \
                    --only-binary=:all: \
                    --platform manylinux2014_aarch64 \
                    --implementation cp \
                    --abi cp311 \
                    --python-version 311 \
                    -d "$WHEEL_DIR" 2>&1 | tail -3 || true
            fi
        done < "$REQUIREMENTS_FILE"
    fi

    # Copy wheels to assets directory
    if [[ -d "$WHEEL_DIR" ]]; then
        for wheel in "$WHEEL_DIR"/*.whl; do
            [[ -f "$wheel" ]] || continue
            cp "$wheel" "$WHEEL_ASSETS_DIR/"
        done
        local wheel_count=$(ls -1 "$WHEEL_ASSETS_DIR"/*.whl 2>/dev/null | wc -l)
        success "Prepared $wheel_count Python wheels in assets/python-wheels/"
    else
        warn "No wheels directory found, skipping wheel assets"
    fi
}