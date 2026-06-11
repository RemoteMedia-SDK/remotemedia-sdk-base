#!/bin/bash
# Read Python version configuration from central config file
# Usage: source scripts/read-python-config.sh
# Provides: PYTHON_VERSION, PYTHON_MAJOR_MINOR, ABI3_FEATURE, LIBPYTHON_NAME, etc.

CONFIG_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/config/python-version.toml"

if [[ ! -f "${CONFIG_FILE}" ]]; then
    echo "ERROR: Config file not found: ${CONFIG_FILE}" >&2
    exit 1
fi

# Extract values using simple grep/sed (no external deps)
export PYTHON_VERSION=$(grep '^version\s*=' "${CONFIG_FILE}" | head -1 | sed -E 's/.*=\s*"([^"]+)".*/\1/')
export PYTHON_MAJOR_MINOR=$(grep '^major_minor\s*=' "${CONFIG_FILE}" | head -1 | sed -E 's/.*=\s*"([^"]+)".*/\1/')
export ABI3_FEATURE=$(grep '^abi3_feature\s*=' "${CONFIG_FILE}" | head -1 | sed -E 's/.*=\s*"([^"]+)".*/\1/')
export LIBPYTHON_NAME=$(grep '^libpython_name\s*=' "${CONFIG_FILE}" | head -1 | sed -E 's/.*=\s*"([^"]+)".*/\1/')
export P4A_DIST_NAME=$(grep '^p4a_dist_name\s*=' "${CONFIG_FILE}" | head -1 | sed -E 's/.*=\s*"([^"]+)".*/\1/')
export P4A_PYTHON_VERSION=$(grep '^p4a_python_version\s*=' "${CONFIG_FILE}" | head -1 | sed -E 's/.*=\s*"([^"]+)".*/\1/')
export CROSS_PYTHON_VERSION=$(grep '^cross_python_version\s*=' "${CONFIG_FILE}" | head -1 | sed -E 's/.*=\s*"([^"]+)".*/\1/')
export CROSS_LIBPYTHON=$(grep '^cross_libpython\s*=' "${CONFIG_FILE}" | head -1 | sed -E 's/.*=\s*"([^"]+)".*/\1/')
export WHEEL_TAG=$(grep '^wheel_tag\s*=' "${CONFIG_FILE}" | head -1 | sed -E 's/.*=\s*"([^"]+)".*/\1/')
export WHEEL_ABI=$(grep '^wheel_abi\s*=' "${CONFIG_FILE}" | head -1 | sed -E 's/.*=\s*"([^"]+)".*/\1/')
export WHEEL_PLATFORM=$(grep '^wheel_platform\s*=' "${CONFIG_FILE}" | head -1 | sed -E 's/.*=\s*"([^"]+)".*/\1/')
export SYSCONFIGDATA_SUFFIX=$(grep '^sysconfigdata_suffix\s*=' "${CONFIG_FILE}" | head -1 | sed -E 's/.*=\s*"([^"]+)".*/\1/')
export P4A_PYTHON_VERSION_MAJOR_MINOR=$(grep '^p4a_python_version_major_minor\s*=' "${CONFIG_FILE}" | head -1 | sed -E 's/.*=\s*"([^"]+)".*/\1/')
export CROSS_LIBPYTHON="${CROSS_LIBPYTHON:-${LIBPYTHON_NAME}}"
export P4A_DIST_NAME="${P4A_DIST_NAME:-remotemedia_hermes}"
export ABI3_FEATURE="${ABI3_FEATURE:-abi3-py310}"

# Export version-specific derived values
export PYTHON_VERSION_TAG="${PYTHON_VERSION}"
export PYTHON_VERSION_SHORT="${PYTHON_MAJOR_MINOR}"
