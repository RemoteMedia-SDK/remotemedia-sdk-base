#!/bin/bash
# Run WebRTC server with correct Python environment

# Use the Python from our SDK setup
export PYTHON=/home/acidhax/anaconda3/bin/python

# Verify protobuf version
PROTOBUF_VERSION=$(${PYTHON} -c "import google.protobuf; print(google.protobuf.__version__)" 2>&1)
echo "Using Python: ${PYTHON}"
echo "Protobuf version: ${PROTOBUF_VERSION}"

if [ "$PROTOBUF_VERSION" != "6.31.1" ]; then
    echo "⚠️  WARNING: Protobuf version mismatch!"
    echo "   Expected: 6.31.1"
    echo "   Got: $PROTOBUF_VERSION"
    echo ""
    echo "   Install correct version:"
    echo "   ${PYTHON} -m pip install --upgrade protobuf==6.31.1"
    echo ""
fi

# Add the SDK python-client to PYTHONPATH
SDK_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
export PYTHONPATH="${SDK_ROOT}/python-client:${PYTHONPATH}"

echo "PYTHONPATH includes: ${SDK_ROOT}/python-client"
echo ""

# Run the server
cd "$(dirname "${BASH_SOURCE[0]}")"
cargo run --bin webrtc_server "$@"

