#!/bin/bash
#
# publish-pip.sh - Publish RemoteMedia FFI Python package to PyPI
#
# This script publishes the built wheels and sdist to PyPI or TestPyPI.
# It requires the MATURIN_PYPI_TOKEN environment variable to be set.
#
# Usage:
#   ./scripts/publish-pip.sh              # Publish to PyPI
#   ./scripts/publish-pip.sh --test       # Publish to TestPyPI
#   ./scripts/publish-pip.sh --dry-run    # Dry run (show what would be published)
#
# Environment variables:
#   MATURIN_PYPI_TOKEN      - PyPI API token (required for PyPI)
#   MATURIN_TEST_PYPI_TOKEN - TestPyPI API token (required for TestPyPI)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Script directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FFI_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DIST_DIR="$FFI_DIR/dist"

# Default options
USE_TEST_PYPI=false
DRY_RUN=false
SKIP_BUILD=false

print_usage() {
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --test        Publish to TestPyPI instead of PyPI"
    echo "  --dry-run     Show what would be published without uploading"
    echo "  --skip-build  Skip building, use existing wheels in dist/"
    echo "  --help        Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  MATURIN_PYPI_TOKEN      - PyPI API token"
    echo "  MATURIN_TEST_PYPI_TOKEN - TestPyPI API token"
    echo ""
    echo "Examples:"
    echo "  $0                    # Build and publish to PyPI"
    echo "  $0 --test             # Build and publish to TestPyPI"
    echo "  $0 --dry-run          # Show what would be published"
    echo "  $0 --skip-build       # Publish existing wheels"
    echo ""
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            print_usage
            exit 0
            ;;
        --test)
            USE_TEST_PYPI=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        *)
            echo -e "${RED}Error: Unknown argument '$1'${NC}"
            print_usage
            exit 1
            ;;
    esac
done

# Determine repository
if [ "$USE_TEST_PYPI" = true ]; then
    REPOSITORY="testpypi"
    REPOSITORY_URL="https://test.pypi.org/simple/"
    TOKEN_VAR="MATURIN_TEST_PYPI_TOKEN"
    PYPI_TOKEN="${MATURIN_TEST_PYPI_TOKEN:-}"
else
    REPOSITORY="pypi"
    REPOSITORY_URL="https://pypi.org/simple/"
    TOKEN_VAR="MATURIN_PYPI_TOKEN"
    PYPI_TOKEN="${MATURIN_PYPI_TOKEN:-}"
fi

echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  RemoteMedia Python Package Publisher${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  Package:     ${GREEN}remotemedia-ffi${NC}"
echo -e "  Repository:  ${GREEN}$REPOSITORY${NC}"
echo -e "  Dry run:     $([ "$DRY_RUN" = true ] && echo -e "${YELLOW}Yes${NC}" || echo -e "${GREEN}No${NC}")"
echo ""

# Check for API token
if [ "$DRY_RUN" = false ] && [ -z "$PYPI_TOKEN" ]; then
    echo -e "${RED}Error: $TOKEN_VAR environment variable not set.${NC}"
    echo ""
    echo "To get an API token:"
    if [ "$USE_TEST_PYPI" = true ]; then
        echo "  1. Go to https://test.pypi.org/manage/account/token/"
    else
        echo "  1. Go to https://pypi.org/manage/account/token/"
    fi
    echo "  2. Create a new API token"
    echo "  3. Export it: export $TOKEN_VAR='pypi-...'"
    echo ""
    exit 1
fi

# Build if needed
if [ "$SKIP_BUILD" = false ]; then
    echo -e "${CYAN}[1/3]${NC} Building packages..."
    "$SCRIPT_DIR/build-pip.sh" --release --all
else
    echo -e "${CYAN}[1/3]${NC} Skipping build (using existing packages)..."
fi

# Verify dist directory
echo -e "${CYAN}[2/3]${NC} Verifying packages..."

if [ ! -d "$DIST_DIR" ]; then
    echo -e "${RED}Error: dist/ directory not found. Run build first.${NC}"
    exit 1
fi

WHEEL_COUNT=$(ls -1 "$DIST_DIR"/*.whl 2>/dev/null | wc -l || echo "0")
SDIST_COUNT=$(ls -1 "$DIST_DIR"/*.tar.gz 2>/dev/null | wc -l || echo "0")

if [ "$WHEEL_COUNT" -eq 0 ] && [ "$SDIST_COUNT" -eq 0 ]; then
    echo -e "${RED}Error: No packages found in dist/. Run build first.${NC}"
    exit 1
fi

echo -e "  ${GREEN}✓${NC} Found $WHEEL_COUNT wheel(s) and $SDIST_COUNT sdist(s)"

# List packages to publish
echo ""
echo -e "  ${CYAN}Packages to publish:${NC}"
for f in "$DIST_DIR"/*.whl "$DIST_DIR"/*.tar.gz; do
    if [ -f "$f" ]; then
        SIZE=$(du -h "$f" | cut -f1)
        echo -e "    ${GREEN}→${NC} $(basename "$f") ($SIZE)"
    fi
done
echo ""

# Publish
echo -e "${CYAN}[3/3]${NC} Publishing to $REPOSITORY..."

cd "$FFI_DIR"

if [ "$DRY_RUN" = true ]; then
    echo ""
    echo -e "${YELLOW}DRY RUN - Would execute:${NC}"
    echo -e "  maturin upload --repository $REPOSITORY $DIST_DIR/*"
    echo ""
    echo -e "${GREEN}Packages that would be published:${NC}"
    ls -la "$DIST_DIR"/*.whl "$DIST_DIR"/*.tar.gz 2>/dev/null || true
else
    # Use twine for more control, or maturin upload
    if command -v twine &> /dev/null; then
        echo -e "  Using twine for upload..."
        
        if [ "$USE_TEST_PYPI" = true ]; then
            TWINE_PASSWORD="$PYPI_TOKEN" twine upload \
                --repository-url https://test.pypi.org/legacy/ \
                --username __token__ \
                "$DIST_DIR"/*.whl "$DIST_DIR"/*.tar.gz
        else
            TWINE_PASSWORD="$PYPI_TOKEN" twine upload \
                --username __token__ \
                "$DIST_DIR"/*.whl "$DIST_DIR"/*.tar.gz
        fi
    else
        echo -e "  Using maturin for upload..."
        
        # Set token via environment variable
        export MATURIN_PYPI_TOKEN="$PYPI_TOKEN"
        
        if [ "$USE_TEST_PYPI" = true ]; then
            maturin upload --repository testpypi "$DIST_DIR"/*.whl "$DIST_DIR"/*.tar.gz
        else
            maturin upload "$DIST_DIR"/*.whl "$DIST_DIR"/*.tar.gz
        fi
    fi
fi

echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  ✅ Publish complete!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo ""

if [ "$USE_TEST_PYPI" = true ]; then
    echo -e "Install from TestPyPI:"
    echo -e "  ${BLUE}pip install -i https://test.pypi.org/simple/ remotemedia-ffi${NC}"
else
    echo -e "Install from PyPI:"
    echo -e "  ${BLUE}pip install remotemedia-ffi${NC}"
fi
echo ""
