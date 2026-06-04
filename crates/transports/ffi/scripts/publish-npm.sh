#!/bin/bash
#
# publish-npm.sh - Publish RemoteMedia Node.js native addon to npm
#
# This script publishes the built native addon to npm. It handles:
# - Publishing the main @matbee/remotemedia-native package
# - Optionally publishing platform-specific packages
#
# Usage:
#   ./scripts/publish-npm.sh              # Publish to npm
#   ./scripts/publish-npm.sh --dry-run    # Dry run (show what would be published)
#   ./scripts/publish-npm.sh --tag beta   # Publish with a specific tag
#
# Environment variables:
#   NPM_TOKEN  - npm auth token (required)

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
NODEJS_DIR="$FFI_DIR/nodejs"

# Default options
DRY_RUN=false
SKIP_BUILD=false
NPM_TAG="latest"
ACCESS="public"

print_usage() {
    echo ""
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  --dry-run     Show what would be published without uploading"
    echo "  --skip-build  Skip building, use existing binaries"
    echo "  --tag TAG     npm dist-tag (default: latest)"
    echo "  --access ACC  Package access level: public or restricted (default: public)"
    echo "  --help        Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  NPM_TOKEN  - npm auth token (required for publishing)"
    echo ""
    echo "Examples:"
    echo "  $0                    # Build and publish"
    echo "  $0 --dry-run          # Show what would be published"
    echo "  $0 --tag beta         # Publish with beta tag"
    echo "  $0 --skip-build       # Publish existing build"
    echo ""
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            print_usage
            exit 0
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --tag)
            NPM_TAG="$2"
            shift 2
            ;;
        --access)
            ACCESS="$2"
            shift 2
            ;;
        *)
            echo -e "${RED}Error: Unknown argument '$1'${NC}"
            print_usage
            exit 1
            ;;
    esac
done

echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  RemoteMedia Node.js Package Publisher${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  Package:  ${GREEN}@matbee/remotemedia-native${NC}"
echo -e "  Tag:      ${GREEN}$NPM_TAG${NC}"
echo -e "  Access:   ${GREEN}$ACCESS${NC}"
echo -e "  Dry run:  $([ "$DRY_RUN" = true ] && echo -e "${YELLOW}Yes${NC}" || echo -e "${GREEN}No${NC}")"
echo ""

# Check for npm token
if [ "$DRY_RUN" = false ] && [ -z "$NPM_TOKEN" ]; then
    # Check if already logged in
    if ! npm whoami &>/dev/null; then
        echo -e "${RED}Error: Not logged in to npm and NPM_TOKEN not set.${NC}"
        echo ""
        echo "Either:"
        echo "  1. Run 'npm login' to authenticate interactively"
        echo "  2. Set NPM_TOKEN environment variable"
        echo ""
        echo "To create a token:"
        echo "  1. Go to https://www.npmjs.com/settings/<username>/tokens"
        echo "  2. Create a new access token (Automation)"
        echo "  3. Export it: export NPM_TOKEN='npm_...'"
        echo ""
        exit 1
    fi
fi

# Setup npm auth from token
setup_npm_auth() {
    if [ -n "$NPM_TOKEN" ]; then
        echo -e "${CYAN}Setting up npm authentication...${NC}"
        echo "//registry.npmjs.org/:_authToken=$NPM_TOKEN" > "$NODEJS_DIR/.npmrc"
        echo -e "  ${GREEN}✓${NC} Auth configured"
    fi
}

# Build if needed
if [ "$SKIP_BUILD" = false ]; then
    echo -e "${CYAN}[1/4]${NC} Building packages..."
    "$SCRIPT_DIR/build-npm.sh" --release
else
    echo -e "${CYAN}[1/4]${NC} Skipping build (using existing binaries)..."
fi

# Verify native binaries exist
echo -e "${CYAN}[2/4]${NC} Verifying native binaries..."

cd "$NODEJS_DIR"

# Check for at least one .node file
NODE_FILES=$(ls -1 *.node 2>/dev/null || ls -1 ../../../target/release/*.node 2>/dev/null || echo "")

if [ -z "$NODE_FILES" ]; then
    echo -e "${RED}Error: No native binaries found. Run build first.${NC}"
    exit 1
fi

echo -e "  ${GREEN}✓${NC} Found native binaries:"
shopt -s nullglob
for f in *.node ../../../target/release/*.node; do
    if [ -f "$f" ]; then
        SIZE=$(du -h "$f" | cut -f1)
        echo -e "    ${GREEN}→${NC} $(basename "$f") ($SIZE)"
    fi
done
shopt -u nullglob

# Get version from package.json
PACKAGE_VERSION=$(node -p "require('./package.json').version")
PACKAGE_NAME=$(node -p "require('./package.json').name")

echo ""
echo -e "  ${CYAN}Package:${NC} $PACKAGE_NAME@$PACKAGE_VERSION"

# Verify package contents
echo -e "${CYAN}[3/4]${NC} Preparing package..."

# Generate types if not skipping build (prepublishOnly would rebuild, so skip it)
if [ "$SKIP_BUILD" = false ] && [ "$DRY_RUN" = false ]; then
    npm run generate-types 2>/dev/null || true
fi

# Show what would be published
echo ""
echo -e "  ${CYAN}Files to be published:${NC}"
npm pack --dry-run 2>/dev/null | head -20 || {
    echo "    index.js"
    echo "    index.d.ts"
    echo "    *.node"
}

# Publish
echo -e "${CYAN}[4/4]${NC} Publishing to npm..."

if [ "$DRY_RUN" = true ]; then
    echo ""
    echo -e "${YELLOW}DRY RUN - Would execute:${NC}"
    echo -e "  npm publish --access $ACCESS --tag $NPM_TAG"
    echo ""
    echo -e "${GREEN}Package contents:${NC}"
    npm pack --dry-run 2>&1 | head -30 || true
else
    setup_npm_auth
    
    # Publish with specified options
    npm publish --access "$ACCESS" --tag "$NPM_TAG" || {
        ERROR_CODE=$?
        
        # Check if it's a version conflict
        if [ $ERROR_CODE -eq 1 ]; then
            echo -e "${YELLOW}Warning: Package may already exist at this version.${NC}"
            echo -e "To publish a new version, update version in package.json first."
        fi
        
        # Cleanup auth
        rm -f "$NODEJS_DIR/.npmrc"
        exit $ERROR_CODE
    }
    
    # Cleanup auth
    rm -f "$NODEJS_DIR/.npmrc"
fi

cd "$FFI_DIR"

echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  ✅ Publish complete!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "Install from npm:"
echo -e "  ${BLUE}npm install $PACKAGE_NAME${NC}"
if [ "$NPM_TAG" != "latest" ]; then
    echo -e "  ${BLUE}npm install $PACKAGE_NAME@$NPM_TAG${NC}"
fi
echo ""
