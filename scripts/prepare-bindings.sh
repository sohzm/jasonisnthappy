#!/bin/bash
# Script to build the Rust library and copy it to all binding directories

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Building Rust FFI library...${NC}"
cd bindings/ffi
cargo build --release
cd ../..

# Detect platform and architecture
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

# Map architecture names
if [ "$ARCH" = "x86_64" ]; then
    ARCH="amd64"
elif [ "$ARCH" = "aarch64" ]; then
    ARCH="arm64"
fi

# Determine library extension
if [ "$OS" = "darwin" ]; then
    LIB_EXT="dylib"
    LIB_DIR="darwin-${ARCH}"
elif [ "$OS" = "linux" ]; then
    LIB_EXT="so"
    LIB_DIR="linux-${ARCH}"
else
    echo "Unsupported OS: $OS"
    exit 1
fi

LIB_NAME="libjasonisnthappy.${LIB_EXT}"

echo -e "${BLUE}Detected platform: ${LIB_DIR}${NC}"
echo -e "${BLUE}Copying library to bindings...${NC}"

# Copy to Go bindings
echo -e "${GREEN}→ Go bindings${NC}"
mkdir -p "bindings/go/lib/${LIB_DIR}"
cp "target/release/${LIB_NAME}" "bindings/go/lib/${LIB_DIR}/"

# Copy to Node bindings
echo -e "${GREEN}→ Node bindings${NC}"
mkdir -p "bindings/napi/lib/${LIB_DIR}"
cp "target/release/${LIB_NAME}" "bindings/napi/lib/${LIB_DIR}/"

# Copy to Python bindings
echo -e "${GREEN}→ Python bindings${NC}"
mkdir -p "bindings/python/jasonisnthappy/lib/${LIB_DIR}"
cp "target/release/${LIB_NAME}" "bindings/python/jasonisnthappy/lib/${LIB_DIR}/"

echo -e "${GREEN}✅ Done! Library copied to all bindings${NC}"
echo ""
echo "You can now run tests:"
echo "  Go:     cd bindings/go/tests && go test -v"
echo "  Node:   cd bindings/napi/tests && npx jest integration.test.js"
echo "  Python: cd bindings/python/tests && python -m pytest test_integration.py -v"
