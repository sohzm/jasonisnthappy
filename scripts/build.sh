#!/bin/bash
# Simple build script - builds for all platforms and outputs to builds/ directory
set -e

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILDS_DIR="$REPO_ROOT/builds"

echo "🔨 Building jasonisnthappy FFI (dynamic + static libs)..."
echo ""

# Clean and create builds directory
rm -rf "$BUILDS_DIR"
mkdir -p "$BUILDS_DIR"

cd "$REPO_ROOT/bindings/ffi"

# =============================================================================
# macOS ARM64 (native build)
# =============================================================================
echo "📦 Building macOS ARM64 (native)..."
cargo build --release
cp target/release/libjasonisnthappy.dylib "$BUILDS_DIR/darwin-arm64-dynamic.dylib"
cp target/release/libjasonisnthappy.a "$BUILDS_DIR/darwin-arm64-static.a"

# Build NAPI .node file
cd "$REPO_ROOT/bindings/napi"
cargo build --release
cp target/release/libjasonisnthappy_napi.dylib "$BUILDS_DIR/darwin-arm64.node"
cd "$REPO_ROOT/bindings/ffi"

echo "✅ macOS ARM64 done"
echo ""

# =============================================================================
# macOS Intel (cross-compile)
# =============================================================================
echo "📦 Building macOS Intel (cross-compile)..."
rustup target add x86_64-apple-darwin 2>/dev/null || true
cargo build --release --target x86_64-apple-darwin
cp target/x86_64-apple-darwin/release/libjasonisnthappy.dylib "$BUILDS_DIR/darwin-amd64-dynamic.dylib"
cp target/x86_64-apple-darwin/release/libjasonisnthappy.a "$BUILDS_DIR/darwin-amd64-static.a"

# Build NAPI .node file
cd "$REPO_ROOT/bindings/napi"
cargo build --release --target x86_64-apple-darwin
cp target/x86_64-apple-darwin/release/libjasonisnthappy_napi.dylib "$BUILDS_DIR/darwin-amd64.node"
cd "$REPO_ROOT/bindings/ffi"

echo "✅ macOS Intel done"
echo ""

# =============================================================================
# Linux x64 (Docker)
# =============================================================================
echo "📦 Building Linux x64 (Docker)..."
docker run --rm --platform linux/amd64 \
    -v "$REPO_ROOT:/workspace" \
    -w /workspace/bindings/ffi \
    rust:1.83 \
    bash -c "
        cargo build --release --target x86_64-unknown-linux-gnu
    "

cp target/x86_64-unknown-linux-gnu/release/libjasonisnthappy.so "$BUILDS_DIR/linux-amd64-dynamic.so"
cp target/x86_64-unknown-linux-gnu/release/libjasonisnthappy.a "$BUILDS_DIR/linux-amd64-static.a"

# Build NAPI .node file (needs newer Rust than FFI)
cd "$REPO_ROOT/bindings/napi"
docker run --rm --platform linux/amd64 \
    -v "$REPO_ROOT:/workspace" \
    -w /workspace/bindings/napi \
    rust:latest \
    bash -c "
        cargo build --release --target x86_64-unknown-linux-gnu
    "
cp target/x86_64-unknown-linux-gnu/release/libjasonisnthappy_napi.so "$BUILDS_DIR/linux-amd64.node"
cd "$REPO_ROOT/bindings/ffi"

echo "✅ Linux x64 done"
echo ""

# =============================================================================
# Linux ARM64 (Docker)
# =============================================================================
echo "📦 Building Linux ARM64 (Docker)..."
docker run --rm --platform linux/arm64 \
    -v "$REPO_ROOT:/workspace" \
    -w /workspace/bindings/ffi \
    rust:1.83 \
    bash -c "
        cargo build --release --target aarch64-unknown-linux-gnu
    "

cp target/aarch64-unknown-linux-gnu/release/libjasonisnthappy.so "$BUILDS_DIR/linux-arm64-dynamic.so"
cp target/aarch64-unknown-linux-gnu/release/libjasonisnthappy.a "$BUILDS_DIR/linux-arm64-static.a"

# Build NAPI .node file (needs newer Rust than FFI)
cd "$REPO_ROOT/bindings/napi"
docker run --rm --platform linux/arm64 \
    -v "$REPO_ROOT:/workspace" \
    -w /workspace/bindings/napi \
    rust:latest \
    bash -c "
        cargo build --release --target aarch64-unknown-linux-gnu
    "
cp target/aarch64-unknown-linux-gnu/release/libjasonisnthappy_napi.so "$BUILDS_DIR/linux-arm64.node"
cd "$REPO_ROOT/bindings/ffi"

echo "✅ Linux ARM64 done"
echo ""

# =============================================================================
# Windows x64 (Docker)
# =============================================================================
echo "📦 Building Windows x64 (Docker)..."
docker run --rm --platform linux/amd64 \
    -v "$REPO_ROOT:/workspace" \
    -w /workspace/bindings/ffi \
    rust:1.83 \
    bash -c "
        apt-get update -qq && apt-get install -y -qq gcc-mingw-w64-x86-64
        rustup target add x86_64-pc-windows-gnu
        cargo build --release --target x86_64-pc-windows-gnu
    "

cp target/x86_64-pc-windows-gnu/release/jasonisnthappy.dll "$BUILDS_DIR/windows-amd64-dynamic.dll"
cp target/x86_64-pc-windows-gnu/release/libjasonisnthappy.a "$BUILDS_DIR/windows-amd64-static.lib"

# Build NAPI .node file (needs newer Rust than FFI)
cd "$REPO_ROOT/bindings/napi"
docker run --rm --platform linux/amd64 \
    -v "$REPO_ROOT:/workspace" \
    -w /workspace/bindings/napi \
    rust:latest \
    bash -c "
        apt-get update -qq && apt-get install -y -qq gcc-mingw-w64-x86-64
        rustup target add x86_64-pc-windows-gnu
        cargo build --release --target x86_64-pc-windows-gnu
    "
cp target/x86_64-pc-windows-gnu/release/jasonisnthappy_napi.dll "$BUILDS_DIR/windows-amd64.node"
cd "$REPO_ROOT/bindings/ffi"

echo "✅ Windows x64 done"
echo ""

# =============================================================================
# Windows ARM64 (Docker) - DISABLED
# Cross-compiling to Windows ARM64 from Linux requires Windows SDK
# This is a very uncommon platform, so we skip it for now
# =============================================================================
# echo "📦 Building Windows ARM64 (Docker)..."
# docker run --rm --platform linux/amd64 \
#     -v "$REPO_ROOT:/workspace" \
#     -w /workspace/bindings/ffi \
#     rust:1.83 \
#     bash -c "
#         rustup target add aarch64-pc-windows-gnullvm
#         cargo build --release --target aarch64-pc-windows-gnullvm
#     "
#
# cp target/aarch64-pc-windows-gnullvm/release/jasonisnthappy.dll "$BUILDS_DIR/windows-arm64-dynamic.dll"
# echo "✅ Windows ARM64 done"
# echo ""

# =============================================================================
# Build Go Bindings
# =============================================================================
echo "📦 Building Go bindings..."
mkdir -p "$BUILDS_DIR/go"

# Copy Go source files
cp "$REPO_ROOT/bindings/go/jasonisnthappy.go" "$BUILDS_DIR/go/"
cp "$REPO_ROOT/bindings/go/jasonisnthappy.h" "$BUILDS_DIR/go/"
cp "$REPO_ROOT/bindings/go/go.mod" "$BUILDS_DIR/go/" 2>/dev/null || echo "module jasonisnthappy" > "$BUILDS_DIR/go/go.mod"

# Copy static libraries for Go CGo
cp "$BUILDS_DIR/darwin-arm64-static.a" "$BUILDS_DIR/go/libjasonisnthappy-darwin-arm64.a"
cp "$BUILDS_DIR/darwin-amd64-static.a" "$BUILDS_DIR/go/libjasonisnthappy-darwin-amd64.a"
cp "$BUILDS_DIR/linux-amd64-static.a" "$BUILDS_DIR/go/libjasonisnthappy-linux-amd64.a"
cp "$BUILDS_DIR/linux-arm64-static.a" "$BUILDS_DIR/go/libjasonisnthappy-linux-arm64.a"
cp "$BUILDS_DIR/windows-amd64-static.lib" "$BUILDS_DIR/go/libjasonisnthappy-windows-amd64.a"

# Create a README for Go bindings
cat > "$BUILDS_DIR/go/README.md" << 'EOF'
# jasonisnthappy Go Bindings

Go bindings for the jasonisnthappy database using CGo.

## Installation

```bash
go get github.com/sohzm/jasonisnthappy/bindings/go
```

## Usage

```go
import "github.com/sohzm/jasonisnthappy/bindings/go"

db, err := jasonisnthappy.Open("mydb.db")
if err != nil {
    panic(err)
}
defer db.Close()
```

See examples/ for more usage examples.
EOF

echo "✅ Go bindings packaged"
echo ""

# =============================================================================
# Build Python Wheels
# =============================================================================
echo "📦 Building Python wheels..."
mkdir -p "$BUILDS_DIR/python"

cd "$REPO_ROOT/bindings/python"

# Build wheels for different platforms
# macOS ARM64
echo "  Building Python wheel for macOS ARM64..."
cp "$BUILDS_DIR/darwin-arm64-dynamic.dylib" jasonisnthappy/libjasonisnthappy.dylib
python3 setup.py bdist_wheel --plat-name macosx_11_0_arm64
cp dist/*.whl "$BUILDS_DIR/python/" || true
rm -rf build dist *.egg-info
rm jasonisnthappy/libjasonisnthappy.dylib

# macOS Intel
echo "  Building Python wheel for macOS Intel..."
cp "$BUILDS_DIR/darwin-amd64-dynamic.dylib" jasonisnthappy/libjasonisnthappy.dylib
python3 setup.py bdist_wheel --plat-name macosx_10_9_x86_64
cp dist/*.whl "$BUILDS_DIR/python/" || true
rm -rf build dist *.egg-info
rm jasonisnthappy/libjasonisnthappy.dylib

# Linux x64
echo "  Building Python wheel for Linux x64..."
cp "$BUILDS_DIR/linux-amd64-dynamic.so" jasonisnthappy/libjasonisnthappy.so
python3 setup.py bdist_wheel --plat-name manylinux2014_x86_64
cp dist/*.whl "$BUILDS_DIR/python/" || true
rm -rf build dist *.egg-info
rm jasonisnthappy/libjasonisnthappy.so

# Linux ARM64
echo "  Building Python wheel for Linux ARM64..."
cp "$BUILDS_DIR/linux-arm64-dynamic.so" jasonisnthappy/libjasonisnthappy.so
python3 setup.py bdist_wheel --plat-name manylinux2014_aarch64
cp dist/*.whl "$BUILDS_DIR/python/" || true
rm -rf build dist *.egg-info
rm jasonisnthappy/libjasonisnthappy.so

# Windows x64
echo "  Building Python wheel for Windows x64..."
cp "$BUILDS_DIR/windows-amd64-dynamic.dll" jasonisnthappy/jasonisnthappy.dll
python3 setup.py bdist_wheel --plat-name win_amd64
cp dist/*.whl "$BUILDS_DIR/python/" || true
rm -rf build dist *.egg-info
rm jasonisnthappy/jasonisnthappy.dll

cd "$REPO_ROOT"

echo "✅ Python wheels built"
echo ""

# =============================================================================
# Package Node.js Bindings
# =============================================================================
echo "📦 Packaging Node.js bindings..."
mkdir -p "$BUILDS_DIR/nodejs"

# Copy Node.js package files
cp "$REPO_ROOT/bindings/napi/package.json" "$BUILDS_DIR/nodejs/" 2>/dev/null || echo '{"name":"jasonisnthappy","version":"0.1.0"}' > "$BUILDS_DIR/nodejs/package.json"
cp "$REPO_ROOT/bindings/napi/index.js" "$BUILDS_DIR/nodejs/" 2>/dev/null || true
cp "$REPO_ROOT/bindings/napi/index.d.ts" "$BUILDS_DIR/nodejs/" 2>/dev/null || true

# Copy .node files
cp "$BUILDS_DIR/darwin-arm64.node" "$BUILDS_DIR/nodejs/"
cp "$BUILDS_DIR/darwin-amd64.node" "$BUILDS_DIR/nodejs/"
cp "$BUILDS_DIR/linux-amd64.node" "$BUILDS_DIR/nodejs/"
cp "$BUILDS_DIR/linux-arm64.node" "$BUILDS_DIR/nodejs/"
cp "$BUILDS_DIR/windows-amd64.node" "$BUILDS_DIR/nodejs/"

echo "✅ Node.js bindings packaged"
echo ""

echo "========================================="
echo "✅ All builds complete!"
echo "========================================="
echo ""
echo "📁 Binaries are in: builds/"
ls -lh "$BUILDS_DIR"
echo ""
echo "Files ready for release:"
echo ""
echo "Dynamic libraries (for Python with FFI):"
echo "  - darwin-arm64-dynamic.dylib (macOS Apple Silicon)"
echo "  - darwin-amd64-dynamic.dylib (macOS Intel)"
echo "  - linux-amd64-dynamic.so (Linux x64)"
echo "  - linux-arm64-dynamic.so (Linux ARM64)"
echo "  - windows-amd64-dynamic.dll (Windows x64)"
echo ""
echo "Static libraries (for Go static linking):"
echo "  - darwin-arm64-static.a (macOS Apple Silicon)"
echo "  - darwin-amd64-static.a (macOS Intel)"
echo "  - linux-amd64-static.a (Linux x64)"
echo "  - linux-arm64-static.a (Linux ARM64)"
echo "  - windows-amd64-static.lib (Windows x64)"
echo ""
echo "NAPI .node files (for Node.js native addons):"
echo "  - darwin-arm64.node (macOS Apple Silicon)"
echo "  - darwin-amd64.node (macOS Intel)"
echo "  - linux-amd64.node (Linux x64)"
echo "  - linux-arm64.node (Linux ARM64)"
echo "  - windows-amd64.node (Windows x64)"
echo ""
echo "Go bindings package: builds/go/"
echo "Python wheels: builds/python/"
echo "Node.js package: builds/nodejs/"
echo ""
echo "Note: Windows ARM64 build is disabled (requires Windows SDK)"
echo ""
echo "📤 Next steps:"
echo "  1. Test the bindings work"
echo "  2. Create a GitHub release (tag: v0.1.0)"
echo "  3. Upload ALL files from builds/ to the release"
echo "  4. Publish to PyPI (Python wheels)"
echo "  5. Publish to npm (Node.js package)"
echo "  6. See docs/PUBLISHING.md for complete release checklist"
echo ""
