# Publishing Guide

## How the Bindings Work

### Architecture

```
builds/                              # You upload these to GitHub Releases
├── darwin-arm64-dynamic.dylib       # macOS Apple Silicon
├── darwin-amd64-dynamic.dylib       # macOS Intel
├── linux-amd64-dynamic.so           # Linux x64
├── linux-arm64-dynamic.so           # Linux ARM64 (Raspberry Pi, AWS Graviton)
└── windows-amd64-dynamic.dll        # Windows x64

When users install:
├── Go → auto-downloads on first build (via init() + CGO)
├── npm → auto-downloads during install (via postinstall hook)
└── pip → auto-downloads during install (via setup.py custom command)
```

**Note**: All bindings use **dynamic linking only**. No static libraries are built.

### How Binaries Are Used

**Go (CGO with auto-download)**:
- User runs: `go get github.com/sohzm/jasonisnthappy/bindings/go`
- On first `go build` or `go run`, the `init()` function downloads the library
- Library is saved to `$GOPATH/pkg/mod/.../bindings/go/lib/<platform>/`
- CGO links against the downloaded library automatically
- **No manual steps!** Works like other CGO packages (go-sqlite3, etc.)

**JavaScript (dynamic linking with auto-download)**:
- User runs: `npm install @sohzm/jasonisnthappy`
- npm downloads package from npmjs.com
- **Postinstall hook** automatically runs `download-libs.js` to download the correct library
- Library is saved to `node_modules/@sohzm/jasonisnthappy/lib/<platform>/`
- Downloads during install, not runtime! ✅

**Python (dynamic linking with auto-download)**:
- User runs: `pip install jasonisnthappy`
- pip downloads package from pypi.org
- **Custom setup.py command** automatically runs `download_libs.py` during installation
- Library is saved to site-packages `jasonisnthappy/lib/<platform>/`
- Downloads during install, not runtime! ✅
- Fallback: If install download fails, `loader.py` can download on first import

---

## Build & Release Process

### 1. Build Binaries

```bash
# Quick test (macOS only - builds both ARM64 and Intel)
./scripts/build-simple.sh

# Full build (all platforms - requires Docker)
./scripts/build.sh
```

This creates `builds/` directory with dynamic libraries for 5 platforms:
- macOS ARM64 & Intel
- Linux x64 & ARM64
- Windows x64

**Note:** Windows ARM64 is not built (requires Windows SDK for cross-compilation).

### 2. Create GitHub Release

```bash
# Create and push a tag
git tag v0.1.0
git push origin v0.1.0

# Go to GitHub → Releases → Create new release
# - Choose tag: v0.1.0
# - Upload all files from builds/
```

### 3. Update Version in Code

Update the version in:
- `bindings/go/go.mod` (already uses git tags)
- `bindings/napi/package.json` → `"version": "0.1.0"`
- `bindings/python/pyproject.toml` → `version = "0.1.0"`

---

## Binary Distribution Strategy

All bindings use **auto-download** for native libraries:

**How it works:**
- Packages are published to registries **without** binaries (~few KB)
- Binaries are automatically downloaded from GitHub releases
- Binaries are cached locally (~1 MB per platform)
- Users only download what they need for their platform

**Download timing:**
- **npm**: Downloads during `npm install` (postinstall hook)
- **pip**: Downloads during `pip install` (setup.py custom command)
- **Go**: Downloads on first `go build` (init() function)

**Benefits:**
- Small package size (~few KB instead of ~6 MB)
- Users only download binaries for their platform
- Saves bandwidth on package registries
- Similar to how PyTorch, TensorFlow, and other native-extension packages work

**Install locations:**
- **npm**: `node_modules/@sohzm/jasonisnthappy/lib/<platform>/`
- **pip**: `site-packages/jasonisnthappy/lib/<platform>/`
- **Go**: `$GOPATH/pkg/mod/.../bindings/go/lib/<platform>/` (in module cache)

**Requirements:**
- Internet connection during install/first build
- GitHub releases must be available
- **Go**: CGO enabled (default on most systems)

---

## Publishing to Package Registries

### Go (No Action Needed!)

Go fetches directly from GitHub:
```bash
# Users just do:
go get github.com/sohzm/jasonisnthappy/bindings/go@v0.1.0
```

The binaries auto-download on first use via `loader.go`. No manual steps required!

### npm

```bash
cd bindings/napi

# Make sure package.json has correct version
# Binaries auto-download during postinstall

npm publish
```

The package is small (~few KB) because binaries are downloaded on-demand.

Users install with:
```bash
npm install @sohzm/jasonisnthappy
# Postinstall automatically downloads the correct library!
```

### PyPI

```bash
cd bindings/python

# Make sure pyproject.toml has correct version
# Binaries auto-download on first import

python -m build
python -m twine upload dist/*
```

The package is small (~few KB) because binaries are downloaded on-demand.

Users install with:
```bash
pip install jasonisnthappy
# Library auto-downloads on first import!
```

---

## GitHub Releases URL Pattern

### Direct Download URLs

For a specific version:
```
https://github.com/sohzm/jasonisnthappy/releases/download/v0.1.0/darwin-arm64-dynamic.dylib
```

For the **latest** release (always points to newest):
```
https://github.com/sohzm/jasonisnthappy/releases/latest/download/darwin-arm64-dynamic.dylib
```

**Important**: The `/latest/` URL automatically redirects to the newest release!

### All Available Files

```
# macOS
darwin-arm64-dynamic.dylib
darwin-amd64-dynamic.dylib

# Linux
linux-amd64-dynamic.so
linux-arm64-dynamic.so

# Windows
windows-amd64-dynamic.dll
```

### Recommended Approach

**For stable releases**: Hardcode the version
```
https://github.com/sohzm/jasonisnthappy/releases/download/v0.1.0/darwin-arm64-dynamic.dylib
```

**For auto-updates**: Use `/latest/` (this is what Go bindings use)
```
https://github.com/sohzm/jasonisnthappy/releases/latest/download/darwin-arm64-dynamic.dylib
```

---

## Auto-Download Implementation Details

All bindings now have auto-download implemented! Here's how they work:

### npm (postinstall script)

**Implementation:** `bindings/napi/download-libs.js`

The script is configured in `package.json`:
```json
{
  "scripts": {
    "postinstall": "node download-libs.js"
  }
}
```

Key features:
- Auto-detects platform and architecture
- Downloads from GitHub releases `/latest/` URL
- Follows redirects (GitHub uses 302 redirects)
- Shows download progress
- Skips download if library already exists
- Saves to `lib/<platform>/` directory
- Provides helpful error messages with manual download instructions

### Python (setup.py custom command)

**Implementation:** `bindings/python/download_libs.py` + custom setup.py

The download is triggered during `pip install` via setup.py:
```python
from setuptools.command.install import install

class PostInstallCommand(install):
    def run(self):
        install.run(self)
        subprocess.check_call([sys.executable, 'download_libs.py'])

setup(
    cmdclass={'install': PostInstallCommand}
)
```

Key features of `download_libs.py`:
- Auto-detects platform and architecture
- Downloads from GitHub releases `/latest/` URL
- Installs to `jasonisnthappy/lib/<platform>/` in site-packages
- Shows download progress during `pip install`
- Skips download if library already exists
- If download fails, falls back to runtime download via `loader.py`
- Similar to how PyTorch/TensorFlow handle native binaries

### Go (CGO + loader)

**Implementation:** `bindings/go/jasonisnthappy.go` (CGO) + `bindings/go/loader.go` (auto-download)

Uses **proper CGO** with automatic library download:

```go
/*
#cgo darwin,arm64 LDFLAGS: -L${SRCDIR}/lib/darwin-arm64 -ljasonisnthappy
#cgo linux,amd64 LDFLAGS: -L${SRCDIR}/lib/linux-amd64 -ljasonisnthappy
// ... platform-specific LDFLAGS

#include "jasonisnthappy.h"
*/
import "C"

func init() {
    // Download library if missing
    if err := ensureLibrary(); err != nil {
        panic(err)
    }
}
```

**Key features:**
- Uses CGO for native linking (standard Go approach)
- Auto-detects platform and architecture
- Downloads library to `lib/<platform>/` on first build
- CGO `LDFLAGS` point to the downloaded library
- Uses `sync.Once` for thread-safe one-time download
- Works like other CGO packages (go-sqlite3, go-python, etc.)

**User experience:**
```bash
go get github.com/sohzm/jasonisnthappy/bindings/go
go build  # Downloads library on first build
# Just works!
```

---

## Testing Before Publishing

### Test npm Package Locally

```bash
cd bindings/napi
npm pack
# This creates a .tgz file

cd /tmp
npm install /path/to/jasonisnthappy-0.1.0.tgz
node -e "const db = require('@sohzm/jasonisnthappy'); console.log('Works!');"
```

### Test Python Package Locally

```bash
cd bindings/python
pip install -e .
python -c "import jasonisnthappy; print('Works!')"
```

### Test Go Package Locally

```bash
cd bindings/go
# Libraries auto-download on first use, just run tests
go test
```

Or test the auto-download:
```bash
# Clear cache to test fresh download
rm -rf ~/.jasonisnthappy/
go test
# Should auto-download and pass
```

---

## Release Checklist

- [ ] Build all binaries: `./scripts/build.sh`
- [ ] Test binaries work on your platform
- [ ] Create git tag: `git tag v0.1.0`
- [ ] Push tag: `git push origin v0.1.0`
- [ ] Create GitHub release
- [ ] Upload all files from `builds/` to the release
- [ ] Update version numbers in package files
- [ ] Test packages locally
- [ ] Publish to npm: `cd bindings/napi && npm publish`
- [ ] Publish to PyPI: `cd bindings/python && python -m build && twine upload dist/*`
- [ ] Update documentation with installation instructions

---

## Version Management

### Semantic Versioning

Use semver: `vMAJOR.MINOR.PATCH`

- `v0.1.0` - Initial release
- `v0.1.1` - Bug fixes
- `v0.2.0` - New features
- `v1.0.0` - Stable release

### Updating Versions

When releasing a new version:

1. Update version in all places:
   - `bindings/napi/package.json`
   - `bindings/python/pyproject.toml`
   - Git tag

2. Build and upload new binaries

3. The `/latest/` URL will automatically point to the new release!

---

## Platform Support

All bindings support:
- **macOS**: Intel (x64) and Apple Silicon (ARM64)
- **Linux**: x64 and ARM64 (Raspberry Pi, AWS Graviton, etc.)
- **Windows**: x64 only (ARM64 not currently supported)

All platforms use **dynamic linking** (`.dylib`, `.so`, `.dll` files).

---

## Summary

**Simple workflow:**

1. `./scripts/build.sh` → builds all dynamic libraries for 5 platforms
2. Create GitHub release with tag → upload all binaries from `builds/`
3. Publish npm/PyPI packages (binaries auto-download for users)
4. Go users fetch from GitHub directly (binaries auto-download on first use)

**Auto-download implementation:**
- **npm**: ✅ Downloads during `npm install` via postinstall hook (download-libs.js)
- **pip**: ✅ Downloads during `pip install` via setup.py custom command (download_libs.py)
- **Go**: ✅ Downloads on first `go build` via init() + CGO (standard Go approach)

**All three languages** use proper package ecosystem patterns - downloads happen automatically!
