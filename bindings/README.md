# jasonisnthappy Language Bindings

Bindings for Go, JavaScript (Node/Deno/Bun), and Python.

## Quick Links

- **Go**: [bindings/go/](go/)
- **JavaScript**: [bindings/napi/](napi/)
- **Python**: [bindings/python/](python/)

## Architecture

```
Rust Core (jasonisnthappy)
         ↓
    C FFI Layer (bindings/ffi/)
         ↓
   ┌─────┼─────┐
   ↓     ↓     ↓
  Go    JS    Python
```

All bindings use a shared C FFI layer that exposes database functions as C-compatible APIs.

## Building

### Quick Test (macOS only)
```bash
./scripts/build-simple.sh
```

### Full Build (All Platforms - requires Docker)
```bash
./scripts/build.sh
```

This builds:
- macOS ARM64 (native)
- macOS Intel (cross-compile)
- Linux x64 (Docker)
- Linux ARM64 (Docker)
- Windows x64 (Docker)

Output goes to `builds/` with flat naming:
```
builds/
├── darwin-arm64-dynamic.dylib   (macOS Apple Silicon)
├── darwin-amd64-dynamic.dylib   (macOS Intel)
├── linux-amd64-dynamic.so       (Linux x64)
├── linux-arm64-dynamic.so       (Linux ARM64 - Raspberry Pi, AWS Graviton)
└── windows-amd64-dynamic.dll    (Windows x64)
```

Note: Windows ARM64 is not currently built (requires Windows SDK for cross-compilation).

All languages use **dynamic linking** - libraries auto-download on first use!

## Publishing

See [PUBLISHING.md](PUBLISHING.md) for complete guide.

**Short version:**
1. Build: `./scripts/build.sh`
2. Create GitHub release with tag (e.g., `v0.1.0`)
3. Upload all files from `builds/` to the release
4. Publish npm/PyPI packages (they auto-download from releases)

## Docker Container

Uses `rust:1.83` (official Rust Docker image) with cross-compilation tools for Windows and Linux ARM.

## GitHub Releases

Binaries are hosted on GitHub Releases:
```
https://github.com/sohzm/jasonisnthappy/releases/latest/download/<filename>
```

The `/latest/` URL automatically points to the newest release!
