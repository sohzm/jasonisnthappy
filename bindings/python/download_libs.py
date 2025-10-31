#!/usr/bin/env python3
"""
Auto-download script for jasonisnthappy native libraries
Downloads the correct platform binary from GitHub releases during pip install
"""

import os
import platform
import sys
import urllib.request
from pathlib import Path

RELEASE_URL = "https://github.com/sohzm/jasonisnthappy/releases/latest/download"


def get_platform_info():
    """Detect platform and return library information."""
    system = platform.system()
    machine = platform.machine().lower()

    if system == "Darwin":  # macOS
        if machine == "arm64":
            return {
                "dir": "darwin-arm64",
                "file": "darwin-arm64-dynamic.dylib",
                "dest": "libjasonisnthappy.dylib",
            }
        elif machine in ("x86_64", "amd64"):
            return {
                "dir": "darwin-amd64",
                "file": "darwin-amd64-dynamic.dylib",
                "dest": "libjasonisnthappy.dylib",
            }
        else:
            raise RuntimeError(f"Unsupported macOS architecture: {machine}")
    elif system == "Linux":
        if machine in ("aarch64", "arm64"):
            return {
                "dir": "linux-arm64",
                "file": "linux-arm64-dynamic.so",
                "dest": "libjasonisnthappy.so",
            }
        elif machine in ("x86_64", "amd64"):
            return {
                "dir": "linux-amd64",
                "file": "linux-amd64-dynamic.so",
                "dest": "libjasonisnthappy.so",
            }
        else:
            raise RuntimeError(f"Unsupported Linux architecture: {machine}")
    elif system == "Windows":
        if machine in ("arm64", "aarch64"):
            raise RuntimeError(
                "Windows ARM64 is not currently supported. "
                "Pre-built binaries are only available for Windows x64. "
                "Please use Windows x64 or build from source."
            )
        elif machine in ("x86_64", "amd64"):
            return {
                "dir": "windows-amd64",
                "file": "windows-amd64-dynamic.dll",
                "dest": "jasonisnthappy.dll",
            }
        else:
            raise RuntimeError(f"Unsupported Windows architecture: {machine}")
    else:
        raise RuntimeError(f"Unsupported platform: {system}")


def download_file(url, dest_path):
    """Download file from URL to destination path with progress."""
    print(f"Downloading from {url}...", file=sys.stderr)

    def reporthook(count, block_size, total_size):
        if total_size > 0:
            percent = min(int(count * block_size * 100 / total_size), 100)
            sys.stderr.write(f"\rProgress: {percent}%")
            sys.stderr.flush()

    try:
        urllib.request.urlretrieve(url, dest_path, reporthook=reporthook)
        sys.stderr.write("\n")
        sys.stderr.flush()
    except Exception as e:
        if dest_path.exists():
            dest_path.unlink()
        raise


def get_install_dir(platform_info):
    """Find where to install the library."""
    # First, try to find installed package in site-packages
    try:
        import jasonisnthappy as pkg
        pkg_dir = Path(pkg.__file__).parent / "lib" / platform_info["dir"]
        return pkg_dir
    except ImportError:
        pass

    # Fall back to source directory (for pre-install or editable installs)
    return Path(__file__).parent / "jasonisnthappy" / "lib" / platform_info["dir"]


def main():
    try:
        platform_info = get_platform_info()

        # Find install directory (site-packages or source)
        package_dir = get_install_dir(platform_info)
        package_dir.mkdir(parents=True, exist_ok=True)
        dest_path = package_dir / platform_info["dest"]

        # Skip if already exists
        if dest_path.exists():
            print(f"✓ Library already exists at {dest_path}", file=sys.stderr)
            return

        url = f"{RELEASE_URL}/{platform_info['file']}"
        print(
            f"Downloading jasonisnthappy native library for {platform.system()}-{platform.machine()}...",
            file=sys.stderr,
        )

        download_file(url, dest_path)

        print(f"✓ Successfully downloaded to {dest_path}", file=sys.stderr)

    except Exception as error:
        print(f"✗ Failed to download native library: {error}", file=sys.stderr)
        print("\nYou can manually download the library from:", file=sys.stderr)
        print("https://github.com/sohzm/jasonisnthappy/releases/latest", file=sys.stderr)
        print("And place it in the jasonisnthappy/lib/<platform>/ directory.\n", file=sys.stderr)
        # Don't fail the install, fallback to runtime download
        sys.exit(0)


if __name__ == "__main__":
    main()
