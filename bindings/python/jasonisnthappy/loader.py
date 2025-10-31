"""
Loader for jasonisnthappy native library.
The library should be downloaded during pip install.
"""

import os
import platform
from pathlib import Path
from typing import Optional

_lib_path: Optional[str] = None


def _get_platform_info():
    """Detect platform and return library information."""
    system = platform.system()
    machine = platform.machine().lower()

    if system == "Darwin":  # macOS
        if machine == "arm64":
            return {"dir": "darwin-arm64", "dest": "libjasonisnthappy.dylib"}
        elif machine in ("x86_64", "amd64"):
            return {"dir": "darwin-amd64", "dest": "libjasonisnthappy.dylib"}
    elif system == "Linux":
        if machine in ("aarch64", "arm64"):
            return {"dir": "linux-arm64", "dest": "libjasonisnthappy.so"}
        elif machine in ("x86_64", "amd64"):
            return {"dir": "linux-amd64", "dest": "libjasonisnthappy.so"}
    elif system == "Windows":
        if machine in ("x86_64", "amd64"):
            return {"dir": "windows-amd64", "dest": "jasonisnthappy.dll"}

    return None


def get_library_path() -> str:
    """Get the path to the native library. Raises if not found."""
    global _lib_path

    if _lib_path is not None:
        return _lib_path

    platform_info = _get_platform_info()
    if platform_info is None:
        raise RuntimeError(
            f"Unsupported platform: {platform.system()}/{platform.machine()}\n"
            f"Supported platforms: macOS (x64, arm64), Linux (x64, arm64), Windows (x64)"
        )

    # Check in package lib directory
    lib_path = Path(__file__).parent / "lib" / platform_info["dir"] / platform_info["dest"]

    if not lib_path.exists():
        raise RuntimeError(
            f"Native library not found at {lib_path}\n\n"
            f"The library should have been downloaded during 'pip install'.\n"
            f"Try reinstalling: pip uninstall jasonisnthappy && pip install jasonisnthappy\n\n"
            f"Or manually download from:\n"
            f"https://github.com/sohzm/jasonisnthappy/releases/latest"
        )

    _lib_path = str(lib_path)
    return _lib_path
