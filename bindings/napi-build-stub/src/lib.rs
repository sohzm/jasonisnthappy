// Stub implementation of napi-build for cross-compilation to Windows
// This avoids the libnode.dll check when cross-compiling from Linux/macOS to Windows

pub fn setup() {
  let target = std::env::var("TARGET").unwrap_or_default();
  let host = std::env::var("HOST").unwrap_or_default();

  // Check if we're cross-compiling to Windows from a non-Windows host
  let is_cross_compiling_to_windows = target.contains("windows") && !host.contains("windows");

  if is_cross_compiling_to_windows {
    // Emit minimal linker flags for Windows DLL cross-compilation
    // The actual Node.js linking happens at runtime when Node.js loads the .node file
    println!("cargo:rustc-cdylib-link-arg=-Wl,--export-all-symbols");
    println!("cargo::rustc-check-cfg=cfg(tokio_unstable)");
  } else {
    // For native builds, we can't use the real napi-build because it's been patched
    // So we emit the same config that napi-build would emit for non-Windows targets
    println!("cargo::rustc-check-cfg=cfg(tokio_unstable)");
  }
}
