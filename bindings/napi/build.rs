fn main() {
  // Use our patched napi-build which handles cross-compilation to Windows gracefully
  napi_build::setup();
}
