# Running Tests

## Rust Core Tests

```bash
# Unit and integration tests
cargo test --lib
cargo test --tests

# Stress tests
cargo test --test stress_tests -- --nocapture

# Regression tests
cargo test --test regression -- --nocapture
```

## Language Bindings Tests

First, build the FFI library:
```bash
cd bindings/ffi && cargo build --release && cd ../..
```

Or use the script to build and copy to all bindings:
```bash
./scripts/prepare-bindings.sh
```

### Go
```bash
cd bindings/go/tests
go test -v
```

### JavaScript/Node.js
```bash
cd bindings/napi
npm install
cd tests
npx jest integration.test.js --verbose
```

### Python
```bash
cd bindings/python
pip install -e .
cd tests
python -m pytest test_integration.py -v
```

## Run All Tests

```bash
./scripts/run-all-tests.sh
```
