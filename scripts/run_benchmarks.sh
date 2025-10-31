#!/bin/bash
#
# Comprehensive Benchmark Suite Runner
# Runs all benchmarks, organizes results, and generates reports
#
# Usage: ./scripts/run_benchmarks.sh

set -e

echo "========================================="
echo "  jasonisnthappy Benchmark Suite"
echo "========================================="
echo

# Create results directory with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="benchmark_results/${TIMESTAMP}"
mkdir -p "$RESULTS_DIR"

echo "Results will be saved to: $RESULTS_DIR"
echo

# Run main benchmarks
echo "--- Running Main Benchmarks ---"
cargo bench --bench database_benchmarks -- --save-baseline latest \
    2>&1 | tee "$RESULTS_DIR/benchmarks.log"

# Copy Criterion HTML reports
if [ -d "target/criterion" ]; then
    echo
    echo "--- Copying Criterion Reports ---"
    cp -r target/criterion "$RESULTS_DIR/"
    echo "HTML reports copied to $RESULTS_DIR/criterion/"
fi

# Run additional benchmarks if they exist
if cargo bench --bench=* 2>/dev/null | grep -q "bench"; then
    echo
    echo "--- Running Additional Benchmarks ---"
    cargo bench 2>&1 | tee -a "$RESULTS_DIR/all_benchmarks.log"
fi

# Generate summary
echo
echo "--- Generating Summary ---"

# Extract key metrics from the log
echo "Benchmark Summary" > "$RESULTS_DIR/SUMMARY.txt"
echo "================" >> "$RESULTS_DIR/SUMMARY.txt"
echo "Date: $(date)" >> "$RESULTS_DIR/SUMMARY.txt"
echo "Commit: $(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')" >> "$RESULTS_DIR/SUMMARY.txt"
echo >> "$RESULTS_DIR/SUMMARY.txt"

# Extract benchmark results (simplified - adjust based on actual output format)
grep -E "(time:|thrpt:)" "$RESULTS_DIR/benchmarks.log" >> "$RESULTS_DIR/SUMMARY.txt" 2>/dev/null || true

echo
echo "========================================="
echo "  Benchmark Suite Complete!"
echo "========================================="
echo
echo "Results location: $RESULTS_DIR"
echo
echo "View HTML reports:"
echo "  open $RESULTS_DIR/criterion/report/index.html"
echo
echo "Quick analysis:"
cat "$RESULTS_DIR/SUMMARY.txt"
