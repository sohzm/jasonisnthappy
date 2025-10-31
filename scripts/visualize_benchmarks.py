#!/usr/bin/env python3
"""
Benchmark Visualization Tool
Generates text-based charts from Criterion benchmark results

Usage: python3 scripts/visualize_benchmarks.py [results_dir]
"""

import sys
import json
import os
from pathlib import Path
from typing import Dict, List, Tuple

def load_criterion_results(criterion_dir: Path) -> Dict:
    """Load all Criterion benchmark results"""
    results = {}

    if not criterion_dir.exists():
        print(f"Error: Directory not found: {criterion_dir}")
        return results

    # Find all estimates.json files
    for estimates_file in criterion_dir.rglob("new/estimates.json"):
        bench_name = estimates_file.parent.parent.name

        try:
            with open(estimates_file) as f:
                data = json.load(f)

            # Extract mean time in nanoseconds
            mean_ns = data.get("mean", {}).get("point_estimate", 0)
            mean_ms = mean_ns / 1_000_000  # Convert to milliseconds

            results[bench_name] = {
                "mean_ms": mean_ms,
                "mean_ns": mean_ns,
            }
        except Exception as e:
            print(f"Warning: Could not parse {estimates_file}: {e}")

    return results

def create_text_bar_chart(data: Dict[str, float], title: str, unit: str = "ms"):
    """Create a text-based bar chart"""
    if not data:
        print(f"\nNo data available for: {title}")
        return

    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)

    # Find max value for scaling
    max_val = max(data.values()) if data else 1
    max_name_len = max(len(name) for name in data.keys()) if data else 0

    # Sort by value
    sorted_items = sorted(data.items(), key=lambda x: x[1], reverse=True)

    for name, value in sorted_items:
        # Scale bar to fit in 40 characters
        bar_len = int((value / max_val) * 40) if max_val > 0 else 0
        bar = '█' * bar_len

        print(f"{name:<{max_name_len}} │ {bar} {value:.2f} {unit}")

def analyze_throughput(results: Dict) -> Dict[str, float]:
    """Calculate throughput (ops/sec) from mean times"""
    throughput = {}

    for name, data in results.items():
        mean_ns = data["mean_ns"]
        if mean_ns > 0:
            ops_per_sec = 1_000_000_000 / mean_ns
            throughput[name] = ops_per_sec

    return throughput

def analyze_by_category(results: Dict) -> Dict[str, Dict]:
    """Group results by benchmark category"""
    categories = {}

    for name, data in results.items():
        # Try to extract category from name
        if "/" in name:
            category = name.split("/")[0]
        elif "_" in name:
            category = name.split("_")[0]
        else:
            category = "other"

        if category not in categories:
            categories[category] = {}

        categories[category][name] = data["mean_ms"]

    return categories

def print_summary_table(results: Dict):
    """Print a summary table of all benchmarks"""
    print(f"\n{'='*80}")
    print(f"  Benchmark Summary")
    print('='*80)
    print(f"{'Benchmark':<40} {'Mean Time':<15} {'Throughput':<20}")
    print('-'*80)

    sorted_results = sorted(results.items(), key=lambda x: x[1]["mean_ms"])

    for name, data in sorted_results:
        mean_ms = data["mean_ms"]
        ops_per_sec = 1_000_000_000 / data["mean_ns"] if data["mean_ns"] > 0 else 0

        # Format times appropriately
        if mean_ms < 1:
            time_str = f"{mean_ms*1000:.2f} µs"
        else:
            time_str = f"{mean_ms:.2f} ms"

        throughput_str = f"{ops_per_sec:,.0f} ops/sec"

        print(f"{name:<40} {time_str:<15} {throughput_str:<20}")

def main():
    # Get results directory from command line or use default
    if len(sys.argv) > 1:
        results_dir = Path(sys.argv[1])
    else:
        # Find most recent results directory
        results_base = Path("benchmark_results")
        if results_base.exists():
            subdirs = [d for d in results_base.iterdir() if d.is_dir()]
            if subdirs:
                results_dir = max(subdirs, key=os.path.getmtime)
            else:
                results_dir = Path("target/criterion")
        else:
            results_dir = Path("target/criterion")

    criterion_dir = results_dir / "criterion" if (results_dir / "criterion").exists() else results_dir

    print(f"Loading benchmark results from: {criterion_dir}")

    # Load results
    results = load_criterion_results(criterion_dir)

    if not results:
        print("\nNo benchmark results found!")
        print(f"Run benchmarks first with: cargo bench")
        sys.exit(1)

    print(f"\nFound {len(results)} benchmark(s)")

    # Generate visualizations
    print_summary_table(results)

    # Throughput chart
    throughput = analyze_throughput(results)
    create_text_bar_chart(throughput, "Throughput by Benchmark", "ops/sec")

    # Mean time chart
    mean_times = {name: data["mean_ms"] for name, data in results.items()}
    create_text_bar_chart(mean_times, "Mean Time by Benchmark", "ms")

    # Category analysis
    categories = analyze_by_category(results)
    for category, benches in categories.items():
        create_text_bar_chart(benches, f"Benchmarks: {category}", "ms")

    # Top 5 fastest/slowest
    sorted_by_speed = sorted(results.items(), key=lambda x: x[1]["mean_ms"])

    print(f"\n{'='*60}")
    print("  Top 5 Fastest Benchmarks")
    print('='*60)
    for name, data in sorted_by_speed[:5]:
        print(f"  {name}: {data['mean_ms']:.3f} ms")

    print(f"\n{'='*60}")
    print("  Top 5 Slowest Benchmarks")
    print('='*60)
    for name, data in reversed(sorted_by_speed[-5:]):
        print(f"  {name}: {data['mean_ms']:.3f} ms")

    print(f"\n{'='*60}")
    print("✓ Visualization complete!")
    print('='*60)

if __name__ == "__main__":
    main()
