<img src="https://jasonisnthappy.soham.sh/uwu/main.png" alt="jasonisnthappy" />

<br/>

A lightweight, embedded document database written in Rust with ACID transactions and MVCC support.

## Features

- **Pure Rust** — no `unsafe` code in the core library
- **Cross-Platform** — macOS, Linux, and Windows
- **ACID Transactions** — full commit/rollback with conflict detection
- **MVCC** — snapshot isolation; reads never block writes
- **Document Storage** — JSON documents with automatic ID generation and upsert
- **B-tree Storage Engine** — copy-on-write with single-field, compound, and unique indexes
- **Write-Ahead Logging (WAL)** — checksummed for crash recovery and durability
- **Full-text Search** — TF-IDF scoring with Unicode tokenization
- **Query Language** — logical, comparison, and membership operators with dot notation
- **Aggregation Pipeline** — group, match, sort, project, and accumulator stages
- **Schema Validation** — JSON Schema enforcement on documents
- **Change Streams** — real-time notifications on insert, update, and delete
- **Bulk Operations** — high-throughput batch inserts and writes
- **QueryBuilder** — fluent API with sorting, pagination, and projections
- **Backup & Restore** — point-in-time snapshots with file locking
- **Garbage Collection** — reclaim space from old MVCC versions
- **Read-only Mode** — safe concurrent access
- **Web UI & REST API** — built-in dashboard and CRUD endpoints
- **CLI** — interactive REPL and command-line access
- **Metrics** — track transactions, cache, WAL, and errors
- **Configurable** — tunable cache, checkpoints, permissions, and limits
- **LRU Page Cache** — in-memory caching with corruption detection
- **Language Bindings** — Go, Python, and JavaScript (Node/Deno/Bun) via C FFI

## Quick Start

```rust
use jasonisnthappy::core::database::Database;
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Database::open("mydb.db")?;

    let mut tx = db.begin()?;
    let mut users = tx.collection("users");

    let user = json!({"name": "Alice", "age": 30});
    let id = users.insert(user)?;

    tx.commit()?;
    db.close()?;

    Ok(())
}
```

## Performance Benchmarks

All benchmarks run on a single-threaded workload with ACID guarantees and MVCC enabled.

### Write Performance (100 documents per transaction)

| Benchmark | Concurrency | Average | Min | Max |
|-----------|-------------|---------|-----|-----|
| WriteOnly | 1 thread | 8.45ms | 6.31ms | 10.33ms |
| WriteOnly | 4 threads | 2.28ms | 2.28ms | 2.28ms |
| WriteOnly | 16 threads | 1.48ms | 1.48ms | 1.48ms |

*Measures sustained write throughput with full durability (WAL + fsync)*

### Read Performance (1500 documents)

| Benchmark | Concurrency | Average | Min | Max |
|-----------|-------------|---------|-----|-----|
| Read | 1 thread | 0.026ms | 0.014ms | 0.067ms |
| Read | 4 threads | 0.017ms | 0.017ms | 0.017ms |
| Read | 16 threads | 0.009ms | 0.009ms | 0.009ms |

*Measures concurrent read performance with MVCC snapshot isolation*

### Bulk Insert Performance (per transaction)

| Documents | Average | Min | Max | Throughput |
|-----------|---------|-----|-----|------------|
| 1 | 8.11ms | 5.98ms | 10.39ms | ~123 docs/sec |
| 10 | 9.16ms | 7.95ms | 10.01ms | ~1,092 docs/sec |
| 50 | 12.97ms | 9.50ms | 16.07ms | ~3,855 docs/sec |
| 100 | 18.02ms | 12.92ms | 61.56ms | ~5,549 docs/sec |
| 500 | 34.65ms | 21.98ms | 54.88ms | ~14,430 docs/sec |
| 1000 | 52.21ms | 33.15ms | 56.18ms | ~19,153 docs/sec |

*Bulk inserts show significant throughput improvements due to B-tree batching*

### Query Performance

| Operation | Dataset Size | Average | Min | Max |
|-----------|-------------|---------|-----|-----|
| Find | 100 docs | 0.020ms | 0.016ms | 0.052ms |
| Find | 1500 docs | 0.023ms | 0.013ms | 0.218ms |
| Find | 2500 docs | 0.029ms | 0.018ms | 0.093ms |

*Query performance remains consistently fast even with larger datasets*

### Update Performance

| Operation | Average | Min | Max |
|-----------|---------|-----|-----|
| Update | 7.90ms | 6.08ms | 10.68ms |

*Updates include MVCC versioning and WAL logging for full ACID compliance*

### Key Performance Characteristics

- **ACID Compliant**: All operations include full transaction support with durability guarantees
- **MVCC Overhead**: Minimal - reads don't block writes, writes don't block reads
- **Bulk Efficiency**: ~19,150 documents/sec for bulk inserts (1000 docs/tx)
- **Query Speed**: Sub-millisecond queries even on 2500+ document collections
- **Concurrent Writes**: Linear scaling with thread count up to core count
- **Single Insert**: ~8ms with full fsync durability (WAL + DB file)

## Running Tests

```bash
# Unit and integration tests
cargo test --lib
cargo test --tests

# Stress tests
cargo test --test stress_tests -- --nocapture

# Regression tests
cargo test --test regression -- --nocapture

# Debug tests
cargo test --test debug -- --nocapture
```

## Running Benchmarks

```bash
cargo run --release --example bench_all
```

## Language Bindings

jasonisnthappy can be used from multiple languages:

- **Rust**: Use the crate directly (this repo)
- **Go**: [bindings/go/](bindings/go/)
- **JavaScript** (Node/Deno/Bun): [bindings/napi/](bindings/napi/)
- **Python**: [bindings/python/](bindings/python/)

All bindings use a shared C FFI layer. See [bindings/README.md](bindings/README.md) for details.

### Building Bindings

```bash
# Build for all platforms (requires Docker)
./scripts/build.sh

# Or just macOS for quick testing
./scripts/build-simple.sh
```

Outputs go to `builds/` directory ready for GitHub releases.

See [bindings/PUBLISHING.md](bindings/PUBLISHING.md) for publishing guide.

## Star History

<a href="https://www.star-history.com/#sohzm/jasonisnthappy&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=sohzm/jasonisnthappy&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=sohzm/jasonisnthappy&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=sohzm/jasonisnthappy&type=date&legend=top-left" />
 </picture>
</a>

## License

MIT
