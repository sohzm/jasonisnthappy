# jasonisnthappy Go Bindings

Go bindings for the jasonisnthappy embedded document database.

## Installation

```bash
# 1. Get the package
go get github.com/sohzm/jasonisnthappy/bindings/go

# 2. Download the native library for your platform
go generate github.com/sohzm/jasonisnthappy/bindings/go

# 3. Build your app
go build
```

That's it! Your binary is now **fully self-contained** with no external dependencies.

### How It Works

1. `go generate` downloads the pre-compiled static library (`.a` file) from GitHub releases
2. The library is cached in `lib/<platform>/` directory
3. CGO statically links it into your final binary
4. Result: **Single standalone binary** with zero runtime dependencies

**Benefits:**
- ✅ Single self-contained binary
- ✅ No external library files needed
- ✅ Perfect for production deployment
- ✅ Works in Docker containers, cloud functions, etc.
- ✅ No Rust compiler needed

## Quick Start

```go
package main

import (
    "fmt"
    "log"

    db "github.com/sohzm/jasonisnthappy/bindings/go"
)

func main() {
    // Open database
    database, err := db.Open("./my_database.db")
    if err != nil {
        log.Fatal(err)
    }
    defer database.Close()

    // Begin transaction
    tx, err := database.BeginTransaction()
    if err != nil {
        log.Fatal(err)
    }
    defer tx.Rollback() // Auto-rollback if not committed

    // Insert document
    doc := map[string]interface{}{
        "name": "Alice",
        "age":  30,
        "email": "alice@example.com",
    }
    id, err := tx.Insert("users", doc)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Inserted document with ID: %s\n", id)

    // Find by ID
    var result map[string]interface{}
    found, err := tx.FindByID("users", id, &result)
    if err != nil {
        log.Fatal(err)
    }
    if found {
        fmt.Printf("Found: %+v\n", result)
    }

    // Update
    doc["age"] = 31
    err = tx.UpdateByID("users", id, doc)
    if err != nil {
        log.Fatal(err)
    }

    // Find all
    var all []map[string]interface{}
    err = tx.FindAll("users", &all)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("All users: %+v\n", all)

    // Commit transaction
    err = tx.Commit()
    if err != nil {
        log.Fatal(err)
    }
}
```

## API Reference

### Database

```go
// Open a database
func Open(path string) (*Database, error)

// Close the database
func (d *Database) Close()

// Start a new transaction
func (d *Database) BeginTransaction() (*Transaction, error)
```

### Transaction

```go
// Insert a document into a collection
func (t *Transaction) Insert(collectionName string, doc interface{}) (string, error)

// Find document by ID
func (t *Transaction) FindByID(collectionName, id string, result interface{}) (bool, error)

// Update document by ID
func (t *Transaction) UpdateByID(collectionName, id string, doc interface{}) error

// Delete document by ID
func (t *Transaction) DeleteByID(collectionName, id string) error

// Find all documents in a collection
func (t *Transaction) FindAll(collectionName string, result interface{}) error

// Commit the transaction
func (t *Transaction) Commit() error

// Rollback the transaction (safe to call multiple times)
func (t *Transaction) Rollback()
```

## Requirements

- **CGO enabled** (default on most systems)
- Internet connection on first build (to download native library)

If you need to disable CGO, this package won't work as it requires native bindings.

## Platform Support

- **macOS**: ARM64 (Apple Silicon) and x64 (Intel)
- **Linux**: ARM64 and x64
- **Windows**: x64

Windows ARM64 is not currently supported.

## Building From Source

If you want to build the native libraries yourself:

```bash
# From repository root
./scripts/build.sh  # Builds both dynamic and static for all platforms

# For dynamic linking:
cp builds/darwin-arm64-dynamic.dylib bindings/go/lib/darwin-arm64/libjasonisnthappy.dylib

# For static linking:
cp builds/darwin-arm64-static.a bindings/go/lib/darwin-arm64/libjasonisnthappy.a
```

## Troubleshooting

### Library download fails
- Ensure you have internet connectivity
- Check that the GitHub release exists with the required files
- Verify your platform is supported

### Build fails with CGO errors
- Ensure you have a C compiler installed:
  - **macOS**: `xcode-select --install`
  - **Linux**: `apt install build-essential` or `yum install gcc`
  - **Windows**: Install MinGW-w64

### Static linking fails
- Make sure you ran `go generate` first
- Check that `.a` file exists: `ls bindings/go/lib/*/`
- Verify you have system dependencies installed

## License

Same as the main jasonisnthappy project.
