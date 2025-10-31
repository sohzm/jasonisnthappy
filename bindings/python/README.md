# jasonisnthappy

Python bindings for the jasonisnthappy embedded database.

## Installation

```bash
pip install jasonisnthappy
```

**Important:** You need to download the pre-built dynamic libraries for your platform from the [releases page](https://github.com/sohzm/jasonisnthappy/releases) and place them in the appropriate `lib/` directory.

## Quick Start

```python
from jasonisnthappy import Database

# Open database
db = Database.open("./my_database.db")

try:
    # Begin transaction
    tx = db.begin_transaction()

    try:
        # Get collection
        users = tx.collection("users")

        # Insert document
        doc_id = users.insert({
            "name": "Alice",
            "age": 30,
            "email": "alice@example.com"
        })
        print(f"Inserted document with ID: {doc_id}")

        # Find by ID
        user = users.find_by_id(doc_id)
        print(f"Found: {user}")

        # Update
        users.update(doc_id, {
            "name": "Alice",
            "age": 31,
            "email": "alice@example.com"
        })

        # Query
        results = users.query("age > 25")
        print(f"Query results: {results}")

        # Delete
        users.delete(doc_id)

        # Commit transaction
        tx.commit()
    except Exception as e:
        # Rollback on error
        tx.rollback()
        raise
finally:
    db.close()
```

## Context Managers

The library supports Python context managers for automatic resource cleanup:

```python
from jasonisnthappy import Database

with Database.open("./my_database.db") as db:
    with db.begin_transaction() as tx:
        users = tx.collection("users")

        doc_id = users.insert({"name": "Alice", "age": 30})
        user = users.find_by_id(doc_id)
        print(user)

        # Transaction is automatically committed if no exception occurs
        # or rolled back if an exception is raised
```

## Type Hints

The library includes full type hints:

```python
from typing import Dict, Any, List
from jasonisnthappy import Database, Transaction, Collection

db: Database = Database.open("./my_database.db")
tx: Transaction = db.begin_transaction()
users: Collection = tx.collection("users")

doc: Dict[str, Any] = {"name": "Alice", "age": 30}
doc_id: int = users.insert(doc)

result: Dict[str, Any] | None = users.find_by_id(doc_id)
results: List[Dict[str, Any]] = users.query("age > 25")
```

## API Reference

### Database

- `Database.open(path: str) -> Database` - Opens a database
- `close() -> None` - Closes the database
- `begin_transaction() -> Transaction` - Begins a new transaction

### Transaction

- `commit() -> None` - Commits the transaction
- `rollback() -> None` - Rolls back the transaction
- `collection(name: str) -> Collection` - Gets or creates a collection

### Collection

- `insert(doc: Dict[str, Any]) -> int` - Inserts a document, returns ID
- `find_by_id(doc_id: int) -> Dict[str, Any] | None` - Finds a document by ID
- `update(doc_id: int, doc: Dict[str, Any]) -> None` - Updates a document
- `delete(doc_id: int) -> None` - Deletes a document
- `query(query_str: str) -> List[Dict[str, Any]]` - Executes a query

## Platform Support

Pre-built binaries are provided for:
- macOS (Intel and Apple Silicon)
- Linux (x86_64)
- Windows (x86_64)

## Building

To build the dynamic libraries yourself:

```bash
# From repository root
cd bindings/ffi
cargo build --release

# Copy library to Python bindings
# macOS:
cp target/release/libjasonisnthappy.dylib ../python/jasonisnthappy/lib/darwin-arm64/

# Linux:
cp target/release/libjasonisnthappy.so ../python/jasonisnthappy/lib/linux-amd64/

# Windows:
cp target/release/jasonisnthappy.dll ../python/jasonisnthappy/lib/windows-amd64/
```

## Development

To install in development mode:

```bash
cd bindings/python
pip install -e .
```

## License

Same as the main jasonisnthappy project.
