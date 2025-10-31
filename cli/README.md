# jasonisnthappy CLI

A powerful command-line interface for the jasonisnthappy document database.

## Installation

```bash
cd cli
cargo build --release
```

The binary will be available at `target/release/jasonisnthappy`.

You can also install it globally:

```bash
cargo install --path .
```

## Usage

### Command Mode

Execute single commands directly:

```bash
# Show database info
jasonisnthappy mydb.db db info

# Insert a document
jasonisnthappy mydb.db doc insert users '{"name": "Alice", "age": 30}'

# Find documents
jasonisnthappy mydb.db doc find users '{"age": {"$gt": 25}}'

# Create an index
jasonisnthappy mydb.db index create users email --unique

# Backup database
jasonisnthappy mydb.db db backup mydb_backup.db
```

### Interactive REPL Mode

Start an interactive shell:

```bash
jasonisnthappy mydb.db --interactive
# or simply
jasonisnthappy mydb.db
```

Then use commands interactively:

```
db> use users
✓ Now using collection 'users'

users> insert {"name": "Bob", "email": "bob@example.com"}
✓ Document inserted with ID: 1

users> find {"name": "Bob"}
[
  {
    "_id": 1,
    "name": "Bob",
    "email": "bob@example.com"
  }
]

users> count
{"count": 1}

users> exit
```

### Web UI Mode

Start the database with web UI enabled:

```bash
jasonisnthappy mydb.db --web-ui
```

Or specify a custom address:

```bash
jasonisnthappy mydb.db --web-ui --web-address 0.0.0.0:3000
```

## Commands Reference

### Database Commands

```bash
# Show database information
jasonisnthappy <db> db info

# Create backup
jasonisnthappy <db> db backup <destination>

# Compact database
jasonisnthappy <db> db compact

# List all collections
jasonisnthappy <db> db collections
```

### Collection Commands

```bash
# List collections
jasonisnthappy <db> collection list

# Create collection
jasonisnthappy <db> collection create <name>

# Drop collection
jasonisnthappy <db> collection drop <name>

# Show collection info
jasonisnthappy <db> collection info <name>
```

### Document Commands

```bash
# Insert document
jasonisnthappy <db> doc insert <collection> '{"field": "value"}'

# Find documents
jasonisnthappy <db> doc find <collection> '{"field": "value"}'

# Find with limit and skip
jasonisnthappy <db> doc find <collection> '{}' --limit 10 --skip 5

# Find one document
jasonisnthappy <db> doc find-one <collection> '{"_id": 1}'

# Update documents
jasonisnthappy <db> doc update <collection> '{"name": "old"}' '{"$set": {"name": "new"}}'

# Delete documents
jasonisnthappy <db> doc delete <collection> '{"status": "archived"}'

# Count documents
jasonisnthappy <db> doc count <collection> '{"active": true}'
```

### Query Commands

```bash
# Run query with filters and sorting
jasonisnthappy <db> query run <collection> \
  --filter '{"age": {"$gt": 18}}' \
  --sort age \
  --order desc \
  --limit 10

# Aggregation pipeline
jasonisnthappy <db> query aggregate <collection> '[
  {"$group": {"_id": "$category", "count": {"$sum": 1}}},
  {"$sort": {"count": -1}}
]'

# Full-text search
jasonisnthappy <db> query search <collection> "search terms" --limit 20
```

### Index Commands

```bash
# List indexes
jasonisnthappy <db> index list <collection>

# Create index
jasonisnthappy <db> index create <collection> <field>

# Create unique index
jasonisnthappy <db> index create <collection> <field> --unique

# Drop index
jasonisnthappy <db> index drop <collection> <index-name>
```

### Schema Commands

```bash
# Set schema
jasonisnthappy <db> schema set <collection> '{
  "fields": {
    "name": {"type": "string", "required": true},
    "age": {"type": "number"}
  }
}'

# Get schema
jasonisnthappy <db> schema get <collection>

# Validate all documents
jasonisnthappy <db> schema validate <collection>
```

### Monitoring Commands

```bash
# Show metrics
jasonisnthappy <db> metrics

# Watch for changes (real-time)
jasonisnthappy <db> watch <collection>
```

### Import/Export

```bash
# Export collection to JSON
jasonisnthappy <db> export <collection> output.json

# Import collection from JSON
jasonisnthappy <db> import <collection> input.json
```

## Output Formats

Control output format with the `--format` flag:

```bash
# JSON (compact)
jasonisnthappy mydb.db doc find users '{}' --format json

# Pretty JSON with colors (default)
jasonisnthappy mydb.db doc find users '{}' --format pretty

# Table format
jasonisnthappy mydb.db doc find users '{}' --format table
```

## Interactive Shell Commands

When in REPL mode:

| Command | Description |
|---------|-------------|
| `help` | Show available commands |
| `info` | Show database information |
| `collections` | List all collections |
| `metrics` | Show database metrics |
| `use <collection>` | Select a collection |
| `create <collection>` | Create a new collection |
| `drop` | Drop current collection |
| `insert {...}` | Insert document |
| `find [{...}]` | Find documents |
| `count [{...}]` | Count documents |
| `update {query} {update}` | Update documents |
| `delete {query}` | Delete documents |
| `indexes` | List indexes |
| `create-index <field>` | Create index |
| `backup <dest>` | Backup database |
| `export <file>` | Export collection |
| `import <file>` | Import collection |
| `exit` | Exit shell |

## Examples

### Creating and Populating a Database

```bash
# Start interactive mode
jasonisnthappy mydb.db

db> create users
✓ Collection 'users' created

db> use users

users> insert {"name": "Alice", "email": "alice@example.com", "age": 30}
✓ Document inserted with ID: 1

users> insert {"name": "Bob", "email": "bob@example.com", "age": 25}
✓ Document inserted with ID: 2

users> create-index email --unique
✓ Created unique index on field 'email' in collection 'users'

users> find
[
  {"_id": 1, "name": "Alice", "email": "alice@example.com", "age": 30},
  {"_id": 2, "name": "Bob", "email": "bob@example.com", "age": 25}
]
```

### Querying Data

```bash
# Find users older than 25
jasonisnthappy mydb.db query run users --filter '{"age": {"$gt": 25}}'

# Search by text
jasonisnthappy mydb.db query search users "alice"

# Aggregate by age
jasonisnthappy mydb.db query aggregate users '[
  {"$group": {"_id": "$age", "count": {"$sum": 1}}}
]'
```

### Backup and Export

```bash
# Create full backup
jasonisnthappy mydb.db db backup mydb_backup.db

# Export specific collection
jasonisnthappy mydb.db export users users_backup.json

# Import into new database
jasonisnthappy newdb.db import users users_backup.json
```

### Real-time Monitoring

```bash
# In one terminal, watch for changes
jasonisnthappy mydb.db watch users

# In another terminal, make changes
jasonisnthappy mydb.db doc insert users '{"name": "Charlie"}'

# First terminal will show:
# {
#   "operation": "Insert",
#   "document_id": 3,
#   "document": {"_id": 3, "name": "Charlie"},
#   "timestamp": 1234567890
# }
```

## Advanced Usage

### Schema Validation

```bash
# Set schema with validation rules
jasonisnthappy mydb.db schema set users '{
  "fields": {
    "name": {"type": "string", "required": true},
    "email": {"type": "string", "required": true},
    "age": {"type": "number", "minimum": 0, "maximum": 150}
  }
}'

# Validate all existing documents
jasonisnthappy mydb.db schema validate users
```

### Batch Operations

```bash
# Create JSON file with multiple documents
cat > users.json << 'EOF'
[
  {"name": "Alice", "age": 30},
  {"name": "Bob", "age": 25},
  {"name": "Charlie", "age": 35}
]
EOF

# Import in batch
jasonisnthappy mydb.db import users users.json
```

### Web UI with CLI

```bash
# Start with web UI
jasonisnthappy mydb.db --web-ui

# Open http://127.0.0.1:8080 in browser
# Use web UI for visual exploration
# Use CLI for scripting and automation
```

## Tips

1. **Use REPL for exploration**: Interactive mode is great for exploring data and testing queries
2. **Use command mode for automation**: Script repetitive tasks with direct commands
3. **Export before major changes**: Always backup or export before dropping collections or major updates
4. **Watch for debugging**: Use `watch` to see real-time changes during development
5. **Table format for readability**: Use `--format table` when viewing small result sets
6. **JSON format for piping**: Use `--format json` when piping output to other tools

## Troubleshooting

### Command not found after install

Make sure `~/.cargo/bin` is in your PATH:

```bash
export PATH="$HOME/.cargo/bin:$PATH"
```

### Web UI feature disabled

Rebuild with web UI feature:

```bash
cargo build --release --features web-ui
```

### Permission denied

Ensure you have write permissions to the database file and directory.

## License

MIT
