# LocalDB

A lightweight, multi-threaded, in-memory relational database implementation in C++.

## Features

- In-memory relational database with table support
- Disk persistence (save to and load from files)
- ACID transactions
- Multi-threading support with reader-writer locks
- Basic SQL-like operations: create, read, update, delete
- Data types: INTEGER, FLOAT, TEXT, BLOB
- Constraints: PRIMARY KEY, NOT NULL, UNIQUE
- Command-line interface (CLI) for interactive use

## Building

```bash
mkdir -p build
cd build
cmake ..
make
```

## Running

```bash
# Run the demo application
./bin/localdb

# Run the CLI
./bin/localdb_cli
```

## Command-Line Interface

LocalDB includes a command-line interface for interactive database operations:

```bash
# Start with an empty database
./bin/localdb_cli

# Load an existing database file
./bin/localdb_cli --load my_database.bin
```

### Available Commands

| Command | Description | Example |
|---------|-------------|---------|
| `help` | Display help information | `help`, `help create_table` |
| `create_table` | Create a new table | `create_table users id:INT:PK:NN:UQ name:TEXT:0:NN:0 age:INT` |
| `drop_table` | Drop a table | `drop_table users` |
| `list_tables` | List all tables | `list_tables` |
| `describe_table` | Show table schema | `describe_table users` |
| `insert` | Insert a row | `insert users 1 "John Doe" 30` |
| `select` | Query data | `select users`, `select users WHERE 0 = 1` |
| `delete` | Delete rows | `delete users WHERE 0 = 1` |
| `begin` | Begin a transaction | `begin` |
| `commit` | Commit a transaction | `commit` |
| `rollback` | Rollback a transaction | `rollback` |
| `save` | Save database to file | `save my_database.bin` |
| `load` | Load database from file | `load my_database.bin` |
| `exit`, `quit` | Exit the program | `exit` |

#### Column Definition Format

When creating tables, columns are defined using the format: `name:type:primary_key:not_null:unique`

- `name`: Column name
- `type`: INT, FLOAT, TEXT, BLOB
- `primary_key`: 1, true, PK (or 0, false to disable)
- `not_null`: 1, true, NN (or 0, false to disable)
- `unique`: 1, true, UQ (or 0, false to disable)

Example: `id:INT:PK:NN:UQ` creates an integer column named "id" that is a primary key, not null, and unique.

## Testing

The project uses Google Test for unit testing. To build and run the tests:

```bash
mkdir -p build
cd build
cmake ..
make
./bin/localdb_test
```

The tests cover:
- Value class (data storage)
- Table operations (insert, update, select, delete)
- Database management (create/drop tables)
- Transactions (commit, rollback)
- Multi-threading support
- Disk persistence (save/load)

## Implementation Details

- Uses C++17 features for modern, clean code
- Employs `std::shared_mutex` for concurrent read/exclusive write operations
- Uses RAII principles for resource management
- Implements a simple transaction system with rollback capability
- Thread-safe operations via mutex locks
- Binary serialization for disk persistence

## API Usage Example

```cpp
#include "localdb.h"

// Create a database
localdb::Database db;

// Define columns for a table
std::vector<localdb::Column> columns = {
    {"id", localdb::Column::INT, true, true, true},  // Primary key
    {"name", localdb::Column::TEXT, false, true, false},
    {"age", localdb::Column::INT, false, false, false}
};

// Create a table
db.createTable("users", columns);

// Begin a transaction
auto transaction = db.beginTransaction();

// Insert a row
localdb::Row row;
row.push_back(localdb::Value(1));  // ID
row.push_back(localdb::Value(std::string("John Doe")));  // Name
row.push_back(localdb::Value(30));  // Age

transaction->insert("users", row);

// Query data
auto results = transaction->select("users", [](const localdb::Row& row) {
    return row[0].asInt() == 1;  // Where ID = 1
});

// Commit the transaction
transaction->commit();

// Save database to file
db.saveToFile("my_database.bin");

// Load database from file
localdb::Database loadedDb;
loadedDb.loadFromFile("my_database.bin");
```

## Disk Persistence Example

```cpp
// Save database to disk
db.saveToFile("database.bin");

// Load database from disk
localdb::Database loadedDb;
if (loadedDb.loadFromFile("database.bin")) {
    // Database loaded successfully
    auto tables = loadedDb.getTableNames();
    
    // Make changes to the loaded database
    auto tx = loadedDb.beginTransaction();
    // ... perform operations
    tx->commit();
    
    // Save changes back to disk
    loadedDb.saveToFile("database.bin");
}
```

## License

MIT 