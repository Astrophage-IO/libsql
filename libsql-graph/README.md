# libsql-graph

Native graph database engine for [libSQL](https://github.com/tursodatabase/libsql). Supports Cypher queries, ACID transactions, write-ahead logging, and a cost-based query optimizer — all as a single Rust crate with zero external dependencies.

## Quick Start

```rust
use libsql_graph::prelude::*;

let mut engine = GraphEngine::create("my.db", 4096).unwrap();

// Create nodes via Cypher
engine.query("CREATE (a:Person {name: 'Alice', age: 30})").unwrap();
engine.query("CREATE (b:Person {name: 'Bob', age: 25})").unwrap();

// Create relationships
engine.create_relationship(0, 1, "KNOWS").unwrap();

// Query
let result = engine.query(
    "MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN b.name, b.age"
).unwrap();

for row in &result.rows {
    println!("{} (age {})", row[0], row[1]);
}
```

## Features

### Storage
- Fixed-size records: 64B nodes, 64B relationships, 64B properties, 32B tokens
- Index-free adjacency via doubly-linked relationship chains
- String overflow for values exceeding inline capacity
- Dense node (supernode) support with relationship group chains
- Label index bitmap scans

### Cypher
- Full query pipeline: lexer, recursive descent parser, planner, optimizer, executor
- `CREATE`, `MATCH`, `WHERE`, `RETURN`, `DELETE`, `SET`, `MERGE`, `WITH`, `OPTIONAL MATCH`, `UNWIND`
- `ORDER BY`, `LIMIT`, `SKIP`, `DISTINCT`, `CASE WHEN`
- Aggregations: `count`, `avg`, `sum`, `min`, `max`
- String predicates: `STARTS WITH`, `ENDS WITH`, `CONTAINS`, regex (`=~`)
- Parameterized queries
- 30+ built-in functions

### Transactions
- Explicit `begin()` / `commit()` / `rollback()`
- Auto-commit when no explicit transaction is active
- Atomic `TransactionBatch` for multi-statement execution

### Write-Ahead Log
- CRC32-checksummed frames with chain validation
- Crash recovery: committed transactions replayed, incomplete discarded
- Auto-checkpoint at configurable threshold
- Transparent to callers

### Query Optimizer
- 3-pass optimization: indexed label scan rewrite, predicate pushdown, join reorder
- Incremental statistics (per-label, per-relationship-type counts)
- Cost model with cardinality estimation
- `EXPLAIN` and `PROFILE` support

### Pager Abstraction
- Generic `Pager` trait — swap storage backends
- `FilePager`: disk-backed with WAL
- `MemPager`: in-memory for testing

## CLI Tool

```bash
cargo build -p libsql-graph --bin graph_cli

# Single query
./target/debug/graph_cli my.db "MATCH (p:Person) RETURN p.name"

# REPL mode (pipe from stdin)
echo 'CREATE (a:Person {name: "Alice"})' | ./target/debug/graph_cli my.db
```

Output is JSON:
```json
{"columns":["p.name"],"rows":[["Alice"],["Bob"]],"stats":{"nodes_created":0,"relationships_created":0,"properties_set":0,"nodes_deleted":0}}
```

## Python Integration

```python
import subprocess, json

def query(db, cypher):
    r = subprocess.run(["graph_cli", db, cypher], capture_output=True, text=True)
    return json.loads(r.stdout)

query("my.db", "CREATE (a:Person {name: 'Alice'})")
result = query("my.db", "MATCH (p:Person) RETURN p.name")
print(result["rows"])  # [["Alice"]]
```

See `examples/demo.py` for a full ingestion + query demo.

## Tests

```bash
cargo test -p libsql-graph           # all 352 tests
cargo test --test battle              # 78 battle tests (10 subsystems)
cargo test --test integration         # 9 integration tests
```

## Architecture

```
libsql-graph/
  src/
    storage/          # Page-level storage
      pager.rs        # Pager trait
      pager_bridge.rs # FilePager (disk + WAL)
      mem_pager.rs    # MemPager (in-memory)
      wal.rs          # Write-ahead log
      database.rs     # GraphDatabase<P: Pager>
      header.rs       # File header (magic, counters, store roots)
      node_store.rs   # Node records
      rel_store.rs    # Relationship records
      property_store.rs # Property chains
      token_store.rs  # Label/type name tokens
      stats.rs        # Incremental statistics
      ...
    cypher/           # Query engine
      lexer.rs        # Tokenizer
      parser.rs       # Recursive descent parser
      ast.rs          # Abstract syntax tree
      planner.rs      # Logical plan generation
      optimizer.rs    # 3-pass optimizer
      cost.rs         # Cost model
      executor.rs     # Plan execution
      explain.rs      # EXPLAIN / PROFILE
    graph.rs          # GraphEngine<P> — public API
    cursor.rs         # BFS, DFS, shortest path, Dijkstra
    batch.rs          # Batch import builders
    integrity.rs      # Graph integrity checker
    dump.rs           # Cypher dump / stats dump
    error.rs          # Error types
    lib.rs            # Crate root, exports, prelude
```

All graph code is self-contained in `libsql-graph/`. The only change to the parent workspace is adding `"libsql-graph"` to `Cargo.toml` members.
