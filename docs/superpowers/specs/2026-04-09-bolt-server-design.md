# libsql-graph Bolt v4.4 Server -- Design Spec

## Goal

Enable Python `neo4j` driver (v6.x) to connect to libsql-graph via the Neo4j Bolt binary protocol. Users connect with `bolt://localhost:7687` and run Cypher queries against the embedded graph engine.

## Scope

- Bolt protocol v4.4 only (no v5.x LOGON/LOGOFF/TELEMETRY)
- Python `neo4j` driver as primary target (connects in direct/bolt:// mode)
- No routing, no TLS, no auth enforcement (accept any credentials)
- No clustering, no multi-database (single graph per server)

## Non-Goals

- Bolt v5.x support (future work, incremental upgrade)
- Neo4j REST/HTTP API
- TLS termination (use a reverse proxy if needed)
- Multi-database routing (ROUTE message returns NOT_SUPPORTED)
- Temporal/spatial PackStream types (not in libsql-graph's type system)

---

## Architecture

### New Crate: `libsql-graph-bolt`

```
libsql-graph-bolt/
  src/
    packstream/
      mod.rs          -- re-exports
      encode.rs       -- PackStream v1 serializer
      decode.rs       -- PackStream v1 deserializer
      value.rs        -- PackValue enum (the wire-level value type)
    protocol/
      mod.rs          -- re-exports
      message.rs      -- Bolt message types (Request/Response enums)
      state.rs        -- Server state machine (NEGOTIATION -> READY -> ...)
      handshake.rs    -- 20-byte handshake parsing + 4-byte response
    transport.rs      -- Chunked message framing (read/write over AsyncRead/AsyncWrite)
    session.rs        -- Per-connection handler: maps Bolt messages to GraphEngine calls
    server.rs         -- TCP listener, connection accept loop
    types.rs          -- Mapping between GraphEngine Value <-> PackStream Value
    error.rs          -- BoltError type
    lib.rs            -- Public API: BoltServer
  Cargo.toml
  tests/
    integration.rs    -- Python driver integration test
```

### Dependencies

- `tokio` (rt-multi-thread, net, io-util, macros) -- async TCP
- `bytes` -- buffer management for PackStream codec
- `libsql-graph` -- the engine (path dependency)
- `log` / `tracing` -- diagnostics

### Connection Model

Each TCP connection gets its own `GraphEngine` opened on the same database path. libsql-graph uses file-level locking via WAL, so concurrent readers work. Write transactions serialize at the engine level.

```
TCP Listener (port 7687)
  |
  +-- Connection 1 --> Session { state_machine, GraphEngine }
  +-- Connection 2 --> Session { state_machine, GraphEngine }
  +-- Connection N --> ...
```

Alternatively, a single `GraphEngine` behind `Arc<Mutex<GraphEngine>>` shared across connections. Simpler, and write serialization is natural. Start with this approach.

---

## PackStream v1 Codec

### Wire Value Type

```rust
enum PackValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<PackValue>),
    Map(Vec<(String, PackValue)>),  // ordered key-value pairs
    Struct { tag: u8, fields: Vec<PackValue> },
}
```

### Encoding Rules

Use the smallest representation:

| Type | Tiny (inline) | 8-bit size | 16-bit size | 32-bit size |
|------|--------------|------------|-------------|-------------|
| Int  | 0x00-0x7F (0..127), 0xF0-0xFF (-16..-1) | 0xC8 (-128..-17) | 0xC9 | 0xCA, 0xCB |
| String | 0x80+len (0..15 bytes) | 0xD0 | 0xD1 | 0xD2 |
| List | 0x90+count (0..15) | 0xD4 | 0xD5 | 0xD6 |
| Map  | 0xA0+count (0..15) | 0xD8 | 0xD9 | 0xDA |
| Struct | 0xB0+fields (0..15) + tag byte | -- | -- | -- |
| Bytes | -- | 0xCC | 0xCD | 0xCE |
| Null | 0xC0 | -- | -- | -- |
| Bool | 0xC2 (false), 0xC3 (true) | -- | -- | -- |
| Float | 0xC1 + 8 bytes IEEE 754 BE | -- | -- | -- |

All multi-byte integers are big-endian. Strings are UTF-8 byte length, not char count.

---

## Bolt v4.4 Protocol

### Handshake

Client sends 20 bytes:
- Bytes 0-3: magic `0x6060B017`
- Bytes 4-19: four 4-byte version slots (each: `[reserved, range, minor, major]`)

Server parses slots, finds highest supported version. For us: match anything with major=4, minor>=4. Respond with `00 00 04 04`. If no match, respond `00 00 00 00` and close.

### Chunked Transport

After handshake, all messages are chunked:
- 2-byte BE length prefix + payload bytes (max 65535 per chunk)
- Message ends with `00 00` (zero-length chunk)
- A message can span multiple chunks

### Messages

#### Request Messages (client -> server)

| Message | Tag | Fields | Notes |
|---------|-----|--------|-------|
| HELLO | 0x01 | extra: Map | user_agent, scheme, principal, credentials |
| GOODBYE | 0x02 | (none) | Close connection, no response |
| RESET | 0x0F | (none) | Return to READY state |
| RUN | 0x10 | query: String, params: Map, extra: Map | Execute Cypher |
| BEGIN | 0x11 | extra: Map | Start explicit transaction |
| COMMIT | 0x12 | (none) | Commit transaction |
| ROLLBACK | 0x13 | (none) | Rollback transaction |
| DISCARD | 0x2F | extra: Map | Discard N records (n, qid) |
| PULL | 0x3F | extra: Map | Pull N records (n, qid) |

#### Response Messages (server -> client)

| Message | Tag | Fields |
|---------|-----|--------|
| SUCCESS | 0x70 | metadata: Map |
| RECORD | 0x71 | data: List |
| IGNORED | 0x7E | (none) |
| FAILURE | 0x7F | metadata: Map (code, message) |

### Server State Machine

```
NEGOTIATION --HELLO(ok)--> READY
READY --RUN(ok)--> STREAMING
READY --BEGIN(ok)--> TX_READY
STREAMING --PULL/DISCARD(done)--> READY
STREAMING --PULL/DISCARD(has_more)--> STREAMING
TX_READY --RUN(ok)--> TX_STREAMING
TX_READY --COMMIT(ok)--> READY
TX_READY --ROLLBACK(ok)--> READY
TX_STREAMING --PULL/DISCARD(done)--> TX_READY
TX_STREAMING --PULL/DISCARD(has_more)--> TX_STREAMING
Any --failure--> FAILED
FAILED --RESET(ok)--> READY
Any --GOODBYE--> DEFUNCT
```

In FAILED state, all messages except RESET get IGNORED responses.

### Minimum Viable Responses

**HELLO SUCCESS:**
```
{"server": "LibSQL-Graph/0.1.0", "connection_id": "bolt-<id>"}
```

**RUN SUCCESS:**
```
{"fields": ["col1", "col2"], "t_first": 0}
```

**PULL SUCCESS (final):**
```
{"type": "w", "t_last": 0, "db": "libsql-graph"}
```

**PULL SUCCESS (with stats, for write queries):**
```
{"type": "w", "t_last": 0, "db": "libsql-graph",
 "stats": {"nodes-created": 1, "properties-set": 2}}
```

**BEGIN SUCCESS:** `{}`
**COMMIT SUCCESS:** `{"bookmark": "bk:<counter>"}`
**ROLLBACK SUCCESS:** `{}`

---

## Type Mapping

### libsql-graph Value -> PackStream Value (for RECORD data)

| GraphEngine Value | PackValue |
|-------------------|-----------|
| Value::Null | PackValue::Null |
| Value::Bool(b) | PackValue::Bool(b) |
| Value::Integer(n) | PackValue::Int(n) |
| Value::Float(f) | PackValue::Float(f) |
| Value::String(s) | PackValue::String(s) |
| Value::List(items) | PackValue::List(mapped items) |
| Value::Node(id) | PackValue::Struct { tag: 0x4E, fields: [id, labels, properties] } |
| Value::Rel(id) | PackValue::Struct { tag: 0x52, fields: [id, start, end, type, properties] } |

For Node and Rel structs, we need to fetch labels/properties from the engine when serializing. This requires the session to have engine access during PULL.

### PackStream Value -> Cypher parameter (for RUN params)

| PackValue | Rust/Cypher |
|-----------|-------------|
| PackValue::Null | Value::Null |
| PackValue::Bool(b) | Value::Bool(b) |
| PackValue::Int(n) | Value::Integer(n) |
| PackValue::Float(f) | Value::Float(f) |
| PackValue::String(s) | Value::String(s) |
| PackValue::List(items) | Value::List(mapped items) |
| PackValue::Map(_) | Map parameter (for property maps in CREATE) |

---

## Session Lifecycle

```
1. Accept TCP connection
2. Read handshake (20 bytes), negotiate v4.4, send 4-byte response
3. Enter message loop:
   a. Read chunked message from transport
   b. Decode PackStream -> Bolt Request message
   c. Check state machine for valid transition
   d. If invalid state: send IGNORED (or FAILURE), stay in current state
   e. If valid: dispatch to handler
   f. Handler interacts with GraphEngine, produces Response message(s)
   g. Encode Response -> PackStream -> chunked transport -> TCP
   h. Update state machine
4. On GOODBYE or disconnect: clean up, close engine
```

### Query Execution Flow (RUN + PULL)

RUN stores the query + params but does NOT execute yet. It validates the query and returns column names.
PULL executes and streams results.

Actually, since libsql-graph's `query()` returns all results at once (not streaming), we:
1. On RUN: execute the query immediately, store the `QueryResult` in the session
2. On RUN SUCCESS: return `fields` from the stored result
3. On PULL: iterate over stored rows, send RECORD for each, then SUCCESS

This simplifies implementation -- no lazy evaluation needed.

---

## Public API

```rust
pub struct BoltServer {
    listener: TcpListener,
    db_path: String,
}

impl BoltServer {
    pub async fn bind(addr: &str, db_path: &str) -> Result<Self, BoltError>;
    pub async fn run(&self) -> Result<(), BoltError>;  // accept loop, runs forever
}
```

Usage:
```rust
#[tokio::main]
async fn main() {
    let server = BoltServer::bind("0.0.0.0:7687", "./my-graph.db").await.unwrap();
    server.run().await.unwrap();
}
```

Binary in `libsql-graph-bolt/src/bin/bolt_server.rs`:
```
cargo run --bin bolt-server -- --db ./my-graph.db --port 7687
```

---

## Testing Strategy

1. **Unit tests**: PackStream encode/decode round-trips, message parsing, state machine transitions
2. **Integration test**: Start server in-process, connect with Python `neo4j` driver, run queries, assert results
3. **Manual smoke test**: Python script that creates nodes, queries, uses transactions

Integration test script (`tests/test_bolt.py`):
```python
from neo4j import GraphDatabase
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "test"))
with driver.session() as s:
    s.run("CREATE (n:Person {name: 'Alice', age: 30})")
    result = s.run("MATCH (n:Person) RETURN n.name, n.age")
    for record in result:
        assert record["n.name"] == "Alice"
driver.close()
```

---

## Stats Mapping

libsql-graph `QueryStats` -> Bolt stats map:

| QueryStats field | Bolt stats key |
|-----------------|----------------|
| nodes_created | nodes-created |
| relationships_created | relationships-created |
| properties_set | properties-set |
| nodes_deleted | nodes-deleted |

---

## Error Mapping

| GraphError variant | Bolt error code |
|-------------------|-----------------|
| QueryError / ParseError | Neo.ClientError.Statement.SyntaxError |
| NodeNotFound / RelNotFound | Neo.ClientError.Statement.EntityNotFound |
| ConstraintViolation | Neo.ClientError.Schema.ConstraintValidationFailed |
| IoError / PagerError | Neo.DatabaseError.General.UnknownError |
| NotInTransaction | Neo.ClientError.Transaction.TransactionNotFound |
