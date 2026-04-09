# Bolt v4.4 Server -- Implementation Plan

Reference: [Design Spec](./2026-04-09-bolt-server-design.md)

## Step 1: Crate scaffold + PackStream codec

**Goal:** New `libsql-graph-bolt` crate with a fully tested PackStream v1 encoder/decoder.

**Files to create:**
- `libsql-graph-bolt/Cargo.toml` -- deps: tokio, bytes, libsql-graph (path)
- `libsql-graph-bolt/src/lib.rs` -- crate root, module declarations
- `libsql-graph-bolt/src/error.rs` -- `BoltError` enum
- `libsql-graph-bolt/src/packstream/mod.rs` -- re-exports
- `libsql-graph-bolt/src/packstream/value.rs` -- `PackValue` enum
- `libsql-graph-bolt/src/packstream/encode.rs` -- `encode(value: &PackValue, buf: &mut BytesMut)`
- `libsql-graph-bolt/src/packstream/decode.rs` -- `decode(buf: &mut Bytes) -> Result<PackValue, BoltError>`

**Acceptance criteria:**
- Round-trip encode/decode for every PackValue variant
- Correct marker selection (smallest encoding)
- Big-endian byte order
- Edge cases: empty string, -16, -17, 127, 128, empty map/list, nested structs
- Unit tests pass

**Estimated complexity:** Medium. Mechanical but needs precision on marker byte ranges.

---

## Step 2: Chunked transport layer

**Goal:** Read and write chunked messages over async TCP streams.

**Files to create:**
- `libsql-graph-bolt/src/transport.rs`

**Implementation:**
- `async fn read_message(stream: &mut TcpStream) -> Result<Bytes, BoltError>` -- reads chunks until `00 00`, assembles into single buffer
- `async fn write_message(stream: &mut TcpStream, data: &[u8]) -> Result<(), BoltError>` -- splits into chunks (max 65535 bytes each), writes `00 00` terminator

**Acceptance criteria:**
- Correctly reads multi-chunk messages
- Correctly writes messages that exceed 65535 bytes into multiple chunks
- Handles partial reads (TCP may deliver partial data)
- Unit tests with in-memory streams

**Estimated complexity:** Low.

---

## Step 3: Bolt message types + handshake

**Goal:** Define request/response message enums and implement handshake negotiation.

**Files to create:**
- `libsql-graph-bolt/src/protocol/mod.rs`
- `libsql-graph-bolt/src/protocol/message.rs` -- `BoltRequest` and `BoltResponse` enums with parse/serialize using PackStream
- `libsql-graph-bolt/src/protocol/handshake.rs` -- parse 20-byte client handshake, negotiate v4.4, write 4-byte response

**BoltRequest enum:**
```
Hello { extra: HashMap<String, PackValue> }
Goodbye
Reset
Run { query: String, params: HashMap<String, PackValue>, extra: HashMap<String, PackValue> }
Begin { extra: HashMap<String, PackValue> }
Commit
Rollback
Discard { n: i64, qid: i64 }
Pull { n: i64, qid: i64 }
```

**BoltResponse enum:**
```
Success { metadata: HashMap<String, PackValue> }
Record { data: Vec<PackValue> }
Ignored
Failure { code: String, message: String }
```

**Message parsing:** decode PackValue from transport, match on Struct tag byte, extract fields.
**Message serializing:** build PackValue::Struct with correct tag, encode via PackStream.

**Acceptance criteria:**
- Parse every request message type from raw bytes
- Serialize every response message type to correct bytes
- Handshake accepts v4.4, rejects unsupported versions
- Round-trip tests for each message type

**Estimated complexity:** Medium.

---

## Step 4: Server state machine

**Goal:** Implement the Bolt v4.4 state machine that governs which messages are valid in which state.

**Files to create:**
- `libsql-graph-bolt/src/protocol/state.rs`

**States:** Negotiation, Ready, Streaming, TxReady, TxStreaming, Failed, Interrupted, Defunct

**Implementation:**
- `enum BoltState` with all states
- `fn transition(state: &BoltState, request: &BoltRequest, success: bool) -> BoltState`
- `fn is_valid(state: &BoltState, request: &BoltRequest) -> bool`
- In Failed state, all non-RESET messages return Ignored
- RESET from any state returns to Ready

**Acceptance criteria:**
- All transitions from the spec are covered
- Invalid transitions detected
- Failed state correctly ignores everything except Reset
- Unit tests for every state transition

**Estimated complexity:** Low. Pure logic, no I/O.

---

## Step 5: Type mapping (GraphEngine <-> PackStream)

**Goal:** Convert between libsql-graph's `Value`/`QueryResult` types and PackStream values.

**Files to create:**
- `libsql-graph-bolt/src/types.rs`

**Functions:**
- `fn graph_value_to_pack(value: &Value, engine: &GraphEngine) -> PackValue` -- converts query result values
- `fn pack_to_param(value: &PackValue) -> HashMap<String, Value>` -- converts RUN parameters
- `fn query_stats_to_map(stats: &QueryStats) -> HashMap<String, PackValue>` -- converts stats
- `fn graph_error_to_bolt(err: &GraphError) -> (String, String)` -- error code + message

**Node/Rel handling:** When Value::Node(id) is encountered, fetch the node's labels and properties from the engine to build the full Bolt Node struct (tag 0x4E).

**Acceptance criteria:**
- All Value variants map correctly
- Node struct includes id, labels list, properties map
- Rel struct includes id, start_id, end_id, type, properties map
- Parameter maps decode correctly for query_with_params
- Stats map uses hyphenated keys (nodes-created, not nodes_created)

**Estimated complexity:** Medium. Node/Rel serialization needs engine access.

---

## Step 6: Session handler

**Goal:** The core per-connection logic that ties everything together.

**Files to create:**
- `libsql-graph-bolt/src/session.rs`

**Session struct:**
```rust
struct Session {
    state: BoltState,
    engine: Arc<Mutex<GraphEngine<FilePager>>>,
    connection_id: String,
    pending_result: Option<QueryResult>,
    pending_cursor: usize,  // index into pending_result.rows
    bookmark_counter: u64,
}
```

**Message handlers:**

- `handle_hello` -- accept any auth, respond SUCCESS with server info
- `handle_run` -- lock engine, call query_with_params(), store result, respond SUCCESS with fields
- `handle_pull` -- iterate stored rows (respecting n parameter), send RECORDs + final SUCCESS
- `handle_discard` -- skip rows, send SUCCESS
- `handle_begin` -- lock engine, call begin(), respond SUCCESS
- `handle_commit` -- call commit(), respond SUCCESS with bookmark
- `handle_rollback` -- call rollback(), respond SUCCESS
- `handle_reset` -- rollback if in tx, clear pending result, return to Ready
- `handle_goodbye` -- mark Defunct, return

**PULL with n parameter:** If n > 0 and n < remaining rows, send n RECORDs + SUCCESS{has_more: true}. If n == -1, send all remaining rows.

**Acceptance criteria:**
- Full auto-commit query flow works (RUN + PULL)
- Explicit transaction flow works (BEGIN + RUN + PULL + COMMIT)
- ROLLBACK properly reverts
- RESET recovers from FAILED state
- Partial PULL (n > 0) works with has_more
- Errors produce FAILURE responses and transition to FAILED

**Estimated complexity:** High. This is the integration point.

---

## Step 7: TCP server + binary

**Goal:** TCP accept loop and CLI binary.

**Files to create:**
- `libsql-graph-bolt/src/server.rs` -- `BoltServer` struct with bind() and run()
- `libsql-graph-bolt/src/bin/bolt_server.rs` -- CLI binary with --db and --port args

**Server.run() loop:**
1. Accept TCP connection
2. Spawn tokio task per connection
3. In task: run handshake, then session message loop
4. On error/disconnect: log and clean up

**CLI binary:**
```
bolt-server --db ./graph.db --port 7687
```

Uses `clap` or simple arg parsing. Creates/opens the database, binds the server, runs.

**Acceptance criteria:**
- Server starts and listens on configured port
- Multiple concurrent connections work
- Clean shutdown on CTRL-C
- Connection errors don't crash the server

**Estimated complexity:** Low-medium.

---

## Step 8: Integration test with Python driver

**Goal:** Prove the Python `neo4j` driver can connect and run queries.

**Files to create:**
- `libsql-graph-bolt/tests/integration.rs` -- starts server, runs Python subprocess
- `libsql-graph-bolt/tests/test_bolt.py` -- Python test script

**Test scenarios:**
1. Connect and disconnect
2. Auto-commit CREATE + MATCH query
3. Explicit transaction with COMMIT
4. Explicit transaction with ROLLBACK (verify data not persisted)
5. Cypher syntax error -> driver receives error
6. Multiple sequential queries in one session
7. Parameterized queries ($name parameter)

**Acceptance criteria:**
- All 7 scenarios pass
- Python driver reports no protocol errors
- Data created in one query is visible in subsequent queries

**Estimated complexity:** Medium. Requires coordinating Rust server + Python subprocess.

---

## Execution Order

Steps 1-4 are foundational and mostly independent (1 and 2 can be parallelized, 3 depends on 1, 4 is independent). Steps 5-6 depend on 1-4. Step 7 depends on 6. Step 8 depends on 7.

```
Step 1 (PackStream) ──┐
                       ├── Step 3 (Messages) ──┐
Step 2 (Transport) ────┘                       │
                                               ├── Step 6 (Session) ── Step 7 (Server) ── Step 8 (Integration)
Step 4 (State Machine) ────────────────────────┤
                                               │
Step 5 (Type Mapping) ─────────────────────────┘
```

Steps 1+2 can be built in parallel.
Steps 4+5 can be built in parallel (after 1).
Step 3 needs Step 1 (uses PackValue).
Step 6 needs Steps 2-5.
Step 7 needs Step 6.
Step 8 needs Step 7.
