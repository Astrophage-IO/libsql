# libsql-graph Phase 2: Storage Integration, Transactions, Durability & Query Optimization

## Overview

Phase 2 evolves libsql-graph from a standalone file-based graph engine into one that integrates with libsql's storage infrastructure. Four workstreams run in dependency order:

1. **Pager trait abstraction** -- decouple from concrete `GraphPager`
2. **Real transaction semantics** -- multi-operation ACID transactions
3. **WAL-backed durability** -- crash recovery via write-ahead log
4. **Query optimizer** -- cost-based planning with statistics and index awareness

## 1. Pager Trait Abstraction

### Current state
`GraphPager` in `pager_bridge.rs` is a concrete struct with direct `File` I/O. Every store (`NodeStore`, `RelStore`, etc.) receives `&mut GraphPager` by concrete type. The TODO on line 1 says to replace with SQLite pager FFI.

### Design
Introduce a `Pager` trait that captures the page-level contract:

```rust
pub trait Pager {
    fn db_size(&self) -> u32;
    fn page_size(&self) -> usize;
    fn get_page(&mut self, pgno: u32) -> Result<PageHandle, GraphError>;
    fn alloc_page(&mut self) -> Result<(u32, PageHandle), GraphError>;
    fn write_page(&mut self, handle: &PageHandle) -> Result<(), GraphError>;
    fn begin_read(&mut self) -> Result<(), GraphError>;
    fn begin_write(&mut self) -> Result<(), GraphError>;
    fn commit(&mut self) -> Result<(), GraphError>;
    fn rollback(&mut self) -> Result<(), GraphError>;
}
```

`GraphPager` becomes `impl Pager`. `GraphDatabase`, all stores, and `GraphEngine` become generic over `P: Pager`. A future `FfiPager` wrapping SQLite's pager will also `impl Pager`.

`PageHandle` stays as-is -- it's already an owned byte buffer decoupled from the pager.

### Migration plan
- Add the trait to `storage/pager.rs` (new file, replaces `pager_bridge` module name).
- Keep `GraphPager` renamed to `FilePager` implementing the trait.
- Make `GraphDatabase<P: Pager>`, propagate generic through `GraphEngine<P>`.
- Public API uses a type alias: `pub type DefaultGraphEngine = GraphEngine<FilePager>` so existing users don't break.
- All store methods change `pager: &mut GraphPager` to `pager: &mut P`.

## 2. Transaction Semantics

### Current state
Every mutation (`create_node`, `create_relationship`, `set_node_property`) calls `begin_write()` at the top and `flush_and_commit()` at the bottom -- auto-commit per operation. `TransactionBatch` just loops over queries; if query 3 fails, queries 1-2 are already committed.

### Design
Add explicit transaction control to `GraphEngine`:

```rust
impl<P: Pager> GraphEngine<P> {
    pub fn begin(&mut self) -> Result<(), GraphError>;
    pub fn commit(&mut self) -> Result<(), GraphError>;
    pub fn rollback(&mut self) -> Result<(), GraphError>;
    pub fn in_transaction(&self) -> bool;
}
```

Behavior:
- When `in_transaction()` is true, mutations skip internal `begin_write`/`flush_and_commit` calls. The caller controls commit boundaries.
- When `in_transaction()` is false (default), mutations auto-commit as they do today -- backward compatible.
- `TransactionBatch` uses `begin()`/`commit()` internally so all queries are atomic.
- Nested `begin()` calls return `TransactionActive` error (no savepoints in v1).

Implementation:
- Add `tx_depth: u32` field to `GraphEngine`. `begin()` sets it to 1, `commit()`/`rollback()` sets it back to 0.
- Extract mutation guts from `create_node` etc. into internal `_create_node_inner` that doesn't touch transactions.
- Public methods check `tx_depth`: if 0, auto-commit wrapper; if >0, call inner directly.

## 3. WAL-Backed Durability

### Current state
`FilePager::commit()` sorts dirty pages, writes them sequentially to the data file, then fsyncs. No WAL. Crash during commit = corrupt file. Rollback only works for in-memory dirty pages (pre-commit).

### Design
Add a write-ahead log to `FilePager` (the built-in pager, not the trait -- WAL is an implementation detail of a particular pager).

WAL format:
```
[WAL Header: 32 bytes]
  magic: u32 = 0x4C53_4757  ("LSGW")
  version: u32 = 1
  page_size: u32
  checkpoint_seq: u64
  salt1: u32
  salt2: u32
  checksum: u32

[Frame]*
  [Frame Header: 24 bytes]
    pgno: u32
    db_size_after: u32  (0 if not a commit frame)
    salt1: u32
    salt2: u32
    checksum_1: u32
    checksum_2: u32
  [Page Data: page_size bytes]
```

Commit protocol:
1. `begin_write()` -- no change
2. `write_page()` -- buffer dirty pages (as today)
3. `commit()`:
   a. Append all dirty pages as WAL frames
   b. Write commit frame (last frame has `db_size_after != 0`)
   c. fsync WAL file
   d. Clear dirty pages
4. Checkpoint (periodic or explicit):
   a. Copy WAL frames back to main database file
   b. fsync database file
   c. Reset WAL

Recovery on open:
1. If WAL file exists, replay committed frames (those with a valid commit marker) into the database file.
2. Discard incomplete transactions (frames after last commit marker).
3. Checkpoint and delete WAL.

Read path:
- `get_page()` checks WAL index (in-memory hash map of pgno -> latest WAL offset) before reading from database file.

### Scope limits
- Single-writer, single-reader (no concurrent readers from WAL). This matches current model.
- No shared-memory WAL index. The WAL is private to the process.
- Checkpoint is explicit (`engine.checkpoint()`) or automatic after N frames (default 1000).

## 4. Query Optimizer

### Current state
The planner (`cypher/planner.rs`) does a direct AST-to-plan translation with no cost estimation. `NodeScan` always does a full label scan. The label index exists but isn't used by the planner. No statistics.

### Design

### 4a. Statistics collector
Add to `GraphHeader`:
```
label_node_counts: Vec<(u32, u64)>  // label_id -> count
rel_type_counts: Vec<(u32, u64)>    // type_id -> count
```

These are maintained incrementally on create/delete operations. Stored in a dedicated stats page (new page type) pointed to from the header.

### 4b. Cost model
Simple cost units:
- Full node scan: `node_count`
- Label scan (via label index): `label_node_count`
- Expand from node: `avg_degree` (estimated from `edge_count / node_count`)
- Property filter: selectivity estimate 0.1 (constant, no histograms yet)

### 4c. Planner improvements

**Index-aware label scan**: When a `NodeScan` has a label, use `LabelIndex` bitmap scan instead of full scan. The planner emits `IndexedNodeScan { label, index_root }` plan step.

**Join ordering**: For multi-MATCH patterns like `(a:Person)-[:KNOWS]->(b)-[:LIVES_IN]->(c:City)`, estimate cost of starting from `a` vs `c` and pick the cheaper direction. Currently always scans left-to-right.

**Predicate pushdown**: Move `WHERE` filters as close to scans as possible. Currently filters are always applied after all expansions.

### 4d. New plan steps
```rust
IndexedNodeScan {
    variable: String,
    label: String,
    index_root: u32,
    properties: Vec<(String, Literal)>,
    optional: bool,
}
```

### Scope limits
- No histogram-based selectivity. Constant 0.1 for property predicates.
- No index on properties (only label index). Property indexes are Phase 3.
- No caching of compiled plans.

## Dependency Order

```
1. Pager Trait  -->  2. Transactions  -->  3. WAL Durability
                                      \
                                       -->  4. Query Optimizer (independent of WAL)
```

Pager trait must come first because transactions and WAL both modify the pager layer. Query optimizer is independent of the storage changes and can proceed in parallel with WAL work.

## Testing Strategy

- **Pager trait**: Existing pager tests must pass with `FilePager`. Add a `MemPager` (in-memory implementation) for fast unit tests.
- **Transactions**: Test rollback across multiple operations, partial failure, auto-commit backward compat.
- **WAL**: Crash simulation (truncate WAL at various points), recovery verification, checkpoint correctness.
- **Optimizer**: EXPLAIN output assertions, benchmark comparisons for known slow patterns.

## Files changed

| Area | New files | Modified files |
|------|-----------|---------------|
| Pager trait | `storage/pager.rs` | `storage/mod.rs`, `storage/database.rs`, `storage/node_store.rs`, `storage/rel_store.rs`, `storage/property_store.rs`, `storage/token_store.rs`, `storage/freespace.rs`, `storage/dense.rs`, `storage/label_index.rs`, `storage/string_overflow.rs` |
| Transactions | -- | `graph.rs`, `batch.rs`, `lib.rs` |
| WAL | `storage/wal.rs` | `storage/pager_bridge.rs` (renamed to `storage/file_pager.rs`) |
| Optimizer | `cypher/optimizer.rs`, `storage/stats.rs` | `cypher/planner.rs`, `cypher/executor.rs`, `storage/header.rs`, `graph.rs` |
