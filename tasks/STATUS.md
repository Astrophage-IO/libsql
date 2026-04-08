# libsql-graph Implementation Status

## Current Phase: Phase 1 - Storage Foundation

## Task Tracker

| # | Task | Status | Agent | Tests |
|---|------|--------|-------|-------|
| 001 | Crate scaffold + pager FFI bridge | done | agent-1 | 6/6 |
| 002 | Page types, header page, graph file create/open | done | agent-2 | 6/6 |
| 003 | Node store (alloc/read/write/delete) | pending | - | - |
| 004 | Relationship store + doubly-linked chains | pending | - | - |
| 005 | Token store (label/type name <-> ID) | pending | - | - |
| 006 | Free-space bitmap manager | pending | - | - |
| 007 | Property store (inline + overflow) | pending | - | - |
| 008 | Basic graph API (create_node, create_rel, get_neighbors) | pending | - | - |
| 009 | Integration tests (CRUD, chain integrity, crash recovery) | pending | - | - |

## Phase 2 - Cypher & Traversal (not started)

| # | Task | Status |
|---|------|--------|
| 010 | Cypher lexer + parser (lalrpop) | pending |
| 011 | Cypher planner (AST -> TraversalPlan) | pending |
| 012 | Executor + native cursors (NodeCursor, RelChainCursor) | pending |
| 013 | BFS / DFS / Dijkstra on native storage | pending |
| 014 | Multi-hop pattern matching | pending |
| 015 | Cypher mutations (CREATE, SET, DELETE) | pending |

## Phase 3 - Server Integration (not started)

| # | Task | Status |
|---|------|--------|
| 016 | HTTP endpoints (/v1/graph/cypher) | pending |
| 017 | Virtual table bridge (graph_cypher TVF) | pending |
| 018 | Replication wiring for graph pager | pending |

## Phase 4 - Performance (not started)

| # | Task | Status |
|---|------|--------|
| 019 | Dense node support (relationship groups) | pending |
| 020 | Label index (bitmap scan pages) | pending |
| 021 | Parallel BFS | pending |
| 022 | Benchmark suite (LDBC, SNAP) | pending |

## Handoff Notes

### Task 002 (agent-2)

**What was implemented:**
- `src/storage/header.rs` -- `GraphHeader` struct with `new()`, `read()`, `write()`, `validate()`. Magic bytes `LSGRAPH\0`, format version 1. Respects the byte 24-99 reserved region (pager use). All fields serialized in little-endian.
- `src/storage/database.rs` -- `GraphDatabase` struct with `create()`, `open()`, `flush_header()`, `next_node_id()`, `next_rel_id()`, `next_prop_id()`, `next_token_id()`, `header()`, `pager()`, `page_size()`.
- `src/error.rs` -- Added `InvalidMagic` and `UnsupportedVersion(u32)` variants to `GraphError`.
- `src/storage/mod.rs` -- Added `pub mod header; pub mod database;`.
- 6 new tests all passing, 12 total (6 task-001 + 6 task-002), clippy clean.

**Key design decisions:**
- `GraphDatabase::create()` allocates 6 pages in a single write transaction: page 1 (header) + 5 root pages (node, rel, prop, token, freemap). Each root page gets its `PageHeader` initialized with the correct `PageType`.
- `GraphDatabase::open()` reads the page size from bytes 12-15 of the raw file (before opening the pager), then opens the pager with that size, reads page 1, and validates magic + version.
- Auto-increment IDs are in-memory counters on `GraphHeader`. `flush_header()` persists them to page 1. The `next_*_id()` methods return the current value and increment.
- Header field sizes follow the task spec: `next_node_id`/`next_rel_id`/`next_prop_id` are u64, `next_token_id` is u32. The design doc showed 4-byte fields but the task spec is authoritative.

**For next task (003):**
- `GraphDatabase` is the entry point. Use `db.pager()` to get a `&mut GraphPager` for page I/O.
- `db.header().node_store_root` gives the first page of the node store.
- Call `db.next_node_id()` to allocate node IDs, then `db.flush_header()` after committing to persist the counter.
- The node store root page already has `PageType::NodeStore` in its `PageHeader`.

---

### Task 001 (agent-1)

**What was implemented:**
- Created `libsql-graph/` crate with full module structure: `lib.rs`, `error.rs`, `storage/mod.rs`, `storage/pager_bridge.rs`, `storage/page.rs`
- Added `"libsql-graph"` to workspace members in root `Cargo.toml`
- `GraphError` enum with `IoError`, `PagerError`, `CorruptPage`, `InvalidPageNumber`, `NoTransaction`, `TransactionActive`
- `GraphPager` with `open()`, `db_size()`, `page_size()`, `get_page()`, `alloc_page()`, `begin_read()`, `begin_write()`, `write_page()`, `commit()`, `rollback()`, `close()`
- `PageHandle` with `page_number()`, `data()`, `data_mut()`, `page_size()`
- `PageType` enum (8 variants) and `PageHeader` with `read()`/`write()` serialization
- All 6 tests passing, clippy clean

**Deviations from spec:**
- Used a pure-Rust file-based pager instead of FFI wrapper. The `sqlite3Pager*` functions are NOT exposed in `libsql-ffi` -- they are internal SQLite symbols in `sqlite3.c` only. The TODO at the top of `pager_bridge.rs` marks this for future replacement.
- Added `write_page(&PageHandle)` method since our pager is copy-based (handles own their data buffer rather than pointing into a shared page cache). In the FFI version, `data_mut()` would call `sqlite3PagerWrite` directly and mutate in place. The `write_page` call is the pure-Rust equivalent of flushing the handle's buffer back to the dirty page set.
- `begin_read()` is a no-op in the file-based pager (no shared lock needed for single-process file I/O).

**FFI situation:**
- `libsql-ffi/src/lib.rs` contains zero Pager-related bindings
- Pager functions exist only inside `libsql-ffi/bundled/src/sqlite3.c` (amalgamation, ~511 occurrences) as internal/static symbols
- To use the real pager later, you would need to either: (a) add `SQLITE_API` declarations for the pager functions and regenerate bindings, (b) write a C shim that wraps internal calls, or (c) use `libsql-sqlite3/src/pager.h` headers with a custom build

**For next task (002):**
- The pager bridge is ready to use. Import `crate::storage::pager_bridge::{GraphPager, PageHandle}` and `crate::storage::page::{PageType, PageHeader, PAGE_HEADER_SIZE}`
- The `write_page` pattern is: `alloc_page()` or `get_page()`, mutate via `data_mut()`, then call `pager.write_page(&handle)` before commit
- Page numbers are 1-based (page 0 is invalid)

---
