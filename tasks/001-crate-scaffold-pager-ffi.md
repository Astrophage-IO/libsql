# Task 001: Crate Scaffold + Pager FFI Bridge

**Status**: done
**Depends on**: nothing (first task)
**Design doc**: `docs/plans/2026-04-08-graph-engine-design.md`

## Objective

Create the `libsql-graph` Rust crate and build a safe Rust wrapper around libsql's C pager API. This is the foundation everything else builds on.

## Deliverables

### 1. Crate scaffold

Create `libsql-graph/` at the repo root with:

```
libsql-graph/
  Cargo.toml          # depends on libsql-ffi for C bindings
  src/
    lib.rs            # pub mod storage; pub mod error;
    error.rs          # GraphError enum (IoError, PagerError, CorruptPage, etc.)
    storage/
      mod.rs          # pub mod pager_bridge; pub mod page;
      pager_bridge.rs # Safe Rust wrapper around sqlite3Pager* FFI
      page.rs         # PageType enum, PageHeader struct, raw page buffer access
```

Add `"libsql-graph"` to the workspace `members` in the root `Cargo.toml`.

### 2. Pager FFI Bridge (`storage/pager_bridge.rs`)

Wrap these C functions from `libsql-ffi` in safe Rust:

```rust
pub struct GraphPager {
    inner: *mut ffi::Pager,
    page_size: usize,
}

impl GraphPager {
    /// Open a new pager on a .graph file
    /// Calls: sqlite3PagerOpen(vfs, wal_manager, &pPager, path, nExtra, flags, vfsFlags, xReinit, 0)
    pub fn open(path: &str, page_size: u32) -> Result<Self, GraphError>;
    
    /// Get current database size in pages
    pub fn db_size(&self) -> u32;
    
    /// Fetch a page by number. Returns a PageHandle (RAII wrapper that calls Unref on drop).
    /// Calls: sqlite3PagerGet(pPager, pgno, &pPage, flags)
    pub fn get_page(&self, pgno: u32) -> Result<PageHandle, GraphError>;
    
    /// Allocate a new page (beyond current db_size, with PAGER_GET_NOCONTENT)
    pub fn alloc_page(&self) -> Result<(u32, PageHandle), GraphError>;
    
    /// Begin a write transaction
    /// Calls: sqlite3PagerBegin(pPager, 1, 0)
    pub fn begin_write(&self) -> Result<(), GraphError>;
    
    /// Begin a read transaction (shared lock)
    /// Calls: sqlite3PagerSharedLock(pPager)
    pub fn begin_read(&self) -> Result<(), GraphError>;
    
    /// Commit the current transaction (phase one + two)
    pub fn commit(&self) -> Result<(), GraphError>;
    
    /// Rollback the current transaction
    pub fn rollback(&self) -> Result<(), GraphError>;
    
    /// Close the pager
    /// Called automatically on Drop
    pub fn close(&mut self) -> Result<(), GraphError>;
}

/// RAII page handle. Calls sqlite3PagerUnref on drop.
pub struct PageHandle {
    inner: *mut ffi::DbPage,
    page_size: usize,
}

impl PageHandle {
    /// Get raw page number
    pub fn page_number(&self) -> u32;
    
    /// Get immutable reference to page data
    /// Calls: sqlite3PagerGetData(pPage) 
    pub fn data(&self) -> &[u8];
    
    /// Get mutable reference to page data (marks page dirty automatically)
    /// Calls: sqlite3PagerWrite(pPage) then sqlite3PagerGetData(pPage)
    pub fn data_mut(&mut self) -> Result<&mut [u8], GraphError>;
}

impl Drop for PageHandle {
    fn drop(&mut self) {
        // sqlite3PagerUnref(self.inner)
    }
}
```

### 3. Page Types (`storage/page.rs`)

```rust
#[repr(u8)]
pub enum PageType {
    Header = 0x00,
    NodeStore = 0x01,
    RelStore = 0x02,
    PropertyStore = 0x03,
    TokenStore = 0x04,
    FreeBitmap = 0x05,
    RelGroup = 0x06,
    StringOverflow = 0x07,
}

pub const PAGE_HEADER_SIZE: usize = 8;

#[repr(C, packed)]
pub struct PageHeader {
    pub page_type: u8,
    pub flags: u8,
    pub record_count: u16,
    pub next_page: u32,
}

impl PageHeader {
    pub fn read(data: &[u8]) -> Self { ... }
    pub fn write(&self, data: &mut [u8]) { ... }
}
```

## Tests to Write

All tests go in `libsql-graph/src/storage/tests.rs` or `libsql-graph/tests/`.

### Test 1: `test_pager_open_close`
- Open a pager on a temp file
- Verify db_size() == 0
- Close pager
- Reopen, verify still works

### Test 2: `test_alloc_and_write_page`
- Open pager, begin write tx
- Allocate a page
- Write known bytes to it via data_mut()
- Commit
- Reopen pager, read the page, verify bytes match

### Test 3: `test_multiple_pages`
- Allocate 100 pages, write unique data to each
- Commit, reopen, verify all 100 pages

### Test 4: `test_page_header_roundtrip`
- Create a PageHeader, write to bytes, read back, verify all fields match

### Test 5: `test_transaction_rollback`
- Begin write tx, write to a page, rollback
- Verify the page reverts to previous state

### Test 6: `test_page_handle_drop`
- Get a page handle, drop it, get it again
- Verify no double-free or corruption

## How to Verify

```bash
cd libsql-graph && cargo test
```

All 6 tests must pass. `cargo clippy` must have no warnings.

## FFI Reference

Check these files for the exact C function signatures:
- `libsql-ffi/bundled/src/sqlite3.h` or `libsql-sqlite3/src/pager.h` for pager API
- `libsql-ffi/src/lib.rs` for existing Rust bindings (may need to add new bindings)
- `libsql-sys/src/wal/ffi.rs` for the pattern of wrapping C APIs in safe Rust

## Important Notes

- The pager API (`sqlite3PagerOpen` etc.) may NOT be exposed in `libsql-ffi` yet since it's an internal API, not part of the public `sqlite3.h`. You may need to:
  1. Add the pager function declarations to `libsql-ffi/bundled/bindings/session_bindgen.rs` or create a new header file
  2. Or use `libsql-sys` which has lower-level access
  3. Or write a small C shim that wraps the internal pager calls and exposes them as public symbols
- Check what's already exposed before adding new bindings
- The `sqlite3PagerOpen` function needs a VFS and WAL manager. Look at how `sqlite3BtreeOpen` in `btree.c` calls it for the exact parameter values to use.

## Handoff

When done, update `tasks/STATUS.md`:
1. Mark task 001 as `done`
2. Write a handoff note describing:
   - What was implemented
   - Any deviations from this spec
   - What the FFI situation looked like (what was already exposed, what you had to add)
   - Any issues the next task should know about
3. The next task is `002-page-types-header.md`
