# Task 002: Page Types, Header Page, Graph File Create/Open

**Status**: pending
**Depends on**: 001 (pager FFI bridge)
**Design doc**: `docs/plans/2026-04-08-graph-engine-design.md` -- Section 3 "Native Storage Engine"

## Objective

Implement the graph file header page (page 1) and the `GraphDatabase` struct that creates/opens `.graph` files with proper initialization.

## Deliverables

### 1. Graph File Header (`storage/header.rs`)

```rust
pub const GRAPH_MAGIC: &[u8; 8] = b"LSGRAPH\0";
pub const FORMAT_VERSION: u32 = 1;

/// Graph header lives on page 1 of the .graph file.
/// Bytes 24-99 are reserved for pager use (change counter, etc.)
pub struct GraphHeader {
    pub magic: [u8; 8],            // offset 0
    pub format_version: u32,       // offset 8
    pub page_size: u32,            // offset 12
    pub node_count: u64,           // offset 16
    // bytes 24-99: RESERVED FOR PAGER
    pub edge_count: u64,           // offset 100
    pub node_store_root: u32,      // offset 108
    pub rel_store_root: u32,       // offset 112
    pub prop_store_root: u32,      // offset 116
    pub token_store_root: u32,     // offset 120
    pub freemap_root: u32,         // offset 124
    pub next_node_id: u64,         // offset 128
    pub next_rel_id: u64,          // offset 136
    pub next_prop_id: u64,         // offset 144
    pub next_token_id: u32,        // offset 152
    pub label_count: u32,          // offset 156
    pub rel_type_count: u32,       // offset 160
    pub dense_threshold: u32,      // offset 164 (default 50)
}

impl GraphHeader {
    pub fn new(page_size: u32) -> Self { ... }
    pub fn read(page_data: &[u8]) -> Result<Self, GraphError> { ... }
    pub fn write(&self, page_data: &mut [u8]) -> Result<(), GraphError> { ... }
    pub fn validate(&self) -> Result<(), GraphError> { ... } // check magic, version
}
```

### 2. GraphDatabase (`storage/database.rs`)

```rust
pub struct GraphDatabase {
    pager: GraphPager,
    header: GraphHeader,
    page_size: u32,
}

impl GraphDatabase {
    /// Create a new graph database file.
    /// Initializes page 1 with the graph header.
    /// Allocates initial root pages for each store.
    pub fn create(path: &str, page_size: u32) -> Result<Self, GraphError> {
        let pager = GraphPager::open(path, page_size)?;
        pager.begin_write()?;
        
        // Allocate page 1 (header)
        let (_, mut header_page) = pager.alloc_page()?;
        let mut header = GraphHeader::new(page_size);
        
        // Allocate root pages for each store
        let (node_root, _) = pager.alloc_page()?;
        let (rel_root, _) = pager.alloc_page()?;
        let (prop_root, _) = pager.alloc_page()?;
        let (token_root, _) = pager.alloc_page()?;
        let (freemap_root, _) = pager.alloc_page()?;
        
        header.node_store_root = node_root;
        header.rel_store_root = rel_root;
        header.prop_store_root = prop_root;
        header.token_store_root = token_root;
        header.freemap_root = freemap_root;
        
        // Write header to page 1
        header.write(header_page.data_mut()?)?;
        
        // Initialize root pages with correct page type headers
        // ...
        
        pager.commit()?;
        Ok(Self { pager, header, page_size })
    }
    
    /// Open an existing graph database file.
    /// Reads and validates the header from page 1.
    pub fn open(path: &str) -> Result<Self, GraphError> {
        // Read page 1, parse header, validate magic + version
    }
    
    /// Flush header changes (counters, counts) to page 1
    pub fn flush_header(&mut self) -> Result<(), GraphError> { ... }
    
    /// Get the next auto-increment ID for a store
    pub fn next_node_id(&mut self) -> u64 { ... }
    pub fn next_rel_id(&mut self) -> u64 { ... }
    pub fn next_prop_id(&mut self) -> u64 { ... }
    pub fn next_token_id(&mut self) -> u32 { ... }
}
```

### 3. Update module structure

```
storage/
  mod.rs              # add pub mod header; pub mod database;
  pager_bridge.rs     # from task 001
  page.rs             # from task 001
  header.rs           # NEW
  database.rs         # NEW
```

## Tests

### Test 1: `test_create_graph_database`
- Create a new graph DB at a temp path
- Verify header page has correct magic bytes
- Verify all store root pages were allocated (non-zero)
- Close and reopen, verify header still valid

### Test 2: `test_open_existing_graph`
- Create a graph DB, close it
- Open it again, verify header matches

### Test 3: `test_open_invalid_file`
- Create a random file, try to open as graph DB
- Should return error (bad magic)

### Test 4: `test_header_roundtrip`
- Create GraphHeader with known values
- Write to a byte buffer, read back, verify all fields

### Test 5: `test_auto_increment_ids`
- Create graph DB, call next_node_id() 100 times
- Verify IDs are sequential (0, 1, 2, ..., 99)
- Close, reopen, verify next_node_id() returns 100

### Test 6: `test_store_root_pages_initialized`
- Create graph DB
- Read each root page, verify page type header is correct
- Node store root should have PageType::NodeStore, etc.

## Verification

```bash
cd libsql-graph && cargo test
```

All tests pass. Previous task 001 tests still pass.

## Handoff

Update `tasks/STATUS.md` with completion status and notes.
Next task: `003-node-store.md`
