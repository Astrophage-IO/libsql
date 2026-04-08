# Task 003: Node Store (Alloc/Read/Write/Delete)

**Status**: pending
**Depends on**: 002 (header page, GraphDatabase)
**Design doc**: `docs/plans/2026-04-08-graph-engine-design.md` -- Section 4.1 "Node Record"

## Objective

Implement the node record store: fixed-size 64-byte node records packed into pages, with allocation, read, write, and delete operations.

## Record Format (64 bytes)

```
Offset  Size  Field
0       1     flags              [inUse:1][dense:1][hasInlineProps:1][reserved:5]
1       3     label_token_id     primary label (24-bit)
4       4     first_rel_page     page number of first relationship
8       2     first_rel_slot     slot within that page
10      4     first_prop_page    page of first property record
14      2     first_prop_slot    slot within that page
16      4     extra_labels_page  page of additional labels (0 if single-label)
20      2     extra_labels_slot
22      2     rel_count          relationship count
24      40    inline_properties  up to 40 bytes of inlined property data
```

## Deliverables

### 1. Node Record (`storage/node_store.rs`)

```rust
pub const NODE_RECORD_SIZE: usize = 64;

pub struct NodeRecord {
    pub flags: u8,
    pub label_token_id: u32,        // only 24 bits used
    pub first_rel: RecordAddress,   // (page, slot) of first relationship
    pub first_prop: RecordAddress,  // (page, slot) of first property
    pub extra_labels: RecordAddress,
    pub rel_count: u16,
    pub inline_properties: [u8; 40],
}

impl NodeRecord {
    pub fn new(label_token_id: u32) -> Self;
    pub fn is_in_use(&self) -> bool;
    pub fn is_dense(&self) -> bool;
    pub fn from_bytes(data: &[u8]) -> Self;
    pub fn to_bytes(&self, buf: &mut [u8]);
}
```

### 2. Node Store (`storage/node_store.rs`)

```rust
pub struct NodeStore {
    pager: /* shared ref to GraphPager */,
    root_page: u32,
    page_size: usize,
    records_per_page: usize,
}

impl NodeStore {
    pub fn new(pager: ..., root_page: u32, page_size: usize) -> Self;
    
    /// Compute page:slot address from node ID
    pub fn address(&self, node_id: u64) -> RecordAddress;
    
    /// Read a node record by ID. O(1).
    pub fn read(&self, node_id: u64) -> Result<NodeRecord, GraphError>;
    
    /// Write a node record at the given ID. O(1).
    pub fn write(&self, node_id: u64, record: &NodeRecord) -> Result<(), GraphError>;
    
    /// Allocate a new node. Gets next ID from header, ensures page exists.
    pub fn alloc(&self, header: &mut GraphHeader) -> Result<(u64, RecordAddress), GraphError>;
    
    /// Mark a node as deleted (set inUse=0). Does NOT free the slot yet.
    pub fn delete(&self, node_id: u64) -> Result<(), GraphError>;
}
```

### 3. RecordAddress helper

```rust
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RecordAddress {
    pub page: u32,
    pub slot: u16,
}

impl RecordAddress {
    pub const NULL: Self = Self { page: 0, slot: 0 };
    pub fn is_null(&self) -> bool { self.page == 0 && self.slot == 0 }
    pub fn byte_offset(slot: u16, record_size: usize) -> usize {
        PAGE_HEADER_SIZE + slot as usize * record_size
    }
}
```

## Tests

### Test 1: `test_node_record_roundtrip`
- Create a NodeRecord with known values
- Serialize to bytes, deserialize, verify all fields match

### Test 2: `test_node_store_alloc_and_read`
- Create GraphDatabase, get NodeStore
- Alloc a node with label_token_id=1
- Read it back, verify fields

### Test 3: `test_node_store_multiple_nodes`
- Alloc 200 nodes (fills ~3 pages at 63 nodes/page)
- Read each back, verify correct label and ID

### Test 4: `test_node_store_delete`
- Alloc a node, verify in_use=true
- Delete it, verify in_use=false
- Read it, verify still readable but not in_use

### Test 5: `test_node_store_persistence`
- Alloc 50 nodes, commit, close database
- Reopen, read all 50, verify data intact

### Test 6: `test_node_address_calculation`
- For page_size=4096, verify:
  - node 0 -> page=root, slot=0
  - node 62 -> page=root, slot=62
  - node 63 -> page=root+1, slot=0
  - node 126 -> page=root+1, slot=63... etc

### Test 7: `test_node_inline_properties`
- Create node with 40 bytes of inline property data
- Read back, verify inline_properties match exactly

## Verification

```bash
cd libsql-graph && cargo test
```

All tests pass. All previous tests still pass.

## Handoff

Update `tasks/STATUS.md`. Next task: `004-relationship-store.md`
