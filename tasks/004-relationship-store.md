# Task 004: Relationship Store + Doubly-Linked Chains

**Status**: pending
**Depends on**: 003 (node store)
**Design doc**: `docs/plans/2026-04-08-graph-engine-design.md` -- Section 4.2, 5 "Index-Free Adjacency"

## Objective

Implement the relationship record store with doubly-linked chain operations. This is the core of index-free adjacency -- the most critical data structure in the engine.

## Record Format (64 bytes)

```
Offset  Size  Field
0       1     flags              [inUse:1][firstInSrcChain:1][firstInDstChain:1][reserved:5]
1       3     type_token_id      relationship type (24-bit)
4       4     source_node_page
8       2     source_node_slot
10      4     target_node_page
14      2     target_node_slot
16      4     src_prev_rel_page
20      2     src_prev_rel_slot
22      4     src_next_rel_page
26      2     src_next_rel_slot
28      4     dst_prev_rel_page
32      2     dst_prev_rel_slot
34      4     dst_next_rel_page
38      2     dst_next_rel_slot
40      4     first_prop_page
44      2     first_prop_slot
46      18    inline_properties
```

## Deliverables

### 1. Relationship Record + Store

```rust
pub const REL_RECORD_SIZE: usize = 64;

pub struct RelRecord {
    pub flags: u8,
    pub type_token_id: u32,
    pub source_node: RecordAddress,
    pub target_node: RecordAddress,
    pub src_prev_rel: RecordAddress,
    pub src_next_rel: RecordAddress,
    pub dst_prev_rel: RecordAddress,
    pub dst_next_rel: RecordAddress,
    pub first_prop: RecordAddress,
    pub inline_properties: [u8; 18],
}

pub struct RelStore { ... } // same pattern as NodeStore
```

### 2. Chain Operations (THE CRITICAL PART)

```rust
impl RelStore {
    /// Insert a new relationship between source and target nodes.
    /// This performs HEAD INSERTION into both chains:
    ///   1. Alloc new rel record
    ///   2. Set new_rel.src_next = source_node.first_rel
    ///   3. If old head exists, set old_head.src_prev = new_rel
    ///   4. Set source_node.first_rel = new_rel
    ///   5. Repeat for target chain (dst_next/dst_prev)
    ///   6. Increment rel_count on both nodes
    /// All operations are O(1).
    pub fn insert(
        &self,
        source_node_id: u64,
        target_node_id: u64,
        type_token_id: u32,
        node_store: &NodeStore,
        header: &mut GraphHeader,
    ) -> Result<u64, GraphError>;  // returns rel_id
    
    /// Delete a relationship by relinking its neighbors in both chains.
    /// O(1) -- no chain traversal needed thanks to doubly-linked list.
    pub fn delete(
        &self,
        rel_id: u64,
        node_store: &NodeStore,
    ) -> Result<(), GraphError>;
    
    /// Iterate all relationships of a node (outgoing, incoming, or both).
    /// Returns a RelChainIterator that follows the linked list.
    pub fn iter_node_rels(
        &self,
        node: &NodeRecord,
        node_addr: RecordAddress,
        direction: Direction,
    ) -> RelChainIterator;
}

pub enum Direction { Outgoing, Incoming, Both }

pub struct RelChainIterator {
    rel_store: /* ref */,
    current: RecordAddress,
    anchor_node: RecordAddress,
    direction: Direction,
}

impl Iterator for RelChainIterator {
    type Item = Result<(u64, RelRecord), GraphError>;
    fn next(&mut self) -> Option<Self::Item> {
        // Follow src_next or dst_next depending on which chain we're on
    }
}
```

## Tests

### Test 1: `test_rel_record_roundtrip`
- Serialize/deserialize, verify all fields

### Test 2: `test_insert_single_relationship`
- Create 2 nodes (A, B), insert rel A->B
- Verify A.first_rel points to the new rel
- Verify B.first_rel points to the new rel
- Verify rel.source = A, rel.target = B

### Test 3: `test_insert_multiple_rels_same_source`
- Create node A, nodes B, C, D
- Insert A->B, A->C, A->D
- Iterate A's outgoing rels, verify all 3 found
- Verify chain order (head insertion: D, C, B)

### Test 4: `test_bidirectional_chains`
- Create A->B and C->B
- Iterate B's incoming rels, verify both A and C found
- Iterate A's outgoing, verify B found
- Iterate C's outgoing, verify B found

### Test 5: `test_delete_middle_of_chain`
- Insert A->B, A->C, A->D (chain: D<->C<->B)
- Delete the C relationship
- Iterate A's rels, verify only D and B remain
- Verify D.src_next = B and B.src_prev = D (relinked)

### Test 6: `test_delete_head_of_chain`
- Insert A->B, A->C (chain: C<->B)
- Delete C (the head)
- Verify A.first_rel now points to B
- Verify B.src_prev = NULL

### Test 7: `test_delete_only_rel`
- Insert A->B (single rel)
- Delete it
- Verify A.first_rel = NULL, B.first_rel = NULL

### Test 8: `test_chain_integrity_after_many_ops`
- Insert 100 rels from node A to nodes 1..100
- Delete every other one (odds)
- Iterate A's chain, verify exactly 50 remain
- Verify chain is fully connected (no broken links)

### Test 9: `test_persistence_of_chains`
- Insert several rels, commit, close
- Reopen, iterate chains, verify intact

### Test 10: `test_both_direction_iteration`
- Create a triangle: A->B, B->C, C->A
- From B, iterate outgoing: should find C
- From B, iterate incoming: should find A

## Verification

```bash
cd libsql-graph && cargo test
```

All tests pass. Chain integrity is the most critical thing to validate.

## Handoff

Update `tasks/STATUS.md`. Next task: `005-token-store.md`
