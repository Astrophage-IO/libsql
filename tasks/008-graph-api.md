# Task 008: Basic Graph API

**Status**: pending
**Depends on**: 003, 004, 005, 007 (all stores)

## Objective

Wire all stores together into a high-level `Graph` API that supports creating nodes, creating relationships, reading neighbors, and reading properties. This is the public API that the Cypher executor will call.

## Deliverables

```rust
pub struct Graph {
    db: GraphDatabase,
    node_store: NodeStore,
    rel_store: RelStore,
    prop_store: PropertyStore,
    token_store: TokenStore,
    freespace: FreeSpaceManager,
}

impl Graph {
    pub fn create(path: &str) -> Result<Self>;
    pub fn open(path: &str) -> Result<Self>;
    
    // Node operations
    pub fn create_node(&mut self, label: &str, props: &[(&str, PropertyValue)]) -> Result<NodeId>;
    pub fn get_node(&self, id: NodeId) -> Result<Node>;
    pub fn delete_node(&mut self, id: NodeId) -> Result<()>; // must have no rels
    pub fn detach_delete_node(&mut self, id: NodeId) -> Result<()>; // delete rels first
    
    // Relationship operations  
    pub fn create_relationship(&mut self, src: NodeId, dst: NodeId, rel_type: &str, props: &[(&str, PropertyValue)]) -> Result<RelId>;
    pub fn get_relationship(&self, id: RelId) -> Result<Relationship>;
    pub fn delete_relationship(&mut self, id: RelId) -> Result<()>;
    
    // Traversal
    pub fn get_neighbors(&self, node_id: NodeId, direction: Direction, rel_type: Option<&str>) -> Result<Vec<NodeId>>;
    pub fn get_relationships(&self, node_id: NodeId, direction: Direction, rel_type: Option<&str>) -> Result<Vec<Relationship>>;
    
    // Properties
    pub fn set_property(&mut self, node_id: NodeId, key: &str, value: PropertyValue) -> Result<()>;
    pub fn get_property(&self, node_id: NodeId, key: &str) -> Result<Option<PropertyValue>>;
    
    // Transaction
    pub fn begin(&mut self) -> Result<()>;
    pub fn commit(&mut self) -> Result<()>;
    pub fn rollback(&mut self) -> Result<()>;
    
    // Stats
    pub fn node_count(&self) -> u64;
    pub fn edge_count(&self) -> u64;
}
```

## Tests

1. `test_create_and_read_node` -- full round-trip with label and properties
2. `test_create_relationship_and_traverse` -- A->B, get_neighbors(A, Out) returns [B]
3. `test_social_network_small` -- create 10 people, 20 KNOWS edges, traverse 2 hops from one
4. `test_detach_delete` -- create node with 5 rels, detach_delete, verify all rels gone
5. `test_delete_node_with_rels_fails` -- delete_node should fail if rels exist
6. `test_get_neighbors_by_type` -- A->B (KNOWS), A->C (WORKS_AT), filter by type
7. `test_bidirectional_traversal` -- get incoming vs outgoing vs both
8. `test_transaction_commit_persist` -- create, commit, reopen, verify
9. `test_transaction_rollback` -- create, rollback, verify nothing persisted
10. `test_1000_nodes_10000_edges` -- stress test, verify chain integrity

## Verification

```bash
cd libsql-graph && cargo test
```

ALL tests from tasks 001-008 pass. This completes Phase 1.

## Handoff

Update STATUS.md: mark Phase 1 complete. Next: Phase 2 tasks (Cypher parser).
