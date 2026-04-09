use libsql_graph::cursor::{bfs, RelChainCursor};
use libsql_graph::prelude::*;
use libsql_graph::storage::node_store::NodeStore;
use libsql_graph::storage::rel_store::RelStore;

const PAGE_SIZE: u32 = 65536;

fn temp_path() -> String {
    let f = tempfile::NamedTempFile::new().unwrap();
    let p = f.path().to_str().unwrap().to_string();
    drop(f);
    p
}

fn build_chain_and_hub() -> (
    GraphEngine<libsql_graph::storage::pager_bridge::FilePager>,
    String,
) {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, PAGE_SIZE).unwrap();

    for i in 0u64..100 {
        let id = engine.create_node("Person").unwrap();
        assert_eq!(id, i);
        engine
            .set_node_property(i, "name", PropertyValue::ShortString(format!("P{}", i)))
            .unwrap();
    }

    for i in 0u64..99 {
        engine.create_relationship(i, i + 1, "FOLLOWS").unwrap();
    }

    let hub_id = engine.create_node("Person").unwrap();
    assert_eq!(hub_id, 100);
    engine
        .set_node_property(hub_id, "name", PropertyValue::ShortString("Hub".into()))
        .unwrap();

    for i in 0u64..100 {
        engine.create_relationship(hub_id, i, "KNOWS").unwrap();
    }

    assert_eq!(engine.node_count(), 101);
    assert_eq!(engine.edge_count(), 199);

    (engine, path)
}

#[test]
fn test_get_neighbors_head_of_chain() {
    let (mut engine, path) = build_chain_and_hub();

    let neighbors = engine.get_neighbors(0, Direction::Outgoing).unwrap();
    let neighbor_ids: Vec<u64> = neighbors.iter().map(|(id, _)| *id).collect();
    assert!(
        neighbor_ids.contains(&1),
        "node 0 outgoing should contain node 1, got {:?}",
        neighbor_ids
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_get_neighbors_mid_chain_both() {
    let (mut engine, path) = build_chain_and_hub();

    let neighbors = engine.get_neighbors(50, Direction::Both).unwrap();
    let neighbor_ids: Vec<u64> = neighbors.iter().map(|(id, _)| *id).collect();
    assert!(
        neighbor_ids.contains(&49),
        "node 50 Both should contain predecessor 49, got {:?}",
        neighbor_ids
    );
    assert!(
        neighbor_ids.contains(&51),
        "node 50 Both should contain successor 51, got {:?}",
        neighbor_ids
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_hub_supernode_outgoing() {
    let (mut engine, path) = build_chain_and_hub();

    let neighbors = engine.get_neighbors(100, Direction::Outgoing).unwrap();
    let neighbor_ids: Vec<u64> = neighbors.iter().map(|(id, _)| *id).collect();
    assert_eq!(
        neighbor_ids.len(),
        100,
        "hub node should have 100 outgoing KNOWS neighbors, got {}",
        neighbor_ids.len()
    );

    for i in 0u64..100 {
        assert!(
            neighbor_ids.contains(&i),
            "hub outgoing neighbors missing node {}",
            i
        );
    }

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_two_hop_cypher_query() {
    let (mut engine, path) = build_chain_and_hub();

    let result = engine
        .query("MATCH (a:Person {name: 'P0'})-[:FOLLOWS]->(b)-[:FOLLOWS]->(c) RETURN c.name")
        .unwrap();
    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 row, got {}",
        result.rows.len()
    );
    assert_eq!(
        result.rows[0][0],
        libsql_graph::cypher::executor::Value::String("P2".into()),
        "two-hop from P0 should reach P2"
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_follow_chain_five_hops() {
    let (mut engine, path) = build_chain_and_hub();

    let node_store = NodeStore::new(engine.db().header().node_store_root, PAGE_SIZE as usize);
    let rel_store = RelStore::new(engine.db().header().rel_store_root, PAGE_SIZE as usize);

    let follows_token = engine.get_or_create_rel_type("FOLLOWS").unwrap();

    let mut current = 0u64;
    for hop in 0..5 {
        let node = node_store.read_node(engine.db().pager(), current).unwrap();
        assert!(
            !node.first_rel.is_null(),
            "node {} should have relationships at hop {}",
            current,
            hop
        );
        let anchor = node_store.address(current);
        let mut cursor = RelChainCursor::new(node.first_rel, anchor, Direction::Outgoing)
            .with_type_filter(follows_token);
        let neighbors = cursor
            .collect_neighbors(engine.db().pager(), &rel_store, &node_store)
            .unwrap();

        assert!(
            !neighbors.is_empty(),
            "hop {} from node {} should have outgoing FOLLOWS neighbor",
            hop,
            current
        );
        current = neighbors[0].neighbor_id;
    }
    assert_eq!(
        current, 5,
        "following FOLLOWS 5 times from P0 should reach P5, got P{}",
        current
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_delete_relationship_breaks_chain() {
    let (mut engine, path) = build_chain_and_hub();

    let neighbors_of_50 = engine.get_neighbors(50, Direction::Outgoing).unwrap();
    let follows_rel_addr = neighbors_of_50
        .iter()
        .find(|(id, _)| *id == 51)
        .map(|(_, addr)| *addr)
        .expect("node 50 should have outgoing edge to 51");

    let rel_store = RelStore::new(engine.db().header().rel_store_root, PAGE_SIZE as usize);
    let rel_50_51 = rel_store
        .read_rel_at(engine.db().pager(), follows_rel_addr)
        .unwrap();
    let follows_token = rel_50_51.type_token_id;

    let rpp = rel_store.records_per_page() as u64;
    let store_root = rel_store.address(0).page;
    let rel_id = (follows_rel_addr.page - store_root) as u64 * rpp + follows_rel_addr.slot as u64;

    engine.delete_relationship(rel_id).unwrap();

    let neighbors_after = engine.get_neighbors(50, Direction::Outgoing).unwrap();
    let still_has_51 = neighbors_after.iter().any(|(id, _)| *id == 51);
    assert!(
        !still_has_51,
        "after deleting the P50->P51 relationship, P50 should not have P51 as outgoing neighbor"
    );

    let node_store = NodeStore::new(engine.db().header().node_store_root, PAGE_SIZE as usize);
    let rel_store2 = RelStore::new(engine.db().header().rel_store_root, PAGE_SIZE as usize);

    let mut reachable_via_follows = std::collections::HashSet::new();
    let mut frontier = vec![0u64];
    while let Some(nid) = frontier.pop() {
        if !reachable_via_follows.insert(nid) {
            continue;
        }
        let node = node_store.read_node(engine.db().pager(), nid).unwrap();
        if !node.in_use() || node.first_rel.is_null() {
            continue;
        }
        let anchor = node_store.address(nid);
        let mut cursor = RelChainCursor::new(node.first_rel, anchor, Direction::Outgoing)
            .with_type_filter(follows_token);
        let nbrs = cursor
            .collect_neighbors(engine.db().pager(), &rel_store2, &node_store)
            .unwrap();
        for entry in nbrs {
            if !reachable_via_follows.contains(&entry.neighbor_id) {
                frontier.push(entry.neighbor_id);
            }
        }
    }

    assert!(
        !reachable_via_follows.contains(&99),
        "after breaking P50->P51, P99 should NOT be reachable from P0 via FOLLOWS chain"
    );
    assert!(
        reachable_via_follows.contains(&50),
        "P50 should still be reachable from P0 via FOLLOWS chain"
    );
    assert!(
        !reachable_via_follows.contains(&51),
        "P51 should NOT be reachable from P0 via FOLLOWS chain after break"
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_bfs_reaches_all_chain_nodes() {
    let (mut engine, path) = build_chain_and_hub();

    let node_store = NodeStore::new(engine.db().header().node_store_root, PAGE_SIZE as usize);
    let rel_store = RelStore::new(engine.db().header().rel_store_root, PAGE_SIZE as usize);

    let result = bfs(
        engine.db().pager(),
        &node_store,
        &rel_store,
        0,
        200,
        Direction::Outgoing,
    )
    .unwrap();

    let ids: std::collections::HashSet<u64> = result.iter().map(|(id, _)| *id).collect();
    for i in 0u64..100 {
        assert!(
            ids.contains(&i),
            "BFS from 0 outgoing should reach node {}",
            i
        );
    }

    drop(engine);
    let _ = std::fs::remove_file(&path);
}
