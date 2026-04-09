use libsql_graph::prelude::*;
use libsql_graph::integrity::{check_integrity, IntegrityError};

fn temp_path() -> String {
    let f = tempfile::NamedTempFile::new().unwrap();
    let p = f.path().to_str().unwrap().to_string();
    drop(f);
    p
}

#[test]
fn test_hub_and_50_spokes() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    let hub = engine.create_node("Hub").unwrap();
    let mut spoke_ids = Vec::new();
    for i in 0..50 {
        let s = engine.create_node("Spoke").unwrap();
        spoke_ids.push(s);
        engine
            .create_relationship(hub, s, "FOLLOWS")
            .unwrap_or_else(|e| panic!("create_relationship hub->{i} failed: {e}"));
    }

    engine.convert_to_dense(hub).unwrap();
    assert!(engine.is_dense(hub).unwrap(), "hub should be dense after conversion");

    let neighbors = engine.get_neighbors(hub, Direction::Outgoing).unwrap();
    assert_eq!(
        neighbors.len(),
        50,
        "expected 50 outgoing neighbors, got {}",
        neighbors.len()
    );
    let neighbor_ids: Vec<u64> = neighbors.iter().map(|(id, _)| *id).collect();
    for s in &spoke_ids {
        assert!(
            neighbor_ids.contains(s),
            "spoke {} not found in dense neighbors",
            s
        );
    }

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_mixed_rel_types_dense_groups() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    let hub = engine.create_node("Hub").unwrap();
    let mut nodes = Vec::new();
    for _ in 0..35 {
        nodes.push(engine.create_node("Peer").unwrap());
    }

    for i in 0..20 {
        engine.create_relationship(hub, nodes[i], "FOLLOWS").unwrap();
    }
    for i in 20..35 {
        engine.create_relationship(nodes[i], hub, "KNOWS").unwrap();
    }
    for _ in 0..5 {
        engine.create_relationship(hub, hub, "SELF").unwrap();
    }

    engine.convert_to_dense(hub).unwrap();
    assert!(engine.is_dense(hub).unwrap());

    let groups = engine.get_dense_groups(hub).unwrap();
    assert!(
        !groups.is_empty(),
        "dense groups should not be empty after conversion"
    );

    let follows_token = engine.get_or_create_rel_type("FOLLOWS").unwrap();
    let knows_token = engine.get_or_create_rel_type("KNOWS").unwrap();
    let self_token = engine.get_or_create_rel_type("SELF").unwrap();

    let mut follows_out = 0u32;
    let mut knows_in = 0u32;
    let mut self_loop = 0u32;

    for &(type_id, out_count, in_count, loop_count) in &groups {
        if type_id == follows_token {
            follows_out += out_count;
        }
        if type_id == knows_token {
            knows_in += in_count;
        }
        if type_id == self_token {
            self_loop += loop_count;
        }
    }

    assert_eq!(follows_out, 20, "expected 20 FOLLOWS outgoing, got {follows_out}");
    assert_eq!(knows_in, 15, "expected 15 KNOWS incoming, got {knows_in}");
    assert_eq!(self_loop, 5, "expected 5 SELF loops, got {self_loop}");

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_cypher_query_after_dense_conversion() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    let hub = engine.create_node("Hub").unwrap();
    for _ in 0..20 {
        let s = engine.create_node("Spoke").unwrap();
        engine.create_relationship(hub, s, "FOLLOWS").unwrap();
    }
    for _ in 0..10 {
        let s = engine.create_node("Other").unwrap();
        engine.create_relationship(s, hub, "KNOWS").unwrap();
    }

    engine.convert_to_dense(hub).unwrap();

    let result = engine
        .query("MATCH (h:Hub)-[:FOLLOWS]->(s) RETURN count(s)")
        .unwrap();
    assert_eq!(
        result.rows.len(),
        1,
        "expected 1 result row, got {}",
        result.rows.len()
    );
    assert_eq!(
        result.rows[0][0],
        Value::Integer(20),
        "expected count 20 for FOLLOWS, got {:?}",
        result.rows[0][0]
    );

    let result2 = engine
        .query("MATCH (h:Hub)<-[:KNOWS]-(k) RETURN count(k)")
        .unwrap();
    assert_eq!(
        result2.rows[0][0],
        Value::Integer(10),
        "expected count 10 for incoming KNOWS, got {:?}",
        result2.rows[0][0]
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_properties_persist_through_dense_conversion() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    let hub = engine.create_node("Hub").unwrap();
    engine
        .set_node_property(hub, "name", PropertyValue::ShortString("central".into()))
        .unwrap();
    engine
        .set_node_property(hub, "rank", PropertyValue::Int32(1))
        .unwrap();

    for _ in 0..10 {
        let s = engine.create_node("Spoke").unwrap();
        engine.create_relationship(hub, s, "LINK").unwrap();
    }

    let name_before = engine.get_node_property(hub, "name").unwrap();
    let rank_before = engine.get_node_property(hub, "rank").unwrap();
    assert_eq!(name_before, Some(PropertyValue::ShortString("central".into())));
    assert_eq!(rank_before, Some(PropertyValue::Int32(1)));

    engine.convert_to_dense(hub).unwrap();
    assert!(engine.is_dense(hub).unwrap());

    let name_after = engine.get_node_property(hub, "name").unwrap();
    let rank_after = engine.get_node_property(hub, "rank").unwrap();
    assert_eq!(
        name_after,
        Some(PropertyValue::ShortString("central".into())),
        "name property lost after dense conversion"
    );
    assert_eq!(
        rank_after,
        Some(PropertyValue::Int32(1)),
        "rank property lost after dense conversion"
    );

    engine
        .set_node_property(hub, "score", PropertyValue::Int32(99))
        .unwrap();
    let score = engine.get_node_property(hub, "score").unwrap();
    assert_eq!(score, Some(PropertyValue::Int32(99)), "post-conversion property set failed");

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_double_convert_is_noop() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    let hub = engine.create_node("Hub").unwrap();
    for _ in 0..10 {
        let s = engine.create_node("Spoke").unwrap();
        engine.create_relationship(hub, s, "FOLLOWS").unwrap();
    }

    engine.convert_to_dense(hub).unwrap();
    assert!(engine.is_dense(hub).unwrap());

    let groups_first = engine.get_dense_groups(hub).unwrap();
    let neighbors_first = engine.get_neighbors(hub, Direction::Outgoing).unwrap();

    engine.convert_to_dense(hub).unwrap();
    assert!(engine.is_dense(hub).unwrap(), "still dense after second convert");

    let groups_second = engine.get_dense_groups(hub).unwrap();
    let neighbors_second = engine.get_neighbors(hub, Direction::Outgoing).unwrap();

    assert_eq!(
        groups_first, groups_second,
        "groups changed after double conversion"
    );
    assert_eq!(
        neighbors_first.len(),
        neighbors_second.len(),
        "neighbor count changed after double conversion"
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_dense_persists_across_close_reopen() {
    let path = temp_path();

    {
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        let hub = engine.create_node("Hub").unwrap();
        for _ in 0..15 {
            let s = engine.create_node("Spoke").unwrap();
            engine.create_relationship(hub, s, "FOLLOWS").unwrap();
        }
        engine.convert_to_dense(hub).unwrap();
        assert!(engine.is_dense(hub).unwrap());
        drop(engine);
    }

    {
        let mut engine = GraphEngine::open(&path).unwrap();
        assert!(
            engine.is_dense(0).unwrap(),
            "dense flag did not persist after close/reopen"
        );

        let neighbors = engine.get_neighbors(0, Direction::Outgoing).unwrap();
        assert_eq!(
            neighbors.len(),
            15,
            "expected 15 neighbors after reopen, got {}",
            neighbors.len()
        );

        let result = engine
            .query("MATCH (h:Hub)-[:FOLLOWS]->(s) RETURN count(s)")
            .unwrap();
        assert_eq!(
            result.rows[0][0],
            Value::Integer(15),
            "cypher count wrong after reopen: {:?}",
            result.rows[0][0]
        );

        drop(engine);
    }

    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_integrity_check_dense_node() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    let hub = engine.create_node("Hub").unwrap();
    for _ in 0..20 {
        let s = engine.create_node("Spoke").unwrap();
        engine.create_relationship(hub, s, "FOLLOWS").unwrap();
    }

    engine.convert_to_dense(hub).unwrap();
    assert!(engine.is_dense(hub).unwrap());

    let report = check_integrity(&mut engine).unwrap();

    let non_count_errors: Vec<&IntegrityError> = report
        .errors
        .iter()
        .filter(|e| !matches!(e, IntegrityError::CountMismatch { node_id, .. } if *node_id == hub))
        .collect();
    assert!(
        non_count_errors.is_empty(),
        "unexpected integrity errors (excluding hub CountMismatch): {:?}",
        non_count_errors
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}
