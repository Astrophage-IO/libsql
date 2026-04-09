use libsql_graph::prelude::*;
use libsql_graph::integrity::{check_integrity, IntegrityError};

fn temp_path() -> String {
    let f = tempfile::NamedTempFile::new().unwrap();
    let p = f.path().to_str().unwrap().to_string();
    drop(f);
    p
}

#[test]
fn test_cycle_delete_middle_node() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    for _ in 0..10 {
        engine.create_node("Ring").unwrap();
    }
    for i in 0u64..10 {
        engine.create_relationship(i, (i + 1) % 10, "NEXT").unwrap();
    }
    assert_eq!(engine.node_count(), 10);
    assert_eq!(engine.edge_count(), 10);

    engine.detach_delete_node(5).unwrap();

    assert_eq!(engine.node_count(), 9);
    assert!(engine.edge_count() <= 8);

    let node5 = engine.get_node(5).unwrap();
    assert!(!node5.in_use());

    let n4_out = engine.get_neighbors(4, Direction::Outgoing).unwrap();
    assert!(
        !n4_out.iter().any(|(id, _)| *id == 5),
        "node 4 should no longer point to deleted node 5"
    );

    let n6_in = engine.get_neighbors(6, Direction::Incoming).unwrap();
    assert!(
        !n6_in.iter().any(|(id, _)| *id == 5),
        "node 6 should no longer have incoming from deleted node 5"
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_star_pattern_delete_spokes() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.create_node("Hub").unwrap();
    for _ in 0..10 {
        engine.create_node("Spoke").unwrap();
    }
    for spoke in 1u64..=10 {
        engine.create_relationship(0, spoke, "CONNECTS").unwrap();
    }

    let initial_neighbors = engine.get_neighbors(0, Direction::Outgoing).unwrap();
    assert_eq!(initial_neighbors.len(), 10);

    for spoke in 1u64..=5 {
        engine.detach_delete_node(spoke).unwrap();
    }

    let remaining = engine.get_neighbors(0, Direction::Outgoing).unwrap();
    assert_eq!(
        remaining.len(),
        5,
        "hub should have exactly 5 remaining neighbors, got {}",
        remaining.len()
    );

    let hub = engine.get_node(0).unwrap();
    assert!(hub.in_use());

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_delete_single_relationship() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.create_node("A").unwrap();
    engine.create_node("B").unwrap();
    engine.create_node("C").unwrap();
    engine.create_relationship(0, 1, "R1").unwrap();
    engine.create_relationship(1, 2, "R2").unwrap();
    assert_eq!(engine.edge_count(), 2);

    engine.delete_relationship(0).unwrap();

    assert_eq!(engine.edge_count(), 1);

    let a = engine.get_node(0).unwrap();
    assert!(a.in_use(), "endpoint A must still exist");
    let b = engine.get_node(1).unwrap();
    assert!(b.in_use(), "endpoint B must still exist");
    let c = engine.get_node(2).unwrap();
    assert!(c.in_use(), "endpoint C must still exist");

    let a_out = engine.get_neighbors(0, Direction::Outgoing).unwrap();
    assert!(a_out.is_empty(), "A should have no outgoing rels");

    let b_out = engine.get_neighbors(1, Direction::Outgoing).unwrap();
    assert_eq!(b_out.len(), 1, "B should still have its outgoing rel to C");

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_delete_all_nodes_one_by_one() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    for _ in 0..10 {
        engine.create_node("Ephemeral").unwrap();
    }
    for i in 0u64..9 {
        engine.create_relationship(i, i + 1, "CHAIN").unwrap();
    }
    assert_eq!(engine.node_count(), 10);
    assert_eq!(engine.edge_count(), 9);

    for id in 0u64..10 {
        engine.detach_delete_node(id).unwrap();
    }

    assert_eq!(engine.node_count(), 0);
    assert_eq!(engine.edge_count(), 0);

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_delete_already_deleted_node() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.create_node("Temp").unwrap();
    engine.detach_delete_node(0).unwrap();
    assert_eq!(engine.node_count(), 0);

    let result = engine.detach_delete_node(0);
    assert!(result.is_ok(), "deleting already-deleted node should be a no-op, got: {:?}", result);

    assert_eq!(engine.node_count(), 0);

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_cypher_detach_delete() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.query("CREATE (p:Person {name: 'ToDelete'})").unwrap();
    engine.query("CREATE (q:Person {name: 'Keeper'})").unwrap();
    engine.create_relationship(0, 1, "KNOWS").unwrap();
    assert_eq!(engine.node_count(), 2);
    assert_eq!(engine.edge_count(), 1);

    let result = engine
        .query("MATCH (p:Person {name: 'ToDelete'}) DETACH DELETE p")
        .unwrap();
    assert_eq!(result.stats.nodes_deleted, 1);
    assert_eq!(engine.node_count(), 1);
    assert_eq!(engine.edge_count(), 0);

    let remaining = engine
        .query("MATCH (p:Person) RETURN p.name")
        .unwrap();
    assert_eq!(remaining.rows.len(), 1);

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_integrity_after_heavy_deletes() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    for _ in 0..20 {
        engine.create_node("Node").unwrap();
    }
    for i in 0u64..19 {
        engine.create_relationship(i, i + 1, "LINK").unwrap();
    }
    engine.create_relationship(19, 0, "LINK").unwrap();

    for id in (0u64..20).step_by(3) {
        engine.detach_delete_node(id).unwrap();
    }

    let report = check_integrity(&mut engine).unwrap();
    let real_errors: Vec<_> = report
        .errors
        .iter()
        .filter(|e| !matches!(e, IntegrityError::CountMismatch { .. }))
        .collect();
    assert!(
        real_errors.is_empty(),
        "integrity errors (excluding CountMismatch): {:?}",
        real_errors
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_close_reopen_after_deletes() {
    let path = temp_path();

    {
        let mut engine = GraphEngine::create(&path, 4096).unwrap();

        for _ in 0..10 {
            engine.create_node("Persistent").unwrap();
        }
        for i in 0u64..9 {
            engine.create_relationship(i, i + 1, "SEQ").unwrap();
        }

        engine.detach_delete_node(3).unwrap();
        engine.detach_delete_node(7).unwrap();
        engine.delete_relationship(0).unwrap();

        assert_eq!(engine.node_count(), 8);
    }

    {
        let mut engine = GraphEngine::open(&path).unwrap();

        assert_eq!(engine.node_count(), 8);

        let n3 = engine.get_node(3).unwrap();
        assert!(!n3.in_use(), "node 3 should still be deleted after reopen");

        let n7 = engine.get_node(7).unwrap();
        assert!(!n7.in_use(), "node 7 should still be deleted after reopen");

        let report = check_integrity(&mut engine).unwrap();
        let real_errors: Vec<_> = report
            .errors
            .iter()
            .filter(|e| !matches!(e, IntegrityError::CountMismatch { .. }))
            .collect();
        assert!(
            real_errors.is_empty(),
            "integrity errors after reopen (excluding CountMismatch): {:?}",
            real_errors
        );
    }

    let _ = std::fs::remove_file(&path);
}
