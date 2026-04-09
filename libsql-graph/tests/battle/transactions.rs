use libsql_graph::prelude::*;
use libsql_graph::{GraphError, TransactionBatch};

fn temp_path() -> String {
    let f = tempfile::NamedTempFile::new().unwrap();
    let p = f.path().to_str().unwrap().to_string();
    drop(f);
    p
}

#[test]
fn t1_commit_50_nodes() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.begin().unwrap();
    for i in 0..50 {
        let id = engine.create_node("Item").unwrap();
        assert_eq!(id, i);
    }
    engine.commit().unwrap();

    assert_eq!(engine.node_count(), 50);

    let result = engine.query("MATCH (n:Item) RETURN count(*)").unwrap();
    assert_eq!(result.rows[0][0], Value::Integer(50));

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t2_rollback_50_nodes() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.begin().unwrap();
    for _ in 0..50 {
        engine.create_node("Item").unwrap();
    }
    engine.commit().unwrap();
    assert_eq!(engine.node_count(), 50);

    engine.begin().unwrap();
    for _ in 0..50 {
        engine.create_node("Item").unwrap();
    }
    assert_eq!(engine.node_count(), 100);
    engine.rollback().unwrap();

    assert_eq!(engine.node_count(), 50);

    let result = engine.query("MATCH (n:Item) RETURN count(*)").unwrap();
    assert_eq!(result.rows[0][0], Value::Integer(50));

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t3_next_node_id_restored_after_rollback() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.begin().unwrap();
    for _ in 0..50 {
        engine.create_node("Item").unwrap();
    }
    engine.commit().unwrap();
    assert_eq!(engine.db().header().next_node_id, 50);

    engine.begin().unwrap();
    for _ in 0..50 {
        engine.create_node("Extra").unwrap();
    }
    assert_eq!(engine.db().header().next_node_id, 100);
    engine.rollback().unwrap();

    assert_eq!(
        engine.db().header().next_node_id, 50,
        "next_node_id should revert to 50 after rollback"
    );

    let new_id = engine.create_node("Post").unwrap();
    assert_eq!(new_id, 50, "first node after rollback should get id 50, not 100");
    assert_eq!(engine.node_count(), 51);

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t4_commit_nodes_rels_properties() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.begin().unwrap();

    let alice = engine.create_node("Person").unwrap();
    let bob = engine.create_node("Person").unwrap();
    let acme = engine.create_node("Company").unwrap();

    engine
        .set_node_property(alice, "name", PropertyValue::ShortString("Alice".into()))
        .unwrap();
    engine
        .set_node_property(bob, "name", PropertyValue::ShortString("Bob".into()))
        .unwrap();
    engine
        .set_node_property(acme, "name", PropertyValue::ShortString("Acme".into()))
        .unwrap();
    engine
        .set_node_property(alice, "age", PropertyValue::Int32(30))
        .unwrap();

    let r0 = engine.create_relationship(alice, bob, "KNOWS").unwrap();
    let _r1 = engine.create_relationship(alice, acme, "WORKS_AT").unwrap();
    engine.set_rel_property(r0, "since", PropertyValue::Int32(2020)).unwrap();

    engine.commit().unwrap();

    assert_eq!(engine.node_count(), 3);
    assert_eq!(engine.edge_count(), 2);

    let name = engine.get_node_property(alice, "name").unwrap();
    assert_eq!(name, Some(PropertyValue::ShortString("Alice".into())));

    let age = engine.get_node_property(alice, "age").unwrap();
    assert_eq!(age, Some(PropertyValue::Int32(30)));

    let result = engine
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, r.since, b.name")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("Alice".into()));
    assert_eq!(result.rows[0][1], Value::Integer(2020));
    assert_eq!(result.rows[0][2], Value::String("Bob".into()));

    let result = engine
        .query("MATCH (a:Person)-[:WORKS_AT]->(c:Company) RETURN a.name, c.name")
        .unwrap();
    assert_eq!(result.rows.len(), 1);

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t5_rollback_nodes_with_properties() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.begin().unwrap();
    for i in 0..20 {
        let id = engine.create_node("Temp").unwrap();
        engine
            .set_node_property(id, "idx", PropertyValue::Int32(i as i32))
            .unwrap();
        engine
            .set_node_property(id, "tag", PropertyValue::ShortString(format!("node-{i}")))
            .unwrap();
    }
    assert_eq!(engine.node_count(), 20);
    engine.rollback().unwrap();

    assert_eq!(engine.node_count(), 0);

    let result = engine.query("MATCH (n:Temp) RETURN count(*)").unwrap();
    assert_eq!(result.rows[0][0], Value::Integer(0));

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t6_transaction_batch_all_valid() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    let results = TransactionBatch::new(&mut engine)
        .add("CREATE (a:Hero {name: 'Thor'})")
        .add("CREATE (b:Hero {name: 'Loki'})")
        .add("CREATE (c:Hero {name: 'Odin'})")
        .execute()
        .unwrap();

    assert_eq!(results.len(), 3);
    for r in &results {
        assert_eq!(r.stats.nodes_created, 1);
    }
    assert_eq!(engine.node_count(), 3);

    let result = engine.query("MATCH (h:Hero) RETURN h.name ORDER BY h.name").unwrap();
    assert_eq!(result.rows.len(), 3);

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t7_transaction_batch_partial_failure_rolls_back() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.create_node("Existing").unwrap();
    assert_eq!(engine.node_count(), 1);

    let result = TransactionBatch::new(&mut engine)
        .add("CREATE (a:Villain {name: 'Thanos'})")
        .add("CREATE (b:Villain {name: 'Ultron'})")
        .add("THIS IS COMPLETELY INVALID CYPHER !!!")
        .execute();

    assert!(result.is_err(), "batch with invalid query must fail");
    if let Err(GraphError::QueryParse(_)) = &result {
    } else {
        panic!("expected QueryParse error, got: {:?}", result);
    }

    assert_eq!(
        engine.node_count(), 1,
        "node count must remain 1 — the two valid CREATEs should be rolled back"
    );

    assert_eq!(
        engine.db().header().next_node_id, 1,
        "next_node_id must revert to 1 — batch rollback must undo the id counter"
    );

    let result = engine.query("MATCH (n:Existing) RETURN count(*)").unwrap();
    assert_eq!(
        result.rows[0][0],
        Value::Integer(1),
        "only the pre-existing node should remain"
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t8_persistence_across_reopen() {
    let path = temp_path();

    {
        let mut engine = GraphEngine::create(&path, 4096).unwrap();

        engine.begin().unwrap();
        for i in 0..10 {
            let id = engine.create_node("Persistent").unwrap();
            engine
                .set_node_property(id, "val", PropertyValue::Int32(i as i32))
                .unwrap();
        }
        engine.create_relationship(0, 1, "LINKED").unwrap();
        engine.create_relationship(1, 2, "LINKED").unwrap();
        engine.commit().unwrap();

        assert_eq!(engine.node_count(), 10);
        assert_eq!(engine.edge_count(), 2);

        engine.begin().unwrap();
        for _ in 0..5 {
            engine.create_node("Ghost").unwrap();
        }
        assert_eq!(engine.node_count(), 15);
        engine.rollback().unwrap();
        assert_eq!(engine.node_count(), 10);

        drop(engine);
    }

    {
        let mut engine = GraphEngine::open(&path).unwrap();

        assert_eq!(engine.node_count(), 10, "committed nodes must survive reopen");
        assert_eq!(engine.edge_count(), 2, "committed edges must survive reopen");

        let result = engine.query("MATCH (n:Persistent) RETURN count(*)").unwrap();
        assert_eq!(result.rows[0][0], Value::Integer(10));

        let result = engine.query("MATCH (n) RETURN count(*)").unwrap();
        assert_eq!(
            result.rows[0][0],
            Value::Integer(10),
            "only 10 committed nodes should exist — no ghost nodes"
        );

        let val = engine.get_node_property(0, "val").unwrap();
        assert_eq!(val, Some(PropertyValue::Int32(0)));
        let val = engine.get_node_property(9, "val").unwrap();
        assert_eq!(val, Some(PropertyValue::Int32(9)));

        let result = engine
            .query("MATCH (a:Persistent)-[:LINKED]->(b:Persistent) RETURN a.val, b.val ORDER BY a.val")
            .unwrap();
        assert_eq!(result.rows.len(), 2);

        assert_eq!(
            engine.db().header().next_node_id, 10,
            "next_node_id should be 10 — rolled-back ghost nodes must not have advanced it"
        );

        let report = libsql_graph::integrity::check_integrity(&mut engine).unwrap();
        assert!(report.is_ok(), "integrity errors: {:?}", report.errors);

        drop(engine);
    }

    let _ = std::fs::remove_file(&path);
}
