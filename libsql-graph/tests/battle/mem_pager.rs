use libsql_graph::cypher::executor::Value;
use libsql_graph::graph::{Direction, GraphEngine, TransactionBatch};
use libsql_graph::storage::database::GraphDatabase;
use libsql_graph::storage::mem_pager::MemPager;
use libsql_graph::storage::pager::Pager;
use libsql_graph::storage::property_store::PropertyValue;

fn mem_engine() -> GraphEngine<MemPager> {
    let pager = MemPager::new(4096);
    let db = GraphDatabase::from_pager(pager, 4096).unwrap();
    GraphEngine::from_database(db)
}

#[test]
fn test_create_50_nodes_with_properties() {
    let mut engine = mem_engine();

    for i in 0..50u64 {
        let label = if i % 2 == 0 { "Person" } else { "Company" };
        let nid = engine.create_node(label).unwrap();
        assert_eq!(nid, i);

        engine
            .set_node_property(
                nid,
                "name",
                PropertyValue::ShortString(format!("node_{}", i)),
            )
            .unwrap();
        engine
            .set_node_property(nid, "index", PropertyValue::Int32(i as i32))
            .unwrap();
    }

    assert_eq!(engine.node_count(), 50);

    for i in 0..50u64 {
        let name = engine.get_node_property(i, "name").unwrap();
        assert_eq!(
            name,
            Some(PropertyValue::ShortString(format!("node_{}", i))),
            "name mismatch on node {}",
            i
        );
        let idx = engine.get_node_property(i, "index").unwrap();
        assert_eq!(
            idx,
            Some(PropertyValue::Int32(i as i32)),
            "index mismatch on node {}",
            i
        );
    }

    println!("[PASS] 50 nodes created with properties, all read back correctly");
}

#[test]
fn test_relationships_and_traversal() {
    let mut engine = mem_engine();

    for _ in 0..5 {
        engine.create_node("Node").unwrap();
    }
    // chain: 0->1->2->3->4
    for i in 0u64..4 {
        engine.create_relationship(i, i + 1, "NEXT").unwrap();
    }

    assert_eq!(engine.edge_count(), 4);

    let out_0 = engine.get_neighbors(0, Direction::Outgoing).unwrap();
    assert_eq!(out_0.len(), 1);
    assert_eq!(out_0[0].0, 1);

    let out_3 = engine.get_neighbors(3, Direction::Outgoing).unwrap();
    assert_eq!(out_3.len(), 1);
    assert_eq!(out_3[0].0, 4);

    let in_4 = engine.get_neighbors(4, Direction::Incoming).unwrap();
    assert_eq!(in_4.len(), 1);
    assert_eq!(in_4[0].0, 3);

    let both_2 = engine.get_neighbors(2, Direction::Both).unwrap();
    assert_eq!(both_2.len(), 2);

    let result = engine
        .query("MATCH (a:Node)-[:NEXT]->(b:Node) RETURN a, b")
        .unwrap();
    assert_eq!(result.rows.len(), 4);

    println!("[PASS] relationships created and traversal works on MemPager");
}

#[test]
fn test_cypher_create_match_where_return() {
    let mut engine = mem_engine();

    engine
        .query("CREATE (a:Person {name: 'Alice', age: 30})")
        .unwrap();
    engine
        .query("CREATE (b:Person {name: 'Bob', age: 25})")
        .unwrap();
    engine
        .query("CREATE (c:Person {name: 'Charlie', age: 35})")
        .unwrap();
    engine.query("CREATE (d:Company {name: 'Acme'})").unwrap();

    assert_eq!(engine.node_count(), 4);

    engine.create_relationship(0, 1, "KNOWS").unwrap();
    engine.create_relationship(0, 2, "KNOWS").unwrap();
    engine.create_relationship(1, 3, "WORKS_AT").unwrap();

    let result = engine
        .query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.columns, vec!["a.name", "b.name"]);

    let result = engine
        .query("MATCH (a:Person) WHERE a.age >= 30 RETURN a.name ORDER BY a.age DESC")
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::String("Charlie".into()));
    assert_eq!(result.rows[1][0], Value::String("Alice".into()));

    println!("[PASS] Cypher CREATE, MATCH, WHERE, RETURN all work on MemPager");
}

#[test]
fn test_cypher_aggregations() {
    let mut engine = mem_engine();

    engine
        .query("CREATE (a:Person {name: 'Alice', age: 30})")
        .unwrap();
    engine
        .query("CREATE (b:Person {name: 'Bob', age: 25})")
        .unwrap();
    engine
        .query("CREATE (c:Person {name: 'Charlie', age: 35})")
        .unwrap();

    engine.create_relationship(0, 1, "KNOWS").unwrap();
    engine.create_relationship(0, 2, "KNOWS").unwrap();

    let result = engine
        .query("MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name, count(b) AS friends")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1], Value::Integer(2));

    let result = engine.query("MATCH (a:Person) RETURN avg(a.age)").unwrap();
    assert_eq!(result.rows[0][0], Value::Float(30.0));

    let result = engine
        .query("MATCH (a:Person) RETURN min(a.age), max(a.age), sum(a.age)")
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Integer(25));
    assert_eq!(result.rows[0][1], Value::Integer(35));
    assert_eq!(result.rows[0][2], Value::Integer(90));

    let result = engine.query("MATCH (a:Person) RETURN count(*)").unwrap();
    assert_eq!(result.rows[0][0], Value::Integer(3));

    println!("[PASS] count, avg, min, max, sum all work on MemPager");
}

#[test]
fn test_transaction_rollback() {
    let mut engine = mem_engine();

    engine.create_node("Person").unwrap();
    engine.create_node("Person").unwrap();
    assert_eq!(engine.node_count(), 2);

    engine.begin().unwrap();
    engine.create_node("Person").unwrap();
    engine.create_node("Person").unwrap();
    engine.create_node("Person").unwrap();
    assert_eq!(engine.node_count(), 5);
    engine.rollback().unwrap();

    assert_eq!(
        engine.node_count(),
        2,
        "rollback must restore node_count to 2"
    );

    let node = engine.get_node(0).unwrap();
    assert!(node.in_use(), "pre-existing node 0 must survive rollback");
    let node = engine.get_node(1).unwrap();
    assert!(node.in_use(), "pre-existing node 1 must survive rollback");

    println!("[PASS] transaction rollback restores node count on MemPager");
}

#[test]
fn test_transaction_batch_atomic_commit() {
    let mut engine = mem_engine();

    let results = TransactionBatch::new(&mut engine)
        .add("CREATE (a:Person {name: 'Alice', age: 30})")
        .add("CREATE (b:Person {name: 'Bob', age: 25})")
        .add("CREATE (c:Person {name: 'Charlie', age: 35})")
        .execute()
        .unwrap();

    assert_eq!(results.len(), 3);
    assert_eq!(engine.node_count(), 3);

    for r in &results {
        assert_eq!(r.stats.nodes_created, 1);
    }

    let result = engine
        .query("MATCH (p:Person) RETURN p.name ORDER BY p.name")
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][0], Value::String("Alice".into()));
    assert_eq!(result.rows[1][0], Value::String("Bob".into()));
    assert_eq!(result.rows[2][0], Value::String("Charlie".into()));

    println!("[PASS] TransactionBatch atomic commit works on MemPager");
}

#[test]
fn test_transaction_batch_rollback_on_error() {
    let mut engine = mem_engine();

    engine.create_node("Existing").unwrap();
    assert_eq!(engine.node_count(), 1);

    let result = TransactionBatch::new(&mut engine)
        .add("CREATE (a:Person {name: 'Alice'})")
        .add("INVALID CYPHER SYNTAX HERE")
        .execute();

    assert!(result.is_err());
    assert_eq!(
        engine.node_count(),
        1,
        "batch failure must roll back, keeping only pre-existing node"
    );

    println!("[PASS] TransactionBatch rolls back on error on MemPager");
}

#[test]
fn test_cypher_detach_delete() {
    let mut engine = mem_engine();

    engine.query("CREATE (a:Person {name: 'Alice'})").unwrap();
    engine.query("CREATE (b:Person {name: 'Bob'})").unwrap();
    engine.query("CREATE (c:Person {name: 'Charlie'})").unwrap();

    engine.create_relationship(0, 1, "KNOWS").unwrap();
    engine.create_relationship(1, 2, "KNOWS").unwrap();

    assert_eq!(engine.node_count(), 3);
    assert_eq!(engine.edge_count(), 2);

    let result = engine
        .query("MATCH (n:Person {name: 'Bob'}) DETACH DELETE n")
        .unwrap();
    assert_eq!(result.stats.nodes_deleted, 1);

    assert_eq!(engine.node_count(), 2);

    let remaining = engine
        .query("MATCH (p:Person) RETURN p.name ORDER BY p.name")
        .unwrap();
    assert_eq!(remaining.rows.len(), 2);
    assert_eq!(remaining.rows[0][0], Value::String("Alice".into()));
    assert_eq!(remaining.rows[1][0], Value::String("Charlie".into()));

    println!("[PASS] Cypher DETACH DELETE works on MemPager");
}

#[test]
fn test_schema_returns_labels_and_rel_types() {
    let mut engine = mem_engine();

    engine.create_node("Person").unwrap();
    engine.create_node("Company").unwrap();
    engine.create_node("City").unwrap();
    engine.create_relationship(0, 1, "WORKS_AT").unwrap();
    engine.create_relationship(0, 2, "LIVES_IN").unwrap();

    let schema = engine.schema().unwrap();
    assert_eq!(schema.node_count, 3);
    assert_eq!(schema.edge_count, 2);

    let label_names: Vec<&str> = schema.labels.iter().map(|l| l.name.as_str()).collect();
    assert!(label_names.contains(&"Person"), "missing Person label");
    assert!(label_names.contains(&"Company"), "missing Company label");
    assert!(label_names.contains(&"City"), "missing City label");

    let rel_names: Vec<&str> = schema.rel_types.iter().map(|r| r.name.as_str()).collect();
    assert!(rel_names.contains(&"WORKS_AT"), "missing WORKS_AT rel type");
    assert!(rel_names.contains(&"LIVES_IN"), "missing LIVES_IN rel type");

    println!("[PASS] schema() returns correct labels and rel types on MemPager");
}

#[test]
fn test_rollback_restores_page_allocations() {
    let pager = MemPager::new(4096);
    let db = GraphDatabase::from_pager(pager, 4096).unwrap();
    let mut engine = GraphEngine::from_database(db);

    let db_size_before = engine.db().pager().db_size();
    println!("db_size before begin: {}", db_size_before);

    engine.begin().unwrap();
    for i in 0..200 {
        let label = if i % 2 == 0 { "TypeA" } else { "TypeB" };
        let nid = engine.create_node(label).unwrap();
        engine
            .set_node_property(
                nid,
                "payload",
                PropertyValue::ShortString(format!("data_{}_padding_for_size", i)),
            )
            .unwrap();
    }
    for i in 0u64..199 {
        engine.create_relationship(i, i + 1, "CHAIN").unwrap();
    }
    let db_size_during = engine.db().pager().db_size();
    println!("db_size during tx: {}", db_size_during);
    assert!(
        db_size_during > db_size_before,
        "page allocations should grow during transaction (before={}, during={})",
        db_size_before,
        db_size_during
    );

    engine.rollback().unwrap();

    let db_size_after = engine.db().pager().db_size();
    println!("db_size after rollback: {}", db_size_after);
    assert_eq!(
        db_size_after, db_size_before,
        "rollback must restore db_size from {} back to {}",
        db_size_after, db_size_before
    );

    assert_eq!(
        engine.node_count(),
        0,
        "node_count must be 0 after rollback"
    );
    assert_eq!(
        engine.edge_count(),
        0,
        "edge_count must be 0 after rollback"
    );

    println!("[PASS] rollback restores page allocations (db_size resets) on MemPager");
}
