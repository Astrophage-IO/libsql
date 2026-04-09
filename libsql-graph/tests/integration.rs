use libsql_graph::prelude::*;
use std::collections::HashMap;

fn temp_path() -> String {
    let f = tempfile::NamedTempFile::new().unwrap();
    let p = f.path().to_str().unwrap().to_string();
    drop(f);
    p
}

#[test]
fn test_full_social_network_lifecycle() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    let r = engine
        .query("CREATE (a:Person {name: 'Alice', age: 30})")
        .unwrap();
    assert_eq!(r.stats.nodes_created, 1);

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
    engine.create_relationship(1, 2, "FOLLOWS").unwrap();
    engine.create_relationship(0, 3, "WORKS_AT").unwrap();
    engine.create_relationship(1, 3, "WORKS_AT").unwrap();
    assert_eq!(engine.edge_count(), 5);

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

    let result = engine
        .query("MATCH (a:Person) RETURN collect(a.name)")
        .unwrap();
    if let Value::List(names) = &result.rows[0][0] {
        assert_eq!(names.len(), 3);
    } else {
        panic!("expected list");
    }

    engine
        .query("MATCH (n:Person {name: 'Alice'}) SET n.age = 31 RETURN n")
        .unwrap();
    let result = engine
        .query("MATCH (n:Person {name: 'Alice'}) RETURN n.age")
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Integer(31));

    engine.query("MERGE (n:Person {name: 'Dave'})").unwrap();
    engine.query("MERGE (n:Person {name: 'Dave'})").unwrap();
    assert_eq!(engine.node_count(), 5);

    let result = engine
        .query("MATCH (a:Person) WHERE a.name CONTAINS 'li' RETURN a.name")
        .unwrap();
    assert_eq!(result.rows.len(), 2);

    let result = engine
        .query("MATCH (a:Person) WHERE a.name STARTS WITH 'Ch' RETURN a.name")
        .unwrap();
    assert_eq!(result.rows.len(), 1);

    let result = engine
        .query("MATCH (a:Person) WHERE a.age IS NOT NULL RETURN count(*)")
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Integer(3));

    let result = engine
        .query("MATCH (a:Person) RETURN a.name SKIP 1 LIMIT 2")
        .unwrap();
    assert_eq!(result.rows.len(), 2);

    let result = engine
        .query("MATCH (a:Person) RETURN DISTINCT a.age")
        .unwrap();
    let ages: Vec<&Value> = result.rows.iter().map(|r| &r[0]).collect();
    let unique: std::collections::HashSet<String> = ages.iter().map(|v| format!("{v}")).collect();
    assert_eq!(unique.len(), ages.len());

    let result = engine
        .query(
            "MATCH (a:Person) RETURN a.name, CASE WHEN a.age > 30 THEN 'senior' ELSE 'junior' END",
        )
        .unwrap();
    assert_eq!(result.rows.len(), 4); // Alice, Bob, Charlie, Dave

    let result = engine
        .query("MATCH (a:Person) RETURN toLower(a.name), size(a.name)")
        .unwrap();
    assert!(result.rows.len() > 0);

    let plan = engine
        .explain("MATCH (a:Person)-[:KNOWS]->(b) WHERE a.age > 25 RETURN b.name LIMIT 10")
        .unwrap();
    assert!(plan.contains("NodeScan"));
    assert!(plan.contains("Expand"));
    assert!(plan.contains("Filter"));

    let schema = engine.schema().unwrap();
    assert!(schema.node_count >= 4);
    assert!(schema.labels.iter().any(|l| l.name == "Person"));
    assert!(schema.rel_types.iter().any(|r| r.name == "KNOWS"));

    let stats = libsql_graph::integrity::store_stats(&mut engine).unwrap();
    assert!(stats.total_pages > 0);
    assert!(stats.file_size_bytes > 0);

    let report = libsql_graph::integrity::check_integrity(&mut engine).unwrap();
    assert!(report.is_ok(), "integrity errors: {:?}", report.errors);

    let cypher_dump = libsql_graph::dump::dump_cypher(&mut engine).unwrap();
    assert!(cypher_dump.contains("CREATE"));
    assert!(cypher_dump.contains("Person"));

    drop(engine);

    let mut engine = GraphEngine::open(&path).unwrap();
    assert!(engine.node_count() >= 4);
    let result = engine
        .query("MATCH (a:Person {name: 'Alice'}) RETURN a.age")
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Integer(31));

    let report = libsql_graph::integrity::check_integrity(&mut engine).unwrap();
    assert!(report.is_ok());

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_parameterized_queries() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine
        .query("CREATE (a:Person {name: 'Alice', age: 30})")
        .unwrap();
    engine
        .query("CREATE (b:Person {name: 'Bob', age: 25})")
        .unwrap();

    let mut params = HashMap::new();
    params.insert("name".to_string(), Value::String("Alice".into()));

    let result = engine
        .query_with_params("MATCH (a:Person) WHERE a.name = $name RETURN a.age", params)
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Integer(30));

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_batch_import_and_query() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    let node_ids = BatchNodeBuilder::new()
        .add_with_props(
            "Person",
            vec![
                ("name", PropertyValue::ShortString("Alice".into())),
                ("age", PropertyValue::Int32(30)),
            ],
        )
        .add_with_props(
            "Person",
            vec![
                ("name", PropertyValue::ShortString("Bob".into())),
                ("age", PropertyValue::Int32(25)),
            ],
        )
        .add_with_props(
            "Person",
            vec![
                ("name", PropertyValue::ShortString("Charlie".into())),
                ("age", PropertyValue::Int32(35)),
            ],
        )
        .execute(&mut engine)
        .unwrap();
    assert_eq!(node_ids.len(), 3);

    BatchRelBuilder::new()
        .add(0, 1, "KNOWS")
        .add(0, 2, "KNOWS")
        .add(1, 2, "FOLLOWS")
        .execute(&mut engine)
        .unwrap();

    let result = engine
        .query("MATCH (a:Person)-[:KNOWS]->(b) RETURN a.name, b.name ORDER BY b.name")
        .unwrap();
    assert_eq!(result.rows.len(), 2);

    let report = libsql_graph::integrity::check_integrity(&mut engine).unwrap();
    assert!(report.is_ok());

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_long_string_properties() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.create_node("Document").unwrap();
    let long_text = "The quick brown fox ".repeat(100);
    engine
        .set_node_property(0, "content", PropertyValue::ShortString(long_text.clone()))
        .unwrap();

    let result = engine.get_node_property(0, "content").unwrap();
    assert_eq!(result, Some(PropertyValue::ShortString(long_text.clone())));

    let result = engine
        .query("MATCH (d:Document) RETURN size(d.content)")
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Integer(long_text.len() as i64));

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_delete_and_integrity() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    for _ in 0..10 {
        engine.create_node("Node").unwrap();
    }
    for i in 0u64..9 {
        engine.create_relationship(i, i + 1, "NEXT").unwrap();
    }

    engine.detach_delete_node(5).unwrap();
    assert_eq!(engine.node_count(), 9);

    let report = libsql_graph::integrity::check_integrity(&mut engine).unwrap();
    assert!(report.is_ok(), "errors: {:?}", report.errors);

    engine
        .query("MATCH (n:Node) WHERE n IS NOT NULL RETURN count(*)")
        .unwrap();

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_error_handling() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    let result = engine.query("INVALID CYPHER SYNTAX");
    assert!(result.is_err());
    if let Err(GraphError::QueryParse(_)) = result {
    } else {
        panic!("expected QueryParse error, got {:?}", result);
    }

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_graph_traversal_algorithms() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    //   0 -> 1 -> 2 -> 3 -> 4
    for _ in 0..5 {
        engine.create_node("Node").unwrap();
    }
    for i in 0u64..4 {
        engine.create_relationship(i, i + 1, "NEXT").unwrap();
    }

    let node_store = libsql_graph::storage::node_store::NodeStore::new(
        engine.db().header().node_store_root,
        4096,
    );
    let rel_store =
        libsql_graph::storage::rel_store::RelStore::new(engine.db().header().rel_store_root, 4096);

    let bfs_result = libsql_graph::cursor::bfs(
        engine.db().pager(),
        &node_store,
        &rel_store,
        0,
        10,
        Direction::Outgoing,
    )
    .unwrap();
    assert_eq!(bfs_result.len(), 5);

    let path_result =
        libsql_graph::cursor::shortest_path(engine.db().pager(), &node_store, &rel_store, 0, 4, 10)
            .unwrap();
    assert!(path_result.is_some());
    let p = path_result.unwrap();
    assert_eq!(p.len(), 5);
    assert_eq!(p[0], 0);
    assert_eq!(p[4], 4);

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_relationship_variables_and_properties() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.create_node("Person").unwrap();
    engine.create_node("Person").unwrap();
    engine.create_relationship(0, 1, "KNOWS").unwrap();
    engine
        .set_rel_property(0, "since", PropertyValue::Int32(2020))
        .unwrap();
    engine
        .set_node_property(0, "name", PropertyValue::ShortString("Alice".into()))
        .unwrap();
    engine
        .set_node_property(1, "name", PropertyValue::ShortString("Bob".into()))
        .unwrap();

    let result = engine
        .query("MATCH (a)-[r:KNOWS]->(b) RETURN a.name, type(r), r.since, b.name")
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::String("Alice".into()));
    assert_eq!(result.rows[0][1], Value::String("KNOWS".into()));
    assert_eq!(result.rows[0][2], Value::Integer(2020));
    assert_eq!(result.rows[0][3], Value::String("Bob".into()));

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_unwind_and_list_operations() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    let result = engine
        .query("UNWIND [1, 2, 3, 4, 5] AS x RETURN x")
        .unwrap();
    assert_eq!(result.rows.len(), 5);
    assert_eq!(result.rows[0][0], Value::Integer(1));
    assert_eq!(result.rows[4][0], Value::Integer(5));

    drop(engine);
    let _ = std::fs::remove_file(&path);
}
