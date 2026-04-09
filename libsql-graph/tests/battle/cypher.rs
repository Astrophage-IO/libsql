use libsql_graph::prelude::*;

fn temp_path() -> String {
    let f = tempfile::NamedTempFile::new().unwrap();
    let p = f.path().to_str().unwrap().to_string();
    drop(f);
    p
}

fn build_social_network() -> (DefaultGraphEngine, String) {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    let persons: Vec<(&str, i32)> = vec![
        ("Alice", 28),
        ("Bob", 35),
        ("Charlie", 22),
        ("Diana", 41),
        ("Eve", 30),
        ("Frank", 27),
        ("Grace", 33),
        ("Hank", 45),
        ("Irene", 29),
        ("Jack", 38),
        ("Karen", 24),
        ("Leo", 31),
        ("Mona", 36),
        ("Nate", 26),
        ("Olivia", 40),
        ("Paul", 23),
        ("Quinn", 34),
        ("Rita", 29),
        ("Sam", 37),
        ("Tina", 32),
    ];

    for (name, age) in &persons {
        engine
            .query(&format!(
                "CREATE (p:Person {{name: '{}', age: {}}})",
                name, age
            ))
            .unwrap();
    }

    let companies = ["Acme", "Bolt", "Crux", "Dynamo", "Echo"];
    for name in &companies {
        engine
            .query(&format!("CREATE (c:Company {{name: '{}'}})", name))
            .unwrap();
    }

    assert_eq!(engine.node_count(), 25);

    let knows_pairs: Vec<(u64, u64)> = vec![
        (0, 1),
        (0, 2),
        (1, 3),
        (2, 4),
        (3, 5),
        (4, 6),
        (5, 7),
        (6, 8),
        (7, 9),
        (8, 9),
        (10, 11),
        (12, 13),
        (14, 15),
        (16, 17),
        (18, 19),
    ];
    for (a, b) in &knows_pairs {
        engine.create_relationship(*a, *b, "KNOWS").unwrap();
    }

    for i in 0u64..20 {
        let company_id = 20 + (i % 5);
        engine
            .create_relationship(i, company_id, "WORKS_AT")
            .unwrap();
    }

    (engine, path)
}

#[test]
fn t01_order_by_name() {
    let (mut engine, path) = build_social_network();

    let result = engine
        .query("MATCH (p:Person) RETURN p.name ORDER BY p.name")
        .unwrap();

    assert_eq!(result.rows.len(), 20);

    let names: Vec<String> = result
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected String, got {:?}", other),
        })
        .collect();

    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted, "names should be in alphabetical order");
    assert_eq!(names[0], "Alice");
    assert_eq!(names[19], "Tina");

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t02_where_age_filter() {
    let (mut engine, path) = build_social_network();

    let result = engine
        .query("MATCH (p:Person) WHERE p.age > 30 RETURN p.name")
        .unwrap();

    let names: Vec<String> = result
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected String, got {:?}", other),
        })
        .collect();

    let expected_over_30 = [
        "Bob", "Diana", "Grace", "Hank", "Jack", "Leo", "Mona", "Olivia", "Quinn", "Sam", "Tina",
    ];

    assert_eq!(names.len(), expected_over_30.len());
    for name in &expected_over_30 {
        assert!(
            names.contains(&name.to_string()),
            "{} should be in results",
            name
        );
    }

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t03_count_persons() {
    let (mut engine, path) = build_social_network();

    let result = engine.query("MATCH (p:Person) RETURN count(p)").unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Integer(20));

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t04_avg_age() {
    let (mut engine, path) = build_social_network();

    let result = engine.query("MATCH (p:Person) RETURN avg(p.age)").unwrap();

    assert_eq!(result.rows.len(), 1);
    match &result.rows[0][0] {
        Value::Float(avg) => {
            let expected_avg = (28
                + 35
                + 22
                + 41
                + 30
                + 27
                + 33
                + 45
                + 29
                + 38
                + 24
                + 31
                + 36
                + 26
                + 40
                + 23
                + 34
                + 29
                + 37
                + 32) as f64
                / 20.0;
            assert!(
                (*avg - expected_avg).abs() < 0.01,
                "avg was {} but expected {}",
                avg,
                expected_avg
            );
        }
        other => panic!("expected Float for avg, got {:?}", other),
    }

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t05_limit() {
    let (mut engine, path) = build_social_network();

    let result = engine
        .query("MATCH (p:Person) RETURN p.name LIMIT 5")
        .unwrap();

    assert_eq!(result.rows.len(), 5);

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t06_skip_limit() {
    let (mut engine, path) = build_social_network();

    let first5 = engine
        .query("MATCH (p:Person) RETURN p.name LIMIT 5")
        .unwrap();
    let skipped = engine
        .query("MATCH (p:Person) RETURN p.name SKIP 10 LIMIT 5")
        .unwrap();

    assert_eq!(first5.rows.len(), 5);
    assert_eq!(skipped.rows.len(), 5);

    let first5_names: Vec<&Value> = first5.rows.iter().map(|r| &r[0]).collect();
    let skipped_names: Vec<&Value> = skipped.rows.iter().map(|r| &r[0]).collect();
    assert_ne!(
        first5_names, skipped_names,
        "SKIP 10 LIMIT 5 should return different rows than LIMIT 5"
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t07_relationship_traversal() {
    let (mut engine, path) = build_social_network();

    let result = engine
        .query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name")
        .unwrap();

    assert_eq!(result.columns, vec!["a.name", "b.name"]);
    assert_eq!(result.rows.len(), 15, "should have 15 KNOWS relationships");

    let has_alice_bob = result
        .rows
        .iter()
        .any(|r| r[0] == Value::String("Alice".into()) && r[1] == Value::String("Bob".into()));
    assert!(has_alice_bob, "Alice->Bob KNOWS should exist");

    let has_alice_charlie = result
        .rows
        .iter()
        .any(|r| r[0] == Value::String("Alice".into()) && r[1] == Value::String("Charlie".into()));
    assert!(has_alice_charlie, "Alice->Charlie KNOWS should exist");

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t08_aggregation_with_grouping() {
    let (mut engine, path) = build_social_network();

    let result = engine
        .query("MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN c.name, count(p)")
        .unwrap();

    assert_eq!(result.rows.len(), 5, "should have 5 companies");

    for row in &result.rows {
        match &row[1] {
            Value::Integer(count) => {
                assert_eq!(*count, 4, "each company should have 4 employees (20/5)");
            }
            other => panic!("expected Integer count, got {:?}", other),
        }
    }

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t09_distinct() {
    let (mut engine, path) = build_social_network();

    let result = engine
        .query("MATCH (p:Person) RETURN DISTINCT p.age ORDER BY p.age")
        .unwrap();

    let ages: Vec<i64> = result
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(n) => *n,
            other => panic!("expected Integer, got {:?}", other),
        })
        .collect();

    let unique_count = {
        let mut s = ages.clone();
        s.sort();
        s.dedup();
        s.len()
    };
    assert_eq!(
        ages.len(),
        unique_count,
        "DISTINCT should have eliminated duplicate ages"
    );

    for i in 1..ages.len() {
        assert!(ages[i] >= ages[i - 1], "ages should be in ascending order");
    }

    assert!(
        ages.len() < 20,
        "some ages are duplicated (29 appears twice), so distinct count < 20"
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t10_starts_with() {
    let (mut engine, path) = build_social_network();

    let result = engine
        .query("MATCH (p:Person) WHERE p.name STARTS WITH 'A' RETURN p.name")
        .unwrap();

    let names: Vec<String> = result
        .rows
        .iter()
        .map(|r| match &r[0] {
            Value::String(s) => s.clone(),
            other => panic!("expected String, got {:?}", other),
        })
        .collect();

    assert_eq!(names.len(), 1);
    assert_eq!(names[0], "Alice");

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t11_invalid_query_returns_error() {
    let (mut engine, path) = build_social_network();

    let result = engine.query("THIS IS NOT VALID CYPHER AT ALL");
    assert!(result.is_err(), "invalid query should return Err");

    let result2 = engine.query("MATCH (p:Person) RETURN p.name");
    assert!(
        result2.is_ok(),
        "engine should still work after invalid query"
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn t12_empty_result() {
    let (mut engine, path) = build_social_network();

    let result = engine
        .query("MATCH (p:Person {name: 'NonExistent'}) RETURN p")
        .unwrap();

    assert_eq!(
        result.rows.len(),
        0,
        "non-existent person should return empty"
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}
