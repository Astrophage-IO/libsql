use libsql_graph::prelude::*;

fn temp_path() -> String {
    let f = tempfile::NamedTempFile::new().unwrap();
    let p = f.path().to_str().unwrap().to_string();
    drop(f);
    p
}

#[test]
fn test_diverse_value_types() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.create_node("TypeTest").unwrap();

    engine
        .set_node_property(0, "short_str", PropertyValue::ShortString("hello".into()))
        .unwrap();
    engine
        .set_node_property(0, "i32_val", PropertyValue::Int32(42))
        .unwrap();
    engine
        .set_node_property(0, "i64_val", PropertyValue::Int64(9_000_000_000))
        .unwrap();
    engine
        .set_node_property(0, "f64_val", PropertyValue::Float64(3.14159265))
        .unwrap();
    engine
        .set_node_property(0, "bool_true", PropertyValue::Bool(true))
        .unwrap();
    engine
        .set_node_property(0, "bool_false", PropertyValue::Bool(false))
        .unwrap();

    assert_eq!(
        engine.get_node_property(0, "short_str").unwrap(),
        Some(PropertyValue::ShortString("hello".into()))
    );
    assert_eq!(
        engine.get_node_property(0, "i32_val").unwrap(),
        Some(PropertyValue::Int32(42))
    );
    assert_eq!(
        engine.get_node_property(0, "i64_val").unwrap(),
        Some(PropertyValue::Int64(9_000_000_000))
    );
    assert_eq!(
        engine.get_node_property(0, "f64_val").unwrap(),
        Some(PropertyValue::Float64(3.14159265))
    );
    assert_eq!(
        engine.get_node_property(0, "bool_true").unwrap(),
        Some(PropertyValue::Bool(true))
    );
    assert_eq!(
        engine.get_node_property(0, "bool_false").unwrap(),
        Some(PropertyValue::Bool(false))
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_long_string_overflow() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.create_node("Document").unwrap();

    let long = "abcdefghij".repeat(20);
    assert!(long.len() >= 100);
    engine
        .set_node_property(0, "body", PropertyValue::ShortString(long.clone()))
        .unwrap();

    let got = engine.get_node_property(0, "body").unwrap();
    assert_eq!(got, Some(PropertyValue::ShortString(long)));

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_twenty_properties_on_single_node() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.create_node("Loaded").unwrap();

    for i in 0..20 {
        let key = format!("prop_{i}");
        engine
            .set_node_property(0, &key, PropertyValue::Int32(i))
            .unwrap();
    }

    let all = engine.get_all_node_properties(0).unwrap();
    assert_eq!(all.len(), 20, "expected 20 properties, got {}", all.len());

    for i in 0..20 {
        let key = format!("prop_{i}");
        let val = engine.get_node_property(0, &key).unwrap();
        assert_eq!(val, Some(PropertyValue::Int32(i)), "mismatch for {key}");
    }

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_property_update_overwrite() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.create_node("Mutable").unwrap();
    engine
        .set_node_property(0, "version", PropertyValue::Int32(1))
        .unwrap();
    assert_eq!(
        engine.get_node_property(0, "version").unwrap(),
        Some(PropertyValue::Int32(1))
    );

    engine
        .set_node_property(0, "version", PropertyValue::Int32(2))
        .unwrap();
    assert_eq!(
        engine.get_node_property(0, "version").unwrap(),
        Some(PropertyValue::Int32(2))
    );

    engine
        .set_node_property(0, "version", PropertyValue::ShortString("three".into()))
        .unwrap();
    assert_eq!(
        engine.get_node_property(0, "version").unwrap(),
        Some(PropertyValue::ShortString("three".into()))
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_relationship_properties() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.create_node("Person").unwrap();
    engine.create_node("Person").unwrap();
    engine.create_relationship(0, 1, "FRIENDS").unwrap();

    engine
        .set_rel_property(0, "since", PropertyValue::Int32(2019))
        .unwrap();
    engine
        .set_rel_property(0, "weight", PropertyValue::Float64(0.95))
        .unwrap();
    engine
        .set_rel_property(0, "active", PropertyValue::Bool(true))
        .unwrap();

    assert_eq!(
        engine.get_rel_property(0, "since").unwrap(),
        Some(PropertyValue::Int32(2019))
    );
    assert_eq!(
        engine.get_rel_property(0, "weight").unwrap(),
        Some(PropertyValue::Float64(0.95))
    );
    assert_eq!(
        engine.get_rel_property(0, "active").unwrap(),
        Some(PropertyValue::Bool(true))
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_persistence_across_reopen() {
    let path = temp_path();

    {
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        engine.create_node("Persist").unwrap();
        engine
            .set_node_property(0, "name", PropertyValue::ShortString("durable".into()))
            .unwrap();
        engine
            .set_node_property(0, "count", PropertyValue::Int64(42))
            .unwrap();
        engine
            .set_node_property(0, "flag", PropertyValue::Bool(true))
            .unwrap();

        engine.create_node("Persist").unwrap();
        engine.create_relationship(0, 1, "LINK").unwrap();
        engine
            .set_rel_property(0, "weight", PropertyValue::Float64(1.5))
            .unwrap();
    }

    {
        let mut engine = GraphEngine::open(&path).unwrap();

        assert_eq!(
            engine.get_node_property(0, "name").unwrap(),
            Some(PropertyValue::ShortString("durable".into()))
        );
        assert_eq!(
            engine.get_node_property(0, "count").unwrap(),
            Some(PropertyValue::Int64(42))
        );
        assert_eq!(
            engine.get_node_property(0, "flag").unwrap(),
            Some(PropertyValue::Bool(true))
        );
        assert_eq!(
            engine.get_rel_property(0, "weight").unwrap(),
            Some(PropertyValue::Float64(1.5))
        );
    }

    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_same_key_different_nodes() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    for i in 0..5u64 {
        engine.create_node("Item").unwrap();
        engine
            .set_node_property(i, "score", PropertyValue::Int32(i as i32 * 10))
            .unwrap();
    }

    for i in 0..5u64 {
        assert_eq!(
            engine.get_node_property(i, "score").unwrap(),
            Some(PropertyValue::Int32(i as i32 * 10)),
            "node {i} score mismatch"
        );
    }

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_cypher_inline_properties() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine
        .query("CREATE (a:Person {name: 'Xavier', age: 25})")
        .unwrap();

    assert_eq!(
        engine.get_node_property(0, "name").unwrap(),
        Some(PropertyValue::ShortString("Xavier".into()))
    );
    assert_eq!(
        engine.get_node_property(0, "age").unwrap(),
        Some(PropertyValue::Int32(25))
    );

    engine
        .query("CREATE (b:Device {model: 'Z-9000', active: true})")
        .unwrap();
    assert_eq!(
        engine.get_node_property(1, "model").unwrap(),
        Some(PropertyValue::ShortString("Z-9000".into()))
    );
    assert_eq!(
        engine.get_node_property(1, "active").unwrap(),
        Some(PropertyValue::Bool(true))
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_empty_string_property() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.create_node("Edge").unwrap();
    engine
        .set_node_property(0, "tag", PropertyValue::ShortString(String::new()))
        .unwrap();

    let val = engine.get_node_property(0, "tag").unwrap();
    assert_eq!(val, Some(PropertyValue::ShortString(String::new())));

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_large_int64_value() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 4096).unwrap();

    engine.create_node("Extremes").unwrap();
    engine
        .set_node_property(0, "max_i64", PropertyValue::Int64(i64::MAX))
        .unwrap();
    engine
        .set_node_property(0, "min_i64", PropertyValue::Int64(i64::MIN))
        .unwrap();
    engine
        .set_node_property(0, "neg_i32", PropertyValue::Int32(i32::MIN))
        .unwrap();

    assert_eq!(
        engine.get_node_property(0, "max_i64").unwrap(),
        Some(PropertyValue::Int64(i64::MAX))
    );
    assert_eq!(
        engine.get_node_property(0, "min_i64").unwrap(),
        Some(PropertyValue::Int64(i64::MIN))
    );
    assert_eq!(
        engine.get_node_property(0, "neg_i32").unwrap(),
        Some(PropertyValue::Int32(i32::MIN))
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}
