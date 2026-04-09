use libsql_graph::prelude::*;
use libsql_graph::{DefaultGraphEngine, PropertyValue};

fn temp_path() -> String {
    let f = tempfile::NamedTempFile::new().unwrap();
    let p = f.path().to_str().unwrap().to_string();
    drop(f);
    p
}

fn label_id(engine: &mut DefaultGraphEngine, name: &str) -> u32 {
    let map = engine.label_name_to_id();
    *map.get(name)
        .unwrap_or_else(|| panic!("label '{name}' not found in label_name_to_id"))
}

fn rel_type_id(engine: &mut DefaultGraphEngine, name: &str) -> u32 {
    let schema = engine.schema().unwrap();
    schema
        .rel_types
        .iter()
        .find(|r| r.name == name)
        .unwrap_or_else(|| panic!("rel type '{name}' not found in schema"))
        .token_id
}

#[test]
fn test_label_counts_after_bulk_create() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 65536).unwrap();

    engine.begin().unwrap();
    for i in 0u64..100 {
        let nid = engine.create_node("Person").unwrap();
        engine
            .set_node_property(nid, "name", PropertyValue::ShortString(format!("p{i}")))
            .unwrap();
    }
    for i in 0u64..10 {
        let nid = engine.create_node("Company").unwrap();
        engine
            .set_node_property(nid, "name", PropertyValue::ShortString(format!("c{i}")))
            .unwrap();
    }

    let person_tok = label_id(&mut engine, "Person");
    let company_tok = label_id(&mut engine, "Company");
    engine.commit().unwrap();

    assert_eq!(engine.stats().node_count, 110);
    assert_eq!(engine.node_count(), 110);

    let stats = engine.stats().clone();
    assert_eq!(
        stats.label_counts.get(&person_tok).copied().unwrap_or(0),
        100
    );
    assert_eq!(
        stats.label_counts.get(&company_tok).copied().unwrap_or(0),
        10
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_rel_type_counts_after_bulk_create() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 65536).unwrap();

    let mut nb = BatchNodeBuilder::new();
    for _ in 0..110 {
        nb = nb.add("Node");
    }
    nb.execute(&mut engine).unwrap();

    let mut kb = BatchRelBuilder::new();
    for i in 0u64..100 {
        kb = kb.add(i, (i + 1) % 100, "KNOWS");
    }
    kb.execute(&mut engine).unwrap();

    let mut kb2 = BatchRelBuilder::new();
    for i in 100u64..200 {
        let src = i - 100;
        let dst = (src + 50) % 100;
        kb2 = kb2.add(src, dst, "KNOWS");
    }
    kb2.execute(&mut engine).unwrap();

    let mut wb = BatchRelBuilder::new();
    for i in 0u64..50 {
        wb = wb.add(i, 100 + (i % 10), "WORKS_AT");
    }
    wb.execute(&mut engine).unwrap();

    assert_eq!(engine.stats().edge_count, 250);
    assert_eq!(engine.edge_count(), 250);

    let knows_tok = rel_type_id(&mut engine, "KNOWS");
    let works_at_tok = rel_type_id(&mut engine, "WORKS_AT");
    let stats = engine.stats().clone();
    assert_eq!(
        stats.rel_type_counts.get(&knows_tok).copied().unwrap_or(0),
        200
    );
    assert_eq!(
        stats
            .rel_type_counts
            .get(&works_at_tok)
            .copied()
            .unwrap_or(0),
        50
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_avg_degree() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 65536).unwrap();

    let mut nb = BatchNodeBuilder::new();
    for _ in 0..110 {
        nb = nb.add("Node");
    }
    nb.execute(&mut engine).unwrap();

    let mut rb = BatchRelBuilder::new();
    for i in 0u64..100 {
        rb = rb.add(i, (i + 1) % 100, "KNOWS");
    }
    for i in 0u64..100 {
        rb = rb.add(i, (i + 50) % 100, "KNOWS");
    }
    for i in 0u64..50 {
        rb = rb.add(i, 100 + (i % 10), "WORKS_AT");
    }
    rb.execute(&mut engine).unwrap();

    let avg = engine.stats().avg_degree();
    let expected = 250.0 / 110.0;
    assert!(
        (avg - expected).abs() < 0.01,
        "avg_degree {avg} not close to expected {expected}"
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_label_counts_after_delete() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 65536).unwrap();

    let mut pb = BatchNodeBuilder::new();
    for i in 0u64..100 {
        pb = pb.add_with_props(
            "Person",
            vec![("name", PropertyValue::ShortString(format!("p{i}")))],
        );
    }
    pb.execute(&mut engine).unwrap();

    let mut cb = BatchNodeBuilder::new();
    for i in 0u64..10 {
        cb = cb.add_with_props(
            "Company",
            vec![("name", PropertyValue::ShortString(format!("c{i}")))],
        );
    }
    cb.execute(&mut engine).unwrap();

    let person_tok = label_id(&mut engine, "Person");
    let company_tok = label_id(&mut engine, "Company");

    for id in 0u64..20 {
        engine.detach_delete_node(id).unwrap();
    }

    let stats = engine.stats().clone();
    assert_eq!(stats.node_count, 90);
    assert_eq!(
        stats.label_counts.get(&person_tok).copied().unwrap_or(0),
        80
    );
    assert_eq!(
        stats.label_counts.get(&company_tok).copied().unwrap_or(0),
        10
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_stats_persistence_across_close_reopen() {
    let path = temp_path();

    let (person_tok, company_tok, knows_tok);
    {
        let mut engine = GraphEngine::create(&path, 65536).unwrap();

        let mut nb = BatchNodeBuilder::new();
        for _ in 0..50 {
            nb = nb.add("Person");
        }
        for _ in 0..5 {
            nb = nb.add("Company");
        }
        nb.execute(&mut engine).unwrap();

        let mut rb = BatchRelBuilder::new();
        for i in 0u64..30 {
            rb = rb.add(i, (i + 1) % 50, "KNOWS");
        }
        rb.execute(&mut engine).unwrap();

        person_tok = label_id(&mut engine, "Person");
        company_tok = label_id(&mut engine, "Company");
        knows_tok = rel_type_id(&mut engine, "KNOWS");

        let stats = engine.stats().clone();
        assert_eq!(stats.node_count, 55);
        assert_eq!(stats.edge_count, 30);
        drop(engine);
    }

    {
        let engine = GraphEngine::open(&path).unwrap();
        let stats = engine.stats().clone();
        assert_eq!(stats.node_count, 55, "node_count after reopen");
        assert_eq!(stats.edge_count, 30, "edge_count after reopen");
        assert_eq!(stats.label_counts.len(), 2, "should have 2 label types");
        assert_eq!(
            stats.label_counts.get(&person_tok).copied().unwrap_or(0),
            50
        );
        assert_eq!(
            stats.label_counts.get(&company_tok).copied().unwrap_or(0),
            5
        );
        assert_eq!(stats.rel_type_counts.len(), 1, "should have 1 rel type");
        assert_eq!(
            stats.rel_type_counts.get(&knows_tok).copied().unwrap_or(0),
            30
        );
        drop(engine);
    }

    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_explain_output_has_cost_and_scan() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 65536).unwrap();

    let mut nb = BatchNodeBuilder::new();
    for i in 0u64..20 {
        nb = nb.add_with_props(
            "Person",
            vec![("name", PropertyValue::ShortString(format!("p{i}")))],
        );
    }
    nb.execute(&mut engine).unwrap();

    let plan = engine.explain("MATCH (p:Person) RETURN p.name").unwrap();
    println!("EXPLAIN output:\n{plan}");

    assert!(
        plan.contains("cost"),
        "EXPLAIN should contain cost info, got:\n{plan}"
    );

    let has_scan = plan.contains("IndexedNodeScan") || plan.contains("NodeScan");
    assert!(has_scan, "EXPLAIN should show a scan step, got:\n{plan}");

    assert!(
        plan.contains("Project"),
        "EXPLAIN should show projection, got:\n{plan}"
    );

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_profile_returns_timing_and_results() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 65536).unwrap();

    let mut nb = BatchNodeBuilder::new();
    for i in 0u64..10 {
        nb = nb.add_with_props(
            "Person",
            vec![("name", PropertyValue::ShortString(format!("p{i}")))],
        );
    }
    nb.execute(&mut engine).unwrap();

    let profile = engine.profile("MATCH (p:Person) RETURN p.name").unwrap();
    println!("PROFILE output:\n{profile}");

    assert!(profile.total_time_us > 0, "total_time should be > 0");
    assert!(
        profile.exec_time_us <= profile.total_time_us,
        "exec_time should be <= total_time"
    );
    assert_eq!(profile.result.rows.len(), 10);
    assert!(!profile.plan.is_empty(), "plan text should not be empty");

    for row in &profile.result.rows {
        match &row[0] {
            libsql_graph::Value::String(s) => assert!(s.starts_with('p')),
            other => panic!("expected string, got {other:?}"),
        }
    }

    drop(engine);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_optimizer_transparency_works_at_query() {
    let path = temp_path();
    let mut engine = GraphEngine::create(&path, 65536).unwrap();

    let mut nb = BatchNodeBuilder::new();
    for i in 0u64..20 {
        nb = nb.add_with_props(
            "Person",
            vec![("name", PropertyValue::ShortString(format!("person_{i}")))],
        );
    }
    for i in 0u64..5 {
        nb = nb.add_with_props(
            "Company",
            vec![("name", PropertyValue::ShortString(format!("company_{i}")))],
        );
    }
    nb.execute(&mut engine).unwrap();

    let mut rb = BatchRelBuilder::new();
    for i in 0u64..20 {
        let company_id = 20 + (i % 5);
        rb = rb.add(i, company_id, "WORKS_AT");
    }
    rb.execute(&mut engine).unwrap();

    let result = engine
        .query("MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name")
        .unwrap();

    assert_eq!(
        result.rows.len(),
        20,
        "should get 20 person->company rows, got {}",
        result.rows.len()
    );
    assert_eq!(result.columns, vec!["p.name", "c.name"]);

    for row in &result.rows {
        match &row[0] {
            libsql_graph::Value::String(s) => assert!(
                s.starts_with("person_"),
                "person name should start with 'person_', got '{s}'"
            ),
            other => panic!("expected person name string, got {other:?}"),
        }
        match &row[1] {
            libsql_graph::Value::String(s) => assert!(
                s.starts_with("company_"),
                "company name should start with 'company_', got '{s}'"
            ),
            other => panic!("expected company name string, got {other:?}"),
        }
    }

    let mut person_names: Vec<String> = result
        .rows
        .iter()
        .filter_map(|r| {
            if let libsql_graph::Value::String(s) = &r[0] {
                Some(s.clone())
            } else {
                None
            }
        })
        .collect();
    person_names.sort();
    person_names.dedup();
    assert_eq!(person_names.len(), 20, "all 20 persons should appear");

    let mut company_names: Vec<String> = result
        .rows
        .iter()
        .filter_map(|r| {
            if let libsql_graph::Value::String(s) = &r[1] {
                Some(s.clone())
            } else {
                None
            }
        })
        .collect();
    company_names.sort();
    company_names.dedup();
    assert_eq!(company_names.len(), 5, "all 5 companies should appear");

    drop(engine);
    let _ = std::fs::remove_file(&path);
}
