use libsql_graph::prelude::*;

fn main() {
    let path = "/tmp/libsql-graph-e2e-demo.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}-wal"));

    println!("=== libsql-graph End-to-End Demo ===\n");

    // --- Phase 1: Batch data ingestion via Cypher ---
    println!("--- Phase 1: Data Ingestion ---");
    let mut engine = GraphEngine::create(path, 4096).unwrap();

    engine.begin().unwrap();

    let queries = vec![
        "CREATE (a:Person {name: 'Alice', age: 30})",
        "CREATE (b:Person {name: 'Bob', age: 25})",
        "CREATE (c:Person {name: 'Charlie', age: 35})",
        "CREATE (d:Person {name: 'Diana', age: 28})",
        "CREATE (e:Person {name: 'Eve', age: 32})",
        "CREATE (f:Company {name: 'Acme Corp'})",
        "CREATE (g:Company {name: 'Globex'})",
        "CREATE (h:City {name: 'New York'})",
        "CREATE (i:City {name: 'London'})",
        "CREATE (j:City {name: 'Tokyo'})",
    ];

    for q in &queries {
        let r = engine.query(q).unwrap();
        println!(
            "  {} -> nodes_created={}, props_set={}",
            q, r.stats.nodes_created, r.stats.properties_set
        );
    }

    engine.commit().unwrap();
    println!("  Committed {} nodes\n", engine.node_count());

    // --- Phase 2: Create relationships ---
    println!("--- Phase 2: Relationships ---");
    engine.begin().unwrap();

    let rels = vec![
        (0, 1, "KNOWS"),
        (0, 2, "KNOWS"),
        (1, 3, "KNOWS"),
        (2, 4, "KNOWS"),
        (3, 4, "KNOWS"),
        (0, 5, "WORKS_AT"),
        (1, 5, "WORKS_AT"),
        (2, 6, "WORKS_AT"),
        (3, 6, "WORKS_AT"),
        (4, 5, "WORKS_AT"),
        (0, 7, "LIVES_IN"),
        (1, 7, "LIVES_IN"),
        (2, 8, "LIVES_IN"),
        (3, 9, "LIVES_IN"),
        (4, 8, "LIVES_IN"),
    ];

    for (src, dst, rel_type) in &rels {
        engine.create_relationship(*src, *dst, rel_type).unwrap();
    }

    engine.commit().unwrap();
    println!("  Created {} relationships", engine.edge_count());
    println!("  Stats: avg_degree={:.1}\n", engine.stats().avg_degree());

    // --- Phase 3: Cypher queries ---
    println!("--- Phase 3: Cypher Queries ---\n");

    // Query 1: Find all people
    println!("Query: MATCH (p:Person) RETURN p.name, p.age");
    let result = engine
        .query("MATCH (p:Person) RETURN p.name, p.age")
        .unwrap();
    println!("  Columns: {:?}", result.columns);
    for row in &result.rows {
        println!("  {:?}", row);
    }
    println!();

    // Query 2: Who does Alice know?
    println!("Query: MATCH (a:Person {{name: 'Alice'}})-[:KNOWS]->(b) RETURN b.name");
    let result = engine
        .query("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN b.name")
        .unwrap();
    println!("  Alice knows:");
    for row in &result.rows {
        println!("    - {:?}", row[0]);
    }
    println!();

    // Query 3: 2-hop friends of Alice
    println!("Query: MATCH (a:Person {{name: 'Alice'}})-[:KNOWS]->()-[:KNOWS]->(c) RETURN DISTINCT c.name");
    let result = engine.query("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b)-[:KNOWS]->(c) WHERE a <> c RETURN DISTINCT c.name").unwrap();
    println!("  Alice's friends-of-friends:");
    for row in &result.rows {
        println!("    - {:?}", row[0]);
    }
    println!();

    // Query 4: Who works where?
    println!("Query: MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name");
    let result = engine
        .query("MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name")
        .unwrap();
    for row in &result.rows {
        println!("  {} works at {}", row[0], row[1]);
    }
    println!();

    // Query 5: Count people per city
    println!("Query: MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN c.name, count(p)");
    let result = engine
        .query("MATCH (p:Person)-[:LIVES_IN]->(c:City) RETURN c.name, count(p)")
        .unwrap();
    for row in &result.rows {
        println!("  {} has {} residents", row[0], row[1]);
    }
    println!();

    // Query 6: Aggregation — average age
    println!("Query: MATCH (p:Person) RETURN count(p), avg(p.age)");
    let result = engine
        .query("MATCH (p:Person) RETURN count(p), avg(p.age)")
        .unwrap();
    println!(
        "  People: {}, Avg age: {}",
        result.rows[0][0], result.rows[0][1]
    );
    println!();

    // --- Phase 4: EXPLAIN with optimizer ---
    println!("--- Phase 4: Query Plan ---\n");
    let plan = engine
        .explain("MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name")
        .unwrap();
    println!("{}", plan);

    // --- Phase 5: Profile ---
    println!("--- Phase 5: Profile ---\n");
    let profile = engine
        .profile("MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN p.name, f.name")
        .unwrap();
    println!("{}", profile);

    // --- Phase 6: Schema ---
    println!("--- Phase 6: Schema ---\n");
    let schema = engine.schema().unwrap();
    println!(
        "  Nodes: {}, Edges: {}",
        schema.node_count, schema.edge_count
    );
    println!(
        "  Labels: {:?}",
        schema.labels.iter().map(|l| &l.name).collect::<Vec<_>>()
    );
    println!(
        "  Rel types: {:?}",
        schema.rel_types.iter().map(|r| &r.name).collect::<Vec<_>>()
    );
    println!();

    // --- Phase 7: Persistence test ---
    println!("--- Phase 7: Persistence ---\n");
    let node_count = engine.node_count();
    let edge_count = engine.edge_count();
    drop(engine);

    let mut engine = GraphEngine::open(path).unwrap();
    assert_eq!(engine.node_count(), node_count);
    assert_eq!(engine.edge_count(), edge_count);
    println!(
        "  Reopened: {} nodes, {} edges -- OK",
        engine.node_count(),
        engine.edge_count()
    );

    let result = engine
        .query("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(b) RETURN b.name")
        .unwrap();
    println!(
        "  Alice still knows {} people after reopen -- OK",
        result.rows.len()
    );
    println!();

    // --- Phase 8: Transaction rollback ---
    println!("--- Phase 8: Transaction Rollback ---\n");
    let before = engine.node_count();
    engine.begin().unwrap();
    engine
        .query("CREATE (x:Temp {name: 'ShouldNotExist'})")
        .unwrap();
    println!("  After CREATE in tx: {} nodes", engine.node_count());
    engine.rollback().unwrap();
    println!("  After ROLLBACK: {} nodes (restored)", engine.node_count());
    assert_eq!(engine.node_count(), before);
    println!();

    // Cleanup
    drop(engine);
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}-wal"));

    println!("=== All phases passed! ===");
}
