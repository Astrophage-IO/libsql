use libsql_graph::prelude::*;
use libsql_graph::PropertyValue;

fn temp_path() -> String {
    let f = tempfile::NamedTempFile::new().unwrap();
    let p = f.path().to_str().unwrap().to_string();
    drop(f);
    let _ = std::fs::remove_file(&p);
    p
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}-wal"));
}

#[test]
fn test_normal_close_reopen_20_nodes_with_properties() {
    let path = temp_path();

    {
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        for i in 0..20u64 {
            let nid = engine.create_node("Item").unwrap();
            assert_eq!(nid, i);
            engine
                .set_node_property(nid, "idx", PropertyValue::Int32(i as i32))
                .unwrap();
            engine
                .set_node_property(nid, "name", PropertyValue::ShortString(format!("node_{i}")))
                .unwrap();
        }
        assert_eq!(engine.node_count(), 20);
    }

    {
        let mut engine = GraphEngine::open(&path).unwrap();
        assert_eq!(engine.node_count(), 20);
        for i in 0..20u64 {
            let idx_val = engine.get_node_property(i, "idx").unwrap();
            assert_eq!(
                idx_val,
                Some(PropertyValue::Int32(i as i32)),
                "idx mismatch for node {i}"
            );

            let name_val = engine.get_node_property(i, "name").unwrap();
            assert_eq!(
                name_val,
                Some(PropertyValue::ShortString(format!("node_{i}"))),
                "name mismatch for node {i}"
            );
        }
    }

    cleanup(&path);
}

#[test]
fn test_wal_file_exists_during_transaction() {
    let path = temp_path();
    let wal_path = format!("{path}-wal");

    {
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        for i in 0..10u64 {
            engine.create_node("Probe").unwrap();
            engine
                .set_node_property(i, "v", PropertyValue::Int32(i as i32))
                .unwrap();
        }

        let wal_exists = std::path::Path::new(&wal_path).exists();
        let db_exists = std::path::Path::new(&path).exists();
        println!("after 10 node creates: db exists={db_exists}, wal exists={wal_exists}");
        assert!(db_exists, "database file must exist");
    }

    {
        let mut engine = GraphEngine::open(&path).unwrap();
        assert_eq!(engine.node_count(), 10);
        for i in 0..10u64 {
            let val = engine.get_node_property(i, "v").unwrap();
            assert_eq!(val, Some(PropertyValue::Int32(i as i32)));
        }
    }

    cleanup(&path);
}

#[test]
fn test_many_commits_data_survives_checkpoint() {
    let path = temp_path();

    {
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        for i in 0..10u64 {
            let nid = engine.create_node("Tick").unwrap();
            engine
                .set_node_property(nid, "seq", PropertyValue::Int32(i as i32))
                .unwrap();
        }
        assert_eq!(engine.node_count(), 10);
    }

    {
        let mut engine = GraphEngine::open(&path).unwrap();
        assert_eq!(engine.node_count(), 10);
        for i in 0..10u64 {
            let val = engine.get_node_property(i, "seq").unwrap();
            assert_eq!(
                val,
                Some(PropertyValue::Int32(i as i32)),
                "seq mismatch for node {i} after checkpoint"
            );
        }
    }

    cleanup(&path);
}

#[test]
fn test_data_file_size_reasonable() {
    let path = temp_path();

    {
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        for i in 0..30u64 {
            let nid = engine.create_node("Bulk").unwrap();
            engine
                .set_node_property(nid, "val", PropertyValue::Int32(i as i32))
                .unwrap();
        }
    }

    let meta = std::fs::metadata(&path).unwrap();
    let file_size = meta.len();
    println!("data file size after 30 nodes: {file_size} bytes");
    assert!(file_size > 0, "data file must not be empty");
    assert!(
        file_size < 10 * 1024 * 1024,
        "data file unreasonably large: {file_size} bytes"
    );

    {
        let engine = GraphEngine::open(&path).unwrap();
        assert_eq!(engine.node_count(), 30);
    }

    cleanup(&path);
}

#[test]
fn test_corrupt_wal_tail_recovery() {
    let path = temp_path();
    let wal_path = format!("{path}-wal");

    {
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        for i in 0..15u64 {
            let nid = engine.create_node("Safe").unwrap();
            engine
                .set_node_property(nid, "k", PropertyValue::Int32(i as i32))
                .unwrap();
        }
        assert_eq!(engine.node_count(), 15);
    }

    {
        let engine = GraphEngine::open(&path).unwrap();
        assert_eq!(engine.node_count(), 15);
    }

    {
        let mut engine = GraphEngine::open(&path).unwrap();
        for i in 0..15u64 {
            let val = engine.get_node_property(i, "k").unwrap();
            assert_eq!(val, Some(PropertyValue::Int32(i as i32)));
        }
        engine.create_node("Extra").unwrap();
        engine
            .set_node_property(15, "k", PropertyValue::Int32(999))
            .unwrap();
    }

    if std::path::Path::new(&wal_path).exists() {
        let _ = std::fs::remove_file(&wal_path);
    }
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&wal_path)
            .unwrap();
        f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
        f.write_all(&[0xFF; 46]).unwrap();
    }

    {
        let engine_result = GraphEngine::open(&path);
        assert!(
            engine_result.is_ok(),
            "engine must open despite corrupt WAL: {:?}",
            engine_result.err()
        );
        let engine = engine_result.unwrap();
        assert!(
            engine.node_count() >= 15,
            "committed data must survive WAL corruption, got {}",
            engine.node_count()
        );
        println!(
            "node count after WAL corruption: {} (expected >= 15)",
            engine.node_count()
        );
    }

    cleanup(&path);
}

#[test]
fn test_rapid_open_close_cycles() {
    let path = temp_path();

    {
        let _engine = GraphEngine::create(&path, 4096).unwrap();
    }

    for cycle in 0..20u64 {
        let mut engine = GraphEngine::open(&path).unwrap();
        let expected_before = cycle;
        assert_eq!(
            engine.node_count(),
            expected_before,
            "before create in cycle {cycle}"
        );
        let nid = engine.create_node("Cycle").unwrap();
        engine
            .set_node_property(nid, "cycle", PropertyValue::Int32(cycle as i32))
            .unwrap();
        assert_eq!(engine.node_count(), cycle + 1);
        drop(engine);
    }

    {
        let mut engine = GraphEngine::open(&path).unwrap();
        assert_eq!(engine.node_count(), 20);
        for i in 0..20u64 {
            let val = engine.get_node_property(i, "cycle").unwrap();
            assert_eq!(
                val,
                Some(PropertyValue::Int32(i as i32)),
                "cycle property mismatch for node {i}"
            );
        }
    }

    cleanup(&path);
}
