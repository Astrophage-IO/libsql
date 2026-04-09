use crate::error::GraphError;
use crate::graph::GraphEngine;
use crate::storage::pager::Pager;

pub fn dump_cypher<P: Pager>(engine: &mut GraphEngine<P>) -> Result<String, GraphError> {
    let mut lines = Vec::new();
    let max_node = engine.db().header().next_node_id;
    let max_rel = engine.db().header().next_rel_id;

    for id in 0..max_node {
        let node = engine.get_node(id)?;
        if !node.in_use() {
            continue;
        }

        let label = engine.get_label_name(node.label_token_id)?;
        let props = engine.get_all_node_properties(id)?;
        let props_str = if props.is_empty() {
            String::new()
        } else {
            let kv: Vec<String> = props
                .iter()
                .map(|(k, v)| {
                    let val_str = match v {
                        crate::storage::property_store::PropertyValue::Null => "null".into(),
                        crate::storage::property_store::PropertyValue::Bool(b) => b.to_string(),
                        crate::storage::property_store::PropertyValue::Int32(n) => n.to_string(),
                        crate::storage::property_store::PropertyValue::Int64(n) => n.to_string(),
                        crate::storage::property_store::PropertyValue::Float64(f) => f.to_string(),
                        crate::storage::property_store::PropertyValue::ShortString(s) => {
                            format!("'{}'", s.replace('\'', "\\'"))
                        }
                        crate::storage::property_store::PropertyValue::Overflow(_) => {
                            "'<overflow>'".into()
                        }
                    };
                    format!("{k}: {val_str}")
                })
                .collect();
            format!(" {{{}}}", kv.join(", "))
        };

        lines.push(format!("CREATE (n{id}:{label}{props_str});"));
    }

    for id in 0..max_rel {
        let rel = engine.get_rel(id)?;
        if !rel.in_use() {
            continue;
        }

        let type_name = engine.get_rel_type_name(rel.type_token_id)?;
        let src_id = {
            let rpp = engine.node_store().records_per_page() as u64;
            let root = engine.db().header().node_store_root;
            (rel.source_node.page - root) as u64 * rpp + rel.source_node.slot as u64
        };
        let dst_id = {
            let rpp = engine.node_store().records_per_page() as u64;
            let root = engine.db().header().node_store_root;
            (rel.target_node.page - root) as u64 * rpp + rel.target_node.slot as u64
        };

        lines.push(format!("MATCH (a) WHERE id(a) = {src_id} MATCH (b) WHERE id(b) = {dst_id} CREATE (a)-[:{type_name}]->(b);"));
    }

    Ok(lines.join("\n"))
}

pub fn dump_stats<P: Pager>(engine: &mut GraphEngine<P>) -> Result<String, GraphError> {
    let schema = engine.schema()?;
    let mut lines = Vec::new();

    lines.push(format!("Nodes: {}", schema.node_count));
    lines.push(format!("Edges: {}", schema.edge_count));
    lines.push(format!("Labels: {}", schema.labels.len()));
    for l in &schema.labels {
        lines.push(format!("  :{} (token_id={})", l.name, l.token_id));
    }
    lines.push(format!("Relationship types: {}", schema.rel_types.len()));
    for r in &schema.rel_types {
        lines.push(format!("  :{} (token_id={})", r.name, r.token_id));
    }

    Ok(lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::property_store::PropertyValue;
    use tempfile::NamedTempFile;

    fn temp_path() -> String {
        let f = NamedTempFile::new().unwrap();
        let p = f.path().to_str().unwrap().to_string();
        drop(f);
        p
    }

    #[test]
    fn test_dump_cypher() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        engine.create_node("Person").unwrap();
        engine.create_node("Person").unwrap();
        engine
            .set_node_property(0, "name", PropertyValue::ShortString("Alice".into()))
            .unwrap();
        engine.create_relationship(0, 1, "KNOWS").unwrap();

        let dump = dump_cypher(&mut engine).unwrap();
        assert!(dump.contains("CREATE (n0:Person"));
        assert!(dump.contains("name: 'Alice'"));
        assert!(dump.contains(":KNOWS"));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_dump_stats() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        engine.create_node("Person").unwrap();
        engine.create_node("Company").unwrap();
        engine.create_relationship(0, 1, "WORKS_AT").unwrap();

        let stats = dump_stats(&mut engine).unwrap();
        assert!(stats.contains("Nodes: 2"));
        assert!(stats.contains("Edges: 1"));
        assert!(stats.contains(":Person"));
        assert!(stats.contains(":Company"));
        assert!(stats.contains(":WORKS_AT"));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_dump_empty_graph() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        let dump = dump_cypher(&mut engine).unwrap();
        assert!(dump.is_empty());

        let stats = dump_stats(&mut engine).unwrap();
        assert!(stats.contains("Nodes: 0"));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }
}
