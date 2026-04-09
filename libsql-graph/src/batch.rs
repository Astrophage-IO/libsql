use crate::error::GraphError;
use crate::graph::GraphEngine;
use crate::storage::property_store::PropertyValue;

pub struct BatchNodeBuilder {
    entries: Vec<BatchNode>,
}

struct BatchNode {
    label: String,
    properties: Vec<(String, PropertyValue)>,
}

impl BatchNodeBuilder {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn add(mut self, label: &str) -> Self {
        self.entries.push(BatchNode {
            label: label.to_string(),
            properties: Vec::new(),
        });
        self
    }

    pub fn add_with_props(mut self, label: &str, properties: Vec<(&str, PropertyValue)>) -> Self {
        self.entries.push(BatchNode {
            label: label.to_string(),
            properties: properties
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        });
        self
    }

    pub fn count(&self) -> usize {
        self.entries.len()
    }

    pub fn execute(self, engine: &mut GraphEngine) -> Result<Vec<u64>, GraphError> {
        let mut ids = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            let node_id = engine.create_node(&entry.label)?;
            ids.push(node_id);
        }
        for (i, entry) in self.entries.iter().enumerate() {
            for (key, value) in &entry.properties {
                engine.set_node_property(ids[i], key, value.clone())?;
            }
        }
        Ok(ids)
    }
}

pub struct BatchRelBuilder {
    entries: Vec<BatchRel>,
}

struct BatchRel {
    source_id: u64,
    target_id: u64,
    rel_type: String,
}

impl BatchRelBuilder {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn add(mut self, source_id: u64, target_id: u64, rel_type: &str) -> Self {
        self.entries.push(BatchRel {
            source_id,
            target_id,
            rel_type: rel_type.to_string(),
        });
        self
    }

    pub fn count(&self) -> usize {
        self.entries.len()
    }

    pub fn execute(self, engine: &mut GraphEngine) -> Result<Vec<u64>, GraphError> {
        let mut ids = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            let rel_id =
                engine.create_relationship(entry.source_id, entry.target_id, &entry.rel_type)?;
            ids.push(rel_id);
        }
        Ok(ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn temp_path() -> String {
        let f = NamedTempFile::new().unwrap();
        let p = f.path().to_str().unwrap().to_string();
        drop(f);
        p
    }

    #[test]
    fn test_batch_create_nodes() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();

        let ids = BatchNodeBuilder::new()
            .add("Person")
            .add("Person")
            .add("Company")
            .execute(&mut engine)
            .unwrap();

        assert_eq!(ids.len(), 3);
        assert_eq!(engine.node_count(), 3);
        assert_eq!(ids, vec![0, 1, 2]);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_batch_create_nodes_with_props() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();

        let ids = BatchNodeBuilder::new()
            .add_with_props(
                "Person",
                vec![
                    ("name", PropertyValue::ShortString("Alice".into())),
                    ("age", PropertyValue::Int32(30)),
                ],
            )
            .add_with_props(
                "Person",
                vec![("name", PropertyValue::ShortString("Bob".into()))],
            )
            .execute(&mut engine)
            .unwrap();

        assert_eq!(ids.len(), 2);

        let name = engine.get_node_property(0, "name").unwrap();
        assert_eq!(name, Some(PropertyValue::ShortString("Alice".into())));

        let age = engine.get_node_property(0, "age").unwrap();
        assert_eq!(age, Some(PropertyValue::Int32(30)));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_batch_create_relationships() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();

        BatchNodeBuilder::new()
            .add("Person")
            .add("Person")
            .add("Person")
            .execute(&mut engine)
            .unwrap();

        let rel_ids = BatchRelBuilder::new()
            .add(0, 1, "KNOWS")
            .add(0, 2, "KNOWS")
            .add(1, 2, "FOLLOWS")
            .execute(&mut engine)
            .unwrap();

        assert_eq!(rel_ids.len(), 3);
        assert_eq!(engine.edge_count(), 3);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_batch_large() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();

        let mut builder = BatchNodeBuilder::new();
        for _ in 0..200 {
            builder = builder.add("Node");
        }
        let ids = builder.execute(&mut engine).unwrap();
        assert_eq!(ids.len(), 200);
        assert_eq!(engine.node_count(), 200);

        let node = engine.get_node(199).unwrap();
        assert!(node.in_use());

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }
}
