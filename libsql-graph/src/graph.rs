use std::collections::HashMap;

use crate::cypher::executor::{self, QueryResult, Value};
use crate::cypher::explain;
use crate::cypher::{parser, planner};
use crate::error::GraphError;
use crate::storage::database::GraphDatabase;
use crate::storage::node_store::{NodeRecord, NodeStore};
use crate::storage::property_store::{
    PropertyBlock, PropertyRecord, PropertyStore, PropertyValue,
};
use crate::storage::record::RecordAddress;
use crate::storage::rel_store::{RelRecord, RelStore};
use crate::storage::token_store::{
    TokenRecord, TokenStore, TOKEN_KIND_LABEL, TOKEN_KIND_REL_TYPE,
};

pub struct GraphEngine {
    db: GraphDatabase,
    node_store: NodeStore,
    rel_store: RelStore,
    token_store: TokenStore,
    property_store: PropertyStore,
}

impl GraphEngine {
    pub fn create(path: &str, page_size: u32) -> Result<Self, GraphError> {
        let db = GraphDatabase::create(path, page_size)?;
        let ps = db.page_size() as usize;
        let h = db.header();

        Ok(Self {
            node_store: NodeStore::new(h.node_store_root, ps),
            rel_store: RelStore::new(h.rel_store_root, ps),
            token_store: TokenStore::new(h.token_store_root, ps),
            property_store: PropertyStore::new(h.prop_store_root, ps),
            db,
        })
    }

    pub fn open(path: &str) -> Result<Self, GraphError> {
        let db = GraphDatabase::open(path)?;
        let ps = db.page_size() as usize;
        let h = db.header();

        Ok(Self {
            node_store: NodeStore::new(h.node_store_root, ps),
            rel_store: RelStore::new(h.rel_store_root, ps),
            token_store: TokenStore::new(h.token_store_root, ps),
            property_store: PropertyStore::new(h.prop_store_root, ps),
            db,
        })
    }

    pub fn get_or_create_label(&mut self, name: &str) -> Result<u32, GraphError> {
        let next = self.db.header().next_token_id;
        if let Some(id) = self.token_store.find_by_name(
            self.db.pager(),
            name,
            TOKEN_KIND_LABEL,
            next,
        )? {
            return Ok(id);
        }

        let id = self.db.next_token_id();
        let record = TokenRecord::new(id, TOKEN_KIND_LABEL, name);
        self.token_store.create_token(self.db.pager(), &record)?;
        self.db.header_mut().label_count += 1;
        Ok(id)
    }

    pub fn get_or_create_rel_type(&mut self, name: &str) -> Result<u32, GraphError> {
        let next = self.db.header().next_token_id;
        if let Some(id) = self.token_store.find_by_name(
            self.db.pager(),
            name,
            TOKEN_KIND_REL_TYPE,
            next,
        )? {
            return Ok(id);
        }

        let id = self.db.next_token_id();
        let record = TokenRecord::new(id, TOKEN_KIND_REL_TYPE, name);
        self.token_store.create_token(self.db.pager(), &record)?;
        self.db.header_mut().rel_type_count += 1;
        Ok(id)
    }

    pub fn create_node(&mut self, label: &str) -> Result<u64, GraphError> {
        self.db.pager().begin_write()?;

        let label_id = self.get_or_create_label(label)?;
        let node_id = self.db.next_node_id();
        let record = NodeRecord::new(label_id);
        self.node_store
            .create_node(self.db.pager(), node_id, &record)?;

        self.db.header_mut().node_count += 1;
        self.flush_and_commit()?;
        Ok(node_id)
    }

    pub fn get_node(&mut self, node_id: u64) -> Result<NodeRecord, GraphError> {
        self.node_store.read_node(self.db.pager(), node_id)
    }

    pub fn create_relationship(
        &mut self,
        source_id: u64,
        target_id: u64,
        rel_type: &str,
    ) -> Result<u64, GraphError> {
        self.db.pager().begin_write()?;

        let type_id = self.get_or_create_rel_type(rel_type)?;
        let rel_id = self.db.next_rel_id();

        let src_addr = self.node_store.address(source_id);
        let dst_addr = self.node_store.address(target_id);

        let mut src_node = self.node_store.read_node(self.db.pager(), source_id)?;
        let mut dst_node = self.node_store.read_node(self.db.pager(), target_id)?;

        let mut rel = RelRecord::new(type_id, src_addr, dst_addr);

        if src_node.first_rel.is_null() {
            rel.set_first_in_src(true);
        } else {
            rel.src_next_rel = src_node.first_rel;
            rel.set_first_in_src(true);
        }

        if dst_node.first_rel.is_null() {
            rel.set_first_in_dst(true);
        } else {
            rel.dst_next_rel = dst_node.first_rel;
            rel.set_first_in_dst(true);
        }

        let rel_addr = self.rel_store.create_rel(self.db.pager(), rel_id, &rel)?;

        if !src_node.first_rel.is_null() {
            let mut old_head = self
                .rel_store
                .read_rel_at(self.db.pager(), src_node.first_rel)?;
            if old_head.source_node == src_addr {
                old_head.src_prev_rel = rel_addr;
                old_head.set_first_in_src(false);
            } else {
                old_head.dst_prev_rel = rel_addr;
                old_head.set_first_in_dst(false);
            }
            self.rel_store
                .write_rel_at(self.db.pager(), src_node.first_rel, &old_head)?;
        }

        if !dst_node.first_rel.is_null() && dst_node.first_rel != src_node.first_rel {
            let mut old_head = self
                .rel_store
                .read_rel_at(self.db.pager(), dst_node.first_rel)?;
            if old_head.target_node == dst_addr {
                old_head.dst_prev_rel = rel_addr;
                old_head.set_first_in_dst(false);
            } else {
                old_head.src_prev_rel = rel_addr;
                old_head.set_first_in_src(false);
            }
            self.rel_store
                .write_rel_at(self.db.pager(), dst_node.first_rel, &old_head)?;
        }

        src_node.first_rel = rel_addr;
        src_node.rel_count = src_node.rel_count.saturating_add(1);
        self.node_store
            .write_node(self.db.pager(), source_id, &src_node)?;

        dst_node.first_rel = rel_addr;
        dst_node.rel_count = dst_node.rel_count.saturating_add(1);
        self.node_store
            .write_node(self.db.pager(), target_id, &dst_node)?;

        self.db.header_mut().edge_count += 1;
        self.flush_and_commit()?;
        Ok(rel_id)
    }

    pub fn get_neighbors(
        &mut self,
        node_id: u64,
        direction: Direction,
    ) -> Result<Vec<(u64, RecordAddress)>, GraphError> {
        let node = self.node_store.read_node(self.db.pager(), node_id)?;
        if !node.in_use() {
            return Ok(vec![]);
        }

        let node_addr = self.node_store.address(node_id);
        let mut neighbors = Vec::new();
        let mut current = node.first_rel;

        while !current.is_null() {
            let rel = self.rel_store.read_rel_at(self.db.pager(), current)?;
            if !rel.in_use() {
                break;
            }

            let is_source = rel.source_node == node_addr;
            let is_target = rel.target_node == node_addr;

            match direction {
                Direction::Outgoing if is_source => {
                    neighbors.push((self.addr_to_node_id(rel.target_node), current));
                }
                Direction::Incoming if is_target => {
                    neighbors.push((self.addr_to_node_id(rel.source_node), current));
                }
                Direction::Both => {
                    if is_source {
                        neighbors.push((self.addr_to_node_id(rel.target_node), current));
                    }
                    if is_target && rel.source_node != rel.target_node {
                        neighbors.push((self.addr_to_node_id(rel.source_node), current));
                    }
                }
                _ => {}
            }

            if is_source {
                current = rel.src_next_rel;
            } else {
                current = rel.dst_next_rel;
            }
        }

        Ok(neighbors)
    }

    pub fn node_count(&self) -> u64 {
        self.db.header().node_count
    }

    pub fn edge_count(&self) -> u64 {
        self.db.header().edge_count
    }

    pub fn query(&mut self, cypher: &str) -> Result<QueryResult, GraphError> {
        let stmt = parser::parse(cypher)
            .map_err(|e| GraphError::PagerError(format!("parse error: {e}")))?;
        let plan = planner::plan(&stmt)
            .map_err(|e| GraphError::PagerError(format!("plan error: {e}")))?;
        executor::execute(self, &plan, &HashMap::new())
    }

    pub fn query_with_params(
        &mut self,
        cypher: &str,
        params: HashMap<String, Value>,
    ) -> Result<QueryResult, GraphError> {
        let stmt = parser::parse(cypher)
            .map_err(|e| GraphError::PagerError(format!("parse error: {e}")))?;
        let plan = planner::plan(&stmt)
            .map_err(|e| GraphError::PagerError(format!("plan error: {e}")))?;
        executor::execute(self, &plan, &params)
    }

    pub fn explain(&self, cypher: &str) -> Result<String, GraphError> {
        let stmt = parser::parse(cypher)
            .map_err(|e| GraphError::PagerError(format!("parse error: {e}")))?;
        let plan = planner::plan(&stmt)
            .map_err(|e| GraphError::PagerError(format!("plan error: {e}")))?;
        Ok(explain::explain(&plan))
    }

    pub fn get_or_create_prop_key(&mut self, name: &str) -> Result<u16, GraphError> {
        let next = self.db.header().next_token_id;
        if let Some(id) = self.token_store.find_by_name(
            self.db.pager(),
            name,
            TOKEN_KIND_LABEL,
            next,
        )? {
            return Ok(id as u16);
        }
        if let Some(id) = self.token_store.find_by_name(
            self.db.pager(),
            name,
            TOKEN_KIND_REL_TYPE,
            next,
        )? {
            return Ok(id as u16);
        }
        let id = self.db.next_token_id();
        let record = TokenRecord::new(id, TOKEN_KIND_LABEL, name);
        self.token_store.create_token(self.db.pager(), &record)?;
        Ok(id as u16)
    }

    pub fn set_node_property(
        &mut self,
        node_id: u64,
        key: &str,
        value: PropertyValue,
    ) -> Result<(), GraphError> {
        self.db.pager().begin_write()?;
        let key_id = self.get_or_create_prop_key(key)?;
        let mut node = self.node_store.read_node(self.db.pager(), node_id)?;

        if node.first_prop.is_null() {
            let prop_id = self.db.next_prop_id();
            let mut record = PropertyRecord::new();
            record.add_block(PropertyBlock::new(key_id, &value));
            let addr = self
                .property_store
                .create_record(self.db.pager(), prop_id, &record)?;
            node.first_prop = addr;
            self.node_store
                .write_node(self.db.pager(), node_id, &node)?;
        } else {
            let mut current = node.first_prop;
            let mut prev = RecordAddress::NULL;
            while !current.is_null() {
                let mut record = self
                    .property_store
                    .read_record(self.db.pager(), current)?;
                if record.set_block(key_id, PropertyBlock::new(key_id, &value)) {
                    self.property_store
                        .write_record(self.db.pager(), current, &record)?;
                    self.flush_and_commit()?;
                    return Ok(());
                }
                prev = current;
                current = record.next_prop;
            }
            let prop_id = self.db.next_prop_id();
            let mut new_record = PropertyRecord::new();
            new_record.add_block(PropertyBlock::new(key_id, &value));
            let new_addr = self
                .property_store
                .create_record(self.db.pager(), prop_id, &new_record)?;
            let mut prev_record = self
                .property_store
                .read_record(self.db.pager(), prev)?;
            prev_record.next_prop = new_addr;
            self.property_store
                .write_record(self.db.pager(), prev, &prev_record)?;
        }

        self.flush_and_commit()
    }

    pub fn get_node_property(
        &mut self,
        node_id: u64,
        key: &str,
    ) -> Result<Option<PropertyValue>, GraphError> {
        let key_id = {
            let next = self.db.header().next_token_id;
            let label = self.token_store.find_by_name(
                self.db.pager(),
                key,
                TOKEN_KIND_LABEL,
                next,
            )?;
            let rel = self.token_store.find_by_name(
                self.db.pager(),
                key,
                TOKEN_KIND_REL_TYPE,
                next,
            )?;
            match label.or(rel) {
                Some(id) => id as u16,
                None => return Ok(None),
            }
        };

        let node = self.node_store.read_node(self.db.pager(), node_id)?;
        if node.first_prop.is_null() {
            return Ok(None);
        }
        self.property_store
            .get_property(self.db.pager(), node.first_prop, key_id)
    }

    pub fn get_all_node_properties(
        &mut self,
        node_id: u64,
    ) -> Result<Vec<(String, PropertyValue)>, GraphError> {
        let node = self.node_store.read_node(self.db.pager(), node_id)?;
        if node.first_prop.is_null() {
            return Ok(vec![]);
        }
        let raw = self
            .property_store
            .get_all_properties(self.db.pager(), node.first_prop)?;

        let mut result = Vec::with_capacity(raw.len());
        for (key_id, val) in raw {
            let token = self
                .token_store
                .read_token(self.db.pager(), key_id as u32)?;
            result.push((token.name_str().to_string(), val));
        }
        Ok(result)
    }

    pub fn set_rel_property(
        &mut self,
        rel_id: u64,
        key: &str,
        value: PropertyValue,
    ) -> Result<(), GraphError> {
        self.db.pager().begin_write()?;
        let key_id = self.get_or_create_prop_key(key)?;
        let rel = self.rel_store.read_rel(self.db.pager(), rel_id)?;

        if rel.first_prop.is_null() {
            let prop_id = self.db.next_prop_id();
            let mut record = PropertyRecord::new();
            record.add_block(PropertyBlock::new(key_id, &value));
            let addr = self
                .property_store
                .create_record(self.db.pager(), prop_id, &record)?;
            let mut updated_rel = rel;
            updated_rel.first_prop = addr;
            self.rel_store
                .write_rel(self.db.pager(), rel_id, &updated_rel)?;
        } else {
            let mut current = rel.first_prop;
            let mut prev = RecordAddress::NULL;
            while !current.is_null() {
                let mut record = self
                    .property_store
                    .read_record(self.db.pager(), current)?;
                if record.set_block(key_id, PropertyBlock::new(key_id, &value)) {
                    self.property_store
                        .write_record(self.db.pager(), current, &record)?;
                    self.flush_and_commit()?;
                    return Ok(());
                }
                prev = current;
                current = record.next_prop;
            }
            let prop_id = self.db.next_prop_id();
            let mut new_record = PropertyRecord::new();
            new_record.add_block(PropertyBlock::new(key_id, &value));
            let new_addr = self
                .property_store
                .create_record(self.db.pager(), prop_id, &new_record)?;
            let mut prev_record = self
                .property_store
                .read_record(self.db.pager(), prev)?;
            prev_record.next_prop = new_addr;
            self.property_store
                .write_record(self.db.pager(), prev, &prev_record)?;
        }

        self.flush_and_commit()
    }

    pub fn get_rel_property(
        &mut self,
        rel_id: u64,
        key: &str,
    ) -> Result<Option<PropertyValue>, GraphError> {
        let key_id = {
            let next = self.db.header().next_token_id;
            let label = self.token_store.find_by_name(
                self.db.pager(),
                key,
                TOKEN_KIND_LABEL,
                next,
            )?;
            let rel = self.token_store.find_by_name(
                self.db.pager(),
                key,
                TOKEN_KIND_REL_TYPE,
                next,
            )?;
            match label.or(rel) {
                Some(id) => id as u16,
                None => return Ok(None),
            }
        };

        let rel = self.rel_store.read_rel(self.db.pager(), rel_id)?;
        if rel.first_prop.is_null() {
            return Ok(None);
        }
        self.property_store
            .get_property(self.db.pager(), rel.first_prop, key_id)
    }

    pub fn get_rel(&mut self, rel_id: u64) -> Result<RelRecord, GraphError> {
        self.rel_store.read_rel(self.db.pager(), rel_id)
    }

    pub fn get_rel_type_name(&mut self, type_token_id: u32) -> Result<String, GraphError> {
        let token = self
            .token_store
            .read_token(self.db.pager(), type_token_id)?;
        Ok(token.name_str().to_string())
    }

    pub fn delete_relationship(&mut self, rel_id: u64) -> Result<(), GraphError> {
        self.db.pager().begin_write()?;

        let rel = self.rel_store.read_rel(self.db.pager(), rel_id)?;
        if !rel.in_use() {
            self.db.pager().commit()?;
            return Ok(());
        }

        let src_id = self.addr_to_node_id(rel.source_node);
        let dst_id = self.addr_to_node_id(rel.target_node);
        let rel_addr = self.rel_store.address(rel_id);

        self.unlink_rel_from_chain(rel_addr, &rel, rel.source_node, true)?;
        if rel.source_node != rel.target_node {
            self.unlink_rel_from_chain(rel_addr, &rel, rel.target_node, false)?;
        }

        self.rel_store.delete_rel(self.db.pager(), rel_id)?;

        let mut src_node = self.node_store.read_node(self.db.pager(), src_id)?;
        src_node.rel_count = src_node.rel_count.saturating_sub(1);
        self.node_store
            .write_node(self.db.pager(), src_id, &src_node)?;

        if rel.source_node != rel.target_node {
            let mut dst_node = self.node_store.read_node(self.db.pager(), dst_id)?;
            dst_node.rel_count = dst_node.rel_count.saturating_sub(1);
            self.node_store
                .write_node(self.db.pager(), dst_id, &dst_node)?;
        }

        self.db.header_mut().edge_count = self.db.header().edge_count.saturating_sub(1);
        self.flush_and_commit()
    }

    pub fn detach_delete_node(&mut self, node_id: u64) -> Result<(), GraphError> {
        self.db.pager().begin_write()?;

        let node = self.node_store.read_node(self.db.pager(), node_id)?;
        if !node.in_use() {
            self.db.pager().commit()?;
            return Ok(());
        }

        let node_addr = self.node_store.address(node_id);
        let mut rel_ids_to_delete = Vec::new();
        let mut current = node.first_rel;
        while !current.is_null() {
            let rel = self.rel_store.read_rel_at(self.db.pager(), current)?;
            if !rel.in_use() {
                break;
            }
            let is_source = rel.source_node == node_addr;
            let rpp = self.rel_store.records_per_page() as u64;
            let rel_id = (current.page - self.db.header().rel_store_root) as u64 * rpp
                + current.slot as u64;
            rel_ids_to_delete.push(rel_id);

            current = if is_source {
                rel.src_next_rel
            } else {
                rel.dst_next_rel
            };
        }

        self.db.pager().commit()?;

        for rel_id in rel_ids_to_delete {
            self.delete_relationship(rel_id)?;
        }

        self.db.pager().begin_write()?;
        self.node_store.delete_node(self.db.pager(), node_id)?;
        self.db.header_mut().node_count = self.db.header().node_count.saturating_sub(1);
        self.flush_and_commit()
    }

    pub fn get_label_name(&mut self, label_token_id: u32) -> Result<String, GraphError> {
        let token = self
            .token_store
            .read_token(self.db.pager(), label_token_id)?;
        Ok(token.name_str().to_string())
    }

    pub fn db(&mut self) -> &mut GraphDatabase {
        &mut self.db
    }

    pub fn node_store(&self) -> &NodeStore {
        &self.node_store
    }

    pub fn rel_store(&self) -> &RelStore {
        &self.rel_store
    }

    fn unlink_rel_from_chain(
        &mut self,
        rel_addr: RecordAddress,
        rel: &RelRecord,
        node_addr: RecordAddress,
        is_src_chain: bool,
    ) -> Result<(), GraphError> {
        let (prev, next) = if is_src_chain {
            (rel.src_prev_rel, rel.src_next_rel)
        } else {
            (rel.dst_prev_rel, rel.dst_next_rel)
        };

        if !prev.is_null() {
            let mut prev_rel = self.rel_store.read_rel_at(self.db.pager(), prev)?;
            if is_src_chain && prev_rel.source_node == rel.source_node {
                prev_rel.src_next_rel = next;
            } else if is_src_chain {
                prev_rel.dst_next_rel = next;
            } else if prev_rel.target_node == rel.target_node {
                prev_rel.dst_next_rel = next;
            } else {
                prev_rel.src_next_rel = next;
            }
            self.rel_store
                .write_rel_at(self.db.pager(), prev, &prev_rel)?;
        }

        if !next.is_null() {
            let mut next_rel = self.rel_store.read_rel_at(self.db.pager(), next)?;
            if is_src_chain && next_rel.source_node == rel.source_node {
                next_rel.src_prev_rel = prev;
            } else if is_src_chain {
                next_rel.dst_prev_rel = prev;
            } else if next_rel.target_node == rel.target_node {
                next_rel.dst_prev_rel = prev;
            } else {
                next_rel.src_prev_rel = prev;
            }
            self.rel_store
                .write_rel_at(self.db.pager(), next, &next_rel)?;
        }

        let node_id = self.addr_to_node_id(node_addr);
        let mut node = self.node_store.read_node(self.db.pager(), node_id)?;
        if node.first_rel == rel_addr {
            node.first_rel = next;
            self.node_store
                .write_node(self.db.pager(), node_id, &node)?;
        }

        Ok(())
    }

    fn addr_to_node_id(&self, addr: RecordAddress) -> u64 {
        let rpp = self.node_store.records_per_page() as u64;
        let page_offset = (addr.page - self.db.header().node_store_root) as u64;
        page_offset * rpp + addr.slot as u64
    }

    fn flush_and_commit(&mut self) -> Result<(), GraphError> {
        let mut header_page = self.db.pager().get_page(1)?;
        self.db.header().write(header_page.data_mut()?)?;
        self.db.pager().write_page(&header_page)?;
        self.db.pager().commit()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
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
    fn test_create_engine() {
        let path = temp_path();
        let engine = GraphEngine::create(&path, 4096).unwrap();
        assert_eq!(engine.node_count(), 0);
        assert_eq!(engine.edge_count(), 0);
        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_create_nodes() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();

        let id0 = engine.create_node("Person").unwrap();
        let id1 = engine.create_node("Person").unwrap();
        let id2 = engine.create_node("Company").unwrap();

        assert_eq!(id0, 0);
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(engine.node_count(), 3);

        let n0 = engine.get_node(0).unwrap();
        let n2 = engine.get_node(2).unwrap();
        assert!(n0.in_use());
        assert!(n2.in_use());
        assert_ne!(n0.label_token_id, n2.label_token_id);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_create_relationship() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();

        let alice = engine.create_node("Person").unwrap();
        let bob = engine.create_node("Person").unwrap();

        let rel_id = engine.create_relationship(alice, bob, "KNOWS").unwrap();
        assert_eq!(rel_id, 0);
        assert_eq!(engine.edge_count(), 1);

        let alice_node = engine.get_node(alice).unwrap();
        assert_eq!(alice_node.rel_count, 1);
        assert!(!alice_node.first_rel.is_null());

        let bob_node = engine.get_node(bob).unwrap();
        assert_eq!(bob_node.rel_count, 1);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_get_neighbors_outgoing() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();

        let a = engine.create_node("Person").unwrap();
        let b = engine.create_node("Person").unwrap();
        let c = engine.create_node("Person").unwrap();

        engine.create_relationship(a, b, "KNOWS").unwrap();
        engine.create_relationship(a, c, "KNOWS").unwrap();

        let neighbors = engine.get_neighbors(a, Direction::Outgoing).unwrap();
        assert_eq!(neighbors.len(), 2);

        let neighbor_ids: Vec<u64> = neighbors.iter().map(|(id, _)| *id).collect();
        assert!(neighbor_ids.contains(&b));
        assert!(neighbor_ids.contains(&c));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_get_neighbors_incoming() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();

        let a = engine.create_node("Person").unwrap();
        let b = engine.create_node("Person").unwrap();
        let c = engine.create_node("Person").unwrap();

        engine.create_relationship(a, c, "KNOWS").unwrap();
        engine.create_relationship(b, c, "KNOWS").unwrap();

        let incoming = engine.get_neighbors(c, Direction::Incoming).unwrap();
        assert_eq!(incoming.len(), 2);

        let ids: Vec<u64> = incoming.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&a));
        assert!(ids.contains(&b));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_get_neighbors_both() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();

        let a = engine.create_node("Person").unwrap();
        let b = engine.create_node("Person").unwrap();
        let c = engine.create_node("Person").unwrap();

        engine.create_relationship(a, b, "KNOWS").unwrap();
        engine.create_relationship(c, a, "FOLLOWS").unwrap();

        let both = engine.get_neighbors(a, Direction::Both).unwrap();
        assert_eq!(both.len(), 2);

        let ids: Vec<u64> = both.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&b));
        assert!(ids.contains(&c));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_reopen_engine() {
        let path = temp_path();

        {
            let mut engine = GraphEngine::create(&path, 4096).unwrap();
            engine.create_node("Person").unwrap();
            engine.create_node("Company").unwrap();
            engine.create_relationship(0, 1, "WORKS_AT").unwrap();
        }

        {
            let mut engine = GraphEngine::open(&path).unwrap();
            assert_eq!(engine.node_count(), 2);
            assert_eq!(engine.edge_count(), 1);

            let neighbors = engine.get_neighbors(0, Direction::Outgoing).unwrap();
            assert_eq!(neighbors.len(), 1);
            assert_eq!(neighbors[0].0, 1);
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_set_and_get_node_property() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        let alice = engine.create_node("Person").unwrap();

        engine
            .set_node_property(alice, "name", PropertyValue::ShortString("Alice".into()))
            .unwrap();
        engine
            .set_node_property(alice, "age", PropertyValue::Int32(28))
            .unwrap();

        let name = engine.get_node_property(alice, "name").unwrap();
        assert_eq!(name, Some(PropertyValue::ShortString("Alice".into())));

        let age = engine.get_node_property(alice, "age").unwrap();
        assert_eq!(age, Some(PropertyValue::Int32(28)));

        let missing = engine.get_node_property(alice, "email").unwrap();
        assert_eq!(missing, None);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_update_node_property() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        let node = engine.create_node("Person").unwrap();

        engine
            .set_node_property(node, "age", PropertyValue::Int32(25))
            .unwrap();
        engine
            .set_node_property(node, "age", PropertyValue::Int32(26))
            .unwrap();

        let age = engine.get_node_property(node, "age").unwrap();
        assert_eq!(age, Some(PropertyValue::Int32(26)));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_get_all_node_properties() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        let node = engine.create_node("Person").unwrap();

        engine
            .set_node_property(node, "name", PropertyValue::ShortString("Bob".into()))
            .unwrap();
        engine
            .set_node_property(node, "active", PropertyValue::Bool(true))
            .unwrap();

        let props = engine.get_all_node_properties(node).unwrap();
        assert_eq!(props.len(), 2);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_properties_persist_across_reopen() {
        let path = temp_path();

        {
            let mut engine = GraphEngine::create(&path, 4096).unwrap();
            let node = engine.create_node("Person").unwrap();
            engine
                .set_node_property(node, "name", PropertyValue::ShortString("Eve".into()))
                .unwrap();
        }

        {
            let mut engine = GraphEngine::open(&path).unwrap();
            let name = engine.get_node_property(0, "name").unwrap();
            assert_eq!(name, Some(PropertyValue::ShortString("Eve".into())));
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_delete_relationship() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();

        let a = engine.create_node("Person").unwrap();
        let b = engine.create_node("Person").unwrap();
        let c = engine.create_node("Person").unwrap();

        let r0 = engine.create_relationship(a, b, "KNOWS").unwrap();
        let _r1 = engine.create_relationship(a, c, "KNOWS").unwrap();
        assert_eq!(engine.edge_count(), 2);

        engine.delete_relationship(r0).unwrap();
        assert_eq!(engine.edge_count(), 1);

        let a_node = engine.get_node(a).unwrap();
        assert_eq!(a_node.rel_count, 1);

        let b_node = engine.get_node(b).unwrap();
        assert_eq!(b_node.rel_count, 0);

        let neighbors = engine.get_neighbors(a, Direction::Outgoing).unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].0, c);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_delete_only_relationship() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();

        let a = engine.create_node("Person").unwrap();
        let b = engine.create_node("Person").unwrap();

        let r = engine.create_relationship(a, b, "KNOWS").unwrap();
        engine.delete_relationship(r).unwrap();

        assert_eq!(engine.edge_count(), 0);
        let a_node = engine.get_node(a).unwrap();
        assert_eq!(a_node.rel_count, 0);
        assert!(a_node.first_rel.is_null());

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_detach_delete_node() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();

        let a = engine.create_node("Person").unwrap();
        let b = engine.create_node("Person").unwrap();
        let c = engine.create_node("Person").unwrap();

        engine.create_relationship(a, b, "KNOWS").unwrap();
        engine.create_relationship(a, c, "KNOWS").unwrap();
        engine.create_relationship(b, c, "FRIENDS").unwrap();

        engine.detach_delete_node(a).unwrap();

        assert_eq!(engine.node_count(), 2);
        assert_eq!(engine.edge_count(), 1);

        let a_node = engine.get_node(a).unwrap();
        assert!(!a_node.in_use());

        let b_node = engine.get_node(b).unwrap();
        assert_eq!(b_node.rel_count, 1);

        let c_neighbors = engine.get_neighbors(c, Direction::Both).unwrap();
        assert_eq!(c_neighbors.len(), 1);
        assert_eq!(c_neighbors[0].0, b);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_detach_delete_isolated_node() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();

        let a = engine.create_node("Person").unwrap();
        engine.detach_delete_node(a).unwrap();

        assert_eq!(engine.node_count(), 0);
        let a_node = engine.get_node(a).unwrap();
        assert!(!a_node.in_use());

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_get_label_name() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        let node = engine.create_node("Person").unwrap();

        let label_id = engine.get_node(node).unwrap().label_token_id;
        let name = engine.get_label_name(label_id).unwrap();
        assert_eq!(name, "Person");

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_multiple_relationships() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();

        let a = engine.create_node("Person").unwrap();
        let b = engine.create_node("Person").unwrap();
        let c = engine.create_node("Person").unwrap();
        let d = engine.create_node("Person").unwrap();

        engine.create_relationship(a, b, "KNOWS").unwrap();
        engine.create_relationship(a, c, "KNOWS").unwrap();
        engine.create_relationship(a, d, "KNOWS").unwrap();
        engine.create_relationship(b, c, "KNOWS").unwrap();

        assert_eq!(engine.edge_count(), 4);

        let a_out = engine.get_neighbors(a, Direction::Outgoing).unwrap();
        assert_eq!(a_out.len(), 3);

        let b_both = engine.get_neighbors(b, Direction::Both).unwrap();
        assert_eq!(b_both.len(), 2);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }
}
