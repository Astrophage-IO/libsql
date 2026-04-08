use crate::error::GraphError;
use crate::graph::Direction;
use crate::storage::node_store::{NodeRecord, NodeStore};
use crate::storage::pager_bridge::GraphPager;
use crate::storage::record::RecordAddress;
use crate::storage::rel_store::{RelRecord, RelStore};

pub struct NodeCursor {
    current_id: u64,
    max_id: u64,
}

impl NodeCursor {
    pub fn new(_store_root: u32, _page_size: usize, max_id: u64) -> Self {
        Self {
            current_id: 0,
            max_id,
        }
    }

    pub fn seek(&mut self, node_id: u64) {
        self.current_id = node_id;
    }

    pub fn current_id(&self) -> u64 {
        self.current_id
    }

    pub fn read(
        &self,
        pager: &mut GraphPager,
        node_store: &NodeStore,
    ) -> Result<NodeRecord, GraphError> {
        node_store.read_node(pager, self.current_id)
    }

    pub fn next(
        &mut self,
        pager: &mut GraphPager,
        node_store: &NodeStore,
    ) -> Result<bool, GraphError> {
        loop {
            self.current_id += 1;
            if self.current_id >= self.max_id {
                return Ok(false);
            }
            let record = node_store.read_node(pager, self.current_id)?;
            if record.in_use() {
                return Ok(true);
            }
        }
    }

    pub fn scan_label(
        &mut self,
        pager: &mut GraphPager,
        node_store: &NodeStore,
        label_token: u32,
    ) -> Result<bool, GraphError> {
        loop {
            if self.current_id >= self.max_id {
                return Ok(false);
            }
            let record = node_store.read_node(pager, self.current_id)?;
            if record.in_use() && record.label_token_id == label_token {
                return Ok(true);
            }
            self.current_id += 1;
        }
    }

    pub fn next_with_label(
        &mut self,
        pager: &mut GraphPager,
        node_store: &NodeStore,
        label_token: u32,
    ) -> Result<bool, GraphError> {
        self.current_id += 1;
        self.scan_label(pager, node_store, label_token)
    }
}

pub struct RelChainCursor {
    current_rel: RecordAddress,
    anchor_addr: RecordAddress,
    direction: Direction,
    type_filter: Option<u32>,
}

impl RelChainCursor {
    pub fn new(
        first_rel: RecordAddress,
        anchor_addr: RecordAddress,
        direction: Direction,
    ) -> Self {
        Self {
            current_rel: first_rel,
            anchor_addr,
            direction,
            type_filter: None,
        }
    }

    pub fn with_type_filter(mut self, type_token: u32) -> Self {
        self.type_filter = Some(type_token);
        self
    }

    pub fn next(
        &mut self,
        pager: &mut GraphPager,
        rel_store: &RelStore,
    ) -> Result<Option<(RelRecord, RecordAddress)>, GraphError> {
        loop {
            if self.current_rel.is_null() {
                return Ok(None);
            }

            let addr = self.current_rel;
            let rel = rel_store.read_rel_at(pager, addr)?;

            if !rel.in_use() {
                return Ok(None);
            }

            let is_source = rel.source_node == self.anchor_addr;
            let is_target = rel.target_node == self.anchor_addr;

            let advance = if is_source {
                rel.src_next_rel
            } else {
                rel.dst_next_rel
            };
            self.current_rel = advance;

            if let Some(type_filter) = self.type_filter {
                if rel.type_token_id != type_filter {
                    continue;
                }
            }

            let matches = match self.direction {
                Direction::Outgoing => is_source,
                Direction::Incoming => is_target,
                Direction::Both => is_source || is_target,
            };

            if matches {
                return Ok(Some((rel, addr)));
            }
        }
    }

    pub fn collect_neighbors(
        &mut self,
        pager: &mut GraphPager,
        rel_store: &RelStore,
        node_store: &NodeStore,
    ) -> Result<Vec<NeighborEntry>, GraphError> {
        let mut results = Vec::new();
        while let Some((rel, rel_addr)) = self.next(pager, rel_store)? {
            let is_source = rel.source_node == self.anchor_addr;
            let neighbor_addr = if is_source {
                rel.target_node
            } else {
                rel.source_node
            };

            let rpp = node_store.records_per_page() as u64;
            let store_root = node_store.address(0).page;
            let neighbor_id =
                (neighbor_addr.page - store_root) as u64 * rpp + neighbor_addr.slot as u64;

            results.push(NeighborEntry {
                neighbor_id,
                neighbor_addr,
                rel_addr,
                rel_type_token: rel.type_token_id,
                outgoing: is_source,
            });
        }
        Ok(results)
    }
}

#[derive(Debug, Clone)]
pub struct NeighborEntry {
    pub neighbor_id: u64,
    pub neighbor_addr: RecordAddress,
    pub rel_addr: RecordAddress,
    pub rel_type_token: u32,
    pub outgoing: bool,
}

pub fn bfs(
    pager: &mut GraphPager,
    node_store: &NodeStore,
    rel_store: &RelStore,
    start_id: u64,
    max_depth: u32,
    direction: Direction,
) -> Result<Vec<(u64, u32)>, GraphError> {
    use std::collections::{HashSet, VecDeque};

    let mut visited: HashSet<u64> = HashSet::new();
    let mut queue: VecDeque<(u64, u32)> = VecDeque::new();
    let mut result: Vec<(u64, u32)> = Vec::new();

    visited.insert(start_id);
    queue.push_back((start_id, 0));

    while let Some((current_id, depth)) = queue.pop_front() {
        result.push((current_id, depth));

        if depth >= max_depth {
            continue;
        }

        let node = node_store.read_node(pager, current_id)?;
        if !node.in_use() || node.first_rel.is_null() {
            continue;
        }

        let anchor = node_store.address(current_id);
        let mut cursor = RelChainCursor::new(node.first_rel, anchor, direction);
        let neighbors = cursor.collect_neighbors(pager, rel_store, node_store)?;

        for entry in neighbors {
            if visited.insert(entry.neighbor_id) {
                queue.push_back((entry.neighbor_id, depth + 1));
            }
        }
    }

    Ok(result)
}

pub fn shortest_path(
    pager: &mut GraphPager,
    node_store: &NodeStore,
    rel_store: &RelStore,
    start_id: u64,
    target_id: u64,
    max_depth: u32,
) -> Result<Option<Vec<u64>>, GraphError> {
    use std::collections::{HashMap, VecDeque};

    if start_id == target_id {
        return Ok(Some(vec![start_id]));
    }

    let mut parent: HashMap<u64, u64> = HashMap::new();
    let mut queue: VecDeque<(u64, u32)> = VecDeque::new();

    parent.insert(start_id, u64::MAX);
    queue.push_back((start_id, 0));

    while let Some((current_id, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }

        let node = node_store.read_node(pager, current_id)?;
        if !node.in_use() || node.first_rel.is_null() {
            continue;
        }

        let anchor = node_store.address(current_id);
        let mut cursor = RelChainCursor::new(node.first_rel, anchor, Direction::Both);
        let neighbors = cursor.collect_neighbors(pager, rel_store, node_store)?;

        for entry in neighbors {
            if parent.contains_key(&entry.neighbor_id) {
                continue;
            }
            parent.insert(entry.neighbor_id, current_id);

            if entry.neighbor_id == target_id {
                let mut path = vec![target_id];
                let mut cur = current_id;
                while cur != u64::MAX {
                    path.push(cur);
                    cur = *parent.get(&cur).unwrap_or(&u64::MAX);
                }
                path.reverse();
                return Ok(Some(path));
            }

            queue.push_back((entry.neighbor_id, depth + 1));
        }
    }

    Ok(None)
}

pub fn dfs(
    pager: &mut GraphPager,
    node_store: &NodeStore,
    rel_store: &RelStore,
    start_id: u64,
    max_depth: u32,
    direction: Direction,
) -> Result<Vec<(u64, u32)>, GraphError> {
    use std::collections::HashSet;

    let mut visited: HashSet<u64> = HashSet::new();
    let mut stack: Vec<(u64, u32)> = vec![(start_id, 0)];
    let mut result: Vec<(u64, u32)> = Vec::new();

    while let Some((current_id, depth)) = stack.pop() {
        if !visited.insert(current_id) {
            continue;
        }

        result.push((current_id, depth));

        if depth >= max_depth {
            continue;
        }

        let node = node_store.read_node(pager, current_id)?;
        if !node.in_use() || node.first_rel.is_null() {
            continue;
        }

        let anchor = node_store.address(current_id);
        let mut cursor = RelChainCursor::new(node.first_rel, anchor, direction);
        let neighbors = cursor.collect_neighbors(pager, rel_store, node_store)?;

        for entry in neighbors {
            if !visited.contains(&entry.neighbor_id) {
                stack.push((entry.neighbor_id, depth + 1));
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphEngine;
    use tempfile::NamedTempFile;

    fn temp_path() -> String {
        let f = NamedTempFile::new().unwrap();
        let p = f.path().to_str().unwrap().to_string();
        drop(f);
        p
    }

    fn build_test_graph(path: &str) -> GraphEngine {
        let mut engine = GraphEngine::create(path, 4096).unwrap();
        //    0:Alice --KNOWS--> 1:Bob
        //    0:Alice --KNOWS--> 2:Charlie
        //    1:Bob   --KNOWS--> 3:Dave
        //    2:Charlie --KNOWS--> 3:Dave
        //    3:Dave  --KNOWS--> 4:Eve
        engine.create_node("Person").unwrap(); // 0
        engine.create_node("Person").unwrap(); // 1
        engine.create_node("Person").unwrap(); // 2
        engine.create_node("Person").unwrap(); // 3
        engine.create_node("Person").unwrap(); // 4
        engine.create_relationship(0, 1, "KNOWS").unwrap();
        engine.create_relationship(0, 2, "KNOWS").unwrap();
        engine.create_relationship(1, 3, "KNOWS").unwrap();
        engine.create_relationship(2, 3, "KNOWS").unwrap();
        engine.create_relationship(3, 4, "KNOWS").unwrap();
        engine
    }

    #[test]
    fn test_node_cursor_scan() {
        let path = temp_path();
        let mut engine = build_test_graph(&path);

        let node_store = NodeStore::new(engine.db().header().node_store_root, 4096);
        let mut cursor = NodeCursor::new(
            engine.db().header().node_store_root,
            4096,
            engine.node_count(),
        );

        cursor.seek(0);
        let node = cursor.read(engine.db().pager(), &node_store).unwrap();
        assert!(node.in_use());

        let mut count = 1;
        while cursor.next(engine.db().pager(), &node_store).unwrap() {
            count += 1;
        }
        assert_eq!(count, 5);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_node_cursor_label_scan() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        engine.create_node("Person").unwrap();
        engine.create_node("Company").unwrap();
        engine.create_node("Person").unwrap();
        engine.create_node("City").unwrap();
        engine.create_node("Person").unwrap();

        let node_store = NodeStore::new(engine.db().header().node_store_root, 4096);
        let person_label = engine.get_node(0).unwrap().label_token_id;

        let mut cursor = NodeCursor::new(
            engine.db().header().node_store_root,
            4096,
            engine.node_count(),
        );

        let mut person_ids = Vec::new();
        if cursor.scan_label(engine.db().pager(), &node_store, person_label).unwrap() {
            person_ids.push(cursor.current_id());
            while cursor
                .next_with_label(engine.db().pager(), &node_store, person_label)
                .unwrap()
            {
                person_ids.push(cursor.current_id());
            }
        }
        assert_eq!(person_ids, vec![0, 2, 4]);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_rel_chain_cursor() {
        let path = temp_path();
        let mut engine = build_test_graph(&path);

        let node_store = NodeStore::new(engine.db().header().node_store_root, 4096);
        let rel_store = RelStore::new(engine.db().header().rel_store_root, 4096);

        let alice = engine.get_node(0).unwrap();
        let anchor = node_store.address(0);
        let mut cursor = RelChainCursor::new(alice.first_rel, anchor, Direction::Outgoing);
        let neighbors =
            cursor.collect_neighbors(engine.db().pager(), &rel_store, &node_store).unwrap();

        assert_eq!(neighbors.len(), 2);
        let ids: Vec<u64> = neighbors.iter().map(|n| n.neighbor_id).collect();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_bfs() {
        let path = temp_path();
        let mut engine = build_test_graph(&path);

        let node_store = NodeStore::new(engine.db().header().node_store_root, 4096);
        let rel_store = RelStore::new(engine.db().header().rel_store_root, 4096);

        let result = bfs(
            engine.db().pager(),
            &node_store,
            &rel_store,
            0,
            10,
            Direction::Outgoing,
        )
        .unwrap();

        let ids: Vec<u64> = result.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids[0], 0);
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
        assert!(ids.contains(&3));
        assert!(ids.contains(&4));
        assert_eq!(ids.len(), 5);

        let depths: std::collections::HashMap<u64, u32> = result.into_iter().collect();
        assert_eq!(depths[&0], 0);
        assert_eq!(depths[&1], 1);
        assert_eq!(depths[&2], 1);
        assert_eq!(depths[&3], 2);
        assert_eq!(depths[&4], 3);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_bfs_max_depth() {
        let path = temp_path();
        let mut engine = build_test_graph(&path);

        let node_store = NodeStore::new(engine.db().header().node_store_root, 4096);
        let rel_store = RelStore::new(engine.db().header().rel_store_root, 4096);

        let result = bfs(
            engine.db().pager(),
            &node_store,
            &rel_store,
            0,
            1,
            Direction::Outgoing,
        )
        .unwrap();

        let ids: Vec<u64> = result.iter().map(|(id, _)| *id).collect();
        assert!(ids.contains(&0));
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
        assert!(!ids.contains(&3));
        assert!(!ids.contains(&4));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_shortest_path() {
        let path = temp_path();
        let mut engine = build_test_graph(&path);

        let node_store = NodeStore::new(engine.db().header().node_store_root, 4096);
        let rel_store = RelStore::new(engine.db().header().rel_store_root, 4096);

        let path_result = shortest_path(
            engine.db().pager(),
            &node_store,
            &rel_store,
            0,
            4,
            10,
        )
        .unwrap();

        assert!(path_result.is_some());
        let p = path_result.unwrap();
        assert_eq!(*p.first().unwrap(), 0);
        assert_eq!(*p.last().unwrap(), 4);
        assert_eq!(p.len(), 4); // 0 -> 1|2 -> 3 -> 4

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_shortest_path_same_node() {
        let path = temp_path();
        let mut engine = build_test_graph(&path);

        let node_store = NodeStore::new(engine.db().header().node_store_root, 4096);
        let rel_store = RelStore::new(engine.db().header().rel_store_root, 4096);

        let result = shortest_path(
            engine.db().pager(),
            &node_store,
            &rel_store,
            2,
            2,
            10,
        )
        .unwrap();
        assert_eq!(result, Some(vec![2]));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_shortest_path_unreachable() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        engine.create_node("A").unwrap();
        engine.create_node("B").unwrap();

        let node_store = NodeStore::new(engine.db().header().node_store_root, 4096);
        let rel_store = RelStore::new(engine.db().header().rel_store_root, 4096);

        let result = shortest_path(
            engine.db().pager(),
            &node_store,
            &rel_store,
            0,
            1,
            10,
        )
        .unwrap();
        assert_eq!(result, None);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_dfs() {
        let path = temp_path();
        let mut engine = build_test_graph(&path);

        let node_store = NodeStore::new(engine.db().header().node_store_root, 4096);
        let rel_store = RelStore::new(engine.db().header().rel_store_root, 4096);

        let result = dfs(
            engine.db().pager(),
            &node_store,
            &rel_store,
            0,
            10,
            Direction::Outgoing,
        )
        .unwrap();

        let ids: Vec<u64> = result.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids[0], 0);
        assert_eq!(ids.len(), 5);
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
        assert!(ids.contains(&3));
        assert!(ids.contains(&4));

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }
}
