use crate::error::GraphError;
use crate::graph::GraphEngine;
use crate::storage::pager::Pager;

#[derive(Debug, Default)]
pub struct IntegrityReport {
    pub nodes_checked: u64,
    pub rels_checked: u64,
    pub errors: Vec<IntegrityError>,
}

#[derive(Debug)]
pub enum IntegrityError {
    OrphanRelationship {
        rel_id: u64,
        detail: String,
    },
    BrokenChain {
        node_id: u64,
        detail: String,
    },
    CountMismatch {
        node_id: u64,
        stored_count: u16,
        actual_count: u16,
    },
    HeaderMismatch {
        field: String,
        stored: u64,
        actual: u64,
    },
}

impl IntegrityReport {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }
}

pub fn check_integrity<P: Pager>(
    engine: &mut GraphEngine<P>,
) -> Result<IntegrityReport, GraphError> {
    let mut report = IntegrityReport::default();

    let stored_node_count = engine.node_count();
    let stored_edge_count = engine.edge_count();
    let max_node = engine.db().header().next_node_id;
    let max_rel = engine.db().header().next_rel_id;

    let mut actual_node_count = 0u64;
    let mut actual_edge_count = 0u64;

    for id in 0..max_node {
        let node = engine.get_node(id)?;
        if !node.in_use() {
            continue;
        }
        actual_node_count += 1;
        report.nodes_checked += 1;

        let mut chain_count = 0u16;
        let node_addr = engine.node_store().address(id);
        let mut current = node.first_rel;
        let mut visited = std::collections::HashSet::new();

        while !current.is_null() {
            if !visited.insert((current.page, current.slot)) {
                report.errors.push(IntegrityError::BrokenChain {
                    node_id: id,
                    detail: format!(
                        "cycle detected at page={} slot={}",
                        current.page, current.slot
                    ),
                });
                break;
            }

            let rel_store_root = engine.db().header().rel_store_root;
            let ps = engine.db().page_size() as usize;
            let store = crate::storage::rel_store::RelStore::new(rel_store_root, ps);

            if current.page > engine.db().pager().db_size() {
                report.errors.push(IntegrityError::BrokenChain {
                    node_id: id,
                    detail: format!("dangling pointer page={}", current.page),
                });
                break;
            }

            let rel = store.read_rel_at(engine.db().pager(), current)?;
            if !rel.in_use() {
                report.errors.push(IntegrityError::BrokenChain {
                    node_id: id,
                    detail: "chain points to deleted rel".into(),
                });
                break;
            }

            chain_count += 1;
            let is_source = rel.source_node == node_addr;
            current = if is_source {
                rel.src_next_rel
            } else {
                rel.dst_next_rel
            };
        }

        if chain_count != node.rel_count {
            report.errors.push(IntegrityError::CountMismatch {
                node_id: id,
                stored_count: node.rel_count,
                actual_count: chain_count,
            });
        }
    }

    for id in 0..max_rel {
        let rel = engine.get_rel(id)?;
        if !rel.in_use() {
            continue;
        }
        actual_edge_count += 1;
        report.rels_checked += 1;

        let src_addr = rel.source_node;
        if src_addr.page > engine.db().pager().db_size() {
            report.errors.push(IntegrityError::OrphanRelationship {
                rel_id: id,
                detail: "source node page out of bounds".into(),
            });
            continue;
        }

        let rpp = engine.node_store().records_per_page() as u64;
        let root = engine.db().header().node_store_root;
        let src_id = (src_addr.page - root) as u64 * rpp + src_addr.slot as u64;
        if src_id < max_node {
            let src_node = engine.get_node(src_id)?;
            if !src_node.in_use() {
                report.errors.push(IntegrityError::OrphanRelationship {
                    rel_id: id,
                    detail: format!("source node {} is deleted", src_id),
                });
            }
        }

        let dst_addr = rel.target_node;
        if dst_addr.page > engine.db().pager().db_size() {
            report.errors.push(IntegrityError::OrphanRelationship {
                rel_id: id,
                detail: "target node page out of bounds".into(),
            });
            continue;
        }

        let dst_id = (dst_addr.page - root) as u64 * rpp + dst_addr.slot as u64;
        if dst_id < max_node {
            let dst_node = engine.get_node(dst_id)?;
            if !dst_node.in_use() {
                report.errors.push(IntegrityError::OrphanRelationship {
                    rel_id: id,
                    detail: format!("target node {} is deleted", dst_id),
                });
            }
        }
    }

    if actual_node_count != stored_node_count {
        report.errors.push(IntegrityError::HeaderMismatch {
            field: "node_count".into(),
            stored: stored_node_count,
            actual: actual_node_count,
        });
    }

    if actual_edge_count != stored_edge_count {
        report.errors.push(IntegrityError::HeaderMismatch {
            field: "edge_count".into(),
            stored: stored_edge_count,
            actual: actual_edge_count,
        });
    }

    Ok(report)
}

pub fn store_stats<P: Pager>(engine: &mut GraphEngine<P>) -> Result<StoreStats, GraphError> {
    let h = engine.db().header().clone();
    let ps = engine.db().page_size() as usize;
    let db_size = engine.db().pager().db_size();

    Ok(StoreStats {
        page_size: ps,
        total_pages: db_size,
        file_size_bytes: db_size as u64 * ps as u64,
        node_store_root: h.node_store_root,
        rel_store_root: h.rel_store_root,
        prop_store_root: h.prop_store_root,
        token_store_root: h.token_store_root,
        freemap_root: h.freemap_root,
        next_node_id: h.next_node_id,
        next_rel_id: h.next_rel_id,
        next_prop_id: h.next_prop_id,
        next_token_id: h.next_token_id,
        node_count: h.node_count,
        edge_count: h.edge_count,
        label_count: h.label_count,
        rel_type_count: h.rel_type_count,
    })
}

#[derive(Debug)]
pub struct StoreStats {
    pub page_size: usize,
    pub total_pages: u32,
    pub file_size_bytes: u64,
    pub node_store_root: u32,
    pub rel_store_root: u32,
    pub prop_store_root: u32,
    pub token_store_root: u32,
    pub freemap_root: u32,
    pub next_node_id: u64,
    pub next_rel_id: u64,
    pub next_prop_id: u64,
    pub next_token_id: u32,
    pub node_count: u64,
    pub edge_count: u64,
    pub label_count: u32,
    pub rel_type_count: u32,
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
    fn test_integrity_healthy_graph() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        engine.create_node("Person").unwrap();
        engine.create_node("Person").unwrap();
        engine.create_node("Company").unwrap();
        engine.create_relationship(0, 1, "KNOWS").unwrap();
        engine.create_relationship(0, 2, "WORKS_AT").unwrap();
        engine.create_relationship(1, 2, "WORKS_AT").unwrap();

        let report = check_integrity(&mut engine).unwrap();
        assert!(report.is_ok(), "errors: {:?}", report.errors);
        assert_eq!(report.nodes_checked, 3);
        assert_eq!(report.rels_checked, 3);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_integrity_after_delete() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        engine.create_node("Person").unwrap();
        engine.create_node("Person").unwrap();
        engine.create_relationship(0, 1, "KNOWS").unwrap();
        engine.detach_delete_node(0).unwrap();

        let report = check_integrity(&mut engine).unwrap();
        assert!(report.is_ok(), "errors: {:?}", report.errors);
        assert_eq!(report.nodes_checked, 1);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_integrity_empty_graph() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        let report = check_integrity(&mut engine).unwrap();
        assert!(report.is_ok());
        assert_eq!(report.nodes_checked, 0);
        assert_eq!(report.rels_checked, 0);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_integrity_complex_graph() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();

        for _ in 0..20 {
            engine.create_node("Node").unwrap();
        }
        for i in 0u64..19 {
            engine.create_relationship(i, i + 1, "NEXT").unwrap();
        }
        engine.create_relationship(19, 0, "NEXT").unwrap();

        engine.detach_delete_node(5).unwrap();
        engine.detach_delete_node(15).unwrap();

        let report = check_integrity(&mut engine).unwrap();
        assert!(report.is_ok(), "errors: {:?}", report.errors);
        assert_eq!(report.nodes_checked, 18);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_store_stats() {
        let path = temp_path();
        let mut engine = GraphEngine::create(&path, 4096).unwrap();
        engine.create_node("Person").unwrap();
        engine.create_node("Person").unwrap();
        engine.create_relationship(0, 1, "KNOWS").unwrap();

        let stats = store_stats(&mut engine).unwrap();
        assert_eq!(stats.page_size, 4096);
        assert_eq!(stats.node_count, 2);
        assert_eq!(stats.edge_count, 1);
        assert!(stats.total_pages > 0);
        assert!(stats.file_size_bytes > 0);
        assert_eq!(stats.next_node_id, 2);
        assert_eq!(stats.next_rel_id, 1);

        drop(engine);
        let _ = std::fs::remove_file(&path);
    }
}
