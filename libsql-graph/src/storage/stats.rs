use std::collections::HashMap;

use crate::error::GraphError;

#[derive(Debug, Clone, PartialEq)]
pub struct GraphStats {
    pub node_count: u64,
    pub edge_count: u64,
    pub label_counts: HashMap<u32, u64>,
    pub rel_type_counts: HashMap<u32, u64>,
}

impl GraphStats {
    pub fn new() -> Self {
        Self {
            node_count: 0,
            edge_count: 0,
            label_counts: HashMap::new(),
            rel_type_counts: HashMap::new(),
        }
    }

    pub fn avg_degree(&self) -> f64 {
        if self.node_count == 0 {
            return 0.0;
        }
        self.edge_count as f64 / self.node_count as f64
    }

    pub fn label_selectivity(&self, label_id: u32) -> f64 {
        if self.node_count == 0 {
            return 0.0;
        }
        let count = self.label_counts.get(&label_id).copied().unwrap_or(0);
        count as f64 / self.node_count as f64
    }

    pub fn label_count_for_id(&self, label_id: u32) -> u64 {
        self.label_counts.get(&label_id).copied().unwrap_or(0)
    }

    pub fn serialize(&self, buf: &mut [u8]) -> Result<usize, GraphError> {
        let required = 20 + self.label_counts.len() * 12 + self.rel_type_counts.len() * 12;
        if buf.len() < required {
            return Err(GraphError::CorruptPage(0));
        }

        let mut pos = 0;

        buf[pos..pos + 8].copy_from_slice(&self.node_count.to_le_bytes());
        pos += 8;
        buf[pos..pos + 8].copy_from_slice(&self.edge_count.to_le_bytes());
        pos += 8;

        let label_len = self.label_counts.len() as u32;
        buf[pos..pos + 4].copy_from_slice(&label_len.to_le_bytes());
        pos += 4;
        for (&k, &v) in &self.label_counts {
            buf[pos..pos + 4].copy_from_slice(&k.to_le_bytes());
            pos += 4;
            buf[pos..pos + 8].copy_from_slice(&v.to_le_bytes());
            pos += 8;
        }

        let rel_len = self.rel_type_counts.len() as u32;
        buf[pos..pos + 4].copy_from_slice(&rel_len.to_le_bytes());
        pos += 4;
        for (&k, &v) in &self.rel_type_counts {
            buf[pos..pos + 4].copy_from_slice(&k.to_le_bytes());
            pos += 4;
            buf[pos..pos + 8].copy_from_slice(&v.to_le_bytes());
            pos += 8;
        }

        Ok(pos)
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self, GraphError> {
        if buf.len() < 20 {
            return Err(GraphError::CorruptPage(0));
        }
        let mut pos = 0;

        let node_count = u64::from_le_bytes(
            buf[pos..pos + 8]
                .try_into()
                .map_err(|_| GraphError::CorruptPage(0))?,
        );
        pos += 8;
        let edge_count = u64::from_le_bytes(
            buf[pos..pos + 8]
                .try_into()
                .map_err(|_| GraphError::CorruptPage(0))?,
        );
        pos += 8;

        let label_len = u32::from_le_bytes(
            buf[pos..pos + 4]
                .try_into()
                .map_err(|_| GraphError::CorruptPage(0))?,
        ) as usize;
        pos += 4;
        if label_len > 10_000 {
            return Err(GraphError::CorruptPage(0));
        }
        if pos + label_len * 12 > buf.len() {
            return Err(GraphError::CorruptPage(0));
        }
        let mut label_counts = HashMap::with_capacity(label_len);
        for _ in 0..label_len {
            let k = u32::from_le_bytes(
                buf[pos..pos + 4]
                    .try_into()
                    .map_err(|_| GraphError::CorruptPage(0))?,
            );
            pos += 4;
            let v = u64::from_le_bytes(
                buf[pos..pos + 8]
                    .try_into()
                    .map_err(|_| GraphError::CorruptPage(0))?,
            );
            pos += 8;
            label_counts.insert(k, v);
        }

        if pos + 4 > buf.len() {
            return Err(GraphError::CorruptPage(0));
        }
        let rel_len = u32::from_le_bytes(
            buf[pos..pos + 4]
                .try_into()
                .map_err(|_| GraphError::CorruptPage(0))?,
        ) as usize;
        pos += 4;
        if rel_len > 10_000 {
            return Err(GraphError::CorruptPage(0));
        }
        if pos + rel_len * 12 > buf.len() {
            return Err(GraphError::CorruptPage(0));
        }
        let mut rel_type_counts = HashMap::with_capacity(rel_len);
        for _ in 0..rel_len {
            let k = u32::from_le_bytes(
                buf[pos..pos + 4]
                    .try_into()
                    .map_err(|_| GraphError::CorruptPage(0))?,
            );
            pos += 4;
            let v = u64::from_le_bytes(
                buf[pos..pos + 8]
                    .try_into()
                    .map_err(|_| GraphError::CorruptPage(0))?,
            );
            pos += 8;
            rel_type_counts.insert(k, v);
        }

        Ok(Self {
            node_count,
            edge_count,
            label_counts,
            rel_type_counts,
        })
    }
}

impl Default for GraphStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stats_page_roundtrip() {
        let mut stats = GraphStats::new();
        stats.node_count = 150;
        stats.edge_count = 300;
        stats.label_counts.insert(0, 100);
        stats.label_counts.insert(1, 50);
        stats.rel_type_counts.insert(0, 200);
        stats.rel_type_counts.insert(1, 100);

        let mut buf = vec![0u8; 4096];
        let written = stats.serialize(&mut buf).unwrap();
        assert!(written > 0);

        let restored = GraphStats::deserialize(&buf).unwrap();
        assert_eq!(restored.node_count, 150);
        assert_eq!(restored.edge_count, 300);
        assert_eq!(restored.label_counts.get(&0), Some(&100));
        assert_eq!(restored.label_counts.get(&1), Some(&50));
        assert_eq!(restored.rel_type_counts.get(&0), Some(&200));
        assert_eq!(restored.rel_type_counts.get(&1), Some(&100));
    }

    #[test]
    fn test_avg_degree() {
        let mut stats = GraphStats::new();
        stats.node_count = 10;
        stats.edge_count = 20;
        assert!((stats.avg_degree() - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_avg_degree_empty() {
        let stats = GraphStats::new();
        assert!((stats.avg_degree() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_label_selectivity() {
        let mut stats = GraphStats::new();
        stats.node_count = 100;
        stats.label_counts.insert(0, 25);
        assert!((stats.label_selectivity(0) - 0.25).abs() < f64::EPSILON);
        assert!((stats.label_selectivity(99) - 0.0).abs() < f64::EPSILON);
    }
}
