use crate::error::GraphError;
use crate::storage::pager_bridge::GraphPager;

const LABEL_INDEX_PAGE_TYPE: u8 = 0x08;
const LABEL_INDEX_HEADER_SIZE: usize = 12;

pub struct LabelIndex {
    page_size: usize,
}

impl LabelIndex {
    pub fn new(page_size: usize) -> Self {
        Self { page_size }
    }

    fn bits_per_page(&self) -> usize {
        (self.page_size - LABEL_INDEX_HEADER_SIZE) * 8
    }

    fn bit_offset(&self, node_id: u64) -> usize {
        (node_id as usize) % self.bits_per_page()
    }

    fn read_or_create_page(
        &self,
        pager: &mut GraphPager,
        pgno: u32,
        label_token: u32,
    ) -> Result<(), GraphError> {
        while pager.db_size() < pgno {
            let (_, mut page) = pager.alloc_page()?;
            let data = page.data_mut()?;
            data[0] = LABEL_INDEX_PAGE_TYPE;
            data[1] = 0;
            data[2..4].copy_from_slice(&0u16.to_le_bytes());
            data[4..8].copy_from_slice(&0u32.to_le_bytes());
            data[8..12].copy_from_slice(&label_token.to_le_bytes());
            pager.write_page(&page)?;
        }
        Ok(())
    }

    pub fn set_label(
        &self,
        pager: &mut GraphPager,
        label_token: u32,
        node_id: u64,
        index_root: u32,
    ) -> Result<(), GraphError> {
        let page_idx = node_id as u32 / self.bits_per_page() as u32;
        let pgno = index_root + page_idx;
        self.read_or_create_page(pager, pgno, label_token)?;

        let mut page = pager.get_page(pgno)?;
        let data = page.data_mut()?;
        let bit = self.bit_offset(node_id);
        let byte_idx = LABEL_INDEX_HEADER_SIZE + bit / 8;
        let bit_idx = bit % 8;
        data[byte_idx] |= 1 << bit_idx;
        pager.write_page(&page)?;
        Ok(())
    }

    pub fn clear_label(
        &self,
        pager: &mut GraphPager,
        node_id: u64,
        index_root: u32,
    ) -> Result<(), GraphError> {
        let page_idx = node_id as u32 / self.bits_per_page() as u32;
        let pgno = index_root + page_idx;
        if pgno > pager.db_size() {
            return Ok(());
        }

        let mut page = pager.get_page(pgno)?;
        let data = page.data_mut()?;
        let bit = self.bit_offset(node_id);
        let byte_idx = LABEL_INDEX_HEADER_SIZE + bit / 8;
        let bit_idx = bit % 8;
        data[byte_idx] &= !(1 << bit_idx);
        pager.write_page(&page)?;
        Ok(())
    }

    pub fn has_label(
        &self,
        pager: &mut GraphPager,
        node_id: u64,
        index_root: u32,
    ) -> Result<bool, GraphError> {
        let page_idx = node_id as u32 / self.bits_per_page() as u32;
        let pgno = index_root + page_idx;
        if pgno > pager.db_size() {
            return Ok(false);
        }

        let page = pager.get_page(pgno)?;
        let data = page.data();
        let bit = self.bit_offset(node_id);
        let byte_idx = LABEL_INDEX_HEADER_SIZE + bit / 8;
        let bit_idx = bit % 8;
        Ok((data[byte_idx] >> bit_idx) & 1 == 1)
    }

    pub fn scan(
        &self,
        pager: &mut GraphPager,
        index_root: u32,
        max_node_id: u64,
    ) -> Result<Vec<u64>, GraphError> {
        let mut results = Vec::new();
        let bpp = self.bits_per_page();
        let max_pages = (max_node_id as usize).div_ceil(bpp);

        for page_idx in 0..max_pages {
            let pgno = index_root + page_idx as u32;
            if pgno > pager.db_size() {
                break;
            }

            let page = pager.get_page(pgno)?;
            let data = page.data();
            let base_id = page_idx as u64 * bpp as u64;

            for byte_offset in 0..(bpp / 8) {
                let byte = data[LABEL_INDEX_HEADER_SIZE + byte_offset];
                if byte == 0 {
                    continue;
                }
                for bit in 0..8u64 {
                    if (byte >> bit) & 1 == 1 {
                        let node_id = base_id + byte_offset as u64 * 8 + bit;
                        if node_id < max_node_id {
                            results.push(node_id);
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    pub fn count(
        &self,
        pager: &mut GraphPager,
        index_root: u32,
        max_node_id: u64,
    ) -> Result<u64, GraphError> {
        let mut total = 0u64;
        let bpp = self.bits_per_page();
        let max_pages = (max_node_id as usize).div_ceil(bpp);

        for page_idx in 0..max_pages {
            let pgno = index_root + page_idx as u32;
            if pgno > pager.db_size() {
                break;
            }

            let page = pager.get_page(pgno)?;
            let data = page.data();

            for byte_offset in 0..(bpp / 8) {
                let byte = data[LABEL_INDEX_HEADER_SIZE + byte_offset];
                total += byte.count_ones() as u64;
            }
        }

        // Subtract any bits beyond max_node_id in the last page
        let last_valid_bit = max_node_id as usize % bpp;
        if last_valid_bit > 0 && max_pages > 0 {
            let pgno = index_root + (max_pages - 1) as u32;
            if pgno <= pager.db_size() {
                let page = pager.get_page(pgno)?;
                let data = page.data();
                let last_byte_idx = last_valid_bit / 8;
                let last_bit_idx = last_valid_bit % 8;
                if last_bit_idx > 0 {
                    let byte = data[LABEL_INDEX_HEADER_SIZE + last_byte_idx];
                    let overflow_bits = byte >> last_bit_idx;
                    total -= overflow_bits.count_ones() as u64;
                }
                for byte_offset in (last_byte_idx + 1)..(bpp / 8) {
                    let byte = data[LABEL_INDEX_HEADER_SIZE + byte_offset];
                    total -= byte.count_ones() as u64;
                }
            }
        }

        Ok(total)
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

    fn setup_pager(path: &str, extra_pages: u32) -> GraphPager {
        let mut pager = GraphPager::open(path, 4096).unwrap();
        pager.begin_write().unwrap();
        for _ in 0..extra_pages {
            let (_, mut page) = pager.alloc_page().unwrap();
            page.data_mut().unwrap().fill(0);
            pager.write_page(&page).unwrap();
        }
        pager.commit().unwrap();
        pager
    }

    #[test]
    fn test_set_and_has_label() {
        let path = temp_path();
        let mut pager = setup_pager(&path, 10);
        let idx = LabelIndex::new(4096);
        let root = 5;

        pager.begin_write().unwrap();
        idx.set_label(&mut pager, 1, 0, root).unwrap();
        idx.set_label(&mut pager, 1, 7, root).unwrap();
        idx.set_label(&mut pager, 1, 100, root).unwrap();
        pager.commit().unwrap();

        assert!(idx.has_label(&mut pager, 0, root).unwrap());
        assert!(idx.has_label(&mut pager, 7, root).unwrap());
        assert!(idx.has_label(&mut pager, 100, root).unwrap());
        assert!(!idx.has_label(&mut pager, 1, root).unwrap());
        assert!(!idx.has_label(&mut pager, 50, root).unwrap());

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_clear_label() {
        let path = temp_path();
        let mut pager = setup_pager(&path, 10);
        let idx = LabelIndex::new(4096);
        let root = 5;

        pager.begin_write().unwrap();
        idx.set_label(&mut pager, 1, 42, root).unwrap();
        assert!(idx.has_label(&mut pager, 42, root).unwrap());
        idx.clear_label(&mut pager, 42, root).unwrap();
        assert!(!idx.has_label(&mut pager, 42, root).unwrap());
        pager.commit().unwrap();

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_scan() {
        let path = temp_path();
        let mut pager = setup_pager(&path, 10);
        let idx = LabelIndex::new(4096);
        let root = 5;

        pager.begin_write().unwrap();
        let ids = vec![0, 5, 10, 50, 99];
        for &id in &ids {
            idx.set_label(&mut pager, 1, id, root).unwrap();
        }
        pager.commit().unwrap();

        let scanned = idx.scan(&mut pager, root, 100).unwrap();
        assert_eq!(scanned, ids);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_count() {
        let path = temp_path();
        let mut pager = setup_pager(&path, 10);
        let idx = LabelIndex::new(4096);
        let root = 5;

        pager.begin_write().unwrap();
        for i in 0..50u64 {
            idx.set_label(&mut pager, 1, i * 2, root).unwrap(); // even numbers 0-98
        }
        pager.commit().unwrap();

        let count = idx.count(&mut pager, root, 100).unwrap();
        assert_eq!(count, 50);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_bits_per_page() {
        let idx = LabelIndex::new(4096);
        assert_eq!(idx.bits_per_page(), (4096 - 12) * 8);
        assert_eq!(idx.bits_per_page(), 32672);
    }

    #[test]
    fn test_scan_empty() {
        let path = temp_path();
        let mut pager = setup_pager(&path, 10);
        let idx = LabelIndex::new(4096);
        let root = 5;

        let scanned = idx.scan(&mut pager, root, 100).unwrap();
        assert_eq!(scanned.len(), 0);

        let _ = std::fs::remove_file(&path);
    }
}
