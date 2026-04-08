use crate::error::GraphError;
use crate::storage::page::{PageHeader, PageType, PAGE_HEADER_SIZE};
use crate::storage::pager_bridge::GraphPager;

pub struct FreeSpaceManager {
    freemap_root: u32,
    page_size: usize,
}

impl FreeSpaceManager {
    pub fn new(freemap_root: u32, page_size: usize) -> Self {
        Self {
            freemap_root,
            page_size,
        }
    }

    fn bitmap_capacity(&self) -> usize {
        (self.page_size - PAGE_HEADER_SIZE) * 8
    }

    fn bitmap_page_for_slot(
        &self,
        global_slot: u64,
    ) -> (u32, usize) {
        let cap = self.bitmap_capacity() as u64;
        let bitmap_index = (global_slot / cap) as u32;
        let bit_offset = (global_slot % cap) as usize;
        (self.freemap_root + bitmap_index, bit_offset)
    }

    fn ensure_bitmap_page(
        &self,
        pager: &mut GraphPager,
        bitmap_pgno: u32,
    ) -> Result<(), GraphError> {
        while pager.db_size() < bitmap_pgno {
            let (_, mut page) = pager.alloc_page()?;
            let ph = PageHeader {
                page_type: PageType::FreeBitmap as u8,
                flags: 0,
                record_count: 0,
                next_page: 0,
            };
            ph.write(&mut page.data_mut()?[..PAGE_HEADER_SIZE]);
            pager.write_page(&page)?;
        }
        Ok(())
    }

    pub fn mark_used(
        &self,
        pager: &mut GraphPager,
        global_slot: u64,
    ) -> Result<(), GraphError> {
        let (bitmap_pgno, bit_offset) = self.bitmap_page_for_slot(global_slot);
        self.ensure_bitmap_page(pager, bitmap_pgno)?;

        let mut page = pager.get_page(bitmap_pgno)?;
        let data = page.data_mut()?;

        let byte_idx = PAGE_HEADER_SIZE + bit_offset / 8;
        let bit_idx = bit_offset % 8;
        data[byte_idx] |= 1 << bit_idx;

        pager.write_page(&page)?;
        Ok(())
    }

    pub fn mark_free(
        &self,
        pager: &mut GraphPager,
        global_slot: u64,
    ) -> Result<(), GraphError> {
        let (bitmap_pgno, bit_offset) = self.bitmap_page_for_slot(global_slot);
        if bitmap_pgno > pager.db_size() {
            return Ok(());
        }

        let mut page = pager.get_page(bitmap_pgno)?;
        let data = page.data_mut()?;

        let byte_idx = PAGE_HEADER_SIZE + bit_offset / 8;
        let bit_idx = bit_offset % 8;
        data[byte_idx] &= !(1 << bit_idx);

        pager.write_page(&page)?;
        Ok(())
    }

    pub fn is_used(
        &self,
        pager: &mut GraphPager,
        global_slot: u64,
    ) -> Result<bool, GraphError> {
        let (bitmap_pgno, bit_offset) = self.bitmap_page_for_slot(global_slot);
        if bitmap_pgno > pager.db_size() {
            return Ok(false);
        }

        let page = pager.get_page(bitmap_pgno)?;
        let data = page.data();

        let byte_idx = PAGE_HEADER_SIZE + bit_offset / 8;
        let bit_idx = bit_offset % 8;
        Ok((data[byte_idx] >> bit_idx) & 1 == 1)
    }

    pub fn find_free_slot(
        &self,
        pager: &mut GraphPager,
        hint_start: u64,
        max_slots: u64,
    ) -> Result<Option<u64>, GraphError> {
        for slot in hint_start..max_slots {
            if !self.is_used(pager, slot)? {
                return Ok(Some(slot));
            }
        }
        Ok(None)
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

    fn setup_pager(path: &str) -> (GraphPager, u32) {
        let mut pager = GraphPager::open(path, 4096).unwrap();
        pager.begin_write().unwrap();

        let (_, mut header_page) = pager.alloc_page().unwrap();
        header_page.data_mut().unwrap()[0..8].copy_from_slice(b"LSGRAPH\0");
        pager.write_page(&header_page).unwrap();

        let (root, mut bitmap_page) = pager.alloc_page().unwrap();
        let ph = PageHeader {
            page_type: PageType::FreeBitmap as u8,
            flags: 0,
            record_count: 0,
            next_page: 0,
        };
        ph.write(&mut bitmap_page.data_mut().unwrap()[..PAGE_HEADER_SIZE]);
        pager.write_page(&bitmap_page).unwrap();

        pager.commit().unwrap();
        (pager, root)
    }

    #[test]
    fn test_mark_and_check() {
        let path = temp_path();
        let (mut pager, root) = setup_pager(&path);

        let fsm = FreeSpaceManager::new(root, 4096);

        pager.begin_write().unwrap();
        assert!(!fsm.is_used(&mut pager, 0).unwrap());
        assert!(!fsm.is_used(&mut pager, 100).unwrap());

        fsm.mark_used(&mut pager, 0).unwrap();
        fsm.mark_used(&mut pager, 100).unwrap();
        fsm.mark_used(&mut pager, 7).unwrap();

        assert!(fsm.is_used(&mut pager, 0).unwrap());
        assert!(fsm.is_used(&mut pager, 100).unwrap());
        assert!(fsm.is_used(&mut pager, 7).unwrap());
        assert!(!fsm.is_used(&mut pager, 1).unwrap());
        assert!(!fsm.is_used(&mut pager, 99).unwrap());

        pager.commit().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_mark_free() {
        let path = temp_path();
        let (mut pager, root) = setup_pager(&path);

        let fsm = FreeSpaceManager::new(root, 4096);

        pager.begin_write().unwrap();
        fsm.mark_used(&mut pager, 42).unwrap();
        assert!(fsm.is_used(&mut pager, 42).unwrap());

        fsm.mark_free(&mut pager, 42).unwrap();
        assert!(!fsm.is_used(&mut pager, 42).unwrap());

        pager.commit().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_find_free_slot() {
        let path = temp_path();
        let (mut pager, root) = setup_pager(&path);

        let fsm = FreeSpaceManager::new(root, 4096);

        pager.begin_write().unwrap();
        fsm.mark_used(&mut pager, 0).unwrap();
        fsm.mark_used(&mut pager, 1).unwrap();
        fsm.mark_used(&mut pager, 2).unwrap();

        let slot = fsm.find_free_slot(&mut pager, 0, 100).unwrap();
        assert_eq!(slot, Some(3));

        fsm.mark_used(&mut pager, 3).unwrap();
        let slot = fsm.find_free_slot(&mut pager, 0, 100).unwrap();
        assert_eq!(slot, Some(4));

        let slot = fsm.find_free_slot(&mut pager, 2, 100).unwrap();
        assert_eq!(slot, Some(4));

        pager.commit().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_find_free_slot_with_gap() {
        let path = temp_path();
        let (mut pager, root) = setup_pager(&path);

        let fsm = FreeSpaceManager::new(root, 4096);

        pager.begin_write().unwrap();
        fsm.mark_used(&mut pager, 0).unwrap();
        fsm.mark_used(&mut pager, 1).unwrap();
        fsm.mark_used(&mut pager, 3).unwrap();

        let slot = fsm.find_free_slot(&mut pager, 0, 100).unwrap();
        assert_eq!(slot, Some(2));

        pager.commit().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_no_free_slots() {
        let path = temp_path();
        let (mut pager, root) = setup_pager(&path);

        let fsm = FreeSpaceManager::new(root, 4096);

        pager.begin_write().unwrap();
        for i in 0..10u64 {
            fsm.mark_used(&mut pager, i).unwrap();
        }

        let slot = fsm.find_free_slot(&mut pager, 0, 10).unwrap();
        assert_eq!(slot, None);

        pager.commit().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_bitmap_capacity() {
        let fsm = FreeSpaceManager::new(2, 4096);
        let cap = fsm.bitmap_capacity();
        assert_eq!(cap, (4096 - 8) * 8);
        assert_eq!(cap, 32704);
    }
}
