use std::collections::HashMap;

use crate::error::GraphError;
use crate::storage::pager::Pager;
use crate::storage::pager_bridge::PageHandle;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxState {
    None,
    Write,
}

pub struct MemPager {
    pages: HashMap<u32, Vec<u8>>,
    page_size: usize,
    db_size: u32,
    tx_state: TxState,
    snapshot: Option<(HashMap<u32, Vec<u8>>, u32)>,
}

impl MemPager {
    pub fn new(page_size: usize) -> Self {
        Self {
            pages: HashMap::new(),
            page_size,
            db_size: 0,
            tx_state: TxState::None,
            snapshot: None,
        }
    }
}

impl Pager for MemPager {
    fn db_size(&self) -> u32 {
        self.db_size
    }

    fn page_size(&self) -> usize {
        self.page_size
    }

    fn get_page(&mut self, pgno: u32) -> Result<PageHandle, GraphError> {
        if pgno == 0 || pgno > self.db_size {
            return Err(GraphError::InvalidPageNumber(pgno));
        }
        let data = self
            .pages
            .get(&pgno)
            .cloned()
            .unwrap_or_else(|| vec![0u8; self.page_size]);
        Ok(PageHandle::new(data, pgno, self.page_size))
    }

    fn alloc_page(&mut self) -> Result<(u32, PageHandle), GraphError> {
        if self.tx_state != TxState::Write {
            return Err(GraphError::NoTransaction);
        }
        self.db_size += 1;
        let pgno = self.db_size;
        let buf = vec![0u8; self.page_size];
        self.pages.insert(pgno, buf.clone());
        Ok((pgno, PageHandle::new(buf, pgno, self.page_size)))
    }

    fn write_page(&mut self, handle: &PageHandle) -> Result<(), GraphError> {
        if self.tx_state != TxState::Write {
            return Err(GraphError::NoTransaction);
        }
        self.pages.insert(handle.page_number(), handle.data().to_vec());
        Ok(())
    }

    fn begin_read(&mut self) -> Result<(), GraphError> {
        Ok(())
    }

    fn begin_write(&mut self) -> Result<(), GraphError> {
        if self.tx_state == TxState::Write {
            return Err(GraphError::TransactionActive);
        }
        self.tx_state = TxState::Write;
        self.snapshot = Some((self.pages.clone(), self.db_size));
        Ok(())
    }

    fn commit(&mut self) -> Result<(), GraphError> {
        if self.tx_state != TxState::Write {
            return Err(GraphError::NoTransaction);
        }
        self.snapshot = None;
        self.tx_state = TxState::None;
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), GraphError> {
        if self.tx_state != TxState::Write {
            return Err(GraphError::NoTransaction);
        }
        if let Some((pages, db_size)) = self.snapshot.take() {
            self.pages = pages;
            self.db_size = db_size;
        }
        self.tx_state = TxState::None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mem_pager_alloc_write_read() {
        let mut pager = MemPager::new(4096);
        pager.begin_write().unwrap();

        let (pgno, mut handle) = pager.alloc_page().unwrap();
        assert_eq!(pgno, 1);
        handle.data_mut().unwrap()[0] = 0xAB;
        pager.write_page(&handle).unwrap();
        pager.commit().unwrap();

        let h = pager.get_page(1).unwrap();
        assert_eq!(h.data()[0], 0xAB);
    }

    #[test]
    fn test_mem_pager_rollback() {
        let mut pager = MemPager::new(4096);
        pager.begin_write().unwrap();
        let (_, mut handle) = pager.alloc_page().unwrap();
        handle.data_mut().unwrap()[0] = 0xFF;
        pager.write_page(&handle).unwrap();
        pager.commit().unwrap();
        assert_eq!(pager.db_size(), 1);

        pager.begin_write().unwrap();
        let (_, mut handle2) = pager.alloc_page().unwrap();
        handle2.data_mut().unwrap()[0] = 0xBB;
        pager.write_page(&handle2).unwrap();
        pager.rollback().unwrap();

        assert_eq!(pager.db_size(), 1);
        let h = pager.get_page(1).unwrap();
        assert_eq!(h.data()[0], 0xFF);
    }

    #[test]
    fn test_mem_pager_multiple_pages() {
        let mut pager = MemPager::new(4096);
        pager.begin_write().unwrap();
        for i in 1..=10u32 {
            let (pgno, mut handle) = pager.alloc_page().unwrap();
            assert_eq!(pgno, i);
            handle.data_mut().unwrap()[0..4].copy_from_slice(&i.to_le_bytes());
            pager.write_page(&handle).unwrap();
        }
        pager.commit().unwrap();

        assert_eq!(pager.db_size(), 10);
        for i in 1..=10u32 {
            let h = pager.get_page(i).unwrap();
            assert_eq!(&h.data()[0..4], &i.to_le_bytes());
        }
    }
}
