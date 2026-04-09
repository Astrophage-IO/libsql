// TODO: Replace this pure-Rust file-based pager with the real sqlite3Pager FFI
// once pager symbols are exposed in libsql-ffi. The public API is designed to
// match the eventual FFI wrapper 1:1.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::GraphError;
use crate::storage::pager::Pager;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TxState {
    None,
    Write,
}

pub struct FilePager {
    file: File,
    page_size: usize,
    db_size: u32,
    tx_state: TxState,
    dirty_pages: HashMap<u32, Vec<u8>>,
    original_pages: HashMap<u32, Vec<u8>>,
}

impl FilePager {
    pub fn open(path: &str, page_size: u32) -> Result<Self, GraphError> {
        let p = Path::new(path);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(p)?;

        let file_len = file.metadata()?.len();
        let ps = page_size as usize;
        let db_size = if file_len == 0 {
            0
        } else {
            (file_len / ps as u64) as u32
        };

        Ok(Self {
            file,
            page_size: ps,
            db_size,
            tx_state: TxState::None,
            dirty_pages: HashMap::new(),
            original_pages: HashMap::new(),
        })
    }

    pub fn close(&mut self) -> Result<(), GraphError> {
        if self.tx_state == TxState::Write {
            self.rollback()?;
        }
        self.tx_state = TxState::None;
        self.file.sync_all()?;
        Ok(())
    }
}

impl Pager for FilePager {
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

        if let Some(data) = self.dirty_pages.get(&pgno) {
            return Ok(PageHandle {
                data: data.clone(),
                pgno,
                page_size: self.page_size,
            });
        }

        let offset = (pgno as u64 - 1) * self.page_size as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; self.page_size];
        self.file.read_exact(&mut buf)?;

        Ok(PageHandle {
            data: buf,
            pgno,
            page_size: self.page_size,
        })
    }

    fn alloc_page(&mut self) -> Result<(u32, PageHandle), GraphError> {
        if self.tx_state != TxState::Write {
            return Err(GraphError::NoTransaction);
        }

        self.db_size += 1;
        let pgno = self.db_size;
        let buf = vec![0u8; self.page_size];

        self.dirty_pages.insert(pgno, buf.clone());

        Ok((
            pgno,
            PageHandle {
                data: buf,
                pgno,
                page_size: self.page_size,
            },
        ))
    }

    fn write_page(&mut self, handle: &PageHandle) -> Result<(), GraphError> {
        if self.tx_state != TxState::Write {
            return Err(GraphError::NoTransaction);
        }

        if let std::collections::hash_map::Entry::Vacant(e) =
            self.original_pages.entry(handle.pgno)
        {
            let file_len = self.file.metadata()?.len();
            let offset = (handle.pgno as u64 - 1) * self.page_size as u64;
            if offset < file_len {
                self.file.seek(SeekFrom::Start(offset))?;
                let mut orig = vec![0u8; self.page_size];
                let _ = self.file.read(&mut orig);
                e.insert(orig);
            }
        }

        self.dirty_pages.insert(handle.pgno, handle.data.clone());
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
        self.dirty_pages.clear();
        self.original_pages.clear();
        Ok(())
    }

    fn commit(&mut self) -> Result<(), GraphError> {
        if self.tx_state != TxState::Write {
            return Err(GraphError::NoTransaction);
        }

        let mut pages: Vec<(u32, Vec<u8>)> = self.dirty_pages.drain().collect();
        pages.sort_by_key(|(pgno, _)| *pgno);

        for (pgno, data) in &pages {
            let offset = (*pgno as u64 - 1) * self.page_size as u64;
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.write_all(data)?;
        }
        self.file.sync_all()?;

        self.original_pages.clear();
        self.tx_state = TxState::None;
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), GraphError> {
        if self.tx_state != TxState::Write {
            return Err(GraphError::NoTransaction);
        }

        let alloc_count = self
            .dirty_pages
            .keys()
            .filter(|pgno| !self.original_pages.contains_key(pgno))
            .count() as u32;

        self.db_size = self.db_size.saturating_sub(alloc_count);
        self.dirty_pages.clear();
        self.original_pages.clear();
        self.tx_state = TxState::None;
        Ok(())
    }
}

impl Drop for FilePager {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

pub struct PageHandle {
    data: Vec<u8>,
    pgno: u32,
    page_size: usize,
}

impl PageHandle {
    pub fn new(data: Vec<u8>, pgno: u32, page_size: usize) -> Self {
        Self { data, pgno, page_size }
    }

    pub fn page_number(&self) -> u32 {
        self.pgno
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn data_mut(&mut self) -> Result<&mut [u8], GraphError> {
        Ok(&mut self.data)
    }

    pub fn page_size(&self) -> usize {
        self.page_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::{PageHeader, PageType};
    use tempfile::NamedTempFile;

    fn temp_path() -> String {
        let f = NamedTempFile::new().unwrap();
        let p = f.path().to_str().unwrap().to_string();
        drop(f);
        p
    }

    #[test]
    fn test_pager_open_close() {
        let path = temp_path();
        {
            let pager = FilePager::open(&path, 4096).unwrap();
            assert_eq!(pager.db_size(), 0);
        }
        {
            let pager = FilePager::open(&path, 4096).unwrap();
            assert_eq!(pager.db_size(), 0);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_alloc_and_write_page() {
        let path = temp_path();
        let pattern: Vec<u8> = (0..4096).map(|i| (i % 251) as u8).collect();

        {
            let mut pager = FilePager::open(&path, 4096).unwrap();
            pager.begin_write().unwrap();
            let (pgno, mut handle) = pager.alloc_page().unwrap();
            assert_eq!(pgno, 1);
            handle.data_mut().unwrap().copy_from_slice(&pattern);
            pager.write_page(&handle).unwrap();
            pager.commit().unwrap();
        }

        {
            let mut pager = FilePager::open(&path, 4096).unwrap();
            assert_eq!(pager.db_size(), 1);
            let handle = pager.get_page(1).unwrap();
            assert_eq!(handle.data(), &pattern[..]);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_multiple_pages() {
        let path = temp_path();
        let page_count = 100u32;

        {
            let mut pager = FilePager::open(&path, 4096).unwrap();
            pager.begin_write().unwrap();
            for i in 1..=page_count {
                let (_pgno, mut handle) = pager.alloc_page().unwrap();
                let data = handle.data_mut().unwrap();
                let tag = i.to_le_bytes();
                data[0..4].copy_from_slice(&tag);
                data[4092..4096].copy_from_slice(&tag);
                pager.write_page(&handle).unwrap();
            }
            pager.commit().unwrap();
        }

        {
            let mut pager = FilePager::open(&path, 4096).unwrap();
            assert_eq!(pager.db_size(), page_count);
            for i in 1..=page_count {
                let handle = pager.get_page(i).unwrap();
                let tag = i.to_le_bytes();
                assert_eq!(&handle.data()[0..4], &tag);
                assert_eq!(&handle.data()[4092..4096], &tag);
            }
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_page_header_roundtrip() {
        let original = PageHeader {
            page_type: PageType::RelStore as u8,
            flags: 0b1010_0101,
            record_count: 1234,
            next_page: 0x00DEAD42,
        };

        let mut buf = [0u8; 8];
        original.write(&mut buf);

        let decoded = PageHeader::read(&buf);
        assert_eq!(decoded.page_type, original.page_type);
        assert_eq!(decoded.flags, original.flags);
        assert_eq!(decoded.record_count, original.record_count);
        assert_eq!(decoded.next_page, original.next_page);
    }

    #[test]
    fn test_transaction_rollback() {
        let path = temp_path();

        {
            let mut pager = FilePager::open(&path, 4096).unwrap();
            pager.begin_write().unwrap();
            let (_pgno, mut handle) = pager.alloc_page().unwrap();
            handle.data_mut().unwrap()[0] = 0xAA;
            pager.write_page(&handle).unwrap();
            pager.commit().unwrap();
        }

        {
            let mut pager = FilePager::open(&path, 4096).unwrap();
            assert_eq!(pager.db_size(), 1);
            pager.begin_write().unwrap();

            let mut handle = pager.get_page(1).unwrap();
            handle.data_mut().unwrap()[0] = 0xFF;
            pager.write_page(&handle).unwrap();

            let (_, mut new_handle) = pager.alloc_page().unwrap();
            new_handle.data_mut().unwrap()[0] = 0xBB;
            pager.write_page(&new_handle).unwrap();

            pager.rollback().unwrap();
            assert_eq!(pager.db_size(), 1);
        }

        {
            let mut pager = FilePager::open(&path, 4096).unwrap();
            let handle = pager.get_page(1).unwrap();
            assert_eq!(handle.data()[0], 0xAA);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_page_handle_drop() {
        let path = temp_path();
        let mut pager = FilePager::open(&path, 4096).unwrap();
        pager.begin_write().unwrap();

        let (pgno, mut handle) = pager.alloc_page().unwrap();
        handle.data_mut().unwrap()[0] = 0x42;
        pager.write_page(&handle).unwrap();
        pager.commit().unwrap();

        drop(handle);

        let handle2 = pager.get_page(pgno).unwrap();
        assert_eq!(handle2.data()[0], 0x42);
        assert_eq!(handle2.page_number(), pgno);
        drop(handle2);

        let handle3 = pager.get_page(pgno).unwrap();
        assert_eq!(handle3.data()[0], 0x42);

        drop(pager);
        let _ = std::fs::remove_file(&path);
    }
}
