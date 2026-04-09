use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::error::GraphError;
use crate::storage::pager::Pager;
use crate::storage::wal::{self, WalIndex, WalReader, WalWriter};

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
    wal_writer: Option<WalWriter>,
    wal_index: WalIndex,
    wal_path: String,
    checkpoint_threshold: u32,
    pre_tx_db_size: u32,
}

impl FilePager {
    pub fn open(path: &str, page_size: u32) -> Result<Self, GraphError> {
        let p = Path::new(path);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(p)?;

        let file_len = file.metadata()?.len();
        let ps = page_size as usize;
        let mut db_size = if file_len == 0 {
            0
        } else {
            (file_len / ps as u64) as u32
        };

        let wal_path = format!("{path}-wal");

        if let Some(mut reader) = WalReader::open(&wal_path)? {
            let replayed = reader.checkpoint(&mut file)?;
            if replayed > 0 {
                let new_len = file.metadata()?.len();
                db_size = (new_len / ps as u64) as u32;
            }
            drop(reader);
            let _ = std::fs::remove_file(&wal_path);
        }

        Ok(Self {
            file,
            page_size: ps,
            db_size,
            tx_state: TxState::None,
            dirty_pages: HashMap::new(),
            wal_writer: None,
            wal_index: WalIndex::new(),
            wal_path,
            checkpoint_threshold: 1000,
            pre_tx_db_size: db_size,
        })
    }

    pub fn close(&mut self) -> Result<(), GraphError> {
        if self.tx_state == TxState::Write {
            self.rollback()?;
        }
        self.checkpoint()?;
        if self.wal_writer.take().is_some() {
            let _ = std::fs::remove_file(&self.wal_path);
        }
        self.tx_state = TxState::None;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn checkpoint(&mut self) -> Result<(), GraphError> {
        if self.wal_writer.is_none() {
            return Ok(());
        }

        {
            let mut reader = match WalReader::open(&self.wal_path)? {
                Some(r) => r,
                None => return Ok(()),
            };
            reader.checkpoint(&mut self.file)?;
        }

        if let Some(ref mut writer) = self.wal_writer {
            writer.reset(self.page_size as u32)?;
        }

        self.wal_index.clear();

        Ok(())
    }

    pub fn set_checkpoint_threshold(&mut self, threshold: u32) {
        self.checkpoint_threshold = threshold;
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

        if let Some(offset) = self.wal_index.get(pgno) {
            if let Some(ref mut writer) = self.wal_writer {
                let data = wal::read_frame_data_at(writer.file_mut(), offset, self.page_size)?;
                return Ok(PageHandle {
                    data,
                    pgno,
                    page_size: self.page_size,
                });
            }
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
        self.pre_tx_db_size = self.db_size;
        self.dirty_pages.clear();
        Ok(())
    }

    fn commit(&mut self) -> Result<(), GraphError> {
        if self.tx_state != TxState::Write {
            return Err(GraphError::NoTransaction);
        }

        let mut pages: Vec<(u32, Vec<u8>)> = self.dirty_pages.drain().collect();
        pages.sort_by_key(|(pgno, _)| *pgno);

        if pages.is_empty() {
            self.tx_state = TxState::None;
            return Ok(());
        }

        if self.wal_writer.is_none() {
            self.wal_writer = Some(WalWriter::create(&self.wal_path, self.page_size as u32)?);
        }

        let writer = self.wal_writer.as_mut().unwrap();
        let total = pages.len();

        for (idx, (pgno, data)) in pages.iter().enumerate() {
            let is_commit = idx == total - 1;
            let frame_offset = wal::wal_header_size() as u64
                + writer.frame_count() as u64
                    * (wal::frame_header_size() as u64 + self.page_size as u64);

            writer.append_frame(
                *pgno,
                data,
                is_commit,
                if is_commit { self.db_size } else { 0 },
            )?;
            self.wal_index.insert(*pgno, frame_offset);
        }

        writer.sync()?;

        if writer.frame_count() >= self.checkpoint_threshold {
            self.checkpoint()?;
        }

        self.tx_state = TxState::None;
        Ok(())
    }

    fn rollback(&mut self) -> Result<(), GraphError> {
        if self.tx_state != TxState::Write {
            return Err(GraphError::NoTransaction);
        }

        self.db_size = self.pre_tx_db_size;
        self.dirty_pages.clear();
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
        Self {
            data,
            pgno,
            page_size,
        }
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

    #[test]
    fn test_wal_aware_reads() {
        let path = temp_path();
        let mut pager = FilePager::open(&path, 4096).unwrap();

        pager.begin_write().unwrap();
        let (pgno, mut handle) = pager.alloc_page().unwrap();
        handle.data_mut().unwrap()[0] = 0xDE;
        handle.data_mut().unwrap()[1] = 0xAD;
        pager.write_page(&handle).unwrap();
        pager.commit().unwrap();

        let wal_path = format!("{path}-wal");
        assert!(std::path::Path::new(&wal_path).exists());

        pager.begin_write().unwrap();
        let h = pager.get_page(pgno).unwrap();
        assert_eq!(h.data()[0], 0xDE);
        assert_eq!(h.data()[1], 0xAD);
        pager.commit().unwrap();

        pager.begin_write().unwrap();
        let mut h2 = pager.get_page(pgno).unwrap();
        h2.data_mut().unwrap()[0] = 0xBE;
        h2.data_mut().unwrap()[1] = 0xEF;
        pager.write_page(&h2).unwrap();
        pager.commit().unwrap();

        let h3 = pager.get_page(pgno).unwrap();
        assert_eq!(h3.data()[0], 0xBE);
        assert_eq!(h3.data()[1], 0xEF);

        drop(pager);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&wal_path);
    }

    #[test]
    fn test_auto_checkpoint() {
        let path = temp_path();
        let wal_path = format!("{path}-wal");
        let mut pager = FilePager::open(&path, 4096).unwrap();
        pager.set_checkpoint_threshold(5);

        for i in 0u32..6 {
            pager.begin_write().unwrap();
            let (_pgno, mut handle) = pager.alloc_page().unwrap();
            handle.data_mut().unwrap()[0..4].copy_from_slice(&(i + 1).to_le_bytes());
            pager.write_page(&handle).unwrap();
            pager.commit().unwrap();
        }

        let db_file_len = std::fs::metadata(&path).unwrap().len();
        assert!(db_file_len >= 5 * 4096);

        for pgno in 1..=6u32 {
            let h = pager.get_page(pgno).unwrap();
            let tag = u32::from_le_bytes(h.data()[0..4].try_into().unwrap());
            assert_eq!(tag, pgno);
        }

        drop(pager);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&wal_path);
    }

    #[test]
    fn test_clean_open_no_wal() {
        let path = temp_path();
        let wal_path = format!("{path}-wal");

        assert!(!std::path::Path::new(&wal_path).exists());

        let mut pager = FilePager::open(&path, 4096).unwrap();
        assert_eq!(pager.db_size(), 0);

        pager.begin_write().unwrap();
        let (_pgno, mut handle) = pager.alloc_page().unwrap();
        handle.data_mut().unwrap()[0] = 0x42;
        pager.write_page(&handle).unwrap();
        pager.commit().unwrap();

        assert_eq!(pager.db_size(), 1);
        let h = pager.get_page(1).unwrap();
        assert_eq!(h.data()[0], 0x42);

        drop(pager);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&wal_path);
    }

    #[test]
    fn test_wal_end_to_end_with_corruption() {
        use crate::graph::GraphEngine;
        use std::io::Write;

        let path = temp_path();
        let wal_path = format!("{path}-wal");

        {
            let mut engine = GraphEngine::create(&path, 4096).unwrap();
            engine.create_node("Person").unwrap();
            engine.create_node("Company").unwrap();
            engine
                .set_node_property(
                    0,
                    "name",
                    crate::storage::property_store::PropertyValue::ShortString("Alice".into()),
                )
                .unwrap();
            engine.create_relationship(0, 1, "WORKS_AT").unwrap();
        }

        {
            let mut engine = GraphEngine::open(&path).unwrap();
            assert_eq!(engine.node_count(), 2);
            assert_eq!(engine.edge_count(), 1);
            let name = engine.get_node_property(0, "name").unwrap();
            assert_eq!(
                name,
                Some(crate::storage::property_store::PropertyValue::ShortString(
                    "Alice".into()
                ))
            );
        }

        {
            let mut engine = GraphEngine::open(&path).unwrap();
            engine.create_node("Person").unwrap();
            engine
                .set_node_property(
                    2,
                    "name",
                    crate::storage::property_store::PropertyValue::ShortString("Bob".into()),
                )
                .unwrap();
        }

        if std::path::Path::new(&wal_path).exists() {
            let _ = std::fs::remove_file(&wal_path);
        }

        {
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(false)
                .open(&wal_path)
                .unwrap();
            f.write_all(&[0xFF; 50]).unwrap();
        }

        {
            let engine = GraphEngine::open(&path).unwrap();
            assert!(engine.node_count() >= 2);
        }

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&wal_path);
    }
}
