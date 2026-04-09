use crate::error::GraphError;
use crate::storage::pager::Pager;
use crate::storage::record::RecordAddress;

const STRING_OVERFLOW_PAGE_TYPE: u8 = 0x07;
const OVERFLOW_HEADER_SIZE: usize = 12;

pub struct StringOverflowStore {
    page_size: usize,
}

impl StringOverflowStore {
    pub fn new(page_size: usize) -> Self {
        Self { page_size }
    }

    fn data_capacity(&self) -> usize {
        self.page_size - OVERFLOW_HEADER_SIZE
    }

    pub fn write_string(
        &self,
        pager: &mut impl Pager,
        text: &str,
    ) -> Result<RecordAddress, GraphError> {
        let bytes = text.as_bytes();
        let cap = self.data_capacity();
        let pages_needed = bytes.len().div_ceil(cap);
        let mut first_addr = RecordAddress::NULL;
        let mut prev_pgno: Option<u32> = None;

        for chunk_idx in 0..pages_needed {
            let (pgno, mut page) = pager.alloc_page()?;
            let data = page.data_mut()?;

            data[0] = STRING_OVERFLOW_PAGE_TYPE;
            data[1] = 0;

            let start = chunk_idx * cap;
            let end = (start + cap).min(bytes.len());
            let chunk = &bytes[start..end];
            let chunk_len = chunk.len() as u32;

            data[4..8].copy_from_slice(&chunk_len.to_le_bytes());
            data[8..12].copy_from_slice(&0u32.to_le_bytes());

            data[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk.len()].copy_from_slice(chunk);

            pager.write_page(&page)?;

            if chunk_idx == 0 {
                first_addr = RecordAddress::new(pgno, 0);
            }
            if let Some(prev) = prev_pgno {
                let mut prev_page = pager.get_page(prev)?;
                let pd = prev_page.data_mut()?;
                pd[8..12].copy_from_slice(&pgno.to_le_bytes());
                pager.write_page(&prev_page)?;
            }
            prev_pgno = Some(pgno);
        }

        Ok(first_addr)
    }

    pub fn read_string(
        &self,
        pager: &mut impl Pager,
        addr: RecordAddress,
    ) -> Result<String, GraphError> {
        if addr.is_null() {
            return Ok(String::new());
        }

        let mut result = Vec::new();
        let mut current_pgno = addr.page;

        loop {
            if current_pgno == 0 || current_pgno > pager.db_size() {
                break;
            }

            let page = pager.get_page(current_pgno)?;
            let data = page.data();

            let chunk_len = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
            let next_pgno = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);

            let end = (OVERFLOW_HEADER_SIZE + chunk_len).min(data.len());
            result.extend_from_slice(&data[OVERFLOW_HEADER_SIZE..end]);

            current_pgno = next_pgno;
        }

        String::from_utf8(result)
            .map_err(|_| GraphError::PagerError("invalid UTF-8 in overflow".into()))
    }

    pub fn delete_string(
        &self,
        pager: &mut impl Pager,
        addr: RecordAddress,
    ) -> Result<(), GraphError> {
        if addr.is_null() {
            return Ok(());
        }

        let mut current_pgno = addr.page;
        while current_pgno != 0 && current_pgno <= pager.db_size() {
            let page = pager.get_page(current_pgno)?;
            let data = page.data();
            let next_pgno = u32::from_le_bytes([data[8], data[9], data[10], data[11]]);

            let mut page = pager.get_page(current_pgno)?;
            page.data_mut()?[0..OVERFLOW_HEADER_SIZE].fill(0);
            pager.write_page(&page)?;

            current_pgno = next_pgno;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pager_bridge::FilePager;
    use tempfile::NamedTempFile;

    fn temp_path() -> String {
        let f = NamedTempFile::new().unwrap();
        let p = f.path().to_str().unwrap().to_string();
        drop(f);
        p
    }

    fn setup_pager(path: &str) -> FilePager {
        let mut pager = FilePager::open(path, 4096).unwrap();
        pager.begin_write().unwrap();
        let (_, mut page) = pager.alloc_page().unwrap();
        page.data_mut().unwrap().fill(0);
        pager.write_page(&page).unwrap();
        pager.commit().unwrap();
        pager
    }

    #[test]
    fn test_write_read_short_string() {
        let path = temp_path();
        let mut pager = setup_pager(&path);
        let store = StringOverflowStore::new(4096);

        pager.begin_write().unwrap();
        let addr = store.write_string(&mut pager, "hello world").unwrap();
        pager.commit().unwrap();

        let result = store.read_string(&mut pager, addr).unwrap();
        assert_eq!(result, "hello world");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_write_read_long_string() {
        let path = temp_path();
        let mut pager = setup_pager(&path);
        let store = StringOverflowStore::new(4096);

        let long_text: String = "abcdefghij".repeat(500);
        assert!(long_text.len() > 4096);

        pager.begin_write().unwrap();
        let addr = store.write_string(&mut pager, &long_text).unwrap();
        pager.commit().unwrap();

        let result = store.read_string(&mut pager, addr).unwrap();
        assert_eq!(result, long_text);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_write_read_exact_page_boundary() {
        let path = temp_path();
        let mut pager = setup_pager(&path);
        let store = StringOverflowStore::new(4096);
        let cap = store.data_capacity();

        let exact_text: String = "x".repeat(cap);

        pager.begin_write().unwrap();
        let addr = store.write_string(&mut pager, &exact_text).unwrap();
        pager.commit().unwrap();

        let result = store.read_string(&mut pager, addr).unwrap();
        assert_eq!(result, exact_text);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_write_read_empty_string() {
        let path = temp_path();
        let mut pager = setup_pager(&path);
        let store = StringOverflowStore::new(4096);

        pager.begin_write().unwrap();
        let addr = store.write_string(&mut pager, "").unwrap();
        pager.commit().unwrap();

        let result = store.read_string(&mut pager, addr).unwrap();
        assert_eq!(result, "");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_read_null_addr() {
        let path = temp_path();
        let mut pager = setup_pager(&path);
        let store = StringOverflowStore::new(4096);

        let result = store.read_string(&mut pager, RecordAddress::NULL).unwrap();
        assert_eq!(result, "");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_multiple_strings() {
        let path = temp_path();
        let mut pager = setup_pager(&path);
        let store = StringOverflowStore::new(4096);

        let medium = "medium text with some content".repeat(10);
        let long = "long".repeat(2000);
        let texts = vec!["short", &medium, &long];

        pager.begin_write().unwrap();
        let addrs: Vec<RecordAddress> = texts
            .iter()
            .map(|t| store.write_string(&mut pager, t).unwrap())
            .collect();
        pager.commit().unwrap();

        for (addr, expected) in addrs.iter().zip(texts.iter()) {
            let result = store.read_string(&mut pager, *addr).unwrap();
            assert_eq!(&result, expected);
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_unicode_string() {
        let path = temp_path();
        let mut pager = setup_pager(&path);
        let store = StringOverflowStore::new(4096);

        let unicode = "日本語のテスト🚀 émojis and ñ special chars";

        pager.begin_write().unwrap();
        let addr = store.write_string(&mut pager, unicode).unwrap();
        pager.commit().unwrap();

        let result = store.read_string(&mut pager, addr).unwrap();
        assert_eq!(result, unicode);

        let _ = std::fs::remove_file(&path);
    }
}
