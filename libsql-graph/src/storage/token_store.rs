use crate::error::GraphError;
use crate::storage::page::{PageHeader, PageType, PAGE_HEADER_SIZE};
use crate::storage::pager::Pager;
use crate::storage::record::{
    address_for_id, records_per_page, RecordAddress, TOKEN_RECORD_SIZE,
};

pub const TOKEN_FLAG_IN_USE: u8 = 0b1000_0000;
pub const TOKEN_KIND_LABEL: u8 = 0;
pub const TOKEN_KIND_REL_TYPE: u8 = 1;
pub const TOKEN_MAX_INLINE_NAME: usize = 27;

#[derive(Debug, Clone)]
pub struct TokenRecord {
    pub flags: u8,
    pub token_id: u32,
    pub name_length: u8,
    pub name: [u8; TOKEN_MAX_INLINE_NAME],
}

impl TokenRecord {
    pub fn new(token_id: u32, kind: u8, name: &str) -> Self {
        let bytes = name.as_bytes();
        let len = bytes.len().min(TOKEN_MAX_INLINE_NAME);
        let mut name_buf = [0u8; TOKEN_MAX_INLINE_NAME];
        name_buf[..len].copy_from_slice(&bytes[..len]);

        Self {
            flags: TOKEN_FLAG_IN_USE | ((kind & 1) << 6),
            token_id,
            name_length: len as u8,
            name: name_buf,
        }
    }

    pub fn in_use(&self) -> bool {
        self.flags & TOKEN_FLAG_IN_USE != 0
    }

    pub fn kind(&self) -> u8 {
        (self.flags >> 6) & 1
    }

    pub fn name_str(&self) -> &str {
        std::str::from_utf8(&self.name[..self.name_length as usize]).unwrap_or("")
    }

    pub fn read(data: &[u8]) -> Self {
        let token_id = u32::from_le_bytes([data[1], data[2], data[3], 0]);
        let name_length = data[4];
        let mut name = [0u8; TOKEN_MAX_INLINE_NAME];
        let len = (name_length as usize).min(TOKEN_MAX_INLINE_NAME);
        name[..len].copy_from_slice(&data[5..5 + len]);

        Self {
            flags: data[0],
            token_id,
            name_length,
            name,
        }
    }

    pub fn write(&self, data: &mut [u8]) {
        data[0] = self.flags;
        let tid = self.token_id.to_le_bytes();
        data[1] = tid[0];
        data[2] = tid[1];
        data[3] = tid[2];
        data[4] = self.name_length;
        let len = (self.name_length as usize).min(TOKEN_MAX_INLINE_NAME);
        data[5..5 + len].copy_from_slice(&self.name[..len]);
        if 5 + len < TOKEN_RECORD_SIZE {
            data[5 + len..TOKEN_RECORD_SIZE].fill(0);
        }
    }
}

pub struct TokenStore {
    store_root: u32,
    page_size: usize,
}

impl TokenStore {
    pub fn new(store_root: u32, page_size: usize) -> Self {
        Self {
            store_root,
            page_size,
        }
    }

    pub fn records_per_page(&self) -> usize {
        records_per_page(self.page_size, TOKEN_RECORD_SIZE, PAGE_HEADER_SIZE)
    }

    pub fn address(&self, token_id: u32) -> RecordAddress {
        address_for_id(
            token_id as u64,
            self.store_root,
            self.page_size,
            TOKEN_RECORD_SIZE,
            PAGE_HEADER_SIZE,
        )
    }

    fn ensure_page_exists(
        &self,
        pager: &mut impl Pager,
        addr: &RecordAddress,
    ) -> Result<(), GraphError> {
        while pager.db_size() < addr.page {
            let (_, mut page) = pager.alloc_page()?;
            let ph = PageHeader {
                page_type: PageType::TokenStore as u8,
                flags: 0,
                record_count: 0,
                next_page: 0,
            };
            ph.write(&mut page.data_mut()?[..PAGE_HEADER_SIZE]);
            pager.write_page(&page)?;
        }
        Ok(())
    }

    pub fn create_token(
        &self,
        pager: &mut impl Pager,
        record: &TokenRecord,
    ) -> Result<RecordAddress, GraphError> {
        let addr = self.address(record.token_id);
        self.ensure_page_exists(pager, &addr)?;

        let mut page = pager.get_page(addr.page)?;
        let data = page.data_mut()?;

        let offset = addr.byte_offset(TOKEN_RECORD_SIZE, PAGE_HEADER_SIZE);
        record.write(&mut data[offset..offset + TOKEN_RECORD_SIZE]);

        let mut ph = PageHeader::read(data);
        ph.record_count += 1;
        ph.write(&mut data[..PAGE_HEADER_SIZE]);

        pager.write_page(&page)?;
        Ok(addr)
    }

    pub fn read_token(
        &self,
        pager: &mut impl Pager,
        token_id: u32,
    ) -> Result<TokenRecord, GraphError> {
        let addr = self.address(token_id);
        if addr.page > pager.db_size() {
            return Err(GraphError::InvalidPageNumber(addr.page));
        }

        let page = pager.get_page(addr.page)?;
        let data = page.data();
        let offset = addr.byte_offset(TOKEN_RECORD_SIZE, PAGE_HEADER_SIZE);
        Ok(TokenRecord::read(&data[offset..offset + TOKEN_RECORD_SIZE]))
    }

    pub fn find_by_name(
        &self,
        pager: &mut impl Pager,
        name: &str,
        kind: u8,
        next_token_id: u32,
    ) -> Result<Option<u32>, GraphError> {
        for id in 0..next_token_id {
            let addr = self.address(id);
            if addr.page > pager.db_size() {
                break;
            }
            let record = self.read_token(pager, id)?;
            if record.in_use() && record.kind() == kind && record.name_str() == name {
                return Ok(Some(id));
            }
        }
        Ok(None)
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

    fn setup_pager(path: &str) -> (FilePager, u32) {
        let mut pager = FilePager::open(path, 4096).unwrap();
        pager.begin_write().unwrap();

        let (_, mut header_page) = pager.alloc_page().unwrap();
        header_page.data_mut().unwrap()[0..8].copy_from_slice(b"LSGRAPH\0");
        pager.write_page(&header_page).unwrap();

        let (root, mut token_page) = pager.alloc_page().unwrap();
        let ph = PageHeader {
            page_type: PageType::TokenStore as u8,
            flags: 0,
            record_count: 0,
            next_page: 0,
        };
        ph.write(&mut token_page.data_mut().unwrap()[..PAGE_HEADER_SIZE]);
        pager.write_page(&token_page).unwrap();

        pager.commit().unwrap();
        (pager, root)
    }

    #[test]
    fn test_token_record_roundtrip() {
        let record = TokenRecord::new(42, TOKEN_KIND_LABEL, "Person");

        let mut buf = [0u8; TOKEN_RECORD_SIZE];
        record.write(&mut buf);

        let decoded = TokenRecord::read(&buf);
        assert!(decoded.in_use());
        assert_eq!(decoded.kind(), TOKEN_KIND_LABEL);
        assert_eq!(decoded.token_id, 42);
        assert_eq!(decoded.name_str(), "Person");
    }

    #[test]
    fn test_token_rel_type() {
        let record = TokenRecord::new(7, TOKEN_KIND_REL_TYPE, "KNOWS");

        let mut buf = [0u8; TOKEN_RECORD_SIZE];
        record.write(&mut buf);

        let decoded = TokenRecord::read(&buf);
        assert_eq!(decoded.kind(), TOKEN_KIND_REL_TYPE);
        assert_eq!(decoded.name_str(), "KNOWS");
    }

    #[test]
    fn test_token_max_length_name() {
        let long_name = "abcdefghijklmnopqrstuvwxyz_";
        assert_eq!(long_name.len(), TOKEN_MAX_INLINE_NAME);
        let record = TokenRecord::new(0, TOKEN_KIND_LABEL, long_name);

        let mut buf = [0u8; TOKEN_RECORD_SIZE];
        record.write(&mut buf);

        let decoded = TokenRecord::read(&buf);
        assert_eq!(decoded.name_str(), long_name);
    }

    #[test]
    fn test_token_name_truncation() {
        let too_long = "this_name_is_way_too_long_for_inline_storage!!";
        let record = TokenRecord::new(0, TOKEN_KIND_LABEL, too_long);
        assert_eq!(record.name_length as usize, TOKEN_MAX_INLINE_NAME);
        assert_eq!(record.name_str(), &too_long[..TOKEN_MAX_INLINE_NAME]);
    }

    #[test]
    fn test_token_store_create_and_read() {
        let path = temp_path();
        let (mut pager, root) = setup_pager(&path);

        let store = TokenStore::new(root, 4096);

        pager.begin_write().unwrap();
        let record = TokenRecord::new(0, TOKEN_KIND_LABEL, "Person");
        store.create_token(&mut pager, &record).unwrap();
        let record2 = TokenRecord::new(1, TOKEN_KIND_REL_TYPE, "KNOWS");
        store.create_token(&mut pager, &record2).unwrap();
        pager.commit().unwrap();

        let r0 = store.read_token(&mut pager, 0).unwrap();
        assert_eq!(r0.name_str(), "Person");
        assert_eq!(r0.kind(), TOKEN_KIND_LABEL);

        let r1 = store.read_token(&mut pager, 1).unwrap();
        assert_eq!(r1.name_str(), "KNOWS");
        assert_eq!(r1.kind(), TOKEN_KIND_REL_TYPE);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_token_store_find_by_name() {
        let path = temp_path();
        let (mut pager, root) = setup_pager(&path);

        let store = TokenStore::new(root, 4096);

        pager.begin_write().unwrap();
        for (id, name) in ["Person", "Company", "City"].iter().enumerate() {
            let record = TokenRecord::new(id as u32, TOKEN_KIND_LABEL, name);
            store.create_token(&mut pager, &record).unwrap();
        }
        pager.commit().unwrap();

        let found = store
            .find_by_name(&mut pager, "Company", TOKEN_KIND_LABEL, 3)
            .unwrap();
        assert_eq!(found, Some(1));

        let not_found = store
            .find_by_name(&mut pager, "Unknown", TOKEN_KIND_LABEL, 3)
            .unwrap();
        assert_eq!(not_found, None);

        let wrong_kind = store
            .find_by_name(&mut pager, "Person", TOKEN_KIND_REL_TYPE, 3)
            .unwrap();
        assert_eq!(wrong_kind, None);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_token_24bit_id() {
        let max_id: u32 = 0x00FF_FFFF;
        let record = TokenRecord::new(max_id, TOKEN_KIND_LABEL, "X");

        let mut buf = [0u8; TOKEN_RECORD_SIZE];
        record.write(&mut buf);

        let decoded = TokenRecord::read(&buf);
        assert_eq!(decoded.token_id, max_id);
    }
}
