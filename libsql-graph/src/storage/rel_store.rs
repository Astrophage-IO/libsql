use crate::error::GraphError;
use crate::storage::page::{PageHeader, PageType, PAGE_HEADER_SIZE};
use crate::storage::pager::Pager;
use crate::storage::record::{
    address_for_id, records_per_page, RecordAddress, REL_RECORD_SIZE,
};

pub const REL_FLAG_IN_USE: u8 = 0b1000_0000;
pub const REL_FLAG_FIRST_IN_SRC: u8 = 0b0100_0000;
pub const REL_FLAG_FIRST_IN_DST: u8 = 0b0010_0000;
pub const REL_INLINE_PROP_SIZE: usize = 18;

#[derive(Debug, Clone)]
pub struct RelRecord {
    pub flags: u8,
    pub type_token_id: u32,
    pub source_node: RecordAddress,
    pub target_node: RecordAddress,
    pub src_prev_rel: RecordAddress,
    pub src_next_rel: RecordAddress,
    pub dst_prev_rel: RecordAddress,
    pub dst_next_rel: RecordAddress,
    pub first_prop: RecordAddress,
    pub inline_properties: [u8; REL_INLINE_PROP_SIZE],
}

impl RelRecord {
    pub fn new(
        type_token_id: u32,
        source_node: RecordAddress,
        target_node: RecordAddress,
    ) -> Self {
        Self {
            flags: REL_FLAG_IN_USE,
            type_token_id,
            source_node,
            target_node,
            src_prev_rel: RecordAddress::NULL,
            src_next_rel: RecordAddress::NULL,
            dst_prev_rel: RecordAddress::NULL,
            dst_next_rel: RecordAddress::NULL,
            first_prop: RecordAddress::NULL,
            inline_properties: [0u8; REL_INLINE_PROP_SIZE],
        }
    }

    pub fn in_use(&self) -> bool {
        self.flags & REL_FLAG_IN_USE != 0
    }

    pub fn is_first_in_src_chain(&self) -> bool {
        self.flags & REL_FLAG_FIRST_IN_SRC != 0
    }

    pub fn is_first_in_dst_chain(&self) -> bool {
        self.flags & REL_FLAG_FIRST_IN_DST != 0
    }

    pub fn set_first_in_src(&mut self, first: bool) {
        if first {
            self.flags |= REL_FLAG_FIRST_IN_SRC;
        } else {
            self.flags &= !REL_FLAG_FIRST_IN_SRC;
        }
    }

    pub fn set_first_in_dst(&mut self, first: bool) {
        if first {
            self.flags |= REL_FLAG_FIRST_IN_DST;
        } else {
            self.flags &= !REL_FLAG_FIRST_IN_DST;
        }
    }

    pub fn read(data: &[u8]) -> Self {
        Self {
            flags: data[0],
            type_token_id: u32::from_le_bytes([data[1], data[2], data[3], 0]),
            source_node: RecordAddress::new(
                u32::from_le_bytes([data[4], data[5], data[6], data[7]]),
                u16::from_le_bytes([data[8], data[9]]),
            ),
            target_node: RecordAddress::new(
                u32::from_le_bytes([data[10], data[11], data[12], data[13]]),
                u16::from_le_bytes([data[14], data[15]]),
            ),
            src_prev_rel: RecordAddress::new(
                u32::from_le_bytes([data[16], data[17], data[18], data[19]]),
                u16::from_le_bytes([data[20], data[21]]),
            ),
            src_next_rel: RecordAddress::new(
                u32::from_le_bytes([data[22], data[23], data[24], data[25]]),
                u16::from_le_bytes([data[26], data[27]]),
            ),
            dst_prev_rel: RecordAddress::new(
                u32::from_le_bytes([data[28], data[29], data[30], data[31]]),
                u16::from_le_bytes([data[32], data[33]]),
            ),
            dst_next_rel: RecordAddress::new(
                u32::from_le_bytes([data[34], data[35], data[36], data[37]]),
                u16::from_le_bytes([data[38], data[39]]),
            ),
            first_prop: RecordAddress::new(
                u32::from_le_bytes([data[40], data[41], data[42], data[43]]),
                u16::from_le_bytes([data[44], data[45]]),
            ),
            inline_properties: {
                let mut props = [0u8; REL_INLINE_PROP_SIZE];
                props.copy_from_slice(&data[46..64]);
                props
            },
        }
    }

    pub fn write(&self, data: &mut [u8]) {
        data[0] = self.flags;
        let tt = self.type_token_id.to_le_bytes();
        data[1] = tt[0];
        data[2] = tt[1];
        data[3] = tt[2];

        data[4..8].copy_from_slice(&self.source_node.page.to_le_bytes());
        data[8..10].copy_from_slice(&self.source_node.slot.to_le_bytes());

        data[10..14].copy_from_slice(&self.target_node.page.to_le_bytes());
        data[14..16].copy_from_slice(&self.target_node.slot.to_le_bytes());

        data[16..20].copy_from_slice(&self.src_prev_rel.page.to_le_bytes());
        data[20..22].copy_from_slice(&self.src_prev_rel.slot.to_le_bytes());

        data[22..26].copy_from_slice(&self.src_next_rel.page.to_le_bytes());
        data[26..28].copy_from_slice(&self.src_next_rel.slot.to_le_bytes());

        data[28..32].copy_from_slice(&self.dst_prev_rel.page.to_le_bytes());
        data[32..34].copy_from_slice(&self.dst_prev_rel.slot.to_le_bytes());

        data[34..38].copy_from_slice(&self.dst_next_rel.page.to_le_bytes());
        data[38..40].copy_from_slice(&self.dst_next_rel.slot.to_le_bytes());

        data[40..44].copy_from_slice(&self.first_prop.page.to_le_bytes());
        data[44..46].copy_from_slice(&self.first_prop.slot.to_le_bytes());

        data[46..64].copy_from_slice(&self.inline_properties);
    }
}

pub struct RelStore {
    store_root: u32,
    page_size: usize,
}

impl RelStore {
    pub fn new(store_root: u32, page_size: usize) -> Self {
        Self {
            store_root,
            page_size,
        }
    }

    pub fn records_per_page(&self) -> usize {
        records_per_page(self.page_size, REL_RECORD_SIZE, PAGE_HEADER_SIZE)
    }

    pub fn address(&self, rel_id: u64) -> RecordAddress {
        address_for_id(
            rel_id,
            self.store_root,
            self.page_size,
            REL_RECORD_SIZE,
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
                page_type: PageType::RelStore as u8,
                flags: 0,
                record_count: 0,
                next_page: 0,
            };
            ph.write(&mut page.data_mut()?[..PAGE_HEADER_SIZE]);
            pager.write_page(&page)?;
        }
        Ok(())
    }

    pub fn create_rel(
        &self,
        pager: &mut impl Pager,
        rel_id: u64,
        record: &RelRecord,
    ) -> Result<RecordAddress, GraphError> {
        let addr = self.address(rel_id);
        self.ensure_page_exists(pager, &addr)?;

        let mut page = pager.get_page(addr.page)?;
        let data = page.data_mut()?;

        let offset = addr.byte_offset(REL_RECORD_SIZE, PAGE_HEADER_SIZE);
        record.write(&mut data[offset..offset + REL_RECORD_SIZE]);

        let mut ph = PageHeader::read(data);
        ph.record_count += 1;
        ph.write(&mut data[..PAGE_HEADER_SIZE]);

        pager.write_page(&page)?;
        Ok(addr)
    }

    pub fn read_rel(
        &self,
        pager: &mut impl Pager,
        rel_id: u64,
    ) -> Result<RelRecord, GraphError> {
        let addr = self.address(rel_id);
        if addr.page > pager.db_size() {
            return Err(GraphError::InvalidPageNumber(addr.page));
        }

        let page = pager.get_page(addr.page)?;
        let data = page.data();
        let offset = addr.byte_offset(REL_RECORD_SIZE, PAGE_HEADER_SIZE);
        Ok(RelRecord::read(&data[offset..offset + REL_RECORD_SIZE]))
    }

    pub fn read_rel_at(
        &self,
        pager: &mut impl Pager,
        addr: RecordAddress,
    ) -> Result<RelRecord, GraphError> {
        if addr.page > pager.db_size() {
            return Err(GraphError::InvalidPageNumber(addr.page));
        }

        let page = pager.get_page(addr.page)?;
        let data = page.data();
        let offset = addr.byte_offset(REL_RECORD_SIZE, PAGE_HEADER_SIZE);
        Ok(RelRecord::read(&data[offset..offset + REL_RECORD_SIZE]))
    }

    pub fn write_rel(
        &self,
        pager: &mut impl Pager,
        rel_id: u64,
        record: &RelRecord,
    ) -> Result<(), GraphError> {
        let addr = self.address(rel_id);
        self.write_rel_at(pager, addr, record)
    }

    pub fn write_rel_at(
        &self,
        pager: &mut impl Pager,
        addr: RecordAddress,
        record: &RelRecord,
    ) -> Result<(), GraphError> {
        if addr.page > pager.db_size() {
            return Err(GraphError::InvalidPageNumber(addr.page));
        }

        let mut page = pager.get_page(addr.page)?;
        let data = page.data_mut()?;
        let offset = addr.byte_offset(REL_RECORD_SIZE, PAGE_HEADER_SIZE);
        record.write(&mut data[offset..offset + REL_RECORD_SIZE]);
        pager.write_page(&page)?;
        Ok(())
    }

    pub fn delete_rel(
        &self,
        pager: &mut impl Pager,
        rel_id: u64,
    ) -> Result<(), GraphError> {
        let addr = self.address(rel_id);
        if addr.page > pager.db_size() {
            return Err(GraphError::InvalidPageNumber(addr.page));
        }

        let mut page = pager.get_page(addr.page)?;
        let data = page.data_mut()?;
        let offset = addr.byte_offset(REL_RECORD_SIZE, PAGE_HEADER_SIZE);
        data[offset..offset + REL_RECORD_SIZE].fill(0);

        let mut ph = PageHeader::read(data);
        ph.record_count = ph.record_count.saturating_sub(1);
        ph.write(&mut data[..PAGE_HEADER_SIZE]);

        pager.write_page(&page)?;
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

    fn setup_pager(path: &str) -> (FilePager, u32) {
        let mut pager = FilePager::open(path, 4096).unwrap();
        pager.begin_write().unwrap();

        let (_, mut header_page) = pager.alloc_page().unwrap();
        header_page.data_mut().unwrap()[0..8].copy_from_slice(b"LSGRAPH\0");
        pager.write_page(&header_page).unwrap();

        let (root, mut rel_page) = pager.alloc_page().unwrap();
        let ph = PageHeader {
            page_type: PageType::RelStore as u8,
            flags: 0,
            record_count: 0,
            next_page: 0,
        };
        ph.write(&mut rel_page.data_mut().unwrap()[..PAGE_HEADER_SIZE]);
        pager.write_page(&rel_page).unwrap();

        pager.commit().unwrap();
        (pager, root)
    }

    #[test]
    fn test_rel_record_roundtrip() {
        let src = RecordAddress::new(10, 5);
        let dst = RecordAddress::new(20, 3);
        let mut record = RelRecord::new(99, src, dst);
        record.src_prev_rel = RecordAddress::new(30, 1);
        record.src_next_rel = RecordAddress::new(31, 2);
        record.dst_prev_rel = RecordAddress::new(40, 1);
        record.dst_next_rel = RecordAddress::new(41, 2);
        record.first_prop = RecordAddress::new(50, 0);
        record.inline_properties[0] = 0xAA;
        record.inline_properties[17] = 0xBB;
        record.set_first_in_src(true);
        record.set_first_in_dst(true);

        let mut buf = [0u8; REL_RECORD_SIZE];
        record.write(&mut buf);

        let decoded = RelRecord::read(&buf);
        assert!(decoded.in_use());
        assert!(decoded.is_first_in_src_chain());
        assert!(decoded.is_first_in_dst_chain());
        assert_eq!(decoded.type_token_id, 99);
        assert_eq!(decoded.source_node, src);
        assert_eq!(decoded.target_node, dst);
        assert_eq!(decoded.src_prev_rel, RecordAddress::new(30, 1));
        assert_eq!(decoded.src_next_rel, RecordAddress::new(31, 2));
        assert_eq!(decoded.dst_prev_rel, RecordAddress::new(40, 1));
        assert_eq!(decoded.dst_next_rel, RecordAddress::new(41, 2));
        assert_eq!(decoded.first_prop, RecordAddress::new(50, 0));
        assert_eq!(decoded.inline_properties[0], 0xAA);
        assert_eq!(decoded.inline_properties[17], 0xBB);
    }

    #[test]
    fn test_rel_store_create_and_read() {
        let path = temp_path();
        let (mut pager, root) = setup_pager(&path);

        let store = RelStore::new(root, 4096);
        let src = RecordAddress::new(2, 0);
        let dst = RecordAddress::new(2, 1);

        pager.begin_write().unwrap();
        let record = RelRecord::new(1, src, dst);
        store.create_rel(&mut pager, 0, &record).unwrap();
        pager.commit().unwrap();

        let read_back = store.read_rel(&mut pager, 0).unwrap();
        assert!(read_back.in_use());
        assert_eq!(read_back.type_token_id, 1);
        assert_eq!(read_back.source_node, src);
        assert_eq!(read_back.target_node, dst);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_rel_chain_linking() {
        let path = temp_path();
        let (mut pager, root) = setup_pager(&path);

        let store = RelStore::new(root, 4096);
        let node_a = RecordAddress::new(2, 0);
        let node_b = RecordAddress::new(2, 1);
        let node_c = RecordAddress::new(2, 2);

        pager.begin_write().unwrap();

        let mut rel0 = RelRecord::new(1, node_a, node_b);
        rel0.set_first_in_src(true);
        rel0.set_first_in_dst(true);
        let addr0 = store.create_rel(&mut pager, 0, &rel0).unwrap();

        let mut rel1 = RelRecord::new(1, node_a, node_c);
        rel1.set_first_in_src(true);
        rel1.set_first_in_dst(true);
        rel1.src_next_rel = addr0;
        let addr1 = store.create_rel(&mut pager, 1, &rel1).unwrap();

        let mut rel0_updated = store.read_rel(&mut pager, 0).unwrap();
        rel0_updated.src_prev_rel = addr1;
        rel0_updated.set_first_in_src(false);
        store.write_rel(&mut pager, 0, &rel0_updated).unwrap();

        pager.commit().unwrap();

        let r1 = store.read_rel(&mut pager, 1).unwrap();
        assert!(r1.is_first_in_src_chain());
        assert_eq!(r1.src_next_rel, addr0);

        let r0 = store.read_rel(&mut pager, 0).unwrap();
        assert!(!r0.is_first_in_src_chain());
        assert_eq!(r0.src_prev_rel, addr1);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_rel_24bit_type_token() {
        let max_type: u32 = 0x00FF_FFFF;
        let record = RelRecord::new(max_type, RecordAddress::NULL, RecordAddress::NULL);

        let mut buf = [0u8; REL_RECORD_SIZE];
        record.write(&mut buf);

        let decoded = RelRecord::read(&buf);
        assert_eq!(decoded.type_token_id, max_type);
    }
}
