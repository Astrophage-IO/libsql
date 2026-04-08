use crate::error::GraphError;
use crate::storage::page::{PageHeader, PageType, PAGE_HEADER_SIZE};
use crate::storage::pager_bridge::GraphPager;
use crate::storage::record::{
    address_for_id, records_per_page, RecordAddress, NODE_RECORD_SIZE,
};

pub const NODE_FLAG_IN_USE: u8 = 0b1000_0000;
pub const NODE_FLAG_DENSE: u8 = 0b0100_0000;
pub const NODE_FLAG_HAS_INLINE_PROPS: u8 = 0b0010_0000;
pub const NODE_INLINE_PROP_SIZE: usize = 40;

#[derive(Debug, Clone)]
pub struct NodeRecord {
    pub flags: u8,
    pub label_token_id: u32,
    pub first_rel: RecordAddress,
    pub first_prop: RecordAddress,
    pub extra_labels: RecordAddress,
    pub rel_count: u16,
    pub inline_properties: [u8; NODE_INLINE_PROP_SIZE],
}

impl NodeRecord {
    pub fn new(label_token_id: u32) -> Self {
        Self {
            flags: NODE_FLAG_IN_USE,
            label_token_id,
            first_rel: RecordAddress::NULL,
            first_prop: RecordAddress::NULL,
            extra_labels: RecordAddress::NULL,
            rel_count: 0,
            inline_properties: [0u8; NODE_INLINE_PROP_SIZE],
        }
    }

    pub fn in_use(&self) -> bool {
        self.flags & NODE_FLAG_IN_USE != 0
    }

    pub fn is_dense(&self) -> bool {
        self.flags & NODE_FLAG_DENSE != 0
    }

    pub fn set_dense(&mut self, dense: bool) {
        if dense {
            self.flags |= NODE_FLAG_DENSE;
        } else {
            self.flags &= !NODE_FLAG_DENSE;
        }
    }

    pub fn has_inline_props(&self) -> bool {
        self.flags & NODE_FLAG_HAS_INLINE_PROPS != 0
    }

    pub fn set_has_inline_props(&mut self, has: bool) {
        if has {
            self.flags |= NODE_FLAG_HAS_INLINE_PROPS;
        } else {
            self.flags &= !NODE_FLAG_HAS_INLINE_PROPS;
        }
    }

    pub fn read(data: &[u8]) -> Self {
        let label_token_id =
            u32::from_le_bytes([data[1], data[2], data[3], 0]);

        Self {
            flags: data[0],
            label_token_id,
            first_rel: RecordAddress::new(
                u32::from_le_bytes([data[4], data[5], data[6], data[7]]),
                u16::from_le_bytes([data[8], data[9]]),
            ),
            first_prop: RecordAddress::new(
                u32::from_le_bytes([data[10], data[11], data[12], data[13]]),
                u16::from_le_bytes([data[14], data[15]]),
            ),
            extra_labels: RecordAddress::new(
                u32::from_le_bytes([data[16], data[17], data[18], data[19]]),
                u16::from_le_bytes([data[20], data[21]]),
            ),
            rel_count: u16::from_le_bytes([data[22], data[23]]),
            inline_properties: {
                let mut props = [0u8; NODE_INLINE_PROP_SIZE];
                props.copy_from_slice(&data[24..64]);
                props
            },
        }
    }

    pub fn write(&self, data: &mut [u8]) {
        data[0] = self.flags;
        let label = self.label_token_id.to_le_bytes();
        data[1] = label[0];
        data[2] = label[1];
        data[3] = label[2];

        data[4..8].copy_from_slice(&self.first_rel.page.to_le_bytes());
        data[8..10].copy_from_slice(&self.first_rel.slot.to_le_bytes());

        data[10..14].copy_from_slice(&self.first_prop.page.to_le_bytes());
        data[14..16].copy_from_slice(&self.first_prop.slot.to_le_bytes());

        data[16..20].copy_from_slice(&self.extra_labels.page.to_le_bytes());
        data[20..22].copy_from_slice(&self.extra_labels.slot.to_le_bytes());

        data[22..24].copy_from_slice(&self.rel_count.to_le_bytes());
        data[24..64].copy_from_slice(&self.inline_properties);
    }
}

pub struct NodeStore {
    store_root: u32,
    page_size: usize,
}

impl NodeStore {
    pub fn new(store_root: u32, page_size: usize) -> Self {
        Self {
            store_root,
            page_size,
        }
    }

    pub fn records_per_page(&self) -> usize {
        records_per_page(self.page_size, NODE_RECORD_SIZE, PAGE_HEADER_SIZE)
    }

    pub fn address(&self, node_id: u64) -> RecordAddress {
        address_for_id(
            node_id,
            self.store_root,
            self.page_size,
            NODE_RECORD_SIZE,
            PAGE_HEADER_SIZE,
        )
    }

    fn ensure_page_exists(
        &self,
        pager: &mut GraphPager,
        addr: &RecordAddress,
    ) -> Result<(), GraphError> {
        while pager.db_size() < addr.page {
            let (_, mut page) = pager.alloc_page()?;
            let ph = PageHeader {
                page_type: PageType::NodeStore as u8,
                flags: 0,
                record_count: 0,
                next_page: 0,
            };
            ph.write(&mut page.data_mut()?[..PAGE_HEADER_SIZE]);
            pager.write_page(&page)?;
        }
        Ok(())
    }

    pub fn create_node(
        &self,
        pager: &mut GraphPager,
        node_id: u64,
        record: &NodeRecord,
    ) -> Result<RecordAddress, GraphError> {
        let addr = self.address(node_id);
        self.ensure_page_exists(pager, &addr)?;

        let mut page = pager.get_page(addr.page)?;
        let data = page.data_mut()?;

        let offset = addr.byte_offset(NODE_RECORD_SIZE, PAGE_HEADER_SIZE);
        record.write(&mut data[offset..offset + NODE_RECORD_SIZE]);

        let mut ph = PageHeader::read(data);
        ph.record_count += 1;
        ph.write(&mut data[..PAGE_HEADER_SIZE]);

        pager.write_page(&page)?;
        Ok(addr)
    }

    pub fn read_node(
        &self,
        pager: &mut GraphPager,
        node_id: u64,
    ) -> Result<NodeRecord, GraphError> {
        let addr = self.address(node_id);
        if addr.page > pager.db_size() {
            return Err(GraphError::InvalidPageNumber(addr.page));
        }

        let page = pager.get_page(addr.page)?;
        let data = page.data();
        let offset = addr.byte_offset(NODE_RECORD_SIZE, PAGE_HEADER_SIZE);
        Ok(NodeRecord::read(&data[offset..offset + NODE_RECORD_SIZE]))
    }

    pub fn write_node(
        &self,
        pager: &mut GraphPager,
        node_id: u64,
        record: &NodeRecord,
    ) -> Result<(), GraphError> {
        let addr = self.address(node_id);
        if addr.page > pager.db_size() {
            return Err(GraphError::InvalidPageNumber(addr.page));
        }

        let mut page = pager.get_page(addr.page)?;
        let data = page.data_mut()?;
        let offset = addr.byte_offset(NODE_RECORD_SIZE, PAGE_HEADER_SIZE);
        record.write(&mut data[offset..offset + NODE_RECORD_SIZE]);
        pager.write_page(&page)?;
        Ok(())
    }

    pub fn delete_node(
        &self,
        pager: &mut GraphPager,
        node_id: u64,
    ) -> Result<(), GraphError> {
        let addr = self.address(node_id);
        if addr.page > pager.db_size() {
            return Err(GraphError::InvalidPageNumber(addr.page));
        }

        let mut page = pager.get_page(addr.page)?;
        let data = page.data_mut()?;
        let offset = addr.byte_offset(NODE_RECORD_SIZE, PAGE_HEADER_SIZE);

        data[offset..offset + NODE_RECORD_SIZE].fill(0);

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

        let (root, mut node_page) = pager.alloc_page().unwrap();
        let ph = PageHeader {
            page_type: PageType::NodeStore as u8,
            flags: 0,
            record_count: 0,
            next_page: 0,
        };
        ph.write(&mut node_page.data_mut().unwrap()[..PAGE_HEADER_SIZE]);
        pager.write_page(&node_page).unwrap();

        pager.commit().unwrap();
        (pager, root)
    }

    #[test]
    fn test_node_record_roundtrip() {
        let mut record = NodeRecord::new(42);
        record.first_rel = RecordAddress::new(10, 5);
        record.first_prop = RecordAddress::new(20, 3);
        record.extra_labels = RecordAddress::new(30, 7);
        record.rel_count = 123;
        record.inline_properties[0] = 0xAB;
        record.inline_properties[39] = 0xCD;

        let mut buf = [0u8; NODE_RECORD_SIZE];
        record.write(&mut buf);

        let decoded = NodeRecord::read(&buf);
        assert!(decoded.in_use());
        assert!(!decoded.is_dense());
        assert_eq!(decoded.label_token_id, 42);
        assert_eq!(decoded.first_rel.page, 10);
        assert_eq!(decoded.first_rel.slot, 5);
        assert_eq!(decoded.first_prop.page, 20);
        assert_eq!(decoded.first_prop.slot, 3);
        assert_eq!(decoded.extra_labels.page, 30);
        assert_eq!(decoded.extra_labels.slot, 7);
        assert_eq!(decoded.rel_count, 123);
        assert_eq!(decoded.inline_properties[0], 0xAB);
        assert_eq!(decoded.inline_properties[39], 0xCD);
    }

    #[test]
    fn test_node_record_flags() {
        let mut record = NodeRecord::new(0);
        assert!(record.in_use());
        assert!(!record.is_dense());
        assert!(!record.has_inline_props());

        record.set_dense(true);
        assert!(record.is_dense());

        record.set_has_inline_props(true);
        assert!(record.has_inline_props());

        record.set_dense(false);
        assert!(!record.is_dense());
        assert!(record.has_inline_props());
    }

    #[test]
    fn test_node_store_create_and_read() {
        let path = temp_path();
        let (mut pager, root) = setup_pager(&path);

        let store = NodeStore::new(root, 4096);
        let rpp = store.records_per_page();
        assert_eq!(rpp, 63);

        pager.begin_write().unwrap();
        for i in 0..10u64 {
            let record = NodeRecord::new(i as u32);
            store.create_node(&mut pager, i, &record).unwrap();
        }
        pager.commit().unwrap();

        for i in 0..10u64 {
            let record = store.read_node(&mut pager, i).unwrap();
            assert!(record.in_use());
            assert_eq!(record.label_token_id, i as u32);
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_node_store_update() {
        let path = temp_path();
        let (mut pager, root) = setup_pager(&path);

        let store = NodeStore::new(root, 4096);
        pager.begin_write().unwrap();

        let record = NodeRecord::new(1);
        store.create_node(&mut pager, 0, &record).unwrap();
        pager.commit().unwrap();

        pager.begin_write().unwrap();
        let mut updated = store.read_node(&mut pager, 0).unwrap();
        updated.rel_count = 42;
        updated.first_rel = RecordAddress::new(5, 3);
        store.write_node(&mut pager, 0, &updated).unwrap();
        pager.commit().unwrap();

        let read_back = store.read_node(&mut pager, 0).unwrap();
        assert_eq!(read_back.rel_count, 42);
        assert_eq!(read_back.first_rel.page, 5);
        assert_eq!(read_back.first_rel.slot, 3);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_node_store_delete() {
        let path = temp_path();
        let (mut pager, root) = setup_pager(&path);

        let store = NodeStore::new(root, 4096);
        pager.begin_write().unwrap();

        let record = NodeRecord::new(7);
        store.create_node(&mut pager, 0, &record).unwrap();
        pager.commit().unwrap();

        pager.begin_write().unwrap();
        store.delete_node(&mut pager, 0).unwrap();
        pager.commit().unwrap();

        let deleted = store.read_node(&mut pager, 0).unwrap();
        assert!(!deleted.in_use());
        assert_eq!(deleted.label_token_id, 0);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_node_store_cross_page_allocation() {
        let path = temp_path();
        let (mut pager, root) = setup_pager(&path);

        let store = NodeStore::new(root, 4096);
        let rpp = store.records_per_page();

        pager.begin_write().unwrap();
        for i in 0..(rpp as u64 + 5) {
            let record = NodeRecord::new(i as u32);
            store.create_node(&mut pager, i, &record).unwrap();
        }
        pager.commit().unwrap();

        for i in 0..(rpp as u64 + 5) {
            let record = store.read_node(&mut pager, i).unwrap();
            assert!(record.in_use());
            assert_eq!(record.label_token_id, i as u32);
        }

        let addr_first = store.address(0);
        let addr_last = store.address(rpp as u64 + 4);
        assert_eq!(addr_first.page, root);
        assert_eq!(addr_last.page, root + 1);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_node_record_24bit_label() {
        let max_label: u32 = 0x00FF_FFFF;
        let record = NodeRecord::new(max_label);

        let mut buf = [0u8; NODE_RECORD_SIZE];
        record.write(&mut buf);

        let decoded = NodeRecord::read(&buf);
        assert_eq!(decoded.label_token_id, max_label);
    }

    #[test]
    fn test_record_address_computation() {
        let store = NodeStore::new(2, 4096);

        let addr0 = store.address(0);
        assert_eq!(addr0.page, 2);
        assert_eq!(addr0.slot, 0);

        let addr62 = store.address(62);
        assert_eq!(addr62.page, 2);
        assert_eq!(addr62.slot, 62);

        let addr63 = store.address(63);
        assert_eq!(addr63.page, 3);
        assert_eq!(addr63.slot, 0);

        let addr126 = store.address(126);
        assert_eq!(addr126.page, 4);
        assert_eq!(addr126.slot, 0);
    }
}
