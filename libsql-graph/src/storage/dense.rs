use crate::error::GraphError;
use crate::storage::page::{PageHeader, PageType, PAGE_HEADER_SIZE};
use crate::storage::pager::Pager;
use crate::storage::record::{records_per_page, RecordAddress};

pub const REL_GROUP_RECORD_SIZE: usize = 64;
pub const REL_GROUP_FLAG_IN_USE: u8 = 0b1000_0000;

#[derive(Debug, Clone)]
pub struct RelGroupRecord {
    pub flags: u8,
    pub type_token_id: u32,
    pub next_group: RecordAddress,
    pub out_first_rel: RecordAddress,
    pub in_first_rel: RecordAddress,
    pub loop_first_rel: RecordAddress,
    pub out_count: u32,
    pub in_count: u32,
    pub loop_count: u32,
}

impl RelGroupRecord {
    pub fn new(type_token_id: u32) -> Self {
        Self {
            flags: REL_GROUP_FLAG_IN_USE,
            type_token_id,
            next_group: RecordAddress::NULL,
            out_first_rel: RecordAddress::NULL,
            in_first_rel: RecordAddress::NULL,
            loop_first_rel: RecordAddress::NULL,
            out_count: 0,
            in_count: 0,
            loop_count: 0,
        }
    }

    pub fn in_use(&self) -> bool {
        self.flags & REL_GROUP_FLAG_IN_USE != 0
    }

    pub fn total_count(&self) -> u32 {
        self.out_count + self.in_count + self.loop_count
    }

    pub fn read(data: &[u8]) -> Self {
        Self {
            flags: data[0],
            type_token_id: u32::from_le_bytes([data[1], data[2], data[3], 0]),
            next_group: RecordAddress::new(
                u32::from_le_bytes([data[4], data[5], data[6], data[7]]),
                u16::from_le_bytes([data[8], data[9]]),
            ),
            out_first_rel: RecordAddress::new(
                u32::from_le_bytes([data[10], data[11], data[12], data[13]]),
                u16::from_le_bytes([data[14], data[15]]),
            ),
            in_first_rel: RecordAddress::new(
                u32::from_le_bytes([data[16], data[17], data[18], data[19]]),
                u16::from_le_bytes([data[20], data[21]]),
            ),
            loop_first_rel: RecordAddress::new(
                u32::from_le_bytes([data[22], data[23], data[24], data[25]]),
                u16::from_le_bytes([data[26], data[27]]),
            ),
            out_count: u32::from_le_bytes([data[28], data[29], data[30], data[31]]),
            in_count: u32::from_le_bytes([data[32], data[33], data[34], data[35]]),
            loop_count: u32::from_le_bytes([data[36], data[37], data[38], data[39]]),
        }
    }

    pub fn write(&self, data: &mut [u8]) {
        data[0] = self.flags;
        let tt = self.type_token_id.to_le_bytes();
        data[1] = tt[0];
        data[2] = tt[1];
        data[3] = tt[2];
        data[4..8].copy_from_slice(&self.next_group.page.to_le_bytes());
        data[8..10].copy_from_slice(&self.next_group.slot.to_le_bytes());
        data[10..14].copy_from_slice(&self.out_first_rel.page.to_le_bytes());
        data[14..16].copy_from_slice(&self.out_first_rel.slot.to_le_bytes());
        data[16..20].copy_from_slice(&self.in_first_rel.page.to_le_bytes());
        data[20..22].copy_from_slice(&self.in_first_rel.slot.to_le_bytes());
        data[22..26].copy_from_slice(&self.loop_first_rel.page.to_le_bytes());
        data[26..28].copy_from_slice(&self.loop_first_rel.slot.to_le_bytes());
        data[28..32].copy_from_slice(&self.out_count.to_le_bytes());
        data[32..36].copy_from_slice(&self.in_count.to_le_bytes());
        data[36..40].copy_from_slice(&self.loop_count.to_le_bytes());
        data[40..64].fill(0);
    }
}

pub struct RelGroupStore {
    page_size: usize,
}

impl RelGroupStore {
    pub fn new(page_size: usize) -> Self {
        Self { page_size }
    }

    pub fn records_per_page(&self) -> usize {
        records_per_page(self.page_size, REL_GROUP_RECORD_SIZE, PAGE_HEADER_SIZE)
    }

    pub fn create_group(
        &self,
        pager: &mut impl Pager,
        record: &RelGroupRecord,
    ) -> Result<RecordAddress, GraphError> {
        let (pgno, mut page) = pager.alloc_page()?;
        let data = page.data_mut()?;
        let ph = PageHeader {
            page_type: PageType::RelGroup as u8,
            flags: 0,
            record_count: 1,
            next_page: 0,
        };
        ph.write(&mut data[..PAGE_HEADER_SIZE]);
        record.write(&mut data[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + REL_GROUP_RECORD_SIZE]);
        pager.write_page(&page)?;
        Ok(RecordAddress::new(pgno, 0))
    }

    pub fn read_group(
        &self,
        pager: &mut impl Pager,
        addr: RecordAddress,
    ) -> Result<RelGroupRecord, GraphError> {
        if addr.page > pager.db_size() {
            return Err(GraphError::InvalidPageNumber(addr.page));
        }
        let page = pager.get_page(addr.page)?;
        let data = page.data();
        let offset = PAGE_HEADER_SIZE + addr.slot as usize * REL_GROUP_RECORD_SIZE;
        Ok(RelGroupRecord::read(
            &data[offset..offset + REL_GROUP_RECORD_SIZE],
        ))
    }

    pub fn write_group(
        &self,
        pager: &mut impl Pager,
        addr: RecordAddress,
        record: &RelGroupRecord,
    ) -> Result<(), GraphError> {
        if addr.page > pager.db_size() {
            return Err(GraphError::InvalidPageNumber(addr.page));
        }
        let mut page = pager.get_page(addr.page)?;
        let data = page.data_mut()?;
        let offset = PAGE_HEADER_SIZE + addr.slot as usize * REL_GROUP_RECORD_SIZE;
        record.write(&mut data[offset..offset + REL_GROUP_RECORD_SIZE]);
        pager.write_page(&page)?;
        Ok(())
    }

    pub fn find_or_create_group(
        &self,
        pager: &mut impl Pager,
        first_group: RecordAddress,
        type_token_id: u32,
    ) -> Result<(RecordAddress, RelGroupRecord, bool), GraphError> {
        let mut current = first_group;
        let mut prev = RecordAddress::NULL;

        while !current.is_null() {
            let group = self.read_group(pager, current)?;
            if group.type_token_id == type_token_id {
                return Ok((current, group, false));
            }
            prev = current;
            current = group.next_group;
        }

        let new_group = RelGroupRecord::new(type_token_id);
        let addr = self.create_group(pager, &new_group)?;

        if !prev.is_null() {
            let mut prev_group = self.read_group(pager, prev)?;
            prev_group.next_group = addr;
            self.write_group(pager, prev, &prev_group)?;
        }

        Ok((addr, new_group, true))
    }

    pub fn iter_groups(
        &self,
        pager: &mut impl Pager,
        first_group: RecordAddress,
    ) -> Result<Vec<(RecordAddress, RelGroupRecord)>, GraphError> {
        let mut result = Vec::new();
        let mut current = first_group;
        while !current.is_null() {
            let group = self.read_group(pager, current)?;
            if !group.in_use() {
                break;
            }
            let next = group.next_group;
            result.push((current, group));
            current = next;
        }
        Ok(result)
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
    fn test_rel_group_record_roundtrip() {
        let mut record = RelGroupRecord::new(42);
        record.next_group = RecordAddress::new(10, 1);
        record.out_first_rel = RecordAddress::new(20, 3);
        record.in_first_rel = RecordAddress::new(30, 5);
        record.loop_first_rel = RecordAddress::new(40, 7);
        record.out_count = 100;
        record.in_count = 200;
        record.loop_count = 5;

        let mut buf = [0u8; REL_GROUP_RECORD_SIZE];
        record.write(&mut buf);

        let decoded = RelGroupRecord::read(&buf);
        assert!(decoded.in_use());
        assert_eq!(decoded.type_token_id, 42);
        assert_eq!(decoded.next_group, RecordAddress::new(10, 1));
        assert_eq!(decoded.out_first_rel, RecordAddress::new(20, 3));
        assert_eq!(decoded.in_first_rel, RecordAddress::new(30, 5));
        assert_eq!(decoded.loop_first_rel, RecordAddress::new(40, 7));
        assert_eq!(decoded.out_count, 100);
        assert_eq!(decoded.in_count, 200);
        assert_eq!(decoded.loop_count, 5);
        assert_eq!(decoded.total_count(), 305);
    }

    #[test]
    fn test_rel_group_store_create_and_read() {
        let path = temp_path();
        let mut pager = setup_pager(&path);
        let store = RelGroupStore::new(4096);

        pager.begin_write().unwrap();
        let record = RelGroupRecord::new(1);
        let addr = store.create_group(&mut pager, &record).unwrap();
        pager.commit().unwrap();

        let read_back = store.read_group(&mut pager, addr).unwrap();
        assert!(read_back.in_use());
        assert_eq!(read_back.type_token_id, 1);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_rel_group_chain() {
        let path = temp_path();
        let mut pager = setup_pager(&path);
        let store = RelGroupStore::new(4096);

        pager.begin_write().unwrap();

        let mut g1 = RelGroupRecord::new(1);
        let addr1 = store.create_group(&mut pager, &g1).unwrap();

        let g2 = RelGroupRecord::new(2);
        let addr2 = store.create_group(&mut pager, &g2).unwrap();

        g1.next_group = addr2;
        store.write_group(&mut pager, addr1, &g1).unwrap();

        pager.commit().unwrap();

        let groups = store.iter_groups(&mut pager, addr1).unwrap();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].1.type_token_id, 1);
        assert_eq!(groups[1].1.type_token_id, 2);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_find_or_create_group() {
        let path = temp_path();
        let mut pager = setup_pager(&path);
        let store = RelGroupStore::new(4096);

        pager.begin_write().unwrap();
        let g1 = RelGroupRecord::new(10);
        let first = store.create_group(&mut pager, &g1).unwrap();

        let (addr, _, created) = store.find_or_create_group(&mut pager, first, 10).unwrap();
        assert!(!created);
        assert_eq!(addr, first);

        let (addr2, _, created2) = store.find_or_create_group(&mut pager, first, 20).unwrap();
        assert!(created2);
        assert_ne!(addr2, first);

        let (addr3, _, created3) = store.find_or_create_group(&mut pager, first, 20).unwrap();
        assert!(!created3);
        assert_eq!(addr3, addr2);

        pager.commit().unwrap();

        let groups = store.iter_groups(&mut pager, first).unwrap();
        assert_eq!(groups.len(), 2);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_group_counts() {
        let path = temp_path();
        let mut pager = setup_pager(&path);
        let store = RelGroupStore::new(4096);

        pager.begin_write().unwrap();
        let mut g = RelGroupRecord::new(1);
        g.out_count = 10;
        g.in_count = 20;
        g.loop_count = 3;
        let addr = store.create_group(&mut pager, &g).unwrap();
        pager.commit().unwrap();

        let read_back = store.read_group(&mut pager, addr).unwrap();
        assert_eq!(read_back.out_count, 10);
        assert_eq!(read_back.in_count, 20);
        assert_eq!(read_back.loop_count, 3);
        assert_eq!(read_back.total_count(), 33);

        let _ = std::fs::remove_file(&path);
    }
}
