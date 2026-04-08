use crate::error::GraphError;
use crate::storage::page::{PageHeader, PageType, PAGE_HEADER_SIZE};
use crate::storage::pager_bridge::GraphPager;
use crate::storage::record::{
    address_for_id, records_per_page, RecordAddress, PROPERTY_RECORD_SIZE,
};

pub const PROP_FLAG_IN_USE: u8 = 0b1000_0000;
pub const PROP_BLOCK_SIZE: usize = 14;
pub const PROP_MAX_BLOCKS: usize = 4;
pub const PROP_VALUE_MAX_INLINE: usize = 10;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropertyType {
    Null = 0x00,
    Bool = 0x01,
    Int32 = 0x02,
    Int64 = 0x03,
    Float64 = 0x04,
    ShortString = 0x05,
    String = 0x06,
    Blob = 0x07,
    StringArray = 0x08,
    IntArray = 0x09,
}

impl PropertyType {
    pub fn from_u8(v: u8) -> Self {
        match v {
            0x01 => Self::Bool,
            0x02 => Self::Int32,
            0x03 => Self::Int64,
            0x04 => Self::Float64,
            0x05 => Self::ShortString,
            0x06 => Self::String,
            0x07 => Self::Blob,
            0x08 => Self::StringArray,
            0x09 => Self::IntArray,
            _ => Self::Null,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    ShortString(String),
    Overflow(RecordAddress),
}

impl PropertyValue {
    pub fn prop_type(&self) -> PropertyType {
        match self {
            Self::Null => PropertyType::Null,
            Self::Bool(_) => PropertyType::Bool,
            Self::Int32(_) => PropertyType::Int32,
            Self::Int64(_) => PropertyType::Int64,
            Self::Float64(_) => PropertyType::Float64,
            Self::ShortString(_) => PropertyType::ShortString,
            Self::Overflow(_) => PropertyType::String,
        }
    }

    pub fn encode(&self, buf: &mut [u8]) -> u8 {
        match self {
            Self::Null => 0,
            Self::Bool(v) => {
                buf[0] = *v as u8;
                1
            }
            Self::Int32(v) => {
                buf[..4].copy_from_slice(&v.to_le_bytes());
                4
            }
            Self::Int64(v) => {
                buf[..8].copy_from_slice(&v.to_le_bytes());
                8
            }
            Self::Float64(v) => {
                buf[..8].copy_from_slice(&v.to_le_bytes());
                8
            }
            Self::ShortString(s) => {
                let bytes = s.as_bytes();
                let len = bytes.len().min(PROP_VALUE_MAX_INLINE);
                buf[..len].copy_from_slice(&bytes[..len]);
                len as u8
            }
            Self::Overflow(addr) => {
                addr.write(buf);
                6
            }
        }
    }

    pub fn decode(prop_type: PropertyType, size: u8, data: &[u8]) -> Self {
        match prop_type {
            PropertyType::Null => Self::Null,
            PropertyType::Bool => Self::Bool(data[0] != 0),
            PropertyType::Int32 => {
                Self::Int32(i32::from_le_bytes(data[..4].try_into().unwrap()))
            }
            PropertyType::Int64 => {
                Self::Int64(i64::from_le_bytes(data[..8].try_into().unwrap()))
            }
            PropertyType::Float64 => {
                Self::Float64(f64::from_le_bytes(data[..8].try_into().unwrap()))
            }
            PropertyType::ShortString => {
                let s = std::str::from_utf8(&data[..size as usize]).unwrap_or("");
                Self::ShortString(s.to_string())
            }
            PropertyType::String | PropertyType::Blob
            | PropertyType::StringArray | PropertyType::IntArray => {
                Self::Overflow(RecordAddress::read(data))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct PropertyBlock {
    pub key_token_id: u16,
    pub prop_type: PropertyType,
    pub size: u8,
    pub value: [u8; PROP_VALUE_MAX_INLINE],
}

impl PropertyBlock {
    pub fn new(key_token_id: u16, value: &PropertyValue) -> Self {
        let mut val_buf = [0u8; PROP_VALUE_MAX_INLINE];
        let size = value.encode(&mut val_buf);
        Self {
            key_token_id,
            prop_type: value.prop_type(),
            size,
            value: val_buf,
        }
    }

    pub fn read(data: &[u8]) -> Self {
        let key_token_id = u16::from_le_bytes([data[0], data[1]]);
        let prop_type = PropertyType::from_u8(data[2]);
        let size = data[3];
        let mut value = [0u8; PROP_VALUE_MAX_INLINE];
        value.copy_from_slice(&data[4..14]);
        Self {
            key_token_id,
            prop_type,
            size,
            value,
        }
    }

    pub fn write(&self, data: &mut [u8]) {
        data[0..2].copy_from_slice(&self.key_token_id.to_le_bytes());
        data[2] = self.prop_type as u8;
        data[3] = self.size;
        data[4..14].copy_from_slice(&self.value);
    }

    pub fn decode_value(&self) -> PropertyValue {
        PropertyValue::decode(self.prop_type, self.size, &self.value)
    }

    pub fn is_empty(&self) -> bool {
        self.key_token_id == 0 && self.prop_type as u8 == 0
    }
}

#[derive(Debug, Clone)]
pub struct PropertyRecord {
    pub flags: u8,
    pub block_count: u8,
    pub next_prop: RecordAddress,
    pub blocks: [PropertyBlock; PROP_MAX_BLOCKS],
}

impl PropertyRecord {
    pub fn new() -> Self {
        Self {
            flags: PROP_FLAG_IN_USE,
            block_count: 0,
            next_prop: RecordAddress::NULL,
            blocks: std::array::from_fn(|_| PropertyBlock {
                key_token_id: 0,
                prop_type: PropertyType::Null,
                size: 0,
                value: [0u8; PROP_VALUE_MAX_INLINE],
            }),
        }
    }

    pub fn in_use(&self) -> bool {
        self.flags & PROP_FLAG_IN_USE != 0
    }

    pub fn add_block(&mut self, block: PropertyBlock) -> bool {
        if self.block_count as usize >= PROP_MAX_BLOCKS {
            return false;
        }
        self.blocks[self.block_count as usize] = block;
        self.block_count += 1;
        true
    }

    pub fn find_block(&self, key_token_id: u16) -> Option<&PropertyBlock> {
        for i in 0..self.block_count as usize {
            if self.blocks[i].key_token_id == key_token_id {
                return Some(&self.blocks[i]);
            }
        }
        None
    }

    pub fn set_block(&mut self, key_token_id: u16, block: PropertyBlock) -> bool {
        for i in 0..self.block_count as usize {
            if self.blocks[i].key_token_id == key_token_id {
                self.blocks[i] = block;
                return true;
            }
        }
        self.add_block(block)
    }

    pub fn remove_block(&mut self, key_token_id: u16) -> bool {
        for i in 0..self.block_count as usize {
            if self.blocks[i].key_token_id == key_token_id {
                for j in i..self.block_count as usize - 1 {
                    self.blocks[j] = self.blocks[j + 1].clone();
                }
                self.block_count -= 1;
                self.blocks[self.block_count as usize] = PropertyBlock {
                    key_token_id: 0,
                    prop_type: PropertyType::Null,
                    size: 0,
                    value: [0u8; PROP_VALUE_MAX_INLINE],
                };
                return true;
            }
        }
        false
    }

    pub fn read(data: &[u8]) -> Self {
        let flags = data[0];
        let block_count = data[1];
        let next_prop = RecordAddress::new(
            u32::from_le_bytes([data[2], data[3], data[4], data[5]]),
            u16::from_le_bytes([data[6], data[7]]),
        );

        let blocks = std::array::from_fn(|i| {
            let offset = 8 + i * PROP_BLOCK_SIZE;
            PropertyBlock::read(&data[offset..offset + PROP_BLOCK_SIZE])
        });

        Self {
            flags,
            block_count,
            next_prop,
            blocks,
        }
    }

    pub fn write(&self, data: &mut [u8]) {
        data[0] = self.flags;
        data[1] = self.block_count;
        data[2..6].copy_from_slice(&self.next_prop.page.to_le_bytes());
        data[6..8].copy_from_slice(&self.next_prop.slot.to_le_bytes());

        for i in 0..PROP_MAX_BLOCKS {
            let offset = 8 + i * PROP_BLOCK_SIZE;
            self.blocks[i].write(&mut data[offset..offset + PROP_BLOCK_SIZE]);
        }
    }
}

pub struct PropertyStore {
    store_root: u32,
    page_size: usize,
}

impl PropertyStore {
    pub fn new(store_root: u32, page_size: usize) -> Self {
        Self {
            store_root,
            page_size,
        }
    }

    pub fn records_per_page(&self) -> usize {
        records_per_page(self.page_size, PROPERTY_RECORD_SIZE, PAGE_HEADER_SIZE)
    }

    pub fn address(&self, prop_id: u64) -> RecordAddress {
        address_for_id(
            prop_id,
            self.store_root,
            self.page_size,
            PROPERTY_RECORD_SIZE,
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
                page_type: PageType::PropertyStore as u8,
                flags: 0,
                record_count: 0,
                next_page: 0,
            };
            ph.write(&mut page.data_mut()?[..PAGE_HEADER_SIZE]);
            pager.write_page(&page)?;
        }
        Ok(())
    }

    pub fn create_record(
        &self,
        pager: &mut GraphPager,
        prop_id: u64,
        record: &PropertyRecord,
    ) -> Result<RecordAddress, GraphError> {
        let addr = self.address(prop_id);
        self.ensure_page_exists(pager, &addr)?;

        let mut page = pager.get_page(addr.page)?;
        let data = page.data_mut()?;

        let offset = addr.byte_offset(PROPERTY_RECORD_SIZE, PAGE_HEADER_SIZE);
        record.write(&mut data[offset..offset + PROPERTY_RECORD_SIZE]);

        let mut ph = PageHeader::read(data);
        ph.record_count += 1;
        ph.write(&mut data[..PAGE_HEADER_SIZE]);

        pager.write_page(&page)?;
        Ok(addr)
    }

    pub fn read_record(
        &self,
        pager: &mut GraphPager,
        addr: RecordAddress,
    ) -> Result<PropertyRecord, GraphError> {
        if addr.page > pager.db_size() {
            return Err(GraphError::InvalidPageNumber(addr.page));
        }

        let page = pager.get_page(addr.page)?;
        let data = page.data();
        let offset = addr.byte_offset(PROPERTY_RECORD_SIZE, PAGE_HEADER_SIZE);
        Ok(PropertyRecord::read(
            &data[offset..offset + PROPERTY_RECORD_SIZE],
        ))
    }

    pub fn write_record(
        &self,
        pager: &mut GraphPager,
        addr: RecordAddress,
        record: &PropertyRecord,
    ) -> Result<(), GraphError> {
        if addr.page > pager.db_size() {
            return Err(GraphError::InvalidPageNumber(addr.page));
        }

        let mut page = pager.get_page(addr.page)?;
        let data = page.data_mut()?;
        let offset = addr.byte_offset(PROPERTY_RECORD_SIZE, PAGE_HEADER_SIZE);
        record.write(&mut data[offset..offset + PROPERTY_RECORD_SIZE]);
        pager.write_page(&page)?;
        Ok(())
    }

    pub fn get_property(
        &self,
        pager: &mut GraphPager,
        first_prop: RecordAddress,
        key_token_id: u16,
    ) -> Result<Option<PropertyValue>, GraphError> {
        let mut current = first_prop;
        while !current.is_null() {
            let record = self.read_record(pager, current)?;
            if !record.in_use() {
                break;
            }
            if let Some(block) = record.find_block(key_token_id) {
                return Ok(Some(block.decode_value()));
            }
            current = record.next_prop;
        }
        Ok(None)
    }

    pub fn get_all_properties(
        &self,
        pager: &mut GraphPager,
        first_prop: RecordAddress,
    ) -> Result<Vec<(u16, PropertyValue)>, GraphError> {
        let mut result = Vec::new();
        let mut current = first_prop;
        while !current.is_null() {
            let record = self.read_record(pager, current)?;
            if !record.in_use() {
                break;
            }
            for i in 0..record.block_count as usize {
                let block = &record.blocks[i];
                if !block.is_empty() {
                    result.push((block.key_token_id, block.decode_value()));
                }
            }
            current = record.next_prop;
        }
        Ok(result)
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

        let (root, mut prop_page) = pager.alloc_page().unwrap();
        let ph = PageHeader {
            page_type: PageType::PropertyStore as u8,
            flags: 0,
            record_count: 0,
            next_page: 0,
        };
        ph.write(&mut prop_page.data_mut().unwrap()[..PAGE_HEADER_SIZE]);
        pager.write_page(&prop_page).unwrap();

        pager.commit().unwrap();
        (pager, root)
    }

    #[test]
    fn test_property_block_roundtrip() {
        let block = PropertyBlock::new(42, &PropertyValue::Int64(123456789));
        let mut buf = [0u8; PROP_BLOCK_SIZE];
        block.write(&mut buf);
        let decoded = PropertyBlock::read(&buf);
        assert_eq!(decoded.key_token_id, 42);
        assert_eq!(decoded.decode_value(), PropertyValue::Int64(123456789));
    }

    #[test]
    fn test_property_value_types() {
        let cases: Vec<PropertyValue> = vec![
            PropertyValue::Null,
            PropertyValue::Bool(true),
            PropertyValue::Bool(false),
            PropertyValue::Int32(-42),
            PropertyValue::Int32(i32::MAX),
            PropertyValue::Int64(i64::MIN),
            PropertyValue::Float64(3.14159),
            PropertyValue::ShortString("hello".to_string()),
            PropertyValue::ShortString("".to_string()),
            PropertyValue::ShortString("1234567890".to_string()),
        ];

        for val in &cases {
            let block = PropertyBlock::new(1, val);
            let mut buf = [0u8; PROP_BLOCK_SIZE];
            block.write(&mut buf);
            let decoded = PropertyBlock::read(&buf).decode_value();
            assert_eq!(&decoded, val, "Roundtrip failed for {:?}", val);
        }
    }

    #[test]
    fn test_property_record_roundtrip() {
        let mut record = PropertyRecord::new();
        record.add_block(PropertyBlock::new(1, &PropertyValue::Int32(100)));
        record.add_block(PropertyBlock::new(2, &PropertyValue::ShortString("Alice".into())));
        record.add_block(PropertyBlock::new(3, &PropertyValue::Bool(true)));
        record.next_prop = RecordAddress::new(10, 5);

        let mut buf = [0u8; PROPERTY_RECORD_SIZE];
        record.write(&mut buf);

        let decoded = PropertyRecord::read(&buf);
        assert!(decoded.in_use());
        assert_eq!(decoded.block_count, 3);
        assert_eq!(decoded.next_prop, RecordAddress::new(10, 5));

        let b0 = decoded.find_block(1).unwrap();
        assert_eq!(b0.decode_value(), PropertyValue::Int32(100));

        let b1 = decoded.find_block(2).unwrap();
        assert_eq!(
            b1.decode_value(),
            PropertyValue::ShortString("Alice".into())
        );

        let b2 = decoded.find_block(3).unwrap();
        assert_eq!(b2.decode_value(), PropertyValue::Bool(true));
    }

    #[test]
    fn test_property_record_max_blocks() {
        let mut record = PropertyRecord::new();
        for i in 0..4u16 {
            assert!(record.add_block(PropertyBlock::new(i, &PropertyValue::Int32(i as i32))));
        }
        assert!(!record.add_block(PropertyBlock::new(5, &PropertyValue::Null)));
        assert_eq!(record.block_count, 4);
    }

    #[test]
    fn test_property_record_set_and_remove() {
        let mut record = PropertyRecord::new();
        record.add_block(PropertyBlock::new(1, &PropertyValue::Int32(10)));
        record.add_block(PropertyBlock::new(2, &PropertyValue::Int32(20)));

        record.set_block(1, PropertyBlock::new(1, &PropertyValue::Int32(99)));
        assert_eq!(
            record.find_block(1).unwrap().decode_value(),
            PropertyValue::Int32(99)
        );
        assert_eq!(record.block_count, 2);

        assert!(record.remove_block(1));
        assert_eq!(record.block_count, 1);
        assert!(record.find_block(1).is_none());
        assert_eq!(
            record.find_block(2).unwrap().decode_value(),
            PropertyValue::Int32(20)
        );
    }

    #[test]
    fn test_property_store_create_and_read() {
        let path = temp_path();
        let (mut pager, root) = setup_pager(&path);

        let store = PropertyStore::new(root, 4096);

        let mut record = PropertyRecord::new();
        record.add_block(PropertyBlock::new(1, &PropertyValue::ShortString("Alice".into())));
        record.add_block(PropertyBlock::new(2, &PropertyValue::Int32(28)));

        pager.begin_write().unwrap();
        let addr = store.create_record(&mut pager, 0, &record).unwrap();
        pager.commit().unwrap();

        let read_back = store.read_record(&mut pager, addr).unwrap();
        assert!(read_back.in_use());
        assert_eq!(read_back.block_count, 2);

        let name = store.get_property(&mut pager, addr, 1).unwrap();
        assert_eq!(name, Some(PropertyValue::ShortString("Alice".into())));

        let age = store.get_property(&mut pager, addr, 2).unwrap();
        assert_eq!(age, Some(PropertyValue::Int32(28)));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_property_chain_traversal() {
        let path = temp_path();
        let (mut pager, root) = setup_pager(&path);

        let store = PropertyStore::new(root, 4096);

        pager.begin_write().unwrap();

        let mut rec1 = PropertyRecord::new();
        rec1.add_block(PropertyBlock::new(10, &PropertyValue::Float64(9.81)));
        let addr1 = store.create_record(&mut pager, 1, &rec1).unwrap();

        let mut rec0 = PropertyRecord::new();
        rec0.add_block(PropertyBlock::new(1, &PropertyValue::Int32(42)));
        rec0.add_block(PropertyBlock::new(2, &PropertyValue::Bool(true)));
        rec0.next_prop = addr1;
        let addr0 = store.create_record(&mut pager, 0, &rec0).unwrap();

        pager.commit().unwrap();

        let val = store.get_property(&mut pager, addr0, 10).unwrap();
        assert_eq!(val, Some(PropertyValue::Float64(9.81)));

        let all = store.get_all_properties(&mut pager, addr0).unwrap();
        assert_eq!(all.len(), 3);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_property_not_found() {
        let path = temp_path();
        let (mut pager, root) = setup_pager(&path);

        let store = PropertyStore::new(root, 4096);

        let mut record = PropertyRecord::new();
        record.add_block(PropertyBlock::new(1, &PropertyValue::Int32(10)));

        pager.begin_write().unwrap();
        let addr = store.create_record(&mut pager, 0, &record).unwrap();
        pager.commit().unwrap();

        let missing = store.get_property(&mut pager, addr, 99).unwrap();
        assert_eq!(missing, None);

        let null_addr = store.get_property(&mut pager, RecordAddress::NULL, 1).unwrap();
        assert_eq!(null_addr, None);

        let _ = std::fs::remove_file(&path);
    }
}
