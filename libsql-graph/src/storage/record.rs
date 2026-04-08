pub const NODE_RECORD_SIZE: usize = 64;
pub const REL_RECORD_SIZE: usize = 64;
pub const PROPERTY_RECORD_SIZE: usize = 64;
pub const TOKEN_RECORD_SIZE: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct RecordAddress {
    pub page: u32,
    pub slot: u16,
}

impl RecordAddress {
    pub const NULL: Self = Self { page: 0, slot: 0 };

    pub fn new(page: u32, slot: u16) -> Self {
        Self { page, slot }
    }

    pub fn is_null(&self) -> bool {
        self.page == 0 && self.slot == 0
    }

    pub fn read(data: &[u8]) -> Self {
        Self {
            page: u32::from_le_bytes([data[0], data[1], data[2], data[3]]),
            slot: u16::from_le_bytes([data[4], data[5]]),
        }
    }

    pub fn write(&self, data: &mut [u8]) {
        data[0..4].copy_from_slice(&self.page.to_le_bytes());
        data[4..6].copy_from_slice(&self.slot.to_le_bytes());
    }

    pub fn byte_offset(&self, record_size: usize, page_header_size: usize) -> usize {
        page_header_size + self.slot as usize * record_size
    }
}

pub fn records_per_page(page_size: usize, record_size: usize, page_header_size: usize) -> usize {
    (page_size - page_header_size) / record_size
}

pub fn address_for_id(
    id: u64,
    store_root: u32,
    page_size: usize,
    record_size: usize,
    page_header_size: usize,
) -> RecordAddress {
    let rpp = records_per_page(page_size, record_size, page_header_size) as u64;
    RecordAddress {
        page: store_root + (id / rpp) as u32,
        slot: (id % rpp) as u16,
    }
}
