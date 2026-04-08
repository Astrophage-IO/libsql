#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    Header = 0x00,
    NodeStore = 0x01,
    RelStore = 0x02,
    PropertyStore = 0x03,
    TokenStore = 0x04,
    FreeBitmap = 0x05,
    RelGroup = 0x06,
    StringOverflow = 0x07,
}

pub const PAGE_HEADER_SIZE: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    pub page_type: u8,
    pub flags: u8,
    pub record_count: u16,
    pub next_page: u32,
}

impl PageHeader {
    pub fn read(data: &[u8]) -> Self {
        Self {
            page_type: data[0],
            flags: data[1],
            record_count: u16::from_le_bytes([data[2], data[3]]),
            next_page: u32::from_le_bytes([data[4], data[5], data[6], data[7]]),
        }
    }

    pub fn write(&self, data: &mut [u8]) {
        data[0] = self.page_type;
        data[1] = self.flags;
        let rc = self.record_count.to_le_bytes();
        data[2] = rc[0];
        data[3] = rc[1];
        let np = self.next_page.to_le_bytes();
        data[4] = np[0];
        data[5] = np[1];
        data[6] = np[2];
        data[7] = np[3];
    }
}
