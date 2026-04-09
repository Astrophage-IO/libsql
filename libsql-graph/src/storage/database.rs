use crate::error::GraphError;
use crate::storage::header::GraphHeader;
use crate::storage::page::{PageHeader, PageType, PAGE_HEADER_SIZE};
use crate::storage::pager::Pager;
use crate::storage::pager_bridge::{FilePager, PageHandle};

pub struct GraphDatabase<P: Pager> {
    pager: P,
    header: GraphHeader,
    page_size: u32,
}

impl GraphDatabase<FilePager> {
    pub fn create(path: &str, page_size: u32) -> Result<Self, GraphError> {
        let mut pager = FilePager::open(path, page_size)?;
        pager.begin_write()?;

        let (_, mut header_page) = pager.alloc_page()?;
        let mut header = GraphHeader::new(page_size);

        let (node_root, mut node_page) = pager.alloc_page()?;
        let (rel_root, mut rel_page) = pager.alloc_page()?;
        let (prop_root, mut prop_page) = pager.alloc_page()?;
        let (token_root, mut token_page) = pager.alloc_page()?;
        let (freemap_root, mut freemap_page) = pager.alloc_page()?;

        header.node_store_root = node_root;
        header.rel_store_root = rel_root;
        header.prop_store_root = prop_root;
        header.token_store_root = token_root;
        header.freemap_root = freemap_root;

        header.write(header_page.data_mut()?)?;
        pager.write_page(&header_page)?;

        let root_pages: &mut [(&mut PageHandle, PageType)] = &mut [
            (&mut node_page, PageType::NodeStore),
            (&mut rel_page, PageType::RelStore),
            (&mut prop_page, PageType::PropertyStore),
            (&mut token_page, PageType::TokenStore),
            (&mut freemap_page, PageType::FreeBitmap),
        ];
        for (page, page_type) in root_pages.iter_mut() {
            let ph = PageHeader {
                page_type: *page_type as u8,
                flags: 0,
                record_count: 0,
                next_page: 0,
            };
            ph.write(&mut page.data_mut()?[..PAGE_HEADER_SIZE]);
            pager.write_page(page)?;
        }

        pager.commit()?;
        Ok(Self {
            pager,
            header,
            page_size,
        })
    }

    pub fn open(path: &str) -> Result<Self, GraphError> {
        let page_size = Self::read_page_size(path)?;
        let mut pager = FilePager::open(path, page_size)?;

        if pager.db_size() < 1 {
            return Err(GraphError::CorruptPage(1));
        }

        let header_page = pager.get_page(1)?;
        let header = GraphHeader::read(header_page.data())?;
        header.validate()?;

        Ok(Self {
            pager,
            header,
            page_size,
        })
    }

    fn read_page_size(path: &str) -> Result<u32, GraphError> {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = std::fs::File::open(path)?;
        let mut buf = [0u8; 4];
        f.seek(SeekFrom::Start(12))?;
        f.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }
}

impl<P: Pager> GraphDatabase<P> {
    pub fn from_pager(mut pager: P, page_size: u32) -> Result<Self, GraphError> {
        pager.begin_write()?;

        let (_, mut header_page) = pager.alloc_page()?;
        let mut header = GraphHeader::new(page_size);

        let (node_root, mut node_page) = pager.alloc_page()?;
        let (rel_root, mut rel_page) = pager.alloc_page()?;
        let (prop_root, mut prop_page) = pager.alloc_page()?;
        let (token_root, mut token_page) = pager.alloc_page()?;
        let (freemap_root, mut freemap_page) = pager.alloc_page()?;

        header.node_store_root = node_root;
        header.rel_store_root = rel_root;
        header.prop_store_root = prop_root;
        header.token_store_root = token_root;
        header.freemap_root = freemap_root;

        header.write(header_page.data_mut()?)?;
        pager.write_page(&header_page)?;

        let root_pages: &mut [(&mut PageHandle, PageType)] = &mut [
            (&mut node_page, PageType::NodeStore),
            (&mut rel_page, PageType::RelStore),
            (&mut prop_page, PageType::PropertyStore),
            (&mut token_page, PageType::TokenStore),
            (&mut freemap_page, PageType::FreeBitmap),
        ];
        for (page, page_type) in root_pages.iter_mut() {
            let ph = PageHeader {
                page_type: *page_type as u8,
                flags: 0,
                record_count: 0,
                next_page: 0,
            };
            ph.write(&mut page.data_mut()?[..PAGE_HEADER_SIZE]);
            pager.write_page(page)?;
        }

        pager.commit()?;
        Ok(Self {
            pager,
            header,
            page_size,
        })
    }

    pub fn open_pager(mut pager: P) -> Result<Self, GraphError> {
        if pager.db_size() < 1 {
            return Err(GraphError::CorruptPage(1));
        }

        let header_page = pager.get_page(1)?;
        let header = GraphHeader::read(header_page.data())?;
        header.validate()?;
        let page_size = header.page_size;

        Ok(Self {
            pager,
            header,
            page_size,
        })
    }

    pub fn flush_header(&mut self) -> Result<(), GraphError> {
        self.pager.begin_write()?;
        let mut header_page = self.pager.get_page(1)?;
        self.header.write(header_page.data_mut()?)?;
        self.pager.write_page(&header_page)?;
        self.pager.commit()?;
        Ok(())
    }

    pub fn next_node_id(&mut self) -> u64 {
        let id = self.header.next_node_id;
        self.header.next_node_id += 1;
        id
    }

    pub fn next_rel_id(&mut self) -> u64 {
        let id = self.header.next_rel_id;
        self.header.next_rel_id += 1;
        id
    }

    pub fn next_prop_id(&mut self) -> u64 {
        let id = self.header.next_prop_id;
        self.header.next_prop_id += 1;
        id
    }

    pub fn next_token_id(&mut self) -> u32 {
        let id = self.header.next_token_id;
        self.header.next_token_id += 1;
        id
    }

    pub fn header(&self) -> &GraphHeader {
        &self.header
    }

    pub fn header_mut(&mut self) -> &mut GraphHeader {
        &mut self.header
    }

    pub fn pager(&mut self) -> &mut P {
        &mut self.pager
    }

    pub fn page_size(&self) -> u32 {
        self.page_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::header::{GraphHeader, FORMAT_VERSION, GRAPH_MAGIC};
    use crate::storage::page::{PageHeader, PageType};
    use tempfile::NamedTempFile;

    fn temp_path() -> String {
        let f = NamedTempFile::new().unwrap();
        let p = f.path().to_str().unwrap().to_string();
        drop(f);
        p
    }

    #[test]
    fn test_create_graph_database() {
        let path = temp_path();
        {
            let db = GraphDatabase::create(&path, 4096).unwrap();
            let h = db.header();
            assert_eq!(&h.magic, GRAPH_MAGIC);
            assert_eq!(h.format_version, FORMAT_VERSION);
            assert_eq!(h.page_size, 4096);
            assert!(h.node_store_root > 0);
            assert!(h.rel_store_root > 0);
            assert!(h.prop_store_root > 0);
            assert!(h.token_store_root > 0);
            assert!(h.freemap_root > 0);
            assert_eq!(h.dense_threshold, 50);
        }
        {
            let db = GraphDatabase::open(&path).unwrap();
            let h = db.header();
            assert_eq!(&h.magic, GRAPH_MAGIC);
            assert_eq!(h.format_version, FORMAT_VERSION);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_open_existing_graph() {
        let path = temp_path();
        let (node_root, rel_root, prop_root, token_root, freemap_root);
        {
            let db = GraphDatabase::create(&path, 4096).unwrap();
            node_root = db.header().node_store_root;
            rel_root = db.header().rel_store_root;
            prop_root = db.header().prop_store_root;
            token_root = db.header().token_store_root;
            freemap_root = db.header().freemap_root;
        }
        {
            let db = GraphDatabase::open(&path).unwrap();
            assert_eq!(db.header().node_store_root, node_root);
            assert_eq!(db.header().rel_store_root, rel_root);
            assert_eq!(db.header().prop_store_root, prop_root);
            assert_eq!(db.header().token_store_root, token_root);
            assert_eq!(db.header().freemap_root, freemap_root);
            assert_eq!(db.header().page_size, 4096);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_open_invalid_file() {
        let path = temp_path();
        std::fs::write(&path, b"this is not a graph database file at all!!").unwrap();
        let result = GraphDatabase::open(&path);
        assert!(result.is_err());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_header_roundtrip() {
        let mut header = GraphHeader::new(8192);
        header.node_count = 12345;
        header.edge_count = 67890;
        header.node_store_root = 2;
        header.rel_store_root = 3;
        header.prop_store_root = 4;
        header.token_store_root = 5;
        header.freemap_root = 6;
        header.next_node_id = 1000;
        header.next_rel_id = 2000;
        header.next_prop_id = 3000;
        header.next_token_id = 400;
        header.label_count = 10;
        header.rel_type_count = 20;
        header.dense_threshold = 75;

        let mut buf = vec![0u8; 4096];
        header.write(&mut buf).unwrap();

        let decoded = GraphHeader::read(&buf).unwrap();
        assert_eq!(&decoded.magic, GRAPH_MAGIC);
        assert_eq!(decoded.format_version, FORMAT_VERSION);
        assert_eq!(decoded.page_size, 8192);
        assert_eq!(decoded.node_count, 12345);
        assert_eq!(decoded.edge_count, 67890);
        assert_eq!(decoded.node_store_root, 2);
        assert_eq!(decoded.rel_store_root, 3);
        assert_eq!(decoded.prop_store_root, 4);
        assert_eq!(decoded.token_store_root, 5);
        assert_eq!(decoded.freemap_root, 6);
        assert_eq!(decoded.next_node_id, 1000);
        assert_eq!(decoded.next_rel_id, 2000);
        assert_eq!(decoded.next_prop_id, 3000);
        assert_eq!(decoded.next_token_id, 400);
        assert_eq!(decoded.label_count, 10);
        assert_eq!(decoded.rel_type_count, 20);
        assert_eq!(decoded.dense_threshold, 75);
    }

    #[test]
    fn test_auto_increment_ids() {
        let path = temp_path();
        {
            let mut db = GraphDatabase::create(&path, 4096).unwrap();
            for i in 0..100u64 {
                assert_eq!(db.next_node_id(), i);
            }
            db.flush_header().unwrap();
        }
        {
            let mut db = GraphDatabase::open(&path).unwrap();
            assert_eq!(db.next_node_id(), 100);
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_store_root_pages_initialized() {
        let path = temp_path();
        let mut db = GraphDatabase::create(&path, 4096).unwrap();

        let cases: &[(u32, PageType)] = &[
            (db.header().node_store_root, PageType::NodeStore),
            (db.header().rel_store_root, PageType::RelStore),
            (db.header().prop_store_root, PageType::PropertyStore),
            (db.header().token_store_root, PageType::TokenStore),
            (db.header().freemap_root, PageType::FreeBitmap),
        ];

        for &(pgno, expected_type) in cases {
            let handle = db.pager().get_page(pgno).unwrap();
            let ph = PageHeader::read(handle.data());
            assert_eq!(ph.page_type, expected_type as u8);
            assert_eq!(ph.record_count, 0);
            assert_eq!(ph.next_page, 0);
        }

        drop(db);
        let _ = std::fs::remove_file(&path);
    }
}
