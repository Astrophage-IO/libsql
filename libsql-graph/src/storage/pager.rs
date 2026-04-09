use crate::error::GraphError;
use crate::storage::pager_bridge::PageHandle;

pub trait Pager {
    fn db_size(&self) -> u32;
    fn page_size(&self) -> usize;
    fn get_page(&mut self, pgno: u32) -> Result<PageHandle, GraphError>;
    fn alloc_page(&mut self) -> Result<(u32, PageHandle), GraphError>;
    fn write_page(&mut self, handle: &PageHandle) -> Result<(), GraphError>;
    fn begin_read(&mut self) -> Result<(), GraphError>;
    fn begin_write(&mut self) -> Result<(), GraphError>;
    fn commit(&mut self) -> Result<(), GraphError>;
    fn rollback(&mut self) -> Result<(), GraphError>;
}
