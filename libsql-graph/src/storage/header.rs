use crate::error::GraphError;

pub const GRAPH_MAGIC: &[u8; 8] = b"LSGRAPH\0";
pub const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone)]
pub struct GraphHeader {
    pub magic: [u8; 8],
    pub format_version: u32,
    pub page_size: u32,
    pub node_count: u64,
    // bytes 24-99: RESERVED FOR PAGER
    pub edge_count: u64,
    pub node_store_root: u32,
    pub rel_store_root: u32,
    pub prop_store_root: u32,
    pub token_store_root: u32,
    pub freemap_root: u32,
    pub next_node_id: u64,
    pub next_rel_id: u64,
    pub next_prop_id: u64,
    pub next_token_id: u32,
    pub label_count: u32,
    pub rel_type_count: u32,
    pub dense_threshold: u32,
}

impl GraphHeader {
    pub fn new(page_size: u32) -> Self {
        Self {
            magic: *GRAPH_MAGIC,
            format_version: FORMAT_VERSION,
            page_size,
            node_count: 0,
            edge_count: 0,
            node_store_root: 0,
            rel_store_root: 0,
            prop_store_root: 0,
            token_store_root: 0,
            freemap_root: 0,
            next_node_id: 0,
            next_rel_id: 0,
            next_prop_id: 0,
            next_token_id: 0,
            label_count: 0,
            rel_type_count: 0,
            dense_threshold: 50,
        }
    }

    pub fn read(data: &[u8]) -> Result<Self, GraphError> {
        if data.len() < 168 {
            return Err(GraphError::CorruptPage(1));
        }

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&data[0..8]);

        Ok(Self {
            magic,
            format_version: u32::from_le_bytes(data[8..12].try_into().unwrap()),
            page_size: u32::from_le_bytes(data[12..16].try_into().unwrap()),
            node_count: u64::from_le_bytes(data[16..24].try_into().unwrap()),
            // skip bytes 24-99
            edge_count: u64::from_le_bytes(data[100..108].try_into().unwrap()),
            node_store_root: u32::from_le_bytes(data[108..112].try_into().unwrap()),
            rel_store_root: u32::from_le_bytes(data[112..116].try_into().unwrap()),
            prop_store_root: u32::from_le_bytes(data[116..120].try_into().unwrap()),
            token_store_root: u32::from_le_bytes(data[120..124].try_into().unwrap()),
            freemap_root: u32::from_le_bytes(data[124..128].try_into().unwrap()),
            next_node_id: u64::from_le_bytes(data[128..136].try_into().unwrap()),
            next_rel_id: u64::from_le_bytes(data[136..144].try_into().unwrap()),
            next_prop_id: u64::from_le_bytes(data[144..152].try_into().unwrap()),
            next_token_id: u32::from_le_bytes(data[152..156].try_into().unwrap()),
            label_count: u32::from_le_bytes(data[156..160].try_into().unwrap()),
            rel_type_count: u32::from_le_bytes(data[160..164].try_into().unwrap()),
            dense_threshold: u32::from_le_bytes(data[164..168].try_into().unwrap()),
        })
    }

    pub fn write(&self, data: &mut [u8]) -> Result<(), GraphError> {
        if data.len() < 168 {
            return Err(GraphError::CorruptPage(1));
        }

        data[0..8].copy_from_slice(&self.magic);
        data[8..12].copy_from_slice(&self.format_version.to_le_bytes());
        data[12..16].copy_from_slice(&self.page_size.to_le_bytes());
        data[16..24].copy_from_slice(&self.node_count.to_le_bytes());
        // skip bytes 24-99
        data[100..108].copy_from_slice(&self.edge_count.to_le_bytes());
        data[108..112].copy_from_slice(&self.node_store_root.to_le_bytes());
        data[112..116].copy_from_slice(&self.rel_store_root.to_le_bytes());
        data[116..120].copy_from_slice(&self.prop_store_root.to_le_bytes());
        data[120..124].copy_from_slice(&self.token_store_root.to_le_bytes());
        data[124..128].copy_from_slice(&self.freemap_root.to_le_bytes());
        data[128..136].copy_from_slice(&self.next_node_id.to_le_bytes());
        data[136..144].copy_from_slice(&self.next_rel_id.to_le_bytes());
        data[144..152].copy_from_slice(&self.next_prop_id.to_le_bytes());
        data[152..156].copy_from_slice(&self.next_token_id.to_le_bytes());
        data[156..160].copy_from_slice(&self.label_count.to_le_bytes());
        data[160..164].copy_from_slice(&self.rel_type_count.to_le_bytes());
        data[164..168].copy_from_slice(&self.dense_threshold.to_le_bytes());
        Ok(())
    }

    pub fn validate(&self) -> Result<(), GraphError> {
        if self.magic != *GRAPH_MAGIC {
            return Err(GraphError::InvalidMagic);
        }
        if self.format_version != FORMAT_VERSION {
            return Err(GraphError::UnsupportedVersion(self.format_version));
        }
        Ok(())
    }
}
