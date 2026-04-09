use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::GraphError;

const WAL_MAGIC: u32 = 0x4C53_4757;
const WAL_VERSION: u32 = 1;
const WAL_HEADER_SIZE: usize = 32;
const FRAME_HEADER_SIZE: usize = 24;

pub fn wal_header_size() -> usize {
    WAL_HEADER_SIZE
}

pub fn frame_header_size() -> usize {
    FRAME_HEADER_SIZE
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

fn crc32_combine(prev: u32, data: &[u8]) -> u32 {
    let mut buf = Vec::with_capacity(4 + data.len());
    buf.extend_from_slice(&prev.to_le_bytes());
    buf.extend_from_slice(data);
    crc32(&buf)
}

#[derive(Debug, Clone)]
pub struct WalFrame {
    pub pgno: u32,
    pub db_size_after: u32,
    pub data: Vec<u8>,
}

pub struct WalWriter {
    file: File,
    frame_count: u32,
    salt1: u32,
    salt2: u32,
    checksum_chain: u32,
}

impl WalWriter {
    pub fn create(wal_path: &str, page_size: u32) -> Result<Self, GraphError> {
        let salt1 = 0x5A5A_5A5A_u32;
        let salt2 = 0xA5A5_A5A5_u32;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(wal_path)?;

        let mut header = [0u8; WAL_HEADER_SIZE];
        header[0..4].copy_from_slice(&WAL_MAGIC.to_le_bytes());
        header[4..8].copy_from_slice(&WAL_VERSION.to_le_bytes());
        header[8..12].copy_from_slice(&page_size.to_le_bytes());
        header[12..20].copy_from_slice(&0u64.to_le_bytes()); // checkpoint_seq
        header[20..24].copy_from_slice(&salt1.to_le_bytes());
        header[24..28].copy_from_slice(&salt2.to_le_bytes());
        let cksum = crc32(&header[0..28]);
        header[28..32].copy_from_slice(&cksum.to_le_bytes());

        file.write_all(&header)?;

        Ok(Self {
            file,
            frame_count: 0,
            salt1,
            salt2,
            checksum_chain: 0,
        })
    }

    pub fn open(wal_path: &str) -> Result<Self, GraphError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(wal_path)?;

        let mut header = [0u8; WAL_HEADER_SIZE];
        file.read_exact(&mut header)?;

        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        if magic != WAL_MAGIC {
            return Err(GraphError::PagerError("invalid WAL magic".into()));
        }

        let page_size = u32::from_le_bytes(header[8..12].try_into().unwrap());
        let salt1 = u32::from_le_bytes(header[20..24].try_into().unwrap());
        let salt2 = u32::from_le_bytes(header[24..28].try_into().unwrap());
        let stored_cksum = u32::from_le_bytes(header[28..32].try_into().unwrap());
        let computed_cksum = crc32(&header[0..28]);
        if stored_cksum != computed_cksum {
            return Err(GraphError::PagerError("corrupt WAL header checksum".into()));
        }

        let file_len = file.metadata()?.len();
        let frame_size = FRAME_HEADER_SIZE as u64 + page_size as u64;
        let data_len = file_len.saturating_sub(WAL_HEADER_SIZE as u64);
        let frame_count = (data_len / frame_size) as u32;

        let mut checksum_chain: u32 = 0;
        for i in 0..frame_count {
            let offset = WAL_HEADER_SIZE as u64 + i as u64 * frame_size;
            file.seek(SeekFrom::Start(offset))?;
            let mut fh = [0u8; FRAME_HEADER_SIZE];
            file.read_exact(&mut fh)?;
            let mut page_data = vec![0u8; page_size as usize];
            file.read_exact(&mut page_data)?;

            let stored_c2 = u32::from_le_bytes(fh[20..24].try_into().unwrap());
            let mut fh_for_crc = fh;
            fh_for_crc[16..24].fill(0);
            let mut c1_buf = Vec::with_capacity(20 + page_data.len());
            c1_buf.extend_from_slice(&fh_for_crc[0..20]);
            c1_buf.extend_from_slice(&page_data);
            let c1 = crc32(&c1_buf);
            checksum_chain = crc32_combine(checksum_chain, &c1.to_le_bytes());
            if checksum_chain != stored_c2 {
                break;
            }
        }

        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            file,
            frame_count,
            salt1,
            salt2,
            checksum_chain,
        })
    }

    pub fn append_frame(
        &mut self,
        pgno: u32,
        data: &[u8],
        is_commit: bool,
        db_size: u32,
    ) -> Result<(), GraphError> {
        let db_size_after = if is_commit { db_size } else { 0 };

        let mut fh = [0u8; FRAME_HEADER_SIZE];
        fh[0..4].copy_from_slice(&pgno.to_le_bytes());
        fh[4..8].copy_from_slice(&db_size_after.to_le_bytes());
        fh[8..12].copy_from_slice(&self.salt1.to_le_bytes());
        fh[12..16].copy_from_slice(&self.salt2.to_le_bytes());

        let mut c1_buf = Vec::with_capacity(20 + data.len());
        c1_buf.extend_from_slice(&fh[0..20]);
        c1_buf.extend_from_slice(data);
        let c1 = crc32(&c1_buf);
        fh[16..20].copy_from_slice(&c1.to_le_bytes());

        let c2 = crc32_combine(self.checksum_chain, &c1.to_le_bytes());
        fh[20..24].copy_from_slice(&c2.to_le_bytes());

        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&fh)?;
        self.file.write_all(data)?;

        self.checksum_chain = c2;
        self.frame_count += 1;

        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), GraphError> {
        self.file.sync_all()?;
        Ok(())
    }

    pub fn frame_count(&self) -> u32 {
        self.frame_count
    }

    pub fn file_mut(&mut self) -> &mut File {
        &mut self.file
    }

    pub fn reset(&mut self, page_size: u32) -> Result<(), GraphError> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;

        let mut header = [0u8; WAL_HEADER_SIZE];
        header[0..4].copy_from_slice(&WAL_MAGIC.to_le_bytes());
        header[4..8].copy_from_slice(&WAL_VERSION.to_le_bytes());
        header[8..12].copy_from_slice(&page_size.to_le_bytes());
        header[12..20].copy_from_slice(&0u64.to_le_bytes());
        header[20..24].copy_from_slice(&self.salt1.to_le_bytes());
        header[24..28].copy_from_slice(&self.salt2.to_le_bytes());
        let cksum = crc32(&header[0..28]);
        header[28..32].copy_from_slice(&cksum.to_le_bytes());

        self.file.write_all(&header)?;
        self.file.sync_all()?;

        self.frame_count = 0;
        self.checksum_chain = 0;

        Ok(())
    }
}

pub struct WalReader {
    file: File,
    page_size: usize,
}

impl WalReader {
    pub fn open(wal_path: &str) -> Result<Option<Self>, GraphError> {
        if !Path::new(wal_path).exists() {
            return Ok(None);
        }

        let mut file = OpenOptions::new().read(true).open(wal_path)?;
        let file_len = file.metadata()?.len();
        if file_len < WAL_HEADER_SIZE as u64 {
            return Ok(None);
        }

        let mut header = [0u8; WAL_HEADER_SIZE];
        file.read_exact(&mut header)?;

        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        if magic != WAL_MAGIC {
            return Ok(None);
        }

        let stored_cksum = u32::from_le_bytes(header[28..32].try_into().unwrap());
        let computed_cksum = crc32(&header[0..28]);
        if stored_cksum != computed_cksum {
            return Ok(None);
        }

        let page_size = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;

        Ok(Some(Self { file, page_size }))
    }

    pub fn read_committed_frames(&mut self) -> Result<Vec<WalFrame>, GraphError> {
        self.file.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))?;

        let file_len = self.file.metadata()?.len();
        let frame_size = FRAME_HEADER_SIZE as u64 + self.page_size as u64;
        let data_len = file_len.saturating_sub(WAL_HEADER_SIZE as u64);
        let max_frames = data_len / frame_size;

        let mut all_frames = Vec::new();
        let mut last_commit_idx: Option<usize> = None;
        let mut checksum_chain: u32 = 0;

        for i in 0..max_frames {
            let offset = WAL_HEADER_SIZE as u64 + i * frame_size;
            self.file.seek(SeekFrom::Start(offset))?;

            let mut fh = [0u8; FRAME_HEADER_SIZE];
            if self.file.read_exact(&mut fh).is_err() {
                break;
            }

            let mut page_data = vec![0u8; self.page_size];
            if self.file.read_exact(&mut page_data).is_err() {
                break;
            }

            let stored_c1 = u32::from_le_bytes(fh[16..20].try_into().unwrap());
            let stored_c2 = u32::from_le_bytes(fh[20..24].try_into().unwrap());

            let mut fh_for_crc = fh;
            fh_for_crc[16..24].fill(0);
            let mut c1_buf = Vec::with_capacity(20 + self.page_size);
            c1_buf.extend_from_slice(&fh_for_crc[0..20]);
            c1_buf.extend_from_slice(&page_data);
            let c1 = crc32(&c1_buf);

            if c1 != stored_c1 {
                break;
            }

            let c2 = crc32_combine(checksum_chain, &c1.to_le_bytes());
            if c2 != stored_c2 {
                break;
            }

            checksum_chain = c2;

            let pgno = u32::from_le_bytes(fh[0..4].try_into().unwrap());
            let db_size_after = u32::from_le_bytes(fh[4..8].try_into().unwrap());

            all_frames.push(WalFrame {
                pgno,
                db_size_after,
                data: page_data,
            });

            if db_size_after > 0 {
                last_commit_idx = Some(all_frames.len());
            }
        }

        match last_commit_idx {
            Some(end) => {
                all_frames.truncate(end);
                Ok(all_frames)
            }
            None => Ok(Vec::new()),
        }
    }

    pub fn checkpoint(&mut self, db_file: &mut File) -> Result<u32, GraphError> {
        let frames = self.read_committed_frames()?;
        let count = frames.len() as u32;
        let page_size = self.page_size;

        for frame in &frames {
            let offset = (frame.pgno as u64 - 1) * page_size as u64;
            db_file.seek(SeekFrom::Start(offset))?;
            db_file.write_all(&frame.data)?;
        }

        if count > 0 {
            db_file.sync_all()?;
        }

        Ok(count)
    }
}

pub struct WalIndex {
    page_map: HashMap<u32, u64>,
}

impl Default for WalIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl WalIndex {
    pub fn new() -> Self {
        Self {
            page_map: HashMap::new(),
        }
    }

    pub fn insert(&mut self, pgno: u32, offset: u64) {
        self.page_map.insert(pgno, offset);
    }

    pub fn get(&self, pgno: u32) -> Option<u64> {
        self.page_map.get(&pgno).copied()
    }

    pub fn clear(&mut self) {
        self.page_map.clear();
    }
}

pub fn read_frame_data_at(file: &mut File, offset: u64, page_size: usize) -> Result<Vec<u8>, GraphError> {
    file.seek(SeekFrom::Start(offset + FRAME_HEADER_SIZE as u64))?;
    let mut buf = vec![0u8; page_size];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn temp_wal_path() -> String {
        let f = NamedTempFile::new().unwrap();
        let p = f.path().to_str().unwrap().to_string();
        drop(f);
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn test_wal_header_roundtrip() {
        let path = temp_wal_path();
        let page_size = 4096u32;

        {
            let _writer = WalWriter::create(&path, page_size).unwrap();
        }

        {
            let mut file = File::open(&path).unwrap();
            let mut header = [0u8; WAL_HEADER_SIZE];
            file.read_exact(&mut header).unwrap();

            let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
            assert_eq!(magic, WAL_MAGIC);

            let version = u32::from_le_bytes(header[4..8].try_into().unwrap());
            assert_eq!(version, WAL_VERSION);

            let ps = u32::from_le_bytes(header[8..12].try_into().unwrap());
            assert_eq!(ps, page_size);

            let stored_cksum = u32::from_le_bytes(header[28..32].try_into().unwrap());
            let computed_cksum = crc32(&header[0..28]);
            assert_eq!(stored_cksum, computed_cksum);
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_frame_append_and_read() {
        let path = temp_wal_path();
        let page_size = 4096u32;

        {
            let mut writer = WalWriter::create(&path, page_size).unwrap();
            for i in 0u32..5 {
                let mut data = vec![0u8; page_size as usize];
                data[0..4].copy_from_slice(&(i + 1).to_le_bytes());
                let is_commit = i == 4;
                writer
                    .append_frame(i + 1, &data, is_commit, if is_commit { 5 } else { 0 })
                    .unwrap();
            }
            writer.sync().unwrap();
            assert_eq!(writer.frame_count(), 5);
        }

        {
            let mut reader = WalReader::open(&path).unwrap().unwrap();
            let frames = reader.read_committed_frames().unwrap();
            assert_eq!(frames.len(), 5);
            for (i, frame) in frames.iter().enumerate() {
                assert_eq!(frame.pgno, (i + 1) as u32);
                let tag = u32::from_le_bytes(frame.data[0..4].try_into().unwrap());
                assert_eq!(tag, (i + 1) as u32);
            }
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_commit_frame_detection() {
        let path = temp_wal_path();
        let page_size = 4096u32;

        {
            let mut writer = WalWriter::create(&path, page_size).unwrap();
            let data = vec![0xAA; page_size as usize];
            writer.append_frame(1, &data, false, 0).unwrap();
            writer.append_frame(2, &data, false, 0).unwrap();
            writer.append_frame(3, &data, false, 0).unwrap();
            writer.append_frame(4, &data, true, 4).unwrap();
            writer.append_frame(5, &data, false, 0).unwrap();
            writer.append_frame(6, &data, false, 0).unwrap();
            writer.sync().unwrap();
        }

        {
            let mut reader = WalReader::open(&path).unwrap().unwrap();
            let frames = reader.read_committed_frames().unwrap();
            assert_eq!(frames.len(), 4);
            assert_eq!(frames[3].pgno, 4);
            assert!(frames[3].db_size_after > 0);
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_checkpoint_to_db() {
        let db_path = {
            let f = NamedTempFile::new().unwrap();
            let p = f.path().to_str().unwrap().to_string();
            drop(f);
            p
        };
        let wal_path = format!("{db_path}-wal");
        let page_size = 4096u32;

        {
            let mut db_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&db_path)
                .unwrap();
            let blank = vec![0u8; page_size as usize];
            for _ in 0..3 {
                db_file.write_all(&blank).unwrap();
            }
            db_file.sync_all().unwrap();
        }

        {
            let mut writer = WalWriter::create(&wal_path, page_size).unwrap();
            for pgno in 1..=3u32 {
                let mut data = vec![0u8; page_size as usize];
                data[0..4].copy_from_slice(&(pgno * 100).to_le_bytes());
                let is_commit = pgno == 3;
                writer
                    .append_frame(pgno, &data, is_commit, if is_commit { 3 } else { 0 })
                    .unwrap();
            }
            writer.sync().unwrap();
        }

        {
            let mut reader = WalReader::open(&wal_path).unwrap().unwrap();
            let mut db_file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&db_path)
                .unwrap();
            let count = reader.checkpoint(&mut db_file).unwrap();
            assert_eq!(count, 3);
        }

        {
            let mut db_file = File::open(&db_path).unwrap();
            for pgno in 1..=3u32 {
                let offset = (pgno as u64 - 1) * page_size as u64;
                db_file.seek(SeekFrom::Start(offset)).unwrap();
                let mut buf = vec![0u8; page_size as usize];
                db_file.read_exact(&mut buf).unwrap();
                let tag = u32::from_le_bytes(buf[0..4].try_into().unwrap());
                assert_eq!(tag, pgno * 100);
            }
        }

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_file(&wal_path);
    }

    #[test]
    fn test_crash_recovery() {
        let path = temp_wal_path();
        let page_size = 4096u32;

        {
            let mut writer = WalWriter::create(&path, page_size).unwrap();

            let mut d1 = vec![0u8; page_size as usize];
            d1[0] = 0xAA;
            writer.append_frame(1, &d1, false, 0).unwrap();

            let mut d2 = vec![0u8; page_size as usize];
            d2[0] = 0xBB;
            writer.append_frame(2, &d2, true, 2).unwrap();

            let mut d3 = vec![0u8; page_size as usize];
            d3[0] = 0xCC;
            writer.append_frame(3, &d3, false, 0).unwrap();

            writer.sync().unwrap();
        }

        {
            let mut file = OpenOptions::new().write(true).open(&path).unwrap();
            let file_len = file.metadata().unwrap().len();
            file.seek(SeekFrom::Start(file_len - 4)).unwrap();
            file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
        }

        {
            let mut reader = WalReader::open(&path).unwrap().unwrap();
            let frames = reader.read_committed_frames().unwrap();
            assert_eq!(frames.len(), 2);
            assert_eq!(frames[0].data[0], 0xAA);
            assert_eq!(frames[1].data[0], 0xBB);
        }

        let _ = std::fs::remove_file(&path);
    }
}
