
use crate::core::constants::*;
use crate::core::errors::*;
use crate::core::metrics::Metrics;
use crate::core::pager::Pager;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

const WAL_HEADER_SIZE: usize = 32;
const WAL_FRAME_SIZE: usize = PAGE_SIZE + 28;
const STAT_CACHE_TTL: Duration = Duration::from_millis(100);
const WAL_BUFFER_SIZE: usize = 64 * 1024;

const WAL_MAGIC: [u8; 4] = *b"WLOG";

#[derive(Debug, Clone)]
pub struct WALHeader {
    pub magic: [u8; 4],
    pub version: u32,
    pub salt1: u32,
    pub salt2: u32,
}

#[derive(Debug, Clone)]
pub struct WALFrame {
    pub tx_id: u64,
    pub page_num: u64,
    pub page_data: Vec<u8>,
    pub checksum: u32,
    pub salt1: u32,
    pub salt2: u32,
}

struct WALInner {
    file: File,
    writer: BufWriter<File>,
    header: WALHeader,
    frame_num: u64,
    checksum_buf: Vec<u8>,

    cached_file_size: i64,
    cache_timestamp: Option<Instant>,
    file_position: i64,
}

pub struct WAL {
    inner: Arc<Mutex<WALInner>>,
    metrics: Arc<RwLock<Option<Arc<Metrics>>>>,
}

impl WAL {
    #[cfg_attr(not(unix), allow(unused_variables))]
    pub fn open(db_path: &str, permissions: u32) -> Result<Self> {
        let wal_path = format!("{}-wal", db_path);

        let is_new = !Path::new(&wal_path).exists();

        #[cfg(unix)]
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(permissions)
            .open(&wal_path)?;

        #[cfg(not(unix))]
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&wal_path)?;

        let writer_file = file.try_clone()?;
        let writer = BufWriter::with_capacity(WAL_BUFFER_SIZE, writer_file);

        let mut inner = WALInner {
            file,
            writer,
            header: WALHeader {
                magic: WAL_MAGIC,
                version: 1,
                salt1: generate_salt(),
                salt2: generate_salt(),
            },
            frame_num: 0,
            checksum_buf: vec![0u8; 16 + PAGE_SIZE],
            cached_file_size: 0,
            cache_timestamp: None,
            file_position: -1,
        };

        if is_new {
            inner.write_header()?;
        } else {
            inner.header = inner.read_header()?;
            inner.frame_num = inner.count_frames();
        }

        Ok(WAL {
            inner: Arc::new(Mutex::new(inner)),
            metrics: Arc::new(RwLock::new(None)),
        })
    }

    pub(crate) fn set_metrics(&self, metrics: Arc<Metrics>) {
        if let Ok(mut m) = self.metrics.write() {
            *m = Some(metrics);
        }
    }

    pub fn write_frame(&self, tx_id: u64, page_num: u64, page_data: Vec<u8>) -> Result<()> {
        if page_data.len() != PAGE_SIZE {
            return Err(Error::InvalidPageSize);
        }

        let mut inner = self.inner.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "wal.inner".to_string() })?;

        let frame = WALFrame {
            tx_id,
            page_num,
            page_data,
            checksum: 0,
            salt1: inner.header.salt1,
            salt2: inner.header.salt2,
        };

        let checksum = inner.calculate_checksum(&frame);
        let frame = WALFrame { checksum, ..frame };

        let mut data = vec![0u8; WAL_FRAME_SIZE];
        serialize_frame_into(&frame, &mut data);

        let offset = WAL_HEADER_SIZE as i64 + (inner.frame_num as i64 * WAL_FRAME_SIZE as i64);

        // Only seek if we're not at the expected position (e.g., after reads or file reopen)
        // For sequential writes, this optimization avoids unnecessary seeks and flushes
        if inner.file_position != offset {
            inner.writer.flush()?;
            inner.writer.get_mut().seek(SeekFrom::Start(offset as u64))?;
        }

        inner.writer.write_all(&data)?;
        inner.file_position = offset + WAL_FRAME_SIZE as i64;

        inner.frame_num += 1;
        inner.cached_file_size += WAL_FRAME_SIZE as i64;
        inner.cache_timestamp = Some(Instant::now());

        // Track metrics (must happen after releasing the inner lock to avoid deadlock)
        drop(inner);
        if let Ok(m) = self.metrics.read() {
            if let Some(metrics) = m.as_ref() {
                metrics.wal_write(WAL_FRAME_SIZE as u64);
            }
        }

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut inner = self.inner.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "wal.inner".to_string() })?;
        inner.writer.flush()?;
        inner.file.sync_all()?;
        Ok(())
    }

    pub fn frame_count(&self) -> u64 {
        self.inner.lock()
            .map(|inner| inner.frame_num)
            .unwrap_or(0)
    }

    pub fn refresh_frame_count(&self) -> Result<()> {
        let mut inner = self.inner.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "wal.inner".to_string() })?;

        let now = Instant::now();
        if let Some(cache_time) = inner.cache_timestamp {
            if now.duration_since(cache_time) < STAT_CACHE_TTL {
                let size = inner.cached_file_size;
                if size < WAL_HEADER_SIZE as i64 {
                    return Ok(());
                }

                let file_frames = ((size - WAL_HEADER_SIZE as i64) / WAL_FRAME_SIZE as i64) as u64;
                if file_frames > inner.frame_num {
                    let metadata = inner.file.metadata()?;
                    inner.cached_file_size = metadata.len() as i64;
                    inner.cache_timestamp = Some(now);
                    inner.frame_num = inner.count_frames();
                }
                return Ok(());
            }
        }

        let metadata = inner.file.metadata()?;
        let size = metadata.len() as i64;
        inner.cached_file_size = size;
        inner.cache_timestamp = Some(now);

        if size < WAL_HEADER_SIZE as i64 {
            return Ok(());
        }

        let file_frames = ((size - WAL_HEADER_SIZE as i64) / WAL_FRAME_SIZE as i64) as u64;

        if file_frames > inner.frame_num {
            inner.frame_num = inner.count_frames();
        }

        Ok(())
    }

    pub fn read_frame(&self, frame_num: u64) -> Result<WALFrame> {
        let mut inner = self.inner.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "wal.inner".to_string() })?;

        if frame_num >= inner.frame_num {
            return Err(Error::Other("EOF".to_string()));
        }

        let offset = WAL_HEADER_SIZE as i64 + (frame_num as i64 * WAL_FRAME_SIZE as i64);
        inner.file.seek(SeekFrom::Start(offset as u64))?;

        let mut data = vec![0u8; WAL_FRAME_SIZE];
        inner.file.read_exact(&mut data)?;
        inner.file_position = offset + data.len() as i64;

        let frame = parse_frame(&data)?;

        let expected_checksum = inner.calculate_checksum(&frame);
        if frame.checksum != expected_checksum {
            return Err(Error::WALChecksumFail);
        }

        Ok(frame)
    }

    pub fn read_all_frames(&self) -> Result<Vec<WALFrame>> {
        let inner = self.inner.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "wal.inner".to_string() })?;
        let frame_count = inner.frame_num;
        drop(inner);

        let mut frames = Vec::with_capacity(frame_count as usize);
        for i in 0..frame_count {
            let frame = self.read_frame(i)?;
            frames.push(frame);
        }

        Ok(frames)
    }

    pub fn checkpoint(&self, pager: &Pager) -> Result<()> {
        let mut inner = self.inner.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "wal.inner".to_string() })?;

        inner.frame_num = inner.count_frames();

        // Use buffered reader to reduce syscalls
        use std::io::BufReader;

        let salt1 = inner.header.salt1;
        let salt2 = inner.header.salt2;
        let frame_count = inner.frame_num;

        inner.file.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))?;

        // Create BufReader in a scope to release the file borrow
        let mut frame_buf = vec![0u8; WAL_FRAME_SIZE];
        let mut raw_frames = Vec::with_capacity(frame_count as usize);

        {
            let mut reader = BufReader::with_capacity(64 * 1024, &mut inner.file);

            for _i in 0..frame_count {
                match reader.read_exact(&mut frame_buf) {
                    Ok(_) => {
                        // Parse frame but defer checksum validation
                        let frame = match parse_frame(&frame_buf) {
                            Ok(f) => f,
                            Err(_) => break,
                        };

                        if frame.salt1 != salt1 || frame.salt2 != salt2 {
                            break;
                        }

                        raw_frames.push(frame);
                    }
                    Err(_) => break,
                }
            }
        }

        // Now validate checksums with inner available
        let mut frames = Vec::with_capacity(raw_frames.len());
        for frame in raw_frames {
            let expected_checksum = inner.calculate_checksum(&frame);
            if frame.checksum != expected_checksum {
                break;
            }
            frames.push(frame);
        }

        inner.file_position = -1;

        use std::collections::HashMap;
        let mut page_map: HashMap<PageNum, Vec<u8>> = HashMap::new();
        for frame in frames {
            if frame.page_num != 0 {
                page_map.insert(frame.page_num, frame.page_data);  // Later writes overwrite earlier ones
            }
        }

        let pages: Vec<(PageNum, Vec<u8>)> = page_map.into_iter().collect();

        pager.write_pages_direct(pages)?;

        pager.flush()?;

        inner.file.set_len(WAL_HEADER_SIZE as u64)?;
        inner.frame_num = 0;
        inner.cached_file_size = WAL_HEADER_SIZE as i64;
        inner.cache_timestamp = Some(Instant::now());
        inner.file_position = -1;

        inner.file.sync_all()?;

        // Track metrics (must happen after releasing the inner lock)
        drop(inner);
        if let Ok(m) = self.metrics.read() {
            if let Some(metrics) = m.as_ref() {
                metrics.checkpoint_completed();
            }
        }

        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        let inner = self.inner.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "wal.inner".to_string() })?;
        inner.file.sync_all()?;
        Ok(())
    }
}

impl WALInner {
    fn write_header(&mut self) -> Result<()> {
        let mut data = vec![0u8; WAL_HEADER_SIZE];

        data[0..4].copy_from_slice(&self.header.magic);
        data[4..8].copy_from_slice(&self.header.version.to_le_bytes());
        data[8..12].copy_from_slice(&self.header.salt1.to_le_bytes());
        data[12..16].copy_from_slice(&self.header.salt2.to_le_bytes());

        self.writer.flush()?;
        self.writer.get_mut().seek(SeekFrom::Start(0))?;
        self.writer.write_all(&data)?;
        self.file_position = data.len() as i64;

        self.writer.flush()?;
        self.file.sync_all()?;

        Ok(())
    }

    fn read_header(&mut self) -> Result<WALHeader> {
        let mut data = vec![0u8; WAL_HEADER_SIZE];

        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_exact(&mut data)?;
        self.file_position = data.len() as i64;

        let mut magic = [0u8; 4];
        magic.copy_from_slice(&data[0..4]);

        let header = WALHeader {
            magic,
            version: u32::from_le_bytes(data[4..8].try_into()
                .map_err(|_| Error::DataCorruption { details: "invalid version bytes in WAL header".to_string() })?),
            salt1: u32::from_le_bytes(data[8..12].try_into()
                .map_err(|_| Error::DataCorruption { details: "invalid salt1 bytes in WAL header".to_string() })?),
            salt2: u32::from_le_bytes(data[12..16].try_into()
                .map_err(|_| Error::DataCorruption { details: "invalid salt2 bytes in WAL header".to_string() })?),
        };

        if header.magic != WAL_MAGIC {
            return Err(Error::WALCorrupted);
        }

        Ok(header)
    }

    fn calculate_checksum(&mut self, frame: &WALFrame) -> u32 {
        self.checksum_buf[0..8].copy_from_slice(&frame.tx_id.to_le_bytes());
        self.checksum_buf[8..16].copy_from_slice(&frame.page_num.to_le_bytes());
        self.checksum_buf[16..16 + PAGE_SIZE].copy_from_slice(&frame.page_data);

        let mut crc = crc32_ieee(&self.checksum_buf[..16 + PAGE_SIZE]);
        crc ^= frame.salt1;
        crc ^= frame.salt2;

        crc
    }

    fn count_frames(&mut self) -> u64 {
        let size = match self.file.metadata() {
            Ok(meta) => meta.len() as i64,
            Err(_) => return 0,
        };

        if size < WAL_HEADER_SIZE as i64 {
            return 0;
        }

        let max_possible_frames = ((size - WAL_HEADER_SIZE as i64) / WAL_FRAME_SIZE as i64) as u64;
        let mut valid_frame_count = 0u64;

        let mut data = vec![0u8; WAL_FRAME_SIZE];

        for i in 0..max_possible_frames {
            let offset = WAL_HEADER_SIZE as i64 + (i as i64 * WAL_FRAME_SIZE as i64);

            if self.file.seek(SeekFrom::Start(offset as u64)).is_err() {
                break;
            }

            match self.file.read_exact(&mut data) {
                Ok(_) => {
                    self.file_position = offset + data.len() as i64;

                    let frame = match parse_frame_header(&data) {
                        Ok(f) => f,
                        Err(_) => break,
                    };

                    if frame.salt1 != self.header.salt1 || frame.salt2 != self.header.salt2 {
                        break;
                    }

                    let expected_checksum = self.calculate_checksum(&frame);
                    if frame.checksum != expected_checksum {
                        break;
                    }

                    valid_frame_count += 1;
                }
                Err(_) => break,
            }
        }

        valid_frame_count
    }
}

fn serialize_frame_into(frame: &WALFrame, data: &mut [u8]) {
    data[0..8].copy_from_slice(&frame.tx_id.to_le_bytes());
    data[8..16].copy_from_slice(&frame.page_num.to_le_bytes());
    data[16..20].copy_from_slice(&frame.salt1.to_le_bytes());
    data[20..24].copy_from_slice(&frame.salt2.to_le_bytes());
    data[24..24 + PAGE_SIZE].copy_from_slice(&frame.page_data);
    data[24 + PAGE_SIZE..28 + PAGE_SIZE].copy_from_slice(&frame.checksum.to_le_bytes());
}

fn parse_frame(data: &[u8]) -> Result<WALFrame> {
    let mut page_data = vec![0u8; PAGE_SIZE];
    page_data.copy_from_slice(&data[24..24 + PAGE_SIZE]);

    Ok(WALFrame {
        tx_id: u64::from_le_bytes(data[0..8].try_into()?),
        page_num: u64::from_le_bytes(data[8..16].try_into()?),
        salt1: u32::from_le_bytes(data[16..20].try_into()?),
        salt2: u32::from_le_bytes(data[20..24].try_into()?),
        page_data,
        checksum: u32::from_le_bytes(data[24 + PAGE_SIZE..28 + PAGE_SIZE].try_into()?),
    })
}

fn parse_frame_header(data: &[u8]) -> Result<WALFrame> {
    let page_data = data[24..24 + PAGE_SIZE].to_vec();

    Ok(WALFrame {
        tx_id: u64::from_le_bytes(data[0..8].try_into()?),
        page_num: u64::from_le_bytes(data[8..16].try_into()?),
        salt1: u32::from_le_bytes(data[16..20].try_into()?),
        salt2: u32::from_le_bytes(data[20..24].try_into()?),
        page_data,
        checksum: u32::from_le_bytes(data[24 + PAGE_SIZE..28 + PAGE_SIZE].try_into()?),
    })
}

fn generate_salt() -> u32 {
    use std::time::SystemTime;
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_secs() as u32,
        Err(_) => 12345,
    }
}

fn crc32_ieee(data: &[u8]) -> u32 {
    const CRC32_TABLE: [u32; 256] = generate_crc32_table();

    let mut crc = 0xFFFFFFFF_u32;
    for &byte in data {
        let index = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32_TABLE[index];
    }
    !crc
}

const fn generate_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_wal_create() {
        let path = "/tmp/test_wal_create.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}-wal", path));

        let wal = WAL::open(path, 0o644).unwrap();
        assert_eq!(wal.frame_count(), 0);

        wal.close().unwrap();
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_wal_write_read_frame() {
        let path = "/tmp/test_wal_frame.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}-wal", path));

        let wal = WAL::open(path, 0o644).unwrap();

        let page_data = vec![42u8; PAGE_SIZE];
        wal.write_frame(1, 100, page_data.clone()).unwrap();
        wal.sync().unwrap();

        assert_eq!(wal.frame_count(), 1);

        let frame = wal.read_frame(0).unwrap();
        assert_eq!(frame.tx_id, 1);
        assert_eq!(frame.page_num, 100);
        assert_eq!(frame.page_data, page_data);

        wal.close().unwrap();
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_wal_multiple_frames() {
        let path = "/tmp/test_wal_multi.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}-wal", path));

        let wal = WAL::open(path, 0o644).unwrap();

        for i in 0..10 {
            let page_data = vec![i as u8; PAGE_SIZE];
            wal.write_frame(1, i, page_data).unwrap();
        }
        wal.sync().unwrap();

        assert_eq!(wal.frame_count(), 10);

        let frames = wal.read_all_frames().unwrap();
        assert_eq!(frames.len(), 10);

        for (i, frame) in frames.iter().enumerate() {
            assert_eq!(frame.page_num, i as u64);
            assert_eq!(frame.page_data[0], i as u8);
        }

        wal.close().unwrap();
        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_wal_reopen() {
        let path = "/tmp/test_wal_reopen.db";
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}-wal", path));

        {
            let wal = WAL::open(path, 0o644).unwrap();
            let page_data = vec![99u8; PAGE_SIZE];
            wal.write_frame(1, 50, page_data).unwrap();
            wal.sync().unwrap();
            wal.close().unwrap();
        }

        {
            let wal = WAL::open(path, 0o644).unwrap();
            assert_eq!(wal.frame_count(), 1);

            let frame = wal.read_frame(0).unwrap();
            assert_eq!(frame.page_num, 50);
            assert_eq!(frame.page_data[0], 99);

            wal.close().unwrap();
        }

        let _ = fs::remove_file(format!("{}-wal", path));
    }

    #[test]
    fn test_crc32_ieee() {
        assert_eq!(crc32_ieee(b""), 0);
        assert_eq!(crc32_ieee(b"123456789"), 0xCBF43926);
        assert_eq!(crc32_ieee(b"The quick brown fox jumps over the lazy dog"), 0x414FA339);
    }
}
