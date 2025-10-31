
use crate::core::constants::*;
use crate::core::errors::*;
use crate::core::lru_cache::LRUCache;
use crate::core::metrics::Metrics;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::{Arc, RwLock, Mutex};

#[derive(Debug, Clone)]
pub struct Header {
    pub magic: [u8; 4],
    pub version: u32,
    pub page_size: u32,
    pub num_pages: u64,
    pub free_count: u32,
    pub metadata_page: u64,
    pub next_tx_id: u64,
    pub free_list: Vec<PageNum>,
}

impl Header {
    #[cfg(test)]
    fn new() -> Self {
        Self {
            magic: MAGIC,
            version: VERSION,
            page_size: PAGE_SIZE as u32,
            num_pages: 1,
            free_count: 0,
            metadata_page: 0,
            next_tx_id: 1,
            free_list: Vec::new(),
        }
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![0u8; PAGE_SIZE];
        let mut offset = 0;

        buf[offset..offset + 4].copy_from_slice(&self.magic);
        offset += 4;

        buf[offset..offset + 4].copy_from_slice(&self.version.to_le_bytes());
        offset += 4;

        buf[offset..offset + 4].copy_from_slice(&self.page_size.to_le_bytes());
        offset += 4;

        buf[offset..offset + 8].copy_from_slice(&self.num_pages.to_le_bytes());
        offset += 8;

        buf[offset..offset + 4].copy_from_slice(&self.free_count.to_le_bytes());
        offset += 4;

        buf[offset..offset + 8].copy_from_slice(&self.metadata_page.to_le_bytes());
        offset += 8;

        buf[offset..offset + 8].copy_from_slice(&self.next_tx_id.to_le_bytes());
        offset += 8;

        for &page_num in &self.free_list {
            if offset + 8 > PAGE_SIZE {
                break;
            }
            buf[offset..offset + 8].copy_from_slice(&page_num.to_le_bytes());
            offset += 8;
        }

        buf
    }

    fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < PAGE_SIZE {
            return Err(Error::InvalidPageSize);
        }

        let mut offset = 0;

        let mut magic = [0u8; 4];
        magic.copy_from_slice(&data[offset..offset + 4]);
        if magic != MAGIC {
            return Err(Error::InvalidMagic);
        }
        offset += 4;

        let version = u32::from_le_bytes(data[offset..offset + 4].try_into()
            .map_err(|_| Error::DataCorruption { details: "failed to parse version from header".to_string() })?);
        if version != VERSION {
            return Err(Error::InvalidVersion);
        }
        offset += 4;

        let page_size = u32::from_le_bytes(data[offset..offset + 4].try_into()
            .map_err(|_| Error::DataCorruption { details: "failed to parse page_size from header".to_string() })?);
        if page_size != PAGE_SIZE as u32 {
            return Err(Error::InvalidPageSize);
        }
        offset += 4;

        let num_pages = u64::from_le_bytes(data[offset..offset + 8].try_into()
            .map_err(|_| Error::DataCorruption { details: "failed to parse num_pages from header".to_string() })?);
        offset += 8;

        let free_count = u32::from_le_bytes(data[offset..offset + 4].try_into()
            .map_err(|_| Error::DataCorruption { details: "failed to parse free_count from header".to_string() })?);
        offset += 4;

        let metadata_page = u64::from_le_bytes(data[offset..offset + 8].try_into()
            .map_err(|_| Error::DataCorruption { details: "failed to parse metadata_page from header".to_string() })?);
        offset += 8;

        let next_tx_id = u64::from_le_bytes(data[offset..offset + 8].try_into()
            .map_err(|_| Error::DataCorruption { details: "failed to parse next_tx_id from header".to_string() })?);
        offset += 8;

        let mut free_list = Vec::new();
        for _ in 0..free_count {
            if offset + 8 > PAGE_SIZE {
                break;
            }
            let page_num = u64::from_le_bytes(data[offset..offset + 8].try_into()
                .map_err(|_| Error::DataCorruption { details: "failed to parse free_list entry from header".to_string() })?);
            free_list.push(page_num);
            offset += 8;
        }

        Ok(Self {
            magic,
            version,
            page_size,
            num_pages,
            free_count,
            metadata_page,
            next_tx_id,
            free_list,
        })
    }
}

pub struct Pager {
    file: Arc<Mutex<File>>,
    cache: LRUCache,
    num_pages: Arc<RwLock<u64>>,
    metadata_page: Arc<RwLock<u64>>,
    next_tx_id: Arc<RwLock<u64>>,
    free_list: Arc<RwLock<Vec<PageNum>>>,
    read_only: bool,
    metrics: Arc<RwLock<Option<Arc<Metrics>>>>,
}

impl Pager {
    #[cfg_attr(not(unix), allow(unused_variables))]
    pub fn open(path: &str, cache_size: usize, permissions: u32, read_only: bool) -> Result<Self> {
        let path_obj = Path::new(path);
        let exists = path_obj.exists();

        let file = if read_only {
            OpenOptions::new()
                .read(true)
                .open(path)?
        } else {
            let mut options = OpenOptions::new();
            options.read(true).write(true);

            if !exists {
                options.create(true);
            }

            let f = options.open(path)?;

            #[cfg(unix)]
            {
                let metadata = f.metadata()?;
                let mut perms = metadata.permissions();
                perms.set_mode(permissions);
                f.set_permissions(perms)?;
            }

            f
        };

        let cache = LRUCache::new(cache_size);

        let mut pager = Self {
            file: Arc::new(Mutex::new(file)),
            cache,
            num_pages: Arc::new(RwLock::new(1)),
            metadata_page: Arc::new(RwLock::new(0)),
            next_tx_id: Arc::new(RwLock::new(1)),
            free_list: Arc::new(RwLock::new(Vec::new())),
            read_only,
            metrics: Arc::new(RwLock::new(None)),
        };

        if exists {
            pager.read_header()?;
        } else if !read_only {
            pager.write_header()?;
        }

        Ok(pager)
    }

    pub fn read_header(&mut self) -> Result<()> {
        let mut file = self.file.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.file".to_string() })?;

        // Get file size for validation
        let file_size = file.seek(SeekFrom::End(0))?;
        file.seek(SeekFrom::Start(0))?;

        let mut buf = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut buf)?;

        let header = Header::deserialize(&buf)?;

        // === CORRUPTION DETECTION VALIDATION ===

        // 1. num_pages must be at least 1 (header page always exists)
        if header.num_pages < 1 {
            return Err(Error::DataCorruption {
                details: format!(
                    "invalid num_pages: {} (must be at least 1 for header page)",
                    header.num_pages
                ),
            });
        }

        // 2. File size must be large enough to contain all claimed pages
        let expected_min_size = header.num_pages * PAGE_SIZE as u64;
        if file_size < expected_min_size {
            return Err(Error::DataCorruption {
                details: format!(
                    "file truncated: header claims {} pages ({} bytes) but file is only {} bytes",
                    header.num_pages, expected_min_size, file_size
                ),
            });
        }

        // 3. metadata_page must be within valid range (0 means not set, otherwise < num_pages)
        if header.metadata_page != 0 && header.metadata_page >= header.num_pages {
            return Err(Error::DataCorruption {
                details: format!(
                    "invalid metadata_page: {} (must be < num_pages {})",
                    header.metadata_page, header.num_pages
                ),
            });
        }

        // 4. All free_list entries must be within valid range
        for (i, &page_num) in header.free_list.iter().enumerate() {
            if page_num >= header.num_pages {
                return Err(Error::DataCorruption {
                    details: format!(
                        "invalid free_list entry at index {}: page {} (must be < num_pages {})",
                        i, page_num, header.num_pages
                    ),
                });
            }
            // Also check that page 0 (header) is never in free list
            if page_num == 0 {
                return Err(Error::DataCorruption {
                    details: format!(
                        "invalid free_list entry at index {}: page 0 (header page) cannot be free",
                        i
                    ),
                });
            }
        }

        // 5. Check for duplicate entries in free_list
        let mut seen_pages: HashSet<u64> = HashSet::new();
        for (i, &page_num) in header.free_list.iter().enumerate() {
            if !seen_pages.insert(page_num) {
                return Err(Error::DataCorruption {
                    details: format!(
                        "duplicate page {} in free_list at index {} (same page cannot be free twice)",
                        page_num, i
                    ),
                });
            }
        }

        // 6. metadata_page must not be in free_list (it's in use!)
        if header.metadata_page != 0 && seen_pages.contains(&header.metadata_page) {
            return Err(Error::DataCorruption {
                details: format!(
                    "metadata_page {} is in free_list (metadata page is in use, cannot be free)",
                    header.metadata_page
                ),
            });
        }

        // === END VALIDATION ===

        *self.num_pages.write()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.num_pages".to_string() })? = header.num_pages;
        *self.metadata_page.write()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.metadata_page".to_string() })? = header.metadata_page;
        *self.next_tx_id.write()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.next_tx_id".to_string() })? = header.next_tx_id;
        *self.free_list.write()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.free_list".to_string() })? = header.free_list;

        Ok(())
    }

    pub fn get_header_data(&self) -> Result<Vec<u8>> {
        let num_pages = *self.num_pages.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.num_pages".to_string() })?;
        let free_list = self.free_list.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.free_list".to_string() })?;
        let metadata_page = *self.metadata_page.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.metadata_page".to_string() })?;
        let next_tx_id = *self.next_tx_id.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.next_tx_id".to_string() })?;

        let header = Header {
            magic: MAGIC,
            version: VERSION,
            page_size: PAGE_SIZE as u32,
            num_pages,
            free_count: free_list.len() as u32,
            metadata_page,
            next_tx_id,
            free_list: free_list.clone(),
        };

        Ok(header.serialize())
    }

    pub fn write_header(&self) -> Result<()> {
        if self.read_only {
            return Err(Error::Other("cannot write header: database is read-only".to_string()));
        }

        let data = self.get_header_data()?;

        let mut file = self.file.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.file".to_string() })?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&data)?;
        file.sync_all()?;

        Ok(())
    }

    pub fn write_header_no_sync(&self) -> Result<()> {
        if self.read_only {
            return Err(Error::Other("cannot write header: database is read-only".to_string()));
        }

        let data = self.get_header_data()?;

        let mut file = self.file.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.file".to_string() })?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&data)?;
        // No sync - caller is responsible (usually WAL sync handles durability)

        Ok(())
    }

    pub fn sync_data_only(&self) -> Result<()> {
        if self.read_only {
            return Ok(());
        }

        // sync_data() is faster than sync_all() because it only syncs file content,
        // not metadata (atime, mtime, etc). This is sufficient for ensuring data
        // visibility across processes while maintaining performance.
        let file = self.file.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.file".to_string() })?;
        file.sync_data()?;
        Ok(())
    }

    pub fn read_page(&self, page_num: PageNum) -> Result<Vec<u8>> {
        if let Some(data) = self.cache.get(page_num) {
            // Cache hit
            if let Ok(guard) = self.metrics.read() {
                if let Some(metrics) = guard.as_ref() {
                    metrics.cache_hit();
                }
            }
            return Ok(data);
        }

        // Cache miss
        if let Ok(guard) = self.metrics.read() {
            if let Some(metrics) = guard.as_ref() {
                metrics.cache_miss();
            }
        }

        let mut file = self.file.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.file".to_string() })?;
        let offset = page_num * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset))?;

        let mut buf = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut buf)?;

        self.cache.put(page_num, buf.clone());

        Ok(buf)
    }

    pub fn read_page_shared(&self, page_num: PageNum) -> Result<Vec<u8>> {
        if let Some(data) = self.cache.get_shared(page_num) {
            // Cache hit
            if let Ok(guard) = self.metrics.read() {
                if let Some(metrics) = guard.as_ref() {
                    metrics.cache_hit();
                }
            }
            return Ok(data);
        }

        // Cache miss
        if let Ok(guard) = self.metrics.read() {
            if let Some(metrics) = guard.as_ref() {
                metrics.cache_miss();
            }
        }

        let mut file = self.file.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.file".to_string() })?;
        let offset = page_num * PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset))?;

        let mut buf = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut buf)?;

        self.cache.put(page_num, buf.clone());

        Ok(buf)
    }

    pub fn write_page(&self, page_num: PageNum, data: &[u8]) -> Result<()> {
        if self.read_only {
            return Err(Error::Other("cannot write page: database is read-only".to_string()));
        }

        if data.len() != PAGE_SIZE {
            return Err(Error::InvalidPageSize);
        }

        self.cache.put(page_num, data.to_vec());
        self.cache.mark_dirty(page_num);

        Ok(())
    }

    pub fn write_page_transfer(&self, page_num: PageNum, data: Vec<u8>) -> Result<()> {
        if self.read_only {
            return Err(Error::Other("cannot write page: database is read-only".to_string()));
        }

        if data.len() != PAGE_SIZE {
            return Err(Error::InvalidPageSize);
        }

        self.cache.put_dirty(page_num, data);

        Ok(())
    }

    pub fn alloc_page(&self) -> Result<PageNum> {
        self.alloc_page_minimum(0)
    }

    /// Allocate a page with page number >= min_page.
    /// Prevents root regression: when allocating for a root node, pass the current root
    /// as min_page to ensure the new root doesn't use a recycled page with a lower number.
    pub fn alloc_page_minimum(&self, min_page: PageNum) -> Result<PageNum> {
        if self.read_only {
            return Err(Error::Other("cannot allocate page: database is read-only".to_string()));
        }

        // Try free list, but skip pages below minimum
        let mut free_list = self.free_list.write()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.free_list".to_string() })?;

        while let Some(page_num) = free_list.pop() {
            if page_num >= min_page {
                drop(free_list);
                return Ok(page_num);
            }
            // Page too low, skip it and try next
        }
        drop(free_list);

        // No suitable page in free list, allocate fresh page
        let mut num_pages = self.num_pages.write()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.num_pages".to_string() })?;
        let page_num = *num_pages;
        *num_pages += 1;

        // Track metrics
        if let Ok(guard) = self.metrics.read() {
            if let Some(metrics) = guard.as_ref() {
                metrics.page_allocated();
            }
        }

        Ok(page_num)
    }

    pub fn free_page(&self, page_num: PageNum) -> Result<()> {
        if self.read_only {
            return Err(Error::Other("cannot free page: database is read-only".to_string()));
        }

        let mut free_list = self.free_list.write()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.free_list".to_string() })?;
        free_list.push(page_num);

        self.cache.remove(page_num);

        // Track metrics
        if let Ok(guard) = self.metrics.read() {
            if let Some(metrics) = guard.as_ref() {
                metrics.page_freed();
            }
        }

        Ok(())
    }

    /// Writes pages directly to disk, bypassing the cache entirely.
    /// Used during checkpoint to avoid cache pollution and lock overhead.
    ///
    /// TIER 2 OPTIMIZATION: Batches consecutive pages into single write syscalls.
    /// Instead of 5,000+ individual write()s, we group sequential pages and write them together.
    pub fn write_pages_direct(&self, pages: Vec<(PageNum, Vec<u8>)>) -> Result<()> {
        if self.read_only {
            return Err(Error::Other("cannot write pages: database is read-only".to_string()));
        }

        if pages.is_empty() {
            return Ok(());
        }

        // Sort by page number for sequential writes (PostgreSQL-style)
        let mut sorted_pages = pages;
        sorted_pages.sort_unstable_by_key(|(page_num, _)| *page_num);

        // Validate all pages first
        for (_, data) in &sorted_pages {
            if data.len() != PAGE_SIZE {
                return Err(Error::InvalidPageSize);
            }
        }

        let mut file = self.file.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.file".to_string() })?;

        let mut batch_start_idx = 0;

        while batch_start_idx < sorted_pages.len() {
            let (start_page_num, _) = sorted_pages[batch_start_idx];
            let mut batch_end_idx = batch_start_idx;

            // Find all consecutive pages starting from batch_start_idx
            while batch_end_idx + 1 < sorted_pages.len() {
                let (curr_page, _) = sorted_pages[batch_end_idx];
                let (next_page, _) = sorted_pages[batch_end_idx + 1];

                if next_page == curr_page + 1 {
                    batch_end_idx += 1;
                } else {
                    break;
                }
            }

            // Write batch [batch_start_idx..=batch_end_idx]
            let batch_size = batch_end_idx - batch_start_idx + 1;

            if batch_size == 1 {
                // Single page - write directly
                let (page_num, data) = &sorted_pages[batch_start_idx];
                let offset = page_num * PAGE_SIZE as u64;
                file.seek(SeekFrom::Start(offset))?;
                file.write_all(data)?;
            } else {
                // Multiple consecutive pages - batch them into one write
                let mut batch_buffer = Vec::with_capacity(batch_size * PAGE_SIZE);
                for i in batch_start_idx..=batch_end_idx {
                    batch_buffer.extend_from_slice(&sorted_pages[i].1);
                }

                let offset = start_page_num * PAGE_SIZE as u64;
                file.seek(SeekFrom::Start(offset))?;
                file.write_all(&batch_buffer)?;
            }

            batch_start_idx = batch_end_idx + 1;
        }

        file.sync_all()?;

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        if self.read_only {
            return Ok(());
        }

        let mut dirty_pages = self.cache.get_all_dirty();

        dirty_pages.sort_unstable();

        let mut file = self.file.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.file".to_string() })?;
        for page_num in &dirty_pages {
            if let Some(data) = self.cache.get_read_only(*page_num) {
                let offset = page_num * PAGE_SIZE as u64;
                file.seek(SeekFrom::Start(offset))?;
                file.write_all(&data)?;
            }
        }

        file.sync_all()?;
        self.cache.clear_all_dirty();

        Ok(())
    }

    pub fn flush_no_sync(&self) -> Result<()> {
        if self.read_only {
            return Ok(());
        }

        let dirty_pages = self.cache.get_all_dirty();

        let mut file = self.file.lock()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.file".to_string() })?;
        for page_num in &dirty_pages {
            if let Some(data) = self.cache.get_read_only(*page_num) {
                let offset = page_num * PAGE_SIZE as u64;
                file.seek(SeekFrom::Start(offset))?;
                file.write_all(&data)?;
            }
        }
        // No sync - caller handles it
        drop(file);

        self.cache.clear_all_dirty();

        Ok(())
    }

    pub fn close(self) -> Result<()> {
        if !self.read_only {
            self.flush()?;
            self.write_header()?;
        }
        Ok(())
    }

    pub fn allocate_transaction_id(&self) -> Result<TransactionID> {
        let mut next_tx_id = self.next_tx_id.write()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.next_tx_id".to_string() })?;
        let tx_id = *next_tx_id;
        *next_tx_id += 1;
        Ok(tx_id)
    }

    pub fn get_current_transaction_id(&self) -> Result<TransactionID> {
        Ok(*self.next_tx_id.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.next_tx_id".to_string() })?)
    }

    pub fn set_next_transaction_id(&self, tx_id: TransactionID) -> Result<()> {
        *self.next_tx_id.write()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.next_tx_id".to_string() })? = tx_id;
        Ok(())
    }

    pub fn num_pages(&self) -> Result<u64> {
        Ok(*self.num_pages.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.num_pages".to_string() })?)
    }

    pub fn set_num_pages(&self, n: u64) -> Result<()> {
        *self.num_pages.write()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.num_pages".to_string() })? = n;
        Ok(())
    }

    pub fn metadata_page(&self) -> Result<u64> {
        Ok(*self.metadata_page.read()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.metadata_page".to_string() })?)
    }

    pub fn set_metadata_page(&self, page_num: u64) -> Result<()> {
        *self.metadata_page.write()
            .map_err(|_| Error::LockPoisoned { lock_name: "pager.metadata_page".to_string() })? = page_num;
        Ok(())
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    pub(crate) fn set_metrics(&self, metrics: Arc<Metrics>) {
        if let Ok(mut guard) = self.metrics.write() {
            *guard = Some(metrics);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_header_serialization() {
        let header = Header::new();
        let data = header.serialize();
        let parsed = Header::deserialize(&data).unwrap();

        assert_eq!(parsed.magic, MAGIC);
        assert_eq!(parsed.version, VERSION);
        assert_eq!(parsed.page_size, PAGE_SIZE as u32);
    }

    #[test]
    fn test_pager_create() {
        let path = "/tmp/test_pager_create.db";
        let _ = fs::remove_file(path);

        let pager = Pager::open(path, 100, 0o644, false).unwrap();
        assert_eq!(pager.num_pages().unwrap(), 1);

        pager.close().unwrap();
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_page_alloc_and_write() {
        let path = "/tmp/test_page_alloc.db";
        let _ = fs::remove_file(path);

        let pager = Pager::open(path, 100, 0o644, false).unwrap();

        let page_num = pager.alloc_page().unwrap();
        assert_eq!(page_num, 1);

        let data = vec![42u8; PAGE_SIZE];
        pager.write_page_transfer(page_num, data.clone()).unwrap();

        let read_data = pager.read_page(page_num).unwrap();
        assert_eq!(read_data, data);

        pager.close().unwrap();
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_free_list() {
        let path = "/tmp/test_free_list.db";
        let _ = fs::remove_file(path);

        let pager = Pager::open(path, 100, 0o644, false).unwrap();

        let page1 = pager.alloc_page().unwrap();
        let page2 = pager.alloc_page().unwrap();

        assert_eq!(page1, 1);
        assert_eq!(page2, 2);

        pager.free_page(page1).unwrap();

        let page3 = pager.alloc_page().unwrap();
        assert_eq!(page3, page1);

        pager.close().unwrap();
        let _ = fs::remove_file(path);
    }
}
