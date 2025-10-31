
use crate::core::constants::*;
use crate::core::errors::*;
use crate::core::pager::Pager;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct Document {
    pub id: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct VersionedDocument {
    pub id: String,
    pub data: Vec<u8>,
    pub xmin: TransactionID,
    pub xmax: TransactionID,
}

impl VersionedDocument {
    pub fn is_visible(&self, snapshot_id: TransactionID) -> bool {
        if self.xmin > snapshot_id {
            return false;
        }

        if self.xmax != 0 && self.xmax <= snapshot_id {
            return false;
        }

        true
    }
}

pub fn write_document(
    pager: &Pager,
    doc_id: &str,
    data: &[u8],
) -> Result<PageNum> {
    let id_bytes = doc_id.as_bytes();
    let total_size = DOC_ID_LEN_SIZE + id_bytes.len() + DATA_LEN_SIZE + data.len() + OVERFLOW_SIZE;

    if total_size <= PAGE_SIZE {
        write_single_page(pager, doc_id, data)
    } else {
        write_multi_page(pager, doc_id, data)
    }
}

fn write_single_page(
    pager: &Pager,
    doc_id: &str,
    data: &[u8],
) -> Result<PageNum> {
    let page_num = pager.alloc_page()?;

    let mut buf = crate::core::buffer_pool::get_page_buffer();
    let mut offset = 0;

    let id_bytes = doc_id.as_bytes();
    buf[offset..offset + 2].copy_from_slice(&(id_bytes.len() as u16).to_le_bytes());
    offset += 2;

    buf[offset..offset + id_bytes.len()].copy_from_slice(id_bytes);
    offset += id_bytes.len();

    buf[offset..offset + 4].copy_from_slice(&(data.len() as u32).to_le_bytes());
    offset += 4;

    buf[offset..offset + data.len()].copy_from_slice(data);
    offset += data.len();

    buf[offset..offset + 8].copy_from_slice(&0u64.to_le_bytes());

    pager.write_page_transfer(page_num, buf)?;
    Ok(page_num)
}

fn write_multi_page(
    pager: &Pager,
    doc_id: &str,
    data: &[u8],
) -> Result<PageNum> {
    let first_page = pager.alloc_page()?;
    let id_bytes = doc_id.as_bytes();

    let first_page_available = PAGE_SIZE - DOC_ID_LEN_SIZE - id_bytes.len() - DATA_LEN_SIZE - OVERFLOW_SIZE;
    let first_chunk_size = first_page_available.min(data.len());

    let mut chunks = Vec::new();
    let mut remaining = &data[first_chunk_size..];
    while !remaining.is_empty() {
        let chunk_size = remaining.len().min(MAX_OVERFLOW_DATA);
        chunks.push(&remaining[..chunk_size]);
        remaining = &remaining[chunk_size..];
    }

    let mut next_overflow = 0u64;
    for chunk in chunks.iter().rev() {
        let overflow_page = pager.alloc_page()?;

        let mut overflow_buf = crate::core::buffer_pool::get_page_buffer();

        overflow_buf[..chunk.len()].copy_from_slice(chunk);
        overflow_buf[PAGE_SIZE - 8..].copy_from_slice(&next_overflow.to_le_bytes());

        pager.write_page_transfer(overflow_page, overflow_buf)?;

        next_overflow = overflow_page;
    }

    let mut first_buf = crate::core::buffer_pool::get_page_buffer();
    let mut offset = 0;

    first_buf[offset..offset + 2].copy_from_slice(&(id_bytes.len() as u16).to_le_bytes());
    offset += 2;

    first_buf[offset..offset + id_bytes.len()].copy_from_slice(id_bytes);
    offset += id_bytes.len();

    first_buf[offset..offset + 4].copy_from_slice(&(data.len() as u32).to_le_bytes());
    offset += 4;

    first_buf[offset..offset + first_chunk_size].copy_from_slice(&data[..first_chunk_size]);

    let overflow_offset = PAGE_SIZE - 8;
    first_buf[overflow_offset..overflow_offset + 8].copy_from_slice(&next_overflow.to_le_bytes());

    pager.write_page_transfer(first_page, first_buf)?;
    Ok(first_page)
}

pub fn read_document(pager: &Pager, page_num: PageNum) -> Result<Document> {
    let page_data = pager.read_page(page_num)?;
    let mut offset = 0;

    let id_len = u16::from_le_bytes(page_data[offset..offset + 2].try_into().unwrap()) as usize;
    offset += 2;

    if id_len == 0 || id_len > 255 {
        return Err(Error::InvalidDocument);
    }

    if offset + id_len > page_data.len() {
        return Err(Error::InvalidDocument);
    }

    let doc_id = String::from_utf8(page_data[offset..offset + id_len].to_vec())
        .map_err(|_| Error::InvalidDocument)?;
    offset += id_len;

    if offset + 4 > page_data.len() {
        return Err(Error::InvalidDocument);
    }

    let data_len = u32::from_le_bytes(page_data[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    const MAX_DOCUMENT_SIZE: usize = 1024 * 1024 * 1024;
    if data_len > MAX_DOCUMENT_SIZE {
        return Err(Error::InvalidDocument);
    }

    let overflow_offset = PAGE_SIZE - 8;
    let overflow_page = u64::from_le_bytes(page_data[overflow_offset..overflow_offset + 8].try_into().unwrap());

    if overflow_page == 0 {
        // Single page document - validate we can read the data
        if offset + data_len > page_data.len() {
            return Err(Error::InvalidDocument);
        }
        let data = page_data[offset..offset + data_len].to_vec();
        Ok(Document { id: doc_id, data })
    } else {
        // Multi-page document with overflow chain
        let first_chunk_size = PAGE_SIZE - DOC_ID_LEN_SIZE - id_len - DATA_LEN_SIZE - OVERFLOW_SIZE;
        let mut data = Vec::with_capacity(data_len);
        let first_data_len = first_chunk_size.min(data_len);

        data.extend_from_slice(&page_data[offset..offset + first_data_len]);

        // Read overflow chain with cycle detection
        let mut current_overflow = overflow_page;
        let mut visited_pages = HashSet::new();
        let mut chain_length = 0;

        while current_overflow != 0 && data.len() < data_len {
            // Detect cycles - if we've seen this page before, it's a cycle
            if visited_pages.contains(&current_overflow) {
                return Err(Error::Other(format!(
                    "Overflow chain cycle detected at page {}. Document is corrupted.",
                    current_overflow
                )));
            }
            visited_pages.insert(current_overflow);

            // Check max chain length to prevent excessive reads
            chain_length += 1;
            if chain_length > MAX_OVERFLOW_CHAIN_LENGTH {
                return Err(Error::Other(format!(
                    "Overflow chain too long (>{} pages). Document may be corrupted.",
                    MAX_OVERFLOW_CHAIN_LENGTH
                )));
            }

            let overflow_data = pager.read_page(current_overflow)?;
            let remaining = data_len - data.len();
            let chunk_size = remaining.min(MAX_OVERFLOW_DATA);

            data.extend_from_slice(&overflow_data[..chunk_size]);

            current_overflow = u64::from_le_bytes(
                overflow_data[PAGE_SIZE - 8..].try_into().unwrap()
            );
        }

        // Verify we read exactly the expected amount of data
        if data.len() != data_len {
            return Err(Error::Other(format!(
                "Document size mismatch: expected {} bytes, got {} bytes. Document may be corrupted.",
                data_len, data.len()
            )));
        }

        Ok(Document { id: doc_id, data })
    }
}

pub fn delete_document(pager: &Pager, page_num: PageNum) -> Result<()> {
    let page_data = pager.read_page(page_num)?;

    let overflow_offset = PAGE_SIZE - 8;
    let mut overflow_page = u64::from_le_bytes(
        page_data[overflow_offset..overflow_offset + 8].try_into().unwrap()
    );

    // Track visited pages to detect cycles
    let mut visited_pages = HashSet::new();
    let mut chain_length = 0;

    while overflow_page != 0 {
        // Detect cycles - if we've seen this page before, it's a cycle
        if visited_pages.contains(&overflow_page) {
            return Err(Error::Other(format!(
                "Overflow chain cycle detected at page {} during delete. Document is corrupted.",
                overflow_page
            )));
        }
        visited_pages.insert(overflow_page);

        // Check max chain length to prevent excessive iterations
        chain_length += 1;
        if chain_length > MAX_OVERFLOW_CHAIN_LENGTH {
            return Err(Error::Other(format!(
                "Overflow chain too long (>{} pages) during delete. Document may be corrupted.",
                MAX_OVERFLOW_CHAIN_LENGTH
            )));
        }

        let overflow_data = pager.read_page(overflow_page)?;
        let next_overflow = u64::from_le_bytes(
            overflow_data[PAGE_SIZE - 8..].try_into().unwrap()
        );

        pager.free_page(overflow_page)?;
        overflow_page = next_overflow;
    }

    pager.free_page(page_num)?;
    Ok(())
}

pub fn write_versioned_document(
    pager: &Pager,
    doc_id: &str,
    data: &[u8],
    xmin: TransactionID,
    xmax: TransactionID,
    tx_writes: &mut HashMap<PageNum, Vec<u8>>,
) -> Result<(PageNum, Vec<u8>)> {
    if doc_id.len() > 255 {
        return Err(Error::InvalidDocument);
    }

    let id_bytes = doc_id.as_bytes();
    let first_page_header = XMIN_SIZE + XMAX_SIZE + DOC_ID_LEN_SIZE + id_bytes.len() + DATA_LEN_SIZE + OVERFLOW_SIZE;
    let first_page_capacity = PAGE_SIZE - first_page_header;

    let first_page_num = pager.alloc_page()?;
    let mut first_page_data = vec![0u8; PAGE_SIZE];
    let mut offset = 0;

    // Write xmin
    first_page_data[offset..offset + 8].copy_from_slice(&xmin.to_le_bytes());
    offset += 8;

    // Write xmax
    first_page_data[offset..offset + 8].copy_from_slice(&xmax.to_le_bytes());
    offset += 8;

    // Write docID length
    first_page_data[offset..offset + 2].copy_from_slice(&(id_bytes.len() as u16).to_le_bytes());
    offset += 2;

    // Write docID
    first_page_data[offset..offset + id_bytes.len()].copy_from_slice(id_bytes);
    offset += id_bytes.len();

    // Write data length
    first_page_data[offset..offset + 4].copy_from_slice(&(data.len() as u32).to_le_bytes());
    offset += 4;

    // Write first chunk of data
    let mut data_offset = 0;
    let chunk_size = first_page_capacity.min(data.len());
    first_page_data[offset..offset + chunk_size].copy_from_slice(&data[data_offset..data_offset + chunk_size]);
    offset += chunk_size;
    data_offset += chunk_size;

    // Overflow pointer (initially 0)
    let overflow_offset = offset;
    first_page_data[overflow_offset..overflow_offset + 8].copy_from_slice(&0u64.to_le_bytes());

    // Write overflow pages if needed
    let mut allocated_pages = vec![first_page_num];
    let mut prev_page_num = first_page_num;
    let mut prev_overflow_offset = overflow_offset;
    let mut prev_overflow_page_data: Option<Vec<u8>> = None;

    while data_offset < data.len() {
        let overflow_page_num = pager.alloc_page().map_err(|e| {
            for p in &allocated_pages {
                let _ = pager.free_page(*p);
            }
            e
        })?;
        allocated_pages.push(overflow_page_num);

        // Allocate new overflow page
        let mut overflow_page_data = vec![0u8; PAGE_SIZE];
        let chunk_size = MAX_OVERFLOW_DATA.min(data.len() - data_offset);
        overflow_page_data[..chunk_size].copy_from_slice(&data[data_offset..data_offset + chunk_size]);
        data_offset += chunk_size;

        let overflow_page_data_offset = PAGE_SIZE - OVERFLOW_SIZE;
        overflow_page_data[overflow_page_data_offset..overflow_page_data_offset + 8].copy_from_slice(&0u64.to_le_bytes());

        // Update the PREVIOUS page's overflow pointer to point to this new page
        if prev_page_num == first_page_num {
            // Previous was first page - update it directly
            first_page_data[prev_overflow_offset..prev_overflow_offset + 8].copy_from_slice(&overflow_page_num.to_le_bytes());
        } else {
            // Previous was an overflow page - update it and write it
            if let Some(ref mut prev_buf) = prev_overflow_page_data {
                prev_buf[prev_overflow_offset..prev_overflow_offset + 8].copy_from_slice(&overflow_page_num.to_le_bytes());

                // Add to write buffer
                let prev_page_copy = prev_buf.clone();
                tx_writes.insert(prev_page_num, prev_page_copy.clone());

                pager.write_page_transfer(prev_page_num, prev_buf.clone()).map_err(|e| {
                    for p in &allocated_pages {
                        let _ = pager.free_page(*p);
                    }
                    e
                })?;
            }
        }

        // This overflow page becomes the new "previous"
        prev_page_num = overflow_page_num;
        prev_overflow_offset = overflow_page_data_offset;
        prev_overflow_page_data = Some(overflow_page_data);
    }

    // Write the last overflow page if there is one
    if let Some(ref last_overflow_buf) = prev_overflow_page_data {
        // Add to write buffer
        let last_page_copy = last_overflow_buf.clone();
        tx_writes.insert(prev_page_num, last_page_copy.clone());

        pager.write_page_transfer(prev_page_num, last_overflow_buf.clone()).map_err(|e| {
            for p in &allocated_pages {
                let _ = pager.free_page(*p);
            }
            e
        })?;
    }

    // Make a copy of the first page data to return
    let first_page_copy = first_page_data.clone();

    // Add first page to write buffer
    tx_writes.insert(first_page_num, first_page_copy.clone());

    pager.write_page_transfer(first_page_num, first_page_data).map_err(|e| {
        for p in &allocated_pages {
            let _ = pager.free_page(*p);
        }
        e
    })?;

    Ok((first_page_num, first_page_copy))
}

pub fn read_versioned_document(
    pager: &Pager,
    page_num: PageNum,
    tx_writes: &HashMap<PageNum, Vec<u8>>,
) -> Result<VersionedDocument> {
    // Check transaction's write buffer first (page may not be flushed yet)
    let page_data = if let Some(data) = tx_writes.get(&page_num) {
        data.clone()
    } else {
        pager.read_page(page_num)?
    };

    let mut offset = 0;

    // Read xmin
    let xmin = u64::from_le_bytes(page_data[offset..offset + 8].try_into().unwrap());
    offset += 8;

    // Read xmax
    let xmax = u64::from_le_bytes(page_data[offset..offset + 8].try_into().unwrap());
    offset += 8;

    // Read docID length
    let id_len = u16::from_le_bytes(page_data[offset..offset + 2].try_into().unwrap()) as usize;
    offset += 2;

    if id_len == 0 || id_len > page_data.len() - offset {
        return Err(Error::InvalidDocument);
    }

    // Read docID
    let id = String::from_utf8(page_data[offset..offset + id_len].to_vec())
        .map_err(|_| Error::InvalidDocument)?;
    offset += id_len;

    // Read data length
    let data_len = u32::from_le_bytes(page_data[offset..offset + 4].try_into().unwrap()) as usize;
    offset += 4;

    const MAX_DOCUMENT_SIZE: usize = 1024 * 1024 * 1024;
    if data_len > MAX_DOCUMENT_SIZE {
        return Err(Error::InvalidDocument);
    }

    let first_page_capacity = PAGE_SIZE - (XMIN_SIZE + XMAX_SIZE + DOC_ID_LEN_SIZE + id_len + DATA_LEN_SIZE + OVERFLOW_SIZE);

    let mut data = vec![0u8; data_len];
    let mut data_offset = 0;

    // Read first chunk
    let first_chunk_size = first_page_capacity.min(data_len);
    data[data_offset..data_offset + first_chunk_size].copy_from_slice(&page_data[offset..offset + first_chunk_size]);
    data_offset += first_chunk_size;
    offset += first_chunk_size;

    // Read overflow pointer
    let mut overflow_page_num = u64::from_le_bytes(page_data[offset..offset + 8].try_into().unwrap());

    // Track visited pages to detect cycles
    let mut visited_pages = HashSet::new();
    let mut chain_length = 0;

    // Read overflow pages
    while overflow_page_num != 0 && data_offset < data_len {
        // Detect cycles - if we've seen this page before, it's a cycle
        if visited_pages.contains(&overflow_page_num) {
            return Err(Error::Other(format!(
                "Overflow chain cycle detected at page {}. Versioned document is corrupted.",
                overflow_page_num
            )));
        }
        visited_pages.insert(overflow_page_num);

        // Check max chain length to prevent excessive reads
        chain_length += 1;
        if chain_length > MAX_OVERFLOW_CHAIN_LENGTH {
            return Err(Error::Other(format!(
                "Overflow chain too long (>{} pages). Versioned document may be corrupted.",
                MAX_OVERFLOW_CHAIN_LENGTH
            )));
        }

        // Check transaction's write buffer first
        let overflow_data = if let Some(data) = tx_writes.get(&overflow_page_num) {
            data.clone()
        } else {
            pager.read_page(overflow_page_num)?
        };

        let chunk_size = MAX_OVERFLOW_DATA.min(data_len - data_offset);

        data[data_offset..data_offset + chunk_size].copy_from_slice(&overflow_data[..chunk_size]);
        data_offset += chunk_size;

        if PAGE_SIZE < OVERFLOW_SIZE {
            return Err(Error::InvalidDocument);
        }
        overflow_page_num = u64::from_le_bytes(overflow_data[PAGE_SIZE - OVERFLOW_SIZE..PAGE_SIZE].try_into().unwrap());
    }

    // Verify we read exactly the expected amount of data
    if data_offset != data_len {
        return Err(Error::Other(format!(
            "Versioned document size mismatch: expected {} bytes, got {} bytes. Document may be corrupted.",
            data_len, data_offset
        )));
    }

    Ok(VersionedDocument {
        id,
        data,
        xmin,
        xmax,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_write_read_small_document() {
        let path = "/tmp/test_doc_small.db";
        let _ = fs::remove_file(path);

        let pager = Pager::open(path, 100, 0o644, false).unwrap();

        let data = b"Hello, World!";
        let page_num = write_document(&pager, "doc1", data).unwrap();

        let doc = read_document(&pager, page_num).unwrap();
        assert_eq!(doc.id, "doc1");
        assert_eq!(doc.data, data);

        pager.close().unwrap();
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_write_read_large_document() {
        let path = "/tmp/test_doc_large.db";
        let _ = fs::remove_file(path);

        let pager = Pager::open(path, 100, 0o644, false).unwrap();

        let data = vec![42u8; PAGE_SIZE * 2];
        let page_num = write_document(&pager, "large_doc", &data).unwrap();

        let doc = read_document(&pager, page_num).unwrap();
        assert_eq!(doc.id, "large_doc");
        assert_eq!(doc.data.len(), PAGE_SIZE * 2);
        assert_eq!(doc.data, data);

        pager.close().unwrap();
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_delete_document() {
        let path = "/tmp/test_doc_delete.db";
        let _ = fs::remove_file(path);

        let pager = Pager::open(path, 100, 0o644, false).unwrap();

        let data = b"Delete me";
        let page_num = write_document(&pager, "doc_to_delete", data).unwrap();

        let doc = read_document(&pager, page_num).unwrap();
        assert_eq!(doc.id, "doc_to_delete");

        delete_document(&pager, page_num).unwrap();


        pager.close().unwrap();
        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_versioned_document_visibility() {
        let doc = VersionedDocument {
            id: "test".to_string(),
            data: vec![1, 2, 3],
            xmin: 5,
            xmax: 0,
        };

        assert!(!doc.is_visible(4));

        assert!(doc.is_visible(5));
        assert!(doc.is_visible(10));

        let deleted_doc = VersionedDocument {
            id: "test".to_string(),
            data: vec![1, 2, 3],
            xmin: 5,
            xmax: 10,
        };

        assert!(deleted_doc.is_visible(5));
        assert!(deleted_doc.is_visible(9));

        assert!(!deleted_doc.is_visible(10));
        assert!(!deleted_doc.is_visible(15));
    }
}
