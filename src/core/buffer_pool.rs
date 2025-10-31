
use crate::core::constants::PAGE_SIZE;
use std::sync::Mutex;

pub struct BufferPool {
    buffers: Mutex<Vec<Vec<u8>>>,
    max_size: usize,
}

impl BufferPool {
    pub fn new(max_size: usize) -> Self {
        Self {
            buffers: Mutex::new(Vec::new()),
            max_size,
        }
    }

    pub fn get(&self) -> Vec<u8> {
        if let Ok(mut buffers) = self.buffers.lock() {
            buffers.pop().unwrap_or_else(|| vec![0u8; PAGE_SIZE])
        } else {
            vec![0u8; PAGE_SIZE]
        }
    }

    pub fn put(&self, mut buf: Vec<u8>) {
        if buf.len() != PAGE_SIZE {
            return;
        }

        if let Ok(mut buffers) = self.buffers.lock() {
            if buffers.len() < self.max_size {
                buf.fill(0);
                buffers.push(buf);
            }
        }
    }

    /// Get the current number of buffers in the pool
    pub fn len(&self) -> usize {
        self.buffers.lock().map(|b| b.len()).unwrap_or(0)
    }

    /// Check if the pool is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the maximum capacity of the pool
    pub fn capacity(&self) -> usize {
        self.max_size
    }
}

static NODE_SERIALIZE_POOL: once_cell::sync::Lazy<BufferPool> =
    once_cell::sync::Lazy::new(|| BufferPool::new(128));

pub fn get_node_serialize_buffer() -> Vec<u8> {
    NODE_SERIALIZE_POOL.get()
}

pub fn put_node_serialize_buffer(buf: Vec<u8>) {
    NODE_SERIALIZE_POOL.put(buf);
}

static PAGE_BUFFER_POOL: once_cell::sync::Lazy<BufferPool> =
    once_cell::sync::Lazy::new(|| BufferPool::new(256));

pub fn get_page_buffer() -> Vec<u8> {
    PAGE_BUFFER_POOL.get()
}

pub fn put_page_buffer(buf: Vec<u8>) {
    PAGE_BUFFER_POOL.put(buf);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool() {
        let pool = BufferPool::new(10);

        let buf1 = pool.get();
        assert_eq!(buf1.len(), PAGE_SIZE);

        pool.put(buf1);

        let buf2 = pool.get();
        assert_eq!(buf2.len(), PAGE_SIZE);

        let count_before = pool.buffers.lock().expect("lock poisoned").len();
        pool.put(buf2);
        let count_after = pool.buffers.lock().expect("lock poisoned").len();
        assert_eq!(count_after, count_before + 1);
    }

    #[test]
    fn test_buffer_pool_max_size() {
        let pool = BufferPool::new(2);

        let buf1 = pool.get();
        let buf2 = pool.get();
        let buf3 = pool.get();

        pool.put(buf1);
        pool.put(buf2);
        pool.put(buf3);

        assert_eq!(pool.buffers.lock().expect("lock poisoned").len(), 2);
    }

    #[test]
    fn test_global_pools() {
        let buf1 = get_node_serialize_buffer();
        assert_eq!(buf1.len(), PAGE_SIZE);
        put_node_serialize_buffer(buf1);

        let buf2 = get_page_buffer();
        assert_eq!(buf2.len(), PAGE_SIZE);
        put_page_buffer(buf2);
    }
}
