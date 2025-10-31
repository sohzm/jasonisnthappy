
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use crate::core::PageNum;
use crate::core::errors::PoisonedLockExt;

#[derive(Clone)]
struct Node {
    page_num: PageNum,
    data: Vec<u8>,
    prev: Option<usize>,
    next: Option<usize>,
}

struct LRUCacheInner {
    capacity: usize,
    cache: HashMap<PageNum, usize>, // Maps page_num to index in nodes vec
    nodes: Vec<Option<Node>>,       // Node storage
    head: Option<usize>,
    tail: Option<usize>,
    free_list: Vec<usize>,          // Indices of freed nodes
    dirty: HashMap<PageNum, bool>,
    failed_evictions: usize,        // Counter for consecutive failed evictions
}

impl LRUCacheInner {
    fn new(capacity: usize) -> Self {
        LRUCacheInner {
            capacity,
            cache: HashMap::new(),
            nodes: Vec::new(),
            head: None,
            tail: None,
            free_list: Vec::new(),
            dirty: HashMap::new(),
            failed_evictions: 0,
        }
    }

    fn allocate_node(&mut self, page_num: PageNum, data: Vec<u8>) -> usize {
        let node = Node {
            page_num,
            data,
            prev: None,
            next: None,
        };

        if let Some(idx) = self.free_list.pop() {
            self.nodes[idx] = Some(node);
            idx
        } else {
            self.nodes.push(Some(node));
            self.nodes.len() - 1
        }
    }

    fn move_to_front(&mut self, idx: usize) {
        if Some(idx) == self.head {
            return;
        }

        // Extract node info first
        let (prev_idx, next_idx) = if let Some(node) = &self.nodes[idx] {
            (node.prev, node.next)
        } else {
            return;
        };

        // Update neighbors
        if let Some(prev) = prev_idx {
            if let Some(prev_node) = &mut self.nodes[prev] {
                prev_node.next = next_idx;
            }
        }
        if let Some(next) = next_idx {
            if let Some(next_node) = &mut self.nodes[next] {
                next_node.prev = prev_idx;
            }
        }
        if Some(idx) == self.tail {
            self.tail = prev_idx;
        }

        // Update node to be at front
        if let Some(node) = &mut self.nodes[idx] {
            node.prev = None;
            node.next = self.head;
        }

        // Update old head
        if let Some(old_head_idx) = self.head {
            if let Some(old_head) = &mut self.nodes[old_head_idx] {
                old_head.prev = Some(idx);
            }
        }

        self.head = Some(idx);

        if self.tail.is_none() {
            self.tail = Some(idx);
        }
    }

    fn remove_node(&mut self, idx: usize) {
        // Extract node info first
        let (prev_idx, next_idx) = if let Some(node) = &self.nodes[idx] {
            (node.prev, node.next)
        } else {
            return;
        };

        if Some(idx) == self.head {
            self.head = next_idx;
        }
        if Some(idx) == self.tail {
            self.tail = prev_idx;
        }

        // Update neighbors
        if let Some(prev) = prev_idx {
            if let Some(prev_node) = &mut self.nodes[prev] {
                prev_node.next = next_idx;
            }
        }
        if let Some(next) = next_idx {
            if let Some(next_node) = &mut self.nodes[next] {
                next_node.prev = prev_idx;
            }
        }

        self.nodes[idx] = None;
        self.free_list.push(idx);
    }

    fn get(&mut self, page_num: PageNum) -> Option<Vec<u8>> {
        if let Some(&idx) = self.cache.get(&page_num) {
            self.move_to_front(idx);
            if let Some(node) = &self.nodes[idx] {
                Some(node.data.clone())
            } else {
                None
            }
        } else {
            None
        }
    }

    fn get_read_only(&self, page_num: PageNum) -> Option<Vec<u8>> {
        if let Some(&idx) = self.cache.get(&page_num) {
            if let Some(node) = &self.nodes[idx] {
                Some(node.data.clone())
            } else {
                None
            }
        } else {
            None
        }
    }

    fn put(&mut self, page_num: PageNum, data: Vec<u8>) {
        if let Some(&idx) = self.cache.get(&page_num) {
            self.move_to_front(idx);
            if let Some(node) = &mut self.nodes[idx] {
                node.data = data;
            }
        } else {
            let idx = self.allocate_node(page_num, data);
            self.cache.insert(page_num, idx);

            // Add to front
            if let Some(node) = &mut self.nodes[idx] {
                node.prev = None;
                node.next = self.head;
            }

            if let Some(old_head_idx) = self.head {
                if let Some(old_head) = &mut self.nodes[old_head_idx] {
                    old_head.prev = Some(idx);
                }
            }

            self.head = Some(idx);

            if self.tail.is_none() {
                self.tail = Some(idx);
            }

            if self.cache.len() > self.capacity {
                self.evict();
            }
        }
    }

    // Atomic put + mark dirty: adds page to cache and marks dirty, then evicts if needed
    // This prevents race condition where flush() might see dirty flag but page not in cache
    fn put_dirty_atomic(&mut self, page_num: PageNum, data: Vec<u8>) {
        // If page already exists, just update it
        if let Some(&idx) = self.cache.get(&page_num) {
            self.move_to_front(idx);
            if let Some(node) = &mut self.nodes[idx] {
                node.data = data;
            }
            self.dirty.insert(page_num, true);
            return;
        }

        // New page: add to cache first, then mark dirty, then evict if needed
        let idx = self.allocate_node(page_num, data);
        self.cache.insert(page_num, idx);

        // Add to front of LRU list
        if let Some(node) = &mut self.nodes[idx] {
            node.prev = None;
            node.next = self.head;
        }

        if let Some(old_head_idx) = self.head {
            if let Some(old_head) = &mut self.nodes[old_head_idx] {
                old_head.prev = Some(idx);
            }
        }

        self.head = Some(idx);

        if self.tail.is_none() {
            self.tail = Some(idx);
        }

        // Mark dirty AFTER adding to cache to prevent race with flush()
        self.dirty.insert(page_num, true);

        // Now check if we need to evict
        if self.cache.len() > self.capacity {
            self.evict();
        }
    }

    fn evict(&mut self) {
        let mut current = self.tail;
        let mut checked = 0;
        let max_checks = self.cache.len();
        let mut found_clean = false;

        // First pass: try to evict a clean (non-dirty) page
        while let Some(idx) = current {
            if checked >= max_checks {
                break;
            }
            checked += 1;

            if let Some(node) = &self.nodes[idx] {
                let page_num = node.page_num;
                let prev = node.prev;

                if !self.dirty.get(&page_num).copied().unwrap_or(false) {
                    // Found a clean page - evict it
                    self.cache.remove(&page_num);
                    self.remove_node(idx);
                    self.failed_evictions = 0; // Reset counter on successful eviction
                    found_clean = true;
                    break;
                }

                current = prev;
            } else {
                break;
            }
        }

        if !found_clean {
            self.failed_evictions += 1;
        }
    }

    fn remove(&mut self, page_num: PageNum) {
        if let Some(idx) = self.cache.remove(&page_num) {
            self.remove_node(idx);
            self.dirty.remove(&page_num);
        }
    }

    fn len(&self) -> usize {
        self.cache.len()
    }
}

pub struct LRUCache {
    inner: Arc<RwLock<LRUCacheInner>>,
}

impl LRUCache {
    pub fn new(capacity: usize) -> Self {
        let capacity = if capacity == 0 { 1000 } else { capacity };

        Self {
            inner: Arc::new(RwLock::new(LRUCacheInner::new(capacity))),
        }
    }

    pub fn get(&self, page_num: PageNum) -> Option<Vec<u8>> {
        let mut inner = self.inner.write()
            .recover_poison();
        inner.get(page_num)
    }

    pub fn get_read_only(&self, page_num: PageNum) -> Option<Vec<u8>> {
        let inner = self.inner.read()
            .recover_poison();
        inner.get_read_only(page_num)
    }

    pub fn get_shared(&self, page_num: PageNum) -> Option<Vec<u8>> {
        let inner = self.inner.read()
            .recover_poison();
        inner.get_read_only(page_num)
    }

    pub fn put(&self, page_num: PageNum, data: Vec<u8>) {
        let mut inner = self.inner.write()
            .recover_poison();
        inner.put(page_num, data);
    }

    pub fn mark_dirty(&self, page_num: PageNum) {
        let mut inner = self.inner.write()
            .recover_poison();
        inner.dirty.insert(page_num, true);
    }

    pub fn put_dirty(&self, page_num: PageNum, data: Vec<u8>) {
        let mut inner = self.inner.write()
            .recover_poison();
        // Use atomic version that adds to cache and marks dirty without eviction window
        inner.put_dirty_atomic(page_num, data);
    }

    pub fn clear_dirty(&self, page_num: PageNum) {
        let mut inner = self.inner.write()
            .recover_poison();
        inner.dirty.remove(&page_num);
    }

    pub fn is_dirty(&self, page_num: PageNum) -> bool {
        let inner = self.inner.read()
            .recover_poison();
        inner.dirty.get(&page_num).copied().unwrap_or(false)
    }

    pub fn get_all_dirty(&self) -> Vec<PageNum> {
        let inner = self.inner.read()
            .recover_poison();
        inner.dirty.keys().copied().collect()
    }

    pub fn clear_all_dirty(&self) {
        let mut inner = self.inner.write()
            .recover_poison();
        inner.dirty.clear();
    }

    pub fn remove(&self, page_num: PageNum) {
        let mut inner = self.inner.write()
            .recover_poison();
        inner.remove(page_num);
    }

    pub fn len(&self) -> usize {
        let inner = self.inner.read()
            .recover_poison();
        inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let cache = LRUCache::new(3);

        cache.put(1, vec![1, 2, 3]);
        assert_eq!(cache.get(1), Some(vec![1, 2, 3]));
        assert_eq!(cache.len(), 1);

        assert_eq!(cache.get(99), None);
    }

    #[test]
    fn test_eviction() {
        let cache = LRUCache::new(3);

        cache.put(1, vec![1]);
        cache.put(2, vec![2]);
        cache.put(3, vec![3]);
        cache.put(4, vec![4]);

        assert_eq!(cache.get(1), None);
        assert_eq!(cache.get(2), Some(vec![2]));
        assert_eq!(cache.get(3), Some(vec![3]));
        assert_eq!(cache.get(4), Some(vec![4]));
    }

    #[test]
    fn test_dirty_no_evict() {
        let cache = LRUCache::new(2);

        cache.put(1, vec![1]);
        cache.mark_dirty(1);
        cache.put(2, vec![2]);
        cache.put(3, vec![3]);

        assert_eq!(cache.get(1), Some(vec![1]));
    }

    #[test]
    fn test_dirty_tracking() {
        let cache = LRUCache::new(5);

        cache.put(1, vec![1]);
        cache.put(2, vec![2]);

        cache.mark_dirty(1);
        assert!(cache.is_dirty(1));
        assert!(!cache.is_dirty(2));

        let dirty = cache.get_all_dirty();
        assert_eq!(dirty.len(), 1);
        assert!(dirty.contains(&1));

        cache.clear_dirty(1);
        assert!(!cache.is_dirty(1));

        cache.mark_dirty(1);
        cache.mark_dirty(2);
        cache.clear_all_dirty();
        assert!(!cache.is_dirty(1));
        assert!(!cache.is_dirty(2));
    }

    #[test]
    fn test_lru_order() {
        let cache = LRUCache::new(3);

        cache.put(1, vec![1]);
        cache.put(2, vec![2]);
        cache.put(3, vec![3]);

        cache.get(1);

        cache.put(4, vec![4]);

        assert_eq!(cache.get(1), Some(vec![1]));
        assert_eq!(cache.get(2), None);
        assert_eq!(cache.get(3), Some(vec![3]));
        assert_eq!(cache.get(4), Some(vec![4]));
    }
}
