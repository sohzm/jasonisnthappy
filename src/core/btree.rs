
use crate::core::constants::*;
use crate::core::errors::*;
use crate::core::pager::Pager;
use crate::core::buffer_pool::get_node_serialize_buffer;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use crate::core::errors::PoisonedLockExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum NodeType {
    InternalNode = 0,
    LeafNode = 1,
}

impl NodeType {
    fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(NodeType::InternalNode),
            1 => Ok(NodeType::LeafNode),
            _ => Err(Error::Other(format!("invalid node type: {}", value))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LeafEntry {
    pub key: String,
    pub value: u64,
}

#[derive(Debug, Clone)]
pub struct BTreeNode {
    pub page_num: PageNum,
    pub node_type: NodeType,
    pub num_keys: u16,
    pub parent: u64,

    pub entries: Vec<LeafEntry>,
    pub next_leaf: u64,

    pub keys: Vec<String>,
    pub children: Vec<u64>,
}

impl BTreeNode {
    pub(crate) fn new_leaf(page_num: PageNum) -> Self {
        Self {
            page_num,
            node_type: NodeType::LeafNode,
            num_keys: 0,
            parent: 0,
            entries: Vec::new(),
            next_leaf: 0,
            keys: Vec::new(),
            children: Vec::new(),
        }
    }

    pub(crate) fn new_internal(page_num: PageNum) -> Self {
        Self {
            page_num,
            node_type: NodeType::InternalNode,
            num_keys: 0,
            parent: 0,
            entries: Vec::new(),
            next_leaf: 0,
            keys: Vec::new(),
            children: Vec::new(),
        }
    }
}

// Inner struct to hold all mutable state behind a single lock
struct BTreeInner {
    root_page: u64,
    tx_active: bool,
    cow_pages: HashMap<u64, u64>,
    new_pages: HashMap<u64, bool>,
    modified_root: u64,
}

pub struct BTree {
    pager: Arc<Pager>,
    inner: Arc<RwLock<BTreeInner>>,
}

impl BTree {
    pub fn new(pager: Arc<Pager>) -> Result<Self> {
        let root_page = pager.alloc_page()?;

        let bt = Self {
            pager: pager.clone(),
            inner: Arc::new(RwLock::new(BTreeInner {
                root_page,
                tx_active: false,
                cow_pages: HashMap::new(),
                new_pages: HashMap::new(),
                modified_root: 0,
            })),
        };

        let root = BTreeNode::new_leaf(root_page);
        bt.write_node(&root)?;

        Ok(bt)
    }

    pub fn open(pager: Arc<Pager>, root_page: u64) -> Self {
        Self {
            pager,
            inner: Arc::new(RwLock::new(BTreeInner {
                root_page,
                tx_active: false,
                cow_pages: HashMap::new(),
                new_pages: HashMap::new(),
                modified_root: 0,
            })),
        }
    }

    /// Get a reference to the pager
    pub fn pager(&self) -> &Arc<Pager> {
        &self.pager
    }

    fn get_root_page(&self) -> u64 {
        let inner = self.inner.read()
            .recover_poison();
        if inner.modified_root != 0 {
            inner.modified_root
        } else {
            inner.root_page
        }
    }

    pub fn search(&self, doc_id: &str) -> Result<u64> {
        let root = self.get_root_page();
        let mut node = self.read_node(root)?;

        while node.node_type == NodeType::InternalNode {
            let child_page = self.find_child(&node, doc_id)?;
            node = self.read_node(child_page)?;
        }

        for entry in &node.entries {
            if entry.key == doc_id {
                return Ok(entry.value);
            }
        }

        Err(Error::NotFound)
    }

    pub fn insert(&self, doc_id: &str, page_num: u64) -> Result<()> {
        let root = self.get_root_page();
        let node = self.read_node(root)?;

        let (mut leaf, path) = self.find_leaf(&node, doc_id, vec![root])?;

        let new_entry = LeafEntry {
            key: doc_id.to_string(),
            value: page_num,
        };
        self.insert_into_leaf(&mut leaf, new_entry);

        if leaf.entries.len() > BTREE_ORDER {
            return self.split_leaf(&mut leaf, path);
        }

        self.write_node(&leaf)?;

        self.update_path_after_modification(leaf.page_num, &path)?;

        Ok(())
    }

    pub fn delete(&self, doc_id: &str) -> Result<()> {
        let root = self.get_root_page();
        let node = self.read_node(root)?;

        let (mut leaf, _) = self.find_leaf(&node, doc_id, vec![root])?;

        let mut found = false;
        for i in 0..leaf.entries.len() {
            if leaf.entries[i].key == doc_id {
                leaf.entries.remove(i);
                leaf.num_keys = leaf.entries.len() as u16;
                found = true;
                break;
            }
        }

        if !found {
            return Err(Error::NotFound);
        }

        self.write_node(&leaf)?;
        Ok(())
    }

    pub fn iterator(&self) -> Result<BTreeIterator<'_>> {
        let root = self.get_root_page();
        let mut node = self.read_node(root)?;

        while node.node_type == NodeType::InternalNode {
            if node.children.is_empty() {
                return Err(Error::Other("internal node has no children".to_string()));
            }
            node = self.read_node(node.children[0])?;
        }

        Ok(BTreeIterator {
            bt: self,
            current_leaf: Some(node),
            index: 0,
            started: false,
        })
    }

    pub fn count(&self) -> Result<usize> {
        let mut iter = self.iterator()?;
        let mut count = 0;
        while iter.next() {
            count += 1;
        }
        Ok(count)
    }

    pub fn root_page(&self) -> u64 {
        self.get_root_page()
    }

    pub fn begin_transaction(&self) {
        let mut inner = self.inner.write()
            .recover_poison();
        inner.tx_active = true;
        inner.cow_pages.clear();
        inner.new_pages.clear();
        inner.modified_root = 0;
    }

    pub fn commit_transaction(&self) {
        let mut inner = self.inner.write()
            .recover_poison();
        if inner.modified_root != 0 {
            inner.root_page = inner.modified_root;
        }

        inner.tx_active = false;
        inner.cow_pages.clear();
        inner.new_pages.clear();
        inner.modified_root = 0;
    }

    pub fn rollback_transaction(&self) {
        let inner = self.inner.write()
            .recover_poison();
        let cow_pages_clone: Vec<u64> = inner.cow_pages.values().copied().collect();
        drop(inner); // Release lock before calling pager methods

        for new_page in cow_pages_clone {
            let _ = self.pager.free_page(new_page);
        }

        let mut inner = self.inner.write()
            .recover_poison();
        inner.tx_active = false;
        inner.cow_pages.clear();
        inner.new_pages.clear();
        inner.modified_root = 0;
    }


    fn find_child(&self, node: &BTreeNode, key: &str) -> Result<u64> {
        if node.children.is_empty() {
            return Err(Error::Corruption {
                component: "btree".to_string(),
                page_num: node.page_num,
                details: "internal node has no children".to_string(),
            });
        }

        let mut idx = 0;
        for (i, node_key) in node.keys.iter().enumerate() {
            if key < node_key {
                break;
            }
            idx = i + 1;
        }

        if idx >= node.children.len() {
            return Err(Error::Corruption {
                component: "btree".to_string(),
                page_num: node.page_num,
                details: format!("child index {} out of range (have {} children)", idx, node.children.len()),
            });
        }

        Ok(node.children[idx])
    }

    fn find_leaf(&self, node: &BTreeNode, key: &str, mut path: Vec<u64>) -> Result<(BTreeNode, Vec<u64>)> {
        if node.node_type == NodeType::LeafNode {
            return Ok((node.clone(), path));
        }

        let child_page = self.find_child(node, key)?;
        let child = self.read_node(child_page)?;
        path.push(child_page);

        self.find_leaf(&child, key, path)
    }

    fn insert_into_leaf(&self, leaf: &mut BTreeNode, entry: LeafEntry) {
        for e in leaf.entries.iter_mut() {
            if e.key == entry.key {
                e.value = entry.value;
                return;
            }
        }

        let mut idx = leaf.entries.len();
        for (i, e) in leaf.entries.iter().enumerate() {
            if entry.key < e.key {
                idx = i;
                break;
            }
        }

        leaf.entries.insert(idx, entry);
        leaf.num_keys = leaf.entries.len() as u16;
    }

    fn split_leaf(&self, leaf: &mut BTreeNode, path: Vec<u64>) -> Result<()> {
        let mid = leaf.entries.len() / 2;

        let new_leaf_page = self.pager.alloc_page()?;

        {
            let mut inner = self.inner.write()
                .recover_poison();
            if inner.tx_active {
                inner.new_pages.insert(new_leaf_page, true);
            }
        }

        let mut new_leaf = BTreeNode::new_leaf(new_leaf_page);
        new_leaf.entries = leaf.entries.split_off(mid);
        new_leaf.num_keys = new_leaf.entries.len() as u16;
        new_leaf.next_leaf = leaf.next_leaf;

        leaf.next_leaf = new_leaf_page;
        leaf.num_keys = leaf.entries.len() as u16;

        self.write_node(leaf)?;
        self.write_node(&new_leaf)?;

        let promote_key = new_leaf.entries[0].key.clone();

        if path.len() == 1 {
            return self.create_new_root(leaf.page_num, promote_key, new_leaf_page);
        }

        let parent_page = path[path.len() - 2];
        let mut parent = self.read_node(parent_page)?;

        let old_leaf_page = path[path.len() - 1];
        if old_leaf_page != leaf.page_num {
            for child in parent.children.iter_mut() {
                if *child == old_leaf_page {
                    *child = leaf.page_num;
                    break;
                }
            }
        }

        let mut parent_path = path;
        parent_path.pop();
        self.insert_into_parent(&mut parent, promote_key, new_leaf_page, parent_path)
    }

    fn insert_into_parent(&self, parent: &mut BTreeNode, key: String, right_child: u64, path: Vec<u64>) -> Result<()> {
        let mut idx = parent.keys.len();
        for (i, k) in parent.keys.iter().enumerate() {
            if key < *k {
                idx = i;
                break;
            }
        }

        parent.keys.insert(idx, key);
        parent.children.insert(idx + 1, right_child);
        parent.num_keys = parent.keys.len() as u16;

        if parent.keys.len() > BTREE_ORDER {
            return self.split_internal(parent, path);
        }

        self.write_node(parent)?;
        Ok(())
    }

    fn split_internal(&self, node: &mut BTreeNode, path: Vec<u64>) -> Result<()> {
        let mid = node.keys.len() / 2;

        let new_node_page = self.pager.alloc_page()?;

        {
            let mut inner = self.inner.write()
                .recover_poison();
            if inner.tx_active {
                inner.new_pages.insert(new_node_page, true);
            }
        }

        let mut new_node = BTreeNode::new_internal(new_node_page);
        let promote_key = node.keys[mid].clone();

        new_node.keys = node.keys.split_off(mid + 1);
        node.keys.pop();
        new_node.children = node.children.split_off(mid + 1);

        node.num_keys = node.keys.len() as u16;
        new_node.num_keys = new_node.keys.len() as u16;

        self.write_node(node)?;
        self.write_node(&new_node)?;

        if path.len() == 1 {
            return self.create_new_root(node.page_num, promote_key, new_node_page);
        }

        let parent_page = path[path.len() - 2];
        let mut parent = self.read_node(parent_page)?;

        let old_node_page = path[path.len() - 1];
        if old_node_page != node.page_num {
            for child in parent.children.iter_mut() {
                if *child == old_node_page {
                    *child = node.page_num;
                    break;
                }
            }
        }

        let mut parent_path = path;
        parent_path.pop();
        self.insert_into_parent(&mut parent, promote_key, new_node_page, parent_path)
    }

    fn create_new_root(&self, left_child: u64, key: String, right_child: u64) -> Result<()> {
        let new_root_page = self.pager.alloc_page()?;

        {
            let mut inner = self.inner.write()
                .recover_poison();
            if inner.tx_active {
                inner.new_pages.insert(new_root_page, true);
            }
        }

        let mut new_root = BTreeNode::new_internal(new_root_page);
        new_root.num_keys = 1;
        new_root.keys = vec![key];
        new_root.children = vec![left_child, right_child];

        self.write_node(&new_root)?;

        {
            let mut inner = self.inner.write()
                .recover_poison();
            if inner.tx_active {
                inner.modified_root = new_root_page;
            } else {
                inner.root_page = new_root_page;
            }
        }

        Ok(())
    }

    fn update_path_after_modification(&self, _modified_page: u64, path: &[u64]) -> Result<()> {
        if path.len() <= 1 {
            return Ok(());
        }

        for i in (0..path.len() - 1).rev() {
            let parent_page = path[i];
            let mut parent = self.read_node(parent_page)?;

            let old_child_page = path[i + 1];

            let new_child_page = {
                let inner = self.inner.read()
                    .recover_poison();
                inner.cow_pages.get(&old_child_page).copied()
            };

            if let Some(new_child_page) = new_child_page {
                let mut updated = false;
                for child in parent.children.iter_mut() {
                    if *child == old_child_page {
                        *child = new_child_page;
                        updated = true;
                        break;
                    }
                }

                if updated {
                    self.write_node(&parent)?;
                }
            }
        }

        Ok(())
    }

    fn read_node(&self, page_num: u64) -> Result<BTreeNode> {
        let actual_page = {
            let inner = self.inner.read()
                .recover_poison();
            if inner.tx_active {
                inner.cow_pages.get(&page_num).copied().unwrap_or(page_num)
            } else {
                page_num
            }
        };

        let data = self.pager.read_page(actual_page)?;
        deserialize_node(actual_page, &data)
    }

    fn write_node(&self, node: &BTreeNode) -> Result<()> {
        let mut page_num = node.page_num;
        let original_page = node.page_num;

        let root_page = self.get_root_page();

        {
            let mut inner = self.inner.write()
                .recover_poison();
            if inner.tx_active {
                let is_new = inner.new_pages.contains_key(&page_num);

                if is_new {
                    page_num = node.page_num;
                } else {
                    if let Some(&existing_cow) = inner.cow_pages.get(&page_num) {
                        page_num = existing_cow;
                    } else {
                        let new_page = self.pager.alloc_page()?;
                        inner.cow_pages.insert(page_num, new_page);
                        inner.new_pages.insert(new_page, true);
                        page_num = new_page;

                        if original_page == root_page {
                            inner.modified_root = new_page;
                        }
                    }
                }
            }
        }

        let mut data = get_node_serialize_buffer();
        serialize_node_into(node, &mut data, page_num);
        self.pager.write_page_transfer(page_num, data)?;


        Ok(())
    }
}

pub(crate) fn serialize_node_into(node: &BTreeNode, data: &mut [u8], _page_num: u64) {
    let mut offset = 0;

    data[offset] = node.node_type as u8;
    offset += 1;

    data[offset..offset + 2].copy_from_slice(&node.num_keys.to_le_bytes());
    offset += 2;

    data[offset..offset + 8].copy_from_slice(&node.parent.to_le_bytes());
    offset += 8;

    if node.node_type == NodeType::LeafNode {
        data[offset..offset + 8].copy_from_slice(&node.next_leaf.to_le_bytes());
        offset += 8;

        data[offset..offset + 2].copy_from_slice(&(node.entries.len() as u16).to_le_bytes());
        offset += 2;

        for entry in &node.entries {
            let key_bytes = entry.key.as_bytes();
            data[offset..offset + 2].copy_from_slice(&(key_bytes.len() as u16).to_le_bytes());
            offset += 2;
            data[offset..offset + key_bytes.len()].copy_from_slice(key_bytes);
            offset += key_bytes.len();
            data[offset..offset + 8].copy_from_slice(&entry.value.to_le_bytes());
            offset += 8;
        }
    } else {
        data[offset..offset + 2].copy_from_slice(&(node.children.len() as u16).to_le_bytes());
        offset += 2;

        for &child in &node.children {
            data[offset..offset + 8].copy_from_slice(&child.to_le_bytes());
            offset += 8;
        }

        for key in &node.keys {
            let key_bytes = key.as_bytes();
            data[offset..offset + 2].copy_from_slice(&(key_bytes.len() as u16).to_le_bytes());
            offset += 2;
            data[offset..offset + key_bytes.len()].copy_from_slice(key_bytes);
            offset += key_bytes.len();
        }
    }
}

pub(crate) fn deserialize_node(page_num: u64, data: &[u8]) -> Result<BTreeNode> {
    if data.len() < 11 {
        return Err(Error::Corruption {
            component: "btree".to_string(),
            page_num,
            details: format!("buffer too small: {} bytes", data.len()),
        });
    }

    let mut offset = 0;

    let node_type = NodeType::from_u8(data[offset])?;
    offset += 1;

    let num_keys = u16::from_le_bytes([data[offset], data[offset + 1]]);
    offset += 2;

    let parent = u64::from_le_bytes(data[offset..offset + 8].try_into()
        .map_err(|_| Error::DataCorruption { details: "invalid parent bytes in btree node".to_string() })?);
    offset += 8;

    let mut node = BTreeNode {
        page_num,
        node_type,
        num_keys,
        parent,
        entries: Vec::new(),
        next_leaf: 0,
        keys: Vec::new(),
        children: Vec::new(),
    };

    if node_type == NodeType::LeafNode {
        if offset + 8 > data.len() {
            return Err(Error::Corruption {
                component: "btree".to_string(),
                page_num,
                details: "insufficient data for nextLeaf".to_string(),
            });
        }
        node.next_leaf = u64::from_le_bytes(data[offset..offset + 8].try_into()
            .map_err(|_| Error::DataCorruption { details: "invalid next_leaf bytes in btree leaf node".to_string() })?);
        offset += 8;

        if offset + 2 > data.len() {
            return Err(Error::Corruption {
                component: "btree".to_string(),
                page_num,
                details: "insufficient data for numEntries".to_string(),
            });
        }
        let num_entries = u16::from_le_bytes([data[offset], data[offset + 1]]);
        offset += 2;

        if num_entries > 1000 {
            return Err(Error::Corruption {
                component: "btree".to_string(),
                page_num,
                details: format!("numEntries={} is unreasonably large", num_entries),
            });
        }

        for _ in 0..num_entries {
            if offset + 2 > data.len() {
                return Err(Error::Corruption {
                    component: "btree".to_string(),
                    page_num,
                    details: "insufficient data for entry keyLen".to_string(),
                });
            }
            let key_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2;

            if key_len > 1000 {
                return Err(Error::Corruption {
                    component: "btree".to_string(),
                    page_num,
                    details: format!("keyLen={} is unreasonably large", key_len),
                });
            }

            if offset + key_len > data.len() {
                return Err(Error::Corruption {
                    component: "btree".to_string(),
                    page_num,
                    details: "insufficient data for entry key".to_string(),
                });
            }
            let key = String::from_utf8(data[offset..offset + key_len].to_vec())
                .map_err(|_| Error::InvalidDocument)?;
            offset += key_len;

            if offset + 8 > data.len() {
                return Err(Error::Corruption {
                    component: "btree".to_string(),
                    page_num,
                    details: "insufficient data for entry value".to_string(),
                });
            }
            let value = u64::from_le_bytes(data[offset..offset + 8].try_into()
                .map_err(|_| Error::DataCorruption { details: "invalid value bytes in btree leaf entry".to_string() })?);
            offset += 8;

            node.entries.push(LeafEntry { key, value });
        }
    } else {
        if offset + 2 > data.len() {
            return Err(Error::Corruption {
                component: "btree".to_string(),
                page_num,
                details: "insufficient data for numChildren".to_string(),
            });
        }
        let num_children = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;

        if num_children > 1000 {
            return Err(Error::Corruption {
                component: "btree".to_string(),
                page_num,
                details: format!("numChildren={} is unreasonably large", num_children),
            });
        }

        for _ in 0..num_children {
            if offset + 8 > data.len() {
                return Err(Error::Corruption {
                    component: "btree".to_string(),
                    page_num,
                    details: "insufficient data for child".to_string(),
                });
            }
            let child = u64::from_le_bytes(data[offset..offset + 8].try_into()
                .map_err(|_| Error::DataCorruption { details: "invalid child bytes in btree internal node".to_string() })?);
            offset += 8;
            node.children.push(child);
        }

        for _ in 0..num_keys {
            if offset + 2 > data.len() {
                return Err(Error::Corruption {
                    component: "btree".to_string(),
                    page_num,
                    details: "insufficient data for key keyLen".to_string(),
                });
            }
            let key_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2;

            if key_len > 1000 {
                return Err(Error::Corruption {
                    component: "btree".to_string(),
                    page_num,
                    details: format!("key keyLen={} is unreasonably large", key_len),
                });
            }

            if offset + key_len > data.len() {
                return Err(Error::Corruption {
                    component: "btree".to_string(),
                    page_num,
                    details: "insufficient data for key".to_string(),
                });
            }
            let key = String::from_utf8(data[offset..offset + key_len].to_vec())
                .map_err(|_| Error::InvalidDocument)?;
            offset += key_len;
            node.keys.push(key);
        }

        if num_keys > 0 && node.children.len() != num_keys as usize + 1 {
            return Err(Error::Corruption {
                component: "btree".to_string(),
                page_num,
                details: format!(
                    "has {} keys but {} children (expected {})",
                    num_keys,
                    node.children.len(),
                    num_keys + 1
                ),
            });
        }
    }

    Ok(node)
}

pub struct BTreeIterator<'a> {
    bt: &'a BTree,
    current_leaf: Option<BTreeNode>,
    index: usize,
    started: bool,
}

impl<'a> BTreeIterator<'a> {
    pub fn next(&mut self) -> bool {
        if self.current_leaf.is_none() {
            return false;
        }

        if !self.started {
            self.started = true;
            let entries_len = self.current_leaf.as_ref().unwrap().entries.len();
            return entries_len > 0;
        }

        self.index += 1;

        let current = self.current_leaf.as_ref().unwrap();
        if self.index >= current.entries.len() {
            if current.next_leaf == 0 {
                return false;
            }

            match self.bt.read_node(current.next_leaf) {
                Ok(next_leaf) => {
                    self.current_leaf = Some(next_leaf);
                    self.index = 0;
                }
                Err(_) => return false,
            }
        }

        self.index < self.current_leaf.as_ref().unwrap().entries.len()
    }

    pub fn entry(&self) -> (&str, u64) {
        let current = self.current_leaf.as_ref().unwrap();
        if self.index >= current.entries.len() {
            return ("", 0);
        }
        let entry = &current.entries[self.index];
        (&entry.key, entry.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Arc;

    #[test]
    fn test_btree_create() {
        let path = "/tmp/test_btree_create.db";
        let _ = fs::remove_file(path);

        let pager = Arc::new(Pager::open(path, 100, 0o644, false).unwrap());
        let bt = BTree::new(pager).unwrap();

        assert_eq!(bt.count().unwrap(), 0);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_btree_insert_search() {
        let path = "/tmp/test_btree_insert.db";
        let _ = fs::remove_file(path);

        let pager = Arc::new(Pager::open(path, 100, 0o644, false).unwrap());
        let bt = BTree::new(pager).unwrap();

        bt.insert("doc1", 100).unwrap();
        bt.insert("doc2", 200).unwrap();
        bt.insert("doc3", 300).unwrap();

        assert_eq!(bt.search("doc1").unwrap(), 100);
        assert_eq!(bt.search("doc2").unwrap(), 200);
        assert_eq!(bt.search("doc3").unwrap(), 300);
        assert!(bt.search("doc4").is_err());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_btree_update() {
        let path = "/tmp/test_btree_update.db";
        let _ = fs::remove_file(path);

        let pager = Arc::new(Pager::open(path, 100, 0o644, false).unwrap());
        let bt = BTree::new(pager).unwrap();

        bt.insert("doc1", 100).unwrap();
        assert_eq!(bt.search("doc1").unwrap(), 100);

        bt.insert("doc1", 999).unwrap();
        assert_eq!(bt.search("doc1").unwrap(), 999);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_btree_delete() {
        let path = "/tmp/test_btree_delete.db";
        let _ = fs::remove_file(path);

        let pager = Arc::new(Pager::open(path, 100, 0o644, false).unwrap());
        let bt = BTree::new(pager).unwrap();

        bt.insert("doc1", 100).unwrap();
        bt.insert("doc2", 200).unwrap();

        assert_eq!(bt.count().unwrap(), 2);

        bt.delete("doc1").unwrap();
        assert!(bt.search("doc1").is_err());
        assert_eq!(bt.search("doc2").unwrap(), 200);
        assert_eq!(bt.count().unwrap(), 1);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_btree_iterator() {
        let path = "/tmp/test_btree_iterator.db";
        let _ = fs::remove_file(path);

        let pager = Arc::new(Pager::open(path, 100, 0o644, false).unwrap());
        let bt = BTree::new(pager).unwrap();

        bt.insert("doc1", 100).unwrap();
        bt.insert("doc3", 300).unwrap();
        bt.insert("doc2", 200).unwrap();

        let mut iter = bt.iterator().unwrap();
        let mut entries: Vec<(String, u64)> = Vec::new();
        while iter.next() {
            let (key, value) = iter.entry();
            entries.push((key.to_string(), value));
        }

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], ("doc1".to_string(), 100));
        assert_eq!(entries[1], ("doc2".to_string(), 200));
        assert_eq!(entries[2], ("doc3".to_string(), 300));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn test_btree_large_insert() {
        let path = "/tmp/test_btree_large.db";
        let _ = fs::remove_file(path);

        let pager = Arc::new(Pager::open(path, 1000, 0o644, false).unwrap());
        let bt = BTree::new(pager).unwrap();

        for i in 0..100 {
            let doc_id = format!("doc{:04}", i);
            bt.insert(&doc_id, i as u64).unwrap();
        }

        assert_eq!(bt.count().unwrap(), 100);

        for i in 0..100 {
            let doc_id = format!("doc{:04}", i);
            assert_eq!(bt.search(&doc_id).unwrap(), i as u64);
        }

        let _ = fs::remove_file(path);
    }
}
