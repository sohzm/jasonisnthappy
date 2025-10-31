
use crate::core::btree::{BTreeNode, NodeType, LeafEntry, serialize_node_into, deserialize_node};
use crate::core::constants::*;
use crate::core::errors::*;
use crate::core::pager::Pager;
use crate::core::buffer_pool::get_node_serialize_buffer;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct TxBTree {
    snapshot_root: PageNum,

    modified_root: PageNum,

    cow_pages: HashMap<PageNum, PageNum>,

    new_pages: HashMap<PageNum, bool>,

    pager: Arc<Pager>,

    tx_writes: Arc<RwLock<HashMap<PageNum, Vec<u8>>>>,
}

impl TxBTree {
    pub fn new(
        pager: Arc<Pager>,
        snapshot_root: PageNum,
        tx_writes: Arc<RwLock<HashMap<PageNum, Vec<u8>>>>,
    ) -> Self {
        Self {
            snapshot_root,
            modified_root: 0,
            cow_pages: HashMap::new(),
            new_pages: HashMap::new(),
            pager,
            tx_writes,
        }
    }

    pub fn create_empty(
        pager: Arc<Pager>,
        tx_writes: Arc<RwLock<HashMap<PageNum, Vec<u8>>>>,
    ) -> Result<Self> {
        let root_page = pager.alloc_page()?;

        let mut tb = Self {
            snapshot_root: root_page,
            modified_root: 0,
            cow_pages: HashMap::new(),
            new_pages: HashMap::new(),
            pager,
            tx_writes,
        };

        tb.new_pages.insert(root_page, true);

        let root = BTreeNode::new_leaf(root_page);
        let _ = tb.write_node(&root)?;

        Ok(tb)
    }

    pub fn get_current_root(&self) -> PageNum {
        if self.modified_root != 0 {
            self.modified_root
        } else {
            self.snapshot_root
        }
    }

    pub fn get_cow_pages(&self) -> &HashMap<PageNum, PageNum> {
        &self.cow_pages
    }

    pub fn get_new_pages(&self) -> &HashMap<PageNum, bool> {
        &self.new_pages
    }

    pub fn search(&self, doc_id: &str) -> Result<PageNum> {
        let root_page = self.get_current_root();
        let mut node = self.read_node(root_page)?;

        while node.node_type == NodeType::InternalNode {
            let child_page = self.find_child(&node, doc_id)?;
            node = self.read_node(child_page)?;
        }

        for entry in &node.entries {
            if entry.key == doc_id {
                return Ok(entry.value);
            }
        }

        Err(Error::DocumentNotFound {
            collection: "".to_string(),
            id: doc_id.to_string(),
        })
    }

    pub fn insert(&mut self, doc_id: &str, page_num: PageNum) -> Result<()> {
        let root_page = self.get_current_root();

        let node = self.read_node(root_page)?;

        let (mut leaf, path) = self.find_leaf(node, doc_id, vec![root_page])?;

        let new_entry = LeafEntry {
            key: doc_id.to_string(),
            value: page_num,
        };

        let mut inserted = false;
        for (i, entry) in leaf.entries.iter_mut().enumerate() {
            match entry.key.as_str().cmp(doc_id) {
                std::cmp::Ordering::Equal => {
                    entry.value = page_num;
                    inserted = true;
                    break;
                }
                std::cmp::Ordering::Greater => {
                    leaf.entries.insert(i, new_entry.clone());
                    inserted = true;
                    break;
                }
                std::cmp::Ordering::Less => continue,
            }
        }

        if !inserted {
            leaf.entries.push(new_entry);
        }

        leaf.num_keys = leaf.entries.len() as u16;

        if leaf.entries.len() > BTREE_ORDER {
            return self.split_leaf(leaf, path);
        }

        if leaf.next_leaf != 0 {
            let root = self.read_node(self.get_current_root())?;
            if root.node_type == NodeType::InternalNode {
                let next_leaf_in_tree = self.is_page_in_tree(&root, leaf.next_leaf);
                if !next_leaf_in_tree {
                    if let Some(correct_next) = self.find_correct_next_leaf(&root, &leaf)? {
                        leaf.next_leaf = correct_next;
                    }
                }
            }
        }

        let original_leaf_page = leaf.page_num;
        let actual_page = self.write_node(&leaf)?;

        if actual_page != original_leaf_page {
            // Update predecessor's next_leaf pointer to point to the new COW page
            // This prevents the iterator from traversing to the old page
            let _ = self.update_predecessor_next_leaf(original_leaf_page, actual_page);
        }

        self.update_path_after_modification(leaf.page_num, path)
    }

    /// Updates the page_num for an existing key without delete+insert
    pub fn update(&mut self, doc_id: &str, new_page_num: PageNum) -> Result<()> {
        let root_page = self.get_current_root();
        let node = self.read_node(root_page)?;

        let (mut leaf, path) = self.find_leaf(node, doc_id, vec![root_page])?;

        // Find and update the entry
        let mut found = false;
        for entry in &mut leaf.entries {
            if entry.key == doc_id {
                entry.value = new_page_num;
                found = true;
                break;
            }
        }

        if !found {
            return Err(Error::DocumentNotFound {
                collection: "".to_string(),
                id: doc_id.to_string(),
            });
        }

        let _ = self.write_node(&leaf)?;

        self.update_path_after_modification(leaf.page_num, path)
    }

    pub fn delete(&mut self, doc_id: &str) -> Result<()> {
        let root_page = self.get_current_root();
        let node = self.read_node(root_page)?;

        let (mut leaf, path) = self.find_leaf(node, doc_id, vec![root_page])?;

        let original_len = leaf.entries.len();
        leaf.entries.retain(|e| e.key != doc_id);

        if leaf.entries.len() == original_len {
            return Err(Error::DocumentNotFound {
                collection: "".to_string(),
                id: doc_id.to_string(),
            });
        }

        leaf.num_keys = leaf.entries.len() as u16;
        let _ = self.write_node(&leaf)?;

        self.update_path_after_modification(leaf.page_num, path)
    }

    pub fn has_prefix(&self, prefix: &str) -> Result<bool> {
        let root_page = self.get_current_root();
        self.has_prefix_in_node(root_page, prefix)
    }

    pub fn iterator(&self) -> Result<TxBTreeIterator<'_>> {
        let root_page = self.get_current_root();
        let mut node = self.read_node(root_page)?;

        while node.node_type == NodeType::InternalNode {
            if node.children.is_empty() {
                return Err(Error::Other("internal node has no children".to_string()));
            }
            let child_page = node.children[0];
            node = self.read_node(child_page)?;
        }

        Ok(TxBTreeIterator {
            btree: self,
            current_leaf: Some(node),
            index: 0,
            started: false,
        })
    }

    fn has_prefix_in_node(&self, page_num: PageNum, prefix: &str) -> Result<bool> {
        if page_num == 0 {
            return Ok(false);
        }

        let node = self.read_node(page_num)?;

        if node.node_type == NodeType::LeafNode {
            for entry in &node.entries {
                if entry.key.starts_with(prefix) {
                    return Ok(true);
                }
            }
            return Ok(false);
        }

        for child_page in &node.children {
            if self.has_prefix_in_node(*child_page, prefix)? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn read_node(&self, page_num: PageNum) -> Result<BTreeNode> {
        let actual_page = self.cow_pages.get(&page_num).copied().unwrap_or(page_num);

        let data = {
            let writes = self.tx_writes.read()
                .map_err(|_| Error::LockPoisoned { lock_name: "tx_btree.tx_writes".to_string() })?;
            if let Some(page_data) = writes.get(&actual_page) {
                page_data.clone()
            } else {
                drop(writes);
                self.pager.read_page(actual_page)?
            }
        };

        deserialize_node(actual_page, &data)
    }

    fn write_node(&mut self, node: &BTreeNode) -> Result<PageNum> {
        let original_page_num = node.page_num;
        let mut page_num = node.page_num;
        let mut node_to_write = node.clone();

        if !self.new_pages.contains_key(&page_num) {
            if let Some(&existing_cow) = self.cow_pages.get(&page_num) {
                page_num = existing_cow;
                node_to_write.page_num = page_num;
            } else {
                let new_page = if original_page_num == self.get_current_root() {
                    let current_root = self.get_current_root();
                    self.pager.alloc_page_minimum(current_root)?
                } else {
                    self.pager.alloc_page()?
                };

                self.cow_pages.insert(page_num, new_page);
                self.new_pages.insert(new_page, true);
                page_num = new_page;
                node_to_write.page_num = new_page;

                if original_page_num == self.get_current_root() {
                    self.modified_root = new_page;
                }
            }
        }

        let mut data = get_node_serialize_buffer();
        serialize_node_into(&node_to_write, &mut data, page_num);

        {
            let mut writes = self.tx_writes.write()
                .map_err(|_| Error::LockPoisoned { lock_name: "tx_btree.tx_writes".to_string() })?;
            writes.insert(page_num, data.clone());
        }

        // Write to pager cache as well for immediate availability
        // This is safe for MVCC because:
        // 1. Pager cache is in-memory and process-local
        // 2. Changes don't persist until WAL sync + flush during commit
        // 3. Conflict detection reads committed state before commit
        self.pager.write_page_transfer(page_num, data)?;

        Ok(page_num)
    }

    fn find_child(&self, node: &BTreeNode, key: &str) -> Result<PageNum> {
        for (i, node_key) in node.keys.iter().enumerate() {
            if key < node_key.as_str() {
                return Ok(node.children[i]);
            }
        }
        Ok(*node.children.last().unwrap())
    }

    fn find_leaf(
        &self,
        mut node: BTreeNode,
        key: &str,
        mut path: Vec<PageNum>,
    ) -> Result<(BTreeNode, Vec<PageNum>)> {
        while node.node_type == NodeType::InternalNode {
            let child_page = self.find_child(&node, key)?;
            path.push(child_page);
            node = self.read_node(child_page)?;
        }

        Ok((node, path))
    }

    fn split_leaf(&mut self, mut leaf: BTreeNode, path: Vec<PageNum>) -> Result<()> {
        let mid = leaf.entries.len() / 2;

        let right_page = self.pager.alloc_page()?;
        self.new_pages.insert(right_page, true);

        let next_page = if leaf.next_leaf != 0 {
            self.cow_pages.get(&leaf.next_leaf).copied().unwrap_or(leaf.next_leaf)
        } else {
            0
        };

        let mut right = BTreeNode::new_leaf(right_page);
        right.entries = leaf.entries.split_off(mid);
        right.num_keys = right.entries.len() as u16;
        right.next_leaf = next_page;

        leaf.num_keys = leaf.entries.len() as u16;
        leaf.next_leaf = right_page;

        let split_key = right.entries[0].key.clone();

        let actual_left_page = self.write_node(&leaf)?;
        let actual_right_page = self.write_node(&right)?;

        if path.len() == 1 {
            self.create_new_root(actual_left_page, actual_right_page, split_key)?;
        } else {
            let parent_page = path[path.len() - 2];
            let mut parent = self.read_node(parent_page)?;

            let old_leaf_page = path[path.len() - 1];
            if old_leaf_page != actual_left_page {
                for child in parent.children.iter_mut() {
                    if *child == old_leaf_page {
                        *child = actual_left_page;
                        break;
                    }
                }
            }

            // Use insert_into_parent which handles internal node splits
            self.insert_into_parent(parent, split_key, actual_right_page, path[..path.len() - 1].to_vec())?;
        }

        Ok(())
    }

    fn create_new_root(&mut self, left: PageNum, right: PageNum, key: String) -> Result<()> {
        let current_root = self.get_current_root();
        let new_root_page = self.pager.alloc_page_minimum(current_root)?;

        self.new_pages.insert(new_root_page, true);

        let mut new_root = BTreeNode::new_internal(new_root_page);
        new_root.keys = vec![key];
        new_root.children = vec![left, right];
        new_root.num_keys = 1;

        let actual_root_page = self.write_node(&new_root)?;
        self.modified_root = actual_root_page;

        Ok(())
    }

    fn insert_into_parent(&mut self, mut parent: BTreeNode, key: String, right_child: PageNum, path: Vec<PageNum>) -> Result<()> {
        // Find insertion position
        let mut insert_pos = parent.keys.len();
        for (i, node_key) in parent.keys.iter().enumerate() {
            if &key < node_key {
                insert_pos = i;
                break;
            }
        }

        // Insert key and child pointer
        parent.keys.insert(insert_pos, key);
        parent.children.insert(insert_pos + 1, right_child);
        parent.num_keys = parent.keys.len() as u16;

        // Check if parent needs to split
        if parent.keys.len() > BTREE_ORDER {
            return self.split_internal(parent, path);
        }

        let _ = self.write_node(&parent)?;

        self.update_path_after_modification(parent.page_num, path)?;

        Ok(())
    }

    fn split_internal(&mut self, mut node: BTreeNode, path: Vec<PageNum>) -> Result<()> {
        let mid = node.keys.len() / 2;

        // Allocate new node for right half
        let new_node_page = self.pager.alloc_page()?;
        self.new_pages.insert(new_node_page, true);

        // The key at mid is promoted to parent
        let promote_key = node.keys[mid].clone();

        // Create new node with right half (keys after mid)
        let mut new_node = BTreeNode::new_internal(new_node_page);
        new_node.keys = node.keys.split_off(mid + 1); // Keys after mid
        new_node.children = node.children.split_off(mid + 1); // Children after mid
        new_node.num_keys = new_node.keys.len() as u16;

        // Remove the promoted key from original node
        node.keys.pop(); // Remove the key at mid (now at end after split_off)
        node.num_keys = node.keys.len() as u16;

        // Write both nodes
        let _ = self.write_node(&node)?;
        let _ = self.write_node(&new_node)?;

        // If splitting root, create new root
        if path.len() == 1 {
            return self.create_new_root(node.page_num, new_node.page_num, promote_key);
        }

        // Otherwise, insert promoted key into parent
        let parent_page = path[path.len() - 2];
        let mut parent = self.read_node(parent_page)?;

        // Update parent's child pointer if node was COW'd
        let old_node_page = path[path.len() - 1];
        if old_node_page != node.page_num {
            for child in parent.children.iter_mut() {
                if *child == old_node_page {
                    *child = node.page_num;
                    break;
                }
            }
        }

        // Recursively insert into parent (which may cause further splits)
        self.insert_into_parent(parent, promote_key, new_node.page_num, path[..path.len() - 1].to_vec())
    }

    fn update_path_after_modification(&mut self, _modified_page: PageNum, path: Vec<PageNum>) -> Result<()> {
        if path.len() <= 1 {
            return Ok(());
        }

        for i in (0..path.len() - 1).rev() {
            let parent_page = path[i];
            let mut parent = self.read_node(parent_page)?;

            let old_child_page = path[i + 1];
            let mut updated = false;

            for child_ptr in parent.children.iter_mut() {
                if *child_ptr == old_child_page {
                    if let Some(&new_child_page) = self.cow_pages.get(&old_child_page) {
                        *child_ptr = new_child_page;
                        updated = true;
                        break;
                    }
                }
            }

            if updated {
                let _ = self.write_node(&parent)?;
            }
        }

        Ok(())
    }

    fn update_predecessor_next_leaf(&mut self, old_page: PageNum, new_page: PageNum) -> Result<()> {
        let root_page = self.get_current_root();
        let root = self.read_node(root_page)?;

        let mut current_page = if root.node_type == NodeType::LeafNode {
            root_page
        } else {
            let mut node = root.clone();
            while node.node_type == NodeType::InternalNode {
                let first_child = node.children[0];
                node = self.read_node(first_child)?;
            }
            node.page_num
        };

        loop {
            let mut leaf = self.read_node(current_page)?;

            if leaf.next_leaf == old_page {
                let old_pred_page = leaf.page_num;

                let pred_path = self.find_any_leaf_path(&root, old_pred_page, vec![root_page])
                    .ok_or_else(|| Error::Corruption {
                        component: "BTree".to_string(),
                        page_num: old_pred_page,
                        details: "Could not find predecessor path before COW".to_string()
                    })?;

                leaf.next_leaf = new_page;
                let new_pred_page = self.write_node(&leaf)?;

                if new_pred_page != old_pred_page {
                    self.update_path_after_modification(old_pred_page, pred_path)?;
                }

                return Ok(());
            }

            if leaf.next_leaf == 0 {
                return Ok(());
            }

            current_page = leaf.next_leaf;
        }
    }


    fn find_any_leaf_path(&self, node: &BTreeNode, target_leaf: PageNum, mut path: Vec<PageNum>) -> Option<Vec<PageNum>> {
        if node.node_type == NodeType::LeafNode {
            return if node.page_num == target_leaf {
                Some(path)
            } else {
                None
            };
        }

        for &child_page in &node.children {
            path.push(child_page);
            let child = self.read_node(child_page).ok()?;
            if let Some(result) = self.find_any_leaf_path(&child, target_leaf, path.clone()) {
                return Some(result);
            }
            path.pop();
        }

        None
    }

    fn is_page_in_tree(&self, node: &BTreeNode, target_page: PageNum) -> bool {
        if node.page_num == target_page {
            return true;
        }

        if node.node_type == NodeType::InternalNode {
            for &child_page in &node.children {
                if let Ok(child) = self.read_node(child_page) {
                    if self.is_page_in_tree(&child, target_page) {
                        return true;
                    }
                }
            }
        }

        false
    }

    fn find_correct_next_leaf(&self, root: &BTreeNode, leaf: &BTreeNode) -> Result<Option<PageNum>> {
        if leaf.entries.is_empty() {
            return Ok(None);
        }

        let last_key = &leaf.entries.last().unwrap().key;

        Ok(self.find_next_leaf_by_key(root, last_key))
    }

    fn find_next_leaf_by_key(&self, node: &BTreeNode, key: &str) -> Option<PageNum> {
        if node.node_type == NodeType::LeafNode {
            if let Some(first_entry) = node.entries.first() {
                if first_entry.key.as_str() > key {
                    return Some(node.page_num);
                }
            }
            return None;
        }

        for (i, node_key) in node.keys.iter().enumerate() {
            if key < node_key.as_str() {
                for child_idx in i..node.children.len() {
                    if let Ok(child) = self.read_node(node.children[child_idx]) {
                        if let Some(result) = self.find_next_leaf_by_key(&child, key) {
                            return Some(result);
                        }
                    }
                }
                return None;
            }
        }

        if let Ok(child) = self.read_node(*node.children.last().unwrap()) {
            return self.find_next_leaf_by_key(&child, key);
        }

        None
    }

}

pub struct TxBTreeIterator<'a> {
    btree: &'a TxBTree,
    current_leaf: Option<BTreeNode>,
    index: usize,
    started: bool,
}

impl<'a> TxBTreeIterator<'a> {
    pub fn next(&mut self) -> bool {
        if self.current_leaf.is_none() {
            return false;
        }

        if !self.started {
            self.started = true;
            return self.current_leaf.as_ref().unwrap().entries.len() > 0;
        }

        self.index += 1;

        let current = self.current_leaf.as_ref().unwrap();
        if self.index >= current.entries.len() {
            let next_leaf_result = if current.next_leaf != 0 {
                self.btree.read_node(current.next_leaf).ok()
            } else {
                None
            };

            let next_leaf = if let Some(leaf) = next_leaf_result {
                let root = match self.btree.read_node(self.btree.get_current_root()) {
                    Ok(r) => r,
                    Err(_) => return false,
                };

                if self.btree.is_page_in_tree(&root, leaf.page_num) {
                    Some(leaf)
                } else {
                    if let Some(last_entry) = current.entries.last() {
                        self.btree.find_next_leaf_by_key(&root, &last_entry.key)
                            .and_then(|page| self.btree.read_node(page).ok())
                    } else {
                        None
                    }
                }
            } else {
                let root = match self.btree.read_node(self.btree.get_current_root()) {
                    Ok(r) => r,
                    Err(_) => return false,
                };

                if let Some(last_entry) = current.entries.last() {
                    self.btree.find_next_leaf_by_key(&root, &last_entry.key)
                        .and_then(|page| self.btree.read_node(page).ok())
                } else {
                    None
                }
            };

            if let Some(next) = next_leaf {
                self.current_leaf = Some(next);
                self.index = 0;
            } else {
                return false;
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
