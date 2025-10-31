use crate::core::errors::*;
use crate::core::query::parser::parse_query;
use serde_json::Value;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::sync::{Arc, RwLock};
use std::collections::HashMap;

/// Type of change operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeOperation {
    /// A document was inserted
    Insert,
    /// A document was updated
    Update,
    /// A document was deleted
    Delete,
}

/// Event emitted when a document changes
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    /// The collection name where the change occurred
    pub collection: String,
    /// The type of operation
    pub operation: ChangeOperation,
    /// The document ID
    pub doc_id: String,
    /// The document data (None for Delete operations)
    pub document: Option<Value>,
}

/// Internal watcher structure
pub(crate) struct Watcher {
    pub(crate) id: String,
    pub(crate) sender: Sender<ChangeEvent>,
    pub(crate) filter: Option<String>,
}

impl Watcher {
    /// Check if this watcher should receive the event based on its filter
    pub(crate) fn matches(&self, event: &ChangeEvent) -> bool {
        if let Some(filter) = &self.filter {
            if let Some(doc) = &event.document {
                // Parse and evaluate the filter query
                if let Ok(ast) = parse_query(filter) {
                    if let Some(doc_obj) = doc.as_object() {
                        return ast.eval(doc_obj);
                    }
                }
                // If parsing or evaluation fails, don't send the event
                return false;
            }
            // Delete operations with filters don't match (no document to filter)
            return false;
        }
        // No filter means match everything
        true
    }

    /// Send an event to this watcher
    pub(crate) fn send(&self, event: ChangeEvent) -> bool {
        self.sender.send(event).is_ok()
    }
}

/// Storage for all watchers
pub(crate) type WatcherStorage = Arc<RwLock<HashMap<String, Vec<Watcher>>>>;

/// Create a new watcher storage
pub(crate) fn new_watcher_storage() -> WatcherStorage {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Builder for creating a watcher
pub struct WatchBuilder<'a> {
    collection: &'a str,
    storage: WatcherStorage,
    filter: Option<String>,
}

impl<'a> WatchBuilder<'a> {
    pub(crate) fn new(collection: &'a str, storage: WatcherStorage) -> Self {
        Self {
            collection,
            storage,
            filter: None,
        }
    }

    /// Add a filter to the watcher (only events matching this query will be sent)
    ///
    /// # Example
    /// ```no_run
    /// use jasonisnthappy::Database;
    ///
    /// # fn main() -> jasonisnthappy::Result<()> {
    /// let db = Database::open("my.db")?;
    /// let collection = db.collection("users");
    /// let (_handle, _rx) = collection.watch()
    ///     .filter("age > 18 and city is \"NYC\"")
    ///     .subscribe()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn filter(mut self, query: &str) -> Self {
        self.filter = Some(query.to_string());
        self
    }

    /// Subscribe to changes and return a channel receiver
    ///
    /// # Returns
    /// A tuple of (WatchHandle, Receiver) where:
    /// - WatchHandle: Automatically unsubscribes when dropped
    /// - Receiver: Channel to receive change events
    pub fn subscribe(self) -> Result<(WatchHandle, Receiver<ChangeEvent>)> {
        let (sender, receiver) = channel();
        let watcher_id = generate_watcher_id();

        let watcher = Watcher {
            id: watcher_id.clone(),
            sender,
            filter: self.filter,
        };

        // Add watcher to storage
        {
            let mut storage = self.storage.write()
                .map_err(|_| Error::LockPoisoned { lock_name: "watcher_storage".to_string() })?;

            storage.entry(self.collection.to_string())
                .or_insert_with(Vec::new)
                .push(watcher);
        }

        let handle = WatchHandle {
            collection: self.collection.to_string(),
            watcher_id,
            storage: self.storage.clone(),
        };

        Ok((handle, receiver))
    }
}

/// Handle returned when subscribing to changes
/// Automatically unsubscribes when dropped (RAII pattern)
pub struct WatchHandle {
    collection: String,
    watcher_id: String,
    storage: WatcherStorage,
}

impl Drop for WatchHandle {
    fn drop(&mut self) {
        // Remove watcher from storage
        if let Ok(mut storage) = self.storage.write() {
            if let Some(watchers) = storage.get_mut(&self.collection) {
                watchers.retain(|w| w.id != self.watcher_id);
                // Clean up empty collections
                if watchers.is_empty() {
                    storage.remove(&self.collection);
                }
            }
        }
    }
}

impl WatchHandle {
    /// Get the watcher ID
    pub fn id(&self) -> &str {
        &self.watcher_id
    }

    /// Get the collection being watched
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// Manually unsubscribe (equivalent to dropping the handle)
    pub fn unsubscribe(self) {
        drop(self);
    }
}

/// Emit a change event to all watchers of a collection
pub(crate) fn emit_change(
    storage: &WatcherStorage,
    collection: &str,
    operation: ChangeOperation,
    doc_id: &str,
    document: Option<Value>,
) {
    let event = ChangeEvent {
        collection: collection.to_string(),
        operation,
        doc_id: doc_id.to_string(),
        document,
    };

    // Send to all matching watchers and collect dead ones
    let mut dead_watchers = Vec::new();

    {
        // Read lock scope
        if let Ok(storage) = storage.read() {
            if let Some(watchers) = storage.get(collection) {
                for watcher in watchers {
                    if watcher.matches(&event) {
                        if !watcher.send(event.clone()) {
                            // Channel closed, mark for removal
                            dead_watchers.push(watcher.id.clone());
                        }
                    }
                }
            }
        }
    } // Read lock released here

    // Clean up dead watchers
    if !dead_watchers.is_empty() {
        if let Ok(mut storage) = storage.write() {
            if let Some(watchers) = storage.get_mut(collection) {
                watchers.retain(|w| !dead_watchers.contains(&w.id));
                if watchers.is_empty() {
                    storage.remove(collection);
                }
            }
        }
    }
}

fn generate_watcher_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hash, Hasher};

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let random_state = RandomState::new();
    let mut hasher = random_state.build_hasher();
    timestamp.hash(&mut hasher);
    let random_part = hasher.finish();

    format!("watch_{}_{:x}", timestamp, random_part)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_watcher_matches_no_filter() {
        let (tx, _rx) = channel();
        let watcher = Watcher {
            id: "test".to_string(),
            sender: tx,
            filter: None,
        };

        let event = ChangeEvent {
            collection: "users".to_string(),
            operation: ChangeOperation::Insert,
            doc_id: "1".to_string(),
            document: Some(json!({"name": "Alice", "age": 30})),
        };

        assert!(watcher.matches(&event));
    }

    #[test]
    fn test_watcher_matches_with_filter() {
        let (tx, _rx) = channel();
        let watcher = Watcher {
            id: "test".to_string(),
            sender: tx,
            filter: Some("age > 25".to_string()),
        };

        let event1 = ChangeEvent {
            collection: "users".to_string(),
            operation: ChangeOperation::Insert,
            doc_id: "1".to_string(),
            document: Some(json!({"name": "Alice", "age": 30})),
        };

        let event2 = ChangeEvent {
            collection: "users".to_string(),
            operation: ChangeOperation::Insert,
            doc_id: "2".to_string(),
            document: Some(json!({"name": "Bob", "age": 20})),
        };

        assert!(watcher.matches(&event1));
        assert!(!watcher.matches(&event2));
    }

    #[test]
    fn test_watcher_matches_delete_with_filter() {
        let (tx, _rx) = channel();
        let watcher = Watcher {
            id: "test".to_string(),
            sender: tx,
            filter: Some("age > 25".to_string()),
        };

        let event = ChangeEvent {
            collection: "users".to_string(),
            operation: ChangeOperation::Delete,
            doc_id: "1".to_string(),
            document: None,
        };

        // Delete events with filters don't match (no document to filter)
        assert!(!watcher.matches(&event));
    }

    #[test]
    fn test_emit_change() {
        let storage = new_watcher_storage();
        let (tx, rx) = channel();

        let watcher = Watcher {
            id: "test".to_string(),
            sender: tx,
            filter: None,
        };

        // Add watcher to storage
        {
            let mut s = storage.write().recover_poison();
            s.insert("users".to_string(), vec![watcher]);
        }

        // Emit an event
        emit_change(
            &storage,
            "users",
            ChangeOperation::Insert,
            "1",
            Some(json!({"name": "Alice"})),
        );

        // Verify event received
        let event = rx.recv().unwrap();
        assert_eq!(event.collection, "users");
        assert_eq!(event.operation, ChangeOperation::Insert);
        assert_eq!(event.doc_id, "1");
        assert!(event.document.is_some());
    }

    #[test]
    fn test_watch_handle_auto_cleanup() {
        let storage = new_watcher_storage();
        let (tx, _rx) = channel();

        let watcher = Watcher {
            id: "test".to_string(),
            sender: tx,
            filter: None,
        };

        // Add watcher to storage
        {
            let mut s = storage.write().recover_poison();
            s.insert("users".to_string(), vec![watcher]);
        }

        // Create handle
        let handle = WatchHandle {
            collection: "users".to_string(),
            watcher_id: "test".to_string(),
            storage: storage.clone(),
        };

        // Verify watcher exists
        {
            let s = storage.read().recover_poison();
            assert_eq!(s.get("users").unwrap().len(), 1);
        }

        // Drop handle
        drop(handle);

        // Verify watcher was removed
        {
            let s = storage.read().recover_poison();
            assert!(s.get("users").is_none());
        }
    }

    #[test]
    fn test_generate_watcher_id() {
        let id1 = generate_watcher_id();
        let id2 = generate_watcher_id();

        assert!(!id1.is_empty());
        assert!(!id2.is_empty());
        assert_ne!(id1, id2);
        assert!(id1.starts_with("watch_"));
    }
}
