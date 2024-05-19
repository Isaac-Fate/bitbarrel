use std::{ collections::BTreeMap, sync::Arc };
use bytes::Bytes;
use parking_lot::RwLock;

use crate::data::LogRecordPos;
use super::Indexer;

/// A B-Tree implementation for the index,
/// which mainly encapsulates the `BTreeMap`.
pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Bytes, LogRecordPos>>>,
}

impl BTree {
    pub fn new() -> Self {
        Self { tree: Arc::new(RwLock::new(BTreeMap::new())) }
    }
}

impl Indexer for BTree {
    fn put(&self, key: Bytes, pos: LogRecordPos) -> bool {
        // Aquiring write lock
        let mut write_guard = self.tree.write();

        // Insert into the index
        write_guard.insert(key, pos);

        // Awalys return true
        // since we do not care whether this operation
        // inserts or updates data
        true
    }

    fn get(&self, key: Bytes) -> Option<LogRecordPos> {
        // Aquiring read lock
        let read_guard = self.tree.read();

        // Get the log record
        read_guard.get(&key).copied()
    }

    fn delete(&self, key: Bytes) -> bool {
        // Aquiring write lock
        let mut write_guard = self.tree.write();

        // Delete from the index
        // Return true if this key exists in the index
        write_guard.remove(&key).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_put() {
        // Create an empty BTree
        let btree = BTree::new();

        // Put the key and position pairs
        assert!(btree.put(Bytes::from(vec![1, 2, 3]), LogRecordPos { file_id: 0, offest: 42 }));
        assert!(btree.put(Bytes::from("hello"), LogRecordPos { file_id: 1, offest: 16 }));
    }

    #[test]
    fn test_btree_get() {
        // Create an empty BTree
        let btree = BTree::new();

        // Put the key and position pairs
        assert!(btree.put(Bytes::from(vec![1, 2, 3]), LogRecordPos { file_id: 0, offest: 42 }));
        assert!(btree.put(Bytes::from("hello"), LogRecordPos { file_id: 1, offest: 16 }));

        // Get the positions
        assert_eq!(
            btree.get(Bytes::from(vec![1, 2, 3])),
            Some(LogRecordPos { file_id: 0, offest: 42 })
        );
        assert_eq!(btree.get(Bytes::from("hello")), Some(LogRecordPos { file_id: 1, offest: 16 }));

        // The keys do not exist
        assert_eq!(btree.get(Bytes::from(vec![1, 2])), None);
        assert_eq!(btree.get(Bytes::from("world")), None);
    }

    #[test]
    fn test_btree_delete() {
        // Create an empty BTree
        let btree = BTree::new();

        // Put the key and position pairs
        assert!(btree.put(Bytes::from(vec![1, 2, 3]), LogRecordPos { file_id: 0, offest: 42 }));
        assert!(btree.put(Bytes::from("hello"), LogRecordPos { file_id: 1, offest: 16 }));

        // Delete a key
        assert!(btree.delete(Bytes::from(vec![1, 2, 3])));

        // Delete non-existent keys
        assert!(!btree.delete(Bytes::from(vec![1, 2])));
        assert!(!btree.delete(Bytes::from("world")));
    }
}
