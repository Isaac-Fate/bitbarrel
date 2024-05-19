pub mod btree;

use bytes::Bytes;
use crate::data::LogRecordPos;

/// A trait for the index.
pub trait Indexer {
    /// Puts the log record associated with the key into the index.
    fn put(&self, key: Bytes, pos: LogRecordPos) -> bool;

    // Gets the log record associated with the key from the index.
    fn get(&self, key: Bytes) -> Option<LogRecordPos>;

    // Deletes the log record associated with the key from the index.
    fn delete(&self, key: Bytes) -> bool;
}
