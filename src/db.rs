use std::{ collections::HashMap, path::PathBuf, sync::Arc };

use bytes::Bytes;
use parking_lot::RwLock;
use crate::{
    data::{ DataFile, LogRecord, LogRecordPos, LogRecordStatus },
    index::Indexer,
    Error,
    Result,
};

/// Bitcast database engine.
pub struct Engine {
    config: Arc<Config>,

    /// The active file that is being written to.
    active_file: Arc<RwLock<DataFile>>,

    /// Old data files.
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,

    /// In-memory index.
    index: Box<dyn Indexer>,
}

impl Engine {
    /// Stores the given key and value in the database.
    /// The key cannot be empty.
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        // Check if the key is empty
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        // Create a new log record
        let log_record = LogRecord {
            key,
            value,
            status: LogRecordStatus::Normal,
        };

        // Append the log record
        let log_record_pos = self.append_log_record(&log_record)?;

        // Update the index
        let index_update_success = self.index.put(log_record.encode(), log_record_pos);

        // Return an error if the index update failed
        if !index_update_success {
            return Err(Error::IndexUpdate);
        }

        Ok(())
    }

    fn append_log_record(&self, log_record: &LogRecord) -> Result<LogRecordPos> {
        // Encode the lof record to bytes
        let bytes = log_record.encode();
        let bytes_length = bytes.len() as u64;

        // Aquire write lock on active file
        let mut active_file_write_guard = self.active_file.write();

        // The bytes to write will exceed the max size of the active file
        if active_file_write_guard.write_offset() + bytes_length > self.config.max_data_file_size {
            // Sync the active file to disk
            active_file_write_guard.sync()?;

            // Current active data file
            let active_data_file = DataFile::new(
                &self.config.data_file_dir,
                active_file_write_guard.file_id()
            );

            // Put into the old files
            let mut older_files_write_guard = self.older_files.write();
            older_files_write_guard.insert(active_file_write_guard.file_id(), active_data_file);

            // Create a new data file
            let data_file = DataFile::new(
                &self.config.data_file_dir,
                // Increment the file ID
                active_file_write_guard.file_id() + 1
            );

            // Set to active file
            *active_file_write_guard = data_file;
        }

        // Get write offset of the active file
        active_file_write_guard.write(&bytes)?;

        // Sync the active file to disk if required
        if self.config.sync_writes {
            active_file_write_guard.sync()?;
        }

        // Return log record position
        Ok(LogRecordPos {
            file_id: active_file_write_guard.file_id(),
            offest: active_file_write_guard.write_offset(),
        })
    }
}

pub struct Config {
    /// Directory where data files are stored.
    pub data_file_dir: PathBuf,

    /// Max size of a data file.
    pub max_data_file_size: u64,

    /// Whether to sync writes to disk each time a log record is appended.
    pub sync_writes: bool,
}
